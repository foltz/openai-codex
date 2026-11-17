use crate::outgoing_message::ConnectionId;
use crate::outgoing_message::ConnectionRequestId;
use crate::outgoing_message::OutgoingMessageSender;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ThreadGoal;
use codex_app_server_protocol::ThreadHistoryBuilder;
use codex_app_server_protocol::ThreadHistoryTurnMetadata;
use codex_app_server_protocol::ThreadInteractiveSubscriptionChangedNotification;
use codex_app_server_protocol::ThreadInteractiveSubscriptionEntry;
use codex_app_server_protocol::ThreadInteractiveSubscriptionListResponse;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadRetentionAcquireResponse;
use codex_app_server_protocol::ThreadRetentionRefusalReason;
use codex_app_server_protocol::ThreadSettings;
use codex_app_server_protocol::Turn;
use codex_app_server_protocol::TurnError;
use codex_app_server_protocol::TurnItemsView;
use codex_core::CodexThread;
use codex_core::ThreadConfigSnapshot;
use codex_file_watcher::WatchRegistration;
use codex_protocol::ThreadId;
#[cfg(test)]
use codex_protocol::config_types::MultiAgentMode;
use codex_protocol::items::AgentMessageContent as CoreAgentMessageContent;
use codex_protocol::items::TurnItem as CoreTurnItem;
use codex_protocol::models::MessagePhase;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::RolloutItem;
use codex_rollout::state_db::StateDbHandle;
use codex_utils_path_uri::LegacyAppPathString;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::Weak;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tracing::error;
use uuid::Uuid;

type PendingInterruptQueue = Vec<ConnectionRequestId>;

// The unload that inserts an entry owns its removal. Dropping the sender wakes deferred
// listener attachment only after that unload finishes cleanup.
pub(crate) type PendingThreadUnloads = Arc<Mutex<HashMap<ThreadId, watch::Sender<()>>>>;

pub(crate) mod recovery;
mod retirement;
pub(crate) use retirement::RetentionRetirementClaim;
pub(crate) use retirement::RetentionRetirementRefusal;
pub(crate) use retirement::RetentionSnapshot;

/// A server-minted, process-local identity for one eligible connection.
///
/// This deliberately cannot be constructed from a `ConnectionId`: connection
/// routing and retention authority have different lifetimes and must not be
/// interchangeable at call sites.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum RetentionPrincipalOwner {
    /// No lifecycle census has established whether this principal is
    /// connection-owned or belongs to a thread runtime. This state must never
    /// grant retention authority.
    #[allow(dead_code)] // Retained as the fail-closed principal classification.
    Unclassified,
    /// The principal belongs to a connection whose lifetime is independent of
    /// any app-server thread.
    ConnectionOwned,
    /// The principal belongs to a runtime owned by the named thread.
    #[allow(dead_code)] // Structural R009 control; no production caller exists at this basis.
    ThreadOwned(ThreadId),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct RetentionPrincipalId {
    id: Uuid,
    owner: RetentionPrincipalOwner,
}

#[derive(Clone, Copy, Debug)]
enum RetentionAction {
    Acquired,
    Released,
    Unsubscribed,
    Cleared,
    ConnectionClosed,
    ThreadRemoved,
}

fn record_retention_action(
    thread_id: ThreadId,
    principal: RetentionPrincipalId,
    action: RetentionAction,
) {
    tracing::info!(
        thread_id = %thread_id,
        retention_principal_id = %principal.id,
        retention_principal_owner = ?principal.owner(),
        retention_action = ?action,
        "thread retention authority changed"
    );
}

impl RetentionPrincipalId {
    #[cfg(test)]
    pub(crate) fn unclassified() -> Self {
        Self {
            id: Uuid::now_v7(),
            owner: RetentionPrincipalOwner::Unclassified,
        }
    }

    pub(crate) fn connection_owned() -> Self {
        Self {
            id: Uuid::now_v7(),
            owner: RetentionPrincipalOwner::ConnectionOwned,
        }
    }

    #[allow(dead_code)] // Structural R009 control; exercised by the test kernel.
    pub(crate) fn for_thread_runtime(thread_id: ThreadId) -> Self {
        Self {
            id: Uuid::now_v7(),
            owner: RetentionPrincipalOwner::ThreadOwned(thread_id),
        }
    }

    pub(crate) fn owner(self) -> RetentionPrincipalOwner {
        self.owner
    }
}

/// A server-minted, process-local capability for one exact retention grant.
///
/// It is intentionally distinct from both the retaining principal and the
/// target `ThreadId`; the wire representation is produced only at the request
/// boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RetentionGrantId {
    Minted(Uuid),
    Unrecognized,
}

impl RetentionGrantId {
    fn new() -> Self {
        Self::Minted(Uuid::now_v7())
    }

    pub(crate) fn from_wire(value: &str) -> Self {
        Uuid::parse_str(value)
            .map(Self::Minted)
            .unwrap_or(Self::Unrecognized)
    }

    pub(crate) fn into_wire(self) -> String {
        match self {
            Self::Minted(value) => value.to_string(),
            Self::Unrecognized => unreachable!("unrecognized handle is never minted by the server"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RetentionAcquireOutcome {
    Acquired { grant_id: RetentionGrantId },
    AlreadyHeld { grant_id: RetentionGrantId },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RetentionReleaseOutcome {
    Released,
    NotHeld,
    GrantMismatch,
}

/// Opt-in retention requested by `thread/start`, `thread/resume` or
/// `thread/fork` with `retainForConnection`. The principal is derived from the
/// server-established session exactly as for `thread/retention/acquire`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConnectionRetention {
    NotRequested,
    Refused(ThreadRetentionRefusalReason),
    Requested(RetentionPrincipalId),
}

/// Request params paired with the retention authority the session resolved
/// for their `retainForConnection` flag.
#[derive(Clone, Debug)]
pub(crate) struct RetainingParams<P> {
    pub(crate) params: P,
    pub(crate) retention: ConnectionRetention,
}

impl<P> RetainingParams<P> {
    pub(crate) fn not_requested(params: P) -> Self {
        Self {
            params,
            retention: ConnectionRetention::NotRequested,
        }
    }
}

/// A grant minted by an in-flight request, released if that request fails
/// before its response reports the grant.
pub(crate) type MintedRetention = (ThreadId, RetentionPrincipalId, RetentionGrantId);

impl ConnectionRetention {
    /// No grant is held because the authority was lost or revoked before the
    /// response (a failed attach, or a clear whose subscription did not move).
    pub(crate) fn unavailable(self) -> ConnectionRetentionOutcome {
        self.unattempted(ThreadRetentionRefusalReason::AuthorityUnavailable)
    }

    /// The requesting connection closed, so its principal is no longer live.
    pub(crate) fn connection_closed(self) -> ConnectionRetentionOutcome {
        self.unattempted(ThreadRetentionRefusalReason::IneligiblePrincipal)
    }

    /// The outcome when no grant could be attempted for this request.
    pub(crate) fn unattempted(
        self,
        reason: ThreadRetentionRefusalReason,
    ) -> ConnectionRetentionOutcome {
        match self {
            Self::NotRequested => ConnectionRetentionOutcome::NotRequested,
            Self::Refused(reason) => ConnectionRetentionOutcome::Refused(reason),
            Self::Requested(_) => ConnectionRetentionOutcome::Refused(reason),
        }
    }
}

/// Server-side result of an atomic subscription-and-grant. It keeps the
/// minted grant so a failed request can release exactly what it created.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ConnectionRetentionOutcome {
    NotRequested,
    Refused(ThreadRetentionRefusalReason),
    Granted {
        principal: RetentionPrincipalId,
        outcome: Result<RetentionAcquireOutcome, RetentionAuthorityError>,
    },
}

impl ConnectionRetentionOutcome {
    /// Wire evidence for the response; `None` only when not requested.
    pub(crate) fn evidence(&self) -> Option<ThreadRetentionAcquireResponse> {
        match self {
            Self::NotRequested => None,
            Self::Refused(reason) => {
                Some(ThreadRetentionAcquireResponse::Refused { reason: *reason })
            }
            Self::Granted { outcome, .. } => Some(
                crate::message_processor::retention_acquire_response(outcome.clone()),
            ),
        }
    }

    /// A grant newly minted by this request. Pre-existing grants are never
    /// returned, so failure cleanup cannot release another adoption's grant.
    pub(crate) fn minted_grant(&self) -> Option<(RetentionPrincipalId, RetentionGrantId)> {
        match self {
            Self::Granted {
                principal,
                outcome: Ok(RetentionAcquireOutcome::Acquired { grant_id }),
            } => Some((*principal, grant_id.clone())),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RetentionAuthorityError {
    IneligiblePrincipal,
    AuthorityUnavailable,
    UnknownThread,
    SelfRetention,
    LifecycleClosed,
}

pub(crate) struct PendingThreadResumeRequest {
    pub(crate) request_id: ConnectionRequestId,
    pub(crate) history_items: Vec<RolloutItem>,
    /// Usage attribution already resolved while cold-loading a paginated child.
    pub(crate) cold_resume_token_usage_turn_id: Option<String>,
    pub(crate) config_snapshot: ThreadConfigSnapshot,
    pub(crate) instruction_sources: Vec<LegacyAppPathString>,
    pub(crate) thread_summary: codex_app_server_protocol::Thread,
    pub(crate) emit_thread_goal_update: bool,
    pub(crate) thread_goal_state_db: Option<StateDbHandle>,
    pub(crate) include_turns: bool,
    pub(crate) initial_turns_page:
        Option<codex_app_server_protocol::ThreadResumeInitialTurnsPageParams>,
    pub(crate) paginated_turns: Option<Vec<Turn>>,
    pub(crate) paginated_initial_turns_page: Option<codex_app_server_protocol::TurnsPage>,
    pub(crate) paginated_initial_turns_page_with_active_slot:
        Option<codex_app_server_protocol::TurnsPage>,
    pub(crate) resume_cursor_store: Option<Arc<dyn codex_thread_store::ThreadStore>>,
    pub(crate) redact_resume_payloads: bool,
    /// Installed atomically with this connection's warm-resume subscription.
    pub(crate) retention: ConnectionRetention,
}

// ThreadListenerCommand is used to perform operations in the context of the thread listener, for serialization purposes.
pub(crate) enum ThreadListenerCommand {
    // SendThreadResumeResponse is used to resume an already running thread by sending the thread's history to the client and atomically subscribing for new updates.
    SendThreadResumeResponse {
        request: Box<PendingThreadResumeRequest>,
        completion_tx: oneshot::Sender<()>,
    },
    // EmitThreadGoalUpdated is used to order goal updates with running-thread resume responses and goal clears.
    EmitThreadGoalUpdated {
        turn_id: Option<String>,
        goal: ThreadGoal,
    },
    // EmitThreadQueueChanged orders durable queue updates with thread notifications.
    EmitThreadQueueChanged,
    // EmitWarning is used to order extension warnings with other thread notifications.
    EmitWarning {
        message: String,
    },
    // EmitThreadGoalCleared is used to order app-server goal clears with running-thread resume responses.
    EmitThreadGoalCleared,
    // EmitThreadGoalSnapshot is used to read and emit the latest goal state in the listener order.
    EmitThreadGoalSnapshot {
        state_db: StateDbHandle,
    },
    // ResolveServerRequest is used to notify the client that the request has been resolved.
    // It is executed in the thread listener's context to ensure that the resolved notification is ordered with regard to the request itself.
    ResolveServerRequest {
        request_id: RequestId,
        completion_tx: oneshot::Sender<()>,
    },
}

/// Retained until core resolves or withdraws the prompt, even if the client replies first.
pub(crate) struct PendingNetworkApproval {
    pub(crate) request_id: RequestId,
    pub(crate) permission_guard: crate::thread_status::ThreadWatchActiveGuard,
}

/// Per-conversation accumulation of the latest states e.g. error message while a turn runs.
#[derive(Default, Clone)]
pub(crate) struct TurnSummary {
    pub(crate) started_at: Option<i64>,
    pub(crate) command_start_times: HashMap<String, i64>,
    pub(crate) last_error: Option<TurnError>,
    pub(crate) last_agent_message: Option<ThreadItem>,
}

impl TurnSummary {
    pub(crate) fn record_command_start(&mut self, item_id: String, started_at_ms: i64) -> bool {
        if let std::collections::hash_map::Entry::Vacant(entry) =
            self.command_start_times.entry(item_id)
        {
            entry.insert(started_at_ms);
            true
        } else {
            false
        }
    }
}

#[derive(Default)]
pub(crate) struct ThreadState {
    goal_resume_lock: Arc<Mutex<()>>,
    pub(crate) pending_interrupts: PendingInterruptQueue,
    pub(crate) network_approvals: HashMap<String, PendingNetworkApproval>,
    pub(crate) turn_summary: TurnSummary,
    pub(crate) last_terminal_turn_id: Option<String>,
    /// Lets an internal runtime replacement wait until the old listener has processed Core's
    /// `ShutdownComplete` event before that listener is superseded.
    pub(crate) shutdown_drain: Option<watch::Receiver<bool>>,
    pub(crate) cancel_tx: Option<oneshot::Sender<()>>,
    pub(crate) listener_completion:
        Option<futures::future::Shared<futures::future::BoxFuture<'static, bool>>>,
    pub(crate) experimental_raw_events: bool,
    pub(crate) listener_generation: u64,
    last_thread_settings: Option<ThreadSettings>,
    listener_command_tx: Option<mpsc::UnboundedSender<ThreadListenerCommand>>,
    current_turn_history: ThreadHistoryBuilder,
    history_mode: ThreadHistoryMode,
    evict_superseded_agent_text: bool,
    listener_thread: Option<Weak<CodexThread>>,
    watch_registration: WatchRegistration,
}

impl ThreadState {
    /// Serializes goal edits, cold resume, and terminal cleanup for this exact state.
    pub(crate) async fn lock_goal_resume(state: &Mutex<Self>) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = Arc::clone(&state.lock().await.goal_resume_lock);
        lock.lock_owned().await
    }

    pub(crate) fn listener_matches(&self, conversation: &Arc<CodexThread>) -> bool {
        self.listener_thread
            .as_ref()
            .and_then(Weak::upgrade)
            .is_some_and(|existing| Arc::ptr_eq(&existing, conversation))
    }

    /// A shut-down runtime can still have terminal events buffered for its listener.
    pub(crate) fn pending_shutdown_drain(&self) -> Option<watch::Receiver<bool>> {
        let drain = self.shutdown_drain.as_ref()?;
        let thread = self.listener_thread.as_ref()?.upgrade()?;
        (thread.has_shutdown_status() && !*drain.borrow() && drain.has_changed().is_ok())
            .then(|| drain.clone())
    }

    pub(crate) fn set_listener(
        &mut self,
        cancel_tx: oneshot::Sender<()>,
        shutdown_drain: watch::Receiver<bool>,
        conversation: &Arc<CodexThread>,
        watch_registration: WatchRegistration,
        thread_settings_baseline: ThreadSettings,
        history_mode: ThreadHistoryMode,
    ) -> (mpsc::UnboundedReceiver<ThreadListenerCommand>, u64) {
        if let Some(previous) = self.cancel_tx.replace(cancel_tx) {
            let _ = previous.send(());
        }
        self.shutdown_drain = Some(shutdown_drain);
        self.listener_generation = self.listener_generation.wrapping_add(1);
        self.last_thread_settings = Some(thread_settings_baseline);
        self.history_mode = history_mode;
        self.evict_superseded_agent_text =
            history_mode == ThreadHistoryMode::Paginated && conversation.rollout_path().is_some();
        let (listener_command_tx, listener_command_rx) = mpsc::unbounded_channel();
        self.listener_command_tx = Some(listener_command_tx);
        self.listener_thread = Some(Arc::downgrade(conversation));
        self.watch_registration = watch_registration;
        (listener_command_rx, self.listener_generation)
    }

    pub(crate) fn clear_listener(&mut self) {
        if let Some(cancel_tx) = self.cancel_tx.take() {
            let _ = cancel_tx.send(());
        }
        self.shutdown_drain = None;
        self.listener_command_tx = None;
        self.current_turn_history.reset();
        self.listener_thread = None;
        self.watch_registration = WatchRegistration::default();
    }

    pub(crate) fn set_experimental_raw_events(&mut self, enabled: bool) {
        self.experimental_raw_events = enabled;
    }

    pub(crate) fn listener_command_tx(
        &self,
    ) -> Option<mpsc::UnboundedSender<ThreadListenerCommand>> {
        self.listener_command_tx.clone()
    }

    pub(crate) fn active_turn_snapshot(&self) -> Option<Turn> {
        self.current_turn_history.active_turn_snapshot()
    }

    /// Returns the same turn ID as `active_turn_snapshot` without cloning its items.
    pub(crate) fn active_turn_id(&self) -> Option<&str> {
        self.current_turn_history.active_turn_id()
    }

    pub(crate) fn active_turn_snapshot_with_items_view(
        &self,
        items_view: TurnItemsView,
    ) -> Option<Turn> {
        self.current_turn_history
            .active_turn_snapshot_with_items_view(items_view)
    }

    pub(crate) fn active_turn_metadata_snapshot(&self) -> Option<ThreadHistoryTurnMetadata> {
        self.current_turn_history.active_turn_metadata_snapshot()
    }

    pub(crate) fn track_current_turn_event(&mut self, event_turn_id: &str, event: &EventMsg) {
        if let EventMsg::TurnStarted(payload) = event {
            self.turn_summary = TurnSummary {
                started_at: payload.started_at,
                ..Default::default()
            };
        }
        if let EventMsg::ItemCompleted(payload) = event
            && let CoreTurnItem::AgentMessage(item) = &payload.item
            && matches!(item.phase, Some(MessagePhase::FinalAnswer) | None)
            && item.content.iter().any(|content| {
                matches!(content, CoreAgentMessageContent::Text { text } if !text.trim().is_empty())
            })
        {
            self.turn_summary.last_agent_message = Some(ThreadItem::from(payload));
        }
        if self.evict_superseded_agent_text {
            self.current_turn_history.handle_live_paginated_event(event);
        } else {
            self.current_turn_history
                .handle_event_for_history_mode(event, self.history_mode);
        }
        if matches!(event, EventMsg::TurnAborted(_) | EventMsg::TurnComplete(_)) {
            self.last_terminal_turn_id = Some(event_turn_id.to_string());
            if !self.current_turn_history.has_active_turn() {
                self.current_turn_history.reset();
            }
        }
    }

    pub(crate) fn note_thread_settings(&mut self, thread_settings: ThreadSettings) -> bool {
        let changed = self.last_thread_settings.as_ref() != Some(&thread_settings);
        self.last_thread_settings = Some(thread_settings);
        changed
    }
}

pub(crate) async fn resolve_server_request_on_thread_listener(
    thread_state: &Arc<Mutex<ThreadState>>,
    request_id: RequestId,
) {
    let (completion_tx, completion_rx) = oneshot::channel();
    let listener_command_tx = {
        let state = thread_state.lock().await;
        state.listener_command_tx()
    };
    let Some(listener_command_tx) = listener_command_tx else {
        error!("failed to remove pending client request: thread listener is not running");
        return;
    };

    if listener_command_tx
        .send(ThreadListenerCommand::ResolveServerRequest {
            request_id,
            completion_tx,
        })
        .is_err()
    {
        error!(
            "failed to remove pending client request: thread listener command channel is closed"
        );
        return;
    }

    if let Err(err) = completion_rx.await {
        error!("failed to remove pending client request: {err}");
    }
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn retaining_listener_install_is_atomic_and_reuses_only_its_exact_grant() {
        let manager = ThreadStateManager::new();
        let thread = ThreadId::new();
        let connection = ConnectionId(1);
        let principal = RetentionPrincipalId::connection_owned();
        manager
            .connection_initialized(
                connection,
                ConnectionCapabilities {
                    retention_principal: Some(principal),
                    ..Default::default()
                },
            )
            .await;
        let candidate = manager.created_listener_state(thread).await;
        let request = Some((connection, ConnectionRetention::Requested(principal)));
        let refused = manager
            .subscribe_and_install_listener_retaining(
                thread,
                &candidate,
                &[connection],
                request,
                |_| Err::<(), _>("not installed"),
            )
            .await;
        assert!(matches!(refused, Err("not installed")));
        assert!(!manager.state.lock().await.lifecycle.contains_key(&thread));
        let (_, _, attached, outcome) = manager
            .subscribe_and_install_listener_retaining(
                thread,
                &candidate,
                &[connection],
                request,
                |_| Ok::<_, ()>(()),
            )
            .await
            .unwrap()
            .unwrap();
        assert!(attached);
        let (owner, minted) = outcome.minted_grant().expect("exact new grant");
        assert_eq!(owner, principal);
        let snapshot = manager.subscribe_to_retention(thread).await.unwrap();
        assert!(
            snapshot.borrow().granted,
            "grant precedes listener driver publication"
        );
        let (_, _, _, again) = manager
            .subscribe_and_install_listener_retaining(
                thread,
                &candidate,
                &[connection],
                request,
                |_| Ok::<_, ()>(()),
            )
            .await
            .unwrap()
            .unwrap();
        assert!(
            again.minted_grant().is_none(),
            "pre-existing grant must not be released as new"
        );
        assert_eq!(
            again.evidence(),
            Some(ThreadRetentionAcquireResponse::AlreadyHeld {
                grant_id: minted.into_wire(),
            })
        );
        manager.remove_connection(connection).await;
        assert!(
            !snapshot.borrow().granted,
            "ordinary connection close revokes its grant"
        );
    }

    #[tokio::test]
    async fn stale_listener_candidate_cannot_grant_retention_to_replacement_state() {
        let manager = ThreadStateManager::new();
        let thread = ThreadId::new();
        let connection = ConnectionId(1);
        let principal = RetentionPrincipalId::connection_owned();
        manager
            .connection_initialized(
                connection,
                ConnectionCapabilities {
                    retention_principal: Some(principal),
                    ..Default::default()
                },
            )
            .await;
        let stale = manager.created_listener_state(thread).await;
        let replacement = manager.thread_state(thread).await;
        assert!(!Arc::ptr_eq(stale.state(), &replacement));
        let mut invoked = false;
        let result = manager
            .subscribe_and_install_listener_retaining(
                thread,
                &stale,
                &[connection],
                Some((connection, ConnectionRetention::Requested(principal))),
                |_| {
                    invoked = true;
                    Ok::<_, ()>(())
                },
            )
            .await
            .unwrap();
        assert!(result.is_none());
        assert!(!invoked);
        assert!(
            !manager
                .subscribe_to_retention(thread)
                .await
                .unwrap()
                .borrow()
                .granted
        );
    }

    #[tokio::test]
    async fn atomic_listener_install_preserves_upstream_failure_and_exact_subscription_identity() {
        let manager = ThreadStateManager::new();
        let id = ThreadId::new();
        let connection = ConnectionId(1);
        manager
            .connection_initialized(
                connection,
                ConnectionCapabilities {
                    trusted_interactive: true,
                    ..Default::default()
                },
            )
            .await;
        let candidate = manager.created_listener_state(id).await;
        let failed = manager
            .subscribe_and_install_listener(id, &candidate, &[connection], |_| {
                Err::<(), _>("installation refused")
            })
            .await;
        assert!(matches!(failed, Err("installation refused")));
        {
            let state = manager.state.lock().await;
            assert!(!state.threads.contains_key(&id));
            assert!(!state.lifecycle.contains_key(&id));
        }
        assert!(
            manager
                .thread_interactive_subscription_list()
                .await
                .entries
                .is_empty()
        );
        let installed = manager
            .subscribe_and_install_listener(id, &candidate, &[connection], |attached| {
                Ok::<_, ()>(attached)
            })
            .await
            .unwrap()
            .unwrap();
        assert!(installed.0 && installed.2 && *installed.1.borrow());
        {
            let state = manager.state.lock().await;
            assert!(Arc::ptr_eq(&state.threads[&id].state, candidate.state()));
            assert!(state.lifecycle[&id].is_identity(candidate.state()));
        }
        let first = manager.thread_interactive_subscription_list().await;
        assert_eq!(first.entries[0].interactive_subscription_count, 1);
        manager
            .subscribe_and_install_listener(id, &candidate, &[connection], |_| Ok::<_, ()>(()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(manager.thread_interactive_subscription_list().await, first);
    }
    use super::*;
    use crate::outgoing_message::OutgoingEnvelope;
    use crate::outgoing_message::OutgoingMessage;
    use codex_analytics::AnalyticsEventsClient;
    use codex_app_server_protocol::ApprovalsReviewer;
    use codex_app_server_protocol::AskForApproval;
    use codex_app_server_protocol::SandboxPolicy;
    use codex_protocol::config_types::CollaborationMode;
    use codex_protocol::config_types::ModeKind;
    use codex_protocol::config_types::Settings;
    use codex_utils_absolute_path::AbsolutePathBuf;
    use pretty_assertions::assert_eq;
    use tokio::time::Duration;
    use tokio::time::timeout;

    #[test]
    fn note_thread_settings_reports_only_effective_changes() {
        let mut state = ThreadState::default();
        let initial = thread_settings("mock-model");
        let updated = thread_settings("mock-model-2");

        let results = vec![
            state.note_thread_settings(initial.clone()),
            state.note_thread_settings(initial),
            state.note_thread_settings(updated.clone()),
            state.note_thread_settings(updated),
        ];

        assert_eq!(results, vec![true, false, true, false]);
    }

    #[tokio::test]
    async fn clear_authority_reservation_distinguishes_rejection_causes() {
        let manager = ThreadStateManager::new();
        let unknown_thread_id = ThreadId::new();
        let thread_id = ThreadId::new();
        let subscribed_connection = ConnectionId(1);
        let other_connection = ConnectionId(2);

        manager
            .connection_initialized(subscribed_connection, ConnectionCapabilities::default())
            .await;
        manager
            .connection_initialized(other_connection, ConnectionCapabilities::default())
            .await;

        assert_eq!(
            manager
                .reserve_clear_transition_authority(unknown_thread_id, subscribed_connection)
                .await,
            Err(ClearTransitionAuthorityError::UnknownPredecessor)
        );

        manager
            .try_ensure_connection_subscribed(
                thread_id,
                subscribed_connection,
                /* experimental_raw_events */ false,
            )
            .await
            .expect("connection should be live");

        assert_eq!(
            manager
                .reserve_clear_transition_authority(thread_id, other_connection)
                .await,
            Err(ClearTransitionAuthorityError::NotSubscribed)
        );
        assert_eq!(
            manager
                .reserve_clear_transition_authority(thread_id, subscribed_connection)
                .await,
            Ok(())
        );
        assert_eq!(
            manager
                .reserve_clear_transition_authority(thread_id, subscribed_connection)
                .await,
            Err(ClearTransitionAuthorityError::TransitionConflict)
        );
    }

    #[tokio::test]
    async fn releasing_clear_authority_allows_a_new_subscriber_to_reserve() {
        let manager = ThreadStateManager::new();
        let thread_id = ThreadId::new();
        let first_connection = ConnectionId(1);
        let second_connection = ConnectionId(2);

        for connection_id in [first_connection, second_connection] {
            manager
                .connection_initialized(connection_id, ConnectionCapabilities::default())
                .await;
            manager
                .try_ensure_connection_subscribed(
                    thread_id,
                    connection_id,
                    /* experimental_raw_events */ false,
                )
                .await
                .expect("connection should be live");
        }

        assert_eq!(
            manager
                .reserve_clear_transition_authority(thread_id, first_connection)
                .await,
            Ok(())
        );
        assert!(manager.release_clear_transition_authority(thread_id).await);
        assert!(!manager.release_clear_transition_authority(thread_id).await);
        assert_eq!(
            manager
                .reserve_clear_transition_authority(thread_id, second_connection)
                .await,
            Ok(())
        );
    }

    #[tokio::test]
    async fn subscription_snapshot_tracks_only_trusted_interactive_subscriptions() {
        let manager = ThreadStateManager::new();
        let thread_id = ThreadId::new();
        let trusted_connection = ConnectionId(1);
        let untrusted_connection = ConnectionId(2);

        let startup_snapshot = manager.thread_interactive_subscription_list().await;
        assert_eq!(startup_snapshot.revision, 0);
        assert!(startup_snapshot.entries.is_empty());

        manager
            .connection_initialized(
                trusted_connection,
                ConnectionCapabilities {
                    request_attestation: false,
                    trusted_interactive: true,
                    retention_principal: None,
                },
            )
            .await;
        manager
            .connection_initialized(untrusted_connection, ConnectionCapabilities::default())
            .await;

        manager
            .try_ensure_connection_subscribed(
                thread_id,
                untrusted_connection,
                /* experimental_raw_events */ false,
            )
            .await
            .expect("untrusted connection should be live");
        let untrusted_snapshot = manager.thread_interactive_subscription_list().await;
        assert_eq!(untrusted_snapshot.revision, 0);
        assert!(untrusted_snapshot.entries.is_empty());

        manager
            .try_ensure_connection_subscribed(
                thread_id,
                trusted_connection,
                /* experimental_raw_events */ false,
            )
            .await
            .expect("trusted connection should be live");
        manager
            .try_ensure_connection_subscribed(
                thread_id,
                trusted_connection,
                /* experimental_raw_events */ false,
            )
            .await
            .expect("trusted connection should remain live");
        let attached_snapshot = manager.thread_interactive_subscription_list().await;
        assert_eq!(attached_snapshot.generation, startup_snapshot.generation);
        assert_eq!(attached_snapshot.revision, 1);
        assert_eq!(
            attached_snapshot.entries,
            vec![ThreadInteractiveSubscriptionEntry {
                thread_id: thread_id.to_string(),
                interactive_subscription_count: 1,
            }]
        );

        manager.remove_connection(trusted_connection).await;
        let closed_snapshot = manager.thread_interactive_subscription_list().await;
        assert_eq!(closed_snapshot.revision, 2);
        assert!(closed_snapshot.entries.is_empty());
    }

    #[tokio::test]
    async fn one_interactive_connection_can_subscribe_to_distinct_threads() {
        let manager = ThreadStateManager::new();
        let first = ThreadId::new();
        let second = ThreadId::new();
        let connection = ConnectionId(1);
        manager
            .connection_initialized(
                connection,
                ConnectionCapabilities {
                    trusted_interactive: true,
                    ..Default::default()
                },
            )
            .await;
        for thread_id in [first, second, first] {
            manager
                .try_ensure_connection_subscribed(
                    thread_id, connection, /*experimental_raw_events*/ false,
                )
                .await
                .expect("live interactive connection subscribes");
        }
        let before = manager.thread_interactive_subscription_list().await;
        let mut entries = [first, second]
            .into_iter()
            .map(|thread_id| ThreadInteractiveSubscriptionEntry {
                thread_id: thread_id.to_string(),
                interactive_subscription_count: 1,
            })
            .collect::<Vec<_>>();
        entries.sort_unstable_by(|left, right| left.thread_id.cmp(&right.thread_id));
        assert_eq!(
            before,
            ThreadInteractiveSubscriptionListResponse {
                generation: before.generation.clone(),
                revision: 2,
                entries,
            }
        );
        manager.remove_connection(connection).await;
        assert_eq!(
            manager.thread_interactive_subscription_list().await,
            ThreadInteractiveSubscriptionListResponse {
                generation: before.generation,
                revision: 3,
                entries: Vec::new(),
            }
        );
    }

    #[tokio::test]
    async fn reconnect_does_not_retain_interactive_subscription_without_fresh_entitlement() {
        let manager = ThreadStateManager::new();
        let thread_id = ThreadId::new();
        let connection_id = ConnectionId(1);

        manager
            .connection_initialized(
                connection_id,
                ConnectionCapabilities {
                    request_attestation: false,
                    trusted_interactive: true,
                    retention_principal: None,
                },
            )
            .await;
        manager
            .try_ensure_connection_subscribed(
                thread_id,
                connection_id,
                /* experimental_raw_events */ false,
            )
            .await
            .expect("entitled connection should be live");
        assert_eq!(
            manager
                .thread_interactive_subscription_list()
                .await
                .entries
                .len(),
            1
        );

        manager.remove_connection(connection_id).await;
        manager
            .connection_initialized(connection_id, ConnectionCapabilities::default())
            .await;
        manager
            .try_ensure_connection_subscribed(
                thread_id,
                connection_id,
                /* experimental_raw_events */ false,
            )
            .await
            .expect("reconnected connection should be live");

        let snapshot = manager.thread_interactive_subscription_list().await;
        assert!(snapshot.entries.is_empty());
        assert_eq!(snapshot.revision, 2);
    }

    #[tokio::test]
    async fn interactive_subscription_notifications_are_revisioned_and_ignore_duplicate_subscriptions()
     {
        let manager = ThreadStateManager::new();
        let thread_id = ThreadId::new();
        let connection_id = ConnectionId(1);
        let (outgoing_tx, mut outgoing_rx) = mpsc::channel(8);
        manager.set_interactive_subscription_notification_outgoing(Arc::new(
            OutgoingMessageSender::new(outgoing_tx, AnalyticsEventsClient::disabled()),
        ));

        manager
            .connection_initialized(
                connection_id,
                ConnectionCapabilities {
                    request_attestation: false,
                    trusted_interactive: true,
                    retention_principal: None,
                },
            )
            .await;
        let generation = manager
            .thread_interactive_subscription_list()
            .await
            .generation;

        manager
            .try_ensure_connection_subscribed(
                thread_id,
                connection_id,
                /* experimental_raw_events */ false,
            )
            .await
            .expect("trusted connection should be live");
        let attached = recv_interactive_subscription_changed_notification(&mut outgoing_rx).await;
        assert_eq!(attached.generation, generation);
        assert_eq!(attached.revision, 1);
        assert_eq!(
            attached.changes,
            vec![ThreadInteractiveSubscriptionEntry {
                thread_id: thread_id.to_string(),
                interactive_subscription_count: 1,
            }]
        );

        manager
            .try_ensure_connection_subscribed(
                thread_id,
                connection_id,
                /* experimental_raw_events */ false,
            )
            .await
            .expect("duplicate subscription should remain live");
        assert!(
            timeout(Duration::from_millis(50), outgoing_rx.recv())
                .await
                .is_err()
        );

        assert!(
            manager
                .unsubscribe_connection_from_thread(thread_id, connection_id)
                .await
        );
        let detached = recv_interactive_subscription_changed_notification(&mut outgoing_rx).await;
        assert_eq!(detached.generation, generation);
        assert_eq!(detached.revision, 2);
        assert_eq!(
            detached.changes,
            vec![ThreadInteractiveSubscriptionEntry {
                thread_id: thread_id.to_string(),
                interactive_subscription_count: 0,
            }]
        );

        manager
            .try_ensure_connection_subscribed(
                thread_id,
                connection_id,
                /* experimental_raw_events */ false,
            )
            .await
            .expect("re-attached connection should be live");
        let reattached = recv_interactive_subscription_changed_notification(&mut outgoing_rx).await;
        assert_eq!(reattached.revision, 3);

        manager.remove_connection(connection_id).await;
        let closed = recv_interactive_subscription_changed_notification(&mut outgoing_rx).await;
        assert_eq!(closed.generation, generation);
        assert_eq!(closed.revision, 4);
        assert_eq!(
            closed.changes,
            vec![ThreadInteractiveSubscriptionEntry {
                thread_id: thread_id.to_string(),
                interactive_subscription_count: 0,
            }]
        );
    }

    #[tokio::test]
    async fn interactive_subscription_revision_overflow_rotates_generation_before_publishing() {
        let manager = ThreadStateManager::new();
        let thread_id = ThreadId::new();
        let connection_id = ConnectionId(1);
        manager
            .connection_initialized(
                connection_id,
                ConnectionCapabilities {
                    request_attestation: false,
                    trusted_interactive: true,
                    retention_principal: None,
                },
            )
            .await;

        let old_generation = {
            let mut state = manager.state.lock().await;
            state.interactive_subscription_revision = u64::MAX;
            state.interactive_subscription_generation.clone()
        };
        manager
            .try_ensure_connection_subscribed(
                thread_id,
                connection_id,
                /* experimental_raw_events */ false,
            )
            .await
            .expect("trusted connection should be live");

        let snapshot = manager.thread_interactive_subscription_list().await;
        assert_ne!(snapshot.generation, old_generation);
        assert_eq!(snapshot.revision, 0);
        assert_eq!(snapshot.entries.len(), 1);
    }

    #[tokio::test]
    async fn clear_moves_only_requester_with_one_interactive_subscription_change() {
        let manager = ThreadStateManager::new();
        let predecessor_thread_id = ThreadId::new();
        let successor_thread_id = ThreadId::new();
        let requester = ConnectionId(1);
        let bystander = ConnectionId(2);
        for connection_id in [requester, bystander] {
            manager
                .connection_initialized(
                    connection_id,
                    ConnectionCapabilities {
                        request_attestation: false,
                        trusted_interactive: true,
                        retention_principal: None,
                    },
                )
                .await;
            manager
                .try_ensure_connection_subscribed(
                    predecessor_thread_id,
                    connection_id,
                    /* experimental_raw_events */ false,
                )
                .await
                .expect("trusted connection should be live");
        }
        let before = manager.thread_interactive_subscription_list().await;
        let (outgoing_tx, mut outgoing_rx) = mpsc::channel(8);
        manager.set_interactive_subscription_notification_outgoing(Arc::new(
            OutgoingMessageSender::new(outgoing_tx, AnalyticsEventsClient::disabled()),
        ));

        assert!(
            manager
                .move_connection_for_clear(predecessor_thread_id, successor_thread_id, requester)
                .await
        );

        let changed = recv_interactive_subscription_changed_notification(&mut outgoing_rx).await;
        assert_eq!(changed.generation, before.generation);
        assert_eq!(changed.revision, before.revision + 1);
        let mut expected_entries = vec![
            ThreadInteractiveSubscriptionEntry {
                thread_id: predecessor_thread_id.to_string(),
                interactive_subscription_count: 1,
            },
            ThreadInteractiveSubscriptionEntry {
                thread_id: successor_thread_id.to_string(),
                interactive_subscription_count: 1,
            },
        ];
        expected_entries.sort_unstable_by(|left, right| left.thread_id.cmp(&right.thread_id));
        assert_eq!(changed.changes, expected_entries);
        let snapshot = manager.thread_interactive_subscription_list().await;
        assert_eq!(snapshot.revision, changed.revision);
        assert_eq!(snapshot.entries, expected_entries);
        assert!(
            timeout(Duration::from_millis(50), outgoing_rx.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn clear_moves_no_subscription_for_disconnected_requester() {
        for keep_bystander in [false, true] {
            let manager = ThreadStateManager::new();
            let predecessor = ThreadId::new();
            let successor = ThreadId::new();
            let requester = ConnectionId(1);
            let bystander = ConnectionId(2);
            let connections = if keep_bystander {
                vec![requester, bystander]
            } else {
                vec![requester]
            };
            for connection in &connections {
                manager
                    .connection_initialized(
                        *connection,
                        ConnectionCapabilities {
                            request_attestation: false,
                            trusted_interactive: true,
                            retention_principal: None,
                        },
                    )
                    .await;
                manager
                    .try_ensure_connection_subscribed(
                        predecessor,
                        *connection,
                        /* experimental_raw_events */ false,
                    )
                    .await
                    .expect("trusted connection must initially be attached");
            }
            assert_eq!(
                manager.thread_interactive_subscription_list().await.entries,
                vec![ThreadInteractiveSubscriptionEntry {
                    thread_id: predecessor.to_string(),
                    interactive_subscription_count: connections.len() as u32,
                }]
            );

            manager.remove_connection(requester).await;
            let disconnected = manager.thread_interactive_subscription_list().await;
            let expected_entries = if keep_bystander {
                vec![ThreadInteractiveSubscriptionEntry {
                    thread_id: predecessor.to_string(),
                    interactive_subscription_count: 1,
                }]
            } else {
                vec![]
            };
            assert_eq!(disconnected.entries, expected_entries);
            assert!(
                !manager
                    .move_connection_for_clear(predecessor, successor, requester)
                    .await
            );
            assert_eq!(
                manager.thread_interactive_subscription_list().await,
                disconnected
            );
            assert!(!manager.has_subscribers(successor).await);
        }
    }

    #[tokio::test]
    async fn disclosed_clear_successor_cannot_be_resumed_before_atomic_move() {
        let manager = ThreadStateManager::new();
        let predecessor_thread_id = ThreadId::new();
        let successor_thread_id = ThreadId::new();
        let requester = ConnectionId(1);
        manager
            .connection_initialized(
                requester,
                ConnectionCapabilities {
                    request_attestation: false,
                    trusted_interactive: true,
                    retention_principal: None,
                },
            )
            .await;
        manager
            .try_ensure_connection_subscribed(predecessor_thread_id, requester, false)
            .await
            .expect("requester should be attached to predecessor");
        assert_eq!(
            manager
                .reserve_clear_transition_authority(predecessor_thread_id, requester)
                .await,
            Ok(())
        );
        assert!(
            manager
                .reserve_clear_successor_subscription(predecessor_thread_id, successor_thread_id)
                .await
        );
        let (outgoing_tx, mut outgoing_rx) = mpsc::channel(4);
        manager.set_interactive_subscription_notification_outgoing(Arc::new(
            OutgoingMessageSender::new(outgoing_tx, AnalyticsEventsClient::disabled()),
        ));

        assert!(matches!(
            manager
                .try_ensure_connection_subscribed_for_listener(
                    successor_thread_id,
                    requester,
                    false,
                )
                .await,
            Err(ConnectionSubscriptionError::ClearSuccessorReserved)
        ));
        assert!(
            timeout(Duration::from_millis(50), outgoing_rx.recv())
                .await
                .is_err(),
            "the rejected resume must not publish B before the clear move"
        );
        assert!(
            manager
                .move_connection_for_clear(predecessor_thread_id, successor_thread_id, requester)
                .await
        );
        let changed = recv_interactive_subscription_changed_notification(&mut outgoing_rx).await;
        assert_eq!(changed.changes.len(), 2);
        assert!(manager.has_subscribers(successor_thread_id).await);
        assert!(!manager.has_subscribers(predecessor_thread_id).await);
        assert!(
            manager
                .release_clear_transition_authority(predecessor_thread_id)
                .await
        );
    }

    #[tokio::test]
    async fn disclosed_successor_stays_reserved_after_request_scope_releases() {
        let manager = ThreadStateManager::new();
        let predecessor_thread_id = ThreadId::new();
        let successor_thread_id = ThreadId::new();
        let requester = ConnectionId(1);
        manager
            .connection_initialized(requester, ConnectionCapabilities::default())
            .await;
        manager
            .try_ensure_connection_subscribed(predecessor_thread_id, requester, false)
            .await
            .expect("requester should be attached to predecessor");
        assert!(
            manager
                .reserve_clear_transition_authority(predecessor_thread_id, requester)
                .await
                .is_ok()
        );
        assert!(
            manager
                .reserve_clear_successor_subscription(predecessor_thread_id, successor_thread_id)
                .await
        );

        assert!(
            manager
                .release_clear_transition_authority(predecessor_thread_id)
                .await
        );
        assert!(matches!(
            manager
                .try_add_connection_to_thread(successor_thread_id, requester)
                .await,
            Err(ConnectionSubscriptionError::ClearSuccessorReserved)
        ));
        assert!(
            manager
                .terminalize_clear_successor_subscription_reservation(
                    predecessor_thread_id,
                    successor_thread_id
                )
                .await
        );
        assert!(
            manager
                .try_add_connection_to_thread(successor_thread_id, requester)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn retention_grants_are_exact_principal_handles_and_idempotent_when_spent() {
        let manager = ThreadStateManager::new();
        let thread_id = ThreadId::new();
        let connection = ConnectionId(1);
        let principal = RetentionPrincipalId::connection_owned();
        manager
            .connection_initialized(
                connection,
                ConnectionCapabilities {
                    retention_principal: Some(principal),
                    ..ConnectionCapabilities::default()
                },
            )
            .await;
        manager.thread_state(thread_id).await;

        let RetentionAcquireOutcome::Acquired { grant_id } = manager
            .acquire_retention(thread_id, principal)
            .await
            .expect("live server-minted principal is eligible")
        else {
            panic!("first acquire must mint a grant");
        };
        assert_eq!(
            manager.acquire_retention(thread_id, principal).await,
            Ok(RetentionAcquireOutcome::AlreadyHeld {
                grant_id: grant_id.clone()
            })
        );
        let other_principal = RetentionPrincipalId::connection_owned();
        manager
            .connection_initialized(
                ConnectionId(2),
                ConnectionCapabilities {
                    retention_principal: Some(other_principal),
                    ..ConnectionCapabilities::default()
                },
            )
            .await;
        assert_eq!(
            manager
                .release_retention(thread_id, other_principal, &grant_id)
                .await,
            Ok(RetentionReleaseOutcome::NotHeld),
            "a second live principal cannot release the first principal's grant"
        );
        assert_eq!(
            manager
                .release_retention(
                    thread_id,
                    principal,
                    &RetentionGrantId::from_wire(&Uuid::now_v7().to_string()),
                )
                .await,
            Ok(RetentionReleaseOutcome::GrantMismatch)
        );
        assert_eq!(
            manager
                .release_retention(thread_id, principal, &grant_id)
                .await,
            Ok(RetentionReleaseOutcome::Released)
        );
        assert_eq!(
            manager
                .release_retention(thread_id, principal, &grant_id)
                .await,
            Ok(RetentionReleaseOutcome::NotHeld)
        );
        assert_eq!(
            manager
                .release_retention(
                    thread_id,
                    principal,
                    &RetentionGrantId::from_wire(&Uuid::now_v7().to_string()),
                )
                .await,
            Ok(RetentionReleaseOutcome::NotHeld)
        );
    }

    #[tokio::test]
    async fn thread_owned_runtime_cannot_retain_its_own_thread() {
        let manager = ThreadStateManager::new();
        let thread_id = ThreadId::new();
        let principal = RetentionPrincipalId::for_thread_runtime(thread_id);
        manager
            .connection_initialized(
                ConnectionId(1),
                ConnectionCapabilities {
                    retention_principal: Some(principal),
                    ..ConnectionCapabilities::default()
                },
            )
            .await;
        manager.thread_state(thread_id).await;

        assert_eq!(
            manager.acquire_retention(thread_id, principal).await,
            Err(RetentionAuthorityError::SelfRetention)
        );
    }

    #[tokio::test]
    async fn thread_owned_runtime_may_retain_a_different_thread() {
        let manager = ThreadStateManager::new();
        let owning_thread_id = ThreadId::new();
        let target_thread_id = ThreadId::new();
        let principal = RetentionPrincipalId::for_thread_runtime(owning_thread_id);
        manager
            .connection_initialized(
                ConnectionId(1),
                ConnectionCapabilities {
                    retention_principal: Some(principal),
                    ..ConnectionCapabilities::default()
                },
            )
            .await;
        manager.thread_state(target_thread_id).await;

        assert!(matches!(
            manager.acquire_retention(target_thread_id, principal).await,
            Ok(RetentionAcquireOutcome::Acquired { .. })
        ));
    }

    #[tokio::test]
    async fn unclassified_principal_cannot_retain_a_thread() {
        let manager = ThreadStateManager::new();
        let thread_id = ThreadId::new();
        let principal = RetentionPrincipalId::unclassified();
        manager
            .connection_initialized(
                ConnectionId(1),
                ConnectionCapabilities {
                    retention_principal: Some(principal),
                    ..ConnectionCapabilities::default()
                },
            )
            .await;
        manager.thread_state(thread_id).await;

        assert_eq!(
            manager.acquire_retention(thread_id, principal).await,
            Err(RetentionAuthorityError::AuthorityUnavailable)
        );
        assert_eq!(
            manager.acquire_retention(ThreadId::new(), principal).await,
            Err(RetentionAuthorityError::AuthorityUnavailable),
            "unclassified authority must not disclose whether a target exists"
        );
        let handle = RetentionGrantId::from_wire(&Uuid::now_v7().to_string());
        assert_eq!(
            manager
                .release_retention(thread_id, principal, &handle)
                .await,
            Err(RetentionAuthorityError::AuthorityUnavailable)
        );
        assert_eq!(
            manager
                .release_retention(ThreadId::new(), principal, &handle)
                .await,
            Err(RetentionAuthorityError::AuthorityUnavailable),
            "unclassified release must not disclose whether a target exists"
        );
    }

    #[tokio::test]
    async fn removing_thread_state_revokes_its_retention_grants() {
        let manager = ThreadStateManager::new();
        let thread_id = ThreadId::new();
        let principal = RetentionPrincipalId::connection_owned();
        manager
            .connection_initialized(
                ConnectionId(1),
                ConnectionCapabilities {
                    retention_principal: Some(principal),
                    ..ConnectionCapabilities::default()
                },
            )
            .await;
        manager.thread_state(thread_id).await;
        let RetentionAcquireOutcome::Acquired { grant_id } = manager
            .acquire_retention(thread_id, principal)
            .await
            .expect("acquire should succeed")
        else {
            panic!("first acquire must mint a grant");
        };

        manager.remove_thread_state(thread_id).await;
        assert_eq!(
            manager
                .release_retention(thread_id, principal, &grant_id)
                .await,
            Ok(RetentionReleaseOutcome::NotHeld),
            "a removed thread has no active grant"
        );
        manager.thread_state(thread_id).await;
        assert!(
            matches!(
                manager.acquire_retention(thread_id, principal).await,
                Ok(RetentionAcquireOutcome::Acquired { .. })
            ),
            "a reused thread id must not inherit a removed thread's grant"
        );
    }

    #[tokio::test]
    async fn missing_thread_with_stale_grant_index_fails_closed() {
        let manager = ThreadStateManager::new();
        let thread_id = ThreadId::new();
        let principal = RetentionPrincipalId::connection_owned();
        let grant_id = RetentionGrantId::new();
        manager
            .connection_initialized(
                ConnectionId(1),
                ConnectionCapabilities {
                    retention_principal: Some(principal),
                    ..ConnectionCapabilities::default()
                },
            )
            .await;
        {
            let mut state = manager.state.lock().await;
            state
                .retention_grants_by_thread
                .entry(thread_id)
                .or_default()
                .insert(principal, grant_id.clone());
            state
                .retention_threads_by_principal
                .entry(principal)
                .or_default()
                .insert(thread_id);
        }

        assert_eq!(
            manager
                .release_retention(thread_id, principal, &grant_id)
                .await,
            Err(RetentionAuthorityError::AuthorityUnavailable)
        );
        let state = manager.state.lock().await;
        assert_eq!(
            state
                .retention_grants_by_thread
                .get(&thread_id)
                .and_then(|grants| grants.get(&principal)),
            Some(&grant_id),
            "fail-closed release must not mutate stale custody"
        );
    }

    #[tokio::test]
    async fn unsubscribe_and_disconnect_revoke_exact_retention_authority() {
        let manager = ThreadStateManager::new();
        let thread_id = ThreadId::new();
        let connection = ConnectionId(1);
        let principal = RetentionPrincipalId::connection_owned();
        manager
            .connection_initialized(
                connection,
                ConnectionCapabilities {
                    retention_principal: Some(principal),
                    ..ConnectionCapabilities::default()
                },
            )
            .await;
        manager
            .try_ensure_connection_subscribed(thread_id, connection, false)
            .await
            .expect("connection should subscribe");
        let RetentionAcquireOutcome::Acquired { grant_id } = manager
            .acquire_retention(thread_id, principal)
            .await
            .expect("acquire should succeed")
        else {
            panic!("first acquire must mint a grant");
        };

        assert!(
            manager
                .unsubscribe_connection_from_thread(thread_id, connection)
                .await
        );
        assert_eq!(
            manager
                .release_retention(thread_id, principal, &grant_id)
                .await,
            Ok(RetentionReleaseOutcome::NotHeld)
        );

        let RetentionAcquireOutcome::Acquired { .. } = manager
            .acquire_retention(thread_id, principal)
            .await
            .expect("unsubscribe does not revoke the live principal")
        else {
            panic!("reacquire must mint a new grant");
        };
        manager.remove_connection(connection).await;
        assert_eq!(
            manager.acquire_retention(thread_id, principal).await,
            Err(RetentionAuthorityError::IneligiblePrincipal)
        );
    }

    #[tokio::test]
    async fn abrupt_disconnect_revokes_retention_indexes_before_retiring_authority() {
        let manager = ThreadStateManager::new();
        let thread_id = ThreadId::new();
        let connection = ConnectionId(1);
        let principal = RetentionPrincipalId::connection_owned();
        manager
            .connection_initialized(
                connection,
                ConnectionCapabilities {
                    retention_principal: Some(principal),
                    ..ConnectionCapabilities::default()
                },
            )
            .await;
        manager
            .try_ensure_connection_subscribed(thread_id, connection, false)
            .await
            .expect("connection should subscribe");
        let RetentionAcquireOutcome::Acquired { grant_id } = manager
            .acquire_retention(thread_id, principal)
            .await
            .expect("acquire should succeed")
        else {
            panic!("first acquire must mint a grant");
        };

        manager.remove_connection(connection).await;

        {
            let state = manager.state.lock().await;
            assert!(!state.retention_grants_by_thread.contains_key(&thread_id));
            assert!(
                !state
                    .retention_threads_by_principal
                    .contains_key(&principal)
            );
        }
        assert_eq!(
            manager
                .release_retention(thread_id, principal, &grant_id)
                .await,
            Err(RetentionAuthorityError::IneligiblePrincipal)
        );
    }

    #[tokio::test]
    async fn clear_revokes_the_requesters_predecessor_retention_grant() {
        let manager = ThreadStateManager::new();
        let predecessor_thread_id = ThreadId::new();
        let successor_thread_id = ThreadId::new();
        let requester = ConnectionId(1);
        let principal = RetentionPrincipalId::connection_owned();
        manager
            .connection_initialized(
                requester,
                ConnectionCapabilities {
                    retention_principal: Some(principal),
                    ..ConnectionCapabilities::default()
                },
            )
            .await;
        manager
            .try_ensure_connection_subscribed(predecessor_thread_id, requester, false)
            .await
            .expect("requester should be subscribed to the clear predecessor");
        let RetentionAcquireOutcome::Acquired { grant_id } = manager
            .acquire_retention(predecessor_thread_id, principal)
            .await
            .expect("requester principal should acquire an exact predecessor grant")
        else {
            panic!("first acquire must mint a grant");
        };

        assert!(
            manager
                .move_connection_for_clear(predecessor_thread_id, successor_thread_id, requester)
                .await
        );
        assert_eq!(
            manager
                .release_retention(predecessor_thread_id, principal, &grant_id)
                .await,
            Ok(RetentionReleaseOutcome::NotHeld),
            "clear must revoke A's exact grant"
        );
        assert!(matches!(
            manager
                .acquire_retention(successor_thread_id, principal)
                .await,
            Ok(RetentionAcquireOutcome::Acquired { .. })
        ));
    }

    async fn recv_interactive_subscription_changed_notification(
        outgoing_rx: &mut mpsc::Receiver<OutgoingEnvelope>,
    ) -> ThreadInteractiveSubscriptionChangedNotification {
        let envelope = timeout(Duration::from_secs(1), outgoing_rx.recv())
            .await
            .expect("timed out waiting for subscription notification")
            .expect("outgoing channel closed unexpectedly");
        let OutgoingEnvelope::Broadcast { message } = envelope else {
            panic!("expected broadcast subscription notification");
        };
        let OutgoingMessage::AppServerNotification(envelope) = message else {
            panic!("expected app-server subscription notification");
        };
        let ServerNotification::ThreadInteractiveSubscriptionChanged(notification) =
            envelope.notification
        else {
            panic!("expected kcf/thread/interactiveSubscription/changed notification");
        };
        notification
    }

    fn thread_settings(model: &str) -> ThreadSettings {
        ThreadSettings {
            disabled_plugin_ids: Vec::new(),
            environments: Some(Vec::new()),
            cwd: AbsolutePathBuf::from_absolute_path("/tmp").expect("absolute path"),
            approval_policy: AskForApproval::OnRequest,
            approvals_reviewer: ApprovalsReviewer::User,
            sandbox_policy: SandboxPolicy::ReadOnly {
                network_access: false,
            },
            active_permission_profile: None,
            model: model.to_string(),
            model_provider: "mock_provider".to_string(),
            service_tier: None,
            effort: None,
            summary: None,
            collaboration_mode: CollaborationMode {
                mode: ModeKind::Default,
                settings: Settings {
                    model: model.to_string(),
                    reasoning_effort: None,
                    developer_instructions: None,
                },
            },
            multi_agent_mode: MultiAgentMode::ExplicitRequestOnly,
            personality: None,
        }
    }

    #[tokio::test]
    async fn atomic_connection_retention_is_installed_with_the_subscription() {
        let manager = ThreadStateManager::new();
        let thread_id = ThreadId::new();
        let principal = RetentionPrincipalId::connection_owned();
        let connection = ConnectionId(1);
        manager
            .connection_initialized(
                connection,
                ConnectionCapabilities {
                    retention_principal: Some(principal),
                    ..ConnectionCapabilities::default()
                },
            )
            .await;

        let retained = manager
            .try_add_connection_to_thread_retaining(
                thread_id,
                connection,
                ConnectionRetention::Requested(principal),
            )
            .await
            .expect("live connection subscribes");
        let Some((minted_principal, grant_id)) = retained.minted_grant() else {
            panic!("requested retention must mint a grant: {retained:?}");
        };
        assert_eq!(minted_principal, principal);
        assert_eq!(
            retained.evidence(),
            Some(ThreadRetentionAcquireResponse::Acquired {
                grant_id: grant_id.clone().into_wire(),
            })
        );
        // The lifecycle row is granted before any listener could arm unload.
        assert!(
            manager
                .subscribe_to_retention(thread_id)
                .await
                .expect("lifecycle row")
                .borrow()
                .granted
        );
        assert_eq!(
            manager.acquire_retention(thread_id, principal).await,
            Ok(RetentionAcquireOutcome::AlreadyHeld { grant_id })
        );

        // Without the flag nothing is granted and no evidence is produced.
        let passive = ThreadId::new();
        assert_eq!(
            manager
                .try_add_connection_to_thread_retaining(
                    passive,
                    connection,
                    ConnectionRetention::NotRequested,
                )
                .await
                .expect("live connection subscribes"),
            ConnectionRetentionOutcome::NotRequested
        );
        assert!(
            !manager
                .subscribe_to_retention(passive)
                .await
                .expect("lifecycle row")
                .borrow()
                .granted
        );

        // Failure before publication removes observation state, which revokes
        // the grant; a later incarnation mints a fresh one.
        manager.remove_thread_state(thread_id).await;
        manager.thread_state(thread_id).await;
        assert!(matches!(
            manager.acquire_retention(thread_id, principal).await,
            Ok(RetentionAcquireOutcome::Acquired { .. })
        ));
    }

    #[tokio::test]
    async fn atomic_connection_retention_never_outlives_its_connection() {
        let manager = ThreadStateManager::new();
        let thread_id = ThreadId::new();
        let principal = RetentionPrincipalId::connection_owned();
        let connection = ConnectionId(1);
        let closed_principal = RetentionPrincipalId::connection_owned();
        let closed = ConnectionId(2);
        for (connection, principal) in [(connection, principal), (closed, closed_principal)] {
            manager
                .connection_initialized(
                    connection,
                    ConnectionCapabilities {
                        retention_principal: Some(principal),
                        ..ConnectionCapabilities::default()
                    },
                )
                .await;
        }

        // Closed before the request reaches the lifecycle lock: refused.
        manager.remove_connection(closed).await;
        assert!(matches!(
            manager
                .try_add_connection_to_thread_retaining(
                    thread_id,
                    closed,
                    ConnectionRetention::Requested(closed_principal),
                )
                .await,
            Err(ConnectionSubscriptionError::ConnectionClosed)
        ));

        // Closed after the grant: the ordinary revocation removes it.
        let retained = manager
            .try_add_connection_to_thread_retaining(
                thread_id,
                connection,
                ConnectionRetention::Requested(principal),
            )
            .await
            .expect("live connection subscribes");
        assert!(retained.minted_grant().is_some());
        manager.remove_connection(connection).await;
        {
            let state = manager.state.lock().await;
            assert!(
                state
                    .retention_grants_by_thread
                    .get(&thread_id)
                    .is_none_or(HashMap::is_empty)
            );
            assert!(
                !state
                    .retention_threads_by_principal
                    .contains_key(&principal)
            );
        }
        assert!(
            !manager
                .subscribe_to_retention(thread_id)
                .await
                .expect("lifecycle row")
                .borrow()
                .granted
        );
    }

    #[tokio::test]
    async fn atomic_connection_retention_refuses_ineligible_requests_explicitly() {
        let manager = ThreadStateManager::new();
        let thread_id = ThreadId::new();
        let principal = RetentionPrincipalId::connection_owned();
        let foreign_principal = RetentionPrincipalId::connection_owned();
        let unclassified = RetentionPrincipalId::unclassified();
        for (connection, principal) in [
            (ConnectionId(1), principal),
            (ConnectionId(2), foreign_principal),
            (ConnectionId(3), unclassified),
        ] {
            manager
                .connection_initialized(
                    connection,
                    ConnectionCapabilities {
                        retention_principal: Some(principal),
                        ..ConnectionCapabilities::default()
                    },
                )
                .await;
        }
        let refused = |reason| Some(ThreadRetentionAcquireResponse::Refused { reason });

        // Session-level ineligibility is reported, not silently skipped.
        let session_refused = manager
            .try_add_connection_to_thread_retaining(
                thread_id,
                ConnectionId(1),
                ConnectionRetention::Refused(ThreadRetentionRefusalReason::IneligiblePrincipal),
            )
            .await
            .expect("live connection subscribes");
        assert_eq!(
            session_refused.evidence(),
            refused(ThreadRetentionRefusalReason::IneligiblePrincipal)
        );
        // Another live connection's principal is never accepted.
        let foreign = manager
            .try_add_connection_to_thread_retaining(
                thread_id,
                ConnectionId(1),
                ConnectionRetention::Requested(foreign_principal),
            )
            .await
            .expect("live connection subscribes");
        assert_eq!(foreign.minted_grant(), None);
        assert_eq!(
            foreign.evidence(),
            refused(ThreadRetentionRefusalReason::IneligiblePrincipal)
        );
        // An unclassified principal fails closed exactly as acquisition does.
        let unavailable = manager
            .try_add_connection_to_thread_retaining(
                thread_id,
                ConnectionId(3),
                ConnectionRetention::Requested(unclassified),
            )
            .await
            .expect("live connection subscribes");
        assert_eq!(
            unavailable.evidence(),
            refused(ThreadRetentionRefusalReason::AuthorityUnavailable)
        );
        assert!(
            !manager
                .subscribe_to_retention(thread_id)
                .await
                .expect("lifecycle row")
                .borrow()
                .granted
        );
    }

    #[tokio::test]
    async fn retention_install_after_connection_revoke_leaves_no_grant() {
        let manager = ThreadStateManager::new();
        let thread_id = ThreadId::new();
        let principal = RetentionPrincipalId::connection_owned();
        let connection = ConnectionId(1);
        manager
            .connection_initialized(
                connection,
                ConnectionCapabilities {
                    retention_principal: Some(principal),
                    ..ConnectionCapabilities::default()
                },
            )
            .await;
        // The clear successor exists but is not yet subscribed by the requester.
        manager.thread_state(thread_id).await;
        // Session eligibility already passed when the request was dispatched.
        let retention = ConnectionRetention::Requested(principal);
        // The connection closes and its revocation runs before the install.
        manager.remove_connection(connection).await;

        let outcome = manager
            .grant_connection_retention(thread_id, connection, retention)
            .await;
        assert_eq!(
            outcome,
            ConnectionRetentionOutcome::Granted {
                principal,
                outcome: Err(RetentionAuthorityError::IneligiblePrincipal),
            }
        );
        assert_eq!(outcome.minted_grant(), None);
        assert!(matches!(
            manager
                .try_add_connection_to_thread_retaining(thread_id, connection, retention)
                .await,
            Err(ConnectionSubscriptionError::ConnectionClosed)
        ));
        assert!(matches!(
            manager
                .try_ensure_connection_subscribed_for_listener_retaining(
                    thread_id, connection, /*experimental_raw_events*/ false, retention,
                )
                .await,
            Err(ConnectionSubscriptionError::ConnectionClosed)
        ));
        assert_eq!(
            manager.acquire_retention(thread_id, principal).await,
            Err(RetentionAuthorityError::IneligiblePrincipal)
        );
        let state = manager.state.lock().await;
        assert!(
            state
                .retention_grants_by_thread
                .get(&thread_id)
                .is_none_or(HashMap::is_empty)
        );
        assert!(
            !state
                .retention_threads_by_principal
                .contains_key(&principal)
        );
    }
}

struct ThreadEntry {
    state: Arc<Mutex<ThreadState>>,
    connection_ids: HashSet<ConnectionId>,
    has_connections_watcher: watch::Sender<bool>,
}

impl Default for ThreadEntry {
    fn default() -> Self {
        Self::new(Arc::new(Mutex::new(ThreadState::default())))
    }
}

impl ThreadEntry {
    fn new(state: Arc<Mutex<ThreadState>>) -> Self {
        Self {
            state,
            connection_ids: HashSet::new(),
            has_connections_watcher: watch::channel(false).0,
        }
    }

    fn update_has_connections(&self) {
        let _ = self.has_connections_watcher.send_if_modified(|current| {
            let prev = *current;
            *current = !self.connection_ids.is_empty();
            prev != *current
        });
    }
}

#[derive(Default)]
struct ThreadStateManagerInner {
    retirement_claims_closed: bool,
    lifecycle: HashMap<ThreadId, retirement::RetentionLifecycle>,
    retired_lifecycle: HashMap<(ThreadId, uuid::Uuid), retirement::RetentionLifecycle>,
    live_connections: HashMap<ConnectionId, ConnectionCapabilities>,
    threads: HashMap<ThreadId, ThreadEntry>,
    thread_ids_by_connection: HashMap<ConnectionId, HashSet<ThreadId>>,
    // Retention authority is intentionally distinct from observation. The
    // forward and inverse maps are updated in one mutex domain so a grant can
    // neither survive its principal nor be released by another principal.
    retention_grants_by_thread: HashMap<ThreadId, HashMap<RetentionPrincipalId, RetentionGrantId>>,
    retention_threads_by_principal: HashMap<RetentionPrincipalId, HashSet<ThreadId>>,
    // Keyed by predecessor; each records the exact observation entry the
    // clear reserved, so a stale same-id incarnation is never fenced by it.
    clear_transition_reservations: HashMap<ThreadId, ClearReservation>,
    // B is disclosed to the requester before the durable A -> B subscription
    // move completes. Keep it unavailable to ordinary subscribe/resume until
    // that single atomic move consumes the predecessor subscription.
    clear_successor_reservations: HashMap<ThreadId, ThreadId>,
    interactive_subscription_generation: String,
    interactive_subscription_revision: u64,
    interactive_subscription_counts: HashMap<ThreadId, u32>,
}

/// One held clear reservation: the exact observation entry it fences plus a
/// per-reservation token whose `Arc` identity is the exact-reservation key.
struct ClearReservation {
    identity: Weak<Mutex<ThreadState>>,
    token: Arc<ClearReservationToken>,
}

/// Lock-free state a guard records on its own reservation from `Drop`.
#[derive(Default)]
struct ClearReservationToken {
    /// Admitted clear work ended without a proven join (its future was
    /// dropped mid-clear). Retained for the life of this process and reported
    /// incomplete at shutdown; never paired with `pending_release`.
    unproven: std::sync::atomic::AtomicBool,
    /// Proven pre-work cancellation whose `Drop` could not take the manager
    /// mutex. The next ordinary path that holds it consumes this flag and
    /// releases exactly this reservation (`consume_pending_clear_release`).
    pending_release: std::sync::atomic::AtomicBool,
}

/// What a successful clear reservation hands to its guard.
struct ReservedClear {
    identity: Weak<Mutex<ThreadState>>,
    token: Arc<ClearReservationToken>,
    wake: Option<Arc<tokio::sync::Notify>>,
}

#[derive(Debug)]
pub(crate) enum ConnectionSubscriptionError {
    ConnectionClosed,
    ClearSuccessorReserved,
}

impl ThreadStateManagerInner {
    /// Validation and reservation share this mutex transaction, as does the
    /// snapshot publication that disarms idle unload for the exact entry.
    fn reserve_clear_transition_locked(
        &mut self,
        predecessor_thread_id: ThreadId,
        connection_id: ConnectionId,
    ) -> Result<ReservedClear, ClearTransitionAuthorityError> {
        // A deferred pre-work release of the previous reservation is consumed
        // first, so it never blocks (or leaks into) a new clear.
        self.consume_pending_clear_release(predecessor_thread_id);
        let Some(thread_entry) = self.threads.get(&predecessor_thread_id) else {
            return Err(ClearTransitionAuthorityError::UnknownPredecessor);
        };
        if !thread_entry.connection_ids.contains(&connection_id) {
            return Err(ClearTransitionAuthorityError::NotSubscribed);
        }
        let identity = Arc::downgrade(&thread_entry.state);
        if self
            .clear_transition_reservations
            .contains_key(&predecessor_thread_id)
        {
            return Err(ClearTransitionAuthorityError::TransitionConflict);
        }
        let token = Arc::new(ClearReservationToken::default());
        self.clear_transition_reservations.insert(
            predecessor_thread_id,
            ClearReservation {
                identity: identity.clone(),
                token: Arc::clone(&token),
            },
        );
        self.publish_clear_hold(predecessor_thread_id);
        let wake = self
            .lifecycle
            .get(&predecessor_thread_id)
            .map(retirement::RetentionLifecycle::clear_wake);
        Ok(ReservedClear {
            identity,
            token,
            wake,
        })
    }

    /// Removes the reservation only if it is still the exact one `token`
    /// identifies (same entry and same reservation), then re-arms idle unload.
    fn release_exact_clear_reservation(
        &mut self,
        thread_id: ThreadId,
        identity: &Weak<Mutex<ThreadState>>,
        token: &Arc<ClearReservationToken>,
    ) -> bool {
        if !self
            .clear_transition_reservations
            .get(&thread_id)
            .is_some_and(|held| held.identity.ptr_eq(identity) && Arc::ptr_eq(&held.token, token))
        {
            return false;
        }
        self.clear_transition_reservations.remove(&thread_id);
        self.publish_clear_hold(thread_id);
        true
    }

    /// Consumes a deferred pre-work release, under this mutex. Only the flag
    /// on the reservation currently held for `thread_id` counts: a flag left
    /// on an older reservation's token can never release a newer one.
    fn consume_pending_clear_release(&mut self, thread_id: ThreadId) -> bool {
        if !self
            .clear_transition_reservations
            .get(&thread_id)
            .is_some_and(|held| {
                held.token
                    .pending_release
                    .load(std::sync::atomic::Ordering::Acquire)
            })
        {
            return false;
        }
        self.clear_transition_reservations.remove(&thread_id);
        self.publish_clear_hold(thread_id);
        true
    }

    fn ensure_thread_entry(&mut self, thread_id: ThreadId) -> &mut ThreadEntry {
        match self.threads.entry(thread_id) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let thread = ThreadEntry::default();
                self.lifecycle.entry(thread_id).or_insert_with(|| {
                    retirement::RetentionLifecycle::new(Arc::downgrade(&thread.state))
                });
                entry.insert(thread)
            }
        }
    }

    /// The exact authority checks of `thread/retention/acquire`. Every grant,
    /// explicit or atomic with a subscription, is installed only here.
    fn grant_retention_locked(
        &mut self,
        thread_id: ThreadId,
        principal: RetentionPrincipalId,
    ) -> Result<RetentionAcquireOutcome, RetentionAuthorityError> {
        if !self.retention_principal_is_live(principal) {
            return Err(RetentionAuthorityError::IneligiblePrincipal);
        }
        match principal.owner() {
            RetentionPrincipalOwner::ConnectionOwned => {}
            RetentionPrincipalOwner::Unclassified => {
                return Err(RetentionAuthorityError::AuthorityUnavailable);
            }
            RetentionPrincipalOwner::ThreadOwned(owner) if owner == thread_id => {
                return Err(RetentionAuthorityError::SelfRetention);
            }
            RetentionPrincipalOwner::ThreadOwned(_) => {}
        }
        if !self.threads.contains_key(&thread_id) {
            return Err(RetentionAuthorityError::UnknownThread);
        }

        let record = self
            .lifecycle
            .get(&thread_id)
            .ok_or(RetentionAuthorityError::AuthorityUnavailable)?;
        if record.is_retiring() {
            return Err(RetentionAuthorityError::LifecycleClosed);
        }

        let grants = self
            .retention_grants_by_thread
            .entry(thread_id)
            .or_default();
        if let Some(grant_id) = grants.get(&principal) {
            return Ok(RetentionAcquireOutcome::AlreadyHeld {
                grant_id: grant_id.clone(),
            });
        }

        let grant_id = RetentionGrantId::new();
        grants.insert(principal, grant_id.clone());
        self.retention_threads_by_principal
            .entry(principal)
            .or_default()
            .insert(thread_id);
        self.publish_retention(thread_id);
        record_retention_action(thread_id, principal, RetentionAction::Acquired);
        Ok(RetentionAcquireOutcome::Acquired { grant_id })
    }

    /// Opt-in retention installed in the same critical section that subscribes
    /// `connection_id`. The principal must be that live connection's own
    /// server-minted identity, and the lifecycle row must belong to the exact
    /// observation entry being subscribed: a row kept for another incarnation
    /// (for example a retiring row with a sticky report) is never adopted.
    fn grant_connection_retention_locked(
        &mut self,
        thread_id: ThreadId,
        connection_id: ConnectionId,
        retention: ConnectionRetention,
    ) -> ConnectionRetentionOutcome {
        let principal = match retention {
            ConnectionRetention::NotRequested => return ConnectionRetentionOutcome::NotRequested,
            ConnectionRetention::Refused(reason) => {
                return ConnectionRetentionOutcome::Refused(reason);
            }
            ConnectionRetention::Requested(principal) => principal,
        };
        let outcome = if self
            .live_connections
            .get(&connection_id)
            .and_then(|capabilities| capabilities.retention_principal)
            != Some(principal)
        {
            Err(RetentionAuthorityError::IneligiblePrincipal)
        } else if self
            .threads
            .get(&thread_id)
            .zip(self.lifecycle.get(&thread_id))
            .is_some_and(|(entry, record)| !record.is_identity(&entry.state))
        {
            Err(RetentionAuthorityError::AuthorityUnavailable)
        } else {
            self.grant_retention_locked(thread_id, principal)
        };
        ConnectionRetentionOutcome::Granted { principal, outcome }
    }

    fn retention_principal_is_live(&self, principal: RetentionPrincipalId) -> bool {
        self.live_connections
            .values()
            .any(|capabilities| capabilities.retention_principal == Some(principal))
    }

    fn revoke_retention_grant(
        &mut self,
        thread_id: ThreadId,
        principal: RetentionPrincipalId,
        action: RetentionAction,
    ) -> bool {
        let removed = self
            .retention_grants_by_thread
            .get_mut(&thread_id)
            .and_then(|grants| grants.remove(&principal));
        if self
            .retention_grants_by_thread
            .get(&thread_id)
            .is_some_and(HashMap::is_empty)
        {
            self.retention_grants_by_thread.remove(&thread_id);
        }
        if let Some(thread_ids) = self.retention_threads_by_principal.get_mut(&principal) {
            thread_ids.remove(&thread_id);
            if thread_ids.is_empty() {
                self.retention_threads_by_principal.remove(&principal);
            }
        }
        self.publish_retention(thread_id);
        if removed.is_some() {
            record_retention_action(thread_id, principal, action);
        }
        removed.is_some()
    }

    fn revoke_all_retention_for_principal(&mut self, principal: RetentionPrincipalId) {
        let thread_ids = self
            .retention_threads_by_principal
            .remove(&principal)
            .unwrap_or_default();
        for thread_id in thread_ids {
            if let Some(grants) = self.retention_grants_by_thread.get_mut(&thread_id) {
                let removed = grants.remove(&principal);
                if grants.is_empty() {
                    self.retention_grants_by_thread.remove(&thread_id);
                }
                if removed.is_some() {
                    record_retention_action(
                        thread_id,
                        principal,
                        RetentionAction::ConnectionClosed,
                    );
                }
            }
            self.publish_retention(thread_id);
        }
    }

    fn revoke_all_retention_for_thread(&mut self, thread_id: ThreadId) {
        let Some(grants) = self.retention_grants_by_thread.remove(&thread_id) else {
            return;
        };
        for principal in grants.into_keys() {
            let mut remove_principal_index = false;
            if let Some(thread_ids) = self.retention_threads_by_principal.get_mut(&principal) {
                thread_ids.remove(&thread_id);
                remove_principal_index = thread_ids.is_empty();
            }
            if remove_principal_index {
                self.retention_threads_by_principal.remove(&principal);
            }
            record_retention_action(thread_id, principal, RetentionAction::ThreadRemoved);
        }
        self.publish_retention(thread_id);
    }

    fn add_interactive_subscription(
        &mut self,
        thread_id: ThreadId,
    ) -> ThreadInteractiveSubscriptionEntry {
        let interactive_subscription_count = self
            .interactive_subscription_counts
            .entry(thread_id)
            .or_default();
        *interactive_subscription_count = interactive_subscription_count.saturating_add(1);
        ThreadInteractiveSubscriptionEntry {
            thread_id: thread_id.to_string(),
            interactive_subscription_count: *interactive_subscription_count,
        }
    }

    fn remove_interactive_subscription(
        &mut self,
        thread_id: ThreadId,
    ) -> Option<ThreadInteractiveSubscriptionEntry> {
        let interactive_subscription_count = {
            let interactive_subscription_count =
                self.interactive_subscription_counts.get_mut(&thread_id)?;
            *interactive_subscription_count = interactive_subscription_count.saturating_sub(1);
            *interactive_subscription_count
        };
        let entry = ThreadInteractiveSubscriptionEntry {
            thread_id: thread_id.to_string(),
            interactive_subscription_count,
        };
        if interactive_subscription_count == 0 {
            self.interactive_subscription_counts.remove(&thread_id);
        }
        Some(entry)
    }

    fn interactive_subscription_change(
        &mut self,
        mut changes: Vec<ThreadInteractiveSubscriptionEntry>,
    ) -> Option<ThreadInteractiveSubscriptionChangedNotification> {
        if changes.is_empty() {
            return None;
        }
        changes.sort_unstable_by(|left, right| left.thread_id.cmp(&right.thread_id));
        self.interactive_subscription_revision =
            match self.interactive_subscription_revision.checked_add(1) {
                Some(revision) => revision,
                None => {
                    // Never publish two distinct changes with one revision. A new
                    // generation makes every consumer resnapshot rather than
                    // accepting an ambiguous incremental history.
                    self.interactive_subscription_generation = Uuid::now_v7().to_string();
                    0
                }
            };
        Some(ThreadInteractiveSubscriptionChangedNotification {
            generation: self.interactive_subscription_generation.clone(),
            revision: self.interactive_subscription_revision,
            changes,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClearTransitionAuthorityError {
    UnknownPredecessor,
    NotSubscribed,
    TransitionConflict,
}

/// Why the authoritative A -> B clear subscription move did not happen.
/// Only `ConnectionClosed` is evidence that the requester disconnected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClearSubscriptionMoveRefusal {
    /// The requester is no longer in `live_connections`.
    ConnectionClosed,
    /// The predecessor's observation entry is gone (retired or removed).
    PredecessorUnavailable,
    /// The live requester is no longer subscribed to the predecessor.
    NotSubscribedToPredecessor,
    /// The live requester is already subscribed to the successor.
    AlreadySubscribedToSuccessor,
    /// A trusted interactive requester's predecessor count is missing.
    InteractiveSubscriptionMissing,
}

/// Custody of one exact clear reservation.
///
/// `release` frees it, and the clear calls that only after its own future has
/// run to a return: completion, or a refusal whose awaited work (hook
/// children, DB writes) has itself returned.
///
/// Dropping an armed guard distinguishes two cases by the `begin_work` stage
/// marker, which the clear sets immediately before its first clear work (the
/// durable transition reservation, then hooks and the subscription move):
/// - No work began: nothing can be in flight, so the drop releases the exact
///   reservation inline if the manager mutex is free (its critical sections
///   never await). If it is contended, the drop sets `pending_release` on its
///   own reservation token and wakes the thread's listener; the next ordinary
///   path holding the mutex (that listener's wake, an idle or resume claim,
///   or a new clear reservation) consumes it and releases exactly this
///   reservation, re-arming idle unload. No task is spawned and nothing polls.
/// - Admitted work began: the drop is not proof that hook children or DB
///   writes stopped, so the reservation and the predecessor's idle fence are
///   retained and marked unproven, with no lock, task, or polling. Nothing
///   releases it in-process; host shutdown reports it incomplete without
///   waiting (`ThreadRequestProcessor::shutdown_threads`). A process restart
///   only ends this in-memory reservation; it neither proves nor reconciles
///   the interrupted clear's effects, which keep the existing durable
///   clear/resume behaviour.
pub(crate) struct ClearTransitionAuthorityGuard {
    manager: ThreadStateManager,
    thread_id: ThreadId,
    identity: Weak<Mutex<ThreadState>>,
    token: Arc<ClearReservationToken>,
    wake: Option<Arc<tokio::sync::Notify>>,
    work_started: bool,
    armed: bool,
}

impl ClearTransitionAuthorityGuard {
    /// Stage marker: positively records that clear work is about to begin.
    /// From here on a drop without `release` retains the reservation.
    pub(crate) fn begin_work(&mut self) {
        self.work_started = true;
    }

    /// Called only after the clear's own future has returned. Disarms only
    /// once the manager mutex is held, so a release cancelled while waiting
    /// for it still leaves the reservation retained and marked unproven.
    pub(crate) async fn release(mut self) -> bool {
        let manager = self.manager.clone();
        let mut state = manager.state.lock().await;
        self.armed = false;
        state.release_exact_clear_reservation(self.thread_id, &self.identity, &self.token)
    }
}

impl Drop for ClearTransitionAuthorityGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if !self.work_started {
            // Proven no-work: no hook, DB write or subscription move began.
            if let Ok(mut state) = self.manager.state.try_lock() {
                state.release_exact_clear_reservation(self.thread_id, &self.identity, &self.token);
                return;
            }
            // Contended: defer to the next ordinary path holding the mutex.
            self.token
                .pending_release
                .store(true, std::sync::atomic::Ordering::Release);
            if let Some(wake) = &self.wake {
                wake.notify_one();
            }
            return;
        }
        self.token
            .unproven
            .store(true, std::sync::atomic::Ordering::Release);
        tracing::warn!(
            thread_id = %self.thread_id,
            "admitted clear ended without a proven join; reservation retained as incomplete"
        );
    }
}

/// The manager mutex, held by a test (see `hold_state_lock_for_test`).
#[cfg(test)]
pub(crate) struct StateLockHold {
    guard: tokio::sync::OwnedMutexGuard<ThreadStateManagerInner>,
}

#[cfg(test)]
impl StateLockHold {
    /// `(unproven, pending_release)` of the reservation held for `thread_id`.
    pub(crate) fn clear_flags(&self, thread_id: ThreadId) -> Option<(bool, bool)> {
        self.guard
            .clear_transition_reservations
            .get(&thread_id)
            .map(|held| {
                (
                    held.token
                        .unproven
                        .load(std::sync::atomic::Ordering::Acquire),
                    held.token
                        .pending_release
                        .load(std::sync::atomic::Ordering::Acquire),
                )
            })
    }
}

/// Server-established connection properties. Client-provided initialize data
/// must not set `trusted_interactive` without the later entitlement boundary.
#[derive(Clone, Copy, Default)]
pub(crate) struct ConnectionCapabilities {
    pub(crate) request_attestation: bool,
    pub(crate) trusted_interactive: bool,
    pub(crate) retention_principal: Option<RetentionPrincipalId>,
}

#[derive(Clone)]
pub(crate) struct ThreadStateManager {
    state: Arc<Mutex<ThreadStateManagerInner>>,
    // Extension event sinks are synchronous, so they need an await-free way to
    // enqueue work on the active per-thread listener.
    listener_commands:
        Arc<StdMutex<HashMap<ThreadId, mpsc::UnboundedSender<ThreadListenerCommand>>>>,
    interactive_subscription_notification_outgoing:
        Arc<StdMutex<Option<Arc<OutgoingMessageSender>>>>,
}

impl Default for ThreadStateManager {
    fn default() -> Self {
        Self::new()
    }
}

/// A listener's state, which may not have been published to the manager yet.
pub(crate) enum ListenerStateCandidate {
    Existing(Arc<Mutex<ThreadState>>),
    Unpublished(Arc<Mutex<ThreadState>>),
}

impl ListenerStateCandidate {
    pub(crate) fn state(&self) -> &Arc<Mutex<ThreadState>> {
        match self {
            Self::Existing(state) | Self::Unpublished(state) => state,
        }
    }
}

impl ThreadStateManager {
    /// Coordinates goal edits with cold/path-based resume for just this thread.
    pub(crate) async fn lock_goal_resume(
        &self,
        thread_id: ThreadId,
    ) -> tokio::sync::OwnedMutexGuard<()> {
        loop {
            let thread_state = self.thread_state(thread_id).await;
            let guard = ThreadState::lock_goal_resume(&thread_state).await;
            // Teardown may have replaced the entry while this caller waited for the lock.
            if self
                .state
                .lock()
                .await
                .threads
                .get(&thread_id)
                .is_some_and(|entry| Arc::ptr_eq(&entry.state, &thread_state))
            {
                return guard;
            }
        }
    }

    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(ThreadStateManagerInner {
                interactive_subscription_generation: Uuid::now_v7().to_string(),
                ..ThreadStateManagerInner::default()
            })),
            listener_commands: Arc::new(StdMutex::new(HashMap::new())),
            interactive_subscription_notification_outgoing: Arc::new(StdMutex::new(None)),
        }
    }

    pub(crate) fn set_interactive_subscription_notification_outgoing(
        &self,
        outgoing: Arc<OutgoingMessageSender>,
    ) {
        *self
            .interactive_subscription_notification_outgoing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(outgoing);
    }

    pub(crate) async fn thread_interactive_subscription_list(
        &self,
    ) -> ThreadInteractiveSubscriptionListResponse {
        let state = self.state.lock().await;
        let mut entries = state
            .interactive_subscription_counts
            .iter()
            .map(
                |(thread_id, interactive_subscription_count)| ThreadInteractiveSubscriptionEntry {
                    thread_id: thread_id.to_string(),
                    interactive_subscription_count: *interactive_subscription_count,
                },
            )
            .collect::<Vec<_>>();
        entries.sort_unstable_by(|left, right| left.thread_id.cmp(&right.thread_id));
        ThreadInteractiveSubscriptionListResponse {
            generation: state.interactive_subscription_generation.clone(),
            revision: state.interactive_subscription_revision,
            entries,
        }
    }

    /// Exercises explicit retention authority for one exact loaded thread.
    ///
    /// The principal is accepted only when a live connection supplied the
    /// server-minted identity during initialization. This keeps request
    /// contents, subscription, and client-declared roles out of the authority
    /// decision.
    pub(crate) async fn acquire_retention(
        &self,
        thread_id: ThreadId,
        principal: RetentionPrincipalId,
    ) -> Result<RetentionAcquireOutcome, RetentionAuthorityError> {
        let mut state = self.state.lock().await;
        state.grant_retention_locked(thread_id, principal)
    }

    /// Releases only the caller's exact active grant. A random or stale handle
    /// cannot release a currently active grant, while spent and never-issued
    /// handles are intentionally indistinguishable when no grant remains.
    pub(crate) async fn release_retention(
        &self,
        thread_id: ThreadId,
        principal: RetentionPrincipalId,
        grant_id: &RetentionGrantId,
    ) -> Result<RetentionReleaseOutcome, RetentionAuthorityError> {
        let mut state = self.state.lock().await;
        if !state.retention_principal_is_live(principal) {
            return Err(RetentionAuthorityError::IneligiblePrincipal);
        }
        match principal.owner() {
            RetentionPrincipalOwner::Unclassified => {
                return Err(RetentionAuthorityError::AuthorityUnavailable);
            }
            RetentionPrincipalOwner::ConnectionOwned | RetentionPrincipalOwner::ThreadOwned(_) => {}
        }
        if !state.threads.contains_key(&thread_id) {
            let stale_grant = state
                .retention_grants_by_thread
                .get(&thread_id)
                .is_some_and(|grants| grants.contains_key(&principal))
                || state
                    .retention_threads_by_principal
                    .get(&principal)
                    .is_some_and(|thread_ids| thread_ids.contains(&thread_id));
            return if stale_grant {
                Err(RetentionAuthorityError::AuthorityUnavailable)
            } else {
                Ok(RetentionReleaseOutcome::NotHeld)
            };
        }
        let Some(active_grant_id) = state
            .retention_grants_by_thread
            .get(&thread_id)
            .and_then(|grants| grants.get(&principal))
            .cloned()
        else {
            return Ok(RetentionReleaseOutcome::NotHeld);
        };
        if active_grant_id != *grant_id {
            return Ok(RetentionReleaseOutcome::GrantMismatch);
        }
        state.revoke_retention_grant(thread_id, principal, RetentionAction::Released);
        Ok(RetentionReleaseOutcome::Released)
    }

    async fn publish_interactive_subscription_change(
        &self,
        change: Option<ThreadInteractiveSubscriptionChangedNotification>,
    ) {
        let Some(change) = change else {
            return;
        };
        let outgoing = self
            .interactive_subscription_notification_outgoing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(outgoing) = outgoing {
            outgoing
                .send_server_notification(ServerNotification::ThreadInteractiveSubscriptionChanged(
                    change,
                ))
                .await;
        }
    }

    pub(crate) async fn connection_initialized(
        &self,
        connection_id: ConnectionId,
        capabilities: ConnectionCapabilities,
    ) {
        let change = {
            let mut state = self.state.lock().await;
            let previous = state.live_connections.insert(connection_id, capabilities);
            let Some(previous) = previous else {
                return;
            };
            if previous.trusted_interactive == capabilities.trusted_interactive {
                return;
            }
            let thread_ids = state
                .thread_ids_by_connection
                .get(&connection_id)
                .cloned()
                .unwrap_or_default();
            let changes = thread_ids
                .into_iter()
                .filter_map(|thread_id| {
                    if capabilities.trusted_interactive {
                        Some(state.add_interactive_subscription(thread_id))
                    } else {
                        state.remove_interactive_subscription(thread_id)
                    }
                })
                .collect();
            state.interactive_subscription_change(changes)
        };
        self.publish_interactive_subscription_change(change).await;
    }

    /// Validates client authority and reserves the predecessor for one clear transition.
    ///
    /// Validation and reservation occur under the same mutex so two subscribed clients
    /// cannot both acquire clear authority for the same predecessor.
    #[cfg(test)]
    pub(crate) async fn reserve_clear_transition_authority(
        &self,
        predecessor_thread_id: ThreadId,
        connection_id: ConnectionId,
    ) -> Result<(), ClearTransitionAuthorityError> {
        let mut state = self.state.lock().await;
        state
            .reserve_clear_transition_locked(predecessor_thread_id, connection_id)
            .map(|_| ())
    }

    /// `reserve_clear_transition_authority` that arms its release guard in the
    /// same mutex transaction, so no cancellation point separates the two.
    pub(crate) async fn reserve_clear_transition_authority_guarded(
        &self,
        predecessor_thread_id: ThreadId,
        connection_id: ConnectionId,
    ) -> Result<ClearTransitionAuthorityGuard, ClearTransitionAuthorityError> {
        let mut state = self.state.lock().await;
        let ReservedClear {
            identity,
            token,
            wake,
        } = state.reserve_clear_transition_locked(predecessor_thread_id, connection_id)?;
        Ok(ClearTransitionAuthorityGuard {
            manager: self.clone(),
            thread_id: predecessor_thread_id,
            identity,
            token,
            wake,
            work_started: false,
            armed: true,
        })
    }

    /// The per-thread wake a pre-work clear cancellation signals when its
    /// `Drop` could not take the mutex. The listener consumes on wake.
    pub(crate) async fn clear_release_wake(
        &self,
        thread_id: ThreadId,
    ) -> Option<Arc<tokio::sync::Notify>> {
        self.state
            .lock()
            .await
            .lifecycle
            .get(&thread_id)
            .map(retirement::RetentionLifecycle::clear_wake)
    }

    /// Consumes a deferred pre-work release for the exact held reservation.
    pub(crate) async fn consume_pending_clear_release(&self, thread_id: ThreadId) -> bool {
        self.state
            .lock()
            .await
            .consume_pending_clear_release(thread_id)
    }

    /// Holds the manager mutex so a test can make `Drop` contend for it.
    #[cfg(test)]
    pub(crate) async fn hold_state_lock_for_test(&self) -> StateLockHold {
        StateLockHold {
            guard: Arc::clone(&self.state).lock_owned().await,
        }
    }

    /// Clear reservations still held, as `(predecessor, unproven)`, for
    /// incomplete reporting only. Shutdown never waits on them; the bounded
    /// manager shutdown proceeds under its own owner deadline.
    pub(crate) async fn held_clear_transition_reservations(&self) -> Vec<(ThreadId, bool)> {
        self.state
            .lock()
            .await
            .clear_transition_reservations
            .iter()
            .map(|(thread_id, held)| {
                (
                    *thread_id,
                    held.token
                        .unproven
                        .load(std::sync::atomic::Ordering::Acquire),
                )
            })
            .collect()
    }

    #[cfg(test)]
    pub(crate) async fn release_clear_transition_authority(
        &self,
        predecessor_thread_id: ThreadId,
    ) -> bool {
        let mut state = self.state.lock().await;
        if state
            .clear_transition_reservations
            .remove(&predecessor_thread_id)
            .is_none()
        {
            return false;
        }
        state.publish_clear_hold(predecessor_thread_id);
        true
    }

    /// Releases B only after the durable clear reaches its terminal
    /// disposition. This is deliberately separate from the request-scoped A
    /// authority release: an error after B is disclosed must remain
    /// fail-closed until reconciliation can account for the durable record.
    pub(crate) async fn terminalize_clear_successor_subscription_reservation(
        &self,
        predecessor_thread_id: ThreadId,
        successor_thread_id: ThreadId,
    ) -> bool {
        let mut state = self.state.lock().await;
        matches!(
            state.clear_successor_reservations.get(&successor_thread_id),
            Some(predecessor) if *predecessor == predecessor_thread_id
        ) && state
            .clear_successor_reservations
            .remove(&successor_thread_id)
            .is_some()
    }

    /// Prevents an ordinary resume from attaching the disclosed clear
    /// successor before the authoritative A -> B move consumes A.
    pub(crate) async fn reserve_clear_successor_subscription(
        &self,
        predecessor_thread_id: ThreadId,
        successor_thread_id: ThreadId,
    ) -> bool {
        let mut state = self.state.lock().await;
        if !state
            .clear_transition_reservations
            .contains_key(&predecessor_thread_id)
            || state
                .clear_successor_reservations
                .contains_key(&successor_thread_id)
        {
            return false;
        }
        state
            .clear_successor_reservations
            .insert(successor_thread_id, predecessor_thread_id);
        true
    }

    pub(crate) async fn first_attestation_capable_connection_for_thread(
        &self,
        thread_id: ThreadId,
    ) -> Option<ConnectionId> {
        let state = self.state.lock().await;
        state
            .threads
            .get(&thread_id)?
            .connection_ids
            .iter()
            .filter_map(|connection_id| {
                state
                    .live_connections
                    .get(connection_id)?
                    .request_attestation
                    .then_some(*connection_id)
            })
            .min_by_key(|connection_id| connection_id.0)
    }

    pub(crate) async fn wait_for_thread_subscriber(&self, thread_id: ThreadId) {
        let mut has_connections = {
            let mut state = self.state.lock().await;
            state
                .ensure_thread_entry(thread_id)
                .has_connections_watcher
                .subscribe()
        };
        while !*has_connections.borrow_and_update() {
            if has_connections.changed().await.is_err() {
                break;
            }
        }
    }

    pub(crate) async fn subscribed_connection_ids(&self, thread_id: ThreadId) -> Vec<ConnectionId> {
        let state = self.state.lock().await;
        state
            .threads
            .get(&thread_id)
            .map(|thread_entry| thread_entry.connection_ids.iter().copied().collect())
            .unwrap_or_default()
    }

    pub(crate) async fn thread_state(&self, thread_id: ThreadId) -> Arc<Mutex<ThreadState>> {
        let mut state = self.state.lock().await;
        state.ensure_thread_entry(thread_id).state.clone()
    }

    pub(crate) async fn created_listener_state(
        &self,
        thread_id: ThreadId,
    ) -> ListenerStateCandidate {
        match self.state.lock().await.threads.get(&thread_id) {
            Some(entry) => ListenerStateCandidate::Existing(Arc::clone(&entry.state)),
            None => {
                ListenerStateCandidate::Unpublished(Arc::new(Mutex::new(ThreadState::default())))
            }
        }
    }

    pub(crate) fn current_listener_command_tx(
        &self,
        thread_id: ThreadId,
    ) -> Option<mpsc::UnboundedSender<ThreadListenerCommand>> {
        self.listener_commands
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&thread_id)
            .cloned()
    }

    pub(crate) fn register_listener_command_tx(
        &self,
        thread_id: ThreadId,
        tx: mpsc::UnboundedSender<ThreadListenerCommand>,
    ) {
        self.listener_commands
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(thread_id, tx);
    }

    pub(crate) fn unregister_listener_command_tx(&self, thread_id: ThreadId) {
        self.listener_commands
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&thread_id);
    }

    pub(crate) async fn remove_thread_state(&self, thread_id: ThreadId) {
        let (thread_state, interactive_subscription_change) = {
            let mut state = self.state.lock().await;
            self.unregister_listener_command_tx(thread_id);
            let thread_state = state
                .threads
                .remove(&thread_id)
                .map(|thread_entry| thread_entry.state);
            state.revoke_all_retention_for_thread(thread_id);
            if state
                .lifecycle
                .get(&thread_id)
                .is_some_and(|record| !record.is_retiring() || record.has_complete_report())
            {
                state.lifecycle.remove(&thread_id);
            }
            state.thread_ids_by_connection.retain(|_, thread_ids| {
                thread_ids.remove(&thread_id);
                !thread_ids.is_empty()
            });
            let changes = state
                .interactive_subscription_counts
                .remove(&thread_id)
                .map(|_| ThreadInteractiveSubscriptionEntry {
                    thread_id: thread_id.to_string(),
                    interactive_subscription_count: 0,
                })
                .into_iter()
                .collect();
            (thread_state, state.interactive_subscription_change(changes))
        };
        self.publish_interactive_subscription_change(interactive_subscription_change)
            .await;

        if let Some(thread_state) = thread_state {
            let mut thread_state = thread_state.lock().await;
            tracing::debug!(
                thread_id = %thread_id,
                listener_generation = thread_state.listener_generation,
                had_listener = thread_state.cancel_tx.is_some(),
                had_active_turn = thread_state.active_turn_id().is_some(),
                "clearing thread listener during thread-state teardown"
            );
            thread_state.clear_listener();
        }
    }

    /// Removes an unsubscribed listener's entry without clearing a replacement's state.
    #[expect(
        clippy::await_holding_invalid_type,
        reason = "the listener generation must stay fixed while removing its map entry"
    )]
    pub(crate) async fn remove_unsubscribed_listener(
        &self,
        thread_id: ThreadId,
        expected: &Arc<Mutex<ThreadState>>,
        generation: u64,
    ) -> bool {
        let mut thread_state = expected.lock().await;
        let mut state = self.state.lock().await;
        if thread_state.listener_generation != generation
            || !state.threads.get(&thread_id).is_some_and(|entry| {
                Arc::ptr_eq(&entry.state, expected) && entry.connection_ids.is_empty()
            })
        {
            return false;
        }
        state.threads.remove(&thread_id);
        self.unregister_listener_command_tx(thread_id);
        thread_state.clear_listener();
        true
    }

    pub(crate) async fn clear_all_listeners(&self) {
        let thread_states = {
            let state = self.state.lock().await;
            state
                .threads
                .iter()
                .map(|(thread_id, thread_entry)| (*thread_id, thread_entry.state.clone()))
                .collect::<Vec<_>>()
        };

        for (thread_id, thread_state) in thread_states {
            self.unregister_listener_command_tx(thread_id);
            let mut thread_state = thread_state.lock().await;
            tracing::debug!(
                thread_id = %thread_id,
                listener_generation = thread_state.listener_generation,
                had_listener = thread_state.cancel_tx.is_some(),
                had_active_turn = thread_state.active_turn_id().is_some(),
                "clearing thread listener during app-server shutdown"
            );
            thread_state.clear_listener();
        }
    }

    pub(crate) async fn unsubscribe_connection_from_thread(
        &self,
        thread_id: ThreadId,
        connection_id: ConnectionId,
    ) -> bool {
        let change = {
            let mut state = self.state.lock().await;
            if !state
                .thread_ids_by_connection
                .get(&connection_id)
                .is_some_and(|thread_ids| thread_ids.contains(&thread_id))
            {
                return false;
            }

            let trusted_interactive = state
                .live_connections
                .get(&connection_id)
                .is_some_and(|capabilities| capabilities.trusted_interactive);

            if let Some(thread_ids) = state.thread_ids_by_connection.get_mut(&connection_id) {
                thread_ids.remove(&thread_id);
                if thread_ids.is_empty() {
                    state.thread_ids_by_connection.remove(&connection_id);
                }
            }
            if let Some(thread_entry) = state.threads.get_mut(&thread_id) {
                thread_entry.connection_ids.remove(&connection_id);
                thread_entry.update_has_connections();
            }
            if let Some(principal) = state
                .live_connections
                .get(&connection_id)
                .and_then(|capabilities| capabilities.retention_principal)
            {
                state.revoke_retention_grant(thread_id, principal, RetentionAction::Unsubscribed);
            }
            let changes = trusted_interactive
                .then(|| state.remove_interactive_subscription(thread_id))
                .flatten()
                .into_iter()
                .collect();
            state.interactive_subscription_change(changes)
        };
        self.publish_interactive_subscription_change(change).await;
        true
    }

    /// Moves one live connection from an authoritative clear predecessor to
    /// its successor with one subscription revision. The caller has already
    /// established the durable A/B/T transition and listener readiness.
    #[cfg(test)]
    pub(crate) async fn move_connection_for_clear(
        &self,
        predecessor_thread_id: ThreadId,
        successor_thread_id: ThreadId,
        connection_id: ConnectionId,
    ) -> bool {
        self.try_move_connection_for_clear(
            predecessor_thread_id,
            successor_thread_id,
            connection_id,
        )
        .await
        .is_ok()
    }

    /// `move_connection_for_clear` with the exact refusal cause, classified
    /// under the same mutex as the move so liveness is evidence, not inference.
    pub(crate) async fn try_move_connection_for_clear(
        &self,
        predecessor_thread_id: ThreadId,
        successor_thread_id: ThreadId,
        connection_id: ConnectionId,
    ) -> Result<(), ClearSubscriptionMoveRefusal> {
        let change = {
            let mut state = self.state.lock().await;
            if !state.live_connections.contains_key(&connection_id) {
                return Err(ClearSubscriptionMoveRefusal::ConnectionClosed);
            }
            if !state.threads.contains_key(&predecessor_thread_id) {
                return Err(ClearSubscriptionMoveRefusal::PredecessorUnavailable);
            }
            if !state
                .thread_ids_by_connection
                .get(&connection_id)
                .is_some_and(|thread_ids| thread_ids.contains(&predecessor_thread_id))
            {
                return Err(ClearSubscriptionMoveRefusal::NotSubscribedToPredecessor);
            }
            if state
                .thread_ids_by_connection
                .get(&connection_id)
                .is_some_and(|thread_ids| thread_ids.contains(&successor_thread_id))
            {
                return Err(ClearSubscriptionMoveRefusal::AlreadySubscribedToSuccessor);
            }

            let trusted_interactive = state
                .live_connections
                .get(&connection_id)
                .is_some_and(|capabilities| capabilities.trusted_interactive);
            let retention_principal = state
                .live_connections
                .get(&connection_id)
                .and_then(|capabilities| capabilities.retention_principal);
            if trusted_interactive
                && !state
                    .interactive_subscription_counts
                    .contains_key(&predecessor_thread_id)
            {
                return Err(ClearSubscriptionMoveRefusal::InteractiveSubscriptionMissing);
            }

            let Some(thread_ids) = state.thread_ids_by_connection.get_mut(&connection_id) else {
                return Err(ClearSubscriptionMoveRefusal::NotSubscribedToPredecessor);
            };
            thread_ids.remove(&predecessor_thread_id);
            thread_ids.insert(successor_thread_id);

            if let Some(predecessor) = state.threads.get_mut(&predecessor_thread_id) {
                predecessor.connection_ids.remove(&connection_id);
                predecessor.update_has_connections();
            } else {
                return Err(ClearSubscriptionMoveRefusal::PredecessorUnavailable);
            }
            let successor = state.ensure_thread_entry(successor_thread_id);
            successor.connection_ids.insert(connection_id);
            successor.update_has_connections();

            // The authoritative A -> B clear transition moves observation,
            // not retention. Revoke only after every move guard has passed,
            // in this same mutex transaction; the successor never inherits
            // the predecessor's exact grant.
            if let Some(principal) = retention_principal {
                state.revoke_retention_grant(
                    predecessor_thread_id,
                    principal,
                    RetentionAction::Cleared,
                );
            }

            let changes = if trusted_interactive {
                let Some(predecessor_change) =
                    state.remove_interactive_subscription(predecessor_thread_id)
                else {
                    return Err(ClearSubscriptionMoveRefusal::InteractiveSubscriptionMissing);
                };
                let successor_change = state.add_interactive_subscription(successor_thread_id);
                vec![successor_change, predecessor_change]
            } else {
                Vec::new()
            };
            state.interactive_subscription_change(changes)
        };
        self.publish_interactive_subscription_change(change).await;
        Ok(())
    }

    pub(crate) async fn has_subscribers(&self, thread_id: ThreadId) -> bool {
        self.state
            .lock()
            .await
            .threads
            .get(&thread_id)
            .is_some_and(|thread_entry| !thread_entry.connection_ids.is_empty())
    }

    /// Publish only after listener installation succeeds; preserve upstream's
    /// unpublished candidate identity while updating retention/subscription
    /// authority in the same manager transaction.
    pub(crate) async fn subscribe_and_install_listener<T, E>(
        &self,
        thread_id: ThreadId,
        candidate: &ListenerStateCandidate,
        connection_ids: &[ConnectionId],
        install: impl FnOnce(bool) -> Result<T, E>,
    ) -> Result<Option<(T, watch::Receiver<bool>, bool)>, E> {
        self.subscribe_and_install_listener_retaining(
            thread_id,
            candidate,
            connection_ids,
            None,
            install,
        )
        .await
        .map(|value| value.map(|(installed, watcher, attached, _)| (installed, watcher, attached)))
    }

    pub(crate) async fn subscribe_and_install_listener_retaining<T, E>(
        &self,
        thread_id: ThreadId,
        candidate: &ListenerStateCandidate,
        connection_ids: &[ConnectionId],
        retention: Option<(ConnectionId, ConnectionRetention)>,
        install: impl FnOnce(bool) -> Result<T, E>,
    ) -> Result<Option<(T, watch::Receiver<bool>, bool, ConnectionRetentionOutcome)>, E> {
        let (installed, watcher, attached, change, retained) = {
            let mut state = self.state.lock().await;
            if state.clear_successor_reservations.contains_key(&thread_id)
                || state
                    .lifecycle
                    .get(&thread_id)
                    .is_some_and(|record| record.is_retiring())
            {
                return Ok(None);
            }
            match (state.threads.get(&thread_id), candidate) {
                (Some(entry), _) if Arc::ptr_eq(&entry.state, candidate.state()) => {}
                (None, ListenerStateCandidate::Unpublished(_)) => {}
                _ => return Ok(None),
            }
            let attached = connection_ids
                .iter()
                .any(|id| state.live_connections.contains_key(id));
            let installed = install(attached)?;
            state
                .threads
                .entry(thread_id)
                .or_insert_with(|| ThreadEntry::new(Arc::clone(candidate.state())));
            state.lifecycle.entry(thread_id).or_insert_with(|| {
                retirement::RetentionLifecycle::new(Arc::downgrade(candidate.state()))
            });
            let mut changes = Vec::new();
            for &id in connection_ids {
                if !state.live_connections.contains_key(&id) {
                    continue;
                }
                let added = state
                    .thread_ids_by_connection
                    .entry(id)
                    .or_default()
                    .insert(thread_id);
                state
                    .threads
                    .get_mut(&thread_id)
                    .expect("installed entry")
                    .connection_ids
                    .insert(id);
                if added
                    && state
                        .live_connections
                        .get(&id)
                        .is_some_and(|caps| caps.trusted_interactive)
                {
                    changes.push(state.add_interactive_subscription(thread_id));
                }
            }
            let entry = state.threads.get(&thread_id).expect("installed entry");
            entry.update_has_connections();
            let watcher = entry.has_connections_watcher.subscribe();
            let retained = match retention {
                Some((id, request)) if connection_ids.contains(&id) => {
                    state.grant_connection_retention_locked(thread_id, id, request)
                }
                Some(_) => ConnectionRetentionOutcome::Refused(
                    ThreadRetentionRefusalReason::IneligiblePrincipal,
                ),
                None => ConnectionRetentionOutcome::NotRequested,
            };
            let change = state.interactive_subscription_change(changes);
            (installed, watcher, attached, change, retained)
        };
        self.publish_interactive_subscription_change(change).await;
        Ok(Some((installed, watcher, attached, retained)))
    }

    #[allow(dead_code)] // Used by in-crate state tests; live requests use the guarded variant.
    pub(crate) async fn try_ensure_connection_subscribed(
        &self,
        thread_id: ThreadId,
        connection_id: ConnectionId,
        experimental_raw_events: bool,
    ) -> Option<Arc<Mutex<ThreadState>>> {
        self.try_ensure_connection_subscribed_inner(
            thread_id,
            connection_id,
            experimental_raw_events,
            ConnectionRetention::NotRequested,
        )
        .await
        .ok()
        .map(|(thread_state, _)| thread_state)
    }

    /// Production listeners subscribe through the `_retaining` variant.
    #[cfg(test)]
    pub(crate) async fn try_ensure_connection_subscribed_for_listener(
        &self,
        thread_id: ThreadId,
        connection_id: ConnectionId,
        experimental_raw_events: bool,
    ) -> Result<Arc<Mutex<ThreadState>>, ConnectionSubscriptionError> {
        self.try_ensure_connection_subscribed_for_listener_retaining(
            thread_id,
            connection_id,
            experimental_raw_events,
            ConnectionRetention::NotRequested,
        )
        .await
        .map(|(thread_state, _)| thread_state)
    }

    /// Subscribes for a listener attach, installing
    /// requested retention in the subscription's critical section, before
    /// the caller can attach a listener that arms idle retirement.
    pub(crate) async fn try_ensure_connection_subscribed_for_listener_retaining(
        &self,
        thread_id: ThreadId,
        connection_id: ConnectionId,
        experimental_raw_events: bool,
        retention: ConnectionRetention,
    ) -> Result<(Arc<Mutex<ThreadState>>, ConnectionRetentionOutcome), ConnectionSubscriptionError>
    {
        self.try_ensure_connection_subscribed_inner(
            thread_id,
            connection_id,
            experimental_raw_events,
            retention,
        )
        .await
    }

    async fn try_ensure_connection_subscribed_inner(
        &self,
        thread_id: ThreadId,
        connection_id: ConnectionId,
        experimental_raw_events: bool,
        retention: ConnectionRetention,
    ) -> Result<(Arc<Mutex<ThreadState>>, ConnectionRetentionOutcome), ConnectionSubscriptionError>
    {
        let (thread_state, interactive_subscription_change, retained) = {
            let mut state = self.state.lock().await;
            // A reserved clear successor is owned by its clear transition even
            // when the requesting connection already closed: no listener attach.
            if state.clear_successor_reservations.contains_key(&thread_id) {
                return Err(ConnectionSubscriptionError::ClearSuccessorReserved);
            }
            if !state.live_connections.contains_key(&connection_id) {
                return Err(ConnectionSubscriptionError::ConnectionClosed);
            }
            let was_added = state
                .thread_ids_by_connection
                .entry(connection_id)
                .or_default()
                .insert(thread_id);
            let thread_state = {
                let thread_entry = state.ensure_thread_entry(thread_id);
                thread_entry.connection_ids.insert(connection_id);
                thread_entry.update_has_connections();
                thread_entry.state.clone()
            };
            let retained =
                state.grant_connection_retention_locked(thread_id, connection_id, retention);
            let changes = state
                .live_connections
                .get(&connection_id)
                .is_some_and(|capabilities| capabilities.trusted_interactive)
                .then(|| was_added.then(|| state.add_interactive_subscription(thread_id)))
                .flatten()
                .into_iter()
                .collect();
            (
                thread_state,
                state.interactive_subscription_change(changes),
                retained,
            )
        };
        self.publish_interactive_subscription_change(interactive_subscription_change)
            .await;
        {
            let mut thread_state_guard = thread_state.lock().await;
            if experimental_raw_events {
                thread_state_guard.set_experimental_raw_events(/*enabled*/ true);
            }
        }
        Ok((thread_state, retained))
    }

    /// Production subscriptions go through the `_retaining` variant.
    #[cfg(test)]
    pub(crate) async fn try_add_connection_to_thread(
        &self,
        thread_id: ThreadId,
        connection_id: ConnectionId,
    ) -> Result<(), ConnectionSubscriptionError> {
        self.try_add_connection_to_thread_retaining(
            thread_id,
            connection_id,
            ConnectionRetention::NotRequested,
        )
        .await
        .map(|_| ())
    }

    /// Installs requested retention for `connection_id` on an existing,
    /// not-yet-subscribed thread (the clear successor, before its listener
    /// starts). The connection must still be live; its later close revokes
    /// the grant through `remove_connection`.
    pub(crate) async fn grant_connection_retention(
        &self,
        thread_id: ThreadId,
        connection_id: ConnectionId,
        retention: ConnectionRetention,
    ) -> ConnectionRetentionOutcome {
        let mut state = self.state.lock().await;
        state.grant_connection_retention_locked(thread_id, connection_id, retention)
    }

    /// Subscribes `connection_id` and, when requested, installs its retention
    /// grant in the same manager critical section. A connection that already
    /// closed is refused before any grant; one that closes afterwards loses
    /// the grant through `remove_connection`'s ordinary revocation.
    pub(crate) async fn try_add_connection_to_thread_retaining(
        &self,
        thread_id: ThreadId,
        connection_id: ConnectionId,
        retention: ConnectionRetention,
    ) -> Result<ConnectionRetentionOutcome, ConnectionSubscriptionError> {
        let (change, retained) = {
            let mut state = self.state.lock().await;
            if !state.live_connections.contains_key(&connection_id) {
                return Err(ConnectionSubscriptionError::ConnectionClosed);
            }
            if state.clear_successor_reservations.contains_key(&thread_id) {
                return Err(ConnectionSubscriptionError::ClearSuccessorReserved);
            }
            let was_added = state
                .thread_ids_by_connection
                .entry(connection_id)
                .or_default()
                .insert(thread_id);
            {
                let thread_entry = state.ensure_thread_entry(thread_id);
                thread_entry.connection_ids.insert(connection_id);
                thread_entry.update_has_connections();
            }
            let retained =
                state.grant_connection_retention_locked(thread_id, connection_id, retention);
            let changes = state
                .live_connections
                .get(&connection_id)
                .is_some_and(|capabilities| capabilities.trusted_interactive)
                .then(|| was_added.then(|| state.add_interactive_subscription(thread_id)))
                .flatten()
                .into_iter()
                .collect();
            (state.interactive_subscription_change(changes), retained)
        };
        self.publish_interactive_subscription_change(change).await;
        Ok(retained)
    }

    pub(crate) async fn remove_connection(&self, connection_id: ConnectionId) -> Vec<ThreadId> {
        let (thread_ids, interactive_subscription_change) = {
            let mut state = self.state.lock().await;
            let removed_capabilities = state.live_connections.remove(&connection_id);
            let trusted_interactive = removed_capabilities
                .as_ref()
                .is_some_and(|capabilities| capabilities.trusted_interactive);
            if let Some(principal) =
                removed_capabilities.and_then(|capabilities| capabilities.retention_principal)
            {
                state.revoke_all_retention_for_principal(principal);
            }
            let thread_ids = state
                .thread_ids_by_connection
                .remove(&connection_id)
                .unwrap_or_default();
            for thread_id in &thread_ids {
                if let Some(thread_entry) = state.threads.get_mut(thread_id) {
                    thread_entry.connection_ids.remove(&connection_id);
                    thread_entry.update_has_connections();
                }
            }
            let changes = if trusted_interactive {
                {
                    thread_ids
                        .iter()
                        .filter_map(|thread_id| state.remove_interactive_subscription(*thread_id))
                        .collect()
                }
            } else {
                Default::default()
            };
            let interactive_subscription_change = state.interactive_subscription_change(changes);
            let threads_to_unload = thread_ids
                .into_iter()
                .filter(|thread_id| {
                    state
                        .threads
                        .get(thread_id)
                        .is_some_and(|thread_entry| thread_entry.connection_ids.is_empty())
                })
                .collect::<Vec<_>>();
            (threads_to_unload, interactive_subscription_change)
        };
        self.publish_interactive_subscription_change(interactive_subscription_change)
            .await;
        thread_ids
    }

    #[cfg(test)]
    pub(crate) async fn subscribe_to_has_connections(
        &self,
        thread_id: ThreadId,
    ) -> Option<watch::Receiver<bool>> {
        let state = self.state.lock().await;
        state
            .threads
            .get(&thread_id)
            .map(|thread_entry| thread_entry.has_connections_watcher.subscribe())
    }
}
