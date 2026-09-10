use crate::outgoing_message::ConnectionId;
use crate::outgoing_message::ConnectionRequestId;
use crate::outgoing_message::OutgoingMessageSender;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ThreadAttachmentChangedNotification;
use codex_app_server_protocol::ThreadAttachmentEntry;
use codex_app_server_protocol::ThreadAttachmentListResponse;
use codex_app_server_protocol::ThreadGoal;
use codex_app_server_protocol::ThreadHistoryBuilder;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadSettings;
use codex_app_server_protocol::Turn;
use codex_app_server_protocol::TurnError;
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
use codex_rollout::RolloutItem;
use codex_rollout::state_db::StateDbHandle;
use codex_utils_path_uri::LegacyAppPathString;
use std::collections::HashMap;
use std::collections::HashSet;
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

mod retirement;
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
}

// ThreadListenerCommand is used to perform operations in the context of the thread listener, for serialization purposes.
pub(crate) enum ThreadListenerCommand {
    // SendThreadResumeResponse is used to resume an already running thread by sending the thread's history to the client and atomically subscribing for new updates.
    SendThreadResumeResponse(Box<PendingThreadResumeRequest>),
    // EmitThreadGoalUpdated is used to order goal updates with running-thread resume responses and goal clears.
    EmitThreadGoalUpdated {
        turn_id: Option<String>,
        goal: ThreadGoal,
    },
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

/// Per-conversation accumulation of the latest states e.g. error message while a turn runs.
#[derive(Default, Clone)]
pub(crate) struct TurnSummary {
    pub(crate) started_at: Option<i64>,
    pub(crate) command_execution_started: HashSet<String>,
    pub(crate) last_error: Option<TurnError>,
    pub(crate) last_agent_message: Option<ThreadItem>,
}

#[derive(Default)]
pub(crate) struct ThreadState {
    pub(crate) pending_interrupts: PendingInterruptQueue,
    pub(crate) pending_rollbacks: Option<ConnectionRequestId>,
    pub(crate) turn_summary: TurnSummary,
    pub(crate) last_terminal_turn_id: Option<String>,
    pub(crate) cancel_tx: Option<oneshot::Sender<()>>,
    pub(crate) experimental_raw_events: bool,
    pub(crate) listener_generation: u64,
    last_thread_settings: Option<ThreadSettings>,
    listener_command_tx: Option<mpsc::UnboundedSender<ThreadListenerCommand>>,
    current_turn_history: ThreadHistoryBuilder,
    listener_thread: Option<Weak<CodexThread>>,
    watch_registration: WatchRegistration,
}

impl ThreadState {
    pub(crate) fn listener_matches(&self, conversation: &Arc<CodexThread>) -> bool {
        self.listener_thread
            .as_ref()
            .and_then(Weak::upgrade)
            .is_some_and(|existing| Arc::ptr_eq(&existing, conversation))
    }

    pub(crate) fn set_listener(
        &mut self,
        cancel_tx: oneshot::Sender<()>,
        conversation: &Arc<CodexThread>,
        watch_registration: WatchRegistration,
        thread_settings_baseline: ThreadSettings,
    ) -> (
        mpsc::UnboundedReceiver<ThreadListenerCommand>,
        u64,
        Option<oneshot::Sender<()>>,
    ) {
        let previous = self.cancel_tx.replace(cancel_tx);
        self.listener_generation = self.listener_generation.wrapping_add(1);
        self.last_thread_settings = Some(thread_settings_baseline);
        let (listener_command_tx, listener_command_rx) = mpsc::unbounded_channel();
        self.listener_command_tx = Some(listener_command_tx);
        self.listener_thread = Some(Arc::downgrade(conversation));
        self.watch_registration = watch_registration;
        (listener_command_rx, self.listener_generation, previous)
    }

    pub(crate) fn clear_listener(&mut self) {
        if let Some(cancel_tx) = self.cancel_tx.take() {
            let _ = cancel_tx.send(());
        }
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

    pub(crate) fn track_current_turn_event(&mut self, event_turn_id: &str, event: &EventMsg) {
        if let EventMsg::TurnStarted(payload) = event {
            self.turn_summary.started_at = payload.started_at;
        }
        if let EventMsg::ItemCompleted(payload) = event
            && let CoreTurnItem::AgentMessage(item) = &payload.item
            && matches!(item.phase, Some(MessagePhase::FinalAnswer) | None)
            && item.content.iter().any(|content| {
                matches!(content, CoreAgentMessageContent::Text { text } if !text.trim().is_empty())
            })
        {
            self.turn_summary.last_agent_message =
                Some(ThreadItem::from(CoreTurnItem::AgentMessage(item.clone())));
        }
        self.current_turn_history.handle_event(event);
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
    async fn attachment_snapshot_tracks_only_trusted_interactive_subscriptions() {
        let manager = ThreadStateManager::new();
        let thread_id = ThreadId::new();
        let trusted_connection = ConnectionId(1);
        let untrusted_connection = ConnectionId(2);

        let startup_snapshot = manager.thread_attachment_list().await;
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
        let untrusted_snapshot = manager.thread_attachment_list().await;
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
        let attached_snapshot = manager.thread_attachment_list().await;
        assert_eq!(attached_snapshot.generation, startup_snapshot.generation);
        assert_eq!(attached_snapshot.revision, 1);
        assert_eq!(
            attached_snapshot.entries,
            vec![ThreadAttachmentEntry {
                thread_id: thread_id.to_string(),
                interactive_attachment_count: 1,
            }]
        );

        manager.remove_connection(trusted_connection).await;
        let closed_snapshot = manager.thread_attachment_list().await;
        assert_eq!(closed_snapshot.revision, 2);
        assert!(closed_snapshot.entries.is_empty());
    }

    #[tokio::test]
    async fn reconnect_does_not_retain_interactive_attachment_without_fresh_entitlement() {
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
        assert_eq!(manager.thread_attachment_list().await.entries.len(), 1);

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

        let snapshot = manager.thread_attachment_list().await;
        assert!(snapshot.entries.is_empty());
        assert_eq!(snapshot.revision, 2);
    }

    #[tokio::test]
    async fn attachment_notifications_are_revisioned_and_ignore_duplicate_subscriptions() {
        let manager = ThreadStateManager::new();
        let thread_id = ThreadId::new();
        let connection_id = ConnectionId(1);
        let (outgoing_tx, mut outgoing_rx) = mpsc::channel(8);
        manager.set_attachment_notification_outgoing(Arc::new(OutgoingMessageSender::new(
            outgoing_tx,
            AnalyticsEventsClient::disabled(),
        )));

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
        let generation = manager.thread_attachment_list().await.generation;

        manager
            .try_ensure_connection_subscribed(
                thread_id,
                connection_id,
                /* experimental_raw_events */ false,
            )
            .await
            .expect("trusted connection should be live");
        let attached = recv_attachment_changed_notification(&mut outgoing_rx).await;
        assert_eq!(attached.generation, generation);
        assert_eq!(attached.revision, 1);
        assert_eq!(
            attached.changes,
            vec![ThreadAttachmentEntry {
                thread_id: thread_id.to_string(),
                interactive_attachment_count: 1,
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
        let detached = recv_attachment_changed_notification(&mut outgoing_rx).await;
        assert_eq!(detached.generation, generation);
        assert_eq!(detached.revision, 2);
        assert_eq!(
            detached.changes,
            vec![ThreadAttachmentEntry {
                thread_id: thread_id.to_string(),
                interactive_attachment_count: 0,
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
        let reattached = recv_attachment_changed_notification(&mut outgoing_rx).await;
        assert_eq!(reattached.revision, 3);

        manager.remove_connection(connection_id).await;
        let closed = recv_attachment_changed_notification(&mut outgoing_rx).await;
        assert_eq!(closed.generation, generation);
        assert_eq!(closed.revision, 4);
        assert_eq!(
            closed.changes,
            vec![ThreadAttachmentEntry {
                thread_id: thread_id.to_string(),
                interactive_attachment_count: 0,
            }]
        );
    }

    #[tokio::test]
    async fn attachment_revision_overflow_rotates_generation_before_publishing() {
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
            state.attachment_revision = u64::MAX;
            state.attachment_generation.clone()
        };
        manager
            .try_ensure_connection_subscribed(
                thread_id,
                connection_id,
                /* experimental_raw_events */ false,
            )
            .await
            .expect("trusted connection should be live");

        let snapshot = manager.thread_attachment_list().await;
        assert_ne!(snapshot.generation, old_generation);
        assert_eq!(snapshot.revision, 0);
        assert_eq!(snapshot.entries.len(), 1);
    }

    #[tokio::test]
    async fn clear_moves_only_requester_with_one_attachment_change() {
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
        let before = manager.thread_attachment_list().await;
        let (outgoing_tx, mut outgoing_rx) = mpsc::channel(8);
        manager.set_attachment_notification_outgoing(Arc::new(OutgoingMessageSender::new(
            outgoing_tx,
            AnalyticsEventsClient::disabled(),
        )));

        assert!(
            manager
                .move_connection_for_clear(predecessor_thread_id, successor_thread_id, requester)
                .await
        );

        let changed = recv_attachment_changed_notification(&mut outgoing_rx).await;
        assert_eq!(changed.generation, before.generation);
        assert_eq!(changed.revision, before.revision + 1);
        let mut expected_entries = vec![
            ThreadAttachmentEntry {
                thread_id: predecessor_thread_id.to_string(),
                interactive_attachment_count: 1,
            },
            ThreadAttachmentEntry {
                thread_id: successor_thread_id.to_string(),
                interactive_attachment_count: 1,
            },
        ];
        expected_entries.sort_unstable_by(|left, right| left.thread_id.cmp(&right.thread_id));
        assert_eq!(changed.changes, expected_entries);
        let snapshot = manager.thread_attachment_list().await;
        assert_eq!(snapshot.revision, changed.revision);
        assert_eq!(snapshot.entries, expected_entries);
        assert!(
            timeout(Duration::from_millis(50), outgoing_rx.recv())
                .await
                .is_err()
        );
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
                .reserve_clear_successor_attachment(predecessor_thread_id, successor_thread_id)
                .await
        );
        let (outgoing_tx, mut outgoing_rx) = mpsc::channel(4);
        manager.set_attachment_notification_outgoing(Arc::new(OutgoingMessageSender::new(
            outgoing_tx,
            AnalyticsEventsClient::disabled(),
        )));

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
        let changed = recv_attachment_changed_notification(&mut outgoing_rx).await;
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
                .reserve_clear_successor_attachment(predecessor_thread_id, successor_thread_id)
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
                .terminalize_clear_successor_attachment(predecessor_thread_id, successor_thread_id)
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

        let state = manager.state.lock().await;
        assert!(!state.retention_grants_by_thread.contains_key(&thread_id));
        assert!(
            !state
                .retention_threads_by_principal
                .contains_key(&principal)
        );
        drop(state);
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

    async fn recv_attachment_changed_notification(
        outgoing_rx: &mut mpsc::Receiver<OutgoingEnvelope>,
    ) -> ThreadAttachmentChangedNotification {
        let envelope = timeout(Duration::from_secs(1), outgoing_rx.recv())
            .await
            .expect("timed out waiting for attachment notification")
            .expect("outgoing channel closed unexpectedly");
        let OutgoingEnvelope::Broadcast { message } = envelope else {
            panic!("expected broadcast attachment notification");
        };
        let OutgoingMessage::AppServerNotification(envelope) = message else {
            panic!("expected app-server attachment notification");
        };
        let ServerNotification::ThreadAttachmentChanged(notification) = envelope.notification
        else {
            panic!("expected thread/attachment/changed notification");
        };
        notification
    }

    fn thread_settings(model: &str) -> ThreadSettings {
        ThreadSettings {
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
}

struct ThreadEntry {
    state: Arc<Mutex<ThreadState>>,
    connection_ids: HashSet<ConnectionId>,
    has_connections_watcher: watch::Sender<bool>,
}

impl Default for ThreadEntry {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(ThreadState::default())),
            connection_ids: HashSet::new(),
            has_connections_watcher: watch::channel(false).0,
        }
    }
}

impl ThreadEntry {
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
    live_connections: HashMap<ConnectionId, ConnectionCapabilities>,
    threads: HashMap<ThreadId, ThreadEntry>,
    thread_ids_by_connection: HashMap<ConnectionId, HashSet<ThreadId>>,
    // Retention authority is intentionally distinct from observation. The
    // forward and inverse maps are updated in one mutex domain so a grant can
    // neither survive its principal nor be released by another principal.
    retention_grants_by_thread: HashMap<ThreadId, HashMap<RetentionPrincipalId, RetentionGrantId>>,
    retention_threads_by_principal: HashMap<RetentionPrincipalId, HashSet<ThreadId>>,
    clear_transition_reservations: HashSet<ThreadId>,
    // B is disclosed to the requester before the durable A -> B attachment
    // move completes. Keep it unavailable to ordinary subscribe/resume until
    // that single atomic move consumes the predecessor attachment.
    clear_successor_reservations: HashMap<ThreadId, ThreadId>,
    attachment_generation: String,
    attachment_revision: u64,
    attachment_counts: HashMap<ThreadId, u32>,
}

#[derive(Debug)]
pub(crate) enum ConnectionSubscriptionError {
    ConnectionClosed,
    ClearSuccessorReserved,
}

impl ThreadStateManagerInner {
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

    fn add_interactive_attachment(&mut self, thread_id: ThreadId) -> ThreadAttachmentEntry {
        let interactive_attachment_count = self.attachment_counts.entry(thread_id).or_default();
        *interactive_attachment_count = interactive_attachment_count.saturating_add(1);
        ThreadAttachmentEntry {
            thread_id: thread_id.to_string(),
            interactive_attachment_count: *interactive_attachment_count,
        }
    }

    fn remove_interactive_attachment(
        &mut self,
        thread_id: ThreadId,
    ) -> Option<ThreadAttachmentEntry> {
        let interactive_attachment_count = {
            let interactive_attachment_count = self.attachment_counts.get_mut(&thread_id)?;
            *interactive_attachment_count = interactive_attachment_count.saturating_sub(1);
            *interactive_attachment_count
        };
        let entry = ThreadAttachmentEntry {
            thread_id: thread_id.to_string(),
            interactive_attachment_count,
        };
        if interactive_attachment_count == 0 {
            self.attachment_counts.remove(&thread_id);
        }
        Some(entry)
    }

    fn attachment_change(
        &mut self,
        mut changes: Vec<ThreadAttachmentEntry>,
    ) -> Option<ThreadAttachmentChangedNotification> {
        if changes.is_empty() {
            return None;
        }
        changes.sort_unstable_by(|left, right| left.thread_id.cmp(&right.thread_id));
        self.attachment_revision = match self.attachment_revision.checked_add(1) {
            Some(revision) => revision,
            None => {
                // Never publish two distinct changes with one revision. A new
                // generation makes every consumer resnapshot rather than
                // accepting an ambiguous incremental history.
                self.attachment_generation = Uuid::now_v7().to_string();
                0
            }
        };
        Some(ThreadAttachmentChangedNotification {
            generation: self.attachment_generation.clone(),
            revision: self.attachment_revision,
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
    attachment_notification_outgoing: Arc<StdMutex<Option<Arc<OutgoingMessageSender>>>>,
}

impl Default for ThreadStateManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ThreadStateManager {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(ThreadStateManagerInner {
                attachment_generation: Uuid::now_v7().to_string(),
                ..ThreadStateManagerInner::default()
            })),
            listener_commands: Arc::new(StdMutex::new(HashMap::new())),
            attachment_notification_outgoing: Arc::new(StdMutex::new(None)),
        }
    }

    pub(crate) fn set_attachment_notification_outgoing(
        &self,
        outgoing: Arc<OutgoingMessageSender>,
    ) {
        *self
            .attachment_notification_outgoing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(outgoing);
    }

    pub(crate) async fn thread_attachment_list(&self) -> ThreadAttachmentListResponse {
        let state = self.state.lock().await;
        let mut entries = state
            .attachment_counts
            .iter()
            .map(
                |(thread_id, interactive_attachment_count)| ThreadAttachmentEntry {
                    thread_id: thread_id.to_string(),
                    interactive_attachment_count: *interactive_attachment_count,
                },
            )
            .collect::<Vec<_>>();
        entries.sort_unstable_by(|left, right| left.thread_id.cmp(&right.thread_id));
        ThreadAttachmentListResponse {
            generation: state.attachment_generation.clone(),
            revision: state.attachment_revision,
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
        if !state.retention_principal_is_live(principal) {
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
        if !state.threads.contains_key(&thread_id) {
            return Err(RetentionAuthorityError::UnknownThread);
        }

        let record = state
            .lifecycle
            .get(&thread_id)
            .ok_or(RetentionAuthorityError::AuthorityUnavailable)?;
        if record.is_retiring() {
            return Err(RetentionAuthorityError::LifecycleClosed);
        }

        let grants = state
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
        state
            .retention_threads_by_principal
            .entry(principal)
            .or_default()
            .insert(thread_id);
        state.publish_retention(thread_id);
        record_retention_action(thread_id, principal, RetentionAction::Acquired);
        Ok(RetentionAcquireOutcome::Acquired { grant_id })
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

    async fn publish_attachment_change(&self, change: Option<ThreadAttachmentChangedNotification>) {
        let Some(change) = change else {
            return;
        };
        let outgoing = self
            .attachment_notification_outgoing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(outgoing) = outgoing {
            outgoing
                .send_server_notification(ServerNotification::ThreadAttachmentChanged(change))
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
                        Some(state.add_interactive_attachment(thread_id))
                    } else {
                        state.remove_interactive_attachment(thread_id)
                    }
                })
                .collect();
            state.attachment_change(changes)
        };
        self.publish_attachment_change(change).await;
    }

    /// Validates client authority and reserves the predecessor for one clear transition.
    ///
    /// Validation and reservation occur under the same mutex so two subscribed clients
    /// cannot both acquire clear authority for the same predecessor.
    pub(crate) async fn reserve_clear_transition_authority(
        &self,
        predecessor_thread_id: ThreadId,
        connection_id: ConnectionId,
    ) -> Result<(), ClearTransitionAuthorityError> {
        let mut state = self.state.lock().await;
        let Some(thread_entry) = state.threads.get(&predecessor_thread_id) else {
            return Err(ClearTransitionAuthorityError::UnknownPredecessor);
        };
        if !thread_entry.connection_ids.contains(&connection_id) {
            return Err(ClearTransitionAuthorityError::NotSubscribed);
        }
        if !state
            .clear_transition_reservations
            .insert(predecessor_thread_id)
        {
            return Err(ClearTransitionAuthorityError::TransitionConflict);
        }
        Ok(())
    }

    pub(crate) async fn release_clear_transition_authority(
        &self,
        predecessor_thread_id: ThreadId,
    ) -> bool {
        self.state
            .lock()
            .await
            .clear_transition_reservations
            .remove(&predecessor_thread_id)
    }

    /// Releases B only after the durable clear reaches its terminal
    /// disposition. This is deliberately separate from the request-scoped A
    /// authority release: an error after B is disclosed must remain
    /// fail-closed until reconciliation can account for the durable record.
    pub(crate) async fn terminalize_clear_successor_attachment(
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
    pub(crate) async fn reserve_clear_successor_attachment(
        &self,
        predecessor_thread_id: ThreadId,
        successor_thread_id: ThreadId,
    ) -> bool {
        let mut state = self.state.lock().await;
        if !state
            .clear_transition_reservations
            .contains(&predecessor_thread_id)
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
        let (thread_state, attachment_change) = {
            let mut state = self.state.lock().await;
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
                .attachment_counts
                .remove(&thread_id)
                .map(|_| ThreadAttachmentEntry {
                    thread_id: thread_id.to_string(),
                    interactive_attachment_count: 0,
                })
                .into_iter()
                .collect();
            (thread_state, state.attachment_change(changes))
        };
        self.publish_attachment_change(attachment_change).await;
        self.unregister_listener_command_tx(thread_id);

        if let Some(thread_state) = thread_state {
            let mut thread_state = thread_state.lock().await;
            tracing::debug!(
                thread_id = %thread_id,
                listener_generation = thread_state.listener_generation,
                had_listener = thread_state.cancel_tx.is_some(),
                had_active_turn = thread_state.active_turn_snapshot().is_some(),
                "clearing thread listener during thread-state teardown"
            );
            thread_state.clear_listener();
        }
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
                had_active_turn = thread_state.active_turn_snapshot().is_some(),
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
            if !state.threads.contains_key(&thread_id) {
                return false;
            }

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
                .then(|| state.remove_interactive_attachment(thread_id))
                .flatten()
                .into_iter()
                .collect();
            state.attachment_change(changes)
        };
        self.publish_attachment_change(change).await;
        true
    }

    /// Moves one live connection from an authoritative clear predecessor to
    /// its successor with one attachment revision. The caller has already
    /// established the durable A/B/T transition and listener readiness.
    pub(crate) async fn move_connection_for_clear(
        &self,
        predecessor_thread_id: ThreadId,
        successor_thread_id: ThreadId,
        connection_id: ConnectionId,
    ) -> bool {
        let change = {
            let mut state = self.state.lock().await;
            if !state.live_connections.contains_key(&connection_id)
                || !state
                    .thread_ids_by_connection
                    .get(&connection_id)
                    .is_some_and(|thread_ids| thread_ids.contains(&predecessor_thread_id))
                || state
                    .thread_ids_by_connection
                    .get(&connection_id)
                    .is_some_and(|thread_ids| thread_ids.contains(&successor_thread_id))
                || !state.threads.contains_key(&predecessor_thread_id)
            {
                return false;
            }

            let trusted_interactive = state
                .live_connections
                .get(&connection_id)
                .is_some_and(|capabilities| capabilities.trusted_interactive);
            let retention_principal = state
                .live_connections
                .get(&connection_id)
                .and_then(|capabilities| capabilities.retention_principal);
            if trusted_interactive && !state.attachment_counts.contains_key(&predecessor_thread_id)
            {
                return false;
            }

            let Some(thread_ids) = state.thread_ids_by_connection.get_mut(&connection_id) else {
                return false;
            };
            thread_ids.remove(&predecessor_thread_id);
            thread_ids.insert(successor_thread_id);

            if let Some(predecessor) = state.threads.get_mut(&predecessor_thread_id) {
                predecessor.connection_ids.remove(&connection_id);
                predecessor.update_has_connections();
            } else {
                return false;
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
                    state.remove_interactive_attachment(predecessor_thread_id)
                else {
                    return false;
                };
                let successor_change = state.add_interactive_attachment(successor_thread_id);
                vec![successor_change, predecessor_change]
            } else {
                Vec::new()
            };
            state.attachment_change(changes)
        };
        self.publish_attachment_change(change).await;
        true
    }

    #[cfg(test)]
    pub(crate) async fn has_subscribers(&self, thread_id: ThreadId) -> bool {
        self.state
            .lock()
            .await
            .threads
            .get(&thread_id)
            .is_some_and(|thread_entry| !thread_entry.connection_ids.is_empty())
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
        )
        .await
        .ok()
    }

    pub(crate) async fn try_ensure_connection_subscribed_for_listener(
        &self,
        thread_id: ThreadId,
        connection_id: ConnectionId,
        experimental_raw_events: bool,
    ) -> Result<Arc<Mutex<ThreadState>>, ConnectionSubscriptionError> {
        self.try_ensure_connection_subscribed_inner(
            thread_id,
            connection_id,
            experimental_raw_events,
        )
        .await
    }

    async fn try_ensure_connection_subscribed_inner(
        &self,
        thread_id: ThreadId,
        connection_id: ConnectionId,
        experimental_raw_events: bool,
    ) -> Result<Arc<Mutex<ThreadState>>, ConnectionSubscriptionError> {
        let (thread_state, attachment_change) = {
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
            let thread_state = {
                let thread_entry = state.ensure_thread_entry(thread_id);
                thread_entry.connection_ids.insert(connection_id);
                thread_entry.update_has_connections();
                thread_entry.state.clone()
            };
            let changes = state
                .live_connections
                .get(&connection_id)
                .is_some_and(|capabilities| capabilities.trusted_interactive)
                .then(|| was_added.then(|| state.add_interactive_attachment(thread_id)))
                .flatten()
                .into_iter()
                .collect();
            (thread_state, state.attachment_change(changes))
        };
        self.publish_attachment_change(attachment_change).await;
        {
            let mut thread_state_guard = thread_state.lock().await;
            if experimental_raw_events {
                thread_state_guard.set_experimental_raw_events(/*enabled*/ true);
            }
        }
        Ok(thread_state)
    }

    pub(crate) async fn try_add_connection_to_thread(
        &self,
        thread_id: ThreadId,
        connection_id: ConnectionId,
    ) -> Result<(), ConnectionSubscriptionError> {
        let change = {
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
            let changes = state
                .live_connections
                .get(&connection_id)
                .is_some_and(|capabilities| capabilities.trusted_interactive)
                .then(|| was_added.then(|| state.add_interactive_attachment(thread_id)))
                .flatten()
                .into_iter()
                .collect();
            state.attachment_change(changes)
        };
        self.publish_attachment_change(change).await;
        Ok(())
    }

    pub(crate) async fn remove_connection(&self, connection_id: ConnectionId) -> Vec<ThreadId> {
        let (thread_ids, attachment_change) = {
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
                        .filter_map(|thread_id| state.remove_interactive_attachment(*thread_id))
                        .collect()
                }
            } else {
                Default::default()
            };
            let attachment_change = state.attachment_change(changes);
            let threads_to_unload = thread_ids
                .into_iter()
                .filter(|thread_id| {
                    state
                        .threads
                        .get(thread_id)
                        .is_some_and(|thread_entry| thread_entry.connection_ids.is_empty())
                })
                .collect::<Vec<_>>();
            (threads_to_unload, attachment_change)
        };
        self.publish_attachment_change(attachment_change).await;
        thread_ids
    }

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
