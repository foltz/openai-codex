//! Captures action-time parent context and starts or recovers its Guardian reviewer.
//! Checkpoint recovery allows one fresh attempt under the original review deadline.
//! Both attempts share captured context and use separate recovery flags.
//! Completed assessments are revalidated before checkpoint publication and failed-review capture.
//! Checkpoints are captured before final authorization validation and published without yielding.
//! Recorded reviewer items and interruptions are forwarded to the parent's raw-event subscription.

use codex_guardian_reviewer::ReviewDeadline;
use std::sync::atomic::AtomicBool;

use super::*;
use crate::guardian::input_budget::CheckpointRecovery;
use crate::session::turn_context::{OriginatingAccountRecord, ReviewerAccountFence};
use codex_features::Feature;
use codex_guardian_reviewer::ReviewerPool;
use codex_guardian_reviewer::ReviewerRequest;
use codex_guardian_reviewer::ReviewerSelection;
use codex_protocol::protocol::TurnEnvironmentSelection;

#[derive(Clone)]
pub struct PreparedGuardianContext {
    parent: Arc<Session>,
    context: GuardianReviewContext,
    config: Config,
    context_policy: ReviewContextPolicy,
    key: GuardianReviewSessionReuseKey,
    parent_compaction: Option<ResponseItem>,
    reviewer_selection: ReviewerSelection,
    account_fence: Option<Arc<OriginatingAccountRecord>>,
    pub history_reset: CancellationToken,
}

/// Finite setup for one active review/prewarm. The pool caches only the
/// resulting session, never this operation owner or the reusable projection.
#[derive(Clone)]
pub struct GuardianInvocationSetup {
    context: Arc<PreparedGuardianContext>,
    work: Option<Arc<dyn codex_extension_api::HostOperationWork>>,
}

impl std::ops::Deref for GuardianInvocationSetup {
    type Target = PreparedGuardianContext;
    fn deref(&self) -> &Self::Target {
        &self.context
    }
}
impl std::ops::DerefMut for GuardianInvocationSetup {
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::make_mut(&mut self.context)
    }
}
impl GuardianInvocationSetup {
    pub(super) fn operation_work(&self) -> Option<&dyn codex_extension_api::HostOperationWork> {
        self.work.as_deref()
    }

    /// Retain metadata without extending this finite invocation's work.
    pub fn projection(&self) -> Arc<PreparedGuardianContext> {
        Arc::clone(&self.context)
    }

    pub fn new(
        context: PreparedGuardianContext,
        work: Option<Box<dyn codex_extension_api::HostOperationWork>>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            work.is_some()
                || !context
                    .parent
                    .services
                    .host_admission
                    .as_ref()
                    .is_some_and(|host| host.requires_account_work()),
            "Guardian invocation requires account work"
        );
        Ok(Self {
            context: Arc::new(context),
            work: work.map(Arc::from),
        })
    }

    /// Derive before constructing options; the retained ThreadManager constructor
    /// consumes this child instead of looking up an unrelated request task.
    pub async fn thread_options(
        &self,
        kind: GuardianReviewSessionKind,
        snapshot: Option<GuardianReviewForkSnapshot>,
    ) -> anyhow::Result<(crate::StartThreadOptions, GuardianReviewState)> {
        let work = self
            .work
            .as_deref()
            .map(|work| work.derive_operation())
            .transpose()
            .map_err(|_| {
                anyhow::anyhow!("Guardian invocation cannot admit reviewer construction")
            })?;
        let (mut options, state) = self.context.thread_options_inner(kind, snapshot).await?;
        options.account_work = work;
        Ok((options, state))
    }
}

impl PreparedGuardianContext {
    async fn prepare(
        parent: Arc<Session>,
        mut context: GuardianReviewContext,
        config: Config,
        history: &ContextManager,
        node_repl_policy: &GuardianNodeReplPolicy,
        access: codex_mcp::McpAttemptAccess<'_>,
        account_fence: Option<Arc<OriginatingAccountRecord>>,
    ) -> anyhow::Result<Self> {
        let _work = parent.derive_mcp_attempt_work(access)?;
        let (reset_version, history_reset) = parent.history_reset().await;
        // Preparation may have raced with a history reset before capturing the reviewer context.
        if reset_version != history.reset_version {
            history_reset.cancel();
        }
        let context_mode =
            GuardianContextMode::from_history(history.conversation_history_snapshot().as_ref());
        let context_policy = ReviewContextPolicy::for_context(context_mode, &config.features);
        let root_review_version =
            context_policy::root_review_version(context_policy, &parent).await;
        let parent_compaction = context_policy.parent_compaction(history.annotated_items())?;
        let mut instructions = parent.inherited_instructions().await;
        context.capture_project_instructions(&parent).await;
        instructions.project_snapshot = context.project_instructions.clone();
        let mut key = GuardianReviewSessionReuseKey::from_spawn_config(
            &config,
            instructions,
            history.history_version(),
            context_mode,
        )
        .with_environments(context.environments())
        .with_node_repl_policy_eligibility(context.model_info.computer_use_review_required())
        .with_node_repl_policy(node_repl_policy);
        key.root_review_version = root_review_version;
        key.parent_reset_version = history.reset_version;
        key.account_scope = account_fence.as_ref().map(|fence| {
            Arc::new(ReviewerAccountScope::new(
                &parent.services.auth_manager,
                fence.owner_generation(),
                fence.source_login_revisions(),
                fence.renewal_binding().cloned(),
                context.gateway_admission(),
            ))
        });
        Ok(Self {
            parent,
            context,
            config,
            context_policy,
            key,
            parent_compaction,
            reviewer_selection: ReviewerSelection::ReuseIfAvailable,
            account_fence,
            history_reset,
        })
    }
}

impl PreparedGuardianContext {
    fn reviewer_binding_matches(&self, reviewer: &GuardianReviewSession) -> bool {
        let Some(fence) = &self.account_fence else {
            return true;
        };
        let manager = &reviewer.session.services.auth_manager;
        manager.owner_pin_source().is_none()
            || (manager.owner_pin_gateway() == Some(self.context.gateway_admission())
                && manager.owner_pin_renewal().as_ref() == fence.renewal_binding()
                && fence.verify_current(manager).is_ok())
    }

    pub fn reuse_key(
        &self,
        previous: Option<&GuardianReviewSession>,
    ) -> GuardianReviewSessionReuseKey {
        let mut key = self.key.clone();
        if self.context_policy != ReviewContextPolicy::ThreadOwned
            && self.parent_compaction.is_none()
            && let Some(previous) = previous
        {
            // Without a decryptable summary, the existing reviewer may hold the
            // only remaining authorization or restriction from parent history.
            key.parent_history_version = previous.reuse_key.parent_history_version;
        }
        key
    }

    /// Moves fork history into startup options; only reusable reviewers retain a rollback checkpoint.
    /// Guardian selects the agent's identity and lifecycle.
    pub async fn thread_options(
        &self,
        kind: GuardianReviewSessionKind,
        snapshot: Option<GuardianReviewForkSnapshot>,
    ) -> anyhow::Result<(crate::StartThreadOptions, GuardianReviewState)> {
        self.parent
            .derive_mcp_attempt_work(codex_mcp::McpAttemptAccess::Unscoped)?;
        self.thread_options_inner(kind, snapshot).await
    }

    async fn thread_options_inner(
        &self,
        kind: GuardianReviewSessionKind,
        snapshot: Option<GuardianReviewForkSnapshot>,
    ) -> anyhow::Result<(crate::StartThreadOptions, GuardianReviewState)> {
        let auth_manager = match &self.account_fence {
            Some(fence) => fence
                .pinned_manager(
                    &self.parent.services.auth_manager,
                    self.context.gateway_admission(),
                )
                .map_err(|reason| {
                    anyhow::anyhow!("account admission refused Guardian construction: {reason}")
                })?,
            None => Arc::clone(&self.parent.services.auth_manager),
        };
        let (state, initial_history) = GuardianReviewState::for_session(
            kind,
            snapshot,
            self.reviewer_selection,
            self.parent_compaction.as_ref(),
        );
        let mut config = self.config.clone();
        if self.account_fence.is_some() {
            config.analytics_enabled = Some(false);
        }
        config.model_provider.supports_websockets &= self
            .parent
            .services
            .model_client
            .responses_websocket_enabled();
        let mut options = crate::StartThreadOptions {
            prompt_cache_key: self.parent.services.model_client.prompt_cache_key_policy(),
            history_mode: Some(codex_protocol::protocol::ThreadHistoryMode::Paginated),
            internal_parent: Some(crate::thread_manager::InternalSessionParent {
                thread_id: self.parent.thread_id(),
                auth_manager,
                agent_control: crate::agent::control::AgentControlInit::Provided {
                    control: Arc::clone(&self.parent.services.agent_control),
                    runtime: self.parent.services.local_agent_runtime.clone(),
                },
                originator: self.context.turn().originator.clone(),
                // Review the same applied instructions captured by the reuse key.
                // A live provider could advance independently while reviewing this action.
                inherited_instructions: Some(SessionInstructions {
                    user: self.key.user_instructions.clone(),
                    thread: self.key.thread_instructions.clone(),
                    project_snapshot: self.key.project_instructions.clone(),
                    ..Default::default()
                }),
            }),
            initial_history: initial_history.unwrap_or(InitialHistory::New),
            environments: Some(
                self.context
                    .environments()
                    .to_selections()
                    .into_iter()
                    .map(TurnEnvironmentSelection::into_request)
                    .collect(),
            ),
            inherited_environments: Some(self.context.environments().clone()),
            client_mcp_extensions: self.parent.services.client_mcp_extensions.clone(),
            ..crate::StartThreadOptions::new(config, /*control_endpoint*/ None)
        };
        if let Some(fence) = &self.account_fence {
            options
                .thread_extension_init
                .insert(ReviewerAccountFence(Arc::clone(fence)));
        }
        Ok((options, state))
    }

    /// Binds context bookkeeping to an agent that Guardian has already started.
    pub async fn bind_thread(
        &self,
        thread: &crate::CodexThread,
        context: GuardianReviewSessionReuseKey,
        mut state: GuardianReviewState,
        cancellation: CancellationToken,
    ) -> GuardianReviewSession {
        let session = Arc::clone(&thread.session);
        if self.parent.enabled(Feature::GuardianRecordedItems) {
            session
                .parent_observer_event_tx
                .get_or_init(|| self.parent.get_tx_event());
        }
        let io = SessionIo {
            tx_sub: (thread.io.tx_sub.clone()).into(),
            rx_event: thread.io.rx_event.clone(),
            agent_status: thread.io.agent_status.clone(),
            session_loop_termination: thread.io.session_loop_termination.clone(),
        };
        let inherited = session.inherited_instructions().await;
        {
            let history = session.clone_history().await;
            state.transcript_history_version = history.history_version();
            state.fresh_parent_checkpoint &= self
                .parent_compaction
                .as_ref()
                .zip(codex_history::CompactionCheckpoint::latest(
                    history.annotated_items(),
                ))
                .is_some_and(|(parent, loaded)| parent == loaded.item);
        }
        let context = GuardianReviewSessionReuseKey {
            user_instructions: inherited.user,
            thread_instructions: inherited.thread,
            ..context
        };
        if self.account_fence.is_none() {
            crate::session::emit_subagent_session_started(
                &self.parent.services.analytics_events_client,
                self.parent.app_server_client_metadata().await,
                session.session_id(),
                session.thread_id(),
                Some(self.parent.thread_id()),
                session.thread_config_snapshot().await,
                SubAgentSource::Other(GUARDIAN_REVIEWER_NAME.to_owned()),
                /*resumed_created_at*/ None,
            );
        }
        GuardianReviewSession {
            session,
            io,
            cancel_token: cancellation,
            reuse_key: context,
            state: Mutex::new(state),
        }
    }
}

#[derive(Clone)]
pub(in crate::guardian) struct PreparedReview {
    context: Arc<GuardianInvocationSetup>,
    params: Arc<GuardianReviewSessionParams>,
    recovery_requested: Arc<AtomicBool>,
    // One replacement under the original deadline; never retry a served request.
    reviewer_replaced: Arc<AtomicBool>,
}

struct ReviewerFenceScope<'a> {
    client: &'a crate::client::ModelClient,
    previous: Option<Arc<OriginatingAccountRecord>>,
}
impl<'a> ReviewerFenceScope<'a> {
    fn install(session: &'a Session, fence: &Arc<OriginatingAccountRecord>) -> Self {
        let client = &session.services.model_client;
        let previous = client.account_fence();
        client.set_account_fence(Some(Arc::clone(fence)));
        Self { client, previous }
    }
}
impl Drop for ReviewerFenceScope<'_> {
    fn drop(&mut self) {
        self.client.set_account_fence(self.previous.take());
    }
}

impl ReviewerRequest for PreparedReview {
    type Session = GuardianReviewSession;

    fn setup(&self) -> Arc<GuardianInvocationSetup> {
        Arc::clone(&self.context)
    }
    fn context(&self, previous: Option<&GuardianReviewSession>) -> GuardianReviewSessionReuseKey {
        self.context.reuse_key(previous)
    }
    fn requires_fresh_session(&self) -> bool {
        self.context.reviewer_selection == ReviewerSelection::FreshParentCheckpoint
    }
    fn deadline(&self) -> ReviewDeadline {
        self.params.deadline
    }
    fn cancellation(&self) -> Option<&CancellationToken> {
        self.params.external_cancel.as_ref()
    }

    async fn run(
        &self,
        session: &GuardianReviewSession,
        kind: GuardianReviewSessionKind,
    ) -> ReviewSessionResult {
        let _fence_scope = if let Some(fence) = &self.context.account_fence {
            if let Err(reason) = fence.verify_current(&self.context.parent.services.auth_manager) {
                return ReviewSessionResult {
                    outcome: GuardianReviewSessionOutcome::PromptBuildFailed(refused_review(
                        reason,
                    )),
                    disposition: SessionDisposition::Discard,
                    analytics: GuardianReviewAnalyticsResult::without_session(),
                };
            }
            if !self.context.reviewer_binding_matches(session) {
                self.reviewer_replaced.store(true, Ordering::Release);
                return ReviewSessionResult {
                    outcome: GuardianReviewSessionOutcome::PromptBuildFailed(refused_review(
                        "reviewer authentication binding changed",
                    )),
                    disposition: SessionDisposition::Discard,
                    analytics: GuardianReviewAnalyticsResult::without_session(),
                };
            }
            Some(ReviewerFenceScope::install(&session.session, fence))
        } else {
            None
        };
        // A prewarmed session can already be fresh on the initial attempt. Consume
        // this before running: even a failed review can leave new input in history.
        let fresh_parent_checkpoint =
            std::mem::take(&mut session.state.lock().await.fresh_parent_checkpoint);
        if self.context.parent_compaction.is_some() {
            session
                .session
                .services
                .thread_extension_data
                .insert(CheckpointRecovery {
                    requested: Arc::clone(&self.recovery_requested),
                    history_version: session.session.clone_history().await.history_version(),
                    fresh_parent_checkpoint,
                });
        } else {
            session
                .session
                .services
                .thread_extension_data
                .remove::<CheckpointRecovery>();
        }
        let mut result = Box::pin(run_review_on_session(
            session,
            &self.params,
            self.context.operation_work(),
            kind,
            self.params.deadline,
        ))
        .await;
        let recovery_requested = self.recovery_requested.load(Ordering::Acquire);
        if recovery_requested {
            result.disposition = SessionDisposition::Discard;
        }
        let checkpoint = if result.disposition == SessionDisposition::Reusable
            && matches!(result.outcome, GuardianReviewSessionOutcome::Completed(_))
            && !matches!(kind, GuardianReviewSessionKind::EphemeralForked)
        {
            let items = session.session.guardian_fork_history().await;
            let history_version = session.session.clone_history().await.history_version();
            Some((items, history_version, session.state.lock().await))
        } else {
            None
        };
        // All checkpoint capture and lock acquisition must precede validation. No await
        // may follow a valid assessment before publication; new guidance during capture
        // invalidates either verdict without overwriting the last committed review.
        if let GuardianReviewSessionOutcome::Completed(Ok(Some(response))) = &result.outcome
            && response.assessment().is_ok()
            && let Some(authorization) = &self.params.authorization
            && let Some(invalidation) = authorization
                .invalidation(
                    &self.params.parent_session,
                    self.params.external_cancel.as_ref(),
                )
                .await
        {
            let outcome = invalidation.outcome;
            result.analytics.context_changed |= invalidation.context_changed;
            result.disposition =
                if matches!(outcome, GuardianReviewSessionOutcome::StaleAuthorization)
                    && result.disposition == SessionDisposition::Reusable
                {
                    SessionDisposition::Rollback
                } else {
                    SessionDisposition::Discard
                };
            result.outcome = outcome;
            return result;
        }
        // All awaited checkpoint/authorization work is complete. Re-prove origin
        // before publishing a result; no account switch can relabel the verdict.
        if let Some(fence) = &self.context.account_fence
            && let Err(reason) = fence.verify_current(&self.context.parent.services.auth_manager)
        {
            result.outcome =
                GuardianReviewSessionOutcome::PromptBuildFailed(refused_review(reason));
            result.disposition = SessionDisposition::Discard;
            return result;
        }
        if let Some((items, history_version, mut state)) = checkpoint {
            state.commit_snapshot(
                items,
                history_version,
                session
                    .session
                    .services
                    .thread_extension_data
                    .get::<CheckpointRecovery>()
                    .as_deref(),
            );
        }
        if self.context.reviewer_selection == ReviewerSelection::FreshParentCheckpoint
            || !recovery_requested
        {
            record_failed_review(&session.session, &self.params, &result.outcome).await;
        }
        result
    }
}

pub(crate) async fn run_guardian_review_session(
    pool: Arc<ReviewerPool<GuardianReviewSession>>,
    params: GuardianReviewSessionParams,
) -> (GuardianReviewSessionOutcome, GuardianReviewAnalyticsResult) {
    let context_mode = GuardianContextMode::from_history(
        params
            .parent_history
            .conversation_history_snapshot()
            .as_ref(),
    );
    let (outcome, mut analytics) = match prepare_review(params).await {
        Ok(mut prepared) => {
            let mut result = Box::pin(pool.review(prepared.clone())).await;
            if prepared.reviewer_replaced.swap(false, Ordering::AcqRel) {
                // The old reviewer ran no request. Its replacement consumes the
                // same finite invocation and the same original deadline.
                result = Box::pin(pool.review(prepared.clone())).await;
            }
            if prepared.recovery_requested.load(Ordering::Acquire) {
                // One restart only, under the original deadline. An oversized parent
                // checkpoint must fail rather than repeatedly compact and recreate.
                Arc::make_mut(&mut prepared.context).reviewer_selection =
                    ReviewerSelection::FreshParentCheckpoint;
                prepared.recovery_requested = Arc::default();
                let (outcome, mut analytics) = pool.review(prepared).await;
                analytics.context_changed |= result.1.context_changed;
                (outcome, analytics)
            } else {
                result
            }
        }
        Err(error) => (
            GuardianReviewSessionOutcome::PromptBuildFailed(error),
            GuardianReviewAnalyticsResult::without_session(),
        ),
    };
    // Keep the captured mode even when preparation or reviewer startup fails.
    analytics.guardian_context_mode = Some(context_mode.as_str());
    (outcome, analytics)
}

pub(in crate::guardian) async fn prepare_review(
    params: GuardianReviewSessionParams,
) -> anyhow::Result<PreparedReview> {
    let (work, fence) = review_account_work(&params)?;
    prepare_review_with_authority(params, work, fence).await
}

fn refused_review(reason: &str) -> anyhow::Error {
    anyhow::anyhow!("account admission refused Guardian review setup: {reason}")
}

type ReviewAccount = (
    Option<Box<dyn codex_extension_api::HostOperationWork>>,
    Option<Arc<OriginatingAccountRecord>>,
);

/// Account custody for one Guardian review. Review content always comes from
/// the originating turn A (`parent_context`, upstream #49880); this chooses
/// only the finite account work that the review itself consumes.
///
/// - A still holds custody (in-turn review): derive from A, as before.
/// - A has ended: fresh limited admission for this review only, gated on A's
///   admission-time `OriginatingAccountRecord`. Refused when the record is
///   missing, the server is draining, admission is closed (managed transition)
///   or exhausted (`RetryAfter` is a refusal; nothing waits), or the atomic
///   owner snapshot differs. The check runs while the new permit is held; the
///   returned fence re-proves the owner on every reviewer request, which also
///   covers native auth changes that bypass the permit barrier.
pub(super) fn review_account_work(
    params: &GuardianReviewSessionParams,
) -> anyhow::Result<ReviewAccount> {
    let session = &params.parent_session;
    let Some(host) = session.services.host_admission.as_ref() else {
        return Ok((None, None));
    };
    let origin = params.parent_context.turn();
    match host.derive_operation_work(&session.services.thread_extension_data, &origin.sub_id) {
        Ok(work) if work.is_some() || !host.requires_account_work() => return Ok((work, None)),
        Ok(None) => {}
        Ok(Some(_)) => unreachable!("guard accepts present work"),
        Err(codex_extension_api::TurnWorkRefused::Unavailable) => {}
        Err(codex_extension_api::TurnWorkRefused::RetryAfter(_)) => {
            return Err(refused_review("originating turn custody exhausted"));
        }
    }
    let Some(record) = origin.extension_data.get::<OriginatingAccountRecord>() else {
        return Err(refused_review("originating turn has no account record"));
    };
    // Point check of the shutdown gate; the probe is released immediately.
    if host.admit_turn_start().is_none() {
        return Err(refused_review("server is draining"));
    }
    let work = host.admit_operation_work().map_err(|_| {
        refused_review("account work admission closed (managed transition or exhaustion)")
    })?;
    record
        .verify_current(&session.services.auth_manager)
        .map_err(refused_review)?;
    Ok((work, Some(record)))
}

/// An isolated request has explicit operation custody, not a synthetic live turn.
pub(in crate::guardian) async fn prepare_review_with_work(
    params: GuardianReviewSessionParams,
    work: Option<Box<dyn codex_extension_api::HostOperationWork>>,
) -> anyhow::Result<PreparedReview> {
    let fence = if params
        .parent_session
        .services
        .host_admission
        .as_ref()
        .is_some_and(|host| host.requires_account_work())
    {
        let record = params
            .parent_context
            .turn()
            .extension_data
            .get::<OriginatingAccountRecord>()
            .ok_or_else(|| anyhow::anyhow!("Guardian origin account record unavailable"))?;
        record
            .verify_current(&params.parent_session.services.auth_manager)
            .map_err(|reason| anyhow::anyhow!("Guardian account admission refused: {reason}"))?;
        Some(record)
    } else {
        None
    };
    prepare_review_with_authority(params, work, fence).await
}

async fn prepare_review_with_authority(
    params: GuardianReviewSessionParams,
    work: Option<Box<dyn codex_extension_api::HostOperationWork>>,
    account_fence: Option<Arc<OriginatingAccountRecord>>,
) -> anyhow::Result<PreparedReview> {
    anyhow::ensure!(
        work.is_some()
            || !params
                .parent_session
                .services
                .host_admission
                .as_ref()
                .is_some_and(|host| host.requires_account_work()),
        "Guardian preparation requires account work"
    );
    if let Some(fence) = &account_fence {
        params.detached_admission.record(Arc::clone(fence));
    }
    let preparation_work = work
        .as_deref()
        .map(|work| work.derive_operation())
        .transpose()
        .map_err(|_| anyhow::anyhow!("Guardian preparation work refused"))?
        .map(|work| {
            Box::new(crate::session::McpOperationWork(work)) as Box<dyn codex_mcp::McpAttemptWork>
        });
    let context = PreparedGuardianContext::prepare(
        Arc::clone(&params.parent_session),
        params.parent_context.clone(),
        params.spawn_config.clone(),
        &params.parent_history,
        &params.node_repl_policy,
        codex_mcp::McpAttemptAccess::from_work(preparation_work.as_deref()),
        account_fence,
    )
    .await?;
    Ok(PreparedReview {
        context: Arc::new(GuardianInvocationSetup::new(context, work)?),
        params: Arc::new(params),
        recovery_requested: Arc::default(),
        reviewer_replaced: Arc::default(),
    })
}

/// Captures the same startup context used by the existing prompt builder.
/// The caller owns scheduling, cancellation, and the reviewer pool.
pub async fn prepare_review_prewarm(
    parent: &crate::CodexThread,
) -> anyhow::Result<PreparedGuardianContext> {
    let work = parent.session.request_mcp_work()?;
    let turn = parent
        .session
        .new_startup_prewarm_turn_with_sub_id(crate::session::INITIAL_SUBMIT_ID.to_owned())
        .await;
    prepare_prewarm(
        Arc::clone(&parent.session),
        turn,
        codex_mcp::McpAttemptAccess::from_work(work.as_deref()),
    )
    .await
}

/// Prepare a finite reviewer invocation using authority captured by the caller
/// before it spawned this job. No ambient request lookup occurs here.
pub async fn prepare_review_prewarm_invocation(
    parent: &crate::CodexThread,
    work: Option<Box<dyn codex_extension_api::HostOperationWork>>,
) -> anyhow::Result<GuardianInvocationSetup> {
    anyhow::ensure!(
        work.is_some()
            || !parent
                .session
                .services
                .host_admission
                .as_ref()
                .is_some_and(|host| host.requires_account_work()),
        "Guardian prewarm requires account work"
    );
    let preparation_work = work
        .as_deref()
        .map(|work| work.derive_operation())
        .transpose()
        .map_err(|_| anyhow::anyhow!("Guardian prewarm work refused"))?
        .map(|work| {
            Box::new(crate::session::McpOperationWork(work)) as Box<dyn codex_mcp::McpAttemptWork>
        });
    let turn = parent
        .session
        .new_startup_prewarm_turn_with_sub_id(crate::session::INITIAL_SUBMIT_ID.to_owned())
        .await;
    let context = prepare_prewarm(
        Arc::clone(&parent.session),
        turn,
        codex_mcp::McpAttemptAccess::from_work(preparation_work.as_deref()),
    )
    .await?;
    GuardianInvocationSetup::new(context, work)
}

pub(super) fn prepare_prewarm<'a>(
    parent: Arc<Session>,
    turn: Arc<TurnContext>,
    access: codex_mcp::McpAttemptAccess<'a>,
) -> BoxFuture<'a, anyhow::Result<PreparedGuardianContext>> {
    Box::pin(async move {
        let work = parent.derive_mcp_attempt_work(access)?;
        let access = codex_mcp::McpAttemptAccess::from_work(work.as_deref());
        let context = GuardianReviewContext::from(turn);
        let config = guardian_review_session_config(&parent, &context).await?;
        let history = parent.clone_history().await;
        PreparedGuardianContext::prepare(
            Arc::clone(&parent),
            context,
            config.spawn_config,
            &history,
            &config.node_repl_policy,
            access,
            None,
        )
        .await
    })
}
