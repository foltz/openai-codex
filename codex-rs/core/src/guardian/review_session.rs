use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::Weak;
use std::time::Duration;

use anyhow::anyhow;
use codex_analytics::GuardianReviewAnalyticsResult;
use codex_analytics::GuardianReviewSessionAnalyticsParams;
use codex_analytics::GuardianReviewSessionKind;
use codex_extension_api::LoadedUserInstructions;
use codex_extension_api::UserInstructions;
use codex_history::InitialHistory;
use codex_history::RolloutItem;
use codex_protocol::ThreadId;
use codex_protocol::config_types::AutoCompactTokenLimitScope;
use codex_protocol::config_types::Personality;
use codex_protocol::config_types::ReasoningSummary as ReasoningSummaryConfig;
use codex_protocol::models::BaseInstructionsProvenance;
use codex_protocol::models::PermissionProfile;
use codex_protocol::models::ResponseItem;
use codex_protocol::openai_models::ModelMessages;
use codex_protocol::openai_models::ReasoningEffort as ReasoningEffortConfig;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::CodexErrorInfo;
use codex_protocol::protocol::ErrorEvent;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::protocol::TokenUsage;
use futures::FutureExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use serde_json::Value;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::codex_delegate::DelegateForwardingReceipts;
use crate::codex_delegate::run_codex_thread_interactive_with_custody;
use crate::config::Config;
use crate::config::Constrained;
use crate::config::ManagedFeatures;
use crate::config::NetworkProxySpec;
use crate::config::Permissions;
use crate::context::ContextualUserFragment;
use crate::context::GuardianFollowupReviewReminder;
use crate::session::GitEnrichmentPolicy;
use crate::session::SessionIo;
use crate::session::SessionRetirementIo;
use crate::session::session::Session;
use crate::session::startup_custody::SessionStartupCustody;
use crate::session::turn_context::TurnContext;
use codex_config::types::McpServerConfig;
use codex_features::Feature;
use codex_model_provider_info::ModelProviderInfo;
use codex_utils_path_uri::PathUri;

use super::ApprovalRequestReasons;
use super::GUARDIAN_REVIEWER_NAME;
use super::GuardianApprovalRequest;
use super::GuardianReviewContext;
use super::prompt::BUNDLED_GUARDIAN_POLICY;
use super::prompt::BUNDLED_GUARDIAN_POLICY_TEMPLATE;
use super::prompt::GuardianPromptMode;
use super::prompt::GuardianTranscriptCursor;
use super::prompt::build_guardian_prompt_items_with_parent_turn;
use super::prompt::guardian_policy_prompt_with_config_and_template;
use super::review::guardian_review_session_config;

const GUARDIAN_INTERRUPT_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
#[derive(Debug)]
pub(crate) enum GuardianReviewSessionOutcome {
    Completed(anyhow::Result<Option<String>>),
    PromptBuildFailed(anyhow::Error),
    SessionFailed {
        error: anyhow::Error,
        error_info: Option<CodexErrorInfo>,
    },
    TimedOut,
    Aborted,
}

pub(crate) struct GuardianReviewSessionParams {
    pub(crate) parent_session: Arc<Session>,
    pub(crate) parent_context: GuardianReviewContext,
    pub(crate) spawn_config: Config,
    pub(crate) request: GuardianApprovalRequest,
    pub(crate) reasons: ApprovalRequestReasons,
    pub(crate) schema: Value,
    pub(crate) model: String,
    pub(crate) reasoning_effort: Option<ReasoningEffortConfig>,
    pub(crate) guardian_default_review_model_id: String,
    pub(crate) guardian_catalog_contains_auto_review: bool,
    pub(crate) guardian_review_model_overridden: bool,
    pub(crate) guardian_review_model_override: Option<String>,
    pub(crate) reasoning_summary: ReasoningSummaryConfig,
    pub(crate) personality: Option<Personality>,
    pub(crate) external_cancel: Option<CancellationToken>,
    pub(crate) deadline: tokio::time::Instant,
}

#[derive(Default)]
pub(crate) struct GuardianReviewSessionManager {
    state: Arc<Mutex<GuardianReviewSessionState>>,
    cancellation_token: CancellationToken,
    cleanup: GuardianCleanupRegistry,
}

#[derive(Default)]
struct GuardianReviewSessionState {
    trunk: Option<Arc<GuardianReviewSession>>,
    ephemeral_reviews: Vec<Arc<GuardianReviewSession>>,
}

struct GuardianReviewSession {
    session: Arc<Session>,
    io: SessionIo,
    reuse_key: GuardianReviewSessionReuseKey,
    review_lock: Semaphore,
    state: Mutex<GuardianReviewState>,
    cleanup: Arc<GuardianChildCleanup>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GuardianTaskOutcome {
    Clean,
    Failed,
    Panicked,
}

type GuardianTaskReceipt = Shared<BoxFuture<'static, GuardianTaskOutcome>>;

struct GuardianChildCleanupState {
    completion: Option<GuardianTaskReceipt>,
    completion_phase: Option<GuardianCleanupPhase>,
    auxiliary: Vec<GuardianTaskReceipt>,
    auxiliary_open: bool,
    retirement: Option<crate::codex_thread::ThreadRetirement>,
}

impl Default for GuardianChildCleanupState {
    fn default() -> Self {
        Self {
            completion: None,
            completion_phase: None,
            auxiliary: Vec::new(),
            auxiliary_open: true,
            retirement: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GuardianCleanupPhase {
    Legacy,
    DeadlineBound,
}

struct GuardianChildCleanup {
    session: Arc<Session>,
    io: SessionRetirementIo,
    cancel_token: CancellationToken,
    forwarding: DelegateForwardingReceipts,
    state: StdMutex<GuardianChildCleanupState>,
}

impl GuardianChildCleanup {
    fn new(
        session: Arc<Session>,
        io: SessionRetirementIo,
        cancel_token: CancellationToken,
        forwarding: DelegateForwardingReceipts,
    ) -> Arc<Self> {
        Arc::new(Self {
            session,
            io,
            cancel_token,
            forwarding,
            state: StdMutex::new(GuardianChildCleanupState::default()),
        })
    }

    fn begin_legacy(&self) -> GuardianTaskReceipt {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(completion) = state.completion.as_ref() {
            return completion.clone();
        }
        self.cancel_token.cancel();
        let io = self.io.clone();
        let forwarding = self.forwarding.clone();
        let task = tokio::spawn(async move {
            let (shutdown, forwarding) = tokio::join!(io.shutdown_and_wait(), forwarding.wait());
            if shutdown.is_ok() && forwarding {
                GuardianTaskOutcome::Clean
            } else {
                GuardianTaskOutcome::Failed
            }
        });
        let completion = retain_guardian_task(task);
        state.completion = Some(completion.clone());
        state.completion_phase = Some(GuardianCleanupPhase::Legacy);
        completion
    }

    fn auxiliary_receipts(&self) -> Vec<GuardianTaskReceipt> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .auxiliary
            .clone()
    }

    fn close_auxiliary_admission(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .auxiliary_open = false;
    }

    fn spawn_auxiliary<F>(&self, future: F)
    where
        F: Future<Output = GuardianTaskOutcome> + Send + 'static,
    {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.auxiliary_open {
            return;
        }
        state
            .auxiliary
            .push(retain_guardian_task(tokio::spawn(future)));
    }

    async fn auxiliary_clean_until(&self, deadline: tokio::time::Instant) -> bool {
        let tasks = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.auxiliary_open = false;
            state.auxiliary.clone()
        };
        tokio::time::timeout_at(deadline, futures::future::join_all(tasks))
            .await
            .is_ok_and(|outcomes| {
                outcomes
                    .iter()
                    .all(|outcome| *outcome == GuardianTaskOutcome::Clean)
            })
    }

    async fn auxiliary_clean(&self) -> bool {
        let tasks = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.auxiliary_open = false;
            state.auxiliary.clone()
        };
        futures::future::join_all(tasks)
            .await
            .iter()
            .all(|outcome| *outcome == GuardianTaskOutcome::Clean)
    }

    fn begin_bounded(&self, deadline: tokio::time::Instant) -> GuardianTaskReceipt {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(completion) = state.completion.as_ref() {
            if state.completion_phase == Some(GuardianCleanupPhase::Legacy)
                && completion.peek().is_none()
            {
                return futures::future::ready(GuardianTaskOutcome::Failed)
                    .boxed()
                    .shared();
            }
            return completion.clone();
        }
        if tokio::time::Instant::now() >= deadline {
            return futures::future::ready(GuardianTaskOutcome::Failed)
                .boxed()
                .shared();
        }
        let ticket = match crate::codex_thread::ThreadRetirement::from_session(
            Arc::clone(&self.session),
            self.io.clone(),
            deadline,
        ) {
            Ok(ticket) => ticket,
            Err(_) => {
                return futures::future::ready(GuardianTaskOutcome::Failed)
                    .boxed()
                    .shared();
            }
        };
        state.retirement = Some(ticket.clone());
        self.cancel_token.cancel();
        let forwarding = self.forwarding.clone();
        let task = tokio::spawn(async move {
            let observed = tokio::time::timeout_at(deadline, async {
                let (report, forwarding) = tokio::join!(ticket.wait(), forwarding.wait());
                report.is_complete() && forwarding
            })
            .await;
            if matches!(observed, Ok(true)) {
                GuardianTaskOutcome::Clean
            } else {
                GuardianTaskOutcome::Failed
            }
        });
        let completion = retain_guardian_task(task);
        state.completion = Some(completion.clone());
        state.completion_phase = Some(GuardianCleanupPhase::DeadlineBound);
        completion
    }
}

fn retain_guardian_task(task: tokio::task::JoinHandle<GuardianTaskOutcome>) -> GuardianTaskReceipt {
    async move {
        match task.await {
            Ok(outcome) => outcome,
            Err(error) if error.is_cancelled() => GuardianTaskOutcome::Failed,
            Err(_) => GuardianTaskOutcome::Panicked,
        }
    }
    .boxed()
    .shared()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GuardianConstructionOutcome {
    Returned,
    Cancelled,
    Panicked,
}

type GuardianConstructionReceipt = Shared<BoxFuture<'static, GuardianConstructionOutcome>>;

fn retain_guardian_construction(
    task: tokio::task::JoinHandle<GuardianConstructionOutcome>,
) -> GuardianConstructionReceipt {
    async move {
        match task.await {
            Ok(outcome) => outcome,
            Err(error) if error.is_cancelled() => GuardianConstructionOutcome::Cancelled,
            Err(_) => GuardianConstructionOutcome::Panicked,
        }
    }
    .boxed()
    .shared()
}

struct GuardianConstruction {
    completion: GuardianConstructionReceipt,
    startup: Arc<SessionStartupCustody>,
    child: Arc<StdMutex<Option<Arc<GuardianReviewSession>>>>,
    retirement: StdMutex<Option<GuardianTaskReceipt>>,
}

impl GuardianConstruction {
    fn child_is(&self, child: &Arc<GuardianReviewSession>) -> bool {
        self.child
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|owned| Arc::ptr_eq(owned, child))
    }

    fn begin_legacy_retirement(&self) -> Option<GuardianTaskReceipt> {
        let child = self
            .child
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()?;
        let mut retirement = self
            .retirement
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(retirement) = retirement.as_ref() {
            return Some(retirement.clone());
        }
        child.cleanup.close_auxiliary_admission();
        let primary = child.cleanup.begin_legacy();
        let auxiliary = child.cleanup.auxiliary_receipts();
        let receipt = retain_guardian_task(tokio::spawn(async move {
            if primary.await == GuardianTaskOutcome::Clean
                && futures::future::join_all(auxiliary)
                    .await
                    .iter()
                    .all(|outcome| *outcome == GuardianTaskOutcome::Clean)
            {
                GuardianTaskOutcome::Clean
            } else {
                GuardianTaskOutcome::Failed
            }
        }));
        *retirement = Some(receipt.clone());
        Some(receipt)
    }
}

#[derive(Clone, Default)]
struct GuardianCleanupRegistry {
    state: Arc<StdMutex<GuardianCleanupRegistryState>>,
    #[cfg(test)]
    pause_after_retain: Arc<StdMutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>>,
    #[cfg(test)]
    pause_after_persistence:
        Arc<StdMutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>>,
}

#[derive(Default)]
struct GuardianCleanupRegistryState {
    closed: bool,
    deadline: Option<tokio::time::Instant>,
    constructions: Vec<Arc<GuardianConstruction>>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct GuardianCleanupReport {
    pub(crate) complete: bool,
    pub(crate) panicked: bool,
    pub(crate) unavailable: bool,
}

impl GuardianCleanupReport {
    pub(crate) fn is_clean(&self) -> bool {
        self.complete && !self.panicked && !self.unavailable
    }
}

impl GuardianCleanupRegistry {
    fn compact_terminal_construction(&self, construction: &Arc<GuardianConstruction>) {
        let removed = {
            let Ok(mut state) = self.state.lock() else {
                return;
            };
            let Some(index) = state
                .constructions
                .iter()
                .position(|owned| Arc::ptr_eq(owned, construction))
            else {
                return;
            };
            if construction.completion.peek() != Some(&GuardianConstructionOutcome::Returned)
                || !construction.startup.is_empty()
                || construction
                    .retirement
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .as_ref()
                    .is_none_or(|receipt| receipt.peek() != Some(&GuardianTaskOutcome::Clean))
            {
                return;
            }
            state.constructions.swap_remove(index)
        };
        // The last guardian owner can release Session resources. Do not run
        // that destructor under the registry admission mutex.
        drop(removed);
    }

    fn retire_child_in_background(&self, child: &Arc<GuardianReviewSession>) {
        let construction = {
            let Ok(state) = self.state.lock() else {
                return;
            };
            state
                .constructions
                .iter()
                .find(|construction| construction.child_is(child))
                .cloned()
        };
        let Some(construction) = construction else {
            return;
        };
        let Some(retirement) = construction.begin_legacy_retirement() else {
            return;
        };
        let registry = self.clone();
        // This observer does not own or retry cleanup. The construction keeps
        // the exact primary and auxiliary receipts; the observer only releases
        // that custody after their terminal clean result is already recorded.
        tokio::spawn(async move {
            if retirement.await == GuardianTaskOutcome::Clean
                && construction.completion.clone().await == GuardianConstructionOutcome::Returned
            {
                registry.compact_terminal_construction(&construction);
            }
        });
    }

    fn close_until(&self, deadline: tokio::time::Instant) -> Result<tokio::time::Instant, ()> {
        match self.state.lock() {
            Ok(mut state) => {
                state.closed = true;
                Ok(*state.deadline.get_or_insert(deadline))
            }
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.closed = true;
                state.deadline.get_or_insert(deadline);
                Err(())
            }
        }
    }

    fn close_legacy(&self) -> Result<(), ()> {
        match self.state.lock() {
            Ok(mut state) => {
                state.closed = true;
                Ok(())
            }
            Err(poisoned) => {
                poisoned.into_inner().closed = true;
                Err(())
            }
        }
    }

    #[cfg(test)]
    fn pause_next_construction_after_retain_for_test(
        &self,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    ) {
        *self
            .pause_after_retain
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((entered, release));
    }

    #[cfg(test)]
    fn pause_next_construction_after_persistence_for_test(
        &self,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    ) {
        *self
            .pause_after_persistence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((entered, release));
    }

    #[cfg(test)]
    fn retain_child_for_test(&self, child: Arc<GuardianReviewSession>) {
        let construction = Arc::new(GuardianConstruction {
            completion: futures::future::ready(GuardianConstructionOutcome::Returned)
                .boxed()
                .shared(),
            startup: Arc::new(SessionStartupCustody::default()),
            child: Arc::new(StdMutex::new(Some(child))),
            retirement: StdMutex::new(None),
        });
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .constructions
            .push(construction);
    }

    fn register<F, Fut>(
        &self,
        factory: F,
    ) -> anyhow::Result<BoxFuture<'static, anyhow::Result<Arc<GuardianReviewSession>>>>
    where
        F: FnOnce(Arc<SessionStartupCustody>) -> Fut + Send + 'static,
        Fut: Future<Output = anyhow::Result<GuardianReviewSession>> + Send + 'static,
    {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("guardian cleanup custody unavailable"))?;
        if state.closed {
            return Err(anyhow!("guardian cleanup admission closed"));
        }
        let startup = Arc::new(SessionStartupCustody::default());
        #[cfg(test)]
        if let Some((entered, release)) = self
            .pause_after_retain
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            startup.pause_after_retain_for_test(entered, release);
        }
        #[cfg(test)]
        if let Some((entered, release)) = self
            .pause_after_persistence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            startup.pause_after_persistence_for_test(entered, release);
        }
        let child = Arc::new(StdMutex::new(None));
        let child_for_task = Arc::clone(&child);
        let startup_for_task = Arc::clone(&startup);
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            match std::panic::AssertUnwindSafe(factory(Arc::clone(&startup_for_task)))
                .catch_unwind()
                .await
            {
                Ok(result) => {
                    let result = result.map(Arc::new);
                    if let Ok(review_session) = &result {
                        *child_for_task
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) =
                            Some(Arc::clone(review_session));
                        startup_for_task.published();
                    }
                    let _ = result_tx.send(result);
                    GuardianConstructionOutcome::Returned
                }
                Err(_) => GuardianConstructionOutcome::Panicked,
            }
        });
        let completion = retain_guardian_construction(task);
        state.constructions.push(Arc::new(GuardianConstruction {
            completion: completion.clone(),
            startup,
            child,
            retirement: StdMutex::new(None),
        }));
        Ok(async move {
            if completion.await == GuardianConstructionOutcome::Panicked {
                return Err(anyhow!("guardian construction panicked"));
            }
            result_rx
                .await
                .map_err(|_| anyhow!("guardian construction result unavailable"))?
        }
        .boxed())
    }

    async fn shutdown_until(&self, deadline: tokio::time::Instant) -> GuardianCleanupReport {
        let Ok(deadline) = self.close_until(deadline) else {
            return GuardianCleanupReport {
                unavailable: true,
                ..Default::default()
            };
        };
        self.shutdown_closed_until(deadline).await
    }

    async fn shutdown_closed_until(&self, deadline: tokio::time::Instant) -> GuardianCleanupReport {
        let constructions = {
            let Ok(state) = self.state.lock() else {
                return GuardianCleanupReport {
                    unavailable: true,
                    ..Default::default()
                };
            };
            state.constructions.clone()
        };
        let outcomes = futures::future::join_all(constructions.iter().map(|construction| async {
            let constructor =
                tokio::time::timeout_at(deadline, construction.completion.clone()).await;
            let Ok(constructor) = constructor else {
                return GuardianTaskOutcome::Failed;
            };
            let startup = if construction.startup.is_empty() {
                true
            } else {
                construction
                    .startup
                    .shutdown_until(deadline)
                    .await
                    .is_complete()
            };
            let child = construction
                .child
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            let child = match child {
                Some(child) => {
                    let (cleanup, auxiliary) = tokio::join!(
                        tokio::time::timeout_at(deadline, child.cleanup.begin_bounded(deadline)),
                        child.cleanup.auxiliary_clean_until(deadline),
                    );
                    cleanup.is_ok_and(|outcome| outcome == GuardianTaskOutcome::Clean) && auxiliary
                }
                None => true,
            };
            if constructor == GuardianConstructionOutcome::Returned && startup && child {
                GuardianTaskOutcome::Clean
            } else if constructor == GuardianConstructionOutcome::Panicked {
                GuardianTaskOutcome::Panicked
            } else {
                GuardianTaskOutcome::Failed
            }
        }))
        .await;
        let report = GuardianCleanupReport {
            complete: outcomes
                .iter()
                .all(|outcome| *outcome == GuardianTaskOutcome::Clean),
            panicked: outcomes
                .iter()
                .any(|outcome| *outcome == GuardianTaskOutcome::Panicked),
            unavailable: false,
        };
        if report.is_clean()
            && let Ok(mut state) = self.state.lock()
        {
            state.constructions.clear();
        }
        report
    }

    async fn shutdown_legacy(&self) -> GuardianCleanupReport {
        if self.close_legacy().is_err() {
            return GuardianCleanupReport {
                unavailable: true,
                ..Default::default()
            };
        }
        self.shutdown_closed_legacy().await
    }

    async fn shutdown_closed_legacy(&self) -> GuardianCleanupReport {
        let constructions = {
            let Ok(state) = self.state.lock() else {
                return GuardianCleanupReport {
                    unavailable: true,
                    ..Default::default()
                };
            };
            state.constructions.clone()
        };
        let outcomes = futures::future::join_all(constructions.iter().map(|construction| async {
            let constructor = construction.completion.clone().await;
            let startup = construction.startup.shutdown_legacy().await;
            let child = construction
                .child
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            match child {
                Some(child) => {
                    let (cleanup, auxiliary) = tokio::join!(
                        child.cleanup.begin_legacy(),
                        child.cleanup.auxiliary_clean(),
                    );
                    if cleanup == GuardianTaskOutcome::Clean && auxiliary {
                        GuardianTaskOutcome::Clean
                    } else {
                        GuardianTaskOutcome::Failed
                    }
                }
                None if constructor == GuardianConstructionOutcome::Returned && startup => {
                    GuardianTaskOutcome::Clean
                }
                _ if constructor == GuardianConstructionOutcome::Panicked => {
                    GuardianTaskOutcome::Panicked
                }
                _ => GuardianTaskOutcome::Failed,
            }
        }))
        .await;
        GuardianCleanupReport {
            complete: outcomes
                .iter()
                .all(|outcome| *outcome == GuardianTaskOutcome::Clean),
            panicked: outcomes
                .iter()
                .any(|outcome| *outcome == GuardianTaskOutcome::Panicked),
            unavailable: false,
        }
    }
}

struct GuardianReviewState {
    prior_review_count: usize,
    last_reviewed_transcript_cursor: Option<GuardianTranscriptCursor>,
    last_committed_fork_snapshot: Option<GuardianReviewForkSnapshot>,
}

fn had_prior_review_context(prompt_mode: &GuardianPromptMode) -> bool {
    matches!(prompt_mode, GuardianPromptMode::Delta { .. })
}

fn token_usage_delta(start: &TokenUsage, end: &TokenUsage) -> TokenUsage {
    TokenUsage {
        input_tokens: (end.input_tokens - start.input_tokens).max(0),
        cached_input_tokens: (end.cached_input_tokens - start.cached_input_tokens).max(0),
        cache_write_input_tokens: (end.cache_write_input_tokens - start.cache_write_input_tokens)
            .max(0),
        output_tokens: (end.output_tokens - start.output_tokens).max(0),
        reasoning_output_tokens: (end.reasoning_output_tokens - start.reasoning_output_tokens)
            .max(0),
        total_tokens: (end.total_tokens - start.total_tokens).max(0),
        codex_rollout_budget_units: None,
    }
}

struct EphemeralReviewCleanup {
    state: Arc<Mutex<GuardianReviewSessionState>>,
    registry: GuardianCleanupRegistry,
    review_session: Option<Arc<GuardianReviewSession>>,
}

#[derive(Clone)]
struct GuardianReviewForkSnapshot {
    initial_history: InitialHistory,
    prior_review_count: usize,
    last_reviewed_transcript_cursor: Option<GuardianTranscriptCursor>,
}

#[derive(Debug, Clone, PartialEq)]
struct GuardianReviewSessionReuseKey {
    // Only include settings that affect spawned-session behavior and parent
    // history rewrites that invalidate existing reviewer context.
    parent_history_version: u64,
    model: Option<String>,
    model_provider_id: String,
    model_provider: ModelProviderInfo,
    model_context_window: Option<i64>,
    model_auto_compact_token_limit: Option<i64>,
    model_auto_compact_token_limit_scope: AutoCompactTokenLimitScope,
    model_reasoning_effort: Option<ReasoningEffortConfig>,
    model_reasoning_summary: Option<ReasoningSummaryConfig>,
    permissions: Permissions,
    developer_instructions: Option<String>,
    base_instructions: Option<String>,
    user_instructions: Option<UserInstructions>,
    compact_prompt: Option<String>,
    cwd: PathUri,
    mcp_servers: Constrained<HashMap<String, McpServerConfig>>,
    codex_linux_sandbox_exe: Option<PathBuf>,
    main_execve_wrapper_exe: Option<PathBuf>,
    zsh_path: Option<PathBuf>,
    features: ManagedFeatures,
    use_experimental_unified_exec_tool: bool,
}

impl GuardianReviewSessionReuseKey {
    fn from_spawn_config(
        spawn_config: &Config,
        user_instructions: Option<UserInstructions>,
        parent_history_version: u64,
    ) -> Self {
        Self {
            parent_history_version: if spawn_config
                .features
                .enabled(Feature::GuardianReuseParentCompaction)
            {
                parent_history_version
            } else {
                0
            },
            model: spawn_config.model.clone(),
            model_provider_id: spawn_config.model_provider_id.clone(),
            model_provider: spawn_config.model_provider.clone(),
            model_context_window: spawn_config.model_context_window,
            model_auto_compact_token_limit: spawn_config.model_auto_compact_token_limit,
            model_auto_compact_token_limit_scope: spawn_config.model_auto_compact_token_limit_scope,
            model_reasoning_effort: spawn_config.model_reasoning_effort.clone(),
            model_reasoning_summary: spawn_config.model_reasoning_summary,
            permissions: spawn_config.permissions.clone(),
            developer_instructions: spawn_config.developer_instructions.clone(),
            base_instructions: spawn_config.base_instructions.clone(),
            user_instructions,
            compact_prompt: spawn_config.compact_prompt.clone(),
            cwd: PathUri::from_abs_path(&spawn_config.cwd),
            mcp_servers: spawn_config.mcp_servers.clone(),
            codex_linux_sandbox_exe: spawn_config.codex_linux_sandbox_exe.clone(),
            main_execve_wrapper_exe: spawn_config.main_execve_wrapper_exe.clone(),
            zsh_path: spawn_config.zsh_path.clone(),
            features: spawn_config.features.clone(),
            use_experimental_unified_exec_tool: spawn_config.use_experimental_unified_exec_tool,
        }
    }
}

fn encrypted_parent_compaction(items: &[ResponseItem]) -> Option<ResponseItem> {
    let item = items.iter().rev().find(|item| {
        matches!(
            item,
            ResponseItem::Compaction { .. } | ResponseItem::ContextCompaction { .. }
        )
    })?;

    match item {
        ResponseItem::Compaction {
            id: Some(_),
            encrypted_content,
            ..
        } if !encrypted_content.is_empty() => Some(item.clone()),
        ResponseItem::ContextCompaction {
            id: Some(_),
            encrypted_content: Some(encrypted_content),
            ..
        } if !encrypted_content.is_empty() => Some(item.clone()),
        _ => None,
    }
}

pub(crate) fn prompt_cache_key_override_for_review_session(
    session_source: &SessionSource,
    parent_thread_id: Option<ThreadId>,
) -> Option<String> {
    let SessionSource::SubAgent(SubAgentSource::Other(name)) = session_source else {
        return None;
    };
    if name != GUARDIAN_REVIEWER_NAME {
        return None;
    }
    let parent_thread_id = parent_thread_id?;
    Some(format!("guardian:{parent_thread_id}"))
}

impl GuardianReviewSession {
    fn shutdown_in_background(self: &Arc<Self>, registry: &GuardianCleanupRegistry) {
        registry.retire_child_in_background(self);
    }

    async fn fork_snapshot(&self) -> Option<GuardianReviewForkSnapshot> {
        self.state.lock().await.last_committed_fork_snapshot.clone()
    }

    async fn refresh_last_committed_fork_snapshot(&self) {
        match load_rollout_items_for_fork(&self.session).await {
            Ok(Some(items)) if !items.is_empty() => {
                let mut state = self.state.lock().await;
                let prior_review_count = state.prior_review_count;
                let last_reviewed_transcript_cursor = state.last_reviewed_transcript_cursor;
                state.last_committed_fork_snapshot = Some(GuardianReviewForkSnapshot {
                    initial_history: InitialHistory::Forked(items),
                    prior_review_count,
                    last_reviewed_transcript_cursor,
                });
            }
            Ok(Some(_)) => {}
            Ok(None) => {}
            Err(err) => {
                warn!("failed to refresh guardian trunk rollout snapshot: {err}");
            }
        }
    }
}

impl EphemeralReviewCleanup {
    fn new(
        state: Arc<Mutex<GuardianReviewSessionState>>,
        registry: GuardianCleanupRegistry,
        review_session: Arc<GuardianReviewSession>,
    ) -> Self {
        Self {
            state,
            registry,
            review_session: Some(review_session),
        }
    }

    fn disarm(&mut self) {
        self.review_session = None;
    }
}

impl Drop for EphemeralReviewCleanup {
    fn drop(&mut self) {
        let Some(review_session) = self.review_session.take() else {
            return;
        };
        let state = Arc::clone(&self.state);
        let registry = self.registry.clone();
        let cleanup = Arc::clone(&review_session.cleanup);
        let review_session = Arc::downgrade(&review_session);
        cleanup.spawn_auxiliary(async move {
            let review_session = {
                let mut state = state.lock().await;
                state
                    .ephemeral_reviews
                    .iter()
                    .position(|active_review| {
                        Weak::ptr_eq(&Arc::downgrade(active_review), &review_session)
                    })
                    .map(|index| state.ephemeral_reviews.swap_remove(index))
            };
            if let Some(review_session) = review_session {
                registry.retire_child_in_background(&review_session);
            }
            GuardianTaskOutcome::Clean
        });
    }
}

impl GuardianReviewSessionManager {
    async fn spawn_owned(
        &self,
        parent_session: &Arc<Session>,
        parent_context: GuardianReviewContext,
        spawn_config: Config,
        reuse_key: GuardianReviewSessionReuseKey,
        cancel_token: CancellationToken,
        parent_compaction: Option<ResponseItem>,
        fork_snapshot: Option<GuardianReviewForkSnapshot>,
    ) -> anyhow::Result<Arc<GuardianReviewSession>> {
        let user_instructions = LoadedUserInstructions {
            instructions: parent_session.user_instructions().await,
            warnings: Vec::new(),
        };
        let parent_session = Arc::downgrade(parent_session);
        self.cleanup
            .register(move |startup_custody| {
                spawn_guardian_review_session(
                    parent_session,
                    user_instructions,
                    parent_context,
                    spawn_config,
                    reuse_key,
                    cancel_token,
                    parent_compaction,
                    fork_snapshot,
                    startup_custody,
                )
            })?
            .await
    }

    pub(crate) fn initialize(
        &self,
        parent_session: Arc<Session>,
        parent_turn: Arc<TurnContext>,
    ) -> BoxFuture<'_, anyhow::Result<()>> {
        // Boxing breaks the Session::new -> Guardian -> Session::new future recursion.
        Box::pin(async move {
            let spawn_config = guardian_review_session_config(&parent_session, &parent_turn)
                .await?
                .spawn_config;
            let parent_history = parent_session.clone_history().await;
            let parent_compaction = spawn_config
                .features
                .enabled(Feature::GuardianReuseParentCompaction)
                .then(|| encrypted_parent_compaction(parent_history.raw_items()))
                .flatten();
            let reuse_key = GuardianReviewSessionReuseKey::from_spawn_config(
                &spawn_config,
                parent_session.user_instructions().await,
                parent_history.history_version(),
            );
            let spawn_cancel_token = self.cancellation_token.child_token();
            let spawn_cancel_guard = spawn_cancel_token.clone().drop_guard();
            let review_session = self
                .spawn_owned(
                    &parent_session,
                    GuardianReviewContext::from(parent_turn),
                    spawn_config,
                    reuse_key,
                    spawn_cancel_token.clone(),
                    parent_compaction,
                    /*fork_snapshot*/ None,
                )
                .await?;
            // A first review or shutdown may win while eager initialization is in flight;
            // install only if neither has happened.
            let mut state = self.state.lock().await;
            if !spawn_cancel_token.is_cancelled() && state.trunk.is_none() {
                state.trunk = Some(review_session);
                drop(spawn_cancel_guard.disarm());
            }
            Ok(())
        })
    }

    pub(crate) async fn trunk_rollout_path(&self) -> Option<PathBuf> {
        let trunk = self.state.lock().await.trunk.clone()?;
        trunk.session.ensure_rollout_materialized().await;
        match trunk.session.current_rollout_path().await {
            Ok(path) => path,
            Err(err) => {
                warn!("failed to resolve guardian trunk rollout path: {err}");
                None
            }
        }
    }

    pub(crate) async fn shutdown(&self) -> GuardianCleanupReport {
        self.cancellation_token.cancel();
        if self.cleanup.close_legacy().is_err() {
            return GuardianCleanupReport {
                unavailable: true,
                ..Default::default()
            };
        }
        {
            let mut state = self.state.lock().await;
            state.trunk.take();
            state.ephemeral_reviews.clear();
        }
        self.cleanup.shutdown_closed_legacy().await
    }

    pub(crate) async fn shutdown_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> GuardianCleanupReport {
        self.cancellation_token.cancel();
        let Ok(deadline) = self.cleanup.close_until(deadline) else {
            return GuardianCleanupReport {
                unavailable: true,
                ..Default::default()
            };
        };
        let clear_state = async {
            if self
                .state
                .try_lock()
                .is_ok_and(|state| state.trunk.is_none() && state.ephemeral_reviews.is_empty())
            {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline) => false,
                mut state = self.state.lock() => {
                    state.trunk.take();
                    state.ephemeral_reviews.clear();
                    true
                }
            }
        };
        let (mut report, state_cleared) =
            tokio::join!(self.cleanup.shutdown_closed_until(deadline), clear_state,);
        report.complete &= state_cleared;
        report
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "review session selection and trunk spawning must stay serialized"
    )]
    pub(super) async fn run_review(
        &self,
        params: GuardianReviewSessionParams,
    ) -> (GuardianReviewSessionOutcome, GuardianReviewAnalyticsResult) {
        let deadline = params.deadline;
        let parent_history = params.parent_session.clone_history().await;
        let parent_compaction = params
            .spawn_config
            .features
            .enabled(Feature::GuardianReuseParentCompaction)
            .then(|| encrypted_parent_compaction(parent_history.raw_items()))
            .flatten();
        let mut next_reuse_key = GuardianReviewSessionReuseKey::from_spawn_config(
            &params.spawn_config,
            params.parent_session.user_instructions().await,
            parent_history.history_version(),
        );
        let mut stale_trunk_to_shutdown = None;
        let mut spawned_trunk = false;
        let trunk_candidate = match run_before_review_deadline(
            deadline,
            params.external_cancel.as_ref(),
            self.state.lock(),
        )
        .await
        {
            Ok(mut state) => {
                if parent_compaction.is_none()
                    && let Some(trunk) = state.trunk.as_ref()
                {
                    // Without a decryptable summary, the existing reviewer may
                    // hold the only remaining authorization or restriction.
                    next_reuse_key.parent_history_version = trunk.reuse_key.parent_history_version;
                }
                if let Some(trunk) = state.trunk.as_ref()
                    && trunk.reuse_key != next_reuse_key
                    && trunk.review_lock.try_acquire().is_ok()
                {
                    stale_trunk_to_shutdown = state.trunk.take();
                }

                if state.trunk.is_none() {
                    let spawn_cancel_token = self.cancellation_token.child_token();
                    let review_session = match run_before_review_deadline_with_cancel(
                        deadline,
                        params.external_cancel.as_ref(),
                        &spawn_cancel_token,
                        Box::pin(self.spawn_owned(
                            &params.parent_session,
                            params.parent_context.clone(),
                            params.spawn_config.clone(),
                            next_reuse_key.clone(),
                            spawn_cancel_token.clone(),
                            parent_compaction.clone(),
                            /*fork_snapshot*/ None,
                        )),
                    )
                    .await
                    {
                        Ok(Ok(review_session)) => review_session,
                        Ok(Err(err)) => {
                            return (
                                GuardianReviewSessionOutcome::PromptBuildFailed(err),
                                GuardianReviewAnalyticsResult::without_session(),
                            );
                        }
                        Err(outcome) => {
                            return (outcome, GuardianReviewAnalyticsResult::without_session());
                        }
                    };
                    state.trunk = Some(Arc::clone(&review_session));
                    spawned_trunk = true;
                }

                state.trunk.as_ref().cloned()
            }
            Err(outcome) => {
                return (outcome, GuardianReviewAnalyticsResult::without_session());
            }
        };

        if let Some(review_session) = stale_trunk_to_shutdown {
            review_session.shutdown_in_background(&self.cleanup);
        }

        let Some(trunk) = trunk_candidate else {
            return (
                GuardianReviewSessionOutcome::Completed(Err(anyhow!(
                    "guardian review session was not available after spawn"
                ))),
                GuardianReviewAnalyticsResult::without_session(),
            );
        };

        if trunk.reuse_key != next_reuse_key {
            return Box::pin(self.run_ephemeral_review(
                params,
                next_reuse_key,
                deadline,
                parent_compaction,
                /*fork_snapshot*/ None,
            ))
            .await;
        }

        let trunk_guard = match trunk.review_lock.try_acquire() {
            Ok(trunk_guard) => trunk_guard,
            Err(_) => {
                return Box::pin(self.run_ephemeral_review(
                    params,
                    next_reuse_key,
                    deadline,
                    parent_compaction,
                    trunk.fork_snapshot().await,
                ))
                .await;
            }
        };

        let guardian_session_kind = if spawned_trunk {
            GuardianReviewSessionKind::TrunkNew
        } else {
            GuardianReviewSessionKind::TrunkReused
        };
        let (outcome, keep_review_session, analytics_result) = Box::pin(run_review_on_session(
            trunk.as_ref(),
            &params,
            guardian_session_kind,
            deadline,
        ))
        .await;
        if keep_review_session && matches!(outcome, GuardianReviewSessionOutcome::Completed(_)) {
            trunk.refresh_last_committed_fork_snapshot().await;
        }
        drop(trunk_guard);

        if keep_review_session {
            (outcome, analytics_result)
        } else {
            if let Some(review_session) = self.remove_trunk_if_current(&trunk).await {
                review_session.shutdown_in_background(&self.cleanup);
            }
            (outcome, analytics_result)
        }
    }

    #[cfg(test)]
    pub(crate) async fn cache_for_test(&self, session: Arc<Session>, io: SessionIo) {
        let reuse_key = GuardianReviewSessionReuseKey::from_spawn_config(
            session.get_config().await.as_ref(),
            session.user_instructions().await,
            session.clone_history().await.history_version(),
        );
        let cancel_token = CancellationToken::new();
        let cleanup = GuardianChildCleanup::new(
            Arc::clone(&session),
            io.retirement_io(),
            cancel_token.clone(),
            DelegateForwardingReceipts::completed(),
        );
        let review_session = Arc::new(GuardianReviewSession {
            reuse_key,
            session,
            io,
            review_lock: Semaphore::new(/*permits*/ 1),
            state: Mutex::new(GuardianReviewState {
                prior_review_count: 0,
                last_reviewed_transcript_cursor: None,
                last_committed_fork_snapshot: None,
            }),
            cleanup,
        });
        self.cleanup
            .retain_child_for_test(Arc::clone(&review_session));
        self.state.lock().await.trunk = Some(review_session);
    }

    #[cfg(test)]
    pub(crate) async fn register_ephemeral_for_test(&self, session: Arc<Session>, io: SessionIo) {
        let reuse_key = GuardianReviewSessionReuseKey::from_spawn_config(
            session.get_config().await.as_ref(),
            session.user_instructions().await,
            session.clone_history().await.history_version(),
        );
        let cancel_token = CancellationToken::new();
        let cleanup = GuardianChildCleanup::new(
            Arc::clone(&session),
            io.retirement_io(),
            cancel_token.clone(),
            DelegateForwardingReceipts::completed(),
        );
        let review_session = Arc::new(GuardianReviewSession {
            reuse_key,
            session,
            io,
            review_lock: Semaphore::new(/*permits*/ 1),
            state: Mutex::new(GuardianReviewState {
                prior_review_count: 0,
                last_reviewed_transcript_cursor: None,
                last_committed_fork_snapshot: None,
            }),
            cleanup,
        });
        self.cleanup
            .retain_child_for_test(Arc::clone(&review_session));
        self.state
            .lock()
            .await
            .ephemeral_reviews
            .push(review_session);
    }

    #[cfg(test)]
    pub(crate) async fn committed_fork_rollout_items_for_test(&self) -> Option<Vec<RolloutItem>> {
        let trunk = self.state.lock().await.trunk.clone()?;
        let state = trunk.state.lock().await;
        let snapshot = state.last_committed_fork_snapshot.as_ref()?;
        match &snapshot.initial_history {
            InitialHistory::Forked(items) => Some(items.clone()),
            InitialHistory::New | InitialHistory::Cleared | InitialHistory::Resumed(_) => None,
        }
    }

    #[cfg(test)]
    pub(crate) async fn send_trunk_event_raw_for_test(&self, event: Event) {
        let trunk = self
            .state
            .lock()
            .await
            .trunk
            .clone()
            .expect("guardian trunk should exist");
        trunk.session.send_event_raw(event).await;
    }

    async fn remove_trunk_if_current(
        &self,
        trunk: &Arc<GuardianReviewSession>,
    ) -> Option<Arc<GuardianReviewSession>> {
        let mut state = self.state.lock().await;
        if state
            .trunk
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, trunk))
        {
            state.trunk.take()
        } else {
            None
        }
    }

    async fn register_active_ephemeral(&self, review_session: Arc<GuardianReviewSession>) {
        self.state
            .lock()
            .await
            .ephemeral_reviews
            .push(review_session);
    }

    async fn take_active_ephemeral(
        &self,
        review_session: &Arc<GuardianReviewSession>,
    ) -> Option<Arc<GuardianReviewSession>> {
        let mut state = self.state.lock().await;
        let ephemeral_review_index = state
            .ephemeral_reviews
            .iter()
            .position(|active_review| Arc::ptr_eq(active_review, review_session))?;
        Some(state.ephemeral_reviews.swap_remove(ephemeral_review_index))
    }

    async fn run_ephemeral_review(
        &self,
        params: GuardianReviewSessionParams,
        reuse_key: GuardianReviewSessionReuseKey,
        deadline: tokio::time::Instant,
        parent_compaction: Option<ResponseItem>,
        fork_snapshot: Option<GuardianReviewForkSnapshot>,
    ) -> (GuardianReviewSessionOutcome, GuardianReviewAnalyticsResult) {
        let spawn_cancel_token = self.cancellation_token.child_token();
        let mut fork_config = params.spawn_config.clone();
        fork_config.ephemeral = true;
        let review_session = match run_before_review_deadline_with_cancel(
            deadline,
            params.external_cancel.as_ref(),
            &spawn_cancel_token,
            Box::pin(self.spawn_owned(
                &params.parent_session,
                params.parent_context.clone(),
                fork_config,
                reuse_key,
                spawn_cancel_token.clone(),
                parent_compaction,
                fork_snapshot,
            )),
        )
        .await
        {
            Ok(Ok(review_session)) => review_session,
            Ok(Err(err)) => {
                return (
                    GuardianReviewSessionOutcome::PromptBuildFailed(err),
                    GuardianReviewAnalyticsResult::without_session(),
                );
            }
            Err(outcome) => {
                return (outcome, GuardianReviewAnalyticsResult::without_session());
            }
        };
        self.register_active_ephemeral(Arc::clone(&review_session))
            .await;
        let mut cleanup = EphemeralReviewCleanup::new(
            Arc::clone(&self.state),
            self.cleanup.clone(),
            Arc::clone(&review_session),
        );

        let (outcome, _, analytics_result) = Box::pin(run_review_on_session(
            review_session.as_ref(),
            &params,
            GuardianReviewSessionKind::EphemeralForked,
            deadline,
        ))
        .await;
        if let Some(review_session) = self.take_active_ephemeral(&review_session).await {
            cleanup.disarm();
            review_session.shutdown_in_background(&self.cleanup);
        }
        (outcome, analytics_result)
    }
}

async fn spawn_guardian_review_session(
    parent_session: Weak<Session>,
    user_instructions: LoadedUserInstructions,
    parent_context: GuardianReviewContext,
    spawn_config: Config,
    reuse_key: GuardianReviewSessionReuseKey,
    cancel_token: CancellationToken,
    parent_compaction: Option<ResponseItem>,
    fork_snapshot: Option<GuardianReviewForkSnapshot>,
    startup_custody: Arc<SessionStartupCustody>,
) -> anyhow::Result<GuardianReviewSession> {
    let (initial_history, prior_review_count, initial_transcript_cursor) = match fork_snapshot {
        Some(fork_snapshot) => (
            Some(fork_snapshot.initial_history),
            fork_snapshot.prior_review_count,
            fork_snapshot.last_reviewed_transcript_cursor,
        ),
        None => (
            parent_compaction
                .map(|item| InitialHistory::Forked(vec![RolloutItem::ResponseItem(item)])),
            0,
            None,
        ),
    };
    let parent = parent_session
        .upgrade()
        .ok_or_else(|| anyhow!("guardian parent session is unavailable"))?;
    let auth_manager = parent.services.auth_manager.clone();
    let models_manager = parent.services.models_manager.clone();
    drop(parent);
    let (session, io, forwarding) = Box::pin(run_codex_thread_interactive_with_custody(
        spawn_config,
        auth_manager,
        models_manager,
        parent_session,
        user_instructions,
        Arc::clone(parent_context.turn()),
        parent_context.environments().clone(),
        cancel_token.clone(),
        SubAgentSource::Other(GUARDIAN_REVIEWER_NAME.to_string()),
        initial_history,
        GitEnrichmentPolicy::Skip,
        codex_sandboxing::WindowsSandboxProxySettingsMode::Preserve,
        Some(startup_custody),
    ))
    .await?;

    let cleanup = GuardianChildCleanup::new(
        Arc::clone(&session),
        io.retirement_io(),
        cancel_token.clone(),
        forwarding,
    );

    Ok(GuardianReviewSession {
        session,
        io,
        reuse_key,
        review_lock: Semaphore::new(/*permits*/ 1),
        state: Mutex::new(GuardianReviewState {
            prior_review_count,
            last_reviewed_transcript_cursor: initial_transcript_cursor,
            last_committed_fork_snapshot: None,
        }),
        cleanup,
    })
}

async fn run_review_on_session(
    review_session: &GuardianReviewSession,
    params: &GuardianReviewSessionParams,
    guardian_session_kind: GuardianReviewSessionKind,
    deadline: tokio::time::Instant,
) -> (
    GuardianReviewSessionOutcome,
    bool,
    GuardianReviewAnalyticsResult,
) {
    let (send_followup_reminder, prompt_mode) = {
        let state = review_session.state.lock().await;

        let send_followup_reminder = state.prior_review_count == 1;
        let prompt_mode = if state.prior_review_count == 0 {
            GuardianPromptMode::Full
        } else if let Some(cursor) = state.last_reviewed_transcript_cursor {
            GuardianPromptMode::Delta { cursor }
        } else {
            GuardianPromptMode::Full
        };

        (send_followup_reminder, prompt_mode)
    };
    let model_info = params
        .parent_session
        .services
        .models_manager
        .get_model_info(
            params.model.as_str(),
            &params.spawn_config.to_models_manager_config(),
        )
        .await;
    let guardian_reasoning_effort = params
        .reasoning_effort
        .clone()
        .or_else(|| model_info.default_reasoning_level.clone());
    let mut analytics_result =
        GuardianReviewAnalyticsResult::from_session(GuardianReviewSessionAnalyticsParams {
            guardian_thread_id: review_session.session.thread_id().to_string(),
            guardian_session_kind,
            guardian_model: params.model.clone(),
            guardian_reasoning_effort: guardian_reasoning_effort.map(|effort| effort.to_string()),
            guardian_default_review_model_id: params.guardian_default_review_model_id.clone(),
            guardian_catalog_contains_auto_review: params.guardian_catalog_contains_auto_review,
            guardian_review_model_overridden: params.guardian_review_model_overridden,
            guardian_review_model_override: params.guardian_review_model_override.clone(),
            guardian_model_provider_id: params.spawn_config.model_provider_id.clone(),
            had_prior_review_context: had_prior_review_context(&prompt_mode),
        });
    if send_followup_reminder {
        append_guardian_followup_reminder(review_session).await;
    }

    let prompt_items = run_before_review_deadline(
        deadline,
        params.external_cancel.as_ref(),
        Box::pin(async {
            params
                .parent_session
                .services
                .network_approval
                .sync_session_approved_hosts_to(&review_session.session.services.network_approval)
                .await;

            build_guardian_prompt_items_with_parent_turn(
                params.parent_session.as_ref(),
                Some(&params.parent_context),
                params.reasons.clone(),
                params.request.clone(),
                prompt_mode,
            )
            .await
        }),
    )
    .await;
    let prompt_items = match prompt_items {
        Ok(prompt_items) => prompt_items,
        Err(outcome) => return (outcome, false, analytics_result),
    };
    let prompt_items = match prompt_items {
        Ok(prompt_items) => prompt_items,
        Err(err) => {
            return (
                GuardianReviewSessionOutcome::PromptBuildFailed(err.into()),
                false,
                analytics_result,
            );
        }
    };
    let reviewed_action_truncated = prompt_items.reviewed_action_truncated;
    let transcript_cursor = prompt_items.transcript_cursor;
    let token_usage_at_review_start = review_session
        .session
        .total_token_usage()
        .await
        .unwrap_or_default();
    let guardian_permission_profile = PermissionProfile::read_only();
    let parent_turn_environments = params.parent_context.environments().to_selections();
    // TODO(anp): Migrate guardian review thread settings to a PathUri fallback cwd so foreign
    // parent environments do not fall back to the host-native config cwd.
    let parent_turn_legacy_fallback_cwd = params
        .parent_context
        .environments()
        .primary()
        .and_then(|environment| environment.cwd().to_abs_path().ok())
        .unwrap_or_else(|| params.parent_context.turn().config.cwd.clone());

    let submission = review_session.io.submit_with_trace(
        Op::UserInput {
            items: prompt_items.items,
            final_output_json_schema: Some(params.schema.clone()),
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings: codex_protocol::protocol::ThreadSettingsOverrides {
                environments: Some(codex_protocol::protocol::TurnEnvironmentSelections::new(
                    parent_turn_legacy_fallback_cwd,
                    parent_turn_environments,
                )),
                approval_policy: Some(AskForApproval::Never),
                sandbox_policy: None,
                permission_profile: Some(guardian_permission_profile),
                summary: Some(params.reasoning_summary),
                personality: params.personality,
                collaboration_mode: Some(codex_protocol::config_types::CollaborationMode {
                    mode: codex_protocol::config_types::ModeKind::Default,
                    settings: codex_protocol::config_types::Settings {
                        model: params.model.clone(),
                        reasoning_effort: params.reasoning_effort.clone(),
                        developer_instructions: None,
                    },
                }),
                ..Default::default()
            },
        },
        /*trace*/ None,
        Some(params.parent_context.turn().sub_id.clone()),
    );
    let submit_result = run_before_review_deadline(
        deadline,
        params.external_cancel.as_ref(),
        Box::pin(submission),
    )
    .await;
    let child_turn_id = match submit_result {
        Ok(Ok(child_turn_id)) => child_turn_id,
        Ok(Err(err)) => {
            return (
                GuardianReviewSessionOutcome::SessionFailed {
                    error: err.into(),
                    error_info: None,
                },
                false,
                analytics_result,
            );
        }
        Err(outcome) => return (outcome, false, analytics_result),
    };
    analytics_result.reviewed_action_truncated = reviewed_action_truncated;

    let outcome = wait_for_guardian_review(
        review_session,
        child_turn_id.as_str(),
        deadline,
        params.external_cancel.as_ref(),
        &mut analytics_result,
    )
    .await;
    if matches!(outcome.0, GuardianReviewSessionOutcome::Completed(_)) {
        if outcome.2
            && let Some(total_token_usage) = review_session.session.total_token_usage().await
        {
            analytics_result.token_usage = Some(token_usage_delta(
                &token_usage_at_review_start,
                &total_token_usage,
            ));
        }
        let mut state = review_session.state.lock().await;
        state.prior_review_count = state.prior_review_count.saturating_add(1);
        state.last_reviewed_transcript_cursor = Some(transcript_cursor);
    }
    (outcome.0, outcome.1, analytics_result)
}

async fn append_guardian_followup_reminder(review_session: &GuardianReviewSession) {
    let reminder: ResponseItem = ContextualUserFragment::into(GuardianFollowupReviewReminder);
    review_session
        .session
        .inject_no_new_turn(vec![reminder], /*current_turn_context*/ None)
        .await;
}

async fn load_rollout_items_for_fork(
    session: &Session,
) -> anyhow::Result<Option<Vec<RolloutItem>>> {
    session.try_ensure_rollout_materialized().await?;
    session.flush_rollout().await?;
    let live_thread = session.live_thread_for_persistence("guardian review fork")?;
    let history = live_thread.load_history(/*include_archived*/ true).await?;
    Ok(Some(history.items))
}

async fn wait_for_guardian_review(
    review_session: &GuardianReviewSession,
    expected_turn_id: &str,
    deadline: tokio::time::Instant,
    external_cancel: Option<&CancellationToken>,
    analytics_result: &mut GuardianReviewAnalyticsResult,
) -> (GuardianReviewSessionOutcome, bool, bool) {
    let timeout = tokio::time::sleep_until(deadline);
    tokio::pin!(timeout);
    let mut last_error: Option<ErrorEvent> = None;

    loop {
        tokio::select! {
            _ = &mut timeout => {
                let keep_review_session = interrupt_and_drain_turn(
                    &review_session.io,
                    expected_turn_id,
                )
                .await
                .is_ok();
                return (GuardianReviewSessionOutcome::TimedOut, keep_review_session, false);
            }
            _ = async {
                if let Some(cancel_token) = external_cancel {
                    cancel_token.cancelled().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                let keep_review_session = interrupt_and_drain_turn(
                    &review_session.io,
                    expected_turn_id,
                )
                .await
                .is_ok();
                return (GuardianReviewSessionOutcome::Aborted, keep_review_session, false);
            }
            event = review_session.io.next_event() => {
                match event {
                    Ok(event) if !event_matches_turn(&event, expected_turn_id) => {}
                    Ok(event) => match event.msg {
                        EventMsg::TurnComplete(turn_complete) => {
                            analytics_result.time_to_first_token_ms = turn_complete
                                .time_to_first_token_ms
                                .and_then(|ms| u64::try_from(ms).ok());
                            if turn_complete.last_agent_message.is_none()
                                && let Some(error) = last_error
                            {
                                return (
                                    GuardianReviewSessionOutcome::SessionFailed {
                                        error: anyhow!(error.message),
                                        error_info: error.codex_error_info,
                                    },
                                    true,
                                    true,
                                );
                            }
                            return (
                                GuardianReviewSessionOutcome::Completed(Ok(turn_complete.last_agent_message)),
                                true,
                                true,
                            );
                        }
                        EventMsg::Error(error) => {
                            last_error = Some(error);
                        }
                        EventMsg::TurnAborted(_) => {
                            return (GuardianReviewSessionOutcome::Aborted, true, false);
                        }
                        _ => {}
                    },
                    Err(err) => {
                        return (
                            GuardianReviewSessionOutcome::Completed(Err(err.into())),
                            false,
                            false,
                        );
                    }
                }
            }
        }
    }
}

fn event_matches_turn(event: &Event, expected_turn_id: &str) -> bool {
    if event.id != expected_turn_id {
        return false;
    }

    match &event.msg {
        EventMsg::TurnComplete(turn_complete) => turn_complete.turn_id == expected_turn_id,
        EventMsg::TurnAborted(turn_aborted) => {
            turn_aborted.turn_id.as_deref() == Some(expected_turn_id)
        }
        _ => true,
    }
}

pub(crate) fn build_guardian_review_session_config(
    parent_config: &Config,
    live_network_config: Option<codex_network_proxy::NetworkProxyConfig>,
    active_model: &str,
    reasoning_effort: Option<codex_protocol::openai_models::ReasoningEffort>,
    model_messages: Option<&ModelMessages>,
) -> anyhow::Result<Config> {
    let mut guardian_config = parent_config.clone();
    guardian_config.model = Some(active_model.to_string());
    guardian_config.model_reasoning_effort = reasoning_effort;
    guardian_config.model_provider.request_max_retries = Some(1);
    guardian_config.model_provider.stream_max_retries = Some(1);
    guardian_config.include_skill_instructions = false;
    guardian_config.memories.use_memories = false;
    guardian_config.memories.dedicated_tools = false;
    let catalog_auto_review = model_messages.and_then(|messages| messages.auto_review.as_ref());
    let tenant_policy_config = parent_config
        .guardian_policy_config
        .as_deref()
        .or_else(|| catalog_auto_review.and_then(|messages| messages.policy.as_deref()))
        .unwrap_or(BUNDLED_GUARDIAN_POLICY);
    let policy_template = catalog_auto_review
        .and_then(|messages| messages.policy_template.as_deref())
        .unwrap_or(BUNDLED_GUARDIAN_POLICY_TEMPLATE);
    guardian_config.base_instructions = Some(guardian_policy_prompt_with_config_and_template(
        tenant_policy_config,
        policy_template,
    ));
    guardian_config.base_instructions_provenance = Some(BaseInstructionsProvenance::Custom);
    guardian_config.notify = None;
    guardian_config.developer_instructions = None;
    guardian_config.permissions.approval_policy = Constrained::allow_only(AskForApproval::Never);
    guardian_config
        .permissions
        .set_permission_profile(PermissionProfile::read_only())
        .map_err(|err| {
            anyhow::anyhow!("guardian review session could not set permission profile: {err}")
        })?;
    guardian_config.include_apps_instructions = false;
    guardian_config
        .mcp_servers
        .set(HashMap::new())
        .map_err(|err| {
            anyhow::anyhow!("guardian review session could not clear MCP servers: {err}")
        })?;
    if let Some(live_network_config) = live_network_config
        && guardian_config.permissions.network.is_some()
    {
        let network_constraints = guardian_config
            .config_layer_stack
            .requirements()
            .network
            .as_ref()
            .map(|network| network.value.clone());
        guardian_config.permissions.network = Some(NetworkProxySpec::from_config_and_constraints(
            live_network_config,
            network_constraints,
            guardian_config.permissions.permission_profile(),
        )?);
    }
    for feature in [
        Feature::Collab,
        Feature::MultiAgentV2,
        Feature::CodexHooks,
        Feature::Apps,
        Feature::Plugins,
        Feature::WebSearchRequest,
        Feature::WebSearchCached,
    ] {
        guardian_config.features.disable(feature).map_err(|err| {
            anyhow::anyhow!(
                "guardian review session could not disable `features.{}`: {err}",
                feature.key()
            )
        })?;
        if guardian_config.features.enabled(feature) {
            warn!(
                "guardian review session could not disable `features.{}`; continuing with the feature enabled",
                feature.key()
            );
        }
    }
    Ok(guardian_config)
}

async fn run_before_review_deadline<T>(
    deadline: tokio::time::Instant,
    external_cancel: Option<&CancellationToken>,
    future: impl Future<Output = T>,
) -> Result<T, GuardianReviewSessionOutcome> {
    tokio::select! {
        _ = tokio::time::sleep_until(deadline) => Err(GuardianReviewSessionOutcome::TimedOut),
        result = future => Ok(result),
        _ = async {
            if let Some(cancel_token) = external_cancel {
                cancel_token.cancelled().await;
            } else {
                std::future::pending::<()>().await;
            }
        } => Err(GuardianReviewSessionOutcome::Aborted),
    }
}

async fn run_before_review_deadline_with_cancel<T>(
    deadline: tokio::time::Instant,
    external_cancel: Option<&CancellationToken>,
    cancel_token: &CancellationToken,
    future: impl Future<Output = T>,
) -> Result<T, GuardianReviewSessionOutcome> {
    let result = run_before_review_deadline(deadline, external_cancel, future).await;
    if result.is_err() {
        cancel_token.cancel();
    }
    result
}

async fn interrupt_and_drain_turn(io: &SessionIo, expected_turn_id: &str) -> anyhow::Result<()> {
    let _ = io.submit(Op::Interrupt).await;

    tokio::time::timeout(GUARDIAN_INTERRUPT_DRAIN_TIMEOUT, async {
        loop {
            let event = io.next_event().await?;
            if event_matches_turn(&event, expected_turn_id)
                && matches!(
                    event.msg,
                    EventMsg::TurnAborted(_) | EventMsg::TurnComplete(_)
                )
            {
                return Ok::<(), anyhow::Error>(());
            }
        }
    })
    .await
    .map_err(|_| anyhow!("timed out draining guardian review session after interrupt"))??;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_protocol::openai_models::AutoReviewMessages;
    use codex_protocol::protocol::AgentStatus;
    use codex_protocol::protocol::ErrorEvent;
    use codex_protocol::protocol::Submission;
    use codex_protocol::protocol::TurnAbortReason;
    use codex_protocol::protocol::TurnAbortedEvent;
    use codex_protocol::protocol::TurnCompleteEvent;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    async fn test_review_session() -> (
        GuardianReviewSession,
        async_channel::Sender<Event>,
        async_channel::Receiver<Submission>,
    ) {
        let (session, _turn, _rx) = crate::session::tests::make_session_and_context_with_rx().await;
        let (tx_sub, rx_sub) = async_channel::bounded(4);
        let (tx_event, rx_event) = async_channel::unbounded();
        let (_agent_status_tx, agent_status) =
            tokio::sync::watch::channel(AgentStatus::PendingInit);
        let reuse_key = GuardianReviewSessionReuseKey::from_spawn_config(
            session.get_config().await.as_ref(),
            session.user_instructions().await,
            session.clone_history().await.history_version(),
        );
        let io = SessionIo {
            tx_sub: tx_sub.into(),
            rx_event,
            agent_status,
            session_loop_termination: crate::session::completed_session_loop_termination(),
        };
        let cancel_token = CancellationToken::new();
        let cleanup = GuardianChildCleanup::new(
            Arc::clone(&session),
            io.retirement_io(),
            cancel_token.clone(),
            DelegateForwardingReceipts::completed(),
        );

        (
            GuardianReviewSession {
                session,
                io,
                reuse_key,
                review_lock: Semaphore::new(/*permits*/ 1),
                state: Mutex::new(GuardianReviewState {
                    prior_review_count: 0,
                    last_reviewed_transcript_cursor: None,
                    last_committed_fork_snapshot: None,
                }),
                cleanup,
            },
            tx_event,
            rx_sub,
        )
    }

    fn turn_complete_event(
        turn_id: &str,
        last_agent_message: Option<&str>,
        time_to_first_token_ms: Option<i64>,
    ) -> Event {
        Event {
            id: turn_id.to_string(),
            msg: EventMsg::TurnComplete(TurnCompleteEvent {
                turn_id: turn_id.to_string(),
                started_at: None,
                last_agent_message: last_agent_message.map(str::to_string),
                error: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms,
            }),
        }
    }

    fn turn_aborted_event(turn_id: &str) -> Event {
        Event {
            id: turn_id.to_string(),
            msg: EventMsg::TurnAborted(TurnAbortedEvent {
                turn_id: Some(turn_id.to_string()),
                started_at: None,
                reason: TurnAbortReason::Interrupted,
                completed_at: None,
                duration_ms: None,
            }),
        }
    }

    async fn test_review_params() -> GuardianReviewSessionParams {
        let (session, turn) = crate::session::tests::make_session_and_context().await;
        let model = turn.model_info.slug.clone();
        let reasoning_effort = turn.reasoning_effort.clone();
        let reasoning_summary = turn.reasoning_summary;
        let personality = turn.personality;
        #[allow(deprecated)]
        let cwd = turn.cwd.clone();
        let spawn_config = build_guardian_review_session_config(
            turn.config.as_ref(),
            /*live_network_config*/ None,
            model.as_str(),
            reasoning_effort.clone(),
            /*model_messages*/ None,
        )
        .expect("guardian config");

        GuardianReviewSessionParams {
            parent_session: Arc::new(session),
            parent_context: GuardianReviewContext::from(Arc::new(turn)),
            spawn_config,
            request: GuardianApprovalRequest::Shell {
                id: "shell-1".to_string(),
                command: vec!["git".to_string(), "status".to_string()],
                cwd,
                sandbox_permissions: crate::sandboxing::SandboxPermissions::UseDefault,
                additional_permissions: None,
                justification: Some("Inspect repo state.".to_string()),
            },
            reasons: ApprovalRequestReasons::default(),
            schema: super::super::prompt::guardian_output_schema(),
            model,
            reasoning_effort,
            guardian_default_review_model_id: "codex-auto-review".to_string(),
            guardian_catalog_contains_auto_review: true,
            guardian_review_model_overridden: false,
            guardian_review_model_override: None,
            reasoning_summary,
            personality,
            external_cancel: None,
            deadline: tokio::time::Instant::now() + Duration::from_secs(30),
        }
    }

    async fn review_with_held_forwarding()
    -> (Arc<GuardianReviewSession>, tokio::sync::oneshot::Sender<()>) {
        let (mut review_session, _tx_event, _rx_sub) = test_review_session().await;
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let events = tokio::spawn(async move {
            let _ = release_rx.await;
        });
        let ops = tokio::spawn(async {});
        review_session.cleanup = GuardianChildCleanup::new(
            Arc::clone(&review_session.session),
            review_session.io.retirement_io(),
            CancellationToken::new(),
            DelegateForwardingReceipts::from_handles(events, ops),
        );
        (Arc::new(review_session), release_tx)
    }

    #[tokio::test]
    async fn bounded_observation_refuses_a_pending_legacy_cleanup_without_polling_it() {
        let (review, release_tx) = review_with_held_forwarding().await;
        let cleanup = Arc::clone(&review.cleanup);
        let legacy = cleanup.begin_legacy();
        tokio::task::yield_now().await;
        assert!(legacy.peek().is_none());

        assert_eq!(
            cleanup
                .begin_bounded(tokio::time::Instant::now() + Duration::from_secs(3))
                .await,
            GuardianTaskOutcome::Failed
        );
        assert!(legacy.peek().is_none());

        let _ = release_tx.send(());
        assert_eq!(legacy.await, GuardianTaskOutcome::Clean);
    }

    #[tokio::test]
    async fn cancelled_guardian_wrappers_are_failures_without_false_panic_attribution() {
        let task = tokio::spawn(async {
            std::future::pending::<()>().await;
            GuardianTaskOutcome::Clean
        });
        task.abort();
        assert_eq!(
            retain_guardian_task(task).await,
            GuardianTaskOutcome::Failed
        );

        let construction = tokio::spawn(async {
            std::future::pending::<()>().await;
            GuardianConstructionOutcome::Returned
        });
        construction.abort();
        assert_eq!(
            retain_guardian_construction(construction).await,
            GuardianConstructionOutcome::Cancelled
        );
    }

    #[tokio::test]
    async fn bounded_observation_replays_a_terminal_legacy_receipt() {
        let (review, release_tx) = review_with_held_forwarding().await;
        let cleanup = Arc::clone(&review.cleanup);
        let legacy = cleanup.begin_legacy();
        let _ = release_tx.send(());
        assert_eq!(legacy.await, GuardianTaskOutcome::Clean);

        assert_eq!(
            cleanup.begin_bounded(tokio::time::Instant::now()).await,
            GuardianTaskOutcome::Clean
        );
    }

    #[tokio::test(start_paused = true)]
    async fn multiple_children_share_one_bound_and_retain_forwarding_after_expiry() {
        let registry = GuardianCleanupRegistry::default();
        let (first, release_first) = review_with_held_forwarding().await;
        let (second, release_second) = review_with_held_forwarding().await;
        let (release_auxiliary, held_auxiliary) = tokio::sync::oneshot::channel();
        first.cleanup.spawn_auxiliary(async move {
            let _ = held_auxiliary.await;
            GuardianTaskOutcome::Clean
        });
        registry.retain_child_for_test(Arc::clone(&first));
        registry.retain_child_for_test(Arc::clone(&second));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let registry_for_shutdown = registry.clone();
        let shutdown =
            tokio::spawn(async move { registry_for_shutdown.shutdown_until(deadline).await });
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        for child in [&first, &second] {
            let state = child.cleanup.state.lock().expect("child cleanup state");
            assert_eq!(
                state.completion_phase,
                Some(GuardianCleanupPhase::DeadlineBound)
            );
            assert_eq!(
                state
                    .retirement
                    .as_ref()
                    .expect("retirement ticket")
                    .deadline(),
                deadline
            );
        }

        tokio::time::advance(Duration::from_secs(2)).await;
        let report = shutdown.await.expect("bounded guardian shutdown");
        assert!(!report.is_clean());
        assert!(first.cleanup.forwarding.events.peek().is_none());
        assert!(second.cleanup.forwarding.events.peek().is_none());
        {
            let state = first.cleanup.state.lock().expect("child cleanup state");
            assert!(
                !state.auxiliary_open,
                "primary failure must not skip auxiliary admission closure"
            );
            assert!(state.auxiliary[0].peek().is_none());
        }
        let _ = release_first.send(());
        let _ = release_second.send(());
        let _ = release_auxiliary.send(());
    }

    #[tokio::test]
    async fn failed_child_cleanup_propagates_guardian_failed_to_parent_cleanup() {
        let (parent, _turn) = crate::session::tests::make_session_and_context().await;
        let parent = Arc::new(parent);
        let (child, _turn) = crate::session::tests::make_session_and_context().await;
        let child = Arc::new(child);
        let (tx_sub, _rx_sub) = async_channel::bounded(4);
        let (_tx_event, rx_event) = async_channel::unbounded();
        let child_loop = tokio::spawn(async move {
            panic!("child loop failure fixture");
        });
        let child_io = SessionIo {
            tx_sub: tx_sub.into(),
            rx_event,
            agent_status: tokio::sync::watch::channel(AgentStatus::PendingInit).1,
            session_loop_termination: crate::session::session_loop_termination_from_handle(
                child_loop,
            ),
        };
        parent
            .guardian_review_session
            .cache_for_test(child, child_io)
            .await;
        let owner = parent.cleanup_owner();
        owner
            .bind_deadline(tokio::time::Instant::now() + Duration::from_secs(3))
            .expect("bind parent cleanup deadline");

        assert_eq!(
            owner.observe(parent).await,
            crate::session::retirement::CleanupExecution::GuardianFailed
        );
    }

    #[tokio::test]
    async fn failed_child_cleanup_with_a_normal_loop_propagates_guardian_failed() {
        let (parent, _turn) = crate::session::tests::make_session_and_context().await;
        let parent = Arc::new(parent);
        let (child, _turn) = crate::session::tests::make_session_and_context().await;
        let child = Arc::new(child);
        assert!(matches!(
            child
                .task_joins
                .shutdown_until(tokio::time::Instant::now())
                .await,
            crate::tasks::TaskJoinOutcome::Complete { .. }
        ));
        child.spawn_startup_auxiliary(async {}).await;
        let (tx_sub, rx_sub) = async_channel::bounded::<Submission>(4);
        let (_tx_event, rx_event) = async_channel::unbounded();
        let child_loop = tokio::spawn(async move {
            let shutdown = rx_sub.recv().await.expect("child shutdown submission");
            assert_eq!(shutdown.op, Op::Shutdown);
        });
        let child_io = SessionIo {
            tx_sub: tx_sub.into(),
            rx_event,
            agent_status: tokio::sync::watch::channel(AgentStatus::PendingInit).1,
            session_loop_termination: crate::session::session_loop_termination_from_handle(
                child_loop,
            ),
        };
        parent
            .guardian_review_session
            .cache_for_test(child, child_io)
            .await;
        let owner = parent.cleanup_owner();
        owner
            .bind_deadline(tokio::time::Instant::now() + Duration::from_secs(3))
            .expect("bind parent cleanup deadline");

        assert_eq!(
            owner.observe(parent).await,
            crate::session::retirement::CleanupExecution::GuardianFailed
        );
    }

    #[tokio::test]
    async fn constructor_custody_survives_a_dropped_caller_and_close_rejects_late_birth() {
        let registry = GuardianCleanupRegistry::default();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let caller = registry
            .register(move |_startup_custody| async move {
                let _ = started_tx.send(());
                let _ = release_rx.await;
                Err(anyhow!("test construction ended without a child"))
            })
            .expect("register construction");
        drop(caller);
        started_rx.await.expect("construction should start");

        let registry_for_shutdown = registry.clone();
        let shutdown = tokio::spawn(async move {
            registry_for_shutdown
                .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(3))
                .await
        });
        tokio::task::yield_now().await;
        assert!(!shutdown.is_finished());
        assert!(
            registry
                .register(|_startup_custody| async move {
                    Err(anyhow!("late construction must not run"))
                })
                .is_err()
        );

        let _ = release_tx.send(());
        assert!(shutdown.await.expect("shutdown task").is_clean());
    }

    #[tokio::test(start_paused = true)]
    async fn manager_shutdown_closes_registry_before_waiting_for_review_state() {
        let manager = Arc::new(GuardianReviewSessionManager::default());
        let state_guard = manager.state.lock().await;
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let caller = manager
            .cleanup
            .register(move |_startup_custody| async move {
                let _ = started_tx.send(());
                let _ = release_rx.await;
                Err(anyhow!("fixture constructor released"))
            })
            .expect("register construction");
        drop(caller);
        started_rx.await.expect("constructor started");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let manager_for_shutdown = Arc::clone(&manager);
        let shutdown =
            tokio::spawn(async move { manager_for_shutdown.shutdown_until(deadline).await });
        tokio::task::yield_now().await;
        assert!(
            manager
                .cleanup
                .state
                .lock()
                .expect("cleanup registry")
                .closed
        );
        assert!(
            manager
                .cleanup
                .register(|_startup_custody| async move {
                    Err(anyhow!("late construction must not run"))
                })
                .is_err(),
            "shutdown entry must reject births before the async state wait"
        );

        tokio::time::advance(Duration::from_secs(2)).await;
        let report = shutdown.await.expect("bounded shutdown");
        assert!(!report.is_clean());
        drop(state_guard);
        let _ = release_tx.send(());
    }

    #[tokio::test(start_paused = true)]
    async fn manager_shutdown_replays_observed_empty_state_after_deadline() {
        let manager = GuardianReviewSessionManager::default();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        assert!(manager.shutdown_until(deadline).await.is_clean());

        tokio::time::advance(Duration::from_secs(3)).await;
        assert!(
            manager.shutdown_until(deadline).await.is_clean(),
            "authoritative empty state and clean registry evidence must replay"
        );
    }

    #[tokio::test]
    async fn real_construction_cancellation_cleans_the_session_retained_at_birth() {
        let params = test_review_params().await;
        // A production manager is owned by its parent Session. Keep that
        // external root alive while testing ownership of the unpublished child.
        let parent_root = Arc::clone(&params.parent_session);
        let manager = Arc::new(GuardianReviewSessionManager::default());
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        manager
            .cleanup
            .pause_next_construction_after_retain_for_test(
                Arc::clone(&entered),
                Arc::clone(&release),
            );
        let manager_for_initialize = Arc::clone(&manager);
        let initialize = tokio::spawn(async move {
            manager_for_initialize
                .initialize(
                    params.parent_session,
                    Arc::clone(params.parent_context.turn()),
                )
                .await
        });
        entered.notified().await;
        let construction = manager
            .cleanup
            .state
            .lock()
            .expect("cleanup registry")
            .constructions
            .first()
            .cloned()
            .expect("construction reservation");
        assert!(
            !construction.startup.is_empty(),
            "Session::new must retain the child before the caller is cancelled"
        );

        initialize.abort();
        assert!(
            initialize
                .await
                .expect_err("initialize caller should be cancelled")
                .is_cancelled()
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let manager_for_shutdown = Arc::clone(&manager);
        let shutdown =
            tokio::spawn(async move { manager_for_shutdown.shutdown_until(deadline).await });
        tokio::task::yield_now().await;
        assert!(construction.completion.peek().is_none());
        assert!(!shutdown.is_finished());
        release.notify_one();
        let report = shutdown.await.expect("manager shutdown");
        let constructor = construction.completion.peek().copied();

        assert!(
            report.is_clean(),
            "constructor={constructor:?}, report={report:?}"
        );
        assert_eq!(
            constructor,
            Some(GuardianConstructionOutcome::Returned),
            "manager cleanup must observe constructor terminal before startup cleanup"
        );
        assert!(construction.startup.is_empty());
        drop(parent_root);
    }

    #[tokio::test]
    async fn caller_cancellation_does_not_drop_pre_session_persistence_or_constructor() {
        let params = test_review_params().await;
        let parent_root = Arc::clone(&params.parent_session);
        let manager = Arc::new(GuardianReviewSessionManager::default());
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        manager
            .cleanup
            .pause_next_construction_after_persistence_for_test(
                Arc::clone(&entered),
                Arc::clone(&release),
            );
        let manager_for_initialize = Arc::clone(&manager);
        let initialize = tokio::spawn(async move {
            manager_for_initialize
                .initialize(
                    params.parent_session,
                    Arc::clone(params.parent_context.turn()),
                )
                .await
        });
        entered.notified().await;
        let construction = manager
            .cleanup
            .state
            .lock()
            .expect("cleanup registry")
            .constructions
            .first()
            .cloned()
            .expect("construction reservation");
        assert!(construction.startup.has_pre_session_persistence_for_test());

        initialize.abort();
        assert!(
            initialize
                .await
                .expect_err("initialize caller should be cancelled")
                .is_cancelled()
        );
        let manager_for_shutdown = Arc::clone(&manager);
        let shutdown = tokio::spawn(async move {
            manager_for_shutdown
                .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(20))
                .await
        });
        tokio::task::yield_now().await;
        assert!(
            construction.completion.peek().is_none(),
            "caller cancellation must not terminate the owned constructor"
        );
        assert!(!shutdown.is_finished());

        release.notify_one();
        let report = shutdown.await.expect("manager shutdown");
        assert!(report.is_clean(), "{report:?}");
        assert_eq!(
            construction.completion.peek().copied(),
            Some(GuardianConstructionOutcome::Returned)
        );
        drop(parent_root);
    }

    #[tokio::test]
    async fn auxiliary_snapshot_closes_admission_before_reporting_clean() {
        let (review_session, _tx_event, _rx_sub) = test_review_session().await;
        let cleanup = Arc::clone(&review_session.cleanup);
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        cleanup.spawn_auxiliary(async move {
            let _ = release_rx.await;
            GuardianTaskOutcome::Clean
        });

        let cleanup_for_observer = Arc::clone(&cleanup);
        let observer = tokio::spawn(async move {
            cleanup_for_observer
                .auxiliary_clean_until(tokio::time::Instant::now() + Duration::from_secs(3))
                .await
        });
        for _ in 0..10 {
            if !cleanup
                .state
                .lock()
                .expect("child cleanup state")
                .auxiliary_open
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            !cleanup
                .state
                .lock()
                .expect("child cleanup state")
                .auxiliary_open,
            "observer must close admission before the late-wrapper assertion"
        );
        assert!(!observer.is_finished());

        let late_wrapper_ran = Arc::new(AtomicBool::new(false));
        let late_wrapper_ran_in_task = Arc::clone(&late_wrapper_ran);
        cleanup.spawn_auxiliary(async move {
            late_wrapper_ran_in_task.store(true, Ordering::SeqCst);
            GuardianTaskOutcome::Clean
        });
        let _ = release_tx.send(());

        assert!(observer.await.expect("auxiliary observer"));
        tokio::task::yield_now().await;
        assert!(!late_wrapper_ran.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn removed_real_guardian_child_remains_owned_through_legacy_shutdown() {
        let params = test_review_params().await;
        let manager = GuardianReviewSessionManager::default();
        manager
            .initialize(
                Arc::clone(&params.parent_session),
                Arc::clone(params.parent_context.turn()),
            )
            .await
            .expect("initialize Guardian session");
        let removed = manager
            .state
            .lock()
            .await
            .trunk
            .take()
            .expect("constructed Guardian child");
        removed.shutdown_in_background(&manager.cleanup);
        drop(removed);

        assert_eq!(
            manager
                .cleanup
                .state
                .lock()
                .expect("cleanup registry")
                .constructions
                .len(),
            1,
            "active-list removal must not remove cleanup custody"
        );
        let report = manager.shutdown().await;

        assert!(report.is_clean(), "{report:?}");
    }

    #[tokio::test]
    async fn background_retirement_retains_pending_child_then_compacts_terminal_child() {
        let registry = GuardianCleanupRegistry::default();
        let (child, release) = review_with_held_forwarding().await;
        registry.retain_child_for_test(Arc::clone(&child));

        registry.retire_child_in_background(&child);
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            registry
                .state
                .lock()
                .expect("cleanup registry")
                .constructions
                .len(),
            1,
            "pending forwarding work must keep the removed child in custody"
        );

        let _ = release.send(());
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if registry
                    .state
                    .lock()
                    .expect("cleanup registry")
                    .constructions
                    .is_empty()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("terminal guardian child should release registry custody");
    }

    #[tokio::test]
    async fn spawned_guardian_session_preserves_windows_sandbox_proxy_settings() {
        let params = test_review_params().await;
        let manager = GuardianReviewSessionManager::default();
        manager
            .initialize(
                params.parent_session,
                Arc::clone(params.parent_context.turn()),
            )
            .await
            .expect("initialize Guardian session");
        let mode = manager
            .state
            .lock()
            .await
            .trunk
            .as_ref()
            .expect("Guardian session")
            .session
            .windows_sandbox_proxy_settings_mode;

        let child = manager
            .state
            .lock()
            .await
            .trunk
            .as_ref()
            .expect("Guardian session")
            .session
            .clone();

        assert_eq!(
            mode,
            codex_sandboxing::WindowsSandboxProxySettingsMode::Preserve
        );
        assert!(
            !child.failed_initialization_persistence_for_test(),
            "successful construction must restore normal persistence shutdown"
        );
        manager.shutdown().await;
    }

    #[tokio::test]
    async fn guardian_review_session_config_change_invalidates_cached_session() {
        let parent_config = crate::config::test_config().await;
        let cached_spawn_config = build_guardian_review_session_config(
            &parent_config,
            /*live_network_config*/ None,
            "active-model",
            /*reasoning_effort*/ None,
            /*model_messages*/ None,
        )
        .expect("cached guardian config");
        let cached_reuse_key = GuardianReviewSessionReuseKey::from_spawn_config(
            &cached_spawn_config,
            /*user_instructions*/ None,
            /*parent_history_version*/ 0,
        );

        let mut changed_parent_config = parent_config;
        changed_parent_config.model_provider.base_url =
            Some("https://guardian.example.invalid/v1".to_string());
        let next_spawn_config = build_guardian_review_session_config(
            &changed_parent_config,
            /*live_network_config*/ None,
            "active-model",
            /*reasoning_effort*/ None,
            /*model_messages*/ None,
        )
        .expect("next guardian config");
        let next_reuse_key = GuardianReviewSessionReuseKey::from_spawn_config(
            &next_spawn_config,
            /*user_instructions*/ None,
            /*parent_history_version*/ 0,
        );

        assert_eq!(
            cached_reuse_key.cwd,
            PathUri::from_abs_path(&cached_spawn_config.cwd)
        );
        assert_ne!(cached_reuse_key, next_reuse_key);
        assert_eq!(
            cached_reuse_key,
            GuardianReviewSessionReuseKey::from_spawn_config(
                &cached_spawn_config,
                /*user_instructions*/ None,
                /*parent_history_version*/ 0,
            )
        );

        assert_eq!(
            cached_reuse_key,
            GuardianReviewSessionReuseKey::from_spawn_config(
                &cached_spawn_config,
                /*user_instructions*/ None,
                /*parent_history_version*/ 1,
            )
        );

        let mut compaction_enabled_config = cached_spawn_config;
        compaction_enabled_config
            .features
            .enable(Feature::GuardianReuseParentCompaction)
            .expect("Guardian parent-compaction reuse should be configurable");
        assert_ne!(
            GuardianReviewSessionReuseKey::from_spawn_config(
                &compaction_enabled_config,
                /*user_instructions*/ None,
                /*parent_history_version*/ 0,
            ),
            GuardianReviewSessionReuseKey::from_spawn_config(
                &compaction_enabled_config,
                /*user_instructions*/ None,
                /*parent_history_version*/ 1,
            )
        );
    }

    #[test]
    fn encrypted_parent_compaction_requires_original_item_id() {
        let item = ResponseItem::Compaction {
            id: Some(codex_protocol::ResponseItemId::from_server(
                "cmp_guardian_parent_summary".to_string(),
            )),
            encrypted_content: "encrypted guardian parent summary".to_string(),
            internal_chat_message_metadata_passthrough: None,
        };

        assert_eq!(
            encrypted_parent_compaction(std::slice::from_ref(&item)),
            Some(item)
        );
        assert_eq!(
            encrypted_parent_compaction(&[ResponseItem::Compaction {
                id: None,
                encrypted_content: "encrypted guardian parent summary".to_string(),
                internal_chat_message_metadata_passthrough: None,
            }]),
            None
        );
    }

    #[tokio::test]
    async fn guardian_prompt_cache_key_is_scoped_to_parent_thread() {
        let session_source =
            SessionSource::SubAgent(SubAgentSource::Other(GUARDIAN_REVIEWER_NAME.to_string()));
        let parent_thread_id = ThreadId::new();
        let key =
            prompt_cache_key_override_for_review_session(&session_source, Some(parent_thread_id))
                .expect("guardian prompt cache key");

        assert_eq!(key, format!("guardian:{parent_thread_id}"));
        assert!(
            key.len() <= 64,
            "guardian prompt cache key should fit the Responses API limit"
        );
        assert_eq!(
            key,
            prompt_cache_key_override_for_review_session(&session_source, Some(parent_thread_id))
                .expect("same guardian prompt cache key")
        );
        assert_ne!(
            key,
            prompt_cache_key_override_for_review_session(&session_source, Some(ThreadId::new()))
                .expect("different parent guardian prompt cache key")
        );
        assert_eq!(
            None,
            prompt_cache_key_override_for_review_session(
                &SessionSource::Cli,
                Some(parent_thread_id)
            )
        );
        assert_eq!(
            None,
            prompt_cache_key_override_for_review_session(
                &session_source,
                /*parent_thread_id*/ None
            )
        );
    }

    #[tokio::test]
    async fn guardian_review_session_compact_scope_change_invalidates_cached_session() {
        let parent_config = crate::config::test_config().await;
        let cached_spawn_config = build_guardian_review_session_config(
            &parent_config,
            /*live_network_config*/ None,
            "active-model",
            /*reasoning_effort*/ None,
            /*model_messages*/ None,
        )
        .expect("cached guardian config");
        let cached_reuse_key = GuardianReviewSessionReuseKey::from_spawn_config(
            &cached_spawn_config,
            /*user_instructions*/ None,
            /*parent_history_version*/ 0,
        );

        let mut changed_parent_config = parent_config;
        changed_parent_config.model_auto_compact_token_limit_scope =
            AutoCompactTokenLimitScope::BodyAfterPrefix;
        let next_spawn_config = build_guardian_review_session_config(
            &changed_parent_config,
            /*live_network_config*/ None,
            "active-model",
            /*reasoning_effort*/ None,
            /*model_messages*/ None,
        )
        .expect("next guardian config");
        let next_reuse_key = GuardianReviewSessionReuseKey::from_spawn_config(
            &next_spawn_config,
            /*user_instructions*/ None,
            /*parent_history_version*/ 0,
        );

        assert_ne!(cached_reuse_key, next_reuse_key);
    }

    #[tokio::test]
    async fn guardian_review_session_config_disables_hooks() {
        let mut parent_config = crate::config::test_config().await;
        parent_config
            .features
            .enable(Feature::CodexHooks)
            .expect("enable hooks on parent config");

        let guardian_config = build_guardian_review_session_config(
            &parent_config,
            /*live_network_config*/ None,
            "active-model",
            /*reasoning_effort*/ None,
            /*model_messages*/ None,
        )
        .expect("guardian config");

        assert!(!guardian_config.features.enabled(Feature::CodexHooks));
    }

    #[tokio::test]
    async fn guardian_review_session_config_disables_skill_instructions() {
        let mut parent_config = crate::config::test_config().await;
        parent_config.include_skill_instructions = true;

        let guardian_config = build_guardian_review_session_config(
            &parent_config,
            /*live_network_config*/ None,
            "active-model",
            /*reasoning_effort*/ None,
            /*model_messages*/ None,
        )
        .expect("guardian config");

        assert!(!guardian_config.include_skill_instructions);
    }

    #[tokio::test]
    async fn guardian_review_session_config_prefers_managed_policy_and_uses_catalog_template() {
        let mut parent_config = crate::config::test_config().await;
        let managed_policy = "Use the managed Guardian policy.";
        let catalog_template = "Catalog Guardian template:\n{{ tenant_policy_config }}";
        parent_config.guardian_policy_config = Some(managed_policy.to_string());
        let model_messages = ModelMessages {
            instructions_template: None,
            instructions_variables: None,
            approvals: None,
            collaboration_modes: None,
            auto_review: Some(AutoReviewMessages {
                policy: Some("Use the catalog Guardian policy.".to_string()),
                policy_template: Some(catalog_template.to_string()),
            }),
            permissions: None,
            token_budget: None,
        };

        let guardian_config = build_guardian_review_session_config(
            &parent_config,
            /*live_network_config*/ None,
            "active-model",
            /*reasoning_effort*/ None,
            Some(&model_messages),
        )
        .expect("guardian config");

        assert_eq!(
            guardian_config.base_instructions,
            Some(guardian_policy_prompt_with_config_and_template(
                managed_policy,
                catalog_template,
            ))
        );
    }

    #[tokio::test]
    async fn guardian_review_session_config_preserves_explicit_empty_catalog_policy() {
        let parent_config = crate::config::test_config().await;
        let model_messages = ModelMessages {
            instructions_template: None,
            instructions_variables: None,
            approvals: None,
            collaboration_modes: None,
            auto_review: Some(AutoReviewMessages {
                policy: Some(String::new()),
                policy_template: None,
            }),
            permissions: None,
            token_budget: None,
        };

        let guardian_config = build_guardian_review_session_config(
            &parent_config,
            /*live_network_config*/ None,
            "active-model",
            /*reasoning_effort*/ None,
            Some(&model_messages),
        )
        .expect("guardian config");

        assert_eq!(
            guardian_config.base_instructions,
            Some(guardian_policy_prompt_with_config_and_template(
                "",
                BUNDLED_GUARDIAN_POLICY_TEMPLATE,
            ))
        );
        assert_ne!(
            guardian_config.base_instructions,
            Some(guardian_policy_prompt_with_config_and_template(
                BUNDLED_GUARDIAN_POLICY,
                BUNDLED_GUARDIAN_POLICY_TEMPLATE,
            ))
        );
    }

    #[tokio::test]
    async fn guardian_review_session_config_preserves_explicit_empty_catalog_template() {
        let parent_config = crate::config::test_config().await;
        let catalog_policy = "Use the catalog Guardian policy.";
        let model_messages = ModelMessages {
            instructions_template: None,
            instructions_variables: None,
            approvals: None,
            collaboration_modes: None,
            auto_review: Some(AutoReviewMessages {
                policy: Some(catalog_policy.to_string()),
                policy_template: Some(String::new()),
            }),
            permissions: None,
            token_budget: None,
        };

        let guardian_config = build_guardian_review_session_config(
            &parent_config,
            /*live_network_config*/ None,
            "active-model",
            /*reasoning_effort*/ None,
            Some(&model_messages),
        )
        .expect("guardian config");

        assert_eq!(
            guardian_config.base_instructions,
            Some(guardian_policy_prompt_with_config_and_template(
                catalog_policy,
                "",
            ))
        );
        assert_ne!(
            guardian_config.base_instructions,
            Some(guardian_policy_prompt_with_config_and_template(
                catalog_policy,
                BUNDLED_GUARDIAN_POLICY_TEMPLATE,
            ))
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn run_before_review_deadline_times_out_before_future_completes() {
        let outcome = run_before_review_deadline(
            tokio::time::Instant::now() + Duration::from_millis(10),
            /*external_cancel*/ None,
            async {
                tokio::time::sleep(Duration::from_millis(50)).await;
            },
        )
        .await;

        assert!(matches!(
            outcome,
            Err(GuardianReviewSessionOutcome::TimedOut)
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn run_before_review_deadline_aborts_when_cancelled() {
        let cancel_token = CancellationToken::new();
        let canceller = cancel_token.clone();
        drop(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            canceller.cancel();
        }));

        let outcome = run_before_review_deadline(
            tokio::time::Instant::now() + Duration::from_secs(1),
            Some(&cancel_token),
            std::future::pending::<()>(),
        )
        .await;

        assert!(matches!(
            outcome,
            Err(GuardianReviewSessionOutcome::Aborted)
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn run_before_review_deadline_with_cancel_cancels_token_on_timeout() {
        let cancel_token = CancellationToken::new();

        let outcome = run_before_review_deadline_with_cancel(
            tokio::time::Instant::now() + Duration::from_millis(10),
            /*external_cancel*/ None,
            &cancel_token,
            async {
                tokio::time::sleep(Duration::from_millis(50)).await;
            },
        )
        .await;

        assert!(matches!(
            outcome,
            Err(GuardianReviewSessionOutcome::TimedOut)
        ));
        assert!(cancel_token.is_cancelled());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn run_before_review_deadline_with_cancel_cancels_token_on_abort() {
        let external_cancel = CancellationToken::new();
        let external_canceller = external_cancel.clone();
        let cancel_token = CancellationToken::new();
        drop(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            external_canceller.cancel();
        }));

        let outcome = run_before_review_deadline_with_cancel(
            tokio::time::Instant::now() + Duration::from_secs(1),
            Some(&external_cancel),
            &cancel_token,
            std::future::pending::<()>(),
        )
        .await;

        assert!(matches!(
            outcome,
            Err(GuardianReviewSessionOutcome::Aborted)
        ));
        assert!(cancel_token.is_cancelled());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn run_before_review_deadline_with_cancel_preserves_token_on_success() {
        let cancel_token = CancellationToken::new();

        let outcome = run_before_review_deadline_with_cancel(
            tokio::time::Instant::now() + Duration::from_secs(1),
            /*external_cancel*/ None,
            &cancel_token,
            async { 42usize },
        )
        .await;

        assert_eq!(outcome.unwrap(), 42);
        assert!(!cancel_token.is_cancelled());
    }

    #[test]
    fn had_prior_review_context_tracks_prompt_mode() {
        assert!(!had_prior_review_context(&GuardianPromptMode::Full));
        assert!(had_prior_review_context(&GuardianPromptMode::Delta {
            cursor: GuardianTranscriptCursor {
                parent_history_version: 7,
                transcript_entry_count: 42,
            }
        }));
    }

    #[test]
    fn token_usage_delta_never_reports_negative_usage() {
        let start = TokenUsage {
            input_tokens: 10,
            cached_input_tokens: 8,
            cache_write_input_tokens: 8,
            output_tokens: 6,
            reasoning_output_tokens: 4,
            total_tokens: 28,
            codex_rollout_budget_units: None,
        };
        let end = TokenUsage {
            input_tokens: 15,
            cached_input_tokens: 7,
            cache_write_input_tokens: 7,
            output_tokens: 10,
            reasoning_output_tokens: 2,
            total_tokens: 34,
            codex_rollout_budget_units: None,
        };

        assert_eq!(
            token_usage_delta(&start, &end),
            TokenUsage {
                input_tokens: 5,
                cached_input_tokens: 0,
                cache_write_input_tokens: 0,
                output_tokens: 4,
                reasoning_output_tokens: 0,
                total_tokens: 6,
                codex_rollout_budget_units: None,
            }
        );
    }

    #[tokio::test]
    async fn run_review_on_reused_session_waits_for_submitted_turn() {
        let (review_session, tx_event, rx_sub) = test_review_session().await;
        {
            let mut state = review_session.state.lock().await;
            state.prior_review_count = 1;
            state.last_reviewed_transcript_cursor = Some(GuardianTranscriptCursor {
                parent_history_version: 0,
                transcript_entry_count: 0,
            });
        }
        let params = test_review_params().await;

        let review = tokio::spawn(async move {
            run_review_on_session(
                &review_session,
                &params,
                GuardianReviewSessionKind::TrunkReused,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
        });
        let submission = rx_sub.recv().await.expect("guardian submission");
        tx_event
            .send(turn_complete_event("prior-turn", Some("stale"), Some(9)))
            .await
            .expect("queue prior turn completion");
        tx_event
            .send(turn_complete_event(
                submission.id.as_str(),
                Some("fresh"),
                Some(42),
            ))
            .await
            .expect("queue submitted turn completion");

        let (outcome, keep_review_session, analytics_result) =
            review.await.expect("review task should complete");
        let GuardianReviewSessionOutcome::Completed(Ok(last_agent_message)) = outcome else {
            panic!("expected submitted turn completion");
        };
        assert_eq!(last_agent_message.as_deref(), Some("fresh"));
        assert_eq!(analytics_result.time_to_first_token_ms, Some(42));
        assert!(keep_review_session);
    }

    #[tokio::test]
    async fn run_review_removes_trunk_when_event_stream_is_broken() {
        let (mut review_session, tx_event, _rx_sub) = test_review_session().await;
        let params = test_review_params().await;
        review_session.reuse_key = GuardianReviewSessionReuseKey::from_spawn_config(
            &params.spawn_config,
            params.parent_session.user_instructions().await,
            params
                .parent_session
                .clone_history()
                .await
                .history_version(),
        );
        let manager = GuardianReviewSessionManager {
            state: Arc::new(Mutex::new(GuardianReviewSessionState {
                trunk: Some(Arc::new(review_session)),
                ephemeral_reviews: Vec::new(),
            })),
            ..Default::default()
        };
        drop(tx_event);

        let (outcome, _) = manager.run_review(params).await;

        assert!(matches!(
            outcome,
            GuardianReviewSessionOutcome::Completed(Err(_))
        ));
        assert!(manager.state.lock().await.trunk.is_none());
    }

    #[tokio::test]
    async fn wait_for_guardian_review_ignores_prior_turn_completion() {
        let (review_session, tx_event, _rx_sub) = test_review_session().await;
        tx_event
            .send(turn_complete_event("prior-turn", Some("stale"), Some(9)))
            .await
            .expect("queue prior turn completion");
        tx_event
            .send(turn_complete_event("current-turn", Some("fresh"), Some(42)))
            .await
            .expect("queue current turn completion");

        let mut analytics_result = GuardianReviewAnalyticsResult::without_session();
        let (outcome, keep_review_session, capture_token_usage) = wait_for_guardian_review(
            &review_session,
            "current-turn",
            tokio::time::Instant::now() + Duration::from_secs(1),
            /*external_cancel*/ None,
            &mut analytics_result,
        )
        .await;

        let GuardianReviewSessionOutcome::Completed(Ok(last_agent_message)) = outcome else {
            panic!("expected current turn completion");
        };
        assert_eq!(last_agent_message.as_deref(), Some("fresh"));
        assert_eq!(analytics_result.time_to_first_token_ms, Some(42));
        assert!(keep_review_session);
        assert!(capture_token_usage);
    }

    #[tokio::test]
    async fn wait_for_guardian_review_ignores_prior_turn_errors() {
        let (review_session, tx_event, _rx_sub) = test_review_session().await;
        tx_event
            .send(Event {
                id: "prior-turn".to_string(),
                msg: EventMsg::Error(ErrorEvent {
                    message: "stale guardian error".to_string(),
                    codex_error_info: None,
                }),
            })
            .await
            .expect("queue prior turn error");
        tx_event
            .send(turn_complete_event(
                "current-turn",
                /*last_agent_message*/ None,
                Some(42),
            ))
            .await
            .expect("queue current turn completion");

        let mut analytics_result = GuardianReviewAnalyticsResult::without_session();
        let (outcome, keep_review_session, capture_token_usage) = wait_for_guardian_review(
            &review_session,
            "current-turn",
            tokio::time::Instant::now() + Duration::from_secs(1),
            /*external_cancel*/ None,
            &mut analytics_result,
        )
        .await;

        let GuardianReviewSessionOutcome::Completed(Ok(last_agent_message)) = outcome else {
            panic!("expected current turn completion");
        };
        assert_eq!(last_agent_message, None);
        assert_eq!(analytics_result.time_to_first_token_ms, Some(42));
        assert!(keep_review_session);
        assert!(capture_token_usage);
    }

    #[tokio::test]
    async fn wait_for_guardian_review_preserves_structured_session_error() {
        let (review_session, tx_event, _rx_sub) = test_review_session().await;
        tx_event
            .send(Event {
                id: "current-turn".to_string(),
                msg: EventMsg::Error(ErrorEvent {
                    message: "temporary failure".to_string(),
                    codex_error_info: Some(CodexErrorInfo::ServerOverloaded),
                }),
            })
            .await
            .expect("queue guardian error");
        tx_event
            .send(turn_complete_event(
                "current-turn",
                /*last_agent_message*/ None,
                Some(42),
            ))
            .await
            .expect("queue current turn completion");

        let mut analytics_result = GuardianReviewAnalyticsResult::without_session();
        let (outcome, keep_review_session, capture_token_usage) = wait_for_guardian_review(
            &review_session,
            "current-turn",
            tokio::time::Instant::now() + Duration::from_secs(1),
            /*external_cancel*/ None,
            &mut analytics_result,
        )
        .await;

        let GuardianReviewSessionOutcome::SessionFailed { error, error_info } = outcome else {
            panic!("expected structured session failure");
        };
        assert_eq!(error.to_string(), "temporary failure");
        assert_eq!(error_info, Some(CodexErrorInfo::ServerOverloaded));
        assert!(keep_review_session);
        assert!(capture_token_usage);
    }

    #[tokio::test]
    async fn wait_for_guardian_review_ignores_prior_turn_aborts() {
        let (review_session, tx_event, _rx_sub) = test_review_session().await;
        tx_event
            .send(turn_aborted_event("prior-turn"))
            .await
            .expect("queue prior turn abort");
        tx_event
            .send(turn_complete_event("current-turn", Some("fresh"), Some(42)))
            .await
            .expect("queue current turn completion");

        let mut analytics_result = GuardianReviewAnalyticsResult::without_session();
        let (outcome, keep_review_session, capture_token_usage) = wait_for_guardian_review(
            &review_session,
            "current-turn",
            tokio::time::Instant::now() + Duration::from_secs(1),
            /*external_cancel*/ None,
            &mut analytics_result,
        )
        .await;

        let GuardianReviewSessionOutcome::Completed(Ok(last_agent_message)) = outcome else {
            panic!("expected current turn completion");
        };
        assert_eq!(last_agent_message.as_deref(), Some("fresh"));
        assert_eq!(analytics_result.time_to_first_token_ms, Some(42));
        assert!(keep_review_session);
        assert!(capture_token_usage);
    }

    #[tokio::test]
    async fn wait_for_guardian_review_timeout_drains_expected_turn_after_stale_terminal_event() {
        let (review_session, tx_event, rx_sub) = test_review_session().await;
        tx_event
            .send(turn_complete_event("prior-turn", Some("stale"), Some(9)))
            .await
            .expect("queue prior turn completion");
        let tx_interrupt_event = tx_event.clone();
        let interrupt_response = tokio::spawn(async move {
            let submission = rx_sub.recv().await.expect("interrupt submission");
            assert!(matches!(submission.op, Op::Interrupt));
            tx_interrupt_event
                .send(turn_aborted_event("current-turn"))
                .await
                .expect("queue current turn abort");
        });

        let mut analytics_result = GuardianReviewAnalyticsResult::without_session();
        let (outcome, keep_review_session, capture_token_usage) = wait_for_guardian_review(
            &review_session,
            "current-turn",
            tokio::time::Instant::now() + Duration::from_millis(10),
            /*external_cancel*/ None,
            &mut analytics_result,
        )
        .await;

        interrupt_response
            .await
            .expect("interrupt response task should complete");
        assert!(matches!(outcome, GuardianReviewSessionOutcome::TimedOut));
        assert!(keep_review_session);
        assert!(!capture_token_usage);
    }

    #[tokio::test]
    async fn wait_for_guardian_review_cancel_drains_expected_turn_after_stale_terminal_event() {
        let (review_session, tx_event, rx_sub) = test_review_session().await;
        tx_event
            .send(turn_complete_event("prior-turn", Some("stale"), Some(9)))
            .await
            .expect("queue prior turn completion");
        let tx_interrupt_event = tx_event.clone();
        let interrupt_response = tokio::spawn(async move {
            let submission = rx_sub.recv().await.expect("interrupt submission");
            assert!(matches!(submission.op, Op::Interrupt));
            tx_interrupt_event
                .send(turn_aborted_event("current-turn"))
                .await
                .expect("queue current turn abort");
        });
        let external_cancel = CancellationToken::new();
        external_cancel.cancel();

        let mut analytics_result = GuardianReviewAnalyticsResult::without_session();
        let (outcome, keep_review_session, capture_token_usage) = wait_for_guardian_review(
            &review_session,
            "current-turn",
            tokio::time::Instant::now() + Duration::from_secs(1),
            Some(&external_cancel),
            &mut analytics_result,
        )
        .await;

        interrupt_response
            .await
            .expect("interrupt response task should complete");
        assert!(matches!(outcome, GuardianReviewSessionOutcome::Aborted));
        assert!(keep_review_session);
        assert!(!capture_token_usage);
    }

    #[tokio::test]
    async fn interrupt_and_drain_turn_ignores_prior_turn_completion() {
        let (review_session, tx_event, _rx_sub) = test_review_session().await;
        tx_event
            .send(turn_complete_event("prior-turn", Some("stale"), Some(9)))
            .await
            .expect("queue prior turn completion");
        tx_event
            .send(turn_aborted_event("current-turn"))
            .await
            .expect("queue current turn abort");

        interrupt_and_drain_turn(&review_session.io, "current-turn")
            .await
            .expect("drain current turn");

        assert!(review_session.io.rx_event.try_recv().is_err());
    }
}
