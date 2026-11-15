//! Cancellation-safe ownership of the common session teardown sequence.
//!
//! This execution receipt is not yet a terminal receipt for every resource.
//! In particular, legacy cleanup steps still have their existing error policy.

use super::session::Session;
use crate::sandboxing::cleanup_census::SandboxCleanupCensus;
use crate::sandboxing::cleanup_census::SandboxCleanupCensusTotals;
use codex_sandboxing::CleanupDeadlineScope;
use futures::FutureExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::watch;
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CleanupExecution {
    Finished {
        persistence_failed: bool,
    },
    Panicked,
    TimedOut,
    AuthorityUnavailable,
    McpFailed,
    McpPrewarmFailed,
    TaskJoinFailed,
    ConversationShutdownFailed,
    CodeModeShutdownFailed,
    /// Everything else finished cleanly, but the session's sandbox cleanup
    /// census holds unproven helper cleanup (`timed_out` when some passed
    /// its deadline). Sticky like every receipt: never a clean outcome, so
    /// no ShutdownComplete, no recovery, no reuse of the old authority.
    SandboxCleanupUnproven {
        timed_out: bool,
    },
}

impl CleanupExecution {
    /// Qualifies an otherwise clean cleanup with the session's census at the
    /// moment cleanup finished. Any other outcome is kept as recorded.
    pub(crate) fn with_sandbox_cleanup_census(self, census: SandboxCleanupCensusTotals) -> Self {
        if self
            == (Self::Finished {
                persistence_failed: false,
            })
            && census.has_unproven()
        {
            return Self::SandboxCleanupUnproven {
                timed_out: census.has_timed_out(),
            };
        }
        self
    }
}

#[derive(Clone, Debug)]
enum McpCleanup {
    Observed(Arc<codex_mcp::RuntimeWorkerDrainReport>),
    Panicked,
}

#[derive(Clone, Copy)]
pub(super) enum CleanupMode {
    Legacy,
    DeadlineBound,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CleanupSequence {
    Ordinary,
    Suspension,
}

#[derive(Default)]
struct CleanupState {
    sequence: Option<CleanupSequence>,
    completion: Option<Shared<BoxFuture<'static, CleanupExecution>>>,
    mcp_completion: Option<Shared<BoxFuture<'static, McpCleanup>>>,
    task_completion: Option<Shared<BoxFuture<'static, crate::tasks::TaskJoinOutcome>>>,
    common_completion: Option<Shared<BoxFuture<'static, CleanupExecution>>>,
    deadline: Option<Instant>,
    reconciliation: Option<Arc<codex_mcp::RuntimeWorkerDrainReport>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeadlineBindingError {
    LegacyCleanupStarted,
    AuthorityUnavailable,
}

pub(crate) struct SessionCleanupOwner {
    state: Mutex<CleanupState>,
    deadline_changed: watch::Sender<Option<Instant>>,
    /// The session's sandbox cleanup scope. Owners are re-created through a
    /// weak slot, but each one shares the session's single scope, so a
    /// deadline bound on any owner reaches every live sandbox launch.
    sandbox_cleanup_scope: CleanupDeadlineScope,
    /// The session's sandbox cleanup census: positive MCP reconciliation
    /// never overrides unproven sandbox cleanup.
    sandbox_cleanup_census: Arc<SandboxCleanupCensus>,
    // Live deadline clamp for cleanup that started in Legacy (unbounded)
    // mode. It never grants Legacy cleanup deadline authority; it only stops
    // driving it at the owning shutdown deadline.
    legacy_clamp: Arc<LegacyClamp>,
    // Partial Legacy work stopped by the clamp: retained unpolled (never
    // aborted) until this owner is released.
    legacy_retained: Arc<Mutex<Option<BoxFuture<'static, CleanupExecution>>>>,
}

impl Default for SessionCleanupOwner {
    fn default() -> Self {
        Self::new(CleanupDeadlineScope::new(), Arc::default())
    }
}

/// What a deadline binding does to a cleanup that already started in Legacy
/// mode: always refused; clamped only at the permanent host/manager shutdown
/// edge.
#[derive(Clone, Copy, Eq, PartialEq)]
enum LegacyAtBind {
    Refuse,
    RefuseAndClamp,
}

/// The live clamp deadline an in-flight Legacy cleanup observes: it only
/// tightens, and every tightening wakes the driver.
#[derive(Default)]
pub(super) struct LegacyClamp {
    deadline: Mutex<Option<Instant>>,
    changed: Arc<tokio::sync::Notify>,
}

impl LegacyClamp {
    pub(super) fn tighten(&self, deadline: Instant) {
        {
            let mut current = self
                .deadline
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *current = Some(current.map_or(deadline, |current| current.min(deadline)));
        }
        self.changed.notify_waiters();
    }

    fn deadline(&self) -> Option<Instant> {
        *self
            .deadline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Drive Legacy cleanup `work` until it finishes or the live clamp deadline
/// passes; the clock is checked before every poll of `work`. At the deadline
/// the exact partial work is handed to `retained` (never polled again, never
/// aborted) and the sticky outcome is `TimedOut` (the result is shared).
pub(super) fn clamp_legacy_cleanup(
    work: BoxFuture<'static, CleanupExecution>,
    clamp: Arc<LegacyClamp>,
    retained: std::sync::Weak<Mutex<Option<BoxFuture<'static, CleanupExecution>>>>,
) -> BoxFuture<'static, CleanupExecution> {
    let mut work = Some(work);
    let mut timer: Option<(Instant, std::pin::Pin<Box<tokio::time::Sleep>>)> = None;
    let mut changed = Box::pin(Arc::clone(&clamp.changed).notified_owned());
    std::future::poll_fn(move |cx| {
        loop {
            // Register for tightenings before reading the deadline.
            if changed.as_mut().poll(cx).is_ready() {
                changed = Box::pin(Arc::clone(&clamp.changed).notified_owned());
                continue;
            }
            if let Some(deadline) = clamp.deadline() {
                if Instant::now() >= deadline {
                    if let (Some(partial), Some(retained)) = (work.take(), retained.upgrade()) {
                        *retained
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(partial);
                    }
                    return std::task::Poll::Ready(CleanupExecution::TimedOut);
                }
                if timer.as_ref().is_none_or(|(armed, _)| *armed != deadline) {
                    timer = Some((deadline, Box::pin(tokio::time::sleep_until(deadline))));
                }
                if let Some((_, sleep)) = timer.as_mut()
                    && sleep.as_mut().poll(cx).is_ready()
                {
                    continue;
                }
            }
            let Some(partial) = work.as_mut() else {
                // Already stopped by the clamp: sticky.
                return std::task::Poll::Ready(CleanupExecution::TimedOut);
            };
            return partial.as_mut().poll(cx);
        }
    })
    .boxed()
}

impl SessionCleanupOwner {
    pub(crate) fn new(
        sandbox_cleanup_scope: CleanupDeadlineScope,
        sandbox_cleanup_census: Arc<SandboxCleanupCensus>,
    ) -> Self {
        Self {
            state: Mutex::new(CleanupState::default()),
            deadline_changed: watch::channel(None).0,
            sandbox_cleanup_scope,
            sandbox_cleanup_census,
            legacy_clamp: Arc::new(LegacyClamp::default()),
            legacy_retained: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn recovery_ready(&self) -> bool {
        let Ok(state) = self.state.lock() else {
            return false;
        };
        state
            .common_completion
            .as_ref()
            .and_then(|work| work.peek())
            == Some(&CleanupExecution::Finished {
                persistence_failed: false,
            })
            && state.task_completion.as_ref().and_then(|work| work.peek())
                == Some(&crate::tasks::TaskJoinOutcome::Complete { panicked: false })
    }

    /// Recorded positive MCP proof. Never claimed while the session's
    /// sandbox cleanup is unproven: that ownership must stay retained.
    pub(crate) fn reconciled(&self) -> bool {
        if self.sandbox_cleanup_census.totals().has_unproven() {
            return false;
        }
        self.state.lock().is_ok_and(|state| {
            state
                .reconciliation
                .as_ref()
                .is_some_and(|report| report.are_recorded_workers_drained())
        })
    }

    pub(crate) fn record_reconciliation(
        &self,
        report: codex_mcp::RuntimeWorkerDrainReport,
    ) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        if !report.are_recorded_workers_drained() {
            return false;
        }
        state.reconciliation = Some(Arc::new(report));
        true
    }

    /// Read an already recorded cleanup result without creating or polling work.
    /// This does not retroactively bind legacy cleanup to a later deadline.
    pub(crate) fn completed(&self) -> Option<CleanupExecution> {
        let state = self.state.try_lock().ok()?;
        state.completion.as_ref()?.peek().copied()
    }

    /// Must precede ordinary Shutdown submission. An existing unbounded cleanup
    /// cannot retroactively establish the exact-thread deadline contract.
    ///
    /// Ordinary binding (reset, retirement, startup custody): a cleanup that
    /// already started in Legacy mode is refused and otherwise left alone, so
    /// it can still finish and become terminal.
    pub(crate) fn bind_deadline(&self, deadline: Instant) -> Result<Instant, DeadlineBindingError> {
        self.bind_deadline_inner(deadline, LegacyAtBind::Refuse)
    }

    /// Permanent host/manager shutdown edge only
    /// (`ThreadManager::bind_shutdown_deadlines_now`, reached from the
    /// standalone host's intake and from the permanent
    /// `ThreadManager::begin_shutdown`, and publication after the edge
    /// latched): as `bind_deadline`, and a cleanup that already started in
    /// Legacy mode is additionally clamped live to `deadline`. Its authority
    /// is unchanged (still refused, and counted by the caller), but it is
    /// never driven past the host's owning shutdown deadline.
    pub(crate) fn bind_shutdown_edge_deadline(
        &self,
        deadline: Instant,
    ) -> Result<Instant, DeadlineBindingError> {
        self.bind_deadline_inner(deadline, LegacyAtBind::RefuseAndClamp)
    }

    fn bind_deadline_inner(
        &self,
        deadline: Instant,
        legacy: LegacyAtBind,
    ) -> Result<Instant, DeadlineBindingError> {
        // The retirement deadline bounds the session's sandbox launches even
        // when the cleanup sequence itself refuses the binding: tightening
        // only ever moves their cleanup deadlines earlier.
        self.sandbox_cleanup_scope.tighten(deadline.into_std());
        let mut state = self
            .state
            .lock()
            .map_err(|_| DeadlineBindingError::AuthorityUnavailable)?;
        if state.completion.is_some() && state.deadline.is_none() {
            if legacy == LegacyAtBind::RefuseAndClamp {
                self.legacy_clamp.tighten(deadline);
            }
            return Err(DeadlineBindingError::LegacyCleanupStarted);
        }
        let deadline = state
            .deadline
            .map_or(deadline, |current| current.min(deadline));
        state.deadline = Some(deadline);
        self.deadline_changed.send_replace(Some(deadline));
        Ok(deadline)
    }

    pub(crate) async fn observe(&self, session: Arc<Session>) -> CleanupExecution {
        self.observe_sequence(session, CleanupSequence::Ordinary)
            .await
    }

    /// Retain suspension's stop/flush/close sequence in the same owner used by
    /// loop termination and population retirement. Ordinary cleanup may observe
    /// this receipt, but cannot substitute for suspension's stricter sequence.
    pub(super) async fn observe_suspension(&self, session: Arc<Session>) -> CleanupExecution {
        self.observe_sequence(session, CleanupSequence::Suspension)
            .await
    }

    async fn observe_sequence(
        &self,
        session: Arc<Session>,
        sequence: CleanupSequence,
    ) -> CleanupExecution {
        let completion = {
            let Ok(mut state) = self.state.lock() else {
                return CleanupExecution::AuthorityUnavailable;
            };
            if sequence == CleanupSequence::Suspension
                && state.completion.is_some()
                && state.sequence != Some(CleanupSequence::Suspension)
            {
                // Another sequence already owns cleanup. Never execute a
                // second sequence or claim it supplied suspension's flush.
                return CleanupExecution::AuthorityUnavailable;
            }
            if state.completion.is_none() {
                state.sequence = Some(sequence);
                let mcp = state.deadline.map(|deadline| {
                    let session = Arc::clone(&session);
                    async move {
                        // Freeze refresh admission without waiting behind a
                        // network handshake. The runtime registry owns and
                        // drains already-admitted physical attempts.
                        session.mcp_refresh.close();
                        let root = session.services.mcp_runtime.retirement_owner();
                        match AssertUnwindSafe(root.drain_workers_until(deadline))
                            .catch_unwind()
                            .await
                        {
                            Ok(report) => McpCleanup::Observed(Arc::new(report)),
                            Err(_) => McpCleanup::Panicked,
                        }
                    }
                    .boxed()
                    .shared()
                });
                state.mcp_completion = mcp.clone();
                let tasks = state.deadline.map(|deadline| {
                    let session = Arc::clone(&session);
                    async move {
                        AssertUnwindSafe(async {
                            session.close_task_admission().await;
                            session.task_joins.shutdown_until(deadline).await
                        })
                        .catch_unwind()
                        .await
                        .unwrap_or(crate::tasks::TaskJoinOutcome::Failed)
                    }
                    .boxed()
                    .shared()
                });
                state.task_completion = tasks.clone();
                let mode = if state.deadline.is_some() {
                    CleanupMode::DeadlineBound
                } else {
                    CleanupMode::Legacy
                };
                let legacy_clamp = Arc::clone(&self.legacy_clamp);
                let legacy_retained = Arc::downgrade(&self.legacy_retained);
                let common = async move {
                    let cleanup = async {
                        match sequence {
                            CleanupSequence::Ordinary => {
                                super::handlers::cleanup_session(&session, mode).await
                            }
                            CleanupSequence::Suspension => {
                                super::turn_suspension::cleanup_suspended_session(&session).await
                            }
                        }
                    };
                    match AssertUnwindSafe(cleanup).catch_unwind().await {
                        Ok(result) => result,
                        Err(_) => CleanupExecution::Panicked,
                    }
                }
                .boxed();
                let common = match mode {
                    // Legacy cleanup keeps its authority, but a shutdown edge
                    // arriving while it is in flight clamps it live.
                    CleanupMode::Legacy => {
                        clamp_legacy_cleanup(common, legacy_clamp, legacy_retained)
                    }
                    CleanupMode::DeadlineBound => common,
                }
                .shared();
                state.common_completion = Some(common.clone());
                state.completion = Some(
                    async move {
                        if let (Some(mcp), Some(tasks)) = (mcp, tasks) {
                            // Neither common cleanup failure nor a pending lock
                            // suppresses the independent MCP proof driver.
                            let (mcp, tasks, common) = tokio::join!(mcp, tasks, common);
                            if common == CleanupExecution::Panicked {
                                return common;
                            }
                            if !matches!(tasks, crate::tasks::TaskJoinOutcome::Complete { .. }) {
                                return CleanupExecution::TaskJoinFailed;
                            }
                            match mcp {
                                McpCleanup::Observed(report)
                                    if report.are_recorded_workers_drained() =>
                                {
                                    common
                                }
                                McpCleanup::Observed(_) | McpCleanup::Panicked => {
                                    CleanupExecution::McpFailed
                                }
                            }
                        } else {
                            common.await
                        }
                    }
                    .boxed()
                    .shared(),
                );
            }
            match &state.completion {
                Some(completion) => completion.clone(),
                None => return CleanupExecution::AuthorityUnavailable,
            }
        };
        let mut changes = self.deadline_changed.subscribe();
        loop {
            if let Some(result) = completion.peek() {
                return *result;
            }
            let deadline = *changes.borrow_and_update();
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return CleanupExecution::TimedOut;
            }
            let expiry = async move {
                match deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                biased;
                changed = changes.changed() => {
                    if changed.is_err() {
                        return CleanupExecution::AuthorityUnavailable;
                    }
                }
                _ = expiry => return CleanupExecution::TimedOut,
                result = completion.clone() => return result,
            }
        }
    }
}

impl Session {
    /// The loop and its external observation retain the owner strongly. The
    /// back-reference is weak because the retained cleanup future owns Session.
    pub(crate) fn cleanup_owner(&self) -> Arc<SessionCleanupOwner> {
        let mut slot = self
            .cleanup_owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(owner) = slot.upgrade() {
            return owner;
        }
        let owner = Arc::new(SessionCleanupOwner::new(
            self.services.sandbox_cleanup_scope().clone(),
            Arc::clone(self.services.sandbox_cleanup_census()),
        ));
        *slot = Arc::downgrade(&owner);
        owner
    }
}

#[cfg(test)]
#[path = "retirement_tests.rs"]
pub(crate) mod tests;
