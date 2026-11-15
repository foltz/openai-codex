//! Provisional custody component: not yet attached to the 0.169 production constructor.
//! Early census/MCP resources must be integrated before its absence result is
//! used for constructor admission or qualification.
//!
//! Keeps a not-yet-published session reachable if initialization fails.
//!
//! The construction registry owns this cell independently of the constructor
//! future. Session only weakly references its cleanup owner, so retaining both
//! here does not create a cycle.
//!
//! For a failed manager-owned start this cell also owns the start's agent-tree
//! teardown guard. Membership is completed only after a deadline-bound cleanup
//! observation is terminal, so upstream tree shutdown still waits for it.

use super::SessionRetirementIo;
use super::retirement::CleanupExecution;
use super::retirement::SessionCleanupOwner;
use super::session::Session;
use crate::ThreadCleanupOutcome;
use crate::ThreadLoopOutcome;
use crate::ThreadShutdownOutcome;
use crate::agent::control::AgentTreeTeardownGuard;
use crate::codex_thread::ThreadRetirement;
use crate::codex_thread::ThreadRetirementError;
use crate::codex_thread::ThreadRetirementReport;
use codex_protocol::ThreadId;
#[cfg(test)]
use codex_thread_store::LiveThread;
use codex_thread_store::LiveThreadInitGuard;
use futures::FutureExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::time::Instant;
use tracing::warn;

pub(crate) type RegistryDeadlineProbe = Box<dyn Fn() -> Option<Instant> + Send + Sync>;
type PersistenceDisposalReceipt = Shared<BoxFuture<'static, bool>>;

struct PreSessionPersistence {
    thread_id: ThreadId,
    guard: Arc<tokio::sync::Mutex<LiveThreadInitGuard>>,
    disposal: Option<PersistenceDisposal>,
}

struct PersistenceDisposal {
    receipt: PersistenceDisposalReceipt,
    phase: PersistenceDisposalPhase,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum PersistenceDisposalPhase {
    Legacy,
    DeadlineBound,
}

pub(crate) struct UnpublishedSession {
    pub(crate) session: Arc<Session>,
    pub(crate) cleanup: Arc<SessionCleanupOwner>,
    pub(crate) io: Option<SessionRetirementIo>,
    retirement: Option<ThreadRetirement>,
}

#[derive(Default)]
pub(crate) struct SessionStartupCustody {
    state: Mutex<StartupState>,
    /// Weak view of the owning registry's latched deadline.
    registry_deadline: std::sync::OnceLock<RegistryDeadlineProbe>,
    #[cfg(test)]
    pause_after_retain: Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>,
    #[cfg(test)]
    pause_after_persistence: Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>,
}

#[derive(Default)]
struct StartupState {
    session: Option<UnpublishedSession>,
    persistence: Option<PreSessionPersistence>,
    observed: Option<StartupCleanup>,
    deadline: Option<Instant>,
    teardown: Option<AgentTreeTeardownGuard>,
    /// Caller work (e.g. a managed lifetime's task token) kept with an
    /// expired, unreceipted start until a terminal receipt or drop.
    owners: Vec<Box<dyn Send>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StartupCleanup {
    NoUnpublishedSession,
    BeforeLoop(CleanupExecution),
    Loop(ThreadRetirementReport),
    Refused(ThreadRetirementError),
    TimedOut,
}

impl StartupCleanup {
    pub(crate) fn is_complete(self) -> bool {
        match self {
            Self::NoUnpublishedSession => true,
            Self::BeforeLoop(CleanupExecution::Finished {
                persistence_failed: false,
            }) => true,
            Self::Loop(report) => report.is_complete(),
            _ => false,
        }
    }

    /// `None` while cleanup may still progress (timeout, refusal). Otherwise
    /// the fixed failure kind to record before membership is completed.
    fn teardown_settlement(self) -> Option<Option<&'static str>> {
        if self.is_complete() {
            return Some(None);
        }
        match self {
            Self::NoUnpublishedSession => Some(None),
            Self::BeforeLoop(execution) => cleanup_failure_kind(execution.into()).map(Some),
            Self::Loop(report) => {
                if report.session_loop == ThreadLoopOutcome::TimedOut {
                    return None;
                }
                let cleanup = cleanup_failure_kind(report.cleanup)?;
                if cleanup != "complete" {
                    return Some(Some(cleanup));
                }
                Some(Some(match (report.session_loop, report.ordinary) {
                    (ThreadLoopOutcome::Panicked, _) => "session_loop_panicked",
                    (ThreadLoopOutcome::Cancelled, _) => "session_loop_cancelled",
                    (_, ThreadShutdownOutcome::SubmitFailed) => "shutdown_submit_failed",
                    (_, ThreadShutdownOutcome::TimedOut) => "shutdown_timed_out",
                    _ => "cleanup_incomplete",
                }))
            }
            Self::Refused(_) | Self::TimedOut => None,
        }
    }
}

/// `None` for non-terminal cleanup; `"complete"` for a clean receipt.
fn cleanup_failure_kind(cleanup: ThreadCleanupOutcome) -> Option<&'static str> {
    Some(match cleanup {
        ThreadCleanupOutcome::Finished {
            persistence_failed: false,
        } => "complete",
        ThreadCleanupOutcome::Finished {
            persistence_failed: true,
        } => "persistence_failed",
        ThreadCleanupOutcome::Panicked => "cleanup_panicked",
        ThreadCleanupOutcome::McpFailed => "mcp_failed",
        ThreadCleanupOutcome::McpPrewarmFailed => "mcp_prewarm_failed",
        ThreadCleanupOutcome::TaskJoinFailed => "task_join_failed",
        ThreadCleanupOutcome::ConversationShutdownFailed => "conversation_shutdown_failed",
        ThreadCleanupOutcome::CodeModeShutdownFailed => "code_mode_shutdown_failed",
        ThreadCleanupOutcome::SandboxCleanupUnproven { timed_out: true } => {
            "sandbox_cleanup_timed_out"
        }
        ThreadCleanupOutcome::SandboxCleanupUnproven { timed_out: false } => {
            "sandbox_cleanup_unproven"
        }
        ThreadCleanupOutcome::TimedOut | ThreadCleanupOutcome::AuthorityUnavailable => {
            return None;
        }
    })
}

#[cfg(test)]
#[path = "startup_custody_tests.rs"]
mod tests;

impl SessionStartupCustody {
    #[cfg(test)]
    pub(super) fn retain_persistence(&self, thread_id: ThreadId, live_thread: LiveThread) {
        self.retain_persistence_guard(
            thread_id,
            Arc::new(tokio::sync::Mutex::new(LiveThreadInitGuard::new(Some(
                live_thread,
            )))),
        );
    }

    /// Retain an acquisition whose disposal cannot progress while the returned
    /// lock is held, so a test can observe exactly when disposal is driven.
    #[cfg(test)]
    pub(crate) async fn retain_blocked_persistence_for_test(
        &self,
        thread_id: ThreadId,
    ) -> tokio::sync::OwnedMutexGuard<LiveThreadInitGuard> {
        let guard = Arc::new(tokio::sync::Mutex::new(LiveThreadInitGuard::default()));
        let held = Arc::clone(&guard).lock_owned().await;
        self.retain_persistence_guard(thread_id, guard);
        held
    }

    /// Retain acquisition before its first poll, not just its eventual writer.
    pub(super) fn retain_persistence_guard(
        &self,
        thread_id: ThreadId,
        guard: Arc<tokio::sync::Mutex<LiveThreadInitGuard>>,
    ) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.persistence = Some(PreSessionPersistence {
            thread_id,
            guard,
            disposal: None,
        });
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn pause_after_persistence_for_test(
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
    pub(super) async fn wait_after_persistence_for_test(&self) {
        let pause = self
            .pause_after_persistence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some((entered, release)) = pause {
            entered.notify_one();
            release.notified().await;
        }
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn has_pre_session_persistence_for_test(&self) -> bool {
        self.state
            .lock()
            .is_ok_and(|state| state.session.is_none() && state.persistence.is_some())
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn pause_after_retain_for_test(
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
    pub(super) async fn wait_after_retain_for_test(&self) {
        let pause = self
            .pause_after_retain
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some((entered, release)) = pause {
            entered.notify_one();
            release.notified().await;
        }
    }

    /// Called synchronously at Session birth, before startup can await or fail.
    pub(super) fn retain(&self, session: &Arc<Session>) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        if state
            .persistence
            .as_ref()
            .is_some_and(|persistence| persistence.thread_id != session.thread_id)
        {
            return false;
        }
        if let Some(persistence) = state.persistence.as_ref() {
            // Construction has released the acquisition lock before Session birth.
            // Do not transfer custody if that invariant is violated.
            let Ok(mut guard) = persistence.guard.try_lock() else {
                return false;
            };
            guard.commit();
        }
        state.persistence.take();
        state.session = Some(UnpublishedSession {
            session: Arc::clone(session),
            cleanup: session.cleanup_owner(),
            io: None,
            retirement: None,
        });
        true
    }

    pub(super) fn attach_io(&self, io: SessionRetirementIo) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(session) = state.session.as_mut() {
            session.io = Some(io);
        }
    }

    /// Retain a failed manager-owned start's tree membership until this cell's
    /// cleanup is terminal. Dropping the cell first records `GuardAbandoned`.
    pub(crate) fn adopt_teardown(&self, mut teardown: AgentTreeTeardownGuard) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(pending) = state.session.as_ref() {
            teardown.set_thread_id(pending.session.thread_id);
        }
        let previous = state.teardown.replace(teardown);
        drop(state);
        drop(previous);
    }

    /// Tighten-only: a later, shorter bound (e.g. a manager deadline latched
    /// after the constructor's own cleanup began) wins; a later, longer one
    /// never extends the first. Once elapsed, every observation is a sticky
    /// TimedOut that drives no work.
    fn tighten(state: &mut StartupState, deadline: Instant) -> Instant {
        let deadline = state
            .deadline
            .map_or(deadline, |current| current.min(deadline));
        state.deadline = Some(deadline);
        deadline
    }

    /// Clamp this cell, and an unpublished session's cleanup owner, to an
    /// earlier deadline without starting any work.
    pub(crate) fn tighten_deadline(&self, deadline: Instant) {
        let cleanup = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let deadline = Self::tighten(&mut state, deadline);
            state
                .session
                .as_ref()
                .map(|pending| (Arc::clone(&pending.cleanup), deadline))
        };
        if let Some((cleanup, deadline)) = cleanup {
            // Legacy cleanup cannot accept a bound; it remains unobserved here.
            let _ = cleanup.bind_deadline(deadline);
        }
    }

    pub(crate) fn set_registry_deadline_probe(&self, probe: RegistryDeadlineProbe) {
        let _ = self.registry_deadline.set(probe);
    }

    /// Prove registry expiry even if no constructor arm or drain has touched
    /// this cell yet: tighten (only) to the elapsed registry deadline, which
    /// also binds an unpublished session's cleanup owner via bind_deadline.
    pub(crate) fn latch_registry_expiry(&self) {
        let Some(deadline) = self.registry_deadline.get().and_then(|probe| probe()) else {
            return;
        };
        if Instant::now() >= deadline {
            self.tighten_deadline(deadline);
        }
    }

    /// Proven expiry: the latched bound has elapsed, so any observation is
    /// inspection only.
    pub(crate) fn elapsed_deadline(&self) -> Option<Instant> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state
            .deadline
            .filter(|deadline| Instant::now() >= *deadline)
    }

    pub(crate) fn retain_owner(&self, owner: Box<dyn Send>) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .owners
            .push(owner);
    }

    #[cfg(test)]
    pub(crate) fn retained_for_test(&self) -> (bool, usize, bool) {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Third: whether any cleanup ran for the retained start (a persistence
        // disposal was created or the unpublished session's cleanup finished).
        (
            state.teardown.is_some(),
            state.owners.len(),
            state
                .persistence
                .as_ref()
                .is_some_and(|persistence| persistence.disposal.is_some())
                || state
                    .session
                    .as_ref()
                    .is_some_and(|pending| pending.cleanup.completed().is_some()),
        )
    }

    /// Record any terminal failure on the retained guard, then complete it.
    /// Non-terminal observations keep membership; nothing is detached.
    fn settle_teardown(&self, cleanup: StartupCleanup) {
        let Some(failure) = cleanup.teardown_settlement() else {
            return;
        };
        self.complete_teardown(failure);
    }

    fn complete_teardown(&self, failure: Option<&'static str>) {
        let (teardown, owners) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (state.teardown.take(), std::mem::take(&mut state.owners))
        };
        if let Some(teardown) = teardown {
            if let Some(kind) = failure {
                teardown.record_shutdown_failure("startup_custody", kind);
            }
            teardown.complete();
        }
        // Retained caller work is released with the receipt, after membership.
        drop(owners);
    }

    /// Publication has transferred the exact session to a CodexThread owner.
    pub(crate) fn published(&self) {
        let previous = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .session
            .take();
        drop(previous);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.state
            .lock()
            .is_ok_and(|state| state.session.is_none() && state.persistence.is_none())
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "the retained disposal receipt exclusively owns acquisition and discard through this guard"
    )]
    fn persistence_disposal(
        persistence: &mut PreSessionPersistence,
        phase: PersistenceDisposalPhase,
    ) -> PersistenceDisposalReceipt {
        persistence
            .disposal
            .get_or_insert_with(|| {
                let guard = Arc::clone(&persistence.guard);
                let thread_id = persistence.thread_id;
                let receipt = async move {
                    let complete = guard.lock().await.discard_with_result().await.is_ok();
                    if !complete {
                        warn!(
                            persistence_operation = "discard_failed_initialization",
                            thread_id = %thread_id,
                            "failed initialization persistence disposal incomplete"
                        );
                    }
                    complete
                }
                .boxed()
                .shared();
                PersistenceDisposal { receipt, phase }
            })
            .receipt
            .clone()
    }

    fn finish_persistence_disposal(&self, thread_id: ThreadId, complete: bool) -> StartupCleanup {
        let Ok(mut state) = self.state.lock() else {
            return StartupCleanup::Refused(ThreadRetirementError::AuthorityUnavailable);
        };
        if let Some(observed) = state.observed {
            return observed;
        }
        if !state
            .persistence
            .as_ref()
            .is_some_and(|persistence| persistence.thread_id == thread_id)
        {
            return StartupCleanup::Refused(ThreadRetirementError::AuthorityUnavailable);
        }
        if complete {
            state.observed = Some(StartupCleanup::BeforeLoop(CleanupExecution::Finished {
                persistence_failed: false,
            }));
            state.persistence.take();
        }
        StartupCleanup::BeforeLoop(CleanupExecution::Finished {
            persistence_failed: !complete,
        })
    }

    async fn shutdown_persistence_until(&self, deadline: Instant) -> Option<StartupCleanup> {
        let (deadline, thread_id, disposal) = {
            let Ok(mut state) = self.state.lock() else {
                return Some(StartupCleanup::Refused(
                    ThreadRetirementError::AuthorityUnavailable,
                ));
            };
            if let Some(observed) = state.observed {
                return Some(observed);
            }
            if state.session.is_some() {
                return None;
            }
            let deadline = Self::tighten(&mut state, deadline);
            let Some(persistence) = state.persistence.as_mut() else {
                return Some(StartupCleanup::NoUnpublishedSession);
            };
            if let Some(complete) = persistence
                .disposal
                .as_ref()
                .and_then(|disposal| disposal.receipt.peek())
                .copied()
            {
                let thread_id = persistence.thread_id;
                drop(state);
                return Some(self.finish_persistence_disposal(thread_id, complete));
            }
            if persistence
                .disposal
                .as_ref()
                .is_some_and(|disposal| disposal.phase == PersistenceDisposalPhase::Legacy)
            {
                return Some(StartupCleanup::Refused(
                    ThreadRetirementError::LegacyCleanupStarted,
                ));
            }
            if Instant::now() >= deadline {
                return Some(StartupCleanup::TimedOut);
            }
            (
                deadline,
                persistence.thread_id,
                Self::persistence_disposal(persistence, PersistenceDisposalPhase::DeadlineBound),
            )
        };
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => Some(StartupCleanup::TimedOut),
            complete = disposal => Some(self.finish_persistence_disposal(thread_id, complete)),
        }
    }

    #[expect(
        clippy::expect_used,
        reason = "the unbounded branch constructs a disposal receipt; the bounded branch returns before use"
    )]
    async fn shutdown_persistence_legacy(&self) -> Option<bool> {
        let (thread_id, disposal, bounded_deadline) = {
            let Ok(mut state) = self.state.lock() else {
                return Some(false);
            };
            if let Some(observed) = state.observed {
                return Some(observed.is_complete());
            }
            if state.session.is_some() {
                return None;
            }
            let state_deadline = state.deadline;
            let Some(persistence) = state.persistence.as_mut() else {
                return Some(true);
            };
            let disposal_phase = persistence.disposal.as_ref().map(|disposal| disposal.phase);
            let bounded_deadline =
                state_deadline.filter(|_| disposal_phase != Some(PersistenceDisposalPhase::Legacy));
            if bounded_deadline.is_some() {
                (persistence.thread_id, None, bounded_deadline)
            } else {
                (
                    persistence.thread_id,
                    Some(Self::persistence_disposal(
                        persistence,
                        PersistenceDisposalPhase::Legacy,
                    )),
                    None,
                )
            }
        };
        if let Some(deadline) = bounded_deadline {
            return Some(
                self.shutdown_persistence_until(deadline)
                    .await
                    .is_some_and(StartupCleanup::is_complete),
            );
        }
        let complete = disposal.expect("legacy disposal receipt").await;
        Some(
            self.finish_persistence_disposal(thread_id, complete)
                .is_complete(),
        )
    }

    /// Drive the pre-existing unbounded cleanup path after construction is
    /// terminal. This does not relabel legacy work with a later deadline.
    pub(crate) fn shutdown_legacy(&self) -> futures::future::BoxFuture<'_, bool> {
        Box::pin(async move {
            let complete = self.shutdown_legacy_observed().await;
            if complete {
                // Only positive proof releases adopted membership here; an
                // incomplete legacy observation keeps it with this cell.
                self.complete_teardown(None);
            }
            complete
        })
    }

    fn shutdown_legacy_observed(&self) -> futures::future::BoxFuture<'_, bool> {
        Box::pin(async move {
            if let Some(complete) = self.shutdown_persistence_legacy().await {
                return complete;
            }
            let (session, cleanup, io) = {
                let Ok(state) = self.state.lock() else {
                    return false;
                };
                let Some(pending) = state.session.as_ref() else {
                    return true;
                };
                (
                    Arc::clone(&pending.session),
                    Arc::clone(&pending.cleanup),
                    pending.io.clone(),
                )
            };
            let complete = if let Some(io) = io {
                let (loop_result, cleanup_result) =
                    tokio::join!(io.shutdown_and_wait(), cleanup.observe(session));
                loop_result.is_ok()
                    && cleanup_result
                        == (CleanupExecution::Finished {
                            persistence_failed: false,
                        })
            } else {
                cleanup.observe(session).await
                    == (CleanupExecution::Finished {
                        persistence_failed: false,
                    })
            };
            if complete {
                let previous = {
                    let Ok(mut state) = self.state.lock() else {
                        return false;
                    };
                    state.session.take()
                };
                drop(previous);
            }
            complete
        })
    }

    /// Only after the start future is terminal: cleanup must not race startup
    /// mutating the same Session. The caller keeps this cell on incomplete proof.
    /// A terminal observation settles adopted tree membership exactly once.
    pub(crate) fn shutdown_until(
        &self,
        deadline: Instant,
    ) -> futures::future::BoxFuture<'_, StartupCleanup> {
        Box::pin(async move {
            let result = self.shutdown_until_observed(deadline).await;
            self.settle_teardown(result);
            result
        })
    }

    fn shutdown_until_observed(
        &self,
        deadline: Instant,
    ) -> futures::future::BoxFuture<'_, StartupCleanup> {
        Box::pin(async move {
            if let Some(result) = self.shutdown_persistence_until(deadline).await {
                return result;
            }
            let (deadline, session, cleanup, ticket) = {
                let Ok(mut state) = self.state.lock() else {
                    return StartupCleanup::Refused(ThreadRetirementError::AuthorityUnavailable);
                };
                if let Some(observed) = state.observed {
                    return observed;
                }
                let deadline = Self::tighten(&mut state, deadline);
                let Some(pending) = state.session.as_mut() else {
                    return StartupCleanup::NoUnpublishedSession;
                };
                if Instant::now() >= deadline {
                    return StartupCleanup::TimedOut;
                }
                if pending.retirement.is_none()
                    && let Some(io) = pending.io.as_ref()
                {
                    match ThreadRetirement::from_session(
                        Arc::clone(&pending.session),
                        io.clone(),
                        deadline,
                    ) {
                        Ok(ticket) => pending.retirement = Some(ticket),
                        Err(error) => return StartupCleanup::Refused(error),
                    }
                }
                (
                    deadline,
                    Arc::clone(&pending.session),
                    Arc::clone(&pending.cleanup),
                    pending.retirement.clone(),
                )
            };
            let result = if let Some(ticket) = ticket {
                StartupCleanup::Loop(ticket.wait().await)
            } else {
                match cleanup.bind_deadline(deadline) {
                    Ok(_) => StartupCleanup::BeforeLoop(cleanup.observe(session).await),
                    Err(super::retirement::DeadlineBindingError::LegacyCleanupStarted) => {
                        StartupCleanup::Refused(ThreadRetirementError::LegacyCleanupStarted)
                    }
                    Err(super::retirement::DeadlineBindingError::AuthorityUnavailable) => {
                        StartupCleanup::Refused(ThreadRetirementError::AuthorityUnavailable)
                    }
                }
            };
            if result.teardown_settlement().is_some() {
                // A terminal failed receipt is replayed, not re-timed, by later
                // drains; only positive proof releases the session itself.
                let previous = {
                    let Ok(mut state) = self.state.lock() else {
                        return StartupCleanup::Refused(
                            ThreadRetirementError::AuthorityUnavailable,
                        );
                    };
                    state.observed = Some(result);
                    if result.is_complete() {
                        state.session.take()
                    } else {
                        None
                    }
                };
                drop(previous);
            }
            result
        })
    }
}
