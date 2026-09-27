//! Keeps a not-yet-published session reachable if initialization fails.
//!
//! The construction registry owns this cell independently of the constructor
//! future. Session only weakly references its cleanup owner, so retaining both
//! here does not create a cycle.

use super::SessionRetirementIo;
use super::retirement::CleanupExecution;
use super::retirement::SessionCleanupOwner;
use super::session::Session;
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
    pub(crate) fn has_pre_session_persistence_for_test(&self) -> bool {
        self.state
            .lock()
            .is_ok_and(|state| state.session.is_none() && state.persistence.is_some())
    }

    #[cfg(test)]
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
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
            let deadline = *state.deadline.get_or_insert(deadline);
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

    /// Only after the constructor is terminal: cleanup must not race startup
    /// mutating the same Session. The caller keeps this cell on incomplete proof.
    pub(crate) fn shutdown_until(
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
                let deadline = *state.deadline.get_or_insert(deadline);
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
            if result.is_complete() {
                let previous = {
                    let Ok(mut state) = self.state.lock() else {
                        return StartupCleanup::Refused(
                            ThreadRetirementError::AuthorityUnavailable,
                        );
                    };
                    state.observed = Some(result);
                    state.session.take()
                };
                drop(previous);
            }
            result
        })
    }
}
