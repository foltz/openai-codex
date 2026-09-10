//! Keeps a not-yet-published session reachable if initialization fails.
//!
//! The construction registry owns this cell independently of the constructor
//! future. Session only weakly references its cleanup owner, so retaining both
//! here does not create a cycle.

use super::SessionRetirementIo;
use super::retirement::CleanupExecution;
use super::retirement::SessionCleanupOwner;
use super::session::Session;
use crate::codex_thread::ThreadCleanupOutcome;
use crate::codex_thread::ThreadLoopOutcome;
use crate::codex_thread::ThreadRetirement;
use crate::codex_thread::ThreadRetirementError;
use crate::codex_thread::ThreadRetirementReport;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::time::Instant;

pub(crate) struct UnpublishedSession {
    pub(crate) session: Arc<Session>,
    pub(crate) cleanup: Arc<SessionCleanupOwner>,
    pub(crate) io: Option<SessionRetirementIo>,
    retirement: Option<ThreadRetirement>,
}

#[derive(Default)]
pub(crate) struct SessionStartupCustody {
    state: Mutex<StartupState>,
}

#[derive(Default)]
struct StartupState {
    session: Option<UnpublishedSession>,
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
            Self::Loop(report) => {
                report.session_loop != ThreadLoopOutcome::TimedOut
                    && report.cleanup
                        == ThreadCleanupOutcome::Finished {
                            persistence_failed: false,
                        }
            }
            _ => false,
        }
    }
}

impl SessionStartupCustody {
    /// Called synchronously at Session birth, before startup can await or fail.
    pub(super) fn retain(&self, session: &Arc<Session>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.session = Some(UnpublishedSession {
            session: Arc::clone(session),
            cleanup: session.cleanup_owner(),
            io: None,
            retirement: None,
        });
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
        self.state.lock().is_ok_and(|state| state.session.is_none())
    }

    /// Only after the constructor is terminal: cleanup must not race startup
    /// mutating the same Session. The caller keeps this cell on incomplete proof.
    pub(crate) async fn shutdown_until(&self, deadline: Instant) -> StartupCleanup {
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
                    return StartupCleanup::Refused(ThreadRetirementError::AuthorityUnavailable);
                };
                state.observed = Some(result);
                state.session.take()
            };
            drop(previous);
        }
        result
    }
}
