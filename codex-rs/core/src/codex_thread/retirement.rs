//! Exact-thread shutdown ownership. Ordinary shutdown classification is never
//! replaced by the fallback's loop-abort or cleanup result.

use super::CodexThread;
use crate::session::SessionLoopOutcome;
use crate::session::retirement::CleanupExecution;
use crate::session::retirement::DeadlineBindingError;
use futures::FutureExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

const ORDINARY_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// Preserved ordinary shutdown classification, independent of fallback work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ThreadShutdownOutcome {
    Complete,
    SubmitFailed,
    TimedOut,
}

/// Observed outcome of this exact session loop, not proof of resource cleanup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ThreadLoopOutcome {
    Normal,
    Cancelled,
    Panicked,
    TimedOut,
}

/// Cleanup execution receipt. API failures are not internal-task join proofs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ThreadCleanupOutcome {
    Finished { persistence_failed: bool },
    Panicked,
    TimedOut,
    AuthorityUnavailable,
    McpFailed,
    McpPrewarmFailed,
    TaskJoinFailed,
    ConversationShutdownFailed,
    CodeModeShutdownFailed,
}

impl From<CleanupExecution> for ThreadCleanupOutcome {
    fn from(cleanup: CleanupExecution) -> Self {
        match cleanup {
            CleanupExecution::Finished { persistence_failed } => {
                Self::Finished { persistence_failed }
            }
            CleanupExecution::Panicked => Self::Panicked,
            CleanupExecution::TimedOut => Self::TimedOut,
            CleanupExecution::AuthorityUnavailable => Self::AuthorityUnavailable,
            CleanupExecution::McpFailed => Self::McpFailed,
            CleanupExecution::McpPrewarmFailed => Self::McpPrewarmFailed,
            CleanupExecution::TaskJoinFailed => Self::TaskJoinFailed,
            CleanupExecution::ConversationShutdownFailed => Self::ConversationShutdownFailed,
            CleanupExecution::CodeModeShutdownFailed => Self::CodeModeShutdownFailed,
        }
    }
}

/// Independent facts needed by the lifecycle owner; no fallback emits a
/// synthetic ShutdownComplete event or rewrites `ordinary`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ThreadRetirementReport {
    pub ordinary: ThreadShutdownOutcome,
    pub session_loop: ThreadLoopOutcome,
    pub cleanup: ThreadCleanupOutcome,
}

impl ThreadRetirementReport {
    /// Positive evidence for exact-thread retirement. A normal loop join is
    /// not enough unless the retained cleanup owner also returned cleanly.
    pub fn is_complete(&self) -> bool {
        matches!(self.ordinary, ThreadShutdownOutcome::Complete)
            && matches!(self.session_loop, ThreadLoopOutcome::Normal)
            && matches!(
                self.cleanup,
                ThreadCleanupOutcome::Finished {
                    persistence_failed: false
                }
            )
    }
}

/// Failure to establish an original-deadline retirement owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ThreadRetirementError {
    AuthorityUnavailable,
    LegacyCleanupStarted,
    TaskAdmissionBusy,
    ActiveWork,
    DeadlineExpired,
}

/// Cloned observers drive one retained attempt. Dropping an observer neither
/// resubmits shutdown nor loses the cleanup or exact-loop join owner.
#[derive(Clone)]
pub struct ThreadRetirement {
    deadline: Instant,
    pub(super) completion: Shared<BoxFuture<'static, ThreadRetirementReport>>,
    progress: tokio::sync::watch::Sender<ThreadRetirementReport>,
}

impl ThreadRetirement {
    /// The first caller's bound; subsequent callers cannot refresh it.
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Drive or replay the same retirement transaction.
    pub async fn wait(&self) -> ThreadRetirementReport {
        if let Some(report) = self.completion.peek() {
            return *report;
        }
        if Instant::now() >= self.deadline {
            return *self.progress.borrow();
        }
        // timeout_at polls its inner future first, including after expiry.
        // Never resume a paused side-effecting transaction outside its bound.
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(self.deadline) => *self.progress.borrow(),
            report = self.completion.clone() => report,
        }
    }
}

impl CodexThread {
    /// Previously recorded resource cleanup plus this exact loop's join.
    /// Used for custody compaction, not to invent an ordinary classification
    /// or certify that legacy work ran under a newly supplied deadline.
    pub(crate) fn observed_terminal_cleanup(&self) -> Option<SessionLoopOutcome> {
        if !self.session.cleanup_owner().reconciled()
            && self.session.cleanup_owner().completed()
                != Some(CleanupExecution::Finished {
                    persistence_failed: false,
                })
        {
            return None;
        }
        self.io.session_loop_termination.observed()
    }

    /// Atomically checks idle work and closes task admission before returning
    /// the retained ticket. No await or waiting for an in-flight admission is
    /// allowed: callers may hold their lifecycle-authority commit lock.
    pub fn try_begin_idle_retirement(
        &self,
        deadline: Instant,
    ) -> Result<ThreadRetirement, ThreadRetirementError> {
        if Instant::now() >= deadline {
            return Err(ThreadRetirementError::DeadlineExpired);
        }
        let active = self
            .session
            .active_turn
            .try_lock()
            .map_err(|_| ThreadRetirementError::TaskAdmissionBusy)?;
        if active.is_some()
            || matches!(
                *self.io.agent_status.borrow(),
                crate::agent::AgentStatus::Running
            )
        {
            return Err(ThreadRetirementError::ActiveWork);
        }
        let ticket = self
            .io
            .tx_sub
            .with_idle_admission(|| self.begin_retirement(deadline))
            .map_err(|error| match error {
                crate::session::IdleAdmissionError::Busy => {
                    ThreadRetirementError::TaskAdmissionBusy
                }
                crate::session::IdleAdmissionError::Outstanding => {
                    ThreadRetirementError::ActiveWork
                }
                crate::session::IdleAdmissionError::Unavailable => {
                    ThreadRetirementError::AuthorityUnavailable
                }
            })??;
        self.session
            .task_admission_closed
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(ticket)
    }

    /// Prepare retirement of an already unload-committed exact thread.
    ///
    /// The caller retains this ticket until its report is consumed. This does
    /// not grant authority to unload an active thread. Binding precedes the
    /// first shutdown submission; repeated calls preserve the original ticket.
    pub fn begin_retirement(
        &self,
        deadline: Instant,
    ) -> Result<ThreadRetirement, ThreadRetirementError> {
        let mut slot = self
            .retirement
            .lock()
            .map_err(|_| ThreadRetirementError::AuthorityUnavailable)?;
        if let Some(ticket) = &*slot {
            return Ok(ticket.clone());
        }
        let ticket = ThreadRetirement::from_session(
            Arc::clone(&self.session),
            self.io.retirement_io(),
            deadline,
        )?;
        *slot = Some(ticket.clone());
        Ok(ticket)
    }
}

impl ThreadRetirement {
    /// Shared with startup custody when a loop exists but publication failed.
    pub(crate) fn from_session(
        session: Arc<crate::session::session::Session>,
        io: crate::session::SessionRetirementIo,
        deadline: Instant,
    ) -> Result<Self, ThreadRetirementError> {
        let owner = session.cleanup_owner();
        let deadline = owner.bind_deadline(deadline).map_err(|error| match error {
            DeadlineBindingError::LegacyCleanupStarted => {
                ThreadRetirementError::LegacyCleanupStarted
            }
            DeadlineBindingError::AuthorityUnavailable => {
                ThreadRetirementError::AuthorityUnavailable
            }
        })?;
        let progress = tokio::sync::watch::channel(ThreadRetirementReport {
            ordinary: ThreadShutdownOutcome::TimedOut,
            session_loop: ThreadLoopOutcome::TimedOut,
            cleanup: ThreadCleanupOutcome::TimedOut,
        })
        .0;
        let observed = progress.clone();
        // The future owns Session and IO, never CodexThread: storing its
        // original here must not create a thread -> future -> thread cycle.
        let completion = async move {
            if Instant::now() >= deadline {
                return ThreadRetirementReport {
                    ordinary: ThreadShutdownOutcome::TimedOut,
                    session_loop: ThreadLoopOutcome::TimedOut,
                    cleanup: ThreadCleanupOutcome::TimedOut,
                };
            }
            let ordinary_deadline = deadline.min(Instant::now() + ORDINARY_SHUTDOWN_TIMEOUT);
            let ordinary = tokio::time::timeout_at(ordinary_deadline, async {
                session.close_task_admission().await;
                io.tx_sub.close_admission();
                // The legacy wrapper tolerates InternalAgentDied and then
                // classifies the exact join: a previously normal exit is
                // Complete even when no new Shutdown event was emitted.
                let _ = io.submit_shutdown().await;
                // Preserve the queued Shutdown, but refuse subsequent work.
                io.tx_sub.close();
                let outcome = io.session_loop_termination.clone().await;
                observed.send_modify(|report| {
                    report.session_loop = match outcome {
                        SessionLoopOutcome::Normal => ThreadLoopOutcome::Normal,
                        SessionLoopOutcome::Cancelled => ThreadLoopOutcome::Cancelled,
                        SessionLoopOutcome::Panicked => ThreadLoopOutcome::Panicked,
                    }
                });
                match outcome {
                    SessionLoopOutcome::Normal => ThreadShutdownOutcome::Complete,
                    SessionLoopOutcome::Cancelled | SessionLoopOutcome::Panicked => {
                        ThreadShutdownOutcome::SubmitFailed
                    }
                }
            })
            .await
            .unwrap_or(ThreadShutdownOutcome::TimedOut);
            observed.send_modify(|report| report.ordinary = ordinary);

            if Instant::now() >= deadline {
                return *observed.borrow();
            }
            io.tx_sub.close();
            if ordinary != ThreadShutdownOutcome::Complete {
                io.session_loop_termination.request_abort();
            }
            // Aborting the loop can drop its cleanup observer, but not the
            // owner's original. Drive that same cleanup independently now.
            let (session_loop, cleanup) = tokio::join!(
                tokio::time::timeout_at(deadline, io.session_loop_termination.clone()),
                owner.observe(session),
            );
            ThreadRetirementReport {
                ordinary,
                session_loop: match session_loop {
                    Ok(SessionLoopOutcome::Normal) => ThreadLoopOutcome::Normal,
                    Ok(SessionLoopOutcome::Cancelled) => ThreadLoopOutcome::Cancelled,
                    Ok(SessionLoopOutcome::Panicked) => ThreadLoopOutcome::Panicked,
                    Err(_) => ThreadLoopOutcome::TimedOut,
                },
                cleanup: cleanup.into(),
            }
        }
        .boxed()
        .shared();
        let ticket = ThreadRetirement {
            deadline,
            completion,
            progress,
        };
        Ok(ticket)
    }
}
