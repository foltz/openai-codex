//! New recovery observations never rewrite the original retirement transaction.
use super::CodexThread;
use super::ThreadCleanupOutcome;
use super::ThreadLoopOutcome;
use super::ThreadShutdownOutcome;
use crate::session::SessionLoopOutcome;
use crate::session::retirement::CleanupExecution;
use futures::FutureExt;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::Ordering;
use tokio::time::Instant;

/// Local quiescence is deliberately weaker than complete physical cleanup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ThreadRecoveryOutcome {
    Reconciled,
    Quiescent,
    Ineligible,
}

impl CodexThread {
    /// A closed runtime cannot be presented as an ordinary usable warm session.
    pub fn is_closing(&self) -> bool {
        self.io.tx_sub.is_closed() || self.session.task_admission_closed.load(Ordering::Acquire)
    }

    /// Independent positive receipts required before any replacement can be considered.
    /// Unproven sandbox cleanup keeps the old authority retained.
    pub fn retirement_is_quiescent(&self) -> bool {
        if self.sandbox_cleanup_outcome().is_some() {
            return false;
        }
        let Ok(retirement) = self.retirement.lock() else {
            return false;
        };
        let Some(original) = retirement
            .as_ref()
            .and_then(|ticket| ticket.completion.peek())
        else {
            return false;
        };
        original.ordinary == ThreadShutdownOutcome::Complete
            && original.session_loop == ThreadLoopOutcome::Normal
            && self.io.tx_sub.is_closed()
            && self.session.task_admission_closed.load(Ordering::Acquire)
            // Complete original cleanup already proves every owner joined,
            // including a historically panicked task. The stricter non-panicked
            // component proof remains mandatory for incomplete handoff/retry.
            && (original.is_complete() || self.session.cleanup_owner().recovery_ready())
            && self
                .session
                .services
                .live_thread
                .as_ref()
                .is_none_or(codex_thread_store::LiveThread::is_sealed)
    }

    /// Read-only evidence that this exact runtime released its resources after
    /// an incomplete original retirement report. Unlike
    /// `retirement_is_quiescent`, it does not require `ordinary == Complete`:
    /// the original report stays as recorded and is never rewritten here.
    ///
    /// Requires: the exact session loop already joined `Normal`; the cleanup
    /// owner attached at loop birth already recorded
    /// `Finished { persistence_failed: false }`; submission and task admission
    /// are closed; and the live writer, if any, is sealed through its write
    /// gate. Every check is a peek: this never polls, drives, rebinds or
    /// refreshes cleanup, and a pending or contended observation is `false`.
    pub fn late_release_is_quiescent(&self) -> bool {
        let termination = &self.io.session_loop_termination;
        self.sandbox_cleanup_outcome().is_none()
            && termination.observed() == Some(SessionLoopOutcome::Normal)
            && termination.cleanup_completed()
                == Some(CleanupExecution::Finished {
                    persistence_failed: false,
                })
            && self.io.tx_sub.is_closed()
            && self.session.task_admission_closed.load(Ordering::Acquire)
            && self
                .session
                .services
                .live_thread
                .as_ref()
                .is_none_or(codex_thread_store::LiveThread::is_sealed)
    }

    /// A separate bounded attempt over existing MCP owners, not a new original deadline.
    pub async fn reconcile_retirement_until(&self, deadline: Instant) -> ThreadRecoveryOutcome {
        if !self.retirement_is_quiescent() {
            return ThreadRecoveryOutcome::Ineligible;
        }
        if self.retirement_reconciled() {
            return ThreadRecoveryOutcome::Reconciled;
        }
        if Instant::now() >= deadline {
            return ThreadRecoveryOutcome::Quiescent;
        }
        let Ok(report) = AssertUnwindSafe(
            self.session
                .services
                .mcp_runtime
                .retirement_owner()
                .drain_workers_until(deadline),
        )
        .catch_unwind()
        .await
        else {
            return ThreadRecoveryOutcome::Quiescent;
        };
        if self.session.cleanup_owner().record_reconciliation(report) {
            ThreadRecoveryOutcome::Reconciled
        } else {
            ThreadRecoveryOutcome::Quiescent
        }
    }

    /// Recorded exact-runtime positive MCP proof; historical failure still replays unchanged.
    pub fn retirement_reconciled(&self) -> bool {
        self.session.cleanup_owner().reconciled()
    }
}

#[cfg(test)]
#[path = "recovery_tests.rs"]
mod tests;
