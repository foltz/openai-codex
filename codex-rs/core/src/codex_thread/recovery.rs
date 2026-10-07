//! New recovery observations never rewrite the original retirement transaction.
use super::CodexThread;
use super::ThreadLoopOutcome;
use super::ThreadShutdownOutcome;
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
    pub fn retirement_is_quiescent(&self) -> bool {
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
            && self.session.cleanup_owner().recovery_ready()
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
        let Ok(report) =
            AssertUnwindSafe(self.session.services.mcp_runtime.shutdown_until(deadline))
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
