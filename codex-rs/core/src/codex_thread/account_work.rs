//! Submission from a finite operation that can outlive its originating turn.

use super::CodexThread;
use super::ThreadCleanupOutcome;
use super::ThreadLoopOutcome;
use super::ThreadRetirementReport;
use super::ThreadShutdownOutcome;
use crate::session::SessionLoopOutcome;
use codex_extension_api::HostOperationWork;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::turn_input::NotSubmittedReason;
use codex_protocol::turn_input::StartIfIdleSubmission;
use codex_protocol::turn_input::TurnInputMode;
use codex_protocol::turn_input::TurnInputRequest;
use codex_protocol::turn_input::TurnInputSubmission;

impl CodexThread {
    /// Observe ordinary shutdown, exact loop termination, and retained cleanup
    /// separately. A pre-shutdown loop panic still needs its cleanup driven.
    /// This binds no new deadline and cannot turn a failed loop into success.
    pub async fn shutdown_and_wait_with_cleanup(&self) -> ThreadRetirementReport {
        let ordinary = match self.io.shutdown_and_wait().await {
            Ok(()) => ThreadShutdownOutcome::Complete,
            Err(_) => ThreadShutdownOutcome::SubmitFailed,
        };
        let session_loop = match self.io.session_loop_termination.clone().await {
            SessionLoopOutcome::Normal => ThreadLoopOutcome::Normal,
            SessionLoopOutcome::Cancelled => ThreadLoopOutcome::Cancelled,
            SessionLoopOutcome::Panicked => ThreadLoopOutcome::Panicked,
        };
        let cleanup = self.session.cleanup_owner().observe(std::sync::Arc::clone(&self.session)).await;
        ThreadRetirementReport { ordinary, session_loop, cleanup: ThreadCleanupOutcome::from(cleanup) }
    }

    /// Start an idle child using an operation's still-live authority, even
    /// during drain. Bind to this exact destination's store and loop receipt;
    /// do not reconstruct authority from the originating turn's ID.
    pub async fn start_turn_if_idle_from_operation(
        &self,
        request: TurnInputRequest,
        operation: &dyn HostOperationWork,
    ) -> CodexResult<StartIfIdleSubmission> {
        let work = match operation.derive_turn_work(
            &self.session.services.thread_extension_data,
            Box::pin(self.termination_receipt()),
        ) {
            Ok(work) => work,
            Err(_) => return Ok(StartIfIdleSubmission::NotSubmitted {
                reason: NotSubmittedReason::ServerDraining,
            }),
        };
        self.session.services.agent_control
            .ensure_execution_capacity_for_turn_start(self).await?;
        match self.io.submit_turn_input(request, TurnInputMode::StartIfIdle, Some(work)).await? {
            TurnInputSubmission::Started { turn_id } => Ok(StartIfIdleSubmission::Started { turn_id }),
            TurnInputSubmission::NotSubmitted { reason } => Ok(StartIfIdleSubmission::NotSubmitted { reason }),
            TurnInputSubmission::Steered { .. } => unreachable!("start-if-idle submission cannot steer"),
        }
    }
}
