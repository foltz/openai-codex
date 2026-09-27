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
    /// A reconnect after an idle event subscription is new background work.
    /// Refuse before opening when admission is closed; do not wait for reopen
    /// or derive from the already-completed stream-start request.
    pub fn admit_mcp_event_stream_retry(
        &self,
    ) -> anyhow::Result<Option<Box<dyn codex_mcp::McpAttemptWork>>> {
        let Some(host) = &self.session.services.host_admission else {
            return Ok(None);
        };
        host.admit_operation_work()
            .map(|work| {
                work.map(|work| {
                    Box::new(crate::session::McpOperationWork(work))
                        as Box<dyn codex_mcp::McpAttemptWork>
                })
            })
            .map_err(|_| anyhow::anyhow!("MCP event stream retry account work is unavailable"))
    }

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
        let cleanup = self
            .session
            .cleanup_owner()
            .observe(std::sync::Arc::clone(&self.session))
            .await;
        ThreadRetirementReport {
            ordinary,
            session_loop,
            cleanup: ThreadCleanupOutcome::from(cleanup),
        }
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
            Err(_) => {
                return Ok(StartIfIdleSubmission::NotSubmitted {
                    reason: NotSubmittedReason::ServerDraining,
                });
            }
        };
        self.session
            .services
            .agent_control
            .ensure_execution_capacity_for_turn_start(self)
            .await?;
        match self
            .io
            .submit_turn_input(request, TurnInputMode::StartIfIdle, Some(work))
            .await?
        {
            TurnInputSubmission::Started { turn_id } => {
                Ok(StartIfIdleSubmission::Started { turn_id })
            }
            TurnInputSubmission::NotSubmitted { reason } => {
                Ok(StartIfIdleSubmission::NotSubmitted { reason })
            }
            TurnInputSubmission::Steered { .. } => {
                unreachable!("start-if-idle submission cannot steer")
            }
        }
    }
}
