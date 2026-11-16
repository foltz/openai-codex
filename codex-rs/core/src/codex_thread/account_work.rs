//! Finite account-work entry points and truthful ordinary cleanup observation.
use super::CodexThread;
use super::ThreadCleanupOutcome;
use super::ThreadLoopOutcome;
use super::ThreadRetirementReport;
use super::ThreadShutdownOutcome;
use crate::session::SessionLoopOutcome;

impl CodexThread {
    /// A reconnect after an idle event subscription is new background work.
    /// Refuse before opening when admission is closed; do not wait for reopen
    /// or derive from the already-completed stream-start request.
    pub fn admit_mcp_event_stream_retry(
        &self,
    ) -> anyhow::Result<Option<Box<dyn codex_mcp::McpAttemptWork>>> {
        admit_retry_from_host(self.session.services.host_admission.as_deref())
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
            cleanup: ThreadCleanupOutcome::with_sandbox_cleanup_census(
                cleanup,
                self.session.services.sandbox_cleanup_census().totals(),
            ),
        }
    }

    /// Compatibility entry for an explicit finite operation. The current
    /// implementation binds work to this destination before any capacity wait.
    pub async fn start_turn_if_idle_from_operation(
        &self,
        request: codex_protocol::turn_input::TurnInputRequest,
        operation: &dyn codex_extension_api::HostOperationWork,
    ) -> codex_protocol::error::Result<codex_protocol::turn_input::StartIfIdleSubmission> {
        self.start_turn_if_idle_with_work(request, operation).await
    }
}

// Required host policy never converts a missing finite root into Ungated.
fn admit_retry_from_host(
    host: Option<&dyn codex_extension_api::TurnStartAdmission>,
) -> anyhow::Result<Option<Box<dyn codex_mcp::McpAttemptWork>>> {
    let Some(host) = host else {
        return Ok(None);
    };
    let work = host
        .admit_operation_work()
        .map_err(|_| anyhow::anyhow!("MCP event stream retry account work is unavailable"))?;
    anyhow::ensure!(
        work.is_some() || !host.requires_account_work(),
        "MCP event stream retry account work is unavailable"
    );
    Ok(work.map(|work| {
        Box::new(crate::session::McpOperationWork(work)) as Box<dyn codex_mcp::McpAttemptWork>
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_extension_api::{HostOperationWork, TurnStartAdmission, TurnWorkRefused};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct Work(Arc<AtomicUsize>);
    impl Drop for Work {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    impl HostOperationWork for Work {
        fn derive_operation(&self) -> Result<Box<dyn HostOperationWork>, TurnWorkRefused> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(Self(Arc::clone(&self.0))))
        }
    }
    struct Host {
        required: bool,
        mode: u8,
        live: Arc<AtomicUsize>,
        starts: AtomicUsize,
    }
    impl TurnStartAdmission for Host {
        fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
            panic!("not a turn start")
        }
        fn requires_account_work(&self) -> bool {
            self.required
        }
        fn derive_request_operation_work(
            &self,
        ) -> Result<Option<Box<dyn HostOperationWork>>, TurnWorkRefused> {
            panic!("idle retries never borrow an ambient request")
        }
        fn admit_operation_work(
            &self,
        ) -> Result<Option<Box<dyn HostOperationWork>>, TurnWorkRefused> {
            self.starts.fetch_add(1, Ordering::SeqCst);
            match self.mode {
                0 => Ok(None),
                1 => {
                    self.live.fetch_add(1, Ordering::SeqCst);
                    Ok(Some(Box::new(Work(Arc::clone(&self.live)))))
                }
                _ => Err(TurnWorkRefused::Unavailable),
            }
        }
    }
    #[test]
    fn ungated_idle_retry_is_absence_only_without_required_host_policy() {
        assert!(admit_retry_from_host(None).unwrap().is_none());
        let host = Host {
            required: false,
            mode: 0,
            live: Arc::default(),
            starts: AtomicUsize::new(0),
        };
        assert!(admit_retry_from_host(Some(&host)).unwrap().is_none());
        assert_eq!(host.starts.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn required_missing_or_refused_idle_retry_never_becomes_ungated() {
        for mode in [0, 2] {
            let host = Host {
                required: true,
                mode,
                live: Arc::default(),
                starts: AtomicUsize::new(0),
            };
            assert!(admit_retry_from_host(Some(&host)).is_err());
            assert_eq!(host.live.load(Ordering::SeqCst), 0);
            assert_eq!(host.starts.load(Ordering::SeqCst), 1);
        }
    }
    #[test]
    fn fresh_retry_keeps_finite_descendant_work_until_the_last_attempt_drops() {
        let host = Host {
            required: true,
            mode: 1,
            live: Arc::default(),
            starts: AtomicUsize::new(0),
        };
        let root = admit_retry_from_host(Some(&host)).unwrap().unwrap();
        let child = root.derive_attempt().unwrap();
        assert_eq!(host.starts.load(Ordering::SeqCst), 1);
        assert_eq!(host.live.load(Ordering::SeqCst), 2);
        drop(root);
        assert_eq!(host.live.load(Ordering::SeqCst), 1);
        drop(child);
        assert_eq!(host.live.load(Ordering::SeqCst), 0);
    }
}
