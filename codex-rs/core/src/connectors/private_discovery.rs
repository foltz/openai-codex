//! Retirement for the private app-list routine, not general MCP termination.
//! Only connectors.rs can construct through this control. Its local runtime
//! uses a fresh context without a shared pool and never opens event streams.
use codex_mcp::McpRuntime;
use codex_mcp::McpRuntimeRetirement;
use codex_mcp::RuntimeWorkerDrainReport;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::time::Instant;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AppDiscoveryTerminationReport {
    /// The one-shot scope closed before constructing any runtime.
    Unused,
    PrivateRuntime(Box<RuntimeWorkerDrainReport>),
    TimedOut,
    Unavailable,
}
impl AppDiscoveryTerminationReport {
    pub fn is_complete(&self) -> bool {
        match self {
            Self::Unused => true,
            Self::PrivateRuntime(report) => {
                report.are_recorded_workers_drained()
                    && report
                        .connections
                        .connections
                        .iter()
                        .all(|connection| connection.shared.is_none())
            }
            Self::TimedOut | Self::Unavailable => false,
        }
    }
}
#[derive(Default)]
struct State {
    root: McpRuntimeRetirement,
    entered: bool,
    started: bool,
    closed: bool,
    deadline: Option<Instant>,
    terminal: Option<AppDiscoveryTerminationReport>,
}
/// External custody, without access to the runtime or its private root.
#[derive(Clone, Default)]
pub struct AppDiscoveryRetirement(Arc<Mutex<State>>);

pub(super) struct DiscoveryScope(AppDiscoveryRetirement);
impl Drop for DiscoveryScope {
    fn drop(&mut self) {
        let root = {
            let Ok(mut state) = self.0.0.lock() else {
                return;
            };
            if state.started {
                return;
            }
            state.closed = true;
            state.terminal = Some(AppDiscoveryTerminationReport::Unused);
            state.root.clone()
        };
        root.close_registration();
    }
}
impl AppDiscoveryRetirement {
    pub(super) fn scope(&self) -> anyhow::Result<DiscoveryScope> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("app discovery custody unavailable"))?;
        anyhow::ensure!(
            !state.closed && !state.entered,
            "app discovery custody is closed or already used"
        );
        state.entered = true;
        Ok(DiscoveryScope(self.clone()))
    }
    /// Synchronous construction and registration share the close gate. The
    /// returned runtime stays local to the fixed discovery routine.
    pub(super) fn begin(&self, prefix: bool) -> anyhow::Result<McpRuntime> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("app discovery custody unavailable"))?;
        anyhow::ensure!(
            state.entered && !state.closed && !state.started,
            "app discovery startup refused"
        );
        let runtime = McpRuntime::empty_in_retirement(prefix, state.root.clone());
        state.started = true;
        Ok(runtime)
    }
    pub fn close_registration(&self) {
        let root = {
            let mut state = self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.closed = true;
            state.root.clone()
        };
        root.close_registration();
    }
    pub fn is_retired(&self) -> bool {
        self.0.lock().is_ok_and(|state| {
            state
                .terminal
                .as_ref()
                .is_some_and(AppDiscoveryTerminationReport::is_complete)
        })
    }
    /// The first deadline wins. Expired observations do not start new cleanup.
    /// Abandoning the observer never drops the externally retained root.
    pub async fn shutdown_until(&self, deadline: Instant) -> AppDiscoveryTerminationReport {
        let (root, deadline, started) = {
            let mut state = match self.0.lock() {
                Ok(state) => state,
                Err(error) => {
                    let mut state = error.into_inner();
                    state.closed = true;
                    let root = state.root.clone();
                    drop(state);
                    root.close_registration();
                    return AppDiscoveryTerminationReport::Unavailable;
                }
            };
            if let Some(report) = &state.terminal {
                return report.clone();
            }
            state.closed = true;
            let deadline = *state.deadline.get_or_insert(deadline);
            (state.root.clone(), deadline, state.started)
        };
        root.close_registration();
        let report = if !started {
            AppDiscoveryTerminationReport::Unused
        } else if Instant::now() >= deadline {
            AppDiscoveryTerminationReport::TimedOut
        } else {
            AppDiscoveryTerminationReport::PrivateRuntime(Box::new(
                root.drain_workers_until(deadline).await,
            ))
        };
        if report.is_complete() {
            let Ok(mut state) = self.0.lock() else {
                return AppDiscoveryTerminationReport::Unavailable;
            };
            state.terminal = Some(report.clone());
        }
        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn unused_scope_is_closed_without_claiming_a_constructed_runtime() {
        let owner = AppDiscoveryRetirement::default();
        let scope = owner.scope().unwrap();
        drop(scope);
        assert_eq!(
            owner.shutdown_until(Instant::now()).await,
            AppDiscoveryTerminationReport::Unused
        );
        assert!(owner.is_retired());
        assert!(owner.scope().is_err());
        assert!(owner.begin(true).is_err());
    }
    #[tokio::test]
    async fn closure_refuses_construction_before_any_runtime_worker_is_registered() {
        let owner = AppDiscoveryRetirement::default();
        let _scope = owner.scope().unwrap();
        owner.close_registration();
        assert!(owner.begin(true).is_err());
        assert_eq!(
            owner.shutdown_until(Instant::now()).await,
            AppDiscoveryTerminationReport::Unused
        );
    }
    #[tokio::test]
    async fn actual_runtime_join_is_retained_when_the_first_observer_is_dropped() {
        let owner = AppDiscoveryRetirement::default();
        let _scope = owner.scope().unwrap();
        let runtime = owner.begin(true).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut observation = Box::pin(owner.shutdown_until(deadline));
        assert!(futures::poll!(observation.as_mut()).is_pending());
        drop(observation);
        drop(runtime);
        let report = owner.shutdown_until(deadline).await;
        assert!(report.is_complete(), "{report:?}");
        assert!(matches!(
            report,
            AppDiscoveryTerminationReport::PrivateRuntime(_)
        ));
        assert!(owner.is_retired());
        assert_eq!(owner.shutdown_until(Instant::now()).await, report);
    }
    #[tokio::test(start_paused = true)]
    async fn expired_first_observation_never_starts_cleanup_or_accepts_a_fresh_budget() {
        let owner = AppDiscoveryRetirement::default();
        let _scope = owner.scope().unwrap();
        let runtime = owner.begin(true).unwrap();
        let deadline = Instant::now();
        assert_eq!(
            owner.shutdown_until(deadline).await,
            AppDiscoveryTerminationReport::TimedOut
        );
        assert_eq!(
            owner
                .shutdown_until(deadline + Duration::from_secs(10))
                .await,
            AppDiscoveryTerminationReport::TimedOut
        );
        assert_eq!(Instant::now(), deadline);
        let workers = runtime.worker_owner().observe_until(Instant::now()).await;
        assert_eq!(workers.registered_at_end, 0);
        assert!(!owner.is_retired());
        // Explicit test teardown, not completion under the expired observation.
        runtime.shutdown().await;
    }
    #[tokio::test]
    async fn poisoned_custody_refuses_without_relocking_or_claiming_retirement() {
        let owner = AppDiscoveryRetirement::default();
        let _scope = owner.scope().unwrap();
        let runtime = owner.begin(true).unwrap();
        let state = Arc::clone(&owner.0);
        assert!(
            std::thread::spawn(move || {
                let _guard = state.lock().unwrap();
                panic!("controlled discovery custody poison");
            })
            .join()
            .is_err()
        );
        assert_eq!(
            owner
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await,
            AppDiscoveryTerminationReport::Unavailable
        );
        assert!(!owner.is_retired());
        assert!(owner.begin(true).is_err());
        runtime.shutdown().await;
    }
}
