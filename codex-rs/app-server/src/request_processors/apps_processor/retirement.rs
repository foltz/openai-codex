use codex_mcp::McpRuntimeRetirement;
use codex_mcp::RuntimeTerminationReport;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use tokio::time::Instant;

#[derive(Default)]
struct State {
    closed: bool,
    deadline: Option<Instant>,
    runtimes: Vec<McpRuntimeRetirement>,
    terminal: Option<AppsRuntimeDrain>,
}

#[derive(Default)]
pub(super) struct AppsRuntimeOwners {
    state: Arc<Mutex<State>>,
}

#[derive(Clone)]
pub(super) struct AppsRuntimeTicket(Weak<Mutex<State>>);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AppsRuntimeDrain {
    pub unavailable: bool,
    pub reports: Vec<RuntimeTerminationReport>,
}

impl AppsRuntimeTicket {
    pub(super) fn reserve(&self) -> Result<McpRuntimeRetirement, ()> {
        let state = self.0.upgrade().ok_or(())?;
        let mut state = state.lock().map_err(|_| ())?;
        if state.closed {
            return Err(());
        }
        state.runtimes.retain(|runtime| !runtime.is_retired());
        let runtime = McpRuntimeRetirement::default();
        state.runtimes.push(runtime.clone());
        Ok(runtime)
    }
}

impl AppsRuntimeOwners {
    pub(super) fn ticket(&self) -> AppsRuntimeTicket {
        AppsRuntimeTicket(Arc::downgrade(&self.state))
    }

    pub(super) fn close(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.closed = true;
        for runtime in &state.runtimes {
            runtime.close_registration();
        }
    }

    pub(super) async fn shutdown_until(&self, deadline: Instant) -> AppsRuntimeDrain {
        self.close();
        let (deadline, runtimes, unavailable) = {
            let locked = self.state.lock();
            let unavailable = locked.is_err();
            let mut state = locked.unwrap_or_else(std::sync::PoisonError::into_inner);
            if !unavailable && let Some(report) = &state.terminal {
                return report.clone();
            }
            let deadline = *state.deadline.get_or_insert(deadline);
            (deadline, state.runtimes.clone(), unavailable)
        };
        let reports = futures::future::join_all(
            runtimes
                .iter()
                .map(|runtime| runtime.shutdown_until(deadline)),
        )
        .await;
        // Only cached positive proof releases custody, never the caller's join.
        let report = AppsRuntimeDrain {
            unavailable,
            reports,
        };
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !report.unavailable
            && report
                .reports
                .iter()
                .all(RuntimeTerminationReport::is_complete)
        {
            state.runtimes.clear();
            state.terminal = Some(report.clone());
        }
        report
    }
}

#[cfg(test)]
#[path = "retirement_tests.rs"]
mod tests;
