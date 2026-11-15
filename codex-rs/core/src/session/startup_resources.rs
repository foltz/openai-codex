//! Pre-Session resources retained independently of the initialization future.
//! Scope completion is not a constructor task join: registry cleanup must still
//! wait for that original join before driving this owner.
use super::retirement::CleanupExecution;
use super::retirement::DeadlineBindingError;
use super::session::Session;
use crate::sandboxing::cleanup_census::SandboxCleanupCensus;
use futures::FutureExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::time::Instant;

#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    Legacy,
    Bound,
}

#[derive(Default)]
struct State {
    finished: bool,
    closed: bool,
    root: Option<codex_mcp::McpRuntimeRetirement>,
    phase: Option<Phase>,
    deadline: Option<Instant>,
    shutdown_clamp: Option<Instant>,
    completion: Option<Shared<BoxFuture<'static, CleanupExecution>>>,
}

pub(super) struct StartupResources {
    census: Arc<SandboxCleanupCensus>,
    scope: codex_sandboxing::CleanupDeadlineScope,
    state: Mutex<State>,
    deadline_changed: tokio::sync::watch::Sender<Option<Instant>>,
    mcp_report: Arc<Mutex<Option<Arc<codex_mcp::RuntimeWorkerDrainReport>>>>,
}

pub(super) struct StartupResourceGuard(Arc<StartupResources>);

impl Drop for StartupResourceGuard {
    fn drop(&mut self) {
        // Dropping the scope after its initialization future has dropped must
        // not itself start cleanup or pretend that a Tokio join was observed.
        if let Ok(mut state) = self.0.state.lock() {
            state.finished = true;
            state.closed = true;
        }
    }
}

impl StartupResources {
    pub(super) fn begin() -> (Arc<Self>, StartupResourceGuard) {
        let resources = Arc::new(Self {
            census: Arc::default(),
            scope: codex_sandboxing::CleanupDeadlineScope::new(),
            state: Mutex::default(),
            deadline_changed: tokio::sync::watch::channel(None).0,
            mcp_report: Arc::default(),
        });
        let guard = StartupResourceGuard(Arc::clone(&resources));
        (resources, guard)
    }

    pub(super) fn finished(&self) -> bool {
        self.state.lock().is_ok_and(|state| state.finished)
    }

    pub(super) fn cleanup_scope(&self) -> codex_sandboxing::CleanupDeadlineScope {
        self.scope.clone()
    }

    pub(super) fn census(&self) -> Arc<SandboxCleanupCensus> {
        Arc::clone(&self.census)
    }

    /// Register the real root before synchronous runtime construction can fail.
    pub(super) fn create_mcp_runtime(
        &self,
        prefix: bool,
    ) -> Result<Arc<codex_mcp::McpRuntime>, ()> {
        let root = codex_mcp::McpRuntimeRetirement::default();
        {
            let mut state = self.state.lock().map_err(|_| ())?;
            if state.closed
                || state.finished
                || state.root.is_some()
                || state
                    .deadline
                    .is_some_and(|deadline| Instant::now() >= deadline)
                || state
                    .shutdown_clamp
                    .is_some_and(|deadline| Instant::now() >= deadline)
            {
                return Err(());
            }
            state.root = Some(root.clone());
        }
        Ok(Arc::new(codex_mcp::McpRuntime::empty_in_retirement(
            prefix, root,
        )))
    }

    pub(super) fn matches(&self, session: &Session) -> bool {
        self.state.lock().is_ok_and(|state| {
            !state.finished
                && state.completion.is_none()
                && Arc::ptr_eq(&self.census, session.services.sandbox_cleanup_census())
                && self
                    .scope
                    .same_scope(session.services.sandbox_cleanup_scope())
                && state.root.as_ref().is_some_and(|root| {
                    root.same_scope(&session.services.mcp_runtime.retirement_owner())
                })
        })
    }

    pub(super) fn bind_deadline(&self, deadline: Instant) -> Result<Instant, DeadlineBindingError> {
        self.scope.tighten(deadline.into_std());
        let mut state = self
            .state
            .lock()
            .map_err(|_| DeadlineBindingError::AuthorityUnavailable)?;
        if state.phase == Some(Phase::Legacy) {
            return Err(DeadlineBindingError::LegacyCleanupStarted);
        }
        let deadline = state
            .deadline
            .map_or(deadline, |current| current.min(deadline));
        state.deadline = Some(deadline);
        self.deadline_changed.send_replace(Some(
            state
                .shutdown_clamp
                .map_or(deadline, |clamp| clamp.min(deadline)),
        ));
        Ok(deadline)
    }

    /// Permanent manager shutdown edge only. A legacy driver keeps its phase,
    /// but no observer may drive its retained original work after this clock.
    pub(super) fn clamp_shutdown_edge(&self, deadline: Instant) {
        self.scope.tighten(deadline.into_std());
        if let Ok(mut state) = self.state.lock() {
            let clamp = state
                .shutdown_clamp
                .map_or(deadline, |current| current.min(deadline));
            state.shutdown_clamp = Some(clamp);
            self.deadline_changed
                .send_replace(Some(state.deadline.map_or(clamp, |bound| bound.min(clamp))));
        }
    }

    pub(super) async fn observe_until(
        &self,
        deadline: Instant,
    ) -> Result<CleanupExecution, DeadlineBindingError> {
        self.bind_deadline(deadline)?;
        Ok(self.observe(Phase::Bound).await)
    }

    pub(super) async fn observe_legacy(&self) -> CleanupExecution {
        self.observe(Phase::Legacy).await
    }

    async fn observe(&self, requested: Phase) -> CleanupExecution {
        let completion = {
            let Ok(mut state) = self.state.lock() else {
                return CleanupExecution::AuthorityUnavailable;
            };
            if !state.finished {
                return CleanupExecution::AuthorityUnavailable;
            }
            if state.completion.is_none() {
                if state
                    .deadline
                    .is_some_and(|deadline| Instant::now() >= deadline)
                    || state
                        .shutdown_clamp
                        .is_some_and(|deadline| Instant::now() >= deadline)
                {
                    return CleanupExecution::TimedOut;
                }
                let phase = if state.deadline.is_some() {
                    Phase::Bound
                } else {
                    requested
                };
                state.phase = Some(phase);
                let root = state.root.clone();
                let census = Arc::clone(&self.census);
                let report_slot = Arc::clone(&self.mcp_report);
                // Legacy keeps its historical MCP budget, never new deadline authority.
                let deadline = state
                    .deadline
                    .unwrap_or_else(|| Instant::now() + std::time::Duration::from_secs(30));
                state.completion = Some(
                    async move {
                        let outcome = if let Some(root) = root {
                            match std::panic::AssertUnwindSafe(root.drain_workers_until(deadline))
                                .catch_unwind()
                                .await
                            {
                                Ok(report) => {
                                    let complete = report.are_recorded_workers_drained();
                                    *report_slot
                                        .lock()
                                        .unwrap_or_else(std::sync::PoisonError::into_inner) =
                                        Some(Arc::new(report));
                                    if complete {
                                        CleanupExecution::Finished {
                                            persistence_failed: false,
                                        }
                                    } else {
                                        CleanupExecution::McpFailed
                                    }
                                }
                                Err(_) => CleanupExecution::Panicked,
                            }
                        } else {
                            // No runtime factory was entered before the scope ended.
                            CleanupExecution::Finished {
                                persistence_failed: false,
                            }
                        };
                        outcome.with_sandbox_cleanup_census(census.totals())
                    }
                    .boxed()
                    .shared(),
                );
            }
            state.completion.as_ref().unwrap().clone()
        };
        let mut changes = self.deadline_changed.subscribe();
        loop {
            if let Some(outcome) = completion.peek() {
                return *outcome;
            }
            let deadline = *changes.borrow_and_update();
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return CleanupExecution::TimedOut;
            }
            let expiry = async move {
                match deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                biased;
                changed = changes.changed() => if changed.is_err() { return CleanupExecution::AuthorityUnavailable; },
                _ = expiry => return CleanupExecution::TimedOut,
                outcome = completion.clone() => return outcome,
            }
        }
    }
}

#[cfg(test)]
mod tests;
