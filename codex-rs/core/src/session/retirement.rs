//! Cancellation-safe ownership of the common session teardown sequence.
//!
//! This execution receipt is not yet a terminal receipt for every resource.
//! In particular, legacy cleanup steps still have their existing error policy.

use super::session::Session;
use futures::FutureExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::watch;
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CleanupExecution {
    Finished { persistence_failed: bool },
    Panicked,
    TimedOut,
    AuthorityUnavailable,
    McpFailed,
    TaskJoinFailed,
    ConversationShutdownFailed,
    CodeModeShutdownFailed,
}

#[derive(Clone, Debug)]
enum McpCleanup {
    Observed(Arc<codex_mcp::RuntimeTerminationReport>),
    Panicked,
}

#[derive(Clone, Copy)]
pub(super) enum CleanupMode {
    Legacy,
    DeadlineBound,
}

#[derive(Default)]
struct CleanupState {
    completion: Option<Shared<BoxFuture<'static, CleanupExecution>>>,
    mcp_completion: Option<Shared<BoxFuture<'static, McpCleanup>>>,
    task_completion: Option<Shared<BoxFuture<'static, crate::tasks::TaskJoinOutcome>>>,
    common_completion: Option<Shared<BoxFuture<'static, CleanupExecution>>>,
    deadline: Option<Instant>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeadlineBindingError {
    LegacyCleanupStarted,
    AuthorityUnavailable,
}

pub(crate) struct SessionCleanupOwner {
    state: Mutex<CleanupState>,
    deadline_changed: watch::Sender<Option<Instant>>,
}

impl Default for SessionCleanupOwner {
    fn default() -> Self {
        Self {
            state: Mutex::new(CleanupState::default()),
            deadline_changed: watch::channel(None).0,
        }
    }
}

impl SessionCleanupOwner {
    /// Read an already recorded cleanup result without creating or polling work.
    /// This does not retroactively bind legacy cleanup to a later deadline.
    pub(crate) fn completed(&self) -> Option<CleanupExecution> {
        let state = self.state.try_lock().ok()?;
        state.completion.as_ref()?.peek().copied()
    }

    /// Must precede ordinary Shutdown submission. An existing unbounded cleanup
    /// cannot retroactively establish the exact-thread deadline contract.
    pub(crate) fn bind_deadline(&self, deadline: Instant) -> Result<Instant, DeadlineBindingError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DeadlineBindingError::AuthorityUnavailable)?;
        if state.completion.is_some() && state.deadline.is_none() {
            return Err(DeadlineBindingError::LegacyCleanupStarted);
        }
        let deadline = state
            .deadline
            .map_or(deadline, |current| current.min(deadline));
        state.deadline = Some(deadline);
        self.deadline_changed.send_replace(Some(deadline));
        Ok(deadline)
    }

    pub(crate) async fn observe(&self, session: Arc<Session>) -> CleanupExecution {
        let completion = {
            let Ok(mut state) = self.state.lock() else {
                return CleanupExecution::AuthorityUnavailable;
            };
            if state.completion.is_none() {
                let mcp = state.deadline.map(|deadline| {
                    let session = Arc::clone(&session);
                    async move {
                        // Freeze refresh admission without waiting behind a
                        // network handshake. The runtime registry owns and
                        // drains already-admitted physical attempts.
                        session.mcp_refresh.close();
                        match AssertUnwindSafe(
                            session.services.mcp_runtime.shutdown_until(deadline),
                        )
                        .catch_unwind()
                        .await
                        {
                            Ok(report) => McpCleanup::Observed(Arc::new(report)),
                            Err(_) => McpCleanup::Panicked,
                        }
                    }
                    .boxed()
                    .shared()
                });
                state.mcp_completion = mcp.clone();
                let tasks = state.deadline.map(|deadline| {
                    let session = Arc::clone(&session);
                    async move {
                        AssertUnwindSafe(async {
                            session.close_task_admission().await;
                            session.task_joins.shutdown_until(deadline).await
                        })
                        .catch_unwind()
                        .await
                        .unwrap_or(crate::tasks::TaskJoinOutcome::Failed)
                    }
                    .boxed()
                    .shared()
                });
                state.task_completion = tasks.clone();
                let mode = if mcp.is_some() {
                    CleanupMode::DeadlineBound
                } else {
                    CleanupMode::Legacy
                };
                let common = async move {
                    match AssertUnwindSafe(super::handlers::cleanup_session(&session, mode))
                        .catch_unwind()
                        .await
                    {
                        Ok(result) => result,
                        Err(_) => CleanupExecution::Panicked,
                    }
                }
                .boxed()
                .shared();
                state.common_completion = Some(common.clone());
                state.completion = Some(
                    async move {
                        if let (Some(mcp), Some(tasks)) = (mcp, tasks) {
                            // Neither common cleanup failure nor a pending lock
                            // suppresses the independent MCP proof driver.
                            let (mcp, tasks, common) = tokio::join!(mcp, tasks, common);
                            if common == CleanupExecution::Panicked {
                                return common;
                            }
                            if !matches!(tasks, crate::tasks::TaskJoinOutcome::Complete { .. }) {
                                return CleanupExecution::TaskJoinFailed;
                            }
                            match mcp {
                                McpCleanup::Observed(report) if report.is_complete() => common,
                                McpCleanup::Observed(_) | McpCleanup::Panicked => {
                                    CleanupExecution::McpFailed
                                }
                            }
                        } else {
                            common.await
                        }
                    }
                    .boxed()
                    .shared(),
                );
            }
            match &state.completion {
                Some(completion) => completion.clone(),
                None => return CleanupExecution::AuthorityUnavailable,
            }
        };
        let mut changes = self.deadline_changed.subscribe();
        loop {
            if let Some(result) = completion.peek() {
                return *result;
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
                changed = changes.changed() => {
                    if changed.is_err() {
                        return CleanupExecution::AuthorityUnavailable;
                    }
                }
                _ = expiry => return CleanupExecution::TimedOut,
                result = completion.clone() => return result,
            }
        }
    }
}

impl Session {
    /// The loop and its external observation retain the owner strongly. The
    /// back-reference is weak because the retained cleanup future owns Session.
    pub(crate) fn cleanup_owner(&self) -> Arc<SessionCleanupOwner> {
        let mut slot = self
            .cleanup_owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(owner) = slot.upgrade() {
            return owner;
        }
        let owner = Arc::new(SessionCleanupOwner::default());
        *slot = Arc::downgrade(&owner);
        owner
    }
}

#[cfg(test)]
#[path = "retirement_tests.rs"]
mod tests;
