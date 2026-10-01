//! Join ownership for running and normally-finishing session tasks.

use futures::FutureExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::task::AbortHandle;
use tokio::task::JoinHandle;
use tokio::time::Instant;

type Completion = Shared<BoxFuture<'static, bool>>;

#[derive(Clone)]
struct Entry {
    abort: AbortHandle,
    completion: Completion,
    active: Arc<AtomicBool>,
}

#[derive(Default)]
struct State {
    closed: bool,
    failed: bool,
    panicked: bool,
    entries: Vec<Entry>,
}

/// Retains actual joins even after a task removes itself from the active slot.
#[derive(Default)]
pub(crate) struct TaskJoinRegistry {
    state: Mutex<State>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TaskJoinOutcome {
    Complete { panicked: bool },
    Failed,
    TimedOut,
}

/// Keeps the active-slot's abort-on-drop behavior without owning the join.
pub(crate) struct TaskAbortHandle {
    abort: AbortHandle,
    armed: bool,
    active: Arc<AtomicBool>,
    completion: Option<Completion>,
}

impl TaskAbortHandle {
    pub(crate) fn is_finished(&self) -> bool {
        self.abort.is_finished()
    }

    pub(crate) async fn wait(&self) -> bool {
        match &self.completion {
            Some(completion) => completion.clone().await,
            None => false,
        }
    }

    pub(crate) fn abort(&self) {
        self.abort.abort();
    }

    pub(crate) fn detach(mut self) {
        self.armed = false;
    }
}

impl Drop for TaskAbortHandle {
    fn drop(&mut self) {
        self.active.store(false, Ordering::Release);
        if self.armed {
            self.abort.abort();
        }
    }
}

impl TaskJoinRegistry {
    /// Called under the session's active-turn admission lock immediately after
    /// spawn. Even a violated closed gate retains the late join and fails closed.
    pub(crate) fn register(&self, handle: JoinHandle<()>) -> TaskAbortHandle {
        let abort = handle.abort_handle();
        let active = Arc::new(AtomicBool::new(true));
        let mut capability = TaskAbortHandle {
            abort: abort.clone(),
            armed: true,
            active: Arc::clone(&active),
            completion: None,
        };
        let completion = async move {
            match handle.await {
                Ok(()) => true,
                Err(error) => error.is_cancelled(),
            }
        }
        .boxed()
        .shared();
        capability.completion = Some(completion.clone());
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.failed |= self.state.is_poisoned();
        let mut panicked = false;
        // These futures only poll Tokio joins; no task body or cleanup callback
        // is run here. Reaping prevents retention of every historical task.
        state.entries.retain(|entry| {
            if entry.active.load(Ordering::Acquire) || !entry.abort.is_finished() {
                return true;
            }
            match entry.completion.clone().now_or_never() {
                Some(ok) => {
                    panicked |= !ok;
                    false
                }
                None => true,
            }
        });
        state.panicked |= panicked;
        if state.closed {
            state.failed = true;
            abort.abort();
        }
        state.entries.push(Entry {
            abort,
            completion,
            active,
        });
        capability
    }

    /// The caller closes session task admission before invoking this method.
    /// Timeouts drop only observers; the registry retains all original joins.
    pub(crate) async fn shutdown_until(&self, deadline: Instant) -> TaskJoinOutcome {
        let entries = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.closed = true;
            state.failed |= self.state.is_poisoned();
            // Replay only settled observations before checking the deadline.
            // Do not poll a previously unobserved join outside the budget.
            let mut panicked = false;
            state.entries.retain(|entry| match entry.completion.peek() {
                Some(ok) => {
                    panicked |= !ok;
                    false
                }
                None => true,
            });
            state.panicked |= panicked;
            if state.entries.is_empty() {
                return if state.failed {
                    TaskJoinOutcome::Failed
                } else {
                    TaskJoinOutcome::Complete {
                        panicked: state.panicked,
                    }
                };
            }
            state.entries.clone()
        };
        if Instant::now() >= deadline {
            return TaskJoinOutcome::TimedOut;
        }
        for entry in &entries {
            entry.abort.abort();
        }
        let completed =
            futures::future::join_all(entries.iter().map(|entry| entry.completion.clone()));
        let Ok(outcomes) = tokio::time::timeout_at(deadline, completed).await else {
            return TaskJoinOutcome::TimedOut;
        };
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.failed |= self.state.is_poisoned();
        state.panicked |= outcomes.into_iter().any(|ok| !ok);
        state
            .entries
            .retain(|entry| entry.completion.peek().is_none());
        if state.failed {
            TaskJoinOutcome::Failed
        } else {
            TaskJoinOutcome::Complete {
                panicked: state.panicked,
            }
        }
    }
}
