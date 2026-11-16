//! Birth-time custody for independently censused processor task families.

use futures::FutureExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProcessorTaskJoin {
    Joined,
    Cancelled,
    Panicked,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ProcessorTaskDrain {
    pub terminal: bool,
    pub cancelled: bool,
    pub panicked: bool,
    pub unavailable: bool,
    /// The wrapper joined, but a dependency-owned subordinate had no
    /// observable terminal receipt. This remains sticky and blocks cleanup
    /// completion rather than being mistaken for a clean join.
    pub unverified: bool,
}

impl ProcessorTaskDrain {
    pub(crate) fn is_clean(self) -> bool {
        self.terminal && !self.cancelled && !self.panicked && !self.unavailable && !self.unverified
    }
}

type Receipt = Shared<BoxFuture<'static, ProcessorTaskJoin>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProcessorTaskAdmissionError {
    Closed,
    Unavailable,
}

#[derive(Default)]
struct State {
    closed: bool,
    deadline: Option<Instant>,
    tasks: Vec<Receipt>,
    cancelled: bool,
    panicked: bool,
    unverified: bool,
}

impl State {
    fn compact(&mut self) {
        self.tasks
            .retain(|receipt| match receipt.clone().now_or_never() {
                Some(ProcessorTaskJoin::Joined) => false,
                Some(ProcessorTaskJoin::Cancelled) => {
                    self.cancelled = true;
                    false
                }
                Some(ProcessorTaskJoin::Panicked) => {
                    self.panicked = true;
                    false
                }
                None => true,
            });
    }
}

/// Owns joins, not a detached reaper. Task captures must not own this registry.
/// Shutdown closes the same lock used for birth and keeps every original
/// until actual join observation; compaction retains bounded failure facts.
#[derive(Clone, Default)]
pub(crate) struct ProcessorTasks {
    state: Arc<Mutex<State>>,
}

/// Nested work may register into its parent's census without retaining that
/// census through the task future owned by it.
#[derive(Clone)]
pub(crate) struct ProcessorTaskTicket {
    state: Weak<Mutex<State>>,
}

impl ProcessorTaskTicket {
    pub(crate) fn spawn(
        &self,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> Result<Receipt, ProcessorTaskAdmissionError> {
        let state = self
            .state
            .upgrade()
            .ok_or(ProcessorTaskAdmissionError::Unavailable)?;
        ProcessorTasks { state }.spawn(future)
    }
}

impl ProcessorTasks {
    pub(crate) fn ticket(&self) -> ProcessorTaskTicket {
        ProcessorTaskTicket {
            state: Arc::downgrade(&self.state),
        }
    }

    pub(crate) fn close_registration(&self) -> Result<(), ProcessorTaskAdmissionError> {
        self.state
            .lock()
            .map_err(|_| ProcessorTaskAdmissionError::Unavailable)?
            .closed = true;
        Ok(())
    }

    pub(crate) fn spawn(
        &self,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> Result<Receipt, ProcessorTaskAdmissionError> {
        self.spawn_inner(future, false)
    }

    pub(crate) fn spawn_unverified(
        &self,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> Result<Receipt, ProcessorTaskAdmissionError> {
        self.spawn_inner(future, true)
    }

    fn spawn_inner(
        &self,
        future: impl Future<Output = ()> + Send + 'static,
        unverified: bool,
    ) -> Result<Receipt, ProcessorTaskAdmissionError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ProcessorTaskAdmissionError::Unavailable)?;
        if state.closed {
            return Err(ProcessorTaskAdmissionError::Closed);
        }
        state.compact();
        let task = tokio::spawn(future);
        let receipt = async move {
            match task.await {
                Ok(()) => ProcessorTaskJoin::Joined,
                Err(error) if error.is_cancelled() => ProcessorTaskJoin::Cancelled,
                Err(_) => ProcessorTaskJoin::Panicked,
            }
        }
        .boxed()
        .shared();
        state.tasks.push(receipt.clone());
        state.unverified |= unverified;
        Ok(receipt)
    }

    pub(crate) async fn shutdown_until(&self, deadline: Instant) -> ProcessorTaskDrain {
        let (deadline, tasks) = {
            let Ok(mut state) = self.state.lock() else {
                return ProcessorTaskDrain {
                    unavailable: true,
                    ..Default::default()
                };
            };
            state.closed = true;
            let deadline = *state.deadline.get_or_insert(deadline);
            state.compact();
            (deadline, state.tasks.clone())
        };
        if !tasks.is_empty() && Instant::now() < deadline {
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline) => {},
                _ = futures::future::join_all(tasks) => {},
            }
        }
        let Ok(mut state) = self.state.lock() else {
            return ProcessorTaskDrain {
                unavailable: true,
                ..Default::default()
            };
        };
        // Only polls pure JoinHandle observers, never the spawned task body.
        state.compact();
        ProcessorTaskDrain {
            terminal: state.tasks.is_empty(),
            cancelled: state.cancelled,
            panicked: state.panicked,
            unavailable: false,
            unverified: state.unverified,
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.state.lock().unwrap().tasks.len()
    }
}

#[cfg(test)]
#[path = "processor_task_retirement_tests.rs"]
mod tests;
