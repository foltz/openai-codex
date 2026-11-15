//! Original runtime worker joins. An open-admission snapshot is not a closed
//! runtime census, protocol acknowledgement, or physical transport proof.

use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::task::AbortHandle;

use futures::FutureExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use tokio::runtime::Handle;
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpWorkerKind {
    StartupSummary,
    DeferredStartup,
    Reconnect,
    DiscoveryCleanup,
    AuthNotification,
    ConnectionShutdown,
    RuntimeShutdown,
    EagerStartup,
    EmaRevocation,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum WorkerExit {
    Joined,
    Panicked,
    Cancelled { stop_requested: bool },
}

pub(crate) type Completion = Shared<BoxFuture<'static, WorkerExit>>;

/// Worker exit counts are not protocol acknowledgements or physical retirement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpWorkerCounts {
    pub kind: McpWorkerKind,
    pub joined: u64,
    pub panicked: u64,
    pub cancelled: u64,
    /// Subset of cancelled with an explicit stop request before observed exit.
    /// This is not a causal assertion or positive physical-retirement proof.
    pub requested_cancelled: u64,
    pub pending: u64,
}

impl McpWorkerCounts {
    fn empty(kind: McpWorkerKind) -> Self {
        Self {
            kind,
            joined: 0,
            panicked: 0,
            cancelled: 0,
            requested_cancelled: 0,
            pending: 0,
        }
    }

    fn add(&mut self, exit: WorkerExit) {
        match exit {
            WorkerExit::Joined => self.joined += 1,
            WorkerExit::Panicked => self.panicked += 1,
            WorkerExit::Cancelled { stop_requested } => {
                self.cancelled += 1;
                self.requested_cancelled += u64::from(stop_requested);
            }
        }
    }
}

/// Snapshot only: admission stays open and another chat may create more work.
/// A positive join is never substituted for a lower physical-retirement report.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpWorkerSnapshot {
    pub registered_at_start: u64,
    pub registered_at_end: u64,
    pub counts: Vec<McpWorkerCounts>,
    pub poisoned: bool,
}

struct State {
    registered: u64,
    completed: Vec<McpWorkerCounts>,
    pending: Vec<(McpWorkerKind, Completion)>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            registered: 0,
            completed: [
                McpWorkerKind::StartupSummary,
                McpWorkerKind::DeferredStartup,
                McpWorkerKind::Reconnect,
                McpWorkerKind::DiscoveryCleanup,
                McpWorkerKind::AuthNotification,
                McpWorkerKind::ConnectionShutdown,
                McpWorkerKind::RuntimeShutdown,
                McpWorkerKind::EagerStartup,
                McpWorkerKind::EmaRevocation,
            ]
            .into_iter()
            .map(McpWorkerCounts::empty)
            .collect(),
            pending: Vec::new(),
        }
    }
}

fn index(kind: McpWorkerKind) -> usize {
    match kind {
        McpWorkerKind::StartupSummary => 0,
        McpWorkerKind::DeferredStartup => 1,
        McpWorkerKind::Reconnect => 2,
        McpWorkerKind::DiscoveryCleanup => 3,
        McpWorkerKind::AuthNotification => 4,
        McpWorkerKind::ConnectionShutdown => 5,
        McpWorkerKind::RuntimeShutdown => 6,
        McpWorkerKind::EagerStartup => 7,
        McpWorkerKind::EmaRevocation => 8,
    }
}

impl State {
    fn compact(&mut self, poll: bool) {
        let completed = &mut self.completed;
        // Only Tokio JoinHandles are polled, never an undriven worker body.
        self.pending.retain(|(kind, completion)| {
            let exit = if poll {
                completion.clone().now_or_never()
            } else {
                completion.peek().copied()
            };
            if let Some(exit) = exit {
                completed[index(*kind)].add(exit);
                false
            } else {
                true
            }
        });
    }
}

#[derive(Clone, Default)]
pub struct McpWorkerOwner {
    state: Arc<Mutex<State>>,
}

/// Preserve managed auth's final-owner abort semantics while the ledger owns
/// and observes the original JoinHandle, including its cancellation outcome.
pub(crate) struct McpAbortOnDrop {
    abort: TrackedAbort,
}

impl McpAbortOnDrop {
    pub(crate) fn stop(&self) {
        self.abort.stop();
    }
}

impl Drop for McpAbortOnDrop {
    fn drop(&mut self) {
        self.stop();
    }
}

struct TrackedAbort {
    handle: AbortHandle,
    requested: Arc<AtomicBool>,
}

impl TrackedAbort {
    fn stop(&self) {
        // Never reclassify an already finished task because its last guard was
        // dropped late. Record intent before requesting Tokio cancellation;
        // only the original JoinHandle can establish the actual exit outcome.
        if !self.handle.is_finished() {
            self.requested.store(true, Ordering::SeqCst);
            self.handle.abort();
        }
    }
}

struct EagerAbortGroup(Vec<TrackedAbort>);

impl Drop for EagerAbortGroup {
    fn drop(&mut self) {
        for abort in &self.0 {
            abort.stop();
        }
    }
}

impl McpWorkerOwner {
    /// Like the previous JoinSet batch, failure or caller cancellation aborts
    /// siblings. Every child's original join stays observable after that abort.
    /// Payloads travel through a separate channel: the ledger must not become
    /// an extra ordinary owner of completed managed clients.
    pub(crate) async fn join_eager_batch<T: Send + 'static>(
        &self,
        bodies: impl IntoIterator<Item = impl Future<Output = T> + Send + 'static>,
    ) -> Result<Vec<T>, String> {
        use futures::StreamExt;
        let mut aborts = EagerAbortGroup(Vec::new());
        let mut pending = futures::stream::FuturesUnordered::new();
        for body in bodies {
            let (send, receive) = tokio::sync::oneshot::channel();
            let (completion, abort) = self.spawn_tracked(
                &Handle::current(),
                McpWorkerKind::EagerStartup,
                async move {
                    let _ = send.send(body.await);
                },
            );
            aborts.0.push(abort);
            pending.push(async move {
                let value = receive.await;
                match (value, completion.await) {
                    (Ok(value), WorkerExit::Joined) => Ok(value),
                    (_, outcome) => Err(format!(
                        "eager startup worker result unavailable: {outcome:?}"
                    )),
                }
            });
        }
        let mut outputs = Vec::new();
        while let Some(result) = pending.next().await {
            outputs.push(result?);
        }
        aborts.0.clear();
        Ok(outputs)
    }

    pub(crate) fn spawn_result<T: Clone + Send + Sync + 'static>(
        &self,
        runtime: &Handle,
        kind: McpWorkerKind,
        body: impl Future<Output = T> + Send + 'static,
    ) -> Shared<BoxFuture<'static, Result<T, String>>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.compact(true);
        let handle = runtime.spawn(body);
        let result = async move {
            match handle.await {
                Ok(value) => Ok(value),
                Err(error) => Err((
                    if error.is_panic() {
                        WorkerExit::Panicked
                    } else {
                        WorkerExit::Cancelled {
                            stop_requested: false,
                        }
                    },
                    error.to_string(),
                )),
            }
        }
        .boxed()
        .shared();
        let observer = result
            .clone()
            .map(|result| match result {
                Ok(_) => WorkerExit::Joined,
                Err((outcome, _)) => outcome,
            })
            .boxed()
            .shared();
        state.registered += 1;
        state.pending.push((kind, observer));
        result
            .map(|result| result.map_err(|(_, error)| error))
            .boxed()
            .shared()
    }

    pub(crate) fn spawn(
        &self,
        runtime: &Handle,
        kind: McpWorkerKind,
        body: impl Future<Output = ()> + Send + 'static,
    ) -> Completion {
        self.spawn_tracked(runtime, kind, body).0
    }

    pub(crate) fn spawn_abort_on_drop(
        &self,
        runtime: &Handle,
        kind: McpWorkerKind,
        body: impl Future<Output = ()> + Send + 'static,
    ) -> McpAbortOnDrop {
        let (_, abort) = self.spawn_tracked(runtime, kind, body);
        McpAbortOnDrop { abort }
    }

    fn spawn_tracked(
        &self,
        runtime: &Handle,
        kind: McpWorkerKind,
        body: impl Future<Output = ()> + Send + 'static,
    ) -> (Completion, TrackedAbort) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.compact(true);
        let handle = runtime.spawn(body);
        let requested = Arc::new(AtomicBool::new(false));
        let abort = TrackedAbort {
            handle: handle.abort_handle(),
            requested: Arc::clone(&requested),
        };
        let completion = async move {
            match handle.await {
                Ok(()) => WorkerExit::Joined,
                Err(error) if error.is_panic() => WorkerExit::Panicked,
                Err(_) => WorkerExit::Cancelled {
                    stop_requested: requested.load(Ordering::SeqCst),
                },
            }
        }
        .boxed()
        .shared();
        state.registered += 1;
        state.pending.push((kind, completion.clone()));
        (completion, abort)
    }

    pub async fn observe_until(&self, deadline: Instant) -> McpWorkerSnapshot {
        let (registered_at_start, mut counts, pending) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.compact(false);
            (
                state.registered,
                state.completed.clone(),
                state.pending.clone(),
            )
        };
        for (kind, completion) in pending {
            let exit = if let Some(exit) = completion.peek() {
                Some(*exit)
            } else if Instant::now() >= deadline {
                None
            } else {
                tokio::time::timeout_at(deadline, completion).await.ok()
            };
            if let Some(exit) = exit {
                counts[index(kind)].add(exit);
            } else {
                counts[index(kind)].pending += 1;
            }
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.compact(false);
        McpWorkerSnapshot {
            registered_at_start,
            registered_at_end: state.registered,
            counts,
            poisoned: self.state.is_poisoned(),
        }
    }
}

#[cfg(test)]
#[path = "worker_joins_tests.rs"]
mod tests;
