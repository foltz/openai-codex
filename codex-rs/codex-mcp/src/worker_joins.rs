//! Original runtime worker joins. An open-admission snapshot is not a closed
//! runtime census, protocol acknowledgement, or physical transport proof.

use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
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
    NotAdmitted,
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
            WorkerExit::NotAdmitted => unreachable!("refusal is never an original worker join"),
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
    pub startup_admission_closed: bool,
    pub all_admission_closed: bool,
    pub refused_at_start: u64,
    pub refused_at_end: u64,
    pub registered_at_start: u64,
    pub registered_at_end: u64,
    pub counts: Vec<McpWorkerCounts>,
    pub poisoned: bool,
}

struct State {
    startup_closed: bool,
    all_closed: bool,
    refused: u64,
    registered: u64,
    completed: Vec<McpWorkerCounts>,
    pending: Vec<(McpWorkerKind, Completion)>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            startup_closed: false,
            all_closed: false,
            refused: 0,
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

impl McpWorkerKind {
    fn is_cleanup(self) -> bool {
        matches!(
            self,
            Self::DiscoveryCleanup | Self::ConnectionShutdown | Self::RuntimeShutdown
        )
    }
}

impl State {
    fn admit(&mut self, kind: McpWorkerKind, poisoned: bool) -> bool {
        if self.all_closed || (!kind.is_cleanup() && (self.startup_closed || poisoned)) {
            self.refused += 1;
            false
        } else {
            true
        }
    }

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
    pub fn close_startup_admission(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .startup_closed = true;
    }

    pub(crate) fn same_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }

    /// Like the previous JoinSet batch, failure or caller cancellation aborts
    /// siblings. Every child's original join stays observable after that abort.
    /// Payloads travel through a separate channel: the ledger must not become
    /// an extra ordinary owner of completed managed clients.
    pub(crate) async fn join_eager_batch<T: Send + 'static>(
        &self,
        bodies: impl IntoIterator<Item = impl Future<Output = T> + Send + 'static>,
    ) -> Result<Option<Vec<T>>, String> {
        use futures::StreamExt;
        let mut aborts = EagerAbortGroup(Vec::new());
        let mut pending = futures::stream::FuturesUnordered::new();
        for body in bodies {
            let (send, receive) = tokio::sync::oneshot::channel();
            let Ok((completion, abort)) = self.try_spawn_tracked(
                &Handle::current(),
                McpWorkerKind::EagerStartup,
                async move {
                    let _ = send.send(body.await);
                },
            ) else {
                // Refusal drops all remaining bodies and requests abort on
                // already-born siblings; every original join remains retained.
                return Ok(None);
            };
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
        Ok(Some(outputs))
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
        if !state.admit(kind, self.state.is_poisoned()) {
            return futures::future::ready(Err("MCP worker startup admission closed".to_owned()))
                .boxed()
                .shared();
        }
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
        self.try_spawn(runtime, kind, body).unwrap_or_else(|()| {
            futures::future::ready(WorkerExit::NotAdmitted)
                .boxed()
                .shared()
        })
    }

    pub(crate) fn try_spawn(
        &self,
        runtime: &Handle,
        kind: McpWorkerKind,
        body: impl Future<Output = ()> + Send + 'static,
    ) -> Result<Completion, ()> {
        self.try_spawn_tracked(runtime, kind, body)
            .map(|(completion, _)| completion)
    }

    pub(crate) fn spawn_abort_on_drop(
        &self,
        runtime: &Handle,
        kind: McpWorkerKind,
        body: impl Future<Output = ()> + Send + 'static,
    ) -> Result<McpAbortOnDrop, ()> {
        self.try_spawn_tracked(runtime, kind, body)
            .map(|(_, abort)| McpAbortOnDrop { abort })
    }

    fn try_spawn_tracked(
        &self,
        runtime: &Handle,
        kind: McpWorkerKind,
        body: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(Completion, TrackedAbort), ()> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.admit(kind, self.state.is_poisoned()) {
            return Err(());
        }
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
        Ok((completion, abort))
    }

    /// Call only after the containing constructor/connection population has
    /// settled positively. Newly born cleanup is observed under the same bound.
    pub(crate) async fn drain_and_seal_until(&self, deadline: Instant) -> McpWorkerSnapshot {
        loop {
            let observed = self.observe_until(deadline).await;
            {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.compact(false);
                if state.startup_closed && state.pending.is_empty() && !self.state.is_poisoned() {
                    state.all_closed = true;
                    return McpWorkerSnapshot {
                        startup_admission_closed: true,
                        all_admission_closed: true,
                        refused_at_start: state.refused,
                        refused_at_end: state.refused,
                        registered_at_start: state.registered,
                        registered_at_end: state.registered,
                        counts: state.completed.clone(),
                        poisoned: false,
                    };
                }
            }
            if Instant::now() >= deadline || !observed.startup_admission_closed || observed.poisoned
            {
                return observed;
            }
            // A cleanup worker may have been born while its parent was being
            // joined. It must get its own observed join, never a quiet-period
            // assumption or an extended deadline.
        }
    }

    pub async fn observe_until(&self, deadline: Instant) -> McpWorkerSnapshot {
        let (registered_at_start, refused_at_start, mut counts, pending) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.compact(false);
            (
                state.registered,
                state.refused,
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
            startup_admission_closed: state.startup_closed,
            all_admission_closed: state.all_closed,
            refused_at_start,
            refused_at_end: state.refused,
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
