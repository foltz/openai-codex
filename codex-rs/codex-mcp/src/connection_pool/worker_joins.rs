//! Original pool-worker joins, separate from chat unload and physical receipts.

use std::future::Future;
use std::sync::Mutex;

use futures::FutureExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use futures::future::WeakShared;
use tokio::runtime::Handle;
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PoolWorkerKind {
    Startup,
    Release,
}

#[derive(Clone, Copy)]
pub(super) enum WorkerExit {
    Joined,
    Panicked,
    Cancelled,
}

pub(super) type Completion = Shared<BoxFuture<'static, WorkerExit>>;
pub(super) type WeakCompletion = WeakShared<BoxFuture<'static, WorkerExit>>;

/// Worker exit counts are not protocol acknowledgements or physical retirement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PoolWorkerCounts {
    pub kind: PoolWorkerKind,
    pub joined: u64,
    pub panicked: u64,
    pub cancelled: u64,
    pub pending: u64,
}

impl PoolWorkerCounts {
    fn empty(kind: PoolWorkerKind) -> Self {
        Self {
            kind,
            joined: 0,
            panicked: 0,
            cancelled: 0,
            pending: 0,
        }
    }

    fn add(&mut self, exit: WorkerExit) {
        match exit {
            WorkerExit::Joined => self.joined += 1,
            WorkerExit::Panicked => self.panicked += 1,
            WorkerExit::Cancelled => self.cancelled += 1,
        }
    }
}

/// Snapshot only: admission stays open and another chat may create more work.
/// A positive join is never substituted for a lower physical-retirement report.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PoolWorkerSnapshot {
    pub registered_at_start: u64,
    pub registered_at_end: u64,
    pub counts: [PoolWorkerCounts; 2],
    pub poisoned: bool,
}

struct State {
    registered: u64,
    completed: [PoolWorkerCounts; 2],
    pending: Vec<(PoolWorkerKind, Completion)>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            registered: 0,
            completed: [
                PoolWorkerCounts::empty(PoolWorkerKind::Startup),
                PoolWorkerCounts::empty(PoolWorkerKind::Release),
            ],
            pending: Vec::new(),
        }
    }
}

fn index(kind: PoolWorkerKind) -> usize {
    match kind {
        PoolWorkerKind::Startup => 0,
        PoolWorkerKind::Release => 1,
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

#[derive(Default)]
pub(super) struct PoolWorkerJoins {
    state: Mutex<State>,
}

impl PoolWorkerJoins {
    pub(super) fn spawn(
        &self,
        runtime: &Handle,
        kind: PoolWorkerKind,
        body: impl Future<Output = ()> + Send + 'static,
    ) -> Completion {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.compact(true);
        // Birth and registration share the ledger lock, with no intervening await.
        let handle = runtime.spawn(body);
        let completion = async move {
            match handle.await {
                Ok(()) => WorkerExit::Joined,
                Err(error) if error.is_panic() => WorkerExit::Panicked,
                Err(_) => WorkerExit::Cancelled,
            }
        }
        .boxed()
        .shared();
        state.registered += 1;
        state.pending.push((kind, completion.clone()));
        completion
    }

    pub(super) fn retain(&self, kind: PoolWorkerKind, completion: Completion) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.compact(true);
        state.registered += 1;
        state.pending.push((kind, completion));
    }

    pub(super) async fn observe_until(&self, deadline: Instant) -> PoolWorkerSnapshot {
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
        PoolWorkerSnapshot {
            registered_at_start,
            registered_at_end: state.registered,
            counts,
            poisoned: self.state.is_poisoned(),
        }
    }
}
