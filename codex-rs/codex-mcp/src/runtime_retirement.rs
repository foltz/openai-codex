//! External connection-construction and per-chat release census. This is not
//! yet the runtime's generation/worker/retained-subscription termination proof.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::SlotReleaseSnapshot;
use crate::connection_pool::ConnectionSlot;
use crate::private_retirement::PrivateClientRetirement;
use crate::private_retirement::PrivateTerminationReport;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeConnectionResult {
    pub id: u64,
    pub private: PrivateTerminationReport,
    pub shared: Option<SlotReleaseSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeConnectionReport {
    pub registration_closed: bool,
    pub registered_at_start: u64,
    pub registered_at_end: u64,
    pub connections: Vec<RuntimeConnectionResult>,
    pub generations_admitted: u64,
    pub generations_settled: u64,
    pub generations_published: u64,
    pub constructing_generations: Vec<u64>,
    pub poisoned: bool,
}

impl RuntimeConnectionReport {
    /// Only registered connection obligations. This never substitutes for
    /// outstanding constructor/generation/worker or independent event custody.
    pub fn are_connections_released(&self) -> bool {
        self.registration_closed
            && !self.poisoned
            && self.registered_at_start == self.registered_at_end
            && self.connections.len() as u64 == self.registered_at_end
            && self.constructing_generations.is_empty()
            && self.generations_admitted == self.generations_settled
            && self.connections.iter().all(|record| {
                record.private.is_complete()
                    && record
                        .shared
                        .as_ref()
                        .is_none_or(SlotReleaseSnapshot::is_released)
            })
    }
}

struct ConnectionOwner {
    id: u64,
    private: PrivateClientRetirement,
    shared: Option<Arc<ConnectionSlot>>,
    cancellation: CancellationToken,
}

#[derive(Default)]
struct State {
    closed: bool,
    generations_admitted: u64,
    generations_settled: u64,
    generations_published: u64,
    constructing_generations: BTreeSet<u64>,
    connections: Vec<Arc<ConnectionOwner>>,
}

/// Retain before asynchronous connection construction or cleanup can be dropped.
/// Records hold no ManagedClient, ordinary LeaseOwners or strong ConnectionLease.
#[derive(Clone, Default)]
pub struct McpRuntimeRetirement {
    state: Arc<Mutex<State>>,
    generations_changed: Arc<Notify>,
}

/// The permit is weak: storing a constructor must not create a containing
/// census cycle. Settlement means the future returned/dropped, not a Tokio join.
pub(crate) struct RuntimeGenerationPermit {
    state: Weak<Mutex<State>>,
    changed: Weak<Notify>,
    id: u64,
    published: AtomicBool,
}

impl RuntimeGenerationPermit {
    fn with_gate<T>(&self, published: bool, operation: impl FnOnce() -> T) -> Option<T> {
        let state = self.state.upgrade()?;
        let mut state = state.lock().ok()?;
        if state.closed || (published && self.published.load(Ordering::Relaxed)) {
            return None;
        }
        let value = operation();
        if published {
            self.published.store(true, Ordering::Relaxed);
            state.generations_published += 1;
        }
        Some(value)
    }

    pub(crate) fn with_open_gate<T>(&self, operation: impl FnOnce() -> T) -> Option<T> {
        self.with_gate(false, operation)
    }

    pub(crate) fn publish_if_open<T>(&self, operation: impl FnOnce() -> T) -> Option<T> {
        self.with_gate(true, operation)
    }
}

impl Drop for RuntimeGenerationPermit {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            let mut state = state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.constructing_generations.remove(&self.id) {
                state.generations_settled += 1;
            }
        }
        if let Some(changed) = self.changed.upgrade() {
            changed.notify_waiters();
        }
    }
}

impl McpRuntimeRetirement {
    pub(crate) fn admit_generation(&self) -> Result<RuntimeGenerationPermit, ()> {
        let mut state = self.state.lock().map_err(|_| ())?;
        if state.closed {
            return Err(());
        }
        let id = state.generations_admitted;
        state.generations_admitted = id.checked_add(1).ok_or(())?;
        state.constructing_generations.insert(id);
        Ok(RuntimeGenerationPermit {
            state: Arc::downgrade(&self.state),
            changed: Arc::downgrade(&self.generations_changed),
            id,
            published: AtomicBool::new(false),
        })
    }

    async fn wait_for_generations_until(&self, deadline: Instant) {
        loop {
            let changed = self.generations_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .constructing_generations
                .is_empty()
            {
                return;
            }
            if Instant::now() >= deadline {
                return;
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return;
            }
        }
    }

    pub fn is_closed(&self) -> bool {
        self.state.lock().map_or(true, |state| state.closed)
    }

    pub(crate) fn register_connection(
        &self,
        private: PrivateClientRetirement,
        shared: Option<Arc<ConnectionSlot>>,
        cancellation: CancellationToken,
    ) -> Result<u64, ()> {
        let mut state = self.state.lock().map_err(|_| ())?;
        if state.closed {
            return Err(());
        }
        let id = state.connections.len() as u64;
        state.connections.push(Arc::new(ConnectionOwner {
            id,
            private,
            shared,
            cancellation,
        }));
        Ok(id)
    }

    pub fn close_registration(&self) {
        let records = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.closed = true;
            // Globally fence every registered gate before the first cancellation
            // can wake work on another executor thread.
            for record in &state.connections {
                record.private.fence_construction();
                if let Some(slot) = &record.shared {
                    slot.close_acquisition();
                }
            }
            state.connections.clone()
        };
        for record in records {
            record.private.close_registration();
            record.cancellation.cancel();
            if let Some(slot) = &record.shared {
                slot.release_closed_acquisitions();
            }
        }
    }

    pub async fn shutdown_until(&self, deadline: Instant) -> RuntimeConnectionReport {
        self.close_registration();
        let records = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .connections
            .clone();
        let drain_connections =
            futures::future::join_all(records.iter().map(|record| async move {
                let (private, shared) =
                    tokio::join!(record.private.shutdown_until(deadline), async {
                        match &record.shared {
                            Some(slot) => Some(slot.retire_release_until(deadline).await),
                            None => None,
                        }
                    });
                RuntimeConnectionResult {
                    id: record.id,
                    private,
                    shared,
                }
            }));
        let (connections, ()) =
            tokio::join!(drain_connections, self.wait_for_generations_until(deadline));
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        RuntimeConnectionReport {
            registration_closed: state.closed,
            registered_at_start: records.len() as u64,
            registered_at_end: state.connections.len() as u64,
            connections,
            generations_admitted: state.generations_admitted,
            generations_settled: state.generations_settled,
            generations_published: state.generations_published,
            constructing_generations: state.constructing_generations.iter().copied().collect(),
            poisoned: self.state.is_poisoned(),
        }
    }
}

#[cfg(test)]
#[path = "runtime_retirement_tests.rs"]
mod tests;
