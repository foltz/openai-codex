//! External connection-construction and per-chat release census. This is not
//! yet the runtime's generation/worker/retained-subscription termination proof.

use std::sync::Arc;
use std::sync::Mutex;

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
    connections: Vec<Arc<ConnectionOwner>>,
}

/// Retain before asynchronous connection construction or cleanup can be dropped.
/// Records hold no ManagedClient, ordinary LeaseOwners or strong ConnectionLease.
#[derive(Clone, Default)]
pub struct McpRuntimeRetirement {
    state: Arc<Mutex<State>>,
}

impl McpRuntimeRetirement {
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
        let connections = futures::future::join_all(records.iter().map(|record| async move {
            let (private, shared) = tokio::join!(record.private.shutdown_until(deadline), async {
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
        }))
        .await;
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        RuntimeConnectionReport {
            registration_closed: state.closed,
            registered_at_start: records.len() as u64,
            registered_at_end: state.connections.len() as u64,
            connections,
            poisoned: self.state.is_poisoned(),
        }
    }
}

#[cfg(test)]
#[path = "runtime_retirement_tests.rs"]
mod tests;
