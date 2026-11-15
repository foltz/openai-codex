//! Physical custody allocated before a pooled connection factory can run.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use codex_rmcp_client::PhysicalRetirementReport;
use codex_rmcp_client::RmcpClient;
use codex_rmcp_client::RmcpClientRetirement;
use tokio::time::Instant;

/// Open-admission snapshot, never a whole-process termination claim.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PoolPhysicalSnapshot {
    pub registered_at_start: u64,
    pub registered_at_end: u64,
    pub retired_before: u64,
    pub connections: Vec<(u64, PhysicalRetirementReport)>,
    pub held_connections: Vec<u64>,
    pub foreign_connections: Vec<u64>,
    pub poisoned: bool,
}

pub(super) struct PhysicalOwner {
    pub(super) lower: RmcpClientRetirement,
    id: u64,
    released: AtomicBool,
    // Keep unexpected lower custody visible, without cancelling a possibly
    // foreign/shared connection for which this pool has no release authority.
    foreign: Mutex<Vec<RmcpClientRetirement>>,
}

impl PhysicalOwner {
    pub(super) fn mark_released(&self) {
        self.lower.close_registration();
        self.released.store(true, Ordering::Release);
    }

    pub(super) fn released(&self) -> bool {
        self.released.load(Ordering::Acquire)
    }

    pub(super) fn accepts(&self, client: &RmcpClient) -> bool {
        let lower = client.retirement();
        if self.lower.downgrade().same_registry(&lower.downgrade()) {
            return true;
        }
        let mut foreign = self
            .foreign
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !foreign
            .iter()
            .any(|entry| entry.downgrade().same_registry(&lower.downgrade()))
        {
            foreign.push(lower);
        }
        false
    }

    fn has_foreign(&self) -> bool {
        self.foreign.is_poisoned()
            || !self
                .foreign
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty()
    }
}

#[derive(Default)]
struct State {
    registered: u64,
    retired: u64,
    owners: Vec<Arc<PhysicalOwner>>,
}

impl State {
    fn compact(&mut self) {
        let retired = &mut self.retired;
        self.owners.retain(|owner| {
            if owner.released()
                && !owner.has_foreign()
                && owner.lower.retirement_proof().is_proven()
            {
                *retired += 1;
                false
            } else {
                true
            }
        });
    }
}

#[derive(Default)]
pub(super) struct PoolPhysicalOwners {
    state: Mutex<State>,
}

impl PoolPhysicalOwners {
    pub(super) fn reserve(&self) -> Arc<PhysicalOwner> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.compact();
        let owner = Arc::new(PhysicalOwner {
            id: state.registered,
            lower: RmcpClientRetirement::default(),
            released: AtomicBool::new(false),
            foreign: Mutex::new(Vec::new()),
        });
        state.registered += 1;
        state.owners.push(Arc::clone(&owner));
        owner
    }

    pub(super) async fn retire_released_until(&self, deadline: Instant) -> PoolPhysicalSnapshot {
        let (registered_at_start, retired_before, owners) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.compact();
            (state.registered, state.retired, state.owners.clone())
        };
        let mut held_connections = Vec::new();
        let mut foreign_connections = Vec::new();
        let mut released = Vec::new();
        for owner in owners {
            if owner.has_foreign() {
                foreign_connections.push(owner.id);
            }
            if owner.released() {
                released.push(owner);
            } else {
                held_connections.push(owner.id);
            }
        }
        let connections = futures::future::join_all(
            released
                .iter()
                .map(|owner| async { (owner.id, owner.lower.shutdown_until(deadline).await) }),
        )
        .await;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.compact();
        PoolPhysicalSnapshot {
            registered_at_start,
            registered_at_end: state.registered,
            retired_before,
            connections,
            held_connections,
            foreign_connections,
            poisoned: self.state.is_poisoned(),
        }
    }
}
