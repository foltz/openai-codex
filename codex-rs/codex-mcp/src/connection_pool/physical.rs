//! Physical custody allocated before a pooled connection factory can run.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use codex_rmcp_client::PhysicalRetirementReport;
use codex_rmcp_client::RmcpClient;
use codex_rmcp_client::RmcpClientRetirement;
use tokio::time::Instant;

use super::worker_joins::Completion;
use super::worker_joins::WeakCompletion;

/// Open-admission snapshot, never a whole-process termination claim.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PoolPhysicalSnapshot {
    pub registered_at_start: u64,
    pub registered_at_end: u64,
    pub retired_before: u64,
    pub connections: Vec<(u64, PhysicalRetirementReport)>,
    pub held_connections: Vec<u64>,
    pub foreign_connections: Vec<u64>,
    pub constructing_connections: Vec<u64>,
    pub poisoned: bool,
}

pub(super) struct PhysicalOwner {
    pub(super) lower: RmcpClientRetirement,
    id: u64,
    released: AtomicBool,
    // Keep unexpected lower custody visible, without cancelling a possibly
    // foreign/shared connection for which this pool has no release authority.
    binding: Mutex<BindingState>,
}

#[derive(Default)]
struct BindingState {
    settled: bool,
    startup_join: Option<WeakCompletion>,
    foreign: Vec<RmcpClientRetirement>,
}

impl PhysicalOwner {
    pub(super) fn observe_startup(&self, completion: &Completion) {
        self.binding
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .startup_join = completion.downgrade();
    }

    async fn wait_startup_until(&self, deadline: Instant) {
        if Instant::now() >= deadline {
            return;
        }
        let completion = self
            .binding
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .startup_join
            .as_ref()
            .and_then(WeakCompletion::upgrade);
        if let Some(completion) = completion {
            let _ = tokio::time::timeout_at(deadline, completion).await;
        }
    }

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
        let mut binding = self
            .binding
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !binding
            .foreign
            .iter()
            .any(|entry| entry.downgrade().same_registry(&lower.downgrade()))
        {
            binding.foreign.push(lower);
        }
        false
    }

    pub(super) fn settle_startup(&self, client: Option<&RmcpClient>) {
        if let Some(client) = client {
            self.accepts(client);
        }
        self.binding
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .settled = true;
    }

    fn binding_state(&self) -> (bool, bool) {
        let binding = self
            .binding
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (
            binding.settled,
            self.binding.is_poisoned() || !binding.foreign.is_empty(),
        )
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
            let (settled, foreign) = owner.binding_state();
            if owner.released() && settled && !foreign && owner.lower.retirement_proof().is_proven()
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
            binding: Mutex::new(BindingState::default()),
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
        let mut constructing_connections = Vec::new();
        let mut released = Vec::new();
        for owner in owners {
            let (settled, foreign) = owner.binding_state();
            if foreign {
                foreign_connections.push(owner.id);
            }
            if !settled {
                constructing_connections.push(owner.id);
            }
            if owner.released() {
                released.push(owner);
            } else {
                held_connections.push(owner.id);
            }
        }
        let connections = futures::future::join_all(released.iter().map(|owner| async {
            // Closing the lower gate fences late constructors. Observe
            // only this owner's original startup join, not other chats.
            let (mut report, ()) = tokio::join!(
                owner.lower.shutdown_until(deadline),
                owner.wait_startup_until(deadline),
            );
            // Startup can return its transport lease after the first
            // lower sample. Reconcile once, within the same budget.
            if !report.is_complete() && Instant::now() < deadline {
                report = owner.lower.shutdown_until(deadline).await;
            }
            (owner.id, report)
        }))
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
            constructing_connections,
            poisoned: self.state.is_poisoned(),
        }
    }
}
