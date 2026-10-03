//! Supersession marks ordinary lifetime without discarding retirement custody.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use codex_rmcp_client::DEFAULT_RETIREMENT_TIMEOUT;
use codex_rmcp_client::RmcpClientRetirement;
use tokio::runtime::Handle;
use tokio::time::Instant;
use tracing::warn;

use super::McpConnectionSet;
use super::McpServerConnection;
use crate::runtime_retirement::ConnectionRetirementOwner;
use crate::runtime_retirement::RuntimeRetirementRegistry;
use crate::runtime_retirement::RuntimeTaskOutcome;
use crate::runtime_retirement::RuntimeTaskTicket;

pub(super) struct ConnectionRetirement {
    superseded: AtomicBool,
    connection_id: usize,
    // Acquired at construction, not by upgrading a weak ticket during Drop.
    // The registry owns lower records, never this connection.
    registry: RuntimeRetirementRegistry,
    lower: RmcpClientRetirement,
    ticket: RuntimeTaskTicket,
}

impl ConnectionRetirement {
    pub(super) fn new(
        registry: RuntimeRetirementRegistry,
        owner: &ConnectionRetirementOwner,
    ) -> Self {
        Self {
            superseded: AtomicBool::new(false),
            connection_id: owner.id(),
            registry,
            lower: owner.lower(),
            ticket: owner.task_ticket(),
        }
    }

    pub(super) fn retire_if_superseded(
        &self,
    ) -> Option<tokio::task::JoinHandle<RuntimeTaskOutcome>> {
        if !self.superseded.load(Ordering::Acquire) {
            return None;
        }
        self.lower.close_registration();
        let connection_id = self.connection_id;
        let Ok(handle) = Handle::try_current() else {
            warn!(
                connection_id,
                "Superseded MCP cleanup is undriven: no Tokio runtime"
            );
            return None;
        };
        let lower = self.lower.clone();
        let Ok(completion) = self.ticket.register(move || async move {
            let report = lower
                .shutdown_until(Instant::now() + DEFAULT_RETIREMENT_TIMEOUT)
                .await;
            if !report.is_complete() {
                warn!(
                    connection_id,
                    ?report,
                    "Superseded MCP physical retirement is incomplete"
                );
            }
            // This task acknowledges execution only. Incomplete physical
            // outcomes remain in the lower registry for explicit retry.
            RuntimeTaskOutcome::Complete
        }) else {
            // Explicit retirement has closed admission and owns the drain.
            // A poisoned registry is likewise never a completion claim.
            return None;
        };
        let registry = self.registry.clone();
        // The keeper is OUTSIDE the registry-retained shared future, so there
        // is no registry -> task -> registry cycle. It also spans queued polls.
        Some(handle.spawn(async move {
            let outcome = completion.await;
            drop(registry);
            outcome
        }))
    }
}

impl McpConnectionSet {
    pub(crate) fn mark_superseded_by(&self, incoming: &Self) {
        let carried: HashSet<_> = incoming
            .servers
            .values()
            .map(|view| Arc::as_ptr(&view.connection))
            .collect();
        for view in self.servers.values() {
            if !carried.contains(&Arc::as_ptr(&view.connection)) {
                view.connection
                    .retirement
                    .superseded
                    .store(true, Ordering::Release);
            }
        }
    }

    pub(crate) fn connection_by_name(&self, server: &str) -> Option<Arc<McpServerConnection>> {
        self.servers
            .get(server)
            .map(|view| Arc::clone(&view.connection))
    }
}

#[cfg(test)]
#[path = "retirement_tests.rs"]
mod tests;
