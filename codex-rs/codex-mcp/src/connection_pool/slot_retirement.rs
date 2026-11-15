//! Exact per-slot release receipts, without another ordinary lease owner.

use super::ConnectionLease;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::Weak;

use codex_rmcp_client::PhysicalRetirementReport;
use tokio::time::Instant;

use super::ConnectionSlot;
use super::PoolWorkerSnapshot;
use super::SlotState;
use super::physical::PhysicalOwner;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SlotReleaseOutcome {
    AwaitingOrdinaryRelease,
    ReleaseWorkerRunning,
    ReleasedWithoutServerLease,
    ServerLeaseAcknowledged,
    ServerReleaseFailed(String),
    RequiresPhysicalProof,
    ForeignRegistry,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SlotReleaseSnapshot {
    pub registered_at_start: u64,
    pub registered_at_end: u64,
    pub acquisition_closed: bool,
    pub outcomes: Vec<(u64, SlotReleaseOutcome)>,
    pub physical: Vec<(u64, PhysicalRetirementReport)>,
    pub workers: PoolWorkerSnapshot,
    pub poisoned: bool,
}

impl SlotReleaseSnapshot {
    /// This slot's logical obligations only. A successful non-final release
    /// explicitly does not assert that another chat's physical transport ended.
    pub fn is_released(&self) -> bool {
        self.acquisition_closed
            && !self.poisoned
            && !self.workers.poisoned
            && self.registered_at_start == self.registered_at_end
            && self.workers.registered_at_start == self.workers.registered_at_end
            && self
                .workers
                .counts
                .iter()
                .all(|c| c.pending + c.panicked + c.cancelled == 0)
            && self.outcomes.iter().all(|(id, outcome)| match outcome {
                SlotReleaseOutcome::ReleasedWithoutServerLease
                | SlotReleaseOutcome::ServerLeaseAcknowledged => true,
                SlotReleaseOutcome::RequiresPhysicalProof
                | SlotReleaseOutcome::ServerReleaseFailed(_) => self
                    .physical
                    .iter()
                    .any(|(observed, report)| observed == id && report.is_complete()),
                _ => false,
            })
    }
}

pub(super) struct ReleaseReceipt {
    lease: OnceLock<Weak<ConnectionLease>>,
    id: u64,
    physical: Arc<PhysicalOwner>,
    outcome: Mutex<SlotReleaseOutcome>,
}

impl ReleaseReceipt {
    pub(super) fn bind(&self, lease: &Arc<ConnectionLease>) {
        assert!(
            self.lease.set(Arc::downgrade(lease)).is_ok(),
            "release authority is bound once"
        );
    }
    pub(super) fn record(&self, outcome: SlotReleaseOutcome) {
        *self
            .outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = outcome;
    }
    fn snapshot(&self) -> SlotReleaseOutcome {
        self.outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

#[derive(Default)]
pub(super) struct ReleaseReceipts {
    records: Mutex<Vec<Arc<ReleaseReceipt>>>,
}

impl ReleaseReceipts {
    pub(super) fn reserve(&self, physical: Arc<PhysicalOwner>) -> Arc<ReleaseReceipt> {
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let receipt = Arc::new(ReleaseReceipt {
            lease: OnceLock::new(),
            id: records.len() as u64,
            physical,
            outcome: Mutex::new(SlotReleaseOutcome::AwaitingOrdinaryRelease),
        });
        records.push(Arc::clone(&receipt));
        receipt
    }
}

impl ConnectionSlot {
    /// Call only after all runtime acquisition/lower gates are fenced. These
    /// weak upgrades name this slot's exact authorities, not other chat leases.
    pub(crate) fn release_closed_acquisitions(&self) {
        self.close_acquisition();
        let records = self
            .release_receipts
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        for record in records {
            if let Some(lease) = record.lease.get().and_then(Weak::upgrade) {
                let _ = lease.release();
            }
        }
    }

    /// Close only this slot's acquisition gate. Unlike supersession retire(),
    /// this does not evict the still-shared entry from the process reuse pool.
    pub(crate) fn close_acquisition(&self) {
        *self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = SlotState::Retired;
    }

    /// Retain original release replies/joins; only final released physical
    /// owners may be driven. All observations use this one caller deadline.
    pub(crate) async fn retire_release_until(&self, deadline: Instant) -> SlotReleaseSnapshot {
        let acquisition_closed = matches!(
            *self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            SlotState::Retired
        );
        let records = self
            .release_receipts
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let workers = self.release_workers.observe_until(deadline).await;
        let mut physical = Vec::new();
        for record in &records {
            if matches!(
                record.snapshot(),
                SlotReleaseOutcome::RequiresPhysicalProof
                    | SlotReleaseOutcome::ServerReleaseFailed(_)
            ) && record.physical.released()
            {
                physical.push((
                    record.id,
                    record.physical.lower.shutdown_until(deadline).await,
                ));
            }
        }
        let outcomes = records
            .iter()
            .map(|record| (record.id, record.snapshot()))
            .collect();
        let poisoned = self.state.is_poisoned()
            || self.release_receipts.records.is_poisoned()
            || records.iter().any(|record| record.outcome.is_poisoned());
        SlotReleaseSnapshot {
            registered_at_start: records.len() as u64,
            registered_at_end: self
                .release_receipts
                .records
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len() as u64,
            acquisition_closed,
            outcomes,
            physical,
            workers,
            poisoned,
        }
    }
}
