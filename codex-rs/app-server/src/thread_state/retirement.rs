//! Retention authority's atomic handoff to retirement. This table is distinct
//! from removable observation state; a claim alone is not cleanup proof.

use super::ThreadId;
use super::ThreadState;
use super::ThreadStateManager;
use super::ThreadStateManagerInner;
use futures::StreamExt;
use std::sync::Arc;
use std::sync::Weak;
use tokio::sync::Mutex;
use tokio::sync::watch;
use tokio::time::Instant;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RetentionSnapshot {
    pub(crate) generation: Uuid,
    pub(crate) revision: Uuid,
    pub(crate) granted: bool,
    pub(crate) unretained_since: Instant,
    pub(crate) retiring: bool,
}

pub(super) struct RetentionLifecycle {
    identity: Weak<Mutex<ThreadState>>,
    snapshot: watch::Sender<RetentionSnapshot>,
    ticket: Option<codex_core::ThreadRetirement>,
    report: Option<codex_core::ThreadRetirementReport>,
}

impl RetentionLifecycle {
    pub(super) fn new(identity: Weak<Mutex<ThreadState>>) -> Self {
        Self {
            identity,
            ticket: None,
            report: None,
            snapshot: watch::channel(RetentionSnapshot {
                generation: Uuid::now_v7(),
                revision: Uuid::now_v7(),
                granted: false,
                unretained_since: Instant::now(),
                retiring: false,
            })
            .0,
        }
    }
}

impl RetentionLifecycle {
    pub(super) fn is_retiring(&self) -> bool {
        self.snapshot.borrow().retiring
    }

    pub(super) fn has_complete_report(&self) -> bool {
        self.report
            .as_ref()
            .is_some_and(codex_core::ThreadRetirementReport::is_complete)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RetentionRetirementClaim {
    pub(crate) thread_id: ThreadId,
    pub(crate) generation: Uuid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RetentionRetirementRefusal {
    UnknownThread,
    Changed,
    Retained,
    AlreadyRetiring,
    Busy,
    ActiveWork,
    AuthorityUnavailable,
    DeadlineExpired,
}

impl ThreadStateManagerInner {
    pub(super) fn publish_retention(&mut self, thread_id: ThreadId) {
        let granted = self
            .retention_grants_by_thread
            .get(&thread_id)
            .is_some_and(|grants| !grants.is_empty());
        let Some(record) = self.lifecycle.get_mut(&thread_id) else {
            return;
        };
        record.snapshot.send_if_modified(|snapshot| {
            if snapshot.granted == granted {
                return false;
            }
            snapshot.granted = granted;
            snapshot.revision = Uuid::now_v7();
            if !granted {
                snapshot.unretained_since = Instant::now();
            }
            true
        });
    }
}

impl ThreadStateManager {
    /// Drive every committed ticket, including one whose original caller was
    /// lost before its first poll. The table retains the originals throughout;
    /// cancellation of this observer cannot abandon ownership or refresh bounds.
    pub(crate) async fn drain_retirement_tickets(
        &self,
    ) -> Vec<(RetentionRetirementClaim, codex_core::ThreadRetirementReport)> {
        let tickets = {
            let mut state = self.state.lock().await;
            // Freeze the claim population in the same mutex as admission,
            // before taking the drain snapshot.
            state.retirement_claims_closed = true;
            state
                .lifecycle
                .iter()
                .filter_map(|(thread_id, record)| {
                    record.ticket.clone().map(|ticket| {
                        (
                            RetentionRetirementClaim {
                                thread_id: *thread_id,
                                generation: record.snapshot.borrow().generation,
                            },
                            ticket,
                        )
                    })
                })
                .collect::<Vec<_>>()
        };
        let mut pending = tickets
            .into_iter()
            .map(|(claim, ticket)| async move { (claim, ticket.wait().await) })
            .collect::<futures::stream::FuturesUnordered<_>>();
        let mut reports = Vec::new();
        while let Some((claim, report)) = pending.next().await {
            let mut state = self.state.lock().await;
            if let Some(record) = state.lifecycle.get_mut(&claim.thread_id)
                && record.snapshot.borrow().generation == claim.generation
                && record.is_retiring()
            {
                record.report = Some(report);
            }
            reports.push((claim, report));
        }
        reports
    }

    pub(crate) async fn record_retirement_report(
        &self,
        claim: RetentionRetirementClaim,
        report: codex_core::ThreadRetirementReport,
    ) -> bool {
        let mut state = self.state.lock().await;
        let Some(record) = state.lifecycle.get_mut(&claim.thread_id) else {
            return false;
        };
        if record.snapshot.borrow().generation != claim.generation || !record.is_retiring() {
            return false;
        }
        record.report = Some(report);
        true
    }

    /// Commit exact-runtime retirement while holding both authority and
    /// observation identity stable. All inner acquisitions are non-waiting;
    /// no manager guard crosses an await after the initial acquisition.
    pub(crate) async fn claim_thread_retirement(
        &self,
        thread_id: ThreadId,
        expected: RetentionSnapshot,
        thread: &Arc<codex_core::CodexThread>,
        listener_generation: u64,
        deadline: Instant,
    ) -> Result<(RetentionRetirementClaim, codex_core::ThreadRetirement), RetentionRetirementRefusal>
    {
        let mut state = self.state.lock().await;
        if state.retirement_claims_closed {
            return Err(RetentionRetirementRefusal::AuthorityUnavailable);
        }
        let entry = state
            .threads
            .get(&thread_id)
            .map(|entry| Arc::clone(&entry.state))
            .ok_or(RetentionRetirementRefusal::UnknownThread)?;
        let listener = entry
            .try_lock()
            .map_err(|_| RetentionRetirementRefusal::Busy)?;
        if listener.listener_generation != listener_generation || !listener.listener_matches(thread)
        {
            return Err(RetentionRetirementRefusal::Changed);
        }
        state.publish_retention(thread_id);
        let record = state
            .lifecycle
            .get_mut(&thread_id)
            .ok_or(RetentionRetirementRefusal::AuthorityUnavailable)?;
        let current = *record.snapshot.borrow();
        if !record.identity.ptr_eq(&Arc::downgrade(&entry)) || current != expected {
            return Err(RetentionRetirementRefusal::Changed);
        }
        if current.retiring {
            return Err(RetentionRetirementRefusal::AlreadyRetiring);
        }
        if current.granted {
            return Err(RetentionRetirementRefusal::Retained);
        }
        let ticket = thread
            .try_begin_idle_retirement(deadline)
            .map_err(|error| match error {
                codex_core::ThreadRetirementError::TaskAdmissionBusy => {
                    RetentionRetirementRefusal::Busy
                }
                codex_core::ThreadRetirementError::ActiveWork => {
                    RetentionRetirementRefusal::ActiveWork
                }
                codex_core::ThreadRetirementError::DeadlineExpired => {
                    RetentionRetirementRefusal::DeadlineExpired
                }
                codex_core::ThreadRetirementError::AuthorityUnavailable
                | codex_core::ThreadRetirementError::LegacyCleanupStarted => {
                    RetentionRetirementRefusal::AuthorityUnavailable
                }
            })?;
        // No fallible step remains after the core admission fence closes.
        // Keep the original ticket before observation state can be removed.
        record.ticket = Some(ticket.clone());
        record
            .snapshot
            .send_modify(|snapshot| snapshot.retiring = true);
        Ok((
            RetentionRetirementClaim {
                thread_id,
                generation: current.generation,
            },
            ticket,
        ))
    }

    pub(crate) async fn subscribe_to_retention(
        &self,
        thread_id: ThreadId,
    ) -> Option<watch::Receiver<RetentionSnapshot>> {
        let mut state = self.state.lock().await;
        if !state.threads.contains_key(&thread_id) {
            return None;
        }
        state.publish_retention(thread_id);
        state
            .lifecycle
            .get(&thread_id)
            .map(|record| record.snapshot.subscribe())
    }

    /// Claims only the retention-authority limb. The caller must separately
    /// prove idle/exact-thread identity and retain its cleanup transaction.
    /// Acquisition and this CAS share the manager mutex; neither can win after
    /// the other. A complete acquire/release interval invalidates the snapshot.
    #[cfg(test)]
    async fn claim_retention_retirement(
        &self,
        thread_id: ThreadId,
        expected: RetentionSnapshot,
    ) -> Result<RetentionRetirementClaim, RetentionRetirementRefusal> {
        let mut state = self.state.lock().await;
        let identity = state
            .threads
            .get(&thread_id)
            .map(|entry| Arc::downgrade(&entry.state))
            .ok_or(RetentionRetirementRefusal::UnknownThread)?;
        state.publish_retention(thread_id);
        let record = state
            .lifecycle
            .get_mut(&thread_id)
            .ok_or(RetentionRetirementRefusal::UnknownThread)?;
        let current = *record.snapshot.borrow();
        if !record.identity.ptr_eq(&identity) {
            return Err(RetentionRetirementRefusal::Changed);
        }
        if current.retiring {
            return Err(RetentionRetirementRefusal::AlreadyRetiring);
        }
        if current.granted {
            return Err(RetentionRetirementRefusal::Retained);
        }
        if current != expected {
            return Err(RetentionRetirementRefusal::Changed);
        }
        record
            .snapshot
            .send_modify(|snapshot| snapshot.retiring = true);
        Ok(RetentionRetirementClaim {
            thread_id,
            generation: current.generation,
        })
    }
}

#[cfg(test)]
#[path = "retirement_tests.rs"]
mod tests;
