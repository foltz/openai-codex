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
    runtime: Weak<codex_core::CodexThread>,
    reconciled: bool,
    listener_completion: Option<futures::future::Shared<futures::future::BoxFuture<'static, bool>>>,
}

impl RetentionLifecycle {
    pub(super) fn new(identity: Weak<Mutex<ThreadState>>) -> Self {
        Self {
            identity,
            ticket: None,
            report: None,
            runtime: Weak::new(),
            reconciled: false,
            listener_completion: None,
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

pub(crate) struct RetirementCandidate {
    pub(crate) ticket: codex_core::ThreadRetirement,
    pub(crate) claim: RetentionRetirementClaim,
    pub(crate) runtime: Arc<codex_core::CodexThread>,
    pub(crate) listener: Option<Arc<Mutex<ThreadState>>>,
    pub(crate) completion:
        Option<futures::future::Shared<futures::future::BoxFuture<'static, bool>>>,
}

impl ThreadStateManager {
    pub(crate) async fn retirement_candidate(
        &self,
        thread_id: ThreadId,
    ) -> Result<Option<RetirementCandidate>, &'static str> {
        let state = self.state.lock().await;
        let record = match state.lifecycle.get(&thread_id) {
            Some(record) if record.is_retiring() => Some(record),
            Some(_) => None,
            None => state.threads.get(&thread_id).and_then(|entry| {
                state
                    .retired_lifecycle
                    .iter()
                    .find_map(|((id, _), record)| {
                        (*id == thread_id && record.identity.ptr_eq(&Arc::downgrade(&entry.state)))
                            .then_some(record)
                    })
            }),
        };
        let Some(record) = record else {
            return Ok(None);
        };
        let runtime = record
            .runtime
            .upgrade()
            .ok_or("retired runtime custody unavailable")?;
        let ticket = record
            .ticket
            .clone()
            .ok_or("original retirement ticket custody unavailable")?;
        Ok(Some(RetirementCandidate {
            ticket,
            claim: RetentionRetirementClaim {
                thread_id,
                generation: record.snapshot.borrow().generation,
            },
            runtime,
            listener: record.identity.upgrade(),
            completion: record.listener_completion.clone(),
        }))
    }

    /// Move history/custody before observation removal; a fresh current entry gets fresh grants.
    pub(crate) async fn archive_retirement(
        &self,
        claim: RetentionRetirementClaim,
        runtime: &Arc<codex_core::CodexThread>,
    ) -> bool {
        let mut state = self.state.lock().await;
        if state.retirement_claims_closed {
            return false;
        }
        let Some(record) = state.lifecycle.get(&claim.thread_id) else {
            let Some(record) = state
                .retired_lifecycle
                .get_mut(&(claim.thread_id, claim.generation))
            else {
                return false;
            };
            if !record.runtime.ptr_eq(&Arc::downgrade(runtime))
                || !runtime.retirement_is_quiescent()
            {
                return false;
            }
            record.reconciled |= runtime.retirement_reconciled();
            return true;
        };
        if !record.is_retiring()
            || record.snapshot.borrow().generation != claim.generation
            || !record.runtime.ptr_eq(&Arc::downgrade(runtime))
            || !runtime.retirement_is_quiescent()
        {
            return false;
        }
        let Some(mut record) = state.lifecycle.remove(&claim.thread_id) else {
            return false;
        };
        record.reconciled = runtime.retirement_reconciled();
        state
            .retired_lifecycle
            .insert((claim.thread_id, claim.generation), record);
        true
    }

    pub(crate) async fn reconciled_retirements(
        &self,
    ) -> std::collections::HashSet<(ThreadId, Uuid)> {
        let state = self.state.lock().await;
        state
            .retired_lifecycle
            .iter()
            .filter_map(|(claim, record)| record.reconciled.then_some(*claim))
            .collect()
    }

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
                .map(|(id, record)| (*id, record))
                .chain(
                    state
                        .retired_lifecycle
                        .iter()
                        .map(|((id, _), record)| (*id, record)),
                )
                .filter_map(|(thread_id, record)| {
                    record.ticket.clone().map(|ticket| {
                        (
                            RetentionRetirementClaim {
                                thread_id,
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
        record.runtime = Arc::downgrade(thread);
        record.listener_completion = listener.listener_completion.clone();
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
