//! Permanent host shutdown of the processor's two thread populations.
//!
//! Manager custody includes removed and unpublished runtimes. Prior idle
//! tickets separately retain their original classifications and deadlines.

use crate::thread_state::ThreadStateManager;
use codex_core::ThreadManager;
use codex_core::ThreadManagerRetirementError;
use codex_core::ThreadManagerRetirementReport;
use codex_core::ThreadRetirementReport;
use codex_protocol::ThreadId;
use futures::FutureExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::watch;
use tokio::time::Instant;
use uuid::Uuid;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ProcessorThreadShutdown {
    // None means not yet observed, never an empty successful population.
    pub manager: Option<Result<ThreadManagerRetirementReport, ThreadManagerRetirementError>>,
    pub prior: Option<Vec<(ThreadId, Uuid, ThreadRetirementReport)>>,
    pub panicked: bool,
    pub deadline_expired: bool,
}

impl ProcessorThreadShutdown {
    pub(crate) fn is_complete(&self) -> bool {
        !self.panicked
            && !self.deadline_expired
            && self.manager.as_ref().is_some_and(|result| {
                result
                    .as_ref()
                    .is_ok_and(ThreadManagerRetirementReport::is_complete)
            })
            && self
                .prior
                .as_ref()
                .is_some_and(|reports| reports.iter().all(|(_, _, report)| report.is_complete()))
    }
}

#[derive(Clone, Default)]
pub(crate) struct ThreadShutdownOwner {
    attempt: Arc<Mutex<Option<ProcessorThreadRetirement>>>,
}

/// The host retains this ticket or its processor until positive completion.
/// The original is not self-driving; the host supplies the bounded driver.
#[derive(Clone)]
pub(crate) struct ProcessorThreadRetirement {
    deadline: Instant,
    completion: Shared<BoxFuture<'static, ProcessorThreadShutdown>>,
    progress: watch::Receiver<ProcessorThreadShutdown>,
    // Completed incomplete futures drop their captures. This independent cell
    // retains both populations even after an incomplete report becomes ready.
    _custody: Arc<Mutex<Option<ThreadCustody>>>,
}

struct ThreadCustody {
    _manager: Arc<ThreadManager>,
    _state: ThreadStateManager,
}

impl ThreadShutdownOwner {
    pub(crate) fn begin(
        &self,
        manager: &Arc<ThreadManager>,
        state: &ThreadStateManager,
        deadline: Instant,
    ) -> Result<ProcessorThreadRetirement, ThreadManagerRetirementError> {
        let mut slot = self
            .attempt
            .lock()
            .map_err(|_| ThreadManagerRetirementError::AuthorityUnavailable)?;
        if let Some(attempt) = slot.as_ref() {
            return Ok(attempt.clone());
        }
        // Close construction/publication before returning, not on first poll.
        // A failed manager admission must not suppress prior-ticket cleanup.
        let manager_ticket = manager.begin_shutdown(deadline);
        let deadline = manager_ticket
            .as_ref()
            .map_or(deadline, |ticket| deadline.min(ticket.deadline()));
        let custody = Arc::new(Mutex::new(Some(ThreadCustody {
            _manager: Arc::clone(manager),
            _state: state.clone(),
        })));
        let release = Arc::downgrade(&custody);
        let state = state.clone();
        let (progress_tx, progress) = watch::channel(ProcessorThreadShutdown::default());
        let completion = async move {
            let manager_work = async {
                let result = match manager_ticket {
                    Ok(ticket) => ticket.wait().await,
                    Err(error) => Err(error),
                };
                progress_tx.send_modify(|report| report.manager = Some(result));
            };
            let prior_work = async {
                let reports = state
                    .drain_retirement_tickets()
                    .await
                    .into_iter()
                    .map(|(claim, report)| (claim.thread_id, claim.generation, report))
                    .collect();
                progress_tx.send_modify(|report| report.prior = Some(reports));
            };
            // Neither an unavailable/contended lifecycle table nor a manager
            // failure may starve the other population's actual cleanup.
            let (manager_result, prior_result) = tokio::join!(
                AssertUnwindSafe(manager_work).catch_unwind(),
                AssertUnwindSafe(prior_work).catch_unwind(),
            );
            if manager_result.is_err() || prior_result.is_err() {
                progress_tx.send_modify(|report| report.panicked = true);
            }
            let report = progress_tx.borrow().clone();
            if report.is_complete()
                && let Some(custody) = release.upgrade()
            {
                let released = custody
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                drop(released);
            }
            report
        }
        .boxed()
        .shared();
        let attempt = ProcessorThreadRetirement {
            deadline,
            completion,
            progress,
            _custody: custody,
        };
        *slot = Some(attempt.clone());
        Ok(attempt)
    }
}

impl ProcessorThreadRetirement {
    pub(crate) fn deadline(&self) -> Instant {
        self.deadline
    }

    pub(crate) async fn wait(&self) -> ProcessorThreadShutdown {
        if let Some(report) = self.completion.peek() {
            return report.clone();
        }
        if Instant::now() < self.deadline {
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(self.deadline) => {},
                report = self.completion.clone() => return report,
            }
        }
        // Do not first-poll/resume side effects after expiry. Recorded partial
        // evidence survives, and the same custody remains available to host.
        let mut report = self.progress.borrow().clone();
        report.deadline_expired = true;
        report
    }
}

#[cfg(test)]
#[path = "shutdown_tests.rs"]
pub(crate) mod tests;
