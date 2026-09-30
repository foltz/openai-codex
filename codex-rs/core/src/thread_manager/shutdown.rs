//! One permanent manager shutdown, independent of observer cancellation.
//!
//! Lookup membership is not the owned population: removed runtimes remain in
//! construction custody until their exact cleanup is positively observed.

use super::ThreadManager;
use super::retirement::ConstructionDrain;
use crate::ThreadRetirementError;
use crate::ThreadRetirementReport;
use crate::session::SessionLoopOutcome;
use codex_protocol::ThreadId;
use futures::FutureExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::time::Instant;

/// Fixed failure evidence. The manager keeps the original owners on failure;
/// this value is not a capability to discard or detach them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ThreadManagerRetirementError {
    AuthorityUnavailable,
    Panicked,
    TimedOut,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RuntimeOutcome {
    /// Already recorded cleanup, not a new ordinary shutdown classification.
    PreviouslyCompleted(SessionLoopOutcome),
    Retirement(ThreadRetirementReport),
    Refused(ThreadRetirementError),
}

impl RuntimeOutcome {
    fn is_complete(self) -> bool {
        match self {
            // A previously recorded cleanup receipt still needs the exact
            // session-loop terminal outcome. Cleanup completion alone cannot
            // certify that the thread itself retired; a cancelled or panicked
            // loop remains non-terminal evidence.
            Self::PreviouslyCompleted(outcome) => {
                matches!(outcome, SessionLoopOutcome::Normal)
            }
            // Manager shutdown does not turn an ordinary failure or an
            // abnormal loop exit into successful thread retirement.
            Self::Retirement(report) => report.is_complete(),
            Self::Refused(_) => false,
        }
    }
}

/// Constructor, resource-cleanup and exact-map-removal evidence remain
/// conjunctive. Debug output contains fixed categories and thread IDs only.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ThreadManagerRetirementReport {
    constructors: ConstructionDrain,
    runtimes: Vec<(ThreadId, RuntimeOutcome)>,
    map_empty: bool,
}

impl ThreadManagerRetirementReport {
    pub fn is_complete(&self) -> bool {
        self.constructors.finished
            && !self.constructors.panicked
            && !self.constructors.unavailable
            && self.constructors.unpublished == 0
            && self
                .constructors
                .sessions
                .iter()
                .all(|outcome| outcome.is_complete())
            && self
                .runtimes
                .iter()
                .all(|(_, outcome)| outcome.is_complete())
            && self.map_empty
    }
}

/// Observation of the manager's retained original. It is deliberately not
/// self-driving: the enclosing host owns the driver and must retain the manager
/// or this custody-bearing ticket if incomplete. Repeated observations cannot
/// extend the first deadline.
#[derive(Clone)]
pub struct ThreadManagerRetirement {
    deadline: Instant,
    completion: Shared<
        BoxFuture<'static, Result<ThreadManagerRetirementReport, ThreadManagerRetirementError>>,
    >,
    // A completed *incomplete report* must not drop the resources captured by
    // its future. Only positive proof clears this independent custody cell.
    _custody: Arc<Mutex<Option<RetirementCustody>>>,
}

struct RetirementCustody {
    _state: Arc<super::ThreadManagerState>,
    _constructions: super::retirement::ThreadConstructions,
}

impl ThreadManagerRetirement {
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    pub async fn wait(
        &self,
    ) -> Result<ThreadManagerRetirementReport, ThreadManagerRetirementError> {
        if let Some(result) = self.completion.peek() {
            return result.clone();
        }
        if Instant::now() >= self.deadline {
            return Err(ThreadManagerRetirementError::TimedOut);
        }
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(self.deadline) => Err(ThreadManagerRetirementError::TimedOut),
            result = self.completion.clone() => result,
        }
    }
}

impl ThreadManager {
    /// Permanently close construction/publication and retain one original
    /// all-thread shutdown before any cancellable wait. This is not the legacy
    /// reusable bulk-shutdown API and must not be used as an account reset gate
    /// that expects this same manager to admit new threads afterward.
    pub fn begin_shutdown(
        &self,
        deadline: Instant,
    ) -> Result<ThreadManagerRetirement, ThreadManagerRetirementError> {
        let mut slot = self
            .shutdown
            .lock()
            .map_err(|_| ThreadManagerRetirementError::AuthorityUnavailable)?;
        if let Some(ticket) = slot.as_ref() {
            return Ok(ticket.clone());
        }
        let deadline = self
            .constructions
            .close_until(deadline)
            .map_err(|_| ThreadManagerRetirementError::AuthorityUnavailable)?;
        let constructions = self.constructions.clone();
        let state = Arc::clone(&self.state);
        let custody = Arc::new(Mutex::new(Some(RetirementCustody {
            _state: Arc::clone(&state),
            _constructions: constructions.clone(),
        })));
        let release_custody = Arc::downgrade(&custody);
        // Manager owns this future, but the future does not own Manager. State
        // carries only a weak construction ticket, so neither path cycles.
        let completion = AssertUnwindSafe(async move {
            let population = constructions.published();
            // Final publication shares the already-closed gate: constructors
            // still in flight can only fail publication and drain their own
            // unpublished-session custody, never add a missed runtime here.
            let runtime_work =
                futures::future::join_all(population.iter().map(|thread| async move {
                    if let Some(outcome) = thread.observed_terminal_cleanup() {
                        RuntimeOutcome::PreviouslyCompleted(outcome)
                    } else if Instant::now() >= deadline {
                        RuntimeOutcome::Refused(ThreadRetirementError::DeadlineExpired)
                    } else {
                        match thread.begin_retirement(deadline) {
                            Ok(ticket) => RuntimeOutcome::Retirement(ticket.wait().await),
                            Err(error) => RuntimeOutcome::Refused(error),
                        }
                    }
                }));
            let (constructors, outcomes) =
                tokio::join!(constructions.drain_until(deadline), runtime_work);
            let mut runtimes = population
                .iter()
                .zip(&outcomes)
                .map(|(thread, outcome)| (thread.session.thread_id(), *outcome))
                .collect::<Vec<_>>();
            // The lookup lock cannot starve constructor/resource retirement:
            // acquire it only after those independent proofs have been driven.
            let mut threads = state.threads.write().await;
            if Instant::now() >= deadline {
                return Err(ThreadManagerRetirementError::TimedOut);
            }
            let mut remove = Vec::new();
            for (id, thread) in threads.iter() {
                if let Some((_, outcome)) = population
                    .iter()
                    .zip(&outcomes)
                    .find(|(owned, _)| Arc::ptr_eq(owned, thread))
                {
                    if outcome.is_complete() {
                        remove.push(*id);
                    }
                } else if let Some(outcome) = thread.observed_terminal_cleanup() {
                    // Normal-operation compaction may already have released
                    // registry custody, while the lookup still holds a shell.
                    runtimes.push((*id, RuntimeOutcome::PreviouslyCompleted(outcome)));
                    remove.push(*id);
                }
            }
            let removed = remove
                .into_iter()
                .filter_map(|id| threads.remove(&id))
                .collect::<Vec<_>>();
            let map_empty = threads.is_empty();
            drop(threads);
            drop(removed);
            // Only observed positive entries compact; failures retain their
            // original Thread/Session/cleanup owners independently of this report.
            drop(constructions.published());
            let report = ThreadManagerRetirementReport {
                constructors,
                runtimes,
                map_empty,
            };
            if report.is_complete()
                && let Some(custody) = release_custody.upgrade()
            {
                let released = custody
                    .lock()
                    .map_err(|_| ThreadManagerRetirementError::AuthorityUnavailable)?
                    .take();
                drop(released);
            }
            Ok(report)
        })
        .catch_unwind()
        .map(|result| result.unwrap_or(Err(ThreadManagerRetirementError::Panicked)))
        .boxed()
        .shared();
        let ticket = ThreadManagerRetirement {
            deadline,
            completion,
            _custody: custody,
        };
        *slot = Some(ticket.clone());
        Ok(ticket)
    }
}

#[cfg(test)]
#[path = "shutdown_tests.rs"]
mod tests;
