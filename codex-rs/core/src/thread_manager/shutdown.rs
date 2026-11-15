//! Unloads locally controlled idle MAv2 children and exposes tracked agent-tree teardown.
//! Also retains the original permanent manager retirement transaction.

use super::CodexThread;
use super::retirement::ConstructionDrain;
use crate::ThreadRetirementError;
use crate::ThreadRetirementReport;
use crate::session::SessionLoopOutcome;
use futures::FutureExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use std::panic::AssertUnwindSafe;
use std::sync::Mutex;
use tokio::time::Instant;

use super::ThreadManager;
use crate::agent::control::AgentTreeShutdownState;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CodexResult;
use codex_thread_store::ThreadStoreError;
use std::fmt;
use std::sync::Arc;
use uuid::Uuid;

/// Return a stable, payload-free category for a thread-store error.
pub(crate) fn thread_store_error_kind(error: &ThreadStoreError) -> &'static str {
    match error {
        ThreadStoreError::ThreadNotFound { .. } => "thread_store_not_found",
        ThreadStoreError::InvalidRequest { .. } => "thread_store_invalid_request",
        ThreadStoreError::Conflict { .. } => "thread_store_conflict",
        ThreadStoreError::Unsupported { .. } => "thread_store_unsupported",
        ThreadStoreError::Retryable { .. } => "thread_store_retryable",
        ThreadStoreError::OutcomeUnknown { .. } => "thread_store_outcome_unknown",
        ThreadStoreError::Internal { .. } => "thread_store_internal",
    }
}

/// A safely loggable reason that a tracked part of an agent tree failed to shut down.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentTreeShutdownFailureReason {
    /// An operation returned an error at a specific step. The kind is a stable,
    /// non-sensitive classification.
    OperationFailed {
        phase: &'static str,
        error_kind: &'static str,
    },
    /// A tracked operation was dropped before it could report that its work completed.
    GuardAbandoned,
}

/// One failure observed during the lifetime or teardown of an agent tree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentTreeShutdownFailure {
    pub operation: &'static str,
    pub thread_id: Option<ThreadId>,
    pub reason: AgentTreeShutdownFailureReason,
}

impl AgentTreeShutdownFailure {
    /// Classifies an operation failure without retaining arbitrary error messages.
    pub fn operation_failed(
        operation: &'static str,
        phase: &'static str,
        thread_id: Option<ThreadId>,
        error_kind: &'static str,
    ) -> Self {
        Self {
            operation,
            thread_id,
            reason: AgentTreeShutdownFailureReason::OperationFailed { phase, error_kind },
        }
    }
}

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

/// The bounded set of failures observed for one local agent-tree instance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentTreeShutdownReport {
    /// Correlation ID for this runtime instance; this is not a thread's ID.
    pub tree_id: Uuid,
    /// Failures retained in recording order. Each failure is also logged when recorded.
    pub failures: Vec<AgentTreeShutdownFailure>,
    /// Number of additional failures omitted after the retention limit was reached.
    pub omitted_failures: usize,
}

impl fmt::Display for AgentTreeShutdownReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "agent tree shutdown did not complete cleanly: tree_id={}, failures=[",
            self.tree_id
        )?;
        for (index, failure) in self.failures.iter().enumerate() {
            if index > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{{operation={}", failure.operation)?;
            if let AgentTreeShutdownFailureReason::OperationFailed { phase, .. } = &failure.reason {
                write!(f, ", phase={phase}")?;
            }
            if let Some(thread_id) = failure.thread_id {
                write!(f, ", thread_id={thread_id}")?;
            }
            match &failure.reason {
                AgentTreeShutdownFailureReason::OperationFailed { error_kind, .. } => {
                    write!(f, ", reason=operation_failed, error_kind={error_kind}")?;
                }
                AgentTreeShutdownFailureReason::GuardAbandoned => {
                    write!(f, ", reason=guard_abandoned")?;
                }
            }
            write!(f, "}}")?;
        }
        write!(f, "]; omitted_failures={}", self.omitted_failures)
    }
}

impl std::error::Error for AgentTreeShutdownReport {}

/// Completion handle for one exact local agent tree.
#[derive(Clone, Debug)]
#[must_use = "agent-tree shutdown is not complete until wait() finishes"]
pub struct AgentTreeShutdown {
    state: Arc<AgentTreeShutdownState>,
}

impl AgentTreeShutdown {
    /// Waits for every operation and session admitted before the shutdown fence to finish.
    /// Returns an error if cleanup or persistence writer termination failed. Registry removal and
    /// caller-owned persistence handoff remain with the caller. Dropping this future does not
    /// cancel shutdown.
    pub async fn wait(&self) -> CodexResult<()> {
        self.state.wait().await
    }

    /// Waits for the tree and returns its recorded failure report if cleanup was unsuccessful.
    /// Each call observes the same completed report. Dropping the future does not cancel shutdown.
    pub async fn wait_detailed(&self) -> Result<(), AgentTreeShutdownReport> {
        self.state.wait_detailed().await
    }
}

impl ThreadManager {
    /// Unloads this exact idle MAv2 child using the manager's local controller backend,
    /// preserving mail and environments for reloading. Host controllers own their eviction
    /// and recovery; managers configured with a host controller factory are rejected.
    /// Dropping the future after shutdown starts does not cancel cleanup.
    pub async fn try_evict_v2_thread(
        &self,
        thread: Arc<crate::CodexThread>,
    ) -> CodexResult<crate::ThreadEvictionOutcome> {
        // Backend selection is fixed before the manager is shared or starts any threads.
        if self.state.agent_control_factory.is_some() {
            return Err(CodexErr::InvalidRequest(
                "idle eviction requires the local agent controller".to_owned(),
            ));
        }
        let Ok(membership) = thread.session.services.local_agent_runtime.admit_start() else {
            return Ok(crate::ThreadEvictionOutcome::Busy);
        };
        self.state.try_evict_v2_thread(thread, &membership).await
    }

    /// Requests shutdown of a loaded thread and every session sharing its local runtime.
    ///
    /// Returns after fencing new starts and signalling admitted sessions. Callers can wait on the
    /// returned handle before completing registry cleanup or a persistence handoff. This fences
    /// only starts sharing the current runtime; callers must separately serialize top-level loads
    /// or resumes of the same thread ID through that handoff.
    pub async fn request_agent_tree_shutdown(
        &self,
        thread_id: ThreadId,
    ) -> CodexResult<AgentTreeShutdown> {
        let thread = self.get_thread(thread_id).await?;
        Ok(AgentTreeShutdown {
            state: thread
                .session
                .services
                .local_agent_runtime
                .request_shutdown(),
        })
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
        // Permanent host/manager shutdown edge, with this same deadline (no new
        // budget): bind every reachable runtime through the edge method and
        // latch it in the registry. A runtime whose cleanup already started in
        // Legacy mode is refused (the retained completion below re-observes
        // and reports it) and clamped live to this deadline, so it is never
        // driven past this host's owner deadline. Reusable reset
        // (`shutdown_all_threads_bounded`) and per-thread retirement keep the
        // ordinary, non-clamping binding.
        let _ = self.bind_shutdown_deadlines_now(deadline);
        // Fence every reachable agent tree before any constructor or runtime is
        // drained: child, delegate and Guardian starts admitted through a shared
        // runtime are refused, and in-flight session spawns are cancelled inside
        // their retained constructors, so they still appear in the drain below.
        // Every non-terminal map entry is also published population; the lookup
        // is best-effort coverage for compacted shells sharing a live tree.
        let mut reachable = self.constructions.published();
        if let Ok(threads) = self.state.threads.try_read() {
            reachable.extend(threads.values().cloned());
        }
        fence_agent_trees(&reachable, deadline);
        drop(reachable);
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

/// Bind each reachable thread's exact retirement deadline, then request upstream
/// agent-tree shutdown on its runtime. Tree shutdown signals session loops
/// directly, and cleanup that began unbounded can never accept a later deadline,
/// so binding precedes the signal. Requests are idempotent per shared runtime.
/// The tree handles are not retained: manager evidence stays the exact-thread
/// and constructor drain, whose failures keep their original owners.
fn fence_agent_trees(threads: &[Arc<CodexThread>], deadline: Instant) {
    for thread in threads {
        // Bound the session's sandbox launch cleanup directly as well: the
        // thread may already be past its own binding (terminal cleanup
        // observed, or the deadline elapsed) while launches still run.
        thread
            .session
            .services
            .sandbox_cleanup_scope()
            .tighten(deadline.into_std());
        if thread.observed_terminal_cleanup().is_none() && Instant::now() < deadline {
            // A refusal here is re-observed and reported by the retained completion.
            let _ = thread.begin_retirement(deadline);
        }
        drop(
            thread
                .session
                .services
                .local_agent_runtime
                .request_shutdown(),
        );
    }
}

#[cfg(test)]
#[path = "shutdown_tests.rs"]
mod tests;
