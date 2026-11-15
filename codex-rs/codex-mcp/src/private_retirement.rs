//! Private startup-attempt custody, separate from pooled ordinary leases.
//! This registry does not cover pooled connections or the whole MCP runtime.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;

use codex_rmcp_client::PhysicalRetirementReport;
use codex_rmcp_client::RmcpClientRetirement;
use futures::FutureExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// Task completion does not imply physical transport retirement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrivateTaskOutcome {
    Complete,
    Skipped,
    Failed,
    TimedOut,
}

/// Exact census result; failures retain the corresponding owners for retry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrivateTerminationReport {
    pub connections: Vec<(usize, PhysicalRetirementReport)>,
    pub tasks: Vec<(usize, PrivateTaskOutcome)>,
}

impl PrivateTerminationReport {
    pub fn is_complete(&self) -> bool {
        self.connections
            .iter()
            .all(|(_, report)| report.is_complete())
            && self.tasks.iter().all(|(_, result)| {
                matches!(
                    result,
                    PrivateTaskOutcome::Complete | PrivateTaskOutcome::Skipped
                )
            })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("MCP runtime registration is closed")]
pub(crate) struct PrivateRegistrationClosed;

type TaskCompletion = Shared<BoxFuture<'static, PrivateTaskOutcome>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TaskAdmission {
    Dormant,
    Admitted,
    Skipped,
}

#[derive(Clone)]
struct TaskOwner {
    completion: TaskCompletion,
    admission: Arc<Mutex<TaskAdmission>>,
}

#[derive(Default)]
struct RegistryState {
    closed: bool,
    internal_deadline: Option<Instant>,
    connections: Vec<Arc<ConnectionRetirementOwner>>,
    tasks: Vec<TaskOwner>,
    terminal: Option<PrivateTerminationReport>,
}

/// This owner must outlive every binding, startup task, and retirement observer.
#[derive(Clone, Default)]
pub(crate) struct PrivateRetirementRegistry {
    state: Arc<Mutex<RegistryState>>,
}

/// Custody for private startup attempts that may be dropped or whose constructor
/// may be cancelled. This exposes observation and closure, not task admission.
#[derive(Clone, Default)]
pub struct PrivateClientRetirement {
    pub(crate) registry: PrivateRetirementRegistry,
}

impl PrivateClientRetirement {
    /// Preserve lazy startup while retaining its exact original completion and
    /// lower owner before any factory can be polled. The factory must not put a
    /// strong containing registry into the returned future.
    pub(crate) fn track_startup(
        &self,
        cancellation: CancellationToken,
        factory: impl FnOnce(RmcpClientRetirement) -> crate::rmcp_client::ManagedClientFuture,
    ) -> crate::rmcp_client::ManagedClientFuture {
        use crate::rmcp_client::StartupOutcomeError;
        let refused = || {
            futures::future::ready(Err(StartupOutcomeError::Cancelled))
                .boxed()
                .shared()
        };
        let Ok(owner) = self.registry.register_connection(cancellation) else {
            return refused();
        };
        let startup = factory(owner.lower());
        let observed = startup.clone();
        let Ok(completion) = owner.task_ticket().register(move || async move {
            // Settled work is separate from positive lower physical proof.
            let _ = observed.await;
            PrivateTaskOutcome::Complete
        }) else {
            return refused();
        };
        async move {
            match completion.await {
                PrivateTaskOutcome::Complete => startup.await,
                PrivateTaskOutcome::Skipped
                | PrivateTaskOutcome::Failed
                | PrivateTaskOutcome::TimedOut => Err(StartupOutcomeError::Cancelled),
            }
        }
        .boxed()
        .shared()
    }

    /// Background cleanup cannot move an earlier deadline forward. A caller's
    /// explicit shutdown_until remains the separate bounded retry API.
    pub(crate) async fn shutdown_within_latched(
        &self,
        deadline: Instant,
    ) -> PrivateTerminationReport {
        let deadline = {
            let mut state = self
                .registry
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let deadline = state
                .internal_deadline
                .map_or(deadline, |old| old.min(deadline));
            state.internal_deadline = Some(deadline);
            deadline
        };
        self.shutdown_until(deadline).await
    }

    /// Refuse further physical construction before beginning asynchronous drain.
    pub fn close_registration(&self) {
        self.registry.close_registration();
    }

    /// Observe the retained exact population; incomplete results retain custody.
    pub async fn shutdown_until(&self, deadline: Instant) -> PrivateTerminationReport {
        self.registry.shutdown_until(deadline).await
    }

    /// Only a previously observed positive report permits owner compaction.
    pub fn is_retired(&self) -> bool {
        self.registry
            .state
            .lock()
            .is_ok_and(|state| state.terminal.is_some())
    }
}

/// Stable identity reused with an exact McpServerConnection, never a whole set.
pub(crate) struct ConnectionRetirementOwner {
    id: usize,
    lower: RmcpClientRetirement,
    cancellation: CancellationToken,
    registry: Weak<Mutex<RegistryState>>,
}

/// Tasks capture this weak ticket, not their strong containing runtime owner.
#[derive(Clone)]
pub(crate) struct PrivateTaskTicket {
    registry: Weak<Mutex<RegistryState>>,
}

impl ConnectionRetirementOwner {
    pub(crate) fn id(&self) -> usize {
        self.id
    }
    pub(crate) fn lower(&self) -> RmcpClientRetirement {
        self.lower.clone()
    }
    pub(crate) fn task_ticket(&self) -> PrivateTaskTicket {
        PrivateTaskTicket {
            registry: self.registry.clone(),
        }
    }
}

impl PrivateRetirementRegistry {
    /// Reserve before AsyncManagedClient construction or set publication.
    pub(crate) fn register_connection(
        &self,
        cancellation: CancellationToken,
    ) -> Result<Arc<ConnectionRetirementOwner>, PrivateRegistrationClosed> {
        let mut state = self.state.lock().map_err(|_| PrivateRegistrationClosed)?;
        if state.closed {
            return Err(PrivateRegistrationClosed);
        }
        let owner = Arc::new(ConnectionRetirementOwner {
            id: state.connections.len(),
            lower: RmcpClientRetirement::default(),
            cancellation,
            registry: Arc::downgrade(&self.state),
        });
        state.connections.push(Arc::clone(&owner));
        Ok(owner)
    }

    /// Includes runtime-wide startup-summary work, not just one connection.
    pub(crate) fn task_ticket(&self) -> PrivateTaskTicket {
        PrivateTaskTicket {
            registry: Arc::downgrade(&self.state),
        }
    }

    pub(crate) fn close_registration(&self) {
        let state = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.closed = true;
            for task in &state.tasks {
                let mut admission = task
                    .admission
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if *admission == TaskAdmission::Dormant {
                    *admission = TaskAdmission::Skipped;
                }
            }
            // Lower gates close before any cancelled future can be driven again.
            for owner in &state.connections {
                owner.lower.close_registration();
            }
            state.connections.clone()
        };
        for owner in state {
            owner.cancellation.cancel();
        }
    }

    pub(crate) async fn shutdown_until(&self, deadline: Instant) -> PrivateTerminationReport {
        self.close_registration();
        let (connections, tasks) = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(report) = &state.terminal {
                return report.clone();
            }
            (state.connections.clone(), state.tasks.clone())
        };
        let close = futures::future::join_all(
            connections
                .iter()
                .map(|owner| async { (owner.id, owner.lower.shutdown_until(deadline).await) }),
        );
        let drain =
            futures::future::join_all(tasks.into_iter().enumerate().map(|(id, task)| async move {
                if let Some(outcome) = task.completion.peek() {
                    return (id, *outcome);
                }
                if Instant::now() >= deadline {
                    return (id, PrivateTaskOutcome::TimedOut);
                }
                (
                    id,
                    tokio::time::timeout_at(deadline, task.completion)
                        .await
                        .unwrap_or(PrivateTaskOutcome::TimedOut),
                )
            }));
        let (mut reports, mut tasks) = tokio::join!(close, drain);
        if tasks
            .iter()
            .all(|(_, outcome)| *outcome != PrivateTaskOutcome::TimedOut)
            && reports.iter().any(|(_, report)| !report.is_complete())
        {
            // Startup can still hold a just-created pending transport while
            // the first cleanup pass observes it. Once all upper work has
            // drained (including failed work), reconcile returned leases under
            // the SAME deadline. A failed task still prevents overall success.
            // The first pass remains concurrent so a stuck startup never delays
            // termination requests for already-attached physical processes.
            reports = futures::future::join_all(
                connections
                    .iter()
                    .map(|owner| async { (owner.id, owner.lower.shutdown_until(deadline).await) }),
            )
            .await;
        }
        // Recovering the guard permits best-effort resource cleanup, never a
        // successful ownership claim after an interrupted registry mutation.
        if self.state.is_poisoned() {
            tasks.push((tasks.len(), PrivateTaskOutcome::Failed));
        }
        let report = PrivateTerminationReport {
            connections: reports,
            tasks,
        };
        if report.is_complete() {
            let retired = {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.terminal = Some(report.clone());
                (
                    std::mem::take(&mut state.connections),
                    std::mem::take(&mut state.tasks),
                )
            };
            drop(retired);
        }
        report
    }
}

impl PrivateTaskTicket {
    /// Retains the original completion before first poll. Poll returned clones
    /// to drive normal startup. Factories must not capture a strong registry or
    /// connection owner: that would create a registry/future ownership cycle.
    pub(crate) fn register<F, Fut>(
        &self,
        factory: F,
    ) -> Result<TaskCompletion, PrivateRegistrationClosed>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = PrivateTaskOutcome> + Send + 'static,
    {
        let registry = self.registry.upgrade().ok_or(PrivateRegistrationClosed)?;
        let mut state = registry.lock().map_err(|_| PrivateRegistrationClosed)?;
        if state.closed {
            return Err(PrivateRegistrationClosed);
        }
        let weak = self.registry.clone();
        let admission = Arc::new(Mutex::new(TaskAdmission::Dormant));
        let observed_admission = Arc::clone(&admission);
        let task = async move {
            let Some(registry) = weak.upgrade() else {
                return PrivateTaskOutcome::Skipped;
            };
            {
                let Ok(state) = registry.lock() else {
                    return PrivateTaskOutcome::Failed;
                };
                let Ok(mut admission) = observed_admission.lock() else {
                    return PrivateTaskOutcome::Failed;
                };
                if state.closed || *admission == TaskAdmission::Skipped {
                    *admission = TaskAdmission::Skipped;
                    return PrivateTaskOutcome::Skipped;
                }
                // This transition, not later factory execution, admits the
                // task atomically against close_registration. Previously
                // admitted work may run/drain after the gate has closed.
                *admission = TaskAdmission::Admitted;
            }
            drop(registry);
            AssertUnwindSafe(async move { factory().await })
                .catch_unwind()
                .await
                .unwrap_or(PrivateTaskOutcome::Failed)
        }
        .boxed()
        .shared();
        state.tasks.push(TaskOwner {
            completion: task.clone(),
            admission,
        });
        Ok(task)
    }
}

#[cfg(test)]
#[path = "private_retirement_tests.rs"]
mod tests;
