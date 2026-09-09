//! Private embedded-task custody. A joined task is not a cleanup receipt.

use futures::FutureExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use std::future::Future;
use std::io;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use tokio::sync::watch;
use tokio::task::AbortHandle;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::IN_PROCESS_CONNECTION_ID;
use crate::message_processor::ConnectionSessionState;
use crate::message_processor::MessageProcessor;
use crate::request_processors::AccountLoginReport;
use crate::request_processors::ProcessorThreadShutdown;

/// Host custody must outlive every embedded client started in it, including
/// startup failures and incomplete shutdown. Dropping this host is not a
/// successful shutdown. It is deliberately not a global background reaper.
#[derive(Default)]
pub struct InProcessHost {
    state: Mutex<HostCustody>,
}

#[derive(Default)]
struct HostCustody {
    closed: bool,
    runtimes: Vec<Arc<RuntimeCustody>>,
}

impl InProcessHost {
    pub(super) fn reserve(&self) -> io::Result<Arc<RuntimeCustody>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("embedded host custody unavailable"))?;
        if state.closed {
            return Err(io::Error::other("embedded host registration closed"));
        }
        let custody = Arc::new(RuntimeCustody::default());
        state.runtimes.push(Arc::clone(&custody));
        Ok(custody)
    }

    /// Freeze runtime and nested task births under the same locks used by
    /// registration. This is only admission closure, never a shutdown receipt.
    pub fn close_registration(&self) -> io::Result<()> {
        let (mut state, mut unavailable) = match self.state.lock() {
            Ok(state) => (state, false),
            Err(poisoned) => (poisoned.into_inner(), true),
        };
        state.closed = true;
        for runtime in &state.runtimes {
            match runtime.owners.lock() {
                Ok(mut owners) => owners.closed = true,
                Err(poisoned) => {
                    poisoned.into_inner().closed = true;
                    unavailable = true;
                }
            }
        }
        if unavailable {
            Err(io::Error::other("embedded runtime custody unavailable"))
        } else {
            Ok(())
        }
    }

    /// Freeze admission and observe every runtime retained by this host under
    /// one caller-owned absolute deadline. A report is evidence only: the
    /// current legacy processor cleanup is deliberately reported as
    /// `ReturnedUnverified` and therefore cannot be promoted to host Complete.
    pub async fn observe_until(&self, deadline: Instant) -> Vec<RuntimeShutdownReport> {
        // Registration closure is synchronous and idempotent. Do it here as
        // well as at explicit shutdown call sites so a direct observer cannot
        // snapshot a population while a new runtime is still being born.
        let _ = self.close_registration();
        let runtimes = match self.state.lock() {
            Ok(state) => state.runtimes.iter().map(Arc::clone).collect::<Vec<_>>(),
            Err(poisoned) => poisoned
                .into_inner()
                .runtimes
                .iter()
                .map(Arc::clone)
                .collect::<Vec<_>>(),
        };
        let reports = futures::future::join_all(
            runtimes
                .iter()
                .map(|runtime| runtime.observe_until(deadline)),
        )
        .await;
        // Only the exact positive predicate releases a host slot. Timeout,
        // cancellation, join failure, and legacy unverified cleanup retain
        // their original custody for replay or explicit process supersession.
        if let Ok(mut state) = self.state.lock() {
            state.runtimes.retain(|candidate| {
                !runtimes.iter().zip(&reports).any(|(runtime, report)| {
                    Arc::ptr_eq(candidate, runtime) && report.is_proven_complete()
                })
            });
        }
        reports
    }
}

#[derive(Clone, Copy)]
pub(super) enum RuntimeTask {
    Runtime,
    Processor,
    Outbound,
}

#[derive(Default)]
struct RuntimeOwners {
    closed: bool,
    runtime: Option<Arc<EmbeddedTaskOwner>>,
    processor: Option<Arc<EmbeddedTaskOwner>>,
    outbound: Option<Arc<EmbeddedTaskOwner>>,
    cleanup: Option<Arc<ProcessorCleanupOwner>>,
    cleanup_driver: Option<Arc<ProcessorCleanupDriver>>,
}

#[derive(Default)]
pub(super) struct RuntimeCustody {
    owners: Mutex<RuntimeOwners>,
}

impl RuntimeCustody {
    pub(super) fn spawn(
        &self,
        role: RuntimeTask,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> io::Result<Arc<EmbeddedTaskOwner>> {
        // Acquire before spawning: lock failure cannot leave an unregistered
        // task. Tokio does not synchronously poll the task inside spawn.
        let mut owners = self
            .owners
            .lock()
            .map_err(|_| io::Error::other("embedded runtime custody unavailable"))?;
        if owners.closed {
            return Err(io::Error::other(
                "embedded runtime task registration closed",
            ));
        }
        let slot = match role {
            RuntimeTask::Runtime => &mut owners.runtime,
            RuntimeTask::Processor => &mut owners.processor,
            RuntimeTask::Outbound => &mut owners.outbound,
        };
        if slot.is_some() {
            return Err(io::Error::other("embedded runtime task already registered"));
        }
        let owner = Arc::new(EmbeddedTaskOwner::spawn(future));
        *slot = Some(Arc::clone(&owner));
        Ok(owner)
    }

    pub(super) async fn observe_until(&self, deadline: Instant) -> RuntimeShutdownReport {
        let (runtime, processor, outbound, cleanup_driver) = {
            let mut owners = match self.owners.lock() {
                Ok(owners) => owners,
                Err(poisoned) => poisoned.into_inner(),
            };
            if owners.cleanup_driver.is_none()
                && let Some(cleanup) = owners.cleanup.as_ref()
            {
                owners.cleanup_driver = Some(Arc::new(ProcessorCleanupDriver::start(
                    Arc::clone(cleanup),
                    deadline,
                )));
            }
            (
                owners.runtime.as_ref().map(Arc::clone),
                owners.processor.as_ref().map(Arc::clone),
                owners.outbound.as_ref().map(Arc::clone),
                owners.cleanup_driver.as_ref().map(Arc::clone),
            )
        };

        let runtime = async {
            match runtime {
                Some(owner) => owner.observe_until(deadline).await,
                None => TaskObservation::Terminated(TaskTermination::Normal),
            }
        };
        let processor = async {
            match processor {
                Some(owner) => owner.observe_until(deadline).await,
                None => TaskObservation::Terminated(TaskTermination::Normal),
            }
        };
        let outbound = async {
            match outbound {
                Some(owner) => owner.observe_until(deadline).await,
                None => TaskObservation::Terminated(TaskTermination::Normal),
            }
        };
        let cleanup = async {
            match cleanup_driver {
                Some(driver) => Some(driver.observe_until(deadline).await),
                None => None,
            }
        };
        let (runtime, processor, outbound, cleanup) =
            tokio::join!(runtime, processor, outbound, cleanup);
        RuntimeShutdownReport {
            runtime,
            processor,
            outbound,
            cleanup,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeShutdownReport {
    pub runtime: TaskObservation,
    pub processor: TaskObservation,
    pub outbound: TaskObservation,
    pub cleanup: Option<(TaskObservation, ProcessorCleanupProgress)>,
}

impl RuntimeShutdownReport {
    /// A task join is never enough for Complete. Until the processor cleanup
    /// APIs return result-bearing resource receipts, this predicate remains
    /// false for the legacy `ReturnedUnverified` outcome.
    pub fn is_proven_complete(&self) -> bool {
        matches!(
            (
                &self.runtime,
                &self.processor,
                &self.outbound,
                &self.cleanup
            ),
            (
                TaskObservation::Terminated(TaskTermination::Normal),
                TaskObservation::Terminated(TaskTermination::Normal),
                TaskObservation::Terminated(TaskTermination::Normal),
                Some((
                    TaskObservation::Terminated(TaskTermination::Normal),
                    ProcessorCleanupProgress::Observed(
                        ProcessorCleanupExecution::ReturnedWithEvidence {
                            background_clean: true,
                            login_clean: true,
                            threads_clean: true,
                        },
                    ),
                )),
            )
        )
    }
}

/// Runtime tasks must capture this weak ticket, never their containing slot.
/// The host and client retain the slot; an executing task cannot form a cycle.
pub(super) struct RuntimeCustodyTicket(pub(super) Weak<RuntimeCustody>);

impl RuntimeCustodyTicket {
    pub(super) fn spawn(
        &self,
        role: RuntimeTask,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> io::Result<Arc<EmbeddedTaskOwner>> {
        self.0
            .upgrade()
            .ok_or_else(|| io::Error::other("embedded runtime host was dropped"))?
            .spawn(role, future)
    }

    pub(super) fn attach_cleanup(&self, cleanup: Arc<ProcessorCleanupOwner>) -> io::Result<()> {
        // Unlike task birth, transfer of an already-created resource must
        // remain possible after closure. The retained runtime join bounds
        // this late attachment population for the shutdown observer.
        let custody = self
            .0
            .upgrade()
            .ok_or_else(|| io::Error::other("embedded runtime host was dropped"))?;
        let mut owners = custody
            .owners
            .lock()
            .map_err(|_| io::Error::other("embedded runtime custody unavailable"))?;
        if owners.cleanup.is_some() {
            return Err(io::Error::other("embedded cleanup already registered"));
        }
        owners.cleanup = Some(cleanup);
        Ok(())
    }
}

/// Execution evidence only: the legacy cleanup calls do not yet return all
/// required resource receipts, so even ReturnedUnverified is not retirement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessorCleanupExecution {
    ReturnedUnverified,
    ReturnedWithEvidence {
        background_clean: bool,
        login_clean: bool,
        threads_clean: bool,
    },
    Panicked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessorCleanupProgress {
    Pending,
    Observed(ProcessorCleanupExecution),
    TimedOut,
}

/// Owns both the bounded poller and the original it polls. The spawned poller
/// captures only a cleanup clone and a progress sender, never this controller.
/// On deadline expiry the original remains here without being polled again.
pub(super) struct ProcessorCleanupDriver {
    _cleanup: Arc<ProcessorCleanupOwner>,
    task: EmbeddedTaskOwner,
    progress: watch::Receiver<ProcessorCleanupProgress>,
    deadline: Instant,
}

impl ProcessorCleanupDriver {
    pub(super) fn start(cleanup: Arc<ProcessorCleanupOwner>, deadline: Instant) -> Self {
        let (progress_tx, progress) = watch::channel(ProcessorCleanupProgress::Pending);
        let driven_cleanup = Arc::clone(&cleanup);
        let task = EmbeddedTaskOwner::spawn(async move {
            let observed = if let Some(outcome) = driven_cleanup.completion.peek() {
                ProcessorCleanupProgress::Observed(*outcome)
            } else if Instant::now() >= deadline {
                ProcessorCleanupProgress::TimedOut
            } else {
                tokio::select! {
                    biased;
                    _ = tokio::time::sleep_until(deadline) => ProcessorCleanupProgress::TimedOut,
                    result = driven_cleanup.drive_with_evidence(deadline) => ProcessorCleanupProgress::Observed(result),
                }
            };
            progress_tx.send_replace(observed);
        });
        Self {
            _cleanup: cleanup,
            task,
            progress,
            deadline,
        }
    }

    /// A shorter caller wait cannot shorten the driver's lifetime or extend
    /// its original bound. The join limb is separate from execution progress.
    pub(super) async fn observe_until(
        &self,
        observer_deadline: Instant,
    ) -> (TaskObservation, ProcessorCleanupProgress) {
        let task = self
            .task
            .observe_until(observer_deadline.min(self.deadline))
            .await;
        (task, *self.progress.borrow())
    }
}

/// Retains the original cleanup future independently of the processor loop.
/// The enclosing host must retain this owner and supply a driving observer;
/// storing Shared alone neither starts cleanup nor proves that it progressed.
pub(super) struct ProcessorCleanupOwner {
    completion: Shared<BoxFuture<'static, ProcessorCleanupExecution>>,
    // A ready Shared drops its future captures, including on unverified return
    // or panic. Keep resource custody independent of that execution receipt.
    // Neither current outcome proves cleanup, so neither may clear this owner.
    custody: Option<(Arc<MessageProcessor>, Arc<ConnectionSessionState>)>,
    subordinate: Option<Arc<MessageProcessor>>,
}

impl ProcessorCleanupOwner {
    pub(super) fn new(
        processor: Arc<MessageProcessor>,
        session: Arc<ConnectionSessionState>,
    ) -> Self {
        let custody = (Arc::clone(&processor), Arc::clone(&session));
        let cleanup_processor = Arc::clone(&processor);
        let mut owner = Self::from_cleanup(async move {
            cleanup_processor.clear_runtime_references();
            cleanup_processor.cancel_active_login().await;
            cleanup_processor
                .connection_closed(IN_PROCESS_CONNECTION_ID, &session)
                .await;
            cleanup_processor.clear_all_thread_listeners().await;
        });
        owner.custody = Some(custody);
        owner.subordinate = Some(processor);
        owner
    }

    fn from_cleanup(cleanup: impl Future<Output = ()> + Send + 'static) -> Self {
        let completion = async move {
            match AssertUnwindSafe(cleanup).catch_unwind().await {
                Ok(()) => ProcessorCleanupExecution::ReturnedUnverified,
                Err(_) => ProcessorCleanupExecution::Panicked,
            }
        }
        .boxed()
        .shared();
        Self {
            completion,
            custody: None,
            subordinate: None,
        }
    }

    /// The ordinary processor loop drives this under its existing lifetime.
    /// The host controller must bound its own observer with the original
    /// deadline; cancelling either observer preserves this original future.
    pub(super) async fn drive(&self) -> ProcessorCleanupExecution {
        self.completion.clone().await
    }

    async fn drive_with_evidence(&self, deadline: Instant) -> ProcessorCleanupExecution {
        let base = self.drive().await;
        let base_ok = !matches!(base, ProcessorCleanupExecution::Panicked);
        let Some(processor) = self.subordinate.as_ref() else {
            return base;
        };

        let login = processor.begin_login_shutdown(deadline).ok();
        let threads = processor.begin_thread_shutdown(deadline).ok();
        let (background, login, threads) = tokio::join!(
            AssertUnwindSafe(processor.drain_background_tasks_until(deadline)).catch_unwind(),
            async {
                match login {
                    Some(login) => AssertUnwindSafe(login.wait()).catch_unwind().await,
                    None => Ok(AccountLoginReport {
                        tasks: Default::default(),
                        browsers: Err(()),
                        admission_unavailable: true,
                    }),
                }
            },
            async {
                match threads {
                    Some(threads) => AssertUnwindSafe(threads.wait()).catch_unwind().await,
                    None => Ok(ProcessorThreadShutdown {
                        panicked: true,
                        ..Default::default()
                    }),
                }
            },
        );

        ProcessorCleanupExecution::ReturnedWithEvidence {
            background_clean: base_ok && background.is_ok_and(|report| report.is_clean()),
            login_clean: base_ok && login.is_ok_and(|report| report.is_clean()),
            threads_clean: base_ok && threads.is_ok_and(|report| report.is_complete()),
        }
    }
}

/// Terminal evidence about one task, independent of resource cleanup results.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskTermination {
    Normal,
    Cancelled,
    Panicked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskObservation {
    Terminated(TaskTermination),
    TimedOut,
}

type JoinReceipt = Shared<BoxFuture<'static, TaskTermination>>;

/// The host retains this original until terminal observation or actual process
/// exit. It has no timer-triggered abort and does not expose process authority.
/// Dropping an observer never takes the original JoinHandle out of this owner.
pub(super) struct EmbeddedTaskOwner {
    completion: JoinReceipt,
    terminal_hint: AbortHandle,
}

impl EmbeddedTaskOwner {
    /// Spawn and retain the task synchronously, before any caller await.
    /// The spawned task progresses without an observation future being polled.
    /// Its captures must not strongly own the host containing this owner.
    pub(super) fn spawn(task: impl Future<Output = ()> + Send + 'static) -> Self {
        Self::from_handle(tokio::spawn(task))
    }

    pub(super) fn from_handle(handle: JoinHandle<()>) -> Self {
        let terminal_hint = handle.abort_handle();
        let completion = async move {
            match handle.await {
                Ok(()) => TaskTermination::Normal,
                Err(error) if error.is_cancelled() => TaskTermination::Cancelled,
                Err(_) => TaskTermination::Panicked,
            }
        }
        .boxed()
        .shared();
        Self {
            completion,
            terminal_hint,
        }
    }

    pub(super) fn request_abort(&self) {
        self.terminal_hint.abort();
    }

    pub(super) async fn join(&self) -> TaskTermination {
        self.completion.clone().await
    }

    /// Observe without consuming custody or introducing a new cleanup budget.
    /// The caller supplies its already-established absolute deadline.
    pub(super) async fn observe_until(&self, deadline: Instant) -> TaskObservation {
        if let Some(outcome) = self.completion.peek() {
            return TaskObservation::Terminated(*outcome);
        }
        // This future only observes a JoinHandle: polling it cannot resume
        // the task or cleanup. The hint alone is not proof; normalize the
        // actual join before reporting terminality, even after the deadline.
        if self.terminal_hint.is_finished()
            && let Some(outcome) = self.completion.clone().now_or_never()
        {
            return TaskObservation::Terminated(outcome);
        }
        if Instant::now() >= deadline {
            return TaskObservation::TimedOut;
        }
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => TaskObservation::TimedOut,
            outcome = self.completion.clone() => TaskObservation::Terminated(outcome),
        }
    }
}

#[cfg(test)]
#[path = "shutdown_tests.rs"]
mod tests;
