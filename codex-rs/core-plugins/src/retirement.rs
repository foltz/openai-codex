//! Result-bearing ownership for plugin workers started during account/config
//! refresh. A cancellation request is not a terminal receipt: the registry
//! retains the original join until it is actually observed or the common
//! deadline expires.

use futures::FutureExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::runtime::Handle;
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum JoinOutcome {
    Joined,
    Cancelled,
    Panicked,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PluginTaskDrain {
    pub terminal: bool,
    pub cancelled: bool,
    pub panicked: bool,
    pub unavailable: bool,
}

impl PluginTaskDrain {
    pub fn is_clean(self) -> bool {
        self.terminal && !self.cancelled && !self.panicked && !self.unavailable
    }
}

pub(crate) type Receipt = Shared<BoxFuture<'static, JoinOutcome>>;

#[derive(Default)]
struct State {
    closed: bool,
    deadline: Option<Instant>,
    tasks: Vec<Receipt>,
    cancelled: bool,
    panicked: bool,
    unavailable: bool,
    deadline_changed: Arc<tokio::sync::Notify>,
}

impl State {
    fn compact(&mut self) {
        self.tasks
            .retain(|receipt| match receipt.clone().now_or_never() {
                Some(JoinOutcome::Joined) => false,
                Some(JoinOutcome::Cancelled) => {
                    self.cancelled = true;
                    false
                }
                Some(JoinOutcome::Panicked) => {
                    self.panicked = true;
                    false
                }
                Some(JoinOutcome::Unavailable) => {
                    self.unavailable = true;
                    false
                }
                None => true,
            });
    }
}

#[derive(Clone, Default)]
pub struct PluginTaskRegistry {
    state: Arc<Mutex<State>>,
}

/// Cooperative stop for an OS-thread plugin worker, derived from the
/// registry's latched shutdown deadline (the host's owner deadline, clamped
/// by its stage cap). Workers check it between steps and clamp each step's
/// own timeout to the remaining budget; at expiry they stop scheduling work
/// and return, so the retained join completes instead of pinning the
/// blocking pool and `Runtime::drop`.
#[derive(Clone, Default)]
pub struct WorkerStop {
    state: Option<Arc<Mutex<State>>>,
}

impl WorkerStop {
    pub(crate) fn is_managed(&self) -> bool {
        self.state.is_some()
    }

    /// A joined worker cannot hide an IO/identity/pipe custody failure even if
    /// an intermediate caller logs its ordinary Result and continues.
    pub(crate) fn mark_unavailable(&self) {
        if let Some(state) = &self.state {
            state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .unavailable = true;
        }
    }

    /// Bound an owned read step even when shutdown first latches a deadline
    /// after IO begins. No elapsed deadline polls a new step or extends its cap.
    pub(crate) async fn run_until<T>(
        &self,
        cap: std::time::Duration,
        future: impl Future<Output = T>,
    ) -> Option<T> {
        let cap_deadline = Instant::now() + cap;
        let Some(state) = self.state.as_ref() else {
            return tokio::time::timeout_at(cap_deadline, future).await.ok();
        };
        let changed = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .deadline_changed
            .clone();
        tokio::pin!(future);
        loop {
            let notification = changed.notified();
            tokio::pin!(notification);
            notification.as_mut().enable();
            let deadline = self
                .deadline()
                .map_or(cap_deadline, |owner| owner.min(cap_deadline));
            if Instant::now() >= deadline {
                return None;
            }
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline) => return None,
                _ = &mut notification => {},
                result = &mut future => return Some(result),
            }
        }
    }

    /// No owner: never stops (tests and callers without a registry).
    pub fn never() -> Self {
        Self::default()
    }

    fn deadline(&self) -> Option<Instant> {
        let state = self.state.as_ref()?;
        // A poisoned registry still records its deadline.
        let state = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.deadline
    }

    /// True once the latched deadline has passed.
    pub fn is_stopped(&self) -> bool {
        self.deadline()
            .is_some_and(|deadline| Instant::now() >= deadline)
    }

    /// `min(cap, remaining)`; zero once stopped. Never extends the deadline.
    pub fn step_timeout(&self, cap: std::time::Duration) -> std::time::Duration {
        match self.deadline() {
            Some(deadline) => cap.min(deadline.saturating_duration_since(Instant::now())),
            None => cap,
        }
    }
}

impl PluginTaskRegistry {
    /// The stop signal for workers admitted to this registry.
    pub fn worker_stop(&self) -> WorkerStop {
        WorkerStop {
            state: Some(Arc::clone(&self.state)),
        }
    }

    pub fn close(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.closed = true;
        }
    }

    pub(crate) fn spawn(
        &self,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> Result<Receipt, ()> {
        let runtime = Handle::try_current().map_err(|_| ())?;
        self.spawn_on(&runtime, future)
    }

    /// Spawn on an explicitly retained runtime. This is required when the
    /// caller is an OS thread: `tokio::runtime::Handle::try_current()` cannot
    /// recover an ambient runtime there, but the runtime that owns the
    /// registry can still admit and retain the task's join receipt.
    pub(crate) fn spawn_on(
        &self,
        runtime: &Handle,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> Result<Receipt, ()> {
        self.spawn_with(future, |future| runtime.spawn(future))
    }

    fn spawn_with<F, S>(&self, future: F, spawn: S) -> Result<Receipt, ()>
    where
        F: Future<Output = ()> + Send + 'static,
        S: FnOnce(F) -> tokio::task::JoinHandle<()>,
    {
        let mut state = self.state.lock().map_err(|_| ())?;
        if state.closed {
            return Err(());
        }
        state.compact();
        let task = spawn(future);
        let receipt = async move {
            match task.await {
                Ok(()) => JoinOutcome::Joined,
                Err(error) if error.is_cancelled() => JoinOutcome::Cancelled,
                Err(_) => JoinOutcome::Panicked,
            }
        }
        .boxed()
        .shared();
        state.tasks.push(receipt.clone());
        Ok(receipt)
    }

    pub(crate) fn spawn_thread<F>(&self, name: &'static str, thread: F) -> Result<Receipt, ()>
    where
        F: FnOnce() + Send + 'static,
    {
        // Runtime and registry admission precede even creation of the OS
        // thread. The retained blocking original creates AND joins it; no
        // abortable async wrapper can drop a started thread's JoinHandle.
        let runtime = Handle::try_current().map_err(|_| ())?;
        let mut state = self.state.lock().map_err(|_| ())?;
        if state.closed {
            return Err(());
        }
        state.compact();
        let task = runtime.spawn_blocking(move || {
            match std::thread::Builder::new()
                .name(name.to_owned())
                .spawn(thread)
            {
                Ok(handle) => match handle.join() {
                    Ok(()) => JoinOutcome::Joined,
                    Err(_) => JoinOutcome::Panicked,
                },
                Err(_) => JoinOutcome::Unavailable,
            }
        });
        let receipt = async move {
            match task.await {
                Ok(outcome) => outcome,
                Err(error) if error.is_cancelled() => JoinOutcome::Cancelled,
                Err(_) => JoinOutcome::Panicked,
            }
        }
        .boxed()
        .shared();
        state.tasks.push(receipt.clone());
        Ok(receipt)
    }

    pub async fn shutdown_until(&self, deadline: Instant) -> PluginTaskDrain {
        let (deadline, tasks) = {
            let Ok(mut state) = self.state.lock() else {
                return PluginTaskDrain {
                    unavailable: true,
                    ..Default::default()
                };
            };
            state.closed = true;
            let first_deadline = state.deadline.is_none();
            let deadline = *state.deadline.get_or_insert(deadline);
            if first_deadline {
                state.deadline_changed.notify_waiters();
            }
            state.compact();
            (deadline, state.tasks.clone())
        };
        if !tasks.is_empty() && Instant::now() < deadline {
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline) => {},
                _ = futures::future::join_all(tasks) => {},
            }
        }
        let Ok(mut state) = self.state.lock() else {
            return PluginTaskDrain {
                unavailable: true,
                ..Default::default()
            };
        };
        state.compact();
        PluginTaskDrain {
            terminal: state.tasks.is_empty(),
            cancelled: state.cancelled,
            panicked: state.panicked,
            unavailable: state.unavailable,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use tokio::time::Duration;

    #[tokio::test(flavor = "current_thread")]
    async fn close_rejects_late_task_birth_without_starting_it() {
        let registry = PluginTaskRegistry::default();
        registry.close();
        let started = Arc::new(AtomicBool::new(false));
        let started_for_task = Arc::clone(&started);
        assert!(
            registry
                .spawn(async move {
                    started_for_task.store(true, Ordering::SeqCst);
                })
                .is_err()
        );
        assert!(
            registry
                .spawn_thread("rejected-plugin-worker", || {})
                .is_err()
        );
        assert!(!started.load(Ordering::SeqCst));
        let report = registry
            .shutdown_until(Instant::now() + Duration::from_millis(20))
            .await;
        assert!(report.is_clean());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn timed_out_task_is_retained_and_replayed_until_joined() {
        let registry = PluginTaskRegistry::default();
        let (release, wait) = tokio::sync::oneshot::channel::<()>();
        drop(
            registry
                .spawn(async move {
                    let _ = wait.await;
                })
                .expect("task admission"),
        );

        let first = registry
            .shutdown_until(Instant::now() + Duration::from_millis(5))
            .await;
        assert!(!first.terminal);
        assert!(!first.is_clean());

        release.send(()).expect("release pending task");
        tokio::task::yield_now().await;
        let second = registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(second.is_clean());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn panic_is_terminal_but_not_clean() {
        let registry = PluginTaskRegistry::default();
        drop(
            registry
                .spawn(async {
                    panic!("plugin worker test panic");
                })
                .expect("task admission"),
        );
        let report = registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(report.terminal);
        assert!(report.panicked);
        assert!(!report.is_clean());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn os_thread_join_is_observed_before_clean_report() {
        let registry = PluginTaskRegistry::default();
        let finished = Arc::new(AtomicBool::new(false));
        let finished_for_thread = Arc::clone(&finished);
        drop(
            registry
                .spawn_thread("observed-plugin-worker", move || {
                    finished_for_thread.store(true, Ordering::SeqCst);
                })
                .expect("thread admission"),
        );
        let report = registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(report.is_clean());
        assert!(finished.load(Ordering::SeqCst));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn os_thread_receives_its_attribution_name() {
        let registry = PluginTaskRegistry::default();
        let (name_tx, name_rx) = std::sync::mpsc::channel();
        drop(
            registry
                .spawn_thread("plugins-worker-attribution-test", move || {
                    name_tx
                        .send(std::thread::current().name().map(str::to_owned))
                        .expect("thread name receiver should remain live");
                })
                .expect("thread admission"),
        );

        assert_eq!(
            name_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("worker should report its name"),
            Some("plugins-worker-attribution-test".to_owned())
        );
        assert!(
            registry
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await
                .is_clean()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn explicit_runtime_spawn_is_usable_from_an_os_thread() {
        let registry = PluginTaskRegistry::default();
        let runtime = Handle::current();
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
        let thread_registry = registry.clone();
        let thread = std::thread::spawn(move || {
            // The registry retains the join receipt; this caller observes
            // completion through the signal below.
            drop(
                thread_registry
                    .spawn_on(&runtime, async move {
                        let _ = finished_tx.send(());
                    })
                    .expect("explicit runtime should admit the task"),
            );
        });
        thread.join().expect("OS callback thread should return");
        finished_rx
            .await
            .expect("task should run on supplied runtime");
        let report = registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(report.is_clean());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stalled_worker_is_incomplete_at_the_bounded_deadline() {
        let registry = PluginTaskRegistry::default();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        // A fake non-cooperative (a) worker: blocked, like an in-flight syscall.
        drop(
            registry
                .spawn_thread("fake-stalled-plugin-worker", move || {
                    let _ = release_rx.recv();
                })
                .expect("worker admission"),
        );

        let started = std::time::Instant::now();
        let report = registry
            .shutdown_until(Instant::now() + Duration::from_millis(100))
            .await;
        // Bounded drain; the join is reported incomplete, never clean.
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(!report.terminal);
        assert!(!report.is_clean());
        // Sticky: a later observation with a new deadline keeps the original.
        let replay = registry
            .shutdown_until(Instant::now() + Duration::from_secs(60))
            .await;
        assert!(!replay.terminal);

        drop(release_tx);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn worker_stop_clamps_steps_to_the_latched_deadline() {
        let registry = PluginTaskRegistry::default();
        let stop = registry.worker_stop();
        let cap = Duration::from_secs(30);
        let budget = Duration::from_millis(20);
        let one_ms = Duration::from_millis(1);
        assert!(!stop.is_stopped());
        assert_eq!(stop.step_timeout(cap), cap);

        // An empty registry drains immediately, so the paused clock has not
        // moved: the deadline is latched but not yet reached.
        let report = registry.shutdown_until(Instant::now() + budget).await;
        assert!(report.is_clean());
        assert!(!stop.is_stopped());
        assert_eq!(stop.step_timeout(cap), budget);
        assert_eq!(stop.step_timeout(one_ms), one_ms);

        // Latched: a later shutdown with a longer deadline never extends it.
        registry
            .shutdown_until(Instant::now() + Duration::from_secs(60))
            .await;
        assert_eq!(stop.step_timeout(cap), budget);

        // One millisecond before the deadline: still running, budget clamped.
        tokio::time::advance(budget - one_ms).await;
        assert!(!stop.is_stopped());
        assert_eq!(stop.step_timeout(cap), one_ms);

        // Exactly at the deadline: stopped, and no step budget remains.
        tokio::time::advance(one_ms).await;
        assert!(stop.is_stopped());
        assert_eq!(stop.step_timeout(cap), Duration::ZERO);
        assert_eq!(WorkerStop::never().step_timeout(cap), cap);
    }

    /// A fake curated-sync worker on its own OS thread. Each step waits for
    /// the test's grant, checks the stop as the real worker does between
    /// git/HTTP steps, and reports what it observed. The worker enters the
    /// runtime context so `WorkerStop` reads the test's paused clock.
    struct GatedWorker {
        step: std::sync::mpsc::Sender<()>,
        observed: tokio::sync::mpsc::UnboundedReceiver<bool>,
        receipt: Receipt,
        steps: Arc<std::sync::atomic::AtomicUsize>,
    }

    fn spawn_gated_worker(registry: &PluginTaskRegistry) -> GatedWorker {
        let clock = Handle::current();
        let stop = registry.worker_stop();
        let (step, step_rx) = std::sync::mpsc::channel::<()>();
        let (observed_tx, observed) = tokio::sync::mpsc::unbounded_channel();
        let steps = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let worker_steps = Arc::clone(&steps);
        let receipt = registry
            .spawn_thread("fake-curated-sync", move || {
                let _clock = clock.enter();
                // A closed gate means the worker's own work is complete.
                while step_rx.recv().is_ok() {
                    let stopped = stop.is_stopped();
                    if !stopped {
                        worker_steps.fetch_add(1, Ordering::SeqCst);
                    }
                    let _ = observed_tx.send(stopped);
                    if stopped {
                        return;
                    }
                }
            })
            .expect("worker admission");
        GatedWorker {
            step,
            observed,
            receipt,
            steps,
        }
    }

    impl GatedWorker {
        async fn run_step(&mut self) -> bool {
            self.step.send(()).expect("worker is waiting for a step");
            self.observed.recv().await.expect("worker reports its step")
        }
    }

    #[test]
    fn cooperatively_stopped_worker_no_longer_pins_runtime_drop() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .expect("runtime");
        let registry = PluginTaskRegistry::default();
        let budget = Duration::from_millis(200);
        let one_ms = Duration::from_millis(1);
        runtime.block_on(async {
            let mut worker = spawn_gated_worker(&registry);
            assert!(!worker.run_step().await);
            assert!(!worker.run_step().await);

            // The worker is parked between steps, unjoined, for the whole
            // budget: the drain waits up to, and not past, the deadline.
            let deadline = Instant::now() + budget;
            let drain = registry.shutdown_until(deadline);
            tokio::pin!(drain);
            assert!((&mut drain).now_or_never().is_none());
            tokio::time::advance(budget - one_ms).await;
            assert!((&mut drain).now_or_never().is_none());
            assert!(!registry.worker_stop().is_stopped());
            tokio::time::advance(one_ms).await;
            let report = drain.await;
            assert_eq!(Instant::now(), deadline);
            assert!(!report.terminal);
            assert!(!report.is_clean(), "stopped at the deadline, not completed");

            // The worker's next stop check observes expiry and it returns
            // without doing another unit of work.
            assert!(worker.run_step().await, "worker observes the stop");
            assert_eq!(worker.steps.load(Ordering::SeqCst), 2);
            assert_eq!(worker.receipt.await, JoinOutcome::Joined);

            // The retained join is now observed; the latched deadline is
            // replayed rather than extended.
            let replay = registry
                .shutdown_until(Instant::now() + Duration::from_secs(60))
                .await;
            assert!(replay.is_clean());
            assert_eq!(Instant::now(), deadline);
        });

        // The worker returned, so the retained spawn_blocking join finished
        // and the runtime drop does not wait on a still-running worker.
        let dropped = std::time::Instant::now();
        drop(runtime);
        assert!(dropped.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn worker_joined_before_the_deadline_is_clean() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .expect("runtime");
        let registry = PluginTaskRegistry::default();
        runtime.block_on(async {
            let mut worker = spawn_gated_worker(&registry);
            assert!(!worker.run_step().await);

            let deadline = Instant::now() + Duration::from_millis(200);
            let drain = registry.shutdown_until(deadline);
            tokio::pin!(drain);
            assert!((&mut drain).now_or_never().is_none());
            // The worker finishes its own work while the drain waits. The
            // clock is never advanced, so the join precedes the deadline.
            drop(worker.step);
            let report = drain.await;
            assert!(Instant::now() < deadline);
            assert!(report.is_clean());
            assert!(!registry.worker_stop().is_stopped());
            assert_eq!(worker.steps.load(Ordering::SeqCst), 1);
            assert_eq!(worker.receipt.await, JoinOutcome::Joined);
        });
        drop(runtime);
    }

    #[test]
    fn missing_runtime_refuses_before_async_or_os_thread_birth() {
        let registry = PluginTaskRegistry::default();
        let started = Arc::new(AtomicBool::new(false));
        let thread_started = started.clone();
        assert!(
            registry
                .spawn_thread("must-not-start", move || {
                    thread_started.store(true, Ordering::SeqCst);
                })
                .is_err()
        );
        let async_started = started.clone();
        assert!(
            registry
                .spawn(async move {
                    async_started.store(true, Ordering::SeqCst);
                })
                .is_err()
        );
        assert!(!started.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn named_os_thread_panic_is_not_clean_wrapper_success() {
        let registry = PluginTaskRegistry::default();
        drop(
            registry
                .spawn_thread("panicking-plugin-original", || {
                    panic!("synthetic plugin OS original panic");
                })
                .unwrap(),
        );
        let report = registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(report.terminal && report.panicked && !report.is_clean());
        assert!(registry.shutdown_until(Instant::now()).await.panicked);
    }

    #[tokio::test]
    async fn cancelled_os_thread_observer_keeps_actual_original_join() {
        let registry = PluginTaskRegistry::default();
        let (started, entered) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let receipt = registry
            .spawn_thread("observed-plugin-original", move || {
                let _ = started.send(());
                released.recv().expect("positive original release");
            })
            .unwrap();
        entered.await.unwrap();
        let observer = tokio::spawn(receipt);
        observer.abort();
        assert!(observer.await.unwrap_err().is_cancelled());
        let deadline = Instant::now();
        assert!(!registry.shutdown_until(deadline).await.terminal);
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let report = registry
                    .shutdown_until(Instant::now() + Duration::from_secs(30))
                    .await;
                if report.terminal {
                    assert!(report.is_clean());
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn in_flight_step_observes_a_deadline_latched_after_its_first_poll() {
        let registry = PluginTaskRegistry::default();
        let stop = registry.worker_stop();
        let step = stop.run_until(Duration::from_secs(30), std::future::pending::<()>());
        tokio::pin!(step);
        assert!(futures::poll!(step.as_mut()).is_pending());
        let deadline = Instant::now() + Duration::from_secs(1);
        assert!(registry.shutdown_until(deadline).await.is_clean());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(step.await, None);
        assert_eq!(Instant::now(), deadline);
    }

    #[tokio::test]
    async fn expired_owner_does_not_poll_a_new_budgeted_step() {
        let registry = PluginTaskRegistry::default();
        let stop = registry.worker_stop();
        assert!(registry.shutdown_until(Instant::now()).await.is_clean());
        let polled = AtomicBool::new(false);
        assert_eq!(
            stop.run_until(Duration::from_secs(30), async {
                polled.store(true, Ordering::SeqCst);
            })
            .await,
            None
        );
        assert!(!polled.load(Ordering::SeqCst));
    }
}
