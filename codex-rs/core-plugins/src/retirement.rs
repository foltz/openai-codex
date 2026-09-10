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
                None => true,
            });
    }
}

#[derive(Clone, Default)]
pub struct PluginTaskRegistry {
    state: Arc<Mutex<State>>,
}

impl PluginTaskRegistry {
    pub fn close(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.closed = true;
        }
    }

    pub(crate) fn spawn(
        &self,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> Result<Receipt, ()> {
        self.spawn_with(future, tokio::spawn)
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
        // Register the wrapper before starting the OS thread. Otherwise a
        // close racing this call could leave a started thread with its only
        // JoinHandle dropped by the failed admission path.
        let mut state = self.state.lock().map_err(|_| ())?;
        if state.closed {
            return Err(());
        }
        state.compact();
        let handle = std::thread::Builder::new()
            .name(name.to_owned())
            .spawn(thread)
            .map_err(|_| ())?;
        let task = tokio::spawn(async move {
            let result = tokio::task::spawn_blocking(move || handle.join()).await;
            if !matches!(result, Ok(Ok(()))) {
                // The wrapper itself is the retained receipt. A panic in the
                // OS thread is terminal but diagnostic-worthy, never clean.
                panic!("plugin worker thread failed");
            }
        });
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

    pub async fn shutdown_until(&self, deadline: Instant) -> PluginTaskDrain {
        let (deadline, tasks) = {
            let Ok(mut state) = self.state.lock() else {
                return PluginTaskDrain {
                    unavailable: true,
                    ..Default::default()
                };
            };
            state.closed = true;
            let deadline = *state.deadline.get_or_insert(deadline);
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
            unavailable: false,
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
            let _ = thread_registry
                .spawn_on(&runtime, async move {
                    let _ = finished_tx.send(());
                })
                .expect("explicit runtime should admit the task");
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
}
