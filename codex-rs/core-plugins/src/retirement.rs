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
        let mut state = self.state.lock().map_err(|_| ())?;
        if state.closed {
            return Err(());
        }
        state.compact();
        let task = tokio::spawn(future);
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

    pub(crate) fn spawn_thread<F>(&self, thread: F) -> Result<Receipt, ()>
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
        let handle = std::thread::Builder::new().spawn(thread).map_err(|_| ())?;
        let task = tokio::spawn(async move {
            let result = tokio::task::spawn_blocking(move || handle.join()).await;
            if result.is_err() || result.expect("checked above").is_err() {
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
    use std::sync::atomic::{AtomicBool, Ordering};
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
        assert!(registry.spawn_thread(|| {}).is_err());
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
        let _ = registry
            .spawn(async move {
                let _ = wait.await;
            })
            .expect("task admission");

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
        let _ = registry
            .spawn(async {
                panic!("plugin worker test panic");
            })
            .expect("task admission");
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
        let _ = registry
            .spawn_thread(move || {
                finished_for_thread.store(true, Ordering::SeqCst);
            })
            .expect("thread admission");
        let report = registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(report.is_clean());
        assert!(finished.load(Ordering::SeqCst));
    }
}
