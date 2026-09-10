use std::sync::Arc;
use std::time::Duration;

use codex_http_client::HttpClientFactory;
use codex_models_manager::manager::RefreshStrategy;
use codex_models_manager::manager::SharedModelsManager;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const MODELS_REFRESH_INTERVAL: Duration = Duration::from_secs(3 * 60);

#[derive(Debug)]
pub(crate) struct ModelsRefreshWorker {
    shutdown: CancellationToken,
    completion: Mutex<RefreshCompletion>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ModelsRefreshShutdown {
    Joined,
    Cancelled,
    Panicked,
    TimedOut,
}

#[derive(Debug)]
enum RefreshCompletion {
    Running(JoinHandle<()>),
    Finished(ModelsRefreshShutdown),
}

impl ModelsRefreshWorker {
    pub(crate) fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// Observe the actual worker, retaining its join on timeout/cancellation.
    /// This mutex is the exclusive join poll right, not a worker dependency;
    /// the worker never needs it to finish. No timeout aborts an active fetch.
    pub(crate) async fn shutdown_until(&self, deadline: Instant) -> ModelsRefreshShutdown {
        if let Ok(completion) = self.completion.try_lock()
            && let RefreshCompletion::Finished(outcome) = &*completion
        {
            return *outcome;
        }
        if Instant::now() >= deadline {
            return ModelsRefreshShutdown::TimedOut;
        }
        self.shutdown();
        let mut completion = tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => return ModelsRefreshShutdown::TimedOut,
            completion = self.completion.lock() => completion,
        };
        let task = match &mut *completion {
            RefreshCompletion::Finished(outcome) => return *outcome,
            RefreshCompletion::Running(task) => task,
        };
        let joined = tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => return ModelsRefreshShutdown::TimedOut,
            joined = task => joined,
        };
        let outcome = match joined {
            Ok(()) => ModelsRefreshShutdown::Joined,
            Err(error) if error.is_cancelled() => ModelsRefreshShutdown::Cancelled,
            Err(_) => ModelsRefreshShutdown::Panicked,
        };
        *completion = RefreshCompletion::Finished(outcome);
        outcome
    }
}

impl Drop for ModelsRefreshWorker {
    fn drop(&mut self) {
        self.shutdown();
    }
}

pub(crate) fn spawn(
    models_manager: &SharedModelsManager,
    http_client_factory: HttpClientFactory,
) -> ModelsRefreshWorker {
    spawn_with_interval(models_manager, http_client_factory, MODELS_REFRESH_INTERVAL)
}

fn spawn_with_interval(
    models_manager: &SharedModelsManager,
    http_client_factory: HttpClientFactory,
    refresh_interval: Duration,
) -> ModelsRefreshWorker {
    let models_manager = Arc::downgrade(models_manager);
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let task = tokio::spawn(async move {
        loop {
            if worker_shutdown.is_cancelled() {
                break;
            }
            let Some(models_manager) = models_manager.upgrade() else {
                break;
            };
            models_manager
                .list_models(RefreshStrategy::Online, http_client_factory.clone())
                .await;
            drop(models_manager);

            tokio::select! {
                _ = worker_shutdown.cancelled() => break,
                _ = tokio::time::sleep(refresh_interval) => {}
            }
        }
    });
    ModelsRefreshWorker {
        shutdown,
        completion: Mutex::new(RefreshCompletion::Running(task)),
    }
}

#[cfg(test)]
#[path = "models_refresh_worker_tests.rs"]
mod tests;
