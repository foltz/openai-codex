use std::sync::Arc;
use std::time::Duration;

use codex_models_manager::manager::RefreshStrategy;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::model_catalog::ModelCatalog;

const MODELS_REFRESH_INTERVAL: Duration = Duration::from_secs(4 * 60 + 30);

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

pub(crate) fn spawn(model_catalog: &Arc<ModelCatalog>) -> ModelsRefreshWorker {
    spawn_with_interval(model_catalog, MODELS_REFRESH_INTERVAL)
}

fn spawn_with_interval(
    model_catalog: &Arc<ModelCatalog>,
    refresh_interval: Duration,
) -> ModelsRefreshWorker {
    let model_catalog = Arc::downgrade(model_catalog);
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let task = tokio::spawn(async move {
        loop {
            if worker_shutdown.is_cancelled() {
                break;
            }
            let Some(model_catalog) = model_catalog.upgrade() else {
                break;
            };
            if let Err(err) = model_catalog.list_models(RefreshStrategy::Online).await {
                // Parser diagnostics can include provider credentials from the source TOML.
                tracing::warn!(error_kind = ?err.kind(), "model catalog refresh blocked by provider requirements");
            }
            drop(model_catalog);

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
