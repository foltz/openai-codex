use std::future::Future;
use std::future::pending;

use codex_app_server_transport::OwnerExpiry;
use tokio::task::JoinError;
use tokio::task::JoinSet;
use tracing::warn;

pub(crate) struct ConnectionCleanupTasks {
    // `true` when the cleanup finished. `false` when it stopped at the owner
    // expiry and handed its unpolled future back to `expiry` (cooperative
    // (b) work: no abort, no fresh budget).
    tasks: JoinSet<bool>,
    expiry: Option<OwnerExpiry>,
    failed: bool,
}

impl ConnectionCleanupTasks {
    /// Existing unbounded host path, retained until standalone owner wiring.
    pub(crate) fn new() -> Self {
        Self {
            tasks: JoinSet::new(),
            expiry: None,
            failed: false,
        }
    }

    pub(crate) fn with_expiry(expiry: OwnerExpiry) -> Self {
        Self {
            tasks: JoinSet::new(),
            expiry: Some(expiry),
            failed: false,
        }
    }

    pub(crate) fn spawn(&mut self, future: impl Future<Output = ()> + Send + 'static) {
        let expiry = self.expiry.clone();
        self.tasks.spawn(async move {
            match expiry {
                Some(expiry) => {
                    matches!(expiry.run("connection_cleanup", future).await, Ok(Some(())))
                }
                None => {
                    future.await;
                    true
                }
            }
        });
    }

    pub(crate) async fn reap_next(&mut self) {
        if self.tasks.is_empty() {
            pending::<()>().await;
        }
        if let Some(result) = self.tasks.join_next().await {
            self.failed |= !log_cleanup_result(result);
        }
    }

    /// Joins every cleanup task. Returns `true` only when all of them
    /// finished; a task that stopped at owner expiry has already handed back
    /// its owner, so this join returns without polling it further.
    pub(crate) async fn drain(&mut self) -> bool {
        let mut finished = !self.failed;
        while let Some(result) = self.tasks.join_next().await {
            finished &= log_cleanup_result(result);
        }
        self.failed |= !finished;
        finished
    }

    pub(crate) fn abort(&mut self) {
        self.tasks.abort_all();
    }
}

impl Drop for ConnectionCleanupTasks {
    fn drop(&mut self) {
        // Releasing the host's owner never aborts. `abort` remains the explicit
        // force path.
        if self.expiry.is_some() {
            self.tasks.detach_all();
        }
    }
}

fn log_cleanup_result(result: Result<bool, JoinError>) -> bool {
    match result {
        Ok(finished) => finished,
        Err(err) => {
            if !err.is_cancelled() {
                warn!("connection cleanup task failed: {err}");
            }
            false
        }
    }
}

#[cfg(test)]
#[path = "connection_cleanup_tests.rs"]
mod tests;
