//! Share observation of one original refresh join; no detached reaper.
use futures::FutureExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use tokio::task::AbortHandle;
use tokio::task::JoinError;
use tokio::task::JoinHandle;

/// Fixed terminal causes; no retained backend text or opaque panic payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CloudRefreshJoinError {
    Cancelled,
    Panicked,
}
impl CloudRefreshJoinError {
    pub(crate) fn is_cancelled(self) -> bool {
        self == Self::Cancelled
    }
}

/// The home census retains a clone before the loader can escape. Observer drop
/// neither aborts the original nor discards its join. Completed observations
/// retain only the fixed result after the census releases its failure payload.
/// The shared future has dropped the original task captures.
#[derive(Clone)]
pub(crate) struct CloudRefreshTask {
    abort: AbortHandle,
    failure_payload: Arc<Mutex<Option<JoinError>>>,
    receipt: Shared<BoxFuture<'static, Result<(), CloudRefreshJoinError>>>,
}
impl CloudRefreshTask {
    pub(crate) fn new(original: JoinHandle<()>) -> Self {
        let failure_payload = Arc::new(Mutex::new(None));
        let capture = Arc::clone(&failure_payload);
        Self {
            abort: original.abort_handle(),
            failure_payload,
            receipt: async move {
                match original.await {
                    Ok(()) => Ok(()),
                    Err(error) => {
                        let kind = if error.is_cancelled() {
                            CloudRefreshJoinError::Cancelled
                        } else {
                            CloudRefreshJoinError::Panicked
                        };
                        // This shared future evaluates once. Retain the opaque
                        // payload until the home census releases its own lock.
                        // No arbitrary panic-payload Drop runs during join polling.
                        *capture
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error);
                        Err(kind)
                    }
                }
            }
            .boxed()
            .shared(),
        }
    }
    pub(crate) fn take_failure_payload(&self) -> Option<JoinError> {
        self.failure_payload
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    pub(crate) fn abort(&self) {
        self.abort.abort();
    }
    pub(crate) fn is_finished(&self) -> bool {
        self.abort.is_finished()
    }
}
impl Future for CloudRefreshTask {
    type Output = Result<(), CloudRefreshJoinError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.receipt).poll(cx)
    }
}

#[cfg(test)]
#[path = "refresh_task_tests.rs"]
mod tests;
