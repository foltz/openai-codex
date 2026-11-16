//! Fenced ownership of classifier background tasks, not idle account work.
use codex_guardian_reviewer::ReviewerTasks;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use tokio::task::JoinHandle;

#[derive(Default)]
pub(super) struct ClassifierTasks {
    closed: Mutex<bool>,
    runtime: ReviewerTasks,
}

// Field order is deliberate: all task payloads are destroyed before the
// terminal tracker token, including unpolled futures and Tokio aborts.
pub(super) struct Tracked<F> {
    future: Pin<Box<F>>,
    _terminal: Box<dyn Send>,
}
impl<F: Future> Future for Tracked<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().future.as_mut().poll(cx)
    }
}
// Reserved while admission is open, before transport I/O. Cleanup may use it
// after closure without admitting a new task or releasing the original token.
pub(super) struct Continuation {
    terminal: Box<dyn Send + Sync>,
}
impl Continuation {
    pub(super) fn track<F: Future + Send + 'static>(self, future: F) -> Tracked<F> {
        Tracked {
            future: Box::pin(future),
            _terminal: self.terminal,
        }
    }
}
impl ClassifierTasks {
    pub(super) fn continuation(&self) -> Option<Continuation> {
        let closed = self.closed.lock().ok()?;
        if *closed {
            return None;
        }
        Some(Continuation {
            terminal: Box::new(self.runtime.tasks.token()),
        })
    }

    pub(super) fn track<F: Future + Send + 'static>(&self, future: F) -> Option<Tracked<F>> {
        self.continuation()
            .map(|continuation| continuation.track(future))
    }

    pub(super) fn spawn<F: Future<Output = ()> + Send + 'static>(
        &self,
        future: F,
    ) -> Option<JoinHandle<()>> {
        let cancel = self.runtime.cancellation.clone();
        self.track(async move {
            tokio::select! { biased; _ = cancel.cancelled() => {}, () = future => {} }
        })
        .map(tokio::spawn)
    }

    pub(super) async fn cancelled(&self) {
        self.runtime.cancellation.cancelled().await;
    }

    pub(super) fn with_open<T>(&self, action: impl FnOnce() -> T) -> Option<T> {
        let closed = self.closed.lock().ok()?;
        if *closed {
            return None;
        }
        Some(action())
    }

    pub(super) async fn shutdown(&self) {
        {
            if let Ok(mut closed) = self.closed.lock() {
                *closed = true;
            }
            // A poisoned fence already refuses every registration.
            self.runtime.cancellation.cancel();
            self.runtime.tasks.close();
        }
        self.runtime.tasks.wait().await;
    }
}

#[cfg(test)]
#[path = "tasks_tests.rs"]
mod tests;
