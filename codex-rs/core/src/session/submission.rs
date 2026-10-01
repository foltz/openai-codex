//! Core-owned queue metadata; forwarding transfers the residency guard with the operation.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Poll;

use async_channel::Sender;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::W3cTraceContext;
use tokio::sync::OwnedRwLockReadGuard;
use tokio_util::sync::CancellationToken;

#[cfg(test)]
#[path = "submission_tests.rs"]
mod tests;

#[derive(Debug)]
#[allow(dead_code, reason = "Turn ancestry is retained in Debug diagnostics.")]
pub(crate) struct Submission {
    pub id: String,
    pub op: Op,
    /// Optional W3C trace carrier propagated across async submission handoffs.
    pub trace: Option<W3cTraceContext>,
    pub parent_turn_id: Option<String>,
    pub root_turn_id: Option<String>,
    /// Keeps a V2 recipient resident until this submission is handled or dropped.
    pub residency_guard: Option<OwnedRwLockReadGuard<()>>,
}

#[derive(Clone)]
pub(crate) struct SubmissionSender {
    sender: Sender<Submission>,
    closed: Arc<Mutex<Admission>>,
    wake: CancellationToken,
}

#[derive(Default)]
struct Admission {
    closed: bool,
    failed: bool,
    outstanding: usize,
    // The single dispatch consumer follows channel FIFO. Successful sends
    // append under the same gate that dispatch takes, including private sends;
    // caller-controlled IDs are not accounting identities.
    queued: VecDeque<SubmissionKind>,
}

#[derive(Clone, Copy)]
enum SubmissionKind {
    Public,
    Shutdown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IdleAdmissionError {
    Busy,
    Outstanding,
    Unavailable,
}

#[derive(Clone)]
pub(crate) struct SubmissionDispatch(Arc<Mutex<Admission>>);

pub(crate) struct DispatchGuard {
    admission: Arc<Mutex<Admission>>,
    kind: Option<SubmissionKind>,
}

impl SubmissionDispatch {
    pub(crate) fn begin(&self) -> DispatchGuard {
        let kind = self.0.lock().ok().and_then(|mut admission| {
            let kind = admission.queued.pop_front();
            if kind.is_none() {
                admission.failed = true;
            }
            kind
        });
        DispatchGuard {
            admission: Arc::clone(&self.0),
            kind,
        }
    }
}

impl Drop for DispatchGuard {
    fn drop(&mut self) {
        if let Ok(mut admission) = self.admission.lock()
            && matches!(self.kind, Some(SubmissionKind::Public))
        {
            match admission.outstanding.checked_sub(1) {
                Some(count) => admission.outstanding = count,
                None => admission.failed = true,
            }
        }
    }
}

impl From<Sender<Submission>> for SubmissionSender {
    fn from(sender: Sender<Submission>) -> Self {
        Self {
            sender,
            closed: Arc::new(Mutex::new(Admission::default())),
            wake: CancellationToken::new(),
        }
    }
}

impl SubmissionSender {
    pub(crate) fn dispatch_control(&self) -> SubmissionDispatch {
        SubmissionDispatch(Arc::clone(&self.closed))
    }

    pub(crate) fn with_idle_admission<T, E>(
        &self,
        commit: impl FnOnce() -> Result<T, E>,
    ) -> Result<Result<T, E>, IdleAdmissionError> {
        let mut admission = self.closed.try_lock().map_err(|error| match error {
            std::sync::TryLockError::WouldBlock => IdleAdmissionError::Busy,
            std::sync::TryLockError::Poisoned(_) => IdleAdmissionError::Unavailable,
        })?;
        if admission.failed {
            return Err(IdleAdmissionError::Unavailable);
        }
        if admission.outstanding != 0 {
            return Err(IdleAdmissionError::Outstanding);
        }
        let result = commit();
        if result.is_ok() {
            admission.closed = true;
            self.wake.cancel();
        }
        Ok(result)
    }

    pub(crate) fn close_admission(&self) {
        // Poison refuses all later sends, including privileged ones.
        if let Ok(mut closed) = self.closed.lock() {
            closed.closed = true;
        }
        self.wake.cancel();
    }

    pub(crate) fn close(&self) {
        self.close_admission();
        self.sender.close();
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.wake.is_cancelled() || self.sender.is_closed()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.sender.len()
    }

    pub(crate) async fn send(&self, submission: Submission) -> Result<(), ()> {
        let send = self.sender.send(submission);
        tokio::pin!(send);
        tokio::select! {
            biased;
            _ = self.wake.cancelled() => Err(()),
            result = std::future::poll_fn(|cx| {
                let Ok(mut closed) = self.closed.lock() else { return Poll::Ready(Err(())); };
                if closed.closed || closed.failed { return Poll::Ready(Err(())); }
                // Never retain the guard across Pending. Closure and enqueue
                // therefore have exactly one ordering even for a full channel.
                match send.as_mut().poll(cx) {
                    Poll::Ready(Ok(())) => {
                        closed.outstanding += 1;
                        closed.queued.push_back(SubmissionKind::Public);
                        Poll::Ready(Ok(()))
                    }
                    Poll::Ready(Err(_)) => Poll::Ready(Err(())),
                    Poll::Pending => Poll::Pending,
                }
            }) => result,
        }
    }

    pub(super) async fn send_shutdown(&self, submission: Submission) -> Result<(), ()> {
        let send = self.sender.send(submission);
        tokio::pin!(send);
        std::future::poll_fn(|cx| {
            let Ok(mut guard) = self.closed.lock() else {
                return Poll::Ready(Err(()));
            };
            if guard.failed {
                return Poll::Ready(Err(()));
            }
            // Only the closed decision is bypassed, never the poll mutex.
            match send.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => {
                    guard.queued.push_back(SubmissionKind::Shutdown);
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(_)) => Poll::Ready(Err(())),
                Poll::Pending => Poll::Pending,
            }
        })
        .await
    }
}
