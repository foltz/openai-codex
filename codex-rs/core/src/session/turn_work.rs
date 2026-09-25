//! Join-only fallback for host work admitted inside a session, without a
//! CodexThread handle, notification listener, or detached observation task.

use super::Session;
use super::SessionLoopOutcome;
use futures::future::BoxFuture;
use futures::future::Shared;

/// Coalesced host-wide retry hints. The submission loop polls `wait`; no
/// watcher is spawned and no submission sender is retained. A hint never
/// carries account authority or consumes mail.
#[derive(Default)]
pub(crate) struct MailboxWorkRetry {
    pending: std::sync::Mutex<Option<BoxFuture<'static, ()>>>,
    changed: tokio::sync::Notify,
}

impl MailboxWorkRetry {
    pub(crate) fn defer(&self, retry: BoxFuture<'static, ()>) {
        *self.pending.lock().expect("mailbox retry poisoned") = Some(retry);
        self.changed.notify_one();
    }

    pub(crate) async fn wait(&self) {
        loop {
            self.changed.notified().await;
            let retry = self.pending.lock().expect("mailbox retry poisoned").take();
            if let Some(retry) = retry {
                retry.await;
                return;
            }
        }
    }
}

pub(super) struct SessionLoopWorkReceipt(pub(super) Shared<BoxFuture<'static, SessionLoopOutcome>>);

impl Session {
    pub(crate) fn turn_work_termination(&self) -> BoxFuture<'static, ()> {
        let receipt = self.services.thread_extension_data.get::<SessionLoopWorkReceipt>();
        Box::pin(async move {
            match receipt {
                Some(receipt) => { receipt.0.clone().await; }
                // A session constructed without a submission loop has no
                // positive termination evidence. Never invent a clean fallback.
                None => std::future::pending().await,
            }
        })
    }
}

#[cfg(test)]
#[path = "turn_work_tests.rs"]
mod tests;
