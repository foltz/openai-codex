//! Join-only fallback for host work admitted inside a session, without a
//! CodexThread handle, notification listener, or detached observation task.

use super::Session;
use super::SessionLoopOutcome;
use codex_extension_api::TurnWorkRefused;
use codex_protocol::host_turn_work::HostTurnWork;
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
    /// Contributor isolation must not bypass the process host's admission.
    pub(crate) fn admit_turn_work(
        &self,
        termination: BoxFuture<'static, ()>,
    ) -> Result<Option<Box<dyn HostTurnWork>>, TurnWorkRefused> {
        match &self.services.host_admission {
            Some(host) => host.admit_turn_work(&self.services.thread_extension_data, termination),
            None => Ok(None),
        }
    }

    pub(crate) fn finish_isolated_turn_work(&self, turn_id: &str) {
        // Non-isolated sessions already forward through their host lifecycle
        // contributor. Isolated sessions have no such contributor, and a
        // public resumed turn may have no owner forwarding its terminal event.
        if self.services.extensions.host_admission().is_none()
            && let Some(host) = &self.services.host_admission
        {
            // Logical terminal only, not evidence of physical task cleanup.
            host.turn_work_terminal(&self.services.thread_extension_data, turn_id);
        }
    }

    pub(crate) fn turn_work_termination(&self) -> BoxFuture<'static, ()> {
        let receipt = self
            .services
            .thread_extension_data
            .get::<SessionLoopWorkReceipt>();
        Box::pin(async move {
            match receipt {
                Some(receipt) => {
                    receipt.0.clone().await;
                }
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
