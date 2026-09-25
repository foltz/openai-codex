//! Join-only fallback for host work admitted inside a session, without a
//! CodexThread handle, notification listener, or detached observation task.

use super::Session;
use super::SessionLoopOutcome;
use futures::future::BoxFuture;
use futures::future::Shared;

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
