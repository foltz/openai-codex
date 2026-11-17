//! Retained EMA request authority and its original response-quiescence wait.
//! This observes the real authority lock; it is not a watcher join or login proof.

use codex_rmcp_client::EmaRequestAuthority;
use futures::FutureExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeEmaOutcome {
    Quiesced,
    Panicked,
    Unobserved,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeEmaDrainResult {
    pub id: u64,
    pub revoked: bool,
    pub outcome: RuntimeEmaOutcome,
}

pub(crate) struct EmaRetirementOwner {
    authority: EmaRequestAuthority,
    completion: OnceLock<Shared<BoxFuture<'static, RuntimeEmaOutcome>>>,
}

impl EmaRetirementOwner {
    pub(crate) fn new(authority: EmaRequestAuthority) -> Arc<Self> {
        Arc::new(Self {
            authority,
            completion: OnceLock::new(),
        })
    }

    pub(crate) fn revoke(&self) {
        self.authority.revoke();
    }

    pub(crate) async fn observe_until(&self, id: u64, deadline: Instant) -> RuntimeEmaDrainResult {
        let completion = self.completion.get_or_init(|| {
            let authority = self.authority.clone();
            AssertUnwindSafe(async move {
                authority.quiesce().await;
            })
            .catch_unwind()
            .map(|result| {
                if result.is_ok() {
                    RuntimeEmaOutcome::Quiesced
                } else {
                    RuntimeEmaOutcome::Panicked
                }
            })
            .boxed()
            .shared()
        });
        let outcome = if let Some(outcome) = completion.peek() {
            *outcome
        } else if Instant::now() >= deadline {
            RuntimeEmaOutcome::Unobserved
        } else {
            tokio::time::timeout_at(deadline, completion.clone())
                .await
                .unwrap_or(RuntimeEmaOutcome::Unobserved)
        };
        RuntimeEmaDrainResult {
            id,
            revoked: self.authority.is_revoked(),
            outcome,
        }
    }
}

#[cfg(test)]
#[path = "ema_retirement_tests.rs"]
mod tests;
