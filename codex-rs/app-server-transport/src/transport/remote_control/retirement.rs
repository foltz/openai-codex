//! Retained task receipts: observer cancellation never discards cleanup evidence.

use futures::FutureExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use std::sync::Mutex;
use tokio::task::JoinHandle;

pub(super) type Receipt = Shared<BoxFuture<'static, bool>>;

/// Keeps failed and pending work across reset attempts, compacting only joined successes.
#[derive(Default)]
pub(super) struct Retirement {
    receipts: Mutex<Vec<Receipt>>,
}

impl Retirement {
    pub(super) fn retain(&self, receipt: Receipt) {
        let mut receipts = self
            .receipts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        receipts.retain(|receipt| receipt.clone().now_or_never() != Some(true));
        receipts.push(receipt);
    }

    pub(super) fn track(&self, task: JoinHandle<bool>) -> Receipt {
        let receipt = async move { task.await.unwrap_or(false) }.boxed().shared();
        self.retain(receipt.clone());
        receipt
    }

    pub(super) fn snapshot(&self) -> Vec<Receipt> {
        self.receipts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(super) async fn observe(&self) -> bool {
        // Await every receipt, even if one failed. Failure remains sticky for retries.
        futures::future::join_all(self.snapshot())
            .await
            .into_iter()
            .all(|clean| clean)
    }
}

#[cfg(test)]
#[path = "retirement_tests.rs"]
mod tests;
