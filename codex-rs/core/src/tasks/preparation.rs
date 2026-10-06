//! Completion of mailbox and idle preparation across replacement tasks.

use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub(crate) struct MailboxPreparationSlot {
    state: Arc<Mutex<PreparationState>>,
}

#[derive(Default)]
struct PreparationState {
    current: Option<Arc<Preparation>>,
    replacing: usize,
    publication: Option<CancellationToken>,
}

struct Preparation {
    cancellation: CancellationToken,
    completed: CancellationToken,
}

pub(crate) struct MailboxPreparation {
    slot: Arc<Mutex<PreparationState>>,
    preparation: Arc<Preparation>,
}

pub(super) struct MailboxReplacement<'a> {
    slot: &'a MailboxPreparationSlot,
    wake: &'a Notify,
}

impl MailboxPreparationSlot {
    #[expect(
        clippy::expect_used,
        reason = "publication ordering requires unpoisoned state"
    )]
    pub(super) fn pending_publication(&self) -> Option<CancellationToken> {
        self.state
            .lock()
            .expect("mailbox preparation poisoned")
            .publication
            .as_ref()
            .filter(|ready| !ready.is_cancelled())
            .cloned()
    }

    /// Called under active_turn at installation. The witness survives an abort
    /// taking that task, so a different replacer cannot publish ahead of it.
    #[expect(
        clippy::expect_used,
        reason = "publication ordering requires unpoisoned state"
    )]
    pub(super) fn register_publication(&self, ready: CancellationToken) {
        self.state
            .lock()
            .expect("mailbox preparation poisoned")
            .publication = Some(ready);
    }

    #[expect(
        clippy::expect_used,
        reason = "poisoned preparation state cannot authorize a new start"
    )]
    pub(crate) fn is_busy(&self) -> bool {
        let state = self.state.lock().expect("mailbox preparation poisoned");
        state.current.is_some() || state.replacing > 0
    }

    #[expect(
        clippy::expect_used,
        reason = "poisoned preparation state cannot authorize a retry"
    )]
    pub(super) fn defer_if_busy(&self, wake: &Notify) -> bool {
        let state = self.state.lock().expect("mailbox preparation poisoned");
        if state.replacing > 0 {
            // The last replacement guard owns the retry wake.
            return true;
        }
        if state.current.is_some() {
            // Preserve a new trigger while the loop finishes a stale owner.
            wake.notify_one();
            return true;
        }
        false
    }

    #[expect(
        clippy::expect_used,
        reason = "poisoned preparation state cannot accept a new owner"
    )]
    pub(crate) fn register(&self) -> MailboxPreparation {
        let preparation = Arc::new(Preparation {
            cancellation: CancellationToken::new(),
            completed: CancellationToken::new(),
        });
        self.state
            .lock()
            .expect("mailbox preparation poisoned")
            .current = Some(Arc::clone(&preparation));
        MailboxPreparation {
            slot: Arc::clone(&self.state),
            preparation,
        }
    }

    #[expect(
        clippy::expect_used,
        reason = "replacement requires unpoisoned owner and witness state"
    )]
    pub(super) fn begin_replacement<'a>(
        &'a self,
        wake: &'a Notify,
    ) -> (MailboxReplacement<'a>, Option<CancellationToken>) {
        let mut state = self.state.lock().expect("mailbox preparation poisoned");
        state.replacing += 1;
        let completed = state.current.as_ref().map(|preparation| {
            preparation.cancellation.cancel();
            preparation.completed.clone()
        });
        (MailboxReplacement { slot: self, wake }, completed)
    }

    #[expect(
        clippy::expect_used,
        reason = "poisoned preparation state cannot supply a trustworthy witness"
    )]
    pub(super) fn cancel_for_replacement(&self) -> Option<CancellationToken> {
        let state = self.state.lock().expect("mailbox preparation poisoned");
        state.current.as_ref().map(|preparation| {
            preparation.cancellation.cancel();
            preparation.completed.clone()
        })
    }
}

impl MailboxPreparation {
    pub(super) async fn cancelled(&self) {
        self.preparation.cancellation.cancelled().await;
    }

    pub(super) fn was_replaced(&self) -> bool {
        self.preparation.cancellation.is_cancelled()
    }
}

impl Drop for MailboxPreparation {
    #[expect(
        clippy::expect_used,
        reason = "do not signal completion over poisoned preparation ownership"
    )]
    fn drop(&mut self) {
        let mut state = self.slot.lock().expect("mailbox preparation poisoned");
        if state
            .current
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &self.preparation))
        {
            state.current.take();
        }
        self.preparation.completed.cancel();
    }
}

impl Drop for MailboxReplacement<'_> {
    #[expect(
        clippy::expect_used,
        reason = "do not authorize a retry over a poisoned replacement count"
    )]
    fn drop(&mut self) {
        let mut state = self
            .slot
            .state
            .lock()
            .expect("mailbox preparation poisoned");
        state.replacing -= 1;
        if state.replacing == 0 {
            self.wake.notify_one();
        }
    }
}
