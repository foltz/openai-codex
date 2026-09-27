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
    pub(crate) fn is_busy(&self) -> bool {
        let state = self.state.lock().expect("mailbox preparation poisoned");
        state.current.is_some() || state.replacing > 0
    }

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

    pub(crate) fn register(&self) -> MailboxPreparation {
        let preparation = Arc::new(Preparation {
            cancellation: CancellationToken::new(),
            completed: CancellationToken::new(),
        });
        self.state.lock().expect("mailbox preparation poisoned").current = Some(Arc::clone(&preparation));
        MailboxPreparation {
            slot: Arc::clone(&self.state),
            preparation,
        }
    }

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
    fn drop(&mut self) {
        let mut state = self.slot.lock().expect("mailbox preparation poisoned");
        if state.current.as_ref().is_some_and(|current| Arc::ptr_eq(current, &self.preparation)) {
            state.current.take();
        }
        self.preparation.completed.cancel();
    }
}

impl Drop for MailboxReplacement<'_> {
    fn drop(&mut self) {
        let mut state = self.slot.state.lock().expect("mailbox preparation poisoned");
        state.replacing -= 1;
        if state.replacing == 0 {
            self.wake.notify_one();
        }
    }
}
