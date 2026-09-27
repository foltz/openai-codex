//! Cancellation of mailbox preparation polled alongside submission dispatch.

use std::sync::Arc;
use std::sync::Mutex;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub(crate) struct MailboxPreparationSlot {
    current: Arc<Mutex<Option<Arc<Preparation>>>>,
}

struct Preparation {
    cancellation: CancellationToken,
}

pub(super) struct MailboxPreparation {
    slot: Arc<Mutex<Option<Arc<Preparation>>>>,
    preparation: Arc<Preparation>,
}

impl MailboxPreparationSlot {
    pub(super) fn register(&self) -> MailboxPreparation {
        let preparation = Arc::new(Preparation {
            cancellation: CancellationToken::new(),
        });
        *self.current.lock().expect("mailbox preparation poisoned") = Some(Arc::clone(&preparation));
        MailboxPreparation {
            slot: Arc::clone(&self.current),
            preparation,
        }
    }

    pub(super) fn cancel_for_replacement(&self) {
        let current = self.current.lock().expect("mailbox preparation poisoned");
        if let Some(preparation) = current.as_ref() {
            // The submission loop owns preparation. External completion and
            // interruption producers wake that loop instead of preparing here.
            preparation.cancellation.cancel();
        }
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
        let mut current = self.slot.lock().expect("mailbox preparation poisoned");
        if current.as_ref().is_some_and(|current| Arc::ptr_eq(current, &self.preparation)) {
            current.take();
        }
    }
}
