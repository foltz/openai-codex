//! Incarnation-local write admission and irreversible persistence closure.
use super::LiveThread;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

impl LiveThread {
    /// Positive incarnation-local proof. Old clones cannot touch a successor writer.
    pub fn is_sealed(&self) -> bool {
        self.write_gate.try_lock().is_ok_and(|sealed| *sealed)
    }

    pub(super) async fn mutation_guard(
        &self,
    ) -> ThreadStoreResult<tokio::sync::MutexGuard<'_, bool>> {
        let sealed = self.write_gate.lock().await;
        if *sealed {
            return Err(ThreadStoreError::ThreadNotFound {
                thread_id: self.thread_id,
            });
        }
        Ok(sealed)
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "the incarnation write gate must remain held until admitted persistence completes"
    )]
    pub async fn shutdown(&self) -> ThreadStoreResult<()> {
        let mut sealed = self.write_gate.lock().await;
        if *sealed {
            return Ok(());
        }
        let metadata_result = self
            .flush_pending_metadata_update_for_existing_history()
            .await;
        let shutdown_result = self.thread_store.shutdown_thread(self.thread_id).await;
        let result = match (metadata_result, shutdown_result) {
            (Err(metadata_error), Err(shutdown_error)) => Err(ThreadStoreError::Internal {
                message: format!(
                    "thread metadata update failed: {metadata_error}; thread shutdown failed: {shutdown_error}"
                ),
            }),
            (Err(metadata_error), Ok(())) => Err(metadata_error),
            (Ok(()), result) => result,
        };
        if result.is_ok() {
            *sealed = true;
        }
        result
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "the incarnation write gate must remain held until admitted persistence completes"
    )]
    pub async fn discard(&self) -> ThreadStoreResult<()> {
        let mut sealed = self.write_gate.lock().await;
        if *sealed {
            return Err(ThreadStoreError::ThreadNotFound {
                thread_id: self.thread_id,
            });
        }
        self.thread_store.discard_thread(self.thread_id).await?;
        *sealed = true;
        Ok(())
    }
}
