//! Reusable account-transition observations, not permanent manager retirement.

use super::ThreadManager;
use std::time::Duration;
use tokio::time::Instant;

impl ThreadManager {
    /// Capture the current request's authority before spawning a finite job.
    /// Absence remains ungated; exhausted derivation never acquires fresh work.
    pub fn derive_request_operation_work(
        &self,
    ) -> codex_protocol::error::Result<Option<Box<dyn codex_extension_api::HostOperationWork>>> {
        match self.state.extensions.host_admission() {
            Some(host) => host.derive_request_operation_work().map_err(|_| {
                codex_protocol::error::CodexErr::Fatal("request account work cannot admit background operation".to_owned())
            }),
            None => Ok(None),
        }
    }

    /// Drive already-admitted constructors during account draining, without
    /// closing threads or initiating retirement. Cancelling this observer does
    /// not discard the retained constructors or their operation custody.
    pub async fn drive_admitted_constructions(&self) {
        self.constructions.drive_account_work().await;
    }

    /// Retire startup resources that never reached the thread lookup map.
    /// Pending constructors or incomplete cleanup refuse reset completion;
    /// this never starts a constructor or closes this reusable manager.
    pub async fn shutdown_unpublished_constructions_bounded(&self, timeout: Duration) -> bool {
        self.constructions
            .retire_completed_startup_until(Instant::now() + timeout)
            .await
    }
}
