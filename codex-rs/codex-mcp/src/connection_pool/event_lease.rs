//! A private event transport has one ordinary subscription owner. It never
//! borrows release authority over a shared pool connection.

use std::future::Future;
use std::sync::Arc;

use codex_rmcp_client::RmcpClient;
use codex_rmcp_client::RmcpClientRetirement;
use tokio::runtime::Handle;
use tokio::time::Instant;

use super::McpConnectionPool;
use super::PoolWorkerKind;
use super::physical::PhysicalOwner;
use super::worker_joins::PoolWorkerJoins;

pub(crate) struct EventConnectionLease {
    owner: Option<Arc<PhysicalOwner>>,
    workers: Arc<PoolWorkerJoins>,
    runtime: Handle,
}

impl McpConnectionPool {
    /// Reserve before calling an independent subscription's transport factory.
    pub(crate) fn reserve_event_connection(&self) -> EventConnectionLease {
        EventConnectionLease {
            owner: Some(self.physical.reserve()),
            workers: Arc::clone(&self.workers),
            runtime: Handle::current(),
        }
    }

    /// Retain the actual request-cancellation worker at birth. Joining it is
    /// neither a protocol acknowledgement nor positive physical cleanup.
    pub(crate) fn spawn_event_release(
        &self,
        runtime: &Handle,
        body: impl Future<Output = ()> + Send + 'static,
    ) {
        drop(self.workers.spawn(runtime, PoolWorkerKind::Release, body));
    }
}

impl EventConnectionLease {
    pub(crate) fn retirement(&self) -> RmcpClientRetirement {
        self.owner.as_ref().expect("live event lease").lower.clone()
    }

    pub(crate) fn bind(&self, client: &RmcpClient) -> bool {
        let owner = self.owner.as_ref().expect("live event lease");
        let accepted = owner.accepts(client);
        owner.settle_startup(Some(client));
        accepted
    }

    /// Keep the existing cancellation grace, then drop the client and drive
    /// bounded physical closure. The process ledger owns the original join.
    pub(crate) fn release_after(mut self, before_close: impl Future<Output = ()> + Send + 'static) {
        let owner = self.owner.take().expect("live event lease");
        owner.mark_released();
        owner.settle_startup(None);
        drop(
            self.workers
                .spawn(&self.runtime, PoolWorkerKind::Release, async move {
                    before_close.await;
                    let _ = owner
                        .lower
                        .shutdown_within_latched(
                            Instant::now() + codex_rmcp_client::DEFAULT_RETIREMENT_TIMEOUT,
                        )
                        .await;
                }),
        );
    }
}

impl Drop for EventConnectionLease {
    fn drop(&mut self) {
        let Some(owner) = self.owner.take() else {
            return;
        };
        // Constructor cancellation/unwind fences all late lower allocations
        // synchronously, before its retained cleanup worker is polled.
        owner.mark_released();
        owner.settle_startup(None);
        drop(
            self.workers
                .spawn(&self.runtime, PoolWorkerKind::Release, async move {
                    let _ = owner
                        .lower
                        .shutdown_within_latched(
                            Instant::now() + codex_rmcp_client::DEFAULT_RETIREMENT_TIMEOUT,
                        )
                        .await;
                }),
        );
    }
}
