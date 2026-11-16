//! Adapter to the existing original-join ledger and shared account work, not another registry.
use crate::processor_task_retirement::{ProcessorTaskJoin, ProcessorTaskTicket, ProcessorTasks};
use codex_extension_api::HostOperationWork;
use codex_model_provider::{
    AuthTaskAccess, AuthTaskFailure, AuthTaskFuture, AuthTaskObservation, AuthTaskSpawner,
};
use std::future::Future;
use std::sync::Arc;

struct CatalogAuthTasks {
    ticket: ProcessorTaskTicket,
    work: Option<Arc<dyn HostOperationWork>>,
}
impl std::fmt::Debug for CatalogAuthTasks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CatalogAuthTasks")
    }
}
impl AuthTaskSpawner for CatalogAuthTasks {
    fn spawn(&self, future: AuthTaskFuture) -> Result<AuthTaskObservation, AuthTaskFailure> {
        // Derive before registration/spawn. No fresh fallback during drain or on exhaustion.
        let work = self
            .work
            .as_deref()
            .map(|work| work.derive_operation())
            .transpose()
            .map_err(|_| AuthTaskFailure::Refused)?;
        let receipt = self
            .ticket
            .spawn(async move {
                let _work = work;
                future.await;
            })
            .map_err(|_| AuthTaskFailure::Unavailable)?;
        Ok(Box::pin(async move {
            match receipt.await {
                ProcessorTaskJoin::Joined => Ok(()),
                ProcessorTaskJoin::Cancelled => Err(AuthTaskFailure::Cancelled),
                ProcessorTaskJoin::Panicked => Err(AuthTaskFailure::Panicked),
            }
        }))
    }
}

/// A finite operation holds only a weak registration ticket, never its own ledger.
pub(crate) fn spawner(
    ticket: ProcessorTaskTicket,
    work: Option<Box<dyn HostOperationWork>>,
) -> Arc<dyn AuthTaskSpawner> {
    Arc::new(CatalogAuthTasks {
        ticket,
        work: work.map(Arc::from),
    })
}

pub(crate) async fn scope<T>(
    tasks: &ProcessorTasks,
    work: Option<Box<dyn HostOperationWork>>,
    future: impl Future<Output = T>,
) -> codex_protocol::error::Result<T> {
    codex_model_provider::with_auth_task_scope(
        AuthTaskAccess::Admitted(spawner(tasks.ticket(), work)),
        future,
    )
    .await
}
