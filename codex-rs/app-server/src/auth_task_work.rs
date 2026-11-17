//! Adapter to the existing original-join ledger and shared account work, not another registry.
use crate::processor_task_retirement::{ProcessorTaskJoin, ProcessorTaskTicket, ProcessorTasks};
use codex_extension_api::HostOperationWork;
use codex_model_provider::{
    AuthTaskAccess, AuthTaskBlocking, AuthTaskFailure, AuthTaskFuture, AuthTaskObservation,
    AuthTaskSpawner,
};
use std::future::Future;
use std::sync::Arc;

struct CatalogAuthTasks {
    ticket: ProcessorTaskTicket,
    work: Option<Arc<dyn HostOperationWork>>,
    require_account_work: bool,
}
impl std::fmt::Debug for CatalogAuthTasks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CatalogAuthTasks")
    }
}
impl CatalogAuthTasks {
    fn derive_work(&self) -> Result<Option<Box<dyn HostOperationWork>>, AuthTaskFailure> {
        match self.work.as_deref() {
            Some(work) => work
                .derive_operation()
                .map(Some)
                .map_err(|_| AuthTaskFailure::Refused),
            None if self.require_account_work => Err(AuthTaskFailure::Refused),
            None => Ok(None),
        }
    }
}

impl AuthTaskSpawner for CatalogAuthTasks {
    fn spawn(&self, future: AuthTaskFuture) -> Result<AuthTaskObservation, AuthTaskFailure> {
        // Derive before registration/spawn. No fresh fallback during drain or on exhaustion.
        let work = self.derive_work()?;
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
    fn spawn_blocking(
        &self,
        task: AuthTaskBlocking,
    ) -> Result<AuthTaskObservation, AuthTaskFailure> {
        let work = self.derive_work()?;
        let receipt = self
            .ticket
            .spawn_blocking(move || {
                let _work = work;
                task();
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
        require_account_work: false,
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

/// Scope a retained finite producer using only its weak registration ticket.
/// Failed descendant custody stays visible after the producer itself joins.
pub(crate) async fn scope_with_ticket<T>(
    ticket: ProcessorTaskTicket,
    work: Option<Box<dyn HostOperationWork>>,
    future: impl Future<Output = T>,
) -> codex_protocol::error::Result<T> {
    let result = codex_model_provider::with_auth_task_scope(
        AuthTaskAccess::Admitted(spawner(ticket.clone(), work)),
        future,
    )
    .await;
    if result.is_err() {
        let _ = ticket.mark_unverified();
    }
    result
}

/// Independent RPCs may execute pure handlers, but cannot manufacture an
/// ungated auth descendant. Permit-bearing requests derive custody from the
/// same actual work used by host turns; scope failure stays sticky.
pub(crate) async fn scope_account_request<T>(
    ticket: ProcessorTaskTicket,
    work: Option<Box<dyn HostOperationWork>>,
    future: impl Future<Output = T>,
) -> codex_protocol::error::Result<T> {
    let result = codex_model_provider::with_auth_task_scope(
        AuthTaskAccess::Admitted(Arc::new(CatalogAuthTasks {
            ticket: ticket.clone(),
            work: work.map(Arc::from),
            require_account_work: true,
        })),
        future,
    )
    .await;
    if result.is_err() {
        let _ = ticket.mark_unverified();
    }
    result
}

#[cfg(test)]
#[path = "auth_task_work_tests.rs"]
mod tests;
