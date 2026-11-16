//! Original auth-task custody supplied by a finite host invocation, never a provider factory.
use codex_protocol::error::{CodexErr, Result};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

pub type AuthTaskFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
pub type AuthTaskObservation =
    Pin<Box<dyn Future<Output = std::result::Result<(), AuthTaskFailure>> + Send + 'static>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthTaskFailure {
    Refused,
    Unavailable,
    Cancelled,
    Panicked,
}
impl AuthTaskFailure {
    fn error(self) -> CodexErr {
        CodexErr::Fatal(format!("provider auth task custody failed: {self:?}"))
    }
}

/// Must capture/derive before dispatch and retain the original join independently of observers.
/// The future owns no credential output: its private caller channel owns that result.
pub trait AuthTaskSpawner: std::fmt::Debug + Send + Sync {
    fn spawn(
        &self,
        task: AuthTaskFuture,
    ) -> std::result::Result<AuthTaskObservation, AuthTaskFailure>;
}

pub enum AuthTaskAccess {
    /// Existing independent callers have no managed account host.
    Ungated,
    /// Explicit Required absence refuses before polling the operation.
    Required,
    Admitted(Arc<dyn AuthTaskSpawner>),
}

#[derive(Clone)]
struct Context {
    spawner: Arc<dyn AuthTaskSpawner>,
    failure: Arc<Mutex<Option<AuthTaskFailure>>>,
}
impl Context {
    fn fail(&self, reason: AuthTaskFailure) -> CodexErr {
        if let Ok(mut failure) = self.failure.lock() {
            failure.get_or_insert(reason);
        }
        reason.error()
    }
}
tokio::task_local! { static AUTH_TASK_CONTEXT: Context; }

/// Only the actual operation owns this scope. Child spawns transfer it explicitly.
/// A sticky custody failure cannot be hidden by an upstream Option/bool auth fallback.
pub async fn with_auth_task_scope<T>(
    access: AuthTaskAccess,
    future: impl Future<Output = T>,
) -> Result<T> {
    let spawner = match access {
        AuthTaskAccess::Ungated => return Ok(future.await),
        AuthTaskAccess::Required => return Err(AuthTaskFailure::Refused.error()),
        AuthTaskAccess::Admitted(spawner) => spawner,
    };
    let context = Context {
        spawner,
        failure: Arc::new(Mutex::new(None)),
    };
    let result = AUTH_TASK_CONTEXT.scope(context.clone(), future).await;
    let failure = *context
        .failure
        .lock()
        .map_err(|_| AuthTaskFailure::Unavailable.error())?;
    match failure {
        Some(reason) => Err(reason.error()),
        None => Ok(result),
    }
}

pub(crate) async fn spawn<T: Send + 'static>(
    future: impl Future<Output = T> + Send + 'static,
) -> Result<T> {
    let Some(context) = AUTH_TASK_CONTEXT.try_with(Clone::clone).ok() else {
        // Preserve the existing unmanaged caller's original Tokio join/error behavior.
        return tokio::spawn(future).await.map_err(Into::into);
    };
    let (send, receive) = oneshot::channel();
    let child_context = context.clone();
    let task = Box::pin(async move {
        let value = AUTH_TASK_CONTEXT.scope(child_context, future).await;
        // Observer cancellation discards only this result, never the credential save.
        let _ = send.send(value);
    });
    let observation = context
        .spawner
        .spawn(task)
        .map_err(|error| context.fail(error))?;
    observation.await.map_err(|error| context.fail(error))?;
    receive
        .await
        .map_err(|_| context.fail(AuthTaskFailure::Unavailable))
}
