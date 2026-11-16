//! Original auth-task custody supplied by a finite host invocation, never a provider factory.
use codex_protocol::error::{CodexErr, Result};
pub use codex_protocol::host_auth_work::{
    AuthTaskFailure, AuthTaskFuture, AuthTaskObservation, AuthTaskSpawner,
};
use std::future::Future;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

/// Cloning shares the same finite admitted root; it does not reopen admission.
#[derive(Clone)]
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

/// Register finite provider work in its caller's original host task owner.
/// The returned observer may be dropped without cancelling that original task.
/// Unmanaged callers retain the existing detached Tokio behavior.
pub fn spawn_provider_task(
    future: AuthTaskFuture,
) -> std::result::Result<Option<AuthTaskObservation>, AuthTaskFailure> {
    let Some(context) = AUTH_TASK_CONTEXT.try_with(Clone::clone).ok() else {
        drop(tokio::spawn(future));
        return Ok(None);
    };
    dispatch_provider_task(context, future).map(Some)
}

/// Transfer an already admitted finite root across a task boundary. Required
/// absence refuses before polling; Ungated preserves any inherited context.
pub fn spawn_provider_task_with_access(
    access: AuthTaskAccess,
    future: AuthTaskFuture,
) -> std::result::Result<Option<AuthTaskObservation>, AuthTaskFailure> {
    match access {
        AuthTaskAccess::Ungated => spawn_provider_task(future),
        AuthTaskAccess::Required => Err(AuthTaskFailure::Refused),
        AuthTaskAccess::Admitted(spawner) => dispatch_provider_task(
            Context {
                spawner,
                failure: Arc::new(Mutex::new(None)),
            },
            future,
        )
        .map(Some),
    }
}

fn dispatch_provider_task(
    context: Context,
    future: AuthTaskFuture,
) -> std::result::Result<AuthTaskObservation, AuthTaskFailure> {
    let child_context = context.clone();
    // The host derives work and retains the original join before dispatch.
    let observation = context.spawner.spawn(Box::pin(async move {
        AUTH_TASK_CONTEXT.scope(child_context, future).await;
    }));
    let observation = observation.inspect_err(|reason| {
        let _ = context.fail(*reason);
    })?;
    Ok(Box::pin(async move {
        observation.await.inspect_err(|reason| {
            let _ = context.fail(*reason);
        })?;
        // An enclosing scope may already have returned its observer. A child
        // registration error caught inside the parent must still be observable.
        let failure = *context
            .failure
            .lock()
            .map_err(|_| AuthTaskFailure::Unavailable)?;
        match failure {
            Some(reason) => Err(reason),
            None => Ok(()),
        }
    }))
}

/// Own a direct credential call until its original task completes. A cancelled
/// observer discards only its private result; it never aborts credential work.
/// The typed custody failure is cloneable for shared constructor reads.
pub async fn run_auth_call<T: Send + 'static>(
    access: AuthTaskAccess,
    future: impl Future<Output = T> + Send + 'static,
) -> std::result::Result<T, AuthTaskFailure> {
    if matches!(&access, AuthTaskAccess::Ungated) && AUTH_TASK_CONTEXT.try_with(|_| ()).is_err() {
        // Direct unmanaged calls stay inline, including their caller task locals.
        return Ok(future.await);
    }
    let (send, receive) = oneshot::channel();
    let observation = spawn_provider_task_with_access(
        access,
        Box::pin(async move {
            let value = future.await;
            let _ = send.send(value);
        }),
    )?
    .ok_or(AuthTaskFailure::Unavailable)?;
    observation.await?;
    receive.await.map_err(|_| AuthTaskFailure::Unavailable)
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
