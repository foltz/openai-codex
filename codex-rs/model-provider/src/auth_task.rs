//! Original auth-task custody supplied by a finite host invocation, never a provider factory.
use codex_protocol::error::{CodexErr, Result};
pub use codex_protocol::host_auth_work::{
    AuthTaskBlocking, AuthTaskFailure, AuthTaskFuture, AuthTaskObservation, AuthTaskSpawner,
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
    // The observation aliases terminal evidence, not finite account authority.
    // Only the actual original holds the spawner/root until its body ends.
    let observed_failure = context.failure.clone();
    Ok(Box::pin(async move {
        observation.await.inspect_err(|reason| {
            if let Ok(mut failure) = observed_failure.lock() {
                failure.get_or_insert(*reason);
            }
        })?;
        // An enclosing scope may already have returned its observer. A child
        // registration error caught inside the parent must still be observable.
        let failure = *observed_failure
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

/// Endpoint for one finite queued request, never a reusable service identity.
/// Clones alias that SAME original. Dropping every endpoint lets it finish after
/// all accepted commands; no endpoint is retained by its own receiver.
#[derive(Clone)]
pub struct DeferredAuthTask {
    commands: tokio::sync::mpsc::UnboundedSender<AuthTaskFuture>,
    failure: Arc<Mutex<Option<AuthTaskFailure>>>,
}

#[derive(Debug)]
struct RefusedDeferredAuthTask;
impl AuthTaskSpawner for RefusedDeferredAuthTask {
    fn spawn(
        &self,
        task: AuthTaskFuture,
    ) -> std::result::Result<AuthTaskObservation, AuthTaskFailure> {
        drop(task);
        Err(AuthTaskFailure::Unavailable)
    }
}
impl DeferredAuthTask {
    /// Fire-and-observe through the original host ledger, without creating a
    /// supervisor task. This must only carry effects of the admitted request.
    pub fn dispatch(&self, future: AuthTaskFuture) -> std::result::Result<(), AuthTaskFailure> {
        let Err(rejected) = self.commands.send(future) else {
            return Ok(());
        };
        // A failed command cannot borrow its sender's unrelated root or fall
        // back to unscoped auth births from captured destructors. This context
        // has NO authority; it explicitly refuses every nested auth birth.
        let refusal = Context {
            spawner: Arc::new(RefusedDeferredAuthTask),
            failure: self.failure.clone(),
        };
        let _ = refusal.fail(AuthTaskFailure::Unavailable);
        AUTH_TASK_CONTEXT.sync_scope(refusal, || drop(rejected.0));
        Err(AuthTaskFailure::Unavailable)
    }

    /// A cancelled result observer cannot cancel an accepted physical command.
    pub async fn run<T: Send + 'static>(
        &self,
        future: impl Future<Output = T> + Send + 'static,
    ) -> std::result::Result<T, AuthTaskFailure> {
        let (send, receive) = oneshot::channel();
        self.dispatch(Box::pin(async move {
            let value = future.await;
            let _ = send.send(value);
        }))?;
        receive.await.map_err(|_| AuthTaskFailure::Unavailable)
    }
}

/// Admit the actual finite original before a caller publishes its queue entry.
/// No inherited context means no managed task and no implicit fresh admission.
/// The caller MUST retain the returned observation in its owning task family.
pub fn prepare_deferred_auth_task()
-> std::result::Result<Option<(DeferredAuthTask, AuthTaskObservation)>, AuthTaskFailure> {
    let Some(context) = AUTH_TASK_CONTEXT.try_with(Clone::clone).ok() else {
        return Ok(None);
    };
    let (commands, mut receiver) = tokio::sync::mpsc::unbounded_channel::<AuthTaskFuture>();
    let failure = context.failure.clone();
    let observation = dispatch_provider_task(
        context,
        Box::pin(async move {
            while let Some(command) = receiver.recv().await {
                command.await;
            }
        }),
    )?;
    Ok(Some((DeferredAuthTask { commands, failure }, observation)))
}

/// Legacy unmanaged JoinError is preserved; custody failure is a distinct
/// managed error, never an invalid request or implicit ungated fallback.
#[derive(Debug)]
pub enum AuthBlockingCallError {
    Join(tokio::task::JoinError),
    Custody(AuthTaskFailure),
}
impl std::fmt::Display for AuthBlockingCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Join(error) => std::fmt::Display::fmt(error, f),
            Self::Custody(failure) => write!(f, "blocking task custody failed: {failure:?}"),
        }
    }
}
impl std::error::Error for AuthBlockingCallError {}

/// The supplied scope factory is entered ONLY on a physically admitted managed
/// blocking original. Its guard need not be Send: it never leaves that thread.
/// It must not hold account authority in a reusable factory or manager.
pub async fn run_blocking_call_with_scope<T, F, S, G>(
    scope: S,
    work: F,
) -> std::result::Result<T, AuthBlockingCallError>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
    S: FnOnce() -> G + Send + 'static,
    G: 'static,
{
    let Some(context) = AUTH_TASK_CONTEXT.try_with(Clone::clone).ok() else {
        // Match the existing independent caller's Tokio blocking join semantics.
        drop(scope);
        return tokio::task::spawn_blocking(work)
            .await
            .map_err(AuthBlockingCallError::Join);
    };
    let (send, receive) = oneshot::channel();
    let child_context = context.clone();
    let observation = context
        .spawner
        .spawn_blocking(Box::new(move || {
            // sync_scope restores on panic and permits nested auth/provider births
            // from this exact finite root, not from an idle worker/factory.
            AUTH_TASK_CONTEXT.sync_scope(child_context, || {
                let _scope = scope();
                let value = work();
                // A cancelled observer drops its result inside the same owned scope.
                let _ = send.send(value);
            });
        }))
        .map_err(|failure| {
            let _ = context.fail(failure);
            AuthBlockingCallError::Custody(failure)
        })?;
    observation.await.map_err(|failure| {
        let _ = context.fail(failure);
        AuthBlockingCallError::Custody(failure)
    })?;
    let failure = *context
        .failure
        .lock()
        .map_err(|_| AuthBlockingCallError::Custody(AuthTaskFailure::Unavailable))?;
    if let Some(failure) = failure {
        return Err(AuthBlockingCallError::Custody(failure));
    }
    receive.await.map_err(|_| {
        let _ = context.fail(AuthTaskFailure::Unavailable);
        AuthBlockingCallError::Custody(AuthTaskFailure::Unavailable)
    })
}

pub async fn run_blocking_call<T: Send + 'static>(
    work: impl FnOnce() -> T + Send + 'static,
) -> std::result::Result<T, AuthBlockingCallError> {
    run_blocking_call_with_scope(|| (), work).await
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn unmanaged_blocking_call_keeps_legacy_thread_and_does_not_enter_managed_scope() {
        let caller = std::thread::current().id();
        let entered = Arc::new(AtomicBool::new(false));
        let entered_scope = entered.clone();
        let (worker, context_absent, value) = run_blocking_call_with_scope(
            move || {
                entered_scope.store(true, Ordering::SeqCst);
            },
            || {
                (
                    std::thread::current().id(),
                    AUTH_TASK_CONTEXT.try_with(|_| ()).is_err(),
                    17,
                )
            },
        )
        .await
        .unwrap();
        assert_ne!(worker, caller);
        assert!(context_absent);
        assert_eq!(value, 17);
        assert!(!entered.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn unmanaged_blocking_panic_retains_legacy_join_error_variant() {
        match run_blocking_call(|| -> () {
            panic!("synthetic unmanaged blocking panic");
        })
        .await
        .unwrap_err()
        {
            AuthBlockingCallError::Join(error) => assert!(error.is_panic()),
            AuthBlockingCallError::Custody(failure) => panic!("legacy join changed to {failure:?}"),
        }
    }

    #[tokio::test]
    async fn unsupported_managed_blocking_host_refuses_scope_and_body_with_sticky_failure() {
        #[derive(Debug)]
        struct Unsupported;
        impl AuthTaskSpawner for Unsupported {
            fn spawn(
                &self,
                _: AuthTaskFuture,
            ) -> std::result::Result<AuthTaskObservation, AuthTaskFailure> {
                panic!("must not wrap blocking work in async fallback");
            }
        }
        let scoped = Arc::new(AtomicBool::new(false));
        let polled = Arc::new(AtomicBool::new(false));
        let scoped_work = scoped.clone();
        let polled_work = polled.clone();
        let result = with_auth_task_scope(
            AuthTaskAccess::Admitted(Arc::new(Unsupported)),
            async move {
                let _ = run_blocking_call_with_scope(
                    move || {
                        scoped_work.store(true, Ordering::SeqCst);
                    },
                    move || polled_work.store(true, Ordering::SeqCst),
                )
                .await;
                // Even deliberately swallowing the inner error cannot hide it.
            },
        )
        .await;
        assert!(result.is_err());
        assert!(!scoped.load(Ordering::SeqCst));
        assert!(!polled.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn required_absence_never_enters_blocking_scope_or_body() {
        let entered = Arc::new(AtomicBool::new(false));
        let polled = Arc::new(AtomicBool::new(false));
        let a = entered.clone();
        let b = polled.clone();
        assert!(
            with_auth_task_scope(AuthTaskAccess::Required, async move {
                run_blocking_call_with_scope(
                    move || a.store(true, Ordering::SeqCst),
                    move || b.store(true, Ordering::SeqCst),
                )
                .await
            })
            .await
            .is_err()
        );
        assert!(!entered.load(Ordering::SeqCst));
        assert!(!polled.load(Ordering::SeqCst));
    }
}
