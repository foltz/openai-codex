//! Original auth-task custody supplied by a finite host invocation, never a provider factory.
use crate::error::{CodexErr, Result};
pub use crate::host_auth_work::{
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
    let prior_failure = context
        .failure
        .lock()
        .map(|failure| *failure)
        .unwrap_or(Some(AuthTaskFailure::Unavailable));
    if let Some(reason) = prior_failure {
        // Captured destructors of an already-refused operation cannot borrow
        // its caller's still-live spawner or fall back to unmanaged auth work.
        let refusal = Context {
            spawner: Arc::new(RefusedDeferredAuthTask),
            failure: context.failure.clone(),
        };
        AUTH_TASK_CONTEXT.sync_scope(refusal, || drop(future));
        return Err(reason);
    }
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
/// Record an explicit managed birth refusal without granting authority.
/// Independent callers have no context to mutate and retain their behavior.
pub fn record_auth_task_failure(failure: AuthTaskFailure) -> AuthTaskFailure {
    let _ = AUTH_TASK_CONTEXT.try_with(|context| {
        let _ = context.fail(failure);
    });
    failure
}

impl DeferredAuthTask {
    pub fn same_original(&self, other: &Self) -> bool {
        self.commands.same_channel(&other.commands)
    }

    pub fn record_failure(&self, failure: AuthTaskFailure) {
        if let Ok(mut slot) = self.failure.lock() {
            slot.get_or_insert(failure);
        }
    }

    fn known_failure(&self) -> Option<AuthTaskFailure> {
        self.failure
            .lock()
            .map(|failure| *failure)
            .unwrap_or(Some(AuthTaskFailure::Unavailable))
    }

    fn drop_refused<T>(&self, value: T, failure: AuthTaskFailure) {
        let refusal = Context {
            spawner: Arc::new(RefusedDeferredAuthTask),
            failure: self.failure.clone(),
        };
        let _ = refusal.fail(failure);
        AUTH_TASK_CONTEXT.sync_scope(refusal, || drop(value));
    }
    /// Fire-and-observe through the original host ledger, without creating a
    /// supervisor task. This must only carry effects of the admitted request.
    pub fn dispatch(&self, future: AuthTaskFuture) -> std::result::Result<(), AuthTaskFailure> {
        if let Some(failure) = self.known_failure() {
            self.drop_refused(future, failure);
            return Err(failure);
        }
        let Err(rejected) = self.commands.send(future) else {
            return Ok(());
        };
        // A failed command cannot borrow its sender's unrelated root or fall
        // back to unscoped auth births from captured destructors. This context
        // has NO authority; it explicitly refuses every nested auth birth.
        self.drop_refused(rejected.0, AuthTaskFailure::Unavailable);
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
        let value = receive.await.map_err(|_| AuthTaskFailure::Unavailable)?;
        if let Some(failure) = self.known_failure() {
            self.drop_refused(value, failure);
            return Err(failure);
        }
        Ok(value)
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

/// The invoker and accepted physical original briefly share this work until
/// dispatch returns. Rejection can therefore mark its precise failure before
/// captured values are dropped. Observers never retain this root-bearing cell.
struct NamedAuthThreadWork {
    work: Mutex<Option<AuthTaskBlocking>>,
    context: Option<Context>,
    failure: Arc<Mutex<Option<AuthTaskFailure>>>,
}
impl NamedAuthThreadWork {
    fn fail(&self, reason: AuthTaskFailure) {
        if let Ok(mut failure) = self.failure.lock() {
            failure.get_or_insert(reason);
        }
    }
    fn known_failure(&self) -> Option<AuthTaskFailure> {
        self.failure
            .lock()
            .map(|failure| *failure)
            .unwrap_or(Some(AuthTaskFailure::Unavailable))
    }
    fn drop_refused(&self, work: AuthTaskBlocking) {
        if self.context.is_some() {
            let refusal = Context {
                spawner: Arc::new(RefusedDeferredAuthTask),
                failure: self.failure.clone(),
            };
            AUTH_TASK_CONTEXT.sync_scope(refusal, || drop(work));
        } else {
            drop(work);
        }
    }
    fn run(&self) {
        let work = match self.work.lock() {
            Ok(mut work) => work.take(),
            Err(_) => {
                self.fail(AuthTaskFailure::Unavailable);
                return;
            }
        };
        let Some(work) = work else {
            self.fail(AuthTaskFailure::Unavailable);
            return;
        };
        if self.known_failure().is_some() {
            self.drop_refused(work);
            return;
        }
        match &self.context {
            Some(context) => AUTH_TASK_CONTEXT.sync_scope(context.clone(), work),
            None => work(),
        }
    }
}
impl Drop for NamedAuthThreadWork {
    fn drop(&mut self) {
        let work = self
            .work
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(work) = work {
            self.fail(AuthTaskFailure::Unavailable);
            self.drop_refused(work);
        }
    }
}

/// Admit the actual physical original before it creates a named OS thread.
/// That original joins the thread even when every result observer is dropped.
/// Only the executing work carries the finite auth root; its observation holds
/// failure evidence only. Independent callers keep the same physical/name/join
/// behavior, with no auth context added to their body.
pub fn spawn_named_auth_thread(
    name: &'static str,
    work: AuthTaskBlocking,
) -> std::result::Result<AuthTaskObservation, AuthTaskFailure> {
    let context = AUTH_TASK_CONTEXT.try_with(Clone::clone).ok();
    let failure = context
        .as_ref()
        .map(|context| context.failure.clone())
        .unwrap_or_else(|| Arc::new(Mutex::new(None)));
    let work = Arc::new(NamedAuthThreadWork {
        work: Mutex::new(Some(work)),
        context: context.clone(),
        failure: failure.clone(),
    });
    if let Some(reason) = work.known_failure() {
        return Err(reason);
    }
    let original_work = work.clone();
    let original: AuthTaskBlocking = Box::new(move || {
        let named_work = original_work.clone();
        match std::thread::Builder::new()
            .name(name.to_owned())
            .spawn(move || named_work.run())
        {
            Ok(thread) => {
                if let Err(panic) = thread.join() {
                    original_work.fail(AuthTaskFailure::Panicked);
                    std::panic::resume_unwind(panic);
                }
            }
            Err(_) => original_work.fail(AuthTaskFailure::Unavailable),
        }
    });
    let dispatched = match &context {
        Some(context) => context.spawner.spawn_blocking(original),
        None => match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                let original = runtime.spawn_blocking(original);
                Ok(Box::pin(async move {
                    original.await.map_err(|error| {
                        if error.is_cancelled() {
                            AuthTaskFailure::Cancelled
                        } else {
                            AuthTaskFailure::Panicked
                        }
                    })
                }) as AuthTaskObservation)
            }
            Err(_) => Err(AuthTaskFailure::Unavailable),
        },
    };
    let observation = dispatched.inspect_err(|reason| work.fail(*reason))?;
    // The local Arc drops on return; only accepted executing work owns the root.
    Ok(Box::pin(async move {
        observation.await.inspect_err(|reason| {
            if let Ok(mut failure) = failure.lock() {
                failure.get_or_insert(*reason);
            }
        })?;
        let failure = *failure.lock().map_err(|_| AuthTaskFailure::Unavailable)?;
        match failure {
            Some(reason) => Err(reason),
            None => Ok(()),
        }
    }))
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
    // Reuse the physical-work capsule: a refused or abandoned captured body
    // must be destroyed under a refusing context, never its caller's root.
    let work = Arc::new(NamedAuthThreadWork {
        work: Mutex::new(Some(Box::new(move || {
            let _scope = scope();
            let value = work();
            // A cancelled observer drops its result in the physical scope.
            let _ = send.send(value);
        }))),
        context: Some(context.clone()),
        failure: context.failure.clone(),
    });
    if let Some(reason) = work.known_failure() {
        return Err(AuthBlockingCallError::Custody(reason));
    }
    let original_work = work.clone();
    let observation = context
        .spawner
        .spawn_blocking(Box::new(move || original_work.run()))
        .map_err(|failure| {
            work.fail(failure);
            AuthBlockingCallError::Custody(failure)
        })?;
    // Only the accepted physical original retains the capsule across await.
    // If a host abandons it, Drop records failure and releases the result sender.
    drop(work);
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

#[doc(hidden)]
pub async fn spawn<T: Send + 'static>(
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

#[cfg(test)]
mod named_thread_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    #[tokio::test]
    async fn independent_named_thread_keeps_name_and_no_auth_scope() {
        let (send, receive) = oneshot::channel();
        let original = spawn_named_auth_thread(
            "provider-independent-original",
            Box::new(move || {
                let _ = send.send((
                    std::thread::current().name().map(str::to_owned),
                    AUTH_TASK_CONTEXT.try_with(|_| ()).is_err(),
                ));
            }),
        )
        .unwrap();
        original.await.unwrap();
        assert_eq!(
            receive.await.unwrap(),
            (Some("provider-independent-original".into()), true)
        );
    }
    #[tokio::test]
    async fn named_thread_panic_is_original_failure_not_a_successful_join() {
        let original = spawn_named_auth_thread(
            "provider-panicked-original",
            Box::new(|| {
                panic!("synthetic named original panic");
            }),
        )
        .unwrap();
        assert_eq!(original.await, Err(AuthTaskFailure::Panicked));
    }
    #[test]
    fn independent_named_thread_without_runtime_refuses_before_birth() {
        let polled = Arc::new(AtomicBool::new(false));
        let body = polled.clone();
        assert_eq!(
            spawn_named_auth_thread(
                "must-not-start",
                Box::new(move || {
                    body.store(true, Ordering::SeqCst);
                })
            )
            .err(),
            Some(AuthTaskFailure::Unavailable)
        );
        assert!(!polled.load(Ordering::SeqCst));
    }
    #[derive(Debug)]
    struct NoPhysicalFallback;
    impl AuthTaskSpawner for NoPhysicalFallback {
        fn spawn(
            &self,
            _: AuthTaskFuture,
        ) -> std::result::Result<AuthTaskObservation, AuthTaskFailure> {
            panic!("must not fall back to async or borrow the rejected sender root");
        }
    }
    struct RefusedCapture {
        refused: Arc<AtomicBool>,
        polled: Arc<AtomicBool>,
    }
    impl Drop for RefusedCapture {
        fn drop(&mut self) {
            let polled = self.polled.clone();
            let result = spawn_provider_task(Box::pin(async move {
                polled.store(true, Ordering::SeqCst);
            }));
            self.refused.store(result.is_err(), Ordering::SeqCst);
        }
    }
    #[tokio::test]
    async fn unsupported_named_physical_host_refuses_body_and_capture_birth() {
        let refused = Arc::new(AtomicBool::new(false));
        let nested = Arc::new(AtomicBool::new(false));
        let polled = Arc::new(AtomicBool::new(false));
        let body = polled.clone();
        let capture = RefusedCapture {
            refused: refused.clone(),
            polled: nested.clone(),
        };
        let result = with_auth_task_scope(
            AuthTaskAccess::Admitted(Arc::new(NoPhysicalFallback)),
            async move {
                assert_eq!(
                    spawn_named_auth_thread(
                        "must-not-start",
                        Box::new(move || {
                            std::hint::black_box(capture);
                            body.store(true, Ordering::SeqCst);
                        })
                    )
                    .err(),
                    Some(AuthTaskFailure::Unavailable)
                );
            },
        )
        .await;
        assert!(result.is_err());
        assert!(refused.load(Ordering::SeqCst));
        assert!(!nested.load(Ordering::SeqCst) && !polled.load(Ordering::SeqCst));
    }
    #[tokio::test]
    async fn known_failed_async_scope_refuses_body_and_captured_auth_birth() {
        let refused = Arc::new(AtomicBool::new(false));
        let nested = Arc::new(AtomicBool::new(false));
        let polled = Arc::new(AtomicBool::new(false));
        let body = polled.clone();
        let capture = RefusedCapture {
            refused: refused.clone(),
            polled: nested.clone(),
        };
        let result = with_auth_task_scope(
            AuthTaskAccess::Admitted(Arc::new(NoPhysicalFallback)),
            async move {
                record_auth_task_failure(AuthTaskFailure::Refused);
                assert_eq!(
                    spawn_provider_task(Box::pin(async move {
                        std::hint::black_box(capture);
                        body.store(true, Ordering::SeqCst);
                    }))
                    .err(),
                    Some(AuthTaskFailure::Refused)
                );
            },
        )
        .await;
        assert!(result.is_err());
        assert!(refused.load(Ordering::SeqCst));
        assert!(!nested.load(Ordering::SeqCst) && !polled.load(Ordering::SeqCst));
    }
}

#[cfg(test)]
mod blocking_refusal_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[derive(Debug, Default)]
    struct Host {
        births: AtomicUsize,
        abandon: bool,
    }
    impl AuthTaskSpawner for Host {
        fn spawn(
            &self,
            _: AuthTaskFuture,
        ) -> std::result::Result<AuthTaskObservation, AuthTaskFailure> {
            panic!("refused captures must not borrow the positive caller root");
        }
        fn spawn_blocking(
            &self,
            task: AuthTaskBlocking,
        ) -> std::result::Result<AuthTaskObservation, AuthTaskFailure> {
            self.births.fetch_add(1, Ordering::SeqCst);
            drop(task);
            if self.abandon {
                Ok(Box::pin(async { Ok(()) }))
            } else {
                Err(AuthTaskFailure::Unavailable)
            }
        }
    }
    struct Reenter {
        refused: Arc<AtomicBool>,
        polled: Arc<AtomicBool>,
    }
    impl Drop for Reenter {
        fn drop(&mut self) {
            let polled = self.polled.clone();
            let result = spawn_provider_task(Box::pin(async move {
                polled.store(true, Ordering::SeqCst);
            }));
            self.refused.store(result.is_err(), Ordering::SeqCst);
        }
    }
    async fn check_refusal(prior_failure: bool, abandon: bool) {
        let host = Arc::new(Host {
            abandon,
            ..Host::default()
        });
        let refused = Arc::new(AtomicBool::new(false));
        let polled = Arc::new(AtomicBool::new(false));
        let invoked = Arc::new(AtomicBool::new(false));
        let scoped = Arc::new(AtomicBool::new(false));
        let capture = Reenter {
            refused: refused.clone(),
            polled: polled.clone(),
        };
        let body = invoked.clone();
        let enter = scoped.clone();
        let result = with_auth_task_scope(AuthTaskAccess::Admitted(host.clone()), async {
            if prior_failure {
                record_auth_task_failure(AuthTaskFailure::Refused);
            }
            let inner = run_blocking_call_with_scope(
                move || {
                    enter.store(true, Ordering::SeqCst);
                },
                move || {
                    std::hint::black_box(capture);
                    body.store(true, Ordering::SeqCst);
                },
            )
            .await;
            assert!(matches!(inner, Err(AuthBlockingCallError::Custody(_))));
        })
        .await;
        assert!(result.is_err());
        assert!(refused.load(Ordering::SeqCst));
        assert!(!polled.load(Ordering::SeqCst));
        assert!(!invoked.load(Ordering::SeqCst));
        assert!(!scoped.load(Ordering::SeqCst));
        assert_eq!(
            host.births.load(Ordering::SeqCst),
            usize::from(!prior_failure)
        );
    }
    #[tokio::test]
    async fn blocking_dispatch_refusal_destroys_capture_without_borrowing_caller_root() {
        check_refusal(false, false).await;
    }
    #[tokio::test]
    async fn already_failed_context_refuses_blocking_birth_and_capture_reentry() {
        check_refusal(true, false).await;
    }
    #[tokio::test]
    async fn abandoned_blocking_body_is_failure_not_success_or_a_pending_result() {
        check_refusal(false, true).await;
    }
}
