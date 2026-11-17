//! Compatibility entrypoints for the single protocol-owned auth custody context.
pub(crate) use codex_protocol::host_auth_task::spawn;
pub use codex_protocol::host_auth_task::{
    AuthBlockingCallError, AuthTaskAccess, AuthTaskBlocking, AuthTaskFailure, AuthTaskFuture,
    AuthTaskObservation, AuthTaskSpawner, DeferredAuthTask, prepare_deferred_auth_task,
    record_auth_task_failure, run_auth_call, run_blocking_call, run_blocking_call_with_scope,
    spawn_named_auth_thread, spawn_provider_task, spawn_provider_task_with_access,
    with_auth_task_scope,
};

#[cfg(test)]
mod shared_context_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::sync::oneshot;

    #[derive(Debug, Default)]
    struct PhysicalHost {
        originals: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    }
    impl AuthTaskSpawner for PhysicalHost {
        fn spawn(&self, _: AuthTaskFuture) -> Result<AuthTaskObservation, AuthTaskFailure> {
            Err(AuthTaskFailure::Refused)
        }
        fn spawn_blocking(
            &self,
            task: AuthTaskBlocking,
        ) -> Result<AuthTaskObservation, AuthTaskFailure> {
            let (send, receive) = oneshot::channel();
            let original = tokio::task::spawn_blocking(move || {
                task();
                let _ = send.send(());
            });
            self.originals.lock().unwrap().push(original);
            Ok(Box::pin(async move {
                receive.await.map_err(|_| AuthTaskFailure::Panicked)
            }))
        }
    }
    impl PhysicalHost {
        async fn join(&self) {
            let originals = std::mem::take(&mut *self.originals.lock().unwrap());
            for original in originals {
                original.await.unwrap();
            }
        }
    }

    #[tokio::test]
    async fn protocol_blocking_helper_uses_model_provider_scope_and_sticky_failure() {
        let host = Arc::new(PhysicalHost::default());
        let result = with_auth_task_scope(AuthTaskAccess::Admitted(host.clone()), async {
            let result = codex_protocol::host_auth_task::run_blocking_call(|| {
                record_auth_task_failure(AuthTaskFailure::Refused);
                17
            })
            .await;
            assert!(matches!(
                result,
                Err(AuthBlockingCallError::Custody(AuthTaskFailure::Refused))
            ));
        })
        .await;
        assert!(result.is_err());
        host.join().await;
    }

    #[tokio::test]
    async fn protocol_blocking_original_survives_dropped_model_provider_observer() {
        let host = Arc::new(PhysicalHost::default());
        let caller_host = host.clone();
        let completed = Arc::new(AtomicBool::new(false));
        let work_completed = completed.clone();
        let (started, observed_start) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let observer = tokio::spawn(async move {
            with_auth_task_scope(AuthTaskAccess::Admitted(caller_host), async move {
                codex_protocol::host_auth_task::run_blocking_call(move || {
                    let _ = started.send(());
                    released.blocking_recv().unwrap();
                    work_completed.store(true, Ordering::SeqCst);
                })
                .await
            })
            .await
        });
        observed_start.await.unwrap();
        observer.abort();
        assert!(observer.await.unwrap_err().is_cancelled());
        assert!(!completed.load(Ordering::SeqCst));
        release.send(()).unwrap();
        host.join().await;
        assert!(completed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn protocol_blocking_helper_cannot_bypass_unsupported_model_provider_host() {
        #[derive(Debug)]
        struct Unsupported;
        impl AuthTaskSpawner for Unsupported {
            fn spawn(&self, _: AuthTaskFuture) -> Result<AuthTaskObservation, AuthTaskFailure> {
                panic!("no async substitution for physical blocking work");
            }
        }
        let invoked = Arc::new(AtomicBool::new(false));
        let work = invoked.clone();
        let result = with_auth_task_scope(AuthTaskAccess::Admitted(Arc::new(Unsupported)), async {
            assert!(
                codex_protocol::host_auth_task::run_blocking_call(move || {
                    work.store(true, Ordering::SeqCst);
                })
                .await
                .is_err()
            );
        })
        .await;
        assert!(result.is_err());
        assert!(!invoked.load(Ordering::SeqCst));
    }
}
