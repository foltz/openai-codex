use super::PersistenceOutcome;
use super::PersistenceRegistry;
use futures::FutureExt;
use std::sync::Arc;

#[tokio::test]
async fn cancelled_observer_keeps_persistence_worker_for_replay() {
    let registry = Arc::new(PersistenceRegistry::default());
    let (release, held) = tokio::sync::oneshot::channel();
    drop(
        registry
            .spawn(move || {
                held.blocking_recv().unwrap();
                Ok(())
            })
            .unwrap(),
    );
    registry.close();

    let mut first = Box::pin(registry.wait());
    assert!(first.as_mut().now_or_never().is_none());
    drop(first);
    release.send(()).unwrap();

    assert_eq!(registry.wait().await, PersistenceOutcome::Joined);
    assert_eq!(registry.wait().await, PersistenceOutcome::Joined);
}

#[tokio::test]
async fn closed_persistence_registry_refuses_new_worker() {
    let registry = PersistenceRegistry::default();
    registry.close();
    let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let marker = Arc::clone(&started);
    assert!(
        registry
            .spawn(move || {
                marker.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            })
            .is_err()
    );
    assert!(!started.load(std::sync::atomic::Ordering::SeqCst));
}
