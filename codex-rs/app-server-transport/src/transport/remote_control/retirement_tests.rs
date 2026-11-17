use super::*;
use pretty_assertions::assert_eq;
use tokio::sync::oneshot;
use tokio::time::Duration;

#[tokio::test]
async fn cancelled_observation_replays_the_same_joined_worker() {
    let retirement = Retirement::default();
    let (release, held) = oneshot::channel();
    drop(retirement.track(tokio::spawn(async move { held.await.is_ok() })));
    assert!(
        tokio::time::timeout(Duration::from_millis(10), retirement.observe())
            .await
            .is_err()
    );
    release.send(()).expect("original worker still owned");
    assert!(retirement.observe().await);
    assert!(retirement.observe().await);
}

#[tokio::test]
async fn failed_or_panicked_workers_never_become_clean_on_retry() {
    let retirement = Retirement::default();
    drop(retirement.track(tokio::spawn(async { false })));
    drop(retirement.track(tokio::spawn(async { panic!("synthetic worker panic") })));
    assert!(!retirement.observe().await);
    drop(retirement.track(tokio::spawn(async { true })));
    assert!(!retirement.observe().await);
    assert_eq!(retirement.snapshot().len(), 3);
}

#[tokio::test]
async fn compaction_drops_join_panic_payload_outside_owner_lock() {
    use std::sync::Arc;
    use std::sync::Weak;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    struct Payload {
        owner: Weak<Retirement>,
        outside: Arc<AtomicBool>,
    }
    impl Drop for Payload {
        fn drop(&mut self) {
            let owner = self.owner.upgrade().expect("owner retained");
            self.outside
                .store(owner.receipts.try_lock().is_ok(), Ordering::SeqCst);
        }
    }
    let owner = Arc::new(Retirement::default());
    let outside = Arc::new(AtomicBool::new(false));
    let payload = Payload {
        owner: Arc::downgrade(&owner),
        outside: outside.clone(),
    };
    let task: tokio::task::JoinHandle<bool> =
        tokio::spawn(async move { std::panic::panic_any(payload) });
    let actual_original = task.abort_handle();
    drop(owner.track(task));
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !actual_original.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("original task terminates");
    owner.retain(futures::future::ready(true).boxed().shared());
    assert!(
        outside.load(Ordering::SeqCst),
        "payload destruction may re-enter owner"
    );
    assert!(
        !owner.observe().await,
        "panic failure remains sticky after compaction"
    );
}
