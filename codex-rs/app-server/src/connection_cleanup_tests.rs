use super::*;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Cleanup that does one unit of work per second and never finishes.
async fn ticking(work: Arc<AtomicUsize>, finished: Arc<AtomicBool>) {
    for _ in 0..1_000 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        work.fetch_add(1, Ordering::SeqCst);
    }
    finished.store(true, Ordering::SeqCst);
}

#[tokio::test(start_paused = true)]
async fn cleanup_stops_at_owner_expiry_and_hands_back_its_owner() {
    let expired = CancellationToken::new();
    // The external owner keeps retained cleanup for the whole test.
    let custody = codex_app_server_transport::OwnerCustody::new(expired.clone());
    let expiry = custody.expiry();
    let work = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(AtomicBool::new(false));
    let mut tasks = ConnectionCleanupTasks::with_expiry(expiry.clone());
    tasks.spawn(ticking(Arc::clone(&work), Arc::clone(&finished)));

    tokio::time::sleep(Duration::from_millis(5_500)).await;
    expired.cancel();
    // The task returns cooperatively, so the join needs no timer.
    let all_finished = tasks.drain().await;

    assert!(!all_finished, "an owner handed back is not completion");
    assert_eq!(work.load(Ordering::SeqCst), 5);
    assert_eq!(expiry.returned_tasks(), vec!["connection_cleanup"]);
    tokio::time::advance(Duration::from_secs(600)).await;
    tokio::task::yield_now().await;
    assert_eq!(work.load(Ordering::SeqCst), 5, "no unit after expiry");
    assert!(!finished.load(Ordering::SeqCst));
    drop(custody);
}

#[tokio::test(start_paused = true)]
async fn cleanup_before_expiry_is_driven_to_completion() {
    let custody = codex_app_server_transport::OwnerCustody::new(CancellationToken::new());
    let expiry = custody.expiry();
    let done = Arc::new(AtomicBool::new(false));
    let mut tasks = ConnectionCleanupTasks::with_expiry(expiry.clone());
    let observed = Arc::clone(&done);
    tasks.spawn(async move {
        tokio::time::sleep(Duration::from_secs(20)).await;
        observed.store(true, Ordering::SeqCst);
    });

    assert!(tasks.drain().await);
    assert!(done.load(Ordering::SeqCst));
    assert!(expiry.returned_tasks().is_empty());
}

#[tokio::test]
async fn already_reaped_panic_remains_incomplete_on_every_drain() {
    let mut tasks = ConnectionCleanupTasks::new();
    tasks.spawn(async {
        panic!("cleanup failed");
    });
    tasks.reap_next().await;
    assert!(!tasks.drain().await);
    assert!(!tasks.drain().await);
}

#[tokio::test]
async fn already_reaped_success_allows_clean_drain() {
    let mut tasks = ConnectionCleanupTasks::new();
    tasks.spawn(async {});
    tasks.reap_next().await;
    assert!(tasks.drain().await);
}

#[tokio::test]
async fn missing_custody_refuses_cleanup_before_polling() {
    let polled = Arc::new(AtomicBool::new(false));
    let observed = polled.clone();
    let mut tasks = ConnectionCleanupTasks::with_expiry(OwnerExpiry::never());
    tasks.spawn(async move {
        observed.store(true, Ordering::SeqCst);
    });
    tasks.reap_next().await;
    assert!(!tasks.drain().await);
    assert!(!polled.load(Ordering::SeqCst));
}
