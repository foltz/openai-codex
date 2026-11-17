use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[tokio::test(start_paused = true)]
async fn first_edge_retains_original_deadline_across_later_signals() {
    let (latch, _custody) = OwnerLatch::new();
    let start = Instant::now();
    let first = latch.latch();
    assert_eq!(first, start + SHUTDOWN_ACK_TIMEOUT);
    tokio::time::advance(Duration::from_secs(5)).await;
    assert_eq!(latch.latch(), first);
    assert_eq!(latch.expiry().deadline(), Some(first));
}

#[tokio::test]
async fn early_and_late_callbacks_run_once_outside_action_lock() {
    let (latch, _custody) = OwnerLatch::new();
    let latch = Arc::new(latch);
    let calls = Arc::new(AtomicUsize::new(0));
    for before in [true, false] {
        if !before {
            latch.latch();
        }
        let weak = Arc::downgrade(&latch);
        let calls = calls.clone();
        latch.on_edge(move |deadline| {
            let latch = weak.upgrade().unwrap();
            assert!(latch.intake_actions.try_lock().is_ok());
            assert_eq!(latch.expiry().deadline(), Some(deadline));
            calls.fetch_add(1, Ordering::SeqCst);
        });
    }
    latch.latch();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn callback_can_register_another_callback_after_latch() {
    let (latch, _custody) = OwnerLatch::new();
    let latch = Arc::new(latch);
    let weak = Arc::downgrade(&latch);
    let calls = Arc::new(AtomicUsize::new(0));
    let nested_calls = calls.clone();
    latch.on_edge(move |deadline| {
        let latch = weak.upgrade().unwrap();
        let nested_calls = Arc::clone(&nested_calls);
        latch.on_edge(move |nested| {
            assert_eq!(nested, deadline);
            nested_calls.fetch_add(1, Ordering::SeqCst);
        });
    });
    latch.latch();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_registration_and_latch_do_not_lose_callbacks() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for _ in 0..128 {
            let (latch, _custody) = OwnerLatch::new();
            let latch = Arc::new(latch);
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let calls = Arc::new(AtomicUsize::new(0));
            let register = tokio::task::spawn_blocking({
                let latch = latch.clone();
                let barrier = barrier.clone();
                let calls = calls.clone();
                move || {
                    barrier.wait();
                    latch.on_edge(move |_| {
                        calls.fetch_add(1, Ordering::SeqCst);
                    });
                }
            });
            let edge = tokio::task::spawn_blocking({
                let latch = latch.clone();
                move || {
                    barrier.wait();
                    latch.latch();
                }
            });
            register.await.unwrap();
            edge.await.unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn intake_hook_uses_weak_latch_and_first_edge() {
    let (latch, custody) = OwnerLatch::new();
    let latch = Arc::new(latch);
    install_intake_latch(&latch);
    let expiry = latch.expiry();
    expiry.mark_shutdown_edge();
    let deadline = expiry.deadline();
    assert!(deadline.is_some());
    expiry.mark_shutdown_edge();
    assert_eq!(expiry.deadline(), deadline);
    let weak = Arc::downgrade(&latch);
    drop(latch);
    assert!(weak.upgrade().is_none());
    expiry.mark_shutdown_edge();
    drop(custody);
}
