//! Internal child custody is distinct from the incoming RPC's response ticket.
use super::*;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::sync::oneshot;

#[tokio::test]
async fn closed_child_is_not_polled_or_counted_as_an_unanswered_rpc() {
    let gate = ConnectionRpcGate::new();
    gate.close().await;
    let polled = AtomicBool::new(false);
    let result = gate
        .run_child(async {
            polled.store(true, Ordering::Release);
            42
        })
        .await;
    assert_eq!(result, Err(ConnectionRpcChildError::Closed));
    assert!(!polled.load(Ordering::Acquire));
    assert_eq!(gate.unanswered(), 0);
    assert!(gate.shutdown_with_evidence().await.is_clean());
}

#[tokio::test]
async fn child_panic_remains_incomplete_on_every_shutdown_observation() {
    let gate = ConnectionRpcGate::new();
    let result: Result<(), _> = gate
        .run_child(async { panic!("synthetic child panic") })
        .await;
    assert_eq!(result, Err(ConnectionRpcChildError::Panicked));
    assert!(!gate.shutdown_with_evidence().await.is_clean());
    assert!(!gate.shutdown_with_evidence().await.is_clean());
    assert_eq!(gate.unanswered(), 0);
}

#[tokio::test]
async fn dropped_result_observer_does_not_release_the_original_child() {
    let gate = Arc::new(ConnectionRpcGate::new());
    let tasks = TaskTracker::new();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (send, receive) = oneshot::channel();
    let child = gate.clone();
    let started = entered.clone();
    let finish = release.clone();
    let original = tasks.spawn(async move {
        let result = child
            .run_child(async move {
                started.notify_one();
                finish.notified().await;
                42
            })
            .await;
        let _ = send.send(result);
    });
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    drop(receive);
    drop(original); // Only drop the observer; the tracker retains the original.
    tasks.close();
    assert_eq!(gate.inflight_count(), 1);
    assert!(gate.shutdown_with_evidence().now_or_never().is_none());
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), tasks.wait())
        .await
        .unwrap();
    assert!(gate.shutdown_with_evidence().await.is_clean());
    assert_eq!(gate.unanswered(), 0);
}
