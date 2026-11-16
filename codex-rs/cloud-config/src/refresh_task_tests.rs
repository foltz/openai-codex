use super::*;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

async fn observe_after_pending(
    mut receipt: CloudRefreshTask,
    started: oneshot::Sender<()>,
) -> Result<(), CloudRefreshJoinError> {
    let mut started = Some(started);
    std::future::poll_fn(|cx| {
        let result = Pin::new(&mut receipt).poll(cx);
        if result.is_pending()
            && let Some(started) = started.take()
        {
            started.send(()).unwrap();
        }
        result
    })
    .await
}

#[tokio::test]
async fn two_observers_receive_the_same_original_join_without_lost_wakes() {
    let (release, held) = oneshot::channel();
    let receipt = CloudRefreshTask::new(tokio::spawn(async move {
        held.await.unwrap();
    }));
    let (left_started, left_pending) = oneshot::channel();
    let (right_started, right_pending) = oneshot::channel();
    let left = tokio::spawn(observe_after_pending(receipt.clone(), left_started));
    let right = tokio::spawn(observe_after_pending(receipt.clone(), right_started));
    tokio::time::timeout(Duration::from_secs(1), async {
        left_pending.await.unwrap();
        right_pending.await.unwrap();
    })
    .await
    .unwrap();
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        left.await.unwrap().unwrap();
        right.await.unwrap().unwrap();
        receipt.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn dropping_an_observer_does_not_cancel_the_original() {
    let (release, held) = oneshot::channel();
    let receipt = CloudRefreshTask::new(tokio::spawn(async move {
        held.await.unwrap();
    }));
    let (started, pending) = oneshot::channel();
    let observer = tokio::spawn(observe_after_pending(receipt.clone(), started));
    tokio::time::timeout(Duration::from_secs(1), pending)
        .await
        .unwrap()
        .unwrap();
    observer.abort();
    assert!(observer.await.unwrap_err().is_cancelled());
    assert!(!receipt.is_finished());
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), receipt)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn unused_completed_observation_does_not_retain_original_captures() {
    let capture = Arc::new(());
    let weak = Arc::downgrade(&capture);
    let (entered, started) = oneshot::channel();
    let (release, held) = oneshot::channel();
    let receipt = CloudRefreshTask::new(tokio::spawn(async move {
        let _capture = capture;
        entered.send(()).unwrap();
        held.await.unwrap();
    }));
    let unused = receipt.clone();
    tokio::time::timeout(Duration::from_secs(1), started)
        .await
        .unwrap()
        .unwrap();
    assert!(weak.upgrade().is_some());
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), receipt)
        .await
        .unwrap()
        .unwrap();
    assert!(weak.upgrade().is_none());
    unused.await.unwrap();
}

#[tokio::test]
async fn original_panic_remains_visible_to_every_observer() {
    let receipt = CloudRefreshTask::new(tokio::spawn(async {
        panic!("synthetic original panic");
    }));
    let other = receipt.clone();
    let failure = tokio::time::timeout(Duration::from_secs(1), receipt)
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(failure, CloudRefreshJoinError::Panicked);
    assert_eq!(failure, other.await.unwrap_err());
}
