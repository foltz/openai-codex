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
