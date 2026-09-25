use super::*;
use pretty_assertions::assert_eq;
use std::task::Poll;
use std::time::Duration;

#[tokio::test]
async fn unpolled_getter_does_not_enter_the_retirement_census() {
    let lifecycle = HomeLifecycle::default();
    let getter = lifecycle.load(/*expected_generation*/ 0, std::future::pending());
    lifecycle.retire_owners().await.expect("retire without polling getter");
    assert_eq!(lifecycle.owners.lock().expect("owners").active_getters, 0);
    assert!(getter.await.is_err());
}

#[tokio::test]
async fn retirement_cancels_polled_getter_and_waits_for_its_drop() {
    let lifecycle = Arc::new(HomeLifecycle::default());
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, held) = tokio::sync::oneshot::channel::<()>();
    let worker_lifecycle = Arc::clone(&lifecycle);
    let worker = tokio::spawn(async move {
        worker_lifecycle.load(/*expected_generation*/ 0, async {
            entered.send(()).expect("entered");
            let _ = held.await;
            Ok(None)
        }).await
    });
    started.await.expect("started");
    lifecycle.retire_owners().await.expect("retire getter");
    assert!(worker.await.expect("join getter").is_err());
    assert!(release.send(()).is_err(), "the caller-polled I/O future was dropped");
    assert_eq!(lifecycle.owners.lock().expect("owners").active_getters, 0);
}

#[tokio::test]
async fn completion_rechecks_generation_even_if_work_returns_ready() {
    let lifecycle = HomeLifecycle::default();
    let result = lifecycle.load(/*expected_generation*/ 0, async {
        lifecycle.generation.fetch_add(1, Ordering::AcqRel);
        Ok(None)
    }).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn reset_waits_for_an_admitted_publication_even_after_its_caller_drops() {
    let lifecycle = Arc::new(HomeLifecycle::default());
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, held) = tokio::sync::oneshot::channel::<()>();
    let publication = Arc::clone(&lifecycle.publication);
    // Same ownership shape as the service's unabortable publication task.
    let writer = tokio::spawn(async move {
        let _lease = publication.lock().await;
        entered.send(()).expect("entered");
        held.await.expect("release write");
    });
    drop(writer);
    started.await.expect("writer owns publication");
    let mut retirement = Box::pin(lifecycle.retire_owners());
    std::future::poll_fn(|cx| {
        assert!(retirement.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    }).await;
    assert_eq!(lifecycle.generation.load(Ordering::Acquire), 1);
    release.send(()).expect("release");
    tokio::time::timeout(Duration::from_secs(1), retirement).await.expect("bounded retirement").expect("retired");
}

#[tokio::test]
async fn global_replacement_cancellation_is_clean_but_panic_fails_retirement() {
    let lifecycle = HomeLifecycle::default();
    let refresher = tokio::spawn(std::future::pending::<()>());
    let abort = refresher.abort_handle();
    lifecycle.owners.lock().expect("owners").tasks.push(refresher);
    abort.abort();
    lifecycle.retire_owners().await.expect("replacement cancellation is clean");

    let panicked = tokio::spawn(async { panic!("fixture owner panic") });
    while !panicked.is_finished() {
        tokio::task::yield_now().await;
    }
    lifecycle.owners.lock().expect("owners").tasks.push(panicked);
    assert!(lifecycle.retire_owners().await.is_err());
    assert!(lifecycle.owners.lock().expect("owners").retiring);
    lifecycle.retire_owners().await.expect("retry observes prior failed owner");
}

#[cfg(unix)]
#[test]
fn uncreated_home_aliases_share_the_same_custody() {
    let root = tempfile::tempdir().expect("root");
    let real = root.path().join("real");
    let alias = root.path().join("alias");
    std::fs::create_dir(&real).expect("real");
    std::os::unix::fs::symlink(&real, &alias).expect("alias");
    let first = home_lifecycle(&real.join("new-home")).expect("first");
    let second = home_lifecycle(&alias.join("new-home")).expect("second");
    assert!(Arc::ptr_eq(&first, &second));
}
