use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};

fn count(snapshot: &McpWorkerSnapshot, kind: McpWorkerKind) -> &McpWorkerCounts {
    snapshot.counts.iter().find(|c| c.kind == kind).unwrap()
}

#[tokio::test]
async fn original_worker_join_survives_dropped_observer_and_runtime_owner() {
    let external = McpWorkerOwner::default();
    let runtime_owner = external.clone();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let resource = Arc::new(());
    let weak = Arc::downgrade(&resource);
    let seen = Arc::clone(&entered);
    let resume = Arc::clone(&release);
    drop(runtime_owner.spawn(
        &Handle::current(),
        McpWorkerKind::StartupSummary,
        async move {
            let _resource = resource;
            seen.notify_one();
            resume.notified().await;
        },
    ));
    entered.notified().await;
    drop(runtime_owner);
    let observer_owner = external.clone();
    let observer = tokio::spawn(async move {
        observer_owner
            .observe_until(Instant::now() + Duration::from_secs(1))
            .await
    });
    observer.abort();
    assert!(observer.await.unwrap_err().is_cancelled());
    assert!(weak.upgrade().is_some());
    assert_eq!(
        count(
            &external.observe_until(Instant::now()).await,
            McpWorkerKind::StartupSummary
        )
        .pending,
        1
    );
    release.notify_one();
    let report = external
        .observe_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(count(&report, McpWorkerKind::StartupSummary).joined, 1);
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn expired_observer_does_not_poll_a_finished_unobserved_join() {
    let owner = McpWorkerOwner::default();
    let (sent, received) = tokio::sync::oneshot::channel();
    let original = owner.spawn(
        &Handle::current(),
        McpWorkerKind::DeferredStartup,
        async move {
            sent.send(()).unwrap();
        },
    );
    received.await.unwrap();
    tokio::task::yield_now().await;
    assert!(original.peek().is_none());
    let expired = owner.observe_until(Instant::now()).await;
    assert_eq!(count(&expired, McpWorkerKind::DeferredStartup).pending, 1);
    assert!(original.peek().is_none());
    let joined = owner
        .observe_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(count(&joined, McpWorkerKind::DeferredStartup).joined, 1);
}

#[tokio::test]
async fn panic_and_abort_outcomes_survive_later_compaction() {
    let owner = McpWorkerOwner::default();
    drop(
        owner.spawn(&Handle::current(), McpWorkerKind::Reconnect, async {
            panic!("injected reconnect worker panic")
        }),
    );
    let abort = owner.spawn_abort_on_drop(
        &Handle::current(),
        McpWorkerKind::AuthNotification,
        std::future::pending(),
    );
    drop(abort);
    let first = owner
        .observe_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(count(&first, McpWorkerKind::Reconnect).panicked, 1);
    assert_eq!(count(&first, McpWorkerKind::AuthNotification).cancelled, 1);
    drop(owner.spawn(&Handle::current(), McpWorkerKind::Reconnect, async {}));
    let later = owner
        .observe_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(count(&later, McpWorkerKind::Reconnect).panicked, 1);
    assert_eq!(count(&later, McpWorkerKind::Reconnect).joined, 1);
    assert_eq!(count(&later, McpWorkerKind::AuthNotification).cancelled, 1);
    assert!(owner.state.lock().unwrap().pending.is_empty());
}

#[tokio::test]
async fn result_worker_keeps_its_original_value_after_observer_cancellation() {
    let owner = McpWorkerOwner::default();
    let release = Arc::new(Notify::new());
    let resume = Arc::clone(&release);
    let value = Arc::new(AtomicUsize::new(0));
    let returned = Arc::clone(&value);
    let result = owner.spawn_result(
        &Handle::current(),
        McpWorkerKind::DiscoveryCleanup,
        async move {
            resume.notified().await;
            returned.fetch_add(1, Ordering::SeqCst);
            returned
        },
    );
    let observer = tokio::spawn(result.clone());
    observer.abort();
    assert!(observer.await.unwrap_err().is_cancelled());
    release.notify_one();
    let actual = result.await.unwrap();
    assert!(Arc::ptr_eq(&actual, &value));
    assert_eq!(actual.load(Ordering::SeqCst), 1);
    assert_eq!(
        count(
            &owner
                .observe_until(Instant::now() + Duration::from_secs(1))
                .await,
            McpWorkerKind::DiscoveryCleanup
        )
        .joined,
        1
    );
}

#[tokio::test]
async fn snapshot_marks_admission_that_raced_with_its_original_population() {
    let owner = McpWorkerOwner::default();
    let release = Arc::new(Semaphore::new(0));
    let first_release = Arc::clone(&release);
    drop(owner.spawn(
        &Handle::current(),
        McpWorkerKind::StartupSummary,
        async move {
            first_release.acquire().await.unwrap().forget();
        },
    ));
    let mut observation = Box::pin(owner.observe_until(Instant::now() + Duration::from_secs(1)));
    assert!(futures::poll!(&mut observation).is_pending());
    drop(owner.spawn(&Handle::current(), McpWorkerKind::DeferredStartup, async {}));
    release.add_permits(1);
    let snapshot = observation.await;
    assert_eq!(snapshot.registered_at_start, 1);
    assert_eq!(snapshot.registered_at_end, 2);
    assert_eq!(count(&snapshot, McpWorkerKind::StartupSummary).joined, 1);
    // Newly admitted work is deliberately not relabelled as part of that census.
    let next = owner
        .observe_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(count(&next, McpWorkerKind::DeferredStartup).joined, 1);
}

#[tokio::test]
async fn poisoned_registry_is_disclosed_after_best_effort_join_observation() {
    let owner = McpWorkerOwner::default();
    drop(owner.spawn(&Handle::current(), McpWorkerKind::RuntimeShutdown, async {}));
    let interrupted = owner.clone();
    assert!(
        std::thread::spawn(move || {
            let _guard = interrupted.state.lock().unwrap();
            panic!("injected registry interruption");
        })
        .join()
        .is_err()
    );
    let snapshot = owner
        .observe_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert!(snapshot.poisoned);
    assert_eq!(count(&snapshot, McpWorkerKind::RuntimeShutdown).joined, 1);
}

#[tokio::test]
async fn all_worker_families_retain_distinct_counts_without_cancelling_siblings() {
    let owner = McpWorkerOwner::default();
    let release = Arc::new(Semaphore::new(0));
    let kinds = [
        McpWorkerKind::StartupSummary,
        McpWorkerKind::DeferredStartup,
        McpWorkerKind::Reconnect,
        McpWorkerKind::DiscoveryCleanup,
        McpWorkerKind::AuthNotification,
        McpWorkerKind::ConnectionShutdown,
        McpWorkerKind::RuntimeShutdown,
    ];
    for kind in kinds {
        let release = Arc::clone(&release);
        drop(owner.spawn(&Handle::current(), kind, async move {
            release.acquire().await.unwrap().forget();
        }));
    }
    let pending = owner.observe_until(Instant::now()).await;
    assert_eq!(pending.registered_at_start, 7);
    assert!(
        pending
            .counts
            .iter()
            .all(|c| c.pending == 1 && c.joined + c.panicked + c.cancelled == 0)
    );
    release.add_permits(7);
    let done = owner
        .observe_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert!(
        done.counts
            .iter()
            .all(|c| c.joined == 1 && c.pending + c.panicked + c.cancelled == 0)
    );
}

#[tokio::test]
async fn actual_empty_runtime_shutdown_retains_both_shutdown_workers() {
    let runtime = crate::McpRuntime::empty(true);
    let external = runtime.worker_owner();
    runtime.shutdown().await;
    drop(runtime);
    let snapshot = external
        .observe_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(count(&snapshot, McpWorkerKind::RuntimeShutdown).joined, 1);
    assert_eq!(
        count(&snapshot, McpWorkerKind::ConnectionShutdown).joined,
        1
    );
    assert!(
        snapshot
            .counts
            .iter()
            .all(|c| c.pending + c.panicked + c.cancelled == 0)
    );
}
