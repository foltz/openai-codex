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
    let abort = owner
        .spawn_abort_on_drop(
            &Handle::current(),
            McpWorkerKind::AuthNotification,
            std::future::pending(),
        )
        .unwrap();
    drop(abort);
    let first = owner
        .observe_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(count(&first, McpWorkerKind::Reconnect).panicked, 1);
    assert_eq!(count(&first, McpWorkerKind::AuthNotification).cancelled, 1);
    assert_eq!(
        count(&first, McpWorkerKind::AuthNotification).requested_cancelled,
        1
    );
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
        McpWorkerKind::EagerStartup,
        McpWorkerKind::EmaRevocation,
    ];
    for kind in kinds {
        let release = Arc::clone(&release);
        drop(owner.spawn(&Handle::current(), kind, async move {
            release.acquire().await.unwrap().forget();
        }));
    }
    let pending = owner.observe_until(Instant::now()).await;
    assert_eq!(pending.registered_at_start, 9);
    assert!(
        pending
            .counts
            .iter()
            .all(|c| c.pending == 1 && c.joined + c.panicked + c.cancelled == 0)
    );
    release.add_permits(9);
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

#[tokio::test]
async fn eager_batch_caller_cancellation_aborts_and_retains_both_original_joins() {
    let owner = McpWorkerOwner::default();
    let entered = Arc::new(Semaphore::new(0));
    let resource = Arc::new(());
    let weak = Arc::downgrade(&resource);
    let mut bodies = Vec::new();
    for _ in 0..2 {
        let entered = Arc::clone(&entered);
        let resource = Arc::clone(&resource);
        bodies.push(
            async move {
                let _resource = resource;
                entered.add_permits(1);
                std::future::pending::<()>().await;
            }
            .boxed(),
        );
    }
    drop(resource);
    let caller_owner = owner.clone();
    let caller = tokio::spawn(async move { caller_owner.join_eager_batch(bodies).await });
    entered.acquire_many(2).await.unwrap().forget();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    let report = owner
        .observe_until(Instant::now() + Duration::from_secs(1))
        .await;
    let eager = count(&report, McpWorkerKind::EagerStartup);
    assert_eq!(eager.cancelled, 2);
    assert_eq!(eager.requested_cancelled, 2);
    assert_eq!(eager.joined + eager.panicked + eager.pending, 0);
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn eager_child_panic_aborts_sibling_and_keeps_both_outcomes() {
    let owner = McpWorkerOwner::default();
    let entered = Arc::new(Notify::new());
    let observed = Arc::clone(&entered);
    let panic_now = Arc::new(Notify::new());
    let fail = Arc::clone(&panic_now);
    let bodies = vec![
        async move {
            observed.notify_one();
            std::future::pending::<()>().await;
        }
        .boxed(),
        async move {
            fail.notified().await;
            panic!("injected eager child panic");
        }
        .boxed(),
    ];
    let caller_owner = owner.clone();
    let caller = tokio::spawn(async move { caller_owner.join_eager_batch(bodies).await });
    entered.notified().await;
    panic_now.notify_one();
    let result = caller.await.unwrap();
    assert!(matches!(result, Err(ref error) if error.contains("Panicked")));
    let report = owner
        .observe_until(Instant::now() + Duration::from_secs(1))
        .await;
    let eager = count(&report, McpWorkerKind::EagerStartup);
    assert_eq!(eager.panicked, 1);
    assert_eq!(eager.cancelled, 1);
    assert_eq!(eager.requested_cancelled, 1);
    assert_eq!(eager.joined + eager.pending, 0);
}

#[tokio::test]
async fn eager_results_do_not_remain_as_ordinary_holders_in_the_join_ledger() {
    let owner = McpWorkerOwner::default();
    let resource = Arc::new(());
    let weak = Arc::downgrade(&resource);
    let result = owner
        .join_eager_batch(vec![async move { resource }.boxed()])
        .await
        .unwrap()
        .unwrap();
    assert!(weak.upgrade().is_some());
    drop(result);
    // No extra spawn, observation or compaction is needed to release outputs.
    assert!(weak.upgrade().is_none());
    let report = owner
        .observe_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(count(&report, McpWorkerKind::EagerStartup).joined, 1);
}

#[tokio::test]
async fn ema_abort_guard_follows_final_ordinary_owner_and_retains_original_join() {
    let owner = McpWorkerOwner::default();
    let entered = Arc::new(Notify::new());
    let seen = Arc::clone(&entered);
    let resource = Arc::new(());
    let weak = Arc::downgrade(&resource);
    let guard = Arc::new(
        owner
            .spawn_abort_on_drop(
                &Handle::current(),
                McpWorkerKind::EmaRevocation,
                async move {
                    let _resource = resource;
                    seen.notify_one();
                    std::future::pending::<()>().await;
                },
            )
            .unwrap(),
    );
    entered.notified().await;
    let other = Arc::clone(&guard);
    drop(guard);
    let held = owner.observe_until(Instant::now()).await;
    assert_eq!(count(&held, McpWorkerKind::EmaRevocation).pending, 1);
    assert!(weak.upgrade().is_some());
    drop(other);
    let report = owner
        .observe_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(count(&report, McpWorkerKind::EmaRevocation).cancelled, 1);
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn stop_request_is_not_completion_and_survives_dropped_observer() {
    let owner = McpWorkerOwner::default();
    let guard = owner
        .spawn_abort_on_drop(
            &Handle::current(),
            McpWorkerKind::AuthNotification,
            std::future::pending(),
        )
        .unwrap();
    guard.stop();
    let unobserved = owner.observe_until(Instant::now()).await;
    let unobserved = count(&unobserved, McpWorkerKind::AuthNotification);
    assert_eq!(unobserved.pending, 1);
    assert_eq!(unobserved.cancelled + unobserved.requested_cancelled, 0);
    let observing = owner.clone();
    let observer = tokio::spawn(async move {
        observing
            .observe_until(Instant::now() + Duration::from_secs(1))
            .await
    });
    observer.abort();
    assert!(observer.await.unwrap_err().is_cancelled());
    let settled = owner
        .observe_until(Instant::now() + Duration::from_secs(1))
        .await;
    let settled = count(&settled, McpWorkerKind::AuthNotification);
    assert_eq!(settled.cancelled, 1);
    assert_eq!(settled.requested_cancelled, 1);
    drop(guard);
    let replay = owner.observe_until(Instant::now()).await;
    assert_eq!(count(&replay, McpWorkerKind::AuthNotification), settled);
}

#[tokio::test]
async fn late_guard_drop_does_not_reclassify_unrequested_cancellation() {
    let owner = McpWorkerOwner::default();
    let guard = owner
        .spawn_abort_on_drop(
            &Handle::current(),
            McpWorkerKind::EmaRevocation,
            std::future::pending(),
        )
        .unwrap();
    // Simulate cancellation outside the retained stop authority.
    guard.abort.handle.abort();
    let first = owner
        .observe_until(Instant::now() + Duration::from_secs(1))
        .await;
    let first = count(&first, McpWorkerKind::EmaRevocation);
    assert_eq!(first.cancelled, 1);
    assert_eq!(first.requested_cancelled, 0);
    guard.stop();
    drop(guard);
    drop(owner.spawn(&Handle::current(), McpWorkerKind::Reconnect, async {}));
    let replay = owner
        .observe_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(count(&replay, McpWorkerKind::EmaRevocation), first);
}

#[tokio::test]
async fn stop_requests_do_not_relabel_natural_exit_or_panic() {
    let owner = McpWorkerOwner::default();
    let natural = owner
        .spawn_abort_on_drop(&Handle::current(), McpWorkerKind::AuthNotification, async {
        })
        .unwrap();
    let panicked = owner
        .spawn_abort_on_drop(&Handle::current(), McpWorkerKind::EmaRevocation, async {
            panic!("injected watcher panic");
        })
        .unwrap();
    let first = owner
        .observe_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(count(&first, McpWorkerKind::AuthNotification).joined, 1);
    assert_eq!(count(&first, McpWorkerKind::EmaRevocation).panicked, 1);
    natural.stop();
    panicked.stop();
    drop(natural);
    drop(panicked);
    let replay = owner.observe_until(Instant::now()).await;
    assert_eq!(replay.counts, first.counts);
    assert!(
        replay
            .counts
            .iter()
            .all(|c| c.cancelled + c.requested_cancelled == 0)
    );
}

#[tokio::test]
async fn closed_worker_admission_refuses_startup_but_keeps_actual_cleanup_joins() {
    let owner = McpWorkerOwner::default();
    let called = Arc::new(AtomicUsize::new(0));
    owner.close_startup_admission();
    for kind in [
        McpWorkerKind::StartupSummary,
        McpWorkerKind::DeferredStartup,
        McpWorkerKind::Reconnect,
        McpWorkerKind::AuthNotification,
        McpWorkerKind::EagerStartup,
        McpWorkerKind::EmaRevocation,
    ] {
        let observed = Arc::clone(&called);
        assert!(matches!(
            owner
                .spawn(&Handle::current(), kind, async move {
                    observed.fetch_add(1, Ordering::SeqCst);
                })
                .await,
            WorkerExit::NotAdmitted
        ));
    }
    assert!(
        owner
            .spawn_abort_on_drop(
                &Handle::current(),
                McpWorkerKind::AuthNotification,
                std::future::pending()
            )
            .is_err()
    );
    let observed = Arc::clone(&called);
    assert!(
        owner
            .spawn_result(&Handle::current(), McpWorkerKind::Reconnect, async move {
                observed.fetch_add(1, Ordering::SeqCst)
            })
            .await
            .is_err()
    );
    assert_eq!(called.load(Ordering::SeqCst), 0);
    for kind in [
        McpWorkerKind::DiscoveryCleanup,
        McpWorkerKind::ConnectionShutdown,
        McpWorkerKind::RuntimeShutdown,
    ] {
        let observed = Arc::clone(&called);
        assert!(matches!(
            owner
                .spawn(&Handle::current(), kind, async move {
                    observed.fetch_add(1, Ordering::SeqCst);
                })
                .await,
            WorkerExit::Joined
        ));
    }
    let report = owner
        .observe_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert!(report.startup_admission_closed);
    assert_eq!(report.refused_at_end, 8);
    assert_eq!(report.registered_at_end, 3);
    assert_eq!(called.load(Ordering::SeqCst), 3);
    assert!(
        report
            .counts
            .iter()
            .all(|c| c.pending + c.cancelled + c.panicked == 0)
    );
}

#[tokio::test]
async fn eager_admission_closure_aborts_only_already_born_children_without_summary_panic() {
    let owner = McpWorkerOwner::default();
    let called = Arc::new(AtomicUsize::new(0));
    let closing = owner.clone();
    let bodies = (0..2).map(|index| {
        if index == 1 {
            closing.close_startup_admission();
        }
        let called = Arc::clone(&called);
        async move {
            called.fetch_add(1, Ordering::SeqCst);
            std::future::pending::<()>().await;
        }
    });
    assert!(owner.join_eager_batch(bodies).await.unwrap().is_none());
    let report = owner
        .observe_until(Instant::now() + Duration::from_secs(1))
        .await;
    let eager = count(&report, McpWorkerKind::EagerStartup);
    assert_eq!(report.registered_at_end, 1);
    assert_eq!(report.refused_at_end, 1);
    assert_eq!(eager.cancelled, 1);
    assert_eq!(eager.requested_cancelled, 1);
    assert_eq!(eager.pending + eager.panicked + eager.joined, 0);
    assert_eq!(called.load(Ordering::SeqCst), 0);
}
