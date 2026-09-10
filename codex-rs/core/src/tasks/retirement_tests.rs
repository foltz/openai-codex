use super::*;
use pretty_assertions::assert_eq;
use std::time::Duration;

#[tokio::test]
async fn handle_keeps_its_exact_failed_join_after_registry_compaction() {
    let registry = TaskJoinRegistry::default();
    let handle = registry.register(tokio::spawn(async { panic!("specific join fixture") }));
    assert!(!handle.wait().await);
    assert_eq!(
        registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await,
        TaskJoinOutcome::Complete { panicked: true }
    );
    assert!(registry.state.lock().expect("state").entries.is_empty());
    handle.abort();
    assert!(
        !handle.wait().await,
        "compaction must not replace this task's failed receipt with success"
    );
}

#[tokio::test(start_paused = true)]
async fn settled_join_replays_after_deadline_without_reobserving() {
    let registry = TaskJoinRegistry::default();
    let handle = registry.register(tokio::spawn(async {}));
    let completion = registry.state.lock().expect("state").entries[0]
        .completion
        .clone();
    assert!(completion.await);
    handle.detach();
    let deadline = Instant::now();
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(
        registry.shutdown_until(deadline).await,
        TaskJoinOutcome::Complete { panicked: false }
    );
    assert!(registry.state.lock().expect("state").entries.is_empty());
}

#[tokio::test]
async fn many_completed_tasks_retain_only_the_latest_join() {
    let registry = TaskJoinRegistry::default();
    for _ in 0..64 {
        let handle = registry.register(tokio::spawn(async {}));
        let completion = {
            let state = registry.state.lock().expect("state");
            assert_eq!(state.entries.len(), 1);
            state.entries[0].completion.clone()
        };
        assert!(completion.await);
        handle.detach();
    }
    assert_eq!(
        registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await,
        TaskJoinOutcome::Complete { panicked: false }
    );
    assert!(registry.state.lock().expect("state").entries.is_empty());
}

#[tokio::test]
async fn dropping_active_capability_aborts_but_does_not_discard_join() {
    let registry = TaskJoinRegistry::default();
    let handle = registry.register(tokio::spawn(std::future::pending::<()>()));
    let completion = registry.state.lock().expect("state").entries[0]
        .completion
        .clone();
    assert!(completion.clone().now_or_never().is_none());
    drop(handle);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), completion)
            .await
            .expect("abort joined")
    );
    assert_eq!(
        registry.state.lock().expect("state").entries.len(),
        1,
        "observing a clone must not remove the registry's original proof"
    );
}

#[tokio::test]
async fn detached_active_handle_still_has_an_observable_join() {
    let registry = TaskJoinRegistry::default();
    registry
        .register(tokio::spawn(std::future::pending::<()>()))
        .detach();
    assert_eq!(
        registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await,
        TaskJoinOutcome::Complete { panicked: false }
    );
    assert!(registry.state.lock().expect("state").entries.is_empty());
    assert_eq!(
        registry.shutdown_until(Instant::now()).await,
        TaskJoinOutcome::Complete { panicked: false }
    );
}

#[tokio::test(start_paused = true)]
async fn expired_observer_retains_join_for_retry() {
    let registry = TaskJoinRegistry::default();
    let handle = registry.register(tokio::spawn(std::future::pending::<()>()));
    assert_eq!(
        registry.shutdown_until(Instant::now()).await,
        TaskJoinOutcome::TimedOut
    );
    assert!(!handle.abort.is_finished());
    assert_eq!(
        registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await,
        TaskJoinOutcome::Complete { panicked: false }
    );
    assert!(handle.abort.is_finished());
}

#[tokio::test]
async fn completed_join_compaction_keeps_failure_sticky() {
    let registry = TaskJoinRegistry::default();
    let handle = registry.register(tokio::spawn(async { panic!("task failure fixture") }));
    while !handle.abort.is_finished() {
        tokio::task::yield_now().await;
    }
    handle.detach();
    let next = registry.register(tokio::spawn(std::future::pending::<()>()));
    assert_eq!(registry.state.lock().expect("state").entries.len(), 1);
    assert_eq!(
        registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await,
        TaskJoinOutcome::Complete { panicked: true }
    );
    drop(next);
}
