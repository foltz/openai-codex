use super::*;
use futures::FutureExt;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn reopen_retry_observes_both_registered_and_not_yet_polled_waiters() {
    let permits = AccountWorkPermits::new();
    permits.close();
    let waiting = permits.wait_until_available();
    tokio::pin!(waiting);
    assert!(waiting.as_mut().now_or_never().is_none());
    permits.wake_waiters();
    assert!(waiting.as_mut().now_or_never().is_none());
    permits.reopen();
    assert_eq!(waiting.as_mut().now_or_never(), Some(()));

    permits.close();
    let not_polled = permits.wait_until_available();
    permits.reopen();
    assert_eq!(not_polled.now_or_never(), Some(()));
    assert_eq!(permits.admitted_count(), 0, "a wake is not an admission");
}

#[tokio::test]
async fn admitted_request_derives_a_turn_after_close_and_outlives_request_return() {
    use codex_extension_api::TurnStartAdmission;
    let coordinator = ManagedTransitionCoordinator::new();
    let admission = crate::account_turn_admission::AccountTurnAdmission {
        shutdown: crate::turn_admission::TurnAdmission::default(),
        permits: coordinator.account_work_permits(),
        work: coordinator.account_turn_work(),
    };
    let store = codex_extension_api::ExtensionData::new("thread");
    let request_guard = coordinator.try_acquire_account_work_permit().unwrap();
    let request = super::tests::request(coordinator.process_instance_id().await, "admitted-request");
    let transition = coordinator.start_dispatch(request, /*caller_authorized*/ true);
    tokio::pin!(transition);
    assert!(transition.as_mut().now_or_never().is_none());
    assert!(admission.admit_turn_work(&store, Box::pin(std::future::pending())).is_err());
    crate::account_turn_admission::within_request(Some(request_guard), async {
        let mut work = admission.admit_turn_work(&store, Box::pin(std::future::pending()))
            .unwrap().unwrap();
        work.bind_submission("late-turn");
        work.retain_until_terminal();
        assert_eq!(coordinator.account_work_permits().admitted_count(), 2);
    }).await;
    assert_eq!(coordinator.account_work_permits().admitted_count(), 1);
    assert!(transition.as_mut().now_or_never().is_none());
    admission.turn_work_terminal(&store, "late-turn");
    let result = transition.await;
    assert!(matches!(result, StartManagedTransitionResponse::Accepted { .. }));
    assert_eq!(coordinator.account_work_permits().admitted_count(), 0);
}

#[tokio::test]
async fn child_derives_before_parent_reply_and_keeps_custody_after_parent_terminal() {
    use codex_extension_api::TurnStartAdmission;
    let coordinator = ManagedTransitionCoordinator::new();
    let admission = crate::account_turn_admission::AccountTurnAdmission {
        shutdown: crate::turn_admission::TurnAdmission::default(),
        permits: coordinator.account_work_permits(),
        work: coordinator.account_turn_work(),
    };
    let parent = codex_extension_api::ExtensionData::new("parent");
    let child = codex_extension_api::ExtensionData::new("child");
    let mut pending = admission.admit_turn_work(&parent, Box::pin(std::future::pending()))
        .unwrap().unwrap();
    // The queue binds before handling; it has not yet returned Started.
    pending.bind_submission("parent-turn");
    coordinator.account_work_permits.close();
    let mut descendant = admission.derive_turn_work(
        &parent, "parent-turn", &child, Box::pin(std::future::pending()),
    ).unwrap().unwrap();
    descendant.bind_submission("child-turn");
    descendant.retain_until_terminal();
    pending.retain_until_terminal();
    admission.turn_work_terminal(&parent, "parent-turn");
    assert_eq!(coordinator.account_work_permits().admitted_count(), 1);
    assert!(admission.derive_turn_work(
        &parent, "parent-turn", &child, Box::pin(std::future::pending()),
    ).is_err());
    admission.turn_work_terminal(&child, "child-turn");
    assert_eq!(coordinator.account_work_permits().admitted_count(), 0);
}

#[tokio::test]
async fn coordinator_drain_observes_listenerless_loop_termination() {
    let coordinator = ManagedTransitionCoordinator::new();
    let (ended, termination) = tokio::sync::oneshot::channel();
    let session = coordinator.account_turn_work().session(async move {
        let _ = termination.await;
    }.boxed());
    session.begin(coordinator.try_acquire_account_work_permit().unwrap())
        .unwrap().bind("suspended-turn".into());
    let request = super::tests::request(coordinator.process_instance_id().await, "loop-drain");
    let start = coordinator.start_dispatch(request, /*caller_authorized*/ true);
    tokio::pin!(start);
    assert!(start.as_mut().now_or_never().is_none());
    assert_eq!(coordinator.account_work_permits().admitted_count(), 1);
    assert!(coordinator.try_acquire_account_work_permit().is_none());
    ended.send(()).unwrap();
    let response = tokio::time::timeout(Duration::from_secs(1), start).await.unwrap();
    let StartManagedTransitionResponse::Accepted { status } = response else {
        panic!("joined loop must release the drain's outstanding work");
    };
    assert_eq!(status.phase, ManagedTransitionPhase::Draining);
    assert_eq!(coordinator.account_work_permits().admitted_count(), 0);
}

#[tokio::test(start_paused = true)]
async fn live_listenerless_turn_times_out_without_releasing_or_cancelling_its_work() {
    let coordinator = ManagedTransitionCoordinator::new();
    let (ended, termination) = tokio::sync::oneshot::channel();
    let session = coordinator.account_turn_work().session(async move {
        let _ = termination.await;
    }.boxed());
    session.begin(coordinator.try_acquire_account_work_permit().unwrap())
        .unwrap().bind("held-turn".into());
    let request = super::tests::request(coordinator.process_instance_id().await, "held-loop");
    let response = coordinator.start_dispatch(request, /*caller_authorized*/ true).await;
    let StartManagedTransitionResponse::Accepted { status } = response else {
        panic!("admitted transition must retain a quarantine result");
    };
    assert_eq!(status.phase, ManagedTransitionPhase::Quarantined);
    assert_eq!(coordinator.account_work_permits().admitted_count(), 1);
    assert!(!ended.is_closed(), "deadline must not cancel the retained loop receipt");
    assert!(coordinator.try_acquire_account_work_permit().is_none());
    session.terminal("held-turn");
    assert_eq!(coordinator.account_work_permits().admitted_count(), 0);
}

#[test]
fn admitted_parent_can_derive_after_close_without_reopening_fresh_admission() {
    let permits = AccountWorkPermits::new();
    let parent = permits.try_acquire().unwrap();
    permits.close();
    let child = parent.try_derive().unwrap();
    assert_eq!(permits.admitted_count(), 2);
    assert!(permits.try_acquire().is_none());
    drop(parent);
    assert_eq!(permits.admitted_count(), 1);
    let descendant = child.try_derive().unwrap();
    drop(child);
    assert_eq!(permits.admitted_count(), 1);
    drop(descendant);
    assert_eq!(permits.admitted_count(), 0);
    assert!(permits.try_acquire().is_none());
}

#[test]
fn derived_work_stays_in_its_parents_registry() {
    let first = AccountWorkPermits::new();
    let second = AccountWorkPermits::new();
    let parent = first.try_acquire().unwrap();
    first.close();
    second.close();
    let child = parent.try_derive().unwrap();
    drop(parent);
    assert_eq!((first.admitted_count(), second.admitted_count()), (1, 0));
    drop(child);
    assert_eq!((first.admitted_count(), second.admitted_count()), (0, 0));
}

#[test]
fn exhausted_derivation_preserves_count_and_closure() {
    for closed in [0, ACCOUNT_WORK_CLOSED] {
        let permits = AccountWorkPermits::new();
        let parent = permits.try_acquire().unwrap();
        // Simulate count exhaustion while retaining one real parent. Restore
        // its actual population before dropping it; no fictitious guards exist.
        let exhausted = closed | ACCOUNT_WORK_COUNT_MASK;
        permits.inner.state.store(exhausted, Ordering::Release);
        assert!(parent.try_derive().is_none());
        assert_eq!(permits.inner.state.load(Ordering::Acquire), exhausted);
        permits.inner.state.store(closed | 1, Ordering::Release);
        drop(parent);
        assert_eq!(permits.inner.state.load(Ordering::Acquire), closed);
    }
}

#[tokio::test]
async fn derived_work_keeps_drain_nonzero_after_its_parent_finishes() {
    let permits = AccountWorkPermits::new();
    let parent = permits.try_acquire().unwrap();
    permits.close();
    let child = parent.try_derive().unwrap();
    drop(parent);
    let notified = permits.notified();
    assert_eq!(permits.admitted_count(), 1);
    drop(child);
    tokio::time::timeout(Duration::from_secs(1), notified)
        .await
        .unwrap();
    assert_eq!(permits.admitted_count(), 0);
}

#[test]
fn close_preserves_admitted_work_and_reopen_preserves_its_count() {
    let permits = AccountWorkPermits::new();
    let first = permits.try_acquire().unwrap();
    let second = permits.try_acquire().unwrap();
    permits.close();
    permits.close();
    assert!(permits.try_acquire().is_none());
    assert_eq!(permits.admitted_count(), 2);
    drop(first);
    assert_eq!(permits.admitted_count(), 1);
    permits.reopen();
    assert_eq!(permits.admitted_count(), 1);
    let third = permits.try_acquire().unwrap();
    assert_eq!(permits.admitted_count(), 2);
    permits.close();
    drop(second);
    drop(third);
    assert_eq!(permits.admitted_count(), 0);
    assert!(permits.try_acquire().is_none());
    permits.reopen();
    drop(permits.try_acquire().unwrap());
    assert_eq!(permits.admitted_count(), 0);
}

#[test]
fn exhausted_count_refuses_without_overflowing_into_the_closed_bit() {
    let permits = AccountWorkPermits::new();
    // Private representation fixture; no fabricated live guards are dropped.
    permits
        .inner
        .state
        .store(ACCOUNT_WORK_COUNT_MASK, Ordering::Release);
    assert!(permits.try_acquire().is_none());
    assert_eq!(
        permits.inner.state.load(Ordering::Acquire),
        ACCOUNT_WORK_COUNT_MASK
    );
    permits.close();
    assert_eq!(permits.admitted_count(), ACCOUNT_WORK_COUNT_MASK);
    assert!(permits.try_acquire().is_none());
    permits.reopen();
    assert_eq!(
        permits.inner.state.load(Ordering::Acquire),
        ACCOUNT_WORK_COUNT_MASK
    );
}

#[tokio::test]
async fn release_to_zero_wakes_an_observer_created_before_count_check() {
    let permits = AccountWorkPermits::new();
    let permit = permits.try_acquire().unwrap();
    permits.close();
    let notified = permits.notified();
    assert_eq!(permits.admitted_count(), 1);
    drop(permit);
    tokio::time::timeout(Duration::from_secs(1), notified)
        .await
        .unwrap();
    assert_eq!(permits.admitted_count(), 0);
    assert!(permits.try_acquire().is_none());
}

#[test]
fn concurrent_close_either_refuses_acquisition_or_observes_its_live_guard() {
    // Supplementary scheduling coverage, not a weak-memory model. The proof
    // relies on both operations modifying the same atomic word in production.
    for _ in 0..128 {
        let permits = AccountWorkPermits::new();
        let start = std::sync::Barrier::new(2);
        let (permit, count_at_close) = std::thread::scope(|scope| {
            let acquire = scope.spawn(|| {
                start.wait();
                permits.try_acquire()
            });
            let close = scope.spawn(|| {
                start.wait();
                permits.close();
                permits.admitted_count()
            });
            // The returned guard stays alive in the join result, so a zero
            // count cannot be explained by a successful caller releasing it.
            (acquire.join().unwrap(), close.join().unwrap())
        });
        assert_eq!(count_at_close, u64::from(permit.is_some()));
        assert_eq!(permits.admitted_count(), count_at_close);
        assert!(permits.try_acquire().is_none());
        drop(permit);
        assert_eq!(permits.admitted_count(), 0);
    }
}
