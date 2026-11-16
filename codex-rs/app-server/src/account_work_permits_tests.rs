use super::*;
use futures::FutureExt;
use pretty_assertions::assert_eq;
use tokio::time::Duration;

#[tokio::test]
async fn operation_outlives_origin_loop_and_derives_child_during_drain() {
    use codex_extension_api::TurnStartAdmission;
    let permits = AccountWorkPermits::new();
    let registry = crate::account_turn_work::AccountTurnWork::default();
    let admission = crate::account_turn_admission::AccountTurnAdmission {
        shutdown: crate::turn_admission::TurnAdmission::default(),
        permits: permits.clone(),
        work: registry.clone(),
    };
    let parent = codex_extension_api::ExtensionData::new("parent");
    let (ended, termination) = tokio::sync::oneshot::channel();
    let mut turn = admission
        .admit_turn_work(
            &parent,
            Box::pin(async move {
                let _ = termination.await;
            }),
        )
        .unwrap()
        .unwrap();
    turn.bind_submission("parent-turn");
    turn.retain_until_terminal();
    let operation = admission
        .derive_operation_work(&parent, "parent-turn")
        .unwrap()
        .unwrap();
    permits.close();
    ended.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), registry.observe_terminated())
        .await
        .unwrap();
    assert_eq!(
        permits.admitted_count(),
        1,
        "loop exit must not retire the operation"
    );

    let descendant = operation.derive_operation().unwrap();
    drop(operation);
    let child = codex_extension_api::ExtensionData::new("child");
    let mut child_turn = descendant
        .derive_turn_work(&child, Box::pin(std::future::pending()))
        .unwrap();
    child_turn.bind_submission("child-turn");
    child_turn.retain_until_terminal();
    drop(descendant);
    assert_eq!(
        permits.admitted_count(),
        1,
        "accepted child owns independent custody"
    );
    admission.turn_work_terminal(&child, "child-turn");
    assert_eq!(permits.admitted_count(), 0);
}

#[tokio::test]
async fn optional_background_admission_is_fresh_even_inside_an_admitted_request() {
    use codex_extension_api::TurnStartAdmission;
    let permits = AccountWorkPermits::new();
    let admission = crate::account_turn_admission::AccountTurnAdmission {
        shutdown: crate::turn_admission::TurnAdmission::default(),
        permits: permits.clone(),
        work: crate::account_turn_work::AccountTurnWork::default(),
    };
    let operation = admission.admit_operation_work().unwrap().unwrap();
    crate::account_turn_admission::within_request(permits.try_acquire(), async {
        permits.close();
        assert!(matches!(
            admission.admit_operation_work(),
            Err(codex_extension_api::TurnWorkRefused::RetryAfter(_))
        ));
        assert_eq!(permits.admitted_count(), 2);
    })
    .await;
    assert_eq!(permits.admitted_count(), 1);
    drop(operation);
    assert_eq!(permits.admitted_count(), 0);
}

#[tokio::test]
async fn request_operation_capture_derives_during_drain_without_a_turn_entry() {
    use codex_extension_api::TurnStartAdmission;
    let permits = AccountWorkPermits::new();
    let admission = crate::account_turn_admission::AccountTurnAdmission {
        shutdown: crate::turn_admission::TurnAdmission::default(),
        permits: permits.clone(),
        work: crate::account_turn_work::AccountTurnWork::default(),
    };
    assert!(admission.derive_request_operation_work().unwrap().is_none());
    let operation = crate::account_turn_admission::within_request(permits.try_acquire(), async {
        permits.close();
        admission.derive_request_operation_work().unwrap().unwrap()
    })
    .await;
    assert_eq!(permits.admitted_count(), 1);
    assert!(permits.try_acquire().is_none());
    drop(operation);
    assert_eq!(permits.admitted_count(), 0);
}

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
async fn child_derives_before_parent_reply_and_keeps_custody_after_parent_terminal() {
    use codex_extension_api::TurnStartAdmission;
    let permits = AccountWorkPermits::new();
    let admission = crate::account_turn_admission::AccountTurnAdmission {
        shutdown: crate::turn_admission::TurnAdmission::default(),
        permits: permits.clone(),
        work: crate::account_turn_work::AccountTurnWork::default(),
    };
    let parent = codex_extension_api::ExtensionData::new("parent");
    let child = codex_extension_api::ExtensionData::new("child");
    let mut pending = admission
        .admit_turn_work(&parent, Box::pin(std::future::pending()))
        .unwrap()
        .unwrap();
    // The queue binds before handling; it has not yet returned Started.
    pending.bind_submission("parent-turn");
    permits.close();
    let mut descendant = admission
        .derive_turn_work(
            &parent,
            "parent-turn",
            &child,
            Box::pin(std::future::pending()),
        )
        .unwrap()
        .unwrap();
    descendant.bind_submission("child-turn");
    descendant.retain_until_terminal();
    pending.retain_until_terminal();
    admission.turn_work_terminal(&parent, "parent-turn");
    assert_eq!(permits.clone().admitted_count(), 1);
    assert!(
        admission
            .derive_turn_work(
                &parent,
                "parent-turn",
                &child,
                Box::pin(std::future::pending()),
            )
            .is_err()
    );
    admission.turn_work_terminal(&child, "child-turn");
    assert_eq!(permits.clone().admitted_count(), 0);
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
