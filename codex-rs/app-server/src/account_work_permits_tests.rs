use super::*;
use pretty_assertions::assert_eq;

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
