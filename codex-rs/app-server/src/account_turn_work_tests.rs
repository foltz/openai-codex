use super::*;
use crate::managed_transition::AccountWorkPermits;
use pretty_assertions::assert_eq;
use std::time::Duration;
use tokio::sync::oneshot;

fn session(registry: &AccountTurnWork) -> (AccountTurnSession, oneshot::Sender<()>) {
    let (ended, termination) = oneshot::channel();
    let session = registry.session(
        async move {
            let _ = termination.await;
        }
        .boxed(),
    );
    (session, ended)
}

#[test]
fn logical_terminal_releases_all_matching_leases_but_not_another_turn() {
    let registry = AccountTurnWork::default();
    let permits = AccountWorkPermits::new();
    let (session, _ended) = session(&registry);
    session
        .begin(permits.try_acquire().unwrap())
        .unwrap()
        .bind("one".into());
    let child = session.derive("one").unwrap();
    session.begin(child).unwrap().bind("one".into());
    session
        .begin(permits.try_acquire().unwrap())
        .unwrap()
        .bind("two".into());
    assert_eq!(permits.admitted_count(), 3);
    session.terminal("one");
    session.terminal("one");
    assert_eq!(permits.admitted_count(), 1);
    assert!(session.derive("one").is_none());
    session.terminal("two");
    assert_eq!(permits.admitted_count(), 0);
    assert!(registry.inner.sessions.lock().unwrap().is_empty());
}

#[test]
fn terminal_before_multiple_bindings_releases_each_matching_submission() {
    let registry = AccountTurnWork::default();
    let permits = AccountWorkPermits::new();
    let (session, _ended) = session(&registry);
    let first = session.begin(permits.try_acquire().unwrap()).unwrap();
    let second = session.begin(permits.try_acquire().unwrap()).unwrap();
    let unrelated = session.begin(permits.try_acquire().unwrap()).unwrap();
    session.terminal("fast");
    first.bind("fast".into());
    second.bind("fast".into());
    unrelated.bind("other".into());
    assert_eq!(permits.admitted_count(), 1);
    session.terminal("other");
    assert_eq!(permits.admitted_count(), 0);
}

#[test]
fn failed_submission_drops_only_its_pending_slot() {
    let registry = AccountTurnWork::default();
    let permits = AccountWorkPermits::new();
    let (session, _ended) = session(&registry);
    session
        .begin(permits.try_acquire().unwrap())
        .unwrap()
        .bind("running".into());
    let failed = session.begin(permits.try_acquire().unwrap()).unwrap();
    drop(failed);
    assert_eq!(permits.admitted_count(), 1);
    session.terminal("running");
    assert_eq!(permits.admitted_count(), 0);
}

#[tokio::test]
async fn cancelled_drain_observation_preserves_work_until_loop_termination() {
    let registry = AccountTurnWork::default();
    let permits = AccountWorkPermits::new();
    let (session, ended) = session(&registry);
    session
        .begin(permits.try_acquire().unwrap())
        .unwrap()
        .bind("suspended".into());
    assert!(registry.observe_terminated().now_or_never().is_none());
    drop(session);
    assert_eq!(permits.admitted_count(), 1);
    ended.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), registry.observe_terminated())
        .await
        .unwrap();
    assert_eq!(permits.admitted_count(), 0);
}

#[tokio::test]
async fn old_loop_cannot_release_recovered_turn_in_a_new_loop() {
    let registry = AccountTurnWork::default();
    let permits = AccountWorkPermits::new();
    let (old, old_ended) = session(&registry);
    let (new, _new_ended) = session(&registry);
    old.begin(permits.try_acquire().unwrap())
        .unwrap()
        .bind("same-turn-id".into());
    new.begin(permits.try_acquire().unwrap())
        .unwrap()
        .bind("same-turn-id".into());
    old_ended.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), registry.observe_terminated())
        .await
        .unwrap();
    old.terminal("same-turn-id");
    assert_eq!(permits.admitted_count(), 1);
    assert!(old.begin(permits.try_acquire().unwrap()).is_none());
    assert_eq!(permits.admitted_count(), 1);
    new.terminal("same-turn-id");
    assert_eq!(permits.admitted_count(), 0);
}

#[tokio::test]
async fn terminated_parent_cannot_derive_before_drain_observes_its_receipt() {
    let registry = AccountTurnWork::default();
    let permits = AccountWorkPermits::new();
    let (parent, ended) = session(&registry);
    parent
        .begin(permits.try_acquire().unwrap())
        .unwrap()
        .bind("parent".into());
    ended.send(()).unwrap();
    assert!(parent.derive("parent").is_none());
    assert_eq!(permits.admitted_count(), 1);
    tokio::time::timeout(Duration::from_secs(1), registry.observe_terminated())
        .await
        .unwrap();
    assert_eq!(permits.admitted_count(), 0);
}

#[tokio::test]
async fn drain_observer_notices_a_session_registered_after_it_started() {
    let registry = AccountTurnWork::default();
    let permits = AccountWorkPermits::new();
    let observe = registry.observe_terminated();
    tokio::pin!(observe);
    assert!(observe.as_mut().now_or_never().is_none());
    let (session, ended) = session(&registry);
    session
        .begin(permits.try_acquire().unwrap())
        .unwrap()
        .bind("late".into());
    ended.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), observe)
        .await
        .unwrap();
    assert_eq!(permits.admitted_count(), 0);
}
