use super::*;
use crate::runtime::test_support::unique_temp_dir;
use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;
use std::sync::Arc;

async fn runtime() -> Arc<StateRuntime> {
    let home = unique_temp_dir();
    StateRuntime::init(
        crate::SqliteConfig::new_for_testing(home.as_path().abs()),
        "test-provider".to_string(),
    )
    .await
    .expect("state runtime")
}

fn reserved_record(outcome: ClearTransitionReserveOutcome) -> ClearTransitionRecord {
    let ClearTransitionReserveOutcome::Reserved(record) = outcome else {
        panic!("expected a new reservation");
    };
    record
}

#[tokio::test]
async fn migration_creates_clear_transition_store_on_an_empty_database() {
    let runtime = runtime().await;

    let table: String = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'clear_transitions'",
    )
    .fetch_one(runtime.pool.as_ref())
    .await
    .unwrap();

    assert_eq!("clear_transitions", table);
}

#[tokio::test]
async fn reservation_is_stable_and_unique_for_both_thread_identities() {
    let runtime = runtime().await;
    let transition_id = ClearTransitionId::new();
    let predecessor = ThreadId::new();
    let successor = ThreadId::new();
    let record = reserved_record(
        runtime
            .reserve_clear_transition(transition_id, predecessor, successor)
            .await
            .unwrap(),
    );

    assert_eq!(transition_id, record.transition_id);
    assert_eq!(predecessor, record.predecessor_thread_id);
    assert_eq!(successor, record.successor_thread_id);
    assert_eq!(ClearTransitionPhase::Reserved, record.phase);

    let repeated = runtime
        .reserve_clear_transition(ClearTransitionId::new(), predecessor, ThreadId::new())
        .await
        .unwrap();
    assert_eq!(
        ClearTransitionReserveOutcome::PredecessorAlreadyReserved(record.clone()),
        repeated
    );

    let repeated = runtime
        .reserve_clear_transition(ClearTransitionId::new(), ThreadId::new(), successor)
        .await
        .unwrap();
    assert_eq!(
        ClearTransitionReserveOutcome::SuccessorAlreadyReserved(record),
        repeated
    );
}

#[tokio::test]
async fn concurrent_reservations_for_one_predecessor_have_one_winner() {
    let runtime = runtime().await;
    let other = StateRuntime::init(runtime.sqlite().clone(), "test-provider".to_string())
        .await
        .unwrap();
    let predecessor = ThreadId::new();
    let first_id = ClearTransitionId::new();
    let second_id = ClearTransitionId::new();
    let (first, second) = tokio::join!(
        runtime.reserve_clear_transition(first_id, predecessor, ThreadId::new()),
        other.reserve_clear_transition(second_id, predecessor, ThreadId::new()),
    );
    let outcomes = [first.unwrap(), second.unwrap()];

    assert_eq!(
        1,
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, ClearTransitionReserveOutcome::Reserved(_)))
            .count()
    );
    assert_eq!(
        1,
        outcomes
            .iter()
            .filter(|outcome| matches!(
                outcome,
                ClearTransitionReserveOutcome::PredecessorAlreadyReserved(_)
            ))
            .count()
    );
}

#[tokio::test]
async fn phases_only_advance_one_valid_step_from_the_expected_state() {
    let runtime = runtime().await;
    let transition_id = ClearTransitionId::new();
    reserved_record(
        runtime
            .reserve_clear_transition(transition_id, ThreadId::new(), ThreadId::new())
            .await
            .unwrap(),
    );

    assert!(
        runtime
            .advance_clear_transition_phase(
                transition_id,
                ClearTransitionPhase::Reserved,
                ClearTransitionPhase::SuccessorCreated,
            )
            .await
            .unwrap()
    );
    assert!(
        !runtime
            .advance_clear_transition_phase(
                transition_id,
                ClearTransitionPhase::Reserved,
                ClearTransitionPhase::SuccessorCreated,
            )
            .await
            .unwrap()
    );
    assert!(
        runtime
            .advance_clear_transition_phase(
                transition_id,
                ClearTransitionPhase::SuccessorCreated,
                ClearTransitionPhase::Completed,
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn abandoned_precreation_reservation_releases_both_identities_for_retry() {
    let runtime = runtime().await;
    let transition_id = ClearTransitionId::new();
    let predecessor = ThreadId::new();
    let reserved_successor = ThreadId::new();
    reserved_record(
        runtime
            .reserve_clear_transition(transition_id, predecessor, reserved_successor)
            .await
            .unwrap(),
    );

    assert!(
        runtime
            .abandon_clear_transition(transition_id, ClearTransitionPhase::Reserved)
            .await
            .unwrap()
    );
    assert!(
        runtime
            .get_clear_transition_by_predecessor(predecessor)
            .await
            .unwrap()
            .is_none()
    );
    assert!(runtime.list_incomplete_clear_transitions().await.unwrap().is_empty());

    reserved_record(
        runtime
            .reserve_clear_transition(
                ClearTransitionId::new(),
                predecessor,
                reserved_successor,
            )
            .await
            .unwrap(),
    );
}

#[tokio::test]
async fn transition_cannot_be_abandoned_after_successor_creation() {
    let runtime = runtime().await;
    let transition_id = ClearTransitionId::new();
    reserved_record(
        runtime
            .reserve_clear_transition(transition_id, ThreadId::new(), ThreadId::new())
            .await
            .unwrap(),
    );
    assert!(
        runtime
            .advance_clear_transition_phase(
                transition_id,
                ClearTransitionPhase::Reserved,
                ClearTransitionPhase::SuccessorCreated,
            )
            .await
            .unwrap()
    );

    assert!(
        runtime
            .abandon_clear_transition(
                transition_id,
                ClearTransitionPhase::SuccessorCreated,
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn start_evidence_cannot_be_claimed_before_end_evidence_is_terminal() {
    let runtime = runtime().await;
    let transition_id = ClearTransitionId::new();
    reserved_record(
        runtime
            .reserve_clear_transition(transition_id, ThreadId::new(), ThreadId::new())
            .await
            .unwrap(),
    );

    assert!(
        !runtime
            .advance_clear_transition_evidence(
                transition_id,
                ClearTransitionEvidenceKind::Start,
                ClearTransitionEvidenceState::Pending,
                ClearTransitionEvidenceState::Claimed,
            )
            .await
            .unwrap()
    );
    for (expected, next) in [
        (
            ClearTransitionEvidenceState::Pending,
            ClearTransitionEvidenceState::Claimed,
        ),
        (
            ClearTransitionEvidenceState::Claimed,
            ClearTransitionEvidenceState::Delivered,
        ),
    ] {
        assert!(
            runtime
                .advance_clear_transition_evidence(
                    transition_id,
                    ClearTransitionEvidenceKind::End,
                    expected,
                    next,
                )
                .await
                .unwrap()
        );
    }
    assert!(
        runtime
            .advance_clear_transition_evidence(
                transition_id,
                ClearTransitionEvidenceKind::Start,
                ClearTransitionEvidenceState::Pending,
                ClearTransitionEvidenceState::Claimed,
            )
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn incomplete_transition_is_recovered_after_runtime_restart() {
    let runtime = runtime().await;
    let transition_id = ClearTransitionId::new();
    let expected = reserved_record(
        runtime
            .reserve_clear_transition(transition_id, ThreadId::new(), ThreadId::new())
            .await
            .unwrap(),
    );
    let restarted = StateRuntime::init(runtime.sqlite().clone(), "test-provider".to_string())
        .await
        .unwrap();

    assert_eq!(
        vec![expected],
        restarted.list_incomplete_clear_transitions().await.unwrap()
    );
}
