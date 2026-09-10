use super::*;
use crate::outgoing_message::ConnectionId;
use crate::thread_state::ConnectionCapabilities;
use crate::thread_state::RetentionAcquireOutcome;
use crate::thread_state::RetentionAuthorityError;
use crate::thread_state::RetentionPrincipalId;
use crate::thread_state::RetentionReleaseOutcome;
use pretty_assertions::assert_eq;
use std::sync::Arc;

#[tokio::test]
async fn processor_shutdown_lifecycle_lock_cannot_starve_runtime_cleanup() {
    let (_home, manager, config) = crate::request_processors::thread_shutdown_fixture().await;
    let started = manager
        .start_thread(codex_core::StartThreadOptions::new(config))
        .await
        .unwrap();
    let state = ThreadStateManager::new();
    let held = state.state.lock().await;
    let owner = crate::request_processors::ThreadShutdownOwner::default();
    let deadline = Instant::now() + std::time::Duration::from_secs(20);
    let ticket = owner.begin(&manager, &state, deadline).unwrap();
    let observer = ticket.clone();
    let driver = tokio::spawn(async move { observer.wait().await });
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !manager.list_thread_ids().await.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("manager must retire while lifecycle table remains held");
    assert!(!driver.is_finished());
    driver.abort();
    assert!(driver.await.unwrap_err().is_cancelled());
    drop(held);
    let report = ticket.wait().await;
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(report.prior, Some(Vec::new()));
    assert!(state.state.lock().await.retirement_claims_closed);
    assert_eq!(ticket.deadline(), deadline);
    drop(started);
}

#[tokio::test]
async fn processor_shutdown_preserves_actual_prior_ticket_after_observation_removal() {
    let (_home, manager, config) = crate::request_processors::thread_shutdown_fixture().await;
    let old = manager
        .start_thread(codex_core::StartThreadOptions::new(config.clone()))
        .await
        .unwrap();
    let original_deadline = Instant::now() + std::time::Duration::from_secs(20);
    let prior = old.thread.begin_retirement(original_deadline).unwrap();
    let original = prior.wait().await;
    assert_eq!(
        original.cleanup,
        codex_core::ThreadCleanupOutcome::Finished {
            persistence_failed: false
        }
    );
    let state = ThreadStateManager::new();
    state.thread_state(old.thread_id).await;
    let generation = {
        let mut locked = state.state.lock().await;
        let record = locked.lifecycle.get_mut(&old.thread_id).unwrap();
        // Install an actual already-completed core ticket in the retained
        // lifecycle row. This test concerns draining, not claim authorization.
        record.ticket = Some(prior.clone());
        record
            .snapshot
            .send_modify(|snapshot| snapshot.retiring = true);
        record.snapshot.borrow().generation
    };
    state.remove_thread_state(old.thread_id).await;
    let current = manager
        .start_thread(codex_core::StartThreadOptions::new(config))
        .await
        .unwrap();
    let owner = crate::request_processors::ThreadShutdownOwner::default();
    let ticket = owner.begin(&manager, &state, original_deadline).unwrap();
    let report = ticket.wait().await;
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(
        report.prior,
        Some(vec![(old.thread_id, generation, original)])
    );
    assert_eq!(prior.deadline(), original_deadline);
    assert_eq!(prior.wait().await, original);
    assert!(manager.list_thread_ids().await.is_empty());
    drop((old, current));
}

async fn fixture() -> (ThreadStateManager, ThreadId, RetentionPrincipalId) {
    let manager = ThreadStateManager::new();
    let thread_id = ThreadId::new();
    let principal = RetentionPrincipalId::connection_owned();
    manager
        .connection_initialized(
            ConnectionId(1),
            ConnectionCapabilities {
                retention_principal: Some(principal),
                trusted_interactive: true,
                ..ConnectionCapabilities::default()
            },
        )
        .await;
    manager.thread_state(thread_id).await;
    (manager, thread_id, principal)
}

#[tokio::test]
async fn zero_grant_observation_does_not_block_or_change_retirement_authority() {
    let (manager, thread_id, _) = fixture().await;
    manager
        .try_add_connection_to_thread(thread_id, ConnectionId(1))
        .await
        .expect("observe");
    let before = serde_json::to_value(manager.thread_attachment_list().await).expect("snapshot");
    let watch = manager
        .subscribe_to_retention(thread_id)
        .await
        .expect("watch");
    let expected = *watch.borrow();
    assert!(
        manager
            .claim_retention_retirement(thread_id, expected)
            .await
            .is_ok()
    );
    assert_eq!(
        manager.subscribed_connection_ids(thread_id).await,
        vec![ConnectionId(1)]
    );
    assert_eq!(
        serde_json::to_value(manager.thread_attachment_list().await).expect("snapshot"),
        before
    );
}

#[tokio::test]
async fn missing_or_recreated_lifecycle_cannot_rearm_a_stale_claim() {
    let (manager, thread_id, principal) = fixture().await;
    let watch = manager
        .subscribe_to_retention(thread_id)
        .await
        .expect("watch");
    let stale = *watch.borrow();
    manager.remove_thread_state(thread_id).await;
    manager.thread_state(thread_id).await;
    assert_eq!(
        manager.claim_retention_retirement(thread_id, stale).await,
        Err(RetentionRetirementRefusal::Changed)
    );
    manager.state.lock().await.lifecycle.remove(&thread_id);
    manager.thread_state(thread_id).await;
    assert!(manager.subscribe_to_retention(thread_id).await.is_none());
    assert_eq!(
        manager.acquire_retention(thread_id, principal).await,
        Err(RetentionAuthorityError::AuthorityUnavailable)
    );
    assert_eq!(
        manager.claim_retention_retirement(thread_id, stale).await,
        Err(RetentionRetirementRefusal::UnknownThread)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acquisition_and_retirement_claim_have_one_atomic_winner() {
    let (manager, thread_id, principal) = fixture().await;
    let watch = manager
        .subscribe_to_retention(thread_id)
        .await
        .expect("watch");
    let expected = *watch.borrow();
    let gate = Arc::new(tokio::sync::Barrier::new(2));
    let acquiring = manager.clone();
    let acquiring_gate = Arc::clone(&gate);
    let acquire = tokio::spawn(async move {
        acquiring_gate.wait().await;
        acquiring.acquire_retention(thread_id, principal).await
    });
    let retiring = manager.clone();
    let retire = tokio::spawn(async move {
        gate.wait().await;
        retiring
            .claim_retention_retirement(thread_id, expected)
            .await
    });
    match (
        acquire.await.expect("acquire"),
        retire.await.expect("retire"),
    ) {
        (
            Ok(RetentionAcquireOutcome::Acquired { .. }),
            Err(RetentionRetirementRefusal::Retained),
        ) => {
            assert!(watch.borrow().granted);
            assert!(!watch.borrow().retiring);
        }
        (Err(RetentionAuthorityError::LifecycleClosed), Ok(claim)) => {
            assert_eq!(claim.thread_id, thread_id);
            assert!(watch.borrow().retiring);
            assert!(!watch.borrow().granted);
        }
        result => panic!("acquire and retirement cannot both win: {result:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn unseen_acquire_release_invalidates_the_unretained_snapshot() {
    let (manager, thread_id, principal) = fixture().await;
    let watch = manager
        .subscribe_to_retention(thread_id)
        .await
        .expect("watch");
    let expected = *watch.borrow();
    let RetentionAcquireOutcome::Acquired { grant_id } = manager
        .acquire_retention(thread_id, principal)
        .await
        .expect("grant")
    else {
        panic!("new grant");
    };
    assert_eq!(
        manager
            .release_retention(thread_id, principal, &grant_id)
            .await,
        Ok(RetentionReleaseOutcome::Released)
    );
    assert_eq!(
        watch.borrow().unretained_since,
        expected.unretained_since,
        "same paused instant deliberately defeats timestamp-only CAS"
    );
    assert_eq!(
        manager
            .claim_retention_retirement(thread_id, expected)
            .await,
        Err(RetentionRetirementRefusal::Changed)
    );
    let current = *watch.borrow();
    assert!(
        manager
            .claim_retention_retirement(thread_id, current)
            .await
            .is_ok()
    );
}

#[tokio::test(start_paused = true)]
async fn only_final_grant_loss_starts_the_unretained_clock() {
    let (manager, thread_id, first) = fixture().await;
    let second = RetentionPrincipalId::connection_owned();
    manager
        .connection_initialized(
            ConnectionId(2),
            ConnectionCapabilities {
                retention_principal: Some(second),
                trusted_interactive: true,
                ..ConnectionCapabilities::default()
            },
        )
        .await;
    let watch = manager.subscribe_to_retention(thread_id).await.unwrap();
    let initial = *watch.borrow();
    let RetentionAcquireOutcome::Acquired { grant_id: first_id } =
        manager.acquire_retention(thread_id, first).await.unwrap()
    else {
        panic!("first grant")
    };
    let RetentionAcquireOutcome::Acquired {
        grant_id: second_id,
    } = manager.acquire_retention(thread_id, second).await.unwrap()
    else {
        panic!("second grant")
    };
    let retained = *watch.borrow();
    tokio::time::advance(std::time::Duration::from_secs(3)).await;
    assert_eq!(
        manager.release_retention(thread_id, first, &first_id).await,
        Ok(RetentionReleaseOutcome::Released)
    );
    assert_eq!(
        *watch.borrow(),
        retained,
        "partial loss must not start or refresh eligibility"
    );
    assert_eq!(watch.borrow().unretained_since, initial.unretained_since);
    tokio::time::advance(std::time::Duration::from_secs(2)).await;
    assert_eq!(
        manager
            .release_retention(thread_id, second, &second_id)
            .await,
        Ok(RetentionReleaseOutcome::Released)
    );
    let unretained = *watch.borrow();
    assert!(!unretained.granted);
    assert_eq!(unretained.unretained_since, Instant::now());
    assert_ne!(unretained.revision, retained.revision);
    tokio::time::advance(std::time::Duration::from_secs(7)).await;
    assert_eq!(
        manager
            .release_retention(thread_id, second, &second_id)
            .await,
        Ok(RetentionReleaseOutcome::NotHeld)
    );
    assert_eq!(
        *watch.borrow(),
        unretained,
        "idempotent loss must not postpone retirement"
    );
}

#[tokio::test]
async fn observation_removal_cannot_erase_a_retirement_claim() {
    let (manager, thread_id, principal) = fixture().await;
    let watch = manager
        .subscribe_to_retention(thread_id)
        .await
        .expect("watch");
    let expected = *watch.borrow();
    let claim = manager
        .claim_retention_retirement(thread_id, expected)
        .await
        .expect("claim");
    manager.remove_thread_state(thread_id).await;
    manager.thread_state(thread_id).await;
    assert_eq!(
        manager.acquire_retention(thread_id, principal).await,
        Err(RetentionAuthorityError::LifecycleClosed)
    );
    assert_eq!(watch.borrow().generation, claim.generation);
    assert!(watch.borrow().retiring);
}
