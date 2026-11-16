use super::tests::real_auth_manager;
use super::tests::request;
use super::tests::write_chatgpt_auth_for_intended_account;
use super::*;
use crate::outgoing_message::OutgoingEnvelope;
use crate::outgoing_message::OutgoingMessage;
use crate::outgoing_message::OutgoingMessageSender;
use codex_analytics::AnalyticsEventsClient;
use codex_app_server_protocol::ServerNotification;
use pretty_assertions::assert_eq;
use tokio::sync::mpsc;

struct Reset;
impl ResetInventory for Reset {
    fn reset_all(&self) -> ResetInventoryFuture<'_> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn refused_and_cancelled_transitions_never_publish_account_events() {
    let (home, coordinator, auth, _outgoing, mut rx) = fixture().await;
    let process = coordinator.process_instance_id().await;
    let mut start = request(process.clone(), "cancelled");
    start.expected_auth_fingerprint = Some(account_fingerprint("account-a"));
    start.intended_result_auth_fingerprint = Some(account_fingerprint("account-b"));
    write_chatgpt_auth_for_intended_account(home.path(), "account-b");
    assert!(matches!(
        coordinator.start_dispatch(start.clone(), false).await,
        StartManagedTransitionResponse::Refused { .. }
    ));
    let mut invalid = start.clone();
    invalid.process_instance_id = "wrong-process".to_owned();
    assert!(matches!(
        coordinator.start_dispatch(invalid, true).await,
        StartManagedTransitionResponse::Refused { .. }
    ));
    assert!(rx.try_recv().is_err());

    // A real admitted request keeps Start in Draining; cancellation is driven
    // through the same public dispatcher used by a second connection.
    let work = coordinator.try_acquire_account_work_permit().unwrap();
    let run = coordinator.start_dispatch(start, true);
    tokio::pin!(run);
    loop {
        assert!(futures::poll!(run.as_mut()).is_pending());
        if coordinator
            .state
            .lock()
            .await
            .active
            .as_ref()
            .is_some_and(|active| active.phase == ManagedTransitionPhase::Draining)
        {
            break;
        }
        tokio::task::yield_now().await;
    }
    let cancelled = coordinator
        .cancel_dispatch(
            CancelManagedTransitionParams {
                contract_version:
                    codex_app_server_protocol::MANAGED_AUTH_TRANSITION_CONTRACT_VERSION,
                process_instance_id: process,
                transition_id: "cancelled".to_owned(),
            },
            true,
        )
        .await;
    let CancelManagedTransitionResponse::Accepted { status } = cancelled else {
        panic!("pre-install cancellation must succeed");
    };
    assert_eq!(status.phase, ManagedTransitionPhase::Cancelled);
    drop(work);
    let _ = run.await;
    assert_eq!(
        auth.authoritative_managed_auth_fingerprint().unwrap(),
        Some(account_fingerprint("account-a"))
    );
    assert!(rx.try_recv().is_err());
    assert!(coordinator.try_acquire_account_work_permit().is_some());
}

#[tokio::test]
async fn source_or_cache_change_during_reserved_capacity_wait_never_publishes() {
    for reload_cache in [false, true] {
        let (home, coordinator, auth, outgoing, mut rx) = fixture().await;
        let held = outgoing
            .reserve_account_projection_until(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        let mut start = request(coordinator.process_instance_id().await, "changed");
        start.expected_auth_fingerprint = Some(account_fingerprint("account-a"));
        start.intended_result_auth_fingerprint = Some(account_fingerprint("account-b"));
        write_chatgpt_auth_for_intended_account(home.path(), "account-b");
        coordinator.admit(start).await.unwrap();
        let (prepared, precondition) = {
            let state = coordinator.state.lock().await;
            let active = state.active.as_ref().unwrap();
            (
                active.prepared_adoption.clone().unwrap(),
                active.adoption_precondition.clone().unwrap(),
            )
        };
        coordinator
            .advance("changed", ManagedTransitionPhase::Draining)
            .await
            .unwrap();
        coordinator
            .advance("changed", ManagedTransitionPhase::Adopting)
            .await
            .unwrap();
        let ManagedAdoptionInstallOutcome::Installed { fingerprint } =
            auth.install_prepared_managed_adoption(&prepared, &precondition)
        else {
            panic!("fixture must install the exact prepared B");
        };
        coordinator
            .advance("changed", ManagedTransitionPhase::Resetting)
            .await
            .unwrap();
        // Exercise the real terminal function directly so its first Pending
        // poll is exactly the capacity wait, not an earlier reset/source read.
        let terminal = coordinator.complete_adoption("changed", fingerprint);
        tokio::pin!(terminal);
        assert!(futures::poll!(terminal.as_mut()).is_pending());
        write_chatgpt_auth_for_intended_account(home.path(), "account-c");
        if reload_cache {
            // reload's bool compares auth modes, not account identity. Prove
            // the actual authoritative cache changed instead of that signal.
            auth.reload().await;
            assert_eq!(
                auth.authoritative_managed_auth_fingerprint().unwrap(),
                Some(account_fingerprint("account-c"))
            );
        }
        drop(held);
        let _ = terminal.await;
        let state = coordinator.state.lock().await;
        let result = &state.completed["changed"];
        assert_eq!(result.phase, ManagedTransitionPhase::Quarantined);
        assert_eq!(
            result.refusal,
            Some(if reload_cache {
                ManagedTransitionRefusalKind::StaleAuthRevision
            } else {
                ManagedTransitionRefusalKind::AuthSourceChanged
            })
        );
        assert!(coordinator.try_acquire_account_work_permit().is_none());
        assert!(rx.try_recv().is_err());
    }
}

#[tokio::test]
async fn expired_projection_quarantines_then_fresh_recovery_publishes_once() {
    let (home, coordinator, _auth, outgoing, mut rx) = fixture().await;
    let held = outgoing
        .reserve_account_projection_until(tokio::time::Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    let mut start = request(coordinator.process_instance_id().await, "expired");
    start.expected_auth_fingerprint = Some(account_fingerprint("account-a"));
    start.intended_result_auth_fingerprint = Some(account_fingerprint("account-b"));
    write_chatgpt_auth_for_intended_account(home.path(), "account-b");
    let run = coordinator.start_dispatch(start, true);
    tokio::pin!(run);
    loop {
        assert!(futures::poll!(run.as_mut()).is_pending());
        if coordinator
            .state
            .lock()
            .await
            .active
            .as_ref()
            .is_some_and(|active| active.phase == ManagedTransitionPhase::Resetting)
        {
            break;
        }
        tokio::task::yield_now().await;
    }
    // All fixture creation is finished; the blocked queue's deadline is now
    // driven deterministically rather than sleeping five wall-clock seconds.
    tokio::time::pause();
    let _ = run.await;
    tokio::time::resume();
    let mut retry = request(coordinator.process_instance_id().await, "recovery");
    {
        let state = coordinator.state.lock().await;
        let failed = &state.completed["expired"];
        assert_eq!(failed.phase, ManagedTransitionPhase::Quarantined);
        assert!(matches!(
            failed.reset_failure,
            Some(ResetInventoryError::AccountProjection(
                crate::outgoing_message::AccountProjectionReservationError::TimedOut
            ))
        ));
        retry.expected_auth_revision = state.auth_revision;
        retry.expected_transition_revision = state.transition_revision;
        retry.expected_auth_fingerprint = state.auth_fingerprint.clone();
        retry.intended_result_auth_fingerprint = state.auth_fingerprint.clone();
    }
    assert!(coordinator.try_acquire_account_work_permit().is_none());
    assert!(rx.try_recv().is_err());
    drop(held);
    let _ = coordinator.start_dispatch(retry.clone(), true).await;
    {
        let state = coordinator.state.lock().await;
        assert_eq!(
            state.completed["recovery"].phase,
            ManagedTransitionPhase::Succeeded
        );
        assert_eq!(
            state.completed["expired"].phase,
            ManagedTransitionPhase::Quarantined
        );
    }
    assert!(matches!(
        rx.try_recv().unwrap(),
        OutgoingEnvelope::Broadcast {
            message: OutgoingMessage::AppServerNotification(_)
        }
    ));
    let _ = coordinator.start_dispatch(retry, true).await;
    assert!(rx.try_recv().is_err());
}

async fn fixture() -> (
    tempfile::TempDir,
    ManagedTransitionCoordinator,
    Arc<AuthManager>,
    Arc<OutgoingMessageSender>,
    mpsc::Receiver<OutgoingEnvelope>,
) {
    let home = tempfile::TempDir::new().unwrap();
    write_chatgpt_auth_for_intended_account(home.path(), "account-a");
    let auth = Arc::new(real_auth_manager(home.path()).await);
    let (tx, rx) = mpsc::channel(1);
    let outgoing = Arc::new(OutgoingMessageSender::new(
        tx,
        AnalyticsEventsClient::disabled(),
    ));
    let coordinator = ManagedTransitionCoordinator::with_adoption_and_account_projection(
        AuthoritativeAuthState::from_auth_manager(&auth),
        Arc::new(UnsetTargetEvidenceSource),
        Arc::clone(&auth),
        Arc::new(Reset),
        Arc::clone(&outgoing),
    );
    (home, coordinator, auth, outgoing, rx)
}

#[tokio::test]
async fn account_event_is_present_at_the_exact_reopen_point() {
    let (home, coordinator, _auth, _outgoing, mut rx) = fixture().await;
    let mut start = request(coordinator.process_instance_id().await, "reopen-order");
    start.expected_auth_fingerprint = Some(account_fingerprint("account-a"));
    start.intended_result_auth_fingerprint = Some(account_fingerprint("account-b"));
    write_chatgpt_auth_for_intended_account(home.path(), "account-b");
    let permits = coordinator.account_work_permits.clone();
    let observed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed_at_reopen = Arc::clone(&observed);
    *coordinator
        .account_work_permits
        .inner
        .after_reopen
        .lock()
        .unwrap() = Some(Box::new(move || {
        let _work = permits.try_acquire().expect("account work is now admitted");
        let OutgoingEnvelope::Broadcast {
            message: OutgoingMessage::AppServerNotification(envelope),
        } = rx
            .try_recv()
            .expect("publication must precede reopened admission")
        else {
            panic!("expected account notification");
        };
        assert!(matches!(
            envelope.notification,
            ServerNotification::AccountUpdated(_)
        ));
        observed_at_reopen.store(true, Ordering::Release);
    }));
    assert!(matches!(
        coordinator.start_dispatch(start, true).await,
        StartManagedTransitionResponse::Accepted { .. }
    ));
    assert!(observed.load(Ordering::Acquire));
}

#[tokio::test]
async fn successful_adoption_and_logout_publish_once_before_reopen() {
    for logout in [false, true] {
        let (home, coordinator, auth, _outgoing, mut rx) = fixture().await;
        let mut start = request(coordinator.process_instance_id().await, "project");
        start.expected_auth_fingerprint = Some(account_fingerprint("account-a"));
        if logout {
            start.intent = ManagedTransitionIntent::AdoptManagedLogout;
            start.intended_result_auth_fingerprint = None;
            std::fs::remove_file(home.path().join("auth.json")).unwrap();
        } else {
            start.intended_result_auth_fingerprint = Some(account_fingerprint("account-b"));
            write_chatgpt_auth_for_intended_account(home.path(), "account-b");
        }
        let result = coordinator.start_dispatch(start.clone(), true).await;
        {
            let state = coordinator.state.lock().await;
            assert_eq!(
                state.completed["project"].phase,
                ManagedTransitionPhase::Succeeded
            );
        }
        assert!(coordinator.try_acquire_account_work_permit().is_some());
        // Already enqueued, without polling any separate publication task.
        let OutgoingEnvelope::Broadcast {
            message: OutgoingMessage::AppServerNotification(envelope),
        } = rx.try_recv().unwrap()
        else {
            panic!("expected account notification");
        };
        let ServerNotification::AccountUpdated(actual) = envelope.notification else {
            panic!("must not synthesize login completed");
        };
        let expected = auth
            .with_managed_account_projection(
                start.intended_result_auth_fingerprint.as_deref(),
                |mode, plan| codex_app_server_protocol::AccountUpdatedNotification {
                    auth_mode: mode.map(crate::auth_mode::auth_mode_to_api),
                    plan_type: plan,
                },
            )
            .unwrap();
        assert_eq!(actual, expected);
        assert!(matches!(
            result,
            StartManagedTransitionResponse::Accepted { .. }
        ));
        let StartManagedTransitionResponse::Refused { refusal } =
            coordinator.start_dispatch(start, true).await
        else {
            panic!("completed replay must refuse");
        };
        assert_eq!(refusal.kind, ManagedTransitionRefusalKind::CompletedReplay);
        assert!(rx.try_recv().is_err());
    }
}

#[tokio::test]
async fn full_queue_holds_resetting_and_closed_queue_quarantines() {
    let (home, coordinator, _auth, outgoing, mut rx) = fixture().await;
    let held = outgoing
        .reserve_account_projection_until(tokio::time::Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    let mut start = request(coordinator.process_instance_id().await, "blocked");
    start.expected_auth_fingerprint = Some(account_fingerprint("account-a"));
    start.intended_result_auth_fingerprint = Some(account_fingerprint("account-b"));
    write_chatgpt_auth_for_intended_account(home.path(), "account-b");
    let run = coordinator.start_dispatch(start, true);
    tokio::pin!(run);
    loop {
        assert!(futures::poll!(run.as_mut()).is_pending());
        if coordinator
            .state
            .lock()
            .await
            .active
            .as_ref()
            .is_some_and(|active| active.phase == ManagedTransitionPhase::Resetting)
        {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(coordinator.try_acquire_account_work_permit().is_none());
    assert!(rx.try_recv().is_err());
    rx.close();
    drop(held);
    let _ = run.await;
    let state = coordinator.state.lock().await;
    let completed = &state.completed["blocked"];
    assert_eq!(completed.phase, ManagedTransitionPhase::Quarantined);
    assert_eq!(
        completed.refusal,
        Some(ManagedTransitionRefusalKind::ResetFailed)
    );
    assert!(matches!(
        completed.reset_failure,
        Some(ResetInventoryError::AccountProjection(
            crate::outgoing_message::AccountProjectionReservationError::Closed
        ))
    ));
    assert!(coordinator.try_acquire_account_work_permit().is_none());
    assert!(rx.try_recv().is_err());
}
