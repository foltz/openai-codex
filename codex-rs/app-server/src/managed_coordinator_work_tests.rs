use super::*;
use futures::FutureExt;
use pretty_assertions::assert_eq;

#[test_case::test_case(false; "inherited child")]
#[test_case::test_case(true; "isolated child")]
#[tokio::test]
async fn detached_operation_constructs_and_starts_child_after_request_and_parent_end(
    isolated: bool,
) -> anyhow::Result<()> {
    use codex_extension_api::ExtensionRegistryBuilder;
    use codex_protocol::protocol::EventMsg;
    use codex_protocol::user_input::UserInput;
    use core_test_support::responses;
    use core_test_support::test_codex::test_codex;
    use core_test_support::wait_for_event;

    let permits = AccountWorkPermits::new();
    let mut extensions = ExtensionRegistryBuilder::new();
    extensions.turn_start_admission(Arc::new(
        crate::account_turn_admission::AccountTurnAdmission {
            shutdown: crate::turn_admission::TurnAdmission::default(),
            permits: permits.clone(),
            work: crate::account_turn_work::AccountTurnWork::default(),
        },
    ));
    extensions.turn_lifecycle_contributor(Arc::new(
        crate::account_turn_admission::AccountTurnLifecycle,
    ));
    let server = responses::start_mock_server().await;
    let response = responses::mount_sse_sequence(
        &server,
        vec![
            responses::sse_completed("child"),
            responses::sse_completed("own-turn"),
        ],
    )
    .await;
    let test = test_codex()
        .with_extensions(Arc::new(extensions.build()))
        .build_with_auto_env(&server)
        .await?;
    let operation = crate::account_turn_admission::within_request(permits.try_acquire(), async {
        test.thread_manager
            .derive_request_operation_work()
            .unwrap()
            .unwrap()
    })
    .await;
    let retry = test.codex.admit_mcp_event_stream_retry()?.unwrap();
    drop(retry);
    crate::account_turn_admission::within_request(permits.try_acquire(), async {
        permits.close();
        // An idle stream's reconnect is a new producer, not a descendant
        // of whichever request happens to be polling it.
        assert!(test.codex.admit_mcp_event_stream_retry().is_err());
    })
    .await;
    assert!(permits.try_acquire().is_none());
    assert!(
        test.codex
            .shutdown_and_wait_with_cleanup()
            .await
            .is_complete()
    );

    // There is no live originating request or turn to rediscover here.
    let mut options = codex_core::StartThreadOptions {
        account_work: Some(operation.derive_operation().unwrap()),
        ..codex_core::StartThreadOptions::new(test.config.clone(), None)
    };
    if isolated {
        options
            .thread_extension_init
            .insert(codex_extension_api::SessionIsolation::Isolated);
    }
    let child = test.thread_manager.start_thread(options).await?;
    let input = || {
        codex_core::TurnInputRequest::user_input(vec![UserInput::Text {
            text: "consolidate".to_owned(),
            text_elements: Vec::new(),
        }])
    };
    assert!(matches!(
        child.thread.start_turn_if_idle(input()).await?,
        codex_core::StartIfIdleSubmission::NotSubmitted { .. }
    ));
    assert!(matches!(
        child
            .thread
            .start_turn_if_idle_from_operation(input(), operation.as_ref())
            .await?,
        codex_core::StartIfIdleSubmission::Started { .. }
    ));
    wait_for_event(&child.thread, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
    assert_eq!(response.requests().len(), 1);
    assert_eq!(
        permits.admitted_count(),
        1,
        "completed child must hold no idle turn permit"
    );
    drop(operation);
    assert_eq!(permits.admitted_count(), 0);
    permits.reopen();
    assert!(matches!(
        child.thread.start_turn_if_idle(input()).await?,
        codex_core::StartIfIdleSubmission::Started { .. }
    ));
    wait_for_event(&child.thread, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
    assert_eq!(response.requests().len(), 2);
    assert_eq!(
        permits.admitted_count(),
        0,
        "own-admitted child must release before shutdown"
    );
    assert!(
        child
            .thread
            .shutdown_and_wait_with_cleanup()
            .await
            .is_complete()
    );
    Ok(())
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
    let request =
        super::tests::request(coordinator.process_instance_id().await, "admitted-request");
    let transition = coordinator.start_dispatch(request, /*caller_authorized*/ true);
    tokio::pin!(transition);
    assert!(transition.as_mut().now_or_never().is_none());
    assert!(
        admission
            .admit_turn_work(&store, Box::pin(std::future::pending()))
            .is_err()
    );
    crate::account_turn_admission::within_request(Some(request_guard), async {
        let mut work = admission
            .admit_turn_work(&store, Box::pin(std::future::pending()))
            .unwrap()
            .unwrap();
        work.bind_submission("late-turn");
        work.retain_until_terminal();
        assert_eq!(coordinator.account_work_permits().admitted_count(), 2);
    })
    .await;
    assert_eq!(coordinator.account_work_permits().admitted_count(), 1);
    assert!(transition.as_mut().now_or_never().is_none());
    admission.turn_work_terminal(&store, "late-turn");
    let result = transition.await;
    assert!(matches!(
        result,
        StartManagedTransitionResponse::Accepted { .. }
    ));
    assert_eq!(coordinator.account_work_permits().admitted_count(), 0);
}

#[tokio::test]
async fn coordinator_drain_observes_listenerless_loop_termination() {
    let coordinator = ManagedTransitionCoordinator::new();
    let (ended, termination) = tokio::sync::oneshot::channel();
    let session = coordinator.account_turn_work().session(
        async move {
            let _ = termination.await;
        }
        .boxed(),
    );
    session
        .begin(coordinator.try_acquire_account_work_permit().unwrap())
        .unwrap()
        .bind("suspended-turn".into());
    let request = super::tests::request(coordinator.process_instance_id().await, "loop-drain");
    let start = coordinator.start_dispatch(request, /*caller_authorized*/ true);
    tokio::pin!(start);
    assert!(start.as_mut().now_or_never().is_none());
    assert_eq!(coordinator.account_work_permits().admitted_count(), 1);
    assert!(coordinator.try_acquire_account_work_permit().is_none());
    ended.send(()).unwrap();
    let response = tokio::time::timeout(Duration::from_secs(1), start)
        .await
        .unwrap();
    let StartManagedTransitionResponse::Accepted { status } = response else {
        panic!("joined loop must release the drain's outstanding work");
    };
    assert_eq!(status.phase, ManagedTransitionPhase::Draining);
    assert_eq!(coordinator.account_work_permits().admitted_count(), 0);
}

#[tokio::test(start_paused = true)]
async fn live_listenerless_turn_times_out_without_releasing_or_cancelling_its_work() {
    let coordinator = ManagedTransitionCoordinator::new();
    let (ended, termination) = tokio::sync::oneshot::channel::<()>();
    let session = coordinator.account_turn_work().session(
        async move {
            let _ = termination.await;
        }
        .boxed(),
    );
    session
        .begin(coordinator.try_acquire_account_work_permit().unwrap())
        .unwrap()
        .bind("held-turn".into());
    let request = super::tests::request(coordinator.process_instance_id().await, "held-loop");
    let response = coordinator
        .start_dispatch(request, /*caller_authorized*/ true)
        .await;
    let StartManagedTransitionResponse::Accepted { status } = response else {
        panic!("admitted transition must retain a quarantine result");
    };
    assert_eq!(status.phase, ManagedTransitionPhase::Quarantined);
    assert_eq!(coordinator.account_work_permits().admitted_count(), 1);
    assert!(
        !ended.is_closed(),
        "deadline must not cancel the retained loop receipt"
    );
    assert!(coordinator.try_acquire_account_work_permit().is_none());
    session.terminal("held-turn");
    assert_eq!(coordinator.account_work_permits().admitted_count(), 0);
}
