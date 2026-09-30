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
async fn processor_work_keeps_request_custody_after_observer_drop() {
    use crate::account_turn_admission::derive_request_work;
    use crate::account_turn_admission::within_request;
    use crate::processor_task_retirement::ProcessorTasks;

    let permits = AccountWorkPermits::new();
    let tasks = ProcessorTasks::default();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let (descendant_tx, descendant_rx) = tokio::sync::oneshot::channel();
    let mut request = Box::pin(within_request(permits.try_acquire(), async {
        let construction_work = derive_request_work().unwrap();
        let receipt = tasks
            .spawn(within_request(construction_work, async move {
                started_tx.send(()).unwrap();
                release_rx.await.unwrap();
                // The explicitly transferred scope can derive after CLOSED;
                // no request observer or fresh admission is needed.
                assert!(descendant_tx.send(derive_request_work().unwrap()).is_ok());
            }))
            .unwrap();
        receipt.await
    }));
    assert!(request.as_mut().now_or_never().is_none());
    tokio::time::timeout(Duration::from_secs(1), started_rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(permits.admitted_count(), 2);
    permits.close();
    drop(request);
    assert_eq!(permits.admitted_count(), 1);
    assert!(permits.try_acquire().is_none());

    release_tx.send(()).unwrap();
    let descendant = tokio::time::timeout(Duration::from_secs(1), descendant_rx)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        tasks
            .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .is_clean()
    );
    assert_eq!(permits.admitted_count(), 1);
    drop(descendant);
    assert_eq!(permits.admitted_count(), 0);
}

#[tokio::test]
async fn refused_processor_registration_releases_captured_account_work() {
    use crate::account_turn_admission::derive_request_work;
    use crate::account_turn_admission::within_request;
    use crate::processor_task_retirement::ProcessorTaskAdmissionError;
    use crate::processor_task_retirement::ProcessorTasks;

    let permits = AccountWorkPermits::new();
    let tasks = ProcessorTasks::default();
    tasks.close_registration().unwrap();
    within_request(permits.try_acquire(), async {
        let construction_work = derive_request_work().unwrap();
        assert_eq!(permits.admitted_count(), 2);
        let result = tasks.spawn(within_request(construction_work, async {
            panic!("refused work must not run");
        }));
        assert!(matches!(result, Err(ProcessorTaskAdmissionError::Closed)));
        assert_eq!((permits.admitted_count(), tasks.len()), (1, 0));
    })
    .await;
    assert_eq!(permits.admitted_count(), 0);
}

#[tokio::test]
#[expect(
    clippy::async_yields_async,
    reason = "returning task receipts ends the parent request scope before observing descendant panic or cancellation"
)]
async fn transferred_request_scope_releases_on_panic_and_task_cancellation() {
    use crate::account_turn_admission::derive_request_work;
    use crate::account_turn_admission::within_request;
    use crate::processor_task_retirement::ProcessorTaskJoin;
    use crate::processor_task_retirement::ProcessorTasks;

    let permits = AccountWorkPermits::new();
    let tasks = ProcessorTasks::default();
    let receipt = within_request(permits.try_acquire(), async {
        tasks
            .spawn(within_request(derive_request_work().unwrap(), async {
                panic!("synthetic constructor panic");
            }))
            .unwrap()
    })
    .await;
    assert_eq!(receipt.await, ProcessorTaskJoin::Panicked);
    assert_eq!(permits.admitted_count(), 0);

    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let task = within_request(permits.try_acquire(), async {
        tokio::spawn(within_request(derive_request_work().unwrap(), async move {
            started_tx.send(()).unwrap();
            std::future::pending::<()>().await;
        }))
    })
    .await;
    tokio::time::timeout(Duration::from_secs(1), started_rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(permits.admitted_count(), 1);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(permits.admitted_count(), 0);
}

#[tokio::test]
async fn request_scope_absence_and_exhaustion_are_distinct_before_spawn() {
    use crate::account_turn_admission::derive_request_work;
    use crate::account_turn_admission::within_request;
    use crate::processor_task_retirement::ProcessorTaskJoin;
    use crate::processor_task_retirement::ProcessorTasks;
    use codex_extension_api::TurnWorkRefused;

    let permits = AccountWorkPermits::new();
    let tasks = ProcessorTasks::default();
    let ungated = derive_request_work().unwrap();
    assert!(ungated.is_none());
    assert_eq!(
        tasks
            .spawn(within_request(ungated, async {}))
            .unwrap()
            .await,
        ProcessorTaskJoin::Joined
    );
    let exhausted_tasks = ProcessorTasks::default();
    within_request(permits.try_acquire(), async {
        let exhausted = ACCOUNT_WORK_CLOSED | ACCOUNT_WORK_COUNT_MASK;
        permits.inner.state.store(exhausted, Ordering::Release);
        let result =
            derive_request_work().map(|work| exhausted_tasks.spawn(within_request(work, async {})));
        assert!(matches!(result, Err(TurnWorkRefused::Unavailable)));
        assert_eq!(exhausted_tasks.len(), 0);
        assert_eq!(permits.inner.state.load(Ordering::Acquire), exhausted);
        // Restore the one real guard before its scope drops.
        permits
            .inner
            .state
            .store(ACCOUNT_WORK_CLOSED | 1, Ordering::Release);
    })
    .await;
    assert_eq!(
        permits.inner.state.load(Ordering::Acquire),
        ACCOUNT_WORK_CLOSED
    );
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
    let mut pending = admission
        .admit_turn_work(&parent, Box::pin(std::future::pending()))
        .unwrap()
        .unwrap();
    // The queue binds before handling; it has not yet returned Started.
    pending.bind_submission("parent-turn");
    coordinator.account_work_permits.close();
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
    assert_eq!(coordinator.account_work_permits().admitted_count(), 1);
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
