use super::*;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::oneshot;

#[tokio::test]
async fn cancelling_public_start_after_runtime_birth_preserves_host_custody() {
    let (args, _home) =
        super::super::tests::test_start_args(codex_protocol::protocol::SessionSource::Cli, 1).await;
    let host = InProcessHost::default();
    let mut startup = Box::pin(super::super::start_in_host(&host, args));
    // On this current-thread runtime, inspect after each explicit poll and
    // before yielding. Once spawn has registered its handle, cancellation
    // happens before the runtime gets its next scheduler turn.
    let runtime = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            assert!(futures::poll!(startup.as_mut()).is_pending());
            let registered = {
                let state = host.state.lock().unwrap();
                state
                    .runtimes
                    .first()
                    .and_then(|slot| slot.owners.lock().unwrap().runtime.as_ref().map(Arc::clone))
            };
            if let Some(runtime) = registered {
                break runtime;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("public startup must register its real runtime");
    drop(startup);

    assert_eq!(
        runtime
            .observe_until(Instant::now() + Duration::from_secs(10))
            .await,
        TaskObservation::Terminated(TaskTermination::Normal)
    );
    let reports = host
        .observe_until(Instant::now() + Duration::from_secs(10))
        .await;
    assert_eq!(reports.len(), 1);
    assert!(matches!(
        reports[0].cleanup,
        Some((
            TaskObservation::Terminated(TaskTermination::Normal),
            ProcessorCleanupProgress::Observed(
                ProcessorCleanupExecution::ReturnedWithEvidence { .. }
            ),
        ))
    ));
    let (processor, outbound, cleanup) = {
        let state = host.state.lock().unwrap();
        assert_eq!(state.runtimes.len(), 1);
        let owners = state.runtimes[0].owners.lock().unwrap();
        (
            Arc::clone(
                owners
                    .processor
                    .as_ref()
                    .expect("real processor was registered"),
            ),
            Arc::clone(
                owners
                    .outbound
                    .as_ref()
                    .expect("real outbound was registered"),
            ),
            Arc::clone(owners.cleanup.as_ref().expect("real cleanup was retained")),
        )
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    assert_eq!(
        processor.observe_until(deadline).await,
        TaskObservation::Terminated(TaskTermination::Normal)
    );
    assert_eq!(
        outbound.observe_until(deadline).await,
        TaskObservation::Terminated(TaskTermination::Normal)
    );
    // Do not drive cleanup to make the test pass: the ordinary EOF path must
    // already have completed the exact retained original.
    assert_eq!(
        cleanup.completion.peek(),
        Some(&ProcessorCleanupExecution::ReturnedUnverified)
    );
    // All three tasks have joined and the future has returned, but this is
    // still unverified cleanup. Its resource roots must outlive the future's
    // captures and remain in host custody, not just a cached diagnostic.
    let (processor_resource, session_resource) = cleanup.custody.as_ref().unwrap();
    let weak_processor = Arc::downgrade(processor_resource);
    let weak_session = Arc::downgrade(session_resource);
    drop(cleanup);
    assert!(weak_processor.upgrade().is_some());
    assert!(weak_session.upgrade().is_some());
    // Deliberate final-root destruction is only a cycle probe, not a supported
    // successful shutdown disposition for a surviving embedding host.
    drop(host);
    assert!(weak_processor.upgrade().is_none());
    assert!(weak_session.upgrade().is_none());
}

#[tokio::test]
async fn poisoned_host_refuses_success_but_still_closes_existing_task_gates() {
    let host = InProcessHost::default();
    let custody = host.reserve().unwrap();
    let poison = std::panic::catch_unwind(|| {
        let _state = host.state.lock().unwrap();
        panic!("controlled host poison");
    });
    assert!(poison.is_err());
    assert!(host.close_registration().is_err());
    assert!(host.reserve().is_err());
    assert!(custody.spawn(RuntimeTask::Runtime, async {}).is_err());
}

#[tokio::test]
async fn host_retains_cleanup_after_cancelled_startup_observer_and_runtime_exit() {
    let host = InProcessHost::default();
    let custody = host.reserve().unwrap();
    let weak_custody = Arc::downgrade(&custody);
    let resource = Arc::new(());
    let weak_resource = Arc::downgrade(&resource);
    let cleanup = Arc::new(ProcessorCleanupOwner::from_cleanup(async move {
        drop(resource);
    }));
    RuntimeCustodyTicket(weak_custody.clone())
        .attach_cleanup(cleanup)
        .unwrap();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let runtime = custody
        .spawn(RuntimeTask::Runtime, async move {
            entered_tx.send(()).unwrap();
            release_rx.await.unwrap();
        })
        .unwrap();
    let observer = tokio::spawn(async move {
        let _custody = custody;
        std::future::pending::<()>().await;
    });
    entered_rx.await.unwrap();
    observer.abort();
    assert!(observer.await.unwrap_err().is_cancelled());
    release_tx.send(()).unwrap();
    assert_eq!(runtime.join().await, TaskTermination::Normal);
    drop(runtime);
    assert!(weak_custody.upgrade().is_some());
    assert!(weak_resource.upgrade().is_some());

    let cleanup = {
        let state = host.state.lock().unwrap();
        let owners = state.runtimes[0].owners.lock().unwrap();
        Arc::clone(owners.cleanup.as_ref().unwrap())
    };
    assert_eq!(
        cleanup.drive().await,
        ProcessorCleanupExecution::ReturnedUnverified
    );
    assert!(weak_resource.upgrade().is_none());
    drop(cleanup);
    drop(host);
    assert!(weak_custody.upgrade().is_none());
}

#[tokio::test]
async fn host_freeze_refuses_task_birth_but_retains_already_created_cleanup() {
    let host = InProcessHost::default();
    let custody = host.reserve().unwrap();
    let ticket = RuntimeCustodyTicket(Arc::downgrade(&custody));
    host.close_registration().unwrap();
    assert!(host.reserve().is_err());
    let calls = Arc::new(AtomicUsize::new(0));
    let task_calls = Arc::clone(&calls);
    assert!(
        ticket
            .spawn(RuntimeTask::Processor, async move {
                task_calls.fetch_add(1, Ordering::SeqCst);
            })
            .is_err()
    );
    let resource = Arc::new(());
    let weak = Arc::downgrade(&resource);
    ticket
        .attach_cleanup(Arc::new(ProcessorCleanupOwner::from_cleanup(async move {
            drop(resource);
        })))
        .unwrap();
    tokio::task::yield_now().await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(weak.upgrade().is_some());
    host.close_registration().unwrap();
    assert!(weak.upgrade().is_some());
    let cleanup = Arc::clone(custody.owners.lock().unwrap().cleanup.as_ref().unwrap());
    assert_eq!(
        cleanup.drive().await,
        ProcessorCleanupExecution::ReturnedUnverified
    );
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn host_observation_keeps_join_and_cleanup_evidence_separate() {
    let host = InProcessHost::default();
    let custody = host.reserve().unwrap();
    let ticket = RuntimeCustodyTicket(Arc::downgrade(&custody));
    ticket
        .attach_cleanup(Arc::new(ProcessorCleanupOwner::from_cleanup(async {})))
        .unwrap();
    custody
        .spawn(RuntimeTask::Runtime, async {})
        .expect("runtime join should be registered");
    host.close_registration().unwrap();

    let reports = host
        .observe_until(Instant::now() + Duration::from_secs(5))
        .await;
    assert_eq!(reports.len(), 1);
    let report = &reports[0];
    assert_eq!(
        report.runtime,
        TaskObservation::Terminated(TaskTermination::Normal)
    );
    assert_eq!(
        report.cleanup,
        Some((
            TaskObservation::Terminated(TaskTermination::Normal),
            ProcessorCleanupProgress::Observed(ProcessorCleanupExecution::ReturnedUnverified),
        ))
    );
    assert!(!report.is_proven_complete());
}

#[tokio::test]
async fn client_slot_keeps_host_ticket_valid_and_expired_ticket_cannot_spawn() {
    let host = InProcessHost::default();
    let client_slot = host.reserve().unwrap();
    let ticket = RuntimeCustodyTicket(Arc::downgrade(&client_slot));
    drop(host);
    let task = ticket.spawn(RuntimeTask::Runtime, async {}).unwrap();
    assert_eq!(task.join().await, TaskTermination::Normal);
    drop(task);
    drop(client_slot);

    let calls = Arc::new(AtomicUsize::new(0));
    let task_calls = Arc::clone(&calls);
    assert!(
        ticket
            .spawn(RuntimeTask::Processor, async move {
                task_calls.fetch_add(1, Ordering::SeqCst);
            })
            .is_err()
    );
    tokio::task::yield_now().await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn expired_driver_never_first_polls_ready_cleanup() {
    let calls = Arc::new(AtomicUsize::new(0));
    let cleanup_calls = Arc::clone(&calls);
    let cleanup = Arc::new(ProcessorCleanupOwner::from_cleanup(async move {
        cleanup_calls.fetch_add(1, Ordering::SeqCst);
    }));
    let driver = ProcessorCleanupDriver::start(cleanup, Instant::now());
    tokio::task::yield_now().await;
    assert_eq!(
        driver
            .observe_until(Instant::now() + Duration::from_secs(60))
            .await,
        (
            TaskObservation::Terminated(TaskTermination::Normal),
            ProcessorCleanupProgress::TimedOut,
        )
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn driver_progresses_without_observers_but_never_resumes_after_its_deadline() {
    let resource = Arc::new(());
    let weak = Arc::downgrade(&resource);
    let resumed = Arc::new(AtomicUsize::new(0));
    let cleanup_resumed = Arc::clone(&resumed);
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let cleanup = Arc::new(ProcessorCleanupOwner::from_cleanup(async move {
        let _resource = resource;
        entered_tx.send(()).unwrap();
        release_rx.await.unwrap();
        cleanup_resumed.fetch_add(1, Ordering::SeqCst);
    }));
    let deadline = Instant::now() + Duration::from_secs(1);
    let driver = ProcessorCleanupDriver::start(cleanup, deadline);
    // No observer is needed for the driver to enter cleanup.
    entered_rx.await.unwrap();
    let mut observer = Box::pin(driver.observe_until(deadline));
    assert!(futures::poll!(observer.as_mut()).is_pending());
    drop(observer);
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        *driver.progress.borrow(),
        ProcessorCleanupProgress::TimedOut
    );
    assert!(weak.upgrade().is_some());

    release_tx.send(()).unwrap();
    let result = driver
        .observe_until(Instant::now() + Duration::from_secs(60))
        .await;
    assert_eq!(
        result,
        (
            TaskObservation::Terminated(TaskTermination::Normal),
            ProcessorCleanupProgress::TimedOut,
        )
    );
    assert_eq!(resumed.load(Ordering::SeqCst), 0);
    assert!(weak.upgrade().is_some());
    // Explicit test teardown, not a supported host completion disposition.
    drop(driver);
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn driver_observers_share_execution_and_join_without_an_owner_cycle() {
    let cleanup = Arc::new(ProcessorCleanupOwner::from_cleanup(async {}));
    let weak = Arc::downgrade(&cleanup);
    let deadline = Instant::now() + Duration::from_secs(5);
    let driver = ProcessorCleanupDriver::start(cleanup, deadline);
    let expected = (
        TaskObservation::Terminated(TaskTermination::Normal),
        ProcessorCleanupProgress::Observed(ProcessorCleanupExecution::ReturnedUnverified),
    );
    let (first, second) = tokio::join!(
        driver.observe_until(deadline),
        driver.observe_until(deadline)
    );
    assert_eq!((first, second), (expected, expected));
    drop(driver);
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn processor_abort_preserves_in_progress_cleanup_for_external_driver() {
    let resource = Arc::new(());
    let weak = Arc::downgrade(&resource);
    let calls = Arc::new(AtomicUsize::new(0));
    let cleanup_calls = Arc::clone(&calls);
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let owner = Arc::new(ProcessorCleanupOwner::from_cleanup(async move {
        let _resource = resource;
        cleanup_calls.fetch_add(1, Ordering::SeqCst);
        entered_tx.send(()).unwrap();
        release_rx.await.unwrap();
    }));
    let processor_owner = Arc::clone(&owner);
    let processor = tokio::spawn(async move { processor_owner.drive().await });
    entered_rx.await.unwrap();
    processor.abort();
    assert!(processor.await.unwrap_err().is_cancelled());
    assert!(weak.upgrade().is_some());

    release_tx.send(()).unwrap();
    let (first, second) = tokio::join!(owner.drive(), owner.drive());
    assert_eq!(
        (first, second, owner.drive().await),
        (
            ProcessorCleanupExecution::ReturnedUnverified,
            ProcessorCleanupExecution::ReturnedUnverified,
            ProcessorCleanupExecution::ReturnedUnverified,
        )
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn cleanup_panic_is_sticky_and_not_a_normal_task_receipt() {
    let owner = ProcessorCleanupOwner::from_cleanup(async { panic!("controlled cleanup panic") });
    assert_eq!(owner.drive().await, ProcessorCleanupExecution::Panicked);
    assert_eq!(owner.drive().await, ProcessorCleanupExecution::Panicked);
}

#[tokio::test]
async fn cancelled_observer_preserves_task_custody_and_actual_join() {
    let resource = Arc::new(());
    let weak = Arc::downgrade(&resource);
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let owner = EmbeddedTaskOwner::spawn(async move {
        let _resource = resource;
        entered_tx.send(()).unwrap();
        release_rx.await.unwrap();
    });
    entered_rx.await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut observer = Box::pin(owner.observe_until(deadline));
    assert!(futures::poll!(observer.as_mut()).is_pending());
    drop(observer);
    assert!(weak.upgrade().is_some());
    release_tx.send(()).unwrap();
    assert_eq!(
        owner.observe_until(deadline).await,
        TaskObservation::Terminated(TaskTermination::Normal)
    );
    assert!(weak.upgrade().is_none());
    assert_eq!(
        owner.observe_until(Instant::now()).await,
        TaskObservation::Terminated(TaskTermination::Normal)
    );
}

#[tokio::test(start_paused = true)]
async fn elapsed_observation_does_not_abort_owned_task() {
    let resource = Arc::new(());
    let weak = Arc::downgrade(&resource);
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let owner = EmbeddedTaskOwner::spawn(async move {
        let _resource = resource;
        entered_tx.send(()).unwrap();
        release_rx.await.unwrap();
    });
    entered_rx.await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    assert_eq!(
        owner.observe_until(deadline).await,
        TaskObservation::TimedOut
    );
    assert!(weak.upgrade().is_some());
    release_tx.send(()).unwrap();
    // This is a separate observation, not a refresh of a retirement ticket.
    assert_eq!(
        owner
            .observe_until(Instant::now() + Duration::from_secs(1))
            .await,
        TaskObservation::Terminated(TaskTermination::Normal)
    );
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn task_failure_is_sticky_and_distinct_from_normal_termination() {
    let owner = EmbeddedTaskOwner::spawn(async { panic!("controlled task panic") });
    assert_eq!(
        owner
            .observe_until(Instant::now() + Duration::from_secs(5))
            .await,
        TaskObservation::Terminated(TaskTermination::Panicked)
    );
    assert_eq!(
        owner.observe_until(Instant::now()).await,
        TaskObservation::Terminated(TaskTermination::Panicked)
    );

    let task = tokio::spawn(std::future::pending::<()>());
    task.abort();
    let cancelled = EmbeddedTaskOwner::from_handle(task);
    assert_eq!(
        cancelled
            .observe_until(Instant::now() + Duration::from_secs(5))
            .await,
        TaskObservation::Terminated(TaskTermination::Cancelled)
    );
}
