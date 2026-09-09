use super::*;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn panic_is_sticky_after_join_compaction() {
    let tasks = ProcessorTasks::default();
    let receipt = tasks
        .spawn(async { panic!("controlled thread-start panic") })
        .unwrap();
    assert_eq!(receipt.await, ProcessorTaskJoin::Panicked);
    let report = tasks.shutdown_until(Instant::now()).await;
    assert!(report.terminal);
    assert!(report.panicked);
    assert!(!report.is_clean());
    assert_eq!(tasks.len(), 0);
    assert_eq!(tasks.shutdown_until(Instant::now()).await, report);
    assert!(tasks.spawn(async {}).is_err());
}

#[tokio::test]
async fn joined_dependency_worker_without_receipt_blocks_clean_completion() {
    let tasks = ProcessorTasks::default();
    let receipt = tasks.spawn_unverified(async {}).unwrap();
    assert_eq!(receipt.await, ProcessorTaskJoin::Joined);
    let report = tasks.shutdown_until(Instant::now()).await;
    assert!(report.terminal);
    assert!(report.unverified);
    assert!(!report.is_clean());
}

#[tokio::test(start_paused = true)]
async fn cancelled_observer_preserves_original_deadline_and_custody() {
    let tasks = ProcessorTasks::default();
    let resource = Arc::new(());
    let weak = Arc::downgrade(&resource);
    let (release, released) = tokio::sync::oneshot::channel();
    let receipt = tasks
        .spawn(async move {
            let _resource = resource;
            let _ = released.await;
        })
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    let mut observer = Box::pin(tasks.shutdown_until(deadline));
    assert!(futures::poll!(observer.as_mut()).is_pending());
    drop(observer);
    let report = tasks
        .shutdown_until(deadline + Duration::from_secs(20))
        .await;
    assert_eq!(Instant::now(), deadline);
    assert!(!report.terminal);
    assert!(weak.upgrade().is_some());
    assert_eq!(tasks.len(), 1);
    assert!(tasks.spawn(async {}).is_err());
    release.send(()).unwrap();
    assert_eq!(receipt.await, ProcessorTaskJoin::Joined);
    assert!(tasks.shutdown_until(deadline).await.is_clean());
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn sequential_tasks_compact_only_observed_joins() {
    let tasks = ProcessorTasks::default();
    for _ in 0..100 {
        assert_eq!(
            tasks.spawn(async {}).unwrap().await,
            ProcessorTaskJoin::Joined
        );
        assert_eq!(tasks.len(), 1);
    }
    assert!(tasks.shutdown_until(Instant::now()).await.is_clean());
    assert_eq!(tasks.len(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cloned_processor_birth_racing_close_is_refused_or_retained() {
    for _ in 0..32 {
        let tasks = ProcessorTasks::default();
        let peer = tasks.clone();
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let peer_barrier = Arc::clone(&barrier);
        let (release, released) = tokio::sync::oneshot::channel();
        let spawner = tokio::spawn(async move {
            peer_barrier.wait().await;
            peer.spawn(async move {
                let _ = released.await;
            })
        });
        barrier.wait().await;
        let report = tasks.shutdown_until(Instant::now()).await;
        match spawner.await.unwrap() {
            Ok(receipt) => {
                assert!(!report.terminal);
                assert_eq!(tasks.len(), 1);
                release.send(()).unwrap();
                assert_eq!(receipt.await, ProcessorTaskJoin::Joined);
            }
            Err(error) => {
                assert_eq!(error, ProcessorTaskAdmissionError::Closed);
                assert!(report.is_clean());
                assert_eq!(tasks.len(), 0);
            }
        }
        assert!(tasks.shutdown_until(Instant::now()).await.is_clean());
    }
}

#[tokio::test]
async fn poisoned_admission_refuses_without_spawning_or_claiming_success() {
    let tasks = ProcessorTasks::default();
    let state = Arc::clone(&tasks.state);
    assert!(
        std::thread::spawn(move || {
            let _guard = state.lock().unwrap();
            panic!("controlled admission poison");
        })
        .join()
        .is_err()
    );
    let result = tasks.spawn(async { panic!("refused task must never run") });
    assert!(matches!(
        result,
        Err(ProcessorTaskAdmissionError::Unavailable)
    ));
    let report = tasks.shutdown_until(Instant::now()).await;
    assert!(report.unavailable);
    assert!(!report.terminal);
    assert!(!report.is_clean());
}

#[tokio::test]
async fn nested_task_remains_owned_after_parent_returns() {
    let tasks = ProcessorTasks::default();
    let ticket = tasks.ticket();
    let (release, released) = tokio::sync::oneshot::channel();
    let (receipt_tx, receipt_rx) = tokio::sync::oneshot::channel();
    let outer = tasks
        .spawn(async move {
            let inner = ticket
                .spawn(async move {
                    let _ = released.await;
                })
                .unwrap();
            let _ = receipt_tx.send(inner);
        })
        .unwrap();
    assert_eq!(outer.await, ProcessorTaskJoin::Joined);
    let inner = receipt_rx.await.unwrap();
    assert!(!tasks.shutdown_until(Instant::now()).await.terminal);
    assert_eq!(tasks.len(), 1);
    release.send(()).unwrap();
    assert_eq!(inner.await, ProcessorTaskJoin::Joined);
    assert!(tasks.shutdown_until(Instant::now()).await.is_clean());
}

#[tokio::test]
async fn nested_ticket_cannot_reopen_or_keep_registry_alive() {
    let tasks = ProcessorTasks::default();
    let ticket = tasks.ticket();
    tasks.close_registration().unwrap();
    assert!(matches!(
        ticket.spawn(async {}),
        Err(ProcessorTaskAdmissionError::Closed)
    ));
    drop(tasks);
    assert!(matches!(
        ticket.spawn(async {}),
        Err(ProcessorTaskAdmissionError::Unavailable)
    ));
}
