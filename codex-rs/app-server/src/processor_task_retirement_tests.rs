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

#[tokio::test]
async fn blocking_original_survives_observer_abort_and_deadline() {
    let tasks = ProcessorTasks::default();
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let receipt = tasks
        .ticket()
        .spawn_blocking(move || {
            entered.send(()).unwrap();
            released.recv().unwrap();
        })
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), started)
        .await
        .unwrap()
        .unwrap();
    let observer = tokio::spawn(receipt.clone());
    observer.abort();
    assert!(observer.await.unwrap_err().is_cancelled());
    let report = tasks.shutdown_until(Instant::now()).await;
    release.send(()).unwrap();
    assert!(!report.terminal);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), receipt)
            .await
            .unwrap(),
        ProcessorTaskJoin::Joined
    );
    assert!(tasks.shutdown_until(Instant::now()).await.is_clean());
}

#[tokio::test]
async fn closed_ticket_refuses_blocking_body_before_dispatch() {
    let tasks = ProcessorTasks::default();
    tasks.close_registration().unwrap();
    let polled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let work = polled.clone();
    assert_eq!(
        tasks
            .ticket()
            .spawn_blocking(move || work.store(true, std::sync::atomic::Ordering::SeqCst))
            .err(),
        Some(ProcessorTaskAdmissionError::Closed)
    );
    assert!(!polled.load(std::sync::atomic::Ordering::SeqCst));
    assert!(tasks.shutdown_until(Instant::now()).await.is_clean());
}

#[test]
fn dead_ticket_and_missing_runtime_refuse_blocking_birth_without_fallback() {
    let dead = {
        let tasks = ProcessorTasks::default();
        tasks.ticket()
    };
    assert_eq!(
        dead.spawn_blocking(|| panic!("dead owner must not dispatch"))
            .err(),
        Some(ProcessorTaskAdmissionError::Unavailable)
    );
    let live = ProcessorTasks::default();
    assert_eq!(
        live.ticket()
            .spawn_blocking(|| panic!("missing runtime must not dispatch"))
            .err(),
        Some(ProcessorTaskAdmissionError::Unavailable)
    );
    assert_eq!(live.len(), 0);
}

#[tokio::test]
async fn blocking_panic_is_sticky_after_original_join() {
    let tasks = ProcessorTasks::default();
    let receipt = tasks
        .spawn_blocking(|| panic!("synthetic physical blocking panic"))
        .unwrap();
    assert_eq!(receipt.await, ProcessorTaskJoin::Panicked);
    for _ in 0..2 {
        let report = tasks.shutdown_until(Instant::now()).await;
        assert!(report.terminal && report.panicked && !report.is_clean());
    }
}

#[tokio::test]
async fn panic_payload_compaction_reenters_registry_outside_its_lock() {
    struct Payload {
        ticket: ProcessorTaskTicket,
        unlocked: Arc<std::sync::atomic::AtomicBool>,
        child_finished: Arc<std::sync::atomic::AtomicBool>,
    }
    impl Drop for Payload {
        fn drop(&mut self) {
            let registry = self.ticket.state.upgrade().unwrap();
            let unlocked = registry.try_lock().is_ok();
            self.unlocked
                .store(unlocked, std::sync::atomic::Ordering::SeqCst);
            // Fail the old lock-holding implementation without deadlocking the
            // test. The fixed implementation also exercises real re-entry.
            if unlocked {
                let finished = self.child_finished.clone();
                self.ticket
                    .spawn(async move {
                        finished.store(true, std::sync::atomic::Ordering::SeqCst);
                    })
                    .unwrap();
            }
        }
    }
    let tasks = ProcessorTasks::default();
    let unlocked = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let child_finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let payload = Payload {
        ticket: tasks.ticket(),
        unlocked: unlocked.clone(),
        child_finished: child_finished.clone(),
    };
    let (entered, started) = tokio::sync::oneshot::channel();
    let receipt = tasks
        .spawn(async move {
            entered.send(()).unwrap();
            std::panic::panic_any(payload);
        })
        .unwrap();
    started.await.unwrap();
    tokio::task::yield_now().await;
    let _ = tasks.pending_len_for_test();
    assert!(unlocked.load(std::sync::atomic::Ordering::SeqCst));
    // The unused observation must not retain the already-disposed payload.
    assert_eq!(receipt.peek(), Some(&ProcessorTaskJoin::Panicked));
    let drain = tasks
        .observe_until(Instant::now() + std::time::Duration::from_secs(1))
        .await;
    assert!(drain.terminal);
    assert!(drain.panicked);
    assert!(!drain.is_clean());
    assert!(child_finished.load(std::sync::atomic::Ordering::SeqCst));
}
