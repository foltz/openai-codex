use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::time::{Duration, Instant};

#[tokio::test]
async fn refused_auth_descendant_cannot_be_compacted_into_clean_producer_join() {
    let tasks = ProcessorTasks::default();
    let ticket = tasks.ticket();
    let polled = Arc::new(AtomicBool::new(false));
    let child_polled = Arc::clone(&polled);
    let (ready, entered) = tokio::sync::oneshot::channel();
    let (resume, released) = tokio::sync::oneshot::channel();
    let producer = tasks
        .spawn(async move {
            let result = scope_with_ticket(ticket, None, async move {
                ready.send(()).unwrap();
                released.await.unwrap();
                codex_model_provider::run_auth_call(AuthTaskAccess::Ungated, async move {
                    child_polled.store(true, Ordering::SeqCst);
                })
                .await
            })
            .await;
            assert!(result.is_err());
        })
        .unwrap();
    entered.await.unwrap();
    tasks.close_registration().unwrap();
    resume.send(()).unwrap();
    // The producer wrapper finishes normally: only subordinate evidence can
    // distinguish this from a falsely clean empty census after compaction.
    assert_eq!(producer.await, ProcessorTaskJoin::Joined);
    let report = tasks
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert!(report.terminal);
    assert!(report.unverified);
    assert!(!report.is_clean());
    assert!(!polled.load(Ordering::SeqCst));
    assert_eq!(tasks.shutdown_until(Instant::now()).await, report);
}

#[tokio::test]
async fn independent_rpc_runs_pure_work_but_cannot_spawn_auth_work() {
    let tasks = ProcessorTasks::default();
    assert_eq!(
        scope_account_request(tasks.ticket(), None, async { 42 })
            .await
            .unwrap(),
        42
    );
    let polled = Arc::new(AtomicBool::new(false));
    let observed = Arc::clone(&polled);
    let result = scope_account_request(tasks.ticket(), None, async move {
        // Deliberately swallow the direct refusal: the outer context must
        // still refuse rather than treating this as a successful observation.
        codex_model_provider::run_auth_call(AuthTaskAccess::Ungated, async move {
            observed.store(true, Ordering::SeqCst);
        })
        .await
        .ok()
    })
    .await;
    assert!(result.is_err());
    assert!(!polled.load(Ordering::SeqCst));
}

#[tokio::test]
async fn admitted_rpc_auth_original_keeps_count_after_observer_cancellation() {
    use crate::account_operation_work::AccountOperationWork;
    use crate::account_turn_work::AccountTurnWork;
    use crate::account_work_permits::AccountWorkPermits;
    let permits = AccountWorkPermits::new();
    let tasks = ProcessorTasks::default();
    let work = AccountOperationWork {
        permit: permits.try_acquire().unwrap(),
        turns: AccountTurnWork::default(),
        auth_tasks: Some(tasks.ticket()),
    };
    let ticket = tasks.ticket();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
    let observer = tokio::spawn(async move {
        scope_account_request(ticket, Some(Box::new(work)), async move {
            codex_model_provider::run_auth_call(AuthTaskAccess::Ungated, async move {
                started_tx.send(()).unwrap();
                finish_rx.await.unwrap();
            })
            .await
        })
        .await
    });
    started_rx.await.unwrap();
    permits.close();
    observer.abort();
    assert!(observer.await.unwrap_err().is_cancelled());
    assert!(permits.admitted_count() > 0);
    assert!(permits.try_acquire().is_none());
    finish_tx.send(()).unwrap();
    assert!(
        tasks
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .is_clean()
    );
    assert_eq!(permits.admitted_count(), 0);
}
