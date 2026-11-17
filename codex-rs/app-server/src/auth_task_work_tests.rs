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
