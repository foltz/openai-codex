use super::SessionLoopOutcome;
use super::tests::make_session_and_context;
use codex_extension_api::HostOperationWork;
use codex_extension_api::TurnStartAdmission;
use codex_extension_api::TurnWorkRefused;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Debug, Default)]
struct BackgroundGate {
    open: AtomicBool,
    waiting: Arc<Notify>,
    reopened: Arc<Notify>,
}

impl TurnStartAdmission for BackgroundGate {
    fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
        panic!("MCP prewarm must use operation admission");
    }

    fn admit_operation_work(&self) -> Result<Option<Box<dyn HostOperationWork>>, TurnWorkRefused> {
        if self.open.load(Ordering::Acquire) {
            return Ok(None);
        }
        let waiting = Arc::clone(&self.waiting);
        let reopened = Arc::clone(&self.reopened);
        Err(TurnWorkRefused::RetryAfter(Box::pin(async move {
            waiting.notify_one();
            reopened.notified().await;
        })))
    }
}

#[tokio::test]
async fn mcp_background_refusal_preserves_foreground_progress_and_shutdown() {
    let (mut session, _context) = make_session_and_context().await;
    let gate = Arc::new(BackgroundGate::default());
    session.services.host_admission = Some(gate.clone());
    let (requests, receiver) = async_channel::bounded(1);
    session.mcp_prewarm_tx = requests;
    let session = Arc::new(session);
    let (_auth_sender, auth_changes) = tokio::sync::watch::channel(0);
    session.start_mcp_prewarm_worker(receiver, auth_changes);
    session.request_mcp_runtime_refresh();

    tokio::time::timeout(Duration::from_secs(5), gate.waiting.notified())
        .await
        .expect("worker should wait for background admission");
    assert!(session.mcp_refresh.is_pending());
    assert_eq!(
        Arc::strong_count(&session),
        1,
        "idle retry must not retain the session"
    );

    // An already-admitted foreground invocation must not wait behind the
    // background barrier. This fixture has no real account-work registry.
    tokio::time::timeout(Duration::from_secs(5), session.refresh_mcp_if_dirty())
        .await
        .expect("foreground refresh must progress while background admission is closed");
    assert!(!session.mcp_refresh.is_pending());

    session.mark_mcp_runtime_dirty();
    gate.open.store(true, Ordering::Release);
    gate.reopened.notify_one();
    tokio::time::timeout(Duration::from_secs(5), async {
        while session.mcp_refresh.is_pending() {
            tokio::task::yield_now().await;
        }
        let _completed = session
            .mcp_refresh
            .acquire()
            .await
            .expect("refresh gate open");
    })
    .await
    .expect("reopened worker should publish the pending refresh");

    gate.open.store(false, Ordering::Release);
    session.request_mcp_runtime_refresh();
    tokio::time::timeout(Duration::from_secs(5), gate.waiting.notified())
        .await
        .expect("worker should wait for the closed barrier again");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), session.stop_mcp_prewarm_worker())
            .await
            .expect("shutdown must not wait for account admission"),
        SessionLoopOutcome::Normal
    );
}
