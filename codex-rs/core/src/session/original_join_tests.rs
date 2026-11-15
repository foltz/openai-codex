use super::*;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn original_join_retains_normal_result_for_every_observer() {
    let termination = session_loop_termination_from_handle(tokio::spawn(async {}));
    assert_eq!(termination.clone().await, SessionLoopOutcome::Normal);
    assert_eq!(termination.clone().await, SessionLoopOutcome::Normal);
    assert_eq!(termination.observed(), Some(SessionLoopOutcome::Normal));
}

#[tokio::test]
async fn original_join_retains_panic_instead_of_success() {
    let termination = session_loop_termination_from_handle(tokio::spawn(async {
        panic!("original join panic witness");
    }));
    assert_eq!(termination.clone().await, SessionLoopOutcome::Panicked);
    assert_eq!(termination.observed(), Some(SessionLoopOutcome::Panicked));
}

#[tokio::test]
async fn abort_request_requires_the_original_cancelled_join() {
    let termination = session_loop_termination_from_handle(tokio::spawn(std::future::pending()));
    assert_eq!(termination.observed(), None);
    termination.request_abort();
    assert_eq!(termination.clone().await, SessionLoopOutcome::Cancelled);
    assert_eq!(termination.observed(), Some(SessionLoopOutcome::Cancelled));
}

#[tokio::test]
async fn cancelled_join_observer_keeps_the_original_live_task() {
    let (release, wait) = oneshot::channel();
    let termination = session_loop_termination_from_handle(tokio::spawn(async move {
        wait.await.expect("original task remains alive");
    }));
    let mut observer = Box::pin(termination.clone());
    assert!(futures::poll!(observer.as_mut()).is_pending());
    drop(observer);
    assert_eq!(termination.observed(), None);
    release
        .send(())
        .expect("observer did not abort original task");
    assert_eq!(termination.clone().await, SessionLoopOutcome::Normal);
}

#[tokio::test]
async fn late_abort_does_not_reclassify_an_observed_normal_join() {
    let termination = session_loop_termination_from_handle(tokio::spawn(async {}));
    assert_eq!(termination.clone().await, SessionLoopOutcome::Normal);
    termination.request_abort();
    assert_eq!(termination.await, SessionLoopOutcome::Normal);
}

async fn io_with_original(handle: JoinHandle<()>) -> SessionIo {
    let (tx_sub, rx_sub) = async_channel::bounded(1);
    drop(rx_sub);
    let (_tx_event, rx_event) = async_channel::unbounded();
    SessionIo {
        tx_sub: tx_sub.into(),
        rx_event,
        agent_status: watch::channel(AgentStatus::PendingInit).1,
        session_loop_termination: session_loop_termination_from_handle(handle),
    }
}

#[tokio::test]
async fn shutdown_wait_refuses_an_original_loop_panic() {
    let io = io_with_original(tokio::spawn(async { panic!("loop panic witness") })).await;
    let error = io
        .shutdown_and_wait()
        .await
        .expect_err("panic is not normal shutdown");
    assert!(matches!(
        error.details(),
        CodexErrorDetails::InternalAgentDied
    ));
    assert_eq!(
        io.session_loop_termination.observed(),
        Some(SessionLoopOutcome::Panicked)
    );
}

#[tokio::test]
async fn shutdown_wait_refuses_an_original_loop_cancellation() {
    let io = io_with_original(tokio::spawn(std::future::pending())).await;
    io.session_loop_termination.request_abort();
    let error = io
        .shutdown_and_wait()
        .await
        .expect_err("abort is not normal shutdown");
    assert!(matches!(
        error.details(),
        CodexErrorDetails::InternalAgentDied
    ));
    assert_eq!(
        io.session_loop_termination.observed(),
        Some(SessionLoopOutcome::Cancelled)
    );
}

#[tokio::test]
async fn cancelled_prewarm_stop_keeps_the_original_join_in_session() {
    let (session, _) = tests::make_session_and_context().await;
    assert!(session.mcp_prewarm_task.lock().unwrap().is_none());
    let (release, wait) = oneshot::channel();
    let original = session_loop_termination_from_handle(tokio::spawn(async move {
        wait.await.expect("prewarm original remains alive");
    }));
    *session.mcp_prewarm_task.lock().unwrap() = Some(original.clone());
    let mut stop = Box::pin(session.stop_mcp_prewarm_worker());
    assert!(futures::poll!(stop.as_mut()).is_pending());
    drop(stop);
    assert!(session.mcp_prewarm_shutdown.is_cancelled());
    assert_eq!(original.observed(), None);
    assert!(session.mcp_prewarm_task.lock().unwrap().is_some());
    release
        .send(())
        .expect("cancelled stop did not abort original");
    assert_eq!(
        session.stop_mcp_prewarm_worker().await,
        SessionLoopOutcome::Normal
    );
    assert_eq!(original.observed(), Some(SessionLoopOutcome::Normal));
    assert_eq!(
        session.stop_mcp_prewarm_worker().await,
        SessionLoopOutcome::Normal
    );
}

#[tokio::test]
async fn prewarm_stop_retains_the_original_panic() {
    let (session, _) = tests::make_session_and_context().await;
    *session.mcp_prewarm_task.lock().unwrap() =
        Some(session_loop_termination_from_handle(tokio::spawn(async {
            panic!("prewarm panic witness")
        })));
    assert_eq!(
        session.stop_mcp_prewarm_worker().await,
        SessionLoopOutcome::Panicked
    );
    assert_eq!(
        session.stop_mcp_prewarm_worker().await,
        SessionLoopOutcome::Panicked
    );
}

#[tokio::test]
async fn prewarm_without_a_created_worker_has_no_join_to_observe() {
    let (session, _) = tests::make_session_and_context().await;
    assert!(session.mcp_prewarm_task.lock().unwrap().is_none());
    assert_eq!(
        session.stop_mcp_prewarm_worker().await,
        SessionLoopOutcome::Normal
    );
    assert!(session.mcp_prewarm_shutdown.is_cancelled());
    assert!(session.mcp_prewarm_task.lock().unwrap().is_none());
}
