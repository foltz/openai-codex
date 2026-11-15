//! Public idle-only input must never steer or queue behind active work.
use super::*;
use codex_app_server_protocol::TurnStartIfIdleParams;
use codex_app_server_protocol::TurnStartIfIdleResponse;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn idle_only_start_refuses_active_then_starts_when_idle() -> Result<()> {
    skip_if_remote!(
        Ok(()),
        "uses the approval-blocked host-local command fixture"
    );
    let server = create_mock_responses_server_sequence_unchecked(vec![
        blocked_turn_response()?,
        create_final_assistant_message_sse_response("original finished")?,
        create_final_assistant_message_sse_response("recovery accepted")?,
    ])
    .await;
    let (mut app, _home, server) = queue_app_with_server(server).await?;
    let thread_id = app
        .start_thread(ThreadStartParams::default())
        .await?
        .thread
        .id;
    let (active_turn_id, approval_id) = start_blocked_turn(&mut app, &thread_id).await?;
    app.clear_message_buffer();
    let request = app
        .send_raw_request(
            "turn/startIfIdle",
            Some(json!({
                "threadId": thread_id,
                "input": [{"type": "text", "text": "MUST_NOT_BE_STEERED", "text_elements": []}]
            })),
        )
        .await?;
    let error = timeout(
        READ_TIMEOUT,
        app.read_stream_until_error_message(RequestId::Integer(request)),
    )
    .await??;
    assert_eq!(error.error.code, -32600);
    assert_eq!(
        error.error.message,
        "thread is not idle; input was not submitted"
    );
    assert!(list_queue(&mut app, &thread_id).await?.data.is_empty());
    let read: ThreadReadResponse = app
        .request(|request_id| ClientRequest::ThreadRead {
            request_id,
            params: ThreadReadParams {
                thread_id: thread_id.clone(),
                include_turns: true,
            },
        })
        .await?;
    assert_eq!(read.thread.turns.len(), 1);
    assert_eq!(read.thread.turns[0].id, active_turn_id);
    assert!(!serde_json::to_string(&read.thread)?.contains("MUST_NOT_BE_STEERED"));
    assert!(
        !app.pending_notification_methods()
            .iter()
            .any(|method| method == "turn/started")
    );
    decline_approval(&mut app, approval_id).await?;
    let completed: TurnCompletedNotification =
        timeout(READ_TIMEOUT, app.read_notification("turn/completed")).await??;
    assert_eq!(completed.turn.id, active_turn_id);
    let started: TurnStartIfIdleResponse = app
        .request(|request_id| ClientRequest::TurnStartIfIdle {
            request_id,
            params: TurnStartIfIdleParams {
                thread_id: thread_id.clone(),
                input: vec![text("REASSESS_RECOVERY_GOAL")],
                ..Default::default()
            },
        })
        .await?;
    assert_ne!(started.turn.id, active_turn_id);
    assert_eq!(
        started.turn.root_turn_id.as_deref(),
        Some(started.turn.id.as_str())
    );
    let completed: TurnCompletedNotification =
        timeout(READ_TIMEOUT, app.read_notification("turn/completed")).await??;
    assert_eq!(completed.turn.id, started.turn.id);
    let requests = server
        .received_requests()
        .await
        .context("mock request capture unavailable")?;
    let requests = requests
        .iter()
        .filter(|request| request.url.path().ends_with("/responses"))
        .collect::<Vec<_>>();
    assert_eq!(requests.len(), 3);
    for request in requests {
        assert!(
            !request.body_json::<Value>()?["input"]
                .to_string()
                .contains("MUST_NOT_BE_STEERED")
        );
    }
    Ok(())
}

#[tokio::test]
async fn idle_only_start_requires_experimental_handshake() -> Result<()> {
    let codex_home = TempDir::new()?;
    let mut app = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build()
        .await?;
    app.initialize_with_capabilities(
        ClientInfo {
            name: "idle-only-experimental-gate".to_string(),
            title: None,
            version: "0.1.0".to_string(),
        },
        Some(InitializeCapabilities::default()),
    )
    .await?;
    let request_id = app
        .send_raw_request(
            "turn/startIfIdle",
            Some(json!({
                "threadId": "00000000-0000-0000-0000-000000000001",
                "input": [{"type": "text", "text": "do not submit", "text_elements": []}]
            })),
        )
        .await?;
    let error = timeout(
        READ_TIMEOUT,
        app.read_stream_until_error_message(RequestId::Integer(request_id)),
    )
    .await??;
    assert_eq!(error.error.code, -32600);
    assert_eq!(
        error.error.message,
        "turn/startIfIdle requires experimentalApi capability"
    );
    Ok(())
}
