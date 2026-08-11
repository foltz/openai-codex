use super::connection_handling_websocket::WsClient;
use super::connection_handling_websocket::connect_websocket;
use super::connection_handling_websocket::create_config_toml;
use super::connection_handling_websocket::read_error_for_id;
use super::connection_handling_websocket::read_jsonrpc_message;
use super::connection_handling_websocket::read_response_for_id;
use super::connection_handling_websocket::send_initialize_request;
use super::connection_handling_websocket::send_request;
use super::connection_handling_websocket::spawn_websocket_server;
use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use app_test_support::create_mock_responses_server_sequence_unchecked;
use app_test_support::to_response;
use codex_app_server_protocol::JSONRPCMessage;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ThreadClearEndedNotification;
use codex_app_server_protocol::ThreadClearErrorCode;
use codex_app_server_protocol::ThreadClearParams;
use codex_app_server_protocol::ThreadClearResponse;
use codex_app_server_protocol::ThreadClearStartedNotification;
use codex_app_server_protocol::ThreadReadParams;
use codex_app_server_protocol::ThreadReadResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::ThreadStartSource;
use codex_app_server_protocol::ThreadStartedNotification;
use codex_protocol::ThreadId;
use codex_state::ClearTransitionEvidenceState;
use codex_state::ClearTransitionId;
use codex_state::ClearTransitionPhase;
use codex_state::ClearTransitionReserveOutcome;
use codex_state::StateRuntime;
use core_test_support::PathExt;
use serde_json::json;
use std::collections::HashMap;
use tempfile::TempDir;
use tokio::time::Duration;
use tokio::time::timeout;

#[derive(Debug)]
enum ClearEvidence {
    Ended(ThreadClearEndedNotification),
    Started(Box<ThreadClearStartedNotification>),
}

enum ClearAttempt {
    Won(Box<ThreadClearResponse>, Vec<ClearEvidence>),
    Lost(ThreadClearErrorCode, Vec<ClearEvidence>),
}

#[tokio::test]
async fn thread_clear_returns_one_ordered_requester_scoped_pair() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;
    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;
    let mut requester = connect_websocket(bind_addr).await?;
    let mut bystander = connect_websocket(bind_addr).await?;
    initialize(&mut requester, 1, "requester").await?;
    initialize(&mut bystander, 2, "bystander").await?;

    let predecessor = start_thread(&mut requester, 3).await?;
    send_clear(&mut requester, 5, &predecessor).await?;
    let (response, evidence) = read_clear_outcome(&mut requester, 5).await?;

    let [ClearEvidence::Ended(ended), ClearEvidence::Started(started)] = evidence.as_slice() else {
        bail!("clear evidence was not emitted end-before-start: {evidence:?}");
    };
    assert_eq!(response.transition_id, ended.transition_id);
    assert_eq!(response.transition_id, started.transition_id);
    assert_eq!(response.predecessor_thread_id, predecessor);
    assert_eq!(ended.predecessor_thread_id, predecessor);
    assert_eq!(started.predecessor_thread_id, predecessor);
    assert_eq!(ended.successor_thread_id, response.successor_thread.id);
    assert_eq!(started.successor_thread.id, response.successor_thread.id);

    assert_no_clear_evidence(&mut bystander).await?;

    // The requester moved to B, while an unrelated connection saw no authoritative evidence.
    assert_unsubscribe_status(&mut requester, 6, &predecessor, "notSubscribed").await?;
    assert_unsubscribe_status(
        &mut requester,
        7,
        &response.successor_thread.id,
        "unsubscribed",
    )
    .await?;
    assert_unsubscribe_status(&mut bystander, 8, &predecessor, "notSubscribed").await?;

    send_clear(&mut requester, 9, &predecessor).await?;
    assert_clear_error(&mut requester, 9, ThreadClearErrorCode::TransitionCompleted).await?;
    assert_no_clear_evidence(&mut requester).await?;

    process.kill().await.context("failed to stop app-server")?;
    Ok(())
}

#[tokio::test]
async fn thread_clear_rejects_unknown_and_current_non_subscriber_without_evidence() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;
    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;
    let mut owner = connect_websocket(bind_addr).await?;
    let mut other = connect_websocket(bind_addr).await?;
    initialize(&mut owner, 1, "owner").await?;
    initialize(&mut other, 2, "other").await?;

    send_clear(&mut other, 3, "00000000-0000-0000-0000-000000000001").await?;
    assert_clear_error(&mut other, 3, ThreadClearErrorCode::UnknownPredecessor).await?;
    assert_no_clear_evidence(&mut other).await?;

    let predecessor = start_thread(&mut owner, 4).await?;
    send_clear(&mut other, 5, &predecessor).await?;
    assert_clear_error(&mut other, 5, ThreadClearErrorCode::NotSubscribed).await?;
    assert_no_clear_evidence(&mut other).await?;

    process.kill().await.context("failed to stop app-server")?;
    Ok(())
}

#[tokio::test]
async fn thread_clear_keeps_bystander_on_usable_predecessor_without_authoritative_evidence()
-> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;
    std::fs::write(
        codex_home.path().join("hooks.json"),
        json!({
            "hooks": {
                "SessionStart": [{
                    "matcher": "startup",
                    "hooks": [{ "type": "command", "command": "true" }]
                }]
            }
        })
        .to_string(),
    )?;
    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;
    let mut requester = connect_websocket(bind_addr).await?;
    let mut bystander = connect_websocket(bind_addr).await?;
    initialize(&mut requester, 1, "requester").await?;
    initialize(&mut bystander, 2, "bystander").await?;

    let predecessor = start_thread_with_config(
        &mut requester,
        3,
        Some(HashMap::from([(
            "bypass_hook_trust".to_string(),
            json!(true),
        )])),
    )
    .await?;
    resume_thread(&mut bystander, 4, &predecessor).await?;

    send_clear(&mut requester, 5, &predecessor).await?;
    let (response, _) = read_clear_outcome(&mut requester, 5).await?;
    let successor = response.successor_thread.id;

    let started =
        read_non_authoritative_clear_broadcast(&mut bystander, &predecessor, &successor).await?;
    assert_eq!(started.thread.id, successor);
    assert_eq!(started.session_start_source, Some(ThreadStartSource::Clear));
    assert_eq!(
        started.clear_predecessor_thread_id.as_deref(),
        Some(predecessor.as_str())
    );
    assert_no_clear_evidence(&mut bystander).await?;

    // The broadcast is display context only: it does not attach the bystander
    // to B or detach it from its still-readable subscription to A.
    assert_unsubscribe_status(&mut bystander, 6, &successor, "notSubscribed").await?;
    let predecessor_thread = read_thread(&mut bystander, 7, &predecessor).await?;
    assert_eq!(predecessor_thread.thread.id, predecessor);
    assert_unsubscribe_status(&mut bystander, 8, &predecessor, "unsubscribed").await?;

    // Only the requester moved from A to B.
    assert_unsubscribe_status(&mut requester, 9, &predecessor, "notSubscribed").await?;
    assert_unsubscribe_status(&mut requester, 10, &successor, "unsubscribed").await?;

    process.kill().await.context("failed to stop app-server")?;
    Ok(())
}

#[tokio::test]
async fn startup_recovery_abandons_reserved_transition_without_a_successor() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;
    let transition_id = ClearTransitionId::new();
    let predecessor = ThreadId::new();
    let successor = ThreadId::new();
    let state = StateRuntime::init(
        codex_state::SqliteConfig::new_for_testing(codex_home.path().abs()),
        "test-provider".to_string(),
    )
    .await?;
    let ClearTransitionReserveOutcome::Reserved(_) = state
        .reserve_clear_transition(transition_id, predecessor, successor)
        .await?
    else {
        bail!("expected fresh transition reservation");
    };
    drop(state);

    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;
    let mut client = connect_websocket(bind_addr).await?;
    initialize(&mut client, 1, "recovery-observer").await?;

    let state = StateRuntime::init(
        codex_state::SqliteConfig::new_for_testing(codex_home.path().abs()),
        "test-provider".to_string(),
    )
    .await?;
    let recovered = state
        .get_clear_transition(transition_id)
        .await?
        .context("recovery must retain the transition audit row")?;
    assert_eq!(recovered.phase, ClearTransitionPhase::Abandoned);
    assert!(
        state
            .get_clear_transition_by_predecessor(predecessor)
            .await?
            .is_none(),
        "abandonment must release the predecessor for a later clear"
    );

    process.kill().await.context("failed to stop app-server")?;
    Ok(())
}

#[tokio::test]
async fn startup_recovery_terminalizes_undispatched_evidence_without_replay() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;
    let transition_id = ClearTransitionId::new();
    let state = StateRuntime::init(
        codex_state::SqliteConfig::new_for_testing(codex_home.path().abs()),
        "test-provider".to_string(),
    )
    .await?;
    let ClearTransitionReserveOutcome::Reserved(_) = state
        .reserve_clear_transition(transition_id, ThreadId::new(), ThreadId::new())
        .await?
    else {
        bail!("expected fresh transition reservation");
    };
    for (expected, next) in [
        (
            ClearTransitionPhase::Reserved,
            ClearTransitionPhase::SuccessorCreated,
        ),
        (
            ClearTransitionPhase::SuccessorCreated,
            ClearTransitionPhase::Committed,
        ),
        (
            ClearTransitionPhase::Committed,
            ClearTransitionPhase::EvidenceClaimed,
        ),
    ] {
        assert!(
            state
                .advance_clear_transition_phase(transition_id, expected, next)
                .await?
        );
    }
    drop(state);

    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;
    let mut client = connect_websocket(bind_addr).await?;
    initialize(&mut client, 1, "recovery-observer").await?;
    assert_no_clear_evidence(&mut client).await?;

    let state = StateRuntime::init(
        codex_state::SqliteConfig::new_for_testing(codex_home.path().abs()),
        "test-provider".to_string(),
    )
    .await?;
    let recovered = state
        .get_clear_transition(transition_id)
        .await?
        .context("recovery must retain transition state")?;
    assert_eq!(recovered.phase, ClearTransitionPhase::Completed);
    assert_eq!(
        recovered.end_evidence_state,
        ClearTransitionEvidenceState::Failed
    );
    assert_eq!(
        recovered.start_evidence_state,
        ClearTransitionEvidenceState::Failed
    );

    process.kill().await.context("failed to stop app-server")?;
    Ok(())
}

#[tokio::test]
async fn concurrent_thread_clear_has_one_authoritative_winner() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;
    std::fs::write(
        codex_home.path().join("hooks.json"),
        json!({
            "hooks": {
                "SessionStart": [{
                    "matcher": "startup",
                    "hooks": [{ "type": "command", "command": "true" }]
                }]
            }
        })
        .to_string(),
    )?;
    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;
    let mut first = connect_websocket(bind_addr).await?;
    let mut second = connect_websocket(bind_addr).await?;
    initialize(&mut first, 1, "first").await?;
    initialize(&mut second, 2, "second").await?;

    let predecessor = start_thread_with_config(
        &mut first,
        3,
        Some(HashMap::from([(
            "bypass_hook_trust".to_string(),
            json!(true),
        )])),
    )
    .await?;
    resume_thread(&mut second, 4, &predecessor).await?;

    send_clear(&mut first, 5, &predecessor).await?;
    send_clear(&mut second, 6, &predecessor).await?;
    let (first_attempt, second_attempt) = tokio::join!(
        read_clear_attempt(&mut first, 5),
        read_clear_attempt(&mut second, 6)
    );
    let attempts = [first_attempt?, second_attempt?];
    assert_eq!(
        attempts
            .iter()
            .filter(|attempt| matches!(attempt, ClearAttempt::Won(_, _)))
            .count(),
        1
    );
    assert_eq!(
        attempts
            .iter()
            .filter(|attempt| matches!(attempt, ClearAttempt::Lost(_, _)))
            .count(),
        1
    );
    for attempt in attempts {
        match attempt {
            ClearAttempt::Won(response, evidence) => {
                assert_eq!(evidence.len(), 2);
                assert_ne!(response.successor_thread.id, predecessor);
            }
            ClearAttempt::Lost(code, evidence) => {
                assert!(matches!(
                    code,
                    ThreadClearErrorCode::TransitionConflict
                        | ThreadClearErrorCode::TransitionCompleted
                ));
                assert!(evidence.is_empty());
            }
        }
    }

    process.kill().await.context("failed to stop app-server")?;
    Ok(())
}

async fn initialize(stream: &mut WsClient, id: i64, name: &str) -> Result<()> {
    send_initialize_request(stream, id, name).await?;
    read_response_for_id(stream, id).await?;
    Ok(())
}

async fn start_thread(stream: &mut WsClient, id: i64) -> Result<String> {
    start_thread_with_config(stream, id, None).await
}

async fn start_thread_with_config(
    stream: &mut WsClient,
    id: i64,
    config: Option<HashMap<String, serde_json::Value>>,
) -> Result<String> {
    send_request(
        stream,
        "thread/start",
        id,
        Some(serde_json::to_value(ThreadStartParams {
            model: Some("mock-model".to_string()),
            config,
            ..Default::default()
        })?),
    )
    .await?;
    let response: ThreadStartResponse = to_response(read_response_for_id(stream, id).await?)?;
    Ok(response.thread.id)
}

async fn resume_thread(stream: &mut WsClient, id: i64, thread_id: &str) -> Result<()> {
    send_request(
        stream,
        "thread/resume",
        id,
        Some(json!({ "threadId": thread_id })),
    )
    .await?;
    let target = RequestId::Integer(id);
    loop {
        match read_jsonrpc_message(stream).await? {
            JSONRPCMessage::Response(response) if response.id == target => return Ok(()),
            JSONRPCMessage::Error(error) if error.id == target => {
                bail!("thread/resume failed: {error:?}")
            }
            _ => {}
        }
    }
}

async fn read_thread(
    stream: &mut WsClient,
    id: i64,
    thread_id: &str,
) -> Result<ThreadReadResponse> {
    send_request(
        stream,
        "thread/read",
        id,
        Some(serde_json::to_value(ThreadReadParams {
            thread_id: thread_id.to_string(),
            include_turns: false,
        })?),
    )
    .await?;
    to_response(read_response_for_id(stream, id).await?)
}

async fn send_clear(stream: &mut WsClient, id: i64, thread_id: &str) -> Result<()> {
    send_request(
        stream,
        "thread/clear",
        id,
        Some(serde_json::to_value(ThreadClearParams {
            thread_id: thread_id.to_string(),
        })?),
    )
    .await
}

async fn read_clear_outcome(
    stream: &mut WsClient,
    id: i64,
) -> Result<(ThreadClearResponse, Vec<ClearEvidence>)> {
    let target = RequestId::Integer(id);
    let mut evidence = Vec::new();
    loop {
        match read_jsonrpc_message(stream).await? {
            JSONRPCMessage::Error(error) if error.id == target => {
                bail!("thread/clear failed: {error:?}");
            }
            JSONRPCMessage::Response(response) if response.id == target => {
                if evidence.len() != 2 {
                    bail!("thread/clear responded before its complete evidence pair");
                }
                return Ok((to_response(response)?, evidence));
            }
            JSONRPCMessage::Notification(notification)
                if notification.method == "thread/clear/ended" =>
            {
                evidence.push(ClearEvidence::Ended(serde_json::from_value(
                    notification.params.context("clear/ended params")?,
                )?));
            }
            JSONRPCMessage::Notification(notification)
                if notification.method == "thread/clear/started" =>
            {
                evidence.push(ClearEvidence::Started(Box::new(serde_json::from_value(
                    notification.params.context("clear/started params")?,
                )?)));
            }
            _ => {}
        }
    }
}

async fn read_clear_attempt(stream: &mut WsClient, id: i64) -> Result<ClearAttempt> {
    let target = RequestId::Integer(id);
    let mut evidence = Vec::new();
    loop {
        match read_jsonrpc_message(stream).await? {
            JSONRPCMessage::Response(response) if response.id == target => {
                return Ok(ClearAttempt::Won(
                    Box::new(to_response(response)?),
                    evidence,
                ));
            }
            JSONRPCMessage::Error(error) if error.id == target => {
                let code = error
                    .error
                    .data
                    .and_then(|data| data.get("code").cloned())
                    .context("thread/clear error must carry stable code")?;
                return Ok(ClearAttempt::Lost(serde_json::from_value(code)?, evidence));
            }
            JSONRPCMessage::Notification(notification)
                if notification.method == "thread/clear/ended" =>
            {
                evidence.push(ClearEvidence::Ended(serde_json::from_value(
                    notification.params.context("clear/ended params")?,
                )?));
            }
            JSONRPCMessage::Notification(notification)
                if notification.method == "thread/clear/started" =>
            {
                evidence.push(ClearEvidence::Started(Box::new(serde_json::from_value(
                    notification.params.context("clear/started params")?,
                )?)));
            }
            _ => {}
        }
    }
}

async fn assert_no_clear_evidence(stream: &mut WsClient) -> Result<()> {
    loop {
        let Ok(message) = timeout(Duration::from_millis(300), read_jsonrpc_message(stream)).await
        else {
            return Ok(());
        };
        match message? {
            JSONRPCMessage::Notification(notification)
                if matches!(
                    notification.method.as_str(),
                    "thread/clear/ended" | "thread/clear/started"
                ) =>
            {
                bail!("bystander received requester-scoped clear evidence")
            }
            _ => {}
        }
    }
}

async fn read_non_authoritative_clear_broadcast(
    stream: &mut WsClient,
    predecessor_thread_id: &str,
    successor_thread_id: &str,
) -> Result<ThreadStartedNotification> {
    timeout(Duration::from_secs(5), async {
        loop {
            match read_jsonrpc_message(stream).await? {
                JSONRPCMessage::Notification(notification)
                    if matches!(
                        notification.method.as_str(),
                        "thread/clear/ended" | "thread/clear/started"
                    ) =>
                {
                    bail!("bystander received requester-scoped clear evidence")
                }
                JSONRPCMessage::Notification(notification)
                    if notification.method == "thread/started" =>
                {
                    let params = notification.params.context("thread/started params")?;
                    let started: ThreadStartedNotification =
                        serde_json::from_value(params.clone())?;
                    if started.thread.id == successor_thread_id {
                        assert_eq!(
                            started.clear_predecessor_thread_id.as_deref(),
                            Some(predecessor_thread_id)
                        );
                        assert!(
                            params.get("transitionId").is_none(),
                            "non-authoritative thread/started must not carry transition identity"
                        );
                        return Ok(started);
                    }
                }
                _ => {}
            }
        }
    })
    .await
    .context("timed out waiting for non-authoritative clear thread/started broadcast")?
}

async fn assert_clear_error(
    stream: &mut WsClient,
    id: i64,
    expected: ThreadClearErrorCode,
) -> Result<()> {
    let error = read_error_for_id(stream, id).await?;
    assert_eq!(error.error.data, Some(json!({ "code": expected })));
    Ok(())
}

async fn assert_unsubscribe_status(
    stream: &mut WsClient,
    id: i64,
    thread_id: &str,
    expected: &str,
) -> Result<()> {
    send_request(
        stream,
        "thread/unsubscribe",
        id,
        Some(json!({ "threadId": thread_id })),
    )
    .await?;
    let response = read_response_for_id(stream, id).await?;
    assert_eq!(
        response.result.get("status").and_then(|v| v.as_str()),
        Some(expected)
    );
    Ok(())
}
