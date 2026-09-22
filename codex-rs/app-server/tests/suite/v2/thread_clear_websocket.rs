use super::connection_handling_websocket::WsClient;
use super::connection_handling_websocket::connect_websocket;
use super::connection_handling_websocket::create_config_toml;
use super::connection_handling_websocket::read_error_for_id;
use super::connection_handling_websocket::read_jsonrpc_message;
use super::connection_handling_websocket::read_response_for_id;
use super::connection_handling_websocket::send_initialize_request;
use super::connection_handling_websocket::send_request;
use super::connection_handling_websocket::spawn_websocket_server;
use super::connection_handling_websocket::spawn_websocket_server_with_args;
use super::connection_handling_websocket::spawn_websocket_server_with_env;
use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use app_test_support::create_fake_rollout;
use app_test_support::create_mock_responses_server_repeating_assistant;
use app_test_support::create_mock_responses_server_sequence_unchecked;
use app_test_support::rollout_path;
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
use codex_state::ClearTransitionEvidenceKind;
use codex_state::ClearTransitionEvidenceState;
use codex_state::ClearTransitionId;
use codex_state::ClearTransitionPhase;
use codex_state::ClearTransitionReserveOutcome;
use codex_state::StateRuntime;
use core_test_support::PathExt;
use serde_json::json;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::str::FromStr;
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
async fn clear_successor_preserves_thread_source_after_preview_and_cold_resume() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;
    let sqlite_home = codex_home.path().to_str().context("UTF-8 test home")?;
    let (mut process, bind_addr) =
        spawn_websocket_server_with_env(codex_home.path(), &[("CODEX_SQLITE_HOME", sqlite_home)])
            .await?;
    let mut client = connect_websocket(bind_addr).await?;
    send_request(
        &mut client,
        "initialize",
        1,
        Some(json!({
            "clientInfo": { "name": "classification-test", "version": "1" },
            "capabilities": { "experimentalApi": true }
        })),
    )
    .await?;
    read_response_for_id(&mut client, 1).await?;
    send_request(
        &mut client,
        "thread/start",
        2,
        Some(json!({ "model": "mock-model", "threadSource": "user" })),
    )
    .await?;
    let started: ThreadStartResponse = to_response(read_response_for_id(&mut client, 2).await?)?;
    let predecessor = started.thread.id;
    send_clear(&mut client, 3, &predecessor).await?;
    let (cleared, _) = read_clear_outcome(&mut client, 3).await?;
    let successor = cleared.successor_thread.id;
    let before_preview = read_thread(&mut client, 4, &successor).await?;
    assert_eq!(
        serde_json::to_value(&before_preview.thread)?["threadSource"],
        "user"
    );

    // A real turn also exercises the preview-bearing legacy read path.
    // No synthetic metadata or live fallback.
    send_request(
        &mut client,
        "turn/start",
        5,
        Some(json!({
            "threadId": successor,
            "input": [{ "type": "text", "text": "classification preview", "text_elements": [] }]
        })),
    )
    .await?;
    read_response_for_id(&mut client, 5).await?;
    timeout(Duration::from_secs(60), async {
        loop {
            if let JSONRPCMessage::Notification(notification) =
                read_jsonrpc_message(&mut client).await?
                && notification.method == "turn/completed"
            {
                let params = notification.params.context("turn completion params")?;
                assert_eq!(params["threadId"], successor);
                assert_eq!(params["turn"]["status"], "completed");
                return Ok::<_, anyhow::Error>(());
            }
        }
    })
    .await??;
    // Verify persisted history as well as the preview-only projection.
    send_request(
        &mut client,
        "thread/read",
        6,
        Some(json!({
            "threadId": successor, "includeTurns": true
        })),
    )
    .await?;
    let persisted: ThreadReadResponse = to_response(read_response_for_id(&mut client, 6).await?)?;
    assert!(!persisted.thread.turns.is_empty());
    let after_preview = read_thread(&mut client, 7, &successor).await?;
    assert_eq!(after_preview.thread.preview, "classification preview");
    assert_eq!(
        serde_json::to_value(&after_preview.thread)?["threadSource"],
        "user"
    );

    // This WebSocket observer has no trusted interactive attachment. Its
    // attachment state must not turn a persisted user classification into null.
    send_request(&mut client, "thread/attachment/list", 8, Some(json!({}))).await?;
    let attachments = read_response_for_id(&mut client, 8).await?;
    let entries = attachments.result["entries"]
        .as_array()
        .context("attachment entries")?;
    assert!(!entries.iter().any(|entry| {
        entry["threadId"] == successor
            && entry["interactiveAttachmentCount"]
                .as_u64()
                .unwrap_or_default()
                > 0
    }));

    // Restarting the disposable server proves an unloaded read cannot be
    // rescued by an in-memory session classification.
    process.kill().await?;
    drop(client);
    let (mut restarted, bind_addr) =
        spawn_websocket_server_with_env(codex_home.path(), &[("CODEX_SQLITE_HOME", sqlite_home)])
            .await?;
    let mut client = connect_websocket(bind_addr).await?;
    initialize(&mut client, 9, "cold-classification-test").await?;
    let unloaded = read_thread(&mut client, 10, &successor).await?;
    assert_eq!(
        serde_json::to_value(&unloaded.thread)?["threadSource"],
        "user"
    );
    assert_eq!(
        unloaded.thread.status,
        codex_app_server_protocol::ThreadStatus::NotLoaded
    );
    resume_thread(&mut client, 11, &successor).await?;
    let resumed = read_thread(&mut client, 12, &successor).await?;
    assert_eq!(
        serde_json::to_value(&resumed.thread)?["threadSource"],
        "user"
    );
    restarted.kill().await?;
    Ok(())
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
async fn startup_recovery_converges_every_persisted_phase_without_replay() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;
    let state = StateRuntime::init(
        codex_state::SqliteConfig::new_for_testing(codex_home.path().abs()),
        "test-provider".to_string(),
    )
    .await?;
    let mut cases = Vec::new();
    for (index, phase) in [
        ClearTransitionPhase::Reserved,
        ClearTransitionPhase::SuccessorCreated,
        ClearTransitionPhase::Committed,
        ClearTransitionPhase::EvidenceClaimed,
        ClearTransitionPhase::Completed,
    ]
    .into_iter()
    .enumerate()
    {
        let transition_id = ClearTransitionId::new();
        let predecessor = ThreadId::new();
        let successor =
            create_clear_successor_rollout(codex_home.path(), index, predecessor, transition_id)?;
        let ClearTransitionReserveOutcome::Reserved(_) = state
            .reserve_clear_transition(transition_id, predecessor, successor)
            .await?
        else {
            bail!("expected fresh transition reservation");
        };
        seed_transition_phase(&state, transition_id, phase).await?;
        if phase == ClearTransitionPhase::EvidenceClaimed {
            seed_evidence_state(
                &state,
                transition_id,
                ClearTransitionEvidenceKind::End,
                ClearTransitionEvidenceState::Delivered,
            )
            .await?;
            seed_evidence_state(
                &state,
                transition_id,
                ClearTransitionEvidenceKind::Start,
                ClearTransitionEvidenceState::Claimed,
            )
            .await?;
        } else if phase == ClearTransitionPhase::Completed {
            seed_evidence_state(
                &state,
                transition_id,
                ClearTransitionEvidenceKind::End,
                ClearTransitionEvidenceState::Delivered,
            )
            .await?;
            seed_evidence_state(
                &state,
                transition_id,
                ClearTransitionEvidenceKind::Start,
                ClearTransitionEvidenceState::Delivered,
            )
            .await?;
        }
        cases.push((transition_id, phase, predecessor, successor));
    }
    drop(state);

    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;
    let mut client = connect_websocket(bind_addr).await?;
    initialize(&mut client, 1, "recovery-matrix").await?;
    assert_no_clear_evidence(&mut client).await?;

    let state = StateRuntime::init(
        codex_state::SqliteConfig::new_for_testing(codex_home.path().abs()),
        "test-provider".to_string(),
    )
    .await?;
    for (transition_id, original_phase, predecessor, successor) in cases {
        let recovered = state
            .get_clear_transition(transition_id)
            .await?
            .context("recovered transition")?;
        assert_eq!(recovered.phase, ClearTransitionPhase::Completed);
        assert_eq!(recovered.predecessor_thread_id, predecessor);
        assert_eq!(recovered.successor_thread_id, successor);
        if original_phase == ClearTransitionPhase::Completed {
            assert_eq!(
                recovered.end_evidence_state,
                ClearTransitionEvidenceState::Delivered
            );
            assert_eq!(
                recovered.start_evidence_state,
                ClearTransitionEvidenceState::Delivered
            );
        } else if original_phase == ClearTransitionPhase::EvidenceClaimed {
            assert_eq!(
                recovered.end_evidence_state,
                ClearTransitionEvidenceState::Delivered
            );
            assert_eq!(
                recovered.start_evidence_state,
                ClearTransitionEvidenceState::Failed
            );
        } else {
            assert_eq!(
                recovered.end_evidence_state,
                ClearTransitionEvidenceState::Failed
            );
            assert_eq!(
                recovered.start_evidence_state,
                ClearTransitionEvidenceState::Failed
            );
        }
    }

    process.kill().await.context("failed to stop app-server")?;
    Ok(())
}

#[tokio::test]
async fn reconnect_after_completed_clear_restores_exact_successor_without_replay() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;
    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;
    let mut requester = connect_websocket(bind_addr).await?;
    initialize(&mut requester, 1, "original-requester").await?;
    let predecessor = start_thread(&mut requester, 2).await?;
    send_clear(&mut requester, 3, &predecessor).await?;
    let (response, _) = read_clear_outcome(&mut requester, 3).await?;
    let successor = response.successor_thread.id.clone();
    drop(requester);

    let mut reconnected = connect_websocket(bind_addr).await?;
    initialize(&mut reconnected, 4, "reconnected-requester").await?;
    resume_thread(&mut reconnected, 5, &successor).await?;
    assert_no_clear_evidence(&mut reconnected).await?;
    let restored = read_thread(&mut reconnected, 6, &successor).await?;
    assert_eq!(restored.thread.id, successor);

    let state = StateRuntime::init(
        codex_state::SqliteConfig::new_for_testing(codex_home.path().abs()),
        "test-provider".to_string(),
    )
    .await?;
    let transition_id = ClearTransitionId::from_str(&response.transition_id)?;
    let record = state
        .get_clear_transition(transition_id)
        .await?
        .context("completed transition after reconnect")?;
    assert_eq!(record.predecessor_thread_id.to_string(), predecessor);
    assert_eq!(record.successor_thread_id.to_string(), restored.thread.id);
    assert_eq!(record.phase, ClearTransitionPhase::Completed);

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

#[tokio::test]
async fn clear_hook_timeout_is_nonfatal_and_transition_still_completes() -> Result<()> {
    run_failed_clear_hook_case("import time\ntime.sleep(5)\n", 1).await
}

#[tokio::test]
async fn clear_hook_nonzero_exit_is_nonfatal_and_transition_still_completes() -> Result<()> {
    run_failed_clear_hook_case(
        "import sys\nsys.stderr.write('clear hook exited seven')\nsys.exit(7)\n",
        3,
    )
    .await
}

#[tokio::test]
async fn clear_hook_launch_failure_is_nonfatal_and_transition_still_completes() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;
    enable_codex_hooks(codex_home.path())?;
    std::fs::write(
        codex_home.path().join("hooks.json"),
        json!({
            "hooks": {
                "SessionEnd": [{
                    "matcher": "clear",
                    "hooks": [{
                        "type": "command",
                        "command": "/definitely/missing/codex-clear-hook",
                        "timeout": 3
                    }]
                }]
            }
        })
        .to_string(),
    )?;
    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;
    let mut client = connect_websocket(bind_addr).await?;
    initialize(&mut client, 1, "hook-launch-failure").await?;
    let predecessor = start_thread_with_config(
        &mut client,
        2,
        Some(HashMap::from([(
            "bypass_hook_trust".to_string(),
            json!(true),
        )])),
    )
    .await?;

    send_clear(&mut client, 3, &predecessor).await?;
    let (response, evidence) = read_clear_outcome(&mut client, 3).await?;
    assert_eq!(evidence.len(), 2);
    assert_completed_transition(codex_home.path(), &response.transition_id).await?;

    process.kill().await.context("failed to stop app-server")?;
    Ok(())
}

#[tokio::test]
async fn clear_hook_serialization_failure_is_nonfatal_and_transition_still_completes() -> Result<()>
{
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;
    enable_codex_hooks(codex_home.path())?;
    let unexpected_marker = codex_home.path().join("serialization-hook-ran");
    std::fs::write(
        codex_home.path().join("hooks.json"),
        json!({
            "hooks": {
                "SessionEnd": [{
                    "matcher": "clear",
                    "hooks": [{
                        "type": "command",
                        "command": format!("touch {}", unexpected_marker.display()),
                        "timeout": 3
                    }]
                }]
            }
        })
        .to_string(),
    )?;
    let (mut process, bind_addr) = spawn_websocket_server_with_env(
        codex_home.path(),
        &[(
            "CODEX_HOOKS_SESSION_END_SERIALIZATION_FAILURE_FOR_TESTS",
            "1",
        )],
    )
    .await?;
    let mut client = connect_websocket(bind_addr).await?;
    initialize(&mut client, 1, "hook-serialization-failure").await?;
    let predecessor = start_thread_with_config(
        &mut client,
        2,
        Some(HashMap::from([(
            "bypass_hook_trust".to_string(),
            json!(true),
        )])),
    )
    .await?;

    send_clear(&mut client, 3, &predecessor).await?;
    let (response, evidence) = read_clear_outcome(&mut client, 3).await?;
    assert_eq!(evidence.len(), 2);
    assert!(
        !unexpected_marker.exists(),
        "serialization failure must occur before handler launch"
    );
    assert_completed_transition(codex_home.path(), &response.transition_id).await?;

    process.kill().await.context("failed to stop app-server")?;
    Ok(())
}

#[tokio::test]
async fn cleared_and_control_threads_keep_operational_other_idle_unload() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;
    let hook_log = write_lifecycle_logging_hooks(codex_home.path())?;
    let (mut process, bind_addr) = spawn_websocket_server_with_args(
        codex_home.path(),
        "ws://127.0.0.1:0",
        &["-c".to_string(), "thread_unload_delay_secs=1".to_string()],
    )
    .await?;
    let mut client = connect_websocket(bind_addr).await?;
    initialize(&mut client, 1, "idle-unload").await?;

    let config = Some(HashMap::from([(
        "bypass_hook_trust".to_string(),
        json!(true),
    )]));
    let predecessor = start_thread_with_config(&mut client, 2, config.clone()).await?;
    send_clear(&mut client, 3, &predecessor).await?;
    let (response, _) = read_clear_outcome(&mut client, 3).await?;
    wait_for_thread_closed(&mut client, &predecessor).await?;

    let control = start_thread_with_config(&mut client, 4, config).await?;
    assert_unsubscribe_status(&mut client, 5, &control, "unsubscribed").await?;
    wait_for_thread_closed(&mut client, &control).await?;

    let payloads = wait_for_hook_payloads(&hook_log, 3).await?;
    assert_eq!(
        payloads
            .iter()
            .map(|payload| {
                (
                    payload["session_id"].as_str().unwrap_or_default(),
                    payload["reason"].as_str().unwrap_or_default(),
                )
            })
            .collect::<Vec<_>>(),
        vec![
            (predecessor.as_str(), "clear"),
            (predecessor.as_str(), "other"),
            (control.as_str(), "other"),
        ]
    );
    assert_eq!(response.predecessor_thread_id, predecessor);

    process.kill().await.context("failed to stop app-server")?;
    Ok(())
}

async fn run_failed_clear_hook_case(script: &str, timeout_sec: u64) -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;
    enable_codex_hooks(codex_home.path())?;
    let started_marker = write_clear_failure_hook(codex_home.path(), script, timeout_sec)?;
    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;
    let mut client = connect_websocket(bind_addr).await?;
    initialize(&mut client, 1, "hook-failure").await?;
    let predecessor = start_thread_with_config(
        &mut client,
        2,
        Some(HashMap::from([(
            "bypass_hook_trust".to_string(),
            json!(true),
        )])),
    )
    .await?;

    send_clear(&mut client, 3, &predecessor).await?;
    let (response, evidence) = read_clear_outcome(&mut client, 3).await?;
    assert_eq!(evidence.len(), 2);
    assert!(started_marker.exists(), "failed hook never started");

    assert_completed_transition(codex_home.path(), &response.transition_id).await?;

    // The hook engine unit tests assert the exact diagnostic for this failure
    // mode. This integration boundary proves that the same real handler was
    // entered and that its failure cannot cancel or corrupt A/B/T.

    process.kill().await.context("failed to stop app-server")?;
    Ok(())
}

async fn assert_completed_transition(codex_home: &Path, transition_id: &str) -> Result<()> {
    let state = StateRuntime::init(
        codex_state::SqliteConfig::new_for_testing(codex_home.abs()),
        "test-provider".to_string(),
    )
    .await?;
    let transition_id = ClearTransitionId::from_str(transition_id)?;
    let record = state
        .get_clear_transition(transition_id)
        .await?
        .context("completed clear transition")?;
    assert_eq!(record.phase, ClearTransitionPhase::Completed);
    Ok(())
}

fn enable_codex_hooks(codex_home: &Path) -> Result<()> {
    let config_path = codex_home.join("config.toml");
    let mut config = std::fs::read_to_string(&config_path)?;
    config.push_str("\n[features]\ncodex_hooks = true\n");
    std::fs::write(config_path, config)?;
    Ok(())
}

fn write_clear_failure_hook(
    codex_home: &Path,
    script: &str,
    timeout_sec: u64,
) -> Result<std::path::PathBuf> {
    let script_path = codex_home.join("clear-failure-hook.py");
    let started_marker = codex_home.join("clear-failure-hook.started");
    std::fs::write(
        &script_path,
        format!(
            "from pathlib import Path\nPath(r\"{}\").write_text('started', encoding='utf-8')\n{script}",
            started_marker.display()
        ),
    )?;
    std::fs::write(
        codex_home.join("hooks.json"),
        json!({
            "hooks": {
                "SessionEnd": [{
                    "matcher": "clear",
                    "hooks": [{
                        "type": "command",
                        "command": format!("python3 {}", script_path.display()),
                        "timeout": timeout_sec
                    }]
                }]
            }
        })
        .to_string(),
    )?;
    Ok(started_marker)
}

fn write_lifecycle_logging_hooks(codex_home: &Path) -> Result<std::path::PathBuf> {
    let log_path = codex_home.join("clear-lifecycle.jsonl");
    let script_path = codex_home.join("clear-lifecycle-hook.py");
    std::fs::write(
        &script_path,
        format!(
            r#"import json
from pathlib import Path
import sys

payload = json.load(sys.stdin)
with Path(r"{}").open("a", encoding="utf-8") as handle:
    handle.write(json.dumps(payload) + "\n")
"#,
            log_path.display()
        ),
    )?;
    let command = format!("python3 {}", script_path.display());
    std::fs::write(
        codex_home.join("hooks.json"),
        json!({
            "hooks": {
                "SessionEnd": [
                    {
                        "matcher": "clear",
                        "hooks": [{ "type": "command", "command": command, "timeout": 3 }]
                    },
                    {
                        "matcher": "other",
                        "hooks": [{ "type": "command", "command": command, "timeout": 3 }]
                    }
                ]
            }
        })
        .to_string(),
    )?;
    Ok(log_path)
}

async fn wait_for_hook_payloads(
    log_path: &Path,
    expected: usize,
) -> Result<Vec<serde_json::Value>> {
    timeout(Duration::from_secs(10), async {
        loop {
            let payloads = std::fs::read_to_string(log_path)
                .unwrap_or_default()
                .lines()
                .map(serde_json::from_str)
                .collect::<Result<Vec<_>, _>>()?;
            if payloads.len() >= expected {
                return Ok(payloads);
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .context("timed out waiting for lifecycle hook payloads")?
}

async fn wait_for_thread_closed(stream: &mut WsClient, thread_id: &str) -> Result<()> {
    timeout(Duration::from_secs(10), async {
        loop {
            if let JSONRPCMessage::Notification(notification) = read_jsonrpc_message(stream).await?
                && notification.method == "thread/closed"
                && notification
                    .params
                    .as_ref()
                    .and_then(|params| params.get("threadId"))
                    .and_then(serde_json::Value::as_str)
                    == Some(thread_id)
            {
                return Ok(());
            }
        }
    })
    .await
    .context("timed out waiting for idle thread unload")?
}

fn create_clear_successor_rollout(
    codex_home: &Path,
    index: usize,
    predecessor: ThreadId,
    transition_id: ClearTransitionId,
) -> Result<ThreadId> {
    let filename_ts = format!("2025-01-01T00-00-{index:02}");
    let thread_id = create_fake_rollout(
        codex_home,
        &filename_ts,
        "2025-01-01T00:00:00Z",
        "clear successor",
        Some("mock-provider"),
        /*git_info*/ None,
    )?;
    let rollout_path = rollout_path(codex_home, &filename_ts, &thread_id);
    let mut lines = fs::read_to_string(&rollout_path)?
        .lines()
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<Result<Vec<_>, _>>()?;
    lines[0]["payload"]["clear_predecessor_thread_id"] = json!(predecessor.to_string());
    lines[0]["payload"]["clear_transition_id"] = json!(transition_id.to_string());
    let contents = lines
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(rollout_path, format!("{contents}\n"))?;
    ThreadId::from_string(&thread_id).map_err(Into::into)
}

async fn seed_transition_phase(
    state: &StateRuntime,
    transition_id: ClearTransitionId,
    target: ClearTransitionPhase,
) -> Result<()> {
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
        (
            ClearTransitionPhase::EvidenceClaimed,
            ClearTransitionPhase::Completed,
        ),
    ] {
        if expected == target {
            break;
        }
        assert!(
            state
                .advance_clear_transition_phase(transition_id, expected, next)
                .await?
        );
        if next == target {
            break;
        }
    }
    Ok(())
}

async fn seed_evidence_state(
    state: &StateRuntime,
    transition_id: ClearTransitionId,
    kind: ClearTransitionEvidenceKind,
    target: ClearTransitionEvidenceState,
) -> Result<()> {
    if target == ClearTransitionEvidenceState::Pending {
        return Ok(());
    }
    assert!(
        state
            .advance_clear_transition_evidence(
                transition_id,
                kind,
                ClearTransitionEvidenceState::Pending,
                ClearTransitionEvidenceState::Claimed,
            )
            .await?
    );
    if target != ClearTransitionEvidenceState::Claimed {
        assert!(
            state
                .advance_clear_transition_evidence(
                    transition_id,
                    kind,
                    ClearTransitionEvidenceState::Claimed,
                    target,
                )
                .await?
        );
    }
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
