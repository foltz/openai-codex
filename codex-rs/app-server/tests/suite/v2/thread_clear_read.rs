use super::*;
use pretty_assertions::assert_eq;
use serde_json::Value;

async fn initialize_reader(client: &mut WsClient) -> Result<()> {
    send_request(
        client,
        "initialize",
        1,
        Some(json!({
            "clientInfo": {"name": "clear-reader-test", "version": "1"},
            "capabilities": {"experimentalApi": true}
        })),
    )
    .await?;
    read_response_for_id(client, 1).await?;
    Ok(())
}

async fn read_transition(client: &mut WsClient, id: i64, successor: &str) -> Result<Value> {
    send_request(
        client,
        "thread/clear/read",
        id,
        Some(json!({"successorThreadId": successor})),
    )
    .await?;
    Ok(read_response_for_id(client, id).await?.result)
}

fn transition(disposition: &str, predecessor: &str, successor: &str, id: &str) -> Value {
    json!({"disposition": disposition, "transition": {
        "transitionId": id, "predecessorThreadId": predecessor, "successorThreadId": successor
    }})
}

#[tokio::test]
async fn clear_read_seeded_phases_need_no_rollout_and_do_not_mutate_state() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let home = TempDir::new()?;
    create_config_toml(home.path(), &server.uri(), "never")?;
    let (mut process, addr) = spawn_websocket_server_with_env(
        home.path(),
        &[("CODEX_SQLITE_HOME", home.path().to_str().context("home")?)],
    )
    .await?;
    let mut client = connect_websocket(addr).await?;
    initialize_reader(&mut client).await?;
    // Seed after startup so reconciliation cannot consume the fixture. There
    // is deliberately no successor rollout, including in pre-persistence phases.
    let state = StateRuntime::init(
        codex_state::SqliteConfig::new_for_testing(home.path().abs()),
        "test-provider".into(),
    )
    .await?;
    for phase in [
        ClearTransitionPhase::Reserved,
        ClearTransitionPhase::SuccessorCreated,
        ClearTransitionPhase::Committed,
        ClearTransitionPhase::EvidenceClaimed,
        ClearTransitionPhase::Completed,
    ] {
        let a = ThreadId::new();
        let b = ThreadId::new();
        let t = ClearTransitionId::new();
        state.reserve_clear_transition(t, a, b).await?;
        seed_transition_phase(&state, t, phase).await?;
        let before = state.get_clear_transition(t).await?;
        let disposition = if phase == ClearTransitionPhase::Completed {
            "complete"
        } else {
            "pending"
        };
        let expected = transition(disposition, &a.to_string(), &b.to_string(), &t.to_string());
        assert_eq!(
            read_transition(&mut client, 2, &b.to_string()).await?,
            expected
        );
        assert_eq!(
            read_transition(&mut client, 3, &b.to_string()).await?,
            expected
        );
        assert_eq!(state.get_clear_transition(t).await?, before);
        assert_eq!(
            read_transition(&mut client, 4, &a.to_string()).await?,
            json!({"disposition":"none", "transition":null})
        );
    }
    assert!(!home.path().join("sessions").exists());
    let a = ThreadId::new();
    let b = ThreadId::new();
    let t = ClearTransitionId::new();
    state.reserve_clear_transition(t, a, b).await?;
    assert_eq!(
        read_transition(&mut client, 5, &b.to_string()).await?,
        transition("pending", &a.to_string(), &b.to_string(), &t.to_string())
    );
    assert!(
        state
            .abandon_clear_transition(t, ClearTransitionPhase::Reserved)
            .await?
    );
    assert_eq!(
        read_transition(&mut client, 6, &b.to_string()).await?,
        json!({"disposition":"none", "transition":null})
    );
    send_request(
        &mut client,
        "thread/clear/read",
        7,
        Some(json!({"successorThreadId":"not-a-thread"})),
    )
    .await?;
    assert_eq!(read_error_for_id(&mut client, 7).await?.error.code, -32600);
    // Corrupt only this disposable database. The public adapter must not turn
    // a failed decode into the successful absence variant.
    let b = ThreadId::new();
    state
        .reserve_clear_transition(ClearTransitionId::new(), ThreadId::new(), b)
        .await?;
    let output = tokio::process::Command::new("python3")
        .arg("-c")
        .arg("import sqlite3, sys; db = sqlite3.connect(sys.argv[1]); db.execute(\"UPDATE clear_transitions SET predecessor_thread_id = 'invalid' WHERE successor_thread_id = ?\", (sys.argv[2],)); db.commit()")
        .arg(state.sqlite().state_db_path().as_path())
        .arg(b.to_string())
        .output().await?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    send_request(
        &mut client,
        "thread/clear/read",
        8,
        Some(json!({"successorThreadId": b.to_string()})),
    )
    .await?;
    let error = read_error_for_id(&mut client, 8).await?;
    assert_eq!(error.error.code, -32603);
    assert!(
        error
            .error
            .message
            .contains("failed to read clear transition")
    );
    process.kill().await?;
    Ok(())
}

#[tokio::test]
async fn clear_read_real_chain_survives_restart_and_cold_resume() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let home = TempDir::new()?;
    create_config_toml(home.path(), &server.uri(), "never")?;
    let env = [("CODEX_SQLITE_HOME", home.path().to_str().context("home")?)];
    let (mut process, addr) = spawn_websocket_server_with_env(home.path(), &env).await?;
    let mut client = connect_websocket(addr).await?;
    initialize_reader(&mut client).await?;
    let a = start_thread(&mut client, 2).await?;
    let none = json!({"disposition":"none", "transition":null});
    assert_eq!(read_transition(&mut client, 3, &a).await?, none);
    send_clear(&mut client, 4, &a).await?;
    let (first, evidence) = read_clear_outcome(&mut client, 4).await?;
    assert_eq!(evidence.len(), 2);
    let b = first.successor_thread.id;
    let ab = transition("complete", &a, &b, &first.transition_id);
    assert_eq!(read_transition(&mut client, 5, &b).await?, ab);
    send_clear(&mut client, 6, &b).await?;
    let (second, _) = read_clear_outcome(&mut client, 6).await?;
    let c = second.successor_thread.id;
    let bc = transition("complete", &b, &c, &second.transition_id);
    assert_eq!(read_transition(&mut client, 7, &c).await?, bc);
    assert_eq!(read_transition(&mut client, 8, &b).await?, ab);
    assert_eq!(read_transition(&mut client, 9, &a).await?, none);
    // Fixture control only: these WebSocket clients are never trusted-interactive.
    // The trusted disconnect/move regression lives in thread_state's unit tests.
    send_request(&mut client, "thread/attachment/list", 10, Some(json!({}))).await?;
    assert_eq!(
        read_response_for_id(&mut client, 10).await?.result["entries"],
        json!([])
    );
    process.kill().await?;
    let (mut process, addr) = spawn_websocket_server_with_env(home.path(), &env).await?;
    let mut client = connect_websocket(addr).await?;
    initialize_reader(&mut client).await?;
    // No loaded thread or rollout read is required by the observation API.
    assert_eq!(read_transition(&mut client, 2, &b).await?, ab);
    assert_eq!(read_transition(&mut client, 3, &c).await?, bc);
    resume_thread(&mut client, 4, &b).await?;
    assert_eq!(read_transition(&mut client, 5, &b).await?, ab);
    // The empty predecessor need not have materialized a rollout. Use an
    // ordinary persisted thread for the non-successor cold-resume control.
    let ordinary = create_fake_rollout(
        home.path(),
        "2025-01-01T00-00-01",
        "2025-01-01T00:00:01Z",
        "ordinary thread",
        Some("mock_provider"),
        None,
    )?;
    resume_thread(&mut client, 6, &ordinary).await?;
    assert_eq!(read_transition(&mut client, 7, &ordinary).await?, none);
    send_request(&mut client, "thread/fork", 8, Some(json!({"threadId": b}))).await?;
    let fork = read_response_for_id(&mut client, 8).await?;
    let fork_id = fork.result["thread"]["id"].as_str().context("fork id")?;
    assert_eq!(read_transition(&mut client, 9, fork_id).await?, none);
    process.kill().await?;
    Ok(())
}

#[tokio::test]
async fn clear_read_is_concurrent_with_hook_and_complete_after_requester_disconnect() -> Result<()>
{
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let home = TempDir::new()?;
    create_config_toml(home.path(), &server.uri(), "never")?;
    enable_codex_hooks(home.path())?;
    let release = home.path().join("release-hook");
    let script = format!(
        "import time\nwhile not Path({:?}).exists():\n    time.sleep(0.02)\n",
        release.to_str().context("release path")?
    );
    let marker = write_clear_failure_hook(home.path(), &script, 10)?;
    let (mut process, addr) = spawn_websocket_server_with_env(
        home.path(),
        &[("CODEX_SQLITE_HOME", home.path().to_str().context("home")?)],
    )
    .await?;
    let mut requester = connect_websocket(addr).await?;
    initialize_reader(&mut requester).await?;
    let a = start_thread_with_config(
        &mut requester,
        2,
        Some(HashMap::from([("bypass_hook_trust".into(), json!(true))])),
    )
    .await?;
    let mut observer = connect_websocket(addr).await?;
    initialize_reader(&mut observer).await?;
    send_clear(&mut requester, 3, &a).await?;
    timeout(Duration::from_secs(5), async {
        while !marker.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    let state = StateRuntime::init(
        codex_state::SqliteConfig::new_for_testing(home.path().abs()),
        "test-provider".into(),
    )
    .await?;
    let record = state
        .get_clear_transition_by_predecessor(ThreadId::from_string(&a)?)
        .await?
        .context("reserved clear")?;
    assert_eq!(record.phase, ClearTransitionPhase::EvidenceClaimed);
    let b = record.successor_thread_id.to_string();
    let t = record.transition_id.to_string();
    assert_eq!(
        timeout(
            Duration::from_secs(2),
            read_transition(&mut observer, 2, &b)
        )
        .await??,
        transition("pending", &a, &b, &t)
    );
    assert!(!release.exists());
    // Disconnect before allowing clear to reach the attachment move.
    requester.close(None).await?;
    drop(requester);
    fs::write(&release, "release")?;
    timeout(Duration::from_secs(10), async {
        loop {
            let value = read_transition(&mut observer, 3, &b).await?;
            if value["disposition"] == "complete" {
                assert_eq!(value, transition("complete", &a, &b, &t));
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    // Non-discriminating for disconnect: this observer was never entitled.
    // clear_moves_no_attachment_for_disconnected_requester covers trusted cleanup.
    send_request(&mut observer, "thread/attachment/list", 4, Some(json!({}))).await?;
    assert_eq!(
        read_response_for_id(&mut observer, 4).await?.result["entries"],
        json!([])
    );
    process.kill().await?;
    Ok(())
}

#[tokio::test]
async fn clear_read_reconciliation_distinguishes_abandoned_stuck_and_complete() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let home = TempDir::new()?;
    create_config_toml(home.path(), &server.uri(), "never")?;
    let state = StateRuntime::init(
        codex_state::SqliteConfig::new_for_testing(home.path().abs()),
        "test-provider".into(),
    )
    .await?;
    let mut cases = Vec::new();
    for phase in [
        ClearTransitionPhase::Reserved,
        ClearTransitionPhase::SuccessorCreated,
        ClearTransitionPhase::EvidenceClaimed,
    ] {
        let a = ThreadId::new();
        let b = ThreadId::new();
        let t = ClearTransitionId::new();
        state.reserve_clear_transition(t, a, b).await?;
        seed_transition_phase(&state, t, phase).await?;
        cases.push((a, b, t));
    }
    // No rollout: startup abandons Reserved, cannot repair SuccessorCreated,
    // and terminalizes EvidenceClaimed without replaying evidence.
    let (mut process, addr) = spawn_websocket_server_with_env(
        home.path(),
        &[("CODEX_SQLITE_HOME", home.path().to_str().context("home")?)],
    )
    .await?;
    let mut client = connect_websocket(addr).await?;
    initialize_reader(&mut client).await?;
    assert_eq!(
        read_transition(&mut client, 2, &cases[0].1.to_string()).await?,
        json!({"disposition":"none", "transition":null})
    );
    let (a, b, t) = cases[1];
    let pending = transition("pending", &a.to_string(), &b.to_string(), &t.to_string());
    assert_eq!(
        read_transition(&mut client, 3, &b.to_string()).await?,
        pending
    );
    assert_eq!(
        read_transition(&mut client, 4, &b.to_string()).await?,
        pending
    );
    let (a, b, t) = cases[2];
    assert_eq!(
        read_transition(&mut client, 5, &b.to_string()).await?,
        transition("complete", &a.to_string(), &b.to_string(), &t.to_string())
    );
    let recovered = state.get_clear_transition(t).await?.context("recovered")?;
    assert_eq!(
        recovered.end_evidence_state,
        ClearTransitionEvidenceState::Failed
    );
    assert_eq!(
        recovered.start_evidence_state,
        ClearTransitionEvidenceState::Failed
    );
    assert_no_clear_evidence(&mut client).await?;
    process.kill().await?;
    Ok(())
}

#[tokio::test]
async fn clear_read_nonlocal_store_is_error_not_none() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let home = TempDir::new()?;
    create_config_toml(home.path(), &server.uri(), "never")?;
    let config = home.path().join("config.toml");
    fs::write(
        &config,
        format!(
            "{}\n[experimental_thread_store]\ntype = \"in_memory\"\nid = \"clear-read-test\"\n",
            fs::read_to_string(&config)?
        ),
    )?;
    let (mut process, addr) = spawn_websocket_server_with_env(
        home.path(),
        &[("CODEX_SQLITE_HOME", home.path().to_str().context("home")?)],
    )
    .await?;
    let mut client = connect_websocket(addr).await?;
    initialize_reader(&mut client).await?;
    send_request(
        &mut client,
        "thread/clear/read",
        2,
        Some(json!({"successorThreadId": ThreadId::new().to_string()})),
    )
    .await?;
    let error = read_error_for_id(&mut client, 2).await?;
    assert_eq!(error.error.code, -32600);
    assert!(error.error.message.contains("durable local thread store"));
    process.kill().await?;
    Ok(())
}
