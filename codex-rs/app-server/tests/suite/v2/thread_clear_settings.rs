use super::*;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_protocol::config_types::ModeKind;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn clear_requires_model_settings_before_durable_reservation() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let home = TempDir::new()?;
    create_config_toml(home.path(), &server.uri(), "never")?;
    let sqlite_home = home.path().to_str().context("UTF-8 home")?;
    let (mut process, bind_addr) =
        spawn_websocket_server_with_env(home.path(), &[("CODEX_SQLITE_HOME", sqlite_home)]).await?;
    let mut client = connect_websocket(bind_addr).await?;
    initialize(&mut client, 1, "required-clear-settings").await?;
    let predecessor = start_thread(&mut client, 2).await?;
    let state = StateRuntime::init(
        codex_state::SqliteConfig::new_for_testing(home.path().abs()),
        "mock_provider".to_string(),
    )
    .await?;

    for (id, params) in [
        (3, json!({"threadId": predecessor})),
        (4, json!({"threadId": predecessor, "modelSettings": null})),
    ] {
        send_request(&mut client, "thread/clear", id, Some(params)).await?;
        let error = read_error_for_id(&mut client, id).await?;
        assert_eq!(
            serde_json::to_value(error.error)?,
            json!({
                "code": -32600,
                "message": "thread/clear requires resolved modelSettings"
            })
        );
        assert!(
            state
                .get_clear_transition_by_predecessor(ThreadId::from_string(&predecessor)?)
                .await?
                .is_none()
        );
        assert_no_clear_evidence(&mut client).await?;
        assert_eq!(
            read_thread(&mut client, id + 10, &predecessor)
                .await?
                .thread
                .id,
            predecessor
        );
    }
    // A successful follow-up proves the transient authority was released on both errors.
    send_clear(&mut client, 20, &predecessor).await?;
    let (cleared, evidence) = read_clear_outcome(&mut client, 20).await?;
    assert_ne!(cleared.successor_thread.id, predecessor);
    assert_eq!(evidence.len(), 2);
    process.kill().await?;
    Ok(())
}

#[tokio::test]
async fn clear_applies_resolved_scalars_and_resets_plan_without_other_config_changes() -> Result<()>
{
    assert_resolved_clear(json!({"model": "fresh-model", "reasoningEffort": "high"})).await
}

#[tokio::test]
async fn clear_unset_effort_does_not_retain_launch_effort() -> Result<()> {
    assert_resolved_clear(json!({"model": "fresh-model", "reasoningEffort": null})).await
}

#[tokio::test]
async fn clear_unset_model_matches_fresh_start_default() -> Result<()> {
    assert_resolved_clear(json!({"model": null, "reasoningEffort": null})).await
}

async fn assert_resolved_clear(model_settings: serde_json::Value) -> Result<()> {
    let model = model_settings["model"].as_str();
    let effort = model_settings["reasoningEffort"].as_str();
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let home = TempDir::new()?;
    create_config_toml(home.path(), &server.uri(), "never")?;
    let (mut process, bind_addr) = spawn_websocket_server(home.path()).await?;
    let mut client = connect_websocket(bind_addr).await?;
    send_request(
        &mut client,
        "initialize",
        1,
        Some(json!({
            "clientInfo": {"name": "clear-resolved-settings", "version": "1"},
            "capabilities": {"experimentalApi": true}
        })),
    )
    .await?;
    read_response_for_id(&mut client, 1).await?;
    send_request(
        &mut client,
        "thread/start",
        2,
        Some(json!({
            "model": "mock-model", "serviceTier": "flex",
            "config": {"model_reasoning_effort": "medium"}
        })),
    )
    .await?;
    let launch: ThreadStartResponse = to_response(read_response_for_id(&mut client, 2).await?)?;
    let predecessor = launch.thread.id.clone();
    send_request(
        &mut client,
        "thread/settings/update",
        3,
        Some(json!({
            "threadId": predecessor,
            "collaborationMode": {
                "mode": "plan",
                "settings": {
                    "model": "live-only-model", "reasoning_effort": "low",
                    "developer_instructions": "Keep planning without executing."
                }
            }
        })),
    )
    .await?;
    read_response_for_id(&mut client, 3).await?;
    // Match a real fresh start against the same newly saved defaults, including None.
    let path = home.path().join("config.toml");
    let original = fs::read_to_string(&path)?;
    let mut updated = original.replace("model = \"mock-model\"\n", "");
    if let Some(model) = model {
        updated = format!("model = {model:?}\n{updated}");
    }
    if let Some(effort) = effort {
        updated = format!("model_reasoning_effort = {effort:?}\n{updated}");
    }
    fs::write(path, updated)?;
    send_request(&mut client, "thread/start", 4, Some(json!({}))).await?;
    let fresh: ThreadStartResponse = to_response(read_response_for_id(&mut client, 4).await?)?;

    // Raw JSON also runs on the pre-fix server, which ignores modelSettings.
    send_request(
        &mut client,
        "thread/clear",
        5,
        Some(json!({
            "threadId": predecessor,
            "modelSettings": model_settings
        })),
    )
    .await?;
    let (cleared, _) = read_clear_outcome(&mut client, 5).await?;
    assert_eq!(
        (
            &cleared.successor_thread.model,
            &cleared.successor_thread.reasoning_effort
        ),
        (&fresh.thread.model, &fresh.thread.reasoning_effort)
    );
    let read = read_thread(&mut client, 6, &cleared.successor_thread.id).await?;
    assert_eq!(
        (read.thread.model, read.thread.reasoning_effort),
        (
            cleared.successor_thread.model.clone(),
            cleared.successor_thread.reasoning_effort.clone()
        )
    );
    send_request(
        &mut client,
        "thread/resume",
        7,
        Some(json!({"threadId": cleared.successor_thread.id})),
    )
    .await?;
    let effective: ThreadResumeResponse = to_response(read_response_for_id(&mut client, 7).await?)?;
    assert_eq!(
        (
            &effective.model_provider,
            &effective.service_tier,
            &effective.approval_policy,
            &effective.approvals_reviewer,
            &effective.sandbox
        ),
        (
            &launch.model_provider,
            &launch.service_tier,
            &launch.approval_policy,
            &launch.approvals_reviewer,
            &launch.sandbox
        )
    );
    let collaboration = effective.collaboration_mode.context("effective mode")?;
    assert_eq!(collaboration.mode, ModeKind::Default);
    assert_eq!(collaboration.settings.developer_instructions, None);
    assert_eq!(
        (effective.model, effective.reasoning_effort),
        (fresh.model, fresh.reasoning_effort)
    );
    process.kill().await?;
    Ok(())
}
