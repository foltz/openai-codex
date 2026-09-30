use super::*;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn recovery_context_is_replayable_distinct_and_preserves_live_predecessor() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let home = TempDir::new()?;
    create_config_toml(home.path(), &server.uri(), "never")?;
    let (mut process, addr) = spawn_websocket_server_with_env(
        home.path(),
        &[("CODEX_SQLITE_HOME", home.path().to_str().context("home")?)],
    )
    .await?;
    let mut client = connect_websocket(addr).await?;
    send_request(
        &mut client,
        "initialize",
        1,
        Some(json!({
            "clientInfo": {"name": "recovery-test", "version": "1"},
            "capabilities": {"experimentalApi": true}
        })),
    )
    .await?;
    read_response_for_id(&mut client, 1).await?;
    send_request(
        &mut client,
        "thread/clear/recovery/read",
        2,
        Some(json!({})),
    )
    .await?;
    assert_eq!(
        read_response_for_id(&mut client, 2).await?.result,
        json!({
            "contractVersion": 1, "durability": "durable", "observation": {"type": "support"}
        })
    );
    let state = StateRuntime::init(
        codex_state::SqliteConfig::new_for_testing(home.path().abs()),
        "test-provider".into(),
    )
    .await?;
    let pending = ThreadId::new();
    state.reserve_clear_recovery(pending, None).await?;
    for (id, phase) in [(4, "pending"), (5, "failed")] {
        if phase == "failed" {
            state
                .finish_clear_recovery(pending, codex_state::ClearRecoveryPhase::Failed)
                .await?;
        }
        send_request(
            &mut client,
            "thread/clear/recovery/read",
            id,
            Some(json!({"successorThreadId": pending.to_string()})),
        )
        .await?;
        assert_eq!(
            read_response_for_id(&mut client, id).await?.result,
            json!({
                "contractVersion": 1, "durability": "durable", "observation": {
                    "type": phase, "recovery": {
                        "successorThreadId": pending.to_string(),
                        "context": {"contractVersion": 1, "predecessorThreadId": null},
                        "durability": "durable"
                    }
                }
            })
        );
    }
    let a = start_thread(&mut client, 3).await?;
    for (offset, ephemeral, predecessor) in [(10, false, Some(a.clone())), (20, true, None)] {
        let context = json!({"contractVersion": 1, "predecessorThreadId": predecessor});
        send_request(
            &mut client,
            "thread/start",
            offset,
            Some(json!({
                "model": "mock-model", "sessionStartSource": "clear",
                "clearRecovery": context, "ephemeral": ephemeral
            })),
        )
        .await?;
        let started = read_response_for_id(&mut client, offset).await?.result;
        let b = started["thread"]["id"].as_str().context("successor")?;
        let recovery = json!({
            "successorThreadId": b, "context": context, "durability": "durable"
        });
        assert_eq!(started["clearRecovery"], recovery);
        send_request(
            &mut client,
            "thread/clear/recovery/read",
            offset + 1,
            Some(json!({"successorThreadId": b})),
        )
        .await?;
        assert_eq!(
            read_response_for_id(&mut client, offset + 1).await?.result,
            json!({
                "contractVersion": 1, "durability": "durable",
                "observation": {"type": "complete", "recovery": recovery}
            })
        );
        send_request(
            &mut client,
            "thread/clear/read",
            offset + 2,
            Some(json!({"successorThreadId": b})),
        )
        .await?;
        assert_eq!(
            read_response_for_id(&mut client, offset + 2).await?.result,
            json!({"disposition": "none", "transition": null})
        );
    }
    // Independent recovery neither retires A nor requires it to be unavailable.
    send_request(&mut client, "thread/loaded/list", 30, Some(json!({}))).await?;
    let loaded = read_response_for_id(&mut client, 30).await?.result;
    assert!(
        loaded["data"]
            .as_array()
            .context("loaded")?
            .contains(&json!(a))
    );
    for (id, context, source, legacy) in [
        (
            40,
            json!({"contractVersion": 2, "predecessorThreadId": a}),
            "clear",
            None,
        ),
        (
            41,
            json!({"contractVersion": 1, "predecessorThreadId": "bad"}),
            "clear",
            None,
        ),
        (
            42,
            json!({"contractVersion": 1, "predecessorThreadId": a}),
            "startup",
            None,
        ),
        (
            43,
            json!({"contractVersion": 1, "predecessorThreadId": a}),
            "clear",
            Some(a.clone()),
        ),
    ] {
        send_request(
            &mut client,
            "thread/start",
            id,
            Some(json!({
                "sessionStartSource": source, "clearRecovery": context,
                "clearPredecessorThreadId": legacy
            })),
        )
        .await?;
        read_error_for_id(&mut client, id).await?;
    }
    process.kill().await?;
    Ok(())
}
