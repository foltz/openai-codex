//! DB-backed `thread/clear` RPC witnesses for the predecessor idle-unload
//! fence. A's real clear SessionEnd hook is a test-controlled gate: it records
//! entry, then blocks until the test creates the gate file. The test opens it
//! only after observing, on the server's own clock, that A's 1s idle deadline
//! has long elapsed: two sequential unretained control threads, started after
//! the gate was entered, each idle-unload through the same production path.
//!
//! Non-vacuity: core does not refuse idle retirement while a clear hook runs.
//! `CodexThread::try_begin_idle_retirement` (core/src/codex_thread/
//! retirement.rs:184-211) refuses only for an active turn, `AgentStatus::
//! Running`, or outstanding `tx_sub` submissions. The clear SessionEnd hook
//! (core/src/hook_runtime.rs:579-624) runs inline in the app-server clear task
//! and touches none of these, and `HookStarted` (app-server/src/
//! bespoke_event_handling.rs:1144) does not change thread status. Without the
//! app-server reservation fence, A retires during the gate, so
//! `assert_held_predecessor_not_retired` fails: `thread/closed(A)` is seen
//! while held, or A is missing from `thread/loaded/list`. The original flake
//! log shows exactly that retirement mid-clear.

use super::*;
use pretty_assertions::assert_eq;

struct ClearGate {
    log: std::path::PathBuf,
    entered: std::path::PathBuf,
    gate: std::path::PathBuf,
}

/// SessionEnd hooks that log every payload. The `clear` hook then writes
/// `entered` and blocks until `gate` exists (bounded inside the hook, well
/// under its timeout, so a broken test cannot wedge the server).
fn write_gated_clear_hooks(codex_home: &Path) -> Result<ClearGate> {
    let gate = ClearGate {
        log: codex_home.join("clear-fence.jsonl"),
        entered: codex_home.join("clear-fence.entered"),
        gate: codex_home.join("clear-fence.gate"),
    };
    let script_path = codex_home.join("clear-fence-hook.py");
    std::fs::write(
        &script_path,
        format!(
            r#"import json
import sys
import time
from pathlib import Path

payload = json.load(sys.stdin)
with Path(r"{log}").open("a", encoding="utf-8") as handle:
    handle.write(json.dumps(payload) + "\n")
if payload.get("reason") == "clear":
    Path(r"{entered}").write_text("entered", encoding="utf-8")
    deadline = time.monotonic() + 45
    while not Path(r"{gate}").exists() and time.monotonic() < deadline:
        time.sleep(0.02)
"#,
            log = gate.log.display(),
            entered = gate.entered.display(),
            gate = gate.gate.display(),
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
                        "hooks": [{ "type": "command", "command": command, "timeout": 60 }]
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
    Ok(gate)
}

fn bypass_hook_trust() -> Option<HashMap<String, serde_json::Value>> {
    Some(HashMap::from([(
        "bypass_hook_trust".to_string(),
        json!(true),
    )]))
}

fn closed_thread_id(message: &JSONRPCMessage) -> Option<&str> {
    match message {
        JSONRPCMessage::Notification(notification) if notification.method == "thread/closed" => {
            notification
                .params
                .as_ref()
                .and_then(|params| params.get("threadId"))
                .and_then(serde_json::Value::as_str)
        }
        _ => None,
    }
}

/// Waits for `thread/closed(target)`, recording every closure seen meanwhile.
async fn wait_closed_recording(
    stream: &mut WsClient,
    target: &str,
    seen: &mut Vec<String>,
) -> Result<()> {
    timeout(Duration::from_secs(10), async {
        loop {
            let message = read_jsonrpc_message(stream).await?;
            if let Some(thread_id) = closed_thread_id(&message) {
                seen.push(thread_id.to_string());
                if thread_id == target {
                    return Ok::<(), anyhow::Error>(());
                }
            }
        }
    })
    .await
    .context("timed out waiting for control thread idle unload")?
}

async fn loaded_thread_ids(stream: &mut WsClient, id: i64) -> Result<Vec<String>> {
    send_request(
        stream,
        "thread/loaded/list",
        id,
        Some(serde_json::to_value(
            codex_app_server_protocol::ThreadLoadedListParams::default(),
        )?),
    )
    .await?;
    Ok(
        to_response::<codex_app_server_protocol::ThreadLoadedListResponse>(
            read_response_for_id(stream, id).await?,
        )?
        .data,
    )
}

/// With the gate entered (the admitted clear holds A's reservation), observe
/// A's idle deadline elapse on the server clock, then prove A was not retired.
/// Two sequential unretained control threads are started on `observer` after
/// entry; each closes only after its own full 1s idle delay, so A's earlier
/// deadline has passed by more than one full delay plus A's own retirement
/// time before the assertions run. This is the assertion that fails without
/// fix-production.patch.
async fn assert_held_predecessor_not_retired(
    observer: &mut WsClient,
    gate: &ClearGate,
    predecessor: &str,
    next_id: i64,
) -> Result<()> {
    core_test_support::fs_wait::wait_for_path_exists(&gate.entered, Duration::from_secs(10))
        .await?;
    let mut closed_while_held = Vec::new();
    for offset in 0..2 {
        let control = start_thread_with_config(observer, next_id + offset, None).await?;
        wait_closed_recording(observer, &control, &mut closed_while_held).await?;
    }
    assert!(
        !closed_while_held.iter().any(|id| id == predecessor),
        "predecessor retired while its clear held the reservation: {closed_while_held:?}"
    );
    assert!(
        loaded_thread_ids(observer, next_id + 2)
            .await?
            .iter()
            .any(|id| id == predecessor),
        "predecessor unloaded while its clear held the reservation"
    );
    assert!(
        !gate.gate.exists(),
        "gate opened before the fence was observed"
    );
    Ok(())
}

/// A's causal hook order: its clear SessionEnd strictly before its idle stop.
fn assert_predecessor_hook_order(payloads: &[serde_json::Value], predecessor: &str) {
    assert_eq!(
        payloads
            .iter()
            .filter(|payload| payload["session_id"].as_str() == Some(predecessor))
            .map(|payload| payload["reason"].as_str().unwrap_or_default())
            .collect::<Vec<_>>(),
        vec!["clear", "other"]
    );
}

/// Durable phase of A's clear transition, from the real state DB.
async fn predecessor_transition_phase(
    codex_home: &Path,
    predecessor: &str,
) -> Result<ClearTransitionPhase> {
    let state = StateRuntime::init(
        codex_state::SqliteConfig::new_for_testing(codex_home.abs()),
        "test-provider".to_string(),
    )
    .await?;
    Ok(state
        .get_clear_transition_by_predecessor(
            ThreadId::from_string(predecessor).map_err(|err| anyhow::anyhow!("{err}"))?,
        )
        .await?
        .context("clear transition record")?
        .phase)
}

/// (a) + (c): A's idle deadline elapses while the admitted clear is gated.
/// A is not retired mid-clear; once the test opens the gate the clear
/// completes durably and A unloads only after the clear released it.
#[tokio::test]
async fn idle_deadline_elapsing_mid_clear_does_not_retire_predecessor() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;
    let gate = write_gated_clear_hooks(codex_home.path())?;
    let (mut process, bind_addr) = spawn_websocket_server_with_args(
        codex_home.path(),
        "ws://127.0.0.1:0",
        &["-c".to_string(), "thread_unload_delay_secs=1".to_string()],
    )
    .await?;
    let mut observer = connect_websocket(bind_addr).await?;
    initialize(&mut observer, 1, "clear-fence-observer").await?;
    let mut requester = connect_websocket(bind_addr).await?;
    initialize(&mut requester, 1, "clear-fence-requester").await?;

    let predecessor = start_thread_with_config(&mut requester, 2, bypass_hook_trust()).await?;
    send_clear(&mut requester, 3, &predecessor).await?;
    assert_held_predecessor_not_retired(&mut observer, &gate, &predecessor, 10).await?;
    std::fs::write(&gate.gate, "open")?;

    let target = RequestId::Integer(3);
    let response: ThreadClearResponse = timeout(Duration::from_secs(10), async {
        loop {
            match read_jsonrpc_message(&mut requester).await? {
                JSONRPCMessage::Response(response) if response.id == target => {
                    return to_response(response);
                }
                JSONRPCMessage::Error(error) if error.id == target => {
                    bail!("thread/clear failed: {error:?}");
                }
                _ => {}
            }
        }
    })
    .await
    .context("timed out waiting for the released clear")??;
    assert_eq!(response.predecessor_thread_id, predecessor);
    assert_completed_transition(codex_home.path(), &response.transition_id).await?;

    wait_for_thread_closed(&mut observer, &predecessor).await?;
    let payloads = wait_for_hook_payloads(&gate.log, 2).await?;
    assert_predecessor_hook_order(&payloads, &predecessor);

    process.kill().await.context("failed to stop app-server")?;
    Ok(())
}

/// (b) The requester disconnects while its admitted clear is gated. The RPC
/// gate lets the started handler finish (`ConnectionRpcGate::run`), so the
/// clear is not cancelled: A stays loaded and fenced while the work is
/// unproven, and unloads only after the clear's join completes durably and
/// releases the reservation. A dropped clear future is covered by
/// `shutdown_tests::cancelled_clear_retains_reservation_unproven_*`.
#[tokio::test]
async fn requester_disconnect_mid_clear_keeps_predecessor_until_proven_completion() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;
    let gate = write_gated_clear_hooks(codex_home.path())?;
    let (mut process, bind_addr) = spawn_websocket_server_with_args(
        codex_home.path(),
        "ws://127.0.0.1:0",
        &["-c".to_string(), "thread_unload_delay_secs=1".to_string()],
    )
    .await?;
    let mut observer = connect_websocket(bind_addr).await?;
    initialize(&mut observer, 1, "clear-fence-observer").await?;
    let mut requester = connect_websocket(bind_addr).await?;
    initialize(&mut requester, 1, "clear-fence-requester").await?;

    let predecessor = start_thread_with_config(&mut requester, 2, bypass_hook_trust()).await?;
    send_clear(&mut requester, 3, &predecessor).await?;
    core_test_support::fs_wait::wait_for_path_exists(&gate.entered, Duration::from_secs(10))
        .await?;
    drop(requester);

    assert_held_predecessor_not_retired(&mut observer, &gate, &predecessor, 10).await?;
    assert_ne!(
        predecessor_transition_phase(codex_home.path(), &predecessor).await?,
        ClearTransitionPhase::Completed,
        "the gated clear is still unproven"
    );
    std::fs::write(&gate.gate, "open")?;

    wait_for_thread_closed(&mut observer, &predecessor).await?;
    assert_eq!(
        predecessor_transition_phase(codex_home.path(), &predecessor).await?,
        ClearTransitionPhase::Completed
    );
    let payloads = wait_for_hook_payloads(&gate.log, 2).await?;
    assert_predecessor_hook_order(&payloads, &predecessor);

    process.kill().await.context("failed to stop app-server")?;
    Ok(())
}
