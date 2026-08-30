//! Real-artifact proof for distribution Issue 17 Slice 3 (R003-R005):
//! `RmcpClient`'s post-reconnect hook actually runs during a genuine
//! Streamable HTTP session-expiry recovery (not a simulated call to
//! `reinitialize_after_session_expiry`), gates the recovered connection's
//! usability, and — on rejection — leaves the connection permanently
//! unusable rather than retryable, matching `kcf-runtime/04`'s "KCF may
//! retry only through a fresh or explicitly reset unbound connection."
//!
//! This test intentionally does not depend on `codex-mcp`'s
//! `codex/thread-identity` semantics (that dependency direction does not
//! exist — `rmcp-client` cannot know about it) and instead proves the
//! generic hook mechanism itself against a real spawned streamable-HTTP
//! test server, reusing the same harness as the existing session-expiry
//! coverage in `streamable_http_recovery.rs`.

mod streamable_http_test_support;

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use codex_rmcp_client::PostReconnectHook;
use futures::FutureExt as _;
use pretty_assertions::assert_eq;

use streamable_http_test_support::arm_session_post_failure;
use streamable_http_test_support::call_echo_tool;
use streamable_http_test_support::create_client;
use streamable_http_test_support::expected_echo_result;
use streamable_http_test_support::spawn_streamable_http_server;

fn tracking_hook(calls: Arc<AtomicUsize>, accept: bool) -> PostReconnectHook {
    Box::new(move |_context, _peer_info| {
        let calls = Arc::clone(&calls);
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            if accept {
                Ok(())
            } else {
                Err(anyhow::anyhow!("synthetic post-reconnect hook rejection"))
            }
        }
        .boxed()
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn post_reconnect_hook_runs_exactly_once_during_session_expiry_recovery() -> anyhow::Result<()>
{
    let (_server, base_url) = spawn_streamable_http_server().await?;
    let client = create_client(&base_url).await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let client = client.with_post_reconnect_hook(tracking_hook(Arc::clone(&calls), true));

    let warmup = call_echo_tool(&client, "warmup").await?;
    assert_eq!(warmup, expected_echo_result("warmup"));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "the hook must not run for ordinary operations on a connection that never recovers"
    );

    arm_session_post_failure(
        &base_url,
        /*status*/ 404,
        /*remaining*/ 1,
        /*www_authenticate_headers*/ &[],
    )
    .await?;

    let recovered = call_echo_tool(&client, "recovered").await?;
    assert_eq!(recovered, expected_echo_result("recovered"));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the hook must run exactly once, for the one real session-expiry recovery"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn post_reconnect_hook_rejection_blocks_recovery_and_closes_the_connection()
-> anyhow::Result<()> {
    let (_server, base_url) = spawn_streamable_http_server().await?;
    let client = create_client(&base_url).await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let client = client.with_post_reconnect_hook(tracking_hook(Arc::clone(&calls), false));

    let warmup = call_echo_tool(&client, "warmup").await?;
    assert_eq!(warmup, expected_echo_result("warmup"));

    arm_session_post_failure(
        &base_url,
        /*status*/ 404,
        /*remaining*/ 1,
        /*www_authenticate_headers*/ &[],
    )
    .await?;

    let error = call_echo_tool(&client, "should-fail").await.unwrap_err();
    assert!(
        format!("{error:#}").contains("post-reconnect binding gate"),
        "expected the hook rejection to surface in the operation's error, got: {error:#}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    // Per kcf-runtime/04: "the physical connection is unusable for all
    // provider operations... KCF may retry only through a fresh or
    // explicitly reset unbound connection." Proven two ways: the client
    // reports itself closed, and a second call fails immediately without
    // re-running the hook (no retry loop that could eventually slip an
    // operation through on this same connection).
    assert!(
        client.is_closed().await,
        "a rejected post-reconnect hook must close the connection, not leave it retryable"
    );

    let second_error = call_echo_tool(&client, "still-should-fail")
        .await
        .unwrap_err();
    assert!(
        format!("{second_error:#}")
            .to_lowercase()
            .contains("shut down"),
        "a subsequent call on the closed connection should fail immediately, got: {second_error:#}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the hook must not run again on a connection that is already closed"
    );

    Ok(())
}
