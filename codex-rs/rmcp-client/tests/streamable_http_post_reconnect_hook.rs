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

/// R04 remediation proof: a bounded timeout passed into the post-reconnect
/// hook's send must actually fail the recovery closed when the provider
/// never responds, rather than holding the recovery — and therefore every
/// operation waiting on it — open indefinitely.
///
/// `test_streamable_http_server` (the shared real subprocess used by the
/// tests above) has no control for making it stop responding to a specific
/// request, and adding one would be a change to shared test infrastructure
/// far outside this remediation's bounded scope. This test instead builds a
/// minimal, self-contained real streamable-HTTP session lifecycle with
/// `wiremock` — genuine HTTP requests/responses over a real listener, not a
/// simulated call to internal recovery functions — including the
/// `Mcp-Session-Id` response header the SDK's streamable-HTTP adapter
/// requires before it will recognize a `404` as session-expiry
/// (`StreamableHttpClientAdapterError::SessionExpired404` triggers only
/// when a session id was already established, confirmed by reading
/// `http_client_adapter.rs` before writing this mock rather than assuming
/// a bare 404 suffices).
mod bounded_recovery_bind_timeout {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use codex_config::types::AuthKeyringBackendKind;
    use codex_config::types::OAuthCredentialsStoreMode;
    use codex_rmcp_client::ElicitationAction;
    use codex_rmcp_client::ElicitationResponse;
    use codex_rmcp_client::McpProtocolMode;
    use codex_rmcp_client::PostReconnectHook;
    use codex_rmcp_client::RmcpClient;
    use futures::FutureExt as _;
    use rmcp::model::ClientCapabilities;
    use rmcp::model::Implementation;
    use rmcp::model::InitializeRequestParams;
    use rmcp::model::ProtocolVersion;
    use wiremock::Mock;
    use wiremock::MockServer;
    use wiremock::Request;
    use wiremock::ResponseTemplate;
    use wiremock::matchers::method;
    use wiremock::matchers::path;

    fn initialize_params() -> InitializeRequestParams {
        InitializeRequestParams::new(
            ClientCapabilities::default(),
            Implementation::new("bounded-recovery-timeout-test", "0.0.0"),
        )
        .with_protocol_version(ProtocolVersion::V_2025_06_18)
    }

    const SESSION_ID: &str = "test-session-1";
    const HANG_METHOD: &str = "test/non-responding-bind";
    const BOUNDED_TIMEOUT: Duration = Duration::from_millis(300);
    /// Long enough (relative to `BOUNDED_TIMEOUT` and the test's own outer
    /// timeout below) that a test relying on this ever completing would
    /// fail, proving the client — not the mock — is what bounds the wait.
    const NEVER_RESPONDS: Duration = Duration::from_secs(30);

    fn initialize_response(request: &serde_json::Value) -> ResponseTemplate {
        ResponseTemplate::new(200)
            .append_header("Mcp-Session-Id", SESSION_ID)
            .set_body_json(serde_json::json!({
                "jsonrpc": "2.0",
                "id": request["id"],
                "result": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "bounded-recovery-timeout-test", "version": "1.0.0"},
                },
            }))
    }

    fn hang_hook(calls: std::sync::Arc<AtomicUsize>) -> PostReconnectHook {
        Box::new(move |context, _peer_info| {
            let calls = std::sync::Arc::clone(&calls);
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                context
                    .send_custom_request(HANG_METHOD, None, Some(BOUNDED_TIMEOUT))
                    .await
                    .map(|_| ())
            }
            .boxed()
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn non_responding_recovery_bind_fails_closed_within_the_bounded_timeout()
    -> anyhow::Result<()> {
        let armed = std::sync::Arc::new(AtomicBool::new(false));
        let armed_for_mock = std::sync::Arc::clone(&armed);
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp"))
            .respond_with(move |request: &Request| {
                let body: serde_json::Value = request.body_json().expect("valid JSON-RPC request");
                match body["method"].as_str() {
                    Some("initialize") => initialize_response(&body),
                    Some("notifications/initialized") => ResponseTemplate::new(202),
                    Some("tools/list") => {
                        if armed_for_mock.swap(false, Ordering::SeqCst) {
                            // Simulate Streamable HTTP session expiry: a 404
                            // is only interpreted as session-expiry once a
                            // session id has been established, which the
                            // `initialize` response above already sent.
                            ResponseTemplate::new(404)
                        } else {
                            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                                "jsonrpc": "2.0",
                                "id": body["id"],
                                "result": {"tools": []},
                            }))
                        }
                    }
                    Some(HANG_METHOD) => ResponseTemplate::new(200).set_delay(NEVER_RESPONDS),
                    other => panic!("unexpected bounded-timeout test method: {other:?}"),
                }
            })
            .mount(&server)
            .await;

        let client = RmcpClient::new_streamable_http_client_with_protocol_mode(
            "bounded-recovery-timeout-test",
            &format!("{}/mcp", server.uri()),
            /*bearer_token*/ None,
            /*http_headers*/ None,
            /*env_http_headers*/ None,
            OAuthCredentialsStoreMode::File,
            AuthKeyringBackendKind::default(),
            codex_exec_server::Environment::default_for_tests().get_http_client(),
            /*auth_provider*/ None,
            McpProtocolMode::Legacy,
        )
        .await?;
        let hook_calls = std::sync::Arc::new(AtomicUsize::new(0));
        let client = client.with_post_reconnect_hook(hang_hook(std::sync::Arc::clone(&hook_calls)));

        client
            .initialize(
                initialize_params(),
                Some(Duration::from_secs(5)),
                Box::new(|_, _| {
                    async {
                        Ok(ElicitationResponse {
                            action: ElicitationAction::Accept,
                            content: Some(serde_json::json!({})),
                            meta: None,
                        })
                    }
                    .boxed()
                }),
            )
            .await
            .expect("initialize should succeed");

        client
            .list_tools(/*params*/ None, Some(Duration::from_secs(5)))
            .await
            .expect("warmup tools/list should succeed before session expiry is armed");

        armed.store(true, Ordering::SeqCst);

        let started = std::time::Instant::now();
        let result = client
            .list_tools(/*params*/ None, Some(Duration::from_secs(5)))
            .await;
        let elapsed = started.elapsed();

        assert!(
            result.is_err(),
            "the operation that triggered recovery must fail once the recovery bind times out"
        );
        assert_eq!(
            hook_calls.load(Ordering::SeqCst),
            1,
            "the hook must have actually run during recovery"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "the bounded recovery-bind timeout ({BOUNDED_TIMEOUT:?}) must fail the operation \
             closed well before its own outer timeout; took {elapsed:?}"
        );
        assert!(
            client.is_closed().await,
            "a recovery bind that never gets an acknowledgement must close the connection, \
             not leave it retryable, exactly like an explicitly rejected acknowledgement"
        );

        Ok(())
    }
}

/// R05 remediation proof: the existing recovery tests show the hook runs
/// and gates success/failure, but do not establish *ordering* — that a
/// successful bind's request/response cycle is actually complete on the
/// wire before the interrupted operation is retried, not merely that both
/// eventually happen. This module captures the mock server's own arrival
/// order (not a client-side inference) and asserts the bind request
/// appears before the retried `tools/list` request in that real,
/// server-observed sequence.
mod recovery_success_bind_before_retry_ordering {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use codex_config::types::AuthKeyringBackendKind;
    use codex_config::types::OAuthCredentialsStoreMode;
    use codex_rmcp_client::ElicitationAction;
    use codex_rmcp_client::ElicitationResponse;
    use codex_rmcp_client::McpProtocolMode;
    use codex_rmcp_client::PostReconnectHook;
    use codex_rmcp_client::RmcpClient;
    use futures::FutureExt as _;
    use rmcp::model::ClientCapabilities;
    use rmcp::model::Implementation;
    use rmcp::model::InitializeRequestParams;
    use rmcp::model::ProtocolVersion;
    use wiremock::Mock;
    use wiremock::MockServer;
    use wiremock::Request;
    use wiremock::ResponseTemplate;
    use wiremock::matchers::method;
    use wiremock::matchers::path;

    const SESSION_ID: &str = "test-session-2";
    const BIND_METHOD: &str = "test/ordering-bind";

    fn initialize_params() -> InitializeRequestParams {
        InitializeRequestParams::new(
            ClientCapabilities::default(),
            Implementation::new("recovery-ordering-test", "0.0.0"),
        )
        .with_protocol_version(ProtocolVersion::V_2025_06_18)
    }

    fn initialize_response(request: &serde_json::Value) -> ResponseTemplate {
        ResponseTemplate::new(200)
            .append_header("Mcp-Session-Id", SESSION_ID)
            .set_body_json(serde_json::json!({
                "jsonrpc": "2.0",
                "id": request["id"],
                "result": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "recovery-ordering-test", "version": "1.0.0"},
                },
            }))
    }

    fn accepting_hook() -> PostReconnectHook {
        Box::new(move |context, _peer_info| {
            async move {
                context
                    .send_custom_request(
                        BIND_METHOD,
                        Some(serde_json::json!({"version": 1})),
                        Some(Duration::from_secs(5)),
                    )
                    .await
                    .map(|_| ())
            }
            .boxed()
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn successful_recovery_bind_completes_before_the_retried_operation_is_sent()
    -> anyhow::Result<()> {
        let armed = std::sync::Arc::new(AtomicBool::new(false));
        let armed_for_mock = std::sync::Arc::clone(&armed);
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp"))
            .respond_with(move |request: &Request| {
                let body: serde_json::Value = request.body_json().expect("valid JSON-RPC request");
                match body["method"].as_str() {
                    Some("initialize") => initialize_response(&body),
                    Some("notifications/initialized") => ResponseTemplate::new(202),
                    Some("tools/list") => {
                        if armed_for_mock.swap(false, Ordering::SeqCst) {
                            ResponseTemplate::new(404)
                        } else {
                            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                                "jsonrpc": "2.0",
                                "id": body["id"],
                                "result": {"tools": []},
                            }))
                        }
                    }
                    Some(BIND_METHOD) => {
                        ResponseTemplate::new(200).set_body_json(serde_json::json!({
                            "jsonrpc": "2.0",
                            "id": body["id"],
                            "result": {"version": 1, "accepted": true},
                        }))
                    }
                    other => panic!("unexpected ordering test method: {other:?}"),
                }
            })
            .mount(&server)
            .await;

        let client = RmcpClient::new_streamable_http_client_with_protocol_mode(
            "recovery-ordering-test",
            &format!("{}/mcp", server.uri()),
            /*bearer_token*/ None,
            /*http_headers*/ None,
            /*env_http_headers*/ None,
            OAuthCredentialsStoreMode::File,
            AuthKeyringBackendKind::default(),
            codex_exec_server::Environment::default_for_tests().get_http_client(),
            /*auth_provider*/ None,
            McpProtocolMode::Legacy,
        )
        .await?;
        let client = client.with_post_reconnect_hook(accepting_hook());

        client
            .initialize(
                initialize_params(),
                Some(Duration::from_secs(5)),
                Box::new(|_, _| {
                    async {
                        Ok(ElicitationResponse {
                            action: ElicitationAction::Accept,
                            content: Some(serde_json::json!({})),
                            meta: None,
                        })
                    }
                    .boxed()
                }),
            )
            .await
            .expect("initialize should succeed");
        client
            .list_tools(/*params*/ None, Some(Duration::from_secs(5)))
            .await
            .expect("warmup tools/list should succeed before session expiry is armed");

        armed.store(true, Ordering::SeqCst);

        client
            .list_tools(/*params*/ None, Some(Duration::from_secs(5)))
            .await
            .expect("the interrupted tools/list must succeed once recovery binds successfully");

        let received = server
            .received_requests()
            .await
            .expect("mock server should record requests");
        let methods: Vec<String> = received
            .iter()
            .filter_map(|request| {
                request
                    .body_json::<serde_json::Value>()
                    .ok()
                    .and_then(|body| {
                        body.get("method")
                            .and_then(|m| m.as_str())
                            .map(str::to_string)
                    })
            })
            .collect();

        let bind_index = methods
            .iter()
            .position(|method| method == BIND_METHOD)
            .expect("bind request must have been sent");
        let last_tools_list_index = methods
            .iter()
            .rposition(|method| method == "tools/list")
            .expect("a tools/list request must have been sent");
        assert!(
            bind_index < last_tools_list_index,
            "the bind request must reach the wire, in the mock server's own observed arrival \
             order, strictly before the retried tools/list request; observed order: {methods:?}"
        );

        Ok(())
    }
}
