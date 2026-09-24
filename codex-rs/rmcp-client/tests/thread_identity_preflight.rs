//! Source-admission preflight for distribution Issue 17 (KCF-IND-D001).
//!
//! This test exercises only the real locked-artifact deserialization
//! behavior for a non-object `experimental` capability entry. It adds no
//! `codex/thread-identity` feature behavior.

use std::time::Duration;

use codex_config::types::AuthKeyringBackendKind;
use codex_config::types::OAuthCredentialsStoreMode;
use codex_exec_server::Environment;
use codex_rmcp_client::ElicitationAction;
use codex_rmcp_client::ElicitationResponse;
use codex_rmcp_client::McpProtocolMode;
use codex_rmcp_client::RmcpClient;
use futures::FutureExt;
use rmcp::model::ClientCapabilities;
use rmcp::model::Implementation;
use rmcp::model::InitializeRequestParams;
use rmcp::model::ProtocolVersion;
use serde_json::Value;
use serde_json::json;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::Request;
use wiremock::ResponseTemplate;
use wiremock::matchers::method;
use wiremock::matchers::path;

const LEGACY_VERSION: &str = "2025-06-18";

fn initialize_params() -> InitializeRequestParams {
    InitializeRequestParams::new(
        ClientCapabilities::default(),
        Implementation::new("codex-thread-identity-preflight", "0.0.0"),
    )
    .with_protocol_version(ProtocolVersion::V_2025_06_18)
}

/// A legacy initialize response whose `capabilities.experimental` entry for
/// a plausible capability key is a JSON string, not an object — the exact
/// shape `kcf-runtime/04`'s "Required proof before source admission" item 2
/// requires proving against the real locked artifact.
fn non_object_experimental_entry_response(request: &Value) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({
        "jsonrpc": "2.0",
        "id": request["id"],
        "result": {
            "protocolVersion": LEGACY_VERSION,
            "capabilities": {
                "tools": {},
                "experimental": {
                    "codex/thread-identity": "not-an-object"
                }
            },
            "serverInfo": {"name": "preflight-non-object-experimental", "version": "1.0.0"},
        },
    }))
}

#[tokio::test]
async fn non_object_experimental_entry_observed_behavior() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .respond_with(|request: &Request| {
            let body: Value = request.body_json().expect("valid JSON-RPC request");
            match body["method"].as_str() {
                Some("initialize") => non_object_experimental_entry_response(&body),
                Some("notifications/initialized") => ResponseTemplate::new(202),
                other => panic!("unexpected preflight method: {other:?}"),
            }
        })
        .mount(&server)
        .await;

    let client = RmcpClient::new_streamable_http_client_with_protocol_mode(
        "thread-identity-preflight",
        &format!("{}/mcp", server.uri()),
        /*bearer_token*/ None,
        /*http_headers*/ None,
        /*env_http_headers*/ None,
        OAuthCredentialsStoreMode::File,
        AuthKeyringBackendKind::default(),
        Environment::default_for_tests().get_http_client(),
        /*auth_provider*/ None,
        McpProtocolMode::Legacy,
    )
    .await?;

    let result = client
        .initialize(
            initialize_params(),
            Some(Duration::from_secs(5)),
            Box::new(|_, _| {
                async {
                    Ok(ElicitationResponse {
                        action: ElicitationAction::Accept,
                        content: Some(json!({})),
                        meta: None,
                    })
                }
                .boxed()
            }),
        )
        .await;

    // Preflight observation, recorded before this assertion was written:
    // `initialize()` fails with "expect initialized result, but received:
    // ... CustomResult(...)" because `rmcp::model::ServerCapabilities`'s
    // `experimental: Option<BTreeMap<String, JsonObject>>` cannot
    // deserialize a non-object value for a specific key, so the whole
    // typed result falls through to an untyped custom-result variant that
    // the handshake rejects. This matches `kcf-runtime/04`'s expectation
    // ("expected to fail handshake deserialization") against the real
    // locked artifact, not a type-level inference.
    let error = result.expect_err(
        "a non-object experimental[\"codex/thread-identity\"] value is expected to fail \
         handshake deserialization against the real locked artifact per kcf-runtime/04",
    );
    let message = format!("{error:#}");
    assert!(
        message.contains("expect initialized result"),
        "unexpected failure mode for non-object experimental entry: {message}"
    );
    println!("PREFLIGHT OBSERVED: initialize() failed as expected: {message}");

    client.shutdown().await;
    Ok(())
}
