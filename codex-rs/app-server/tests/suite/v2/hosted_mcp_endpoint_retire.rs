//! A real websocket app-server daemon with client-hosted MCP endpoints:
//! declaration by challenge, exact retirement with receipts, and refusals.
//! This drives the wire consumer, not the actual TUI final-exit implementation.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use app_test_support::create_mock_responses_server_repeating_assistant;
use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::Method;
use axum::http::Request;
use axum::http::StatusCode;
use axum::http::header::AUTHORIZATION;
use axum::middleware;
use axum::middleware::Next;
use axum::response::Response;
use codex_app_server_protocol::ClientInfo;
use codex_app_server_protocol::InitializeCapabilities;
use codex_app_server_protocol::InitializeParams;
use codex_app_server_protocol::JSONRPCError;
use codex_app_server_protocol::JSONRPCMessage;
use codex_app_server_protocol::McpHostedEndpointDeclareParams;
use codex_app_server_protocol::McpHostedEndpointDeclareResponse;
use codex_app_server_protocol::McpHostedEndpointRefusalReason;
use codex_app_server_protocol::McpHostedEndpointRetireParams;
use codex_app_server_protocol::McpHostedEndpointRetireResponse;
use codex_app_server_protocol::McpHostedEndpointRetirementOutcome;
use codex_app_server_protocol::McpServerToolCallParams;
use codex_app_server_protocol::McpServerToolCallResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ThreadReadParams;
use codex_app_server_protocol::ThreadReadResponse;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::UserInput;
use pretty_assertions::assert_eq;
use rmcp::handler::server::ServerHandler;
use rmcp::model::CallToolRequestParams;
use rmcp::model::CallToolResult;
use rmcp::model::ContentBlock;
use rmcp::model::JsonObject;
use rmcp::model::ListToolsResult;
use rmcp::model::ServerCapabilities;
use rmcp::model::ServerConfig;
use rmcp::model::Tool;
use rmcp::model::ToolAnnotations;
use rmcp::service::RequestContext;
use rmcp::service::RoleServer;
use rmcp::transport::StreamableHttpServerConfig;
use rmcp::transport::StreamableHttpService;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::json;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::connection_handling_websocket::WsClient;
use super::connection_handling_websocket::connect_websocket;
use super::connection_handling_websocket::create_config_toml;
use super::connection_handling_websocket::read_jsonrpc_message;
use super::connection_handling_websocket::send_request;
use super::connection_handling_websocket::spawn_websocket_server;

const SERVER_NAME: &str = "codex_tui";

#[derive(Clone)]
struct PingServer;

impl ServerHandler for PingServer {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
    }

    async fn list_tools(
        &self,
        _request: Option<rmcp::model::PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, rmcp::ErrorData> {
        let schema: JsonObject = serde_json::from_value(json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        }))
        .map_err(|err| rmcp::ErrorData::internal_error(err.to_string(), None))?;
        let mut tool = Tool::new(
            Cow::Borrowed("ping"),
            Cow::Borrowed("Answer pong."),
            Arc::new(schema),
        );
        tool.annotations = Some(ToolAnnotations::new().read_only(true));
        Ok(ListToolsResult::with_all_items(vec![tool]))
    }

    async fn call_tool(
        &self,
        _request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<rmcp::model::CallToolResponse, rmcp::ErrorData> {
        Ok(CallToolResult::success(vec![ContentBlock::text("pong")]).into())
    }
}

struct DeleteGate {
    authorization: String,
    hang_delete: bool,
    deletes: Arc<AtomicUsize>,
}

async fn gate_mcp(
    State(gate): State<Arc<DeleteGate>>,
    request: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    if request
        .headers()
        .get(AUTHORIZATION)
        .is_none_or(|value| value.as_bytes() != gate.authorization.as_bytes())
    {
        return Err(StatusCode::UNAUTHORIZED);
    }
    if request.method() == Method::DELETE {
        gate.deletes.fetch_add(1, Ordering::SeqCst);
        if gate.hang_delete {
            std::future::pending::<()>().await;
        }
    }
    Ok(next.run(request).await)
}

async fn answer_proof(State(proof): State<Arc<(String, String)>>, nonce: String) -> String {
    codex_rmcp_client::hosted_endpoint_proof(&proof.0, &nonce, &proof.1)
}

/// A stand-in for one TUI's hosted tools server.
struct HostedEndpoint {
    url: String,
    authorization: String,
    capability: String,
    deletes: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl Drop for HostedEndpoint {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl HostedEndpoint {
    async fn start(tag: &str, hang_delete: bool) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/mcp", listener.local_addr()?);
        let authorization = format!("Bearer config-{tag}");
        let capability = format!("retirement-capability-{tag}");
        let deletes = Arc::new(AtomicUsize::new(0));
        let service = StreamableHttpService::new(
            || Ok(PingServer),
            Arc::new(LocalSessionManager::default()),
            StreamableHttpServerConfig::default(),
        );
        let gate = Arc::new(DeleteGate {
            authorization: authorization.clone(),
            hang_delete,
            deletes: Arc::clone(&deletes),
        });
        let router = Router::new()
            .nest_service("/mcp", service)
            .layer(middleware::from_fn_with_state(gate, gate_mcp))
            .merge(
                Router::new()
                    .route(
                        codex_rmcp_client::HOSTED_ENDPOINT_PROOF_PATH,
                        axum::routing::post(answer_proof),
                    )
                    .with_state(Arc::new((capability.clone(), url.clone()))),
            );
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        Ok(Self {
            url,
            authorization,
            capability,
            deletes,
            task,
        })
    }

    fn deletes(&self) -> usize {
        self.deletes.load(Ordering::SeqCst)
    }

    fn declare(&self, capability: &str) -> McpHostedEndpointDeclareParams {
        McpHostedEndpointDeclareParams {
            url: self.url.clone(),
            authorization: self.authorization.clone(),
            retirement_capability: capability.to_owned(),
            timeout_ms: 2_000,
        }
    }

    fn retire(&self, capability: &str, timeout_ms: u64) -> McpHostedEndpointRetireParams {
        McpHostedEndpointRetireParams {
            url: self.url.clone(),
            authorization: self.authorization.clone(),
            retirement_capability: capability.to_owned(),
            timeout_ms,
        }
    }

    fn overrides(&self) -> HashMap<String, serde_json::Value> {
        HashMap::from([(
            format!("mcp_servers.{SERVER_NAME}"),
            json!({"url": self.url, "http_headers": {"Authorization": self.authorization}}),
        )])
    }

    fn thread_start(&self) -> ThreadStartParams {
        ThreadStartParams {
            model: Some("mock-model".to_string()),
            config: Some(self.overrides()),
            ..Default::default()
        }
    }

    fn thread_resume(&self, thread_id: &str) -> ThreadResumeParams {
        ThreadResumeParams {
            thread_id: thread_id.to_string(),
            config: Some(self.overrides()),
            ..Default::default()
        }
    }

    fn host_config(&self, name: &str) -> String {
        format!(
            "\n[mcp_servers.{name}]\nurl = \"{}\"\nhttp_headers = {{ Authorization = \"{}\" }}\n",
            self.url, self.authorization
        )
    }
}

enum Reply {
    Result(serde_json::Value),
    Error(JSONRPCError),
}

async fn exchange(
    stream: &mut WsClient,
    id: i64,
    method: &str,
    params: impl Serialize,
) -> Result<Reply> {
    send_request(stream, method, id, Some(serde_json::to_value(params)?)).await?;
    let target = RequestId::Integer(id);
    loop {
        match read_jsonrpc_message(stream).await? {
            JSONRPCMessage::Response(response) if response.id == target => {
                return Ok(Reply::Result(serde_json::to_value(response.result)?));
            }
            JSONRPCMessage::Error(error) if error.id == target => return Ok(Reply::Error(error)),
            _ => {}
        }
    }
}

async fn call<T: DeserializeOwned>(
    stream: &mut WsClient,
    id: i64,
    method: &str,
    params: impl Serialize,
) -> Result<T> {
    match exchange(stream, id, method, params).await? {
        Reply::Result(value) => Ok(serde_json::from_value(value)?),
        Reply::Error(error) => bail!("{method} failed: {error:?}"),
    }
}

async fn call_error(
    stream: &mut WsClient,
    id: i64,
    method: &str,
    params: impl Serialize,
) -> Result<JSONRPCError> {
    match exchange(stream, id, method, params).await? {
        Reply::Result(value) => bail!("{method} unexpectedly succeeded: {value}"),
        Reply::Error(error) => Ok(error),
    }
}

async fn connect_client(bind_addr: std::net::SocketAddr, name: &str) -> Result<WsClient> {
    let mut stream = connect_websocket(bind_addr).await?;
    let _: serde_json::Value = call(
        &mut stream,
        /*id*/ 1,
        "initialize",
        InitializeParams {
            client_info: ClientInfo {
                name: name.to_string(),
                title: None,
                version: "0.1.0".to_string(),
            },
            capabilities: Some(InitializeCapabilities {
                experimental_api: true,
                ..Default::default()
            }),
        },
    )
    .await?;
    Ok(stream)
}

/// Starts a thread whose overrides name `endpoint` and proves it connected.
async fn start_connected_thread(
    stream: &mut WsClient,
    id: i64,
    endpoint: &HostedEndpoint,
) -> Result<String> {
    let started: ThreadStartResponse =
        call(stream, id, "thread/start", endpoint.thread_start()).await?;
    ping(stream, id + 1, &started.thread.id, SERVER_NAME).await?;
    Ok(started.thread.id)
}

async fn ping(stream: &mut WsClient, id: i64, thread_id: &str, server: &str) -> Result<()> {
    let response: McpServerToolCallResponse = call(
        stream,
        id,
        "mcpServer/tool/call",
        McpServerToolCallParams {
            thread_id: thread_id.to_string(),
            server: server.to_string(),
            tool: "ping".to_string(),
            arguments: Some(json!({})),
            meta: None,
        },
    )
    .await?;
    assert_eq!(response.is_error, Some(false));
    Ok(())
}

/// Runs a real user turn against the mocked model and waits for both the
/// `turn/start` response and this thread's `turn/completed` notification.
async fn complete_turn(stream: &mut WsClient, id: i64, thread_id: &str) -> Result<()> {
    send_request(
        stream,
        "turn/start",
        id,
        Some(serde_json::to_value(TurnStartParams {
            thread_id: thread_id.to_string(),
            input: vec![UserInput::Text {
                text: "Persist this thread".to_string(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        })?),
    )
    .await?;
    let target = RequestId::Integer(id);
    let (mut started, mut completed) = (false, false);
    while !(started && completed) {
        match read_jsonrpc_message(stream).await? {
            JSONRPCMessage::Response(response) if response.id == target => started = true,
            JSONRPCMessage::Error(error) if error.id == target => {
                bail!("turn/start failed: {error:?}")
            }
            JSONRPCMessage::Notification(notification)
                if notification.method == "turn/completed"
                    && notification_thread_id(&notification.params) == Some(thread_id) =>
            {
                completed = true;
            }
            _ => {}
        }
    }
    Ok(())
}

fn notification_thread_id(params: &Option<serde_json::Value>) -> Option<&str> {
    params
        .as_ref()
        .and_then(|params| params.get("threadId"))
        .and_then(serde_json::Value::as_str)
}

const DECLARE: &str = "kcf/mcpServer/hostedEndpoint/declare";
const RETIRE: &str = "kcf/mcpServer/hostedEndpoint/retire";

fn refused(reason: McpHostedEndpointRefusalReason) -> McpHostedEndpointRetireResponse {
    McpHostedEndpointRetireResponse::Refused { reason }
}

fn declare_refused(reason: McpHostedEndpointRefusalReason) -> McpHostedEndpointDeclareResponse {
    McpHostedEndpointDeclareResponse::Refused { reason }
}

async fn with_daemon<F, Fut>(extra_config: String, scenario: F) -> Result<()>
where
    F: FnOnce(std::net::SocketAddr) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let responses = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &responses.uri(), "never")?;
    if !extra_config.is_empty() {
        let path = codex_home.path().join("config.toml");
        let config = std::fs::read_to_string(&path)?;
        std::fs::write(&path, format!("{config}{extra_config}"))?;
    }
    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;
    let result = scenario(bind_addr).await;
    process
        .kill()
        .await
        .context("failed to stop websocket app-server process")?;
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_exit_retires_only_its_own_endpoint_with_receipts() -> Result<()> {
    let mine = HostedEndpoint::start("mine", /*hang_delete*/ false).await?;
    let theirs = HostedEndpoint::start("theirs", /*hang_delete*/ false).await?;
    with_daemon(String::new(), |bind_addr| async move {
        let mut ws1 = connect_client(bind_addr, "tui-one").await?;
        let mut ws2 = connect_client(bind_addr, "tui-two").await?;

        // No blind probes: an endpoint this connection never named is refused.
        let unused: McpHostedEndpointDeclareResponse =
            call(&mut ws1, 10, DECLARE, mine.declare(&mine.capability)).await?;
        assert_eq!(
            unused,
            declare_refused(McpHostedEndpointRefusalReason::NotUsed)
        );

        let my_thread = start_connected_thread(&mut ws1, 20, &mine).await?;
        let their_thread = start_connected_thread(&mut ws2, 20, &theirs).await?;
        let declared: McpHostedEndpointDeclareResponse =
            call(&mut ws2, 30, DECLARE, theirs.declare(&theirs.capability)).await?;
        assert!(matches!(
            declared,
            McpHostedEndpointDeclareResponse::Declared { .. }
        ));

        // ws2 shares mine's config credential (it names the endpoint in its own
        // thread), but not the retirement capability: the challenge fails.
        let shared: ThreadStartResponse =
            call(&mut ws2, 31, "thread/start", mine.thread_start()).await?;
        let forged: McpHostedEndpointDeclareResponse =
            call(&mut ws2, 32, DECLARE, mine.declare("forged")).await?;
        assert_eq!(
            forged,
            declare_refused(McpHostedEndpointRefusalReason::ProofFailed)
        );
        let other: McpHostedEndpointRetireResponse =
            call(&mut ws2, 33, RETIRE, mine.retire(&mine.capability, 5_000)).await?;
        assert_eq!(other, refused(McpHostedEndpointRefusalReason::NotOwner));
        assert_eq!(mine.deletes(), 0);

        // A real mocked turn persists the thread, so it can be resumed after
        // the reconnect. Require positive persistence evidence first.
        complete_turn(&mut ws1, 34, &my_thread).await?;
        let read: ThreadReadResponse = call(
            &mut ws1,
            35,
            "thread/read",
            ThreadReadParams {
                thread_id: my_thread.clone(),
                include_turns: true,
            },
        )
        .await?;
        assert!(
            !read.thread.turns.is_empty(),
            "the completed turn is persisted"
        );
        let rollout = read
            .thread
            .path
            .clone()
            .context("a persisted thread has a rollout path")?;
        assert!(rollout.exists(), "rollout {} exists", rollout.display());

        // Close, then reconnect: use and declaration did not survive the connection.
        drop(ws1);
        let mut ws1 = connect_client(bind_addr, "tui-one-reconnected").await?;
        let stale: McpHostedEndpointRetireResponse =
            call(&mut ws1, 40, RETIRE, mine.retire(&mine.capability, 5_000)).await?;
        assert_eq!(stale, refused(McpHostedEndpointRefusalReason::NotOwner));
        let unused: McpHostedEndpointDeclareResponse =
            call(&mut ws1, 41, DECLARE, mine.declare(&mine.capability)).await?;
        assert_eq!(
            unused,
            declare_refused(McpHostedEndpointRefusalReason::NotUsed)
        );
        let _: serde_json::Value = call(
            &mut ws1,
            42,
            "thread/resume",
            mine.thread_resume(&my_thread),
        )
        .await?;
        let declared: McpHostedEndpointDeclareResponse =
            call(&mut ws1, 43, DECLARE, mine.declare(&mine.capability)).await?;
        assert!(matches!(
            declared,
            McpHostedEndpointDeclareResponse::Declared { .. }
        ));

        let retired: McpHostedEndpointRetireResponse =
            call(&mut ws1, 44, RETIRE, mine.retire(&mine.capability, 5_000)).await?;
        let McpHostedEndpointRetireResponse::Retired { receipts, census } = retired else {
            bail!("expected a proven retirement, got {retired:?}");
        };
        // Every connection to the dying endpoint is retired, including the
        // thread that only shared its config credential; all with proof.
        assert!(
            receipts
                .iter()
                .any(|receipt| receipt.thread_id.as_deref() == Some(my_thread.as_str()))
        );
        assert!(receipts.iter().all(|receipt| {
            receipt.outcome == McpHostedEndpointRetirementOutcome::Retired
                && receipt.thread_id.as_deref() != Some(their_thread.as_str())
        }));
        let _ = shared;
        assert_eq!((census.unknown, census.possibly_untracked), (0, false));
        assert!(mine.deletes() >= 1, "the DELETE reached the live endpoint");

        // Re-admission of the retired endpoint is refused.
        let readmit = call_error(&mut ws1, 50, "thread/start", mine.thread_start()).await?;
        assert!(readmit.error.message.contains("retired"), "{readmit:?}");
        let redeclare: McpHostedEndpointDeclareResponse =
            call(&mut ws1, 51, DECLARE, mine.declare(&mine.capability)).await?;
        assert_eq!(
            redeclare,
            declare_refused(McpHostedEndpointRefusalReason::Retired)
        );

        // The other client's endpoint and thread carry on untouched.
        ping(&mut ws2, 60, &their_thread, SERVER_NAME).await?;
        assert_eq!(theirs.deletes(), 0);
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unproven_or_untracked_retirement_is_incomplete_without_extension() -> Result<()> {
    let hung = HostedEndpoint::start("hung", /*hang_delete*/ true).await?;
    let early = HostedEndpoint::start("early", /*hang_delete*/ false).await?;
    // The same endpoint is also host-configured, so a thread connects to it
    // before any connection names it in an override.
    with_daemon(early.host_config("early"), |bind_addr| async move {
        let mut ws = connect_client(bind_addr, "tui").await?;
        let plain: ThreadStartResponse = call(
            &mut ws,
            10,
            "thread/start",
            ThreadStartParams {
                model: Some("mock-model".to_string()),
                ..Default::default()
            },
        )
        .await?;
        ping(&mut ws, 11, &plain.thread.id, "early").await?;
        start_connected_thread(&mut ws, 12, &early).await?;
        let declared: McpHostedEndpointDeclareResponse =
            call(&mut ws, 14, DECLARE, early.declare(&early.capability)).await?;
        assert!(matches!(
            declared,
            McpHostedEndpointDeclareResponse::Declared { .. }
        ));
        let untracked: McpHostedEndpointRetireResponse =
            call(&mut ws, 15, RETIRE, early.retire(&early.capability, 5_000)).await?;
        let McpHostedEndpointRetireResponse::Incomplete { census, .. } = untracked else {
            bail!("unknown ownership must be incomplete, got {untracked:?}");
        };
        assert!(census.possibly_untracked);

        let hung_thread = start_connected_thread(&mut ws, 20, &hung).await?;
        let declared: McpHostedEndpointDeclareResponse =
            call(&mut ws, 22, DECLARE, hung.declare(&hung.capability)).await?;
        assert!(matches!(
            declared,
            McpHostedEndpointDeclareResponse::Declared { .. }
        ));

        let expired: McpHostedEndpointRetireResponse =
            call(&mut ws, 30, RETIRE, hung.retire(&hung.capability, 0)).await?;
        assert_eq!(expired, refused(McpHostedEndpointRefusalReason::Expired));
        assert_eq!(hung.deletes(), 0, "expired intake drives no cleanup");

        let started = Instant::now();
        let timed_out: McpHostedEndpointRetireResponse =
            call(&mut ws, 31, RETIRE, hung.retire(&hung.capability, 500)).await?;
        assert!(
            started.elapsed() < Duration::from_millis(500) + Duration::from_secs(3),
            "retirement ran {:?} past its budget",
            started.elapsed()
        );
        let McpHostedEndpointRetireResponse::Incomplete { receipts, .. } = timed_out else {
            bail!("a hung DELETE must stay incomplete, got {timed_out:?}");
        };
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].thread_id.as_deref(), Some(hung_thread.as_str()));
        assert_ne!(
            receipts[0].outcome,
            McpHostedEndpointRetirementOutcome::Retired
        );
        let deletes = hung.deletes();
        assert!(deletes >= 1, "the DELETE was sent but never answered");

        // Sticky, and the expired latched deadline is retained: a later,
        // generous budget drives no new cleanup.
        let again: McpHostedEndpointRetireResponse =
            call(&mut ws, 32, RETIRE, hung.retire(&hung.capability, 5_000)).await?;
        assert!(matches!(
            again,
            McpHostedEndpointRetireResponse::Incomplete { .. }
        ));
        assert_eq!(hung.deletes(), deletes);
        Ok(())
    })
    .await
}
