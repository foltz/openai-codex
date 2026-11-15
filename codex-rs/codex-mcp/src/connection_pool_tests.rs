//! Tests for shared MCP connection ownership and protocol negotiation.

use super::*;
use crate::McpServerPermissions;
use crate::binding_clients::McpBindingClients;
use crate::connection_manager::tests::create_ready_async_managed_client;
use crate::connection_manager::tests::create_test_managed_client_for_transport;
use crate::connection_manager::tests::create_test_tool;
use crate::connection_manager::tests::reusable_server_identity;
use crate::runtime::McpRuntimeContext;
use codex_config::McpServerConfig;
use codex_exec_server::EnvironmentManager;
use codex_login::CodexAuth;
use codex_rmcp_client::Elicitation;
use codex_rmcp_client::InProcessTransportFactory;
use codex_rmcp_client::RmcpClient;
use codex_rmcp_client::RmcpClientRetirement;
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use rmcp::ServerHandler;
use rmcp::ServiceExt;
use rmcp::model::ClientCapabilities;
use rmcp::model::Implementation;
use rmcp::model::InitializeRequestParams;
use rmcp::model::ListResourceTemplatesResult;
use rmcp::model::ListResourcesResult;
use rmcp::model::ListToolsResult;
use rmcp::model::PaginatedRequestParams;
use rmcp::model::ProtocolVersion;
use rmcp::model::ServerCapabilities;
use rmcp::model::ServerConfig;
use rmcp::model::Tool;
use rmcp::service::RequestContext;
use rmcp::service::RoleServer;
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::sync::Notify;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;

#[derive(Clone)]
pub(crate) struct TestServer {
    tools: Arc<Mutex<Vec<Tool>>>,
    block_listing: Arc<AtomicBool>,
    started_listings: Arc<Semaphore>,
    cancelled_listings: Arc<Semaphore>,
}

impl Default for TestServer {
    fn default() -> Self {
        Self {
            tools: Arc::new(Mutex::new(vec![
                create_test_tool("test_server", "test").tool,
            ])),
            block_listing: Arc::default(),
            started_listings: Arc::new(Semaphore::new(/*permits*/ 0)),
            cancelled_listings: Arc::new(Semaphore::new(/*permits*/ 0)),
        }
    }
}

impl ServerHandler for TestServer {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .build(),
        )
    }

    async fn call_tool(
        &self,
        _request: rmcp::model::CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<rmcp::model::CallToolResponse, rmcp::ErrorData> {
        Ok(rmcp::model::CallToolResult::success(Vec::new()).into())
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, rmcp::ErrorData> {
        if self.block_listing.load(Ordering::Acquire) {
            context.ct.cancelled().await;
            self.cancelled_listings.add_permits(1);
            return Err(rmcp::ErrorData::internal_error(
                "listing cancelled",
                /*data*/ None,
            ));
        }
        Ok(ListToolsResult::with_all_items(
            self.tools.lock().unwrap().clone(),
        ))
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, rmcp::ErrorData> {
        if self.block_listing.load(Ordering::Acquire) {
            self.started_listings.add_permits(1);
            context.ct.cancelled().await;
            self.cancelled_listings.add_permits(1);
        }
        Ok(ListResourcesResult::with_all_items(Vec::new()))
    }

    async fn list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, rmcp::ErrorData> {
        if self.block_listing.load(Ordering::Acquire) {
            self.started_listings.add_permits(1);
            context.ct.cancelled().await;
            self.cancelled_listings.add_permits(1);
        }
        Ok(ListResourceTemplatesResult::with_all_items(Vec::new()))
    }
}

impl InProcessTransportFactory for TestServer {
    fn open(&self) -> BoxFuture<'static, std::io::Result<tokio::io::DuplexStream>> {
        let server = self.clone();
        async move {
            let (client, transport) = tokio::io::duplex(4096);
            tokio::spawn(async move {
                server
                    .serve(transport)
                    .await
                    .unwrap()
                    .waiting()
                    .await
                    .unwrap();
            });
            Ok(client)
        }
        .boxed()
    }
}

pub(crate) fn key(context: &McpRuntimeContext, config: &McpServerConfig) -> ConnectionKey {
    ConnectionKey {
        server_name: "test_server".to_string(),
        identity: reusable_server_identity("test_server", config, context),
        account: ConnectorRuntimeContextKey::personal(
            /*account_id*/ None, /*chatgpt_user_id*/ None,
        ),
        auth_state: None,
        startup_timeout: Duration::from_secs(30),
        catalog_item_limit: 100,
    }
}

pub(crate) fn context() -> McpRuntimeContext {
    McpRuntimeContext::new(
        Arc::new(EnvironmentManager::default_for_tests()),
        std::env::temp_dir(),
    )
}

pub(crate) fn config() -> McpServerConfig {
    serde_json::from_value(json!({"command": "test-server", "args": ["server.mjs"]})).unwrap()
}

pub(crate) fn startup(
    sharing: ConnectionSharing,
    starts: Arc<AtomicUsize>,
    first_started: Arc<Notify>,
    release_first: Arc<Notify>,
    server: impl InProcessTransportFactory + Clone + 'static,
) -> impl Fn(
    CancellationToken,
    Option<CancellationToken>,
    Option<RmcpClientRetirement>,
) -> ManagedClientFuture {
    startup_with_auth(sharing, starts, first_started, release_first, server, None)
}

fn startup_with_auth(
    sharing: ConnectionSharing,
    starts: Arc<AtomicUsize>,
    first_started: Arc<Notify>,
    release_first: Arc<Notify>,
    server: impl InProcessTransportFactory + Clone + 'static,
    auth: Option<(
        crate::McpWorkerOwner,
        tokio::sync::watch::Receiver<AuthChangeState>,
    )>,
) -> impl Fn(
    CancellationToken,
    Option<CancellationToken>,
    Option<RmcpClientRetirement>,
) -> ManagedClientFuture {
    move |cancellation, _sharing_declined, retirement| {
        let sharing = sharing.clone();
        let starts = Arc::clone(&starts);
        let first_started = Arc::clone(&first_started);
        let release_first = Arc::clone(&release_first);
        let server = server.clone();
        let auth = auth.clone();
        async move {
            async {
                if starts.fetch_add(1, Ordering::SeqCst) == 0 {
                    first_started.notify_one();
                    release_first.notified().await;
                }
                let transport = RmcpClient::new_in_process_client_in_retirement(
                    Arc::new(server),
                    retirement.unwrap_or_default(),
                )
                .await
                .expect("create test MCP client");
                let mut client = create_test_managed_client_for_transport(Vec::new(), transport);
                let initialized = client
                    .client
                    .initialize(
                        InitializeRequestParams::new(
                            ClientCapabilities::default(),
                            Implementation::new("test", "1"),
                        )
                        .with_protocol_version(ProtocolVersion::V_2025_06_18),
                        Some(Duration::from_secs(30)),
                        Box::new(|_, _| {
                            futures::future::ready(Ok(ElicitationResponse {
                                action: ElicitationAction::Decline,
                                content: None,
                                meta: None,
                            }))
                            .boxed()
                        }),
                    )
                    .await?;
                if let Some((workers, receiver)) = auth {
                    client._auth_change_notifications = crate::auth_changes::start(
                        &workers,
                        Arc::clone(&client.client),
                        &initialized.capabilities,
                        Some(receiver),
                    )
                    .await?;
                }
                let (tools, meta) = list_tools_for_client_uncached(
                    "test_server",
                    /*is_codex_apps_mcp_server*/ false,
                    "test",
                    &client.client,
                    /*timeout*/ None,
                    /*catalog_item_limit*/ 100,
                    /*server_instructions*/ None,
                )
                .await?;
                client.tool_catalog =
                    Arc::new(ClientToolCatalog::new(tools, meta, /*updates*/ None));
                client.connection_sharing = sharing;
                Ok(client)
            }
            .or_cancel(&cancellation)
            .await
            .unwrap_or(Err(StartupOutcomeError::Cancelled))
        }
        .boxed()
        .shared()
    }
}

struct LeaseFrame {
    message: serde_json::Value,
    replies: mpsc::UnboundedSender<serde_json::Value>,
}

impl LeaseFrame {
    fn reply(&self, result: serde_json::Value) {
        self.replies
            .send(json!({"jsonrpc": "2.0", "id": self.message["id"], "result": result}))
            .unwrap();
    }
}

#[derive(Clone)]
struct LeaseTransport(mpsc::UnboundedSender<LeaseFrame>, bool);

impl InProcessTransportFactory for LeaseTransport {
    fn open(&self) -> BoxFuture<'static, std::io::Result<tokio::io::DuplexStream>> {
        let (client, server) = tokio::io::duplex(4096);
        let (reader, mut writer) = tokio::io::split(server);
        let mut lines = BufReader::new(reader).lines();
        let frames = self.0.clone();
        let auth_changes = self.1;
        let (replies, mut responses) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(response) = responses.recv() => {
                        writer.write_all(format!("{response}\n").as_bytes()).await.unwrap();
                    }
                    line = lines.next_line() => {
                        let Some(line) = line.unwrap() else { break };
                        let frame = LeaseFrame {
                            message: serde_json::from_str(&line).unwrap(),
                            replies: replies.clone(),
                        };
                        match frame.message["method"].as_str().unwrap() {
                            "initialize" => {
                                let capabilities = if auth_changes {
                                    json!({"tools": {}, "experimental": {"codex/auth-change": {}}})
                                } else { json!({"tools": {}}) };
                                frame.reply(json!({
                                    "protocolVersion": "2025-06-18", "capabilities": capabilities,
                                    "serverInfo": {"name": "test", "version": "1"}
                                }));
                            },
                            "notifications/initialized" => {}
                            "tools/list" => frame.reply(json!({"tools": []})),
                            _ => frames.send(frame).unwrap(),
                        }
                    }
                }
            }
        });
        futures::future::ready(Ok(client)).boxed()
    }
}

struct LeasePeer {
    pool: McpConnectionPool,
    key: ConnectionKey,
    transport: LeaseTransport,
    frames: mpsc::UnboundedReceiver<LeaseFrame>,
}

impl LeasePeer {
    fn new() -> Self {
        let (tx, frames) = mpsc::unbounded_channel();
        Self {
            pool: McpConnectionPool::default(),
            key: key(&context(), &config()),
            transport: LeaseTransport(tx, false),
            frames,
        }
    }

    fn attach(&self) -> tokio::task::JoinHandle<Result<ManagedClient, StartupOutcomeError>> {
        let pool = self.pool.clone();
        let key = self.key.clone();
        let transport = self.transport.clone();
        tokio::spawn(async move {
            let ready = Arc::<Notify>::default();
            ready.notify_one();
            let launch = startup(
                ConnectionSharing::Negotiated,
                Arc::default(),
                Arc::default(),
                ready,
                transport,
            );
            pool.connect(
                Arc::default(),
                key,
                Arc::new(Mutex::new(Some(
                    json!({"extensions": {LEASE_CAPABILITY: {}}}),
                ))),
                /*tool_catalog_cache_context*/ None,
                CancellationToken::new(),
                launch,
            )
            .await
        })
    }

    async fn next(&mut self, method: &str) -> LeaseFrame {
        let frame = tokio::time::timeout(Duration::from_secs(2), self.frames.recv())
            .await
            .expect("expected a frame from the client")
            .unwrap();
        assert_eq!(frame.message["method"], method);
        frame
    }

    async fn chat(&mut self) -> (ManagedClient, serde_json::Value) {
        let attach = self.attach();
        let open = self.next("codex/connection/open").await;
        open.reply(json!({}));
        (
            attach.await.unwrap().unwrap(),
            open.message["params"]["leaseId"].clone(),
        )
    }
}

#[tokio::test(start_paused = true)]
async fn releasing_lease_cancels_pending_call_before_release() {
    let mut peer = LeasePeer::new();
    let (first, first_id) = peer.chat().await;
    let (other, other_id) = peer.chat().await;
    let call = first.call_tool(
        "wait".into(),
        /*arguments*/ None,
        |_| Ok(None),
        /*timeout*/ None,
    );
    tokio::pin!(call);
    let request = tokio::select! {
        result = &mut call => panic!("call completed without a reply: {result:?}"),
        frame = peer.next("tools/call") => frame,
    };
    let ConnectionSharing::Shared(lease) = &first.connection_sharing else {
        panic!("expected shared lease")
    };
    lease.release();
    // Leave the caller unpolled: the release must drive the cancellation write itself.
    let cancel = peer.next("notifications/cancelled").await;
    assert_eq!(cancel.message["params"]["requestId"], request.message["id"]);
    let release = peer.next("codex/connection/release").await;
    assert_eq!(release.message["params"]["leaseId"], first_id);
    release.reply(json!({}));
    assert!(call.await.is_err());

    let (result, ()) = tokio::join!(
        other.call_tool(
            "test".into(),
            /*arguments*/ None,
            |_| Ok(None),
            /*timeout*/ None,
        ),
        async {
            let request = peer.next("tools/call").await;
            assert_eq!(request.message["params"]["_meta"][LEASE_META_KEY], other_id);
            request.reply(json!({"content": []}));
        }
    );
    result.unwrap();
}

#[tokio::test(start_paused = true)]
async fn dropping_lease_outside_runtime_releases_it() {
    let mut peer = LeasePeer::new();
    let (_other, _) = peer.chat().await;
    let (chat, id) = peer.chat().await;
    std::thread::spawn(move || drop(chat)).join().unwrap();
    let release = peer.next("codex/connection/release").await;
    assert_eq!(release.message["params"]["leaseId"], id);
    release.reply(json!({}));
}

#[tokio::test(start_paused = true)]
async fn timed_out_open_is_released_after_the_server_acknowledges_it() {
    let mut peer = LeasePeer::new();
    let (_other, _) = peer.chat().await;
    let attach = peer.attach();
    let open = peer.next("codex/connection/open").await;
    tokio::time::advance(LEASE_REQUEST_TIMEOUT).await;
    assert!(attach.await.unwrap().is_err());
    assert!(
        tokio::time::timeout(Duration::from_secs(1), peer.frames.recv())
            .await
            .is_err()
    );
    open.reply(json!({}));
    let release = peer.next("codex/connection/release").await;
    assert_eq!(
        release.message["params"]["leaseId"],
        open.message["params"]["leaseId"]
    );
    release.reply(json!({}));
}

#[tokio::test]
async fn shared_startup_survives_first_chat_cancellation_and_rejects_released_bindings() {
    let pool = McpConnectionPool::default();
    let key = key(&context(), &config());
    let server = TestServer::default();
    let starts = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let launch = startup(
        ConnectionSharing::Negotiated,
        Arc::clone(&starts),
        Arc::clone(&started),
        Arc::clone(&release),
        server.clone(),
    );
    let first_cancel = CancellationToken::new();
    let first = pool.connect(
        Arc::default(),
        key.clone(),
        Arc::default(),
        /*tool_catalog_cache_context*/ None,
        first_cancel.clone(),
        &launch,
    );
    let second = pool.connect(
        Arc::default(),
        key.clone(),
        Arc::default(),
        /*tool_catalog_cache_context*/ None,
        CancellationToken::new(),
        &launch,
    );
    tokio::pin!(first, second);
    assert!(futures::poll!(&mut first).is_pending());
    assert!(futures::poll!(&mut second).is_pending());
    started.notified().await;
    first_cancel.cancel();
    assert!(matches!(first.await, Err(StartupOutcomeError::Cancelled)));
    release.notify_one();
    let client = second.await.unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert!(!client.client.is_closed().await);

    let other = pool
        .connect(
            Arc::default(),
            key,
            Arc::default(),
            /*tool_catalog_cache_context*/ None,
            CancellationToken::new(),
            &launch,
        )
        .await
        .unwrap();
    let mut config = crate::mcp::tests::test_mcp_config(std::env::temp_dir());
    config
        .server_permissions
        .insert("test_server".to_string(), McpServerPermissions::Thread);
    let prepared = crate::binding::PreparedMcpCall::new(
        Arc::new(crate::connection_manager::McpConnectionSet::empty(
            /*prefix_mcp_tool_names*/ true,
        )),
        Arc::new(client.clone()),
        Arc::new(config),
        client.tool_catalog.read(Arc::new).await,
        create_test_tool("test_server", "test"),
        crate::server::McpServerMetadata {
            environment_id: "local".to_string(),
            pollutes_memory: false,
            origin: None,
            supports_parallel_tool_calls: true,
            default_tools_approval_mode: None,
            tool_approval_modes: Default::default(),
        },
        /*plugin_id*/ None,
        /*selected_plugin_server*/ false,
    )
    .unwrap();
    let bindings = McpBindingClients::new(
        HashMap::from([("test_server".to_string(), Arc::new(client.clone()))]),
        Vec::new(),
    );
    server.block_listing.store(true, Ordering::Release);
    let waiting = async {
        tokio::join!(
            bindings.list_resources("test_server", /*params*/ None),
            bindings.list_resource_templates("test_server", /*params*/ None),
        )
    };
    tokio::pin!(waiting);
    tokio::select! {
        _ = &mut waiting => panic!("resource listings must wait for cancellation"),
        permits = server.started_listings.acquire_many(2) => permits.unwrap().forget(),
    }
    let mut owned = create_ready_async_managed_client(Vec::new()).await;
    owned.client = futures::future::ready(Ok(client.clone())).boxed().shared();
    owned.shutdown().await;
    let (resources, templates) = waiting.await;
    assert!(resources.is_err());
    assert!(templates.is_err());
    tokio::time::timeout(
        Duration::from_secs(1),
        server.cancelled_listings.acquire_many(2),
    )
    .await
    .expect("released resource listings must be cancelled")
    .unwrap()
    .forget();
    let prepared_error = prepared
        .call_with_preparation(/*requested_timeout*/ None, || async {
            panic!("released bindings must not perform irreversible preparation")
        })
        .await
        .unwrap_err();
    assert!(
        prepared_error
            .to_string()
            .contains("lease has been released")
    );
    other
        .call_tool(
            "test".to_string(),
            /*arguments*/ None,
            |_| Ok(None),
            /*timeout*/ None,
        )
        .await
        .unwrap();
    owned.client = futures::future::ready(Ok(other)).boxed().shared();
    owned.shutdown().await;
}

#[tokio::test]
async fn callback_during_startup_releases_waiting_chats_without_sharing_authority() {
    let pool = McpConnectionPool::default();
    let key = key(&context(), &config());
    let starts = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let second_callback = Arc::new(Notify::new());
    let release_second = Arc::new(Notify::new());
    let callbacks = Arc::new(Mutex::new(Vec::new()));
    let launch = |name| {
        let launch = startup(
            ConnectionSharing::Private,
            Arc::clone(&starts),
            Arc::clone(&started),
            Arc::clone(&release),
            TestServer::default(),
        );
        let callbacks = Arc::clone(&callbacks);
        let second_callback = Arc::clone(&second_callback);
        let release_second = Arc::clone(&release_second);
        move |cancellation, sharing_declined: Option<CancellationToken>, retirement| {
            let startup = launch(cancellation, sharing_declined.clone(), retirement);
            let callbacks = Arc::clone(&callbacks);
            let second_callback = Arc::clone(&second_callback);
            let release_second = Arc::clone(&release_second);
            async move {
                let mut client = startup.await?;
                let (negotiation, callback) = SharingNegotiation::new(
                    Box::new(move |_, _| {
                        callbacks.lock().unwrap().push(name);
                        futures::future::ready(Ok(ElicitationResponse {
                            action: ElicitationAction::Decline,
                            content: None,
                            meta: None,
                        }))
                        .boxed()
                    }),
                    sharing_declined.unwrap_or_default(),
                );
                callback(
                    rmcp::model::RequestId::Number(0),
                    Elicitation::OpenAiForm {
                        meta: None,
                        message: "startup prompt".to_string(),
                        requested_schema: json!({}),
                    },
                )
                .await
                .unwrap();
                if name == "first" {
                    second_callback.notified().await;
                } else {
                    second_callback.notify_one();
                    release_second.notified().await;
                }
                client.connection_sharing = negotiation.finish(&ProtocolVersion::V_2026_07_28);
                Ok(client)
            }
            .boxed()
            .shared()
        }
    };
    let first = pool.connect(
        Arc::default(),
        key.clone(),
        Arc::default(),
        /*tool_catalog_cache_context*/ None,
        CancellationToken::new(),
        launch("first"),
    );
    let second = pool.connect(
        Arc::default(),
        key,
        Arc::default(),
        /*tool_catalog_cache_context*/ None,
        CancellationToken::new(),
        launch("second"),
    );
    tokio::pin!(first, second);
    assert!(futures::poll!(&mut first).is_pending());
    assert!(futures::poll!(&mut second).is_pending());
    started.notified().await;
    release.notify_one();
    let first = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::select! {
            first = &mut first => first,
            _ = &mut second => panic!("the joining chat should still be starting"),
        }
    })
    .await
    .expect("the joining chat must start before the first finishes")
    .unwrap();
    first
        .call_tool(
            "test".to_string(),
            /*arguments*/ None,
            |_| Ok(None),
            /*timeout*/ None,
        )
        .await
        .unwrap();
    let mut owner = create_ready_async_managed_client(Vec::new()).await;
    owner.client = futures::future::ready(Ok(first.clone())).boxed().shared();
    tokio::time::timeout(Duration::from_secs(10), owner.shutdown())
        .await
        .expect("private owner shutdown must not wait for the joining chat");
    assert!(first.client.is_closed().await);

    release_second.notify_one();
    let second = second.await.unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 2);
    assert_eq!(*callbacks.lock().unwrap(), ["first", "second"]);
    second
        .call_tool(
            "test".to_string(),
            /*arguments*/ None,
            |_| Ok(None),
            /*timeout*/ None,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn connection_key_changes_with_the_owner_but_survives_credential_refresh() {
    let manager =
        AuthManager::from_auth_for_testing(CodexAuth::create_dummy_chatgpt_auth_for_testing());
    let mut original = key(&context(), &config());
    original.auth_state = Some((manager, AuthChangeState::default()));
    let mut refreshed = original.clone();
    refreshed.auth_state.as_mut().unwrap().1.generation += 1;
    assert!(original == refreshed);

    refreshed.auth_state.as_mut().unwrap().1.owner_generation += 1;
    assert!(original != refreshed);
}

#[tokio::test(start_paused = true)]
async fn new_chat_relists_tools_without_invalidating_or_waiting_for_existing_calls() {
    let pool = McpConnectionPool::default();
    let key = key(&context(), &config());
    let server = TestServer::default();
    let starts = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    release.notify_one();
    let launch = startup(
        ConnectionSharing::Negotiated,
        Arc::clone(&starts),
        Arc::default(),
        release,
        server.clone(),
    );
    let first = pool
        .connect(
            Arc::default(),
            key.clone(),
            Arc::default(),
            /*tool_catalog_cache_context*/ None,
            CancellationToken::new(),
            &launch,
        )
        .await
        .unwrap();
    let before = first.tool_catalog.read(|snapshot| snapshot).await;
    let release_call = Notify::new();
    let call = first
        .tool_catalog
        .run_with_snapshot(&before, || release_call.notified());
    tokio::pin!(call);
    assert!(futures::poll!(&mut call).is_pending());
    for tool_name in ["test", "new"] {
        *server.tools.lock().unwrap() = vec![create_test_tool("test_server", tool_name).tool];
        let second = tokio::time::timeout(
            Duration::from_secs(1),
            pool.connect(
                Arc::default(),
                key.clone(),
                Arc::default(),
                /*tool_catalog_cache_context*/ None,
                CancellationToken::new(),
                &launch,
            ),
        )
        .await
        .expect("new chats must not wait for existing calls")
        .unwrap();
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        assert_eq!(
            second
                .tool_catalog
                .read(|snapshot| snapshot.tools[0].tool.name.to_string())
                .await,
            tool_name
        );
        tokio::time::timeout(
            Duration::from_secs(1),
            second.tool_catalog.refresh(
                || async { Ok((vec![create_test_tool("test_server", "refreshed")], None, ())) },
                || Some(()),
                |_, _, ()| {},
            ),
        )
        .await
        .expect("one chat's refresh must not wait for another chat's calls")
        .unwrap();
        assert_eq!(
            first
                .tool_catalog
                .read(|snapshot| snapshot.tools[0].tool.name.to_string())
                .await,
            "test"
        );
    }
    release_call.notify_one();
    assert_eq!(call.await, Some(()));

    server.block_listing.store(true, Ordering::Release);
    let failed = pool
        .connect(
            Arc::default(),
            key,
            Arc::default(),
            /*tool_catalog_cache_context*/ None,
            CancellationToken::new(),
            &launch,
        )
        .await;
    assert!(failed.is_err());
    let _cancelled =
        tokio::time::timeout(Duration::from_secs(1), server.cancelled_listings.acquire())
            .await
            .expect("timed-out listings must be cancelled")
            .unwrap();
    first
        .call_tool(
            "test".to_string(),
            /*arguments*/ None,
            |_| Ok(None),
            /*timeout*/ None,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn negotiation_requires_the_stateless_protocol_and_detaches_startup_callbacks() {
    for (protocol_version, expected_sharing, expected_action) in [
        (
            ProtocolVersion::V_2025_06_18,
            false,
            ElicitationAction::Decline,
        ),
        (
            ProtocolVersion::V_2026_07_28,
            true,
            ElicitationAction::Cancel,
        ),
    ] {
        let (negotiation, callback) = SharingNegotiation::new(
            Box::new(|_, _| {
                futures::future::ready(Ok(ElicitationResponse {
                    action: ElicitationAction::Decline,
                    content: None,
                    meta: None,
                }))
                .boxed()
            }),
            CancellationToken::new(),
        );
        let sharing = negotiation.finish(&protocol_version);
        assert_eq!(
            matches!(sharing, ConnectionSharing::Negotiated),
            expected_sharing
        );
        let response = callback(
            rmcp::model::RequestId::Number(0),
            Elicitation::OpenAiForm {
                meta: None,
                message: "unexpected".to_string(),
                requested_schema: json!({}),
            },
        )
        .await
        .unwrap();
        assert_eq!(response.action, expected_action);
    }
}

#[tokio::test(start_paused = true)]
async fn late_release_keeps_its_original_join_after_chat_unload_and_observer_cancellation() {
    let mut peer = LeasePeer::new();
    let (first, first_id) = peer.chat().await;
    let (other, other_id) = peer.chat().await;
    let ConnectionSharing::Shared(lease) = &first.connection_sharing else {
        panic!("expected shared lease");
    };
    let slot = lease.release().expect("first release owns worker");
    slot.close_acquisition();
    let release = peer.next("codex/connection/release").await;
    assert_eq!(release.message["params"]["leaseId"], first_id);
    // Do not acknowledge release yet. Preserve upstream's prompt chat-unload
    // behavior while retaining the distinct worker join through the late tail.
    tokio::time::advance(LEASE_REQUEST_TIMEOUT).await;
    tokio::time::timeout(Duration::from_secs(1), slot.wait_for_shutdown())
        .await
        .expect("chat unload does not wait for a late release reply");
    assert!(
        lease.release().is_none(),
        "a repeated release cannot create another worker"
    );
    let mut observer = Box::pin(
        slot.release_workers
            .observe_until(tokio::time::Instant::now() + Duration::from_secs(10)),
    );
    assert!(futures::poll!(observer.as_mut()).is_pending());
    // Cancel only the observer, not the global/per-slot original-join custody.
    drop(observer);
    let pending = slot
        .release_workers
        .observe_until(tokio::time::Instant::now())
        .await;
    assert_eq!(pending.registered_at_start, 1);
    assert_eq!(pending.counts[1].pending, 1);
    assert_eq!(pending.counts[1].joined, 0);
    let logical = slot.retire_release_until(tokio::time::Instant::now()).await;
    assert!(
        !logical.is_released(),
        "prompt unload is not a server release reply"
    );
    assert_eq!(
        logical.outcomes,
        vec![(0, slot_retirement::SlotReleaseOutcome::ReleaseWorkerRunning)]
    );

    let (result, ()) = tokio::join!(
        other.call_tool("test".into(), None, |_| Ok(None), None),
        async {
            let request = peer.next("tools/call").await;
            assert_eq!(request.message["params"]["_meta"][LEASE_META_KEY], other_id);
            request.reply(json!({"content": []}));
        }
    );
    result.unwrap();
    assert!(!other.client.is_closed().await);
    release.reply(json!({}));
    let settled = slot
        .release_workers
        .observe_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(settled.counts[1].joined, 1);
    assert_eq!(settled.counts[1].pending, 0);
    assert_eq!(settled.counts[1].panicked, 0);
    assert_eq!(settled.counts[1].cancelled, 0);
    assert!(!settled.poisoned);
    let logical = slot.retire_release_until(tokio::time::Instant::now()).await;
    assert!(logical.is_released());
    assert_eq!(
        logical.outcomes,
        vec![(
            0,
            slot_retirement::SlotReleaseOutcome::ServerLeaseAcknowledged
        )]
    );
    assert!(
        logical.physical.is_empty(),
        "non-final release is not physical shutdown"
    );
    // Settled observation replays even when the next observer has no budget.
    assert_eq!(
        slot.release_workers
            .observe_until(tokio::time::Instant::now())
            .await,
        settled
    );
    let global = peer
        .pool
        .observe_workers_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(global.registered_at_start, 2);
    assert_eq!(global.registered_at_end, 2);
    assert_eq!(global.counts[0].joined, 1);
    assert_eq!(global.counts[1].joined, 1);
    assert!(
        !other.client.is_closed().await,
        "join observation does not shut down another holder"
    );
    let ConnectionSharing::Shared(last) = &other.connection_sharing else {
        panic!("expected final shared lease");
    };
    last.release()
        .expect("final owner release")
        .wait_for_shutdown()
        .await;
    let final_workers = peer
        .pool
        .observe_workers_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(final_workers.counts[1].joined, 2);
    assert_eq!(final_workers.counts[1].pending, 0);
    assert!(other.client.is_closed().await);
}

#[tokio::test(start_paused = true)]
async fn pool_keeps_late_release_join_when_the_chat_and_its_slot_are_dropped() {
    let mut peer = LeasePeer::new();
    let (first, _) = peer.chat().await;
    let (other, _) = peer.chat().await;
    let ConnectionSharing::Shared(first_lease) = &first.connection_sharing else {
        panic!("expected shared lease");
    };
    let weak_slot = Arc::downgrade(&first_lease.slot);
    drop(first);
    assert!(
        weak_slot.upgrade().is_none(),
        "the ordinary chat slot must be gone"
    );
    let release = peer.next("codex/connection/release").await;
    tokio::time::advance(LEASE_REQUEST_TIMEOUT).await;
    tokio::task::yield_now().await;
    let pending = peer
        .pool
        .observe_workers_until(tokio::time::Instant::now())
        .await;
    assert_eq!(pending.registered_at_start, 2);
    assert_eq!(pending.counts[1].pending, 1);
    release.reply(json!({}));
    let settled = peer
        .pool
        .observe_workers_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(settled.counts[0].joined, 1);
    assert_eq!(settled.counts[1].joined, 1);
    assert_eq!(settled.counts[1].pending, 0);
    assert!(!other.client.is_closed().await);
    let ConnectionSharing::Shared(last) = &other.connection_sharing else {
        panic!("expected final shared lease");
    };
    last.release()
        .expect("final owner release")
        .wait_for_shutdown()
        .await;
    let final_workers = peer
        .pool
        .observe_workers_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(final_workers.counts[1].joined, 2);
    assert_eq!(final_workers.counts[1].pending, 0);
    assert!(other.client.is_closed().await);
}

#[tokio::test]
async fn worker_join_failure_stays_visible_after_later_pool_work_is_compacted() {
    let pool = McpConnectionPool::default();
    drop(pool.workers.spawn(
        &tokio::runtime::Handle::current(),
        PoolWorkerKind::Release,
        async {
            panic!("injected pool release worker failure");
        },
    ));
    let failed = pool
        .observe_workers_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(failed.counts[1].joined, 0);
    assert_eq!(failed.counts[1].panicked, 1);
    drop(pool.workers.spawn(
        &tokio::runtime::Handle::current(),
        PoolWorkerKind::Release,
        async {},
    ));
    let later = pool
        .observe_workers_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(later.registered_at_start, 2);
    assert_eq!(later.counts[1].joined, 1);
    assert_eq!(later.counts[1].panicked, 1);
    assert_eq!(later.counts[1].pending, 0);
}

#[tokio::test]
async fn expired_worker_observer_does_not_poll_an_unobserved_join_or_lose_it() {
    let pool = McpConnectionPool::default();
    let finished_body = Arc::new(Notify::new());
    let signal = Arc::clone(&finished_body);
    drop(pool.workers.spawn(
        &tokio::runtime::Handle::current(),
        PoolWorkerKind::Release,
        async move {
            signal.notify_one();
        },
    ));
    tokio::time::timeout(Duration::from_secs(1), finished_body.notified())
        .await
        .expect("worker ran");
    let expired = pool
        .observe_workers_until(tokio::time::Instant::now())
        .await;
    assert_eq!(expired.counts[1].joined, 0);
    assert_eq!(expired.counts[1].pending, 1);
    let settled = pool
        .observe_workers_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(settled.counts[1].joined, 1);
    assert_eq!(settled.counts[1].pending, 0);
    assert_eq!(
        pool.observe_workers_until(tokio::time::Instant::now())
            .await,
        settled
    );
}

#[tokio::test(start_paused = true)]
async fn physical_pool_retirement_waits_for_the_final_ordinary_holder() {
    let mut peer = LeasePeer::new();
    let (first, _) = peer.chat().await;
    let (other, _) = peer.chat().await;
    let ConnectionSharing::Shared(first_lease) = &first.connection_sharing else {
        panic!("expected shared lease");
    };
    first_lease.release();
    let release = peer.next("codex/connection/release").await;
    let before = peer
        .pool
        .retire_released_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(before.registered_at_start, 1);
    assert_eq!(before.held_connections, vec![0]);
    assert!(before.connections.is_empty());
    assert!(before.foreign_connections.is_empty());
    assert!(!other.client.is_closed().await);
    let (result, ()) = tokio::join!(
        other.call_tool("test".into(), None, |_| Ok(None), None),
        async {
            peer.next("tools/call").await.reply(json!({"content": []}));
        }
    );
    result.unwrap();
    release.reply(json!({}));
    let ConnectionSharing::Shared(last) = &other.connection_sharing else {
        panic!("expected final shared lease");
    };
    last.release()
        .expect("final owner")
        .wait_for_shutdown()
        .await;
    let after = peer
        .pool
        .retire_released_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert!(after.held_connections.is_empty());
    assert!(after.foreign_connections.is_empty());
    assert!(!after.poisoned);
    assert_eq!(after.retired_before + after.connections.len() as u64, 1);
    assert!(
        after
            .connections
            .iter()
            .all(|(_, report)| !report.attempts.is_empty() && report.is_complete())
    );
    assert!(other.client.retirement().retirement_proof().is_proven());
    let workers = peer
        .pool
        .observe_workers_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(workers.counts[1].joined, 2);
    assert_eq!(workers.counts[1].pending, 0);
}

#[derive(Clone)]
struct DelayedPoolTransport {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    server: TestServer,
}

impl InProcessTransportFactory for DelayedPoolTransport {
    fn open(&self) -> BoxFuture<'static, std::io::Result<tokio::io::DuplexStream>> {
        let this = self.clone();
        async move {
            this.entered.notify_one();
            this.release.notified().await;
            this.server.open().await
        }
        .boxed()
    }
}

#[tokio::test(start_paused = true)]
async fn cancelled_pool_constructor_retains_its_late_transport_owner() {
    let pool = McpConnectionPool::default();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let ready = Arc::new(Notify::new());
    ready.notify_one();
    let launch = startup(
        ConnectionSharing::Negotiated,
        Arc::default(),
        Arc::default(),
        ready,
        DelayedPoolTransport {
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
            server: TestServer::default(),
        },
    );
    let mut connecting = Box::pin(pool.connect(
        Arc::default(),
        key(&context(), &config()),
        Arc::default(),
        None,
        CancellationToken::new(),
        launch,
    ));
    tokio::select! {
        _ = entered.notified() => {}
        _ = &mut connecting => panic!("constructor must wait for the transport"),
    }
    drop(connecting);
    let pending = pool
        .retire_released_until(tokio::time::Instant::now())
        .await;
    assert!(pending.held_connections.is_empty());
    assert_eq!(pending.connections.len(), 1);
    assert!(!pending.connections[0].1.is_complete());
    release.notify_one();
    let settled = pool
        .retire_released_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert!(settled.held_connections.is_empty());
    assert!(settled.foreign_connections.is_empty());
    assert_eq!(settled.retired_before + settled.connections.len() as u64, 1);
    assert!(
        settled
            .connections
            .iter()
            .all(|(_, report)| !report.attempts.is_empty() && report.is_complete()),
        "late transport retirement: {settled:?}"
    );
    let workers = pool
        .observe_workers_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(workers.counts[0].joined, 1);
    assert_eq!(workers.counts[1].joined, 1);
    assert_eq!(workers.counts[0].pending + workers.counts[1].pending, 0);
    let compacted = pool
        .retire_released_until(tokio::time::Instant::now())
        .await;
    assert_eq!(compacted.retired_before, 1);
    assert!(compacted.connections.is_empty());
    assert!(compacted.constructing_connections.is_empty());
}

#[tokio::test]
async fn pool_refuses_foreign_retirement_owner_without_closing_that_clients_transport() {
    let mut source_peer = LeasePeer::new();
    let (foreign, _) = source_peer.chat().await;
    let supplied = foreign.clone();
    let bad_pool = McpConnectionPool::default();
    let result = bad_pool
        .connect(
            Arc::default(),
            key(&context(), &config()),
            Arc::default(),
            None,
            CancellationToken::new(),
            move |_, _, _| {
                futures::future::ready(Ok(supplied.clone()))
                    .boxed()
                    .shared()
            },
        )
        .await;
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("foreign owner must refuse publication"),
    };
    assert!(error.to_string().contains("retained retirement owner"));
    let report = bad_pool
        .retire_released_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(report.foreign_connections, vec![0]);
    assert!(report.held_connections.is_empty());
    assert_eq!(report.retired_before, 0);
    let workers = bad_pool
        .observe_workers_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(workers.counts[1].joined, 1);
    assert!(!foreign.client.is_closed().await);
    let (result, ()) = tokio::join!(
        foreign.call_tool("test".into(), None, |_| Ok(None), None),
        async {
            source_peer
                .next("tools/call")
                .await
                .reply(json!({"content": []}));
        }
    );
    result.unwrap();
    let ConnectionSharing::Shared(last) = &foreign.connection_sharing else {
        panic!("expected foreign holder still intact");
    };
    last.release()
        .expect("fixture owner releases its own connection")
        .wait_for_shutdown()
        .await;
    let _ = source_peer
        .pool
        .observe_workers_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert!(foreign.client.is_closed().await);
}

#[derive(Clone)]
struct ObservedPoolTransport(Arc<AtomicBool>);

impl InProcessTransportFactory for ObservedPoolTransport {
    fn open(&self) -> BoxFuture<'static, std::io::Result<tokio::io::DuplexStream>> {
        self.0.store(true, Ordering::Release);
        TestServer::default().open()
    }
}

#[tokio::test(start_paused = true)]
async fn released_pool_owner_is_retained_until_its_factory_settles() {
    let pool = McpConnectionPool::default();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let opened = Arc::new(AtomicBool::new(false));
    let launch = {
        let entered = Arc::clone(&entered);
        let release = Arc::clone(&release);
        let opened = Arc::clone(&opened);
        move |_, _, retirement: Option<RmcpClientRetirement>| {
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            let opened = Arc::clone(&opened);
            async move {
                entered.notify_one();
                release.notified().await;
                let result = RmcpClient::new_in_process_client_in_retirement(
                    Arc::new(ObservedPoolTransport(Arc::clone(&opened))),
                    retirement.expect("pooled owner supplied before factory"),
                )
                .await;
                match result {
                    Err(error) => assert!(error.to_string().contains("registration is closed")),
                    Ok(client) => {
                        client.shutdown().await;
                        panic!("closed owner must fence a late constructor");
                    }
                }
                assert!(!opened.load(Ordering::Acquire));
                Err(StartupOutcomeError::Cancelled)
            }
            .boxed()
            .shared()
        }
    };
    let mut connecting = Box::pin(pool.connect(
        Arc::default(),
        key(&context(), &config()),
        Arc::default(),
        None,
        CancellationToken::new(),
        launch,
    ));
    tokio::select! {
        _ = entered.notified() => {}
        _ = &mut connecting => panic!("factory must wait before construction"),
    }
    drop(connecting);
    let before = pool
        .retire_released_until(tokio::time::Instant::now())
        .await;
    assert_eq!(before.retired_before, 0);
    assert_eq!(before.constructing_connections, vec![0]);
    assert_eq!(before.connections.len(), 1);
    assert!(before.connections[0].1.is_complete());
    assert!(before.connections[0].1.attempts.is_empty());
    let repeated = pool
        .retire_released_until(tokio::time::Instant::now())
        .await;
    assert_eq!(
        repeated.retired_before, 0,
        "empty lower proof must not compact a running factory"
    );
    assert_eq!(repeated.constructing_connections, vec![0]);
    release.notify_one();
    let workers = pool
        .observe_workers_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(workers.counts[0].joined, 1);
    assert_eq!(workers.counts[1].joined, 1);
    assert!(!opened.load(Ordering::Acquire));
    let after = pool
        .retire_released_until(tokio::time::Instant::now())
        .await;
    assert_eq!(after.retired_before, 1);
    assert!(after.connections.is_empty());
    assert!(after.constructing_connections.is_empty());
    assert!(after.foreign_connections.is_empty());
}

async fn private_event_stream(
    pool: &McpConnectionPool,
) -> (
    crate::McpEventStream,
    mpsc::UnboundedReceiver<LeaseFrame>,
    Arc<RmcpClient>,
) {
    let (tx, frames) = mpsc::unbounded_channel();
    let lease = pool.reserve_event_connection();
    let ready = Arc::<Notify>::default();
    ready.notify_one();
    let launch = startup(
        ConnectionSharing::Private,
        Arc::default(),
        Arc::default(),
        ready,
        LeaseTransport(tx, false),
    );
    let managed = launch(CancellationToken::new(), None, Some(lease.retirement()))
        .await
        .unwrap();
    assert!(lease.bind(&managed.client));
    let client = Arc::clone(&managed.client);
    let stream = crate::McpEventStream::open(
        Arc::clone(&client),
        crate::resource_client::EventStreamCustody::Private(lease),
        pool.clone(),
        tokio::sync::watch::channel(()).1,
        "test",
        &json!({}),
        None,
    )
    .await
    .unwrap();
    (stream, frames, client)
}

async fn event_frame(frames: &mut mpsc::UnboundedReceiver<LeaseFrame>, method: &str) -> LeaseFrame {
    let frame = tokio::time::timeout(Duration::from_secs(1), frames.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frame.message["method"], method);
    frame
}

async fn assert_event_pool_retired(pool: &McpConnectionPool) {
    let workers = pool
        .observe_workers_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert!(workers.counts[1].joined > 0);
    assert_eq!(workers.counts[1].pending, 0);
    assert_eq!(workers.counts[1].panicked + workers.counts[1].cancelled, 0);
    let report = pool
        .retire_released_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert!(report.held_connections.is_empty());
    assert!(report.foreign_connections.is_empty());
    assert!(report.constructing_connections.is_empty());
    assert_eq!(report.retired_before + report.connections.len() as u64, 1);
    assert!(
        report
            .connections
            .iter()
            .all(|(_, physical)| physical.is_complete())
    );
}

#[tokio::test]
async fn borrowed_event_stream_keeps_ordinary_lease_until_cancellation_finishes() {
    let mut peer = LeasePeer::new();
    let (other, _) = peer.chat().await;
    let (managed, id) = peer.chat().await;
    let stream = crate::McpEventStream::open(
        Arc::clone(&managed.client),
        crate::resource_client::EventStreamCustody::Borrowed(managed),
        peer.pool.clone(),
        tokio::sync::watch::channel(()).1,
        "test",
        &json!({}),
        None,
    )
    .await
    .unwrap();
    let request = peer.next("events/stream").await;
    assert!(
        tokio::time::timeout(Duration::from_millis(20), peer.frames.recv())
            .await
            .is_err(),
        "opening a stream must not drop its ordinary lease"
    );
    drop(stream);
    let cancel = peer.next("notifications/cancelled").await;
    assert_eq!(cancel.message["params"]["requestId"], request.message["id"]);
    request.reply(json!({}));
    let release = peer.next("codex/connection/release").await;
    assert_eq!(release.message["params"]["leaseId"], id);
    release.reply(json!({}));
    let (result, ()) = tokio::join!(
        other.call_tool("test".into(), None, |_| Ok(None), None),
        async {
            peer.next("tools/call").await.reply(json!({"content": []}));
        }
    );
    result.unwrap();
    drop(other);
    assert_event_pool_retired(&peer.pool).await;
}

#[tokio::test]
async fn borrowed_event_completion_releases_only_its_holder() {
    let mut peer = LeasePeer::new();
    let (other, _) = peer.chat().await;
    let (managed, id) = peer.chat().await;
    let mut stream = crate::McpEventStream::open(
        Arc::clone(&managed.client),
        crate::resource_client::EventStreamCustody::Borrowed(managed),
        peer.pool.clone(),
        tokio::sync::watch::channel(()).1,
        "test",
        &json!({}),
        None,
    )
    .await
    .unwrap();
    peer.next("events/stream").await.reply(json!({}));
    assert!(stream.recv().await.unwrap().is_none());
    let release = peer.next("codex/connection/release").await;
    assert_eq!(release.message["params"]["leaseId"], id);
    release.reply(json!({}));
    assert!(!other.client.is_closed().await);
    drop(stream);
    drop(other);
    assert_event_pool_retired(&peer.pool).await;
}

#[tokio::test]
async fn independent_event_stream_keeps_its_transport_until_drop() {
    let pool = McpConnectionPool::default();
    let (stream, mut frames, client) = private_event_stream(&pool).await;
    let request = event_frame(&mut frames, "events/stream").await;
    let held = pool
        .retire_released_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(held.held_connections, vec![0]);
    assert!(held.connections.is_empty());
    assert!(!client.is_closed().await);
    drop(stream);
    let cancel = event_frame(&mut frames, "notifications/cancelled").await;
    assert_eq!(cancel.message["params"]["requestId"], request.message["id"]);
    assert_event_pool_retired(&pool).await;
    assert!(client.is_closed().await);
}

#[tokio::test]
async fn independent_event_completion_is_retained_and_physically_closed() {
    let pool = McpConnectionPool::default();
    let (mut stream, mut frames, client) = private_event_stream(&pool).await;
    event_frame(&mut frames, "events/stream")
        .await
        .reply(json!({}));
    assert!(stream.recv().await.unwrap().is_none());
    assert_event_pool_retired(&pool).await;
    assert!(client.is_closed().await);
    drop(stream);
}

#[tokio::test]
async fn event_stream_drop_outside_runtime_keeps_original_cleanup_join() {
    let pool = McpConnectionPool::default();
    let (stream, mut frames, client) = private_event_stream(&pool).await;
    let _request = event_frame(&mut frames, "events/stream").await;
    std::thread::spawn(move || drop(stream)).join().unwrap();
    event_frame(&mut frames, "notifications/cancelled").await;
    assert_event_pool_retired(&pool).await;
    assert!(client.is_closed().await);
}

#[tokio::test]
async fn event_release_original_join_survives_observer_cancellation() {
    let pool = McpConnectionPool::default();
    let lease = pool.reserve_event_connection();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let seen = Arc::clone(&entered);
    let resume = Arc::clone(&release);
    lease.release_after(async move {
        seen.notify_one();
        resume.notified().await;
    });
    entered.notified().await;
    let observer_pool = pool.clone();
    let observer = tokio::spawn(async move {
        observer_pool
            .observe_workers_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
    });
    observer.abort();
    assert!(observer.await.unwrap_err().is_cancelled());
    let pending = pool
        .observe_workers_until(tokio::time::Instant::now())
        .await;
    assert_eq!(pending.counts[1].pending, 1);
    release.notify_one();
    assert_event_pool_retired(&pool).await;
}

#[tokio::test]
async fn event_constructor_cancellation_keeps_late_physical_custody() {
    let pool = McpConnectionPool::default();
    let lease = pool.reserve_event_connection();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let factory = Arc::new(DelayedPoolTransport {
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
        server: TestServer::default(),
    });
    let mut constructing = Box::pin(RmcpClient::new_in_process_client_in_retirement(
        factory,
        lease.retirement(),
    ));
    tokio::select! { _ = entered.notified() => {}, _ = &mut constructing => panic!("factory must remain gated") }
    drop(lease);
    drop(constructing);
    let pending = pool
        .retire_released_until(tokio::time::Instant::now())
        .await;
    assert_eq!(pending.retired_before, 0);
    assert_eq!(pending.connections.len(), 1);
    assert!(!pending.connections[0].1.is_complete());
    release.notify_one();
    assert_event_pool_retired(&pool).await;
}

#[tokio::test]
async fn event_binding_refuses_foreign_transport_without_closing_it() {
    let mut peer = LeasePeer::new();
    let (foreign, _) = peer.chat().await;
    let pool = McpConnectionPool::default();
    let lease = pool.reserve_event_connection();
    assert!(!lease.bind(&foreign.client));
    drop(lease);
    let report = pool
        .retire_released_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(report.foreign_connections, vec![0]);
    assert_eq!(report.retired_before, 0);
    let (result, ()) = tokio::join!(
        foreign.call_tool("test".into(), None, |_| Ok(None), None),
        async {
            peer.next("tools/call").await.reply(json!({"content": []}));
        }
    );
    result.unwrap();
    drop(foreign);
    assert_event_pool_retired(&peer.pool).await;
}

#[tokio::test]
async fn closed_slot_gate_is_not_release_and_does_not_evict_another_chat() {
    let mut peer = LeasePeer::new();
    let (first, _) = peer.chat().await;
    let ConnectionSharing::Shared(lease) = &first.connection_sharing else {
        panic!("shared lease");
    };
    let slot = Arc::clone(&lease.slot);
    slot.close_acquisition();
    let held = slot.retire_release_until(tokio::time::Instant::now()).await;
    assert!(!held.is_released());
    assert_eq!(
        held.outcomes,
        vec![(
            0,
            slot_retirement::SlotReleaseOutcome::AwaitingOrdinaryRelease
        )]
    );
    let (other, _) = peer.chat().await;
    let population = peer
        .pool
        .retire_released_until(tokio::time::Instant::now())
        .await;
    assert_eq!(
        population.registered_at_start, 1,
        "closing a slot must keep the shared process eligible for reuse"
    );
    lease.release().unwrap();
    peer.next("codex/connection/release").await.reply(json!({}));
    let released = slot
        .retire_release_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert!(released.is_released(), "{released:?}");
    assert!(released.physical.is_empty());
    let (result, ()) = tokio::join!(
        other.call_tool("test".into(), None, |_| Ok(None), None),
        async {
            peer.next("tools/call").await.reply(json!({"content": []}));
        }
    );
    result.unwrap();
    drop(other);
    assert_event_pool_retired(&peer.pool).await;
}

#[tokio::test]
async fn joined_release_worker_with_server_error_is_not_positive_release() {
    let mut peer = LeasePeer::new();
    let (first, _) = peer.chat().await;
    let (other, _) = peer.chat().await;
    let ConnectionSharing::Shared(lease) = &first.connection_sharing else {
        panic!("shared lease");
    };
    let slot = Arc::clone(&lease.slot);
    slot.close_acquisition();
    lease.release().unwrap();
    let release = peer.next("codex/connection/release").await;
    release
        .replies
        .send(json!({"jsonrpc": "2.0", "id": release.message["id"],
        "error": {"code": -32603, "message": "injected lease release refusal"}}))
        .unwrap();
    let report = slot
        .retire_release_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(report.workers.counts[1].joined, 1);
    assert!(!report.is_released());
    assert!(
        matches!(&report.outcomes[..], [(0, slot_retirement::SlotReleaseOutcome::ServerReleaseFailed(error))] if error.contains("injected lease release refusal"))
    );
    let (result, ()) = tokio::join!(
        other.call_tool("test".into(), None, |_| Ok(None), None),
        async {
            peer.next("tools/call").await.reply(json!({"content": []}));
        }
    );
    result.unwrap();
    drop(other);
    assert_event_pool_retired(&peer.pool).await;
    let recovered = slot
        .retire_release_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(
        recovered.outcomes, report.outcomes,
        "the server refusal stays visible"
    );
    assert!(
        recovered.is_released(),
        "positive final physical proof may discharge the failed logical release: {recovered:?}"
    );
    assert_eq!(recovered.physical.len(), 1);
    assert!(!recovered.physical[0].1.attempts.is_empty());
}

#[tokio::test]
async fn final_slot_release_requires_actual_physical_proof_and_replays_it() {
    let mut peer = LeasePeer::new();
    let (managed, _) = peer.chat().await;
    let ConnectionSharing::Shared(lease) = &managed.connection_sharing else {
        panic!("shared lease");
    };
    let slot = Arc::clone(&lease.slot);
    slot.close_acquisition();
    lease.release().unwrap();
    let report = slot
        .retire_release_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(
        report.outcomes,
        vec![(
            0,
            slot_retirement::SlotReleaseOutcome::RequiresPhysicalProof
        )]
    );
    assert_eq!(report.physical.len(), 1);
    assert!(!report.physical[0].1.attempts.is_empty());
    assert!(report.is_released(), "{report:?}");
    assert!(managed.client.is_closed().await);
    assert!(lease.release().is_none());
    assert_eq!(
        slot.retire_release_until(tokio::time::Instant::now()).await,
        report
    );
}

#[tokio::test]
async fn nonfinal_connection_without_remote_lease_has_explicit_logical_receipt() {
    let pool = McpConnectionPool::default();
    let ready = Arc::<Notify>::default();
    ready.notify_one();
    let launch = startup(
        ConnectionSharing::Negotiated,
        Arc::default(),
        Arc::default(),
        ready,
        TestServer::default(),
    );
    let first_slot = Arc::new(ConnectionSlot::default());
    let shared_key = key(&context(), &config());
    let first = pool
        .connect(
            Arc::clone(&first_slot),
            shared_key.clone(),
            Arc::default(),
            None,
            CancellationToken::new(),
            &launch,
        )
        .await
        .unwrap();
    let other = pool
        .connect(
            Arc::default(),
            shared_key,
            Arc::default(),
            None,
            CancellationToken::new(),
            &launch,
        )
        .await
        .unwrap();
    assert!(
        Arc::ptr_eq(&first.client, &other.client),
        "fixture must hold the same actual transport twice"
    );
    let ConnectionSharing::Shared(lease) = &first.connection_sharing else {
        panic!("shared lease");
    };
    assert!(lease.server_lease.get().is_none());
    first_slot.close_acquisition();
    assert!(
        lease.release().is_none(),
        "no remote protocol worker exists for this non-final holder"
    );
    let report = first_slot
        .retire_release_until(tokio::time::Instant::now())
        .await;
    assert_eq!(
        report.outcomes,
        vec![(
            0,
            slot_retirement::SlotReleaseOutcome::ReleasedWithoutServerLease
        )]
    );
    assert_eq!(report.workers.registered_at_end, 0);
    assert!(report.physical.is_empty());
    assert!(report.is_released());
    other
        .call_tool("test".into(), None, |_| Ok(None), None)
        .await
        .unwrap();
    drop(other);
    assert_event_pool_retired(&pool).await;
}

#[tokio::test]
async fn closed_slot_refuses_late_factory_before_allocating_a_release_record() {
    let pool = McpConnectionPool::default();
    let slot = Arc::new(ConnectionSlot::default());
    slot.close_acquisition();
    let result = pool
        .connect(
            Arc::clone(&slot),
            key(&context(), &config()),
            Arc::default(),
            None,
            CancellationToken::new(),
            |_, _, _| panic!("closed slot must refuse before factory invocation"),
        )
        .await;
    assert!(matches!(result, Err(StartupOutcomeError::Cancelled)));
    let report = slot.retire_release_until(tokio::time::Instant::now()).await;
    assert_eq!(report.registered_at_start + report.registered_at_end, 0);
    assert!(report.outcomes.is_empty());
    assert!(report.is_released());
    assert_eq!(
        pool.retire_released_until(tokio::time::Instant::now())
            .await
            .registered_at_end,
        0
    );
}

#[tokio::test]
async fn external_runtime_connection_owner_releases_only_its_exact_chat_lease() {
    let mut peer = LeasePeer::new();
    let (first, _) = peer.chat().await;
    let (other, _) = peer.chat().await;
    let ConnectionSharing::Shared(lease) = &first.connection_sharing else {
        panic!("shared lease");
    };
    let root = crate::McpRuntimeRetirement::default();
    root.register_connection(
        Default::default(),
        Some(Arc::clone(&lease.slot)),
        CancellationToken::new(),
    )
    .unwrap();
    root.close_registration();
    peer.next("codex/connection/release").await.reply(json!({}));
    let report = root
        .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert!(report.are_connections_released(), "{report:?}");
    assert!(
        report.connections[0]
            .shared
            .as_ref()
            .unwrap()
            .physical
            .is_empty()
    );
    let (result, ()) = tokio::join!(
        other.call_tool("test".into(), None, |_| Ok(None), None),
        async {
            peer.next("tools/call").await.reply(json!({"content": []}));
        }
    );
    result.unwrap();
    drop(other);
    assert_event_pool_retired(&peer.pool).await;
    assert!(
        first.client.is_closed().await,
        "the retained census must not prevent actual final-owner shutdown"
    );
}

#[tokio::test]
async fn retained_runtime_census_does_not_keep_an_ordinary_chat_lease_alive() {
    let mut peer = LeasePeer::new();
    let (first, _) = peer.chat().await;
    let (other, _) = peer.chat().await;
    let ConnectionSharing::Shared(lease) = &first.connection_sharing else {
        panic!("shared lease");
    };
    let weak = Arc::downgrade(lease);
    let root = crate::McpRuntimeRetirement::default();
    root.register_connection(
        Default::default(),
        Some(Arc::clone(&lease.slot)),
        CancellationToken::new(),
    )
    .unwrap();
    drop(first);
    assert!(
        weak.upgrade().is_none(),
        "the external census stores only weak release authority"
    );
    peer.next("codex/connection/release").await.reply(json!({}));
    let report = root
        .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert!(report.are_connections_released());
    assert!(!other.client.is_closed().await);
    drop(other);
    assert_event_pool_retired(&peer.pool).await;
}

#[tokio::test]
async fn pooled_auth_watcher_survives_creator_release_and_stops_on_final_transport_release() {
    let mut peer = LeasePeer::new();
    peer.transport.1 = true;
    let session_context = context();
    let session_workers = session_context.workers.clone();
    let process_workers = session_context
        .clone()
        .for_pooled_connection(&peer.pool)
        .workers;
    let (changes, receiver) = tokio::sync::watch::channel(AuthChangeState::default());
    let pool = peer.pool.clone();
    let key = peer.key.clone();
    let transport = peer.transport.clone();
    let attached = tokio::spawn(async move {
        let ready = Arc::<Notify>::default();
        ready.notify_one();
        let launch = startup_with_auth(
            ConnectionSharing::Negotiated,
            Arc::default(),
            Arc::default(),
            ready,
            transport,
            Some((process_workers, receiver)),
        );
        pool.connect(
            Arc::default(),
            key,
            Arc::new(Mutex::new(Some(
                json!({"extensions": {LEASE_CAPABILITY: {}}}),
            ))),
            None,
            CancellationToken::new(),
            launch,
        )
        .await
    });
    let initial = peer.next("notifications/codex/authChanged").await;
    assert_eq!(
        initial.message["params"],
        json!({"generation": 0, "ownerGeneration": 0})
    );
    peer.next("codex/connection/open").await.reply(json!({}));
    let first = attached.await.unwrap().unwrap();
    assert!(
        first._auth_change_notifications.is_some(),
        "actual opt-in initialization must create the watcher"
    );
    let (other, _) = peer.chat().await;
    assert!(Arc::ptr_eq(&first.client, &other.client));
    let ConnectionSharing::Shared(lease) = &first.connection_sharing else {
        panic!("shared lease");
    };
    let root = session_context.retirement.as_ref().unwrap();
    root.register_connection(
        Default::default(),
        Some(Arc::clone(&lease.slot)),
        CancellationToken::new(),
    )
    .unwrap();
    root.close_registration();
    peer.next("codex/connection/release").await.reply(json!({}));
    assert!(
        root.shutdown_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .are_connections_released()
    );
    assert_eq!(
        session_workers
            .observe_until(tokio::time::Instant::now())
            .await
            .registered_at_end,
        0
    );
    let shared = peer
        .pool
        .observe_background_workers_until(tokio::time::Instant::now())
        .await;
    let watcher = shared
        .counts
        .iter()
        .find(|c| c.kind == crate::McpWorkerKind::AuthNotification)
        .unwrap();
    assert_eq!(watcher.pending, 1);
    assert_eq!(watcher.cancelled + watcher.panicked + watcher.joined, 0);
    changes.send_modify(|state| state.generation += 1);
    let update = peer.next("notifications/codex/authChanged").await;
    assert_eq!(
        update.message["params"],
        json!({"generation": 1, "ownerGeneration": 0})
    );
    let (result, ()) = tokio::join!(
        other.call_tool("test".into(), None, |_| Ok(None), None),
        async {
            peer.next("tools/call").await.reply(json!({"content": []}));
        }
    );
    result.unwrap();
    let ConnectionSharing::Shared(last) = &other.connection_sharing else {
        panic!("shared lease");
    };
    last.release().unwrap().wait_for_shutdown().await;
    tokio::time::timeout(Duration::from_secs(1), changes.closed())
        .await
        .unwrap();
    let finished = peer
        .pool
        .observe_background_workers_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    let watcher = finished
        .counts
        .iter()
        .find(|c| c.kind == crate::McpWorkerKind::AuthNotification)
        .unwrap();
    assert_eq!(watcher.cancelled, 1);
    assert_eq!(watcher.pending + watcher.panicked + watcher.joined, 0);
    assert!(
        first._auth_change_notifications.is_some(),
        "retained old bindings must not keep a stopped watcher alive"
    );
    assert!(other.client.is_closed().await);
    assert_event_pool_retired(&peer.pool).await;
}

#[tokio::test]
async fn pooled_worker_context_preserves_connection_identity_and_external_process_custody() {
    let pool = McpConnectionPool::default();
    let session = context();
    let session_owner = session.workers.clone();
    let identity = key(&session, &config());
    let pooled = session.clone().for_pooled_connection(&pool);
    assert!(identity == key(&pooled, &config()));
    let release = Arc::<Notify>::default();
    let resume = Arc::clone(&release);
    drop(pooled.workers.spawn(
        &tokio::runtime::Handle::current(),
        crate::McpWorkerKind::DiscoveryCleanup,
        async move {
            resume.notified().await;
        },
    ));
    drop(pooled);
    drop(session);
    assert_eq!(
        session_owner
            .observe_until(tokio::time::Instant::now())
            .await
            .registered_at_end,
        0
    );
    let pending = pool
        .observe_background_workers_until(tokio::time::Instant::now())
        .await;
    assert_eq!(pending.registered_at_end, 1);
    assert_eq!(
        pending
            .counts
            .iter()
            .find(|c| c.kind == crate::McpWorkerKind::DiscoveryCleanup)
            .unwrap()
            .pending,
        1
    );
    release.notify_one();
    let joined = pool
        .observe_background_workers_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(
        joined
            .counts
            .iter()
            .find(|c| c.kind == crate::McpWorkerKind::DiscoveryCleanup)
            .unwrap()
            .joined,
        1
    );
}
