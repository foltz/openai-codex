//! RMCP client lifecycle for MCP server connections.
//!
//! This module owns startup of individual RMCP clients: building the transport,
//! initializing the server, listing raw tools, applying per-server tool filters,
//! and exposing cached Codex Apps tools while a client is still connecting.
//! Higher-level aggregation and resource/tool APIs live in
//! [`crate::connection_manager`].

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::env;
use std::ffi::OsString;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use crate::codex_apps::normalize_codex_apps_callable_name;
use crate::codex_apps::normalize_codex_apps_callable_namespace;
use crate::codex_apps::normalize_codex_apps_tool_title;
use crate::codex_apps::prepare_openai_file_params_for_model;
use crate::elicitation::ElicitationRequestManager;
use crate::mcp::CODEX_APPS_MCP_SERVER_NAME;
use crate::mcp::ToolPluginProvenance;
use crate::openai_docs_source_attribution::maybe_with_openai_docs_source_attribution;
use crate::pagination::collect_paginated_with_limit;
use crate::runtime::McpRuntimeContext;
use crate::runtime::emit_duration;
use crate::server::EffectiveMcpServer;
use crate::server::has_explicit_http_authorization;
use crate::tool_catalog_cache::McpToolCatalogCacheContext;
use crate::tool_catalog_cache::McpToolCatalogFetchTicket;
use crate::tools::ToolInfo;
use anyhow::Result;
use anyhow::anyhow;
use async_channel::Sender;
use codex_api::SharedAuthProvider;
use codex_async_utils::CancelErr;
use codex_async_utils::OrCancelExt;
use codex_config::McpServerAuth;
use codex_config::McpServerConfig;
use codex_config::McpServerTransportConfig;
use codex_config::types::AuthKeyringBackendKind;
use codex_config::types::OAuthCredentialsStoreMode;
use codex_connectors::ConnectorRuntimeContext;
use codex_connectors::ConnectorRuntimeFetchSource;
use codex_exec_server::Environment;
use codex_protocol::mcp::ClientMcpExtensions;
use codex_protocol::mcp::McpServerInfo;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::McpStartupStatus;
use codex_protocol::protocol::McpStartupUpdateEvent;
use codex_rmcp_client::ExecutorStdioServerLauncher;
use codex_rmcp_client::LocalStdioServerLauncher;
use codex_rmcp_client::McpProtocolMode;
use codex_rmcp_client::PostReconnectHook;
use codex_rmcp_client::RmcpClient;
use codex_rmcp_client::StdioServerLauncher;
use codex_rmcp_client::StreamableHttpRedirectMode;
use codex_rmcp_client::ToolWithConnectorId;
use codex_rmcp_client::is_authentication_required_error;
use futures::future::BoxFuture;
use futures::future::FutureExt;
use futures::future::Shared;
use rmcp::model::ClientCapabilities;
use rmcp::model::CustomResult;
use rmcp::model::ElicitationCapability;
use rmcp::model::Implementation;
use rmcp::model::InitializeRequestParams;
use rmcp::model::ProtocolVersion;
use rmcp::model::ServerPeerInfo;
use rmcp::model::ServerResult;
use rmcp::model::Tool as RmcpTool;
use tokio::time::Instant as TokioInstant;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing::instrument;
use tracing::warn;

/// MCP server capability indicating that Codex should include [`SandboxState`]
/// in tool-call request `_meta` under this key.
pub const MCP_SANDBOX_STATE_META_CAPABILITY: &str = "codex/sandbox-state-meta";
/// Experimental MCP server capability for development and testing only; production servers should
/// not use it. Its `cacheable: false` property disables sharing tool definitions across connections.
const MCP_TOOL_CATALOG_CACHE_CAPABILITY: &str = "codex/tool-catalog-cache";
const MCP_TOOL_CATALOG_CACHEABLE_PROPERTY: &str = "cacheable";
/// Capability-gated thread-identity bind method and experimental-capability
/// key (the same string serves both roles); see
/// `kcf-runtime/04-mcp-thread-identity-contract.md`.
const MCP_THREAD_IDENTITY_CAPABILITY: &str = "codex/thread-identity";
const MCP_THREAD_IDENTITY_VERSIONS_PROPERTY: &str = "versions";
const MCP_THREAD_IDENTITY_SUPPORTED_VERSION: i64 = 1;
pub(crate) const MCP_TOOLS_LIST_DURATION_METRIC: &str = "codex.mcp.tools.list.duration_ms";
pub(crate) const MCP_TOOLS_FETCH_UNCACHED_DURATION_METRIC: &str =
    "codex.mcp.tools.fetch_uncached.duration_ms";
pub(crate) const CODEX_APPS_REFRESH_DURATION_METRIC: &str = "codex.apps.refresh.duration_ms";
pub(crate) const DEFAULT_STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) const DEFAULT_TOOL_TIMEOUT: Duration = Duration::from_secs(300);

pub(crate) const CODEX_APPS_RECONNECT_INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const CODEX_APPS_RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(30);

const UNTRUSTED_CONNECTOR_META_KEYS: &[&str] = &[
    "connector_id",
    "connector_name",
    "connector_display_name",
    "connector_description",
    "connectorDescription",
];

#[derive(Clone)]
pub(crate) struct ManagedClient {
    pub(crate) client: Arc<RmcpClient>,
    pub(crate) server_info: McpServerInfo,
    pub(crate) tools: Vec<ToolInfo>,
    pub(crate) tool_timeout: Option<Duration>,
    pub(crate) server_instructions: Option<String>,
    pub(crate) server_supports_sandbox_state_meta_capability: bool,
    pub(crate) codex_apps_tools_cache_context: Option<ConnectorRuntimeContext<ToolInfo>>,
}

impl ManagedClient {
    pub(crate) fn listed_tools(&self) -> Vec<ToolInfo> {
        let total_start = Instant::now();
        if let Some(tools) = self
            .codex_apps_tools_cache_context
            .as_ref()
            .and_then(ConnectorRuntimeContext::current_tools)
        {
            emit_duration(
                MCP_TOOLS_LIST_DURATION_METRIC,
                total_start.elapsed(),
                &[("cache", "hit")],
            );
            return tools;
        }

        if self.codex_apps_tools_cache_context.is_some() {
            emit_duration(
                MCP_TOOLS_LIST_DURATION_METRIC,
                total_start.elapsed(),
                &[("cache", "miss")],
            );
        }

        self.tools.clone()
    }
}

pub(crate) type ManagedClientFuture =
    Shared<BoxFuture<'static, Result<ManagedClient, StartupOutcomeError>>>;

#[derive(Default)]
struct CodexAppsStartupReconnectState {
    current_client: Option<ManagedClient>,
    reconnect_in_flight: bool,
    consecutive_failures: u32,
    retry_not_before: Option<TokioInstant>,
}

#[derive(Clone)]
struct CodexAppsStartupStatusContext {
    submit_id: String,
    server_name: String,
    tx_event: Sender<Event>,
}

pub(crate) struct CodexAppsStartupReconnect {
    factory: Arc<dyn Fn() -> ManagedClientFuture + Send + Sync>,
    state: StdMutex<CodexAppsStartupReconnectState>,
    startup_status_context: Option<CodexAppsStartupStatusContext>,
}

impl CodexAppsStartupReconnect {
    pub(crate) fn new(factory: Arc<dyn Fn() -> ManagedClientFuture + Send + Sync>) -> Self {
        Self {
            factory,
            state: StdMutex::new(CodexAppsStartupReconnectState::default()),
            startup_status_context: None,
        }
    }

    fn with_startup_status_context(
        mut self,
        submit_id: String,
        server_name: String,
        tx_event: Option<Sender<Event>>,
    ) -> Self {
        self.startup_status_context = tx_event.map(|tx_event| CodexAppsStartupStatusContext {
            submit_id,
            server_name,
            tx_event,
        });
        self
    }

    fn current_client(&self) -> Option<ManagedClient> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .current_client
            .clone()
    }

    fn reconnect_in_background(self: &Arc<Self>) {
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.current_client.is_some() || state.reconnect_in_flight {
                return;
            }
            if state
                .retry_not_before
                .is_some_and(|retry_not_before| TokioInstant::now() < retry_not_before)
            {
                return;
            }
            state.reconnect_in_flight = true;
        }

        let reconnect = Arc::clone(self);
        tokio::spawn(async move {
            let result = (reconnect.factory)().await;
            let startup_status_context = reconnect.startup_status_context.clone();
            let recovered = {
                let mut state = reconnect
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.reconnect_in_flight = false;
                match result {
                    Ok(client) => {
                        state.current_client = Some(client);
                        state.consecutive_failures = 0;
                        state.retry_not_before = None;
                        true
                    }
                    Err(error) => {
                        state.consecutive_failures = state.consecutive_failures.saturating_add(1);
                        let retry_after = codex_apps_reconnect_backoff(state.consecutive_failures);
                        state.retry_not_before = Some(TokioInstant::now() + retry_after);
                        warn!(
                            error = %error,
                            retry_after_ms = retry_after.as_millis(),
                            "Apps MCP startup reconnect failed; continuing with cached tools"
                        );
                        false
                    }
                }
            };

            if recovered && let Some(context) = startup_status_context {
                let _ = context
                    .tx_event
                    .send(Event {
                        id: context.submit_id,
                        msg: EventMsg::McpStartupUpdate(McpStartupUpdateEvent {
                            server: context.server_name,
                            status: McpStartupStatus::Ready,
                        }),
                    })
                    .await;
            }
        });
    }
}

fn codex_apps_reconnect_backoff(consecutive_failures: u32) -> Duration {
    let exponent = consecutive_failures.saturating_sub(1).min(5);
    CODEX_APPS_RECONNECT_INITIAL_BACKOFF
        .saturating_mul(1 << exponent)
        .min(CODEX_APPS_RECONNECT_MAX_BACKOFF)
}

#[derive(Clone)]
struct ManagedClientStartup {
    server_name: String,
    server: EffectiveMcpServer,
    store_mode: OAuthCredentialsStoreMode,
    keyring_backend_kind: AuthKeyringBackendKind,
    tx_event: Option<Sender<Event>>,
    elicitation_requests: ElicitationRequestManager,
    codex_apps_tools_cache_context: Option<ConnectorRuntimeContext<ToolInfo>>,
    tool_catalog_cache_context: Option<McpToolCatalogCacheContext>,
    runtime_context: McpRuntimeContext,
    resolved_environment: std::result::Result<Option<Arc<Environment>>, String>,
    runtime_auth_provider: Option<SharedAuthProvider>,
    client_elicitation_capability: ElicitationCapability,
    client_mcp_extensions: ClientMcpExtensions,
    protocol_mode: McpProtocolMode,
    catalog_item_limit: usize,
    cancel_token: CancellationToken,
    startup_complete: Arc<AtomicBool>,
    canonical_thread_id: String,
}

impl ManagedClientStartup {
    fn start(&self) -> ManagedClientFuture {
        let Self {
            server_name,
            server,
            store_mode,
            keyring_backend_kind,
            tx_event,
            elicitation_requests,
            codex_apps_tools_cache_context,
            tool_catalog_cache_context,
            runtime_context,
            resolved_environment,
            runtime_auth_provider,
            client_elicitation_capability,
            client_mcp_extensions,
            protocol_mode,
            catalog_item_limit,
            cancel_token,
            startup_complete,
            canonical_thread_id,
        } = self.clone();
        let is_codex_apps_mcp_server = server_name == CODEX_APPS_MCP_SERVER_NAME;
        let thread_identity_eligible = server.config().thread_identity_eligible;
        let startup_timeout = server
            .config()
            .startup_timeout_sec
            .unwrap_or(DEFAULT_STARTUP_TIMEOUT);
        let cancel_token_for_fut = cancel_token;
        async move {
            let tool_catalog_fetch_ticket = tool_catalog_cache_context
                .as_ref()
                .map(McpToolCatalogCacheContext::begin_fetch);
            let refresh_start = is_codex_apps_mcp_server.then(Instant::now);
            let outcome = match async {
                if let Err(error) = validate_mcp_server_name(&server_name) {
                    return Err(error.into());
                }

                let client = match tokio::time::timeout(
                    startup_timeout,
                    make_rmcp_client(
                        &server_name,
                        server.clone(),
                        store_mode,
                        keyring_backend_kind,
                        runtime_context,
                        resolved_environment,
                        runtime_auth_provider,
                        protocol_mode,
                    ),
                )
                .await
                {
                    Ok(result) => {
                        let client = result?;
                        let client = if thread_identity_eligible {
                            client.with_post_reconnect_hook(thread_identity_reconnect_hook(
                                canonical_thread_id.clone(),
                            ))
                        } else {
                            client
                        };
                        Arc::new(client)
                    }
                    Err(_) => {
                        return Err(StartupOutcomeError::from(anyhow!(
                            "MCP client startup timed out after {startup_timeout:?}"
                        )));
                    }
                };
                start_server_task(
                    server_name,
                    client,
                    StartServerTaskParams {
                        is_codex_apps_mcp_server,
                        startup_timeout: Some(startup_timeout),
                        tx_event,
                        elicitation_requests,
                        codex_apps_tools_cache_context,
                        tool_catalog_cache_context,
                        tool_catalog_fetch_ticket,
                        client_elicitation_capability,
                        client_mcp_extensions,
                        catalog_item_limit,
                        thread_identity_eligible,
                        canonical_thread_id,
                    },
                )
                .await
            }
            .or_cancel(&cancel_token_for_fut)
            .await
            {
                Ok(result) => result,
                Err(CancelErr::Cancelled) => Err(StartupOutcomeError::Cancelled),
            };
            if outcome.is_ok()
                && let Some(refresh_start) = refresh_start
            {
                emit_duration(
                    CODEX_APPS_REFRESH_DURATION_METRIC,
                    refresh_start.elapsed(),
                    &[("path", "legacy"), ("trigger", "initial")],
                );
            }

            startup_complete.store(true, Ordering::Release);
            outcome
        }
        .in_current_span()
        .boxed()
        .shared()
    }
}

#[derive(Clone)]
pub(crate) struct AsyncManagedClient {
    pub(crate) client: ManagedClientFuture,
    pub(crate) is_codex_apps_mcp_server: bool,
    pub(crate) cached_server_info: Option<McpServerInfo>,
    pub(crate) codex_apps_tools_cache_context: Option<ConnectorRuntimeContext<ToolInfo>>,
    pub(crate) tool_catalog_cache_context: Option<McpToolCatalogCacheContext>,
    pub(crate) startup_complete: Arc<AtomicBool>,
    pub(crate) startup_reconnect: Option<Arc<CodexAppsStartupReconnect>>,
    pub(crate) cancel_token: CancellationToken,
}

impl AsyncManagedClient {
    // Keep this constructor flat so the startup inputs remain readable at the
    // single call site instead of introducing a one-off params wrapper.
    #[instrument(level = "trace", skip_all, fields(server_name = %server_name))]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        server_name: String,
        startup_submit_id: String,
        server: EffectiveMcpServer,
        store_mode: OAuthCredentialsStoreMode,
        keyring_backend_kind: AuthKeyringBackendKind,
        cancel_token: CancellationToken,
        tx_event: Option<Sender<Event>>,
        elicitation_requests: ElicitationRequestManager,
        codex_apps_tools_cache_context: Option<ConnectorRuntimeContext<ToolInfo>>,
        tool_catalog_cache_context: Option<McpToolCatalogCacheContext>,
        runtime_context: McpRuntimeContext,
        resolved_environment: std::result::Result<Option<Arc<Environment>>, String>,
        runtime_auth_provider: Option<SharedAuthProvider>,
        client_elicitation_capability: ElicitationCapability,
        client_mcp_extensions: ClientMcpExtensions,
        protocol_mode: McpProtocolMode,
        catalog_item_limit: usize,
        canonical_thread_id: String,
    ) -> Self {
        let is_codex_apps_mcp_server = server_name == CODEX_APPS_MCP_SERVER_NAME;
        let reconnect_server_name = server_name.clone();
        let reconnect_tx_event = tx_event.clone();
        let cached_server_info = if is_codex_apps_mcp_server {
            codex_apps_tools_cache_context
                .as_ref()
                .and_then(ConnectorRuntimeContext::cached_server_info)
        } else {
            None
        };
        let startup_complete = Arc::new(AtomicBool::new(false));
        let startup = Arc::new(ManagedClientStartup {
            server_name,
            server,
            store_mode,
            keyring_backend_kind,
            tx_event,
            elicitation_requests,
            codex_apps_tools_cache_context: codex_apps_tools_cache_context.clone(),
            tool_catalog_cache_context: tool_catalog_cache_context.clone(),
            runtime_context,
            resolved_environment,
            runtime_auth_provider,
            client_elicitation_capability,
            client_mcp_extensions,
            protocol_mode,
            catalog_item_limit,
            cancel_token: cancel_token.clone(),
            startup_complete: Arc::clone(&startup_complete),
            canonical_thread_id,
        });
        let client = startup.start();
        let startup_reconnect = is_codex_apps_mcp_server.then(|| {
            let startup = Arc::clone(&startup);
            Arc::new(
                CodexAppsStartupReconnect::new(Arc::new(move || startup.start()))
                    .with_startup_status_context(
                        startup_submit_id,
                        reconnect_server_name,
                        reconnect_tx_event,
                    ),
            )
        });
        Self {
            client,
            is_codex_apps_mcp_server,
            cached_server_info,
            codex_apps_tools_cache_context,
            tool_catalog_cache_context,
            startup_complete,
            startup_reconnect,
            cancel_token,
        }
    }

    pub(crate) async fn client(&self) -> Result<ManagedClient, StartupOutcomeError> {
        if let Some(client) = self
            .startup_reconnect
            .as_ref()
            .and_then(|reconnect| reconnect.current_client())
        {
            return Ok(client);
        }
        self.client.clone().await
    }

    pub(crate) fn ready_transport(&self) -> Option<Arc<RmcpClient>> {
        let recovered = self.startup_reconnect.as_ref().and_then(|reconnect| {
            reconnect
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .current_client
                .as_ref()
                .map(|client| Arc::clone(&client.client))
        });
        recovered.or_else(|| {
            self.client
                .peek()
                .and_then(|result| result.as_ref().ok())
                .map(|client| Arc::clone(&client.client))
        })
    }

    pub(crate) async fn reconnect_failed_startup(&self) {
        let Some(startup_reconnect) = self.startup_reconnect.as_ref() else {
            return;
        };
        if !self.startup_complete.load(Ordering::Acquire) {
            return;
        }
        if matches!(self.client().await, Err(StartupOutcomeError::Failed { .. })) {
            startup_reconnect.reconnect_in_background();
        }
    }

    pub(crate) async fn shutdown(&self) {
        self.cancel_token.cancel();
        match self.client().await {
            Ok(client) => client.client.shutdown().await,
            Err(StartupOutcomeError::Cancelled) => {}
            Err(error) => {
                warn!("failed to initialize MCP client during shutdown: {error:#}");
            }
        }
    }

    pub(crate) fn has_cached_tools(&self) -> bool {
        self.codex_apps_tools_cache_context
            .as_ref()
            .is_some_and(ConnectorRuntimeContext::has_current_tools)
            || self
                .tool_catalog_cache_context
                .as_ref()
                .is_some_and(McpToolCatalogCacheContext::has_tools)
    }

    fn cached_tools(&self) -> Option<Vec<ToolInfo>> {
        self.codex_apps_tools_cache_context
            .as_ref()
            .and_then(ConnectorRuntimeContext::current_tools)
            .or_else(|| {
                self.tool_catalog_cache_context
                    .as_ref()
                    .and_then(McpToolCatalogCacheContext::current_tools)
            })
    }

    pub(crate) async fn listed_tools(&self) -> Option<Vec<ToolInfo>> {
        // Plugin provenance is resolved per-session rather than stored in shared cache payloads.
        if !self.startup_complete.load(Ordering::Acquire)
            && let Some(startup_tools) = self.cached_tools()
        {
            Some(startup_tools)
        } else {
            match self.client().await {
                Ok(client) => Some(client.listed_tools()),
                Err(_) if self.is_codex_apps_mcp_server => self.cached_tools(),
                Err(_) => None,
            }
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub(crate) enum StartupOutcomeError {
    #[error("MCP startup cancelled")]
    Cancelled,
    // We can't store the original error here because anyhow::Error doesn't implement
    // `Clone`.
    #[error("MCP startup failed: {error}")]
    Failed {
        error: String,
        is_authentication_required: bool,
    },
}

impl StartupOutcomeError {
    pub(crate) fn is_authentication_required(&self) -> bool {
        match self {
            Self::Cancelled => false,
            Self::Failed {
                is_authentication_required,
                ..
            } => *is_authentication_required,
        }
    }
}

impl From<anyhow::Error> for StartupOutcomeError {
    fn from(error: anyhow::Error) -> Self {
        let is_authentication_required = is_authentication_required_error(&error);
        Self::Failed {
            error: error.to_string(),
            is_authentication_required,
        }
    }
}

#[instrument(level = "trace", skip_all, fields(server_name = %server_name))]
pub(crate) async fn list_tools_for_client_uncached(
    server_name: &str,
    is_codex_apps_mcp_server: bool,
    codex_apps_refresh_trigger: &'static str,
    client: &Arc<RmcpClient>,
    timeout: Option<Duration>,
    catalog_item_limit: usize,
    server_instructions: Option<&str>,
) -> Result<Vec<ToolInfo>> {
    let fetch_start = Instant::now();
    let protocol_mode = client.protocol_mode();
    let tools = collect_paginated_with_limit("tools/list", timeout, catalog_item_limit, |params| {
        let client = Arc::clone(client);
        async move {
            let response = client
                .list_tools_with_connector_ids(params, timeout)
                .await?;
            let next_cursor = match protocol_mode {
                McpProtocolMode::Legacy => None,
                McpProtocolMode::V20260728 => response.next_cursor,
            };
            Ok((response.tools, next_cursor))
        }
    })
    .await?
    .into_iter()
    .map(|tool| {
        tool_info_from_listed_tool(
            server_name,
            is_codex_apps_mcp_server,
            server_instructions,
            tool,
        )
    })
    .collect();
    if is_codex_apps_mcp_server {
        emit_duration(
            MCP_TOOLS_FETCH_UNCACHED_DURATION_METRIC,
            fetch_start.elapsed(),
            &[("trigger", codex_apps_refresh_trigger)],
        );
    } else {
        emit_duration(
            MCP_TOOLS_FETCH_UNCACHED_DURATION_METRIC,
            fetch_start.elapsed(),
            &[],
        );
    }
    Ok(tools)
}

/// Presents declared Codex Apps file parameters to the model as local-path inputs and adds plugin
/// names to each tool. Plugin membership is resolved by connector ID, falling back to the MCP
/// server when absent.
pub(crate) fn prepare_codex_apps_tools_for_model(
    mut tools: Vec<ToolInfo>,
    tool_plugin_provenance: &ToolPluginProvenance,
) -> Vec<ToolInfo> {
    for tool in &mut tools {
        prepare_openai_file_params_for_model(tool);
        let plugin_names = match tool.connector_id.as_deref() {
            Some(connector_id) => {
                tool_plugin_provenance.plugin_display_names_for_connector_id(connector_id)
            }
            None => tool_plugin_provenance
                .plugin_display_names_for_mcp_server_name(tool.server_name.as_str()),
        };
        add_plugin_provenance_to_tool(tool, plugin_names);
    }
    tools
}

/// Stores plugin names on the tool and appends a model-visible plugin membership note.
fn add_plugin_provenance_to_tool(tool: &mut ToolInfo, plugin_names: &[String]) {
    tool.plugin_display_names = plugin_names.to_vec();
    if plugin_names.is_empty() {
        return;
    }

    let plugin_source_note = if plugin_names.len() == 1 {
        format!("This tool is part of plugin `{}`.", plugin_names[0])
    } else {
        format!(
            "This tool is part of plugins {}.",
            plugin_names
                .iter()
                .map(|plugin_name| format!("`{plugin_name}`"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let description = tool
        .tool
        .description
        .as_deref()
        .map(str::trim)
        .unwrap_or("");
    let annotated_description = if description.is_empty() {
        plugin_source_note
    } else if matches!(description.chars().last(), Some('.' | '!' | '?')) {
        format!("{description} {plugin_source_note}")
    } else {
        format!("{description}. {plugin_source_note}")
    };
    tool.tool.description = Some(Cow::Owned(annotated_description));
}

/// Adds server-scoped plugin names to regular MCP tools without changing their input schemas.
pub(crate) fn prepare_regular_mcp_tools_for_model(
    mut tools: Vec<ToolInfo>,
    tool_plugin_provenance: &ToolPluginProvenance,
) -> Vec<ToolInfo> {
    for tool in &mut tools {
        let plugin_names = tool_plugin_provenance
            .plugin_display_names_for_mcp_server_name(tool.server_name.as_str());
        add_plugin_provenance_to_tool(tool, plugin_names);
    }
    tools
}

fn tool_info_from_listed_tool(
    server_name: &str,
    is_codex_apps_mcp_server: bool,
    server_instructions: Option<&str>,
    tool: ToolWithConnectorId,
) -> ToolInfo {
    if is_codex_apps_mcp_server {
        codex_apps_tool_info_from_listed_tool(server_name, server_instructions, tool)
    } else {
        regular_mcp_tool_info_from_listed_tool(server_name, server_instructions, tool)
    }
}

/// Converts a Codex Apps tool by preserving connector fields, removing connector prefixes from
/// model-visible names and titles, and using the connector description for its tool namespace.
fn codex_apps_tool_info_from_listed_tool(
    server_name: &str,
    server_instructions: Option<&str>,
    tool: ToolWithConnectorId,
) -> ToolInfo {
    let mut tool_def = tool.tool;
    let connector_id = tool.connector_id;
    let connector_name = tool.connector_name;
    let connector_description = tool.connector_description;
    let callable_name = normalize_codex_apps_callable_name(
        &tool_def.name,
        connector_id.as_deref(),
        connector_name.as_deref(),
    );
    let callable_namespace =
        normalize_codex_apps_callable_namespace(server_name, connector_name.as_deref());
    if let Some(title) = tool_def.title.as_deref() {
        let normalized_title = normalize_codex_apps_tool_title(connector_name.as_deref(), title);
        if tool_def.title.as_deref() != Some(normalized_title.as_str()) {
            tool_def.title = Some(normalized_title);
        }
    }
    let has_connector_metadata =
        connector_id.is_some() || connector_name.is_some() || connector_description.is_some();
    let namespace_description = if has_connector_metadata {
        connector_description
    } else {
        server_instructions.map(str::to_string)
    };
    ToolInfo {
        server_name: server_name.to_owned(),
        supports_parallel_tool_calls: false,
        server_origin: None,
        callable_name,
        callable_namespace,
        namespace_description,
        tool: tool_def,
        openai_file_input_optional_fields: HashMap::new(),
        connector_id,
        connector_name,
        plugin_display_names: Vec::new(),
    }
}

/// Converts a regular MCP tool by removing reserved connector metadata, keeping its raw tool name,
/// and using the MCP server name and instructions for the model-visible namespace.
fn regular_mcp_tool_info_from_listed_tool(
    server_name: &str,
    server_instructions: Option<&str>,
    tool: ToolWithConnectorId,
) -> ToolInfo {
    let mut tool_def = tool.tool;
    strip_untrusted_connector_meta(&mut tool_def);
    ToolInfo {
        server_name: server_name.to_owned(),
        supports_parallel_tool_calls: false,
        server_origin: None,
        callable_name: tool_def.name.to_string(),
        callable_namespace: server_name.to_string(),
        namespace_description: server_instructions.map(str::to_string),
        tool: tool_def,
        openai_file_input_optional_fields: HashMap::new(),
        connector_id: None,
        connector_name: None,
        plugin_display_names: Vec::new(),
    }
}

fn strip_untrusted_connector_meta(tool: &mut RmcpTool) {
    if let Some(meta) = tool.meta.as_mut() {
        meta.retain(|key, _| !is_untrusted_connector_meta_key(key));
    }
}

fn is_untrusted_connector_meta_key(key: &str) -> bool {
    UNTRUSTED_CONNECTOR_META_KEYS.contains(&key)
}

fn resolve_bearer_token(
    server_name: &str,
    bearer_token_env_var: Option<&str>,
) -> Result<Option<String>> {
    let Some(env_var) = bearer_token_env_var else {
        return Ok(None);
    };

    match env::var(env_var) {
        Ok(value) => {
            if value.is_empty() {
                Err(anyhow!(
                    "Environment variable {env_var} for MCP server '{server_name}' is empty"
                ))
            } else {
                Ok(Some(value))
            }
        }
        Err(env::VarError::NotPresent) => Err(anyhow!(
            "Environment variable {env_var} for MCP server '{server_name}' is not set"
        )),
        Err(env::VarError::NotUnicode(_)) => Err(anyhow!(
            "Environment variable {env_var} for MCP server '{server_name}' contains invalid Unicode"
        )),
    }
}

fn validate_mcp_server_name(server_name: &str) -> Result<()> {
    let re = regex_lite::Regex::new(r"^[a-zA-Z0-9_-]+$")?;
    if !re.is_match(server_name) {
        return Err(anyhow!(
            "Invalid MCP server name '{server_name}': must match pattern {pattern}",
            pattern = re.as_str()
        ));
    }
    Ok(())
}

#[instrument(level = "trace", skip_all, fields(server_name = %server_name))]
async fn start_server_task(
    server_name: String,
    client: Arc<RmcpClient>,
    params: StartServerTaskParams,
) -> Result<ManagedClient, StartupOutcomeError> {
    let StartServerTaskParams {
        is_codex_apps_mcp_server,
        startup_timeout,
        tx_event,
        elicitation_requests,
        codex_apps_tools_cache_context,
        tool_catalog_cache_context,
        tool_catalog_fetch_ticket,
        client_elicitation_capability,
        client_mcp_extensions,
        catalog_item_limit,
        thread_identity_eligible,
        canonical_thread_id,
    } = params;
    let params =
        mcp_initialize_request_params(client_elicitation_capability, client_mcp_extensions);
    let send_elicitation = elicitation_requests.make_sender(server_name.clone(), tx_event);

    let initialize_result = client
        .initialize(params, startup_timeout, send_elicitation)
        .await
        .map_err(StartupOutcomeError::from)?;

    if thread_identity_eligible {
        bind_thread_identity(
            &client,
            &initialize_result,
            &canonical_thread_id,
            startup_timeout,
        )
        .await?;
    }

    let server_disables_tool_catalog_cache = initialize_result
        .capabilities
        .experimental
        .as_ref()
        .and_then(|experimental| experimental.get(MCP_TOOL_CATALOG_CACHE_CAPABILITY))
        .and_then(|capability| capability.get(MCP_TOOL_CATALOG_CACHEABLE_PROPERTY))
        .and_then(serde_json::Value::as_bool)
        == Some(false);
    if server_disables_tool_catalog_cache
        && let Some(cache_context) = tool_catalog_cache_context.as_ref()
    {
        cache_context.disable();
    }
    let server_supports_sandbox_state_meta_capability = initialize_result
        .capabilities
        .experimental
        .as_ref()
        .and_then(|exp| exp.get(MCP_SANDBOX_STATE_META_CAPABILITY))
        .is_some();
    let list_start = Instant::now();
    let fetch_ticket = codex_apps_tools_cache_context
        .as_ref()
        .map(|cache_context| cache_context.begin_fetch(ConnectorRuntimeFetchSource::Startup));
    let client_tools = list_tools_for_client_uncached(
        &server_name,
        is_codex_apps_mcp_server,
        /*codex_apps_refresh_trigger*/ "initial",
        &client,
        startup_timeout,
        catalog_item_limit,
        initialize_result.instructions.as_deref(),
    )
    .await
    .map_err(StartupOutcomeError::from)?;
    let server_info =
        mcp_server_info_from_implementation(&server_name, initialize_result.server_info);
    let shared_tools = match (codex_apps_tools_cache_context.as_ref(), fetch_ticket) {
        (Some(cache_context), Some(fetch_ticket)) => cache_context.publish_if_newest_accepted(
            fetch_ticket,
            &server_info,
            client_tools.clone(),
        ),
        (None, None) => client_tools.clone(),
        _ => unreachable!("Codex Apps fetch ticket requires cache context"),
    };
    let has_shared_tool_catalog = is_codex_apps_mcp_server || tool_catalog_cache_context.is_some();
    if let (Some(cache_context), Some(fetch_ticket)) = (
        tool_catalog_cache_context.as_ref(),
        tool_catalog_fetch_ticket,
    ) {
        cache_context.publish_if_newest(fetch_ticket, &shared_tools);
    }
    if has_shared_tool_catalog {
        emit_duration(
            MCP_TOOLS_LIST_DURATION_METRIC,
            list_start.elapsed(),
            &[("cache", "miss")],
        );
    }
    let managed = ManagedClient {
        client: Arc::clone(&client),
        server_info,
        tools: client_tools,
        tool_timeout: None,
        server_instructions: initialize_result.instructions,
        server_supports_sandbox_state_meta_capability,
        codex_apps_tools_cache_context,
    };

    Ok(managed)
}

/// Validates a `codex/thread-identity` declaration's `versions` value
/// against `kcf-runtime/04`'s exact grammar: a non-empty JSON array of
/// distinct positive integers, compatible only when it contains `1`. An
/// object that is missing `versions`, has a malformed list (wrong type,
/// empty, non-positive, non-integer, or duplicate entries), or lacks `1`
/// is an unsupported declaration in its entirety — not "supported because
/// `1` appears somewhere in an otherwise malformed array."
fn is_compatible_thread_identity_versions(versions: &serde_json::Value) -> bool {
    let Some(entries) = versions.as_array() else {
        return false;
    };
    if entries.is_empty() {
        return false;
    }
    let mut seen = std::collections::HashSet::new();
    for entry in entries {
        let Some(version) = entry.as_i64() else {
            return false;
        };
        if version <= 0 {
            return false;
        }
        if !seen.insert(version) {
            return false;
        }
    }
    seen.contains(&MCP_THREAD_IDENTITY_SUPPORTED_VERSION)
}

/// Whether `peer_info` declares a compatible `codex/thread-identity`
/// version, per `kcf-runtime/04`. Shared by both the startup bind path and
/// the session-expiry recovery rebind hook so declaration-reading logic
/// exists exactly once.
fn declares_compatible_thread_identity(peer_info: &ServerPeerInfo) -> bool {
    peer_info
        .capabilities
        .experimental
        .as_ref()
        .and_then(|experimental| experimental.get(MCP_THREAD_IDENTITY_CAPABILITY))
        .and_then(|capability| capability.get(MCP_THREAD_IDENTITY_VERSIONS_PROPERTY))
        .is_some_and(is_compatible_thread_identity_versions)
}

fn thread_identity_bind_params(canonical_thread_id: &str) -> serde_json::Value {
    serde_json::json!({
        "version": MCP_THREAD_IDENTITY_SUPPORTED_VERSION,
        "threadId": canonical_thread_id,
    })
}

/// Whether `response` is exactly the required `{"version": 1, "accepted":
/// true}` acknowledgement. Shared by both bind paths for the same reason
/// as [`declares_compatible_thread_identity`].
fn thread_identity_ack_accepted(response: &ServerResult) -> bool {
    matches!(
        response,
        ServerResult::CustomResult(CustomResult(value))
            if value == &serde_json::json!({
                "version": MCP_THREAD_IDENTITY_SUPPORTED_VERSION,
                "accepted": true,
            })
    )
}

/// Binds the connection-scoped `codex/thread-identity` capability if the
/// server declared a compatible version, per
/// `kcf-runtime/04-mcp-thread-identity-contract.md`. Only called for
/// servers explicitly opted into `thread_identity_eligible`. A server that
/// does not declare `codex/thread-identity`, or declares only incompatible
/// versions, is left unbound and startup proceeds normally — declining to
/// bind is not an error. A server that declares a compatible version but
/// then rejects or malforms the bind acknowledgement fails startup for this
/// connection, so no provider operation is ever admitted on an unbound
/// connection.
async fn bind_thread_identity(
    client: &RmcpClient,
    initialize_result: &ServerPeerInfo,
    canonical_thread_id: &str,
    startup_timeout: Option<Duration>,
) -> Result<(), StartupOutcomeError> {
    if !declares_compatible_thread_identity(initialize_result) {
        return Ok(());
    }

    let response = client
        .send_custom_request_with_timeout(
            MCP_THREAD_IDENTITY_CAPABILITY,
            Some(thread_identity_bind_params(canonical_thread_id)),
            startup_timeout,
        )
        .await
        .map_err(StartupOutcomeError::from)?;
    if thread_identity_ack_accepted(&response) {
        Ok(())
    } else {
        Err(StartupOutcomeError::from(anyhow!(
            "MCP server declared codex/thread-identity but rejected or malformed the bind acknowledgement"
        )))
    }
}

/// Rebinds `codex/thread-identity` after `RmcpClient` transparently
/// recovers a Streamable HTTP session-expiry: installed as a
/// [`codex_rmcp_client::PostReconnectHook`] on eligible connections only
/// (see `make_rmcp_client`), so it is a complete no-op — no extra request,
/// no extra latency — for every ordinary connection. Shares
/// `declares_compatible_thread_identity`/`thread_identity_ack_accepted`
/// with the startup path (`bind_thread_identity`) so the declaration
/// grammar and acknowledgement shape are checked identically in both
/// places; only how the request is *sent* differs, because the recovery
/// path must operate on the specific freshly reconnected physical
/// connection via `ReconnectContext` rather than through
/// `RmcpClient`'s own (not-yet-`Ready`) state — see `ReconnectContext`'s
/// doc comment in `rmcp-client` for why.
fn thread_identity_reconnect_hook(canonical_thread_id: String) -> PostReconnectHook {
    Box::new(move |context, peer_info| {
        let canonical_thread_id = canonical_thread_id.clone();
        async move {
            if !declares_compatible_thread_identity(peer_info) {
                return Ok(());
            }
            let response = context
                .send_custom_request(
                    MCP_THREAD_IDENTITY_CAPABILITY,
                    Some(thread_identity_bind_params(&canonical_thread_id)),
                    /*timeout*/ None,
                )
                .await?;
            if thread_identity_ack_accepted(&response) {
                Ok(())
            } else {
                Err(anyhow!(
                    "MCP server declared codex/thread-identity but rejected or malformed the \
                     recovery bind acknowledgement"
                ))
            }
        }
        .boxed()
    })
}

fn mcp_initialize_request_params(
    client_elicitation_capability: ElicitationCapability,
    client_mcp_extensions: ClientMcpExtensions,
) -> InitializeRequestParams {
    let mut capabilities = ClientCapabilities::default();
    capabilities.elicitation = Some(client_elicitation_capability);
    let extensions = client_mcp_extensions
        .iter()
        .filter_map(|(id, settings)| {
            settings
                .as_object()
                .cloned()
                .map(|settings| (id.to_string(), settings))
        })
        .collect::<BTreeMap<_, _>>();
    if !extensions.is_empty() {
        capabilities.extensions = Some(extensions);
    }
    InitializeRequestParams::new(
        capabilities,
        Implementation::new("codex-mcp-client", env!("CARGO_PKG_VERSION")).with_title("Codex"),
    )
    .with_protocol_version(ProtocolVersion::V_2025_06_18)
}

fn mcp_server_info_from_implementation(
    server_name: &str,
    server_info: Option<Implementation>,
) -> McpServerInfo {
    let server_info = server_info.unwrap_or_else(|| Implementation::new(server_name, ""));
    McpServerInfo {
        name: server_info.name,
        title: server_info.title,
        version: server_info.version,
        description: server_info.description,
        icons: server_info.icons.map(|icons| {
            icons
                .into_iter()
                .filter_map(|icon| serde_json::to_value(icon).ok())
                .collect()
        }),
        website_url: server_info.website_url,
    }
}

struct StartServerTaskParams {
    is_codex_apps_mcp_server: bool,
    startup_timeout: Option<Duration>, // TODO: cancel_token should handle this.
    tx_event: Option<Sender<Event>>,
    elicitation_requests: ElicitationRequestManager,
    codex_apps_tools_cache_context: Option<ConnectorRuntimeContext<ToolInfo>>,
    tool_catalog_cache_context: Option<McpToolCatalogCacheContext>,
    tool_catalog_fetch_ticket: Option<McpToolCatalogFetchTicket>,
    client_elicitation_capability: ElicitationCapability,
    client_mcp_extensions: ClientMcpExtensions,
    catalog_item_limit: usize,
    thread_identity_eligible: bool,
    canonical_thread_id: String,
}

#[allow(clippy::too_many_arguments)]
#[instrument(level = "trace", skip_all, fields(server_name = %server_name))]
async fn make_rmcp_client(
    server_name: &str,
    server: EffectiveMcpServer,
    store_mode: OAuthCredentialsStoreMode,
    keyring_backend_kind: AuthKeyringBackendKind,
    runtime_context: McpRuntimeContext,
    resolved_environment: std::result::Result<Option<Arc<Environment>>, String>,
    runtime_auth_provider: Option<SharedAuthProvider>,
    protocol_mode: McpProtocolMode,
) -> Result<RmcpClient, StartupOutcomeError> {
    let config = server.config().clone();
    if matches!(config.auth, McpServerAuth::ChatGpt)
        && !config.is_local_environment()
        && !has_explicit_http_authorization(&config)
    {
        return Err(StartupOutcomeError::from(anyhow!(
            "executor-owned MCP server `{server_name}` cannot use hosted ChatGPT authentication; configure executor-owned credentials instead"
        )));
    }
    let resolved_environment =
        resolved_environment.map_err(|err| StartupOutcomeError::from(anyhow!(err)))?;
    let is_local_environment = config.is_local_environment();
    let oauth_credential_name = config.oauth_credential_name(server_name);
    let McpServerConfig { transport, .. } = config;

    match transport {
        McpServerTransportConfig::Stdio {
            command,
            args,
            env,
            env_vars,
            cwd,
        } => {
            let command_os: OsString = command.into();
            let args_os: Vec<OsString> = args.into_iter().map(Into::into).collect();
            let env_os = env.map(|env| {
                env.into_iter()
                    .map(|(key, value)| (key.into(), value.into()))
                    .collect::<HashMap<_, _>>()
            });
            let launcher = if is_local_environment {
                // TODO(starr): Unify local stdio MCP launch with
                // `ExecutorStdioServerLauncher` once the executor-backed path
                // preserves `LocalStdioServerLauncher` semantics.
                Arc::new(LocalStdioServerLauncher::new(
                    runtime_context.local_stdio_fallback_cwd(),
                )) as Arc<dyn StdioServerLauncher>
            } else {
                let Some(environment) = resolved_environment.as_ref() else {
                    unreachable!(
                        "non-local stdio MCP servers resolve an environment before launch"
                    );
                };
                Arc::new(ExecutorStdioServerLauncher::new(
                    environment.get_exec_backend(),
                )) as Arc<dyn StdioServerLauncher>
            };

            let cwd = cwd.map(codex_utils_path_uri::LegacyAppPathString::into_string);
            RmcpClient::new_stdio_client_with_protocol_mode(
                command_os,
                args_os,
                env_os,
                &env_vars,
                cwd,
                launcher,
                protocol_mode,
            )
            .await
            .map_err(|err| StartupOutcomeError::from(anyhow!(err)))
        }
        McpServerTransportConfig::StreamableHttp {
            url,
            http_headers,
            env_http_headers,
            bearer_token_env_var,
        } => {
            let http_client = resolved_environment.as_ref().map_or_else(
                || runtime_context.local_http_client(),
                |environment| environment.get_http_client(),
            );
            let http_client = maybe_with_openai_docs_source_attribution(&url, http_client);
            let resolved_bearer_token =
                match resolve_bearer_token(server_name, bearer_token_env_var.as_deref()) {
                    Ok(token) => token,
                    Err(error) => return Err(error.into()),
                };
            let redirect_mode = if server.is_agent_plugin() {
                StreamableHttpRedirectMode::AgentPluginV1
            } else {
                StreamableHttpRedirectMode::Legacy
            };
            RmcpClient::new_streamable_http_client_with_protocol_mode_and_redirect_mode(
                oauth_credential_name.as_ref(),
                &url,
                resolved_bearer_token,
                http_headers,
                env_http_headers,
                store_mode,
                keyring_backend_kind,
                http_client,
                runtime_auth_provider,
                protocol_mode,
                redirect_mode,
            )
            .await
            .map_err(StartupOutcomeError::from)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::elicitation::ElicitationRequestRouter;
    use codex_protocol::mcp::MCP_APP_UI_EXTENSION_ID;
    use codex_protocol::mcp::OPENAI_FORM_EXTENSION_ID;
    use codex_protocol::models::PermissionProfile;
    use codex_protocol::protocol::AskForApproval;
    use codex_rmcp_client::ElicitationAction;
    use codex_rmcp_client::ElicitationResponse;
    use pretty_assertions::assert_eq;
    use rmcp::model::ClientCapabilities;
    use rmcp::model::JsonObject;
    use rmcp::model::MetaObject;
    use rmcp::transport::auth::AuthError;
    use wiremock::Mock;
    use wiremock::MockServer;
    use wiremock::Request;
    use wiremock::ResponseTemplate;
    use wiremock::matchers::method;
    use wiremock::matchers::path;

    const THREAD_IDENTITY_TEST_LEGACY_VERSION: &str = "2025-06-18";

    fn thread_identity_test_initialize_params() -> InitializeRequestParams {
        InitializeRequestParams::new(
            ClientCapabilities::default(),
            Implementation::new("codex-thread-identity-bind-test", "0.0.0"),
        )
        .with_protocol_version(ProtocolVersion::V_2025_06_18)
    }

    fn thread_identity_test_initialize_response(
        request: &serde_json::Value,
        experimental_versions: Option<&serde_json::Value>,
    ) -> ResponseTemplate {
        let mut capabilities = serde_json::json!({"tools": {}});
        if let Some(versions) = experimental_versions {
            capabilities["experimental"] = serde_json::json!({
                MCP_THREAD_IDENTITY_CAPABILITY: {"versions": versions},
            });
        }
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "jsonrpc": "2.0",
            "id": request["id"],
            "result": {
                "protocolVersion": THREAD_IDENTITY_TEST_LEGACY_VERSION,
                "capabilities": capabilities,
                "serverInfo": {"name": "thread-identity-bind-test-server", "version": "1.0.0"},
            },
        }))
    }

    /// Mounts a mock server that declares `experimental_versions` (or no
    /// declaration at all, when `None`) on `initialize`, and responds to the
    /// `codex/thread-identity` bind request with `bind_response` (a full
    /// JSON-RPC `result` value). Returns the connected, initialized client
    /// plus the mock server (kept alive so `received_requests` works).
    async fn start_thread_identity_test_client(
        experimental_versions: Option<serde_json::Value>,
        bind_response: serde_json::Value,
    ) -> (RmcpClient, MockServer) {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp"))
            .respond_with(move |request: &Request| {
                let body: serde_json::Value = request.body_json().expect("valid JSON-RPC request");
                match body["method"].as_str() {
                    Some("initialize") => thread_identity_test_initialize_response(
                        &body,
                        experimental_versions.as_ref(),
                    ),
                    Some("notifications/initialized") => ResponseTemplate::new(202),
                    Some(MCP_THREAD_IDENTITY_CAPABILITY) => ResponseTemplate::new(200)
                        .set_body_json(serde_json::json!({
                            "jsonrpc": "2.0",
                            "id": body["id"],
                            "result": bind_response,
                        })),
                    other => panic!("unexpected thread-identity test method: {other:?}"),
                }
            })
            .mount(&server)
            .await;

        let client = RmcpClient::new_streamable_http_client_with_protocol_mode(
            "thread-identity-bind-test",
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
        .await
        .expect("client should construct");
        (client, server)
    }

    async fn thread_identity_test_initialize(client: &RmcpClient) -> ServerPeerInfo {
        client
            .initialize(
                thread_identity_test_initialize_params(),
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
            .expect("initialize should succeed")
    }

    #[tokio::test]
    async fn bind_thread_identity_sends_exact_request_and_accepts_matching_ack() {
        let (client, server) = start_thread_identity_test_client(
            Some(serde_json::json!([1])),
            serde_json::json!({"version": 1, "accepted": true}),
        )
        .await;
        let initialize_result = thread_identity_test_initialize(&client).await;
        let client = Arc::new(client);

        let result = bind_thread_identity(
            &client,
            &initialize_result,
            "thread-abc-123",
            Some(Duration::from_secs(5)),
        )
        .await;

        assert!(result.is_ok(), "expected bind to succeed: {result:?}");

        let bind_request = server
            .received_requests()
            .await
            .expect("mock server should record requests")
            .into_iter()
            .map(|request| request.body_json::<serde_json::Value>().unwrap())
            .find(|body| body["method"] == MCP_THREAD_IDENTITY_CAPABILITY)
            .expect("the bind request should have been sent");
        // The rmcp SDK's `send_request` path attaches a standard
        // `_meta.progressToken` to every outbound custom request — observed
        // here against the real client, not assumed. This is uniform
        // JSON-RPC/MCP progress-tracking envelope machinery applied to every
        // custom request the SDK sends, not something `bind_thread_identity`
        // opts into or controls, and it carries no thread/session/identity
        // data (just an SDK-assigned integer counter). Rather than silently
        // loosening the assertion, assert the full param key set and the
        // full shape of `_meta` explicitly, so any future SDK change that
        // starts smuggling more than a bare progress token through this
        // envelope breaks this test.
        let params = bind_request["params"]
            .as_object()
            .expect("bind request params should be an object");
        let mut actual_keys: Vec<&str> = params.keys().map(String::as_str).collect();
        actual_keys.sort_unstable();
        assert_eq!(
            actual_keys,
            vec!["_meta", "threadId", "version"],
            "bind request params must carry only kcf-runtime/04's version/threadId plus \
             the SDK's own request envelope metadata — nothing else"
        );
        assert_eq!(params["version"], serde_json::json!(1));
        assert_eq!(params["threadId"], serde_json::json!("thread-abc-123"));
        let meta = params["_meta"]
            .as_object()
            .expect("_meta should be an object");
        let mut meta_keys: Vec<&str> = meta.keys().map(String::as_str).collect();
        meta_keys.sort_unstable();
        assert_eq!(
            meta_keys,
            vec!["progressToken"],
            "the SDK envelope's _meta must carry only a progress token, no identity data"
        );
        assert!(
            meta["progressToken"].is_number(),
            "progressToken should be the SDK's own counter, not a smuggled identity value"
        );

        client.shutdown().await;
    }

    #[tokio::test]
    async fn bind_thread_identity_fails_on_mismatched_ack() {
        let (client, _server) = start_thread_identity_test_client(
            Some(serde_json::json!([1])),
            serde_json::json!({"version": 1, "accepted": false}),
        )
        .await;
        let initialize_result = thread_identity_test_initialize(&client).await;
        let client = Arc::new(client);

        let result = bind_thread_identity(
            &client,
            &initialize_result,
            "thread-abc-123",
            Some(Duration::from_secs(5)),
        )
        .await;

        assert!(
            result.is_err(),
            "a rejected/malformed ack must fail the bind, blocking all provider operations \
             on this connection"
        );

        client.shutdown().await;
    }

    #[tokio::test]
    async fn bind_thread_identity_skips_silently_when_declaration_is_incompatible() {
        let (client, server) = start_thread_identity_test_client(
            Some(serde_json::json!([2])),
            serde_json::json!({"version": 1, "accepted": true}),
        )
        .await;
        let initialize_result = thread_identity_test_initialize(&client).await;
        let client = Arc::new(client);

        let result = bind_thread_identity(
            &client,
            &initialize_result,
            "thread-abc-123",
            Some(Duration::from_secs(5)),
        )
        .await;

        assert!(
            result.is_ok(),
            "an incompatible declaration must not error, only skip binding"
        );
        let sent_bind_request = server
            .received_requests()
            .await
            .expect("mock server should record requests")
            .into_iter()
            .map(|request| request.body_json::<serde_json::Value>().unwrap())
            .any(|body| body["method"] == MCP_THREAD_IDENTITY_CAPABILITY);
        assert!(
            !sent_bind_request,
            "no bind request should be sent for an incompatible/absent declaration"
        );

        client.shutdown().await;
    }

    /// Exercises the real production entry point (`start_server_task`), not
    /// just the private `bind_thread_identity` helper, to prove R003/R004's
    /// "bind before every provider operation" requirement holds at the
    /// actual call site: when a thread-identity-eligible server declares a
    /// compatible version but rejects the bind acknowledgement, startup
    /// fails and `tools/list` is never sent on that connection.
    #[tokio::test]
    async fn start_server_task_blocks_tools_list_when_bind_ack_is_rejected() {
        let tools_list_called = Arc::new(AtomicBool::new(false));
        let tools_list_called_for_mock = Arc::clone(&tools_list_called);
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp"))
            .respond_with(move |request: &Request| {
                let body: serde_json::Value = request.body_json().expect("valid JSON-RPC request");
                match body["method"].as_str() {
                    Some("initialize") => thread_identity_test_initialize_response(
                        &body,
                        Some(&serde_json::json!([1])),
                    ),
                    Some("notifications/initialized") => ResponseTemplate::new(202),
                    Some(MCP_THREAD_IDENTITY_CAPABILITY) => ResponseTemplate::new(200)
                        .set_body_json(serde_json::json!({
                            "jsonrpc": "2.0",
                            "id": body["id"],
                            "result": {"version": 1, "accepted": false},
                        })),
                    Some("tools/list") => {
                        tools_list_called_for_mock.store(true, Ordering::SeqCst);
                        ResponseTemplate::new(200).set_body_json(serde_json::json!({
                            "jsonrpc": "2.0",
                            "id": body["id"],
                            "result": {"tools": []},
                        }))
                    }
                    other => panic!("unexpected start_server_task test method: {other:?}"),
                }
            })
            .mount(&server)
            .await;

        let client = RmcpClient::new_streamable_http_client_with_protocol_mode(
            "start-server-task-bind-reject-test",
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
        .await
        .expect("client should construct");
        let client = Arc::new(client);

        let result = start_server_task(
            "start-server-task-bind-reject-test".to_string(),
            Arc::clone(&client),
            StartServerTaskParams {
                is_codex_apps_mcp_server: false,
                startup_timeout: Some(Duration::from_secs(5)),
                tx_event: None,
                elicitation_requests: ElicitationRequestManager::new(
                    AskForApproval::default(),
                    PermissionProfile::read_only(),
                    /*reviewer*/ None,
                    /*lifecycle*/ None,
                    ElicitationRequestRouter::default(),
                ),
                codex_apps_tools_cache_context: None,
                tool_catalog_cache_context: None,
                tool_catalog_fetch_ticket: None,
                client_elicitation_capability: ElicitationCapability::default(),
                client_mcp_extensions: ClientMcpExtensions::default(),
                catalog_item_limit: 100,
                thread_identity_eligible: true,
                canonical_thread_id: "thread-abc-123".to_string(),
            },
        )
        .await;

        assert!(
            result.is_err(),
            "startup must fail when the server rejects the bind acknowledgement"
        );
        assert!(
            !tools_list_called.load(Ordering::SeqCst),
            "tools/list must never be requested on a connection whose bind was rejected"
        );

        client.shutdown().await;
    }

    /// R006 disclosure-containment proof, real-artifact style: runs a
    /// thread-identity-eligible connection through its complete real
    /// startup — `initialize`, the bind request, and `tools/list` — and
    /// inspects **every** raw wire request actually sent (URL, headers,
    /// and full JSON body), not just the ones this code is expected to
    /// touch. Asserts the canonical thread ID appears in exactly the one
    /// request `kcf-runtime/04` authorizes (`codex/thread-identity`'s
    /// `params.threadId`) and nowhere else — not in `initialize`, not in
    /// `tools/list`, not in any URL or header. A marker distinctive enough
    /// that it cannot collide with any real protocol field name is used so
    /// an accidental leak through an unexpected path would still be
    /// caught.
    #[tokio::test]
    async fn thread_identity_never_leaks_outside_the_bind_request() {
        const THREAD_ID_MARKER: &str = "thread-marker-zzq7-must-not-leak-elsewhere";

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp"))
            .respond_with(move |request: &Request| {
                let body: serde_json::Value = request.body_json().expect("valid JSON-RPC request");
                match body["method"].as_str() {
                    Some("initialize") => thread_identity_test_initialize_response(
                        &body,
                        Some(&serde_json::json!([1])),
                    ),
                    Some("notifications/initialized") => ResponseTemplate::new(202),
                    Some(MCP_THREAD_IDENTITY_CAPABILITY) => ResponseTemplate::new(200)
                        .set_body_json(serde_json::json!({
                            "jsonrpc": "2.0",
                            "id": body["id"],
                            "result": {"version": 1, "accepted": true},
                        })),
                    Some("tools/list") => {
                        ResponseTemplate::new(200).set_body_json(serde_json::json!({
                            "jsonrpc": "2.0",
                            "id": body["id"],
                            "result": {"tools": []},
                        }))
                    }
                    other => panic!("unexpected containment test method: {other:?}"),
                }
            })
            .mount(&server)
            .await;

        let client_mcp_extensions = ClientMcpExtensions::new([(
            MCP_APP_UI_EXTENSION_ID.to_string(),
            serde_json::json!({"unrelated": "value"}),
        )]);

        let client = RmcpClient::new_streamable_http_client_with_protocol_mode(
            "thread-identity-containment-test",
            &format!("{}/mcp", server.uri()),
            /*bearer_token*/ None,
            /*http_headers*/
            Some(HashMap::from([(
                "X-Unrelated-Header".to_string(),
                "unrelated-value".to_string(),
            )])),
            /*env_http_headers*/ None,
            OAuthCredentialsStoreMode::File,
            AuthKeyringBackendKind::default(),
            codex_exec_server::Environment::default_for_tests().get_http_client(),
            /*auth_provider*/ None,
            McpProtocolMode::Legacy,
        )
        .await
        .expect("client should construct");
        let client = Arc::new(client);

        let result = start_server_task(
            "thread-identity-containment-test".to_string(),
            Arc::clone(&client),
            StartServerTaskParams {
                is_codex_apps_mcp_server: false,
                startup_timeout: Some(Duration::from_secs(5)),
                tx_event: None,
                elicitation_requests: ElicitationRequestManager::new(
                    AskForApproval::default(),
                    PermissionProfile::read_only(),
                    /*reviewer*/ None,
                    /*lifecycle*/ None,
                    ElicitationRequestRouter::default(),
                ),
                codex_apps_tools_cache_context: None,
                tool_catalog_cache_context: None,
                tool_catalog_fetch_ticket: None,
                client_elicitation_capability: ElicitationCapability::default(),
                client_mcp_extensions,
                catalog_item_limit: 100,
                thread_identity_eligible: true,
                canonical_thread_id: THREAD_ID_MARKER.to_string(),
            },
        )
        .await;
        assert!(
            result.is_ok(),
            "expected full startup to succeed: {:?}",
            result.as_ref().err().map(ToString::to_string)
        );

        let received = server
            .received_requests()
            .await
            .expect("mock server should record requests");
        assert!(
            received.len() >= 4,
            "expected at least initialize, notifications/initialized, bind, and tools/list; got {}",
            received.len()
        );

        let mut bind_requests_with_marker = 0;
        for request in &received {
            let body_text = String::from_utf8_lossy(&request.body);
            let contains_marker = body_text.contains(THREAD_ID_MARKER)
                || request.url.as_str().contains(THREAD_ID_MARKER)
                || request
                    .headers
                    .iter()
                    .any(|(_, value)| value.to_str().unwrap_or("").contains(THREAD_ID_MARKER));
            let method = serde_json::from_slice::<serde_json::Value>(&request.body)
                .ok()
                .and_then(|body| {
                    body.get("method")
                        .and_then(|m| m.as_str())
                        .map(str::to_string)
                });
            if contains_marker {
                assert_eq!(
                    method.as_deref(),
                    Some(MCP_THREAD_IDENTITY_CAPABILITY),
                    "thread ID marker leaked into a request other than the bind request: \
                     method={method:?} url={} body={body_text}",
                    request.url
                );
                bind_requests_with_marker += 1;
            }
        }
        assert_eq!(
            bind_requests_with_marker, 1,
            "the thread ID marker must appear in exactly the one bind request"
        );

        client.shutdown().await;
    }

    #[test]
    fn thread_identity_versions_accepts_array_containing_one() {
        assert!(is_compatible_thread_identity_versions(&serde_json::json!(
            [1]
        )));
        assert!(is_compatible_thread_identity_versions(&serde_json::json!(
            [2, 1]
        )));
    }

    #[test]
    fn thread_identity_versions_rejects_missing_one() {
        assert!(!is_compatible_thread_identity_versions(&serde_json::json!(
            [2]
        )));
        assert!(!is_compatible_thread_identity_versions(&serde_json::json!(
            []
        )));
    }

    #[test]
    fn thread_identity_versions_rejects_wrong_type() {
        assert!(!is_compatible_thread_identity_versions(&serde_json::json!(
            "1"
        )));
        assert!(!is_compatible_thread_identity_versions(&serde_json::json!(
            1
        )));
        assert!(!is_compatible_thread_identity_versions(&serde_json::json!(
            null
        )));
    }

    #[test]
    fn thread_identity_versions_rejects_malformed_list() {
        // Duplicate entries.
        assert!(!is_compatible_thread_identity_versions(&serde_json::json!(
            [1, 1]
        )));
        // Non-positive entry.
        assert!(!is_compatible_thread_identity_versions(&serde_json::json!(
            [0, 1]
        )));
        assert!(!is_compatible_thread_identity_versions(&serde_json::json!(
            [-1, 1]
        )));
        // Non-integer entries.
        assert!(!is_compatible_thread_identity_versions(&serde_json::json!(
            [1, "x"]
        )));
        assert!(!is_compatible_thread_identity_versions(&serde_json::json!(
            [1.5]
        )));
    }

    #[test]
    fn startup_outcome_error_identifies_authentication_required() {
        let error = anyhow::Error::new(AuthError::AuthorizationRequired)
            .context("failed to initialize MCP server");

        let error = StartupOutcomeError::from(error);

        assert!(error.is_authentication_required());
    }

    #[test]
    fn missing_server_implementation_uses_configured_server_name() {
        assert_eq!(
            mcp_server_info_from_implementation("configured-server", /*server_info*/ None),
            McpServerInfo {
                name: "configured-server".to_string(),
                title: None,
                version: String::new(),
                description: None,
                icons: None,
                website_url: None,
            }
        );
    }

    #[test]
    fn advertised_server_implementation_takes_precedence_over_configured_name() {
        assert_eq!(
            mcp_server_info_from_implementation(
                "configured-server",
                Some(
                    Implementation::new("advertised-server", "1.2.3")
                        .with_title("Advertised server")
                        .with_description("Advertised description")
                        .with_website_url("https://example.com"),
                ),
            ),
            McpServerInfo {
                name: "advertised-server".to_string(),
                title: Some("Advertised server".to_string()),
                version: "1.2.3".to_string(),
                description: Some("Advertised description".to_string()),
                icons: None,
                website_url: Some("https://example.com".to_string()),
            }
        );
    }

    #[test]
    fn mcp_initialize_advertises_client_extensions() {
        let unsupported = mcp_initialize_request_params(
            ElicitationCapability::default(),
            ClientMcpExtensions::default(),
        );
        assert_eq!(unsupported.capabilities.extensions, None);

        let app_ui = serde_json::json!({
            "mimeTypes": ["text/html;profile=mcp-app"],
            "futureField": {"preserved": true},
        });
        let supported = mcp_initialize_request_params(
            ElicitationCapability::default(),
            ClientMcpExtensions::new([
                (OPENAI_FORM_EXTENSION_ID.to_string(), serde_json::json!({})),
                (MCP_APP_UI_EXTENSION_ID.to_string(), app_ui.clone()),
            ]),
        );
        assert_eq!(
            supported.capabilities.extensions,
            Some(BTreeMap::from([
                (OPENAI_FORM_EXTENSION_ID.to_string(), JsonObject::new()),
                (
                    MCP_APP_UI_EXTENSION_ID.to_string(),
                    app_ui.as_object().cloned().expect("app UI settings"),
                ),
            ]))
        );
    }

    fn tool_with_connector_meta() -> RmcpTool {
        RmcpTool::new(
            "capture_file_upload",
            "test tool",
            Arc::new(JsonObject::default()),
        )
        .with_meta(MetaObject(
            serde_json::json!({
                "connector_id": "connector_gmail",
                "connector_name": "Gmail",
                "connector_display_name": "Gmail",
                "connector_description": "Mail connector",
                "connectorDescription": "Mail connector",
                "connectorFutureField": "future connector metadata",
                "CONNECTOR_UPPERCASE": "uppercase connector metadata",
                "openai/fileParams": ["file"],
                "custom": "kept"
            })
            .as_object()
            .expect("object")
            .clone(),
        ))
    }

    #[test]
    fn custom_mcp_connector_metadata_is_stripped() {
        let mut tool = tool_with_connector_meta();

        strip_untrusted_connector_meta(&mut tool);

        let meta = tool.meta.as_ref().expect("meta");
        for key in [
            "connector_id",
            "connector_name",
            "connector_display_name",
            "connector_description",
            "connectorDescription",
        ] {
            assert!(!meta.0.contains_key(key), "{key} should be stripped");
        }
        assert!(meta.0.contains_key("connectorFutureField"));
        assert!(meta.0.contains_key("CONNECTOR_UPPERCASE"));
        assert!(meta.0.contains_key("openai/fileParams"));
        assert_eq!(
            meta.0.get("custom").and_then(|value| value.as_str()),
            Some("kept")
        );
    }

    #[test]
    fn codex_apps_connector_metadata_is_preserved() {
        let tool = tool_with_connector_meta();
        let expected_tool = tool.clone();

        let tool_info = tool_info_from_listed_tool(
            CODEX_APPS_MCP_SERVER_NAME,
            /*is_codex_apps_mcp_server*/ true,
            /*server_instructions*/ None,
            ToolWithConnectorId {
                tool,
                connector_id: Some("connector_gmail".to_string()),
                connector_name: Some("Gmail".to_string()),
                connector_description: Some("Mail connector".to_string()),
            },
        );

        let expected = ToolInfo {
            server_name: CODEX_APPS_MCP_SERVER_NAME.to_string(),
            supports_parallel_tool_calls: false,
            server_origin: None,
            callable_name: "capture_file_upload".to_string(),
            callable_namespace: "codex_apps__gmail".to_string(),
            namespace_description: Some("Mail connector".to_string()),
            tool: expected_tool,
            openai_file_input_optional_fields: HashMap::new(),
            connector_id: Some("connector_gmail".to_string()),
            connector_name: Some("Gmail".to_string()),
            plugin_display_names: Vec::new(),
        };
        assert_eq!(
            serde_json::to_value(tool_info).expect("serialize actual tool info"),
            serde_json::to_value(expected).expect("serialize expected tool info")
        );
    }
}
