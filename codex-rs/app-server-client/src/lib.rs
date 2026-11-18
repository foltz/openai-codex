// Release builds exceed rustc's default query depth when laying out the async
// MessageProcessor::handle_initialized_client_request future.
#![recursion_limit = "256"]

//! Shared in-process app-server client facade for CLI surfaces.
//!
//! This crate wraps [`codex_app_server::in_process`] behind a single async API
//! used by surfaces like TUI and exec. It centralizes:
//!
//! - Runtime startup and initialize-capabilities handshake.
//! - Typed caller-provided startup identity (`SessionSource` + client name).
//! - Typed and raw request/notification dispatch.
//! - Server request resolution and rejection.
//! - Ordered, lossless event consumption that cannot block request processing.
//! - Bounded shutdown observation with retained original-task custody.
//!
//! The facade interposes a worker task between the caller and the underlying
//! [`InProcessClientHandle`](codex_app_server::in_process::InProcessClientHandle),
//! bridging async `mpsc` channels on both sides. Commands and the underlying
//! runtime remain bounded; the local consumer event queue is unbounded so
//! unread notifications cannot prevent request responses from being delivered.

mod cloud;
pub mod cloud_backend;
pub use codex_cloud_client::ProjectEnvironment;
mod path;
mod remote;
mod remote_websocket;
pub use cloud::CloudClient;
pub use cloud::CloudCommand;
pub use cloud::CloudOperation;
pub use cloud::CloudRequestHandle;
pub use cloud::CloudUserMessageReceipt;
mod server_request;
pub use server_request::AppServerReplyHandle;

use std::error::Error;
use std::fmt;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::io::Result as IoResult;
use std::sync::Arc;
use std::time::Duration;

pub use codex_app_server::app_server_control_socket_path;
pub use codex_app_server::in_process::DEFAULT_IN_PROCESS_CHANNEL_CAPACITY;
pub use codex_app_server::in_process::EmbeddedNetworkPolicy;
pub use codex_app_server::in_process::InProcessHost;
pub use codex_app_server::in_process::InProcessServerEvent;
use codex_app_server::in_process::InProcessStartArgs;
use codex_app_server::in_process::LogDbLayer;
pub use codex_app_server::in_process::StateDbHandle;
pub use codex_app_server::verified_local_daemon_socket;
use codex_app_server_protocol::ClientInfo;
use codex_app_server_protocol::ClientNotification;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::ConfigWarningNotification;
use codex_app_server_protocol::InitializeCapabilities;
use codex_app_server_protocol::InitializeParams;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::Result as JsonRpcResult;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ServerRequest;
use codex_arg0::Arg0DispatchPaths;
use codex_config::CloudConfigBundleLoader;
use codex_config::LoaderOverrides;
use codex_config::NoopThreadConfigLoader;
use codex_core::config::Config;
pub use codex_core::otel_init::build_provider as build_otel_provider;
pub use codex_exec_server::EnvironmentManager;
pub use codex_exec_server::ExecServerRuntimeOptions;
use codex_feedback::CodexFeedback;
use codex_protocol::protocol::SessionSource;
use codex_utils_absolute_path::AbsolutePathBuf;
use serde::de::DeserializeOwned;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::time::Instant;
#[cfg(test)]
use tokio::time::timeout;
use tokio::time::timeout_at;
use toml::Value as TomlValue;
use tracing::warn;

pub use crate::path::AppServerPath;
pub use crate::remote::FinalClose;
pub use crate::remote::RemoteAppServerClient;
pub use crate::remote::RemoteAppServerConnectArgs;
pub use crate::remote::RemoteAppServerEndpoint;

/// Transitional access to core-only embedded app-server types.
///
/// New TUI behavior should prefer the app-server protocol methods. This
/// module exists so clients can remove a direct `codex-core` dependency
/// while legacy startup/config paths are migrated to RPCs.
pub mod legacy_core {
    pub mod config {
        pub use codex_core::config::*;

        pub mod edit {
            pub use codex_core::config::edit::*;
        }
    }
}

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
// Covers the embedded drain, its analytics flush, and final task join.
const IN_PROCESS_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(45);

/// Raw app-server request result for typed in-process requests.
///
/// Even on the in-process path, successful responses still travel back through
/// the same JSON-RPC result envelope used by socket/stdio transports because
/// `MessageProcessor` continues to produce that shape internally.
pub type RequestResult = std::result::Result<JsonRpcResult, JSONRPCErrorError>;

#[derive(Debug, Clone)]
pub enum AppServerEvent {
    Lagged {
        skipped: usize,
    },
    ServerNotification(Box<ServerNotification>),
    ServerRequest(Box<ServerRequest>),
    /// A native Cloud reply stream ended. The underlying thread can still be running.
    ThreadStreamDisconnected(Box<codex_app_server_protocol::ErrorNotification>),
    Disconnected {
        message: String,
    },
}

impl From<InProcessServerEvent> for AppServerEvent {
    fn from(value: InProcessServerEvent) -> Self {
        match value {
            InProcessServerEvent::Lagged { skipped } => Self::Lagged { skipped },
            InProcessServerEvent::ServerNotification(notification) => {
                Self::ServerNotification(notification)
            }
            InProcessServerEvent::ServerRequest(request) => Self::ServerRequest(request),
        }
    }
}

/// Layered error for [`InProcessAppServerClient::request_typed`].
///
/// This keeps transport failures, server-side JSON-RPC failures, and response
/// decode failures distinct so callers can decide whether to retry, surface a
/// server error, or treat the response as an internal request/response mismatch.
#[derive(Debug)]
pub enum TypedRequestError {
    Transport {
        method: String,
        source: IoError,
    },
    Server {
        method: String,
        source: JSONRPCErrorError,
    },
    Deserialize {
        method: String,
        source: serde_json::Error,
    },
}

impl fmt::Display for TypedRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport { method, source } => {
                write!(f, "{method} transport error: {source}")
            }
            Self::Server { method, source } => {
                write!(
                    f,
                    "{method} failed: {} (code {})",
                    source.message, source.code
                )?;
                if let Some(data) = source.data.as_ref() {
                    write!(f, ", data: {data}")?;
                }
                Ok(())
            }
            Self::Deserialize { method, source } => {
                write!(f, "{method} response decode error: {source}")
            }
        }
    }
}

impl Error for TypedRequestError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Transport { source, .. } => Some(source),
            Self::Server { .. } => None,
            Self::Deserialize { source, .. } => Some(source),
        }
    }
}

// Await transport in a separate statement before calling this: dropping the
// completed future must precede any caller-provided Deserialize implementation.
fn decode_typed_response<T>(
    method: &str,
    response: IoResult<RequestResult>,
) -> Result<T, TypedRequestError>
where
    T: DeserializeOwned,
{
    let response = response.map_err(|source| TypedRequestError::Transport {
        method: method.to_string(),
        source,
    })?;
    let result = response.map_err(|source| TypedRequestError::Server {
        method: method.to_string(),
        source,
    })?;
    serde_json::from_value(result).map_err(|source| TypedRequestError::Deserialize {
        method: method.to_string(),
        source,
    })
}

#[derive(Clone)]
pub struct InProcessClientStartArgs {
    /// This client collects fresh human decisions for managed command checkpoints.
    pub required_human_approval: bool,
    /// Resolved argv0 dispatch paths used by command execution internals.
    pub arg0_paths: Arg0DispatchPaths,
    /// Shared config used to initialize app-server runtime.
    pub config: Arc<Config>,
    /// CLI config overrides that are already parsed into TOML values.
    pub cli_overrides: Vec<(String, TomlValue)>,
    /// Loader override knobs used by config API paths.
    pub loader_overrides: LoaderOverrides,
    /// Whether config API paths should reject unknown config fields.
    pub strict_config: bool,
    /// Preloaded cloud config bundle provider.
    pub cloud_config_bundle: CloudConfigBundleLoader,
    /// Policy shared with transports created by the embedder before startup.
    pub embedded_network_policy: EmbeddedNetworkPolicy,
    /// Feedback sink used by app-server/core telemetry and logs.
    pub feedback: CodexFeedback,
    /// SQLite tracing layer used to flush recently emitted logs before feedback upload.
    pub log_db: Option<LogDbLayer>,
    /// Process-wide SQLite state handle shared with the embedded app-server.
    pub state_db: Option<StateDbHandle>,
    /// Environment manager used by core execution and filesystem operations.
    pub environment_manager: Arc<EnvironmentManager>,
    /// Startup warnings emitted after initialize succeeds.
    pub config_warnings: Vec<ConfigWarningNotification>,
    /// Session source recorded in app-server thread metadata.
    pub session_source: SessionSource,
    /// Whether auth loading should honor the `CODEX_API_KEY` environment variable.
    pub enable_codex_api_key_env: bool,
    /// Client name reported during initialize.
    pub client_name: String,
    /// Client version reported during initialize.
    pub client_version: String,
    /// Whether experimental APIs are requested at initialize time.
    pub experimental_api: bool,
    /// Whether MCP servers may send `openai/form` elicitation requests.
    pub mcp_server_openai_form_elicitation: bool,
    /// Notification methods this client opts out of receiving.
    pub opt_out_notification_methods: Vec<String>,
    /// Queue capacity for command and embedded-runtime channels (clamped to at least 1).
    pub channel_capacity: usize,
}

impl InProcessClientStartArgs {
    /// Builds initialize params from caller-provided metadata.
    pub fn initialize_params(&self) -> InitializeParams {
        let capabilities = InitializeCapabilities {
            interactive_client: false,
            required_human_approval: self.required_human_approval,
            explicit_gateway_oauth: false,
            experimental_api: self.experimental_api,
            request_attestation: false,
            extensions: None,
            opt_out_notification_methods: if self.opt_out_notification_methods.is_empty() {
                None
            } else {
                Some(self.opt_out_notification_methods.clone())
            },
            mcp_server_openai_form_elicitation: self.mcp_server_openai_form_elicitation,
        };

        InitializeParams {
            client_info: ClientInfo {
                name: self.client_name.clone(),
                title: None,
                version: self.client_version.clone(),
            },
            capabilities: Some(capabilities),
        }
    }

    fn into_runtime_start_args(self) -> InProcessStartArgs {
        let initialize = self.initialize_params();
        InProcessStartArgs {
            arg0_paths: self.arg0_paths,
            config: self.config,
            cli_overrides: self.cli_overrides,
            loader_overrides: self.loader_overrides,
            strict_config: self.strict_config,
            cloud_config_bundle: self.cloud_config_bundle,
            embedded_network_policy: self.embedded_network_policy,
            thread_config_loader: Arc::new(NoopThreadConfigLoader),
            feedback: self.feedback,
            log_db: self.log_db,
            state_db: self.state_db,
            environment_manager: self.environment_manager,
            config_warnings: self.config_warnings,
            session_source: self.session_source,
            enable_codex_api_key_env: self.enable_codex_api_key_env,
            initialize,
            channel_capacity: self.channel_capacity,
        }
    }
}

/// Internal command sent from public facade methods to the worker task.
///
/// Each variant carries a oneshot sender so the caller can `await` the
/// result without holding a mutable reference to the client.
enum ClientCommand {
    Request {
        request: Box<ClientRequest>,
        response_tx: oneshot::Sender<IoResult<RequestResult>>,
    },
    Notify {
        notification: ClientNotification,
        response_tx: oneshot::Sender<IoResult<()>>,
    },
    ResolveServerRequest {
        request_id: RequestId,
        result: JsonRpcResult,
        response_tx: oneshot::Sender<IoResult<()>>,
    },
    RejectServerRequest {
        request_id: RequestId,
        error: JSONRPCErrorError,
        response_tx: oneshot::Sender<IoResult<()>>,
    },
    Shutdown {
        response_tx: oneshot::Sender<IoResult<()>>,
        deadline: Instant,
    },
}

/// Async facade over the in-process app-server runtime.
///
/// This type owns a worker task that bridges between:
/// - caller-facing async `mpsc` channels used by TUI/exec
/// - [`codex_app_server::in_process::InProcessClientHandle`], which speaks to
///   the embedded `MessageProcessor`
///
/// The facade intentionally preserves the server's request/notification/event
/// model instead of exposing direct core runtime handles. That keeps in-process
/// callers aligned with app-server behavior while still avoiding a process
/// boundary.
pub struct InProcessAppServerClient {
    command_tx: mpsc::Sender<ClientCommand>,
    event_rx: mpsc::UnboundedReceiver<InProcessServerEvent>,
    worker_handle: tokio::task::JoinHandle<()>,
    // Retain original runtime custody even after a bounded observer returns.
    _host: Arc<InProcessHost>,
}

#[derive(Clone)]
pub struct InProcessAppServerRequestHandle {
    command_tx: mpsc::Sender<ClientCommand>,
}

#[derive(Clone)]
pub enum AppServerRequestHandle {
    Cloud(CloudRequestHandle),
    InProcess(InProcessAppServerRequestHandle),
    Remote(crate::remote::RemoteAppServerRequestHandle),
}

pub enum AppServerClient {
    Cloud(CloudClient),
    InProcess(InProcessAppServerClient),
    Remote(RemoteAppServerClient),
}

/// Answers a command the worker will not run because its owner is gone.
fn fail_closed_command(command: ClientCommand) {
    let error = || {
        IoError::new(
            ErrorKind::BrokenPipe,
            "in-process app-server client was dropped",
        )
    };
    match command {
        ClientCommand::Request { response_tx, .. } => {
            let _ = response_tx.send(Err(error()));
        }
        ClientCommand::Notify { response_tx, .. }
        | ClientCommand::ResolveServerRequest { response_tx, .. }
        | ClientCommand::RejectServerRequest { response_tx, .. }
        | ClientCommand::Shutdown { response_tx, .. } => {
            let _ = response_tx.send(Err(error()));
        }
    }
}

impl InProcessAppServerClient {
    /// Starts the in-process runtime and facade worker task.
    ///
    /// The returned client is ready for requests and ordered event consumption.
    /// Request queues remain bounded without blocking on unread notifications.
    #[deprecated(note = "use start_in_host with host custody retained by the embedding caller")]
    pub async fn start(args: InProcessClientStartArgs) -> IoResult<Self> {
        let host = Arc::new(InProcessHost::default());
        let result = Self::start_in_host(Arc::clone(&host), args).await;
        drop(host);
        result
    }

    /// Starts an embedded client with custody supplied before the first
    /// cancellable await. The caller must retain the host through startup and
    /// any incomplete shutdown report; the worker also retains it while the
    /// facade is alive so a timed-out observer cannot discard runtime custody.
    pub async fn start_in_host(
        host: Arc<InProcessHost>,
        args: InProcessClientStartArgs,
    ) -> IoResult<Self> {
        let channel_capacity = args.channel_capacity.max(1);
        let runtime_args = args.into_runtime_start_args();
        let mut handle = codex_app_server::in_process::start_in_host(&host, runtime_args).await?;
        let request_sender = handle.sender();
        let (command_tx, mut command_rx) = mpsc::channel::<ClientCommand>(channel_capacity);
        // e9996ec62a preserved transcript events by awaiting a bounded queue, but that can
        // deadlock a foreground request whose response is behind unread notifications.
        // Match the remote-client fix in 79ea57715636: only this local consumer queue is
        // unbounded; commands and the embedded runtime stay bounded and events remain ordered.
        let (event_tx, event_rx) = mpsc::unbounded_channel::<InProcessServerEvent>();
        // Set only when the event consumer is dropped without an explicit
        // shutdown. Detached request tasks then fail with BrokenPipe instead
        // of keeping a dropped owner's work alive.
        let (owner_dropped_tx, owner_dropped_rx) = watch::channel(false);

        let worker_host = Arc::clone(&host);
        let worker_handle = tokio::spawn(async move {
            // Keep the host alive if the caller's bounded observer returns
            // incomplete and drops its JoinHandle. The worker owns the same
            // low-level client/host pair and remains the durable observer.
            let _host = worker_host;
            loop {
                tokio::select! {
                    // Consumer-closed first: once the owner is gone no further
                    // command is admitted, however fast retained handles send.
                    biased;
                    () = event_tx.closed() => {
                        // Cloned request handles do not own the runtime lifetime.
                        // Stop admitting commands first, so retained handles that
                        // keep sending cannot delay this exit; the drain is then
                        // bounded by what is already buffered. Explicit shutdown
                        // queues its Shutdown before releasing the consumer, so if
                        // one exists it is buffered here.
                        command_rx.close();
                        let mut explicit_shutdown = None;
                        while let Ok(command) = command_rx.try_recv() {
                            match command {
                                ClientCommand::Shutdown { response_tx, deadline } => {
                                    explicit_shutdown = Some((response_tx, deadline));
                                }
                                command => fail_closed_command(command),
                            }
                        }
                        match explicit_shutdown {
                            Some((response_tx, deadline)) => {
                                let shutdown_result = handle.shutdown_until(deadline).await;
                                let _ = response_tx.send(shutdown_result);
                            }
                            None => {
                                let _ = owner_dropped_tx.send_replace(true);
                                // Same bounded runtime shutdown as the closed
                                // command-channel path. Its cleanup closes the
                                // embedded connection, which revokes grants.
                                let _ = handle.shutdown().await;
                            }
                        }
                        break;
                    }
                    command = command_rx.recv() => {
                        match command {
                            Some(ClientCommand::Request { request, response_tx }) => {
                                let request_sender = request_sender.clone();
                                let mut owner_dropped = owner_dropped_rx.clone();
                                // Request waits happen on a detached task so
                                // this loop can keep draining runtime events
                                // while the request is blocked on client input.
                                tokio::spawn(async move {
                                    // Device ceremonies belong to the waiting UI. Preserve
                                    // its cancellation through this buffering task.
                                    let cancellable = matches!(*request,
                                        ClientRequest::UserVerificationStatus { .. }
                                        | ClientRequest::UserVerificationEnroll { .. }
                                        | ClientRequest::UserVerificationDelete { .. }
                                        | ClientRequest::UserVerificationVerify { .. });
                                    let mut response_tx = response_tx;
                                    tokio::select! {
                                        // An owner drop beats a response that became
                                        // ready in the same poll: that response would
                                        // otherwise be the runtime's shutdown error.
                                        biased;
                                        _ = response_tx.closed(), if cancellable => {}
                                        // `false` means the worker exited without an
                                        // owner drop; that disables only this branch.
                                        true = async move {
                                            owner_dropped.wait_for(|dropped| *dropped).await.is_ok()
                                        } => {
                                            let _ = response_tx.send(Err(IoError::new(
                                                ErrorKind::BrokenPipe,
                                                "in-process app-server client was dropped",
                                            )));
                                        }
                                        result = request_sender.request(*request) => {
                                            let _ = response_tx.send(result);
                                        }
                                    }
                                });
                            }
                            Some(ClientCommand::Notify {
                                notification,
                                response_tx,
                            }) => {
                                let result = request_sender.notify(notification);
                                let _ = response_tx.send(result);
                            }
                            Some(ClientCommand::ResolveServerRequest {
                                request_id,
                                result,
                                response_tx,
                            }) => {
                                let send_result =
                                    request_sender.respond_to_server_request(request_id, result);
                                let _ = response_tx.send(send_result);
                            }
                            Some(ClientCommand::RejectServerRequest {
                                request_id,
                                error,
                                response_tx,
                            }) => {
                                let send_result = request_sender.fail_server_request(request_id, error);
                                let _ = response_tx.send(send_result);
                            }
                            Some(ClientCommand::Shutdown {
                                response_tx,
                                deadline,
                            }) => {
                                let shutdown_result = handle.shutdown_until(deadline).await;
                                let _ = response_tx.send(shutdown_result);
                                break;
                            }
                            None => {
                                let _ = handle.shutdown().await;
                                break;
                            }
                        }
                    }
                    event = handle.next_event() => {
                        let Some(event) = event else {
                            break;
                        };
                        if let InProcessServerEvent::ServerRequest(request) = &event
                            && let ServerRequest::ChatgptAuthTokensRefresh { request_id, .. } =
                                request.as_ref()
                        {
                            let send_result = request_sender.fail_server_request(
                                request_id.clone(),
                                JSONRPCErrorError {
                                    code: -32000,
                                    message: "chatgpt auth token refresh is not supported for in-process app-server clients".to_string(),
                                    data: None,
                                },
                            );
                            if let Err(err) = send_result {
                                warn!(
                                    "failed to reject unsupported chatgpt auth token refresh request: {err}"
                                );
                            }
                            continue;
                        }

                        // A failed send means the consumer is gone; the biased
                        // `event_tx.closed()` branch exits on the next iteration.
                        let _ = event_tx.send(event);
                    }
                }
            }
        });

        Ok(Self {
            command_tx,
            event_rx,
            worker_handle,
            _host: host,
        })
    }

    pub fn request_handle(&self) -> InProcessAppServerRequestHandle {
        InProcessAppServerRequestHandle {
            command_tx: self.command_tx.clone(),
        }
    }

    /// Sends a typed client request and returns raw JSON-RPC result.
    ///
    /// Callers that expect a concrete response type should usually prefer
    /// [`request_typed`](Self::request_typed).
    pub async fn request(&self, request: ClientRequest) -> IoResult<RequestResult> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(ClientCommand::Request {
                request: Box::new(request),
                response_tx,
            })
            .await
            .map_err(|_| {
                IoError::new(
                    ErrorKind::BrokenPipe,
                    "in-process app-server worker channel is closed",
                )
            })?;
        response_rx.await.map_err(|_| {
            IoError::new(
                ErrorKind::BrokenPipe,
                "in-process app-server request channel is closed",
            )
        })?
    }

    /// Sends a typed client request and decodes the successful response body.
    ///
    /// This still deserializes from a JSON value produced by app-server's
    /// JSON-RPC result envelope. Because the caller chooses `T`, `Deserialize`
    /// failures indicate an internal request/response mismatch at the call site
    /// (or an in-process bug), not transport skew from an external client.
    pub async fn request_typed<T>(&self, request: ClientRequest) -> Result<T, TypedRequestError>
    where
        T: DeserializeOwned,
    {
        let method = request.method_name();
        let response = self.request(request).await;
        decode_typed_response(method, response)
    }

    /// Sends a typed client notification.
    pub async fn notify(&self, notification: ClientNotification) -> IoResult<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(ClientCommand::Notify {
                notification,
                response_tx,
            })
            .await
            .map_err(|_| {
                IoError::new(
                    ErrorKind::BrokenPipe,
                    "in-process app-server worker channel is closed",
                )
            })?;
        response_rx.await.map_err(|_| {
            IoError::new(
                ErrorKind::BrokenPipe,
                "in-process app-server notify channel is closed",
            )
        })?
    }

    /// Resolves a pending server request.
    ///
    /// This should only be called with request IDs obtained from the current
    /// client's event stream.
    pub async fn resolve_server_request(
        &self,
        request_id: RequestId,
        result: JsonRpcResult,
    ) -> IoResult<()> {
        self.request_handle()
            .resolve_server_request(request_id, result)
            .await
    }

    /// Rejects a pending server request with JSON-RPC error payload.
    pub async fn reject_server_request(
        &self,
        request_id: RequestId,
        error: JSONRPCErrorError,
    ) -> IoResult<()> {
        self.request_handle()
            .reject_server_request(request_id, error)
            .await
    }

    /// Returns the next in-process event, or `None` when worker exits.
    ///
    /// Events remain ordered and are retained while callers await requests.
    pub async fn next_event(&mut self) -> Option<InProcessServerEvent> {
        self.event_rx.recv().await
    }

    /// Shuts down worker and in-process runtime with bounded wait.
    ///
    /// If graceful shutdown exceeds the bound, returns an incomplete I/O
    /// error. The worker is deliberately not aborted: its host-owned runtime
    /// and cleanup receipts remain observable after this caller returns.
    pub async fn shutdown(self) -> IoResult<()> {
        self.shutdown_until(Instant::now() + IN_PROCESS_SHUTDOWN_TIMEOUT)
            .await
    }

    async fn shutdown_until(self, deadline: Instant) -> IoResult<()> {
        let Self {
            command_tx,
            event_rx,
            worker_handle,
            _host,
        } = self;
        let mut worker_handle = worker_handle;
        // Keep the event consumer alive until the worker has been observed or
        // the deadline elapsed. Dropping it first would let the worker's
        // owner-drop exit win before this caller-deadlined Shutdown is queued.
        let mut command_error = None;
        let (response_tx, response_rx) = oneshot::channel();
        match timeout_at(
            deadline,
            command_tx.send(ClientCommand::Shutdown {
                response_tx,
                deadline,
            }),
        )
        .await
        {
            Ok(Ok(())) => match timeout_at(deadline, response_rx).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => command_error = Some(error),
                Ok(Err(_)) => {
                    command_error = Some(IoError::new(
                        ErrorKind::BrokenPipe,
                        "in-process app-server shutdown channel is closed",
                    ));
                }
                Err(_) => {
                    command_error = Some(IoError::new(
                        ErrorKind::TimedOut,
                        "in-process app-server shutdown acknowledgement timed out",
                    ));
                }
            },
            Ok(Err(_)) => {
                command_error = Some(IoError::new(
                    ErrorKind::BrokenPipe,
                    "in-process app-server worker channel is closed",
                ));
            }
            Err(_) => {
                command_error = Some(IoError::new(
                    ErrorKind::TimedOut,
                    "in-process app-server shutdown command timed out",
                ));
            }
        }

        let result = match timeout_at(deadline, &mut worker_handle).await {
            Ok(Ok(())) => command_error.map_or(Ok(()), Err),
            Ok(Err(_)) => Err(IoError::other("in-process app-server worker panicked")),
            Err(_) => Err(IoError::new(
                ErrorKind::TimedOut,
                "in-process app-server worker shutdown timed out",
            )),
        };
        drop(event_rx);
        result
    }
}

impl InProcessAppServerRequestHandle {
    pub(crate) async fn resolve_server_request(
        &self,
        request_id: RequestId,
        result: JsonRpcResult,
    ) -> IoResult<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(ClientCommand::ResolveServerRequest {
                request_id,
                result,
                response_tx,
            })
            .await
            .map_err(|_| {
                IoError::new(
                    ErrorKind::BrokenPipe,
                    "in-process app-server worker channel is closed",
                )
            })?;
        response_rx.await.map_err(|_| {
            IoError::new(
                ErrorKind::BrokenPipe,
                "in-process app-server resolve channel is closed",
            )
        })?
    }

    /// Rejects a pending server request with JSON-RPC error payload.
    pub(crate) async fn reject_server_request(
        &self,
        request_id: RequestId,
        error: JSONRPCErrorError,
    ) -> IoResult<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(ClientCommand::RejectServerRequest {
                request_id,
                error,
                response_tx,
            })
            .await
            .map_err(|_| {
                IoError::new(
                    ErrorKind::BrokenPipe,
                    "in-process app-server worker channel is closed",
                )
            })?;
        response_rx.await.map_err(|_| {
            IoError::new(
                ErrorKind::BrokenPipe,
                "in-process app-server reject channel is closed",
            )
        })?
    }

    pub async fn request(&self, request: ClientRequest) -> IoResult<RequestResult> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(ClientCommand::Request {
                request: Box::new(request),
                response_tx,
            })
            .await
            .map_err(|_| {
                IoError::new(
                    ErrorKind::BrokenPipe,
                    "in-process app-server worker channel is closed",
                )
            })?;
        response_rx.await.map_err(|_| {
            IoError::new(
                ErrorKind::BrokenPipe,
                "in-process app-server request channel is closed",
            )
        })?
    }

    pub async fn request_typed<T>(&self, request: ClientRequest) -> Result<T, TypedRequestError>
    where
        T: DeserializeOwned,
    {
        let method = request.method_name();
        let response = self.request(request).await;
        decode_typed_response(method, response)
    }
}

impl AppServerRequestHandle {
    /// Starts a native cloud thread with host project metadata without adding wire fields.
    pub async fn start_cloud_project_thread(
        &self,
        request_id: RequestId,
        params: codex_app_server_protocol::ThreadStartParams,
        project: codex_cloud_client::ProjectEnvironment,
    ) -> Result<codex_app_server_protocol::ThreadStartResponse, TypedRequestError> {
        let response = match self {
            Self::Cloud(handle) => {
                handle
                    .start_project_thread(request_id, params, project)
                    .await
            }
            Self::InProcess(_) | Self::Remote(_) => Err(IoError::new(
                std::io::ErrorKind::Unsupported,
                "Project checkout requires the native cloud transport.",
            )),
        };
        decode_typed_response("thread/start", response)
    }

    pub(crate) async fn resolve_server_request(
        &self,
        request_id: RequestId,
        result: JsonRpcResult,
    ) -> IoResult<()> {
        match self {
            Self::InProcess(handle) => handle.resolve_server_request(request_id, result).await,
            Self::Remote(handle) => handle.resolve_server_request(request_id, result).await,
            Self::Cloud(handle) => handle.resolve_server_request(request_id, result).await,
        }
    }

    pub(crate) async fn reject_server_request(
        &self,
        request_id: RequestId,
        error: JSONRPCErrorError,
    ) -> IoResult<()> {
        match self {
            Self::InProcess(handle) => handle.reject_server_request(request_id, error).await,
            Self::Remote(handle) => handle.reject_server_request(request_id, error).await,
            Self::Cloud(handle) => handle.reject_server_request(request_id, error).await,
        }
    }

    pub async fn request(&self, request: ClientRequest) -> IoResult<RequestResult> {
        match self {
            Self::InProcess(handle) => handle.request(request).await,
            Self::Remote(handle) => handle.request(request).await,
            Self::Cloud(handle) => handle.request(request).await,
        }
    }

    pub async fn request_typed<T>(&self, request: ClientRequest) -> Result<T, TypedRequestError>
    where
        T: DeserializeOwned,
    {
        let method = request.method_name();
        let response = self.request(request).await;
        decode_typed_response(method, response)
    }
}

impl AppServerClient {
    /// App-server platform family, which can differ from the executor's platform.
    /// Older remote servers may omit this metadata.
    pub fn platform_family(&self) -> Option<&str> {
        match self {
            Self::InProcess(_) => Some(std::env::consts::FAMILY),
            Self::Remote(client) => client.platform_family(),
            Self::Cloud(_) => None,
        }
    }

    /// App-server operating system as reported at initialization, including unknown values.
    pub fn platform_os(&self) -> Option<&str> {
        match self {
            Self::InProcess(_) => Some(std::env::consts::OS),
            Self::Remote(client) => client.platform_os(),
            Self::Cloud(_) => None,
        }
    }

    pub fn codex_home(&self, local_codex_home: &AbsolutePathBuf) -> Option<AppServerPath> {
        match self {
            Self::InProcess(_) => Some(AppServerPath::from_app_server(
                local_codex_home.display().to_string(),
            )),
            Self::Remote(client) => client.codex_home().map(AppServerPath::from_app_server),
            Self::Cloud(_) => None,
        }
    }

    pub async fn request(&self, request: ClientRequest) -> IoResult<RequestResult> {
        match self {
            Self::InProcess(client) => client.request(request).await,
            Self::Remote(client) => client.request(request).await,
            Self::Cloud(client) => client.request(request).await,
        }
    }

    pub async fn request_typed<T>(&self, request: ClientRequest) -> Result<T, TypedRequestError>
    where
        T: DeserializeOwned,
    {
        let method = request.method_name();
        let response = self.request(request).await;
        decode_typed_response(method, response)
    }

    pub async fn notify(&self, notification: ClientNotification) -> IoResult<()> {
        match self {
            Self::InProcess(client) => client.notify(notification).await,
            Self::Remote(client) => client.notify(notification).await,
            Self::Cloud(client) => client.notify(notification).await,
        }
    }

    pub async fn resolve_server_request(
        &self,
        request_id: RequestId,
        result: JsonRpcResult,
    ) -> IoResult<()> {
        match self {
            Self::InProcess(client) => client.resolve_server_request(request_id, result).await,
            Self::Remote(client) => client.resolve_server_request(request_id, result).await,
            Self::Cloud(client) => client.resolve_server_request(request_id, result).await,
        }
    }

    pub async fn reject_server_request(
        &self,
        request_id: RequestId,
        error: JSONRPCErrorError,
    ) -> IoResult<()> {
        match self {
            Self::InProcess(client) => client.reject_server_request(request_id, error).await,
            Self::Remote(client) => client.reject_server_request(request_id, error).await,
            Self::Cloud(client) => client.reject_server_request(request_id, error).await,
        }
    }

    pub async fn next_event(&mut self) -> Option<AppServerEvent> {
        match self {
            Self::InProcess(client) => client.next_event().await.map(Into::into),
            Self::Remote(client) => client.next_event().await,
            Self::Cloud(client) => client.next_event().await,
        }
    }

    pub fn begin_final_exit(&self, deadline: Instant) {
        if let Self::Remote(client) = self {
            client.begin_final_exit(deadline);
        }
    }

    pub async fn shutdown_by(self, deadline: Instant) -> FinalClose {
        let result = match self {
            Self::Remote(client) => return client.shutdown_by(deadline).await,
            Self::InProcess(client) => client.shutdown_until(deadline).await,
            Self::Cloud(client) => client.shutdown_until(deadline).await,
        };
        match result {
            Ok(()) => FinalClose::Closed,
            Err(error) if error.kind() == ErrorKind::TimedOut => FinalClose::TimedOut(error),
            Err(error) => FinalClose::CloseFailed(error),
        }
    }

    pub async fn shutdown(self) -> IoResult<()> {
        match self {
            Self::InProcess(client) => client.shutdown().await,
            Self::Remote(client) => client.shutdown().await,
            Self::Cloud(client) => client.shutdown().await,
        }
    }

    pub fn request_handle(&self) -> AppServerRequestHandle {
        match self {
            Self::InProcess(client) => AppServerRequestHandle::InProcess(client.request_handle()),
            Self::Remote(client) => AppServerRequestHandle::Remote(client.request_handle()),
            Self::Cloud(client) => AppServerRequestHandle::Cloud(client.request_handle()),
        }
    }
}

#[cfg(test)]
mod tests {
    #[path = "reply_tests.rs"]
    mod replies;
    use super::*;
    use codex_app_server_protocol::AccountUpdatedNotification;
    use codex_app_server_protocol::ConfigRequirementsReadResponse;
    use codex_app_server_protocol::GetAccountResponse;
    use codex_app_server_protocol::JSONRPCMessage;
    use codex_app_server_protocol::JSONRPCRequest;
    use codex_app_server_protocol::JSONRPCResponse;
    use codex_app_server_protocol::ServerNotification;
    use codex_app_server_protocol::SessionSource as ApiSessionSource;
    use codex_app_server_protocol::ThreadSettingsUpdateParams;
    use codex_app_server_protocol::ThreadSettingsUpdateResponse;
    use codex_app_server_protocol::ThreadStartParams;
    use codex_app_server_protocol::ThreadStartResponse;
    use codex_app_server_protocol::ToolRequestUserInputParams;
    use codex_app_server_protocol::ToolRequestUserInputQuestion;
    use codex_core::config::ConfigBuilder;
    use codex_core::init_state_db;
    use codex_protocol::config_types::Personality;
    use codex_uds::UnixListener;
    use codex_utils_absolute_path::AbsolutePathBuf;
    use futures::SinkExt;
    use futures::StreamExt;
    use futures::poll;
    use pretty_assertions::assert_eq;
    use std::ops::Deref;
    use std::path::Path;
    use tempfile::TempDir;
    use tokio::net::TcpListener;
    use tokio::time::Duration;
    use tokio::time::timeout;
    use tokio_tungstenite::accept_async;
    use tokio_tungstenite::accept_hdr_async;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::handshake::server::Request as WebSocketRequest;
    use tokio_tungstenite::tungstenite::handshake::server::Response as WebSocketResponse;
    use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;

    async fn build_test_config() -> Config {
        match ConfigBuilder::default().build().await {
            Ok(config) => config,
            Err(_) => Config::load_default_with_cli_overrides(Vec::new())
                .await
                .expect("default config should load"),
        }
    }

    async fn build_test_config_for_codex_home(codex_home: &Path) -> Config {
        // These local client tests exercise requests and shutdown, not marketplace
        // availability. Network-bound plugin warmup has dedicated retirement tests.
        let overrides = vec![("features.plugins".to_string(), toml::Value::Boolean(false))];
        match ConfigBuilder::default()
            .codex_home(codex_home.to_path_buf())
            .cli_overrides(overrides.clone())
            .build()
            .await
        {
            Ok(config) => config,
            Err(_) => Config::load_default_with_cli_overrides_for_codex_home(
                codex_home.to_path_buf(),
                overrides,
            )
            .await
            .expect("default config should load"),
        }
    }

    struct TestClient {
        _codex_home: TempDir,
        _host: Arc<InProcessHost>,
        client: InProcessAppServerClient,
    }

    impl Deref for TestClient {
        type Target = InProcessAppServerClient;

        fn deref(&self) -> &Self::Target {
            &self.client
        }
    }

    impl TestClient {
        async fn shutdown(self) -> IoResult<()> {
            self.client.shutdown().await
        }
    }

    async fn start_test_client_with_capacity(
        session_source: SessionSource,
        channel_capacity: usize,
    ) -> TestClient {
        let codex_home = TempDir::new().expect("temp dir");
        let config = Arc::new(build_test_config_for_codex_home(codex_home.path()).await);
        let state_db = init_state_db(config.as_ref())
            .await
            .expect("state db should initialize for in-process test");
        let host = Arc::new(InProcessHost::default());
        let client = InProcessAppServerClient::start_in_host(
            Arc::clone(&host),
            InProcessClientStartArgs {
                required_human_approval: false,
                arg0_paths: Arg0DispatchPaths::default(),
                config,
                cli_overrides: Vec::new(),
                loader_overrides: LoaderOverrides::default(),
                strict_config: false,
                cloud_config_bundle: CloudConfigBundleLoader::default(),
                embedded_network_policy: Default::default(),
                feedback: CodexFeedback::new(),
                log_db: None,
                state_db: Some(state_db),
                environment_manager: Arc::new(EnvironmentManager::default_for_tests()),
                config_warnings: Vec::new(),
                session_source,
                enable_codex_api_key_env: false,
                client_name: "codex-app-server-client-test".to_string(),
                client_version: "0.0.0-test".to_string(),
                experimental_api: true,
                mcp_server_openai_form_elicitation: false,
                opt_out_notification_methods: Vec::new(),
                channel_capacity,
            },
        )
        .await
        .expect("in-process app-server client should start");

        TestClient {
            _codex_home: codex_home,
            _host: host,
            client,
        }
    }

    async fn start_test_client(session_source: SessionSource) -> TestClient {
        start_test_client_with_capacity(session_source, DEFAULT_IN_PROCESS_CHANNEL_CAPACITY).await
    }

    async fn start_test_remote_server<F, Fut>(handler: F) -> String
    where
        F: FnOnce(tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>) -> Fut
            + Send
            + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        start_test_remote_server_with_auth(/*expected_auth_token*/ None, handler).await
    }

    async fn start_test_remote_server_with_auth<F, Fut>(
        expected_auth_token: Option<String>,
        handler: F,
    ) -> String
    where
        F: FnOnce(tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>) -> Fut
            + Send
            + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener address");
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept should succeed");
            let websocket = accept_hdr_async(
                stream,
                move |request: &WebSocketRequest, response: WebSocketResponse| {
                    let provided_auth_token = request
                        .headers()
                        .get(AUTHORIZATION)
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_owned);
                    let expected_auth_token = expected_auth_token
                        .as_ref()
                        .map(|token| format!("Bearer {token}"));
                    assert_eq!(provided_auth_token, expected_auth_token);
                    Ok(response)
                },
            )
            .await
            .expect("websocket upgrade should succeed");
            handler(websocket).await;
        });
        format!("ws://{addr}")
    }

    async fn expect_remote_initialize<S>(websocket: &mut tokio_tungstenite::WebSocketStream<S>)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        expect_remote_initialize_with_metadata(
            websocket,
            serde_json::json!({
                "userAgent": "codex_cli_rs/9.8.7-test (Test OS; x86_64) rust",
                "codexHome": "/server/.codex",
            }),
        )
        .await;
    }

    async fn expect_remote_initialize_with_metadata<S>(
        websocket: &mut tokio_tungstenite::WebSocketStream<S>,
        metadata: serde_json::Value,
    ) where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let JSONRPCMessage::Request(request) = read_websocket_message(websocket).await else {
            panic!("expected initialize request");
        };
        assert_eq!(request.method, "initialize");
        write_websocket_message(
            websocket,
            JSONRPCMessage::Response(JSONRPCResponse {
                id: request.id,
                result: metadata,
            }),
        )
        .await;

        let JSONRPCMessage::Notification(notification) = read_websocket_message(websocket).await
        else {
            panic!("expected initialized notification");
        };
        assert_eq!(notification.method, "initialized");
    }

    async fn read_websocket_message<S>(
        websocket: &mut tokio_tungstenite::WebSocketStream<S>,
    ) -> JSONRPCMessage
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        loop {
            let frame = websocket
                .next()
                .await
                .expect("frame should be available")
                .expect("frame should decode");
            match frame {
                Message::Text(text) => {
                    return serde_json::from_str::<JSONRPCMessage>(&text)
                        .expect("text frame should be valid JSON-RPC");
                }
                Message::Binary(_) | Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {
                    continue;
                }
                Message::Close(_) => panic!("unexpected close frame"),
            }
        }
    }

    async fn write_websocket_message<S>(
        websocket: &mut tokio_tungstenite::WebSocketStream<S>,
        message: JSONRPCMessage,
    ) where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        websocket
            .send(Message::Text(
                serde_json::to_string(&message)
                    .expect("message should serialize")
                    .into(),
            ))
            .await
            .expect("message should send");
    }

    fn command_execution_output_delta_notification(delta: &str) -> ServerNotification {
        ServerNotification::CommandExecutionOutputDelta(
            codex_app_server_protocol::CommandExecutionOutputDeltaNotification {
                thread_id: "thread".to_string(),
                turn_id: "turn".to_string(),
                item_id: "item".to_string(),
                delta: delta.to_string(),
            },
        )
    }

    fn agent_message_delta_notification(delta: &str) -> ServerNotification {
        ServerNotification::AgentMessageDelta(
            codex_app_server_protocol::AgentMessageDeltaNotification {
                thread_id: "thread".to_string(),
                turn_id: "turn".to_string(),
                item_id: "item".to_string(),
                delta: delta.to_string(),
            },
        )
    }

    fn item_completed_notification(text: &str) -> ServerNotification {
        ServerNotification::ItemCompleted(
            codex_app_server_protocol::ItemCompletedNotification::new(
                "thread".to_string(),
                "turn".to_string(),
                codex_app_server_protocol::ThreadItemPayload::AgentMessage {
                    moderation: None,
                    id: "item".to_string(),
                    text: text.to_string(),
                    phase: None,
                    memory_citation: None,
                    delivery: None,
                    questions: None,
                },
                /*started_at_ms*/ None,
                /*completed_at_ms*/ 0,
            ),
        )
    }

    fn turn_completed_notification() -> ServerNotification {
        ServerNotification::TurnCompleted(codex_app_server_protocol::TurnCompletedNotification {
            thread_id: "thread".to_string(),
            turn: codex_app_server_protocol::Turn {
                id: "turn".to_string(),
                root_turn_id: None,
                items_view: codex_app_server_protocol::TurnItemsView::Full,
                items: Vec::new(),
                status: codex_app_server_protocol::TurnStatus::Completed,
                error: None,
                started_at: None,
                completed_at: Some(0),
                duration_ms: Some(1),
            },
        })
    }

    #[test]
    fn remote_interactive_request_is_explicit_and_independent_of_human_approval() {
        for interactive in [false, true] {
            let mut args = test_remote_connect_args("ws://127.0.0.1:1".into());
            args.interactive_client = interactive;
            args.required_human_approval = false;
            let capabilities = args.initialize_params().capabilities.unwrap();
            assert_eq!(capabilities.interactive_client, interactive);
            assert!(!capabilities.required_human_approval);
        }
    }

    fn test_remote_connect_args(websocket_url: String) -> RemoteAppServerConnectArgs {
        RemoteAppServerConnectArgs {
            interactive_client: false,
            required_human_approval: false,
            endpoint: RemoteAppServerEndpoint::WebSocket {
                websocket_url,
                auth_token: None,
            },
            client_name: "codex-app-server-client-test".to_string(),
            client_version: "0.0.0-test".to_string(),
            experimental_api: true,
            mcp_server_openai_form_elicitation: false,
            opt_out_notification_methods: Vec::new(),
            channel_capacity: 8,
        }
    }

    #[test]
    fn remote_initialize_params_forward_openai_form_capability() {
        let mut args = test_remote_connect_args("ws://localhost/rpc".to_string());
        args.mcp_server_openai_form_elicitation = true;

        assert!(
            args.initialize_params()
                .capabilities
                .expect("initialize capabilities")
                .mcp_server_openai_form_elicitation
        );
    }

    #[tokio::test]
    async fn typed_request_drop_cancels_waiting_send_and_reply() {
        let (command_tx, mut commands) = mpsc::channel(/*buffer*/ 1);
        let handle =
            AppServerRequestHandle::InProcess(InProcessAppServerRequestHandle { command_tx });
        let request = ClientRequest::ConfigRequirementsRead {
            request_id: RequestId::String("typed-request".to_string()),
            params: None,
        };
        let mut pending = Box::pin(handle.request_typed::<serde_json::Value>(request.clone()));
        assert!(matches!(
            commands.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(poll!(&mut pending).is_pending());
        let mut blocked = Box::pin(handle.request_typed::<serde_json::Value>(request.clone()));
        assert!(poll!(&mut blocked).is_pending());
        drop(blocked);

        let ClientCommand::Request {
            request: sent,
            response_tx,
        } = commands.try_recv().expect("first request should be queued")
        else {
            panic!("expected request command");
        };
        assert_eq!(*sent, request);
        assert!(!response_tx.is_closed());
        drop(pending);
        assert!(response_tx.is_closed());
        assert!(matches!(
            commands.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn typed_request_roundtrip_works() {
        let TestClient {
            _codex_home,
            _host,
            client,
        } = start_test_client(SessionSource::Exec).await;
        let client = AppServerClient::InProcess(client);
        assert_eq!(
            (client.platform_family(), client.platform_os()),
            (Some(std::env::consts::FAMILY), Some(std::env::consts::OS))
        );
        let _response: ConfigRequirementsReadResponse = client
            .request_typed(ClientRequest::ConfigRequirementsRead {
                request_id: RequestId::Integer(1),
                params: None,
            })
            .await
            .expect("typed request should succeed");
        client.shutdown().await.expect("shutdown should complete");
    }

    #[tokio::test]
    async fn typed_request_reports_json_rpc_errors() {
        let client = start_test_client(SessionSource::Exec).await;
        let err = client
            .request_typed::<ConfigRequirementsReadResponse>(ClientRequest::ThreadRead {
                request_id: RequestId::Integer(99),
                params: codex_app_server_protocol::ThreadReadParams {
                    thread_id: "missing-thread".to_string(),
                    include_turns: false,
                },
            })
            .await
            .expect_err("missing thread should return a JSON-RPC error");
        assert!(
            err.to_string().starts_with("thread/read failed:"),
            "expected method-qualified JSON-RPC failure message"
        );
        client.shutdown().await.expect("shutdown should complete");
    }

    #[tokio::test]
    async fn caller_provided_session_source_is_applied() {
        for (session_source, expected_source) in [
            (SessionSource::Exec, ApiSessionSource::Exec),
            (SessionSource::Cli, ApiSessionSource::Cli),
        ] {
            let client = start_test_client(session_source).await;
            let parsed: ThreadStartResponse = client
                .request_typed(ClientRequest::ThreadStart {
                    request_id: RequestId::Integer(2),
                    params: ThreadStartParams {
                        ephemeral: Some(true),
                        ..ThreadStartParams::default()
                    },
                })
                .await
                .expect("thread/start should succeed");
            assert_eq!(parsed.thread.source, expected_source);
            client.shutdown().await.expect("shutdown should complete");
        }
    }

    #[tokio::test]
    async fn threads_started_via_app_server_are_visible_through_typed_requests() {
        let client = start_test_client(SessionSource::Cli).await;

        let response: ThreadStartResponse = client
            .request_typed(ClientRequest::ThreadStart {
                request_id: RequestId::Integer(3),
                params: ThreadStartParams {
                    ephemeral: Some(true),
                    ..ThreadStartParams::default()
                },
            })
            .await
            .expect("thread/start should succeed");
        let read = client
            .request_typed::<codex_app_server_protocol::ThreadReadResponse>(
                ClientRequest::ThreadRead {
                    request_id: RequestId::Integer(4),
                    params: codex_app_server_protocol::ThreadReadParams {
                        thread_id: response.thread.id.clone(),
                        include_turns: false,
                    },
                },
            )
            .await
            .expect("thread/read should return the newly started thread");
        assert_eq!(read.thread.id, response.thread.id);

        client.shutdown().await.expect("shutdown should complete");
    }

    #[tokio::test]
    async fn tiny_channel_capacity_still_supports_request_roundtrip() {
        let client =
            start_test_client_with_capacity(SessionSource::Exec, /*channel_capacity*/ 1).await;
        let _response: ConfigRequirementsReadResponse = client
            .request_typed(ClientRequest::ConfigRequirementsRead {
                request_id: RequestId::Integer(1),
                params: None,
            })
            .await
            .expect("typed request should succeed");
        client.shutdown().await.expect("shutdown should complete");
    }

    #[tokio::test]
    async fn unread_lossless_notifications_do_not_block_in_process_requests() {
        let mut client =
            start_test_client_with_capacity(SessionSource::Cli, /*channel_capacity*/ 1).await;
        let thread: ThreadStartResponse = client
            .request_typed(ClientRequest::ThreadStart {
                request_id: RequestId::Integer(1),
                params: ThreadStartParams {
                    ephemeral: Some(true),
                    personality: Some(Personality::None),
                    ..ThreadStartParams::default()
                },
            })
            .await
            .expect("thread/start should succeed");
        let request_handle = client.request_handle();

        timeout(Duration::from_secs(2), async {
            for (index, personality) in [
                Personality::Friendly,
                Personality::Pragmatic,
                Personality::Friendly,
                Personality::Pragmatic,
            ]
            .into_iter()
            .enumerate()
            {
                let _: ThreadSettingsUpdateResponse = request_handle
                    .request_typed(ClientRequest::ThreadSettingsUpdate {
                        request_id: RequestId::Integer((index + 2) as i64),
                        params: ThreadSettingsUpdateParams {
                            thread_id: thread.thread.id.clone(),
                            personality: Some(personality),
                            ..ThreadSettingsUpdateParams::default()
                        },
                    })
                    .await
                    .expect("thread/settings/update should succeed");
            }

            let _: ConfigRequirementsReadResponse = request_handle
                .request_typed(ClientRequest::ConfigRequirementsRead {
                    request_id: RequestId::Integer(10),
                    params: None,
                })
                .await
                .expect("configuration request should succeed");
        })
        .await
        .expect("unread lossless notifications must not block app-server requests");

        let mut personalities = Vec::new();
        timeout(Duration::from_secs(2), async {
            while personalities.len() < 4 {
                if let Some(InProcessServerEvent::ServerNotification(notification)) =
                    client.client.next_event().await
                    && let ServerNotification::ThreadSettingsUpdated(notification) =
                        notification.as_ref()
                {
                    personalities.push(notification.thread_settings.personality);
                }
            }
        })
        .await
        .expect("queued settings notifications should remain readable");
        assert_eq!(
            personalities,
            vec![
                Some(Personality::Friendly),
                Some(Personality::Pragmatic),
                Some(Personality::Friendly),
                Some(Personality::Pragmatic),
            ]
        );

        client.shutdown().await.expect("shutdown should complete");
    }

    #[tokio::test]
    async fn remote_platform_metadata_preserves_reported_and_missing_values() {
        for (family, os) in [
            (Some("windows"), Some("windows")),
            (Some("unix"), Some("linux")),
            (Some("future-family"), Some("future-os")),
            (Some("unix"), None),
            (None, Some("linux")),
            (None, None),
        ] {
            let websocket_url = start_test_remote_server(move |mut websocket| async move {
                let mut metadata = serde_json::json!({});
                if let Some(family) = family {
                    metadata["platformFamily"] = family.into();
                }
                if let Some(os) = os {
                    metadata["platformOs"] = os.into();
                }
                expect_remote_initialize_with_metadata(&mut websocket, metadata).await;
                websocket.close(None).await.expect("close should succeed");
            })
            .await;
            let client = AppServerClient::Remote(
                RemoteAppServerClient::connect(test_remote_connect_args(websocket_url))
                    .await
                    .expect("remote client should connect"),
            );
            assert_eq!(
                (client.platform_family(), client.platform_os()),
                (family, os)
            );
            client.shutdown().await.expect("shutdown should complete");
        }
    }

    #[tokio::test]
    async fn remote_typed_request_roundtrip_works() {
        let websocket_url = start_test_remote_server(|mut websocket| async move {
            expect_remote_initialize(&mut websocket).await;
            let JSONRPCMessage::Request(request) = read_websocket_message(&mut websocket).await
            else {
                panic!("expected account/read request");
            };
            assert_eq!(request.method, "account/read");
            write_websocket_message(
                &mut websocket,
                JSONRPCMessage::Response(JSONRPCResponse {
                    id: request.id,
                    result: serde_json::to_value(GetAccountResponse {
                        auth_observation: None,
                        user_id: None,
                        workspace_routing: None,
                        account: None,
                        requires_openai_auth: false,
                    })
                    .expect("response should serialize"),
                }),
            )
            .await;
            websocket.close(None).await.expect("close should succeed");
        })
        .await;
        let client = RemoteAppServerClient::connect(test_remote_connect_args(websocket_url))
            .await
            .expect("remote client should connect");

        assert_eq!(client.server_version(), Some("9.8.7-test"));
        assert_eq!(client.codex_home(), Some("/server/.codex"));
        let client = AppServerClient::Remote(client);
        let response: GetAccountResponse = client
            .request_typed(ClientRequest::GetAccount {
                request_id: RequestId::Integer(1),
                params: codex_app_server_protocol::GetAccountParams {
                    cache_only: false,
                    refresh_token: false,
                },
            })
            .await
            .expect("typed request should succeed");
        assert_eq!(response.account, None);

        client.shutdown().await.expect("shutdown should complete");
    }

    #[tokio::test]
    async fn remote_unix_socket_typed_request_roundtrip_works() {
        let socket_dir = TempDir::new().expect("socket dir");
        let socket_path = AbsolutePathBuf::from_absolute_path(socket_dir.path().join("codex.sock"))
            .expect("socket path should resolve");
        let mut listener = UnixListener::bind(socket_path.as_path())
            .await
            .expect("listener should bind");
        tokio::spawn(async move {
            let stream = listener.accept().await.expect("accept should succeed");
            let mut websocket = accept_async(stream)
                .await
                .expect("websocket upgrade should succeed");
            expect_remote_initialize(&mut websocket).await;
            let JSONRPCMessage::Request(request) = read_websocket_message(&mut websocket).await
            else {
                panic!("expected account/read request");
            };
            assert_eq!(request.method, "account/read");
            write_websocket_message(
                &mut websocket,
                JSONRPCMessage::Response(JSONRPCResponse {
                    id: request.id,
                    result: serde_json::to_value(GetAccountResponse {
                        auth_observation: None,
                        user_id: None,
                        workspace_routing: None,
                        account: None,
                        requires_openai_auth: false,
                    })
                    .expect("response should serialize"),
                }),
            )
            .await;
            websocket.close(None).await.expect("close should succeed");
        });
        let client = RemoteAppServerClient::connect(RemoteAppServerConnectArgs {
            interactive_client: false,
            required_human_approval: false,
            endpoint: RemoteAppServerEndpoint::UnixSocket { socket_path },
            client_name: "codex-app-server-client-test".to_string(),
            client_version: "0.0.0-test".to_string(),
            experimental_api: true,
            mcp_server_openai_form_elicitation: false,
            opt_out_notification_methods: Vec::new(),
            channel_capacity: 8,
        })
        .await
        .expect("remote client should connect");

        let response: GetAccountResponse = client
            .request_typed(ClientRequest::GetAccount {
                request_id: RequestId::Integer(1),
                params: codex_app_server_protocol::GetAccountParams {
                    cache_only: false,
                    refresh_token: false,
                },
            })
            .await
            .expect("typed request should succeed");
        assert_eq!(response.account, None);

        client.shutdown().await.expect("shutdown should complete");
    }

    #[tokio::test]
    async fn remote_typed_request_accepts_large_single_frame_response() {
        let padding = "x".repeat((17 << 20) + 1024);
        let websocket_url = start_test_remote_server(move |mut websocket| async move {
            expect_remote_initialize(&mut websocket).await;
            let JSONRPCMessage::Request(request) = read_websocket_message(&mut websocket).await
            else {
                panic!("expected account/read request");
            };
            assert_eq!(request.method, "account/read");
            write_websocket_message(
                &mut websocket,
                JSONRPCMessage::Response(JSONRPCResponse {
                    id: request.id,
                    result: serde_json::json!({
                        "account": null,
                        "requiresOpenaiAuth": false,
                        "padding": padding,
                    }),
                }),
            )
            .await;
            websocket.close(None).await.expect("close should succeed");
        })
        .await;
        let client = RemoteAppServerClient::connect(test_remote_connect_args(websocket_url))
            .await
            .expect("remote client should connect");

        let response: GetAccountResponse = client
            .request_typed(ClientRequest::GetAccount {
                request_id: RequestId::Integer(1),
                params: codex_app_server_protocol::GetAccountParams {
                    cache_only: false,
                    refresh_token: false,
                },
            })
            .await
            .expect("large typed request should succeed");
        assert_eq!(
            response,
            GetAccountResponse {
                auth_observation: None,
                user_id: None,
                workspace_routing: None,
                account: None,
                requires_openai_auth: false,
            }
        );

        client.shutdown().await.expect("shutdown should complete");
    }

    #[tokio::test]
    async fn remote_connect_includes_auth_header_when_configured() {
        let auth_token = "remote-bearer-token".to_string();
        let websocket_url = start_test_remote_server_with_auth(
            Some(auth_token.clone()),
            |mut websocket| async move {
                expect_remote_initialize(&mut websocket).await;
                websocket.close(None).await.expect("close should succeed");
            },
        )
        .await;
        let client = RemoteAppServerClient::connect(RemoteAppServerConnectArgs {
            interactive_client: false,
            required_human_approval: false,
            endpoint: RemoteAppServerEndpoint::WebSocket {
                websocket_url,
                auth_token: Some(auth_token),
            },
            client_name: "codex-app-server-client-test".to_string(),
            client_version: "0.0.0-test".to_string(),
            experimental_api: true,
            mcp_server_openai_form_elicitation: false,
            opt_out_notification_methods: Vec::new(),
            channel_capacity: 8,
        })
        .await
        .expect("remote client should connect");

        client.shutdown().await.expect("shutdown should complete");
    }

    #[tokio::test]
    async fn remote_connect_rejects_non_loopback_ws_when_auth_configured() {
        let result = RemoteAppServerClient::connect(RemoteAppServerConnectArgs {
            interactive_client: false,
            required_human_approval: false,
            endpoint: RemoteAppServerEndpoint::WebSocket {
                websocket_url: "ws://example.com:4500".to_string(),
                auth_token: Some("remote-bearer-token".to_string()),
            },
            client_name: "codex-app-server-client-test".to_string(),
            client_version: "0.0.0-test".to_string(),
            experimental_api: true,
            mcp_server_openai_form_elicitation: false,
            opt_out_notification_methods: Vec::new(),
            channel_capacity: 8,
        })
        .await;
        let err = match result {
            Ok(_) => panic!("non-loopback ws should be rejected before connect"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert!(
            err.to_string()
                .contains("remote auth tokens require `wss://` or loopback `ws://` URLs")
        );
    }

    #[test]
    fn remote_auth_token_transport_policy_allows_wss_and_loopback_ws() {
        assert!(crate::remote::websocket_url_supports_auth_token(
            &url::Url::parse("wss://example.com:443").expect("wss URL should parse")
        ));
        assert!(crate::remote::websocket_url_supports_auth_token(
            &url::Url::parse("ws://127.0.0.1:4500").expect("loopback ws URL should parse")
        ));
        assert!(!crate::remote::websocket_url_supports_auth_token(
            &url::Url::parse("ws://example.com:4500").expect("non-loopback ws URL should parse")
        ));
    }

    #[tokio::test]
    async fn remote_duplicate_request_id_keeps_original_waiter() {
        let (first_request_seen_tx, first_request_seen_rx) = tokio::sync::oneshot::channel();
        let websocket_url = start_test_remote_server(|mut websocket| async move {
            expect_remote_initialize(&mut websocket).await;
            let JSONRPCMessage::Request(request) = read_websocket_message(&mut websocket).await
            else {
                panic!("expected account/read request");
            };
            assert_eq!(request.method, "account/read");
            first_request_seen_tx
                .send(request.id.clone())
                .expect("request id should send");
            assert!(
                timeout(
                    Duration::from_millis(100),
                    read_websocket_message(&mut websocket)
                )
                .await
                .is_err(),
                "duplicate request should not be forwarded to the server"
            );
            write_websocket_message(
                &mut websocket,
                JSONRPCMessage::Response(JSONRPCResponse {
                    id: request.id,
                    result: serde_json::to_value(GetAccountResponse {
                        auth_observation: None,
                        user_id: None,
                        workspace_routing: None,
                        account: None,
                        requires_openai_auth: false,
                    })
                    .expect("response should serialize"),
                }),
            )
            .await;
            let _ = websocket.next().await;
        })
        .await;
        let client = RemoteAppServerClient::connect(test_remote_connect_args(websocket_url))
            .await
            .expect("remote client should connect");
        let first_request_handle = client.request_handle();
        let second_request_handle = first_request_handle.clone();

        let first_request = tokio::spawn(async move {
            first_request_handle
                .request_typed::<GetAccountResponse>(ClientRequest::GetAccount {
                    request_id: RequestId::Integer(1),
                    params: codex_app_server_protocol::GetAccountParams {
                        cache_only: false,
                        refresh_token: false,
                    },
                })
                .await
        });

        let first_request_id = first_request_seen_rx
            .await
            .expect("server should observe the first request");
        assert_eq!(first_request_id, RequestId::Integer(1));

        let second_err = second_request_handle
            .request_typed::<GetAccountResponse>(ClientRequest::GetAccount {
                request_id: RequestId::Integer(1),
                params: codex_app_server_protocol::GetAccountParams {
                    cache_only: false,
                    refresh_token: false,
                },
            })
            .await
            .expect_err("duplicate request id should be rejected");
        assert_eq!(
            second_err.to_string(),
            "account/read transport error: duplicate remote app-server request id `1`"
        );

        let first_response = first_request
            .await
            .expect("first request task should join")
            .expect("first request should succeed");
        assert_eq!(
            first_response,
            GetAccountResponse {
                auth_observation: None,
                user_id: None,
                workspace_routing: None,
                account: None,
                requires_openai_auth: false,
            }
        );

        client.shutdown().await.expect("shutdown should complete");
    }

    #[tokio::test]
    async fn remote_notifications_arrive_over_websocket() {
        let (close_tx, close_rx) = tokio::sync::oneshot::channel();
        let websocket_url = start_test_remote_server(|mut websocket| async move {
            expect_remote_initialize(&mut websocket).await;
            let notification = JSONRPCMessage::Notification(
                serde_json::from_value(
                    serde_json::to_value(ServerNotification::AccountUpdated(
                        AccountUpdatedNotification {
                            auth_mode: None,
                            plan_type: None,
                        },
                    ))
                    .expect("notification should serialize"),
                )
                .expect("notification should convert to JSON-RPC"),
            );
            // Only the first notification is consumed; the other can arrive during shutdown.
            write_websocket_message(&mut websocket, notification.clone()).await;
            write_websocket_message(&mut websocket, notification).await;
            let _ = close_tx.send(websocket.next().await);
        })
        .await;
        let mut client = RemoteAppServerClient::connect(test_remote_connect_args(websocket_url))
            .await
            .expect("remote client should connect");

        let event = client.next_event().await.expect("event should arrive");
        assert!(matches!(
            event,
            AppServerEvent::ServerNotification(notification)
                if matches!(notification.as_ref(), ServerNotification::AccountUpdated(_))
        ));

        client.shutdown().await.expect("shutdown should complete");
        assert_eq!(
            close_rx
                .await
                .expect("server should observe shutdown")
                .expect("server should receive a frame")
                .expect("websocket should close cleanly"),
            Message::Close(None),
        );
    }

    #[tokio::test]
    async fn remote_backpressure_preserves_transcript_notifications() {
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let websocket_url = start_test_remote_server(|mut websocket| async move {
            expect_remote_initialize(&mut websocket).await;
            for notification in [
                command_execution_output_delta_notification("stdout-1"),
                command_execution_output_delta_notification("stdout-2"),
                agent_message_delta_notification("hello"),
                item_completed_notification("hello"),
                turn_completed_notification(),
            ] {
                write_websocket_message(
                    &mut websocket,
                    JSONRPCMessage::Notification(
                        serde_json::from_value(
                            serde_json::to_value(notification)
                                .expect("notification should serialize"),
                        )
                        .expect("notification should convert to JSON-RPC"),
                    ),
                )
                .await;
            }
            let _ = done_rx.await;
        })
        .await;
        let mut client = RemoteAppServerClient::connect(RemoteAppServerConnectArgs {
            interactive_client: false,
            channel_capacity: 1,
            ..test_remote_connect_args(websocket_url)
        })
        .await
        .expect("remote client should connect");

        let first_event = timeout(Duration::from_secs(2), client.next_event())
            .await
            .expect("first event should arrive before timeout")
            .expect("event stream should stay open");
        assert!(matches!(
            first_event,
            AppServerEvent::ServerNotification(notification)
                if matches!(
                    notification.as_ref(),
                    ServerNotification::CommandExecutionOutputDelta(notification)
                        if notification.delta == "stdout-1"
                )
        ));

        let mut remaining_events = Vec::new();
        for _ in 0..4 {
            remaining_events.push(
                timeout(Duration::from_secs(2), client.next_event())
                    .await
                    .expect("event should arrive before timeout")
                    .expect("event stream should stay open"),
            );
        }

        let mut transcript_event_names = Vec::new();
        for event in &remaining_events {
            match event {
                AppServerEvent::Lagged { skipped: 1 } => {}
                AppServerEvent::ServerNotification(notification) => match notification.as_ref() {
                    ServerNotification::CommandExecutionOutputDelta(notification)
                        if notification.delta == "stdout-2" => {}
                    ServerNotification::AgentMessageDelta(notification)
                        if notification.delta == "hello" =>
                    {
                        transcript_event_names.push("agent_message_delta");
                    }
                    ServerNotification::ItemCompleted(notification)
                        if matches!(
                            &notification.item.payload,
                            codex_app_server_protocol::ThreadItemPayload::AgentMessage { text, .. }
                                if text == "hello"
                        ) =>
                    {
                        transcript_event_names.push("item_completed");
                    }
                    ServerNotification::TurnCompleted(notification)
                        if notification.turn.status
                            == codex_app_server_protocol::TurnStatus::Completed =>
                    {
                        transcript_event_names.push("turn_completed");
                    }
                    _ => panic!("unexpected remaining event: {event:?}"),
                },
                _ => panic!("unexpected remaining event: {event:?}"),
            }
        }
        assert_eq!(
            transcript_event_names,
            vec!["agent_message_delta", "item_completed", "turn_completed"]
        );

        done_tx
            .send(())
            .expect("server completion signal should send");
        client.shutdown().await.expect("shutdown should complete");
    }

    #[tokio::test]
    async fn remote_server_request_resolution_roundtrip_works() {
        let websocket_url = start_test_remote_server(|mut websocket| async move {
            expect_remote_initialize(&mut websocket).await;
            let request_id = RequestId::String("srv-1".to_string());
            let server_request = JSONRPCRequest {
                id: request_id.clone(),
                method: "item/tool/requestUserInput".to_string(),
                params: Some(
                    serde_json::to_value(ToolRequestUserInputParams {
                        thread_id: "thread-1".to_string(),
                        turn_id: "turn-1".to_string(),
                        item_id: "call-1".to_string(),
                        questions: vec![ToolRequestUserInputQuestion {
                            id: "question-1".to_string(),
                            header: "Mode".to_string(),
                            question: "Pick one".to_string(),
                            is_other: false,
                            is_secret: false,
                            options: Some(vec![]),
                        }],
                        is_blocking: true,
                        auto_resolution_ms: None,
                    })
                    .expect("params should serialize"),
                ),
                trace: None,
            };
            write_websocket_message(&mut websocket, JSONRPCMessage::Request(server_request)).await;

            let JSONRPCMessage::Response(response) = read_websocket_message(&mut websocket).await
            else {
                panic!("expected server request response");
            };
            assert_eq!(response.id, request_id);
        })
        .await;
        let mut client = RemoteAppServerClient::connect(test_remote_connect_args(websocket_url))
            .await
            .expect("remote client should connect");

        let AppServerEvent::ServerRequest(request) = client
            .next_event()
            .await
            .expect("request event should arrive")
        else {
            panic!("expected server request event");
        };
        client
            .resolve_server_request(request.id().clone(), serde_json::json!({}))
            .await
            .expect("server request should resolve");

        client.shutdown().await.expect("shutdown should complete");
    }

    #[tokio::test]
    async fn remote_server_request_received_during_initialize_is_delivered() {
        let websocket_url = start_test_remote_server(|mut websocket| async move {
            let JSONRPCMessage::Request(request) = read_websocket_message(&mut websocket).await
            else {
                panic!("expected initialize request");
            };
            assert_eq!(request.method, "initialize");

            let request_id = RequestId::String("srv-init".to_string());
            write_websocket_message(
                &mut websocket,
                JSONRPCMessage::Request(JSONRPCRequest {
                    id: request_id.clone(),
                    method: "item/tool/requestUserInput".to_string(),
                    params: Some(
                        serde_json::to_value(ToolRequestUserInputParams {
                            thread_id: "thread-1".to_string(),
                            turn_id: "turn-1".to_string(),
                            item_id: "call-1".to_string(),
                            questions: vec![ToolRequestUserInputQuestion {
                                id: "question-1".to_string(),
                                header: "Mode".to_string(),
                                question: "Pick one".to_string(),
                                is_other: false,
                                is_secret: false,
                                options: Some(vec![]),
                            }],
                            is_blocking: true,
                            auto_resolution_ms: None,
                        })
                        .expect("params should serialize"),
                    ),
                    trace: None,
                }),
            )
            .await;
            write_websocket_message(
                &mut websocket,
                JSONRPCMessage::Response(JSONRPCResponse {
                    id: request.id,
                    result: serde_json::json!({}),
                }),
            )
            .await;

            let JSONRPCMessage::Notification(notification) =
                read_websocket_message(&mut websocket).await
            else {
                panic!("expected initialized notification");
            };
            assert_eq!(notification.method, "initialized");

            let JSONRPCMessage::Response(response) = read_websocket_message(&mut websocket).await
            else {
                panic!("expected server request response");
            };
            assert_eq!(response.id, request_id);
        })
        .await;
        let mut client = RemoteAppServerClient::connect(test_remote_connect_args(websocket_url))
            .await
            .expect("remote client should connect");

        let AppServerEvent::ServerRequest(request) = client
            .next_event()
            .await
            .expect("request event should arrive")
        else {
            panic!("expected server request event");
        };
        client
            .resolve_server_request(request.id().clone(), serde_json::json!({}))
            .await
            .expect("server request should resolve");

        client.shutdown().await.expect("shutdown should complete");
    }

    #[tokio::test]
    async fn remote_unknown_server_request_is_rejected() {
        let websocket_url = start_test_remote_server(|mut websocket| async move {
            expect_remote_initialize(&mut websocket).await;
            let request_id = RequestId::String("srv-unknown".to_string());
            write_websocket_message(
                &mut websocket,
                JSONRPCMessage::Request(JSONRPCRequest {
                    id: request_id.clone(),
                    method: "thread/unknown".to_string(),
                    params: None,
                    trace: None,
                }),
            )
            .await;

            let JSONRPCMessage::Error(response) = read_websocket_message(&mut websocket).await
            else {
                panic!("expected JSON-RPC error response");
            };
            assert_eq!(response.id, request_id);
            assert_eq!(response.error.code, -32601);
            assert_eq!(
                response.error.message,
                "unsupported remote app-server request `thread/unknown`"
            );
        })
        .await;
        let client = RemoteAppServerClient::connect(test_remote_connect_args(websocket_url))
            .await
            .expect("remote client should connect");

        client.shutdown().await.expect("shutdown should complete");
    }

    #[tokio::test]
    async fn remote_disconnect_surfaces_as_event() {
        let websocket_url = start_test_remote_server(|mut websocket| async move {
            expect_remote_initialize(&mut websocket).await;
            websocket.close(None).await.expect("close should succeed");
        })
        .await;
        let mut client = RemoteAppServerClient::connect(test_remote_connect_args(websocket_url))
            .await
            .expect("remote client should connect");

        let event = client
            .next_event()
            .await
            .expect("disconnect event should arrive");
        assert!(matches!(event, AppServerEvent::Disconnected { .. }));
    }

    #[test]
    fn typed_request_error_exposes_sources() {
        let transport = TypedRequestError::Transport {
            method: "config/read".to_string(),
            source: IoError::new(ErrorKind::BrokenPipe, "closed"),
        };
        assert_eq!(std::error::Error::source(&transport).is_some(), true);

        let server = TypedRequestError::Server {
            method: "thread/read".to_string(),
            source: JSONRPCErrorError {
                code: -32603,
                data: Some(serde_json::json!({"detail": "config lock mismatch"})),
                message: "internal".to_string(),
            },
        };
        assert_eq!(std::error::Error::source(&server).is_some(), false);
        assert_eq!(
            server.to_string(),
            "thread/read failed: internal (code -32603), data: {\"detail\":\"config lock mismatch\"}"
        );

        let deserialize = TypedRequestError::Deserialize {
            method: "thread/start".to_string(),
            source: serde_json::from_str::<u32>("\"nope\"")
                .expect_err("invalid integer should return deserialize error"),
        };
        assert_eq!(std::error::Error::source(&deserialize).is_some(), true);
    }

    async fn assert_embedded_shutdown_deadline(final_exit: bool) {
        let (command_tx, mut command_rx) = mpsc::channel(1);
        let (_event_tx, event_rx) = mpsc::unbounded_channel();
        let (release_tx, release_rx) = oneshot::channel();
        let (finished_tx, finished_rx) = oneshot::channel();
        let start = Instant::now();
        let deadline = start
            + if final_exit {
                Duration::from_millis(100)
            } else {
                IN_PROCESS_SHUTDOWN_TIMEOUT
            };
        let worker_handle = tokio::spawn(async move {
            let ClientCommand::Shutdown {
                response_tx,
                deadline: received,
            } = command_rx.recv().await.expect("shutdown admitted")
            else {
                panic!("only shutdown is expected");
            };
            assert_eq!(
                received, deadline,
                "the worker receives the original deadline"
            );
            release_rx
                .await
                .expect("the fixture releases the original worker");
            assert!(response_tx.is_closed(), "timeout closed only the observer");
            finished_tx
                .send(())
                .expect("the original worker was not aborted");
        });
        let client = AppServerClient::InProcess(InProcessAppServerClient {
            command_tx,
            event_rx,
            worker_handle,
            _host: Arc::new(InProcessHost::default()),
        });
        if final_exit {
            assert!(matches!(
                client.shutdown_by(deadline).await,
                FinalClose::TimedOut(_)
            ));
        } else {
            let error = client
                .shutdown()
                .await
                .expect_err("stalled embedded close times out");
            assert_eq!(error.kind(), ErrorKind::TimedOut);
        }
        assert_eq!(
            Instant::now(),
            deadline,
            "no new 45-second budget is started"
        );
        release_tx
            .send(())
            .expect("the original worker remains alive");
        finished_rx
            .await
            .expect("the released worker exits without abort");
    }

    #[tokio::test(start_paused = true)]
    async fn embedded_final_close_passes_the_shared_deadline_and_does_not_abort() {
        assert_embedded_shutdown_deadline(/*final_exit*/ true).await;
    }

    #[tokio::test(start_paused = true)]
    async fn embedded_ordinary_close_keeps_its_original_budget() {
        assert_embedded_shutdown_deadline(/*final_exit*/ false).await;
    }

    #[tokio::test]
    async fn next_event_surfaces_lagged_markers() {
        let (command_tx, _) = mpsc::channel(1);
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let worker_handle = tokio::spawn(async {});
        event_tx
            .send(InProcessServerEvent::Lagged { skipped: 3 })
            .expect("lagged marker should enqueue");
        drop(event_tx);

        let mut client = InProcessAppServerClient {
            command_tx,
            event_rx,
            worker_handle,
            _host: Arc::new(InProcessHost::default()),
        };

        let event = timeout(Duration::from_secs(2), client.next_event())
            .await
            .expect("lagged marker should arrive before timeout");
        assert!(matches!(
            event,
            Some(InProcessServerEvent::Lagged { skipped: 3 })
        ));

        client.shutdown().await.expect("shutdown should complete");
    }

    #[tokio::test]
    async fn runtime_start_args_forward_environment_manager_and_openai_form_capability() {
        let config = Arc::new(build_test_config().await);
        let environment_manager = Arc::new(
            EnvironmentManager::create_for_tests(
                Some("ws://127.0.0.1:8765".to_string()),
                Some(
                    ExecServerRuntimeOptions::new(
                        std::env::current_exe().expect("current exe"),
                        /*codex_linux_sandbox_exe*/ None,
                    )
                    .expect("runtime paths"),
                ),
            )
            .await,
        );

        let runtime_args = InProcessClientStartArgs {
            required_human_approval: false,
            arg0_paths: Arg0DispatchPaths::default(),
            config: config.clone(),
            cli_overrides: Vec::new(),
            loader_overrides: LoaderOverrides::default(),
            strict_config: false,
            cloud_config_bundle: CloudConfigBundleLoader::default(),
            embedded_network_policy: Default::default(),
            feedback: CodexFeedback::new(),
            log_db: None,
            state_db: None,
            environment_manager: environment_manager.clone(),
            config_warnings: Vec::new(),
            session_source: SessionSource::Exec,
            enable_codex_api_key_env: false,
            client_name: "codex-app-server-client-test".to_string(),
            client_version: "0.0.0-test".to_string(),
            experimental_api: true,
            mcp_server_openai_form_elicitation: true,
            opt_out_notification_methods: Vec::new(),
            channel_capacity: DEFAULT_IN_PROCESS_CHANNEL_CAPACITY,
        }
        .into_runtime_start_args();

        assert_eq!(runtime_args.config, config);
        assert!(
            runtime_args
                .initialize
                .capabilities
                .expect("initialize capabilities")
                .mcp_server_openai_form_elicitation
        );
        assert!(Arc::ptr_eq(
            &runtime_args.environment_manager,
            &environment_manager
        ));
        assert!(
            runtime_args
                .environment_manager
                .default_environment()
                .expect("default environment")
                .is_remote()
        );
    }

    #[tokio::test]
    async fn shutdown_completes_promptly_without_retained_managers() {
        let client = start_test_client(SessionSource::Cli).await;

        timeout(Duration::from_secs(1), client.shutdown())
            .await
            .expect("shutdown should not wait for the 5s fallback timeout")
            .expect("shutdown should complete");
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_waits_for_in_process_drain() {
        use std::sync::atomic::AtomicBool;
        use std::sync::atomic::Ordering;

        let (command_tx, mut command_rx) = mpsc::channel(1);
        let (_event_tx, event_rx) = mpsc::unbounded_channel();
        let completed = Arc::new(AtomicBool::new(false));
        let worker_completed = Arc::clone(&completed);
        let worker_handle = tokio::spawn(async move {
            let response_tx = match command_rx.recv().await {
                Some(ClientCommand::Shutdown { response_tx, .. }) => response_tx,
                _ => panic!("expected shutdown command"),
            };
            tokio::time::sleep(Duration::from_secs(30)).await;
            worker_completed.store(true, Ordering::Release);
            let _ = response_tx.send(Ok(()));
        });
        let client = InProcessAppServerClient {
            command_tx,
            event_rx,
            worker_handle,
            _host: Arc::new(InProcessHost::default()),
        };

        client.shutdown().await.expect("shutdown should complete");
        assert!(completed.load(Ordering::Acquire));
    }
    #[tokio::test(start_paused = true)]
    async fn shutdown_timeout_retains_original_worker() {
        let (command_tx, mut command_rx) = mpsc::channel(1);
        let (_event_tx, event_rx) = mpsc::unbounded_channel();
        let (release_tx, release_rx) = oneshot::channel();
        let (done_tx, done_rx) = oneshot::channel();
        let worker_handle = tokio::spawn(async move {
            let Some(ClientCommand::Shutdown { response_tx, .. }) = command_rx.recv().await else {
                panic!("expected shutdown");
            };
            let _ = release_rx.await;
            let _ = response_tx.send(Ok(()));
            let _ = done_tx.send(());
        });
        let client = InProcessAppServerClient {
            command_tx,
            event_rx,
            worker_handle,
            _host: Arc::new(InProcessHost::default()),
        };
        let error = client
            .shutdown()
            .await
            .expect_err("unjoined worker is incomplete");
        assert_eq!(error.kind(), ErrorKind::TimedOut);
        release_tx
            .send(())
            .expect("original worker was not aborted");
        done_rx
            .await
            .expect("original worker can still complete after observer timeout");
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_error_still_observes_original_worker() {
        let (command_tx, mut command_rx) = mpsc::channel(1);
        let (_event_tx, event_rx) = mpsc::unbounded_channel();
        let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worker_completed = Arc::clone(&completed);
        let worker_handle = tokio::spawn(async move {
            let Some(ClientCommand::Shutdown { response_tx, .. }) = command_rx.recv().await else {
                panic!("expected shutdown");
            };
            let _ = response_tx.send(Err(IoError::other("original cleanup incomplete")));
            tokio::time::sleep(Duration::from_secs(2)).await;
            worker_completed.store(true, std::sync::atomic::Ordering::Release);
        });
        let client = InProcessAppServerClient {
            command_tx,
            event_rx,
            worker_handle,
            _host: Arc::new(InProcessHost::default()),
        };
        let error = client
            .shutdown()
            .await
            .expect_err("cleanup error must propagate");
        assert_eq!(error.to_string(), "original cleanup incomplete");
        assert!(completed.load(std::sync::atomic::Ordering::Acquire));
    }

    #[tokio::test]
    async fn shutdown_worker_panic_is_not_clean() {
        let (command_tx, mut command_rx) = mpsc::channel(1);
        let (_event_tx, event_rx) = mpsc::unbounded_channel();
        let worker_handle = tokio::spawn(async move {
            let _command = command_rx.recv().await;
            panic!("original worker failed");
        });
        let client = InProcessAppServerClient {
            command_tx,
            event_rx,
            worker_handle,
            _host: Arc::new(InProcessHost::default()),
        };
        let error = client
            .shutdown()
            .await
            .expect_err("panic cannot be successful shutdown");
        assert_eq!(error.kind(), ErrorKind::Other);
        assert!(error.to_string().contains("worker panicked"));
    }
    #[tokio::test]
    async fn dropping_remote_client_closes_connection_with_request_handles_retained() {
        let (seen_tx, seen_rx) = tokio::sync::oneshot::channel();
        let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();
        let websocket_url = start_test_remote_server(|mut websocket| async move {
            expect_remote_initialize(&mut websocket).await;
            let JSONRPCMessage::Request(request) = read_websocket_message(&mut websocket).await
            else {
                panic!("expected account/read request");
            };
            assert_eq!(request.method, "account/read");
            seen_tx.send(()).expect("request observer remains alive");
            assert!(matches!(
                websocket.next().await,
                Some(Ok(Message::Close(_)))
            ));
            closed_tx.send(()).expect("close observer remains alive");
        })
        .await;
        let client = RemoteAppServerClient::connect(test_remote_connect_args(websocket_url))
            .await
            .expect("remote client should connect");
        let handle = client.request_handle();
        let pending_handle = handle.clone();
        let request = |id| ClientRequest::GetAccount {
            request_id: RequestId::Integer(id),
            params: codex_app_server_protocol::GetAccountParams {
                cache_only: false,
                refresh_token: false,
            },
        };
        let pending = tokio::spawn(async move { pending_handle.request(request(1)).await });
        timeout(SHUTDOWN_TIMEOUT, seen_rx)
            .await
            .expect("request reaches server")
            .expect("server observes request");
        drop(client);
        timeout(SHUTDOWN_TIMEOUT, closed_rx)
            .await
            .expect("dropping the event consumer closes the websocket")
            .expect("server observes close");
        let pending_error = timeout(SHUTDOWN_TIMEOUT, pending)
            .await
            .expect("pending request finishes")
            .expect("request task joins")
            .expect_err("pending request fails when client closes");
        assert_eq!(pending_error.kind(), ErrorKind::BrokenPipe);
        let error = timeout(SHUTDOWN_TIMEOUT, handle.request(request(2)))
            .await
            .expect("retained handle never hangs after client drop")
            .expect_err("retained handle no longer owns a connection");
        assert_eq!(error.kind(), ErrorKind::BrokenPipe);
    }

    fn config_requirements_request(id: i64) -> ClientRequest {
        ClientRequest::ConfigRequirementsRead {
            request_id: RequestId::Integer(id),
            params: None,
        }
    }

    // Owner drop: the event consumer is released and no Shutdown is ever
    // queued, while a cloned handle and an in-flight request are retained.
    #[cfg(unix)]
    #[tokio::test]
    async fn owner_drop_fails_in_flight_request_with_retained_handle() {
        use codex_app_server_protocol::CommandExecParams;
        use codex_app_server_protocol::SandboxPolicy;

        let TestClient {
            _codex_home,
            _host,
            client,
        } = start_test_client(SessionSource::Cli).await;
        let handle = client.request_handle();
        let pending_handle = handle.clone();
        let process_id = "owner-drop-in-flight".to_string();
        let request = ClientRequest::OneOffCommandExec {
            request_id: RequestId::Integer(1),
            params: CommandExecParams {
                command: vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    "printf ready; sleep 30".to_string(),
                ],
                process_id: Some(process_id.clone()),
                tty: false,
                stream_stdin: false,
                stream_stdout_stderr: true,
                output_bytes_cap: None,
                disable_output_cap: false,
                disable_timeout: false,
                timeout_ms: None,
                cwd: None,
                env: None,
                size: None,
                sandbox_policy: Some(SandboxPolicy::DangerFullAccess),
                permission_profile: None,
            },
        };
        let pending = tokio::spawn(async move { pending_handle.request(request).await });
        let InProcessAppServerClient {
            command_tx,
            mut event_rx,
            worker_handle,
            _host: client_host,
        } = client;

        // Output from the started process proves the runtime received the
        // request; its response is deferred until the process exits.
        timeout(SHUTDOWN_TIMEOUT, async {
            loop {
                match event_rx.recv().await {
                    Some(InProcessServerEvent::ServerNotification(notification))
                        if matches!(
                            notification.as_ref(),
                            ServerNotification::CommandExecOutputDelta(delta)
                                if delta.process_id == process_id
                        ) =>
                    {
                        break;
                    }
                    Some(_) => {}
                    None => panic!("event stream closed before the request was in flight"),
                }
            }
        })
        .await
        .expect("command/exec reaches the runtime and starts streaming");
        assert!(!pending.is_finished(), "request must still be in flight");

        // Drop the owner field by field, in declaration order, keeping only the
        // worker JoinHandle so its exit can be observed. Dropping a JoinHandle
        // never affects the task, so this matches dropping the whole client.
        drop(command_tx);
        drop(event_rx);
        drop(client_host);

        timeout(IN_PROCESS_SHUTDOWN_TIMEOUT, worker_handle)
            .await
            .expect("worker exits after its event consumer is dropped")
            .expect("worker does not panic");
        let pending_error = timeout(SHUTDOWN_TIMEOUT, pending)
            .await
            .expect("in-flight request finishes")
            .expect("request task joins")
            .expect_err("in-flight request fails when the owner is dropped");
        assert_eq!(pending_error.kind(), ErrorKind::BrokenPipe);
        let error = timeout(
            SHUTDOWN_TIMEOUT,
            handle.request(config_requirements_request(2)),
        )
        .await
        .expect("retained handle never hangs after owner drop")
        .expect_err("retained handle no longer owns a runtime");
        assert_eq!(error.kind(), ErrorKind::BrokenPipe);
    }

    // Explicit order: Shutdown is queued before the consumer-closed signal is
    // observable. The closed-first worker drains it from the closed channel,
    // so the caller's deadline and ack are used, never the owner-drop path.
    #[tokio::test]
    async fn shutdown_queued_before_consumer_close_takes_explicit_path() {
        let TestClient {
            _codex_home,
            _host,
            client,
        } = start_test_client(SessionSource::Cli).await;
        let handle = client.request_handle();
        let InProcessAppServerClient {
            command_tx,
            event_rx,
            worker_handle,
            _host: _client_host,
        } = client;
        let deadline = Instant::now() + IN_PROCESS_SHUTDOWN_TIMEOUT;
        let (response_tx, response_rx) = oneshot::channel();
        command_tx
            .try_send(ClientCommand::Shutdown {
                response_tx,
                deadline,
            })
            .unwrap_or_else(|_| panic!("shutdown should be queued"));
        drop(event_rx);

        timeout_at(deadline, response_rx)
            .await
            .expect("explicit shutdown is acknowledged before its deadline")
            .expect("explicit shutdown is not short-circuited by consumer close")
            .expect("explicit shutdown should complete");
        timeout_at(deadline, worker_handle)
            .await
            .expect("worker exits after acknowledging shutdown")
            .expect("worker does not panic");
        let error = timeout(
            SHUTDOWN_TIMEOUT,
            handle.request(config_requirements_request(1)),
        )
        .await
        .expect("retained handle never hangs after shutdown")
        .expect_err("retained handle no longer owns a runtime");
        assert_eq!(error.kind(), ErrorKind::BrokenPipe);
    }

    // Owner-drop order: the consumer closes with no Shutdown queued while a
    // command sender is still held. The worker must exit on its own and close
    // the command channel, so a late Shutdown is refused rather than acked.
    #[tokio::test]
    async fn consumer_close_without_shutdown_takes_owner_drop_path() {
        let TestClient {
            _codex_home,
            _host,
            client,
        } = start_test_client(SessionSource::Cli).await;
        let InProcessAppServerClient {
            command_tx,
            event_rx,
            worker_handle,
            _host: _client_host,
        } = client;
        drop(event_rx);

        timeout(IN_PROCESS_SHUTDOWN_TIMEOUT, worker_handle)
            .await
            .expect("worker exits although a command sender is retained")
            .expect("worker does not panic");
        let (response_tx, _response_rx) = oneshot::channel();
        assert!(matches!(
            command_tx.try_send(ClientCommand::Shutdown {
                response_tx,
                deadline: Instant::now() + SHUTDOWN_TIMEOUT,
            }),
            Err(mpsc::error::TrySendError::Closed(_))
        ));
    }

    fn queue_request(
        command_tx: &mpsc::Sender<ClientCommand>,
        id: i64,
    ) -> oneshot::Receiver<IoResult<RequestResult>> {
        let (response_tx, response_rx) = oneshot::channel();
        command_tx
            .try_send(ClientCommand::Request {
                request: Box::new(config_requirements_request(id)),
                response_tx,
            })
            .unwrap_or_else(|_| panic!("request should be buffered"));
        response_rx
    }

    // Retained handles keep sending after the owner drops. The closed-first
    // worker stops admitting commands at once, so the flood cannot delay its
    // exit, and every spammed request fails closed.
    #[tokio::test]
    async fn owner_drop_exits_while_retained_handles_spam_commands() {
        const SPAMMERS: i64 = 4;
        let TestClient {
            _codex_home,
            _host,
            client,
        } = start_test_client(SessionSource::Cli).await;
        let handle = client.request_handle();
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let spammers = (0..SPAMMERS)
            .map(|spammer| {
                let handle = handle.clone();
                let mut started_tx = Some(started_tx.clone());
                tokio::spawn(async move {
                    let mut sequence = 0;
                    loop {
                        sequence += 1;
                        let request = config_requirements_request(spammer * 1_000_000 + sequence);
                        match handle.request(request).await {
                            Ok(_) => {
                                if let Some(started_tx) = started_tx.take() {
                                    let _ = started_tx.send(());
                                }
                            }
                            Err(error) => return error,
                        }
                    }
                })
            })
            .collect::<Vec<_>>();
        drop(started_tx);
        for _ in 0..SPAMMERS {
            timeout(SHUTDOWN_TIMEOUT, started_rx.recv())
                .await
                .expect("every spammer is admitted before the owner drops")
                .expect("spammer reports admission");
        }

        let InProcessAppServerClient {
            command_tx,
            event_rx,
            worker_handle,
            _host: client_host,
        } = client;
        drop(command_tx);
        drop(event_rx);
        drop(client_host);

        timeout(IN_PROCESS_SHUTDOWN_TIMEOUT, worker_handle)
            .await
            .expect("worker exits although retained handles keep sending")
            .expect("worker does not panic");
        for spammer in spammers {
            let error = timeout(SHUTDOWN_TIMEOUT, spammer)
                .await
                .expect("spammer stops once the owner is gone")
                .expect("spammer joins");
            assert_eq!(error.kind(), ErrorKind::BrokenPipe);
        }
    }

    // A Shutdown buffered among spammed commands when the consumer closes
    // still takes the explicit path with its caller deadline and ack; every
    // other buffered command is answered with BrokenPipe, never run.
    #[tokio::test]
    async fn shutdown_buffered_with_spam_takes_explicit_path() {
        let TestClient {
            _codex_home,
            _host,
            client,
        } = start_test_client(SessionSource::Cli).await;
        let InProcessAppServerClient {
            command_tx,
            event_rx,
            worker_handle,
            _host: _client_host,
        } = client;
        // `try_send` cannot yield, so on this current-thread runtime the whole
        // buffer and the consumer close are in place before the worker polls.
        let mut spam = vec![queue_request(&command_tx, 1), queue_request(&command_tx, 2)];
        let deadline = Instant::now() + IN_PROCESS_SHUTDOWN_TIMEOUT;
        let (response_tx, response_rx) = oneshot::channel();
        command_tx
            .try_send(ClientCommand::Shutdown {
                response_tx,
                deadline,
            })
            .unwrap_or_else(|_| panic!("shutdown should be buffered"));
        spam.push(queue_request(&command_tx, 3));
        spam.push(queue_request(&command_tx, 4));
        drop(event_rx);

        timeout_at(deadline, response_rx)
            .await
            .expect("explicit shutdown is acknowledged before its deadline")
            .expect("explicit shutdown is not short-circuited by consumer close")
            .expect("explicit shutdown should complete");
        timeout_at(deadline, worker_handle)
            .await
            .expect("worker exits after acknowledging shutdown")
            .expect("worker does not panic");
        for response_rx in spam {
            let error = response_rx
                .await
                .expect("buffered request is answered")
                .expect_err("buffered request fails closed");
            assert_eq!(error.kind(), ErrorKind::BrokenPipe);
        }
        let (response_tx, _response_rx) = oneshot::channel();
        assert!(matches!(
            command_tx.try_send(ClientCommand::Request {
                request: Box::new(config_requirements_request(5)),
                response_tx,
            }),
            Err(mpsc::error::TrySendError::Closed(_))
        ));
    }

    #[tokio::test]
    async fn explicit_shutdown_with_retained_handle_still_acknowledges() {
        let client = start_test_client(SessionSource::Cli).await;
        let handle = client.request_handle();

        timeout(IN_PROCESS_SHUTDOWN_TIMEOUT, client.shutdown())
            .await
            .expect("explicit shutdown stays within its deadline")
            .expect("explicit shutdown should complete");
        let error = timeout(
            SHUTDOWN_TIMEOUT,
            handle.request(config_requirements_request(1)),
        )
        .await
        .expect("retained handle never hangs after shutdown")
        .expect_err("retained handle no longer owns a runtime");
        assert_eq!(error.kind(), ErrorKind::BrokenPipe);
    }
}
