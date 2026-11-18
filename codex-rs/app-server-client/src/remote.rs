/*
This module implements the remote app-server client transport.

It owns the remote connection lifecycle, including the initialize/initialized
handshake, JSON-RPC request/response routing, server-request resolution, and
notification streaming. Remote connections always carry WebSocket frames, over
either TCP WebSocket URLs or local Unix sockets. The rest of the crate uses the
same `AppServerEvent` surface for both in-process and remote transports, so
callers such as the TUI can switch between them without changing their
higher-level session logic.
*/

use std::collections::HashMap;
use std::collections::VecDeque;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::io::Result as IoResult;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::AppServerEvent;
use crate::RequestResult;
use crate::SHUTDOWN_TIMEOUT;
use crate::TypedRequestError;
use crate::decode_typed_response;
use crate::remote_websocket::RemoteWebSocket;
use crate::remote_websocket::WebSocketHandshake;
use codex_app_server::in_process::ClientWorkerOwner;
use codex_app_server::in_process::InProcessHost;
use codex_app_server::in_process::TaskObservation;
use codex_app_server::in_process::TaskTermination;
use codex_app_server_protocol::ClientInfo;
use codex_app_server_protocol::ClientNotification;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::InitializeCapabilities;
use codex_app_server_protocol::InitializeParams;
use codex_app_server_protocol::JSONRPCError;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::JSONRPCMessage;
use codex_app_server_protocol::JSONRPCNotification;
use codex_app_server_protocol::JSONRPCRequest;
use codex_app_server_protocol::JSONRPCResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::Result as JsonRpcResult;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ServerRequest;
use codex_app_server_transport::WEBSOCKET_COMPRESSION_HEADER;
use codex_app_server_transport::ZSTD_STREAM_PROTOCOL;
use codex_uds::UnixStream;
use codex_utils_absolute_path::AbsolutePathBuf;
use codex_utils_rustls_provider::ensure_rustls_crypto_provider;
use serde::de::DeserializeOwned;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio::time::timeout;
use tokio::time::timeout_at;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::client_async_with_config;
use tokio_tungstenite::connect_async_with_config;
use tokio_tungstenite::tungstenite::Error as TungsteniteError;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tracing::warn;
use url::Url;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const INITIALIZE_TIMEOUT: Duration = Duration::from_secs(10);
const REMOTE_APP_SERVER_MAX_WEBSOCKET_MESSAGE_SIZE: usize = 128 << 20;
// Tungstenite still needs an HTTP request URI for the WebSocket handshake;
// the bytes travel over the Unix socket, not TCP.
const UDS_WEBSOCKET_HANDSHAKE_URL: &str = "ws://localhost/rpc";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteAppServerEndpoint {
    WebSocket {
        websocket_url: String,
        auth_token: Option<String>,
    },
    UnixSocket {
        socket_path: AbsolutePathBuf,
    },
}

#[derive(Debug, Clone)]
pub struct RemoteAppServerConnectArgs {
    /// This client collects fresh human decisions for managed command checkpoints.
    pub required_human_approval: bool,
    /// Request interactive behavior; the server independently proves the peer.
    pub interactive_client: bool,
    pub endpoint: RemoteAppServerEndpoint,
    pub client_name: String,
    pub client_version: String,
    pub experimental_api: bool,
    pub mcp_server_openai_form_elicitation: bool,
    pub opt_out_notification_methods: Vec<String>,
    pub channel_capacity: usize,
}
impl RemoteAppServerConnectArgs {
    pub(crate) fn initialize_params(&self) -> InitializeParams {
        let capabilities = InitializeCapabilities {
            interactive_client: self.interactive_client,
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
}

pub(crate) fn websocket_url_supports_auth_token(url: &Url) -> bool {
    match (url.scheme(), url.host()) {
        ("wss", Some(_)) => true,
        ("ws", Some(url::Host::Domain(domain))) => domain.eq_ignore_ascii_case("localhost"),
        ("ws", Some(url::Host::Ipv4(addr))) => addr.is_loopback(),
        ("ws", Some(url::Host::Ipv6(addr))) => addr.is_loopback(),
        _ => false,
    }
}

enum RemoteClientCommand {
    Request {
        request: Box<JSONRPCRequest>,
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
    },
}

#[derive(Default)]
struct RemoteServerMetadata {
    server_version: Option<String>,
    codex_home: Option<String>,
    platform_family: Option<String>,
    platform_os: Option<String>,
}

pub struct RemoteAppServerClient {
    command_tx: mpsc::Sender<RemoteClientCommand>,
    event_rx: mpsc::UnboundedReceiver<AppServerEvent>,
    pending_events: VecDeque<AppServerEvent>,
    metadata: RemoteServerMetadata,
    worker_handle: RemoteWorker,
    terminal_error: WorkerTerminalError,
    worker_custody: Option<Arc<InProcessHost>>,
    final_exit: FinalExitLatch,
}

/// The transport failure or final-exit deadline the worker observed before it stopped. The worker
/// records it before refusing queued commands, so `shutdown` reports the same
/// failure whether its close was refused in the queue or could no longer be
/// queued because the worker had already exited.
#[derive(Clone, Default)]
struct WorkerTerminalError {
    failure: Arc<std::sync::Mutex<Option<(ErrorKind, String)>>>,
    #[cfg(test)]
    write_pending: Arc<tokio::sync::Notify>,
}

impl WorkerTerminalError {
    /// Keeps the first failure; a worker stops once, so later records are not expected.
    fn record(&self, kind: ErrorKind, message: &str) {
        let mut slot = self
            .failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot.is_none() {
            *slot = Some((kind, message.to_string()));
        }
    }

    fn error(&self) -> Option<IoError> {
        self.failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|(kind, message)| IoError::new(*kind, message.clone()))
    }
}

enum RemoteWorker {
    Unhosted(Option<tokio::task::JoinHandle<()>>),
    Hosted(Arc<ClientWorkerOwner>),
}
impl RemoteWorker {
    async fn observe_until(&mut self, deadline: Instant) -> IoResult<()> {
        match self {
            Self::Hosted(owner) => match owner.observe_until(deadline).await {
                TaskObservation::Terminated(TaskTermination::Normal) => Ok(()),
                TaskObservation::Terminated(other) => Err(IoError::other(format!(
                    "remote app-server worker join failed: {other:?}"
                ))),
                TaskObservation::TimedOut => Err(IoError::new(
                    ErrorKind::TimedOut,
                    "remote app-server worker shutdown timed out; original retained by run host",
                )),
            },
            Self::Unhosted(Some(handle)) => match timeout_at(deadline, handle).await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(IoError::other(format!(
                    "remote app-server worker join failed: {error}"
                ))),
                Err(_) => Err(IoError::new(
                    ErrorKind::TimedOut,
                    "remote app-server worker shutdown timed out; unhosted original observer is unretained",
                )),
            },
            Self::Unhosted(None) => Err(IoError::other(
                "remote app-server worker observer unavailable",
            )),
        }
    }
}

/// Requests that tear down state this connection owns and that the final-exit
/// path legitimately issues. Exact methods only:
/// - hosted-endpoint declare and retire (tui `app_server_session.rs`
///   `retire_hosted_mcp_endpoint`);
/// - retention release from settling adoptions
///   (tui `app_server_session/retention.rs` `ThreadRetentionRelease`).
///
/// Admission name for answers to server requests (resolve and reject). Not
/// owned teardown, so it is refused at the edge.
const SERVER_REQUEST_RESPONSE: &str = "server/request/response";

const OWNED_TEARDOWN_METHODS: &[&str] = &[
    "kcf/mcpServer/hostedEndpoint/declare",
    "kcf/mcpServer/hostedEndpoint/retire",
    "thread/retention/release",
];

/// Shared final-exit state: the edge, and the EARLIEST deadline any owner
/// latched. Every handle and the worker observe the same latch.
#[derive(Clone)]
struct FinalExitLatch {
    edge: Arc<AtomicBool>,
    deadline: Arc<watch::Sender<Option<tokio::time::Instant>>>,
}

impl FinalExitLatch {
    fn new() -> Self {
        Self {
            edge: Arc::new(AtomicBool::new(false)),
            deadline: Arc::new(watch::channel(None).0),
        }
    }

    /// Sets the edge and latches the earliest deadline; never extends it.
    fn begin(&self, deadline: tokio::time::Instant) -> tokio::time::Instant {
        self.edge.store(true, Ordering::Release);
        self.deadline.send_modify(|latched| {
            *latched = Some(latched.map_or(deadline, |current| current.min(deadline)));
        });
        self.deadline().unwrap_or(deadline)
    }

    fn deadline(&self) -> Option<tokio::time::Instant> {
        *self.deadline.borrow()
    }

    fn expired(&self) -> bool {
        self.deadline()
            .is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
    }

    /// After expiry nothing is admitted; at the edge only exact owned
    /// teardown methods are admitted.
    fn admit(&self, method: &str) -> IoResult<()> {
        if self.expired() {
            return Err(IoError::new(
                ErrorKind::TimedOut,
                "remote app-server final-exit deadline has expired; no work is admitted",
            ));
        }
        if self.edge.load(Ordering::Acquire) && !OWNED_TEARDOWN_METHODS.contains(&method) {
            return Err(IoError::new(
                ErrorKind::ConnectionAborted,
                format!(
                    "remote app-server connection is in final exit; `{method}` is not an owned teardown request"
                ),
            ));
        }
        Ok(())
    }
}

/// Resolves only when a latched final-exit deadline is reached. Cancel-safe.
async fn final_exit_deadline_reached(latch: &mut watch::Receiver<Option<tokio::time::Instant>>) {
    loop {
        let current = *latch.borrow_and_update();
        match current {
            Some(deadline) => {
                tokio::select! {
                    () = tokio::time::sleep_until(deadline) => return,
                    changed = latch.changed() => {
                        if changed.is_err() {
                            tokio::time::sleep_until(deadline).await;
                            return;
                        }
                    }
                }
            }
            None => {
                if latch.changed().await.is_err() {
                    std::future::pending::<()>().await;
                }
            }
        }
    }
}

fn final_exit_incomplete() -> (ErrorKind, String) {
    (
        ErrorKind::TimedOut,
        "remote app-server final-exit deadline expired; the worker stopped incomplete".to_string(),
    )
}

/// Only Closed proves the close and original worker join completed.
/// TimedOut carries an error, never the original: hosted originals already
/// belong to the run host before any cancellable shutdown starts.
#[derive(Debug)]
pub enum FinalClose {
    Closed,
    CloseFailed(IoError),
    TimedOut(IoError),
    JoinFailed(String),
}

impl FinalClose {
    pub fn is_closed(&self) -> bool {
        matches!(self, Self::Closed)
    }
    fn into_result(self) -> IoResult<()> {
        match self {
            Self::Closed => Ok(()),
            Self::CloseFailed(error) | Self::TimedOut(error) => Err(error),
            Self::JoinFailed(message) => Err(IoError::other(message)),
        }
    }
}

#[derive(Clone)]
pub struct RemoteAppServerRequestHandle {
    command_tx: mpsc::Sender<RemoteClientCommand>,
    final_exit: FinalExitLatch,
}

enum SocketPeerPolicy {
    ExplicitEndpoint,
    #[cfg(windows)]
    NonElevatedCurrentUser,
}

/// Test-only observation of a production write entering the pending state.
/// Outside tests this is the identity, so production behaviour is unchanged.
#[cfg(test)]
fn observe_write_pending<F>(
    write: F,
    fence: &WorkerTerminalError,
) -> impl std::future::Future<Output = F::Output>
where
    F: std::future::Future,
{
    struct PendingSignal<F> {
        write: std::pin::Pin<Box<F>>,
        notify: Arc<tokio::sync::Notify>,
        signalled: bool,
    }
    impl<F: std::future::Future> std::future::Future for PendingSignal<F> {
        type Output = F::Output;
        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<F::Output> {
            let poll = std::future::Future::poll(self.write.as_mut(), cx);
            if poll.is_pending() && !self.signalled {
                self.signalled = true;
                self.notify.notify_one();
            }
            poll
        }
    }
    PendingSignal {
        write: Box::pin(write),
        notify: Arc::clone(&fence.write_pending),
        signalled: false,
    }
}

#[cfg(not(test))]
fn observe_write_pending<F>(write: F, _fence: &WorkerTerminalError) -> F {
    write
}

/// Answers a command that will never be executed, explicitly.
fn refuse_command(command: RemoteClientCommand, kind: ErrorKind, message: &str) {
    let error = || IoError::new(kind, message.to_string());
    match command {
        RemoteClientCommand::Request { response_tx, .. } => {
            let _ = response_tx.send(Err(error()));
        }
        RemoteClientCommand::Notify { response_tx, .. }
        | RemoteClientCommand::ResolveServerRequest { response_tx, .. }
        | RemoteClientCommand::RejectServerRequest { response_tx, .. }
        | RemoteClientCommand::Shutdown { response_tx } => {
            let _ = response_tx.send(Err(error()));
        }
    }
}

impl RemoteAppServerClient {
    /// Bind before exposing this client to cancellable shutdown. The run owner
    /// must retain its own Arc across replaced sessions and incomplete results.
    pub fn with_worker_custody(mut self, host: Arc<InProcessHost>) -> IoResult<Self> {
        let RemoteWorker::Unhosted(slot) = &mut self.worker_handle else {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                "remote worker custody already bound",
            ));
        };
        let handle = slot
            .take()
            .ok_or_else(|| IoError::other("remote worker handle unavailable"))?;
        match host.bind_client_worker(handle) {
            Ok(owner) => self.worker_handle = RemoteWorker::Hosted(owner),
            Err(handle) => {
                *slot = Some(handle);
                return Err(IoError::other("run host refused remote worker custody"));
            }
        }
        self.worker_custody = Some(host);
        Ok(self)
    }

    pub fn worker_custody(&self) -> Option<Arc<InProcessHost>> {
        self.worker_custody.clone()
    }

    pub async fn connect(args: RemoteAppServerConnectArgs) -> IoResult<Self> {
        Self::connect_with_policy(args, SocketPeerPolicy::ExplicitEndpoint).await
    }

    /// Connects to an implicitly discovered Windows daemon, verifying its peer
    /// token before the WebSocket handshake or any session requests.
    #[cfg(windows)]
    pub async fn connect_local_daemon(args: RemoteAppServerConnectArgs) -> IoResult<Self> {
        if !matches!(args.endpoint, RemoteAppServerEndpoint::UnixSocket { .. }) {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                "local daemon requires a Unix socket",
            ));
        }
        Self::connect_with_policy(args, SocketPeerPolicy::NonElevatedCurrentUser).await
    }

    async fn connect_with_policy(
        args: RemoteAppServerConnectArgs,
        peer_policy: SocketPeerPolicy,
    ) -> IoResult<Self> {
        let channel_capacity = args.channel_capacity.max(1);
        let initialize_params = args.initialize_params();
        match args.endpoint {
            RemoteAppServerEndpoint::WebSocket {
                websocket_url,
                auth_token,
            } => {
                let (endpoint, stream) =
                    connect_websocket_endpoint(websocket_url, auth_token).await?;
                Self::connect_with_stream(channel_capacity, endpoint, stream, initialize_params)
                    .await
            }
            RemoteAppServerEndpoint::UnixSocket { socket_path } => {
                let (endpoint, stream) =
                    connect_unix_socket_endpoint(socket_path, peer_policy).await?;
                Self::connect_with_stream(channel_capacity, endpoint, stream, initialize_params)
                    .await
            }
        }
    }

    pub fn server_version(&self) -> Option<&str> {
        self.metadata.server_version.as_deref()
    }

    pub fn codex_home(&self) -> Option<&str> {
        self.metadata.codex_home.as_deref()
    }

    pub fn platform_family(&self) -> Option<&str> {
        self.metadata.platform_family.as_deref()
    }

    pub fn platform_os(&self) -> Option<&str> {
        self.metadata.platform_os.as_deref()
    }

    async fn connect_with_stream<S>(
        channel_capacity: usize,
        endpoint: String,
        stream: WebSocketHandshake<S>,
        initialize_params: InitializeParams,
    ) -> IoResult<Self>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let mut stream =
            RemoteWebSocket::from_handshake(stream, REMOTE_APP_SERVER_MAX_WEBSOCKET_MESSAGE_SIZE)?;
        let (pending_events, metadata) = initialize_remote_connection(
            &mut stream,
            &endpoint,
            initialize_params,
            INITIALIZE_TIMEOUT,
        )
        .await?;

        let (command_tx, mut command_rx) = mpsc::channel::<RemoteClientCommand>(channel_capacity);
        let (event_tx, event_rx) = mpsc::unbounded_channel::<AppServerEvent>();
        let terminal_error = WorkerTerminalError::default();
        let worker_terminal_error = terminal_error.clone();
        let final_exit = FinalExitLatch::new();
        let worker_final_exit = final_exit.clone();
        let mut final_deadline = final_exit.deadline.subscribe();
        let worker_handle = tokio::spawn(async move {
            let mut pending_requests =
                HashMap::<RequestId, oneshot::Sender<IoResult<RequestResult>>>::new();
            let mut worker_exit_error: Option<(ErrorKind, String)> = None;
            let mut transport_failed = false;
            // The outer deadline covers the whole worker operation, including
            // a blocked socket write or close. Dropping that operation is an
            // incomplete terminal outcome, never a successful close or abort
            // of a separately owned task. The original join stays in its host.
            tokio::select! {
                biased;
                () = final_exit_deadline_reached(&mut final_deadline) => {
                    let (kind, message) = final_exit_incomplete();
                    worker_terminal_error.record(kind, &message);
                    worker_exit_error = Some((kind, message));
                }
                () = async {
            loop {
                tokio::select! {
                    biased;
                    () = event_tx.closed() => {
                        let _ = timeout(SHUTDOWN_TIMEOUT, stream.close()).await;
                        break;
                    }
                    command = command_rx.recv() => {
                        let Some(command) = command else {
                            let _ = stream.close().await;
                            break;
                        };
                        let method = match &command {
                            RemoteClientCommand::Request { request, .. } => Some(request.method.clone()),
                            RemoteClientCommand::Notify { notification, .. } => Some(notification.to_string()),
                            RemoteClientCommand::ResolveServerRequest { .. }
                            | RemoteClientCommand::RejectServerRequest { .. } => Some(SERVER_REQUEST_RESPONSE.to_owned()),
                            RemoteClientCommand::Shutdown { .. } => None,
                        };
                        if let Some(method) = method
                            && let Err(error) = worker_final_exit.admit(&method)
                        {
                            refuse_command(command, error.kind(), &format!("{error} (refused at dequeue)"));
                            continue;
                        }
                        match command {
                            RemoteClientCommand::Request { request, response_tx } => {
                                let request_id = request.id.clone();
                                if pending_requests.contains_key(&request_id) {
                                    let _ = response_tx.send(Err(IoError::new(
                                        ErrorKind::InvalidInput,
                                        format!("duplicate remote app-server request id `{request_id}`"),
                                    )));
                                    continue;
                                }
                                pending_requests.insert(request_id.clone(), response_tx);
                                if let Err(err) = observe_write_pending(write_jsonrpc_message(
                                    &mut stream,
                                    JSONRPCMessage::Request(*request),
                                    &endpoint,
                                ), &worker_terminal_error)
                                .await
                                {
                                    let err_message = err.to_string();
                                    let message = format!(
                                        "remote app server at `{endpoint}` write failed: {err_message}"
                                    );
                                    if let Some(response_tx) = pending_requests.remove(&request_id) {
                                        let _ = response_tx.send(Err(err));
                                    }
                                    let _ = deliver_event(
                                        &event_tx,
                                        AppServerEvent::Disconnected {
                                            message: message.clone(),
                                        },
                                    );
                                    transport_failed = true;
                                    worker_exit_error = Some((ErrorKind::BrokenPipe, message));
                                    break;
                                }
                            }
                            RemoteClientCommand::Notify { notification, response_tx } => {
                                let result = write_jsonrpc_message(
                                    &mut stream,
                                    JSONRPCMessage::Notification(
                                        jsonrpc_notification_from_client_notification(notification),
                                    ),
                                    &endpoint,
                                )
                                .await;
                                let failure = result.as_ref().err().map(|error| format!(
                                    "remote app server at `{endpoint}` write failed: {error}"
                                ));
                                let _ = response_tx.send(result);
                                if let Some(message) = failure {
                                    let _ = deliver_event(&event_tx, AppServerEvent::Disconnected { message: message.clone() });
                                    transport_failed = true;
                                    worker_exit_error = Some((ErrorKind::BrokenPipe, message));
                                    break;
                                }
                            }
                            RemoteClientCommand::ResolveServerRequest {
                                request_id,
                                result,
                                response_tx,
                            } => {
                                let result = write_jsonrpc_message(
                                    &mut stream,
                                    JSONRPCMessage::Response(JSONRPCResponse {
                                        id: request_id,
                                        result,
                                    }),
                                    &endpoint,
                                )
                                .await;
                                let failure = result.as_ref().err().map(|error| format!(
                                    "remote app server at `{endpoint}` write failed: {error}"
                                ));
                                let _ = response_tx.send(result);
                                if let Some(message) = failure {
                                    let _ = deliver_event(&event_tx, AppServerEvent::Disconnected { message: message.clone() });
                                    transport_failed = true;
                                    worker_exit_error = Some((ErrorKind::BrokenPipe, message));
                                    break;
                                }
                            }
                            RemoteClientCommand::RejectServerRequest {
                                request_id,
                                error,
                                response_tx,
                            } => {
                                let result = write_jsonrpc_message(
                                    &mut stream,
                                    JSONRPCMessage::Error(JSONRPCError {
                                        error,
                                        id: request_id,
                                    }),
                                    &endpoint,
                                )
                                .await;
                                let failure = result.as_ref().err().map(|error| format!(
                                    "remote app server at `{endpoint}` write failed: {error}"
                                ));
                                let _ = response_tx.send(result);
                                if let Some(message) = failure {
                                    let _ = deliver_event(&event_tx, AppServerEvent::Disconnected { message: message.clone() });
                                    transport_failed = true;
                                    worker_exit_error = Some((ErrorKind::BrokenPipe, message));
                                    break;
                                }
                            }
                            RemoteClientCommand::Shutdown { response_tx } => {
                                let close_result = stream.close().await.or_else(|err| {
                                    if websocket_close_error_is_already_closed(&err) {
                                        Ok(())
                                    } else {
                                        Err(IoError::other(format!(
                                            "failed to close websocket app server `{endpoint}`: {err}"
                                        )))
                                    }
                                });
                                let _ = response_tx.send(close_result);
                                break;
                            }
                        }
                    }
                    message = stream.next() => {
                        match message {
                            Some(Ok(Message::Text(text))) => {
                                match serde_json::from_str::<JSONRPCMessage>(&text) {
                                    Ok(JSONRPCMessage::Response(response)) => {
                                        if let Some(response_tx) = pending_requests.remove(&response.id) {
                                            let _ = response_tx.send(Ok(Ok(response.result)));
                                        }
                                    }
                                    Ok(JSONRPCMessage::Error(error)) => {
                                        if let Some(response_tx) = pending_requests.remove(&error.id) {
                                            let _ = response_tx.send(Ok(Err(error.error)));
                                        }
                                    }
                                    Ok(JSONRPCMessage::Notification(notification)) => {
                                        if let Some(event) =
                                            app_server_event_from_notification(notification)
                                            && let Err(err) = deliver_event(
                                                &event_tx,
                                                event,
                                            )
                                            {
                                                warn!(%err, "failed to deliver remote app-server event");
                                                break;
                                            }
                                    }
                                    Ok(JSONRPCMessage::Request(request)) => {
                                        let request_id = request.id.clone();
                                        let method = request.method.clone();
                                        match ServerRequest::try_from(request) {
                                            Ok(request) => {
                                                if let Err(err) = deliver_event(
                                                    &event_tx,
                                                    AppServerEvent::ServerRequest(Box::new(request)),
                                                )
                                                {
                                                    warn!(%err, "failed to deliver remote app-server server request");
                                                    break;
                                                }
                                            }
                                            Err(err) => {
                                                warn!(%err, method, "rejecting unknown remote app-server request");
                                                if worker_final_exit.admit(SERVER_REQUEST_RESPONSE).is_err() {
                                                    continue;
                                                }
                                                if let Err(reject_err) = write_jsonrpc_message(
                                                    &mut stream,
                                                    JSONRPCMessage::Error(JSONRPCError {
                                                        error: JSONRPCErrorError {
                                                            code: -32601,
                                                            message: format!(
                                                                "unsupported remote app-server request `{method}`"
                                                            ),
                                                            data: None,
                                                        },
                                                        id: request_id,
                                                    }),
                                                    &endpoint,
                                                )
                                                .await
                                                {
                                                    let err_message = reject_err.to_string();
                                                    let message = format!(
                                                        "remote app server at `{endpoint}` write failed: {err_message}"
                                                    );
                                                    let _ = deliver_event(
                                                        &event_tx,
                                                        AppServerEvent::Disconnected {
                                                            message: message.clone(),
                                                        },
                                                    );
                                                    transport_failed = true;
                                                    worker_exit_error =
                                                        Some((ErrorKind::BrokenPipe, message));
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        let message = format!(
                                            "remote app server at `{endpoint}` sent invalid JSON-RPC: {err}"
                                        );
                                        let _ = deliver_event(
                                            &event_tx,
                                            AppServerEvent::Disconnected {
                                                message: message.clone(),
                                            },
                                        );
                                        transport_failed = true;
                                        worker_exit_error =
                                            Some((ErrorKind::InvalidData, message));
                                        break;
                                    }
                                }
                            }
                            Some(Ok(Message::Close(frame))) => {
                                let reason = frame
                                    .as_ref()
                                    .map(|frame| frame.reason.to_string())
                                    .filter(|reason| !reason.is_empty())
                                    .unwrap_or_else(|| "connection closed".to_string());
                                let message = format!(
                                    "remote app server at `{endpoint}` disconnected: {reason}"
                                );
                                let _ = deliver_event(
                                    &event_tx,
                                    AppServerEvent::Disconnected {
                                        message: message.clone(),
                                    },
                                );
                                worker_exit_error = Some((
                                    ErrorKind::ConnectionAborted,
                                    message,
                                ));
                                break;
                            }
                            Some(Ok(Message::Binary(_)))
                            | Some(Ok(Message::Ping(_)))
                            | Some(Ok(Message::Pong(_)))
                            | Some(Ok(Message::Frame(_))) => {}
                            Some(Err(err)) => {
                                let message = format!(
                                    "remote app server at `{endpoint}` transport failed: {err}"
                                );
                                let _ = deliver_event(
                                    &event_tx,
                                    AppServerEvent::Disconnected {
                                        message: message.clone(),
                                    },
                                );
                                transport_failed = true;
                                worker_exit_error = Some((ErrorKind::InvalidData, message));
                                break;
                            }
                            None => {
                                let message = format!(
                                    "remote app server at `{endpoint}` closed the connection"
                                );
                                let _ = deliver_event(
                                    &event_tx,
                                    AppServerEvent::Disconnected {
                                        message: message.clone(),
                                    },
                                );
                                worker_exit_error = Some((ErrorKind::UnexpectedEof, message));
                                break;
                            }
                        }
                    }
                }
            }

                } => {}
            }

            let (err_kind, err_message) = worker_exit_error.unwrap_or_else(|| {
                (
                    ErrorKind::BrokenPipe,
                    "remote app-server worker channel is closed".to_string(),
                )
            });
            if transport_failed {
                worker_terminal_error.record(err_kind, &err_message);
            }
            for (_, response_tx) in pending_requests {
                let _ = response_tx.send(Err(IoError::new(err_kind, err_message.clone())));
            }
            command_rx.close();
            while let Some(command) = command_rx.recv().await {
                refuse_command(command, err_kind, &err_message);
            }
        });

        Ok(Self {
            command_tx,
            event_rx,
            pending_events: pending_events.into(),
            metadata,
            worker_handle: RemoteWorker::Unhosted(Some(worker_handle)),
            terminal_error,
            worker_custody: None,
            final_exit,
        })
    }

    pub fn request_handle(&self) -> RemoteAppServerRequestHandle {
        RemoteAppServerRequestHandle {
            command_tx: self.command_tx.clone(),
            final_exit: self.final_exit.clone(),
        }
    }

    pub async fn request(&self, request: ClientRequest) -> IoResult<RequestResult> {
        self.request_handle().request(request).await
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
        self.final_exit.admit(&notification.to_string())?;
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(RemoteClientCommand::Notify {
                notification,
                response_tx,
            })
            .await
            .map_err(|_| {
                IoError::new(
                    ErrorKind::BrokenPipe,
                    "remote app-server worker channel is closed",
                )
            })?;
        response_rx.await.map_err(|_| {
            IoError::new(
                ErrorKind::BrokenPipe,
                "remote app-server notify channel is closed",
            )
        })?
    }

    pub async fn resolve_server_request(
        &self,
        request_id: RequestId,
        result: JsonRpcResult,
    ) -> IoResult<()> {
        self.request_handle()
            .resolve_server_request(request_id, result)
            .await
    }

    pub async fn reject_server_request(
        &self,
        request_id: RequestId,
        error: JSONRPCErrorError,
    ) -> IoResult<()> {
        self.request_handle()
            .reject_server_request(request_id, error)
            .await
    }

    pub async fn next_event(&mut self) -> Option<AppServerEvent> {
        if let Some(event) = self.pending_events.pop_front() {
            return Some(event);
        }
        self.event_rx.recv().await
    }

    pub fn begin_final_exit(&self, deadline: Instant) {
        self.final_exit.begin(deadline);
    }

    pub async fn shutdown_by(self, deadline: Instant) -> FinalClose {
        let deadline = self.final_exit.begin(deadline);
        self.close_until(deadline).await
    }

    pub async fn shutdown(self) -> IoResult<()> {
        // Reconnect shutdown remains unlatched and has its ordinary budget.
        self.close_until(Instant::now() + SHUTDOWN_TIMEOUT)
            .await
            .into_result()
    }

    async fn close_until(self, deadline: Instant) -> FinalClose {
        let Self {
            command_tx,
            event_rx,
            pending_events: _,
            metadata: _,
            mut worker_handle,
            terminal_error,
            worker_custody: _host,
            final_exit: _latch,
        } = self;
        // One caller budget covers admission, acknowledgement and original join.
        // Hosted originals remain in the run host even if this observer is cancelled.
        let (response_tx, response_rx) = oneshot::channel();
        let close_error = if Instant::now() >= deadline {
            Some(IoError::new(
                ErrorKind::TimedOut,
                "remote app-server final-exit deadline expired before close admission",
            ))
        } else {
            match timeout_at(
                deadline,
                command_tx.send(RemoteClientCommand::Shutdown { response_tx }),
            )
            .await
            {
                Ok(Ok(())) => match timeout_at(deadline, response_rx).await {
                    Ok(Ok(Ok(()))) => None,
                    Ok(Ok(Err(error))) => Some(error),
                    Ok(Err(_)) => terminal_error.error(),
                    Err(_) => Some(IoError::new(
                        ErrorKind::TimedOut,
                        "remote app-server shutdown acknowledgement timed out",
                    )),
                },
                Ok(Err(_)) => terminal_error.error(),
                Err(_) => Some(IoError::new(
                    ErrorKind::TimedOut,
                    "remote app-server shutdown admission timed out",
                )),
            }
        };
        let joined = worker_handle.observe_until(deadline).await;
        // Preserve event delivery until the close and original join are observed.
        drop(event_rx);
        match joined {
            Err(error) if error.kind() == ErrorKind::TimedOut => FinalClose::TimedOut(error),
            Err(error) => FinalClose::JoinFailed(error.to_string()),
            Ok(()) => match close_error {
                None => FinalClose::Closed,
                Some(error) if error.kind() == ErrorKind::TimedOut => FinalClose::TimedOut(error),
                Some(error) => FinalClose::CloseFailed(error),
            },
        }
    }
}

impl RemoteAppServerRequestHandle {
    pub(crate) async fn resolve_server_request(
        &self,
        request_id: RequestId,
        result: JsonRpcResult,
    ) -> IoResult<()> {
        self.final_exit.admit(SERVER_REQUEST_RESPONSE)?;
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(RemoteClientCommand::ResolveServerRequest {
                request_id,
                result,
                response_tx,
            })
            .await
            .map_err(|_| {
                IoError::new(
                    ErrorKind::BrokenPipe,
                    "remote app-server worker channel is closed",
                )
            })?;
        response_rx.await.map_err(|_| {
            IoError::new(
                ErrorKind::BrokenPipe,
                "remote app-server resolve channel is closed",
            )
        })?
    }

    pub(crate) async fn reject_server_request(
        &self,
        request_id: RequestId,
        error: JSONRPCErrorError,
    ) -> IoResult<()> {
        self.final_exit.admit(SERVER_REQUEST_RESPONSE)?;
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(RemoteClientCommand::RejectServerRequest {
                request_id,
                error,
                response_tx,
            })
            .await
            .map_err(|_| {
                IoError::new(
                    ErrorKind::BrokenPipe,
                    "remote app-server worker channel is closed",
                )
            })?;
        response_rx.await.map_err(|_| {
            IoError::new(
                ErrorKind::BrokenPipe,
                "remote app-server reject channel is closed",
            )
        })?
    }

    pub async fn request(&self, request: ClientRequest) -> IoResult<RequestResult> {
        self.request_json_rpc(jsonrpc_request_from_client_request(request))
            .await
    }

    pub async fn request_json_rpc(&self, request: JSONRPCRequest) -> IoResult<RequestResult> {
        self.final_exit.admit(&request.method)?;
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(RemoteClientCommand::Request {
                request: Box::new(request),
                response_tx,
            })
            .await
            .map_err(|_| {
                IoError::new(
                    ErrorKind::BrokenPipe,
                    "remote app-server worker channel is closed",
                )
            })?;
        response_rx.await.map_err(|_| {
            IoError::new(
                ErrorKind::BrokenPipe,
                "remote app-server request channel is closed",
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

async fn connect_websocket_endpoint(
    websocket_url: String,
    auth_token: Option<String>,
) -> IoResult<(String, WebSocketHandshake<MaybeTlsStream<TcpStream>>)> {
    let url = Url::parse(&websocket_url).map_err(|err| {
        IoError::new(
            ErrorKind::InvalidInput,
            format!("invalid websocket URL `{websocket_url}`: {err}"),
        )
    })?;
    if auth_token.is_some() && !websocket_url_supports_auth_token(&url) {
        return Err(IoError::new(
            ErrorKind::InvalidInput,
            format!(
                "remote auth tokens require `wss://` or loopback `ws://` URLs; got `{websocket_url}`"
            ),
        ));
    }

    let mut request = websocket_request(url.as_str()).map_err(|err| {
        IoError::new(
            ErrorKind::InvalidInput,
            format!("invalid websocket URL `{websocket_url}`: {err}"),
        )
    })?;
    if let Some(auth_token) = auth_token.as_deref() {
        let header_value =
            HeaderValue::from_str(&format!("Bearer {auth_token}")).map_err(|err| {
                IoError::new(
                    ErrorKind::InvalidInput,
                    format!("invalid remote authorization header value: {err}"),
                )
            })?;
        request.headers_mut().insert(AUTHORIZATION, header_value);
    }

    ensure_rustls_crypto_provider();
    let websocket_config = remote_websocket_config();
    let stream = timeout(
        CONNECT_TIMEOUT,
        connect_async_with_config(
            request,
            Some(websocket_config),
            /*disable_nagle*/ false,
        ),
    )
    .await
    .map_err(|_| {
        IoError::new(
            ErrorKind::TimedOut,
            format!("timed out connecting to remote app server at `{websocket_url}`"),
        )
    })?
    .map_err(|err| {
        IoError::other(format!(
            "failed to connect to remote app server at `{websocket_url}`: {err}"
        ))
    })?;

    Ok((websocket_url, stream))
}

async fn connect_unix_socket_endpoint(
    socket_path: AbsolutePathBuf,
    peer_policy: SocketPeerPolicy,
) -> IoResult<(String, WebSocketHandshake<UnixStream>)> {
    let endpoint = format!("unix://{}", socket_path.display());
    let request = websocket_request(UDS_WEBSOCKET_HANDSHAKE_URL).map_err(|err| {
        IoError::new(
            ErrorKind::InvalidInput,
            format!("invalid UDS websocket handshake URL: {err}"),
        )
    })?;
    let stream = timeout(CONNECT_TIMEOUT, UnixStream::connect(socket_path.as_path()))
        .await
        .map_err(|_| {
            IoError::new(
                ErrorKind::TimedOut,
                format!("timed out connecting to remote app server at `{endpoint}`"),
            )
        })?
        .map_err(|err| {
            IoError::other(format!(
                "failed to connect to remote app server at `{endpoint}`: {err}"
            ))
        })?;
    if let Some(policy) = std::env::var_os("CODEX_UNIX_PEER_POLICY") {
        if policy != "same-image" {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                "unsupported Unix peer policy",
            ));
        }
        codex_app_server_transport::PeerExecutableIdentity::verify_same_image_peer(&stream)?;
    }
    match peer_policy {
        SocketPeerPolicy::ExplicitEndpoint => {}
        #[cfg(windows)]
        SocketPeerPolicy::NonElevatedCurrentUser => stream.ensure_non_elevated_peer()?,
    }
    let websocket_config = remote_websocket_config();
    let stream = timeout(
        CONNECT_TIMEOUT,
        client_async_with_config(request, stream, Some(websocket_config)),
    )
    .await
    .map_err(|_| {
        IoError::new(
            ErrorKind::TimedOut,
            format!("timed out upgrading remote app server at `{endpoint}`"),
        )
    })?
    .map_err(|err| {
        IoError::other(format!(
            "failed to upgrade remote app server at `{endpoint}`: {err}"
        ))
    })?;

    Ok((endpoint, stream))
}

fn websocket_request(url: &str) -> Result<Request, TungsteniteError> {
    let mut request = url.into_client_request()?;
    request.headers_mut().insert(
        WEBSOCKET_COMPRESSION_HEADER,
        HeaderValue::from_static(ZSTD_STREAM_PROTOCOL),
    );
    Ok(request)
}

fn remote_websocket_config() -> WebSocketConfig {
    WebSocketConfig::default()
        .max_frame_size(Some(REMOTE_APP_SERVER_MAX_WEBSOCKET_MESSAGE_SIZE))
        .max_message_size(Some(REMOTE_APP_SERVER_MAX_WEBSOCKET_MESSAGE_SIZE))
}

async fn initialize_remote_connection<S>(
    stream: &mut RemoteWebSocket<S>,
    endpoint: &str,
    params: InitializeParams,
    initialize_timeout: Duration,
) -> IoResult<(Vec<AppServerEvent>, RemoteServerMetadata)>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let initialize_request_id = RequestId::String("initialize".to_string());
    let mut pending_events = Vec::new();
    let mut metadata = RemoteServerMetadata::default();
    write_jsonrpc_message(
        stream,
        JSONRPCMessage::Request(jsonrpc_request_from_client_request(
            ClientRequest::Initialize {
                request_id: initialize_request_id.clone(),
                params,
            },
        )),
        endpoint,
    )
    .await?;

    timeout(initialize_timeout, async {
        loop {
            match stream.next().await {
                Some(Ok(Message::Text(text))) => {
                    let message = serde_json::from_str::<JSONRPCMessage>(&text).map_err(|err| {
                        IoError::other(format!(
                            "remote app server at `{endpoint}` sent invalid initialize response: {err}"
                        ))
                    })?;
                    match message {
                        JSONRPCMessage::Response(response) if response.id == initialize_request_id => {
                            metadata.server_version = response
                                .result
                                .get("userAgent")
                                .and_then(serde_json::Value::as_str)
                                .and_then(|user_agent| {
                                    let (_, rest) = user_agent.split_once('/')?;
                                    rest.split_whitespace().next().map(str::to_string)
                                });
                            metadata.codex_home = response
                                .result
                                .get("codexHome")
                                .and_then(serde_json::Value::as_str)
                                .filter(|codex_home| !codex_home.is_empty())
                                .map(str::to_string);
                            metadata.platform_family = response.result.get("platformFamily")
                                .and_then(serde_json::Value::as_str).map(str::to_string);
                            metadata.platform_os = response.result.get("platformOs")
                                .and_then(serde_json::Value::as_str).map(str::to_string);
                            break Ok(());
                        }
                        JSONRPCMessage::Error(error) if error.id == initialize_request_id => {
                            break Err(IoError::other(format!(
                                "remote app server at `{endpoint}` rejected initialize: {}",
                                error.error.message
                            )));
                        }
                        JSONRPCMessage::Notification(notification) => {
                            if let Some(event) = app_server_event_from_notification(notification) {
                                pending_events.push(event);
                            }
                        }
                        JSONRPCMessage::Request(request) => {
                            let request_id = request.id.clone();
                            let method = request.method.clone();
                            match ServerRequest::try_from(request) {
                                Ok(request) => {
                                    pending_events
                                        .push(AppServerEvent::ServerRequest(Box::new(request)));
                                }
                                Err(err) => {
                                    warn!(%err, method, "rejecting unknown remote app-server request during initialize");
                                    write_jsonrpc_message(
                                        stream,
                                        JSONRPCMessage::Error(JSONRPCError {
                                            error: JSONRPCErrorError {
                                                code: -32601,
                                                message: format!(
                                                    "unsupported remote app-server request `{method}`"
                                                ),
                                                data: None,
                                            },
                                            id: request_id,
                                        }),
                                        endpoint,
                                    )
                                    .await?;
                                }
                            }
                        }
                        JSONRPCMessage::Response(_) | JSONRPCMessage::Error(_) => {}
                    }
                }
                Some(Ok(Message::Binary(_)))
                | Some(Ok(Message::Ping(_)))
                | Some(Ok(Message::Pong(_)))
                | Some(Ok(Message::Frame(_))) => {}
                Some(Ok(Message::Close(frame))) => {
                    let reason = frame
                        .as_ref()
                        .map(|frame| frame.reason.to_string())
                        .filter(|reason| !reason.is_empty())
                        .unwrap_or_else(|| "connection closed during initialize".to_string());
                    break Err(IoError::new(
                        ErrorKind::ConnectionAborted,
                        format!(
                            "remote app server at `{endpoint}` closed during initialize: {reason}"
                        ),
                    ));
                }
                Some(Err(err)) => {
                    break Err(IoError::other(format!(
                        "remote app server at `{endpoint}` transport failed during initialize: {err}"
                    )));
                }
                None => {
                    break Err(IoError::new(
                        ErrorKind::UnexpectedEof,
                        format!("remote app server at `{endpoint}` closed during initialize"),
                    ));
                }
            }
        }
    })
    .await
    .map_err(|_| {
        IoError::new(
            ErrorKind::TimedOut,
            format!("timed out waiting for initialize response from `{endpoint}`"),
        )
    })??;

    write_jsonrpc_message(
        stream,
        JSONRPCMessage::Notification(jsonrpc_notification_from_client_notification(
            ClientNotification::Initialized,
        )),
        endpoint,
    )
    .await?;

    Ok((pending_events, metadata))
}

fn app_server_event_from_notification(notification: JSONRPCNotification) -> Option<AppServerEvent> {
    match ServerNotification::try_from(notification) {
        Ok(notification) => Some(AppServerEvent::ServerNotification(Box::new(notification))),
        Err(_) => None,
    }
}

fn deliver_event(
    event_tx: &mpsc::UnboundedSender<AppServerEvent>,
    event: AppServerEvent,
) -> IoResult<()> {
    event_tx.send(event).map_err(|_| {
        IoError::new(
            ErrorKind::BrokenPipe,
            "remote app-server event consumer channel is closed",
        )
    })
}

fn jsonrpc_request_from_client_request(request: ClientRequest) -> JSONRPCRequest {
    let value = match serde_json::to_value(request) {
        Ok(value) => value,
        Err(err) => panic!("client request should serialize: {err}"),
    };
    match serde_json::from_value(value) {
        Ok(request) => request,
        Err(err) => panic!("client request should encode as JSON-RPC request: {err}"),
    }
}

fn jsonrpc_notification_from_client_notification(
    notification: ClientNotification,
) -> JSONRPCNotification {
    let value = match serde_json::to_value(notification) {
        Ok(value) => value,
        Err(err) => panic!("client notification should serialize: {err}"),
    };
    match serde_json::from_value(value) {
        Ok(notification) => notification,
        Err(err) => panic!("client notification should encode as JSON-RPC notification: {err}"),
    }
}

async fn write_jsonrpc_message<S>(
    stream: &mut RemoteWebSocket<S>,
    message: JSONRPCMessage,
    endpoint: &str,
) -> IoResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let payload = serde_json::to_string(&message).map_err(IoError::other)?;
    stream.send_text(payload).await.map_err(|err| {
        IoError::other(format!(
            "failed to write websocket message to `{endpoint}`: {err}"
        ))
    })
}

fn websocket_close_error_is_already_closed(err: &TungsteniteError) -> bool {
    match err {
        TungsteniteError::ConnectionClosed | TungsteniteError::AlreadyClosed => true,
        TungsteniteError::Io(err) => matches!(
            err.kind(),
            ErrorKind::BrokenPipe | ErrorKind::ConnectionReset | ErrorKind::NotConnected
        ),
        _ => false,
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shutdown_tolerates_worker_exit_after_command_is_queued() {
        let (command_tx, mut command_rx) = mpsc::channel(1);
        let (event_tx, event_rx) = mpsc::unbounded_channel::<AppServerEvent>();
        let (delivered_tx, delivered_rx) = oneshot::channel();
        let worker_handle = tokio::spawn(async move {
            let _ = command_rx.recv().await;
            // Force an event after shutdown is queued but before this worker has exited.
            let result = deliver_event(
                &event_tx,
                AppServerEvent::ServerNotification(Box::new(ServerNotification::AccountUpdated(
                    codex_app_server_protocol::AccountUpdatedNotification {
                        auth_mode: None,
                        plan_type: None,
                    },
                ))),
            );
            let _ = delivered_tx.send(result);
        });
        let client = RemoteAppServerClient {
            command_tx,
            event_rx,
            pending_events: VecDeque::new(),
            metadata: RemoteServerMetadata::default(),
            worker_handle: RemoteWorker::Unhosted(Some(worker_handle)),
            terminal_error: WorkerTerminalError::default(),
            worker_custody: None,
            final_exit: FinalExitLatch::new(),
        };

        client
            .shutdown()
            .await
            .expect("shutdown should complete when worker exits first");
        delivered_rx
            .await
            .expect("worker should attempt delivery")
            .expect("an in-flight event must not abort the worker during shutdown");
    }

    use futures::SinkExt;
    use futures::StreamExt;
    const BIG_PAYLOAD_BYTES: usize = 32 << 20;
    fn initialize_params() -> InitializeParams {
        InitializeParams {
            client_info: ClientInfo {
                name: "remote-shutdown-test".to_string(),
                title: None,
                version: "0".to_string(),
            },
            capabilities: None,
        }
    }

    fn big_request() -> JSONRPCRequest {
        JSONRPCRequest {
            id: RequestId::String("big".to_string()),
            method: "thread/start".to_string(),
            params: Some(serde_json::json!({ "blob": "x".repeat(BIG_PAYLOAD_BYTES) })),
            trace: None,
        }
    }

    enum PeerExit {
        /// Drop the connection without a close handshake.
        Reset,
        /// Await the client's Close, flush the queued reply, then drop.
        CloseHandshake,
    }

    /// The production worker connected to a websocket peer over loopback TCP.
    /// The peer answers `initialize`, reads `initialized`, then reads nothing
    /// more until `release` fires; it then exits as `exit` says.
    async fn production_client_with_gated_peer(
        exit: PeerExit,
    ) -> (
        RemoteAppServerClient,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr().expect("address");
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let peer = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.expect("accept");
            let mut websocket = tokio_tungstenite::accept_async(tcp)
                .await
                .expect("websocket handshake");
            let Some(Ok(Message::Text(text))) = websocket.next().await else {
                panic!("expected the initialize request");
            };
            let Ok(JSONRPCMessage::Request(request)) =
                serde_json::from_str::<JSONRPCMessage>(&text)
            else {
                panic!("expected a JSON-RPC initialize request");
            };
            assert_eq!(request.method, "initialize");
            let response = JSONRPCMessage::Response(JSONRPCResponse {
                id: request.id,
                result: serde_json::json!({ "userAgent": "peer/1.0" }),
            });
            websocket
                .send(Message::Text(
                    serde_json::to_string(&response).expect("json").into(),
                ))
                .await
                .expect("initialize response");
            let initialized = websocket.next().await;
            assert!(
                matches!(initialized, Some(Ok(Message::Text(_)))),
                "expected the initialized notification: {initialized:?}"
            );
            release_rx.await.expect("release gate");
            match exit {
                PeerExit::Reset => drop(websocket),
                PeerExit::CloseHandshake => {
                    let frame = websocket.next().await;
                    assert!(
                        matches!(frame, Some(Ok(Message::Close(_)))),
                        "expected the client's close frame: {frame:?}"
                    );
                    // Only flushes the reply queued on receipt of the client's Close.
                    SinkExt::close(&mut websocket)
                        .await
                        .expect("flush the close reply");
                }
            }
        });
        let endpoint = format!("ws://{address}/");
        let tcp = tokio::net::TcpStream::connect(address)
            .await
            .expect("connect");
        let (websocket, response) = tokio_tungstenite::client_async(endpoint.clone(), tcp)
            .await
            .expect("client handshake");
        let client = RemoteAppServerClient::connect_with_stream(
            /*channel_capacity*/ 8,
            endpoint,
            (websocket, response),
            initialize_params(),
        )
        .await
        .expect("initialize");
        (client, release_tx, peer)
    }

    /// Starts a request too large for the socket buffers and proves the
    /// production write is blocked on the peer, which is not reading.
    async fn block_worker_in_write(
        client: &RemoteAppServerClient,
    ) -> tokio::task::JoinHandle<IoResult<RequestResult>> {
        let handle = client.request_handle();
        let big = tokio::spawn(async move { handle.request_json_rpc(big_request()).await });
        tokio::time::timeout(
            Duration::from_secs(30),
            client.terminal_error.write_pending.notified(),
        )
        .await
        .expect("the production write reached the blocked state");
        big
    }

    async fn disconnected_message(events: &mut mpsc::UnboundedReceiver<AppServerEvent>) -> String {
        let event = tokio::time::timeout(Duration::from_secs(30), events.recv())
            .await
            .expect("the worker reports its failure");
        match event {
            Some(AppServerEvent::Disconnected { message }) => message,
            other => panic!("expected a disconnect: {other:?}"),
        }
    }

    /// (a) The worker observed a reset without a closing handshake and has
    /// exited before `shutdown` is called: the close cannot be queued, and
    /// `shutdown` still reports the observed failure.
    #[tokio::test]
    async fn shutdown_reports_a_read_reset_observed_before_the_close_is_queued() {
        let (mut client, release, peer) = production_client_with_gated_peer(PeerExit::Reset).await;
        release.send(()).expect("peer is waiting");
        let observed = disconnected_message(&mut client.event_rx).await;
        assert!(observed.contains("without closing handshake"), "{observed}");
        // Positive gate: the worker closed its command queue, which it does
        // only after recording the failure and leaving its loop.
        tokio::time::timeout(Duration::from_secs(30), client.command_tx.closed())
            .await
            .expect("the worker exited");

        let error = client
            .shutdown()
            .await
            .expect_err("a reset observed before the close is not a clean close");

        assert_eq!(error.kind(), ErrorKind::InvalidData, "{error}");
        assert_eq!(error.to_string(), observed);
        peer.await.expect("peer task");
    }

    /// The same write-side reset in the exited-first order, for comparison with (b).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_reports_a_write_reset_after_the_worker_exited() {
        let (mut client, release, peer) = production_client_with_gated_peer(PeerExit::Reset).await;
        let big = block_worker_in_write(&client).await;
        // The peer drops with the blocked request unread, so the write fails.
        release.send(()).expect("peer is waiting");
        let observed = disconnected_message(&mut client.event_rx).await;
        tokio::time::timeout(Duration::from_secs(30), client.command_tx.closed())
            .await
            .expect("the worker exited");

        let error = client
            .shutdown()
            .await
            .expect_err("a reset observed before the close is not a clean close");

        assert_eq!(error.kind(), ErrorKind::BrokenPipe, "{error}");
        assert_eq!(error.to_string(), observed);
        let big = tokio::time::timeout(Duration::from_secs(30), big)
            .await
            .expect("the blocked request is answered")
            .expect("request task");
        assert!(big.is_err(), "the blocked request failed with the write");
        peer.await.expect("peer task");
    }

    /// (b) The close is queued while the worker is still blocked in a write;
    /// the peer then resets and the worker exits without running the close.
    /// `shutdown` reports the same failure as the exited-first order.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_reports_a_write_reset_that_ends_the_worker_while_the_close_is_queued() {
        let (mut client, release, peer) = production_client_with_gated_peer(PeerExit::Reset).await;
        let big = block_worker_in_write(&client).await;
        // Keep the event stream: `shutdown` consumes the client.
        let mut events = std::mem::replace(&mut client.event_rx, mpsc::unbounded_channel().1);
        let queue = client.request_handle().command_tx;
        let idle_capacity = queue.capacity();
        let shutdown = tokio::spawn(client.shutdown());
        // Positive gate: the close occupies a queue slot behind the blocked write.
        tokio::time::timeout(Duration::from_secs(30), async {
            while queue.capacity() >= idle_capacity {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the close is queued");
        release.send(()).expect("peer is waiting");

        let error = tokio::time::timeout(Duration::from_secs(30), shutdown)
            .await
            .expect("shutdown returns")
            .expect("shutdown task")
            .expect_err("a close refused by an exiting worker is not a clean close");

        let observed = disconnected_message(&mut events).await;
        assert_eq!(error.kind(), ErrorKind::BrokenPipe, "{error}");
        assert_eq!(error.to_string(), observed);
        let big = tokio::time::timeout(Duration::from_secs(30), big)
            .await
            .expect("the blocked request is answered")
            .expect("request task");
        assert!(big.is_err(), "the blocked request failed with the write");
        peer.await.expect("peer task");
    }

    /// (c) A clean close: the worker runs the close and the peer completes the
    /// handshake. Nothing was recorded, so `shutdown` succeeds.
    #[tokio::test]
    async fn shutdown_after_a_clean_close_handshake_is_ok() {
        let (client, release, peer) =
            production_client_with_gated_peer(PeerExit::CloseHandshake).await;
        release.send(()).expect("peer is waiting");

        client.shutdown().await.expect("a clean close succeeds");

        tokio::time::timeout(Duration::from_secs(30), peer)
            .await
            .expect("peer completes the close handshake")
            .expect("peer task");
    }

    fn controlled_shutdown_client(
        early_error: bool,
    ) -> (
        RemoteAppServerClient,
        oneshot::Sender<()>,
        oneshot::Receiver<()>,
        oneshot::Receiver<()>,
    ) {
        let (command_tx, mut command_rx) = mpsc::channel(1);
        let (_event_tx, event_rx) = mpsc::unbounded_channel();
        let (release_tx, release_rx) = oneshot::channel();
        let (entered_tx, entered_rx) = oneshot::channel();
        let (done_tx, done_rx) = oneshot::channel();
        let worker_handle = tokio::spawn(async move {
            if let Some(RemoteClientCommand::Shutdown { response_tx }) = command_rx.recv().await {
                let _ = entered_tx.send(());
                if early_error {
                    let _ = response_tx.send(Err(IoError::new(
                        ErrorKind::BrokenPipe,
                        "observed close failure",
                    )));
                    let _ = release_rx.await;
                } else {
                    let _ = release_rx.await;
                    let _ = response_tx.send(Ok(()));
                }
            }
            let _ = done_tx.send(());
        });
        (
            RemoteAppServerClient {
                command_tx,
                event_rx,
                pending_events: VecDeque::new(),
                metadata: RemoteServerMetadata::default(),
                worker_handle: RemoteWorker::Unhosted(Some(worker_handle)),
                terminal_error: WorkerTerminalError::default(),
                worker_custody: None,
                final_exit: FinalExitLatch::new(),
            },
            release_tx,
            entered_rx,
            done_rx,
        )
    }

    #[tokio::test(start_paused = true)]
    async fn final_close_timeout_keeps_the_original_in_its_run_host() {
        let host = Arc::new(InProcessHost::default());
        let (client, release, entered, done) = controlled_shutdown_client(false);
        let client = client.with_worker_custody(Arc::clone(&host)).unwrap();
        let deadline = Instant::now() + Duration::from_millis(100);
        let closing = tokio::spawn(client.shutdown_by(deadline));
        entered.await.unwrap();
        let close = closing.await.unwrap();
        let FinalClose::TimedOut(error) = close else {
            panic!("not a completed close: {close:?}");
        };
        assert!(error.to_string().contains("original retained by run host"));
        assert_eq!(Instant::now(), deadline);
        assert_eq!(
            host.observe_client_workers_until(Instant::now()).await,
            vec![TaskObservation::TimedOut]
        );
        release.send(()).unwrap();
        done.await.unwrap();
        assert_eq!(
            host.observe_client_workers_until(Instant::now() + Duration::from_secs(1))
                .await,
            vec![TaskObservation::Terminated(TaskTermination::Normal)]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn final_exit_stops_a_real_blocked_write_and_refuses_queued_ordinary_work() {
        let host = Arc::new(InProcessHost::default());
        let (client, release, peer) = production_client_with_gated_peer(PeerExit::Reset).await;
        let client = client.with_worker_custody(Arc::clone(&host)).unwrap();
        let big = block_worker_in_write(&client).await;
        let handle = client.request_handle();
        let idle_capacity = handle.command_tx.capacity();
        let queued_handle = handle.clone();
        let queued = tokio::spawn(async move {
            queued_handle
                .request_json_rpc(JSONRPCRequest {
                    id: RequestId::Integer(9),
                    method: "thread/loaded/list".to_owned(),
                    params: None,
                    trace: None,
                })
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while handle.command_tx.capacity() >= idle_capacity {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let deadline = Instant::now() + Duration::from_millis(100);
        client.begin_final_exit(deadline);
        assert_eq!(
            handle
                .request_json_rpc(JSONRPCRequest {
                    id: RequestId::Integer(10),
                    method: "turn/start".to_owned(),
                    params: None,
                    trace: None
                })
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::ConnectionAborted
        );
        let close = client.shutdown_by(deadline).await;
        assert!(!close.is_closed(), "abandoned write must remain incomplete");
        assert_eq!(big.await.unwrap().unwrap_err().kind(), ErrorKind::TimedOut);
        assert!(queued.await.unwrap().is_err());
        assert_eq!(
            host.observe_client_workers_until(Instant::now() + Duration::from_secs(2))
                .await,
            vec![TaskObservation::Terminated(TaskTermination::Normal)]
        );
        release.send(()).unwrap();
        peer.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn hosted_shutdown_timeout_keeps_original_observable() {
        let host = Arc::new(InProcessHost::default());
        let (client, release, entered, done) = controlled_shutdown_client(false);
        let client = client
            .with_worker_custody(Arc::clone(&host))
            .expect("bind before shutdown");
        let shutdown = tokio::spawn(client.shutdown());
        entered.await.expect("original reached shutdown");
        let error = shutdown
            .await
            .expect("observer task")
            .expect_err("original still live");
        assert_eq!(error.kind(), ErrorKind::TimedOut);
        assert_eq!(
            host.observe_client_workers_until(Instant::now())
                .await
                .expect("host observation"),
            vec![TaskObservation::TimedOut]
        );
        release.send(()).expect("original was not aborted");
        done.await.expect("original completed after release");
        assert_eq!(
            host.observe_client_workers_until(Instant::now() + Duration::from_secs(1))
                .await
                .expect("re-observe original"),
            vec![TaskObservation::Terminated(TaskTermination::Normal)]
        );
        assert!(
            host.observe_client_workers_until(Instant::now())
                .await
                .expect("normal compaction")
                .is_empty()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_shutdown_observer_cannot_take_hosted_original() {
        let host = Arc::new(InProcessHost::default());
        let (client, release, entered, done) = controlled_shutdown_client(false);
        let client = client
            .with_worker_custody(Arc::clone(&host))
            .expect("bind before shutdown");
        let shutdown = tokio::spawn(client.shutdown());
        entered.await.expect("original reached shutdown");
        // Cancel only the observer; the original worker is already held by the run host.
        shutdown.abort();
        assert!(
            shutdown
                .await
                .expect_err("observer cancelled")
                .is_cancelled()
        );
        assert_eq!(
            host.observe_client_workers_until(Instant::now())
                .await
                .expect("host observation"),
            vec![TaskObservation::TimedOut]
        );
        release
            .send(())
            .expect("original survived observer cancellation");
        done.await.expect("original completed after release");
        assert_eq!(
            host.observe_client_workers_until(Instant::now() + Duration::from_secs(1))
                .await
                .expect("original join"),
            vec![TaskObservation::Terminated(TaskTermination::Normal)]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn unhosted_timeout_is_incomplete_and_does_not_abort_original() {
        let (client, release, _entered, done) = controlled_shutdown_client(false);
        let error = client.shutdown().await.expect_err("no positive join");
        assert_eq!(error.kind(), ErrorKind::TimedOut);
        assert!(error.to_string().contains("unhosted"), "{error}");
        release
            .send(())
            .expect("compatibility original was not aborted");
        done.await.expect("original can still complete");
    }

    #[tokio::test(start_paused = true)]
    async fn reported_close_error_waits_for_original_join() {
        let host = Arc::new(InProcessHost::default());
        let (client, release, entered, done) = controlled_shutdown_client(true);
        let client = client
            .with_worker_custody(Arc::clone(&host))
            .expect("host bind");
        let mut shutdown = Box::pin(client.shutdown());
        assert!(futures::poll!(shutdown.as_mut()).is_pending());
        entered.await.expect("close command received");
        assert!(
            futures::poll!(shutdown.as_mut()).is_pending(),
            "close error alone is not a worker join"
        );
        release.send(()).expect("release original");
        let error = shutdown
            .await
            .expect_err("joined close failure remains failed");
        assert_eq!(error.to_string(), "observed close failure");
        done.await.expect("original completed");
        assert_eq!(
            host.observe_client_workers_until(Instant::now())
                .await
                .expect("original join retained"),
            vec![TaskObservation::Terminated(TaskTermination::Normal)]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn closed_host_returns_the_exact_unregistered_handle() {
        let host = InProcessHost::default();
        host.close_registration().expect("close host");
        let (release, wait) = oneshot::channel::<()>();
        let original = tokio::spawn(async move {
            let _ = wait.await;
        });
        let id = original.id();
        let Err(returned) = host.bind_client_worker(original) else {
            panic!("closed host accepted worker");
        };
        assert_eq!(returned.id(), id);
        release.send(()).expect("original still alive");
        returned.await.expect("caller can join refused original");
        assert!(
            host.observe_client_workers_until(Instant::now())
                .await
                .expect("no registered clients")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn failed_client_worker_stays_visible_in_host() {
        let host = InProcessHost::default();
        let original = tokio::spawn(async {
            panic!("client worker panic fixture");
        });
        let Ok(owner) = host.bind_client_worker(original) else {
            panic!("open host refused worker");
        };
        assert_eq!(
            owner
                .observe_until(Instant::now() + Duration::from_secs(1))
                .await,
            TaskObservation::Terminated(TaskTermination::Panicked)
        );
        for _ in 0..2 {
            assert_eq!(
                host.observe_client_workers_until(Instant::now())
                    .await
                    .expect("failed receipt"),
                vec![TaskObservation::Terminated(TaskTermination::Panicked)]
            );
        }
    }
}

#[cfg(test)]
mod final_exit_tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn latch_keeps_the_earliest_deadline_and_exact_teardown_boundary() {
        let latch = FinalExitLatch::new();
        assert!(latch.admit("turn/start").is_ok());
        let deadline = Instant::now() + Duration::from_secs(2);
        assert_eq!(latch.begin(deadline), deadline);
        assert_eq!(latch.begin(deadline + Duration::from_secs(5)), deadline);
        for method in OWNED_TEARDOWN_METHODS {
            assert!(latch.admit(method).is_ok());
        }
        for method in [
            "turn/start",
            SERVER_REQUEST_RESPONSE,
            "kcf/mcpServer/hostedEndpoint/retireAll",
            "thread/retention/release/extra",
        ] {
            assert_eq!(
                latch.admit(method).unwrap_err().kind(),
                ErrorKind::ConnectionAborted
            );
        }
        let earlier = Instant::now() + Duration::from_millis(50);
        assert_eq!(latch.begin(earlier), earlier);
        tokio::time::advance(Duration::from_millis(50)).await;
        for method in OWNED_TEARDOWN_METHODS {
            assert_eq!(latch.admit(method).unwrap_err().kind(), ErrorKind::TimedOut);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn expired_final_close_does_not_enqueue_a_shutdown() {
        let host = Arc::new(InProcessHost::default());
        let (command_tx, mut command_rx) = mpsc::channel(1);
        let (_event_tx, event_rx) = mpsc::unbounded_channel();
        let (observed_tx, observed_rx) = oneshot::channel();
        let worker = tokio::spawn(async move {
            let _ = observed_tx.send(command_rx.recv().await.is_some());
        });
        let client = RemoteAppServerClient {
            command_tx,
            event_rx,
            pending_events: VecDeque::new(),
            metadata: RemoteServerMetadata::default(),
            worker_handle: RemoteWorker::Unhosted(Some(worker)),
            terminal_error: WorkerTerminalError::default(),
            worker_custody: None,
            final_exit: FinalExitLatch::new(),
        }
        .with_worker_custody(Arc::clone(&host))
        .unwrap();
        let close = client.shutdown_by(Instant::now()).await;
        assert!(matches!(close, FinalClose::TimedOut(_)));
        assert!(
            !observed_rx.await.unwrap(),
            "no Shutdown reached the worker after expiry"
        );
        assert_eq!(
            host.observe_client_workers_until(Instant::now() + Duration::from_secs(1))
                .await,
            vec![TaskObservation::Terminated(TaskTermination::Normal)]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn shared_request_handle_refuses_after_expiry_without_enqueueing() {
        let (tx, mut rx) = mpsc::channel(4);
        let latch = FinalExitLatch::new();
        let handle = RemoteAppServerRequestHandle {
            command_tx: tx,
            final_exit: latch.clone(),
        };
        let clone = handle.clone();
        latch.begin(Instant::now());
        let request = JSONRPCRequest {
            id: RequestId::Integer(1),
            method: OWNED_TEARDOWN_METHODS[0].to_owned(),
            params: None,
            trace: None,
        };
        assert_eq!(
            clone.request_json_rpc(request).await.unwrap_err().kind(),
            ErrorKind::TimedOut
        );
        assert_eq!(
            clone
                .resolve_server_request(RequestId::Integer(2), serde_json::json!({}))
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::TimedOut
        );
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn waiting_deadline_observes_tightening_without_extending() {
        let latch = FinalExitLatch::new();
        let first = Instant::now() + Duration::from_secs(10);
        latch.begin(first);
        let mut receiver = latch.deadline.subscribe();
        let waiter = tokio::spawn(async move {
            final_exit_deadline_reached(&mut receiver).await;
        });
        tokio::task::yield_now().await;
        let earlier = Instant::now() + Duration::from_secs(1);
        latch.begin(earlier);
        latch.begin(first);
        waiter.await.unwrap();
        assert_eq!(Instant::now(), earlier);
    }
}
