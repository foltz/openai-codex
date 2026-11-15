use std::{
    borrow::Cow,
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use futures::{
    Stream, StreamExt,
    future::BoxFuture,
    stream::{BoxStream, FuturesUnordered},
};
use http::{HeaderName, HeaderValue, StatusCode};
pub use sse_stream::Error as SseError;
use sse_stream::Sse;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use super::common::client_side_sse::{
    DEFAULT_MAX_SSE_EVENT_SIZE, ExponentialBackoff, SseRetryPolicy, SseStreamReconnect,
};
use crate::{
    RoleClient,
    model::{
        ClientJsonRpcMessage, ClientNotification, ClientRequest, ErrorData, GetExtensions, GetMeta,
        InitializedNotification, JsonObject, ProtocolVersion, RequestId, ServerJsonRpcMessage,
        ServerResult,
    },
    service::InboundStreamOrigin,
    transport::{
        common::{client_side_sse::SseAutoReconnectStream, mcp_headers},
        worker::{
            CloseDeadline, RequestCancellationRegistration, Worker, WorkerQuitReason,
            WorkerSendRequest, WorkerTransport,
        },
    },
};

type BoxedSseStream = BoxStream<'static, Result<Sse, SseError>>;
type SseTaskResult<E> = (Option<RequestId>, Result<(), StreamableHttpError<E>>);
const SESSION_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);

fn build_request_headers(
    base: &HashMap<HeaderName, HeaderValue>,
    message: &ClientJsonRpcMessage,
    tool_cache: &HashMap<String, Arc<JsonObject>>,
    version: &ProtocolVersion,
) -> HashMap<HeaderName, HeaderValue> {
    use serde_json::Value;

    let mut headers = base.clone();
    if *version >= ProtocolVersion::STANDARD_HEADERS
        && let Ok(value) = serde_json::to_value(message)
    {
        let schema = value
            .get("method")
            .and_then(Value::as_str)
            .filter(|method| *method == "tools/call")
            .and_then(|_| value.get("params"))
            .and_then(|params| params.get("name"))
            .and_then(Value::as_str)
            .and_then(|name| tool_cache.get(name))
            .map(Arc::as_ref);
        for (name, val) in mcp_headers::standard_request_headers(&value, schema) {
            headers.insert(name, val);
        }
    }
    headers
}

fn request_version_headers(
    base: &HashMap<HeaderName, HeaderValue>,
    message: &ClientJsonRpcMessage,
    fallback: &ProtocolVersion,
    tool_cache: &HashMap<String, Arc<JsonObject>>,
) -> (ProtocolVersion, HashMap<HeaderName, HeaderValue>) {
    let version = match message {
        ClientJsonRpcMessage::Request(request) => request
            .request
            .get_meta()
            .protocol_version()
            .unwrap_or_else(|| fallback.clone()),
        _ => fallback.clone(),
    };
    let mut headers = build_request_headers(base, message, tool_cache, &version);
    if let Ok(value) = HeaderValue::from_str(version.as_str()) {
        headers.insert(HeaderName::from_static("mcp-protocol-version"), value);
    }
    (version, headers)
}

fn cache_tools_from_response(
    cache: &mut HashMap<String, Arc<JsonObject>>,
    message: &mut ServerJsonRpcMessage,
    protocol_version: &ProtocolVersion,
) {
    if protocol_version < &ProtocolVersion::STANDARD_HEADERS {
        return;
    }
    if let ServerJsonRpcMessage::Response(response) = message
        && let ServerResult::ListToolsResult(list) = &mut response.result
    {
        list.tools.retain(|tool| {
                let Err(reason) =
                    mcp_headers::validate_param_header_annotations(&tool.input_schema)
                else {
                    cache.insert(tool.name.to_string(), tool.input_schema.clone());
                    return true;
                };
                tracing::warn!(tool = %tool.name, "rejecting invalid x-mcp-header annotations: {reason}");
                false
            });
    }
}

fn negotiate_version_headers(
    init_response: &ServerJsonRpcMessage,
    base: HashMap<HeaderName, HeaderValue>,
) -> (ProtocolVersion, HashMap<HeaderName, HeaderValue>) {
    let mut version = ProtocolVersion::default();
    let mut headers = base;
    if let ServerJsonRpcMessage::Response(response) = init_response
        && let ServerResult::InitializeResult(init_result) = &response.result
    {
        version = init_result.protocol_version.clone();
        // HeaderName::from_static requires lowercase
        if let Ok(hv) = HeaderValue::from_str(init_result.protocol_version.as_str()) {
            headers.insert(HeaderName::from_static("mcp-protocol-version"), hv);
        }
    }
    (version, headers)
}

#[derive(Debug, Error)]
#[error("authorization required: {www_authenticate_header}")]
#[non_exhaustive]
pub struct AuthRequiredError {
    pub www_authenticate_header: String,
}

impl AuthRequiredError {
    /// Create a new `AuthRequiredError` instance.
    pub fn new(www_authenticate_header: String) -> Self {
        Self {
            www_authenticate_header,
        }
    }
}

#[derive(Debug, Error)]
#[error("insufficient scope: {www_authenticate_header}")]
#[non_exhaustive]
pub struct InsufficientScopeError {
    pub www_authenticate_header: String,
    pub required_scope: Option<String>,
}

impl InsufficientScopeError {
    /// Create a new `InsufficientScopeError` instance.
    pub fn new(www_authenticate_header: String, required_scope: Option<String>) -> Self {
        Self {
            www_authenticate_header,
            required_scope,
        }
    }

    /// check if scope upgrade is possible (i.e., we know what scope is required)
    pub fn can_upgrade(&self) -> bool {
        self.required_scope.is_some()
    }

    /// get the required scope for upgrade
    pub fn get_required_scope(&self) -> Option<&str> {
        self.required_scope.as_deref()
    }
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum StreamableHttpError<E: std::error::Error + Send + Sync + 'static> {
    #[error("SSE error: {0}")]
    Sse(#[from] SseError),
    #[error("Io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Client error: {0}")]
    Client(E),
    #[error("unexpected end of stream")]
    UnexpectedEndOfStream,
    #[error("unexpected server response: {0}")]
    UnexpectedServerResponse(Cow<'static, str>),
    #[error("Unexpected content type: {0:?}")]
    UnexpectedContentType(Option<String>),
    #[error("Server does not support SSE")]
    ServerDoesNotSupportSse,
    #[error("Server does not support delete session")]
    ServerDoesNotSupportDeleteSession,
    #[error("Tokio join error: {0}")]
    TokioJoinError(#[from] tokio::task::JoinError),
    #[error("Deserialize error: {0}")]
    Deserialize(#[from] serde_json::Error),
    #[error("Transport channel closed")]
    TransportChannelClosed,
    #[error("Missing session id in HTTP response")]
    MissingSessionIdInResponse,
    #[cfg(feature = "auth")]
    #[error("Auth error: {0}")]
    Auth(#[from] crate::transport::auth::AuthError),
    #[error("Auth required")]
    AuthRequired(#[source] AuthRequiredError),
    #[error("Insufficient scope")]
    InsufficientScope(#[source] InsufficientScopeError),
    #[error("Header name '{0}' is reserved and conflicts with default headers")]
    ReservedHeaderConflict(String),
    #[error("Session expired (HTTP 404)")]
    SessionExpired,
    /// Session recovery timed out. The server may have processed an interrupted POST.
    #[error("Session recovery timed out; the server may have processed the POST")]
    SessionRecoveryTimeout,
    /// The server answered the session re-initialization with a JSON-RPC error.
    #[error("Session re-initialization failed: {0}")]
    ReinitializationFailed(ErrorData),
    /// A cancellation or reply POST did not finish in time.
    #[error("Control POST timed out")]
    ControlRequestTimeout,
    /// The worker stopped without proving that its session was deleted and
    /// its stream tasks stopped. [`WorkerTransport::close`] returns this.
    #[error("transport cleanup incomplete: {reason}")]
    CleanupIncomplete {
        session_id: Option<Arc<str>>,
        reason: Cow<'static, str>,
    },
}

/// Bytes of an unusable JSON body kept in
/// [`StreamableHttpError::UnexpectedServerResponse`].
pub(crate) const JSON_RPC_BODY_PREVIEW_LEN: usize = 256;

/// Short, lossy view of a body that failed to parse as JSON-RPC.
pub(crate) fn json_rpc_body_preview(body: &[u8]) -> Cow<'static, str> {
    if body.is_empty() {
        return Cow::Borrowed("<empty>");
    }
    let lossy = String::from_utf8_lossy(body);
    if lossy.len() <= JSON_RPC_BODY_PREVIEW_LEN {
        return Cow::Owned(lossy.into_owned());
    }
    let mut end = JSON_RPC_BODY_PREVIEW_LEN;
    while end > 0 && !lossy.is_char_boundary(end) {
        end -= 1;
    }
    Cow::Owned(lossy[..end].to_owned())
}

/// Request-POST parse failure, including a body preview so empty / HTML / `{}`
/// are distinguishable from the serde diagnostic alone.
pub(crate) fn json_rpc_parse_error_message(err: &serde_json::Error, body: &[u8]) -> String {
    format!(
        "could not parse JSON response as ServerJsonRpcMessage: {err}: {}",
        json_rpc_body_preview(body)
    )
}

impl<E: std::error::Error + Send + Sync + 'static> StreamableHttpError<E> {
    /// The `WWW-Authenticate` challenge carried by this error, when the
    /// server answered 401 ([`AuthRequired`](Self::AuthRequired)) or 403
    /// ([`InsufficientScope`](Self::InsufficientScope)). Feed it to
    /// [`AuthorizationRequest::with_challenge`](crate::transport::auth::AuthorizationRequest::with_challenge)
    /// to authorize reactively.
    #[cfg(feature = "auth")]
    pub fn auth_challenge(&self) -> Option<&str> {
        match self {
            Self::AuthRequired(error) => Some(&error.www_authenticate_header),
            Self::InsufficientScope(error) => Some(&error.www_authenticate_header),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Error)]
#[non_exhaustive]
pub enum StreamableHttpProtocolError {
    #[error("Missing session id in response")]
    MissingSessionIdInResponse,
}

#[expect(
    clippy::large_enum_variant,
    reason = "boxing the streaming response would add an allocation to the common response path"
)]
#[non_exhaustive]
pub enum StreamableHttpPostResponse {
    Accepted,
    Json(ServerJsonRpcMessage, Option<String>),
    Sse(BoxedSseStream, Option<String>),
}

impl std::fmt::Debug for StreamableHttpPostResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Accepted => write!(f, "Accepted"),
            Self::Json(arg0, arg1) => f.debug_tuple("Json").field(arg0).field(arg1).finish(),
            Self::Sse(_, arg1) => f.debug_tuple("Sse").field(arg1).finish(),
        }
    }
}

/// The earlier of the local cleanup cap and the owner's latched deadline.
fn effective_close_deadline(
    cap: tokio::time::Instant,
    latched: Option<tokio::time::Instant>,
) -> tokio::time::Instant {
    latched.map_or(cap, |latched| latched.min(cap))
}

/// Polls `step` until it finishes, or returns `None` at the effective close deadline.
///
/// Expiry is checked first on every wake-up, before `step` is polled, and the
/// owner's latched deadline is re-read whenever it tightens. The watch lock is
/// held only to copy the deadline, never while `step` runs.
async fn until_close_deadline<F: Future>(
    cap: tokio::time::Instant,
    deadline_changes: &mut tokio::sync::watch::Receiver<Option<tokio::time::Instant>>,
    step: F,
) -> Option<F::Output> {
    let mut step = std::pin::pin!(step);
    let mut following = true;
    loop {
        let latched = *deadline_changes.borrow_and_update();
        let deadline = effective_close_deadline(cap, latched);
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => return None,
            changed = deadline_changes.changed(), if following => {
                // The handle owns the sender, so this only ends with the owner;
                // keep the last deadline then.
                following = changed.is_ok();
            }
            output = &mut step => return Some(output),
        }
    }
}

/// The session a server assigned in a POST response's headers, before its body is read.
fn post_response_session_id(response: &StreamableHttpPostResponse) -> Option<Arc<str>> {
    match response {
        StreamableHttpPostResponse::Json(_, session_id)
        | StreamableHttpPostResponse::Sse(_, session_id) => session_id.as_deref().map(Arc::from),
        StreamableHttpPostResponse::Accepted => None,
    }
}

impl StreamableHttpPostResponse {
    pub async fn expect_initialized<E>(
        self,
    ) -> Result<(ServerJsonRpcMessage, Option<String>), StreamableHttpError<E>>
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        match self {
            Self::Json(message, session_id) => Ok((message, session_id)),
            Self::Sse(mut stream, session_id) => {
                while let Some(event) = stream.next().await {
                    let event = event?;
                    let payload = event.data.unwrap_or_default();
                    if payload.trim().is_empty() {
                        continue;
                    }

                    let message: ServerJsonRpcMessage = serde_json::from_str(&payload)?;

                    if matches!(
                        message,
                        ServerJsonRpcMessage::Response(_) | ServerJsonRpcMessage::Error(_)
                    ) {
                        return Ok((message, session_id));
                    }

                    debug!(
                        ?message,
                        "received message before initialize response; continuing to drain stream"
                    );
                }

                Err(StreamableHttpError::UnexpectedServerResponse(
                    "empty sse stream".into(),
                ))
            }
            _ => Err(StreamableHttpError::UnexpectedServerResponse(
                "expect initialized, accepted".into(),
            )),
        }
    }

    pub fn expect_json<E>(self) -> Result<ServerJsonRpcMessage, StreamableHttpError<E>>
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        match self {
            Self::Json(message, ..) => Ok(message),
            got => Err(StreamableHttpError::UnexpectedServerResponse(
                format!("expect json, got {got:?}").into(),
            )),
        }
    }

    pub fn expect_accepted_or_json<E>(self) -> Result<(), StreamableHttpError<E>>
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        match self {
            Self::Accepted => Ok(()),
            // Tolerate servers that return 200 with JSON for notifications
            Self::Json(..) => Ok(()),
            got => Err(StreamableHttpError::UnexpectedServerResponse(
                format!("expect accepted or json, got {got:?}").into(),
            )),
        }
    }
}

/// Convert a sessionless discovery rejection into a response the lifecycle
/// layer can classify. The server's own JSON-RPC error is preserved so the
/// lifecycle can tell a modern rejection (retry at a supported version) from a
/// legacy one; `invalid_request` is synthesized only when the body has no error
/// to keep, as with an empty or plain-text 4xx from middleware.
///
/// The id is re-correlated because such middleware rejections cannot echo it
/// (the Python SDK sends the literal `"server-error"`); otherwise the lifecycle
/// discards the response as uncorrelated without classifying it.
///
/// Keep authentication failures and server errors on their original paths.
pub(super) fn legacy_discover_response(
    message: &ClientJsonRpcMessage,
    session_was_attached: bool,
    status: StatusCode,
    body: &str,
) -> Option<StreamableHttpPostResponse> {
    if session_was_attached
        || !status.is_client_error()
        || matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN)
    {
        return None;
    }

    let ClientJsonRpcMessage::Request(request) = message else {
        return None;
    };
    if !matches!(request.request, ClientRequest::DiscoverRequest(_)) {
        return None;
    }

    let error = match serde_json::from_str::<ServerJsonRpcMessage>(body) {
        Ok(ServerJsonRpcMessage::Error(error)) => error.error,
        _ => ErrorData::invalid_request(
            format!("server/discover rejected with HTTP {status}: {body}"),
            None,
        ),
    };
    Some(StreamableHttpPostResponse::Json(
        ServerJsonRpcMessage::error(error, Some(request.id.clone())),
        None,
    ))
}

/// HTTP backend used by [`StreamableHttpClientTransport`].
///
/// Custom implementations that parse SSE responses must override
/// [`Self::post_message_with_max_sse_event_size`] and
/// [`Self::get_stream_with_max_sse_event_size`] to enforce the transport's
/// configured event-size limit.
///
/// For legacy http, the transport keeps an open response stream alive until
/// its cancellation send finishes or is dropped. This lets a custom client
/// handle the cancellation using state owned by that stream.
pub trait StreamableHttpClient: Clone + Send + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    fn post_message(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> impl Future<Output = Result<StreamableHttpPostResponse, StreamableHttpError<Self::Error>>>
    + Send
    + '_;
    /// Send a message while enforcing a maximum raw SSE event size.
    ///
    /// `max_sse_event_size` is not a per-request option: it is the
    /// transport-wide [`StreamableHttpClientTransportConfig::max_sse_event_size`]
    /// value, passed identically on every call because the limit must be applied
    /// inside the client (at the raw byte layer, before SSE parsing) rather than
    /// by the caller.
    ///
    /// Custom clients that parse SSE responses should override this method.
    /// The default implementation delegates to [`Self::post_message`].
    fn post_message_with_max_sse_event_size(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
        _max_sse_event_size: usize,
    ) -> impl Future<Output = Result<StreamableHttpPostResponse, StreamableHttpError<Self::Error>>>
    + Send
    + '_ {
        self.post_message(uri, message, session_id, auth_header, custom_headers)
    }
    fn delete_session(
        &self,
        uri: Arc<str>,
        session_id: Arc<str>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> impl Future<Output = Result<(), StreamableHttpError<Self::Error>>> + Send + '_;
    /// Open an SSE stream, optionally scoped to a legacy session.
    ///
    /// `session_id` is `None` when resuming a stateless response using only
    /// `last_event_id`.
    fn get_stream(
        &self,
        uri: Arc<str>,
        session_id: Option<Arc<str>>,
        last_event_id: Option<String>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> impl Future<
        Output = Result<
            BoxStream<'static, Result<Sse, SseError>>,
            StreamableHttpError<Self::Error>,
        >,
    > + Send
    + '_;
    /// Open an SSE stream while enforcing a maximum raw event size.
    ///
    /// `max_sse_event_size` is not a per-request option: it is the
    /// transport-wide [`StreamableHttpClientTransportConfig::max_sse_event_size`]
    /// value, passed identically on every call because the limit must be applied
    /// inside the client (at the raw byte layer, before SSE parsing) rather than
    /// by the caller.
    ///
    /// Custom clients that parse SSE responses should override this method.
    /// The default implementation delegates to [`Self::get_stream`].
    fn get_stream_with_max_sse_event_size(
        &self,
        uri: Arc<str>,
        session_id: Option<Arc<str>>,
        last_event_id: Option<String>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
        _max_sse_event_size: usize,
    ) -> impl Future<
        Output = Result<
            BoxStream<'static, Result<Sse, SseError>>,
            StreamableHttpError<Self::Error>,
        >,
    > + Send
    + '_ {
        self.get_stream(uri, session_id, last_event_id, auth_header, custom_headers)
    }
}

#[non_exhaustive]
pub struct RetryConfig {
    pub max_times: Option<usize>,
    pub min_duration: Duration,
}

struct StreamableHttpClientReconnect<C> {
    pub client: C,
    pub session_id: Option<Arc<str>>,
    pub uri: Arc<str>,
    pub auth_header: Option<String>,
    pub custom_headers: HashMap<HeaderName, HeaderValue>,
    pub max_sse_event_size: usize,
}

impl<C: StreamableHttpClient> SseStreamReconnect for StreamableHttpClientReconnect<C> {
    type Error = StreamableHttpError<C::Error>;
    type Future = BoxFuture<'static, Result<BoxedSseStream, Self::Error>>;
    fn retry_connection(&mut self, last_event_id: Option<&str>) -> Self::Future {
        let client = self.client.clone();
        let uri = self.uri.clone();
        let session_id = self.session_id.clone();
        let auth_header = self.auth_header.clone();
        let custom_headers = self.custom_headers.clone();
        let max_sse_event_size = self.max_sse_event_size;
        let last_event_id = last_event_id.map(|s| s.to_owned());
        Box::pin(async move {
            client
                .get_stream_with_max_sse_event_size(
                    uri,
                    session_id,
                    last_event_id,
                    auth_header,
                    custom_headers,
                    max_sse_event_size,
                )
                .await
        })
    }

    fn map_fatal_stream_error(&mut self, error: SseError) -> Option<Self::Error> {
        Some(StreamableHttpError::Sse(error))
    }
}

/// Info retained for cleaning up the session when the worker exits.
struct SessionCleanupInfo<C> {
    client: C,
    uri: Arc<str>,
    session_id: Arc<str>,
    auth_header: Option<String>,
    protocol_headers: HashMap<HeaderName, HeaderValue>,
    /// The server answered `initialize` for exactly this session id, so a 404
    /// to its DELETE proves the session is already gone.
    established: bool,
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct StreamableHttpClientWorker<C: StreamableHttpClient> {
    pub client: C,
    pub config: StreamableHttpClientTransportConfig,
}

struct PostResult<C: StreamableHttpClient> {
    send_request: WorkerSendRequest<StreamableHttpClientWorker<C>>,
    // None means the send future was dropped, or the request or transport was cancelled.
    response: Option<Result<StreamableHttpPostResponse, StreamableHttpError<C::Error>>>,
    // The protocol version used to send this POST.
    version: ProtocolVersion,
}

struct PostSession {
    id: Option<Arc<str>>,
    headers: HashMap<HeaderName, HeaderValue>,
    version: ProtocolVersion,
    cancellation: CancellationToken,
}

impl<C: StreamableHttpClient + Default> StreamableHttpClientWorker<C> {
    pub fn new_simple(url: impl Into<Arc<str>>) -> Self {
        Self {
            client: C::default(),
            config: StreamableHttpClientTransportConfig {
                uri: url.into(),
                ..Default::default()
            },
        }
    }
}

impl<C: StreamableHttpClient> StreamableHttpClientWorker<C> {
    pub fn new(client: C, config: StreamableHttpClientTransportConfig) -> Self {
        Self { client, config }
    }
}

impl<C: StreamableHttpClient> StreamableHttpClientWorker<C> {
    // Run initialization and protocol-version changes without other ordinary POSTs.
    fn is_ordering_barrier(
        message: &ClientJsonRpcMessage,
        negotiated_version: &ProtocolVersion,
    ) -> bool {
        match message {
            ClientJsonRpcMessage::Request(request) => {
                matches!(
                    &request.request,
                    ClientRequest::InitializeRequest(_) | ClientRequest::DiscoverRequest(_)
                ) || request
                    .request
                    .get_meta()
                    .protocol_version()
                    .is_some_and(|version| &version != negotiated_version)
            }
            ClientJsonRpcMessage::Notification(notification) => matches!(
                &notification.notification,
                ClientNotification::InitializedNotification(_)
            ),
            _ => false,
        }
    }

    fn post_request(
        client: C,
        config: &StreamableHttpClientTransportConfig,
        mut send_request: WorkerSendRequest<Self>,
        session: PostSession,
        transport_cancellation: CancellationToken,
    ) -> BoxFuture<'static, PostResult<C>> {
        let uri = config.uri.clone();
        let auth_header = config.auth_header.clone();
        let max_sse_event_size = config.max_sse_event_size;
        let control_request_timeout = config.control_request_timeout;
        let is_control = Self::is_control_message(&send_request.message);
        let cancellation = send_request
            .cancellation_token()
            .unwrap_or_else(|| transport_cancellation.child_token());
        Box::pin(async move {
            let response = tokio::select! {
                biased;
                _ = cancellation.cancelled() => None,
                _ = send_request.responder.closed() => None,
                _ = session.cancellation.cancelled() => {
                    Some(Err(StreamableHttpError::SessionRecoveryTimeout))
                },
                _ = tokio::time::sleep(control_request_timeout), if is_control => {
                    Some(Err(StreamableHttpError::ControlRequestTimeout))
                },
                response = client.post_message_with_max_sse_event_size(
                    uri,
                    send_request.message.clone(),
                    session.id,
                    auth_header,
                    session.headers,
                    max_sse_event_size,
                ) => Some(response),
            };
            PostResult {
                send_request,
                response,
                version: session.version,
            }
        })
    }

    fn cancellation_request_id(message: &ClientJsonRpcMessage) -> Option<&RequestId> {
        match message {
            ClientJsonRpcMessage::Notification(notification) => match &notification.notification {
                ClientNotification::CancelledNotification(cancelled) => {
                    cancelled.params.request_id.as_ref()
                }
                _ => None,
            },
            _ => None,
        }
    }

    fn client_request_id(message: &ClientJsonRpcMessage) -> Option<RequestId> {
        match message {
            ClientJsonRpcMessage::Request(request) => Some(request.id.clone()),
            _ => None,
        }
    }

    fn server_response_id(message: &ServerJsonRpcMessage) -> Option<&RequestId> {
        match message {
            ServerJsonRpcMessage::Response(response) => Some(&response.id),
            ServerJsonRpcMessage::Error(error) => error.id.as_ref(),
            _ => None,
        }
    }

    fn clear_stream_response_pending(
        pending_stream_response_ids: &mut HashSet<RequestId>,
        message: &ServerJsonRpcMessage,
    ) -> Option<RequestId> {
        let response_id = Self::server_response_id(message)?;
        if let Some(id) = pending_stream_response_ids.take(response_id) {
            return Some(id);
        }
        let id = RequestId::Number(response_id.numeric_string_value()?);
        pending_stream_response_ids.take(&id)
    }

    async fn drain_queued_stream_messages(
        sse_worker_rx: &mut tokio::sync::mpsc::Receiver<ServerJsonRpcMessage>,
        context: &mut super::worker::WorkerContext<Self>,
        pending_stream_response_ids: &mut HashSet<RequestId>,
    ) -> Result<(), WorkerQuitReason<StreamableHttpError<C::Error>>> {
        loop {
            match sse_worker_rx.try_recv() {
                Ok(message) => {
                    let _ =
                        Self::clear_stream_response_pending(pending_stream_response_ids, &message);
                    context.send_to_handler(message).await?;
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => return Ok(()),
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => return Ok(()),
            }
        }
    }

    async fn fail_pending_stream_responses(
        context: &mut super::worker::WorkerContext<Self>,
        pending_stream_response_ids: &mut HashSet<RequestId>,
    ) -> Result<(), WorkerQuitReason<StreamableHttpError<C::Error>>> {
        if pending_stream_response_ids.is_empty() {
            return Ok(());
        }

        let pending_ids = std::mem::take(pending_stream_response_ids);
        for id in pending_ids {
            context
                .send_to_handler(ServerJsonRpcMessage::error(
                    ErrorData::internal_error(
                        "streamable HTTP session was re-initialized before the response arrived",
                        None,
                    ),
                    Some(id),
                ))
                .await?;
        }
        Ok(())
    }

    async fn fail_pending_responses_except_retries(
        context: &mut super::worker::WorkerContext<Self>,
        pending_stream_response_ids: &mut HashSet<RequestId>,
        recovery_posts: &VecDeque<WorkerSendRequest<Self>>,
    ) -> Result<(), WorkerQuitReason<StreamableHttpError<C::Error>>> {
        // Keep only retries that have not already received a stream response.
        let retry_ids: Vec<_> = recovery_posts
            .iter()
            .filter_map(|request| Self::client_request_id(&request.message))
            .filter(|id| pending_stream_response_ids.remove(id))
            .collect();
        Self::fail_pending_stream_responses(context, pending_stream_response_ids).await?;
        pending_stream_response_ids.extend(retry_ids);
        Ok(())
    }

    fn fail_recovery_posts(
        recovery_posts: &mut VecDeque<WorkerSendRequest<Self>>,
        pending_stream_response_ids: &mut HashSet<RequestId>,
        error: StreamableHttpError<C::Error>,
    ) {
        // The backend error cannot be cloned. Return it to one caller
        // and return the original session-expired error to the others.
        let mut recovery_error = Some(error);
        for send_request in recovery_posts.drain(..) {
            let pending = Self::client_request_id(&send_request.message)
                .is_none_or(|id| pending_stream_response_ids.remove(&id));
            let result = if pending {
                Err(recovery_error
                    .take()
                    .unwrap_or(StreamableHttpError::SessionExpired))
            } else {
                Ok(())
            };
            let _ = send_request.responder.send(result);
        }
    }

    /// Convert an SSE stream into JSON-RPC messages with reconnect semantics.
    ///
    /// This is used for request-scoped SSE responses as well as the standalone
    /// GET stream. A request-scoped stream can close before its response arrives,
    /// and SEP-1699 requires the client to honor `retry` and resume with
    /// `Last-Event-ID` in that case.
    fn reconnecting_sse_to_jsonrpc(
        stream: BoxedSseStream,
        client: C,
        session_id: Option<Arc<str>>,
        uri: Arc<str>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
        max_sse_event_size: usize,
        retry_config: Arc<dyn SseRetryPolicy>,
    ) -> impl Stream<Item = Result<ServerJsonRpcMessage, StreamableHttpError<C::Error>>> + Send + 'static
    {
        SseAutoReconnectStream::new_after_event_id(
            stream,
            StreamableHttpClientReconnect {
                client,
                session_id,
                uri,
                auth_header,
                custom_headers,
                max_sse_event_size,
            },
            retry_config,
        )
    }

    /// Convert a POST response SSE stream into JSON-RPC messages.
    ///
    /// Request-scoped streams resume via GET once the server has supplied an
    /// event ID. The session header remains optional for stateless transports.
    fn response_sse_to_jsonrpc(
        stream: BoxedSseStream,
        session_id: Option<Arc<str>>,
        client: C,
        uri: Arc<str>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
        max_sse_event_size: usize,
        retry_config: Arc<dyn SseRetryPolicy>,
    ) -> BoxStream<'static, Result<ServerJsonRpcMessage, StreamableHttpError<C::Error>>> {
        Self::reconnecting_sse_to_jsonrpc(
            stream,
            client,
            session_id,
            uri,
            auth_header,
            custom_headers,
            max_sse_event_size,
            retry_config,
        )
        .boxed()
    }

    async fn run_response_stream(
        mut sse_stream: BoxStream<
            'static,
            Result<ServerJsonRpcMessage, StreamableHttpError<C::Error>>,
        >,
        sse_worker_tx: tokio::sync::mpsc::Sender<ServerJsonRpcMessage>,
        origin: InboundStreamOrigin,
        request_ct: CancellationToken,
        stream_ct: CancellationToken,
        uses_modern_http: bool,
    ) -> Result<(), StreamableHttpError<C::Error>> {
        tokio::select! {
            biased;
            _ = request_ct.cancelled(), if !uses_modern_http => {
                // Stop reading, but keep the stream until the adapter
                // handles cancellation or the send is dropped.
                stream_ct.cancelled().await;
                Ok(())
            }
            result = Self::execute_sse_stream(
                sse_stream.as_mut(), sse_worker_tx, origin, true, stream_ct.clone(),
            ) => result,
        }
    }

    async fn execute_sse_stream(
        sse_stream: impl Stream<Item = Result<ServerJsonRpcMessage, StreamableHttpError<C::Error>>>
        + Send,
        sse_worker_tx: tokio::sync::mpsc::Sender<ServerJsonRpcMessage>,
        origin: InboundStreamOrigin,
        close_on_response: bool,
        ct: CancellationToken,
    ) -> Result<(), StreamableHttpError<C::Error>> {
        let mut sse_stream = std::pin::pin!(sse_stream);
        loop {
            let message = tokio::select! {
                event = sse_stream.next() => {
                    event
                }
                _ = ct.cancelled() => {
                    tracing::debug!("cancelled");
                    break;
                }
            };
            let Some(mut message) = message.transpose()? else {
                break;
            };
            // SEP-2260: mark inbound requests with the stream they arrived on
            // for the client receive-side association check.
            if let ServerJsonRpcMessage::Request(request) = &mut message {
                request.request.extensions_mut().insert(origin.clone());
            }
            let is_response = matches!(
                message,
                ServerJsonRpcMessage::Response(_) | ServerJsonRpcMessage::Error(_)
            );
            let yield_result = sse_worker_tx.send(message).await;
            if yield_result.is_err() {
                tracing::trace!("streamable http transport worker dropped, exiting");
                break;
            }
            if close_on_response && is_response {
                tracing::debug!("got response, draining sse stream for connection reuse");
                // Consume the remaining stream so the HTTP/1.1 connection
                // returns to the pool cleanly.
                let _ = tokio::time::timeout(std::time::Duration::from_millis(50), async {
                    while sse_stream.next().await.is_some() {}
                })
                .await;
                break;
            }
        }
        Ok(())
    }

    fn spawn_common_stream(
        streams: &mut tokio::task::JoinSet<SseTaskResult<C::Error>>,
        client: C,
        session_id: Arc<str>,
        config: &StreamableHttpClientTransportConfig,
        protocol_headers: HashMap<HeaderName, HeaderValue>,
        sse_worker_tx: tokio::sync::mpsc::Sender<ServerJsonRpcMessage>,
        transport_task_ct: CancellationToken,
    ) {
        let uri = config.uri.clone();
        let auth_header = config.auth_header.clone();
        let retry_config = config.retry_config.clone();
        let reconnect_uri = config.uri.clone();
        let reconnect_auth_header = config.auth_header.clone();
        let max_sse_event_size = config.max_sse_event_size;

        streams.spawn(async move {
            let result = match client
                .get_stream_with_max_sse_event_size(
                    uri,
                    Some(session_id.clone()),
                    None,
                    auth_header,
                    protocol_headers.clone(),
                    max_sse_event_size,
                )
                .await
            {
                Ok(stream) => {
                    let sse_stream = SseAutoReconnectStream::new(
                        stream,
                        StreamableHttpClientReconnect {
                            client,
                            session_id: Some(session_id),
                            uri: reconnect_uri,
                            auth_header: reconnect_auth_header,
                            custom_headers: protocol_headers,
                            max_sse_event_size,
                        },
                        retry_config,
                    );
                    Self::execute_sse_stream(
                        sse_stream,
                        sse_worker_tx,
                        InboundStreamOrigin::Unassociated,
                        false,
                        transport_task_ct.child_token(),
                    )
                    .await
                }
                Err(StreamableHttpError::ServerDoesNotSupportSse) => {
                    tracing::debug!("server doesn't support sse, skip common stream");
                    Ok(())
                }
                Err(error) => {
                    tracing::error!("fail to get common stream: {error}");
                    Err(error)
                }
            };
            (None, result)
        });
    }

    /// Ends `run` once the server may hold a session.
    ///
    /// Aborts and joins the stream tasks, then deletes the session. Every step
    /// stops at the effective deadline: the earlier of `SESSION_CLEANUP_TIMEOUT`
    /// from now and the owner's [`CloseDeadline`], which is followed live, so a
    /// deadline the owner tightens mid-step cuts that step short. Local cleanup
    /// is judged first, and a reason once recorded is never removed, so no later
    /// step or DELETE status can turn incomplete cleanup into complete cleanup.
    /// Incomplete cleanup replaces `quit_reason` with
    /// [`StreamableHttpError::CleanupIncomplete`], which `close()` returns.
    async fn finish_session(
        streams: &mut tokio::task::JoinSet<SseTaskResult<C::Error>>,
        cleanup: Option<SessionCleanupInfo<C>>,
        unproven: Option<&str>,
        close_deadline: CloseDeadline,
        quit_reason: Result<(), WorkerQuitReason<StreamableHttpError<C::Error>>>,
    ) -> Result<(), WorkerQuitReason<StreamableHttpError<C::Error>>> {
        let cap = tokio::time::Instant::now() + SESSION_CLEANUP_TIMEOUT;
        let mut deadline_changes = close_deadline.subscribe();
        let mut incomplete = Vec::new();
        if let Some(reason) = unproven {
            incomplete.push(reason.to_owned());
        }
        streams.abort_all();
        if !streams.is_empty() {
            let joined = until_close_deadline(cap, &mut deadline_changes, async {
                let mut panicked = false;
                while let Some(joined) = streams.join_next().await {
                    panicked |= joined.is_err_and(|error| error.is_panic());
                }
                panicked
            })
            .await;
            match joined {
                Some(false) => {}
                Some(true) => incomplete.push("a stream task panicked".to_owned()),
                None => incomplete.push("stream tasks did not stop".to_owned()),
            }
        }
        let session_id = cleanup.as_ref().map(|cleanup| cleanup.session_id.clone());
        if let Some(cleanup) = cleanup {
            let cleanup_session_id = cleanup.session_id.clone();
            let established = cleanup.established;
            // Re-check before building the request: never start a DELETE once
            // the effective deadline has passed.
            if tokio::time::Instant::now() >= effective_close_deadline(cap, close_deadline.get()) {
                tracing::warn!(
                    session_id = cleanup_session_id.as_ref(),
                    "close deadline expired before delete session"
                );
                incomplete.push("close deadline expired".to_owned());
            } else {
                match until_close_deadline(
                    cap,
                    &mut deadline_changes,
                    cleanup.client.delete_session(
                        cleanup.uri,
                        cleanup.session_id,
                        cleanup.auth_header,
                        cleanup.protocol_headers,
                    ),
                )
                .await
                {
                    Some(Ok(_)) => {
                        tracing::info!(
                            session_id = cleanup_session_id.as_ref(),
                            "delete session success"
                        )
                    }
                    // HTTP 405: the MCP 2025-11-25 transport spec (Session
                    // Management) lets a server refuse client-initiated
                    // termination. This proves only that the local transport
                    // was released, not that the server deleted the session.
                    Some(Err(StreamableHttpError::ServerDoesNotSupportDeleteSession)) => {
                        tracing::info!(
                            session_id = cleanup_session_id.as_ref(),
                            "server doesn't support delete session; only local cleanup is proven"
                        )
                    }
                    // HTTP 404 to a DELETE carrying the session id the server
                    // answered `initialize` for: that session is already gone.
                    Some(Err(StreamableHttpError::SessionExpired)) if established => {
                        tracing::info!(
                            session_id = cleanup_session_id.as_ref(),
                            "session already absent"
                        )
                    }
                    Some(Err(e)) => {
                        tracing::error!(
                            session_id = cleanup_session_id.as_ref(),
                            "fail to delete session: {e}"
                        );
                        incomplete.push(format!("delete session failed: {e}"));
                    }
                    None => {
                        tracing::warn!(
                            session_id = cleanup_session_id.as_ref(),
                            "delete session stopped at the close deadline"
                        );
                        incomplete.push("delete session timed out".to_owned());
                    }
                }
            }
        }
        if incomplete.is_empty() {
            return quit_reason;
        }
        Err(WorkerQuitReason::fatal(
            StreamableHttpError::CleanupIncomplete {
                session_id,
                reason: incomplete.join("; ").into(),
            },
            "transport cleanup",
        ))
    }

    /// Performs a transparent re-initialization handshake after a session-expired 404.
    ///
    /// Takes an owned clone of the client (avoiding `&self` across `.await` so the
    /// future remains `Send` without requiring `C: Sync`).  POSTs the saved
    /// initialize request without a session ID, extracts the new session ID and
    /// protocol version, sends `notifications/initialized`, and returns the new
    /// `(session_id, protocol_headers)` pair.  The init result message is **not**
    /// forwarded to the handler because the handler already processed the original
    /// initialization. If the server answers the initialize with a JSON-RPC error,
    /// returns an error without sending `notifications/initialized`.
    async fn perform_reinitialization(
        client: C,
        saved_init_request: ClientJsonRpcMessage,
        uri: Arc<str>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
        max_sse_event_size: usize,
    ) -> Result<
        (
            Option<Arc<str>>,
            ProtocolVersion,
            HashMap<HeaderName, HeaderValue>,
        ),
        StreamableHttpError<C::Error>,
    > {
        let (init_msg, new_session_id_str) = client
            .post_message_with_max_sse_event_size(
                uri.clone(),
                saved_init_request,
                None,
                auth_header.clone(),
                custom_headers.clone(),
                max_sse_event_size,
            )
            .await?
            .expect_initialized::<C::Error>()
            .await?;
        if let ServerJsonRpcMessage::Error(error) = init_msg {
            return Err(StreamableHttpError::ReinitializationFailed(error.error));
        }

        let new_session_id: Option<Arc<str>> = new_session_id_str.map(|s| Arc::from(s.as_str()));

        let (negotiated_version, new_protocol_headers) =
            negotiate_version_headers(&init_msg, custom_headers);

        let initialized_notification = ClientJsonRpcMessage::notification(
            ClientNotification::InitializedNotification(InitializedNotification {
                method: Default::default(),
                extensions: Default::default(),
            }),
        );
        // SEP-2243: notifications carry no Mcp-Param-*, so an empty tool cache suffices.
        let initialized_headers = build_request_headers(
            &new_protocol_headers,
            &initialized_notification,
            &HashMap::new(),
            &negotiated_version,
        );
        client
            .post_message_with_max_sse_event_size(
                uri,
                initialized_notification,
                new_session_id.clone(),
                auth_header,
                initialized_headers,
                max_sse_event_size,
            )
            .await?
            .expect_accepted_or_json::<C::Error>()?;

        Ok((new_session_id, negotiated_version, new_protocol_headers))
    }
}

impl<C: StreamableHttpClient> Worker for StreamableHttpClientWorker<C> {
    type Role = RoleClient;
    type Error = StreamableHttpError<C::Error>;
    fn is_control_message(message: &ClientJsonRpcMessage) -> bool {
        match message {
            ClientJsonRpcMessage::Response(_) | ClientJsonRpcMessage::Error(_) => true,
            ClientJsonRpcMessage::Notification(notification) => matches!(
                notification.notification,
                ClientNotification::CancelledNotification(_)
            ),
            ClientJsonRpcMessage::Request(_) => false,
        }
    }
    fn supports_request_cancellation() -> bool {
        true
    }
    fn close_result(
        quit_reason: Result<(), WorkerQuitReason<Self::Error>>,
    ) -> Result<(), Self::Error> {
        match quit_reason {
            Err(WorkerQuitReason::Fatal {
                error: error @ StreamableHttpError::CleanupIncomplete { .. },
                ..
            }) => Err(error),
            _ => Ok(()),
        }
    }
    fn err_closed() -> Self::Error {
        StreamableHttpError::TransportChannelClosed
    }
    fn err_join(e: tokio::task::JoinError) -> Self::Error {
        StreamableHttpError::TokioJoinError(e)
    }
    fn config(&self) -> super::worker::WorkerConfig {
        super::worker::WorkerConfig {
            name: Some("StreamableHttpClientWorker".into()),
            channel_buffer_capacity: self.config.channel_buffer_capacity,
        }
    }
    async fn run(
        self,
        mut context: super::worker::WorkerContext<Self>,
    ) -> Result<(), WorkerQuitReason<Self::Error>> {
        let channel_buffer_capacity = self.config.channel_buffer_capacity;
        let (sse_worker_tx, mut sse_worker_rx) =
            tokio::sync::mpsc::channel::<ServerJsonRpcMessage>(channel_buffer_capacity);
        let config = self.config.clone();
        let transport_task_ct = context.cancellation_token.clone();
        let _drop_guard = transport_task_ct.clone().drop_guard();
        // `close()` cancels `transport_task_ct` and then joins this task, so every
        // startup await observes it; `recv_from_handler` and `send_to_handler` do so
        // themselves. Once the server may hold a session, every exit goes through
        // `finish_session`, the same cleanup the main loop ends with.
        let mut streams = tokio::task::JoinSet::new();
        let WorkerSendRequest {
            responder,
            message: startup_request,
            ..
        } = context.recv_from_handler().await?;
        let is_legacy_startup = matches!(
            &startup_request,
            ClientJsonRpcMessage::Request(request)
                if matches!(&request.request, ClientRequest::InitializeRequest(_))
        );
        let mut saved_init_request = is_legacy_startup.then(|| startup_request.clone());
        let empty_tool_cache = HashMap::new();
        let (bootstrap_version, bootstrap_headers) = if is_legacy_startup {
            (ProtocolVersion::default(), config.custom_headers.clone())
        } else {
            request_version_headers(
                &config.custom_headers,
                &startup_request,
                &ProtocolVersion::default(),
                &empty_tool_cache,
            )
        };
        let posted = tokio::select! {
            biased;
            _ = transport_task_ct.cancelled() => return Err(WorkerQuitReason::Cancelled),
            posted = self.client.post_message_with_max_sse_event_size(
                config.uri.clone(),
                startup_request,
                None,
                config.auth_header.clone(),
                bootstrap_headers.clone(),
                config.max_sse_event_size,
            ) => posted,
        };
        let (message, session_id) = match posted {
            Ok(res) => {
                let _ = responder.send(Ok(()));
                let response_session_id = post_response_session_id(&res);
                let initialized = tokio::select! {
                    biased;
                    _ = transport_task_ct.cancelled() => Err(WorkerQuitReason::Cancelled),
                    initialized = res.expect_initialized::<C::Error>() => initialized.map_err(
                        WorkerQuitReason::fatal_context("process initialize response"),
                    ),
                };
                match initialized {
                    Ok(initialized) => initialized,
                    Err(quit_reason) => {
                        // A legacy server may have created the session before its body failed.
                        let cleanup =
                            response_session_id
                                .filter(|_| is_legacy_startup)
                                .map(|session_id| SessionCleanupInfo {
                                    client: self.client.clone(),
                                    uri: config.uri.clone(),
                                    session_id,
                                    auth_header: config.auth_header.clone(),
                                    protocol_headers: bootstrap_headers.clone(),
                                    established: false,
                                });
                        return Self::finish_session(
                            &mut streams,
                            cleanup,
                            None,
                            context.close_deadline_handle(),
                            Err(quit_reason),
                        )
                        .await;
                    }
                }
            }
            Err(err) => {
                let msg = format!("{:?}", err);
                let _ = responder.send(Err(err));
                return Err(WorkerQuitReason::fatal(
                    StreamableHttpError::TransportChannelClosed,
                    msg,
                ));
            }
        };
        let mut uses_modern_http = !is_legacy_startup;
        let mut session_id: Option<Arc<str>> = if uses_modern_http {
            None
        } else if let Some(session_id) = session_id {
            Some(session_id.into())
        } else {
            if !self.config.allow_stateless {
                return Err(WorkerQuitReason::fatal(
                    StreamableHttpError::<C::Error>::MissingSessionIdInResponse,
                    "process initialize response",
                ));
            }
            None
        };
        let (mut negotiated_version, mut protocol_headers) = if is_legacy_startup {
            negotiate_version_headers(&message, config.custom_headers.clone())
        } else {
            (bootstrap_version, bootstrap_headers)
        };
        // SEP-2243: tool input schemas (name -> schema) cached from tools/list responses,
        // used to promote annotated tools/call arguments to Mcp-Param-* headers.
        let mut tool_header_cache: HashMap<String, Arc<JsonObject>> = HashMap::new();

        // Store session info for cleanup when run() exits (not spawned, so cleanup completes before close() returns)
        let mut session_cleanup_info = session_id.as_ref().map(|sid| SessionCleanupInfo {
            client: self.client.clone(),
            uri: config.uri.clone(),
            session_id: sid.clone(),
            auth_header: config.auth_header.clone(),
            protocol_headers: protocol_headers.clone(),
            established: true,
        });

        let startup_result: Result<(), WorkerQuitReason<Self::Error>> = 'startup: {
            if let Err(quit_reason) = context.send_to_handler(message).await {
                break 'startup Err(quit_reason);
            }
            if !is_legacy_startup {
                break 'startup Ok(());
            }
            let initialized_notification = match context.recv_from_handler().await {
                Ok(initialized_notification) => initialized_notification,
                Err(quit_reason) => break 'startup Err(quit_reason),
            };
            let initialized_headers = build_request_headers(
                &protocol_headers,
                &initialized_notification.message,
                &tool_header_cache,
                &negotiated_version,
            );
            let posted = tokio::select! {
                biased;
                _ = transport_task_ct.cancelled() => Err(WorkerQuitReason::Cancelled),
                posted = self.client.post_message_with_max_sse_event_size(
                    config.uri.clone(),
                    initialized_notification.message,
                    session_id.clone(),
                    config.auth_header.clone(),
                    initialized_headers,
                    config.max_sse_event_size,
                ) => posted.map_err(WorkerQuitReason::fatal_context(
                    "send initialized notification",
                )),
            };
            let accepted = posted.and_then(|response| {
                response.expect_accepted_or_json::<C::Error>().map_err(
                    WorkerQuitReason::fatal_context("process initialized notification response"),
                )
            });
            if let Err(quit_reason) = accepted {
                break 'startup Err(quit_reason);
            }
            let _ = initialized_notification.responder.send(Ok(()));
            Ok(())
        };
        if let Err(quit_reason) = startup_result {
            transport_task_ct.cancel();
            return Self::finish_session(
                &mut streams,
                session_cleanup_info,
                None,
                context.close_deadline_handle(),
                Err(quit_reason),
            )
            .await;
        }
        #[expect(
            clippy::large_enum_variant,
            reason = "the event is short-lived and boxing would add allocation in the event loop"
        )]
        enum Event<C: StreamableHttpClient> {
            ClientMessage(WorkerSendRequest<StreamableHttpClientWorker<C>>),
            ControlMessage(WorkerSendRequest<StreamableHttpClientWorker<C>>),
            StartPost(WorkerSendRequest<StreamableHttpClientWorker<C>>),
            PostResult(PostResult<C>),
            RecoveryTimeout,
            ServerMessage(ServerJsonRpcMessage),
            StreamResult {
                request_id: Option<RequestId>,
                result: Result<(), StreamableHttpError<C::Error>>,
            },
        }
        let mut pending_stream_response_ids = HashSet::new();
        let mut request_stream_cancellations =
            HashMap::<RequestId, Arc<RequestCancellationRegistration>>::new();
        let mut posts = FuturesUnordered::<BoxFuture<'static, PostResult<C>>>::new();
        let mut control_posts = FuturesUnordered::<BoxFuture<'static, PostResult<C>>>::new();
        let mut session_cancellation = CancellationToken::new();
        let mut pending_message: Option<WorkerSendRequest<Self>> = None;
        let mut recovery_posts = VecDeque::<WorkerSendRequest<Self>>::new();
        let mut recovery_deadline: Option<tokio::time::Instant> = None;
        let mut retrying_recovery = false;
        let mut reinit_in_flight = false;
        // A later successful attempt cannot account for an earlier unknown session.
        let mut reinit_unproven = false;
        let mut barrier_in_flight = false;
        let max_concurrent_requests = config.max_concurrent_requests.max(1);
        let mut awaiting_fallback_initialized = false;
        if let Some(session_id) = &session_id {
            Self::spawn_common_stream(
                &mut streams,
                self.client.clone(),
                session_id.clone(),
                &config,
                protocol_headers.clone(),
                sse_worker_tx.clone(),
                transport_task_ct.clone(),
            );
        }
        // Each POST uses the session and headers chosen when it starts.
        // Only this loop updates the current session and protocol version.
        let loop_result: Result<(), WorkerQuitReason<Self::Error>> = 'main_loop: loop {
            if retrying_recovery && recovery_posts.is_empty() && posts.is_empty() {
                retrying_recovery = false;
            }
            if !retrying_recovery
                && !recovery_posts.is_empty()
                && posts.is_empty()
                && control_posts.is_empty()
            {
                // Old POSTs have finished or reached the drain deadline.
                // Retry only ordinary POSTs that returned SessionExpired, at most once each.
                session_cancellation.cancel();
                recovery_deadline = None;
                reinit_in_flight = true;
                let recovery = tokio::select! {
                    _ = transport_task_ct.cancelled() => {
                        break 'main_loop Err(WorkerQuitReason::Cancelled);
                    }
                    result = tokio::time::timeout(
                        config.session_recovery_timeout,
                        Self::perform_reinitialization(
                            self.client.clone(),
                            saved_init_request.clone().expect("session recovery requires an initialize request"),
                            config.uri.clone(),
                            config.auth_header.clone(),
                            config.custom_headers.clone(),
                            config.max_sse_event_size,
                        ),
                    ) => result.unwrap_or(Err(StreamableHttpError::SessionRecoveryTimeout)),
                };
                match recovery {
                    Ok((new_session_id, new_version, new_headers)) => {
                        reinit_in_flight = false;
                        // Take custody before any fallible response draining or task join.
                        session_id = new_session_id;
                        negotiated_version = new_version;
                        protocol_headers = new_headers;
                        session_cleanup_info = session_id.as_ref().map(|sid| SessionCleanupInfo {
                            client: self.client.clone(),
                            uri: config.uri.clone(),
                            session_id: sid.clone(),
                            auth_header: config.auth_header.clone(),
                            protocol_headers: protocol_headers.clone(),
                            established: true,
                        });
                        streams.abort_all();
                        while streams.join_next().await.is_some() {}
                        request_stream_cancellations.clear();
                        if let Err(quit_reason) = Self::drain_queued_stream_messages(
                            &mut sse_worker_rx,
                            &mut context,
                            &mut pending_stream_response_ids,
                        )
                        .await
                        {
                            break 'main_loop Err(quit_reason);
                        }
                        if let Err(quit_reason) = Self::fail_pending_responses_except_retries(
                            &mut context,
                            &mut pending_stream_response_ids,
                            &recovery_posts,
                        )
                        .await
                        {
                            break 'main_loop Err(quit_reason);
                        }
                        // Do not send controls queued during recovery to the new session.
                        context.advance_control_generation();
                        session_cancellation = CancellationToken::new();
                        if let Some(session_id) = &session_id {
                            Self::spawn_common_stream(
                                &mut streams,
                                self.client.clone(),
                                session_id.clone(),
                                &config,
                                protocol_headers.clone(),
                                sse_worker_tx.clone(),
                                transport_task_ct.clone(),
                            );
                        }
                        retrying_recovery = true;
                    }
                    Err(error) => {
                        reinit_in_flight = false;
                        reinit_unproven = true;
                        session_cancellation = CancellationToken::new();
                        Self::fail_recovery_posts(
                            &mut recovery_posts,
                            &mut pending_stream_response_ids,
                            error,
                        );
                    }
                }
                continue;
            }

            let has_post_capacity = posts.len() < max_concurrent_requests;
            let may_start = (retrying_recovery || recovery_posts.is_empty())
                && !barrier_in_flight
                && has_post_capacity;
            let may_receive = may_start && pending_message.is_none() && !retrying_recovery;
            let queued = if retrying_recovery {
                recovery_posts.front_mut()
            } else {
                pending_message.as_mut()
            };
            let has_queued = queued.is_some();
            let can_process_queued = queued.as_ref().is_some_and(|request| {
                let retry_completed = retrying_recovery
                    && Self::client_request_id(&request.message)
                        .is_some_and(|id| !pending_stream_response_ids.contains(&id));
                let ordering_satisfied =
                    !Self::is_ordering_barrier(&request.message, &negotiated_version)
                        || (posts.is_empty() && control_posts.is_empty());
                retry_completed || (may_start && ordering_satisfied)
            });
            let event = tokio::select! {
                _ = async {
                    if can_process_queued {
                        return;
                    }
                    let request = queued.expect("a POST is queued");
                    let cancellation = request.cancellation_token().unwrap_or_default();
                    tokio::select! {
                        _ = request.responder.closed() => {}
                        _ = cancellation.cancelled() => {}
                    }
                }, if has_queued => {
                    let request = if retrying_recovery {
                        recovery_posts.pop_front()
                    } else {
                        pending_message.take()
                    };
                    Event::StartPost(request.expect("a POST is ready to start"))
                }
                _ = transport_task_ct.cancelled() => {
                    tracing::debug!("cancelled");
                    break 'main_loop Err(WorkerQuitReason::Cancelled);
                }
                message = context.from_handler_rx.recv(), if may_receive => {
                    match message {
                        Some(msg) => Event::ClientMessage(msg),
                        None => break 'main_loop Err(WorkerQuitReason::HandlerTerminated),
                    }
                },
                message = context.control_from_handler_rx.recv(),
                    if control_posts.is_empty() && !session_cancellation.is_cancelled() => {
                    match message {
                        Some(msg) => Event::ControlMessage(msg),
                        None => break 'main_loop Err(WorkerQuitReason::HandlerTerminated),
                    }
                },
                Some(result) = posts.next(), if !posts.is_empty() => {
                    Event::PostResult(result)
                },
                Some(result) = control_posts.next(), if !control_posts.is_empty() => {
                    Event::PostResult(result)
                },
                _ = async {
                    if let Some(deadline) = recovery_deadline {
                        tokio::time::sleep_until(deadline).await;
                    }
                }, if recovery_deadline.is_some() => Event::RecoveryTimeout,
                message = sse_worker_rx.recv() => {
                    let Some(message) = message else {
                        tracing::trace!("transport dropped, exiting");
                        break 'main_loop Err(WorkerQuitReason::HandlerTerminated);
                    };
                    Event::ServerMessage(message)
                },
                terminated_stream = streams.join_next(), if !streams.is_empty() => {
                    match terminated_stream {
                        Some(Ok((request_id, result))) => {
                            Event::StreamResult { request_id, result }
                        }
                        Some(Err(error)) => Event::StreamResult {
                            request_id: None,
                            result: Err(StreamableHttpError::TokioJoinError(error)),
                        },
                        None => continue,
                    }
                }
            };
            match event {
                Event::ClientMessage(send_request) => {
                    pending_message = Some(send_request);
                }
                Event::ControlMessage(send_request) => {
                    if send_request.responder.is_closed() {
                        continue;
                    }
                    let cancellation_request_id =
                        Self::cancellation_request_id(&send_request.message);
                    let stale = send_request.control_generation() != context.control_generation();
                    if stale {
                        // Do not send old controls to a replacement session.
                        let result = match cancellation_request_id {
                            Some(_) => Ok(()),
                            None => Err(StreamableHttpError::SessionExpired),
                        };
                        let _ = send_request.responder.send(result);
                        continue;
                    }
                    if let Some(request_id) = cancellation_request_id {
                        drop(request_stream_cancellations.remove(request_id));
                        pending_stream_response_ids.remove(request_id);
                        if uses_modern_http {
                            let _ = send_request.responder.send(Ok(()));
                            continue;
                        }
                    }
                    let (version, headers) = request_version_headers(
                        &protocol_headers,
                        &send_request.message,
                        &negotiated_version,
                        &tool_header_cache,
                    );
                    control_posts.push(Self::post_request(
                        self.client.clone(),
                        &config,
                        send_request,
                        PostSession {
                            id: session_id.clone(),
                            headers,
                            version,
                            cancellation: session_cancellation.clone(),
                        },
                        transport_task_ct.clone(),
                    ));
                }
                Event::RecoveryTimeout => {
                    recovery_deadline = None;
                    session_cancellation.cancel();
                    tracing::warn!("old-session POSTs did not finish before the recovery deadline");
                }
                Event::StartPost(send_request) => {
                    let request_id = Self::client_request_id(&send_request.message);
                    let send_cancelled = send_request.responder.is_closed()
                        || send_request
                            .cancellation_token()
                            .is_some_and(|token| token.is_cancelled());
                    let retry_completed = retrying_recovery
                        && request_id
                            .as_ref()
                            .is_some_and(|id| !pending_stream_response_ids.contains(id));
                    if send_cancelled || retry_completed {
                        if retrying_recovery && let Some(id) = &request_id {
                            pending_stream_response_ids.remove(id);
                        }
                        let _ = send_request.responder.send(Ok(()));
                        continue;
                    }
                    let message = &send_request.message;
                    let is_fallback_initialize = saved_init_request.is_none()
                        && matches!(
                            message,
                            ClientJsonRpcMessage::Request(request)
                                if matches!(
                                    &request.request,
                                    ClientRequest::InitializeRequest(_)
                                )
                        );
                    if is_fallback_initialize {
                        saved_init_request = Some(message.clone());
                        let WorkerSendRequest {
                            message, responder, ..
                        } = send_request;
                        // Servers do not assign sessions to `server/discover`, so a
                        // fallback initialize starts from a clean slate: no session
                        // ID, no cleanup state, and no streams to tear down.
                        debug_assert!(
                            session_id.is_none()
                                && session_cleanup_info.is_none()
                                && streams.is_empty(),
                            "discover bootstrap must not create session state"
                        );
                        uses_modern_http = false;

                        let response = tokio::select! {
                            biased;
                            _ = transport_task_ct.cancelled() => {
                                break 'main_loop Err(WorkerQuitReason::Cancelled);
                            }
                            response = self.client.post_message_with_max_sse_event_size(
                                config.uri.clone(),
                                message,
                                None,
                                config.auth_header.clone(),
                                config.custom_headers.clone(),
                                config.max_sse_event_size,
                            ) => response,
                        };
                        let response = match response {
                            Ok(response) => {
                                let _ = responder.send(Ok(()));
                                response
                            }
                            Err(error) => {
                                let _ = responder.send(Err(error));
                                continue;
                            }
                        };
                        let response_session_id = post_response_session_id(&response);
                        let initialized = tokio::select! {
                            biased;
                            _ = transport_task_ct.cancelled() => Err(WorkerQuitReason::Cancelled),
                            initialized = response.expect_initialized::<C::Error>() => initialized
                                .map_err(WorkerQuitReason::fatal_context(
                                    "process fallback initialize response",
                                )),
                        };
                        let (initialize_response, new_session_id) = match initialized {
                            Ok(initialized) => initialized,
                            Err(quit_reason) => {
                                // The server may have created the session before its body failed.
                                session_cleanup_info =
                                    response_session_id.map(|session_id| SessionCleanupInfo {
                                        client: self.client.clone(),
                                        uri: config.uri.clone(),
                                        session_id,
                                        auth_header: config.auth_header.clone(),
                                        protocol_headers: config.custom_headers.clone(),
                                        established: false,
                                    });
                                break 'main_loop Err(quit_reason);
                            }
                        };
                        session_id = new_session_id.map(Arc::from);
                        if session_id.is_none() && !config.allow_stateless {
                            break 'main_loop Err(WorkerQuitReason::fatal(
                                StreamableHttpError::<C::Error>::MissingSessionIdInResponse,
                                "process fallback initialize response",
                            ));
                        }
                        (negotiated_version, protocol_headers) = negotiate_version_headers(
                            &initialize_response,
                            config.custom_headers.clone(),
                        );
                        session_cleanup_info =
                            session_id.as_ref().map(|session_id| SessionCleanupInfo {
                                client: self.client.clone(),
                                uri: config.uri.clone(),
                                session_id: session_id.clone(),
                                auth_header: config.auth_header.clone(),
                                protocol_headers: protocol_headers.clone(),
                                established: true,
                            });
                        if let Err(quit_reason) = context.send_to_handler(initialize_response).await
                        {
                            break 'main_loop Err(quit_reason);
                        }
                        awaiting_fallback_initialized = true;
                        continue;
                    }

                    let barrier = Self::is_ordering_barrier(message, &negotiated_version);
                    debug_assert!(!barrier || (posts.is_empty() && control_posts.is_empty()));
                    let inline_version = match message {
                        ClientJsonRpcMessage::Request(request) => {
                            request.request.get_meta().protocol_version()
                        }
                        _ => None,
                    };
                    let (request_version, request_headers) = request_version_headers(
                        &protocol_headers,
                        message,
                        &negotiated_version,
                        &tool_header_cache,
                    );
                    if inline_version.is_some() {
                        negotiated_version = request_version.clone();
                        if let Ok(value) = HeaderValue::from_str(request_version.as_str()) {
                            protocol_headers
                                .insert(HeaderName::from_static("mcp-protocol-version"), value);
                        }
                        if let Some(cleanup) = &mut session_cleanup_info {
                            cleanup.protocol_headers = protocol_headers.clone();
                        }
                    }
                    barrier_in_flight = barrier;
                    // The common stream can return a response before this POST finishes.
                    if let Some(request_id) = request_id {
                        pending_stream_response_ids.insert(request_id);
                    }
                    posts.push(Self::post_request(
                        self.client.clone(),
                        &config,
                        send_request,
                        PostSession {
                            id: session_id.clone(),
                            headers: request_headers,
                            version: request_version,
                            cancellation: session_cancellation.clone(),
                        },
                        transport_task_ct.clone(),
                    ));
                }
                Event::PostResult(PostResult {
                    send_request,
                    response,
                    version,
                }) => {
                    let is_control = Self::is_control_message(&send_request.message);
                    if !is_control {
                        // An ordering barrier runs without other ordinary POSTs.
                        barrier_in_flight = false;
                    }
                    let request_id = Self::client_request_id(&send_request.message);
                    if request_id
                        .as_ref()
                        .is_some_and(|id| !pending_stream_response_ids.contains(id))
                    {
                        // A stream response or cancellation already completed this request.
                        let _ = send_request.responder.send(Ok(()));
                        continue;
                    }
                    let will_retry =
                        matches!(&response, Some(Err(StreamableHttpError::SessionExpired)))
                            && !is_control
                            && !retrying_recovery
                            && config.reinit_on_expired_session
                            && saved_init_request.is_some();
                    let awaits_stream_response = matches!(
                        &response,
                        Some(Ok(StreamableHttpPostResponse::Accepted
                            | StreamableHttpPostResponse::Sse(..)))
                    );
                    if !will_retry
                        && !awaits_stream_response
                        && let Some(id) = &request_id
                    {
                        pending_stream_response_ids.remove(id);
                    }
                    let Some(response) = response else {
                        let _ = send_request.responder.send(Ok(()));
                        continue;
                    };
                    if will_retry {
                        if recovery_posts.is_empty() {
                            recovery_deadline =
                                Some(tokio::time::Instant::now() + config.session_recovery_timeout);
                        }
                        recovery_posts.push_back(send_request);
                        continue;
                    }
                    let request_cancellation = send_request.cancellation_registration();
                    let WorkerSendRequest {
                        message, responder, ..
                    } = send_request;
                    let is_initialized_notification = matches!(
                        &message,
                        ClientJsonRpcMessage::Notification(notification)
                            if matches!(
                                &notification.notification,
                                ClientNotification::InitializedNotification(_)
                            )
                    );
                    let send_result = match response {
                        Err(e) => Err(e),
                        Ok(StreamableHttpPostResponse::Accepted) => {
                            tracing::trace!("client message accepted");
                            Ok(())
                        }
                        Ok(StreamableHttpPostResponse::Json(mut message, ..)) => {
                            cache_tools_from_response(
                                &mut tool_header_cache,
                                &mut message,
                                &version,
                            );
                            if let Err(quit_reason) = context.send_to_handler(message).await {
                                break 'main_loop Err(quit_reason);
                            }
                            Ok(())
                        }
                        Ok(StreamableHttpPostResponse::Sse(stream, ..)) => {
                            let stream_request_id = request_id;
                            let sse_stream = Self::response_sse_to_jsonrpc(
                                stream,
                                session_id.clone(),
                                self.client.clone(),
                                config.uri.clone(),
                                config.auth_header.clone(),
                                protocol_headers.clone(),
                                config.max_sse_event_size,
                                self.config.retry_config.clone(),
                            );
                            let request_ct = request_cancellation
                                .as_ref()
                                .map(|registration| registration.token())
                                .unwrap_or_else(|| transport_task_ct.child_token());
                            // A legacy adapter may need the open stream to handle cancellation.
                            let stream_ct = if uses_modern_http {
                                request_ct.clone()
                            } else {
                                request_cancellation
                                    .as_ref()
                                    .map(|registration| registration.lifetime_token())
                                    .unwrap_or_else(|| request_ct.clone())
                            };
                            if let (Some(request_id), Some(registration)) =
                                (stream_request_id.as_ref(), request_cancellation)
                            {
                                request_stream_cancellations
                                    .insert(request_id.clone(), registration);
                            }
                            let stream_tx = sse_worker_tx.clone();
                            let origin = match &stream_request_id {
                                Some(id) => InboundStreamOrigin::OutboundRequest(id.clone()),
                                None => InboundStreamOrigin::Unassociated,
                            };
                            streams.spawn(async move {
                                let result = Self::run_response_stream(
                                    sse_stream,
                                    stream_tx,
                                    origin,
                                    request_ct,
                                    stream_ct,
                                    uses_modern_http,
                                )
                                .await;
                                (stream_request_id, result)
                            });
                            tracing::trace!("got new sse stream");
                            Ok(())
                        }
                    };
                    if send_result.is_ok()
                        && awaiting_fallback_initialized
                        && is_initialized_notification
                    {
                        if let Some(session_id) = &session_id {
                            Self::spawn_common_stream(
                                &mut streams,
                                self.client.clone(),
                                session_id.clone(),
                                &config,
                                protocol_headers.clone(),
                                sse_worker_tx.clone(),
                                transport_task_ct.clone(),
                            );
                        }
                        awaiting_fallback_initialized = false;
                    }
                    let _ = responder.send(send_result);
                }
                Event::ServerMessage(mut json_rpc_message) => {
                    // Match against all pending requests, not just open response streams.
                    if let Some(request_id) = Self::clear_stream_response_pending(
                        &mut pending_stream_response_ids,
                        &json_rpc_message,
                    ) {
                        drop(request_stream_cancellations.remove(&request_id));
                    }
                    cache_tools_from_response(
                        &mut tool_header_cache,
                        &mut json_rpc_message,
                        &negotiated_version,
                    );
                    // send the message to the handler
                    if let Err(e) = context.send_to_handler(json_rpc_message).await {
                        break 'main_loop Err(e);
                    }
                }
                Event::StreamResult { request_id, result } => {
                    if let Some(request_id) = request_id {
                        if let Err(quit_reason) = Self::drain_queued_stream_messages(
                            &mut sse_worker_rx,
                            &mut context,
                            &mut pending_stream_response_ids,
                        )
                        .await
                        {
                            break 'main_loop Err(quit_reason);
                        }
                        let cancelled = request_stream_cancellations
                            .remove(&request_id)
                            .is_some_and(|registration| registration.token().is_cancelled());
                        if pending_stream_response_ids.remove(&request_id)
                            && !cancelled
                            && let Err(quit_reason) = context
                                .send_to_handler(ServerJsonRpcMessage::error(
                                    ErrorData::transport_closed(
                                        "streamable HTTP response stream closed before its final response",
                                    ),
                                    Some(request_id),
                                ))
                                .await
                        {
                            break 'main_loop Err(quit_reason);
                        }
                    }
                    if result.is_err() {
                        tracing::warn!(
                            "sse client event stream terminated with error: {:?}",
                            result
                        );
                    }
                }
            }
        };

        // Stop outstanding http requests before deleting their session.
        transport_task_ct.cancel();
        drop(posts);
        drop(control_posts);
        drop(pending_message);
        drop(recovery_posts);
        let close_deadline = context.close_deadline_handle();
        Self::finish_session(
            &mut streams,
            session_cleanup_info,
            (reinit_in_flight || reinit_unproven)
                .then_some("session recovery may have created an unaccounted session"),
            close_deadline,
            loop_result,
        )
        .await
    }
}

/// A client-agnostic HTTP transport for RMCP that supports streaming responses.
///
/// This transport allows you to choose your preferred HTTP client implementation
/// by implementing the [`StreamableHttpClient`] trait. The transport handles
/// session management, SSE streaming, and automatic reconnection.
///
/// # Usage
///
/// ## Using reqwest
///
/// ```rust,no_run
/// use rmcp::transport::StreamableHttpClientTransport;
///
/// // Enable the reqwest feature in Cargo.toml:
/// // rmcp = { version = "0.5", features = ["transport-streamable-http-client-reqwest"] }
///
/// let transport = StreamableHttpClientTransport::from_uri("http://localhost:8000/mcp");
/// ```
///
/// ## Using a custom HTTP client
///
/// ```rust,no_run
/// use rmcp::transport::streamable_http_client::{
///     StreamableHttpClient,
///     StreamableHttpClientTransport,
///     StreamableHttpClientTransportConfig
/// };
/// use std::sync::Arc;
/// use std::collections::HashMap;
/// use futures::stream::BoxStream;
/// use rmcp::model::ClientJsonRpcMessage;
/// use http::{HeaderName, HeaderValue};
/// use sse_stream::{Sse, Error as SseError};
///
/// #[derive(Clone)]
/// struct MyHttpClient;
///
/// #[derive(Debug, thiserror::Error)]
/// struct MyError;
///
/// impl std::fmt::Display for MyError {
///     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
///         write!(f, "MyError")
///     }
/// }
///
/// impl StreamableHttpClient for MyHttpClient {
///     type Error = MyError;
///
///     async fn post_message(
///         &self,
///         _uri: Arc<str>,
///         _message: ClientJsonRpcMessage,
///         _session_id: Option<Arc<str>>,
///         _auth_header: Option<String>,
///         _custom_headers: HashMap<HeaderName, HeaderValue>,
///     ) -> Result<rmcp::transport::streamable_http_client::StreamableHttpPostResponse, rmcp::transport::streamable_http_client::StreamableHttpError<Self::Error>> {
///         todo!()
///     }
///
///     async fn delete_session(
///         &self,
///         _uri: Arc<str>,
///         _session_id: Arc<str>,
///         _auth_header: Option<String>,
///         _custom_headers: HashMap<HeaderName, HeaderValue>,
///     ) -> Result<(), rmcp::transport::streamable_http_client::StreamableHttpError<Self::Error>> {
///         todo!()
///     }
///
///     async fn get_stream(
///         &self,
///         _uri: Arc<str>,
///         _session_id: Option<Arc<str>>,
///         _last_event_id: Option<String>,
///         _auth_header: Option<String>,
///         _custom_headers: HashMap<HeaderName, HeaderValue>,
///     ) -> Result<BoxStream<'static, Result<Sse, SseError>>, rmcp::transport::streamable_http_client::StreamableHttpError<Self::Error>> {
///         todo!()
///     }
/// }
///
/// let transport = StreamableHttpClientTransport::with_client(
///     MyHttpClient,
///     StreamableHttpClientTransportConfig::with_uri("http://localhost:8000/mcp")
/// );
/// ```
///
/// # Feature Flags
///
/// - `transport-streamable-http-client`: Base feature providing the generic transport infrastructure
/// - `transport-streamable-http-client-reqwest`: Includes reqwest HTTP client support with convenience methods
pub type StreamableHttpClientTransport<C> = WorkerTransport<StreamableHttpClientWorker<C>>;

impl<C: StreamableHttpClient> StreamableHttpClientTransport<C> {
    /// Creates a new transport with a custom HTTP client implementation.
    ///
    /// This method allows you to use any HTTP client that implements the [`StreamableHttpClient`] trait.
    /// Use this when you want to use a custom HTTP client or when the reqwest feature is not enabled.
    ///
    /// # Arguments
    ///
    /// * `client` - Your HTTP client implementation
    /// * `config` - Transport configuration including the server URI
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use rmcp::transport::streamable_http_client::{
    ///     StreamableHttpClient,
    ///     StreamableHttpClientTransport,
    ///     StreamableHttpClientTransportConfig
    /// };
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    /// use futures::stream::BoxStream;
    /// use rmcp::model::ClientJsonRpcMessage;
    /// use http::{HeaderName, HeaderValue};
    /// use sse_stream::{Sse, Error as SseError};
    ///
    /// // Define your custom client
    /// #[derive(Clone)]
    /// struct MyHttpClient;
    ///
    /// #[derive(Debug, thiserror::Error)]
    /// struct MyError;
    ///
    /// impl std::fmt::Display for MyError {
    ///     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    ///         write!(f, "MyError")
    ///     }
    /// }
    ///
    /// impl StreamableHttpClient for MyHttpClient {
    ///     type Error = MyError;
    ///
    ///     async fn post_message(
    ///         &self,
    ///         _uri: Arc<str>,
    ///         _message: ClientJsonRpcMessage,
    ///         _session_id: Option<Arc<str>>,
    ///         _auth_header: Option<String>,
    ///         _custom_headers: HashMap<HeaderName, HeaderValue>,
    ///     ) -> Result<rmcp::transport::streamable_http_client::StreamableHttpPostResponse, rmcp::transport::streamable_http_client::StreamableHttpError<Self::Error>> {
    ///         todo!()
    ///     }
    ///
    ///     async fn delete_session(
    ///         &self,
    ///         _uri: Arc<str>,
    ///         _session_id: Arc<str>,
    ///         _auth_header: Option<String>,
    ///         _custom_headers: HashMap<HeaderName, HeaderValue>,
    ///     ) -> Result<(), rmcp::transport::streamable_http_client::StreamableHttpError<Self::Error>> {
    ///         todo!()
    ///     }
    ///
    ///     async fn get_stream(
    ///         &self,
    ///         _uri: Arc<str>,
    ///         _session_id: Option<Arc<str>>,
    ///         _last_event_id: Option<String>,
    ///         _auth_header: Option<String>,
    ///         _custom_headers: HashMap<HeaderName, HeaderValue>,
    ///     ) -> Result<BoxStream<'static, Result<Sse, SseError>>, rmcp::transport::streamable_http_client::StreamableHttpError<Self::Error>> {
    ///         todo!()
    ///     }
    /// }
    ///
    /// let transport = StreamableHttpClientTransport::with_client(
    ///     MyHttpClient,
    ///     StreamableHttpClientTransportConfig::with_uri("http://localhost:8000/mcp")
    /// );
    /// ```
    pub fn with_client(client: C, config: StreamableHttpClientTransportConfig) -> Self {
        let worker = StreamableHttpClientWorker::new(client, config);
        WorkerTransport::spawn(worker)
    }
}
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct StreamableHttpClientTransportConfig {
    pub uri: Arc<str>,
    pub retry_config: Arc<dyn SseRetryPolicy>,
    pub channel_buffer_capacity: usize,
    /// Maximum number of ordinary http POSTs in progress (default: 16).
    /// A POST stops counting when it completes or opens an sse response stream.
    /// Zero is treated as one. Cancellation and replies use a separate queue
    /// with one extra POST slot and use [`Self::control_request_timeout`].
    pub max_concurrent_requests: usize,
    /// Maximum time a cancellation or reply POST can run after it starts (default: five seconds).
    /// Time spent waiting in the control queue does not count toward this timeout.
    pub control_request_timeout: Duration,
    /// Maximum wait for old POSTs to finish before session recovery (default: five seconds).
    /// The new initialization handshake has a separate timeout of the same length.
    /// An unfinished old POST returns [`StreamableHttpError::SessionRecoveryTimeout`]
    /// and is not retried because the server may have processed it.
    pub session_recovery_timeout: Duration,
    /// if true, the transport will not require a session to be established
    pub allow_stateless: bool,
    /// The value to send in the authorization header
    pub auth_header: Option<String>,
    /// Custom HTTP headers to include with every request
    pub custom_headers: HashMap<HeaderName, HeaderValue>,
    /// Maximum raw size of one SSE event accepted from the server.
    ///
    /// The built-in reqwest and Unix socket clients enforce this value. Custom
    /// [`StreamableHttpClient`] implementations must override the corresponding
    /// `*_with_max_sse_event_size` methods to enforce it.
    pub max_sse_event_size: usize,
    /// Automatically creates a new session when the server reports an expired
    /// session (`http 404`).
    ///
    /// Ordinary POSTs that fail with `SessionExpired` in the same session share one
    /// recovery attempt:
    /// 1. Wait for old POSTs, up to [`Self::session_recovery_timeout`].
    /// 2. Repeat the original `initialize` handshake and open new streams.
    /// 3. Retry each ordinary POST that failed with `SessionExpired` once.
    ///
    /// Control POSTs and other POST failures are not retried. If recovery or a retry
    /// fails, the transport returns an error to the caller.
    pub reinit_on_expired_session: bool,
}

impl StreamableHttpClientTransportConfig {
    pub fn with_uri(uri: impl Into<Arc<str>>) -> Self {
        Self {
            uri: uri.into(),
            ..Default::default()
        }
    }

    /// Set how many ordinary POSTs can run at once. One keeps them serial; zero also means one.
    pub fn max_concurrent_requests(mut self, limit: usize) -> Self {
        self.max_concurrent_requests = limit.max(1);
        self
    }

    /// Set the timeout for cancellation and reply POSTs, starting when each POST starts.
    pub fn control_request_timeout(mut self, timeout: Duration) -> Self {
        self.control_request_timeout = timeout;
        self
    }

    /// Set the separate timeouts for waiting for old POSTs and creating a replacement session.
    pub fn session_recovery_timeout(mut self, timeout: Duration) -> Self {
        self.session_recovery_timeout = timeout;
        self
    }

    /// Set the authorization header to send with requests
    ///
    /// # Arguments
    ///
    /// * `value` - A bearer token without the `Bearer ` prefix
    pub fn auth_header<T: Into<String>>(mut self, value: T) -> Self {
        // set our authorization header
        self.auth_header = Some(value.into());
        self
    }

    /// Set custom HTTP headers to include with every request
    ///
    /// # Arguments
    ///
    /// * `custom_headers` - A HashMap of header names to header values
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::collections::HashMap;
    /// use http::{HeaderName, HeaderValue};
    /// use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
    ///
    /// let mut headers = HashMap::new();
    /// headers.insert(
    ///     HeaderName::from_static("x-custom-header"),
    ///     HeaderValue::from_static("custom-value")
    /// );
    ///
    /// let config = StreamableHttpClientTransportConfig::with_uri("http://localhost:8000")
    ///     .custom_headers(headers);
    /// ```
    pub fn custom_headers(mut self, custom_headers: HashMap<HeaderName, HeaderValue>) -> Self {
        self.custom_headers = custom_headers;
        self
    }

    /// Set the maximum raw size of one SSE event accepted from the server.
    pub fn max_sse_event_size(mut self, bytes: usize) -> Self {
        self.max_sse_event_size = bytes;
        self
    }

    /// Set whether the transport should attempt transparent re-initialization on session expiration
    /// See [`Self::reinit_on_expired_session`] for details.
    /// # Example
    /// ```rust,no_run
    /// use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
    /// let config = StreamableHttpClientTransportConfig::with_uri("http://localhost:8000")
    ///     .reinit_on_expired_session(true);
    /// ```
    pub fn reinit_on_expired_session(mut self, enable: bool) -> Self {
        self.reinit_on_expired_session = enable;
        self
    }
}

impl Default for StreamableHttpClientTransportConfig {
    fn default() -> Self {
        Self {
            uri: "localhost".into(),
            retry_config: Arc::new(ExponentialBackoff::default()),
            channel_buffer_capacity: 16,
            max_concurrent_requests: 16,
            control_request_timeout: Duration::from_secs(5),
            session_recovery_timeout: Duration::from_secs(5),
            allow_stateless: true,
            auth_header: None,
            custom_headers: HashMap::new(),
            max_sse_event_size: DEFAULT_MAX_SSE_EVENT_SIZE,
            reinit_on_expired_session: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use serde_json::json;

    use super::*;
    use crate::{
        model::{
            GetExtensions, ListToolsResult, NumberOrString, ServerRequest, ServerResult, Tool,
        },
        service::InboundStreamOrigin,
        transport::Transport,
    };

    #[test]
    fn json_rpc_body_preview_empty_html_and_object() {
        assert_eq!(json_rpc_body_preview(b""), "<empty>");
        assert_eq!(json_rpc_body_preview(b"{}"), "{}");
        assert_eq!(
            json_rpc_body_preview(b"<html>not json</html>"),
            "<html>not json</html>"
        );
    }

    #[test]
    fn json_rpc_body_preview_truncates_long_bodies() {
        let long = vec![b'x'; JSON_RPC_BODY_PREVIEW_LEN + 40];
        let preview = json_rpc_body_preview(&long);
        assert_eq!(preview.len(), JSON_RPC_BODY_PREVIEW_LEN);
        assert!(preview.chars().all(|c| c == 'x'));
    }

    #[test]
    fn json_rpc_parse_error_message_includes_preview() {
        let err = serde_json::from_slice::<ServerJsonRpcMessage>(b"{}").unwrap_err();
        let msg = json_rpc_parse_error_message(&err, b"{}");
        assert!(msg.contains("could not parse JSON response as ServerJsonRpcMessage"));
        assert!(msg.contains("{}"), "missing body preview: {msg}");
    }

    #[expect(
        deprecated,
        reason = "Sampling is deprecated by SEP-2577 but remains the canonical restricted request"
    )]
    fn sampling_request_message(id: i64) -> ServerJsonRpcMessage {
        use crate::model::{CreateMessageRequest, CreateMessageRequestParams, SamplingMessage};
        ServerJsonRpcMessage::request(
            ServerRequest::CreateMessageRequest(CreateMessageRequest::new(
                CreateMessageRequestParams::new(vec![SamplingMessage::user_text("hi")], 16),
            )),
            NumberOrString::Number(id),
        )
    }

    #[tokio::test]
    async fn execute_sse_stream_marks_inbound_requests_with_origin() {
        for origin in [
            InboundStreamOrigin::Unassociated,
            InboundStreamOrigin::OutboundRequest(RequestId::Number(3)),
        ] {
            let response = ServerJsonRpcMessage::response(
                ServerResult::ListToolsResult(ListToolsResult::default()),
                NumberOrString::Number(1),
            );
            let stream = futures::stream::iter([Ok(sampling_request_message(9)), Ok(response)]);
            let (tx, mut rx) = tokio::sync::mpsc::channel(4);
            StreamableHttpClientWorker::<StatelessReconnectClient>::execute_sse_stream(
                stream,
                tx,
                origin.clone(),
                false,
                CancellationToken::new(),
            )
            .await
            .expect("stream completes");

            let ServerJsonRpcMessage::Request(request) =
                rx.recv().await.expect("request forwarded")
            else {
                panic!("expected request first");
            };
            assert_eq!(
                request.request.extensions().get::<InboundStreamOrigin>(),
                Some(&origin),
                "inbound requests must carry their stream origin"
            );
            // Responses are correlated by JSON-RPC id; no marker needed or added.
            assert!(matches!(
                rx.recv().await.expect("response forwarded"),
                ServerJsonRpcMessage::Response(_)
            ));
        }
    }

    type ReconnectAttempt = (Option<String>, Option<String>);

    #[derive(Clone, Default)]
    struct StatelessReconnectClient {
        reconnects: Arc<Mutex<Vec<ReconnectAttempt>>>,
    }

    impl StreamableHttpClient for StatelessReconnectClient {
        type Error = std::io::Error;

        async fn post_message(
            &self,
            _uri: Arc<str>,
            _message: ClientJsonRpcMessage,
            _session_id: Option<Arc<str>>,
            _auth_header: Option<String>,
            _custom_headers: HashMap<HeaderName, HeaderValue>,
        ) -> Result<StreamableHttpPostResponse, StreamableHttpError<Self::Error>> {
            Err(StreamableHttpError::UnexpectedServerResponse(
                "unexpected POST".into(),
            ))
        }

        async fn delete_session(
            &self,
            _uri: Arc<str>,
            _session_id: Arc<str>,
            _auth_header: Option<String>,
            _custom_headers: HashMap<HeaderName, HeaderValue>,
        ) -> Result<(), StreamableHttpError<Self::Error>> {
            Ok(())
        }

        async fn get_stream(
            &self,
            _uri: Arc<str>,
            session_id: Option<Arc<str>>,
            last_event_id: Option<String>,
            _auth_header: Option<String>,
            _custom_headers: HashMap<HeaderName, HeaderValue>,
        ) -> Result<BoxedSseStream, StreamableHttpError<Self::Error>> {
            self.reconnects
                .lock()
                .expect("lock reconnects")
                .push((session_id.map(|id| id.to_string()), last_event_id));
            let response = ServerJsonRpcMessage::response(
                ServerResult::ListToolsResult(ListToolsResult::default()),
                NumberOrString::Number(1),
            );
            Ok(futures::stream::once(async move {
                Ok(Sse {
                    event: None,
                    data: Some(serde_json::to_string(&response).expect("serialize response")),
                    id: Some("event-1".into()),
                    retry: None,
                })
            })
            .boxed())
        }
    }

    #[tokio::test]
    async fn stateless_response_reconnects_with_last_event_id() {
        let initial = futures::stream::iter([Ok(Sse {
            event: None,
            data: None,
            id: Some("event-0".into()),
            retry: Some(0),
        })])
        .boxed();
        let client = StatelessReconnectClient::default();
        let reconnects = client.reconnects.clone();
        let stream =
            StreamableHttpClientWorker::<StatelessReconnectClient>::response_sse_to_jsonrpc(
                initial,
                None,
                client,
                Arc::from("http://localhost/mcp"),
                None,
                HashMap::new(),
                DEFAULT_MAX_SSE_EVENT_SIZE,
                Arc::new(ExponentialBackoff {
                    max_times: Some(1),
                    base_duration: Duration::ZERO,
                    max_delay: None,
                }),
            );
        let mut stream = std::pin::pin!(stream);

        let message = stream.next().await.expect("replayed response").unwrap();

        assert!(matches!(message, ServerJsonRpcMessage::Response(_)));
        assert_eq!(
            reconnects.lock().expect("lock reconnects").as_slice(),
            &[(None, Some("event-0".into()))]
        );
    }

    #[derive(Clone, Default)]
    struct ResumedRequestClient {
        reconnects: Arc<Mutex<Vec<ReconnectAttempt>>>,
    }

    impl StreamableHttpClient for ResumedRequestClient {
        type Error = std::io::Error;

        async fn post_message(
            &self,
            _uri: Arc<str>,
            _message: ClientJsonRpcMessage,
            _session_id: Option<Arc<str>>,
            _auth_header: Option<String>,
            _custom_headers: HashMap<HeaderName, HeaderValue>,
        ) -> Result<StreamableHttpPostResponse, StreamableHttpError<Self::Error>> {
            Err(StreamableHttpError::UnexpectedServerResponse(
                "unexpected POST".into(),
            ))
        }

        async fn delete_session(
            &self,
            _uri: Arc<str>,
            _session_id: Arc<str>,
            _auth_header: Option<String>,
            _custom_headers: HashMap<HeaderName, HeaderValue>,
        ) -> Result<(), StreamableHttpError<Self::Error>> {
            Ok(())
        }

        async fn get_stream(
            &self,
            _uri: Arc<str>,
            session_id: Option<Arc<str>>,
            last_event_id: Option<String>,
            _auth_header: Option<String>,
            _custom_headers: HashMap<HeaderName, HeaderValue>,
        ) -> Result<BoxedSseStream, StreamableHttpError<Self::Error>> {
            self.reconnects
                .lock()
                .expect("lock reconnects")
                .push((session_id.map(|id| id.to_string()), last_event_id));
            let request = sampling_request_message(9);
            let response = ServerJsonRpcMessage::response(
                ServerResult::ListToolsResult(ListToolsResult::default()),
                NumberOrString::Number(1),
            );
            // Stay open after the response, like a live connection, so the
            // post-response drain in `execute_sse_stream` doesn't trigger
            // further reconnects.
            Ok(futures::stream::iter([request, response].map(|message| {
                Ok(Sse {
                    event: None,
                    data: Some(serde_json::to_string(&message).expect("serialize message")),
                    id: None,
                    retry: None,
                })
            }))
            .chain(futures::stream::pending())
            .boxed())
        }
    }

    /// SEP-1699 resumes a broken POST SSE stream via GET + Last-Event-ID
    /// beneath `execute_sse_stream`, so the SEP-2260 origin marker must span
    /// resumes; if reconnection were hoisted above the marker attach point,
    /// replayed associated requests would be wrongly rejected with -32602.
    #[tokio::test]
    async fn resumed_post_stream_requests_keep_outbound_origin() {
        let initial = futures::stream::iter([Ok(Sse {
            event: None,
            data: None,
            id: Some("e1".into()),
            retry: Some(0),
        })])
        .boxed();
        let client = ResumedRequestClient::default();
        let reconnects = client.reconnects.clone();
        let sse_stream =
            StreamableHttpClientWorker::<ResumedRequestClient>::response_sse_to_jsonrpc(
                initial,
                None,
                client,
                Arc::from("http://localhost/mcp"),
                None,
                HashMap::new(),
                DEFAULT_MAX_SSE_EVENT_SIZE,
                Arc::new(ExponentialBackoff {
                    max_times: Some(1),
                    base_duration: Duration::ZERO,
                    max_delay: None,
                }),
            );

        let origin = InboundStreamOrigin::OutboundRequest(RequestId::Number(3));
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        StreamableHttpClientWorker::<ResumedRequestClient>::execute_sse_stream(
            sse_stream,
            tx,
            origin.clone(),
            true,
            CancellationToken::new(),
        )
        .await
        .expect("stream completes");

        assert_eq!(
            reconnects.lock().expect("lock reconnects").as_slice(),
            &[(None, Some("e1".into()))],
            "the request must arrive on the resumed connection"
        );
        let ServerJsonRpcMessage::Request(request) = rx.recv().await.expect("request forwarded")
        else {
            panic!("expected request first");
        };
        assert_eq!(
            request.request.extensions().get::<InboundStreamOrigin>(),
            Some(&origin),
            "origin marker must survive SSE resumption"
        );
        assert!(matches!(
            rx.recv().await.expect("response forwarded"),
            ServerJsonRpcMessage::Response(_)
        ));
    }

    const SESSION_ID: &str = "session-1";

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum StartupStall {
        Nothing,
        InitializePost,
        InitializeBody,
        InitializedPost,
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum DeleteOutcome {
        Succeeds,
        /// HTTP 404, as clients report it.
        NotFound,
        /// HTTP 405, as rmcp's reqwest client reports it.
        NotAllowed,
        /// Any other status, as the codex adapter reports it.
        Status(u16),
        Hangs,
    }

    /// Panics when dropped, so a task aborted while holding it ends in a panic.
    struct PanicOnDrop;

    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            panic!("stream task panicked while it was aborted");
        }
    }

    /// A legacy server that assigns `SESSION_ID` on `initialize`, can stall one
    /// startup step, and records every POSTed method and DELETEd session.
    #[derive(Clone)]
    struct StartupClient {
        stall: StartupStall,
        delete: DeleteOutcome,
        stream_panics_on_abort: bool,
        recovery_stall: Option<StartupStall>,
        posts: Arc<Mutex<Vec<String>>>,
        deletes: Arc<Mutex<Vec<String>>>,
        posted: Arc<tokio::sync::Notify>,
    }

    impl StartupClient {
        fn new(stall: StartupStall, delete: DeleteOutcome) -> Self {
            Self {
                stall,
                delete,
                stream_panics_on_abort: false,
                recovery_stall: None,
                posts: Arc::default(),
                deletes: Arc::default(),
                posted: Arc::default(),
            }
        }

        fn with_stream_panic_on_abort(mut self) -> Self {
            self.stream_panics_on_abort = true;
            self
        }

        fn posts(&self) -> Vec<String> {
            self.posts.lock().expect("lock posts").clone()
        }

        fn deletes(&self) -> Vec<String> {
            self.deletes.lock().expect("lock deletes").clone()
        }
    }

    impl StreamableHttpClient for StartupClient {
        type Error = std::io::Error;

        async fn post_message(
            &self,
            _uri: Arc<str>,
            message: ClientJsonRpcMessage,
            _session_id: Option<Arc<str>>,
            _auth_header: Option<String>,
            _custom_headers: HashMap<HeaderName, HeaderValue>,
        ) -> Result<StreamableHttpPostResponse, StreamableHttpError<Self::Error>> {
            let request = serde_json::to_value(&message).expect("serialize client message");
            let method = request["method"].as_str().unwrap_or_default().to_owned();
            self.posts.lock().expect("lock posts").push(method.clone());
            self.posted.notify_one();
            if let Some(recovery_stall) = self.recovery_stall {
                let initializes = self.posts().iter().filter(|method| method.as_str() == "initialize").count();
                if method == "tools/list" {
                    return Err(StreamableHttpError::SessionExpired);
                }
                if initializes > 1 && method == "initialize" {
                    if recovery_stall == StartupStall::InitializePost {
                        return futures::future::pending().await;
                    }
                    let response = serde_json::from_value(json!({
                        "jsonrpc": "2.0", "id": request["id"],
                        "error": {"code": -32603, "message": "recovery refused"}
                    })).expect("recovery error response");
                    return Ok(StreamableHttpPostResponse::Json(response, Some("unknown-new-session".into())));
                }
            }
            match (method.as_str(), self.stall) {
                ("initialize", StartupStall::InitializePost)
                | ("notifications/initialized", StartupStall::InitializedPost) => {
                    futures::future::pending().await
                }
                ("initialize", StartupStall::InitializeBody) => {
                    Ok(StreamableHttpPostResponse::Sse(
                        futures::stream::pending().boxed(),
                        Some(SESSION_ID.into()),
                    ))
                }
                ("initialize", _) => {
                    let response = serde_json::from_value(json!({
                        "jsonrpc": "2.0",
                        "id": request["id"],
                        "result": {
                            "protocolVersion": "2025-06-18",
                            "capabilities": {},
                            "serverInfo": { "name": "startup-test", "version": "1" },
                        },
                    }))
                    .expect("initialize response");
                    Ok(StreamableHttpPostResponse::Json(
                        response,
                        Some(SESSION_ID.into()),
                    ))
                }
                _ => Ok(StreamableHttpPostResponse::Accepted),
            }
        }

        async fn delete_session(
            &self,
            _uri: Arc<str>,
            session_id: Arc<str>,
            _auth_header: Option<String>,
            _custom_headers: HashMap<HeaderName, HeaderValue>,
        ) -> Result<(), StreamableHttpError<Self::Error>> {
            self.deletes
                .lock()
                .expect("lock deletes")
                .push(session_id.to_string());
            match self.delete {
                DeleteOutcome::Succeeds => Ok(()),
                DeleteOutcome::NotFound => Err(StreamableHttpError::SessionExpired),
                DeleteOutcome::NotAllowed => {
                    Err(StreamableHttpError::ServerDoesNotSupportDeleteSession)
                }
                DeleteOutcome::Status(status) => {
                    Err(StreamableHttpError::UnexpectedServerResponse(
                        format!("DELETE returned HTTP {status}").into(),
                    ))
                }
                DeleteOutcome::Hangs => futures::future::pending().await,
            }
        }

        async fn get_stream(
            &self,
            _uri: Arc<str>,
            _session_id: Option<Arc<str>>,
            _last_event_id: Option<String>,
            _auth_header: Option<String>,
            _custom_headers: HashMap<HeaderName, HeaderValue>,
        ) -> Result<BoxedSseStream, StreamableHttpError<Self::Error>> {
            if self.stream_panics_on_abort {
                // Opening the stream does not observe cancellation, so only
                // the abort in `finish_session` ends this task.
                let _guard = PanicOnDrop;
                futures::future::pending::<()>().await;
            }
            Err(StreamableHttpError::ServerDoesNotSupportSse)
        }
    }

    type StartupTransport = WorkerTransport<StreamableHttpClientWorker<StartupClient>>;

    fn startup_worker(client: &StartupClient) -> StreamableHttpClientWorker<StartupClient> {
        StreamableHttpClientWorker::new(
            client.clone(),
            StreamableHttpClientTransportConfig::with_uri("http://localhost/mcp"),
        )
    }

    fn initialize_request() -> ClientJsonRpcMessage {
        serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": { "name": "startup-test", "version": "1" },
            },
        }))
        .expect("initialize request")
    }

    fn initialized_notification() -> ClientJsonRpcMessage {
        serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
        }))
        .expect("initialized notification")
    }

    /// Paused time turns a close that waits on an unobserved await into a
    /// timeout failure instead of a hang.
    async fn close_within(
        transport: &mut StartupTransport,
        budget: Duration,
    ) -> Result<(), StreamableHttpError<std::io::Error>> {
        tokio::time::timeout(budget, transport.close())
            .await
            .expect("close returns within its budget")
    }

    /// Delivers the initialize response, leaving the worker waiting for
    /// `notifications/initialized`.
    async fn deliver_initialize_response(transport: &mut StartupTransport) {
        transport
            .send(initialize_request())
            .await
            .expect("initialize is posted");
        assert!(
            transport.receive().await.is_some(),
            "initialize response is delivered"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn startup_cancel_without_a_session_returns_promptly_without_delete() {
        for stall in [StartupStall::Nothing, StartupStall::InitializePost] {
            let client = StartupClient::new(stall, DeleteOutcome::Succeeds);
            let mut transport = StartupTransport::spawn(startup_worker(&client));
            let send = (stall == StartupStall::InitializePost)
                .then(|| tokio::spawn(transport.send(initialize_request())));
            if send.is_some() {
                client.posted.notified().await;
            }

            close_within(&mut transport, Duration::from_secs(1))
                .await
                .expect("no session to clean up");

            if let Some(send) = send {
                assert!(send.await.expect("send task").is_err());
            }
            let expected_posts = match stall {
                StartupStall::InitializePost => vec!["initialize".to_owned()],
                _ => Vec::new(),
            };
            assert_eq!(
                (client.posts(), client.deletes()),
                (expected_posts, Vec::new())
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn startup_cancel_after_initialize_response_deletes_session() {
        let client = StartupClient::new(StartupStall::Nothing, DeleteOutcome::Succeeds);
        let mut transport = StartupTransport::spawn(startup_worker(&client));
        deliver_initialize_response(&mut transport).await;

        close_within(&mut transport, Duration::from_secs(1))
            .await
            .expect("session cleanup completes");

        assert_eq!(
            (client.posts(), client.deletes()),
            (vec!["initialize".to_owned()], vec![SESSION_ID.to_owned()])
        );
    }

    #[tokio::test(start_paused = true)]
    async fn startup_cancel_with_a_session_in_flight_deletes_session() {
        for stall in [StartupStall::InitializeBody, StartupStall::InitializedPost] {
            let client = StartupClient::new(stall, DeleteOutcome::Succeeds);
            let mut transport = StartupTransport::spawn(startup_worker(&client));
            transport
                .send(initialize_request())
                .await
                .expect("initialize is posted");
            client.posted.notified().await;
            if stall == StartupStall::InitializedPost {
                assert!(transport.receive().await.is_some());
                let send = transport.send(initialized_notification());
                tokio::spawn(send);
                client.posted.notified().await;
            }

            close_within(&mut transport, Duration::from_secs(1))
                .await
                .expect("session cleanup completes");

            assert_eq!(client.deletes(), vec![SESSION_ID.to_owned()]);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn startup_handler_failure_after_session_deletes_session() {
        let client = StartupClient::new(StartupStall::Nothing, DeleteOutcome::Succeeds);
        let (to_handler_tx, to_handler_rx) = tokio::sync::mpsc::channel(1);
        drop(to_handler_rx);
        let context =
            crate::transport::worker::test_context::<StreamableHttpClientWorker<StartupClient>>(
                vec![initialize_request()],
                to_handler_tx,
                CancellationToken::new(),
                Default::default(),
            );

        let quit_reason =
            tokio::time::timeout(Duration::from_secs(1), startup_worker(&client).run(context))
                .await
                .expect("worker exits");

        assert!(matches!(
            quit_reason,
            Err(WorkerQuitReason::HandlerTerminated)
        ));
        assert_eq!(client.deletes(), vec![SESSION_ID.to_owned()]);
    }

    #[tokio::test(start_paused = true)]
    async fn completed_startup_close_deletes_session() {
        let client = StartupClient::new(StartupStall::Nothing, DeleteOutcome::Succeeds);
        let mut transport = StartupTransport::spawn(startup_worker(&client));
        deliver_initialize_response(&mut transport).await;
        transport
            .send(initialized_notification())
            .await
            .expect("initialized is posted");

        close_within(&mut transport, Duration::from_secs(1))
            .await
            .expect("session cleanup completes");

        assert_eq!(client.deletes(), vec![SESSION_ID.to_owned()]);
    }

    #[tokio::test(start_paused = true)]
    async fn close_reports_unproven_session_delete() {
        for delete in [DeleteOutcome::Status(500), DeleteOutcome::Hangs] {
            let client = StartupClient::new(StartupStall::Nothing, delete);
            let mut transport = StartupTransport::spawn(startup_worker(&client));
            deliver_initialize_response(&mut transport).await;

            // A hanging DELETE ends at the existing cleanup budget, not later.
            let closed = close_within(
                &mut transport,
                SESSION_CLEANUP_TIMEOUT + Duration::from_secs(1),
            )
            .await;

            assert!(
                matches!(
                    &closed,
                    Err(StreamableHttpError::CleanupIncomplete {
                        session_id: Some(session_id),
                        ..
                    }) if &**session_id == SESSION_ID
                ),
                "{closed:?}"
            );
            assert_eq!(client.deletes(), vec![SESSION_ID.to_owned()]);
        }
    }

    /// Completes startup, so the main loop runs with the common stream open,
    /// then closes the transport.
    async fn close_established_session(
        client: &StartupClient,
    ) -> Result<(), StreamableHttpError<std::io::Error>> {
        let mut transport = StartupTransport::spawn(startup_worker(client));
        deliver_initialize_response(&mut transport).await;
        transport
            .send(initialized_notification())
            .await
            .expect("initialized is posted");
        close_within(
            &mut transport,
            SESSION_CLEANUP_TIMEOUT + Duration::from_secs(1),
        )
        .await
    }

    fn incomplete_reason(closed: &Result<(), StreamableHttpError<std::io::Error>>) -> Option<&str> {
        match closed {
            Err(StreamableHttpError::CleanupIncomplete { reason, .. }) => Some(&**reason),
            _ => None,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn not_found_for_the_established_session_is_proven_absent() {
        let client = StartupClient::new(StartupStall::Nothing, DeleteOutcome::NotFound);
        let closed = close_established_session(&client).await;
        assert!(closed.is_ok(), "{closed:?}");
        assert_eq!(client.deletes(), vec![SESSION_ID.to_owned()]);
    }

    #[tokio::test(start_paused = true)]
    async fn not_allowed_delete_proves_only_local_cleanup() {
        // A 405 keeps the existing compatibility: close succeeds because the
        // local transport is released, though the server may keep the session.
        let client = StartupClient::new(StartupStall::Nothing, DeleteOutcome::NotAllowed);
        let closed = close_established_session(&client).await;
        assert!(closed.is_ok(), "{closed:?}");
        assert_eq!(client.deletes(), vec![SESSION_ID.to_owned()]);
    }

    #[tokio::test(start_paused = true)]
    async fn delete_status_never_masks_a_failed_stream_join() {
        for delete in [
            DeleteOutcome::Succeeds,
            DeleteOutcome::NotFound,
            DeleteOutcome::NotAllowed,
        ] {
            let client =
                StartupClient::new(StartupStall::Nothing, delete).with_stream_panic_on_abort();
            let closed = close_established_session(&client).await;
            assert_eq!(
                incomplete_reason(&closed),
                Some("a stream task panicked"),
                "{closed:?}"
            );
            assert_eq!(client.deletes(), vec![SESSION_ID.to_owned()]);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn other_delete_statuses_are_incomplete() {
        for status in [400, 403, 410, 500, 503] {
            let client = StartupClient::new(StartupStall::Nothing, DeleteOutcome::Status(status));
            let closed = close_established_session(&client).await;
            assert_eq!(
                incomplete_reason(&closed).map(str::to_owned),
                Some(format!(
                    "delete session failed: unexpected server response: DELETE returned HTTP {status}"
                ))
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn not_found_for_an_unestablished_session_is_incomplete() {
        // The server never answered `initialize` for this id, so its 404 does
        // not identify a session this client established.
        let client = StartupClient::new(StartupStall::InitializeBody, DeleteOutcome::NotFound);
        let mut transport = StartupTransport::spawn(startup_worker(&client));
        transport
            .send(initialize_request())
            .await
            .expect("initialize is posted");
        let closed = close_within(&mut transport, Duration::from_secs(1)).await;
        assert!(
            incomplete_reason(&closed).is_some_and(|reason| reason.contains("Session expired")),
            "{closed:?}"
        );
        assert_eq!(client.deletes(), vec![SESSION_ID.to_owned()]);
    }

    #[tokio::test(start_paused = true)]
    async fn close_deadline_bounds_session_cleanup() {
        // A latched deadline shorter than the cleanup budget ends cleanup
        // there; a longer one keeps the budget's cap.
        for (close_deadline, expected) in [
            (Duration::from_secs(2), Duration::from_secs(2)),
            (Duration::from_secs(60), SESSION_CLEANUP_TIMEOUT),
        ] {
            let client = StartupClient::new(StartupStall::Nothing, DeleteOutcome::Hangs);
            let mut transport = StartupTransport::spawn(startup_worker(&client));
            deliver_initialize_response(&mut transport).await;
            let started = tokio::time::Instant::now();
            transport.close_deadline().latch(started + close_deadline);

            let closed =
                close_within(&mut transport, close_deadline + Duration::from_secs(1)).await;

            let elapsed = started.elapsed();
            assert!(
                elapsed >= expected && elapsed < expected + Duration::from_millis(10),
                "{elapsed:?}"
            );
            assert_eq!(incomplete_reason(&closed), Some("delete session timed out"));
            assert_eq!(client.deletes(), vec![SESSION_ID.to_owned()]);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn expired_close_deadline_starts_no_delete() {
        let client = StartupClient::new(StartupStall::Nothing, DeleteOutcome::Succeeds);
        let mut transport = StartupTransport::spawn(startup_worker(&client));
        deliver_initialize_response(&mut transport).await;
        transport
            .close_deadline()
            .latch(tokio::time::Instant::now());

        let closed = close_within(&mut transport, Duration::from_secs(1)).await;

        assert_eq!(incomplete_reason(&closed), Some("close deadline expired"));
        assert!(client.deletes().is_empty());
    }

    type StartupQuit = Result<(), WorkerQuitReason<StreamableHttpError<std::io::Error>>>;

    fn established_cleanup(client: &StartupClient) -> SessionCleanupInfo<StartupClient> {
        SessionCleanupInfo {
            client: client.clone(),
            uri: "http://localhost/mcp".into(),
            session_id: SESSION_ID.into(),
            auth_header: None,
            protocol_headers: HashMap::new(),
            established: true,
        }
    }

    fn quit_incomplete_reason(quit: &StartupQuit) -> Option<&str> {
        match quit {
            Err(WorkerQuitReason::Fatal {
                error: StreamableHttpError::CleanupIncomplete { reason, .. },
                ..
            }) => Some(&**reason),
            _ => None,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn failed_or_cancelled_reinitialization_cannot_close_successfully() {
        for (stall, cancel) in [
            (StartupStall::Nothing, false),
            (StartupStall::InitializePost, false),
            (StartupStall::InitializePost, true),
        ] {
            let mut client = StartupClient::new(StartupStall::Nothing, DeleteOutcome::NotFound);
            client.recovery_stall = Some(stall);
            let mut worker = startup_worker(&client);
            worker.config.reinit_on_expired_session = true;
            worker.config.session_recovery_timeout = Duration::from_secs(1);
            let mut transport = StartupTransport::spawn(worker);
            deliver_initialize_response(&mut transport).await;
            transport.send(initialized_notification()).await.expect("initialized");
            let request = serde_json::from_value(json!({
                "jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}
            })).expect("list tools request");
            let mut send = Some(tokio::spawn(transport.send(request)));
            while client.posts().iter().filter(|method| method.as_str() == "initialize").count() < 2 {
                client.posted.notified().await;
            }
            if !cancel {
                assert!(tokio::time::timeout(Duration::from_secs(3), send.take().expect("request handle")).await.expect("recovery completes").expect("request task").is_err());
            }
            let result = close_within(&mut transport, Duration::from_secs(2)).await;
            assert_eq!(incomplete_reason(&result), Some("session recovery may have created an unaccounted session"));
            assert_eq!(client.deletes(), vec![SESSION_ID.to_owned()]);
            if cancel {
                assert!(send.take().expect("request handle").await.expect("cancelled request task").is_err());
            }
        }
    }

    #[tokio::test]
    async fn unaccounted_recovery_stays_incomplete_after_known_session_delete() {
        for delete in [DeleteOutcome::Succeeds, DeleteOutcome::NotFound] {
            let client = StartupClient::new(StartupStall::Nothing, delete);
            let mut streams = tokio::task::JoinSet::new();
            let mut cleanup = established_cleanup(&client);
            cleanup.session_id = "recovered-session".into();
            let quit = StreamableHttpClientWorker::<StartupClient>::finish_session(
                &mut streams,
                Some(cleanup),
                Some("earlier recovery session is unaccounted"),
                CloseDeadline::default(),
                Ok(()),
            )
            .await;
            assert_eq!(quit_incomplete_reason(&quit), Some("earlier recovery session is unaccounted"));
            assert_eq!(client.deletes(), vec!["recovered-session".to_owned()]);
        }
    }

    /// A stream task that never stops: it lives on a runtime nobody drives, so
    /// its abort is never processed and joining it waits forever.
    fn stuck_streams() -> tokio::task::JoinSet<SseTaskResult<std::io::Error>> {
        // Leaked: dropping a runtime inside an async context panics.
        let undriven = Box::leak(Box::new(
            tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("undriven runtime"),
        ));
        let mut streams = tokio::task::JoinSet::new();
        streams.spawn_on(async { (None, Ok(())) }, undriven.handle());
        streams
    }

    /// Runs `finish_session` while latching `tighten_to` (relative to the
    /// start) 100 ms in, and returns its result and how long it took.
    async fn finish_with_tighten(
        client: &StartupClient,
        mut streams: tokio::task::JoinSet<SseTaskResult<std::io::Error>>,
        initial: Duration,
        tighten_to: Duration,
    ) -> (StartupQuit, Duration) {
        let started = tokio::time::Instant::now();
        let close_deadline = CloseDeadline::default();
        close_deadline.latch(started + initial);
        let tighten = {
            let close_deadline = close_deadline.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                close_deadline.latch(started + tighten_to);
            }
        };
        let (quit, ()) = tokio::join!(
            StreamableHttpClientWorker::<StartupClient>::finish_session(
                &mut streams,
                Some(established_cleanup(client)),
                None,
                close_deadline,
                Ok(()),
            ),
            tighten,
        );
        (quit, started.elapsed())
    }

    #[tokio::test(start_paused = true)]
    async fn tightened_deadline_stops_the_stream_join() {
        let client = StartupClient::new(StartupStall::Nothing, DeleteOutcome::Succeeds);
        let (quit, elapsed) = finish_with_tighten(
            &client,
            stuck_streams(),
            Duration::from_secs(30),
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(elapsed, Duration::from_secs(1));
        assert_eq!(
            quit_incomplete_reason(&quit),
            Some("stream tasks did not stop; close deadline expired")
        );
        assert!(client.deletes().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn tightened_deadline_stops_the_delete() {
        let client = StartupClient::new(StartupStall::Nothing, DeleteOutcome::Hangs);
        let (quit, elapsed) = finish_with_tighten(
            &client,
            tokio::task::JoinSet::new(),
            Duration::from_secs(30),
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(elapsed, Duration::from_secs(1));
        assert_eq!(
            quit_incomplete_reason(&quit),
            Some("delete session timed out")
        );
        assert_eq!(client.deletes(), vec![SESSION_ID.to_owned()]);
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_tightened_into_the_past_starts_no_delete() {
        // Tightened at 100 ms to 50 ms, while the stream join is still pending.
        let client = StartupClient::new(StartupStall::Nothing, DeleteOutcome::Succeeds);
        let (quit, elapsed) = finish_with_tighten(
            &client,
            stuck_streams(),
            Duration::from_secs(30),
            Duration::from_millis(50),
        )
        .await;
        assert_eq!(elapsed, Duration::from_millis(100));
        assert_eq!(
            quit_incomplete_reason(&quit),
            Some("stream tasks did not stop; close deadline expired")
        );
        assert!(
            client.deletes().is_empty(),
            "no DELETE request after expiry"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn loosened_deadline_is_ignored() {
        let client = StartupClient::new(StartupStall::Nothing, DeleteOutcome::Hangs);
        let (quit, elapsed) = finish_with_tighten(
            &client,
            tokio::task::JoinSet::new(),
            Duration::from_secs(1),
            Duration::from_secs(30),
        )
        .await;
        assert_eq!(elapsed, Duration::from_secs(1));
        assert_eq!(
            quit_incomplete_reason(&quit),
            Some("delete session timed out")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cleanup_cap_applies_under_a_longer_owner_deadline() {
        let client = StartupClient::new(StartupStall::Nothing, DeleteOutcome::Hangs);
        let (quit, elapsed) = finish_with_tighten(
            &client,
            tokio::task::JoinSet::new(),
            Duration::from_secs(60),
            Duration::from_secs(30),
        )
        .await;
        assert_eq!(elapsed, SESSION_CLEANUP_TIMEOUT);
        assert_eq!(
            quit_incomplete_reason(&quit),
            Some("delete session timed out")
        );
    }

    fn tool(name: &'static str, annotation: serde_json::Value) -> Tool {
        let schema = json!({
            "type": "object",
            "properties": {
                "value": annotation,
            },
        });
        Tool::new(
            name,
            name,
            Arc::new(schema.as_object().expect("object schema").clone()),
        )
    }

    #[test]
    fn cache_tools_removes_invalid_header_annotations() {
        let valid = tool(
            "valid",
            json!({ "type": "string", "x-mcp-header": "Value" }),
        );
        let invalid = tool("invalid", json!({ "type": "string", "x-mcp-header": "" }));
        let mut message = ServerJsonRpcMessage::response(
            ServerResult::ListToolsResult(ListToolsResult::with_all_items(vec![valid, invalid])),
            NumberOrString::Number(1),
        );
        let mut cache = HashMap::new();

        cache_tools_from_response(&mut cache, &mut message, &ProtocolVersion::V_2026_07_28);

        let ServerJsonRpcMessage::Response(response) = &mut message else {
            panic!("expected tools/list response");
        };
        let ServerResult::ListToolsResult(result) = &mut response.result else {
            panic!("expected tools/list result");
        };
        assert_eq!(
            (
                result
                    .tools
                    .iter()
                    .map(|tool| tool.name.as_ref())
                    .collect::<Vec<_>>(),
                cache.keys().map(String::as_str).collect::<Vec<_>>(),
            ),
            (vec!["valid"], vec!["valid"])
        );
    }

    #[test]
    fn cache_tools_preserves_pre_standard_header_results() {
        let invalid = tool("legacy", json!({ "type": "string", "x-mcp-header": "" }));
        let mut message = ServerJsonRpcMessage::response(
            ServerResult::ListToolsResult(ListToolsResult::with_all_items(vec![invalid])),
            NumberOrString::Number(1),
        );
        let mut cache = HashMap::new();

        cache_tools_from_response(&mut cache, &mut message, &ProtocolVersion::V_2025_11_25);

        let ServerJsonRpcMessage::Response(response) = message else {
            panic!("expected tools/list response");
        };
        let ServerResult::ListToolsResult(result) = response.result else {
            panic!("expected tools/list result");
        };
        assert_eq!(
            result
                .tools
                .iter()
                .map(|tool| tool.name.as_ref())
                .collect::<Vec<_>>(),
            vec!["legacy"]
        );
    }

    #[cfg(feature = "transport-streamable-http-client-reqwest")]
    #[test]
    fn clear_stream_response_pending_accepts_stringified_numeric_id() {
        let mut pending = HashSet::from([NumberOrString::Number(1)]);
        let response = ServerJsonRpcMessage::response(
            ServerResult::ListToolsResult(ListToolsResult::default()),
            NumberOrString::String("1".into()),
        );

        let matched_id =
            StreamableHttpClientWorker::<reqwest::Client>::clear_stream_response_pending(
                &mut pending,
                &response,
            );

        assert_eq!(matched_id, Some(NumberOrString::Number(1)));
        assert!(pending.is_empty());
    }

    #[cfg(feature = "transport-streamable-http-client-reqwest")]
    #[test]
    fn clear_stream_response_pending_prefers_exact_string_id() {
        let string_id = NumberOrString::String("1".into());
        let mut pending = HashSet::from([NumberOrString::Number(1), string_id.clone()]);
        let response = ServerJsonRpcMessage::response(
            ServerResult::ListToolsResult(ListToolsResult::default()),
            string_id.clone(),
        );

        let matched_id =
            StreamableHttpClientWorker::<reqwest::Client>::clear_stream_response_pending(
                &mut pending,
                &response,
            );

        assert_eq!(matched_id, Some(string_id));
        assert_eq!(pending, HashSet::from([NumberOrString::Number(1)]));
    }
}
