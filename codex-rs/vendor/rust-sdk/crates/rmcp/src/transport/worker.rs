use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{
        Arc, Mutex, PoisonError, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Level};

use super::{IntoTransport, Transport};
use crate::{
    model::{CancelledNotification, JsonRpcMessage, RequestId},
    service::{RxJsonRpcMessage, ServiceRole, TxJsonRpcMessage},
};

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WorkerQuitReason<E> {
    #[error("Join error {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("Transport fatal {error}, when {context}")]
    Fatal {
        error: E,
        context: Cow<'static, str>,
    },
    #[error("Transport cancelled")]
    Cancelled,
    #[error("Transport closed")]
    TransportClosed,
    #[error("Handler terminated")]
    HandlerTerminated,
    #[error("Worker idle timeout after {}ms", _0.as_millis())]
    IdleTimeout(Duration),
}

impl<E: std::error::Error + Send + 'static> WorkerQuitReason<E> {
    pub fn fatal(error: E, context: impl Into<Cow<'static, str>>) -> Self {
        Self::Fatal {
            error,
            context: context.into(),
        }
    }
    pub fn fatal_context(context: impl Into<Cow<'static, str>>) -> impl FnOnce(E) -> Self {
        |e| Self::Fatal {
            error: e,
            context: context.into(),
        }
    }
}

pub trait Worker: Sized + Send + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    type Role: ServiceRole;
    fn err_closed() -> Self::Error;
    fn err_join(e: tokio::task::JoinError) -> Self::Error;
    fn run(
        self,
        context: WorkerContext<Self>,
    ) -> impl Future<Output = Result<(), WorkerQuitReason<Self::Error>>> + Send;
    fn config(&self) -> WorkerConfig {
        WorkerConfig::default()
    }
    /// Return true to send this message through the separate control queue.
    ///
    /// Workers that opt in must read [`WorkerContext::control_from_handler_rx`]
    /// and preserve any required ordering with ordinary messages.
    fn is_control_message(_message: &TxJsonRpcMessage<Self::Role>) -> bool {
        false
    }
    /// Return true to register outgoing requests for cancellation before they enter a queue.
    ///
    /// Workers that opt in must honor [`WorkerSendRequest::cancellation_token`].
    fn supports_request_cancellation() -> bool {
        false
    }
    /// Map a joined worker's quit reason to the result of [`WorkerTransport::close`].
    ///
    /// The default reports success for every reason: once joined, the worker
    /// owns no further work. Override it when the worker's exit performs
    /// cleanup that callers must not mistake for success if it did not complete.
    fn close_result(
        _quit_reason: Result<(), WorkerQuitReason<Self::Error>>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

type RequestCancellations = Arc<Mutex<HashMap<RequestId, Weak<RequestCancellationRegistration>>>>;

/// Keeps a request's cancellation token registered for a chosen lifetime.
pub(crate) struct RequestCancellationRegistration {
    id: RequestId,
    lifetime: CancellationToken,
    cancellation: CancellationToken,
    pending: RequestCancellations,
}

impl RequestCancellationRegistration {
    fn new(id: RequestId, token: CancellationToken, pending: RequestCancellations) -> Arc<Self> {
        let registration = Arc::new(Self {
            id: id.clone(),
            cancellation: token.child_token(),
            lifetime: token,
            pending,
        });
        registration
            .pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(id, Arc::downgrade(&registration));
        registration
    }

    /// Return the token kept alive by this registration.
    pub(crate) fn token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    /// Return the token cancelled when the request lifetime ends.
    pub(crate) fn lifetime_token(&self) -> CancellationToken {
        self.lifetime.clone()
    }
}

impl Drop for RequestCancellationRegistration {
    fn drop(&mut self) {
        self.lifetime.cancel();
        let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
        if pending
            .get(&self.id)
            .is_some_and(|current| std::ptr::eq(current.as_ptr(), self))
        {
            pending.remove(&self.id);
        }
    }
}

#[non_exhaustive]
pub struct WorkerSendRequest<W: Worker> {
    pub message: TxJsonRpcMessage<W::Role>,
    pub responder: tokio::sync::oneshot::Sender<Result<(), W::Error>>,
    cancellation: Option<Arc<RequestCancellationRegistration>>,
    control_generation: u64,
}

impl<W: Worker> WorkerSendRequest<W> {
    /// Return the token registered before this request entered the send queue.
    ///
    /// This is present only for requests sent to a worker that enables
    /// [`Worker::supports_request_cancellation`]. It is not sent over the wire.
    /// Keep this request alive while its work is active. Cloning the token does
    /// not keep its cancellation registration alive.
    pub fn cancellation_token(&self) -> Option<CancellationToken> {
        self.cancellation
            .as_deref()
            .map(RequestCancellationRegistration::token)
    }

    /// Keep the same cancellation registration alive after the send completes.
    #[cfg(feature = "transport-streamable-http-client")]
    pub(crate) fn cancellation_registration(&self) -> Option<Arc<RequestCancellationRegistration>> {
        self.cancellation.clone()
    }

    /// Return the local generation captured when [`Transport::send`] created its future.
    ///
    /// This happens before polling or queue admission. The value is not sent over
    /// the wire; the worker decides whether a message from an older generation is valid.
    /// It identifies the outbound send, not the session that started an inbound handler.
    pub fn control_generation(&self) -> u64 {
        self.control_generation
    }
}

/// The latest instant by which a closing worker must finish its exit cleanup.
///
/// An owner latches it, before or while the transport closes, and the worker
/// follows it for as long as its exit cleanup runs. Latching keeps the earliest
/// instant, so a later, longer deadline never extends one that is already
/// latched, and a tighter one wakes every [`subscribe`](Self::subscribe)r.
#[derive(Clone, Debug)]
pub struct CloseDeadline(Arc<tokio::sync::watch::Sender<Option<tokio::time::Instant>>>);

impl Default for CloseDeadline {
    fn default() -> Self {
        Self(Arc::new(tokio::sync::watch::channel(None).0))
    }
}

impl CloseDeadline {
    /// Latch `deadline`, keeping any earlier deadline already latched.
    ///
    /// Only a tighter deadline notifies subscribers. The watch lock is held
    /// just for this comparison.
    pub fn latch(&self, deadline: tokio::time::Instant) {
        self.0.send_if_modified(|latched| {
            if latched.is_some_and(|latched| latched <= deadline) {
                return false;
            }
            *latched = Some(deadline);
            true
        });
    }

    /// Return the latched deadline, if any.
    pub fn get(&self) -> Option<tokio::time::Instant> {
        *self.0.borrow()
    }

    /// Watch the latched deadline. The receiver is notified when it tightens.
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<Option<tokio::time::Instant>> {
        self.0.subscribe()
    }
}

pub struct WorkerTransport<W: Worker> {
    rx: tokio::sync::mpsc::Receiver<RxJsonRpcMessage<W::Role>>,
    send_service: tokio::sync::mpsc::Sender<WorkerSendRequest<W>>,
    control_send_service: tokio::sync::mpsc::Sender<WorkerSendRequest<W>>,
    request_cancellations: RequestCancellations,
    control_generation: Arc<AtomicU64>,
    join_handle: Option<tokio::task::JoinHandle<Result<(), WorkerQuitReason<W::Error>>>>,
    /// Set once a completed join closed with an error, so later closes fail too.
    close_failed: bool,
    _drop_guard: tokio_util::sync::DropGuard,
    ct: CancellationToken,
    close_deadline: CloseDeadline,
}

#[non_exhaustive]
pub struct WorkerConfig {
    pub name: Option<String>,
    pub channel_buffer_capacity: usize,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            name: None,
            channel_buffer_capacity: 16,
        }
    }
}
#[non_exhaustive]
pub enum WorkerAdapter {}

impl<W: Worker> IntoTransport<W::Role, W::Error, WorkerAdapter> for W {
    fn into_transport(self) -> impl Transport<W::Role, Error = W::Error> + 'static {
        WorkerTransport::spawn(self)
    }
}

impl<W: Worker> WorkerTransport<W> {
    pub fn cancel_token(&self) -> CancellationToken {
        self.ct.clone()
    }
    pub fn spawn(worker: W) -> Self {
        Self::spawn_with_ct(worker, CancellationToken::new())
    }
    /// Return the deadline handle the worker reads when its exit cleanup starts.
    pub fn close_deadline(&self) -> CloseDeadline {
        self.close_deadline.clone()
    }
    pub fn spawn_with_ct(worker: W, transport_task_ct: CancellationToken) -> Self {
        Self::spawn_with_close_deadline(worker, transport_task_ct, CloseDeadline::default())
    }
    /// Spawn with a deadline handle the owner may latch before closing.
    pub fn spawn_with_close_deadline(
        worker: W,
        transport_task_ct: CancellationToken,
        close_deadline: CloseDeadline,
    ) -> Self {
        let config = worker.config();
        let worker_name = config.name;
        let (to_transport_tx, from_handler_rx) =
            tokio::sync::mpsc::channel::<WorkerSendRequest<W>>(config.channel_buffer_capacity);
        let (control_to_transport_tx, control_from_handler_rx) =
            tokio::sync::mpsc::channel::<WorkerSendRequest<W>>(config.channel_buffer_capacity);
        let (to_handler_tx, from_transport_rx) =
            tokio::sync::mpsc::channel::<RxJsonRpcMessage<W::Role>>(config.channel_buffer_capacity);
        let request_cancellations = RequestCancellations::default();
        let control_generation = Arc::new(AtomicU64::new(0));
        let context = WorkerContext {
            to_handler_tx,
            from_handler_rx,
            control_from_handler_rx,
            control_generation: control_generation.clone(),
            cancellation_token: transport_task_ct.clone(),
            close_deadline: close_deadline.clone(),
        };

        let join_handle = tokio::spawn(async move {
            worker
                .run(context)
                .instrument(tracing::span!(
                    Level::TRACE,
                    "transport_worker",
                    name = worker_name,
                ))
                .await
                .inspect_err(|e| match e {
                    WorkerQuitReason::Cancelled
                    | WorkerQuitReason::TransportClosed
                    | WorkerQuitReason::HandlerTerminated
                    | WorkerQuitReason::IdleTimeout(_) => {
                        tracing::debug!("worker quit with reason: {:?}", e);
                    }
                    WorkerQuitReason::Join(e) => {
                        tracing::error!("worker quit with join error: {:?}", e);
                    }
                    WorkerQuitReason::Fatal { error, context } => {
                        tracing::error!("worker quit with fatal: {error}, when {context}");
                    }
                })
                .inspect(|_| {
                    tracing::debug!("worker quit");
                })
        });
        Self {
            rx: from_transport_rx,
            send_service: to_transport_tx,
            control_send_service: control_to_transport_tx,
            request_cancellations,
            control_generation,
            join_handle: Some(join_handle),
            close_failed: false,
            ct: transport_task_ct.clone(),
            _drop_guard: transport_task_ct.drop_guard(),
            close_deadline,
        }
    }

    fn cancel_request_from_notification(
        &self,
        notification: &<W::Role as ServiceRole>::Not,
    ) -> Option<Arc<RequestCancellationRegistration>> {
        let cancelled: CancelledNotification = notification.clone().try_into().ok()?;
        let id = cancelled.params.request_id.as_ref()?;
        let target = {
            let pending = self
                .request_cancellations
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            pending.get(id).and_then(Weak::upgrade)
        }?;
        // Signal cancellation even if the control queue is full.
        target.token().cancel();
        Some(target)
    }
}

#[non_exhaustive]
pub struct SendRequest<W: Worker> {
    pub message: TxJsonRpcMessage<W::Role>,
    pub responder: tokio::sync::oneshot::Sender<RxJsonRpcMessage<W::Role>>,
}

#[non_exhaustive]
pub struct WorkerContext<W: Worker> {
    pub to_handler_tx: tokio::sync::mpsc::Sender<RxJsonRpcMessage<W::Role>>,
    pub from_handler_rx: tokio::sync::mpsc::Receiver<WorkerSendRequest<W>>,
    /// Messages selected by [`Worker::is_control_message`].
    pub control_from_handler_rx: tokio::sync::mpsc::Receiver<WorkerSendRequest<W>>,
    pub cancellation_token: CancellationToken,
    control_generation: Arc<AtomicU64>,
    close_deadline: CloseDeadline,
}

impl<W: Worker> WorkerContext<W> {
    /// Return the deadline the transport's owner latched for closing, if any.
    ///
    /// Exit cleanup must not run past it.
    pub fn close_deadline(&self) -> Option<tokio::time::Instant> {
        self.close_deadline.get()
    }

    /// Return the owner's deadline handle, to follow it as it tightens.
    pub fn close_deadline_handle(&self) -> CloseDeadline {
        self.close_deadline.clone()
    }

    /// Return the local generation that newly created sends will capture.
    ///
    /// Workers may use this value to check messages from an earlier connection
    /// or session. The generic transport does not check it automatically.
    pub fn control_generation(&self) -> u64 {
        self.control_generation.load(Ordering::SeqCst)
    }

    /// Advance the local generation, wrapping at [`u64::MAX`], and return its new value.
    ///
    /// Only subsequent calls to [`Transport::send`] capture the new value.
    /// Advancing does not drain queues, cancel work, or reject older messages;
    /// the worker is responsible for those actions.
    pub fn advance_control_generation(&self) -> u64 {
        self.control_generation
            .fetch_add(1, Ordering::SeqCst)
            .wrapping_add(1)
    }

    /// Deliver a message to the handler.
    ///
    /// Returns [`WorkerQuitReason::Cancelled`] if the transport is cancelled
    /// while the handler's queue is full. [`WorkerTransport::close`] cancels
    /// and then joins the worker while the transport still owns the receiver,
    /// so a send that ignored cancellation could keep `close` waiting forever.
    pub async fn send_to_handler(
        &mut self,
        item: RxJsonRpcMessage<W::Role>,
    ) -> Result<(), WorkerQuitReason<W::Error>> {
        tokio::select! {
            biased;
            result = self.to_handler_tx.send(item) => {
                result.map_err(|_| WorkerQuitReason::HandlerTerminated)
            }
            _ = self.cancellation_token.cancelled() => Err(WorkerQuitReason::Cancelled),
        }
    }

    /// Receive the next message from the handler.
    ///
    /// Returns [`WorkerQuitReason::Cancelled`] once the transport is cancelled.
    /// [`WorkerTransport::close`] cancels and then joins the worker while the
    /// transport still owns the sender, so a receive that ignored cancellation
    /// could keep `close` waiting forever.
    pub async fn recv_from_handler(
        &mut self,
    ) -> Result<WorkerSendRequest<W>, WorkerQuitReason<W::Error>> {
        tokio::select! {
            biased;
            _ = self.cancellation_token.cancelled() => Err(WorkerQuitReason::Cancelled),
            request = self.from_handler_rx.recv() => {
                request.ok_or(WorkerQuitReason::HandlerTerminated)
            }
        }
    }
}

/// Build a context whose queued handler requests are `requests`, followed by
/// the handler terminating, and whose handler-bound messages go to `to_handler_tx`.
#[cfg(all(test, feature = "transport-streamable-http-client"))]
pub(crate) fn test_context<W: Worker>(
    requests: Vec<TxJsonRpcMessage<W::Role>>,
    to_handler_tx: tokio::sync::mpsc::Sender<RxJsonRpcMessage<W::Role>>,
    cancellation_token: CancellationToken,
    close_deadline: CloseDeadline,
) -> WorkerContext<W> {
    let (request_tx, from_handler_rx) = tokio::sync::mpsc::channel(requests.len().max(1));
    for message in requests {
        let (responder, _) = tokio::sync::oneshot::channel();
        request_tx
            .try_send(WorkerSendRequest {
                message,
                responder,
                cancellation: None,
                control_generation: 0,
            })
            .unwrap_or_else(|_| panic!("the request queue holds every request"));
    }
    let (_, control_from_handler_rx) = tokio::sync::mpsc::channel(1);
    WorkerContext {
        to_handler_tx,
        from_handler_rx,
        control_from_handler_rx,
        cancellation_token,
        control_generation: Arc::new(AtomicU64::new(0)),
        close_deadline,
    }
}

impl<W: Worker> Transport<W::Role> for WorkerTransport<W> {
    type Error = W::Error;

    fn send(
        &mut self,
        item: TxJsonRpcMessage<W::Role>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        let control_generation = self.control_generation.load(Ordering::SeqCst);
        let mut cancellation_target = None;
        let registration = if W::supports_request_cancellation() {
            match &item {
                JsonRpcMessage::Request(request) => Some(RequestCancellationRegistration::new(
                    request.id.clone(),
                    self.ct.child_token(),
                    self.request_cancellations.clone(),
                )),
                JsonRpcMessage::Notification(notification) => {
                    cancellation_target =
                        self.cancel_request_from_notification(&notification.notification);
                    None
                }
                _ => None,
            }
        } else {
            None
        };
        let tx = if W::is_control_message(&item) {
            self.control_send_service.clone()
        } else {
            self.send_service.clone()
        };
        let cancellation_guard = registration
            .as_ref()
            .map(|registration| registration.lifetime_token().drop_guard());
        let target_guard = cancellation_target
            .as_ref()
            .map(|target| target.lifetime_token().drop_guard());
        let (responder, receiver) = tokio::sync::oneshot::channel();
        let request = WorkerSendRequest {
            message: item,
            responder,
            cancellation: registration,
            control_generation,
        };
        async move {
            // Keep the stream alive until its cancellation is handled or abandoned.
            let _cancellation_target = cancellation_target;
            let _target_guard = target_guard;
            tx.send(request).await.map_err(|_| W::err_closed())?;
            receiver.await.map_err(|_| W::err_closed())??;
            if let Some(guard) = cancellation_guard {
                let _ = guard.disarm();
            }
            Ok(())
        }
    }
    async fn receive(&mut self) -> Option<RxJsonRpcMessage<W::Role>> {
        self.rx.recv().await
    }
    async fn close(&mut self) -> Result<(), Self::Error> {
        // Keep the handle until the join completes. A close cancelled mid-join
        // leaves the worker running, so a retried close must await the same
        // join rather than report a still-running worker as closed. Once
        // joined, a failed close stays failed for every later close.
        let Some(handle) = self.join_handle.as_mut() else {
            return if self.close_failed {
                Err(W::err_closed())
            } else {
                Ok(())
            };
        };
        self.ct.cancel();
        let joined = handle.await;
        self.join_handle = None;
        let result = joined.map_err(W::err_join).and_then(W::close_result);
        self.close_failed = result.is_err();
        result
    }
}

#[cfg(all(test, feature = "client"))]
mod tests {
    use std::io;

    use super::*;
    use crate::{
        model::{ClientJsonRpcMessage, ServerJsonRpcMessage},
        service::RoleClient,
    };

    struct TestWorker(tokio::sync::oneshot::Sender<WorkerContext<Self>>);

    impl Worker for TestWorker {
        type Error = io::Error;
        type Role = RoleClient;

        fn err_closed() -> Self::Error {
            io::Error::other("worker closed")
        }

        fn err_join(error: tokio::task::JoinError) -> Self::Error {
            io::Error::other(error)
        }

        async fn run(
            self,
            context: WorkerContext<Self>,
        ) -> Result<(), WorkerQuitReason<Self::Error>> {
            let cancellation = context.cancellation_token.clone();
            self.0
                .send(context)
                .map_err(|_| WorkerQuitReason::HandlerTerminated)?;
            cancellation.cancelled().await;
            Ok(())
        }

        fn is_control_message(message: &ClientJsonRpcMessage) -> bool {
            matches!(message, JsonRpcMessage::Notification(_))
        }

        fn supports_request_cancellation() -> bool {
            true
        }
    }

    fn cancellation_message(id: RequestId) -> ClientJsonRpcMessage {
        serde_json::from_value(serde_json::json!({
            "jsonrpc": "2.0",
            "method": "notifications/cancelled",
            "params": { "requestId": id },
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn cancellation_matches_request_id_exactly() {
        let (context_tx, context_rx) = tokio::sync::oneshot::channel();
        let mut transport = WorkerTransport::spawn(TestWorker(context_tx));
        let _context = context_rx.await.unwrap();
        let registration = RequestCancellationRegistration::new(
            RequestId::Number(7),
            CancellationToken::new(),
            transport.request_cancellations.clone(),
        );

        drop(transport.send(cancellation_message(RequestId::String("7".into()))));
        assert!(!registration.token().is_cancelled());

        drop(transport.send(cancellation_message(RequestId::Number(7))));
        assert!(registration.token().is_cancelled());
        transport.close().await.unwrap();
    }

    #[tokio::test]
    async fn abandoned_cancellation_send_ends_request_lifetime() {
        let (context_tx, context_rx) = tokio::sync::oneshot::channel();
        let mut transport = WorkerTransport::spawn(TestWorker(context_tx));
        let mut context = context_rx.await.unwrap();

        for admitted in [false, true] {
            let id = RequestId::Number(7);
            let registration = RequestCancellationRegistration::new(
                id.clone(),
                CancellationToken::new(),
                transport.request_cancellations.clone(),
            );
            let lifetime = registration.lifetime_token();
            let weak = Arc::downgrade(&registration);
            let mut send = Box::pin(transport.send(cancellation_message(id)));
            assert!(registration.token().is_cancelled());
            assert!(!lifetime.is_cancelled());

            let queued = if admitted {
                assert!(futures::poll!(send.as_mut()).is_pending());
                Some(context.control_from_handler_rx.recv().await.unwrap())
            } else {
                None
            };
            drop(send);

            // An open stream can still own the registration after cancellation is abandoned.
            assert!(lifetime.is_cancelled());
            assert!(weak.upgrade().is_some());
            drop(registration);
            assert!(weak.upgrade().is_none());
            assert!(queued.is_none_or(|request| request.responder.is_closed()));
            assert!(transport.request_cancellations.lock().unwrap().is_empty());
        }
        transport.close().await.unwrap();
    }

    /// Waits on one of the handler channels until it fails, then reports
    /// whether it failed because the transport was cancelled.
    struct HandlerChannelWorker {
        fills_handler_queue: bool,
        cancelled: tokio::sync::oneshot::Sender<bool>,
    }

    impl Worker for HandlerChannelWorker {
        type Error = io::Error;
        type Role = RoleClient;

        fn err_closed() -> Self::Error {
            io::Error::other("worker closed")
        }

        fn err_join(error: tokio::task::JoinError) -> Self::Error {
            io::Error::other(error)
        }

        async fn run(
            self,
            mut context: WorkerContext<Self>,
        ) -> Result<(), WorkerQuitReason<Self::Error>> {
            let quit_reason = if self.fills_handler_queue {
                loop {
                    let notification: ServerJsonRpcMessage =
                        serde_json::from_value(serde_json::json!({
                            "jsonrpc": "2.0",
                            "method": "notifications/tools/list_changed",
                        }))
                        .unwrap();
                    if let Err(quit_reason) = context.send_to_handler(notification).await {
                        break quit_reason;
                    }
                }
            } else {
                match context.recv_from_handler().await {
                    Ok(_) => return Ok(()),
                    Err(quit_reason) => quit_reason,
                }
            };
            let _ = self
                .cancelled
                .send(matches!(quit_reason, WorkerQuitReason::Cancelled));
            Err(quit_reason)
        }
    }

    #[tokio::test(start_paused = true)]
    async fn close_joins_a_worker_waiting_on_a_handler_channel() {
        // The handler side of both channels stays open inside the transport
        // being closed, so only cancellation can end these waits. Paused time
        // turns a missed cancellation into a timeout instead of a hang.
        for fills_handler_queue in [false, true] {
            let (cancelled_tx, cancelled_rx) = tokio::sync::oneshot::channel();
            let mut transport = WorkerTransport::spawn(HandlerChannelWorker {
                fills_handler_queue,
                cancelled: cancelled_tx,
            });
            tokio::time::timeout(Duration::from_secs(1), transport.close())
                .await
                .expect("close joins the worker")
                .unwrap();
            assert!(cancelled_rx.await.unwrap(), "{fills_handler_queue}");
        }
    }

    /// Keeps running after cancellation until released, like exit cleanup
    /// that outlives the caller's close budget.
    struct SlowExitWorker {
        release: tokio::sync::oneshot::Receiver<()>,
        exited: tokio::sync::oneshot::Sender<()>,
    }

    impl Worker for SlowExitWorker {
        type Error = io::Error;
        type Role = RoleClient;

        fn err_closed() -> Self::Error {
            io::Error::other("worker closed")
        }

        fn err_join(error: tokio::task::JoinError) -> Self::Error {
            io::Error::other(error)
        }

        async fn run(
            self,
            context: WorkerContext<Self>,
        ) -> Result<(), WorkerQuitReason<Self::Error>> {
            context.cancellation_token.cancelled().await;
            let _ = self.release.await;
            let _ = self.exited.send(());
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn retried_close_awaits_the_worker_a_cancelled_close_left_running() {
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let (exited_tx, mut exited_rx) = tokio::sync::oneshot::channel();
        let mut transport = WorkerTransport::spawn(SlowExitWorker {
            release: release_rx,
            exited: exited_tx,
        });

        // Cancel the first close while it is joining.
        tokio::time::timeout(Duration::from_secs(1), transport.close())
            .await
            .expect_err("the worker is still running");
        assert!(transport.join_handle.is_some(), "join ownership is kept");

        // A retried close must not report the still-running worker closed.
        tokio::time::timeout(Duration::from_secs(1), transport.close())
            .await
            .expect_err("a retried close keeps awaiting the same join");
        assert!(exited_rx.try_recv().is_err(), "the worker has not exited");

        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), transport.close())
            .await
            .expect("close joins the released worker")
            .unwrap();
        assert!(transport.join_handle.is_none());
        exited_rx
            .try_recv()
            .expect("the worker exited before close returned");
        // A clean close stays clean.
        transport.close().await.unwrap();
    }

    /// Exits on cancellation with a quit reason whose cleanup is unproven.
    struct FailedCloseWorker;

    impl Worker for FailedCloseWorker {
        type Error = io::Error;
        type Role = RoleClient;

        fn err_closed() -> Self::Error {
            io::Error::other("worker closed")
        }

        fn err_join(error: tokio::task::JoinError) -> Self::Error {
            io::Error::other(error)
        }

        async fn run(
            self,
            context: WorkerContext<Self>,
        ) -> Result<(), WorkerQuitReason<Self::Error>> {
            context.cancellation_token.cancelled().await;
            Err(WorkerQuitReason::fatal(
                io::Error::other("cleanup incomplete"),
                "closing",
            ))
        }

        fn close_result(
            quit_reason: Result<(), WorkerQuitReason<Self::Error>>,
        ) -> Result<(), Self::Error> {
            match quit_reason {
                Err(WorkerQuitReason::Fatal { error, .. }) => Err(error),
                _ => Ok(()),
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn failed_close_stays_failed_on_every_later_close() {
        let mut transport = WorkerTransport::spawn(FailedCloseWorker);
        let error = tokio::time::timeout(Duration::from_secs(1), transport.close())
            .await
            .expect("close joins the worker")
            .expect_err("unproven cleanup fails the close");
        assert_eq!(error.to_string(), "cleanup incomplete");
        assert!(transport.join_handle.is_none());
        for _ in 0..2 {
            transport
                .close()
                .await
                .expect_err("a later close never upgrades a failed close");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn close_deadline_keeps_the_earliest_latch() {
        let now = tokio::time::Instant::now();
        let deadline = CloseDeadline::default();
        assert_eq!(deadline.get(), None);
        deadline.latch(now + Duration::from_secs(10));
        deadline.clone().latch(now + Duration::from_secs(20));
        assert_eq!(deadline.get(), Some(now + Duration::from_secs(10)));
        deadline.latch(now + Duration::from_secs(5));
        assert_eq!(deadline.get(), Some(now + Duration::from_secs(5)));
    }

    #[tokio::test(start_paused = true)]
    async fn close_deadline_wakes_subscribers_only_when_it_tightens() {
        let now = tokio::time::Instant::now();
        let deadline = CloseDeadline::default();
        let mut changes = deadline.subscribe();
        deadline.latch(now + Duration::from_secs(10));
        assert!(changes.has_changed().unwrap());
        assert_eq!(
            *changes.borrow_and_update(),
            Some(now + Duration::from_secs(10))
        );
        deadline.latch(now + Duration::from_secs(20));
        assert!(!changes.has_changed().unwrap(), "loosening must not wake");
        deadline.latch(now + Duration::from_secs(1));
        assert!(changes.has_changed().unwrap());
        assert_eq!(
            *changes.borrow_and_update(),
            Some(now + Duration::from_secs(1))
        );
    }

    #[test]
    fn dropping_an_old_registration_preserves_a_reused_id() {
        let pending = RequestCancellations::default();
        let id = RequestId::Number(7);
        let old = RequestCancellationRegistration::new(
            id.clone(),
            CancellationToken::new(),
            pending.clone(),
        );
        let current = RequestCancellationRegistration::new(
            id.clone(),
            CancellationToken::new(),
            pending.clone(),
        );
        drop(old);
        let registered = pending.lock().unwrap().get(&id).cloned().unwrap();
        assert!(Weak::ptr_eq(&registered, &Arc::downgrade(&current)));
        drop(current);
        assert!(pending.lock().unwrap().is_empty());
    }
}
