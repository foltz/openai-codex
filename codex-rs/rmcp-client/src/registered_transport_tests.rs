use super::DeferredWorkerTransport;
use crate::retirement::AcknowledgedTransport;
use crate::{
    InProcessTransportFactory, PhysicalRetirementOutcome, RmcpClient, RmcpClientRetirement,
};
use futures::FutureExt;
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use rmcp::model::ClientJsonRpcMessage;
use rmcp::service::RoleClient;
use rmcp::transport::worker::{Worker, WorkerContext, WorkerQuitReason};
use rmcp::transport::{Transport, WorkerTransport};
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader, DuplexStream};
use tokio::sync::Semaphore;
use tokio::time::Instant;

struct PausedFactory {
    gate: Arc<Semaphore>,
    opened: Arc<AtomicUsize>,
    stream: Arc<Mutex<Option<DuplexStream>>>,
}

#[tokio::test(start_paused = true)]
async fn retirement_cancels_retained_handshake_and_observes_transport_close() {
    let (stream, server) = tokio::io::duplex(8192);
    let mut server = BufReader::new(server);
    let factory = Arc::new(PausedFactory {
        gate: Arc::new(Semaphore::new(1)),
        opened: Arc::new(AtomicUsize::new(0)),
        stream: Arc::new(Mutex::new(Some(stream))),
    });
    let registry = RmcpClientRetirement::default();
    let client = RmcpClient::new_in_process_client_in_retirement(factory, registry.clone())
        .await
        .expect("construct client");
    let mut initialize = Box::pin(client.initialize(
        rmcp::model::ClientInfo::default(),
        Some(Duration::from_secs(120)),
        Box::new(|_, _| Box::pin(async { panic!("unexpected elicitation") })),
    ));
    let mut request = String::new();
    tokio::select! {
        _ = initialize.as_mut() => panic!("server has not answered initialize"),
        result = server.read_line(&mut request) => { result.expect("read initialize"); }
    }
    let request: serde_json::Value = serde_json::from_str(&request).expect("initialize JSON");
    assert_eq!(request["method"], "initialize");
    drop(initialize);

    // The observation bound is deliberately shorter than retirement's deadline
    // and the handshake timeout. No server reply is needed to cancel the phase.
    let report = tokio::time::timeout(
        Duration::from_secs(1),
        registry.shutdown_until(Instant::now() + Duration::from_secs(5)),
    )
    .await
    .expect("retirement must not wait for the stalled handshake");
    assert_eq!(
        report.attempts,
        vec![(0, PhysicalRetirementOutcome::Complete)]
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), server.read(&mut [0]))
            .await
            .expect("transport closes promptly")
            .expect("read closed transport"),
        0
    );
    assert_eq!(registry.shutdown_until(Instant::now()).await, report);
}

impl InProcessTransportFactory for PausedFactory {
    fn open(&self) -> BoxFuture<'static, io::Result<DuplexStream>> {
        self.opened.fetch_add(1, Ordering::SeqCst);
        let gate = Arc::clone(&self.gate);
        let stream = Arc::clone(&self.stream);
        async move {
            gate.acquire().await.expect("test gate open").forget();
            stream
                .lock()
                .expect("stream lock")
                .take()
                .ok_or_else(|| io::Error::other("stream already taken"))
        }
        .boxed()
    }
}

#[tokio::test]
async fn cancelled_constructor_remains_owned_until_pending_transport_is_closed() {
    let (client, mut server) = tokio::io::duplex(64);
    let gate = Arc::new(Semaphore::new(0));
    let opened = Arc::new(AtomicUsize::new(0));
    let factory = Arc::new(PausedFactory {
        gate: Arc::clone(&gate),
        opened: Arc::clone(&opened),
        stream: Arc::new(Mutex::new(Some(client))),
    });
    let registry = RmcpClientRetirement::default();
    let mut constructor = Box::pin(RmcpClient::new_in_process_client_in_retirement(
        factory,
        registry.clone(),
    ));
    assert!(futures::poll!(constructor.as_mut()).is_pending());
    assert_eq!(opened.load(Ordering::SeqCst), 1);
    drop(constructor);
    gate.add_permits(1);
    let report = registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(
        report.attempts,
        vec![(0, PhysicalRetirementOutcome::Complete)]
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), server.read(&mut [0]))
            .await
            .expect("retirement releases the stream")
            .expect("read EOF"),
        0
    );
}

#[tokio::test]
async fn never_initialized_client_returns_transport_before_retirement_ack() {
    let (client, mut server) = tokio::io::duplex(64);
    let factory = Arc::new(PausedFactory {
        gate: Arc::new(Semaphore::new(1)),
        opened: Arc::new(AtomicUsize::new(0)),
        stream: Arc::new(Mutex::new(Some(client))),
    });
    let client = RmcpClient::new_in_process_client(factory)
        .await
        .expect("construct");
    let report = client
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(
        report.attempts,
        vec![(0, PhysicalRetirementOutcome::Complete)]
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), server.read(&mut [0]))
            .await
            .expect("retirement releases the stream")
            .expect("read EOF"),
        0
    );
}

#[tokio::test]
async fn closed_registry_refuses_constructor_before_factory_is_called() {
    let (client, _server) = tokio::io::duplex(64);
    let opened = Arc::new(AtomicUsize::new(0));
    let factory = Arc::new(PausedFactory {
        gate: Arc::new(Semaphore::new(1)),
        opened: Arc::clone(&opened),
        stream: Arc::new(Mutex::new(Some(client))),
    });
    let registry = RmcpClientRetirement::default();
    assert!(
        registry
            .shutdown_until(Instant::now())
            .await
            .attempts
            .is_empty()
    );
    assert!(
        RmcpClient::new_in_process_client_in_retirement(factory, registry)
            .await
            .is_err()
    );
    assert_eq!(opened.load(Ordering::SeqCst), 0);
}

/// Like rmcp's streamable HTTP worker, waits for its startup request, then
/// runs until cancelled.
struct StartupWorker {
    runs: Arc<AtomicUsize>,
}

impl Worker for StartupWorker {
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
        self.runs.fetch_add(1, Ordering::SeqCst);
        let startup = context.recv_from_handler().await?;
        let _ = startup.responder.send(Ok(()));
        context.cancellation_token.cancelled().await;
        Ok(())
    }
}

fn startup_request() -> ClientJsonRpcMessage {
    serde_json::from_value(serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "ping",
    }))
    .expect("ping request")
}

#[tokio::test(start_paused = true)]
async fn rmcp_worker_close_joins_a_worker_waiting_for_its_startup_request() {
    // The transport being closed owns the handler's sender, so only the
    // cancellation token can end this receive. Paused time turns a regression
    // into a timeout instead of a hang.
    let runs = Arc::new(AtomicUsize::new(0));
    let mut transport = WorkerTransport::spawn(StartupWorker {
        runs: Arc::clone(&runs),
    });
    tokio::time::timeout(Duration::from_secs(1), transport.close())
        .await
        .expect("close joins the worker before the retirement deadline")
        .expect("worker joins");
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn unused_deferred_worker_closes_with_terminal_receipt() {
    let runs = Arc::new(AtomicUsize::new(0));
    let deferred = DeferredWorkerTransport::new(StartupWorker {
        runs: Arc::clone(&runs),
    });
    assert!(!deferred.is_started());
    let (transport, receipt) = AcknowledgedTransport::new(deferred);
    // Dropping the handshake owner returns the transport to its receipt.
    drop(transport);
    let closed = tokio::time::timeout(
        Duration::from_secs(1),
        receipt.close_returned_until(Instant::now() + Duration::from_secs(30)),
    )
    .await
    .expect("unused transport closes before the retirement deadline");
    assert_eq!(closed, Ok(()));
    assert_eq!(runs.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn started_deferred_worker_closes_through_rmcp_worker() {
    let runs = Arc::new(AtomicUsize::new(0));
    let mut deferred = DeferredWorkerTransport::new(StartupWorker {
        runs: Arc::clone(&runs),
    });
    deferred
        .send(startup_request())
        .await
        .expect("worker accepts startup request");
    assert!(deferred.is_started());
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    let (transport, receipt) = AcknowledgedTransport::new(deferred);
    drop(transport);
    let closed = tokio::time::timeout(
        Duration::from_secs(1),
        receipt.close_returned_until(Instant::now() + Duration::from_secs(30)),
    )
    .await
    .expect("started transport joins its cancelled worker");
    assert_eq!(closed, Ok(()));
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}
