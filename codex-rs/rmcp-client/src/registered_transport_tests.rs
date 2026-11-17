use super::DeferredWorkerTransport;
use crate::InProcessTransportFactory;
use crate::PhysicalRetirementOutcome;
use crate::RmcpClient;
use crate::RmcpClientRetirement;
use crate::retirement::AcknowledgedTransport;
use futures::FutureExt;
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use rmcp::model::ClientJsonRpcMessage;
use rmcp::service::RoleClient;
use rmcp::transport::Transport;
use rmcp::transport::WorkerTransport;
use rmcp::transport::worker::Worker;
use rmcp::transport::worker::WorkerContext;
use rmcp::transport::worker::WorkerQuitReason;
use std::io;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;
use tokio::io::BufReader;
use tokio::io::DuplexStream;
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
        rmcp::model::ClientConfig::default(),
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

struct FixtureServer;
impl rmcp::ServerHandler for FixtureServer {}

#[derive(Default)]
struct ServingFactory {
    opened: Arc<AtomicUsize>,
    recovery_gate: Option<Arc<Semaphore>>,
}

impl InProcessTransportFactory for ServingFactory {
    fn open(&self) -> BoxFuture<'static, io::Result<DuplexStream>> {
        use rmcp::ServiceExt;
        let index = self.opened.fetch_add(1, Ordering::SeqCst);
        let recovery_gate = self.recovery_gate.clone();
        async move {
            if index > 0
                && let Some(gate) = recovery_gate
            {
                gate.acquire()
                    .await
                    .expect("recovery creation gate")
                    .forget();
            }
            let (client, server) = tokio::io::duplex(8192);
            tokio::spawn(async move {
                let service: rmcp::service::RunningService<rmcp::RoleServer, FixtureServer> =
                    FixtureServer
                        .serve(server)
                        .await
                        .expect("fixture server initializes");
                service.waiting().await.expect("fixture server exits");
            });
            Ok(client)
        }
        .boxed()
    }
}

async fn serving_client(
    factory: Arc<ServingFactory>,
    registry: RmcpClientRetirement,
) -> RmcpClient {
    let client = RmcpClient::new_in_process_client_in_retirement(factory, registry)
        .await
        .expect("create");
    client
        .initialize(
            rmcp::model::ClientConfig::default(),
            Some(Duration::from_secs(5)),
            Box::new(|_, _| Box::pin(async { panic!("unexpected elicitation") })),
        )
        .await
        .expect("initialize");
    client
}

#[tokio::test]
async fn live_client_retirement_waits_for_typed_lease_then_retries_with_fresh_budget() {
    let registry = RmcpClientRetirement::default();
    let client = serving_client(Arc::new(ServingFactory::default()), registry).await;
    let lease = client.service().await.expect("live lease");
    let first = client
        .shutdown_until(Instant::now() + Duration::from_millis(20))
        .await;
    assert!(
        !first.is_complete(),
        "a held request lease prevents terminal proof"
    );
    assert_eq!(first.attempts.len(), 1);
    assert!(
        lease.reacquire().is_err(),
        "closed admission refuses another retry"
    );
    drop(lease);
    assert!(
        client
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .is_complete()
    );
}

#[tokio::test]
async fn concurrent_and_stale_recovery_leases_create_only_one_successor() {
    let factory = Arc::new(ServingFactory::default());
    let client = serving_client(Arc::clone(&factory), RmcpClientRetirement::default()).await;
    let failed = client.service().await.expect("original lease");
    let (first, second) = tokio::join!(
        client.reinitialize_after_session_expiry(&failed),
        client.reinitialize_after_session_expiry(&failed),
    );
    first.expect("first recovery");
    second.expect("concurrent recovery joins installed successor");
    assert_eq!(factory.opened.load(Ordering::SeqCst), 2);
    client
        .reinitialize_after_session_expiry(&failed)
        .await
        .expect("stale lease is already superseded");
    assert_eq!(factory.opened.load(Ordering::SeqCst), 2);
    drop(failed);
    let report = client
        .shutdown_until(Instant::now() + Duration::from_secs(5))
        .await;
    assert_eq!(report.attempts.len(), 2);
    assert!(report.is_complete());
}

#[tokio::test]
async fn external_retirement_owner_joins_a_dropped_initialized_client() {
    let registry = RmcpClientRetirement::default();
    let client = serving_client(Arc::new(ServingFactory::default()), registry.clone()).await;
    drop(client);
    let report = registry
        .shutdown_until(Instant::now() + Duration::from_secs(5))
        .await;
    assert_eq!(report.attempts.len(), 1);
    assert!(report.is_complete());
}

#[tokio::test]
async fn close_during_recovery_install_retains_the_connected_successor_receipt() {
    let gate = Arc::new(Semaphore::new(0));
    let factory = Arc::new(ServingFactory {
        recovery_gate: Some(Arc::clone(&gate)),
        ..Default::default()
    });
    let registry = RmcpClientRetirement::default();
    let client = Arc::new(serving_client(Arc::clone(&factory), registry.clone()).await);
    let failed = client.service().await.expect("original lease");
    let mut recovery = Box::pin(client.reinitialize_after_session_expiry(&failed));
    // Pass the recovery's initial state check, then hold the install lock.
    tokio::time::timeout(Duration::from_secs(5), async {
        while factory.opened.load(Ordering::SeqCst) < 2 {
            tokio::select! {
                result = recovery.as_mut() => panic!("recovery finished before install barrier: {result:?}"),
                _ = tokio::task::yield_now() => {}
            }
        }
    }).await.expect("successor creation admitted");
    let (locked, lock_ready) = tokio::sync::oneshot::channel();
    let (close, close_requested) = std::sync::mpsc::channel();
    let install_client = Arc::clone(&client);
    let runtime = tokio::runtime::Handle::current();
    // Keep the install barrier on a blocking worker, not across an async await.
    let holder = tokio::task::spawn_blocking(move || {
        let mut state = runtime.block_on(install_client.state.lock());
        if locked.send(()).is_err() {
            return;
        }
        // If the test unwinds, sender disconnection releases the guard without
        // fabricating a close. The successful path closes under the same lock.
        if close_requested.recv().is_ok() {
            *state = super::super::ClientState::Closed;
        }
    });
    lock_ready.await.expect("install lock held");
    gate.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), async {
        while registry.attached_service_count() < 2 {
            tokio::select! {
                result = recovery.as_mut() => panic!("recovery installed through held state lock: {result:?}"),
                _ = tokio::task::yield_now() => {}
            }
        }
    }).await.expect("successor has a retained service and receipt");
    // Close wins precisely after connect but before the new Ready install.
    close.send(()).expect("request close under install lock");
    holder.await.expect("install lock holder joined");
    assert!(recovery.await.is_err());
    drop(failed);
    let report = registry
        .shutdown_until(Instant::now() + Duration::from_secs(5))
        .await;
    assert_eq!(report.attempts.len(), 2);
    assert!(report.is_complete());
}

struct ReturnProbe {
    returned: Arc<AtomicUsize>,
    closed: Arc<AtomicUsize>,
    fail_close: bool,
}
impl Transport<RoleClient> for ReturnProbe {
    type Error = io::Error;
    fn send(
        &mut self,
        _: rmcp::service::TxJsonRpcMessage<RoleClient>,
    ) -> impl Future<Output = io::Result<()>> + Send + 'static {
        std::future::ready(Ok(()))
    }
    async fn receive(&mut self) -> Option<rmcp::service::RxJsonRpcMessage<RoleClient>> {
        std::future::pending().await
    }
    async fn close(&mut self) -> io::Result<()> {
        self.closed.fetch_add(1, Ordering::SeqCst);
        if self.fail_close {
            Err(io::Error::other("injected exact close failure"))
        } else {
            Ok(())
        }
    }
}
fn return_probe(
    returned: Arc<AtomicUsize>,
    closed: Arc<AtomicUsize>,
    fail_close: bool,
) -> (
    AcknowledgedTransport<ReturnProbe>,
    crate::retirement::TransportCloseReceipt,
) {
    let (transport, receipt) = AcknowledgedTransport::new(ReturnProbe {
        returned,
        closed,
        fail_close,
    });
    (
        transport.with_on_return(|probe| {
            probe.returned.fetch_add(1, Ordering::SeqCst);
        }),
        receipt,
    )
}

#[tokio::test]
async fn return_callback_runs_once_for_abandonment_and_never_for_live_or_closed_transport() {
    let returned = Arc::new(AtomicUsize::new(0));
    let closed = Arc::new(AtomicUsize::new(0));
    let (transport, receipt) = return_probe(returned.clone(), closed.clone(), false);
    assert_eq!(
        returned.load(Ordering::SeqCst),
        0,
        "live transport has not returned"
    );
    drop(transport);
    assert_eq!(returned.load(Ordering::SeqCst), 1);
    receipt
        .close_returned_until(Instant::now() + Duration::from_secs(1))
        .await
        .expect("exact close");
    receipt
        .close_returned_until(Instant::now() + Duration::from_secs(1))
        .await
        .expect("repeat observation");
    assert_eq!(returned.load(Ordering::SeqCst), 1);
    assert_eq!(closed.load(Ordering::SeqCst), 1);

    let (mut transport, receipt) = return_probe(returned.clone(), closed.clone(), false);
    transport.close().await.expect("normal terminal close");
    drop(transport);
    assert_eq!(
        returned.load(Ordering::SeqCst),
        1,
        "normal successful close is not abandonment"
    );
    receipt
        .close_returned_until(Instant::now())
        .await
        .expect("cached exact close");
    assert_eq!(closed.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn failed_handshake_keeps_original_error_and_failed_exact_close_receipt() {
    let registry = RmcpClientRetirement::default();
    let ticket = registry.reserve_attempt().expect("reserve");
    let returned = Arc::new(AtomicUsize::new(0));
    let closed = Arc::new(AtomicUsize::new(0));
    let (transport, receipt) = return_probe(returned.clone(), closed.clone(), true);
    ticket.attach_transport(receipt.clone(), None);
    let result = super::retain_phase(ticket, move |ticket| async move {
        super::bind_service(
            async move {
                drop(transport);
                Err(rmcp::service::ClientInitializeError::ConnectionClosed(
                    "original handshake discriminator".into(),
                ))
            },
            ticket,
            receipt,
        )
        .await
    })
    .await;
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("handshake must fail"),
    };
    let error = error
        .downcast_ref::<super::super::streamable_http_retry::HandshakeError>()
        .expect("original handshake error wrapper");
    assert!(
        matches!(&error.source, rmcp::service::ClientInitializeError::ConnectionClosed(message) if message=="original handshake discriminator")
    );
    assert_eq!(returned.load(Ordering::SeqCst), 1);
    assert_eq!(closed.load(Ordering::SeqCst), 1);
    let report = registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(
        report.attempts,
        vec![(0, PhysicalRetirementOutcome::LogicalFailed)]
    );
}
