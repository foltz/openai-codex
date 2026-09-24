use std::io;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures::FutureExt;
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;
use tokio::io::BufReader;
use tokio::io::DuplexStream;
use tokio::sync::Semaphore;
use tokio::time::Instant;

use crate::InProcessTransportFactory;
use crate::PhysicalRetirementOutcome;
use crate::RmcpClient;
use crate::RmcpClientRetirement;

struct PausedFactory {
    gate: Arc<Semaphore>,
    opened: Arc<AtomicUsize>,
    stream: Arc<Mutex<Option<DuplexStream>>>,
}

#[tokio::test]
async fn retirement_joins_http_worker_with_stalled_initialize() {
    for body_kind in ["headers", "application/json", "text/event-stream"] {
        exercise_http_initialize_retirement(body_kind, true).await;
    }
}

#[tokio::test]
async fn uncancelled_http_initialize_completes_when_released() {
    for body_kind in ["headers", "application/json", "text/event-stream"] {
        exercise_http_initialize_retirement(body_kind, false).await;
    }
}

async fn exercise_http_initialize_retirement(body_kind: &'static str, retire: bool) {
    use axum::Json;
    use axum::Router;
    use axum::body::Body;
    use axum::http::Response;
    use axum::routing::post;
    use codex_config::types::AuthKeyringBackendKind;
    use codex_config::types::OAuthCredentialsStoreMode;
    use codex_exec_server::Environment;
    use tokio::sync::Notify;

    let entered = Arc::new(Notify::new());
    let gate = Arc::new(Semaphore::new(0));
    let app = Router::new().route("/mcp", post({
        let entered = Arc::clone(&entered);
        let gate = Arc::clone(&gate);
        move |Json(request): Json<serde_json::Value>| {
            let entered = Arc::clone(&entered);
            let gate = Arc::clone(&gate);
            async move {
                if request["method"] == "notifications/initialized" {
                    return Response::builder().status(202).body(Body::empty()).expect("accepted");
                }
                assert_eq!(request["method"], "initialize");
                let response = serde_json::json!({
                    "jsonrpc": "2.0", "id": request["id"],
                    "result": {"protocolVersion": "2025-06-18", "capabilities": {},
                        "serverInfo": {"name": "held-initialize", "version": "1"}}
                }).to_string();
                if body_kind == "headers" {
                    entered.notify_one();
                    gate.acquire().await.expect("release gate").forget();
                    return Response::builder().header("content-type", "application/json")
                        .body(Body::from(response)).expect("JSON response");
                }
                let body = futures::stream::once(async move {
                    entered.notify_one();
                    gate.acquire().await.expect("release body").forget();
                    let body = if body_kind == "text/event-stream" {
                        format!("event: message\ndata: {response}\n\n")
                    } else { response };
                    Ok::<_, io::Error>(body)
                });
                Response::builder().header("content-type", body_kind)
                    .body(Body::from_stream(body)).expect("streaming response")
            }
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}/mcp", listener.local_addr().expect("address"));
    let server = tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
    let client = RmcpClient::new_streamable_http_client(
        "retirement-http", &url, Some("fixture-bearer".to_string()), None, None,
        OAuthCredentialsStoreMode::File, AuthKeyringBackendKind::default(),
        Environment::default_for_tests().get_http_client(), None,
    ).await.expect("construct HTTP client");
    let mut initialize = Box::pin(client.initialize(
        rmcp::model::ClientInfo::default(), Some(Duration::from_secs(120)),
        Box::new(|_, _| Box::pin(async { panic!("unexpected elicitation") })),
    ));
    tokio::time::timeout(Duration::from_secs(20), async {
        tokio::select! {
            result = initialize.as_mut() => panic!("initialize completed before release: {result:?}"),
            _ = entered.notified() => {},
        }
    }).await.expect("initialize reached held HTTP response");
    if retire {
        drop(initialize);
        let report = tokio::time::timeout(Duration::from_secs(2),
            client.shutdown_until(Instant::now() + Duration::from_secs(5)))
            .await.expect("HTTP worker must join before the retirement deadline");
        assert_eq!(report.attempts, vec![(0, PhysicalRetirementOutcome::Complete)]);
        assert_eq!(client.shutdown_until(Instant::now()).await, report);
    } else {
        gate.add_permits(1);
        tokio::time::timeout(Duration::from_secs(5), initialize).await
            .expect("released initialize finishes").expect("initialize succeeds");
        let report = client.shutdown_until(Instant::now() + Duration::from_secs(5)).await;
        assert_eq!(report.attempts, vec![(0, PhysicalRetirementOutcome::Complete)]);
    }
    gate.add_permits(1);
    server.abort();
    let _ = server.await;
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
    assert_eq!(report.attempts, vec![(0, PhysicalRetirementOutcome::Complete)]);
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
