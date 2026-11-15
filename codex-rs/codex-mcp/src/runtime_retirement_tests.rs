use super::*;
use futures::FutureExt;
use futures::future::BoxFuture;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Semaphore;

#[tokio::test]
async fn runtime_closure_fences_every_registered_gate_before_first_cancellation() {
    let root = McpRuntimeRetirement::default();
    let first = PrivateClientRetirement::default();
    let second = PrivateClientRetirement::default();
    let cancellation = CancellationToken::new();
    let second_slot = Arc::new(ConnectionSlot::default());
    root.register_connection(first.clone(), None, cancellation.clone())
        .unwrap();
    root.register_connection(
        second.clone(),
        Some(Arc::clone(&second_slot)),
        CancellationToken::new(),
    )
    .unwrap();
    // Bind the cancellation to a real registered private lower owner as well.
    first
        .registry
        .register_connection(cancellation.clone())
        .unwrap();
    let attempted = Arc::new(AtomicUsize::new(0));
    let private_attempted = Arc::clone(&attempted);
    let shared_attempted = Arc::clone(&attempted);
    let ready = Arc::new(Semaphore::new(0));
    let entered = Arc::clone(&ready);
    let task = first
        .registry
        .task_ticket()
        .register(move || async move {
            entered.add_permits(1);
            cancellation.cancelled().await;
            let private = second.track_startup(CancellationToken::new(), |_| {
                private_attempted.fetch_add(1, Ordering::SeqCst);
                panic!("second private gate must already be fenced")
            });
            assert!(matches!(
                private.await,
                Err(crate::rmcp_client::StartupOutcomeError::Cancelled)
            ));
            let pool = crate::McpConnectionPool::default();
            let shared = pool
                .connect(
                    second_slot,
                    crate::connection_pool::tests::key(
                        &crate::connection_pool::tests::context(),
                        &crate::connection_pool::tests::config(),
                    ),
                    Arc::default(),
                    None,
                    CancellationToken::new(),
                    move |_, _, _| {
                        shared_attempted.fetch_add(1, Ordering::SeqCst);
                        panic!("second shared gate must already be fenced")
                    },
                )
                .await;
            assert!(matches!(
                shared,
                Err(crate::rmcp_client::StartupOutcomeError::Cancelled)
            ));
            crate::private_retirement::PrivateTaskOutcome::Complete
        })
        .unwrap();
    let caller = tokio::spawn(task);
    ready.acquire().await.unwrap().forget();
    root.close_registration();
    caller.await.unwrap();
    let report = root
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert!(report.are_connections_released(), "{report:?}");
    assert_eq!(attempted.load(Ordering::SeqCst), 0);
    assert_eq!(report.registered_at_end, 2);
    assert!(
        root.register_connection(
            PrivateClientRetirement::default(),
            None,
            CancellationToken::new()
        )
        .is_err()
    );
}

struct GatedTransport {
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
    stream: Arc<Mutex<Option<tokio::io::DuplexStream>>>,
}

impl codex_rmcp_client::InProcessTransportFactory for GatedTransport {
    fn open(&self) -> BoxFuture<'static, std::io::Result<tokio::io::DuplexStream>> {
        let entered = Arc::clone(&self.entered);
        let release = Arc::clone(&self.release);
        let stream = Arc::clone(&self.stream);
        async move {
            entered.add_permits(1);
            release.acquire().await.unwrap().forget();
            stream
                .lock()
                .unwrap()
                .take()
                .ok_or_else(|| std::io::Error::other("already consumed"))
        }
        .boxed()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_runtime_owner_retains_cancelled_constructor_and_actual_late_transport() {
    use tokio::io::AsyncReadExt;
    let root = McpRuntimeRetirement::default();
    let private = PrivateClientRetirement::default();
    root.register_connection(private.clone(), None, CancellationToken::new())
        .unwrap();
    let (stream, mut peer) = tokio::io::duplex(64);
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let factory = Arc::new(GatedTransport {
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
        stream: Arc::new(Mutex::new(Some(stream))),
    });
    let startup = private.track_startup(CancellationToken::new(), move |lower| {
        async move {
            let client =
                codex_rmcp_client::RmcpClient::new_in_process_client_in_retirement(factory, lower)
                    .await
                    .map_err(anyhow::Error::from)?;
            Ok(
                crate::connection_manager::tests::create_test_managed_client_for_transport(
                    Vec::new(),
                    client,
                ),
            )
        }
        .boxed()
        .shared()
    });
    let caller = tokio::spawn(startup);
    entered.acquire().await.unwrap().forget();
    caller.abort();
    assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
    drop(private);
    let pending = root.shutdown_until(Instant::now()).await;
    assert!(!pending.are_connections_released());
    assert_eq!(pending.connections.len(), 1);
    release.add_permits(1);
    let deadline = Instant::now() + Duration::from_secs(1);
    let report = root.shutdown_until(deadline).await;
    assert!(report.are_connections_released(), "{report:?}");
    assert!(
        !report.connections[0].private.connections[0]
            .1
            .attempts
            .is_empty()
    );
    assert_eq!(
        tokio::time::timeout_at(deadline, peer.read(&mut [0]))
            .await
            .unwrap()
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn startup_context_detaches_containing_census_before_lazy_future_retention() {
    let root = McpRuntimeRetirement::default();
    let weak = Arc::downgrade(&root.state);
    let mut context = crate::connection_pool::tests::context();
    context.retirement = Some(root.clone());
    let detached = context.without_retirement_owner();
    assert!(detached.retirement.is_none());
    let private = PrivateClientRetirement::default();
    root.register_connection(private.clone(), None, CancellationToken::new())
        .unwrap();
    let startup = private.track_startup(CancellationToken::new(), move |_| {
        async move {
            let _detached = detached;
            std::future::pending().await
        }
        .boxed()
        .shared()
    });
    drop(startup);
    drop(private);
    drop(root);
    assert!(
        weak.upgrade().is_none(),
        "a dormant original startup must not retain its own containing census"
    );
}

#[tokio::test]
async fn empty_runtime_exposes_external_connection_owner_before_shutdown() {
    let external = McpRuntimeRetirement::default();
    let runtime = crate::McpRuntime::empty_in_retirement(true, external.clone());
    assert!(!external.is_closed());
    assert!(Arc::ptr_eq(
        &external.state,
        &runtime.retirement_owner().state
    ));
    runtime.shutdown().await;
    drop(runtime);
    let report = external.shutdown_until(Instant::now()).await;
    assert!(report.are_connections_released());
    assert_eq!(report.registered_at_end, 0);
}
