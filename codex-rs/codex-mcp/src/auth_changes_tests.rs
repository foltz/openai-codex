use super::*;
use codex_rmcp_client::InProcessTransportFactory;
use futures::FutureExt;
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use rmcp::ServiceExt;
use rmcp::model::ClientCapabilities;
use rmcp::model::CustomNotification;
use rmcp::model::Implementation;
use rmcp::model::InitializeRequestParams;
use tokio::sync::mpsc;
use tokio::time::timeout;

#[derive(Clone)]
struct NotificationServer(mpsc::Sender<serde_json::Value>);

impl rmcp::ServerHandler for NotificationServer {
    fn get_info(&self) -> rmcp::model::ServerInfo {
        let mut info = rmcp::model::ServerInfo::default();
        info.capabilities.experimental = Some([(CAPABILITY.to_owned(), Default::default())].into());
        info
    }

    async fn on_custom_notification(
        &self,
        notification: CustomNotification,
        _context: rmcp::service::NotificationContext<rmcp::RoleServer>,
    ) {
        self.0.send(json!(notification)).await.unwrap();
    }
}

impl InProcessTransportFactory for NotificationServer {
    fn open(&self) -> BoxFuture<'static, std::io::Result<tokio::io::DuplexStream>> {
        let server = self.clone();
        async move {
            let (client, transport) = tokio::io::duplex(/*max_buf_size*/ 4096);
            tokio::spawn(async move {
                let service = server.serve(transport).await.unwrap();
                service.waiting().await.unwrap();
            });
            Ok(client)
        }
        .boxed()
    }
}

#[tokio::test]
async fn auth_notifications_require_opt_in_and_follow_client_lifetime() -> Result<()> {
    let workers = crate::McpWorkerOwner::default();
    let (notifications, mut received) = mpsc::channel(/*buffer*/ 8);
    let client = Arc::new(
        RmcpClient::new_in_process_client(Arc::new(NotificationServer(notifications))).await?,
    );
    client
        .initialize(
            InitializeRequestParams::new(
                ClientCapabilities::default(),
                Implementation::new("test", "1"),
            ),
            Some(SEND_TIMEOUT),
            Box::new(|_, _| async { anyhow::bail!("unexpected elicitation") }.boxed()),
        )
        .await?;
    let (changes, receiver) = watch::channel(AuthChangeState::default());
    let mut capabilities = ServerCapabilities::default();
    assert!(
        start(
            &workers,
            Arc::clone(&client),
            &capabilities,
            Some(receiver.clone())
        )
        .await?
        .is_none()
    );
    capabilities.experimental = Some([(CAPABILITY.to_string(), Default::default())].into());
    assert!(
        start(
            &workers,
            Arc::clone(&client),
            &capabilities,
            /*changes*/ None
        )
        .await?
        .is_none()
    );
    assert_eq!(received.try_recv(), Err(mpsc::error::TryRecvError::Empty));
    let watcher = start(&workers, Arc::clone(&client), &capabilities, Some(receiver))
        .await?
        .unwrap();
    assert_eq!(
        timeout(SEND_TIMEOUT, received.recv()).await?,
        Some(
            json!({"method": NOTIFICATION, "params": {"_meta": {}, "generation": 0, "ownerGeneration": 0}})
        ),
    );
    changes.send_modify(|state| state.generation += 1);
    assert_eq!(
        timeout(SEND_TIMEOUT, received.recv()).await?,
        Some(
            json!({"method": NOTIFICATION, "params": {"_meta": {}, "generation": 1, "ownerGeneration": 0}})
        ),
    );
    for _ in 0..2 {
        changes.send_modify(|state| {
            state.generation += 1;
            state.owner_generation += 1;
        });
    }
    assert_eq!(
        timeout(SEND_TIMEOUT, received.recv()).await?,
        Some(
            json!({"method": NOTIFICATION, "params": {"_meta": {}, "generation": 3, "ownerGeneration": 2}})
        ),
    );
    drop(watcher);
    timeout(SEND_TIMEOUT, changes.closed()).await?;
    let report = workers
        .observe_until(tokio::time::Instant::now() + SEND_TIMEOUT)
        .await;
    let watcher = report
        .counts
        .iter()
        .find(|count| count.kind == crate::McpWorkerKind::AuthNotification)
        .unwrap();
    assert_eq!(watcher.cancelled, 1);
    assert_eq!(watcher.requested_cancelled, 1);
    assert_eq!(watcher.pending + watcher.panicked + watcher.joined, 0);
    client.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn explicit_private_shutdown_stops_its_watcher_even_with_retained_client_bindings()
-> Result<()> {
    let session_context = crate::connection_pool::tests::context();
    let workers = session_context.workers.clone();
    let pool = crate::McpConnectionPool::default();
    let (notifications, mut received) = mpsc::channel(8);
    let client =
        RmcpClient::new_in_process_client(Arc::new(NotificationServer(notifications))).await?;
    client
        .initialize(
            InitializeRequestParams::new(
                ClientCapabilities::default(),
                Implementation::new("private-watch", "1"),
            ),
            Some(SEND_TIMEOUT),
            Box::new(|_, _| async { anyhow::bail!("unexpected elicitation") }.boxed()),
        )
        .await?;
    let mut managed = crate::connection_manager::tests::create_test_managed_client_for_transport(
        Vec::new(),
        client,
    );
    let (changes, receiver) = watch::channel(AuthChangeState::default());
    let mut capabilities = ServerCapabilities::default();
    capabilities.experimental = Some([(CAPABILITY.to_string(), Default::default())].into());
    managed._auth_change_notifications = start(
        &workers,
        Arc::clone(&managed.client),
        &capabilities,
        Some(receiver),
    )
    .await?;
    assert!(managed._auth_change_notifications.is_some());
    assert!(timeout(SEND_TIMEOUT, received.recv()).await?.is_some());
    let retained = managed.clone();
    let mut async_client =
        crate::connection_manager::tests::create_ready_async_managed_client(Vec::new()).await;
    async_client.client = futures::future::ready(Ok(managed)).boxed().shared();
    async_client.shutdown().await;
    timeout(SEND_TIMEOUT, changes.closed()).await?;
    assert!(retained._auth_change_notifications.is_some());
    assert!(retained.client.is_closed().await);
    let result = workers
        .observe_until(tokio::time::Instant::now() + SEND_TIMEOUT)
        .await;
    let watcher = result
        .counts
        .iter()
        .find(|c| c.kind == crate::McpWorkerKind::AuthNotification)
        .unwrap();
    assert_eq!(watcher.cancelled, 1);
    assert_eq!(watcher.requested_cancelled, 1);
    assert_eq!(watcher.pending + watcher.joined + watcher.panicked, 0);
    assert_eq!(
        pool.observe_background_workers_until(tokio::time::Instant::now())
            .await
            .registered_at_end,
        0
    );
    Ok(())
}

#[tokio::test]
async fn closed_auth_worker_admission_refuses_before_initial_notification() -> Result<()> {
    let workers = crate::McpWorkerOwner::default();
    let (notifications, mut received) = mpsc::channel(8);
    let client = Arc::new(
        RmcpClient::new_in_process_client(Arc::new(NotificationServer(notifications))).await?,
    );
    client
        .initialize(
            InitializeRequestParams::new(
                ClientCapabilities::default(),
                Implementation::new("test", "1"),
            ),
            Some(SEND_TIMEOUT),
            Box::new(|_, _| async { anyhow::bail!("unexpected elicitation") }.boxed()),
        )
        .await?;
    let (changes, receiver) = watch::channel(AuthChangeState::default());
    let mut capabilities = ServerCapabilities::default();
    capabilities.experimental = Some([(CAPABILITY.to_owned(), Default::default())].into());
    workers.close_startup_admission();
    assert!(
        start(&workers, Arc::clone(&client), &capabilities, Some(receiver))
            .await
            .is_err()
    );
    timeout(SEND_TIMEOUT, changes.closed()).await?;
    assert!(matches!(
        received.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    let report = workers.observe_until(tokio::time::Instant::now()).await;
    assert_eq!(report.registered_at_end, 0);
    assert_eq!(report.refused_at_end, 1);
    client.shutdown().await;
    Ok(())
}

async fn private_auth_fixture(
    lower: codex_rmcp_client::RmcpClientRetirement,
    workers: crate::McpWorkerOwner,
    notifications: mpsc::Sender<serde_json::Value>,
    changes: watch::Receiver<AuthChangeState>,
) -> Result<crate::rmcp_client::ManagedClient> {
    let client = RmcpClient::new_in_process_client_in_retirement(
        Arc::new(NotificationServer(notifications)),
        lower,
    )
    .await?;
    let initialized = client
        .initialize(
            InitializeRequestParams::new(
                ClientCapabilities::default(),
                Implementation::new("checked-private-watch", "1"),
            ),
            Some(SEND_TIMEOUT),
            Box::new(|_, _| async { anyhow::bail!("unexpected elicitation") }.boxed()),
        )
        .await?;
    let mut managed = crate::connection_manager::tests::create_test_managed_client_for_transport(
        Vec::new(),
        client,
    );
    managed._auth_change_notifications = start(
        &workers,
        Arc::clone(&managed.client),
        &initialized.capabilities,
        Some(changes),
    )
    .await?;
    Ok(managed)
}

#[tokio::test]
async fn private_external_retirement_stops_checked_watcher_with_retained_bindings() -> Result<()> {
    let runtime = crate::McpRuntime::empty(true);
    let root = runtime.retirement_owner();
    let workers = runtime.worker_owner();
    let private = crate::private_retirement::PrivateClientRetirement::default();
    let cancel = tokio_util::sync::CancellationToken::new();
    root.register_connection(private.clone(), None, cancel.clone())
        .unwrap();
    let (notifications, mut received) = mpsc::channel(8);
    let (changes, receiver) = watch::channel(AuthChangeState::default());
    let future = private.track_startup(cancel, move |lower| {
        async move {
            private_auth_fixture(lower, workers, notifications, receiver)
                .await
                .map_err(Into::into)
        }
        .boxed()
        .shared()
    });
    let retained = future.clone().await?;
    assert!(timeout(SEND_TIMEOUT, received.recv()).await?.is_some());
    assert!(retained._auth_change_notifications.is_some());
    let report = root
        .drain_workers_until(tokio::time::Instant::now() + SEND_TIMEOUT)
        .await;
    assert!(report.are_recorded_workers_drained(), "{report:?}");
    timeout(SEND_TIMEOUT, changes.closed()).await?;
    assert!(retained.client.is_closed().await);
    let watcher = report.workers[0]
        .counts
        .iter()
        .find(|count| count.kind == crate::McpWorkerKind::AuthNotification)
        .unwrap();
    assert_eq!(watcher.cancelled, 1);
    assert_eq!(watcher.requested_cancelled, 1);
    assert!(retained._auth_change_notifications.is_some());
    drop(future);
    Ok(())
}

#[tokio::test]
async fn private_watcher_completing_after_closure_stops_on_checked_late_binding() -> Result<()> {
    let runtime = crate::McpRuntime::empty(true);
    let root = runtime.retirement_owner();
    let workers = runtime.worker_owner();
    let private = crate::private_retirement::PrivateClientRetirement::default();
    let cancel = tokio_util::sync::CancellationToken::new();
    root.register_connection(private.clone(), None, cancel.clone())
        .unwrap();
    let (notifications, mut received) = mpsc::channel(8);
    let (changes, receiver) = watch::channel(AuthChangeState::default());
    let release = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::clone(&release);
    let future = private.track_startup(cancel, move |lower| {
        async move {
            let managed = private_auth_fixture(lower, workers, notifications, receiver).await?;
            resume.notified().await;
            Ok(managed)
        }
        .boxed()
        .shared()
    });
    let caller = tokio::spawn(future.clone());
    assert!(timeout(SEND_TIMEOUT, received.recv()).await?.is_some());
    root.close_registration();
    let incomplete = root
        .drain_workers_until(tokio::time::Instant::now() + Duration::from_millis(20))
        .await;
    assert!(!incomplete.are_recorded_workers_drained());
    release.notify_one();
    let retained = caller.await??;
    let report = root
        .drain_workers_until(tokio::time::Instant::now() + SEND_TIMEOUT)
        .await;
    assert!(report.are_recorded_workers_drained(), "{report:?}");
    timeout(SEND_TIMEOUT, changes.closed()).await?;
    assert!(retained.client.is_closed().await);
    assert!(retained._auth_change_notifications.is_some());
    Ok(())
}

#[tokio::test]
async fn private_foreign_binding_never_borrows_stop_authority_over_its_auth_watcher() -> Result<()>
{
    let runtime = crate::McpRuntime::empty(true);
    let root = runtime.retirement_owner();
    let foreign_workers = crate::McpWorkerOwner::default();
    let background = foreign_workers.clone();
    let private = crate::private_retirement::PrivateClientRetirement::default();
    let cancel = tokio_util::sync::CancellationToken::new();
    root.register_connection(private.clone(), None, cancel.clone())
        .unwrap();
    let (notifications, mut received) = mpsc::channel(8);
    let (changes, receiver) = watch::channel(AuthChangeState::default());
    let (send, retain) = tokio::sync::oneshot::channel();
    let future = private.track_startup(cancel, move |_expected_lower| {
        async move {
            let managed =
                private_auth_fixture(Default::default(), background, notifications, receiver)
                    .await?;
            let _ = send.send(managed.clone());
            Ok(managed)
        }
        .boxed()
        .shared()
    });
    assert!(future.await.is_err());
    let retained = retain.await?;
    assert!(timeout(SEND_TIMEOUT, received.recv()).await?.is_some());
    let report = root
        .drain_workers_until(tokio::time::Instant::now() + SEND_TIMEOUT)
        .await;
    assert!(!report.are_recorded_workers_drained());
    assert!(
        !report.connections.connections[0]
            .private
            .foreign_connections
            .is_empty()
    );
    let still_running = foreign_workers
        .observe_until(tokio::time::Instant::now())
        .await;
    let watcher = still_running
        .counts
        .iter()
        .find(|count| count.kind == crate::McpWorkerKind::AuthNotification)
        .unwrap();
    assert_eq!(watcher.pending, 1);
    assert_eq!(watcher.cancelled + watcher.joined + watcher.panicked, 0);
    changes.send_modify(|state| state.generation += 1);
    let notification = timeout(SEND_TIMEOUT, received.recv()).await?.unwrap();
    assert_eq!(notification["params"]["generation"], json!(1));
    retained.stop_auth_change_notifications();
    retained.client.shutdown().await;
    timeout(SEND_TIMEOUT, changes.closed()).await?;
    let stopped = foreign_workers
        .observe_until(tokio::time::Instant::now() + SEND_TIMEOUT)
        .await;
    assert_eq!(
        stopped
            .counts
            .iter()
            .find(|count| count.kind == crate::McpWorkerKind::AuthNotification)
            .unwrap()
            .requested_cancelled,
        1
    );
    Ok(())
}
