use super::*;
use crate::{McpAttemptAccess, McpAttemptRefused, McpAttemptRequirement, McpAttemptWork};
use pretty_assertions::assert_eq;
use rmcp::model::{
    ClientCapabilities, Implementation, InitializeRequestParams, ListResourceTemplatesResult,
    ListResourcesResult, PaginatedRequestParams, ReadResourceRequestParams, ReadResourceResponse,
    ServerCapabilities, ServerConfig,
};
use rmcp::service::RequestContext;
use rmcp::{RoleServer, ServerHandler, ServiceExt};
use std::sync::{Mutex, atomic::AtomicUsize};
use std::time::Duration;

#[derive(Default)]
struct Counts {
    live: AtomicUsize,
    derived: AtomicUsize,
    refuse: AtomicBool,
}
struct Work(Arc<Counts>);
impl McpAttemptWork for Work {
    fn derive_attempt(&self) -> Result<Box<dyn McpAttemptWork>, McpAttemptRefused> {
        if self.0.refuse.load(Ordering::SeqCst) {
            return Err(McpAttemptRefused);
        }
        self.0.derived.fetch_add(1, Ordering::SeqCst);
        self.0.live.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(Self(self.0.clone())))
    }
}
impl Drop for Work {
    fn drop(&mut self) {
        self.0.live.fetch_sub(1, Ordering::SeqCst);
    }
}
fn work() -> Work {
    let counts = Arc::new(Counts::default());
    counts.live.store(1, Ordering::SeqCst);
    Work(counts)
}

#[derive(Clone, Default)]
struct Server {
    calls: Arc<AtomicUsize>,
    hold: Arc<AtomicBool>,
    started: Arc<Notify>,
    finish_request: Arc<Notify>,
    tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}
impl Server {
    async fn observe(&self) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        if self.hold.load(Ordering::SeqCst) {
            self.finish_request.notified().await;
        }
    }
    async fn finish(&self, client: &RmcpClient) {
        // Teardown only: cancellation ownership was already asserted at zero.
        // Local cancellation is not proof that remote server work stopped.
        self.finish_request.notify_one();
        tokio::time::timeout(Duration::from_secs(5), client.shutdown())
            .await
            .unwrap();
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
        for task in tasks {
            tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .unwrap()
                .unwrap();
        }
    }
}
impl ServerHandler for Server {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_resources().build())
    }
    async fn list_resources(
        &self,
        _params: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, rmcp::ErrorData> {
        self.observe().await;
        Ok(ListResourcesResult::with_all_items(Vec::new()))
    }
    async fn list_resource_templates(
        &self,
        _params: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, rmcp::ErrorData> {
        self.observe().await;
        Ok(ListResourceTemplatesResult::with_all_items(Vec::new()))
    }
    async fn read_resource(
        &self,
        _params: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, rmcp::ErrorData> {
        self.observe().await;
        Err(rmcp::ErrorData::invalid_params(
            "test read reached exact server",
            None,
        ))
    }
}
impl InProcessTransportFactory for Server {
    fn open(&self) -> futures::future::BoxFuture<'static, io::Result<DuplexStream>> {
        let server = self.clone();
        async move {
            let (client, transport) = tokio::io::duplex(4096);
            let service = server.clone();
            let task = tokio::spawn(async move {
                service
                    .serve(transport)
                    .await
                    .expect("serve resources")
                    .waiting()
                    .await
                    .expect("resource server join");
            });
            server.tasks.lock().unwrap().push(task);
            Ok(client)
        }
        .boxed()
    }
}
async fn fixture(requirement: McpAttemptRequirement) -> (TestStep, Server) {
    let server = Server::default();
    let step = test_step_with_transport(
        "resource",
        AppToolApproval::Approve,
        false,
        Arc::new(server.clone()),
        requirement,
    )
    .await;
    step.client
        .initialize(
            InitializeRequestParams::new(
                ClientCapabilities::default(),
                Implementation::new("resource-work-test", "1"),
            ),
            Some(Duration::from_secs(5)),
            Box::new(|_, _| async { Err(anyhow::anyhow!("unexpected elicitation")) }.boxed()),
        )
        .await
        .unwrap();
    (step, server)
}

#[tokio::test]
async fn required_ready_resource_calls_refuse_unscoped_without_wire_requests() {
    let (step, server) = fixture(McpAttemptRequirement::Required).await;
    assert!(
        step.step
            .list_resources(SERVER_NAME, None)
            .await
            .unwrap_err()
            .is::<McpAttemptRefused>()
    );
    assert!(
        step.step
            .list_resource_templates(SERVER_NAME, None)
            .await
            .unwrap_err()
            .is::<McpAttemptRefused>()
    );
    assert!(
        step.step
            .read_resource(SERVER_NAME, ReadResourceRequestParams::new("test://read"))
            .await
            .unwrap_err()
            .is::<McpAttemptRefused>()
    );
    assert!(
        step.step
            .list_all_resources(|_| true)
            .await
            .by_server
            .is_empty()
    );
    assert!(
        step.step
            .list_all_resource_templates(|_| true)
            .await
            .by_server
            .is_empty()
    );
    assert_eq!(server.calls.load(Ordering::SeqCst), 0);
    server.finish(&step.client).await;
}

#[tokio::test]
async fn exhausted_ready_resource_calls_do_not_fall_back_even_when_ungated() {
    let (step, server) = fixture(McpAttemptRequirement::Ungated).await;
    let work = work();
    work.0.refuse.store(true, Ordering::SeqCst);
    let access = McpAttemptAccess::Admitted(&work);
    assert!(
        step.step
            .list_resources_with_authority(SERVER_NAME, None, access)
            .await
            .is_err()
    );
    assert!(
        step.step
            .list_resource_templates_with_authority(SERVER_NAME, None, access)
            .await
            .is_err()
    );
    assert!(
        step.step
            .read_resource_with_authority(
                SERVER_NAME,
                ReadResourceRequestParams::new("test://read"),
                access
            )
            .await
            .is_err()
    );
    assert!(
        step.step
            .list_all_resources_with_authority(|_| true, access)
            .await
            .by_server
            .is_empty()
    );
    assert!(
        step.step
            .list_all_resource_templates_with_authority(|_| true, access)
            .await
            .by_server
            .is_empty()
    );
    assert_eq!(server.calls.load(Ordering::SeqCst), 0);
    assert_eq!(work.0.derived.load(Ordering::SeqCst), 0);
    server.finish(&step.client).await;
}

#[tokio::test]
async fn ready_resource_operations_derive_and_release_each_invocation() {
    let (step, server) = fixture(McpAttemptRequirement::Required).await;
    let work = work();
    let access = McpAttemptAccess::Admitted(&work);
    step.step
        .list_resources_with_authority(SERVER_NAME, None, access)
        .await
        .unwrap();
    step.step
        .list_resource_templates_with_authority(SERVER_NAME, None, access)
        .await
        .unwrap();
    let error = step
        .step
        .read_resource_with_authority(
            SERVER_NAME,
            ReadResourceRequestParams::new("test://read"),
            access,
        )
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("test read reached exact server"));
    assert_eq!(
        step.step
            .list_all_resources_with_authority(|_| true, access)
            .await
            .by_server
            .len(),
        1
    );
    assert_eq!(
        step.step
            .list_all_resource_templates_with_authority(|_| true, access)
            .await
            .by_server
            .len(),
        1
    );
    assert_eq!(server.calls.load(Ordering::SeqCst), 5);
    assert_eq!(work.0.derived.load(Ordering::SeqCst), 5);
    assert_eq!(
        work.0.live.load(Ordering::SeqCst),
        1,
        "idle binding retains no invocation work"
    );
    let counts = work.0.clone();
    drop(work);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    server.finish(&step.client).await;
}

#[tokio::test]
async fn resource_fanout_owns_work_until_cancelled_task_is_dropped() {
    let (step, server) = fixture(McpAttemptRequirement::Required).await;
    server.hold.store(true, Ordering::SeqCst);
    let work = work();
    let counts = work.0.clone();
    let binding = step.step.clone();
    let request = tokio::spawn(async move {
        binding
            .list_all_resources_with_authority(|_| true, McpAttemptAccess::Admitted(&work))
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), server.started.notified())
        .await
        .unwrap();
    assert_eq!(
        counts.live.load(Ordering::SeqCst),
        2,
        "parent plus independently owned fanout task"
    );
    request.abort();
    assert!(matches!(request.await, Err(error) if error.is_cancelled()));
    tokio::time::timeout(Duration::from_secs(5), async {
        while counts.live.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(server.calls.load(Ordering::SeqCst), 1);
    server.finish(&step.client).await;
}

#[tokio::test]
async fn resource_filters_preserve_deferred_names_without_deriving_or_dispatching() {
    let (step, server) = fixture(McpAttemptRequirement::Required).await;
    let clients = McpBindingClients::new(
        HashMap::from([(
            SERVER_NAME.to_owned(),
            step.step.clients.client(SERVER_NAME).unwrap(),
        )]),
        vec!["later".into(), "hidden".into()],
    );
    let work = work();
    let result = clients
        .list_all_resources(
            |name| name == "later",
            McpAttemptRequirement::Required,
            McpAttemptAccess::Admitted(&work),
        )
        .await;
    assert!(result.by_server.is_empty());
    assert_eq!(result.deferred_servers, vec!["later"]);
    assert_eq!(work.0.derived.load(Ordering::SeqCst), 0);
    assert_eq!(server.calls.load(Ordering::SeqCst), 0);
    server.finish(&step.client).await;
}

async fn status_manager(step: &TestStep) -> McpConnectionSet {
    let mut manager = McpConnectionSet::empty(true);
    manager.attempt_requirement = McpAttemptRequirement::Required;
    let managed = step
        .step
        .prepare_call(SERVER_NAME, TOOL_NAME)
        .unwrap()
        .client;
    let mut client =
        crate::connection_manager::tests::create_ready_async_managed_client(Vec::new()).await;
    client.client = futures::future::ready(Ok((*managed).clone()))
        .boxed()
        .shared()
        .into();
    manager.insert_test_client(SERVER_NAME, client);
    manager
}
#[tokio::test]
async fn status_collection_refuses_missing_and_exhausted_work_without_wire_requests() {
    let (step, server) = fixture(McpAttemptRequirement::Required).await;
    let manager = status_manager(&step).await;
    let root = work();
    for access in [
        McpAttemptAccess::Unscoped,
        McpAttemptAccess::Admitted(&root),
    ] {
        root.0.refuse.store(true, Ordering::SeqCst);
        let error = crate::mcp::collect_mcp_server_status_snapshot_from_manager(
            &manager,
            HashMap::new(),
            vec![SERVER_NAME.into()],
            crate::McpSnapshotDetail::Full,
            access,
        )
        .await
        .err()
        .unwrap();
        assert!(error.is::<McpAttemptRefused>());
        assert_eq!(server.calls.load(Ordering::SeqCst), 0);
        assert_eq!(root.0.live.load(Ordering::SeqCst), 1);
    }
    server.finish(&step.client).await;
}
#[tokio::test]
async fn admitted_status_collection_preserves_detail_filters_and_retains_no_work() {
    let (step, server) = fixture(McpAttemptRequirement::Required).await;
    let manager = status_manager(&step).await;
    let root = work();
    let access = McpAttemptAccess::Admitted(&root);
    let snapshot = crate::mcp::collect_mcp_server_status_snapshot_from_manager(
        &manager,
        HashMap::new(),
        vec![SERVER_NAME.into()],
        crate::McpSnapshotDetail::Full,
        access,
    )
    .await
    .unwrap();
    assert_eq!(server.calls.load(Ordering::SeqCst), 2);
    assert!(snapshot.resources.contains_key(SERVER_NAME));
    assert!(snapshot.resource_templates.contains_key(SERVER_NAME));
    assert_eq!(root.0.live.load(Ordering::SeqCst), 1);
    let tools = crate::mcp::collect_mcp_server_status_snapshot_from_manager(
        &manager,
        HashMap::new(),
        vec![SERVER_NAME.into()],
        crate::McpSnapshotDetail::ToolsAndAuthOnly,
        access,
    )
    .await
    .unwrap();
    assert!(tools.resources.is_empty() && tools.resource_templates.is_empty());
    assert_eq!(server.calls.load(Ordering::SeqCst), 2);
    let filtered = crate::mcp::collect_mcp_server_status_snapshot_from_manager(
        &manager,
        HashMap::new(),
        vec!["absent".into()],
        crate::McpSnapshotDetail::Full,
        access,
    )
    .await
    .unwrap();
    assert!(filtered.resources.is_empty() && filtered.resource_templates.is_empty());
    assert_eq!(server.calls.load(Ordering::SeqCst), 2);
    let counts = root.0.clone();
    drop(root);
    assert_eq!(
        counts.live.load(Ordering::SeqCst),
        0,
        "retained status snapshot owns no work"
    );
    assert_eq!(snapshot.server_names, vec![SERVER_NAME]);
    server.finish(&step.client).await;
}
#[tokio::test]
async fn cancelled_status_collection_keeps_fanout_work_until_task_destruction() {
    let (step, server) = fixture(McpAttemptRequirement::Required).await;
    let manager = status_manager(&step).await;
    let root = work();
    server.hold.store(true, Ordering::SeqCst);
    let mut status = Box::pin(crate::mcp::collect_mcp_server_status_snapshot_from_manager(
        &manager,
        HashMap::new(),
        vec![SERVER_NAME.into()],
        crate::McpSnapshotDetail::Full,
        McpAttemptAccess::Admitted(&root),
    ));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            assert!(futures::poll!(&mut status).is_pending());
            if server.calls.load(Ordering::SeqCst) == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        root.0.live.load(Ordering::SeqCst) >= 4,
        "root, collector and each live fanout have separate work"
    );
    drop(status);
    tokio::time::timeout(Duration::from_secs(5), async {
        while root.0.live.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(server.calls.load(Ordering::SeqCst), 2);
    // Teardown releases both remote handlers only after local task ownership was checked.
    server.finish_request.notify_waiters();
    server.finish(&step.client).await;
}
#[tokio::test]
async fn direct_ready_resource_operations_refuse_without_authority_and_release_valid_work() {
    let (step, server) = fixture(McpAttemptRequirement::Required).await;
    let manager = status_manager(&step).await;
    let root = work();
    for access in [
        McpAttemptAccess::Unscoped,
        McpAttemptAccess::Admitted(&root),
    ] {
        root.0.refuse.store(true, Ordering::SeqCst);
        assert!(
            manager
                .list_resources_with_authority(SERVER_NAME, None, access)
                .await
                .unwrap_err()
                .is::<McpAttemptRefused>()
        );
        assert!(
            manager
                .list_resource_templates_with_authority(SERVER_NAME, None, access)
                .await
                .unwrap_err()
                .is::<McpAttemptRefused>()
        );
        assert!(
            manager
                .read_resource_with_authority(
                    SERVER_NAME,
                    ReadResourceRequestParams::new("test://read"),
                    access
                )
                .await
                .unwrap_err()
                .is::<McpAttemptRefused>()
        );
        assert_eq!(server.calls.load(Ordering::SeqCst), 0);
        assert_eq!(root.0.live.load(Ordering::SeqCst), 1);
    }
    root.0.refuse.store(false, Ordering::SeqCst);
    let access = McpAttemptAccess::Admitted(&root);
    manager
        .list_resources_with_authority(SERVER_NAME, None, access)
        .await
        .unwrap();
    manager
        .list_resource_templates_with_authority(SERVER_NAME, None, access)
        .await
        .unwrap();
    let error = manager
        .read_resource_with_authority(
            SERVER_NAME,
            ReadResourceRequestParams::new("test://read"),
            access,
        )
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("test read reached exact server"));
    assert_eq!(server.calls.load(Ordering::SeqCst), 3);
    assert_eq!(root.0.live.load(Ordering::SeqCst), 1);
    server.finish(&step.client).await;
}
