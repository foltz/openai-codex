use super::*;
use crate::{McpAttemptAccess, McpAttemptRefused, McpAttemptRequirement, McpAttemptWork};
use rmcp::model::{
    ClientCapabilities, Implementation, InitializeRequestParams, ListResourceTemplatesResult,
    ListResourcesResult, PaginatedRequestParams, ReadResourceRequestParams, ReadResourceResult,
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
    tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}
impl Server {
    async fn observe(&self) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        if self.hold.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
    }
    async fn finish(&self, client: &RmcpClient) {
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
    ) -> Result<ReadResourceResult, rmcp::ErrorData> {
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
    assert!(
        step.step
            .read_resource_with_authority(
                SERVER_NAME,
                ReadResourceRequestParams::new("test://read"),
                access
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("test read reached exact server")
    );
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
    assert!(request.await.unwrap_err().is_cancelled());
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
