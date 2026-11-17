//! Prepared-call invocation custody. Local wire peer, no account/provider/OS sandbox.
use super::*;
use crate::McpAttemptAccess;
use crate::McpAttemptRefused;
use crate::McpAttemptRequirement;
use crate::McpAttemptWork;
use pretty_assertions::assert_eq;
use rmcp::RoleServer;
use rmcp::ServerHandler;
use rmcp::ServiceExt;
use rmcp::model::CallToolRequestParams;
use rmcp::model::CallToolResponse;
use rmcp::model::ClientCapabilities;
use rmcp::model::Implementation;
use rmcp::model::InitializeRequestParams;
use rmcp::model::ServerCapabilities;
use rmcp::model::ServerConfig;
use rmcp::service::RequestContext;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
#[derive(Default)]
struct Counts {
    live: AtomicUsize,
    refuse: AtomicBool,
}
struct Work(Arc<Counts>);
impl Work {
    fn new(c: &Arc<Counts>) -> Self {
        c.live.fetch_add(1, Ordering::SeqCst);
        Self(c.clone())
    }
}
impl Drop for Work {
    fn drop(&mut self) {
        self.0.live.fetch_sub(1, Ordering::SeqCst);
    }
}
impl McpAttemptWork for Work {
    fn derive_attempt(&self) -> Result<Box<dyn McpAttemptWork>, McpAttemptRefused> {
        if self.0.refuse.load(Ordering::SeqCst) {
            return Err(McpAttemptRefused);
        }
        Ok(Box::new(Self::new(&self.0)))
    }
}
#[derive(Clone, Default)]
struct Server {
    calls: Arc<AtomicUsize>,
    hold: Arc<AtomicBool>,
    started: Arc<Notify>,
    release: Arc<Notify>,
    tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}
impl ServerHandler for Server {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
    }
    async fn call_tool(
        &self,
        _: CallToolRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, rmcp::ErrorData> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        if self.hold.load(Ordering::SeqCst) {
            self.release.notified().await;
        }
        Ok(rmcp::model::CallToolResult::success(Vec::new()).into())
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
                    .unwrap()
                    .waiting()
                    .await
                    .unwrap();
            });
            server.tasks.lock().unwrap().push(task);
            Ok(client)
        }
        .boxed()
    }
}
async fn fixture(required: bool) -> (TestStep, Server) {
    let server = Server::default();
    let step = test_step_with_transport(
        "tool-work",
        AppToolApproval::Approve,
        false,
        Arc::new(server.clone()),
        if required {
            McpAttemptRequirement::Required
        } else {
            McpAttemptRequirement::Ungated
        },
    )
    .await;
    step.client
        .initialize(
            InitializeRequestParams::new(
                ClientCapabilities::default(),
                Implementation::new("tool-work-test", "1"),
            ),
            Some(Duration::from_secs(5)),
            Box::new(|_, _| async { Err(anyhow::anyhow!("unexpected elicitation")) }.boxed()),
        )
        .await
        .unwrap();
    (step, server)
}
async fn finish(step: &TestStep, server: &Server) {
    // Only teardown releases the remote hold. Local cancellation below proves no remote stop.
    server.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), step.client.shutdown())
        .await
        .unwrap();
    let tasks = std::mem::take(&mut *server.tasks.lock().unwrap());
    for task in tasks {
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
    }
}
#[tokio::test]
async fn required_prepared_calls_refuse_before_catalog_waits_or_preparation() {
    let (step, server) = fixture(true).await;
    let counts = Arc::new(Counts::default());
    let root = Work::new(&counts);
    let prepared = step.step.prepare_call(SERVER_NAME, TOOL_NAME).unwrap();
    let mut first = Box::pin(prepared.call_with_preparation_and_authority(
        None,
        McpAttemptAccess::Admitted(&root),
        || std::future::pending(),
    ));
    assert!(futures::poll!(&mut first).is_pending());
    let mut refresh = Box::pin(step.tool_catalog.refresh(
        || async { Ok((step.step.tools().to_vec(), None, ())) },
        || Some(()),
        |_, _, ()| {},
    ));
    assert!(futures::poll!(&mut refresh).is_pending());
    let ran = AtomicBool::new(false);
    for access in [
        McpAttemptAccess::Unscoped,
        McpAttemptAccess::Admitted(&root),
    ] {
        counts.refuse.store(true, Ordering::SeqCst);
        let mut refused =
            Box::pin(
                prepared.call_with_preparation_and_authority(None, access, || async {
                    ran.store(true, Ordering::SeqCst);
                    Ok((None, None))
                }),
            );
        let std::task::Poll::Ready(Err(error)) = futures::poll!(&mut refused) else {
            panic!("refusal must precede blocked catalog")
        };
        assert!(error.is::<McpAttemptRefused>());
    }
    assert!(!ran.load(Ordering::SeqCst));
    assert_eq!(server.calls.load(Ordering::SeqCst), 0);
    assert_eq!(counts.live.load(Ordering::SeqCst), 2);
    drop(refresh);
    drop(first);
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    let error = prepared.call(None, None, None).await.unwrap_err();
    assert!(error.is::<McpAttemptRefused>());
    finish(&step, &server).await;
}
#[tokio::test]
async fn cancelled_catalog_waiter_releases_only_its_invocation_work() {
    let (step, server) = fixture(true).await;
    let counts = Arc::new(Counts::default());
    let root = Work::new(&counts);
    let prepared = step.step.prepare_call(SERVER_NAME, TOOL_NAME).unwrap();
    let mut first = Box::pin(prepared.call_with_preparation_and_authority(
        None,
        McpAttemptAccess::Admitted(&root),
        || std::future::pending(),
    ));
    assert!(futures::poll!(&mut first).is_pending());
    let mut refresh = Box::pin(step.tool_catalog.refresh(
        || async { Ok((step.step.tools().to_vec(), None, ())) },
        || Some(()),
        |_, _, ()| {},
    ));
    assert!(futures::poll!(&mut refresh).is_pending());
    let ran = AtomicBool::new(false);
    let mut waiter = Box::pin(prepared.call_with_preparation_and_authority(
        None,
        McpAttemptAccess::Admitted(&root),
        || async {
            ran.store(true, Ordering::SeqCst);
            Ok((None, None))
        },
    ));
    assert!(futures::poll!(&mut waiter).is_pending());
    assert_eq!(counts.live.load(Ordering::SeqCst), 3);
    drop(waiter);
    assert_eq!(counts.live.load(Ordering::SeqCst), 2);
    assert!(!ran.load(Ordering::SeqCst));
    drop(refresh);
    drop(first);
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    assert_eq!(server.calls.load(Ordering::SeqCst), 0);
    finish(&step, &server).await;
}
#[tokio::test]
async fn admitted_preparation_failure_and_success_leave_no_work_in_retained_calls() {
    let (step, server) = fixture(true).await;
    let counts = Arc::new(Counts::default());
    let root = Work::new(&counts);
    let prepared = step.step.prepare_call(SERVER_NAME, TOOL_NAME).unwrap();
    assert!(
        prepared
            .call_with_preparation_and_authority(
                None,
                McpAttemptAccess::Admitted(&root),
                || async { Err(anyhow::anyhow!("stop in admitted preparation")) }
            )
            .await
            .is_err()
    );
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    assert_eq!(server.calls.load(Ordering::SeqCst), 0);
    prepared
        .call_with_authority(
            None,
            None,
            Some(Duration::from_secs(5)),
            McpAttemptAccess::Admitted(&root),
        )
        .await
        .unwrap();
    assert_eq!(server.calls.load(Ordering::SeqCst), 1);
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    drop(root);
    assert_eq!(
        counts.live.load(Ordering::SeqCst),
        0,
        "retained binding/call contain no work"
    );
    finish(&step, &server).await;
}
#[tokio::test(start_paused = true)]
async fn cancelled_and_timed_out_wire_calls_release_local_invocation_work() {
    for timeout in [false, true] {
        let (step, server) = fixture(true).await;
        let counts = Arc::new(Counts::default());
        let root = Arc::new(Work::new(&counts));
        let prepared = step.step.prepare_call(SERVER_NAME, TOOL_NAME).unwrap();
        server.hold.store(true, Ordering::SeqCst);
        let work = root.clone();
        let call = tokio::spawn(async move {
            prepared
                .call_with_authority(
                    None,
                    None,
                    Some(if timeout {
                        Duration::from_secs(1)
                    } else {
                        Duration::from_secs(5)
                    }),
                    McpAttemptAccess::Admitted(work.as_ref()),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), server.started.notified())
            .await
            .unwrap();
        assert_eq!(counts.live.load(Ordering::SeqCst), 2);
        if timeout {
            tokio::time::advance(Duration::from_secs(1)).await;
            assert!(
                tokio::time::timeout(Duration::from_secs(5), call)
                    .await
                    .unwrap()
                    .unwrap()
                    .is_err()
            );
        } else {
            call.abort();
            assert!(call.await.unwrap_err().is_cancelled());
        }
        assert_eq!(counts.live.load(Ordering::SeqCst), 1);
        assert_eq!(server.calls.load(Ordering::SeqCst), 1);
        finish(&step, &server).await;
        drop(root);
        assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    }
}
#[tokio::test]
async fn ungated_prepared_calls_keep_the_legacy_success_path() {
    let (step, server) = fixture(false).await;
    let prepared = step.step.prepare_call(SERVER_NAME, TOOL_NAME).unwrap();
    prepared
        .call(None, None, Some(Duration::from_secs(5)))
        .await
        .unwrap();
    assert_eq!(server.calls.load(Ordering::SeqCst), 1);
    finish(&step, &server).await;
}
