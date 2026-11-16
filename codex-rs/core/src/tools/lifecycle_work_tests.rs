//! Actual Core tool-start dispatch with finite synthetic owners, no provider.
use super::*;
use codex_extension_api::{
    ExtensionFuture, HostOperationWork, ToolLifecycleContributor, TurnStartAdmission,
    TurnWorkRefused,
};
use codex_mcp::{McpAttemptAccess, McpAttemptWork};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Default, Debug)]
struct Counts {
    live: AtomicUsize,
    turns: AtomicUsize,
    requests: AtomicUsize,
    refuse: AtomicBool,
    missing: AtomicBool,
}
#[derive(Debug)]
struct Work(Arc<Counts>);
impl Work {
    fn new(c: &Arc<Counts>) -> Self {
        c.live.fetch_add(1, Ordering::SeqCst);
        Self(Arc::clone(c))
    }
}
impl Drop for Work {
    fn drop(&mut self) {
        self.0.live.fetch_sub(1, Ordering::SeqCst);
    }
}
impl HostOperationWork for Work {
    fn derive_operation(&self) -> Result<Box<dyn HostOperationWork>, TurnWorkRefused> {
        if self.0.refuse.load(Ordering::SeqCst) {
            return Err(TurnWorkRefused::Unavailable);
        }
        Ok(Box::new(Self::new(&self.0)))
    }
    fn derive_turn_work(
        &self,
        _: &codex_extension_api::ExtensionData,
        _: ExtensionFuture<'static, ()>,
    ) -> Result<Box<dyn codex_protocol::host_turn_work::HostTurnWork>, TurnWorkRefused> {
        Err(TurnWorkRefused::Unavailable)
    }
}
#[derive(Debug)]
struct Host {
    counts: Arc<Counts>,
    thread: String,
    turn: String,
}
impl TurnStartAdmission for Host {
    fn requires_account_work(&self) -> bool {
        true
    }
    fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
        panic!("no fresh admission")
    }
    fn admit_operation_work(&self) -> Result<Option<Box<dyn HostOperationWork>>, TurnWorkRefused> {
        panic!("no fresh admission")
    }
    fn derive_request_operation_work(
        &self,
    ) -> Result<Option<Box<dyn HostOperationWork>>, TurnWorkRefused> {
        self.counts.requests.fetch_add(1, Ordering::SeqCst);
        panic!("must derive exact turn, never request")
    }
    fn derive_operation_work(
        &self,
        store: &codex_extension_api::ExtensionData,
        turn: &str,
    ) -> Result<Option<Box<dyn HostOperationWork>>, TurnWorkRefused> {
        assert_eq!(store.level_id(), self.thread);
        assert_eq!(turn, self.turn);
        self.counts.turns.fetch_add(1, Ordering::SeqCst);
        if self.counts.refuse.load(Ordering::SeqCst) {
            return Err(TurnWorkRefused::Unavailable);
        }
        if self.counts.missing.load(Ordering::SeqCst) {
            return Ok(None);
        }
        Ok(Some(Box::new(Work::new(&self.counts))))
    }
}
#[derive(Default)]
struct Callback {
    calls: AtomicUsize,
    refused: AtomicUsize,
    unscoped: AtomicUsize,
    park: AtomicBool,
    entered: tokio::sync::Notify,
    wait: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    worker: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}
impl ToolLifecycleContributor for Callback {
    fn on_tool_start<'a>(&'a self, input: ToolStartInput<'a>) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            let child: Option<Box<dyn McpAttemptWork>> = match input.mcp_access {
                Ok(McpAttemptAccess::Admitted(work)) => Some(work.derive_attempt().unwrap()),
                Ok(McpAttemptAccess::Unscoped) => {
                    self.unscoped.fetch_add(1, Ordering::SeqCst);
                    None
                }
                Err(_) => {
                    self.refused.fetch_add(1, Ordering::SeqCst);
                    None
                }
            };
            self.calls.fetch_add(1, Ordering::SeqCst);
            input.thread_store.insert(true); // metadata is not account work
            self.entered.notify_one();
            if self.park.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            let wait = self.wait.lock().unwrap().take();
            if let Some(wait) = wait {
                *self.worker.lock().unwrap() = Some(tokio::spawn(async move {
                    let _work = child;
                    let _ = wait.await;
                }));
            }
        })
    }
}
async fn invocation(
    callback: Option<&Arc<Callback>>,
    counts: Option<&Arc<Counts>>,
) -> ToolInvocation {
    let (mut session, turn) = crate::session::tests::make_session_and_context().await;
    if let Some(counts) = counts {
        session.services.host_admission = Some(Arc::new(Host {
            counts: Arc::clone(counts),
            thread: session.services.thread_extension_data.level_id().to_owned(),
            turn: turn.sub_id.clone(),
        }));
    }
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::new();
    if let Some(callback) = callback {
        builder.tool_lifecycle_contributor(callback.clone());
    }
    session.services.extensions = Arc::new(builder.build());
    let turn = Arc::new(turn);
    ToolInvocation {
        session: Arc::new(session),
        step_context: crate::session::step_context::StepContext::for_test(Arc::clone(&turn)),
        turn,
        cancellation_token: tokio_util::sync::CancellationToken::new(),
        tracker: Arc::new(tokio::sync::Mutex::new(
            crate::turn_diff_tracker::TurnDiffTracker::new(),
        )),
        call_id: "finite-tool-start".to_owned(),
        tool_name: ToolName::plain("echo"),
        source: ToolCallSource::Direct,
        native_call: None,
        environments: Default::default(),
        payload: codex_tools::ToolPayload::Function {
            arguments: "{}".to_owned(),
        },
    }
}
#[tokio::test]
async fn tool_start_captures_exact_turn_and_releases_work_without_cache_retention() {
    let c = Arc::new(Counts::default());
    let callback = Arc::new(Callback::default());
    let i = invocation(Some(&callback), Some(&c)).await;
    notify_tool_start(&i, None).await;
    assert_eq!(callback.calls.load(Ordering::SeqCst), 1);
    assert_eq!(callback.unscoped.load(Ordering::SeqCst), 0);
    assert_eq!(callback.refused.load(Ordering::SeqCst), 0);
    assert_eq!(c.turns.load(Ordering::SeqCst), 1);
    assert_eq!(c.requests.load(Ordering::SeqCst), 0);
    assert_eq!(c.live.load(Ordering::SeqCst), 0);
    assert!(
        *i.session
            .services
            .thread_extension_data
            .get::<bool>()
            .unwrap()
    );
}
#[tokio::test]
async fn tool_start_without_hooks_never_derives_or_admits_work() {
    let c = Arc::new(Counts::default());
    c.refuse.store(true, Ordering::SeqCst);
    let i = invocation(None, Some(&c)).await;
    notify_tool_start(&i, None).await;
    assert_eq!(c.turns.load(Ordering::SeqCst), 0);
    assert_eq!(c.requests.load(Ordering::SeqCst), 0);
    assert_eq!(c.live.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn tool_start_required_missing_and_exhausted_work_remain_refused() {
    for missing in [true, false] {
        let c = Arc::new(Counts::default());
        c.missing.store(missing, Ordering::SeqCst);
        c.refuse.store(!missing, Ordering::SeqCst);
        let callback = Arc::new(Callback::default());
        let i = invocation(Some(&callback), Some(&c)).await;
        notify_tool_start(&i, None).await;
        assert_eq!(callback.calls.load(Ordering::SeqCst), 1);
        assert_eq!(callback.refused.load(Ordering::SeqCst), 1);
        assert_eq!(callback.unscoped.load(Ordering::SeqCst), 0);
        assert_eq!(c.turns.load(Ordering::SeqCst), 1);
        assert_eq!(c.requests.load(Ordering::SeqCst), 0);
        assert_eq!(c.live.load(Ordering::SeqCst), 0);
    }
}
#[tokio::test]
async fn tool_start_explicit_ungated_callback_remains_ungated() {
    let callback = Arc::new(Callback::default());
    let i = invocation(Some(&callback), None).await;
    notify_tool_start(&i, None).await;
    assert_eq!(callback.calls.load(Ordering::SeqCst), 1);
    assert_eq!(callback.unscoped.load(Ordering::SeqCst), 1);
    assert_eq!(callback.refused.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn cancelling_tool_start_drops_only_its_finite_callback_work() {
    let c = Arc::new(Counts::default());
    let callback = Arc::new(Callback::default());
    callback.park.store(true, Ordering::SeqCst);
    let i = invocation(Some(&callback), Some(&c)).await;
    let mut run = Box::pin(notify_tool_start(&i, None));
    tokio::select! { biased; ()=&mut run=>panic!("callback must park"), ()=callback.entered.notified()=>{} }
    assert_eq!(c.live.load(Ordering::SeqCst), 2);
    drop(run);
    assert_eq!(c.live.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn tool_start_background_child_outlives_callback_until_actual_task_destruction() {
    let c = Arc::new(Counts::default());
    let callback = Arc::new(Callback::default());
    let (release, wait) = tokio::sync::oneshot::channel();
    *callback.wait.lock().unwrap() = Some(wait);
    let i = invocation(Some(&callback), Some(&c)).await;
    notify_tool_start(&i, None).await;
    assert_eq!(c.live.load(Ordering::SeqCst), 1);
    drop(i);
    assert_eq!(c.live.load(Ordering::SeqCst), 1);
    let worker = callback.worker.lock().unwrap().take().unwrap();
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    assert_eq!(c.live.load(Ordering::SeqCst), 0);
    assert!(release.send(()).is_err());
}
