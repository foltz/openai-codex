//! Actual spawned tool dispatch and callback custody with synthetic account work.
use super::*;
use crate::session::step_context::StepContext;
use crate::tools::context::FunctionToolOutput;
use crate::tools::context::ToolInvocation;
use crate::tools::registry::CoreToolRuntime;
use crate::tools::registry::ToolExecutor;
use crate::tools::registry::ToolRegistry;
use crate::tools::router::ToolRouter;
use crate::turn_diff_tracker::TurnDiffTracker;
use codex_extension_api::ExtensionData;
use codex_extension_api::ExtensionFuture;
use codex_extension_api::HostOperationWork;
use codex_extension_api::ToolCallOutcome;
use codex_extension_api::TurnStartAdmission;
use codex_extension_api::TurnWorkRefused;
use codex_protocol::host_turn_work::HostTurnWork;
use codex_protocol::openai_models::ToolMode;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Debug)]
struct State {
    live: AtomicUsize,
    captures: AtomicUsize,
    children: AtomicUsize,
    requests: AtomicUsize,
    mode: AtomicUsize,
    readiness_calls: AtomicUsize,
    handlers: AtomicUsize,
    hold_ready: AtomicBool,
    handler_mode: AtomicUsize,
    hold_finish: AtomicBool,
    ready_entered: Notify,
    handler_entered: Notify,
    finish_entered: Notify,
    allow_finish: Notify,
    dropped: Notify,
    payload_drops: AtomicUsize,
    min_live_on_payload_drop: AtomicUsize,
    outcomes: Mutex<Vec<ToolCallOutcome>>,
}
impl Default for State {
    fn default() -> Self {
        Self {
            live: AtomicUsize::new(0),
            captures: AtomicUsize::new(0),
            children: AtomicUsize::new(0),
            requests: AtomicUsize::new(0),
            mode: AtomicUsize::new(0),
            readiness_calls: AtomicUsize::new(0),
            handlers: AtomicUsize::new(0),
            hold_ready: AtomicBool::new(false),
            handler_mode: AtomicUsize::new(0),
            hold_finish: AtomicBool::new(false),
            ready_entered: Notify::new(),
            handler_entered: Notify::new(),
            finish_entered: Notify::new(),
            allow_finish: Notify::new(),
            dropped: Notify::new(),
            payload_drops: AtomicUsize::new(0),
            min_live_on_payload_drop: AtomicUsize::new(usize::MAX),
            outcomes: Mutex::new(Vec::new()),
        }
    }
}
#[derive(Debug)]
struct Work(Arc<State>);
impl Work {
    fn new(state: &Arc<State>) -> Self {
        state.live.fetch_add(1, Ordering::SeqCst);
        Self(state.clone())
    }
}
impl Drop for Work {
    fn drop(&mut self) {
        assert!(self.0.live.fetch_sub(1, Ordering::SeqCst) > 0);
    }
}
impl HostOperationWork for Work {
    fn derive_operation(&self) -> Result<Box<dyn HostOperationWork>, TurnWorkRefused> {
        self.0.children.fetch_add(1, Ordering::SeqCst);
        if self.0.mode.load(Ordering::SeqCst) == 3 {
            return Err(TurnWorkRefused::Unavailable);
        }
        Ok(Box::new(Self::new(&self.0)))
    }
    fn derive_turn_work(
        &self,
        _: &ExtensionData,
        _: ExtensionFuture<'static, ()>,
    ) -> Result<Box<dyn HostTurnWork>, TurnWorkRefused> {
        panic!("a tool dispatch does not start a turn")
    }
}
#[derive(Debug)]
struct Host {
    state: Arc<State>,
    thread: String,
    turn: String,
}
impl TurnStartAdmission for Host {
    fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
        panic!("no fresh turn admission")
    }
    fn requires_account_work(&self) -> bool {
        true
    }
    fn admit_operation_work(&self) -> Result<Option<Box<dyn HostOperationWork>>, TurnWorkRefused> {
        panic!("no fresh operation admission")
    }
    fn derive_request_operation_work(
        &self,
    ) -> Result<Option<Box<dyn HostOperationWork>>, TurnWorkRefused> {
        self.state.requests.fetch_add(1, Ordering::SeqCst);
        panic!("must capture the exact turn, not a request")
    }
    fn derive_operation_work(
        &self,
        store: &ExtensionData,
        turn: &str,
    ) -> Result<Option<Box<dyn HostOperationWork>>, TurnWorkRefused> {
        assert_eq!(store.level_id(), self.thread);
        assert_eq!(turn, self.turn);
        self.state.captures.fetch_add(1, Ordering::SeqCst);
        match self.state.mode.load(Ordering::SeqCst) {
            1 => Err(TurnWorkRefused::Unavailable),
            2 => Ok(None),
            4 => Err(TurnWorkRefused::RetryAfter(
                Box::pin(std::future::pending()),
            )),
            _ => Ok(Some(Box::new(Work::new(&self.state)))),
        }
    }
}
struct PayloadDrop(Arc<State>);
impl Drop for PayloadDrop {
    fn drop(&mut self) {
        self.0
            .min_live_on_payload_drop
            .fetch_min(self.0.live.load(Ordering::SeqCst), Ordering::SeqCst);
        self.0.payload_drops.fetch_add(1, Ordering::SeqCst);
        self.0.dropped.notify_one();
    }
}
struct Handler(Arc<State>);
impl ToolExecutor<ToolInvocation> for Handler {
    fn tool_name(&self) -> codex_tools::ToolName {
        codex_tools::ToolName::plain("owned_tool")
    }
    fn spec(&self) -> codex_tools::ToolSpec {
        codex_tools::ToolSpec::Function(codex_tools::ResponsesApiTool {
            name: "owned_tool".into(),
            description: "Finite original dispatch fixture".into(),
            strict: false,
            defer_loading: None,
            parameters: codex_tools::JsonSchema::default(),
            output_schema: None,
        })
    }
    fn handle<'a>(&'a self, _: ToolInvocation) -> codex_tools::ToolExecutorFuture<'a>
    where
        ToolInvocation: 'a,
    {
        Box::pin(async move {
            let _payload = PayloadDrop(self.0.clone());
            self.0.handlers.fetch_add(1, Ordering::SeqCst);
            self.0.handler_entered.notify_one();
            match self.0.handler_mode.load(Ordering::SeqCst) {
                1 => std::future::pending::<()>().await,
                2 => panic!("intentional original tool task panic"),
                _ => {}
            }
            Ok(
                Box::new(FunctionToolOutput::from_text("ok".into(), Some(true)))
                    as Box<dyn crate::tools::context::ToolOutput>,
            )
        })
    }
}
impl CoreToolRuntime for Handler {
    fn wait_until_ready<'a>(
        &'a self,
        _: &'a Arc<Session>,
        _: &'a crate::session::turn_context::TurnContext,
    ) -> Option<futures::future::BoxFuture<'a, ()>> {
        self.0.readiness_calls.fetch_add(1, Ordering::SeqCst);
        self.0.hold_ready.load(Ordering::SeqCst).then(|| {
            Box::pin(async move {
                let _payload = PayloadDrop(self.0.clone());
                self.0.ready_entered.notify_one();
                std::future::pending::<()>().await;
            }) as futures::future::BoxFuture<'a, ()>
        })
    }
}
struct Finish(Arc<State>);
impl codex_extension_api::ToolLifecycleContributor for Finish {
    fn on_tool_finish<'a>(
        &'a self,
        input: codex_extension_api::ToolFinishInput<'a>,
    ) -> codex_extension_api::ToolLifecycleFuture<'a> {
        Box::pin(async move {
            let _payload = PayloadDrop(self.0.clone());
            self.0.finish_entered.notify_one();
            if self.0.hold_finish.load(Ordering::SeqCst) {
                self.0.allow_finish.notified().await;
            }
            self.0.outcomes.lock().unwrap().push(input.outcome);
        })
    }
}
async fn fixture(with_finish: bool) -> (ToolCallRuntime, Arc<State>) {
    let (mut session, turn) = crate::session::tests::make_session_and_context().await;
    let state = Arc::new(State::default());
    session.services.host_admission = Some(Arc::new(Host {
        state: state.clone(),
        thread: session.services.thread_extension_data.level_id().into(),
        turn: turn.sub_id.clone(),
    }));
    let mut extensions = codex_extension_api::ExtensionRegistryBuilder::new();
    if with_finish {
        extensions.tool_lifecycle_contributor(Arc::new(Finish(state.clone())));
    }
    session.services.extensions = Arc::new(extensions.build());
    let router = ToolRouter::from_parts(
        ToolRegistry::with_handler_for_test(Arc::new(Handler(state.clone()))),
        Vec::new(),
        ToolMode::Direct,
        BTreeMap::new(),
        None,
        None,
        &[],
    );
    let step = StepContext::for_test(Arc::new(turn)).with_tool_router_for_test(Arc::new(router));
    (
        ToolCallRuntime::new(
            Arc::new(session),
            step,
            Arc::new(tokio::sync::Mutex::new(TurnDiffTracker::new())),
        ),
        state,
    )
}
fn run(
    runtime: ToolCallRuntime,
    source: ToolCallSource,
    token: CancellationToken,
) -> impl std::future::Future<Output = Result<AnyToolResult, FunctionCallError>> {
    let step = runtime.step_context.clone();
    runtime.handle_tool_call_with_source(
        step,
        ToolCall {
            tool_name: codex_tools::ToolName::plain("owned_tool"),
            call_id: "owned-call".into(),
            payload: ToolPayload::Function {
                arguments: "{}".into(),
            },
            encrypted_function_args: None,
            native_call: None,
        },
        source,
        token,
        Arc::default(),
    )
}
async fn zero(state: &State) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while state.live.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn required_missing_refused_retry_or_exhausted_tool_work_never_spawns() {
    for mode in [1, 2, 3, 4] {
        let (runtime, state) = fixture(true).await;
        state.mode.store(mode, Ordering::SeqCst);
        let result = run(
            runtime.clone(),
            ToolCallSource::Direct,
            CancellationToken::new(),
        )
        .await;
        assert!(
            matches!(result, Err(FunctionCallError::RespondToModel(ref message)) if message == "tool account work is unavailable")
        );
        assert_eq!(state.captures.load(Ordering::SeqCst), 1);
        assert_eq!(state.live.load(Ordering::SeqCst), 0);
        assert_eq!(state.requests.load(Ordering::SeqCst), 0);
        assert_eq!(state.readiness_calls.load(Ordering::SeqCst), 0);
        assert_eq!(state.handlers.load(Ordering::SeqCst), 0);
        assert!(state.outcomes.lock().unwrap().is_empty());
    }
}
#[tokio::test(flavor = "current_thread")]
async fn tool_work_is_captured_before_poll_and_dispatch_outlives_dropped_waiter_until_destruction()
{
    let (runtime, state) = fixture(false).await;
    state.hold_ready.store(true, Ordering::SeqCst);
    let call = run(
        runtime.clone(),
        ToolCallSource::Direct,
        CancellationToken::new(),
    );
    assert_eq!(state.captures.load(Ordering::SeqCst), 1);
    assert_eq!(state.children.load(Ordering::SeqCst), 1);
    assert_eq!(state.live.load(Ordering::SeqCst), 2);
    tokio::time::timeout(Duration::from_secs(5), state.ready_entered.notified())
        .await
        .unwrap();
    drop(call);
    assert_eq!(
        state.live.load(Ordering::SeqCst),
        usize::from(state.payload_drops.load(Ordering::SeqCst) == 0),
        "the dispatch lease can disappear only after actual payload destruction"
    );
    tokio::time::timeout(Duration::from_secs(5), state.dropped.notified())
        .await
        .unwrap();
    zero(&state).await;
    assert!(state.min_live_on_payload_drop.load(Ordering::SeqCst) >= 1);
    assert_eq!(state.handlers.load(Ordering::SeqCst), 0);
    assert_eq!(state.requests.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn completed_tool_finish_callback_keeps_original_work_through_cancellation() {
    let (runtime, state) = fixture(true).await;
    state.hold_finish.store(true, Ordering::SeqCst);
    let token = CancellationToken::new();
    let caller = tokio::spawn(run(runtime.clone(), ToolCallSource::Direct, token.clone()));
    tokio::time::timeout(Duration::from_secs(5), state.finish_entered.notified())
        .await
        .unwrap();
    assert_eq!(state.live.load(Ordering::SeqCst), 2);
    token.cancel();
    tokio::task::yield_now().await;
    assert!(!caller.is_finished());
    assert_eq!(state.live.load(Ordering::SeqCst), 2);
    state.allow_finish.notify_one();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), caller)
            .await
            .unwrap()
            .unwrap()
            .is_ok()
    );
    assert_eq!(
        *state.outcomes.lock().unwrap(),
        vec![ToolCallOutcome::Completed { success: true }]
    );
    assert_eq!(state.live.load(Ordering::SeqCst), 0);
    assert!(state.min_live_on_payload_drop.load(Ordering::SeqCst) >= 1);
}
#[tokio::test]
async fn abort_callback_keeps_completion_work_after_original_dispatch_is_joined() {
    let (runtime, state) = fixture(true).await;
    state.handler_mode.store(1, Ordering::SeqCst);
    state.hold_finish.store(true, Ordering::SeqCst);
    let token = CancellationToken::new();
    let caller = tokio::spawn(run(runtime.clone(), ToolCallSource::Direct, token.clone()));
    tokio::time::timeout(Duration::from_secs(5), state.handler_entered.notified())
        .await
        .unwrap();
    assert_eq!(state.live.load(Ordering::SeqCst), 2);
    token.cancel();
    tokio::time::timeout(Duration::from_secs(5), state.finish_entered.notified())
        .await
        .unwrap();
    assert_eq!(state.live.load(Ordering::SeqCst), 1);
    assert_eq!(state.payload_drops.load(Ordering::SeqCst), 1);
    state.allow_finish.notify_one();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), caller)
            .await
            .unwrap()
            .unwrap()
            .is_ok()
    );
    assert_eq!(
        *state.outcomes.lock().unwrap(),
        vec![ToolCallOutcome::Aborted]
    );
    assert_eq!(state.live.load(Ordering::SeqCst), 0);
    assert!(state.min_live_on_payload_drop.load(Ordering::SeqCst) >= 1);
}
#[tokio::test]
async fn original_tool_panic_is_observed_and_payload_drops_before_account_work() {
    let (runtime, state) = fixture(false).await;
    state.handler_mode.store(2, Ordering::SeqCst);
    let result = run(
        runtime.clone(),
        ToolCallSource::Direct,
        CancellationToken::new(),
    )
    .await;
    assert!(
        matches!(result, Err(FunctionCallError::Fatal(ref message)) if message.contains("tool task failed to receive") && message.contains("panic"))
    );
    assert_eq!(state.live.load(Ordering::SeqCst), 0);
    assert!(state.min_live_on_payload_drop.load(Ordering::SeqCst) >= 1);
    assert_eq!(state.handlers.load(Ordering::SeqCst), 1);
}
struct UnpolledPayload {
    _payload: PayloadDrop,
}
impl std::future::Future for UnpolledPayload {
    type Output = ();
    fn poll(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        std::task::Poll::Pending
    }
}
#[test]
fn unpolled_tool_payload_is_destroyed_before_its_account_operation() {
    let state = Arc::new(State::default());
    let work = Work::new(&state);
    let future = ToolWorkFuture::new(
        UnpolledPayload {
            _payload: PayloadDrop(state.clone()),
        },
        Some(Box::new(work)),
    );
    drop(future);
    assert_eq!(state.payload_drops.load(Ordering::SeqCst), 1);
    assert_eq!(state.min_live_on_payload_drop.load(Ordering::SeqCst), 1);
    assert_eq!(state.live.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn direct_and_nested_tagged_dispatch_keep_no_lease_on_reusable_runtime() {
    let (runtime, state) = fixture(true).await;
    for source in [
        ToolCallSource::Direct,
        ToolCallSource::CodeMode {
            cell_id: "cell".into(),
            runtime_tool_call_id: "nested".into(),
        },
    ] {
        let result = run(runtime.clone(), source, CancellationToken::new())
            .await
            .unwrap();
        assert!(result.result.success_for_logging());
        assert_eq!(state.live.load(Ordering::SeqCst), 0);
    }
    assert_eq!(state.handlers.load(Ordering::SeqCst), 2);
    assert_eq!(state.requests.load(Ordering::SeqCst), 0);
    assert_eq!(
        *state.outcomes.lock().unwrap(),
        vec![
            ToolCallOutcome::Completed { success: true },
            ToolCallOutcome::Completed { success: true }
        ]
    );
}

#[tokio::test]
async fn standalone_router_keeps_exact_turn_work_through_callback_cancellation() {
    let (runtime, state) = fixture(true).await;
    state.hold_finish.store(true, Ordering::SeqCst);
    let router = runtime.step_context.tool_router.clone();
    let session = runtime.session.clone();
    let step = runtime.step_context.clone();
    let tracker = runtime.tracker.clone();
    let caller = tokio::spawn(async move {
        router
            .dispatch_tool_call_with_code_mode_result(
                session,
                step,
                CancellationToken::new(),
                tracker,
                ToolCall {
                    tool_name: codex_tools::ToolName::plain("owned_tool"),
                    call_id: "standalone".into(),
                    payload: ToolPayload::Function {
                        arguments: "{}".into(),
                    },
                    encrypted_function_args: None,
                    native_call: None,
                },
                ToolCallSource::Direct,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), state.finish_entered.notified())
        .await
        .unwrap();
    assert_eq!(state.live.load(Ordering::SeqCst), 1);
    assert_eq!(state.requests.load(Ordering::SeqCst), 0);
    caller.abort();
    assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
    assert_eq!(state.live.load(Ordering::SeqCst), 0);
    assert!(state.min_live_on_payload_drop.load(Ordering::SeqCst) >= 1);
    assert!(state.outcomes.lock().unwrap().is_empty());
}
