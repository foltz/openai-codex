//! Invocation custody and refusal before Core MCP refresh side effects.
//! Real Session refresh/permit/invalidation paths, synthetic account authority.
use super::*;
use codex_extension_api::{
    ExtensionData, ExtensionFuture, HostOperationWork, TurnStartAdmission, TurnWorkRefused,
};
use codex_mcp::{McpAttemptAccess, McpAttemptRefused, McpAttemptWork};
use pretty_assertions::assert_eq;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Debug, Default)]
struct Counts {
    live: Arc<AtomicUsize>,
    derives: AtomicUsize,
    refuse: AtomicBool,
    refuse_after: AtomicUsize,
    turns: AtomicUsize,
}
#[derive(Debug)]
struct Work(Arc<Counts>);
impl Work {
    fn new(counts: &Arc<Counts>) -> Self {
        counts.live.fetch_add(1, Ordering::SeqCst);
        Self(counts.clone())
    }
}
impl Drop for Work {
    fn drop(&mut self) {
        self.0.live.fetch_sub(1, Ordering::SeqCst);
    }
}
impl McpAttemptWork for Work {
    fn derive_attempt(&self) -> Result<Box<dyn McpAttemptWork>, McpAttemptRefused> {
        let limit = self.0.refuse_after.load(Ordering::SeqCst);
        if self.0.refuse.load(Ordering::SeqCst)
            || (limit != 0 && self.0.derives.load(Ordering::SeqCst) >= limit)
        {
            return Err(McpAttemptRefused);
        }
        self.0.derives.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(Self::new(&self.0)))
    }
}
impl HostOperationWork for Work {
    fn derive_operation(&self) -> Result<Box<dyn HostOperationWork>, TurnWorkRefused> {
        if self.0.refuse.load(Ordering::SeqCst) {
            return Err(TurnWorkRefused::Unavailable);
        }
        self.0.derives.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(Self::new(&self.0)))
    }
    fn derive_turn_work(
        &self,
        _: &ExtensionData,
        _: ExtensionFuture<'static, ()>,
    ) -> Result<Box<dyn codex_protocol::host_turn_work::HostTurnWork>, TurnWorkRefused> {
        Err(TurnWorkRefused::Unavailable)
    }
}
#[derive(Debug)]
struct Host {
    counts: Arc<Counts>,
    store: String,
    turn: String,
}
impl TurnStartAdmission for Host {
    fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
        panic!("no fresh admission fallback")
    }
    fn requires_account_work(&self) -> bool {
        true
    }
    fn derive_operation_work(
        &self,
        store: &ExtensionData,
        turn: &str,
    ) -> Result<Option<Box<dyn HostOperationWork>>, TurnWorkRefused> {
        self.counts.turns.fetch_add(1, Ordering::SeqCst);
        if self.counts.refuse.load(Ordering::SeqCst)
            || store.level_id() != self.store
            || turn != self.turn
        {
            return Err(TurnWorkRefused::Unavailable);
        }
        Ok(Some(Box::new(Work::new(&self.counts))))
    }
}
async fn fixture() -> (Arc<Session>, TurnContext, Arc<Counts>) {
    let (mut session, turn) = crate::session::tests::make_session_and_context().await;
    let counts = Arc::new(Counts::default());
    session.services.host_admission = Some(Arc::new(Host {
        counts: counts.clone(),
        store: session.services.thread_extension_data.level_id().to_owned(),
        turn: turn.sub_id.clone(),
    }));
    session.mark_mcp_runtime_dirty(McpRefreshReason::ExplicitInvalidation);
    (Arc::new(session), turn, counts)
}

#[tokio::test]
async fn required_unscoped_refresh_and_apps_refuse_before_the_held_permit() {
    let (session, _, counts) = fixture().await;
    let _permit = session.mcp_refresh.acquire().await.unwrap();
    let refresh = session.refresh_mcp_if_dirty_with_authority(McpAttemptAccess::Unscoped);
    futures::pin_mut!(refresh);
    assert!(matches!(
        futures::poll!(refresh.as_mut()),
        std::task::Poll::Ready(Err(_))
    ));
    let apps = session.refresh_codex_apps_tools(McpAttemptAccess::Unscoped);
    futures::pin_mut!(apps);
    assert!(matches!(
        futures::poll!(apps.as_mut()),
        std::task::Poll::Ready(Err(_))
    ));
    assert!(session.mcp_refresh.is_pending());
    assert_eq!(session.mcp_refresh.apps_refreshes.len(), 0);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn exhausted_explicit_refresh_refuses_before_the_held_permit_without_fallback() {
    let (session, _, counts) = fixture().await;
    let root = Work::new(&counts);
    counts.refuse.store(true, Ordering::SeqCst);
    let _permit = session.mcp_refresh.acquire().await.unwrap();
    let refresh = session.refresh_mcp_if_dirty_with_authority(McpAttemptAccess::Admitted(&root));
    futures::pin_mut!(refresh);
    assert!(matches!(
        futures::poll!(refresh.as_mut()),
        std::task::Poll::Ready(Err(_))
    ));
    assert!(session.mcp_refresh.is_pending());
    assert_eq!(counts.derives.load(Ordering::SeqCst), 0);
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelled_refresh_owns_work_and_restores_the_claimed_invalidation() {
    let (session, _, counts) = fixture().await;
    let root = Work::new(&counts);
    let _state = session.state.lock().await;
    {
        let mut refresh = Box::pin(
            session.refresh_mcp_if_dirty_with_authority(McpAttemptAccess::Admitted(&root)),
        );
        assert!(futures::poll!(&mut refresh).is_pending());
        assert!(
            !session.mcp_refresh.is_pending(),
            "refresh reached the claimed-state await"
        );
        assert_eq!(counts.live.load(Ordering::SeqCst), 2);
    }
    assert!(session.mcp_refresh.is_pending());
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelled_apps_refresh_releases_its_coalescing_token_and_work() {
    let (session, _, counts) = fixture().await;
    let root = Work::new(&counts);
    let _state = session.state.lock().await;
    {
        let mut refresh =
            Box::pin(session.refresh_codex_apps_tools(McpAttemptAccess::Admitted(&root)));
        assert!(futures::poll!(&mut refresh).is_pending());
        assert_eq!(session.mcp_refresh.apps_refreshes.len(), 1);
        assert_eq!(counts.live.load(Ordering::SeqCst), 2);
        assert!(!session.mcp_refresh.is_pending());
    }
    assert_eq!(session.mcp_refresh.apps_refreshes.len(), 0);
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    assert!(session.mcp_refresh.is_pending());
}

#[tokio::test]
async fn admitted_refresh_publishes_pending_state_without_retaining_idle_work() {
    let (session, _, counts) = fixture().await;
    let root = Work::new(&counts);
    session
        .refresh_mcp_if_dirty_with_authority(McpAttemptAccess::Admitted(&root))
        .await
        .unwrap();
    assert!(!session.mcp_refresh.is_pending());
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while counts.live.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        counts.derives.load(Ordering::SeqCst) >= 2,
        "refresh and publication own separate finite children"
    );
    drop(root);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn failed_publication_derivation_restores_pending_state_without_replacing_runtime() {
    let (session, _, counts) = fixture().await;
    let root = Work::new(&counts);
    let before =
        codex_mcp::McpResourceClient::new(session.services.mcp_runtime.clone()).cache_key();
    let state = session.state.lock().await;
    let mut refresh =
        Box::pin(session.refresh_mcp_if_dirty_with_authority(McpAttemptAccess::Admitted(&root)));
    assert!(futures::poll!(&mut refresh).is_pending());
    assert!(!session.mcp_refresh.is_pending());
    counts.refuse.store(true, Ordering::SeqCst);
    drop(state);
    assert!(refresh.await.is_err());
    assert!(session.mcp_refresh.is_pending());
    assert!(
        before
            == codex_mcp::McpResourceClient::new(session.services.mcp_runtime.clone()).cache_key(),
        "failed publication keeps the same opaque runtime identity"
    );
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
}

fn advertised_tool() -> codex_mcp::ToolInfo {
    codex_mcp::ToolInfo {
        server_name: "test".into(),
        supports_parallel_tool_calls: false,
        server_origin: None,
        callable_name: "test".into(),
        callable_namespace: "test".into(),
        namespace_description: None,
        tool: rmcp::model::Tool::new("test", "test", Arc::new(Default::default())),
        openai_file_input_optional_fields: Default::default(),
        connector_id: None,
        connector_name: None,
        plugin_display_names: Vec::new(),
    }
}
#[tokio::test]
async fn readiness_prepare_and_hard_refresh_refuse_the_wrong_turn_before_the_permit() {
    let (session, mut turn, counts) = fixture().await;
    turn.sub_id = "wrong-turn".into();
    let _permit = session.mcp_refresh.acquire().await.unwrap();
    assert!(session.wait_for_mcp_server(&turn, "test").await.is_err());
    assert!(
        session
            .prepare_mcp_call(&turn, &advertised_tool())
            .await
            .is_err()
    );
    assert!(
        session
            .hard_refresh_latest_codex_apps_tools(&turn)
            .await
            .is_err()
    );
    assert_eq!(counts.turns.load(Ordering::SeqCst), 3);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    assert!(session.mcp_refresh.is_pending());
}

#[tokio::test]
async fn step_capture_refuses_missing_turn_and_unscoped_standalone_before_state_wait() {
    let (session, mut turn, counts) = fixture().await;
    turn.sub_id = "uninstalled".into();
    let turn = Arc::new(turn);
    let cancellation = tokio_util::sync::CancellationToken::new();
    let _state = session.state.lock().await;
    let mut capture = Box::pin(session.capture_step_context(turn.clone(), &cancellation));
    assert!(matches!(
        futures::poll!(&mut capture),
        std::task::Poll::Ready(Err(_))
    ));
    let mut standalone = Box::pin(session.capture_step_context_with_authority(
        turn,
        &cancellation,
        McpAttemptAccess::Unscoped,
    ));
    assert!(matches!(
        futures::poll!(&mut standalone),
        std::task::Poll::Ready(Err(_))
    ));
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    assert_eq!(counts.turns.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelled_step_capture_releases_work_despite_retained_turn_context() {
    let (session, turn, counts) = fixture().await;
    let turn = Arc::new(turn);
    let cancellation = tokio_util::sync::CancellationToken::new();
    let _state = session.state.lock().await;
    {
        let mut capture = Box::pin(session.capture_step_context(turn.clone(), &cancellation));
        assert!(futures::poll!(&mut capture).is_pending());
        assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    }
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    assert_eq!(
        Arc::strong_count(&turn),
        1,
        "only the test retains the context"
    );
}

#[tokio::test]
async fn captured_step_does_not_retain_invocation_work_when_kept_alive() {
    let (session, turn, counts) = fixture().await;
    let step = session
        .capture_step_context(Arc::new(turn), &tokio_util::sync::CancellationToken::new())
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while counts.live.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(counts.turns.load(Ordering::SeqCst), 1);
    assert!(counts.derives.load(Ordering::SeqCst) > 0);
    assert!(Arc::strong_count(&step) >= 1);
}

#[tokio::test]
async fn standalone_capture_uses_explicit_work_without_a_live_turn_lookup() {
    let (session, mut turn, counts) = fixture().await;
    turn.sub_id = "standalone-uninstalled".into();
    let root = Work::new(&counts);
    let step = session
        .capture_step_context_with_authority(
            Arc::new(turn),
            &tokio_util::sync::CancellationToken::new(),
            McpAttemptAccess::Admitted(&root),
        )
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while counts.live.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(counts.turns.load(Ordering::SeqCst), 0);
    assert_eq!(step.turn.sub_id, "standalone-uninstalled");
    drop(root);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn required_step_binding_refuses_before_invalidating_or_waiting_on_state() {
    let (session, turn, counts) = fixture().await;
    let _prior = session.mcp_refresh.take_pending();
    let _state = session.state.lock().await;
    let required_plugins = std::collections::HashSet::new();
    let mut binding = Box::pin(session.mcp_runtime_for_step(
        &turn,
        &[],
        &[],
        &required_plugins,
        McpAttemptAccess::Unscoped,
    ));
    assert!(matches!(
        futures::poll!(&mut binding),
        std::task::Poll::Ready(Err(_))
    ));
    assert!(!session.mcp_refresh.is_pending());
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
}

#[derive(Default)]
struct NeedsMcp(std::sync::atomic::AtomicUsize);
impl codex_extension_api::TurnLifecycleContributor for NeedsMcp {
    fn turn_start_phase(&self, _: &ExtensionData) -> codex_extension_api::TurnStartPhase {
        codex_extension_api::TurnStartPhase::RegularTaskStart
    }
    fn requires_mcp_runtime(&self, _: &ExtensionData) -> bool {
        true
    }
    fn on_turn_start<'a>(
        &'a self,
        _: codex_extension_api::TurnStartInput<'a>,
    ) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
        })
    }
}
#[tokio::test]
async fn regular_task_reports_lifecycle_admission_failure_without_running_contributor() {
    let (session, turn, counts) = fixture().await;
    let mut session = Arc::try_unwrap(session).ok().unwrap();
    let (events_tx, events_rx) = async_channel::unbounded();
    session.tx_event = events_tx;
    let recorder = Arc::new(NeedsMcp::default());
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::new();
    builder.turn_start_admission(session.services.host_admission.as_ref().unwrap().clone());
    builder.turn_lifecycle_contributor(recorder.clone());
    session.services.extensions = Arc::new(builder.build());
    counts.refuse.store(true, Ordering::SeqCst);
    let session = Arc::new(session);
    assert!(
        session
            .try_spawn_task(Arc::new(turn), Vec::new(), crate::tasks::RegularTask::new())
            .await
    );
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut started = false;
        loop {
            let event = events_rx.recv().await.unwrap();
            match event.msg {
                EventMsg::TurnStarted(_) => started = true,
                EventMsg::Error(error) => {
                    assert!(
                        started,
                        "regular task was really committed before its callback refused"
                    );
                    assert!(
                        error
                            .message
                            .contains("MCP turn account work is unavailable")
                    );
                    break;
                }
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(recorder.0.load(Ordering::SeqCst), 0);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    assert_eq!(counts.turns.load(Ordering::SeqCst), 1);
    session.close_task_admission().await;
    let joined = session
        .task_joins
        .shutdown_until(tokio::time::Instant::now() + std::time::Duration::from_secs(5))
        .await;
    assert!(matches!(
        joined,
        crate::tasks::TaskJoinOutcome::Complete { panicked: false }
    ));
}

#[derive(Default)]
struct AdmittedExtensionExecutor {
    calls: AtomicUsize,
    pause: AtomicBool,
    resume: tokio::sync::Notify,
}
impl<'call> codex_tools::ToolExecutor<codex_tools::ToolCall<'call>> for AdmittedExtensionExecutor {
    fn tool_name(&self) -> codex_tools::ToolName {
        codex_tools::ToolName::plain("authority_probe")
    }
    fn spec(&self) -> codex_tools::ToolSpec {
        codex_tools::ToolSpec::Function(codex_tools::ResponsesApiTool {
            name: "authority_probe".into(),
            description: "Test invocation authority".into(),
            strict: false,
            parameters: codex_tools::JsonSchema::default(),
            output_schema: None,
            defer_loading: None,
        })
    }
    fn handle<'a>(
        &'a self,
        call: codex_tools::ToolCall<'call>,
    ) -> codex_tools::ToolExecutorFuture<'a>
    where
        'call: 'a,
    {
        Box::pin(async move {
            let McpAttemptAccess::Admitted(work) = call.mcp_access.map_err(|error| {
                codex_tools::FunctionCallError::RespondToModel(error.to_string())
            })?
            else {
                return Err(codex_tools::FunctionCallError::RespondToModel(
                    "missing callback work".into(),
                ));
            };
            let _child = work.derive_attempt().map_err(|error| {
                codex_tools::FunctionCallError::RespondToModel(error.to_string())
            })?;
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.pause.swap(false, Ordering::SeqCst) {
                self.resume.notified().await;
            }
            Ok(Box::new(codex_tools::JsonToolOutput::new(
                serde_json::json!({"ok":true}),
            )) as Box<dyn codex_tools::ToolOutput>)
        })
    }
}
fn extension_invocation(
    session: Arc<Session>,
    turn: Arc<TurnContext>,
) -> crate::tools::context::ToolInvocation {
    crate::tools::context::ToolInvocation {
        session,
        step_context: crate::session::step_context::StepContext::for_test(turn.clone()),
        turn,
        cancellation_token: CancellationToken::new(),
        tracker: Arc::new(tokio::sync::Mutex::new(
            crate::turn_diff_tracker::TurnDiffTracker::new(),
        )),
        call_id: "authority-call".into(),
        tool_name: codex_tools::ToolName::plain("authority_probe"),
        source: crate::tools::context::ToolCallSource::CodeMode {
            cell_id: "cell".into(),
            runtime_tool_call_id: "nested".into(),
        },
        payload: crate::tools::context::ToolPayload::Function {
            arguments: "{}".into(),
        },
        native_call: None,
        environments: Default::default(),
    }
}
#[tokio::test]
async fn extension_tool_refuses_exhausted_or_wrong_turn_before_history_wait() {
    use codex_tools::ToolExecutor;
    let executor = Arc::new(AdmittedExtensionExecutor::default());
    let adapter =
        crate::tools::handlers::extension_tools::ExtensionToolAdapter::new(executor.clone());
    for wrong in [false, true] {
        let (session, mut turn, counts) = fixture().await;
        if wrong {
            turn.sub_id = "wrong-turn".into();
        } else {
            counts.refuse.store(true, Ordering::SeqCst);
        }
        let _state = session.state.lock().await;
        let mut call = adapter.handle(extension_invocation(session.clone(), Arc::new(turn)));
        assert!(matches!(
            futures::poll!(&mut call),
            std::task::Poll::Ready(Err(_))
        ));
        assert_eq!(counts.live.load(Ordering::SeqCst), 0);
        assert_eq!(counts.turns.load(Ordering::SeqCst), 1);
    }
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cancelled_extension_tool_history_wait_releases_work_without_calling_executor() {
    use codex_tools::ToolExecutor;
    let (session, turn, counts) = fixture().await;
    let turn = Arc::new(turn);
    let invocation = extension_invocation(session.clone(), turn.clone());
    let retained_step = invocation.step_context.clone();
    let executor = Arc::new(AdmittedExtensionExecutor::default());
    let adapter =
        crate::tools::handlers::extension_tools::ExtensionToolAdapter::new(executor.clone());
    let _state = session.state.lock().await;
    let mut call = adapter.handle(invocation);
    assert!(futures::poll!(&mut call).is_pending());
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    drop(call);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    drop((retained_step, turn));
}
#[tokio::test]
async fn extension_callback_work_is_finite_on_cancellation_and_success() {
    use codex_tools::ToolExecutor;
    let (session, turn, counts) = fixture().await;
    let invocation = extension_invocation(session.clone(), Arc::new(turn));
    let retained_step = invocation.step_context.clone();
    let executor = Arc::new(AdmittedExtensionExecutor::default());
    executor.pause.store(true, Ordering::SeqCst);
    let adapter =
        crate::tools::handlers::extension_tools::ExtensionToolAdapter::new(executor.clone());
    let mut call = adapter.handle(invocation.clone());
    assert!(futures::poll!(&mut call).is_pending());
    assert_eq!(counts.live.load(Ordering::SeqCst), 2);
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    drop(call);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    assert!(adapter.handle(invocation).await.is_ok());
    assert_eq!(executor.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        counts.live.load(Ordering::SeqCst),
        0,
        "retained tool/step must own no callback work"
    );
    drop(retained_step);
}

fn disabled_skill_dependency() -> HashMap<String, codex_config::McpServerConfig> {
    HashMap::from([(
        "skill-dependency".into(),
        serde_json::from_value(
            serde_json::json!({"command":"no-such-test-mcp-server","enabled":false}),
        )
        .unwrap(),
    )])
}
#[tokio::test]
async fn dependency_refresh_refuses_before_the_permit_and_configuration_mutation() {
    let (session, _, counts) = fixture().await;
    let original = session.get_config().await;
    let root = Work::new(&counts);
    let _permit = session.mcp_refresh.acquire().await.unwrap();
    for access in [
        McpAttemptAccess::Unscoped,
        McpAttemptAccess::Admitted(&root),
    ] {
        counts.refuse.store(true, Ordering::SeqCst);
        let mut refresh =
            Box::pin(session.refresh_mcp_servers_now(disabled_skill_dependency(), None, access));
        assert!(matches!(
            futures::poll!(&mut refresh),
            std::task::Poll::Ready(Err(_))
        ));
        assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    }
    assert!(Arc::ptr_eq(&session.get_config().await, &original));
    assert!(session.mcp_refresh.is_pending());
}
#[tokio::test]
async fn cancelled_dependency_refresh_waiter_releases_work_without_mutating_configuration() {
    let (session, _, counts) = fixture().await;
    let original = session.get_config().await;
    let root = Work::new(&counts);
    let _permit = session.mcp_refresh.acquire().await.unwrap();
    let mut refresh = Box::pin(session.refresh_mcp_servers_now(
        disabled_skill_dependency(),
        None,
        McpAttemptAccess::Admitted(&root),
    ));
    assert!(futures::poll!(&mut refresh).is_pending());
    assert_eq!(counts.live.load(Ordering::SeqCst), 2);
    drop(refresh);
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    assert!(Arc::ptr_eq(&session.get_config().await, &original));
    assert!(session.mcp_refresh.is_pending());
}
#[tokio::test]
async fn successful_dependency_publication_leaves_no_work_on_the_idle_runtime() {
    let (session, _, counts) = fixture().await;
    let root = Work::new(&counts);
    session
        .refresh_mcp_servers_now(
            disabled_skill_dependency(),
            None,
            McpAttemptAccess::Admitted(&root),
        )
        .await
        .unwrap();
    assert!(!session.mcp_refresh.is_pending());
    assert!(
        session
            .get_config()
            .await
            .mcp_servers
            .get()
            .contains_key("skill-dependency")
    );
    let binding = session
        .services
        .mcp_runtime
        .current_binding()
        .await
        .unwrap();
    assert!(codex_mcp::configured_mcp_servers(binding.config()).contains_key("skill-dependency"));
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    drop(root);
    assert_eq!(
        counts.live.load(Ordering::SeqCst),
        0,
        "idle runtime/binding must own no work"
    );
}
#[tokio::test]
async fn refused_dependency_publication_restores_invalidation_and_keeps_the_old_runtime() {
    let (session, _, counts) = fixture().await;
    let root = Work::new(&counts);
    let client = codex_mcp::McpResourceClient::new(session.services.mcp_runtime.clone());
    let original = client.cache_key();
    counts.refuse_after.store(1, Ordering::SeqCst);
    assert!(
        session
            .refresh_mcp_servers_now(
                disabled_skill_dependency(),
                None,
                McpAttemptAccess::Admitted(&root)
            )
            .await
            .is_err()
    );
    assert!(session.mcp_refresh.is_pending());
    assert_eq!(client.cache_key(), original);
    // The existing configuration edit survives a failed publication for a later reconcile.
    assert!(
        session
            .get_config()
            .await
            .mcp_servers
            .get()
            .contains_key("skill-dependency")
    );
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    counts.refuse_after.store(0, Ordering::SeqCst);
    session
        .refresh_mcp_if_dirty_with_authority(McpAttemptAccess::Admitted(&root))
        .await
        .unwrap();
    assert!(!session.mcp_refresh.is_pending());
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
}
