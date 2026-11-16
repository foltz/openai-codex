use super::*;
use crate::session::Submission;
use crate::session::completed_session_loop_termination;
use crate::session::tests::make_session_and_context;
use codex_extension_api::ExtensionData;
use codex_extension_api::ExtensionFuture;
use codex_extension_api::ExtensionRegistryBuilder;
use codex_extension_api::TurnStartAdmission;
use codex_extension_api::TurnWorkRefused;
use codex_protocol::host_turn_work::HostTurnAction;
use codex_protocol::host_turn_work::HostTurnWork;
use codex_protocol::protocol::ReviewRequest;
use codex_protocol::protocol::ReviewTarget;
use codex_protocol::protocol::SessionConfiguredEvent;
use pretty_assertions::assert_eq;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

#[derive(Debug, Default)]
struct Admission {
    closed: AtomicBool,
    calls: AtomicUsize,
    dropped: Arc<AtomicUsize>,
}

#[derive(Debug)]
struct Work(Arc<AtomicUsize>);

impl HostTurnWork for Work {
    fn bind_submission(&mut self, _turn_id: &str) {}

    fn retain_until_terminal(self: Box<Self>) {
        panic!("queue-only fixture must not start a turn");
    }
}

impl Drop for Work {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl TurnStartAdmission for Admission {
    fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
        Some(Box::new(()))
    }

    fn admit_turn_work(
        &self,
        _store: &ExtensionData,
        _termination: ExtensionFuture<'static, ()>,
    ) -> Result<Option<Box<dyn HostTurnWork>>, TurnWorkRefused> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.closed.load(Ordering::SeqCst) {
            return Err(TurnWorkRefused::Unavailable);
        }
        Ok(Some(Box::new(Work(Arc::clone(&self.dropped)))))
    }
}

async fn fixture() -> (
    CodexThread,
    async_channel::Receiver<Submission>,
    Arc<Admission>,
) {
    let (mut session, turn) = make_session_and_context().await;
    let gate = Arc::new(Admission::default());
    let mut extensions = ExtensionRegistryBuilder::new();
    extensions.turn_start_admission(gate.clone());
    session.services.extensions = Arc::new(extensions.build());
    session.services.host_admission = session.services.extensions.host_admission();
    let configured = SessionConfiguredEvent {
        session_id: session.session_id(),
        thread_id: session.thread_id,
        forked_from_id: None,
        parent_thread_id: None,
        thread_source: None,
        thread_name: None,
        model: "fixture".to_string(),
        model_provider_id: "fixture".to_string(),
        service_tier: None,
        approval_policy: codex_protocol::protocol::AskForApproval::Never,
        approvals_reviewer: codex_protocol::config_types::ApprovalsReviewer::User,
        permission_profile: codex_protocol::models::PermissionProfile::Disabled,
        active_permission_profile: None,
        cwd: turn.config.cwd.clone(),
        reasoning_effort: None,
        network_proxy: None,
        rollout_path: None,
    };
    let (tx_sub, rx_sub) = async_channel::bounded(1);
    let (_tx_event, rx_event) = async_channel::unbounded();
    let io = SessionIo {
        tx_sub: tx_sub.into(),
        rx_event,
        agent_status: tokio::sync::watch::channel(crate::agent::AgentStatus::PendingInit).1,
        session_loop_termination: completed_session_loop_termination(),
    };
    let thread = CodexThread::new(
        Arc::new(session),
        io,
        ThreadStartupMetadata::from(&configured),
        /*rollout_path*/ None,
        SessionSource::Exec,
    );
    (thread, rx_sub, gate)
}

fn review() -> ReviewRequest {
    ReviewRequest {
        target: ReviewTarget::UncommittedChanges,
        user_facing_hint: Some("review fixture".to_string()),
    }
}

fn trace() -> W3cTraceContext {
    W3cTraceContext {
        traceparent: Some("00-00000000000000000000000000000011-0000000000000022-01".to_string()),
        tracestate: Some("vendor=value".to_string()),
    }
}

#[tokio::test]
async fn traced_compact_and_review_keep_trace_and_one_queued_lease() {
    let (thread, rx, gate) = fixture().await;
    for op in [
        Op::Compact,
        Op::Review {
            review_request: review(),
        },
    ] {
        let calls_before = gate.calls.load(Ordering::SeqCst);
        let dropped_before = gate.dropped.load(Ordering::SeqCst);
        let is_compact = matches!(&op, Op::Compact);
        let id = thread.submit_with_trace(op, Some(trace())).await.unwrap();
        let queued = rx.recv().await.unwrap();
        assert_eq!(queued.id, id);
        assert_eq!(queued.trace, Some(trace()));
        assert_eq!((queued.parent_turn_id, queued.root_turn_id), (None, None));
        assert_eq!(gate.calls.load(Ordering::SeqCst), calls_before + 1);
        assert_eq!(gate.dropped.load(Ordering::SeqCst), dropped_before);
        let Op::HostTurn { action, work } = queued.op else {
            panic!("traced submission bypassed host admission");
        };
        match action {
            HostTurnAction::Compact => assert!(is_compact),
            HostTurnAction::Review(request) => {
                assert!(!is_compact);
                assert_eq!(request, review());
            }
        }
        drop(work);
        assert_eq!(gate.dropped.load(Ordering::SeqCst), dropped_before + 1);
    }
}

#[tokio::test]
async fn direct_compact_and_review_admit_exactly_once() {
    let (thread, rx, gate) = fixture().await;
    for op in [
        Op::Compact,
        Op::Review {
            review_request: review(),
        },
    ] {
        let before = gate.calls.load(Ordering::SeqCst);
        thread.submit(op).await.unwrap();
        let queued = rx.recv().await.unwrap();
        assert!(matches!(&queued.op, Op::HostTurn { .. }));
        assert_eq!(gate.calls.load(Ordering::SeqCst), before + 1);
        drop(queued);
        assert_eq!(gate.dropped.load(Ordering::SeqCst), before + 1);
    }
}

#[tokio::test]
async fn refused_traced_work_does_not_enqueue_and_interrupt_still_passes() {
    let (thread, rx, gate) = fixture().await;
    gate.closed.store(true, Ordering::SeqCst);
    for op in [
        Op::Compact,
        Op::Review {
            review_request: review(),
        },
    ] {
        let error = thread
            .submit_with_trace(op, Some(trace()))
            .await
            .unwrap_err();
        assert!(matches!(error.details(),
            codex_protocol::error::CodexErrorDetails::Fatal(message)
                if message == "account work admission is closed"));
        assert!(rx.is_empty());
    }
    assert_eq!(gate.calls.load(Ordering::SeqCst), 2);
    assert_eq!(gate.dropped.load(Ordering::SeqCst), 0);
    thread
        .submit_with_trace(Op::Interrupt, Some(trace()))
        .await
        .unwrap();
    let queued = rx.recv().await.unwrap();
    assert!(matches!(queued.op, Op::Interrupt));
    assert_eq!(queued.trace, Some(trace()));
    assert_eq!(gate.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn cancelled_traced_enqueue_releases_unqueued_work() {
    let (thread, rx, gate) = fixture().await;
    thread.submit(Op::Interrupt).await.unwrap();
    let mut submission = Box::pin(thread.submit_with_trace(Op::Compact, Some(trace())));
    assert!(futures::poll!(&mut submission).is_pending());
    assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
    assert_eq!(gate.dropped.load(Ordering::SeqCst), 0);
    drop(submission);
    assert_eq!(gate.dropped.load(Ordering::SeqCst), 1);
    assert!(matches!(rx.recv().await.unwrap().op, Op::Interrupt));
    assert!(rx.is_empty());
}

fn typed_request() -> TurnInputRequest {
    TurnInputRequest::new(codex_protocol::turn_input::TurnInput::ResponseItem(
        serde_json::from_value(serde_json::json!({
            "type": "message", "role": "user", "content": [{"type":"input_text", "text":"queued source fixture"}]
        })).unwrap(),
    ))
}

#[test_case::test_case(TurnInputMode::StartOrSteer; "start or steer")]
#[test_case::test_case(TurnInputMode::StartIfIdle; "idle start")]
#[test_case::test_case(TurnInputMode::ContinueIfIdle { expected_previous_turn_id: "previous".into() }; "continuation")]
#[tokio::test]
async fn typed_enqueue_keeps_lease_after_reply_waiter_is_cancelled(mode: TurnInputMode) {
    let (thread, rx, gate) = fixture().await;
    let thread = Arc::new(thread);
    let mut request = typed_request();
    request.trace = Some(trace());
    let caller_thread = Arc::clone(&thread);
    let caller = tokio::spawn(async move {
        caller_thread
            .submit_turn_input_with_mode(request, mode)
            .await
    });
    let queued = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(queued.trace, Some(trace()));
    assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
    assert_eq!(gate.dropped.load(Ordering::SeqCst), 0);
    assert!(matches!(
        &queued.op,
        Op::TurnInput {
            host_work: Some(_),
            ..
        }
    ));
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert_eq!(
        gate.dropped.load(Ordering::SeqCst),
        0,
        "queued original, not reply waiter, owns the work"
    );
    drop(queued);
    assert_eq!(gate.dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelled_typed_enqueue_releases_work_before_it_reaches_the_queue() {
    let (thread, rx, gate) = fixture().await;
    thread.submit(Op::Interrupt).await.unwrap();
    let mut caller =
        Box::pin(thread.submit_turn_input_with_mode(typed_request(), TurnInputMode::StartIfIdle));
    assert!(futures::poll!(caller.as_mut()).is_pending());
    assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
    assert_eq!(gate.dropped.load(Ordering::SeqCst), 0);
    drop(caller);
    assert_eq!(gate.dropped.load(Ordering::SeqCst), 1);
    assert!(matches!(rx.recv().await.unwrap().op, Op::Interrupt));
    assert!(rx.is_empty());
}

#[tokio::test]
async fn closed_public_start_and_recovery_admission_refuse_before_enqueue() {
    let (thread, rx, gate) = fixture().await;
    gate.closed.store(true, Ordering::SeqCst);
    for mode in [
        TurnInputMode::StartOrSteer,
        TurnInputMode::StartIfIdle,
        TurnInputMode::ContinueIfIdle {
            expected_previous_turn_id: "previous".into(),
        },
    ] {
        assert_eq!(
            thread
                .submit_turn_input_with_mode(typed_request(), mode)
                .await
                .unwrap(),
            TurnInputSubmission::NotSubmitted {
                reason: codex_protocol::turn_input::NotSubmittedReason::ServerDraining
            }
        );
    }
    assert_eq!(
        thread
            .recover_turn_if_idle(RecoverTurnRequest {
                turn_id: "original-id".into(),
                thread_settings: Default::default(),
                trace: None,
                prompt_cache_ttl: None,
                cyber_access_program: None,
            })
            .await
            .unwrap(),
        StartIfIdleSubmission::NotSubmitted {
            reason: codex_protocol::turn_input::NotSubmittedReason::ServerDraining
        }
    );
    assert_eq!(gate.calls.load(Ordering::SeqCst), 4);
    assert_eq!(gate.dropped.load(Ordering::SeqCst), 0);
    assert!(rx.is_empty());
}

#[tokio::test]
async fn pure_steering_does_not_reopen_closed_account_admission() {
    let (thread, rx, gate) = fixture().await;
    gate.closed.store(true, Ordering::SeqCst);
    let mut caller = Box::pin(thread.steer_turn(typed_request(), "active".into()));
    assert!(futures::poll!(caller.as_mut()).is_pending());
    let queued = rx.recv().await.unwrap();
    let Op::TurnInput {
        host_work, reply, ..
    } = queued.op
    else {
        panic!("typed steering")
    };
    assert!(host_work.is_none());
    assert_eq!(gate.calls.load(Ordering::SeqCst), 0);
    reply
        .send(Ok(TurnInputSubmission::Steered {
            turn_id: "active".into(),
            root_turn_id: "active".into(),
        }))
        .unwrap();
    assert_eq!(
        caller.await.unwrap(),
        SteerSubmission::Steered {
            turn_id: "active".into()
        }
    );
}

#[tokio::test]
async fn recovery_queue_retains_original_id_trace_and_lease_after_waiter_drop() {
    let (thread, rx, gate) = fixture().await;
    let mut caller = Box::pin(thread.recover_turn_if_idle(RecoverTurnRequest {
        turn_id: "original-recovery-id".into(),
        thread_settings: Default::default(),
        trace: Some(trace()),
        prompt_cache_ttl: Some(codex_protocol::turn_input::PromptCacheTtl::TwelveHours),
        cyber_access_program: None,
    }));
    assert!(futures::poll!(caller.as_mut()).is_pending());
    let queued = rx.recv().await.unwrap();
    assert_eq!(queued.id, "original-recovery-id");
    assert_eq!(queued.trace, Some(trace()));
    let Op::RecoverTurn {
        host_work: Some(work),
        start_options,
        ..
    } = queued.op
    else {
        panic!("owned recovery")
    };
    assert_eq!(
        start_options.prompt_cache_ttl,
        Some(codex_protocol::turn_input::PromptCacheTtl::TwelveHours)
    );
    drop(caller);
    assert_eq!(gate.dropped.load(Ordering::SeqCst), 0);
    drop(work);
    assert_eq!(gate.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn queued_typed_delivery_owns_residency_after_reply_waiter_drop() {
    let (thread, rx, _) = fixture().await;
    let residency = Arc::new(RwLock::new(()));
    let guard = Arc::clone(&residency).read_owned().await;
    let mut caller = Box::pin(thread.io.submit_turn_input(
        typed_request(),
        TurnInputMode::StartOrSteer,
        None,
        Some(guard),
    ));
    assert!(futures::poll!(caller.as_mut()).is_pending());
    let queued = rx.recv().await.unwrap();
    drop(caller);
    assert!(Arc::clone(&residency).try_write_owned().is_err());
    drop(queued);
    assert!(Arc::clone(&residency).try_write_owned().is_ok());
}

#[derive(Debug, Default)]
struct RefusingMcpRequest(std::sync::atomic::AtomicUsize);
impl codex_extension_api::TurnStartAdmission for RefusingMcpRequest {
    fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
        panic!("no fresh fallback")
    }
    fn requires_account_work(&self) -> bool {
        true
    }
    fn derive_request_operation_work(
        &self,
    ) -> Result<Option<Box<dyn codex_extension_api::HostOperationWork>>, TurnWorkRefused> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(TurnWorkRefused::Unavailable)
    }
}
#[tokio::test]
async fn public_mcp_requests_refuse_before_refresh_or_rpc_without_request_authority() {
    let (mut thread, queue, _) = fixture().await;
    let gate = Arc::new(RefusingMcpRequest::default());
    Arc::get_mut(&mut thread.session)
        .unwrap()
        .services
        .host_admission = Some(gate.clone());
    assert!(
        thread
            .refresh_codex_apps_tools()
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("request account work")
    );
    assert!(
        thread
            .read_mcp_resource("test", ReadResourceRequestParams::new("test://read"))
            .await
            .unwrap_err()
            .to_string()
            .contains("request account work")
    );
    assert!(
        thread
            .read_mcp_resource_for_call("call", ReadResourceRequestParams::new("test://read"))
            .await
            .unwrap_err()
            .to_string()
            .contains("request account work")
    );
    assert!(
        thread
            .start_mcp_event_stream("test", serde_json::json!({}), None)
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("request account work")
    );
    assert!(
        thread
            .call_mcp_tool("test", "tool", None, None)
            .await
            .unwrap_err()
            .to_string()
            .contains("request account work")
    );
    assert!(
        thread
            .mcp_server_status_snapshot("test", codex_mcp::McpSnapshotDetail::Full, None)
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("request account work")
    );
    assert_eq!(gate.0.load(Ordering::SeqCst), 6);
    assert!(
        thread
            .start_mcp_event_stream_with_authority(
                "test",
                serde_json::json!({}),
                None,
                codex_mcp::McpAttemptAccess::Unscoped
            )
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("account work is required")
    );
    assert!(
        queue.is_empty(),
        "request refusal cannot become a queued turn"
    );
}

fn history_message(text: &str) -> ResponseItem {
    ResponseItem::Message {
        status: None,
        encrypted_content: None,
        id: None,
        role: "developer".into(),
        content: vec![codex_protocol::models::ContentItem::InputText { text: text.into() }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    }
}

#[derive(Debug, Default)]
struct HistoryWorkCounts {
    live: AtomicUsize,
    requests: AtomicUsize,
    derives: AtomicUsize,
    refuse_child: AtomicBool,
}
#[derive(Debug)]
struct HistoryWork(Arc<HistoryWorkCounts>);
impl Drop for HistoryWork {
    fn drop(&mut self) {
        self.0.live.fetch_sub(1, Ordering::SeqCst);
    }
}
impl codex_extension_api::HostOperationWork for HistoryWork {
    fn derive_operation(
        &self,
    ) -> Result<Box<dyn codex_extension_api::HostOperationWork>, TurnWorkRefused> {
        self.0.derives.fetch_add(1, Ordering::SeqCst);
        if self.0.refuse_child.load(Ordering::SeqCst) {
            return Err(TurnWorkRefused::Unavailable);
        }
        self.0.live.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(Self(Arc::clone(&self.0))))
    }
    fn derive_turn_work(
        &self,
        _: &ExtensionData,
        _: ExtensionFuture<'static, ()>,
    ) -> Result<Box<dyn HostTurnWork>, TurnWorkRefused> {
        panic!("history must not create a turn")
    }
}
#[derive(Debug)]
struct HistoryAdmission(Arc<HistoryWorkCounts>);
impl TurnStartAdmission for HistoryAdmission {
    fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
        panic!("no fresh fallback")
    }
    fn requires_account_work(&self) -> bool {
        true
    }
    fn derive_request_operation_work(
        &self,
    ) -> Result<Option<Box<dyn codex_extension_api::HostOperationWork>>, TurnWorkRefused> {
        self.0.requests.fetch_add(1, Ordering::SeqCst);
        self.0.live.fetch_add(1, Ordering::SeqCst);
        Ok(Some(Box::new(HistoryWork(Arc::clone(&self.0)))))
    }
    fn derive_operation_work(
        &self,
        _: &ExtensionData,
        _: &str,
    ) -> Result<Option<Box<dyn codex_extension_api::HostOperationWork>>, TurnWorkRefused> {
        panic!("standalone history cannot borrow an installed turn")
    }
}
async fn history_fixture() -> (CodexThread, Arc<HistoryWorkCounts>) {
    let (mut thread, _, _) = fixture().await;
    let counts = Arc::new(HistoryWorkCounts::default());
    Arc::get_mut(&mut thread.session)
        .unwrap()
        .services
        .host_admission = Some(Arc::new(HistoryAdmission(Arc::clone(&counts))));
    (thread, counts)
}

#[tokio::test]
async fn history_preparation_refuses_request_work_before_recording_context() {
    let (mut thread, queue, _) = fixture().await;
    let gate = Arc::new(RefusingMcpRequest::default());
    Arc::get_mut(&mut thread.session)
        .unwrap()
        .services
        .host_admission = Some(gate.clone());
    let mut call =
        Box::pin(thread.inject_response_items_for_turn(vec![history_message("refused")]));
    assert!(matches!(
        futures::poll!(&mut call),
        std::task::Poll::Ready(Err(_))
    ));
    drop(call);
    assert_eq!(gate.0.load(Ordering::SeqCst), 1);
    assert!(thread.session.reference_context_item().await.is_none());
    assert_eq!(thread.session.clone_history().await.raw_items().count(), 0);
    assert!(queue.is_empty());
}

#[tokio::test]
async fn history_preparation_uses_request_work_for_initial_and_existing_context() {
    let (thread, counts) = history_fixture().await;
    assert!(thread.session.reference_context_item().await.is_none());
    thread
        .inject_response_items_for_turn(vec![history_message("first")])
        .await
        .unwrap();
    assert!(thread.session.reference_context_item().await.is_some());
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    assert!(counts.derives.load(Ordering::SeqCst) > 0);
    let derives = counts.derives.load(Ordering::SeqCst);
    thread
        .inject_response_items_for_turn(vec![history_message("second")])
        .await
        .unwrap();
    assert_eq!(counts.requests.load(Ordering::SeqCst), 2);
    assert_eq!(
        counts.derives.load(Ordering::SeqCst),
        derives,
        "existing history does not rediscover skills/tools"
    );
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    let history = thread.session.clone_history().await;
    let items: Vec<_> = history.raw_items().collect();
    assert!(items.contains(&&history_message("first")));
    assert!(items.contains(&&history_message("second")));
}

#[tokio::test]
async fn cancelled_history_preparation_releases_request_work_without_recording() {
    let (thread, counts) = history_fixture().await;
    // Initial capture retains its context only after acquiring this lock.
    let guard = thread.session.active_turn.lock().await;
    let mut call =
        Box::pin(thread.inject_response_items_for_turn(vec![history_message("cancelled")]));
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            assert!(futures::poll!(&mut call).is_pending());
            if counts.live.load(Ordering::SeqCst) >= 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(counts.requests.load(Ordering::SeqCst), 1);
    drop(call);
    drop(guard);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    assert!(thread.session.reference_context_item().await.is_none());
    assert_eq!(thread.session.clone_history().await.raw_items().count(), 0);
}

#[tokio::test]
async fn exhausted_history_capture_does_not_record_or_admit_a_fresh_turn() {
    let (thread, counts) = history_fixture().await;
    counts.refuse_child.store(true, Ordering::SeqCst);
    assert!(
        thread
            .inject_response_items_for_turn(vec![history_message("exhausted")])
            .await
            .is_err()
    );
    assert_eq!(counts.requests.load(Ordering::SeqCst), 1);
    assert_eq!(counts.derives.load(Ordering::SeqCst), 1);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    assert!(thread.session.reference_context_item().await.is_none());
    assert_eq!(thread.session.clone_history().await.raw_items().count(), 0);
}
