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
        initial_messages: None,
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
