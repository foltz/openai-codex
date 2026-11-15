//! Actual task installation boundaries with held preparation and counted mail.

use super::*;
use codex_extension_api::{ExtensionFuture, TurnLifecycleContributor, TurnStartInput};
use codex_protocol::host_turn_work::HostTurnWork;
use codex_protocol::protocol::InterAgentCommunication;
use std::sync::Mutex as SyncMutex;

#[derive(Default)]
struct HoldStart {
    entered: CancellationToken,
    release: CancellationToken,
}
impl TurnLifecycleContributor for HoldStart {
    fn on_turn_start<'a>(&'a self, _: TurnStartInput<'a>) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            self.entered.cancel();
            self.release.cancelled().await;
        })
    }
}

#[derive(Debug)]
struct Work(Arc<SyncMutex<Vec<String>>>);
impl HostTurnWork for Work {
    fn bind_submission(&mut self, turn_id: &str) {
        self.0.lock().unwrap().push(format!("bound:{turn_id}"));
    }
    fn retain_until_terminal(self: Box<Self>) {
        self.0.lock().unwrap().push("retained".into());
    }
}
impl Drop for Work {
    fn drop(&mut self) {
        self.0.lock().unwrap().push("released".into());
    }
}

struct InstalledTask(CancellationToken);
impl SessionTask for InstalledTask {
    fn kind(&self) -> TaskKind {
        TaskKind::Regular
    }
    fn span_name(&self) -> &'static str {
        "session_task.installed_mailbox_witness"
    }
    async fn run(
        self: Arc<Self>,
        session: Arc<Session>,
        ctx: Arc<TurnContext>,
        _: Vec<TurnInput>,
        _: CancellationToken,
    ) -> SessionTaskResult {
        {
            let active = session.active_turn.lock().await;
            let task = active
                .as_ref()
                .and_then(|turn| turn.task.as_ref())
                .expect("run sees installed task");
            assert!(Arc::ptr_eq(&task.turn_context, &ctx));
            assert!(task.commit_ready.is_cancelled());
        }
        assert_eq!(
            session.state.lock().await.last_started_turn_id.as_deref(),
            Some(ctx.sub_id.as_str())
        );
        self.0.cancel();
        std::future::pending().await
    }
}

async fn fixture() -> (
    Arc<Session>,
    Arc<TurnContext>,
    Arc<HoldStart>,
    Arc<SyncMutex<Vec<String>>>,
) {
    let (mut session, ctx) = crate::session::tests::make_session_and_context().await;
    let gate = Arc::new(HoldStart::default());
    let mut extensions = codex_extension_api::ExtensionRegistryBuilder::new();
    extensions.turn_lifecycle_contributor(gate.clone());
    session.services.extensions = Arc::new(extensions.build());
    let session = Arc::new(session);
    let events = Arc::new(SyncMutex::new(Vec::new()));
    let message = InterAgentCommunication::new(
        codex_protocol::AgentPath::root(),
        codex_protocol::AgentPath::try_from("/root/worker").unwrap(),
        Vec::new(),
        "reserved task mail".into(),
        true,
    );
    session.input_queue.enqueue_mailbox_with_work(
        message.into(),
        Default::default(),
        Some(Box::new(Work(Arc::clone(&events)))),
    );
    (session, Arc::new(ctx), gate, events)
}

async fn reach_preparation<F: std::future::Future>(start: std::pin::Pin<&mut F>, gate: &HoldStart) {
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::select! {
            _ = gate.entered.cancelled() => {}
            _ = start => panic!("start returned before preparation witness"),
        }
    })
    .await
    .expect("actual start reaches preparation");
}

#[tokio::test]
async fn cancelled_actual_task_preparation_restores_mail_then_only_retry_commits() {
    let (session, ctx, gate, events) = fixture().await;
    let before = session.state.lock().await.last_started_turn_id.clone();
    let mut start = Box::pin(session.start_task(
        Arc::clone(&ctx),
        Vec::new(),
        InstalledTask(CancellationToken::new()),
    ));
    reach_preparation(start.as_mut(), &gate).await;
    assert!(!session.input_queue.has_pending_mailbox_items().await);
    assert!(events.lock().unwrap().is_empty());
    drop(start);
    assert!(session.input_queue.has_pending_mailbox_items().await);
    assert_eq!(session.state.lock().await.last_started_turn_id, before);
    assert!(events.lock().unwrap().is_empty());
    gate.release.cancel();
    let ran = CancellationToken::new();
    assert!(
        session
            .start_task(Arc::clone(&ctx), Vec::new(), InstalledTask(ran.clone()))
            .await
    );
    tokio::time::timeout(Duration::from_secs(10), ran.cancelled())
        .await
        .unwrap();
    assert_eq!(
        *events.lock().unwrap(),
        [
            format!("bound:{}", ctx.sub_id),
            "retained".into(),
            "released".into()
        ]
    );
    assert!(!session.input_queue.has_pending_mailbox_items().await);
    session.abort_all_tasks(TurnAbortReason::Replaced).await;
}

#[tokio::test]
async fn cancelled_final_destination_lock_wait_keeps_metadata_and_work_uncommitted() {
    let (session, ctx, gate, events) = fixture().await;
    let before = session.state.lock().await.last_started_turn_id.clone();
    let mut start =
        Box::pin(session.start_task(ctx, Vec::new(), InstalledTask(CancellationToken::new())));
    reach_preparation(start.as_mut(), &gate).await;
    let turn_state = Arc::clone(
        &session
            .active_turn
            .lock()
            .await
            .as_ref()
            .unwrap()
            .turn_state,
    );
    let destination = turn_state.lock().await;
    gate.release.cancel();
    assert!(futures::poll!(start.as_mut()).is_pending());
    drop(start);
    assert_eq!(session.state.lock().await.last_started_turn_id, before);
    assert!(session.input_queue.has_pending_mailbox_items().await);
    assert!(events.lock().unwrap().is_empty());
    drop(destination);
    assert!(
        session
            .active_turn
            .lock()
            .await
            .as_ref()
            .unwrap()
            .task
            .is_none()
    );
    session.abort_all_tasks(TurnAbortReason::Replaced).await;
}

#[tokio::test]
async fn closing_admission_during_preparation_restores_mail_without_task_or_lineage() {
    let (session, ctx, gate, events) = fixture().await;
    let before = session.state.lock().await.last_started_turn_id.clone();
    let mut start =
        Box::pin(session.start_task(ctx, Vec::new(), InstalledTask(CancellationToken::new())));
    reach_preparation(start.as_mut(), &gate).await;
    session.close_task_admission().await;
    gate.release.cancel();
    assert!(!start.await);
    assert!(session.active_turn.lock().await.is_none());
    assert_eq!(session.state.lock().await.last_started_turn_id, before);
    assert!(session.input_queue.has_pending_mailbox_items().await);
    assert!(events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn explicit_replacement_cancels_pending_mailbox_owner_before_reusing_its_mail() {
    let (session, _, gate, events) = fixture().await;
    let mut old =
        Box::pin(session.maybe_start_turn_for_pending_work_with_sub_id("old-mailbox-start".into()));
    reach_preparation(old.as_mut(), &gate).await;
    let ctx = session
        .new_turn_with_default_settings("replacement-task".into(), Default::default())
        .await;
    let ran = CancellationToken::new();
    let mut new =
        Box::pin(session.try_spawn_task(Arc::clone(&ctx), Vec::new(), InstalledTask(ran.clone())));
    assert!(
        futures::poll!(new.as_mut()).is_pending(),
        "replacement waits for original preparation owner"
    );
    assert!(events.lock().unwrap().is_empty());
    gate.release.cancel();
    let (_, started) =
        tokio::time::timeout(Duration::from_secs(10), async { tokio::join!(old, new) })
            .await
            .unwrap();
    assert!(started);
    tokio::time::timeout(Duration::from_secs(10), ran.cancelled())
        .await
        .unwrap();
    assert_eq!(
        *events.lock().unwrap(),
        ["bound:replacement-task", "retained", "released"]
    );
    assert_eq!(
        session.state.lock().await.last_started_turn_id.as_deref(),
        Some("replacement-task")
    );
    assert!(!session.input_queue.has_pending_mailbox_items().await);
    session.abort_all_tasks(TurnAbortReason::Replaced).await;
}

#[tokio::test]
async fn eviction_fence_refuses_task_before_preparation_or_consumption() {
    let (session, ctx, gate, events) = fixture().await;
    // A private accounted batch prevents even acquiring the eviction fence.
    assert!(
        session
            .input_queue
            .try_fence_mailbox_for_eviction()
            .is_none()
    );
    let batch = session.input_queue.reserve_mailbox();
    assert!(
        session
            .input_queue
            .try_fence_mailbox_for_eviction()
            .is_none()
    );
    drop(batch);
    assert!(events.lock().unwrap().is_empty());
    // An independent empty session can be fenced; final start must refuse it.
    let (empty, _) = crate::session::tests::make_session_and_context().await;
    let empty = Arc::new(empty);
    let fence = empty.input_queue.try_fence_mailbox_for_eviction().unwrap();
    assert!(
        !empty
            .start_task(ctx, Vec::new(), InstalledTask(CancellationToken::new()))
            .await
    );
    assert!(empty.active_turn.lock().await.is_none());
    assert!(!gate.entered.is_cancelled());
    drop(fence);
}

#[tokio::test]
async fn owned_completion_preparation_does_not_block_control_dispatch_or_shutdown() {
    let (session, _, gate, events) = fixture().await;
    let (sender, receiver) = async_channel::unbounded();
    let config = session.get_config().await;
    let loop_task = tokio::spawn(crate::session::submission_loop(
        Arc::clone(&session),
        config,
        receiver,
    ));
    session.input_queue.completion_wake.notify_one();
    tokio::time::timeout(Duration::from_secs(10), gate.entered.cancelled())
        .await
        .unwrap();
    let (reply, response) = tokio::sync::oneshot::channel();
    sender
        .send(crate::session::Submission {
            id: "control-during-preparation".into(),
            op: codex_protocol::protocol::Op::InterruptIfNoPendingInput {
                turn_id: "not-running".into(),
                reply,
            },
            turn_extension_init: None,
            trace: None,
            parent_turn_id: None,
            root_turn_id: None,
            residency_guard: None,
            message_delivery: None,
        })
        .await
        .unwrap();
    assert!(
        !tokio::time::timeout(Duration::from_secs(10), response)
            .await
            .unwrap()
            .unwrap()
    );
    assert!(events.lock().unwrap().is_empty());
    sender
        .send(crate::session::Submission {
            id: "shutdown-held-preparation".into(),
            op: codex_protocol::protocol::Op::Shutdown,
            turn_extension_init: None,
            trace: None,
            parent_turn_id: None,
            root_turn_id: None,
            residency_guard: None,
            message_delivery: None,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), loop_task)
        .await
        .unwrap()
        .unwrap();
    assert!(
        events.lock().unwrap().is_empty(),
        "shutdown never installs the held turn"
    );
    assert!(session.input_queue.has_pending_mailbox_items().await);
    assert!(!session.input_queue.preparation.is_busy());
}
