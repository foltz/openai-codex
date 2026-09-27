use super::*;
use futures::FutureExt;
use pretty_assertions::assert_eq;
use std::time::Duration;

struct PausedStart {
    pause_calls: usize,
    calls: std::sync::atomic::AtomicUsize,
    entered: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
    resumed: std::sync::atomic::AtomicUsize,
}

impl codex_extension_api::TurnLifecycleContributor for PausedStart {
    fn on_turn_start<'a>(
        &'a self,
        _input: codex_extension_api::TurnStartInput<'a>,
    ) -> codex_extension_api::ExtensionFuture<'a, ()> {
        Box::pin(async move {
            if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) >= self.pause_calls {
                return;
            }
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
            self.resumed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        })
    }
}

#[tokio::test]
async fn completion_wake_survives_producer_abort_and_is_driven_by_loop() {
    let (mut session, context) = tests::make_session_and_context().await;
    let pause = Arc::new(PausedStart {
        pause_calls: 1,
        calls: std::sync::atomic::AtomicUsize::new(0),
        entered: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
        resumed: std::sync::atomic::AtomicUsize::new(0),
    });
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::<Config>::new();
    builder.turn_lifecycle_contributor(pause.clone());
    session.services.extensions = Arc::new(builder.build());
    let session = Arc::new(session);
    let mail = InterAgentCommunication::new(
            codex_protocol::AgentPath::root(), codex_protocol::AgentPath::root(),
            Vec::new(), "wake".to_owned(), /*trigger_turn*/ true,
        );
    session.input_queue.enqueue_mailbox_communication(
        mail.clone(),
        codex_protocol::turn_input::TurnStartOptions::default(),
    ).await;
    let producer_session = Arc::clone(&session);
    let (sent, received) = tokio::sync::oneshot::channel();
    let producer = tokio::spawn(async move {
        producer_session.input_queue.completion_wake.notify_one();
        sent.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    received.await.unwrap();
    producer.abort();
    assert!(producer.await.unwrap_err().is_cancelled());
    assert!(session.active_turn.lock().await.is_none());

    let (sender, receiver) = async_channel::bounded(1);
    let loop_task = tokio::spawn(handlers::submission_loop(
        Arc::clone(&session), context.config.clone(), receiver, None,
    ));
    tokio::time::timeout(Duration::from_secs(10), pause.entered.acquire())
        .await.expect("loop drove stored completion wake").unwrap().forget();
    assert!(session.active_turn.lock().await.as_ref().is_some_and(|turn| turn.task.is_none()));
    assert!(!session.input_queue.has_pending_mailbox_items().await,
        "preparing start privately owns its mail");
    let (reply, response) = tokio::sync::oneshot::channel();
    sender.send(codex_protocol::protocol::Submission {
        id: "suspend-during-mailbox-start".into(),
        op: codex_protocol::protocol::Op::SuspendTurnAndShutdown { reply },
        trace: None,
        parent_turn_id: None,
        root_turn_id: None,
    }).await.unwrap();
    assert!(matches!(tokio::time::timeout(Duration::from_secs(10), response)
        .await.expect("suspension remains dispatchable").unwrap().unwrap(),
        codex_protocol::turn_input::SuspendTurnOutcome::NotActive));
    // Do not release the paused start. A queued shutdown must remain
    // dispatchable while the loop owns that preparation future.
    sender.send(codex_protocol::protocol::Submission {
        id: "shutdown-during-mailbox-start".into(),
        op: codex_protocol::protocol::Op::Shutdown,
        trace: None,
        parent_turn_id: None,
        root_turn_id: None,
    }).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), loop_task)
        .await.expect("shutdown preempts held mailbox preparation").unwrap();
    assert!(session.active_turn.lock().await.is_none());
    pause.release.add_permits(1);
    tokio::task::yield_now().await;
    assert_eq!(pause.resumed.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(session.input_queue.drain_mailbox_input_items().await.0,
        vec![TurnInput::InterAgentCommunication(mail)]);
}

#[tokio::test]
async fn closed_submission_channel_takes_precedence_over_completion_wake() {
    let (session, context) = tests::make_session_and_context().await;
    let session = Arc::new(session);
    session.input_queue.completion_wake.notify_one();
    let (sender, receiver) = async_channel::bounded(1);
    drop(sender);
    tokio::time::timeout(Duration::from_secs(10), handlers::submission_loop(
        Arc::clone(&session), context.config.clone(), receiver, None,
    )).await.expect("wake does not retain the submission channel");
    assert!(session.input_queue.completion_wake.notified().now_or_never().is_some());
    assert!(session.active_turn.lock().await.is_none());
}

struct HeldTask;

impl crate::tasks::SessionTask for HeldTask {
    fn kind(&self) -> crate::state::TaskKind { crate::state::TaskKind::Regular }

    fn span_name(&self) -> &'static str { "session_task.mailbox_reservation_test" }

    async fn run(
        self: Arc<Self>,
        _session: Arc<Session>,
        _ctx: Arc<TurnContext>,
        _input: Vec<TurnInput>,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> crate::tasks::SessionTaskResult {
        cancellation.cancelled().await;
        Ok(None)
    }
}

#[tokio::test]
async fn replaced_mailbox_preparation_stops_before_overwriting_installed_task() {
    for registered_after_abort in [false, true] {
    let (mut session, _context) = tests::make_session_and_context().await;
    let pause = Arc::new(PausedStart {
        pause_calls: 2,
        calls: std::sync::atomic::AtomicUsize::new(0),
        entered: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
        resumed: std::sync::atomic::AtomicUsize::new(0),
    });
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::<Config>::new();
    builder.turn_lifecycle_contributor(pause.clone());
    session.services.extensions = Arc::new(builder.build());
    let session = Arc::new(session);
    let mail = InterAgentCommunication::new(
        codex_protocol::AgentPath::root(), codex_protocol::AgentPath::root(),
        Vec::new(), "private first batch".to_owned(), /*trigger_turn*/ true,
    );
    session.input_queue.enqueue_mailbox_communication(mail.clone(), Default::default()).await;
    let mut first = Box::pin(session.maybe_start_turn_for_pending_work_with_sub_id("loser".into()));
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::select! {
            _ = &mut first => panic!("preparation must park in its callback"),
            entered = pause.entered.acquire() => entered.unwrap().forget(),
        }
    }).await.unwrap();
    assert_eq!(session.state.lock().await.last_started_turn_id, None);
    let second = session.new_turn_with_default_settings("replacement".into(), Default::default()).await;
    let mut winner = Box::pin(async {
        if registered_after_abort {
            // Model registration after try_spawn_task's initial cancel/abort.
            session.start_task(second, Vec::new(), HeldTask).await
        } else {
            session.try_spawn_task(second, Vec::new(), HeldTask).await
        }
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::select! {
            _ = &mut winner => panic!("winner must park in its callback"),
            entered = pause.entered.acquire() => entered.unwrap().forget(),
        }
    }).await.unwrap();
    // Losing cleanup runs while the winner is still task-less. It must not
    // clear the winner's different reservation or resume the losing callback.
    let winner_state = Arc::clone(&session.active_turn.lock().await.as_ref().unwrap().turn_state);
    tokio::time::timeout(Duration::from_secs(10), first).await.unwrap();
    assert!(Arc::ptr_eq(&session.active_turn.lock().await.as_ref().unwrap().turn_state, &winner_state));
    assert_eq!(pause.resumed.load(std::sync::atomic::Ordering::SeqCst), 0);
    pause.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(10), winner).await.unwrap().unwrap();
    assert_eq!(pause.resumed.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(pause.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(session.state.lock().await.last_started_turn_id.as_deref(), Some("replacement"));
    {
        let active = session.active_turn.lock().await;
        assert_eq!(active.as_ref().unwrap().task.as_ref().unwrap().turn_context.sub_id, "replacement");
    }
    assert_eq!(session.input_queue.drain_mailbox_input_items().await.0,
        vec![TurnInput::InterAgentCommunication(mail)]);
    session.close_task_admission().await;
    session.abort_all_tasks(codex_protocol::protocol::TurnAbortReason::Interrupted).await;
    let _ = session.task_joins.shutdown_until(tokio::time::Instant::now() + Duration::from_secs(5)).await;
    }
}

#[tokio::test]
async fn reserved_start_refuses_a_cleared_or_replaced_slot_before_callbacks() {
    for replacement in [false, true] {
        let (mut session, context) = tests::make_session_and_context().await;
        let pause = Arc::new(PausedStart {
            pause_calls: 1,
            calls: std::sync::atomic::AtomicUsize::new(0),
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
            resumed: std::sync::atomic::AtomicUsize::new(0),
        });
        let mut builder = codex_extension_api::ExtensionRegistryBuilder::<Config>::new();
        builder.turn_lifecycle_contributor(pause.clone());
        session.services.extensions = Arc::new(builder.build());
        let session = Arc::new(session);
        let original = crate::state::ActiveTurn::default();
        let reservation = Arc::clone(&original.turn_state);
        let successor = replacement.then(crate::state::ActiveTurn::default);
        let successor_state = successor.as_ref().map(|turn| Arc::clone(&turn.turn_state));
        *session.active_turn.lock().await = successor;
        let mail = InterAgentCommunication::new(
            codex_protocol::AgentPath::root(), codex_protocol::AgentPath::root(),
            Vec::new(), "restore refused start".into(), /*trigger_turn*/ true,
        );
        session.input_queue.enqueue_mailbox_communication(mail.clone(), Default::default()).await;
        let result = tokio::time::timeout(Duration::from_secs(10), session.start_reserved_task(
            Arc::new(context), Vec::new(), HeldTask, reservation,
        )).await.unwrap();
        assert!(result.is_err());
        assert_eq!(pause.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(session.state.lock().await.last_started_turn_id, None);
        let active = session.active_turn.lock().await;
        match successor_state {
            Some(expected) => assert!(Arc::ptr_eq(&active.as_ref().unwrap().turn_state, &expected)),
            None => assert!(active.is_none()),
        }
        drop(active);
        assert_eq!(session.input_queue.drain_mailbox_input_items().await.0,
            vec![TurnInput::InterAgentCommunication(mail)]);
    }
}

#[tokio::test]
async fn interrupted_mailbox_preparation_restores_mail_without_rearming_the_wake() {
    let (mut session, _context) = tests::make_session_and_context().await;
    let pause = Arc::new(PausedStart {
        pause_calls: 1,
        calls: std::sync::atomic::AtomicUsize::new(0),
        entered: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
        resumed: std::sync::atomic::AtomicUsize::new(0),
    });
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::<Config>::new();
    builder.turn_lifecycle_contributor(pause.clone());
    session.services.extensions = Arc::new(builder.build());
    let session = Arc::new(session);
    let mail = InterAgentCommunication::new(
        codex_protocol::AgentPath::root(), codex_protocol::AgentPath::root(),
        Vec::new(), "interrupted preparation".into(), /*trigger_turn*/ true,
    );
    session.input_queue.enqueue_mailbox_communication(mail.clone(), Default::default()).await;
    let mut first = Box::pin(session.maybe_start_turn_for_pending_work_with_sub_id("interrupted".into()));
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::select! {
            _ = &mut first => panic!("preparation must park in its callback"),
            entered = pause.entered.acquire() => entered.unwrap().forget(),
        }
    }).await.unwrap();
    session.interrupt_task().await;
    pause.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(10), first).await.unwrap();
    assert!(session.active_turn.lock().await.is_none());
    assert_eq!(session.state.lock().await.last_started_turn_id, None);
    assert!(session.input_queue.completion_wake.notified().now_or_never().is_none());
    assert_eq!(session.input_queue.drain_mailbox_input_items().await.0,
        vec![TurnInput::InterAgentCommunication(mail)]);
}
