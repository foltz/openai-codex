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
            self.resumed
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
        codex_protocol::AgentPath::root(),
        codex_protocol::AgentPath::root(),
        Vec::new(),
        "wake".to_owned(),
        /*trigger_turn*/ true,
    );
    session
        .input_queue
        .enqueue_mailbox_communication(
            mail.clone(),
            codex_protocol::turn_input::TurnStartOptions::default(),
        )
        .await;
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
        Arc::clone(&session),
        context.config.clone(),
        receiver,
        None,
    ));
    tokio::time::timeout(Duration::from_secs(10), pause.entered.acquire())
        .await
        .expect("loop drove stored completion wake")
        .unwrap()
        .forget();
    assert!(
        session
            .active_turn
            .lock()
            .await
            .as_ref()
            .is_some_and(|turn| turn.task.is_none())
    );
    assert!(
        !session.input_queue.has_pending_mailbox_items().await,
        "preparing start privately owns its mail"
    );
    let (reply, response) = tokio::sync::oneshot::channel();
    sender
        .send(codex_protocol::protocol::Submission {
            id: "suspend-during-mailbox-start".into(),
            op: codex_protocol::protocol::Op::SuspendTurnAndShutdown { reply },
            trace: None,
            parent_turn_id: None,
            root_turn_id: None,
        })
        .await
        .unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(10), response)
            .await
            .expect("suspension remains dispatchable")
            .unwrap()
            .unwrap(),
        codex_protocol::turn_input::SuspendTurnOutcome::NotActive
    ));
    // Do not release the paused start. A queued shutdown must remain
    // dispatchable while the loop owns that preparation future.
    sender
        .send(codex_protocol::protocol::Submission {
            id: "shutdown-during-mailbox-start".into(),
            op: codex_protocol::protocol::Op::Shutdown,
            trace: None,
            parent_turn_id: None,
            root_turn_id: None,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), loop_task)
        .await
        .expect("shutdown preempts held mailbox preparation")
        .unwrap();
    assert!(session.active_turn.lock().await.is_none());
    pause.release.add_permits(1);
    tokio::task::yield_now().await;
    assert_eq!(pause.resumed.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(
        session.input_queue.drain_mailbox_input_items().await.0,
        vec![TurnInput::InterAgentCommunication(mail)]
    );
}

#[tokio::test]
async fn closed_submission_channel_takes_precedence_over_completion_wake() {
    let (session, context) = tests::make_session_and_context().await;
    let session = Arc::new(session);
    session.input_queue.completion_wake.notify_one();
    let (sender, receiver) = async_channel::bounded(1);
    drop(sender);
    tokio::time::timeout(
        Duration::from_secs(10),
        handlers::submission_loop(Arc::clone(&session), context.config.clone(), receiver, None),
    )
    .await
    .expect("wake does not retain the submission channel");
    assert!(
        session
            .input_queue
            .completion_wake
            .notified()
            .now_or_never()
            .is_some()
    );
    assert!(session.active_turn.lock().await.is_none());
}

struct HeldTask;

struct HoldInstalledTurn;

impl codex_extension_api::TurnLifecycleContributor for HoldInstalledTurn {
    fn turn_start_phase(
        &self,
        _store: &codex_extension_api::ExtensionData,
    ) -> codex_extension_api::TurnStartPhase {
        codex_extension_api::TurnStartPhase::RegularTaskStart
    }

    fn on_turn_start<'a>(
        &'a self,
        _input: codex_extension_api::TurnStartInput<'a>,
    ) -> codex_extension_api::ExtensionFuture<'a, ()> {
        Box::pin(std::future::pending())
    }
}

impl crate::tasks::SessionTask for HeldTask {
    fn kind(&self) -> crate::state::TaskKind {
        crate::state::TaskKind::Regular
    }

    fn span_name(&self) -> &'static str {
        "session_task.mailbox_reservation_test"
    }

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
            codex_protocol::AgentPath::root(),
            codex_protocol::AgentPath::root(),
            Vec::new(),
            "private first batch".to_owned(),
            /*trigger_turn*/ true,
        );
        session
            .input_queue
            .enqueue_mailbox_communication(mail.clone(), Default::default())
            .await;
        let mut first =
            Box::pin(session.maybe_start_turn_for_pending_work_with_sub_id("loser".into()));
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                _ = &mut first => panic!("preparation must park in its callback"),
                entered = pause.entered.acquire() => entered.unwrap().forget(),
            }
        })
        .await
        .unwrap();
        assert_eq!(session.state.lock().await.last_started_turn_id, None);
        let second = session
            .new_turn_with_default_settings("replacement".into(), Default::default())
            .await;
        let mut winner = Box::pin(async {
            if registered_after_abort {
                // Model registration after try_spawn_task's initial cancel/abort.
                session.start_task(second, Vec::new(), HeldTask).await
            } else {
                session.try_spawn_task(second, Vec::new(), HeldTask).await
            }
        });
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(first, async {
                tokio::select! {
                    _ = &mut winner => panic!("winner must park in its callback"),
                    entered = pause.entered.acquire() => entered.unwrap().forget(),
                }
            });
        })
        .await
        .unwrap();
        // The winner cannot enter callbacks until the losing owner has dropped.
        // Its distinct reservation remains intact while its own callback waits.
        let winner_state = Arc::clone(
            &session
                .active_turn
                .lock()
                .await
                .as_ref()
                .unwrap()
                .turn_state,
        );
        assert!(Arc::ptr_eq(
            &session
                .active_turn
                .lock()
                .await
                .as_ref()
                .unwrap()
                .turn_state,
            &winner_state
        ));
        assert_eq!(pause.resumed.load(std::sync::atomic::Ordering::SeqCst), 0);
        pause.release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(10), winner)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pause.resumed.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(pause.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(
            session.state.lock().await.last_started_turn_id.as_deref(),
            Some("replacement")
        );
        {
            let active = session.active_turn.lock().await;
            assert_eq!(
                active
                    .as_ref()
                    .unwrap()
                    .task
                    .as_ref()
                    .unwrap()
                    .turn_context
                    .sub_id,
                "replacement"
            );
        }
        // The witness restores before the winner's final mailbox capture, so the
        // installed turn owns the batch exactly once, rather than leaving it queued.
        assert_eq!(
            session
                .input_queue
                .take_pending_input_for_turn_state(winner_state.as_ref())
                .await,
            vec![TurnInput::InterAgentCommunication(mail)]
        );
        assert!(
            session
                .input_queue
                .drain_mailbox_input_items()
                .await
                .0
                .is_empty()
        );
        session.close_task_admission().await;
        session
            .abort_all_tasks(codex_protocol::protocol::TurnAbortReason::Interrupted)
            .await;
        let _ = session
            .task_joins
            .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(5))
            .await;
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
            codex_protocol::AgentPath::root(),
            codex_protocol::AgentPath::root(),
            Vec::new(),
            "restore refused start".into(),
            /*trigger_turn*/ true,
        );
        session
            .input_queue
            .enqueue_mailbox_communication(mail.clone(), Default::default())
            .await;
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            session.start_reserved_task(Arc::new(context), Vec::new(), HeldTask, reservation),
        )
        .await
        .unwrap();
        assert!(result.is_err());
        assert_eq!(pause.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(session.state.lock().await.last_started_turn_id, None);
        {
            let active = session.active_turn.lock().await;
            match successor_state {
                Some(expected) => {
                    assert!(Arc::ptr_eq(&active.as_ref().unwrap().turn_state, &expected))
                }
                None => assert!(active.is_none()),
            }
        }
        assert_eq!(
            session.input_queue.drain_mailbox_input_items().await.0,
            vec![TurnInput::InterAgentCommunication(mail)]
        );
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
        codex_protocol::AgentPath::root(),
        codex_protocol::AgentPath::root(),
        Vec::new(),
        "interrupted preparation".into(),
        /*trigger_turn*/ true,
    );
    session
        .input_queue
        .enqueue_mailbox_communication(mail.clone(), Default::default())
        .await;
    let mut first =
        Box::pin(session.maybe_start_turn_for_pending_work_with_sub_id("interrupted".into()));
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::select! {
            _ = &mut first => panic!("preparation must park in its callback"),
            entered = pause.entered.acquire() => entered.unwrap().forget(),
        }
    })
    .await
    .unwrap();
    session.interrupt_task().await;
    pause.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(10), first)
        .await
        .unwrap();
    assert!(session.active_turn.lock().await.is_none());
    assert_eq!(session.state.lock().await.last_started_turn_id, None);
    assert!(
        session
            .input_queue
            .completion_wake
            .notified()
            .now_or_never()
            .is_none()
    );
    assert_eq!(
        session.input_queue.drain_mailbox_input_items().await.0,
        vec![TurnInput::InterAgentCommunication(mail)]
    );
}

#[tokio::test]
async fn off_loop_replacers_wait_without_owning_slots_and_release_mail_on_cancel() {
    let (mut session, _) = tests::make_session_and_context().await;
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
        codex_protocol::AgentPath::root(),
        codex_protocol::AgentPath::root(),
        Vec::new(),
        "cancelled replacement".into(),
        /*trigger_turn*/ true,
    );
    session
        .input_queue
        .enqueue_mailbox_communication(mail.clone(), Default::default())
        .await;
    let mut first = Box::pin(session.maybe_start_turn_for_pending_work_with_sub_id("loser".into()));
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::select! {
            _ = &mut first => panic!("preparation must park"),
            entered = pause.entered.acquire() => entered.unwrap().forget(),
        }
    })
    .await
    .unwrap();
    let original = Arc::clone(
        &session
            .active_turn
            .lock()
            .await
            .as_ref()
            .unwrap()
            .turn_state,
    );
    let mut replacements = Vec::new();
    for id in ["first replacement", "second replacement"] {
        let context = session
            .new_turn_with_default_settings(id.into(), Default::default())
            .await;
        let replacing_session = Arc::clone(&session);
        let replacement = tokio::spawn(async move {
            let mut replacement = Box::pin(async move {
                replacing_session
                    .start_task(context, Vec::new(), HeldTask)
                    .await
            });
            // No locks are contended and New must stop at its completion
            // witness before plugin awaits or a replacement slot exist.
            assert!(replacement.as_mut().now_or_never().is_none());
            replacement
        })
        .await
        .unwrap();
        assert!(Arc::ptr_eq(
            &session
                .active_turn
                .lock()
                .await
                .as_ref()
                .unwrap()
                .turn_state,
            &original
        ));
        replacements.push(replacement);
    }
    drop(replacements.pop());
    tokio::time::timeout(Duration::from_secs(10), first)
        .await
        .unwrap();
    assert!(session.active_turn.lock().await.is_none());
    assert!(
        session
            .input_queue
            .completion_wake
            .notified()
            .now_or_never()
            .is_some()
    );
    session
        .maybe_start_turn_for_pending_work_with_sub_id("still blocked".into())
        .await;
    assert_eq!(pause.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(session.input_queue.has_pending_mailbox_items().await);
    assert!(
        session
            .input_queue
            .completion_wake
            .notified()
            .now_or_never()
            .is_none()
    );
    drop(replacements);
    assert!(
        session
            .input_queue
            .completion_wake
            .notified()
            .now_or_never()
            .is_some()
    );
    let mut retry = Box::pin(session.maybe_start_turn_for_pending_work_with_sub_id("retry".into()));
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::select! {
            _ = &mut retry => panic!("restored mail must reach preparation"),
            entered = pause.entered.acquire() => entered.unwrap().forget(),
        }
    })
    .await
    .unwrap();
    assert_eq!(pause.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    session.close_task_admission().await;
    drop(retry);
    assert_eq!(
        session.input_queue.drain_mailbox_input_items().await.0,
        vec![TurnInput::InterAgentCommunication(mail)]
    );
}

#[tokio::test]
async fn interrupted_registration_defers_new_trigger_and_idle_start_until_owner_drops() {
    let (mut session, _) = tests::make_session_and_context().await;
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
        codex_protocol::AgentPath::root(),
        codex_protocol::AgentPath::root(),
        Vec::new(),
        "interrupted owner".into(),
        /*trigger_turn*/ true,
    );
    session
        .input_queue
        .enqueue_mailbox_communication(mail.clone(), Default::default())
        .await;
    let mut first =
        Box::pin(session.maybe_start_turn_for_pending_work_with_sub_id("interrupted".into()));
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::select! {
            _ = &mut first => panic!("preparation must park"),
            entered = pause.entered.acquire() => entered.unwrap().forget(),
        }
    })
    .await
    .unwrap();
    session.interrupt_task().await;
    let result = turn_input::handle(
        &session,
        codex_protocol::turn_input::TurnInputRequest::user_input(vec![UserInput::Text {
            text: "idle contender".into(),
            text_elements: Vec::new(),
        }]),
        codex_protocol::turn_input::TurnInputMode::StartIfIdle,
        "idle contender".into(),
    )
    .await
    .unwrap();
    assert_eq!(
        result,
        codex_protocol::turn_input::TurnInputSubmission::NotSubmitted {
            reason: codex_protocol::turn_input::NotSubmittedReason::NotIdle,
        }
    );
    session
        .input_queue
        .enqueue_mailbox_communication(mail.clone(), Default::default())
        .await;
    session
        .maybe_start_turn_for_pending_work_with_sub_id("new trigger".into())
        .await;
    assert_eq!(pause.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    pause.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(10), first)
        .await
        .unwrap();
    assert!(session.active_turn.lock().await.is_none());
    assert!(
        session
            .input_queue
            .completion_wake
            .notified()
            .now_or_never()
            .is_some()
    );
    let mut retry =
        Box::pin(session.maybe_start_turn_for_pending_work_with_sub_id("deferred trigger".into()));
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::select! {
            _ = &mut retry => panic!("deferred trigger must reach preparation"),
            entered = pause.entered.acquire() => entered.unwrap().forget(),
        }
    })
    .await
    .unwrap();
    assert_eq!(pause.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    session.close_task_admission().await;
    drop(retry);
    assert_eq!(
        session.input_queue.drain_mailbox_input_items().await.0,
        vec![
            TurnInput::InterAgentCommunication(mail.clone()),
            TurnInput::InterAgentCommunication(mail),
        ]
    );
}

#[tokio::test]
async fn replacement_observes_idle_start_completion_without_cancelling_its_input() {
    let (mut session, _) = tests::make_session_and_context().await;
    let pause = Arc::new(PausedStart {
        pause_calls: 1,
        calls: std::sync::atomic::AtomicUsize::new(0),
        entered: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
        resumed: std::sync::atomic::AtomicUsize::new(0),
    });
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::<Config>::new();
    builder.turn_lifecycle_contributor(pause.clone());
    builder.turn_lifecycle_contributor(Arc::new(HoldInstalledTurn));
    session.services.extensions = Arc::new(builder.build());
    let session = Arc::new(session);
    let mut idle = Box::pin(turn_input::handle(
        &session,
        codex_protocol::turn_input::TurnInputRequest::user_input(vec![UserInput::Text {
            text: "idle input".into(),
            text_elements: Vec::new(),
        }]),
        codex_protocol::turn_input::TurnInputMode::StartIfIdle,
        "idle owner".into(),
    ));
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::select! {
            _ = &mut idle => panic!("idle start must park"),
            entered = pause.entered.acquire() => entered.unwrap().forget(),
        }
    })
    .await
    .unwrap();
    let original = Arc::clone(
        &session
            .active_turn
            .lock()
            .await
            .as_ref()
            .unwrap()
            .turn_state,
    );
    let context = session
        .new_turn_with_default_settings("replacement".into(), Default::default())
        .await;
    let replacing_session = Arc::clone(&session);
    let replacement = tokio::spawn(async move {
        let mut replacement = Box::pin(async move {
            replacing_session
                .start_task(context, Vec::new(), HeldTask)
                .await
        });
        assert!(replacement.as_mut().now_or_never().is_none());
        replacement
    })
    .await
    .unwrap();
    assert!(Arc::ptr_eq(
        &session
            .active_turn
            .lock()
            .await
            .as_ref()
            .unwrap()
            .turn_state,
        &original
    ));
    pause.release.add_permits(1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), idle)
            .await
            .unwrap()
            .unwrap(),
        codex_protocol::turn_input::TurnInputSubmission::Started {
            turn_id: "idle owner".into()
        }
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(10), replacement)
            .await
            .unwrap()
            .is_err()
    );
    assert_eq!(pause.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
        session.state.lock().await.last_started_turn_id.as_deref(),
        Some("idle owner")
    );
    session.close_task_admission().await;
    session
        .abort_all_tasks(codex_protocol::protocol::TurnAbortReason::Interrupted)
        .await;
    let _ = session
        .task_joins
        .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(5))
        .await;
}
