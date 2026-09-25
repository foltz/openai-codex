use super::*;
use futures::FutureExt;
use std::time::Duration;

struct PausedStart {
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
        entered: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
        resumed: std::sync::atomic::AtomicUsize::new(0),
    });
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::<Config>::new();
    builder.turn_lifecycle_contributor(pause.clone());
    session.services.extensions = Arc::new(builder.build());
    let session = Arc::new(session);
    session.input_queue.enqueue_mailbox_communication(
        InterAgentCommunication::new(
            codex_protocol::AgentPath::root(), codex_protocol::AgentPath::root(),
            Vec::new(), "wake".to_owned(), /*trigger_turn*/ true,
        ),
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
