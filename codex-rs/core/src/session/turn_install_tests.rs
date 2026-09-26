use super::*;
use futures::FutureExt;
use pretty_assertions::assert_eq;

#[tokio::test]
#[expect(clippy::await_holding_invalid_type, reason = "hold the destination to exercise commit cancellation")]
async fn cancelled_turn_commit_waiting_for_destination_preserves_previous_id_and_mail() {
    let (session, _context) = tests::make_session_and_context().await;
    session.state.lock().await.last_started_turn_id = Some("previous".into());
    let mail = InterAgentCommunication::new(
        codex_protocol::AgentPath::root(),
        codex_protocol::AgentPath::root(),
        Vec::new(),
        "waiting for destination".into(),
        /*trigger_turn*/ true,
    );
    session.input_queue.enqueue_mailbox_communication(mail.clone(), Default::default()).await;
    let mut active = session.active_turn.lock().await;
    *active = Some(ActiveTurn::default());
    let turn_state = Arc::clone(&active.as_ref().unwrap().turn_state);
    let destination = turn_state.lock().await;
    let mailbox = session.input_queue.reserve_mailbox();
    let mut commit = Box::pin(session.commit_started_turn("refused", &turn_state, mailbox));
    assert!(commit.as_mut().now_or_never().is_none());
    assert!(!session.input_queue.has_pending_mailbox_items().await);
    drop(commit);
    drop(destination);
    assert!(active.as_ref().unwrap().task.is_none());
    assert_eq!(session.state.lock().await.last_started_turn_id.as_deref(), Some("previous"));
    assert!(session.input_queue.take_pending_input_for_turn_state(&turn_state).await.is_empty());
    assert_eq!(session.input_queue.drain_mailbox_input_items().await.0,
        vec![TurnInput::InterAgentCommunication(mail)]);
}

#[tokio::test]
#[expect(clippy::await_holding_invalid_type, reason = "hold session state to exercise commit cancellation")]
async fn cancelled_turn_commit_waiting_for_session_state_preserves_previous_id_and_mail() {
    let (session, _context) = tests::make_session_and_context().await;
    let mail = InterAgentCommunication::new(
        codex_protocol::AgentPath::root(),
        codex_protocol::AgentPath::root(),
        Vec::new(),
        "waiting for session state".into(),
        /*trigger_turn*/ true,
    );
    session.input_queue.enqueue_mailbox_communication(mail.clone(), Default::default()).await;
    let mut active = session.active_turn.lock().await;
    *active = Some(ActiveTurn::default());
    let turn_state = Arc::clone(&active.as_ref().unwrap().turn_state);
    let mut state = session.state.lock().await;
    state.last_started_turn_id = Some("previous".into());
    let mailbox = session.input_queue.reserve_mailbox();
    let mut commit = Box::pin(session.commit_started_turn("refused", &turn_state, mailbox));
    assert!(commit.as_mut().now_or_never().is_none());
    drop(commit);
    assert_eq!(state.last_started_turn_id.as_deref(), Some("previous"));
    assert!(active.as_ref().unwrap().task.is_none());
    assert!(session.input_queue.take_pending_input_for_turn_state(&turn_state).await.is_empty());
    assert_eq!(session.input_queue.drain_mailbox_input_items().await.0,
        vec![TurnInput::InterAgentCommunication(mail)]);
}
