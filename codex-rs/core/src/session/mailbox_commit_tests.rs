use super::*;
use futures::FutureExt;
use pretty_assertions::assert_eq;

fn mail(content: &str) -> InterAgentCommunication {
    InterAgentCommunication::new(
        codex_protocol::AgentPath::root(), codex_protocol::AgentPath::root(),
        Vec::new(), content.to_owned(), /*trigger_turn*/ true,
    )
}

#[tokio::test]
async fn cancelled_commit_restores_batch_while_destination_is_locked() {
    let queue = InputQueue::new();
    let original = mail("held");
    queue.enqueue_mailbox_communication(original.clone(), TurnStartOptions::default()).await;
    let batch = queue.reserve_mailbox();
    let state = Mutex::new(TurnState::default());
    let locked = state.lock().await;
    let mut commit = Box::pin(queue.commit_mailbox_for_turn_state(&state, batch));
    assert!(commit.as_mut().now_or_never().is_none());
    assert!(!queue.has_pending_mailbox_items().await);
    drop(commit);
    drop(locked);
    assert!(queue.take_pending_input_for_turn_state(&state).await.is_empty());
    assert_eq!(queue.drain_mailbox_input_items().await.0,
        vec![TurnInput::InterAgentCommunication(original)]);
}

#[tokio::test]
async fn merged_private_batches_commit_fifo_exactly_once() {
    let queue = InputQueue::new();
    let first = mail("before discovery");
    let second = mail("during discovery");
    queue.enqueue_mailbox_communication(first.clone(), TurnStartOptions::default()).await;
    let mut batch = queue.reserve_mailbox();
    queue.enqueue_mailbox_communication(second.clone(), TurnStartOptions::default()).await;
    batch.append(queue.reserve_mailbox());
    assert!(queue.drain_mailbox_input_items().await.0.is_empty());
    let state = Mutex::new(TurnState::default());
    queue.commit_mailbox_for_turn_state(&state, batch).await;
    assert_eq!(queue.take_pending_input_for_turn_state(&state).await,
        vec![TurnInput::InterAgentCommunication(first), TurnInput::InterAgentCommunication(second)]);
    assert!(queue.take_pending_input_for_turn_state(&state).await.is_empty());
    assert!(!queue.has_pending_mailbox_items().await);
}

#[tokio::test]
async fn dropped_merged_batch_restores_original_order_with_later_arrivals() {
    let queue = InputQueue::new();
    let mails = [mail("one"), mail("two"), mail("three")];
    queue.enqueue_mailbox_communication(mails[0].clone(), TurnStartOptions::default()).await;
    let mut batch = queue.reserve_mailbox();
    queue.enqueue_mailbox_communication(mails[1].clone(), TurnStartOptions::default()).await;
    batch.append(queue.reserve_mailbox());
    queue.enqueue_mailbox_communication(mails[2].clone(), TurnStartOptions::default()).await;
    drop(batch);
    assert_eq!(queue.drain_mailbox_input_items().await.0,
        mails.into_iter().map(TurnInput::InterAgentCommunication).collect::<Vec<_>>());
}
