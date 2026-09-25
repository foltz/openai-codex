use super::*;
use futures::FutureExt;
use pretty_assertions::assert_eq;

#[derive(Debug, PartialEq)]
enum WorkEvent {
    Bound(String),
    Retained,
    Dropped,
}

#[derive(Debug)]
struct RecordedWork {
    events: Arc<std::sync::Mutex<Vec<WorkEvent>>>,
    retained: bool,
}

impl codex_protocol::host_turn_work::HostTurnWork for RecordedWork {
    fn bind_submission(&mut self, turn_id: &str) {
        self.events.lock().unwrap().push(WorkEvent::Bound(turn_id.to_owned()));
    }

    fn retain_until_terminal(mut self: Box<Self>) {
        self.retained = true;
        self.events.lock().unwrap().push(WorkEvent::Retained);
    }
}

impl Drop for RecordedWork {
    fn drop(&mut self) {
        if !self.retained {
            self.events.lock().unwrap().push(WorkEvent::Dropped);
        }
    }
}

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
    let mut commit = Box::pin(queue.commit_mailbox_for_turn_state(&state, batch, "test-turn"));
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
    queue.commit_mailbox_for_turn_state(&state, batch, "test-turn").await;
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

#[tokio::test]
async fn cancelled_commit_keeps_work_with_restored_mail_until_actual_turn_commit() {
    let queue = InputQueue::new();
    let original = mail("admitted before close");
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    queue.enqueue_mailbox_with_work(original.clone(), TurnStartOptions::default(), Some(Box::new(RecordedWork {
        events: Arc::clone(&events), retained: false,
    })));
    let batch = queue.reserve_mailbox();
    assert!(batch.has_turn_work());
    assert!(!queue.reserve_mailbox().has_turn_work());
    let state = Mutex::new(TurnState::default());
    let locked = state.lock().await;
    let mut commit = Box::pin(queue.commit_mailbox_for_turn_state(&state, batch, "refused-turn"));
    assert!(commit.as_mut().now_or_never().is_none());
    drop(commit);
    drop(locked);
    assert!(events.lock().unwrap().is_empty());

    let restored = queue.reserve_mailbox();
    assert!(restored.has_turn_work());
    queue.commit_mailbox_for_turn_state(&state, restored, "installed-turn").await;
    assert_eq!(*events.lock().unwrap(), vec![WorkEvent::Bound("installed-turn".into()), WorkEvent::Retained]);
    assert_eq!(queue.take_pending_input_for_turn_state(&state).await,
        vec![TurnInput::InterAgentCommunication(original)]);
    assert!(!queue.reserve_mailbox().has_turn_work());
}

#[tokio::test]
async fn delivered_mail_binds_work_to_the_consuming_turn_not_the_sending_turn() {
    let queue = InputQueue::new();
    let original = mail("delivered during a turn");
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    queue.enqueue_mailbox_with_work(original.clone(), TurnStartOptions {
        parent_turn_id: Some("sending-turn".into()), ..Default::default()
    }, Some(Box::new(RecordedWork { events: Arc::clone(&events), retained: false })));
    let active = Mutex::new(Some(ActiveTurn::default()));
    let turn_state = Arc::clone(&active.lock().await.as_ref().unwrap().turn_state);
    queue.accept_mailbox_delivery_for_turn_state(&turn_state).await;
    assert_eq!(queue.get_pending_input(&active, "consuming-turn").await.0,
        vec![TurnInput::InterAgentCommunication(original)]);
    assert_eq!(*events.lock().unwrap(), vec![WorkEvent::Bound("consuming-turn".into()), WorkEvent::Retained]);
}

#[tokio::test]
async fn queued_work_is_not_released_by_reservation_drop_but_is_dropped_with_the_queue() {
    let queue = InputQueue::new();
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    queue.enqueue_mailbox_with_work(mail("never consumed"), TurnStartOptions::default(),
        Some(Box::new(RecordedWork { events: Arc::clone(&events), retained: false })));
    drop(queue.reserve_mailbox());
    assert!(events.lock().unwrap().is_empty());
    drop(queue);
    assert_eq!(*events.lock().unwrap(), vec![WorkEvent::Dropped]);
}
