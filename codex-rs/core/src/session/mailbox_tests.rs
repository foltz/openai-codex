use super::*;
use codex_protocol::AgentPath;
use codex_protocol::protocol::InterAgentCommunication;
use futures::FutureExt;
use pretty_assertions::assert_eq;

fn mail(content: &str) -> InterAgentCommunication {
    InterAgentCommunication::new(
        AgentPath::root(),
        AgentPath::try_from("/root/worker").unwrap(),
        Vec::new(),
        content.to_owned(),
        /*trigger_turn*/ true,
    )
}

fn mailbox() -> (Mailbox, watch::Receiver<InputQueueActivity>) {
    let (activity, receiver) = watch::channel(InputQueueActivity::Mailbox);
    (Mailbox::new(activity), receiver)
}

#[test]
fn cancelled_future_restores_complete_mail_and_start_options() {
    let (mailbox, mut activity) = mailbox();
    let original = mail("reserved");
    mailbox.enqueue(
        original.clone(),
        TurnStartOptions {
            parent_turn_id: Some("parent".into()),
            root_turn_id: Some("root".into()),
            turn_trigger: Some("followup".into()),
            service_tier: Some("priority".into()),
            final_output_json_schema: Some(serde_json::json!({"type": "object"})),
            ..Default::default()
        },
    );
    activity.borrow_and_update();
    let mut attempt = Box::pin(async {
        let reservation = mailbox.reserve();
        std::future::pending::<()>().await;
        reservation.into_input("test-turn")
    });
    assert!(attempt.as_mut().now_or_never().is_none());
    assert!(!mailbox.has_pending());
    drop(attempt);
    assert!(mailbox.has_trigger());
    assert!(activity.has_changed().unwrap());
    let (items, options) = mailbox.reserve().into_input("test-turn");
    assert_eq!(
        items,
        vec![TurnInput::InterAgentCommunication(original.into())]
    );
    assert_eq!(
        (
            options.parent_turn_id,
            options.root_turn_id,
            options.turn_trigger,
            options.service_tier,
            options.final_output_json_schema
        ),
        (
            Some("parent".into()),
            Some("root".into()),
            Some("followup".into()),
            Some("priority".into()),
            Some(serde_json::json!({"type": "object"}))
        ),
    );
    assert!(!mailbox.has_pending());
}

#[test]
fn competing_consumer_cannot_take_a_private_reservation() {
    let (mailbox, _activity) = mailbox();
    let first = mail("first");
    let second = mail("second");
    mailbox.enqueue(first.clone(), TurnStartOptions::default());
    let reservation = mailbox.reserve();
    mailbox.enqueue(second.clone(), TurnStartOptions::default());
    assert_eq!(
        mailbox.reserve().into_input("test-turn").0,
        vec![TurnInput::InterAgentCommunication(second.into())]
    );
    drop(reservation);
    assert_eq!(
        mailbox.reserve().into_input("test-turn").0,
        vec![TurnInput::InterAgentCommunication(first.into())]
    );
    assert!(!mailbox.has_pending());
}

#[test]
fn reservations_restore_fifo_regardless_of_drop_order() {
    for oldest_first in [false, true] {
        let (mailbox, _activity) = mailbox();
        let mails = [mail("one"), mail("two"), mail("three"), mail("four")];
        mailbox.enqueue(mails[0].clone(), TurnStartOptions::default());
        mailbox.enqueue(mails[1].clone(), TurnStartOptions::default());
        let first = mailbox.reserve();
        mailbox.enqueue(mails[2].clone(), TurnStartOptions::default());
        let second = mailbox.reserve();
        mailbox.enqueue(mails[3].clone(), TurnStartOptions::default());
        if oldest_first {
            drop(first);
            drop(second);
        } else {
            drop(second);
            drop(first);
        }
        assert_eq!(
            mailbox.reserve().into_input("test-turn").0,
            mails
                .into_iter()
                .map(|mail| TurnInput::InterAgentCommunication(mail.into()))
                .collect::<Vec<_>>()
        );
        assert!(!mailbox.has_pending());
    }
}

#[test]
fn rollback_preserves_latest_trigger_settings_and_parent_consensus() {
    let (mailbox, _activity) = mailbox();
    mailbox.enqueue(
        mail("old"),
        TurnStartOptions {
            parent_turn_id: Some("parent-a".into()),
            root_turn_id: Some("root-a".into()),
            final_output_json_schema: Some(serde_json::json!({"type": "object"})),
            ..Default::default()
        },
    );
    let reservation = mailbox.reserve();
    mailbox.enqueue(
        mail("new"),
        TurnStartOptions {
            parent_turn_id: Some("parent-b".into()),
            root_turn_id: Some("root-b".into()),
            ..Default::default()
        },
    );
    drop(reservation);
    let (_, options) = mailbox.reserve().into_input("test-turn");
    assert_eq!(
        (
            options.parent_turn_id,
            options.root_turn_id,
            options.final_output_json_schema
        ),
        (None, Some("root-a".into()), None)
    );
}

#[test]
fn consumption_does_not_requeue_or_emit_a_rollback_wakeup() {
    let (mailbox, mut activity) = mailbox();
    mailbox.enqueue(mail("consume"), TurnStartOptions::default());
    activity.borrow_and_update();
    let (items, _) = mailbox.reserve().into_input("test-turn");
    assert_eq!(items.len(), 1);
    assert!(!mailbox.has_pending());
    assert!(!activity.has_changed().unwrap());
}

#[test]
fn rollback_preserves_mail_after_mutex_poisoning() {
    let (mailbox, mut activity) = mailbox();
    let original = mail("reserved before poison");
    mailbox.enqueue(original.clone(), TurnStartOptions::default());
    let reservation = mailbox.reserve();
    activity.borrow_and_update();
    let state = Arc::clone(&mailbox.state);
    assert!(
        std::thread::spawn(move || {
            let _guard = state.lock().unwrap();
            panic!("injected mailbox poison");
        })
        .join()
        .is_err()
    );

    drop(reservation);

    let state = mailbox
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(
        state
            .pending
            .values()
            .map(|mail| mail.communication.clone())
            .collect::<Vec<_>>(),
        vec![crate::PendingAgentMessage::from(original)]
    );
    assert!(mailbox.state.is_poisoned());
    assert!(activity.has_changed().unwrap());
}

#[test]
fn eviction_fence_rolls_back_unless_committed() {
    let (mailbox, _activity) = mailbox();
    let mut queued = mail("queued");
    queued.trigger_turn = false;
    mailbox.enqueue(queued.clone(), TurnStartOptions::default());
    let fence = mailbox
        .try_fence_eviction()
        .expect("idle mailbox can be fenced");
    assert!(mailbox.reserve().is_empty());
    drop(fence);
    assert_eq!(
        mailbox.reserve().into_input("turn").0,
        vec![TurnInput::InterAgentCommunication(queued.into())]
    );
    let mut fence = mailbox
        .try_fence_eviction()
        .expect("empty mailbox can be fenced");
    fence.commit();
    drop(fence);
    assert!(mailbox.eviction_fenced());
}

#[test]
fn append_transfers_private_reservation_count_to_empty_receiver() {
    let (mailbox, _activity) = mailbox();
    let mut empty = mailbox.reserve();
    let mut queued = mail("reserved");
    queued.trigger_turn = false;
    mailbox.enqueue(queued, TurnStartOptions::default());
    empty.append(mailbox.reserve());
    assert!(mailbox.try_fence_eviction().is_none());
    drop(empty);
    assert!(mailbox.try_fence_eviction().is_some());
}

#[test]
fn refused_eviction_drain_returns_uncommitted_mail_for_cold_reload() {
    let (mailbox, _activity) = mailbox();
    let mut trigger = mail("trigger");
    trigger.trigger_turn = true;
    mailbox.enqueue(trigger.clone(), TurnStartOptions::default());
    assert!(mailbox.try_fence_eviction().is_none());
    let (pending, _) = match mailbox.drain_for_eviction() {
        Err(refused) => refused,
        Ok(_) => panic!("trigger work must refuse clean eviction"),
    };
    assert_eq!(
        pending
            .into_iter()
            .map(|mail| mail.communication)
            .collect::<Vec<_>>(),
        [crate::PendingAgentMessage::from(trigger)]
    );
    assert!(!mailbox.has_pending());
}

#[tokio::test]
async fn rollback_wakes_a_fenced_mailbox_observer() {
    let (mailbox, mut activity) = mailbox();
    let wake = Arc::new(tokio::sync::Notify::new());
    let fence = mailbox
        .try_fence_eviction()
        .unwrap()
        .with_wake(Arc::clone(&wake));
    activity.borrow_and_update();
    assert!(mailbox.reserve().is_empty());
    drop(fence);
    tokio::time::timeout(std::time::Duration::from_secs(1), wake.notified())
        .await
        .unwrap();
    assert!(activity.has_changed().unwrap());
    assert!(!mailbox.eviction_fenced());
}

#[tokio::test]
async fn aborting_a_task_restores_its_reservation_and_releases_the_count() {
    let (mailbox, _activity) = mailbox();
    let mut original = mail("abort");
    original.trigger_turn = false;
    mailbox.enqueue(original.clone(), TurnStartOptions::default());
    let reservation = mailbox.reserve();
    let (ready, started) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let _reservation = reservation;
        ready.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    started.await.unwrap();
    assert!(mailbox.try_fence_eviction().is_none());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let fence = mailbox
        .try_fence_eviction()
        .expect("abort releases custody");
    drop(fence);
    assert_eq!(
        mailbox.reserve().into_input("turn").0,
        vec![TurnInput::InterAgentCommunication(original.into())]
    );
}

#[derive(Debug)]
struct CountedWork {
    live: Arc<std::sync::atomic::AtomicUsize>,
    events: Arc<Mutex<Vec<String>>>,
}

impl Drop for CountedWork {
    fn drop(&mut self) {
        self.live.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        self.events.lock().unwrap().push("drop-work".into());
    }
}

impl HostTurnWork for CountedWork {
    fn bind_submission(&mut self, turn_id: &str) {
        self.events.lock().unwrap().push(format!("bound:{turn_id}"));
    }
    fn retain_until_terminal(self: Box<Self>) {
        self.events.lock().unwrap().push("retained".into());
    }
}

fn counted_work() -> (
    Box<dyn HostTurnWork>,
    Arc<std::sync::atomic::AtomicUsize>,
    Arc<Mutex<Vec<String>>>,
) {
    let live = Arc::new(std::sync::atomic::AtomicUsize::new(1));
    let events = Arc::new(Mutex::new(Vec::new()));
    (
        Box::new(CountedWork {
            live: Arc::clone(&live),
            events: Arc::clone(&events),
        }),
        live,
        events,
    )
}

#[test]
fn cancelled_reservation_restores_counted_work_and_only_consumption_binds_it() {
    let (mailbox, _activity) = mailbox();
    let (work, live, events) = counted_work();
    mailbox.enqueue_with_work(mail("accounted"), TurnStartOptions::default(), Some(work));
    let first = mailbox.reserve();
    assert!(first.has_turn_work());
    assert!(!mailbox.has_pending());
    assert_eq!(live.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(events.lock().unwrap().is_empty());
    drop(first);
    assert!(mailbox.has_pending());
    assert_eq!(live.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(events.lock().unwrap().is_empty());
    let (items, _) = mailbox.reserve().into_input("actual-consuming-turn");
    assert_eq!(items.len(), 1);
    assert_eq!(
        *events.lock().unwrap(),
        ["bound:actual-consuming-turn", "retained", "drop-work"]
    );
    assert_eq!(live.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(!mailbox.has_pending());
}

#[test]
fn compatibility_drain_refuses_counted_mail_without_consuming_or_poisoning_it() {
    let (mailbox, _activity) = mailbox();
    let (work, live, events) = counted_work();
    mailbox.enqueue_with_work(mail("accounted"), TurnStartOptions::default(), Some(work));
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| mailbox.drain_unaccounted()))
            .is_err()
    );
    assert!(!mailbox.state.is_poisoned());
    assert_eq!(live.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(events.lock().unwrap().is_empty());
    let reservation = mailbox.reserve();
    assert!(reservation.has_turn_work());
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| mailbox.drain_unaccounted()))
            .is_err()
    );
    drop(reservation);
    assert!(mailbox.reserve().has_turn_work());
    assert_eq!(live.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[test]
fn queue_only_mail_cannot_take_a_counted_turn_lease() {
    let (mailbox, _activity) = mailbox();
    let (work, live, events) = counted_work();
    let mut queued = mail("queue-only");
    queued.trigger_turn = false;
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            mailbox.enqueue_with_work(queued, TurnStartOptions::default(), Some(work));
        }))
        .is_err()
    );
    assert!(!mailbox.state.is_poisoned());
    assert_eq!(live.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(*events.lock().unwrap(), ["drop-work"]);
    assert!(!mailbox.has_pending());
}

#[test]
fn message_lifecycle_attachments_survive_rollback_and_release_outside_queue_lock() {
    struct ReleaseProbe {
        state: std::sync::Weak<Mutex<MailboxState>>,
        events: Arc<Mutex<Vec<&'static str>>>,
    }
    impl Drop for ReleaseProbe {
        fn drop(&mut self) {
            let state = self.state.upgrade().expect("queue is still alive");
            assert!(
                state.try_lock().is_ok(),
                "attachment destructor must not run under queue lock"
            );
            self.events.lock().unwrap().push("released");
        }
    }
    struct Lifecycle {
        state: std::sync::Weak<Mutex<MailboxState>>,
        events: Arc<Mutex<Vec<&'static str>>>,
    }
    impl codex_extension_api::MessageLifecycleContributor for Lifecycle {
        fn on_publish(&self, input: &codex_extension_api::MessageLifecycleInput) {
            self.events.lock().unwrap().push("published");
            input.data.insert(ReleaseProbe {
                state: self.state.clone(),
                events: Arc::clone(&self.events),
            });
        }
        fn on_consumed(&self, _: &codex_extension_api::MessageLifecycleInput, _: ThreadId) {
            self.events.lock().unwrap().push("consumed");
        }
    }
    let (mailbox, _activity) = mailbox();
    let events = Arc::new(Mutex::new(Vec::new()));
    let contributor: Arc<dyn codex_extension_api::MessageLifecycleContributor> =
        Arc::new(Lifecycle {
            state: Arc::downgrade(&mailbox.state),
            events: Arc::clone(&events),
        });
    let delivery =
        crate::agent_communication::MessageDelivery::publish(ThreadId::new(), &[contributor]);
    let message = crate::PendingAgentMessage::new(mail("lifecycle"), Some(delivery));
    mailbox.enqueue(message, TurnStartOptions::default());
    drop(mailbox.reserve());
    assert_eq!(*events.lock().unwrap(), ["published"]);
    let (items, _) = mailbox.reserve().into_input("turn");
    assert_eq!(
        *events.lock().unwrap(),
        ["published"],
        "reservation commit is not history consumption"
    );
    let TurnInput::InterAgentCommunication(message) = &items[0] else {
        panic!("expected mail");
    };
    let delivery = message.delivery.as_ref().unwrap();
    delivery.consumed(ThreadId::new());
    assert_eq!(
        *events.lock().unwrap(),
        ["published", "consumed", "released"]
    );
    delivery.consumed(ThreadId::new());
    drop(items);
    assert_eq!(
        *events.lock().unwrap(),
        ["published", "consumed", "released"]
    );
}
