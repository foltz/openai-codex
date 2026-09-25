use super::*;
use codex_protocol::AgentPath;
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
    mailbox.enqueue(original.clone(), TurnStartOptions {
        parent_turn_id: Some("parent".into()),
        root_turn_id: Some("root".into()),
        turn_trigger: Some("followup".into()),
        service_tier: Some("priority".into()),
        final_output_json_schema: Some(serde_json::json!({"type": "object"})),
        ..Default::default()
    });
    activity.borrow_and_update();
    let mut attempt = Box::pin(async {
        let reservation = mailbox.reserve();
        std::future::pending::<()>().await;
        reservation.into_input()
    });
    assert!(attempt.as_mut().now_or_never().is_none());
    assert!(!mailbox.has_pending());
    drop(attempt);
    assert!(mailbox.has_trigger());
    assert!(activity.has_changed().unwrap());
    let (items, options) = mailbox.reserve().into_input();
    assert_eq!(items, vec![TurnInput::InterAgentCommunication(original)]);
    assert_eq!(
        (options.parent_turn_id, options.root_turn_id, options.turn_trigger,
            options.service_tier, options.final_output_json_schema),
        (Some("parent".into()), Some("root".into()), Some("followup".into()),
            Some("priority".into()), Some(serde_json::json!({"type": "object"}))),
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
    assert_eq!(mailbox.reserve().into_input().0,
        vec![TurnInput::InterAgentCommunication(second)]);
    drop(reservation);
    assert_eq!(mailbox.reserve().into_input().0,
        vec![TurnInput::InterAgentCommunication(first)]);
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
        assert_eq!(mailbox.reserve().into_input().0,
            mails.into_iter().map(TurnInput::InterAgentCommunication).collect::<Vec<_>>());
        assert!(!mailbox.has_pending());
    }
}

#[test]
fn rollback_preserves_latest_trigger_settings_and_parent_consensus() {
    let (mailbox, _activity) = mailbox();
    mailbox.enqueue(mail("old"), TurnStartOptions {
        parent_turn_id: Some("parent-a".into()),
        root_turn_id: Some("root-a".into()),
        final_output_json_schema: Some(serde_json::json!({"type": "object"})),
        ..Default::default()
    });
    let reservation = mailbox.reserve();
    mailbox.enqueue(mail("new"), TurnStartOptions {
        parent_turn_id: Some("parent-b".into()),
        root_turn_id: Some("root-b".into()),
        ..Default::default()
    });
    drop(reservation);
    let (_, options) = mailbox.reserve().into_input();
    assert_eq!((options.parent_turn_id, options.root_turn_id, options.final_output_json_schema),
        (None, Some("root-a".into()), None));
}

#[test]
fn consumption_does_not_requeue_or_emit_a_rollback_wakeup() {
    let (mailbox, mut activity) = mailbox();
    mailbox.enqueue(mail("consume"), TurnStartOptions::default());
    activity.borrow_and_update();
    let (items, _) = mailbox.reserve().into_input();
    assert_eq!(items.len(), 1);
    assert!(!mailbox.has_pending());
    assert!(!activity.has_changed().unwrap());
}
