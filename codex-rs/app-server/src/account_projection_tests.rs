use super::*;
use codex_analytics::AnalyticsEventsClient;
use pretty_assertions::assert_eq;
use std::time::Duration;

#[tokio::test]
async fn reservation_holds_capacity_until_publish_or_drop() {
    let (tx, mut rx) = mpsc::channel(1);
    let outgoing = OutgoingMessageSender::new(tx, AnalyticsEventsClient::disabled());
    let deadline = Instant::now() + Duration::from_secs(5);
    let reserved = outgoing
        .reserve_account_projection_until(deadline)
        .await
        .unwrap();
    assert!(rx.try_recv().is_err());
    let waiting = outgoing.reserve_account_projection_until(deadline);
    tokio::pin!(waiting);
    assert!(futures::poll!(waiting.as_mut()).is_pending());
    drop(reserved);
    let reserved = waiting.await.unwrap();
    let notification = AccountUpdatedNotification {
        auth_mode: None,
        plan_type: None,
    };
    reserved.publish(notification.clone());
    let Some(OutgoingEnvelope::Broadcast { message }) = rx.recv().await else {
        panic!("expected account projection broadcast");
    };
    let super::super::OutgoingMessage::AppServerNotification(envelope) = message else {
        panic!("expected notification envelope");
    };
    let ServerNotification::AccountUpdated(actual) = envelope.notification else {
        panic!("expected account update, not another notification");
    };
    assert_eq!(actual, notification);
    assert!(envelope.emitted_at_ms.is_some());
    assert!(rx.try_recv().is_err());
}

#[tokio::test(start_paused = true)]
async fn reservation_refuses_closed_and_expired_queues_without_publication() {
    let (tx, mut rx) = mpsc::channel(1);
    let outgoing = OutgoingMessageSender::new(tx, AnalyticsEventsClient::disabled());
    let deadline = Instant::now() + Duration::from_secs(1);
    let held = outgoing
        .reserve_account_projection_until(deadline)
        .await
        .unwrap();
    assert_eq!(
        outgoing
            .reserve_account_projection_until(deadline)
            .await
            .err(),
        Some(AccountProjectionReservationError::TimedOut)
    );
    drop(held);
    assert!(rx.try_recv().is_err());
    assert_eq!(
        outgoing
            .reserve_account_projection_until(deadline)
            .await
            .err(),
        Some(AccountProjectionReservationError::TimedOut)
    );
    rx.close();
    assert_eq!(
        outgoing
            .reserve_account_projection_until(Instant::now() + Duration::from_secs(1))
            .await
            .err(),
        Some(AccountProjectionReservationError::Closed)
    );
}
