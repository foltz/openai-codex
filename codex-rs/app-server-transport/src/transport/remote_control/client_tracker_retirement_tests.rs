use super::*;

#[tokio::test]
async fn shutdown_observes_detached_close_and_replays_delivery_failure() {
    let (server_events, _server_rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (events, events_rx) = mpsc::channel(CHANNEL_CAPACITY);
    let mut tracker = ClientTracker::new(server_events, events, &CancellationToken::new());
    drop(events_rx);
    // This is the same retained path used when initialize delivery times out.
    drop(tracker.spawn_connection_closed(next_connection_id()));
    assert!(!tracker.shutdown().await);
    assert!(!tracker.shutdown().await);
}

#[tokio::test]
async fn shutdown_waits_for_backpressured_close_without_abandoning_it() {
    let (server_events, _server_rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (events, mut events_rx) = mpsc::channel(1);
    let mut tracker = ClientTracker::new(server_events, events.clone(), &CancellationToken::new());
    let first = next_connection_id();
    let second = next_connection_id();
    events
        .send(TransportEvent::ConnectionClosed {
            connection_id: first,
        })
        .await
        .unwrap();
    drop(tracker.spawn_connection_closed(second));
    assert!(
        timeout(Duration::from_millis(10), tracker.shutdown())
            .await
            .is_err()
    );
    assert!(
        matches!(events_rx.recv().await, Some(TransportEvent::ConnectionClosed { connection_id }) if connection_id == first)
    );
    assert!(tracker.shutdown().await);
    assert!(
        matches!(events_rx.recv().await, Some(TransportEvent::ConnectionClosed { connection_id }) if connection_id == second)
    );
}
