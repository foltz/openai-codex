use super::*;
use crate::outgoing_message::OutgoingError;
use crate::outgoing_message::OutgoingMessage;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::JSONRPCMessage;
use codex_app_server_protocol::JSONRPCRequest;
use codex_app_server_protocol::RequestId;
use pretty_assertions::assert_eq;
use std::time::Duration;

/// Websocket-level witness: the server stopped admitting (its event channel
/// is closed), the peer never reads (its outbound queue is full) and the
/// connection's writer stays open. The not-admitted refusal must not wait
/// past the latched owner deadline, and it is counted as abandoned output.
#[tokio::test(start_paused = true)]
async fn refusal_to_a_stalled_websocket_peer_is_counted_at_the_owner_deadline() {
    let (transport_event_tx, mut transport_event_rx) = mpsc::channel(1);
    transport_event_rx.close();
    // Capacity 1, filled, never drained; the receiver stays alive (open writer).
    let (writer_tx, _open_writer_rx) = mpsc::channel::<QueuedOutgoingMessage>(1);
    writer_tx
        .try_send(QueuedOutgoingMessage::new(OutgoingMessage::Error(
            OutgoingError {
                id: RequestId::Integer(1),
                error: JSONRPCErrorError {
                    code: -32603,
                    message: "earlier reply".to_string(),
                    data: None,
                },
            },
        )))
        .expect("fill the outbound queue");
    let (writer_control_tx, _writer_control_rx) = mpsc::channel::<TungsteniteWebSocketMessage>(1);
    // The caller owns the custody through the whole connection.
    let custody = crate::OwnerCustody::new(CancellationToken::new());
    let owner = custody.expiry();
    let start = tokio::time::Instant::now();
    let deadline = owner
        .arm(start + Duration::from_secs(35))
        .expect("the custody owner is live");
    let request = serde_json::to_string(&JSONRPCMessage::Request(JSONRPCRequest {
        id: RequestId::Integer(9),
        method: "config/read".to_string(),
        params: Some(serde_json::json!({ "includeLayers": false })),
        trace: None,
    }))
    .expect("serialize request");
    let reader = futures::stream::iter([Ok::<_, std::io::Error>(
        TungsteniteWebSocketMessage::text(request),
    )])
    .chain(futures::stream::pending());

    run_websocket_inbound_loop(
        reader,
        transport_event_tx,
        writer_tx,
        writer_control_tx,
        ConnectionId(7),
        CancellationToken::new(),
        None,
        owner.clone(),
    )
    .await;

    // Returned at the latched deadline: no fresh budget, no later polling.
    assert_eq!(tokio::time::Instant::now(), deadline);
    assert_eq!(owner.abandoned_output().messages, 1);
    assert!(owner.abandoned_output().bytes > 0);
    assert_eq!(owner.undeliverable_after_disconnect().messages, 0);
    assert_eq!(owner.returned_tasks(), vec!["not_admitted_refusal"]);
}
