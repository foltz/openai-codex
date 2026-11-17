use super::*;
use crate::connection_rpc_gate::ConnectionRpcGate;
use crate::error_code::internal_error;
use crate::request_serialization::QueuedInitializedRequest;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

fn request_id() -> ConnectionRequestId {
    ConnectionRequestId {
        connection_id: ConnectionId(9),
        request_id: RequestId::Integer(3),
    }
}

#[tokio::test]
async fn queued_refusal_handoff_settles_ticket_and_clears_context() {
    let (sender, mut receiver) = mpsc::channel(4);
    let outgoing = Arc::new(OutgoingMessageSender::new(
        sender,
        codex_analytics::AnalyticsEventsClient::disabled(),
    ));
    let id = request_id();
    outgoing
        .register_request_context(RequestContext::new(
            id.clone(),
            "thread/read",
            tracing::Span::none(),
            None,
        ))
        .await;
    let gate = Arc::new(ConnectionRpcGate::new());
    let reply = outgoing.clone();
    let request = QueuedInitializedRequest::new(gate.clone(), async {
        panic!("closed queued request must not execute");
    })
    .with_refusal(async move {
        reply
            .send_error_delivered(id, internal_error("not admitted"))
            .await
    });
    gate.close().await;
    request.run().await;
    gate.queued_settled().await;
    assert_eq!(gate.unanswered(), 0);
    assert_eq!(outgoing.request_context_count().await, 0);
    match timeout(Duration::from_secs(1), receiver.recv())
        .await
        .unwrap()
        .unwrap()
    {
        OutgoingEnvelope::ToConnection {
            connection_id,
            message: OutgoingMessage::Error(error),
            write_complete_tx,
        } => {
            assert_eq!(connection_id, ConnectionId(9));
            assert_eq!(error.id, RequestId::Integer(3));
            assert_eq!(error.error.message, "not admitted");
            assert!(write_complete_tx.is_none());
        }
        _ => panic!("wrong refusal target or message"),
    }
}

#[tokio::test]
async fn closed_outbound_router_keeps_queued_refusal_unanswered() {
    let (sender, receiver) = mpsc::channel(4);
    drop(receiver);
    let outgoing = Arc::new(OutgoingMessageSender::new(
        sender,
        codex_analytics::AnalyticsEventsClient::disabled(),
    ));
    let gate = Arc::new(ConnectionRpcGate::new());
    let request = QueuedInitializedRequest::new(gate.clone(), async {
        panic!("closed queued request must not execute");
    })
    .with_refusal(async move {
        outgoing
            .send_error_delivered(request_id(), internal_error("not admitted"))
            .await
    });
    gate.close().await;
    request.run().await;
    gate.queued_settled().await;
    assert_eq!(gate.unanswered(), 1);
}

async fn process_refusal(write_completed: bool) {
    let (normal_sender, mut normal_receiver) = mpsc::channel(4);
    let (process_sender, mut process_receiver) = mpsc::channel(4);
    let outgoing = Arc::new(
        OutgoingMessageSender::new(
            normal_sender,
            codex_analytics::AnalyticsEventsClient::disabled(),
        )
        .with_process_reply_sender(process_sender),
    );
    let id = request_id();
    outgoing
        .register_request_context(RequestContext::new(
            id.clone(),
            "process/kill",
            tracing::Span::none(),
            None,
        ))
        .await;
    let sender = outgoing.clone();
    let sending = tokio::spawn(async move {
        sender
            .send_error_delivered(id, internal_error("not admitted"))
            .await
    });
    let envelope = timeout(Duration::from_secs(1), process_receiver.recv())
        .await
        .unwrap()
        .unwrap();
    let OutgoingEnvelope::ToConnection {
        connection_id,
        message: OutgoingMessage::Error(error),
        write_complete_tx,
    } = envelope
    else {
        panic!("wrong process refusal envelope");
    };
    assert_eq!(connection_id, ConnectionId(9));
    assert_eq!(error.id, RequestId::Integer(3));
    let acknowledgement = write_complete_tx.expect("process route preserves write acknowledgement");
    if write_completed {
        acknowledgement.send(()).unwrap();
    } else {
        drop(acknowledgement);
    }
    assert_eq!(
        timeout(Duration::from_secs(1), sending)
            .await
            .unwrap()
            .unwrap(),
        write_completed
    );
    assert!(normal_receiver.try_recv().is_err());
    assert_eq!(outgoing.request_context_count().await, 0);
}

#[tokio::test]
async fn process_refusal_uses_dedicated_channel_and_write_acknowledgement() {
    process_refusal(true).await;
}

#[tokio::test]
async fn dropped_process_write_acknowledgement_is_not_delivered_success() {
    process_refusal(false).await;
}
