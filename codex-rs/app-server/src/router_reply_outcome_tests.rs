use super::*;
use codex_app_server_protocol::RequestId;
use codex_app_server_transport::OutgoingError;
fn reply() -> OutgoingMessage {
    OutgoingMessage::Error(OutgoingError {
        id: RequestId::Integer(1),
        error: crate::error_code::internal_error("test refusal"),
    })
}
fn state(
    tx: mpsc::Sender<QueuedOutgoingMessage>,
    token: Option<CancellationToken>,
) -> OutboundConnectionState {
    OutboundConnectionState::new(
        tx,
        Arc::new(AtomicBool::new(true)),
        Arc::new(AtomicBool::new(true)),
        Arc::new(RwLock::new(HashSet::new())),
        token,
    )
}
async fn route(c: &mut HashMap<ConnectionId, OutboundConnectionState>) -> Option<ReplyDrop> {
    route_outgoing_envelope(
        c,
        OutgoingEnvelope::ToConnection {
            connection_id: ConnectionId(1),
            message: reply(),
            write_complete_tx: None,
        },
    )
    .await
}
#[tokio::test]
async fn absent_connection_reports_reply_loss() {
    assert_eq!(
        route(&mut HashMap::new()).await,
        Some(ReplyDrop::Unregistered(ConnectionId(1)))
    );
}
#[tokio::test]
async fn closed_writer_reports_refusal_without_disconnect_claim() {
    let (tx, rx) = mpsc::channel(1);
    drop(rx);
    let mut c = HashMap::from([(ConnectionId(1), state(tx, None))]);
    assert_eq!(
        route(&mut c).await,
        Some(ReplyDrop::WriterRefused {
            connection_id: ConnectionId(1),
            disconnectable: false
        })
    );
    assert!(c.is_empty());
}
#[tokio::test]
async fn full_writer_reports_refusal_and_requests_disconnect() {
    let (tx, mut rx) = mpsc::channel(1);
    tx.try_send(QueuedOutgoingMessage::new(reply())).unwrap();
    let token = CancellationToken::new();
    let mut c = HashMap::from([(ConnectionId(1), state(tx, Some(token.clone())))]);
    assert_eq!(
        route(&mut c).await,
        Some(ReplyDrop::WriterRefused {
            connection_id: ConnectionId(1),
            disconnectable: true
        })
    );
    assert!(token.is_cancelled());
    assert!(c.is_empty());
    assert!(rx.recv().await.is_some());
    assert!(rx.recv().await.is_none());
}
#[tokio::test]
async fn queued_reply_preserves_writer_ack_custody() {
    let (tx, mut rx) = mpsc::channel(1);
    let mut c = HashMap::from([(ConnectionId(1), state(tx, None))]);
    let (ack, mut done) = tokio::sync::oneshot::channel();
    assert_eq!(
        route_outgoing_envelope(
            &mut c,
            OutgoingEnvelope::ToConnection {
                connection_id: ConnectionId(1),
                message: reply(),
                write_complete_tx: Some(ack)
            }
        )
        .await,
        None
    );
    assert!(matches!(
        done.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    rx.recv()
        .await
        .unwrap()
        .write_complete_tx
        .unwrap()
        .send(())
        .unwrap();
    assert_eq!(done.await, Ok(()));
}
