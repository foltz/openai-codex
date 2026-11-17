use super::*;
use crate::outgoing_message::ConnectionId;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Poll;
use std::time::Duration;
use tokio::time::Instant;

/// A host-like owner whose shutdown-edge hook records when it fired.
fn recording_owner() -> (
    crate::OwnerCustody,
    crate::OwnerExpiry,
    Arc<Mutex<Vec<Instant>>>,
) {
    let custody = crate::OwnerCustody::new(CancellationToken::new());
    let owner = custody.expiry();
    let edges = Arc::new(Mutex::new(Vec::new()));
    assert!(owner.set_shutdown_edge({
        let edges = Arc::clone(&edges);
        move || edges.lock().expect("edges").push(Instant::now())
    }));
    (custody, owner, edges)
}

#[tokio::test(start_paused = true)]
async fn stdin_eof_records_the_shutdown_edge_at_intake_while_the_processor_is_stuck() {
    let (_custody, owner, edges) = recording_owner();
    // A stuck processor: its queue is full and never drained.
    let (transport_event_tx, _stuck_processor_rx) = mpsc::channel(1);
    transport_event_tx
        .try_send(TransportEvent::ConnectionClosed {
            connection_id: ConnectionId(99),
        })
        .expect("fill the processor queue");
    let (writer_tx, _writer_rx) = mpsc::channel(1);
    let (stdin_tx, stdin_rx) = mpsc::channel(1);
    let (client_name_tx, _client_name_rx) = oneshot::channel();
    let hup_stop = StdioCancel::new().expect("hangup stop");
    let mut read = std::pin::pin!(read_stdin_messages(
        stdin_rx,
        transport_event_tx,
        writer_tx,
        ConnectionId(7),
        client_name_tx,
        owner.clone(),
        hup_stop,
    ));
    tokio::time::advance(Duration::from_secs(3)).await;

    let intake = Instant::now();
    drop(stdin_tx);
    let polled = std::future::poll_fn(|cx| Poll::Ready(read.as_mut().poll(cx))).await;

    assert!(
        polled.is_pending(),
        "ConnectionClosed is still blocked behind the stuck processor"
    );
    assert_eq!(
        *edges.lock().expect("edges"),
        vec![intake],
        "the shutdown edge is recorded at EOF intake"
    );
}

#[tokio::test(start_paused = true)]
async fn sigterm_records_the_shutdown_edge_at_intake() {
    let (_custody, owner, edges) = recording_owner();
    let shutdown_signal = CancellationToken::new();
    let stdio_cancel = StdioCancel::new().expect("stdio cancel");
    let mut watch = std::pin::pin!(watch_stdio_stop(
        shutdown_signal.clone(),
        owner.clone(),
        stdio_cancel,
        StdioCancel::new().expect("hangup stop"),
    ));
    let first = std::future::poll_fn(|cx| Poll::Ready(watch.as_mut().poll(cx))).await;
    assert!(first.is_pending());
    tokio::time::advance(Duration::from_secs(4)).await;

    let intake = Instant::now();
    shutdown_signal.cancel();
    watch.await;

    assert_eq!(*edges.lock().expect("edges"), vec![intake]);
}
