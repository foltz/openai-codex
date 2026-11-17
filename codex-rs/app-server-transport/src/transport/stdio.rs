//! Stdio transport with EOF cleanup and Unix SIGTERM shutdown. Dedicated,
//! cancellable I/O threads stop at SIGTERM or the host's owner deadline. A
//! dedicated signal thread observes SIGTERM even when logging or the Tokio
//! runtime is blocked.
//!
//! There is no force-exit watchdog. The host's typed outcome and owner
//! deadline replace the old 45s `process::exit(1)`. Unproven work is reported
//! (`ShutdownIncomplete`) rather than cut short by exiting the process.
//!
//! Stdin EOF is an inbound half-close only. The parent may still read stdout,
//! so every accepted reply is still written. The output recipient counts as
//! gone only on a positive signal: EPIPE, or POLLHUP/POLLERR followed by a
//! failed write.

use super::CHANNEL_CAPACITY;
use super::ConnectionOrigin;
use super::ConnectionProvenance;
use super::RefusalCustody;
use super::RefusalRecipient;
use super::TransportEvent;
use super::forward_incoming_message;
use super::next_connection_id;
use super::serialize_outgoing_message;
#[cfg(unix)]
use super::stdio_threads::HUP_MONITOR_TASK;
use super::stdio_threads::READER_TASK;
use super::stdio_threads::StdioCancel;
use super::stdio_threads::WRITER_TASK;
#[cfg(unix)]
use super::stdio_threads::spawn_hup_monitor;
use super::stdio_threads::spawn_stdin_reader;
use super::stdio_threads::spawn_stdout_writer;
use crate::outgoing_message::QueuedOutgoingMessage;
use codex_app_server_protocol::InitializeParams;
use codex_app_server_protocol::JSONRPCMessage;
use codex_app_server_protocol::JSONRPCRequest;
use std::io::ErrorKind;
use std::io::Result as IoResult;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::error;
use tracing::info;

pub async fn start_stdio_connection(
    transport_event_tx: mpsc::Sender<TransportEvent>,
    initialize_client_name_tx: oneshot::Sender<String>,
    install_shutdown_signal_handler: bool,
    owner_expiry: crate::OwnerExpiry,
) -> IoResult<JoinHandle<()>> {
    let shutdown_signal = CancellationToken::new();
    #[cfg(unix)]
    if install_shutdown_signal_handler {
        use signal_hook::consts::SIGTERM;
        use signal_hook::iterator::Signals;

        // Register before accepting requests. Receiving SIGTERM must not
        // depend on Tokio or synchronous transport logging.
        let mut signals = Signals::new([SIGTERM])?;
        let shutdown_signal = shutdown_signal.clone();
        std::thread::Builder::new()
            .name("app-server-signal".to_string())
            .spawn(move || {
                if signals.forever().next().is_some() {
                    shutdown_signal.cancel();
                }
            })?;
    }
    #[cfg(not(unix))]
    let _ = install_shutdown_signal_handler;
    let connection_id = next_connection_id();
    let (writer_tx, writer_rx) = mpsc::channel::<QueuedOutgoingMessage>(CHANNEL_CAPACITY);
    let writer_tx_for_reader = writer_tx.clone();
    transport_event_tx
        .send(TransportEvent::ConnectionOpened {
            connection_id,
            origin: ConnectionOrigin::Stdio,
            provenance: crate::transport::ConnectionProvenance::Unproven,
            auth: None,
            provenance: ConnectionProvenance::Unproven,
            writer: writer_tx,
            disconnect_sender: None,
        })
        .await
        .map_err(|_| std::io::Error::new(ErrorKind::BrokenPipe, "processor unavailable"))?;

    // Tokio's stdin uses an uncancellable blocking task, which keeps its runtime
    // alive while the client leaves stdin open. Dedicated threads stay off the
    // blocking pool, and on Unix they are cancellable (poll(2) + self-pipe):
    // SIGTERM or the host's owner deadline stops them even when the client
    // keeps stdin open or stops reading stdout. Their real joins go into the
    // owner report; one still running there is reported incomplete.
    let stdio_cancel = StdioCancel::new()?;
    let runtime = tokio::runtime::Handle::current();
    let (stdin_tx, stdin_rx) = mpsc::channel(/*buffer*/ 1);
    let stdin_reader = spawn_stdin_reader(
        stdin_tx,
        stdio_cancel.clone(),
        runtime.clone(),
        owner_expiry.clone(),
    )?;
    owner_expiry.register_blocking(READER_TASK, stdin_reader);

    // Stdin hangup is observed independently of the reader (which can be
    // blocked on a full queue behind a stuck processor), without reading:
    // the first shutdown edge latches before any queue. Stopped at reader
    // exit, SIGTERM, owner expiry or owner release; its join is a receipt.
    let hup_stop = StdioCancel::new()?;
    #[cfg(unix)]
    {
        let monitor = spawn_hup_monitor(
            libc::STDIN_FILENO,
            hup_stop.clone(),
            runtime.clone(),
            owner_expiry.clone(),
        )?;
        owner_expiry.register_blocking(HUP_MONITOR_TASK, monitor);
    }

    // The forwarding future owns writer_rx so cancelling it also releases
    // producers stuck on a full queue.
    let (stdout_tx, stdout_rx) = mpsc::channel::<(String, oneshot::Sender<()>)>(/*buffer*/ 1);
    let stdout_writer = spawn_stdout_writer(
        stdout_rx,
        stdio_cancel.clone(),
        runtime,
        owner_expiry.clone(),
    )?;
    owner_expiry.register_blocking(WRITER_TASK, stdout_writer);

    // Stop both threads cooperatively at SIGTERM or at the owner deadline.
    tokio::spawn(watch_stdio_stop(
        shutdown_signal.clone(),
        owner_expiry.clone(),
        stdio_cancel,
        hup_stop.clone(),
    ));

    let read_messages = read_stdin_messages(
        stdin_rx,
        transport_event_tx.clone(),
        writer_tx_for_reader,
        connection_id,
        initialize_client_name_tx,
        owner_expiry.clone(),
        hup_stop,
    );

    let abandoned_sink = owner_expiry.clone();
    let connection = async move {
        let mut writer_rx = writer_rx;
        // Bytes of a message taken from the queue but not yet handed to the
        // writer thread. Once handed over, the thread counts it if dropped.
        let mut in_hand: Option<usize> = None;
        let sigterm = {
            let write_messages = async {
                while let Some(queued_message) = writer_rx.recv().await {
                    let Some(mut json) = serialize_outgoing_message(queued_message.message) else {
                        continue;
                    };
                    json.push('\n');
                    in_hand = Some(json.len());
                    let (written_tx, written_rx) = oneshot::channel();
                    if stdout_tx.send((json, written_tx)).await.is_err() {
                        break;
                    }
                    in_hand = None;
                    if written_rx.await.is_err() {
                        break;
                    }
                    if let Some(write_complete_tx) = queued_message.write_complete_tx {
                        let _ = write_complete_tx.send(());
                    }
                }
                info!("stdout writer exited (channel closed)");
            };
            tokio::select! {
                _ = shutdown_signal.cancelled() => true,
                _ = async { tokio::join!(read_messages, write_messages); } => false,
            }
        };
        if sigterm {
            // Cancelling both forwarding futures drops their queues before
            // connection teardown, including when EOF already began draining.
            // The dropped output is counted (delivery is not completion).
            info!("SIGTERM received; closing stdio connection (owner deadline applies)");
            let mut messages = u64::from(in_hand.is_some());
            let mut bytes = in_hand.map_or(0, |len| u64::try_from(len).unwrap_or(u64::MAX));
            while let Ok(queued_message) = writer_rx.try_recv() {
                if let Some(json) = serialize_outgoing_message(queued_message.message) {
                    messages = messages.saturating_add(1);
                    bytes = bytes.saturating_add(u64::try_from(json.len() + 1).unwrap_or(u64::MAX));
                }
            }
            abandoned_sink.record_dropped_output(messages, bytes);
            let _ = transport_event_tx
                .send(TransportEvent::ConnectionClosed { connection_id })
                .await;
        }
    };
    // (b) Async forwarding: stops at the host's owner expiry, before its next
    // unit of work, and hands its queues back to the host owner.
    Ok(tokio::spawn(async move {
        if let Err(refused) = owner_expiry.run("stdio_connection", connection).await {
            error!("stdio connection not driven: {refused}");
        }
    }))
}

/// Stops the stdio threads at SIGTERM or at the owner deadline. SIGTERM is
/// the first shutdown edge: it is recorded here, at intake, before the
/// connection queues anything toward a possibly stuck processor.
async fn watch_stdio_stop(
    shutdown_signal: CancellationToken,
    owner: crate::OwnerExpiry,
    stdio_cancel: StdioCancel,
    hup_stop: StdioCancel,
) {
    let expired = owner.expired_token();
    tokio::select! {
        () = shutdown_signal.cancelled() => owner.mark_shutdown_edge(),
        () = expired.cancelled() => {}
        () = owner.owner_released_wait() => {}
    }
    stdio_cancel.cancel();
    hup_stop.cancel();
}

/// Forwards stdin lines to the processor. Stdin EOF is the first shutdown
/// edge (stdio is single-client): it is recorded at intake, before
/// `ConnectionClosed` is queued, so a processor that is stuck (and not
/// draining its queue) cannot delay the owner deadline.
async fn read_stdin_messages(
    mut stdin_rx: mpsc::Receiver<super::stdio_threads::StdinLine>,
    transport_event_tx: mpsc::Sender<TransportEvent>,
    writer_tx: mpsc::Sender<QueuedOutgoingMessage>,
    connection_id: crate::outgoing_message::ConnectionId,
    initialize_client_name_tx: oneshot::Sender<String>,
    owner: crate::OwnerExpiry,
    hup_stop: StdioCancel,
) {
    let mut initialize_client_name_tx = Some(initialize_client_name_tx);
    while let Some(line) = stdin_rx.recv().await {
        let line = match line {
            Ok(line) => line,
            Err(err) => {
                error!("Failed reading stdin: {err}");
                break;
            }
        };
        if let Some(client_name) = stdio_initialize_client_name(&line)
            && let Some(initialize_client_name_tx) = initialize_client_name_tx.take()
        {
            let _ = initialize_client_name_tx.send(client_name);
        }
        if !forward_incoming_message(
            &transport_event_tx,
            &writer_tx,
            connection_id,
            &line,
            RefusalCustody {
                owner: &owner,
                recipient: RefusalRecipient::Stdio,
            },
        )
        .await
        {
            break;
        }
    }

    // First shutdown edge, at intake. The reader is done: so is the hangup
    // monitor (it would only re-record the same edge).
    owner.mark_shutdown_edge();
    hup_stop.cancel();
    // EOF closes only the inbound half. The write side keeps draining
    // until every writer handle is gone, so accepted replies still go out.
    // (Bounded: this future runs inside the owner-run stdio connection.)
    let _ = transport_event_tx
        .send(TransportEvent::ConnectionClosed { connection_id })
        .await;
    debug!("stdin reader finished (EOF)");
}

fn stdio_initialize_client_name(line: &str) -> Option<String> {
    let message = serde_json::from_str::<JSONRPCMessage>(line).ok()?;
    let JSONRPCMessage::Request(JSONRPCRequest { method, params, .. }) = message else {
        return None;
    };
    if method != "initialize" {
        return None;
    }
    let params = serde_json::from_value::<InitializeParams>(params?).ok()?;
    Some(params.client_info.name)
}

#[cfg(test)]
#[path = "stdio_intake_tests.rs"]
mod intake_tests;
