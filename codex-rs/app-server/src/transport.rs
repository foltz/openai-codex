use crate::message_processor::ConnectionSessionState;
use crate::outgoing_message::OutgoingEnvelope;
use codex_app_server_protocol::ExperimentalApi;
use codex_app_server_protocol::ServerRequest;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::warn;

pub use codex_app_server_transport::AppServerTransport;
pub(crate) use codex_app_server_transport::CHANNEL_CAPACITY;
pub(crate) use codex_app_server_transport::ConnectionId;
pub(crate) use codex_app_server_transport::ConnectionOrigin;
pub(crate) use codex_app_server_transport::ConnectionProvenance;
pub(crate) use codex_app_server_transport::DaemonShutdownAccess;
pub(crate) use codex_app_server_transport::OutgoingMessage;
pub(crate) use codex_app_server_transport::QueuedOutgoingMessage;
pub(crate) use codex_app_server_transport::RemoteControlEnableError;
pub(crate) use codex_app_server_transport::RemoteControlHandle;
pub(crate) use codex_app_server_transport::RemoteControlPolicy;
pub(crate) use codex_app_server_transport::RemoteControlStartConfig;
pub use codex_app_server_transport::RemoteControlStartupMode;
pub(crate) use codex_app_server_transport::RemoteControlUnavailable;
pub(crate) use codex_app_server_transport::TransportEvent;
pub(crate) use codex_app_server_transport::acquire_app_server_startup_lock;
pub use codex_app_server_transport::app_server_control_socket_path;
pub(crate) use codex_app_server_transport::app_server_startup_lock_path;
pub(crate) use codex_app_server_transport::start_control_socket_acceptor;
pub(crate) use codex_app_server_transport::start_remote_control;
pub(crate) use codex_app_server_transport::start_stdio_connection;
pub(crate) use codex_app_server_transport::start_websocket_acceptor;
pub use codex_app_server_transport::take_remote_control_disabled_env;
pub use codex_app_server_transport::verified_local_daemon_socket;

pub(crate) struct ConnectionState {
    pub(crate) origin: ConnectionOrigin,
    pub(crate) outbound_initialized: Arc<AtomicBool>,
    pub(crate) outbound_experimental_api_enabled: Arc<AtomicBool>,
    pub(crate) outbound_opted_out_notification_methods: Arc<RwLock<HashSet<String>>>,
    pub(crate) session: Arc<ConnectionSessionState>,
}

impl ConnectionState {
    pub(crate) fn new(
        origin: ConnectionOrigin,
        auth: Option<codex_app_server_transport::ConnectionAuth>,
        provenance: ConnectionProvenance,
        outbound_initialized: Arc<AtomicBool>,
        outbound_experimental_api_enabled: Arc<AtomicBool>,
        outbound_opted_out_notification_methods: Arc<RwLock<HashSet<String>>>,
    ) -> Self {
        let mut session = ConnectionSessionState::with_provenance(origin, provenance);
        let mut rpc_gate = crate::connection_rpc_gate::ConnectionRpcGate::new();
        rpc_gate.auth = auth;
        session.rpc_gate = Arc::new(rpc_gate);
        Self {
            origin,
            outbound_initialized,
            outbound_experimental_api_enabled,
            outbound_opted_out_notification_methods,
            session: Arc::new(session),
        }
    }
}

/// Evaluates the server-owned entitlement half of Issue 05's managed-transition
/// caller authorization (`CODEX-I05-S02-R005`). Deliberately narrower than
/// ordinary interactive trust: an in-process embedder shares the
/// server's own address space and has no independent, externally-verifiable
/// process target to bind against, so it is not accepted here even though it
/// is otherwise a trusted interactive caller for thread attachment. Only a
/// server-established same-executable Unix peer proof qualifies.
pub(crate) fn managed_transition_caller_provenance_authorized(
    provenance: ConnectionProvenance,
) -> bool {
    matches!(provenance, ConnectionProvenance::UnixPeerExecutable(_))
}

pub(crate) fn managed_transition_caller_authorized(
    interactive_client_requested: bool,
    provenance: ConnectionProvenance,
) -> bool {
    interactive_client_requested && managed_transition_caller_provenance_authorized(provenance)
}

#[cfg(test)]
mod managed_transition_caller_tests {
    use super::ConnectionProvenance;
    use super::managed_transition_caller_authorized;
    use super::managed_transition_caller_provenance_authorized;
    use codex_app_server_transport::PeerExecutableIdentity;

    /// `PeerExecutableIdentity`'s own accept-time constructors are private to
    /// its crate (correctly -- production identity must only ever come from a
    /// live accept-time proof). Its `FileIdentity` variant fields are public,
    /// so a synthetic disposable value is constructible here without any new
    /// cross-crate production API; the predicates under test only branch on
    /// the `ConnectionProvenance` variant, never on the identity's own
    /// content, so a fixed synthetic identity is sufficient evidence.
    fn synthetic_peer_identity() -> PeerExecutableIdentity {
        PeerExecutableIdentity::FileIdentity {
            device: 1,
            inode: 1,
        }
    }

    #[test]
    fn in_process_provenance_is_not_authorized() {
        assert!(!managed_transition_caller_provenance_authorized(
            ConnectionProvenance::InProcess
        ));
    }

    #[test]
    fn unproven_provenance_is_not_authorized() {
        assert!(!managed_transition_caller_provenance_authorized(
            ConnectionProvenance::Unproven
        ));
    }

    #[test]
    fn unix_peer_executable_provenance_is_authorized() {
        assert!(managed_transition_caller_provenance_authorized(
            ConnectionProvenance::UnixPeerExecutable(synthetic_peer_identity())
        ));
    }

    #[test]
    fn missing_role_refuses_even_with_qualifying_provenance() {
        assert!(!managed_transition_caller_authorized(
            /*interactive_client_requested*/ false,
            ConnectionProvenance::UnixPeerExecutable(synthetic_peer_identity())
        ));
    }

    #[test]
    fn explicit_role_and_qualifying_provenance_together_authorize() {
        assert!(managed_transition_caller_authorized(
            /*interactive_client_requested*/ true,
            ConnectionProvenance::UnixPeerExecutable(synthetic_peer_identity())
        ));
    }

    #[test]
    fn explicit_role_cannot_promote_in_process_provenance() {
        assert!(!managed_transition_caller_authorized(
            /*interactive_client_requested*/ true,
            ConnectionProvenance::InProcess
        ));
    }
}

pub(crate) struct OutboundConnectionState {
    pub(crate) initialized: Arc<AtomicBool>,
    pub(crate) experimental_api_enabled: Arc<AtomicBool>,
    pub(crate) opted_out_notification_methods: Arc<RwLock<HashSet<String>>>,
    pub(crate) writer: mpsc::Sender<QueuedOutgoingMessage>,
    disconnect_sender: Option<CancellationToken>,
}

impl OutboundConnectionState {
    pub(crate) fn new(
        writer: mpsc::Sender<QueuedOutgoingMessage>,
        initialized: Arc<AtomicBool>,
        experimental_api_enabled: Arc<AtomicBool>,
        opted_out_notification_methods: Arc<RwLock<HashSet<String>>>,
        disconnect_sender: Option<CancellationToken>,
    ) -> Self {
        Self {
            initialized,
            experimental_api_enabled,
            opted_out_notification_methods,
            writer,
            disconnect_sender,
        }
    }

    fn can_disconnect(&self) -> bool {
        self.disconnect_sender.is_some()
    }

    pub(crate) fn request_disconnect(&self) {
        if let Some(disconnect_sender) = &self.disconnect_sender {
            disconnect_sender.cancel();
        }
    }
}

fn should_skip_notification_for_connection(
    connection_state: &OutboundConnectionState,
    message: &OutgoingMessage,
) -> bool {
    let Ok(opted_out_notification_methods) = connection_state.opted_out_notification_methods.read()
    else {
        warn!("failed to read outbound opted-out notifications");
        return false;
    };
    match message {
        OutgoingMessage::AppServerNotification(envelope) => {
            if envelope.notification.experimental_reason().is_some()
                && !connection_state
                    .experimental_api_enabled
                    .load(Ordering::Acquire)
            {
                return true;
            }
            let method = envelope.notification.to_string();
            opted_out_notification_methods.contains(method.as_str())
        }
        _ => false,
    }
}

fn disconnect_connection(
    connections: &mut HashMap<ConnectionId, OutboundConnectionState>,
    connection_id: ConnectionId,
) -> bool {
    if let Some(connection_state) = connections.remove(&connection_id) {
        connection_state.request_disconnect();
        return true;
    }
    false
}

/// A reply (response or error) that did not reach its connection's writer.
/// A disconnect request is not proof of observed peer closure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReplyDrop {
    /// The connection is not, or is no longer, registered with the router.
    Unregistered(ConnectionId),
    /// Its registered writer refused the reply (closed, or full for a
    /// disconnectable connection) and was removed.
    WriterRefused {
        connection_id: ConnectionId,
        disconnectable: bool,
    },
}

enum SendOutcome {
    Queued,
    Skipped,
    Unregistered,
    Refused { disconnectable: bool },
}

async fn send_message_to_connection(
    connections: &mut HashMap<ConnectionId, OutboundConnectionState>,
    connection_id: ConnectionId,
    message: OutgoingMessage,
    write_complete_tx: Option<tokio::sync::oneshot::Sender<()>>,
) -> SendOutcome {
    let Some(connection_state) = connections.get(&connection_id) else {
        warn!("dropping message for disconnected connection: {connection_id:?}");
        return SendOutcome::Unregistered;
    };
    let message = filter_outgoing_message_for_connection(connection_state, message);
    if should_skip_notification_for_connection(connection_state, &message) {
        return SendOutcome::Skipped;
    }

    let writer = connection_state.writer.clone();
    let queued_message = QueuedOutgoingMessage {
        message,
        write_complete_tx,
    };
    if connection_state.can_disconnect() {
        match writer.try_send(queued_message) {
            Ok(()) => SendOutcome::Queued,
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!(
                    "disconnecting slow connection after outbound queue filled: {connection_id:?}"
                );
                disconnect_connection(connections, connection_id);
                SendOutcome::Refused {
                    disconnectable: true,
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                disconnect_connection(connections, connection_id);
                SendOutcome::Refused {
                    disconnectable: true,
                }
            }
        }
    } else if writer.send(queued_message).await.is_err() {
        disconnect_connection(connections, connection_id);
        SendOutcome::Refused {
            disconnectable: false,
        }
    } else {
        SendOutcome::Queued
    }
}

fn filter_outgoing_message_for_connection(
    connection_state: &OutboundConnectionState,
    message: OutgoingMessage,
) -> OutgoingMessage {
    let experimental_api_enabled = connection_state
        .experimental_api_enabled
        .load(Ordering::Acquire);
    match message {
        OutgoingMessage::Request(ServerRequest::CommandExecutionRequestApproval {
            request_id,
            mut params,
        }) => {
            if !experimental_api_enabled {
                params.strip_experimental_fields();
            }
            OutgoingMessage::Request(ServerRequest::CommandExecutionRequestApproval {
                request_id,
                params,
            })
        }
        _ => message,
    }
}

pub(crate) async fn route_outgoing_envelope(
    connections: &mut HashMap<ConnectionId, OutboundConnectionState>,
    envelope: OutgoingEnvelope,
) -> Option<ReplyDrop> {
    match envelope {
        OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            write_complete_tx,
        } => {
            let is_reply = matches!(
                message,
                OutgoingMessage::Response(_) | OutgoingMessage::Error(_)
            );
            let outcome =
                send_message_to_connection(connections, connection_id, message, write_complete_tx)
                    .await;
            match outcome {
                _ if !is_reply => None,
                SendOutcome::Queued | SendOutcome::Skipped => None,
                SendOutcome::Unregistered => Some(ReplyDrop::Unregistered(connection_id)),
                SendOutcome::Refused { disconnectable } => Some(ReplyDrop::WriterRefused {
                    connection_id,
                    disconnectable,
                }),
            }
        }
        OutgoingEnvelope::Broadcast { message } => {
            let target_connections: Vec<ConnectionId> = connections
                .iter()
                .filter_map(|(connection_id, connection_state)| {
                    if connection_state.initialized.load(Ordering::Acquire)
                        && !should_skip_notification_for_connection(connection_state, &message)
                    {
                        Some(*connection_id)
                    } else {
                        None
                    }
                })
                .collect();

            let messages = std::iter::repeat_n(message, target_connections.len());
            for (connection_id, message) in target_connections.into_iter().zip(messages) {
                let _ = send_message_to_connection(
                    connections,
                    connection_id,
                    message,
                    /*write_complete_tx*/ None,
                )
                .await;
            }
            None
        }
    }
}

#[cfg(test)]
#[path = "transport_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "router_reply_outcome_tests.rs"]
mod router_reply_outcome_tests;
