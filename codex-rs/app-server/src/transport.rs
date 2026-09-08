use crate::message_processor::ConnectionSessionState;
use crate::outgoing_message::OutgoingEnvelope;
use crate::thread_state::RetentionPrincipalId;
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
pub(crate) use codex_app_server_transport::DaemonShutdownAccess;
pub(crate) use codex_app_server_transport::ConnectionProvenance;
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
pub use codex_app_server_transport::auth;
pub(crate) use codex_app_server_transport::start_control_socket_acceptor;
pub(crate) use codex_app_server_transport::start_remote_control;
pub(crate) use codex_app_server_transport::start_stdio_connection;
pub(crate) use codex_app_server_transport::start_websocket_acceptor;
pub use codex_app_server_transport::take_remote_control_disabled_env;

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
        // The transport boundary explicitly classifies the principal's owner;
        // upstream connection authentication remains on its separate RPC gate.
        let mut session = ConnectionSessionState::with_provenance(origin, provenance, RetentionPrincipalId::connection_owned());
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

/// Evaluates the server-owned entitlement half of D001. The caller must still
/// require the explicit client role request before granting interactive subscription state.
pub(crate) fn trusted_interactive_provenance(provenance: ConnectionProvenance) -> bool {
    match provenance {
        ConnectionProvenance::InProcess => true,
        // `unix_peer_provenance` verifies the identity while accepting the
        // socket, before any client input is read. This is deliberately a
        // server-established result, not a client-side claim.
        ConnectionProvenance::UnixPeerExecutable(_) => true,
        ConnectionProvenance::Unproven => false,
    }
}

pub(crate) fn trusted_interactive(
    interactive_client_requested: bool,
    provenance: ConnectionProvenance,
) -> bool {
    interactive_client_requested && trusted_interactive_provenance(provenance)
}

/// Evaluates the server-owned entitlement half of Issue 05's managed-transition
/// caller authorization (`CODEX-I05-S02-R005`). Deliberately narrower than
/// [`trusted_interactive_provenance`]: an in-process embedder shares the
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

#[cfg(test)]
mod provenance_tests {
    use super::ConnectionOrigin;
    use super::ConnectionProvenance;
    use super::ConnectionState;
    use super::trusted_interactive;
    use super::trusted_interactive_provenance;
    use crate::message_processor::InitializedConnectionSessionState;
    use crate::thread_state::RetentionPrincipalOwner;
    use codex_app_server_transport::PeerExecutableIdentity;
    use codex_protocol::mcp::ClientMcpExtensions;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::RwLock;
    use std::sync::atomic::AtomicBool;

    #[test]
    fn unproven_connections_cannot_become_interactive() {
        assert!(!trusted_interactive_provenance(
            ConnectionProvenance::Unproven
        ));
    }

    #[test]
    fn interactive_role_is_necessary_even_for_the_embedded_connection() {
        assert!(!trusted_interactive(
            /*interactive_client_requested*/ false,
            ConnectionProvenance::InProcess,
        ));
        assert!(trusted_interactive(
            /*interactive_client_requested*/ true,
            ConnectionProvenance::InProcess,
        ));
    }

    #[test]
    fn interactive_role_cannot_promote_an_unproven_connection() {
        assert!(!trusted_interactive(
            /*interactive_client_requested*/ true,
            ConnectionProvenance::Unproven,
        ));
    }

    #[test]
    fn normal_transport_construction_marks_a_verified_peer_connection_owned() {
        let connection = ConnectionState::new(
            ConnectionOrigin::WebSocket,
            /*auth*/ None,
            ConnectionProvenance::UnixPeerExecutable(
                PeerExecutableIdentity::FileIdentity {
                    device: 1,
                    inode: 2,
                },
            ),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
            Arc::new(RwLock::new(HashSet::new())),
        );

        connection
            .session
            .initialize(InitializedConnectionSessionState {
                experimental_api_enabled: true,
                opted_out_notification_methods: HashSet::new(),
                app_server_client_name: "test".to_string(),
                client_version: "0.0.0".to_string(),
                request_attestation: true,
                interactive_client_requested: true,
                client_mcp_extensions: ClientMcpExtensions::default(),
            })
            .expect("test session initializes once");
        assert_eq!(
            connection
                .session
                .retention_principal()
                .map(super::super::thread_state::RetentionPrincipalId::owner),
            Some(RetentionPrincipalOwner::ConnectionOwned),
            "the normal transport seam must classify its server-minted principal explicitly"
        );
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

async fn send_message_to_connection(
    connections: &mut HashMap<ConnectionId, OutboundConnectionState>,
    connection_id: ConnectionId,
    message: OutgoingMessage,
    write_complete_tx: Option<tokio::sync::oneshot::Sender<()>>,
) -> bool {
    let Some(connection_state) = connections.get(&connection_id) else {
        warn!("dropping message for disconnected connection: {connection_id:?}");
        return false;
    };
    let message = filter_outgoing_message_for_connection(connection_state, message);
    if should_skip_notification_for_connection(connection_state, &message) {
        return false;
    }

    let writer = connection_state.writer.clone();
    let queued_message = QueuedOutgoingMessage {
        message,
        write_complete_tx,
    };
    if connection_state.can_disconnect() {
        match writer.try_send(queued_message) {
            Ok(()) => false,
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!(
                    "disconnecting slow connection after outbound queue filled: {connection_id:?}"
                );
                disconnect_connection(connections, connection_id)
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                disconnect_connection(connections, connection_id)
            }
        }
    } else if writer.send(queued_message).await.is_err() {
        disconnect_connection(connections, connection_id)
    } else {
        false
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
) {
    match envelope {
        OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            write_complete_tx,
        } => {
            let _ =
                send_message_to_connection(connections, connection_id, message, write_complete_tx)
                    .await;
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

            for connection_id in target_connections {
                let _ = send_message_to_connection(
                    connections,
                    connection_id,
                    message.clone(),
                    /*write_complete_tx*/ None,
                )
                .await;
            }
        }
    }
}

#[cfg(test)]
#[path = "transport_tests.rs"]
mod tests;
