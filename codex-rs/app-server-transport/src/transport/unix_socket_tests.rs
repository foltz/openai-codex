use super::AppServerTransport;
use super::CHANNEL_CAPACITY;
use super::ConnectionProvenance;
use super::TransportEvent;
use super::acquire_app_server_startup_lock;
use super::app_server_control_socket_path;
use super::start_control_socket_acceptor;
use codex_app_server_protocol::JSONRPCMessage;
use codex_app_server_protocol::JSONRPCNotification;
use codex_core::config::find_codex_home;
use codex_uds::UnixStream;
use codex_utils_absolute_path::AbsolutePathBuf;
use futures::SinkExt;
use futures::StreamExt;
use pretty_assertions::assert_eq;
use std::io::Result as IoResult;
use std::path::Path;
#[cfg(unix)]
use std::process::Stdio;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio::time::timeout;
use tokio_tungstenite::client_async;
use tokio_tungstenite::tungstenite::Bytes;
use tokio_tungstenite::tungstenite::Message as WebSocketMessage;
use tokio_util::sync::CancellationToken;

#[cfg(unix)]
const UNIX_SOCKET_PEER_HELPER_SOCKET_ENV: &str = "CODEX_UNIX_SOCKET_PEER_HELPER_SOCKET";
#[cfg(unix)]
const UNIX_SOCKET_PEER_HELPER_TEST: &str =
    "transport::unix_socket_tests::unix_socket_peer_helper_connects";

#[test]
fn listen_unix_socket_parses_as_unix_socket_transport() {
    assert_eq!(
        AppServerTransport::from_listen_url("unix://"),
        Ok(AppServerTransport::UnixSocket {
            socket_path: default_control_socket_path()
        })
    );
}

#[test]
fn listen_unix_socket_accepts_absolute_custom_path() {
    assert_eq!(
        AppServerTransport::from_listen_url("unix:///tmp/codex.sock"),
        Ok(AppServerTransport::UnixSocket {
            socket_path: absolute_path("/tmp/codex.sock")
        })
    );
}

#[test]
fn listen_unix_socket_accepts_relative_custom_path() {
    assert_eq!(
        AppServerTransport::from_listen_url("unix://codex.sock"),
        Ok(AppServerTransport::UnixSocket {
            socket_path: AbsolutePathBuf::relative_to_current_dir("codex.sock")
                .expect("relative path should resolve")
        })
    );
}

#[tokio::test]
async fn control_socket_acceptor_upgrades_and_forwards_websocket_text_messages_and_pings() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let socket_path = test_socket_path(temp_dir.path());
    let (transport_event_tx, mut transport_event_rx) =
        mpsc::channel::<TransportEvent>(CHANNEL_CAPACITY);
    let shutdown_token = CancellationToken::new();
    let accept_handle = start_control_socket_acceptor(
        socket_path.clone(),
        transport_event_tx,
        shutdown_token.clone(),
    )
    .await
    .expect("control socket acceptor should start");

    let stream = connect_to_socket(socket_path.as_path())
        .await
        .expect("client should connect");
    let (mut websocket, response) = client_async("ws://localhost/rpc", stream)
        .await
        .expect("websocket upgrade should complete");
    assert_eq!(response.status().as_u16(), 101);

    let opened = timeout(Duration::from_secs(1), transport_event_rx.recv())
        .await
        .expect("connection opened event should arrive")
        .expect("connection opened event");
    let connection_id = match opened {
        TransportEvent::ConnectionOpened {
            connection_id,
            provenance,
            ..
        } => {
            assert_eq!(provenance, expected_same_image_provenance());
            connection_id
        }
        _ => panic!("expected connection opened event"),
    };

    let notification = JSONRPCMessage::Notification(JSONRPCNotification {
        method: "initialized".to_string(),
        params: None,
    });
    websocket
        .send(WebSocketMessage::Text(
            serde_json::to_string(&notification)
                .expect("notification should serialize")
                .into(),
        ))
        .await
        .expect("notification should send");

    let incoming = timeout(Duration::from_secs(1), transport_event_rx.recv())
        .await
        .expect("incoming message event should arrive")
        .expect("incoming message event");
    assert_eq!(
        match incoming {
            TransportEvent::IncomingMessage {
                connection_id: incoming_connection_id,
                message,
            } => (incoming_connection_id, message),
            _ => panic!("expected incoming message event"),
        },
        (connection_id, notification)
    );

    websocket
        .send(WebSocketMessage::Ping(Bytes::from_static(b"check")))
        .await
        .expect("ping should send");
    let pong = timeout(Duration::from_secs(1), websocket.next())
        .await
        .expect("pong should arrive")
        .expect("pong frame")
        .expect("pong should be valid");
    assert_eq!(pong, WebSocketMessage::Pong(Bytes::from_static(b"check")));

    websocket.close(None).await.expect("close should send");
    let closed = timeout(Duration::from_secs(1), transport_event_rx.recv())
        .await
        .expect("connection closed event should arrive")
        .expect("connection closed event");
    assert!(matches!(
        closed,
        TransportEvent::ConnectionClosed {
            connection_id: closed_connection_id,
        } if closed_connection_id == connection_id
    ));

    shutdown_token.cancel();
    accept_handle.await.expect("acceptor should join");
    assert_socket_path_removed(socket_path.as_path());
}

/// This is invoked in a separately spawned copy of this test executable. The
/// parent test chooses whether that executable has the same file identity as
/// the listener or a deliberately copied (different) identity.
#[cfg(unix)]
#[test]
fn unix_socket_peer_helper_connects() {
    let Ok(socket_path) = std::env::var(UNIX_SOCKET_PEER_HELPER_SOCKET_ENV) else {
        return;
    };
    let runtime = tokio::runtime::Runtime::new().expect("helper runtime should start");
    runtime.block_on(async move {
        let stream = connect_to_socket(Path::new(&socket_path))
            .await
            .expect("helper should connect to the control socket");
        let (mut websocket, response) = client_async("ws://localhost/rpc", stream)
            .await
            .expect("helper websocket upgrade should complete");
        assert_eq!(response.status().as_u16(), 101);
        websocket
            .send(WebSocketMessage::Text(
                serde_json::to_string(&JSONRPCMessage::Notification(JSONRPCNotification {
                    method: "initialized".to_string(),
                    params: None,
                }))
                .expect("initialized notification should serialize")
                .into(),
            ))
            .await
            .expect("helper should send initialized notification");
        // Keep the peer alive until accept-time credentials are read. This
        // models the persistent daemon client that entitlement is for; an
        // immediately-exiting helper can disappear before the accept task is
        // scheduled and correctly yields ESRCH.
        tokio::time::sleep(Duration::from_millis(100)).await;
        websocket
            .close(None)
            .await
            .expect("helper should close websocket");
    });
}

#[cfg(unix)]
#[tokio::test]
async fn control_socket_entitles_only_a_same_image_peer() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let socket_path = test_socket_path(temp_dir.path());
    let (transport_event_tx, mut transport_event_rx) =
        mpsc::channel::<TransportEvent>(CHANNEL_CAPACITY);
    let shutdown_token = CancellationToken::new();
    let accept_handle = start_control_socket_acceptor(
        socket_path.clone(),
        transport_event_tx,
        shutdown_token.clone(),
    )
    .await
    .expect("control socket acceptor should start");

    run_peer_helper(
        std::env::current_exe().expect("test executable should resolve"),
        socket_path.as_path(),
    )
    .await;
    assert_eq!(
        recv_connection_provenance(&mut transport_event_rx).await,
        expected_same_image_provenance()
    );

    let copied_executable = temp_dir.path().join("unentitled-peer");
    tokio::fs::copy(
        std::env::current_exe().expect("test executable should resolve"),
        &copied_executable,
    )
    .await
    .expect("test executable should copy");
    use std::os::unix::fs::PermissionsExt;
    tokio::fs::set_permissions(&copied_executable, std::fs::Permissions::from_mode(0o700))
        .await
        .expect("copied helper should be executable");
    run_peer_helper(copied_executable, socket_path.as_path()).await;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    assert_eq!(
        recv_connection_provenance(&mut transport_event_rx).await,
        ConnectionProvenance::Unproven
    );
    #[cfg(target_os = "macos")]
    assert_eq!(
        recv_connection_provenance(&mut transport_event_rx).await,
        expected_same_image_provenance()
    );

    shutdown_token.cancel();
    accept_handle.await.expect("acceptor should join");
}

#[cfg(unix)]
async fn run_peer_helper(executable: std::path::PathBuf, socket_path: &Path) {
    let status = tokio::process::Command::new(executable)
        .arg("--exact")
        .arg(UNIX_SOCKET_PEER_HELPER_TEST)
        .arg("--nocapture")
        .env(UNIX_SOCKET_PEER_HELPER_SOCKET_ENV, socket_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .expect("peer helper should run");
    assert!(status.success(), "peer helper should succeed: {status}");
}

#[cfg(unix)]
fn expected_same_image_provenance() -> ConnectionProvenance {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        ConnectionProvenance::UnixPeerExecutable(
            super::PeerExecutableIdentity::current_process().expect("current executable identity"),
        )
    }
    #[cfg(target_os = "macos")]
    {
        ConnectionProvenance::UnixPeerExecutable(
            super::PeerExecutableIdentity::current_process().expect("current executable identity"),
        )
    }
    #[cfg(not(any(target_os = "linux", target_os = "android", target_os = "macos")))]
    {
        ConnectionProvenance::Unproven
    }
}

#[cfg(unix)]
async fn recv_connection_provenance(
    transport_event_rx: &mut mpsc::Receiver<TransportEvent>,
) -> ConnectionProvenance {
    loop {
        let event = timeout(Duration::from_secs(1), transport_event_rx.recv())
            .await
            .expect("connection event should arrive")
            .expect("transport event channel should remain open");
        if let TransportEvent::ConnectionOpened { provenance, .. } = event {
            return provenance;
        }
    }
}

#[tokio::test]
async fn app_server_startup_lock_serializes_waiters() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let lock_path = test_startup_lock_path(temp_dir.path());
    let first_lock = acquire_app_server_startup_lock(lock_path.clone())
        .await
        .expect("first startup lock should succeed");
    let mut second_lock = tokio::spawn(acquire_app_server_startup_lock(lock_path));

    assert!(
        timeout(Duration::from_millis(100), &mut second_lock)
            .await
            .is_err()
    );

    drop(first_lock);
    second_lock
        .await
        .expect("second startup lock task should join")
        .expect("second startup lock should succeed");
}

#[cfg(unix)]
#[tokio::test]
async fn control_socket_file_is_private_after_bind() {
    use std::os::unix::fs::PermissionsExt;

    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let socket_path = test_socket_path(temp_dir.path());
    let (transport_event_tx, _transport_event_rx) =
        mpsc::channel::<TransportEvent>(CHANNEL_CAPACITY);
    let shutdown_token = CancellationToken::new();
    let accept_handle = start_control_socket_acceptor(
        socket_path.clone(),
        transport_event_tx,
        shutdown_token.clone(),
    )
    .await
    .expect("control socket acceptor should start");

    let metadata = tokio::fs::metadata(socket_path.as_path())
        .await
        .expect("socket metadata should exist");
    assert_eq!(metadata.permissions().mode() & 0o777, 0o600);

    shutdown_token.cancel();
    accept_handle.await.expect("acceptor should join");
}

fn absolute_path(path: &str) -> AbsolutePathBuf {
    AbsolutePathBuf::from_absolute_path(path).expect("absolute path")
}

fn default_control_socket_path() -> AbsolutePathBuf {
    let codex_home = find_codex_home().expect("codex home");
    app_server_control_socket_path(&codex_home).expect("default control socket path")
}

fn test_socket_path(temp_dir: &Path) -> AbsolutePathBuf {
    AbsolutePathBuf::from_absolute_path(
        temp_dir
            .join("app-server-control")
            .join("app-server-control.sock"),
    )
    .expect("socket path should resolve")
}

fn test_startup_lock_path(temp_dir: &Path) -> AbsolutePathBuf {
    AbsolutePathBuf::from_absolute_path(
        temp_dir
            .join("app-server-control")
            .join("app-server-startup.lock"),
    )
    .expect("startup lock path should resolve")
}

async fn connect_to_socket(socket_path: &Path) -> IoResult<UnixStream> {
    UnixStream::connect(socket_path).await
}

#[cfg(unix)]
fn assert_socket_path_removed(socket_path: &Path) {
    assert!(!socket_path.exists());
}

#[cfg(windows)]
fn assert_socket_path_removed(_socket_path: &Path) {
    // uds_windows uses a regular filesystem path as its rendezvous point,
    // but there is no Unix socket filesystem node to assert on.
}
