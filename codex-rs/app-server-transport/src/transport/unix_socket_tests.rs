use super::AppServerTransport;
use super::CHANNEL_CAPACITY;
use super::ConnectionProvenance;
use super::DaemonShutdownAccess;
#[cfg(target_os = "macos")]
use super::PeerExecutableIdentity;
use super::TransportEvent;
use super::acquire_app_server_startup_lock;
use super::app_server_control_socket_path;
#[cfg(target_os = "macos")]
use super::identities_match;
use super::start_control_socket_acceptor;
#[cfg(unix)]
use super::start_control_socket_acceptor_with_bound_hook;
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
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
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

#[cfg(unix)]
#[tokio::test]
async fn bound_hook_failure_removes_rendezvous_and_physical_socket_before_retry() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let socket_path = test_socket_path(temp_dir.path());
    let (events, mut received) = mpsc::channel::<TransportEvent>(CHANNEL_CAPACITY);
    let mut physical_path = None;
    let failure = start_control_socket_acceptor_with_bound_hook(
        socket_path.clone(),
        events,
        CancellationToken::new(),
        DaemonShutdownAccess::Disabled,
        || {
            let physical = std::fs::read_link(socket_path.as_path()).expect("rendezvous published");
            assert!(
                physical.exists(),
                "physical socket bound before publication hook"
            );
            physical_path = Some(physical);
            Err(std::io::Error::other(
                "synthetic target publication refusal",
            ))
        },
    )
    .await
    .expect_err("publication failure must prevent acceptor startup");
    assert_eq!(failure.kind(), std::io::ErrorKind::Other);
    assert!(std::fs::symlink_metadata(socket_path.as_path()).is_err());
    assert!(!physical_path.expect("hook was reached").exists());
    assert!(matches!(
        received.try_recv(),
        Err(mpsc::error::TryRecvError::Disconnected)
    ));

    let (events, _received) = mpsc::channel::<TransportEvent>(CHANNEL_CAPACITY);
    let shutdown = CancellationToken::new();
    let mut published = false;
    let acceptor = start_control_socket_acceptor_with_bound_hook(
        socket_path.clone(),
        events,
        shutdown.clone(),
        DaemonShutdownAccess::Disabled,
        || {
            published = true;
            Ok(())
        },
    )
    .await
    .expect("same rendezvous can restart after failed publication");
    assert!(published);
    shutdown.cancel();
    timeout(Duration::from_secs(5), acceptor)
        .await
        .expect("acceptor stops")
        .expect("acceptor joins");
    assert!(std::fs::symlink_metadata(socket_path.as_path()).is_err());
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
        DaemonShutdownAccess::Disabled,
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
    let advertised_max = response
        .headers()
        .get("x-codex-websocket-max-unfragmented-message-bytes")
        .expect("byte cap header should be advertised")
        .to_str()
        .expect("byte cap header should be ASCII")
        .parse::<usize>()
        .expect("byte cap header should be a number");
    let websocket_config = WebSocketConfig::default();
    assert_eq!(
        advertised_max,
        [
            websocket_config.max_frame_size,
            websocket_config.max_message_size,
        ]
        .into_iter()
        .flatten()
        .min()
        .expect("default websocket config should have an incoming size limit")
    );

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
        DaemonShutdownAccess::Disabled,
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

    // A copied executable retains its original CodeDirectory and is therefore
    // the same build. Re-signing that copy with an explicit different
    // identifier creates a runnable same-signer peer with a different exact
    // static-code hash; it must not inherit the daemon's entitlement.
    #[cfg(target_os = "macos")]
    {
        let different_build = temp_dir.path().join("different-build-peer");
        tokio::fs::copy(
            std::env::current_exe().expect("test executable should resolve"),
            &different_build,
        )
        .await
        .expect("test executable should copy for re-signing");
        tokio::fs::set_permissions(&different_build, std::fs::Permissions::from_mode(0o700))
            .await
            .expect("different-build helper should be executable");
        let resign_status = tokio::process::Command::new("/usr/bin/codesign")
            .args([
                "--force",
                "--sign",
                "-",
                "--identifier",
                "com.openai.codex.kcf007.different-build",
            ])
            .arg(&different_build)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .expect("codesign should run");
        assert!(
            resign_status.success(),
            "codesign should succeed: {resign_status}"
        );
        run_peer_helper(different_build, socket_path.as_path()).await;
        assert_eq!(
            recv_connection_provenance(&mut transport_event_rx).await,
            ConnectionProvenance::Unproven
        );
    }

    shutdown_token.cancel();
    accept_handle.await.expect("acceptor should join");
}

/// This fixture intentionally models the old Darwin comparison: two static
/// codes signed with the same explicit designated requirement. The binaries
/// differ, so Security gives them distinct unique static-code values. A
/// designated-requirement comparison would return true; the production exact
/// identity comparison must return false.
#[cfg(target_os = "macos")]
#[tokio::test]
async fn exact_static_code_hash_rejects_same_designated_requirement_different_build() {
    use std::os::unix::fs::PermissionsExt;

    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let daemon_build = temp_dir.path().join("daemon-build");
    let peer_build = temp_dir.path().join("peer-build");
    tokio::fs::copy("/usr/bin/true", &daemon_build)
        .await
        .expect("true fixture should copy");
    tokio::fs::copy("/usr/bin/false", &peer_build)
        .await
        .expect("false fixture should copy");
    for path in [&daemon_build, &peer_build] {
        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .await
            .expect("fixture should be executable");
        let status = tokio::process::Command::new("/usr/bin/codesign")
            .args([
                "--force",
                "--sign",
                "-",
                "--identifier",
                "com.openai.codex.kcf007.shared-designated-requirement",
                "--requirements",
                "=designated => identifier \"com.openai.codex.kcf007.shared-designated-requirement\"",
            ])
            .arg(path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .expect("codesign should run");
        assert!(status.success(), "codesign should succeed: {status}");
    }

    assert!(
        PeerExecutableIdentity::shares_designated_requirement_with(&daemon_build, &peer_build)
            .expect("fixtures should have inspectable designated requirements"),
        "the superseded designated-requirement comparator would authorize this pair"
    );
    let daemon_identity = PeerExecutableIdentity::static_code_identity_from_path(&daemon_build)
        .expect("daemon fixture should have static-code identity");
    let peer_identity = PeerExecutableIdentity::static_code_identity_from_path(&peer_build)
        .expect("peer fixture should have static-code identity");
    assert_ne!(daemon_identity, peer_identity);
    assert!(
        !identities_match(daemon_identity, peer_identity),
        "the exact static-code comparison must reject the old comparator's accepted pair"
    );
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
async fn shutdown_is_only_accepted_on_managed_local_socket_for_its_own_pid() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let socket_path = test_socket_path(temp_dir.path());
    let (tx, mut rx) = mpsc::channel(CHANNEL_CAPACITY);
    let shutdown = CancellationToken::new();
    let acceptor = start_control_socket_acceptor(
        socket_path.clone(),
        tx,
        shutdown.clone(),
        DaemonShutdownAccess::Disabled,
    )
    .await
    .expect("acceptor");

    let stream = connect_to_socket(socket_path.as_path())
        .await
        .expect("connect");
    assert!(
        client_async("ws://localhost/daemon/shutdown", stream)
            .await
            .is_err()
    );
    assert!(rx.try_recv().is_err());
    shutdown.cancel();
    acceptor.await.expect("acceptor shutdown");

    let (tx, mut rx) = mpsc::channel(CHANNEL_CAPACITY);
    let shutdown = CancellationToken::new();
    let acceptor = start_control_socket_acceptor(
        socket_path.clone(),
        tx,
        shutdown.clone(),
        DaemonShutdownAccess::Managed,
    )
    .await
    .expect("managed acceptor");
    let stream = connect_to_socket(socket_path.as_path())
        .await
        .expect("connect");
    let (mut websocket, _) = client_async("ws://localhost/daemon/shutdown", stream)
        .await
        .expect("upgrade");
    websocket
        .send(WebSocketMessage::Text("0".into()))
        .await
        .expect("wrong pid");
    assert!(!matches!(
        websocket.next().await,
        Some(Ok(WebSocketMessage::Text(_)))
    ));
    assert!(rx.try_recv().is_err());

    let stream = connect_to_socket(socket_path.as_path())
        .await
        .expect("connect");
    let (mut websocket, _) = client_async("ws://localhost/daemon/shutdown", stream)
        .await
        .expect("upgrade");
    let pid = std::process::id().to_string();
    websocket
        .send(WebSocketMessage::Text(pid.clone().into()))
        .await
        .expect("request");
    assert_eq!(
        websocket.next().await.expect("ack").expect("ack frame"),
        WebSocketMessage::Text(pid.into())
    );
    assert!(
        rx.try_recv().is_err(),
        "server must wait until the ack is received"
    );
    websocket.close(None).await.expect("confirm receipt");
    assert!(matches!(
        timeout(Duration::from_secs(2), rx.recv()).await,
        Ok(Some(TransportEvent::DaemonShutdown))
    ));
    shutdown.cancel();
    acceptor.await.expect("acceptor shutdown");
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
async fn control_socket_rejects_writable_parent_without_changing_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o777)).unwrap();
    let path = AbsolutePathBuf::from_absolute_path(directory.path().join("rpc.sock")).unwrap();
    let (tx, _rx) = mpsc::channel(CHANNEL_CAPACITY);
    let error = start_control_socket_acceptor(
        path.clone(),
        tx,
        CancellationToken::new(),
        DaemonShutdownAccess::Disabled,
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    assert_eq!(
        std::fs::metadata(directory.path())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o777
    );
    assert!(std::fs::symlink_metadata(path).is_err());
}

#[cfg(unix)]
#[tokio::test]
async fn control_socket_file_is_private_after_bind() {
    use std::os::unix::fs::PermissionsExt;

    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let socket_path = test_socket_path(temp_dir.path());
    let parent = socket_path.as_path().parent().unwrap();
    std::fs::create_dir_all(parent).unwrap();
    std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o755)).unwrap();
    let (transport_event_tx, _transport_event_rx) =
        mpsc::channel::<TransportEvent>(CHANNEL_CAPACITY);
    let shutdown_token = CancellationToken::new();
    let accept_handle = start_control_socket_acceptor(
        socket_path.clone(),
        transport_event_tx,
        shutdown_token.clone(),
        DaemonShutdownAccess::Disabled,
    )
    .await
    .expect("control socket acceptor should start");

    let metadata = tokio::fs::metadata(socket_path.as_path())
        .await
        .expect("socket metadata should exist");
    assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    assert_eq!(
        std::fs::metadata(parent).unwrap().permissions().mode() & 0o777,
        0o755
    );
    let physical_path = std::fs::read_link(socket_path.as_path()).expect("rendezvous symlink");
    assert_eq!(
        physical_path.parent(),
        Some(
            codex_uds::shared_daemon_socket_directory()
                .unwrap()
                .as_path()
        )
    );

    shutdown_token.cancel();
    accept_handle.await.expect("acceptor should join");
    assert!(!physical_path.exists());
    assert!(std::fs::symlink_metadata(socket_path.as_path()).is_err());

    // Simulate a dangling rendezvous left by an interrupted cleanup.
    std::os::unix::fs::symlink(&physical_path, socket_path.as_path()).unwrap();
    let (sender, _receiver) = mpsc::channel::<TransportEvent>(CHANNEL_CAPACITY);
    let shutdown = CancellationToken::new();
    let (first, second) = tokio::join!(
        start_control_socket_acceptor(
            socket_path.clone(),
            sender.clone(),
            shutdown.clone(),
            DaemonShutdownAccess::Disabled,
        ),
        start_control_socket_acceptor(
            socket_path.clone(),
            sender,
            shutdown.clone(),
            DaemonShutdownAccess::Disabled,
        ),
    );
    let (acceptor, error) = match (first, second) {
        (Ok(acceptor), Err(error)) | (Err(error), Ok(acceptor)) => (acceptor, error),
        _ => panic!("exactly one concurrent restart should replace the stale symlink"),
    };
    assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse);
    let _client = connect_to_socket(socket_path.as_path()).await.unwrap();

    // Cleanup must not remove a replacement at the advertised path.
    std::fs::remove_file(socket_path.as_path()).unwrap();
    std::fs::write(socket_path.as_path(), b"replacement").unwrap();
    shutdown.cancel();
    acceptor.await.unwrap();
    assert_eq!(
        std::fs::read(socket_path.as_path()).unwrap(),
        b"replacement"
    );
    assert!(!physical_path.exists());
}

#[cfg(windows)]
#[tokio::test]
async fn control_socket_pins_directory_until_shutdown() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let socket_path = test_socket_path(temp_dir.path());
    let directory = socket_path.as_path().parent().unwrap();
    let moved = temp_dir.path().join("moved");
    let (tx, _rx) = mpsc::channel::<TransportEvent>(CHANNEL_CAPACITY);
    let shutdown = CancellationToken::new();
    let acceptor = start_control_socket_acceptor(
        socket_path.clone(),
        tx,
        shutdown.clone(),
        DaemonShutdownAccess::Disabled,
    )
    .await
    .expect("acceptor");
    assert!(std::fs::rename(directory, &moved).is_err());
    shutdown.cancel();
    acceptor.await.expect("shutdown");
    std::fs::rename(directory, moved).expect("directory unpinned after cleanup");
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
