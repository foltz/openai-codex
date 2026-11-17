//! Actual-socket source fixtures. Helpers run in scrubbed child processes so
//! their peer-policy environment cannot affect concurrent source tests.
//! Not immutable production CLI/owner, auth or managed-role qualification.
#![cfg(all(test, any(target_os = "linux", target_os = "macos")))]

use std::io;
use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, Instant};

use codex_app_server_client::RemoteAppServerClient;
use codex_app_server_client::RemoteAppServerConnectArgs;
use codex_app_server_client::RemoteAppServerEndpoint;
use codex_uds::UnixListener;
use codex_utils_absolute_path::AbsolutePathBuf;
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::io::AsyncReadExt;
use tokio_tungstenite::tungstenite::Message;

const CASE_ENV: &str = "KCF_TEST_UNIX_PEER_CASE";
const SOCKET_ENV: &str = "KCF_TEST_UNIX_PEER_SOCKET";
const READY_ENV: &str = "KCF_TEST_UNIX_PEER_READY";
const OBSERVED_ENV: &str = "KCF_TEST_UNIX_PEER_OBSERVED";

fn case(name: &str, policy: Option<&str>) {
    let executable = std::env::current_exe().unwrap();
    let mut command = std::process::Command::new(executable);
    command
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env(CASE_ENV, name)
        .args(["--exact", "peer_client_helper", "--nocapture"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(home) = std::env::var_os("HOME") {
        command.env("HOME", home);
    }
    if let Some(tmp) = std::env::var_os("TMPDIR") {
        command.env("TMPDIR", tmp);
    }
    if let Some(policy) = policy {
        command.env("CODEX_UNIX_PEER_POLICY", policy);
    }
    let mut child = command.spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(25);
    loop {
        if child.try_wait().unwrap().is_some() {
            break;
        }
        if Instant::now() >= deadline {
            child.kill().expect("kill exact fixture child");
            let output = child.wait_with_output().unwrap();
            panic!(
                "peer fixture timed out: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "case {name}: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn unix_peer_same_image_initializes() {
    case("same", Some("same-image"));
}
#[test]
fn unix_peer_wrong_image_refuses_before_websocket() {
    case("wrong", Some("same-image"));
}
#[test]
fn unix_peer_copied_build_obeys_platform_identity() {
    case("copy", Some("same-image"));
}
#[test]
fn unix_peer_new_socket_rechecks_after_prior_same_image_connection() {
    case("reconnect", Some("same-image"));
}
#[test]
fn unix_peer_unknown_policy_refuses_before_websocket() {
    case("unknown", Some("unsupported"));
}
#[test]
fn unix_peer_unset_policy_preserves_ordinary_compatibility() {
    case("ordinary", None);
}

// Selected only by a re-exec child. Qualification selects the six unix_peer_*
// cases above, rather than counting unconfigured helpers as behavioral proof.
#[test]
fn peer_client_helper() {
    let Some(case) = std::env::var_os(CASE_ENV) else {
        return;
    };
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let current = std::env::current_exe().unwrap();
        let copy = dir.path().join("copied-peer");
        let wrong = dir.path().join("different-image-peer");
        let case_name = case.to_str().unwrap();
        let needs_wrong = matches!(case_name, "wrong" | "ordinary" | "reconnect");
        use std::os::unix::fs::PermissionsExt;
        for (needed, image) in [(case_name == "copy", &copy), (needs_wrong, &wrong)] {
            if needed {
                let bytes = std::fs::copy(&current, image).unwrap();
                assert_eq!(bytes, std::fs::metadata(&current).unwrap().len());
                std::fs::set_permissions(image, std::fs::Permissions::from_mode(0o700)).unwrap();
            }
        }
        #[cfg(target_os = "macos")]
        if needs_wrong {
            let mut command = tokio::process::Command::new("/usr/bin/codesign");
            command
                .args([
                    "--force",
                    "--sign",
                    "-",
                    "--identifier",
                    "com.openai.codex.kcf-unix-peer-negative",
                ])
                .arg(&wrong)
                .kill_on_drop(true)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::piped());
            let output = tokio::time::timeout(Duration::from_secs(10), command.output())
                .await
                .unwrap()
                .unwrap();
            assert!(
                output.status.success(),
                "ad-hoc fixture signing: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        let socket = dir.path().join("connection.sock");
        match case.to_str().unwrap() {
            "same" => connect_once(&current, &socket, dir.path(), 1, None).await,
            "wrong" => {
                connect_once(
                    &wrong,
                    &socket,
                    dir.path(),
                    1,
                    Some(io::ErrorKind::PermissionDenied),
                )
                .await
            }
            "unknown" => {
                connect_once(
                    &current,
                    &socket,
                    dir.path(),
                    1,
                    Some(io::ErrorKind::InvalidInput),
                )
                .await
            }
            "ordinary" => connect_once(&wrong, &socket, dir.path(), 1, None).await,
            "copy" => {
                #[cfg(target_os = "linux")]
                let expected = Some(io::ErrorKind::PermissionDenied);
                #[cfg(target_os = "macos")]
                let expected = None;
                connect_once(&copy, &socket, dir.path(), 1, expected).await;
            }
            "reconnect" => {
                connect_once(&current, &socket, dir.path(), 1, None).await;
                // Reuse the same pathname after the owned first server exits.
                // A cached approval of this endpoint must not authorize it.
                if socket.exists() {
                    std::fs::remove_file(&socket).unwrap();
                }
                connect_once(
                    &wrong,
                    &socket,
                    dir.path(),
                    2,
                    Some(io::ErrorKind::PermissionDenied),
                )
                .await;
            }
            other => panic!("unknown fixture case {other}"),
        }
    });
}

async fn connect_once(
    executable: &Path,
    socket: &Path,
    root: &Path,
    attempt: usize,
    refusal: Option<io::ErrorKind>,
) {
    let ready = root.join(format!("ready-{attempt}"));
    let observed = root.join(format!("observed-{attempt}.json"));
    let mut command = tokio::process::Command::new(executable);
    command
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env(SOCKET_ENV, socket)
        .env(READY_ENV, &ready)
        .env(OBSERVED_ENV, &observed)
        .env(
            "KCF_TEST_UNIX_PEER_EXPECT_EOF",
            if refusal.is_some() { "1" } else { "0" },
        )
        .args(["--exact", "peer_server_helper", "--nocapture"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut server = command.spawn().unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !ready.exists() {
            assert!(
                server.try_wait().unwrap().is_none(),
                "fixture server exited before binding"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("bounded fixture bind");
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        RemoteAppServerClient::connect(RemoteAppServerConnectArgs {
            interactive_client: false,
            required_human_approval: false,
            endpoint: RemoteAppServerEndpoint::UnixSocket {
                socket_path: AbsolutePathBuf::from_absolute_path(socket).unwrap(),
            },
            client_name: "unix-peer-fixture".into(),
            client_version: "source-test".into(),
            experimental_api: false,
            mcp_server_openai_form_elicitation: false,
            opt_out_notification_methods: Vec::new(),
            channel_capacity: 8,
        }),
    )
    .await
    .expect("bounded real client connection");
    match refusal {
        Some(expected) => match result {
            Ok(_) => panic!("wrong/invalid peer was admitted"),
            Err(error) => assert_eq!(error.kind(), expected),
        },
        None => {
            let client = result.expect("eligible or ordinary connection initializes");
            tokio::time::timeout(Duration::from_secs(3), client.shutdown())
                .await
                .unwrap()
                .unwrap();
        }
    }
    let output = tokio::time::timeout(Duration::from_secs(3), server.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        output.status.success(),
        "fixture server: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let evidence: Value = serde_json::from_slice(&std::fs::read(observed).unwrap()).unwrap();
    if refusal.is_some() {
        assert_eq!(
            evidence,
            json!({"firstReadBytes": 0, "websocketUpgraded": false})
        );
    } else {
        assert_eq!(evidence, json!({"initialize": true, "initialized": true}));
    }
}

#[test]
fn peer_server_helper() {
    let Some(socket) = std::env::var_os(SOCKET_ENV) else {
        return;
    };
    let ready = std::path::PathBuf::from(std::env::var_os(READY_ENV).unwrap());
    let observed = std::path::PathBuf::from(std::env::var_os(OBSERVED_ENV).unwrap());
    let expect_eof = std::env::var("KCF_TEST_UNIX_PEER_EXPECT_EOF").unwrap() == "1";
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        // A helper also has its own bound, so an outer-test timeout cannot
        // leave a detached server waiting indefinitely.
        tokio::time::timeout(Duration::from_secs(8), async {
            let mut listener = UnixListener::bind(Path::new(&socket)).await.unwrap();
            std::fs::write(ready, b"bound").unwrap();
            let mut stream = listener.accept().await.unwrap();
            if expect_eof {
                let count = stream.read(&mut [0; 1]).await.unwrap();
                assert_eq!(count, 0, "policy refusal must precede WebSocket/request bytes");
                std::fs::write(observed, json!({"firstReadBytes": count, "websocketUpgraded": false}).to_string()).unwrap();
                return;
            }
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let Message::Text(message) = websocket.next().await.unwrap().unwrap() else { panic!("initialize text"); };
            let request: Value = serde_json::from_str(&message).unwrap();
            assert_eq!(request["method"], "initialize");
            websocket.send(Message::Text(json!({"jsonrpc": "2.0", "id": request["id"], "result": {"userAgent": "peer-fixture/source-test"}}).to_string().into())).await.unwrap();
            let Message::Text(message) = websocket.next().await.unwrap().unwrap() else { panic!("initialized notification"); };
            let notification: Value = serde_json::from_str(&message).unwrap();
            assert_eq!(notification["method"], "initialized");
            std::fs::write(observed, json!({"initialize": true, "initialized": true}).to_string()).unwrap();
            // Let the client own shutdown; do not generate a spurious transport
            // error while it is completing the initialization handshake.
            while let Some(message) = websocket.next().await {
                match message.unwrap() {
                    Message::Close(_) => { let _ = websocket.flush().await; break; }
                    other => panic!("unexpected post-initialize frame {other:?}"),
                }
            }
        }).await.expect("bounded fixture server lifecycle");
    });
}
