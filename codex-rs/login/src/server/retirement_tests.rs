use super::*;
use pretty_assertions::assert_eq;
use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::time::Duration;

fn server() -> (tempfile::TempDir, crate::LoginServer) {
    let home = tempfile::tempdir().unwrap();
    let mut options = crate::ServerOptions::new(
        home.path().to_path_buf(),
        crate::CLIENT_ID.to_owned(),
        /*forced_chatgpt_workspace_id*/ None,
        crate::AuthCredentialsStoreMode::Ephemeral,
        crate::AuthKeyringBackendKind::default(),
        crate::test_support::transport_default_auth_route_config(),
    );
    options.port = 0;
    options.open_browser = false;
    (home, crate::run_login_server(options).unwrap())
}

#[tokio::test]
async fn real_login_retirement_retains_joins_and_first_deadline() {
    let (_home, server) = server();
    let handle = server.cancel_handle();
    let deadline = Instant::now() + Duration::from_secs(5);
    let retirement = handle.begin_retirement(deadline).unwrap();
    drop(server);
    let second = handle
        .begin_retirement(deadline + Duration::from_secs(60))
        .unwrap();
    drop(handle);
    assert_eq!(second.deadline(), deadline);
    let (first, repeated) = tokio::join!(retirement.wait(), second.wait());
    assert_eq!(
        first,
        LoginRetirementReport {
            callback: Some(LoginWorkerOutcome::Joined),
            response: Some(LoginWorkerOutcome::Joined),
            receiver: Some(LoginWorkerOutcome::Joined),
            http: Some(LoginHttpReport {
                acceptor: Some(LoginWorkerOutcome::Joined),
                ..Default::default()
            }),
            persistence: Some(LoginWorkerOutcome::Joined),
            deadline_expired: false,
        }
    );
    assert_eq!(repeated, first);
    assert_eq!(retirement.wait().await, first);
}

#[tokio::test]
async fn expired_attempt_never_refreshes_or_first_polls_cleanup() {
    let (_home, server) = server();
    let handle = server.cancel_handle();
    let deadline = Instant::now();
    let retirement = handle.begin_retirement(deadline).unwrap();
    let expected = LoginRetirementReport {
        deadline_expired: true,
        ..Default::default()
    };
    assert_eq!(retirement.wait().await, expected);
    let second = handle
        .begin_retirement(deadline + Duration::from_secs(60))
        .unwrap();
    assert_eq!(second.deadline(), deadline);
    assert_eq!(second.wait().await, expected);
    // Explicit test cleanup uses the existing ordinary API, not an extended
    // retirement budget. The expired report must remain unchanged afterwards.
    server.cancel();
    assert!(server.block_until_done().await.is_err());
    assert_eq!(retirement.wait().await, expected);
}

#[tokio::test]
async fn production_cancel_response_is_delivered_before_login_finishes() {
    let (_home, server) = server();
    let port = server.actual_port;
    let response = tokio::task::spawn_blocking(move || {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
        stream
            .write_all(
                format!(
                    "GET /cancel HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
                )
                .as_bytes(),
            )
            .unwrap();
        let mut bytes = Vec::new();
        stream.read_to_end(&mut bytes).unwrap();
        String::from_utf8(bytes).unwrap()
    })
    .await
    .unwrap();
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(
        response
            .to_ascii_lowercase()
            .contains("connection: close\r\n")
    );
    assert!(response.ends_with("Login cancelled"));
    assert!(server.block_until_done().await.is_err());
}
