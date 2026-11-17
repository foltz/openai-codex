//! Bound-grant tests use the actual storage and HTTP refresh lifecycle.
use super::*;
use crate::OAuthRequestCredentials;
use pretty_assertions::assert_eq;
fn grant(client: &OAuthSessionManager, id: &str, access: &str, expiry: i64) {
    client
        .save_token(&StoredToken {
            observation_epoch: None,
            authorization_id: Some(id.into()),
            access_token: access.into(),
            refresh_token: Some("synthetic-refresh".into()),
            expires_at: Some(expiry),
        })
        .unwrap();
}
async fn credentials(
    request: &mut crate::oauth_session::OAuthSessionRequest,
) -> std::io::Result<crate::oauth_session::OAuthRequestCredentials> {
    // Native credential reads and commits include the real encrypted store.
    // Product HTTP deadlines and the unchanged nextest case bound apply.
    request.credentials().await
}
#[tokio::test]
async fn replacement_sign_in_is_refused_before_any_refresh_request() {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    let (client, _home) = client(config(&server), Arc::new(MockKeyringStore::default()));
    grant(
        &client,
        "replacement",
        "replacement-expired",
        Utc::now().timestamp() - 10,
    );
    let mut request = client.begin_bound_request("origin").unwrap();
    assert!(credentials(&mut request).await.is_err());
    server.verify().await;
}
#[tokio::test]
async fn same_grant_refresh_returns_token_and_preserves_bound_identity() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            json!({"access_token":"renewed","token_type":"Bearer","expires_in":3600}),
        ))
        .expect(1)
        .mount(&server)
        .await;
    let (client, _home) = client(config(&server), Arc::new(MockKeyringStore::default()));
    grant(&client, "origin", "expired", Utc::now().timestamp() - 10);
    let mut request = client.begin_bound_request("origin").unwrap();
    let actual = credentials(&mut request).await.unwrap();
    assert_eq!(actual.access_token, "renewed");
    assert_eq!(actual.authorization_id.as_deref(), Some("origin"));
    server.verify().await;
}
#[tokio::test]
async fn rejection_keeps_original_grant_when_another_manager_replaces_sign_in() {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    let keyring = Arc::new(MockKeyringStore::default());
    let (client, home) = client(config(&server), keyring.clone());
    grant(&client, "origin", "first", Utc::now().timestamp() + 3600);
    let mut request = client.begin_bound_request("origin").unwrap();
    assert_eq!(
        credentials(&mut request).await.unwrap().access_token,
        "first"
    );
    assert!(request.reject_credentials());
    assert!(request.is_recovering());
    let replacement = OAuthSessionManager::new(
        config(&server),
        home.path().to_path_buf(),
        transport_default_auth_route_config().http_client_factory(),
        keyring,
    )
    .unwrap();
    grant(
        &replacement,
        "second-sign-in",
        "replacement-expired",
        Utc::now().timestamp() - 10,
    );
    assert!(credentials(&mut request).await.is_err());
    server.verify().await;
}
#[tokio::test]
async fn concurrent_direct_replacement_is_not_overwritten_by_refresh_completion() {
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/token", listener.local_addr().unwrap());
    let (arrived_tx, arrived_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let responder = tokio::spawn(async move {
        let mut socket = tokio::time::timeout(Duration::from_secs(2), async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut chunk = [0u8; 1024];
            loop {
                let n = socket.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&chunk[..n]);
                assert!(bytes.len() < 65536);
                if let Some(head) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = std::str::from_utf8(&bytes[..head]).unwrap();
                    assert!(headers.starts_with("POST /token "));
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .expect("bounded content length");
                    if bytes.len() >= head + 4 + length {
                        break;
                    }
                }
            }
            socket
        })
        .await
        .expect("bounded controlled token request");
        arrived_tx.send(()).unwrap();
        // Deliberate response hold spans real encrypted-store I/O. The existing
        // HTTP deadline and nextest case bound apply, not the handshake timer.
        release_rx.await.unwrap();
        let body = r#"{"access_token":"late-old-grant","token_type":"Bearer","expires_in":3600}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.shutdown().await.unwrap();
        })
        .await
        .expect("bounded controlled token response write");
    });
    let mut settings = loopback_config();
    settings.token_url = endpoint;
    let keyring = Arc::new(MockKeyringStore::default());
    let (client, home) = client(settings.clone(), keyring.clone());
    grant(&client, "origin", "expired", Utc::now().timestamp() - 10);
    let mut request = client.begin_bound_request("origin").unwrap();
    let task = tokio::spawn(async move { request.credentials().await });
    tokio::time::timeout(Duration::from_secs(2), arrived_rx)
        .await
        .unwrap()
        .unwrap();
    let replacement = OAuthSessionManager::new(
        settings,
        home.path().to_path_buf(),
        transport_default_auth_route_config().http_client_factory(),
        keyring,
    )
    .unwrap();
    let writer = replacement.clone();
    tokio::task::spawn_blocking(move || {
        grant(
            &writer,
            "replacement",
            "must-survive",
            Utc::now().timestamp() + 3600,
        );
    })
    .await
    .unwrap();
    release_tx.send(()).unwrap();
    let error = task.await.unwrap().unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    tokio::time::timeout(Duration::from_secs(2), responder)
        .await
        .unwrap()
        .unwrap();
    let stored = replacement.load_token().unwrap().unwrap();
    assert_eq!(stored.access_token, "must-survive");
    assert_eq!(stored.authorization_id.as_deref(), Some("replacement"));
}

#[tokio::test]
async fn old_bound_refusal_does_not_discard_new_grant_pending_rotation() {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    let (client, _home) = client(config(&server), Arc::new(MockKeyringStore::default()));
    grant(
        &client,
        "new-grant",
        "new-access",
        Utc::now().timestamp() + 3600,
    );
    let stored = client.load_token().unwrap().unwrap();
    let mut pending = stored.clone();
    pending.access_token = "new-pending-rotation".into();
    {
        let mut cached = client.state.cached_token.lock().await;
        cached.token = Some(stored);
        cached.pending = Some(pending.clone());
    }
    let mut request = client.begin_bound_request("old-grant").unwrap();
    assert!(credentials(&mut request).await.is_err());
    assert_eq!(
        client
            .state
            .cached_token
            .lock()
            .await
            .pending
            .as_ref()
            .map(|t| t.access_token.as_str()),
        Some("new-pending-rotation")
    );
    server.verify().await;
}
#[test]
fn blank_admission_is_refused_without_token_resolution() {
    let (client, _home) = client(loopback_config(), Arc::new(MockKeyringStore::default()));
    assert!(client.begin_bound_request(" ").is_err());
}

#[tokio::test]
async fn typed_origin_binding_is_captured_with_token_and_reused_without_login() {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    let (client, _home) = client(config(&server), Arc::new(MockKeyringStore::default()));
    grant(
        &client,
        "origin",
        "actual-origin-token",
        Utc::now().timestamp() + 3600,
    );
    let mut origin = client.begin_request();
    let selected = credentials(&mut origin).await.unwrap();
    assert_eq!(selected.access_token, "actual-origin-token");
    let binding = selected.binding().unwrap().clone();
    let mut admitted = client.begin_admitted_request(&binding).unwrap();
    let current = credentials(&mut admitted).await.unwrap();
    assert_eq!(current.binding(), Some(&binding));
    assert_eq!(current.access_token, selected.access_token);
    server.verify().await;
}
#[tokio::test]
async fn typed_binding_refuses_other_home_or_provider_configuration() {
    let server = MockServer::start().await;
    let keyring = Arc::new(MockKeyringStore::default());
    let (origin, home) = client(config(&server), keyring.clone());
    grant(&origin, "origin", "token", Utc::now().timestamp() + 3600);
    let mut request = origin.begin_request();
    let selected = credentials(&mut request).await.unwrap();
    let binding = selected.binding().unwrap();
    let other_home = tempfile::tempdir().unwrap();
    let other = OAuthSessionManager::new(
        config(&server),
        other_home.path().to_path_buf(),
        transport_default_auth_route_config().http_client_factory(),
        keyring.clone(),
    )
    .unwrap();
    assert!(other.begin_admitted_request(binding).is_err());
    let mut changed = config(&server);
    changed.client_id = "another-client".into();
    let changed = OAuthSessionManager::new(
        changed,
        home.path().to_path_buf(),
        transport_default_auth_route_config().http_client_factory(),
        keyring,
    )
    .unwrap();
    assert!(changed.begin_admitted_request(binding).is_err());
}
#[tokio::test]
async fn legacy_token_without_grant_id_does_not_mint_typed_binding() {
    let server = MockServer::start().await;
    let (client, _home) = client(config(&server), Arc::new(MockKeyringStore::default()));
    save_token(
        &client,
        "legacy-token",
        Some("legacy-refresh"),
        Utc::now().timestamp() + 3600,
    );
    let mut request = client.begin_request();
    let selected = credentials(&mut request).await.unwrap();
    assert_eq!(selected.access_token, "legacy-token");
    assert!(selected.binding().is_none());
}

#[tokio::test]
async fn ordinary_observed_resolution_captures_actual_credential_binding() {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    let (client, _home) = client(config(&server), Arc::new(MockKeyringStore::default()));
    grant(&client, "origin", "selected", Utc::now().timestamp() + 3600);
    let observed = tokio::time::timeout(
        Duration::from_secs(2),
        client.resolve_access_token_observed(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(observed.access_token, "selected");
    assert_eq!(observed.authorization_id.as_deref(), Some("origin"));
    let mut admitted = client
        .begin_admitted_request(observed.binding().unwrap())
        .unwrap();
    assert_eq!(
        credentials(&mut admitted).await.unwrap().access_token,
        "selected"
    );
    server.verify().await;
}

fn another(
    client: &OAuthSessionManager,
    home: &std::path::Path,
    keyring: Arc<MockKeyringStore>,
) -> OAuthSessionManager {
    OAuthSessionManager::new(
        client.config().clone(),
        home.to_path_buf(),
        transport_default_auth_route_config().http_client_factory(),
        keyring,
    )
    .unwrap()
}
#[tokio::test]
async fn actual_second_manager_aba_retires_old_epoch_even_with_restored_grant_id() {
    let keyring = Arc::new(MockKeyringStore::default());
    let (client, home) = client(loopback_config(), keyring.clone());
    grant(
        &client,
        "origin",
        "original-token",
        Utc::now().timestamp() + 3600,
    );
    let selected = client.resolve_access_token_observed().await.unwrap();
    let binding = selected.binding().unwrap().clone();
    let other = another(&client, home.path(), keyring);
    grant(
        &other,
        "replacement",
        "second-token",
        Utc::now().timestamp() + 3600,
    );
    grant(
        &other,
        "origin",
        "original-token",
        Utc::now().timestamp() + 3600,
    );
    assert!(client.begin_admitted_request(&binding).is_err());
}
#[tokio::test]
async fn actual_logout_invalidates_other_managers_admission() {
    let keyring = Arc::new(MockKeyringStore::default());
    let (client, home) = client(loopback_config(), keyring.clone());
    grant(&client, "origin", "token", Utc::now().timestamp() + 3600);
    let selected = client.resolve_access_token_observed().await.unwrap();
    let binding = selected.binding().unwrap().clone();
    let other = another(&client, home.path(), keyring);
    // Native encrypted removal is bounded by the existing nextest case policy;
    // it is not a two-second transport-handshake operation.
    assert!(other.logout().await.unwrap());
    assert!(other.load_token().unwrap().is_none());
    assert!(client.begin_admitted_request(&binding).is_err());
}
#[tokio::test]
async fn principal_replacement_with_same_grant_id_retires_binding() {
    use base64::Engine;
    let (client, _home) = client(loopback_config(), Arc::new(MockKeyringStore::default()));
    let token = |sub: &str| {
        format!(
            "e30.{}.signature",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json!({"sub":sub}).to_string())
        )
    };
    grant(
        &client,
        "grant",
        &token("original"),
        Utc::now().timestamp() + 3600,
    );
    let selected = client.resolve_access_token_observed().await.unwrap();
    let binding = selected.binding().unwrap().clone();
    grant(
        &client,
        "grant",
        &token("replacement"),
        Utc::now().timestamp() + 3600,
    );
    assert!(client.begin_admitted_request(&binding).is_err());
}
#[tokio::test]
async fn legacy_credential_id_does_not_authorize_another_storage_backend() {
    let keyring = Arc::new(MockKeyringStore::default());
    let (client, home) = client(loopback_config(), keyring.clone());
    grant(&client, "grant", "token", Utc::now().timestamp() + 3600);
    let selected = client.resolve_access_token_observed().await.unwrap();
    let binding = selected.binding().unwrap().clone();
    let mut settings = client.config().clone();
    settings.credentials_store = codex_model_provider_info::OAuthCredentialsStore::File;
    let other = OAuthSessionManager::new(
        settings,
        home.path().to_path_buf(),
        transport_default_auth_route_config().http_client_factory(),
        keyring,
    )
    .unwrap();
    assert_eq!(other.credential_id(), client.credential_id());
    assert!(other.begin_admitted_request(&binding).is_err());
}
#[test]
fn actual_blocked_load_does_not_block_save_or_publish_stale_epoch() {
    let (client, _home) = client(loopback_config(), Arc::new(MockKeyringStore::default()));
    grant(
        &client,
        "origin",
        "old-token",
        Utc::now().timestamp() + 3600,
    );
    let old = client.load_token().unwrap().unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    *client.state.load_hook.lock().unwrap() = Some(Box::new(move || {
        entered_tx.send(()).unwrap();
        release_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    }));
    let reader = client.clone();
    let (loaded_tx, loaded_rx) = std::sync::mpsc::channel();
    let read = std::thread::spawn(move || {
        loaded_tx
            .send(reader.load_token().unwrap().unwrap())
            .unwrap();
    });
    // The hook runs after native I/O captured old bytes, before observation.
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let writer = client.clone();
    let (saved_tx, saved_rx) = std::sync::mpsc::channel();
    let write = std::thread::spawn(move || {
        grant(
            &writer,
            "replacement",
            "new-token",
            Utc::now().timestamp() + 3600,
        );
        saved_tx.send(()).unwrap();
    });
    // The real write must complete while the old read is still suspended.
    saved_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    release_tx.send(()).unwrap();
    let current = loaded_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    read.join().unwrap();
    write.join().unwrap();
    assert_eq!(current.access_token, "new-token");
    assert_eq!(current.authorization_id.as_deref(), Some("replacement"));
    let stale = super::super::OAuthRequestCredentials::from_resolved(&client, old);
    assert!(stale.binding().is_none());
    let current = super::super::OAuthRequestCredentials::from_resolved(&client, current);
    assert!(current.binding().is_some());
}

#[tokio::test]
async fn resolved_token_cannot_acquire_a_new_binding_after_sign_in_aba() {
    let server = MockServer::start().await;
    let mut settings = config(&server);
    settings.credentials_store = codex_model_provider_info::OAuthCredentialsStore::File;
    let (manager, _home) = client(settings, Arc::new(MockKeyringStore::default()));
    grant(&manager, "origin", "original-access", i64::MAX);
    let original = manager.load_token().unwrap().unwrap();
    let encoded = serde_json::to_value(&original).unwrap();
    assert!(encoded.get("observation_epoch").is_none());
    let unobserved = serde_json::from_value(encoded).unwrap();
    assert!(
        OAuthRequestCredentials::from_resolved(&manager, unobserved)
            .binding()
            .is_none()
    );
    let first = OAuthRequestCredentials::from_resolved(&manager, original.clone());
    let first_binding = first.binding().unwrap().clone();
    assert!(manager.begin_admitted_request(&first_binding).is_ok());

    let cached = manager.resolve_access_token_observed().await.unwrap();
    assert_eq!(cached.binding(), Some(&first_binding));
    // Same-grant renewal keeps continuity; a sign-in ABA must not.
    grant(&manager, "origin", "renewed-access", i64::MAX);
    assert!(manager.begin_admitted_request(&first_binding).is_ok());
    grant(&manager, "replacement", "replacement-access", i64::MAX);
    grant(&manager, "origin", "original-access", i64::MAX);
    assert!(manager.begin_admitted_request(&first_binding).is_err());
    assert!(
        OAuthRequestCredentials::from_resolved(&manager, original)
            .binding()
            .is_none()
    );
    let current = manager.resolve_access_token_observed().await.unwrap();
    assert_eq!(current.access_token, "original-access");
    assert_ne!(current.binding(), Some(&first_binding));
    assert!(
        manager
            .begin_admitted_request(current.binding().unwrap())
            .is_ok()
    );
}
