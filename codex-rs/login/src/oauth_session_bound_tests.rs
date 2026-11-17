//! Bound-grant tests use the actual storage and HTTP refresh lifecycle.
use super::*;
fn grant(client: &OAuthSessionManager, id: &str, access: &str, expiry: i64) {
    client
        .save_token(&StoredToken {
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
    tokio::time::timeout(Duration::from_secs(2), request.credentials())
        .await
        .expect("bounded OAuth request")
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
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/token", listener.local_addr().unwrap());
    let (arrived_tx, arrived_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let responder = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(2),async move {
            let (mut socket,_)=listener.accept().await.unwrap();
            let mut bytes=Vec::new();let mut chunk=[0u8;1024];
            loop {
                let n=socket.read(&mut chunk).await.unwrap();assert!(n>0);bytes.extend_from_slice(&chunk[..n]);assert!(bytes.len()<65536);
                if let Some(head)=bytes.windows(4).position(|w|w==b"\r\n\r\n") {
                    let headers=std::str::from_utf8(&bytes[..head]).unwrap();
                    assert!(headers.starts_with("POST /token "));
                    let length=headers.lines().find_map(|line| {let (name,value)=line.split_once(':')?;name.eq_ignore_ascii_case("content-length").then(||value.trim().parse::<usize>().unwrap())}).expect("bounded content length");
                    if bytes.len()>=head+4+length {break;}
                }
            }
            arrived_tx.send(()).unwrap();release_rx.await.unwrap();
            let body=r#"{"access_token":"late-old-grant","token_type":"Bearer","expires_in":3600}"#;
            let response=format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",body.len(),body);
            socket.write_all(response.as_bytes()).await.unwrap();socket.shutdown().await.unwrap();
        }).await.expect("bounded controlled token response");
    });
    let mut settings = loopback_config();
    settings.token_url = endpoint;
    let keyring = Arc::new(MockKeyringStore::default());
    let (client, home) = client(settings.clone(), keyring.clone());
    grant(&client, "origin", "expired", Utc::now().timestamp() - 10);
    let mut request = client.begin_bound_request("origin").unwrap();
    let task = tokio::spawn(async move { credentials(&mut request).await });
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
    grant(
        &replacement,
        "replacement",
        "must-survive",
        Utc::now().timestamp() + 3600,
    );
    release_tx.send(()).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
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
