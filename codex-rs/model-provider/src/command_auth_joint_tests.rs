//! Real public-constructor to live-header callback integration, synthetic only.
use super::*;
use base64::Engine;
use sha2::Digest;
use std::os::unix::fs::PermissionsExt;

struct Environment(Vec<(&'static str, Option<std::ffi::OsString>)>);
impl Environment {
    fn set(&mut self, key: &'static str, value: Option<&str>) {
        self.0.push((key, std::env::var_os(key)));
        unsafe {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }
}
impl Drop for Environment {
    fn drop(&mut self) {
        for (key, value) in self.0.drain(..).rev() {
            unsafe {
                match value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }
    }
}

#[tokio::test]
#[serial_test::serial(codex_auth_env)]
async fn selected_command_live_callback_uses_actual_rejection_and_refuses_missing_proof() {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
    let home = tempfile::tempdir().unwrap();
    let mut env = Environment(Vec::new());
    for (key, value) in [
        (
            "CODEX_CHATGPT_AUTH_COMMAND",
            Some("/nonexistent/synthetic-provider"),
        ),
        ("CODEX_CHATGPT_AUTH_BINDING", Some("fixture#example")),
        ("CODEX_CHATGPT_AUTH_SOURCE", Some("source")),
        ("CODEX_CHATGPT_AUTH_ACCOUNT", Some("account")),
        ("CODEX_CHATGPT_AUTH_PRINCIPAL", Some("principal")),
        ("OPENAI_FEDERATION_RULE_ID", None),
        ("OPENAI_IDENTITY_TOKEN_FILE", None),
        ("CODEX_ACCESS_TOKEN", Some("unusable-ambient-token")),
        ("OPENAI_API_KEY", Some("synthetic-ambient-key")),
    ] {
        env.set(key, value);
    }
    let expiration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;
    let claims = serde_json::json!({"exp":expiration,"https://api.openai.com/auth":{"chatgpt_account_id":"account","chatgpt_user_id":"principal"}});
    let token = format!(
        "e30.{}.signature",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(claims.to_string())
    );
    let envelope = serde_json::json!({"version":1,"source":"source","binding":"fixture#example","account":"account","principal":"principal","generation":1,"accessToken":token,"expiresAt":expiration,"fingerprint":format!("{:x}",sha2::Sha256::digest(token.as_bytes()))});
    // This is a versioned on-disk compatibility fixture, validated by the real
    // command selector; no private test authority or mocked provider is used.
    let record = serde_json::json!({"auth_mode":"chatgptAuthTokens","tokens":{"id_token":token,"access_token":token,"refresh_token":"","account_id":"account"},"external_auth_owner":envelope});
    let auth_path = home.path().join("auth.json");
    let bytes = serde_json::to_vec(&record).unwrap();
    std::fs::write(&auth_path, &bytes).unwrap();
    std::fs::set_permissions(&auth_path, std::fs::Permissions::from_mode(0o400)).unwrap();
    let manager = codex_login::AuthManager::shared_from_auth_config(
        codex_login::AuthConfig {
            codex_home: home.path().to_path_buf(),
            auth_credentials_store_mode: codex_login::AuthCredentialsStoreMode::File,
            keyring_backend_kind: codex_login::AuthKeyringBackendKind::Direct,
            forced_login_method: None,
            chatgpt_base_url: None,
            forced_chatgpt_workspace_id: None,
            managed_auth_policy: Default::default(),
            auth_route_config: codex_login::test_support::transport_default_auth_route_config(),
        },
        true,
    )
    .await
    .unwrap();
    let expected = manager.auth_cached().unwrap();
    let provider = auth_provider_from_auth_manager(manager.clone(), &expected);
    let actual = provider
        .apply_auth_headers(http::HeaderMap::new())
        .await
        .unwrap();
    assert!(
        actual
            .get(http::header::AUTHORIZATION)
            .is_some_and(|header| header == format!("Bearer {token}").as_str())
    );
    let rejected = http::HeaderMap::from_iter([(
        http::header::AUTHORIZATION,
        http::HeaderValue::from_static("Bearer older-synthetic-token"),
    )]);
    assert!(provider.refresh_after_unauthorized(rejected).await);
    // Unknown evidence must not guess that the current cached token was rejected.
    assert!(
        !provider
            .refresh_after_unauthorized(http::HeaderMap::new())
            .await
    );
    assert!(manager.auth_cached().is_some());
    assert_eq!(std::fs::read(&auth_path).unwrap(), bytes);
    let current_rejected = http::HeaderMap::from_iter([(
        http::header::AUTHORIZATION,
        http::HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
    )]);
    // A rejection of the actual current credential must not claim recovery from
    // the same cache. The nonexistent provider makes renewal fail concretely.
    assert!(!provider.refresh_after_unauthorized(current_rejected).await);
    assert!(provider.apply_auth_headers(http::HeaderMap::new()).await.is_err());
    assert_eq!(std::fs::read(&auth_path).unwrap(), bytes);
    }).await.expect("bounded public constructor and callback sequence");
}
