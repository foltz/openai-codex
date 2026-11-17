//! Real gateway composition and admitted resolution, synthetic file-store only.
use super::*;
use crate::auth::{ResolvedProviderAuth, auth_provider_from_auth};
use codex_login::CodexAuth;
use codex_model_provider_info::{
    GatewayOAuthDelivery, GatewayOAuthSettings, OAuthCredentialsStore,
};
use serde_json::json;
use std::time::Duration;
use wiremock::matchers::any;
use wiremock::{Mock, MockServer, ResponseTemplate};

fn info(server: &MockServer) -> ModelProviderInfo {
    ModelProviderInfo {
        gateway_oauth: Some(GatewayOAuthConfig::OAuth(GatewayOAuthSettings {
            authorization_url: format!("{}/authorize", server.uri()),
            device_authorization_url: None,
            token_url: format!("{}/token", server.uri()),
            client_id: "synthetic-client".into(),
            resource: None,
            scopes: Vec::new(),
            redirect_port: None,
            credentials_store: OAuthCredentialsStore::File,
            delivery: GatewayOAuthDelivery::Header {
                name: "x-gateway-auth".into(),
                scheme: "Bearer".into(),
            },
        })),
        ..ModelProviderInfo::create_openai_provider(Some(server.uri()))
    }
}
fn primary(home: &std::path::Path) -> Arc<AuthManager> {
    AuthManager::from_auth_for_testing_with_home(
        CodexAuth::from_api_key("primary"),
        home.to_path_buf(),
    )
}
fn seed(
    info: &ModelProviderInfo,
    primary: &AuthManager,
    id: Option<&str>,
) -> Arc<OAuthSessionManager> {
    crate::test_support::seed_gateway_auth(
        info,
        primary,
        json!({"access_token":"gateway","authorization_id":id,"refresh_token":"synthetic-refresh","expires_at":i64::MAX}),
    )
}
async fn composed(
    info: &ModelProviderInfo,
    manager: Arc<GatewayAuthManager>,
) -> ResolvedProviderAuth {
    let source = Ok(manager);
    tokio::time::timeout(
        Duration::from_secs(2),
        crate::combined_auth::compose_auth(
            info,
            Some(&source),
            ResolvedProviderAuth::new(auth_provider_from_auth(&CodexAuth::from_api_key("primary"))),
        ),
    )
    .await
    .unwrap()
    .unwrap()
}
#[tokio::test]
async fn configured_composition_preserves_both_headers_and_actual_oauth_admission() {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    let home = tempfile::tempdir().unwrap();
    let primary = primary(home.path());
    let info = info(&server);
    let session = seed(&info, &primary, Some("origin"));
    let manager = Arc::new(GatewayAuthManager(GatewayCredentialSource::OAuth(session)));
    let resolved = composed(&info, manager.clone()).await;
    assert!(matches!(
        &resolved.gateway_admission,
        GatewayAdmission::Bound(_)
    ));
    let headers = resolved
        .auth
        .apply_auth_headers(http::HeaderMap::new())
        .await
        .unwrap();
    assert_eq!(
        headers.get(http::header::AUTHORIZATION).unwrap(),
        "Bearer primary"
    );
    assert_eq!(headers.get("x-gateway-auth").unwrap(), "Bearer gateway");
    let token = tokio::time::timeout(
        Duration::from_secs(2),
        manager.resolve_access_token_for(&resolved.gateway_admission),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(&*token, "gateway");
    server.verify().await;
}
#[tokio::test]
async fn legacy_unbound_gateway_works_ordinary_but_cannot_authorize_detached_work() {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    let home = tempfile::tempdir().unwrap();
    let primary = primary(home.path());
    let info = info(&server);
    let session = seed(&info, &primary, None);
    let manager = Arc::new(GatewayAuthManager(GatewayCredentialSource::OAuth(session)));
    let resolved = composed(&info, manager.clone()).await;
    assert_eq!(resolved.gateway_admission, GatewayAdmission::Unbound);
    assert!(
        manager
            .resolve_access_token_for(&resolved.gateway_admission)
            .await
            .is_err()
    );
    server.verify().await;
}
#[tokio::test]
async fn sign_in_replacement_refuses_actual_composed_origin_without_refresh() {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    let home = tempfile::tempdir().unwrap();
    let primary = primary(home.path());
    let info = info(&server);
    let session = seed(&info, &primary, Some("origin"));
    let manager = Arc::new(GatewayAuthManager(GatewayCredentialSource::OAuth(session)));
    let resolved = composed(&info, manager.clone()).await;
    let _replacement = seed(&info, &primary, Some("replacement"));
    assert!(
        tokio::time::timeout(
            Duration::from_secs(2),
            manager.resolve_access_token_for(&resolved.gateway_admission)
        )
        .await
        .unwrap()
        .is_err()
    );
    server.verify().await;
}
#[tokio::test]
async fn absent_gateway_is_distinct_from_configured_unbound() {
    let info = ModelProviderInfo::create_openai_provider(Some("http://127.0.0.1:1".into()));
    let resolved = crate::combined_auth::compose_auth(
        &info,
        None,
        ResolvedProviderAuth::new(auth_provider_from_auth(&CodexAuth::from_api_key("primary"))),
    )
    .await
    .unwrap();
    assert_eq!(resolved.gateway_admission, GatewayAdmission::NotConfigured);
    let headers = resolved
        .auth
        .apply_auth_headers(http::HeaderMap::new())
        .await
        .unwrap();
    assert_eq!(
        headers.get(http::header::AUTHORIZATION).unwrap(),
        "Bearer primary"
    );
}

#[tokio::test]
async fn admitted_composition_refuses_unbound_and_replaced_sign_in_without_requests() {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    let home = tempfile::tempdir().unwrap();
    let primary = primary(home.path());
    let info = info(&server);
    let session = seed(&info, &primary, Some("origin"));
    let manager = Arc::new(GatewayAuthManager(GatewayCredentialSource::OAuth(session)));
    let ordinary = composed(&info, manager.clone()).await;
    let admission = ordinary.gateway_admission;
    assert_eq!(manager.observed_admission(), admission);
    let source = Ok(manager);
    let resolve = |admission| {
        crate::combined_auth::compose_auth_for(
            &info,
            Some(&source),
            ResolvedProviderAuth::new(auth_provider_from_auth(&CodexAuth::from_api_key("primary"))),
            Some(admission),
        )
    };
    assert!(resolve(GatewayAdmission::Unbound).await.is_err());
    let admitted = resolve(admission.clone()).await.unwrap();
    let headers = admitted
        .auth
        .apply_auth_headers(http::HeaderMap::new())
        .await
        .unwrap();
    assert_eq!(headers.get("x-gateway-auth").unwrap(), "Bearer gateway");
    seed(&info, &primary, Some("replacement"));
    assert!(resolve(admission).await.is_err());
    server.verify().await;
}
