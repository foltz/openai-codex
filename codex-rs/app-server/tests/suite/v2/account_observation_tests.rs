//! Public cache observation, with native remote-control work qualified separately.
use super::*;
use pretty_assertions::assert_eq;
use std::collections::HashMap;

enum RemoteControlFixture {
    Allowed,
    DisabledByRequirements,
}

async fn managed_observation_server(
    remote_control: RemoteControlFixture,
) -> Result<(TempDir, MockServer, TestAppServer)> {
    let home = TempDir::new()?;
    create_config_toml(home.path(), CreateConfigTomlParams::default())?;
    if matches!(remote_control, RemoteControlFixture::DisabledByRequirements) {
        std::fs::write(
            home.path().join("requirements.toml"),
            "allow_remote_control = false\n",
        )?;
    }
    write_chatgpt_auth(
        home.path(),
        ChatGptAuthFixture::new("cached-access")
            .account_id("account")
            .claims(
                ChatGptIdTokenClaims::new()
                    .chatgpt_user_id("principal")
                    .chatgpt_account_id("account"),
            )
            .last_refresh(Some(Utc::now() - ChronoDuration::days(20))),
        AuthCredentialsStoreMode::File,
    )?;
    let issuer = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/oauth/token"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&issuer)
        .await;
    let endpoint = format!("{}/oauth/token", issuer.uri());
    let mut server = TestAppServer::builder()
        .with_codex_home(home.path())
        .with_env_overrides(&[
            ("OPENAI_API_KEY", None),
            (REFRESH_TOKEN_URL_OVERRIDE_ENV_VAR, Some(endpoint.as_str())),
        ])
        .build()
        .await?;
    // Fence synchronous construction; initialization-triggered remote control
    // is a separate native path, tested below rather than disabled in production.
    let barrier = server
        .send_get_account_request(GetAccountParams {
            cache_only: true,
            refresh_token: false,
        })
        .await?;
    let error = server
        .read_stream_until_error_message(RequestId::Integer(barrier))
        .await?;
    assert_eq!(error.error.message, "Not initialized");
    Ok((home, issuer, server))
}

async fn observed_oauth_refreshes(issuer: &MockServer) -> usize {
    let requests = issuer.received_requests().await.unwrap_or_default();
    for (index, request) in requests.iter().enumerate() {
        let body = serde_json::from_slice::<serde_json::Value>(&request.body).ok();
        eprintln!(
            "issuer request {index}: {} {} grant_type={:?}",
            request.method,
            request.url.path(),
            body.as_ref()
                .and_then(|value| value.get("grant_type"))
                .and_then(serde_json::Value::as_str)
        );
    }
    requests
        .iter()
        .filter(|request| request.method.as_str() == "POST" && request.url.path() == "/oauth/token")
        .count()
}

#[tokio::test]
async fn managed_cache_observation_does_not_renew_or_export() -> Result<()> {
    let (home, issuer, mut server) =
        managed_observation_server(RemoteControlFixture::DisabledByRequirements).await?;
    let before = std::fs::read(home.path().join("auth.json"))?;
    let before_calls = observed_oauth_refreshes(&issuer).await;
    server
        .initialize_with_capabilities(
            ClientInfo {
                name: "cache-observer".into(),
                title: None,
                version: "1".into(),
            },
            Some(InitializeCapabilities {
                extensions: Some(HashMap::from([(
                    "codex/auth-observation".into(),
                    json!(true),
                )])),
                ..Default::default()
            }),
        )
        .await?;
    let id = server
        .send_get_account_request(GetAccountParams {
            cache_only: true,
            refresh_token: false,
        })
        .await?;
    let response: GetAccountResponse = server.read_response(id).await?;
    let observed = response
        .auth_observation
        .expect("explicit cache observation");
    assert_eq!(observed.mode, Some(AuthMode::Chatgpt));
    assert_eq!(observed.account_id, Some("account".into()));
    assert_eq!(observed.principal, Some("principal".into()));
    assert!(observed.state_known);
    assert!(observed.provider.is_none());
    assert_eq!(observed_oauth_refreshes(&issuer).await, before_calls);
    assert_eq!(std::fs::read(home.path().join("auth.json"))?, before);
    Ok(())
}

#[tokio::test]
async fn stdio_plain_initialize_can_trigger_native_remote_control_auth_resolution() -> Result<()> {
    let (_home, issuer, mut server) =
        managed_observation_server(RemoteControlFixture::Allowed).await?;
    let before_calls = observed_oauth_refreshes(&issuer).await;
    // No observation extension and no account/read request: the stdio transport
    // hands the first raw client name to native remote-control preference lookup.
    server
        .initialize_with_capabilities(
            ClientInfo {
                name: "remote-control-startup-control".into(),
                title: None,
                version: "1".into(),
            },
            None,
        )
        .await?;
    timeout(Duration::from_secs(5), async {
        loop {
            if observed_oauth_refreshes(&issuer).await > before_calls {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert_eq!(observed_oauth_refreshes(&issuer).await, before_calls + 1);
    Ok(())
}
