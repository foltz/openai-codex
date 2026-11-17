use super::*;
use codex_core::config::ConfigBuilder;
use codex_login::AuthManager;
use codex_login::CodexAuth;

#[tokio::test]
async fn observation_opt_in_is_boolean_and_does_not_enable_automatic_login() -> anyhow::Result<()> {
    for (extension, expected) in [
        (serde_json::json!(true), true),
        (serde_json::json!(false), false),
        (serde_json::json!("true"), false),
        (serde_json::Value::Null, false),
    ] {
        let home = tempfile::tempdir()?;
        let (initializer, _outgoing) = build_initializer(home.path()).await?;
        let session = ConnectionSessionState::new(ConnectionOrigin::Stdio);
        assert!(!session.auth_observation_only());
        let params = serde_json::from_value(serde_json::json!({
            "clientInfo": {"name": "observer-port-test", "version": "1"},
            "capabilities": {"extensions": {"codex/auth-observation": extension}},
        }))?;
        initializer
            .initialize(
                ConnectionId(77),
                RequestId::Integer(1),
                params,
                &session,
                None,
            )
            .await
            .map_err(|error| anyhow::anyhow!(error.message))?;
        assert_eq!(session.auth_observation_only(), expected);
        assert_eq!(
            initializer.gateway_login_initialized.get().is_some(),
            !expected,
            "observer initialization must not consume the first originating-client login decision"
        );
    }
    Ok(())
}

#[tokio::test]
async fn observation_retains_explicit_gateway_oauth_opt_in() -> anyhow::Result<()> {
    let home = tempfile::tempdir()?;
    let (initializer, _outgoing) = build_initializer(home.path()).await?;
    let session = ConnectionSessionState::new(ConnectionOrigin::Stdio);
    let params = serde_json::from_value(serde_json::json!({
        "clientInfo": {"name": "observer-port-test", "version": "1"},
        "capabilities": {"explicitGatewayOauth": true,
            "extensions": {"codex/auth-observation": true}},
    }))?;
    initializer
        .initialize(
            ConnectionId(78),
            RequestId::Integer(1),
            params,
            &session,
            None,
        )
        .await
        .map_err(|error| anyhow::anyhow!(error.message))?;
    assert!(session.auth_observation_only());
    assert!(
        initializer.gateway_login_initialized.get().is_some(),
        "explicit login remains an explicit sticky caller choice, not automatic authorization"
    );
    Ok(())
}

async fn build_initializer(
    home: &std::path::Path,
) -> anyhow::Result<(
    InitializeRequestProcessor,
    tokio::sync::mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
)> {
    let config = Arc::new(
        ConfigBuilder::default()
            .codex_home(home.to_path_buf())
            .loader_overrides(codex_config::LoaderOverrides::without_managed_config_for_tests())
            .build()
            .await?,
    );
    let auth = AuthManager::from_auth_for_testing_with_home(
        CodexAuth::from_api_key("synthetic-observer-key"),
        home.to_path_buf(),
    );
    let analytics = AnalyticsEventsClient::disabled();
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    let outgoing = Arc::new(OutgoingMessageSender::new(tx, analytics.clone()));
    Ok((
        InitializeRequestProcessor::new(
            codex_login::GatewayLoginControl::for_runtime(&auth.runtime_config()),
            outgoing,
            analytics,
            config,
            AppServerRpcTransport::Stdio,
            Arc::new(crate::user_verification::Service::new(auth)),
        ),
        rx,
    ))
}
