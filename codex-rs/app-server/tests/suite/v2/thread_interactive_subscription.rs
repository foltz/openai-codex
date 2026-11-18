//! Public experimental gate, untrusted-role negative and embedded entitlement witnesses.
//! Restored from 360b7705 on the current APIs; Unix peer helper coverage is separate.
//! Public trusted interactive subscription contract and entitlement coverage.
use anyhow::Result;
use app_test_support::DEFAULT_CLIENT_NAME;
use app_test_support::TestAppServer;
use codex_app_server::in_process;
use codex_app_server::in_process::InProcessStartArgs;
use codex_app_server_protocol::ClientInfo;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::InitializeCapabilities;
use codex_app_server_protocol::InitializeParams;
use codex_app_server_protocol::JSONRPCMessage;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::SessionSource;
use codex_app_server_protocol::ThreadInteractiveSubscriptionListParams;
use codex_app_server_protocol::ThreadInteractiveSubscriptionListResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_arg0::Arg0DispatchPaths;
use codex_config::CloudConfigBundleLoader;
use codex_config::LoaderOverrides;
use codex_core::config::ConfigBuilder;
use codex_exec_server::EnvironmentManager;
use codex_feedback::CodexFeedback;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::timeout;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test]
async fn interactive_subscription_list_requires_experimental_opt_in() -> Result<()> {
    let mut server = TestAppServer::builder()
        .without_auto_env()
        .with_args(&["-c", "features.plugins=false"])
        .build()
        .await?;
    let initialized = server
        .initialize_with_capabilities(
            ClientInfo {
                name: DEFAULT_CLIENT_NAME.to_string(),
                title: None,
                version: "0.1.0".to_string(),
            },
            Some(InitializeCapabilities::default()),
        )
        .await?;
    assert!(matches!(initialized, JSONRPCMessage::Response(_)));
    let id = server
        .send_raw_request(
            "kcf/thread/interactiveSubscription/list",
            Some(serde_json::json!({})),
        )
        .await?;
    let error = timeout(
        DEFAULT_TIMEOUT,
        server.read_stream_until_error_message(RequestId::Integer(id)),
    )
    .await??;
    assert_eq!(
        error.error,
        codex_app_server_protocol::JSONRPCErrorError {
            code: -32600,
            message: "kcf/thread/interactiveSubscription/list requires experimentalApi capability"
                .to_string(),
            data: None,
        }
    );
    Ok(())
}

#[tokio::test]
async fn thread_interactive_subscription_list_is_empty_without_server_entitlement() -> Result<()> {
    let mut app_server = TestAppServer::builder()
        .without_auto_env()
        .with_args(&["-c", "features.plugins=false"])
        .build()
        .await?;
    let initialized = app_server
        .initialize_with_capabilities(
            ClientInfo {
                name: DEFAULT_CLIENT_NAME.to_string(),
                title: None,
                version: "0.1.0".to_string(),
            },
            Some(InitializeCapabilities {
                experimental_api: true,
                interactive_client: true,
                ..Default::default()
            }),
        )
        .await?;
    assert!(matches!(initialized, JSONRPCMessage::Response(_)));

    // Populate an actual subscription: an empty server would make this
    // negative pass even if role self-assertion accidentally conferred authority.
    let started_id = app_server
        .send_raw_request("thread/start", Some(serde_json::json!({"ephemeral": true})))
        .await?;
    let started: ThreadStartResponse =
        timeout(DEFAULT_TIMEOUT, app_server.read_response(started_id)).await??;
    assert!(!started.thread.id.is_empty());

    let request_id = app_server
        .send_raw_request(
            "kcf/thread/interactiveSubscription/list",
            Some(serde_json::json!({})),
        )
        .await?;
    let response: ThreadInteractiveSubscriptionListResponse =
        timeout(DEFAULT_TIMEOUT, app_server.read_response(request_id)).await??;

    assert!(!response.generation.is_empty());
    assert_eq!(response.revision, 0);
    assert!(response.entries.is_empty());
    Ok(())
}

#[tokio::test]
async fn embedded_interactive_client_populates_public_subscription_snapshot() -> Result<()> {
    let codex_home = TempDir::new()?;
    let loader_overrides = LoaderOverrides::without_managed_config_for_tests();
    let cli_overrides = vec![("features.plugins".to_string(), toml::Value::Boolean(false))];
    let config = Arc::new(
        ConfigBuilder::default()
            .codex_home(codex_home.path().to_path_buf())
            .fallback_cwd(Some(codex_home.path().to_path_buf()))
            .loader_overrides(loader_overrides.clone())
            .cli_overrides(cli_overrides.clone())
            .build()
            .await?,
    );
    let environment_manager = Arc::new(EnvironmentManager::without_environments(
        config.http_client_factory(),
    ));
    let client = in_process::start(InProcessStartArgs {
        arg0_paths: Arg0DispatchPaths::default(),
        config,
        cli_overrides,
        loader_overrides,
        strict_config: false,
        cloud_config_bundle: CloudConfigBundleLoader::default(),
        embedded_network_policy: Default::default(),
        thread_config_loader: Arc::new(codex_config::NoopThreadConfigLoader),
        feedback: CodexFeedback::new(),
        log_db: None,
        state_db: None,
        environment_manager,
        config_warnings: Vec::new(),
        session_source: SessionSource::Cli.into(),
        enable_codex_api_key_env: false,
        initialize: InitializeParams {
            client_info: ClientInfo {
                name: DEFAULT_CLIENT_NAME.to_string(),
                title: None,
                version: "0.1.0".to_string(),
            },
            capabilities: Some(InitializeCapabilities {
                experimental_api: true,
                interactive_client: true,
                ..Default::default()
            }),
        },
        channel_capacity: in_process::DEFAULT_IN_PROCESS_CHANNEL_CAPACITY,
    })
    .await?;

    let started = client
        .request(ClientRequest::ThreadStart {
            request_id: RequestId::Integer(1),
            params: ThreadStartParams {
                ephemeral: Some(true),
                ..ThreadStartParams::default()
            },
        })
        .await?
        .map_err(|err| anyhow::anyhow!("thread/start should succeed: {err:?}"))?;
    let started: ThreadStartResponse = serde_json::from_value(started)?;
    let snapshot = client
        .request(ClientRequest::ThreadInteractiveSubscriptionList {
            request_id: RequestId::Integer(2),
            params: ThreadInteractiveSubscriptionListParams::default(),
        })
        .await?
        .map_err(|err| {
            anyhow::anyhow!("kcf/thread/interactiveSubscription/list should succeed: {err:?}")
        })?;
    assert_eq!(
        snapshot["entries"],
        serde_json::json!([{
            "threadId": started.thread.id,
            "interactiveSubscriptionCount": 1,
        }]),
    );
    let snapshot: ThreadInteractiveSubscriptionListResponse = serde_json::from_value(snapshot)?;
    assert_eq!(snapshot.revision, 1);
    assert_eq!(snapshot.entries.len(), 1);
    assert_eq!(snapshot.entries[0].thread_id, started.thread.id);
    assert_eq!(snapshot.entries[0].interactive_subscription_count, 1);

    client.shutdown().await?;
    Ok(())
}
