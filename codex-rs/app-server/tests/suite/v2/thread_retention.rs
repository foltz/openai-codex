use anyhow::Result;
use app_test_support::DEFAULT_CLIENT_NAME;
use codex_app_server::in_process;
use codex_app_server::in_process::InProcessStartArgs;
use codex_app_server_protocol::ClientInfo;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::InitializeCapabilities;
use codex_app_server_protocol::InitializeParams;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::SessionSource;
use codex_app_server_protocol::ThreadRetentionAcquireParams;
use codex_app_server_protocol::ThreadRetentionAcquireResponse;
use codex_app_server_protocol::ThreadRetentionReleaseParams;
use codex_app_server_protocol::ThreadRetentionReleaseResponse;
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
use tempfile::TempDir;

async fn start_in_process_client(
    experimental_api: bool,
) -> Result<in_process::InProcessClientHandle> {
    let codex_home = TempDir::new()?;
    let loader_overrides = LoaderOverrides::without_managed_config_for_tests();
    let config = Arc::new(
        ConfigBuilder::default()
            .codex_home(codex_home.path().to_path_buf())
            .fallback_cwd(Some(codex_home.path().to_path_buf()))
            .loader_overrides(loader_overrides.clone())
            .build()
            .await?,
    );
    let client = in_process::start(InProcessStartArgs {
        arg0_paths: Arg0DispatchPaths::default(),
        config,
        cli_overrides: Vec::new(),
        loader_overrides,
        strict_config: false,
        cloud_config_bundle: CloudConfigBundleLoader::default(),
        thread_config_loader: Arc::new(codex_config::NoopThreadConfigLoader),
        feedback: CodexFeedback::new(),
        log_db: None,
        state_db: None,
        environment_manager: Arc::new(EnvironmentManager::default_for_tests()),
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
                experimental_api,
                ..Default::default()
            }),
        },
        channel_capacity: in_process::DEFAULT_IN_PROCESS_CHANNEL_CAPACITY,
    })
    .await?;
    Ok(client)
}

#[tokio::test]
async fn retention_carrier_is_exact_and_uses_opaque_idempotent_handles() -> Result<()> {
    let client = start_in_process_client(true).await?;

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

    let acquire = |request_id| ClientRequest::ThreadRetentionAcquire {
        request_id: RequestId::Integer(request_id),
        params: ThreadRetentionAcquireParams {
            thread_id: started.thread.id.clone(),
        },
    };
    let first: ThreadRetentionAcquireResponse = serde_json::from_value(
        client
            .request(acquire(2))
            .await?
            .map_err(|err| anyhow::anyhow!("retention acquire should succeed: {err:?}"))?,
    )?;
    let ThreadRetentionAcquireResponse::Acquired { grant_id } = first else {
        anyhow::bail!("first retention acquire must mint a grant");
    };
    let second: ThreadRetentionAcquireResponse = serde_json::from_value(
        client
            .request(acquire(3))
            .await?
            .map_err(|err| anyhow::anyhow!("repeat retention acquire should succeed: {err:?}"))?,
    )?;
    assert_eq!(
        second,
        ThreadRetentionAcquireResponse::AlreadyHeld {
            grant_id: grant_id.clone()
        }
    );

    let mismatched: ThreadRetentionReleaseResponse = serde_json::from_value(
        client
            .request(ClientRequest::ThreadRetentionRelease {
                request_id: RequestId::Integer(4),
                params: ThreadRetentionReleaseParams {
                    thread_id: started.thread.id.clone(),
                    grant_id: "different-active-grant".to_string(),
                },
            })
            .await?
            .map_err(|err| anyhow::anyhow!("mismatched release should respond: {err:?}"))?,
    )?;
    assert_eq!(mismatched, ThreadRetentionReleaseResponse::GrantMismatch);

    let release = |request_id, grant_id: String| ClientRequest::ThreadRetentionRelease {
        request_id: RequestId::Integer(request_id),
        params: ThreadRetentionReleaseParams {
            thread_id: started.thread.id.clone(),
            grant_id,
        },
    };
    let released: ThreadRetentionReleaseResponse = serde_json::from_value(
        client
            .request(release(5, grant_id.clone()))
            .await?
            .map_err(|err| anyhow::anyhow!("retention release should succeed: {err:?}"))?,
    )?;
    assert_eq!(released, ThreadRetentionReleaseResponse::Released);
    let spent: ThreadRetentionReleaseResponse = serde_json::from_value(
        client
            .request(release(6, grant_id))
            .await?
            .map_err(|err| anyhow::anyhow!("spent retention release should respond: {err:?}"))?,
    )?;
    let unknown: ThreadRetentionReleaseResponse = serde_json::from_value(
        client
            .request(release(7, "never-issued".to_string()))
            .await?
            .map_err(|err| anyhow::anyhow!("unknown retention release should respond: {err:?}"))?,
    )?;
    assert_eq!(spent, ThreadRetentionReleaseResponse::NotHeld);
    assert_eq!(unknown, ThreadRetentionReleaseResponse::NotHeld);

    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn retention_requires_experimental_opt_in() -> Result<()> {
    let client = start_in_process_client(false).await?;
    let error = client
        .request(ClientRequest::ThreadRetentionAcquire {
            request_id: RequestId::Integer(1),
            params: ThreadRetentionAcquireParams {
                thread_id: "not-reached-without-experimental-opt-in".to_string(),
            },
        })
        .await?
        .expect_err("experimental request must be rejected before handler dispatch");
    assert_eq!(error.code, -32600);
    assert!(error.message.contains("experimental"));
    client.shutdown().await?;
    Ok(())
}
