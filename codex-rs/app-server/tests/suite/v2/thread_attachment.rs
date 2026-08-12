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
use codex_app_server_protocol::ThreadAttachmentListParams;
use codex_app_server_protocol::ThreadAttachmentListResponse;
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

#[cfg(unix)]
use std::io::Read;
#[cfg(unix)]
use std::process::Command;
#[cfg(unix)]
use std::process::Stdio;
#[cfg(unix)]
use tokio::time::sleep;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test]
async fn thread_attachment_list_is_empty_without_server_entitlement() -> Result<()> {
    let mut app_server = TestAppServer::builder().without_auto_env().build().await?;
    let initialized = app_server
        .initialize_with_capabilities(
            ClientInfo {
                name: DEFAULT_CLIENT_NAME.to_string(),
                title: None,
                version: "0.1.0".to_string(),
            },
            Some(InitializeCapabilities {
                experimental_api: true,
                request_attestation: false,
                interactive_client: true,
                opt_out_notification_methods: None,
                mcp_server_openai_form_elicitation: false,
                extensions: None,
            }),
        )
        .await?;
    assert!(matches!(initialized, JSONRPCMessage::Response(_)));

    let request_id = app_server
        .send_thread_attachment_list_request(ThreadAttachmentListParams::default())
        .await?;
    let response: ThreadAttachmentListResponse =
        timeout(DEFAULT_TIMEOUT, app_server.read_response(request_id)).await??;

    assert!(!response.generation.is_empty());
    assert_eq!(response.revision, 0);
    assert!(response.entries.is_empty());
    Ok(())
}

#[tokio::test]
async fn embedded_interactive_client_populates_public_attachment_snapshot() -> Result<()> {
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
        .request(ClientRequest::ThreadAttachmentList {
            request_id: RequestId::Integer(2),
            params: ThreadAttachmentListParams::default(),
        })
        .await?
        .map_err(|err| anyhow::anyhow!("thread/attachment/list should succeed: {err:?}"))?;
    let snapshot: ThreadAttachmentListResponse = serde_json::from_value(snapshot)?;
    assert_eq!(snapshot.revision, 1);
    assert_eq!(snapshot.entries.len(), 1);
    assert_eq!(snapshot.entries[0].thread_id, started.thread.id);
    assert_eq!(snapshot.entries[0].interactive_attachment_count, 1);

    client.shutdown().await?;
    Ok(())
}

/// Exercises the production Unix acceptor, session provenance, interactive
/// role request, exact subscription, and public aggregate in one process
/// boundary. The helper is the server binary itself; a copied binary proves a
/// role request alone remains unentitled.
#[cfg(unix)]
#[tokio::test]
async fn unix_peer_entitlement_reaches_public_attachment_aggregate() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let socket_path = temp_dir.path().join("app-server.sock");
    let codex_home = temp_dir.path().join("codex-home");
    std::fs::create_dir_all(&codex_home)?;
    let binary = codex_utils_cargo_bin::cargo_bin("codex-app-server")?;
    let mut server = Command::new(&binary)
        .args([
            "--listen",
            &format!("unix://{}", socket_path.display()),
            "--disable-plugin-startup-tasks-for-tests",
        ])
        .env("CODEX_HOME", &codex_home)
        .stderr(Stdio::piped())
        .spawn()?;
    let server_stderr = server.stderr.take();
    let mut listener_ready = false;
    for _ in 0..400 {
        if tokio::net::UnixStream::connect(&socket_path).await.is_ok() {
            listener_ready = true;
            break;
        }
        sleep(Duration::from_millis(25)).await;
    }
    if !listener_ready {
        server.kill()?;
        let output = server.wait()?;
        let mut stderr = String::new();
        if let Some(mut reader) = server_stderr {
            let _ = reader.read_to_string(&mut stderr);
        }
        anyhow::bail!("Unix app-server listener did not accept connections: {output}; {stderr}");
    }

    let helper_binary = binary.clone();
    let helper_socket_path = socket_path.clone();
    let helper_codex_home = codex_home.clone();
    let entitled = tokio::task::spawn_blocking(move || {
        Command::new(helper_binary)
            .args([
                "--interactive-attachment-peer-helper",
                helper_socket_path.to_str().expect("socket path utf8"),
            ])
            .env("CODEX_HOME", helper_codex_home)
            .output()
    })
    .await??;
    assert!(
        entitled.status.success(),
        "same-image helper should be entitled: {}",
        String::from_utf8_lossy(&entitled.stderr)
    );

    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        use std::os::unix::fs::PermissionsExt;

        let copied_binary = temp_dir.path().join("unentitled-codex-app-server");
        std::fs::copy(&binary, &copied_binary)?;
        std::fs::set_permissions(&copied_binary, std::fs::Permissions::from_mode(0o700))?;
        let unentitled = Command::new(copied_binary)
            .args([
                "--interactive-attachment-peer-helper",
                socket_path.to_str().expect("socket path utf8"),
                "--interactive-attachment-peer-helper-expect-unproven",
            ])
            .env("CODEX_HOME", &codex_home)
            .status()
            .await?;
        assert!(
            unentitled.success(),
            "different-image helper must remain absent from the aggregate"
        );
    }
    server.kill()?;
    let _ = server.wait()?;
    Ok(())
}
