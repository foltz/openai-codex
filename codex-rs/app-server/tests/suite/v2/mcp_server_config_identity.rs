use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use codex_app_server_protocol::ConfigBatchWriteParams;
use codex_app_server_protocol::ConfigEdit;
use codex_app_server_protocol::McpServerConfigIdentityResponse;
use codex_app_server_protocol::MergeStrategy;
use codex_app_server_protocol::RequestId;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use tokio::time::timeout;

const DEFAULT_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[tokio::test]
async fn mcp_server_config_identity_distinguishes_applied_and_current_config() -> Result<()> {
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new("http://localhost")
        .with_extra_config("[mcp_servers.initial]\ncommand = \"initial\"")
        .write(codex_home.path())?;
    let config_path = codex_home.path().join("config.toml");

    let mut app_server = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized_with_timeout(DEFAULT_READ_TIMEOUT)
        .await?;

    let startup = read_identity(&mut app_server).await?;
    assert_eq!(startup.applied, startup.current);
    assert_eq!(
        startup.applied.layers[0].file_path,
        std::fs::canonicalize(&config_path)?.display().to_string()
    );
    assert!(startup.applied.layers[0].version.starts_with("sha256:"));

    let changed_config = std::fs::read_to_string(&config_path)?
        .replace("command = \"initial\"", "command = \"changed\"");
    std::fs::write(&config_path, changed_config)?;

    let changed = read_identity(&mut app_server).await?;
    assert_eq!(changed.applied, startup.applied);
    assert_ne!(changed.current, changed.applied);

    reload_mcp_config(&mut app_server).await?;

    let reloaded = read_identity(&mut app_server).await?;
    assert_eq!(reloaded.applied, changed.current);
    assert_eq!(reloaded.applied, reloaded.current);

    Ok(())
}

#[tokio::test]
async fn mcp_server_config_identity_supports_missing_selected_config() -> Result<()> {
    let codex_home = TempDir::new()?;

    let mut app_server = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized_with_timeout(DEFAULT_READ_TIMEOUT)
        .await?;

    let identity = read_identity(&mut app_server).await?;
    assert_eq!(identity.applied, identity.current);
    assert_eq!(identity.applied.layers.len(), 1);
    assert!(
        identity.applied.layers[0]
            .file_path
            .ends_with("/config.toml")
    );

    reload_mcp_config(&mut app_server).await?;
    let reloaded = read_identity(&mut app_server).await?;
    assert_eq!(reloaded.applied, reloaded.current);
    assert_eq!(reloaded.applied, identity.applied);
    Ok(())
}

#[tokio::test]
async fn batch_write_runtime_reload_advances_applied_mcp_identity() -> Result<()> {
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new("http://localhost")
        .with_extra_config("[mcp_servers.initial]\ncommand = \"initial\"")
        .write(codex_home.path())?;
    let config_path = codex_home.path().join("config.toml");
    let mut app_server = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized_with_timeout(DEFAULT_READ_TIMEOUT)
        .await?;
    let before = read_identity(&mut app_server).await?;

    let request_id = app_server
        .send_config_batch_write_request(ConfigBatchWriteParams {
            file_path: Some(config_path.display().to_string()),
            edits: vec![ConfigEdit {
                key_path: "mcp_servers.initial.command".to_string(),
                value: serde_json::json!("changed"),
                merge_strategy: MergeStrategy::Replace,
            }],
            expected_version: None,
            reload_user_config: true,
        })
        .await?;
    let _: serde_json::Value =
        timeout(DEFAULT_READ_TIMEOUT, app_server.read_response(request_id)).await??;

    let after = read_identity(&mut app_server).await?;
    assert_ne!(after.applied, before.applied);
    assert_eq!(after.applied, after.current);
    Ok(())
}

#[tokio::test]
async fn mcp_server_config_identity_retains_applied_identity_after_failed_reload() -> Result<()> {
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new("http://localhost")
        .with_extra_config("[mcp_servers.initial]\ncommand = \"initial\"")
        .write(codex_home.path())?;
    let config_path = codex_home.path().join("config.toml");

    let mut app_server = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized_with_timeout(DEFAULT_READ_TIMEOUT)
        .await?;
    let before_failure = read_identity(&mut app_server).await?;

    std::fs::write(&config_path, "[mcp_servers.invalid\n")?;
    let request_id = app_server
        .send_raw_request("config/mcpServer/reload", /*params*/ None)
        .await?;
    let error = timeout(
        DEFAULT_READ_TIMEOUT,
        app_server.read_stream_until_error_message(RequestId::Integer(request_id)),
    )
    .await??;
    assert_eq!(error.error.code, -32603);

    MockResponsesConfig::new("http://localhost")
        .with_extra_config("[mcp_servers.initial]\ncommand = \"initial\"")
        .write(codex_home.path())?;
    let after_failure = read_identity(&mut app_server).await?;
    assert_eq!(after_failure.applied, before_failure.applied);
    assert_eq!(after_failure.current, before_failure.current);

    Ok(())
}

async fn read_identity(app_server: &mut TestAppServer) -> Result<McpServerConfigIdentityResponse> {
    let request_id = app_server
        .send_raw_request("config/mcpServer/identity", /*params*/ None)
        .await?;
    timeout(DEFAULT_READ_TIMEOUT, app_server.read_response(request_id)).await?
}

async fn reload_mcp_config(app_server: &mut TestAppServer) -> Result<()> {
    let request_id = app_server
        .send_raw_request("config/mcpServer/reload", /*params*/ None)
        .await?;
    let _: serde_json::Value =
        timeout(DEFAULT_READ_TIMEOUT, app_server.read_response(request_id)).await??;
    Ok(())
}
