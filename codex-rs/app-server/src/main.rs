#![recursion_limit = "256"]

use clap::Parser;
use codex_app_server::AppServerCodeModeHostArgs;
use codex_app_server::AppServerRuntimeOptions;
use codex_app_server::AppServerTransport;
use codex_app_server::PluginStartupTasks;
use codex_app_server::run_main_with_transport_options;
use codex_arg0::Arg0DispatchPaths;
use codex_arg0::arg0_dispatch_or_else;
use codex_config::LoaderOverrides;
use codex_protocol::protocol::SessionSource;
use codex_utils_cli::CliConfigOverrides;
use codex_websocket_auth::WebsocketAuthArgs;
use std::path::PathBuf;

#[cfg(all(
    target_os = "linux",
    target_env = "musl",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
#[cfg(all(debug_assertions, unix))]
use futures::SinkExt;
#[cfg(all(debug_assertions, unix))]
use futures::StreamExt;

// Debug-only test hook: lets integration tests point the server at a temporary
// managed config file without writing to /etc.
const MANAGED_CONFIG_PATH_ENV_VAR: &str = "CODEX_APP_SERVER_MANAGED_CONFIG_PATH";
const DISABLE_MANAGED_CONFIG_ENV_VAR: &str = "CODEX_APP_SERVER_DISABLE_MANAGED_CONFIG";

#[derive(Debug, Parser)]
#[command(version)]
struct AppServerArgs {
    #[command(flatten)]
    config_overrides: CliConfigOverrides,

    #[command(flatten)]
    code_mode_host: AppServerCodeModeHostArgs,

    /// Transport endpoint URL. Supported values: `stdio://` (default),
    /// `unix://`, `unix://PATH`, `ws://IP:PORT`, `off`.
    #[arg(
        long = "listen",
        value_name = "URL",
        default_value = AppServerTransport::DEFAULT_LISTEN_URL
    )]
    listen: AppServerTransport,

    /// Session source used to derive product restrictions and metadata.
    #[arg(
        long = "session-source",
        value_name = "SOURCE",
        default_value = "vscode",
        value_parser = SessionSource::from_startup_arg
    )]
    session_source: SessionSource,

    #[command(flatten)]
    auth: WebsocketAuthArgs,

    /// Fail if config.toml contains unknown configuration fields.
    #[arg(long = "strict-config", default_value_t = false)]
    strict_config: bool,

    /// Hidden debug-only test hook used by integration tests that spawn the
    /// production app-server binary.
    #[cfg(debug_assertions)]
    #[arg(long = "disable-plugin-startup-tasks-for-tests", hide = true)]
    disable_plugin_startup_tasks_for_tests: bool,

    /// Hidden helper used only by the Unix-peer entitlement integration test.
    /// It connects through the production protocol as this exact binary.
    #[cfg(all(debug_assertions, unix))]
    #[arg(long = "interactive-subscription-peer-helper", hide = true)]
    interactive_subscription_peer_helper: Option<PathBuf>,

    #[cfg(all(debug_assertions, unix))]
    #[arg(
        long = "interactive-subscription-peer-helper-expect-unproven",
        hide = true
    )]
    interactive_subscription_peer_helper_expect_unproven: bool,

    /// Enable remote control for this app-server process without changing persistence.
    #[arg(long = "remote-control", hide = true)]
    remote_control: bool,

    /// Save loaded threads during managed daemon shutdown.
    #[arg(long, hide = true)]
    managed_daemon: bool,
}

fn main() -> anyhow::Result<()> {
    let remote_control_disabled = codex_app_server::take_remote_control_disabled_env();
    arg0_dispatch_or_else(move |arg0_paths: Arg0DispatchPaths| async move {
        let AppServerArgs {
            config_overrides,
            code_mode_host,
            listen,
            session_source,
            auth,
            strict_config,
            #[cfg(debug_assertions)]
            disable_plugin_startup_tasks_for_tests,
            #[cfg(all(debug_assertions, unix))]
            interactive_subscription_peer_helper,
            #[cfg(all(debug_assertions, unix))]
            interactive_subscription_peer_helper_expect_unproven,
            remote_control,
            managed_daemon,
        } = AppServerArgs::parse();
        #[cfg(all(debug_assertions, unix))]
        if let Some(socket_path) = interactive_subscription_peer_helper {
            return run_interactive_subscription_peer_helper(
                socket_path,
                interactive_subscription_peer_helper_expect_unproven,
            )
            .await;
        }

        let loader_overrides = if disable_managed_config_from_debug_env() {
            LoaderOverrides::without_managed_config_for_tests()
        } else {
            managed_config_path_from_debug_env()
                .map(LoaderOverrides::with_managed_config_path_for_tests)
                .unwrap_or_default()
        };
        let transport = listen;
        let auth = auth.try_into_settings()?;
        let mut runtime_options = AppServerRuntimeOptions {
            code_mode_host_transport: code_mode_host.into(),
            managed_daemon,
            ..Default::default()
        };
        #[cfg(debug_assertions)]
        if disable_plugin_startup_tasks_for_tests {
            runtime_options.plugin_startup_tasks = PluginStartupTasks::Skip;
        }
        runtime_options.remote_control_startup_mode =
            match (remote_control, remote_control_disabled) {
                (true, _) => codex_app_server::RemoteControlStartupMode::EnabledEphemeral,
                (false, true) => codex_app_server::RemoteControlStartupMode::DisabledEphemeral,
                (false, false) => codex_app_server::RemoteControlStartupMode::ResolvePersisted,
            };

        let exit = run_main_with_transport_options(
            arg0_paths,
            config_overrides,
            loader_overrides,
            strict_config,
            /*default_analytics_enabled*/ false,
            transport,
            session_source,
            auth,
            runtime_options,
        )
        .await?;
        if exit == codex_app_server::AppServerExit::Forced {
            // Runtime teardown can wait forever for blocked rollout I/O.
            std::process::exit(0);
        }
        Ok(())
    })
}

#[cfg(all(debug_assertions, unix))]
async fn run_interactive_subscription_peer_helper(
    socket_path: PathBuf,
    expect_unproven: bool,
) -> anyhow::Result<()> {
    use tokio_tungstenite::client_async;
    use tokio_tungstenite::tungstenite::Message;

    let stream = tokio::net::UnixStream::connect(socket_path).await?;
    let (mut websocket, response) = client_async("ws://localhost/rpc", stream).await?;
    anyhow::ensure!(
        response.status() == tokio_tungstenite::tungstenite::http::StatusCode::SWITCHING_PROTOCOLS,
        "peer helper upgrade failed: status={}, headers={:?}",
        response.status(),
        response.headers()
    );

    async fn request(
        websocket: &mut tokio_tungstenite::WebSocketStream<tokio::net::UnixStream>,
        id: i64,
        method: &str,
        params: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        websocket
            .send(Message::Text(
                serde_json::json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params})
                    .to_string()
                    .into(),
            ))
            .await?;
        while let Some(message) = websocket.next().await {
            let Message::Text(text) = message? else {
                continue;
            };
            let value: serde_json::Value = serde_json::from_str(&text)?;
            if value.get("id") == Some(&serde_json::json!(id)) {
                return value
                    .get("result")
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("{method} failed: {value}"));
            }
        }
        anyhow::bail!("connection closed while waiting for {method}")
    }

    let _ = request(
        &mut websocket,
        1,
        "initialize",
        serde_json::json!({
            "clientInfo": {"name": "codex-tui", "version": "test"},
            "capabilities": {"experimentalApi": true, "interactiveClient": true}
        }),
    )
    .await?;
    websocket
        .send(Message::Text(
            serde_json::json!({"jsonrpc":"2.0", "method":"initialized"})
                .to_string()
                .into(),
        ))
        .await?;
    let started = request(
        &mut websocket,
        2,
        "thread/start",
        serde_json::json!({"ephemeral": true}),
    )
    .await?;
    let thread_id = started["thread"]["id"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("thread/start did not return a thread id"))?;
    let snapshot = request(
        &mut websocket,
        3,
        "kcf/thread/interactiveSubscription/list",
        serde_json::json!({}),
    )
    .await?;
    let entries = snapshot["entries"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("subscription list did not return entries"))?;
    let contains_thread = entries
        .iter()
        .any(|entry| entry["threadId"].as_str() == Some(thread_id));
    anyhow::ensure!(
        contains_thread != expect_unproven,
        "unexpected subscription entitlement: expect_unproven={expect_unproven}, entries={entries:?}"
    );
    Ok(())
}

fn disable_managed_config_from_debug_env() -> bool {
    #[cfg(debug_assertions)]
    {
        if let Ok(value) = std::env::var(DISABLE_MANAGED_CONFIG_ENV_VAR) {
            return matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES");
        }
    }

    false
}

fn managed_config_path_from_debug_env() -> Option<PathBuf> {
    #[cfg(debug_assertions)]
    {
        if let Ok(value) = std::env::var(MANAGED_CONFIG_PATH_ENV_VAR) {
            return if value.is_empty() {
                None
            } else {
                Some(PathBuf::from(value))
            };
        }
    }

    None
}

#[cfg(test)]
#[path = "main_tests.rs"]
mod tests;
