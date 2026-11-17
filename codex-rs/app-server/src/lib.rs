#![recursion_limit = "256"]
#![deny(clippy::print_stdout, clippy::print_stderr)]

use crate::standalone_shutdown::{STANDALONE_STAGE_CAPS, StandaloneShutdownOwner};
use codex_arg0::Arg0DispatchPaths;
use codex_code_mode::CodeModeSessionProvider;
use codex_code_mode::GrpcCodeModeSessionProvider;
use codex_config::LoaderOverrides;
use codex_config::NoopThreadConfigLoader;
use codex_core::config::Config;
use codex_core::config::UnsupportedUntrustedApprovalPolicyError;
use codex_core::resolve_installation_id;
use codex_login::AuthManager;
#[cfg(debug_assertions)]
use codex_utils_absolute_path::AbsolutePathBuf;
use codex_utils_cli::CliConfigOverrides;
use futures::FutureExt;
use futures::future::BoxFuture;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::ErrorKind;
use std::io::Result as IoResult;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::AtomicBool;

use crate::analytics_utils::analytics_events_client_from_config;
use crate::config_manager::ConfigManager;
use crate::connection_cleanup::ConnectionCleanupTasks;
use crate::message_processor::MessageProcessor;
use crate::message_processor::MessageProcessorArgs;
use crate::outgoing_message::ConnectionId;
use crate::outgoing_message::OutgoingEnvelope;
use crate::outgoing_message::OutgoingMessageSender;
use crate::outgoing_message::QueuedOutgoingMessage;
use crate::plugin_config_reload::PluginStartupConfig;
use crate::transport::CHANNEL_CAPACITY;
use crate::transport::ConnectionOrigin;
use crate::transport::ConnectionState;
use crate::transport::DaemonShutdownAccess;
use crate::transport::OutboundConnectionState;
use crate::transport::RemoteControlPolicy;
use crate::transport::RemoteControlStartConfig;
use crate::transport::TransportEvent;
use crate::transport::acquire_app_server_startup_lock;
use crate::transport::app_server_startup_lock_path;
use crate::transport::route_outgoing_envelope;
use crate::transport::start_control_socket_acceptor;
use crate::transport::start_remote_control;
use crate::transport::start_stdio_connection;
use crate::transport::start_websocket_acceptor;
use codex_analytics::AppServerRpcTransport;
use codex_app_server_protocol::ConfigWarningNotification;
use codex_app_server_protocol::JSONRPCMessage;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::TextPosition as AppTextPosition;
use codex_app_server_protocol::TextRange as AppTextRange;
use codex_app_server_transport::WebSocketCompression;
use codex_app_server_transport::daemon_recovery_file_path;
use codex_config::ConfigLayerSource;
use codex_config::ConfigLoadError;
use codex_config::TextRange as CoreTextRange;
use codex_core::ExecPolicyError;
use codex_core::check_execpolicy_for_warnings;
use codex_core::config::find_codex_home;
use codex_exec_server::EnvironmentManager;
use codex_exec_server::ExecServerRuntimeOptions;
use codex_features::Feature;
use codex_feedback::CodexFeedback;
use codex_protocol::protocol::SessionSource;
use codex_rollout::state_db as rollout_state_db;
use codex_state::log_db;
use codex_websocket_auth::WebsocketAuthSettings;
use codex_websocket_auth::policy_from_settings;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::error;
use tracing::info;
use tracing::warn;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::Registry;
use tracing_subscriber::util::SubscriberInitExt;

const SQLITE_RECOVERY_CONFIG_WARNING_SUMMARY: &str = "Codex rebuilt its local database";

fn is_unsupported_untrusted_approval_policy_error(err: &std::io::Error) -> bool {
    err.get_ref().is_some_and(
        <dyn std::error::Error + Send + Sync + 'static>::is::<
            UnsupportedUntrustedApprovalPolicyError,
        >,
    )
}

pub use codex_agent_message_board_host::delete_agent_message_boards;
pub use codex_agent_message_board_host::install_agent_message_board;

mod account_dependency;
mod account_operation_work;
mod account_turn_admission;
mod account_turn_work;
mod account_work_permits;
mod analytics_utils;
mod app_info;
mod app_server_tracing;
mod attestation;
mod auth_mode;
mod auth_task_work;
mod bespoke_event_handling;
mod code_mode_host;
mod codex_home_metrics;
mod command_exec;
mod config_layer;
#[cfg(test)]
mod core_auth_task_tests;
mod managed_reset;
mod managed_transition;
mod otel_reset_control;
mod startup_config;
pub use startup_config::StartupConfigArgs;
pub use startup_config::print_startup_config;

mod config_manager;
mod config_manager_service;
mod connection_cleanup;
mod connection_rpc_gate;
mod current_time;
mod daemon_thread_recovery;
mod dynamic_tools;
mod effective_plugin_change;
mod error_code;
mod extensions;
mod external_agent_migration;
mod external_auth;
mod filters;
mod fs_watch;
mod fuzzy_file_search;
mod gateway_oauth_notifications;
mod image_url;
pub mod in_process;
mod log_write_warning;
#[cfg(unix)]
mod managed_daemon;
mod mcp_refresh;
mod message_processor;
mod model_catalog;
mod models;
mod models_refresh_worker;
mod notification_media;
mod otel_config;
mod otel_reloader;
mod outgoing_message;
mod plugin_config_reload;
mod processor_task_retirement;
mod request_processors;
mod request_serialization;
mod server_request_error;
mod shutdown_intake;
mod skills_watcher;
mod standalone_shutdown;
mod startup_warnings;
mod thread_sample;
mod thread_state;
mod thread_status;
mod transport;
mod turn_admission;
mod turn_cost_worker;
mod user_verification;
mod user_verification_response;

pub use crate::code_mode_host::AppServerCodeModeHostArgs;
pub use crate::code_mode_host::CodeModeHostTransport;
pub use crate::error_code::INPUT_TOO_LARGE_ERROR_CODE;
pub use crate::error_code::INVALID_PARAMS_ERROR_CODE;
pub use crate::transport::AppServerTransport;
pub use crate::transport::RemoteControlStartupMode;
pub use crate::transport::app_server_control_socket_path;
pub use crate::transport::take_remote_control_disabled_env;
pub use crate::transport::verified_local_daemon_socket;
pub use codex_app_server_transport::PeerExecutableIdentity;

const LOG_FORMAT_ENV_VAR: &str = "LOG_FORMAT";
const OTEL_SERVICE_NAME: &str = "codex-app-server";
#[cfg(debug_assertions)]
const TEST_USER_CONFIG_FILE_ENV_VAR: &str = "CODEX_APP_SERVER_TEST_USER_CONFIG_FILE";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LogFormat {
    Default,
    Json,
}

type StderrLogLayer = Box<dyn Layer<Registry> + Send + Sync + 'static>;

fn stderr_span_events() -> FmtSpan {
    FmtSpan::NEW | FmtSpan::CLOSE
}

/// Delivery accounting for a reply the router could not queue. Positively
/// closed recipients (a websocket `Closed`, or stdio EPIPE) count as
/// undeliverable after disconnect. Anything else (a stdio writer gone without
/// a positive close, e.g. after SIGTERM) is abandoned, which is incomplete.
fn account_reply_drop(
    expiry: &codex_app_server_transport::OwnerExpiry,
    refused_without_close: &mut HashSet<ConnectionId>,
    drop: crate::transport::ReplyDrop,
) {
    match drop {
        crate::transport::ReplyDrop::WriterRefused {
            connection_id,
            disconnectable: false,
        } => {
            refused_without_close.insert(connection_id);
            expiry.record_dropped_output(/*messages*/ 1, /*bytes*/ 0);
        }
        crate::transport::ReplyDrop::WriterRefused {
            disconnectable: true,
            ..
        } => expiry.record_undeliverable_after_disconnect(/*messages*/ 1, /*bytes*/ 0),
        crate::transport::ReplyDrop::Unregistered(connection_id) => {
            if refused_without_close.contains(&connection_id) {
                expiry.record_dropped_output(/*messages*/ 1, /*bytes*/ 0);
            } else {
                expiry.record_undeliverable_after_disconnect(/*messages*/ 1, /*bytes*/ 0);
            }
        }
    }
}

/// Production stages of the standalone drain (see `standalone_shutdown`).
/// Retained by the owner after the drain: processor, retirement ticket and
/// any parked connection cleanup stay owned until the host returns.
struct ProcessorShutdownStages {
    processor: Arc<MessageProcessor>,
    connections: Vec<(ConnectionId, ConnectionState)>,
    cleanup: Option<ConnectionCleanupTasks>,
    thread_retirement: Option<crate::request_processors::ProcessorThreadRetirement>,
    expiry: codex_app_server_transport::OwnerExpiry,
}

impl standalone_shutdown::StandaloneStages for ProcessorShutdownStages {
    fn bind_runtimes(&mut self, owner_deadline: tokio::time::Instant) -> BoxFuture<'static, bool> {
        // Synchronous (no async registry lock): binds every reachable runtime
        // now. Incomplete if this binding, or the earlier one at intake,
        // found a runtime whose cleanup had already started in Legacy mode.
        let binding = self
            .processor
            .bind_thread_shutdown_deadlines_now(owner_deadline);
        let intake = self.processor.intake_thread_binding();
        for census in std::iter::once(binding).chain(intake) {
            if !census.is_complete() {
                warn!(
                    ?census,
                    "standalone shutdown: thread cleanup deadline not bound for every runtime"
                );
            }
        }
        let clean = binding.is_complete() && intake.is_none_or(|census| census.is_complete());
        futures::future::ready(clean).boxed()
    }

    fn connections(&mut self, stage_deadline: tokio::time::Instant) -> BoxFuture<'static, bool> {
        let Some(mut cleanup) = self.cleanup.take() else {
            return futures::future::ready(false).boxed();
        };
        for (connection_id, connection_state) in self.connections.drain(..) {
            let processor = Arc::clone(&self.processor);
            let session = Arc::clone(&connection_state.session);
            cleanup.spawn(async move {
                processor
                    .connection_closed_until(connection_id, &session, Some(stage_deadline))
                    .await;
            });
        }
        // Each close is cooperative: at the latched owner expiry it stops
        // before its next unit and hands its owner back (no abort). A timed-out
        // RPC drain leaves accepted work unproven, so the stage is incomplete.
        // A request accepted before stdin EOF and never answered is output
        // abandoned for a recipient that is still there.
        let processor = Arc::clone(&self.processor);
        let expiry = self.expiry.clone();
        async move {
            let drained = cleanup.drain().await;
            let unanswered = processor.unanswered_after_half_close();
            expiry.record_abandoned(unanswered, /*bytes*/ 0);
            drained && processor.connection_rpc_drains_complete() && unanswered == 0
        }
        .boxed()
    }

    fn background(&mut self, stage_deadline: tokio::time::Instant) -> BoxFuture<'static, bool> {
        let processor = Arc::clone(&self.processor);
        async move {
            let report = processor
                .drain_standalone_background_tasks_until(stage_deadline)
                .await;
            if !report.is_clean() {
                warn!(?report, "standalone background shutdown incomplete");
            }
            report.is_clean()
        }
        .boxed()
    }

    fn plugin_tasks(&mut self, stage_deadline: tokio::time::Instant) -> BoxFuture<'static, bool> {
        let processor = Arc::clone(&self.processor);
        async move {
            let drain = processor.plugin_tasks_shutdown_until(stage_deadline).await;
            if !drain.is_clean() {
                // E.g. the curated-sync OS thread still in a git step: its
                // retained join is reported incomplete, never as stopped.
                warn!(?drain, "standalone plugin worker drain incomplete");
            }
            drain.is_clean()
        }
        .boxed()
    }

    fn threads(&mut self, stage_deadline: tokio::time::Instant) -> BoxFuture<'static, bool> {
        // Permanent: closes construction admission and binds (tightens) each
        // runtime's deadline before its Shutdown submission.
        let processor = Arc::clone(&self.processor);
        match self.processor.begin_thread_shutdown(stage_deadline) {
            Ok(retirement) => {
                self.thread_retirement = Some(retirement.clone());
                async move {
                    // Thread-clear contract (kept from the legacy path): a held
                    // clear reservation is reported incomplete, never awaited.
                    let no_held_clear = processor.report_held_clear_reservations().await;
                    let report = retirement.wait().await;
                    if !report.is_complete() {
                        warn!(
                            ?report,
                            "standalone thread shutdown incomplete; custody retained"
                        );
                    }
                    no_held_clear && report.is_complete()
                }
                .boxed()
            }
            Err(error) => {
                warn!(?error, "standalone thread shutdown custody unavailable");
                async move {
                    let _ = processor.report_held_clear_reservations().await;
                    false
                }
                .boxed()
            }
        }
    }
}

/// Control-plane messages from the processor/transport side to the outbound router task.
///
/// `run_main_with_transport_options` uses two loops/tasks:
/// - processor loop: handles incoming JSON-RPC and request dispatch
/// - outbound loop: performs potentially slow writes to per-connection writers
///
/// `OutboundControlEvent` keeps those loops coordinated without sharing mutable
/// connection state directly. In particular, the outbound loop needs to know
/// when a connection opens/closes so it can route messages correctly.
enum OutboundControlEvent {
    /// Register a new writer for an opened connection.
    Opened {
        connection_id: ConnectionId,
        writer: mpsc::Sender<QueuedOutgoingMessage>,
        disconnect_sender: Option<CancellationToken>,
        initialized: Arc<AtomicBool>,
        experimental_api_enabled: Arc<AtomicBool>,
        opted_out_notification_methods: Arc<RwLock<HashSet<String>>>,
    },
    /// Remove state for a closed/disconnected connection.
    Closed { connection_id: ConnectionId },
    /// Disconnect all connection-oriented clients during graceful restart.
    DisconnectAll,
    /// Route queued drained replies, release writers, then stop.
    CloseAllAfterQueued,
}

#[derive(Default)]
struct ShutdownState {
    requested: bool,
    forced: bool,
    last_logged_running_turn_count: Option<usize>,
}

enum ShutdownAction {
    Noop,
    Finish,
}

#[derive(Clone, Copy)]
enum ShutdownSignal {
    Forceable,
    #[cfg(unix)]
    GracefulOnly,
}

async fn shutdown_signal() -> IoResult<ShutdownSignal> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::SignalKind;
        use tokio::signal::unix::signal;

        let mut term = signal(SignalKind::terminate())?;
        let mut hangup = signal(SignalKind::hangup())?;
        tokio::select! {
            ctrl_c_result = tokio::signal::ctrl_c() => ctrl_c_result.map(|_| ShutdownSignal::Forceable),
            _ = term.recv() => Ok(ShutdownSignal::Forceable),
            _ = hangup.recv() => Ok(ShutdownSignal::GracefulOnly),
        }
    }

    #[cfg(windows)]
    {
        let console_signal = async {
            if tokio::signal::ctrl_c().await.is_err() {
                // A detached daemon has no console; keep its local control path active.
                std::future::pending::<()>().await;
            }
        };
        console_signal.await;
        Ok(ShutdownSignal::Forceable)
    }
}

impl ShutdownState {
    fn requested(&self) -> bool {
        self.requested
    }

    fn forced(&self) -> bool {
        self.forced
    }

    fn on_signal(
        &mut self,
        signal: ShutdownSignal,
        connection_count: usize,
        running_turn_count: usize,
        turn_admission: &turn_admission::TurnAdmission,
    ) {
        if self.requested {
            if matches!(signal, ShutdownSignal::Forceable) {
                self.forced = true;
            }
            return;
        }

        turn_admission.begin_drain();
        self.requested = true;
        self.last_logged_running_turn_count = None;
        info!(
            "received shutdown signal; entering graceful restart drain (connections={}, runningAssistantTurns={}, new client turns rejected)",
            connection_count, running_turn_count,
        );
    }

    fn update(
        &mut self,
        running_turn_count: usize,
        active_admissions: usize,
        connection_count: usize,
    ) -> ShutdownAction {
        if !self.requested {
            return ShutdownAction::Noop;
        }

        if self.forced || (running_turn_count == 0 && active_admissions == 0) {
            if self.forced {
                info!(
                    "received second shutdown signal; forcing restart with {running_turn_count} running assistant turn(s) and {connection_count} connection(s)"
                );
            } else {
                info!(
                    "shutdown signal restart: no assistant turns running; stopping acceptor and disconnecting {connection_count} connection(s)"
                );
            }
            return ShutdownAction::Finish;
        }

        if self.last_logged_running_turn_count != Some(running_turn_count) {
            info!(
                "shutdown signal restart: waiting for {running_turn_count} running assistant turn(s) and {active_admissions} admitted request(s) to finish"
            );
            self.last_logged_running_turn_count = Some(running_turn_count);
        }

        ShutdownAction::Noop
    }
}

fn config_warning_from_error(
    summary: impl Into<String>,
    err: &std::io::Error,
) -> ConfigWarningNotification {
    let (path, range) = match config_error_location(err) {
        Some((path, range)) => (Some(path), Some(range)),
        None => (None, None),
    };
    ConfigWarningNotification {
        summary: summary.into(),
        details: Some(err.to_string()),
        path,
        range,
    }
}

fn config_error_location(err: &std::io::Error) -> Option<(String, AppTextRange)> {
    err.get_ref()
        .and_then(|err| err.downcast_ref::<ConfigLoadError>())
        .map(|err| {
            let config_error = err.config_error();
            (
                config_error.path.to_string_lossy().to_string(),
                app_text_range(&config_error.range),
            )
        })
}

fn exec_policy_warning_location(err: &ExecPolicyError) -> (Option<String>, Option<AppTextRange>) {
    match err {
        ExecPolicyError::ParsePolicy { path, source } => {
            if let Some(location) = source.location() {
                let range = AppTextRange {
                    start: AppTextPosition {
                        line: location.range.start.line,
                        column: location.range.start.column,
                    },
                    end: AppTextPosition {
                        line: location.range.end.line,
                        column: location.range.end.column,
                    },
                };
                return (Some(location.path), Some(range));
            }
            (Some(path.clone()), None)
        }
        _ => (None, None),
    }
}

fn exec_policy_config_warning(err: &ExecPolicyError) -> ConfigWarningNotification {
    let (path, range) = exec_policy_warning_location(err);
    ConfigWarningNotification {
        summary: "Error parsing rules; custom rules not applied.".to_string(),
        details: Some(err.to_string()),
        path,
        range,
    }
}

fn app_text_range(range: &CoreTextRange) -> AppTextRange {
    AppTextRange {
        start: AppTextPosition {
            line: range.start.line,
            column: range.start.column,
        },
        end: AppTextPosition {
            line: range.end.line,
            column: range.end.column,
        },
    }
}

fn project_config_warning(config: &Config) -> Option<ConfigWarningNotification> {
    let mut disabled_folders = Vec::new();

    for layer in config.config_layer_stack.all_layers_low_to_high() {
        let ConfigLayerSource::Project { dot_codex_folder } = &layer.name else {
            continue;
        };
        let Some(disabled_reason) = &layer.disabled_reason else {
            continue;
        };
        disabled_folders.push((
            dot_codex_folder.as_path().display().to_string(),
            disabled_reason.clone(),
        ));
    }

    if disabled_folders.is_empty() {
        return None;
    }

    let mut message = concat!(
        "Project-local config, hooks, and exec policies are disabled in the following folders ",
        "until the project is trusted, but skills still load.\n",
    )
    .to_string();
    for (index, (folder, reason)) in disabled_folders.iter().enumerate() {
        let display_index = index + 1;
        message.push_str(&format!("    {display_index}. {folder}\n"));
        message.push_str(&format!("       {reason}\n"));
    }

    Some(ConfigWarningNotification {
        summary: message,
        details: None,
        path: None,
        range: None,
    })
}

impl LogFormat {
    fn from_env_value(value: Option<&str>) -> Self {
        match value.map(str::trim).map(str::to_ascii_lowercase) {
            Some(value) if value == "json" => Self::Json,
            _ => Self::Default,
        }
    }
}

fn log_format_from_env() -> LogFormat {
    let value = std::env::var(LOG_FORMAT_ENV_VAR).ok();
    LogFormat::from_env_value(value.as_deref())
}

pub async fn run_main(
    arg0_paths: Arg0DispatchPaths,
    cli_config_overrides: CliConfigOverrides,
    loader_overrides: LoaderOverrides,
    strict_config: bool,
    default_analytics_enabled: bool,
) -> IoResult<()> {
    run_main_with_transport_options(
        arg0_paths,
        cli_config_overrides,
        loader_overrides,
        strict_config,
        default_analytics_enabled,
        AppServerTransport::Stdio,
        SessionSource::VSCode,
        WebsocketAuthSettings::default(),
        AppServerRuntimeOptions::default(),
    )
    .await?
    .into_shutdown_result()
}

/// Reports whether shutdown finished or the managed daemon must exit without waiting for I/O.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppServerExit {
    /// Every shutdown stage was positively proven complete within the owner deadline.
    Graceful,
    /// Executables should exit before dropping their runtime; libraries must not exit their host.
    Forced,
    /// Shutdown ran to its owner deadline without proving cleanup finished: a
    /// stage timed out or stayed incomplete, a task handed back its owner, or
    /// a blocking worker was still running. Details are in the
    /// `codex.app_server.standalone_shutdown` report. Never a clean exit:
    /// executables should exit non-zero after a normal runtime drop (which may
    /// still wait on that work); this is not a request to force-exit.
    ShutdownIncomplete,
}

impl AppServerExit {
    /// Shared consumer mapping (CLI, binaries, `run_main`, remote control).
    /// `ShutdownIncomplete` becomes an error so it can never pass for a clean
    /// exit. `Forced` is `Ok`, because the executable has already handled it.
    pub fn into_shutdown_result(self) -> IoResult<()> {
        match self {
            AppServerExit::Graceful | AppServerExit::Forced => Ok(()),
            AppServerExit::ShutdownIncomplete => Err(std::io::Error::other(
                "app-server shutdown incomplete: cleanup was not proven within its deadline",
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginStartupTasks {
    Start,
    Skip,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppServerRuntimeOptions {
    /// Optional host-owned frozen cloud skill catalog and resources, loaded once at startup.
    pub cloud_skills_snapshot: Option<std::path::PathBuf>,
    pub code_mode_host_transport: CodeModeHostTransport,
    pub plugin_startup_tasks: PluginStartupTasks,
    pub remote_control_startup_mode: RemoteControlStartupMode,
    pub install_shutdown_signal_handler: bool,
    pub managed_daemon: bool,
}

impl Default for AppServerRuntimeOptions {
    fn default() -> Self {
        Self {
            cloud_skills_snapshot: None,
            code_mode_host_transport: CodeModeHostTransport::Local,
            plugin_startup_tasks: PluginStartupTasks::Start,
            remote_control_startup_mode: RemoteControlStartupMode::ResolvePersisted,
            install_shutdown_signal_handler: true,
            managed_daemon: false,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn run_main_with_transport_options(
    arg0_paths: Arg0DispatchPaths,
    cli_config_overrides: CliConfigOverrides,
    loader_overrides: LoaderOverrides,
    strict_config: bool,
    default_analytics_enabled: bool,
    transport: AppServerTransport,
    session_source: SessionSource,
    auth: WebsocketAuthSettings,
    runtime_options: AppServerRuntimeOptions,
) -> IoResult<AppServerExit> {
    let cloud_skill_provider = match runtime_options.cloud_skills_snapshot {
        Some(path) => {
            let provider = tokio::task::spawn_blocking(move || {
                codex_skills_extension::FrozenCloudSkillProvider::from_file(&path)
            })
            .await
            .map_err(std::io::Error::other)?
            .map_err(|err| std::io::Error::new(ErrorKind::InvalidInput, err))?;
            Some(Arc::new(provider) as Arc<dyn codex_skills_extension::SkillProvider>)
        }
        None => None,
    };
    #[cfg(target_os = "windows")]
    let _registered_core = codex_windows_sandbox::registered_core_requested();
    let control_endpoint = mcp_control_endpoint_uri(&transport);
    let managed_transition_control_socket_endpoint = match &transport {
        AppServerTransport::UnixSocket { socket_path } => Some(socket_path.display().to_string()),
        _ => None,
    };
    let managed_daemon = matches!(&transport, AppServerTransport::UnixSocket { .. })
        && runtime_options.managed_daemon;
    #[cfg(unix)]
    let nofile_limit_result = managed_daemon.then(managed_daemon::raise_nofile_limit);
    let loader_overrides = loader_overrides_with_test_user_config_file(
        loader_overrides,
        test_user_config_file_from_env(),
    )?;
    let (transport_event_tx, mut transport_event_rx) =
        mpsc::channel::<TransportEvent>(CHANNEL_CAPACITY);
    let (outgoing_tx, mut outgoing_rx) = mpsc::channel::<OutgoingEnvelope>(CHANNEL_CAPACITY);
    let (outbound_control_tx, mut outbound_control_rx) =
        mpsc::channel::<OutboundControlEvent>(CHANNEL_CAPACITY);

    // Parse CLI overrides once and derive the base Config eagerly so later
    // components do not need to work with raw TOML values.
    let cli_kv_overrides = cli_config_overrides.parse_overrides().map_err(|e| {
        std::io::Error::new(
            ErrorKind::InvalidInput,
            format!("error parsing -c overrides: {e}"),
        )
    })?;
    let codex_home = find_codex_home()?;
    let startup_cwd = std::env::current_dir().ok();
    let local_runtime_paths = ExecServerRuntimeOptions::from_optional_paths(
        arg0_paths.codex_self_exe.clone(),
        arg0_paths.codex_linux_sandbox_exe.clone(),
    )?;
    let ignore_user_config = loader_overrides.ignore_user_config;
    let config_manager = ConfigManager::new(
        codex_home.to_path_buf(),
        cli_kv_overrides.clone(),
        loader_overrides,
        strict_config,
        Default::default(),
        arg0_paths.clone(),
        Arc::new(NoopThreadConfigLoader),
    );
    let startup = config_manager
        .load_managed_startup_config(startup_cwd.clone())
        .await?;
    let mut config_warnings = Vec::new();
    let mut plugin_startup_config = PluginStartupConfig::Current;
    let config = match startup.config {
        Ok(config) => config,
        Err(err) if is_unsupported_untrusted_approval_policy_error(&err) => {
            return Err(err);
        }
        Err(err) => {
            if strict_config {
                return Err(err);
            }

            let message = config_warning_from_error("Invalid configuration; using defaults.", &err);
            config_warnings.push(message);
            plugin_startup_config = PluginStartupConfig::Defaults;
            config_manager.load_default_config().await.map_err(|e| {
                std::io::Error::new(
                    ErrorKind::InvalidData,
                    format!("error loading default config after config error: {e}"),
                )
            })?
        }
    };
    config.validate_windows_mxc_requirement()?;
    config.auth_config().validate()?;
    let auth_manager =
        AuthManager::shared_from_config(&config, /*enable_codex_api_key_env*/ false)
            .await
            .map_err(std::io::Error::other)?;
    config_manager.update_managed_config_loader(auth_manager.clone(), &config);
    config_manager
        .sync_default_client_residency_requirement()
        .await;

    #[cfg(target_os = "macos")]
    let local_runtime_paths = local_runtime_paths.with_allowed_symlinked_codex_home(
        codex_config::allowed_symlinked_codex_home(&config.config_layer_stack, &config.codex_home),
    );
    let code_mode_session_provider: Option<Arc<dyn CodeModeSessionProvider>> =
        match &runtime_options.code_mode_host_transport {
            CodeModeHostTransport::Local => None,
            CodeModeHostTransport::Grpc(url) => {
                if !config.features.enabled(Feature::CodeModeHost) {
                    return Err(std::io::Error::new(
                        ErrorKind::InvalidInput,
                        "remote code-mode host requires the code_mode_host feature to be enabled",
                    ));
                }
                Some(Arc::new(
                    GrpcCodeModeSessionProvider::with_http_client_factory(
                        url.to_string(),
                        config.http_client_factory(),
                    ),
                ))
            }
        };
    // Executor credentials belong to the selected environment, not the current account.
    // Each request and connection still acquires a revocable application-policy permit.
    let environment_http_client_factory = config
        .http_client_factory()
        .with_network_policy(config.application_network_policy.clone());
    let environment_manager = if ignore_user_config {
        EnvironmentManager::from_env(Some(local_runtime_paths), environment_http_client_factory)
            .await
    } else {
        EnvironmentManager::from_codex_home(
            codex_home.clone(),
            Some(local_runtime_paths),
            environment_http_client_factory,
        )
        .await
    }
    .map(Arc::new)
    .map_err(std::io::Error::other)?;

    let mut prepared_otel = Some(
        codex_core::otel_init::prepare_provider(
            &config,
            env!("CARGO_PKG_VERSION"),
            Some(OTEL_SERVICE_NAME),
            default_analytics_enabled,
        )
        .map_err(|e| {
            std::io::Error::new(
                ErrorKind::InvalidData,
                format!("error loading otel config: {e}"),
            )
        })?,
    );
    let unix_socket_startup_lock = match &transport {
        AppServerTransport::UnixSocket { .. } => {
            let startup_lock_path = app_server_startup_lock_path(&codex_home)?;
            let startup_lock = acquire_app_server_startup_lock(startup_lock_path).await?;
            Some(startup_lock)
        }
        _ => None,
    };
    let sqlite_warning_index = config_warnings.len();

    if let Ok(Some(err)) = check_execpolicy_for_warnings(&config.config_layer_stack).await {
        config_warnings.push(exec_policy_config_warning(&err));
    }

    if let Some(warning) = project_config_warning(&config) {
        config_warnings.push(warning);
    }
    for warning in &config.startup_warnings {
        config_warnings.push(ConfigWarningNotification {
            summary: warning.clone(),
            details: None,
            path: None,
            range: None,
        });
    }
    if let Some(warning) = codex_core::config::system_bwrap_warning(
        &config.permissions.effective_permission_profile(),
        &config.cwd,
    ) {
        config_warnings.push(ConfigWarningNotification {
            summary: warning,
            details: None,
            path: None,
            range: None,
        });
    }

    let runtime_auth_slot = config
        .features
        .enabled(Feature::RuntimeAgentInstances)
        .then(codex_runtime_agent_instance::RuntimeAuthSlot::default);
    let analytics_events_client = analytics_events_client_from_config(
        Arc::clone(&auth_manager),
        &config,
        runtime_auth_slot.clone(),
    );
    let outgoing_message_sender = Arc::new(OutgoingMessageSender::new(
        outgoing_tx,
        analytics_events_client.clone(),
    ));
    let feedback = CodexFeedback::new();

    // Install a simple subscriber so `tracing` output is visible. Users can
    // control the log level with `RUST_LOG` and switch to JSON logs with
    // `LOG_FORMAT=json`.
    // SQLx enters the caller's span for each command. Skip enter/exit records
    // that can block its worker on stderr while holding a write transaction.
    // Preserve span boundaries, busy/idle timings, and explicit events.
    let stderr_fmt: StderrLogLayer = match log_format_from_env() {
        LogFormat::Json => tracing_subscriber::fmt::layer()
            .json()
            .with_writer(codex_app_server_daemon::stderr_writer)
            .with_span_events(stderr_span_events())
            .with_filter(EnvFilter::from_default_env())
            .boxed(),
        LogFormat::Default => tracing_subscriber::fmt::layer()
            .with_writer(codex_app_server_daemon::stderr_writer)
            .with_span_events(stderr_span_events())
            .with_filter(EnvFilter::from_default_env())
            .boxed(),
    };

    let log_write_warning = log_write_warning::LogWriteWarningReporter::new(
        feedback.clone(),
        &outgoing_message_sender,
        &config,
    );
    let feedback_layer = feedback.logger_layer();
    let feedback_metadata_layer = feedback.metadata_layer();
    let (log_db_layer, log_db_reload) =
        tracing_subscriber::reload::Layer::new(None::<log_db::LogDbLayer>);
    let log_db_layer = log_db_layer.with_filter(log_db::default_filter());
    let (otel_layers, otel_routes) = codex_otel::ManagedTelemetryRoutes::layers();
    tracing_subscriber::registry()
        .with(stderr_fmt)
        .with(feedback_layer)
        .with(feedback_metadata_layer)
        .with(log_db_layer)
        .with(otel_layers)
        .try_init()
        .map_err(|_| std::io::Error::other("app-server tracing subscriber unavailable"))?;
    let otel = otel_routes
        .publish(&mut prepared_otel)
        .map_err(|_| std::io::Error::other("initial telemetry publication unavailable"))?;
    codex_core::otel_init::record_process_start(otel.provider.as_ref(), OTEL_SERVICE_NAME);
    codex_core::otel_init::install_sqlite_telemetry(otel.provider.as_ref(), OTEL_SERVICE_NAME);
    let state_db_init = match init_sqlite_state_db_with_fresh_start_on_corruption(&config).await {
        Ok(state_db_init) => state_db_init,
        Err(err) => {
            return Err(std::io::Error::other(format!(
                "failed to initialize sqlite state runtime under {}: {err:#}",
                config.sqlite_config().home().display()
            )));
        }
    };
    let state_db = state_db_init.state_db;
    if let Some(recovery_notice) = state_db_init.recovery_notice {
        config_warnings.insert(
            sqlite_warning_index,
            ConfigWarningNotification {
                summary: SQLITE_RECOVERY_CONFIG_WARNING_SUMMARY.to_string(),
                details: Some(recovery_notice.details),
                path: None,
                range: None,
            },
        );
    }

    let log_db = state_db
        .clone()
        .map(|state_db| log_db::start(state_db, log_write_warning.clone()));
    log_db_reload
        .reload(log_db.clone())
        .map_err(|_| std::io::Error::other("app-server log database subscriber unavailable"))?;
    if let Some(error) = state_db.as_ref().and_then(|db| db.diagnostic_logs_error()) {
        tracing::warn!(
            event.name = "diagnostic_database_unavailable",
            error,
            "diagnostic database unavailable"
        );
    }

    #[cfg(unix)]
    if let Some(Err(err)) = nofile_limit_result {
        warn!(%err, "failed to raise managed app-server file descriptor limit");
    }
    for warning in &config_warnings {
        match &warning.details {
            Some(details) => error!("{} {}", warning.summary, details),
            None => error!("{}", warning.summary),
        }
    }
    let remote_control_policy = if config
        .config_layer_stack
        .requirements()
        .allow_remote_control
        .as_ref()
        .is_some_and(|requirement| !requirement.value)
    {
        RemoteControlPolicy::DisabledByRequirements
    } else {
        RemoteControlPolicy::Allowed
    };
    let remote_control_startup_mode = runtime_options.remote_control_startup_mode;
    let remote_control_explicitly_requested =
        remote_control_startup_mode == RemoteControlStartupMode::EnabledEphemeral;
    if remote_control_explicitly_requested
        && remote_control_policy == RemoteControlPolicy::DisabledByRequirements
    {
        return Err(std::io::Error::new(
            ErrorKind::InvalidInput,
            "remote control is disabled by managed requirements",
        ));
    }
    let installation_id = resolve_installation_id(&config.codex_home).await?;
    let transport_shutdown_token = CancellationToken::new();
    // Remote enrollment must cancel before RPC drain without shutting down telemetry.
    let remote_control_shutdown_token = transport_shutdown_token.child_token();
    let mut transport_accept_handles = Vec::<JoinHandle<()>>::new();
    // Keep physical custody in this host scope, outside the processor task.
    // Transport EOF/control/signal intake records the first absolute deadline.
    let (owner_latch, host_custody) = shutdown_intake::OwnerLatch::new();
    let owner_latch = Arc::new(owner_latch);
    shutdown_intake::install_intake_latch(&owner_latch);
    let owner_expiry = owner_latch.expiry();

    let websocket_compression = if config
        .features
        .enabled(Feature::AppServerWebSocketCompression)
    {
        WebSocketCompression::Zstd
    } else {
        WebSocketCompression::Plain
    };
    let single_client_mode = matches!(&transport, AppServerTransport::Stdio);
    let graceful_signal_restart_enabled =
        runtime_options.install_shutdown_signal_handler && !single_client_mode;
    if graceful_signal_restart_enabled {
        shutdown_intake::spawn_signal_intake(owner_expiry.clone(), async {
            shutdown_signal().await.is_ok()
        });
    }
    let mut app_server_client_name_rx = None;

    match &transport {
        AppServerTransport::Stdio => {
            let (stdio_client_name_tx, stdio_client_name_rx) = oneshot::channel::<String>();
            app_server_client_name_rx = Some(stdio_client_name_rx);
            let handle = start_stdio_connection(
                transport_event_tx.clone(),
                stdio_client_name_tx,
                runtime_options.install_shutdown_signal_handler,
                owner_expiry.clone(),
            )
            .await?;
            transport_accept_handles.push(handle);
        }
        AppServerTransport::UnixSocket { socket_path } => {
            let accept_handle = start_control_socket_acceptor(
                socket_path.clone(),
                transport_event_tx.clone(),
                transport_shutdown_token.clone(),
                if cfg!(windows)
                    && std::env::var_os(codex_app_server_transport::DAEMON_SHUTDOWN_SOCKET_ENV)
                        .is_some()
                {
                    DaemonShutdownAccess::Managed
                } else {
                    DaemonShutdownAccess::Disabled
                },
                websocket_compression,
                &host_custody,
            )
            .await?;
            transport_accept_handles.push(accept_handle);
        }
        AppServerTransport::WebSocket { bind_address } => {
            let accept_handle = start_websocket_acceptor(
                *bind_address,
                transport_event_tx.clone(),
                transport_shutdown_token.clone(),
                policy_from_settings(&auth)?,
                websocket_compression,
                owner_expiry.clone(),
            )
            .await?;
            transport_accept_handles.push(accept_handle);
        }
        AppServerTransport::Off => {}
    }
    drop(unix_socket_startup_lock);

    let remote_control_enabled = remote_control_policy == RemoteControlPolicy::Allowed
        && remote_control_explicitly_requested
        && state_db.is_some();
    if remote_control_explicitly_requested && state_db.is_none() {
        error!("remote control disabled because sqlite state db is unavailable");
    }
    let no_local_transport = transport_accept_handles.is_empty();
    if no_local_transport
        && remote_control_startup_mode != RemoteControlStartupMode::ResolvePersisted
        && !remote_control_enabled
    {
        return Err(std::io::Error::new(
            ErrorKind::InvalidInput,
            if remote_control_policy == RemoteControlPolicy::DisabledByRequirements {
                "no transport configured; remote control disabled by managed requirements"
            } else if remote_control_explicitly_requested && state_db.is_none() {
                "no transport configured; remote control disabled because sqlite state db is unavailable"
            } else {
                "no transport configured; use --listen or enable remote control"
            },
        ));
    }

    let (remote_control_accept_handle, remote_control_handle) = start_remote_control(
        RemoteControlStartConfig {
            runtime_auth: runtime_auth_slot
                .clone()
                .map(|slot| (slot, config.runtime_auth_mode)),
            remote_control_url: config.chatgpt_base_url.clone(),
            installation_id: installation_id.clone(),
            policy: remote_control_policy,
        },
        state_db.clone(),
        auth_manager.clone(),
        transport_event_tx.clone(),
        remote_control_shutdown_token.clone(),
        app_server_client_name_rx,
        remote_control_startup_mode,
    )
    .await?;
    if no_local_transport
        && remote_control_startup_mode == RemoteControlStartupMode::ResolvePersisted
    {
        let persisted_enabled = match remote_control_handle
            .resolve_persisted_preference(/*app_server_client_name*/ None)
            .await
        {
            Ok(persisted_enabled) => persisted_enabled,
            Err(err) => {
                warn!("failed to resolve persisted remote control preference: {err}");
                false
            }
        };
        if !persisted_enabled {
            transport_shutdown_token.cancel();
            let _ = remote_control_accept_handle.await;
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                if remote_control_policy == RemoteControlPolicy::DisabledByRequirements {
                    "no transport configured; remote control disabled by managed requirements"
                } else {
                    "no transport configured; use --listen or enable remote control"
                },
            ));
        }
    }
    transport_accept_handles.push(remote_control_accept_handle);

    // Only the standalone server measures its local home, not embedded/cloud runtimes.
    if let Some(metrics) = otel
        .provider
        .as_ref()
        .and_then(codex_otel::OtelProvider::metrics)
    {
        codex_home_metrics::spawn(&config, metrics.clone(), transport_shutdown_token.clone());
    }

    let (telemetry_reset, otel_reloader_handle) = otel_reloader::spawn_managed_with_owner_deadline(
        otel,
        otel_routes,
        config_manager.clone(),
        Arc::clone(&auth_manager),
        default_analytics_enabled,
        transport_shutdown_token.clone(),
        owner_latch.subscribe(),
    );

    let router_expiry = owner_expiry.clone();
    let outbound_router = async move {
        let mut outbound_connections = HashMap::<ConnectionId, OutboundConnectionState>::new();
        // Writers that refused a reply without a positive close (e.g. stdio
        // after SIGTERM). Later replies to them are abandoned, not undeliverable.
        let mut refused_without_close = HashSet::<ConnectionId>::new();
        let mut control_open = true;
        loop {
            tokio::select! {
                    biased;
                    event = outbound_control_rx.recv(), if control_open => {
                        let Some(event) = event else {
                            // Never drop queued replies because the control
                            // side closed first: keep routing until the
                            // outgoing channel itself closes.
                            control_open = false;
                            continue;
                        };
                        match event {
                            OutboundControlEvent::Opened {
                                connection_id,
                                writer,
                                disconnect_sender,
                                initialized,
                                experimental_api_enabled,
                                opted_out_notification_methods,
                            } => {
                                outbound_connections.insert(
                                    connection_id,
                                    OutboundConnectionState::new(
                                        writer,
                                        initialized,
                                        experimental_api_enabled,
                                        opted_out_notification_methods,
                                        disconnect_sender,
                                    ),
                                );
                            }
                            OutboundControlEvent::Closed { connection_id } => {
                                outbound_connections.remove(&connection_id);
                            }
                            OutboundControlEvent::DisconnectAll => {
                                info!(
                                    "disconnecting {} outbound websocket connection(s) for graceful restart",
                                    outbound_connections.len()
                                );
                                for connection_state in outbound_connections.values() {
                                    connection_state.request_disconnect();
                                }
                                outbound_connections.clear();
                            }
                            OutboundControlEvent::CloseAllAfterQueued => {
                                // Replies enqueued before the drain finished
                                // are routed first (FIFO), never dropped.
                                while let Ok(envelope) = outgoing_rx.try_recv() {
                                    if let Some(reply_drop) =
                                        route_outgoing_envelope(&mut outbound_connections, envelope)
                                            .await
                                    {
                                        account_reply_drop(
                                            &router_expiry,
                                            &mut refused_without_close,
                                            reply_drop,
                                        );
                                    }
                                }
                                outbound_connections.clear();
                                break;
                            }
                        }
                    }
                    envelope = outgoing_rx.recv() => {
                    let Some(envelope) = envelope else {
                        break;
                    };
                    if let Some(reply_drop) =
                        route_outgoing_envelope(&mut outbound_connections, envelope).await
                    {
                        account_reply_drop(&router_expiry, &mut refused_without_close, reply_drop);
                    }
                }
            }
        }
        info!("outbound router task exited (channel closed)");
    };
    // (b) Stops before its next route at owner expiry and hands its writers
    // and queues back to the host owner; never aborted.
    let outbound_handle = tokio::spawn({
        let owner_expiry = owner_expiry.clone();
        async move {
            if let Err(refused) = owner_expiry.run("outbound_router", outbound_router).await {
                error!("outbound router not driven: {refused}");
            }
        }
    });

    let recovery_file = daemon_recovery_file_path(&config.codex_home);
    let daemon_settings_file = managed_daemon.then(|| {
        config
            .codex_home
            .as_path()
            .join("app-server-daemon/settings.json")
    });
    let processor_handle = standalone_shutdown::spawn_fenced_processor(owner_expiry.clone(), {
        let auth_manager = Arc::clone(&auth_manager);
        let initialize_notification_sender = outgoing_message_sender.clone();
        let outbound_control_tx = outbound_control_tx;
        let (startup_warnings_tx, mut startup_warnings_rx) = mpsc::unbounded_channel();
        let mut processor = MessageProcessor::new(MessageProcessorArgs {
            telemetry_reset,
            managed_transition_control_socket_endpoint,
            managed_transition_process_instance_id: None,
            control_endpoint,
            cloud_skill_provider,
            outgoing: outgoing_message_sender,
            analytics_events_client,
            arg0_paths,
            config: Arc::new(config),
            startup_cwd,
            config_manager,
            environment_manager,
            feedback: feedback.clone(),
            log_db,
            state_db: state_db.clone(),
            config_warnings,
            startup_warnings_tx,
            session_source,
            user_verification: Arc::new(crate::user_verification::Service::new(Arc::clone(
                &auth_manager,
            ))),
            auth_manager,
            runtime_auth_slot,
            installation_id,
            code_mode_session_provider,
            rpc_transport: analytics_rpc_transport(&transport),
            remote_control_handle: Some(remote_control_handle.clone()),
            plugin_startup_tasks: matches!(
                runtime_options.plugin_startup_tasks,
                PluginStartupTasks::Start
            )
            .then_some(plugin_startup_config),
        });
        processor.remote_control_processor.daemon_settings_file = daemon_settings_file;
        let processor = Arc::new(processor);
        standalone_shutdown::register_processor_intake(&owner_latch, &processor);
        let mut thread_created_rx = processor.thread_created_receiver();
        let mut running_turn_count_rx = processor.subscribe_running_assistant_turn_count();
        let mut active_admissions_rx = processor.turn_admission.subscribe_active();
        let mut connections = HashMap::<ConnectionId, ConnectionState>::new();
        let mut connection_cleanup_tasks =
            ConnectionCleanupTasks::with_expiry(owner_expiry.clone());
        let owner_latch = Arc::clone(&owner_latch);
        let owner_expired = owner_expiry.expired_token();
        let mut thread_listener_tasks = tokio::task::JoinSet::new();
        let mut remote_control_status_rx = remote_control_handle.status_receiver();
        let mut remote_control_status = remote_control_status_rx.borrow().clone();
        let transport_shutdown_token = transport_shutdown_token.clone();
        async move {
            processor.reconcile_incomplete_clear_transitions().await;
            let recovery_task = if managed_daemon {
                match daemon_thread_recovery::start_recovery(
                    recovery_file.clone(),
                    Arc::clone(&processor),
                )
                .await
                {
                    Ok(task) => Some(task),
                    Err(err) => {
                        warn!("failed to consume daemon recovery snapshot: {err}");
                        None
                    }
                }
            } else {
                None
            };
            let mut listen_for_threads = true;
            // Keep force signals and daemon control events responsive while saving.
            let mut snapshot = Box::pin(async {
                let processor = Arc::clone(&processor);
                let recovery_file = recovery_file.clone();
                // Run independently: snapshot locks can require the event loop to make progress.
                let task = tokio::spawn(async move {
                    let saved = processor.daemon_recovery_snapshot().await;
                    if let Err(err) = daemon_thread_recovery::snapshot(recovery_file, saved).await {
                        warn!("failed to save threads during daemon shutdown: {err}");
                    }
                });
                let _ = tokio_util::task::AbortOnDropHandle::new(task).await;
            });
            let mut snapshot_finished = !managed_daemon;
            let mut clients_disconnected = false;
            let mut shutdown_state = ShutdownState::default();
            let mut shutdown_signal_future = Box::pin(shutdown_signal());
            let mut shutdown_sends = standalone_shutdown::ShutdownSends::default();
            let exit_reason = loop {
                // First shutdown edge (signal or daemon shutdown): latch the
                // owner before any graceful drain, which then consumes it.
                if shutdown_state.requested() {
                    owner_latch.latch();
                }
                // Sample submissions first: one can publish a running turn before
                // releasing its permit, and shutdown must observe that new turn.
                let active_admissions = *active_admissions_rx.borrow_and_update();
                let running_turn_count = *running_turn_count_rx.borrow_and_update();
                let ready_to_exit = matches!(
                    shutdown_state.update(running_turn_count, active_admissions, connections.len()),
                    ShutdownAction::Finish
                );
                if ready_to_exit {
                    if let Some(task) = &recovery_task {
                        task.abort();
                    }
                    let finished = snapshot_finished || shutdown_state.forced();
                    if finished {
                        // Before cancelling: telemetry must see the owner.
                        owner_latch.latch();
                        transport_shutdown_token.cancel();
                    }
                    if managed_daemon && shutdown_state.forced() {
                        break "forced_shutdown_requested";
                    }
                    if !clients_disconnected {
                        let control = outbound_control_tx.clone();
                        let disconnect = async move {
                            control
                                .send(OutboundControlEvent::DisconnectAll)
                                .await
                                .is_ok()
                        };
                        if shutdown_sends
                            .submit(&owner_latch, "disconnect_all", disconnect)
                            .await
                            .is_none()
                        {
                            break "shutdown_deadline_expired";
                        }
                        clients_disconnected = true;
                    }
                    if finished {
                        break "shutdown_requested";
                    }
                }

                tokio::select! {
                    // The graceful drain consumes the same owner budget.
                    () = owner_expired.cancelled(), if shutdown_state.requested() => {
                        break "shutdown_deadline_expired";
                    }
                    _ = &mut snapshot, if shutdown_state.requested() && active_admissions == 0 && !snapshot_finished => {
                        snapshot_finished = true;
                    }
                    shutdown_signal_result = &mut shutdown_signal_future, if graceful_signal_restart_enabled && !shutdown_state.forced() => {
                        shutdown_signal_future.set(shutdown_signal());
                        let signal = match shutdown_signal_result {
                            Ok(signal) => signal,
                            Err(err) => {
                                warn!("failed to listen for shutdown signal during graceful restart drain: {err}");
                                continue;
                            }
                        };
                        let running_turn_count = *running_turn_count_rx.borrow();
                        shutdown_state.on_signal(signal, connections.len(), running_turn_count, &processor.turn_admission);
                    }
                    changed = running_turn_count_rx.changed(), if shutdown_state.requested() => {
                        if changed.is_err() {
                            warn!("running-turn watcher closed during graceful restart drain");
                        }
                    }
                    changed = active_admissions_rx.changed(), if shutdown_state.requested() => {
                        if changed.is_err() {
                            warn!("turn admission watcher closed during graceful restart drain");
                        }
                    }
                    event = transport_event_rx.recv() => {
                        let Some(event) = event else {
                            owner_latch.latch();
                            break "transport_channel_closed";
                        };
                        if ready_to_exit && !matches!(event, TransportEvent::DaemonShutdown) {
                            if let TransportEvent::ConnectionOpened { disconnect_sender: Some(token), .. } = event {
                                token.cancel();
                            }
                            continue;
                        }
                        match event {
                            TransportEvent::DaemonShutdown => {
                                shutdown_state.on_signal(ShutdownSignal::Forceable, connections.len(), *running_turn_count_rx.borrow(), &processor.turn_admission);
                            }
                            TransportEvent::ConnectionOpened {
                                connection_id,
                                origin,
                                provenance,
                                auth,
                                writer,
                                disconnect_sender,
                            } => {
                                let outbound_initialized = Arc::new(AtomicBool::new(false));
                                let outbound_experimental_api_enabled =
                                    Arc::new(AtomicBool::new(false));
                                let outbound_opted_out_notification_methods =
                                    Arc::new(RwLock::new(HashSet::new()));
                                let control = outbound_control_tx.clone();
                                let opened = OutboundControlEvent::Opened {
                                    connection_id,
                                    writer,
                                    disconnect_sender,
                                    initialized: Arc::clone(&outbound_initialized),
                                    experimental_api_enabled: Arc::clone(
                                        &outbound_experimental_api_enabled,
                                    ),
                                    opted_out_notification_methods: Arc::clone(
                                        &outbound_opted_out_notification_methods,
                                    ),
                                };
                                match shutdown_sends
                                    .submit(&owner_latch, "connection_opened", async move {
                                        control.send(opened).await.is_ok()
                                    })
                                    .await
                                {
                                    Some(true) => {}
                                    Some(false) => break "outbound_router_closed",
                                    None => break "shutdown_deadline_expired",
                                }
                                connections.insert(
                                    connection_id,
                                    ConnectionState::new(
                                        origin,
                                        auth,
                                        provenance,
                                        outbound_initialized,
                                        outbound_experimental_api_enabled,
                                        outbound_opted_out_notification_methods,
                                    ),
                                );
                            }
                            TransportEvent::ConnectionClosed { connection_id } => {
                                let Some(connection_state) = connections.remove(&connection_id) else {
                                    continue;
                                };
                                let stdio_closed = connection_state.origin == ConnectionOrigin::Stdio;
                                if single_client_mode && stdio_closed {
                                    // Stdio EOF is the shutdown edge: latch first.
                                    owner_latch.latch();
                                }
                                // Closes admission (inbound) only. Requests read
                                // but not admitted are refused explicitly.
                                connection_state.session.rpc_gate.close().await;
                                let half_close_deadline =
                                    (single_client_mode && stdio_closed).then(|| owner_latch.latch());
                                // Stdin EOF closes only the inbound half: keep the
                                // stdio output recipient registered so every accepted
                                // reply is still written. Only a positive signal in the
                                // writer (EPIPE/POLLHUP) means the recipient is gone.
                                let outbound_closed = if stdio_closed {
                                    Some(true)
                                } else {
                                    let control = outbound_control_tx.clone();
                                    shutdown_sends
                                        .submit(&owner_latch, "connection_closed", async move {
                                            control
                                                .send(OutboundControlEvent::Closed { connection_id })
                                                .await
                                                .is_ok()
                                        })
                                        .await
                                };
                                let processor = Arc::clone(&processor);
                                connection_cleanup_tasks.spawn(async move {
                                    let session = &connection_state.session;
                                    if stdio_closed {
                                        processor
                                            .connection_half_closed_until(
                                                connection_id,
                                                session,
                                                half_close_deadline,
                                            )
                                            .await;
                                    } else {
                                        processor.connection_closed(connection_id, session).await;
                                    }
                                });
                                match outbound_closed {
                                    Some(true) => {}
                                    Some(false) => break "outbound_router_closed",
                                    None => break "shutdown_deadline_expired",
                                }
                                if single_client_mode && stdio_closed {
                                    // Pending remote enrollment must stop before RPCs drain.
                                    remote_control_shutdown_token.cancel();
                                    break "stdio_connection_closed";
                                }
                            }
                            TransportEvent::IncomingMessage { connection_id, message } => {
                                let Some(connection_state) = connections.get_mut(&connection_id) else {
                                    warn!("dropping message from unknown connection: {connection_id:?}");
                                    if let JSONRPCMessage::Request(request) = message {
                                        let refusing = Arc::clone(&processor);
                                        let refused = shutdown_sends
                                            .submit(&owner_latch, "not_admitted_refusal", async move {
                                                refusing.refuse_not_admitted(connection_id, request.id).await
                                            })
                                            .await;
                                        if refused != Some(true) {
                                            owner_latch.expiry().record_abandoned(/*messages*/ 1, /*bytes*/ 0);
                                        }
                                    }
                                    continue;
                                };
                                if connection_state.session.rpc_gate.is_closed() {
                                    if let JSONRPCMessage::Request(request) = message {
                                        let refusing = Arc::clone(&processor);
                                        let refused = shutdown_sends
                                            .submit(&owner_latch, "not_admitted_refusal", async move {
                                                refusing.refuse_not_admitted(connection_id, request.id).await
                                            })
                                            .await;
                                        if refused != Some(true) {
                                            owner_latch.expiry().record_abandoned(/*messages*/ 1, /*bytes*/ 0);
                                        }
                                    }
                                    continue;
                                }
                                match message {
                                    JSONRPCMessage::Request(request) => {
                                        let was_initialized =
                                            connection_state.session.initialized();
                                        processor
                                            .process_request(
                                                connection_id,
                                                request,
                                                &transport,
                                                Arc::clone(&connection_state.session),
                                            )
                                            .await;
                                        let opted_out_notification_methods_snapshot = connection_state
                                            .session
                                            .opted_out_notification_methods();
                                        let experimental_api_enabled =
                                            connection_state.session.experimental_api_enabled();
                                        let is_initialized = connection_state.session.initialized();
                                        if let Ok(mut opted_out_notification_methods) = connection_state
                                            .outbound_opted_out_notification_methods
                                            .write()
                                        {
                                            *opted_out_notification_methods =
                                                opted_out_notification_methods_snapshot;
                                        } else {
                                            warn!(
                                                "failed to update outbound opted-out notifications"
                                            );
                                        }
                                        connection_state
                                            .outbound_experimental_api_enabled
                                            .store(
                                                experimental_api_enabled,
                                                std::sync::atomic::Ordering::Release,
                                            );
                                        if !was_initialized && is_initialized {
                                            processor
                                                .send_initialize_notifications_to_connection(
                                                    connection_id,
                                                )
                                                .await;
                                            initialize_notification_sender
                                                .send_server_notification_to_connections(
                                                    &[connection_id],
                                                    ServerNotification::RemoteControlStatusChanged(
                                                        remote_control_status.clone(),
                                                    ),
                                                )
                                                .await;
                                            processor
                                                .connection_initialized(connection_id, &connection_state.session)
                                                .await;
                                            connection_state
                                                .outbound_initialized
                                                .store(true, std::sync::atomic::Ordering::Release);
                                        }
                                    }
                                    JSONRPCMessage::Response(response) => {
                                        processor.process_response(connection_id, response).await;
                                    }
                                    JSONRPCMessage::Notification(notification) => {
                                        processor.process_notification(notification).await;
                                    }
                                    JSONRPCMessage::Error(err) => {
                                        processor.process_error(connection_id, err).await;
                                    }
                                }
                            }
                        }
                    }
                    Some(message) = startup_warnings_rx.recv(), if connections.values().any(|state| state.session.initialized()) => {
                        startup_warnings::send_warning(&initialize_notification_sender, message).await;
                    }
                    _ = connection_cleanup_tasks.reap_next() => {}
                    result = thread_listener_tasks.join_next(), if !thread_listener_tasks.is_empty() => {
                        if let Some(Err(err)) = result {
                            warn!("thread listener attachment failed: {err}");
                        }
                    }
                    changed = remote_control_status_rx.changed() => {
                        if changed.is_err() {
                            continue;
                        }
                        let status = remote_control_status_rx.borrow().clone();
                        if remote_control_status == status {
                            continue;
                        }
                        remote_control_status = status.clone();
                        let notification = ServerNotification::RemoteControlStatusChanged(status);
                        initialize_notification_sender
                            .send_server_notification(notification)
                            .await;
                    }
                    created = thread_created_rx.recv(), if listen_for_threads && !ready_to_exit => {
                        match created {
                            Ok(thread_id) => {
                                let mut initialized_connection_ids = Vec::new();
                                for (connection_id, connection_state) in &connections {
                                    if connection_state.session.initialized() {
                                        initialized_connection_ids.push(*connection_id);
                                    }
                                }
                                let processor = Arc::clone(&processor);
                                // Attachment can wait on snapshot locks; keep force shutdown responsive.
                                thread_listener_tasks.spawn(async move {
                                    processor
                                        .try_attach_thread_listener(
                                            thread_id,
                                            initialized_connection_ids,
                                        )
                                        .await;
                                });
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                // TODO(jif) handle lag.
                                // Assumes thread creation volume is low enough that lag never happens.
                                // If it does, we log and continue without resyncing to avoid attaching
                                // listeners for threads that should remain unsubscribed.
                                warn!("thread_created receiver lagged; skipping resync");
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                listen_for_threads = false;
                            }
                        }
                    }
                }
            };

            // Admission is over. Frames already read but never admitted are
            // refused explicitly; closing first makes every later send fail
            // in the transport, which refuses those frames itself.
            transport_event_rx.close();
            while let Ok(event) = transport_event_rx.try_recv() {
                if let TransportEvent::IncomingMessage {
                    connection_id,
                    message: JSONRPCMessage::Request(request),
                } = event
                {
                    let refusing = Arc::clone(&processor);
                    let refused = shutdown_sends
                        .submit(&owner_latch, "not_admitted_refusal", async move {
                            refusing
                                .refuse_not_admitted(connection_id, request.id)
                                .await
                        })
                        .await;
                    if refused != Some(true) {
                        owner_latch
                            .expiry()
                            .record_abandoned(/*messages*/ 1, /*bytes*/ 0);
                    }
                }
            }
            if let Some(task) = recovery_task {
                task.abort();
            }
            drop(snapshot);
            drop(thread_listener_tasks);
            let remaining_connection_count = connections.len();
            let mut owner = StandaloneShutdownOwner::new(owner_latch.latch(), owner_latch.expiry());
            processor.retire_command_exec_sandbox_cleanup(Some(owner.deadline().into_std()));
            if !shutdown_state.forced() {
                let mut stages = ProcessorShutdownStages {
                    processor: Arc::clone(&processor),
                    connections: connections.drain().collect(),
                    cleanup: Some(connection_cleanup_tasks),
                    thread_retirement: None,
                    expiry: owner_latch.expiry(),
                };
                standalone_shutdown::drain_processor_stages(
                    &mut owner,
                    &mut stages,
                    STANDALONE_STAGE_CAPS,
                )
                .await;
                let ProcessorShutdownStages {
                    processor: stages_processor,
                    cleanup,
                    thread_retirement,
                    ..
                } = stages;
                drop(stages_processor);
                owner.retain((cleanup, thread_retirement));
            } else {
                connection_cleanup_tasks.abort();
                owner.record("forced", false);
            }
            shutdown_sends.hand_to(&mut owner);
            standalone_shutdown::submit_close_all(
                &mut owner,
                &outbound_control_tx,
                OutboundControlEvent::CloseAllAfterQueued,
            )
            .await;
            info!(
                exit_reason,
                remaining_connection_count,
                shutdown_forced = shutdown_state.forced(),
                "processor task exited"
            );
            if managed_daemon && shutdown_state.forced() {
                (AppServerExit::Forced, Some(owner))
            } else {
                (AppServerExit::Graceful, Some(owner))
            }
        }
    });

    drop(transport_event_tx);

    let mut shutdown_owner = match standalone_shutdown::join_processor(
        &owner_latch,
        Some(host_custody),
        processor_handle,
    )
    .await
    {
        standalone_shutdown::ProcessorJoin::Forced => return Ok(AppServerExit::Forced),
        standalone_shutdown::ProcessorJoin::Owner(owner) => owner,
    };
    let exit = standalone_shutdown::finish_host_shutdown(
        &mut shutdown_owner,
        outbound_handle,
        || transport_shutdown_token.cancel(),
        otel_reloader_handle,
        |reloader| reloader.shutdown_result == Some(Ok(())),
        futures::future::join_all(transport_accept_handles),
    )
    .await;
    drop(shutdown_owner);
    Ok(exit)
}

struct SqliteRecoveryNotice {
    details: String,
}

struct RecoveredSqliteDatabase {
    database_path: String,
    backup_folder: String,
}

struct StateDbInitResult {
    state_db: Option<rollout_state_db::StateDbHandle>,
    recovery_notice: Option<SqliteRecoveryNotice>,
}

async fn init_sqlite_state_db_with_fresh_start_on_corruption(
    config: &Config,
) -> anyhow::Result<StateDbInitResult> {
    let (result, backups) = codex_state::collect_runtime_db_backups(async {
        let mut attempted_backups = HashSet::new();
        loop {
            let err = match rollout_state_db::try_init(config).await {
                Ok(state_db) => return Ok(state_db),
                Err(err) => err,
            };
            let database_path = codex_state::runtime_db_path_for_corruption_error(&err)
                .unwrap_or_else(|| config.sqlite_config().state_db_path());
            if !codex_state::is_sqlite_corruption_error(&err)
                && !sqlite_home_is_blocking_file(database_path.as_path())
            {
                return Err(err);
            }

            if !attempted_backups.insert(database_path.clone()) {
                return Err(anyhow::anyhow!(
                    "failed to initialize sqlite state runtime after moving damaged database file into a backup folder: {err}"
                ));
            }

            let original_error = err.to_string();
            emit_state_db_backup_warning(&format!(
                "Codex local database at {} appears damaged. Moving it into a backup folder so the app server can rebuild it from saved data.",
                database_path.display()
            ));
            let backups = codex_state::backup_runtime_db_for_fresh_start(database_path.as_path())
                .await
                .map_err(|backup_err| {
                    anyhow::anyhow!(
                        "failed to move damaged sqlite state database files into a backup folder: {backup_err}; original error: {original_error}"
                    )
                })?;
            for backup in &backups {
                emit_state_db_backup_warning(&format!(
                    "Moved damaged Codex local database file {} to {}",
                    backup.original_path.display(),
                    backup.backup_path.display()
                ));
            }
        }
    })
    .await;
    let state_db = result?;
    let mut recovered_databases = Vec::new();
    let mut backup_folders = HashSet::new();
    for backup in backups {
        if let Some(folder) = backup.backup_path.parent()
            && backup_folders.insert(folder.to_path_buf())
        {
            recovered_databases.push(RecoveredSqliteDatabase {
                database_path: backup.original_path.display().to_string(),
                backup_folder: folder.display().to_string(),
            });
        }
    }
    let recovery_notice = sqlite_recovery_notice(&recovered_databases);
    if let Some(notice) = &recovery_notice {
        emit_state_db_backup_warning(SQLITE_RECOVERY_CONFIG_WARNING_SUMMARY);
        emit_state_db_backup_warning(&notice.details);
    }
    Ok(StateDbInitResult {
        state_db: Some(state_db),
        recovery_notice,
    })
}

fn sqlite_home_is_blocking_file(database_path: &Path) -> bool {
    database_path
        .parent()
        .and_then(|path| std::fs::metadata(path).ok())
        .is_some_and(|metadata| metadata.is_file())
}

fn sqlite_recovery_notice(
    recovered_databases: &[RecoveredSqliteDatabase],
) -> Option<SqliteRecoveryNotice> {
    if recovered_databases.is_empty() {
        return None;
    }

    let details = recovered_databases
        .iter()
        .map(|recovered_database| {
            format!(
                "Database path: {}\nBackup folder: {}",
                recovered_database.database_path, recovered_database.backup_folder
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    Some(SqliteRecoveryNotice {
        details: format!(
            "Damaged local databases were rebuilt. Saved conversations remain in rollout files and can restore the thread list and history. Some database-only metadata may be unavailable. The original database files were preserved at the backup locations below.\n\n{details}"
        ),
    })
}

fn emit_state_db_backup_warning(message: &str) {
    warn!("{message}");
    if !tracing::dispatcher::has_been_set() {
        let _ =
            codex_app_server_daemon::stderr_writer().write_all(format!("{message}\n").as_bytes());
    }
}

fn test_user_config_file_from_env() -> Option<std::path::PathBuf> {
    #[cfg(debug_assertions)]
    {
        std::env::var_os(TEST_USER_CONFIG_FILE_ENV_VAR)
            .filter(|value| !value.is_empty())
            .map(std::path::PathBuf::from)
    }

    #[cfg(not(debug_assertions))]
    None
}

fn loader_overrides_with_test_user_config_file(
    mut loader_overrides: LoaderOverrides,
    test_user_config_file: Option<std::path::PathBuf>,
) -> IoResult<LoaderOverrides> {
    #[cfg(debug_assertions)]
    if let Some(path) = test_user_config_file {
        let path = AbsolutePathBuf::from_absolute_path(path).map_err(|err| {
            std::io::Error::new(
                ErrorKind::InvalidInput,
                format!("invalid test user config path: {err}"),
            )
        })?;
        warn!(
            path = %path.as_path().display(),
            "using debug-only app-server test user config file"
        );
        loader_overrides.user_config_path = Some(path);
    }

    #[cfg(not(debug_assertions))]
    let _ = test_user_config_file;

    Ok(loader_overrides)
}

fn analytics_rpc_transport(transport: &AppServerTransport) -> AppServerRpcTransport {
    match transport {
        AppServerTransport::Stdio => AppServerRpcTransport::Stdio,
        AppServerTransport::UnixSocket { .. }
        | AppServerTransport::WebSocket { .. }
        | AppServerTransport::Off => AppServerRpcTransport::Websocket,
    }
}

/// Derive disclosure only from the actual hosting absolute Unix listener.
/// Other transports and relative paths provide no endpoint authority.
fn mcp_control_endpoint_uri(transport: &AppServerTransport) -> Option<String> {
    let AppServerTransport::UnixSocket { socket_path } = transport else {
        return None;
    };
    if !socket_path.is_absolute() {
        return None;
    }
    let path = socket_path.to_str()?;
    let mut endpoint = url::Url::parse("unix:///").ok()?;
    endpoint.set_path(path);
    Some(endpoint.to_string())
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;

#[cfg(test)]
mod shutdown_outcome_tests {
    use super::AppServerExit;
    #[test]
    fn incomplete_shutdown_is_an_error_without_forcing_the_host() {
        assert!(AppServerExit::Graceful.into_shutdown_result().is_ok());
        assert!(AppServerExit::Forced.into_shutdown_result().is_ok());
        let error = AppServerExit::ShutdownIncomplete
            .into_shutdown_result()
            .unwrap_err();
        assert!(error.to_string().contains("shutdown incomplete"));
    }
}
