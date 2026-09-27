//! Launch MCP stdio servers and return the transport rmcp should use.
//!
//! This module owns the "where does the server process run?" decision:
//!
//! - [`LocalStdioServerLauncher`] starts the configured command as a child of
//!   the orchestrator process.
//! - [`ExecutorStdioServerLauncher`] starts the configured command through the
//!   executor process API.
//!
//! Both paths return [`StdioServerTransport`], so `RmcpClient` can hand the
//! resulting byte stream to rmcp without knowing where the process lives. The
//! executor-specific byte adaptation lives in `executor_process_transport`.

use std::collections::HashMap;
use std::ffi::OsString;
use std::future::Future;
use std::io;
#[cfg(windows)]
use std::os::windows::io::OwnedHandle;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
#[cfg(unix)]
use std::thread::sleep;
#[cfg(unix)]
use std::thread::spawn;
use std::time::Duration;

use anyhow::Result;
use anyhow::anyhow;
use codex_config::types::McpServerEnvVar;
use codex_exec_server::ExecBackend;
use codex_exec_server::ExecEnvPolicy;
use codex_exec_server::ExecParams;
use codex_exec_server::ExecProcess;
use codex_exec_server::ExecProcessEvent;
use codex_exec_server::ExecProcessEventReceiver;
use codex_protocol::config_types::ShellEnvironmentPolicyInherit;
use codex_utils_path_uri::LegacyAppPathString;
use codex_utils_path_uri::PathUri;
use codex_utils_pty::Command;
use codex_utils_pty::ProcessMode;
#[cfg(unix)]
use codex_utils_pty::process_group::kill_process_group;
#[cfg(unix)]
use codex_utils_pty::process_group::terminate_process_group;
use futures::FutureExt;
use futures::future::BoxFuture;
use rmcp::service::RoleClient;
use rmcp::service::RxJsonRpcMessage;
use rmcp::service::TxJsonRpcMessage;
use rmcp::transport::Transport;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::sync::broadcast;
use tokio::sync::watch;
use tokio::time::Instant;
use tracing::info;
use tracing::warn;

use crate::executor_process_transport::ExecutorProcessTransport;
use crate::local_stdio_transport::LocalProcessExitObserver;
use crate::local_stdio_transport::LocalStdioTransport;
use crate::program_resolver;
use crate::protocol_mode::McpProtocolMode;
use crate::utils::create_env_for_mcp_server;
use crate::utils::create_env_overlay_for_remote_mcp_server;
use crate::utils::remote_mcp_env_var_names;

// General purpose public code.

/// Launches an MCP stdio server and returns the transport for rmcp.
///
/// This trait is the boundary between MCP lifecycle code and process placement.
/// `RmcpClient` owns MCP operations such as `initialize` and `tools/list`; the
/// launcher owns starting the configured command and producing an rmcp
/// [`Transport`] over the server's stdin/stdout bytes.
pub trait StdioServerLauncher: private::Sealed + Send + Sync {
    /// Start the configured stdio server and return its rmcp-facing transport.
    fn launch(
        &self,
        command: StdioServerCommand,
    ) -> BoxFuture<'static, io::Result<StdioServerTransport>>;
}

/// Command-line process shape shared by stdio server launchers.
#[derive(Clone)]
pub struct StdioServerCommand {
    program: OsString,
    args: Vec<OsString>,
    env: Option<HashMap<OsString, OsString>>,
    env_vars: Vec<McpServerEnvVar>,
    cwd: Option<String>,
    protocol_mode: McpProtocolMode,
}

/// Client-side rmcp transport for a launched MCP stdio server.
///
/// The concrete process placement stays private to this module. `RmcpClient`
/// only sees the standard rmcp transport abstraction and can pass this value
/// directly to `rmcp::service::serve_client`.
pub struct StdioServerTransport {
    inner: StdioServerTransportInner,
    process: StdioServerProcessHandle,
}

enum StdioServerTransportInner {
    Local(LocalStdioTransport),
    Executor(ExecutorProcessTransport),
}

impl Transport<RoleClient> for StdioServerTransport {
    type Error = io::Error;

    fn send(
        &mut self,
        item: TxJsonRpcMessage<RoleClient>,
    ) -> impl Future<Output = std::result::Result<(), Self::Error>> + Send + 'static {
        // Both variants already implement rmcp's transport contract. This
        // wrapper keeps process placement private while leaving rmcp's send
        // semantics unchanged.
        match &mut self.inner {
            StdioServerTransportInner::Local(transport) => transport.send(item).boxed(),
            StdioServerTransportInner::Executor(transport) => transport.send(item).boxed(),
        }
    }

    fn receive(&mut self) -> impl Future<Output = Option<RxJsonRpcMessage<RoleClient>>> + Send {
        // rmcp reads from the same transport shape for both placements. The
        // executor variant turns pushed process-output events back into the
        // line-delimited JSON stream expected by rmcp.
        match &mut self.inner {
            StdioServerTransportInner::Local(transport) => transport.receive().boxed(),
            StdioServerTransportInner::Executor(transport) => transport.receive().boxed(),
        }
    }

    async fn close(&mut self) -> std::result::Result<(), Self::Error> {
        // Preserve termination-first ordering: a sender can hold the input
        // mutex while blocked on a child that no longer reads its pipe.
        // Waiting for that mutex before terminating would bypass the deadline.
        self.process.terminate().await?;
        match &mut self.inner {
            StdioServerTransportInner::Local(transport) => {
                transport.close_after_terminal_observation().await
            }
            StdioServerTransportInner::Executor(transport) => {
                transport.mark_closed_after_terminal_observation();
                Ok(())
            }
        }
    }
}

impl StdioServerTransport {
    pub(crate) fn process_handle(&self) -> StdioServerProcessHandle {
        self.process.clone()
    }
}

impl StdioServerCommand {
    /// Build the stdio process parameters before choosing where the process
    /// runs.
    pub(super) fn new(
        program: OsString,
        args: Vec<OsString>,
        env: Option<HashMap<OsString, OsString>>,
        env_vars: Vec<McpServerEnvVar>,
        cwd: Option<String>,
        protocol_mode: McpProtocolMode,
    ) -> Self {
        Self {
            program,
            args,
            env,
            env_vars,
            cwd,
            protocol_mode,
        }
    }
}

// Local public implementation.

/// Starts MCP stdio servers as local child processes.
///
/// This is the existing behavior for local MCP servers: the orchestrator
/// process spawns the configured command and rmcp talks to the child's local
/// stdin/stdout pipes directly.
#[derive(Clone)]
pub struct LocalStdioServerLauncher {
    fallback_cwd: PathBuf,
}

impl LocalStdioServerLauncher {
    /// Creates a local stdio launcher.
    ///
    /// `fallback_cwd` is used when the MCP server config omits `cwd`, so
    /// relative commands resolve from the caller's runtime working directory.
    pub fn new(fallback_cwd: PathBuf) -> Self {
        Self { fallback_cwd }
    }
}

impl StdioServerLauncher for LocalStdioServerLauncher {
    fn launch(
        &self,
        command: StdioServerCommand,
    ) -> BoxFuture<'static, io::Result<StdioServerTransport>> {
        let fallback_cwd = self.fallback_cwd.clone();
        async move {
            // Keep synchronous program resolution and process creation from blocking the
            // caller's startup deadline.
            tokio::task::spawn_blocking(move || Self::launch_server(command, fallback_cwd))
                .await
                .map_err(io::Error::other)?
        }
        .boxed()
    }
}

// Local private implementation.

#[cfg(unix)]
const PROCESS_GROUP_TERM_GRACE_PERIOD: Duration = Duration::from_secs(2);

// Keep queued stderr diagnostics before closing the reader, even when an
// escaped descendant prevents the pipe from reaching EOF.
const STDERR_READER_DRAIN_GRACE_PERIOD: Duration = Duration::from_millis(250);
pub(super) const PROCESS_RETIREMENT_TIMEOUT: Duration = Duration::from_secs(3);

#[cfg(unix)]
struct LocalProcessTerminator {
    process_group_id: u32,
}

#[cfg(windows)]
enum LocalProcessTerminator {
    Job(codex_utils_pty::JobObject),
    Process(OwnedHandle),
}

#[cfg(not(any(unix, windows)))]
struct LocalProcessTerminator;

#[derive(Clone)]
pub(crate) struct StdioServerProcessHandle {
    inner: Arc<StdioServerProcessHandleInner>,
}

struct StdioServerProcessHandleInner {
    program_name: String,
    kind: StdioServerProcessKind,
    terminal_observed: AtomicBool,
    termination_lock: tokio::sync::Mutex<()>,
    // An escaped descendant can keep stderr open after the MCP server exits.
    stderr_reader: Option<watch::Sender<()>>,
}

enum StdioServerProcessKind {
    Local {
        terminator: Option<LocalProcessTerminator>,
        exit_observer: LocalProcessExitObserver,
    },
    Executor(Arc<dyn ExecProcess>),
}

mod private {
    pub trait Sealed {}
}

impl private::Sealed for LocalStdioServerLauncher {}

impl LocalStdioServerLauncher {
    fn launch_server(
        command: StdioServerCommand,
        fallback_cwd: PathBuf,
    ) -> io::Result<StdioServerTransport> {
        let StdioServerCommand {
            program,
            args,
            env,
            env_vars,
            cwd,
            protocol_mode,
        } = command;
        let program_name = program.to_string_lossy().into_owned();
        let envs = create_env_for_mcp_server(env, &env_vars).map_err(io::Error::other)?;
        let cwd = cwd.map(PathBuf::from).unwrap_or(fallback_cwd);
        let resolved_program =
            program_resolver::resolve(program, &envs, &cwd).map_err(io::Error::other)?;

        let build_command = || {
            let mut command = Command::new(&resolved_program);
            command.current_dir(&cwd).envs(&envs).args(&args);
            command.process_mode(ProcessMode::NewGroup);
            command
        };
        #[cfg(windows)]
        let mut command = build_command();
        #[cfg(not(windows))]
        let command = build_command();
        #[cfg(windows)]
        let job = match codex_utils_pty::JobObject::create_without_breakaway() {
            Ok(job) => {
                command.prepare_suspended_spawn(&job);
                Some(job)
            }
            Err(error) => {
                warn!("Windows MCP process job containment unavailable: {error}");
                None
            }
        };

        let spawn_transport = |command: Command| -> io::Result<(
            StdioServerTransportInner,
            Option<tokio::process::ChildStderr>,
            Option<u32>,
            LocalProcessExitObserver,
        )> {
            let (transport, stderr) =
                LocalStdioTransport::spawn(command, program_name.clone(), protocol_mode)?;
            let process_id = transport.id();
            let exit_observer = transport.exit_observer();
            Ok((
                StdioServerTransportInner::Local(transport),
                stderr,
                process_id,
                exit_observer,
            ))
        };
        let (transport, stderr, process_id, exit_observer) = spawn_transport(command)?;
        #[cfg(windows)]
        let (transport, stderr, process_id, exit_observer, job) = match job {
            Some(job) => match process_id
                .ok_or_else(|| io::Error::other("missing suspended MCP server process id"))
                .and_then(|process_id| job.assign_and_resume_process(process_id))
            {
                Ok(true) => (transport, stderr, process_id, exit_observer, Some(job)),
                Ok(false) => (transport, stderr, process_id, exit_observer, None),
                Err(error) => {
                    warn!(
                        "Windows MCP process job containment failed; retrying without it: {error}"
                    );
                    drop(stderr);
                    drop(transport);
                    drop(job);
                    let (transport, stderr, process_id, exit_observer) =
                        spawn_transport(build_command())?;
                    (transport, stderr, process_id, exit_observer, None)
                }
            },
            None => (transport, stderr, process_id, exit_observer, None),
        };
        #[cfg(windows)]
        let terminator = match job {
            Some(job) => Some(LocalProcessTerminator::Job(job)),
            None => process_id.and_then(|process_id| {
                match codex_utils_pty::JobObject::open_process_handle(process_id) {
                    Ok(handle) => Some(LocalProcessTerminator::Process(handle)),
                    Err(error) => {
                        warn!("Windows MCP process handle unavailable: {error}");
                        None
                    }
                }
            }),
        };
        #[cfg(not(windows))]
        let terminator = process_id.map(LocalProcessTerminator::new);
        let stderr_reader = stderr.map(|stderr| {
            let program_name = program_name.clone();
            let (stop_tx, mut stop_rx) = watch::channel(());
            std::mem::drop(tokio::spawn(async move {
                let mut reader = BufReader::new(stderr).lines();
                // Give queued diagnostics time to reach the logs without waiting
                // indefinitely for a descendant that still has stderr open.
                let drain_deadline = tokio::time::sleep(STDERR_READER_DRAIN_GRACE_PERIOD);
                tokio::pin!(drain_deadline);
                let mut draining = false;
                loop {
                    tokio::select! {
                        biased;
                        _ = &mut drain_deadline, if draining => break,
                        _ = stop_rx.changed(), if !draining => {
                            draining = true;
                            drain_deadline.as_mut().reset(
                                Instant::now() + STDERR_READER_DRAIN_GRACE_PERIOD
                            );
                        }
                        line = reader.next_line() => {
                            match line {
                                Ok(Some(line)) => {
                                    info!("MCP server stderr ({program_name}): {line}");
                                }
                                Ok(None) => break,
                                Err(error) => {
                                    warn!("Failed to read MCP server stderr ({program_name}): {error}");
                                    break;
                                }
                            }
                        },
                    }
                }
            }));
            stop_tx
        });
        let process =
            StdioServerProcessHandle::local(program_name, terminator, exit_observer, stderr_reader);

        Ok(StdioServerTransport {
            inner: transport,
            process,
        })
    }
}

impl LocalProcessTerminator {
    /// Unlike Drop's best-effort signal, explicit retirement observes the
    /// complete existing process-group/job obligation before returning.
    async fn terminate_and_observe(&self) -> io::Result<()> {
        #[cfg(unix)]
        {
            terminate_process_group(self.process_group_id)?;
            let escalation_at = tokio::time::Instant::now() + PROCESS_GROUP_TERM_GRACE_PERIOD;
            let mut escalated = false;
            while codex_utils_pty::process_group::process_group_exists(self.process_group_id)? {
                if !escalated && tokio::time::Instant::now() >= escalation_at {
                    kill_process_group(self.process_group_id)?;
                    escalated = true;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok(())
        }
        #[cfg(windows)]
        {
            match self {
                Self::Job(job) => {
                    job.terminate()?;
                    while job.has_active_processes()? {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Ok(())
                }
                Self::Process(handle) => {
                    codex_utils_pty::JobObject::terminate_process_handle(handle)?;
                    Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "MCP process lacks job-level terminal evidence",
                    ))
                }
            }
        }
        #[cfg(not(any(unix, windows)))]
        {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "MCP process-group observation is unsupported",
            ))
        }
    }

    #[cfg(not(windows))]
    fn new(process_group_id: u32) -> Self {
        #[cfg(unix)]
        {
            Self { process_group_id }
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = process_group_id;
            Self
        }
    }

    #[cfg(unix)]
    fn terminate(&self) {
        let process_group_id = self.process_group_id;
        let should_escalate = match terminate_process_group(process_group_id) {
            Ok(exists) => exists,
            Err(error) => {
                warn!("Failed to terminate MCP process group {process_group_id}: {error}");
                false
            }
        };
        if should_escalate {
            spawn(move || {
                sleep(PROCESS_GROUP_TERM_GRACE_PERIOD);
                if let Err(error) = kill_process_group(process_group_id) {
                    warn!("Failed to kill MCP process group {process_group_id}: {error}");
                }
            });
        }
    }

    #[cfg(windows)]
    fn terminate(&self) {
        let result = match self {
            Self::Job(job) => job.terminate(),
            Self::Process(process_handle) => {
                codex_utils_pty::JobObject::terminate_process_handle(process_handle)
            }
        };
        if let Err(error) = result {
            warn!("Failed to terminate Windows MCP process: {error}");
        }
    }

    #[cfg(not(any(unix, windows)))]
    fn terminate(&self) {}
}

impl StdioServerProcessHandle {
    fn local(
        program_name: String,
        terminator: Option<LocalProcessTerminator>,
        exit_observer: LocalProcessExitObserver,
        stderr_reader: Option<watch::Sender<()>>,
    ) -> Self {
        Self {
            inner: Arc::new(StdioServerProcessHandleInner {
                program_name,
                kind: StdioServerProcessKind::Local {
                    terminator,
                    exit_observer,
                },
                terminal_observed: AtomicBool::new(false),
                termination_lock: tokio::sync::Mutex::new(()),
                stderr_reader,
            }),
        }
    }

    pub(crate) fn executor(program_name: String, process: Arc<dyn ExecProcess>) -> Self {
        Self {
            inner: Arc::new(StdioServerProcessHandleInner {
                program_name,
                kind: StdioServerProcessKind::Executor(process),
                terminal_observed: AtomicBool::new(false),
                termination_lock: tokio::sync::Mutex::new(()),
                stderr_reader: None,
            }),
        }
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "the async attempt lock serializes bounded termination observations, not shared runtime state"
    )]
    pub(crate) async fn terminate(&self) -> io::Result<()> {
        self.terminate_until(tokio::time::Instant::now() + PROCESS_RETIREMENT_TIMEOUT)
            .await
    }

    /// Terminates and observes the process using the caller's absolute
    /// deadline. Retirements with an existing bound must not mint a nested
    /// three-second budget here.
    #[expect(
        clippy::await_holding_invalid_type,
        reason = "the async attempt lock serializes bounded termination observations, not shared runtime state"
    )]
    pub(crate) async fn terminate_until(&self, deadline: Instant) -> io::Result<()> {
        if self.inner.terminal_observed.load(Ordering::Acquire) {
            return Ok(());
        }
        let _attempt = tokio::time::timeout_at(deadline, self.inner.termination_lock.lock())
            .await
            .map_err(|_| {
                io::Error::new(io::ErrorKind::TimedOut, "MCP retirement lock timed out")
            })?;
        if self.inner.terminal_observed.load(Ordering::Acquire) {
            return Ok(());
        }
        let result = async {
            match &self.inner.kind {
                StdioServerProcessKind::Local {
                    terminator,
                    exit_observer,
                } => {
                    let Some(terminator) = terminator else {
                        exit_observer.abort();
                        return Err(io::Error::other(
                            "local MCP process has no process-group terminal evidence",
                        ));
                    };
                    let mut exit_observer = exit_observer.clone();
                    tokio::time::timeout_at(deadline, async {
                        tokio::try_join!(exit_observer.wait(), terminator.terminate_and_observe())?;
                        Ok::<(), io::Error>(())
                    })
                    .await
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::TimedOut, "local MCP exit timed out")
                    })??;
                    self.inner.terminal_observed.store(true, Ordering::Release);
                    Ok(())
                }
                StdioServerProcessKind::Executor(process) => {
                    // Subscribe before the request so a quickly-closing process is
                    // observed through replay rather than mistaken request acceptance.
                    let mut events = process.subscribe_events();
                    // A previous cancelled/failed attempt is not terminal proof.
                    // Resend the idempotent request; successful observation is
                    // retained by this handle independently of bounded replay.
                    tokio::time::timeout_at(deadline, async {
                        process.terminate().await.map_err(io::Error::other)?;
                        await_executor_process_close(&mut events).await
                    })
                    .await
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::TimedOut, "executor MCP close timed out")
                    })??;
                    self.inner.terminal_observed.store(true, Ordering::Release);
                    Ok(())
                }
            }
        }
        .await;
        if let Some(stderr_reader) = &self.inner.stderr_reader {
            stderr_reader.send_replace(());
        }
        result
    }
}

pub(super) async fn await_executor_process_close(
    events: &mut ExecProcessEventReceiver,
) -> io::Result<()> {
    loop {
        if executor_event_proves_close(events.recv().await)? {
            return Ok(());
        }
    }
}

fn executor_event_proves_close(
    event: Result<ExecProcessEvent, broadcast::error::RecvError>,
) -> io::Result<bool> {
    match event {
        Ok(ExecProcessEvent::Closed { .. }) => Ok(true),
        // Exit alone does not prove the output stream has finished. Wait
        // for Closed so stdout cannot still contain an MCP response.
        Ok(ExecProcessEvent::Exited { .. } | ExecProcessEvent::Output(_)) => Ok(false),
        Ok(ExecProcessEvent::Failed(message)) => Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            format!("executor MCP event stream failed before close: {message}"),
        )),
        Err(broadcast::error::RecvError::Lagged(count)) => Err(io::Error::other(format!(
            "executor MCP event stream lagged by {count} events before close"
        ))),
        Err(broadcast::error::RecvError::Closed) => Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "executor MCP event stream closed before process close",
        )),
    }
}

impl Drop for StdioServerProcessHandleInner {
    fn drop(&mut self) {
        if self.terminal_observed.load(Ordering::Acquire) {
            return;
        }

        match &self.kind {
            StdioServerProcessKind::Local {
                terminator: Some(terminator),
                ..
            } => {
                terminator.terminate();
            }
            StdioServerProcessKind::Local {
                terminator: None,
                exit_observer,
            } => exit_observer.abort(),
            StdioServerProcessKind::Executor(process) => {
                let process = Arc::clone(process);
                let program_name = self.program_name.clone();
                let Ok(handle) = tokio::runtime::Handle::try_current() else {
                    warn!(
                        "Could not schedule remote MCP server process termination on drop ({}): no Tokio runtime is available",
                        self.program_name
                    );
                    return;
                };

                std::mem::drop(handle.spawn(async move {
                    if let Err(error) = process.terminate().await {
                        warn!(
                            "Failed to terminate remote MCP server process on drop ({program_name}): {error}"
                        );
                    }
                }));
            }
        }
        if let Some(stderr_reader) = &self.stderr_reader {
            stderr_reader.send_replace(());
        }
    }
}

// Remote public implementation.

/// Starts MCP stdio servers through the executor process API.
///
/// MCP framing still runs in the orchestrator. The executor only owns the
/// child process and transports raw stdin/stdout/stderr bytes, so it does not
/// need to know about MCP methods such as `initialize` or `tools/list`.
///
/// Windows executor-backed servers retain the executor's normal descendant
/// lifetime. MCP-specific containment requires negotiated process ownership:
/// caller-controlled process IDs cannot safely select a destructive policy,
/// and a wrapper may exit while its descendants continue serving requests.
#[derive(Clone)]
pub struct ExecutorStdioServerLauncher {
    exec_backend: Arc<dyn ExecBackend>,
}

impl ExecutorStdioServerLauncher {
    /// Creates a stdio server launcher backed by the executor process API.
    pub fn new(exec_backend: Arc<dyn ExecBackend>) -> Self {
        Self { exec_backend }
    }
}

impl StdioServerLauncher for ExecutorStdioServerLauncher {
    fn launch(
        &self,
        command: StdioServerCommand,
    ) -> BoxFuture<'static, io::Result<StdioServerTransport>> {
        let exec_backend = Arc::clone(&self.exec_backend);
        async move { Self::launch_server(command, exec_backend).await }.boxed()
    }
}

// Remote private implementation.

impl private::Sealed for ExecutorStdioServerLauncher {}

impl ExecutorStdioServerLauncher {
    async fn launch_server(
        command: StdioServerCommand,
        exec_backend: Arc<dyn ExecBackend>,
    ) -> io::Result<StdioServerTransport> {
        let StdioServerCommand {
            program,
            args,
            env,
            env_vars,
            cwd,
            protocol_mode: _,
        } = command;
        let Some(cwd) = cwd else {
            return Err(io::Error::other(
                "executor stdio server requires an explicit cwd",
            ));
        };
        let cwd: PathUri = LegacyAppPathString::from_path(Path::new(&cwd))
            .try_into()
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
        let program_name = program.to_string_lossy().into_owned();
        let envs = create_env_overlay_for_remote_mcp_server(env, &env_vars);
        let remote_env_vars = remote_mcp_env_var_names(&env_vars);
        // The executor protocol carries argv/env as UTF-8 strings. Local stdio can
        // accept arbitrary OsString values because it calls the OS directly; remote
        // stdio must reject non-Unicode command, argument, or environment data
        // before sending an executor request.
        let argv = Self::process_api_argv(&program, &args).map_err(io::Error::other)?;
        let env = Self::process_api_env(envs).map_err(io::Error::other)?;
        let process_id = ExecutorProcessTransport::next_process_id();
        // Start the MCP server process on the executor with raw pipes. `tty=false`
        // keeps stdout as a clean protocol stream, while `pipe_stdin=true` lets
        // rmcp write JSON-RPC requests after the process starts.
        let started = exec_backend
            .start(ExecParams {
                metadata: Default::default(),
                process_id,
                argv,
                cwd,
                shell_snapshot: None,
                env_policy: Some(Self::remote_env_policy(&remote_env_vars)),
                env,
                tty: false,
                pipe_stdin: true,
                arg0: None,
                sandbox: None,
                enforce_managed_network: false,
                managed_network: None,
                network_proxy: None,
            })
            .await
            .map_err(io::Error::other)?;

        let process =
            StdioServerProcessHandle::executor(program_name.clone(), Arc::clone(&started.process));
        Ok(StdioServerTransport {
            inner: StdioServerTransportInner::Executor(ExecutorProcessTransport::new(
                started.process,
                program_name,
            )),
            process,
        })
    }

    fn process_api_argv(program: &OsString, args: &[OsString]) -> Result<Vec<String>> {
        let mut argv = Vec::with_capacity(args.len() + 1);
        argv.push(Self::os_string_to_process_api_string(
            program.clone(),
            "command",
        )?);
        for arg in args {
            argv.push(Self::os_string_to_process_api_string(
                arg.clone(),
                "argument",
            )?);
        }
        Ok(argv)
    }

    fn process_api_env(env: HashMap<OsString, OsString>) -> Result<HashMap<String, String>> {
        env.into_iter()
            .map(|(key, value)| {
                Ok((
                    Self::os_string_to_process_api_string(key, "environment variable name")?,
                    Self::os_string_to_process_api_string(value, "environment variable value")?,
                ))
            })
            .collect()
    }

    fn os_string_to_process_api_string(value: OsString, label: &str) -> Result<String> {
        value
            .into_string()
            .map_err(|_| anyhow!("{label} must be valid Unicode for remote MCP stdio"))
    }

    fn remote_env_policy(remote_env_vars: &[String]) -> ExecEnvPolicy {
        let include_only = if remote_env_vars.is_empty() {
            Vec::new()
        } else {
            // `source = "remote"` means the value is read from the executor's
            // environment, not copied from Codex. Start from `All` only so the
            // named remote variable is available to the filter below; the
            // effective child env is still limited by `include_only`.
            crate::utils::DEFAULT_ENV_VARS
                .iter()
                .map(|name| (*name).to_string())
                .chain(remote_env_vars.iter().cloned())
                .collect()
        };
        ExecEnvPolicy {
            inherit: if remote_env_vars.is_empty() {
                ShellEnvironmentPolicyInherit::Core
            } else {
                ShellEnvironmentPolicyInherit::All
            },
            ignore_default_excludes: true,
            exclude: Vec::new(),
            r#set: HashMap::new(),
            include_only,
        }
    }
}

#[cfg(test)]
#[path = "stdio_server_launcher_tests.rs"]
mod executor_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use codex_protocol::config_types::EnvironmentVariablePattern;
    use codex_protocol::config_types::ShellEnvironmentPolicy;
    use codex_protocol::shell_environment;

    struct NeverClosedProcess {
        id: codex_exec_server::ProcessId,
        requests: std::sync::atomic::AtomicUsize,
        pending_request: bool,
    }

    impl ExecProcess for NeverClosedProcess {
        fn process_id(&self) -> &codex_exec_server::ProcessId {
            &self.id
        }
        fn subscribe_wake(&self) -> tokio::sync::watch::Receiver<u64> {
            tokio::sync::watch::channel(0).1
        }
        fn subscribe_events(&self) -> ExecProcessEventReceiver {
            ExecProcessEventReceiver::empty()
        }
        fn read(
            &self,
            _: Option<u64>,
            _: Option<usize>,
            _: Option<u64>,
        ) -> codex_exec_server::ExecProcessFuture<'_, codex_exec_server::ReadResponse> {
            Box::pin(async { unreachable!("retirement observes events, not output reads") })
        }
        fn write(
            &self,
            _: Vec<u8>,
        ) -> codex_exec_server::ExecProcessFuture<'_, codex_exec_server::WriteResponse> {
            Box::pin(async { unreachable!("retirement never writes input") })
        }
        fn signal(
            &self,
            _: codex_exec_server::ProcessSignal,
        ) -> codex_exec_server::ExecProcessFuture<'_, ()> {
            Box::pin(async { unreachable!("retirement uses terminate") })
        }
        fn terminate(&self) -> codex_exec_server::ExecProcessFuture<'_, ()> {
            self.requests.fetch_add(1, Ordering::Relaxed);
            Box::pin(async {
                if self.pending_request {
                    std::future::pending::<()>().await;
                }
                Ok(())
            })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn executor_request_and_terminal_wait_are_bounded_and_timeout_is_retryable() {
        for pending_request in [false, true] {
            let process = Arc::new(NeverClosedProcess {
                id: "never-closed".into(),
                requests: std::sync::atomic::AtomicUsize::new(0),
                pending_request,
            });
            let handle = StdioServerProcessHandle::executor("test".to_owned(), process.clone());
            for attempt in 1..=2 {
                let start = tokio::time::Instant::now();
                assert_eq!(
                    handle.terminate().await.unwrap_err().kind(),
                    io::ErrorKind::TimedOut
                );
                assert_eq!(start.elapsed(), PROCESS_RETIREMENT_TIMEOUT);
                assert_eq!(process.requests.load(Ordering::Relaxed), attempt);
                assert!(!handle.inner.terminal_observed.load(Ordering::Acquire));
            }
        }
    }

    #[test]
    fn executor_terminal_evidence_never_accepts_exit_failure_or_lost_events() {
        assert!(executor_event_proves_close(Ok(ExecProcessEvent::Closed { seq: 2 })).unwrap());
        assert!(
            !executor_event_proves_close(Ok(ExecProcessEvent::Exited {
                seq: 1,
                exit_code: 0,
                sandbox_denied: None
            }))
            .unwrap()
        );
        for event in [
            Ok(ExecProcessEvent::Failed("lost transport".to_owned())),
            Err(broadcast::error::RecvError::Lagged(1)),
            Err(broadcast::error::RecvError::Closed),
        ] {
            assert!(executor_event_proves_close(event).is_err());
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_close_terminates_before_waiting_for_a_pipe_blocked_sender() {
        for protocol_mode in [McpProtocolMode::Legacy, McpProtocolMode::V20260728] {
            let home = tempfile::tempdir().unwrap();
            let launcher = LocalStdioServerLauncher::new(home.path().to_path_buf());
            let command = StdioServerCommand::new(
                "sleep".into(),
                vec!["30".into()],
                None,
                Vec::new(),
                None,
                protocol_mode,
            );
            let mut transport = launcher.launch(command).await.unwrap();
            let message: TxJsonRpcMessage<RoleClient> = serde_json::from_value(serde_json::json!({
                "jsonrpc":"2.0", "id":1, "method":"ping",
                "params":{"_meta":{"padding":"x".repeat(1024 * 1024)}}
            }))
            .unwrap();
            assert!(serde_json::to_vec(&message).unwrap().len() > 1024 * 1024);
            let mut sender = tokio::spawn(transport.send(message));
            assert!(
                tokio::time::timeout(Duration::from_millis(30), &mut sender)
                    .await
                    .is_err(),
                "the child must actually leave the pipe writer blocked"
            );
            let closed = tokio::time::timeout(
                PROCESS_RETIREMENT_TIMEOUT + Duration::from_secs(1),
                transport.close(),
            )
            .await;
            // Cleanup also runs when the old close-input-first regression is
            // reintroduced, so this negative control cannot strand its child.
            transport.process_handle().terminate().await.unwrap();
            sender
                .await
                .unwrap()
                .expect_err("the dead child's pipe must reject the pending write");
            assert!(
                closed.is_ok(),
                "input lock acquisition must not precede bounded process termination"
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn missing_local_group_terminator_never_claims_retirement() {
        let mut command = Command::new("/bin/sleep");
        command.arg("30");
        let (transport, _) =
            LocalStdioTransport::spawn(command, "test".to_owned(), McpProtocolMode::Legacy)
                .unwrap();
        let handle = StdioServerProcessHandle::local(
            "test".to_owned(),
            /*terminator*/ None,
            transport.exit_observer(),
            /*stderr_reader*/ None,
        );
        for _ in 0..2 {
            assert!(handle.terminate().await.is_err());
            assert!(!handle.inner.terminal_observed.load(Ordering::Acquire));
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_retirement_waits_for_term_ignoring_descendant_and_is_repeatable() {
        for protocol_mode in [McpProtocolMode::Legacy, McpProtocolMode::V20260728] {
            let dir = tempfile::tempdir().unwrap();
            let marker = dir.path().join("descendant-ready");
            let launcher = LocalStdioServerLauncher::new(dir.path().to_path_buf());
            let command = StdioServerCommand::new(
                "sh".into(),
                vec![
                    "-c".into(),
                    "(trap '' TERM; printf ready > \"$1\"; exec sleep 30) & wait".into(),
                    "--".into(),
                    marker.as_os_str().to_owned(),
                ],
                None,
                Vec::new(),
                None,
                protocol_mode,
            );
            let mut transport = launcher.launch(command).await.unwrap();
            tokio::time::timeout(Duration::from_secs(3), async {
                while !marker.exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("descendant ready before retirement");
            let process = transport.process_handle();
            let started = tokio::time::Instant::now();
            let (first, second) = tokio::join!(process.terminate(), process.terminate());
            first.expect("direct child and group exit observed");
            second.expect("concurrent waiter shares terminal proof");
            assert!(started.elapsed() >= PROCESS_GROUP_TERM_GRACE_PERIOD);
            process
                .terminate()
                .await
                .expect("repeated terminal fact retained");
            transport.close().await.unwrap();
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn each_local_protocol_mode_waits_for_terminal_exit_on_close() {
        for protocol_mode in [McpProtocolMode::Legacy, McpProtocolMode::V20260728] {
            let launcher = LocalStdioServerLauncher::new(
                std::env::current_dir().expect("current directory for test launcher"),
            );
            let command = StdioServerCommand::new(
                "sh".into(),
                vec!["-c".into(), "sleep 30".into()],
                None,
                Vec::new(),
                None,
                protocol_mode,
            );
            let mut transport = launcher.launch(command).await.expect("launch test child");

            tokio::time::timeout(Duration::from_secs(2), transport.close())
                .await
                .expect("termination should complete promptly")
                .expect("close should report the child terminal observation");
        }
    }

    #[test]
    fn remote_env_policy_uses_core_env_without_remote_source_vars() {
        let policy = ExecutorStdioServerLauncher::remote_env_policy(&[]);

        assert_eq!(policy.inherit, ShellEnvironmentPolicyInherit::Core);
        assert!(policy.include_only.is_empty());
    }

    #[test]
    fn remote_env_policy_includes_remote_source_vars_without_full_env() {
        let policy = ExecutorStdioServerLauncher::remote_env_policy(&["REMOTE_TOKEN".to_string()]);

        assert_eq!(policy.inherit, ShellEnvironmentPolicyInherit::All);
        assert!(
            policy.include_only.contains(&"REMOTE_TOKEN".to_string()),
            "remote source var should be included in executor env policy"
        );
        assert!(
            policy
                .include_only
                .contains(&crate::utils::DEFAULT_ENV_VARS[0].to_string()),
            "remote default env vars should remain available"
        );
    }

    #[test]
    fn remote_env_policy_effectively_filters_unrequested_vars() {
        let exec_policy =
            ExecutorStdioServerLauncher::remote_env_policy(&["REMOTE_TOKEN".to_string()]);
        let policy = ShellEnvironmentPolicy {
            inherit: exec_policy.inherit,
            ignore_default_excludes: exec_policy.ignore_default_excludes,
            exclude: exec_policy
                .exclude
                .iter()
                .map(|pattern| EnvironmentVariablePattern::new_case_insensitive(pattern))
                .collect(),
            r#set: exec_policy.r#set,
            include_only: exec_policy
                .include_only
                .iter()
                .map(|pattern| EnvironmentVariablePattern::new_case_insensitive(pattern))
                .collect(),
            use_profile: false,
        };

        let env = shell_environment::create_env_from_vars(
            [
                ("PATH".to_string(), "/remote/bin".to_string()),
                ("REMOTE_TOKEN".to_string(), "remote-secret".to_string()),
                (
                    "UNREQUESTED_SECRET".to_string(),
                    "must-not-pass".to_string(),
                ),
            ],
            &policy,
            /*thread_id*/ None,
        );

        assert_eq!(env.get("PATH").map(String::as_str), Some("/remote/bin"));
        assert_eq!(
            env.get("REMOTE_TOKEN").map(String::as_str),
            Some("remote-secret")
        );
        assert!(!env.contains_key("UNREQUESTED_SECRET"));
    }
}
