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
//! Unix local servers use the stdio-only descriptor policy.

use std::collections::HashMap;
use std::ffi::OsString;
use std::future::Future;
use std::io;
#[cfg(windows)]
use std::os::windows::io::OwnedHandle;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(unix)]
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
#[cfg(unix)]
use std::thread::sleep;
use std::time::Duration;

use crate::local_stdio_io;
use crate::local_stdio_transport::DiscoveryOutcome;
use anyhow::Result;
use anyhow::anyhow;
use codex_config::types::McpServerEnvVar;
use codex_exec_server::ExecBackend;
use codex_exec_server::ExecEnvPolicy;
use codex_exec_server::ExecParams;
use codex_exec_server::ExecProcess;
use codex_exec_server::ExecProcessEvent;
use codex_protocol::config_types::ShellEnvironmentPolicyInherit;
use codex_utils_path_uri::LegacyAppPathString;
use codex_utils_path_uri::PathUri;
#[cfg(unix)]
use codex_utils_pty::Child;
use codex_utils_pty::Command;
#[cfg(unix)]
use codex_utils_pty::DescriptorPolicy;
use codex_utils_pty::ProcessMode;
#[cfg(unix)]
use codex_utils_pty::process_group::kill_process_group;
#[cfg(unix)]
use codex_utils_pty::process_group::process_group_exists;
#[cfg(unix)]
use codex_utils_pty::process_group::terminate_process_group;
#[cfg(unix)]
use codex_utils_pty::process_group::wait_for_process_group_exit;
use futures::FutureExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use rmcp::service::RoleClient;
use rmcp::service::RxJsonRpcMessage;
use rmcp::service::TxJsonRpcMessage;
use rmcp::transport::Transport;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::runtime::Handle;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::task::AbortOnDropHandle;
use tracing::info;
use tracing::warn;

use crate::executor_process_transport::ExecutorProcessTransport;
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
    replacement_slot: Option<StdioProcessSlot>,
}

/// A replacement holds the slot from OS spawn through registration. Closing it seals that handoff.
pub(super) type StdioProcessSlot = Arc<AsyncMutex<StdioProcessRegistration>>;

pub(super) struct StdioProcessRegistration {
    process: StdioProcessRef,
    closed: bool,
}

impl StdioProcessRegistration {
    pub(super) fn close(&mut self) -> StdioProcessRef {
        self.closed = true;
        self.process.clone()
    }
}

type DiscoveryCleanupResult = Shared<BoxFuture<'static, Result<(), Arc<io::Error>>>>;

/// One retained cleanup operation, shared by restart, shutdown and later observers.
/// Its owned process outlives the public handle, including when a bounded wait is cancelled.
struct DiscoveryCleanup {
    process: Arc<StdioServerProcessOwner>,
    state: Mutex<DiscoveryCleanupState>,
    observed: AtomicBool,
}

#[derive(Default)]
struct DiscoveryCleanupState {
    reason: Option<StdioTermination>,
    operation: Option<DiscoveryCleanupResult>,
}

impl DiscoveryCleanup {
    fn is_started(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .operation
            .is_some()
    }

    fn start(&self, runtime: &Handle, reason: StdioTermination) -> DiscoveryCleanupResult {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Reset failures and install the next attempt under the same lock. All
        // concurrent retry observers join that attempt; successful steps stay cached.
        if let Some(operation) = &state.operation
            && !matches!(operation.peek(), Some(Err(_)))
        {
            return operation.clone();
        }
        let reason = *state.reason.get_or_insert(reason);
        let process = Arc::clone(&self.process);
        // Submit local cleanup immediately, including on an idle current-thread runtime.
        let task = match &process.kind {
            StdioServerProcessKind::Local(_) => {
                runtime.spawn_blocking(move || process.terminate_local(reason))
            }
            StdioServerProcessKind::Executor(_) => {
                runtime.spawn(async move { process.terminate_executor().await })
            }
        };
        let operation = task
            .map(|joined| match joined {
                Ok(result) => result.map_err(Arc::new),
                Err(error) => Err(Arc::new(io::Error::other(error))),
            })
            .boxed()
            .shared();
        state.operation = Some(operation.clone());
        operation
    }

    async fn wait_until(&self, reason: StdioTermination, deadline: Instant) -> io::Result<()> {
        if self.observed.load(Ordering::Acquire) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "MCP process retirement deadline expired",
            ));
        }
        let operation = self.start(&Handle::try_current().map_err(io::Error::other)?, reason);
        tokio::time::timeout_at(deadline, operation)
            .await
            .map_err(|_| {
                io::Error::new(io::ErrorKind::TimedOut, "MCP process retirement timed out")
            })?
            .map_err(|error| io::Error::new(error.kind(), error))?;
        self.observe_until(deadline).await?;
        self.observed.store(true, Ordering::Release);
        Ok(())
    }

    async fn observe_until(&self, deadline: Instant) -> io::Result<()> {
        #[cfg(unix)]
        self.observe_group_until(deadline, process_group_exists)
            .await?;
        #[cfg(not(unix))]
        let _ = deadline;
        Ok(())
    }

    #[cfg(unix)]
    async fn observe_group_until(
        &self,
        deadline: Instant,
        mut exists: impl FnMut(u32) -> io::Result<bool>,
    ) -> io::Result<()> {
        if let StdioServerProcessKind::Local(Some(LocalProcessTerminator::DiscoveryLeader {
            leader,
        })) = &self.process.kind
        {
            let pid = leader
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pid;
            while exists(pid)? {
                if Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "MCP process group exit was not observed",
                    ));
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(super) struct StdioProcessRef {
    cleanup: Arc<DiscoveryCleanup>,
}

impl StdioProcessRef {
    pub(super) async fn terminate(&self, reason: StdioTermination) -> io::Result<()> {
        self.terminate_until(reason, Instant::now() + PROCESS_RETIREMENT_TIMEOUT)
            .await
    }

    pub(super) async fn terminate_until(
        &self,
        reason: StdioTermination,
        deadline: Instant,
    ) -> io::Result<()> {
        self.cleanup.wait_until(reason, deadline).await
    }
}

/// Client-side rmcp transport for a launched MCP stdio server.
///
/// The concrete process placement stays private to this module. `RmcpClient`
/// only sees the standard rmcp transport abstraction and can pass this value
/// directly to `rmcp::service::serve_client`.
pub struct StdioServerTransport {
    inner: StdioServerTransportInner,
    process: Arc<StdioServerProcessHandle>,
    discovery_cleanup: Option<Arc<DiscoveryCleanup>>,
    protocol_mode: McpProtocolMode,
    restart_on_drop: bool,
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
        if self.discovery_cleanup.is_none() {
            self.process.terminate().await?;
        }
        match &mut self.inner {
            StdioServerTransportInner::Local(transport) => transport.close().await,
            StdioServerTransportInner::Executor(transport) => transport.close().await,
        }
    }
}

impl Drop for StdioServerTransport {
    fn drop(&mut self) {
        // A client or handshake still holding this result may retry after the SDK drops a
        // failed transport. Otherwise the process handle runs its ordinary shutdown on drop.
        if let Some(cleanup) = &self.discovery_cleanup
            // The transport and its handle each hold one intrinsic reference.
            && Arc::strong_count(cleanup) > 2
            && !cleanup.is_started()
            && let Ok(runtime) = Handle::try_current()
        {
            drop(cleanup.start(
                &runtime,
                if self.restart_on_drop {
                    StdioTermination::Restart
                } else {
                    StdioTermination::Shutdown
                },
            ));
        }
    }
}

impl StdioServerTransport {
    pub(super) fn finish_discovery(&mut self) {
        if let StdioServerTransportInner::Local(local) = &mut self.inner {
            local.finish_discovery();
        }
    }

    pub(super) fn protocol_mode(&self) -> McpProtocolMode {
        self.protocol_mode
    }

    /// Observes the first discovery reply or a clean EOF during local stdio startup.
    pub(super) fn watch_discovery(
        &mut self,
        timeout: Option<Duration>,
    ) -> Option<oneshot::Receiver<DiscoveryOutcome>> {
        match &mut self.inner {
            StdioServerTransportInner::Local(local) => Some(local.watch_discovery(timeout)),
            StdioServerTransportInner::Executor(_) => None,
        }
    }

    pub(super) fn process_ref(&self) -> StdioProcessRef {
        StdioProcessRef {
            cleanup: Arc::clone(&self.process.cleanup),
        }
    }

    pub(super) fn process_slot(&self) -> StdioProcessSlot {
        Arc::new(AsyncMutex::new(StdioProcessRegistration {
            process: self.process_ref(),
            closed: false,
        }))
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
            replacement_slot: None,
        }
    }

    /// Registers the replacement with its original client before the native spawn can finish.
    pub(super) fn for_restart(
        &self,
        protocol_mode: McpProtocolMode,
        slot: StdioProcessSlot,
    ) -> Self {
        Self {
            protocol_mode,
            replacement_slot: Some(slot),
            ..self.clone()
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
            // Cancel queued launches when startup is dropped. A worker that already started
            // drops its transport and cleans up the child when it finishes.
            AbortOnDropHandle::new(tokio::task::spawn_blocking(move || {
                Self::launch_server(command, fallback_cwd)
            }))
            .await
            .map_err(io::Error::other)?
        }
        .boxed()
    }
}

// Local private implementation.

#[cfg(unix)]
const PROCESS_GROUP_TERM_GRACE_PERIOD: Duration = Duration::from_secs(2);

#[cfg(unix)]
const RESTART_DRAIN_GRACE_PERIOD: Duration = Duration::from_millis(250);

// Keep queued stderr diagnostics before closing the reader, even when an
// escaped descendant prevents the pipe from reaching EOF.
const STDERR_READER_DRAIN_GRACE_PERIOD: Duration = Duration::from_millis(250);
const PROCESS_RETIREMENT_TIMEOUT: Duration = Duration::from_secs(3);

#[cfg(unix)]
enum LocalProcessTerminator {
    // The SDK may drop the failed transport before the caller can decide whether to restart it.
    DiscoveryLeader { leader: Mutex<UnixGroupCleanup> },
}

#[cfg(unix)]
struct UnixGroupCleanup {
    pid: u32,
    child: Option<Box<Child>>,
    term_sent: bool,
    released: bool,
}

#[cfg(unix)]
impl Drop for UnixGroupCleanup {
    fn drop(&mut self) {
        if self.child.is_some() {
            let _ = kill_process_group_and_warn(self.pid);
        }
    }
}

#[cfg(unix)]
fn kill_process_group_and_warn(pid: u32) -> io::Result<()> {
    kill_process_group(pid)
        .inspect_err(|error| warn!("Failed to kill MCP process group {pid}: {error}"))
}

#[cfg(windows)]
enum LocalProcessTerminator {
    Job {
        job: codex_utils_pty::JobObject,
        discovery_root: Option<OwnedHandle>,
    },
    Process(OwnedHandle),
}

#[cfg(not(any(unix, windows)))]
struct LocalProcessTerminator;

pub(crate) struct StdioServerProcessHandle {
    cleanup: Arc<DiscoveryCleanup>,
}

struct StdioServerProcessOwner {
    program_name: String,
    kind: StdioServerProcessKind,
    stderr_reader: Option<watch::Sender<()>>,
    executor_termination_accepted: AtomicBool,
}

enum StdioServerProcessKind {
    Local(Option<LocalProcessTerminator>),
    Executor(Arc<dyn ExecProcess>),
}

#[derive(Clone, Copy)]
pub(super) enum StdioTermination {
    Shutdown,
    Restart,
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
            replacement_slot,
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
            // MCP uses only stdio; select Explicit to exclude unrelated
            // orchestrator descriptors from the server and commands it launches.
            // Descriptor allowlisting is Unix-only. Windows can still inherit unrelated
            // handles and needs a handle allowlist in the shared spawn backend.
            #[cfg(unix)]
            command.descriptor_policy(DescriptorPolicy::Explicit);
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

        // The async caller can be cancelled while this blocking worker is inside spawn.
        let slot = replacement_slot.as_ref().map(|slot| slot.blocking_lock());
        if slot.as_ref().is_some_and(|slot| slot.closed) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "MCP client is shut down",
            ));
        }
        let stdio = local_stdio_io::spawn(command)?;
        #[cfg(windows)]
        let (stdio, job) = match job {
            Some(job) => match stdio
                .child
                .id()
                .ok_or_else(|| io::Error::other("missing suspended MCP server process id"))
                .and_then(|process_id| job.assign_and_resume_process(process_id))
            {
                Ok(Some(root)) => (stdio, Some((job, root))),
                Ok(None) => (stdio, None),
                Err(error) => {
                    warn!(
                        "Windows MCP process job containment failed; retrying without it: {error}"
                    );
                    drop(stdio);
                    drop(job);
                    (local_stdio_io::spawn(build_command())?, None)
                }
            },
            None => (stdio, None),
        };
        #[cfg(windows)]
        let protocol_mode = if job.is_none() && protocol_mode == McpProtocolMode::DiscoverLocalStdio
        {
            // A probe can require a restart. Without a job we cannot stop its descendants.
            McpProtocolMode::Legacy
        } else {
            protocol_mode
        };
        let process_id = stdio.child.id();
        // A legacy replacement still belongs to the discovery startup that launched it.
        let discovery_owned = (protocol_mode == McpProtocolMode::DiscoverLocalStdio
            || replacement_slot.is_some())
            && process_id.is_some();
        let (transport, stderr) =
            LocalStdioTransport::new(stdio, program_name.clone(), protocol_mode);
        #[cfg(windows)]
        let terminator = match job {
            Some((job, root)) => Some(LocalProcessTerminator::Job {
                job,
                discovery_root: Some(root),
            }),
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
        #[cfg(unix)]
        let (transport, terminator) = {
            let mut transport = transport;
            let terminator = process_id.map(|pid| LocalProcessTerminator::DiscoveryLeader {
                // Every local group keeps its leader until the last destructive signal.
                leader: Mutex::new(UnixGroupCleanup {
                    pid,
                    child: transport.take_child(),
                    term_sent: false,
                    released: false,
                }),
            });
            (transport, terminator)
        };
        #[cfg(not(any(unix, windows)))]
        let terminator = process_id.map(|_| LocalProcessTerminator);
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
        let process = StdioServerProcessHandle::local(program_name, terminator, stderr_reader);
        let discovery_cleanup = Some(Arc::clone(&process.cleanup));
        let transport = StdioServerTransport {
            inner: StdioServerTransportInner::Local(transport),
            process,
            discovery_cleanup,
            protocol_mode,
            restart_on_drop: discovery_owned,
        };
        if let Some(mut slot) = slot {
            slot.process = transport.process_ref();
        }

        Ok(transport)
    }
}

impl LocalProcessTerminator {
    #[cfg(unix)]
    fn terminate_discovery(&self, reason: StdioTermination) -> io::Result<()> {
        let Self::DiscoveryLeader { leader, .. } = self;
        // Hold the stage lock through the last signal and child release. A
        // retry can never signal a group whose identity reservation was released.
        let mut stage = leader
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if stage.released {
            return Ok(());
        }
        if stage.child.is_none() {
            return Err(io::Error::other("missing Unix MCP discovery leader"));
        }
        let pid = stage.pid;
        if !stage.term_sent {
            let grace = match reason {
                StdioTermination::Restart => {
                    sleep(RESTART_DRAIN_GRACE_PERIOD);
                    RESTART_DRAIN_GRACE_PERIOD
                }
                StdioTermination::Shutdown => PROCESS_GROUP_TERM_GRACE_PERIOD,
            };
            let present = terminate_process_group(pid)?;
            stage.term_sent = true;
            if present {
                wait_for_process_group_exit(pid, grace);
            }
        }
        kill_process_group_and_warn(pid)?;
        stage.released = true;
        stage.child.take();
        // Absence is observed separately under each caller's fresh deadline.

        Ok(())
    }

    #[cfg(windows)]
    fn terminate_discovery(&self, _reason: StdioTermination) -> io::Result<()> {
        match self {
            Self::Job {
                job,
                discovery_root: Some(root),
            } => job.terminate_and_wait(root, Duration::from_secs(1)),
            Self::Process(handle) => {
                codex_utils_pty::JobObject::terminate_process_handle(handle)?;
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "MCP process lacks job-level terminal evidence",
                ))
            }
            _ => Err(io::Error::other("missing Windows MCP job root")),
        }
    }

    #[cfg(not(any(unix, windows)))]
    fn terminate_discovery(&self, _reason: StdioTermination) -> io::Result<()> {
        Err(io::Error::other("MCP stdio restart is unsupported"))
    }
}

impl StdioServerProcessHandle {
    fn local(
        program_name: String,
        terminator: Option<LocalProcessTerminator>,
        stderr_reader: Option<watch::Sender<()>>,
    ) -> Arc<Self> {
        Self::new_owner(StdioServerProcessOwner {
            program_name,
            kind: StdioServerProcessKind::Local(terminator),
            stderr_reader,
            executor_termination_accepted: AtomicBool::new(false),
        })
    }

    pub(crate) fn executor(program_name: String, process: Arc<dyn ExecProcess>) -> Arc<Self> {
        Self::new_owner(StdioServerProcessOwner {
            program_name,
            kind: StdioServerProcessKind::Executor(process),
            stderr_reader: None,
            executor_termination_accepted: AtomicBool::new(false),
        })
    }

    fn new_owner(process: StdioServerProcessOwner) -> Arc<Self> {
        Arc::new(Self {
            cleanup: Arc::new(DiscoveryCleanup {
                process: Arc::new(process),
                state: Mutex::new(DiscoveryCleanupState::default()),
                observed: AtomicBool::new(false),
            }),
        })
    }

    pub(crate) async fn terminate(&self) -> io::Result<()> {
        self.terminate_until(Instant::now() + PROCESS_RETIREMENT_TIMEOUT)
            .await
    }

    pub(crate) async fn terminate_until(&self, deadline: Instant) -> io::Result<()> {
        self.cleanup
            .wait_until(StdioTermination::Shutdown, deadline)
            .await
    }
}

impl StdioServerProcessOwner {
    fn stop_stderr(&self) {
        if let Some(stderr_reader) = &self.stderr_reader {
            stderr_reader.send_replace(());
        }
    }

    fn terminate_local(&self, reason: StdioTermination) -> io::Result<()> {
        let result = match &self.kind {
            StdioServerProcessKind::Local(Some(terminator)) => {
                terminator.terminate_discovery(reason)
            }
            _ => Err(io::Error::other("missing MCP process-group terminal owner")),
        };
        self.stop_stderr();
        result
    }

    async fn terminate_executor(&self) -> io::Result<()> {
        let result = match &self.kind {
            StdioServerProcessKind::Executor(process) => {
                let mut events = process.subscribe_events();
                if !self.executor_termination_accepted.load(Ordering::Acquire) {
                    process.terminate().await.map_err(io::Error::other)?;
                    self.executor_termination_accepted
                        .store(true, Ordering::Release);
                }
                loop {
                    match events.recv().await {
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            events = process.subscribe_events();
                            tokio::task::yield_now().await;
                        }
                        event => {
                            if executor_event_proves_close(event)? {
                                break Ok(());
                            }
                        }
                    }
                }
            }
            _ => Err(io::Error::other("missing executor process owner")),
        };
        self.stop_stderr();
        result
    }
}

fn executor_event_proves_close(
    event: Result<ExecProcessEvent, tokio::sync::broadcast::error::RecvError>,
) -> io::Result<bool> {
    match event {
        Ok(ExecProcessEvent::Closed { .. }) => Ok(true),
        Ok(ExecProcessEvent::Exited { .. } | ExecProcessEvent::Output(_)) => Ok(false),
        Ok(ExecProcessEvent::Failed(message)) => {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, message))
        }
        Err(error) => Err(io::Error::other(error)),
    }
}

impl Drop for StdioServerProcessHandle {
    fn drop(&mut self) {
        if !self.cleanup.is_started() {
            if let Ok(runtime) = Handle::try_current() {
                drop(self.cleanup.start(&runtime, StdioTermination::Shutdown));
            } else {
                // Retained StdioProcessRefs can drive this owner on a later runtime.
                warn!(
                    "MCP process cleanup remains unproven ({}): no Tokio runtime",
                    self.cleanup.process.program_name
                );
            }
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
            protocol_mode,
            replacement_slot: _,
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
                env_policy: Some(Self::remote_env_policy(&remote_env_vars, &env)),
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
            discovery_cleanup: None,
            protocol_mode,
            restart_on_drop: false,
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

    fn remote_env_policy(
        remote_env_vars: &[String],
        env: &HashMap<String, String>,
    ) -> ExecEnvPolicy {
        let include_only = if remote_env_vars.is_empty() {
            Vec::new()
        } else {
            // `source = "remote"` means the value is read from the executor's
            // environment, not copied from Codex. Start from `All` only so the
            // named remote variable is available to the filter below; the
            // effective child env is still limited by `include_only`.
            // The orchestrator may be Unix while the executor is Windows.
            // Preserve Windows runtime initialization and temporary-directory
            // inputs even when explicit remote vars activate this allowlist.
            crate::utils::DEFAULT_ENV_VARS
                .iter()
                .chain(["SYSTEMROOT", "TEMP", "TMP", "SYSTEMDRIVE", "LOCALAPPDATA"].iter())
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
            // Apply overrides through the policy's Windows-aware merge, including
            // on older executors. Keep the final overlay for names filtered out below.
            r#set: env.clone(),
            include_only,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_protocol::config_types::EnvironmentVariablePattern;
    use codex_protocol::config_types::ShellEnvironmentPolicy;
    use codex_protocol::shell_environment;
    use pretty_assertions::assert_eq;

    #[cfg(windows)]
    #[test]
    fn idle_local_mcp_pipe_reads_leave_blocking_io_available() -> anyhow::Result<()> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let cwd = std::env::current_dir()?;
            let launcher = LocalStdioServerLauncher::new(cwd.clone());
            for mode in [McpProtocolMode::Legacy, McpProtocolMode::V20260728] {
                // set /p waits for stdin and leaves both output pipes open and quiet.
                let command = StdioServerCommand::new(
                    "cmd.exe".into(),
                    ["/d", "/q", "/c", "set /p idle="]
                        .map(OsString::from)
                        .into(),
                    None,
                    Vec::new(),
                    None,
                    mode,
                );
                let mut transport = launcher.launch(command).await?;
                // Poll the stdout read before scheduling the filesystem operation. The
                // launcher also drains stderr. A blocking read can occupy the only slot.
                let pending = transport.receive().now_or_never().is_none();
                let metadata =
                    tokio::time::timeout(Duration::from_secs(5), tokio::fs::metadata(&cwd)).await;
                // Release the child even on the failing, pre-fix path.
                transport.close().await?;
                anyhow::ensure!(pending, "helper exited before its idle read");
                metadata??;
            }
            Ok(())
        })
    }

    #[test]
    fn executor_terminal_evidence_rejects_exit_failure_and_lost_events() {
        assert!(
            executor_event_proves_close(Ok(ExecProcessEvent::Closed {
                seq: 2,
                sandbox_cleanup: None
            }))
            .expect("closed")
        );
        assert!(
            !executor_event_proves_close(Ok(ExecProcessEvent::Exited {
                seq: 1,
                exit_code: 0,
                sandbox_denied: None
            }))
            .expect("exit is not closed")
        );
        assert!(executor_event_proves_close(Ok(ExecProcessEvent::Failed("lost".into()))).is_err());
        assert!(
            executor_event_proves_close(Err(tokio::sync::broadcast::error::RecvError::Closed))
                .is_err()
        );
        assert!(
            executor_event_proves_close(Err(tokio::sync::broadcast::error::RecvError::Lagged(1)))
                .is_err()
        );
    }

    #[tokio::test]
    async fn missing_process_owner_never_claims_retirement_on_repeat() {
        let process = StdioServerProcessHandle::local("missing".into(), None, None);
        let observed = StdioProcessRef {
            cleanup: Arc::clone(&process.cleanup),
        };
        assert!(
            observed
                .terminate(StdioTermination::Shutdown)
                .await
                .is_err()
        );
        drop(process);
        assert!(
            observed
                .terminate(StdioTermination::Shutdown)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn expired_process_deadline_starts_no_cleanup() {
        let process = StdioServerProcessHandle::local("missing".into(), None, None);
        let error = process
            .terminate_until(Instant::now())
            .await
            .expect_err("expired deadline");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(!process.cleanup.is_started());
    }

    #[test]
    fn no_runtime_drop_retains_unstarted_custody_for_later_observer() {
        let process = StdioServerProcessHandle::local("missing".into(), None, None);
        let observed = StdioProcessRef {
            cleanup: Arc::clone(&process.cleanup),
        };
        drop(process);
        assert!(!observed.cleanup.is_started());
        let runtime = tokio::runtime::Runtime::new().expect("fixture runtime");
        assert!(
            runtime
                .block_on(observed.terminate(StdioTermination::Shutdown))
                .is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn process_cleanup_progresses_while_current_thread_runtime_is_idle() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let ready = root.path().join("ready");
        let script = format!(
            "trap '' TERM; echo ready > '{}'; while :; do sleep 1; done",
            ready.display()
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let (observed, pid) = runtime.block_on(async {
            let command = StdioServerCommand::new(
                "/bin/sh".into(),
                vec!["-c".into(), script.into()],
                None,
                vec![],
                None,
                McpProtocolMode::Legacy,
            );
            let transport =
                LocalStdioServerLauncher::launch_server(command, root.path().to_owned())?;
            let observed = transport.process_ref();
            let pid = match &observed.cleanup.process.kind {
                StdioServerProcessKind::Local(Some(LocalProcessTerminator::DiscoveryLeader {
                    leader,
                })) => leader.lock().expect("leader").pid,
                _ => panic!("owned local group"),
            };
            tokio::time::timeout(Duration::from_secs(3), async {
                while !ready.exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await?;
            drop(transport); // Starts the same once-only blocking cleanup.
            Ok::<_, anyhow::Error>((observed, pid))
        })?;
        // Do not drive the runtime: a detached async prelude would stall here.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while process_group_exists(pid)? && std::time::Instant::now() < deadline {
            sleep(Duration::from_millis(10));
        }
        let absent_while_idle = !process_group_exists(pid)?;
        // Always drive/observe cleanup before asserting, including the regression path.
        runtime.block_on(observed.terminate_until(
            StdioTermination::Shutdown,
            Instant::now() + Duration::from_secs(5),
        ))?;
        assert!(
            absent_while_idle,
            "blocking cleanup must finish while the current-thread runtime is idle"
        );
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ordinary_group_retirement_retains_leader_and_retries_one_operation()
    -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let ready = root.path().join("ready");
        let script = format!(
            "trap '' TERM; echo ready > '{}'; while :; do sleep 1; done",
            ready.display()
        );
        let command = StdioServerCommand::new(
            "/bin/sh".into(),
            vec!["-c".into(), script.into()],
            None,
            vec![],
            None,
            McpProtocolMode::Legacy,
        );
        let transport = LocalStdioServerLauncher::launch_server(command, root.path().to_owned())?;
        let observed = transport.process_ref();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !ready.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        // Keep the transport alive: cleanup itself must own and stop the group.
        let (short, eventual) = tokio::join!(
            observed.terminate_until(
                StdioTermination::Shutdown,
                Instant::now() + Duration::from_millis(20)
            ),
            observed.terminate_until(
                StdioTermination::Shutdown,
                Instant::now() + Duration::from_secs(5)
            ),
        );
        assert_eq!(
            short
                .expect_err("TERM-ignoring group exceeds short wait")
                .kind(),
            io::ErrorKind::TimedOut
        );
        eventual?;
        drop(transport);
        observed
            .terminate_until(
                StdioTermination::Restart,
                Instant::now() + Duration::from_secs(1),
            )
            .await?;
        Ok(())
    }

    #[cfg(unix)]
    struct FailFirstTermination {
        inner: Arc<dyn ExecProcess>,
        calls: std::sync::atomic::AtomicUsize,
    }

    #[cfg(unix)]
    impl ExecProcess for FailFirstTermination {
        fn process_id(&self) -> &codex_exec_server::ProcessId {
            self.inner.process_id()
        }
        fn subscribe_wake(&self) -> watch::Receiver<u64> {
            self.inner.subscribe_wake()
        }
        fn subscribe_events(&self) -> codex_exec_server::ExecProcessEventReceiver {
            self.inner.subscribe_events()
        }
        fn read(
            &self,
            after: Option<u64>,
            max: Option<usize>,
            wait: Option<u64>,
        ) -> codex_exec_server::ExecProcessFuture<'_, codex_exec_server::ReadResponse> {
            self.inner.read(after, max, wait)
        }
        fn write(
            &self,
            chunk: Vec<u8>,
        ) -> codex_exec_server::ExecProcessFuture<'_, codex_exec_server::WriteResponse> {
            self.inner.write(chunk)
        }
        fn signal(
            &self,
            signal: codex_exec_server::ProcessSignal,
        ) -> codex_exec_server::ExecProcessFuture<'_, ()> {
            self.inner.signal(signal)
        }
        fn terminate(&self) -> codex_exec_server::ExecProcessFuture<'_, ()> {
            if self.calls.fetch_add(1, Ordering::AcqRel) == 0 {
                Box::pin(async {
                    Err(codex_exec_server::ExecServerError::Protocol(
                        "injected termination failure".into(),
                    ))
                })
            } else {
                self.inner.terminate()
            }
        }
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn executor_failed_rpc_retries_then_caches_positive_close() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let command = StdioServerCommand::new(
            "/bin/sleep".into(),
            vec!["10".into()],
            None,
            vec![],
            Some(root.path().to_string_lossy().into_owned()),
            McpProtocolMode::Legacy,
        );
        let transport = ExecutorStdioServerLauncher::launch_server(
            command,
            codex_exec_server::Environment::default_for_tests().get_exec_backend(),
        )
        .await?;
        let original = transport.process_ref();
        let StdioServerProcessKind::Executor(inner) = &original.cleanup.process.kind else {
            panic!("executor owner");
        };
        let process = Arc::new(FailFirstTermination {
            inner: Arc::clone(inner),
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let owner = StdioServerProcessHandle::executor("retry fixture".into(), process.clone());
        assert!(owner.terminate().await.is_err());
        let (first, second) = tokio::join!(owner.terminate(), owner.terminate());
        first?;
        second?;
        assert_eq!(process.calls.load(Ordering::Acquire), 2);
        owner.terminate_until(Instant::now()).await?;
        assert_eq!(process.calls.load(Ordering::Acquire), 2);
        drop(transport);
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn released_group_reobserves_after_timeout_without_repeating_signals()
    -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let command = StdioServerCommand::new(
            "/bin/sleep".into(),
            vec!["10".into()],
            None,
            vec![],
            None,
            McpProtocolMode::Legacy,
        );
        let transport = LocalStdioServerLauncher::launch_server(command, root.path().to_owned())?;
        let observed = transport.process_ref();
        let cleanup = &observed.cleanup;
        let operation = cleanup.start(&Handle::current(), StdioTermination::Restart);
        operation.clone().await.map_err(io::Error::other)?;
        // Inject only delayed absence evidence; never invent positive exit evidence.
        assert_eq!(
            cleanup
                .observe_group_until(Instant::now() + Duration::from_millis(20), |_| Ok(true))
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::TimedOut
        );
        observed
            .terminate_until(
                StdioTermination::Restart,
                Instant::now() + Duration::from_secs(3),
            )
            .await?;
        let same_operation = cleanup.start(&Handle::current(), StdioTermination::Shutdown);
        assert!(operation.ptr_eq(&same_operation));
        assert!(cleanup.observed.load(Ordering::Acquire));
        observed
            .terminate_until(StdioTermination::Shutdown, Instant::now())
            .await?;
        drop(transport);
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_held_stage_concurrent_retries_join_one_operation() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let command = StdioServerCommand::new(
            "/bin/sleep".into(),
            vec!["10".into()],
            None,
            vec![],
            None,
            McpProtocolMode::Legacy,
        );
        let transport = LocalStdioServerLauncher::launch_server(command, root.path().to_owned())?;
        let observed = transport.process_ref();
        let cleanup = &observed.cleanup;
        let StdioServerProcessKind::Local(Some(LocalProcessTerminator::DiscoveryLeader { leader })) =
            &cleanup.process.kind
        else {
            panic!("local group");
        };
        // Retain the fixture's reservation outside the stage to force a safe,
        // pre-signal error, then restore it before simultaneous retry admission.
        let child = leader.lock().unwrap().child.take();
        let failed = cleanup.start(&Handle::current(), StdioTermination::Restart);
        assert!(failed.clone().await.is_err());
        leader.lock().unwrap().child = child;
        let runtime = Handle::current();
        let barrier = std::sync::Barrier::new(2);
        let (first, second) = std::thread::scope(|scope| {
            let first = scope.spawn(|| {
                barrier.wait();
                cleanup.start(&runtime, StdioTermination::Restart)
            });
            let second = scope.spawn(|| {
                barrier.wait();
                cleanup.start(&runtime, StdioTermination::Shutdown)
            });
            (first.join().unwrap(), second.join().unwrap())
        });
        assert!(!failed.ptr_eq(&first));
        assert!(first.ptr_eq(&second));
        first.await.map_err(io::Error::other)?;
        observed
            .terminate_until(
                StdioTermination::Shutdown,
                Instant::now() + Duration::from_secs(3),
            )
            .await?;
        assert!(leader.lock().unwrap().released);
        assert!(leader.lock().unwrap().child.is_none());
        drop(transport);
        Ok(())
    }

    #[test]
    fn remote_env_policy_uses_core_env_without_remote_source_vars() {
        let policy = ExecutorStdioServerLauncher::remote_env_policy(&[], &HashMap::new());

        assert_eq!(policy.inherit, ShellEnvironmentPolicyInherit::Core);
        assert!(policy.include_only.is_empty());
    }

    #[test]
    fn remote_env_policy_includes_remote_source_vars_without_full_env() {
        let policy = ExecutorStdioServerLauncher::remote_env_policy(
            &["REMOTE_TOKEN".to_string()],
            &HashMap::new(),
        );

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
        let exec_policy = ExecutorStdioServerLauncher::remote_env_policy(
            &["REMOTE_TOKEN".to_string()],
            &HashMap::new(),
        );
        let mut policy = ShellEnvironmentPolicy {
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
                ("SystemRoot".to_string(), r"C:\Windows".to_string()),
                ("SystemDrive".to_string(), "C:".to_string()),
                (
                    "LocalAppData".to_string(),
                    r"C:\Users\test\AppData\Local".to_string(),
                ),
                (
                    "TEMP".to_string(),
                    r"C:\Users\test\AppData\Local\Temp".to_string(),
                ),
                (
                    "TMP".to_string(),
                    r"C:\Users\test\AppData\Local\Temp".to_string(),
                ),
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
        assert_eq!(env.get("SystemDrive").map(String::as_str), Some("C:"));
        assert_eq!(
            env.get("LocalAppData").map(String::as_str),
            Some(r"C:\Users\test\AppData\Local")
        );
        assert_eq!(
            env.get("SystemRoot").map(String::as_str),
            Some(r"C:\Windows")
        );
        for name in ["TEMP", "TMP"] {
            assert_eq!(
                env.get(name).map(String::as_str),
                Some(r"C:\Users\test\AppData\Local\Temp")
            );
        }
        assert_eq!(
            env.get("REMOTE_TOKEN").map(String::as_str),
            Some("remote-secret")
        );
        assert!(!env.contains_key("UNREQUESTED_SECRET"));

        let overrides = HashMap::from([
            ("SYSTEMDRIVE".to_string(), "D:".to_string()),
            ("LOCALAPPDATA".to_string(), r"D:\McpProfile".to_string()),
            ("EXPLICIT_ONLY".to_string(), "configured".to_string()),
        ]);
        policy.r#set = ExecutorStdioServerLauncher::remote_env_policy(
            &["REMOTE_TOKEN".to_string()],
            &overrides,
        )
        .r#set;
        let mut child_env =
            shell_environment::create_env_from_vars(env.clone(), &policy, /*thread_id*/ None);
        assert_eq!(
            child_env.get("LOCALAPPDATA").map(String::as_str),
            Some(r"D:\McpProfile")
        );
        // Older executors apply the final overlay with a case-sensitive merge.
        child_env.extend(overrides.clone());
        let mut expected = env;
        if cfg!(windows) {
            expected.remove("SystemDrive");
            expected.remove("LocalAppData");
        }
        expected.extend(overrides);
        assert_eq!(child_env, expected);
    }
}
