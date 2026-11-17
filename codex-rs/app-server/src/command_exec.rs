use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use codex_app_server_protocol::CommandExecOutputDeltaNotification;
use codex_app_server_protocol::CommandExecOutputStream;
use codex_app_server_protocol::CommandExecResizeParams;
use codex_app_server_protocol::CommandExecResizeResponse;
use codex_app_server_protocol::CommandExecResponse;
use codex_app_server_protocol::CommandExecSandboxCleanup;
use codex_app_server_protocol::CommandExecTerminalSize;
use codex_app_server_protocol::CommandExecTerminateParams;
use codex_app_server_protocol::CommandExecTerminateResponse;
use codex_app_server_protocol::CommandExecWriteParams;
use codex_app_server_protocol::CommandExecWriteResponse;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::ServerNotification;
use codex_core::config::StartedNetworkProxy;
use codex_core::exec::DEFAULT_EXEC_COMMAND_TIMEOUT_MS;
use codex_core::exec::ExecExpiration;
use codex_core::exec::ExecExpirationOutcome;
use codex_core::exec::IO_DRAIN_TIMEOUT_MS;
use codex_core::sandboxing::ExecRequest;
use codex_protocol::exec_output::SandboxCleanupState;
use codex_protocol::exec_output::SandboxCleanupSummary;
use codex_protocol::exec_output::bytes_to_string_smart;
use codex_sandboxing::CleanupDeadlineScope;
use codex_sandboxing::SandboxCleanupCustody;
use codex_sandboxing::SandboxType;
use codex_utils_pty::DEFAULT_OUTPUT_BYTES_CAP;
use codex_utils_pty::ProcessHandle;
use codex_utils_pty::SpawnedProcess;
use codex_utils_pty::TerminalSize;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;

use crate::error_code::internal_error;
use crate::error_code::invalid_params;
use crate::error_code::invalid_request;
use crate::outgoing_message::ConnectionId;
use crate::outgoing_message::ConnectionRequestId;
use crate::outgoing_message::OutgoingMessageSender;

const EXEC_TIMEOUT_EXIT_CODE: i32 = 124;
const OUTPUT_CHUNK_SIZE_HINT: usize = 64 * 1024;
/// Time a sandbox launch's cleanup gets once this owner retires it (command
/// timeout, terminate request, closed connection, or collection without a
/// deadline): the same budget the owner grants output draining after exit.
const SANDBOX_CLEANUP_GRACE: Duration = Duration::from_millis(IO_DRAIN_TIMEOUT_MS);

#[derive(Clone)]
pub(crate) struct CommandExecManager {
    sessions: Arc<Mutex<HashMap<ConnectionProcessId, CommandExecSession>>>,
    next_generated_process_id: Arc<AtomicI64>,
    /// Retirement scope of every sandbox launch this server owns: server
    /// shutdown tightens it, reaching live launches and any started later.
    sandbox_cleanup_scope: CleanupDeadlineScope,
    /// Shutdown evidence: launches whose cleanup is still uncollected, and
    /// launches whose cleanup was not proven complete.
    sandbox_cleanup_ledger: SandboxCleanupLedger,
}

impl Default for CommandExecManager {
    fn default() -> Self {
        Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
            next_generated_process_id: Arc::new(AtomicI64::new(1)),
            sandbox_cleanup_scope: CleanupDeadlineScope::new(),
            sandbox_cleanup_ledger: SandboxCleanupLedger::default(),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ConnectionProcessId {
    connection_id: ConnectionId,
    process_id: InternalProcessId,
}

#[derive(Clone)]
enum CommandExecSession {
    Active {
        control_tx: mpsc::Sender<CommandControlRequest>,
    },
    UnsupportedWindowsSandbox,
}

enum CommandControl {
    Write { delta: Vec<u8>, close_stdin: bool },
    Resize { size: TerminalSize },
    Terminate,
}

struct CommandControlRequest {
    control: CommandControl,
    response_tx: Option<oneshot::Sender<Result<(), JSONRPCErrorError>>>,
}

pub(crate) struct StartCommandExecParams {
    pub(crate) outgoing: Arc<OutgoingMessageSender>,
    pub(crate) request_id: ConnectionRequestId,
    pub(crate) process_id: Option<String>,
    pub(crate) exec_request: ExecRequest,
    pub(crate) started_network_proxy: Option<StartedNetworkProxy>,
    pub(crate) tty: bool,
    pub(crate) stream_stdin: bool,
    pub(crate) stream_stdout_stderr: bool,
    pub(crate) output_bytes_cap: Option<usize>,
    pub(crate) size: Option<TerminalSize>,
}

struct RunCommandParams {
    outgoing: Arc<OutgoingMessageSender>,
    request_id: ConnectionRequestId,
    process_id: Option<String>,
    spawned: SpawnedProcess,
    control_rx: mpsc::Receiver<CommandControlRequest>,
    stream_stdin: bool,
    stream_stdout_stderr: bool,
    expiration: ExecExpiration,
    output_bytes_cap: Option<usize>,
    /// Cleanup custody of the Linux sandbox launcher this command runs
    /// through, held in the app-server process for the whole run.
    cleanup_custody: Option<SandboxCleanupCustody>,
    /// This launch's entry in the manager's shutdown ledger (with custody).
    cleanup_ledger_entry: Option<SandboxCleanupLedgerEntry>,
}

struct SpawnProcessOutputParams {
    connection_id: ConnectionId,
    process_id: Option<String>,
    output_rx: mpsc::Receiver<Vec<u8>>,
    stdio_timeout_rx: watch::Receiver<bool>,
    outgoing: Arc<OutgoingMessageSender>,
    stream: CommandExecOutputStream,
    stream_output: bool,
    output_bytes_cap: Option<usize>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum InternalProcessId {
    Generated(i64),
    Client(String),
}

trait InternalProcessIdExt {
    fn error_repr(&self) -> String;
}

impl InternalProcessIdExt for InternalProcessId {
    fn error_repr(&self) -> String {
        match self {
            Self::Generated(id) => id.to_string(),
            Self::Client(id) => serde_json::to_string(id).unwrap_or_else(|_| format!("{id:?}")),
        }
    }
}

impl CommandExecManager {
    pub(crate) async fn start(
        &self,
        params: StartCommandExecParams,
    ) -> Result<(), JSONRPCErrorError> {
        let StartCommandExecParams {
            outgoing,
            request_id,
            process_id,
            exec_request,
            started_network_proxy,
            tty,
            stream_stdin,
            stream_stdout_stderr,
            output_bytes_cap,
            size,
        } = params;
        if process_id.is_none() && (tty || stream_stdin || stream_stdout_stderr) {
            return Err(invalid_request(
                "command/exec tty or streaming requires a client-supplied processId",
            ));
        }
        let process_id = process_id.map_or_else(
            || {
                InternalProcessId::Generated(
                    self.next_generated_process_id
                        .fetch_add(1, Ordering::Relaxed),
                )
            },
            InternalProcessId::Client,
        );
        let process_key = ConnectionProcessId {
            connection_id: request_id.connection_id,
            process_id: process_id.clone(),
        };

        // The Windows sandbox runs through core's `execute_env`, never through
        // the Linux sandbox launcher, so it has no cleanup custody to hold.
        if matches!(exec_request.sandbox, SandboxType::WindowsRestrictedToken) {
            if tty || stream_stdin || stream_stdout_stderr {
                return Err(invalid_request(
                    "streaming command/exec is not supported with windows sandbox",
                ));
            }
            if output_bytes_cap != Some(DEFAULT_OUTPUT_BYTES_CAP) {
                return Err(invalid_request(
                    "custom outputBytesCap is not supported with windows sandbox",
                ));
            }
            if let InternalProcessId::Client(_) = &process_id {
                let mut sessions = self.sessions.lock().await;
                if sessions.contains_key(&process_key) {
                    return Err(invalid_request(format!(
                        "duplicate active command/exec process id: {}",
                        process_key.process_id.error_repr(),
                    )));
                }
                sessions.insert(
                    process_key.clone(),
                    CommandExecSession::UnsupportedWindowsSandbox,
                );
            }
            let sessions = Arc::clone(&self.sessions);
            tokio::spawn(async move {
                let _started_network_proxy = started_network_proxy;
                match codex_core::sandboxing::execute_env(exec_request, /*stdout_stream*/ None)
                    .await
                {
                    Ok(output) => {
                        outgoing
                            .send_response(
                                request_id,
                                CommandExecResponse {
                                    exit_code: output.exit_code,
                                    stdout: output.stdout.text,
                                    stderr: output.stderr.text,
                                    sandbox_cleanup: None,
                                },
                            )
                            .await;
                    }
                    Err(err) => {
                        outgoing
                            .send_error(request_id, internal_error(format!("exec failed: {err}")))
                            .await;
                    }
                }
                sessions.lock().await.remove(&process_key);
            });
            return Ok(());
        }

        let ExecRequest {
            mut command,
            cwd,
            env,
            expiration,
            sandbox,
            arg0,
            ..
        } = exec_request;
        // TODO(anp): Keep PathUri through the local command launch boundary.
        let cwd = cwd
            .to_abs_path()
            .map_err(|err| invalid_request(format!("invalid command cwd: {err}")))?;

        let stream_stdin = tty || stream_stdin;
        let stream_stdout_stderr = tty || stream_stdout_stderr;
        let (control_tx, control_rx) = mpsc::channel(32);
        let notification_process_id = match &process_id {
            InternalProcessId::Generated(_) => None,
            InternalProcessId::Client(process_id) => Some(process_id.clone()),
        };

        // The app-server is this launch's owner: it holds the launcher's
        // cleanup custody in its own process and collects the cleanup report
        // before it answers the request.
        let mut cleanup_custody = prepare_sandbox_cleanup_custody(
            sandbox,
            &expiration,
            &mut command,
            Instant::now(),
            Some(&self.sandbox_cleanup_scope),
        )?;
        let inherited_fds = cleanup_custody
            .as_ref()
            .map(SandboxCleanupCustody::inherited_fds)
            .unwrap_or_default();

        let sessions = Arc::clone(&self.sessions);
        let (program, args) = command
            .split_first()
            .ok_or_else(|| invalid_request("command must not be empty"))?;
        {
            let mut sessions = self.sessions.lock().await;
            if sessions.contains_key(&process_key) {
                return Err(invalid_request(format!(
                    "duplicate active command/exec process id: {}",
                    process_key.process_id.error_repr(),
                )));
            }
            sessions.insert(
                process_key.clone(),
                CommandExecSession::Active { control_tx },
            );
        }
        // Register before process birth. Exhaustion/unavailable accounting
        // refuses the launch, never spawns uncounted work.
        let cleanup_ledger_entry = match cleanup_custody.as_ref() {
            Some(_) => match self.sandbox_cleanup_ledger.begin() {
                Ok(entry) => Some(entry),
                Err(error) => {
                    self.sessions.lock().await.remove(&process_key);
                    return Err(error);
                }
            },
            None => None,
        };
        let spawned = if tty {
            codex_utils_pty::spawn_pty_process(
                program,
                args,
                cwd.as_path(),
                &env,
                &arg0,
                size.unwrap_or_default(),
                // The custody pipe ends are CLOEXEC; `Attached` makes them
                // inheritable in the child only. With no descriptors it
                // behaves exactly like `Inherited(&[])`.
                codex_utils_pty::ChildFds::Attached(&inherited_fds),
            )
            .await
        } else if stream_stdin {
            codex_utils_pty::spawn_pipe_process(
                program,
                args,
                cwd.as_path(),
                &env,
                &arg0,
                &inherited_fds,
            )
            .await
        } else {
            codex_utils_pty::spawn_pipe_process_no_stdin(
                program,
                args,
                cwd.as_path(),
                &env,
                &arg0,
                &inherited_fds,
            )
            .await
        };
        if let Some(custody) = &mut cleanup_custody {
            // The launcher pin lets the custody observe the launcher's exit
            // (part of the launch's join). A pin, not a pid: the wait task may
            // already have reaped a fast-exiting launcher. `None` when the
            // spawn failed.
            custody.spawned_with_pin(
                spawned
                    .as_ref()
                    .ok()
                    .and_then(|spawned| spawned.session.take_launcher_pin()),
            );
        }
        let spawned = match spawned {
            Ok(spawned) => spawned,
            Err(err) => {
                // Dropping the custody gives it up: a launcher that never
                // started has nothing to clean.
                self.sessions.lock().await.remove(&process_key);
                return Err(internal_error(format!("failed to spawn command: {err}")));
            }
        };
        tokio::spawn(async move {
            let _started_network_proxy = started_network_proxy;
            run_command(RunCommandParams {
                outgoing,
                request_id: request_id.clone(),
                process_id: notification_process_id,
                spawned,
                control_rx,
                stream_stdin,
                stream_stdout_stderr,
                expiration,
                output_bytes_cap,
                cleanup_custody,
                cleanup_ledger_entry,
            })
            .await;
            sessions.lock().await.remove(&process_key);
        });
        Ok(())
    }

    pub(crate) async fn write(
        &self,
        request_id: ConnectionRequestId,
        params: CommandExecWriteParams,
    ) -> Result<CommandExecWriteResponse, JSONRPCErrorError> {
        if params.delta_base64.is_none() && !params.close_stdin {
            return Err(invalid_params(
                "command/exec/write requires deltaBase64 or closeStdin",
            ));
        }

        let delta = match params.delta_base64 {
            Some(delta_base64) => STANDARD
                .decode(delta_base64)
                .map_err(|err| invalid_params(format!("invalid deltaBase64: {err}")))?,
            None => Vec::new(),
        };

        let target_process_id = ConnectionProcessId {
            connection_id: request_id.connection_id,
            process_id: InternalProcessId::Client(params.process_id),
        };
        self.send_control(
            target_process_id,
            CommandControl::Write {
                delta,
                close_stdin: params.close_stdin,
            },
        )
        .await?;

        Ok(CommandExecWriteResponse {})
    }

    pub(crate) async fn terminate(
        &self,
        request_id: ConnectionRequestId,
        params: CommandExecTerminateParams,
    ) -> Result<CommandExecTerminateResponse, JSONRPCErrorError> {
        let target_process_id = ConnectionProcessId {
            connection_id: request_id.connection_id,
            process_id: InternalProcessId::Client(params.process_id),
        };
        self.send_control(target_process_id, CommandControl::Terminate)
            .await?;
        Ok(CommandExecTerminateResponse {})
    }

    pub(crate) async fn resize(
        &self,
        request_id: ConnectionRequestId,
        params: CommandExecResizeParams,
    ) -> Result<CommandExecResizeResponse, JSONRPCErrorError> {
        let target_process_id = ConnectionProcessId {
            connection_id: request_id.connection_id,
            process_id: InternalProcessId::Client(params.process_id),
        };
        self.send_control(
            target_process_id,
            CommandControl::Resize {
                size: terminal_size_from_protocol(params.size)?,
            },
        )
        .await?;
        Ok(CommandExecResizeResponse {})
    }

    /// Retires the sandbox cleanup of every command/exec launch of this
    /// server, live or started later: each deadline tightens to `deadline`
    /// (never extends), or to now + the drain grace when the retiring path
    /// has no deadline of its own.
    pub(crate) fn retire_sandbox_cleanup(&self, deadline: Option<Instant>) {
        self.sandbox_cleanup_scope
            .tighten(deadline.unwrap_or_else(|| Instant::now() + SANDBOX_CLEANUP_GRACE));
    }

    /// The retirement scope's latched cleanup deadline.
    #[cfg(test)]
    pub(crate) fn sandbox_cleanup_deadline(&self) -> Option<Instant> {
        self.sandbox_cleanup_scope.latched()
    }

    /// Server-shutdown evidence for command/exec sandbox launches: retires
    /// them to `deadline`, then waits (never past it) for every in-flight
    /// launch's cleanup to be collected. Clean only if none is left in flight
    /// and no launch of this server had an unproven cleanup.
    pub(crate) async fn sandbox_cleanup_shutdown_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> CommandExecSandboxShutdown {
        self.retire_sandbox_cleanup(Some(deadline.into_std()));
        self.sandbox_cleanup_ledger.observe_until(deadline).await
    }

    pub(crate) async fn connection_closed(&self, connection_id: ConnectionId) {
        let controls = {
            let mut sessions = self.sessions.lock().await;
            let process_ids = sessions
                .keys()
                .filter(|process_id| process_id.connection_id == connection_id)
                .cloned()
                .collect::<Vec<_>>();
            let mut controls = Vec::with_capacity(process_ids.len());
            for process_id in process_ids {
                if let Some(control) = sessions.remove(&process_id) {
                    controls.push(control);
                }
            }
            controls
        };

        // Terminate also tightens each command's sandbox cleanup deadline; the
        // run task still collects the cleanup report after the process exits.
        for control in controls {
            if let CommandExecSession::Active { control_tx } = control {
                let _ = control_tx
                    .send(CommandControlRequest {
                        control: CommandControl::Terminate,
                        response_tx: None,
                    })
                    .await;
            }
        }
    }

    async fn send_control(
        &self,
        process_id: ConnectionProcessId,
        control: CommandControl,
    ) -> Result<(), JSONRPCErrorError> {
        let session = {
            self.sessions
                .lock()
                .await
                .get(&process_id)
                .cloned()
                .ok_or_else(|| {
                    invalid_request(format!(
                        "no active command/exec for process id {}",
                        process_id.process_id.error_repr(),
                    ))
                })?
        };
        let CommandExecSession::Active { control_tx } = session else {
            return Err(invalid_request(
                "command/exec/write, command/exec/terminate, and command/exec/resize are not supported for windows sandbox processes",
            ));
        };
        let (response_tx, response_rx) = oneshot::channel();
        let request = CommandControlRequest {
            control,
            response_tx: Some(response_tx),
        };
        control_tx
            .send(request)
            .await
            .map_err(|_| command_no_longer_running_error(&process_id.process_id))?;
        response_rx
            .await
            .map_err(|_| command_no_longer_running_error(&process_id.process_id))?
    }
}

async fn run_command(params: RunCommandParams) {
    let RunCommandParams {
        outgoing,
        request_id,
        process_id,
        spawned,
        control_rx,
        stream_stdin,
        stream_stdout_stderr,
        expiration,
        output_bytes_cap,
        cleanup_custody,
        cleanup_ledger_entry,
    } = params;
    // Retiring the command (terminate, closed control channel, expiration)
    // also retires its sandbox cleanup: the deadline only ever tightens. The
    // custody itself stays owned here until it is collected.
    let cleanup_tightener = cleanup_custody
        .as_ref()
        .map(SandboxCleanupCustody::tightener);
    let tighten_sandbox_cleanup = || {
        if let Some(tightener) = &cleanup_tightener {
            tightener.tighten(Instant::now() + SANDBOX_CLEANUP_GRACE);
        }
    };
    let mut control_rx = control_rx;
    let mut control_open = true;
    let expiration = expiration.wait_with_outcome();
    tokio::pin!(expiration);
    let SpawnedProcess {
        session,
        stdout_rx,
        stderr_rx,
        exit_rx,
    } = spawned;
    tokio::pin!(exit_rx);
    let mut expiration_outcome = None;
    let (stdio_timeout_tx, stdio_timeout_rx) = watch::channel(false);

    let stdout_handle = spawn_process_output(SpawnProcessOutputParams {
        connection_id: request_id.connection_id,
        process_id: process_id.clone(),
        output_rx: stdout_rx,
        stdio_timeout_rx: stdio_timeout_rx.clone(),
        outgoing: Arc::clone(&outgoing),
        stream: CommandExecOutputStream::Stdout,
        stream_output: stream_stdout_stderr,
        output_bytes_cap,
    });
    let stderr_handle = spawn_process_output(SpawnProcessOutputParams {
        connection_id: request_id.connection_id,
        process_id: process_id.clone(),
        output_rx: stderr_rx,
        stdio_timeout_rx,
        outgoing: Arc::clone(&outgoing),
        stream: CommandExecOutputStream::Stderr,
        stream_output: stream_stdout_stderr,
        output_bytes_cap,
    });

    let exit_code = loop {
        tokio::select! {
            control = control_rx.recv(), if control_open => {
                match control {
                    Some(CommandControlRequest { control, response_tx }) => {
                        let result = match control {
                            CommandControl::Write { delta, close_stdin } => {
                                handle_process_write(
                                    &session,
                                    stream_stdin,
                                    delta,
                                    close_stdin,
                                ).await
                            }
                            CommandControl::Resize { size } => {
                                handle_process_resize(&session, size)
                            }
                            CommandControl::Terminate => {
                                tighten_sandbox_cleanup();
                                session.request_terminate();
                                Ok(())
                            }
                        };
                        if let Some(response_tx) = response_tx {
                            let _ = response_tx.send(result);
                        }
                    },
                    None => {
                        control_open = false;
                        tighten_sandbox_cleanup();
                        session.request_terminate();
                    }
                }
            }
            outcome = &mut expiration, if expiration_outcome.is_none() => {
                expiration_outcome = Some(outcome);
                tighten_sandbox_cleanup();
                session.request_terminate();
            }
            exit = &mut exit_rx => {
                if matches!(expiration_outcome, Some(ExecExpirationOutcome::TimedOut)) {
                    break EXEC_TIMEOUT_EXIT_CODE;
                } else {
                    break exit.unwrap_or(-1);
                }
            }
        }
    };

    let timeout_handle = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(IO_DRAIN_TIMEOUT_MS)).await;
        let _ = stdio_timeout_tx.send(true);
    });

    let stdout = stdout_handle.await.unwrap_or_default();
    let stderr = stderr_handle.await.unwrap_or_default();
    timeout_handle.abort();

    // Collected after exit and output drain, before the response: the report
    // is bounded by the cleanup deadline, never by the command timeout, and
    // its state is reported as observed (a command timeout stays exit code
    // 124; a cleanup timeout stays `timed_out`).
    let sandbox_cleanup = match cleanup_custody {
        Some(custody) => {
            let summary = custody.collect(SANDBOX_CLEANUP_GRACE).await;
            if let Some(entry) = cleanup_ledger_entry {
                entry.finish(&summary);
            }
            Some(CommandExecSandboxCleanup::from(summary))
        }
        None => None,
    };

    outgoing
        .send_response(
            request_id,
            CommandExecResponse {
                exit_code,
                stdout,
                stderr,
                sandbox_cleanup,
            },
        )
        .await;
}

/// Command/exec part of the processor's shutdown evidence.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct CommandExecSandboxShutdown {
    /// Launches whose cleanup was still uncollected when observed.
    pub in_flight: usize,
    /// Launches of this server whose cleanup was not proven: a collected
    /// state other than `complete`, an unjoined launch, or a custody dropped
    /// before collection. Never cleared: preserved targets stay unproven.
    pub unproven: usize,
    /// Counter failure permanently prevents a clean census or new launch.
    pub unavailable: bool,
}

impl CommandExecSandboxShutdown {
    pub(crate) fn is_clean(&self) -> bool {
        self.in_flight == 0 && self.unproven == 0 && !self.unavailable
    }
}

/// Shared shutdown ledger of a manager's sandbox launches.
#[derive(Clone)]
struct SandboxCleanupLedger {
    state: Arc<watch::Sender<CommandExecSandboxShutdown>>,
}

impl Default for SandboxCleanupLedger {
    fn default() -> Self {
        let (state, _) = watch::channel(CommandExecSandboxShutdown::default());
        Self {
            state: Arc::new(state),
        }
    }
}

impl SandboxCleanupLedger {
    fn begin(&self) -> Result<SandboxCleanupLedgerEntry, JSONRPCErrorError> {
        let mut admitted = false;
        self.state.send_modify(|state| {
            if state.unavailable {
                return;
            }
            if let Some(next) = state.in_flight.checked_add(1) {
                state.in_flight = next;
                admitted = true;
            } else {
                state.unavailable = true;
            }
        });
        if !admitted {
            return Err(internal_error("command cleanup census unavailable"));
        }
        Ok(SandboxCleanupLedgerEntry {
            state: Some(Arc::clone(&self.state)),
        })
    }

    async fn observe_until(&self, deadline: tokio::time::Instant) -> CommandExecSandboxShutdown {
        let mut progress = self.state.subscribe();
        let settled = async {
            // The read guard ends with the condition, before any await.
            while progress.borrow_and_update().in_flight != 0 {
                if progress.changed().await.is_err() {
                    break;
                }
            }
        };
        // An expired wait reports the launches still in flight.
        let _ = tokio::time::timeout_at(deadline, settled).await;
        *self.state.borrow()
    }
}

/// One launch's ledger entry. Settled once: by `finish` with the collected
/// summary, or on drop (custody abandoned before collection) as unproven.
struct SandboxCleanupLedgerEntry {
    state: Option<Arc<watch::Sender<CommandExecSandboxShutdown>>>,
}

impl SandboxCleanupLedgerEntry {
    fn finish(mut self, summary: &SandboxCleanupSummary) {
        self.settle(summary.state == SandboxCleanupState::Complete && summary.joined);
    }

    fn settle(&mut self, proven: bool) {
        if let Some(state) = self.state.take() {
            state.send_modify(|state| {
                if let Some(next) = state.in_flight.checked_sub(1) {
                    state.in_flight = next;
                } else {
                    state.unavailable = true;
                }
                if !proven {
                    if let Some(next) = state.unproven.checked_add(1) {
                        state.unproven = next;
                    } else {
                        state.unavailable = true;
                    }
                }
            });
        }
    }
}

impl Drop for SandboxCleanupLedgerEntry {
    fn drop(&mut self) {
        self.settle(/*proven*/ false);
    }
}

/// Creates the cleanup custody for a launch through the Linux sandbox
/// launcher, registered with the server's retirement `scope`, and attaches it
/// to the launcher argv; `None` for any other launch.
fn prepare_sandbox_cleanup_custody(
    sandbox: SandboxType,
    expiration: &ExecExpiration,
    command: &mut Vec<String>,
    start: Instant,
    scope: Option<&CleanupDeadlineScope>,
) -> Result<Option<SandboxCleanupCustody>, JSONRPCErrorError> {
    let custody = SandboxCleanupCustody::for_launch(
        sandbox,
        original_sandbox_cleanup_deadline(expiration, start),
        scope,
    )
    .map_err(|err| internal_error(format!("failed to create sandbox cleanup custody: {err}")))?;
    if let Some(custody) = &custody {
        custody.attach_to_launcher_argv(command).map_err(|err| {
            internal_error(format!("failed to attach sandbox cleanup custody: {err}"))
        })?;
    }
    Ok(custody)
}

/// The launch's original cleanup deadline: the command timeout plus the
/// output drain this owner grants after exit. A command without a timeout
/// has none until it is terminated or its result is collected; so does a
/// timeout too large to represent as an `Instant`.
fn original_sandbox_cleanup_deadline(
    expiration: &ExecExpiration,
    start: Instant,
) -> Option<Instant> {
    let timeout = match expiration {
        ExecExpiration::Timeout(timeout)
        | ExecExpiration::TimeoutOrCancellation { timeout, .. } => *timeout,
        ExecExpiration::DefaultTimeout => Duration::from_millis(DEFAULT_EXEC_COMMAND_TIMEOUT_MS),
        ExecExpiration::Cancellation(_) => return None,
    };
    start
        .checked_add(timeout)?
        .checked_add(SANDBOX_CLEANUP_GRACE)
}

fn spawn_process_output(params: SpawnProcessOutputParams) -> tokio::task::JoinHandle<String> {
    let SpawnProcessOutputParams {
        connection_id,
        process_id,
        mut output_rx,
        mut stdio_timeout_rx,
        outgoing,
        stream,
        stream_output,
        output_bytes_cap,
    } = params;
    tokio::spawn(async move {
        let mut buffer: Vec<u8> = Vec::new();
        let mut observed_num_bytes = 0usize;
        loop {
            let mut chunk = tokio::select! {
                chunk = output_rx.recv() => match chunk {
                    Some(chunk) => chunk,
                    None => break,
                },
                _ = stdio_timeout_rx.wait_for(|&v| v) => break,
            };
            // Individual chunks are at most 8KiB, so overshooting a bit is acceptable.
            while chunk.len() < OUTPUT_CHUNK_SIZE_HINT
                && let Ok(next_chunk) = output_rx.try_recv()
            {
                chunk.extend_from_slice(&next_chunk);
            }
            let capped_chunk = match output_bytes_cap {
                Some(output_bytes_cap) => {
                    let capped_chunk_len = output_bytes_cap
                        .saturating_sub(observed_num_bytes)
                        .min(chunk.len());
                    observed_num_bytes += capped_chunk_len;
                    &chunk[0..capped_chunk_len]
                }
                None => chunk.as_slice(),
            };
            let cap_reached = Some(observed_num_bytes) == output_bytes_cap;
            if let (true, Some(process_id)) = (stream_output, process_id.as_ref()) {
                outgoing
                    .send_server_notification_to_connection_and_wait(
                        connection_id,
                        ServerNotification::CommandExecOutputDelta(
                            CommandExecOutputDeltaNotification {
                                process_id: process_id.clone(),
                                stream,
                                delta_base64: STANDARD.encode(capped_chunk),
                                cap_reached,
                            },
                        ),
                    )
                    .await;
            } else if !stream_output {
                buffer.extend_from_slice(capped_chunk);
            }
            if cap_reached {
                break;
            }
        }
        bytes_to_string_smart(&buffer)
    })
}

async fn handle_process_write(
    session: &ProcessHandle,
    stream_stdin: bool,
    delta: Vec<u8>,
    close_stdin: bool,
) -> Result<(), JSONRPCErrorError> {
    if !stream_stdin {
        return Err(invalid_request(
            "stdin streaming is not enabled for this command/exec",
        ));
    }
    if !delta.is_empty() {
        session
            .writer_sender()
            .send(delta)
            .await
            .map_err(|_| invalid_request("stdin is already closed"))?;
    }
    if close_stdin {
        session.close_stdin();
    }
    Ok(())
}

fn handle_process_resize(
    session: &ProcessHandle,
    size: TerminalSize,
) -> Result<(), JSONRPCErrorError> {
    session
        .resize(size)
        .map_err(|err| invalid_request(format!("failed to resize PTY: {err}")))
}

pub(crate) fn terminal_size_from_protocol(
    size: CommandExecTerminalSize,
) -> Result<TerminalSize, JSONRPCErrorError> {
    if size.rows == 0 || size.cols == 0 {
        return Err(invalid_params(
            "command/exec size rows and cols must be greater than 0",
        ));
    }
    Ok(TerminalSize {
        rows: size.rows,
        cols: size.cols,
    })
}

fn command_no_longer_running_error(process_id: &InternalProcessId) -> JSONRPCErrorError {
    invalid_request(format!(
        "command/exec {} is no longer running",
        process_id.error_repr(),
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::error_code::INVALID_REQUEST_ERROR_CODE;
    use codex_protocol::config_types::WindowsSandboxLevel;
    use codex_protocol::models::PermissionProfile;
    use codex_utils_absolute_path::AbsolutePathBuf;
    use pretty_assertions::assert_eq;
    #[cfg(not(target_os = "windows"))]
    use tokio::time::Duration;
    #[cfg(not(target_os = "windows"))]
    use tokio::time::timeout;
    #[cfg(not(target_os = "windows"))]
    use tokio_util::sync::CancellationToken;

    use super::*;
    #[cfg(not(target_os = "windows"))]
    use crate::outgoing_message::OutgoingEnvelope;
    #[cfg(not(target_os = "windows"))]
    use crate::outgoing_message::OutgoingMessage;

    fn windows_sandbox_exec_request() -> ExecRequest {
        let cwd = AbsolutePathBuf::current_dir().expect("current dir");
        ExecRequest::new(
            vec!["cmd".to_string()],
            cwd.clone(),
            HashMap::new(),
            /*network*/ None,
            /*network_environment_id*/ None,
            ExecExpiration::DefaultTimeout,
            codex_core::exec::ExecCapturePolicy::ShellTool,
            SandboxType::WindowsRestrictedToken,
            vec![cwd],
            WindowsSandboxLevel::Disabled,
            PermissionProfile::read_only(),
            /*arg0*/ None,
        )
    }

    #[tokio::test]
    async fn windows_sandbox_streaming_exec_is_rejected() {
        let (tx, _rx) = mpsc::channel(1);
        let manager = CommandExecManager::default();
        let err = manager
            .start(StartCommandExecParams {
                outgoing: Arc::new(OutgoingMessageSender::new(
                    tx,
                    codex_analytics::AnalyticsEventsClient::disabled(),
                )),
                request_id: ConnectionRequestId {
                    connection_id: ConnectionId(1),
                    request_id: codex_app_server_protocol::RequestId::Integer(42),
                },
                process_id: Some("proc-42".to_string()),
                exec_request: windows_sandbox_exec_request(),
                started_network_proxy: None,
                tty: false,
                stream_stdin: false,
                stream_stdout_stderr: true,
                output_bytes_cap: None,
                size: None,
            })
            .await
            .expect_err("streaming windows sandbox exec should be rejected");

        assert_eq!(err.code, INVALID_REQUEST_ERROR_CODE);
        assert_eq!(
            err.message,
            "streaming command/exec is not supported with windows sandbox"
        );
    }

    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    async fn windows_sandbox_non_streaming_exec_uses_execution_path() {
        let (tx, mut rx) = mpsc::channel(1);
        let manager = CommandExecManager::default();
        let request_id = ConnectionRequestId {
            connection_id: ConnectionId(7),
            request_id: codex_app_server_protocol::RequestId::Integer(99),
        };

        manager
            .start(StartCommandExecParams {
                outgoing: Arc::new(OutgoingMessageSender::new(
                    tx,
                    codex_analytics::AnalyticsEventsClient::disabled(),
                )),
                request_id: request_id.clone(),
                process_id: Some("proc-99".to_string()),
                exec_request: windows_sandbox_exec_request(),
                started_network_proxy: None,
                tty: false,
                stream_stdin: false,
                stream_stdout_stderr: false,
                output_bytes_cap: Some(DEFAULT_OUTPUT_BYTES_CAP),
                size: None,
            })
            .await
            .expect("non-streaming windows sandbox exec should start");

        let envelope = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("timed out waiting for outgoing message")
            .expect("channel closed before outgoing message");
        let OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            ..
        } = envelope
        else {
            panic!("expected connection-scoped outgoing message");
        };
        assert_eq!(connection_id, request_id.connection_id);
        let OutgoingMessage::Error(error) = message else {
            panic!("expected execution failure to be reported as an error");
        };
        assert_eq!(error.id, request_id.request_id);
        assert!(error.error.message.starts_with("exec failed:"));
    }

    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    async fn cancellation_expiration_keeps_process_alive_until_terminated() {
        let (tx, mut rx) = mpsc::channel(4);
        let manager = CommandExecManager::default();
        let request_id = ConnectionRequestId {
            connection_id: ConnectionId(8),
            request_id: codex_app_server_protocol::RequestId::Integer(100),
        };
        let cwd = AbsolutePathBuf::current_dir().expect("current dir");

        manager
            .start(StartCommandExecParams {
                outgoing: Arc::new(OutgoingMessageSender::new(
                    tx,
                    codex_analytics::AnalyticsEventsClient::disabled(),
                )),
                request_id: request_id.clone(),
                process_id: Some("proc-100".to_string()),
                exec_request: ExecRequest::new(
                    vec!["sh".to_string(), "-lc".to_string(), "sleep 30".to_string()],
                    cwd.clone(),
                    HashMap::new(),
                    /*network*/ None,
                    /*network_environment_id*/ None,
                    ExecExpiration::Cancellation(CancellationToken::new()),
                    codex_core::exec::ExecCapturePolicy::ShellTool,
                    SandboxType::None,
                    vec![cwd.clone()],
                    WindowsSandboxLevel::Disabled,
                    PermissionProfile::read_only(),
                    /*arg0*/ None,
                ),
                started_network_proxy: None,
                tty: false,
                stream_stdin: false,
                stream_stdout_stderr: false,
                output_bytes_cap: Some(DEFAULT_OUTPUT_BYTES_CAP),
                size: None,
            })
            .await
            .expect("cancellation-based exec should start");

        assert!(
            timeout(Duration::from_millis(250), rx.recv())
                .await
                .is_err(),
            "command/exec should remain active until explicit termination",
        );

        manager
            .terminate(
                request_id.clone(),
                CommandExecTerminateParams {
                    process_id: "proc-100".to_string(),
                },
            )
            .await
            .expect("terminate should succeed");

        let envelope = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("timed out waiting for outgoing message")
            .expect("channel closed before outgoing message");
        let OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            ..
        } = envelope
        else {
            panic!("expected connection-scoped outgoing message");
        };
        assert_eq!(connection_id, request_id.connection_id);
        let OutgoingMessage::Response(response) = message else {
            panic!("expected execution response after termination");
        };
        assert_eq!(response.id, request_id.request_id);
        let codex_app_server_protocol::ClientResponsePayload::OneOffCommandExec(response) =
            *response.result
        else {
            panic!("expected command/exec response");
        };
        assert_ne!(response.exit_code, 0);
        assert_eq!(response.stdout, "");
        // The deferred response now drains any already-emitted stderr before
        // replying, so shell startup noise is allowed here.
    }

    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    async fn timeout_or_cancellation_reports_cancellation_without_timeout_exit_code() {
        let (tx, mut rx) = mpsc::channel(4);
        let manager = CommandExecManager::default();
        let request_id = ConnectionRequestId {
            connection_id: ConnectionId(9),
            request_id: codex_app_server_protocol::RequestId::Integer(101),
        };
        let cancellation = CancellationToken::new();
        let cancel = cancellation.clone();
        let cwd = AbsolutePathBuf::current_dir().expect("current dir");

        manager
            .start(StartCommandExecParams {
                outgoing: Arc::new(OutgoingMessageSender::new(
                    tx,
                    codex_analytics::AnalyticsEventsClient::disabled(),
                )),
                request_id: request_id.clone(),
                process_id: Some("proc-101".to_string()),
                exec_request: ExecRequest::new(
                    vec!["sh".to_string(), "-lc".to_string(), "sleep 30".to_string()],
                    cwd.clone(),
                    HashMap::new(),
                    /*network*/ None,
                    /*network_environment_id*/ None,
                    ExecExpiration::TimeoutOrCancellation {
                        timeout: Duration::from_secs(30),
                        cancellation,
                    },
                    codex_core::exec::ExecCapturePolicy::ShellTool,
                    SandboxType::None,
                    vec![cwd],
                    WindowsSandboxLevel::Disabled,
                    PermissionProfile::read_only(),
                    /*arg0*/ None,
                ),
                started_network_proxy: None,
                tty: false,
                stream_stdin: false,
                stream_stdout_stderr: false,
                output_bytes_cap: Some(DEFAULT_OUTPUT_BYTES_CAP),
                size: None,
            })
            .await
            .expect("timeout-or-cancellation exec should start");

        cancel.cancel();

        let envelope = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("timed out waiting for outgoing message")
            .expect("channel closed before outgoing message");
        let OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            ..
        } = envelope
        else {
            panic!("expected connection-scoped outgoing message");
        };
        assert_eq!(connection_id, request_id.connection_id);
        let OutgoingMessage::Response(response) = message else {
            panic!("expected execution response after cancellation");
        };
        assert_eq!(response.id, request_id.request_id);
        let codex_app_server_protocol::ClientResponsePayload::OneOffCommandExec(response) =
            *response.result
        else {
            panic!("expected command/exec response");
        };
        assert_ne!(response.exit_code, EXEC_TIMEOUT_EXIT_CODE);
        // An unsandboxed launch has no launcher, so no cleanup custody.
        assert_eq!(response.sandbox_cleanup, None);
    }

    #[tokio::test]
    async fn windows_sandbox_process_ids_reject_write_requests() {
        let manager = CommandExecManager::default();
        let request_id = ConnectionRequestId {
            connection_id: ConnectionId(11),
            request_id: codex_app_server_protocol::RequestId::Integer(1),
        };
        let process_id = ConnectionProcessId {
            connection_id: request_id.connection_id,
            process_id: InternalProcessId::Client("proc-11".to_string()),
        };
        manager
            .sessions
            .lock()
            .await
            .insert(process_id, CommandExecSession::UnsupportedWindowsSandbox);

        let err = manager
            .write(
                request_id,
                CommandExecWriteParams {
                    process_id: "proc-11".to_string(),
                    delta_base64: Some(STANDARD.encode("hello")),
                    close_stdin: false,
                },
            )
            .await
            .expect_err("windows sandbox process ids should reject command/exec/write");

        assert_eq!(err.code, INVALID_REQUEST_ERROR_CODE);
        assert_eq!(
            err.message,
            "command/exec/write, command/exec/terminate, and command/exec/resize are not supported for windows sandbox processes"
        );
    }

    #[tokio::test]
    async fn windows_sandbox_process_ids_reject_terminate_requests() {
        let manager = CommandExecManager::default();
        let request_id = ConnectionRequestId {
            connection_id: ConnectionId(12),
            request_id: codex_app_server_protocol::RequestId::Integer(2),
        };
        let process_id = ConnectionProcessId {
            connection_id: request_id.connection_id,
            process_id: InternalProcessId::Client("proc-12".to_string()),
        };
        manager
            .sessions
            .lock()
            .await
            .insert(process_id, CommandExecSession::UnsupportedWindowsSandbox);

        let err = manager
            .terminate(
                request_id,
                CommandExecTerminateParams {
                    process_id: "proc-12".to_string(),
                },
            )
            .await
            .expect_err("windows sandbox process ids should reject command/exec/terminate");

        assert_eq!(err.code, INVALID_REQUEST_ERROR_CODE);
        assert_eq!(
            err.message,
            "command/exec/write, command/exec/terminate, and command/exec/resize are not supported for windows sandbox processes"
        );
    }

    #[tokio::test]
    async fn dropped_control_request_is_reported_as_not_running() {
        let manager = CommandExecManager::default();
        let request_id = ConnectionRequestId {
            connection_id: ConnectionId(13),
            request_id: codex_app_server_protocol::RequestId::Integer(3),
        };
        let process_id = InternalProcessId::Client("proc-13".to_string());
        let (control_tx, mut control_rx) = mpsc::channel(1);
        manager.sessions.lock().await.insert(
            ConnectionProcessId {
                connection_id: request_id.connection_id,
                process_id: process_id.clone(),
            },
            CommandExecSession::Active { control_tx },
        );

        tokio::spawn(async move {
            let _request = control_rx
                .recv()
                .await
                .expect("expected queued control request");
        });

        let err = manager
            .terminate(
                request_id,
                CommandExecTerminateParams {
                    process_id: "proc-13".to_string(),
                },
            )
            .await
            .expect_err("dropped control request should be treated as not running");

        assert_eq!(err.code, INVALID_REQUEST_ERROR_CODE);
        assert_eq!(err.message, "command/exec \"proc-13\" is no longer running");
    }

    #[test]
    fn original_sandbox_cleanup_deadline_is_command_timeout_plus_drain() {
        let start = Instant::now();
        let default_timeout = Duration::from_millis(DEFAULT_EXEC_COMMAND_TIMEOUT_MS);

        assert_eq!(
            original_sandbox_cleanup_deadline(
                &ExecExpiration::Timeout(Duration::from_secs(5)),
                start,
            ),
            Some(start + Duration::from_secs(5) + SANDBOX_CLEANUP_GRACE)
        );
        assert_eq!(
            original_sandbox_cleanup_deadline(&ExecExpiration::DefaultTimeout, start),
            Some(start + default_timeout + SANDBOX_CLEANUP_GRACE)
        );
        // `disableTimeout`: no original deadline; terminate or collection
        // supplies one later.
        assert_eq!(
            original_sandbox_cleanup_deadline(
                &ExecExpiration::Cancellation(tokio_util::sync::CancellationToken::new()),
                start,
            ),
            None
        );
    }

    #[test]
    fn unsandboxed_launch_has_no_cleanup_custody() {
        let mut command = vec!["sh".to_string(), "-c".to_string(), "true".to_string()];
        let custody = prepare_sandbox_cleanup_custody(
            SandboxType::None,
            &ExecExpiration::DefaultTimeout,
            &mut command,
            Instant::now(),
            /*scope*/ None,
        )
        .expect("no custody is needed");

        assert!(custody.is_none());
        assert_eq!(
            command,
            vec!["sh".to_string(), "-c".to_string(), "true".to_string()]
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_sandbox_launch_carries_cleanup_custody_in_argv() {
        let mut command = vec![
            "/usr/bin/codex-linux-sandbox".to_string(),
            "--sandbox-policy-cwd".to_string(),
            "/tmp".to_string(),
            "--".to_string(),
            "true".to_string(),
        ];
        let custody = prepare_sandbox_cleanup_custody(
            SandboxType::LinuxSeccomp,
            &ExecExpiration::DefaultTimeout,
            &mut command,
            Instant::now(),
            /*scope*/ None,
        )
        .expect("custody")
        .expect("a Linux sandbox launch has custody");

        let separator = command
            .iter()
            .position(|arg| arg == "--")
            .expect("argv keeps its separator");
        let launcher_args = &command[..separator];
        // The custody flags precede `--`, so the launcher (not the sandboxed
        // command) receives them; the command after `--` is untouched.
        for flag in [
            "--cleanup-deadline-monotonic-ns",
            "--cleanup-deadline-fd",
            "--cleanup-report-fd",
        ] {
            assert!(
                launcher_args.iter().any(|arg| arg == flag),
                "missing {flag} in {command:?}"
            );
        }
        assert_eq!(&command[separator + 1..], ["true".to_string()]);
        let inherited_fds = custody.inherited_fds();
        assert_eq!(inherited_fds.len(), 2);
        for fd in inherited_fds {
            assert!(
                launcher_args.iter().any(|arg| arg == &fd.to_string()),
                "inherited fd {fd} is not named in {command:?}"
            );
        }
        assert!(custody.deadline().is_some());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn retiring_the_manager_scope_tightens_live_and_later_launches() {
        let manager = CommandExecManager::default();
        let launcher_argv = || {
            vec![
                "/usr/bin/codex-linux-sandbox".to_string(),
                "--".to_string(),
                "true".to_string(),
            ]
        };
        let launch = |manager: &CommandExecManager| {
            prepare_sandbox_cleanup_custody(
                SandboxType::LinuxSeccomp,
                // `disableTimeout`: no original deadline of its own.
                &ExecExpiration::Cancellation(tokio_util::sync::CancellationToken::new()),
                &mut launcher_argv(),
                Instant::now(),
                Some(&manager.sandbox_cleanup_scope),
            )
            .expect("custody")
            .expect("a Linux sandbox launch has custody")
        };

        let live = launch(&manager);
        assert_eq!(live.deadline(), None);

        let shutdown_deadline = Instant::now() + Duration::from_secs(1);
        manager.retire_sandbox_cleanup(Some(shutdown_deadline));
        assert_eq!(live.deadline(), Some(shutdown_deadline));

        // Retirement never extends a deadline.
        manager.retire_sandbox_cleanup(Some(shutdown_deadline + Duration::from_secs(60)));
        assert_eq!(live.deadline(), Some(shutdown_deadline));

        // A launch registered after retirement starts with the latched deadline.
        let later = launch(&manager);
        assert_eq!(later.deadline(), Some(shutdown_deadline));
    }

    fn cleanup_summary(state: SandboxCleanupState, joined: bool) -> SandboxCleanupSummary {
        SandboxCleanupSummary {
            state,
            reaped: 1,
            preserved_unproven: 0,
            host_owned: 0,
            handed_off: 0,
            timed_out: 0,
            joined,
        }
    }

    fn shortly() -> tokio::time::Instant {
        tokio::time::Instant::now() + std::time::Duration::from_millis(20)
    }

    #[tokio::test]
    async fn proven_command_exec_cleanup_gives_clean_shutdown_evidence() {
        let manager = CommandExecManager::default();
        assert!(
            manager
                .sandbox_cleanup_shutdown_until(shortly())
                .await
                .is_clean()
        );

        manager
            .sandbox_cleanup_ledger
            .begin()
            .expect("test cleanup census")
            .finish(&cleanup_summary(
                SandboxCleanupState::Complete,
                /*joined*/ true,
            ));

        assert_eq!(
            manager.sandbox_cleanup_shutdown_until(shortly()).await,
            CommandExecSandboxShutdown::default()
        );
    }

    #[tokio::test]
    async fn unproven_command_exec_cleanup_makes_shutdown_evidence_non_clean() {
        for summary in [
            cleanup_summary(SandboxCleanupState::Incomplete, /*joined*/ true),
            cleanup_summary(SandboxCleanupState::TimedOut, /*joined*/ false),
            cleanup_summary(SandboxCleanupState::Unreported, /*joined*/ false),
            // Complete but unjoined: a cleanup helper's exit was not proven.
            cleanup_summary(SandboxCleanupState::Complete, /*joined*/ false),
        ] {
            let manager = CommandExecManager::default();
            manager
                .sandbox_cleanup_ledger
                .begin()
                .expect("test cleanup census")
                .finish(&summary);

            let evidence = manager.sandbox_cleanup_shutdown_until(shortly()).await;

            assert_eq!(
                evidence,
                CommandExecSandboxShutdown {
                    in_flight: 0,
                    unproven: 1,
                    unavailable: false,
                },
                "{summary:?}"
            );
            assert!(!evidence.is_clean());
        }
    }

    #[tokio::test]
    async fn uncollected_or_abandoned_command_exec_cleanup_is_not_clean() {
        let manager = CommandExecManager::default();
        let entry = manager
            .sandbox_cleanup_ledger
            .begin()
            .expect("test cleanup census");

        // Still in flight at the shutdown deadline.
        assert_eq!(
            manager.sandbox_cleanup_shutdown_until(shortly()).await,
            CommandExecSandboxShutdown {
                in_flight: 1,
                unproven: 0,
                unavailable: false,
            }
        );

        // Custody dropped before its cleanup was collected.
        drop(entry);
        assert_eq!(
            manager.sandbox_cleanup_shutdown_until(shortly()).await,
            CommandExecSandboxShutdown {
                in_flight: 0,
                unproven: 1,
                unavailable: false,
            }
        );
    }

    #[tokio::test]
    async fn shutdown_evidence_waits_for_in_flight_cleanup_until_the_deadline() {
        let manager = CommandExecManager::default();
        let entry = manager
            .sandbox_cleanup_ledger
            .begin()
            .expect("test cleanup census");
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        let observer = tokio::spawn({
            let manager = manager.clone();
            async move { manager.sandbox_cleanup_shutdown_until(deadline).await }
        });

        entry.finish(&cleanup_summary(
            SandboxCleanupState::Complete,
            /*joined*/ true,
        ));

        assert_eq!(
            observer.await.expect("observer joins"),
            CommandExecSandboxShutdown::default()
        );
    }
    #[tokio::test]
    async fn exhausted_cleanup_census_refuses_birth_and_never_wraps_to_clean() {
        let ledger = SandboxCleanupLedger::default();
        ledger
            .state
            .send_modify(|state| state.in_flight = usize::MAX);
        assert!(ledger.begin().is_err());
        let seen = ledger.observe_until(tokio::time::Instant::now()).await;
        assert_eq!(seen.in_flight, usize::MAX);
        assert!(seen.unavailable);
        assert!(!seen.is_clean());
        assert!(ledger.begin().is_err());
    }

    #[tokio::test]
    async fn exhausted_unproven_count_stays_sticky_instead_of_becoming_zero() {
        let ledger = SandboxCleanupLedger::default();
        let entry = ledger.begin().expect("initial admission");
        ledger
            .state
            .send_modify(|state| state.unproven = usize::MAX);
        drop(entry);
        let seen = ledger.observe_until(tokio::time::Instant::now()).await;
        assert_eq!(seen.in_flight, 0);
        assert_eq!(seen.unproven, usize::MAX);
        assert!(seen.unavailable);
        assert!(!seen.is_clean());
        assert!(ledger.begin().is_err());
    }
}
