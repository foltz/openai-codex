//! Bounded, compatibility-aware transport for modern locally spawned MCP servers.

use std::future::Future;
use std::io;
use std::process::Stdio;
use std::sync::Arc;

use memchr::memchr;
use rmcp::service::RoleClient;
use rmcp::service::RxJsonRpcMessage;
use rmcp::service::TxJsonRpcMessage;
use rmcp::transport::Transport;
use rmcp::transport::async_rw::AsyncRwTransport;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::process::ChildStderr;
use tokio::process::ChildStdin;
use tokio::process::ChildStdout;
use tokio::process::Command;
use tokio::sync::Mutex;
use tokio::sync::watch;
use tokio::task::AbortHandle;
use tracing::debug;
use tracing::warn;

use crate::incoming_jsonrpc::deserialize_incoming_jsonrpc_message;

/// Match the existing executor stdio transport and the production MCP limit.
pub(crate) const MAX_MCP_STDIO_LINE_BYTES: usize = 8 * 1024 * 1024;

pub(super) struct LocalStdioTransport {
    stdin: Arc<Mutex<Option<ChildStdin>>>,
    stdout: BufReader<ChildStdout>,
    pending_line: Vec<u8>,
    program_name: String,
    process_id: Option<u32>,
    exit_observer: LocalProcessExitObserver,
}

/// Compatibility transport for legacy MCP servers. It retains rmcp's standard
/// AsyncRw framing and error behavior while sharing the local exit observer
/// with the modern bounded transport.
pub(super) struct LegacyLocalStdioTransport {
    transport: AsyncRwTransport<RoleClient, ChildStdout, ChildStdin>,
    process_id: Option<u32>,
    exit_observer: LocalProcessExitObserver,
}

struct SpawnedLocalStdioProcess {
    stdin: ChildStdin,
    stdout: ChildStdout,
    stderr: Option<ChildStderr>,
    process_id: Option<u32>,
    exit_observer: LocalProcessExitObserver,
}

/// One terminal observation for a locally spawned stdio process.
///
/// The supervisor owns the child and sends exactly one terminal result. This
/// lets the transport and the separately-held process handle await the same
/// fact without racing to call `Child::wait`.
#[derive(Clone)]
pub(super) struct LocalProcessExitObserver {
    state: watch::Receiver<LocalProcessExit>,
    supervisor: Arc<LocalProcessSupervisor>,
}

struct LocalProcessSupervisor {
    abort_handle: AbortHandle,
}

impl Drop for LocalProcessSupervisor {
    fn drop(&mut self) {
        // A dropped AbortHandle alone detaches its task. The final observer
        // instead releases the owned Child and preserves kill_on_drop, even
        // when launcher setup fails before a process handle can be created.
        self.abort_handle.abort();
    }
}

#[derive(Clone, Debug, Default)]
enum LocalProcessExit {
    #[default]
    Pending,
    Exited,
    Failed(Arc<str>),
}

impl LocalProcessExitObserver {
    pub(super) async fn wait(&mut self) -> io::Result<()> {
        loop {
            let state = self.state.borrow_and_update().clone();
            match state {
                LocalProcessExit::Pending => {
                    self.state.changed().await.map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "local MCP exit observer closed before terminal exit",
                        )
                    })?;
                }
                LocalProcessExit::Exited => return Ok(()),
                LocalProcessExit::Failed(message) => {
                    return Err(io::Error::other(message.to_string()));
                }
            }
        }
    }

    /// Drops the child owned by the supervisor, preserving `kill_on_drop` when
    /// no platform process-group or job terminator could be created.
    pub(super) fn abort(&self) {
        self.supervisor.abort_handle.abort();
    }
}

impl LocalStdioTransport {
    pub(super) fn spawn(
        command: Command,
        program_name: String,
    ) -> io::Result<(Self, Option<ChildStderr>)> {
        let process = spawn_local_stdio_process(command)?;

        Ok((
            Self {
                stdin: Arc::new(Mutex::new(Some(process.stdin))),
                stdout: BufReader::new(process.stdout),
                pending_line: Vec::new(),
                program_name,
                process_id: process.process_id,
                exit_observer: process.exit_observer,
            },
            process.stderr,
        ))
    }

    pub(super) fn id(&self) -> Option<u32> {
        self.process_id
    }

    pub(super) fn exit_observer(&self) -> LocalProcessExitObserver {
        self.exit_observer.clone()
    }

    pub(super) async fn close_input(&self) {
        self.stdin.lock().await.take();
    }

    async fn receive_message(&mut self) -> Option<RxJsonRpcMessage<RoleClient>> {
        loop {
            let bytes = match self.stdout.fill_buf().await {
                Ok(bytes) => bytes,
                Err(error) => {
                    warn!(
                        "Failed to read MCP server stdout ({}): {error}",
                        self.program_name
                    );
                    return None;
                }
            };

            if bytes.is_empty() {
                if self.pending_line.is_empty() {
                    return None;
                }
                return self.decode_pending_message();
            }

            let newline = memchr(b'\n', bytes);
            let content_len = newline.unwrap_or(bytes.len());
            if content_len > MAX_MCP_STDIO_LINE_BYTES.saturating_sub(self.pending_line.len()) {
                warn!(
                    "MCP stdio line exceeds {MAX_MCP_STDIO_LINE_BYTES} bytes ({}); closing transport",
                    self.program_name
                );
                self.pending_line.clear();
                return None;
            }

            self.pending_line.extend_from_slice(&bytes[..content_len]);
            let consumed = content_len + usize::from(newline.is_some());
            self.stdout.consume(consumed);

            if newline.is_some()
                && let Some(message) = self.decode_pending_message()
            {
                return Some(message);
            }
        }
    }

    fn decode_pending_message(&mut self) -> Option<RxJsonRpcMessage<RoleClient>> {
        let line = std::mem::take(&mut self.pending_line);
        let line = line.strip_suffix(b"\r").unwrap_or(&line);
        match deserialize_incoming_jsonrpc_message(line) {
            Ok(message) => Some(message),
            Err(error) => {
                debug!(
                    "Failed to parse local MCP server message ({}): {error}",
                    self.program_name
                );
                None
            }
        }
    }
}

impl LegacyLocalStdioTransport {
    pub(super) fn spawn(command: Command) -> io::Result<(Self, Option<ChildStderr>)> {
        let process = spawn_local_stdio_process(command)?;
        Ok((
            Self {
                transport: AsyncRwTransport::new(process.stdout, process.stdin),
                process_id: process.process_id,
                exit_observer: process.exit_observer,
            },
            process.stderr,
        ))
    }

    pub(super) fn id(&self) -> Option<u32> {
        self.process_id
    }

    pub(super) fn exit_observer(&self) -> LocalProcessExitObserver {
        self.exit_observer.clone()
    }
}

fn spawn_local_stdio_process(mut command: Command) -> io::Result<SpawnedLocalStdioProcess> {
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn()?;
    let process_id = child.id();
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| io::Error::other("MCP server stdin was not piped"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| io::Error::other("MCP server stdout was not piped"))?;
    let stderr = child.stderr.take();
    let (exit_tx, exit_rx) = watch::channel(LocalProcessExit::Pending);
    let supervisor = tokio::spawn(async move {
        let terminal = match child.wait().await {
            Ok(_) => LocalProcessExit::Exited,
            Err(error) => LocalProcessExit::Failed(Arc::from(error.to_string())),
        };
        let _ = exit_tx.send(terminal);
    });

    Ok(SpawnedLocalStdioProcess {
        stdin,
        stdout,
        stderr,
        process_id,
        exit_observer: LocalProcessExitObserver {
            state: exit_rx,
            supervisor: Arc::new(LocalProcessSupervisor {
                abort_handle: supervisor.abort_handle(),
            }),
        },
    })
}

impl Transport<RoleClient> for LocalStdioTransport {
    type Error = io::Error;

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "complete JSON-RPC frames must hold the stdin lock across writes"
    )]
    fn send(
        &mut self,
        item: TxJsonRpcMessage<RoleClient>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        let stdin = Arc::clone(&self.stdin);
        async move {
            let mut message = serde_json::to_vec(&item).map_err(io::Error::other)?;
            message.push(b'\n');
            let mut guard = stdin.lock().await;
            let stdin = guard
                .as_mut()
                .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "MCP stdin closed"))?;
            stdin.write_all(&message).await?;
            stdin.flush().await
        }
    }

    fn receive(&mut self) -> impl Future<Output = Option<RxJsonRpcMessage<RoleClient>>> + Send {
        self.receive_message()
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.close_input().await;
        let mut exit_observer = self.exit_observer.clone();
        match tokio::time::timeout(
            super::stdio_server_launcher::PROCESS_RETIREMENT_TIMEOUT,
            exit_observer.wait(),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                self.exit_observer.abort();
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "local MCP exit timed out",
                ))
            }
        }
    }
}

impl Transport<RoleClient> for LegacyLocalStdioTransport {
    type Error = io::Error;

    fn send(
        &mut self,
        item: TxJsonRpcMessage<RoleClient>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        self.transport.send(item)
    }

    fn receive(&mut self) -> impl Future<Output = Option<RxJsonRpcMessage<RoleClient>>> + Send {
        self.transport.receive()
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.transport.close().await
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn final_local_observer_drop_aborts_the_child_supervisor() {
        let mut command = Command::new("sleep");
        command.arg("30").kill_on_drop(true);
        let (transport, _) = LocalStdioTransport::spawn(command, "test-server".to_string())
            .expect("spawn test child");
        let observer = transport.exit_observer();
        let supervisor = observer.supervisor.abort_handle.clone();
        drop(transport);
        tokio::task::yield_now().await;
        assert!(
            !supervisor.is_finished(),
            "a remaining observer owns the child"
        );
        drop(observer);
        let stopped = tokio::time::timeout(Duration::from_secs(1), async {
            while !supervisor.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        // Also clean up the child if this negative control detects a regression.
        supervisor.abort();
        stopped.expect("the last observer must not detach its child supervisor");
    }

    #[tokio::test]
    async fn modern_local_exit_observer_waits_for_child_exit() {
        let mut command = Command::new("sh");
        command.args(["-c", "exit 0"]);
        let (transport, _) = LocalStdioTransport::spawn(command, "test-server".to_string())
            .expect("spawn test child");

        let mut observer = transport.exit_observer();
        tokio::time::timeout(Duration::from_secs(1), observer.wait())
            .await
            .expect("child should exit promptly")
            .expect("observer should report terminal exit");
    }

    #[tokio::test]
    async fn legacy_local_exit_observer_waits_for_child_exit() {
        let mut command = Command::new("sh");
        command.args(["-c", "exit 0"]);
        let (transport, _) = LegacyLocalStdioTransport::spawn(command).expect("spawn test child");

        let mut observer = transport.exit_observer();
        tokio::time::timeout(Duration::from_secs(1), observer.wait())
            .await
            .expect("child should exit promptly")
            .expect("observer should report terminal exit");
    }
}
