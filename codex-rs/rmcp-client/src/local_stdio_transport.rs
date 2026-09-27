//! Protocol framing and child lifetime for locally spawned MCP servers.
//!
//! Process creation is platform-specific; protocol selection and shutdown are shared.

use std::future::Future;
use std::io;
use std::sync::Arc;

use codex_utils_pty::Command;
use futures::FutureExt;
use rmcp::service::RoleClient;
use rmcp::service::RxJsonRpcMessage;
use rmcp::service::TxJsonRpcMessage;
use rmcp::transport::Transport;
use rmcp::transport::async_rw::AsyncRwTransport;
use tokio::process::ChildStderr;
use tokio::process::ChildStdin;
use tokio::process::ChildStdout;
use tokio::sync::watch;
use tokio::task::AbortHandle;

use crate::bounded_stdio_transport::BoundedStdioTransport;
use crate::protocol_mode::McpProtocolMode;

pub(super) struct LocalStdioTransport {
    process_id: Option<u32>,
    exit_observer: LocalProcessExitObserver,
    transport: StdioTransport,
}

enum StdioTransport {
    /// Preserve rmcp's existing framing for servers using the initialize handshake.
    Legacy(AsyncRwTransport<RoleClient, ChildStdout, ChildStdin>),
    /// Bound frames and skip messages unknown to the client during 2026-07-28 discovery.
    V20260728(BoundedStdioTransport),
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
        protocol_mode: McpProtocolMode,
    ) -> io::Result<(Self, Option<ChildStderr>)> {
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
        let transport = match protocol_mode {
            McpProtocolMode::Legacy => StdioTransport::Legacy(AsyncRwTransport::new(stdout, stdin)),
            McpProtocolMode::V20260728 => {
                StdioTransport::V20260728(BoundedStdioTransport::new(stdin, stdout, program_name))
            }
        };
        let (exit_tx, exit_rx) = watch::channel(LocalProcessExit::Pending);
        let supervisor = tokio::spawn(async move {
            let terminal = match child.wait().await {
                Ok(_) => LocalProcessExit::Exited,
                Err(error) => LocalProcessExit::Failed(Arc::from(error.to_string())),
            };
            let _ = exit_tx.send(terminal);
        });
        Ok((
            Self {
                process_id,
                exit_observer: LocalProcessExitObserver {
                    state: exit_rx,
                    supervisor: Arc::new(LocalProcessSupervisor {
                        abort_handle: supervisor.abort_handle(),
                    }),
                },
                transport,
            },
            stderr,
        ))
    }

    pub(super) fn id(&self) -> Option<u32> {
        self.process_id
    }

    pub(super) fn exit_observer(&self) -> LocalProcessExitObserver {
        self.exit_observer.clone()
    }

    /// The process owner has already proved exit under its absolute deadline.
    /// Close either framing mode and consume the retained terminal observation
    /// without introducing a second relative process timeout.
    pub(super) async fn close_after_terminal_observation(&mut self) -> io::Result<()> {
        match &mut self.transport {
            StdioTransport::Legacy(transport) => transport.close().await?,
            StdioTransport::V20260728(transport) => transport.close().await?,
        }
        self.exit_observer.clone().wait().await
    }
}

impl Transport<RoleClient> for LocalStdioTransport {
    type Error = io::Error;

    fn send(
        &mut self,
        item: TxJsonRpcMessage<RoleClient>,
    ) -> impl Future<Output = io::Result<()>> + Send + 'static {
        match &mut self.transport {
            StdioTransport::Legacy(transport) => transport.send(item).boxed(),
            StdioTransport::V20260728(transport) => transport.send(item).boxed(),
        }
    }

    fn receive(&mut self) -> impl Future<Output = Option<RxJsonRpcMessage<RoleClient>>> + Send {
        match &mut self.transport {
            StdioTransport::Legacy(transport) => transport.receive().boxed(),
            StdioTransport::V20260728(transport) => transport.receive().boxed(),
        }
    }

    async fn close(&mut self) -> io::Result<()> {
        match &mut self.transport {
            StdioTransport::Legacy(transport) => transport.close().await?,
            StdioTransport::V20260728(transport) => transport.close().await?,
        }
        let mut exit_observer = self.exit_observer.clone();
        match tokio::time::timeout(
            super::stdio_server_launcher::PROCESS_RETIREMENT_TIMEOUT,
            exit_observer.wait(),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "local MCP exit timed out",
            )),
        }
    }
}

#[cfg(all(test, unix))]
#[path = "local_stdio_transport_tests.rs"]
mod tests;
