//! Bind retirement ownership before a newly created transport becomes usable.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Result;
use anyhow::anyhow;
use futures::FutureExt;
use futures::future::BoxFuture;
use rmcp::service::ClientCacheConfig;
use rmcp::service::ClientInitializeError;
use rmcp::service::ClientLifecycleMode;
use rmcp::service::ClientServiceExt;
use rmcp::service::RoleClient;
use rmcp::service::RunningService;
use rmcp::transport::Transport;
use tracing::warn;

use crate::elicitation_client_service::ElicitationClientService;
use crate::event_notification_transport::capture_event_notifications;
use crate::oauth::OAuthRuntime;
use crate::retirement::AcknowledgedTransport;
use crate::retirement::ManagedRunningService;
use crate::retirement::PhysicalAttemptTicket;
use crate::retirement::TransportCloseReceipt;
use crate::stdio_server_launcher::StdioServerProcessHandle;

use super::InitializeDeadlineGuard;
use super::PendingTransport;
use super::streamable_http_retry::HandshakeError;

type Handshake = Box<
    dyn FnOnce(
            ElicitationClientService,
            ClientLifecycleMode,
        ) -> BoxFuture<
            'static,
            Result<RunningService<RoleClient, ElicitationClientService>, ClientInitializeError>,
        > + Send,
>;

/// The closure owns the exact wrapped transport until handshake or drop.
/// Dropping it returns that transport to its retained receipt, including when
/// initialization is never called or registration closes before initialization.
pub(super) struct PendingConnection {
    handshake: Handshake,
    receipt: TransportCloseReceipt,
    ticket: PhysicalAttemptTicket,
    pub(super) oauth: Option<OAuthRuntime>,
    pub(super) retryable: bool,
}

#[cfg(test)]
#[path = "pending_transport_tests.rs"]
mod tests;

impl PendingConnection {
    pub(super) fn new(transport: PendingTransport, ticket: PhysicalAttemptTicket) -> Self {
        match transport {
            PendingTransport::InProcess { transport } => {
                let (reader, writer) = tokio::io::split(transport);
                Self::bind(
                    rmcp::transport::async_rw::AsyncRwTransport::new_client(reader, writer),
                    ticket,
                    /*process*/ None,
                    /*oauth*/ None,
                    /*retryable*/ false,
                )
            }
            PendingTransport::Stdio { transport } => {
                let process = transport.process_handle();
                Self::bind(
                    *transport,
                    ticket,
                    Some(process),
                    /*oauth*/ None,
                    /*retryable*/ false,
                )
            }
            PendingTransport::StreamableHttp { transport } => Self::bind(
                capture_event_notifications(transport),
                ticket,
                /*process*/ None,
                /*oauth*/ None,
                /*retryable*/ true,
            ),
            PendingTransport::StreamableHttpWithOAuth {
                transport,
                oauth_runtime,
            } => Self::bind(
                transport,
                ticket,
                /*process*/ None,
                Some(oauth_runtime),
                /*retryable*/ true,
            ),
            PendingTransport::StreamableHttpWithAccessTokenOnly { transport } => Self::bind(
                transport,
                ticket,
                /*process*/ None,
                /*oauth*/ None,
                /*retryable*/ true,
            ),
        }
    }

    fn bind<T: Transport<RoleClient> + 'static>(
        transport: T,
        ticket: PhysicalAttemptTicket,
        process: Option<StdioServerProcessHandle>,
        oauth: Option<OAuthRuntime>,
        retryable: bool,
    ) -> Self {
        let (transport, receipt) = AcknowledgedTransport::new(transport);
        ticket.attach_transport(receipt.clone(), process);
        Self {
            handshake: Box::new(move |service, lifecycle| {
                service.serve_with_lifecycle(transport, lifecycle).boxed()
            }),
            receipt,
            ticket,
            oauth,
            retryable,
        }
    }

    pub(super) async fn connect(
        self,
        service: ElicitationClientService,
        lifecycle: ClientLifecycleMode,
        timeout: Option<Duration>,
        initialize_deadline: Option<InitializeDeadlineGuard>,
    ) -> Result<(Arc<ManagedRunningService>, Option<OAuthRuntime>)> {
        let ticket = self.ticket.clone();
        let phase = ticket.start_phase(move |ticket| async move {
            // This guard belongs to the retained phase, not its cancellable
            // observer. HTTP requests keep the original handshake budget.
            let _initialize_deadline = initialize_deadline;
            let handshake = (self.handshake)(service, lifecycle);
            let handshake_with_timeout = async move {
                match timeout {
                    Some(duration) => match tokio::time::timeout(duration, handshake).await {
                        Ok(result) => result.map_err(|source| anyhow::Error::from(HandshakeError { source })),
                        Err(_) => Err(anyhow!("timed out handshaking with MCP server after {duration:?}")),
                    },
                    None => handshake.await.map_err(|source| anyhow::Error::from(HandshakeError { source })),
                }
            };
            // Cancel inside the retained phase: dropping the handshake returns
            // its acknowledged transport before the phase is marked terminal.
            // Transport creation remains separately retained until acquisition
            // finishes; arbitrary factories are not assumed cancellation-safe.
            let result = tokio::select! {
                biased;
                _ = ticket.shutdown.cancelled() => Err(anyhow!("MCP handshake cancelled by retirement")),
                result = handshake_with_timeout => result,
            };
            let result = match result {
                Ok(service) => {
                    service.peer().set_response_cache_config(ClientCacheConfig::disabled()).await;
                    let service = Arc::new(ManagedRunningService::new(service, self.receipt));
                    ticket.attach_service(Arc::clone(&service));
                    Ok((service, self.oauth))
                }
                Err(error) => {
                    if let Some(OAuthRuntime::Legacy(runtime)) = self.oauth.as_ref()
                        && let Err(persist_error) = runtime.persist_if_needed().await
                    {
                        warn!("failed to persist OAuth tokens after failed initialize: {persist_error}");
                    }
                    Err(error)
                }
            };
            Arc::new(Mutex::new(Some(result)))
        })?;
        let result = phase
            .await
            .ok_or_else(|| anyhow!("MCP client is shut down"))?;
        result
            .lock()
            .map_err(|_| anyhow!("MCP handshake result lock poisoned"))?
            .take()
            .ok_or_else(|| anyhow!("MCP handshake result already consumed"))?
    }
}
