//! Reserve and retain physical attempts without replacing upstream handshake policy.
use super::RawPendingTransport;
use super::RmcpClient;
use super::TransportRecipe;
use crate::elicitation_client_service::ElicitationClientService;
use crate::http_client_adapter::StreamableHttpClientAdapter;
use crate::oauth::OAuthRuntime;
use crate::retirement::AcknowledgedTransport;
use crate::retirement::ManagedRunningService;
use crate::retirement::PhysicalAttemptTicket;
use crate::retirement::RmcpClientRetirement;
use crate::retirement::TransportCloseReceipt;
use crate::stdio_server_launcher::StdioServerTransport;
use anyhow::Result;
use anyhow::anyhow;
use rmcp::service::ClientCacheConfig;
use rmcp::service::RoleClient;
use rmcp::service::RunningService;
use rmcp::service::RxJsonRpcMessage;
use rmcp::service::TxJsonRpcMessage;
use rmcp::transport::Transport;
use rmcp::transport::WorkerTransport;
use rmcp::transport::auth::AuthClient;
use rmcp::transport::streamable_http_client::StreamableHttpClient;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::transport::streamable_http_client::StreamableHttpClientWorker;
use rmcp::transport::worker::CloseDeadline;
use rmcp::transport::worker::Worker;
use std::sync::Arc;
use std::sync::Mutex;
use tokio_util::sync::CancellationToken;

type InProcessTransport = rmcp::transport::async_rw::AsyncRwTransport<
    RoleClient,
    tokio::io::ReadHalf<tokio::io::DuplexStream>,
    tokio::io::WriteHalf<tokio::io::DuplexStream>,
>;

pub(super) enum PendingTransport {
    InProcess {
        transport: AcknowledgedTransport<InProcessTransport>,
    },
    Stdio {
        transport: Box<AcknowledgedTransport<StdioServerTransport>>,
    },
    StreamableHttp {
        transport: AcknowledgedTransport<DeferredHttpTransport<StreamableHttpClientAdapter>>,
    },
    StreamableHttpWithOAuth {
        transport:
            AcknowledgedTransport<DeferredHttpTransport<AuthClient<StreamableHttpClientAdapter>>>,
        oauth_runtime: OAuthRuntime,
    },
    StreamableHttpWithAccessTokenOnly {
        transport:
            AcknowledgedTransport<DeferredHttpTransport<AuthClient<StreamableHttpClientAdapter>>>,
    },
}

pub(super) struct RegisteredTransport {
    pub(super) transport: PendingTransport,
    pub(super) ticket: PhysicalAttemptTicket,
    pub(super) receipt: TransportCloseReceipt,
}

impl RegisteredTransport {
    pub(super) fn bind(raw: RawPendingTransport, ticket: PhysicalAttemptTicket) -> Self {
        let process = match &raw {
            RawPendingTransport::Stdio { transport } => Some(transport.process_ref()),
            _ => None,
        };
        let (transport, receipt) = match raw {
            RawPendingTransport::InProcess { transport } => {
                let (reader, writer) = tokio::io::split(transport);
                let (transport, receipt) = AcknowledgedTransport::new(
                    rmcp::transport::async_rw::AsyncRwTransport::new_client(reader, writer),
                );
                (PendingTransport::InProcess { transport }, receipt)
            }
            RawPendingTransport::Stdio { transport } => {
                let (transport, receipt) = AcknowledgedTransport::new(*transport);
                (
                    PendingTransport::Stdio {
                        transport: Box::new(
                            transport.with_on_return(StdioServerTransport::finish_discovery),
                        ),
                    },
                    receipt,
                )
            }
            RawPendingTransport::StreamableHttp { transport } => {
                let (transport, receipt) = AcknowledgedTransport::new(
                    transport.with_close_deadline(ticket.close_deadline()),
                );
                (PendingTransport::StreamableHttp { transport }, receipt)
            }
            RawPendingTransport::StreamableHttpWithOAuth {
                transport,
                oauth_runtime,
            } => {
                let (transport, receipt) = AcknowledgedTransport::new(
                    transport.with_close_deadline(ticket.close_deadline()),
                );
                (
                    PendingTransport::StreamableHttpWithOAuth {
                        transport,
                        oauth_runtime,
                    },
                    receipt,
                )
            }
            RawPendingTransport::StreamableHttpWithAccessTokenOnly { transport } => {
                let (transport, receipt) = AcknowledgedTransport::new(
                    transport.with_close_deadline(ticket.close_deadline()),
                );
                (
                    PendingTransport::StreamableHttpWithAccessTokenOnly { transport },
                    receipt,
                )
            }
        };
        ticket.attach_transport(receipt.clone(), process);
        Self {
            transport,
            ticket,
            receipt,
        }
    }
}

/// Stored factories hold only weak tickets, never their registry or owning client.
/// The result cell is taken exactly once; abandoned outputs return transports
/// to their retained close receipt when the drained phase releases its output.
pub(super) async fn retain_phase<T: Send + 'static, F, Fut>(
    ticket: PhysicalAttemptTicket,
    factory: F,
) -> Result<T>
where
    F: FnOnce(PhysicalAttemptTicket) -> Fut + Send + 'static,
    Fut: Future<Output = Result<T>> + Send + 'static,
{
    let phase = ticket.start_phase(move |ticket| async move {
        Arc::new(Mutex::new(Some(factory(ticket).await)))
    })?;
    let result = phase
        .await
        .ok_or_else(|| anyhow!("physical connection registration is closed"))?;
    result
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
        .ok_or_else(|| anyhow!("physical phase result was already consumed"))?
}

pub(super) fn create_registered_transport(
    recipe: TransportRecipe,
    retirement: &RmcpClientRetirement,
) -> impl Future<Output = Result<RegisteredTransport>> + Send + use<> {
    let ticket = retirement.reserve_attempt();
    async move {
        retain_phase(ticket?, move |ticket| async move {
            let raw = RmcpClient::create_pending_transport(&recipe).await?;
            Ok(RegisteredTransport::bind(raw, ticket))
        })
        .await
    }
}

pub(super) async fn bind_service<F>(
    future: F,
    ticket: PhysicalAttemptTicket,
    receipt: TransportCloseReceipt,
) -> Result<Arc<ManagedRunningService<ElicitationClientService>>>
where
    F: Future<
            Output = Result<
                RunningService<RoleClient, ElicitationClientService>,
                rmcp::service::ClientInitializeError,
            >,
        > + Send,
{
    let service = match future.await {
        Ok(service) => service,
        Err(source) => {
            // Preserve EOF-before-restart. A failed initialization returns its
            // exact transport to the receipt; close failure stays recorded there
            // and never changes the upstream handshake classification.
            let _ = receipt
                .close_returned_until(
                    tokio::time::Instant::now() + crate::retirement::DEFAULT_RETIREMENT_TIMEOUT,
                )
                .await;
            return Err(super::streamable_http_retry::HandshakeError { source }.into());
        }
    };
    service
        .peer()
        .set_response_cache_config(ClientCacheConfig::disabled())
        .await;
    let service = Arc::new(ManagedRunningService::new(service, receipt));
    ticket.attach_service(Arc::clone(&service));
    Ok(service)
}

/// Streamable HTTP transport whose rmcp worker task is spawned on first use.
pub(super) type DeferredHttpTransport<C> = DeferredWorkerTransport<StreamableHttpClientWorker<C>>;

pub(super) fn deferred_http_transport<C: StreamableHttpClient>(
    client: C,
    config: StreamableHttpClientTransportConfig,
) -> DeferredHttpTransport<C> {
    DeferredWorkerTransport::new(StreamableHttpClientWorker::new(client, config))
}

/// Owns an rmcp worker that is spawned only when the handshake first uses it.
///
/// `WorkerTransport::close` cancels its token and then joins the worker task.
/// The vendored rmcp worker observes that token at every startup await and
/// deletes any session it created before it exits, so a started transport
/// closes within rmcp's session cleanup budget and reports unproven cleanup
/// as a close error. `with_close_deadline` shares the retirement registry's
/// deadline with that worker, so its cleanup also ends by that deadline.
///
/// Deferring the spawn makes a transport whose handshake never ran own no
/// task, no request and no connection, so its close is an immediate, truthful
/// terminal receipt. Once started, the transport is the rmcp worker itself.
/// Production handshakes send before they receive, so the first `send`
/// spawns the worker and enqueues its request in the same poll; the test
/// `production_handshake_sends_before_it_receives` pins that order.
pub(super) struct DeferredWorkerTransport<W: Worker> {
    state: DeferredWorker<W>,
    close_deadline: CloseDeadline,
}

enum DeferredWorker<W: Worker> {
    Unstarted(W),
    Started(WorkerTransport<W>),
    Closed,
}

impl<W: Worker> DeferredWorkerTransport<W> {
    pub(super) fn new(worker: W) -> Self {
        Self {
            state: DeferredWorker::Unstarted(worker),
            close_deadline: CloseDeadline::default(),
        }
    }

    /// Share the retirement deadline the worker's exit cleanup must end by.
    pub(super) fn with_close_deadline(mut self, close_deadline: CloseDeadline) -> Self {
        self.close_deadline = close_deadline;
        self
    }

    #[cfg(test)]
    pub(super) fn is_started(&self) -> bool {
        matches!(self.state, DeferredWorker::Started(_))
    }

    /// Spawns on first use. A closed transport never starts a worker.
    fn started(&mut self) -> Option<&mut WorkerTransport<W>> {
        self.state = match std::mem::replace(&mut self.state, DeferredWorker::Closed) {
            DeferredWorker::Unstarted(worker) => {
                DeferredWorker::Started(WorkerTransport::spawn_with_close_deadline(
                    worker,
                    CancellationToken::new(),
                    self.close_deadline.clone(),
                ))
            }
            state => state,
        };
        match &mut self.state {
            DeferredWorker::Started(transport) => Some(transport),
            DeferredWorker::Unstarted(_) | DeferredWorker::Closed => None,
        }
    }
}

impl<W: Worker> Transport<W::Role> for DeferredWorkerTransport<W> {
    type Error = W::Error;

    fn send(
        &mut self,
        item: TxJsonRpcMessage<W::Role>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        let send = self.started().map(|transport| transport.send(item));
        async move {
            match send {
                Some(send) => send.await,
                None => Err(W::err_closed()),
            }
        }
    }

    async fn receive(&mut self) -> Option<RxJsonRpcMessage<W::Role>> {
        match self.started() {
            Some(transport) => transport.receive().await,
            None => None,
        }
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        if let DeferredWorker::Started(transport) = &mut self.state {
            return transport.close().await;
        }
        // No task was spawned and no request was issued: dropping the
        // unstarted worker releases only its HTTP client handle.
        self.state = DeferredWorker::Closed;
        Ok(())
    }
}

#[cfg(test)]
#[path = "registered_transport_tests.rs"]
mod tests;
