//! Owned callback HTTP/1 transport. Reply delivery and task joins are distinct.

use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use http_body_util::Full;
use hyper_util::rt::TokioIo;
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::AbortHandle;
use tokio_util::sync::CancellationToken;

pub(super) type Response = http::Response<Full<Bytes>>;

pub(super) struct Request {
    pub(super) url: String,
    reply: oneshot::Sender<Response>,
    delivered: oneshot::Receiver<WorkerOutcome>,
}

impl Request {
    /// Wait for the connection future to write/close, not merely enqueue data.
    /// The server owner separately observes the task's actual join.
    pub(super) async fn respond(self, response: Response) -> io::Result<()> {
        self.reply
            .send(response)
            .map_err(|_| io::Error::other("login response connection closed"))?;
        match self.delivered.await {
            Ok(WorkerOutcome::Joined) => Ok(()),
            Ok(
                WorkerOutcome::Interrupted
                | WorkerOutcome::Failed
                | WorkerOutcome::Cancelled
                | WorkerOutcome::Panicked,
            )
            | Err(_) => Err(io::Error::other("login response was not written")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WorkerOutcome {
    Joined,
    Interrupted,
    Failed,
    Cancelled,
    Panicked,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct ConnectionOutcomes {
    pub(super) joined: u64,
    pub(super) interrupted: u64,
    pub(super) failed: u64,
    pub(super) cancelled: u64,
    pub(super) panicked: u64,
}

impl ConnectionOutcomes {
    fn record(&mut self, outcome: WorkerOutcome) -> Option<()> {
        let count = match outcome {
            WorkerOutcome::Joined => &mut self.joined,
            WorkerOutcome::Interrupted => &mut self.interrupted,
            WorkerOutcome::Failed => &mut self.failed,
            WorkerOutcome::Cancelled => &mut self.cancelled,
            WorkerOutcome::Panicked => &mut self.panicked,
        };
        *count = count.checked_add(1)?;
        Some(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Report {
    pub(super) acceptor: WorkerOutcome,
    pub(super) connections: ConnectionOutcomes,
    pub(super) unavailable: bool,
}

#[derive(Clone)]
struct JoinOwner {
    receipt: Shared<BoxFuture<'static, WorkerOutcome>>,
    ready: AbortHandle,
}

impl JoinOwner {
    fn spawn(future: impl Future<Output = WorkerOutcome> + Send + 'static) -> Self {
        let handle = tokio::spawn(future);
        let ready = handle.abort_handle();
        Self {
            receipt: async move {
                match handle.await {
                    Ok(outcome) => outcome,
                    Err(error) if error.is_cancelled() => WorkerOutcome::Cancelled,
                    Err(_) => WorkerOutcome::Panicked,
                }
            }
            .boxed()
            .shared(),
            ready,
        }
    }
}

#[derive(Default)]
struct State {
    closed: bool,
    unavailable: bool,
    acceptor: Option<JoinOwner>,
    connections: Vec<JoinOwner>,
    outcomes: ConnectionOutcomes,
}

impl State {
    fn compact(&mut self) {
        self.connections.retain(|owner| {
            // Readiness is only a hint: consume the normalized actual join.
            if owner.ready.is_finished()
                && let Some(outcome) = owner.receipt.clone().now_or_never()
            {
                self.unavailable |= self.outcomes.record(outcome).is_none();
                false
            } else {
                true
            }
        });
    }
}

/// The original joins live outside the acceptor and survive its cancellation
/// or panic. Worker futures capture no strong reference back to this owner.
pub(super) struct Server {
    state: Arc<Mutex<State>>,
    shutdown: CancellationToken,
}

impl Server {
    pub(super) fn start(listener: TcpListener) -> io::Result<(Self, mpsc::Receiver<Request>)> {
        let state = Arc::new(Mutex::new(State::default()));
        let shutdown = CancellationToken::new();
        let (requests, receiver) = mpsc::channel(16);
        {
            let mut owned = state
                .lock()
                .map_err(|_| io::Error::other("login HTTP custody unavailable"))?;
            owned.acceptor = Some(JoinOwner::spawn(accept(
                listener,
                requests,
                Arc::downgrade(&state),
                shutdown.clone(),
            )));
        }
        Ok((Self { state, shutdown }, receiver))
    }

    pub(super) fn close(&self) {
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.closed = true;
            state.unavailable |= self.state.is_poisoned();
        }
        self.shutdown.cancel();
    }

    /// The caller bounds this observation with its original login deadline.
    /// No handle is taken out of custody while an observer is cancellable.
    pub(super) async fn wait(&self) -> Report {
        let (acceptor, connections, was_closed) = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (
                state.acceptor.clone(),
                state.connections.clone(),
                state.closed,
            )
        };
        // Admission must be closed before the population snapshot. A caller
        // mistake cannot turn a partial snapshot into a successful report.
        let (acceptor, _) = tokio::join!(
            async {
                match acceptor {
                    Some(owner) => owner.receipt.await,
                    None => WorkerOutcome::Failed,
                }
            },
            futures::future::join_all(connections.into_iter().map(|owner| owner.receipt)),
        );
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.compact();
        Report {
            acceptor,
            connections: state.outcomes,
            unavailable: !was_closed
                || state.unavailable
                || self.state.is_poisoned()
                || !state.connections.is_empty(),
        }
    }
}

#[derive(Debug)]
enum AdmissionError {
    Closed,
    Unavailable,
}

fn register(
    state: &Weak<Mutex<State>>,
    future: impl Future<Output = WorkerOutcome> + Send + 'static,
) -> Result<(), AdmissionError> {
    let state = state.upgrade().ok_or(AdmissionError::Unavailable)?;
    let mut state = state.lock().map_err(|_| AdmissionError::Unavailable)?;
    if state.closed {
        return Err(AdmissionError::Closed);
    }
    state.compact();
    if state.unavailable {
        return Err(AdmissionError::Unavailable);
    }
    state.connections.push(JoinOwner::spawn(future));
    Ok(())
}

async fn accept(
    listener: TcpListener,
    requests: mpsc::Sender<Request>,
    state: Weak<Mutex<State>>,
    shutdown: CancellationToken,
) -> WorkerOutcome {
    // Returning or panicking in the acceptor cannot leave its admitted
    // connections waiting forever. Their original joins remain outside it.
    let _shutdown_on_exit = shutdown.clone().drop_guard();
    loop {
        let stream = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return WorkerOutcome::Joined,
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => stream,
                Err(_) => return WorkerOutcome::Failed,
            },
        };
        if let Err(error) = register(
            &state,
            connection(stream, requests.clone(), shutdown.clone()),
        ) {
            return match error {
                AdmissionError::Closed => WorkerOutcome::Joined,
                AdmissionError::Unavailable => WorkerOutcome::Failed,
            };
        }
    }
}

async fn connection(
    stream: TcpStream,
    requests: mpsc::Sender<Request>,
    shutdown: CancellationToken,
) -> WorkerOutcome {
    let (reply, response) = oneshot::channel();
    let (delivered, delivery) = oneshot::channel();
    let channels = Mutex::new(Some((reply, response, delivery)));
    let service =
        hyper::service::service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
            let channels = channels
                .lock()
                .ok()
                .and_then(|mut channels| channels.take());
            let requests = requests.clone();
            async move {
                let (reply, response, delivered) =
                    channels.ok_or_else(|| io::Error::other("login connection already used"))?;
                requests
                    .send(Request {
                        url: request.uri().to_string(),
                        reply,
                        delivered,
                    })
                    .await
                    .map_err(|_| io::Error::other("login callback closed"))?;
                let mut response: Response = response
                    .await
                    .map_err(|_| io::Error::other("login response unavailable"))?;
                response.headers_mut().insert(
                    http::header::CONNECTION,
                    http::HeaderValue::from_static("close"),
                );
                Ok::<_, io::Error>(response)
            }
        });
    let outcome = {
        let connection = hyper::server::conn::http1::Builder::new()
            .keep_alive(/*val*/ false)
            .serve_connection(TokioIo::new(stream), service);
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => WorkerOutcome::Interrupted,
            result = connection => if result.is_ok() { WorkerOutcome::Joined } else { WorkerOutcome::Failed },
        }
    }; // The connection and its I/O are dropped before acknowledging delivery.
    let _ = delivered.send(outcome);
    outcome
}

#[cfg(test)]
#[path = "http_server_tests.rs"]
mod tests;
