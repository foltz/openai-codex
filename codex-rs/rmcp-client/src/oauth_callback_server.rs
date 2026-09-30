//! Owned HTTP/1 callback transport for the browser OAuth flow.
//!
//! The acceptor and every admitted connection retain their original join
//! receipts outside the accept loop.  A callback response is acknowledged
//! only after the connection future has returned; the caller separately joins
//! the wrapper task.  This keeps a response send from being mistaken for
//! connection retirement.

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

pub(crate) type Response = http::Response<Full<Bytes>>;

pub(crate) struct Request {
    pub(crate) url: String,
    reply: oneshot::Sender<Response>,
    delivered: oneshot::Receiver<WorkerOutcome>,
}

impl Request {
    pub(crate) async fn respond(self, response: Response) -> io::Result<()> {
        self.reply
            .send(response)
            .map_err(|_| io::Error::other("OAuth callback connection closed"))?;
        match self.delivered.await {
            Ok(WorkerOutcome::Joined) => Ok(()),
            Ok(
                WorkerOutcome::Interrupted
                | WorkerOutcome::Failed
                | WorkerOutcome::Cancelled
                | WorkerOutcome::Panicked,
            )
            | Err(_) => Err(io::Error::other("OAuth callback response was not written")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkerOutcome {
    Joined,
    Interrupted,
    Failed,
    Cancelled,
    Panicked,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ConnectionOutcomes {
    pub(crate) joined: u64,
    pub(crate) interrupted: u64,
    pub(crate) failed: u64,
    pub(crate) cancelled: u64,
    pub(crate) panicked: u64,
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
pub(crate) struct Report {
    pub(crate) acceptor: WorkerOutcome,
    pub(crate) connections: ConnectionOutcomes,
    pub(crate) unavailable: bool,
}

struct JoinOwner {
    receipt: Shared<BoxFuture<'static, WorkerOutcome>>,
    abort: AbortHandle,
}

impl JoinOwner {
    fn spawn(future: impl Future<Output = WorkerOutcome> + Send + 'static) -> Self {
        let handle = tokio::spawn(future);
        let abort = handle.abort_handle();
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
            abort,
        }
    }
}

impl Drop for JoinOwner {
    fn drop(&mut self) {
        // A cancelled OAuth flow may drop its observer before it can await the
        // server report.  Abort is only a last-resort signal here; a normal
        // flow always awaits the same receipts before returning.  In
        // particular, dropping a receipt observer must not abort a task that
        // another owner is still observing.
        if self.receipt.peek().is_none() {
            self.abort.abort();
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
            if owner.abort.is_finished()
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

pub(crate) struct Server {
    state: Arc<Mutex<State>>,
    shutdown: CancellationToken,
}

impl Server {
    pub(crate) fn start(listener: TcpListener) -> io::Result<(Self, mpsc::Receiver<Request>)> {
        let state = Arc::new(Mutex::new(State::default()));
        let shutdown = CancellationToken::new();
        let (requests, receiver) = mpsc::channel(16);
        {
            let mut state_guard = state
                .lock()
                .map_err(|_| io::Error::other("OAuth callback custody unavailable"))?;
            state_guard.acceptor = Some(JoinOwner::spawn(accept(
                listener,
                requests,
                Arc::downgrade(&state),
                shutdown.clone(),
            )));
        }
        Ok((Self { state, shutdown }, receiver))
    }

    pub(crate) fn close(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.closed = true;
        state.unavailable |= self.state.is_poisoned();
        drop(state);
        self.shutdown.cancel();
    }

    pub(crate) async fn wait(&self) -> Report {
        let (acceptor, connections, was_closed) = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (
                state.acceptor.as_ref().map(|owner| owner.receipt.clone()),
                state
                    .connections
                    .iter()
                    .map(|owner| owner.receipt.clone())
                    .collect::<Vec<_>>(),
                state.closed,
            )
        };
        let (acceptor, _) = tokio::join!(
            async {
                match acceptor {
                    Some(receipt) => receipt.await,
                    None => WorkerOutcome::Failed,
                }
            },
            futures::future::join_all(connections),
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
                let (reply, response, delivered) = channels
                    .ok_or_else(|| io::Error::other("OAuth callback connection already used"))?;
                requests
                    .send(Request {
                        url: request.uri().to_string(),
                        reply,
                        delivered,
                    })
                    .await
                    .map_err(|_| io::Error::other("OAuth callback dispatcher closed"))?;
                let mut response: Response = response
                    .await
                    .map_err(|_| io::Error::other("OAuth callback response unavailable"))?;
                response.headers_mut().insert(
                    http::header::CONNECTION,
                    http::HeaderValue::from_static("close"),
                );
                Ok::<_, io::Error>(response)
            }
        });
    let outcome = {
        let connection = hyper::server::conn::http1::Builder::new()
            .keep_alive(false)
            .serve_connection(TokioIo::new(stream), service);
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => WorkerOutcome::Interrupted,
            result = connection => if result.is_ok() { WorkerOutcome::Joined } else { WorkerOutcome::Failed },
        }
    };
    let _ = delivered.send(outcome);
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::Full;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn response_ack_and_server_report_observe_real_connection_retirement() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind callback listener");
        let address = listener.local_addr().expect("callback listener address");
        let (server, mut requests) = Server::start(listener).expect("start callback server");

        let client = tokio::spawn(async move {
            let mut stream = TcpStream::connect(address)
                .await
                .expect("connect callback listener");
            stream
                .write_all(
                    b"GET /callback?id=1 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                )
                .await
                .expect("write callback request");
            let mut response = Vec::new();
            stream
                .read_to_end(&mut response)
                .await
                .expect("read callback response");
            response
        });

        let request = requests.recv().await.expect("receive callback request");
        assert_eq!(request.url, "/callback?id=1");
        request
            .respond(http::Response::new(Full::new(Bytes::from_static(b"ok"))))
            .await
            .expect("response should be written before acknowledgement");
        let response = client.await.expect("client task should join");
        assert!(response.starts_with(b"HTTP/1.1 200"));
        assert!(response.ends_with(b"ok"));

        server.close();
        let report = server.wait().await;
        assert_eq!(report.acceptor, WorkerOutcome::Joined);
        assert_eq!(report.connections.joined, 1);
        assert_eq!(report.connections.failed, 0);
        assert_eq!(report.connections.interrupted, 0);
        assert!(!report.unavailable);
    }

    #[tokio::test]
    async fn waiting_does_not_abort_a_connection_still_being_observed() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind callback listener");
        let address = listener.local_addr().expect("callback listener address");
        let (server, mut requests) = Server::start(listener).expect("start callback server");

        let client = tokio::spawn(async move {
            let mut stream = TcpStream::connect(address)
                .await
                .expect("connect callback listener");
            stream
                .write_all(
                    b"GET /callback?id=2 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                )
                .await
                .expect("write callback request");
            let mut response = Vec::new();
            stream
                .read_to_end(&mut response)
                .await
                .expect("read callback response");
            response
        });

        let request = requests.recv().await.expect("receive callback request");
        assert_eq!(request.url, "/callback?id=2");

        // Mark the acceptor closed without cancelling admitted connections.
        // The observer must keep the connection owner alive while it waits;
        // otherwise dropping a temporary owner aborts the very task whose
        // receipt it is meant to observe.
        server
            .state
            .lock()
            .expect("callback state should not be poisoned")
            .closed = true;
        let mut report = Box::pin(server.wait());
        assert!(futures::poll!(report.as_mut()).is_pending());

        request
            .respond(http::Response::new(Full::new(Bytes::from_static(b"ok"))))
            .await
            .expect("response should be written before acknowledgement");
        let response = client.await.expect("client task should join");
        assert!(response.starts_with(b"HTTP/1.1 200"));
        assert!(response.ends_with(b"ok"));

        server.shutdown.cancel();
        let report = report.await;
        assert_eq!(report.acceptor, WorkerOutcome::Joined);
        assert_eq!(report.connections.joined, 1);
        assert_eq!(report.connections.cancelled, 0);
        assert!(!report.unavailable);
    }
}
