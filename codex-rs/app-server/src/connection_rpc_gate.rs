//! Controls RPC admission and draining for each connection.
//! Auth ownership changes reject queued work while started handlers finish cleanup.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use codex_app_server_transport::ConnectionAuth;

use futures::FutureExt;
use futures::future::BoxFuture;
use std::panic::AssertUnwindSafe;
use tokio::sync::Mutex;
use tokio_util::task::TaskTracker;

/// Per-connection gate for incoming messages and initialized RPC handler execution.
///
/// Closing the gate prevents queued handlers from starting while allowing
/// handlers that already acquired a token to finish.
#[derive(Debug)]
pub(crate) struct ConnectionRpcGate {
    pub(crate) auth: Option<ConnectionAuth>,
    accepting: Mutex<bool>,
    tasks: TaskTracker,
    panicked: AtomicBool,
    // Every request read and queued for this connection holds one ticket
    // until it is settled: it started (admitted by `run_admitted`), or it was
    // refused with an explicit error reply.
    queued: TaskTracker,
    // Queued requests released without starting and without a refusal reply.
    unanswered: AtomicU64,
}

/// A request read and queued on this connection that is still owed an
/// answer. It is admitted only when the open gate lets it start; once the
/// gate closed it can only be refused (its explicit error reply). Dropped
/// unsettled, it is counted as unanswered, never silently forgotten.
pub(crate) struct RpcTicket {
    gate: Arc<ConnectionRpcGate>,
    settled: bool,
    // Resolves `true` once the refusal reply was handed to the outbound path.
    refusal: Option<BoxFuture<'static, bool>>,
    // Held while queued; released once admitted or refused.
    queued: Option<tokio_util::task::task_tracker::TaskTrackerToken>,
}

impl RpcTicket {
    /// Sends the explicit refusal reply for a request that will never start.
    pub(crate) fn with_refusal(&mut self, refusal: impl Future<Output = bool> + Send + 'static) {
        self.refusal = Some(Box::pin(refusal));
    }

    /// Settle a request that will never start by refusing it explicitly.
    /// Without a refusal reply, or when the reply could not be handed to the
    /// outbound path, it stays unanswered (counted on drop).
    pub(crate) async fn refuse(mut self) {
        if let Some(refusal) = self.refusal.take()
            && refusal.await
        {
            self.settled = true;
            self.queued = None;
        }
    }
}

impl std::fmt::Debug for RpcTicket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcTicket")
            .field("settled", &self.settled)
            .field("has_refusal", &self.refusal.is_some())
            .finish_non_exhaustive()
    }
}

impl Drop for RpcTicket {
    fn drop(&mut self) {
        if !self.settled {
            // Exhaustion stays visibly nonzero instead of wrapping to a false
            // clean/unanswered-zero shutdown observation.
            let _ =
                self.gate
                    .unanswered
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                        count.checked_add(1)
                    });
        }
    }
}

/// Internal child failure; its already-admitted parent owns the RPC reply.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConnectionRpcChildError {
    Closed,
    Panicked,
}

impl ConnectionRpcGate {
    pub(crate) fn new() -> Self {
        let accepting = true;
        Self {
            auth: None,
            accepting: Mutex::new(accepting),
            tasks: TaskTracker::new(),
            panicked: AtomicBool::new(false),
            queued: TaskTracker::new(),
            unanswered: AtomicU64::new(0),
        }
    }

    /// Record a request read and queued for later execution.
    pub(crate) fn ticket(gate: &Arc<Self>) -> RpcTicket {
        RpcTicket {
            gate: Arc::clone(gate),
            settled: false,
            refusal: None,
            queued: Some(gate.queued.token()),
        }
    }

    /// As `run`, settling `ticket`: the request is admitted only if the gate
    /// is still open; otherwise it is refused explicitly and never started.
    pub(crate) async fn run_admitted<F>(&self, mut ticket: RpcTicket, future: F)
    where
        F: Future<Output = ()>,
    {
        let token = {
            let accepting = self.accepting.lock().await;
            if *accepting && !self.is_closed() {
                ticket.settled = true;
                ticket.queued = None;
                Some(self.tasks.token())
            } else {
                None
            }
        };
        let Some(token) = token else {
            ticket.refuse().await;
            return;
        };

        if AssertUnwindSafe(future).catch_unwind().await.is_err() {
            self.panicked.store(true, Ordering::Release);
        }
        drop(token);
    }

    /// Resolves once every queued request was admitted or released.
    pub(crate) async fn queued_settled(&self) {
        self.queued.close();
        self.queued.wait().await;
    }

    /// Queued requests neither admitted nor refused: still pending, or
    /// released without a reply.
    pub(crate) fn unanswered(&self) -> u64 {
        let pending = u64::try_from(self.queued.len()).unwrap_or(u64::MAX);
        self.unanswered
            .load(Ordering::Acquire)
            .saturating_add(pending)
    }

    /// Run an internal child of an already-admitted RPC. The caller retains
    /// its original in the processor TaskTracker and owns the parent reply.
    /// This is not incoming-request admission: use run_admitted with an actual
    /// RpcTicket for that. Closing still refuses before polling the child,
    /// admitted children join the same drain, and panic remains sticky.
    pub(crate) async fn run_child<F, T>(&self, future: F) -> Result<T, ConnectionRpcChildError>
    where
        F: Future<Output = T>,
    {
        let token = {
            let accepting = self.accepting.lock().await;
            if !*accepting || self.is_closed() {
                return Err(ConnectionRpcChildError::Closed);
            }
            self.tasks.token()
        };
        let result = AssertUnwindSafe(future).catch_unwind().await.map_err(|_| {
            self.panicked.store(true, Ordering::Release);
            ConnectionRpcChildError::Panicked
        });
        drop(token);
        result
    }

    #[cfg(test)]
    pub(crate) async fn run<F>(&self, future: F)
    where
        F: Future<Output = ()>,
    {
        let _ = self.run_child(future).await;
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.tasks.is_closed() || self.auth.as_ref().is_some_and(|auth| !auth.is_current())
    }

    pub(crate) async fn close(&self) {
        let mut accepting = self.accepting.lock().await;
        *accepting = false;
        self.tasks.close();
    }

    pub(crate) async fn shutdown(&self) {
        self.close().await;
        self.tasks.wait().await;
    }

    pub(crate) async fn shutdown_with_evidence(&self) -> ConnectionRpcShutdown {
        self.close().await;
        self.tasks.wait().await;
        ConnectionRpcShutdown {
            panicked: self.panicked.load(Ordering::Acquire),
        }
    }

    #[cfg(test)]
    async fn is_accepting(&self) -> bool {
        *self.accepting.lock().await
    }

    #[cfg(test)]
    fn inflight_count(&self) -> usize {
        self.tasks.len()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ConnectionRpcShutdown {
    pub panicked: bool,
}

impl ConnectionRpcShutdown {
    pub(crate) fn is_clean(self) -> bool {
        !self.panicked
    }
}

impl Default for ConnectionRpcGate {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use tokio::sync::oneshot;
    use tokio::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn run_executes_while_open() {
        let gate = ConnectionRpcGate::new();
        let ran = Arc::new(AtomicBool::new(/*v*/ false));
        let ran_clone = Arc::clone(&ran);

        gate.run(async move {
            ran_clone.store(/*val*/ true, Ordering::Release);
        })
        .await;

        assert!(ran.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn run_drops_future_without_polling_after_close() {
        let gate = ConnectionRpcGate::new();
        gate.close().await;
        let polled = Arc::new(AtomicBool::new(/*v*/ false));
        let polled_clone = Arc::clone(&polled);

        gate.run(async move {
            polled_clone.store(/*val*/ true, Ordering::Release);
        })
        .await;

        assert!(!polled.load(Ordering::Acquire));
        assert!(!gate.is_accepting().await);
    }

    #[tokio::test]
    async fn close_returns_while_started_run_remains_active() {
        let gate = Arc::new(ConnectionRpcGate::new());
        let (started_tx, started_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = oneshot::channel();
        let gate_for_run = Arc::clone(&gate);
        let run_task = tokio::spawn(async move {
            gate_for_run
                .run(async move {
                    started_tx.send(()).expect("receiver should be open");
                    let _ = finish_rx.await;
                })
                .await;
        });

        started_rx.await.expect("run should start");
        gate.close().await;
        assert!(!gate.is_accepting().await);
        assert_eq!(gate.inflight_count(), 1);

        finish_tx
            .send(())
            .expect("running future should be waiting");
        run_task.await.expect("run task should complete");
        gate.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_waits_for_started_run_to_finish() {
        let gate = Arc::new(ConnectionRpcGate::new());
        let (started_tx, started_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = oneshot::channel();
        let gate_for_run = Arc::clone(&gate);
        let run_task = tokio::spawn(async move {
            gate_for_run
                .run(async move {
                    started_tx.send(()).expect("receiver should be open");
                    let _ = finish_rx.await;
                })
                .await;
        });

        started_rx.await.expect("run should start");
        assert_eq!(gate.inflight_count(), 1);

        let gate_for_shutdown = Arc::clone(&gate);
        let shutdown_task = tokio::spawn(async move {
            gate_for_shutdown.shutdown().await;
        });

        timeout(Duration::from_millis(/*millis*/ 50), shutdown_task)
            .await
            .expect_err("shutdown should wait for the running future");

        finish_tx
            .send(())
            .expect("running future should be waiting");
        run_task.await.expect("run task should complete");
        gate.shutdown().await;
        assert_eq!(gate.inflight_count(), 0);
    }

    #[tokio::test]
    async fn shutdown_drops_late_runs_while_waiting_for_inflight_work() {
        let gate = Arc::new(ConnectionRpcGate::new());
        let (started_tx, started_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = oneshot::channel();
        let gate_for_run = Arc::clone(&gate);
        let run_task = tokio::spawn(async move {
            gate_for_run
                .run(async move {
                    started_tx.send(()).expect("receiver should be open");
                    let _ = finish_rx.await;
                })
                .await;
        });

        started_rx.await.expect("run should start");
        let gate_for_shutdown = Arc::clone(&gate);
        let shutdown_task = tokio::spawn(async move {
            gate_for_shutdown.shutdown().await;
        });

        timeout(Duration::from_millis(/*millis*/ 50), shutdown_task)
            .await
            .expect_err("shutdown should wait for the running future");

        let late_polled = Arc::new(AtomicBool::new(/*v*/ false));
        let late_polled_clone = Arc::clone(&late_polled);
        gate.run(async move {
            late_polled_clone.store(/*val*/ true, Ordering::Release);
        })
        .await;

        assert!(!late_polled.load(Ordering::Acquire));

        finish_tx
            .send(())
            .expect("running future should still be waiting");
        run_task.await.expect("run task should complete");
        gate.shutdown().await;
        assert_eq!(gate.inflight_count(), 0);
    }

    #[tokio::test]
    async fn run_is_counted_before_handler_body_continues() {
        let gate = Arc::new(ConnectionRpcGate::new());
        let (entered_tx, entered_rx) = oneshot::channel();
        let (continue_tx, continue_rx) = oneshot::channel();
        let gate_for_run = Arc::clone(&gate);
        let run_task = tokio::spawn(async move {
            gate_for_run
                .run(async move {
                    entered_tx.send(()).expect("receiver should be open");
                    let _ = continue_rx.await;
                })
                .await;
        });

        entered_rx.await.expect("handler body should be entered");
        assert_eq!(gate.inflight_count(), 1);

        continue_tx
            .send(())
            .expect("handler body should still be waiting");
        run_task.await.expect("run task should complete");
        assert_eq!(gate.inflight_count(), 0);
    }

    #[tokio::test]
    async fn shutdown_evidence_distinguishes_handler_panic() {
        let gate = ConnectionRpcGate::new();
        gate.run(async { panic!("controlled RPC handler panic") })
            .await;
        let report = gate.shutdown_with_evidence().await;
        assert!(report.panicked);
        assert!(!report.is_clean());
    }

    #[tokio::test]
    async fn admitted_request_runs_and_is_not_unanswered() {
        let gate = Arc::new(ConnectionRpcGate::new());
        let ticket = ConnectionRpcGate::ticket(&gate);
        let ran = Arc::new(AtomicBool::new(false));
        gate.run_admitted(ticket, {
            let ran = Arc::clone(&ran);
            async move { ran.store(true, Ordering::SeqCst) }
        })
        .await;
        gate.queued_settled().await;
        assert!(ran.load(Ordering::SeqCst));
        assert_eq!(gate.unanswered(), 0);
    }

    #[tokio::test]
    async fn queued_request_after_close_is_refused_explicitly_and_never_started() {
        let gate = Arc::new(ConnectionRpcGate::new());
        let mut ticket = ConnectionRpcGate::ticket(&gate);
        let refused = Arc::new(AtomicBool::new(false));
        ticket.with_refusal({
            let refused = Arc::clone(&refused);
            async move {
                refused.store(true, Ordering::SeqCst);
                true
            }
        });
        // Inbound EOF closes admission while the request is still queued.
        gate.close().await;
        let started = Arc::new(AtomicBool::new(false));
        gate.run_admitted(ticket, {
            let started = Arc::clone(&started);
            async move { started.store(true, Ordering::SeqCst) }
        })
        .await;
        assert!(!started.load(Ordering::SeqCst), "admission stays closed");
        assert!(refused.load(Ordering::SeqCst), "answered with a refusal");
        assert_eq!(gate.unanswered(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn queued_request_stuck_past_the_deadline_is_counted_unanswered() {
        let gate = Arc::new(ConnectionRpcGate::new());
        // Read before EOF, still queued behind a stuck request.
        let ticket = ConnectionRpcGate::ticket(&gate);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(35);

        let settled = tokio::time::timeout_at(deadline, gate.queued_settled()).await;
        assert!(settled.is_err(), "an unsettled request is not drained");
        assert_eq!(tokio::time::Instant::now(), deadline, "no fresh budget");
        assert_eq!(gate.unanswered(), 1, "still owed an answer at the deadline");
        drop(ticket);
        assert_eq!(
            gate.unanswered(),
            1,
            "released without a reply: still counted"
        );
    }

    #[tokio::test]
    async fn refusal_that_could_not_be_sent_is_counted_unanswered() {
        let gate = Arc::new(ConnectionRpcGate::new());
        let mut ticket = ConnectionRpcGate::ticket(&gate);
        // The outbound path is gone: the refusal is not delivered.
        ticket.with_refusal(async { false });
        gate.close().await;
        gate.run_admitted(ticket, async {}).await;
        gate.queued_settled().await;
        assert_eq!(gate.unanswered(), 1);
    }

    #[tokio::test]
    async fn request_released_without_a_refusal_is_counted_unanswered() {
        let gate = Arc::new(ConnectionRpcGate::new());
        ConnectionRpcGate::ticket(&gate).refuse().await;
        gate.queued_settled().await;
        assert_eq!(gate.unanswered(), 1);
    }
    #[test]
    fn unanswered_counter_exhaustion_never_wraps_to_clean_zero() {
        let gate = Arc::new(ConnectionRpcGate::new());
        gate.unanswered.store(u64::MAX, Ordering::Release);
        let ticket = ConnectionRpcGate::ticket(&gate);
        drop(ticket);
        assert_eq!(gate.unanswered(), u64::MAX);
    }
}

#[cfg(test)]
#[path = "connection_rpc_child_tests.rs"]
mod child_tests;
