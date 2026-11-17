//! Owner deadline for the standalone (stdio/websocket) graceful drain.
//!
//! The owner budget is the in-process host's `SHUTDOWN_ACK_TIMEOUT`. It is
//! latched at the FIRST shutdown or admission-close edge (signal, daemon
//! shutdown, stdio EOF close, transport close), before any graceful drain.
//! Each historical processor stage keeps its shorter cap as
//! `min(cap, owner remaining)`. Work is driven only until its stage deadline:
//! expired work is retained, never polled again, and its stage is recorded as
//! `TimedOut`, which is sticky for the report.
//!
//! Post-processor tasks are cooperative. Async tasks (outbound router, stdio
//! forwarding, websocket drain, connection cleanup) run under the latched
//! `OwnerExpiry`, stop before their next unit at expiry and hand back their
//! owners. The telemetry actor clamps its drain to the same latched deadline
//! and returns its owner. The host's joins of those tasks are themselves
//! bounded by the owner deadline: a join still pending there is retained
//! unpolled and its stage is `TimedOut`. Blocking threads are reported
//! incomplete until their real joins prove a normal end.
//!
//! Expiry is always decided by the clock (`now >= deadline`) before a poll,
//! never only by a timer or token that may fire late.

use codex_app_server_transport::AbandonedOutput;
use codex_app_server_transport::OwnerCustody;
use codex_app_server_transport::OwnerExpiry;
use futures::future::BoxFuture;
use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;
use tokio::task::JoinError;
use tokio::time::Instant;
#[cfg(test)]
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::in_process::SHUTDOWN_ACK_TIMEOUT;
use crate::request_processors::BACKGROUND_TASK_DRAIN_CAP;
use crate::request_processors::THREAD_SHUTDOWN_CAP;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StageOutcome {
    Complete,
    Incomplete,
    TimedOut,
}

/// Historical stage caps. `None` means the stage only ever had the owner
/// bound (its inner waits keep their own caps, e.g. the 30s RPC drain).
#[derive(Clone, Copy, Debug)]
pub(crate) struct StageCaps {
    pub(crate) connections: Option<Duration>,
    pub(crate) background: Option<Duration>,
    pub(crate) threads: Option<Duration>,
}

pub(crate) const STANDALONE_STAGE_CAPS: StageCaps = StageCaps {
    connections: None,
    background: Some(BACKGROUND_TASK_DRAIN_CAP),
    threads: Some(THREAD_SHUTDOWN_CAP),
};

pub(crate) use crate::shutdown_intake::OwnerLatch;
#[cfg(test)]
use crate::shutdown_intake::{install_intake_latch, spawn_signal_intake};

/// Processor-side shutdown and control-queue submissions. Each is bounded by
/// the ORIGINAL latched deadline (expiry checked first; before the edge is
/// latched it simply waits, and the edge then bounds it). One still pending
/// at the deadline is retained unpolled, never dropped, and the shutdown is
/// reported incomplete.
#[derive(Default)]
pub(crate) struct ShutdownSends {
    retained: Vec<(&'static str, Box<dyn Send>)>,
}

impl ShutdownSends {
    pub(crate) async fn submit<F>(
        &mut self,
        latch: &OwnerLatch,
        what: &'static str,
        work: F,
    ) -> Option<F::Output>
    where
        F: Future + Send + 'static,
    {
        let mut work = Box::pin(work);
        let output = latch.observe_until_deadline(&mut work).await;
        if output.is_none() {
            warn!(
                what,
                "shutdown submission still pending at the owner deadline; retained (incomplete)"
            );
            self.retained.push((what, Box::new(work)));
        }
        output
    }

    /// Hand every expired submission to the shutdown owner: retained there
    /// and reported incomplete.
    pub(crate) fn hand_to(self, owner: &mut StandaloneShutdownOwner) {
        for (what, work) in self.retained {
            owner.push(what, StageOutcome::TimedOut);
            owner.retain(work);
        }
    }
}

/// The processor task's output through its fence (see `spawn_fenced_processor`).
pub(crate) type FencedProcessorOutput =
    Result<Option<ProcessorTaskOutput>, codex_app_server_transport::NoCustodyOwner>;

/// Spawn the processor body under the owner fence: Tokio polls the
/// `OwnerExpiry::run` wrapper, which checks expiry (and owner release) before
/// every poll of the body. At the deadline the exact unpolled body is moved
/// into the custody (never aborted, never polled again), so a stalled inline
/// init/config/handler future cannot keep being driven after the deadline.
/// Truly uncancellable work (a synchronous call inside one poll, or a
/// `spawn_blocking` thread the body awaits) cannot be fenced by poll gating;
/// see REV8-NOTES for that classification.
pub(crate) fn spawn_fenced_processor<F>(
    expiry: OwnerExpiry,
    body: F,
) -> tokio::task::JoinHandle<FencedProcessorOutput>
where
    F: Future<Output = ProcessorTaskOutput> + Send + 'static,
{
    tokio::spawn(async move { expiry.run("processor", body).await })
}

/// Wire the processor to the first shutdown edge (intake or loop): turn
/// admission closes at once and the cleanup deadlines are bound to the
/// original latched deadline, even if the processor loop is stuck. Holds only
/// a `Weak` processor: no cycle through the latch.
pub(crate) fn register_processor_intake(
    latch: &OwnerLatch,
    processor: &std::sync::Arc<crate::message_processor::MessageProcessor>,
) {
    let weak = std::sync::Arc::downgrade(processor);
    latch.on_edge(move |deadline| {
        if let Some(processor) = weak.upgrade() {
            processor.begin_shutdown_at_intake(deadline);
        }
    });
}

/// The processor task's result as the host sees it.
pub(crate) enum ProcessorJoin {
    /// Managed-daemon forced exit: the host returns at once.
    Forced,
    Owner(StandaloneShutdownOwner),
}

pub(crate) type ProcessorTaskOutput = (crate::AppServerExit, Option<StandaloneShutdownOwner>);

/// Observe the processor join under the original latched deadline, so a
/// stalled or panicked processor path cannot bypass the host budget, then
/// hand the host's explicit custody to the resulting shutdown owner.
///
/// - Joined with an owner: that owner (it holds the latched deadline).
/// - Joined without an owner, or panicked: incomplete (`processor`).
/// - Still pending at the deadline: the exact unjoined handle is retained by
///   the shutdown owner (never aborted, never polled again) and `processor`
///   is `TimedOut`.
///
/// The custody is never parked in the latch or in the retained join, so the
/// processor task (which captures the latch) cannot keep it alive.
pub(crate) async fn join_processor<J>(
    latch: &OwnerLatch,
    custody: Option<OwnerCustody>,
    mut join: J,
) -> ProcessorJoin
where
    J: Future<Output = Result<FencedProcessorOutput, JoinError>> + Unpin + Send + 'static,
{
    let mut owner = match latch.observe_until_deadline(&mut join).await {
        Some(Ok(Ok(Some((crate::AppServerExit::Forced, _))))) => return ProcessorJoin::Forced,
        Some(Ok(Ok(Some((_, Some(owner)))))) => owner,
        Some(Ok(Ok(None))) => {
            // The fence stopped the processor driver at the deadline: its
            // exact unpolled body is retained in the custody (not aborted,
            // never polled again).
            warn!(
                "standalone shutdown incomplete: processor driver stopped at the owner deadline; retained, not aborted"
            );
            let mut owner = StandaloneShutdownOwner::new(latch.latch(), latch.expiry());
            owner.push("processor", StageOutcome::TimedOut);
            owner
        }
        Some(Ok(Err(refused))) => {
            warn!("standalone shutdown incomplete: processor not driven: {refused}");
            latch.failed_processor_owner()
        }
        Some(Ok(Ok(Some((_, None))))) | Some(Err(_)) => {
            // Nothing proves cleanup, so this is never a clean exit. The
            // remaining joins stay under the original latched deadline.
            warn!("standalone shutdown incomplete: processor task failed before its owner report");
            latch.failed_processor_owner()
        }
        None => {
            warn!(
                "standalone shutdown incomplete: processor task still running at the owner deadline; join retained, not aborted"
            );
            let mut owner = StandaloneShutdownOwner::new(latch.latch(), latch.expiry());
            owner.push("processor", StageOutcome::TimedOut);
            owner.retain(join);
            owner
        }
    };
    owner.adopt_custody(custody);
    ProcessorJoin::Owner(owner)
}

/// Submit the close-all handoff to the outbound router under the owner's
/// ORIGINAL deadline. The router may already be retained at expiry with its
/// control queue full, so the send is never awaited unbounded: expired, it
/// is retained unpolled (`TimedOut`); refused (router gone), it is
/// incomplete. The owner itself is always handed back.
pub(crate) async fn submit_close_all<E>(
    owner: &mut StandaloneShutdownOwner,
    control: &tokio::sync::mpsc::Sender<E>,
    event: E,
) where
    E: Send + 'static,
{
    let control = control.clone();
    let deadline = owner.deadline();
    let send = async move { control.send(event).await.is_ok() };
    if let Some(sent) = owner.drive("close_all", deadline, send).await {
        owner.record("close_all", sent);
    }
}

/// Poll `work` until it completes or `deadline`, never at or after it: the
/// clock is checked before every poll. The timer only wakes this future.
pub(crate) async fn poll_before<F>(deadline: Instant, mut work: Pin<&mut F>) -> Option<F::Output>
where
    F: Future + ?Sized,
{
    let mut timer = std::pin::pin!(tokio::time::sleep_until(deadline));
    std::future::poll_fn(|cx| {
        if Instant::now() >= deadline || timer.as_mut().poll(cx).is_ready() {
            return Poll::Ready(None);
        }
        work.as_mut().poll(cx).map(Some)
    })
    .await
}

/// Held by the host until it returns (process exit for the binary). Dropping
/// it releases retained, never-again-polled work; it never aborts tasks.
pub(crate) struct StandaloneShutdownOwner {
    started: Instant,
    deadline: Instant,
    expiry: OwnerExpiry,
    stage_started: Instant,
    stages: Vec<StageRecord>,
    retained: Vec<Box<dyn Send>>,
    // The explicit custody owner of work retained through `OwnerExpiry`
    // (outbound router, stdio/websocket forwarding, cleanup, refusals).
    // Dropping this owner releases that work.
    custody: Option<OwnerCustody>,
}

/// One stage's outcome and wall time, for attribution of the shutdown tail.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StageRecord {
    pub(crate) stage: &'static str,
    pub(crate) outcome: StageOutcome,
    pub(crate) elapsed: Duration,
}

impl StandaloneShutdownOwner {
    /// `deadline` is the latched owner deadline; `expiry` its shared custody.
    pub(crate) fn new(deadline: Instant, expiry: OwnerExpiry) -> Self {
        let started = Instant::now();
        Self {
            started,
            deadline,
            expiry,
            stage_started: started,
            stages: Vec::new(),
            retained: Vec::new(),
            custody: None,
        }
    }

    /// Become the external owner of the latch's custody (its retained work
    /// is released when this owner drops, never before).
    pub(crate) fn adopt_custody(&mut self, custody: Option<OwnerCustody>) {
        if custody.is_some() {
            self.custody = custody;
        }
    }

    pub(crate) fn deadline(&self) -> Instant {
        self.deadline
    }

    /// `min(now + cap, owner)`; a stage can never extend the owner.
    pub(crate) fn stage_deadline(&self, cap: Option<Duration>) -> Instant {
        cap.map_or(self.deadline, |cap| self.deadline.min(Instant::now() + cap))
    }

    /// Drive `work` until it completes or `stage_deadline`, never after. The
    /// expiry check precedes every poll. Expired work is retained unpolled and
    /// the stage is recorded `TimedOut`.
    pub(crate) async fn drive<F>(
        &mut self,
        stage: &'static str,
        stage_deadline: Instant,
        work: F,
    ) -> Option<F::Output>
    where
        F: Future + Send + 'static,
    {
        self.stage_started = Instant::now();
        let mut work = Box::pin(work);
        let completed = poll_before(stage_deadline, work.as_mut()).await;
        if completed.is_none() {
            self.push(stage, StageOutcome::TimedOut);
            self.retained.push(Box::new(work));
        }
        completed
    }

    /// Join cooperative (b) task(s) under the owner deadline, without a
    /// fresh budget: each task should stop at the latched owner expiry and
    /// return, handing any unfinished owner to `OwnerExpiry`. Cooperation is
    /// not trusted to bound the join: if the joins are still pending at the
    /// owner deadline they are retained exactly as they are (never polled
    /// again, never aborted), `stage` is recorded `TimedOut` and `None` is
    /// returned. Otherwise returns the join output and whether any of
    /// `tasks` handed back an owner; the caller records the stage.
    pub(crate) async fn join_cooperative<F>(
        &mut self,
        stage: &'static str,
        tasks: &[&'static str],
        joins: F,
    ) -> Option<(F::Output, bool)>
    where
        F: Future + Send + 'static,
    {
        self.stage_started = Instant::now();
        let mut joins = Box::pin(joins);
        let Some(output) = poll_before(self.deadline, joins.as_mut()).await else {
            self.push(stage, StageOutcome::TimedOut);
            self.retain(joins);
            return None;
        };
        let returned = self.expiry.returned_tasks();
        let owner_returned = tasks.iter().any(|task| returned.contains(task));
        Some((output, owner_returned))
    }

    /// Record the outcome of the stage most recently started by `drive`.
    pub(crate) fn record(&mut self, stage: &'static str, clean: bool) {
        let outcome = if clean {
            StageOutcome::Complete
        } else {
            StageOutcome::Incomplete
        };
        self.push(stage, outcome);
    }

    fn push(&mut self, stage: &'static str, outcome: StageOutcome) {
        let record = StageRecord {
            stage,
            outcome,
            elapsed: self.stage_started.elapsed(),
        };
        debug!(
            event.name = "codex.app_server.standalone_shutdown_stage",
            stage,
            outcome = ?record.outcome,
            elapsed_ms = record.elapsed.as_millis(),
            "standalone shutdown stage finished"
        );
        self.stages.push(record);
    }

    /// Keep an owner (ticket, processor, handle) until host return.
    pub(crate) fn retain<T: Send + 'static>(&mut self, owner: T) {
        self.retained.push(Box::new(owner));
    }

    #[cfg(test)]
    pub(crate) fn stages(&self) -> &[StageRecord] {
        &self.stages
    }

    pub(crate) fn timed_out(&self) -> bool {
        self.stages
            .iter()
            .any(|record| record.outcome == StageOutcome::TimedOut)
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.stages
            .iter()
            .all(|record| record.outcome == StageOutcome::Complete)
            && self.expiry.returned_tasks().is_empty()
            && self.expiry.refused_tasks().is_empty()
            && self.expiry.stopped_by_owner_release().is_empty()
            && self.expiry.incomplete_blocking().is_empty()
            // Delivery is separate from custody: dropped output is never
            // successful completion, even when every thread joined cleanly.
            && self.expiry.abandoned_output() == AbandonedOutput::default()
    }

    /// Truthful report before the host returns. A returned host still drops
    /// its Tokio runtime, and that drop may block on `spawn_blocking` work
    /// (for example a stuck rollout write); this report precedes that drop.
    /// Typed outcome for library and CLI callers. Anything short of positive
    /// proof (a TimedOut or incomplete stage, a handed-back owner, an
    /// unfinished blocking receipt) is `ShutdownIncomplete`, never `Graceful`.
    pub(crate) fn exit_outcome(&self) -> crate::AppServerExit {
        if self.is_complete() {
            crate::AppServerExit::Graceful
        } else {
            crate::AppServerExit::ShutdownIncomplete
        }
    }

    /// Observe (a) blocking receipts until the owner deadline, without
    /// driving or stopping them, so a thread that is about to exit is not
    /// misreported. One still running at the deadline stays incomplete; a
    /// panicked one has nothing left to observe and stays incomplete too.
    pub(crate) async fn observe_blocking(&mut self) {
        while !self.expiry.running_blocking().is_empty() && Instant::now() < self.deadline {
            tokio::time::sleep(
                Duration::from_millis(10)
                    .min(self.deadline.saturating_duration_since(Instant::now())),
            )
            .await;
        }
    }

    /// Its timestamp marks the hand-off to runtime drop: process exit minus
    /// this event is the runtime drop plus process teardown, which no stage owns.
    pub(crate) fn report_before_release(&self) {
        let elapsed_ms = self.started.elapsed().as_millis();
        let owner_remaining_ms = self
            .deadline
            .saturating_duration_since(Instant::now())
            .as_millis();
        let returned_owners = self.expiry.returned_tasks();
        let blocking_incomplete = self.expiry.incomplete_blocking();
        // Every blocking join with its state, e.g. ("stdin_reader", "cancelled"),
        // ("stdout_writer", "cancelled: output abandoned") or "incomplete".
        let blocking = self.expiry.blocking_states();
        let abandoned = self.expiry.abandoned_output();
        let messages_abandoned = abandoned.messages;
        let bytes_abandoned = abandoned.bytes;
        // Separate from abandonment: output for recipients that positively
        // closed. Counted, but not a global incompleteness.
        let undeliverable = self.expiry.undeliverable_after_disconnect();
        let undeliverable_after_disconnect = undeliverable.messages;
        if self.is_complete() {
            info!(
                event.name = "codex.app_server.standalone_shutdown",
                elapsed_ms,
                owner_remaining_ms,
                stages = ?self.stages,
                ?blocking,
                messages_abandoned,
                bytes_abandoned,
                undeliverable_after_disconnect,
                "standalone shutdown complete; runtime drop next"
            );
        } else {
            warn!(
                event.name = "codex.app_server.standalone_shutdown",
                timed_out = self.timed_out(),
                elapsed_ms,
                owner_remaining_ms,
                stages = ?self.stages,
                ?returned_owners,
                refused_unowned = ?self.expiry.refused_tasks(),
                stopped_by_owner_release = ?self.expiry.stopped_by_owner_release(),
                ?blocking_incomplete,
                ?blocking,
                messages_abandoned,
                bytes_abandoned,
                undeliverable_after_disconnect,
                "standalone shutdown incomplete; owners retained; runtime drop next and may block"
            );
        }
    }
}

/// The host's post-processor joins, in historical order, all under the one
/// latched owner deadline (no fresh budget, no abort). Each join still pending
/// at the deadline is retained unpolled with a sticky `TimedOut` stage. The
/// truthful report precedes the returned outcome; the caller keeps `owner`,
/// and everything it retained, until the host returns.
pub(crate) async fn finish_host_shutdown<O, T, R, A>(
    owner: &mut StandaloneShutdownOwner,
    outbound: O,
    stop_transport: impl FnOnce(),
    telemetry: T,
    telemetry_clean: impl FnOnce(&R) -> bool,
    acceptors: A,
) -> crate::AppServerExit
where
    O: Future<Output = Result<(), JoinError>> + Send + 'static,
    T: Future<Output = Result<R, JoinError>> + Send + 'static,
    R: Send + 'static,
    A: Future<Output = Vec<Result<(), JoinError>>> + Send + 'static,
{
    if let Some((joined, owner_returned)) = owner
        .join_cooperative("outbound", &["outbound_router"], outbound)
        .await
    {
        owner.record("outbound", joined.is_ok() && !owner_returned);
    }

    stop_transport();
    // A joined actor is not proof that its exporters retired. The actor's
    // drain is min(30s, latched owner) and returns its owner either way; an
    // exporter worker still running at expiry is (a) and reported incomplete.
    if let Some((joined, _)) = owner.join_cooperative("telemetry", &[], telemetry).await {
        match joined {
            Ok(reloader) => {
                let clean = telemetry_clean(&reloader);
                owner.record("telemetry", clean);
                owner.retain(reloader);
            }
            Err(error) => {
                warn!(
                    cancelled = error.is_cancelled(),
                    panicked = error.is_panic(),
                    "standalone telemetry reloader task failed"
                );
                owner.record("telemetry", /*clean*/ false);
            }
        }
    }
    if let Some((joined, owner_returned)) = owner
        .join_cooperative(
            "acceptors",
            &["stdio_connection", "websocket_acceptor"],
            acceptors,
        )
        .await
    {
        owner.record(
            "acceptors",
            joined.iter().all(Result::is_ok) && !owner_returned,
        );
    }
    // (a) blocking receipts (e.g. the stdout writer thread) are observed, not
    // driven, until the owner deadline; still running then stays incomplete.
    owner.observe_blocking().await;
    // Truthful, typed report first; owners are released only when this host returns.
    owner.report_before_release();
    owner.exit_outcome()
}

/// The processor-side stages of the standalone drain, in historical order.
pub(crate) trait StandaloneStages {
    /// Bind every runtime's cleanup deadline before any Shutdown submission.
    fn bind_runtimes(&mut self, owner_deadline: Instant) -> BoxFuture<'static, bool>;
    fn connections(&mut self, stage_deadline: Instant) -> BoxFuture<'static, bool>;
    fn background(&mut self, stage_deadline: Instant) -> BoxFuture<'static, bool>;
    /// Plugin worker registry census (curated sync); sticky once latched.
    fn plugin_tasks(&mut self, stage_deadline: Instant) -> BoxFuture<'static, bool>;
    fn threads(&mut self, stage_deadline: Instant) -> BoxFuture<'static, bool>;
}

pub(crate) async fn drain_processor_stages<S: StandaloneStages>(
    owner: &mut StandaloneShutdownOwner,
    stages: &mut S,
    caps: StageCaps,
) {
    let deadline = owner.stage_deadline(/*cap*/ None);
    let bind = stages.bind_runtimes(owner.deadline());
    if let Some(clean) = owner.drive("bind_runtimes", deadline, bind).await {
        owner.record("bind_runtimes", clean);
    }
    let deadline = owner.stage_deadline(caps.connections);
    let work = stages.connections(deadline);
    if let Some(clean) = owner.drive("connections", deadline, work).await {
        owner.record("connections", clean);
    }
    let deadline = owner.stage_deadline(caps.background);
    let work = stages.background(deadline);
    if let Some(clean) = owner.drive("background", deadline, work).await {
        owner.record("background", clean);
    }
    let deadline = owner.stage_deadline(caps.background);
    let work = stages.plugin_tasks(deadline);
    if let Some(clean) = owner.drive("plugin_tasks", deadline, work).await {
        owner.record("plugin_tasks", clean);
    }
    let deadline = owner.stage_deadline(caps.threads);
    let work = stages.threads(deadline);
    if let Some(clean) = owner.drive("threads", deadline, work).await {
        owner.record("threads", clean);
    }
}

#[cfg(test)]
#[path = "standalone_shutdown_tests.rs"]
mod tests;
