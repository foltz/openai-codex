use super::*;
use futures::FutureExt;
use pretty_assertions::assert_eq;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use tokio::task::JoinError;

/// Stalled cleanup: counts every poll, never completes, records its drop.
struct Stalled {
    polls: Arc<AtomicUsize>,
    dropped: Arc<AtomicBool>,
}

impl Future for Stalled {
    type Output = bool;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<bool> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        Poll::Pending
    }
}

impl Drop for Stalled {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

#[derive(Clone, Default)]
struct Probe {
    polls: Arc<AtomicUsize>,
    dropped: Arc<AtomicBool>,
}

impl Probe {
    fn stalled(&self) -> BoxFuture<'static, bool> {
        Stalled {
            polls: Arc::clone(&self.polls),
            dropped: Arc::clone(&self.dropped),
        }
        .boxed()
    }

    fn polls(&self) -> usize {
        self.polls.load(Ordering::SeqCst)
    }

    fn dropped(&self) -> bool {
        self.dropped.load(Ordering::SeqCst)
    }
}

/// Each stage either finishes after `delay` or stalls forever.
#[derive(Default)]
struct FakeStages {
    connections: Option<Duration>,
    background: Option<Duration>,
    threads: Option<Duration>,
    /// A fake curated-sync worker that never returns.
    plugin_tasks_stall: bool,
    connections_probe: Probe,
    background_probe: Probe,
    threads_probe: Probe,
    bound_at: Option<Instant>,
    /// The runtime binding found a runtime it could not bind (Legacy cleanup).
    bind_incomplete: bool,
}

fn stage(delay: Option<Duration>, probe: &Probe) -> BoxFuture<'static, bool> {
    match delay {
        Some(delay) => async move {
            tokio::time::sleep(delay).await;
            true
        }
        .boxed(),
        None => probe.stalled(),
    }
}

impl StandaloneStages for FakeStages {
    fn bind_runtimes(&mut self, owner_deadline: Instant) -> BoxFuture<'static, bool> {
        self.bound_at = Some(owner_deadline);
        futures::future::ready(!self.bind_incomplete).boxed()
    }

    fn connections(&mut self, _stage_deadline: Instant) -> BoxFuture<'static, bool> {
        stage(self.connections, &self.connections_probe)
    }

    fn background(&mut self, _stage_deadline: Instant) -> BoxFuture<'static, bool> {
        stage(self.background, &self.background_probe)
    }

    fn plugin_tasks(&mut self, _stage_deadline: Instant) -> BoxFuture<'static, bool> {
        let delay = (!self.plugin_tasks_stall).then_some(Duration::ZERO);
        stage(delay, &Probe::default())
    }

    fn threads(&mut self, _stage_deadline: Instant) -> BoxFuture<'static, bool> {
        stage(self.threads, &self.threads_probe)
    }
}

fn outcome(owner: &StandaloneShutdownOwner, name: &str) -> Option<StageOutcome> {
    owner
        .stages()
        .iter()
        .find(|record| record.stage == name)
        .map(|record| record.outcome)
}

#[tokio::test(start_paused = true)]
async fn stalled_thread_cleanup_keeps_its_shorter_cap_and_times_out() {
    let start = Instant::now();
    let mut owner =
        StandaloneShutdownOwner::new(start + SHUTDOWN_ACK_TIMEOUT, OwnerExpiry::never());
    let mut stages = FakeStages {
        connections: Some(Duration::ZERO),
        background: Some(Duration::ZERO),
        threads: None,
        ..Default::default()
    };

    drain_processor_stages(&mut owner, &mut stages, STANDALONE_STAGE_CAPS).await;

    // The 10s thread cap is shorter than the 35s owner and still applies.
    assert_eq!(Instant::now() - start, THREAD_SHUTDOWN_CAP);
    let threads = owner.stages().last().copied();
    assert_eq!(
        threads.map(|record| (record.stage, record.elapsed)),
        Some(("threads", THREAD_SHUTDOWN_CAP))
    );
    assert_eq!(stages.bound_at, Some(start + SHUTDOWN_ACK_TIMEOUT));
    assert_eq!(outcome(&owner, "connections"), Some(StageOutcome::Complete));
    assert_eq!(outcome(&owner, "threads"), Some(StageOutcome::TimedOut));
    assert!(owner.timed_out());
    assert!(!owner.is_complete());
}

#[tokio::test(start_paused = true)]
async fn standalone_exit_with_stalled_thread_cleanup_returns_by_owner_deadline() {
    let start = Instant::now();
    let mut owner =
        StandaloneShutdownOwner::new(start + SHUTDOWN_ACK_TIMEOUT, OwnerExpiry::never());
    // Connection close legitimately uses 30s (its RPC drain cap); background
    // stalls, so only the 5s owner remainder applies, not its 10s cap; thread
    // cleanup stalls and is reached after the owner has expired.
    let mut stages = FakeStages {
        connections: Some(Duration::from_secs(30)),
        background: None,
        threads: None,
        ..Default::default()
    };

    drain_processor_stages(&mut owner, &mut stages, STANDALONE_STAGE_CAPS).await;

    assert_eq!(Instant::now() - start, SHUTDOWN_ACK_TIMEOUT);
    assert_eq!(outcome(&owner, "connections"), Some(StageOutcome::Complete));
    assert_eq!(outcome(&owner, "background"), Some(StageOutcome::TimedOut));
    assert_eq!(outcome(&owner, "threads"), Some(StageOutcome::TimedOut));
    // Reached after expiry: retained, but never polled even once.
    assert_eq!(stages.threads_probe.polls(), 0);
    assert!(!stages.threads_probe.dropped());
}

#[tokio::test(start_paused = true)]
async fn expired_cleanup_is_retained_and_never_polled_again() {
    let start = Instant::now();
    let mut owner =
        StandaloneShutdownOwner::new(start + SHUTDOWN_ACK_TIMEOUT, OwnerExpiry::never());
    let mut stages = FakeStages {
        connections: Some(Duration::ZERO),
        background: Some(Duration::ZERO),
        threads: None,
        ..Default::default()
    };

    drain_processor_stages(&mut owner, &mut stages, STANDALONE_STAGE_CAPS).await;
    let polls_at_expiry = stages.threads_probe.polls();
    assert!(polls_at_expiry > 0);

    tokio::time::advance(Duration::from_secs(600)).await;
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }

    assert_eq!(stages.threads_probe.polls(), polls_at_expiry);
    // Owner custody: not dropped (not cancelled) until the host releases it.
    assert!(!stages.threads_probe.dropped());
    assert_eq!(outcome(&owner, "threads"), Some(StageOutcome::TimedOut));
    drop(owner);
    assert!(stages.threads_probe.dropped());
}

#[tokio::test(start_paused = true)]
async fn owner_deadline_is_latched_once_and_never_extended() {
    let (latch, _custody) = OwnerLatch::new();
    let observer = latch.subscribe();
    let first = latch.latch();
    tokio::time::advance(Duration::from_secs(5)).await;
    let second = latch.latch();

    assert_eq!(first, second);
    assert_eq!(*observer.borrow(), Some(first));
    let owner = StandaloneShutdownOwner::new(first, latch.expiry());
    assert_eq!(
        owner.stage_deadline(Some(Duration::from_secs(30))),
        first,
        "a 30s cap 5s into a 35s owner is clamped to the owner"
    );
    assert_eq!(
        owner.stage_deadline(Some(THREAD_SHUTDOWN_CAP)),
        Instant::now() + THREAD_SHUTDOWN_CAP
    );
}

#[tokio::test(start_paused = true)]
async fn latch_expiry_token_fires_exactly_at_the_owner_deadline() {
    let (latch, _custody) = OwnerLatch::new();
    let start = Instant::now();
    let deadline = latch.latch();
    let expired = latch.expiry().expired_token();

    tokio::time::sleep_until(deadline - Duration::from_millis(1)).await;
    assert!(!expired.is_cancelled());
    expired.cancelled().await;
    assert_eq!(Instant::now() - start, SHUTDOWN_ACK_TIMEOUT);
}

#[tokio::test(start_paused = true)]
async fn long_graceful_drain_consumes_the_same_owner_budget() {
    let (latch, _custody) = OwnerLatch::new();
    // First shutdown edge (signal / stdio EOF) latches before the drain.
    let edge = Instant::now();
    latch.latch();
    // A 30s graceful drain (waiting for running turns) happens next.
    tokio::time::advance(Duration::from_secs(30)).await;

    // Loop exit re-latches: the original deadline, not a fresh 35s.
    let mut owner = StandaloneShutdownOwner::new(latch.latch(), latch.expiry());
    assert_eq!(owner.deadline(), edge + SHUTDOWN_ACK_TIMEOUT);
    let mut stages = FakeStages {
        connections: Some(Duration::ZERO),
        background: Some(Duration::ZERO),
        threads: None,
        ..Default::default()
    };
    drain_processor_stages(&mut owner, &mut stages, STANDALONE_STAGE_CAPS).await;

    // The 10s thread cap is clamped to the 5s that remain of the same budget.
    assert_eq!(Instant::now() - edge, SHUTDOWN_ACK_TIMEOUT);
    assert_eq!(outcome(&owner, "threads"), Some(StageOutcome::TimedOut));
}

#[tokio::test(start_paused = true)]
async fn stalled_curated_worker_is_incomplete_at_its_bounded_stage() {
    let start = Instant::now();
    let mut owner =
        StandaloneShutdownOwner::new(start + SHUTDOWN_ACK_TIMEOUT, OwnerExpiry::never());
    let mut stages = FakeStages {
        connections: Some(Duration::ZERO),
        background: Some(Duration::ZERO),
        threads: Some(Duration::ZERO),
        plugin_tasks_stall: true,
        ..Default::default()
    };

    drain_processor_stages(&mut owner, &mut stages, STANDALONE_STAGE_CAPS).await;

    // Bounded by min(10s background cap, owner); later stages still run.
    assert_eq!(Instant::now() - start, BACKGROUND_TASK_DRAIN_CAP);
    assert_eq!(
        outcome(&owner, "plugin_tasks"),
        Some(StageOutcome::TimedOut)
    );
    assert_eq!(outcome(&owner, "threads"), Some(StageOutcome::Complete));
    assert_eq!(
        owner.exit_outcome(),
        crate::AppServerExit::ShutdownIncomplete
    );
}

#[tokio::test(start_paused = true)]
async fn stalled_cleanup_can_never_be_reported_as_graceful() {
    let start = Instant::now();
    let mut owner =
        StandaloneShutdownOwner::new(start + SHUTDOWN_ACK_TIMEOUT, OwnerExpiry::never());
    let mut stages = FakeStages {
        connections: Some(Duration::ZERO),
        background: Some(Duration::ZERO),
        threads: None,
        ..Default::default()
    };
    drain_processor_stages(&mut owner, &mut stages, STANDALONE_STAGE_CAPS).await;
    assert_eq!(
        owner.exit_outcome(),
        crate::AppServerExit::ShutdownIncomplete
    );
    assert_ne!(owner.exit_outcome(), crate::AppServerExit::Graceful);

    // A later clean-looking record cannot launder the sticky TimedOut.
    owner.record("acceptors", /*clean*/ true);
    assert_eq!(
        owner.exit_outcome(),
        crate::AppServerExit::ShutdownIncomplete
    );
}

#[tokio::test(start_paused = true)]
async fn clean_drain_stays_graceful() {
    let start = Instant::now();
    let mut owner =
        StandaloneShutdownOwner::new(start + SHUTDOWN_ACK_TIMEOUT, OwnerExpiry::never());
    let mut stages = FakeStages {
        connections: Some(Duration::ZERO),
        background: Some(Duration::ZERO),
        threads: Some(Duration::ZERO),
        ..Default::default()
    };
    drain_processor_stages(&mut owner, &mut stages, STANDALONE_STAGE_CAPS).await;
    assert_eq!(owner.exit_outcome(), crate::AppServerExit::Graceful);
}

#[tokio::test(start_paused = true)]
async fn handed_back_owner_makes_the_exit_incomplete() {
    let expired = CancellationToken::new();
    let custody = codex_app_server_transport::OwnerCustody::new(expired.clone());
    let expiry = custody.expiry();
    expired.cancel();
    assert_eq!(expiry.run("outbound_router", async {}).await, Ok(None));
    let mut owner = StandaloneShutdownOwner::new(Instant::now(), expiry);
    owner.adopt_custody(Some(custody));
    assert_eq!(
        owner.exit_outcome(),
        crate::AppServerExit::ShutdownIncomplete
    );
}

#[tokio::test(start_paused = true)]
async fn abandoned_output_is_incomplete_even_when_custody_is_clean() {
    let expiry = OwnerExpiry::never();
    let start = Instant::now();
    let mut owner = StandaloneShutdownOwner::new(start + SHUTDOWN_ACK_TIMEOUT, expiry.clone());
    let mut stages = FakeStages {
        connections: Some(Duration::ZERO),
        background: Some(Duration::ZERO),
        threads: Some(Duration::ZERO),
        ..Default::default()
    };
    drain_processor_stages(&mut owner, &mut stages, STANDALONE_STAGE_CAPS).await;
    assert_eq!(owner.exit_outcome(), crate::AppServerExit::Graceful);

    // Every thread joined, but a response was dropped: not completion.
    expiry.record_abandoned(/*messages*/ 2, /*bytes*/ 1_400_000);
    assert_eq!(
        owner.exit_outcome(),
        crate::AppServerExit::ShutdownIncomplete
    );
}

fn stalled_join(probe: &Probe) -> impl Future<Output = Result<(), JoinError>> + Send + 'static {
    probe.stalled().map(|_| Ok(()))
}

#[tokio::test(start_paused = true)]
async fn never_finishing_cooperative_join_is_retained_and_timed_out() {
    let start = Instant::now();
    let mut owner =
        StandaloneShutdownOwner::new(start + SHUTDOWN_ACK_TIMEOUT, OwnerExpiry::never());
    let probe = Probe::default();

    let joined = owner
        .join_cooperative("outbound", &["outbound_router"], stalled_join(&probe))
        .await;

    // Cooperation did not bound it; the owner deadline did. No fresh budget.
    assert!(joined.is_none());
    assert_eq!(Instant::now() - start, SHUTDOWN_ACK_TIMEOUT);
    assert_eq!(outcome(&owner, "outbound"), Some(StageOutcome::TimedOut));
    let polls_at_expiry = probe.polls();
    tokio::time::advance(Duration::from_secs(600)).await;
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        probe.polls(),
        polls_at_expiry,
        "retained, never polled again"
    );
    assert!(!probe.dropped(), "retained by the owner, not dropped");
    // Sticky: a later clean record cannot launder the TimedOut join.
    owner.record("outbound", /*clean*/ true);
    assert_eq!(
        owner.exit_outcome(),
        crate::AppServerExit::ShutdownIncomplete
    );
    drop(owner);
    assert!(probe.dropped());
}

#[tokio::test(start_paused = true)]
async fn retained_task_join_is_never_aborted() {
    let start = Instant::now();
    let mut owner =
        StandaloneShutdownOwner::new(start + SHUTDOWN_ACK_TIMEOUT, OwnerExpiry::never());
    let probe = Probe::default();
    // A spawned cooperative task that ignores the owner expiry.
    let task = tokio::spawn(stalled_join(&probe));

    let joined = owner.join_cooperative("acceptors", &[], task).await;

    assert!(joined.is_none());
    assert_eq!(outcome(&owner, "acceptors"), Some(StageOutcome::TimedOut));
    tokio::task::yield_now().await;
    // The exact JoinHandle is retained: the task is neither aborted nor
    // dropped while the owner holds it.
    assert!(!probe.dropped());
    drop(owner);
    tokio::task::yield_now().await;
    assert!(
        !probe.dropped(),
        "releasing a JoinHandle detaches, never aborts"
    );
}

#[tokio::test(start_paused = true)]
async fn drive_never_polls_work_once_the_stage_deadline_has_passed() {
    let start = Instant::now();
    let mut owner =
        StandaloneShutdownOwner::new(start + SHUTDOWN_ACK_TIMEOUT, OwnerExpiry::never());
    let polls = Arc::new(AtomicUsize::new(0));
    let (open_gate, gate) = tokio::sync::oneshot::channel::<()>();
    let work = {
        let polls = Arc::clone(&polls);
        let mut gate = gate;
        std::future::poll_fn(move |cx| {
            polls.fetch_add(1, Ordering::SeqCst);
            Pin::new(&mut gate).poll(cx).map(|_| true)
        })
    };
    let deadline = owner.deadline();
    {
        let mut drive = std::pin::pin!(owner.drive("threads", deadline, work));
        let first = std::future::poll_fn(|cx| Poll::Ready(drive.as_mut().poll(cx))).await;
        assert!(first.is_pending());
        tokio::time::advance(SHUTDOWN_ACK_TIMEOUT).await;
        // Ready to complete only after the deadline.
        open_gate.send(()).expect("work still owns its gate");
        assert_eq!(drive.await, None);
    }
    assert_eq!(
        polls.load(Ordering::SeqCst),
        1,
        "never polled past the deadline"
    );
    assert_eq!(outcome(&owner, "threads"), Some(StageOutcome::TimedOut));
}

#[tokio::test(start_paused = true)]
async fn failed_processor_fallback_is_incomplete_and_bounded_by_the_original_latch() {
    let (latch, _custody) = OwnerLatch::new();
    // The first shutdown edge latched; a 30s graceful drain then ran before
    // the processor task failed.
    let edge = Instant::now();
    latch.latch();
    tokio::time::advance(Duration::from_secs(30)).await;

    let mut owner = latch.failed_processor_owner();
    assert_eq!(owner.deadline(), edge + SHUTDOWN_ACK_TIMEOUT);
    assert_eq!(outcome(&owner, "processor"), Some(StageOutcome::Incomplete));

    let outbound = Probe::default();
    let telemetry = Probe::default();
    let acceptors = Probe::default();
    let mut transport_stopped = false;
    let exit = finish_host_shutdown(
        &mut owner,
        stalled_join(&outbound),
        || transport_stopped = true,
        stalled_join(&telemetry),
        |_| true,
        acceptors.stalled().map(|_| Vec::new()),
    )
    .await;

    // Bounded by the original latch (5s left), not a fresh 35s budget.
    assert_eq!(Instant::now() - edge, SHUTDOWN_ACK_TIMEOUT);
    assert_eq!(exit, crate::AppServerExit::ShutdownIncomplete);
    assert!(transport_stopped);
    assert_eq!(outcome(&owner, "outbound"), Some(StageOutcome::TimedOut));
    assert_eq!(outcome(&owner, "telemetry"), Some(StageOutcome::TimedOut));
    assert_eq!(outcome(&owner, "acceptors"), Some(StageOutcome::TimedOut));
    // Reached after expiry: retained but never polled.
    assert_eq!(telemetry.polls(), 0);
    assert_eq!(acceptors.polls(), 0);
    assert!(!outbound.dropped());
}

#[tokio::test(start_paused = true)]
async fn failed_processor_without_an_earlier_edge_latches_once_and_is_incomplete() {
    let (latch, _custody) = OwnerLatch::new();
    let start = Instant::now();
    let mut owner = latch.failed_processor_owner();
    assert_eq!(owner.deadline(), start + SHUTDOWN_ACK_TIMEOUT);
    assert_eq!(latch.latch(), owner.deadline(), "no second budget");

    let exit = finish_host_shutdown(
        &mut owner,
        async { Ok(()) },
        || {},
        async { Ok(()) },
        |_| true,
        async { Vec::new() },
    )
    .await;

    // Every join finished, but nothing proves the processor stages did.
    assert_eq!(Instant::now(), start);
    assert_eq!(exit, crate::AppServerExit::ShutdownIncomplete);
}

#[tokio::test(start_paused = true)]
async fn clean_host_joins_stay_graceful() {
    let start = Instant::now();
    let mut owner =
        StandaloneShutdownOwner::new(start + SHUTDOWN_ACK_TIMEOUT, OwnerExpiry::never());
    let exit = finish_host_shutdown(
        &mut owner,
        async { Ok(()) },
        || {},
        async { Ok(()) },
        |_| true,
        async { vec![Ok(())] },
    )
    .await;
    assert_eq!(exit, crate::AppServerExit::Graceful);
}

#[tokio::test(start_paused = true)]
async fn panicked_blocking_thread_makes_the_exit_incomplete() {
    let expiry = OwnerExpiry::never();
    let start = Instant::now();
    let mut owner = StandaloneShutdownOwner::new(start + SHUTDOWN_ACK_TIMEOUT, expiry.clone());
    let handle = std::thread::spawn(|| panic!("stdout writer panicked without a note"));
    // Wait for the real thread end (no sleep).
    while !handle.is_finished() {
        std::thread::yield_now();
    }
    expiry.register_blocking("stdout_writer", handle);

    // Nothing left to observe: no wait until the owner deadline.
    owner.observe_blocking().await;
    assert_eq!(Instant::now(), start);
    assert_eq!(expiry.incomplete_blocking(), vec!["stdout_writer"]);
    assert_eq!(
        owner.exit_outcome(),
        crate::AppServerExit::ShutdownIncomplete
    );
}

#[tokio::test(start_paused = true)]
async fn dropping_the_shutdown_owner_releases_work_retained_through_its_handles() {
    let (latch, custody) = OwnerLatch::new();
    let deadline = latch.latch();
    let expiry = latch.expiry();
    let probe = Probe::default();
    // Like the stdio connection: the work captures handles to the very
    // custody that retains it.
    let work = {
        let handles = (expiry.clone(), latch.expiry());
        let stalled = probe.stalled();
        async move {
            let _handles = handles;
            stalled.await;
        }
    };
    assert_eq!(expiry.run("stdio_connection", work).await, Ok(None));
    assert_eq!(Instant::now(), deadline);
    let polls_at_expiry = probe.polls();

    let mut owner = StandaloneShutdownOwner::new(latch.latch(), latch.expiry());
    owner.adopt_custody(Some(custody));
    // The latch no longer owns anything; the shutdown owner does.
    drop(latch);
    tokio::time::advance(Duration::from_secs(600)).await;
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert_eq!(probe.polls(), polls_at_expiry, "retained, never polled");
    assert!(!probe.dropped(), "retained until the owner drops");
    assert_eq!(
        owner.exit_outcome(),
        crate::AppServerExit::ShutdownIncomplete
    );

    drop(owner);
    assert!(probe.dropped(), "released by the shutdown owner: no cycle");
    assert_eq!(probe.polls(), polls_at_expiry);
    // Handles that outlive the owner keep only reporting state.
    assert_eq!(expiry.returned_tasks(), vec!["stdio_connection"]);
}

/// A processor join that never completes; it captures the latch like the
/// real processor task and counts polls and drops.
struct StuckProcessor {
    _latch: std::sync::Arc<OwnerLatch>,
    probe: Probe,
}

impl Future for StuckProcessor {
    type Output = Result<FencedProcessorOutput, JoinError>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.probe.polls.fetch_add(1, Ordering::SeqCst);
        Poll::Pending
    }
}

impl Drop for StuckProcessor {
    fn drop(&mut self) {
        self.probe.dropped.store(true, Ordering::SeqCst);
    }
}

fn outcome_owner(join: ProcessorJoin) -> StandaloneShutdownOwner {
    match join {
        ProcessorJoin::Owner(owner) => owner,
        ProcessorJoin::Forced => panic!("expected a shutdown owner"),
    }
}

#[tokio::test(start_paused = true)]
async fn intake_edge_bounds_a_stuck_processor_join_from_the_intake_instant() {
    let (latch, custody) = OwnerLatch::new();
    let latch = std::sync::Arc::new(latch);
    install_intake_latch(&latch);
    let probe = Probe::default();
    let join = StuckProcessor {
        _latch: std::sync::Arc::clone(&latch),
        probe: probe.clone(),
    };
    // The server runs for a while; then stdin EOF (or a signal) arrives at
    // intake while the processor is stuck and never latches itself.
    tokio::time::advance(Duration::from_secs(7)).await;
    let intake = Instant::now();
    latch.expiry().mark_shutdown_edge();
    assert_eq!(
        *latch.subscribe().borrow(),
        Some(intake + SHUTDOWN_ACK_TIMEOUT),
        "the owner budget starts at intake"
    );

    let owner = outcome_owner(join_processor(&latch, Some(custody), join).await);

    // Bounded by the original deadline from intake: no fresh budget.
    assert_eq!(Instant::now(), intake + SHUTDOWN_ACK_TIMEOUT);
    assert_eq!(owner.deadline(), intake + SHUTDOWN_ACK_TIMEOUT);
    assert_eq!(outcome(&owner, "processor"), Some(StageOutcome::TimedOut));
    assert_eq!(
        owner.exit_outcome(),
        crate::AppServerExit::ShutdownIncomplete
    );
    let polls_at_deadline = probe.polls();
    tokio::time::advance(Duration::from_secs(600)).await;
    tokio::task::yield_now().await;
    assert_eq!(probe.polls(), polls_at_deadline, "retained, never polled");
    assert!(!probe.dropped(), "retained, not aborted");
    // A later processor-loop latch is a no-op: never extended.
    assert_eq!(latch.latch(), intake + SHUTDOWN_ACK_TIMEOUT);

    // No cycle through the latch: dropping the shutdown owner releases the
    // retained join exactly once and the custody it adopted.
    drop(owner);
    assert!(probe.dropped());
    assert!(latch.expiry().owner_released());
    assert_eq!(std::sync::Arc::strong_count(&latch), 1);
}

#[tokio::test(start_paused = true)]
async fn signal_intake_latches_the_first_edge_independently() {
    let (latch, _custody) = OwnerLatch::new();
    let latch = std::sync::Arc::new(latch);
    install_intake_latch(&latch);
    let (signal_tx, signal_rx) = tokio::sync::oneshot::channel::<()>();
    let intake_task = spawn_signal_intake(latch.expiry(), async move { signal_rx.await.is_ok() });
    tokio::time::advance(Duration::from_secs(4)).await;
    assert_eq!(*latch.subscribe().borrow(), None);

    let intake = Instant::now();
    signal_tx.send(()).expect("signal");
    intake_task.await.expect("intake task");
    assert_eq!(
        *latch.subscribe().borrow(),
        Some(intake + SHUTDOWN_ACK_TIMEOUT)
    );
}

#[tokio::test(start_paused = true)]
async fn panicked_processor_join_is_incomplete() {
    let (latch, custody) = OwnerLatch::new();
    let join = tokio::spawn(async { panic!("processor task panicked") });
    let owner = outcome_owner(join_processor(&latch, Some(custody), join).await);
    assert_eq!(outcome(&owner, "processor"), Some(StageOutcome::Incomplete));
    assert_eq!(
        owner.exit_outcome(),
        crate::AppServerExit::ShutdownIncomplete
    );
}

#[tokio::test(start_paused = true)]
async fn managed_forced_processor_join_returns_forced_and_releases_custody() {
    let (latch, custody) = OwnerLatch::new();
    let join = tokio::spawn(async { Ok(Some((crate::AppServerExit::Forced, None))) });
    assert!(matches!(
        join_processor(&latch, Some(custody), join).await,
        ProcessorJoin::Forced
    ));
    assert!(latch.expiry().owner_released());
}

#[tokio::test(start_paused = true)]
async fn close_all_with_a_full_control_queue_returns_at_the_owner_deadline() {
    let start = Instant::now();
    let mut owner =
        StandaloneShutdownOwner::new(start + SHUTDOWN_ACK_TIMEOUT, OwnerExpiry::never());
    // The router is retained at expiry: its control queue is full and its
    // receiver is never polled again.
    let (control, retained_router_rx) = tokio::sync::mpsc::channel::<u32>(1);
    control.try_send(1).expect("fill the control queue");

    submit_close_all(&mut owner, &control, 2).await;

    assert_eq!(Instant::now(), start + SHUTDOWN_ACK_TIMEOUT);
    assert_eq!(outcome(&owner, "close_all"), Some(StageOutcome::TimedOut));
    assert_eq!(
        owner.exit_outcome(),
        crate::AppServerExit::ShutdownIncomplete
    );
    tokio::time::advance(Duration::from_secs(600)).await;
    tokio::task::yield_now().await;
    assert_eq!(retained_router_rx.len(), 1, "no further send");
    // The pending send is retained by the owner and released exactly once.
    assert_eq!(control.strong_count(), 2);
    drop(owner);
    assert_eq!(control.strong_count(), 1);
}

#[tokio::test(start_paused = true)]
async fn shutdown_sends_wait_for_the_edge_then_expire_at_the_original_deadline() {
    let (latch, _custody) = OwnerLatch::new();
    let latch = std::sync::Arc::new(latch);
    install_intake_latch(&latch);
    let (control, _retained_rx) = tokio::sync::mpsc::channel::<u32>(1);
    control.try_send(1).expect("fill the control queue");
    let mut sends = ShutdownSends::default();
    let start = Instant::now();
    let submit = {
        let control = control.clone();
        async move { control.send(2).await.is_ok() }
    };
    let sent = {
        let mut submitting = std::pin::pin!(sends.submit(&latch, "disconnect_all", submit));
        // Before the edge there is no deadline yet: the send just waits.
        let first = std::future::poll_fn(|cx| Poll::Ready(submitting.as_mut().poll(cx))).await;
        assert!(first.is_pending());
        tokio::time::advance(Duration::from_secs(5)).await;
        latch.expiry().mark_shutdown_edge();
        submitting.await
    };
    let edge_at = Instant::now() - SHUTDOWN_ACK_TIMEOUT;
    assert_eq!(edge_at, start + Duration::from_secs(5));

    assert_eq!(sent, None);
    assert_eq!(
        latch.latch(),
        edge_at + SHUTDOWN_ACK_TIMEOUT,
        "bounded by the edge's deadline"
    );
    let mut owner = StandaloneShutdownOwner::new(latch.latch(), latch.expiry());
    sends.hand_to(&mut owner);
    assert_eq!(
        outcome(&owner, "disconnect_all"),
        Some(StageOutcome::TimedOut)
    );
    assert_eq!(
        owner.exit_outcome(),
        crate::AppServerExit::ShutdownIncomplete
    );
    assert_eq!(control.strong_count(), 2, "retained by the owner");
    drop(owner);
    assert_eq!(control.strong_count(), 1);
}

/// Counts every poll of the wrapped work (the processor's inline futures).
struct CountedPolls<F> {
    inner: Pin<Box<F>>,
    polls: Arc<AtomicUsize>,
}

impl<F: Future> Future for CountedPolls<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        self.inner.as_mut().poll(cx)
    }
}

struct DropCount(Arc<AtomicUsize>);

impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(start_paused = true)]
async fn fenced_processor_driver_is_never_polled_after_the_deadline() {
    let (latch, custody) = OwnerLatch::new();
    let latch = Arc::new(latch);
    install_intake_latch(&latch);
    let polls = Arc::new(AtomicUsize::new(0));
    let units = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    // A REAL Tokio-spawned processor body whose inline work handles one unit
    // per input (like a stalled init/config handler that keeps being woken).
    let (unit_tx, mut unit_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let body = CountedPolls {
        inner: Box::pin({
            let units = Arc::clone(&units);
            let probe = DropCount(Arc::clone(&drops));
            let latch = Arc::clone(&latch);
            async move {
                let _probe = probe;
                let _latch = latch;
                while unit_rx.recv().await.is_some() {
                    units.fetch_add(1, Ordering::SeqCst);
                }
                (crate::AppServerExit::Graceful, None)
            }
        }),
        polls: Arc::clone(&polls),
    };
    let join = spawn_fenced_processor(latch.expiry(), body);
    unit_tx.send(()).expect("unit");
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        units.load(Ordering::SeqCst),
        1,
        "the driver works before the edge"
    );

    // Intake (stdio EOF / signal) latches the edge; the body stays stuck.
    let intake = Instant::now();
    latch.expiry().mark_shutdown_edge();
    let owner = match join_processor(&latch, Some(custody), join).await {
        ProcessorJoin::Owner(owner) => owner,
        ProcessorJoin::Forced => panic!("expected a shutdown owner"),
    };
    assert_eq!(Instant::now(), intake + SHUTDOWN_ACK_TIMEOUT);
    assert_eq!(outcome(&owner, "processor"), Some(StageOutcome::TimedOut));
    assert_eq!(
        owner.exit_outcome(),
        crate::AppServerExit::ShutdownIncomplete
    );

    // More input arrives after the deadline: the body would be woken, but
    // the fence never polls it again and no unit runs.
    let polls_at_deadline = polls.load(Ordering::SeqCst);
    let units_at_deadline = units.load(Ordering::SeqCst);
    for _ in 0..3 {
        unit_tx
            .send(())
            .expect("the retained body still owns its receiver");
    }
    tokio::time::advance(Duration::from_secs(600)).await;
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        polls.load(Ordering::SeqCst),
        polls_at_deadline,
        "0 polls after the deadline"
    );
    assert_eq!(
        units.load(Ordering::SeqCst),
        units_at_deadline,
        "0 units after the deadline"
    );
    assert_eq!(drops.load(Ordering::SeqCst), 0, "retained, not aborted");

    // The retained driver is released exactly once with the owner; no
    // cycle through the latch it captured.
    drop(owner);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(
        unit_tx.send(()).is_err(),
        "the body (and its receiver) is gone"
    );
    assert_eq!(Arc::strong_count(&latch), 1);
}

#[tokio::test(start_paused = true)]
async fn unbound_runtime_makes_the_shutdown_incomplete() {
    let start = Instant::now();
    let mut owner =
        StandaloneShutdownOwner::new(start + SHUTDOWN_ACK_TIMEOUT, OwnerExpiry::never());
    let mut stages = FakeStages {
        connections: Some(Duration::ZERO),
        background: Some(Duration::ZERO),
        threads: Some(Duration::ZERO),
        bind_incomplete: true,
        ..Default::default()
    };
    drain_processor_stages(&mut owner, &mut stages, STANDALONE_STAGE_CAPS).await;
    assert_eq!(
        outcome(&owner, "bind_runtimes"),
        Some(StageOutcome::Incomplete)
    );
    assert_eq!(
        owner.exit_outcome(),
        crate::AppServerExit::ShutdownIncomplete
    );
}
