use super::*;
use pretty_assertions::assert_eq;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::Notify;

#[tokio::test(start_paused = true)]
async fn expired_cleanup_does_not_start_and_cannot_refresh_its_budget() {
    let (session, _) = super::super::tests::make_session_and_context().await;
    let session = Arc::new(session);
    let owner = session.cleanup_owner();
    let deadline = Instant::now();
    assert_eq!(owner.bind_deadline(deadline), Ok(deadline));
    assert_eq!(
        owner.observe(Arc::clone(&session)).await,
        CleanupExecution::TimedOut
    );
    assert!(!session.task_admission_closed.load(Ordering::Acquire));
    assert_eq!(
        owner.bind_deadline(deadline + Duration::from_secs(30)),
        Ok(deadline)
    );
    assert_eq!(
        owner.observe(Arc::clone(&session)).await,
        CleanupExecution::TimedOut
    );
    assert!(!session.task_admission_closed.load(Ordering::Acquire));
}

#[tokio::test(start_paused = true)]
#[expect(
    clippy::await_holding_invalid_type,
    reason = "the held admission lock keeps cleanup pending while its deadline is tightened"
)]
async fn tightening_deadline_wakes_an_existing_cleanup_observer() {
    let (session, _) = super::super::tests::make_session_and_context().await;
    let session = Arc::new(session);
    let owner = session.cleanup_owner();
    let started = Instant::now();
    owner
        .bind_deadline(started + Duration::from_secs(30))
        .expect("bind");
    let guard = session.active_turn.lock().await;
    let mut observer = Box::pin(owner.observe(Arc::clone(&session)));
    assert!(futures::poll!(observer.as_mut()).is_pending());
    owner
        .bind_deadline(started + Duration::from_secs(2))
        .expect("tighten");
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(observer.await, CleanupExecution::TimedOut);
    drop(guard);
    assert_eq!(
        owner.observe(Arc::clone(&session)).await,
        CleanupExecution::TimedOut
    );
    assert!(!session.task_admission_closed.load(Ordering::Acquire));
}

#[tokio::test]
async fn legacy_cleanup_cannot_be_relabelled_as_deadline_bound() {
    let (session, _) = super::super::tests::make_session_and_context().await;
    let session = Arc::new(session);
    let owner = session.cleanup_owner();
    assert_eq!(
        owner.observe(session).await,
        CleanupExecution::Finished {
            persistence_failed: false
        }
    );
    assert_eq!(
        owner.bind_deadline(Instant::now()),
        Err(DeadlineBindingError::LegacyCleanupStarted)
    );
}

#[tokio::test]
async fn suspension_cannot_relabel_an_ordinary_cleanup_receipt() {
    let (session, _) = super::super::tests::make_session_and_context().await;
    let session = Arc::new(session);
    let owner = session.cleanup_owner();
    let ordinary = owner.observe(Arc::clone(&session)).await;
    assert_eq!(
        ordinary,
        CleanupExecution::Finished {
            persistence_failed: false
        }
    );
    assert_eq!(
        owner.observe_suspension(session).await,
        CleanupExecution::AuthorityUnavailable
    );
    assert_eq!(owner.completed(), Some(ordinary));
}

#[tokio::test]
async fn ordinary_observer_replays_suspension_persistence_failure() {
    let (session, _) = super::super::tests::make_session_and_context().await;
    let session = Arc::new(session);
    // This fixture has no writer. Ordinary cleanup permits that, but suspension
    // must refuse it, and later observers must not replace that failed receipt.
    assert!(session.live_thread().is_none());
    let owner = session.cleanup_owner();
    let result = owner.observe_suspension(Arc::clone(&session)).await;
    assert_eq!(
        result,
        CleanupExecution::Finished {
            persistence_failed: true
        }
    );
    assert_eq!(owner.completed(), Some(result));
    assert_eq!(owner.observe(session).await, result);
}

#[tokio::test]
#[expect(
    clippy::await_holding_invalid_type,
    reason = "polling suspension cleanup while admission is locked proves cancellation retains the same receipt"
)]
async fn cancelled_suspension_observer_retains_its_cleanup_sequence() {
    let (session, _) = super::super::tests::make_session_and_context().await;
    let session = Arc::new(session);
    let owner = session.cleanup_owner();
    {
        let _guard = session.active_turn.lock().await;
        let mut observer = Box::pin(owner.observe_suspension(Arc::clone(&session)));
        assert!(futures::poll!(observer.as_mut()).is_pending());
        drop(observer);
        assert_eq!(owner.completed(), None);
    }
    // Resuming through the ordinary entry point must still use suspension's
    // strict writer requirement, not construct a second common cleanup.
    assert_eq!(
        owner.observe(session).await,
        CleanupExecution::Finished {
            persistence_failed: true
        }
    );
}

#[tokio::test]
async fn bound_suspension_common_waits_for_producer_join_before_persistence() {
    let (session, _) = super::super::tests::make_session_and_context().await;
    let session = Arc::new(session);
    let owner = session.cleanup_owner();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let task = tokio::task::spawn_blocking(move || {
        let _ = entered_tx.send(());
        let _ = release_rx.recv();
    });
    let _task = session.task_joins.register(task);
    entered_rx
        .await
        .expect("producer entered and cannot be aborted mid-write");
    owner
        .bind_deadline(Instant::now() + Duration::from_secs(20))
        .expect("bind");
    let mut observer = Box::pin(owner.observe_suspension(Arc::clone(&session)));
    assert!(futures::poll!(observer.as_mut()).is_pending());
    let common = owner
        .state
        .lock()
        .expect("state")
        .common_completion
        .clone()
        .expect("common");
    // Observe common alone: waiting only in the owner's parallel task driver
    // would allow this sequence to reach persistence while the producer lives.
    assert!(
        tokio::time::timeout(Duration::from_millis(100), common.clone())
            .await
            .is_err()
    );
    release_tx.send(()).expect("release producer");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), common)
            .await
            .expect("joined"),
        CleanupExecution::Finished {
            persistence_failed: true
        }
    );
    assert_eq!(
        observer.await,
        CleanupExecution::Finished {
            persistence_failed: true
        }
    );
}

#[tokio::test(start_paused = true)]
async fn deadline_bound_cleanup_replays_a_settled_result_after_expiry() {
    let (session, _) = super::super::tests::make_session_and_context().await;
    let session = Arc::new(session);
    let owner = session.cleanup_owner();
    let deadline = Instant::now() + Duration::from_secs(30);
    owner.bind_deadline(deadline).expect("bind");
    let result = owner.observe(Arc::clone(&session)).await;
    assert_eq!(
        result,
        CleanupExecution::Finished {
            persistence_failed: false
        }
    );
    tokio::time::advance(Duration::from_secs(31)).await;
    assert_eq!(owner.observe(session).await, result);
}

#[tokio::test]
async fn cleanup_panic_is_sticky_without_normalizing_the_loop_to_success() {
    struct PanickingStop;
    impl codex_extension_api::ThreadLifecycleContributor<crate::config::Config> for PanickingStop {
        fn on_thread_stop<'a>(
            &'a self,
            _input: codex_extension_api::ThreadStopInput<'a>,
        ) -> codex_extension_api::ExtensionFuture<'a, ()> {
            Box::pin(async { panic!("thread stop fixture") })
        }
    }
    let (mut session, _) = super::super::tests::make_session_and_context().await;
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::new();
    builder.thread_lifecycle_contributor(Arc::new(PanickingStop));
    session.services.extensions = Arc::new(builder.build());
    let session = Arc::new(session);
    let owner = session.cleanup_owner();
    let ordinary_session = Arc::clone(&session);
    let ordinary = tokio::spawn(async move {
        super::super::handlers::shutdown(&ordinary_session, "shutdown".to_string()).await
    });
    assert!(ordinary.await.expect_err("ordinary loop panics").is_panic());
    assert_eq!(owner.observe(session).await, CleanupExecution::Panicked);
}

#[tokio::test]
async fn cancelled_cleanup_observer_resumes_the_same_thread_stop() {
    struct PausedStop {
        calls: Arc<AtomicUsize>,
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }
    impl codex_extension_api::ThreadLifecycleContributor<crate::config::Config> for PausedStop {
        fn on_thread_stop<'a>(
            &'a self,
            _input: codex_extension_api::ThreadStopInput<'a>,
        ) -> codex_extension_api::ExtensionFuture<'a, ()> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.entered.notify_one();
                self.release.notified().await;
            })
        }
    }
    let (mut session, _) = super::super::tests::make_session_and_context().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::new();
    builder.thread_lifecycle_contributor(Arc::new(PausedStop {
        calls: Arc::clone(&calls),
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
    }));
    session.services.extensions = Arc::new(builder.build());
    let session = Arc::new(session);
    let weak_session = Arc::downgrade(&session);
    let owner = session.cleanup_owner();
    let weak_owner = Arc::downgrade(&owner);
    let first_owner = Arc::clone(&owner);
    let first_session = Arc::clone(&session);
    let first = tokio::spawn(async move { first_owner.observe(first_session).await });
    entered.notified().await;
    first.abort();
    assert!(first.await.expect_err("observer cancelled").is_cancelled());
    let mut second = Box::pin(owner.observe(Arc::clone(&session)));
    assert!(futures::poll!(second.as_mut()).is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    release.notify_one();
    assert_eq!(
        second.await,
        CleanupExecution::Finished {
            persistence_failed: false
        }
    );
    assert_eq!(
        owner.observe(Arc::clone(&session)).await,
        CleanupExecution::Finished {
            persistence_failed: false
        }
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    drop(session);
    drop(owner);
    assert!(weak_owner.upgrade().is_none());
    assert!(weak_session.upgrade().is_none());
}

#[tokio::test]
async fn cancelled_or_panicked_mcp_prewarm_never_projects_shutdown_complete() {
    for panics in [false, true] {
        let (session, _, events) = super::super::tests::make_session_and_context_with_rx().await;
        let worker = if panics {
            tokio::spawn(async { panic!("controlled MCP prewarm panic") })
        } else {
            let worker = tokio::spawn(std::future::pending::<()>());
            worker.abort();
            worker
        };
        *session.mcp_prewarm_task.lock().expect("worker owner") =
            Some(super::super::session_loop_termination_from_handle(worker));

        let owner = session.cleanup_owner();
        assert_eq!(
            owner.observe(Arc::clone(&session)).await,
            CleanupExecution::McpPrewarmFailed,
            "{} prewarm must become the distinct cleanup receipt",
            if panics { "panicked" } else { "cancelled" },
        );
        assert!(super::super::handlers::shutdown(&session, "shutdown".to_string()).await);
        while let Ok(event) = events.try_recv() {
            assert!(!matches!(
                event.msg,
                codex_protocol::protocol::EventMsg::ShutdownComplete
            ));
        }
    }
}

#[tokio::test(start_paused = true)]
#[expect(
    clippy::await_holding_invalid_type,
    reason = "blocked common cleanup is the discriminator for independent MCP retirement"
)]
async fn blocked_common_cleanup_does_not_starve_mcp_retirement() {
    let (session, _) = super::super::tests::make_session_and_context().await;
    let session = Arc::new(session);
    let owner = session.cleanup_owner();
    owner
        .bind_deadline(Instant::now() + Duration::from_secs(2))
        .expect("bind");
    let active_turn = session.active_turn.lock().await;
    let mut observer = Box::pin(owner.observe(Arc::clone(&session)));
    assert!(futures::poll!(observer.as_mut()).is_pending());
    let mcp = owner
        .state
        .lock()
        .expect("state")
        .mcp_completion
        .clone()
        .expect("MCP owner");
    match tokio::time::timeout(Duration::from_secs(1), mcp.clone())
        .await
        .expect("MCP settles independently")
    {
        McpCleanup::Observed(report) => assert!(report.are_recorded_workers_drained()),
        other => panic!("MCP proof must settle independently: {other:?}"),
    }
    assert!(matches!(
        session.mcp_refresh.acquire().now_or_never(),
        Some(Err(_))
    ));
    assert!(!session.task_admission_closed.load(Ordering::Acquire));
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(observer.await, CleanupExecution::TimedOut);
    assert!(
        matches!(mcp.peek(), Some(McpCleanup::Observed(report)) if report.are_recorded_workers_drained())
    );
    drop(active_turn);
}

#[tokio::test]
async fn bind_deadline_latches_session_sandbox_cleanup_scope() {
    let (session, _) = super::super::tests::make_session_and_context().await;
    let session = Arc::new(session);
    let scope = session.services.sandbox_cleanup_scope().clone();
    let deadline = Instant::now() + Duration::from_secs(5);
    let owner = session.cleanup_owner();
    assert_eq!(owner.bind_deadline(deadline), Ok(deadline));
    assert_eq!(scope.latched(), Some(deadline.into_std()));
    // A later binding never loosens the latched retirement deadline.
    let _ = owner.bind_deadline(deadline + Duration::from_secs(30));
    assert_eq!(scope.latched(), Some(deadline.into_std()));
}

#[test]
fn cleanup_receipt_is_qualified_only_when_otherwise_clean() {
    use crate::sandboxing::cleanup_census::SandboxCleanupCensus;
    let clean = CleanupExecution::Finished {
        persistence_failed: false,
    };
    let census = SandboxCleanupCensus::default();
    // No launches, or positively none: unchanged.
    census.record(/*may_have_launched*/ false, None);
    assert_eq!(clean.with_sandbox_cleanup_census(census.totals()), clean);
    // A may-have-launched operation without a receipt: unproven.
    census.record(true, None);
    assert_eq!(
        clean.with_sandbox_cleanup_census(census.totals()),
        CleanupExecution::SandboxCleanupUnproven { timed_out: false }
    );
    // Other outcomes keep their own failure.
    assert_eq!(
        CleanupExecution::McpFailed.with_sandbox_cleanup_census(census.totals()),
        CleanupExecution::McpFailed
    );
    assert_eq!(
        CleanupExecution::Finished {
            persistence_failed: true
        }
        .with_sandbox_cleanup_census(census.totals()),
        CleanupExecution::Finished {
            persistence_failed: true
        }
    );
}

#[tokio::test]
async fn unproven_sandbox_cleanup_withholds_shutdown_complete() {
    let (session, _, events) = super::super::tests::make_session_and_context_with_rx().await;
    let fixture = SandboxCleanupSummaryFixture::TIMED_OUT;
    session
        .services
        .sandbox_cleanup_census()
        .record(fixture.may, fixture.receipt);
    let owner = session.cleanup_owner();
    assert_eq!(
        owner.observe(Arc::clone(&session)).await,
        CleanupExecution::SandboxCleanupUnproven { timed_out: true }
    );
    assert!(!owner.recovery_ready());
    assert!(super::super::handlers::shutdown(&session, "shutdown".to_string()).await);
    while let Ok(event) = events.try_recv() {
        assert!(
            !matches!(
                event.msg,
                codex_protocol::protocol::EventMsg::ShutdownComplete
            ),
            "unproven sandbox cleanup must not project a clean shutdown"
        );
    }
}

#[tokio::test]
async fn no_launch_operations_keep_shutdown_clean() {
    let (session, _) = super::super::tests::make_session_and_context().await;
    let session = Arc::new(session);
    // Reads that positively launched no helper: nothing to clean.
    session
        .services
        .sandbox_cleanup_census()
        .record(/*may_have_launched*/ false, /*receipt*/ None);
    assert_eq!(
        session.cleanup_owner().observe(Arc::clone(&session)).await,
        CleanupExecution::Finished {
            persistence_failed: false
        }
    );
}

/// A Legacy cleanup already in flight when the shutdown edge arrives: its
/// live clamp stops the driver at the deadline (0 polls after it), the
/// partial work is retained (not aborted) and `TimedOut` is sticky.
#[tokio::test(start_paused = true)]
async fn in_flight_legacy_cleanup_is_clamped_at_the_edge_deadline() {
    let owner = SessionCleanupOwner::default();
    // Legacy cleanup has started: a completion exists, no deadline bound.
    owner.state.lock().unwrap().completion = Some(
        futures::future::pending::<CleanupExecution>()
            .boxed()
            .shared(),
    );
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let (open_gate, gate) = tokio::sync::oneshot::channel();
    let work = GatedLegacyWork {
        polls: Arc::clone(&polls),
        drops: Arc::clone(&drops),
        gate,
    }
    .boxed();
    let cleanup = clamp_legacy_cleanup(
        work,
        Arc::clone(&owner.legacy_clamp),
        Arc::downgrade(&owner.legacy_retained),
    )
    .shared();
    // The Legacy driver (e.g. an idle unload) is already driving it.
    let driver = tokio::spawn(cleanup.clone());
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        polls.load(Ordering::SeqCst),
        1,
        "Legacy cleanup is in flight"
    );

    let edge = Instant::now();
    // An ordinary binding (reset, retirement) refuses and does NOT clamp.
    assert_eq!(
        owner.bind_deadline(edge + Duration::from_secs(1)),
        Err(DeadlineBindingError::LegacyCleanupStarted)
    );
    // The host/manager shutdown edge arrives while it is in flight, through the
    // edge method: still refused, and now clamped live.
    assert_eq!(
        owner.bind_shutdown_edge_deadline(edge + Duration::from_secs(35)),
        Err(DeadlineBindingError::LegacyCleanupStarted)
    );
    assert_eq!(driver.await.expect("driver"), CleanupExecution::TimedOut);
    assert_eq!(Instant::now(), edge + Duration::from_secs(35));

    let polls_at_deadline = polls.load(Ordering::SeqCst);
    open_gate
        .send(())
        .expect("the retained work still owns its gate");
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
        drops.load(Ordering::SeqCst),
        0,
        "partial work retained, not aborted"
    );
    assert_eq!(cleanup.clone().await, CleanupExecution::TimedOut, "sticky");
    assert!(owner.legacy_retained.lock().unwrap().is_some());
    drop(owner);
    assert_eq!(drops.load(Ordering::SeqCst), 1, "released with its owner");
}

struct SandboxCleanupSummaryFixture {
    may: bool,
    receipt: Option<codex_protocol::exec_output::SandboxCleanupSummary>,
}

impl SandboxCleanupSummaryFixture {
    const TIMED_OUT: Self = Self {
        may: true,
        receipt: Some(codex_protocol::exec_output::SandboxCleanupSummary {
            state: codex_protocol::exec_output::SandboxCleanupState::TimedOut,
            reaped: 0,
            preserved_unproven: 0,
            host_owned: 0,
            handed_off: 0,
            timed_out: 1,
            joined: false,
        }),
    };
}

struct GatedLegacyWork {
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    gate: tokio::sync::oneshot::Receiver<()>,
}

impl std::future::Future for GatedLegacyWork {
    type Output = CleanupExecution;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<CleanupExecution> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        std::pin::Pin::new(&mut self.gate)
            .poll(cx)
            .map(|_| CleanupExecution::Finished {
                persistence_failed: false,
            })
    }
}

impl Drop for GatedLegacyWork {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn normal_loop_without_its_attached_cleanup_receipt_is_refused() {
    let clean = CleanupExecution::Finished {
        persistence_failed: false,
    };
    for receipt in [
        None,
        Some(clean),
        Some(CleanupExecution::McpFailed),
        Some(CleanupExecution::Finished {
            persistence_failed: true,
        }),
    ] {
        let (tx_sub, rx_sub) = async_channel::bounded(1);
        drop(rx_sub);
        let (_tx_event, rx_event) = async_channel::unbounded();
        let mut termination =
            super::super::session_loop_termination_from_handle(tokio::spawn(async {}));
        let owner = Arc::new(SessionCleanupOwner::default());
        termination.cleanup_owner = Some(Arc::clone(&owner));
        let io = super::super::SessionIo {
            tx_sub: tx_sub.into(),
            rx_event,
            agent_status: tokio::sync::watch::channel(super::super::AgentStatus::PendingInit).1,
            session_loop_termination: termination,
        };
        assert!(
            io.shutdown_and_wait().await.is_err(),
            "normal loop lacks cleanup evidence"
        );
        if let Some(receipt) = receipt {
            let original = futures::future::ready(receipt).boxed().shared();
            owner.state.lock().unwrap().completion = Some(original.clone());
            assert!(
                io.shutdown_and_wait().await.is_err(),
                "ready but unobserved is not recorded"
            );
            assert_eq!(original.await, receipt);
            assert_eq!(owner.completed(), Some(receipt));
            assert_eq!(io.shutdown_and_wait().await.is_ok(), receipt == clean);
            assert_eq!(
                owner.completed(),
                Some(receipt),
                "observation did not change the original receipt"
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn blocked_common_hook_does_not_starve_original_task_join() {
    struct PausedStop(Arc<Notify>);
    impl codex_extension_api::ThreadLifecycleContributor<crate::config::Config> for PausedStop {
        fn on_thread_stop<'a>(
            &'a self,
            _input: codex_extension_api::ThreadStopInput<'a>,
        ) -> codex_extension_api::ExtensionFuture<'a, ()> {
            Box::pin(async move {
                self.0.notify_one();
                std::future::pending().await
            })
        }
    }
    let (mut session, _) = super::super::tests::make_session_and_context().await;
    let entered = Arc::new(Notify::new());
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::new();
    builder.thread_lifecycle_contributor(Arc::new(PausedStop(Arc::clone(&entered))));
    session.services.extensions = Arc::new(builder.build());
    let task = session
        .task_joins
        .register(tokio::spawn(std::future::pending()));
    let session = Arc::new(session);
    let owner = session.cleanup_owner();
    owner
        .bind_deadline(Instant::now() + Duration::from_secs(2))
        .unwrap();
    let first_owner = Arc::clone(&owner);
    let first_session = Arc::clone(&session);
    let observer = tokio::spawn(async move { first_owner.observe(first_session).await });
    entered.notified().await;
    let joins = owner.state.lock().unwrap().task_completion.clone().unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), joins)
            .await
            .unwrap(),
        crate::tasks::TaskJoinOutcome::Complete { .. }
    ));
    assert!(
        task.wait().await,
        "original task cancellation was actually joined"
    );
    assert!(
        owner
            .state
            .lock()
            .unwrap()
            .common_completion
            .as_ref()
            .unwrap()
            .peek()
            .is_none()
    );
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(observer.await.unwrap(), CleanupExecution::TimedOut);
    assert!(!owner.recovery_ready());
}

#[derive(Clone, Copy)]
pub(crate) enum ThreadLoopFixture {
    Ordinary,
    Hung,
    Gone,
    Cancelled,
    Panicked,
}

pub(crate) async fn exact_thread_fixture(mode: ThreadLoopFixture) -> crate::CodexThread {
    exact_thread_fixture_with(mode, |_| {}).await
}

async fn exact_thread_fixture_with(
    mode: ThreadLoopFixture,
    configure: impl FnOnce(&mut super::super::session::Session),
) -> crate::CodexThread {
    let (session, turn, rx_event) = super::super::tests::make_session_and_context_with_rx().await;
    let mut session =
        Arc::try_unwrap(session).unwrap_or_else(|_| panic!("fixture session is unique"));
    configure(&mut session);
    let session = Arc::new(session);
    let owner = session.cleanup_owner();
    let loop_cleanup_owner = Arc::clone(&owner);
    let (tx_sub, rx_sub) = async_channel::bounded(1);
    let tx_sub = super::super::SubmissionSender::from(tx_sub);
    let submissions = tx_sub.dispatch_control();
    let loop_session = Arc::clone(&session);
    let config = Arc::clone(&turn.config);
    let task = tokio::spawn(Box::pin(async move {
        let _owner = loop_cleanup_owner;
        match mode {
            ThreadLoopFixture::Ordinary => {
                Box::pin(super::super::handlers::submission_loop_with_dispatch(
                    loop_session,
                    config,
                    rx_sub,
                    Some(submissions),
                ))
                .await;
            }
            ThreadLoopFixture::Hung | ThreadLoopFixture::Cancelled => {
                let _rx_sub = rx_sub;
                std::future::pending::<()>().await;
            }
            ThreadLoopFixture::Gone => drop(rx_sub),
            ThreadLoopFixture::Panicked => panic!("loop panic fixture"),
        }
    }));
    if matches!(mode, ThreadLoopFixture::Cancelled) {
        task.abort();
    }
    let mut termination = super::super::session_loop_termination_from_handle(task);
    termination.cleanup_owner = Some(owner);
    let io = super::super::SessionIo {
        tx_sub,
        rx_event,
        agent_status: tokio::sync::watch::channel(crate::agent::AgentStatus::PendingInit).1,
        session_loop_termination: termination,
    };
    let configured = codex_protocol::protocol::SessionConfiguredEvent {
        session_id: session.session_id(),
        thread_id: session.thread_id,
        forked_from_id: None,
        parent_thread_id: None,
        thread_source: None,
        thread_name: None,
        model: "fixture".to_string(),
        model_provider_id: "fixture".to_string(),
        service_tier: None,
        approval_policy: codex_protocol::protocol::AskForApproval::Never,
        approvals_reviewer: codex_protocol::config_types::ApprovalsReviewer::User,
        permission_profile: codex_protocol::models::PermissionProfile::Disabled,
        active_permission_profile: None,
        cwd: turn.config.cwd.clone(),
        reasoning_effort: None,
        network_proxy: None,
        rollout_path: None,
    };
    crate::CodexThread::new(
        session,
        io,
        crate::thread_startup_metadata::ThreadStartupMetadata::from(&configured),
        None,
        codex_protocol::protocol::SessionSource::Exec,
    )
}

fn settled_owner(thread: &crate::CodexThread) -> Arc<SessionCleanupOwner> {
    thread
        .io
        .session_loop_termination
        .cleanup_owner
        .as_ref()
        .expect("production loop retains cleanup owner")
        .clone()
}

struct CountingCleanup {
    polls: Arc<AtomicUsize>,
    receipt: CleanupExecution,
}

impl std::future::Future for CountingCleanup {
    type Output = CleanupExecution;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<CleanupExecution> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        std::task::Poll::Ready(self.receipt)
    }
}

#[tokio::test(start_paused = true)]
async fn exact_thread_fallback_resumes_the_normal_handlers_cleanup_once() {
    struct PausedStop {
        calls: Arc<AtomicUsize>,
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }
    impl codex_extension_api::ThreadLifecycleContributor<crate::config::Config> for PausedStop {
        fn on_thread_stop<'a>(
            &'a self,
            _input: codex_extension_api::ThreadStopInput<'a>,
        ) -> codex_extension_api::ExtensionFuture<'a, ()> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.entered.notify_one();
                self.release.notified().await;
            })
        }
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let contributor = Arc::new(PausedStop {
        calls: Arc::clone(&calls),
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
    });
    let thread = exact_thread_fixture_with(ThreadLoopFixture::Ordinary, |session| {
        let mut builder = codex_extension_api::ExtensionRegistryBuilder::new();
        builder.thread_lifecycle_contributor(contributor);
        session.services.extensions = Arc::new(builder.build());
    })
    .await;
    let ticket = thread
        .begin_retirement(Instant::now() + Duration::from_secs(20))
        .expect("ticket");
    let observer = ticket.clone();
    let mut waiting = tokio::spawn(async move { observer.wait().await });
    entered.notified().await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_secs(10)).await;
    assert_eq!(
        thread.io.session_loop_termination.clone().await,
        super::super::SessionLoopOutcome::Cancelled
    );
    assert!(
        futures::poll!(&mut waiting).is_pending(),
        "loop abort alone is not cleanup proof"
    );
    release.notify_one();
    let report = waiting.await.expect("retirement observer");
    assert_eq!(
        report,
        crate::ThreadRetirementReport {
            ordinary: crate::ThreadShutdownOutcome::TimedOut,
            session_loop: crate::ThreadLoopOutcome::Cancelled,
            cleanup: crate::ThreadCleanupOutcome::Finished {
                persistence_failed: false
            },
        }
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "fallback must resume, not restart, cleanup"
    );
    assert_eq!(ticket.wait().await, report);
    while let Ok(event) = thread.io.rx_event.try_recv() {
        assert!(!matches!(
            event.msg,
            codex_protocol::protocol::EventMsg::ShutdownComplete
        ));
    }
}

#[tokio::test]
async fn exact_thread_ordinary_retirement_emits_once_and_replays_original_ticket() {
    let thread = exact_thread_fixture(ThreadLoopFixture::Ordinary).await;
    let deadline = Instant::now() + Duration::from_secs(3);
    let first = thread.begin_retirement(deadline).expect("ticket");
    let repeat = thread
        .begin_retirement(deadline + Duration::from_secs(30))
        .expect("same ticket");
    assert_eq!(repeat.deadline(), deadline);
    let (first, repeat) = tokio::join!(first.wait(), repeat.wait());
    assert_eq!(first, repeat);
    assert_eq!(
        first,
        crate::ThreadRetirementReport {
            ordinary: crate::ThreadShutdownOutcome::Complete,
            session_loop: crate::ThreadLoopOutcome::Normal,
            cleanup: crate::ThreadCleanupOutcome::Finished {
                persistence_failed: false
            },
        }
    );
    let mut complete = 0;
    while let Ok(event) = thread.io.rx_event.try_recv() {
        if matches!(
            event.msg,
            codex_protocol::protocol::EventMsg::ShutdownComplete
        ) {
            complete += 1;
        }
    }
    assert_eq!(complete, 1);
}

#[tokio::test]
async fn exact_thread_idle_commit_refuses_busy_or_active_admission_without_effects() {
    let thread = exact_thread_fixture(ThreadLoopFixture::Ordinary).await;
    assert!(matches!(
        thread.try_begin_idle_retirement(Instant::now()),
        Err(crate::ThreadRetirementError::DeadlineExpired)
    ));
    assert!(!thread.session.task_admission_closed.load(Ordering::Acquire));
    let deadline = Instant::now() + Duration::from_secs(3);
    {
        let mut active = thread.session.active_turn.lock().await;
        assert!(matches!(
            thread.try_begin_idle_retirement(deadline),
            Err(crate::ThreadRetirementError::TaskAdmissionBusy)
        ));
        assert!(!thread.session.task_admission_closed.load(Ordering::Acquire));
        *active = Some(crate::state::ActiveTurn::default());
    }
    assert!(matches!(
        thread.try_begin_idle_retirement(deadline),
        Err(crate::ThreadRetirementError::ActiveWork)
    ));
    assert!(!thread.session.task_admission_closed.load(Ordering::Acquire));
    thread.session.active_turn.lock().await.take();
    let ticket = thread
        .try_begin_idle_retirement(deadline)
        .expect("idle commit");
    assert!(thread.session.task_admission_closed.load(Ordering::Acquire));
    let calls = Arc::new(AtomicUsize::new(0));
    let late = Arc::clone(&calls);
    thread
        .session
        .spawn_startup_auxiliary(async move {
            late.fetch_add(1, Ordering::SeqCst);
        })
        .await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        ticket.wait().await.cleanup,
        crate::ThreadCleanupOutcome::Finished {
            persistence_failed: false
        }
    );
}

#[tokio::test]
async fn exact_thread_gone_loop_preserves_legacy_classification_without_synthetic_event() {
    for (mode, ordinary, session_loop) in [
        (
            ThreadLoopFixture::Gone,
            crate::ThreadShutdownOutcome::Complete,
            crate::ThreadLoopOutcome::Normal,
        ),
        (
            ThreadLoopFixture::Cancelled,
            crate::ThreadShutdownOutcome::SubmitFailed,
            crate::ThreadLoopOutcome::Cancelled,
        ),
        (
            ThreadLoopFixture::Panicked,
            crate::ThreadShutdownOutcome::SubmitFailed,
            crate::ThreadLoopOutcome::Panicked,
        ),
    ] {
        let thread = exact_thread_fixture(mode).await;
        thread.io.session_loop_termination.clone().await;
        let legacy = thread.io.shutdown_and_wait().await;
        assert!(
            legacy.is_err(),
            "a bare normal join is not a recorded cleanup receipt"
        );
        let ticket = thread
            .begin_retirement(Instant::now() + Duration::from_secs(3))
            .expect("ticket");
        assert_eq!(
            ticket.wait().await,
            crate::ThreadRetirementReport {
                ordinary,
                session_loop,
                cleanup: crate::ThreadCleanupOutcome::Finished {
                    persistence_failed: false
                },
            }
        );
        assert_eq!(
            thread.io.shutdown_and_wait().await.is_ok(),
            ordinary == crate::ThreadShutdownOutcome::Complete,
            "normal join requires the now-recorded original cleanup receipt",
        );
        while let Ok(event) = thread.io.rx_event.try_recv() {
            assert!(!matches!(
                event.msg,
                codex_protocol::protocol::EventMsg::ShutdownComplete
            ));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn exact_thread_cancelled_observer_resumes_timeout_and_abort_without_resubmit() {
    let thread = exact_thread_fixture(ThreadLoopFixture::Hung).await;
    let start = Instant::now();
    let ticket = thread
        .begin_retirement(start + Duration::from_secs(20))
        .expect("ticket");
    let mut observer = Box::pin(ticket.wait());
    assert!(futures::poll!(observer.as_mut()).is_pending());
    assert_eq!(thread.io.tx_sub.len(), 1, "one shutdown was enqueued");
    drop(observer);
    let repeated = thread
        .begin_retirement(start + Duration::from_secs(60))
        .expect("same ticket");
    let report = repeated.wait().await;
    assert_eq!(
        report,
        crate::ThreadRetirementReport {
            ordinary: crate::ThreadShutdownOutcome::TimedOut,
            session_loop: crate::ThreadLoopOutcome::Cancelled,
            cleanup: crate::ThreadCleanupOutcome::Finished {
                persistence_failed: false
            },
        }
    );
    assert_eq!(ticket.wait().await, report);
    assert_eq!(Instant::now() - start, Duration::from_secs(10));
    while let Ok(event) = thread.io.rx_event.try_recv() {
        assert!(!matches!(
            event.msg,
            codex_protocol::protocol::EventMsg::ShutdownComplete
        ));
    }
}

#[tokio::test(start_paused = true)]
async fn exact_thread_full_submission_queue_freezes_births_before_waiting() {
    let thread = exact_thread_fixture(ThreadLoopFixture::Hung).await;
    thread
        .io
        .submit(codex_protocol::protocol::Op::RunUserShellCommand {
            command: "queued work must not start".to_string(),
            timeout_ms: None,
        })
        .await
        .expect("fill queue");
    let ticket = thread
        .begin_retirement(Instant::now() + Duration::from_secs(20))
        .expect("ticket");
    let mut wait = Box::pin(ticket.wait());
    assert!(futures::poll!(wait.as_mut()).is_pending());
    let births = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&births);
    thread
        .session
        .spawn_startup_auxiliary(async move {
            count.fetch_add(1, Ordering::SeqCst);
        })
        .await;
    tokio::task::yield_now().await;
    assert_eq!(births.load(Ordering::SeqCst), 0);
    assert_eq!(
        thread.io.tx_sub.len(),
        1,
        "shutdown enqueue is actually blocked"
    );
    assert_eq!(
        wait.await,
        crate::ThreadRetirementReport {
            ordinary: crate::ThreadShutdownOutcome::TimedOut,
            session_loop: crate::ThreadLoopOutcome::Cancelled,
            cleanup: crate::ThreadCleanupOutcome::Finished {
                persistence_failed: false
            },
        }
    );
}

#[tokio::test(start_paused = true)]
#[expect(
    clippy::await_holding_invalid_type,
    reason = "the test keeps admission locked until the shutdown observer deadline expires"
)]
async fn exact_thread_expired_observer_does_not_resume_shutdown_enqueue() {
    let thread = exact_thread_fixture(ThreadLoopFixture::Hung).await;
    let active = thread.session.active_turn.lock().await;
    let ticket = thread
        .begin_retirement(Instant::now() + Duration::from_secs(2))
        .expect("ticket");
    let mut wait = Box::pin(ticket.wait());
    assert!(futures::poll!(wait.as_mut()).is_pending());
    drop(wait);
    tokio::time::advance(Duration::from_secs(3)).await;
    drop(active);
    assert_eq!(
        ticket.wait().await,
        crate::ThreadRetirementReport {
            ordinary: crate::ThreadShutdownOutcome::TimedOut,
            session_loop: crate::ThreadLoopOutcome::TimedOut,
            cleanup: crate::ThreadCleanupOutcome::TimedOut,
        }
    );
    assert_eq!(
        thread.io.tx_sub.len(),
        0,
        "expired observation must not enqueue"
    );
    // Fixture cleanup, deliberately after the refusal/no-enqueue assertions.
    thread.io.session_loop_termination.request_abort();
    thread.io.session_loop_termination.clone().await;
}

#[tokio::test]
async fn normal_loop_join_with_failed_cleanup_receipt_is_not_ok() {
    let thread = exact_thread_fixture(ThreadLoopFixture::Gone).await;
    let owner = thread
        .io
        .session_loop_termination
        .cleanup_owner
        .as_ref()
        .expect("production loop retains cleanup owner")
        .clone();
    owner.state.lock().expect("owner").completion = Some(
        futures::future::ready(CleanupExecution::ConversationShutdownFailed)
            .boxed()
            .shared(),
    );
    owner.observe(Arc::clone(&thread.session)).await;
    assert_eq!(
        thread.io.session_loop_termination.cleanup_completed(),
        Some(CleanupExecution::ConversationShutdownFailed)
    );

    assert!(thread.io.shutdown_and_wait().await.is_err());
}

#[tokio::test]
async fn resource_release_requires_normal_join_and_clean_receipt() {
    let clean = CleanupExecution::Finished {
        persistence_failed: false,
    };
    let mut cases = vec![
        (ThreadLoopFixture::Gone, Some(clean), true),
        (
            ThreadLoopFixture::Gone,
            Some(CleanupExecution::Finished {
                persistence_failed: true,
            }),
            false,
        ),
        (ThreadLoopFixture::Cancelled, Some(clean), false),
        (ThreadLoopFixture::Panicked, Some(clean), false),
        // Abandoned or never-started cleanup has no recorded receipt.
        (ThreadLoopFixture::Gone, None, false),
    ];
    for failure in [
        CleanupExecution::Panicked,
        CleanupExecution::TimedOut,
        CleanupExecution::AuthorityUnavailable,
        CleanupExecution::McpFailed,
        CleanupExecution::McpPrewarmFailed,
        CleanupExecution::TaskJoinFailed,
        CleanupExecution::ConversationShutdownFailed,
        CleanupExecution::CodeModeShutdownFailed,
    ] {
        cases.push((ThreadLoopFixture::Gone, Some(failure), false));
    }
    for (case, (mode, receipt, expected)) in cases.into_iter().enumerate() {
        let thread = exact_thread_fixture(mode).await;
        let owner = settled_owner(&thread);
        if let Some(receipt) = receipt {
            // Retained-receipt injection, as in the failed-receipt tests above.
            owner.state.lock().expect("owner").completion =
                Some(futures::future::ready(receipt).boxed().shared());
            assert_eq!(owner.observe(Arc::clone(&thread.session)).await, receipt);
        }
        let released =
            tokio::time::timeout(Duration::from_secs(10), thread.observe_resource_release())
                .await
                .expect("fixture loop joins");
        assert_eq!(released, expected, "case {case}: {receipt:?}");
        assert_eq!(
            thread.io.session_loop_termination.cleanup_completed(),
            receipt,
            "observation must not create or change a receipt"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn resource_release_observation_never_drives_or_rebinds_abandoned_cleanup() {
    struct PausedStop {
        calls: Arc<AtomicUsize>,
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }
    impl codex_extension_api::ThreadLifecycleContributor<crate::config::Config> for PausedStop {
        fn on_thread_stop<'a>(
            &'a self,
            _input: codex_extension_api::ThreadStopInput<'a>,
        ) -> codex_extension_api::ExtensionFuture<'a, ()> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.entered.notify_one();
                self.release.notified().await;
            })
        }
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let contributor = Arc::new(PausedStop {
        calls: Arc::clone(&calls),
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
    });
    let thread = exact_thread_fixture_with(ThreadLoopFixture::Ordinary, |session| {
        let mut builder = codex_extension_api::ExtensionRegistryBuilder::new();
        builder.thread_lifecycle_contributor(contributor);
        session.services.extensions = Arc::new(builder.build());
    })
    .await;
    let deadline = Instant::now() + Duration::from_secs(5);
    let ticket = thread.begin_retirement(deadline).expect("ticket");
    let observer = ticket.clone();
    let waiting = tokio::spawn(async move { observer.wait().await });
    tokio::time::timeout(Duration::from_secs(4), entered.notified())
        .await
        .expect("cleanup reached the stop hook before the deadline");
    let owner = settled_owner(&thread);
    let common = owner
        .state
        .lock()
        .expect("cleanup state")
        .common_completion
        .clone()
        .expect("common cleanup receipt");

    let mut observation = Box::pin(thread.observe_resource_release());
    assert!(futures::poll!(observation.as_mut()).is_pending());
    assert!(owner.completed().is_none());
    assert!(common.peek().is_none());
    assert_eq!(*owner.deadline_changed.borrow(), Some(deadline));
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    tokio::time::advance(Duration::from_secs(5)).await;
    let report = waiting.await.expect("retirement observer");
    assert_eq!(report.ordinary, crate::ThreadShutdownOutcome::TimedOut);
    assert!(!report.is_complete());
    // The loop's own observer abandons cleanup at the deadline; the loop
    // joins without a receipt, so no release is observed.
    assert!(
        !tokio::time::timeout(Duration::from_secs(60), observation)
            .await
            .expect("abandoned loop joins")
    );

    release.notify_one();
    tokio::task::yield_now().await;
    assert!(
        owner.completed().is_none(),
        "observation never resumed cleanup"
    );
    assert!(common.peek().is_none());
    assert_eq!(*owner.deadline_changed.borrow(), Some(deadline));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(ticket.wait().await, report);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_resource_release_keeps_the_timed_out_retirement_report() {
    // A synchronous block inside the loop's own cleanup poll cannot be
    // pre-empted by the deadline (the upstream slow writer-lock shape).
    struct BlockingStop {
        entered: Arc<Notify>,
        release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    }
    impl codex_extension_api::ThreadLifecycleContributor<crate::config::Config> for BlockingStop {
        fn on_thread_stop<'a>(
            &'a self,
            _input: codex_extension_api::ThreadStopInput<'a>,
        ) -> codex_extension_api::ExtensionFuture<'a, ()> {
            Box::pin(async move {
                self.entered.notify_one();
                let _ = self.release.lock().expect("release gate").recv();
            })
        }
    }
    let entered = Arc::new(Notify::new());
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let contributor = Arc::new(BlockingStop {
        entered: Arc::clone(&entered),
        release: std::sync::Mutex::new(release_rx),
    });
    let thread = exact_thread_fixture_with(ThreadLoopFixture::Ordinary, |session| {
        let mut builder = codex_extension_api::ExtensionRegistryBuilder::new();
        builder.thread_lifecycle_contributor(contributor);
        session.services.extensions = Arc::new(builder.build());
    })
    .await;
    // Isolate a late common receipt: parallel MCP/task evidence must already
    // be settled, rather than assuming that starting a drain observed it.
    // These are real original owners, not injected success labels.
    let prepared = thread
        .session
        .services
        .mcp_runtime
        .retirement_owner()
        .drain_workers_until(Instant::now() + Duration::from_secs(10))
        .await;
    assert!(prepared.are_recorded_workers_drained());
    assert!(matches!(
        thread
            .session
            .task_joins
            .shutdown_until(Instant::now() + Duration::from_secs(10))
            .await,
        crate::tasks::TaskJoinOutcome::Complete { panicked: false }
    ));
    let deadline = Instant::now() + Duration::from_secs(2);
    let ticket = thread.begin_retirement(deadline).expect("ticket");
    let observer = ticket.clone();
    let waiting = tokio::spawn(async move { observer.wait().await });
    tokio::time::timeout(Duration::from_secs(10), entered.notified())
        .await
        .expect("loop cleanup blocked in the stop hook");
    let report = tokio::time::timeout(Duration::from_secs(10), waiting)
        .await
        .expect("deadline reached while cleanup is blocked")
        .expect("retirement observer");
    assert_eq!(report.ordinary, crate::ThreadShutdownOutcome::TimedOut);
    assert!(!report.is_complete());
    let owner = settled_owner(&thread);
    assert!(owner.completed().is_none());
    let mut observation = Box::pin(thread.observe_resource_release());
    assert!(futures::poll!(observation.as_mut()).is_pending());

    release_tx.send(()).expect("release blocked cleanup");
    assert!(
        tokio::time::timeout(Duration::from_secs(10), observation)
            .await
            .expect("loop joins after release"),
        "late clean release is positively observed"
    );
    assert_eq!(
        thread.io.session_loop_termination.cleanup_completed(),
        Some(CleanupExecution::Finished {
            persistence_failed: false
        })
    );
    // The separate observation never rewrites the original report.
    assert_eq!(ticket.wait().await, report);
    let repeat = thread
        .begin_retirement(Instant::now() + Duration::from_secs(60))
        .expect("same ticket");
    assert_eq!(repeat.deadline(), deadline);
    assert_eq!(repeat.wait().await, report);
    assert!(!repeat.wait().await.is_complete());
}

#[tokio::test(start_paused = true)]
async fn hung_loop_retains_ticket_and_owner_without_release_observation() {
    let thread = exact_thread_fixture(ThreadLoopFixture::Hung).await;
    let deadline = Instant::now() + Duration::from_secs(5);
    let ticket = thread.begin_retirement(deadline).expect("ticket");
    let report = ticket.wait().await;
    assert_eq!(report.ordinary, crate::ThreadShutdownOutcome::TimedOut);
    assert!(!report.is_complete());
    let owner = settled_owner(&thread);
    assert!(
        tokio::time::timeout(Duration::from_secs(60), thread.observe_resource_release())
            .await
            .is_err(),
        "no release is observed while the exact loop hangs"
    );
    assert!(thread.io.session_loop_termination.observed().is_none());
    assert!(thread.observed_terminal_cleanup().is_none());
    assert!(owner.completed().is_none());
    let repeat = thread
        .begin_retirement(Instant::now() + Duration::from_secs(60))
        .expect("retained ticket");
    assert_eq!(repeat.deadline(), deadline);
    assert_eq!(repeat.wait().await, report);
}

#[tokio::test]
async fn retirement_report_keeps_ownership_when_sandbox_cleanup_is_unproven() {
    use crate::ThreadCleanupOutcome;
    use codex_protocol::exec_output::SandboxCleanupState;
    use codex_protocol::exec_output::SandboxCleanupSummary;
    let clean = CleanupExecution::Finished {
        persistence_failed: false,
    };
    let (session, _) = super::super::tests::make_session_and_context().await;
    let census = session.services.sandbox_cleanup_census();
    // Nothing recorded: a clean receipt stays clean.
    assert_eq!(
        ThreadCleanupOutcome::with_sandbox_cleanup_census(clean, census.totals()),
        ThreadCleanupOutcome::Finished {
            persistence_failed: false
        }
    );

    // A filesystem helper that may have launched but left no receipt.
    census.record(/*may_have_launched*/ true, /*receipt*/ None);
    assert_eq!(
        ThreadCleanupOutcome::with_sandbox_cleanup_census(clean, census.totals()),
        ThreadCleanupOutcome::SandboxCleanupUnproven { timed_out: false }
    );

    // A helper whose cleanup passed its deadline: reported as timed out.
    census.record(
        true,
        Some(SandboxCleanupSummary {
            state: SandboxCleanupState::TimedOut,
            reaped: 0,
            preserved_unproven: 0,
            host_owned: 0,
            handed_off: 0,
            timed_out: 1,
            joined: false,
        }),
    );
    let report = crate::ThreadRetirementReport {
        ordinary: crate::ThreadShutdownOutcome::Complete,
        session_loop: crate::ThreadLoopOutcome::Normal,
        cleanup: ThreadCleanupOutcome::with_sandbox_cleanup_census(clean, census.totals()),
    };
    assert_eq!(
        report.cleanup,
        ThreadCleanupOutcome::SandboxCleanupUnproven { timed_out: true }
    );
    assert!(!report.is_complete(), "retirement must not claim clean");

    // Other cleanup failures are never relabelled by the census.
    assert_eq!(
        ThreadCleanupOutcome::with_sandbox_cleanup_census(
            CleanupExecution::McpFailed,
            census.totals()
        ),
        ThreadCleanupOutcome::McpFailed
    );
}

/// The app-server hosted-endpoint witness reads cleanup only through
/// `observe_resource_release`. Once a receipt is recorded (clean, or the
/// McpFailed of a DELETE to a dead endpoint), repeated observation must bind
/// no deadline and never poll the cleanup completion again.
#[tokio::test]
async fn resource_release_observation_of_recorded_receipts_binds_no_deadline_and_never_polls() {
    let clean = CleanupExecution::Finished {
        persistence_failed: false,
    };
    for receipt in [clean, CleanupExecution::McpFailed] {
        let thread = exact_thread_fixture(ThreadLoopFixture::Gone).await;
        let owner = settled_owner(&thread);
        let polls = Arc::new(AtomicUsize::new(0));
        owner.state.lock().expect("owner").completion = Some(
            CountingCleanup {
                polls: Arc::clone(&polls),
                receipt,
            }
            .boxed()
            .shared(),
        );
        // The recording side settles the receipt exactly once.
        assert_eq!(owner.observe(Arc::clone(&thread.session)).await, receipt);
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        let published_deadline = *owner.deadline_changed.borrow();
        let state_deadline = owner.state.lock().expect("owner").deadline;
        assert_eq!((published_deadline, state_deadline), (None, None));

        for read in 0..3 {
            let released =
                tokio::time::timeout(Duration::from_secs(10), thread.observe_resource_release())
                    .await
                    .expect("fixture loop joins");
            assert_eq!(released, receipt == clean, "read {read}: {receipt:?}");
        }

        assert_eq!(
            polls.load(Ordering::SeqCst),
            1,
            "observation never polls the cleanup completion"
        );
        assert_eq!(*owner.deadline_changed.borrow(), published_deadline);
        assert_eq!(owner.state.lock().expect("owner").deadline, state_deadline);
        assert_eq!(
            thread.io.session_loop_termination.cleanup_completed(),
            Some(receipt),
            "observation never changes the recorded receipt"
        );
    }
}

#[test]
fn a_live_sandbox_launch_withholds_clean_retirement_before_its_guard_drops() {
    let census = Arc::new(SandboxCleanupCensus::default());
    let operation =
        crate::sandboxing::cleanup_census::SandboxCleanupOperation::new(Arc::clone(&census), true);
    let clean = CleanupExecution::Finished {
        persistence_failed: false,
    };
    assert_eq!(
        clean.with_sandbox_cleanup_census(census.totals()),
        CleanupExecution::SandboxCleanupUnproven { timed_out: false }
    );
    operation.finish(codex_file_system::SandboxLaunch::NotLaunched, None);
    assert_eq!(clean.with_sandbox_cleanup_census(census.totals()), clean);
}
