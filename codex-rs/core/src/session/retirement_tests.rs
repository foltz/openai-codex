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
    let (tx_sub, rx_sub) = async_channel::bounded(1);
    drop(rx_sub);
    let (_tx_event, rx_event) = async_channel::unbounded();
    let mut termination =
        super::super::session_loop_termination_from_handle(tokio::spawn(async {}));
    let owner = Arc::new(SessionCleanupOwner::default());
    termination.cleanup_owner = Some(Arc::clone(&owner));
    let io = super::super::SessionIo {
        tx_sub,
        rx_event,
        agent_status: tokio::sync::watch::channel(super::super::AgentStatus::PendingInit).1,
        session_loop_termination: termination,
    };
    assert!(
        io.shutdown_and_wait().await.is_err(),
        "normal loop lacks cleanup evidence"
    );
    owner.state.lock().unwrap().completion = Some(
        futures::future::ready(CleanupExecution::Finished {
            persistence_failed: false,
        })
        .boxed()
        .shared(),
    );
    assert!(
        io.shutdown_and_wait().await.is_ok(),
        "recorded original clean receipt"
    );
    owner.state.lock().unwrap().completion = Some(
        futures::future::ready(CleanupExecution::McpFailed)
            .boxed()
            .shared(),
    );
    assert!(
        io.shutdown_and_wait().await.is_err(),
        "normal loop cannot erase failed cleanup"
    );
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
