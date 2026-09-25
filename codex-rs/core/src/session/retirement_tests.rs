use super::*;
use pretty_assertions::assert_eq;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::Notify;

#[tokio::test]
async fn managed_legacy_cleanup_in_progress_is_retained_as_incomplete() {
    use crate::config::ConfigBuilder;
    use crate::thread_manager::StartThreadOptions;
    use crate::thread_manager::ThreadManager;
    let home = tempfile::tempdir().unwrap();
    let mut config = ConfigBuilder::default().codex_home(home.path().to_path_buf())
        .fallback_cwd(Some(home.path().to_path_buf())).build().await.unwrap();
    config.ephemeral = true;
    let manager = Arc::new(ThreadManager::with_models_provider_and_home_for_tests(
        codex_login::CodexAuth::from_api_key("dummy"), config.model_provider.clone(),
        config.codex_home.to_path_buf(), Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    ));
    let stop = tokio_util::sync::CancellationToken::new();
    let tasks = tokio_util::task::TaskTracker::new();
    let mut options = StartThreadOptions::new(config.clone(), /*control_endpoint*/ None);
    options.thread_extension_init.insert(codex_extension_api::SessionIsolation::Isolated);
    let started = manager.start_thread_until(options, stop.clone().cancelled_owned(), &tasks)
        .await.unwrap();
    let weak = Arc::downgrade(&started.thread);
    let refresh = started.thread.session.mcp_refresh.acquire().await.unwrap();
    stop.cancel();
    tasks.close();
    tokio::time::timeout(Duration::from_secs(20), async {
        while !started.thread.session.task_admission_closed.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    }).await.expect("legacy common cleanup entered; refresh gate prevents completion");
    assert!(started.thread.observed_terminal_cleanup().is_none());
    assert!(matches!(started.thread.begin_retirement(Instant::now() + Duration::from_secs(5)),
        Err(crate::ThreadRetirementError::LegacyCleanupStarted)));
    let report = manager.begin_shutdown(Instant::now() + Duration::from_secs(5))
        .unwrap().wait().await.unwrap();
    assert!(!report.is_complete());
    assert!(weak.upgrade().is_some());
    drop(refresh);
    tokio::time::timeout(Duration::from_secs(5), tasks.wait()).await.expect("cleanup joins");
    assert_eq!(started.thread.observed_terminal_cleanup(), Some(super::super::SessionLoopOutcome::Normal));
    drop(started);
    // Registration compacts completed population before refusing the closed
    // manager. The retained shutdown report itself remains incomplete.
    assert!(manager.start_thread(StartThreadOptions::new(config, /*control_endpoint*/ None)).await.is_err());
    assert!(weak.upgrade().is_none());
}

#[derive(Clone, Copy)]
enum ThreadLoopFixture {
    Ordinary,
    Hung,
    Gone,
    Cancelled,
    Panicked,
}

async fn exact_thread_fixture(mode: ThreadLoopFixture) -> crate::CodexThread {
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
    let task = tokio::spawn(async move {
        let _owner = loop_cleanup_owner;
        match mode {
            ThreadLoopFixture::Ordinary => {
                super::super::handlers::submission_loop(
                    loop_session,
                    config,
                    rx_sub,
                    Some(submissions),
                )
                .await;
            }
            ThreadLoopFixture::Hung | ThreadLoopFixture::Cancelled => {
                let _rx_sub = rx_sub;
                std::future::pending::<()>().await;
            }
            ThreadLoopFixture::Gone => drop(rx_sub),
            ThreadLoopFixture::Panicked => panic!("loop panic fixture"),
        }
    });
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
        initial_messages: None,
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
    let mut active = thread.session.active_turn.lock().await;
    assert!(matches!(
        thread.try_begin_idle_retirement(deadline),
        Err(crate::ThreadRetirementError::TaskAdmissionBusy)
    ));
    assert!(!thread.session.task_admission_closed.load(Ordering::Acquire));
    *active = Some(crate::state::ActiveTurn::default());
    drop(active);
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
        assert_eq!(
            legacy.is_ok(),
            ordinary == crate::ThreadShutdownOutcome::Complete
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
async fn failed_common_api_receipt_never_projects_shutdown_complete() {
    for failure in [
        CleanupExecution::ConversationShutdownFailed,
        CleanupExecution::CodeModeShutdownFailed,
        CleanupExecution::McpPrewarmFailed,
    ] {
        let (session, _, events) = super::super::tests::make_session_and_context_with_rx().await;
        let owner = session.cleanup_owner();
        // Exercise the ordinary consumer of a retained failed receipt. This
        // is not a claim of backend-specific failure-injection coverage.
        owner.state.lock().expect("owner").completion =
            Some(futures::future::ready(failure).boxed().shared());
        assert!(super::super::handlers::shutdown(&session, "shutdown".to_string()).await);
        assert_eq!(owner.observe(Arc::clone(&session)).await, failure);
        while let Ok(event) = events.try_recv() {
            assert!(!matches!(
                event.msg,
                codex_protocol::protocol::EventMsg::ShutdownComplete
            ));
        }
    }
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
async fn consumed_websocket_prewarm_keeps_its_real_join_registered() {
    let (session, _) = super::super::tests::make_session_and_context().await;
    let session = Arc::new(session);
    let entered = Arc::new(Notify::new());
    let worker_entered = Arc::clone(&entered);
    let (result_tx, result_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let _result_tx = result_tx;
        worker_entered.notify_one();
        std::future::pending::<()>().await;
    });
    let original = task.abort_handle();
    session
        .set_session_startup_prewarm(
            crate::session_startup_prewarm::SessionStartupPrewarmHandle::new(
                session.task_joins.register(task),
                result_rx,
                std::time::Instant::now(),
                Duration::from_secs(30),
            ),
        )
        .await;
    entered.notified().await;
    let consumed = session
        .take_session_startup_prewarm()
        .await
        .expect("prewarm");
    drop(consumed);
    let owner = session.cleanup_owner();
    owner
        .bind_deadline(Instant::now() + Duration::from_secs(3))
        .expect("bind");
    assert_eq!(
        owner.observe(Arc::clone(&session)).await,
        CleanupExecution::Finished {
            persistence_failed: false
        }
    );
    assert!(original.is_finished());
    assert_eq!(
        session.task_joins.shutdown_until(Instant::now()).await,
        crate::tasks::TaskJoinOutcome::Complete { panicked: false }
    );
}

#[tokio::test]
async fn legacy_join_failure_withholds_shutdown_complete() {
    let (session, _, events) = super::super::tests::make_session_and_context_with_rx().await;
    let owner = session.cleanup_owner();
    assert_eq!(
        session.task_joins.shutdown_until(Instant::now()).await,
        crate::tasks::TaskJoinOutcome::Complete { panicked: false }
    );
    // Deliberately violate ordering: a birth after registry closure must be
    // retained as an authority failure, never projected as clean shutdown.
    session.spawn_startup_auxiliary(async {}).await;
    assert!(super::super::handlers::shutdown(&session, "shutdown".to_string()).await);
    assert_eq!(
        owner.observe(session).await,
        CleanupExecution::TaskJoinFailed
    );
    while let Ok(event) = events.try_recv() {
        assert!(!matches!(
            event.msg,
            codex_protocol::protocol::EventMsg::ShutdownComplete
        ));
    }
}

#[tokio::test]
async fn auxiliary_startup_is_joined_and_cannot_restart_after_cleanup() {
    struct Dropped(Arc<std::sync::atomic::AtomicBool>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    for deadline_bound in [false, true] {
        let (session, _) = super::super::tests::make_session_and_context().await;
        let session = Arc::new(session);
        let owner = session.cleanup_owner();
        let deadline = Instant::now() + Duration::from_secs(3);
        if deadline_bound {
            owner.bind_deadline(deadline).expect("bind");
        }
        let entered = Arc::new(Notify::new());
        let worker_entered = Arc::clone(&entered);
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worker_dropped = Dropped(Arc::clone(&dropped));
        let held_session = Arc::clone(&session);
        session
            .spawn_startup_auxiliary(async move {
                let _session = held_session;
                let _dropped = worker_dropped;
                worker_entered.notify_one();
                std::future::pending::<()>().await;
            })
            .await;
        entered.notified().await;
        assert_eq!(
            owner.observe(Arc::clone(&session)).await,
            CleanupExecution::Finished {
                persistence_failed: false
            }
        );
        assert!(dropped.load(Ordering::Acquire));
        let late_calls = Arc::new(AtomicUsize::new(0));
        let calls = Arc::clone(&late_calls);
        session
            .spawn_startup_auxiliary(async move {
                calls.fetch_add(1, Ordering::SeqCst);
            })
            .await;
        assert_eq!(late_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            session.task_joins.shutdown_until(Instant::now()).await,
            crate::tasks::TaskJoinOutcome::Complete { panicked: false }
        );
    }
}

#[tokio::test(start_paused = true)]
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
    match mcp.peek() {
        Some(McpCleanup::Observed(report)) => assert!(report.is_complete()),
        other => panic!("MCP proof must settle independently: {other:?}"),
    }
    assert!(matches!(
        session.mcp_refresh.acquire().now_or_never(),
        Some(Err(_))
    ));
    assert!(!session.task_admission_closed.load(Ordering::Acquire));
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(observer.await, CleanupExecution::TimedOut);
    assert!(matches!(mcp.peek(), Some(McpCleanup::Observed(report)) if report.is_complete()));
    drop(active_turn);
}

#[tokio::test(start_paused = true)]
async fn failed_initialization_discard_remains_the_original_common_receipt_after_timeout() {
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
    session
        .failed_initialization_persistence
        .store(true, Ordering::Release);
    let session = Arc::new(session);
    let owner = session.cleanup_owner();
    let deadline = Instant::now() + Duration::from_secs(2);
    owner.bind_deadline(deadline).expect("bind");
    let observer_owner = Arc::clone(&owner);
    let observer_session = Arc::clone(&session);
    let observer = tokio::spawn(async move { observer_owner.observe(observer_session).await });
    entered.notified().await;
    let common = owner
        .state
        .lock()
        .expect("cleanup state")
        .common_completion
        .clone()
        .expect("common cleanup receipt");

    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(
        observer.await.expect("cleanup observer"),
        CleanupExecution::TimedOut
    );
    assert!(common.peek().is_none());
    assert_eq!(
        owner.bind_deadline(deadline + Duration::from_secs(30)),
        Ok(deadline),
        "a later observer cannot refresh the original bound"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    // The production observer does not resume work after expiry. The fixture
    // explicitly drives the retained original receipt to prove it reaches the
    // actual failed-initialization discard path without starting cleanup twice.
    release.notify_one();
    assert_eq!(
        owner.observe(Arc::clone(&session)).await,
        CleanupExecution::TimedOut,
    );
    assert!(
        common.peek().is_none(),
        "even a ready stop hook must not resume common cleanup after expiry",
    );
    assert_eq!(
        common.await,
        CleanupExecution::Finished {
            persistence_failed: false
        }
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

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
    assert_eq!(ordinary, CleanupExecution::Finished { persistence_failed: false });
    assert_eq!(owner.observe_suspension(session).await, CleanupExecution::AuthorityUnavailable);
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
    assert_eq!(result, CleanupExecution::Finished { persistence_failed: true });
    assert_eq!(owner.completed(), Some(result));
    assert_eq!(owner.observe(session).await, result);
}

#[tokio::test]
async fn cancelled_suspension_observer_retains_its_cleanup_sequence() {
    let (session, _) = super::super::tests::make_session_and_context().await;
    let session = Arc::new(session);
    let owner = session.cleanup_owner();
    let guard = session.active_turn.lock().await;
    let mut observer = Box::pin(owner.observe_suspension(Arc::clone(&session)));
    assert!(futures::poll!(observer.as_mut()).is_pending());
    drop(observer);
    assert_eq!(owner.completed(), None);
    drop(guard);
    // Resuming through the ordinary entry point must still use suspension's
    // strict writer requirement, not construct a second common cleanup.
    assert_eq!(owner.observe(session).await,
        CleanupExecution::Finished { persistence_failed: true });
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
