use super::*;
use codex_core::StartThreadOptions;
use codex_core::ThreadCleanupOutcome;
use codex_core::ThreadLoopOutcome;
use codex_core::ThreadRetirementReport;
use codex_core::ThreadShutdownOutcome;
use codex_core::config::Config;
use codex_core::config::ConfigBuilder;
use pretty_assertions::assert_eq;
use std::time::Duration;

pub(crate) async fn fixture() -> (tempfile::TempDir, Arc<ThreadManager>, Config) {
    fixture_with_extensions(codex_extension_api::empty_extension_registry()).await
}

#[tokio::test]
async fn processor_shutdown_prior_reports_require_canonical_retirement_completion() {
    let (_home, manager, _config) = fixture().await;
    let manager_report = manager
        .begin_shutdown(Instant::now() + Duration::from_secs(20))
        .expect("manager retirement")
        .wait()
        .await
        .expect("manager report");
    assert!(manager_report.is_complete());
    let complete = ThreadRetirementReport {
        ordinary: ThreadShutdownOutcome::Complete,
        session_loop: ThreadLoopOutcome::Normal,
        cleanup: ThreadCleanupOutcome::Finished {
            persistence_failed: false,
        },
    };
    assert!(complete.is_complete());
    let report = |prior| ProcessorThreadShutdown {
        manager: Some(Ok(manager_report.clone())),
        prior: Some(vec![(ThreadId::new(), Uuid::new_v4(), prior)]),
        ..Default::default()
    };
    assert!(report(complete).is_complete());

    for incomplete in [
        ThreadRetirementReport {
            ordinary: ThreadShutdownOutcome::SubmitFailed,
            ..complete
        },
        ThreadRetirementReport {
            ordinary: ThreadShutdownOutcome::TimedOut,
            ..complete
        },
        ThreadRetirementReport {
            session_loop: ThreadLoopOutcome::Cancelled,
            ..complete
        },
        ThreadRetirementReport {
            session_loop: ThreadLoopOutcome::Panicked,
            ..complete
        },
        ThreadRetirementReport {
            cleanup: ThreadCleanupOutcome::SandboxCleanupUnproven { timed_out: false },
            ..complete
        },
        ThreadRetirementReport {
            cleanup: ThreadCleanupOutcome::SandboxCleanupUnproven { timed_out: true },
            ..complete
        },
    ] {
        assert!(!report(incomplete).is_complete(), "{incomplete:?}");
    }
}

async fn fixture_with_extensions(
    extensions: Arc<codex_extension_api::ExtensionRegistry<Config>>,
) -> (tempfile::TempDir, Arc<ThreadManager>, Config) {
    let home = tempfile::tempdir().unwrap();
    let mut config = ConfigBuilder::default()
        .codex_home(home.path().to_path_buf())
        .fallback_cwd(Some(home.path().to_path_buf()))
        .build()
        .await
        .unwrap();
    config.ephemeral = true;
    let auth = codex_core::test_support::auth_manager_from_auth(
        codex_login::CodexAuth::from_api_key("dummy"),
    );
    let manager = Arc::new(ThreadManager::new(
        &config,
        Arc::clone(&auth),
        codex_core::build_models_manager(&config, auth),
        codex_core::CodexAppsToolsCache::default(),
        codex_protocol::protocol::SessionSource::Exec,
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        extensions,
        Arc::new(codex_home::CodexHomeUserInstructionsProvider::new(
            config.codex_home.clone(),
        )),
        /*analytics_events_client*/ None,
        codex_core::passthrough_image_store(),
        codex_core::thread_store_from_config(&config, /*state_db*/ None),
        /*agent_graph_store*/ None,
        "processor-thread-retirement-test".to_string(),
        /*attestation_provider*/ None,
        /*external_time_provider*/ None,
    ));
    (home, manager, config)
}

#[tokio::test]
async fn processor_thread_owner_closes_before_poll_and_replays_one_attempt() {
    let (_home, manager, config) = fixture().await;
    let started = manager
        .start_thread(StartThreadOptions::new(config.clone(), None))
        .await
        .unwrap();
    let state = ThreadStateManager::new();
    let owner = ThreadShutdownOwner::default();
    let deadline = Instant::now() + Duration::from_secs(20);
    let ticket = owner.begin(&manager, &state, deadline).unwrap();
    drop(ticket);
    assert!(
        manager
            .start_thread(StartThreadOptions::new(config, None))
            .await
            .is_err()
    );
    let ticket = owner
        .begin(&manager, &state, deadline + Duration::from_secs(20))
        .unwrap();
    assert_eq!(ticket.deadline(), deadline);
    let (first, second) = tokio::join!(ticket.wait(), ticket.wait());
    assert!(first.is_complete(), "{first:?}");
    assert_eq!(first, second);
    assert_eq!(first.prior, Some(Vec::new()));
    assert!(manager.list_thread_ids().await.is_empty());
    assert!(ticket._custody.lock().unwrap().is_none());
    drop(started);
    let weak = Arc::downgrade(&manager);
    drop(manager);
    assert!(
        weak.upgrade().is_none(),
        "positive receipt must release resources"
    );
    assert_eq!(ticket.wait().await, first);
}

#[tokio::test]
async fn processor_thread_expiry_does_not_first_poll_or_release_custody() {
    let (_home, manager, _config) = fixture().await;
    let state = ThreadStateManager::new();
    let owner = ThreadShutdownOwner::default();
    let deadline = Instant::now();
    let ticket = owner.begin(&manager, &state, deadline).unwrap();
    let expected = ProcessorThreadShutdown {
        deadline_expired: true,
        ..Default::default()
    };
    assert_eq!(ticket.wait().await, expected);
    let later = owner
        .begin(&manager, &state, deadline + Duration::from_secs(30))
        .unwrap();
    assert_eq!(later.deadline(), deadline);
    assert_eq!(later.wait().await, expected);
    assert!(ticket.completion.peek().is_none());
    let weak = Arc::downgrade(&manager);
    drop(manager);
    drop(owner);
    drop(later);
    assert!(weak.upgrade().is_some());
    // No runtime was launched. Final-root drop tests cycle freedom only.
    drop(ticket);
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn processor_thread_poison_refuses_without_closing_manager() {
    let (_home, manager, config) = fixture().await;
    let state = ThreadStateManager::new();
    let owner = ThreadShutdownOwner::default();
    let _ = std::panic::catch_unwind(|| {
        let _guard = owner.attempt.lock().unwrap();
        panic!("controlled owner poison");
    });
    assert!(matches!(
        owner.begin(&manager, &state, Instant::now()),
        Err(ThreadManagerRetirementError::AuthorityUnavailable)
    ));
    let started = manager
        .start_thread(StartThreadOptions::new(config, None))
        .await
        .unwrap();
    let report = manager
        .begin_shutdown(Instant::now() + Duration::from_secs(20))
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(report.is_complete(), "{report:?}");
    drop(started);
}

#[tokio::test]
async fn processor_thread_ready_failed_cleanup_keeps_independent_custody() {
    struct PanicOnStop;
    impl codex_extension_api::ThreadLifecycleContributor<Config> for PanicOnStop {
        fn on_thread_stop<'a>(
            &'a self,
            _input: codex_extension_api::ThreadStopInput<'a>,
        ) -> codex_extension_api::ExtensionFuture<'a, ()> {
            Box::pin(async { panic!("controlled thread cleanup failure") })
        }
    }
    let mut extensions = codex_extension_api::ExtensionRegistryBuilder::new();
    extensions.thread_lifecycle_contributor(Arc::new(PanicOnStop));
    let (_home, manager, config) = fixture_with_extensions(Arc::new(extensions.build())).await;
    let started = manager
        .start_thread(StartThreadOptions::new(config, None))
        .await
        .unwrap();
    let state = ThreadStateManager::new();
    let owner = ThreadShutdownOwner::default();
    let ticket = owner
        .begin(&manager, &state, Instant::now() + Duration::from_secs(20))
        .unwrap();
    let report = ticket.wait().await;
    assert!(!report.is_complete(), "{report:?}");
    assert!(
        !report.deadline_expired,
        "must observe actual failure, not expiry"
    );
    assert!(matches!(&report.manager, Some(Ok(report)) if !report.is_complete()));
    assert_eq!(report.prior, Some(Vec::new()));
    assert_eq!(ticket.completion.peek(), Some(&report));
    let weak_manager = Arc::downgrade(&manager);
    let weak_thread = Arc::downgrade(&started.thread);
    drop(started);
    drop(manager);
    drop(owner);
    assert!(weak_manager.upgrade().is_some());
    assert!(weak_thread.upgrade().is_some());
    assert_eq!(ticket.wait().await, report);
    // The loop has ended. This final-root destruction checks no ownership
    // cycle, and does not turn the failed cleanup into supported success.
    drop(ticket);
    assert!(weak_manager.upgrade().is_none());
    assert!(weak_thread.upgrade().is_none());
}

#[tokio::test]
async fn recovery_census_requires_positive_proof_for_the_exact_retired_generation() {
    let (_home, manager, _) = fixture().await;
    let manager_report = manager
        .begin_shutdown(Instant::now() + Duration::from_secs(20))
        .unwrap()
        .wait()
        .await
        .unwrap();
    let id = ThreadId::new();
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    let failed = ThreadRetirementReport {
        ordinary: ThreadShutdownOutcome::Complete,
        session_loop: ThreadLoopOutcome::Normal,
        cleanup: ThreadCleanupOutcome::McpFailed,
    };
    let mut census = ProcessorThreadShutdown {
        manager: Some(Ok(manager_report)),
        prior: Some(vec![(id, first, failed), (id, second, failed)]),
        ..Default::default()
    };
    assert!(
        !census.is_complete(),
        "handoff alone proves no physical completion"
    );
    census.reconciled_prior.insert((id, first));
    assert!(
        !census.is_complete(),
        "one generation cannot acknowledge another"
    );
    census.reconciled_prior.insert((id, second));
    assert!(census.is_complete());
    assert_eq!(
        census.prior.as_ref().unwrap()[0].2,
        failed,
        "history is immutable"
    );
    census.prior.as_mut().unwrap()[1].2.session_loop = ThreadLoopOutcome::Panicked;
    assert!(!census.is_complete());
}

#[tokio::test]
async fn reconciliation_never_certifies_unproven_sandbox_cleanup() {
    let (_home, manager, _) = fixture().await;
    let manager_report = manager
        .begin_shutdown(Instant::now() + Duration::from_secs(20))
        .unwrap()
        .wait()
        .await
        .unwrap();
    let id = ThreadId::new();
    let generation = Uuid::new_v4();
    for timed_out in [false, true] {
        let unproven = ThreadRetirementReport {
            ordinary: ThreadShutdownOutcome::Complete,
            session_loop: ThreadLoopOutcome::Normal,
            cleanup: ThreadCleanupOutcome::SandboxCleanupUnproven { timed_out },
        };
        let mut census = ProcessorThreadShutdown {
            manager: Some(Ok(manager_report.clone())),
            prior: Some(vec![(id, generation, unproven)]),
            ..Default::default()
        };
        // Even a recorded reconciliation of this exact generation cannot turn
        // unproven (or timed-out) sandbox cleanup into a clean shutdown.
        census.reconciled_prior.insert((id, generation));
        assert!(!census.is_complete(), "{unproven:?}");
    }
}

struct IdleUnloadHarness {
    _home: tempfile::TempDir,
    config: Config,
    manager: Arc<ThreadManager>,
    state: ThreadStateManager,
    thread: Arc<codex_core::CodexThread>,
    thread_id: ThreadId,
    pending: Arc<tokio::sync::Mutex<crate::thread_state::recovery::PendingThreadUnloads>>,
    outgoing_rx: tokio::sync::mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
    retention: tokio::sync::watch::Receiver<crate::thread_state::RetentionSnapshot>,
    unload: tokio::task::JoinHandle<Option<crate::thread_state::RetentionRetirementRefusal>>,
    _commands: tokio::sync::mpsc::UnboundedReceiver<crate::thread_state::ThreadListenerCommand>,
}

/// Production idle unload over a real manager thread whose stop hook is the
/// only injected behavior. The unload owner uses its fixed 10s deadline.
async fn start_idle_unload(
    contributor: Arc<dyn codex_extension_api::ThreadLifecycleContributor<Config>>,
) -> IdleUnloadHarness {
    use tracing::instrument::WithSubscriber;
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::new();
    builder.thread_lifecycle_contributor(contributor);
    let (home, manager, config) = fixture_with_extensions(Arc::new(builder.build())).await;
    let started = manager
        .start_thread(StartThreadOptions::new(config.clone(), None))
        .await
        .expect("real session birth");
    let thread_id = started.thread_id;
    let thread = started.thread;
    let state = ThreadStateManager::new();
    let entry = state.thread_state(thread_id).await;
    let (cancel_tx, _cancel_rx) = tokio::sync::oneshot::channel();
    let settings = crate::request_processors::thread_settings_from_config_snapshot(
        &thread.config_snapshot().await,
    );
    let (commands, generation) = entry.lock().await.set_listener(
        cancel_tx,
        &thread,
        codex_file_watcher::WatchRegistration::default(),
        settings,
    );
    let retention = state
        .subscribe_to_retention(thread_id)
        .await
        .expect("lifecycle watch");
    let expected = *retention.borrow();
    let mut unloads = crate::thread_state::recovery::PendingThreadUnloads::default();
    unloads.insert(thread_id);
    let pending = Arc::new(tokio::sync::Mutex::new(unloads));
    let (outgoing_tx, outgoing_rx) = tokio::sync::mpsc::channel(64);
    let outgoing = Arc::new(crate::outgoing_message::OutgoingMessageSender::new(
        outgoing_tx,
        codex_analytics::AnalyticsEventsClient::disabled(),
    ));
    let unload = tokio::spawn(
        crate::request_processors::thread_lifecycle::unload_idle_unretained_thread(
            Arc::clone(&manager),
            outgoing,
            Arc::clone(&pending),
            state.clone(),
            crate::thread_status::ThreadWatchManager::new(),
            thread_id,
            Arc::clone(&thread),
            generation,
            expected,
        )
        // Keep the caller's dispatcher so the slow-shutdown warning is observable.
        .with_current_subscriber(),
    );
    IdleUnloadHarness {
        _home: home,
        config,
        manager,
        state,
        thread,
        thread_id,
        pending,
        outgoing_rx,
        retention,
        unload,
        _commands: commands,
    }
}

fn drain_thread_closed(
    rx: &mut tokio::sync::mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
) -> Vec<String> {
    let mut closed = Vec::new();
    while let Ok(envelope) = rx.try_recv() {
        if let crate::outgoing_message::OutgoingEnvelope::Broadcast {
            message: crate::outgoing_message::OutgoingMessage::AppServerNotification(envelope),
        } = envelope
            && let codex_app_server_protocol::ServerNotification::ThreadClosed(notification) =
                envelope.notification
        {
            closed.push(notification.thread_id);
        }
    }
    closed
}

/// Waits for the production claim, then returns the same retained core ticket.
async fn claimed_ticket(harness: &mut IdleUnloadHarness) -> codex_core::ThreadRetirement {
    tokio::time::timeout(
        Duration::from_secs(10),
        harness.retention.wait_for(|snapshot| snapshot.retiring),
    )
    .await
    .expect("idle unload claims retirement")
    .expect("lifecycle watch");
    let ticket = harness
        .thread
        .begin_retirement(Instant::now() + Duration::from_secs(120))
        .expect("retained ticket");
    assert!(
        ticket.deadline() <= Instant::now() + Duration::from_secs(10),
        "observer must replay the unload's original ticket"
    );
    ticket
}

#[derive(Clone, Default)]
struct EventNames(Arc<Mutex<Vec<String>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for EventNames {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        struct Visitor<'a>(&'a Mutex<Vec<String>>);
        impl tracing::field::Visit for Visitor<'_> {
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                if field.name() == "event.name" {
                    self.0.lock().expect("names").push(value.to_string());
                }
            }
            fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
        }
        event.record(&mut Visitor(&self.0));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_idle_unload_closes_after_late_release_with_timed_out_prior_report() {
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::layer::SubscriberExt;
    // Synchronous block inside the loop's own cleanup poll, like the upstream
    // writer-lock test: the deadline cannot pre-empt it.
    struct BlockingStop {
        entered: Arc<tokio::sync::Notify>,
        release: Mutex<std::sync::mpsc::Receiver<()>>,
    }
    impl codex_extension_api::ThreadLifecycleContributor<Config> for BlockingStop {
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
    let entered = Arc::new(tokio::sync::Notify::new());
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let names = EventNames::default();
    let subscriber = tracing_subscriber::registry().with(names.clone());
    let test = async move {
        let mut harness = start_idle_unload(Arc::new(BlockingStop {
            entered: Arc::clone(&entered),
            release: Mutex::new(release_rx),
        }))
        .await;
        tokio::time::timeout(Duration::from_secs(10), entered.notified())
            .await
            .expect("loop cleanup blocked in the stop hook");
        let ticket = claimed_ticket(&mut harness).await;
        let report = tokio::time::timeout(Duration::from_secs(15), ticket.wait())
            .await
            .expect("unload deadline elapsed");
        assert_eq!(report.ordinary, ThreadShutdownOutcome::TimedOut);
        assert!(!report.is_complete());

        // Custody while the loop hangs past the deadline.
        assert!(!harness.unload.is_finished());
        assert!(harness.pending.lock().await.contains(&harness.thread_id));
        assert!(drain_thread_closed(&mut harness.outgoing_rx).is_empty());
        assert!(
            harness
                .manager
                .list_thread_ids()
                .await
                .contains(&harness.thread_id)
        );
        assert!(harness.retention.borrow().retiring);

        release_tx.send(()).expect("release blocked cleanup");
        tokio::time::timeout(Duration::from_secs(10), &mut harness.unload)
            .await
            .expect("late release finalizes idle unload")
            .expect("unload task");
        assert_eq!(
            drain_thread_closed(&mut harness.outgoing_rx),
            vec![harness.thread_id.to_string()]
        );
        assert!(harness.manager.list_thread_ids().await.is_empty());
        assert!(!harness.pending.lock().await.contains(&harness.thread_id));
        assert_eq!(ticket.wait().await, report, "original report stays sticky");

        let shutdown = ThreadShutdownOwner::default()
            .begin(
                &harness.manager,
                &harness.state,
                Instant::now() + Duration::from_secs(20),
            )
            .expect("processor shutdown")
            .wait()
            .await;
        let prior = shutdown.prior.clone().expect("prior tickets drained");
        assert_eq!(prior.len(), 1);
        assert_eq!(prior[0].0, harness.thread_id);
        assert_eq!(prior[0].2, report);
        assert!(!shutdown.is_complete(), "{shutdown:?}");
    };
    test.with_subscriber(subscriber).await;
    assert!(
        names
            .0
            .lock()
            .expect("names")
            .iter()
            .any(|name| name == "codex.app_server.thread_shutdown_slow")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_unload_without_observed_release_never_closes() {
    // Asynchronously pending cleanup is abandoned at the deadline: the loop
    // joins without a receipt, so release is never observed.
    struct PendingStop {
        entered: Arc<tokio::sync::Notify>,
    }
    impl codex_extension_api::ThreadLifecycleContributor<Config> for PendingStop {
        fn on_thread_stop<'a>(
            &'a self,
            _input: codex_extension_api::ThreadStopInput<'a>,
        ) -> codex_extension_api::ExtensionFuture<'a, ()> {
            Box::pin(async move {
                self.entered.notify_one();
                std::future::pending::<()>().await;
            })
        }
    }
    let entered = Arc::new(tokio::sync::Notify::new());
    let mut harness = start_idle_unload(Arc::new(PendingStop {
        entered: Arc::clone(&entered),
    }))
    .await;
    tokio::time::timeout(Duration::from_secs(10), entered.notified())
        .await
        .expect("cleanup reached the stop hook");
    let ticket = claimed_ticket(&mut harness).await;
    tokio::time::timeout(Duration::from_secs(20), &mut harness.unload)
        .await
        .expect("negative observation ends the unload")
        .expect("unload task");
    let report = ticket.wait().await;
    assert_eq!(report.ordinary, ThreadShutdownOutcome::TimedOut);
    assert!(!report.is_complete());
    assert!(drain_thread_closed(&mut harness.outgoing_rx).is_empty());
    assert!(!harness.pending.lock().await.contains(&harness.thread_id));
    assert!(
        harness
            .manager
            .list_thread_ids()
            .await
            .contains(&harness.thread_id),
        "unproven runtime stays loaded"
    );
    assert!(harness.retention.borrow().retiring, "custody row retained");
    assert_eq!(ticket.wait().await, report);
}

/// Synchronous block inside the loop's own cleanup poll; the unload deadline
/// cannot pre-empt it, so the original report is TimedOut while the loop and
/// its cleanup owner finish cleanly once released.
struct GatedStop {
    entered: Arc<tokio::sync::Notify>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}

impl codex_extension_api::ThreadLifecycleContributor<Config> for GatedStop {
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

/// Asynchronously pending cleanup is abandoned at the deadline: the loop
/// joins without a clean cleanup receipt.
struct AbandonedStop {
    entered: Arc<tokio::sync::Notify>,
}

impl codex_extension_api::ThreadLifecycleContributor<Config> for AbandonedStop {
    fn on_thread_stop<'a>(
        &'a self,
        _input: codex_extension_api::ThreadStopInput<'a>,
    ) -> codex_extension_api::ExtensionFuture<'a, ()> {
        Box::pin(async move {
            self.entered.notify_one();
            std::future::pending::<()>().await;
        })
    }
}

/// Late close archives the exact retiring row instead of stranding it: a
/// same-id successor gets fresh authority, while the old generation's
/// TimedOut report stays sticky and unreconciled in the shutdown census.
/// ThreadStateManager + ThreadManager level: this harness has no RPC processor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_release_archives_custody_and_admits_same_id_successor() {
    use codex_rollout::InitialHistory;
    use codex_rollout::ResumedHistory;
    let entered = Arc::new(tokio::sync::Notify::new());
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let mut harness = start_idle_unload(Arc::new(GatedStop {
        entered: Arc::clone(&entered),
        release: Mutex::new(release_rx),
    }))
    .await;
    tokio::time::timeout(Duration::from_secs(10), entered.notified())
        .await
        .expect("loop cleanup blocked in the stop hook");
    let ticket = claimed_ticket(&mut harness).await;
    let report = tokio::time::timeout(Duration::from_secs(15), ticket.wait())
        .await
        .expect("unload deadline elapsed");
    assert_eq!(report.ordinary, ThreadShutdownOutcome::TimedOut);
    assert!(!report.is_complete());
    let thread_id = harness.thread_id;
    let generation = harness.retention.borrow().generation;
    let claim = crate::thread_state::RetentionRetirementClaim {
        thread_id,
        generation,
    };

    // (d) The loop has not joined: no evidence, no move, nothing driven.
    assert!(!harness.thread.late_release_is_quiescent());
    assert!(
        !harness
            .state
            .archive_late_release(claim, &harness.thread)
            .await
    );
    assert!(harness.retention.borrow().retiring);
    assert!(
        harness
            .state
            .subscribe_to_retention(thread_id)
            .await
            .is_some(),
        "row stays current while the release is unobserved"
    );
    assert!(harness.manager.list_thread_ids().await.contains(&thread_id));

    // (b) A concurrent resume (its own owner scope) is refused while the
    // passive late-release wait holds the unowned idle claim.
    crate::thread_state::recovery::resume_scope(async {
        let mut pending = harness.pending.lock().await;
        assert!(pending.blocks(&thread_id));
        assert_eq!(
            pending.reserve(thread_id),
            Err("thread is closing; retry after the transition completes")
        );
    })
    .await;
    assert!(!harness.unload.is_finished());

    release_tx.send(()).expect("release blocked cleanup");
    tokio::time::timeout(Duration::from_secs(10), &mut harness.unload)
        .await
        .expect("late release finalizes idle unload")
        .expect("unload task");
    assert_eq!(
        drain_thread_closed(&mut harness.outgoing_rx),
        vec![thread_id.to_string()]
    );
    assert!(harness.manager.list_thread_ids().await.is_empty());
    assert!(!harness.pending.lock().await.contains(&thread_id));
    assert!(harness.thread.late_release_is_quiescent());
    // Moved, not erased: the exact row keeps its retiring snapshot and ticket.
    assert!(harness.retention.borrow().retiring);
    assert_eq!(ticket.wait().await, report, "original report stays sticky");
    assert!(
        harness
            .state
            .subscribe_to_retention(thread_id)
            .await
            .is_none(),
        "no current observation or lifecycle row survives the late close"
    );
    assert!(
        !harness
            .state
            .archive_late_release(claim, &harness.thread)
            .await,
        "an archived row cannot be archived twice"
    );

    // (a) Drop every test reference to the old runtime, as production does.
    let IdleUnloadHarness {
        _home,
        config,
        manager,
        state,
        thread,
        ..
    } = harness;
    let old = Arc::downgrade(&thread);
    drop(thread);
    assert!(
        matches!(state.retirement_candidate(thread_id).await, Ok(None)),
        "no stranded retiring row blocks recovery lookup"
    );

    let mut options = StartThreadOptions::new(config, None);
    options.initial_history = InitialHistory::Resumed(ResumedHistory {
        history_revision: None,
        conversation_id: thread_id,
        history: Arc::new(Vec::new()),
        rollout_path: None,
    });
    let successor = manager
        .start_thread(options)
        .await
        .expect("same-id successor runtime");
    assert_eq!(successor.thread_id, thread_id);
    assert!(
        old.upgrade()
            .is_none_or(|old| !Arc::ptr_eq(&old, &successor.thread)),
        "successor is a new runtime"
    );
    state.thread_state(thread_id).await;
    let fresh = state
        .subscribe_to_retention(thread_id)
        .await
        .expect("fresh lifecycle row");
    let fresh = *fresh.borrow();
    assert!(!fresh.retiring);
    assert_ne!(fresh.generation, generation);
    assert!(matches!(
        state.retirement_candidate(thread_id).await,
        Ok(None)
    ));
    let principal = crate::thread_state::RetentionPrincipalId::connection_owned();
    state
        .connection_initialized(
            crate::outgoing_message::ConnectionId(1),
            crate::thread_state::ConnectionCapabilities {
                retention_principal: Some(principal),
                trusted_interactive: true,
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(
        state.acquire_retention(thread_id, principal).await,
        Ok(crate::thread_state::RetentionAcquireOutcome::Acquired { .. })
    ));

    // (c) The census still holds only the old generation's sticky report.
    let shutdown = ThreadShutdownOwner::default()
        .begin(&manager, &state, Instant::now() + Duration::from_secs(20))
        .expect("processor shutdown")
        .wait()
        .await;
    assert_eq!(
        shutdown.prior.clone().expect("prior tickets drained"),
        vec![(thread_id, generation, report)]
    );
    assert!(!shutdown.reconciled_prior.contains(&(thread_id, generation)));
    assert!(!shutdown.is_complete(), "{shutdown:?}");
}

/// (d) Each reachable missing piece of custody or evidence refuses the move
/// and leaves the runtime loaded with its retiring row current.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_late_release_refuses_without_exact_custody_or_receipt() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let mut harness = start_idle_unload(Arc::new(AbandonedStop {
        entered: Arc::clone(&entered),
    }))
    .await;
    tokio::time::timeout(Duration::from_secs(10), entered.notified())
        .await
        .expect("cleanup reached the stop hook");
    let ticket = claimed_ticket(&mut harness).await;
    tokio::time::timeout(Duration::from_secs(20), &mut harness.unload)
        .await
        .expect("negative observation ends the unload")
        .expect("unload task");
    let report = ticket.wait().await;
    assert!(!report.is_complete());
    let thread_id = harness.thread_id;
    let generation = harness.retention.borrow().generation;
    let claim = crate::thread_state::RetentionRetirementClaim {
        thread_id,
        generation,
    };

    // Loop joined, but no clean cleanup receipt was recorded.
    assert!(!harness.thread.late_release_is_quiescent());
    assert!(
        !harness
            .state
            .archive_late_release(claim, &harness.thread)
            .await
    );
    // Wrong generation for the exact runtime.
    assert!(
        !harness
            .state
            .archive_late_release(
                crate::thread_state::RetentionRetirementClaim {
                    thread_id,
                    generation: Uuid::now_v7(),
                },
                &harness.thread,
            )
            .await
    );
    // Wrong runtime for the exact generation.
    let other = harness
        .manager
        .start_thread(StartThreadOptions::new(harness.config.clone(), None))
        .await
        .expect("unrelated runtime");
    assert!(
        !harness
            .state
            .archive_late_release(claim, &other.thread)
            .await
    );
    // A row that never retired.
    harness.state.thread_state(other.thread_id).await;
    let unretired = harness
        .state
        .subscribe_to_retention(other.thread_id)
        .await
        .expect("unretired lifecycle");
    let unretired_generation = unretired.borrow().generation;
    assert!(!unretired.borrow().retiring);
    assert!(
        !harness
            .state
            .archive_late_release(
                crate::thread_state::RetentionRetirementClaim {
                    thread_id: other.thread_id,
                    generation: unretired_generation,
                },
                &other.thread,
            )
            .await
    );

    assert!(
        harness.manager.list_thread_ids().await.contains(&thread_id),
        "unproven runtime stays loaded"
    );
    assert!(harness.retention.borrow().retiring, "custody row retained");
    assert!(
        harness
            .state
            .subscribe_to_retention(thread_id)
            .await
            .is_some_and(|row| row.borrow().retiring),
        "refused rows are never moved"
    );
    assert!(matches!(
        harness.state.retirement_candidate(thread_id).await,
        Ok(Some(_))
    ));
    assert!(drain_thread_closed(&mut harness.outgoing_rx).is_empty());
    assert_eq!(ticket.wait().await, report);
}

/// Port of upstream app-server `thread_unload.rs::
/// idle_unload_finishes_after_slow_writer_shutdown` onto the real listener,
/// zero-delay idle countdown, and `unload_idle_unretained_thread`. The rollout
/// coordination lock is taken after the durable writer exists and before any
/// listener or countdown does, so no ordering depends on elapsed time.
///
/// The production listener is `tokio::spawn`ed and does not inherit a scoped
/// dispatcher, so the capture layer is made the thread default on every runtime
/// worker (and on the `block_on` thread) without touching the global default.
#[test]
fn idle_unload_finishes_after_slow_writer_shutdown() {
    use tracing_subscriber::layer::SubscriberExt;
    let (events_tx, events_rx) = tokio::sync::watch::channel(Vec::new());
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::registry().with(SlowShutdownEvents(Arc::new(events_tx))),
    );
    let worker_dispatch = dispatch.clone();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .on_thread_start(move || {
            // Runtime threads end with the runtime; the guard lives as long.
            std::mem::forget(tracing::dispatcher::set_default(&worker_dispatch));
        })
        .build()
        .expect("test runtime");
    tracing::dispatcher::with_default(&dispatch, || {
        runtime.block_on(idle_unload_finishes_after_slow_writer_shutdown_body(
            events_rx,
        ));
    });
}

const THREAD_SHUTDOWN_SLOW: &str = "codex.app_server.thread_shutdown_slow";

/// Captured `(thread_id, emitted_at)` for each `thread_shutdown_slow` event.
type SlowShutdownCapture = Vec<(String, Instant)>;

struct SlowShutdownEvents(Arc<tokio::sync::watch::Sender<SlowShutdownCapture>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for SlowShutdownEvents {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        #[derive(Default)]
        struct Visitor {
            name: Option<String>,
            thread_id: Option<String>,
        }
        impl tracing::field::Visit for Visitor {
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                match field.name() {
                    "event.name" => self.name = Some(value.to_string()),
                    "thread_id" => self.thread_id = Some(value.to_string()),
                    _ => {}
                }
            }
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                match field.name() {
                    "event.name" => self.name = Some(format!("{value:?}")),
                    "thread_id" => self.thread_id = Some(format!("{value:?}")),
                    _ => {}
                }
            }
        }
        let mut visitor = Visitor::default();
        event.record(&mut visitor);
        if visitor.name.as_deref() == Some(THREAD_SHUTDOWN_SLOW) {
            let thread_id = visitor.thread_id.unwrap_or_default();
            let emitted_at = Instant::now();
            self.0
                .send_modify(|events| events.push((thread_id, emitted_at)));
        }
    }
}

async fn idle_unload_finishes_after_slow_writer_shutdown_body(
    mut slow_events: tokio::sync::watch::Receiver<SlowShutdownCapture>,
) {
    let (home, manager, config) = fixture().await;
    // The shared fixture is ephemeral; the real durable writer needs persistence.
    let mut durable = config.clone();
    durable.ephemeral = false;
    let started = manager
        .start_thread(StartThreadOptions::new(durable, None))
        .await
        .expect("real durable session birth");
    let thread_id = started.thread_id;
    let thread = started.thread;
    let locks = home.path().join("thread-writer-locks");
    assert!(
        locks.join(format!("{thread_id}.lock")).exists(),
        "session birth holds the real thread writer lock"
    );

    // Block the real cross-process writer-lock cleanup before any listener or
    // idle countdown exists. Other tasks, including the 10s deadline, still run.
    let coordination_lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(locks.join(".coordination.lock"))
        .expect("coordination lock file");
    coordination_lock.lock().expect("hold coordination lock");

    let state = ThreadStateManager::new();
    let connection = crate::outgoing_message::ConnectionId(1);
    // Unproven stdio-equivalent capabilities: no retention principal.
    state
        .connection_initialized(
            connection,
            crate::thread_state::ConnectionCapabilities::default(),
        )
        .await;
    state
        .try_add_connection_to_thread(thread_id, connection)
        .await
        .expect("creating connection subscribes");
    let thread_state = state.thread_state(thread_id).await;
    let mut retention = state
        .subscribe_to_retention(thread_id)
        .await
        .expect("lifecycle watch");

    // Upstream `thread/unsubscribe` -> `Unsubscribed`: the thread is loaded
    // (the processor's NotLoaded guard) and the connection was subscribed. In
    // the fork this neither retains nor releases; idle unload owns retirement.
    assert!(manager.get_thread(thread_id).await.is_ok());
    assert!(
        state
            .unsubscribe_connection_from_thread(thread_id, connection)
            .await
    );

    let (outgoing_tx, mut outgoing_rx) = tokio::sync::mpsc::channel(64);
    let outgoing = Arc::new(crate::outgoing_message::OutgoingMessageSender::new(
        outgoing_tx,
        codex_analytics::AnalyticsEventsClient::disabled(),
    ));
    let skills_home =
        codex_utils_absolute_path::AbsolutePathBuf::try_from(home.path()).expect("absolute home");
    let skills_watcher = crate::skills_watcher::SkillsWatcher::new(
        Arc::new(codex_skills_extension::HostSkillsService::new(
            skills_home.clone(),
            /*bundled_skills_enabled*/ false,
        )),
        &skills_home,
        Arc::clone(&outgoing),
    );
    let thread_watch_manager = crate::thread_status::ThreadWatchManager::new();
    thread_watch_manager
        .upsert_thread_silently(&thread_id.to_string())
        .await;
    let pending = Arc::new(tokio::sync::Mutex::new(
        crate::thread_state::recovery::PendingThreadUnloads::default(),
    ));
    let context = crate::request_processors::thread_lifecycle::ListenerTaskContext {
        thread_manager: Arc::clone(&manager),
        thread_state_manager: state.clone(),
        outgoing,
        pending_thread_unloads: Arc::clone(&pending),
        thread_watch_manager,
        codex_home: home.path().to_path_buf(),
        thread_unload_delay: Duration::ZERO,
        skills_watcher,
        turn_cost_worker: None,
    };
    // Production listener: its zero-delay countdown starts here and drives the
    // real idle unload with no further test action.
    crate::request_processors::thread_lifecycle::ensure_listener_task_running(
        context,
        thread_id,
        Arc::clone(&thread),
        thread_state,
    )
    .await
    .expect("listener attaches");

    // Upstream `codex.app_server.thread_shutdown_slow`: the unload's own retained
    // ticket reports TimedOut, which is exactly the condition that emits that
    // warning (the listener is spawned without the test's dispatcher).
    tokio::time::timeout(
        Duration::from_secs(10),
        retention.wait_for(|snapshot| snapshot.retiring),
    )
    .await
    .expect("idle unload claims retirement")
    .expect("lifecycle watch");
    let ticket = thread
        .begin_retirement(Instant::now() + Duration::from_secs(120))
        .expect("retained ticket");
    assert!(
        ticket.deadline() <= Instant::now() + Duration::from_secs(10),
        "observer must replay the unload's original ticket"
    );
    let report = tokio::time::timeout(Duration::from_secs(15), ticket.wait())
        .await
        .expect("unload deadline elapsed");
    assert_eq!(report.ordinary, ThreadShutdownOutcome::TimedOut);
    assert!(!report.is_complete());

    // Upstream `wait_for_json_log_event("codex.app_server.thread_shutdown_slow")`:
    // the warning itself, for this thread, emitted only after the deadline and
    // while the writer lock (hence thread/closed) is still blocked.
    let slow = tokio::time::timeout(
        Duration::from_secs(10),
        slow_events.wait_for(|events| !events.is_empty()),
    )
    .await
    .expect("idle unload never reached the shutdown warning deadline")
    .expect("slow event capture")
    .clone();
    assert_eq!(slow.len(), 1, "{slow:?}");
    assert_eq!(slow[0].0, thread_id.to_string());
    assert!(
        slow[0].1 >= ticket.deadline(),
        "slow warning must follow the retirement deadline"
    );

    // Upstream loaded list `[thread_id]` while the writer is still blocked.
    assert_eq!(manager.list_thread_ids().await, vec![thread_id]);
    assert!(pending.lock().await.contains(&thread_id));
    assert!(drain_thread_closed(&mut outgoing_rx).is_empty());

    // Once persistence can finish, the delayed shutdown must still remove the
    // runtime and broadcast closure without another subscription or request.
    drop(coordination_lock);
    let closed = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let envelope = outgoing_rx.recv().await.expect("outgoing stream");
            if let crate::outgoing_message::OutgoingEnvelope::Broadcast {
                message: crate::outgoing_message::OutgoingMessage::AppServerNotification(envelope),
            } = envelope
                && let codex_app_server_protocol::ServerNotification::ThreadClosed(notification) =
                    envelope.notification
            {
                break notification.thread_id;
            }
        }
    })
    .await
    .expect("slow shutdown never finalized idle unload");
    assert_eq!(closed, thread_id.to_string());
    // Exactly one warning, already emitted before thread/closed was broadcast.
    assert_eq!(*slow_events.borrow(), slow);
    // Upstream empty loaded list.
    assert!(manager.list_thread_ids().await.is_empty());
    assert!(!locks.join(format!("{thread_id}.lock")).exists());
    assert_eq!(ticket.wait().await, report, "original report stays sticky");
}

/// Records the order of A's clear SessionEnd dispatch and its idle stop.
#[derive(Clone, Default)]
struct StopOrder(Arc<Mutex<Vec<&'static str>>>);

impl StopOrder {
    fn push(&self, event: &'static str) {
        self.0.lock().expect("order").push(event);
    }

    fn snapshot(&self) -> Vec<&'static str> {
        self.0.lock().expect("order").clone()
    }
}

impl codex_extension_api::ThreadLifecycleContributor<Config> for StopOrder {
    fn on_thread_stop<'a>(
        &'a self,
        _input: codex_extension_api::ThreadStopInput<'a>,
    ) -> codex_extension_api::ExtensionFuture<'a, ()> {
        Box::pin(async move { self.push("other") })
    }
}

/// A real thread under the production listener with the websocket fixture's
/// 1s idle delay, subscribed by one unretained requester, on paused time.
struct ClearFenceHarness {
    _home: tempfile::TempDir,
    manager: Arc<ThreadManager>,
    state: ThreadStateManager,
    thread: Arc<codex_core::CodexThread>,
    predecessor: ThreadId,
    requester: crate::outgoing_message::ConnectionId,
    thread_state: Arc<tokio::sync::Mutex<crate::thread_state::ThreadState>>,
    retention: tokio::sync::watch::Receiver<crate::thread_state::RetentionSnapshot>,
    outgoing: Arc<crate::outgoing_message::OutgoingMessageSender>,
    outgoing_rx: tokio::sync::mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
    pending: Arc<tokio::sync::Mutex<crate::thread_state::recovery::PendingThreadUnloads>>,
    thread_watch_manager: crate::thread_status::ThreadWatchManager,
}

impl ClearFenceHarness {
    async fn start(order: &StopOrder) -> Self {
        let mut builder = codex_extension_api::ExtensionRegistryBuilder::new();
        builder.thread_lifecycle_contributor(Arc::new(order.clone()));
        let (home, manager, config) = fixture_with_extensions(Arc::new(builder.build())).await;
        let started = manager
            .start_thread(StartThreadOptions::new(config, None))
            .await
            .expect("real session birth");
        let predecessor = started.thread_id;
        let thread = started.thread;
        let state = ThreadStateManager::new();
        let requester = crate::outgoing_message::ConnectionId(1);
        // No retention principal: neither A nor B is retained, as in the flake.
        state
            .connection_initialized(
                requester,
                crate::thread_state::ConnectionCapabilities::default(),
            )
            .await;
        state
            .try_add_connection_to_thread(predecessor, requester)
            .await
            .expect("requester subscribes to A");
        let thread_state = state.thread_state(predecessor).await;
        let retention = state
            .subscribe_to_retention(predecessor)
            .await
            .expect("lifecycle watch");
        let (outgoing_tx, outgoing_rx) = tokio::sync::mpsc::channel(64);
        let outgoing = Arc::new(crate::outgoing_message::OutgoingMessageSender::new(
            outgoing_tx,
            codex_analytics::AnalyticsEventsClient::disabled(),
        ));
        let skills_home = codex_utils_absolute_path::AbsolutePathBuf::try_from(home.path())
            .expect("absolute home");
        let skills_watcher = crate::skills_watcher::SkillsWatcher::new(
            Arc::new(codex_skills_extension::HostSkillsService::new(
                skills_home.clone(),
                /*bundled_skills_enabled*/ false,
            )),
            &skills_home,
            Arc::clone(&outgoing),
        );
        let thread_watch_manager = crate::thread_status::ThreadWatchManager::new();
        thread_watch_manager
            .upsert_thread_silently(&predecessor.to_string())
            .await;
        let pending = Arc::new(tokio::sync::Mutex::new(
            crate::thread_state::recovery::PendingThreadUnloads::default(),
        ));
        let context = crate::request_processors::thread_lifecycle::ListenerTaskContext {
            thread_manager: Arc::clone(&manager),
            thread_state_manager: state.clone(),
            outgoing: Arc::clone(&outgoing),
            pending_thread_unloads: Arc::clone(&pending),
            thread_watch_manager: thread_watch_manager.clone(),
            codex_home: home.path().to_path_buf(),
            // The websocket fixture's `thread_unload_delay_secs=1`.
            thread_unload_delay: Duration::from_secs(1),
            skills_watcher,
            turn_cost_worker: None,
        };
        // From here on only explicit advances move the idle clock.
        tokio::time::pause();
        // The production listener arms A's 1s idle countdown now.
        crate::request_processors::thread_lifecycle::ensure_listener_task_running(
            context,
            predecessor,
            Arc::clone(&thread),
            Arc::clone(&thread_state),
        )
        .await
        .expect("listener attaches");
        Self {
            _home: home,
            manager,
            state,
            thread,
            predecessor,
            requester,
            thread_state,
            retention,
            outgoing,
            outgoing_rx,
            pending,
            thread_watch_manager,
        }
    }

    /// After the reservation is gone: the re-armed idle unload retires A.
    async fn expect_unload_after_rearm(&mut self) {
        tokio::time::advance(Duration::from_millis(1_100)).await;
        // Real time for the actual shutdown and its 10s owner deadline.
        tokio::time::resume();
        let closed = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let envelope = self.outgoing_rx.recv().await.expect("outgoing stream");
                if let crate::outgoing_message::OutgoingEnvelope::Broadcast {
                    message:
                        crate::outgoing_message::OutgoingMessage::AppServerNotification(envelope),
                } = envelope
                    && let codex_app_server_protocol::ServerNotification::ThreadClosed(
                        notification,
                    ) = envelope.notification
                {
                    break notification.thread_id;
                }
            }
        })
        .await
        .expect("A never unloaded after its reservation was released");
        assert_eq!(closed, self.predecessor.to_string());
        assert!(
            !self
                .manager
                .list_thread_ids()
                .await
                .contains(&self.predecessor)
        );
    }

    /// A claim racing the reservation is refused Busy: nothing committed.
    async fn assert_claim_busy(&self) {
        let generation = self.thread_state.lock().await.listener_generation;
        let claim = self
            .state
            .claim_thread_retirement(
                self.predecessor,
                *self.retention.borrow(),
                &self.thread,
                generation,
                Instant::now() + Duration::from_secs(10),
            )
            .await;
        assert!(
            matches!(
                claim,
                Err(crate::thread_state::RetentionRetirementRefusal::Busy)
            ),
            "{:?}",
            claim.map(|_| ())
        );
    }

    /// Lets `elapsed` pass on the idle clock, then proves A was not retired.
    async fn assert_not_retired_after(&mut self, elapsed: Duration, order: &StopOrder) {
        tokio::time::advance(elapsed).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(!self.retention.borrow().retiring, "A retired while fenced");
        assert!(
            self.manager
                .list_thread_ids()
                .await
                .contains(&self.predecessor)
        );
        assert!(drain_thread_closed(&mut self.outgoing_rx).is_empty());
        assert!(
            !order.snapshot().contains(&"other"),
            "A stopped while fenced"
        );
    }
}

/// Regression for the mid-clear idle-unload race (`thread_clear_websocket::
/// cleared_and_control_threads_keep_operational_other_idle_unload`): an
/// unretained predecessor's 1s idle deadline elapsing between the clear
/// reservation and the A -> B subscription move must not retire A.
#[tokio::test]
async fn idle_unload_is_fenced_while_clear_reservation_is_held_and_rearmed_after() {
    let order = StopOrder::default();
    let mut harness = ClearFenceHarness::start(&order).await;
    let (predecessor, requester) = (harness.predecessor, harness.requester);
    let authority = harness
        .state
        .reserve_clear_transition_authority_guarded(predecessor, requester)
        .await
        .expect("clear reserves A");
    assert!(
        harness.retention.borrow().clear_held,
        "reservation disarms idle unload"
    );
    harness.assert_claim_busy().await;
    harness
        .assert_not_retired_after(Duration::from_millis(1_500), &order)
        .await;

    // A's clear SessionEnd, then the authoritative A -> B move, both on the
    // still-loaded runtime.
    harness
        .thread
        .dispatch_clear_session_end(codex_state::ClearTransitionId::new())
        .await;
    order.push("clear");
    let successor = ThreadId::new();
    assert!(
        harness
            .state
            .reserve_clear_successor_subscription(predecessor, successor)
            .await
    );
    assert_eq!(
        harness
            .state
            .try_move_connection_for_clear(predecessor, successor, requester)
            .await,
        Ok(())
    );
    assert!(
        harness
            .state
            .terminalize_clear_successor_subscription_reservation(predecessor, successor)
            .await
    );
    harness
        .assert_not_retired_after(Duration::from_millis(1_500), &order)
        .await;

    // Proven completion releases the exact reservation and re-arms idle unload.
    assert!(authority.release().await);
    assert!(!harness.retention.borrow().clear_held);
    assert!(
        harness
            .state
            .held_clear_transition_reservations()
            .await
            .is_empty()
    );
    harness.expect_unload_after_rearm().await;
    assert_eq!(order.snapshot(), vec!["clear", "other"]);
}

/// The real guard inside a clear future that is cancelled mid-clear: the drop
/// is not proof that hook children or DB writes stopped, so the exact
/// reservation is retained, reported incomplete, never released by polling,
/// and A stays fenced however long the idle delay elapses.
#[tokio::test]
async fn cancelled_clear_retains_reservation_unproven_and_keeps_predecessor_fenced() {
    let order = StopOrder::default();
    let mut harness = ClearFenceHarness::start(&order).await;
    let (predecessor, requester) = (harness.predecessor, harness.requester);
    let (admitted_tx, admitted_rx) = tokio::sync::oneshot::channel();
    let clear = tokio::spawn({
        let state = harness.state.clone();
        async move {
            let mut authority = state
                .reserve_clear_transition_authority_guarded(predecessor, requester)
                .await
                .expect("clear reserves A");
            // The production stage marker: admitted clear work has begun.
            authority.begin_work();
            let _ = admitted_tx.send(());
            // Admitted clear work that never returns (e.g. a hook child).
            std::future::pending::<()>().await;
            authority.release().await
        }
    });
    admitted_rx.await.expect("clear admitted");
    clear.abort();
    assert!(clear.await.expect_err("cancelled").is_cancelled());

    assert_eq!(
        harness.state.held_clear_transition_reservations().await,
        vec![(predecessor, true)],
        "reservation retained and reported incomplete"
    );
    assert!(harness.retention.borrow().clear_held);
    harness.assert_claim_busy().await;
    harness
        .assert_not_retired_after(Duration::from_secs(5), &order)
        .await;
    assert_eq!(
        harness.state.held_clear_transition_reservations().await,
        vec![(predecessor, true)],
        "nothing released the unproven reservation"
    );
}

/// A clear cancelled after reserving but before its stage marker recorded any
/// work: nothing can be in flight, so the drop releases the exact reservation
/// and idle unload is re-armed.
#[tokio::test]
async fn clear_cancelled_before_work_starts_releases_its_reservation() {
    let order = StopOrder::default();
    let mut harness = ClearFenceHarness::start(&order).await;
    let (predecessor, requester) = (harness.predecessor, harness.requester);
    let (reserved_tx, reserved_rx) = tokio::sync::oneshot::channel();
    let clear = tokio::spawn({
        let state = harness.state.clone();
        async move {
            let authority = state
                .reserve_clear_transition_authority_guarded(predecessor, requester)
                .await
                .expect("clear reserves A");
            let _ = reserved_tx.send(());
            // Cancelled before `begin_work`: no clear work exists.
            std::future::pending::<()>().await;
            authority.release().await
        }
    });
    reserved_rx.await.expect("clear reserved");
    assert!(harness.retention.borrow().clear_held);
    clear.abort();
    assert!(clear.await.expect_err("cancelled").is_cancelled());

    assert!(
        harness
            .state
            .held_clear_transition_reservations()
            .await
            .is_empty(),
        "unstarted reservation released on cancellation"
    );
    assert!(!harness.retention.borrow().clear_held);
    harness.expect_unload_after_rearm().await;
    assert_eq!(order.snapshot(), vec!["other"]);
}

/// Claim before cancel: when the idle claim is refused Busy by a held clear
/// reservation, `unload_idle_unretained_thread` has zero side effects. The
/// pending server request survives unresolved, nothing is emitted, and the
/// retention snapshot, pending-unload set and loaded runtime are unchanged.
#[tokio::test]
async fn busy_idle_claim_has_no_cancellation_side_effects() {
    let order = StopOrder::default();
    let mut harness = ClearFenceHarness::start(&order).await;
    let (predecessor, requester) = (harness.predecessor, harness.requester);
    // The listener's view from just before the clear reserved A.
    let generation = harness.thread_state.lock().await.listener_generation;
    let expected = *harness.retention.borrow();
    let _authority = harness
        .state
        .reserve_clear_transition_authority_guarded(predecessor, requester)
        .await
        .expect("clear reserves A");

    let (request_id, mut response_rx) = harness
        .outgoing
        .send_request_to_connections(
            Some(&[requester]),
            codex_app_server_protocol::ServerRequestPayload::ToolRequestUserInput(
                codex_app_server_protocol::ToolRequestUserInputParams {
                    thread_id: predecessor.to_string(),
                    turn_id: "turn-1".to_string(),
                    item_id: "call-1".to_string(),
                    questions: vec![],
                    is_blocking: true,
                    auto_resolution_ms: None,
                },
            ),
            Some(predecessor),
        )
        .await;
    assert!(
        harness.outgoing_rx.recv().await.is_some(),
        "request delivered"
    );
    let pending_ids = |requests: Vec<codex_app_server_protocol::ServerRequest>| {
        requests
            .iter()
            .map(|request| request.id().clone())
            .collect::<Vec<_>>()
    };
    let pending_before = pending_ids(
        harness
            .outgoing
            .pending_requests_for_thread(predecessor)
            .await,
    );
    assert_eq!(pending_before, vec![request_id.clone()]);
    let snapshot_before = *harness.retention.borrow();

    let refused = crate::request_processors::thread_lifecycle::unload_idle_unretained_thread(
        Arc::clone(&harness.manager),
        Arc::clone(&harness.outgoing),
        Arc::clone(&harness.pending),
        harness.state.clone(),
        harness.thread_watch_manager.clone(),
        predecessor,
        Arc::clone(&harness.thread),
        generation,
        expected,
    )
    .await;

    assert_eq!(
        refused,
        Some(crate::thread_state::RetentionRetirementRefusal::Busy)
    );
    assert_eq!(
        pending_ids(
            harness
                .outgoing
                .pending_requests_for_thread(predecessor)
                .await
        ),
        pending_before,
        "pending request was cancelled"
    );
    assert!(
        matches!(
            response_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ),
        "pending request {request_id:?} was resolved"
    );
    assert!(
        harness.outgoing_rx.try_recv().is_err(),
        "an event was emitted"
    );
    assert_eq!(*harness.retention.borrow(), snapshot_before);
    assert!(!harness.retention.borrow().retiring);
    assert!(!harness.pending.lock().await.contains(&predecessor));
    assert!(
        harness
            .manager
            .list_thread_ids()
            .await
            .contains(&predecessor)
    );
    assert!(order.snapshot().is_empty(), "A stopped");
}

/// Spawns a clear future that reserves A (and, when `admitted`, records the
/// production `begin_work` stage marker), then never returns, so the test can
/// cancel it at a chosen point. Returns once the reservation is held.
async fn spawn_parked_clear(
    harness: &ClearFenceHarness,
    admitted: bool,
) -> tokio::task::JoinHandle<bool> {
    let (reserved_tx, reserved_rx) = tokio::sync::oneshot::channel();
    let state = harness.state.clone();
    let (predecessor, requester) = (harness.predecessor, harness.requester);
    let clear = tokio::spawn(async move {
        let mut authority = state
            .reserve_clear_transition_authority_guarded(predecessor, requester)
            .await
            .expect("clear reserves A");
        if admitted {
            authority.begin_work();
        }
        let _ = reserved_tx.send(());
        std::future::pending::<()>().await;
        authority.release().await
    });
    reserved_rx.await.expect("clear reserved");
    clear
}

/// Pre-work cancellation whose `Drop` finds the manager mutex contended: the
/// drop only flags its own reservation; the next ordinary path holding the
/// mutex (the listener's wake, or an idle claim) consumes it, releases exactly
/// that reservation and re-arms idle unload, so A still unloads.
#[tokio::test]
async fn contended_pre_work_cancel_defers_release_to_next_locked_path() {
    let order = StopOrder::default();
    let mut harness = ClearFenceHarness::start(&order).await;
    let predecessor = harness.predecessor;
    let generation = harness.thread_state.lock().await.listener_generation;
    let clear = spawn_parked_clear(&harness, /*admitted*/ false).await;
    let expected = *harness.retention.borrow();
    assert!(expected.clear_held);

    let hold = harness.state.hold_state_lock_for_test().await;
    clear.abort();
    assert!(clear.await.expect_err("cancelled").is_cancelled());
    assert_eq!(
        hold.clear_flags(predecessor),
        Some((false, true)),
        "Drop deferred the release instead of retaining or releasing"
    );
    drop(hold);

    // Whichever ordinary path takes the mutex next consumes the flag. A
    // claim that gets there first is refused Busy (nothing committed); one
    // that arrives after the listener consumed it sees the moved snapshot.
    let claim = harness
        .state
        .claim_thread_retirement(
            predecessor,
            expected,
            &harness.thread,
            generation,
            Instant::now() + Duration::from_secs(10),
        )
        .await;
    assert!(
        matches!(
            claim,
            Err(crate::thread_state::RetentionRetirementRefusal::Busy
                | crate::thread_state::RetentionRetirementRefusal::Changed)
        ),
        "{:?}",
        claim.map(|_| ())
    );
    assert!(
        harness
            .state
            .held_clear_transition_reservations()
            .await
            .is_empty()
    );
    assert!(!harness.retention.borrow().clear_held);
    harness.expect_unload_after_rearm().await;
    assert_eq!(order.snapshot(), vec!["other"]);
}

/// Admitted (post-`begin_work`) cancellation with the mutex contended at
/// `Drop`: the exact reservation is retained as unproven and the deferred
/// release flag is never set, so no later path releases it.
#[tokio::test]
async fn contended_admitted_cancel_retains_reservation_without_release_flag() {
    let order = StopOrder::default();
    let mut harness = ClearFenceHarness::start(&order).await;
    let predecessor = harness.predecessor;
    let clear = spawn_parked_clear(&harness, /*admitted*/ true).await;

    let hold = harness.state.hold_state_lock_for_test().await;
    clear.abort();
    assert!(clear.await.expect_err("cancelled").is_cancelled());
    assert_eq!(hold.clear_flags(predecessor), Some((true, false)));
    drop(hold);

    assert!(
        !harness
            .state
            .consume_pending_clear_release(predecessor)
            .await
    );
    harness.assert_claim_busy().await;
    harness
        .assert_not_retired_after(Duration::from_secs(5), &order)
        .await;
    assert_eq!(
        harness.state.held_clear_transition_reservations().await,
        vec![(predecessor, true)]
    );
}

/// A stale pre-work flag on an older reservation's token cannot release a
/// newer reservation of the same thread: release and consumption are keyed to
/// the exact reservation token.
#[tokio::test]
async fn stale_pre_work_release_flag_cannot_release_newer_reservation() {
    let order = StopOrder::default();
    let mut harness = ClearFenceHarness::start(&order).await;
    let (predecessor, requester) = (harness.predecessor, harness.requester);
    let stale = harness
        .state
        .reserve_clear_transition_authority_guarded(predecessor, requester)
        .await
        .expect("first clear reserves A");
    // The first reservation is removed out from under its still-armed guard,
    // and a newer clear reserves the same entry.
    assert!(
        harness
            .state
            .release_clear_transition_authority(predecessor)
            .await
    );
    let _newer = harness
        .state
        .reserve_clear_transition_authority_guarded(predecessor, requester)
        .await
        .expect("newer clear reserves A");

    // The stale guard drops pre-work while the mutex is contended: its flag
    // lands on its own (orphaned) token only.
    let hold = harness.state.hold_state_lock_for_test().await;
    drop(stale);
    assert_eq!(hold.clear_flags(predecessor), Some((false, false)));
    drop(hold);

    assert!(
        !harness
            .state
            .consume_pending_clear_release(predecessor)
            .await
    );
    harness.assert_claim_busy().await;
    harness
        .assert_not_retired_after(Duration::from_secs(5), &order)
        .await;
    assert_eq!(
        harness.state.held_clear_transition_reservations().await,
        vec![(predecessor, false)]
    );
}
