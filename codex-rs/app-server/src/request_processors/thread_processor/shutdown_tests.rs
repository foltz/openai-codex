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
