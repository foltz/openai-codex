use super::*;
use crate::config::Config;
use crate::config::ConfigBuilder;
use crate::session::SessionLoopOutcome;
use crate::thread_manager::StartThreadOptions;
use crate::thread_manager::ThreadManager;
use pretty_assertions::assert_eq;
use std::time::Duration;
use tokio::time::Instant;

async fn manager() -> (tempfile::TempDir, ThreadManager, Config) {
    let home = tempfile::tempdir().unwrap();
    let mut config = ConfigBuilder::default()
        .codex_home(home.path().to_path_buf())
        .fallback_cwd(Some(home.path().to_path_buf()))
        .build()
        .await
        .unwrap();
    config.ephemeral = true;
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        codex_login::CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    (home, manager, config)
}

#[tokio::test]
async fn removed_runtime_stays_owned_until_exact_cleanup_completes() {
    let (_home, manager, config) = manager().await;
    let started = manager
        .start_thread(StartThreadOptions::new(config))
        .await
        .unwrap();
    let weak = Arc::downgrade(&started.thread);
    let id = started.thread_id;
    drop(manager.remove_thread(&id).await);
    drop(started);
    assert!(manager.list_thread_ids().await.is_empty());
    assert!(weak.upgrade().is_some(), "lookup removal is not retirement");
    let population = manager.constructions.published();
    assert_eq!(population.len(), 1);
    let ticket = population[0]
        .begin_retirement(Instant::now() + Duration::from_secs(20))
        .unwrap();
    let report = ticket.wait().await;
    assert_eq!(
        report.cleanup,
        crate::ThreadCleanupOutcome::Finished {
            persistence_failed: false
        }
    );
    assert_eq!(report.session_loop, crate::ThreadLoopOutcome::Normal);
    drop(population);
    assert!(manager.constructions.published().is_empty());
    assert!(weak.upgrade().is_none());
    assert_eq!(manager.constructions.state.lock().unwrap().compacted, 1);
}

#[tokio::test]
async fn loop_termination_alone_cannot_compact_removed_runtime() {
    let (_home, manager, config) = manager().await;
    let started = manager
        .start_thread(StartThreadOptions::new(config))
        .await
        .unwrap();
    started.thread.io.session_loop_termination.request_abort();
    assert_eq!(
        started.thread.io.session_loop_termination.clone().await,
        SessionLoopOutcome::Cancelled,
    );
    let weak = Arc::downgrade(&started.thread);
    drop(manager.remove_thread(&started.thread_id).await);
    drop(started);
    let population = manager.constructions.published();
    assert_eq!(population.len(), 1);
    assert!(population[0].observed_terminal_cleanup().is_none());
    let report = population[0]
        .begin_retirement(Instant::now() + Duration::from_secs(20))
        .unwrap()
        .wait()
        .await;
    assert_eq!(
        report.cleanup,
        crate::ThreadCleanupOutcome::Finished {
            persistence_failed: false
        }
    );
    assert_eq!(report.session_loop, crate::ThreadLoopOutcome::Cancelled);
    drop(population);
    assert!(manager.constructions.published().is_empty());
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn prior_legacy_cleanup_compacts_without_rebinding_or_reexecution() {
    let (_home, manager, config) = manager().await;
    for _ in 0..6 {
        let started = manager
            .start_thread(StartThreadOptions::new(config.clone()))
            .await
            .unwrap();
        assert_eq!(manager.constructions.published().len(), 1);
        started.thread.shutdown_and_wait().await.unwrap();
        assert!(matches!(
            started
                .thread
                .begin_retirement(Instant::now() + Duration::from_secs(20)),
            Err(crate::ThreadRetirementError::LegacyCleanupStarted),
        ));
        assert_eq!(
            started.thread.observed_terminal_cleanup(),
            Some(SessionLoopOutcome::Normal)
        );
        let weak = Arc::downgrade(&started.thread);
        drop(manager.remove_thread(&started.thread_id).await);
        drop(started);
        assert!(manager.constructions.published().is_empty());
        assert!(weak.upgrade().is_none());
    }
    assert_eq!(manager.constructions.state.lock().unwrap().compacted, 6);
}

#[tokio::test]
async fn managed_lifetime_cleanup_compacts_before_manager_shutdown() {
    let (_home, manager, config) = manager().await;
    let manager = Arc::new(manager);
    let stop = tokio_util::sync::CancellationToken::new();
    let tasks = tokio_util::task::TaskTracker::new();
    let mut options = StartThreadOptions::new(config);
    options.thread_extension_init.insert(codex_extension_api::SessionIsolation::Isolated);
    let started = manager.start_thread_until(options, stop.clone().cancelled_owned(), &tasks)
        .await.unwrap();
    assert_eq!(manager.constructions.published().len(), 1);
    stop.cancel();
    tasks.close();
    tokio::time::timeout(Duration::from_secs(20), tasks.wait()).await
        .expect("managed lifetime cleanup joined");
    assert_eq!(started.thread.observed_terminal_cleanup(), Some(SessionLoopOutcome::Normal));
    assert!(manager.list_thread_ids().await.is_empty());
    assert!(manager.constructions.published().is_empty());
    assert_eq!(manager.constructions.state.lock().unwrap().compacted, 1);
    let report = manager.begin_shutdown(Instant::now() + Duration::from_secs(5))
        .unwrap().wait().await.unwrap();
    assert!(report.is_complete());
    assert_eq!(manager.constructions.state.lock().unwrap().compacted, 1);
}

#[tokio::test]
async fn failed_cleanup_remains_owned_after_loop_join() {
    struct PanicOnStop;
    impl codex_extension_api::ThreadLifecycleContributor<Config> for PanicOnStop {
        fn on_thread_stop<'a>(
            &'a self,
            _input: codex_extension_api::ThreadStopInput<'a>,
        ) -> codex_extension_api::ExtensionFuture<'a, ()> {
            Box::pin(async { panic!("cleanup failure fixture") })
        }
    }
    let (_home, mut manager, config) = manager().await;
    let mut extensions = codex_extension_api::ExtensionRegistryBuilder::new();
    extensions.thread_lifecycle_contributor(Arc::new(PanicOnStop));
    Arc::get_mut(&mut manager.state).unwrap().extensions = Arc::new(extensions.build());
    let started = manager
        .start_thread(StartThreadOptions::new(config))
        .await
        .unwrap();
    assert!(started.thread.shutdown_and_wait().await.is_err());
    assert_eq!(
        started.thread.io.session_loop_termination.clone().await,
        SessionLoopOutcome::Panicked
    );
    let weak = Arc::downgrade(&started.thread);
    drop(manager.remove_thread(&started.thread_id).await);
    drop(started);
    assert_eq!(manager.constructions.published().len(), 1);
    assert!(weak.upgrade().is_some());
    assert_eq!(manager.constructions.state.lock().unwrap().compacted, 0);
}

#[tokio::test]
async fn managed_legacy_cleanup_in_progress_is_retained_as_incomplete() {
    struct HeldStop {
        entered: tokio::sync::Notify,
        release: tokio::sync::Semaphore,
        calls: std::sync::atomic::AtomicUsize,
    }
    impl codex_extension_api::ThreadLifecycleContributor<Config> for HeldStop {
        fn on_thread_stop<'a>(&'a self, _input: codex_extension_api::ThreadStopInput<'a>)
            -> codex_extension_api::ExtensionFuture<'a, ()> {
            Box::pin(async {
                self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                self.entered.notify_one();
                self.release.acquire().await.expect("release cleanup").forget();
            })
        }
    }
    let (_home, mut manager, config) = manager().await;
    let held = Arc::new(HeldStop { entered: tokio::sync::Notify::new(),
        release: tokio::sync::Semaphore::new(0), calls: std::sync::atomic::AtomicUsize::new(0) });
    let mut extensions = codex_extension_api::ExtensionRegistryBuilder::new();
    extensions.thread_lifecycle_contributor(held.clone());
    Arc::get_mut(&mut manager.state).unwrap().extensions = Arc::new(extensions.build());
    let manager = Arc::new(manager);
    let stop = tokio_util::sync::CancellationToken::new();
    let tasks = tokio_util::task::TaskTracker::new();
    let mut options = StartThreadOptions::new(config);
    options.thread_extension_init.insert(codex_extension_api::SessionIsolation::Isolated);
    let started = manager.start_thread_until(options, stop.clone().cancelled_owned(), &tasks)
        .await.unwrap();
    stop.cancel();
    tasks.close();
    tokio::time::timeout(Duration::from_secs(20), held.entered.notified()).await
        .expect("legacy cleanup entered extension stop");
    assert!(started.thread.observed_terminal_cleanup().is_none());
    assert!(matches!(started.thread.begin_retirement(Instant::now() + Duration::from_secs(5)),
        Err(crate::ThreadRetirementError::LegacyCleanupStarted)));
    let report = manager.begin_shutdown(Instant::now() + Duration::from_secs(5))
        .unwrap().wait().await.unwrap();
    assert!(!report.is_complete());
    assert_eq!(manager.constructions.published().len(), 1);
    held.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), tasks.wait()).await.expect("cleanup joins");
    assert_eq!(started.thread.observed_terminal_cleanup(), Some(SessionLoopOutcome::Normal));
    assert!(manager.constructions.published().is_empty());
    assert_eq!(manager.constructions.state.lock().unwrap().compacted, 1);
    assert_eq!(held.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn constructor_admitted_before_close_cannot_publish_after_close() {
    struct HeldInstructions(Mutex<Option<tokio::sync::oneshot::Receiver<()>>>);
    impl codex_extension_api::UserInstructionsProvider for HeldInstructions {
        fn load_user_instructions(&self) -> codex_extension_api::LoadInstructionsFuture<'_> {
            let held = self.0.lock().unwrap().take().unwrap();
            Box::pin(async move {
                held.await.unwrap();
                codex_extension_api::LoadedUserInstructions::default()
            })
        }
    }
    let (_home, mut manager, config) = manager().await;
    let (release, held) = tokio::sync::oneshot::channel();
    Arc::get_mut(&mut manager.state)
        .unwrap()
        .user_instructions_provider = Arc::new(HeldInstructions(Mutex::new(Some(held))));
    let mut start = Box::pin(manager.start_thread(StartThreadOptions::new(config)));
    assert!(futures::poll!(start.as_mut()).is_pending());
    manager.constructions.close();
    release.send(()).unwrap();
    assert!(
        start.await.is_err(),
        "live caller cannot publish beyond the close gate"
    );
    assert!(manager.list_thread_ids().await.is_empty());
    assert!(manager.constructions.published().is_empty());
    let report = manager
        .constructions
        .drain_until(Instant::now() + Duration::from_secs(20))
        .await;
    assert!(report.finished);
    assert_eq!(report.unpublished, 0);
    assert_eq!(report.sessions.len(), 1);
    assert!(
        report.sessions[0].is_complete(),
        "the refused publication still requires actual cleanup"
    );
}
