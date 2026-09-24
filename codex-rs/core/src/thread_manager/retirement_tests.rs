use super::*;
use pretty_assertions::assert_eq;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

#[tokio::test]
async fn cancelled_constructor_observer_preserves_original_until_drain() {
    let owner = ThreadConstructions::default();
    let resource = Arc::new(());
    let weak = Arc::downgrade(&resource);
    let entered = Arc::new(AtomicUsize::new(0));
    let (release, held) = oneshot::channel();
    let count = Arc::clone(&entered);
    let mut observer = owner
        .ticket()
        .register(move |_, _| async move {
            let _resource = resource;
            count.fetch_add(1, Ordering::SeqCst);
            held.await.expect("release constructor");
            Err(CodexErr::InternalAgentDied)
        })
        .expect("admit");
    assert!(futures::poll!(observer.as_mut()).is_pending());
    drop(observer);
    assert!(weak.upgrade().is_some());
    owner.close();
    assert!(
        owner
            .ticket()
            .register(|_, _| async { panic!("closed factory ran") })
            .is_err()
    );
    release.send(()).expect("original retained");
    assert_eq!(
        owner
            .drain_until(Instant::now() + Duration::from_secs(1))
            .await,
        ConstructionDrain {
            finished: true,
            panicked: false,
            unavailable: false,
            unpublished: 0,
            sessions: vec![],
        }
    );
    assert_eq!(entered.load(Ordering::SeqCst), 1);
    assert!(weak.upgrade().is_none());
    assert!(owner.state.lock().unwrap().constructions.is_empty());
}

#[tokio::test(start_paused = true)]
async fn expired_original_deadline_never_resumes_constructor() {
    let owner = ThreadConstructions::default();
    let count = Arc::new(AtomicUsize::new(0));
    let invoked = Arc::clone(&count);
    let observer = owner
        .ticket()
        .register(move |_, _| async move {
            invoked.fetch_add(1, Ordering::SeqCst);
            Err(CodexErr::InternalAgentDied)
        })
        .expect("admit");
    drop(observer);
    let deadline = Instant::now();
    let expected = ConstructionDrain {
        finished: false,
        panicked: false,
        unavailable: false,
        unpublished: 0,
        sessions: vec![],
    };
    assert_eq!(owner.drain_until(deadline).await, expected);
    assert_eq!(
        owner.drain_until(deadline + Duration::from_secs(30)).await,
        expected
    );
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert_eq!(owner.state.lock().unwrap().constructions.len(), 1);
}

#[tokio::test]
async fn constructor_panic_is_sticky_and_dead_ticket_refuses_birth() {
    let owner = ThreadConstructions::default();
    let ticket = owner.ticket();
    let observer = ticket
        .register(|_, _| async { panic!("constructor panic") })
        .expect("admit");
    assert!(observer.await.is_err());
    let deadline = Instant::now() + Duration::from_secs(1);
    let expected = ConstructionDrain {
        finished: true,
        panicked: true,
        unavailable: false,
        unpublished: 0,
        sessions: vec![],
    };
    assert_eq!(owner.drain_until(deadline).await, expected);
    assert_eq!(owner.drain_until(deadline).await, expected);
    drop(owner);
    assert!(
        ticket
            .register(|_, _| async { panic!("expired factory ran") })
            .is_err()
    );
}

#[tokio::test]
async fn completed_constructor_compaction_preserves_prior_panic() {
    let owner = ThreadConstructions::default();
    assert!(
        owner
            .ticket()
            .register(|_, _| async { panic!("first constructor") })
            .unwrap()
            .await
            .is_err()
    );
    for _ in 0..100 {
        assert!(
            owner
                .ticket()
                .register(|_, _| async { Err(CodexErr::InternalAgentDied) })
                .unwrap()
                .await
                .is_err()
        );
        assert_eq!(owner.state.lock().unwrap().constructions.len(), 1);
    }
    assert_eq!(
        owner
            .drain_until(Instant::now() + Duration::from_secs(1))
            .await,
        ConstructionDrain {
            finished: true,
            panicked: true,
            unavailable: false,
            unpublished: 0,
            sessions: vec![],
        }
    );
}

#[tokio::test]
async fn failed_required_mcp_start_keeps_unpublished_session_until_cleanup() {
    use crate::config::ConfigBuilder;
    use crate::thread_manager::StartThreadOptions;
    use crate::thread_manager::ThreadManager;
    let home = tempfile::tempdir().unwrap();
    let mut config = ConfigBuilder::default()
        .codex_home(home.path().to_path_buf())
        .fallback_cwd(Some(home.path().to_path_buf()))
        .build()
        .await
        .unwrap();
    config.ephemeral = true;
    let missing_command = home.path().join("missing-mcp-server");
    let server: codex_config::McpServerConfig = toml::from_str(&format!(
        "command = {}\nrequired = true\nstartup_timeout_sec = 2\n",
        toml::Value::String(missing_command.to_string_lossy().into_owned()),
    ))
    .unwrap();
    config
        .mcp_servers
        .set(std::collections::HashMap::from([(
            "required".to_owned(),
            server,
        )]))
        .unwrap();
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        codex_login::CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    assert!(
        manager
            .start_thread(StartThreadOptions::new(config))
            .await
            .is_err()
    );
    assert!(manager.list_thread_ids().await.is_empty());
    let startup = Arc::clone(&manager.constructions.state.lock().unwrap().constructions[0].startup);
    assert!(
        !startup.is_empty(),
        "failed startup must retain the unpublished Session"
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    let report = manager.constructions.drain_until(deadline).await;
    assert_eq!(
        report,
        ConstructionDrain {
            finished: true,
            panicked: false,
            unavailable: false,
            unpublished: 1,
            sessions: vec![StartupCleanup::BeforeLoop(
                crate::session::retirement::CleanupExecution::McpFailed
            )],
        }
    );
    assert!(
        !startup.is_empty(),
        "failed MCP proof must retain session custody"
    );
    assert_eq!(manager.constructions.drain_until(deadline).await, report);
}

#[tokio::test]
async fn real_constructor_cancelled_before_publication_is_drained_after_close() {
    use crate::config::ConfigBuilder;
    use crate::thread_manager::StartThreadOptions;
    use crate::thread_manager::ThreadManager;
    struct HeldInstructions {
        entered: AtomicUsize,
        receiver: Mutex<Option<oneshot::Receiver<()>>>,
    }
    impl codex_extension_api::UserInstructionsProvider for HeldInstructions {
        fn load_user_instructions(&self) -> codex_extension_api::LoadInstructionsFuture<'_> {
            let receiver = self.receiver.lock().unwrap().take().expect("one load");
            self.entered.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                receiver.await.expect("release instructions");
                codex_extension_api::LoadedUserInstructions::default()
            })
        }
    }
    let home = tempfile::tempdir().unwrap();
    let mut config = ConfigBuilder::default()
        .codex_home(home.path().to_path_buf())
        .fallback_cwd(Some(home.path().to_path_buf()))
        .build()
        .await
        .unwrap();
    config.ephemeral = true;
    let mut manager = ThreadManager::with_models_provider_and_home_for_tests(
        codex_login::CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let (release, receiver) = oneshot::channel();
    let provider = Arc::new(HeldInstructions {
        entered: AtomicUsize::new(0),
        receiver: Mutex::new(Some(receiver)),
    });
    Arc::get_mut(&mut manager.state)
        .expect("builders retain unique State")
        .user_instructions_provider = provider.clone();
    let mut observer = Box::pin(manager.start_thread(StartThreadOptions::new(config.clone())));
    assert!(futures::poll!(observer.as_mut()).is_pending());
    assert_eq!(provider.entered.load(Ordering::SeqCst), 1);
    drop(observer);
    manager.constructions.close();
    assert!(
        manager
            .start_thread(StartThreadOptions::new(config))
            .await
            .is_err()
    );
    assert!(manager.list_thread_ids().await.is_empty());
    release
        .send(())
        .expect("retained constructor still waiting");
    let deadline = Instant::now() + Duration::from_secs(20);
    let report = manager.constructions.drain_until(deadline).await;
    assert_eq!(
        report,
        ConstructionDrain {
            finished: true,
            panicked: false,
            unavailable: false,
            unpublished: 0,
            sessions: vec![StartupCleanup::Loop(
                crate::codex_thread::ThreadRetirementReport {
                    ordinary: crate::codex_thread::ThreadShutdownOutcome::Complete,
                    session_loop: crate::codex_thread::ThreadLoopOutcome::Normal,
                    cleanup: crate::codex_thread::ThreadCleanupOutcome::Finished {
                        persistence_failed: false
                    },
                }
            )],
        }
    );
    assert!(
        manager.list_thread_ids().await.is_empty(),
        "cancelled caller must not publish a successor"
    );
    assert_eq!(manager.constructions.drain_until(deadline).await, report);
    assert_eq!(provider.entered.load(Ordering::SeqCst), 1);
}
