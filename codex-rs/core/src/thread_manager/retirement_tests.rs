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
        .register(/*account_work*/ None, move |_, _| async move {
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
            .register(/*account_work*/ None, |_, _| async { panic!("closed factory ran") })
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
        .register(/*account_work*/ None, move |_, _| async move {
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
        .register(/*account_work*/ None, |_, _| async { panic!("constructor panic") })
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
            .register(/*account_work*/ None, |_, _| async { panic!("expired factory ran") })
            .is_err()
    );
}

#[tokio::test]
async fn completed_constructor_compaction_preserves_prior_panic() {
    let owner = ThreadConstructions::default();
    assert!(
        owner
            .ticket()
            .register(/*account_work*/ None, |_, _| async { panic!("first constructor") })
            .unwrap()
            .await
            .is_err()
    );
    assert!(!owner.retire_completed_startup_until(Instant::now() + Duration::from_secs(1)).await);
    for _ in 0..100 {
        assert!(
            owner
                .ticket()
                .register(/*account_work*/ None, |_, _| async { Err(CodexErr::InternalAgentDied) })
                .unwrap()
                .await
                .is_err()
        );
        assert_eq!(owner.state.lock().unwrap().constructions.len(), 1);
    }
    assert!(owner.retire_completed_startup_until(Instant::now() + Duration::from_secs(1)).await);
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
            .start_thread(StartThreadOptions::new(config, None))
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
    #[derive(Debug, Default)]
    struct RequestHost {
        request_alive: std::sync::atomic::AtomicBool,
        operations: AtomicUsize,
        released: tokio::sync::Notify,
    }
    #[derive(Debug)]
    struct ConstructionWork(Arc<RequestHost>);
    impl Drop for ConstructionWork {
        fn drop(&mut self) {
            self.0.operations.fetch_sub(1, Ordering::SeqCst);
            self.0.released.notify_one();
        }
    }
    impl codex_extension_api::HostOperationWork for ConstructionWork {
        fn derive_operation(&self) -> Result<Box<dyn codex_extension_api::HostOperationWork>, codex_extension_api::TurnWorkRefused> {
            // Existing work may derive descendants after the request ends.
            self.0.operations.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(Self(Arc::clone(&self.0))))
        }
        fn derive_turn_work(
            &self,
            _: &codex_extension_api::ExtensionData,
            _: codex_extension_api::ExtensionFuture<'static, ()>,
        ) -> Result<Box<dyn codex_protocol::host_turn_work::HostTurnWork>, codex_extension_api::TurnWorkRefused> {
            Err(codex_extension_api::TurnWorkRefused::Unavailable)
        }
    }
    #[derive(Debug)]
    struct ConstructionAdmission(Arc<RequestHost>);
    impl codex_extension_api::TurnStartAdmission for ConstructionAdmission {
        fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
            Some(Box::new(()))
        }
        fn derive_request_operation_work(&self) -> Result<Option<Box<dyn codex_extension_api::HostOperationWork>>, codex_extension_api::TurnWorkRefused> {
            if !self.0.request_alive.load(Ordering::SeqCst) {
                return Err(codex_extension_api::TurnWorkRefused::Unavailable);
            }
            self.0.operations.fetch_add(1, Ordering::SeqCst);
            Ok(Some(Box::new(ConstructionWork(Arc::clone(&self.0)))))
        }
    }
    struct HeldInstructions {
        entered: AtomicUsize,
        entry: tokio::sync::Notify,
        receiver: Mutex<Option<oneshot::Receiver<()>>>,
    }
    impl codex_extension_api::UserInstructionsProvider for HeldInstructions {
        fn load_user_instructions(&self) -> codex_extension_api::LoadInstructionsFuture<'_> {
            let receiver = self.receiver.lock().unwrap().take().expect("one load");
            self.entered.fetch_add(1, Ordering::SeqCst);
            self.entry.notify_one();
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
        entry: tokio::sync::Notify::new(),
        receiver: Mutex::new(Some(receiver)),
    });
    Arc::get_mut(&mut manager.state)
        .expect("builders retain unique State")
        .user_instructions_provider = provider.clone();
    let host = Arc::new(RequestHost::default());
    host.request_alive.store(true, Ordering::SeqCst);
    let mut extensions = codex_extension_api::ExtensionRegistryBuilder::new();
    extensions.turn_start_admission(Arc::new(ConstructionAdmission(Arc::clone(&host))));
    Arc::get_mut(&mut manager.state).unwrap().extensions = Arc::new(extensions.build());
    let mut observer = Box::pin(manager.start_thread(StartThreadOptions::new(config.clone(), /*control_endpoint*/ None)));
    // Startup now has asynchronous work before loading instruction providers.
    // Drive the real constructor to the held boundary rather than assuming
    // that its first poll reaches it.
    tokio::time::timeout(Duration::from_secs(20), async {
        tokio::select! {
            _ = provider.entry.notified() => {}
            _ = observer.as_mut() => panic!("constructor returned before instructions were released"),
        }
    })
    .await
    .expect("constructor reaches held instructions");
    assert_eq!(provider.entered.load(Ordering::SeqCst), 1);
    drop(observer);
    host.request_alive.store(false, Ordering::SeqCst);
    assert_eq!(host.operations.load(Ordering::SeqCst), 2);
    assert!(manager.list_thread_ids().await.is_empty());
    release
        .send(())
        .expect("retained constructor still waiting");
    // Account progress must finish counted construction without initiating
    // thread shutdown, which only happens after account adoption.
    tokio::time::timeout(Duration::from_secs(20), async {
        let progress = manager.drive_admitted_constructions();
        tokio::pin!(progress);
        while host.operations.load(Ordering::SeqCst) != 0 {
            tokio::select! {
                _ = host.released.notified() => {},
                _ = &mut progress => panic!("progress observer stopped"),
            }
        }
    })
        .await
        .expect("abandoned constructor completes before retirement");
    assert_eq!(host.operations.load(Ordering::SeqCst), 0);
    let startup = Arc::clone(&manager.constructions.state.lock().unwrap().constructions[0].startup);
    assert!(!startup.is_empty(), "unpublished runtime remains owned until reset");
    assert!(manager.shutdown_unpublished_constructions_bounded(Duration::from_secs(20)).await);
    assert!(startup.is_empty(), "reset retires the runtime outside the lookup map");
    manager.constructions.close();
    assert!(manager.start_thread(StartThreadOptions::new(config, /*control_endpoint*/ None)).await.is_err());
    let deadline = Instant::now() + Duration::from_secs(20);
    let report = manager.constructions.drain_until(deadline).await;
    assert_eq!(
        report,
        ConstructionDrain {
            finished: true,
            panicked: false,
            unavailable: false,
            unpublished: 0,
            sessions: vec![],
        }
    );
    assert!(
        manager.list_thread_ids().await.is_empty(),
        "cancelled caller must not publish a successor"
    );
    assert_eq!(manager.constructions.drain_until(deadline).await, report);
    assert_eq!(provider.entered.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn managed_cancellation_terminates_constructor_before_tracker_join() {
    use crate::config::ConfigBuilder;
    use crate::thread_manager::StartThreadOptions;
    use crate::thread_manager::ThreadManager;
    let server = wiremock::MockServer::start().await;
    let entered = Arc::new(tokio::sync::Notify::new());
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .respond_with({ let entered = Arc::clone(&entered); move |_: &wiremock::Request| {
            entered.notify_one();
            wiremock::ResponseTemplate::new(200).set_delay(Duration::from_secs(120))
        } }).mount(&server).await;
    let home = tempfile::tempdir().unwrap();
    let mut config = ConfigBuilder::default()
        .codex_home(home.path().to_path_buf())
        .fallback_cwd(Some(home.path().to_path_buf()))
        .build().await.unwrap();
    config.ephemeral = true;
    config.mcp_servers.set(std::collections::HashMap::from([("held".to_owned(),
        serde_json::from_value(serde_json::json!({ "url": server.uri(), "required": true,
            "startup_timeout_sec": 120, "http_headers": {"Authorization": "Bearer fixture"} }))
            .expect("MCP fixture config"))])).unwrap();
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        codex_login::CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(), config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let manager = Arc::new(manager);
    let mut options = StartThreadOptions::new(config, /*control_endpoint*/ None);
    options.thread_extension_init.insert(codex_extension_api::SessionIsolation::Isolated);
    let tasks = tokio_util::task::TaskTracker::new();
    let mut start = Box::pin(manager.start_thread_until(options, std::future::pending(), &tasks));
    tokio::time::timeout(Duration::from_secs(20), async {
        tokio::select! {
            _ = entered.notified() => {},
            _ = start.as_mut() => panic!("constructor returned before release"),
        }
    }).await.expect("constructor reaches held MCP initialize");
    drop(start);
    tasks.close();
    tokio::time::timeout(Duration::from_secs(5), tasks.wait()).await
        .expect("managed lifetime joins without releasing MCP initialize");
    // Inspect before driving the drain: a frozen constructor would have no
    // terminal value even if some independent cleanup path had finished.
    assert_eq!(manager.constructions.state.lock().unwrap().constructions.iter()
        .map(|entry| entry.completion.peek().copied()).collect::<Vec<_>>(),
        vec![Some(ConstructionOutcome::Returned)]);
    assert!(manager.list_thread_ids().await.is_empty());
    assert_eq!(manager.constructions.drain_until(Instant::now() + Duration::from_secs(1)).await,
        ConstructionDrain { finished: true, panicked: false, unavailable: false,
            unpublished: 0, sessions: vec![] });
}
