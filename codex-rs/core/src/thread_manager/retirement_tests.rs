use super::*;
use pretty_assertions::assert_eq;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

// Test-only original-task owner for unrelated constructor fixtures. Work and
// factories carry weak tickets, never the owner whose ledger retains them.
type FixtureAuthResult = std::result::Result<(), codex_protocol::host_auth_work::AuthTaskFailure>;
type FixtureAuthReceipt =
    futures::future::Shared<futures::future::BoxFuture<'static, FixtureAuthResult>>;
#[derive(Clone)]
struct FixtureAuthEntry {
    receipt: FixtureAuthReceipt,
    abort: tokio::task::AbortHandle,
}
#[derive(Default)]
struct FixtureAuthState {
    originals: Vec<FixtureAuthEntry>,
    failure: Option<codex_protocol::host_auth_work::AuthTaskFailure>,
}
#[derive(Default)]
pub(crate) struct TestAuthTasks {
    state: std::sync::Mutex<FixtureAuthState>,
}
impl std::fmt::Debug for TestAuthTasks {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("TestAuthTasks")
    }
}
impl TestAuthTasks {
    pub(crate) fn spawner(
        self: &Arc<Self>,
        work: Box<dyn codex_extension_api::HostOperationWork>,
    ) -> Arc<dyn codex_protocol::host_auth_work::AuthTaskSpawner> {
        Arc::new(FixtureAuthSpawner {
            owner: Arc::downgrade(self),
            work,
        })
    }
    fn register(
        &self,
        future: codex_protocol::host_auth_work::AuthTaskFuture,
    ) -> std::result::Result<
        codex_protocol::host_auth_work::AuthTaskObservation,
        codex_protocol::host_auth_work::AuthTaskFailure,
    > {
        use codex_protocol::host_auth_work::AuthTaskFailure;
        let mut state = self
            .state
            .lock()
            .map_err(|_| AuthTaskFailure::Unavailable)?;
        let (start, started) = tokio::sync::oneshot::channel();
        let original = tokio::spawn(async move {
            if started.await.is_ok() {
                future.await;
            }
        });
        let abort = original.abort_handle();
        let receipt = futures::FutureExt::shared(futures::FutureExt::boxed(async move {
            match original.await {
                Ok(()) => Ok(()),
                Err(error) if error.is_cancelled() => Err(AuthTaskFailure::Cancelled),
                Err(_) => Err(AuthTaskFailure::Panicked),
            }
        }));
        state.originals.push(FixtureAuthEntry {
            receipt: receipt.clone(),
            abort,
        });
        drop(state);
        // No body can poll before its original is retained by the external owner.
        start.send(()).map_err(|_| AuthTaskFailure::Unavailable)?;
        Ok(Box::pin(receipt))
    }
    pub(crate) async fn drain(&self) -> FixtureAuthResult {
        use codex_protocol::host_auth_work::AuthTaskFailure;
        loop {
            let originals = {
                let state = self
                    .state
                    .lock()
                    .map_err(|_| AuthTaskFailure::Unavailable)?;
                if state.originals.is_empty() {
                    return state.failure.map_or(Ok(()), Err);
                }
                state.originals.clone()
            };
            for original in originals {
                if let Err(reason) = original.receipt.await {
                    self.state
                        .lock()
                        .map_err(|_| AuthTaskFailure::Unavailable)?
                        .failure
                        .get_or_insert(reason);
                }
            }
            // Cancellation of this drain drops only clones, not the original ledger.
            self.state
                .lock()
                .map_err(|_| AuthTaskFailure::Unavailable)?
                .originals
                .retain(|entry| entry.receipt.peek().is_none());
        }
    }
}
struct FixtureAuthSpawner {
    owner: std::sync::Weak<TestAuthTasks>,
    work: Box<dyn codex_extension_api::HostOperationWork>,
}
impl std::fmt::Debug for FixtureAuthSpawner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("FixtureAuthSpawner")
    }
}
impl codex_protocol::host_auth_work::AuthTaskSpawner for FixtureAuthSpawner {
    fn spawn(
        &self,
        future: codex_protocol::host_auth_work::AuthTaskFuture,
    ) -> std::result::Result<
        codex_protocol::host_auth_work::AuthTaskObservation,
        codex_protocol::host_auth_work::AuthTaskFailure,
    > {
        use codex_protocol::host_auth_work::AuthTaskFailure;
        let owner = self.owner.upgrade().ok_or(AuthTaskFailure::Unavailable)?;
        let work = self
            .work
            .derive_operation()
            .map_err(|_| AuthTaskFailure::Refused)?;
        owner.register(Box::pin(async move {
            let _work = work;
            future.await;
        }))
    }
}

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
            .register(
                /*account_work*/ None,
                |_, _| async { panic!("closed factory ran") }
            )
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
        .register(
            /*account_work*/ None,
            |_, _| async { panic!("constructor panic") },
        )
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
            .register(
                /*account_work*/ None,
                |_, _| async { panic!("expired factory ran") }
            )
            .is_err()
    );
}

#[tokio::test]
async fn completed_constructor_compaction_preserves_prior_panic() {
    let owner = ThreadConstructions::default();
    assert!(
        owner
            .ticket()
            .register(
                /*account_work*/ None,
                |_, _| async { panic!("first constructor") }
            )
            .unwrap()
            .await
            .is_err()
    );
    assert!(
        !owner
            .retire_completed_startup_until(Instant::now() + Duration::from_secs(1))
            .await
    );
    for _ in 0..100 {
        assert!(
            owner
                .ticket()
                .register(
                    /*account_work*/ None,
                    |_, _| async { Err(CodexErr::InternalAgentDied) }
                )
                .unwrap()
                .await
                .is_err()
        );
        assert_eq!(owner.state.lock().unwrap().constructions.len(), 1);
    }
    assert!(
        owner
            .retire_completed_startup_until(Instant::now() + Duration::from_secs(1))
            .await
    );
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
async fn managed_cancellation_terminates_constructor_before_tracker_join() {
    use crate::config::ConfigBuilder;
    use crate::thread_manager::StartThreadOptions;
    use crate::thread_manager::ThreadManager;
    let server = wiremock::MockServer::start().await;
    let entered = Arc::new(tokio::sync::Notify::new());
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .respond_with({
            let entered = Arc::clone(&entered);
            move |_: &wiremock::Request| {
                entered.notify_one();
                wiremock::ResponseTemplate::new(200).set_delay(Duration::from_secs(120))
            }
        })
        .mount(&server)
        .await;
    let home = tempfile::tempdir().unwrap();
    let mut config = ConfigBuilder::default()
        .codex_home(home.path().to_path_buf())
        .fallback_cwd(Some(home.path().to_path_buf()))
        .build()
        .await
        .unwrap();
    config.ephemeral = true;
    config
        .mcp_servers
        .set(std::collections::HashMap::from([(
            "held".to_owned(),
            serde_json::from_value(serde_json::json!({ "url": server.uri(), "required": true,
            "startup_timeout_sec": 120, "http_headers": {"Authorization": "Bearer fixture"} }))
            .expect("MCP fixture config"),
        )]))
        .unwrap();
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        codex_login::CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let manager = Arc::new(manager);
    let mut options = StartThreadOptions::new(config, /*control_endpoint*/ None);
    options
        .thread_extension_init
        .insert(codex_extension_api::SessionIsolation::Isolated);
    let tasks = tokio_util::task::TaskTracker::new();
    let mut start = Box::pin(manager.start_thread_until(options, std::future::pending(), &tasks));
    tokio::time::timeout(Duration::from_secs(20), async {
        tokio::select! {
            _ = entered.notified() => {},
            _ = start.as_mut() => panic!("constructor returned before release"),
        }
    })
    .await
    .expect("constructor reaches held MCP initialize");
    drop(start);
    tasks.close();
    tokio::time::timeout(Duration::from_secs(5), tasks.wait())
        .await
        .expect("managed lifetime joins without releasing MCP initialize");
    // Inspect before driving the drain: a frozen constructor would have no
    // terminal value even if some independent cleanup path had finished.
    assert_eq!(
        manager
            .constructions
            .state
            .lock()
            .unwrap()
            .constructions
            .iter()
            .map(|entry| entry.completion.peek().copied())
            .collect::<Vec<_>>(),
        vec![Some(ConstructionOutcome::Returned)]
    );
    assert!(manager.list_thread_ids().await.is_empty());
    assert_eq!(
        manager
            .constructions
            .drain_until(Instant::now() + Duration::from_secs(1))
            .await,
        ConstructionDrain {
            finished: true,
            panicked: false,
            unavailable: false,
            unpublished: 0,
            sessions: vec![]
        }
    );
}

#[tokio::test]
async fn dropped_observer_requests_cancellation_and_registry_records_outcome() {
    let owner = ThreadConstructions::default();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (done_tx, done_rx) = oneshot::channel();
    let mut observer = owner
        .ticket()
        .register(
            /*account_work*/ None,
            move |_, publication| async move {
                let cancelled = publication.observer_cancellation();
                let _ = entered_tx.send(());
                cancelled.cancelled().await;
                let _ = done_tx.send(());
                Err(CodexErr::TurnAborted)
            },
        )
        .expect("admit");
    assert!(futures::poll!(observer.as_mut()).is_pending());
    tokio::time::timeout(Duration::from_secs(5), entered_rx)
        .await
        .expect("bounded")
        .expect("constructor started");
    drop(observer);
    // Nothing here polls the registry's original; only the observer's
    // progress driver can deliver the constructor to its cancellation branch.
    tokio::time::timeout(Duration::from_secs(5), done_rx)
        .await
        .expect("cancellation request reaches the retained constructor")
        .expect("constructor consumed cancellation");
    tokio::time::timeout(Duration::from_secs(5), async {
        while owner.state.lock().unwrap().constructions[0]
            .completion
            .peek()
            .is_none()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("registry records the constructor outcome");
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
}

#[tokio::test(start_paused = true)]
async fn observer_dropped_during_shutdown_cancels_and_cleans_within_deadline() {
    let owner = ThreadConstructions::default();
    let thread_id = codex_protocol::ThreadId::new();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (done_tx, done_rx) = oneshot::channel();
    let mut observer = owner
        .ticket()
        .register(
            /*account_work*/ None,
            move |custody, publication| async move {
                // An acquired, still unpublished pre-session writer.
                drop(custody.retain_blocked_persistence_for_test(thread_id).await);
                let cancelled = publication.observer_cancellation();
                let _ = entered_tx.send(());
                // Same arms as the real constructor: observer cancellation
                // (never after publication) or registry expiry.
                let abandoned = tokio::select! {
                    biased;
                    _ = publication.unless_published(cancelled.cancelled()) => true,
                    _ = publication.expiry(/*local*/ None) => false,
                };
                let cleanup = publication
                    .clamped_cleanup(&custody, Duration::from_secs(30))
                    .await;
                let _ = done_tx.send((abandoned, Instant::now(), cleanup));
                Err(CodexErr::TurnAborted)
            },
        )
        .expect("admit");
    assert!(futures::poll!(observer.as_mut()).is_pending());
    tokio::time::timeout(Duration::from_secs(5), entered_rx)
        .await
        .expect("bounded")
        .expect("constructor started");
    let start = Instant::now();
    let deadline = owner
        .close_until(start + Duration::from_secs(10))
        .expect("close");
    // The client disconnects mid-start after shutdown closed admission.
    drop(observer);
    let token = owner.state.lock().unwrap().constructions[0]
        .publication
        .observer_cancellation();
    assert!(
        token.is_cancelled(),
        "departure after close still requests constructor cancellation"
    );
    let expected = ConstructionDrain {
        finished: true,
        panicked: false,
        unavailable: false,
        unpublished: 0,
        sessions: vec![StartupCleanup::BeforeLoop(
            crate::session::retirement::CleanupExecution::Finished {
                persistence_failed: false,
            },
        )],
    };
    assert_eq!(owner.drain_until(deadline).await, expected);
    let (abandoned, cleaned_at, cleanup) = done_rx.await.expect("constructor returned");
    assert!(abandoned, "stopped by cancellation, not by registry expiry");
    assert_eq!(cleanup, Some(expected.sessions[0]));
    assert!(cleaned_at < deadline, "clean receipt before the deadline");
    assert!(
        Instant::now() < deadline,
        "the drain did not wait for expiry"
    );
    // Cancellation neither extended nor replaced the latched deadline.
    assert_eq!(
        owner
            .close_until(deadline + Duration::from_secs(60))
            .expect("close"),
        deadline
    );
    assert_eq!(
        owner.drain_until(deadline + Duration::from_secs(60)).await,
        expected
    );
}

#[tokio::test(start_paused = true)]
async fn never_polled_observer_drop_requests_cancellation_without_driving() {
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
    let (completion, publication) = {
        let state = owner.state.lock().unwrap();
        let entry = &state.constructions[0];
        (entry.completion.clone(), entry.publication.clone())
    };
    assert!(
        publication.observer_cancellation().is_cancelled(),
        "a never-polled start is still asked to cancel"
    );
    // No progress observer was spawned: nothing starts the constructor.
    tokio::time::advance(Duration::from_secs(60)).await;
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert!(completion.peek().is_none());
    assert!(!publication.started.load(Ordering::Acquire));
    assert_eq!(count.load(Ordering::SeqCst), 0);
    // An expired drain refuses it without a termination poll.
    let expected = ConstructionDrain {
        finished: false,
        panicked: false,
        unavailable: false,
        unpublished: 0,
        sessions: vec![],
    };
    assert_eq!(owner.drain_until(Instant::now()).await, expected);
    assert!(completion.peek().is_none());
    assert_eq!(count.load(Ordering::SeqCst), 0);
}

// Semantic correction (startup decision #1): this fixture used to assert that
// a departure after close left the token uncancelled. Departure now always
// requests cancellation; only the progress observer is withheld after close,
// so the drain still owns driving the constructor.
#[tokio::test]
async fn dropped_observer_after_close_cancels_and_leaves_driving_to_the_drain() {
    let owner = ThreadConstructions::default();
    let (token_tx, token_rx) = oneshot::channel();
    let (release, held) = oneshot::channel::<()>();
    let mut observer = owner
        .ticket()
        .register(
            /*account_work*/ None,
            move |_, publication| async move {
                let _ = token_tx.send(publication.observer_cancellation());
                held.await.expect("release constructor");
                Err(CodexErr::InternalAgentDied)
            },
        )
        .expect("admit");
    assert!(futures::poll!(observer.as_mut()).is_pending());
    let token = tokio::time::timeout(Duration::from_secs(5), token_rx)
        .await
        .expect("bounded")
        .expect("constructor started");
    owner.close();
    drop(observer);
    assert!(
        token.is_cancelled(),
        "departure after close still requests constructor cancellation"
    );
    release.send(()).expect("constructor still retained");
    // No progress observer after close: the manager drain drives it.
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert!(
        owner.state.lock().unwrap().constructions[0]
            .completion
            .peek()
            .is_none(),
        "after close only the manager drain drives the constructor"
    );
    assert!(
        owner
            .drain_until(Instant::now() + Duration::from_secs(1))
            .await
            .finished
    );
}

#[tokio::test(start_paused = true)]
async fn constructor_cleanup_is_clamped_to_a_later_shorter_manager_deadline() {
    let owner = ThreadConstructions::default();
    let thread_id = codex_protocol::ThreadId::new();
    let (held_tx, held_rx) = oneshot::channel();
    let (done_tx, done_rx) = oneshot::channel();
    let mut observer = owner
        .ticket()
        .register(
            /*account_work*/ None,
            move |custody, publication| async move {
                // A synchronous thread holds the acquisition lock, so no async
                // body keeps the guard across an await.
                let held = AcquisitionHolder::hold(
                    custody.retain_blocked_persistence_for_test(thread_id).await,
                );
                let _ = held_tx.send(held);
                // Same helper the real constructor uses, with its 30s budget.
                let cleanup = publication
                    .clamped_cleanup(&custody, Duration::from_secs(30))
                    .await;
                let _ = done_tx.send((Instant::now(), cleanup));
                Err(CodexErr::TurnAborted)
            },
        )
        .expect("admit");
    assert!(futures::poll!(observer.as_mut()).is_pending());
    let held = tokio::time::timeout(Duration::from_secs(5), held_rx)
        .await
        .expect("bounded")
        .expect("acquisition retained");
    assert!(futures::poll!(observer.as_mut()).is_pending());
    let start = Instant::now();
    let deadline = owner
        .close_until(start + Duration::from_secs(1))
        .expect("close");
    assert_eq!(deadline, start + Duration::from_secs(1));
    // The live observer keeps polling; the constructor must still stop at 1s.
    assert!(
        tokio::time::timeout(Duration::from_secs(5), observer)
            .await
            .expect("bounded")
            .is_err()
    );
    let (stopped_at, cleanup) = tokio::time::timeout(Duration::from_secs(5), done_rx)
        .await
        .expect("bounded")
        .expect("constructor returned");
    assert_eq!(cleanup, None, "cleanup observation abandoned at expiry");
    assert_eq!(
        stopped_at, deadline,
        "stopped at the manager deadline, not 30s"
    );
    let expected = ConstructionDrain {
        finished: true,
        panicked: false,
        unavailable: false,
        unpublished: 1,
        sessions: vec![StartupCleanup::TimedOut],
    };
    assert_eq!(owner.drain_until(deadline).await, expected);
    assert_eq!(Instant::now(), deadline);
    // Releasing the blocker after expiry drives nothing: still sticky TimedOut.
    held.release();
    tokio::time::advance(Duration::from_secs(60)).await;
    assert_eq!(
        owner.drain_until(deadline + Duration::from_secs(60)).await,
        expected
    );
    assert_eq!(owner.state.lock().unwrap().constructions.len(), 1);
}

#[tokio::test]
async fn observer_dropped_after_publication_commit_takes_ordinary_retirement() {
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
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        codex_login::CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    manager
        .constructions
        .state
        .lock()
        .unwrap()
        .pause_after_publish = Some((Arc::clone(&entered), Arc::clone(&release)));
    let mut start = Box::pin(manager.start_thread(StartThreadOptions::new(config, None)));
    tokio::time::timeout(Duration::from_secs(20), async {
        tokio::select! {
            _ = entered.notified() => {}
            _ = start.as_mut() => panic!("completed before the publication barrier"),
        }
    })
    .await
    .expect("constructor reaches the post-publication barrier");
    let (completion, startup) = {
        let state = manager.constructions.state.lock().unwrap();
        let entry = &state.constructions[0];
        (entry.completion.clone(), Arc::clone(&entry.startup))
    };
    assert!(completion.peek().is_none(), "constructor is still pending");
    let ids = manager.list_thread_ids().await;
    assert_eq!(ids.len(), 1, "the runtime is already in the thread map");
    // Departure after the commit: no cancellation and no driver.
    drop(start);
    release.notify_one();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(20), completion)
            .await
            .expect("constructor completes"),
        ConstructionOutcome::Returned
    );
    assert!(
        startup.is_empty(),
        "publication transferred the session; custody ran no cleanup"
    );
    let thread = manager.get_thread(ids[0]).await.expect("still published");
    assert!(
        thread.observed_terminal_cleanup().is_none(),
        "no cleanup ran on the published runtime"
    );
    let report = manager
        .begin_shutdown(Instant::now() + Duration::from_secs(20))
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(report.is_complete(), "{report:?}");
    assert!(thread.observed_terminal_cleanup().is_some());
    assert!(manager.list_thread_ids().await.is_empty());
}

#[tokio::test(start_paused = true)]
async fn departed_observer_of_published_start_never_polls_at_or_after_expiry() {
    let owner = ThreadConstructions::default();
    let start = Instant::now();
    // Every poll of the post-publication tail, with its instant.
    let polls = Arc::new(Mutex::new(Vec::<Instant>::new()));
    let tail = Arc::clone(&polls);
    let finish = CancellationToken::new();
    let finished = finish.clone();
    let mut observer = owner
        .ticket()
        .register(
            /*account_work*/ None,
            move |_, publication| async move {
                publication
                    .publish(|| Ok(()))
                    .expect("publish while admission is open");
                // Post-publication steps on a fixed 100ms grid, so one step
                // becomes ready exactly at the 1s registry deadline.
                let mut next = start;
                while !finished.is_cancelled() {
                    next += Duration::from_millis(100);
                    let mut step = std::pin::pin!(tokio::time::sleep_until(next));
                    std::future::poll_fn(|cx| {
                        tail.lock().unwrap().push(Instant::now());
                        step.as_mut().poll(cx)
                    })
                    .await;
                }
                Err(CodexErr::TurnAborted)
            },
        )
        .expect("admit");
    assert!(futures::poll!(observer.as_mut()).is_pending());
    let (original, mut completion, clock) = {
        let state = owner.state.lock().unwrap();
        let entry = &state.constructions[0];
        (
            entry.completion.clone(),
            entry.completion.clone(),
            entry.publication.clone(),
        )
    };
    assert!(clock.is_published());
    // An ordinary post-commit departure spawns nothing. Stand in for a
    // departed-observer driver whose spawn raced the publication commit.
    drop(observer);
    let driver = tokio::spawn(async move { observe_until_expiry(&mut completion, &clock).await });
    let deadline = owner
        .close_until(start + Duration::from_secs(1))
        .expect("close");
    // Explicit steps, not auto-advance: progress before the deadline, then
    // land exactly on it with the tail's next step also ready.
    tokio::time::advance(Duration::from_millis(950)).await;
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert!(!driver.is_finished());
    let before = polls.lock().unwrap().len();
    assert!(before > 1, "the tail progressed before the deadline");
    tokio::time::advance(deadline - Instant::now()).await;
    assert_eq!(Instant::now(), deadline);
    let outcome = tokio::time::timeout(Duration::from_secs(30), driver)
        .await
        .expect("the departed observer returns at expiry")
        .expect("observer task");
    assert_eq!(outcome, None);
    assert_eq!(Instant::now(), deadline, "returned at the deadline itself");
    let late = |seen: &[Instant]| seen.iter().filter(|at| **at >= deadline).count();
    assert_eq!(
        late(polls.lock().unwrap().as_slice()),
        0,
        "no tail poll at or after expiry"
    );
    // Nothing polls the published start's tail after the deadline.
    tokio::time::advance(Duration::from_secs(60)).await;
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert_eq!(late(polls.lock().unwrap().as_slice()), 0);
    assert!(original.peek().is_none());
    // The registry keeps the unfinished original; a published start is
    // neither refused nor driven by the expired drain.
    let expected = ConstructionDrain {
        finished: false,
        panicked: false,
        unavailable: false,
        unpublished: 0,
        sessions: vec![],
    };
    assert_eq!(owner.drain_until(deadline).await, expected);
    assert_eq!(owner.drain_until(deadline).await, expected);
    assert_eq!(late(polls.lock().unwrap().as_slice()), 0);
    assert_eq!(owner.state.lock().unwrap().constructions.len(), 1);
    finish.cancel();
}

async fn expiry_manager(
    ephemeral: bool,
) -> (
    tempfile::TempDir,
    crate::thread_manager::ThreadManager,
    crate::config::Config,
) {
    let home = tempfile::tempdir().unwrap();
    let mut config = crate::config::ConfigBuilder::default()
        .codex_home(home.path().to_path_buf())
        .fallback_cwd(Some(home.path().to_path_buf()))
        .build()
        .await
        .unwrap();
    config.ephemeral = ephemeral;
    use crate::thread_manager::ThreadManager;
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        codex_login::CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    (home, manager, config)
}

use codex_protocol::error::CodexErrorDetails;

/// Upstream-proven start gate: a required MCP server whose successful
/// initialize is held by the Apps test server's startup control.
struct HeldRequiredMcp {
    control: core_test_support::apps_test_server::AppsTestServerStartupControl,
    release: Option<std::sync::mpsc::Sender<()>>,
    _server: wiremock::MockServer,
}

impl HeldRequiredMcp {
    async fn install(config: &mut crate::config::Config) -> Self {
        let server = core_test_support::responses::start_mock_server().await;
        let (apps, control) =
            core_test_support::apps_test_server::AppsTestServer::mount_with_startup_control(
                &server,
            )
            .await
            .expect("mount Apps MCP startup control");
        let release = control.hold_next_successful_initialize();
        let mut servers = config.mcp_servers.get().clone();
        servers.insert(
            "stalled".to_owned(),
            serde_json::from_value(serde_json::json!({
                "url": format!("{}/api/codex/ps/mcp", apps.chatgpt_base_url),
                "http_headers": { "Authorization": "Bearer synthetic-test-token" },
                "required": true,
                "startup_timeout_sec": 120,
            }))
            .expect("required MCP fixture config"),
        );
        config.mcp_servers.set(servers).expect("set MCP servers");
        Self {
            control,
            release: Some(release),
            _server: server,
        }
    }

    fn initialize_attempts(&self) -> usize {
        self.control.initialize_attempts()
    }

    /// Lets the held initialize answer; an abandoned start must not resume.
    fn release(&mut self) {
        drop(self.release.take());
    }
}

/// Drive `start` until the held initialize is reached. Any earlier result is
/// a fixture failure and is reported with its full error.
async fn reach_held_initialize<F>(mut start: std::pin::Pin<&mut F>, mcp: &HeldRequiredMcp)
where
    F: std::future::Future<Output = CodexResult<NewThread>>,
{
    tokio::time::timeout(Duration::from_secs(20), async {
        while mcp.initialize_attempts() == 0 {
            tokio::select! {
                biased;
                result = start.as_mut() => match result {
                    Err(error) => panic!(
                        "start ended before the held required MCP initialize: {error} \
                         (details: {:?})",
                        error.details()
                    ),
                    Ok(_) => panic!("unexpected publish before held MCP startup"),
                },
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
    })
    .await
    .expect("constructor reaches the held required MCP initialize");
}

#[tokio::test]
async fn live_observer_is_refused_and_constructor_stops_at_manager_expiry() {
    use crate::thread_manager::StartThreadOptions;
    let (_home, manager, mut config) = expiry_manager(/*ephemeral*/ false).await;
    let mut mcp = HeldRequiredMcp::install(&mut config).await;
    let mut start = Box::pin(manager.start_thread(StartThreadOptions::new(config, None)));
    reach_held_initialize(start.as_mut(), &mcp).await;
    tokio::time::pause();
    let deadline = manager
        .constructions
        .close_until(Instant::now() + Duration::from_secs(1))
        .unwrap();
    // Reach the exact deadline before the start is polled again, so the
    // observed clock is the deadline itself rather than a rounded timer tick.
    tokio::time::advance(deadline.saturating_duration_since(Instant::now())).await;
    assert_eq!(Instant::now(), deadline);
    // The caller is still awaiting; it must be refused at expiry, not hang.
    let refused = tokio::time::timeout(Duration::from_secs(5), start)
        .await
        .expect("refused, not hung");
    assert!(
        matches!(
            &refused,
            Err(error)
                if matches!(error.details(), CodexErrorDetails::TurnAborted)
        ),
        "expected the expiry refusal, got {:?}",
        refused.as_ref().map(|thread| thread.thread_id)
    );
    assert_eq!(Instant::now(), deadline);
    assert_eq!(
        mcp.initialize_attempts(),
        1,
        "the held initialize was not retried"
    );
    let report = manager.constructions.drain_until(deadline).await;
    assert!(report.finished && !report.panicked, "{report:?}");
    assert!(
        matches!(
            report.sessions.as_slice(),
            [StartupCleanup::TimedOut]
                | [StartupCleanup::BeforeLoop(
                    crate::session::retirement::CleanupExecution::Finished {
                        persistence_failed: false,
                    }
                )]
        ),
        "no completion is reported without a receipt: {report:?}"
    );
    // Releasing the held initialize after expiry resumes nothing.
    mcp.release();
    tokio::time::advance(Duration::from_secs(60)).await;
    assert_eq!(
        manager
            .constructions
            .drain_until(deadline + Duration::from_secs(60))
            .await,
        report
    );
    assert_eq!(mcp.initialize_attempts(), 1);
    assert!(manager.list_thread_ids().await.is_empty());
    assert!(manager.constructions.published().is_empty());
}

#[tokio::test]
async fn start_published_before_manager_expiry_retires_ordinarily() {
    use crate::thread_manager::StartThreadOptions;
    let (_home, manager, config) = expiry_manager(/*ephemeral*/ true).await;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    manager
        .constructions
        .state
        .lock()
        .unwrap()
        .pause_after_publish = Some((Arc::clone(&entered), Arc::clone(&release)));
    let mut start = Box::pin(manager.start_thread(StartThreadOptions::new(config, None)));
    tokio::time::timeout(Duration::from_secs(20), async {
        tokio::select! {
            _ = entered.notified() => {}
            _ = start.as_mut() => panic!("completed before the publication barrier"),
        }
    })
    .await
    .expect("constructor publishes");
    tokio::time::pause();
    let deadline = manager
        .constructions
        .close_until(Instant::now() + Duration::from_secs(1))
        .unwrap();
    tokio::select! {
        biased;
        _ = start.as_mut() => panic!("still held at the post-publication barrier"),
        _ = tokio::time::sleep_until(deadline + Duration::from_secs(1)) => {}
    }
    assert_eq!(manager.list_thread_ids().await.len(), 1);
    release.notify_one();
    tokio::time::resume();
    let started = tokio::time::timeout(Duration::from_secs(20), start)
        .await
        .expect("bounded")
        .expect("a published start is never refused or cancelled at expiry");
    let report = started
        .thread
        .begin_retirement(Instant::now() + Duration::from_secs(20))
        .expect("ordinary retirement")
        .wait()
        .await;
    assert!(report.is_complete(), "{report:?}");
}

async fn managed_held_start() -> (
    tempfile::TempDir,
    Arc<crate::thread_manager::ThreadManager>,
    crate::thread_manager::StartThreadOptions,
    HeldRequiredMcp,
) {
    let (home, manager, mut config) = expiry_manager(/*ephemeral*/ false).await;
    let mcp = HeldRequiredMcp::install(&mut config).await;
    let mut options =
        crate::thread_manager::StartThreadOptions::new(config, /*control_endpoint*/ None);
    options
        .thread_extension_init
        .insert(codex_extension_api::SessionIsolation::Isolated);
    (home, Arc::new(manager), options, mcp)
}

#[tokio::test]
async fn caller_supplied_startup_after_registry_expiry_inspects_without_new_cleanup() {
    let (_home, manager, options, mut mcp) = managed_held_start().await;
    let tasks = tokio_util::task::TaskTracker::new();
    let mut start = Box::pin(manager.start_thread_until(options, std::future::pending(), &tasks));
    reach_held_initialize(start.as_mut(), &mcp).await;
    tokio::time::pause();
    let deadline = manager
        .constructions
        .close_until(Instant::now() + Duration::from_secs(1))
        .unwrap();
    let refused = tokio::time::timeout(Duration::from_secs(5), start)
        .await
        .expect("refused, not hung");
    assert!(
        matches!(
            &refused,
            Err(error)
                if matches!(error.details(), CodexErrorDetails::TurnAborted)
        ),
        "expected the expiry refusal, got {:?}",
        refused.as_ref().map(|thread| thread.thread_id)
    );
    assert_eq!(
        mcp.initialize_attempts(),
        1,
        "the held initialize was not retried"
    );
    let startup = first_startup(&manager);
    tokio::time::timeout(Duration::from_secs(5), async {
        while startup.retained_for_test().1 == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("lifetime cleanup hands its task token to custody");
    // (membership retained, task work retained, no cleanup ran)
    let retained = (true, 1, false);
    assert_eq!(startup.retained_for_test(), retained);
    assert_eq!(
        startup.elapsed_deadline(),
        Some(deadline),
        "the custody bound is the manager deadline, unchanged"
    );
    tasks.close();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), tasks.wait())
            .await
            .is_err(),
        "the caller's background work stays retained without a receipt"
    );
    let report = manager.constructions.drain_until(deadline).await;
    assert_eq!(report.sessions, vec![StartupCleanup::TimedOut]);
    assert_eq!(report.unpublished, 1);
    mcp.release();
    tokio::time::advance(Duration::from_secs(60)).await;
    assert_eq!(
        manager
            .constructions
            .drain_until(deadline + Duration::from_secs(60))
            .await,
        report
    );
    assert_eq!(startup.retained_for_test(), retained);
    assert_eq!(startup.elapsed_deadline(), Some(deadline));
    assert_eq!(mcp.initialize_attempts(), 1);
    assert!(
        tokio::time::timeout(Duration::from_secs(5), tasks.wait())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn caller_supplied_startup_without_deadline_keeps_normal_cleanup() {
    let (_home, manager, options, mcp) = managed_held_start().await;
    let tasks = tokio_util::task::TaskTracker::new();
    let mut start = Box::pin(manager.start_thread_until(options, std::future::pending(), &tasks));
    reach_held_initialize(start.as_mut(), &mcp).await;
    // Abandonment without any registry deadline: existing legacy cleanup.
    drop(start);
    tasks.close();
    tokio::time::timeout(Duration::from_secs(20), tasks.wait())
        .await
        .expect("existing lifetime cleanup completes and releases its task");
    let startup = first_startup(&manager);
    assert_eq!(
        startup.elapsed_deadline(),
        None,
        "no deadline was ever latched"
    );
    assert!(
        startup.is_empty(),
        "legacy cleanup disposed the unpublished start"
    );
    assert_eq!(startup.retained_for_test(), (false, 0, false));
    drop(mcp);
}

#[tokio::test]
async fn published_managed_thread_lifetime_cleanup_is_unchanged() {
    let (_home, manager, config) = expiry_manager(/*ephemeral*/ true).await;
    let manager = Arc::new(manager);
    let mut options =
        crate::thread_manager::StartThreadOptions::new(config, /*control_endpoint*/ None);
    options
        .thread_extension_init
        .insert(codex_extension_api::SessionIsolation::Isolated);
    let tasks = tokio_util::task::TaskTracker::new();
    let (stop, until) = oneshot::channel::<()>();
    let started = tokio::time::timeout(
        Duration::from_secs(20),
        manager.start_thread_until(
            options,
            async move {
                let _ = until.await;
            },
            &tasks,
        ),
    )
    .await
    .expect("bounded")
    .expect("published managed thread");
    assert_eq!(manager.list_thread_ids().await, vec![started.thread_id]);
    stop.send(()).expect("lifetime still waiting");
    tasks.close();
    tokio::time::timeout(Duration::from_secs(20), tasks.wait())
        .await
        .expect("ordinary owned-thread lifetime cleanup completes");
    assert!(manager.list_thread_ids().await.is_empty());
    let startup = first_startup(&manager);
    assert_eq!(startup.retained_for_test(), (false, 0, false));
}

fn first_startup(manager: &crate::thread_manager::ThreadManager) -> Arc<SessionStartupCustody> {
    let state = manager.constructions.state.lock().unwrap();
    Arc::clone(&state.constructions[0].startup)
}

/// Keeps a retained pre-session acquisition locked on a synchronous thread,
/// so disposal stays blocked without any async test body holding the guard
/// across an await. Dropping the holder (including on a test panic) closes
/// the release channel, which also frees the lock.
struct AcquisitionHolder {
    release: std::sync::mpsc::Sender<()>,
    holder: std::thread::JoinHandle<()>,
}

impl AcquisitionHolder {
    fn hold(guard: tokio::sync::OwnedMutexGuard<codex_thread_store::LiveThreadInitGuard>) -> Self {
        let (held_tx, held_rx) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel::<()>();
        let holder = std::thread::spawn(move || {
            let _guard = guard;
            held_tx.send(()).expect("report held acquisition lock");
            let _ = release_rx.recv();
        });
        // A blocking handshake, not a timer: paused test time must not
        // auto-advance while the holder thread starts.
        held_rx.recv().expect("acquisition holder alive");
        Self { release, holder }
    }

    fn release(self) {
        self.release.send(()).expect("release acquisition lock");
        self.holder.join().expect("acquisition holder finished");
    }
}

#[tokio::test(start_paused = true)]
async fn caller_supplied_cleanup_after_registry_expiry_without_prior_custody_bind() {
    use crate::agent::LocalAgentControl;
    use crate::session::startup::SessionStartup;
    let owner = ThreadConstructions::default();
    let (custody_tx, custody_rx) = oneshot::channel();
    let observer = owner
        .ticket()
        .register(/*account_work*/ None, move |custody, _| async move {
            let _ = custody_tx.send(custody);
            Err(CodexErr::TurnAborted)
        })
        .expect("admit");
    // Finishes while the registry is open: no expiry arm, drain or bind ran.
    assert!(
        tokio::time::timeout(Duration::from_secs(5), observer)
            .await
            .expect("bounded")
            .is_err()
    );
    let custody = custody_rx.await.expect("custody");
    let startup = SessionStartup::default();
    assert!(startup.custody.set(Arc::clone(&custody)).is_ok());
    let control = LocalAgentControl::new(
        std::sync::Weak::new(),
        Arc::new(codex_protocol::ThreadId::new),
        /*rollout_budget*/ None,
    );
    startup.hold_membership(control.runtime.admit_start().expect("membership"));
    let held = AcquisitionHolder::hold(
        custody
            .retain_blocked_persistence_for_test(codex_protocol::ThreadId::new())
            .await,
    );
    let deadline = owner
        .close_until(Instant::now() + Duration::from_secs(1))
        .expect("close");
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(custody.elapsed_deadline(), None, "custody was never bound");
    let tasks = tokio_util::task::TaskTracker::new();
    let returned = tokio::time::timeout(
        Duration::from_secs(5),
        startup.cleanup_with_owner(Box::new(tasks.token())),
    )
    .await
    .expect("inspection is bounded");
    assert!(returned.is_none(), "the caller's task token is retained");
    // (membership retained, task retained, no disposal started)
    let retained = (true, 1, false);
    assert_eq!(custody.retained_for_test(), retained);
    assert_eq!(custody.elapsed_deadline(), Some(deadline));
    tasks.close();
    let tree = control.runtime.request_shutdown();
    for _ in 0..2 {
        assert!(
            tokio::time::timeout(Duration::from_secs(5), tasks.wait())
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(5), tree.wait_detailed())
                .await
                .is_err(),
            "the guard was not completed without a receipt"
        );
        assert_eq!(
            custody.shutdown_until(deadline).await,
            StartupCleanup::TimedOut
        );
        assert_eq!(custody.retained_for_test(), retained);
        tokio::time::advance(Duration::from_secs(60)).await;
    }
    held.release();
}

#[tokio::test(start_paused = true)]
async fn until_retirement_deadline_never_polls_work_after_manager_expiry() {
    let (_home, manager, _config) = expiry_manager(/*ephemeral*/ true).await;
    let polled = Arc::new(AtomicUsize::new(0));

    // No deadline latched: the work runs and its output is returned.
    let counter = Arc::clone(&polled);
    let output = manager
        .until_retirement_deadline(async move {
            counter.fetch_add(1, Ordering::SeqCst);
            7
        })
        .await;
    assert_eq!(output, Some(7));
    assert_eq!(polled.load(Ordering::SeqCst), 1);

    // Latch a short manager deadline and let it elapse.
    let _retirement = manager
        .begin_shutdown(Instant::now() + Duration::from_secs(1))
        .unwrap();
    tokio::time::advance(Duration::from_secs(2)).await;

    // Work whose first poll has a side effect must not run at all.
    let counter = Arc::clone(&polled);
    let output = tokio::time::timeout(
        Duration::from_secs(5),
        manager.until_retirement_deadline(async move {
            counter.fetch_add(1, Ordering::SeqCst);
            8
        }),
    )
    .await
    .expect("expired deadline resolves immediately");
    assert_eq!(output, None);
    assert_eq!(
        polled.load(Ordering::SeqCst),
        1,
        "work was polled after expiry"
    );
}

fn startup_with_membership() -> (
    crate::agent::LocalAgentControl,
    Arc<crate::session::startup::SessionStartup>,
) {
    let control = crate::agent::LocalAgentControl::new(
        std::sync::Weak::new(),
        Arc::new(codex_protocol::ThreadId::new),
        /*rollout_budget*/ None,
    );
    let startup = Arc::new(crate::session::startup::SessionStartup::default());
    startup.hold_membership(control.runtime.admit_start().expect("membership"));
    (control, startup)
}

/// Runs `f` on a plain std thread that has no Tokio runtime context.
fn without_runtime(f: impl FnOnce() + Send + 'static) {
    std::thread::spawn(move || {
        assert!(tokio::runtime::Handle::try_current().is_err());
        f();
    })
    .join()
    .expect("a startup guard dropped without a runtime must not panic");
}

fn observer_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("observer runtime")
}

fn abandoned_without_runtime() -> Vec<crate::AgentTreeShutdownFailureReason> {
    vec![
        crate::AgentTreeShutdownFailureReason::OperationFailed {
            phase: "session_startup_guard",
            error_kind: "dropped_without_runtime",
        },
        crate::AgentTreeShutdownFailureReason::GuardAbandoned,
    ]
}

#[tokio::test]
async fn startup_guard_dropped_off_runtime_hands_cleanup_to_its_runtime() {
    use crate::session::startup::SessionStartupGuard;
    let (control, startup) = startup_with_membership();
    // Built on the runtime, released by a runtime-less last owner.
    let guard = SessionStartupGuard::new(Arc::clone(&startup));
    without_runtime(move || drop(guard));
    let tree = control.runtime.request_shutdown();
    tokio::time::timeout(Duration::from_secs(5), tree.wait_detailed())
        .await
        .expect("the captured runtime drives the cleanup")
        .expect("the driven cleanup completes the membership");
}

#[test]
fn startup_guard_without_any_runtime_reports_incomplete_startup() {
    use crate::session::startup::SessionStartupGuard;
    let (control, startup) = startup_with_membership();
    let guarded = Arc::clone(&startup);
    without_runtime(move || drop(SessionStartupGuard::new(guarded)));
    let tree = control.runtime.request_shutdown();
    let report = observer_runtime()
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(5), tree.wait_detailed()).await
        })
        .expect("membership was released, not leaked")
        .expect_err("an uncleaned startup is never reported complete");
    let reasons = report
        .failures
        .iter()
        .map(|failure| failure.reason.clone())
        .collect::<Vec<_>>();
    assert_eq!(reasons, abandoned_without_runtime());
}

#[test]
fn startup_guard_on_a_shut_down_runtime_hands_membership_to_custody() {
    use crate::session::startup::SessionStartupGuard;
    use crate::session::startup_custody::SessionStartupCustody;
    let (control, startup) = startup_with_membership();
    let custody = Arc::new(SessionStartupCustody::default());
    assert!(startup.custody.set(Arc::clone(&custody)).is_ok());
    let admitting = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("admitting runtime");
    let guard = admitting.block_on(async { SessionStartupGuard::new(Arc::clone(&startup)) });
    drop(admitting);
    without_runtime(move || drop(guard));

    // The unpolled cleanup task was dropped; the registry custody now owns
    // the membership. No cleanup ran and nothing was completed.
    assert_eq!(custody.retained_for_test(), (true, 0, false));
    let tree = control.runtime.request_shutdown();
    let observer = observer_runtime();
    assert!(
        observer
            .block_on(async {
                tokio::time::timeout(Duration::from_millis(100), tree.wait_detailed()).await
            })
            .is_err(),
        "membership is retained for the drain, not silently completed"
    );
    drop(startup);
    drop(custody);
    let report = observer
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(5), tree.wait_detailed()).await
        })
        .expect("dropping the custody releases membership")
        .expect_err("the abandoned startup stays reported as incomplete");
    let reasons = report
        .failures
        .iter()
        .map(|failure| failure.reason.clone())
        .collect::<Vec<_>>();
    assert_eq!(reasons, abandoned_without_runtime());
}

/// Publication race: with the host's shutdown edge already latched, the
/// runtime is gated at the exact moment it becomes visible (in the registry
/// and the lookup map), and a concurrent observer on another thread looks it
/// up there. The FIRST visible runtime is already bound to the edge deadline,
/// so no retirement racing publication can start its cleanup in Legacy mode.
#[tokio::test]
async fn first_visible_runtime_is_already_bound_to_the_shutdown_edge() {
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
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        codex_login::CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    // The host's shutdown edge latched before this runtime is published.
    assert_eq!(manager.bind_shutdown_deadlines_now(deadline).bound, 0);

    let (visible_tx, visible_rx) = std::sync::mpsc::channel::<()>();
    let (observed_tx, observed_rx) = std::sync::mpsc::channel::<()>();
    let observed_rx = std::sync::Mutex::new(observed_rx);
    manager
        .constructions
        .state
        .lock()
        .unwrap()
        .publication_probe = Some(Arc::new(move |_thread: &Arc<CodexThread>| {
        // Gate: hold publication here until the observer has looked.
        visible_tx.send(()).expect("observer listens");
        observed_rx
            .lock()
            .expect("gate")
            .recv()
            .expect("observer reports");
    }));
    let constructions = manager.constructions.clone();
    let observer = std::thread::spawn(move || {
        visible_rx.recv().expect("runtime becomes visible");
        let population = constructions.published();
        assert_eq!(population.len(), 1, "the runtime is visible to observers");
        // A looser binding keeps an existing one: `Ok(deadline)` proves the
        // runtime was already bound when it first became visible.
        let seen = population[0].bind_shutdown_deadline(deadline + Duration::from_secs(60));
        observed_tx.send(()).expect("publisher waits");
        seen
    });

    let started = manager
        .start_thread(StartThreadOptions::new(config, None))
        .await
        .expect("start thread");

    assert_eq!(
        observer.join().expect("observer").expect("bound"),
        deadline,
        "the first visible runtime was already bound"
    );
    drop(started);
}

#[tokio::test(start_paused = true)]
async fn permanent_edge_clamps_unpublished_custody_and_refuses_late_birth() {
    let owner = ThreadConstructions::default();
    let observer = owner
        .ticket()
        .register(None, |_, _| async { Err(CodexErr::InternalAgentDied) })
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    assert!(owner.latch_shutdown_deadline(deadline).is_empty());
    assert_eq!(owner.ticket().deadline(), Some(deadline));
    let startup = Arc::clone(&owner.state.lock().unwrap().constructions[0].startup);
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(startup.elapsed_deadline(), Some(deadline));
    assert!(
        owner
            .ticket()
            .register(None, |_, _| async { panic!("late constructor ran") })
            .is_err()
    );
    assert_eq!(
        owner
            .close_until(deadline + Duration::from_secs(30))
            .unwrap(),
        deadline
    );
    drop(observer);
}

#[tokio::test]
async fn real_constructor_cancelled_before_publication_is_drained_after_close() {
    real_account_constructor_case(false, false).await;
}

#[tokio::test]
async fn explicit_constructor_operation_does_not_rediscover_an_ended_request() {
    real_account_constructor_case(true, false).await;
}

#[tokio::test]
async fn real_initial_mcp_original_keeps_account_work_after_constructor_observer_drop() {
    real_account_constructor_case(false, true).await;
}

async fn real_account_constructor_case(explicit: bool, hold_mcp: bool) {
    use crate::config::ConfigBuilder;
    use crate::thread_manager::StartThreadOptions;
    use crate::thread_manager::ThreadManager;
    #[derive(Debug, Default)]
    struct RequestHost {
        auth: std::sync::Weak<TestAuthTasks>,
        request_alive: std::sync::atomic::AtomicBool,
        operations: AtomicUsize,
        request_derivations: AtomicUsize,
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
        fn auth_task_spawner(
            &self,
        ) -> Result<
            Arc<dyn codex_protocol::host_auth_work::AuthTaskSpawner>,
            codex_extension_api::TurnWorkRefused,
        > {
            let owner = self
                .0
                .auth
                .upgrade()
                .ok_or(codex_extension_api::TurnWorkRefused::Unavailable)?;
            Ok(owner.spawner(self.derive_operation()?))
        }
        fn derive_operation(
            &self,
        ) -> Result<
            Box<dyn codex_extension_api::HostOperationWork>,
            codex_extension_api::TurnWorkRefused,
        > {
            // Existing work may derive descendants after the request ends.
            self.0.operations.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(Self(Arc::clone(&self.0))))
        }
        fn derive_turn_work(
            &self,
            _: &codex_extension_api::ExtensionData,
            _: codex_extension_api::ExtensionFuture<'static, ()>,
        ) -> Result<
            Box<dyn codex_protocol::host_turn_work::HostTurnWork>,
            codex_extension_api::TurnWorkRefused,
        > {
            Err(codex_extension_api::TurnWorkRefused::Unavailable)
        }
    }
    #[derive(Debug)]
    struct ConstructionAdmission(Arc<RequestHost>);
    impl codex_extension_api::TurnStartAdmission for ConstructionAdmission {
        fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
            Some(Box::new(()))
        }
        fn requires_account_work(&self) -> bool {
            true
        }
        fn admit_operation_work(
            &self,
        ) -> Result<
            Option<Box<dyn codex_extension_api::HostOperationWork>>,
            codex_extension_api::TurnWorkRefused,
        > {
            panic!("constructor must never acquire fresh operation admission");
        }
        fn derive_request_operation_work(
            &self,
        ) -> Result<
            Option<Box<dyn codex_extension_api::HostOperationWork>>,
            codex_extension_api::TurnWorkRefused,
        > {
            self.0.request_derivations.fetch_add(1, Ordering::SeqCst);
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
    let mut held_mcp = if hold_mcp {
        Some(HeldRequiredMcp::install(&mut config).await)
    } else {
        None
    };
    let mut manager = ThreadManager::with_models_provider_and_home_for_tests(
        codex_login::CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let (release, receiver) = oneshot::channel();
    let mut instruction_release = Some(release);
    let provider = Arc::new(HeldInstructions {
        entered: AtomicUsize::new(0),
        entry: tokio::sync::Notify::new(),
        receiver: Mutex::new(Some(receiver)),
    });
    Arc::get_mut(&mut manager.state)
        .expect("builders retain unique State")
        .user_instructions_provider = provider.clone();
    let auth_owner = Arc::new(TestAuthTasks::default());
    let host = Arc::new(RequestHost {
        auth: Arc::downgrade(&auth_owner),
        ..Default::default()
    });
    host.request_alive.store(true, Ordering::SeqCst);
    let mut extensions = codex_extension_api::ExtensionRegistryBuilder::new();
    extensions.turn_start_admission(Arc::new(ConstructionAdmission(Arc::clone(&host))));
    Arc::get_mut(&mut manager.state).unwrap().extensions = Arc::new(extensions.build());
    let mut options = StartThreadOptions::new(config.clone(), None);
    if explicit {
        // A delegated operation must not rediscover an already-ended request.
        host.request_alive.store(false, Ordering::SeqCst);
        host.operations.store(1, Ordering::SeqCst);
        options.account_work = Some(Box::new(ConstructionWork(Arc::clone(&host))));
    }
    let mut observer = Box::pin(manager.start_thread(options));
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
    let startup = first_startup(&manager);
    assert_eq!(
        startup.constructor_stage_for_test(),
        Some((false, false, false)),
        "real early-resource owner exists before Session/MCP birth"
    );
    assert_eq!(
        host.request_derivations.load(Ordering::SeqCst),
        usize::from(!explicit),
        "retained construction must not rediscover request authority"
    );
    assert_eq!(host.operations.load(Ordering::SeqCst), 2);
    if let Some(mcp) = &held_mcp {
        instruction_release
            .take()
            .unwrap()
            .send(())
            .expect("advance actual constructor into MCP startup");
        reach_held_initialize(observer.as_mut(), mcp).await;
        assert_eq!(mcp.initialize_attempts(), 1);
        assert_eq!(
            startup.constructor_stage_for_test(),
            Some((true, true, false)),
            "same original owns the real Session and MCP runtime"
        );
        // Constructor and initial-publication input each retain one operation;
        // eager driver and actual original each own one further descendant.
        assert_eq!(host.operations.load(Ordering::SeqCst), 4);
    }
    // Observer departure is a cancellation request consumed by the retained
    // constructor; the registry still owns and records its completion.
    drop(observer);
    host.request_alive.store(false, Ordering::SeqCst);
    assert_eq!(
        host.operations.load(Ordering::SeqCst),
        if hold_mcp { 4 } else { 2 }
    );
    assert!(manager.list_thread_ids().await.is_empty());
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
    auth_owner.drain().await.unwrap();
    assert_eq!(host.operations.load(Ordering::SeqCst), 0);
    if let Some(release) = instruction_release {
        assert!(
            release.send(()).is_err(),
            "the original constructor dropped held instructions on cancellation"
        );
    }
    if let Some(mcp) = &mut held_mcp {
        assert_eq!(
            mcp.initialize_attempts(),
            1,
            "account drain does not retry initialize"
        );
        mcp.release();
    }
    assert_eq!(
        startup.constructor_stage_for_test(),
        Some((false, hold_mcp, true)),
        "original construction ends and releases the unpublished Session"
    );
    assert!(
        manager
            .shutdown_unpublished_constructions_bounded(Duration::from_secs(20))
            .await
    );
    assert!(
        startup.is_empty(),
        "reset retires the runtime outside the lookup map"
    );
    // Check only the original construction before making a separate late
    // start request. That new request may consult the now-closed host itself.
    assert_eq!(
        host.request_derivations.load(Ordering::SeqCst),
        usize::from(!explicit),
        "retained construction must not rediscover request authority"
    );
    manager.constructions.close();
    assert!(
        manager
            .start_thread(StartThreadOptions::new(
                config, /*control_endpoint*/ None
            ))
            .await
            .is_err()
    );
    let deadline = Instant::now() + Duration::from_secs(20);
    let report = manager.constructions.drain_until(deadline).await;
    assert_eq!(
        report,
        ConstructionDrain {
            finished: true,
            panicked: false,
            unavailable: false,
            unpublished: 0,
            sessions: vec![StartupCleanup::BeforeLoop(
                crate::session::retirement::CleanupExecution::Finished {
                    persistence_failed: false,
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
    assert_eq!(
        host.operations.load(Ordering::SeqCst),
        0,
        "late refusal acquires no work or new constructor"
    );
}

tokio::task_local! {
    static MANAGED_REQUEST_SCOPE: ();
}

#[derive(Debug)]
struct ManagedRequestHost {
    auth: std::sync::Weak<TestAuthTasks>,
    required: bool,
    produce_work: bool,
    refuse: bool,
    lookups: AtomicUsize,
    operations: AtomicUsize,
}

#[derive(Debug)]
struct ManagedRequestWork(Arc<ManagedRequestHost>);

impl Drop for ManagedRequestWork {
    fn drop(&mut self) {
        self.0.operations.fetch_sub(1, Ordering::SeqCst);
    }
}

impl codex_extension_api::HostOperationWork for ManagedRequestWork {
    fn auth_task_spawner(
        &self,
    ) -> Result<
        Arc<dyn codex_protocol::host_auth_work::AuthTaskSpawner>,
        codex_extension_api::TurnWorkRefused,
    > {
        let owner = self
            .0
            .auth
            .upgrade()
            .ok_or(codex_extension_api::TurnWorkRefused::Unavailable)?;
        Ok(owner.spawner(self.derive_operation()?))
    }
    fn derive_operation(
        &self,
    ) -> Result<Box<dyn codex_extension_api::HostOperationWork>, codex_extension_api::TurnWorkRefused>
    {
        self.0.operations.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(Self(Arc::clone(&self.0))))
    }

    fn derive_turn_work(
        &self,
        _: &codex_extension_api::ExtensionData,
        _: codex_extension_api::ExtensionFuture<'static, ()>,
    ) -> Result<
        Box<dyn codex_protocol::host_turn_work::HostTurnWork>,
        codex_extension_api::TurnWorkRefused,
    > {
        Err(codex_extension_api::TurnWorkRefused::Unavailable)
    }
}

#[derive(Debug)]
struct ManagedRequestAdmission(Arc<ManagedRequestHost>);

impl codex_extension_api::TurnStartAdmission for ManagedRequestAdmission {
    fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
        Some(Box::new(()))
    }

    fn requires_account_work(&self) -> bool {
        self.0.required
    }

    fn admit_operation_work(
        &self,
    ) -> Result<
        Option<Box<dyn codex_extension_api::HostOperationWork>>,
        codex_extension_api::TurnWorkRefused,
    > {
        panic!("managed constructor must not acquire fresh account admission")
    }

    fn derive_request_operation_work(
        &self,
    ) -> Result<
        Option<Box<dyn codex_extension_api::HostOperationWork>>,
        codex_extension_api::TurnWorkRefused,
    > {
        self.0.lookups.fetch_add(1, Ordering::SeqCst);
        if self.0.refuse {
            return Err(codex_extension_api::TurnWorkRefused::Unavailable);
        }
        if MANAGED_REQUEST_SCOPE.try_with(|_| ()).is_err() || !self.0.produce_work {
            return Ok(None);
        }
        self.0.operations.fetch_add(1, Ordering::SeqCst);
        Ok(Some(Box::new(ManagedRequestWork(Arc::clone(&self.0)))))
    }
}

async fn managed_account_capture_case(explicit: bool, required: bool) {
    use crate::thread_manager::StartThreadOptions;
    let (_home, mut manager, mut config) = expiry_manager(/*ephemeral*/ true).await;
    let mut mcp = HeldRequiredMcp::install(&mut config).await;
    let auth_owner = Arc::new(TestAuthTasks::default());
    let host = Arc::new(ManagedRequestHost {
        auth: Arc::downgrade(&auth_owner),
        required,
        produce_work: required,
        refuse: false,
        lookups: AtomicUsize::new(0),
        operations: AtomicUsize::new(0),
    });
    let mut extensions = codex_extension_api::ExtensionRegistryBuilder::new();
    extensions.turn_start_admission(Arc::new(ManagedRequestAdmission(Arc::clone(&host))));
    Arc::get_mut(&mut manager.state).unwrap().extensions = Arc::new(extensions.build());
    let manager = Arc::new(manager);
    let mut options = StartThreadOptions::new(config, None);
    options
        .thread_extension_init
        .insert(codex_extension_api::SessionIsolation::Isolated);
    if explicit {
        host.operations.store(1, Ordering::SeqCst);
        options.account_work = Some(Box::new(ManagedRequestWork(Arc::clone(&host))));
    }
    let tasks = tokio_util::task::TaskTracker::new();
    let mut start = Box::pin(manager.start_thread_until(options, std::future::pending(), &tasks));
    if explicit {
        assert!(futures::poll!(start.as_mut()).is_pending());
    } else {
        MANAGED_REQUEST_SCOPE
            .scope((), async {
                assert!(futures::poll!(start.as_mut()).is_pending());
            })
            .await;
    }
    // The original caller's task-local scope has ended before the spawned task
    // can run on this current-thread runtime.
    assert!(MANAGED_REQUEST_SCOPE.try_with(|_| ()).is_err());
    assert_eq!(host.lookups.load(Ordering::SeqCst), usize::from(!explicit));
    reach_held_initialize(start.as_mut(), &mcp).await;
    assert_eq!(
        host.operations.load(Ordering::SeqCst),
        if required { 4 } else { 0 }
    );
    assert_eq!(host.lookups.load(Ordering::SeqCst), usize::from(!explicit));
    drop(start);
    tasks.close();
    tokio::time::timeout(Duration::from_secs(20), tasks.wait())
        .await
        .expect("managed original drains");
    auth_owner.drain().await.unwrap();
    assert_eq!(host.operations.load(Ordering::SeqCst), 0);
    assert_eq!(host.lookups.load(Ordering::SeqCst), usize::from(!explicit));
    assert!(manager.state.threads.read().await.is_empty());
    assert_eq!(mcp.initialize_attempts(), 1);
    mcp.release();
}

#[tokio::test]
async fn managed_constructor_captures_task_local_request_before_spawn() {
    managed_account_capture_case(false, true).await;
}

#[tokio::test]
async fn managed_constructor_uses_explicit_operation_without_request_lookup() {
    managed_account_capture_case(true, true).await;
}

#[tokio::test]
async fn managed_constructor_preserves_ungated_empty_capture_without_rediscovery() {
    managed_account_capture_case(false, false).await;
}

async fn managed_account_refusal_case(refuse: bool) {
    use crate::thread_manager::StartThreadOptions;
    let (_home, mut manager, config) = expiry_manager(/*ephemeral*/ true).await;
    let auth_owner = Arc::new(TestAuthTasks::default());
    let host = Arc::new(ManagedRequestHost {
        auth: Arc::downgrade(&auth_owner),
        required: true,
        produce_work: true,
        refuse,
        lookups: AtomicUsize::new(0),
        operations: AtomicUsize::new(0),
    });
    let mut extensions = codex_extension_api::ExtensionRegistryBuilder::new();
    extensions.turn_start_admission(Arc::new(ManagedRequestAdmission(Arc::clone(&host))));
    Arc::get_mut(&mut manager.state).unwrap().extensions = Arc::new(extensions.build());
    let manager = Arc::new(manager);
    let mut options = StartThreadOptions::new(config, None);
    options
        .thread_extension_init
        .insert(codex_extension_api::SessionIsolation::Isolated);
    let tasks = tokio_util::task::TaskTracker::new();
    let result = manager
        .start_thread_until(options, std::future::pending(), &tasks)
        .await;
    let error = result
        .err()
        .expect("missing/exhausted request refuses before task birth");
    assert!(error.to_string().contains(if refuse {
        "request account work cannot admit thread construction"
    } else {
        "thread construction requires account work"
    }));
    assert_eq!(host.lookups.load(Ordering::SeqCst), 1);
    assert_eq!(host.operations.load(Ordering::SeqCst), 0);
    assert!(tasks.is_empty());
    assert!(manager.state.threads.read().await.is_empty());
}

#[tokio::test]
async fn managed_constructor_missing_required_request_refuses_before_task_birth() {
    managed_account_refusal_case(false).await;
}

#[tokio::test]
async fn managed_constructor_exhausted_request_refuses_before_task_birth() {
    managed_account_refusal_case(true).await;
}

#[tokio::test]
async fn agent_constructor_retains_exact_parent_work_across_held_mcp_and_caller_loss() {
    use crate::agent::api::{AgentControl, AgentInput, SpawnRequest};
    use crate::agent::types::SpawnAgentOptions;
    let (_home, manager, mut config) = expiry_manager(/*ephemeral*/ true).await;
    let mut mcp = HeldRequiredMcp::install(&mut config).await;
    let (parent, gate) = crate::agent::turn_authority::tests::parent().await;
    let authority = crate::ParentTurnAuthority::capture(&parent, "live-parent");
    let control = manager.agent_control();
    // The real LocalAgentControl path reaches the actual retained constructor
    // before any initial input can be submitted to the child.
    let mut start = Box::pin(async {
        match control
            .spawn(SpawnRequest {
                caller: parent.thread_id,
                config,
                input: AgentInput::UserInput(Vec::new()),
                source: codex_protocol::protocol::SessionSource::Exec,
                options: SpawnAgentOptions {
                    parent_authority: Some(authority),
                    ..Default::default()
                },
            })
            .await
        {
            Ok(_) => panic!("child cannot publish before held initialize"),
            Err(error) => Err(error),
        }
    });
    reach_held_initialize(start.as_mut(), &mcp).await;
    assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
    assert_eq!(gate.live.load(Ordering::SeqCst), 4);
    gate.active.store(false, Ordering::SeqCst);
    drop(start);
    // This is a continuous progress observer, not a completion future. Keep
    // polling it only until the counted original has actually released work.
    tokio::time::timeout(Duration::from_secs(20), async {
        let progress = manager.drive_admitted_constructions();
        tokio::pin!(progress);
        while gate.live.load(Ordering::SeqCst) != 0 {
            tokio::select! {
                _ = &mut progress => panic!("account progress observer stopped"),
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
    })
    .await
    .expect("agent original releases counted work");
    gate.auth.drain().await.unwrap();
    assert_eq!(gate.live.load(Ordering::SeqCst), 0);
    assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
    assert!(manager.state.threads.read().await.is_empty());
    assert_eq!(mcp.initialize_attempts(), 1);
    mcp.release();
}

#[tokio::test]
async fn expired_parent_refuses_real_agent_constructor_before_mcp_or_publication() {
    use crate::agent::api::{AgentControl, AgentInput, SpawnRequest};
    use crate::agent::types::SpawnAgentOptions;
    let (_home, manager, mut config) = expiry_manager(/*ephemeral*/ true).await;
    let mcp = HeldRequiredMcp::install(&mut config).await;
    let (parent, gate) = crate::agent::turn_authority::tests::parent().await;
    gate.active.store(false, Ordering::SeqCst);
    let error = manager
        .agent_control()
        .spawn(SpawnRequest {
            caller: parent.thread_id,
            config,
            input: AgentInput::UserInput(Vec::new()),
            source: codex_protocol::protocol::SessionSource::Exec,
            options: SpawnAgentOptions {
                parent_authority: Some(crate::ParentTurnAuthority::capture(&parent, "live-parent")),
                ..Default::default()
            },
        })
        .await
        .err()
        .expect("expired parent refuses");
    assert!(
        error
            .to_string()
            .contains("parent account work cannot admit agent construction")
    );
    assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
    assert_eq!(gate.live.load(Ordering::SeqCst), 0);
    assert_eq!(mcp.initialize_attempts(), 0);
    assert!(manager.state.threads.read().await.is_empty());
}

#[tokio::test]
async fn explicit_empty_parent_capture_never_falls_back_to_required_manager_request() {
    use crate::agent::api::{AgentControl, AgentInput, SpawnRequest};
    use crate::agent::types::SpawnAgentOptions;
    let (_home, mut manager, config) = expiry_manager(/*ephemeral*/ true).await;
    let (mut parent, _) = crate::session::tests::make_session_and_context().await;
    parent.services.host_admission = None;
    let parent = Arc::new(parent);
    let (_policy_parent, required_gate) = crate::agent::turn_authority::tests::parent().await;
    let mut extensions = codex_extension_api::ExtensionRegistryBuilder::new();
    extensions.turn_start_admission(required_gate.clone());
    Arc::get_mut(&mut manager.state).unwrap().extensions = Arc::new(extensions.build());
    let error = manager
        .agent_control()
        .spawn(SpawnRequest {
            caller: parent.thread_id,
            config,
            input: AgentInput::UserInput(Vec::new()),
            source: codex_protocol::protocol::SessionSource::Exec,
            options: SpawnAgentOptions {
                parent_authority: Some(crate::ParentTurnAuthority::capture(&parent, "ungated")),
                ..Default::default()
            },
        })
        .await
        .err()
        .expect("empty parent work cannot satisfy required manager policy");
    assert!(
        error
            .to_string()
            .contains("thread construction requires account work")
    );
    // A request lookup on this gate panics, so this also detects rediscovery.
    assert_eq!(required_gate.calls.load(Ordering::SeqCst), 0);
    assert_eq!(required_gate.live.load(Ordering::SeqCst), 0);
    assert!(manager.state.threads.read().await.is_empty());
}

struct ManagedStartWitness(Arc<AtomicUsize>);
impl codex_extension_api::ThreadLifecycleContributor<crate::config::Config>
    for ManagedStartWitness
{
    fn on_thread_start<'a>(
        &'a self,
        input: codex_extension_api::ThreadStartInput<'a, crate::config::Config>,
    ) -> codex_extension_api::ExtensionFuture<'a, ()> {
        Box::pin(async move {
            let Ok(codex_mcp::McpAttemptAccess::Admitted(work)) = input.mcp_access else {
                panic!("explicit Required constructor work")
            };
            let child = work.derive_attempt().expect("constructor callback child");
            self.0.fetch_add(1, Ordering::SeqCst);
            drop(child);
        })
    }
}
struct ManagedReadyWitness(Arc<AtomicUsize>);
impl codex_extension_api::ThreadLifecycleContributor<crate::config::Config>
    for ManagedReadyWitness
{
    fn on_thread_ready<'a>(
        &'a self,
        input: codex_extension_api::ThreadReadyInput<'a, crate::config::Config>,
    ) -> codex_extension_api::ExtensionFuture<'a, ()> {
        Box::pin(async move {
            let work = input
                .account_work
                .expect("constructor authority")
                .expect("required work");
            let child = work.derive_operation().expect("derive callback work");
            self.0.fetch_add(1, Ordering::SeqCst);
            drop(child);
        })
    }
}
#[tokio::test]
async fn managed_constructor_carries_explicit_work_into_ready_callbacks_without_rediscovery() {
    use crate::thread_manager::StartThreadOptions;
    let (_home, mut manager, config) = expiry_manager(true).await;
    let auth_owner = Arc::new(TestAuthTasks::default());
    let host = Arc::new(ManagedRequestHost {
        auth: Arc::downgrade(&auth_owner),
        required: true,
        produce_work: true,
        refuse: false,
        lookups: AtomicUsize::new(0),
        operations: AtomicUsize::new(1),
    });
    let observed = Arc::new(AtomicUsize::new(0));
    let mut extensions = codex_extension_api::ExtensionRegistryBuilder::new();
    extensions.turn_start_admission(Arc::new(ManagedRequestAdmission(Arc::clone(&host))));
    let inherited = Arc::new(extensions.build());
    Arc::get_mut(&mut manager.state).unwrap().extensions = Arc::clone(&inherited);
    let mut isolated = inherited.to_builder();
    let started = Arc::new(AtomicUsize::new(0));
    isolated.thread_lifecycle_contributor(Arc::new(ManagedStartWitness(Arc::clone(&started))));
    isolated.thread_lifecycle_contributor(Arc::new(ManagedReadyWitness(Arc::clone(&observed))));
    let isolated = Arc::new(isolated.build());
    let manager = Arc::new(manager);
    let mut options = StartThreadOptions::new(config, None);
    options.account_work = Some(Box::new(ManagedRequestWork(Arc::clone(&host))));
    options
        .thread_extension_init
        .insert(codex_extension_api::SessionIsolation::Isolated);
    options
        .thread_extension_init
        .insert(codex_extension_api::IsolatedSessionExtensions(isolated));
    let tasks = tokio_util::task::TaskTracker::new();
    let thread = manager
        .start_thread_until(options, std::future::pending(), &tasks)
        .await
        .unwrap();
    assert_eq!(observed.load(Ordering::SeqCst), 1);
    assert_eq!(started.load(Ordering::SeqCst), 1);
    assert_eq!(host.lookups.load(Ordering::SeqCst), 0);
    if let Some(prewarm) = thread.thread.session.take_session_startup_prewarm().await {
        tokio::time::timeout(Duration::from_secs(5), prewarm.abort())
            .await
            .unwrap();
    }
    auth_owner.drain().await.unwrap();
    assert_eq!(host.operations.load(Ordering::SeqCst), 0);
    thread.thread.shutdown_and_wait().await.unwrap();
    tasks.close();
    tokio::time::timeout(Duration::from_secs(20), tasks.wait())
        .await
        .unwrap();
}

struct ManagedResumeWitness(Arc<AtomicUsize>);
impl codex_extension_api::ThreadLifecycleContributor<crate::config::Config>
    for ManagedResumeWitness
{
    fn on_thread_resume<'a>(
        &'a self,
        input: codex_extension_api::ThreadResumeInput<'a>,
    ) -> codex_extension_api::ExtensionFuture<'a, ()> {
        Box::pin(async move {
            let codex_mcp::McpAttemptAccess::Admitted(work) =
                input.mcp_access.expect("constructor authority")
            else {
                panic!("required resume authority became unscoped");
            };
            let child = work.derive_attempt().expect("derive resume callback work");
            self.0.fetch_add(1, Ordering::SeqCst);
            drop(child);
        })
    }
}

#[tokio::test]
async fn resumed_constructor_keeps_explicit_work_with_emitted_or_deferred_ready() {
    for defer_ready in [false, true] {
        let (_home, mut manager, config) = expiry_manager(true).await;
        let auth_owner = Arc::new(TestAuthTasks::default());
        let host = Arc::new(ManagedRequestHost {
            auth: Arc::downgrade(&auth_owner),
            required: true,
            produce_work: true,
            refuse: false,
            lookups: AtomicUsize::new(0),
            operations: AtomicUsize::new(1),
        });
        let observed = Arc::new(AtomicUsize::new(0));
        let mut global = codex_extension_api::ExtensionRegistryBuilder::new();
        global.turn_start_admission(Arc::new(ManagedRequestAdmission(Arc::clone(&host))));
        let global = Arc::new(global.build());
        Arc::get_mut(&mut manager.state).unwrap().extensions = Arc::clone(&global);
        let mut isolated = global.to_builder();
        isolated
            .thread_lifecycle_contributor(Arc::new(ManagedResumeWitness(Arc::clone(&observed))));
        let mut options = crate::thread_manager::StartThreadOptions::new(config.clone(), None);
        options.account_work = Some(Box::new(ManagedRequestWork(Arc::clone(&host))));
        options.initial_history =
            codex_history::InitialHistory::Resumed(codex_history::ResumedHistory {
                conversation_id: codex_protocol::ThreadId::new(),
                history: Arc::new(Vec::new()),
                history_revision: None,
                rollout_path: None,
            });
        options
            .thread_extension_init
            .insert(codex_extension_api::SessionIsolation::Isolated);
        options
            .thread_extension_init
            .insert(codex_extension_api::IsolatedSessionExtensions(Arc::new(
                isolated.build(),
            )));
        let mut request = crate::thread_manager::ThreadSpawnRequest::new(
            options,
            Arc::clone(&manager.state.auth_manager),
            manager.agent_control_for_config(&config),
        );
        if defer_ready {
            request.ready_notification = crate::thread_manager::ThreadReadyNotification::Defer;
        }
        let thread = manager.state.spawn_thread(request).await.unwrap();
        assert_eq!(observed.load(Ordering::SeqCst), 1);
        assert_eq!(host.lookups.load(Ordering::SeqCst), 0);
        if let Some(prewarm) = thread.thread.session.take_session_startup_prewarm().await {
            tokio::time::timeout(Duration::from_secs(5), prewarm.abort())
                .await
                .unwrap();
        }
        auth_owner.drain().await.unwrap();
        assert_eq!(host.operations.load(Ordering::SeqCst), 0);
        thread.thread.shutdown_and_wait().await.unwrap();
    }
}

#[tokio::test]
async fn manager_request_capture_uses_original_scope_and_retains_no_idle_lease() {
    let (_home, mut manager, _) = expiry_manager(true).await;
    assert!(manager.capture_request_operation_work().unwrap().is_none());
    let auth_owner = Arc::new(TestAuthTasks::default());
    let host = Arc::new(ManagedRequestHost {
        auth: Arc::downgrade(&auth_owner),
        required: true,
        produce_work: true,
        refuse: false,
        lookups: AtomicUsize::new(0),
        operations: AtomicUsize::new(0),
    });
    let mut extensions = codex_extension_api::ExtensionRegistryBuilder::new();
    extensions.turn_start_admission(Arc::new(ManagedRequestAdmission(host.clone())));
    Arc::get_mut(&mut manager.state).unwrap().extensions = Arc::new(extensions.build());
    assert!(manager.capture_request_operation_work().is_err());
    assert_eq!(host.operations.load(Ordering::SeqCst), 0);
    let work = MANAGED_REQUEST_SCOPE
        .scope((), async {
            manager.capture_request_operation_work().unwrap().unwrap()
        })
        .await;
    assert_eq!(host.lookups.load(Ordering::SeqCst), 2);
    assert_eq!(host.operations.load(Ordering::SeqCst), 1);
    let child = work.derive_operation().unwrap();
    drop(work);
    assert_eq!(host.operations.load(Ordering::SeqCst), 1);
    drop(child);
    assert_eq!(host.operations.load(Ordering::SeqCst), 0);
    for (required, refuse) in [(true, true), (true, false), (false, false)] {
        let auth_owner = Arc::new(TestAuthTasks::default());
        let host = Arc::new(ManagedRequestHost {
            auth: Arc::downgrade(&auth_owner),
            required,
            produce_work: false,
            refuse,
            lookups: AtomicUsize::new(0),
            operations: AtomicUsize::new(0),
        });
        let mut extensions = codex_extension_api::ExtensionRegistryBuilder::new();
        extensions.turn_start_admission(Arc::new(ManagedRequestAdmission(host.clone())));
        Arc::get_mut(&mut manager.state).unwrap().extensions = Arc::new(extensions.build());
        let captured = MANAGED_REQUEST_SCOPE
            .scope((), async { manager.capture_request_operation_work() })
            .await;
        assert_eq!(captured.is_err(), required);
        if let Ok(work) = captured {
            assert!(work.is_none());
        }
        assert_eq!(host.lookups.load(Ordering::SeqCst), 1);
        assert_eq!(host.operations.load(Ordering::SeqCst), 0);
    }
}

#[derive(Debug)]
struct FixtureAuthWork(Arc<AtomicUsize>);
impl Drop for FixtureAuthWork {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl codex_extension_api::HostOperationWork for FixtureAuthWork {
    fn derive_operation(
        &self,
    ) -> Result<Box<dyn codex_extension_api::HostOperationWork>, codex_extension_api::TurnWorkRefused>
    {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(Self(self.0.clone())))
    }
    fn derive_turn_work(
        &self,
        _: &codex_extension_api::ExtensionData,
        _: codex_extension_api::ExtensionFuture<'static, ()>,
    ) -> Result<
        Box<dyn codex_protocol::host_turn_work::HostTurnWork>,
        codex_extension_api::TurnWorkRefused,
    > {
        Err(codex_extension_api::TurnWorkRefused::Unavailable)
    }
}
fn fixture_auth_spawner(
    owner: &Arc<TestAuthTasks>,
) -> (
    Arc<AtomicUsize>,
    Arc<dyn codex_protocol::host_auth_work::AuthTaskSpawner>,
) {
    let count = Arc::new(AtomicUsize::new(1));
    let spawner = owner.spawner(Box::new(FixtureAuthWork(count.clone())));
    (count, spawner)
}
#[tokio::test]
async fn auth_fixture_observer_drop_keeps_original_and_work() {
    let owner = Arc::new(TestAuthTasks::default());
    let (count, spawner) = fixture_auth_spawner(&owner);
    let (send, receive) = oneshot::channel();
    let observation = spawner
        .spawn(Box::pin(async move {
            receive.await.unwrap();
        }))
        .unwrap();
    drop(spawner);
    drop(observation);
    let mut drain = Box::pin(owner.drain());
    assert!(futures::poll!(drain.as_mut()).is_pending());
    drop(drain);
    assert_eq!(count.load(Ordering::SeqCst), 1);
    send.send(()).unwrap();
    owner.drain().await.unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn auth_fixture_registration_precedes_body_poll() {
    let owner = Arc::new(TestAuthTasks::default());
    let (_, spawner) = fixture_auth_spawner(&owner);
    let weak = Arc::downgrade(&owner);
    spawner
        .spawn(Box::pin(async move {
            assert_eq!(
                weak.upgrade()
                    .unwrap()
                    .state
                    .lock()
                    .unwrap()
                    .originals
                    .len(),
                1
            );
        }))
        .unwrap()
        .await
        .unwrap();
    owner.drain().await.unwrap();
}
#[tokio::test]
async fn auth_fixture_factory_cannot_keep_owner_alive_or_poll_after_retirement() {
    let owner = Arc::new(TestAuthTasks::default());
    let (count, spawner) = fixture_auth_spawner(&owner);
    let weak = Arc::downgrade(&owner);
    drop(owner);
    assert!(weak.upgrade().is_none());
    let polled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let child = polled.clone();
    assert!(matches!(
        spawner.spawn(Box::pin(async move {
            child.store(true, Ordering::SeqCst);
        })),
        Err(codex_protocol::host_auth_work::AuthTaskFailure::Unavailable)
    ));
    assert!(!polled.load(Ordering::SeqCst));
    drop(spawner);
    assert_eq!(count.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn auth_fixture_original_panic_is_sticky() {
    let owner = Arc::new(TestAuthTasks::default());
    let (_, spawner) = fixture_auth_spawner(&owner);
    let observation = spawner
        .spawn(Box::pin(async {
            panic!("synthetic fixture original panic");
        }))
        .unwrap();
    assert_eq!(
        observation.await,
        Err(codex_protocol::host_auth_work::AuthTaskFailure::Panicked)
    );
    assert_eq!(
        owner.drain().await,
        Err(codex_protocol::host_auth_work::AuthTaskFailure::Panicked)
    );
    assert_eq!(
        owner.drain().await,
        Err(codex_protocol::host_auth_work::AuthTaskFailure::Panicked)
    );
}
#[tokio::test]
async fn auth_fixture_original_cancellation_is_not_success() {
    let owner = Arc::new(TestAuthTasks::default());
    let (_, spawner) = fixture_auth_spawner(&owner);
    let observation = spawner.spawn(Box::pin(std::future::pending())).unwrap();
    // Only this synthetic pending unit is aborted, never a credential call.
    owner.state.lock().unwrap().originals[0].abort.abort();
    assert_eq!(
        observation.await,
        Err(codex_protocol::host_auth_work::AuthTaskFailure::Cancelled)
    );
    assert_eq!(
        owner.drain().await,
        Err(codex_protocol::host_auth_work::AuthTaskFailure::Cancelled)
    );
}
