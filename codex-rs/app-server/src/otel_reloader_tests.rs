use super::*;
use crate::otel_reset_control::TelemetryResetError;
use pretty_assertions::assert_eq;
use tracing_subscriber::prelude::*;

struct WatcherReloadObserved(Arc<tokio::sync::Notify>, &'static str);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for WatcherReloadObserved {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        struct NameVisitor(bool, &'static str);
        impl tracing::field::Visit for NameVisitor {
            fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                if field.name() == "event.name" && value == self.1 {
                    self.0 = true;
                }
            }
        }
        let mut visitor = NameVisitor(false, self.1);
        event.record(&mut visitor);
        if visitor.0 {
            self.0.notify_one();
        }
    }
}

struct CountingThreadConfigLoader {
    calls: std::sync::atomic::AtomicUsize,
    entered: tokio::sync::Notify,
    release: Option<tokio::sync::Notify>,
}

impl codex_config::ThreadConfigLoader for CountingThreadConfigLoader {
    fn load(
        &self,
        _: codex_config::ThreadConfigContext,
    ) -> codex_config::ThreadConfigLoaderFuture<'_, Vec<codex_config::ThreadConfigSource>> {
        Box::pin(async move {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.entered.notify_one();
            if let Some(release) = &self.release {
                release.notified().await;
            }
            Ok(Vec::new())
        })
    }
}

#[tokio::test]
async fn managed_generation_supersedes_inflight_watcher_before_publication() {
    #[derive(Clone, Copy)]
    enum Schedule {
        WatcherOnly,
        ManagedQueued,
        WatcherInFlight,
    }
    for schedule in [
        Schedule::WatcherOnly,
        Schedule::ManagedQueued,
        Schedule::WatcherInFlight,
    ] {
        let home = tempfile::tempdir().unwrap();
        let config = Arc::new(
            codex_core::config::ConfigBuilder::default()
                .codex_home(home.path().to_path_buf())
                .build()
                .await
                .unwrap(),
        );
        let auth = AuthManager::from_auth_for_testing_with_home(
            codex_login::CodexAuth::from_api_key("test-only-key"),
            home.path().to_path_buf(),
        );
        let loader = Arc::new(CountingThreadConfigLoader {
            calls: std::sync::atomic::AtomicUsize::new(0),
            entered: tokio::sync::Notify::new(),
            release: matches!(schedule, Schedule::WatcherInFlight).then(tokio::sync::Notify::new),
        });
        let manager = ConfigManager::new(
            home.path().to_path_buf(),
            Vec::new(),
            codex_config::LoaderOverrides::default(),
            false,
            codex_config::CloudConfigBundleLoader::default(),
            codex_arg0::Arg0DispatchPaths::default(),
            loader.clone(),
        );
        let watcher_published = Arc::new(tokio::sync::Notify::new());
        let (layers, routes) = codex_otel::ManagedTelemetryRoutes::layers();
        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::registry()
                .with(WatcherReloadObserved(
                    watcher_published.clone(),
                    "codex.app_server.otel_reloaded",
                ))
                .with(layers),
        );
        let _default = tracing::dispatcher::set_default(&dispatch);
        let mut candidate = Some(
            codex_core::otel_init::prepare_provider(
                &config,
                "test",
                Some(OTEL_SERVICE_NAME),
                false,
            )
            .unwrap(),
        );
        let initial = routes.publish(&mut candidate).unwrap();
        let cancel = CancellationToken::new();
        let (control, task) = spawn_managed(
            initial,
            routes,
            manager,
            auth.clone(),
            false,
            cancel.clone(),
        );
        // Missing file auth resolves synchronously. Assert the scheduling
        // precondition instead of assuming an await did not run the actor.
        let reload = auth.reload();
        tokio::pin!(reload);
        assert_eq!(
            futures::poll!(reload.as_mut()),
            std::task::Poll::Ready(true)
        );
        let generation = *auth.auth_change_receiver().borrow();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let reset = control.reset_until(generation, config, deadline);
        tokio::pin!(reset);
        match schedule {
            Schedule::ManagedQueued => {
                // Both inputs are queued before the actor can run. It must
                // acknowledge the command without loading watcher config.
                assert!(futures::poll!(reset.as_mut()).is_pending());
                tokio::task::yield_now().await;
                assert_eq!(
                    futures::poll!(reset.as_mut()),
                    std::task::Poll::Ready(Ok(()))
                );
            }
            Schedule::WatcherOnly => {
                tokio::time::timeout_at(deadline, watcher_published.notified())
                    .await
                    .unwrap();
            }
            Schedule::WatcherInFlight => {
                tokio::time::timeout_at(deadline, loader.entered.notified())
                    .await
                    .unwrap();
                assert!(futures::poll!(reset.as_mut()).is_pending());
                tokio::task::yield_now().await;
                // Never release the watcher loader. A managed command must
                // cancel this pre-publication work, not wait for and publish it.
                assert_eq!(reset.await, Ok(()));
            }
        }
        cancel.cancel();
        let owner = tokio::time::timeout_at(deadline, task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(owner.shutdown_result, Some(Ok(())));
        assert_eq!(owner.published_generations, vec![generation]);
        assert_eq!(
            loader.calls.load(std::sync::atomic::Ordering::SeqCst),
            usize::from(!matches!(schedule, Schedule::ManagedQueued))
        );
        assert_eq!(
            owner.managed_generation,
            (!matches!(schedule, Schedule::WatcherOnly)).then_some(generation)
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_partial_candidate_is_unpublished_and_drained_before_successful_retry() {
    let home = tempfile::tempdir().unwrap();
    let config = codex_core::config::ConfigBuilder::default()
        .codex_home(home.path().to_path_buf())
        .build()
        .await
        .unwrap();
    let auth = AuthManager::from_auth_for_testing_with_home(
        codex_login::CodexAuth::from_api_key("test-only-key"),
        home.path().to_path_buf(),
    );
    let (layers, routes) = codex_otel::ManagedTelemetryRoutes::layers();
    let _dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layers));
    let mut candidate = Some(
        codex_core::otel_init::prepare_provider(&config, "test", Some(OTEL_SERVICE_NAME), false)
            .unwrap(),
    );
    let (provider, retired) = routes
        .publish(&mut candidate)
        .unwrap()
        .retire_previous(None);
    let mut state = ManagedReloader {
        provider,
        routes,
        retired: vec![retired],
        rejected: Vec::new(),
        managed_generation: None,
        published_generations: Vec::new(),
        shutdown_result: None,
    };
    let mut invalid = config.clone();
    invalid.analytics_enabled = Some(true);
    let exporter = codex_config::types::OtelExporterKind::OtlpHttp {
        endpoint: "http://127.0.0.1:9/unused".to_owned(),
        headers: Default::default(),
        protocol: codex_config::types::OtelHttpProtocol::Json,
        tls: None,
    };
    invalid.otel.exporter = exporter.clone();
    invalid.otel.metrics_exporter = exporter;
    invalid.otel.trace_exporter = codex_config::types::OtelExporterKind::OtlpHttp {
        endpoint: "https://127.0.0.1:9/unused".to_owned(),
        headers: Default::default(),
        protocol: codex_config::types::OtelHttpProtocol::Json,
        tls: Some(codex_config::types::OtelTlsConfig {
            ca_certificate: Some(home.path().join("missing-ca.pem").try_into().unwrap()),
            ..Default::default()
        }),
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    assert_eq!(
        state.apply(&invalid, 0, &auth, false, deadline).await,
        Err(TelemetryResetError::Unavailable)
    );
    assert_eq!(state.rejected.len(), 1);
    assert!(state.provider.is_none());
    assert!(codex_otel::global().is_none());
    assert_eq!(
        state.apply(&config, 0, &auth, false, deadline).await,
        Ok(())
    );
    assert!(state.rejected.is_empty());
    assert!(state.retired.is_empty());
    assert!(codex_otel::global().is_none());
}

#[tokio::test]
async fn managed_origin_waits_for_command_while_later_ordinary_auth_still_reloads() {
    let home = tempfile::tempdir().unwrap();
    let write_account = |account: &str| {
        app_test_support::write_chatgpt_auth(
            home.path(),
            app_test_support::ChatGptAuthFixture::new("origin-fixture-token").account_id(account),
            codex_config::types::AuthCredentialsStoreMode::File,
        )
        .unwrap();
    };
    write_account("watch-origin-a");
    let config = Arc::new(
        codex_core::config::ConfigBuilder::default()
            .codex_home(home.path().to_path_buf())
            .build()
            .await
            .unwrap(),
    );
    let auth = Arc::new(
        AuthManager::new(
            home.path().to_path_buf(),
            /*enable_codex_api_key_env*/ false,
            codex_config::types::AuthCredentialsStoreMode::File,
            /*forced_chatgpt_workspace_id*/ None,
            /*chatgpt_base_url*/ None,
            codex_login::AuthKeyringBackendKind::default(),
            codex_login::test_support::transport_default_auth_route_config(),
        )
        .await,
    );
    let loader = Arc::new(CountingThreadConfigLoader {
        calls: std::sync::atomic::AtomicUsize::new(0),
        entered: tokio::sync::Notify::new(),
        release: None,
    });
    let manager = ConfigManager::new(
        home.path().to_path_buf(),
        Vec::new(),
        codex_config::LoaderOverrides::default(),
        /*strict_config*/ false,
        codex_config::CloudConfigBundleLoader::default(),
        codex_arg0::Arg0DispatchPaths::default(),
        loader.clone(),
    );
    let watcher_published = Arc::new(tokio::sync::Notify::new());
    let watcher_deferred = Arc::new(tokio::sync::Notify::new());
    let (layers, routes) = codex_otel::ManagedTelemetryRoutes::layers();
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::registry()
            .with(WatcherReloadObserved(
                watcher_published.clone(),
                "codex.app_server.otel_reloaded",
            ))
            .with(WatcherReloadObserved(
                watcher_deferred.clone(),
                "codex.app_server.otel_reload_deferred",
            ))
            .with(layers),
    );
    let _default = tracing::dispatcher::set_default(&dispatch);
    let mut candidate = Some(
        codex_core::otel_init::prepare_provider(
            &config,
            "test",
            Some(OTEL_SERVICE_NAME),
            /*default_analytics_enabled*/ false,
        )
        .unwrap(),
    );
    let initial = routes.publish(&mut candidate).unwrap();
    let cancel = CancellationToken::new();
    let (control, task) = spawn_managed(
        initial,
        routes,
        manager,
        auth.clone(),
        /*default_analytics_enabled*/ false,
        cancel.clone(),
    );

    let before = auth
        .capture_managed_adoption_precondition(Some(&AuthManager::managed_account_fingerprint(
            "watch-origin-a",
        )))
        .unwrap();
    write_account("watch-origin-b");
    let intended = AuthManager::managed_account_fingerprint("watch-origin-b");
    let prepared = auth
        .prepare_managed_adoption(Some(&intended), &before)
        .await
        .unwrap();
    assert!(matches!(
        auth.install_prepared_managed_adoption(&prepared, &before),
        codex_login::auth::ManagedAdoptionInstallOutcome::Installed { .. }
    ));
    let managed = *auth.auth_change_receiver().borrow();
    assert!(auth.is_managed_auth_change(managed));

    // Observe the actor consuming this notification without a command. A pair
    // of scheduler yields is insufficient: config loading can still be pending.
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::select! {
            biased;
            _ = loader.entered.notified() => panic!("managed origin reached the watcher config loader"),
            _ = watcher_deferred.notified() => {}
        }
    })
        .await
        .unwrap();
    assert_eq!(loader.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(
        control
            .reset_until(
                managed,
                config.clone(),
                tokio::time::Instant::now() + Duration::from_secs(10)
            )
            .await,
        Ok(())
    );
    assert_eq!(loader.calls.load(std::sync::atomic::Ordering::SeqCst), 0);

    let same = auth
        .capture_managed_adoption_precondition(Some(&intended))
        .unwrap();
    let changes = auth.auth_change_receiver();
    let prepared = auth
        .prepare_managed_adoption(Some(&intended), &same)
        .await
        .unwrap();
    assert!(matches!(
        auth.install_prepared_managed_adoption(&prepared, &same),
        codex_login::auth::ManagedAdoptionInstallOutcome::Installed { .. }
    ));
    assert!(!changes.has_changed().unwrap());
    assert_eq!(*changes.borrow(), managed);
    assert_eq!(
        control
            .reset_until(
                managed,
                config,
                tokio::time::Instant::now() + Duration::from_secs(10)
            )
            .await,
        Ok(())
    );

    write_account("watch-origin-c");
    auth.reload().await;
    let ordinary = *auth.auth_change_receiver().borrow();
    assert!(ordinary > managed);
    assert!(!auth.is_managed_auth_change(ordinary));
    tokio::time::timeout(Duration::from_secs(10), watcher_published.notified())
        .await
        .unwrap();
    assert_eq!(loader.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    cancel.cancel();
    let owner = task.await.unwrap();
    assert_eq!(owner.shutdown_result, Some(Ok(())));
    assert_eq!(
        owner.published_generations,
        vec![managed, managed, ordinary]
    );
}

#[tokio::test]
async fn managed_reloader_refuses_stale_auth_and_returns_terminal_owners_after_current_reset() {
    let home = tempfile::tempdir().unwrap();
    let config = Arc::new(
        codex_core::config::ConfigBuilder::default()
            .codex_home(home.path().to_path_buf())
            .build()
            .await
            .unwrap(),
    );
    let auth = AuthManager::from_auth_for_testing_with_home(
        codex_login::CodexAuth::from_api_key("test-only-key"),
        home.path().to_path_buf(),
    );
    let manager = ConfigManager::new(
        home.path().to_path_buf(),
        Vec::new(),
        codex_config::LoaderOverrides::default(),
        false,
        codex_config::CloudConfigBundleLoader::default(),
        codex_arg0::Arg0DispatchPaths::default(),
        Arc::new(codex_config::NoopThreadConfigLoader),
    );
    let (layers, routes) = codex_otel::ManagedTelemetryRoutes::layers();
    let _dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layers));
    let mut candidate = Some(
        codex_core::otel_init::prepare_provider(&config, "test", Some(OTEL_SERVICE_NAME), false)
            .unwrap(),
    );
    let initial = routes.publish(&mut candidate).unwrap();
    let cancel = CancellationToken::new();
    let (control, task) = spawn_managed(
        initial,
        routes,
        manager,
        Arc::clone(&auth),
        false,
        cancel.clone(),
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    assert_eq!(
        control.reset_until(0, Arc::clone(&config), deadline).await,
        Ok(())
    );
    // A real authoritative cache change also wakes the ordinary watcher.
    assert!(auth.reload().await);
    let current = *auth.auth_change_receiver().borrow();
    assert_ne!(current, 0);
    assert_eq!(
        control.reset_until(0, Arc::clone(&config), deadline).await,
        Err(TelemetryResetError::AuthChanged)
    );
    assert_eq!(
        control
            .reset_until(current, Arc::clone(&config), deadline)
            .await,
        Ok(())
    );
    // Neither a stale replay nor a future generation can replace the fence
    // established by the current authoritative managed command.
    for rejected in [u64::MAX, 0] {
        assert_eq!(
            control
                .reset_until(rejected, Arc::clone(&config), deadline)
                .await,
            Err(TelemetryResetError::AuthChanged)
        );
    }
    cancel.cancel();
    let owner = tokio::time::timeout_at(deadline, task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(owner.shutdown_result, Some(Ok(())));
    assert_eq!(owner.managed_generation, Some(current));
    assert!(owner.provider.is_none());
    assert!(owner.retired.is_empty());
    assert!(owner.rejected.is_empty());
}
