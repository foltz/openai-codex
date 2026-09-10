use super::*;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn real_plugin_callback_keeps_admission_while_waiting_in_config_queue() -> anyhow::Result<()>
{
    real_plugin_callback_fixture(CallbackFixtureMode::Queued).await
}

#[tokio::test]
async fn real_plugin_callback_keeps_admission_through_actual_batch_write() -> anyhow::Result<()> {
    real_plugin_callback_fixture(CallbackFixtureMode::HeldWrite).await
}

#[cfg(unix)]
#[tokio::test]
async fn failed_real_plugin_write_does_not_release_unrelated_admission() -> anyhow::Result<()> {
    real_plugin_callback_fixture(CallbackFixtureMode::FailedWrite).await
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CallbackFixtureMode {
    Queued,
    HeldWrite,
    #[cfg(unix)]
    FailedWrite,
}

struct WriteFailureObserver(Arc<std::sync::atomic::AtomicBool>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for WriteFailureObserver {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        struct Visitor<'a>(&'a std::sync::atomic::AtomicBool);
        impl tracing::field::Visit for Visitor<'_> {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "error"
                    && format!("{value:?}").contains("failed to write hook trust:")
                {
                    self.0.store(true, Ordering::Release);
                }
            }
        }
        event.record(&mut Visitor(&self.0));
    }
}

#[cfg(unix)]
struct RestoreFixturePermissions(std::path::PathBuf, std::fs::Permissions);

#[cfg(unix)]
impl Drop for RestoreFixturePermissions {
    fn drop(&mut self) {
        std::fs::set_permissions(&self.0, self.1.clone()).expect("restore fixture permissions");
    }
}

async fn real_plugin_callback_fixture(mode: CallbackFixtureMode) -> anyhow::Result<()> {
    use crate::config_manager::ConfigManager;
    use crate::mcp_config_identity::AppliedMcpConfigIdentity;
    use crate::outgoing_message::OutgoingMessageSender;
    use crate::request_processors::ConfigRequestProcessor;
    use crate::request_serialization::RequestSerializationAccess;
    use crate::request_serialization::RequestSerializationQueueKey;
    use crate::request_serialization::RequestSerializationQueues;
    use codex_analytics::AnalyticsEventsClient;
    use codex_core::ThreadManager;
    use codex_core::config::ConfigBuilder;
    use codex_core_plugins::EffectivePluginsChange;
    use codex_core_plugins::remote::RemotePluginMaterialization;
    use codex_core_plugins::remote::RemotePluginScope;
    use codex_core_plugins::remote::RemotePluginShareDiscoverability;
    use tracing_subscriber::prelude::*;

    let write_failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let _subscriber = tracing::subscriber::set_default(
        tracing_subscriber::registry().with(WriteFailureObserver(Arc::clone(&write_failed))),
    );
    let hold_write = mode == CallbackFixtureMode::HeldWrite;
    let home = tempfile::tempdir()?;
    if mode != CallbackFixtureMode::Queued {
        let root = home.path().join("plugins/cache/market/test/local");
        std::fs::create_dir_all(root.join(".codex-plugin"))?;
        std::fs::create_dir_all(root.join("hooks"))?;
        std::fs::write(root.join(".codex-plugin/plugin.json"), r#"{"name":"test"}"#)?;
        std::fs::write(
            root.join("hooks/hooks.json"),
            r#"{"hooks":{"SessionStart":[{"hooks":[{"type":"command","command":"echo fixture"}]}]}}"#,
        )?;
        std::fs::write(
            home.path().join("config.toml"),
            "[features]\nplugins = true\nhooks = true\n[plugins.\"test@market\"]\nenabled = true\n",
        )?;
        app_test_support::write_chatgpt_auth(
            home.path(),
            app_test_support::ChatGptAuthFixture::new("fixture-token").account_id("account-a"),
            codex_config::types::AuthCredentialsStoreMode::File,
        )?;
    }
    let config = ConfigBuilder::default()
        .codex_home(home.path().to_path_buf())
        .build()
        .await?;
    let auth = if mode != CallbackFixtureMode::Queued {
        Arc::new(
            AuthManager::new(
                home.path().to_path_buf(),
                false,
                codex_config::types::AuthCredentialsStoreMode::File,
                None,
                None,
                codex_login::AuthKeyringBackendKind::default(),
                codex_login::test_support::transport_default_auth_route_config(),
            )
            .await,
        )
    } else {
        AuthManager::from_auth_for_testing(codex_login::CodexAuth::from_api_key("test"))
    };
    let threads = Arc::new(ThreadManager::new(
        &config,
        Arc::clone(&auth),
        codex_core::build_models_manager(&config, Arc::clone(&auth)),
        codex_core::CodexAppsToolsCache::default(),
        codex_protocol::protocol::SessionSource::Exec,
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        codex_extension_api::empty_extension_registry(),
        Arc::new(codex_core::test_support::EmptyUserInstructionsProvider),
        None,
        codex_core::thread_store_from_config(&config, None),
        None,
        "test-installation".to_string(),
        None,
        None,
    ));
    let config_manager = ConfigManager::without_managed_config_for_tests(home.path().to_path_buf());
    let (outgoing_tx, _outgoing_rx) = tokio::sync::mpsc::channel(1);
    let applied = AppliedMcpConfigIdentity::from_startup_config(&config);
    let write_gate = if hold_write {
        Some(applied.lock_apply().await)
    } else {
        None
    };
    let processor = ConfigRequestProcessor::new(
        Arc::new(OutgoingMessageSender::new(
            outgoing_tx,
            AnalyticsEventsClient::disabled(),
        )),
        config_manager.clone(),
        Arc::clone(&threads),
        applied,
        AnalyticsEventsClient::disabled(),
    );
    let permits = AccountWorkPermits::new();
    let queues = RequestSerializationQueues::default();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    queues
        .enqueue_background(
            RequestSerializationQueueKey::Global("config"),
            RequestSerializationAccess::Exclusive,
            async move {
                let _ = entered_tx.send(());
                let _ = release_rx.await;
            },
        )
        .await;
    entered_rx.await?;
    let callback = crate::effective_plugin_change::effective_plugins_changed_callback(
        auth,
        Arc::clone(&threads),
        config_manager,
        processor,
        queues.clone(),
        crate::processor_task_retirement::ProcessorTasks::default(),
        permits.clone(),
    );
    let thread_owners_before = Arc::strong_count(&threads);
    let plugins = threads.plugins_manager();
    let mut plugin_input = config.plugins_config_input();
    plugin_input.plugins_enabled = true;
    plugins.plugins_for_config(&plugin_input).await;
    assert!(
        plugins
            .plugin_skill_snapshots_for_config(&plugin_input)
            .is_some()
    );
    let skills = threads.skills_service();
    let skill_input = codex_skills_extension::HostSkillsLoadInput::new(
        config.cwd.clone(),
        Vec::new(),
        config.config_layer_stack.clone(),
        false,
    );
    let skills_before = skills.snapshot_for_config(&skill_input, None).await;
    let change = EffectivePluginsChange {
        materialized_remote_plugins: vec![RemotePluginMaterialization {
            plugin_id: codex_plugin::PluginId::new("test".to_string(), "market".to_string())?,
            scope: RemotePluginScope::Workspace,
            discoverability: Some(RemotePluginShareDiscoverability::Listed),
            authenticated_account_id: Some("account-a".to_string()),
        }],
    };
    permits.close();
    callback(change.clone());
    assert_eq!(permits.admitted_count(), 0);
    assert!(
        plugins
            .plugin_skill_snapshots_for_config(&plugin_input)
            .is_some()
    );
    let skills_after_refusal = skills.snapshot_for_config(&skill_input, None).await;
    assert!(std::ptr::eq(
        skills_before.outcome(),
        skills_after_refusal.outcome()
    ));
    // Spawning either callback owner clones ThreadManager synchronously, so
    // this check does not depend on a refused task getting scheduled.
    assert_eq!(Arc::strong_count(&threads), thread_owners_before);
    assert_eq!(
        queues
            .pending_count_for_tests(&RequestSerializationQueueKey::Global("config"))
            .await,
        0
    );
    permits.reopen();
    let unrelated_request = permits.try_acquire().unwrap();
    callback(change);
    // This second real callback has only invalidation work and can finish
    // while the first callback remains blocked behind the config queue.
    callback(EffectivePluginsChange::default());
    permits.close();
    assert_eq!(permits.admitted_count(), 3);
    // Positive controls ensure these observations detect the actual clear.
    assert!(
        plugins
            .plugin_skill_snapshots_for_config(&plugin_input)
            .is_none()
    );
    let skills_after_admission = skills.snapshot_for_config(&skill_input, None).await;
    assert!(!std::ptr::eq(
        skills_before.outcome(),
        skills_after_admission.outcome()
    ));
    // Observe the actual queued request, not a guessed number of scheduler
    // yields. Its direct ThreadManager and ConfigRequestProcessor own two
    // clones; the invalidation's extra clone must already have been released.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if queues
                .pending_count_for_tests(&RequestSerializationQueueKey::Global("config"))
                .await
                == 1
                && Arc::strong_count(&threads) == thread_owners_before + 2
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_eq!(permits.admitted_count(), 2);
    #[cfg(unix)]
    let config_before_write = std::fs::read(home.path().join("config.toml")).ok();
    #[cfg(unix)]
    let _permissions = if mode == CallbackFixtureMode::FailedWrite {
        use std::os::unix::fs::PermissionsExt;
        let original = std::fs::metadata(home.path())?.permissions();
        let restore = RestoreFixturePermissions(home.path().to_path_buf(), original.clone());
        std::fs::set_permissions(
            home.path(),
            std::fs::Permissions::from_mode(original.mode() & !0o222),
        )?;
        Some(restore)
    } else {
        None
    };
    release_tx.send(()).unwrap();
    if hold_write {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if std::fs::read_to_string(home.path().join("config.toml"))
                    .unwrap_or_default()
                    .contains("trusted_hash")
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
        // The actual batch_write has written the file but cannot return:
        // reload_user_config is still waiting for the held apply gate.
        assert_eq!(permits.admitted_count(), 2);
        assert!(permits.try_acquire().is_none());
    }
    drop(write_gate);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let notified = permits.notified();
            if permits.admitted_count() == 1 {
                break;
            }
            notified.await;
        }
    })
    .await?;
    assert!(permits.try_acquire().is_none());
    #[cfg(unix)]
    if mode == CallbackFixtureMode::FailedWrite {
        assert!(
            write_failed.load(Ordering::Acquire),
            "must observe the actual batch-write error, not an earlier exit"
        );
        assert!(
            !std::fs::read_to_string(home.path().join("config.toml"))?.contains("trusted_hash")
        );
        assert_eq!(
            Some(std::fs::read(home.path().join("config.toml"))?),
            config_before_write
        );
        assert_eq!(permits.admitted_count(), 1);
    }
    drop(unrelated_request);
    assert_eq!(permits.admitted_count(), 0);
    Ok(())
}

#[tokio::test]
async fn cancelling_one_shared_effect_releases_only_its_ownership() {
    let permits = AccountWorkPermits::new();
    let permit = Arc::new(permits.try_acquire().unwrap());
    let other_effect = Arc::clone(&permit);
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let worker = tokio::spawn(async move {
        let _permit = permit;
        let _ = entered_tx.send(());
        std::future::pending::<()>().await;
    });
    entered_rx.await.unwrap();
    permits.close();
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    assert_eq!(permits.admitted_count(), 1);
    assert!(permits.try_acquire().is_none());
    let notified = permits.notified();
    drop(other_effect);
    tokio::time::timeout(Duration::from_secs(1), notified)
        .await
        .unwrap();
    assert_eq!(permits.admitted_count(), 0);
}

#[tokio::test]
async fn shared_permit_survives_enqueue_return_until_both_effect_owners_finish() {
    use crate::request_serialization::RequestSerializationAccess;
    use crate::request_serialization::RequestSerializationQueueKey;
    use crate::request_serialization::RequestSerializationQueues;

    let permits = AccountWorkPermits::new();
    let permit = Arc::new(permits.try_acquire().unwrap());
    let invalidation_permit = Arc::clone(&permit);
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
    let queues = RequestSerializationQueues::default();
    queues
        .enqueue_background(
            RequestSerializationQueueKey::Global("config"),
            RequestSerializationAccess::Exclusive,
            async move {
                let _ = entered_tx.send(());
                let _ = release_rx.await;
                drop(permit);
                let _ = finished_tx.send(());
            },
        )
        .await;
    entered_rx.await.unwrap();
    permits.close();
    assert_eq!(permits.admitted_count(), 1);
    assert!(permits.try_acquire().is_none());
    release_tx.send(()).unwrap();
    finished_rx.await.unwrap();
    // Queue completion alone cannot release the other effect's admission.
    assert_eq!(permits.admitted_count(), 1);
    let notified = permits.notified();
    drop(invalidation_permit);
    tokio::time::timeout(Duration::from_secs(1), notified)
        .await
        .unwrap();
    assert_eq!(permits.admitted_count(), 0);
}

#[test]
fn close_preserves_admitted_work_and_reopen_preserves_its_count() {
    let permits = AccountWorkPermits::new();
    let first = permits.try_acquire().unwrap();
    let second = permits.try_acquire().unwrap();
    permits.close();
    permits.close();
    assert!(permits.try_acquire().is_none());
    assert_eq!(permits.admitted_count(), 2);
    drop(first);
    assert_eq!(permits.admitted_count(), 1);
    permits.reopen();
    assert_eq!(permits.admitted_count(), 1);
    let third = permits.try_acquire().unwrap();
    assert_eq!(permits.admitted_count(), 2);
    permits.close();
    drop(second);
    drop(third);
    assert_eq!(permits.admitted_count(), 0);
    assert!(permits.try_acquire().is_none());
    permits.reopen();
    drop(permits.try_acquire().unwrap());
    assert_eq!(permits.admitted_count(), 0);
}

#[test]
fn exhausted_count_refuses_without_overflowing_into_the_closed_bit() {
    let permits = AccountWorkPermits::new();
    // Private representation fixture; no fabricated live guards are dropped.
    permits
        .inner
        .state
        .store(ACCOUNT_WORK_COUNT_MASK, Ordering::Release);
    assert!(permits.try_acquire().is_none());
    assert_eq!(
        permits.inner.state.load(Ordering::Acquire),
        ACCOUNT_WORK_COUNT_MASK
    );
    permits.close();
    assert_eq!(permits.admitted_count(), ACCOUNT_WORK_COUNT_MASK);
    assert!(permits.try_acquire().is_none());
    permits.reopen();
    assert_eq!(
        permits.inner.state.load(Ordering::Acquire),
        ACCOUNT_WORK_COUNT_MASK
    );
}

#[tokio::test]
async fn release_to_zero_wakes_an_observer_created_before_count_check() {
    let permits = AccountWorkPermits::new();
    let permit = permits.try_acquire().unwrap();
    permits.close();
    let notified = permits.notified();
    assert_eq!(permits.admitted_count(), 1);
    drop(permit);
    tokio::time::timeout(Duration::from_secs(1), notified)
        .await
        .unwrap();
    assert_eq!(permits.admitted_count(), 0);
    assert!(permits.try_acquire().is_none());
}

#[test]
fn concurrent_close_either_refuses_acquisition_or_observes_its_live_guard() {
    // Supplementary scheduling coverage, not a weak-memory model. The proof
    // relies on both operations modifying the same atomic word in production.
    for _ in 0..128 {
        let permits = AccountWorkPermits::new();
        let start = std::sync::Barrier::new(2);
        let (permit, count_at_close) = std::thread::scope(|scope| {
            let acquire = scope.spawn(|| {
                start.wait();
                permits.try_acquire()
            });
            let close = scope.spawn(|| {
                start.wait();
                permits.close();
                permits.admitted_count()
            });
            // The returned guard stays alive in the join result, so a zero
            // count cannot be explained by a successful caller releasing it.
            (acquire.join().unwrap(), close.join().unwrap())
        });
        assert_eq!(count_at_close, u64::from(permit.is_some()));
        assert_eq!(permits.admitted_count(), count_at_close);
        assert!(permits.try_acquire().is_none());
        drop(permit);
        assert_eq!(permits.admitted_count(), 0);
    }
}
