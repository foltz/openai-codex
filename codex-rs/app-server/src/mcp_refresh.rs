use crate::config_manager::ConfigManager;
use crate::mcp_config_identity::AppliedMcpConfigIdentity;
use crate::mcp_config_identity::McpConfigIdentity;
use codex_core::CodexThread;
use codex_core::ConfigRefreshOutcome;
use codex_core::ThreadManager;
use codex_core::config::Config;
use std::io;
use std::sync::Arc;
use tracing::warn;

pub(crate) async fn reload_mcp_config(
    thread_manager: &Arc<ThreadManager>,
    config_manager: &ConfigManager,
    applied_mcp_config_identity: &AppliedMcpConfigIdentity,
) -> io::Result<()> {
    // Keep the reload state machine out of the request dispatcher's stack frame.
    Box::pin(reload_mcp_config_with_policy(
        thread_manager,
        config_manager,
        applied_mcp_config_identity,
        ReloadPolicy::Strict,
    ))
    .await
}

pub(crate) async fn reload_mcp_config_best_effort(
    thread_manager: &Arc<ThreadManager>,
    config_manager: &ConfigManager,
    applied_mcp_config_identity: &AppliedMcpConfigIdentity,
) {
    if let Err(err) = Box::pin(reload_mcp_config_with_policy(
        thread_manager,
        config_manager,
        applied_mcp_config_identity,
        ReloadPolicy::BestEffort,
    ))
    .await
    {
        warn!(%err, "failed to reload MCP configuration");
    }
}

#[derive(Clone, Copy)]
enum ReloadPolicy {
    Strict,
    BestEffort,
}

/// A strict reload publishes only configuration materialized from the
/// selected user layers it captured under the apply gate.
fn check_selected_identity(
    candidate_identity: Option<&Option<McpConfigIdentity>>,
    next_config: &Config,
) -> io::Result<()> {
    let identity = McpConfigIdentity::from_config(next_config)?;
    if candidate_identity == Some(&identity) {
        Ok(())
    } else {
        Err(io::Error::other(
            "selected MCP configuration changed while reloading",
        ))
    }
}

async fn reload_mcp_config_with_policy(
    thread_manager: &ThreadManager,
    config_manager: &ConfigManager,
    applied_mcp_config_identity: &AppliedMcpConfigIdentity,
    policy: ReloadPolicy,
) -> io::Result<()> {
    // The host owns this whole MCP transition (KESTREL-CODEX-006/011). Holding
    // the gate serializes it with user reloads and core-initiated refreshes;
    // every expected owner below is captured under it, and only the host-owned
    // `refresh_mcp_config` may publish while it is held (never the core-owned
    // `refresh_runtime_config`, which would re-lock this non-reentrant gate).
    let _apply_guard = applied_mcp_config_identity.lock_apply().await;
    if matches!(policy, ReloadPolicy::BestEffort) {
        // A partial best-effort refresh cannot truthfully claim a single applied
        // process-wide MCP configuration. Invalidate before any thread mutation.
        applied_mcp_config_identity.invalidate();
    }
    // Strict only: the selected user layers this reload publishes, captured
    // from the first host layer load under the gate.
    let mut candidate_identity: Option<Option<McpConfigIdentity>> = None;
    let mut rejected = false;
    let mut load_error = None;
    let mut completed: Vec<Arc<CodexThread>> = Vec::new();
    // Bound both discovery and stale-owner retries so concurrent churn cannot
    // keep the reload request alive indefinitely. Completed publications remain valid.
    for pass in 0..3 {
        let host_loaded = match policy {
            ReloadPolicy::BestEffort => true,
            ReloadPolicy::Strict => match config_manager.load_config_layers(/*cwd*/ None).await {
                Ok(layers) => match McpConfigIdentity::from_layers(&layers) {
                    Ok(identity)
                        if candidate_identity
                            .as_ref()
                            .is_none_or(|candidate| *candidate == identity) =>
                    {
                        candidate_identity = Some(identity);
                        true
                    }
                    Ok(_) => {
                        load_error.get_or_insert(io::Error::other(
                            "selected MCP configuration changed while reloading",
                        ));
                        false
                    }
                    Err(err) => {
                        load_error.get_or_insert(err);
                        false
                    }
                },
                Err(err) => {
                    load_error.get_or_insert(err);
                    false
                }
            },
        };
        let mut targets = Vec::new();
        for thread_id in thread_manager.list_thread_ids().await {
            match thread_manager.get_thread(thread_id).await {
                Ok(thread) => {
                    if !completed.iter().any(|done| Arc::ptr_eq(done, &thread)) {
                        targets.push((thread_id, thread));
                    }
                }
                Err(err) => {
                    warn!(%thread_id, %err, "failed to load thread for MCP configuration refresh");
                    if matches!(policy, ReloadPolicy::Strict) {
                        load_error.get_or_insert(io::Error::other(format!(
                            "failed to load thread {thread_id}: {err}"
                        )));
                    }
                }
            }
        }
        if targets.is_empty() {
            break;
        }
        for (thread_id, target) in targets {
            for attempt in 0..3 {
                let current_config = target.config().await;
                let mut target_load_error = None;
                let next_config = if host_loaded {
                    match load_refresh_config(&current_config, config_manager).await {
                        Ok(next_config) => {
                            if matches!(policy, ReloadPolicy::Strict)
                                && let Err(err) = check_selected_identity(
                                    candidate_identity.as_ref(),
                                    &next_config,
                                )
                            {
                                target_load_error = Some(err);
                                disabled_enterprise_config(&current_config)
                            } else {
                                // Thread configuration can change from here on.
                                // Withdraw the applied identity first so an
                                // incomplete or cancelled reload fails closed.
                                applied_mcp_config_identity.invalidate();
                                next_config
                            }
                        }
                        Err(err) => {
                            if matches!(policy, ReloadPolicy::BestEffort) {
                                warn!(%thread_id, %err, "failed to load session configuration");
                            }
                            target_load_error = Some(err);
                            disabled_enterprise_config(&current_config)
                        }
                    }
                } else {
                    disabled_enterprise_config(&current_config)
                };
                match target.refresh_mcp_config(current_config, next_config).await {
                    ConfigRefreshOutcome::Published => {}
                    ConfigRefreshOutcome::Rejected => rejected = true,
                    ConfigRefreshOutcome::Stale => {
                        if attempt == 2 {
                            target.disable_mcp_enterprise_auth().await;
                            rejected = true;
                        }
                        continue;
                    }
                }
                if matches!(policy, ReloadPolicy::Strict)
                    && let Some(err) = target_load_error
                {
                    load_error.get_or_insert(err);
                }
                break;
            }
            completed.push(target);
        }
        if pass == 2 {
            for thread_id in thread_manager.list_thread_ids().await {
                match thread_manager.get_thread(thread_id).await {
                    Ok(thread) if !completed.iter().any(|done| Arc::ptr_eq(done, &thread)) => {
                        thread.disable_mcp_enterprise_auth().await;
                        rejected = true;
                    }
                    Ok(_) => {}
                    Err(err) => {
                        warn!(%thread_id, %err, "failed to load thread for MCP configuration refresh");
                        if matches!(policy, ReloadPolicy::Strict) {
                            load_error.get_or_insert(io::Error::other(format!(
                                "failed to load thread {thread_id}: {err}"
                            )));
                        }
                    }
                }
            }
        }
    }
    // Finish publishing every session's fail-closed configuration before reporting failure.
    if matches!(policy, ReloadPolicy::Strict)
        && let Some(err) = load_error
    {
        return Err(err);
    }
    if rejected && matches!(policy, ReloadPolicy::Strict) {
        return Err(io::Error::other(
            "enterprise MCP configuration was rejected; affected sessions retained their previous configuration with enterprise MCP disabled",
        ));
    }
    // Every current thread published the candidate layers under this gate.
    if matches!(policy, ReloadPolicy::Strict)
        && let Some(candidate_identity) = candidate_identity
    {
        applied_mcp_config_identity.replace(candidate_identity);
    }
    Ok(())
}

fn disabled_enterprise_config(current_config: &Config) -> Config {
    let mut config = current_config.clone();
    config.disable_mcp_enterprise_auth();
    config.config_layer_stack = config
        .config_layer_stack
        .with_cloud_config_binding(/*binding*/ None);
    config
}

async fn load_refresh_config(
    current_config: &Config,
    config_manager: &ConfigManager,
) -> io::Result<Config> {
    config_manager
        .load_latest_config_with_session_layers(
            &current_config.config_layer_stack,
            &current_config.cwd,
        )
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extensions::ThreadExtensionDependencies;
    use crate::extensions::thread_extensions;
    use codex_arg0::Arg0DispatchPaths;
    use codex_config::CloudConfigBundleLoader;
    use codex_config::LoaderOverrides;
    use codex_config::ThreadConfigContext;
    use codex_config::ThreadConfigLoadError;
    use codex_config::ThreadConfigLoadErrorCode;
    use codex_config::ThreadConfigLoader;
    use codex_config::ThreadConfigSource;
    use codex_config::types::AuthKeyringBackendKind;
    use codex_config::types::McpServerConfig;
    use codex_core::ThreadStoreBuilder;
    use codex_core::config::ConfigOverrides;
    use codex_core::init_state_db;
    use codex_exec_server::EnvironmentManager;
    use codex_extension_api::NoopExtensionEventSink;
    use codex_home::CodexHomeUserInstructionsProvider;
    use codex_login::AuthManager;
    use codex_login::CodexAuth;
    use codex_models_manager::manager::ModelCatalogSettings;
    use codex_protocol::protocol::SessionSource;
    use codex_utils_absolute_path::AbsolutePathBuf;
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use tempfile::TempDir;

    #[tokio::test]
    async fn stale_refresh_stops_after_bounded_attempts() -> anyhow::Result<()> {
        let (_temp_dir, thread_manager, config_manager, loader) = refresh_test_state().await?;
        for thread_id in thread_manager.list_thread_ids().await {
            let thread = thread_manager.get_thread(thread_id).await?;
            if thread.config().await.cwd.ends_with("good") {
                *loader.churn_thread.lock().unwrap() = Some(thread);
            }
        }
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            reload_mcp_config(
                &thread_manager,
                &config_manager,
                &crate::mcp_config_identity::AppliedMcpConfigIdentity::default(),
            ),
        )
        .await
        .expect("reload must terminate under owner churn")
        .expect_err("reload failed closed");
        assert_eq!(loader.good_loads.load(Ordering::Relaxed), 3);
        assert_eq!(loader.bad_loads.load(Ordering::Relaxed), 1);
        Ok(())
    }

    #[tokio::test]
    async fn strict_refresh_reports_thread_planning_failures() -> anyhow::Result<()> {
        let (temp_dir, thread_manager, config_manager, _loader) = refresh_test_state().await?;
        let applied_identity = AppliedMcpConfigIdentity::from_startup_config(
            &config_manager.load_latest_config(None).await?,
        );
        let startup_identity = applied_identity.current();
        assert!(startup_identity.is_some());

        std::fs::write(
            temp_dir.path().join(codex_config::CONFIG_TOML_FILE),
            "[features]\nsecret_auth_storage = true\n",
        )?;

        let err = reload_mcp_config(&thread_manager, &config_manager, &applied_identity)
            .await
            .expect_err("strict refresh should fail");

        assert!(
            applied_identity.current().is_none(),
            "a partial refresh must not advertise either identity"
        );
        assert_eq!(err.to_string(), "failed to load refresh config");
        for thread_id in thread_manager.list_thread_ids().await {
            let config = thread_manager.get_thread(thread_id).await?.config().await;
            let failed = config.cwd.ends_with("bad");
            assert_eq!(
                config.auth_keyring_backend_kind(),
                if failed {
                    AuthKeyringBackendKind::Direct
                } else {
                    AuthKeyringBackendKind::Secrets
                }
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn best_effort_refresh_updates_healthy_threads() -> anyhow::Result<()> {
        let (temp_dir, thread_manager, config_manager, loader) = refresh_test_state().await?;
        let applied_identity = AppliedMcpConfigIdentity::from_startup_config(
            &config_manager.load_latest_config(None).await?,
        );
        let startup_identity = applied_identity.current();
        assert!(startup_identity.is_some());

        std::fs::write(
            temp_dir.path().join(codex_config::CONFIG_TOML_FILE),
            "[features]\nsecret_auth_storage = true\n",
        )?;

        reload_mcp_config_best_effort(&thread_manager, &config_manager, &applied_identity).await;

        assert!(
            applied_identity.current().is_none(),
            "a partial refresh must not advertise either identity"
        );
        assert_eq!(loader.good_loads.load(Ordering::Relaxed), 1);
        assert_eq!(loader.bad_loads.load(Ordering::Relaxed), 1);
        for thread_id in thread_manager.list_thread_ids().await {
            let thread = thread_manager.get_thread(thread_id).await?;
            let config = thread.config().await;
            let expected = if config.cwd.ends_with("good") {
                AuthKeyringBackendKind::Secrets
            } else {
                AuthKeyringBackendKind::Direct
            };
            assert_eq!(config.auth_keyring_backend_kind(), expected);
        }
        Ok(())
    }

    #[tokio::test]
    async fn strict_refresh_preserves_config_when_host_config_cannot_load() -> anyhow::Result<()> {
        let (temp_dir, thread_manager, config_manager, _loader) = refresh_test_state().await?;
        let applied_identity = AppliedMcpConfigIdentity::from_startup_config(
            &config_manager.load_latest_config(None).await?,
        );
        let startup_identity = applied_identity.current();
        assert!(startup_identity.is_some());

        std::fs::write(
            temp_dir.path().join(codex_config::CONFIG_TOML_FILE),
            "[features",
        )?;

        assert!(
            reload_mcp_config(&thread_manager, &config_manager, &applied_identity)
                .await
                .is_err()
        );

        assert_eq!(applied_identity.current(), startup_identity);
        for thread_id in thread_manager.list_thread_ids().await {
            let config = thread_manager.get_thread(thread_id).await?.config().await;
            assert_eq!(
                config.auth_keyring_backend_kind(),
                AuthKeyringBackendKind::Direct
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn invalidation_does_not_reload_thread_config() -> anyhow::Result<()> {
        let (_temp_dir, thread_manager, _config_manager, loader) = refresh_test_state().await?;

        thread_manager.invalidate_mcp_runtimes().await;

        assert_eq!(loader.good_loads.load(Ordering::Relaxed), 0);
        assert_eq!(loader.bad_loads.load(Ordering::Relaxed), 0);
        Ok(())
    }

    #[tokio::test]
    async fn mcp_config_reload_only_applies_mcp_inputs() -> anyhow::Result<()> {
        let (temp_dir, thread_manager, config_manager, _loader) = refresh_test_state().await?;
        std::fs::write(
            temp_dir.path().join(codex_config::CONFIG_TOML_FILE),
            "model = \"unrelated-model-change\"\n[features]\nsecret_auth_storage = true\n",
        )?;

        let mut good_thread = None;
        for thread_id in thread_manager.list_thread_ids().await {
            let thread = thread_manager.get_thread(thread_id).await?;
            let thread_config = thread.config().await;
            if thread_config.cwd.ends_with("good") {
                good_thread = Some(thread);
                break;
            }
        }
        let thread = good_thread.expect("good test thread should exist");
        let original_model = thread.config().await.model.clone();

        let current_config = thread.config().await;
        let refresh_config = load_refresh_config(&current_config, &config_manager).await?;
        let _ = thread
            .refresh_mcp_config(current_config, refresh_config)
            .await;

        assert_eq!(
            thread.config().await.auth_keyring_backend_kind(),
            AuthKeyringBackendKind::Secrets
        );
        assert_eq!(thread.config().await.model, original_model);
        Ok(())
    }

    #[tokio::test]
    async fn refresh_config_preserves_thread_mcp_overrides() -> anyhow::Result<()> {
        let (temp_dir, thread_manager, config_manager, _loader) = refresh_test_state().await?;
        let initial_config_manager =
            ConfigManager::without_managed_config_for_tests(temp_dir.path().to_path_buf());
        let thread_config = initial_config_manager
            .load_for_cwd(
                Some(HashMap::from([
                    (
                        "mcp_servers.thread.command".to_string(),
                        json!("thread-mcp"),
                    ),
                    ("mcp_servers.thread.enabled".to_string(), json!(false)),
                ])),
                ConfigOverrides::default(),
                Some(temp_dir.path().join("good")),
            )
            .await?;
        let thread = thread_manager
            .start_thread(codex_core::StartThreadOptions::new(
                thread_config,
                /*control_endpoint*/ None,
            ))
            .await?
            .thread;
        std::fs::write(
            temp_dir.path().join(codex_config::CONFIG_TOML_FILE),
            r#"
[mcp_servers.global]
command = "global-mcp"
enabled = false
"#,
        )?;

        let current_config = thread.config().await;
        let refresh_config = load_refresh_config(&current_config, &config_manager).await?;
        let mut actual = refresh_config.mcp_servers.get().clone();
        actual.remove(codex_mcp::CODEX_APPS_MCP_SERVER_NAME);
        let expected = serde_json::from_value::<HashMap<String, McpServerConfig>>(json!({
            "global": {
                "command": "global-mcp",
                "enabled": false
            },
            "thread": {
                "command": "thread-mcp",
                "enabled": false
            }
        }))?;

        assert_eq!(actual, expected);
        Ok(())
    }

    #[tokio::test]
    async fn strict_refresh_installs_refreshed_thread_mcp_config() -> anyhow::Result<()> {
        let (temp_dir, thread_manager, config_manager, _loader) = refresh_test_state().await?;
        let applied_identity = AppliedMcpConfigIdentity::from_startup_config(
            &config_manager.load_latest_config(None).await?,
        );
        let startup_identity = applied_identity.current();
        assert!(startup_identity.is_some());

        let mut good_thread = None;
        for thread_id in thread_manager.list_thread_ids().await {
            let thread = thread_manager.get_thread(thread_id).await?;
            let thread_config = thread.config().await;
            if thread_config.cwd.ends_with("good") {
                good_thread = Some(thread);
            } else {
                thread_manager.remove_thread(&thread_id).await;
            }
        }
        let thread = good_thread.expect("good test thread should exist");
        // Config loading can finish before a startup is visible to ThreadManager.
        let pending_startup = config_manager
            .load_for_cwd(
                /*request_overrides*/ None,
                ConfigOverrides::default(),
                Some(temp_dir.path().join("good")),
            )
            .await?;
        assert!(
            !pending_startup
                .config_layer_stack
                .mcp_ema_cancellation()
                .is_cancelled()
        );
        // Prediction forks inherit a published config and may outlive their parent.
        let parent = thread_manager
            .start_thread(codex_core::StartThreadOptions::new(
                pending_startup.clone(),
                /*control_endpoint*/ None,
            ))
            .await?;
        let (inherited_fork, _) = thread_manager
            .fork_options_from_parent(parent.thread_id)
            .await?;
        thread_manager.remove_thread(&parent.thread_id).await;
        std::fs::write(
            temp_dir.path().join(codex_config::CONFIG_TOML_FILE),
            r#"
[mcp_servers.refreshed]
command = "refreshed-mcp"
enabled = false
"#,
        )?;

        reload_mcp_config(&thread_manager, &config_manager, &applied_identity).await?;

        let current_identity = McpConfigIdentity::from_config(&thread.config().await)?;
        assert_ne!(current_identity, startup_identity);
        assert_eq!(applied_identity.current(), current_identity);
        let fork = thread_manager.start_thread(inherited_fork).await?;
        assert!(
            fork.thread
                .config()
                .await
                .config_layer_stack
                .mcp_ema_cancellation()
                .is_cancelled()
        );
        fork.thread.shutdown_and_wait().await?;
        parent.thread.shutdown_and_wait().await?;
        assert!(
            pending_startup
                .config_layer_stack
                .mcp_ema_cancellation()
                .is_cancelled()
        );
        assert!(
            thread
                .config()
                .await
                .config_layer_stack
                .mcp_ema_cancellation()
                .is_cancelled()
        );
        let fresh_startup = config_manager
            .load_for_cwd(
                /*request_overrides*/ None,
                ConfigOverrides::default(),
                Some(temp_dir.path().join("good")),
            )
            .await?;
        assert!(
            !fresh_startup
                .config_layer_stack
                .mcp_ema_cancellation()
                .is_cancelled()
        );

        assert!(
            thread
                .config()
                .await
                .mcp_servers
                .get()
                .contains_key("refreshed")
        );
        Ok(())
    }

    async fn refresh_test_state() -> anyhow::Result<(
        TempDir,
        Arc<ThreadManager>,
        ConfigManager,
        Arc<CountingThreadConfigLoader>,
    )> {
        let temp_dir = TempDir::new()?;
        let good_cwd = temp_dir.path().join("good");
        let bad_cwd = temp_dir.path().join("bad");
        std::fs::create_dir_all(&good_cwd)?;
        std::fs::create_dir_all(&bad_cwd)?;
        std::fs::write(
            temp_dir.path().join(codex_config::CONFIG_TOML_FILE),
            "[features]\nsecret_auth_storage = false\n",
        )?;

        let initial_config_manager =
            ConfigManager::without_managed_config_for_tests(temp_dir.path().to_path_buf());
        let good_config = initial_config_manager
            .load_for_cwd(
                /*request_overrides*/ None,
                ConfigOverrides::default(),
                Some(good_cwd.clone()),
            )
            .await?;
        let bad_config = initial_config_manager
            .load_for_cwd(
                /*request_overrides*/ None,
                ConfigOverrides::default(),
                Some(bad_cwd.clone()),
            )
            .await?;

        let auth_manager = AuthManager::from_auth_for_testing(CodexAuth::from_api_key("dummy"));
        let state_db = init_state_db(&good_config)
            .await
            .expect("refresh tests require state db");
        let thread_store =
            ThreadStoreBuilder::from_config(&good_config, Some(state_db.clone())).build();
        let environment_manager = Arc::new(EnvironmentManager::default_for_tests());
        let executor_skill_provider: Arc<dyn codex_skills_extension::SkillProvider> = Arc::new(
            codex_skills_extension::ExecutorSkillProvider::new_with_restriction_product(
                SessionSource::Exec.restriction_product(),
            ),
        );
        let thread_manager = Arc::new_cyclic(|thread_manager| {
            ThreadManager::new(
                &good_config,
                codex_core::build_startup_models(
                    &good_config,
                    auth_manager.clone(),
                    "11111111-1111-4111-8111-111111111111".to_string(),
                    Arc::new(ModelCatalogSettings::disk(
                        good_config.codex_home.to_path_buf(),
                    )),
                ),
                codex_core::CodexAppsToolsCache::default(),
                SessionSource::Exec,
                Arc::clone(&environment_manager),
                thread_extensions(
                    &good_config,
                    ThreadExtensionDependencies {
                        sample_responses: Arc::new(
                            crate::thread_sample::SampleResponseRegistry::default(),
                        ),
                        cloud_skill_provider: None,
                        event_sink: Arc::new(NoopExtensionEventSink),
                        auth_manager: auth_manager.clone(),
                        state_db: Some(state_db.clone()),
                        analytics_events_client: codex_analytics::AnalyticsEventsClient::disabled(),
                        thread_manager: thread_manager.clone(),
                        goal_service: Arc::new(codex_goal_extension::GoalService::new()),
                        environment_manager: Arc::clone(&environment_manager),
                        executor_skill_provider: Arc::clone(&executor_skill_provider),
                        git_attribution_base_url: good_config.chatgpt_base_url.clone(),
                        http_client_factory: good_config.http_client_factory(),
                        queue_service: None,
                        turn_start_admission: None,
                    },
                ),
                Arc::new(CodexHomeUserInstructionsProvider::new(
                    good_config.codex_home.clone(),
                )),
                /*analytics_events_client*/ None,
                codex_core::passthrough_image_store(),
                Arc::clone(&thread_store),
                codex_core::local_agent_graph_store_from_state_db(Some(&state_db)),
                /*attestation_provider*/ None,
                /*external_time_provider*/ None,
            )
        });
        thread_manager
            .start_thread(codex_core::StartThreadOptions::new(
                good_config,
                /*control_endpoint*/ None,
            ))
            .await?;
        thread_manager
            .start_thread(codex_core::StartThreadOptions::new(
                bad_config, /*control_endpoint*/ None,
            ))
            .await?;

        let loader = Arc::new(CountingThreadConfigLoader {
            good_cwd: AbsolutePathBuf::try_from(good_cwd)?,
            bad_cwd: AbsolutePathBuf::try_from(bad_cwd)?,
            good_loads: AtomicUsize::new(0),
            bad_loads: AtomicUsize::new(0),
            churn_thread: std::sync::Mutex::new(None),
        });
        let config_manager = ConfigManager::new(
            temp_dir.path().to_path_buf(),
            Vec::new(),
            LoaderOverrides::without_managed_config_for_tests(),
            /*strict_config*/ false,
            CloudConfigBundleLoader::default(),
            Arg0DispatchPaths::default(),
            loader.clone(),
        );

        Ok((temp_dir, thread_manager, config_manager, loader))
    }

    struct CountingThreadConfigLoader {
        good_cwd: AbsolutePathBuf,
        bad_cwd: AbsolutePathBuf,
        good_loads: AtomicUsize,
        bad_loads: AtomicUsize,
        churn_thread: std::sync::Mutex<Option<Arc<CodexThread>>>,
    }

    impl CountingThreadConfigLoader {
        async fn load(
            &self,
            context: ThreadConfigContext,
        ) -> Result<Vec<ThreadConfigSource>, ThreadConfigLoadError> {
            if context.cwd.as_ref() == Some(&self.good_cwd) {
                self.good_loads.fetch_add(1, Ordering::Relaxed);
                let thread = self.churn_thread.lock().unwrap().clone();
                if let Some(thread) = thread {
                    let current = thread.config().await;
                    let next = current.as_ref().clone();
                    assert_eq!(
                        thread.refresh_runtime_config(current, next).await,
                        ConfigRefreshOutcome::Published
                    );
                }
            }
            if context.cwd.as_ref() == Some(&self.bad_cwd) {
                self.bad_loads.fetch_add(1, Ordering::Relaxed);
                return Err(ThreadConfigLoadError::new(
                    ThreadConfigLoadErrorCode::Internal,
                    /*status_code*/ None,
                    "failed to load refresh config",
                ));
            }
            Ok(Vec::new())
        }
    }

    impl ThreadConfigLoader for CountingThreadConfigLoader {
        fn load(
            &self,
            context: ThreadConfigContext,
        ) -> codex_config::ThreadConfigLoaderFuture<'_, Vec<ThreadConfigSource>> {
            Box::pin(CountingThreadConfigLoader::load(self, context))
        }
    }
}
