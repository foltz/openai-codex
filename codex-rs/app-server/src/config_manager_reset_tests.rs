//! Real configuration/reset participant controls; no whole-inventory claim.
use super::*;
use crate::managed_transition::ResetInventoryError;
use codex_config::CloudConfigBundleLoadError;
use codex_config::CloudConfigBundleLoadErrorCode;
use codex_features::Feature;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

#[tokio::test]
async fn reset_refuses_invalid_configuration_instead_of_startup_defaults() {
    let home = tempfile::tempdir().unwrap();
    std::fs::write(home.path().join("config.toml"), "not valid = [").unwrap();
    let manager = ConfigManager::without_managed_config_for_tests(home.path().to_path_buf());
    assert!(
        manager
            .load_startup_config(None)
            .await
            .unwrap()
            .used_defaults
    );
    assert!(matches!(
        manager.load_managed_reset_config().await,
        Err(ResetInventoryError::ConfigLoadUnavailable)
    ));
}

#[tokio::test]
async fn reset_refuses_cloud_error_instead_of_an_empty_policy() {
    let home = tempfile::tempdir().unwrap();
    let manager = ConfigManager::new_for_tests(
        home.path().to_path_buf(),
        Vec::new(),
        LoaderOverrides::without_managed_config_for_tests(),
        CloudConfigBundleLoader::from_getter(|| async {
            Err(CloudConfigBundleLoadError::new(
                CloudConfigBundleLoadErrorCode::Timeout,
                None,
                "synthetic cloud failure",
            ))
        }),
    );
    assert!(matches!(
        manager.load_managed_reset_config().await,
        Err(ResetInventoryError::ConfigLoadUnavailable)
    ));
}

#[tokio::test]
async fn reset_refuses_each_poisoned_input_publication() {
    for which in 0..4 {
        let home = tempfile::tempdir().unwrap();
        let manager = ConfigManager::without_managed_config_for_tests(home.path().to_path_buf());
        assert!(
            catch_unwind(AssertUnwindSafe(|| match which {
                0 => {
                    let _guard = manager.cli_overrides.write().unwrap();
                    panic!("poison cli");
                }
                1 => {
                    let _guard = manager.runtime_feature_enablement.write().unwrap();
                    panic!("poison features");
                }
                2 => {
                    let _guard = manager.cloud_config_bundle.write().unwrap();
                    panic!("poison cloud");
                }
                3 => {
                    let _guard = manager.mcp_ema_cancellation.lock().unwrap();
                    panic!("poison EMA roots");
                }
                _ => unreachable!(),
            }))
            .is_err()
        );
        assert!(
            matches!(
                manager.load_managed_reset_config().await,
                Err(ResetInventoryError::ConfigPublicationUnavailable)
            ),
            "input {which}"
        );
    }
}

#[tokio::test]
async fn ordinary_policy_load_also_refuses_a_poisoned_cloud_publication() {
    let home = tempfile::tempdir().unwrap();
    let manager = ConfigManager::without_managed_config_for_tests(home.path().to_path_buf());
    let config = manager.load_non_project_config().await.unwrap();
    let policy = config.application_network_policy.clone();
    assert!(policy.acquire_for_unsupported_sdk().is_ok());
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let _guard = manager.cloud_config_bundle.write().unwrap();
            panic!("poison cloud publication");
        }))
        .is_err()
    );
    assert!(manager.refresh_application_network_policy().await.is_err());
    assert!(policy.acquire_for_unsupported_sdk().is_err());
}

#[tokio::test]
async fn reset_preserves_explicit_feature_policy_and_runtime_defaults() {
    let home = tempfile::tempdir().unwrap();
    std::fs::write(
        home.path().join("config.toml"),
        "[features]\njs_repl = false\n",
    )
    .unwrap();
    let manager = ConfigManager::without_managed_config_for_tests(home.path().to_path_buf());
    manager
        .extend_runtime_feature_enablement([("js_repl".into(), true)])
        .unwrap();
    let config = manager.load_managed_reset_config().await.unwrap();
    assert!(!config.features.enabled(Feature::JsRepl));
    assert_eq!(
        config.runtime_feature_defaults.get(&Feature::JsRepl),
        Some(&true)
    );
}

#[tokio::test]
async fn reset_applies_unprotected_features_and_actual_arg0_paths() {
    let home = tempfile::tempdir().unwrap();
    let mut manager = ConfigManager::without_managed_config_for_tests(home.path().to_path_buf());
    manager
        .extend_runtime_feature_enablement([("js_repl".into(), true)])
        .unwrap();
    let cli = home.path().join("captured-codex");
    let sandbox = home.path().join("captured-sandbox");
    manager.arg0_paths.codex_self_exe = Some(cli.clone());
    manager.arg0_paths.codex_linux_sandbox_exe = Some(sandbox.clone());
    let config = manager.load_managed_reset_config().await.unwrap();
    assert!(config.features.enabled(Feature::JsRepl));
    assert_eq!(config.codex_self_exe, Some(cli));
    assert_eq!(config.codex_linux_sandbox_exe, Some(sandbox));
    assert_eq!(
        config.application_network_policy,
        manager.network_policy.policy()
    );
}

fn reset_fixture_auth() -> Arc<AuthManager> {
    AuthManager::from_auth_for_testing(codex_login::CodexAuth::from_api_key("synthetic-reset-key"))
}

fn reset_fixture_factory() -> codex_http_client::HttpClientFactory {
    codex_http_client::HttpClientFactory::new(
        codex_http_client::OutboundProxyPolicy::ReqwestDefault,
    )
}

#[tokio::test]
async fn cloud_reset_retires_old_loader_and_publishes_validated_replacement() {
    let home = tempfile::tempdir().unwrap();
    let original = codex_cloud_config::managed_config_bundle_loader(
        reset_fixture_auth(),
        "http://127.0.0.1:1".into(),
        home.path().into(),
        reset_fixture_factory(),
        &codex_config::ConfigRequirementsToml::default(),
    );
    assert_eq!(original.get().await.unwrap(), None);
    let manager = ConfigManager::new_for_tests(
        home.path().into(),
        Vec::new(),
        LoaderOverrides::without_managed_config_for_tests(),
        original.clone(),
    );
    let old_roots = {
        let roots = manager.mcp_ema_cancellation.lock().unwrap();
        (roots.startup.clone(), roots.lifetime.clone())
    };
    manager
        .reset_managed_cloud_config(
            reset_fixture_auth(),
            "http://127.0.0.1:1".into(),
            reset_fixture_factory(),
            &codex_config::ConfigRequirementsToml::default(),
        )
        .await
        .unwrap();
    assert!(original.get().await.is_err());
    assert_eq!(
        manager.current_managed_config_loader().get().await.unwrap(),
        None
    );
    assert!(old_roots.0.is_cancelled() && old_roots.1.is_cancelled());
    let roots = manager.mcp_ema_cancellation.lock().unwrap();
    assert!(!roots.startup.is_cancelled() && !roots.lifetime.is_cancelled());
}

#[tokio::test]
async fn cloud_reset_invalid_config_server_retires_old_work_without_chatgpt_fallback() {
    let home = tempfile::tempdir().unwrap();
    let original = codex_cloud_config::managed_config_bundle_loader(
        reset_fixture_auth(),
        "http://127.0.0.1:1".into(),
        home.path().into(),
        reset_fixture_factory(),
        &codex_config::ConfigRequirementsToml::default(),
    );
    assert_eq!(original.get().await.unwrap(), None);
    let manager = ConfigManager::new_for_tests(
        home.path().into(),
        Vec::new(),
        LoaderOverrides::without_managed_config_for_tests(),
        original.clone(),
    );
    let requirements = codex_config::ConfigRequirementsToml {
        config_server: Some(codex_config::ConfigServer {
            base_url: "http://127.0.0.1:1/not-https".into(),
            oauth: None,
        }),
        ..Default::default()
    };
    assert!(matches!(
        manager
            .reset_managed_cloud_config(
                reset_fixture_auth(),
                "http://127.0.0.1:1".into(),
                reset_fixture_factory(),
                &requirements,
            )
            .await,
        Err(ResetInventoryError::CloudConfigUnavailable)
    ));
    assert!(original.get().await.is_err());
    // A default or a new ChatGPT loader would return Ok(None), hiding refusal.
    assert!(manager.current_managed_config_loader().get().await.is_err());
    let roots = manager.mcp_ema_cancellation.lock().unwrap();
    assert!(roots.startup.is_cancelled() && roots.lifetime.is_cancelled());
}

#[tokio::test]
async fn cloud_reset_refuses_poisoned_cloud_publication_or_ema_roots() {
    for cloud in [true, false] {
        let home = tempfile::tempdir().unwrap();
        let manager = ConfigManager::without_managed_config_for_tests(home.path().into());
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                if cloud {
                    let _guard = manager.cloud_config_bundle.write().unwrap();
                    panic!("poison cloud publication");
                } else {
                    let _guard = manager.mcp_ema_cancellation.lock().unwrap();
                    panic!("poison EMA roots");
                }
            }))
            .is_err()
        );
        assert!(matches!(
            manager
                .reset_managed_cloud_config(
                    reset_fixture_auth(),
                    "http://127.0.0.1:1".into(),
                    reset_fixture_factory(),
                    &codex_config::ConfigRequirementsToml::default(),
                )
                .await,
            Err(ResetInventoryError::ConfigPublicationUnavailable)
        ));
    }
}

#[tokio::test]
async fn ordinary_chatgpt_auth_update_keeps_existing_config_server_owner() {
    let home = tempfile::tempdir().unwrap();
    let bootstrap = ConfigManager::without_managed_config_for_tests(home.path().into())
        .load_non_project_config()
        .await
        .unwrap();
    let selected = codex_config::ConfigServer {
        base_url: "https://synthetic-config.invalid/settings".into(),
        oauth: None,
    };
    let manager = ConfigManager::new_for_tests(
        home.path().into(),
        Vec::new(),
        LoaderOverrides::without_managed_config_for_tests(),
        CloudConfigBundleLoader::default().with_config_server(selected.clone()),
    );
    manager.update_managed_config_loader(reset_fixture_auth(), &bootstrap);
    let current = manager.current_managed_config_loader();
    assert_eq!(current.config_server(), Some(&selected));
    assert_eq!(current.get_snapshot().await.config_server, Some(selected));
}
