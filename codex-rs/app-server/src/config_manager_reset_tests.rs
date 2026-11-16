//! Real configuration/reset participant controls; no whole-inventory claim.
use super::*;
use crate::managed_transition::ResetInventoryError;
use codex_config::{CloudConfigBundleLoadError, CloudConfigBundleLoadErrorCode};
use codex_features::Feature;
use std::panic::{AssertUnwindSafe, catch_unwind};

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
