use super::*;
use crate::managed_transition::ResetInventoryError;
use codex_http_client::HttpClientFactory;
use codex_http_client::OutboundProxyPolicy;
use codex_login::CodexAuth;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn managed_config_load_refuses_each_poisoned_input() {
    // The upstream thread loader is immutable; only these three inputs can
    // be poisoned. Do not restore the historical mutable-loader design.
    for input in 0..3 {
        let home = tempfile::tempdir().expect("home");
        let manager = manager(home.path());
        let poison = manager.clone();
        assert!(
            std::thread::spawn(move || match input {
                0 => {
                    let _guard = poison.cli_overrides.write().expect("cli lock");
                    panic!("cli");
                }
                1 => {
                    let _guard = poison.cloud_config_bundle.write().expect("cloud lock");
                    panic!("cloud");
                }
                2 => {
                    let _guard = poison
                        .runtime_feature_enablement
                        .write()
                        .expect("feature lock");
                    panic!("features");
                }
                _ => unreachable!(),
            })
            .join()
            .is_err()
        );
        assert_eq!(
            manager.load_managed_reset_config().await.err(),
            Some(ResetInventoryError::ConfigPublicationUnavailable)
        );
    }
}

fn manager(home: &Path) -> ConfigManager {
    ConfigManager::new(
        home.to_path_buf(),
        Vec::new(),
        LoaderOverrides::default(),
        /*strict_config*/ false,
        CloudConfigBundleLoader::new(std::future::pending()),
        Arg0DispatchPaths::default(),
        Arc::new(codex_config::NoopThreadConfigLoader),
    )
}

#[tokio::test]
async fn managed_cloud_reset_publishes_resolved_absence_over_old_loader() {
    let home = tempfile::tempdir().expect("home");
    let manager = manager(home.path());
    manager
        .reset_managed_cloud_config(
            AuthManager::from_auth_for_testing(CodexAuth::from_api_key("test-key")),
            "http://127.0.0.1:1".to_owned(),
            HttpClientFactory::new(OutboundProxyPolicy::ReqwestDefault),
        )
        .await
        .expect("reset");
    let loader = manager.cloud_config_bundle.read().expect("loader").clone();
    assert_eq!(loader.get().await.expect("resolved absence"), None);
}

#[tokio::test]
async fn managed_cloud_reset_refuses_poisoned_publication() {
    let home = tempfile::tempdir().expect("home");
    let manager = manager(home.path());
    let publication = Arc::clone(&manager.cloud_config_bundle);
    assert!(
        std::thread::spawn(move || {
            let _guard = publication.write().expect("publication");
            panic!("poison publication");
        })
        .join()
        .is_err()
    );
    assert_eq!(
        manager
            .reset_managed_cloud_config(
                AuthManager::from_auth_for_testing(CodexAuth::from_api_key("test-key")),
                "http://127.0.0.1:1".to_owned(),
                HttpClientFactory::new(OutboundProxyPolicy::ReqwestDefault),
            )
            .await,
        Err(ResetInventoryError::ConfigPublicationUnavailable)
    );
}
