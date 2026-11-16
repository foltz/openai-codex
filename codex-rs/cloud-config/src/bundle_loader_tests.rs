use super::*;
use crate::backend::BundleClient;
use codex_config::CloudConfigBundle;
use codex_http_client::OutboundProxyPolicy;
use codex_login::CodexAuth;
use pretty_assertions::assert_eq;
use std::sync::atomic::AtomicUsize;

struct UnexpectedFetch(AtomicUsize);

#[tokio::test]
async fn exhausted_generation_never_wraps_or_reopens_ordinary_construction() {
    let home = tempfile::tempdir().expect("home");
    let lifecycle = home_lifecycle(home.path()).expect("lifecycle");
    lifecycle.generation.store(u64::MAX, Ordering::Release);
    for _ in 0..2 {
        assert!(
            managed_cloud_config_bundle_loader(
                AuthManager::from_auth_for_testing(CodexAuth::from_api_key("test-key")),
                "http://127.0.0.1:1".to_owned(),
                home.path().to_path_buf(),
                HttpClientFactory::new(OutboundProxyPolicy::ReqwestDefault),
                &ConfigRequirementsToml::default(),
            )
            .await
            .is_err()
        );
        assert_eq!(lifecycle.generation.load(Ordering::Acquire), u64::MAX);
        assert!(lifecycle.owners.lock().expect("owners").retiring);
    }
}

impl BundleClient for UnexpectedFetch {
    async fn get_bundle(
        &self,
        _auth: &CodexAuth,
    ) -> Result<CloudConfigBundle, crate::backend::BundleRequestError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(CloudConfigBundle::default())
    }
}

#[tokio::test]
async fn storage_constructor_spanning_completed_reset_cannot_adopt_new_generation() {
    for mode in [StorageMode::Cached, StorageMode::Uncached] {
        let home = tempfile::tempdir().expect("home");
        let path = home.path().to_path_buf();
        let client = Arc::new(UnexpectedFetch(AtomicUsize::new(0)));
        let old_auth = AuthManager::from_auth_for_testing(CodexAuth::from_api_key("old-key"));
        let service = CloudConfigBundleService::new(
            old_auth,
            Arc::clone(&client),
            path.clone(),
            CLOUD_CONFIG_BUNDLE_TIMEOUT,
        );
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, held) = tokio::sync::oneshot::channel();
        let constructor = tokio::spawn(storage_loader(
            path.clone(),
            async move {
                // Models the actual constructor future after capturing old auth,
                // before returning the service to the shared registration path.
                entered.send(()).expect("entered");
                held.await.expect("release constructor");
                Ok(service)
            },
            mode,
        ));
        started.await.expect("construction started");
        let fresh = managed_cloud_config_bundle_loader(
            AuthManager::from_auth_for_testing(CodexAuth::from_api_key("new-key")),
            "http://127.0.0.1:1".to_owned(),
            path.clone(),
            HttpClientFactory::new(OutboundProxyPolicy::ReqwestDefault),
            &ConfigRequirementsToml::default(),
        )
        .await
        .expect("managed reset");
        assert_eq!(
            fresh
                .get()
                .await
                .expect("fresh identity has no cloud bundle"),
            None
        );
        release.send(()).expect("release old constructor");
        assert!(constructor.await.expect("join constructor").is_err());
        assert_eq!(client.0.load(Ordering::SeqCst), 0);
        assert!(
            !path
                .join(crate::cache::CLOUD_CONFIG_BUNDLE_CACHE_FILENAME)
                .exists()
        );
    }
}

#[tokio::test]
async fn personal_auth_one_shot_keeps_generation_fence_without_fetching_policy() {
    let home = tempfile::tempdir().expect("home");
    let mut config = codex_core::config::ConfigBuilder::default()
        .codex_home(home.path().to_path_buf())
        .build()
        .await
        .unwrap()
        .auth_config();
    // Known non-managed plan skips policy fetch; the deliberately invalid HTTP
    // policy endpoint also discriminates an unexpected network/validation path.
    config.chatgpt_base_url = Some("http://127.0.0.1:1".into());
    let auth =
        CodexAuth::from_external_chatgpt_tokens("e30.e30.c2ln", "synthetic-account", Some("plus"))
            .unwrap();
    let loader = managed_config_bundle_loader_for_auth(
        config.clone(),
        &auth,
        &ConfigRequirementsToml::default(),
    )
    .await
    .unwrap();
    assert_eq!(loader.get().await.unwrap(), None);
    let lifecycle = home_lifecycle(home.path()).unwrap();
    lifecycle.retire_owners().await.unwrap();
    assert!(loader.get().await.is_err());
    assert!(loader.get_snapshot().await.bundle.is_err());
    assert!(
        managed_config_bundle_loader_for_auth(config, &auth, &ConfigRequirementsToml::default())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn failed_replacement_constructor_retires_originals_and_keeps_admission_closed() {
    let home = tempfile::tempdir().unwrap();
    let old = CloudConfigBundleService::new(
        AuthManager::from_auth_for_testing(CodexAuth::from_api_key("synthetic-key")),
        Arc::new(UnexpectedFetch(AtomicUsize::new(0))),
        home.path().to_path_buf(),
        CLOUD_CONFIG_BUNDLE_TIMEOUT,
    );
    let (loader, original) = managed_config_bundle_loader_for_service(old).unwrap();
    assert_eq!(loader.get().await.unwrap(), None);
    let result = strict_loader_from_constructor::<crate::chatgpt::ChatGptSource<UnexpectedFetch>>(
        home.path().to_path_buf(),
        || Err(lifecycle_error("synthetic replacement refusal")),
    )
    .await;
    assert!(result.is_err());
    assert!(original.await.unwrap_err().is_cancelled());
    assert!(loader.get().await.is_err());
    let lifecycle = home_lifecycle(home.path()).unwrap();
    assert!(lifecycle.owners.lock().unwrap().retiring);
    let later = CloudConfigBundleService::new(
        AuthManager::from_auth_for_testing(CodexAuth::from_api_key("synthetic-key")),
        Arc::new(UnexpectedFetch(AtomicUsize::new(0))),
        home.path().to_path_buf(),
        CLOUD_CONFIG_BUNDLE_TIMEOUT,
    );
    assert!(managed_config_bundle_loader_for_service(later).is_err());
}
