use super::*;
use codex_config::CloudConfigBundle;
use codex_http_client::OutboundProxyPolicy;
use codex_login::CodexAuth;
use pretty_assertions::assert_eq;
use std::sync::atomic::AtomicUsize;

struct UnexpectedFetch(AtomicUsize);

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
