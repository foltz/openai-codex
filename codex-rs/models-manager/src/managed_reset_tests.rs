//! Managed reset proves real cache/publication fencing; no account/provider integration claim.
use super::*;
use pretty_assertions::assert_eq;

impl OpenAiModelsManager {
    async fn seed_managed_models_for_test(&self, entry: ModelsCacheEntry) -> bool {
        let mut current = self.remote_models.write().await;
        self.apply_remote_models(
            &mut current,
            entry,
            CatalogLoadMode::Refresh(RefreshStrategy::Online),
        )
    }
}

#[derive(Debug)]
struct ManagedResetEndpoint {
    identity: Mutex<String>,
    started: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
    calls: AtomicUsize,
    fail: bool,
}

impl ModelsEndpointClient for ManagedResetEndpoint {
    fn identity(&self) -> Option<String> {
        Some(self.identity.lock().expect("identity lock").clone())
    }

    fn has_command_auth(&self) -> bool {
        false
    }

    fn uses_codex_backend(&self) -> ModelsEndpointFuture<'_, bool> {
        Box::pin(async { true })
    }

    fn list_models<'a>(
        &'a self,
        _client_version: &'a str,
        _http_client_factory: HttpClientFactory,
    ) -> ModelsEndpointFuture<'a, CoreResult<ModelsEndpointResponse>> {
        Box::pin(async move {
            let identity = self.identity().expect("test identity");
            if self.fail {
                return Err(codex_protocol::error::CodexErr::InvalidRequest(
                    "test endpoint failure".to_owned(),
                ));
            }
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                self.started.add_permits(1);
                self.release
                    .acquire()
                    .await
                    .expect("release first fetch")
                    .forget();
            }
            let slug = if call == 0 {
                "old-account"
            } else {
                "new-account"
            };
            Ok(ModelsEndpointResponse {
                models: vec![remote_model(slug, slug, /*priority*/ 0)],
                etag: None,
                identity,
            })
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_reset_fences_inflight_refresh_and_replaces_disk_and_memory() {
    let endpoint = Arc::new(ManagedResetEndpoint {
        identity: Mutex::new("account-a".to_owned()),
        started: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
        calls: AtomicUsize::new(0),
        fail: false,
    });
    let home = tempdir().expect("home");
    let manager = Arc::new(openai_manager_for_tests(
        home.path().to_path_buf(),
        endpoint.clone(),
    ));
    let old_manager = Arc::clone(&manager);
    let old_refresh = tokio::spawn(async move {
        old_manager
            .list_models(RefreshStrategy::Online, DEFAULT_HTTP_CLIENT_FACTORY)
            .await
    });
    endpoint
        .started
        .acquire()
        .await
        .expect("old fetch entered")
        .forget();
    *endpoint.identity.lock().expect("identity lock") = "account-b".to_owned();
    let mut reset = manager.reset_for_managed_auth(DEFAULT_HTTP_CLIENT_FACTORY);
    // Poll the reset while the old network response is held. It must wait for
    // the old publication transaction, not acknowledge and let A arrive later.
    std::future::poll_fn(|cx| {
        assert!(reset.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    endpoint.release.add_permits(1);
    old_refresh.await.expect("old worker");
    assert_eq!(reset.await, Ok(()));
    let expected = vec![remote_model(
        "new-account",
        "new-account",
        /*priority*/ 0,
    )];
    assert_eq!(manager.get_remote_models().await, expected);
    let cache = FileModelsCache::new(home.path().join(MODEL_CACHE_FILE), DEFAULT_MODEL_CACHE_TTL);
    assert_eq!(
        cache
            .load(&crate::client_version_to_whole())
            .await
            .expect("cache read")
            .expect("cache entry")
            .models,
        expected
    );
}

#[tokio::test]
async fn managed_reset_reports_endpoint_failure_instead_of_stale_catalog_success() {
    let endpoint = Arc::new(ManagedResetEndpoint {
        identity: Mutex::new("account-a".to_owned()),
        started: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
        calls: AtomicUsize::new(0),
        fail: true,
    });
    let home = tempdir().expect("home");
    let manager = openai_manager_for_tests(home.path().to_path_buf(), endpoint);
    manager
        .seed_managed_models_for_test(ModelsCacheEntry {
            fetched_at: Utc::now(),
            etag: None,
            client_version: Some(crate::client_version_to_whole()),
            identity: Some("account-a".to_owned()),
            models: vec![remote_model(
                "old-account",
                "old-account",
                /*priority*/ 0,
            )],
        })
        .await;
    assert_eq!(
        manager
            .reset_for_managed_auth(DEFAULT_HTTP_CLIENT_FACTORY)
            .await,
        Err(ManagedModelsResetError::Endpoint)
    );
    assert_eq!(
        manager.get_remote_models().await,
        load_remote_models_from_file().expect("bundled catalog")
    );
}

#[tokio::test]
async fn managed_logout_reset_replaces_old_cache_without_network() {
    let home = tempdir().expect("home");
    let old = remote_model("old-account", "old-account", /*priority*/ 0);
    let cache = Arc::new(FileModelsCache::new(
        home.path().join(MODEL_CACHE_FILE),
        DEFAULT_MODEL_CACHE_TTL,
    ));
    cache
        .store(&ModelsCacheEntry {
            fetched_at: Utc::now(),
            etag: Some("old".to_owned()),
            client_version: Some(crate::client_version_to_whole()),
            identity: Some("test-provider".to_owned()),
            models: vec![old.clone()],
        })
        .await
        .expect("seed old disk cache");
    let endpoint = TestModelsEndpoint::without_refresh(Vec::new());
    let manager = OpenAiModelsManager::new_with_cache(
        cache.clone(),
        endpoint.clone(),
        /*auth_manager*/ None,
    );
    assert!(
        manager
            .seed_managed_models_for_test(ModelsCacheEntry {
                fetched_at: Utc::now(),
                etag: None,
                client_version: Some(crate::client_version_to_whole()),
                identity: endpoint.identity(),
                models: vec![old],
            })
            .await
    );
    assert_eq!(
        manager
            .reset_for_managed_auth(DEFAULT_HTTP_CLIENT_FACTORY)
            .await,
        Ok(())
    );
    let bundled = load_remote_models_from_file().expect("bundled catalog");
    assert_eq!(manager.get_remote_models().await, bundled);
    let invalidation = cache
        .load(&crate::client_version_to_whole())
        .await
        .expect("read disk cache")
        .expect("reset persisted");
    assert_eq!(invalidation.models, bundled);
    assert_eq!(endpoint.fetch_count(), 0);
    assert_eq!(invalidation.identity, None);
    assert!(
        !manager
            .try_load_cache(CatalogLoadMode::Refresh(RefreshStrategy::Offline))
            .await,
        "bundled invalidation is not a remote cache hit"
    );
}

#[tokio::test]
async fn managed_reset_refuses_cache_invalidation_failure() {
    let cache = TestModelsCache::failing(/*load_error*/ false, /*store_error*/ true);
    let endpoint = TestModelsEndpoint::new(Vec::new());
    let manager =
        OpenAiModelsManager::new_with_cache(cache, endpoint.clone(), /*auth_manager*/ None);
    assert_eq!(
        manager
            .reset_for_managed_auth(DEFAULT_HTTP_CLIENT_FACTORY)
            .await,
        Err(ManagedModelsResetError::Cache)
    );
    assert_eq!(endpoint.fetch_count(), 0);
}

#[tokio::test]
async fn managed_reset_refuses_auth_change_while_endpoint_is_pending() {
    let endpoint = Arc::new(ManagedResetEndpoint {
        identity: Mutex::new("account-a".to_owned()),
        started: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
        calls: AtomicUsize::new(0),
        fail: false,
    });
    let auth =
        AuthManager::from_auth_for_testing(CodexAuth::create_dummy_chatgpt_auth_for_testing());
    let manager = Arc::new(OpenAiModelsManager::new_without_cache(
        endpoint.clone(),
        Some(Arc::clone(&auth)),
    ));
    let reset_manager = Arc::clone(&manager);
    let reset = tokio::spawn(async move {
        reset_manager
            .reset_for_managed_auth(DEFAULT_HTTP_CLIENT_FACTORY)
            .await
    });
    endpoint
        .started
        .acquire()
        .await
        .expect("reset fetch entered")
        .forget();
    auth.set_external_auth(Arc::new(TestExternalApiKeyAuth))
        .await
        .expect("change auth");
    endpoint.release.add_permits(1);
    assert_eq!(
        reset.await.expect("reset task"),
        Err(ManagedModelsResetError::AuthChanged)
    );
    assert_eq!(
        manager.get_remote_models().await,
        load_remote_models_from_file().expect("bundled catalog")
    );
}

#[tokio::test]
async fn managed_reset_rejects_response_from_superseded_endpoint_identity() {
    let endpoint = Arc::new(ManagedResetEndpoint {
        identity: Mutex::new("account-a".to_owned()),
        started: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
        calls: AtomicUsize::new(0),
        fail: false,
    });
    let home = tempdir().expect("home");
    let manager = Arc::new(openai_manager_for_tests(
        home.path().into(),
        endpoint.clone(),
    ));
    let reset_manager = Arc::clone(&manager);
    let reset = tokio::spawn(async move {
        reset_manager
            .reset_for_managed_auth(DEFAULT_HTTP_CLIENT_FACTORY)
            .await
    });
    endpoint
        .started
        .acquire()
        .await
        .expect("fetch entered")
        .forget();
    // No AuthManager signal changes: the endpoint identity is an independent fence.
    *endpoint.identity.lock().expect("identity lock") = "account-b".to_owned();
    endpoint.release.add_permits(1);
    assert_eq!(
        reset.await.expect("reset task"),
        Err(ManagedModelsResetError::AuthChanged)
    );
    assert_eq!(
        manager.get_remote_models().await,
        load_remote_models_from_file().expect("bundled")
    );
    let cache = FileModelsCache::new(home.path().join(MODEL_CACHE_FILE), DEFAULT_MODEL_CACHE_TTL);
    let entry = cache
        .load(&crate::client_version_to_whole())
        .await
        .expect("cache read")
        .expect("invalidation");
    assert_eq!(entry.identity, None);
    assert_eq!(
        entry.models,
        load_remote_models_from_file().expect("bundled")
    );
}

#[tokio::test]
async fn managed_reset_respects_disabled_api_key_discovery_without_poisoning_cache() {
    let home = tempdir().expect("home");
    let models = vec![remote_model("dynamic", "Dynamic", /*priority*/ 0)];
    let endpoint = TestModelsEndpoint::without_refresh(vec![models.clone()]);
    let auth = AuthManager::from_auth_for_testing(CodexAuth::from_api_key("test-key"));
    let manager = OpenAiModelsManager::new(home.path().into(), endpoint.clone(), Some(auth));
    assert_eq!(
        manager
            .reset_for_managed_auth(DEFAULT_HTTP_CLIENT_FACTORY)
            .await,
        Ok(())
    );
    assert_eq!(endpoint.fetch_count(), 0);
    assert_eq!(
        manager.get_remote_models().await,
        load_remote_models_from_file().expect("bundled")
    );

    // The reset's bundled invalidation must not count as a remote cache hit.
    manager.set_api_key_model_discovery_enabled(/*enabled*/ true);
    assert_eq!(
        manager
            .raw_model_catalog(
                RefreshStrategy::OnlineIfUncached,
                DEFAULT_HTTP_CLIENT_FACTORY
            )
            .await
            .models,
        models
    );
    assert_eq!(endpoint.fetch_count(), 1);
}

#[derive(Debug)]
struct PartitionEndpoint {
    leaf: Arc<ManagedResetEndpoint>,
    children: Mutex<std::collections::HashMap<crate::ModelCatalogOriginator, Arc<Self>>>,
}

impl PartitionEndpoint {
    fn new(fail: bool) -> Arc<Self> {
        Arc::new(Self {
            leaf: Arc::new(ManagedResetEndpoint {
                identity: Mutex::new("account-a".to_owned()),
                started: tokio::sync::Semaphore::new(0),
                release: tokio::sync::Semaphore::new(0),
                calls: AtomicUsize::new(0),
                fail,
            }),
            children: Mutex::new(std::collections::HashMap::new()),
        })
    }
}

impl ModelsEndpointClient for PartitionEndpoint {
    fn for_originator(
        &self,
        originator: crate::ModelCatalogOriginator,
    ) -> Option<Arc<dyn ModelsEndpointClient>> {
        self.children
            .lock()
            .unwrap()
            .get(&originator)
            .cloned()
            .map(|endpoint| endpoint as Arc<dyn ModelsEndpointClient>)
    }
    fn identity(&self) -> Option<String> {
        self.leaf.identity()
    }
    fn has_command_auth(&self) -> bool {
        self.leaf.has_command_auth()
    }
    fn uses_codex_backend(&self) -> ModelsEndpointFuture<'_, bool> {
        self.leaf.uses_codex_backend()
    }
    fn list_models<'a>(
        &'a self,
        version: &'a str,
        factory: HttpClientFactory,
    ) -> ModelsEndpointFuture<'a, CoreResult<ModelsEndpointResponse>> {
        self.leaf.list_models(version, factory)
    }
}

async fn entered(endpoint: &ManagedResetEndpoint) {
    tokio::time::timeout(Duration::from_secs(1), endpoint.started.acquire())
        .await
        .expect("endpoint starts within fixture bound")
        .unwrap()
        .forget();
}

async fn poll_pending<T>(future: Pin<&mut (impl Future<Output = T> + ?Sized)>) {
    let mut future = future;
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
}

#[tokio::test]
async fn managed_reset_covers_retained_originator_descendants_and_partitioned_cache() {
    use crate::ModelCatalogOriginator::{Work, WorkWeb};
    let root = PartitionEndpoint::new(false);
    let work = PartitionEndpoint::new(false);
    let grandchild = PartitionEndpoint::new(false);
    root.children.lock().unwrap().insert(Work, work.clone());
    work.children
        .lock()
        .unwrap()
        .insert(WorkWeb, grandchild.clone());
    let home = tempdir().unwrap();
    let manager = Arc::new(
        OpenAiModelsManager::new(home.path().into(), root.clone(), None).with_provider_catalog(),
    );
    let child = manager.for_originator("codex_work_cca").unwrap();
    let nested = child.for_originator("codex_work_web").unwrap();
    root.leaf.release.add_permits(1);
    grandchild.leaf.release.add_permits(1);
    manager
        .list_models(RefreshStrategy::Online, DEFAULT_HTTP_CLIENT_FACTORY)
        .await;
    nested
        .list_models(RefreshStrategy::Online, DEFAULT_HTTP_CLIENT_FACTORY)
        .await;
    let old_child = child.clone();
    let old = tokio::spawn(async move {
        old_child
            .list_models(RefreshStrategy::Online, DEFAULT_HTTP_CLIENT_FACTORY)
            .await
    });
    entered(&work.leaf).await;
    for endpoint in [&root, &work, &grandchild] {
        *endpoint.leaf.identity.lock().unwrap() = "account-b".to_owned();
    }
    let mut reset = manager.reset_for_managed_auth(DEFAULT_HTTP_CLIENT_FACTORY);
    poll_pending(reset.as_mut()).await;
    work.leaf.release.add_permits(1);
    old.await.unwrap();
    assert_eq!(reset.await, Ok(()));
    let expected = vec![remote_model("new-account", "new-account", 0)];
    assert_eq!(manager.get_remote_models().await, expected);
    assert_eq!(child.get_remote_models().await, expected);
    assert_eq!(nested.get_remote_models().await, expected);
    let cache: Arc<dyn ModelsCache> = Arc::new(FileModelsCache::new(
        home.path().join(MODEL_CACHE_FILE),
        DEFAULT_MODEL_CACHE_TTL,
    ));
    let child_cache = cache.for_originator(Work).unwrap();
    let nested_cache = child_cache.for_originator(WorkWeb).unwrap();
    for cache in [cache, child_cache, nested_cache] {
        let entry = cache
            .load(&crate::client_version_to_whole())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(entry.identity.as_deref(), Some("account-b"));
        assert_eq!(entry.models, expected);
    }
}

#[tokio::test]
async fn managed_reset_waits_for_parallel_ordinary_originator_publications() {
    let root = PartitionEndpoint::new(false);
    let work = PartitionEndpoint::new(false);
    root.children
        .lock()
        .unwrap()
        .insert(crate::ModelCatalogOriginator::Work, work.clone());
    let manager = Arc::new(
        OpenAiModelsManager::new_without_cache(root.clone(), None).with_provider_catalog(),
    );
    let child = manager.for_originator("codex_work_cca").unwrap();
    let m = manager.clone();
    let first = tokio::spawn(async move {
        m.list_models(RefreshStrategy::Online, DEFAULT_HTTP_CLIENT_FACTORY)
            .await
    });
    let second = tokio::spawn(async move {
        child
            .list_models(RefreshStrategy::Online, DEFAULT_HTTP_CLIENT_FACTORY)
            .await
    });
    // Both ordinary reads enter before either is released: no accidental family serialization.
    entered(&root.leaf).await;
    entered(&work.leaf).await;
    let mut reset = manager.reset_for_managed_auth(DEFAULT_HTTP_CLIENT_FACTORY);
    poll_pending(reset.as_mut()).await;
    root.leaf.release.add_permits(1);
    first.await.unwrap();
    poll_pending(reset.as_mut()).await;
    work.leaf.release.add_permits(1);
    second.await.unwrap();
    assert_eq!(reset.await, Ok(()));
}

#[tokio::test]
async fn managed_reset_propagates_originator_endpoint_failure() {
    let root = PartitionEndpoint::new(false);
    root.leaf.release.add_permits(1);
    let work = PartitionEndpoint::new(true);
    root.children
        .lock()
        .unwrap()
        .insert(crate::ModelCatalogOriginator::Work, work);
    let manager = OpenAiModelsManager::new_without_cache(root, None).with_provider_catalog();
    let child = manager.for_originator("codex_work_cca").unwrap();
    assert_eq!(
        manager
            .reset_for_managed_auth(DEFAULT_HTTP_CLIENT_FACTORY)
            .await,
        Err(ManagedModelsResetError::Endpoint)
    );
    assert!(child.get_remote_models().await.is_empty());
    assert!(child.refreshed_model_catalog().is_none());
}

#[tokio::test]
async fn managed_reset_clears_provider_provenance_and_watchers_on_refetch_failure() {
    let endpoint = TestModelsEndpoint::with_command_auth(vec![
        Ok(vec![remote_model("prior-provider", "Prior", 0)]),
        Err(CodexErr::InvalidRequest(
            "synthetic catalog refusal".to_owned(),
        )),
    ]);
    let manager = OpenAiModelsManager::new_without_cache(endpoint, None).with_provider_catalog();
    manager
        .list_models(RefreshStrategy::Online, DEFAULT_HTTP_CLIENT_FACTORY)
        .await;
    assert!(manager.refreshed_model_catalog().is_some());
    let mut updates = manager.model_catalog_updates().unwrap();
    updates.borrow_and_update();
    assert_eq!(
        manager
            .reset_for_managed_auth(DEFAULT_HTTP_CLIENT_FACTORY)
            .await,
        Err(ManagedModelsResetError::Endpoint)
    );
    assert!(updates.has_changed().unwrap());
    assert!(updates.borrow().is_none());
    assert!(manager.refreshed_model_catalog().is_none());
    let state = manager.remote_models.read().await;
    assert!(!state.initialized);
    assert!(state.provider_model_ids.is_empty());
    assert!(state.entry.identity.is_none());
    assert!(
        state.entry.models.is_empty(),
        "explicit provider must not acquire bundled fallback"
    );
}

#[tokio::test]
async fn managed_reset_static_catalog_preserves_explicit_authority() {
    let models = vec![remote_model("configured", "Configured", 0)];
    let manager = StaticModelsManager::new(
        None,
        ModelsResponse {
            models: models.clone(),
        },
        ModelOrigin::ExplicitConfig,
    );
    assert_eq!(
        manager
            .reset_for_managed_auth(DEFAULT_HTTP_CLIENT_FACTORY)
            .await,
        Ok(())
    );
    assert_eq!(
        manager
            .model_catalog_with_origins(RefreshStrategy::Online, DEFAULT_HTTP_CLIENT_FACTORY)
            .await,
        vec![CatalogModel {
            model: models[0].clone().into(),
            origin: ModelOrigin::ExplicitConfig
        }]
    );
}

#[derive(Debug)]
struct UnknownResetManager(StaticModelsManager);
impl ModelsManager for UnknownResetManager {
    fn ensure_initialized(&self, factory: HttpClientFactory) -> ModelsManagerFuture<'_, ()> {
        self.0.ensure_initialized(factory)
    }
    fn model_catalog_with_origins(
        &self,
        strategy: RefreshStrategy,
        factory: HttpClientFactory,
    ) -> ModelsManagerFuture<'_, Vec<CatalogModel>> {
        self.0.model_catalog_with_origins(strategy, factory)
    }
    fn get_remote_models(&self) -> ModelsManagerFuture<'_, Vec<ModelInfo>> {
        self.0.get_remote_models()
    }
    fn try_get_remote_models(&self) -> Result<Vec<ModelInfo>, TryLockError> {
        self.0.try_get_remote_models()
    }
    fn auth_manager(&self) -> Option<&AuthManager> {
        self.0.auth_manager()
    }
    fn get_model_info<'a>(
        &'a self,
        model: &'a str,
        config: &'a ModelsManagerConfig,
    ) -> ModelsManagerFuture<'a, CatalogModel> {
        self.0.get_model_info(model, config)
    }
    fn list_collaboration_modes(&self) -> Vec<CollaborationModeMask> {
        self.0.list_collaboration_modes()
    }
    fn refresh_if_new_etag(
        &self,
        etag: String,
        factory: HttpClientFactory,
    ) -> ModelsManagerFuture<'_, ()> {
        self.0.refresh_if_new_etag(etag, factory)
    }
}

#[tokio::test]
async fn managed_reset_unknown_implementation_refuses_acknowledgement() {
    let manager = UnknownResetManager(StaticModelsManager::new(
        None,
        ModelsResponse { models: Vec::new() },
        ModelOrigin::ExplicitConfig,
    ));
    assert_eq!(
        manager
            .reset_for_managed_auth(DEFAULT_HTTP_CLIENT_FACTORY)
            .await,
        Err(ManagedModelsResetError::Unsupported)
    );
}

#[derive(Debug)]
struct ControlledStoreCache {
    inner: Arc<TestModelsCache>,
    calls: AtomicUsize,
    hold_at: usize,
    fail_at: usize,
    entered: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}
impl ControlledStoreCache {
    fn new(hold_at: usize, fail_at: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: TestModelsCache::with_entry(ModelsCacheEntry {
                fetched_at: Utc::now(),
                etag: None,
                client_version: None,
                identity: None,
                models: Vec::new(),
            }),
            calls: AtomicUsize::new(0),
            hold_at,
            fail_at,
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        })
    }
}
impl ModelsCache for ControlledStoreCache {
    fn load<'a>(
        &'a self,
        version: &'a str,
    ) -> ModelsCacheFuture<'a, Result<Option<ModelsCacheEntry>, ModelsCacheError>> {
        self.inner.load(version)
    }
    fn store<'a>(
        &'a self,
        entry: &'a ModelsCacheEntry,
    ) -> ModelsCacheFuture<'a, Result<(), ModelsCacheError>> {
        Box::pin(async move {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call == self.hold_at {
                self.entered.add_permits(1);
                self.release.acquire().await.unwrap().forget();
            }
            if call == self.fail_at {
                return Err(ModelsCacheError::new("synthetic store failure"));
            }
            self.inner.store(entry).await
        })
    }
    fn refresh_ttl<'a>(
        &'a self,
        version: &'a str,
        identity: &'a str,
        etag: &'a str,
    ) -> ModelsCacheFuture<'a, Result<(), ModelsCacheError>> {
        self.inner.refresh_ttl(version, identity, etag)
    }
}

#[tokio::test]
async fn managed_reset_refuses_failed_final_cache_publication() {
    let cache = ControlledStoreCache::new(0, 2);
    let endpoint = TestModelsEndpoint::new(vec![vec![remote_model("new-provider", "New", 0)]]);
    let manager =
        OpenAiModelsManager::new_with_cache(cache.clone(), endpoint, None).with_provider_catalog();
    assert_eq!(
        manager
            .reset_for_managed_auth(DEFAULT_HTTP_CLIENT_FACTORY)
            .await,
        Err(ManagedModelsResetError::Cache)
    );
    assert_eq!(cache.calls.load(Ordering::SeqCst), 2);
    assert!(manager.get_remote_models().await.is_empty());
    assert!(manager.refreshed_model_catalog().is_none());
    assert!(
        cache
            .inner
            .entry
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .identity
            .is_none()
    );
}

#[tokio::test]
async fn managed_reset_refuses_auth_change_during_final_cache_publication() {
    let cache = ControlledStoreCache::new(2, 0);
    let endpoint = TestModelsEndpoint::new(vec![vec![remote_model("new-provider", "New", 0)]]);
    let auth =
        AuthManager::from_auth_for_testing(CodexAuth::create_dummy_chatgpt_auth_for_testing());
    let manager = Arc::new(
        OpenAiModelsManager::new_with_cache(cache.clone(), endpoint, Some(auth.clone()))
            .with_provider_catalog(),
    );
    let m = manager.clone();
    let reset =
        tokio::spawn(async move { m.reset_for_managed_auth(DEFAULT_HTTP_CLIENT_FACTORY).await });
    tokio::time::timeout(Duration::from_secs(1), cache.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    auth.set_external_auth(Arc::new(TestExternalApiKeyAuth))
        .await
        .unwrap();
    cache.release.add_permits(1);
    assert_eq!(
        reset.await.unwrap(),
        Err(ManagedModelsResetError::AuthChanged)
    );
    assert!(manager.get_remote_models().await.is_empty());
    assert!(manager.refreshed_model_catalog().is_none());
}

#[tokio::test]
async fn managed_reset_waits_for_prior_cache_write_before_invalidation() {
    let cache = ControlledStoreCache::new(1, 0);
    let endpoint = TestModelsEndpoint::new(vec![
        vec![remote_model("old-provider", "Old", 0)],
        vec![remote_model("new-provider", "New", 0)],
    ]);
    let manager = Arc::new(
        OpenAiModelsManager::new_with_cache(cache.clone(), endpoint, None).with_provider_catalog(),
    );
    let m = manager.clone();
    let old = tokio::spawn(async move {
        m.list_models(RefreshStrategy::Online, DEFAULT_HTTP_CLIENT_FACTORY)
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), cache.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let mut reset = manager.reset_for_managed_auth(DEFAULT_HTTP_CLIENT_FACTORY);
    poll_pending(reset.as_mut()).await;
    assert_eq!(cache.calls.load(Ordering::SeqCst), 1);
    cache.release.add_permits(1);
    old.await.unwrap();
    assert_eq!(reset.await, Ok(()));
    assert_eq!(
        manager.get_remote_models().await,
        vec![remote_model("new-provider", "New", 0)]
    );
    assert_eq!(
        cache.inner.entry.lock().unwrap().as_ref().unwrap().models,
        vec![remote_model("new-provider", "New", 0)]
    );
}

#[tokio::test]
async fn cancelled_reset_observer_does_not_abort_prior_refresh_and_can_recover() {
    let endpoint = PartitionEndpoint::new(false);
    let manager = Arc::new(
        OpenAiModelsManager::new_without_cache(endpoint.clone(), None).with_provider_catalog(),
    );
    let m = manager.clone();
    let old = tokio::spawn(async move {
        m.list_models(RefreshStrategy::Online, DEFAULT_HTTP_CLIENT_FACTORY)
            .await
    });
    entered(&endpoint.leaf).await;
    let mut reset = manager.reset_for_managed_auth(DEFAULT_HTTP_CLIENT_FACTORY);
    poll_pending(reset.as_mut()).await;
    drop(reset);
    assert!(!old.is_finished());
    assert_eq!(endpoint.leaf.calls.load(Ordering::SeqCst), 1);
    endpoint.leaf.release.add_permits(1);
    old.await.unwrap();
    assert_eq!(
        manager
            .reset_for_managed_auth(DEFAULT_HTTP_CLIENT_FACTORY)
            .await,
        Ok(())
    );
    assert_eq!(endpoint.leaf.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn managed_reset_refuses_poisoned_originator_inventory() {
    let manager = OpenAiModelsManager::new_without_cache(TestModelsEndpoint::new(Vec::new()), None);
    let before = manager.get_remote_models().await;
    let _poison = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = manager.originator_managers.lock().unwrap();
        panic!("synthetic inventory poison");
    }));
    assert_eq!(
        manager
            .reset_for_managed_auth(DEFAULT_HTTP_CLIENT_FACTORY)
            .await,
        Err(ManagedModelsResetError::OriginatorInventory)
    );
    assert_eq!(manager.get_remote_models().await, before);
}

#[tokio::test]
async fn new_originator_created_during_reset_cannot_load_before_family_reopens() {
    let root = PartitionEndpoint::new(false);
    let work = PartitionEndpoint::new(false);
    work.leaf.release.add_permits(1);
    root.children
        .lock()
        .unwrap()
        .insert(crate::ModelCatalogOriginator::Work, work.clone());
    let manager = Arc::new(
        OpenAiModelsManager::new_without_cache(root.clone(), None).with_provider_catalog(),
    );
    let m = manager.clone();
    let reset =
        tokio::spawn(async move { m.reset_for_managed_auth(DEFAULT_HTTP_CLIENT_FACTORY).await });
    entered(&root.leaf).await;
    let child = manager.for_originator("codex_work_cca").unwrap();
    assert!(child.get_remote_models().await.is_empty());
    let mut fetch = child.list_models(RefreshStrategy::Online, DEFAULT_HTTP_CLIENT_FACTORY);
    poll_pending(fetch.as_mut()).await;
    assert_eq!(work.leaf.calls.load(Ordering::SeqCst), 0);
    root.leaf.release.add_permits(1);
    assert_eq!(reset.await.unwrap(), Ok(()));
    fetch.await;
    assert_eq!(work.leaf.calls.load(Ordering::SeqCst), 1);
}

// Logout intent advances the complete source generation even if cached auth is already None.
// The legacy credential revision intentionally stays unchanged in this case.
async fn empty_fixture_auth(home: &Path) -> Arc<AuthManager> {
    let auth = AuthManager::from_auth_for_testing_with_home(
        CodexAuth::from_api_key("synthetic-not-persisted"),
        home.to_path_buf(),
    );
    auth.logout()
        .await
        .expect("clear only disposable fixture auth");
    assert!(auth.auth_cached().is_none());
    auth
}

#[tokio::test]
async fn managed_reset_refuses_logout_intent_without_credential_change_during_endpoint() {
    let home = tempdir().unwrap();
    let auth = empty_fixture_auth(home.path()).await;
    let legacy = auth.auth_change_receiver();
    let complete = auth.auth_change_state_receiver();
    let endpoint = PartitionEndpoint::new(false);
    let manager = Arc::new(
        OpenAiModelsManager::new_without_cache(endpoint.clone(), Some(auth.clone()))
            .with_provider_catalog(),
    );
    let m = manager.clone();
    let reset =
        tokio::spawn(async move { m.reset_for_managed_auth(DEFAULT_HTTP_CLIENT_FACTORY).await });
    entered(&endpoint.leaf).await;
    auth.logout()
        .await
        .expect("repeat logout in disposable fixture");
    assert!(
        !legacy.has_changed().unwrap(),
        "credential content has not changed"
    );
    assert!(
        complete.has_changed().unwrap(),
        "source intent generation has changed"
    );
    endpoint.leaf.release.add_permits(1);
    assert_eq!(
        reset.await.unwrap(),
        Err(ManagedModelsResetError::AuthChanged)
    );
    assert!(manager.get_remote_models().await.is_empty());
    assert!(manager.refreshed_model_catalog().is_none());
}

#[tokio::test]
async fn managed_reset_refuses_logout_intent_without_credential_change_during_cache_write() {
    let home = tempdir().unwrap();
    let auth = empty_fixture_auth(home.path()).await;
    let legacy = auth.auth_change_receiver();
    let complete = auth.auth_change_state_receiver();
    let cache = ControlledStoreCache::new(2, 0);
    let endpoint = TestModelsEndpoint::new(vec![vec![remote_model("new-provider", "New", 0)]]);
    let manager = Arc::new(
        OpenAiModelsManager::new_with_cache(cache.clone(), endpoint, Some(auth.clone()))
            .with_provider_catalog(),
    );
    let m = manager.clone();
    let reset =
        tokio::spawn(async move { m.reset_for_managed_auth(DEFAULT_HTTP_CLIENT_FACTORY).await });
    tokio::time::timeout(Duration::from_secs(1), cache.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    auth.logout()
        .await
        .expect("repeat logout in disposable fixture");
    assert!(!legacy.has_changed().unwrap());
    assert!(complete.has_changed().unwrap());
    cache.release.add_permits(1);
    assert_eq!(
        reset.await.unwrap(),
        Err(ManagedModelsResetError::AuthChanged)
    );
    assert!(manager.get_remote_models().await.is_empty());
    assert!(manager.refreshed_model_catalog().is_none());
}

#[tokio::test]
async fn retired_family_invalidates_without_resolving_failing_old_endpoint() {
    let endpoint = TestModelsEndpoint::with_command_auth(vec![Err(CodexErr::InvalidRequest(
        "retired provider must not be contacted".to_string(),
    ))]);
    let cache = Arc::new(TestModelsCache::default());
    let manager = OpenAiModelsManager::new_with_cache(cache.clone(), endpoint.clone(), None)
        .with_provider_catalog();
    assert_eq!(manager.retire_for_managed_auth().await, Ok(()));
    assert_eq!(endpoint.fetch_count(), 0);
    let entry = cache.entry.lock().unwrap().clone().unwrap();
    assert!(entry.identity.is_none());
    assert!(entry.models.is_empty());
    assert!(manager.refreshed_model_catalog().is_none());
}

#[tokio::test]
async fn retired_family_waits_for_original_cache_publication_without_refetch() {
    let cache = ControlledStoreCache::new(1, 0);
    let endpoint = TestModelsEndpoint::new(vec![vec![remote_model("old-provider", "Old", 0)]]);
    let manager = Arc::new(
        OpenAiModelsManager::new_with_cache(cache.clone(), endpoint.clone(), None)
            .with_provider_catalog(),
    );
    let old_manager = manager.clone();
    let old = tokio::spawn(async move {
        old_manager
            .list_models(RefreshStrategy::Online, DEFAULT_HTTP_CLIENT_FACTORY)
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), cache.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let mut retirement = manager.retire_for_managed_auth();
    poll_pending(retirement.as_mut()).await;
    assert_eq!(cache.calls.load(Ordering::SeqCst), 1);
    cache.release.add_permits(1);
    old.await.unwrap();
    assert_eq!(retirement.await, Ok(()));
    assert_eq!(endpoint.fetch_count(), 1);
    let entry = cache.inner.entry.lock().unwrap().clone().unwrap();
    assert!(entry.identity.is_none());
    assert!(entry.models.is_empty());
}

#[tokio::test]
async fn retirement_requires_explicit_manager_support_and_preserves_static_authority() {
    let static_manager = StaticModelsManager::new(
        None,
        ModelsResponse {
            models: vec![remote_model("configured", "Configured", 0)],
        },
        ModelOrigin::ExplicitConfig,
    );
    assert_eq!(static_manager.retire_for_managed_auth().await, Ok(()));
    assert_eq!(static_manager.get_remote_models().await.len(), 1);
    let unknown = UnknownResetManager(static_manager);
    assert_eq!(
        unknown.retire_for_managed_auth().await,
        Err(ManagedModelsResetError::Unsupported)
    );
}

#[tokio::test]
async fn retired_family_refuses_failed_cache_invalidation() {
    let endpoint = TestModelsEndpoint::with_command_auth(vec![Err(CodexErr::InvalidRequest(
        "retired endpoint must not be contacted".to_string(),
    ))]);
    let manager = OpenAiModelsManager::new_with_cache(
        TestModelsCache::failing(false, true),
        endpoint.clone(),
        None,
    );
    assert_eq!(
        manager.retire_for_managed_auth().await,
        Err(ManagedModelsResetError::Cache)
    );
    assert_eq!(endpoint.fetch_count(), 0);
}

#[tokio::test]
async fn staged_discovery_enables_fresh_reset_without_publishing_early() {
    let endpoint =
        TestModelsEndpoint::without_refresh(vec![vec![remote_model("staged", "Staged", 0)]]);
    let auth = AuthManager::from_auth_for_testing(CodexAuth::from_api_key("synthetic-key"));
    let manager = OpenAiModelsManager::new_without_cache(endpoint.clone(), Some(auth));
    manager.set_api_key_model_discovery_enabled(false);
    assert_eq!(
        manager
            .reset_for_managed_auth_with_discovery(DEFAULT_HTTP_CLIENT_FACTORY, true)
            .await,
        Ok(())
    );
    assert_eq!(endpoint.fetch_count(), 1);
    assert!(!manager.settings.api_key_model_discovery_enabled());
    assert!(manager.remote_models.read().await.entry.identity.is_some());
    manager
        .publish_managed_api_key_model_discovery(true)
        .unwrap();
    assert_eq!(
        manager.get_remote_models().await,
        vec![remote_model("staged", "Staged", 0)]
    );
}

#[tokio::test]
async fn staged_discovery_disables_fetch_despite_old_live_enablement() {
    let endpoint =
        TestModelsEndpoint::without_refresh(vec![vec![remote_model("forbidden", "Forbidden", 0)]]);
    let auth = AuthManager::from_auth_for_testing(CodexAuth::from_api_key("synthetic-key"));
    let manager = OpenAiModelsManager::new_without_cache(endpoint.clone(), Some(auth));
    manager.set_api_key_model_discovery_enabled(true);
    assert_eq!(
        manager
            .reset_for_managed_auth_with_discovery(DEFAULT_HTTP_CLIENT_FACTORY, false)
            .await,
        Ok(())
    );
    assert_eq!(endpoint.fetch_count(), 0);
    assert!(manager.settings.api_key_model_discovery_enabled());
    assert!(manager.remote_models.read().await.entry.identity.is_none());
    manager
        .publish_managed_api_key_model_discovery(false)
        .unwrap();
    assert!(!manager.settings.api_key_model_discovery_enabled());
}

#[tokio::test]
async fn staged_discovery_disable_preserves_command_auth_catalog_refresh() {
    let endpoint = TestModelsEndpoint::with_command_auth(vec![Ok(vec![remote_model(
        "command", "Command", 0,
    )])]);
    let manager = OpenAiModelsManager::new_without_cache(endpoint.clone(), None);
    assert_eq!(
        manager
            .reset_for_managed_auth_with_discovery(DEFAULT_HTTP_CLIENT_FACTORY, false)
            .await,
        Ok(())
    );
    assert_eq!(endpoint.fetch_count(), 1);
}

#[test]
fn managed_discovery_publication_refuses_poison_instead_of_recovering() {
    let settings = ModelCatalogSettings::default();
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _live = settings.live_discovery.lock().unwrap();
            panic!("poison synthetic live discovery publication");
        }))
        .is_err()
    );
    assert_eq!(
        settings.publish_managed_api_key_model_discovery(true),
        Err(ManagedModelsResetError::DiscoveryPublication)
    );
}

#[tokio::test]
async fn staged_reset_and_publication_require_explicit_manager_support() {
    let static_manager = StaticModelsManager::new(
        None,
        ModelsResponse { models: Vec::new() },
        ModelOrigin::ExplicitConfig,
    );
    assert_eq!(
        static_manager
            .reset_for_managed_auth_with_discovery(DEFAULT_HTTP_CLIENT_FACTORY, true)
            .await,
        Ok(())
    );
    assert_eq!(
        static_manager.publish_managed_api_key_model_discovery(true),
        Ok(())
    );
    let unknown = UnknownResetManager(static_manager);
    assert_eq!(
        unknown
            .reset_for_managed_auth_with_discovery(DEFAULT_HTTP_CLIENT_FACTORY, true)
            .await,
        Err(ManagedModelsResetError::Unsupported)
    );
    assert_eq!(
        unknown.publish_managed_api_key_model_discovery(true),
        Err(ManagedModelsResetError::Unsupported)
    );
}
