use super::*;
use codex_models_manager::CatalogModel;
use codex_models_manager::ModelsManagerConfig;
use codex_models_manager::manager::ModelsManager;
use codex_models_manager::manager::ModelsManagerFuture;
use codex_models_manager::manager::StaticModelsManager;
use codex_protocol::openai_models::ModelInfo;
use codex_protocol::openai_models::ModelsResponse;
use std::sync::Mutex as StdMutex;

#[derive(Debug)]
struct RecordingManager {
    name: &'static str,
    events: Arc<StdMutex<Vec<String>>>,
    inner: StaticModelsManager,
    fail_retire: bool,
    fail_reset: bool,
}
impl ModelsManager for RecordingManager {
    fn retire_for_managed_auth(
        &self,
    ) -> ModelsManagerFuture<'_, Result<(), ManagedModelsResetError>> {
        Box::pin(async move {
            self.events
                .lock()
                .unwrap()
                .push(format!("{}:retire", self.name));
            if self.fail_retire {
                Err(ManagedModelsResetError::Cache)
            } else {
                Ok(())
            }
        })
    }
    fn reset_for_managed_auth_with_discovery(
        &self,
        _: codex_http_client::HttpClientFactory,
        enabled: bool,
    ) -> ModelsManagerFuture<'_, Result<(), ManagedModelsResetError>> {
        Box::pin(async move {
            self.events
                .lock()
                .unwrap()
                .push(format!("{}:reset:{enabled}", self.name));
            if self.fail_reset {
                Err(ManagedModelsResetError::Endpoint)
            } else {
                Ok(())
            }
        })
    }
    fn ensure_initialized(
        &self,
        factory: codex_http_client::HttpClientFactory,
    ) -> ModelsManagerFuture<'_, ()> {
        self.inner.ensure_initialized(factory)
    }
    fn model_catalog_with_origins(
        &self,
        strategy: RefreshStrategy,
        factory: codex_http_client::HttpClientFactory,
    ) -> ModelsManagerFuture<'_, Vec<CatalogModel>> {
        self.inner.model_catalog_with_origins(strategy, factory)
    }
    fn get_remote_models(&self) -> ModelsManagerFuture<'_, Vec<ModelInfo>> {
        self.inner.get_remote_models()
    }
    fn try_get_remote_models(&self) -> Result<Vec<ModelInfo>, tokio::sync::TryLockError> {
        self.inner.try_get_remote_models()
    }
    fn auth_manager(&self) -> Option<&codex_login::AuthManager> {
        self.inner.auth_manager()
    }
    fn list_collaboration_modes(&self) -> Vec<codex_protocol::config_types::CollaborationModeMask> {
        self.inner.list_collaboration_modes()
    }
    fn get_model_info<'a>(
        &'a self,
        model: &'a str,
        config: &'a ModelsManagerConfig,
    ) -> ModelsManagerFuture<'a, CatalogModel> {
        self.inner.get_model_info(model, config)
    }
    fn refresh_if_new_etag(
        &self,
        etag: String,
        factory: codex_http_client::HttpClientFactory,
    ) -> ModelsManagerFuture<'_, ()> {
        self.inner.refresh_if_new_etag(etag, factory)
    }
}
fn manager(
    name: &'static str,
    events: &Arc<StdMutex<Vec<String>>>,
    fail_retire: bool,
    fail_reset: bool,
) -> SharedModelsManager {
    Arc::new(RecordingManager {
        name,
        events: events.clone(),
        fail_retire,
        fail_reset,
        inner: StaticModelsManager::new(
            None,
            ModelsResponse { models: Vec::new() },
            ModelOrigin::ExplicitConfig,
        ),
    })
}
fn factory() -> codex_http_client::HttpClientFactory {
    codex_http_client::HttpClientFactory::new(
        codex_http_client::OutboundProxyPolicy::ReqwestDefault,
    )
}

#[tokio::test]
async fn old_roots_are_deduplicated_by_identity_before_selected_reset() {
    let events = Arc::new(StdMutex::new(Vec::new()));
    let old = manager("old", &events, false, false);
    let selected = manager("selected", &events, false, false);
    reset_model_roots(old.clone(), old, &selected, factory(), true)
        .await
        .unwrap();
    assert_eq!(
        *events.lock().unwrap(),
        vec!["old:retire", "selected:reset:true"]
    );
}

#[tokio::test]
async fn selected_root_is_reset_once_even_when_all_owners_are_identical() {
    let events = Arc::new(StdMutex::new(Vec::new()));
    let selected = manager("same", &events, false, false);
    reset_model_roots(
        selected.clone(),
        selected.clone(),
        &selected,
        factory(),
        false,
    )
    .await
    .unwrap();
    assert_eq!(*events.lock().unwrap(), vec!["same:reset:false"]);
}

#[tokio::test]
async fn failed_old_family_prevents_selected_refresh() {
    let events = Arc::new(StdMutex::new(Vec::new()));
    let old = manager("old", &events, true, false);
    let selected = manager("selected", &events, false, false);
    assert!(matches!(
        reset_model_roots(old.clone(), old, &selected, factory(), true).await,
        Err(ModelCatalogResetError::Catalog(
            ManagedModelsResetError::Cache
        ))
    ));
    assert_eq!(*events.lock().unwrap(), vec!["old:retire"]);
}

#[tokio::test]
async fn distinct_old_roots_both_retire_and_selected_failure_propagates() {
    let events = Arc::new(StdMutex::new(Vec::new()));
    let startup = manager("startup", &events, false, false);
    let current = manager("current", &events, false, false);
    let selected = manager("selected", &events, false, true);
    assert!(matches!(
        reset_model_roots(startup, current, &selected, factory(), true).await,
        Err(ModelCatalogResetError::Catalog(
            ManagedModelsResetError::Endpoint
        ))
    ));
    assert_eq!(
        *events.lock().unwrap(),
        vec!["startup:retire", "current:retire", "selected:reset:true"]
    );
}

struct SwitchingFactory {
    startup: SharedModelsManager,
    selected: SharedModelsManager,
    builds: std::sync::atomic::AtomicUsize,
    events: Arc<StdMutex<Vec<String>>>,
    fail_publication: bool,
    published: Arc<StdMutex<Option<bool>>>,
}
impl codex_model_provider::ModelCatalogFactory for SwitchingFactory {
    fn build(
        &self,
        _: codex_model_provider::SharedModelProvider,
        _: Option<ModelsResponse>,
        _: bool,
    ) -> SharedModelsManager {
        if self
            .builds
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            == 0
        {
            self.startup.clone()
        } else {
            self.selected.clone()
        }
    }
    fn publish_managed_api_key_model_discovery(
        &self,
        enabled: bool,
    ) -> Result<(), ManagedModelsResetError> {
        self.events
            .lock()
            .unwrap()
            .push(format!("publish:{enabled}"));
        if self.fail_publication {
            Err(ManagedModelsResetError::DiscoveryPublication)
        } else {
            *self.published.lock().unwrap() = Some(enabled);
            Ok(())
        }
    }
}
struct AdapterFixture {
    _home: tempfile::TempDir,
    catalog: ModelCatalog,
    old_config: Arc<Config>,
    startup: SharedModelsManager,
    selected: SharedModelsManager,
    events: Arc<StdMutex<Vec<String>>>,
    published: Arc<StdMutex<Option<bool>>>,
}
async fn adapter_fixture(fail_reset: bool, fail_publication: bool) -> AdapterFixture {
    let home = tempfile::tempdir().unwrap();
    let config_manager = ConfigManager::without_managed_config_for_tests(home.path().to_path_buf());
    let config = Arc::new(config_manager.load_non_project_config().await.unwrap());
    let events = Arc::new(StdMutex::new(Vec::new()));
    let startup_manager = manager("startup", &events, false, false);
    let selected = manager("selected", &events, false, fail_reset);
    let published = Arc::new(StdMutex::new(None));
    let factory = Arc::new(SwitchingFactory {
        startup: startup_manager.clone(),
        selected: selected.clone(),
        builds: std::sync::atomic::AtomicUsize::new(0),
        events: events.clone(),
        fail_publication,
        published: published.clone(),
    });
    let auth = codex_login::AuthManager::from_auth_for_testing_with_home(
        codex_login::CodexAuth::from_api_key("synthetic-unused-key"),
        home.path().to_path_buf(),
    );
    let startup =
        codex_core::build_startup_models(&config, auth, uuid::Uuid::new_v4().to_string(), factory);
    let threads = Arc::new(ThreadManager::new(
        &config,
        startup,
        codex_core::CodexAppsToolsCache::default(),
        codex_protocol::protocol::SessionSource::Exec,
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        Arc::new(codex_extension_api::ExtensionRegistryBuilder::new().build()),
        Arc::new(codex_core::test_support::EmptyUserInstructionsProvider),
        None,
        codex_core::passthrough_image_store(),
        codex_core::ThreadStoreBuilder::from_config(&config, None).build(),
        None,
        None,
        None,
    ));
    let catalog = ModelCatalog::new(config_manager, None, config.clone(), threads);
    std::fs::write(
        home.path().join("config.toml"),
        r#"
model_provider = "replacement"
[model_providers.replacement]
name = "Replacement"
base_url = "https://replacement.invalid/v1"
wire_api = "responses"
"#,
    )
    .unwrap();
    AdapterFixture {
        _home: home,
        catalog,
        old_config: config,
        startup: startup_manager,
        selected,
        events,
        published,
    }
}

#[tokio::test]
async fn actual_adapter_publishes_replacement_only_after_retirement_and_reset() {
    let fixture = adapter_fixture(false, false).await;
    fixture.catalog.reset_for_managed_auth().await.unwrap();
    let state = fixture.catalog.state.lock().await;
    assert_eq!(state.config.model_provider_id, "replacement");
    assert!(Arc::ptr_eq(&state.models_manager, &fixture.selected));
    assert!(!Arc::ptr_eq(&state.config, &fixture.old_config));
    assert!(!state.needs_reload);
    let enabled = state.config.features.enabled(Feature::ApiKeyModelDiscovery);
    assert_eq!(*fixture.published.lock().unwrap(), Some(enabled));
    assert_eq!(
        *fixture.events.lock().unwrap(),
        vec![
            "startup:retire".to_string(),
            format!("selected:reset:{enabled}"),
            format!("publish:{enabled}"),
        ]
    );
}

#[tokio::test]
async fn actual_adapter_failed_selected_reset_keeps_old_state_and_no_publication() {
    let fixture = adapter_fixture(true, false).await;
    assert!(matches!(
        fixture.catalog.reset_for_managed_auth().await,
        Err(ModelCatalogResetError::Catalog(
            ManagedModelsResetError::Endpoint
        ))
    ));
    let state = fixture.catalog.state.lock().await;
    assert!(Arc::ptr_eq(&state.config, &fixture.old_config));
    assert!(Arc::ptr_eq(&state.models_manager, &fixture.startup));
    assert!(state.needs_reload);
    assert_eq!(*fixture.published.lock().unwrap(), None);
    let events = fixture.events.lock().unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0], "startup:retire");
    assert!(events[1].starts_with("selected:reset:"));
}

#[tokio::test]
async fn actual_adapter_failed_feature_publication_keeps_old_state() {
    let fixture = adapter_fixture(false, true).await;
    assert!(matches!(
        fixture.catalog.reset_for_managed_auth().await,
        Err(ModelCatalogResetError::Catalog(
            ManagedModelsResetError::DiscoveryPublication
        ))
    ));
    let state = fixture.catalog.state.lock().await;
    assert!(Arc::ptr_eq(&state.config, &fixture.old_config));
    assert!(Arc::ptr_eq(&state.models_manager, &fixture.startup));
    assert!(state.needs_reload);
    assert_eq!(*fixture.published.lock().unwrap(), None);
    assert_eq!(fixture.events.lock().unwrap().len(), 3);
}
