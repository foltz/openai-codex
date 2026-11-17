use super::*;
use codex_models_manager::manager::{ModelsManager, ModelsManagerFuture, StaticModelsManager};
use codex_models_manager::{CatalogModel, ModelsManagerConfig};
use codex_protocol::openai_models::{ModelInfo, ModelsResponse};
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
