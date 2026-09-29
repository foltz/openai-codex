//! Gates access to the retained startup model catalog on current managed provider requirements.

use std::sync::Arc;

use codex_core::config::Config;
use codex_models_manager::manager::ManagedModelsResetError;
use codex_models_manager::manager::RefreshStrategy;
use codex_models_manager::manager::SharedModelsManager;
use codex_protocol::openai_models::ModelPreset;

use crate::config_manager::ConfigManager;

/// Checks the retained catalog route before model/list and background refreshes.
pub(crate) struct ModelCatalog {
    config_manager: ConfigManager,
    config: Arc<Config>,
    models_manager: SharedModelsManager,
}

/// Secret-safe reset failures: provider diagnostics may contain credentials.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ModelCatalogResetError {
    #[error("model catalog provider requirements refused reset")]
    ProviderRequirements,
    #[error("model catalog reset failed: {0:?}")]
    Catalog(ManagedModelsResetError),
}

impl ModelCatalog {
    pub(crate) async fn reset_for_managed_auth(&self) -> Result<(), ModelCatalogResetError> {
        self.config_manager
            .check_thread_model_provider(&self.config)
            .await
            .map_err(|_| ModelCatalogResetError::ProviderRequirements)?;
        self.models_manager
            .reset_for_managed_auth(self.config.http_client_factory())
            .await
            .map_err(ModelCatalogResetError::Catalog)
    }

    pub(crate) fn new(
        config_manager: ConfigManager,
        config: Arc<Config>,
        models_manager: SharedModelsManager,
    ) -> Self {
        Self {
            config_manager,
            config,
            models_manager,
        }
    }

    pub(crate) async fn list_models(
        &self,
        refresh_strategy: RefreshStrategy,
    ) -> std::io::Result<Vec<ModelPreset>> {
        // Check before consulting even a warm cache, just as turn admission checks
        // the retained session route before using it.
        self.config_manager
            .check_thread_model_provider(&self.config)
            .await?;
        Ok(self
            .models_manager
            .list_models(refresh_strategy, self.config.http_client_factory())
            .await)
    }
}
