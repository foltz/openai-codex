//! Process-owned managed-auth reset inventory. Every stage must acknowledge
//! completion before the coordinator can reopen account-work admission.
//!
//! This is not a whole-host shutdown receipt. Existing event-stream cancellation
//! is covered by its managed transport's local retirement, not a detached task
//! join or remote DELETE acknowledgement. User-owned command-hook effects stay
//! outside this process's account-work admission boundary.

use crate::config_manager::ConfigManager;
use crate::managed_transition::ResetInventory;
use crate::managed_transition::ResetInventoryError;
use crate::managed_transition::ResetInventoryFuture;
use crate::model_catalog::ModelCatalog;
use crate::models_refresh_worker::ModelsRefreshShutdown;
use crate::models_refresh_worker::ModelsRefreshWorker;
use crate::otel_reset_control::TelemetryResetControl;
use crate::transport::RemoteControlHandle;
use codex_core::ThreadManager;
use codex_http_client::HttpClientFactory;
use codex_login::AuthManager;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// Holds the same resources used by ordinary processor requests. The adopted
/// credentials are never passed into reset: each participant reads the process
/// AuthManager after the coordinator has installed the authoritative state.
pub(crate) struct ProductionResetInventory {
    pub(crate) telemetry_reset: TelemetryResetControl,
    pub(crate) config_manager: ConfigManager,
    pub(crate) chatgpt_base_url: String,
    pub(crate) thread_manager: Arc<ThreadManager>,
    pub(crate) models_refresh_worker: Arc<Mutex<ModelsRefreshWorker>>,
    pub(crate) model_catalog: Arc<ModelCatalog>,
    pub(crate) http_client_factory: HttpClientFactory,
    pub(crate) auth_manager: Arc<AuthManager>,
    pub(crate) remote_control_handle: Option<RemoteControlHandle>,
}

const RESET_STAGE_TIMEOUT: Duration = Duration::from_secs(30);

impl ResetInventory for ProductionResetInventory {
    fn drive_admitted_constructions(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(self.thread_manager.drive_admitted_constructions())
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "the worker lock retains exclusive observation and replacement custody across reset cancellation"
    )]
    fn reset_all(&self) -> ResetInventoryFuture<'_> {
        Box::pin(async move {
            let telemetry_generation = *self.auth_manager.auth_change_receiver().borrow();
            if let Some(handle) = &self.remote_control_handle {
                handle
                    .reset_auth_cycle()
                    .await
                    .map_err(|_| ResetInventoryError::RemoteControlUnavailable)?;
            }
            let shutdown_report = self
                .thread_manager
                .shutdown_all_threads_bounded(RESET_STAGE_TIMEOUT)
                .await;
            if !shutdown_report.submit_failed.is_empty() || !shutdown_report.timed_out.is_empty() {
                return Err(ResetInventoryError::ThreadsIncomplete {
                    submit_failed: shutdown_report.submit_failed.len(),
                    timed_out: shutdown_report.timed_out.len(),
                });
            }
            if !self
                .thread_manager
                .shutdown_unpublished_constructions_bounded(RESET_STAGE_TIMEOUT)
                .await
            {
                return Err(ResetInventoryError::ConstructorsIncomplete);
            }
            self.thread_manager.invalidate_mcp_runtimes().await;

            // Resolve and publish the strict new-generation cloud loader before
            // rebuilding configuration or deriving residency/telemetry routes.
            tokio::time::timeout(
                RESET_STAGE_TIMEOUT,
                self.config_manager.reset_managed_cloud_config(
                    Arc::clone(&self.auth_manager),
                    self.chatgpt_base_url.clone(),
                    self.http_client_factory.clone(),
                ),
            )
            .await
            .map_err(|_| ResetInventoryError::CloudConfigTimedOut)??;
            let reset_config = tokio::time::timeout(
                RESET_STAGE_TIMEOUT,
                self.config_manager.load_managed_reset_config(),
            )
            .await
            .map_err(|_| ResetInventoryError::ConfigLoadTimedOut)??;
            codex_login::default_client::try_set_default_client_residency_requirement(
                reset_config.enforce_residency.value(),
            )
            .map_err(|_| ResetInventoryError::ResidencyUnavailable)?;
            self.telemetry_reset
                .reset_until(
                    telemetry_generation,
                    Arc::new(reset_config),
                    tokio::time::Instant::now() + RESET_STAGE_TIMEOUT,
                )
                .await
                .map_err(ResetInventoryError::Telemetry)?;

            // Upstream's plugin manager reads this process's shared AuthManager
            // directly; no separate auth-mode snapshot needs updating.
            self.thread_manager
                .plugins_manager()
                .clear_recommended_plugins_cache();
            self.thread_manager
                .plugins_manager()
                .reset_remote_installed_plugins()
                .await
                .map_err(|_| ResetInventoryError::PluginRetirementUnavailable)?;

            // The mutex is the worker's exclusive observation/replacement right.
            // Keep the old owner in the processor census across cancellation or
            // timeout; replacement is allowed only after positive join evidence.
            {
                let deadline = tokio::time::Instant::now() + RESET_STAGE_TIMEOUT;
                let mut worker =
                    tokio::time::timeout_at(deadline, self.models_refresh_worker.lock())
                        .await
                        .map_err(|_| ResetInventoryError::ModelsWorkerUnavailable)?;
                if worker.shutdown_until(deadline).await != ModelsRefreshShutdown::Joined {
                    return Err(ResetInventoryError::ModelsWorkerUnavailable);
                }
                *worker = crate::models_refresh_worker::spawn(&self.model_catalog);
            }

            // Preserve upstream's managed-provider gate as well as the explicit
            // reset acknowledgement; the background worker alone proves neither.
            tokio::time::timeout(
                RESET_STAGE_TIMEOUT,
                self.model_catalog.reset_for_managed_auth(),
            )
            .await
            .map_err(|_| ResetInventoryError::ModelCatalogTimedOut)?
            .map_err(|error| {
                tracing::warn!(outcome = ?error, "managed model catalog reset refused");
                ResetInventoryError::ModelCatalogUnavailable
            })?;
            Ok(())
        })
    }
}
