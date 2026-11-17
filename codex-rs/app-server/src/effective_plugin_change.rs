use codex_core::McpRefreshReason;
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;

use codex_app_server_protocol::ConfigBatchWriteParams;
use codex_app_server_protocol::ConfigEdit;
use codex_app_server_protocol::MergeStrategy;
use codex_core::ThreadManager;
use codex_core_plugins::EffectivePluginsChange;
use codex_core_plugins::remote::RemotePluginMaterialization;
use codex_core_plugins::remote::RemotePluginScope;
use codex_core_plugins::remote::RemotePluginShareDiscoverability;
use codex_login::AuthManager;
use serde_json::json;
use tracing::warn;

use crate::config_manager::ConfigManager;
use crate::request_processors::ConfigRequestProcessor;
use crate::request_serialization::RequestSerializationAccess;
use crate::request_serialization::RequestSerializationQueueKey;
use crate::request_serialization::RequestSerializationQueues;

/// Transfer only the invoking finite root; the reusable callback factory owns
/// no account authority. Dropping the observer never detaches a managed original.
/// Independent callers retain the provider bridge's ordinary Tokio behavior.
pub(crate) fn spawn_plugin_change_task(
    future: impl Future<Output = ()> + Send + 'static,
) -> Result<Option<codex_model_provider::AuthTaskObservation>, codex_model_provider::AuthTaskFailure>
{
    codex_model_provider::spawn_provider_task(Box::pin(future))
}

/// Poll only with the operation's actual auth custody, and hold that work
/// through completion. The weak ticket cannot keep its own census alive.
async fn run_retained_plugin_change(
    ticket: crate::processor_task_retirement::ProcessorTaskTicket,
    work: Option<Box<dyn codex_extension_api::HostOperationWork>>,
    originator: codex_otel::auth_storage::AuthStorageOriginator,
    future: impl Future<Output = ()>,
) {
    let access = match work.as_deref() {
        Some(work) => match work.auth_task_spawner() {
            Ok(spawner) => codex_model_provider::AuthTaskAccess::Admitted(spawner),
            Err(_) => {
                let _ = ticket.mark_unverified();
                return;
            }
        },
        None => codex_model_provider::AuthTaskAccess::Ungated,
    };
    if codex_model_provider::with_auth_task_scope(
        access,
        originator.scope(async move {
            let _work = work;
            future.await;
        }),
    )
    .await
    .is_err()
    {
        let _ = ticket.mark_unverified();
    }
}

/// Callback factories own no operation authority or strong ThreadManager.
/// Each invocation captures finite work; queued trust effects keep it until
/// their actual body completes, not merely until enqueue returns.
pub(crate) fn effective_plugins_changed_callback(
    auth_manager: Arc<AuthManager>,
    thread_manager: Arc<ThreadManager>,
    config_manager: ConfigManager,
    config_processor: ConfigRequestProcessor,
    request_serialization_queues: RequestSerializationQueues,
    tasks: crate::processor_task_retirement::ProcessorTaskTicket,
) -> Arc<dyn Fn(EffectivePluginsChange) + Send + Sync> {
    let thread_manager = Arc::downgrade(&thread_manager);
    let config_processor = config_processor.downgrade();
    Arc::new(move |change| {
        let Some(thread_manager) = thread_manager.upgrade() else {
            return;
        };
        // Cache invalidation is synchronous and safe during the account barrier.
        thread_manager.plugins_manager().clear_cache();
        thread_manager.skills_service().clear_cache();
        let work = match crate::account_turn_admission::derive_request_work() {
            Ok(Some(_)) => thread_manager
                .capture_request_operation_work()
                .map_err(|_| ()),
            Ok(None) => thread_manager
                .admit_background_operation_work()
                .map_err(|_| ()),
            Err(_) => Err(()),
        };
        let Ok(mut work) = work else {
            return;
        };
        let refresh_work = if change.materialized_remote_plugins.is_empty() {
            work.take()
        } else {
            match work
                .as_deref()
                .map(|work| work.derive_operation())
                .transpose()
            {
                Ok(work) => work,
                Err(_) => {
                    let _ = tasks.mark_unverified();
                    return;
                }
            }
        };
        let originator = codex_otel::auth_storage::AuthStorageOriginator::current();
        let refresh_thread_manager = Arc::clone(&thread_manager);
        let reason = change.metadata_change_id.map_or(
            McpRefreshReason::EffectivePluginsChanged,
            McpRefreshReason::InstalledPluginMetadata,
        );
        let refresh_ticket = tasks.clone();
        if let Err(error) = tasks.spawn(async move {
            run_retained_plugin_change(refresh_ticket, refresh_work, originator, async move {
                refresh_thread_manager
                    .invalidate_mcp_runtimes_for(reason)
                    .await;
                refresh_thread_manager.refresh_hook_runtimes().await;
            })
            .await;
        }) {
            warn!(?error, "effective plugin refresh admission closed");
        }
        if change.materialized_remote_plugins.is_empty() {
            return;
        }
        let Some(config_processor) = config_processor.upgrade() else {
            return;
        };
        let auth_manager = Arc::clone(&auth_manager);
        let config_manager = config_manager.clone();
        let queues = request_serialization_queues.clone();
        let trust_ticket = tasks.clone();
        let observer_ticket = tasks.clone();
        if let Err(error) = tasks.spawn(async move {
            let completed = queues
                .enqueue_background_and_wait(
                    RequestSerializationQueueKey::Global("config"),
                    RequestSerializationAccess::Exclusive,
                    async move {
                        run_retained_plugin_change(trust_ticket, work, originator, async move {
                            if let Err(error) = trust_materialized_plugin_hooks(
                                change.materialized_remote_plugins,
                                &auth_manager,
                                &thread_manager,
                                &config_manager,
                                &config_processor,
                            )
                            .await
                            {
                                warn!(%error, "failed to trust materialized plugin hooks");
                            }
                        })
                        .await;
                    },
                )
                .await;
            if completed.is_err() {
                let _ = observer_ticket.mark_unverified();
            }
        }) {
            warn!(?error, "effective plugin trust admission closed");
        }
    })
}

fn workspace_listed_plugin_ids(
    materializations: Vec<RemotePluginMaterialization>,
    current_account_id: &str,
) -> BTreeSet<String> {
    materializations
        .into_iter()
        .filter(|plugin| {
            plugin.scope == RemotePluginScope::Workspace
                && plugin.discoverability == Some(RemotePluginShareDiscoverability::Listed)
                && plugin.authenticated_account_id.as_deref() == Some(current_account_id)
        })
        .map(|plugin| plugin.plugin_id.as_key())
        .collect()
}

fn hook_trusted_hash_edit(hook_key: &str, current_hash: &str) -> ConfigEdit {
    let escaped_hook_key = hook_key.replace('\\', "\\\\").replace('"', "\\\"");
    ConfigEdit {
        key_path: format!(r#"hooks.state."{escaped_hook_key}".trusted_hash"#),
        value: json!(current_hash),
        merge_strategy: MergeStrategy::Replace,
    }
}

pub(crate) async fn trust_materialized_plugin_hooks(
    materializations: Vec<RemotePluginMaterialization>,
    auth_manager: &AuthManager,
    thread_manager: &ThreadManager,
    config_manager: &ConfigManager,
    config_processor: &ConfigRequestProcessor,
) -> Result<(), String> {
    let Some(current_account_id) = auth_manager
        .auth_cached()
        .and_then(|auth| auth.get_account_id())
    else {
        return Ok(());
    };
    let plugin_ids = workspace_listed_plugin_ids(materializations, &current_account_id);
    if plugin_ids.is_empty() {
        return Ok(());
    }
    let config = config_manager
        .load_latest_config(/*fallback_cwd*/ None)
        .await
        .map_err(|err| format!("failed to reload config: {err}"))?;
    let plugin_outcome = thread_manager
        .plugins_manager()
        .plugins_for_config(&config.plugins_config_input())
        .await;
    let hooks_config = codex_hooks::HooksConfig {
        feature_enabled: true,
        bypass_hook_trust: config.bypass_hook_trust,
        config_layer_stack: Some(config.config_layer_stack),
        plugin_hook_sources: plugin_outcome.effective_plugin_hook_sources(),
        plugin_hook_load_warnings: plugin_outcome.effective_plugin_hook_warnings(),
        ..Default::default()
    };
    let hooks =
        codex_model_provider::run_blocking_call(move || codex_hooks::list_hooks(hooks_config))
            .await
            .map_err(|err| format!("hook discovery task failed: {err}"))?;
    if !hooks.warnings.is_empty() {
        warn!(
            warnings = ?hooks.warnings,
            "hook discovery reported warnings while trusting materialized plugins"
        );
    }
    let edits = hooks
        .hooks
        .into_iter()
        .filter(|hook| {
            hook.plugin_id
                .as_ref()
                .is_some_and(|plugin_id| plugin_ids.contains(plugin_id))
        })
        .map(|hook| hook_trusted_hash_edit(&hook.key, &hook.current_hash))
        .collect::<Vec<_>>();
    if edits.is_empty() {
        return Ok(());
    }
    if auth_manager
        .auth_cached()
        .and_then(|auth| auth.get_account_id())
        .as_deref()
        != Some(current_account_id.as_str())
    {
        warn!("skipping materialized plugin hook trust after account changed");
        return Ok(());
    }

    let params = ConfigBatchWriteParams {
        edits,
        file_path: None,
        expected_version: None,
        reload_user_config: true,
    };
    config_processor
        .batch_write(params)
        .await
        .map_err(|err| format!("failed to write hook trust: {}", err.message))?;
    Ok(())
}

#[cfg(test)]
#[path = "effective_plugin_change_tests.rs"]
mod tests;
