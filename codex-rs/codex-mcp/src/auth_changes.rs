//! Forwards auth invalidations without credentials. The managed client owns the watcher.

use std::sync::Arc;
use std::time::Duration;

use crate::worker_joins::McpAbortOnDrop;
use anyhow::Result;
use codex_login::AuthChangeState;
use codex_rmcp_client::RmcpClient;
use rmcp::model::ServerCapabilities;
use serde_json::json;
use tokio::sync::watch;

pub(crate) const CAPABILITY: &str = "codex/auth-change";
const NOTIFICATION: &str = "notifications/codex/authChanged";
const SEND_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) async fn start(
    workers: &crate::McpWorkerOwner,
    client: Arc<RmcpClient>,
    capabilities: &ServerCapabilities,
    changes: Option<watch::Receiver<AuthChangeState>>,
) -> Result<Option<Arc<McpAbortOnDrop>>> {
    let Some(mut changes) = changes.filter(|_| {
        capabilities
            .experimental
            .as_ref()
            .is_some_and(|capabilities| capabilities.contains_key(CAPABILITY))
    }) else {
        return Ok(None);
    };

    let (send_initial, initial) = tokio::sync::oneshot::channel();
    let task = workers
        .spawn_abort_on_drop(
            &tokio::runtime::Handle::current(),
            crate::McpWorkerKind::AuthNotification,
            async move {
                let result = notify(&client, &mut changes).await;
                let succeeded = result.is_ok();
                if send_initial.send(result).is_err() || !succeeded {
                    return;
                }
                while changes.changed().await.is_ok() {
                    if notify(&client, &mut changes).await.is_err() {
                        tracing::warn!("MCP auth invalidation delivery failed; closing connection");
                        client.shutdown().await;
                        break;
                    }
                }
            },
        )
        .map_err(|()| anyhow::anyhow!("MCP worker startup admission closed"))?;
    initial
        .await
        .map_err(|_| anyhow::anyhow!("MCP auth initial notification worker stopped"))??;
    Ok(Some(Arc::new(task)))
}

async fn notify(client: &RmcpClient, changes: &mut watch::Receiver<AuthChangeState>) -> Result<()> {
    let state = *changes.borrow_and_update();
    tokio::time::timeout(
        SEND_TIMEOUT,
        client.send_custom_notification(
            NOTIFICATION,
            Some(json!({
                "generation": state.generation,
                "ownerGeneration": state.owner_generation,
            })),
        ),
    )
    .await?
}

#[cfg(test)]
#[path = "auth_changes_tests.rs"]
mod tests;
