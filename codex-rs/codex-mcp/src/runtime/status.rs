//! Read selected-server metadata from one published thread runtime.

use std::sync::Arc;

use super::McpRuntime;
use super::McpRuntimeContext;
use crate::McpConfig;
use crate::McpServerStatusSnapshot;
use crate::McpSnapshotDetail;
use crate::ResolvedMcpServer;
use crate::mcp::collect_mcp_server_status_snapshot_from_manager;
use crate::mcp::compute_auth_statuses;
use crate::mcp::effective_mcp_servers;

impl McpRuntime {
    /// Reuses the selected connection and catalog without starting unrelated servers.
    /// Configuration and metadata belong to the same captured runtime publication.
    /// An expected registration excludes mismatched connections before discovery.
    pub async fn server_status_snapshot(
        &self,
        server: &str,
        detail: McpSnapshotDetail,
        expected_registration: Option<&ResolvedMcpServer>,
        runtime_context: &McpRuntimeContext,
    ) -> anyhow::Result<(Arc<McpConfig>, McpServerStatusSnapshot)> {
        self.server_status_snapshot_with_authority(
            server,
            detail,
            expected_registration,
            runtime_context,
            crate::McpAttemptAccess::Unscoped,
        )
        .await
    }

    /// Derive finite invocation work before auth probing and selected discovery.
    /// Returned configuration/status snapshots retain no counted work.
    pub async fn server_status_snapshot_with_authority(
        &self,
        server: &str,
        detail: McpSnapshotDetail,
        expected_registration: Option<&ResolvedMcpServer>,
        runtime_context: &McpRuntimeContext,
        access: crate::McpAttemptAccess<'_>,
    ) -> anyhow::Result<(Arc<McpConfig>, McpServerStatusSnapshot)> {
        let current = self.current.load_full();
        let work = current.connections.attempt_requirement.derive(access)?;
        let access = crate::McpAttemptAccess::from_work(work.as_deref());
        let runtime_context = runtime_context.clone().with_selected_environments(
            Arc::clone(&current.environment_selections),
            current.ready_environments.clone(),
        );
        let config = Arc::clone(
            current
                .config
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("MCP runtime is not configured"))?,
        );
        let mut servers = effective_mcp_servers(&config, current.auth.as_ref());
        servers.retain(|name, _| {
            name == server
                && expected_registration
                    .is_none_or(|expected| config.mcp_server_catalog.server(name) == Some(expected))
        });
        if config.xaa_enabled
            && servers
                .values()
                .any(|server| matches!(server.config().auth, codex_config::McpServerAuth::EmaAuth))
        {
            // Finish selected startup and any rejected-grant cleanup before reading auth.
            current
                .connections
                .list_available_server_infos_with_authority(|name| name == server, access)
                .await;
        }
        let auth_statuses = compute_auth_statuses(
            servers.iter(),
            config.mcp_oauth_credentials_store_mode,
            config.auth_keyring_backend_kind,
            current.auth.as_ref(),
            current.auth_manager.as_ref(),
            &config.ema_state,
            &runtime_context,
        )
        .await;
        let snapshot = collect_mcp_server_status_snapshot_from_manager(
            &current.connections,
            auth_statuses,
            servers.into_keys().collect(),
            detail,
            access,
        )
        .await?;
        Ok((config, snapshot))
    }
}
