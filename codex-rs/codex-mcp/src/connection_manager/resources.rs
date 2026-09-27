use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use rmcp::model::ListResourceTemplatesResult;
use rmcp::model::ListResourcesResult;
use rmcp::model::PaginatedRequestParams;
use rmcp::model::ReadResourceRequestParams;
use rmcp::model::ReadResourceResult;
use rmcp::model::Resource;
use rmcp::model::ResourceTemplate;
use tokio::task::JoinSet;
use tracing::warn;

use super::McpConnectionSet;
use crate::pagination::collect_paginated;
use crate::resource_client::McpResourceServerCacheKey;
use crate::rmcp_client::ManagedClient;

impl McpConnectionSet {
    pub(crate) fn resource_cache_key(
        &self,
        server: &str,
        generation: u64,
    ) -> Option<McpResourceServerCacheKey> {
        self.servers
            .get(server)
            .map(|view| McpResourceServerCacheKey {
                connection: Arc::downgrade(&view.connection),
                generation,
            })
    }

    /// Returns resources from servers selected by `include_server`.
    pub async fn list_all_resources(
        &self,
        include_server: impl Fn(&str) -> bool,
    ) -> HashMap<String, Vec<Resource>> {
        self.list_all_resources_with_authority(include_server, crate::McpAttemptAccess::Unscoped).await
    }

    pub(crate) async fn list_all_resources_with_authority(
        &self,
        include_server: impl Fn(&str) -> bool,
        access: crate::McpAttemptAccess<'_>,
    ) -> HashMap<String, Vec<Resource>> {
        let mut join_set = JoinSet::new();
        for (server_name, view) in self
            .servers
            .iter()
            .filter(|(server_name, _)| include_server(server_name))
        {
            let server_name = server_name.clone();
            let Ok(managed_client) = view.connection.client_with_authority(access).await else {
                continue;
            };
            let timeout = view.tool_timeout;
            let client = managed_client.client;
            let Ok(work) = view.connection.client.client.requirement.derive(access) else {
                warn!("MCP resource listing account work is unavailable for '{server_name}'");
                continue;
            };
            join_set.spawn(async move {
                // JoinSet drop requests abort; the task itself retains work
                // until that abort has actually destroyed its future.
                let _work = work;
                let resources = collect_paginated("resources/list", timeout, |params| {
                    let client = Arc::clone(&client);
                    async move {
                        let response = client.list_resources(params, timeout).await?;
                        Ok((response.resources, response.next_cursor))
                    }
                })
                .await;
                (server_name, resources)
            });
        }

        let mut resources = HashMap::new();
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok((server_name, Ok(server_resources))) => {
                    resources.insert(server_name, server_resources);
                }
                Ok((server_name, Err(error))) => {
                    warn!("Failed to list resources for MCP server '{server_name}': {error:#}");
                }
                Err(error) => {
                    warn!("Task panic when listing resources for MCP server: {error:#}");
                }
            }
        }
        resources
    }

    /// Returns resource templates from servers selected by `include_server`.
    pub async fn list_all_resource_templates(
        &self,
        include_server: impl Fn(&str) -> bool,
    ) -> HashMap<String, Vec<ResourceTemplate>> {
        self.list_all_resource_templates_with_authority(include_server, crate::McpAttemptAccess::Unscoped).await
    }

    pub(crate) async fn list_all_resource_templates_with_authority(
        &self,
        include_server: impl Fn(&str) -> bool,
        access: crate::McpAttemptAccess<'_>,
    ) -> HashMap<String, Vec<ResourceTemplate>> {
        let mut join_set = JoinSet::new();
        for (server_name, view) in self
            .servers
            .iter()
            .filter(|(server_name, _)| include_server(server_name))
        {
            let server_name = server_name.clone();
            let Ok(managed_client) = view.connection.client_with_authority(access).await else {
                continue;
            };
            let timeout = view.tool_timeout;
            let client = managed_client.client;
            let Ok(work) = view.connection.client.client.requirement.derive(access) else {
                warn!("MCP resource template listing account work is unavailable for '{server_name}'");
                continue;
            };
            join_set.spawn(async move {
                let _work = work;
                let templates = collect_paginated("resources/templates/list", timeout, |params| {
                    let client = Arc::clone(&client);
                    async move {
                        let response = client.list_resource_templates(params, timeout).await?;
                        Ok((response.resource_templates, response.next_cursor))
                    }
                })
                .await;
                (server_name, templates)
            });
        }

        let mut templates = HashMap::new();
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok((server_name, Ok(server_templates))) => {
                    templates.insert(server_name, server_templates);
                }
                Ok((server_name, Err(error))) => {
                    warn!(
                        "Failed to list resource templates for MCP server '{server_name}': {error:#}"
                    );
                }
                Err(error) => {
                    warn!("Task panic when listing resource templates for MCP server: {error:#}");
                }
            }
        }
        templates
    }

    pub async fn list_resources(
        &self,
        server: &str,
        params: Option<PaginatedRequestParams>,
    ) -> Result<ListResourcesResult> {
        self.list_resources_with_authority(server, params, crate::McpAttemptAccess::Unscoped).await
    }

    pub(crate) async fn list_resources_with_authority(
        &self,
        server: &str,
        params: Option<PaginatedRequestParams>,
        access: crate::McpAttemptAccess<'_>,
    ) -> Result<ListResourcesResult> {
        let (managed, timeout) = self.client_by_name_with_authority(server, access).await?;
        managed
            .client
            .list_resources(params, timeout)
            .await
            .with_context(|| format!("resources/list failed for `{server}`"))
    }

    pub async fn list_resource_templates(
        &self,
        server: &str,
        params: Option<PaginatedRequestParams>,
    ) -> Result<ListResourceTemplatesResult> {
        self.list_resource_templates_with_authority(server, params, crate::McpAttemptAccess::Unscoped).await
    }

    pub(crate) async fn list_resource_templates_with_authority(
        &self,
        server: &str,
        params: Option<PaginatedRequestParams>,
        access: crate::McpAttemptAccess<'_>,
    ) -> Result<ListResourceTemplatesResult> {
        let (managed, timeout) = self.client_by_name_with_authority(server, access).await?;
        managed
            .client
            .list_resource_templates(params, timeout)
            .await
            .with_context(|| format!("resources/templates/list failed for `{server}`"))
    }

    pub async fn read_resource(
        &self,
        server: &str,
        params: ReadResourceRequestParams,
    ) -> Result<ReadResourceResult> {
        self.read_resource_with_authority(server, params, crate::McpAttemptAccess::Unscoped).await
    }

    pub(crate) async fn read_resource_with_authority(
        &self,
        server: &str,
        params: ReadResourceRequestParams,
        access: crate::McpAttemptAccess<'_>,
    ) -> Result<ReadResourceResult> {
        let (managed, timeout) = self.client_by_name_with_authority(server, access).await?;
        let uri = params.uri.clone();
        managed
            .client
            .read_resource(params, timeout)
            .await
            .with_context(|| format!("resources/read failed for `{server}` ({uri})"))
    }

    pub(crate) async fn client_by_name(
        &self,
        name: &str,
    ) -> Result<(ManagedClient, Option<Duration>)> {
        self.client_by_name_with_authority(name, crate::McpAttemptAccess::Unscoped).await
    }

    pub(crate) async fn client_by_name_with_authority(
        &self,
        name: &str,
        access: crate::McpAttemptAccess<'_>,
    ) -> Result<(ManagedClient, Option<Duration>)> {
        let view = self
            .servers
            .get(name)
            .ok_or_else(|| anyhow!("unknown MCP server '{name}'"))?;
        let client = view
            .connection
            .client_with_authority(access)
            .await
            .map_err(|error| match error {
                crate::rmcp_client::StartupOutcomeError::Refused(refused) => anyhow::Error::new(refused),
                error => anyhow::Error::new(error),
            })
            .context("failed to get client")?;
        Ok((client, view.tool_timeout))
    }
}
