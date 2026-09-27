//! Explicit finite account authority at the Core/MCP boundary.
//!
//! This adapter belongs to an invocation, never an idle runtime or binding.

use super::session::Session;
use super::turn_context::TurnContext;
use codex_extension_api::HostOperationWork;
use codex_mcp::McpAttemptRefused;
use codex_mcp::McpAttemptWork;

pub(crate) struct McpOperationWork(pub(crate) Box<dyn HostOperationWork>);

impl McpAttemptWork for McpOperationWork {
    fn derive_attempt(&self) -> Result<Box<dyn McpAttemptWork>, McpAttemptRefused> {
        self.0
            .derive_operation()
            .map(|work| Box::new(Self(work)) as Box<dyn McpAttemptWork>)
            .map_err(|_| McpAttemptRefused)
    }
}

impl Session {
    /// Capture the exact live turn's authority, including from spawned tool
    /// tasks. A missing turn entry refuses; it never becomes fresh admission.
    pub(crate) fn turn_mcp_work(
        &self,
        turn: &TurnContext,
    ) -> anyhow::Result<Option<Box<dyn McpAttemptWork>>> {
        let Some(host) = &self.services.host_admission else {
            return Ok(None);
        };
        host.derive_operation_work(&self.services.thread_extension_data, &turn.sub_id)
            .map(|work| work.map(|work| Box::new(McpOperationWork(work)) as Box<dyn McpAttemptWork>))
            .map_err(|_| anyhow::anyhow!("MCP turn account work is unavailable"))
    }

    /// Capture before the first await in a host-request entry point. Absence
    /// remains ungated; exhausted derivation refuses without fresh admission.
    pub(crate) fn request_mcp_work(&self) -> anyhow::Result<Option<Box<dyn McpAttemptWork>>> {
        let Some(host) = &self.services.host_admission else {
            return Ok(None);
        };
        host.derive_request_operation_work()
            .map(|work| work.map(|work| Box::new(McpOperationWork(work)) as Box<dyn McpAttemptWork>))
            .map_err(|_| anyhow::anyhow!("MCP request account work is unavailable"))
    }
}
