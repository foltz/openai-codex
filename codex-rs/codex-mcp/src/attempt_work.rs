//! Finite host-account custody for MCP connection attempts.

pub use codex_protocol::mcp_work::McpAttemptAccess;
pub use codex_protocol::mcp_work::McpAttemptRefused;
pub use codex_protocol::mcp_work::McpAttemptWork;

/// Whether this connection belongs to a host with account-work admission.
/// This flag contains no counted authority and may live on an idle client.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum McpAttemptRequirement {
    Ungated,
    Required,
}

impl McpAttemptRequirement {
    pub(crate) fn derive(
        self,
        access: McpAttemptAccess<'_>,
    ) -> Result<Option<Box<dyn McpAttemptWork>>, McpAttemptRefused> {
        match (self, access) {
            (_, McpAttemptAccess::Admitted(work)) => work.derive_attempt().map(Some),
            (Self::Ungated, McpAttemptAccess::Unscoped) => Ok(None),
            (Self::Required, McpAttemptAccess::Unscoped) => Err(McpAttemptRefused),
        }
    }
}
