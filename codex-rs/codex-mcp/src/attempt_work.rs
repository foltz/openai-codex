//! Finite host-account custody for MCP connection attempts.

/// A live account-work lease supplied by the caller, never an idle connection
/// or a reusable startup factory. Implementations derive independently counted
/// work without waiting, acquiring fresh authority, or re-entering MCP.
pub trait McpAttemptWork: Send + Sync {
    /// Derive one descendant attempt from this still-live operation. Refusal
    /// must not fall back to uncounted or newly admitted work.
    fn derive_attempt(&self) -> Result<Box<dyn McpAttemptWork>, McpAttemptRefused>;
}

/// Whether this connection belongs to a host with account-work admission.
/// This flag contains no counted authority and may live on an idle client.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum McpAttemptRequirement {
    Ungated,
    Required,
}

/// Authority for this invocation, not for the lifetime of the MCP runtime.
#[derive(Clone, Copy)]
pub enum McpAttemptAccess<'a> {
    /// May observe admitted work, but cannot start required account work.
    Unscoped,
    Admitted(&'a dyn McpAttemptWork),
}

impl<'a> McpAttemptAccess<'a> {
    pub fn from_work(work: Option<&'a dyn McpAttemptWork>) -> Self {
        match work {
            Some(work) => Self::Admitted(work),
            None => Self::Unscoped,
        }
    }
}

/// No new attempt was admitted. The caller may retry with valid authority;
/// this is not a memoized startup failure or a reason to discard cached tools.
#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("MCP startup account work is unavailable")]
pub struct McpAttemptRefused;

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
