//! Non-serialized, invocation-scoped host custody for MCP work.
//!
//! These carriers live below tools and MCP so extension invocations can borrow
//! authority without depending on the transport implementation. Admission
//! policy belongs to the host and MCP, not to this module.

/// A live account-work lease supplied by the caller, never an idle connection
/// or a reusable startup factory. Implementations derive independently counted
/// work without waiting, acquiring fresh authority, or re-entering MCP.
pub trait McpAttemptWork: Send + Sync {
    /// Derive one descendant attempt from this still-live operation. Refusal
    /// must not fall back to uncounted or newly admitted work.
    fn derive_attempt(&self) -> Result<Box<dyn McpAttemptWork>, McpAttemptRefused>;
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

impl std::fmt::Debug for McpAttemptAccess<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Unscoped => "Unscoped",
            Self::Admitted(_) => "Admitted(<host account work>)",
        })
    }
}

/// No new attempt was admitted. The caller may retry with valid authority;
/// this is not a memoized startup failure or a reason to discard cached tools.
#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("MCP startup account work is unavailable")]
pub struct McpAttemptRefused;
