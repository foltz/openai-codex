//! Turn lifecycle inputs and scheduling phases for host-owned contributors.

use codex_protocol::config_types::CollaborationMode;
use codex_protocol::protocol::CodexErrorInfo;
use codex_protocol::protocol::TokenUsage;
use codex_protocol::protocol::TurnAbortReason;

use crate::ExtensionData;

/// Runs before task registration or during cancellable regular-task startup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TurnStartPhase {
    BeforeTaskRegistration,
    RegularTaskStart,
}

/// Input supplied at the host's irrevocable turn commit boundary.
pub struct TurnCommittedInput<'a> {
    /// Exact identifier of the accepted turn.
    pub turn_id: &'a str,
    /// Store scoped to the host session runtime.
    pub session_store: &'a ExtensionData,
    /// Store scoped to this thread runtime.
    pub thread_store: &'a ExtensionData,
    /// The same turn store supplied during preparation and terminal callbacks.
    pub turn_store: &'a ExtensionData,
}

/// Input supplied when the host starts a turn.
pub struct TurnStartInput<'a> {
    /// MCP attempt authority retained by this turn's host work.
    pub mcp_access: Result<codex_mcp::McpAttemptAccess<'a>, codex_mcp::McpAttemptRefused>,
    /// Stable host-owned turn identifier.
    pub turn_id: &'a str,
    /// Effective collaboration mode for this turn.
    pub collaboration_mode: &'a CollaborationMode,
    /// Total token usage snapshot captured when the turn started.
    /// Present before task registration; absent during regular-task preparation.
    /// Token-accounting contributors must remain in `BeforeTaskRegistration`.
    pub token_usage_at_turn_start: Option<&'a TokenUsage>,
    /// Store scoped to the host session runtime.
    pub session_store: &'a ExtensionData,
    /// Store scoped to this thread runtime.
    pub thread_store: &'a ExtensionData,
    /// Store scoped to this turn runtime.
    pub turn_store: &'a ExtensionData,
}

/// Input supplied when the host completes a turn.
pub struct TurnStopInput<'a> {
    /// Store scoped to the host session runtime.
    pub session_store: &'a ExtensionData,
    /// Store scoped to this thread runtime.
    pub thread_store: &'a ExtensionData,
    /// Store scoped to this turn runtime.
    pub turn_store: &'a ExtensionData,
}

/// Input supplied when the host aborts a turn.
pub struct TurnAbortInput<'a> {
    /// Reason the host aborted the turn.
    pub reason: TurnAbortReason,
    /// Store scoped to the host session runtime.
    pub session_store: &'a ExtensionData,
    /// Store scoped to this thread runtime.
    pub thread_store: &'a ExtensionData,
    /// Store scoped to this turn runtime.
    pub turn_store: &'a ExtensionData,
}

/// Input supplied when the host observes an error for a turn.
pub struct TurnErrorInput<'a> {
    /// Stable host-owned turn identifier.
    pub turn_id: &'a str,
    /// Error surfaced by the host for this turn.
    pub error: CodexErrorInfo,
    /// Store scoped to the host session runtime.
    pub session_store: &'a ExtensionData,
    /// Store scoped to this thread runtime.
    pub thread_store: &'a ExtensionData,
    /// Store scoped to this turn runtime.
    pub turn_store: &'a ExtensionData,
}
