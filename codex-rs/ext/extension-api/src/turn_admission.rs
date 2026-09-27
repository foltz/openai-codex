//! Lets Core turn-input submissions participate in a host's shutdown drain.

/// A host-provided gate checked before Core starts a turn-input submission.
///
/// Implementations return a permit for work admitted before shutdown and
/// Core retains it through submission. `None` skips the start without consuming
/// pending input. Steering an existing turn does not acquire a new permit.
/// Memory-only mailbox wakeups and parent-delegated subagent input bypass this
/// gate so delegated work can finish before exit. Automatic starts remain gated.
pub trait TurnStartAdmission: std::fmt::Debug + Send + Sync {
    fn admit_turn_start(&self) -> Option<Box<dyn Send>>;

    /// Whether this host requires explicit account custody for MCP startup.
    /// Shutdown-only admission hosts remain ungated. Account-gated hosts must
    /// implement operation admission/derivation and return work or refuse;
    /// missing request scope may return no work, but cannot authorize startup.
    fn requires_account_work(&self) -> bool {
        false
    }

    /// Acquire fresh admission for a finite background operation without a
    /// turn-registry entry or an ambient request-scope lookup. A caller must
    /// move the returned custody into the actual operation.
    /// Ungated hosts return `Ok(None)`; a refusal must precede account effects.
    fn admit_operation_work(
        &self,
    ) -> Result<Option<Box<dyn crate::HostOperationWork>>, TurnWorkRefused> {
        Ok(None)
    }

    /// Capture finite work from the currently executing host request before
    /// handing construction to another task. No request scope means `Ok(None)`;
    /// exhausted derivation must refuse, never acquire fresh authority.
    fn derive_request_operation_work(
        &self,
    ) -> Result<Option<Box<dyn crate::HostOperationWork>>, TurnWorkRefused> {
        Ok(None)
    }

    /// Derive finite work from an exact live turn, never from a claimed ID alone.
    fn derive_operation_work(
        &self,
        _parent_store: &crate::ExtensionData,
        _parent_turn_id: &str,
    ) -> Result<Option<Box<dyn crate::HostOperationWork>>, TurnWorkRefused> {
        Ok(None)
    }

    /// Acquire host account-work custody in the submitting task, before the
    /// IO hop. This is separate from the shutdown gate Core rechecks later.
    /// Ungated hosts return `Ok(None)`; refusal must precede submission.
    fn admit_turn_work(
        &self,
        _thread_store: &crate::ExtensionData,
        _termination: crate::ExtensionFuture<'static, ()>,
    ) -> Result<Option<Box<dyn codex_protocol::host_turn_work::HostTurnWork>>, TurnWorkRefused> {
        Ok(None)
    }

    /// Derive separately retained child work from an exact live parent turn.
    /// Isolated children use the parent's host while keeping their own registry
    /// empty. The host must refuse a missing parent rather than reopen admission.
    fn derive_turn_work(
        &self,
        _parent_store: &crate::ExtensionData,
        _parent_turn_id: &str,
        _child_store: &crate::ExtensionData,
        _termination: crate::ExtensionFuture<'static, ()>,
    ) -> Result<Option<Box<dyn codex_protocol::host_turn_work::HostTurnWork>>, TurnWorkRefused> {
        Ok(None)
    }

    /// Logical terminal evidence observed by an existing isolated-child event
    /// consumer. This must not be invoked merely because that consumer is dropped.
    fn turn_work_terminal(&self, _thread_store: &crate::ExtensionData, _turn_id: &str) {}
}

/// The host's account-work barrier refused a new submission.
pub enum TurnWorkRefused {
    /// No retry signal exists, for example because the parent turn has ended.
    Unavailable,
    /// A host-wide admission change may permit retry. This is only a wakeup:
    /// the caller must acquire fresh authority again after it resolves.
    RetryAfter(crate::ExtensionFuture<'static, ()>),
}

impl std::fmt::Debug for TurnWorkRefused {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Unavailable => "TurnWorkRefused::Unavailable",
            Self::RetryAfter(_) => "TurnWorkRefused::RetryAfter",
        })
    }
}
