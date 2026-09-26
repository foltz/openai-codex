//! In-process custody for finite account-dependent operations, not idle services.

/// A counted operation owns this value until its effects finish. Unlike turn
/// work, it is never released by a session-loop or logical-turn terminal event.
/// Hosts must derive independent custody before spawning descendants, and must
/// not put a live operation in a dormant connection or reusable factory.
pub trait HostOperationWork: std::fmt::Debug + Send + Sync {
    /// Derive from this still-live operation, including during a drain. Failure
    /// must not fall back to fresh or ungated execution.
    fn derive_operation(&self) -> Result<Box<dyn HostOperationWork>, crate::TurnWorkRefused>;

    /// Give a child turn its own custody. Only the child work uses the child's
    /// loop/terminal evidence; the originating operation keeps its own lifetime.
    fn derive_turn_work(
        &self,
        child_store: &crate::ExtensionData,
        termination: crate::ExtensionFuture<'static, ()>,
    ) -> Result<Box<dyn codex_protocol::host_turn_work::HostTurnWork>, crate::TurnWorkRefused>;
}
