//! In-process custody for finite account-dependent operations, not idle services.

/// A counted operation owns this value until its effects finish. Unlike turn
/// work, it is never released by a session-loop or logical-turn terminal event.
/// Hosts must derive independent custody before spawning descendants, and must
/// not put a live operation in a dormant connection or reusable factory.
pub trait HostOperationWork: std::fmt::Debug + Send + Sync {
    /// Obtain original auth-task custody from this live operation. An idle
    /// factory or unsupported host supplies no authority and must refuse.
    fn auth_task_spawner(
        &self,
    ) -> Result<
        std::sync::Arc<dyn codex_protocol::host_auth_work::AuthTaskSpawner>,
        crate::TurnWorkRefused,
    > {
        Err(crate::TurnWorkRefused::Unavailable)
    }

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
