//! Finite account effects own raw permits, independently of session retirement.

use crate::account_turn_work::AccountTurnWork;
use crate::managed_transition::AccountWorkPermitGuard;
use codex_extension_api::ExtensionData;
use codex_extension_api::ExtensionFuture;
use codex_extension_api::HostOperationWork;
use codex_extension_api::TurnWorkRefused;
use codex_protocol::host_turn_work::HostTurnWork;

pub(crate) struct AccountOperationWork {
    pub(crate) permit: AccountWorkPermitGuard,
    pub(crate) turns: AccountTurnWork,
}

impl codex_mcp::McpAttemptWork for AccountWorkPermitGuard {
    fn derive_attempt(&self) -> Result<Box<dyn codex_mcp::McpAttemptWork>, codex_mcp::McpAttemptRefused> {
        self.try_derive()
            .map(|work| Box::new(work) as Box<dyn codex_mcp::McpAttemptWork>)
            .ok_or(codex_mcp::McpAttemptRefused)
    }
}

impl std::fmt::Debug for AccountOperationWork {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AccountOperationWork")
    }
}

impl HostOperationWork for AccountOperationWork {
    fn derive_operation(&self) -> Result<Box<dyn HostOperationWork>, TurnWorkRefused> {
        let permit = self.permit.try_derive().ok_or(TurnWorkRefused::Unavailable)?;
        Ok(Box::new(Self { permit, turns: self.turns.clone() }))
    }

    fn derive_turn_work(
        &self,
        child_store: &ExtensionData,
        termination: ExtensionFuture<'static, ()>,
    ) -> Result<Box<dyn HostTurnWork>, TurnWorkRefused> {
        let permit = self.permit.try_derive().ok_or(TurnWorkRefused::Unavailable)?;
        let child = child_store.get_or_init(|| self.turns.session(termination));
        let pending = child.begin(permit).ok_or(TurnWorkRefused::Unavailable)?;
        Ok(Box::new(pending))
    }
}
