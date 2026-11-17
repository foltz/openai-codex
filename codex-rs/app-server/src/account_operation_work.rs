//! Finite account effects own raw permits, independently of session retirement.

use crate::account_turn_work::AccountTurnWork;
use crate::account_work_permits::AccountWorkPermitGuard;
use codex_extension_api::ExtensionData;
use codex_extension_api::ExtensionFuture;
use codex_extension_api::HostOperationWork;
use codex_extension_api::TurnWorkRefused;
use codex_protocol::host_turn_work::HostTurnWork;

pub(crate) struct AccountOperationWork {
    pub(crate) permit: AccountWorkPermitGuard,
    pub(crate) turns: AccountTurnWork,
    pub(crate) auth_tasks: Option<crate::processor_task_retirement::ProcessorTaskTicket>,
}

impl codex_mcp::McpAttemptWork for AccountWorkPermitGuard {
    fn derive_attempt(
        &self,
    ) -> Result<Box<dyn codex_mcp::McpAttemptWork>, codex_mcp::McpAttemptRefused> {
        self.try_derive()
            .map(|work| Box::new(work) as Box<dyn codex_mcp::McpAttemptWork>)
            .ok_or(codex_mcp::McpAttemptRefused)
    }
}

impl codex_mcp::McpAttemptWork for AccountOperationWork {
    fn auth_task_spawner(
        &self,
    ) -> Result<
        std::sync::Arc<dyn codex_protocol::host_auth_work::AuthTaskSpawner>,
        codex_mcp::McpAttemptRefused,
    > {
        HostOperationWork::auth_task_spawner(self).map_err(|_| codex_mcp::McpAttemptRefused)
    }

    fn derive_attempt(
        &self,
    ) -> Result<Box<dyn codex_mcp::McpAttemptWork>, codex_mcp::McpAttemptRefused> {
        Ok(Box::new(Self {
            permit: self
                .permit
                .try_derive()
                .ok_or(codex_mcp::McpAttemptRefused)?,
            turns: self.turns.clone(),
            auth_tasks: self.auth_tasks.clone(),
        }))
    }
}

impl std::fmt::Debug for AccountOperationWork {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AccountOperationWork")
    }
}

impl HostOperationWork for AccountOperationWork {
    fn auth_task_spawner(
        &self,
    ) -> Result<std::sync::Arc<dyn codex_protocol::host_auth_work::AuthTaskSpawner>, TurnWorkRefused>
    {
        let ticket = self
            .auth_tasks
            .clone()
            .ok_or(TurnWorkRefused::Unavailable)?;
        let work = self.derive_operation()?;
        Ok(crate::auth_task_work::spawner(ticket, Some(work)))
    }

    fn derive_operation(&self) -> Result<Box<dyn HostOperationWork>, TurnWorkRefused> {
        let permit = self
            .permit
            .try_derive()
            .ok_or(TurnWorkRefused::Unavailable)?;
        Ok(Box::new(Self {
            permit,
            turns: self.turns.clone(),
            auth_tasks: self.auth_tasks.clone(),
        }))
    }

    fn derive_turn_work(
        &self,
        child_store: &ExtensionData,
        termination: ExtensionFuture<'static, ()>,
    ) -> Result<Box<dyn HostTurnWork>, TurnWorkRefused> {
        let permit = self
            .permit
            .try_derive()
            .ok_or(TurnWorkRefused::Unavailable)?;
        let child = child_store.get_or_init(|| self.turns.session(termination));
        let pending = child.begin(permit).ok_or(TurnWorkRefused::Unavailable)?;
        Ok(Box::new(pending))
    }
}
