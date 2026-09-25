//! Account admission in the caller, shutdown admission in Core, and logical
//! terminal evidence independent of any app-server notification listener.

use crate::account_turn_work::AccountTurnSession;
use crate::account_turn_work::AccountTurnWork;
use crate::managed_transition::AccountWorkPermitGuard;
use crate::managed_transition::AccountWorkPermits;
use crate::turn_admission::TurnAdmission;
use codex_extension_api::ExtensionData;
use codex_extension_api::ExtensionFuture;
use codex_extension_api::TurnAbortInput;
use codex_extension_api::TurnLifecycleContributor;
use codex_extension_api::TurnStartAdmission;
use codex_extension_api::TurnStartInput;
use codex_extension_api::TurnStopInput;
use codex_extension_api::TurnWorkRefused;
use codex_protocol::host_turn_work::HostTurnWork;
use std::future::Future;
use std::sync::Arc;

tokio::task_local! {
    static REQUEST_WORK: Arc<AccountWorkPermitGuard>;
}

/// Scope the actual executed request, not the task that queues or spawns it.
/// Each inline start derives independent custody from this live guard. Nothing
/// assumes this scope propagates through a spawned task or a session IO hop.
pub(crate) async fn within_request<T>(
    permit: Option<AccountWorkPermitGuard>,
    request: impl Future<Output = T>,
) -> T {
    match permit {
        Some(permit) => REQUEST_WORK.scope(Arc::new(permit), request).await,
        None => request.await,
    }
}

pub(crate) struct AccountTurnAdmission {
    pub(crate) shutdown: TurnAdmission,
    pub(crate) permits: AccountWorkPermits,
    pub(crate) work: AccountTurnWork,
}

impl std::fmt::Debug for AccountTurnAdmission {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AccountTurnAdmission")
    }
}

impl TurnStartAdmission for AccountTurnAdmission {
    fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
        self.shutdown.admit_turn_start()
    }

    fn admit_turn_work(
        &self,
        thread_store: &ExtensionData,
        termination: ExtensionFuture<'static, ()>,
    ) -> Result<Option<Box<dyn HostTurnWork>>, TurnWorkRefused> {
        let permit = match REQUEST_WORK.try_with(|parent| parent.try_derive()) {
            Ok(derived) => derived,
            Err(_) => self.permits.try_acquire(),
        }.ok_or(TurnWorkRefused)?;
        let session = thread_store.get_or_init(|| self.work.session(termination));
        let pending = session.begin(permit).ok_or(TurnWorkRefused)?;
        Ok(Some(Box::new(pending)))
    }

    fn derive_turn_work(
        &self,
        parent_store: &ExtensionData,
        parent_turn_id: &str,
        child_store: &ExtensionData,
        termination: ExtensionFuture<'static, ()>,
    ) -> Result<Option<Box<dyn HostTurnWork>>, TurnWorkRefused> {
        let parent = parent_store.get::<AccountTurnSession>().ok_or(TurnWorkRefused)?;
        let permit = parent.derive(parent_turn_id).ok_or(TurnWorkRefused)?;
        let child = child_store.get_or_init(|| self.work.session(termination));
        let pending = child.begin(permit).ok_or(TurnWorkRefused)?;
        Ok(Some(Box::new(pending)))
    }

    fn turn_work_terminal(&self, thread_store: &ExtensionData, turn_id: &str) {
        if let Some(session) = thread_store.get::<AccountTurnSession>() {
            session.terminal(turn_id);
        }
    }
}

struct AccountTurnId(String);

pub(crate) struct AccountTurnLifecycle;

impl TurnLifecycleContributor for AccountTurnLifecycle {
    fn on_turn_start<'a>(&'a self, input: TurnStartInput<'a>) -> ExtensionFuture<'a, ()> {
        input.turn_store.insert(AccountTurnId(input.turn_id.to_owned()));
        Box::pin(std::future::ready(()))
    }

    fn on_turn_stop<'a>(&'a self, input: TurnStopInput<'a>) -> ExtensionFuture<'a, ()> {
        if let Some(session) = input.thread_store.get::<AccountTurnSession>()
            && let Some(turn) = input.turn_store.get::<AccountTurnId>()
        {
            session.terminal(&turn.0);
        }
        Box::pin(std::future::ready(()))
    }

    fn on_turn_abort<'a>(&'a self, input: TurnAbortInput<'a>) -> ExtensionFuture<'a, ()> {
        if let Some(session) = input.thread_store.get::<AccountTurnSession>()
            && let Some(turn) = input.turn_store.get::<AccountTurnId>()
        {
            session.terminal(&turn.0);
        }
        Box::pin(std::future::ready(()))
    }
}
