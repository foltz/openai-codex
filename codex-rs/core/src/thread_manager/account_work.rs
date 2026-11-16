//! Reusable account-transition observations, not permanent manager retirement.

use super::ThreadManager;
use std::time::Duration;
use tokio::time::Instant;

impl ThreadManager {
    /// Capture the current request's authority before spawning a finite job.
    /// Absence remains ungated; exhausted derivation never acquires fresh work.
    pub fn derive_request_operation_work(
        &self,
    ) -> codex_protocol::error::Result<Option<Box<dyn codex_extension_api::HostOperationWork>>>
    {
        match self.state.extensions.host_admission() {
            Some(host) => host.derive_request_operation_work().map_err(|_| {
                codex_protocol::error::CodexErr::Fatal(
                    "request account work cannot admit background operation".to_owned(),
                )
            }),
            None => Ok(None),
        }
    }

    /// Admit a new finite background producer, never infer authority from an ambient request.
    /// A Required host returning no work refuses before any account effect.
    pub fn admit_background_operation_work(
        &self,
    ) -> codex_protocol::error::Result<Option<Box<dyn codex_extension_api::HostOperationWork>>>
    {
        let Some(host) = self.state.extensions.host_admission() else {
            return Ok(None);
        };
        let work = host.admit_operation_work().map_err(|_| {
            codex_protocol::error::CodexErr::Fatal(
                "background account work is unavailable".to_owned(),
            )
        })?;
        if work.is_none() && host.requires_account_work() {
            return Err(codex_protocol::error::CodexErr::Fatal(
                "background account work is required".to_owned(),
            ));
        }
        Ok(work)
    }

    /// Drive already-admitted constructors during account draining, without
    /// closing threads or initiating retirement. Cancelling this observer does
    /// not discard the retained constructors or their operation custody.
    pub async fn drive_admitted_constructions(&self) {
        self.constructions.drive_account_work().await;
    }

    /// Retire startup resources that never reached the thread lookup map.
    /// Pending constructors or incomplete cleanup refuse reset completion;
    /// this never starts a constructor or closes this reusable manager.
    pub async fn shutdown_unpublished_constructions_bounded(&self, timeout: Duration) -> bool {
        self.constructions
            .retire_completed_startup_until(Instant::now() + timeout)
            .await
    }
}

/// A request-scope observation, including an explicit absence. Private to the
/// manager: a spawned task must not repeat an ambient lookup for captured None.
pub(crate) struct CapturedConstructionWork(
    pub(crate) Option<Box<dyn codex_extension_api::HostOperationWork>>,
);

impl CapturedConstructionWork {
    pub(crate) fn derive(&self) -> Result<Self, codex_extension_api::TurnWorkRefused> {
        self.0
            .as_deref()
            .map(|work| work.derive_operation())
            .transpose()
            .map(Self)
    }
}

impl super::ThreadManagerState {
    pub(super) fn capture_construction_work(
        &self,
        options: &mut super::StartThreadOptions,
    ) -> codex_protocol::error::Result<CapturedConstructionWork> {
        let work = match options.account_work.take() {
            Some(work) => Some(work),
            None => match self.extensions.host_admission() {
                Some(host) => host.derive_request_operation_work().map_err(|_| {
                    codex_protocol::error::CodexErr::Fatal(
                        "request account work cannot admit thread construction".to_owned(),
                    )
                })?,
                None => None,
            },
        };
        self.require_construction_work(CapturedConstructionWork(work))
    }

    /// Recheck policy without rediscovering request scope. A captured absence
    /// cannot authorize a host that requires counted construction work.
    pub(super) fn require_construction_work(
        &self,
        captured: CapturedConstructionWork,
    ) -> codex_protocol::error::Result<CapturedConstructionWork> {
        if captured.0.is_none()
            && self
                .extensions
                .host_admission()
                .is_some_and(|host| host.requires_account_work())
        {
            return Err(codex_protocol::error::CodexErr::Fatal(
                "thread construction requires account work".to_owned(),
            ));
        }
        Ok(captured)
    }
}
