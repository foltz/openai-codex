//! Credential-free, process-local transition coordination.
//!
//! This kernel intentionally does not know how to read, write, or install
//! authentication. Slice 2 binds callers and targets before it is exposed via
//! RPC; later slices attach the barrier, durable adoption, and reset work.

use codex_app_server_protocol::CancelManagedTransitionParams;
use codex_app_server_protocol::CancelManagedTransitionResponse;
use codex_app_server_protocol::ManagedTransitionIntent;
use codex_app_server_protocol::ManagedTransitionPhase;
use codex_app_server_protocol::ManagedTransitionRefusal;
use codex_app_server_protocol::ManagedTransitionRefusalKind;
use codex_app_server_protocol::ManagedTransitionStatus;
use codex_app_server_protocol::ReadManagedTransitionParams;
use codex_app_server_protocol::ReadManagedTransitionResponse;
use codex_app_server_protocol::StartManagedTransitionParams;
use codex_app_server_protocol::StartManagedTransitionResponse;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct ManagedTransitionCoordinator {
    state: Arc<Mutex<CoordinatorState>>,
}

#[derive(Debug)]
struct CoordinatorState {
    process_instance_id: String,
    auth_revision: u64,
    transition_revision: u64,
    auth_fingerprint: Option<String>,
    active: Option<TransitionRecord>,
    completed: HashMap<String, TransitionRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TransitionEnvelope {
    transition_id: String,
    process_instance_id: String,
    intent: ManagedTransitionIntent,
    expected_auth_revision: u64,
    expected_transition_revision: u64,
    expected_auth_fingerprint: Option<String>,
}

#[derive(Debug, Clone)]
struct TransitionRecord {
    envelope: TransitionEnvelope,
    phase: ManagedTransitionPhase,
    retryable: bool,
    result_auth_fingerprint: Option<String>,
    refusal: Option<ManagedTransitionRefusalKind>,
}

impl ManagedTransitionCoordinator {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(CoordinatorState {
                process_instance_id: Uuid::now_v7().to_string(),
                auth_revision: 0,
                transition_revision: 0,
                auth_fingerprint: None,
                active: None,
                completed: HashMap::new(),
            })),
        }
    }

    pub(crate) async fn process_instance_id(&self) -> String {
        self.state.lock().await.process_instance_id.clone()
    }

    pub(crate) async fn admit(
        &self,
        params: StartManagedTransitionParams,
    ) -> Result<ManagedTransitionStatus, ManagedTransitionRefusal> {
        let envelope = TransitionEnvelope::from(params);
        let mut state = self.state.lock().await;

        if envelope.transition_id.is_empty() || envelope.process_instance_id.is_empty() {
            return Err(refusal(
                &state,
                &envelope,
                ManagedTransitionRefusalKind::InvalidRequest,
                false,
            ));
        }
        if envelope.process_instance_id != state.process_instance_id {
            return Err(refusal(
                &state,
                &envelope,
                ManagedTransitionRefusalKind::ProcessMismatch,
                true,
            ));
        }
        if let Some(record) = state.completed.get(&envelope.transition_id) {
            return Err(refusal(
                &state,
                &envelope,
                if record.envelope == envelope {
                    ManagedTransitionRefusalKind::CompletedReplay
                } else {
                    ManagedTransitionRefusalKind::TransitionIdConflict
                },
                false,
            ));
        }
        if let Some(record) = &state.active {
            return Err(refusal(
                &state,
                &envelope,
                if record.envelope.transition_id == envelope.transition_id {
                    ManagedTransitionRefusalKind::TransitionIdConflict
                } else {
                    ManagedTransitionRefusalKind::ConcurrentTransition
                },
                true,
            ));
        }
        if envelope.expected_auth_revision != state.auth_revision {
            return Err(refusal(
                &state,
                &envelope,
                ManagedTransitionRefusalKind::StaleAuthRevision,
                true,
            ));
        }
        if envelope.expected_transition_revision != state.transition_revision {
            return Err(refusal(
                &state,
                &envelope,
                ManagedTransitionRefusalKind::StaleTransitionRevision,
                true,
            ));
        }
        if envelope.expected_auth_fingerprint != state.auth_fingerprint {
            return Err(refusal(
                &state,
                &envelope,
                ManagedTransitionRefusalKind::StaleAuthFingerprint,
                true,
            ));
        }

        state.transition_revision += 1;
        let record = TransitionRecord {
            envelope,
            phase: ManagedTransitionPhase::Admitted,
            retryable: false,
            result_auth_fingerprint: None,
            refusal: None,
        };
        let status = status_for(&state, &record);
        state.active = Some(record);
        Ok(status)
    }

    /// Slice 1 exposes the versioned wire contract but deliberately does not
    /// admit a caller. Slice 2 replaces this gate with server-derived caller
    /// authorization before it can create or alter a transition record.
    pub(crate) async fn start_not_admitted_response(
        &self,
        params: StartManagedTransitionParams,
    ) -> StartManagedTransitionResponse {
        let state = self.state.lock().await;
        StartManagedTransitionResponse::Refused {
            refusal: authorization_not_admitted(&state, params.into()),
        }
    }

    pub(crate) async fn read(
        &self,
        params: ReadManagedTransitionParams,
    ) -> Result<ManagedTransitionStatus, ManagedTransitionRefusal> {
        let state = self.state.lock().await;
        let envelope = TransitionEnvelope::read(params);
        if envelope.process_instance_id != state.process_instance_id {
            return Err(refusal(
                &state,
                &envelope,
                ManagedTransitionRefusalKind::ProcessMismatch,
                true,
            ));
        }
        if let Some(record) = state
            .active
            .as_ref()
            .filter(|record| record.envelope.transition_id == envelope.transition_id)
        {
            return Ok(status_for(&state, record));
        }
        if let Some(record) = state.completed.get(&envelope.transition_id) {
            return Ok(status_for(&state, record));
        }
        Err(refusal(
            &state,
            &envelope,
            ManagedTransitionRefusalKind::InvalidRequest,
            true,
        ))
    }

    pub(crate) async fn read_not_admitted_response(
        &self,
        params: ReadManagedTransitionParams,
    ) -> ReadManagedTransitionResponse {
        let state = self.state.lock().await;
        ReadManagedTransitionResponse::Refused {
            refusal: authorization_not_admitted(&state, TransitionEnvelope::read(params)),
        }
    }

    pub(crate) async fn cancel(
        &self,
        params: CancelManagedTransitionParams,
    ) -> Result<ManagedTransitionStatus, ManagedTransitionRefusal> {
        let mut state = self.state.lock().await;
        let envelope = TransitionEnvelope::cancel(params);
        if envelope.process_instance_id != state.process_instance_id {
            return Err(refusal(
                &state,
                &envelope,
                ManagedTransitionRefusalKind::ProcessMismatch,
                true,
            ));
        }
        let Some(record) = state.active.take() else {
            return Err(refusal(
                &state,
                &envelope,
                ManagedTransitionRefusalKind::LateCancellation,
                false,
            ));
        };
        if record.envelope.transition_id != envelope.transition_id {
            state.active = Some(record);
            return Err(refusal(
                &state,
                &envelope,
                ManagedTransitionRefusalKind::TransitionIdConflict,
                true,
            ));
        }
        if record.phase != ManagedTransitionPhase::Admitted {
            state.active = Some(record);
            return Err(refusal(
                &state,
                &envelope,
                ManagedTransitionRefusalKind::LateCancellation,
                false,
            ));
        }

        state.transition_revision += 1;
        let cancelled = TransitionRecord {
            phase: ManagedTransitionPhase::Cancelled,
            retryable: true,
            ..record
        };
        let status = status_for(&state, &cancelled);
        state
            .completed
            .insert(cancelled.envelope.transition_id.clone(), cancelled);
        Ok(status)
    }

    pub(crate) async fn cancel_not_admitted_response(
        &self,
        params: CancelManagedTransitionParams,
    ) -> CancelManagedTransitionResponse {
        let state = self.state.lock().await;
        CancelManagedTransitionResponse::Refused {
            refusal: authorization_not_admitted(&state, TransitionEnvelope::cancel(params)),
        }
    }
}

impl From<StartManagedTransitionParams> for TransitionEnvelope {
    fn from(params: StartManagedTransitionParams) -> Self {
        Self {
            transition_id: params.transition_id,
            process_instance_id: params.process_instance_id,
            intent: params.intent,
            expected_auth_revision: params.expected_auth_revision,
            expected_transition_revision: params.expected_transition_revision,
            expected_auth_fingerprint: params.expected_auth_fingerprint,
        }
    }
}

impl TransitionEnvelope {
    fn read(params: ReadManagedTransitionParams) -> Self {
        Self {
            transition_id: params.transition_id,
            process_instance_id: params.process_instance_id,
            intent: ManagedTransitionIntent::AdoptManagedAuth,
            expected_auth_revision: 0,
            expected_transition_revision: 0,
            expected_auth_fingerprint: None,
        }
    }

    fn cancel(params: CancelManagedTransitionParams) -> Self {
        Self {
            transition_id: params.transition_id,
            process_instance_id: params.process_instance_id,
            intent: ManagedTransitionIntent::AdoptManagedAuth,
            expected_auth_revision: 0,
            expected_transition_revision: 0,
            expected_auth_fingerprint: None,
        }
    }
}

fn refusal(
    state: &CoordinatorState,
    envelope: &TransitionEnvelope,
    kind: ManagedTransitionRefusalKind,
    retryable: bool,
) -> ManagedTransitionRefusal {
    ManagedTransitionRefusal {
        kind,
        retryable,
        process_instance_id: state.process_instance_id.clone(),
        transition_id: envelope.transition_id.clone(),
        auth_revision: state.auth_revision,
        transition_revision: state.transition_revision,
        auth_fingerprint: state.auth_fingerprint.clone(),
    }
}

fn authorization_not_admitted(
    state: &CoordinatorState,
    envelope: TransitionEnvelope,
) -> ManagedTransitionRefusal {
    refusal(
        state,
        &envelope,
        ManagedTransitionRefusalKind::AuthorizationNotAdmitted,
        false,
    )
}

fn status_for(state: &CoordinatorState, record: &TransitionRecord) -> ManagedTransitionStatus {
    ManagedTransitionStatus {
        process_instance_id: state.process_instance_id.clone(),
        transition_id: Some(record.envelope.transition_id.clone()),
        intent: Some(record.envelope.intent),
        phase: record.phase,
        retryable: record.retryable,
        auth_revision: state.auth_revision,
        transition_revision: state.transition_revision,
        prior_auth_fingerprint: record.envelope.expected_auth_fingerprint.clone(),
        result_auth_fingerprint: record.result_auth_fingerprint.clone(),
        refusal: record.refusal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(process_instance_id: String, transition_id: &str) -> StartManagedTransitionParams {
        StartManagedTransitionParams {
            transition_id: transition_id.to_owned(),
            process_instance_id,
            intent: ManagedTransitionIntent::AdoptManagedAuth,
            expected_auth_revision: 0,
            expected_transition_revision: 0,
            expected_auth_fingerprint: None,
        }
    }

    #[tokio::test]
    async fn admits_one_credential_free_transition_and_cancels_it() {
        let coordinator = ManagedTransitionCoordinator::new();
        let process_id = coordinator.process_instance_id().await;
        let admitted = coordinator
            .admit(request(process_id.clone(), "transition-a"))
            .await
            .unwrap();
        assert_eq!(admitted.phase, ManagedTransitionPhase::Admitted);
        assert_eq!(admitted.transition_revision, 1);

        let observed = coordinator
            .read(ReadManagedTransitionParams {
                transition_id: "transition-a".to_owned(),
                process_instance_id: process_id.clone(),
            })
            .await
            .unwrap();
        assert_eq!(observed, admitted);

        let cancelled = coordinator
            .cancel(CancelManagedTransitionParams {
                transition_id: "transition-a".to_owned(),
                process_instance_id: process_id,
            })
            .await
            .unwrap();
        assert_eq!(cancelled.phase, ManagedTransitionPhase::Cancelled);
        assert_eq!(cancelled.transition_revision, 2);
        assert!(cancelled.retryable);
    }

    #[tokio::test]
    async fn refuses_concurrent_and_completed_replay_before_effect() {
        let coordinator = ManagedTransitionCoordinator::new();
        let process_id = coordinator.process_instance_id().await;
        coordinator
            .admit(request(process_id.clone(), "transition-a"))
            .await
            .unwrap();
        let concurrent = coordinator
            .admit(request(process_id.clone(), "transition-b"))
            .await
            .unwrap_err();
        assert_eq!(
            concurrent.kind,
            ManagedTransitionRefusalKind::ConcurrentTransition
        );
        coordinator
            .cancel(CancelManagedTransitionParams {
                transition_id: "transition-a".to_owned(),
                process_instance_id: process_id.clone(),
            })
            .await
            .unwrap();
        let replay = coordinator
            .admit(request(process_id, "transition-a"))
            .await
            .unwrap_err();
        assert_eq!(replay.kind, ManagedTransitionRefusalKind::CompletedReplay);
    }

    #[tokio::test]
    async fn refuses_changed_envelope_and_stale_process() {
        let coordinator = ManagedTransitionCoordinator::new();
        let process_id = coordinator.process_instance_id().await;
        coordinator
            .admit(request(process_id.clone(), "transition-a"))
            .await
            .unwrap();
        let mut conflicting = request(process_id.clone(), "transition-a");
        conflicting.intent = ManagedTransitionIntent::AdoptManagedLogout;
        let conflict = coordinator.admit(conflicting).await.unwrap_err();
        assert_eq!(
            conflict.kind,
            ManagedTransitionRefusalKind::TransitionIdConflict
        );
        let stale = coordinator
            .admit(request("old-process".to_owned(), "transition-b"))
            .await
            .unwrap_err();
        assert_eq!(stale.kind, ManagedTransitionRefusalKind::ProcessMismatch);
    }

    #[tokio::test]
    async fn wire_admission_refuses_until_server_authorization_exists_without_mutation() {
        let coordinator = ManagedTransitionCoordinator::new();
        let process_id = coordinator.process_instance_id().await;
        let refused = coordinator
            .start_not_admitted_response(request(process_id.clone(), "transition-a"))
            .await;
        let StartManagedTransitionResponse::Refused { refusal } = refused else {
            panic!("Slice 1 wire admission must remain authorization-gated");
        };
        assert_eq!(
            refusal.kind,
            ManagedTransitionRefusalKind::AuthorizationNotAdmitted
        );

        let admitted = coordinator
            .admit(request(process_id, "transition-a"))
            .await
            .expect("the refused wire request must not reserve the transition id");
        assert_eq!(admitted.phase, ManagedTransitionPhase::Admitted);
    }
}
