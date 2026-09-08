//! Credential-free, process-local transition coordination.
//!
//! This kernel intentionally does not know how to read, write, or install
//! authentication. Slice 2 binds callers and targets before it is exposed via
//! RPC; later slices attach the barrier, durable adoption, and reset work.

use codex_app_server_protocol::CancelManagedTransitionParams;
use codex_app_server_protocol::CancelManagedTransitionResponse;
use codex_app_server_protocol::MANAGED_AUTH_TRANSITION_CONTRACT_VERSION;
use codex_app_server_protocol::ManagedTransitionIntent;
use codex_app_server_protocol::ManagedTransitionPhase;
use codex_app_server_protocol::ManagedTransitionRefusal;
use codex_app_server_protocol::ManagedTransitionRefusalKind;
use codex_app_server_protocol::ManagedTransitionStatus;
use codex_app_server_protocol::ReadManagedTransitionParams;
use codex_app_server_protocol::ReadManagedTransitionResponse;
use codex_app_server_protocol::StartManagedTransitionParams;
use codex_app_server_protocol::StartManagedTransitionResponse;
use codex_login::AuthManager;
use sha2::Digest;
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct ManagedTransitionCoordinator {
    state: Arc<Mutex<CoordinatorState>>,
    /// Immutable per-coordinator; not behind `state`'s lock since revalidation
    /// only ever queries it, never mutates it.
    target_evidence_source: Arc<dyn TargetEvidenceSource>,
}

/// A fixed, always-consistent-with-itself source, for constructors that do
/// not care about target evidence (every existing Slice 1-era test).
/// `declared_profile: None` and `endpoint: String::new()` are themselves
/// synthetic, never real production values.
struct UnsetTargetEvidenceSource;

impl TargetEvidenceSource for UnsetTargetEvidenceSource {
    fn declared_profile(&self) -> Option<String> {
        None
    }

    fn executable_identity(
        &self,
    ) -> std::io::Result<codex_app_server_transport::PeerExecutableIdentity> {
        Ok(codex_app_server_transport::PeerExecutableIdentity::FileIdentity {
            device: 0,
            inode: 0,
        })
    }

    fn endpoint(&self) -> String {
        String::new()
    }

    fn pid(&self) -> u32 {
        0
    }
}

/// The credential-free transition view of the persisted current auth state.
///
/// A restart deliberately drops all in-process transition records. It does not
/// invent their outcome; it can only begin from the current auth authority
/// observed at startup. The opaque fingerprint is domain-separated and never
/// stores or emits the account identifier used to derive it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AuthoritativeAuthState {
    authority_available: bool,
    auth_revision: u64,
    auth_fingerprint: Option<String>,
}

impl AuthoritativeAuthState {
    pub(crate) fn from_auth_manager(auth_manager: &AuthManager) -> Self {
        match auth_manager.authoritative_auth_cached() {
            Ok(auth) => Self::from_account_id(auth.and_then(|auth| auth.get_account_id())),
            Err(_) => Self::unavailable(),
        }
    }

    fn from_account_id(account_id: Option<String>) -> Self {
        let auth_fingerprint = account_id.map(|account_id| {
            let mut hasher = Sha256::new();
            hasher.update(b"codex-app-server/managed-auth-transition/account/v1\\0");
            hasher.update(account_id.as_bytes());
            format!("{:x}", hasher.finalize())
        });
        Self {
            authority_available: true,
            // This kernel does not perform adoption. The first authoritative
            // snapshot after each process start is therefore revision zero.
            auth_revision: 0,
            auth_fingerprint,
        }
    }

    fn unavailable() -> Self {
        Self {
            authority_available: false,
            auth_revision: 0,
            auth_fingerprint: None,
        }
    }
}

/// The environment variable a future repository-owned command (Slice 5) sets
/// to declare which profile it started this server under. Slice 2 defines and
/// reads this contract now so its target-evidence record is stable; no
/// launcher/profile entry point exists yet (`CODEX-I05-S02-R049`,
/// `CODEX-I05-S02-R053`), so in every current production deployment this
/// reads back `None`.
pub(crate) const MANAGED_PROFILE_ENV_VAR: &str = "KESTREL_CODEX_MANAGED_PROFILE";

/// Injectable source of the server's own token-free target-evidence facts
/// (`CODEX-I05-S02-R004`). Production reads real process/environment state;
/// disposable tests inject synthetic facts (`CODEX-I05-S02-R038`) to prove
/// the coordinator detects a simulated replacement without ever touching a
/// real installed profile, launcher, or live target
/// (`R049-R050`, `R053`, `R060-R062`).
pub(crate) trait TargetEvidenceSource: Send + Sync {
    fn declared_profile(&self) -> Option<String>;
    fn executable_identity(&self) -> std::io::Result<codex_app_server_transport::PeerExecutableIdentity>;
    fn endpoint(&self) -> String;
    fn pid(&self) -> u32;
}

/// Reads the server's own real facts: `MANAGED_PROFILE_ENV_VAR`, the same
/// platform-correct running-executable identity Unix peer provenance already
/// captures, the real OS process id, and the real control-socket path.
pub(crate) struct ProcessTargetEvidenceSource {
    endpoint: String,
}

impl ProcessTargetEvidenceSource {
    pub(crate) fn new(endpoint: String) -> Self {
        Self { endpoint }
    }
}

impl TargetEvidenceSource for ProcessTargetEvidenceSource {
    fn declared_profile(&self) -> Option<String> {
        std::env::var(MANAGED_PROFILE_ENV_VAR).ok()
    }

    fn executable_identity(&self) -> std::io::Result<codex_app_server_transport::PeerExecutableIdentity> {
        codex_app_server_transport::PeerExecutableIdentity::capture_running_process()
    }

    fn endpoint(&self) -> String {
        self.endpoint.clone()
    }

    fn pid(&self) -> u32 {
        std::process::id()
    }
}

/// A server-produced, token-free description of the exact process a caller
/// is bound to (`CODEX-I05-S02-R004`). Evidence only, never a bearer grant:
/// comparison against externally, independently known expected facts is
/// Slice 5's own repository-command responsibility. Re-derived (not cached)
/// at each revalidation point so a legitimate or illegitimate change is
/// observed rather than masked by a stale snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TargetEvidence {
    declared_profile: Option<String>,
    executable_identity: Option<codex_app_server_transport::PeerExecutableIdentity>,
    endpoint: String,
    pid: u32,
    /// A fresh opaque identity for this specific evidence snapshot, distinct
    /// from the coordinator's own process-instance identity: it changes on
    /// every capture, not only on restart, so two captures within the same
    /// process are still distinguishable records.
    record_identity: String,
}

impl TargetEvidence {
    fn capture(source: &dyn TargetEvidenceSource) -> Self {
        Self {
            declared_profile: source.declared_profile(),
            // Lookup failure is evidence of an unavailable target, not a
            // logged-out-shaped `None`; keep it distinct from "not captured".
            executable_identity: source.executable_identity().ok(),
            endpoint: source.endpoint(),
            pid: source.pid(),
            record_identity: Uuid::new_v4().to_string(),
        }
    }

    /// True only when every replacement-sensitive fact this capture observed
    /// is identical to the reference capture (`record_identity` excluded --
    /// it identifies the snapshot itself, not the target). `executable_identity`
    /// missing on *either* side is never treated as a match: an unavailable
    /// executable identity is exactly the "replaced/unreadable process"
    /// condition this check exists to catch, not evidence of consistency.
    fn matches_target(&self, reference: &TargetEvidence) -> bool {
        self.declared_profile == reference.declared_profile
            && self.endpoint == reference.endpoint
            && self.pid == reference.pid
            && matches!(
                (self.executable_identity, reference.executable_identity),
                (Some(a), Some(b)) if a == b
            )
    }
}

#[derive(Debug)]
struct CoordinatorState {
    process_instance_id: String,
    auth_revision: u64,
    transition_revision: u64,
    auth_fingerprint: Option<String>,
    auth_authority_available: bool,
    target_evidence: TargetEvidence,
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
    process_instance_id: String,
    prior_auth_revision: u64,
    result_auth_revision: u64,
    prior_transition_revision: u64,
    result_transition_revision: u64,
    prior_auth_fingerprint: Option<String>,
    result_auth_fingerprint: Option<String>,
    refusal: Option<ManagedTransitionRefusalKind>,
}

impl ManagedTransitionCoordinator {
    pub(crate) fn new() -> Self {
        Self::from_authoritative_auth_state(AuthoritativeAuthState {
            authority_available: true,
            auth_revision: 0,
            auth_fingerprint: None,
        })
    }

    /// Construction with target evidence unset, for the many existing tests
    /// that exercise coordinator/auth behavior and do not care about Slice
    /// 2's own target-binding leg.
    pub(crate) fn from_authoritative_auth_state(
        authoritative_auth: AuthoritativeAuthState,
    ) -> Self {
        Self::from_authoritative_auth_state_and_target_evidence_source(
            authoritative_auth,
            Arc::new(UnsetTargetEvidenceSource),
        )
    }

    /// The real production construction path
    /// (`app-server/src/message_processor.rs`'s constructor) and Slice 2's
    /// own disposable target-evidence tests use this directly.
    pub(crate) fn from_authoritative_auth_state_and_target_evidence_source(
        authoritative_auth: AuthoritativeAuthState,
        target_evidence_source: Arc<dyn TargetEvidenceSource>,
    ) -> Self {
        let target_evidence = TargetEvidence::capture(target_evidence_source.as_ref());
        Self {
            state: Arc::new(Mutex::new(CoordinatorState {
                process_instance_id: Uuid::now_v7().to_string(),
                auth_revision: authoritative_auth.auth_revision,
                transition_revision: 0,
                auth_fingerprint: authoritative_auth.auth_fingerprint,
                auth_authority_available: authoritative_auth.authority_available,
                target_evidence,
                active: None,
                completed: HashMap::new(),
            })),
            target_evidence_source,
        }
    }

    pub(crate) async fn process_instance_id(&self) -> String {
        self.state.lock().await.process_instance_id.clone()
    }

    /// Re-derives current target evidence from this coordinator's own source
    /// and compares it against the reference captured at construction
    /// (`CODEX-I05-S02-R004`). This is the reusable revalidation primitive;
    /// Slice 2 calls it before effect in [`Self::admit`]. Later slices call
    /// it again after writer completion, before dispatch, and before
    /// acknowledgement as those mechanisms come online.
    async fn target_evidence_still_matches(&self) -> bool {
        let current = TargetEvidence::capture(self.target_evidence_source.as_ref());
        let state = self.state.lock().await;
        current.matches_target(&state.target_evidence)
    }

    pub(crate) async fn admit(
        &self,
        params: StartManagedTransitionParams,
    ) -> Result<ManagedTransitionStatus, ManagedTransitionRefusal> {
        let envelope = TransitionEnvelope::from(params);
        // Revalidated before acquiring `state`'s own lock -- this method
        // re-locks internally and must never be called while already held.
        let target_evidence_matches = self.target_evidence_still_matches().await;
        let mut state = self.state.lock().await;

        if !state.auth_authority_available {
            return Err(authoritative_auth_unavailable(&state, &envelope));
        }

        if !target_evidence_matches {
            return Err(refusal(
                &state,
                &envelope,
                ManagedTransitionRefusalKind::InvalidRequest,
                false,
            ));
        }

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
            process_instance_id: state.process_instance_id.clone(),
            prior_auth_revision: state.auth_revision,
            result_auth_revision: state.auth_revision,
            prior_transition_revision: state.transition_revision - 1,
            result_transition_revision: state.transition_revision,
            prior_auth_fingerprint: state.auth_fingerprint.clone(),
            result_auth_fingerprint: state.auth_fingerprint.clone(),
            refusal: None,
        };
        let status = status_for(&record);
        state.active = Some(record);
        Ok(status)
    }

    /// Slice 1 exposes the versioned wire contract but deliberately does not
    /// admit a caller. Slice 2 replaces this gate with server-derived caller
    /// authorization before it can create or alter a transition record.
    /// Issue 05 Slice 2's public wire entry point (`CODEX-I05-S02-R005`).
    /// `caller_authorized` must be exactly
    /// `session.managed_transition_caller_authorized()`, evaluated by the
    /// dispatcher from server-established connection state before this call
    /// -- never re-derived here from caller-supplied data. An unauthorized
    /// caller receives the identical `AuthorizationNotAdmitted` refusal
    /// regardless of target-evidence or coordinator state, so no signal about
    /// server-side health leaks to a caller that has not yet qualified.
    pub(crate) async fn start_dispatch(
        &self,
        params: StartManagedTransitionParams,
        caller_authorized: bool,
    ) -> StartManagedTransitionResponse {
        if !caller_authorized {
            let state = self.state.lock().await;
            return StartManagedTransitionResponse::Refused {
                refusal: authorization_not_admitted(&state, params.into()),
            };
        }
        match self.admit(params).await {
            Ok(status) => StartManagedTransitionResponse::Accepted { status },
            Err(refusal) => StartManagedTransitionResponse::Refused { refusal },
        }
    }

    pub(crate) async fn read(
        &self,
        params: ReadManagedTransitionParams,
    ) -> Result<ManagedTransitionStatus, ManagedTransitionRefusal> {
        let state = self.state.lock().await;
        let envelope = TransitionEnvelope::read(params);
        if !state.auth_authority_available {
            return Err(authoritative_auth_unavailable(&state, &envelope));
        }
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
            return Ok(status_for(record));
        }
        if let Some(record) = state.completed.get(&envelope.transition_id) {
            return Ok(status_for(record));
        }
        Err(refusal(
            &state,
            &envelope,
            ManagedTransitionRefusalKind::InvalidRequest,
            true,
        ))
    }

    /// See [`Self::start_dispatch`] for the `caller_authorized` contract.
    pub(crate) async fn read_dispatch(
        &self,
        params: ReadManagedTransitionParams,
        caller_authorized: bool,
    ) -> ReadManagedTransitionResponse {
        if !caller_authorized {
            let state = self.state.lock().await;
            return ReadManagedTransitionResponse::Refused {
                refusal: authorization_not_admitted(&state, TransitionEnvelope::read(params)),
            };
        }
        match self.read(params).await {
            Ok(status) => ReadManagedTransitionResponse::Accepted { status },
            Err(refusal) => ReadManagedTransitionResponse::Refused { refusal },
        }
    }

    pub(crate) async fn cancel(
        &self,
        params: CancelManagedTransitionParams,
    ) -> Result<ManagedTransitionStatus, ManagedTransitionRefusal> {
        let mut state = self.state.lock().await;
        let envelope = TransitionEnvelope::cancel(params);
        if !state.auth_authority_available {
            return Err(authoritative_auth_unavailable(&state, &envelope));
        }
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
            result_transition_revision: state.transition_revision,
            ..record
        };
        let status = status_for(&cancelled);
        state
            .completed
            .insert(cancelled.envelope.transition_id.clone(), cancelled);
        Ok(status)
    }

    /// See [`Self::start_dispatch`] for the `caller_authorized` contract.
    pub(crate) async fn cancel_dispatch(
        &self,
        params: CancelManagedTransitionParams,
        caller_authorized: bool,
    ) -> CancelManagedTransitionResponse {
        if !caller_authorized {
            let state = self.state.lock().await;
            return CancelManagedTransitionResponse::Refused {
                refusal: authorization_not_admitted(&state, TransitionEnvelope::cancel(params)),
            };
        }
        match self.cancel(params).await {
            Ok(status) => CancelManagedTransitionResponse::Accepted { status },
            Err(refusal) => CancelManagedTransitionResponse::Refused { refusal },
        }
    }

    /// Advances the credential-free kernel through a legal internal phase.
    /// Wire callers remain authorization-gated until Slice 2; later slices use
    /// this one state owner rather than constructing caller-composed progress.
    pub(crate) async fn advance(
        &self,
        transition_id: &str,
        next_phase: ManagedTransitionPhase,
    ) -> Result<ManagedTransitionStatus, ManagedTransitionRefusal> {
        let mut state = self.state.lock().await;
        if !state.auth_authority_available {
            return Err(refusal_for_transition_id(
                &state,
                transition_id,
                ManagedTransitionRefusalKind::AuthoritativeAuthUnavailable,
                true,
            ));
        }
        let Some(record) = state.active.take() else {
            return Err(refusal_for_transition_id(
                &state,
                transition_id,
                ManagedTransitionRefusalKind::InvalidRequest,
                false,
            ));
        };
        if record.envelope.transition_id != transition_id {
            state.active = Some(record);
            return Err(refusal_for_transition_id(
                &state,
                transition_id,
                ManagedTransitionRefusalKind::TransitionIdConflict,
                true,
            ));
        }
        if !legal_phase_edge(record.phase, next_phase) {
            state.active = Some(record);
            return Err(refusal_for_transition_id(
                &state,
                transition_id,
                ManagedTransitionRefusalKind::InvalidRequest,
                false,
            ));
        }

        state.transition_revision += 1;
        let advanced = TransitionRecord {
            phase: next_phase,
            retryable: next_phase == ManagedTransitionPhase::Quarantined,
            result_transition_revision: state.transition_revision,
            ..record
        };
        let status = status_for(&advanced);
        if next_phase.is_terminal() {
            state
                .completed
                .insert(advanced.envelope.transition_id.clone(), advanced);
        } else {
            state.active = Some(advanced);
        }
        Ok(status)
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

fn refusal_for_transition_id(
    state: &CoordinatorState,
    transition_id: &str,
    kind: ManagedTransitionRefusalKind,
    retryable: bool,
) -> ManagedTransitionRefusal {
    ManagedTransitionRefusal {
        kind,
        retryable,
        process_instance_id: state.process_instance_id.clone(),
        transition_id: transition_id.to_owned(),
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

fn authoritative_auth_unavailable(
    state: &CoordinatorState,
    envelope: &TransitionEnvelope,
) -> ManagedTransitionRefusal {
    refusal(
        state,
        envelope,
        ManagedTransitionRefusalKind::AuthoritativeAuthUnavailable,
        true,
    )
}

fn status_for(record: &TransitionRecord) -> ManagedTransitionStatus {
    ManagedTransitionStatus {
        process_instance_id: record.process_instance_id.clone(),
        transition_id: Some(record.envelope.transition_id.clone()),
        intent: Some(record.envelope.intent),
        phase: record.phase,
        retryable: record.retryable,
        prior_auth_revision: record.prior_auth_revision,
        auth_revision: record.result_auth_revision,
        prior_transition_revision: record.prior_transition_revision,
        transition_revision: record.result_transition_revision,
        prior_auth_fingerprint: record.prior_auth_fingerprint.clone(),
        result_auth_fingerprint: record.result_auth_fingerprint.clone(),
        refusal: record.refusal,
    }
}

fn legal_phase_edge(from: ManagedTransitionPhase, to: ManagedTransitionPhase) -> bool {
    matches!(
        (from, to),
        (
            ManagedTransitionPhase::Admitted,
            ManagedTransitionPhase::Draining
        ) | (
            ManagedTransitionPhase::Draining,
            ManagedTransitionPhase::Adopting
        ) | (
            ManagedTransitionPhase::Adopting,
            ManagedTransitionPhase::Resetting
        ) | (
            ManagedTransitionPhase::Resetting,
            ManagedTransitionPhase::Succeeded
        ) | (
            ManagedTransitionPhase::Admitted,
            ManagedTransitionPhase::Quarantined
        ) | (
            ManagedTransitionPhase::Draining,
            ManagedTransitionPhase::Quarantined
        ) | (
            ManagedTransitionPhase::Adopting,
            ManagedTransitionPhase::Quarantined
        ) | (
            ManagedTransitionPhase::Resetting,
            ManagedTransitionPhase::Quarantined
        )
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(process_instance_id: String, transition_id: &str) -> StartManagedTransitionParams {
        request_at_revision(process_instance_id, transition_id, 0)
    }

    fn request_at_revision(
        process_instance_id: String,
        transition_id: &str,
        expected_transition_revision: u64,
    ) -> StartManagedTransitionParams {
        StartManagedTransitionParams {
            contract_version: MANAGED_AUTH_TRANSITION_CONTRACT_VERSION,
            transition_id: transition_id.to_owned(),
            process_instance_id,
            intent: ManagedTransitionIntent::AdoptManagedAuth,
            expected_auth_revision: 0,
            expected_transition_revision,
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
                contract_version: MANAGED_AUTH_TRANSITION_CONTRACT_VERSION,
                transition_id: "transition-a".to_owned(),
                process_instance_id: process_id.clone(),
            })
            .await
            .unwrap();
        assert_eq!(observed, admitted);

        let cancelled = coordinator
            .cancel(CancelManagedTransitionParams {
                contract_version: MANAGED_AUTH_TRANSITION_CONTRACT_VERSION,
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
                contract_version: MANAGED_AUTH_TRANSITION_CONTRACT_VERSION,
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
    async fn wire_admission_refuses_an_unauthorized_caller_without_mutation() {
        let coordinator = ManagedTransitionCoordinator::new();
        let process_id = coordinator.process_instance_id().await;
        let refused = coordinator
            .start_dispatch(
                request(process_id.clone(), "transition-a"),
                /*caller_authorized*/ false,
            )
            .await;
        let StartManagedTransitionResponse::Refused { refusal } = refused else {
            panic!("an unauthorized wire caller must never be admitted");
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

    #[tokio::test]
    async fn wire_admission_admits_an_authorized_caller_with_matching_target_evidence() {
        let coordinator = ManagedTransitionCoordinator::new();
        let process_id = coordinator.process_instance_id().await;
        let accepted = coordinator
            .start_dispatch(
                request(process_id, "transition-a"),
                /*caller_authorized*/ true,
            )
            .await;
        let StartManagedTransitionResponse::Accepted { status } = accepted else {
            panic!("an authorized caller with matching target evidence must be admitted");
        };
        assert_eq!(status.phase, ManagedTransitionPhase::Admitted);
    }

    /// Disposable synthetic target-evidence source (`CODEX-I05-S02-R038`)
    /// whose facts can be swapped mid-test, simulating a replaced process
    /// without touching any real installed profile, launcher, or live target.
    struct SyntheticTargetEvidenceSource {
        facts: std::sync::Mutex<SyntheticTargetFacts>,
    }

    #[derive(Clone)]
    struct SyntheticTargetFacts {
        declared_profile: Option<String>,
        executable_identity_inode: u64,
        endpoint: String,
        pid: u32,
    }

    impl SyntheticTargetFacts {
        fn baseline() -> Self {
            Self {
                declared_profile: Some("dev".to_owned()),
                executable_identity_inode: 42,
                endpoint: "/synthetic/control.sock".to_owned(),
                pid: 4242,
            }
        }
    }

    impl SyntheticTargetEvidenceSource {
        fn new(facts: SyntheticTargetFacts) -> Arc<Self> {
            Arc::new(Self {
                facts: std::sync::Mutex::new(facts),
            })
        }

        fn replace_facts(&self, facts: SyntheticTargetFacts) {
            *self.facts.lock().expect("synthetic facts lock") = facts;
        }
    }

    impl TargetEvidenceSource for SyntheticTargetEvidenceSource {
        fn declared_profile(&self) -> Option<String> {
            self.facts.lock().expect("synthetic facts lock").declared_profile.clone()
        }

        fn executable_identity(
            &self,
        ) -> std::io::Result<codex_app_server_transport::PeerExecutableIdentity> {
            Ok(codex_app_server_transport::PeerExecutableIdentity::FileIdentity {
                device: 1,
                inode: self.facts.lock().expect("synthetic facts lock").executable_identity_inode,
            })
        }

        fn endpoint(&self) -> String {
            self.facts.lock().expect("synthetic facts lock").endpoint.clone()
        }

        fn pid(&self) -> u32 {
            self.facts.lock().expect("synthetic facts lock").pid
        }
    }

    #[tokio::test]
    async fn admit_refuses_when_target_evidence_no_longer_matches_the_captured_reference() {
        let source = SyntheticTargetEvidenceSource::new(SyntheticTargetFacts::baseline());
        let coordinator = ManagedTransitionCoordinator::from_authoritative_auth_state_and_target_evidence_source(
            AuthoritativeAuthState {
                authority_available: true,
                auth_revision: 0,
                auth_fingerprint: None,
            },
            source.clone(),
        );
        let process_id = coordinator.process_instance_id().await;

        // A simulated replacement: the same coordinator, but its target
        // evidence source now reports a different running executable --
        // exactly the "replaced-process" case the plan's own validation
        // signal names.
        let mut replaced = SyntheticTargetFacts::baseline();
        replaced.executable_identity_inode += 1;
        source.replace_facts(replaced);

        let refused = coordinator
            .admit(request(process_id, "transition-a"))
            .await
            .unwrap_err();
        assert_eq!(refused.kind, ManagedTransitionRefusalKind::InvalidRequest);
    }

    #[tokio::test]
    async fn admit_refuses_when_declared_profile_or_endpoint_or_pid_changes() {
        for mutate in [
            (|facts: &mut SyntheticTargetFacts| facts.declared_profile = Some("live".to_owned()))
                as fn(&mut SyntheticTargetFacts),
            |facts: &mut SyntheticTargetFacts| facts.endpoint = "/synthetic/other.sock".to_owned(),
            |facts: &mut SyntheticTargetFacts| facts.pid += 1,
        ] {
            let source = SyntheticTargetEvidenceSource::new(SyntheticTargetFacts::baseline());
            let coordinator = ManagedTransitionCoordinator::from_authoritative_auth_state_and_target_evidence_source(
                AuthoritativeAuthState {
                    authority_available: true,
                    auth_revision: 0,
                    auth_fingerprint: None,
                },
                source.clone(),
            );
            let process_id = coordinator.process_instance_id().await;

            let mut mutated = SyntheticTargetFacts::baseline();
            mutate(&mut mutated);
            source.replace_facts(mutated);

            let refused = coordinator
                .admit(request(process_id, "transition-a"))
                .await
                .unwrap_err();
            assert_eq!(refused.kind, ManagedTransitionRefusalKind::InvalidRequest);
        }
    }

    #[tokio::test]
    async fn admit_succeeds_when_target_evidence_is_re_derived_identically() {
        let source = SyntheticTargetEvidenceSource::new(SyntheticTargetFacts::baseline());
        let coordinator = ManagedTransitionCoordinator::from_authoritative_auth_state_and_target_evidence_source(
            AuthoritativeAuthState {
                authority_available: true,
                auth_revision: 0,
                auth_fingerprint: None,
            },
            source,
        );
        let process_id = coordinator.process_instance_id().await;
        let admitted = coordinator
            .admit(request(process_id, "transition-a"))
            .await
            .expect("unchanged target evidence must not refuse admission");
        assert_eq!(admitted.phase, ManagedTransitionPhase::Admitted);
    }

    fn read_request(
        process_instance_id: String,
        transition_id: &str,
    ) -> ReadManagedTransitionParams {
        ReadManagedTransitionParams {
            contract_version: MANAGED_AUTH_TRANSITION_CONTRACT_VERSION,
            transition_id: transition_id.to_owned(),
            process_instance_id,
        }
    }

    fn cancel_request(
        process_instance_id: String,
        transition_id: &str,
    ) -> CancelManagedTransitionParams {
        CancelManagedTransitionParams {
            contract_version: MANAGED_AUTH_TRANSITION_CONTRACT_VERSION,
            transition_id: transition_id.to_owned(),
            process_instance_id,
        }
    }

    #[tokio::test]
    async fn kernel_enforces_legal_phase_edges_and_terminal_replay_is_snapshot_stable() {
        let coordinator = ManagedTransitionCoordinator::new();
        let process_id = coordinator.process_instance_id().await;
        let admitted = coordinator
            .admit(request(process_id.clone(), "transition-a"))
            .await
            .unwrap();
        let illegal = coordinator
            .advance("transition-a", ManagedTransitionPhase::Adopting)
            .await
            .unwrap_err();
        assert_eq!(illegal.kind, ManagedTransitionRefusalKind::InvalidRequest);
        assert_eq!(
            coordinator
                .read(read_request(process_id.clone(), "transition-a"))
                .await
                .unwrap(),
            admitted
        );

        for phase in [
            ManagedTransitionPhase::Draining,
            ManagedTransitionPhase::Adopting,
            ManagedTransitionPhase::Resetting,
            ManagedTransitionPhase::Succeeded,
        ] {
            coordinator.advance("transition-a", phase).await.unwrap();
        }
        let completed = coordinator
            .read(read_request(process_id.clone(), "transition-a"))
            .await
            .unwrap();
        assert_eq!(completed.phase, ManagedTransitionPhase::Succeeded);
        assert_eq!(
            coordinator
                .advance("transition-a", ManagedTransitionPhase::Quarantined)
                .await
                .unwrap_err()
                .kind,
            ManagedTransitionRefusalKind::InvalidRequest
        );

        coordinator
            .admit(request_at_revision(
                process_id.clone(),
                "transition-b",
                completed.transition_revision,
            ))
            .await
            .unwrap();
        let replayed_a = coordinator
            .read(read_request(process_id, "transition-a"))
            .await
            .unwrap();
        assert_eq!(
            replayed_a, completed,
            "completed A must not borrow B revisions"
        );
    }

    #[tokio::test]
    async fn cancel_is_only_legal_from_admitted_and_preserves_the_active_record_on_refusal() {
        let coordinator = ManagedTransitionCoordinator::new();
        let process_id = coordinator.process_instance_id().await;
        coordinator
            .admit(request(process_id.clone(), "transition-a"))
            .await
            .unwrap();
        coordinator
            .advance("transition-a", ManagedTransitionPhase::Draining)
            .await
            .unwrap();
        let refusal = coordinator
            .cancel(cancel_request(process_id.clone(), "transition-a"))
            .await
            .unwrap_err();
        assert_eq!(refusal.kind, ManagedTransitionRefusalKind::LateCancellation);
        assert_eq!(
            coordinator
                .read(read_request(process_id, "transition-a"))
                .await
                .unwrap()
                .phase,
            ManagedTransitionPhase::Draining
        );
    }

    #[tokio::test]
    async fn cas_fields_and_process_identity_refuse_independently_before_reserving_transition() {
        let coordinator = ManagedTransitionCoordinator::new();
        let process_id = coordinator.process_instance_id().await;
        let mut expected_transition_revision = 0;
        for (suffix, mutate, expected) in [
            (
                "auth",
                Box::new(|request: &mut StartManagedTransitionParams| {
                    request.expected_auth_revision = 1
                }) as Box<dyn Fn(&mut StartManagedTransitionParams)>,
                ManagedTransitionRefusalKind::StaleAuthRevision,
            ),
            (
                "transition",
                Box::new(|request: &mut StartManagedTransitionParams| {
                    request.expected_transition_revision = 1
                }),
                ManagedTransitionRefusalKind::StaleTransitionRevision,
            ),
            (
                "fingerprint",
                Box::new(|request: &mut StartManagedTransitionParams| {
                    request.expected_auth_fingerprint = Some("different".to_owned())
                }),
                ManagedTransitionRefusalKind::StaleAuthFingerprint,
            ),
        ] {
            let id = format!("transition-{suffix}");
            let mut stale =
                request_at_revision(process_id.clone(), &id, expected_transition_revision);
            mutate(&mut stale);
            assert_eq!(coordinator.admit(stale).await.unwrap_err().kind, expected);
            assert_eq!(
                coordinator
                    .admit(request_at_revision(
                        process_id.clone(),
                        &id,
                        expected_transition_revision,
                    ))
                    .await
                    .unwrap()
                    .phase,
                ManagedTransitionPhase::Admitted
            );
            coordinator
                .cancel(cancel_request(process_id.clone(), &id))
                .await
                .unwrap();
            expected_transition_revision += 2;
        }

        assert_eq!(
            coordinator
                .admit(request("other-process".to_owned(), "transition-process"))
                .await
                .unwrap_err()
                .kind,
            ManagedTransitionRefusalKind::ProcessMismatch
        );
    }

    #[tokio::test]
    async fn concurrent_admission_has_one_winner_and_restart_reconstructs_only_current_state() {
        let coordinator = ManagedTransitionCoordinator::new();
        let process_id = coordinator.process_instance_id().await;
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let left_coordinator = coordinator.clone();
        let left_process_id = process_id.clone();
        let left_barrier = Arc::clone(&barrier);
        let left = tokio::spawn(async move {
            left_barrier.wait().await;
            left_coordinator
                .admit(request(left_process_id, "transition-a"))
                .await
        });
        let right_coordinator = coordinator.clone();
        let right_process_id = process_id.clone();
        let right_barrier = Arc::clone(&barrier);
        let right = tokio::spawn(async move {
            right_barrier.wait().await;
            right_coordinator
                .admit(request(right_process_id, "transition-b"))
                .await
        });
        barrier.wait().await;
        let (left, right) = (left.await.unwrap(), right.await.unwrap());
        assert_eq!(
            [left.as_ref().err(), right.as_ref().err()]
                .into_iter()
                .flatten()
                .filter(|refusal| refusal.kind == ManagedTransitionRefusalKind::ConcurrentTransition)
                .count(),
            1
        );

        let authoritative = AuthoritativeAuthState::from_account_id(Some("account-a".to_owned()));
        let expected_fingerprint = authoritative.auth_fingerprint.clone();
        for (boundary, phases, cancel) in [
            ("admitted", vec![], false),
            ("draining", vec![ManagedTransitionPhase::Draining], false),
            (
                "adopting",
                vec![
                    ManagedTransitionPhase::Draining,
                    ManagedTransitionPhase::Adopting,
                ],
                false,
            ),
            (
                "resetting",
                vec![
                    ManagedTransitionPhase::Draining,
                    ManagedTransitionPhase::Adopting,
                    ManagedTransitionPhase::Resetting,
                ],
                false,
            ),
            (
                "succeeded",
                vec![
                    ManagedTransitionPhase::Draining,
                    ManagedTransitionPhase::Adopting,
                    ManagedTransitionPhase::Resetting,
                    ManagedTransitionPhase::Succeeded,
                ],
                false,
            ),
            (
                "quarantined",
                vec![
                    ManagedTransitionPhase::Draining,
                    ManagedTransitionPhase::Quarantined,
                ],
                false,
            ),
            ("cancelled", vec![], true),
        ] {
            let before_restart =
                ManagedTransitionCoordinator::from_authoritative_auth_state(authoritative.clone());
            let old_process_id = before_restart.process_instance_id().await;
            let transition_id = format!("before-restart-{boundary}");
            let mut initial = request(old_process_id.clone(), &transition_id);
            initial.expected_auth_fingerprint = expected_fingerprint.clone();
            before_restart.admit(initial).await.unwrap();
            if cancel {
                before_restart
                    .cancel(cancel_request(old_process_id.clone(), &transition_id))
                    .await
                    .unwrap();
            } else {
                for phase in phases {
                    before_restart.advance(&transition_id, phase).await.unwrap();
                }
            }

            let restarted =
                ManagedTransitionCoordinator::from_authoritative_auth_state(authoritative.clone());
            let restarted_process_id = restarted.process_instance_id().await;
            assert_ne!(restarted_process_id, old_process_id);
            assert_eq!(
                restarted
                    .read(read_request(old_process_id, &transition_id))
                    .await
                    .unwrap_err()
                    .kind,
                ManagedTransitionRefusalKind::ProcessMismatch,
                "{boundary}: old process may not acknowledge a restarted coordinator"
            );
            assert_eq!(
                restarted
                    .read(read_request(restarted_process_id.clone(), &transition_id))
                    .await
                    .unwrap_err()
                    .kind,
                ManagedTransitionRefusalKind::InvalidRequest,
                "{boundary}: a restarted coordinator must not claim an unproven old outcome"
            );
            let mut retry = request(restarted_process_id, &format!("retry-{boundary}"));
            retry.expected_auth_fingerprint = expected_fingerprint.clone();
            let retried = restarted.admit(retry).await.unwrap();
            assert_eq!(retried.result_auth_fingerprint, expected_fingerprint);
            assert_eq!(retried.phase, ManagedTransitionPhase::Admitted);
        }
    }

    /// Builds the real production authority mapping
    /// (`AuthManager::new` -> `AuthoritativeAuthState::from_auth_manager`) and
    /// a coordinator from it, against a genuine on-disk `codex_home`. Used
    /// directly against the coordinator's own internal `admit`/`advance`/
    /// `read`/`cancel` methods, never through the public wire gate, so this
    /// never opens the still-unexposed Slice 2 authorization surface
    /// (`CODEX-I05-S01-R07-001`'s own required correction).
    async fn coordinator_from_real_persisted_auth(codex_home: &std::path::Path) -> (ManagedTransitionCoordinator, String) {
        let auth_manager = codex_login::AuthManager::new(
            codex_home.to_path_buf(),
            /*enable_codex_api_key_env*/ false,
            codex_config::types::AuthCredentialsStoreMode::File,
            /*forced_chatgpt_workspace_id*/ None,
            /*chatgpt_base_url*/ None,
            codex_login::AuthKeyringBackendKind::default(),
            codex_login::test_support::transport_default_auth_route_config(),
        )
        .await;
        let coordinator = ManagedTransitionCoordinator::from_authoritative_auth_state(
            AuthoritativeAuthState::from_auth_manager(&auth_manager),
        );
        let process_id = coordinator.process_instance_id().await;
        (coordinator, process_id)
    }

    const REAL_MAPPER_TEST_ACCOUNT_ID: &str = "intended-managed-account";

    /// A minimal, genuinely parseable (unsigned, `alg: none`) ID token JWT.
    /// `IdTokenInfo`'s own on-disk representation round-trips through its raw
    /// JWT string (`token_data::parse_chatgpt_jwt_claims`), so a `Default`
    /// value cannot survive a real save-then-load cycle -- this constructs
    /// the same minimal shape `app_test_support::encode_id_token` uses.
    fn minimal_id_token_jwt() -> codex_login::token_data::IdTokenInfo {
        use base64::Engine;
        let encode = |value: &serde_json::Value| {
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(serde_json::to_vec(value).expect("serialize jwt part"))
        };
        let header = encode(&serde_json::json!({ "alg": "none", "typ": "JWT" }));
        let payload = encode(&serde_json::json!({}));
        let signature =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"test-signature");
        let raw_jwt = format!("{header}.{payload}.{signature}");
        codex_login::token_data::parse_chatgpt_jwt_claims(&raw_jwt)
            .expect("minimal JWT must parse as valid ID token claims")
    }

    /// Writes a genuine ChatGPT-mode persisted auth record naming a real
    /// account id, so the production mapper reconstructs the durable
    /// intended-account fingerprint rather than a null one (`CODEX-I05-S01-R04`
    /// Round 08's third required correction).
    fn write_chatgpt_auth_for_intended_account(codex_home: &std::path::Path, account_id: &str) {
        let auth = codex_login::AuthDotJson {
            auth_mode: Some(codex_protocol::auth::AuthMode::Chatgpt),
            openai_api_key: None,
            tokens: Some(codex_login::TokenData {
                id_token: minimal_id_token_jwt(),
                access_token: "test-access-token".to_owned(),
                refresh_token: "test-refresh-token".to_owned(),
                account_id: Some(account_id.to_owned()),
            }),
            last_refresh: Some(chrono::Utc::now()),
            agent_identity: None,
            personal_access_token: None,
            bedrock_api_key: None,
        };
        codex_login::save_auth(
            codex_home,
            &auth,
            codex_config::types::AuthCredentialsStoreMode::File,
            codex_login::AuthKeyringBackendKind::default(),
        )
        .expect("write valid auth.json");
    }

    /// Reproduces the exact domain-separated fingerprint derivation in
    /// `AuthoritativeAuthState::from_account_id`, above, so the test can
    /// assert the production mapper reconstructs precisely the intended
    /// account's fingerprint, not merely a non-null one.
    fn expected_fingerprint_for_account(account_id: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(b"codex-app-server/managed-auth-transition/account/v1\\0");
        hasher.update(account_id.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    /// Admits a brand-new transition on an already-constructed coordinator,
    /// drives it all the way to completion, and proves it acknowledges
    /// truthfully with the re-derived intended fingerprint -- R022's
    /// "completes and acknowledges" retry contract, not merely a fresh
    /// `Admitted` re-admission.
    async fn complete_a_fresh_transition(
        coordinator: &ManagedTransitionCoordinator,
        process_id: &str,
        boundary: &str,
        expected_fingerprint: &str,
    ) {
        let transition_id = format!("retry-{boundary}");
        let mut retry = request(process_id.to_owned(), &transition_id);
        retry.expected_auth_fingerprint = Some(expected_fingerprint.to_owned());
        let admitted = coordinator
            .admit(retry)
            .await
            .unwrap_or_else(|e| panic!("{boundary}: retry after restart must be admitted: {e:?}"));
        assert_eq!(admitted.phase, ManagedTransitionPhase::Admitted);
        for phase in [
            ManagedTransitionPhase::Draining,
            ManagedTransitionPhase::Adopting,
            ManagedTransitionPhase::Resetting,
            ManagedTransitionPhase::Succeeded,
        ] {
            coordinator
                .advance(&transition_id, phase)
                .await
                .unwrap_or_else(|e| panic!("{boundary}: retry must advance to {phase:?}: {e:?}"));
        }
        let completed = coordinator
            .read(read_request(process_id.to_owned(), &transition_id))
            .await
            .unwrap_or_else(|e| panic!("{boundary}: completed retry must be readable: {e:?}"));
        assert_eq!(completed.phase, ManagedTransitionPhase::Succeeded);
        assert_eq!(
            completed.result_auth_fingerprint.as_deref(),
            Some(expected_fingerprint),
            "{boundary}: the completed retry must acknowledge the intended account's own fingerprint"
        );
    }

    #[tokio::test]
    async fn restart_reconstructs_every_material_phase_through_the_real_persisted_auth_mapper() {
        let codex_home = tempfile::TempDir::new().expect("create temp codex_home");
        write_chatgpt_auth_for_intended_account(codex_home.path(), REAL_MAPPER_TEST_ACCOUNT_ID);
        let expected_fingerprint = expected_fingerprint_for_account(REAL_MAPPER_TEST_ACCOUNT_ID);

        // Every material phase and each terminal state, each driven from a
        // fresh coordinator built on the real mapper, restarting (a fresh
        // `AuthManager` reading the same unchanged on-disk source) between
        // the admitting process and the process that observes the restart.
        for (boundary, phases, cancel) in [
            ("admitted", vec![], false),
            ("draining", vec![ManagedTransitionPhase::Draining], false),
            (
                "adopting",
                vec![
                    ManagedTransitionPhase::Draining,
                    ManagedTransitionPhase::Adopting,
                ],
                false,
            ),
            (
                "resetting",
                vec![
                    ManagedTransitionPhase::Draining,
                    ManagedTransitionPhase::Adopting,
                    ManagedTransitionPhase::Resetting,
                ],
                false,
            ),
            (
                "succeeded",
                vec![
                    ManagedTransitionPhase::Draining,
                    ManagedTransitionPhase::Adopting,
                    ManagedTransitionPhase::Resetting,
                    ManagedTransitionPhase::Succeeded,
                ],
                false,
            ),
            (
                "quarantined",
                vec![
                    ManagedTransitionPhase::Draining,
                    ManagedTransitionPhase::Quarantined,
                ],
                false,
            ),
            ("cancelled", vec![], true),
        ] {
            let (before_restart, old_process_id) =
                coordinator_from_real_persisted_auth(codex_home.path()).await;
            let transition_id = format!("real-mapper-{boundary}");
            let mut initial = request(old_process_id.clone(), &transition_id);
            initial.expected_auth_fingerprint = Some(expected_fingerprint.clone());
            let admitted = before_restart
                .admit(initial)
                .await
                .unwrap_or_else(|e| panic!("{boundary}: admit under available real auth must succeed: {e:?}"));
            assert_eq!(
                admitted.result_auth_fingerprint.as_deref(),
                Some(expected_fingerprint.as_str()),
                "{boundary}: admission must reconstruct the intended account's own fingerprint"
            );
            if cancel {
                before_restart
                    .cancel(cancel_request(old_process_id.clone(), &transition_id))
                    .await
                    .unwrap();
            } else {
                for phase in phases {
                    before_restart.advance(&transition_id, phase).await.unwrap();
                }
            }
            if boundary == "quarantined" {
                let quarantined_status = before_restart
                    .read(read_request(old_process_id.clone(), &transition_id))
                    .await
                    .unwrap();
                assert!(
                    quarantined_status.retryable,
                    "quarantined is the one terminal phase whose own status must report retryable"
                );
            }

            // Restart: a fresh `AuthManager` re-reads the same, unchanged
            // persisted source and a fresh coordinator is built from it.
            let (restarted, new_process_id) =
                coordinator_from_real_persisted_auth(codex_home.path()).await;
            assert_ne!(
                new_process_id, old_process_id,
                "{boundary}: restart must reconstruct a new process identity"
            );

            // Typed rejection of the old process identity: the restarted
            // coordinator's own internal `read` (not the public wire gate)
            // genuinely compares process identity and refuses the stale one.
            assert_eq!(
                restarted
                    .read(read_request(old_process_id, &transition_id))
                    .await
                    .unwrap_err()
                    .kind,
                ManagedTransitionRefusalKind::ProcessMismatch,
                "{boundary}: old process identity must never resolve a transition after restart"
            );

            // No manufactured old outcome or reservation: the restarted
            // coordinator has no record of the pre-restart transition at all
            // under its own new process identity.
            assert_eq!(
                restarted
                    .read(read_request(new_process_id.clone(), &transition_id))
                    .await
                    .unwrap_err()
                    .kind,
                ManagedTransitionRefusalKind::InvalidRequest,
                "{boundary}: restart must not resurrect or manufacture the pre-restart outcome"
            );

            // Safe current-state retry, on this same restarted process, that
            // completes and acknowledges truthfully with the intended
            // account's own fingerprint -- not merely a fresh `Admitted`
            // re-admission.
            complete_a_fresh_transition(&restarted, &new_process_id, boundary, &expected_fingerprint)
                .await;
        }

        // Initial-load failure: the real mapper must report the coordinator
        // as unavailable, and its own internal gate (not the public wire
        // stub) must refuse with the typed variant before any mutation.
        std::fs::write(codex_home.path().join("auth.json"), "not valid json")
            .expect("write unreadable auth.json");
        let (unavailable, unavailable_process_id) =
            coordinator_from_real_persisted_auth(codex_home.path()).await;
        assert_eq!(
            unavailable
                .admit(request(unavailable_process_id, "unavailable-transition"))
                .await
                .unwrap_err()
                .kind,
            ManagedTransitionRefusalKind::AuthoritativeAuthUnavailable,
            "an unreadable persisted source must refuse admission through the real mapper, not silently log out"
        );

        // Safe retry after repair: restoring the same genuine intended
        // account recovers admission and completes truthfully, proving the
        // source is retried rather than latched into a poisoned state.
        write_chatgpt_auth_for_intended_account(codex_home.path(), REAL_MAPPER_TEST_ACCOUNT_ID);
        let (repaired, repaired_process_id) =
            coordinator_from_real_persisted_auth(codex_home.path()).await;
        complete_a_fresh_transition(&repaired, &repaired_process_id, "repaired", &expected_fingerprint)
            .await;
    }

    #[test]
    fn phase_edge_table_accepts_every_declared_edge_and_rejects_neighbouring_shortcuts() {
        for edge in [
            (
                ManagedTransitionPhase::Admitted,
                ManagedTransitionPhase::Draining,
            ),
            (
                ManagedTransitionPhase::Draining,
                ManagedTransitionPhase::Adopting,
            ),
            (
                ManagedTransitionPhase::Adopting,
                ManagedTransitionPhase::Resetting,
            ),
            (
                ManagedTransitionPhase::Resetting,
                ManagedTransitionPhase::Succeeded,
            ),
            (
                ManagedTransitionPhase::Admitted,
                ManagedTransitionPhase::Quarantined,
            ),
            (
                ManagedTransitionPhase::Draining,
                ManagedTransitionPhase::Quarantined,
            ),
            (
                ManagedTransitionPhase::Adopting,
                ManagedTransitionPhase::Quarantined,
            ),
            (
                ManagedTransitionPhase::Resetting,
                ManagedTransitionPhase::Quarantined,
            ),
        ] {
            assert!(
                legal_phase_edge(edge.0, edge.1),
                "missing legal edge {edge:?}"
            );
        }
        for edge in [
            (
                ManagedTransitionPhase::Admitted,
                ManagedTransitionPhase::Adopting,
            ),
            (
                ManagedTransitionPhase::Draining,
                ManagedTransitionPhase::Succeeded,
            ),
            (
                ManagedTransitionPhase::Succeeded,
                ManagedTransitionPhase::Resetting,
            ),
            (
                ManagedTransitionPhase::Cancelled,
                ManagedTransitionPhase::Draining,
            ),
            (
                ManagedTransitionPhase::Quarantined,
                ManagedTransitionPhase::Admitted,
            ),
        ] {
            assert!(
                !legal_phase_edge(edge.0, edge.1),
                "accepted illegal edge {edge:?}"
            );
        }
    }

    #[tokio::test]
    async fn same_id_conflicts_for_each_immutable_field_preserve_the_active_record() {
        let coordinator = ManagedTransitionCoordinator::new();
        let process_id = coordinator.process_instance_id().await;
        let admitted = coordinator
            .admit(request(process_id.clone(), "transition-a"))
            .await
            .unwrap();

        let mut variants = Vec::new();
        let mut intent = request(process_id.clone(), "transition-a");
        intent.intent = ManagedTransitionIntent::AdoptManagedLogout;
        variants.push((intent, ManagedTransitionRefusalKind::TransitionIdConflict));
        let mut auth_revision = request(process_id.clone(), "transition-a");
        auth_revision.expected_auth_revision = 99;
        variants.push((
            auth_revision,
            ManagedTransitionRefusalKind::TransitionIdConflict,
        ));
        let mut transition_revision = request(process_id.clone(), "transition-a");
        transition_revision.expected_transition_revision = 99;
        variants.push((
            transition_revision,
            ManagedTransitionRefusalKind::TransitionIdConflict,
        ));
        let mut fingerprint = request(process_id.clone(), "transition-a");
        fingerprint.expected_auth_fingerprint = Some("different".to_owned());
        variants.push((
            fingerprint,
            ManagedTransitionRefusalKind::TransitionIdConflict,
        ));
        variants.push((
            request("other-process".to_owned(), "transition-a"),
            ManagedTransitionRefusalKind::ProcessMismatch,
        ));

        for (params, expected) in variants {
            assert_eq!(coordinator.admit(params).await.unwrap_err().kind, expected);
            assert_eq!(
                coordinator
                    .read(read_request(process_id.clone(), "transition-a"))
                    .await
                    .unwrap(),
                admitted
            );
        }
    }

    #[tokio::test]
    async fn every_declared_phase_edge_drives_the_kernel_and_illegal_edge_preserves_status() {
        for (prefix, next) in [
            (vec![], ManagedTransitionPhase::Draining),
            (
                vec![ManagedTransitionPhase::Draining],
                ManagedTransitionPhase::Adopting,
            ),
            (
                vec![
                    ManagedTransitionPhase::Draining,
                    ManagedTransitionPhase::Adopting,
                ],
                ManagedTransitionPhase::Resetting,
            ),
            (
                vec![
                    ManagedTransitionPhase::Draining,
                    ManagedTransitionPhase::Adopting,
                    ManagedTransitionPhase::Resetting,
                ],
                ManagedTransitionPhase::Succeeded,
            ),
            (vec![], ManagedTransitionPhase::Quarantined),
            (
                vec![ManagedTransitionPhase::Draining],
                ManagedTransitionPhase::Quarantined,
            ),
            (
                vec![
                    ManagedTransitionPhase::Draining,
                    ManagedTransitionPhase::Adopting,
                ],
                ManagedTransitionPhase::Quarantined,
            ),
            (
                vec![
                    ManagedTransitionPhase::Draining,
                    ManagedTransitionPhase::Adopting,
                    ManagedTransitionPhase::Resetting,
                ],
                ManagedTransitionPhase::Quarantined,
            ),
        ] {
            let coordinator = ManagedTransitionCoordinator::new();
            let process_id = coordinator.process_instance_id().await;
            coordinator
                .admit(request(process_id.clone(), "transition-a"))
                .await
                .unwrap();
            for phase in prefix {
                coordinator.advance("transition-a", phase).await.unwrap();
            }
            assert_eq!(
                coordinator
                    .advance("transition-a", next)
                    .await
                    .unwrap()
                    .phase,
                next
            );
        }

        let coordinator = ManagedTransitionCoordinator::new();
        let process_id = coordinator.process_instance_id().await;
        let admitted = coordinator
            .admit(request(process_id.clone(), "transition-a"))
            .await
            .unwrap();
        assert_eq!(
            coordinator
                .advance("transition-a", ManagedTransitionPhase::Succeeded)
                .await
                .unwrap_err()
                .kind,
            ManagedTransitionRefusalKind::InvalidRequest
        );
        assert_eq!(
            coordinator
                .read(read_request(process_id, "transition-a"))
                .await
                .unwrap(),
            admitted
        );
    }

    #[tokio::test]
    async fn every_illegal_active_phase_edge_refuses_without_mutating_the_record() {
        let active_phases = [
            ManagedTransitionPhase::Admitted,
            ManagedTransitionPhase::Draining,
            ManagedTransitionPhase::Adopting,
            ManagedTransitionPhase::Resetting,
        ];
        let all_phases = [
            ManagedTransitionPhase::Idle,
            ManagedTransitionPhase::Admitted,
            ManagedTransitionPhase::Draining,
            ManagedTransitionPhase::Adopting,
            ManagedTransitionPhase::Resetting,
            ManagedTransitionPhase::Succeeded,
            ManagedTransitionPhase::Cancelled,
            ManagedTransitionPhase::Quarantined,
        ];

        for from in active_phases {
            let coordinator = ManagedTransitionCoordinator::new();
            let process_id = coordinator.process_instance_id().await;
            coordinator
                .admit(request(process_id.clone(), "transition-a"))
                .await
                .unwrap();
            let prefix: &[ManagedTransitionPhase] = match from {
                ManagedTransitionPhase::Admitted => &[],
                ManagedTransitionPhase::Draining => &[ManagedTransitionPhase::Draining],
                ManagedTransitionPhase::Adopting => &[
                    ManagedTransitionPhase::Draining,
                    ManagedTransitionPhase::Adopting,
                ],
                ManagedTransitionPhase::Resetting => &[
                    ManagedTransitionPhase::Draining,
                    ManagedTransitionPhase::Adopting,
                    ManagedTransitionPhase::Resetting,
                ],
                _ => unreachable!("active phase table excludes terminal states"),
            };
            for phase in prefix {
                coordinator.advance("transition-a", *phase).await.unwrap();
            }
            let before = coordinator
                .read(read_request(process_id.clone(), "transition-a"))
                .await
                .unwrap();
            assert_eq!(before.phase, from);

            for to in all_phases {
                if legal_phase_edge(from, to) {
                    continue;
                }
                assert_eq!(
                    coordinator
                        .advance("transition-a", to)
                        .await
                        .unwrap_err()
                        .kind,
                    ManagedTransitionRefusalKind::InvalidRequest,
                    "{from:?} -> {to:?} must refuse"
                );
                assert_eq!(
                    coordinator
                        .read(read_request(process_id.clone(), "transition-a"))
                        .await
                        .unwrap(),
                    before,
                    "{from:?} -> {to:?} must not mutate the active record"
                );
            }
        }
    }

    #[tokio::test]
    async fn every_wire_gate_refuses_an_unauthorized_caller_without_reserving_or_mutating_a_transition()
     {
        let coordinator = ManagedTransitionCoordinator::new();
        let process_id = coordinator.process_instance_id().await;
        let start = coordinator
            .start_dispatch(
                request(process_id.clone(), "transition-a"),
                /*caller_authorized*/ false,
            )
            .await;
        let read = coordinator
            .read_dispatch(
                read_request(process_id.clone(), "transition-a"),
                /*caller_authorized*/ false,
            )
            .await;
        let cancel = coordinator
            .cancel_dispatch(
                cancel_request(process_id.clone(), "transition-a"),
                /*caller_authorized*/ false,
            )
            .await;
        let refusals = [
            match start {
                StartManagedTransitionResponse::Refused { refusal } => refusal,
                StartManagedTransitionResponse::Accepted { .. } => panic!("start must refuse"),
            },
            match read {
                ReadManagedTransitionResponse::Refused { refusal } => refusal,
                ReadManagedTransitionResponse::Accepted { .. } => panic!("read must refuse"),
            },
            match cancel {
                CancelManagedTransitionResponse::Refused { refusal } => refusal,
                CancelManagedTransitionResponse::Accepted { .. } => panic!("cancel must refuse"),
            },
        ];
        for refusal in refusals {
            assert_eq!(
                refusal.kind,
                ManagedTransitionRefusalKind::AuthorizationNotAdmitted
            );
        }
        assert_eq!(
            coordinator
                .admit(request(process_id, "transition-a"))
                .await
                .unwrap()
                .phase,
            ManagedTransitionPhase::Admitted
        );
    }

    #[tokio::test]
    async fn unavailable_authoritative_auth_never_becomes_a_logged_out_admission() {
        let coordinator = ManagedTransitionCoordinator::from_authoritative_auth_state(
            AuthoritativeAuthState::unavailable(),
        );
        let process_id = coordinator.process_instance_id().await;
        assert_eq!(
            coordinator
                .admit(request(process_id, "transition-a"))
                .await
                .unwrap_err()
                .kind,
            ManagedTransitionRefusalKind::AuthoritativeAuthUnavailable
        );
    }
}
