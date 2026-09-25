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
use codex_login::AuthManager;
use codex_login::auth::ManagedAdoptionInstallOutcome;
use codex_login::auth::ManagedAdoptionPrecondition;
use codex_login::auth::ManagedAdoptionVerificationError;
use codex_login::auth::PreparedManagedAdoption;
#[cfg(test)]
use sha2::Digest;
#[cfg(test)]
use sha2::Sha256;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use uuid::Uuid;

#[cfg(test)]
#[path = "managed_account_projection_tests.rs"]
mod account_projection_tests;

/// The bounded wait for admitted account-dependent work to drain to zero
/// before auth mutation (`CODEX-I05-S03-R012`). Fixed rather than
/// injectable: tests get deterministic control via this codebase's existing
/// `#[tokio::test(start_paused = true)]` plus `tokio::time::advance`
/// convention, not a custom clock trait, so `R012`'s "controllable for
/// deterministic tests" is satisfied without a new abstraction.
pub(crate) const DRAIN_DEADLINE: Duration = Duration::from_secs(30);

/// Bounded, credential-free permit registry for account-dependent work
/// (`CODEX-I05-S03-R009`, `R012`). Holds no auth value and never will --
/// only a barrier flag and an admitted-work count. Deliberately not behind
/// `CoordinatorState`'s `Mutex`: every `Permit`-classified request calls
/// [`Self::try_acquire`] on the dispatch hot path
/// (`crate::account_dependency::classify`), and acquiring the transition
/// state lock there would serialize unrelated ordinary work behind
/// managed-transition bookkeeping it has nothing to do with.
#[derive(Clone)]
pub(crate) struct AccountWorkPermits {
    inner: Arc<AccountWorkPermitsInner>,
}

struct AccountWorkPermitsInner {
    /// Closure and population share one modification order. Separate atomics
    /// would permit an acquirer to see open while the closer sees zero.
    state: AtomicU64,
    /// Shared by permit release and by cancellation of a draining
    /// transition -- both are "something the drain wait should recheck"
    /// events. A spurious wake from the other event class costs only one
    /// extra recheck of the two loop conditions.
    signal: Notify,
    #[cfg(test)]
    after_reopen: std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

const ACCOUNT_WORK_CLOSED: u64 = 1 << 63;
const ACCOUNT_WORK_COUNT_MASK: u64 = ACCOUNT_WORK_CLOSED - 1;

/// Held for the lifetime of one admitted account-dependent request's
/// handling. Releases exactly once, from every terminal path (success,
/// error, or task cancellation/drop), without the holder ever calling a
/// release method (`CODEX-I05-S03-R009`: "release permits exactly once
/// across every terminal path").
pub(crate) struct AccountWorkPermitGuard {
    permits: AccountWorkPermits,
}

impl Drop for AccountWorkPermitGuard {
    fn drop(&mut self) {
        self.permits.inner.state.fetch_sub(1, Ordering::AcqRel);
        self.permits.inner.signal.notify_waiters();
    }
}

impl AccountWorkPermits {
    fn new() -> Self {
        Self {
            inner: Arc::new(AccountWorkPermitsInner {
                state: AtomicU64::new(0),
                signal: Notify::new(),
                #[cfg(test)]
                after_reopen: std::sync::Mutex::new(None),
            }),
        }
    }

    /// The successful CAS either precedes close in this one atomic's
    /// modification order (and is counted), or observes closure and refuses.
    /// Exhaustion also refuses rather than carrying into the closed bit.
    pub(crate) fn try_acquire(&self) -> Option<AccountWorkPermitGuard> {
        self.inner
            .state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                if state & ACCOUNT_WORK_CLOSED != 0 || state == ACCOUNT_WORK_COUNT_MASK {
                    None
                } else {
                    Some(state + 1)
                }
            })
            .ok()?;
        Some(AccountWorkPermitGuard {
            permits: self.clone(),
        })
    }

    fn close(&self) {
        self.inner
            .state
            .fetch_or(ACCOUNT_WORK_CLOSED, Ordering::AcqRel);
    }

    fn reopen(&self) {
        // The coordinator owns the terminal-state invariant authorizing this;
        // cancelling a drain must preserve permits that are still finishing.
        self.inner
            .state
            .fetch_and(ACCOUNT_WORK_COUNT_MASK, Ordering::AcqRel);
        // Observe the actual admission-opening point, not terminal return.
        #[cfg(test)]
        if let Some(observe) = self.inner.after_reopen.lock().unwrap().take() {
            observe();
        }
    }

    pub(crate) fn admitted_count(&self) -> u64 {
        self.inner.state.load(Ordering::Acquire) & ACCOUNT_WORK_COUNT_MASK
    }

    fn wake_waiters(&self) {
        self.inner.signal.notify_waiters();
    }

    /// Per `tokio::sync::Notify`'s documented race-free idiom: the listener
    /// is created before the caller checks any condition, so a release or a
    /// [`Self::wake_waiters`] call that happens between creation and the
    /// caller's `.await` is never lost.
    fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.inner.signal.notified()
    }
}

#[cfg(test)]
#[path = "account_work_permits_tests.rs"]
mod account_work_permits_tests;

#[derive(Clone)]
pub(crate) struct ManagedTransitionCoordinator {
    state: Arc<Mutex<CoordinatorState>>,
    /// Immutable per-coordinator; not behind `state`'s lock since revalidation
    /// only ever queries it, never mutates it.
    target_evidence_source: Arc<dyn TargetEvidenceSource>,
    account_work_permits: AccountWorkPermits,
    /// `None` for every Slice 1-3 construction path (`new`,
    /// `from_authoritative_auth_state[_and_target_evidence_source]`): those
    /// coordinators keep exactly their pre-Slice-4 behavior, closing the
    /// barrier and draining but never auto-continuing into adoption. `Some`
    /// only via
    /// [`Self::with_adoption_and_account_projection`],
    /// the real production path (Issue 05 Slice 4, R014).
    adoption: Option<AdoptionDependencies>,
}

#[derive(Clone)]
struct AdoptionDependencies {
    auth_manager: Arc<AuthManager>,
    reset_inventory: Arc<dyn ResetInventory>,
    outgoing: Option<Arc<crate::outgoing_message::OutgoingMessageSender>>,
}

pub(crate) type ResetInventoryFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), ResetInventoryError>> + Send + 'a>>;

/// Fixed internal inventory causes. Never retain backend text or credentials.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResetInventoryError {
    AccountProjection(crate::outgoing_message::AccountProjectionReservationError),
    Telemetry(crate::otel_reset_control::TelemetryResetError),
    CloudConfigUnavailable,
    ConfigPublicationUnavailable,
    CloudConfigTimedOut,
    ConfigLoadUnavailable,
    ConfigLoadTimedOut,
    ResidencyUnavailable,
    RemoteControlUnavailable,
    ThreadsIncomplete {
        submit_failed: usize,
        timed_out: usize,
    },
    PluginRetirementUnavailable,
    ModelsWorkerUnavailable,
    ModelCatalogTimedOut,
    ModelCatalogUnavailable,
}

#[derive(Clone, Copy, Debug)]
struct TransitionFailure {
    kind: ManagedTransitionRefusalKind,
    reset: Option<ResetInventoryError>,
}

impl From<ManagedTransitionRefusalKind> for TransitionFailure {
    fn from(kind: ManagedTransitionRefusalKind) -> Self {
        Self { kind, reset: None }
    }
}

impl From<ResetInventoryError> for TransitionFailure {
    fn from(reset: ResetInventoryError) -> Self {
        Self {
            kind: ManagedTransitionRefusalKind::ResetFailed,
            reset: Some(reset),
        }
    }
}

/// Injectable, credential-free reset of every account-derived cache/worker
/// after a successful managed-auth adoption (Issue 05 Slice 4, R014). Never
/// receives the adopted auth value -- only "reset now, using whatever
/// `AuthManager` already has installed." Production wiring lives in
/// `message_processor.rs`; disposable tests inject a synthetic
/// implementation to prove the coordinator's phase/barrier sequencing
/// without touching any real subsystem.
pub(crate) trait ResetInventory: Send + Sync {
    fn reset_all(&self) -> ResetInventoryFuture<'_>;
}

/// What `advance_inner` should do to the coordinator's own CAS baseline
/// (`state.auth_revision`/`auth_fingerprint`) alongside a phase transition
/// (Issue 05 Slice 4, R014, R064). `Adopted` and `LoggedOut` are used
/// identically whether the terminal phase is `Succeeded` (ordinary
/// completion) or `Quarantined` (a reset-inventory failure after auth was
/// already installed/logged out) -- in both cases the coordinator's own
/// tracked truth must match what `AuthManager` actually holds.
#[derive(Clone)]
enum AdoptedAuthUpdate {
    Adopted(String),
    LoggedOut,
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
        Ok(
            codex_app_server_transport::PeerExecutableIdentity::FileIdentity {
                device: 0,
                inode: 0,
            },
        )
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
        match auth_manager.authoritative_managed_auth_fingerprint() {
            Ok(auth_fingerprint) => Self {
                authority_available: true,
                auth_revision: 0,
                auth_fingerprint,
            },
            Err(_) => Self::unavailable(),
        }
    }

    #[cfg(test)]
    fn from_account_id(account_id: Option<String>) -> Self {
        let auth_fingerprint = account_id.as_deref().map(account_fingerprint);
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

/// Test fixture adapter for the manager-owned fingerprint derivation.
/// Production coordinator paths receive only fingerprints from AuthManager.
#[cfg(test)]
fn account_fingerprint(account_id: &str) -> String {
    AuthManager::managed_account_fingerprint(account_id)
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
    fn executable_identity(
        &self,
    ) -> std::io::Result<codex_app_server_transport::PeerExecutableIdentity>;
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

    fn executable_identity(
        &self,
    ) -> std::io::Result<codex_app_server_transport::PeerExecutableIdentity> {
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
///
/// Deliberately does not derive `PartialEq`/`Eq`: the only intended
/// comparison is [`Self::matches_target`]. A derived structural equality
/// would obscure that the executable identity is unavailable evidence rather
/// than an ordinary optional field — see `CODEX-I05-S02` verification round
/// 01, M2.
#[derive(Debug, Clone)]
pub(crate) struct TargetEvidence {
    declared_profile: Option<String>,
    executable_identity: Option<codex_app_server_transport::PeerExecutableIdentity>,
    endpoint: String,
    pid: u32,
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
        }
    }

    /// True only when every replacement-sensitive fact this capture observed
    /// is identical to the reference capture. `executable_identity` missing
    /// on *either* side is never treated as a match: an unavailable
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
    pending_reset: Option<PendingResetRecovery>,
}

/// The sole owner of already-applied auth whose reset has not completed.
/// Completed records remain immutable history when ownership transfers.
#[derive(Debug, Clone)]
struct PendingResetRecovery {
    owner_transition_id: String,
    intent: ManagedTransitionIntent,
    auth_fingerprint: Option<String>,
    auth_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TransitionEnvelope {
    transition_id: String,
    process_instance_id: String,
    intent: ManagedTransitionIntent,
    expected_auth_revision: u64,
    expected_transition_revision: u64,
    expected_auth_fingerprint: Option<String>,
    intended_result_auth_fingerprint: Option<String>,
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
    reset_failure: Option<ResetInventoryError>,
    /// True after auth installation until all reset work has completed.
    /// Cancellation may never waive this obligation.
    reset_pending: bool,
    /// Credential-free manager revision captured at admission, never replaced
    /// by a later snapshot after drain or source I/O.
    adoption_precondition: Option<ManagedAdoptionPrecondition>,
    prepared_adoption: Option<Arc<PreparedManagedAdoption>>,
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
        Self::from_authoritative_auth_state_target_evidence_and_process_instance(
            authoritative_auth,
            target_evidence_source,
            Uuid::now_v7().to_string(),
        )
    }

    fn from_authoritative_auth_state_target_evidence_and_process_instance(
        authoritative_auth: AuthoritativeAuthState,
        target_evidence_source: Arc<dyn TargetEvidenceSource>,
        process_instance_id: String,
    ) -> Self {
        let target_evidence = TargetEvidence::capture(target_evidence_source.as_ref());
        Self {
            state: Arc::new(Mutex::new(CoordinatorState {
                process_instance_id,
                auth_revision: authoritative_auth.auth_revision,
                transition_revision: 0,
                auth_fingerprint: authoritative_auth.auth_fingerprint,
                auth_authority_available: authoritative_auth.authority_available,
                target_evidence,
                active: None,
                completed: HashMap::new(),
                pending_reset: None,
            })),
            target_evidence_source,
            account_work_permits: AccountWorkPermits::new(),
            adoption: None,
        }
    }

    /// State-machine fixture without an outgoing consumer. Production must
    /// use the constructor requiring account projection ownership below.
    #[cfg(test)]
    fn from_authoritative_auth_state_target_evidence_and_adoption(
        authoritative_auth: AuthoritativeAuthState,
        target_evidence_source: Arc<dyn TargetEvidenceSource>,
        auth_manager: Arc<AuthManager>,
        reset_inventory: Arc<dyn ResetInventory>,
    ) -> Self {
        let mut coordinator = Self::from_authoritative_auth_state_and_target_evidence_source(
            authoritative_auth,
            target_evidence_source,
        );
        coordinator.adoption = Some(AdoptionDependencies {
            auth_manager,
            reset_inventory,
            outgoing: None,
        });
        coordinator
    }

    /// Production adoption always owns the account notification queue. Only
    /// private state-machine fixtures omit this consumer projection.
    pub(crate) fn with_adoption_and_account_projection(
        authoritative_auth: AuthoritativeAuthState,
        target_evidence_source: Arc<dyn TargetEvidenceSource>,
        auth_manager: Arc<AuthManager>,
        reset_inventory: Arc<dyn ResetInventory>,
        outgoing: Arc<crate::outgoing_message::OutgoingMessageSender>,
    ) -> Self {
        let mut coordinator = Self::from_authoritative_auth_state_and_target_evidence_source(
            authoritative_auth,
            target_evidence_source,
        );
        coordinator.adoption = Some(AdoptionDependencies {
            auth_manager,
            reset_inventory,
            outgoing: Some(outgoing),
        });
        coordinator
    }

    /// Production constructor for a standalone server whose process identity
    /// was selected before its optional immutable target record was published.
    pub(crate) fn with_adoption_account_projection_and_process_instance(
        authoritative_auth: AuthoritativeAuthState,
        target_evidence_source: Arc<dyn TargetEvidenceSource>,
        process_instance_id: String,
        auth_manager: Arc<AuthManager>,
        reset_inventory: Arc<dyn ResetInventory>,
        outgoing: Arc<crate::outgoing_message::OutgoingMessageSender>,
    ) -> Self {
        let mut coordinator =
            Self::from_authoritative_auth_state_target_evidence_and_process_instance(
                authoritative_auth,
                target_evidence_source,
                process_instance_id,
            );
        coordinator.adoption = Some(AdoptionDependencies {
            auth_manager,
            reset_inventory,
            outgoing: Some(outgoing),
        });
        coordinator
    }

    pub(crate) async fn process_instance_id(&self) -> String {
        self.state.lock().await.process_instance_id.clone()
    }

    /// The dispatch-time acquisition point for every `Permit`-classified
    /// request (`crate::account_dependency::classify`,
    /// `CODEX-I05-S03-R009`). Returns `None` when the barrier is closed;
    /// the caller must refuse before any auth/provider effect and must not
    /// queue or retry internally (`R010`).
    pub(crate) fn try_acquire_account_work_permit(&self) -> Option<AccountWorkPermitGuard> {
        self.account_work_permits.try_acquire()
    }

    /// Narrow admission capability for background consumers; does not retain
    /// the coordinator's auth manager or reset inventory.
    pub(crate) fn account_work_permits(&self) -> AccountWorkPermits {
        self.account_work_permits.clone()
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
        let target_evidence_matches = self.target_evidence_still_matches().await;
        let adoption_precondition = {
            let state = self.state.lock().await;
            let recovering = self.validate_admission(&state, &envelope, target_evidence_matches)?;
            if !recovering && let Some(adoption) = &self.adoption {
                Some(
                    adoption
                        .auth_manager
                        .capture_managed_adoption_precondition(
                            envelope.expected_auth_fingerprint.as_deref(),
                        )
                        .map_err(|_| authoritative_auth_unavailable(&state, &envelope))?,
                )
            } else {
                None
            }
        };
        // Source eligibility, intended identity and parsing precede barrier
        // effects. The same opaque candidate later installs after drain;
        // no later read can substitute a different account.
        let prepared_adoption = if let (Some(adoption), Some(precondition)) =
            (&self.adoption, &adoption_precondition)
        {
            match adoption
                .auth_manager
                .prepare_managed_adoption(
                    envelope.intended_result_auth_fingerprint.as_deref(),
                    precondition,
                )
                .await
            {
                Ok(prepared) => Some(Arc::new(prepared)),
                Err(error) => {
                    let state = self.state.lock().await;
                    return Err(match error {
                        ManagedAdoptionVerificationError::CacheUnavailable
                        | ManagedAdoptionVerificationError::CacheChangedConcurrently
                        | ManagedAdoptionVerificationError::ExternalResolutionFailed
                        | ManagedAdoptionVerificationError::BackendIoFailed
                        | ManagedAdoptionVerificationError::ParseFailed
                        | ManagedAdoptionVerificationError::StorageFailed(_) => {
                            authoritative_auth_unavailable(&state, &envelope)
                        }
                        ManagedAdoptionVerificationError::IntendedResultMismatch
                        | ManagedAdoptionVerificationError::RestrictionRejected
                        | ManagedAdoptionVerificationError::MissingStableIdentity
                        | ManagedAdoptionVerificationError::IneligibleAuthMode => refusal(
                            &state,
                            &envelope,
                            ManagedTransitionRefusalKind::InvalidRequest,
                            false,
                        ),
                    });
                }
            }
        } else {
            None
        };
        let target_evidence_matches = self.target_evidence_still_matches().await;
        let mut state = self.state.lock().await;
        let recovering = self.validate_admission(&state, &envelope, target_evidence_matches)?;
        // Keep auth authority locked through the winning state/barrier
        // transaction, not merely through its last validation. Recovery uses
        // the same discipline before transferring the pending reset owner.
        let commit = || {
            state.transition_revision += 1;
            let record = TransitionRecord {
                envelope: envelope.clone(),
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
                reset_failure: None,
                reset_pending: recovering,
                adoption_precondition: adoption_precondition.clone(),
                prepared_adoption,
            };
            if let Some(pending) = &mut state.pending_reset {
                pending.owner_transition_id = record.envelope.transition_id.clone();
            }
            let status = status_for(&record);
            state.active = Some(record);
            self.account_work_permits.close();
            status
        };
        let committed = match &self.adoption {
            Some(adoption) => match &adoption_precondition {
                Some(precondition) => adoption
                    .auth_manager
                    .with_managed_adoption_precondition(precondition, commit),
                None => adoption.auth_manager.with_managed_cached_result(
                    envelope.expected_auth_fingerprint.as_deref(),
                    commit,
                ),
            },
            None => Ok(commit()),
        };
        committed.map_err(|_| authoritative_auth_unavailable(&state, &envelope))
    }

    /// Run before preparation and again at the winning admission commit.
    /// Neither validation pass changes state or reserves the barrier.
    fn validate_admission(
        &self,
        state: &CoordinatorState,
        envelope: &TransitionEnvelope,
        target_evidence_matches: bool,
    ) -> Result<bool, ManagedTransitionRefusal> {
        // A pending reset is the one narrowly-scoped recovery owner that may
        // proceed after terminal authority verification latched the
        // coordinator unavailable. All ordinary admissions remain refused
        // until that exact owner completes.
        if !state.auth_authority_available && state.pending_reset.is_none() {
            return Err(authoritative_auth_unavailable(state, envelope));
        }

        if !target_evidence_matches {
            return Err(refusal(
                state,
                envelope,
                ManagedTransitionRefusalKind::TargetChanged,
                false,
            ));
        }

        if envelope.transition_id.is_empty() || envelope.process_instance_id.is_empty() {
            return Err(refusal(
                state,
                envelope,
                ManagedTransitionRefusalKind::InvalidRequest,
                false,
            ));
        }
        if envelope.process_instance_id != state.process_instance_id {
            return Err(refusal(
                state,
                envelope,
                ManagedTransitionRefusalKind::ProcessMismatch,
                true,
            ));
        }
        if let Some(record) = state.completed.get(&envelope.transition_id) {
            return Err(refusal(
                state,
                envelope,
                if record.envelope == *envelope {
                    ManagedTransitionRefusalKind::CompletedReplay
                } else {
                    ManagedTransitionRefusalKind::TransitionIdConflict
                },
                false,
            ));
        }
        if let Some(record) = &state.active {
            return Err(refusal(
                state,
                envelope,
                if record.envelope.transition_id == envelope.transition_id {
                    ManagedTransitionRefusalKind::TransitionIdConflict
                } else {
                    ManagedTransitionRefusalKind::ConcurrentTransition
                },
                true,
            ));
        }
        let valid_intended_result = match envelope.intent {
            ManagedTransitionIntent::AdoptManagedAuth => envelope
                .intended_result_auth_fingerprint
                .as_ref()
                .is_some_and(|value| !value.is_empty()),
            ManagedTransitionIntent::AdoptManagedLogout => {
                envelope.intended_result_auth_fingerprint.is_none()
            }
        };
        if !valid_intended_result {
            return Err(refusal(
                state,
                envelope,
                ManagedTransitionRefusalKind::InvalidRequest,
                false,
            ));
        }
        let recovering = state.pending_reset.is_some();
        if let Some(pending) = &state.pending_reset
            && (pending.intent != envelope.intent
                || pending.auth_fingerprint != envelope.intended_result_auth_fingerprint
                || pending.auth_fingerprint != state.auth_fingerprint
                || pending.auth_revision != state.auth_revision)
        {
            return Err(refusal(
                state,
                envelope,
                ManagedTransitionRefusalKind::StaleAuthFingerprint,
                true,
            ));
        }
        // Only proof of a previously applied eligible logout permits an
        // otherwise-ineligible logged-out cache to enter reset recovery.
        let logout_recovery =
            recovering && envelope.intent == ManagedTransitionIntent::AdoptManagedLogout;
        if !logout_recovery && let Some(adoption) = &self.adoption {
            match adoption.auth_manager.managed_transition_eligible() {
                Ok(true) => {}
                Ok(false) => {
                    return Err(refusal(
                        state,
                        envelope,
                        ManagedTransitionRefusalKind::InvalidRequest,
                        false,
                    ));
                }
                Err(_) => return Err(authoritative_auth_unavailable(state, envelope)),
            }
        }
        // Pre-install quarantine must still be explicitly cancelled.
        // Post-install records are immutable history; `pending_reset`, not
        // a scan over that history, owns the exclusion during retry chains.
        if state.completed.values().any(|record| {
            record.phase == ManagedTransitionPhase::Quarantined && !record.reset_pending
        }) {
            return Err(refusal(
                state,
                envelope,
                ManagedTransitionRefusalKind::ConcurrentTransition,
                true,
            ));
        }
        if envelope.expected_auth_revision != state.auth_revision {
            return Err(refusal(
                state,
                envelope,
                ManagedTransitionRefusalKind::StaleAuthRevision,
                true,
            ));
        }
        if envelope.expected_transition_revision != state.transition_revision {
            return Err(refusal(
                state,
                envelope,
                ManagedTransitionRefusalKind::StaleTransitionRevision,
                true,
            ));
        }
        if envelope.expected_auth_fingerprint != state.auth_fingerprint {
            return Err(refusal(
                state,
                envelope,
                ManagedTransitionRefusalKind::StaleAuthFingerprint,
                true,
            ));
        }

        Ok(recovering)
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
    ///
    /// **Client-facing latency contract (verification round 01, M1).** A
    /// successful admission does not return immediately: this method also
    /// runs [`Self::close_barrier_and_drain`], so an authorized caller's
    /// `ManagedTransitionStart` response can be held for up to
    /// [`DRAIN_DEADLINE`] before it is sent. The response's `phase` (and,
    /// on timeout, `Quarantined`/`retryable: true`) is the caller's only
    /// signal here; there is currently no distinct "still draining" versus
    /// "hung" indication before that response arrives.
    pub(crate) async fn start_dispatch(
        &self,
        params: StartManagedTransitionParams,
        caller_authorized: bool,
    ) -> StartManagedTransitionResponse {
        if !caller_authorized {
            return StartManagedTransitionResponse::Refused {
                refusal: authorization_not_admitted(params.into()),
            };
        }
        let transition_id = params.transition_id.clone();
        match self.admit(params).await {
            Ok(_admitted) => match self.close_barrier_and_drain(&transition_id).await {
                Ok(status) => StartManagedTransitionResponse::Accepted { status },
                Err(refusal) => StartManagedTransitionResponse::Refused { refusal },
            },
            Err(refusal) => StartManagedTransitionResponse::Refused { refusal },
        }
    }

    /// Awaits zero drain of the process-wide account-work barrier within
    /// [`DRAIN_DEADLINE`] (`CODEX-I05-S03-R009`, `R012`, `R013`). The
    /// barrier itself is already closed by the time this runs -- `admit()`
    /// closes it synchronously in its own critical section (verification
    /// round 02), not this method -- so no observer can ever see an active
    /// Admitted record with the barrier still open. Called once,
    /// immediately after a successful [`Self::admit`], as one continuous
    /// step of `start`'s own wire handling -- the canonical plan describes
    /// barrier close and drain await as part of `start`, not a detached
    /// background task, so a client's `ManagedTransitionStart` response is
    /// not sent until this resolves (bounded to `DRAIN_DEADLINE`).
    ///
    /// A concurrent [`Self::cancel`] of the same transition is a *different*
    /// wire call -- itself never gated by this barrier, since
    /// `crate::account_dependency::classify` treats every
    /// `ManagedTransition*` control request as independent, precisely to
    /// avoid the barrier it closes also blocking the only call that can
    /// reopen it. This method detects that concurrent cancellation by
    /// rechecking the transition's own phase on every wake, not only the
    /// permit count, and reports the transition's now-`Cancelled` status
    /// truthfully instead of fabricating a drain or timeout outcome.
    async fn close_barrier_and_drain(
        &self,
        transition_id: &str,
    ) -> Result<ManagedTransitionStatus, ManagedTransitionRefusal> {
        // `admit()` already closed the barrier synchronously in its own
        // critical section (verification round 02); no separate close call
        // is needed or made here.
        self.advance(transition_id, ManagedTransitionPhase::Draining)
            .await?;

        let deadline = tokio::time::Instant::now() + DRAIN_DEADLINE;
        loop {
            // Race-free: `Notify::notified()` captures its
            // `notify_waiters_calls` baseline at *creation* (before this
            // line ever runs anything else), and `notify_waiters()` always
            // increments that counter even with zero registered listeners.
            // So a release or a concurrent cancel that lands anywhere from
            // here through the `state.lock().await` below -- including
            // before this future is ever polled -- is still correctly
            // observed on its first real poll further down, without
            // needing the deadline. Verification round 01's B1 raised this
            // exact concern and proposed pinning+`enable()`-ing the
            // listener early (`core/src/unified_exec/async_watcher.rs`'s
            // idiom for a *different* hazard); independently verified
            // against `tokio::sync::notify`'s own source and an isolated
            // reproduction before accepting the finding, found the
            // `notify_waiters()`-only case (this file's only usage) does
            // not need it, and the finding was withdrawn as invalid
            // (`notify_one`/`notify_waiters` conflation) rather than fixed.
            let notified = self.account_work_permits.notified();

            let still_draining = {
                let state = self.state.lock().await;
                matches!(
                    &state.active,
                    Some(record)
                        if record.envelope.transition_id == transition_id
                            && record.phase == ManagedTransitionPhase::Draining
                )
            };
            if !still_draining {
                return self.current_transition_status(transition_id).await;
            }
            if self.account_work_permits.admitted_count() == 0 {
                break;
            }

            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                // Deadline elapsed. `advance` sets `retryable: true` for
                // `Quarantined` already; no auth was ever touched and
                // admitted work was never cancelled or killed (R013).
                return self
                    .quarantine(transition_id, ManagedTransitionRefusalKind::DrainTimedOut)
                    .await;
            }
        }

        // Drain completed by permit count reaching zero (not by timeout or a
        // concurrent cancel, both already returned above). Slice 1-3
        // coordinators (`adoption: None`) keep exactly their old behavior:
        // report the still-`Draining` status and go no further. Slice 4's
        // real production coordinator auto-continues through Adopting and
        // Resetting to its terminal outcome (Issue 05 Slice 4, R014).
        match &self.adoption {
            Some(deps) => {
                self.adopt_and_reset(
                    transition_id,
                    deps.auth_manager.as_ref(),
                    deps.reset_inventory.as_ref(),
                )
                .await
            }
            None => self.current_transition_status(transition_id).await,
        }
    }

    /// Drives a successfully-drained transition through Adopting, Resetting,
    /// and its terminal outcome (Succeeded+reopen, atomically, or
    /// Quarantined+closed), per the tokenized-phase contract: `state`'s lock
    /// is taken only to validate and advance the phase (inside [`Self::advance`]
    /// / [`Self::complete_adoption`]); it is never held across the `.await`
    /// that does the actual work (`AuthManager` I/O, the reset inventory).
    /// Called once, only from [`Self::close_barrier_and_drain`]'s
    /// drain-completed-by-count path.
    async fn adopt_and_reset(
        &self,
        transition_id: &str,
        auth_manager: &AuthManager,
        reset_inventory: &dyn ResetInventory,
    ) -> Result<ManagedTransitionStatus, ManagedTransitionRefusal> {
        let recovery = {
            let state = self.state.lock().await;
            state
                .pending_reset
                .as_ref()
                .filter(|pending| pending.owner_transition_id == transition_id)
                .cloned()
        };
        if let Some(pending) = recovery {
            return self
                .resume_pending_reset(transition_id, pending, auth_manager, reset_inventory)
                .await;
        }
        // Draining -> Adopting. A concurrent cancel that already took the
        // transition out of `active` (legal while still Draining) surfaces
        // here as this call's own `Err`, propagated by `?` -- the same
        // "no longer the active transition" refusal every other `advance`
        // caller already gets from this race. The status carries this
        // transition's own declared intent (Slice 1's admission already
        // fixed it), needed below to distinguish adopt-a-new-account from
        // deliberate managed logout (R001, R002).
        self.advance(transition_id, ManagedTransitionPhase::Adopting)
            .await?;
        let (intended_fingerprint, precondition, prepared) = {
            let state = self.state.lock().await;
            match state
                .active
                .as_ref()
                .filter(|record| record.envelope.transition_id == transition_id)
            {
                Some(record) => (
                    record.envelope.intended_result_auth_fingerprint.clone(),
                    record.adoption_precondition.clone(),
                    record.prepared_adoption.clone(),
                ),
                None => (None, None, None),
            }
        };
        let (Some(precondition), Some(prepared)) = (precondition, prepared) else {
            return self
                .quarantine(
                    transition_id,
                    ManagedTransitionRefusalKind::AuthInstallFailed,
                )
                .await;
        };

        // Draining may have taken arbitrarily long. Admission's target proof
        // cannot authorize installation after the selected target changed.
        if !self.target_evidence_still_matches().await {
            return self
                .quarantine(transition_id, ManagedTransitionRefusalKind::TargetChanged)
                .await;
        }

        // Installation consumes the already-classified candidate. This is
        // outside the coordinator mutex because raw source verification can
        // perform I/O, but it never re-parses/replaces the prepared object.
        let installed_fingerprint =
            match auth_manager.install_prepared_managed_adoption(&prepared, &precondition) {
                ManagedAdoptionInstallOutcome::Installed { fingerprint } => fingerprint,
                ManagedAdoptionInstallOutcome::LoggedOut => {
                    return self
                        .reset_and_complete_logout(transition_id, auth_manager, reset_inventory)
                        .await;
                }
                outcome => {
                    let kind = match outcome {
                        ManagedAdoptionInstallOutcome::IntendedResultMismatch => {
                            ManagedTransitionRefusalKind::IntendedResultMismatch
                        }
                        ManagedAdoptionInstallOutcome::CurrentAuthIneligible => {
                            ManagedTransitionRefusalKind::InvalidRequest
                        }
                        ManagedAdoptionInstallOutcome::CacheLockUnavailable
                        | ManagedAdoptionInstallOutcome::SourceReadFailed(_) => {
                            ManagedTransitionRefusalKind::AuthoritativeAuthUnavailable
                        }
                        ManagedAdoptionInstallOutcome::SourceChanged => {
                            ManagedTransitionRefusalKind::AuthSourceChanged
                        }
                        ManagedAdoptionInstallOutcome::CacheChangedConcurrently => {
                            ManagedTransitionRefusalKind::StaleAuthRevision
                        }
                        ManagedAdoptionInstallOutcome::Installed { .. }
                        | ManagedAdoptionInstallOutcome::LoggedOut => {
                            unreachable!("successful installation handled above")
                        }
                    };
                    return self.quarantine(transition_id, kind).await;
                }
            };

        // Adopting -> Resetting.
        self.advance(transition_id, ManagedTransitionPhase::Resetting)
            .await?;

        // No lock held across this await either. A failed reset leaves
        // account-derived caches/workers in an unknown state, so this fails
        // closed to `Quarantined` rather than reporting `Succeeded` over
        // known-stale state (R014). Unlike every other quarantine path
        // above, auth has *already* been installed here -- `quarantine`
        // would leave the coordinator's own CAS baseline at the prior
        // account while `AuthManager` holds the new one (R064's "auth,
        // revision, reset, acknowledgement, and quarantine remain mutually
        // consistent"), so this uses the account-aware quarantine instead.
        if let Err(failure) = self
            .reset_and_verify(
                reset_inventory,
                auth_manager,
                intended_fingerprint.as_deref(),
            )
            .await
        {
            return self
                .quarantine_after_install(transition_id, installed_fingerprint, failure)
                .await;
        }

        // Resetting -> Succeeded, atomically with the new CAS baseline and
        // the barrier reopen (see `complete_adoption`).
        self.complete_adoption(transition_id, installed_fingerprint)
            .await
    }

    /// Fresh-ID recovery never installs auth a second time. Both asynchronous
    /// verification passes run outside the coordinator mutex; the immutable
    /// pending owner prevents cancellation or another admission in between.
    async fn resume_pending_reset(
        &self,
        transition_id: &str,
        pending: PendingResetRecovery,
        auth_manager: &AuthManager,
        reset_inventory: &dyn ResetInventory,
    ) -> Result<ManagedTransitionStatus, ManagedTransitionRefusal> {
        self.advance(transition_id, ManagedTransitionPhase::Adopting)
            .await?;
        if self.account_work_permits.admitted_count() != 0 {
            return self
                .quarantine(transition_id, ManagedTransitionRefusalKind::ResetFailed)
                .await;
        }
        if let Err(failure) = self
            .verify_applied_result(auth_manager, pending.auth_fingerprint.as_deref())
            .await
        {
            return self.quarantine(transition_id, failure).await;
        }
        self.advance(transition_id, ManagedTransitionPhase::Resetting)
            .await?;
        if let Err(failure) = self
            .reset_and_verify(
                reset_inventory,
                auth_manager,
                pending.auth_fingerprint.as_deref(),
            )
            .await
        {
            return self.quarantine(transition_id, failure).await;
        }
        // No auth update: the original application already advanced it.
        self.advance(transition_id, ManagedTransitionPhase::Succeeded)
            .await
    }

    /// The logout counterpart of the Resetting/`complete_adoption` tail
    /// above: auth is already logged out (installed as `None`), so this
    /// still runs the full reset inventory and completes to `Succeeded`
    /// with an empty CAS baseline -- reset failure here gets the same
    /// account-aware (here, account-absent) quarantine treatment.
    async fn reset_and_complete_logout(
        &self,
        transition_id: &str,
        auth_manager: &AuthManager,
        reset_inventory: &dyn ResetInventory,
    ) -> Result<ManagedTransitionStatus, ManagedTransitionRefusal> {
        self.advance(transition_id, ManagedTransitionPhase::Resetting)
            .await?;
        if let Err(failure) = self
            .reset_and_verify(reset_inventory, auth_manager, None)
            .await
        {
            return self.quarantine_after_logout(transition_id, failure).await;
        }
        self.complete_adoption_logout(transition_id).await
    }

    async fn quarantine(
        &self,
        transition_id: &str,
        failure: impl Into<TransitionFailure>,
    ) -> Result<ManagedTransitionStatus, ManagedTransitionRefusal> {
        self.advance_inner(
            transition_id,
            ManagedTransitionPhase::Quarantined,
            None,
            Some(failure.into()),
        )
        .await
    }

    async fn reset_and_verify(
        &self,
        inventory: &dyn ResetInventory,
        auth: &AuthManager,
        fingerprint: Option<&str>,
    ) -> Result<(), TransitionFailure> {
        inventory
            .reset_all()
            .await
            .map_err(TransitionFailure::from)?;
        self.verify_applied_result(auth, fingerprint).await
    }

    async fn verify_applied_result(
        &self,
        auth: &AuthManager,
        fingerprint: Option<&str>,
    ) -> Result<(), TransitionFailure> {
        self.prepare_verified_terminal_result(auth, fingerprint)
            .await
            .map(|_| ())
    }

    async fn prepare_verified_terminal_result(
        &self,
        auth: &AuthManager,
        fingerprint: Option<&str>,
    ) -> Result<codex_login::auth::ManagedTerminalPrecondition, TransitionFailure> {
        if !self.target_evidence_still_matches().await {
            return Err(ManagedTransitionRefusalKind::TargetChanged.into());
        }
        auth.prepare_managed_terminal_commit(fingerprint)
            .await
            .map_err(|error| {
                use codex_login::auth::ManagedAdoptionVerificationError as Error;
                let kind = match error {
                    Error::CacheChangedConcurrently => {
                        ManagedTransitionRefusalKind::StaleAuthRevision
                    }
                    Error::IntendedResultMismatch => {
                        ManagedTransitionRefusalKind::AuthSourceChanged
                    }
                    Error::RestrictionRejected
                    | Error::MissingStableIdentity
                    | Error::IneligibleAuthMode => ManagedTransitionRefusalKind::InvalidRequest,
                    Error::CacheUnavailable
                    | Error::ExternalResolutionFailed
                    | Error::BackendIoFailed
                    | Error::ParseFailed
                    | Error::StorageFailed(_) => {
                        ManagedTransitionRefusalKind::AuthoritativeAuthUnavailable
                    }
                };
                kind.into()
            })
    }

    /// Defensive re-read used only by [`Self::close_barrier_and_drain`]'s
    /// two return points, where the transition is already known to exist
    /// (this coordinator admitted it moments earlier in the same call
    /// chain); the `InvalidRequest` arm should be unreachable in practice.
    async fn current_transition_status(
        &self,
        transition_id: &str,
    ) -> Result<ManagedTransitionStatus, ManagedTransitionRefusal> {
        let state = self.state.lock().await;
        if let Some(record) = state
            .active
            .as_ref()
            .filter(|record| record.envelope.transition_id == transition_id)
        {
            return Ok(status_for(record));
        }
        if let Some(record) = state.completed.get(transition_id) {
            return Ok(status_for(record));
        }
        Err(refusal_for_transition_id(
            &state,
            transition_id,
            ManagedTransitionRefusalKind::InvalidRequest,
            false,
        ))
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
        // The repository-owned writer-first command uses an empty transition
        // id to obtain the current CAS baseline before it has an id to read.
        // A live transition, pending reset, or pre-install quarantine must be observed as a
        // refusal rather than exposing a baseline that would let the writer
        // run while the barrier already has an owner.
        if envelope.transition_id.is_empty() {
            if state.active.is_some()
                || state.pending_reset.is_some()
                || state.completed.values().any(|record| {
                    record.phase == ManagedTransitionPhase::Quarantined && !record.reset_pending
                })
            {
                return Err(refusal(
                    &state,
                    &envelope,
                    ManagedTransitionRefusalKind::ConcurrentTransition,
                    true,
                ));
            }
            return Ok(current_status(&state));
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
            return ReadManagedTransitionResponse::Refused {
                refusal: authorization_not_admitted(TransitionEnvelope::read(params)),
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
        if envelope.process_instance_id != state.process_instance_id {
            return Err(refusal(
                &state,
                &envelope,
                ManagedTransitionRefusalKind::ProcessMismatch,
                true,
            ));
        }

        // `Quarantined` is terminal (`ManagedTransitionPhase::is_terminal`),
        // so a drain-timeout record already lives in `completed`, never in
        // `active`. R013/R016/R017 require quarantine to stay observable
        // and closed to new account-dependent work while it remains, but
        // still support an explicit, safe, attributable cancel of *this
        // exact* transition -- verification round 01's delegated B2 remedy:
        // do not reopen automatically at timeout; only an explicit cancel
        // of the quarantined transition itself reopens the barrier.
        if let Some(record) = state.completed.get(&envelope.transition_id).cloned()
            && record.phase == ManagedTransitionPhase::Quarantined
        {
            if record.reset_pending {
                return Err(refusal(
                    &state,
                    &envelope,
                    ManagedTransitionRefusalKind::LateCancellation,
                    state.auth_authority_available,
                ));
            }
            state.transition_revision += 1;
            let cancelled = TransitionRecord {
                refusal: None,
                reset_failure: None,
                phase: ManagedTransitionPhase::Cancelled,
                prepared_adoption: None,
                retryable: true,
                result_transition_revision: state.transition_revision,
                ..record
            };
            let status = status_for(&cancelled);
            state
                .completed
                .insert(cancelled.envelope.transition_id.clone(), cancelled);
            // Defensive, not load-bearing under normal operation: `admit()`'s
            // own quarantine-conflict refusal (added alongside this fix)
            // already prevents any other transition from becoming active or
            // Quarantined while this one remains unresolved, so this should
            // always evaluate true here. Checked *and acted upon* while
            // still holding `state`'s lock -- not checked under it and then
            // acted on after releasing -- so `admit()` (which needs this
            // same lock) can never admit a new transition in the window
            // between this decision and the barrier actually reopening
            // (verification round 02's TOCTOU correction on top of round
            // 02's own single-owner fix). `reopen`/`wake_waiters` are
            // synchronous, so this holds no lock across an `.await`.
            let no_other_owner = state.active.is_none()
                && state.pending_reset.is_none()
                && !state
                    .completed
                    .values()
                    .any(|other| other.phase == ManagedTransitionPhase::Quarantined);
            if no_other_owner {
                // An authority failure before credential installation may be
                // explicitly cancelled. This is the only recovery from the
                // sticky authority latch; post-install quarantines remain
                // reset_pending and are refused above.
                state.auth_authority_available = true;
                self.account_work_permits.reopen();
                self.account_work_permits.wake_waiters();
            }
            drop(state);
            return Ok(status);
        }

        if !state.auth_authority_available && state.pending_reset.is_none() {
            return Err(authoritative_auth_unavailable(&state, &envelope));
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
        // Admitted and Draining are both pre-mutation phases (`R012`: auth
        // mutation cannot start until zero drain completes); `R017`
        // requires cancellation to work anywhere before that boundary, not
        // only from Admitted. Adopting/Resetting are post-mutation and stay
        // refused as `LateCancellation`, governed by R064 in a later slice.
        if record.reset_pending
            || !matches!(
                record.phase,
                ManagedTransitionPhase::Admitted | ManagedTransitionPhase::Draining
            )
        {
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
            refusal: None,
            reset_failure: None,
            phase: ManagedTransitionPhase::Cancelled,
            prepared_adoption: None,
            retryable: true,
            result_transition_revision: state.transition_revision,
            ..record
        };
        let status = status_for(&cancelled);
        state
            .completed
            .insert(cancelled.envelope.transition_id.clone(), cancelled);
        // The reset_pending guard above also excludes recovery attempts:
        // they co-own the closed barrier with PendingResetRecovery and must
        // never reopen it by cancellation after auth was already installed.
        // Unconditional, not gated on `was_draining` (verification round
        // 03): `admit()` closes the barrier itself as soon as a transition
        // reaches `Admitted` -- it is the sole closer, since
        // `close_barrier_and_drain` no longer closes it separately -- so by
        // the time execution reaches here, the phase guard above has
        // already ensured `record.phase` was `Admitted` or `Draining`,
        // either of which means this cancellation's transition held the
        // barrier closed. A `was_draining`-only gate left an
        // Admitted-phase cancellation with no reopen path at all,
        // permanently closing the barrier with no owner. Reopen so
        // ordinary account-dependent work resumes immediately rather than
        // staying refused for the rest of the process's lifetime over an
        // attempt nothing will ever retry-complete; wake any in-flight
        // `close_barrier_and_drain` so it observes this cancellation
        // instead of running to its own timeout.
        //
        // Called while still holding `state`'s lock, immediately after
        // recording `Cancelled` -- not after releasing it -- for the same
        // reason as the quarantine-cancel branch above: `admit()` needs
        // this same lock, so a new transition can never be admitted in the
        // window between this cancellation taking effect and the barrier
        // actually reopening (verification round 02's TOCTOU correction,
        // preserved here). Synchronous calls; holds no lock across an
        // `.await`.
        self.account_work_permits.reopen();
        self.account_work_permits.wake_waiters();
        drop(state);
        Ok(status)
    }

    /// See [`Self::start_dispatch`] for the `caller_authorized` contract.
    pub(crate) async fn cancel_dispatch(
        &self,
        params: CancelManagedTransitionParams,
        caller_authorized: bool,
    ) -> CancelManagedTransitionResponse {
        if !caller_authorized {
            return CancelManagedTransitionResponse::Refused {
                refusal: authorization_not_admitted(TransitionEnvelope::cancel(params)),
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
        let failure = (next_phase == ManagedTransitionPhase::Quarantined)
            .then(|| ManagedTransitionRefusalKind::AuthInstallFailed.into());
        self.advance_inner(transition_id, next_phase, None, failure)
            .await
    }

    /// Atomically finalizes a successful managed-auth adoption
    /// (Resetting -> Succeeded): records the newly-adopted account's
    /// fingerprint/revision as both this record's result and the
    /// coordinator's new CAS baseline, and reopens the account-work
    /// barrier -- all inside the one `state` critical section this shares
    /// with every other phase transition (Issue 05 Slice 4, R014). Never
    /// receives credential material or raw account identity: the manager
    /// derives `installed_fingerprint` from the exact installed object.
    pub(crate) async fn complete_adoption(
        &self,
        transition_id: &str,
        installed_fingerprint: String,
    ) -> Result<ManagedTransitionStatus, ManagedTransitionRefusal> {
        self.advance_inner(
            transition_id,
            ManagedTransitionPhase::Succeeded,
            Some(AdoptedAuthUpdate::Adopted(installed_fingerprint)),
            None,
        )
        .await
    }

    /// The deliberate-managed-logout counterpart of [`Self::complete_adoption`]
    /// (R001, R002): records an empty CAS baseline (no account) as both
    /// this record's result and the coordinator's new baseline, and
    /// reopens the barrier -- same one critical section, same reopen
    /// coupling.
    async fn complete_adoption_logout(
        &self,
        transition_id: &str,
    ) -> Result<ManagedTransitionStatus, ManagedTransitionRefusal> {
        self.advance_inner(
            transition_id,
            ManagedTransitionPhase::Succeeded,
            Some(AdoptedAuthUpdate::LoggedOut),
            None,
        )
        .await
    }

    /// Quarantine after auth has *already* been installed to
    /// the account represented by `installed_fingerprint` (a reset-inventory failure following a
    /// successful adopt) -- unlike plain [`Self::quarantine`], this also
    /// updates the coordinator's own CAS baseline to match what
    /// `AuthManager` actually holds. Without this, the coordinator would
    /// report Quarantined while still believing the prior account is
    /// current, so a later exact-quarantine cancel (which unconditionally
    /// reopens once no other owner remains) would let a fresh transition
    /// admit against a stale pre-adoption CAS baseline while the real
    /// installed auth had already moved -- the R064 "auth, revision,
    /// reset, acknowledgement, and quarantine remain mutually consistent"
    /// violation this closes. The barrier still does not reopen (this is
    /// `Quarantined`, not `Succeeded`); only the CAS baseline changes.
    async fn quarantine_after_install(
        &self,
        transition_id: &str,
        installed_fingerprint: String,
        failure: TransitionFailure,
    ) -> Result<ManagedTransitionStatus, ManagedTransitionRefusal> {
        self.advance_inner(
            transition_id,
            ManagedTransitionPhase::Quarantined,
            Some(AdoptedAuthUpdate::Adopted(installed_fingerprint)),
            Some(failure),
        )
        .await
    }

    /// The deliberate-managed-logout counterpart of
    /// [`Self::quarantine_after_install`]: a reset-inventory failure after
    /// auth was already logged out (installed as absent) still updates the
    /// CAS baseline to empty, for the identical reason.
    async fn quarantine_after_logout(
        &self,
        transition_id: &str,
        failure: TransitionFailure,
    ) -> Result<ManagedTransitionStatus, ManagedTransitionRefusal> {
        self.advance_inner(
            transition_id,
            ManagedTransitionPhase::Quarantined,
            Some(AdoptedAuthUpdate::LoggedOut),
            Some(failure),
        )
        .await
    }

    async fn advance_inner(
        &self,
        transition_id: &str,
        mut next_phase: ManagedTransitionPhase,
        auth_update: Option<AdoptedAuthUpdate>,
        mut failure: Option<TransitionFailure>,
    ) -> Result<ManagedTransitionStatus, ManagedTransitionRefusal> {
        // Capacity waits precede every terminal lock. Revalidate source and
        // target after that wait; the final cache fence still spans publication.
        let mut projection = None;
        let mut terminal_precondition = None;
        if next_phase == ManagedTransitionPhase::Succeeded
            && let Some(adoption) = &self.adoption
            && let Some(outgoing) = &adoption.outgoing
        {
            match outgoing
                .reserve_account_projection_until(
                    tokio::time::Instant::now() + Duration::from_secs(5),
                )
                .await
            {
                Ok(reserved) => {
                    let expected = match &auth_update {
                        Some(AdoptedAuthUpdate::Adopted(fingerprint)) => Some(fingerprint.clone()),
                        Some(AdoptedAuthUpdate::LoggedOut) => None,
                        None => self
                            .state
                            .lock()
                            .await
                            .pending_reset
                            .as_ref()
                            .and_then(|pending| pending.auth_fingerprint.clone()),
                    };
                    match self
                        .prepare_verified_terminal_result(
                            &adoption.auth_manager,
                            expected.as_deref(),
                        )
                        .await
                    {
                        Ok(precondition) => {
                            projection = Some(reserved);
                            terminal_precondition = Some(precondition);
                        }
                        Err(error) => {
                            next_phase = ManagedTransitionPhase::Quarantined;
                            failure = Some(error);
                        }
                    }
                }
                Err(error) => {
                    next_phase = ManagedTransitionPhase::Quarantined;
                    failure = Some(ResetInventoryError::AccountProjection(error).into());
                }
            }
        }
        let mut state = self.state.lock().await;
        if (next_phase == ManagedTransitionPhase::Quarantined) != failure.is_some() {
            return Err(refusal_for_transition_id(
                &state,
                transition_id,
                ManagedTransitionRefusalKind::InvalidRequest,
                false,
            ));
        }
        if !state.auth_authority_available && state.pending_reset.is_none() {
            return Err(refusal_for_transition_id(
                &state,
                transition_id,
                ManagedTransitionRefusalKind::AuthoritativeAuthUnavailable,
                state.auth_authority_available,
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

        if record.reset_pending
            && let Some(pending) = &state.pending_reset
            && (pending.owner_transition_id != transition_id
                || pending.auth_revision != state.auth_revision
                || pending.auth_fingerprint != state.auth_fingerprint)
        {
            state.active = Some(record);
            return Err(refusal_for_transition_id(
                &state,
                transition_id,
                ManagedTransitionRefusalKind::StaleAuthRevision,
                true,
            ));
        }
        // The manager retains its auth locks through the terminal mutation
        // and barrier reopen. A check returning a bool would leave a second
        // check-act window, even though the coordinator mutex is still held.
        if next_phase.is_terminal()
            && (auth_update.is_some()
                || record.reset_pending
                || record.adoption_precondition.is_some())
            && let Some(adoption) = &self.adoption
        {
            let expected = match &auth_update {
                Some(AdoptedAuthUpdate::Adopted(fingerprint)) => Some(fingerprint.clone()),
                Some(AdoptedAuthUpdate::LoggedOut) => None,
                None => match &state.pending_reset {
                    Some(pending) => pending.auth_fingerprint.clone(),
                    None => record.envelope.expected_auth_fingerprint.clone(),
                },
            };
            let commit = |auth_mode: Option<codex_protocol::auth::AuthMode>, plan_type| {
                self.commit_phase_locked(
                    &mut state,
                    record.clone(),
                    next_phase,
                    auth_update.clone(),
                    failure,
                    projection.take().map(|reserved| {
                        (
                            reserved,
                            codex_app_server_protocol::AccountUpdatedNotification {
                                auth_mode: auth_mode.map(crate::auth_mode::auth_mode_to_api),
                                plan_type,
                            },
                        )
                    }),
                )
            };
            use codex_login::auth::ManagedTerminalVerificationError as TerminalError;
            let result = match terminal_precondition.as_ref() {
                Some(precondition) => adoption
                    .auth_manager
                    .with_managed_terminal_projection(precondition, commit),
                None => adoption
                    .auth_manager
                    .with_managed_account_projection(expected.as_deref(), commit)
                    .map_err(TerminalError::Authority),
            };
            return match result {
                Ok(status) => Ok(status),
                Err(TerminalError::SourceChanged) => Ok(self.commit_phase_locked(
                    &mut state,
                    record,
                    ManagedTransitionPhase::Quarantined,
                    auth_update,
                    Some(ManagedTransitionRefusalKind::AuthSourceChanged.into()),
                    None,
                )),
                Err(TerminalError::SourceUnavailable(_)) => Ok(self.commit_phase_locked(
                    &mut state,
                    record,
                    ManagedTransitionPhase::Quarantined,
                    auth_update,
                    Some(ManagedTransitionRefusalKind::AuthoritativeAuthUnavailable.into()),
                    None,
                )),
                Err(TerminalError::Authority(error)) => {
                    // The callback did not run. Preserve installed-result
                    // history but never report it as current authoritative
                    // truth when another auth owner changed or obscured it.
                    let kind = match error {
                        ManagedAdoptionVerificationError::IntendedResultMismatch
                        | ManagedAdoptionVerificationError::CacheChangedConcurrently => {
                            ManagedTransitionRefusalKind::StaleAuthRevision
                        }
                        _ => ManagedTransitionRefusalKind::AuthoritativeAuthUnavailable,
                    };
                    self.commit_phase_locked(
                        &mut state,
                        record,
                        ManagedTransitionPhase::Quarantined,
                        auth_update,
                        Some(kind.into()),
                        None,
                    );
                    state.auth_authority_available = false;
                    Err(refusal_for_transition_id(
                        &state,
                        transition_id,
                        ManagedTransitionRefusalKind::AuthoritativeAuthUnavailable,
                        state.auth_authority_available,
                    ))
                }
            };
        }
        Ok(self.commit_phase_locked(&mut state, record, next_phase, auth_update, failure, None))
    }

    /// Synchronous coordinator commit. Callers already own its mutex; a
    /// successful post-install terminal commit also holds manager auth locks.
    /// No manager call or asynchronous work belongs in this critical section.
    fn commit_phase_locked(
        &self,
        state: &mut CoordinatorState,
        record: TransitionRecord,
        next_phase: ManagedTransitionPhase,
        auth_update: Option<AdoptedAuthUpdate>,
        failure: Option<TransitionFailure>,
        projection: Option<(
            crate::outgoing_message::AccountProjectionReservation,
            codex_app_server_protocol::AccountUpdatedNotification,
        )>,
    ) -> ManagedTransitionStatus {
        debug_assert_eq!(
            next_phase == ManagedTransitionPhase::Quarantined,
            failure.is_some()
        );
        state.transition_revision += 1;
        let applied_update = auth_update.is_some();
        if let Some(update) = auth_update {
            state.auth_revision += 1;
            state.auth_fingerprint = match update {
                AdoptedAuthUpdate::Adopted(fingerprint) => Some(fingerprint),
                AdoptedAuthUpdate::LoggedOut => None,
            };
        }
        if next_phase == ManagedTransitionPhase::Quarantined && applied_update {
            state.pending_reset = Some(PendingResetRecovery {
                owner_transition_id: record.envelope.transition_id.clone(),
                intent: record.envelope.intent,
                auth_fingerprint: state.auth_fingerprint.clone(),
                auth_revision: state.auth_revision,
            });
        }
        let advanced = TransitionRecord {
            refusal: failure.map(|failure| failure.kind),
            reset_failure: failure.and_then(|failure| failure.reset),
            phase: next_phase,
            prepared_adoption: if next_phase.is_terminal() {
                None
            } else {
                record.prepared_adoption.clone()
            },
            reset_pending: next_phase != ManagedTransitionPhase::Succeeded
                && (record.reset_pending || next_phase == ManagedTransitionPhase::Resetting),
            retryable: next_phase == ManagedTransitionPhase::Quarantined,
            result_transition_revision: state.transition_revision,
            result_auth_revision: state.auth_revision,
            result_auth_fingerprint: state.auth_fingerprint.clone(),
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
        // Success retires the active/pending-reset barrier owners only after
        // the terminal proof and queue admission. Installed-auth quarantine
        // cannot be cancelled open: a fresh transition must take the pending
        // reset, revalidate it, and finish it. The old quarantined record stays
        // immutable history. Pre-install cancellation has its own guarded
        // reopen path; it cannot discharge an installed pending reset.
        let recovering_authority_latch = next_phase == ManagedTransitionPhase::Succeeded
            && state.pending_reset.is_some()
            && !state.auth_authority_available;
        if next_phase == ManagedTransitionPhase::Succeeded {
            state.pending_reset = None;
            if recovering_authority_latch {
                // A successful exact pending-reset recovery proves the
                // authority that was unavailable at quarantine is usable
                // again. Ordinary success cannot reach this branch with the
                // latch cleared because admission remains refused.
                state.auth_authority_available = true;
            }
            if let Some((reserved, notification)) = projection {
                reserved.publish(notification);
            }
            self.account_work_permits.reopen();
            self.account_work_permits.wake_waiters();
        }
        status
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
            intended_result_auth_fingerprint: params.intended_result_auth_fingerprint,
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
            intended_result_auth_fingerprint: None,
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
            intended_result_auth_fingerprint: None,
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

/// Generic, non-disclosing refusal for a caller that has not qualified for
/// managed-transition authorization. Retains only `transition_id` from the
/// caller's own envelope -- a client needing to correlate multiple
/// concurrent start attempts by id genuinely needs it echoed back. Every
/// other field is a fixed, non-live, schema-preserving placeholder rather
/// than the coordinator's real state: `process_instance_id` is always
/// blanked -- unlike `transition_id`, it was never derived from the
/// caller's own request even before this fix, only from `CoordinatorState`,
/// which is exactly the disclosure this fix stops;
/// `auth_revision`/`transition_revision`/`auth_fingerprint` are
/// coordinator-derived and must never appear here at all, so an unauthorized
/// caller cannot poll this refusal as a transition/auth oracle
/// (`CODEX-I05-S02-R005`). Deliberately takes no `&CoordinatorState`, so the
/// dispatch trio no longer needs to acquire the state lock on this path at
/// all.
fn authorization_not_admitted(envelope: TransitionEnvelope) -> ManagedTransitionRefusal {
    ManagedTransitionRefusal {
        kind: ManagedTransitionRefusalKind::AuthorizationNotAdmitted,
        retryable: false,
        process_instance_id: String::new(),
        transition_id: envelope.transition_id,
        auth_revision: 0,
        transition_revision: 0,
        auth_fingerprint: None,
    }
}

fn authoritative_auth_unavailable(
    state: &CoordinatorState,
    envelope: &TransitionEnvelope,
) -> ManagedTransitionRefusal {
    refusal(
        state,
        envelope,
        ManagedTransitionRefusalKind::AuthoritativeAuthUnavailable,
        state.auth_authority_available,
    )
}

fn current_status(state: &CoordinatorState) -> ManagedTransitionStatus {
    ManagedTransitionStatus {
        process_instance_id: state.process_instance_id.clone(),
        transition_id: None,
        intent: None,
        phase: ManagedTransitionPhase::Idle,
        retryable: false,
        prior_auth_revision: state.auth_revision,
        auth_revision: state.auth_revision,
        prior_transition_revision: state.transition_revision,
        transition_revision: state.transition_revision,
        prior_auth_fingerprint: state.auth_fingerprint.clone(),
        result_auth_fingerprint: state.auth_fingerprint.clone(),
        refusal: None,
    }
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
    use codex_app_server_protocol::MANAGED_AUTH_TRANSITION_CONTRACT_VERSION;

    pub(super) fn request(
        process_instance_id: String,
        transition_id: &str,
    ) -> StartManagedTransitionParams {
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
            intended_result_auth_fingerprint: Some(account_fingerprint("account-b")),
        }
    }

    #[tokio::test]
    async fn intended_result_is_required_and_part_of_transition_identity() {
        let coordinator = ManagedTransitionCoordinator::new();
        let process = coordinator.process_instance_id().await;
        let mut missing = request(process.clone(), "intent-missing");
        missing.intended_result_auth_fingerprint = None;
        assert_eq!(
            coordinator.admit(missing).await.unwrap_err().kind,
            ManagedTransitionRefusalKind::InvalidRequest
        );
        assert!(coordinator.try_acquire_account_work_permit().is_some());

        let mut start = request(process, "intent-bound");
        coordinator.admit(start.clone()).await.unwrap();
        start.intended_result_auth_fingerprint = Some(account_fingerprint("account-c"));
        assert_eq!(
            coordinator.admit(start).await.unwrap_err().kind,
            ManagedTransitionRefusalKind::TransitionIdConflict
        );
    }

    #[tokio::test]
    async fn unintended_account_and_logout_with_present_auth_never_install_or_reset() {
        for intent in [
            ManagedTransitionIntent::AdoptManagedAuth,
            ManagedTransitionIntent::AdoptManagedLogout,
        ] {
            let home = tempfile::TempDir::new().unwrap();
            write_chatgpt_auth_for_intended_account(home.path(), "account-a");
            let auth = Arc::new(real_auth_manager(home.path()).await);
            let reset = Arc::new(RecordingResetInventory::new(false));
            let coordinator = ManagedTransitionCoordinator::from_authoritative_auth_state_target_evidence_and_adoption(
                AuthoritativeAuthState::from_auth_manager(&auth), Arc::new(UnsetTargetEvidenceSource),
                Arc::clone(&auth), Arc::clone(&reset) as Arc<dyn ResetInventory>,
            );
            let mut start = request(coordinator.process_instance_id().await, "intended-b");
            start.expected_auth_fingerprint = Some(account_fingerprint("account-a"));
            start.intent = intent;
            start.intended_result_auth_fingerprint = match intent {
                ManagedTransitionIntent::AdoptManagedAuth => Some(account_fingerprint("account-b")),
                ManagedTransitionIntent::AdoptManagedLogout => None,
            };
            write_chatgpt_auth_for_intended_account(home.path(), "account-c");
            let StartManagedTransitionResponse::Refused { refusal } =
                coordinator.start_dispatch(start, true).await
            else {
                panic!("unintended source must refuse before admission");
            };
            assert_eq!(refusal.kind, ManagedTransitionRefusalKind::InvalidRequest);
            assert_eq!(
                auth.authoritative_auth_cached()
                    .unwrap()
                    .unwrap()
                    .get_account_id()
                    .as_deref(),
                Some("account-a")
            );
            assert!(!reset.was_called());
            assert!(coordinator.try_acquire_account_work_permit().is_some());
            assert!(coordinator.state.lock().await.active.is_none());
            assert_eq!(coordinator.state.lock().await.transition_revision, 0);
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
    async fn empty_read_returns_current_cas_baseline_only_when_unowned() {
        let coordinator = ManagedTransitionCoordinator::new();
        let process_id = coordinator.process_instance_id().await;
        let current = coordinator
            .read(ReadManagedTransitionParams {
                contract_version: MANAGED_AUTH_TRANSITION_CONTRACT_VERSION,
                transition_id: String::new(),
                process_instance_id: process_id.clone(),
            })
            .await
            .unwrap();
        assert_eq!(current.phase, ManagedTransitionPhase::Idle);
        assert_eq!(current.transition_id, None);
        assert_eq!(current.auth_revision, 0);
        assert_eq!(current.transition_revision, 0);

        coordinator
            .admit(request(process_id.clone(), "transition-a"))
            .await
            .unwrap();
        assert_eq!(
            coordinator
                .read(ReadManagedTransitionParams {
                    contract_version: MANAGED_AUTH_TRANSITION_CONTRACT_VERSION,
                    transition_id: String::new(),
                    process_instance_id: process_id.clone(),
                })
                .await
                .unwrap_err()
                .kind,
            ManagedTransitionRefusalKind::ConcurrentTransition
        );

        coordinator
            .cancel(cancel_request(process_id.clone(), "transition-a"))
            .await
            .unwrap();
        let after_cancel = coordinator
            .read(ReadManagedTransitionParams {
                contract_version: MANAGED_AUTH_TRANSITION_CONTRACT_VERSION,
                transition_id: String::new(),
                process_instance_id: process_id.clone(),
            })
            .await
            .unwrap();
        assert_eq!(after_cancel.phase, ManagedTransitionPhase::Idle);
        assert_eq!(after_cancel.transition_revision, 2);
    }

    #[tokio::test]
    async fn empty_read_refuses_pending_reset_owner_without_active_record() {
        let coordinator = ManagedTransitionCoordinator::new();
        let process_id = coordinator.process_instance_id().await;
        coordinator.state.lock().await.pending_reset = Some(PendingResetRecovery {
            owner_transition_id: "pending-reset".to_owned(),
            intent: ManagedTransitionIntent::AdoptManagedAuth,
            auth_fingerprint: None,
            auth_revision: 0,
        });

        let refusal = coordinator
            .read(ReadManagedTransitionParams {
                contract_version: MANAGED_AUTH_TRANSITION_CONTRACT_VERSION,
                transition_id: String::new(),
                process_instance_id: process_id,
            })
            .await
            .unwrap_err();
        assert_eq!(
            refusal.kind,
            ManagedTransitionRefusalKind::ConcurrentTransition
        );
    }

    #[tokio::test]
    async fn pending_reset_admission_can_cross_a_latched_authority_only_for_matching_recovery() {
        let coordinator = ManagedTransitionCoordinator::new();
        let process_id = coordinator.process_instance_id().await;
        let current_fingerprint = account_fingerprint("account-b");
        {
            let mut state = coordinator.state.lock().await;
            state.auth_authority_available = false;
            state.auth_fingerprint = Some(current_fingerprint.clone());
            state.pending_reset = Some(PendingResetRecovery {
                owner_transition_id: "original".to_owned(),
                intent: ManagedTransitionIntent::AdoptManagedAuth,
                auth_fingerprint: Some(current_fingerprint.clone()),
                auth_revision: 0,
            });
        }

        let mut recovery = request(process_id, "recovery");
        recovery.expected_auth_fingerprint = Some(current_fingerprint);
        assert_eq!(
            coordinator.admit(recovery).await.unwrap().phase,
            ManagedTransitionPhase::Admitted
        );
        assert!(coordinator.try_acquire_account_work_permit().is_none());
    }

    #[tokio::test]
    async fn authority_quarantine_can_be_explicitly_cancelled_before_install() {
        let coordinator = ManagedTransitionCoordinator::new();
        let process_id = coordinator.process_instance_id().await;
        coordinator
            .admit(request(process_id.clone(), "authority-failure"))
            .await
            .unwrap();
        let quarantined = coordinator
            .advance("authority-failure", ManagedTransitionPhase::Quarantined)
            .await
            .unwrap();
        assert_eq!(quarantined.phase, ManagedTransitionPhase::Quarantined);
        coordinator.state.lock().await.auth_authority_available = false;

        let refused = coordinator
            .read(read_request(process_id.clone(), "authority-failure"))
            .await
            .unwrap_err();
        assert!(!refused.retryable);
        let cancelled = coordinator
            .cancel(cancel_request(process_id.clone(), "authority-failure"))
            .await
            .unwrap();
        assert_eq!(cancelled.phase, ManagedTransitionPhase::Cancelled);
        assert!(
            coordinator.state.lock().await.auth_authority_available,
            "explicit cancellation restores admission after pre-install authority failure"
        );
        assert!(coordinator.try_acquire_account_work_permit().is_some());
    }

    #[tokio::test]
    async fn cancelling_a_still_admitted_transition_reopens_the_barrier() {
        // Verification round 03: `admit()` closes the barrier as soon as a
        // transition reaches `Admitted` (round 02's TOCTOU fix), not only
        // once it reaches `Draining`. The cancellation reopen was still
        // gated on `was_draining`, so cancelling a transition that was
        // cancelled *before* ever advancing past `Admitted` left the
        // barrier permanently closed with no remaining owner and no
        // reopen path. This is the discriminating case that gate missed.
        let coordinator = ManagedTransitionCoordinator::new();
        let process_id = coordinator.process_instance_id().await;
        let admitted = coordinator
            .admit(request(process_id.clone(), "transition-a"))
            .await
            .unwrap();
        assert_eq!(admitted.phase, ManagedTransitionPhase::Admitted);
        assert!(
            coordinator.try_acquire_account_work_permit().is_none(),
            "admit() must close the barrier even before Draining"
        );

        coordinator
            .cancel(cancel_request(process_id, "transition-a"))
            .await
            .expect("cancelling a still-Admitted transition must succeed");

        assert!(
            coordinator.try_acquire_account_work_permit().is_some(),
            "cancelling an Admitted (not yet Draining) transition must \
             still reopen the barrier"
        );
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
    async fn unauthorized_wire_admission_returns_complete_placeholder_without_mutation() {
        let coordinator =
            ManagedTransitionCoordinator::from_authoritative_auth_state(AuthoritativeAuthState {
                authority_available: true,
                auth_revision: 41,
                auth_fingerprint: Some(account_fingerprint("account-a")),
            });
        coordinator.state.lock().await.transition_revision = 7;
        let process_id = coordinator.process_instance_id().await;
        let mut request = request_at_revision(process_id.clone(), "transition-a", 7);
        request.expected_auth_revision = 41;
        request.expected_auth_fingerprint = Some(account_fingerprint("account-a"));
        let refused = coordinator
            .start_dispatch(request.clone(), /*caller_authorized*/ false)
            .await;
        assert_eq!(
            refused,
            StartManagedTransitionResponse::Refused {
                refusal: ManagedTransitionRefusal {
                    kind: ManagedTransitionRefusalKind::AuthorizationNotAdmitted,
                    retryable: false,
                    process_instance_id: String::new(),
                    transition_id: "transition-a".to_owned(),
                    auth_revision: 0,
                    transition_revision: 0,
                    auth_fingerprint: None,
                },
            }
        );

        let admitted = coordinator
            .admit(request)
            .await
            .expect("the refused wire request must not reserve the transition id");
        assert_eq!(admitted.phase, ManagedTransitionPhase::Admitted);
        assert_eq!(admitted.prior_auth_revision, 41);
        assert_eq!(admitted.prior_transition_revision, 7);
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
        // Slice 3: `start_dispatch` now closes the barrier and awaits zero
        // drain as one continuous step of `start`'s own handling (plan
        // Slice 3), so a successful response reports the post-drain phase.
        // With zero admitted account-dependent work outstanding, the drain
        // completes immediately and the transition is already `Draining`
        // (not `Admitted`) by the time this response is observed.
        assert_eq!(status.phase, ManagedTransitionPhase::Draining);
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
            self.facts
                .lock()
                .expect("synthetic facts lock")
                .declared_profile
                .clone()
        }

        fn executable_identity(
            &self,
        ) -> std::io::Result<codex_app_server_transport::PeerExecutableIdentity> {
            Ok(
                codex_app_server_transport::PeerExecutableIdentity::FileIdentity {
                    device: 1,
                    inode: self
                        .facts
                        .lock()
                        .expect("synthetic facts lock")
                        .executable_identity_inode,
                },
            )
        }

        fn endpoint(&self) -> String {
            self.facts
                .lock()
                .expect("synthetic facts lock")
                .endpoint
                .clone()
        }

        fn pid(&self) -> u32 {
            self.facts.lock().expect("synthetic facts lock").pid
        }
    }

    #[tokio::test]
    async fn admit_refuses_when_target_evidence_no_longer_matches_the_captured_reference() {
        let source = SyntheticTargetEvidenceSource::new(SyntheticTargetFacts::baseline());
        let coordinator =
            ManagedTransitionCoordinator::from_authoritative_auth_state_and_target_evidence_source(
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
            .admit(request(process_id.clone(), "transition-a"))
            .await
            .unwrap_err();
        assert_eq!(refused.kind, ManagedTransitionRefusalKind::TargetChanged);

        // Exact no-effect snapshot: the refused attempt must not have
        // reserved the transition id or consumed a transition-revision
        // slot. Restoring the original evidence and re-admitting the same
        // transition id proves both -- a reservation would surface as
        // `TransitionIdConflict`, and a consumed revision would surface as
        // `prior_transition_revision != 0`.
        source.replace_facts(SyntheticTargetFacts::baseline());
        let admitted = coordinator
            .admit(request(process_id, "transition-a"))
            .await
            .expect("the refused attempt must not have reserved the transition id");
        assert_eq!(admitted.phase, ManagedTransitionPhase::Admitted);
        assert_eq!(admitted.prior_transition_revision, 0);
        assert_eq!(admitted.transition_revision, 1);
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
                .admit(request(process_id.clone(), "transition-a"))
                .await
                .unwrap_err();
            assert_eq!(refused.kind, ManagedTransitionRefusalKind::TargetChanged);

            // Exact no-effect snapshot, same rationale as the
            // executable-identity-replacement case above.
            source.replace_facts(SyntheticTargetFacts::baseline());
            let admitted = coordinator
                .admit(request(process_id, "transition-a"))
                .await
                .expect("the refused attempt must not have reserved the transition id");
            assert_eq!(admitted.phase, ManagedTransitionPhase::Admitted);
            assert_eq!(admitted.prior_transition_revision, 0);
            assert_eq!(admitted.transition_revision, 1);
        }
    }

    #[tokio::test]
    async fn admit_succeeds_when_target_evidence_is_re_derived_identically() {
        let source = SyntheticTargetEvidenceSource::new(SyntheticTargetFacts::baseline());
        let coordinator =
            ManagedTransitionCoordinator::from_authoritative_auth_state_and_target_evidence_source(
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
    async fn cancel_is_legal_through_draining_and_late_only_once_adopting_begins() {
        // Admitted and Draining are both pre-mutation (R012); R017 requires
        // cancellation to work through both, not only Admitted -- unlike
        // the pre-Slice-3 assumption this test used to encode.
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
        let cancelled = coordinator
            .cancel(cancel_request(process_id.clone(), "transition-a"))
            .await
            .expect("cancellation during Draining is pre-mutation and must succeed");
        assert_eq!(cancelled.phase, ManagedTransitionPhase::Cancelled);
        assert!(cancelled.retryable);

        // Adopting is past the mutation boundary; late cancellation there
        // is refused and the active record is preserved untouched.
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
        coordinator
            .advance("transition-a", ManagedTransitionPhase::Adopting)
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
            ManagedTransitionPhase::Adopting
        );
    }

    #[tokio::test]
    async fn cancelling_a_draining_transition_reopens_the_barrier_for_new_account_work() {
        let coordinator = ManagedTransitionCoordinator::new();
        let process_id = coordinator.process_instance_id().await;
        // `start_dispatch` (not `admit`) is what actually closes the
        // barrier, as one continuous step of `start`'s own handling; with
        // zero permits outstanding the drain completes immediately and the
        // transition is already Draining by the time this returns.
        let accepted = coordinator
            .start_dispatch(
                request(process_id.clone(), "transition-a"),
                /*caller_authorized*/ true,
            )
            .await;
        let StartManagedTransitionResponse::Accepted { status } = accepted else {
            panic!("admission must succeed with zero outstanding permits");
        };
        assert_eq!(status.phase, ManagedTransitionPhase::Draining);
        assert!(
            coordinator.try_acquire_account_work_permit().is_none(),
            "the barrier must be closed while Draining"
        );

        coordinator
            .cancel(cancel_request(process_id, "transition-a"))
            .await
            .expect("cancellation during Draining must succeed");

        assert!(
            coordinator.try_acquire_account_work_permit().is_some(),
            "cancelling a Draining transition must reopen the barrier"
        );
    }

    #[tokio::test]
    async fn admit_closes_the_barrier_synchronously_before_returning() {
        // Verification round 02's TOCTOU correction, admission-side half:
        // `admit()` itself closes the barrier, inside the same
        // `CoordinatorState` critical section that installs the active
        // record, before releasing the lock -- not `close_barrier_and_drain`
        // afterward as a separate step. Calling `admit()` directly (not
        // `start_dispatch`, which would already have run the drain by the
        // time it returns and so could not distinguish the two) and
        // checking the barrier immediately on return is what proves this:
        // pre-fix, a bare `admit()` call left the barrier open until
        // `close_barrier_and_drain` ran afterward, so this exact assertion
        // would have failed against that code.
        let coordinator = ManagedTransitionCoordinator::new();
        let process_id = coordinator.process_instance_id().await;
        let admitted = coordinator
            .admit(request(process_id, "transition-a"))
            .await
            .unwrap();
        assert_eq!(admitted.phase, ManagedTransitionPhase::Admitted);
        assert!(
            coordinator.try_acquire_account_work_permit().is_none(),
            "admit() must close the barrier itself, before this method \
             returns -- not rely on a later, separate close call"
        );
    }

    #[tokio::test]
    async fn cancellation_reopens_the_barrier_before_a_later_transition_can_admit() {
        // Verification round 02's TOCTOU correction, cancellation-side
        // half, proved sequentially and deterministically: `cancel()`
        // reopens the barrier (and, in the quarantine branch, checks for
        // any other owner) while still holding `state`'s lock, before
        // returning. Because both admission's `close()` and cancellation's
        // `reopen()` now execute only while that same `MutexGuard` is held,
        // and Rust's `Mutex` guarantees mutual exclusion over it, no other
        // call that needs the same lock -- including a later `admit()` --
        // can ever run between one owner's state mutation and its matching
        // barrier mutation. That guarantee is the actual concurrency proof;
        // this test corroborates its observable, sequential consequence: by
        // the time `cancel()` returns, the barrier is already reopened, and
        // a subsequent `admit()` for a new transition succeeds immediately,
        // with no intervening step required to "catch up".
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

        let cancelled = coordinator
            .cancel(cancel_request(process_id.clone(), "transition-a"))
            .await
            .expect("cancelling the Draining transition must succeed");
        assert!(
            coordinator.try_acquire_account_work_permit().is_some(),
            "the barrier must already be reopened by the time cancel() returns"
        );

        let next = coordinator
            .admit(request_at_revision(
                process_id,
                "transition-b",
                cancelled.transition_revision,
            ))
            .await
            .expect("a fresh transition must admit immediately after the prior one's cancel");
        assert_eq!(next.phase, ManagedTransitionPhase::Admitted);
    }

    #[tokio::test]
    async fn a_closed_barrier_refuses_new_account_work_across_repeated_attempts() {
        // R030: the barrier refuses every new-turn attempt, not just the
        // first one, and does so without reserving/mutating anything.
        let coordinator = ManagedTransitionCoordinator::new();
        let process_id = coordinator.process_instance_id().await;
        let accepted = coordinator
            .start_dispatch(
                request(process_id, "transition-a"),
                /*caller_authorized*/ true,
            )
            .await;
        assert!(matches!(
            accepted,
            StartManagedTransitionResponse::Accepted { .. }
        ));

        for _ in 0..3 {
            assert!(
                coordinator.try_acquire_account_work_permit().is_none(),
                "every attempt while Draining must be refused, not only the first"
            );
        }
    }

    #[tokio::test]
    async fn admitted_work_permit_delays_the_drain_until_released_then_it_completes() {
        // R031: pre-barrier admitted work retains its permit and the drain
        // does not report success while it is outstanding; once released,
        // the drain completes and the transition reaches Draining.
        let coordinator = ManagedTransitionCoordinator::new();
        let process_id = coordinator.process_instance_id().await;
        let guard = coordinator
            .try_acquire_account_work_permit()
            .expect("the barrier starts open");

        let start = coordinator.start_dispatch(request(process_id, "transition-a"), true);
        let release_after_yield = async {
            // Let the drain loop observe the nonzero count and register its
            // notification listener before the permit is released, so this
            // exercises the real wait-then-wake path rather than a race
            // that happens to resolve before the drain even starts waiting.
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
            drop(guard);
        };
        let (response, ()) = tokio::join!(start, release_after_yield);
        let StartManagedTransitionResponse::Accepted { status } = response else {
            panic!("admission must eventually succeed once the held permit releases");
        };
        assert_eq!(status.phase, ManagedTransitionPhase::Draining);
    }

    #[tokio::test(start_paused = true)]
    async fn drain_timeout_quarantines_without_touching_auth_or_killing_admitted_work() {
        // R013/R034: a permit that is never released forces the deadline;
        // the outcome is Quarantined/retryable, not a refusal, and no auth
        // field changes from its pre-admission value.
        let coordinator = ManagedTransitionCoordinator::new();
        let process_id = coordinator.process_instance_id().await;
        let auth_before = coordinator.state.lock().await.auth_fingerprint.clone();
        let auth_revision_before = coordinator.state.lock().await.auth_revision;

        let held_guard = coordinator
            .try_acquire_account_work_permit()
            .expect("the barrier starts open");

        let coordinator_for_task = coordinator.clone();
        let handle = tokio::spawn(async move {
            coordinator_for_task
                .start_dispatch(request(process_id, "transition-a"), true)
                .await
        });
        tokio::time::advance(DRAIN_DEADLINE + Duration::from_millis(1)).await;

        let response = handle.await.expect("start_dispatch task");
        let StartManagedTransitionResponse::Accepted { status } = response else {
            panic!("a timed-out drain is a status transition, not a refusal");
        };
        assert_eq!(
            status.refusal,
            Some(ManagedTransitionRefusalKind::DrainTimedOut)
        );
        assert_eq!(status.phase, ManagedTransitionPhase::Quarantined);
        assert!(status.retryable);

        let state = coordinator.state.lock().await;
        assert_eq!(state.auth_fingerprint, auth_before);
        assert_eq!(state.auth_revision, auth_revision_before);
        drop(state);

        // The held permit itself was never force-released; it is still
        // valid and only ends when its own holder drops it, proving the
        // timeout did not kill admitted work (R013).
        drop(held_guard);

        // Verification round 01, B2: a real drain timeout must not
        // auto-reopen the barrier. Quarantine stays observable and closed
        // to new account-dependent work until this exact transition is
        // explicitly cancelled (R016).
        assert!(
            coordinator.try_acquire_account_work_permit().is_none(),
            "a real drain timeout must leave the barrier closed, not reopen it"
        );
    }

    #[tokio::test]
    async fn cancelling_during_drain_wins_the_race_against_the_timeout() {
        // Proves close_barrier_and_drain's per-wake recheck: a cancel that
        // lands while draining must be observed and reported truthfully
        // instead of the drain loop reporting a fabricated outcome.
        let coordinator = ManagedTransitionCoordinator::new();
        let process_id = coordinator.process_instance_id().await;
        let guard = coordinator
            .try_acquire_account_work_permit()
            .expect("the barrier starts open");

        let start = coordinator.start_dispatch(request(process_id.clone(), "transition-a"), true);
        let cancel_after_yield = async {
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
            coordinator
                .cancel(cancel_request(process_id, "transition-a"))
                .await
        };
        let (response, cancel_result) = tokio::join!(start, cancel_after_yield);
        cancel_result.expect("cancellation during Draining must succeed");

        let StartManagedTransitionResponse::Accepted { status } = response else {
            panic!("close_barrier_and_drain reports status via Accepted even when cancelled");
        };
        assert_eq!(status.phase, ManagedTransitionPhase::Cancelled);
        drop(guard);
    }

    #[tokio::test(start_paused = true)]
    async fn an_explicit_cancel_of_the_exact_quarantined_transition_reopens_and_a_fresh_start_revalidates_normally()
     {
        // Verification round 01, B2's delegated remedy end to end: real
        // deadline -> observable Quarantined that refuses new work -> an
        // explicit cancel of that exact transition -> Cancelled/retryable
        // -> barrier reopens -> a fresh Start passes all normal validation
        // (CAS, target evidence, auth availability) rather than inheriting
        // any authority from the quarantined attempt. Timeout itself must
        // not auto-reopen (checked in
        // `drain_timeout_quarantines_without_touching_auth_or_killing_admitted_work`);
        // this test is the recovery half.
        let coordinator = ManagedTransitionCoordinator::new();
        let process_id = coordinator.process_instance_id().await;

        let held_guard = coordinator
            .try_acquire_account_work_permit()
            .expect("the barrier starts open");
        let coordinator_for_task = coordinator.clone();
        let handle = tokio::spawn(async move {
            coordinator_for_task
                .start_dispatch(request(process_id.clone(), "transition-a"), true)
                .await
        });
        tokio::time::advance(DRAIN_DEADLINE + Duration::from_millis(1)).await;
        let response = handle.await.expect("start_dispatch task");
        let StartManagedTransitionResponse::Accepted { status } = response else {
            panic!("a timed-out drain is a status transition, not a refusal");
        };
        assert_eq!(status.phase, ManagedTransitionPhase::Quarantined);
        drop(held_guard);
        assert!(
            coordinator.try_acquire_account_work_permit().is_none(),
            "quarantine must refuse new account-dependent work while it remains"
        );

        // A different transition id must still be refused as LateCancellation
        // (this is not merely "any cancel reopens").
        let process_id = coordinator.process_instance_id().await;
        let wrong_id_refusal = coordinator
            .cancel(cancel_request(process_id.clone(), "transition-b"))
            .await
            .unwrap_err();
        assert_eq!(
            wrong_id_refusal.kind,
            ManagedTransitionRefusalKind::LateCancellation
        );
        assert!(
            coordinator.try_acquire_account_work_permit().is_none(),
            "an unrelated cancel attempt must not reopen the barrier"
        );

        // Cancelling the exact quarantined transition succeeds, reopens the
        // barrier, and produces an attributable retryable Cancelled result.
        let cancelled = coordinator
            .cancel(cancel_request(process_id.clone(), "transition-a"))
            .await
            .expect("cancelling the exact quarantined transition must succeed");
        assert_eq!(cancelled.phase, ManagedTransitionPhase::Cancelled);
        assert!(cancelled.retryable);
        assert!(
            coordinator.try_acquire_account_work_permit().is_some(),
            "the explicit cancel must reopen the barrier"
        );

        // A fresh Start for a new transition id passes all normal
        // validation and is admitted -- it does not inherit authority from
        // the quarantined-then-cancelled attempt. Uses the cancel
        // response's own `transition_revision` as the CAS expectation,
        // exactly as a real client would after reading current status --
        // this is normal revalidation, not special-cased leniency.
        let fresh = coordinator
            .start_dispatch(
                request_at_revision(process_id, "transition-c", cancelled.transition_revision),
                true,
            )
            .await;
        let StartManagedTransitionResponse::Accepted { status } = fresh else {
            panic!("a fresh Start after explicit recovery must be admitted normally");
        };
        assert_eq!(status.phase, ManagedTransitionPhase::Draining);
    }

    #[tokio::test(start_paused = true)]
    async fn a_second_transition_is_refused_while_the_first_remains_quarantined_then_admits_after_explicit_cancel()
     {
        // Verification round 02: a completed Quarantined record still
        // effectively owns the single transition/barrier slot. Before this
        // fix, admit() only ever checked `completed` for the *same*
        // transition id, so an unrelated transition B could become active
        // and Draining while A was still Quarantined -- and an explicit
        // cancel of A would then unconditionally reopen the barrier out
        // from under B's own in-progress drain, admitting account work
        // during B. This proves the discriminating sequence: A quarantines
        // -> B is refused while A remains Quarantined -> cancelling A
        // reopens -> a fresh B then admits normally.
        let coordinator = ManagedTransitionCoordinator::new();
        let process_id = coordinator.process_instance_id().await;

        // Transition A quarantines via a real drain timeout.
        let held_guard = coordinator
            .try_acquire_account_work_permit()
            .expect("the barrier starts open");
        let coordinator_for_task = coordinator.clone();
        let handle = tokio::spawn(async move {
            coordinator_for_task
                .start_dispatch(request(process_id.clone(), "transition-a"), true)
                .await
        });
        tokio::time::advance(DRAIN_DEADLINE + Duration::from_millis(1)).await;
        let response = handle.await.expect("start_dispatch task");
        let StartManagedTransitionResponse::Accepted { status: a_status } = response else {
            panic!("a timed-out drain is a status transition, not a refusal");
        };
        assert_eq!(a_status.phase, ManagedTransitionPhase::Quarantined);
        drop(held_guard);

        // A fresh, otherwise-valid transition B must be refused while A
        // remains quarantined, even though nothing is `active` and B's own
        // CAS/target-evidence would otherwise pass.
        let process_id = coordinator.process_instance_id().await;
        let b_refused = coordinator
            .start_dispatch(
                request_at_revision(
                    process_id.clone(),
                    "transition-b",
                    a_status.transition_revision,
                ),
                true,
            )
            .await;
        let StartManagedTransitionResponse::Refused { refusal } = b_refused else {
            panic!("B must be refused while A is still Quarantined");
        };
        assert_eq!(
            refusal.kind,
            ManagedTransitionRefusalKind::ConcurrentTransition
        );
        assert!(
            coordinator.try_acquire_account_work_permit().is_none(),
            "the barrier must still be closed after B's refused attempt"
        );

        // Cancelling A resolves it and reopens the barrier.
        let cancelled = coordinator
            .cancel(cancel_request(process_id.clone(), "transition-a"))
            .await
            .expect("cancelling the exact quarantined transition must succeed");
        assert_eq!(cancelled.phase, ManagedTransitionPhase::Cancelled);

        // A fresh B now admits normally, once A's quarantine is genuinely
        // resolved rather than superseded.
        let b_admitted = coordinator
            .start_dispatch(
                request_at_revision(process_id, "transition-b", cancelled.transition_revision),
                true,
            )
            .await;
        let StartManagedTransitionResponse::Accepted { status } = b_admitted else {
            panic!("B must admit normally once A's quarantine is explicitly resolved");
        };
        assert_eq!(status.phase, ManagedTransitionPhase::Draining);
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
    async fn coordinator_from_real_persisted_auth(
        codex_home: &std::path::Path,
    ) -> (ManagedTransitionCoordinator, String) {
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
        let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"test-signature");
        let raw_jwt = format!("{header}.{payload}.{signature}");
        codex_login::token_data::parse_chatgpt_jwt_claims(&raw_jwt)
            .expect("minimal JWT must parse as valid ID token claims")
    }

    /// Writes a genuine ChatGPT-mode persisted auth record naming a real
    /// account id, so the production mapper reconstructs the durable
    /// intended-account fingerprint rather than a null one (`CODEX-I05-S01-R04`
    /// Round 08's third required correction).
    pub(super) fn write_chatgpt_auth_for_intended_account(
        codex_home: &std::path::Path,
        account_id: &str,
    ) {
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
            bedrock_access_keys: None,
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
            let admitted = before_restart.admit(initial).await.unwrap_or_else(|e| {
                panic!("{boundary}: admit under available real auth must succeed: {e:?}")
            });
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
                    .read(read_request(old_process_id.clone(), &transition_id))
                    .await
                    .unwrap_err()
                    .kind,
                ManagedTransitionRefusalKind::ProcessMismatch,
                "{boundary}: old process identity must never resolve a transition after restart"
            );

            assert_eq!(
                restarted
                    .cancel(cancel_request(old_process_id, &transition_id))
                    .await
                    .unwrap_err()
                    .kind,
                ManagedTransitionRefusalKind::ProcessMismatch,
                "{boundary}: old process identity must never cancel a transition after restart"
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
            complete_a_fresh_transition(
                &restarted,
                &new_process_id,
                boundary,
                &expected_fingerprint,
            )
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
        complete_a_fresh_transition(
            &repaired,
            &repaired_process_id,
            "repaired",
            &expected_fingerprint,
        )
        .await;
    }

    /// A synthetic [`ResetInventory`] that records whether it ran and can be
    /// told to fail, without touching any real subsystem (Issue 05 Slice 4).
    struct RecordingResetInventory {
        called: AtomicBool,
        should_fail: bool,
    }

    impl RecordingResetInventory {
        fn new(should_fail: bool) -> Self {
            Self {
                called: AtomicBool::new(false),
                should_fail,
            }
        }

        fn was_called(&self) -> bool {
            self.called.load(Ordering::Acquire)
        }
    }

    impl ResetInventory for RecordingResetInventory {
        fn reset_all(&self) -> ResetInventoryFuture<'_> {
            Box::pin(async move {
                self.called.store(true, Ordering::Release);
                if self.should_fail {
                    Err(ResetInventoryError::ModelCatalogUnavailable)
                } else {
                    Ok(())
                }
            })
        }
    }

    pub(super) async fn real_auth_manager(
        codex_home: &std::path::Path,
    ) -> codex_login::AuthManager {
        codex_login::AuthManager::new(
            codex_home.to_path_buf(),
            /*enable_codex_api_key_env*/ false,
            codex_config::types::AuthCredentialsStoreMode::File,
            /*forced_chatgpt_workspace_id*/ None,
            /*chatgpt_base_url*/ None,
            codex_login::AuthKeyringBackendKind::default(),
            codex_login::test_support::transport_default_auth_route_config(),
        )
        .await
    }

    struct RetryResetInventory {
        fails: AtomicBool,
        calls: AtomicU64,
    }

    #[tokio::test]
    async fn target_replacement_during_drain_refuses_before_auth_install() {
        let home = tempfile::TempDir::new().unwrap();
        write_chatgpt_auth_for_intended_account(home.path(), "account-a");
        let manager = Arc::new(real_auth_manager(home.path()).await);
        let resets = Arc::new(RetryResetInventory {
            fails: AtomicBool::new(false),
            calls: AtomicU64::new(0),
        });
        let source = SyntheticTargetEvidenceSource::new(SyntheticTargetFacts::baseline());
        let coordinator = ManagedTransitionCoordinator::from_authoritative_auth_state_target_evidence_and_adoption(
            AuthoritativeAuthState::from_auth_manager(&manager), source.clone(), manager.clone(), resets.clone(),
        );
        let permit = coordinator.try_acquire_account_work_permit().unwrap();
        let mut start = request(coordinator.process_instance_id().await, "changed-target");
        start.expected_auth_fingerprint = Some(account_fingerprint("account-a"));
        write_chatgpt_auth_for_intended_account(home.path(), "account-b");
        coordinator.admit(start).await.unwrap();
        let drain = coordinator.close_barrier_and_drain("changed-target");
        tokio::pin!(drain);
        assert!(futures::poll!(drain.as_mut()).is_pending());
        assert_eq!(
            coordinator
                .state
                .lock()
                .await
                .active
                .as_ref()
                .unwrap()
                .phase,
            ManagedTransitionPhase::Draining
        );

        let mut changed = SyntheticTargetFacts::baseline();
        changed.executable_identity_inode += 1;
        source.replace_facts(changed);
        drop(permit);
        let status = drain.await.unwrap();
        assert_eq!(
            status.refusal,
            Some(ManagedTransitionRefusalKind::TargetChanged)
        );
        assert_eq!(status.phase, ManagedTransitionPhase::Quarantined);
        assert_eq!(status.auth_revision, 0);
        assert_eq!(
            manager.authoritative_managed_auth_fingerprint().unwrap(),
            Some(account_fingerprint("account-a"))
        );
        assert_eq!(resets.calls.load(Ordering::SeqCst), 0);
        assert!(coordinator.try_acquire_account_work_permit().is_none());
    }

    #[tokio::test]
    async fn prepared_source_replacement_refuses_without_installing_or_reparsing_it() {
        let home = tempfile::TempDir::new().unwrap();
        write_chatgpt_auth_for_intended_account(home.path(), "account-a");
        let manager = Arc::new(real_auth_manager(home.path()).await);
        let resets = Arc::new(RetryResetInventory {
            fails: AtomicBool::new(false),
            calls: AtomicU64::new(0),
        });
        let coordinator = ManagedTransitionCoordinator::from_authoritative_auth_state_target_evidence_and_adoption(
            AuthoritativeAuthState::from_auth_manager(&manager), Arc::new(UnsetTargetEvidenceSource), manager.clone(), resets.clone(),
        );
        let mut start = request(coordinator.process_instance_id().await, "prepared-b");
        start.expected_auth_fingerprint = Some(account_fingerprint("account-a"));
        write_chatgpt_auth_for_intended_account(home.path(), "account-b");
        coordinator.admit(start).await.unwrap();
        assert!(
            coordinator
                .state
                .lock()
                .await
                .active
                .as_ref()
                .unwrap()
                .prepared_adoption
                .is_some()
        );
        write_chatgpt_auth_for_intended_account(home.path(), "account-c");
        let status = coordinator
            .close_barrier_and_drain("prepared-b")
            .await
            .unwrap();
        assert_eq!(
            status.refusal,
            Some(ManagedTransitionRefusalKind::AuthSourceChanged)
        );
        assert_eq!(status.phase, ManagedTransitionPhase::Quarantined);
        assert_eq!(status.auth_revision, 0);
        assert_eq!(
            manager
                .authoritative_auth_cached()
                .unwrap()
                .and_then(|auth| auth.get_account_id()),
            Some("account-a".to_owned())
        );
        assert_eq!(resets.calls.load(Ordering::SeqCst), 0);
        assert!(
            coordinator
                .state
                .lock()
                .await
                .completed
                .get("prepared-b")
                .unwrap()
                .prepared_adoption
                .is_none()
        );
        assert!(coordinator.try_acquire_account_work_permit().is_none());
    }

    #[tokio::test]
    async fn adoption_uses_the_admission_precondition_not_a_later_cache_snapshot() {
        let home = tempfile::TempDir::new().unwrap();
        write_chatgpt_auth_for_intended_account(home.path(), "account-a");
        let manager = Arc::new(real_auth_manager(home.path()).await);
        let resets = Arc::new(RetryResetInventory {
            fails: AtomicBool::new(false),
            calls: AtomicU64::new(0),
        });
        let coordinator = ManagedTransitionCoordinator::from_authoritative_auth_state_target_evidence_and_adoption(
            AuthoritativeAuthState::from_auth_manager(&manager), Arc::new(UnsetTargetEvidenceSource), manager.clone(), resets.clone(),
        );
        let mut start = request(coordinator.process_instance_id().await, "admitted-a");
        start.expected_auth_fingerprint = Some(account_fingerprint("account-a"));
        write_chatgpt_auth_for_intended_account(home.path(), "account-b");
        coordinator.admit(start).await.unwrap();
        // An independent writer changes current auth before the adoption
        // function takes its old late snapshot; intended durable B follows.
        write_chatgpt_auth_for_intended_account(home.path(), "account-c");
        let _ = manager.reload().await;
        assert_eq!(
            manager
                .authoritative_auth_cached()
                .unwrap()
                .and_then(|auth| auth.get_account_id()),
            Some("account-c".to_owned())
        );
        write_chatgpt_auth_for_intended_account(home.path(), "account-b");
        let refusal = coordinator
            .close_barrier_and_drain("admitted-a")
            .await
            .unwrap_err();
        assert_eq!(
            refusal.kind,
            ManagedTransitionRefusalKind::AuthoritativeAuthUnavailable
        );
        assert_eq!(
            manager
                .authoritative_auth_cached()
                .unwrap()
                .and_then(|auth| auth.get_account_id()),
            Some("account-c".to_owned())
        );
        assert_eq!(resets.calls.load(Ordering::SeqCst), 0);
        assert_eq!(coordinator.state.lock().await.auth_revision, 0);
        assert!(coordinator.try_acquire_account_work_permit().is_none());
    }

    impl ResetInventory for RetryResetInventory {
        fn reset_all(&self) -> ResetInventoryFuture<'_> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                if self.fails.load(Ordering::SeqCst) {
                    Err(ResetInventoryError::ModelCatalogUnavailable)
                } else {
                    Ok(())
                }
            })
        }
    }

    #[tokio::test]
    async fn pending_reset_recovery_reports_non_quiescent_work_as_reset_failure() {
        let home = tempfile::TempDir::new().unwrap();
        write_chatgpt_auth_for_intended_account(home.path(), "account-b");
        let manager = Arc::new(real_auth_manager(home.path()).await);
        let resets = Arc::new(RetryResetInventory {
            fails: AtomicBool::new(false),
            calls: AtomicU64::new(0),
        });
        let coordinator = ManagedTransitionCoordinator::from_authoritative_auth_state_target_evidence_and_adoption(
            AuthoritativeAuthState::from_auth_manager(&manager),
            Arc::new(UnsetTargetEvidenceSource),
            Arc::clone(&manager),
            resets.clone(),
        );
        let fingerprint = account_fingerprint("account-b");
        let pending = PendingResetRecovery {
            owner_transition_id: "original".to_owned(),
            intent: ManagedTransitionIntent::AdoptManagedAuth,
            auth_fingerprint: Some(fingerprint.clone()),
            auth_revision: 0,
        };
        coordinator.state.lock().await.pending_reset = Some(pending);

        let process = coordinator.process_instance_id().await;
        let mut recovery = request(process, "recovery");
        recovery.expected_auth_fingerprint = Some(fingerprint.clone());
        recovery.intended_result_auth_fingerprint = Some(fingerprint);
        coordinator.admit(recovery).await.unwrap();
        coordinator
            .advance("recovery", ManagedTransitionPhase::Draining)
            .await
            .unwrap();
        let transferred_pending = coordinator
            .state
            .lock()
            .await
            .pending_reset
            .clone()
            .unwrap();
        coordinator
            .account_work_permits
            .inner
            .state
            .fetch_add(1, Ordering::AcqRel);

        let failed = coordinator
            .resume_pending_reset("recovery", transferred_pending, &manager, resets.as_ref())
            .await
            .unwrap();
        coordinator
            .account_work_permits
            .inner
            .state
            .fetch_sub(1, Ordering::AcqRel);

        assert_eq!(failed.phase, ManagedTransitionPhase::Quarantined);
        assert_eq!(
            failed.refusal,
            Some(ManagedTransitionRefusalKind::ResetFailed)
        );
        assert_eq!(resets.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pending_reset_retries_transfer_one_owner_without_reinstall_or_reopening() {
        for intent in [
            ManagedTransitionIntent::AdoptManagedAuth,
            ManagedTransitionIntent::AdoptManagedLogout,
        ] {
            let home = tempfile::TempDir::new().unwrap();
            write_chatgpt_auth_for_intended_account(home.path(), "account-a");
            let manager = Arc::new(real_auth_manager(home.path()).await);
            let resets = Arc::new(RetryResetInventory {
                fails: AtomicBool::new(true),
                calls: AtomicU64::new(0),
            });
            let coordinator = ManagedTransitionCoordinator::from_authoritative_auth_state_target_evidence_and_adoption(
                AuthoritativeAuthState::from_auth_manager(&manager), Arc::new(UnsetTargetEvidenceSource),
                Arc::clone(&manager), resets.clone(),
            );
            if intent == ManagedTransitionIntent::AdoptManagedAuth {
                write_chatgpt_auth_for_intended_account(home.path(), "account-b");
            } else {
                std::fs::remove_file(home.path().join("auth.json")).unwrap();
            }
            let process = coordinator.process_instance_id().await;
            let mut first = request(process.clone(), "original");
            first.intent = intent;
            first.expected_auth_fingerprint = Some(account_fingerprint("account-a"));
            if intent == ManagedTransitionIntent::AdoptManagedLogout {
                first.intended_result_auth_fingerprint = None;
            }
            let StartManagedTransitionResponse::Accepted { status: original } =
                coordinator.start_dispatch(first.clone(), true).await
            else {
                panic!("initial adoption refused")
            };
            assert_eq!(original.phase, ManagedTransitionPhase::Quarantined);
            assert_eq!(
                coordinator.admit(first.clone()).await.unwrap_err().kind,
                ManagedTransitionRefusalKind::CompletedReplay
            );
            first.intended_result_auth_fingerprint = Some("different".to_owned());
            assert_eq!(
                coordinator.admit(first).await.unwrap_err().kind,
                ManagedTransitionRefusalKind::TransitionIdConflict
            );
            let mut fresh =
                request_at_revision(process.clone(), "retry", original.transition_revision);
            fresh.intent = intent;
            fresh.expected_auth_revision = original.auth_revision;
            fresh.expected_auth_fingerprint = original.result_auth_fingerprint.clone();
            fresh.intended_result_auth_fingerprint = original.result_auth_fingerprint.clone();
            let mut stale = fresh.clone();
            stale.expected_auth_revision = original.auth_revision + 1;
            assert_eq!(
                coordinator.admit(stale).await.unwrap_err().kind,
                ManagedTransitionRefusalKind::StaleAuthRevision
            );
            let mut mismatch = fresh.clone();
            mismatch.intended_result_auth_fingerprint = Some("unintended".to_owned());
            assert!(coordinator.admit(mismatch).await.is_err());
            assert_eq!(resets.calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                coordinator
                    .state
                    .lock()
                    .await
                    .pending_reset
                    .as_ref()
                    .unwrap()
                    .owner_transition_id,
                "original"
            );

            let mut other = fresh.clone();
            other.transition_id = "competing-retry".to_owned();
            let a = coordinator.clone();
            let b = coordinator.clone();
            let (left, right) = tokio::join!(
                tokio::spawn(async move { a.admit(fresh).await }),
                tokio::spawn(async move { b.admit(other).await })
            );
            let results = [left.unwrap(), right.unwrap()];
            assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
            let winner = results
                .into_iter()
                .find_map(Result::ok)
                .unwrap()
                .transition_id
                .unwrap();
            assert!(coordinator.try_acquire_account_work_permit().is_none());
            assert_eq!(
                coordinator
                    .cancel(cancel_request(process.clone(), &winner))
                    .await
                    .unwrap_err()
                    .kind,
                ManagedTransitionRefusalKind::LateCancellation
            );
            let failed = coordinator.close_barrier_and_drain(&winner).await.unwrap();
            assert_eq!(failed.phase, ManagedTransitionPhase::Quarantined);
            assert_eq!(failed.auth_revision, original.auth_revision);
            assert!(coordinator.try_acquire_account_work_permit().is_none());
            assert_eq!(
                coordinator
                    .read(read_request(process.clone(), "original"))
                    .await
                    .unwrap(),
                original
            );
            assert_eq!(
                coordinator
                    .state
                    .lock()
                    .await
                    .pending_reset
                    .as_ref()
                    .unwrap()
                    .owner_transition_id,
                winner
            );

            // Model the terminal authority-verification failure that leaves
            // this installed result quarantined: only a request matching the
            // pending owner's intent and fingerprint may enter recovery while
            // ordinary new work remains blocked.
            coordinator.state.lock().await.auth_authority_available = false;
            resets.fails.store(false, Ordering::SeqCst);
            let mut third =
                request_at_revision(process.clone(), "final-retry", failed.transition_revision);
            third.intent = intent;
            third.expected_auth_revision = failed.auth_revision;
            third.expected_auth_fingerprint = failed.result_auth_fingerprint.clone();
            third.intended_result_auth_fingerprint = failed.result_auth_fingerprint.clone();
            let mut ordinary = third.clone();
            ordinary.transition_id = "ordinary-while-authority-unavailable".to_owned();
            ordinary.intent = match intent {
                ManagedTransitionIntent::AdoptManagedAuth => {
                    ManagedTransitionIntent::AdoptManagedLogout
                }
                ManagedTransitionIntent::AdoptManagedLogout => {
                    ManagedTransitionIntent::AdoptManagedAuth
                }
            };
            ordinary.intended_result_auth_fingerprint = match ordinary.intent {
                ManagedTransitionIntent::AdoptManagedAuth => {
                    Some(account_fingerprint("ordinary-account"))
                }
                ManagedTransitionIntent::AdoptManagedLogout => None,
            };
            assert_eq!(
                coordinator.admit(ordinary).await.unwrap_err().kind,
                ManagedTransitionRefusalKind::StaleAuthFingerprint,
                "a cleared authority latch admits only a matching pending-reset recovery"
            );
            let StartManagedTransitionResponse::Accepted { status: success } =
                coordinator.start_dispatch(third, true).await
            else {
                panic!("recovery refused")
            };
            assert_eq!(success.phase, ManagedTransitionPhase::Succeeded);
            assert_eq!(success.auth_revision, original.auth_revision);
            assert_eq!(resets.calls.load(Ordering::SeqCst), 3);
            let state = coordinator.state.lock().await;
            assert!(state.pending_reset.is_none());
            assert!(
                state.auth_authority_available,
                "successful pending-reset recovery must restore the authority latch"
            );
            drop(state);
            assert!(coordinator.try_acquire_account_work_permit().is_some());
            assert_eq!(
                coordinator
                    .read(read_request(process, "original"))
                    .await
                    .unwrap(),
                original
            );
        }
    }

    #[tokio::test]
    async fn pending_reset_recovery_refuses_changed_or_unreadable_source_and_cache() {
        for fault in [
            "source-absent",
            "source-malformed",
            "source-changed",
            "cache-changed",
        ] {
            let home = tempfile::TempDir::new().unwrap();
            write_chatgpt_auth_for_intended_account(home.path(), "account-a");
            let manager = Arc::new(real_auth_manager(home.path()).await);
            let resets = Arc::new(RetryResetInventory {
                fails: AtomicBool::new(true),
                calls: AtomicU64::new(0),
            });
            let coordinator = ManagedTransitionCoordinator::from_authoritative_auth_state_target_evidence_and_adoption(
                AuthoritativeAuthState::from_auth_manager(&manager), Arc::new(UnsetTargetEvidenceSource), manager.clone(), resets.clone(),
            );
            let process = coordinator.process_instance_id().await;
            write_chatgpt_auth_for_intended_account(home.path(), "account-b");
            let mut first = request(process.clone(), "first");
            first.expected_auth_fingerprint = Some(account_fingerprint("account-a"));
            let StartManagedTransitionResponse::Accepted { status: applied } =
                coordinator.start_dispatch(first, true).await
            else {
                panic!("adoption refused")
            };
            assert_eq!(applied.phase, ManagedTransitionPhase::Quarantined);
            match fault {
                "source-absent" => std::fs::remove_file(home.path().join("auth.json")).unwrap(),
                "source-malformed" => {
                    std::fs::write(home.path().join("auth.json"), "invalid").unwrap()
                }
                "source-changed" => {
                    write_chatgpt_auth_for_intended_account(home.path(), "account-c")
                }
                "cache-changed" => {
                    write_chatgpt_auth_for_intended_account(home.path(), "account-c");
                    let _ = manager.reload().await;
                    assert_eq!(
                        manager
                            .authoritative_auth_cached()
                            .unwrap()
                            .and_then(|auth| auth.get_account_id()),
                        Some("account-c".to_owned())
                    );
                    write_chatgpt_auth_for_intended_account(home.path(), "account-b");
                }
                _ => unreachable!(),
            }
            resets.fails.store(false, Ordering::SeqCst);
            let mut retry =
                request_at_revision(process.clone(), "retry", applied.transition_revision);
            retry.expected_auth_revision = applied.auth_revision;
            retry.expected_auth_fingerprint = applied.result_auth_fingerprint.clone();
            let outcome = coordinator.start_dispatch(retry, true).await;
            if fault == "cache-changed" {
                let StartManagedTransitionResponse::Refused { refusal } = outcome else {
                    panic!("changed cache must not be reported as current intended truth")
                };
                assert_eq!(
                    refusal.kind,
                    ManagedTransitionRefusalKind::AuthoritativeAuthUnavailable
                );
                let state = coordinator.state.lock().await;
                assert!(state.active.is_none());
                assert_eq!(
                    state.pending_reset.as_ref().unwrap().owner_transition_id,
                    "first"
                );
                assert_eq!(state.transition_revision, applied.transition_revision);
                drop(state);
                assert!(coordinator.try_acquire_account_work_permit().is_none());
                assert_eq!(resets.calls.load(Ordering::SeqCst), 1);
                continue;
            }
            let StartManagedTransitionResponse::Accepted { status: failed } = outcome else {
                panic!("source-only failure should remain observable quarantine")
            };
            assert_eq!(failed.phase, ManagedTransitionPhase::Quarantined, "{fault}");
            assert_eq!(failed.auth_revision, applied.auth_revision, "{fault}");
            assert_eq!(
                resets.calls.load(Ordering::SeqCst),
                1,
                "{fault}: reset must not execute"
            );
            assert!(coordinator.try_acquire_account_work_permit().is_none());
            assert_eq!(
                coordinator
                    .read(read_request(process, "first"))
                    .await
                    .unwrap(),
                applied
            );
        }
    }

    #[tokio::test]
    async fn logged_out_without_pending_managed_logout_cannot_start_recovery() {
        let home = tempfile::TempDir::new().unwrap();
        let manager = Arc::new(real_auth_manager(home.path()).await);
        let resets = Arc::new(RetryResetInventory {
            fails: AtomicBool::new(false),
            calls: AtomicU64::new(0),
        });
        let coordinator = ManagedTransitionCoordinator::from_authoritative_auth_state_target_evidence_and_adoption(
            AuthoritativeAuthState::from_auth_manager(&manager), Arc::new(UnsetTargetEvidenceSource), manager, resets.clone(),
        );
        let mut start = request(coordinator.process_instance_id().await, "unproven-logout");
        start.intent = ManagedTransitionIntent::AdoptManagedLogout;
        start.intended_result_auth_fingerprint = None;
        assert_eq!(
            coordinator.admit(start).await.unwrap_err().kind,
            ManagedTransitionRefusalKind::InvalidRequest
        );
        assert!(coordinator.state.lock().await.active.is_none());
        assert_eq!(resets.calls.load(Ordering::SeqCst), 0);
        assert!(coordinator.try_acquire_account_work_permit().is_some());
    }

    /// The full production Adopting/Resetting/Succeeded chain, driven
    /// end-to-end through the public `start_dispatch` wire entry point
    /// exactly as `message_processor.rs` calls it, against a genuine
    /// on-disk `codex_home` and a real `AuthManager` (Issue 05 Slice 4,
    /// R014). Proves: the new account's fingerprint becomes both the
    /// transition's own result and the coordinator's new CAS baseline; the
    /// reset inventory actually runs; the barrier reopens; and the auth
    /// manager has genuinely installed the new account, not merely
    /// recorded a fingerprint.
    #[tokio::test]
    async fn full_adoption_reads_installs_resets_and_reopens_with_the_new_accounts_fingerprint() {
        let codex_home = tempfile::TempDir::new().expect("create temp codex_home");
        write_chatgpt_auth_for_intended_account(codex_home.path(), "account-a");
        let auth_manager = Arc::new(real_auth_manager(codex_home.path()).await);
        let reset_inventory = Arc::new(RecordingResetInventory::new(false));
        let coordinator = ManagedTransitionCoordinator::from_authoritative_auth_state_target_evidence_and_adoption(
            AuthoritativeAuthState::from_auth_manager(&auth_manager),
            Arc::new(UnsetTargetEvidenceSource),
            Arc::clone(&auth_manager),
            Arc::clone(&reset_inventory) as Arc<dyn ResetInventory>,
        );
        let process_id = coordinator.process_instance_id().await;

        // A different account lands on disk before the transition reaches
        // Adopting -- simulating the operator's external managed-auth write
        // -- so the coordinator must read this fresh value, not the one
        // captured at admission time.
        write_chatgpt_auth_for_intended_account(codex_home.path(), "account-b");
        let expected_new_fingerprint = expected_fingerprint_for_account("account-b");

        let mut start = request(process_id.clone(), "adopt-b");
        start.expected_auth_fingerprint = Some(expected_fingerprint_for_account("account-a"));
        let status = match coordinator.start_dispatch(start, true).await {
            StartManagedTransitionResponse::Accepted { status } => status,
            StartManagedTransitionResponse::Refused { refusal } => {
                panic!("expected acceptance, got refusal: {refusal:?}")
            }
        };
        assert_eq!(status.phase, ManagedTransitionPhase::Succeeded);
        assert_eq!(
            status.result_auth_fingerprint.as_deref(),
            Some(expected_new_fingerprint.as_str())
        );
        assert!(
            reset_inventory.was_called(),
            "the reset inventory must run before a successful terminal outcome"
        );
        assert!(
            coordinator.try_acquire_account_work_permit().is_some(),
            "the barrier must reopen once adoption succeeds"
        );

        let installed_account_id = auth_manager
            .authoritative_auth_cached()
            .expect("auth manager readable after adoption")
            .and_then(|auth| auth.get_account_id());
        assert_eq!(
            installed_account_id,
            Some("account-b".to_owned()),
            "AuthManager must have the newly-adopted account actually installed, not just a bookkeeping fingerprint"
        );

        // The coordinator's own CAS baseline must reflect the adoption too:
        // a fresh admission using the new fingerprint succeeds immediately.
        let mut retry = request(process_id, "post-adopt-check");
        retry.expected_auth_fingerprint = Some(expected_new_fingerprint);
        retry.expected_auth_revision = status.auth_revision;
        retry.expected_transition_revision = status.transition_revision;
        assert_eq!(
            coordinator.admit(retry).await.unwrap().phase,
            ManagedTransitionPhase::Admitted
        );
    }

    /// An absent source for auth adoption refuses before barrier effects.
    #[tokio::test]
    async fn adoption_source_read_failure_refuses_before_admission_and_reset() {
        let codex_home = tempfile::TempDir::new().expect("create temp codex_home");
        write_chatgpt_auth_for_intended_account(codex_home.path(), "account-a");
        let auth_manager = Arc::new(real_auth_manager(codex_home.path()).await);
        let reset_inventory = Arc::new(RecordingResetInventory::new(false));
        let coordinator = ManagedTransitionCoordinator::from_authoritative_auth_state_target_evidence_and_adoption(
            AuthoritativeAuthState::from_auth_manager(&auth_manager),
            Arc::new(UnsetTargetEvidenceSource),
            Arc::clone(&auth_manager),
            Arc::clone(&reset_inventory) as Arc<dyn ResetInventory>,
        );
        let process_id = coordinator.process_instance_id().await;

        // The source disappears before admission preparation reads it.
        std::fs::remove_file(codex_home.path().join("auth.json")).expect("remove auth.json");

        let mut start = request(process_id, "adopt-missing");
        start.expected_auth_fingerprint = Some(expected_fingerprint_for_account("account-a"));
        let refusal = match coordinator.start_dispatch(start.clone(), true).await {
            StartManagedTransitionResponse::Accepted { .. } => {
                panic!("missing source must not admit")
            }
            StartManagedTransitionResponse::Refused { refusal } => refusal,
        };
        assert_eq!(refusal.kind, ManagedTransitionRefusalKind::InvalidRequest);
        // Absence is an invalid intended auth result; malformed storage is
        // operational unavailability, not a client request error. Neither
        // attempt consumes the transition ID or mutates barrier/revisions.
        std::fs::write(codex_home.path().join("auth.json"), "not-json").unwrap();
        let StartManagedTransitionResponse::Refused { refusal } =
            coordinator.start_dispatch(start, true).await
        else {
            panic!("unreadable source must not admit")
        };
        assert_eq!(
            refusal.kind,
            ManagedTransitionRefusalKind::AuthoritativeAuthUnavailable
        );
        assert!(refusal.retryable);
        assert!(
            !reset_inventory.was_called(),
            "a source-read failure must refuse before ever reaching the reset step"
        );
        assert!(
            coordinator.try_acquire_account_work_permit().is_some(),
            "preparation failure must leave the barrier untouched"
        );
        assert!(coordinator.state.lock().await.active.is_none());
        assert_eq!(coordinator.state.lock().await.transition_revision, 0);
    }

    /// A reset-inventory failure after a successful read/install must also
    /// quarantine (fail closed) rather than report `Succeeded` over
    /// known-stale account-derived caches/workers (R014).
    #[tokio::test]
    async fn adoption_reset_inventory_failure_quarantines_after_installing_auth() {
        let codex_home = tempfile::TempDir::new().expect("create temp codex_home");
        write_chatgpt_auth_for_intended_account(codex_home.path(), "account-a");
        let auth_manager = Arc::new(real_auth_manager(codex_home.path()).await);
        let reset_inventory = Arc::new(RecordingResetInventory::new(true));
        let coordinator = ManagedTransitionCoordinator::from_authoritative_auth_state_target_evidence_and_adoption(
            AuthoritativeAuthState::from_auth_manager(&auth_manager),
            Arc::new(UnsetTargetEvidenceSource),
            Arc::clone(&auth_manager),
            Arc::clone(&reset_inventory) as Arc<dyn ResetInventory>,
        );
        let process_id = coordinator.process_instance_id().await;
        write_chatgpt_auth_for_intended_account(codex_home.path(), "account-b");

        let mut start = request(process_id, "adopt-then-fail-reset");
        start.expected_auth_fingerprint = Some(expected_fingerprint_for_account("account-a"));
        let status = match coordinator.start_dispatch(start, true).await {
            StartManagedTransitionResponse::Accepted { status } => status,
            StartManagedTransitionResponse::Refused { refusal } => {
                panic!(
                    "expected acceptance carrying a quarantined status, got refusal: {refusal:?}"
                )
            }
        };
        assert_eq!(status.phase, ManagedTransitionPhase::Quarantined);
        assert!(status.retryable);
        assert!(
            reset_inventory.was_called(),
            "the reset inventory must have run before it reported failure"
        );
        assert_eq!(
            status.refusal,
            Some(ManagedTransitionRefusalKind::ResetFailed)
        );
        assert_eq!(
            coordinator.state.lock().await.completed["adopt-then-fail-reset"].reset_failure,
            Some(ResetInventoryError::ModelCatalogUnavailable)
        );
        assert_eq!(
            coordinator
                .current_transition_status("adopt-then-fail-reset")
                .await
                .unwrap(),
            status,
            "read must retain the cause committed with the terminal record"
        );
        assert!(
            coordinator.try_acquire_account_work_permit().is_none(),
            "quarantine must keep the barrier closed until an explicit cancel"
        );
        // Auth was already installed before the reset step ran -- this
        // implementation deliberately does not roll it back on a reset
        // failure (rolling back a real credential swap is its own hazard);
        // quarantine exists precisely to make this state visible and
        // require an explicit operator decision rather than silently
        // reporting success over it.
        let installed_account_id = auth_manager
            .authoritative_auth_cached()
            .expect("auth manager readable")
            .and_then(|auth| auth.get_account_id());
        assert_eq!(installed_account_id, Some("account-b".to_owned()));
        let cancellation = coordinator
            .cancel(CancelManagedTransitionParams {
                contract_version: MANAGED_AUTH_TRANSITION_CONTRACT_VERSION,
                transition_id: "adopt-then-fail-reset".to_owned(),
                process_instance_id: coordinator.process_instance_id().await,
            })
            .await
            .expect_err("cancellation must not bypass a pending reset");
        assert_eq!(
            cancellation.kind,
            ManagedTransitionRefusalKind::LateCancellation
        );
        assert!(coordinator.try_acquire_account_work_permit().is_none());
        // The coordinator's own CAS baseline must match what AuthManager
        // actually holds (B), not the pre-adoption account (A) or `None`
        // -- otherwise a later exact-quarantine cancel would reopen the
        // barrier while a fresh admission could still use the stale A
        // fingerprint even though B is truly installed (R064's "auth,
        // revision, reset, acknowledgement, and quarantine remain
        // mutually consistent").
        assert_eq!(
            status.result_auth_fingerprint.as_deref(),
            Some(expected_fingerprint_for_account("account-b").as_str()),
            "quarantine after a post-install reset failure must adopt B's fingerprint as the new CAS baseline"
        );
    }

    /// Legacy (Slice 1-3) coordinators -- constructed with no adoption
    /// dependencies wired -- must keep their exact pre-Slice-4 behavior:
    /// a successfully-drained transition reports `Draining`, not an
    /// auto-continuation into Adopting. This is what makes every existing
    /// Slice 1-3 test above still valid: opting into the Slice 4 chain is a
    /// property of construction, not of drain completion.
    #[tokio::test]
    async fn a_coordinator_without_adoption_dependencies_stops_at_draining_after_a_completed_drain()
    {
        let coordinator = ManagedTransitionCoordinator::new();
        let process_id = coordinator.process_instance_id().await;
        let response = coordinator
            .start_dispatch(request(process_id, "no-adoption-wired"), true)
            .await;
        let status = match response {
            StartManagedTransitionResponse::Accepted { status } => status,
            StartManagedTransitionResponse::Refused { refusal } => {
                panic!("expected acceptance, got refusal: {refusal:?}")
            }
        };
        assert_eq!(status.phase, ManagedTransitionPhase::Draining);
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
