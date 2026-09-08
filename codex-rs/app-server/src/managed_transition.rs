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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use uuid::Uuid;

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
    barrier_closed: AtomicBool,
    admitted: AtomicU64,
    /// Shared by permit release and by cancellation of a draining
    /// transition -- both are "something the drain wait should recheck"
    /// events. A spurious wake from the other event class costs only one
    /// extra recheck of the two loop conditions.
    signal: Notify,
}

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
        self.permits.inner.admitted.fetch_sub(1, Ordering::AcqRel);
        self.permits.inner.signal.notify_waiters();
    }
}

impl AccountWorkPermits {
    fn new() -> Self {
        Self {
            inner: Arc::new(AccountWorkPermitsInner {
                barrier_closed: AtomicBool::new(false),
                admitted: AtomicU64::new(0),
                signal: Notify::new(),
            }),
        }
    }

    /// Increment-then-check, not check-then-increment: this ordering means
    /// a permit acquired concurrently with [`Self::close`] either observes
    /// the close and backs out, or is guaranteed visible to the close's own
    /// drain wait, because it incremented the count before that wait's
    /// first read of it could possibly have happened. No straggler can
    /// slip past the barrier undetected in either direction.
    pub(crate) fn try_acquire(&self) -> Option<AccountWorkPermitGuard> {
        self.inner.admitted.fetch_add(1, Ordering::AcqRel);
        if self.inner.barrier_closed.load(Ordering::Acquire) {
            self.inner.admitted.fetch_sub(1, Ordering::AcqRel);
            self.inner.signal.notify_waiters();
            return None;
        }
        Some(AccountWorkPermitGuard {
            permits: self.clone(),
        })
    }

    fn close(&self) {
        self.inner.barrier_closed.store(true, Ordering::Release);
    }

    fn reopen(&self) {
        self.inner.barrier_closed.store(false, Ordering::Release);
    }

    fn admitted_count(&self) -> u64 {
        self.inner.admitted.load(Ordering::Acquire)
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

#[derive(Clone)]
pub(crate) struct ManagedTransitionCoordinator {
    state: Arc<Mutex<CoordinatorState>>,
    /// Immutable per-coordinator; not behind `state`'s lock since revalidation
    /// only ever queries it, never mutates it.
    target_evidence_source: Arc<dyn TargetEvidenceSource>,
    account_work_permits: AccountWorkPermits,
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
///
/// Deliberately does not derive `PartialEq`/`Eq`: the only intended
/// comparison is [`Self::matches_target`], which excludes `record_identity`.
/// A derived structural equality would silently disagree with that
/// semantics (it would compare `record_identity` too, so it would always be
/// `false` between any two captures) — see `CODEX-I05-S02` verification
/// round 01, M2.
#[derive(Debug, Clone)]
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
            account_work_permits: AccountWorkPermits::new(),
        }
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

    /// Closes the process-wide account-work barrier and awaits zero drain
    /// within [`DRAIN_DEADLINE`] (`CODEX-I05-S03-R009`, `R012`, `R013`).
    /// Called once, synchronously, immediately after a successful
    /// [`Self::admit`] as one continuous step of `start`'s own wire
    /// handling -- the canonical plan describes barrier close and drain
    /// await as part of `start`, not a detached background task, so a
    /// client's `ManagedTransitionStart` response is not sent until this
    /// resolves (bounded to `DRAIN_DEADLINE`).
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
        self.account_work_permits.close();
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
                    .advance(transition_id, ManagedTransitionPhase::Quarantined)
                    .await;
            }
        }

        self.current_transition_status(transition_id).await
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
            drop(state);
            self.account_work_permits.reopen();
            self.account_work_permits.wake_waiters();
            return Ok(status);
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
        if !matches!(
            record.phase,
            ManagedTransitionPhase::Admitted | ManagedTransitionPhase::Draining
        ) {
            state.active = Some(record);
            return Err(refusal(
                &state,
                &envelope,
                ManagedTransitionRefusalKind::LateCancellation,
                false,
            ));
        }
        let was_draining = record.phase == ManagedTransitionPhase::Draining;

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
        drop(state);
        if was_draining {
            // Reopen so ordinary account-dependent work resumes immediately
            // rather than staying refused for the rest of the process's
            // lifetime over an attempt nothing will ever retry-complete;
            // wake any in-flight `close_barrier_and_drain` so it observes
            // this cancellation instead of running to its own timeout.
            self.account_work_permits.reopen();
            self.account_work_permits.wake_waiters();
        }
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
            .admit(request(process_id.clone(), "transition-a"))
            .await
            .unwrap_err();
        assert_eq!(refused.kind, ManagedTransitionRefusalKind::InvalidRequest);

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
            assert_eq!(refused.kind, ManagedTransitionRefusalKind::InvalidRequest);

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
