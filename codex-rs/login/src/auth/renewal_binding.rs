//! Renewal authority identity, captured without reversing provider/cache locks.
use super::*;

/// Exact external authority, fixed principal and mode captured at admission.
/// This identity alone does not grant permission to refresh or publish.
#[derive(Clone)]
pub struct AuthRenewalBinding {
    source: Option<Arc<dyn ExternalAuth>>,
    fixed_principal: Option<FixedPrincipal>,
    auth_mode: Option<AuthMode>,
}
impl AuthRenewalBinding {
    pub(super) fn matches_provider(&self, provider: &Option<Arc<dyn ExternalAuth>>) -> bool {
        same_provider(&self.source, provider)
    }
    pub(super) fn covers_owner(&self, auth: &CodexAuth) -> bool {
        if self.auth_mode != Some(auth.api_auth_mode()) {
            return false;
        }
        let complete = |p: &FixedPrincipal| {
            !p.account_id.trim().is_empty() && !p.chatgpt_user_id.trim().is_empty()
        };
        match (&self.fixed_principal, FixedPrincipal::carried_by(auth)) {
            (Some(fixed), Some(carried)) => complete(fixed) && *fixed == carried,
            (Some(fixed), None) => complete(fixed),
            (None, Some(carried)) => complete(&carried),
            (None, None) => false,
        }
    }
    pub(super) fn is_host(&self) -> bool {
        self.source.is_some()
    }
}

impl PartialEq for AuthRenewalBinding {
    fn eq(&self, other: &Self) -> bool {
        same_provider(&self.source, &other.source)
            && self.fixed_principal == other.fixed_principal
            && self.auth_mode == other.auth_mode
    }
}
impl std::fmt::Debug for AuthRenewalBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthRenewalBinding")
            .field("host_source", &self.source.is_some())
            .field("fixed_principal", &self.fixed_principal)
            .field("auth_mode", &self.auth_mode)
            .finish()
    }
}
fn same_provider(a: &Option<Arc<dyn ExternalAuth>>, b: &Option<Arc<dyn ExternalAuth>>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => Arc::ptr_eq(a, b),
        (None, None) => true,
        _ => false,
    }
}

impl AuthManager {
    pub fn renewal_binding(&self) -> Option<AuthRenewalBinding> {
        self.renewal_snapshot().map(|(_, _, binding)| binding)
    }

    /// Internal prerequisite for pinned admission. No await or external callback
    /// occurs while cache custody is held. All four upstream revisions survive.
    pub(super) fn renewal_snapshot(
        &self,
    ) -> Option<(Option<CodexAuth>, AuthChangeState, AuthRenewalBinding)> {
        if let Some(pin) = &self.owner_pin {
            return Some((
                pin.cached_auth().ok()?,
                *self.auth_change_state_tx.borrow(),
                pin.renewal.clone(),
            ));
        }
        let observed = self.external_auth.read().ok()?.clone();
        let fixed_principal = observed
            .as_ref()
            .and_then(|source| source.fixed_principal());
        // Ordinary clear holds external write -> cache. Take external read first,
        // then command mode/source/native -> cache, never cache -> external.
        let slot = self.external_auth.read().ok()?;
        if !same_provider(&observed, &slot) {
            return None;
        }
        let capture = |cached: &CachedAuth| {
            let binding = AuthRenewalBinding {
                source: observed.clone(),
                fixed_principal,
                auth_mode: cached.auth.as_ref().map(CodexAuth::api_auth_mode),
            };
            (
                cached.auth.clone(),
                *self.auth_change_state_tx.borrow(),
                binding,
            )
        };
        if self.command_source.is_some() {
            let resolved = self.auth_cached()?;
            let publication = self.cache_publication(Some(&resolved)).ok()?;
            if !Self::auths_equal_for_refresh(publication.cache.auth.as_ref(), Some(&resolved)) {
                return None;
            }
            publication.revalidate().ok()?;
            let snapshot = capture(&publication.cache);
            publication.revalidate().ok()?;
            Some(snapshot)
        } else {
            let cached = self.inner.read().ok()?;
            Some(capture(&cached))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::command_cache_publication_tests::{record, selected_fixture};
    use super::*;
    #[test]
    fn native_binding_is_stable_without_inventing_external_authority() {
        let manager = AuthManager::from_auth_for_testing(CodexAuth::from_api_key("synthetic"));
        let a = manager.renewal_binding().unwrap();
        let b = manager.renewal_binding().unwrap();
        assert_eq!(a, b);
        assert!(a.source.is_none());
        assert!(a.fixed_principal.is_none());
        assert_eq!(a.auth_mode, Some(AuthMode::ApiKey));
    }
    #[cfg(unix)]
    #[test]
    fn command_binding_uses_exact_source_and_all_current_revisions() {
        let home = tempfile::tempdir().unwrap();
        let (_, manager) = selected_fixture(home.path(), record(1));
        let revisions = AuthChangeState {
            generation: 3,
            owner_generation: 5,
            source_generation: 7,
            login_generation: 11,
        };
        manager.auth_change_state_tx.send_replace(revisions);
        let (auth, state, binding) = manager.renewal_snapshot().unwrap();
        assert!(auth.is_some());
        assert_eq!(state, revisions);
        assert!(binding.source.is_some());
        assert_eq!(binding.fixed_principal.unwrap().account_id, "account");
    }
    #[cfg(unix)]
    #[test]
    fn replacement_provider_identity_does_not_compare_equal() {
        let a_home = tempfile::tempdir().unwrap();
        let b_home = tempfile::tempdir().unwrap();
        let (_, a) = selected_fixture(a_home.path(), record(1));
        let (_, b) = selected_fixture(b_home.path(), record(1));
        assert_ne!(a.renewal_binding().unwrap(), b.renewal_binding().unwrap());
    }
    #[cfg(unix)]
    #[test]
    fn stale_durable_command_record_cannot_be_captured() {
        let home = tempfile::tempdir().unwrap();
        let (_, manager) = selected_fixture(home.path(), record(1));
        let newer = record(2);
        super::super::super::command_store::commit(home.path(), &newer, &newer.auth().unwrap())
            .unwrap();
        assert!(manager.renewal_snapshot().is_none());
    }
    #[test]
    fn poisoned_provider_slot_is_uncertainty_not_native_binding() {
        let manager = AuthManager::from_auth_for_testing(CodexAuth::from_api_key("synthetic"));
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _slot = manager.external_auth.write().unwrap();
            panic!("synthetic poison");
        }));
        assert!(manager.renewal_binding().is_none());
    }
    struct ReentrantProvider(std::sync::Weak<AuthManager>);
    impl ExternalAuth for ReentrantProvider {
        fn resolve(&self) -> ExternalAuthFuture<'_, CodexAuth> {
            Box::pin(async { Err(std::io::Error::other("must not resolve during snapshot")) })
        }
        fn refresh(&self, _: ExternalAuthRefreshContext) -> ExternalAuthFuture<'_, CodexAuth> {
            self.resolve()
        }
        fn fixed_principal(&self) -> Option<FixedPrincipal> {
            let manager = self.0.upgrade().unwrap();
            // Refuse promptly rather than hanging if either callback lock is held.
            let _cache = manager
                .inner
                .try_write()
                .expect("callback outside cache custody");
            let mut slot = manager
                .external_auth
                .try_write()
                .expect("callback outside provider custody");
            *slot = Some(Arc::new(FixedApiKeyAuth(CodexAuth::from_api_key(
                "replacement",
            ))));
            None
        }
    }
    #[test]
    fn reentrant_provider_replacement_invalidates_observed_binding_without_deadlock() {
        let manager = AuthManager::from_auth_for_testing(CodexAuth::from_api_key("synthetic"));
        *manager.external_auth.write().unwrap() =
            Some(Arc::new(ReentrantProvider(Arc::downgrade(&manager))));
        assert!(manager.renewal_snapshot().is_none());
        assert!(manager.external_auth.try_write().is_ok());
        assert!(manager.inner.try_write().is_ok());
    }
    #[cfg(unix)]
    #[test]
    fn failed_command_capture_releases_provider_and_source_guards() {
        let home = tempfile::tempdir().unwrap();
        let (source, manager) = selected_fixture(home.path(), record(1));
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _cache = manager.inner.write().unwrap();
            panic!("synthetic cache poison");
        }));
        assert!(manager.renewal_snapshot().is_none());
        assert!(source.state.try_lock().is_ok());
        assert!(manager.external_auth.try_write().is_ok());
    }
}

impl AuthManager {
    /// Only a real refresh response may classify the captured credential as
    /// permanently failed. Admission/proof refusals must never poison the source.
    pub(super) fn record_bound_refresh_failure(
        &self,
        expected: &super::super::AuthOwnerSnapshot,
        binding: &AuthRenewalBinding,
        attempted: &CodexAuth,
        error: &RefreshTokenFailedError,
    ) {
        let Ok(authority) = self.external_auth.read() else {
            return;
        };
        if !binding.matches_provider(&authority) {
            return;
        }
        let Ok(mut cached) = self.inner.write() else {
            return;
        };
        if self.owner_snapshot_locked(&cached) == *expected
            && Self::auths_equal_for_refresh(cached.auth.as_ref(), Some(attempted))
        {
            cached.permanent_refresh_failure = Some(AuthScopedRefreshFailure {
                auth: attempted.clone(),
                error: error.clone(),
            });
        }
    }

    /// Guarded host-only publication; invocation and durable commit are separate.
    pub(super) fn publish_auth_for_owner(
        &self,
        expected: &super::super::AuthOwnerSnapshot,
        renewal: &AuthRenewalBinding,
        auth: CodexAuth,
    ) -> Result<bool, RefreshTokenError> {
        let refused =
            || permanent_external_auth_error("owner renewal authority changed or unproven");
        let observed = self.external_auth.read().map_err(|_| refused())?.clone();
        let fixed = observed
            .as_ref()
            .and_then(|source| source.fixed_principal());
        if observed.is_none()
            || !same_provider(&observed, &renewal.source)
            || fixed != renewal.fixed_principal
            || renewal.auth_mode != expected.auth_mode
            || renewal.auth_mode != Some(auth.api_auth_mode())
        {
            return Err(refused());
        }
        let carried_owner = match (&expected.chatgpt_user_id, &expected.account_id) {
            (Some(user), Some(account))
                if !user.trim().is_empty() && !account.trim().is_empty() =>
            {
                Some(FixedPrincipal {
                    chatgpt_user_id: user.clone(),
                    account_id: account.clone(),
                })
            }
            _ => None,
        };
        let principal = match (&renewal.fixed_principal, carried_owner) {
            (Some(fixed), Some(owner)) if fixed == &owner => owner,
            (Some(_), Some(_)) => return Err(refused()),
            (Some(fixed), None)
                if !fixed.chatgpt_user_id.trim().is_empty()
                    && !fixed.account_id.trim().is_empty() =>
            {
                fixed.clone()
            }
            (None, Some(owner)) => owner,
            _ => return Err(refused()),
        };
        match FixedPrincipal::carried_by(&auth) {
            Some(carried) if carried != principal => return Err(refused()),
            None if renewal.fixed_principal.is_none() => return Err(refused()),
            _ => (),
        }
        let slot = self.external_auth.read().map_err(|_| refused())?;
        if !same_provider(&observed, &slot) {
            return Err(refused());
        }
        let mut publication = self
            .cache_publication(Some(&auth))
            .map_err(RefreshTokenError::Transient)?;
        if self.owner_snapshot_locked(&publication.cache) != *expected {
            return Err(refused());
        }
        publication
            .revalidate()
            .map_err(RefreshTokenError::Transient)?;
        self.set_cached_auth_locked_with_owner_proof(&mut publication.cache, Some(auth), true);
        // The setter reports coarse identity changes, not publication acceptance.
        Ok(true)
    }
}

#[cfg(test)]
mod publication_tests {
    use super::*;
    struct FixedAuthority(Option<FixedPrincipal>);
    impl ExternalAuth for FixedAuthority {
        fn resolve(&self) -> ExternalAuthFuture<'_, CodexAuth> {
            Box::pin(async {
                Err(std::io::Error::other(
                    "publication must not invoke authority",
                ))
            })
        }
        fn refresh(&self, _: ExternalAuthRefreshContext) -> ExternalAuthFuture<'_, CodexAuth> {
            self.resolve()
        }
        fn fixed_principal(&self) -> Option<FixedPrincipal> {
            self.0.clone()
        }
    }
    fn principal() -> FixedPrincipal {
        FixedPrincipal {
            chatgpt_user_id: "principal".into(),
            account_id: "account".into(),
        }
    }
    fn manager(auth: CodexAuth, fixed: Option<FixedPrincipal>) -> Arc<AuthManager> {
        let manager = AuthManager::from_auth_for_testing(auth);
        *manager.external_auth.write().unwrap() = Some(Arc::new(FixedAuthority(fixed)));
        manager
    }
    #[test]
    fn proven_opaque_publication_preserves_owner_but_keeps_other_revision_updates() {
        let manager = manager(CodexAuth::from_api_key("old"), Some(principal()));
        let snapshot = manager.owner_snapshot().unwrap();
        let binding = manager.renewal_binding().unwrap();
        assert!(
            manager
                .publish_auth_for_owner(&snapshot, &binding, CodexAuth::from_api_key("renewed"))
                .unwrap()
        );
        assert_eq!(
            manager.auth_cached().unwrap().get_token().unwrap(),
            "renewed"
        );
        let after = manager.owner_snapshot().unwrap();
        assert_eq!(
            after.revisions.owner_generation,
            snapshot.revisions.owner_generation
        );
        assert_eq!(
            after.revisions.generation,
            snapshot.revisions.generation + 1
        );
        assert_eq!(
            after.revisions.source_generation,
            snapshot.revisions.source_generation + 1
        );
        assert_eq!(
            after.revisions.login_generation,
            snapshot.revisions.login_generation + 1
        );
    }
    #[test]
    fn late_result_cannot_overwrite_newer_cached_credential() {
        let manager = manager(CodexAuth::from_api_key("old"), Some(principal()));
        let snapshot = manager.owner_snapshot().unwrap();
        let binding = manager.renewal_binding().unwrap();
        manager.set_cached_auth(Some(CodexAuth::from_api_key("newer")));
        let before = manager.auth_cached().unwrap().get_token().unwrap();
        let revisions = manager.owner_snapshot().unwrap();
        assert!(
            manager
                .publish_auth_for_owner(&snapshot, &binding, CodexAuth::from_api_key("late"))
                .is_err()
        );
        assert_eq!(manager.auth_cached().unwrap().get_token().unwrap(), before);
        assert_eq!(manager.owner_snapshot().unwrap(), revisions);
    }
    #[test]
    fn replaced_authority_refuses_even_same_principal_and_mode() {
        let manager = manager(CodexAuth::from_api_key("old"), Some(principal()));
        let snapshot = manager.owner_snapshot().unwrap();
        let binding = manager.renewal_binding().unwrap();
        *manager.external_auth.write().unwrap() = Some(Arc::new(FixedAuthority(Some(principal()))));
        assert!(
            manager
                .publish_auth_for_owner(&snapshot, &binding, CodexAuth::from_api_key("new"))
                .is_err()
        );
        assert_eq!(manager.owner_snapshot().unwrap(), snapshot);
    }
    #[test]
    fn opaque_credential_without_fixed_principal_is_not_owner_proven() {
        let manager = manager(CodexAuth::from_api_key("old"), None);
        let snapshot = manager.owner_snapshot().unwrap();
        let binding = manager.renewal_binding().unwrap();
        assert!(
            manager
                .publish_auth_for_owner(&snapshot, &binding, CodexAuth::from_api_key("new"))
                .is_err()
        );
        assert_eq!(manager.owner_snapshot().unwrap(), snapshot);
    }
    #[test]
    fn renewal_must_keep_captured_credential_mode() {
        let manager = manager(CodexAuth::from_api_key("old"), Some(principal()));
        let snapshot = manager.owner_snapshot().unwrap();
        let binding = manager.renewal_binding().unwrap();
        let auth = super::super::command_cache_publication_tests::record(1)
            .auth()
            .unwrap();
        assert!(
            manager
                .publish_auth_for_owner(&snapshot, &binding, auth)
                .is_err()
        );
        assert_eq!(manager.owner_snapshot().unwrap(), snapshot);
    }
    #[test]
    fn carried_principal_mismatch_refuses_without_revision_change() {
        let auth = super::super::command_cache_publication_tests::record(1)
            .auth()
            .unwrap();
        let manager = manager(
            auth.clone(),
            Some(FixedPrincipal {
                chatgpt_user_id: "wrong".into(),
                account_id: "account".into(),
            }),
        );
        let snapshot = manager.owner_snapshot().unwrap();
        let binding = manager.renewal_binding().unwrap();
        assert!(
            manager
                .publish_auth_for_owner(&snapshot, &binding, auth)
                .is_err()
        );
        assert_eq!(manager.owner_snapshot().unwrap(), snapshot);
    }
    #[test]
    fn ordinary_identity_free_replacement_still_advances_owner() {
        let manager = AuthManager::from_auth_for_testing(CodexAuth::from_api_key("old"));
        let before = manager.owner_snapshot().unwrap();
        manager.set_cached_auth(Some(CodexAuth::from_api_key("new")));
        assert_eq!(
            manager.owner_snapshot().unwrap().revisions.owner_generation,
            before.revisions.owner_generation + 1
        );
    }
}

impl AuthManager {
    /// Renew through the exact admitted host authority. The caller retains the
    /// finite account-work root; no detached task or replacement ledger is born.
    pub(super) async fn refresh_external_for_owner(
        &self,
        expected: &super::super::AuthOwnerSnapshot,
        renewal: &AuthRenewalBinding,
        failed: Option<AuthCredentialFingerprint>,
    ) -> Result<(), RefreshTokenError> {
        let refused =
            || permanent_external_auth_error("owner renewal authority changed or unproven");
        let _refresh = self.refresh_lock.acquire().await.map_err(|_| refused())?;
        let (attempted, revisions, live) = self.renewal_snapshot().ok_or_else(refused)?;
        let attempted = attempted.ok_or_else(refused)?;
        if revisions != expected.revisions
            || live != *renewal
            || renewal.source.is_none()
            || attempted.api_auth_mode() != expected.auth_mode.ok_or_else(refused)?
            || attempted.get_account_id() != expected.account_id
            || attempted.get_chatgpt_user_id() != expected.chatgpt_user_id
        {
            return Err(refused());
        }
        // Prove principal coverage before invoking an external callback.
        let carried = FixedPrincipal::carried_by(&attempted);
        match (&renewal.fixed_principal, carried) {
            (Some(fixed), Some(carried)) if *fixed != carried => return Err(refused()),
            (None, None) => return Err(refused()),
            (Some(fixed), _)
                if fixed.chatgpt_user_id.trim().is_empty()
                    || fixed.account_id.trim().is_empty() =>
            {
                return Err(refused());
            }
            _ => (),
        }
        if let Some(source) = &self.command_source {
            self.require_command_mode_allowed()?;
            let failed = failed.ok_or_else(|| {
                permanent_external_auth_error("rejected command credential was not recorded")
            })?;
            let auth = source
                .obtain(Some(failed.0))
                .await
                .map_err(RefreshTokenError::Transient)?;
            self.validate_external_auth(&auth, source)?;
            // obtain already publishes through source/native/cache custody.
            // Do not re-publish against the now-obsolete credential revision.
            let current = self
                .auth_for_owner(expected.revisions.owner_generation)
                .map_err(|_| refused())?
                .ok_or_else(refused)?;
            if self.renewal_binding().as_ref() != Some(renewal)
                || !Self::auths_equal_for_refresh(Some(&auth), Some(&current))
            {
                return Err(refused());
            }
            return Ok(());
        }
        let source = renewal.source.as_ref().ok_or_else(refused)?;
        let refreshed = source
            .refresh(ExternalAuthRefreshContext {
                reason: ExternalAuthRefreshReason::Unauthorized,
                previous_account_id: attempted.get_account_id(),
            })
            .await
            .map_err(|error| source.classify_error(error));
        if let Err(RefreshTokenError::Permanent(error)) = &refreshed {
            self.record_bound_refresh_failure(expected, renewal, &attempted, error);
        }
        let auth = refreshed?;
        self.validate_external_auth(&auth, source.as_ref())?;
        self.publish_auth_for_owner(expected, renewal, auth)?;
        Ok(())
    }
}

#[cfg(test)]
mod invocation_tests {
    use super::*;
    struct PendingAuthority {
        started: tokio::sync::Notify,
        release: tokio::sync::Notify,
        calls: std::sync::atomic::AtomicUsize,
    }
    impl ExternalAuth for PendingAuthority {
        fn resolve(&self) -> ExternalAuthFuture<'_, CodexAuth> {
            Box::pin(async { Err(std::io::Error::other("unexpected resolve")) })
        }
        fn refresh(&self, _: ExternalAuthRefreshContext) -> ExternalAuthFuture<'_, CodexAuth> {
            Box::pin(async {
                self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                self.started.notify_one();
                self.release.notified().await;
                Ok(CodexAuth::from_api_key("renewed"))
            })
        }
        fn fixed_principal(&self) -> Option<FixedPrincipal> {
            Some(FixedPrincipal {
                chatgpt_user_id: "principal".into(),
                account_id: "account".into(),
            })
        }
    }
    fn pending() -> (Arc<AuthManager>, Arc<PendingAuthority>) {
        let manager = AuthManager::from_auth_for_testing(CodexAuth::from_api_key("old"));
        let authority = Arc::new(PendingAuthority {
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        *manager.external_auth.write().unwrap() = Some(authority.clone());
        (manager, authority)
    }
    fn begin(manager: &Arc<AuthManager>) -> tokio::task::JoinHandle<Result<(), RefreshTokenError>> {
        let snapshot = manager.owner_snapshot().unwrap();
        let binding = manager.renewal_binding().unwrap();
        let manager = manager.clone();
        tokio::spawn(async move {
            manager
                .refresh_external_for_owner(&snapshot, &binding, None)
                .await
        })
    }
    async fn started(authority: &PendingAuthority) {
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            authority.started.notified(),
        )
        .await
        .unwrap();
    }
    async fn finished(
        task: tokio::task::JoinHandle<Result<(), RefreshTokenError>>,
    ) -> Result<(), RefreshTokenError> {
        tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
    }
    #[tokio::test]
    async fn same_admitted_host_can_publish_after_actual_callback() {
        let (manager, authority) = pending();
        let before = manager.owner_snapshot().unwrap();
        let task = begin(&manager);
        started(&authority).await;
        authority.release.notify_one();
        finished(task).await.unwrap();
        assert_eq!(
            manager.auth_cached().unwrap().get_token().unwrap(),
            "renewed"
        );
        assert_eq!(
            manager.owner_snapshot().unwrap().revisions.owner_generation,
            before.revisions.owner_generation
        );
        assert_eq!(authority.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn owner_change_during_actual_callback_refuses_late_result() {
        let (manager, authority) = pending();
        let task = begin(&manager);
        started(&authority).await;
        manager.set_cached_auth(Some(CodexAuth::from_api_key("new-owner")));
        let before = manager.owner_snapshot().unwrap();
        authority.release.notify_one();
        assert!(finished(task).await.is_err());
        assert_eq!(
            manager.auth_cached().unwrap().get_token().unwrap(),
            "new-owner"
        );
        assert_eq!(manager.owner_snapshot().unwrap(), before);
    }
    #[tokio::test]
    async fn source_replacement_during_actual_callback_refuses_late_result() {
        let (manager, authority) = pending();
        let task = begin(&manager);
        started(&authority).await;
        *manager.external_auth.write().unwrap() = Some(Arc::new(FixedApiKeyAuth(
            CodexAuth::from_api_key("replacement"),
        )));
        let before = manager.owner_snapshot().unwrap();
        authority.release.notify_one();
        assert!(finished(task).await.is_err());
        assert_eq!(manager.auth_cached().unwrap().get_token().unwrap(), "old");
        assert_eq!(manager.owner_snapshot().unwrap(), before);
    }
    #[tokio::test]
    async fn cancelled_callback_releases_existing_refresh_gate() {
        let (manager, authority) = pending();
        let task = begin(&manager);
        started(&authority).await;
        task.abort();
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .unwrap();
        assert!(result.unwrap_err().is_cancelled());
        assert!(manager.refresh_lock.try_acquire().is_ok());
        assert_eq!(manager.auth_cached().unwrap().get_token().unwrap(), "old");
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn command_bound_invocation_refuses_missing_proof_without_mutation() {
        use super::super::command_cache_publication_tests::{record, selected_fixture};
        let home = tempfile::tempdir().unwrap();
        let (_, manager) = selected_fixture(home.path(), record(1));
        let before = manager.owner_snapshot().unwrap();
        let binding = manager.renewal_binding().unwrap();
        let bytes = std::fs::read(home.path().join("auth.json")).unwrap();
        assert!(
            manager
                .refresh_external_for_owner(&before, &binding, None)
                .await
                .is_err()
        );
        assert_eq!(manager.owner_snapshot().unwrap(), before);
        assert_eq!(std::fs::read(home.path().join("auth.json")).unwrap(), bytes);
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn command_old_request_proof_accepts_already_published_current_record() {
        use super::super::command_cache_publication_tests::{record, selected_fixture};
        let home = tempfile::tempdir().unwrap();
        let (_, manager) = selected_fixture(home.path(), record(1));
        let before = manager.owner_snapshot().unwrap();
        let binding = manager.renewal_binding().unwrap();
        let failed = AuthCredentialFingerprint::from_bearer_header("Bearer older-synthetic-token");
        manager
            .refresh_external_for_owner(&before, &binding, failed)
            .await
            .unwrap();
        assert_eq!(manager.owner_snapshot().unwrap(), before);
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn command_current_rejection_cannot_claim_same_cache_recovery() {
        use super::super::command_cache_publication_tests::{record, selected_fixture};
        let home = tempfile::tempdir().unwrap();
        let (source, manager) = selected_fixture(home.path(), record(1));
        let before = manager.owner_snapshot().unwrap();
        let binding = manager.renewal_binding().unwrap();
        let auth = manager.auth_cached().unwrap();
        let failed = AuthCredentialFingerprint::from_bearer_header(&format!(
            "Bearer {}",
            auth.get_token().unwrap()
        ));
        assert!(
            manager
                .refresh_external_for_owner(&before, &binding, failed)
                .await
                .is_err()
        );
        assert!(!source.usable_auth(Some(&auth)));
        assert_eq!(manager.owner_snapshot().unwrap(), before);
    }
}
