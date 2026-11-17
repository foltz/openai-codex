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
