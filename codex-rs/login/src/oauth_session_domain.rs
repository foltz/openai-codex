//! Shared token-free sign-in continuity. Native IO must hold its state guard.
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct GrantIdentity {
    pub authorization_id: Option<String>,
    pub principal: Option<String>,
}
#[derive(Debug, Default)]
pub(super) struct CredentialDomain {
    state: Mutex<DomainState>,
}
#[derive(Debug, Default)]
pub(super) struct DomainState {
    identity: Option<GrantIdentity>,
    epoch: u64,
    closed: bool,
}
impl DomainState {
    pub fn prepare(&mut self, identity: &Option<GrantIdentity>) -> io::Result<()> {
        if self.closed || (self.identity != *identity && self.epoch == u64::MAX) {
            self.closed = true;
            return Err(unavailable());
        }
        Ok(())
    }

    /// Call inside the actual storage read/save/delete critical section. Never
    /// install a stale load after releasing the storage/domain serialization.
    pub fn observe(&mut self, identity: Option<GrantIdentity>) -> io::Result<()> {
        self.prepare(&identity)?;
        if self.identity != identity {
            let Some(epoch) = self.epoch.checked_add(1) else {
                self.closed = true;
                return Err(unavailable());
            };
            self.epoch = epoch;
            self.identity = identity;
        }
        Ok(())
    }
}
fn unavailable() -> io::Error {
    io::Error::other("provider OAuth sign-in continuity unavailable")
}
static DOMAINS: OnceLock<Mutex<HashMap<String, Weak<CredentialDomain>>>> = OnceLock::new();
impl CredentialDomain {
    pub fn shared(credential_id: String) -> io::Result<Arc<Self>> {
        let mut domains = DOMAINS
            .get_or_init(Mutex::default)
            .lock()
            .map_err(|_| unavailable())?;
        domains.retain(|_, domain| domain.strong_count() > 0);
        if let Some(domain) = domains.get(&credential_id).and_then(Weak::upgrade) {
            return Ok(domain);
        }
        let domain = Arc::new(Self::default());
        domains.insert(credential_id, Arc::downgrade(&domain));
        Ok(domain)
    }
    #[cfg(test)]
    pub fn is_locked(&self) -> bool {
        matches!(
            self.state.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        )
    }
    pub fn lock(&self) -> io::Result<MutexGuard<'_, DomainState>> {
        let guard = self.state.lock().map_err(|_| unavailable())?;
        if guard.closed {
            return Err(unavailable());
        }
        Ok(guard)
    }
    pub fn binding(&self, expected: &GrantIdentity) -> Option<u64> {
        let state = self.lock().ok()?;
        (state.identity.as_ref() == Some(expected)).then_some(state.epoch)
    }
    pub fn is_current(&self, expected: &GrantIdentity, epoch: u64) -> bool {
        self.binding(expected) == Some(epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn identity(id: &str) -> GrantIdentity {
        GrantIdentity {
            authorization_id: Some(id.into()),
            principal: Some("principal".into()),
        }
    }
    #[test]
    fn same_grant_renewal_preserves_epoch_but_sign_in_aba_does_not() {
        let domain = CredentialDomain::default();
        let origin = identity("origin");
        domain
            .lock()
            .unwrap()
            .observe(Some(origin.clone()))
            .unwrap();
        let epoch = domain.binding(&origin).unwrap();
        domain
            .lock()
            .unwrap()
            .observe(Some(origin.clone()))
            .unwrap();
        assert!(domain.is_current(&origin, epoch));
        domain
            .lock()
            .unwrap()
            .observe(Some(identity("replacement")))
            .unwrap();
        domain
            .lock()
            .unwrap()
            .observe(Some(origin.clone()))
            .unwrap();
        assert!(!domain.is_current(&origin, epoch));
    }
    #[test]
    fn principal_change_and_logout_retire_original_binding() {
        let domain = CredentialDomain::default();
        let origin = identity("grant");
        domain
            .lock()
            .unwrap()
            .observe(Some(origin.clone()))
            .unwrap();
        let epoch = domain.binding(&origin).unwrap();
        let mut changed = origin.clone();
        changed.principal = Some("another".into());
        domain.lock().unwrap().observe(Some(changed)).unwrap();
        assert!(!domain.is_current(&origin, epoch));
        domain.lock().unwrap().observe(None).unwrap();
        assert!(domain.binding(&origin).is_none());
    }
    #[test]
    fn observed_domain_is_shared_while_any_owner_retains_it() {
        let key = format!("synthetic-domain-{}", std::process::id());
        let first = CredentialDomain::shared(key.clone()).unwrap();
        let retained = first.clone();
        drop(first);
        let second = CredentialDomain::shared(key).unwrap();
        assert!(Arc::ptr_eq(&retained, &second));
    }
    #[test]
    fn exhausted_epoch_closes_permanently_without_wrapping() {
        let domain = CredentialDomain::default();
        {
            let mut state = domain.lock().unwrap();
            state.epoch = u64::MAX;
            assert!(state.observe(Some(identity("new"))).is_err());
        }
        assert!(domain.lock().is_err());
        assert!(!domain.is_current(&identity("new"), 0));
    }
    #[test]
    fn poisoned_domain_is_unavailable_not_new_authority() {
        let domain = CredentialDomain::default();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _state = domain.state.lock().unwrap();
            panic!("synthetic poison");
        }));
        assert!(domain.lock().is_err());
        assert!(domain.binding(&identity("grant")).is_none());
    }
}
