//! A detached caller retains one source owner; it cannot select or mutate another.
use super::super::AuthOwnerChanged;
use super::super::AuthOwnerSnapshot;
use super::*;
use crate::GatewayAdmission;

#[derive(Debug)]
pub(super) struct OwnerPin {
    pub(super) source: Arc<AuthManager>,
    expected: AuthOwnerSnapshot,
    pub(super) renewal: AuthRenewalBinding,
    gateway: GatewayAdmission,
}
impl OwnerPin {
    fn validate(
        &self,
        auth: &Option<CodexAuth>,
        revisions: AuthChangeState,
    ) -> Result<(), AuthOwnerChanged> {
        if revisions.owner_generation != self.expected.revisions.owner_generation
            || auth.as_ref().map(CodexAuth::api_auth_mode) != self.expected.auth_mode
            || auth.as_ref().and_then(CodexAuth::get_account_id) != self.expected.account_id
            || auth.as_ref().and_then(CodexAuth::get_chatgpt_user_id)
                != self.expected.chatgpt_user_id
        {
            return Err(AuthOwnerChanged);
        }
        if self.renewal.is_host() {
            if !auth
                .as_ref()
                .is_some_and(|auth| self.renewal.covers_owner(auth))
            {
                return Err(AuthOwnerChanged);
            }
        } else if revisions.source_generation != self.expected.revisions.source_generation
            || revisions.login_generation != self.expected.revisions.login_generation
        {
            return Err(AuthOwnerChanged);
        }
        Ok(())
    }

    /// Pure cached observation: no storage load, provider call or credential refresh.
    pub(super) fn cached_auth(&self) -> Result<Option<CodexAuth>, AuthOwnerChanged> {
        let modes = self
            .source
            .allowed_auth_modes
            .read()
            .map_err(|_| AuthOwnerChanged)?;
        let authority = self
            .source
            .external_auth
            .read()
            .map_err(|_| AuthOwnerChanged)?;
        if !self.renewal.matches_provider(&authority) {
            return Err(AuthOwnerChanged);
        }
        let cached = self.source.inner.read().map_err(|_| AuthOwnerChanged)?;
        if cached.initial_load_failed {
            return Err(AuthOwnerChanged);
        }
        self.validate(&cached.auth, *self.source.auth_change_state_tx.borrow())?;
        if let Some(auth) = cached.auth.as_ref() {
            if modes
                .as_ref()
                .is_some_and(|modes| !modes.contains(&auth.api_auth_mode()))
            {
                return Err(AuthOwnerChanged);
            }
            validate_auth_restrictions(
                Some(&self.source.allowed_login_methods()),
                self.source.effective_chatgpt_workspaces().as_deref(),
                auth,
            )
            .map_err(|_| AuthOwnerChanged)?;
        }
        Ok(cached.auth.clone())
    }

    pub(super) fn snapshot(
        &self,
    ) -> Result<(Option<CodexAuth>, AuthOwnerSnapshot), AuthOwnerChanged> {
        let (auth, revisions, renewal) = self.source.renewal_snapshot().ok_or(AuthOwnerChanged)?;
        if renewal != self.renewal {
            return Err(AuthOwnerChanged);
        }
        self.validate(&auth, revisions)?;
        // Recheck current policy without turning a cached observation into a reload.
        self.cached_auth()?;
        let snapshot = AuthOwnerSnapshot {
            auth_mode: auth.as_ref().map(CodexAuth::api_auth_mode),
            chatgpt_user_id: auth.as_ref().and_then(CodexAuth::get_chatgpt_user_id),
            account_id: auth.as_ref().and_then(CodexAuth::get_account_id),
            revisions,
        };
        Ok((auth, snapshot))
    }
    pub(super) fn capture(&self) -> Result<Option<CodexAuth>, AuthOwnerChanged> {
        let (auth, _) = self.snapshot()?;
        if let Some(source) = &self.source.command_source {
            if !source.usable_auth(auth.as_ref()) {
                return Err(AuthOwnerChanged);
            }
        }
        Ok(auth)
    }
}
impl AuthManager {
    /// Capture identity, upstream revisions and renewal authority under the same
    /// source cache custody. Pinned callers retain the original source revisions.
    pub fn owner_admission_snapshot(&self) -> Option<(AuthOwnerSnapshot, AuthRenewalBinding)> {
        if let Some(pin) = &self.owner_pin {
            let (_, snapshot) = pin.snapshot().ok()?;
            return Some((snapshot, pin.renewal.clone()));
        }
        let (auth, revisions, renewal) = self.renewal_snapshot()?;
        Some((
            AuthOwnerSnapshot {
                auth_mode: auth.as_ref().map(CodexAuth::api_auth_mode),
                chatgpt_user_id: auth.as_ref().and_then(CodexAuth::get_chatgpt_user_id),
                account_id: auth.as_ref().and_then(CodexAuth::get_account_id),
                revisions,
            },
            renewal,
        ))
    }

    /// Recheck the admitted owner without reloading or resolving credentials.
    /// Credential-only revision changes are allowed; source/login/owner changes are not.
    pub fn verify_owner_binding(
        self: &Arc<Self>,
        expected: &AuthOwnerSnapshot,
        renewal: &AuthRenewalBinding,
    ) -> Result<(), AuthOwnerChanged> {
        if let Some(pin) = &self.owner_pin {
            pin.cached_auth()?;
        }
        let source = self.owner_pin_source().unwrap_or_else(|| Arc::clone(self));
        OwnerPin {
            source,
            expected: expected.clone(),
            renewal: renewal.clone(),
            gateway: GatewayAdmission::NotConfigured,
        }
        .cached_auth()?;
        Ok(())
    }

    /// Retain one admitted source owner; credential renewal cannot select a new login.
    pub fn owner_pinned(
        source: &Arc<Self>,
        expected: &AuthOwnerSnapshot,
        renewal: &AuthRenewalBinding,
        gateway: GatewayAdmission,
    ) -> Result<Arc<Self>, AuthOwnerChanged> {
        if source.owner_pin.is_some() {
            return Err(AuthOwnerChanged);
        }
        let pin = Arc::new(OwnerPin {
            source: Arc::clone(source),
            expected: expected.clone(),
            renewal: renewal.clone(),
            gateway,
        });
        let auth = pin.capture()?;
        let modes = source
            .allowed_auth_modes
            .read()
            .map_err(|_| AuthOwnerChanged)?
            .clone();
        let workspaces = source
            .forced_chatgpt_workspace_id
            .read()
            .map_err(|_| AuthOwnerChanged)?
            .clone();
        let mut manager = Self::from_cached_auth(auth, None, source.runtime_config(), false);
        let m = Arc::get_mut(&mut manager).ok_or(AuthOwnerChanged)?;
        m.enable_codex_api_key_env = false;
        m.auth_credentials_store_mode = source.auth_credentials_store_mode;
        m.keyring_backend_kind = source.keyring_backend_kind;
        m.forced_login_method = source.forced_login_method;
        *m.forced_chatgpt_workspace_id
            .get_mut()
            .map_err(|_| AuthOwnerChanged)? = workspaces;
        m.managed_auth_policy = source.managed_auth_policy.clone();
        m.chatgpt_base_url = source.chatgpt_base_url.clone();
        m.agent_identity_authapi_base_url = source.agent_identity_authapi_base_url.clone();
        *m.allowed_auth_modes
            .get_mut()
            .map_err(|_| AuthOwnerChanged)? = modes;
        m.workload_identity_selected = source.workload_identity_selected;
        if let Some(resolver) = source.workspace_routing_resolver.get() {
            let _ = m.workspace_routing_resolver.set(resolver.clone());
        }
        m.owner_pin = Some(pin.clone());
        pin.capture()?;
        pin.capture()?;
        Ok(manager)
    }
    pub fn owner_pin_gateway(&self) -> Option<GatewayAdmission> {
        self.owner_pin.as_ref().map(|p| p.gateway.clone())
    }
    pub fn owner_pin_renewal(&self) -> Option<AuthRenewalBinding> {
        self.owner_pin.as_ref().map(|p| p.renewal.clone())
    }
    pub fn owner_pin_source(&self) -> Option<Arc<Self>> {
        self.owner_pin.as_ref().map(|p| Arc::clone(&p.source))
    }
    pub(super) fn sync_pinned_owner(&self, pin: &OwnerPin) -> Result<bool, AuthOwnerChanged> {
        let auth = match pin.capture() {
            Ok(auth) => auth,
            Err(error) => {
                if let Ok(mut cached) = self.inner.write() {
                    self.set_cached_auth_locked(&mut cached, None);
                }
                return Err(error);
            }
        };
        let changed = {
            let mut cached = self.inner.write().map_err(|_| AuthOwnerChanged)?;
            self.set_cached_auth_locked_with_owner_proof(&mut cached, auth, true)
        };
        // Never hand out a local copy after its source stopped being the admitted owner.
        if pin.cached_auth().is_err() {
            if let Ok(mut cached) = self.inner.write() {
                self.set_cached_auth_locked(&mut cached, None);
            }
            return Err(AuthOwnerChanged);
        }
        Ok(changed)
    }
    pub(super) async fn refresh_pinned_owner(
        &self,
        pin: &OwnerPin,
        reason: RefreshReason,
        failed: Option<AuthCredentialFingerprint>,
    ) -> Result<(), RefreshTokenError> {
        let refused = || permanent_external_auth_error("pinned authentication owner changed");
        let (_, expected) = pin.snapshot().map_err(|_| refused())?;
        let result = if pin.renewal.is_host() {
            pin.source
                .refresh_external_for_owner(&expected, &pin.renewal, failed)
                .await
        } else {
            pin.source
                .refresh_native_for_owner(&expected, &pin.renewal, reason)
                .await
        };
        result?;
        self.sync_pinned_owner(pin).map_err(|_| refused())?;
        Ok(())
    }
    pub(super) async fn pinned_auth(&self, pin: &OwnerPin) -> Option<CodexAuth> {
        let cached = pin.capture().ok().flatten()?;
        if let Some(reason) = Self::proactive_refresh_reason(&cached) {
            if let Err(error) = self.refresh_pinned_owner(pin, reason, None).await {
                tracing::warn!("owner-pinned token refresh refused: {error}");
            }
        }
        self.sync_pinned_owner(pin).ok()?;
        pin.capture().ok().flatten()
    }
    pub(super) fn require_unpinned_mutation(&self) -> std::io::Result<()> {
        if self.owner_pin.is_some() {
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "owner-pinned auth cannot select, replace or persist a new authority",
            ))
        } else {
            Ok(())
        }
    }
    pub(super) fn agent_identity_bootstrap_owner(&self) -> &Self {
        self.owner_pin.as_ref().map_or(self, |p| p.source.as_ref())
    }
    pub(super) fn recovery_command_source(
        &self,
    ) -> Option<&Arc<super::super::command_source::CommandSource>> {
        self.owner_pin
            .as_ref()
            .map_or(self.command_source.as_ref(), |p| {
                p.source.command_source.as_ref()
            })
    }
}
