//! Resolves and caches the configured gateway command's credential for this provider.

use std::fmt;
use std::sync::Arc;

use codex_http_client::NetworkPolicy;
use codex_model_provider_info::CredentialExportConfig;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result;
use tokio::sync::Mutex;

pub(crate) struct GatewayExport {
    command: CredentialExportConfig,
    policy: NetworkPolicy,
    source: Arc<()>,
    owner: std::sync::Weak<codex_login::AuthManager>,
    cached: Mutex<ExportState>,
}

#[derive(Default)]
struct ExportState {
    generation: u64,
    credential: Option<ExportCredential>,
}
struct ExportCredential {
    token: Arc<str>,
    owner: codex_login::AuthOwnerSnapshot,
    renewal: codex_login::AuthRenewalBinding,
    policy: NetworkPolicy,
}
fn unavailable() -> CodexErr {
    CodexErr::InvalidRequest(
        "gateway export source, account or credential changed since admission".into(),
    )
}

impl fmt::Debug for GatewayExport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GatewayExport").finish_non_exhaustive()
    }
}

impl GatewayExport {
    pub(crate) fn new(
        command: CredentialExportConfig,
        policy: NetworkPolicy,
        owner: &Arc<codex_login::AuthManager>,
    ) -> Self {
        Self {
            command,
            policy,
            source: Arc::new(()),
            owner: Arc::downgrade(owner),
            cached: Mutex::default(),
        }
    }

    pub(crate) async fn invalidate(&self) {
        self.cached.lock().await.credential = None;
    }

    fn valid(&self, credential: &ExportCredential) -> bool {
        self.owner.upgrade().is_some_and(|owner| {
            owner
                .verify_owner_binding(&credential.owner, &credential.renewal)
                .is_ok()
                && credential.policy == self.policy.clone().for_current_account()
        })
    }

    pub(crate) fn observed_binding(&self) -> Option<codex_login::GatewayExportBinding> {
        let state = self.cached.try_lock().ok()?;
        if !self.valid(state.credential.as_ref()?) {
            return None;
        }
        Some(codex_login::GatewayExportBinding::new(
            Arc::clone(&self.source),
            state.generation,
        ))
    }

    /// Detached use cannot execute a command or refresh an unidentifiable grant.
    pub(crate) async fn token_for(
        &self,
        binding: &codex_login::GatewayExportBinding,
    ) -> Result<Arc<str>> {
        let state = self.cached.lock().await;
        if !binding.matches(&self.source, state.generation) {
            return Err(unavailable());
        }
        let cached = state.credential.as_ref().ok_or_else(unavailable)?;
        if !self.valid(cached) {
            return Err(unavailable());
        }
        Ok(Arc::clone(&cached.token))
    }

    pub(crate) async fn token(&self) -> Result<Arc<str>> {
        self.token_observed().await.map(|(token, _)| token)
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "concurrent cache misses must share a single credential export"
    )]
    pub(crate) async fn token_observed(
        &self,
    ) -> Result<(Arc<str>, codex_login::GatewayExportBinding)> {
        let mut state = self.cached.lock().await;
        if let Some(cached) = state.credential.as_ref()
            && self.valid(cached)
        {
            return Ok((
                Arc::clone(&cached.token),
                codex_login::GatewayExportBinding::new(Arc::clone(&self.source), state.generation),
            ));
        }
        state.credential = None;
        let generation = state.generation.checked_add(1).ok_or_else(unavailable)?;
        let owner = self.owner.upgrade().ok_or_else(unavailable)?;
        let (snapshot, renewal) = owner.owner_admission_snapshot().ok_or_else(unavailable)?;
        let policy = self.policy.clone().for_current_account();
        let permit = policy
            .acquire_for_unsupported_sdk()
            .map_err(|error| CodexErr::Fatal(error.to_string()))?;
        let output = permit
            .run(codex_login::run_credential_export(&self.command, "Gateway"))
            .await
            .map_err(|error| CodexErr::Fatal(error.to_string()))?
            .map_err(|error| CodexErr::InvalidRequest(error.to_string()))?;
        let token = std::str::from_utf8(&output)
            .ok()
            .map(str::trim)
            .filter(|value| !value.is_empty() && value.bytes().all(|b| b.is_ascii_graphic()))
            .ok_or_else(|| {
                CodexErr::InvalidRequest(
                    "Gateway credential export must print one access token".into(),
                )
            })?;
        let token = Arc::<str>::from(token);
        let cached = ExportCredential {
            token: Arc::clone(&token),
            owner: snapshot,
            renewal,
            policy,
        };
        if !self.valid(&cached) {
            return Err(unavailable());
        }
        state.generation = generation;
        state.credential = Some(cached);
        Ok((
            token,
            codex_login::GatewayExportBinding::new(Arc::clone(&self.source), generation),
        ))
    }
}

#[cfg(all(test, unix))]
#[path = "gateway_export_tests.rs"]
mod tests;
