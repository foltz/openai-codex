//! Resolves credentials for a gateway, using its configured exporter or OAuth session.

use std::sync::Arc;

use crate::ProviderUnauthorizedRecovery;
use crate::gateway_export::GatewayExport;
use codex_login::AuthManager;
use codex_login::GatewayAuthError;
use codex_login::OAuthSessionManager;
use codex_model_provider_info::GatewayOAuthConfig;
use codex_model_provider_info::ModelProviderInfo;
use codex_protocol::error::CodexErr;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::error::Result;

/// Token-free gateway state captured with the actual selected credential.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GatewayAdmission {
    NotConfigured,
    Unbound,
    Bound(codex_login::OAuthSessionBinding),
}

/// Resolves gateway request credentials and exposes the optional interactive OAuth session.
#[derive(Debug)]
pub struct GatewayAuthManager(GatewayCredentialSource);

#[derive(Debug)]
enum GatewayCredentialSource {
    Export(Arc<GatewayExport>),
    OAuth(Arc<OAuthSessionManager>),
}

impl GatewayAuthManager {
    pub(crate) fn new(
        provider: &ModelProviderInfo,
        config: &GatewayOAuthConfig,
        owner: &Arc<AuthManager>,
    ) -> std::result::Result<Self, String> {
        let state = crate::shared_state::process_shared_state();
        let source = match config {
            GatewayOAuthConfig::Export {
                credential_export, ..
            } => GatewayCredentialSource::Export(state.gateway_export(
                provider,
                credential_export,
                owner,
            )),
            GatewayOAuthConfig::OAuth(settings) => GatewayCredentialSource::OAuth(
                state
                    .gateway_oauth_session(settings, &owner.runtime_config())
                    .map_err(|_| "failed to create provider OAuth HTTP client".to_string())?,
            ),
        };
        Ok(Self(source))
    }

    pub(crate) async fn prepare_retry(&self) -> ProviderUnauthorizedRecovery {
        match &self.0 {
            GatewayCredentialSource::Export(export) => {
                export.invalidate().await;
                ProviderUnauthorizedRecovery::Recovered
            }
            GatewayCredentialSource::OAuth(_) => ProviderUnauthorizedRecovery::NotConfigured,
        }
    }

    /// The session used for explicit sign-in and OAuth status, when configured.
    pub fn oauth_session(&self) -> Option<Arc<OAuthSessionManager>> {
        match &self.0 {
            GatewayCredentialSource::OAuth(session) => Some(Arc::clone(session)),
            GatewayCredentialSource::Export(_) => None,
        }
    }

    pub(crate) fn without_interactive_login(self: &Arc<Self>) -> Arc<Self> {
        let GatewayCredentialSource::OAuth(session) = &self.0 else {
            return Arc::clone(self);
        };
        Arc::new(Self(GatewayCredentialSource::OAuth(Arc::new(
            session.as_ref().clone().without_interactive_login(),
        ))))
    }

    pub async fn resolve_access_token_observed(&self) -> Result<(Arc<str>, GatewayAdmission)> {
        match &self.0 {
            GatewayCredentialSource::Export(export) => export
                .token()
                .await
                .map(|token| (token, GatewayAdmission::Unbound)),
            GatewayCredentialSource::OAuth(session) => {
                let credentials = session
                    .resolve_access_token_observed()
                    .await
                    .map_err(gateway_auth_error)?;
                let admission = credentials
                    .binding()
                    .cloned()
                    .map_or(GatewayAdmission::Unbound, GatewayAdmission::Bound);
                Ok((Arc::from(credentials.access_token), admission))
            }
        }
    }
    /// Noninteractive admitted OAuth use; exporter admitted support is pending.
    pub async fn resolve_access_token_for(&self, admission: &GatewayAdmission) -> Result<Arc<str>> {
        let (GatewayCredentialSource::OAuth(session), GatewayAdmission::Bound(binding)) =
            (&self.0, admission)
        else {
            return Err(CodexErr::InvalidRequest(
                "gateway sign-in is not bound to the originating work".into(),
            ));
        };
        let mut request = session
            .begin_admitted_request(binding)
            .map_err(gateway_auth_error)?;
        Ok(Arc::from(
            request
                .credentials()
                .await
                .map_err(gateway_auth_error)?
                .access_token,
        ))
    }
    pub async fn resolve_access_token(&self) -> Result<Arc<str>> {
        match &self.0 {
            GatewayCredentialSource::Export(export) => export.token().await,
            GatewayCredentialSource::OAuth(session) => session
                .resolve_access_token()
                .await
                .map(Arc::from)
                .map_err(gateway_auth_error),
        }
    }
}

fn gateway_auth_error(error: std::io::Error) -> CodexErr {
    if let Some(login_error) = error
        .get_ref()
        .and_then(|source| source.downcast_ref::<GatewayAuthError>())
    {
        return match login_error {
            GatewayAuthError::LoginRequired => {
                CodexErrorDetails::GatewayLoginRequired(login_error.to_string()).into()
            }
            GatewayAuthError::LoginInProgress => CodexErr::InvalidRequest(login_error.to_string()),
        };
    }
    // Issuer errors may echo arbitrary credentials from configured URLs. Keep the
    // diagnostic safe and bounded, preserving retries for transient failures.
    CodexErr::Io(std::io::Error::new(
        error.kind(),
        "Gateway OAuth authentication failed; check the gateway configuration and credential store.",
    ))
}

#[cfg(test)]
#[path = "gateway_admission_tests.rs"]
mod admission_tests;
