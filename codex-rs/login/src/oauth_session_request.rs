//! Coherent request credentials and unauthorized recovery for configured OAuth consumers.
//! The session manager owns observation, refresh, locking, and persistence; a request
//! retains only its rejected token so transport retries cannot forget recovery.

use super::OAuthSessionManager;
use super::RefreshPolicy;
use super::login;
use std::fmt;
use std::io;

/// Token-free identity captured with an actual OAuth credential. Includes the
/// credential store/config domain; this is not a standalone renewal authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OAuthSessionBinding {
    credential_id: String,
    authorization_id: String,
}

impl OAuthSessionManager {
    /// Reuse only the actual origin's store/config and sign-in, noninteractively.
    pub fn begin_admitted_request(
        &self,
        binding: &OAuthSessionBinding,
    ) -> io::Result<OAuthSessionRequest> {
        if self.credential_id() != binding.credential_id {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "provider OAuth credential domain changed since admission",
            ));
        }
        self.begin_bound_request(&binding.authorization_id)
    }
}

/// A token and the identity of the exact grant that supplied it.
pub struct OAuthRequestCredentials {
    pub access_token: String,
    pub authorization_id: Option<String>,
    binding: Option<OAuthSessionBinding>,
}

impl OAuthRequestCredentials {
    pub fn binding(&self) -> Option<&OAuthSessionBinding> {
        self.binding.as_ref()
    }
}

impl fmt::Debug for OAuthRequestCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OAuthRequestCredentials")
            .field("access_token", &"<redacted>")
            .field("authorization_id", &self.authorization_id)
            .finish()
    }
}

/// One consumer request and its retries, using the existing session's credential state.
pub struct OAuthSessionRequest {
    manager: OAuthSessionManager,
    policy: RefreshPolicy,
    sent_token: Option<String>,
}

impl OAuthSessionRequest {
    pub(super) fn new(manager: OAuthSessionManager) -> Self {
        Self {
            manager,
            policy: RefreshPolicy::ObserveStored,
            sent_token: None,
        }
    }

    pub(super) fn new_bound(manager: OAuthSessionManager, authorization_id: String) -> Self {
        Self {
            manager,
            policy: RefreshPolicy::Bound {
                authorization_id,
                rejected_access_token: None,
            },
            sent_token: None,
        }
    }
    /// Observes persisted credentials using the existing session lifecycle. The returned
    /// identity is captured with the token, never read separately after resolution.
    pub async fn credentials(&mut self) -> io::Result<OAuthRequestCredentials> {
        let token = self
            .manager
            .resolve_credentials_with_browser(self.policy.clone(), login::open_browser)
            .await?;
        self.sent_token = Some(token.access_token.clone());
        let binding = token
            .authorization_id
            .as_ref()
            .filter(|id| !id.trim().is_empty())
            .map(|id| OAuthSessionBinding {
                credential_id: self.manager.credential_id(),
                authorization_id: id.clone(),
            });
        Ok(OAuthRequestCredentials {
            access_token: token.access_token,
            authorization_id: token.authorization_id,
            binding,
        })
    }

    /// Marks the sent credential as rejected. Callers bound unauthorized replays;
    /// transient transport failures retain this state until resolution succeeds.
    pub fn reject_credentials(&mut self) -> bool {
        let Some(token) = self.sent_token.take() else {
            return false;
        };
        self.policy = match &self.policy {
            RefreshPolicy::Bound {
                authorization_id, ..
            } => RefreshPolicy::Bound {
                authorization_id: authorization_id.clone(),
                rejected_access_token: Some(token),
            },
            _ => RefreshPolicy::AfterRejection(token),
        };
        true
    }

    /// Identifies credential failures that belong to unauthorized recovery.
    pub fn is_recovering(&self) -> bool {
        matches!(
            &self.policy,
            RefreshPolicy::AfterRejection(_)
                | RefreshPolicy::Bound {
                    rejected_access_token: Some(_),
                    ..
                }
        )
    }
}
