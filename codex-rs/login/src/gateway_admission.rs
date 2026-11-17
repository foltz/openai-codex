//! Captured gateway state shared with primary owner-pinned authentication.
//! Resolution and enforcement remain in the actual gateway provider.

/// Token-free gateway state captured with the actual selected credential.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GatewayAdmission {
    NotConfigured,
    Unbound,
    Bound(crate::OAuthSessionBinding),
}
