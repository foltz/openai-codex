//! Request-local evidence from authenticated output, never telemetry or a cache.
use codex_api::{
    AuthError, AuthHeaderTelemetry, AuthHeadersFuture, AuthProvider, AuthProviderFuture,
    AuthRefreshFuture, SharedAuthProvider,
};
use codex_http_client::Request;
use codex_login::AuthCredentialFingerprint;
use http::HeaderMap;
use std::sync::{Arc, Mutex};

/// Evidence only; retains no auth provider, credentials or finite account root.
/// Create a fresh pair per API attempt. Never share one recorder between
/// concurrent requests: its snapshot belongs to that attempt's last auth apply.
#[derive(Clone)]
pub struct RequestAuthEvidence(Arc<Mutex<Option<AuthCredentialFingerprint>>>);
impl RequestAuthEvidence {
    pub fn fingerprint(&self) -> Option<AuthCredentialFingerprint> {
        self.0.lock().ok().and_then(|value| value.clone())
    }
    fn clear(&self) {
        if let Ok(mut value) = self.0.lock() {
            *value = None;
        }
    }
    fn record(&self, headers: &HeaderMap) {
        let fingerprint = headers
            .get(http::header::AUTHORIZATION)
            .and_then(|header| header.to_str().ok())
            .and_then(AuthCredentialFingerprint::from_bearer_header);
        if let Ok(mut value) = self.0.lock() {
            *value = fingerprint;
        }
    }
}

struct ObservedRequestAuth {
    inner: SharedAuthProvider,
    evidence: RequestAuthEvidence,
}
impl AuthProvider for ObservedRequestAuth {
    // Sync hints/telemetry must never overwrite evidence of actual async auth.
    fn add_auth_headers(&self, headers: &mut HeaderMap) -> Result<(), AuthError> {
        self.inner.add_auth_headers(headers)
    }
    fn auth_header_telemetry(&self) -> Result<AuthHeaderTelemetry, AuthError> {
        self.inner.auth_header_telemetry()
    }
    fn refresh_after_unauthorized(&self, rejected: HeaderMap) -> AuthRefreshFuture<'_> {
        self.inner.refresh_after_unauthorized(rejected)
    }
    fn apply_auth_headers(&self, headers: HeaderMap) -> AuthHeadersFuture<'_> {
        Box::pin(async move {
            self.evidence.clear();
            let headers = self.inner.apply_auth_headers(headers).await?;
            self.evidence.record(&headers);
            Ok(headers)
        })
    }
    fn apply_auth(&self, request: Request) -> AuthProviderFuture<'_> {
        Box::pin(async move {
            self.evidence.clear();
            // Complete-request signers may replace URL, body and headers. Return
            // that exact result; never reconstruct it through header-only auth.
            let request = self.inner.apply_auth(request).await?;
            self.evidence.record(&request.headers);
            Ok(request)
        })
    }
}

/// Bind one API attempt's delegate to its own initially empty evidence cell.
/// Poison/cancellation/missing bearer stays unavailable; it never guesses from
/// earlier attempts. This does not prove transmission, peer identity or a 401.
pub fn observe_request_auth(
    inner: SharedAuthProvider,
) -> (SharedAuthProvider, RequestAuthEvidence) {
    let evidence = RequestAuthEvidence(Arc::new(Mutex::new(None)));
    (
        Arc::new(ObservedRequestAuth {
            inner,
            evidence: evidence.clone(),
        }),
        evidence,
    )
}

#[cfg(test)]
#[path = "request_auth_evidence_tests.rs"]
mod tests;
