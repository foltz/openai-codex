use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct Dynamic {
    mode: AtomicUsize,
    sequence: AtomicUsize,
    rejected: Mutex<Option<HeaderMap>>,
}
impl AuthProvider for Dynamic {
    fn add_auth_headers(&self, headers: &mut HeaderMap) -> Result<(), AuthError> {
        headers.insert(
            http::header::AUTHORIZATION,
            "Bearer sync-hint".parse().unwrap(),
        );
        Ok(())
    }
    fn auth_header_telemetry(&self) -> Result<AuthHeaderTelemetry, AuthError> {
        Ok(AuthHeaderTelemetry {
            attached: false,
            name: None,
        })
    }
    fn apply_auth_headers(&self, mut headers: HeaderMap) -> AuthHeadersFuture<'_> {
        Box::pin(async move {
            match self.mode.load(Ordering::SeqCst) {
                1 => return Err(AuthError::Transient("synthetic apply failure".into())),
                2 => {
                    headers.remove(http::header::AUTHORIZATION);
                    return Ok(headers);
                }
                3 => std::future::pending::<()>().await,
                _ => {}
            }
            let value = if self.mode.load(Ordering::SeqCst) == 4 {
                format!(
                    "Bearer wire-{}",
                    self.sequence.fetch_add(1, Ordering::SeqCst)
                )
            } else {
                "Bearer async-wire".into()
            };
            headers.insert(http::header::AUTHORIZATION, value.parse().unwrap());
            Ok(headers)
        })
    }
    fn refresh_after_unauthorized(&self, rejected: HeaderMap) -> AuthRefreshFuture<'_> {
        Box::pin(async move {
            *self.rejected.lock().unwrap() = Some(rejected);
            true
        })
    }
}
fn proof(value: &str) -> Option<AuthCredentialFingerprint> {
    AuthCredentialFingerprint::from_bearer_header(value)
}

#[test]
fn sync_hints_and_telemetry_do_not_forge_authentication_evidence() {
    let (auth, evidence) = observe_request_auth(Arc::new(Dynamic::default()));
    let headers = auth.to_auth_headers().unwrap();
    assert_eq!(headers[http::header::AUTHORIZATION], "Bearer sync-hint");
    let telemetry = auth.auth_header_telemetry().unwrap();
    assert!(!telemetry.attached);
    assert_eq!(telemetry.name, None);
    assert!(evidence.fingerprint().is_none());
}

#[tokio::test]
async fn async_authenticated_headers_override_sync_hints_for_evidence() {
    let (auth, evidence) = observe_request_auth(Arc::new(Dynamic::default()));
    let headers = auth.apply_auth_headers(HeaderMap::new()).await.unwrap();
    assert_eq!(headers[http::header::AUTHORIZATION], "Bearer async-wire");
    assert!(evidence.fingerprint() == proof("Bearer async-wire"));
    auth.to_auth_headers().unwrap();
    auth.auth_header_telemetry().unwrap();
    assert!(evidence.fingerprint() == proof("Bearer async-wire"));
}

struct Signer;
impl AuthProvider for Signer {
    fn add_auth_headers(&self, _: &mut HeaderMap) -> Result<(), AuthError> {
        panic!("must forward complete-request signing rather than reconstruct auth");
    }
    fn apply_auth(&self, mut request: Request) -> AuthProviderFuture<'_> {
        Box::pin(async move {
            request.url.push_str("/signed");
            request = request.with_json(&serde_json::json!({"body": "signed"}));
            request.headers.insert(
                http::header::AUTHORIZATION,
                "Bearer signed-wire".parse().unwrap(),
            );
            request
                .headers
                .insert("x-signer", "retained".parse().unwrap());
            Ok(request)
        })
    }
}
#[tokio::test]
async fn complete_signer_request_is_returned_with_body_url_limits_and_headers_unchanged() {
    let (auth, evidence) = observe_request_auth(Arc::new(Signer));
    let mut request = Request::new(http::Method::POST, "https://example.invalid".into());
    request.timeout = Some(std::time::Duration::from_secs(7));
    request.response_body_limit_bytes = Some(19);
    request
        .headers
        .insert("x-original", "kept".parse().unwrap());
    let request = auth.apply_auth(request).await.unwrap();
    assert_eq!(request.url, "https://example.invalid/signed");
    assert_eq!(request.method, http::Method::POST);
    assert_eq!(request.headers["x-signer"], "retained");
    assert_eq!(request.headers["x-original"], "kept");
    assert_eq!(request.timeout, Some(std::time::Duration::from_secs(7)));
    assert_eq!(request.response_body_limit_bytes, Some(19));
    assert!(matches!(
        request.compression,
        codex_http_client::RequestCompression::None
    ));
    match request.body {
        Some(codex_http_client::RequestBody::Json(value)) => {
            assert_eq!(value, serde_json::json!({"body": "signed"}))
        }
        _ => panic!("authoritative signed body was replaced"),
    }
    assert!(evidence.fingerprint() == proof("Bearer signed-wire"));
}

#[tokio::test]
async fn separate_concurrent_attempts_do_not_share_rejection_evidence() {
    let inner = Arc::new(Dynamic::default());
    inner.mode.store(4, Ordering::SeqCst);
    let (first, first_evidence) = observe_request_auth(inner.clone());
    let (second, second_evidence) = observe_request_auth(inner);
    let (first, second) = tokio::join!(
        first.apply_auth_headers(HeaderMap::new()),
        second.apply_auth_headers(HeaderMap::new())
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert_ne!(
        first[http::header::AUTHORIZATION],
        second[http::header::AUTHORIZATION]
    );
    assert!(
        first_evidence.fingerprint() == proof(first[http::header::AUTHORIZATION].to_str().unwrap())
    );
    assert!(
        second_evidence.fingerprint()
            == proof(second[http::header::AUTHORIZATION].to_str().unwrap())
    );
}

#[tokio::test]
async fn cancelled_pending_authentication_cannot_reuse_a_previous_fingerprint() {
    let inner = Arc::new(Dynamic::default());
    let (auth, evidence) = observe_request_auth(inner.clone());
    auth.apply_auth_headers(HeaderMap::new()).await.unwrap();
    assert!(evidence.fingerprint().is_some());
    inner.mode.store(3, Ordering::SeqCst);
    let mut pending = auth.apply_auth_headers(HeaderMap::new());
    tokio::select! { biased;
        _ = &mut pending => panic!("synthetic authentication must remain pending"),
        _ = tokio::task::yield_now() => {}
    }
    assert!(evidence.fingerprint().is_none());
    drop(pending);
    assert!(evidence.fingerprint().is_none());
}

#[tokio::test]
async fn failed_or_non_bearer_application_clears_earlier_evidence() {
    for mode in [1, 2] {
        let inner = Arc::new(Dynamic::default());
        let (auth, evidence) = observe_request_auth(inner.clone());
        auth.apply_auth_headers(HeaderMap::new()).await.unwrap();
        assert!(evidence.fingerprint().is_some());
        inner.mode.store(mode, Ordering::SeqCst);
        let result = auth.apply_auth_headers(HeaderMap::new()).await;
        assert_eq!(result.is_err(), mode == 1);
        assert!(evidence.fingerprint().is_none());
    }
}

#[tokio::test]
async fn poisoned_evidence_stays_unavailable_without_changing_delegate_output() {
    let (auth, evidence) = observe_request_auth(Arc::new(Dynamic::default()));
    auth.apply_auth_headers(HeaderMap::new()).await.unwrap();
    let poison = evidence.0.clone();
    assert!(
        std::thread::spawn(move || {
            let _guard = poison.lock().unwrap();
            panic!("synthetic evidence poison");
        })
        .join()
        .is_err()
    );
    let headers = auth.apply_auth_headers(HeaderMap::new()).await.unwrap();
    assert_eq!(headers[http::header::AUTHORIZATION], "Bearer async-wire");
    assert!(evidence.fingerprint().is_none());
}

#[tokio::test]
async fn refresh_receives_exact_rejected_headers_and_does_not_forge_new_evidence() {
    let inner = Arc::new(Dynamic::default());
    let (auth, evidence) = observe_request_auth(inner.clone());
    auth.apply_auth_headers(HeaderMap::new()).await.unwrap();
    let mut rejected = HeaderMap::new();
    rejected.insert(
        http::header::AUTHORIZATION,
        "Bearer rejected-wire".parse().unwrap(),
    );
    rejected.insert("x-opaque", "retained".parse().unwrap());
    assert!(auth.refresh_after_unauthorized(rejected.clone()).await);
    assert!(inner.rejected.lock().unwrap().as_ref() == Some(&rejected));
    assert!(evidence.fingerprint() == proof("Bearer async-wire"));
}
