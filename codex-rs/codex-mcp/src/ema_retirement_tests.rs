use super::*;
use codex_exec_server::{
    ExecServerError, HttpClient, HttpRedirectPolicy, HttpRequestParams, HttpRequestResponse,
    HttpResponseBodyStream,
};
use codex_rmcp_client::{EmaAccessToken, EmaAuthenticatedHttpClient, EmaTokenSource};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use url::Url;

#[tokio::test]
async fn runtime_closure_revokes_ema_and_observes_original_watcher_and_quiescence() {
    let runtime = crate::McpRuntime::empty(true);
    let root = runtime.retirement_owner();
    let token = CancellationToken::new();
    let authority = EmaRequestAuthority::new(token.clone(), || true);
    let owner = EmaRetirementOwner::new(authority.clone());
    root.register_ema(Arc::clone(&owner));
    root.register_ema(owner);
    drop(runtime.worker_owner().spawn(
        &tokio::runtime::Handle::current(),
        crate::McpWorkerKind::EmaRevocation,
        async move {
            token.cancelled().await;
        },
    ));
    assert!(authority.check_new_request().is_ok());
    let report = root
        .drain_workers_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert!(authority.check_new_request().is_err());
    assert!(report.are_recorded_workers_drained(), "{report:?}");
    assert_eq!(report.ema.len(), 1);
    assert_eq!(report.ema[0].outcome, RuntimeEmaOutcome::Quiesced);
    let watcher = report.workers[0]
        .counts
        .iter()
        .find(|count| count.kind == crate::McpWorkerKind::EmaRevocation)
        .unwrap();
    assert_eq!(watcher.joined, 1);
    assert_eq!(watcher.pending + watcher.cancelled + watcher.panicked, 0);
}

#[tokio::test]
async fn already_created_ema_owner_registering_after_closure_is_immediately_revoked() {
    let runtime = crate::McpRuntime::empty(true);
    let root = runtime.retirement_owner();
    let generation = root.admit_generation().unwrap();
    let authority = EmaRequestAuthority::default();
    let owner = EmaRetirementOwner::new(authority.clone());
    root.close_registration();
    assert!(!authority.is_revoked());
    root.register_ema(owner);
    assert!(authority.is_revoked());
    drop(generation);
    let report = root
        .drain_workers_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert!(report.are_recorded_workers_drained(), "{report:?}");
    assert_eq!(report.ema.len(), 1);
    assert_eq!(report.ema[0].outcome, RuntimeEmaOutcome::Quiesced);
}

#[derive(Clone)]
struct CachedSource {
    endpoint: Url,
    renewals: Arc<AtomicUsize>,
}

impl EmaTokenSource for CachedSource {
    fn endpoint(&self) -> &Url {
        &self.endpoint
    }
    async fn access_token(&self) -> anyhow::Result<EmaAccessToken> {
        self.renewals.fetch_add(1, Ordering::SeqCst);
        anyhow::bail!("fixture must not acquire or renew credentials")
    }
}

struct SuspendedHttp(Arc<AtomicUsize>);
impl HttpClient for SuspendedHttp {
    fn http_request(
        &self,
        _params: HttpRequestParams,
    ) -> BoxFuture<'_, Result<HttpRequestResponse, ExecServerError>> {
        async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            std::future::pending().await
        }
        .boxed()
    }
    fn http_request_stream(
        &self,
        _params: HttpRequestParams,
    ) -> BoxFuture<'_, Result<(HttpRequestResponse, HttpResponseBodyStream), ExecServerError>> {
        async { panic!("unexpected streaming fixture request") }.boxed()
    }
}

#[tokio::test]
async fn suspended_actual_ema_response_keeps_external_drain_incomplete_until_released() {
    let runtime = crate::McpRuntime::empty(true);
    let root = runtime.retirement_owner();
    let authority = EmaRequestAuthority::default();
    root.register_ema(EmaRetirementOwner::new(authority.clone()));
    let calls = Arc::new(AtomicUsize::new(0));
    let source = CachedSource {
        endpoint: Url::parse("https://ema.invalid/mcp").unwrap(),
        renewals: Arc::new(AtomicUsize::new(0)),
    };
    let client = EmaAuthenticatedHttpClient::new(
        Arc::new(SuspendedHttp(Arc::clone(&calls))),
        source.clone(),
        EmaAccessToken {
            access_token: "fixture-only-token".into(),
            expires_in: Some(Duration::from_secs(300)),
            received_at: Instant::now(),
        },
        authority.clone(),
    );
    let mut response = client.http_request(HttpRequestParams {
        method: "POST".into(),
        url: source.endpoint.to_string(),
        headers: Vec::new(),
        body: None,
        timeout_ms: None,
        redirect_policy: HttpRedirectPolicy::Stop,
        request_id: "suspended-ema-fixture".into(),
        stream_response: false,
    });
    // This is the real SDK authority read lease, held by an admitted response
    // whose caller is no longer polling it. No network or credentials are used.
    assert!(futures::poll!(&mut response).is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let incomplete = root
        .drain_workers_until(Instant::now() + Duration::from_millis(20))
        .await;
    assert!(authority.is_revoked());
    assert!(!incomplete.are_recorded_workers_drained());
    assert_eq!(incomplete.ema[0].outcome, RuntimeEmaOutcome::Unobserved);
    assert!(!incomplete.workers[0].all_admission_closed);
    drop(response);
    let complete = root
        .drain_workers_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert!(complete.are_recorded_workers_drained(), "{complete:?}");
    assert_eq!(complete.ema[0].outcome, RuntimeEmaOutcome::Quiesced);
    assert_eq!(source.renewals.load(Ordering::SeqCst), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
