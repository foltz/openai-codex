use super::*;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::header::LOCATION;
use axum::response::IntoResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ThreadStartParams;
use codex_mcp::HostedEndpointConnectionReceipt;
use pretty_assertions::assert_eq;
use serde_json::json;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use tokio::net::TcpListener;

const AUTHORIZATION: &str = "Bearer config-bearer";
const CAPABILITY: &str = "retirement-capability";

fn endpoints() -> HostedMcpEndpoints {
    HostedMcpEndpoints::new(
        HostedEndpointCustody::default(),
        Arc::new(EnvironmentManager::default_for_tests()),
    )
}

fn thread_start_naming(url: &str) -> ClientRequest {
    ClientRequest::ThreadStart {
        request_id: RequestId::Integer(1),
        params: ThreadStartParams {
            config: Some(HashMap::from([(
                "mcp_servers.codex_tui".to_owned(),
                json!({"url": url, "http_headers": {"Authorization": AUTHORIZATION}}),
            )])),
            ..Default::default()
        },
    }
}

fn declare_params(url: &str, capability: &str) -> McpHostedEndpointDeclareParams {
    McpHostedEndpointDeclareParams {
        url: url.to_owned(),
        authorization: AUTHORIZATION.to_owned(),
        retirement_capability: capability.to_owned(),
        timeout_ms: 2_000,
    }
}

fn refused(reason: McpHostedEndpointRefusalReason) -> McpHostedEndpointDeclareResponse {
    McpHostedEndpointDeclareResponse::Refused { reason }
}

#[derive(Clone)]
struct ProofState {
    url: Arc<std::sync::Mutex<String>>,
    hits: Arc<AtomicUsize>,
    behavior: Behavior,
}

#[derive(Clone)]
enum Behavior {
    Answer,
    AnswerAfter(Duration),
    Oversized,
    RedirectTo(String),
    ReplayFirst(Arc<Mutex<Option<String>>>),
}

async fn proof_route(State(state): State<ProofState>, nonce: String) -> axum::response::Response {
    state.hits.fetch_add(1, Ordering::SeqCst);
    let url = state
        .url
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    let proof = codex_rmcp_client::hosted_endpoint_proof(CAPABILITY, &nonce, &url);
    match state.behavior {
        Behavior::Answer => proof.into_response(),
        Behavior::AnswerAfter(delay) => {
            tokio::time::sleep(delay).await;
            proof.into_response()
        }
        Behavior::ReplayFirst(first) => {
            let mut first = first.lock().unwrap_or_else(PoisonError::into_inner);
            first.get_or_insert(proof).clone().into_response()
        }
        Behavior::Oversized => format!("{proof}{}", " ".repeat(10_000)).into_response(),
        Behavior::RedirectTo(location) => {
            (StatusCode::TEMPORARY_REDIRECT, [(LOCATION, location)]).into_response()
        }
    }
}

/// Serves the challenge route; returns the endpoint URL and its hit counter.
async fn serve_proof(behavior: Behavior) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}/mcp", listener.local_addr().expect("addr"));
    let hits = Arc::new(AtomicUsize::new(0));
    let state = ProofState {
        url: Arc::new(std::sync::Mutex::new(url.clone())),
        hits: Arc::clone(&hits),
        behavior,
    };
    let router = Router::new()
        .route(
            codex_rmcp_client::HOSTED_ENDPOINT_PROOF_PATH,
            axum::routing::post(proof_route),
        )
        .with_state(state);
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    (url, hits)
}

async fn used_and_declared(
    endpoints: &HostedMcpEndpoints,
    url: &str,
    capability: &str,
) -> McpHostedEndpointDeclareResponse {
    endpoints
        .admit_request(ConnectionId(1), &thread_start_naming(url))
        .expect("intake");
    endpoints
        .declare(ConnectionId(1), declare_params(url, capability), || false)
        .await
}

#[tokio::test]
async fn declare_succeeds_only_for_a_used_endpoint_that_proves_the_capability() {
    let endpoints = endpoints();
    let (url, hits) = serve_proof(Behavior::Answer).await;
    // Not named by this connection's own overrides: no probe at all.
    assert_eq!(
        endpoints
            .declare(ConnectionId(1), declare_params(&url, CAPABILITY), || false)
            .await,
        refused(McpHostedEndpointRefusalReason::NotUsed)
    );
    assert_eq!(hits.load(Ordering::SeqCst), 0);
    assert!(matches!(
        used_and_declared(&endpoints, &url, CAPABILITY).await,
        McpHostedEndpointDeclareResponse::Declared { .. }
    ));
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_shared_config_credential_without_the_capability_fails_the_challenge() {
    let endpoints = endpoints();
    let (url, _hits) = serve_proof(Behavior::Answer).await;
    assert_eq!(
        used_and_declared(&endpoints, &url, "someone-else").await,
        refused(McpHostedEndpointRefusalReason::ProofFailed)
    );
}

#[tokio::test]
async fn declare_refuses_unsafe_urls_closed_connections_and_expired_budgets_without_probing() {
    let endpoints = endpoints();
    for url in [
        "http://127.0.0.1:80@evil.example:8000/mcp",
        "http://evil.example:8000/mcp",
        "http://localhost:8000/mcp",
        "http://10.0.0.1:8000/mcp",
        "http://[::ffff:127.0.0.1]:8000/mcp",
    ] {
        assert_eq!(
            endpoints
                .declare(ConnectionId(1), declare_params(url, CAPABILITY), || false)
                .await,
            refused(McpHostedEndpointRefusalReason::NotLoopback),
            "{url}"
        );
    }
    let (url, hits) = serve_proof(Behavior::Answer).await;
    endpoints
        .admit_request(ConnectionId(1), &thread_start_naming(&url))
        .expect("intake");
    assert_eq!(
        endpoints
            .declare(ConnectionId(1), declare_params(&url, CAPABILITY), || true)
            .await,
        refused(McpHostedEndpointRefusalReason::ConnectionClosed)
    );
    let mut expired = declare_params(&url, CAPABILITY);
    expired.timeout_ms = 0;
    assert_eq!(
        endpoints.declare(ConnectionId(1), expired, || false).await,
        refused(McpHostedEndpointRefusalReason::Expired)
    );
    assert_eq!(hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn redirect_responses_are_never_followed() {
    let endpoints = endpoints();
    let (target, target_hits) = serve_proof(Behavior::Answer).await;
    let location = codex_rmcp_client::LoopbackEndpoint::parse(&target)
        .expect("target")
        .proof_url()
        .to_string();
    let (url, hits) = serve_proof(Behavior::RedirectTo(location)).await;
    assert_eq!(
        used_and_declared(&endpoints, &url, CAPABILITY).await,
        refused(McpHostedEndpointRefusalReason::ProofFailed)
    );
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    assert_eq!(target_hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn oversized_proof_bodies_are_refused_before_decoding() {
    let endpoints = endpoints();
    let (url, _hits) = serve_proof(Behavior::Oversized).await;
    assert_eq!(
        used_and_declared(&endpoints, &url, CAPABILITY).await,
        refused(McpHostedEndpointRefusalReason::ProofFailed)
    );
}

#[tokio::test]
async fn challenges_are_rate_and_concurrency_limited_per_connection() {
    let endpoints = endpoints();
    let (slow, _) = serve_proof(Behavior::AnswerAfter(Duration::from_millis(500))).await;
    endpoints
        .admit_request(ConnectionId(1), &thread_start_naming(&slow))
        .expect("intake");
    let (first, second) = tokio::join!(
        endpoints.declare(ConnectionId(1), declare_params(&slow, CAPABILITY), || false),
        async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            endpoints
                .declare(ConnectionId(1), declare_params(&slow, CAPABILITY), || false)
                .await
        }
    );
    assert!(matches!(
        first,
        McpHostedEndpointDeclareResponse::Declared { .. }
    ));
    assert_eq!(second, refused(McpHostedEndpointRefusalReason::RateLimited));

    // The window allows a fixed number of challenges per connection.
    let (url, hits) = serve_proof(Behavior::Answer).await;
    endpoints
        .admit_request(ConnectionId(2), &thread_start_naming(&url))
        .expect("intake");
    for _ in 0..CHALLENGES_PER_CONNECTION_PER_WINDOW {
        assert_eq!(
            endpoints
                .declare(ConnectionId(2), declare_params(&url, "wrong"), || false)
                .await,
            refused(McpHostedEndpointRefusalReason::ProofFailed)
        );
    }
    assert_eq!(
        endpoints
            .declare(ConnectionId(2), declare_params(&url, CAPABILITY), || false)
            .await,
        refused(McpHostedEndpointRefusalReason::RateLimited)
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        CHALLENGES_PER_CONNECTION_PER_WINDOW as usize
    );
}

fn retire_params(url: &str, capability: &str, timeout_ms: u64) -> McpHostedEndpointRetireParams {
    McpHostedEndpointRetireParams {
        url: url.to_owned(),
        authorization: AUTHORIZATION.to_owned(),
        retirement_capability: capability.to_owned(),
        timeout_ms,
    }
}

#[tokio::test]
async fn retire_refuses_expired_intake_and_non_owners_then_refuses_readmission() {
    let endpoints = endpoints();
    let (url, _hits) = serve_proof(Behavior::Answer).await;
    assert!(matches!(
        used_and_declared(&endpoints, &url, CAPABILITY).await,
        McpHostedEndpointDeclareResponse::Declared { .. }
    ));
    let not_owner = McpHostedEndpointRetireResponse::Refused {
        reason: McpHostedEndpointRefusalReason::NotOwner,
    };
    assert_eq!(
        endpoints
            .retire(ConnectionId(1), retire_params(&url, CAPABILITY, 0))
            .await,
        McpHostedEndpointRetireResponse::Refused {
            reason: McpHostedEndpointRefusalReason::Expired,
        }
    );
    assert_eq!(
        endpoints
            .retire(ConnectionId(1), retire_params(&url, "forged", 1_000))
            .await,
        not_owner
    );
    assert_eq!(
        endpoints
            .retire(ConnectionId(2), retire_params(&url, CAPABILITY, 1_000))
            .await,
        not_owner
    );
    assert!(
        endpoints
            .admit_request(ConnectionId(3), &thread_start_naming(&url))
            .is_ok()
    );
    assert!(matches!(
        endpoints
            .retire(ConnectionId(1), retire_params(&url, CAPABILITY, 1_000))
            .await,
        McpHostedEndpointRetireResponse::Retired { .. }
    ));
    assert!(
        endpoints
            .admit_request(ConnectionId(3), &thread_start_naming(&url))
            .is_err()
    );
}

#[test]
fn override_endpoints_are_found_in_every_override_shape() {
    let url = "http://127.0.0.1:41005/mcp";
    let expected = vec![(
        EndpointKey::for_streamable_http(url, AUTHORIZATION),
        url.to_owned(),
    )];
    let shapes = [
        HashMap::from([(
            "mcp_servers".to_owned(),
            json!({"t": {"url": url, "http_headers": {"authorization": AUTHORIZATION}}}),
        )]),
        HashMap::from([(
            "mcp_servers.t".to_owned(),
            json!({"url": url, "http_headers": {"Authorization": AUTHORIZATION}}),
        )]),
        HashMap::from([
            ("mcp_servers.t.url".to_owned(), json!(url)),
            (
                "mcp_servers.t.http_headers.Authorization".to_owned(),
                json!(AUTHORIZATION),
            ),
        ]),
    ];
    for shape in shapes {
        assert_eq!(override_endpoints(&shape), expected);
    }
}

fn fixture_census(
    receipts: Vec<(Option<&str>, Vec<PhysicalRetirementOutcome>)>,
    unknown: Vec<Option<&str>>,
    possibly_untracked: bool,
) -> HostedEndpointRetirementCensus {
    HostedEndpointRetirementCensus {
        receipts: receipts
            .into_iter()
            .map(|(thread_id, outcomes)| HostedEndpointConnectionReceipt {
                thread_id: thread_id.map(str::to_owned),
                report: PhysicalRetirementReport {
                    attempts: outcomes.into_iter().enumerate().collect(),
                },
            })
            .collect(),
        proven_before_retirement: 1,
        unknown: unknown
            .into_iter()
            .map(|id| id.map(str::to_owned))
            .collect(),
        possibly_untracked,
        custody: HostedEndpointCustodyCensus::default(),
    }
}

#[test]
fn unknown_or_unproven_evidence_is_never_reported_retired() {
    let response = retire_response(fixture_census(
        vec![
            (Some("ok"), vec![PhysicalRetirementOutcome::Complete]),
            (
                Some("slow"),
                vec![
                    PhysicalRetirementOutcome::Complete,
                    PhysicalRetirementOutcome::TimedOut,
                ],
            ),
            (Some("dead"), vec![PhysicalRetirementOutcome::LogicalFailed]),
        ],
        vec![Some("vanished")],
        false,
    ));
    let McpHostedEndpointRetireResponse::Incomplete { receipts, census } = response else {
        panic!("unproven retirement must not be reported as retired");
    };
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| (receipt.thread_id.as_deref(), receipt.outcome))
            .collect::<Vec<_>>(),
        vec![
            (Some("ok"), McpHostedEndpointRetirementOutcome::Retired),
            (Some("slow"), McpHostedEndpointRetirementOutcome::TimedOut),
            (Some("dead"), McpHostedEndpointRetirementOutcome::Failed),
            (
                Some("vanished"),
                McpHostedEndpointRetirementOutcome::Incomplete
            ),
        ]
    );
    assert_eq!(
        census,
        McpHostedEndpointRetireCensus {
            retired: 1,
            incomplete: 1,
            timed_out: 1,
            failed: 1,
            proven_before_retirement: 1,
            unknown: 1,
            possibly_untracked: false,
            custody: McpHostedEndpointCustodyCensus::default(),
        }
    );
    assert!(matches!(
        retire_response(fixture_census(
            vec![(Some("ok"), vec![PhysicalRetirementOutcome::Complete])],
            Vec::new(),
            true,
        )),
        McpHostedEndpointRetireResponse::Incomplete { .. }
    ));
    assert!(matches!(
        retire_response(fixture_census(
            vec![(Some("ok"), vec![PhysicalRetirementOutcome::Complete])],
            Vec::new(),
            false,
        )),
        McpHostedEndpointRetireResponse::Retired { .. }
    ));
}

#[tokio::test]
async fn network_policy_refusal_blocks_the_challenge_before_any_post() {
    // A managed policy that admits no destination at all, built the same way
    // bootstrap clients narrow theirs (`NetworkPolicy::restrict_to_endpoints`).
    let policy = codex_http_client::NetworkPolicy::unmanaged()
        .restrict_to_endpoints(std::collections::BTreeSet::new());
    let factory = codex_http_client::HttpClientFactory::new(
        codex_http_client::OutboundProxyPolicy::ReqwestDefault,
    )
    .with_network_policy(policy);
    let endpoints = HostedMcpEndpoints::new(
        HostedEndpointCustody::default(),
        Arc::new(EnvironmentManager::without_environments(factory)),
    );
    let (url, hits) = serve_proof(Behavior::Answer).await;
    assert_eq!(
        used_and_declared(&endpoints, &url, CAPABILITY).await,
        refused(McpHostedEndpointRefusalReason::NetworkDenied)
    );
    assert_eq!(hits.load(Ordering::SeqCst), 0, "no POST was sent");
}

#[test]
fn ambiguous_override_paths_and_case_duplicate_headers_mint_no_authority() {
    let url = "http://127.0.0.1:41005/mcp";
    for config in [
        HashMap::from([(
            "mcp_servers.t".to_owned(),
            json!({
                "url":url,"http_headers":{"Authorization":"A","authorization":"B"}
            }),
        )]),
        HashMap::from([
            (
                "mcp_servers.t".to_owned(),
                json!({"url":url,"http_headers":{"Authorization":"A"}}),
            ),
            (
                "mcp_servers.t.http_headers.Authorization".to_owned(),
                json!("B"),
            ),
        ]),
        HashMap::from([
            (
                "mcp_servers".to_owned(),
                json!({"t":{"url":url,"http_headers":{"Authorization":"A"}}}),
            ),
            (
                "mcp_servers.t.url".to_owned(),
                json!("http://127.0.0.1:41006/mcp"),
            ),
        ]),
    ] {
        assert!(override_endpoints(&config).is_empty());
    }
    // Equal contributions do not create an alternative identity or a refusal.
    let config = HashMap::from([
        (
            "mcp_servers.t".to_owned(),
            json!({"url":url,"http_headers":{"Authorization":AUTHORIZATION}}),
        ),
        ("mcp_servers.t.url".to_owned(), json!(url)),
    ]);
    assert_eq!(
        override_endpoints(&config),
        vec![(
            EndpointKey::for_streamable_http(url, AUTHORIZATION),
            url.to_owned()
        )]
    );
}

#[tokio::test]
async fn replayed_proof_is_refused_and_connection_close_revokes_declaration() {
    let endpoints = endpoints();
    let (url, _) = serve_proof(Behavior::ReplayFirst(Arc::new(Mutex::new(None)))).await;
    assert!(matches!(
        used_and_declared(&endpoints, &url, CAPABILITY).await,
        McpHostedEndpointDeclareResponse::Declared { .. }
    ));
    assert_eq!(
        endpoints
            .declare(ConnectionId(1), declare_params(&url, CAPABILITY), || false)
            .await,
        refused(McpHostedEndpointRefusalReason::ProofFailed)
    );
    endpoints.connection_closed(ConnectionId(1));
    assert_eq!(
        endpoints
            .retire(ConnectionId(1), retire_params(&url, CAPABILITY, 1000))
            .await,
        McpHostedEndpointRetireResponse::Refused {
            reason: McpHostedEndpointRefusalReason::NotOwner
        }
    );
}

#[tokio::test]
async fn connection_lost_during_proof_cannot_declare() {
    let endpoints = endpoints();
    let (url, hits) = serve_proof(Behavior::Answer).await;
    endpoints
        .admit_request(ConnectionId(1), &thread_start_naming(&url))
        .expect("intake");
    assert_eq!(
        endpoints
            .declare(ConnectionId(1), declare_params(&url, CAPABILITY), || hits
                .load(Ordering::SeqCst)
                > 0)
            .await,
        refused(McpHostedEndpointRefusalReason::ConnectionClosed)
    );
    assert_eq!(
        endpoints
            .retire(ConnectionId(1), retire_params(&url, CAPABILITY, 1000))
            .await,
        McpHostedEndpointRetireResponse::Refused {
            reason: McpHostedEndpointRefusalReason::NotOwner
        }
    );
}

#[tokio::test]
async fn account_reset_during_real_challenge_refuses_old_ticket_after_reattribution() {
    let endpoints = endpoints();
    let (url, hits) = serve_proof(Behavior::AnswerAfter(Duration::from_millis(200))).await;
    endpoints
        .admit_request(ConnectionId(1), &thread_start_naming(&url))
        .expect("intake");
    let (response, ()) = tokio::join!(
        endpoints.declare(ConnectionId(1), declare_params(&url, CAPABILITY), || false),
        async {
            tokio::time::timeout(Duration::from_secs(1), async {
                while hits.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("challenge started");
            endpoints.custody.reset_for_account_transition();
            endpoints
                .admit_request(ConnectionId(1), &thread_start_naming(&url))
                .expect("reattribution");
        }
    );
    assert_eq!(response, refused(McpHostedEndpointRefusalReason::NotOwner));
    assert!(matches!(
        endpoints
            .declare(ConnectionId(1), declare_params(&url, CAPABILITY), || false)
            .await,
        McpHostedEndpointDeclareResponse::Declared { .. }
    ));
}
