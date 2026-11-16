//! Exercises classifier fallback, cooldown, and cancellation against local servers.

use super::super::ConversationSample;
use super::super::LunaSampler;
use super::super::tests::ProxyPrewarmLimit;
use super::super::tests::proxy_websocket_servers_with_http;
use super::super::tests::sample_request;
use super::super::tests::sampler_config;
use super::*;
use crate::async_scorer::request::PreparedRequest;
use anyhow::Result;
use codex_guardian_context::TranscriptCursor;
use codex_history::ResponseItemEnvelope;
use codex_login::AuthManager;
use codex_login::WorkspaceRouting;
use codex_login::WorkspaceRoutingRequest;
use codex_login::WorkspaceRoutingResolver;
use codex_model_provider::ModelProviderFuture;
use codex_model_provider::create_model_provider;
use codex_model_provider_info::ModelProviderInfo;
use core_test_support::responses;
use core_test_support::skip_if_no_network;
use pretty_assertions::assert_eq;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

// Routes HTTP normally while selectively stalling WebSocket handshakes.
struct Gateway {
    url: String,
    opens: Arc<AtomicUsize>,
    allowed_opens: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl Gateway {
    async fn new(http_url: &str, websocket_url: &str) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let http_target = http_url.trim_start_matches("http://").to_owned();
        let websocket_target = websocket_url.trim_start_matches("ws://").to_owned();
        let opens = Arc::new(AtomicUsize::new(0));
        let allowed_opens = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&opens);
        let allowed = Arc::clone(&allowed_opens);
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            while let Ok((mut incoming, _)) = listener.accept().await {
                let http_target = http_target.clone();
                let websocket_target = websocket_target.clone();
                let count = Arc::clone(&count);
                let allowed = Arc::clone(&allowed);
                connections.spawn(async move {
                    let mut method = [0; 4];
                    incoming.read_exact(&mut method).await?;
                    let target = if &method == b"GET " {
                        let attempt = count.fetch_add(/*val*/ 1, Ordering::SeqCst) + 1;
                        if attempt > allowed.load(Ordering::SeqCst) {
                            tokio::io::copy(&mut incoming, &mut tokio::io::sink()).await?;
                            return Ok::<_, std::io::Error>(());
                        }
                        websocket_target
                    } else {
                        http_target
                    };
                    let mut outgoing = TcpStream::connect(target).await?;
                    outgoing.write_all(&method).await?;
                    tokio::io::copy_bidirectional(&mut incoming, &mut outgoing).await?;
                    Ok(())
                });
            }
        });
        Ok(Self {
            url,
            opens,
            allowed_opens,
            task,
        })
    }
}

impl Drop for Gateway {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn stalled_handshake_does_not_delay_other_warm_sockets() -> Result<()> {
    skip_if_no_network!(Ok(()));
    let stalled = responses::start_websocket_server_with_headers(vec![
        responses::WebSocketConnectionConfig {
            requests: Vec::new(),
            response_headers: Vec::new(),
            accept_delay: Some(Duration::from_secs(/*secs*/ 10)),
            close_after_requests: false,
        },
    ])
    .await;
    let healthy = responses::start_websocket_server(vec![vec![vec![
        responses::ev_output_text_delta("low"),
        responses::ev_completed("score"),
    ]]])
    .await;
    let url = proxy_websocket_servers_with_http(
        &[&stalled, &healthy],
        ProxyPrewarmLimit::AllConnections,
        /*http_url*/ None,
    )
    .await?;
    let sampler = LunaSampler::new(sampler_config(url));
    let opener = sampler.connections.replenish().unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while sampler
            .connections
            .idle_connections
            .lock()
            .unwrap()
            .is_empty()
        {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(!opener.is_finished());
    assert_eq!(
        sampler.sample(sample_request("healthy-socket")).await?,
        "low"
    );
    assert_eq!(healthy.single_connection().len(), 1);
    opener.abort();
    let _ = opener.await;
    drop(stalled);
    healthy.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn replenishes_before_last_idle_socket_is_leased() -> Result<()> {
    skip_if_no_network!(Ok(()));
    let mut servers = Vec::new();
    for _ in 0..INITIAL_WEBSOCKET_CONNECTIONS + 1 {
        servers.push(
            responses::start_websocket_server_with_headers(vec![
                responses::WebSocketConnectionConfig {
                    requests: Vec::new(),
                    response_headers: Vec::new(),
                    accept_delay: None,
                    close_after_requests: false,
                },
            ])
            .await,
        );
    }
    let url = proxy_websocket_servers_with_http(
        &servers.iter().collect::<Vec<_>>(),
        ProxyPrewarmLimit::AllConnections,
        /*http_url*/ None,
    )
    .await?;
    let sampler = LunaSampler::new(sampler_config(url));
    sampler.prewarm().await;
    let held = sampler.connections.lease().await?;
    assert!(matches!(held.connection, Connection::Websocket(_)));
    // A healthy socket remains available, but the next burst needs the reserve restored.
    sampler.wait_for_prewarm(Duration::from_secs(2)).await?;
    let mut burst = Vec::new();
    for _ in 0..INITIAL_WEBSOCKET_CONNECTIONS {
        let lease = sampler.connections.lease().await?;
        assert!(matches!(lease.connection, Connection::Websocket(_)));
        burst.push(lease);
    }
    drop(burst);
    drop(held);
    for server in servers {
        server.shutdown().await;
    }
    Ok(())
}

#[tokio::test]
async fn cold_pool_uses_http_during_open_timeout_then_recovers_after_cooldown() -> Result<()> {
    skip_if_no_network!(Ok(()));
    for uses_codex_backend in [false, true] {
        let http = responses::start_mock_server().await;
        let events = vec![
            responses::ev_output_text_delta("low"),
            responses::ev_completed("score"),
        ];
        let http_mock =
            responses::mount_sse_sequence(&http, vec![responses::sse(events.clone()); 3]).await;
        let mut connections = vec![Vec::new(); INITIAL_WEBSOCKET_CONNECTIONS - 1];
        connections.push(vec![events.clone()]);
        let ws = responses::start_websocket_server(connections).await;
        let gateway = Gateway::new(&http.uri(), ws.uri()).await?;
        let base_path = if uses_codex_backend {
            "/backend-api/codex"
        } else {
            "/v1"
        };
        let base_url = format!("{}{base_path}", gateway.url);
        let mut config = sampler_config(base_url.clone());
        config.provider = create_model_provider(
            ModelProviderInfo::create_openai_provider(Some(base_url)),
            Some(AuthManager::from_auth_for_testing(if uses_codex_backend {
                CodexAuth::create_dummy_chatgpt_auth_for_testing()
            } else {
                CodexAuth::from_api_key("test-api-key")
            })),
        );
        config.service_tier = Some("priority".to_owned());
        let sampler = LunaSampler::new(config);
        let opener = sampler.connections.replenish().unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while gateway.opens.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        let mut request = sample_request("parent-turn");
        request.parent_response_id = Some("resp-parent".to_owned());
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), sampler.sample(request)).await??,
            "low"
        );
        assert_eq!(
            sampler.sample(sample_request("parent-turn-2")).await?,
            "low"
        );
        assert_eq!(
            gateway.opens.load(Ordering::SeqCst),
            INITIAL_WEBSOCKET_CONNECTIONS
        );
        assert!(!opener.is_finished());
        assert!(sampler.connections.replenish().is_none());

        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(15)).await;
        opener.await?;
        tokio::time::resume();
        assert!(sampler.connections.replenish().is_none());
        assert_eq!(
            sampler.sample(sample_request("during-cooldown")).await?,
            "low"
        );
        assert_eq!(
            gateway.opens.load(Ordering::SeqCst),
            INITIAL_WEBSOCKET_CONNECTIONS
        );

        let requests = http_mock.requests();
        let first = &requests[0];
        let expected_path = if uses_codex_backend {
            "/backend-api/codex/responses"
        } else {
            "/v1/responses"
        };
        assert_eq!(first.path(), expected_path);
        assert_eq!(
            first.header("x-codex-guardian").as_deref(),
            uses_codex_backend.then_some("classifier")
        );
        assert!(first.header("authorization").is_some());
        let body = first.body_json();
        assert_eq!(
            body["service_tier"].as_str(),
            if uses_codex_backend {
                None
            } else {
                Some("priority")
            }
        );
        assert_eq!(
            body["client_metadata"]["parent_response_id"].as_str(),
            uses_codex_backend.then_some("resp-parent")
        );

        gateway.allowed_opens.store(usize::MAX, Ordering::SeqCst);
        tokio::time::pause();
        tokio::time::advance(CONNECT_COOLDOWN).await;
        tokio::time::resume();
        sampler.connections.replenish().unwrap().await?;
        assert_eq!(
            gateway.opens.load(Ordering::SeqCst),
            2 * INITIAL_WEBSOCKET_CONNECTIONS
        );
        assert_eq!(
            sampler.sample(sample_request("after-cooldown")).await?,
            "low"
        );
        assert_eq!(http_mock.requests().len(), 3);
    }
    Ok(())
}

#[tokio::test]
async fn cooldown_preserves_healthy_sockets_and_both_transports_share_capacity() -> Result<()> {
    skip_if_no_network!(Ok(()));
    let http = responses::start_mock_server().await;
    let ws = responses::start_websocket_server_with_headers(vec![
        responses::WebSocketConnectionConfig {
            requests: Vec::new(),
            response_headers: Vec::new(),
            accept_delay: None,
            close_after_requests: false,
        },
    ])
    .await;
    let gateway = Gateway::new(&http.uri(), ws.uri()).await?;
    gateway.allowed_opens.store(/*val*/ 1, Ordering::SeqCst);
    let sampler = LunaSampler::new(sampler_config(format!("{}/v1", gateway.url)));
    let opener = sampler.connections.replenish().unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while gateway.opens.load(Ordering::SeqCst) < 2
            || sampler
                .connections
                .idle_connections
                .lock()
                .unwrap()
                .is_empty()
        {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(15)).await;
    opener.await?;
    tokio::time::resume();
    let mut leases = Vec::new();
    leases.push(sampler.connections.lease().await?);
    assert!(matches!(leases[0].connection, Connection::Websocket(_)));
    for _ in 1..MAX_CONCURRENT_REQUESTS {
        let lease = sampler.connections.lease().await?;
        assert!(matches!(lease.connection, Connection::Http(_)));
        leases.push(lease);
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(20), sampler.connections.lease())
            .await
            .is_err()
    );
    leases.pop();
    assert!(matches!(
        sampler.connections.lease().await?.connection,
        Connection::Http(_)
    ));
    assert_eq!(gateway.opens.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn stalled_http_headers_exhaust_the_sampling_retry_budget() -> Result<()> {
    skip_if_no_network!(Ok(()));
    let http = responses::start_mock_server().await;
    let mock = responses::mount_response_sequence(
        &http,
        vec![responses::sse_response(String::new()).set_delay(Duration::from_secs(60)); 3],
    )
    .await;
    let mut config = sampler_config(format!("{}/v1", http.uri()));
    let mut provider = config.provider.info().clone();
    provider.stream_idle_timeout_ms = Some(1_000);
    config.provider = create_model_provider(provider, config.provider.auth_manager());
    let sampler = LunaSampler::new(config);

    tokio::time::pause();
    let sample = tokio::time::timeout(
        Duration::from_secs(10),
        sampler.sample(sample_request("stalled-headers")),
    );
    tokio::pin!(sample);
    let deadline = std::time::Instant::now() + Duration::from_secs(/*secs*/ 30);
    for expected_requests in 1..=3 {
        // Keep the paused runtime runnable until the server records this attempt.
        // Otherwise auto-advance can exhaust a retry before its request arrives.
        while mock.requests().len() < expected_requests {
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "server did not receive request {expected_requests}"
            );
            tokio::select! {
                result = &mut sample => {
                    anyhow::bail!("sampling finished before request {expected_requests}: {result:?}");
                }
                () = tokio::task::yield_now() => {}
            }
        }
        tokio::time::advance(Duration::from_millis(/*millis*/ 1_001)).await;
    }
    let result = sample.await?;
    tokio::time::resume();
    assert!(matches!(
        result,
        Err(LunaSamplerError::Api(ApiError::Transport(
            TransportError::Timeout
        )))
    ));
    assert_eq!(mock.requests().len(), 3);
    assert_eq!(
        sampler.connections.classifications.available_permits(),
        MAX_CONCURRENT_REQUESTS
    );
    Ok(())
}

#[tokio::test]
async fn cancellation_closes_http_before_headers_and_while_draining_the_body() -> Result<()> {
    skip_if_no_network!(Ok(()));
    // The second case publishes a score, then leaves the response body open forever.
    for response in [
        String::new(),
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n{}",
            responses::sse(vec![responses::ev_output_text_delta("low")]),
        ),
    ] {
        let has_score = !response.is_empty();
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let sampler = Arc::new(LunaSampler::new(sampler_config(format!(
            "http://{}/v1",
            listener.local_addr()?,
        ))));
        let (received, mut requests) = tokio::sync::mpsc::unbounded_channel();
        let (closed, mut disconnects) = tokio::sync::mpsc::unbounded_channel();
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            let mut request_id = 0;
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut method = [0; 4];
                socket.read_exact(&mut method).await?;
                if &method != b"POST" {
                    continue;
                }
                request_id += 1;
                let received = received.clone();
                let closed = closed.clone();
                let response = response.clone();
                connections.spawn(async move {
                    socket.write_all(response.as_bytes()).await?;
                    let _ = received.send(request_id);
                    let result = tokio::io::copy(&mut socket, &mut tokio::io::sink()).await;
                    let _ = closed.send(request_id);
                    result
                });
            }
            Ok::<_, std::io::Error>(())
        });
        let mut request = sample_request("first");
        let evidence = std::mem::take(&mut request.input)
            .into_iter()
            .map(ResponseItemEnvelope::new)
            .collect();
        let (input, input_tokens) =
            LunaSampler::prepare_input(&request, /*history*/ None, evidence);
        let sample = ConversationSample {
            input: PreparedRequest {
                input,
                input_tokens,
                existing_context_tokens: 0,
                cursor: TranscriptCursor {
                    parent_history_version: 0,
                    transcript_entry_count: 1,
                },
                truncations: Vec::new(),
                section_costs: Vec::new(),
            },
            sampling: request,
        };
        let (ready, score) = tokio::sync::oneshot::channel();
        let first = Arc::clone(&sampler);
        let pending = tokio::spawn(async move { first.sample_retained(sample, ready).await });
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), requests.recv()).await?,
            Some(1)
        );
        if has_score {
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(5), score)
                    .await???
                    .accept(),
                "low",
            );
            assert!(disconnects.try_recv().is_err());
        }
        // Conversation expiry cancels the retained future, including its unfinished drain.
        pending.abort();
        assert!(pending.await.unwrap_err().is_cancelled());
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), disconnects.recv()).await?,
            Some(1)
        );
        assert_eq!(
            sampler.connections.classifications.available_permits(),
            MAX_CONCURRENT_REQUESTS
        );
    }
    Ok(())
}

struct RoutingPolicy(Mutex<Option<WorkspaceRouting>>);

impl WorkspaceRoutingResolver for RoutingPolicy {
    fn resolve(
        &self,
        _request: WorkspaceRoutingRequest,
    ) -> ModelProviderFuture<'_, std::io::Result<Option<WorkspaceRouting>>> {
        Box::pin(async { Ok(self.0.lock().unwrap().clone()) })
    }
}

#[tokio::test]
async fn sampling_uses_new_credentials_after_logout_and_login() -> Result<()> {
    skip_if_no_network!(Ok(()));
    let home = tempfile::tempdir()?;
    let auth = AuthManager::from_auth_for_testing_with_home(
        CodexAuth::from_api_key("initial-key"),
        home.path().to_path_buf(),
    );
    let server = responses::start_mock_server().await;
    let requests = responses::mount_sse_sequence(
        &server,
        vec![
            responses::sse(vec![
                responses::ev_output_text_delta("low"),
                responses::ev_completed("before-logout"),
            ]),
            responses::sse(vec![
                responses::ev_output_text_delta("low"),
                responses::ev_completed("after-login"),
            ]),
        ],
    )
    .await;
    let mut config = sampler_config(format!("{}/v1", server.uri()));
    config.provider = create_model_provider(config.provider.info().clone(), Some(auth.clone()));
    let sampler = LunaSampler::new(config);
    // Keep both requests on HTTP without starting background WebSocket opens.
    *sampler.connections.retry_after.lock().unwrap() = Some(Instant::now() + CONNECT_COOLDOWN);
    assert_eq!(
        sampler
            .sample(sample_request("before-logout"))
            .await
            .expect("sample before logout"),
        "low"
    );

    auth.logout().await?;
    codex_login::login_with_api_key(
        home.path(),
        "replacement-key",
        codex_login::AuthCredentialsStoreMode::File,
        codex_login::AuthKeyringBackendKind::default(),
    )?;
    auth.reload().await;

    assert_eq!(
        sampler
            .sample(sample_request("after-login"))
            .await
            .expect("sample after signing in again"),
        "low"
    );
    let requests = requests.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0].header("authorization").as_deref(),
        Some("Bearer initial-key")
    );
    assert_eq!(
        requests[1].header("authorization").as_deref(),
        Some("Bearer replacement-key")
    );
    Ok(())
}

#[tokio::test]
async fn http_cache_preserves_no_constraint_redirect_policy_and_configured_headers() -> Result<()> {
    let base_url = "https://localhost:9/backend-api/codex".to_owned();
    let auth =
        AuthManager::from_auth_for_testing(CodexAuth::create_dummy_chatgpt_auth_for_testing());
    let resolver = Arc::new(RoutingPolicy(Mutex::new(None)));
    let routing_owner: Arc<dyn WorkspaceRoutingResolver> = resolver.clone();
    auth.set_workspace_routing_resolver(Arc::downgrade(&routing_owner));
    let mut config = sampler_config(base_url.clone());
    config.provider = create_model_provider(
        ModelProviderInfo::create_openai_provider(Some(base_url)),
        Some(auth),
    );
    let pool = ConnectionPool::new(Arc::new(config));
    // Exercise HTTP leases without starting a background WebSocket opener.
    *pool.retry_after.lock().unwrap() = Some(Instant::now() + CONNECT_COOLDOWN);
    let mut previous: Option<Arc<HttpTransport>> = None;
    for (routing, expected_policy) in [
        (None, ClientRedirectPolicy::Default),
        (Some("NO_CONSTRAINT"), ClientRedirectPolicy::Reject),
        (Some("NO_CONSTRAINT"), ClientRedirectPolicy::Reject),
        (None, ClientRedirectPolicy::Default),
    ] {
        *resolver.0.lock().unwrap() = routing.map(|account_routing_override| WorkspaceRouting {
            chatgpt_account_id: "account_id".into(),
            backend_origin: "https://localhost:9".into(),
            account_routing_override: account_routing_override.into(),
        });
        let setup = pool.client_setup().await?;
        assert!(!setup.provider.headers.contains_key(ACCOUNT_ROUTING_HEADER));
        let lease = pool.lease().await?;
        assert!(matches!(lease.connection, Connection::Http(_)));
        let cached = Arc::clone(pool.http_transport.lock().unwrap().as_ref().unwrap());
        assert_eq!(
            (cached.url.as_str(), cached.redirect_policy),
            (
                "https://localhost:9/backend-api/codex/responses",
                expected_policy
            ),
        );
        if let Some(previous) = previous {
            assert_eq!(
                Arc::ptr_eq(&previous, &cached),
                previous.redirect_policy == expected_policy,
            );
        }
        previous = Some(cached);
    }

    let mut config = sampler_config("https://localhost:9/v1".into());
    let mut info = config.provider.info().clone();
    info.http_headers = Some(std::collections::HashMap::from([(
        ACCOUNT_ROUTING_HEADER.into(),
        "us".into(),
    )]));
    config.provider = create_model_provider(info, config.provider.auth_manager());
    let pool = ConnectionPool::new(Arc::new(config));
    *pool.retry_after.lock().unwrap() = Some(Instant::now() + CONNECT_COOLDOWN);
    assert_eq!(
        pool.client_setup().await?.redirect_policy,
        ClientRedirectPolicy::Default
    );
    let lease = pool.lease().await?;
    assert!(matches!(lease.connection, Connection::Http(_)));
    assert_eq!(
        pool.http_transport
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .redirect_policy,
        ClientRedirectPolicy::Reject,
    );
    Ok(())
}

#[tokio::test]
async fn runtime_transports_authenticate_each_classification() -> Result<()> {
    use base64::Engine;
    use codex_features::RuntimeAuthMode;
    use codex_login::CodexAuth;
    use codex_runtime_agent_instance::AgentHostAuthorization;
    use codex_runtime_agent_instance::RuntimeAuthManager;
    use codex_runtime_agent_instance::RuntimeRegistrationMetadata;
    use codex_runtime_agent_instance::RuntimeRole;
    use serde_json::json;
    use wiremock::Mock;
    use wiremock::MockServer;
    use wiremock::ResponseTemplate;
    use wiremock::matchers::method;
    use wiremock::matchers::path;

    struct MutableAuth(Mutex<CodexAuth>);
    impl codex_login::ExternalAuth for MutableAuth {
        fn resolve(&self) -> codex_login::ExternalAuthFuture<'_, CodexAuth> {
            Box::pin(async { Ok(self.0.lock().unwrap().clone()) })
        }

        fn refresh(
            &self,
            _context: codex_login::ExternalAuthRefreshContext,
        ) -> codex_login::ExternalAuthFuture<'_, CodexAuth> {
            self.resolve()
        }
    }

    skip_if_no_network!(Ok(()));
    for mode in [RuntimeAuthMode::DualHeader, RuntimeAuthMode::AssertionOnly] {
        for transport in [
            "http",
            "websocket",
            "upgrade_required",
            "legacy_endpoint",
            "changed_auth",
            "denied",
        ] {
            let websocket = transport == "websocket";
            let denied = transport == "denied";
            let changed_auth = transport == "changed_auth";
            let source = Arc::new(MutableAuth(Mutex::new(CodexAuth::from_api_key(
                "guardian-key",
            ))));
            let registration = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/register"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "runtime_agent_instance_id": "runtime-guardian",
                    "agent_host_id": "host-guardian",
                    "actor": {"kind": "platform", "user_id": "actor", "organization_id": "org", "project_id": null},
                })))
                .expect(1)
                .mount(&registration)
                .await;
            let response = vec![
                responses::ev_output_text_delta("low"),
                responses::ev_completed("score"),
            ];
            let ws = if websocket {
                // Each peer leaves another response available, so closure cannot force the retry.
                Some(
                    responses::start_websocket_server(vec![
                        vec![response.clone(), response.clone()],
                        vec![response.clone(), response.clone()],
                    ])
                    .await,
                )
            } else {
                None
            };
            let http = MockServer::start().await;
            let upgrade_response = if denied {
                ResponseTemplate::new(403).set_body_json(json!({"error": {
                    "code": "runtime_instance_paused", "type": "runtime_agent_error",
                    "message": "The runtime agent instance is paused."
                }}))
            } else if transport == "legacy_endpoint" {
                ResponseTemplate::new(404)
            } else {
                ResponseTemplate::new(426)
            };
            let handshake_source = Arc::clone(&source);
            Mock::given(method("GET"))
                .and(path("/responses"))
                .respond_with(move |_request: &wiremock::Request| {
                    if changed_auth {
                        *handshake_source.0.lock().unwrap() =
                            CodexAuth::from_api_key("replacement-key");
                    }
                    upgrade_response.clone()
                })
                .expect(match transport {
                    "upgrade_required" | "legacy_endpoint" => 2,
                    "changed_auth" | "denied" => 1,
                    _ => 0,
                })
                .mount(&http)
                .await;
            Mock::given(method("POST"))
                .and(path("/responses"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .insert_header("content-type", "text/event-stream")
                        .set_body_string(responses::sse(response)),
                )
                .expect(if websocket || denied {
                    0
                } else if changed_auth {
                    1
                } else {
                    2
                })
                .mount(&http)
                .await;
            let url = ws.as_ref().map_or_else(
                || http.uri(),
                |ws| ws.uri().replacen("ws://", "http://", /*count*/ 1),
            );
            let mut config = sampler_config(url);
            let credentials =
                AuthManager::from_auth_for_testing(CodexAuth::from_api_key("guardian-key"));
            if changed_auth {
                credentials.set_external_auth(source.clone()).await?;
            }
            let runtime = Arc::new(RuntimeAuthManager::new(
                Arc::clone(&credentials),
                AgentHostAuthorization::InstallationId {
                    installation_id: uuid::Uuid::nil(),
                },
                RuntimeRegistrationMetadata {
                    harness: Some("codex".into()),
                    harness_version: None,
                    role: RuntimeRole::Orchestrator,
                },
                format!("{}/register", registration.uri()).parse()?,
            ));
            let mut info = config.provider.info().clone();
            info.supports_websockets = transport != "http";
            config.provider = codex_model_provider::test_support::runtime_model_provider(
                info,
                Arc::clone(&credentials),
                (runtime, mode),
            );
            let sampler = LunaSampler::new(config);
            sampler.prewarm().await;
            if let Some(ws) = &ws {
                assert!(
                    ws.handshakes().is_empty(),
                    "prewarming must not admit runtime work"
                );
            }
            if denied {
                let error = sampler.sample(sample_request("denied")).await.unwrap_err();
                assert!(
                    matches!(
                        error,
                        LunaSamplerError::Api(ApiError::Transport(TransportError::Http {
                            status: http::StatusCode::FORBIDDEN,
                            ..
                        }))
                    ),
                    "{error:?}"
                );
            } else {
                assert_eq!(sampler.sample(sample_request("first")).await?, "low");
                if changed_auth {
                    assert_eq!(
                        credentials.auth().await.unwrap().get_token()?,
                        "replacement-key"
                    );
                } else {
                    assert_eq!(sampler.sample(sample_request("second")).await?, "low");
                }
            }
            let headers = if let Some(ws) = &ws {
                assert_eq!(
                    ws.connections().iter().map(Vec::len).collect::<Vec<_>>(),
                    vec![1, 1]
                );
                ws.handshakes()
                    .iter()
                    .map(|h| {
                        (
                            h.header("authorization").unwrap(),
                            h.header("x-openai-agent-assertion"),
                        )
                    })
                    .collect::<Vec<_>>()
            } else {
                http.received_requests()
                    .await
                    .unwrap()
                    .iter()
                    .map(|request| {
                        (
                            request.headers["authorization"]
                                .to_str()
                                .unwrap()
                                .to_owned(),
                            request
                                .headers
                                .get("x-openai-agent-assertion")
                                .map(|value| value.to_str().unwrap().to_owned()),
                        )
                    })
                    .collect::<Vec<_>>()
            };
            let expected_attempts = match transport {
                "upgrade_required" | "legacy_endpoint" => 4,
                "denied" => 1,
                _ => 2,
            };
            assert_eq!(headers.len(), expected_attempts);
            let mut proofs = std::collections::BTreeSet::new();
            for (authorization, supplemental) in headers {
                let proof = match mode {
                    RuntimeAuthMode::DualHeader => {
                        assert_eq!(authorization, "Bearer guardian-key");
                        supplemental.unwrap()
                    }
                    RuntimeAuthMode::AssertionOnly => {
                        assert!(supplemental.is_none());
                        authorization
                            .strip_prefix("AgentAssertion ")
                            .unwrap()
                            .to_owned()
                    }
                };
                let payload: serde_json::Value = serde_json::from_slice(
                    &base64::engine::general_purpose::URL_SAFE_NO_PAD
                        .decode(proof.split('.').nth(/*n*/ 2).unwrap())?,
                )?;
                assert_eq!(payload["runtime_agent_instance"], "runtime-guardian");
                assert!(
                    proofs.insert(proof),
                    "each attempt must authenticate afresh"
                );
            }
            registration.verify().await;
            http.verify().await;
            drop(sampler);
            if let Some(ws) = ws {
                ws.shutdown().await;
            }
        }
    }
    Ok(())
}

#[derive(Default)]
struct PrewarmCounts {
    live: AtomicUsize,
    derives: AtomicUsize,
    refuse: std::sync::atomic::AtomicBool,
}
struct PrewarmWork(Arc<PrewarmCounts>);
impl PrewarmWork {
    fn new(c: &Arc<PrewarmCounts>) -> Self {
        c.live.fetch_add(1, Ordering::SeqCst);
        Self(Arc::clone(c))
    }
}
impl Drop for PrewarmWork {
    fn drop(&mut self) {
        self.0.live.fetch_sub(1, Ordering::SeqCst);
    }
}
impl codex_protocol::mcp_work::McpAttemptWork for PrewarmWork {
    fn derive_attempt(
        &self,
    ) -> Result<
        Box<dyn codex_protocol::mcp_work::McpAttemptWork>,
        codex_protocol::mcp_work::McpAttemptRefused,
    > {
        self.0.derives.fetch_add(1, Ordering::SeqCst);
        if self.0.refuse.load(Ordering::SeqCst) {
            return Err(codex_protocol::mcp_work::McpAttemptRefused);
        }
        Ok(Box::new(Self::new(&self.0)))
    }
}
#[tokio::test]
async fn owned_prewarm_refill_keeps_work_after_its_waiter_is_cancelled() -> Result<()> {
    let mut gateway = Gateway::new("http://127.0.0.1:9", "ws://127.0.0.1:9").await?;
    let sampler = LunaSampler::new(sampler_config(gateway.url.clone()));
    let counts = Arc::new(PrewarmCounts::default());
    let work = PrewarmWork::new(&counts);
    let pool = Arc::clone(&sampler.connections);
    let waiter = tokio::spawn(async move {
        pool.prewarm_with_work(Some(Box::new(work))).await;
    });
    let opens = INITIAL_WEBSOCKET_CONNECTIONS.min(MAX_CONCURRENT_OPENS);
    tokio::time::timeout(Duration::from_secs(5), async {
        while gateway.opens.load(Ordering::SeqCst) < opens {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_eq!(counts.live.load(Ordering::SeqCst), opens + 1);
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert_eq!(
        counts.live.load(Ordering::SeqCst),
        opens + 1,
        "actual refill/open tasks, not the waiter, own work"
    );
    gateway.task.abort();
    assert!((&mut gateway.task).await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(5), async {
        while counts.live.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(
        sampler
            .connections
            .idle_connections
            .lock()
            .unwrap()
            .is_empty()
    );
    assert_eq!(counts.derives.load(Ordering::SeqCst), opens);
    assert!(sampler.connections.replenishing.try_lock().is_ok());
    Ok(())
}
#[tokio::test]
async fn cached_classifier_sockets_retain_no_prewarm_work() -> Result<()> {
    let server = responses::start_websocket_server_with_headers(vec![
            responses::WebSocketConnectionConfig {
                requests: Vec::new(),
                response_headers: Vec::new(),
                accept_delay: None,
                close_after_requests: false,
            };
            INITIAL_WEBSOCKET_CONNECTIONS
        ])
    .await;
    let sampler = LunaSampler::new(sampler_config(format!(
        "http://{}/v1",
        server.uri().trim_start_matches("ws://")
    )));
    let counts = Arc::new(PrewarmCounts::default());
    sampler
        .prewarm_with_work(Some(Box::new(PrewarmWork::new(&counts))))
        .await;
    assert_eq!(
        counts.derives.load(Ordering::SeqCst),
        INITIAL_WEBSOCKET_CONNECTIONS
    );
    assert_eq!(
        sampler.connections.idle_connections.lock().unwrap().len(),
        INITIAL_WEBSOCKET_CONNECTIONS
    );
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    sampler.connections.clear();
    server.shutdown().await;
    Ok(())
}
#[tokio::test]
async fn expired_prewarm_work_opens_no_connection() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let sampler = LunaSampler::new(sampler_config(format!(
        "http://{}/v1",
        listener.local_addr()?
    )));
    let counts = Arc::new(PrewarmCounts::default());
    counts.refuse.store(true, Ordering::SeqCst);
    sampler
        .prewarm_with_work(Some(Box::new(PrewarmWork::new(&counts))))
        .await;
    assert_eq!(counts.derives.load(Ordering::SeqCst), 1);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    assert!(
        sampler
            .connections
            .idle_connections
            .lock()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        sampler.connections.sockets.available_permits(),
        MAX_CONCURRENT_REQUESTS
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err()
    );
    Ok(())
}
#[tokio::test]
async fn cancelled_prewarm_lock_wait_releases_only_the_waiter_work() {
    let sampler = LunaSampler::new(sampler_config("http://127.0.0.1:9/v1".to_owned()));
    let guard = Arc::clone(&sampler.connections.replenishing)
        .lock_owned()
        .await;
    let counts = Arc::new(PrewarmCounts::default());
    let mut warm = Box::pin(sampler.prewarm_with_work(Some(Box::new(PrewarmWork::new(&counts)))));
    assert!(
        std::future::Future::poll(
            warm.as_mut(),
            &mut std::task::Context::from_waker(std::task::Waker::noop())
        )
        .is_pending()
    );
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    assert_eq!(counts.derives.load(Ordering::SeqCst), 0);
    drop(warm);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    assert!(
        sampler
            .connections
            .idle_connections
            .lock()
            .unwrap()
            .is_empty()
    );
    drop(guard);
}

#[tokio::test]
async fn classifier_shutdown_joins_detached_refill_and_connection_opens() -> Result<()> {
    let gateway = Gateway::new("http://127.0.0.1:9", "ws://127.0.0.1:9").await?;
    let sampler = LunaSampler::new(sampler_config(gateway.url.clone()));
    let counts = Arc::new(PrewarmCounts::default());
    let work = PrewarmWork::new(&counts);
    let pool = Arc::clone(&sampler.connections);
    let waiter = tokio::spawn(async move {
        pool.prewarm_with_work(Some(Box::new(work))).await;
    });
    let opens = INITIAL_WEBSOCKET_CONNECTIONS.min(MAX_CONCURRENT_OPENS);
    tokio::time::timeout(Duration::from_secs(5), async {
        while gateway.opens.load(Ordering::SeqCst) < opens {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert_eq!(counts.live.load(Ordering::SeqCst), opens + 1);
    tokio::time::timeout(Duration::from_secs(5), sampler.shutdown()).await?;
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    assert!(sampler.connections.workers.with_open(|| ()).is_none());
    assert!(
        sampler
            .connections
            .idle_connections
            .lock()
            .unwrap()
            .is_empty()
    );
    assert!(sampler.connections.replenish().is_none());
    sampler
        .prewarm_with_work(Some(Box::new(PrewarmWork::new(&counts))))
        .await;
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    assert_eq!(counts.derives.load(Ordering::SeqCst), opens);
    assert_eq!(gateway.opens.load(Ordering::SeqCst), opens);
    Ok(())
}
#[tokio::test]
async fn classifier_shutdown_clears_idle_and_refuses_late_connection_return() -> Result<()> {
    let server = responses::start_websocket_server_with_headers(vec![
            responses::WebSocketConnectionConfig {
                requests: Vec::new(),
                response_headers: Vec::new(),
                accept_delay: None,
                close_after_requests: false,
            };
            INITIAL_WEBSOCKET_CONNECTIONS
        ])
    .await;
    let sampler = LunaSampler::new(sampler_config(format!(
        "http://{}/v1",
        server.uri().trim_start_matches("ws://")
    )));
    let counts = Arc::new(PrewarmCounts::default());
    sampler
        .prewarm_with_work(Some(Box::new(PrewarmWork::new(&counts))))
        .await;
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    *sampler.connections.retry_after.lock().unwrap() = Some(Instant::now() + CONNECT_COOLDOWN);
    let lease = sampler.connections.lease().await?;
    assert!(matches!(lease.connection, Connection::Websocket(_)));
    let mut stop = Box::pin(sampler.shutdown());
    assert!(
        std::future::Future::poll(
            stop.as_mut(),
            &mut std::task::Context::from_waker(std::task::Waker::noop()),
        )
        .is_pending()
    );
    assert!(sampler.connections.lease().await.is_err());
    lease.reuse().await;
    tokio::time::timeout(Duration::from_secs(5), stop).await?;
    assert!(
        sampler
            .connections
            .idle_connections
            .lock()
            .unwrap()
            .is_empty()
    );
    assert!(
        sampler
            .connections
            .idle_connections
            .lock()
            .unwrap()
            .is_empty()
    );
    assert!(sampler.connections.lease().await.is_err());
    sampler.shutdown().await;
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn required_sampling_and_leases_refuse_missing_or_expired_work_without_requests() -> Result<()>
{
    let server = responses::start_mock_server().await;
    let mut config = sampler_config(server.uri());
    config.require_account_work = true;
    let sampler = LunaSampler::new(config);
    assert!(matches!(
        sampler.sample(sample_request("missing")).await,
        Err(LunaSamplerError::AccountWorkRefused)
    ));
    assert!(matches!(
        sampler.connections.lease().await,
        Err(LunaSamplerError::AccountWorkRefused)
    ));
    assert!(sampler.connections.replenish().is_none());
    sampler.prewarm().await;
    let counts = Arc::new(PrewarmCounts::default());
    let mut request = sample_request("expired");
    request.account_work = Some(Box::new(PrewarmWork::new(&counts)));
    counts.refuse.store(true, Ordering::SeqCst);
    assert!(matches!(
        sampler.sample(request).await,
        Err(LunaSamplerError::AccountWorkRefused)
    ));
    assert_eq!(counts.derives.load(Ordering::SeqCst), 1);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    assert!(server.received_requests().await.unwrap().is_empty());
    sampler.shutdown().await;
    Ok(())
}
#[tokio::test]
async fn lease_work_is_captured_before_waiting_for_classification_capacity() -> Result<()> {
    let mut config = sampler_config("http://127.0.0.1:9".into());
    config.require_account_work = true;
    let sampler = LunaSampler::new(config);
    let pool = Arc::clone(&sampler.connections);
    let permits = Arc::clone(&pool.classifications)
        .acquire_many_owned(u32::try_from(MAX_CONCURRENT_REQUESTS)?)
        .await?;
    let counts = Arc::new(PrewarmCounts::default());
    let root = PrewarmWork::new(&counts);
    let mut pending = Box::pin(async move {
        let _lease = pool.lease_with_work(Some(&root)).await;
    });
    assert!(
        std::future::Future::poll(
            pending.as_mut(),
            &mut std::task::Context::from_waker(std::task::Waker::noop())
        )
        .is_pending()
    );
    assert_eq!(counts.live.load(Ordering::SeqCst), 2);
    assert_eq!(counts.derives.load(Ordering::SeqCst), 1);
    drop(pending);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    drop(permits);
    sampler.shutdown().await;
    Ok(())
}
#[tokio::test]
async fn automatic_refill_owns_descendants_after_origin_is_dropped() -> Result<()> {
    let gateway = Gateway::new("http://127.0.0.1:9", "ws://127.0.0.1:9").await?;
    let mut config = sampler_config(gateway.url.clone());
    config.require_account_work = true;
    let sampler = LunaSampler::new(config);
    let counts = Arc::new(PrewarmCounts::default());
    let root = PrewarmWork::new(&counts);
    let refill = sampler
        .connections
        .replenish_with_work(Some(&root))
        .expect("admitted automatic refill");
    drop(root);
    let opens = INITIAL_WEBSOCKET_CONNECTIONS.min(MAX_CONCURRENT_OPENS);
    tokio::time::timeout(Duration::from_secs(5), async {
        while gateway.opens.load(Ordering::SeqCst) < opens {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_eq!(counts.live.load(Ordering::SeqCst), opens + 1);
    sampler.shutdown().await;
    refill.await?;
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    assert_eq!(counts.derives.load(Ordering::SeqCst), opens + 1);
    gateway.task.abort();
    assert!(gateway.task.await.unwrap_err().is_cancelled());
    Ok(())
}
#[tokio::test]
async fn response_drain_keeps_lease_account_work_after_score_owner_and_stop_waiter_drop()
-> Result<()> {
    let server =
        responses::start_websocket_server_with_headers(vec![
            responses::WebSocketConnectionConfig {
                requests: Vec::new(),
                response_headers: Vec::new(),
                accept_delay: None,
                close_after_requests: false
            };
            INITIAL_WEBSOCKET_CONNECTIONS
        ])
        .await;
    let mut config = sampler_config(format!(
        "http://{}/v1",
        server.uri().trim_start_matches("ws://")
    ));
    config.require_account_work = true;
    let sampler = LunaSampler::new(config);
    let counts = Arc::new(PrewarmCounts::default());
    sampler
        .prewarm_with_work(Some(Box::new(PrewarmWork::new(&counts))))
        .await;
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    *sampler.connections.retry_after.lock().unwrap() = Some(Instant::now() + CONNECT_COOLDOWN);
    let root = PrewarmWork::new(&counts);
    let lease = sampler.connections.lease_with_work(Some(&root)).await?;
    drop(root);
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    assert!(matches!(lease.connection, Connection::Websocket(_)));
    let (send, rx_event) = tokio::sync::mpsc::channel(1);
    let (interrupt, cancelled) = tokio::sync::oneshot::channel();
    let stream = codex_api::ResponseStream {
        routing_affinity: None,
        rx_event,
        upstream_request_id: None,
        interrupt: Some(interrupt),
    };
    let mut stop = Box::pin(sampler.shutdown());
    assert!(
        std::future::Future::poll(
            stop.as_mut(),
            &mut std::task::Context::from_waker(std::task::Waker::noop())
        )
        .is_pending()
    );
    drop(super::super::response::PendingResponse::new(
        stream, lease, None,
    ));
    cancelled.await?;
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    drop(stop);
    drop(send);
    tokio::time::timeout(Duration::from_secs(5), sampler.shutdown()).await?;
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    server.shutdown().await;
    Ok(())
}
