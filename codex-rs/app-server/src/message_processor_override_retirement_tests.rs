//! Healthy cached overrides must establish bounded custody before initiating shutdown.
use super::recovery_compatibility_tests::completed_turn;
use super::*;
use codex_app_server_protocol::ThreadReadParams;
use codex_app_server_protocol::ThreadReadResponse;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ThreadRetentionAcquireParams;
use codex_app_server_protocol::ThreadRetentionAcquireResponse;
use codex_protocol::ThreadId;
use pretty_assertions::assert_eq;

#[derive(Default)]
struct HistoricalPanicFixture {
    armed: std::sync::atomic::AtomicBool,
    observed: tokio::sync::Notify,
}
impl codex_extension_api::TurnLifecycleContributor for HistoricalPanicFixture {
    fn on_turn_stop<'a>(
        &'a self,
        _input: codex_extension_api::TurnStopInput<'a>,
    ) -> codex_extension_api::ExtensionFuture<'a, ()> {
        Box::pin(async move {
            if self.armed.swap(false, std::sync::atomic::Ordering::SeqCst) {
                self.observed.notify_one();
                panic!("injected historical turn task panic");
            }
        })
    }
}
struct TestContributorGuard(Option<Arc<dyn codex_extension_api::TurnLifecycleContributor>>);
impl Drop for TestContributorGuard {
    fn drop(&mut self) {
        crate::extensions::replace_test_turn_contributor(self.0.take());
    }
}

struct TerminationFailureFixture {
    url: String,
    failures: Arc<std::sync::atomic::AtomicUsize>,
    _proxy: tokio_util::task::AbortOnDropHandle<()>,
    _server: tokio_util::task::AbortOnDropHandle<()>,
}

impl TerminationFailureFixture {
    async fn start() -> Result<Self> {
        use futures::SinkExt;
        use futures::StreamExt;
        use tokio_tungstenite::accept_async;
        use tokio_tungstenite::connect_async;
        use tokio_tungstenite::tungstenite::Message;

        let address = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await?
            .local_addr()?;
        let upstream_url = format!("ws://{address}");
        let runtime = codex_exec_server::ExecServerRuntimeOptions::new(
            codex_utils_cargo_bin::cargo_bin("codex")?,
            /*codex_linux_sandbox_exe*/ None,
        )?;
        let listening_url = upstream_url.clone();
        let server = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
            codex_exec_server::run_main(
                &listening_url,
                runtime,
                codex_http_client::HttpClientFactory::new(
                    codex_http_client::OutboundProxyPolicy::ReqwestDefault,
                ),
            )
            .await
            .unwrap();
        }));
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok((mut connection, _)) = connect_async(&upstream_url).await {
                    connection.close(None).await.unwrap();
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}", listener.local_addr()?);
        let failures = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = Arc::clone(&failures);
        let proxy = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut client = accept_async(socket).await.unwrap();
            let (mut upstream, _) = connect_async(upstream_url).await.unwrap();
            let mut terminate_ids = std::collections::HashSet::new();
            loop {
                tokio::select! {
                    request = client.next() => {
                        let Some(Ok(request)) = request else { break; };
                        if let Message::Text(text) = &request
                            && let Ok(body) = serde_json::from_str::<serde_json::Value>(text)
                            && body["method"] == "process/terminate"
                        {
                            terminate_ids.insert(body["id"].to_string());
                        }
                        // Real execution/termination still runs. Only the typed
                        // termination receipt is failed; no process is leaked.
                        if upstream.send(request).await.is_err() { break; }
                    }
                    response = upstream.next() => {
                        let Some(Ok(mut response)) = response else { break; };
                        if let Message::Text(text) = &response
                            && let Ok(body) = serde_json::from_str::<serde_json::Value>(text)
                            && terminate_ids.remove(&body["id"].to_string())
                        {
                            observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            response = Message::Text(serde_json::json!({
                                "jsonrpc": "2.0", "id": body["id"],
                                "error": {"code": -32000, "message": "injected MCP executor termination receipt failure"},
                            }).to_string().into());
                        }
                        if client.send(response).await.is_err() { break; }
                    }
                }
            }
        }));
        Ok(Self {
            url,
            failures,
            _proxy: proxy,
            _server: server,
        })
    }
}

async fn cached_override(closed_mcp: bool, panic_history: bool) -> Result<()> {
    let panic_fixture = panic_history.then(|| Arc::new(HistoricalPanicFixture::default()));
    let fixture_scope = TestContributorGuard(crate::extensions::replace_test_turn_contributor(
        panic_fixture.as_ref().map(|fixture| {
            Arc::clone(fixture) as Arc<dyn codex_extension_api::TurnLifecycleContributor>
        }),
    ));
    let mut harness = TracingHarness::new_for_recovery().await?;
    drop(fixture_scope);
    let executor = if closed_mcp {
        Some(TerminationFailureFixture::start().await?)
    } else {
        None
    };
    if let Some(executor) = &executor {
        harness
            .processor
            .thread_manager
            .environment_manager()
            .upsert_environment(
                "mcp-failure".to_string(),
                executor.url.clone(),
                /*connect_timeout*/ None,
            )?;
    }
    let config = closed_mcp.then(|| {
        std::collections::HashMap::from([(
            "mcp_servers".to_string(),
            serde_json::json!({"failure": {
                "command": codex_utils_cargo_bin::cargo_bin("test_stdio_server").unwrap(),
                "environment_id": "mcp-failure", "cwd": harness._codex_home.path(),
            }}),
        )])
    });
    let environments = if closed_mcp {
        Some(serde_json::from_value(serde_json::json!([
            {"environmentId": "local", "cwd": harness._codex_home.path()},
            {"environmentId": "mcp-failure", "cwd": harness._codex_home.path()},
        ]))?)
    } else {
        None
    };
    let started: ThreadStartResponse = harness
        .request(
            ClientRequest::ThreadStart {
                request_id: RequestId::Integer(2),
                params: ThreadStartParams {
                    ephemeral: Some(false),
                    history_mode: Some(codex_app_server_protocol::ThreadHistoryMode::Legacy),
                    config,
                    environments,
                    ..Default::default()
                },
            },
            None,
        )
        .await;
    read_thread_started_notification(&mut harness.outgoing_rx).await;
    let id = ThreadId::from_string(&started.thread.id)?;
    completed_turn(
        &mut harness,
        &started.thread.id,
        3,
        "cached override predecessor",
    )
    .await;
    if let Some(fixture) = &panic_fixture {
        fixture
            .armed
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let params = serde_json::from_value(serde_json::json!({
            "threadId": started.thread.id,
            "input": [{"type": "text", "text": "turn whose registered task will panic"}],
        }))?;
        let _: TurnStartResponse = harness
            .request(
                ClientRequest::TurnStart {
                    request_id: RequestId::Integer(30),
                    params,
                },
                None,
            )
            .await;
        tokio::time::timeout(Duration::from_secs(20), fixture.observed.notified()).await?;
        // A later normal turn reaps the actual panicked join and restores idle
        // status. No fabricated cleanup or task outcome is injected.
        completed_turn(
            &mut harness,
            &started.thread.id,
            31,
            "normal turn after historical panic",
        )
        .await;
    }
    let old = harness
        .processor
        .thread_manager
        .get_thread(id)
        .await
        .unwrap();
    let manager = harness.processor.thread_state_manager.clone();
    assert!(
        manager
            .unsubscribe_connection_from_thread(id, TEST_CONNECTION_ID)
            .await
    );
    assert!(manager.retirement_candidate(id).await.unwrap().is_none());
    assert!(!old.is_closing());
    if closed_mcp {
        old.call_mcp_tool(
            "failure",
            "echo",
            Some(serde_json::json!({"message": "ready before retirement"})),
            /*meta*/ None,
        )
        .await?;
    }
    let before_resume = tokio::time::Instant::now();
    let resumed: ThreadResumeResponse = harness
        .request(
            ClientRequest::ThreadResume {
                request_id: RequestId::Integer(4),
                params: serde_json::from_value::<ThreadResumeParams>(
                    serde_json::json!({"threadId": started.thread.id, "model": "override-model"}),
                )?,
            },
            None,
        )
        .await;
    assert_eq!(resumed.thread.id, started.thread.id);
    assert_eq!(resumed.thread.model.as_deref(), Some("override-model"));
    let current = harness
        .processor
        .thread_manager
        .get_thread(id)
        .await
        .unwrap();
    assert!(!Arc::ptr_eq(&old, &current));
    assert!(old.retirement_is_quiescent());
    let ticket = old
        .begin_retirement(tokio::time::Instant::now() + Duration::from_secs(60))
        .unwrap();
    let original_deadline = ticket.deadline();
    assert!(original_deadline >= before_resume);
    assert!(original_deadline <= tokio::time::Instant::now() + Duration::from_secs(10));
    assert_eq!(
        old.begin_retirement(tokio::time::Instant::now() + Duration::from_secs(60))
            .unwrap()
            .deadline(),
        original_deadline
    );
    let original = ticket.wait().await;
    let retained: ThreadRetentionAcquireResponse = harness
        .request(
            ClientRequest::ThreadRetentionAcquire {
                request_id: RequestId::Integer(5),
                params: ThreadRetentionAcquireParams {
                    thread_id: resumed.thread.id.clone(),
                },
            },
            None,
        )
        .await;
    assert!(matches!(
        retained,
        ThreadRetentionAcquireResponse::Acquired { .. }
    ));
    completed_turn(
        &mut harness,
        &resumed.thread.id,
        6,
        "cached override successor",
    )
    .await;
    let read: ThreadReadResponse = harness
        .request(
            ClientRequest::ThreadRead {
                request_id: RequestId::Integer(7),
                params: ThreadReadParams {
                    thread_id: resumed.thread.id.clone(),
                    include_turns: true,
                },
            },
            None,
        )
        .await;
    let history = serde_json::to_string(&read)?;
    assert!(history.contains("cached override predecessor"));
    assert!(history.contains("cached override successor"));
    let reports = manager.drain_retirement_tickets().await;
    assert_eq!(
        reports.len(),
        1,
        "override itself must create and retain the original ticket"
    );
    assert_eq!(reports[0].1, original);
    assert_eq!(ticket.wait().await, original);
    if closed_mcp {
        assert_eq!(
            reports[0].1.cleanup,
            codex_core::ThreadCleanupOutcome::McpFailed
        );
        assert!(
            executor
                .as_ref()
                .unwrap()
                .failures
                .load(std::sync::atomic::Ordering::SeqCst)
                > 0
        );
        assert!(
            !old.retirement_reconciled(),
            "the typed failed receipt remains sticky"
        );
    } else {
        assert!(reports[0].1.is_complete(), "{:?}", reports[0]);
    }
    harness.shutdown().await;
    Ok(())
}

#[test]
#[serial(app_server_tracing)]
fn recovery_cached_override_claims_before_shutdown_and_persists_first_turn() -> Result<()> {
    run_current_thread_test_with_stack("cached override custody", cached_override(false, false))
}

#[test]
#[serial(app_server_tracing)]
fn recovery_cached_override_survives_failed_mcp_termination_receipt() -> Result<()> {
    run_current_thread_test_with_stack("cached override MCP close", cached_override(true, false))
}

#[test]
#[serial(app_server_tracing)]
fn recovery_cached_override_retained_rejoin_releases_reservation() -> Result<()> {
    run_current_thread_test_with_stack("retained override rejoin", async {
        let mut harness = TracingHarness::new_for_recovery().await?;
        let started: ThreadStartResponse = harness
            .request(
                ClientRequest::ThreadStart {
                    request_id: RequestId::Integer(2),
                    params: ThreadStartParams {
                        ephemeral: Some(false),
                        history_mode: Some(codex_app_server_protocol::ThreadHistoryMode::Legacy),
                        ..Default::default()
                    },
                },
                None,
            )
            .await;
        read_thread_started_notification(&mut harness.outgoing_rx).await;
        let id = ThreadId::from_string(&started.thread.id)?;
        completed_turn(
            &mut harness,
            &started.thread.id,
            8,
            "retained override predecessor",
        )
        .await;
        let old = harness
            .processor
            .thread_manager
            .get_thread(id)
            .await
            .unwrap();
        assert!(
            harness
                .processor
                .thread_state_manager
                .unsubscribe_connection_from_thread(id, TEST_CONNECTION_ID)
                .await
        );
        let retained: ThreadRetentionAcquireResponse = harness
            .request(
                ClientRequest::ThreadRetentionAcquire {
                    request_id: RequestId::Integer(3),
                    params: ThreadRetentionAcquireParams {
                        thread_id: started.thread.id.clone(),
                    },
                },
                None,
            )
            .await;
        assert!(matches!(
            retained,
            ThreadRetentionAcquireResponse::Acquired { .. }
        ));
        assert!(
            harness
                .processor
                .thread_state_manager
                .subscribe_to_retention(id)
                .await
                .unwrap()
                .borrow()
                .granted
        );
        let resumed: ThreadResumeResponse = harness.request(ClientRequest::ThreadResume {
            request_id: RequestId::Integer(4), params: serde_json::from_value::<ThreadResumeParams>(
                serde_json::json!({"threadId": started.thread.id, "model": "ignored-override"}))?,
        }, None).await;
        assert_ne!(resumed.thread.model.as_deref(), Some("ignored-override"));
        assert!(Arc::ptr_eq(
            &old,
            &harness
                .processor
                .thread_manager
                .get_thread(id)
                .await
                .unwrap()
        ));
        assert!(!old.is_closing());
        assert!(
            harness
                .processor
                .thread_state_manager
                .retirement_candidate(id)
                .await
                .unwrap()
                .is_none()
        );
        // A second request has another owner. It must not inherit the first
        // owner's reservation, even on the refused-claim warm-rejoin path.
        let second: ThreadResumeResponse = harness
            .request(
                ClientRequest::ThreadResume {
                    request_id: RequestId::Integer(5),
                    params: serde_json::from_value::<ThreadResumeParams>(
                        serde_json::json!({"threadId": started.thread.id}),
                    )?,
                },
                None,
            )
            .await;
        assert_eq!(second.thread.id, started.thread.id);
        completed_turn(&mut harness, &second.thread.id, 6, "retained rejoin turn").await;
        harness.shutdown().await;
        Ok(())
    })
}

#[test]
#[serial(app_server_tracing)]
fn recovery_cached_override_never_rejoins_legacy_closed_runtime() -> Result<()> {
    run_current_thread_test_with_stack("legacy closed override refusal", async {
        let mut harness = TracingHarness::new_for_recovery().await?;
        let started: ThreadStartResponse = harness
            .request(
                ClientRequest::ThreadStart {
                    request_id: RequestId::Integer(2),
                    params: ThreadStartParams {
                        ephemeral: Some(false),
                        history_mode: Some(codex_app_server_protocol::ThreadHistoryMode::Legacy),
                        ..Default::default()
                    },
                },
                None,
            )
            .await;
        read_thread_started_notification(&mut harness.outgoing_rx).await;
        let id = ThreadId::from_string(&started.thread.id)?;
        let old = harness
            .processor
            .thread_manager
            .get_thread(id)
            .await
            .unwrap();
        old.shutdown_and_wait().await.unwrap();
        assert!(old.is_closing());
        harness.processor.process_request(TEST_CONNECTION_ID, request_from_client_request(ClientRequest::ThreadResume {
            request_id: RequestId::Integer(3), params: serde_json::from_value::<ThreadResumeParams>(
                serde_json::json!({"threadId": started.thread.id, "model": "override-model"}))?,
        }), &AppServerTransport::Stdio, Arc::clone(&harness.session)).await;
        loop {
            let envelope = tokio::time::timeout(Duration::from_secs(5), harness.outgoing_rx.recv())
                .await?
                .unwrap();
            if let crate::outgoing_message::OutgoingEnvelope::ToConnection { message, .. } =
                envelope
            {
                match message {
                    crate::outgoing_message::OutgoingMessage::Error(error)
                        if error.id == RequestId::Integer(3) =>
                    {
                        assert!(
                            error
                                .error
                                .message
                                .contains("original retirement custody unavailable"),
                            "{error:?}"
                        );
                        break;
                    }
                    crate::outgoing_message::OutgoingMessage::Response(response)
                        if response.id == RequestId::Integer(3) =>
                    {
                        panic!("closed legacy runtime falsely resumed: {response:?}");
                    }
                    _ => {}
                }
            }
        }
        assert!(Arc::ptr_eq(
            &old,
            &harness
                .processor
                .thread_manager
                .get_thread(id)
                .await
                .unwrap()
        ));
        assert!(
            harness
                .processor
                .thread_state_manager
                .retirement_candidate(id)
                .await
                .unwrap()
                .is_none()
        );
        harness.shutdown().await;
        Ok(())
    })
}

#[test]
#[serial(app_server_tracing)]
fn recovery_cached_override_cancelled_after_claim_can_observe_original_and_resume() -> Result<()> {
    run_current_thread_test_with_stack("cancelled retirement claim", async {
        let mut harness = TracingHarness::new_for_recovery().await?;
        let started: ThreadStartResponse = harness
            .request(
                ClientRequest::ThreadStart {
                    request_id: RequestId::Integer(2),
                    params: ThreadStartParams {
                        ephemeral: Some(false),
                        history_mode: Some(codex_app_server_protocol::ThreadHistoryMode::Legacy),
                        ..Default::default()
                    },
                },
                None,
            )
            .await;
        read_thread_started_notification(&mut harness.outgoing_rx).await;
        let id = ThreadId::from_string(&started.thread.id)?;
        completed_turn(
            &mut harness,
            &started.thread.id,
            3,
            "cancelled claim predecessor",
        )
        .await;
        let old = harness
            .processor
            .thread_manager
            .get_thread(id)
            .await
            .unwrap();
        assert!(
            harness
                .processor
                .thread_state_manager
                .unsubscribe_connection_from_thread(id, TEST_CONNECTION_ID)
                .await
        );
        let processor = Arc::clone(&harness.processor);
        let claimed = Arc::clone(&old);
        let (ready, observed) = tokio::sync::oneshot::channel();
        let request = tokio::spawn(crate::thread_state::recovery::resume_scope(async move {
            processor
                .thread_processor
                .pending_thread_unloads_for_test(id)
                .await;
            let manager = &processor.thread_state_manager;
            let expected = *manager.subscribe_to_retention(id).await.unwrap().borrow();
            let generation = manager
                .thread_state(id)
                .await
                .lock()
                .await
                .listener_generation;
            let (claim, ticket) = manager
                .claim_thread_retirement(
                    id,
                    expected,
                    &claimed,
                    generation,
                    tokio::time::Instant::now() + Duration::from_secs(10),
                )
                .await
                .unwrap();
            assert!(ready.send((claim, ticket)).is_ok());
            // Lose the request at the claim-to-first-poll boundary. No fixture
            // polls or completes the original; the subsequent RPC must do it.
            std::future::pending::<()>().await;
        }));
        let (claim, ticket) = observed.await?;
        let deadline = ticket.deadline();
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        assert!(!old.retirement_is_quiescent());
        let resumed: ThreadResumeResponse = harness.request(ClientRequest::ThreadResume {
            request_id: RequestId::Integer(4), params: serde_json::from_value::<ThreadResumeParams>(
                serde_json::json!({"threadId": started.thread.id, "model": "override-model"}))?,
        }, None).await;
        assert!(!Arc::ptr_eq(
            &old,
            &harness
                .processor
                .thread_manager
                .get_thread(id)
                .await
                .unwrap()
        ));
        assert!(ticket.wait().await.is_complete());
        assert_eq!(ticket.deadline(), deadline);
        assert!(
            harness
                .processor
                .thread_state_manager
                .reconciled_retirements()
                .await
                .contains(&(id, claim.generation))
        );
        completed_turn(
            &mut harness,
            &resumed.thread.id,
            5,
            "cancelled claim successor",
        )
        .await;
        harness.shutdown().await;
        Ok(())
    })
}

#[test]
#[serial(app_server_tracing)]
fn recovery_cached_override_complete_cleanup_with_historical_task_panic() -> Result<()> {
    run_current_thread_test_with_stack(
        "historical panic complete override",
        cached_override(false, true),
    )
}
