//! Real processor RPCs; the MCP failure observation is injected, not a transport-cause claim.
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
use tokio::time::Instant;

async fn inject_idle_observation(
    harness: &TracingHarness,
    id: ThreadId,
) -> (
    Arc<codex_core::CodexThread>,
    codex_core::ThreadRetirement,
    crate::thread_state::RetentionRetirementClaim,
) {
    let old = harness
        .processor
        .thread_manager
        .get_thread(id)
        .await
        .unwrap();
    let manager = &harness.processor.thread_state_manager;
    let state = manager.thread_state(id).await;
    let generation = state.lock().await.listener_generation;
    let expected = *manager.subscribe_to_retention(id).await.unwrap().borrow();
    let (claim, ticket) = manager
        .claim_thread_retirement(
            id,
            expected,
            &old,
            generation,
            Instant::now() + Duration::from_secs(20),
        )
        .await
        .unwrap();
    let actual = ticket.wait().await;
    assert!(actual.is_complete(), "{actual:?}");
    assert!(old.retirement_is_quiescent());
    let mut observed = actual;
    observed.cleanup = codex_core::ThreadCleanupOutcome::McpFailed;
    assert!(
        !crate::request_processors::observe_idle_retirement_report(manager, claim, observed).await
    );
    // The real handler leaves the old lookup alive after this typed incomplete observation.
    assert!(Arc::ptr_eq(
        &old,
        &harness
            .processor
            .thread_manager
            .get_thread(id)
            .await
            .unwrap()
    ));
    let completion = {
        let mut state = state.lock().await;
        if let Some(cancel) = state.cancel_tx.take() {
            let _ = cancel.send(());
        }
        state.listener_completion.clone().unwrap()
    };
    assert!(
        completion.await,
        "old listener's effects joined before the fixture returns"
    );
    (old, ticket, claim)
}

async fn functional_recovery(
    handoff: bool,
    override_model: bool,
    repeated: bool,
    route: &str,
) -> Result<()> {
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
    completed_turn(&mut harness, &started.thread.id, 3, "durable predecessor").await;
    let (old, ticket, first_claim) = inject_idle_observation(&harness, id).await;
    let original = ticket.wait().await;
    let deadline = ticket.deadline();
    if route == "cold" {
        // Surviving retiring authority with no current runtime lookup.
        harness
            .processor
            .thread_manager
            .remove_thread_if_same(&id, &old)
            .await;
    }
    if route == "cancel-archive" || route == "cancel-remove" {
        let processor = Arc::clone(&harness.processor);
        let retained = Arc::clone(&old);
        let detach = route == "cancel-remove";
        let (ready, observed) = tokio::sync::oneshot::channel();
        let transition = tokio::spawn(crate::thread_state::recovery::resume_scope(async move {
            processor
                .thread_processor
                .pending_thread_unloads_for_test(id)
                .await;
            assert!(
                processor
                    .thread_state_manager
                    .archive_retirement(first_claim, &retained)
                    .await
            );
            if detach {
                processor
                    .thread_manager
                    .remove_thread_if_same(&id, &retained)
                    .await;
            }
            ready.send(()).unwrap();
            std::future::pending::<()>().await;
        }));
        observed.await.unwrap();
        transition.abort();
        assert!(transition.await.unwrap_err().is_cancelled());
    }
    if handoff {
        // Exhaust just the fresh attempt's budget: a deterministic unfinished MCP observation.
        // This calls the production transition method; it does not inject successful cleanup.
        crate::thread_state::recovery::resume_scope(async {
            assert!(
                harness
                    .processor
                    .thread_processor
                    .recover_retiring_thread_until(id, Instant::now())
                    .await
                    .unwrap()
            );
        })
        .await;
        assert!(!old.retirement_reconciled());
    }
    let params: ThreadResumeParams = serde_json::from_value(serde_json::json!({
        "threadId": started.thread.id, "model": if override_model {Some("different-model")} else {None},
    }))?;
    if route == "daemon" {
        harness
            .processor
            .thread_processor
            .thread_resume(
                crate::request_processors::ThreadResumeTarget::DaemonRecovery(None),
                params.clone(),
                None,
                None,
                Default::default(),
            )
            .await
            .unwrap();
    }
    let resumed: ThreadResumeResponse = harness
        .request(
            ClientRequest::ThreadResume {
                request_id: RequestId::Integer(4),
                params,
            },
            None,
        )
        .await;
    assert_eq!(resumed.thread.id, started.thread.id);
    let current = harness
        .processor
        .thread_manager
        .get_thread(id)
        .await
        .unwrap();
    assert!(!Arc::ptr_eq(&old, &current));
    let reconciled = !handoff;
    assert_eq!(old.retirement_reconciled(), reconciled);
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
    assert!(
        matches!(retained, ThreadRetentionAcquireResponse::Acquired { .. }),
        "{retained:?}"
    );
    completed_turn(
        &mut harness,
        &resumed.thread.id,
        6,
        "durable recovered turn",
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
    assert!(history.contains("durable predecessor"));
    assert!(history.contains("durable recovered turn"));
    assert_eq!(ticket.deadline(), deadline);
    assert_eq!(ticket.wait().await, original);
    let positive = harness
        .processor
        .thread_state_manager
        .reconciled_retirements()
        .await;
    assert_eq!(positive.contains(&(id, first_claim.generation)), reconciled);
    if repeated {
        // Release the new grant before the next independent idle claim.
        harness
            .processor
            .thread_state_manager
            .remove_connection(TEST_CONNECTION_ID)
            .await;
        let (_, _, second_claim) = inject_idle_observation(&harness, id).await;
        assert_ne!(first_claim.generation, second_claim.generation);
        crate::thread_state::recovery::resume_scope(async {
            assert!(
                harness
                    .processor
                    .thread_processor
                    .recover_retiring_thread_until(id, Instant::now())
                    .await
                    .unwrap()
            );
        })
        .await;
    }
    let reports = harness
        .processor
        .thread_state_manager
        .drain_retirement_tickets()
        .await;
    assert!(reports.iter().any(|(claim, _)| *claim == first_claim));
    if repeated {
        assert_eq!(reports.len(), 2);
    }
    harness.shutdown().await;
    Ok(())
}

#[test]
#[serial(app_server_tracing)]
fn recovery_rpc_reconciles_then_resumes_retains_and_persists_first_turn() -> Result<()> {
    run_current_thread_test_with_stack(
        "recovery reconciliation",
        functional_recovery(false, false, false, "warm"),
    )
}
#[test]
#[serial(app_server_tracing)]
fn recovery_handoff_then_rpc_resumes_with_overrides_and_preserves_retired_generations() -> Result<()>
{
    run_current_thread_test_with_stack(
        "recovery handoff",
        functional_recovery(true, true, true, "warm"),
    )
}

#[test]
#[serial(app_server_tracing)]
fn recovery_daemon_and_cold_paths_replace_the_exact_retired_runtime() -> Result<()> {
    run_current_thread_test_with_stack("recovery alternate routes", async {
        functional_recovery(false, false, false, "daemon").await?;
        functional_recovery(false, false, false, "cold").await
    })
}
#[test]
#[serial(app_server_tracing)]
fn recovery_cancellation_after_archive_and_map_removal_can_resume_again() -> Result<()> {
    run_current_thread_test_with_stack("recovery cancellation", async {
        functional_recovery(false, false, false, "cancel-archive").await?;
        functional_recovery(false, false, false, "cancel-remove").await
    })
}
