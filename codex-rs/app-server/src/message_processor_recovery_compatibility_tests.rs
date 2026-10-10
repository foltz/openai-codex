//! This witness uses only pre-fix APIs so unchanged production can execute it.
use super::*;
use codex_app_server_protocol::ThreadReadParams;
use codex_app_server_protocol::ThreadReadResponse;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ThreadRetentionAcquireParams;
use codex_app_server_protocol::ThreadRetentionAcquireResponse;
use codex_protocol::ThreadId;
use pretty_assertions::assert_eq;

fn compatible_rpc(override_model: bool) -> Result<()> {
    run_current_thread_test_with_stack("recovery baseline-compatible RPC", async move {
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
            "compatibility predecessor",
        )
        .await;
        let old = harness
            .processor
            .thread_manager
            .get_thread(id)
            .await
            .unwrap();
        let manager = &harness.processor.thread_state_manager;
        if override_model {
            assert!(
                manager
                    .unsubscribe_connection_from_thread(id, TEST_CONNECTION_ID)
                    .await
            );
        }
        let state = manager.thread_state(id).await;
        let listener_generation = state.lock().await.listener_generation;
        let expected = *manager.subscribe_to_retention(id).await.unwrap().borrow();
        let (claim, ticket) = manager
            .claim_thread_retirement(
                id,
                expected,
                &old,
                listener_generation,
                tokio::time::Instant::now() + Duration::from_secs(20),
            )
            .await
            .unwrap();
        let original = ticket.wait().await;
        assert!(original.is_complete());
        // Deterministic typed observation at the existing retained report seam.
        let mut incomplete = original;
        incomplete.cleanup = codex_core::ThreadCleanupOutcome::McpFailed;
        assert!(manager.record_retirement_report(claim, incomplete).await);
        if let Some(cancel) = state.lock().await.cancel_tx.take() {
            let _ = cancel.send(());
        }
        let params: ThreadResumeParams = serde_json::from_value(
            serde_json::json!({"threadId": started.thread.id, "model": if override_model {Some("different-model")} else {None}}),
        )?;
        let resumed: ThreadResumeResponse = harness
            .request(
                ClientRequest::ThreadResume {
                    request_id: RequestId::Integer(4),
                    params,
                },
                None,
            )
            .await;
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
        assert!(!Arc::ptr_eq(
            &old,
            &harness
                .processor
                .thread_manager
                .get_thread(id)
                .await
                .unwrap()
        ));
        completed_turn(
            &mut harness,
            &resumed.thread.id,
            6,
            "compatibility recovered turn",
        )
        .await;
        let read: ThreadReadResponse = harness
            .request(
                ClientRequest::ThreadRead {
                    request_id: RequestId::Integer(7),
                    params: ThreadReadParams {
                        thread_id: resumed.thread.id,
                        include_turns: true,
                    },
                },
                None,
            )
            .await;
        let persisted = serde_json::to_string(&read)?;
        assert!(persisted.contains("compatibility recovered turn"));
        assert_eq!(ticket.wait().await, original);
        harness.shutdown().await;
        Ok(())
    })
}

#[test]
#[serial(app_server_tracing)]
fn recovery_compatibility_rpc_must_resume_retain_and_persist_a_first_turn() -> Result<()> {
    compatible_rpc(false)
}
#[test]
#[serial(app_server_tracing)]
fn recovery_compatibility_override_rpc_must_not_reuse_retiring_authority() -> Result<()> {
    compatible_rpc(true)
}

pub(super) async fn completed_turn(
    harness: &mut TracingHarness,
    id: &str,
    request: i64,
    text: &str,
) {
    let params: TurnStartParams = serde_json::from_value(serde_json::json!({
        "threadId": id, "input": [{"type": "text", "text": text}]
    }))
    .unwrap();
    let _: TurnStartResponse = harness
        .request(
            ClientRequest::TurnStart {
                request_id: RequestId::Integer(request),
                params,
            },
            None,
        )
        .await;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let envelope = harness.outgoing_rx.recv().await.unwrap();
            let message = match envelope {
                crate::outgoing_message::OutgoingEnvelope::ToConnection { message, .. }
                | crate::outgoing_message::OutgoingEnvelope::Broadcast { message } => message,
            };
            if let crate::outgoing_message::OutgoingMessage::AppServerNotification(envelope) =
                message
                && let codex_app_server_protocol::ServerNotification::TurnCompleted(done) =
                    envelope.notification
            {
                assert_eq!(done.thread_id, id);
                assert!(done.turn.error.is_none(), "{:?}", done.turn);
                break;
            }
        }
    })
    .await
    .expect("real turn completed");
}
