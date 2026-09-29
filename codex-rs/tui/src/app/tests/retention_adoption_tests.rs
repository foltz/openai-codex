//! Retention at the startup event and old-thread cleanup boundaries.
use super::*;
use codex_app_server_protocol::ThreadRetentionAcquireParams;
use codex_app_server_protocol::ThreadRetentionAcquireResponse;
use pretty_assertions::assert_eq;

async fn probe(
    server: &mut AppServerSession,
    thread_id: ThreadId,
) -> Result<ThreadRetentionAcquireResponse> {
    Ok(server
        .request_handle()
        .request_typed(ClientRequest::ThreadRetentionAcquire {
            request_id: server.next_request_id(),
            params: ThreadRetentionAcquireParams {
                thread_id: thread_id.to_string(),
            },
        })
        .await?)
}

#[derive(Clone, Copy)]
enum StartupOutcome {
    Accepted,
    FailedDelivery,
    Offline,
    Stale,
}

#[tokio::test]
async fn startup_event_owns_provisional_retention_until_installation() -> Result<()> {
    for outcome in [
        StartupOutcome::Accepted,
        StartupOutcome::FailedDelivery,
        StartupOutcome::Offline,
        StartupOutcome::Stale,
    ] {
        let (mut app, _, _) = make_test_app_with_channels().await;
        let mut server = crate::start_embedded_app_server_for_picker(&app.config).await?;
        let pending = crate::app_server_session::start_thread_with_request_handle(
            server.request_handle(),
            server.retention_client(),
            &app.local_settings,
            app.config.clone(),
            server.thread_params_mode(),
            /*remote_cwd_override*/ None,
            server.thread_tool_transport(),
        )
        .await?;
        let thread_id = pending.started.session.thread_id;
        assert!(matches!(
            probe(&mut server, thread_id).await?,
            ThreadRetentionAcquireResponse::AlreadyHeld { .. }
        ));
        app.pending_startup_thread_start = !matches!(outcome, StartupOutcome::Stale);
        let event = AppEvent::StartupThreadStarted {
            result: Ok(pending),
        };
        match outcome {
            StartupOutcome::FailedDelivery => {
                let (tx, rx) = mpsc::unbounded_channel();
                drop(rx);
                AppEventSender::new(tx).send(event);
            }
            StartupOutcome::Offline | StartupOutcome::Accepted | StartupOutcome::Stale => {
                app.reconnect.offline = matches!(outcome, StartupOutcome::Offline);
                let mut tui = crate::tui::test_support::make_test_tui()?;
                app.handle_event(&mut tui, &mut server, event).await?;
            }
        }
        if matches!(outcome, StartupOutcome::Accepted) {
            assert_eq!(app.primary_thread_id, Some(thread_id));
            assert!(matches!(
                probe(&mut server, thread_id).await?,
                ThreadRetentionAcquireResponse::AlreadyHeld { .. }
            ));
        } else {
            // Keep this connection alive while observing release; disconnect
            // revocation alone must not make the test pass.
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    match probe(&mut server, thread_id).await? {
                        ThreadRetentionAcquireResponse::Acquired { .. } => {
                            break Ok::<_, color_eyre::Report>(());
                        }
                        ThreadRetentionAcquireResponse::AlreadyHeld { .. } => {
                            tokio::task::yield_now().await
                        }
                        other => panic!("unexpected probe outcome: {other:?}"),
                    }
                }
            })
            .await??;
            assert_ne!(app.primary_thread_id, Some(thread_id));
        }
        server.thread_unsubscribe(thread_id).await?;
        server.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn resume_cleanup_preserves_primary_displayed_and_side_targets() -> Result<()> {
    for target_index in 0..3 {
        let (mut app, _, _) = make_test_app_with_channels().await;
        let mut server = crate::start_embedded_app_server_for_picker(&app.config).await?;
        let primary = server.start_thread(&app.config).await?;
        let displayed = server.start_thread(&app.config).await?;
        let side = server.start_thread(&app.config).await?;
        let ids = [
            primary.session.thread_id,
            displayed.session.thread_id,
            side.session.thread_id,
        ];
        app.primary_thread_id = Some(ids[0]);
        app.chat_widget.handle_thread_session(displayed.session);
        crate::chatwidget::activate_voice_for_thread(&mut app.chat_widget, ids[1]);
        app.side_threads
            .insert(ids[2], crate::app::side::SideThreadState::new(ids[0]));
        app.shutdown_current_thread_except(&mut server, Some(ids[target_index]))
            .await;
        assert_eq!(
            app.chat_widget.is_current_realtime_attempt(
                ids[1], /*attempt_id*/ 0, /*input_generation*/ 0
            ),
            target_index == 1
        );
        for (index, id) in ids.into_iter().enumerate() {
            assert_eq!(
                matches!(
                    probe(&mut server, id).await?,
                    ThreadRetentionAcquireResponse::AlreadyHeld { .. }
                ),
                index == target_index
            );
        }
        assert_eq!(app.side_threads.contains_key(&ids[2]), target_index == 2);
        server.shutdown().await?;
    }
    Ok(())
}

#[test]
fn clear_preserves_a_tracked_successor_and_reports_retention_refusal_after_commit() -> Result<()> {
    // Match the existing clear UI fixtures' stack, not the product stack.
    std::thread::Builder::new()
        .name("clear-retention-boundary".to_string())
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(async {
                    use codex_app_server_client::AppServerEvent;
                    use codex_app_server_protocol::JSONRPCMessage;
                    use codex_app_server_protocol::ServerNotification;
                    use futures::SinkExt;
                    use futures::StreamExt;
                    use serde_json::json;
                    use tokio_tungstenite::tungstenite::Message;

                    for refuse in [false, true] {
                        let (mut app, _, _) = make_test_app_with_channels().await;
                        let predecessor = ThreadId::new();
                        let successor = ThreadId::new();
                        app.primary_thread_id = Some(predecessor);
                        app.active_thread_id = Some(predecessor);
                        app.chat_widget.handle_thread_session(test_thread_session(
                            predecessor,
                            app.config.cwd.to_path_buf(),
                        ));
                        app.ensure_thread_channel(predecessor);
                        // Simulate notification registration before clear's
                        // response is handled. Its cleanup sweep must skip B.
                        app.ensure_thread_channel(successor);
                        let cwd = app.config.cwd.clone();
                        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
                        let endpoint = crate::resolve_remote_addr(&format!(
                            "ws://{}", listener.local_addr()?
                        ))?;
                        let peer = tokio::spawn(async move {
                            let (stream, _) = listener.accept().await?;
                            let mut socket = tokio_tungstenite::accept_async(stream).await?;
                            let mut methods = Vec::new();
                            while let Some(frame) = socket.next().await {
                                let Message::Text(text) = frame? else { continue };
                                let JSONRPCMessage::Request(request) = serde_json::from_str(&text)? else { continue };
                                methods.push(request.method.clone());
                                let result = match request.method.as_str() {
                                    "initialize" => json!({"userAgent": "retention-test/0.0.0"}),
                                    "thread/clear" => {
                                        assert_eq!(request.params.as_ref().unwrap()["threadId"], predecessor.to_string());
                                        json!({"transitionId": "committed-clear", "predecessorThreadId": predecessor,
                                            "successorThread": {"id": successor, "sessionId": successor, "preview": "", "ephemeral": false,
                                                "modelProvider": "test-provider", "createdAt": 1, "updatedAt": 2,
                                                "status": {"type": "idle"}, "cwd": cwd, "cliVersion": "0.0.0", "source": "cli", "turns": []}})
                                    }
                                    "thread/retention/acquire" => {
                                        assert_eq!(request.params.as_ref().unwrap()["threadId"], successor.to_string());
                                        if refuse { json!({"status": "refused", "reason": "authorityUnavailable"}) }
                                        else { json!({"status": "acquired", "grantId": "successor-grant"}) }
                                    }
                                    "thread/unsubscribe" => {
                                        assert_eq!(request.params.as_ref().unwrap()["threadId"], predecessor.to_string());
                                        json!({"status": "unsubscribed"})
                                    }
                                    method => panic!("unexpected clear request: {method}"),
                                };
                                socket.send(Message::Text(json!({"id": request.id, "result": result}).to_string().into())).await?;
                            }
                            Ok::<_, color_eyre::Report>(methods)
                        });
                        let mut session = AppServerSession::new(
                            crate::connect_remote_app_server(endpoint).await?,
                            crate::app_server_session::ThreadParamsMode::Remote,
                        );
                        let mut tui = crate::tui::test_support::make_test_tui()?;
                        app.clear_displayed_session(
                            &mut tui,
                            &mut session,
                            /*initial_user_message*/ None,
                            /*new_thread_name*/ None,
                        ).await;
                        assert_eq!(app.primary_thread_id, Some(successor));
                        assert_eq!(app.current_displayed_thread_id(), Some(successor));
                        if refuse {
                            let event = tokio::time::timeout(Duration::from_secs(5), session.next_event()).await?;
                            let Some(AppServerEvent::ServerNotification(notification)) = event else { panic!("missing clear warning") };
                            let ServerNotification::Warning(warning) = *notification else { panic!("not a warning") };
                            assert_eq!(warning.message, format!("Clear completed, but successor {successor} could not be retained: thread retention refused for {successor}: AuthorityUnavailable. The successor may already be closed or may close while idle."));
                        }
                        session.shutdown().await?;
                        let methods = tokio::time::timeout(Duration::from_secs(5), peer).await???;
                        assert_eq!(methods.iter().filter(|method| method.as_str() == "thread/clear").count(), 1);
                        assert!(methods.iter().any(|method| method == "thread/unsubscribe"));
                    }
                    Ok(())
                })
        })?
        .join()
        .expect("clear retention test thread")
}
