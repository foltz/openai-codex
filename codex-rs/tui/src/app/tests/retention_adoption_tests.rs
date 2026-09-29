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
        app.side_threads
            .insert(ids[2], crate::app::side::SideThreadState::new(ids[0]));
        app.shutdown_current_thread_except(&mut server, Some(ids[target_index]))
            .await;
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
