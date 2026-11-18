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
    boxed_future!(startup_event_owns_provisional_retention_until_installation_scenario()).await
}

async fn startup_event_owns_provisional_retention_until_installation_scenario() -> Result<()> {
    for outcome in [
        StartupOutcome::Accepted,
        StartupOutcome::FailedDelivery,
        StartupOutcome::Offline,
        StartupOutcome::Stale,
    ] {
        let (mut app, _, _) = make_test_app_with_channels().await;
        app.config
            .features
            .set_enabled(codex_features::Feature::Plugins, false)?;
        let mut server =
            boxed_future!(crate::start_embedded_app_server_for_picker(&app.config)).await?;
        let pending = boxed_future!(crate::app_server_session::prepare_startup_thread(
            server.request_handle(),
            &app.local_settings,
            app.config.clone(),
            server.thread_params_mode(),
            /*remote_cwd_override*/ None,
            server.thread_tool_transport(),
            crate::app_server_session::StartupLaunchChoices::default(),
            Some(server.retention_client()),
        ))
        .await?;
        let thread_id = pending.started.session.thread_id;
        assert!(matches!(
            probe(&mut server, thread_id).await?,
            ThreadRetentionAcquireResponse::AlreadyHeld { .. }
        ));
        app.pending_startup_thread_start = !matches!(outcome, StartupOutcome::Stale);
        let event = AppEvent::StartupThreadStarted {
            backend_id: server.backend_id(),
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
