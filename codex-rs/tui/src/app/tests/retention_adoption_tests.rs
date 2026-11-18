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

#[tokio::test]
async fn resume_observation_and_provisional_adoption_have_distinct_custody() -> Result<()> {
    boxed_future!(resume_observation_and_provisional_adoption_scenario()).await
}

async fn resume_observation_and_provisional_adoption_scenario() -> Result<()> {
    {
        let stack_probe = 0u8;
        eprintln!("COLD_TRACE fixture entry sp={:p}", &stack_probe);
    }
    let (mut app, _, _) = make_test_app_with_channels().await;
    app.config
        .features
        .set_enabled(codex_features::Feature::Plugins, false)?;
    {
        let stack_probe = 0u8;
        eprintln!("COLD_TRACE app ready sp={:p}", &stack_probe);
    }
    let mut server =
        boxed_future!(crate::start_embedded_app_server_for_picker(&app.config)).await?;
    {
        let stack_probe = 0u8;
        eprintln!("COLD_TRACE server ready sp={:p}", &stack_probe);
    }
    let id = ThreadId::from_string(
        &app_test_support::create_fake_rollout(
            app.config.codex_home.as_path(),
            "2026-01-01T00-00-00",
            "2026-01-01T00:00:00Z",
            "saved resume source",
            Some(app.config.model_provider_id.as_str()),
            None,
        )
        .expect("resume source rollout"),
    )?;
    {
        let stack_probe = 0u8;
        eprintln!("COLD_TRACE before resume sp={:p}", &stack_probe);
    }
    boxed_future!(server.resume_thread(
        &app.local_settings,
        app.config.clone(),
        id,
        crate::app_server_session::ResumeModelSettings::PreserveExistingThread
    ))
    .await?;
    {
        let stack_probe = 0u8;
        eprintln!("COLD_TRACE client resume returned sp={:p}", &stack_probe);
    }
    let ThreadRetentionAcquireResponse::Acquired { grant_id } = probe(&mut server, id).await?
    else {
        panic!("observing a thread must not have acquired a retention grant")
    };
    let _: codex_app_server_protocol::ThreadRetentionReleaseResponse = server
        .request_handle()
        .request_typed(ClientRequest::ThreadRetentionRelease {
            request_id: server.next_request_id(),
            params: codex_app_server_protocol::ThreadRetentionReleaseParams {
                thread_id: id.to_string(),
                grant_id,
            },
        })
        .await?;

    let (resumed, pending) = boxed_future!(server.prepare_retained_resume(
        &app.local_settings,
        app.config.clone(),
        id,
        crate::app_server_session::ResumeModelSettings::PreserveExistingThread,
        crate::resume_permissions::ResumePermissions::default(),
    ))
    .await?;
    assert_eq!(resumed.session.thread_id, id);
    assert!(matches!(
        probe(&mut server, id).await?,
        ThreadRetentionAcquireResponse::AlreadyHeld { .. }
    ));
    pending.expect("adoption has provisional custody").commit();
    let (_, existing) = boxed_future!(server.prepare_retained_resume(
        &app.local_settings,
        app.config.clone(),
        id,
        crate::app_server_session::ResumeModelSettings::PreserveExistingThread,
        crate::resume_permissions::ResumePermissions::default(),
    ))
    .await?;
    drop(existing);
    tokio::task::yield_now().await;
    assert!(
        matches!(
            probe(&mut server, id).await?,
            ThreadRetentionAcquireResponse::AlreadyHeld { .. }
        ),
        "abandoning an already-held adoption must not release the committed grant"
    );

    let other = ThreadId::from_string(
        &app_test_support::create_fake_rollout(
            app.config.codex_home.as_path(),
            "2026-01-01T00-00-01",
            "2026-01-01T00:00:01Z",
            "saved resume source",
            Some(app.config.model_provider_id.as_str()),
            None,
        )
        .expect("resume source rollout"),
    )?;
    let (_, pending) = boxed_future!(server.prepare_retained_resume(
        &app.local_settings,
        app.config.clone(),
        other,
        crate::app_server_session::ResumeModelSettings::PreserveExistingThread,
        crate::resume_permissions::ResumePermissions::default(),
    ))
    .await?;
    drop(pending);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match probe(&mut server, other).await? {
                ThreadRetentionAcquireResponse::Acquired { .. } => {
                    break Ok::<_, color_eyre::Report>(());
                }
                ThreadRetentionAcquireResponse::AlreadyHeld { .. } => {
                    tokio::task::yield_now().await
                }
                other => panic!("unexpected retention probe: {other:?}"),
            }
        }
    })
    .await??;
    for thread_id in [id, other] {
        server.thread_unsubscribe(thread_id).await?;
    }
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn fork_preparation_keeps_new_grants_provisional_until_installation() -> Result<()> {
    boxed_future!(fork_preparation_retention_scenario()).await
}

async fn fork_preparation_retention_scenario() -> Result<()> {
    for (install, ephemeral) in [(false, false), (true, false), (false, true), (true, true)] {
        let (mut app, _, _) = make_test_app_with_channels().await;
        app.config
            .features
            .set_enabled(codex_features::Feature::Plugins, false)?;
        let source = ThreadId::from_string(
            &app_test_support::create_fake_rollout(
                app.config.codex_home.as_path(),
                "2026-01-01T00-00-00",
                "2026-01-01T00:00:00Z",
                "saved fork source",
                Some(app.config.model_provider_id.as_str()),
                None,
            )
            .expect("source rollout"),
        )?;
        let mut server =
            boxed_future!(crate::start_embedded_app_server_for_picker(&app.config)).await?;
        let started = boxed_future!(server.resume_thread(
            &app.local_settings,
            app.config.clone(),
            source,
            crate::app_server_session::ResumeModelSettings::PreserveExistingThread,
        ))
        .await?;
        app.enqueue_primary_thread_session(started.session, started.turns)
            .await?;
        let mut fork_config = app.config.clone();
        fork_config.ephemeral = ephemeral;
        let (forked, retention) = boxed_future!(server.prepare_retained_fork_thread_at(
            &app.local_settings,
            fork_config,
            source,
            None,
            None,
            crate::app_server_session::ForkGoalContinuation::DeferUntilNextTurn,
            None,
        ))
        .await?;
        let id = forked.session.thread_id;
        assert_ne!(id, source);
        assert_eq!(server.thread_read(id, false).await?.ephemeral, ephemeral);
        assert!(retention.is_some());
        assert!(matches!(
            probe(&mut server, id).await?,
            ThreadRetentionAcquireResponse::AlreadyHeld { .. }
        ));
        if install {
            let mut tui = crate::tui::test_support::make_test_tui()?;
            boxed_future!(app.replace_chat_widget_with_app_server_thread(
                &mut tui,
                forked,
                super::super::session_lifecycle::ThreadAttachPresentation::SessionLineage,
                None,
            ))
            .await?;
            assert_eq!(app.current_displayed_thread_id(), Some(id));
            retention.expect("provisional fork grant").commit();
            assert!(matches!(
                probe(&mut server, id).await?,
                ThreadRetentionAcquireResponse::AlreadyHeld { .. }
            ));
        } else {
            drop(retention);
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    match probe(&mut server, id).await? {
                        ThreadRetentionAcquireResponse::Acquired { .. } => {
                            break Ok::<_, color_eyre::Report>(());
                        }
                        ThreadRetentionAcquireResponse::AlreadyHeld { .. } => {
                            tokio::task::yield_now().await
                        }
                        other => panic!("unexpected retention probe: {other:?}"),
                    }
                }
            })
            .await??;
            assert_eq!(app.current_displayed_thread_id(), Some(source));
        }
        server.thread_unsubscribe(id).await?;
        server.thread_unsubscribe(source).await?;
        server.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn sidebar_event_owns_retention_until_install_or_abandonment() -> Result<()> {
    boxed_future!(sidebar_event_retention_scenario()).await
}

async fn sidebar_event_retention_scenario() -> Result<()> {
    use crate::app_server_session::SideChatStorage;
    for outcome in ["installed", "boundary-failed", "stale", "discarded"] {
        let (mut app, _, _) = make_test_app_with_channels().await;
        app.config
            .features
            .set_enabled(codex_features::Feature::Plugins, false)?;
        enable_side_retention(&mut app)?;
        let parent = ThreadId::from_string(
            &app_test_support::create_fake_rollout(
                app.config.codex_home.as_path(),
                "2026-01-01T00-00-00",
                "2026-01-01T00:00:00Z",
                "sidebar source",
                Some(app.config.model_provider_id.as_str()),
                None,
            )
            .expect("source rollout"),
        )?;
        let mut server =
            boxed_future!(crate::start_embedded_app_server_for_picker(&app.config)).await?;
        let started = boxed_future!(server.resume_thread(
            &app.local_settings,
            app.config.clone(),
            parent,
            crate::app_server_session::ResumeModelSettings::PreserveExistingThread,
        ))
        .await?;
        app.chat_widget
            .handle_thread_session(started.session.clone());
        app.enqueue_primary_thread_session(started.session, started.turns)
            .await?;
        app.active_thread_id = Some(parent);
        let mut fork = boxed_future!(server.sidebar_fork(
            app.local_settings.clone(),
            app.side_fork_config(),
            parent,
            None,
            SideChatStorage::Temporary,
        ))
        .await?;
        assert_eq!(fork.boundary, Ok(()));
        assert!(fork.retention.is_some());
        let child = fork.thread.session.thread_id;
        let ThreadRetentionAcquireResponse::AlreadyHeld { grant_id } =
            probe(&mut server, child).await?
        else {
            panic!("queued sidebar event must own a grant");
        };
        if outcome == "boundary-failed" {
            // Exercise the real event/install refusal path while keeping the
            // minting connection alive; this is not a transport-close test.
            fork.boundary = Err("injected boundary failure".to_string());
        }
        let request = uuid::Uuid::new_v4();
        let event = AppEvent::SideCreated {
            backend_id: server.backend_id(),
            parent,
            request,
            result: Ok(Box::new(fork)),
        };
        tokio::task::yield_now().await;
        if outcome == "discarded" {
            drop(event);
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    match probe(&mut server, child).await? {
                        ThreadRetentionAcquireResponse::Acquired { .. } => {
                            break Ok::<_, color_eyre::Report>(());
                        }
                        ThreadRetentionAcquireResponse::AlreadyHeld { .. } => {
                            tokio::task::yield_now().await
                        }
                        other => panic!("unexpected sidebar probe: {other:?}"),
                    }
                }
            })
            .await??;
            server.thread_unsubscribe(child).await?;
        } else {
            if matches!(outcome, "installed" | "boundary-failed") {
                let pane = app.sidebars.entry(Some(parent)).or_default();
                pane.creation = Some(super::super::sidebar::SideCreation {
                    owner: server.backend_id(),
                    state: super::super::sidebar::CreationState::Waiting {
                        request,
                        started: Instant::now(),
                    },
                    message: None,
                });
                pane.show(
                    super::super::sidebar::SidebarContent::Chooser,
                    super::super::sidebar::PaneFocus::Main,
                );
            }
            let mut tui = crate::tui::test_support::make_test_tui()?;
            boxed_future!(app.handle_event(&mut tui, &mut server, event)).await?;
            assert_eq!(
                app.side_threads.contains_key(&child),
                outcome == "installed"
            );
            if outcome == "boundary-failed" {
                assert!(matches!(
                    app.sidebars[&Some(parent)]
                        .creation
                        .as_ref()
                        .map(|creation| &creation.state),
                    Some(super::super::sidebar::CreationState::Failed { .. })
                ));
            }
            if outcome == "installed" {
                // Revalidation waits for any earlier abandonment release. An
                // accidental drop would mint a DIFFERENT opaque grant here.
                let (_, observation) = boxed_future!(server.prepare_retained_resume(
                    &app.local_settings,
                    app.config.clone(),
                    child,
                    crate::app_server_session::ResumeModelSettings::PreserveExistingThread,
                    crate::resume_permissions::ResumePermissions::default(),
                ))
                .await?;
                assert_eq!(
                    probe(&mut server, child).await?,
                    ThreadRetentionAcquireResponse::AlreadyHeld {
                        grant_id: grant_id.clone()
                    }
                );
                drop(observation);
                assert!(app.discard_side_thread(&mut server, child).await);
            }
            let released: codex_app_server_protocol::ThreadRetentionReleaseResponse = server
                .request_handle()
                .request_typed(ClientRequest::ThreadRetentionRelease {
                    request_id: server.next_request_id(),
                    params: codex_app_server_protocol::ThreadRetentionReleaseParams {
                        thread_id: child.to_string(),
                        grant_id,
                    },
                })
                .await?;
            assert_eq!(
                released,
                codex_app_server_protocol::ThreadRetentionReleaseResponse::NotHeld {}
            );
        }
        server.thread_unsubscribe(parent).await?;
        server.shutdown().await?;
    }
    Ok(())
}
