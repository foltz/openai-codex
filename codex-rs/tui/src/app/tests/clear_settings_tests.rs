use super::*;
use pretty_assertions::assert_eq;
use serde_json::json;

pub(super) async fn pump_until_turn_request(
    app: &mut App,
    tui: &mut crate::tui::Tui,
    server: &mut AppServerSession,
    events: &mut tokio::sync::mpsc::UnboundedReceiver<AppEvent>,
    requests: &RecordedRequests,
    expected: usize,
) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(15), async {
        while recorded_params(requests, "turn/start").len() < expected {
            while let Ok(event) = events.try_recv() {
                app.handle_event(tui, server, event).await?;
            }
            let pending = app
                .active_thread_rx
                .as_mut()
                .map(|receiver| std::iter::from_fn(|| receiver.try_recv().ok()).collect::<Vec<_>>())
                .unwrap_or_default();
            for event in pending {
                app.handle_thread_event_now_recovering_file_changes(event)
                    .await;
            }
            if let Ok(Some(event)) =
                tokio::time::timeout(Duration::from_millis(20), server.next_event()).await
            {
                app.handle_app_server_event(server, event).await;
            }
        }
        Ok::<(), color_eyre::eyre::Report>(())
    })
    .await??;
    Ok(())
}

#[test]
fn clear_re_resolves_defaults_and_submits_with_default_mode() -> Result<()> {
    std::thread::Builder::new()
        .name("clear-resolved-defaults".to_string())
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(async {
                    let (mut app, mut events, _ops) = make_test_app_with_channels().await;
                    app.config.features.disable(Feature::Plugins)?;
                    app.cli_kv_overrides
                        .push(("features.plugins".to_string(), TomlValue::Boolean(false)));
                    let home = tempdir()?;
                    app.config.codex_home = home.path().to_path_buf().abs();
                    app.config.sqlite = SqliteConfig::new_for_testing(home.path().abs());
                    let path = home.path().join("config.toml");
                    std::fs::write(
                        &path,
                        "model = 'launch-model'\nmodel_reasoning_effort = 'medium'\n",
                    )?;
                    let (mut server, requests, proxy) = start_recording_app_server(
                        &app.config,
                        /*blocked_thread_list*/ None,
                        /*failed_thread_name*/ None,
                    )
                    .await?;
                    let mut tui = crate::tui::test_support::make_test_tui()?;
                    app.start_fresh_session_with_summary_hint(
                        &mut tui,
                        &mut server,
                        /*initial_user_message*/ None,
                        /*new_thread_name*/ None,
                        /*session_start_source*/ None,
                    )
                    .await;
                    for (index, effort) in [Some("high"), None].into_iter().enumerate() {
                        let predecessor = app.current_displayed_thread_id().expect("predecessor");
                        app.chat_widget.set_model("live-only-model");
                        app.chat_widget
                            .set_reasoning_effort(Some(ReasoningEffortConfig::Low));
                        let model = format!("saved-model-{index}");
                        // This is a saved-default change, not a live-settings inheritance request.
                        let config = match effort {
                            Some(effort) => {
                                format!("model = {model:?}\nmodel_reasoning_effort = {effort:?}\n")
                            }
                            None => format!("model = {model:?}\n"),
                        };
                        std::fs::write(&path, config)?;
                        let plan =
                            crate::collaboration_modes::plan_mask(app.model_catalog.as_ref())
                                .expect("Plan preset");
                        app.chat_widget.set_collaboration_mask(plan);
                        app.handle_event(
                            &mut tui,
                            &mut server,
                            AppEvent::ClearUiAndSubmitUserMessage {
                                text: "continue on successor".to_string(),
                            },
                        )
                        .await?;
                        let successor = app.current_displayed_thread_id().expect("successor");
                        assert_ne!(successor, predecessor);
                        pump_until_turn_request(
                            &mut app,
                            &mut tui,
                            &mut server,
                            &mut events,
                            &requests,
                            index + 1,
                        )
                        .await?;
                        let turn = &recorded_params(&requests, "turn/start")[index];
                        assert_eq!(
                            (
                                &turn["threadId"],
                                &turn["model"],
                                &turn["effort"],
                                &turn["collaborationMode"]["mode"]
                            ),
                            (
                                &json!(successor.to_string()),
                                &json!(model),
                                &json!(effort),
                                &json!("default")
                            )
                        );
                        let clear = &recorded_params(&requests, "thread/clear")[index];
                        assert_eq!(
                            clear,
                            &serde_json::json!({
                                "threadId": predecessor.to_string(),
                                "modelSettings": {"model": model, "reasoningEffort": effort}
                            })
                        );
                        let read = server
                            .thread_read(successor, /*include_turns*/ false)
                            .await?;
                        let expected_effort = effort.map(|_| ReasoningEffortConfig::High);
                        assert_eq!(
                            (read.model, read.reasoning_effort),
                            (Some(model.clone()), expected_effort.clone())
                        );
                        assert_eq!(
                            (
                                app.chat_widget.current_model(),
                                app.chat_widget.current_reasoning_effort(),
                                app.chat_widget.active_collaboration_mode_kind()
                            ),
                            (model.as_str(), expected_effort.clone(), ModeKind::Default)
                        );
                        if index == 0 {
                            let rendered = render_bottom_popup(&app.chat_widget, /*width*/ 80)
                                .replace(&app.config.cwd.display().to_string(), "<PROJECT>");
                            insta::assert_snapshot!("clear_resolved_model_and_effort", rendered);
                        }
                    }
                    server.shutdown().await?;
                    proxy.await??;
                    Ok(())
                })
        })?
        .join()
        .expect("clear defaults test thread")
}

#[test]
fn clear_default_resolution_failure_preserves_predecessor_and_message() -> Result<()> {
    std::thread::Builder::new()
        .name("clear-defaults-failure".to_string())
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(async {
                    let (mut app, _events, _ops) = make_test_app_with_channels().await;
                    app.config.features.disable(Feature::Plugins)?;
                    app.cli_kv_overrides
                        .push(("features.plugins".to_string(), TomlValue::Boolean(false)));
                    let home = tempdir()?;
                    app.config.codex_home = home.path().to_path_buf().abs();
                    app.config.sqlite = SqliteConfig::new_for_testing(home.path().abs());
                    let (mut server, requests, proxy) = start_recording_app_server(
                        &app.config,
                        /*blocked_thread_list*/ None,
                        /*failed_thread_name*/ None,
                    )
                    .await?;
                    let mut tui = crate::tui::test_support::make_test_tui()?;
                    app.start_fresh_session_with_summary_hint(
                        &mut tui,
                        &mut server,
                        /*initial_user_message*/ None,
                        /*new_thread_name*/ None,
                        /*session_start_source*/ None,
                    )
                    .await;
                    let predecessor = app.current_displayed_thread_id().expect("predecessor");
                    let original_config = app.config.clone();
                    std::fs::write(home.path().join("config.toml"), "invalid = [")?;
                    app.handle_event(
                        &mut tui,
                        &mut server,
                        AppEvent::ClearUiAndSubmitUserMessage {
                            text: "keep this message".to_string(),
                        },
                    )
                    .await?;
                    assert_eq!(app.current_displayed_thread_id(), Some(predecessor));
                    assert_eq!(app.config, original_config);
                    assert_eq!(
                        app.chat_widget.composer_text_with_pending(),
                        "keep this message"
                    );
                    assert!(recorded_params(&requests, "thread/clear").is_empty());
                    assert!(recorded_params(&requests, "thread/unsubscribe").is_empty());
                    assert!(recorded_params(&requests, "turn/start").is_empty());
                    server.shutdown().await?;
                    proxy.await??;
                    Ok(())
                })
        })?
        .join()
        .expect("clear defaults failure thread")
}
