use super::STRUCTURED_RESPONSE_MAX_BYTES;
use super::TemporaryStructuredThreadOptions;
use super::collect_structured_response;
use super::start_temporary_thread;
use crate::legacy_core::config::ConfigBuilder;
use crate::test_support::PathBufExt;
use codex_app_server_client::AppServerClient;
use codex_app_server_client::AppServerEvent;
use codex_app_server_protocol::AskForApproval;
use codex_app_server_protocol::ItemCompletedNotification;
use codex_app_server_protocol::SandboxPolicy;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ThreadItemPayload;
use codex_app_server_protocol::ThreadSource;
use codex_app_server_protocol::Turn;
use codex_app_server_protocol::TurnCompletedNotification;
use codex_app_server_protocol::TurnStatus;
use codex_config::LoaderOverrides;
use codex_exec_server::EnvironmentManager;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::mpsc::unbounded_channel;

fn agent_message_notification(turn_id: &str, text: &str) -> ServerNotification {
    ServerNotification::ItemCompleted(ItemCompletedNotification::new(
        "thread-1".to_string(),
        turn_id.to_string(),
        ThreadItemPayload::AgentMessage {
            moderation: None,
            id: "message-1".to_string(),
            text: text.to_string(),
            phase: None,
            memory_citation: None,
            delivery: None,
            questions: None,
        },
        /*started_at_ms*/ None,
        /*completed_at_ms*/ 0,
    ))
}

fn turn_completed_notification(turn_id: &str, status: TurnStatus) -> ServerNotification {
    ServerNotification::TurnCompleted(TurnCompletedNotification {
        thread_id: "thread-1".to_string(),
        turn: Turn {
            id: turn_id.to_string(),
            root_turn_id: None,
            items: Vec::new(),
            items_view: Default::default(),
            status,
            error: None,
            started_at: None,
            completed_at: None,
            duration_ms: None,
        },
    })
}

#[tokio::test]
async fn managed_workspace_default_respects_read_only_availability() -> color_eyre::Result<()> {
    for read_only_allowed in [true, false] {
        let codex_home = tempdir()?;
        let requirements_path = codex_home.path().join("requirements.toml");
        std::fs::write(
            &requirements_path,
            format!(
                "allowed_approval_policies = [\"on-request\"]\n\
                 default_permissions = \":workspace\"\n\n\
                 [allowed_permission_profiles]\n\
                 \":read-only\" = {read_only_allowed}\n\
                 \":workspace\" = true\n"
            ),
        )?;
        let loader_overrides = LoaderOverrides {
            system_requirements_path: Some(requirements_path),
            ignore_project_config: true,
            ..LoaderOverrides::without_managed_config_for_tests()
        };
        let config = ConfigBuilder::default()
            .codex_home(codex_home.path().to_path_buf())
            .fallback_cwd(Some(codex_home.path().to_path_buf()))
            .loader_overrides(loader_overrides.clone())
            .build()
            .await?;
        let options = TemporaryStructuredThreadOptions {
            thread_source: ThreadSource::Feature("thread_title".to_string()),
            model: "gpt-5.2".to_string(),
            model_provider: config.model_provider_id.clone(),
            cwd: config.cwd.to_string_lossy().into_owned(),
            active_permission_profile: config
                .permissions
                .active_permission_profile()
                .map(|profile| profile.id),
        };
        let client = crate::start_embedded_app_server(
            Default::default(),
            config,
            Vec::new(),
            loader_overrides,
            /*strict_config*/ false,
            Default::default(),
            codex_feedback::CodexFeedback::new(),
            /*log_db*/ None,
            /*state_db*/ None,
            Arc::new(EnvironmentManager::default_for_tests()),
            Default::default(),
            std::sync::Arc::new(codex_app_server_client::InProcessHost::default()),
        )
        .await?;
        let mut app_server = AppServerClient::InProcess(client);
        let result = start_temporary_thread(&app_server.request_handle(), options).await;
        // Thread/start warnings precede thread/started on the connection.
        tokio::time::timeout(std::time::Duration::from_secs(/*secs*/ 5), async {
            loop {
                let AppServerEvent::ServerNotification(notification) =
                    app_server.next_event().await.expect("thread startup event")
                else {
                    continue;
                };
                match *notification {
                    ServerNotification::ConfigWarning(warning) => {
                        assert!(
                            !warning.summary.contains("`approval_policy`"),
                            "{warning:?}"
                        );
                    }
                    ServerNotification::ThreadStarted(_) => break,
                    _ => {}
                }
            }
        })
        .await?;
        app_server.shutdown().await?;

        if read_only_allowed {
            let response = result?.response;
            assert!(
                response.retention.is_some(),
                "start requests explicit retention evidence"
            );
            assert_eq!(
                (
                    response.active_permission_profile.map(|profile| profile.id),
                    response.approval_policy,
                    response.sandbox,
                ),
                (
                    Some(":read-only".to_string()),
                    AskForApproval::OnRequest,
                    SandboxPolicy::ReadOnly {
                        network_access: false,
                    },
                ),
            );
        } else {
            assert_eq!(
                result.expect_err("read-only must be allowed").to_string(),
                "temporary structured thread did not start with read-only permissions",
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn preserves_custom_permissions_and_disables_required_mcp_servers() -> color_eyre::Result<()>
{
    let (chat_widget, _, _, _) =
        crate::chatwidget::tests::make_chatwidget_manual_with_sender().await;
    let mut config = chat_widget.config_ref().clone();
    let codex_home = tempdir()?;
    let denied_path = codex_home.path().join("denied");
    let denied_key = toml::Value::String(denied_path.display().to_string());
    std::fs::write(
        codex_home.path().join("config.toml"),
        format!(
            "default_permissions = \"title-restricted\"\n\n\
             [permissions.title-restricted.filesystem]\n\
             \":root\" = \"read\"\n\
             {denied_key} = \"deny\"\n\n\
             [mcp_servers.forbidden]\n\
             command = \"codex-auto-title-missing-mcp\"\n\
             required = true\n"
        ),
    )?;
    config.codex_home = codex_home.path().to_path_buf().abs();
    config.sqlite =
        codex_state::SqliteConfig::new_for_testing(codex_home.path().to_path_buf().abs());

    let app_server = crate::start_embedded_app_server_for_picker(&config).await?;
    let response = start_temporary_thread(
        &app_server.request_handle(),
        TemporaryStructuredThreadOptions {
            thread_source: ThreadSource::Feature("thread_title".to_string()),
            model: "gpt-5.2".to_string(),
            model_provider: config.model_provider_id.clone(),
            cwd: config.cwd.display().to_string(),
            active_permission_profile: Some("title-restricted".to_string()),
        },
    )
    .await?
    .response;

    assert!(
        response.retention.is_some(),
        "helper start requests explicit retention"
    );
    assert_eq!(
        (
            response.active_permission_profile.map(|profile| profile.id),
            response.model_provider,
            response.thread.ephemeral,
            response.thread.thread_source,
        ),
        (
            Some("title-restricted".to_string()),
            config.model_provider_id,
            true,
            Some(ThreadSource::Feature("thread_title".to_string())),
        )
    );

    app_server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn returns_latest_matching_assistant_message() {
    let (sender, receiver) = unbounded_channel();
    sender
        .send(agent_message_notification("other-turn", "ignore me"))
        .expect("send unrelated assistant message");
    sender
        .send(turn_completed_notification(
            "other-turn",
            TurnStatus::Completed,
        ))
        .expect("send unrelated completion");
    sender
        .send(agent_message_notification("turn-1", "first"))
        .expect("send first assistant message");
    sender
        .send(agent_message_notification("turn-1", "final"))
        .expect("send final assistant message");
    sender
        .send(turn_completed_notification("turn-1", TurnStatus::Completed))
        .expect("send matching completion");

    let response = collect_structured_response(receiver, "turn-1")
        .await
        .expect("collect structured response");

    assert_eq!(response, "final");
}

#[tokio::test]
async fn rejects_failed_turn() {
    let (sender, receiver) = unbounded_channel();
    sender
        .send(turn_completed_notification("turn-1", TurnStatus::Failed))
        .expect("send failed completion");

    let error = collect_structured_response(receiver, "turn-1")
        .await
        .expect_err("failed turn should not produce a response");

    assert_eq!(
        error.to_string(),
        "temporary structured turn ended with status Failed"
    );
}

#[tokio::test]
async fn rejects_completion_without_assistant_message() {
    let (sender, receiver) = unbounded_channel();
    sender
        .send(turn_completed_notification("turn-1", TurnStatus::Completed))
        .expect("send completion");

    let error = collect_structured_response(receiver, "turn-1")
        .await
        .expect_err("completion without assistant message should fail");

    assert_eq!(
        error.to_string(),
        "temporary structured turn completed without a response"
    );
}

#[tokio::test]
async fn rejects_closed_notification_channel() {
    let (sender, receiver) = unbounded_channel();
    drop(sender);

    let error = collect_structured_response(receiver, "turn-1")
        .await
        .expect_err("closed notification channel should fail");

    assert_eq!(
        error.to_string(),
        "temporary structured turn notification channel closed"
    );
}

#[tokio::test]
async fn rejects_oversized_assistant_message() {
    let (sender, receiver) = unbounded_channel();
    let oversized = "x".repeat(STRUCTURED_RESPONSE_MAX_BYTES + 1);
    sender
        .send(agent_message_notification("turn-1", &oversized))
        .expect("send oversized assistant message");
    sender
        .send(turn_completed_notification("turn-1", TurnStatus::Completed))
        .expect("send matching completion");

    let error = collect_structured_response(receiver, "turn-1")
        .await
        .expect_err("oversized assistant message should be rejected");

    assert_eq!(
        error.to_string(),
        "temporary structured response exceeds 8192 bytes"
    );
}

/// Exercise the same owned carrier used by title/recap events. It must hold
/// the idle window, then release before turn completion, or on failed startup.
#[tokio::test]
async fn temporary_grant_covers_only_the_start_to_turn_window() {
    use super::{TemporaryThread, run_temporary_structured_turn};
    use codex_app_server_client::{CloudClient, CloudOperation};
    use codex_app_server_protocol::{
        ClientRequest, ThreadRetentionAcquireResponse, ThreadRetentionReleaseResponse,
        ThreadUnsubscribeResponse, TurnStartResponse,
    };
    use tokio_util::sync::CancellationToken;

    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        for outcome in ["active", "refused", "cancelled", "discarded"] {
            let (cloud, mut commands, _events) = CloudClient::channel(8);
            let client = AppServerClient::Cloud(cloud);
            let handle = client.request_handle();
            let retention = crate::app_server_session::RetentionClient::temporary_start_grant(
                handle.clone(),
                "temporary".into(),
                Some(ThreadRetentionAcquireResponse::Acquired {
                    grant_id: "exact".into(),
                }),
            )
            .await;
            assert!(retention.is_some());
            let thread = TemporaryThread {
                id: "temporary".into(),
                retention,
            };
            tokio::task::yield_now().await;
            assert!(
                commands.try_recv().is_err(),
                "queued event retains its grant"
            );
            if outcome == "discarded" {
                drop(thread);
                let command = commands.recv().await.unwrap();
                let CloudOperation::Request(request) = command.operation else {
                    panic!("request")
                };
                let ClientRequest::ThreadRetentionRelease { params, .. } = *request else {
                    panic!("release")
                };
                assert_eq!(
                    (params.thread_id.as_str(), params.grant_id.as_str()),
                    ("temporary", "exact")
                );
                command
                    .response
                    .send(Ok(Ok(serde_json::to_value(
                        ThreadRetentionReleaseResponse::Released {},
                    )
                    .unwrap())))
                    .unwrap();
                continue;
            }
            let cancel = CancellationToken::new();
            if outcome == "cancelled" {
                cancel.cancel();
            }
            let (notifications, receiver) = unbounded_channel();
            let task = tokio::spawn(run_temporary_structured_turn(
                handle,
                thread,
                "prompt".into(),
                serde_json::json!({}),
                None,
                receiver,
                cancel,
            ));
            if outcome != "cancelled" {
                let command = commands.recv().await.unwrap();
                let CloudOperation::Request(request) = command.operation else {
                    panic!("request")
                };
                assert!(matches!(*request, ClientRequest::TurnStart { .. }));
                assert!(
                    commands.try_recv().is_err(),
                    "no release before turn/start responds"
                );
                if outcome == "refused" {
                    command
                        .response
                        .send(Err(std::io::Error::other("injected turn refusal")))
                        .unwrap();
                } else {
                    let ServerNotification::TurnCompleted(completed) =
                        turn_completed_notification("turn", TurnStatus::InProgress)
                    else {
                        unreachable!()
                    };
                    command
                        .response
                        .send(Ok(Ok(serde_json::to_value(TurnStartResponse {
                            turn: completed.turn,
                        })
                        .unwrap())))
                        .unwrap();
                }
            }
            let mut released = false;
            let mut unsubscribed = false;
            while !released || !unsubscribed {
                let command = commands.recv().await.unwrap();
                let CloudOperation::Request(request) = command.operation else {
                    panic!("request")
                };
                match *request {
                    ClientRequest::ThreadRetentionRelease { params, .. } => {
                        assert_eq!(
                            (params.thread_id.as_str(), params.grant_id.as_str()),
                            ("temporary", "exact")
                        );
                        command
                            .response
                            .send(Ok(Ok(serde_json::to_value(
                                ThreadRetentionReleaseResponse::Released {},
                            )
                            .unwrap())))
                            .unwrap();
                        released = true;
                        if outcome == "active" {
                            assert!(!task.is_finished(), "release precedes completion");
                            notifications
                                .send(agent_message_notification("turn", "answer"))
                                .unwrap();
                            notifications
                                .send(turn_completed_notification("turn", TurnStatus::Completed))
                                .unwrap();
                        }
                    }
                    ClientRequest::ThreadUnsubscribe { params, .. } => {
                        assert_eq!(params.thread_id, "temporary");
                        command
                            .response
                            .send(Ok(Ok(
                                serde_json::to_value(ThreadUnsubscribeResponse { status: codex_app_server_protocol::ThreadUnsubscribeStatus::Unsubscribed }).unwrap()
                            )))
                            .unwrap();
                        unsubscribed = true;
                    }
                    _ => panic!("unexpected request"),
                }
            }
            let result = task.await.unwrap();
            assert_eq!(result.is_ok(), outcome == "active");
        }
    })
    .await
    .expect("temporary grant lifecycle is bounded");
}

#[tokio::test]
async fn temporary_helpers_do_not_reacquire_or_release_missing_or_existing_grants() {
    use codex_app_server_client::CloudClient;
    use codex_app_server_protocol::{ThreadRetentionAcquireResponse, ThreadRetentionRefusalReason};
    let (cloud, mut commands, _events) = CloudClient::channel(8);
    let client = AppServerClient::Cloud(cloud);
    for evidence in [
        None,
        Some(ThreadRetentionAcquireResponse::AlreadyHeld {
            grant_id: "not-owned".into(),
        }),
        Some(ThreadRetentionAcquireResponse::Refused {
            reason: ThreadRetentionRefusalReason::IneligiblePrincipal,
        }),
    ] {
        let pending = crate::app_server_session::RetentionClient::temporary_start_grant(
            client.request_handle(),
            "temporary".into(),
            evidence,
        )
        .await;
        assert!(pending.is_none());
    }
    tokio::task::yield_now().await;
    assert!(commands.try_recv().is_err());
}
