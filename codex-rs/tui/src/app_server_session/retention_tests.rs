use super::super::AppServerSession;
use super::super::ResumeModelSettings;
use super::super::start_thread_with_request_handle;
use super::RetentionClient;
use crate::legacy_core::config::Config;
use crate::legacy_core::config::ConfigBuilder;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::ThreadRetentionAcquireParams;
use codex_app_server_protocol::ThreadRetentionAcquireResponse;
use codex_app_server_protocol::ThreadRetentionReleaseParams;
use codex_app_server_protocol::ThreadRetentionReleaseResponse;
use codex_protocol::ThreadId;
use color_eyre::eyre::Result;
use pretty_assertions::assert_eq;

#[test]
fn retention_warnings_are_visible_and_ineligible_is_once_per_connection() {
    use crate::history_cell::HistoryCell;
    let (client, mut warnings) = RetentionClient::new();
    for _ in 0..2 {
        let (commit_tx, _commit_rx) = tokio::sync::oneshot::channel();
        super::PendingRetention {
            commit_tx,
            client: client.clone(),
            ineligible: true,
        }
        .commit();
    }
    let warning = warnings.try_recv().unwrap();
    assert!(warnings.try_recv().is_err());
    let rendered = crate::history_cell::new_warning_event(warning.message)
        .transcript_lines(/*width*/ 200)
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    insta::assert_snapshot!(rendered, @"⚠ This connection cannot retain idle sessions. Idle retirement may close the CLI.");
    client.warn_clear("successor", &color_eyre::eyre::eyre!("refused"));
    let rendered = crate::history_cell::new_warning_event(warnings.try_recv().unwrap().message)
        .transcript_lines(/*width*/ 300)
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    insta::assert_snapshot!(rendered, @"⚠ Clear completed, but successor successor could not be retained: refused. The successor may already be closed or may close while idle.");
}

// UI-only startup fixtures supply fabricated thread IDs, not server grants.
// Keep their presentation setup separate from the real acquisition witnesses below.
impl From<super::super::AppServerStartedThread> for super::super::PendingStartupThread {
    fn from(started: super::super::AppServerStartedThread) -> Self {
        let (commit_tx, _commit_rx) = tokio::sync::oneshot::channel();
        let (client, _warnings) = RetentionClient::new();
        Self {
            started,
            retention: super::PendingRetention {
                commit_tx,
                client,
                ineligible: false,
            },
        }
    }
}

async fn held_grant(session: &mut AppServerSession, thread_id: ThreadId) -> Result<String> {
    let response: ThreadRetentionAcquireResponse = session
        .request_handle()
        .request_typed(ClientRequest::ThreadRetentionAcquire {
            request_id: session.next_request_id(),
            params: ThreadRetentionAcquireParams {
                thread_id: thread_id.to_string(),
            },
        })
        .await?;
    match response {
        ThreadRetentionAcquireResponse::AlreadyHeld { grant_id } => Ok(grant_id),
        other => panic!("interactive adoption did not retain its thread: {other:?}"),
    }
}

async fn assert_released(
    session: &mut AppServerSession,
    thread_id: ThreadId,
    grant_id: String,
) -> Result<()> {
    let response: ThreadRetentionReleaseResponse = session
        .request_handle()
        .request_typed(ClientRequest::ThreadRetentionRelease {
            request_id: session.next_request_id(),
            params: ThreadRetentionReleaseParams {
                thread_id: thread_id.to_string(),
                grant_id,
            },
        })
        .await?;
    assert_eq!(response, ThreadRetentionReleaseResponse::NotHeld {});
    Ok(())
}

async fn config(home: &tempfile::TempDir) -> Result<Config> {
    // Persist the delay: the embedded server reloads its own config.
    std::fs::write(
        home.path().join("config.toml"),
        "thread_unload_delay_secs = 5\n",
    )?;
    Ok(ConfigBuilder::default()
        .codex_home(home.path().to_path_buf())
        .fallback_cwd(Some(home.path().to_path_buf()))
        .build()
        .await?)
}

#[tokio::test]
async fn interactive_start_paths_retain_and_unsubscribe_releases() -> Result<()> {
    let home = tempfile::tempdir()?;
    let config = config(&home).await?;
    let mut session = crate::start_embedded_app_server_for_picker(&config).await?;
    let started = session.start_thread(&config).await?;
    let thread_id = started.session.thread_id;
    assert_eq!(
        config.thread_unload_delay,
        std::time::Duration::from_secs(5)
    );
    // The adopted thread must survive idle for more than the configured deadline.
    tokio::time::sleep(std::time::Duration::from_secs(6)).await;
    let grant_id = held_grant(&mut session, thread_id).await?;
    // Repeated adoption is idempotent and retains the exact existing grant.
    session
        .retention
        .retain_thread(session.request_handle(), thread_id.to_string())
        .await?
        .commit();
    assert_eq!(held_grant(&mut session, thread_id).await?, grant_id);
    // Abandoning a repeated adoption must not release the pre-existing grant.
    drop(
        session
            .retention
            .retain_thread(session.request_handle(), thread_id.to_string())
            .await?,
    );
    assert_eq!(held_grant(&mut session, thread_id).await?, grant_id);
    session.thread_unsubscribe(thread_id).await?;
    assert_released(&mut session, thread_id, grant_id).await?;

    let started = start_thread_with_request_handle(
        session.request_handle(),
        session.retention_client(),
        &crate::local_settings::LocalSettings::from(&config),
        config,
        session.thread_params_mode(),
        /*remote_cwd_override*/ None,
        session.thread_tool_transport(),
    )
    .await?;
    let super::super::PendingStartupThread { started, retention } = started;
    retention.commit();
    let thread_id = started.session.thread_id;
    let grant_id = held_grant(&mut session, thread_id).await?;
    session.thread_unsubscribe(thread_id).await?;
    assert_released(&mut session, thread_id, grant_id).await?;
    session.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn interactive_resume_fork_and_clear_retain_the_adopted_thread() -> Result<()> {
    let home = tempfile::tempdir()?;
    let config = config(&home).await?;
    let thread_id = ThreadId::from_string(
        &app_test_support::create_fake_rollout(
            home.path(),
            "2025-01-05T12-00-00",
            "2025-01-05T12:00:00Z",
            "Saved user message",
            Some(config.model_provider_id.as_str()),
            /*git_info*/ None,
        )
        .expect("create source rollout"),
    )?;
    let mut session = crate::start_embedded_app_server_for_picker(&config).await?;
    session
        .resume_thread(
            &crate::local_settings::LocalSettings::from(&config),
            config.clone(),
            thread_id,
            ResumeModelSettings::RestoreFromThread,
        )
        .await?;
    let predecessor_grant = held_grant(&mut session, thread_id).await?;
    let mut fork_config = config;
    fork_config.ephemeral = true;
    let local_settings = crate::local_settings::LocalSettings::from(&fork_config);
    let forked = session
        .fork_thread(&local_settings, fork_config.clone(), thread_id)
        .await?;
    held_grant(&mut session, forked.session.thread_id).await?;
    let side = session
        .fork_side_thread(&local_settings, fork_config, thread_id)
        .await?;
    held_grant(&mut session, side.session.thread_id).await?;
    let cleared = session.thread_clear(thread_id).await?;
    let successor_id = ThreadId::from_string(&cleared.successor_thread.id)?;
    session
        .retain_clear_successor(&cleared.successor_thread.id)
        .await
        .expect("successor retention")
        .commit();
    held_grant(&mut session, successor_id).await?;
    assert_released(&mut session, thread_id, predecessor_grant).await?;
    // Clearing one thread must not discard another thread's retention.
    held_grant(&mut session, forked.session.thread_id).await?;
    session.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn interactive_retention_refusal_is_not_successful_adoption() -> Result<()> {
    let home = tempfile::tempdir()?;
    let config = config(&home).await?;
    let session = crate::start_embedded_app_server_for_picker(&config).await?;
    let error = session
        .retention
        .retain_thread(session.request_handle(), ThreadId::new().to_string())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("UnknownThread"), "{error}");
    session.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn cancelled_remote_acquisition_releases_its_late_grant() -> Result<()> {
    cancelled_acquisition_retry(false).await
}

#[tokio::test]
async fn failed_release_prevents_a_retry_from_claiming_retention() -> Result<()> {
    cancelled_acquisition_retry(true).await
}

async fn cancelled_acquisition_retry(fail_release: bool) -> Result<()> {
    use codex_app_server_client::AppServerRequestHandle;
    use codex_app_server_client::RemoteAppServerClient;
    use codex_app_server_client::RemoteAppServerConnectArgs;
    use codex_app_server_client::RemoteAppServerEndpoint;
    use futures::SinkExt;
    use futures::StreamExt;
    use serde_json::json;
    use tokio::sync::oneshot;
    use tokio::time::Duration;
    use tokio::time::timeout;
    use tokio_tungstenite::tungstenite::Message;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (requested_tx, requested_rx) = oneshot::channel();
    let (respond_tx, respond_rx) = oneshot::channel();
    let (releasing_tx, releasing_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let initialize: serde_json::Value =
            serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        socket
            .send(Message::Text(
                json!({
                    "jsonrpc": "2.0", "id": initialize["id"],
                    "result": {"userAgent": "codex_cli_rs/0.0.0", "codexHome": "/tmp/test"}
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        let _initialized = socket.next().await.unwrap().unwrap();
        let acquire: serde_json::Value =
            serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        assert_eq!(acquire["method"], "thread/retention/acquire");
        assert_eq!(acquire["params"], json!({"threadId": "cancelled-thread"}));
        requested_tx.send(()).unwrap();
        respond_rx.await.unwrap();
        socket
            .send(Message::Text(
                json!({
                    "jsonrpc": "2.0", "id": acquire["id"],
                    "result": {"status": "acquired", "grantId": "late-grant"}
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        let release: serde_json::Value =
            serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        assert_eq!(release["method"], "thread/retention/release");
        assert_eq!(
            release["params"],
            json!({"threadId": "cancelled-thread", "grantId": "late-grant"})
        );
        releasing_tx.send(()).unwrap();
        // A retried adoption must wait for the old release acknowledgement.
        assert!(
            timeout(Duration::from_millis(100), socket.next())
                .await
                .is_err()
        );
        socket
            .send(Message::Text(
                if fail_release {
                    json!({"jsonrpc": "2.0", "id": release["id"], "error": {"code": -32603, "message": "release failed"}})
                } else { json!({
                    "jsonrpc": "2.0", "id": release["id"], "result": {"status": "released"}
                }) }
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        if fail_release {
            assert!(
                timeout(Duration::from_millis(100), socket.next())
                    .await
                    .is_err()
            );
            return;
        }
        let acquire: serde_json::Value =
            serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        assert_eq!(acquire["method"], "thread/retention/acquire");
        socket.send(Message::Text(json!({"jsonrpc": "2.0", "id": acquire["id"], "result": {"status": "acquired", "grantId": "retry-grant"}}).to_string().into())).await.unwrap();
        // A committed retry must not be released by the cancelled predecessor.
        assert!(
            timeout(Duration::from_millis(100), socket.next())
                .await
                .is_err()
        );
    });
    let client = RemoteAppServerClient::connect(RemoteAppServerConnectArgs {
        endpoint: RemoteAppServerEndpoint::WebSocket {
            websocket_url: format!("ws://{address}"),
            auth_token: None,
        },
        client_name: "retention-test".to_string(),
        client_version: "0.0.0".to_string(),
        experimental_api: true,
        mcp_server_openai_form_elicitation: false,
        interactive_client: true,
        opt_out_notification_methods: Vec::new(),
        channel_capacity: 8,
    })
    .await?;
    let (retention, _warnings) = RetentionClient::new();
    let handle = AppServerRequestHandle::Remote(client.request_handle());
    let initial_retention = retention.clone();
    let initial_handle = handle.clone();
    let adoption = tokio::spawn(async move {
        initial_retention
            .retain_thread(initial_handle, "cancelled-thread".to_string())
            .await
    });
    timeout(Duration::from_secs(5), requested_rx).await??;
    adoption.abort();
    assert!(adoption.await.unwrap_err().is_cancelled());
    respond_tx.send(()).unwrap();
    timeout(Duration::from_secs(5), releasing_rx).await??;
    let retry = retention
        .retain_thread(handle, "cancelled-thread".to_string())
        .await;
    if fail_release {
        assert!(
            retry
                .unwrap_err()
                .to_string()
                .contains("prior retention cleanup has not settled")
        );
    } else {
        retry?.commit();
    }
    timeout(Duration::from_secs(5), server).await??;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn abandoned_preparation_releases_a_provisional_grant() -> Result<()> {
    use codex_app_server_protocol::ThreadStartParams;
    use codex_app_server_protocol::ThreadStartResponse;
    use tokio::time::Duration;
    use tokio::time::timeout;

    let home = tempfile::tempdir()?;
    let config = config(&home).await?;
    let mut session = crate::start_embedded_app_server_for_picker(&config).await?;
    let started: ThreadStartResponse = session
        .request_handle()
        .request_typed(ClientRequest::ThreadStart {
            request_id: session.next_request_id(),
            params: ThreadStartParams::default(),
        })
        .await?;
    let thread_id = ThreadId::from_string(&started.thread.id)?;
    let pending = session
        .retention
        .retain_thread(session.request_handle(), started.thread.id.clone())
        .await?;
    let grant_id = held_grant(&mut session, thread_id).await?;
    drop(pending);
    // Observe the release through the public API, without releasing the grant
    // ourselves. A later acquire must mint a different handle.
    let new_grant = timeout(Duration::from_secs(5), async {
        loop {
            let response: ThreadRetentionAcquireResponse = session
                .request_handle()
                .request_typed(ClientRequest::ThreadRetentionAcquire {
                    request_id: session.next_request_id(),
                    params: ThreadRetentionAcquireParams {
                        thread_id: thread_id.to_string(),
                    },
                })
                .await?;
            match response {
                ThreadRetentionAcquireResponse::Acquired { grant_id } => {
                    return Ok::<_, color_eyre::Report>(grant_id);
                }
                ThreadRetentionAcquireResponse::AlreadyHeld { .. } => {
                    tokio::task::yield_now().await
                }
                other => panic!("unexpected retention refusal: {other:?}"),
            }
        }
    })
    .await??;
    assert_ne!(grant_id, new_grant);
    session.thread_unsubscribe(thread_id).await?;
    session.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn observer_resume_does_not_retain() -> Result<()> {
    let home = tempfile::tempdir()?;
    let config = config(&home).await?;
    let thread_id = ThreadId::from_string(
        &app_test_support::create_fake_rollout(
            home.path(),
            "2025-01-05T12-00-00",
            "2025-01-05T12:00:00Z",
            "Saved message",
            Some(config.model_provider_id.as_str()),
            None,
        )
        .expect("source rollout"),
    )?;
    let mut session = crate::start_embedded_app_server_for_picker(&config).await?;
    session
        .observe_thread(
            &crate::local_settings::LocalSettings::from(&config),
            config,
            thread_id,
            ResumeModelSettings::RestoreFromThread,
        )
        .await?;
    let response: ThreadRetentionAcquireResponse = session
        .request_handle()
        .request_typed(ClientRequest::ThreadRetentionAcquire {
            request_id: session.next_request_id(),
            params: ThreadRetentionAcquireParams {
                thread_id: thread_id.to_string(),
            },
        })
        .await?;
    assert!(
        matches!(response, ThreadRetentionAcquireResponse::Acquired { .. }),
        "observer acquired a grant: {response:?}"
    );
    session.thread_unsubscribe(thread_id).await?;
    session.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn definite_server_rejection_allows_later_adoption() -> Result<()> {
    use codex_app_server_client::AppServerRequestHandle;
    use codex_app_server_client::RemoteAppServerClient;
    use codex_app_server_client::RemoteAppServerConnectArgs;
    use codex_app_server_client::RemoteAppServerEndpoint;
    use futures::SinkExt;
    use futures::StreamExt;
    use serde_json::json;
    use tokio_tungstenite::tungstenite::Message;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let initialize: serde_json::Value =
            serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        socket.send(Message::Text(json!({"jsonrpc": "2.0", "id": initialize["id"], "result": {"userAgent": "codex_cli_rs/0.0.0", "codexHome": "/tmp/test"}}).to_string().into())).await.unwrap();
        let _initialized = socket.next().await.unwrap().unwrap();
        for refuse in [true, false] {
            let acquire: serde_json::Value =
                serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap())
                    .unwrap();
            assert_eq!(acquire["method"], "thread/retention/acquire");
            let response = if refuse {
                json!({"jsonrpc": "2.0", "id": acquire["id"], "error": {"code": -32601, "message": "method unavailable"}})
            } else {
                json!({"jsonrpc": "2.0", "id": acquire["id"], "result": {"status": "acquired", "grantId": "retry-grant"}})
            };
            socket
                .send(Message::Text(response.to_string().into()))
                .await
                .unwrap();
        }
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), socket.next())
                .await
                .is_err()
        );
    });
    let client = RemoteAppServerClient::connect(RemoteAppServerConnectArgs {
        endpoint: RemoteAppServerEndpoint::WebSocket {
            websocket_url: format!("ws://{address}"),
            auth_token: None,
        },
        client_name: "retention-test".to_string(),
        client_version: "0.0.0".to_string(),
        experimental_api: true,
        mcp_server_openai_form_elicitation: false,
        interactive_client: true,
        opt_out_notification_methods: Vec::new(),
        channel_capacity: 8,
    })
    .await?;
    let handle = AppServerRequestHandle::Remote(client.request_handle());
    let (retention, _warnings) = RetentionClient::new();
    let error = retention
        .retain_thread(handle.clone(), "retry-thread".to_string())
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("method unavailable"));
    assert!(matches!(
        error.downcast_ref::<codex_app_server_client::TypedRequestError>(),
        Some(codex_app_server_client::TypedRequestError::Server { .. })
    ));
    retention
        .retain_thread(handle, "retry-thread".to_string())
        .await?
        .commit();
    tokio::time::timeout(std::time::Duration::from_secs(5), server).await??;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn retention_transport_failure_preserves_reconnect_cause() -> Result<()> {
    use codex_app_server_client::AppServerRequestHandle;
    use codex_app_server_client::RemoteAppServerClient;
    use codex_app_server_client::RemoteAppServerConnectArgs;
    use codex_app_server_client::RemoteAppServerEndpoint;
    use codex_app_server_client::TypedRequestError;
    use futures::SinkExt;
    use futures::StreamExt;
    use serde_json::json;
    use tokio_tungstenite::tungstenite::Message;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let initialize: serde_json::Value =
            serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        socket.send(Message::Text(json!({"jsonrpc": "2.0", "id": initialize["id"], "result": {"userAgent": "codex_cli_rs/0.0.0", "codexHome": "/tmp/test"}}).to_string().into())).await.unwrap();
        let _initialized = socket.next().await.unwrap().unwrap();
        let acquire: serde_json::Value =
            serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        assert_eq!(acquire["method"], "thread/retention/acquire");
        socket.close(None).await.unwrap();
    });
    let client = RemoteAppServerClient::connect(RemoteAppServerConnectArgs {
        endpoint: RemoteAppServerEndpoint::WebSocket {
            websocket_url: format!("ws://{address}"),
            auth_token: None,
        },
        client_name: "retention-test".to_string(),
        client_version: "0.0.0".to_string(),
        experimental_api: true,
        mcp_server_openai_form_elicitation: false,
        interactive_client: true,
        opt_out_notification_methods: Vec::new(),
        channel_capacity: 8,
    })
    .await?;
    let (retention, _warnings) = RetentionClient::new();
    let error = retention
        .retain_thread(
            AppServerRequestHandle::Remote(client.request_handle()),
            "disconnected-thread".to_string(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<TypedRequestError>(),
        Some(TypedRequestError::Transport { .. })
    ));
    tokio::time::timeout(std::time::Duration::from_secs(5), server).await??;
    client.shutdown().await?;
    Ok(())
}
