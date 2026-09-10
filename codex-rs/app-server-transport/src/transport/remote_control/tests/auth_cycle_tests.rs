use super::*;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn reset_waits_for_its_own_generation_and_repeated_resets_need_new_ack() {
    let (reset, completed) = auth_cycle::AuthCycleReset::new();
    let mut requested = reset.requested.subscribe();
    for generation in [1, 2] {
        let reset = reset.clone();
        let waiter = tokio::spawn(async move { reset.reset().await });
        requested.changed().await.expect("request should arrive");
        assert_eq!(*requested.borrow_and_update(), generation);
        assert!(
            !waiter.is_finished(),
            "older acknowledgement must not suffice"
        );
        completed.send_replace(generation);
        waiter
            .await
            .expect("reset task should join")
            .expect("acknowledged reset should succeed");
    }
}

#[tokio::test(start_paused = true)]
async fn reset_timeout_and_supervisor_loss_never_acknowledge_success() {
    let (reset, completed) = auth_cycle::AuthCycleReset::new();
    assert_eq!(
        reset
            .reset()
            .await
            .expect_err("missing ack must time out")
            .kind(),
        io::ErrorKind::TimedOut
    );
    drop(completed);
    assert_eq!(
        reset
            .reset()
            .await
            .expect_err("missing supervisor must refuse")
            .kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[tokio::test]
async fn normally_finished_supervisor_acknowledges_future_resets() {
    let shutdown = CancellationToken::new();
    let (events, _rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (client_name_tx, client_name_rx) = oneshot::channel();
    let (task, handle) = start_remote_control(
        RemoteControlStartConfig {
            remote_control_url: TEST_REMOTE_CONTROL_URL.to_string(),
            installation_id: TEST_INSTALLATION_ID.to_string(),
            policy: RemoteControlPolicy::Allowed,
        },
        None,
        remote_control_auth_manager(),
        events,
        shutdown.clone(),
        Some(client_name_rx),
        RemoteControlStartupMode::DisabledEphemeral,
    )
    .await
    .expect("supervisor should start");
    drop(client_name_tx);
    timeout(Duration::from_secs(1), task)
        .await
        .expect("missing client name should finish normally")
        .expect("supervisor must not panic");
    assert!(!shutdown.is_cancelled());
    for _ in 0..2 {
        handle
            .reset_auth_cycle()
            .await
            .expect("no runtime remains after normal completion");
    }
}

#[tokio::test]
async fn disabled_auth_reset_preserves_desired_state_and_pairing() {
    let shutdown = CancellationToken::new();
    let (events, _rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (task, handle) = start_remote_control(
        RemoteControlStartConfig {
            remote_control_url: TEST_REMOTE_CONTROL_URL.to_string(),
            installation_id: TEST_INSTALLATION_ID.to_string(),
            policy: RemoteControlPolicy::Allowed,
        },
        None,
        remote_control_auth_manager(),
        events,
        shutdown.clone(),
        None,
        RemoteControlStartupMode::DisabledEphemeral,
    )
    .await
    .expect("disabled supervisor should start");
    let desired = *handle.desired_state_tx.borrow();
    let enrollment = handle.current_enrollment.snapshot();
    handle
        .reset_auth_cycle()
        .await
        .expect("disabled reset should acknowledge");
    assert_eq!(*handle.desired_state_tx.borrow(), desired);
    assert_eq!(handle.current_enrollment.snapshot(), enrollment);
    shutdown.cancel();
    task.await.expect("supervisor should stop");
}

#[tokio::test]
async fn active_auth_reset_closes_old_socket_before_ack_and_reconnects_without_disabling() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let home = TempDir::new().expect("home should create");
    let shutdown = CancellationToken::new();
    let (events, _rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (task, handle) = start_remote_control(
        RemoteControlStartConfig {
            remote_control_url: remote_control_url_for_listener(&listener),
            installation_id: TEST_INSTALLATION_ID.to_string(),
            policy: RemoteControlPolicy::Allowed,
        },
        Some(remote_control_state_runtime(&home).await),
        remote_control_auth_manager(),
        events,
        shutdown.clone(),
        None,
        RemoteControlStartupMode::EnabledEphemeral,
    )
    .await
    .expect("supervisor should start");
    let request = accept_http_request(&listener).await;
    respond_with_json(
        request.stream,
        remote_control_server_token_response(
            "srv_e_test",
            "env_test",
            TEST_REMOTE_CONTROL_SERVER_TOKEN,
        ),
    )
    .await;
    let mut old_socket = accept_remote_control_connection(&listener).await;
    let desired = *handle.desired_state_tx.borrow();
    let enrollment = handle.current_enrollment.snapshot();
    handle
        .reset_auth_cycle()
        .await
        .expect("reset must join old runtime");
    timeout(Duration::from_secs(1), async {
        while let Some(message) = old_socket.next().await {
            match message {
                Ok(tungstenite::Message::Close(_)) | Err(_) => break,
                Ok(_) => {}
            }
        }
    })
    .await
    .expect("old socket must have closed before acknowledgement");
    assert_eq!(*handle.desired_state_tx.borrow(), desired);
    assert_eq!(handle.current_enrollment.snapshot(), enrollment);
    let _new_socket = accept_remote_control_connection(&listener).await;
    shutdown.cancel();
    task.await.expect("supervisor should stop");
}

#[tokio::test]
async fn auth_change_retires_an_active_account_a_socket_and_enrolls_with_account_b() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let home = TempDir::new().expect("home should create");
    let auth = remote_control_auth_manager_with_home(&home);
    let shutdown = CancellationToken::new();
    let (events, _rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (task, _handle) = start_remote_control(
        RemoteControlStartConfig {
            remote_control_url: remote_control_url_for_listener(&listener),
            installation_id: TEST_INSTALLATION_ID.to_string(),
            policy: RemoteControlPolicy::Allowed,
        },
        Some(remote_control_state_runtime(&home).await),
        auth.clone(),
        events,
        shutdown.clone(),
        None,
        RemoteControlStartupMode::EnabledEphemeral,
    )
    .await
    .expect("supervisor should start");
    let request = accept_http_request(&listener).await;
    respond_with_json(
        request.stream,
        remote_control_server_token_response("srv_a", "env_a", TEST_REMOTE_CONTROL_SERVER_TOKEN),
    )
    .await;
    let mut old_socket = accept_remote_control_connection(&listener).await;
    save_auth(
        home.path(),
        &remote_control_auth_dot_json(Some("account_b")),
        AuthCredentialsStoreMode::File,
        AuthKeyringBackendKind::default(),
    )
    .expect("new durable account should save");
    auth.reload().await;
    timeout(Duration::from_secs(5), async {
        while let Some(message) = old_socket.next().await {
            match message {
                Ok(tungstenite::Message::Close(_)) | Err(_) => break,
                Ok(_) => {}
            }
        }
    })
    .await
    .expect("auth change must close a live socket without waiting for backoff");
    let request = accept_http_request(&listener).await;
    assert_eq!(
        request.headers.get(REMOTE_CONTROL_ACCOUNT_ID_HEADER),
        Some(&"account_b".to_string())
    );
    respond_with_json(
        request.stream,
        remote_control_server_token_response(
            "srv_b",
            "env_b",
            TEST_REFRESHED_REMOTE_CONTROL_SERVER_TOKEN,
        ),
    )
    .await;
    let (request, _new_socket) = accept_remote_control_backend_connection(&listener).await;
    assert_eq!(
        request.headers.get("authorization"),
        Some(&format!(
            "Bearer {TEST_REFRESHED_REMOTE_CONTROL_SERVER_TOKEN}"
        ))
    );
    shutdown.cancel();
    task.await.expect("supervisor should stop");
}

#[tokio::test]
async fn undelivered_connection_cleanup_ends_supervisor_without_successful_reset_ack() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let home = TempDir::new().expect("home should create");
    let shutdown = CancellationToken::new();
    let (events, mut events_rx) = mpsc::channel(2);
    let (task, handle) = start_remote_control(
        RemoteControlStartConfig {
            remote_control_url: remote_control_url_for_listener(&listener),
            installation_id: TEST_INSTALLATION_ID.to_string(),
            policy: RemoteControlPolicy::Allowed,
        },
        Some(remote_control_state_runtime(&home).await),
        remote_control_auth_manager(),
        events.clone(),
        shutdown.clone(),
        None,
        RemoteControlStartupMode::EnabledEphemeral,
    )
    .await
    .expect("supervisor should start");
    let request = accept_http_request(&listener).await;
    respond_with_json(
        request.stream,
        remote_control_server_token_response(
            "srv_e_test",
            "env_test",
            TEST_REMOTE_CONTROL_SERVER_TOKEN,
        ),
    )
    .await;
    let mut socket = accept_remote_control_connection(&listener).await;
    send_client_event(
        &mut socket,
        ClientEnvelope {
            event: ClientEvent::ClientMessage {
                message: JSONRPCMessage::Request(codex_app_server_protocol::JSONRPCRequest {
                    id: codex_app_server_protocol::RequestId::Integer(1),
                    method: "initialize".to_string(),
                    params: Some(json!({"clientInfo": {"name": "remote-test", "version": "1"}})),
                    trace: None,
                }),
            },
            client_id: ClientId("client-a".to_string()),
            stream_id: None,
            seq_id: Some(0),
            cursor: None,
        },
    )
    .await;
    let _writer = match events_rx.recv().await.expect("client should open") {
        TransportEvent::ConnectionOpened { writer, .. } => writer,
        other => panic!("expected opened connection, got {other:?}"),
    };
    assert!(matches!(
        events_rx.recv().await,
        Some(TransportEvent::IncomingMessage { .. })
    ));
    for id in [900, 901] {
        events
            .send(TransportEvent::ConnectionClosed {
                connection_id: crate::outgoing_message::ConnectionId(id),
            })
            .await
            .expect("queue should fill");
    }
    let error = timeout(Duration::from_secs(3), handle.reset_auth_cycle())
        .await
        .expect("failed cleanup must bound supervisor completion")
        .expect_err("undelivered cleanup cannot acknowledge success");
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    task.await.expect("bounded shutdown must finish normally");
    assert_eq!(
        handle
            .reset_auth_cycle()
            .await
            .expect_err("failure cannot turn into terminal success")
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    shutdown.cancel();
}
