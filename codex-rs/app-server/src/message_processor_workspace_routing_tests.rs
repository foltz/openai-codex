//! Exercises background routing call sites and explicit account reads through JSON-RPC.

use super::message_processor_tracing_tests::TEST_CONNECTION_ID;
use super::message_processor_tracing_tests::read_response;
use super::*;
use codex_app_server_protocol::GetAccountResponse;
use codex_app_server_protocol::InitializeResponse;
use codex_config::CloudConfigBundleLoader;
use codex_config::LoaderOverrides;
use codex_config::types::AuthCredentialsStoreMode;
use codex_login::AuthKeyringBackendKind;
use pretty_assertions::assert_eq;
use serde_json::json;
use test_case::test_case;
use tokio::sync::mpsc;

#[test_case(None; "signed_out")]
#[test_case(Some("dummy"); "api_key")]
#[tokio::test]
async fn background_routing_call_sites_skip_config(api_key: Option<&str>) -> anyhow::Result<()> {
    let home = tempfile::tempdir()?;
    if let Some(api_key) = api_key {
        codex_login::login_with_api_key(
            home.path(),
            api_key,
            AuthCredentialsStoreMode::File,
            AuthKeyringBackendKind::default(),
        )?;
    }
    let config = Arc::new(
        codex_core::config::ConfigBuilder::default()
            .codex_home(home.path().to_path_buf())
            .loader_overrides(LoaderOverrides::without_managed_config_for_tests())
            .build()
            .await?,
    );
    let auth_manager = Arc::new(
        AuthManager::new(
            home.path().to_path_buf(),
            /*enable_codex_api_key_env*/ false,
            AuthCredentialsStoreMode::File,
            /*forced_chatgpt_workspace_id*/ None,
            Some(config.chatgpt_base_url.clone()),
            AuthKeyringBackendKind::default(),
            config.auth_config().auth_route_config,
        )
        .await,
    );
    let (loads_tx, mut loads_rx) = mpsc::unbounded_channel();
    let loader = CloudConfigBundleLoader::from_getter(move || {
        loads_tx.send(()).expect("config-load observer is alive");
        async { Ok(None) }
    });
    let config_manager = ConfigManager::new_for_tests(
        home.path().to_path_buf(),
        Vec::new(),
        LoaderOverrides::without_managed_config_for_tests(),
        loader,
    );
    let (outgoing_tx, mut outgoing_rx) = mpsc::channel(/*buffer*/ 16);
    let analytics_events_client = AnalyticsEventsClient::disabled();
    let outgoing = Arc::new(OutgoingMessageSender::new(
        outgoing_tx,
        analytics_events_client.clone(),
    ));
    let processor = Arc::new(MessageProcessor::new(MessageProcessorArgs {
        managed_transition_control_socket_endpoint: None,
        managed_transition_process_instance_id: None,
        telemetry_reset: Default::default(),
        control_endpoint: None,
        cloud_skill_provider: None,
        outgoing,
        analytics_events_client,
        arg0_paths: Arg0DispatchPaths::default(),
        startup_cwd: Some(config.cwd.to_path_buf()),
        config,
        config_manager,
        environment_manager: Arc::new(EnvironmentManager::default_for_tests()),
        feedback: CodexFeedback::new(),
        log_db: None,
        state_db: None,
        config_warnings: Vec::new(),
        startup_warnings_tx: mpsc::unbounded_channel().0,
        session_source: SessionSource::VSCode,
        user_verification: Arc::new(crate::user_verification::Service::new(Arc::clone(
            &auth_manager,
        ))),
        auth_manager,
        runtime_auth_slot: None,
        installation_id: "11111111-1111-4111-8111-111111111111".to_string(),
        code_mode_session_provider: None,
        rpc_transport: AppServerRpcTransport::Stdio,
        remote_control_handle: None,
        // Isolate routing from unrelated plugin startup configuration loads.
        plugin_startup_tasks: None,
    }));
    // The model catalog has its own legitimate config refresh, outside routing.
    processor.models_refresh_worker.lock().await.shutdown();

    // Observe the task scheduled by real processor construction, not the helper.
    assert!(
        tokio::time::timeout(Duration::from_secs(/*secs*/ 1), loads_rx.recv())
            .await
            .is_err(),
        "startup routing entered configuration loading"
    );
    let session = Arc::new(ConnectionSessionState::new(
        crate::transport::ConnectionOrigin::Stdio,
    ));
    processor
        .process_request(
            TEST_CONNECTION_ID,
            serde_json::from_value(json!({
                "id": 1, "method": "initialize", "params": {
                    "clientInfo": {"name": "workspace-routing-test", "version": "1"},
                    "capabilities": {"experimentalApi": true, "interactiveClient": true}
                }
            }))?,
            &crate::transport::AppServerTransport::Stdio,
            Arc::clone(&session),
        )
        .await;
    let _: InitializeResponse = read_response(&mut outgoing_rx, /*request_id*/ 1).await;
    // Complete the same post-initialize hook used by the real transports.
    processor
        .connection_initialized(TEST_CONNECTION_ID, &session)
        .await;
    assert!(
        tokio::time::timeout(Duration::from_secs(/*secs*/ 1), loads_rx.recv())
            .await
            .is_err(),
        "connection initialization routing entered configuration loading"
    );

    // The real account handler must bypass the observed configuration loader.
    let cached = processor
        .account_processor
        .get_account(codex_app_server_protocol::GetAccountParams {
            cache_only: true,
            refresh_token: false,
        })
        .await
        .map_err(|error| anyhow::anyhow!(error.message))?
        .expect("cache-only response");
    let cached: GetAccountResponse = serde_json::from_value(serde_json::to_value(cached)?)?;
    assert_eq!(cached.account, None);
    assert_eq!(cached.user_id, None);
    assert_eq!(cached.workspace_routing, None);
    let observation = cached.auth_observation.expect("cache observation");
    assert_eq!(observation.version, 1);
    assert!(observation.state_known);
    assert_eq!(
        observation.mode,
        api_key.map(|_| codex_app_server_protocol::AuthMode::ApiKey)
    );
    assert_eq!(observation.provider, None);
    let conflicting = processor
        .account_processor
        .get_account(codex_app_server_protocol::GetAccountParams {
            cache_only: true,
            refresh_token: true,
        })
        .await
        .expect_err("cache plus refresh is contradictory");
    assert_eq!(conflicting.code, -32600);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), loads_rx.recv())
            .await
            .is_err(),
        "cache-only account/read entered configuration loading",
    );

    processor
        .process_request(
            TEST_CONNECTION_ID,
            serde_json::from_value(json!({
                "id": 2, "method": "account/read", "params": {"refreshToken": false}
            }))?,
            &crate::transport::AppServerTransport::Stdio,
            Arc::clone(&session),
        )
        .await;
    let response: GetAccountResponse = read_response(&mut outgoing_rx, /*request_id*/ 2).await;
    assert_eq!(
        response,
        GetAccountResponse {
            auth_observation: None,
            user_id: None,
            account: api_key.map(|_| codex_app_server_protocol::Account::ApiKey {}),
            requires_openai_auth: true,
            workspace_routing: None,
        }
    );
    assert_eq!(
        loads_rx.try_recv(),
        Ok(()),
        "explicit account/read must reload config"
    );
    // The production constructor, dispatcher and turn host must share this
    // actual barrier. A client-declared role never upgrades stdio provenance.
    for (id, closed) in [(3, false), (6, true)] {
        if closed {
            processor
                .managed_transition_coordinator
                .account_work_permits()
                .close();
            assert!(
                processor
                    .thread_manager
                    .admit_background_operation_work()
                    .is_err()
            );
        }
        processor
            .process_request(
                TEST_CONNECTION_ID,
                serde_json::from_value(json!({
                    "id": id, "method": "account/managedAuthTransition/read", "params": {
                        "contractVersion": 1, "transitionId": "", "processInstanceId": "untrusted"
                    }
                }))?,
                &crate::transport::AppServerTransport::Stdio,
                Arc::clone(&session),
            )
            .await;
        let response: codex_app_server_protocol::ReadManagedTransitionResponse =
            read_response(&mut outgoing_rx, id).await;
        let codex_app_server_protocol::ReadManagedTransitionResponse::Refused { refusal } =
            response
        else {
            panic!("unproven caller must not read coordinator state")
        };
        assert_eq!(
            refusal.kind,
            codex_app_server_protocol::ManagedTransitionRefusalKind::AuthorizationNotAdmitted
        );
        if !closed {
            assert!(
                processor
                    .managed_transition_coordinator
                    .try_acquire_account_work_permit()
                    .is_some()
            );
        }
    }
    processor
        .process_request(
            TEST_CONNECTION_ID,
            serde_json::from_value(json!({
                "id": 7, "method": "account/read", "params": {"refreshToken":false}
            }))?,
            &crate::transport::AppServerTransport::Stdio,
            Arc::clone(&session),
        )
        .await;
    loop {
        let envelope = tokio::time::timeout(Duration::from_secs(5), outgoing_rx.recv())
            .await?
            .expect("outgoing channel remains open");
        if let crate::outgoing_message::OutgoingEnvelope::ToConnection {
            message: crate::outgoing_message::OutgoingMessage::Error(error),
            ..
        } = envelope
        {
            assert_eq!(error.id, codex_app_server_protocol::RequestId::Integer(7));
            assert_eq!(
                error.error.code,
                crate::error_code::ACCOUNT_TRANSITION_IN_PROGRESS_ERROR_CODE
            );
            break;
        }
    }
    processor
        .process_request(
            TEST_CONNECTION_ID,
            serde_json::from_value(json!({
                "id": 8, "method": "account/read", "params": {"cacheOnly":true}
            }))?,
            &crate::transport::AppServerTransport::Stdio,
            Arc::clone(&session),
        )
        .await;
    let cached: GetAccountResponse = read_response(&mut outgoing_rx, 8).await;
    assert!(cached.auth_observation.is_some());
    assert!(
        loads_rx.try_recv().is_err(),
        "barrier refusal/cache observation must not load config"
    );

    // Role/client claims over unproven stdio cannot mint retention. These
    // local lifecycle controls still respond while the account barrier is closed.
    processor
        .process_request(
            TEST_CONNECTION_ID,
            serde_json::from_value(json!({
                "id": 9, "method":"thread/retention/acquire",
                "params":{"threadId": ThreadId::new().to_string()}
            }))?,
            &crate::transport::AppServerTransport::Stdio,
            Arc::clone(&session),
        )
        .await;
    let retained: codex_app_server_protocol::ThreadRetentionAcquireResponse =
        read_response(&mut outgoing_rx, 9).await;
    assert!(matches!(
        retained,
        codex_app_server_protocol::ThreadRetentionAcquireResponse::Refused {
            reason: codex_app_server_protocol::ThreadRetentionRefusalReason::IneligiblePrincipal
        }
    ));
    processor
        .process_request(
            TEST_CONNECTION_ID,
            serde_json::from_value(json!({
                "id": 10, "method":"kcf/thread/interactiveSubscription/list", "params":{}
            }))?,
            &crate::transport::AppServerTransport::Stdio,
            Arc::clone(&session),
        )
        .await;
    let subscriptions: codex_app_server_protocol::ThreadInteractiveSubscriptionListResponse =
        read_response(&mut outgoing_rx, 10).await;
    assert!(subscriptions.entries.is_empty());
    processor.shutdown_threads().await;
    Ok(())
}
