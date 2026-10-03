use super::*;
use pretty_assertions::assert_eq;

fn process_alive(pid: &str) -> anyhow::Result<bool> {
    Ok(std::process::Command::new("/bin/kill")
        .args(["-0", pid])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()?
        .success())
}

#[tokio::test]
async fn external_supersession_retires_after_last_binding_and_keeps_two_owner_replay()
-> anyhow::Result<()> {
    let home = tempdir()?;
    let marker = home.path().join("superseded-pids");
    let context = McpRuntimeContext::new(
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        home.path().to_path_buf(),
    );
    let input = |generation| {
        retirement_runtime_input(
            home.path(),
            retirement_stdio_config(&marker, generation),
            context.clone(),
            McpToolCatalogCache::default(),
            McpStartupPolicy::Eager,
        )
    };
    let control = crate::McpRuntimeRetirement::default();
    let runtime = crate::McpRuntime::new_in_retirement(input("a"), control.clone()).await;
    let required = ["docs".to_owned()];
    let old = runtime
        .current_binding_with_requirements(&required, &HashSet::new())
        .await
        .unwrap();
    runtime.replace(input("a")).await;
    let reused = runtime
        .current_binding_with_requirements(&required, &HashSet::new())
        .await
        .unwrap();
    assert_eq!(std::fs::read_to_string(&marker)?.lines().count(), 1);
    runtime.reconnect_on_next_refresh();
    runtime.replace(input("b")).await;
    let replacement = runtime
        .current_binding_with_requirements(&required, &HashSet::new())
        .await
        .unwrap();
    let pids = std::fs::read_to_string(&marker)?;
    let pids: Vec<_> = pids.lines().collect();
    assert_eq!(pids.len(), 2);
    assert!(process_alive(pids[0])?);
    drop(old);
    assert!(
        process_alive(pids[0])?,
        "another old binding still owns the connection"
    );
    drop(reused);
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut poll = tokio::time::interval(Duration::from_millis(10));
        while process_alive(pids[0])? {
            poll.tick().await;
        }
        anyhow::Ok(())
    })
    .await??;
    assert!(process_alive(pids[1])?);
    assert_eq!(replacement.tools().len(), 1);
    assert!(!control.is_retired());
    let report = control
        .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(5))
        .await;
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(
        report.connections,
        vec![
            (
                0,
                codex_rmcp_client::PhysicalRetirementReport {
                    attempts: vec![(0, codex_rmcp_client::PhysicalRetirementOutcome::Complete)]
                }
            ),
            (
                1,
                codex_rmcp_client::PhysicalRetirementReport {
                    attempts: vec![(0, codex_rmcp_client::PhysicalRetirementOutcome::Complete)]
                }
            ),
        ]
    );
    assert_eq!(
        control.shutdown_until(tokio::time::Instant::now()).await,
        report
    );
    assert!(control.is_retired());
    assert!(!process_alive(pids[1])?);
    Ok(())
}

#[tokio::test]
async fn shared_stream_lease_survives_supersession_and_releases_on_terminal_or_cancel()
-> anyhow::Result<()> {
    for pending in [false, true] {
        let home = tempdir()?;
        let marker = home.path().join("stream-pids");
        let context = McpRuntimeContext::new(
            Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
            home.path().to_path_buf(),
        );
        let mut config = retirement_stdio_config(&marker, "stream");
        if pending && let McpServerTransportConfig::Stdio { args, .. } = &mut config.transport {
            args[1] = args[1].replace(
                "    case \"$request\" in",
                "    case \"$request\" in\n        *'\"events/stream\"'*) continue ;; ",
            );
        }
        let input = |config| {
            retirement_runtime_input(
                home.path(),
                config,
                context.clone(),
                McpToolCatalogCache::default(),
                McpStartupPolicy::Eager,
            )
        };
        let runtime = crate::McpRuntime::new(input(config)).await;
        let binding = runtime
            .current_binding_with_requirements(&["docs".to_owned()], &HashSet::new())
            .await
            .unwrap();
        drop(binding);
        let connection = runtime
            .latest_connections()
            .connection_by_name("docs")
            .unwrap();
        let client = connection.client().await?;
        let (removal, receiver) = tokio::sync::watch::channel(());
        let mut stream = crate::resource_client::McpEventStream::open(
            Arc::clone(&client.client),
            Some(connection),
            receiver,
            "proof",
            &serde_json::json!({}),
            /*request_meta*/ None,
        )
        .await?;
        drop(client);
        runtime.reconnect_on_next_refresh();
        runtime
            .replace(input(retirement_stdio_config(&marker, "replacement")))
            .await;
        let pids = std::fs::read_to_string(&marker)?;
        let pid = pids.lines().next().unwrap();
        assert!(process_alive(pid)?, "stream is the last ordinary holder");
        if pending {
            drop(stream);
        } else {
            assert!(stream.recv().await?.is_none());
            drop(stream);
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            let mut poll = tokio::time::interval(Duration::from_millis(10));
            while process_alive(pid)? {
                poll.tick().await;
            }
            anyhow::Ok(())
        })
        .await??;
        drop(removal);
        assert!(
            runtime
                .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(5))
                .await
                .is_complete()
        );
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_replacements_never_carry_a_superseded_connection() -> anyhow::Result<()> {
    let home = tempdir()?;
    let marker = home.path().join("concurrent-pids");
    let context = McpRuntimeContext::new(
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        home.path().to_path_buf(),
    );
    let input = |generation| {
        retirement_runtime_input(
            home.path(),
            retirement_stdio_config(&marker, generation),
            context.clone(),
            McpToolCatalogCache::default(),
            McpStartupPolicy::Eager,
        )
    };
    let runtime = crate::McpRuntime::new(input("a")).await;
    let old = runtime
        .current_binding_with_requirements(&["docs".to_owned()], &HashSet::new())
        .await
        .unwrap();
    let old_connection = runtime
        .latest_connections()
        .connection_by_name("docs")
        .unwrap();
    tokio::join!(runtime.replace(input("b")), runtime.replace(input("a")));
    let current = runtime
        .latest_connections()
        .connection_by_name("docs")
        .unwrap();
    assert!(
        !Arc::ptr_eq(&old_connection, &current),
        "a stale reusable candidate must not reintroduce A"
    );
    drop((old, old_connection));
    assert!(
        runtime
            .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
            .is_complete()
    );
    Ok(())
}

#[tokio::test]
async fn production_shared_opener_pins_superseded_tool_and_private_opener_stays_outside()
-> anyhow::Result<()> {
    let home = tempdir()?;
    let marker = home.path().join("apps-pids");
    let context = McpRuntimeContext::new(
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        home.path().to_path_buf(),
    );
    let input = |generation| {
        let mut config = retirement_stdio_config(&marker, generation);
        if let McpServerTransportConfig::Stdio { args, .. } = &mut config.transport {
            // Stdio keeps the subscription pending; event notification routing is HTTP-only.
            args[1] = args[1].replace(
                "    case \"$request\" in",
                "    case \"$request\" in\n        *'\"events/stream\"'*) continue ;;",
            );
        }
        let mut input = retirement_runtime_input(
            home.path(),
            config.clone(),
            context.clone(),
            McpToolCatalogCache::default(),
            McpStartupPolicy::Eager,
        );
        input.mcp_servers = HashMap::from([(
            CODEX_APPS_MCP_SERVER_NAME.to_owned(),
            EffectiveMcpServer::from_host_config(config.clone()),
        )]);
        let mut catalog = crate::ResolvedMcpCatalog::builder();
        catalog.register(crate::McpServerRegistration::from_hosted_apps(
            "supersession-test",
            /*contribution_order*/ 0,
            config,
        ));
        let mut runtime_config = crate::mcp::tests::test_mcp_config(home.path().to_path_buf());
        runtime_config.mcp_server_catalog = catalog.build();
        input.config = Arc::new(runtime_config);
        input
    };
    let runtime = Arc::new(crate::McpRuntime::new(input("a")).await);
    assert!(
        runtime
            .latest_wait_for_server_ready(CODEX_APPS_MCP_SERVER_NAME, Duration::from_secs(5))
            .await
    );
    let old_connection = Arc::downgrade(
        &runtime
            .latest_connections()
            .connection_by_name(CODEX_APPS_MCP_SERVER_NAME)
            .unwrap(),
    );
    let client = crate::McpResourceClient::new(Arc::clone(&runtime));
    let shared = client
        .open_event_stream_with_authority(
            "proof",
            &serde_json::json!({}),
            /*request_meta*/ None,
            crate::McpAttemptAccess::Unscoped,
        )
        .await?;
    let opener = client.event_stream_opener()?;
    let private = opener
        .open("proof", &serde_json::json!({}), /*request_meta*/ None)
        .await?;
    let pids = std::fs::read_to_string(&marker)?;
    let pids: Vec<_> = pids.lines().collect();
    assert_eq!(
        pids.len(),
        2,
        "tool and private stream have distinct physical clients"
    );
    runtime.reconnect_on_next_refresh();
    runtime.replace(input("b")).await;
    assert!(
        runtime
            .latest_wait_for_server_ready(CODEX_APPS_MCP_SERVER_NAME, Duration::from_secs(5))
            .await
    );
    assert!(
        old_connection.upgrade().is_some(),
        "the production shared opener must retain the old connection"
    );
    assert!(
        process_alive(pids[0])?,
        "production shared stream must keep its superseded tool connection"
    );
    drop(shared);
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut tick = tokio::time::interval(Duration::from_millis(10));
        while process_alive(pids[0])? {
            tick.tick().await;
        }
        anyhow::Ok(())
    })
    .await
    .map_err(|error| anyhow::anyhow!("superseded tool exit: {error}"))??;
    let report = runtime
        .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(5))
        .await;
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(
        report.connections.len(),
        2,
        "private stream must not enter tool census"
    );
    assert!(
        process_alive(pids[1])?,
        "explicit tool retirement cannot close the private stream"
    );
    drop(private);
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut tick = tokio::time::interval(Duration::from_millis(10));
        while process_alive(pids[1])? {
            tick.tick().await;
        }
        anyhow::Ok(())
    })
    .await
    .map_err(|error| anyhow::anyhow!("private stream exit: {error}"))??;
    Ok(())
}
