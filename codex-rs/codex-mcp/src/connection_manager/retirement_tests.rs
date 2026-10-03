use super::*;
use codex_rmcp_client::InProcessTransportFactory;
use codex_rmcp_client::PhysicalRetirementOutcome;
use codex_rmcp_client::RmcpClient;
use futures::FutureExt;
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use std::sync::Mutex;
use tokio::io::AsyncReadExt;
use tokio::io::DuplexStream;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

struct PausedTransport {
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
    stream: Mutex<Option<DuplexStream>>,
}

impl InProcessTransportFactory for PausedTransport {
    fn open(&self) -> BoxFuture<'static, std::io::Result<DuplexStream>> {
        let stream = self.stream.lock().unwrap().take().unwrap();
        let entered = Arc::clone(&self.entered);
        let release = Arc::clone(&self.release);
        async move {
            entered.add_permits(1);
            release.acquire().await.unwrap().forget();
            Ok(stream)
        }
        .boxed()
    }
}

#[tokio::test(start_paused = true)]
async fn external_early_timeout_is_execution_only_and_explicit_retry_completes() {
    let control = crate::McpRuntimeRetirement::default();
    let owner = control
        .registry
        .register_connection(CancellationToken::new())
        .unwrap();
    let replacement = control
        .registry
        .register_connection(CancellationToken::new())
        .unwrap();
    let retirement = ConnectionRetirement::new(control.registry.clone(), &owner);
    let (stream, mut peer) = tokio::io::duplex(64);
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let factory = Arc::new(PausedTransport {
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
        stream: Mutex::new(Some(stream)),
    });
    let lower = owner.lower();
    let startup = tokio::spawn(async move {
        RmcpClient::new_in_process_client_in_retirement(factory, lower).await
    });
    entered.acquire().await.unwrap().forget();
    retirement.superseded.store(true, Ordering::Release);
    let driver = retirement.retire_if_superseded().unwrap();
    assert_eq!(driver.await.unwrap(), RuntimeTaskOutcome::Complete);
    assert!(!control.is_retired());
    let incomplete = owner.lower().shutdown_until(Instant::now()).await;
    assert_eq!(
        incomplete.attempts,
        vec![(0, PhysicalRetirementOutcome::TimedOut)]
    );
    release.add_permits(1);
    drop(startup.await.unwrap().unwrap());
    let report = control
        .shutdown_until(Instant::now() + DEFAULT_RETIREMENT_TIMEOUT)
        .await;
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(
        report.connections,
        vec![
            (
                0,
                codex_rmcp_client::PhysicalRetirementReport {
                    attempts: vec![(0, PhysicalRetirementOutcome::Complete)]
                }
            ),
            (
                1,
                codex_rmcp_client::PhysicalRetirementReport { attempts: vec![] }
            ),
        ]
    );
    assert_eq!(report.tasks, vec![(0, RuntimeTaskOutcome::Complete)]);
    assert!(control.is_retired());
    assert_eq!(control.shutdown_until(Instant::now()).await, report);
    let mut byte = [0];
    assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
    drop(replacement);
}

#[tokio::test]
async fn queued_driver_keeps_registry_alive_without_a_stored_future_cycle() {
    let registry = RuntimeRetirementRegistry::default();
    let owner = registry
        .register_connection(CancellationToken::new())
        .unwrap();
    let weak_owner = Arc::downgrade(&owner);
    let retirement = ConnectionRetirement::new(registry.clone(), &owner);
    retirement.superseded.store(true, Ordering::Release);
    let driver = retirement.retire_if_superseded().unwrap();
    drop((registry, owner, retirement));
    assert!(
        weak_owner.upgrade().is_some(),
        "poller must retain the census before first poll"
    );
    assert_eq!(driver.await.unwrap(), RuntimeTaskOutcome::Complete);
    assert!(
        weak_owner.upgrade().is_none(),
        "completed retained future must not pin registry"
    );
}

#[tokio::test]
async fn explicit_close_before_first_poll_skips_driver_and_preserves_census() {
    let control = crate::McpRuntimeRetirement::default();
    let owner = control
        .registry
        .register_connection(CancellationToken::new())
        .unwrap();
    let retirement = ConnectionRetirement::new(control.registry.clone(), &owner);
    assert!(
        retirement.retire_if_superseded().is_none(),
        "unmarked release starts nothing"
    );
    retirement.superseded.store(true, Ordering::Release);
    let driver = retirement.retire_if_superseded().unwrap();
    control.close_registration();
    assert_eq!(driver.await.unwrap(), RuntimeTaskOutcome::Skipped);
    let report = control
        .shutdown_until(Instant::now() + DEFAULT_RETIREMENT_TIMEOUT)
        .await;
    assert!(report.is_complete());
    assert_eq!(report.connections.len(), 1);
    assert_eq!(report.tasks, vec![(0, RuntimeTaskOutcome::Skipped)]);
    assert!(retirement.retire_if_superseded().is_none());
}

#[test]
fn missing_executor_does_not_claim_completion_or_discard_external_custody() {
    let control = crate::McpRuntimeRetirement::default();
    let owner = control
        .registry
        .register_connection(CancellationToken::new())
        .unwrap();
    let retirement = ConnectionRetirement::new(control.registry.clone(), &owner);
    retirement.superseded.store(true, Ordering::Release);
    assert!(retirement.retire_if_superseded().is_none());
    drop((owner, retirement));
    assert!(!control.is_retired());
    let executor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert!(
        executor
            .block_on(control.shutdown_until(Instant::now() + DEFAULT_RETIREMENT_TIMEOUT))
            .is_complete()
    );
    assert!(control.is_retired());
}

#[cfg(unix)]
#[tokio::test]
async fn dormant_supersession_has_no_attempt_and_pending_reuse_stays_unmarked() -> anyhow::Result<()>
{
    use crate::McpRuntime;
    use crate::McpRuntimeContext;
    use crate::McpStartupPolicy;
    use crate::connection_manager::tests::create_test_tool;
    use crate::connection_manager::tests::retirement_runtime_input;
    use crate::connection_manager::tests::retirement_stdio_config;
    let home = tempfile::tempdir()?;
    let marker = home.path().join("startup-pids");
    let context = McpRuntimeContext::new(
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        home.path().to_path_buf(),
    );
    let config = retirement_stdio_config(&marker, "dormant");
    let cache = crate::tool_catalog_cache::McpToolCatalogCache::default();
    let runtime_config = crate::mcp::tests::test_mcp_config(home.path().to_path_buf());
    let environment = context.resolve_server_environment("docs", &config).unwrap();
    let cached = cache
        .context(
            "docs",
            &config,
            &context,
            environment.as_ref(),
            (
                &runtime_config.client_elicitation_capability,
                &codex_protocol::mcp::ClientMcpExtensions::default(),
            ),
            /*connection_identity*/ None,
        )
        .unwrap();
    cached.publish_if_newest(cached.begin_fetch(), &[create_test_tool("docs", "proof")]);
    let input = |config, policy| {
        retirement_runtime_input(home.path(), config, context.clone(), cache.clone(), policy)
    };
    let runtime = McpRuntime::new(input(config, McpStartupPolicy::LazyWhenCached)).await;
    let dormant = runtime
        .latest_connections()
        .connection_by_name("docs")
        .unwrap();
    assert!(dormant.startup_is_dormant());
    let dormant_lower = dormant.retirement.lower.clone();
    let mut empty = input(
        retirement_stdio_config(&marker, "empty"),
        McpStartupPolicy::Eager,
    );
    empty.mcp_servers.clear();
    runtime.replace(empty).await;
    assert!(dormant.retirement.superseded.load(Ordering::Acquire));
    drop(dormant);
    let report = dormant_lower
        .shutdown_until(Instant::now() + DEFAULT_RETIREMENT_TIMEOUT)
        .await;
    assert!(report.attempts.is_empty());
    assert!(!marker.exists());
    assert!(
        runtime
            .shutdown_until(Instant::now() + DEFAULT_RETIREMENT_TIMEOUT)
            .await
            .is_complete()
    );

    // The shell acknowledges launch but deliberately never answers initialize.
    let mut pending_config = retirement_stdio_config(&marker, "pending");
    if let codex_config::McpServerTransportConfig::Stdio { args, .. } =
        &mut pending_config.transport
    {
        args[1] = args[1].replace(
            "    case \"$request\" in",
            "    case \"$request\" in\n        *'\"initialize\"'*) continue ;;",
        );
    }
    let runtime = McpRuntime::new(input(pending_config.clone(), McpStartupPolicy::Eager)).await;
    let pending = runtime
        .latest_connections()
        .connection_by_name("docs")
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(10));
        while !marker.exists() {
            tick.tick().await;
        }
    })
    .await?;
    assert!(!pending.client.startup_complete.load(Ordering::Acquire));
    runtime
        .replace(input(pending_config, McpStartupPolicy::Eager))
        .await;
    let carried = runtime
        .latest_connections()
        .connection_by_name("docs")
        .unwrap();
    assert!(Arc::ptr_eq(&pending, &carried));
    assert!(!pending.retirement.superseded.load(Ordering::Acquire));
    let cancellation = pending.client.cancel_token.clone();
    let lower = pending.retirement.lower.clone();
    let mut empty = input(
        retirement_stdio_config(&marker, "empty"),
        McpStartupPolicy::Eager,
    );
    empty.mcp_servers.clear();
    runtime.replace(empty).await;
    assert!(pending.retirement.superseded.load(Ordering::Acquire));
    drop((pending, carried));
    assert!(cancellation.is_cancelled());
    let pid = std::fs::read_to_string(&marker)?;
    let pid = pid.lines().next().unwrap();
    // Observe release-driven exit before any explicit cleanup can mask it.
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(10));
        while std::process::Command::new("/bin/kill")
            .args(["-0", pid])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()?
            .success()
        {
            tick.tick().await;
        }
        anyhow::Ok(())
    })
    .await??;
    let report = lower
        .shutdown_until(Instant::now() + DEFAULT_RETIREMENT_TIMEOUT)
        .await;
    assert!(report.is_complete(), "{report:?}");
    assert!(!report.attempts.is_empty());
    assert!(
        runtime
            .shutdown_until(Instant::now() + DEFAULT_RETIREMENT_TIMEOUT)
            .await
            .is_complete()
    );
    Ok(())
}
