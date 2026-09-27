use super::*;
use crate::McpAttemptAccess;
use crate::McpAttemptRefused;
use crate::McpAttemptRequirement;
use crate::McpAttemptWork;
use pretty_assertions::assert_eq;
use std::sync::atomic::AtomicUsize;

struct Work(Arc<AtomicUsize>);

impl McpAttemptWork for Work {
    fn derive_attempt(&self) -> Result<Box<dyn McpAttemptWork>, McpAttemptRefused> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(Self(Arc::clone(&self.0))))
    }
}

impl Drop for Work {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn eager_driver_owns_construction_work_until_publication_or_refusal() -> anyhow::Result<()> {
    for publish_connection in [false, true] {
        let home = tempdir()?;
        let marker = home.path().join("startup-pid");
        let context = McpRuntimeContext::new(
            Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
            home.path().to_path_buf(),
        );
        let live = Arc::new(AtomicUsize::new(1));
        let mut input = retirement_runtime_input(home.path(), retirement_stdio_config(&marker, "eager"),
            context, McpToolCatalogCache::default(), McpStartupPolicy::Eager);
        input.attempt_requirement = McpAttemptRequirement::Required;
        input.startup_work = Some(Box::new(Work(Arc::clone(&live))));
        let (publish, gate) = McpPublicationGate::pending();
        let connections = McpConnectionSet::new(None, gate, input, ElicitationRequestRouter::default()).await;
        // The constructing call has returned and dropped its input, but the
        // registered driver still owns finite work while publication is pending.
        assert_eq!(live.load(Ordering::SeqCst), 1);
        assert!(!marker.exists());
        if publish_connection {
            publish.send(true).expect("publish connections");
        } else {
            drop(publish);
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while live.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
        }).await.expect("driver reached terminal state");
        let client = &connections.servers["docs"].connection.client;
        if publish_connection {
            assert!(client.ready_client().is_some());
            assert!(marker.exists());
        } else {
            assert!(matches!(client.client().await, Err(StartupOutcomeError::Cancelled)));
            assert!(!marker.exists());
        }
        connections.shutdown().await;
    }
    Ok(())
}

#[tokio::test]
async fn dormant_connection_counts_only_activation_and_driver_outlives_waiter() -> anyhow::Result<()> {
    let home = tempdir()?;
    let marker = home.path().join("activation-pid");
    let context = McpRuntimeContext::new(
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        home.path().to_path_buf(),
    );
    let config = retirement_stdio_config(&marker, "dormant");
    let cache = McpToolCatalogCache::default();
    let runtime_config = crate::mcp::tests::test_mcp_config(home.path().to_path_buf());
    let environment = context.resolve_server_environment("docs", &config).expect("local environment");
    let cached = cache.context("docs", &config, &context, environment.as_ref(),
        (&runtime_config.client_elicitation_capability, &ClientMcpExtensions::default()),
        /*connection_identity*/ None).expect("cacheable server");
    cached.publish_if_newest(cached.begin_fetch(), &[create_test_tool("docs", "proof")]);
    let live = Arc::new(AtomicUsize::new(1));
    let mut input = retirement_runtime_input(home.path(), config, context, cache, McpStartupPolicy::LazyWhenCached);
    input.attempt_requirement = McpAttemptRequirement::Required;
    input.startup_work = Some(Box::new(Work(Arc::clone(&live))));
    let runtime = crate::McpRuntime::new(input).await;
    assert_eq!(live.load(Ordering::SeqCst), 0);
    assert!(!marker.exists());
    let connections = runtime.latest_connections();
    let connection = &connections.servers["docs"].connection;
    assert!(connection.startup_is_dormant());
    assert!(matches!(connection.client().await, Err(StartupOutcomeError::Refused(_))));
    assert!(runtime.current_binding_for_call("docs").await.is_none());
    assert!(connection.startup_is_dormant());
    // The activator can disappear after the first poll. Its derived attempt
    // must continue under the retained driver, without pinning the idle client.
    live.fetch_add(1, Ordering::SeqCst);
    let work = Work(Arc::clone(&live));
    let mut observer = Box::pin(runtime.current_binding_for_call_with_authority("docs", McpAttemptAccess::Admitted(&work)));
    assert!(futures::poll!(&mut observer).is_pending());
    drop(observer);
    drop(work);
    tokio::time::timeout(Duration::from_secs(5), async {
        while live.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    }).await.expect("retained driver completed startup");
    assert!(connection.client.ready_client().is_some());
    assert!(marker.exists());
    live.fetch_add(1, Ordering::SeqCst);
    let work = Work(Arc::clone(&live));
    let access = McpAttemptAccess::Admitted(&work);
    let binding = runtime.current_binding_with_authority(access).await.expect("ready binding");
    assert_eq!(binding.list_all_resources(|_| true).await, HashMap::new());
    assert_eq!(binding.list_all_resource_templates(|_| true).await, HashMap::new());
    let resources = binding.list_all_resources_with_authority(|_| true, access).await;
    let templates = binding.list_all_resource_templates_with_authority(|_| true, access).await;
    assert_eq!(serde_json::to_value(resources)?, serde_json::json!({
        "docs": [{"uri": "test://proof", "name": "proof"}]
    }));
    assert_eq!(serde_json::to_value(templates)?, serde_json::json!({
        "docs": [{"uriTemplate": "test://{id}", "name": "proof-template"}]
    }));
    drop(work);
    assert_eq!(live.load(Ordering::SeqCst), 0, "ready bindings retain no work");
    runtime.shutdown().await;
    Ok(())
}
