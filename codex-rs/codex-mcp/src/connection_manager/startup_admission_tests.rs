use super::*;
use crate::server::EffectiveMcpServer;
use crate::{McpAttemptAccess, McpAttemptRefused, McpAttemptRequirement, McpAttemptWork};
use pretty_assertions::assert_eq;
use std::sync::atomic::AtomicUsize;

#[tokio::test]
async fn dormant_refusal_never_wakes_driver_and_admitted_original_survives_waiter_drop() {
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
    let live = Arc::new(AtomicUsize::new(1));
    let work = Work(Arc::clone(&live));
    let effects = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&effects);
    let (finish, receive) = tokio::sync::oneshot::channel();
    let mut client = tests::create_ready_async_managed_client(Vec::new()).await;
    client.client = crate::rmcp_client::ClientStartup::new(
        async move {
            counted.fetch_add(1, Ordering::SeqCst);
            receive.await.unwrap();
            Err(StartupOutcomeError::Cancelled)
        }
        .boxed()
        .shared(),
        McpAttemptRequirement::Required,
    );
    client.startup_complete.store(false, Ordering::SeqCst);
    let driver_client = client.clone();
    let mut manager = McpConnectionSet::empty(false);
    manager.insert_test_client("docs", client);
    let (trigger, mut receiver) = watch::channel(false);
    let connection =
        Arc::get_mut(&mut manager.servers.get_mut("docs").unwrap().connection).unwrap();
    connection.startup_trigger = Some(trigger);
    assert!(matches!(
        connection.client().await,
        Err(StartupOutcomeError::Refused(_))
    ));
    assert!(!*receiver.borrow());
    assert!(connection.startup_is_dormant());
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert_eq!(live.load(Ordering::SeqCst), 1);
    let driver = tokio::spawn(async move {
        receiver.wait_for(|started| *started).await.unwrap();
        driver_client.client().await
    });
    let mut observer =
        Box::pin(connection.client_with_authority(McpAttemptAccess::Admitted(&work)));
    assert!(futures::poll!(observer.as_mut()).is_pending());
    assert_eq!(live.load(Ordering::SeqCst), 2);
    drop(observer);
    drop(work);
    assert_eq!(live.load(Ordering::SeqCst), 1);
    finish.send(()).unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(3), driver)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(outcome, Err(StartupOutcomeError::Cancelled)));
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(live.load(Ordering::SeqCst), 0);
    assert!(!connection.startup_is_dormant());
}

struct PublicationWork(Arc<AtomicUsize>);
impl McpAttemptWork for PublicationWork {
    fn derive_attempt(&self) -> Result<Box<dyn McpAttemptWork>, McpAttemptRefused> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(Self(Arc::clone(&self.0))))
    }
}
impl Drop for PublicationWork {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

fn publication_input(work: Option<Box<dyn McpAttemptWork>>) -> McpRuntimeInput {
    McpRuntimeInput {
        attempt_requirement: McpAttemptRequirement::Required,
        startup_work: work,
        network_policy_metadata: None,
        runtime_auth: None,
        startup_policy: McpStartupPolicy::Eager,
        config: Arc::new(crate::mcp::tests::test_mcp_config(std::env::temp_dir())),
        plugins_available: false,
        ready_selected_capability_roots: Vec::new(),
        mcp_servers: HashMap::from([(
            "docs".to_string(),
            EffectiveMcpServer::from_host_config(crate::connection_pool::tests::config()),
        )]),
        submit_id: "publication-authority".to_string(),
        tx_event: None,
        startup_cancellation_token: tokio_util::sync::CancellationToken::new(),
        runtime_context: crate::connection_pool::tests::context(),
        codex_apps_tools_cache: codex_connectors::ConnectorRuntimeManager::default(),
        tool_catalog_cache: crate::tool_catalog_cache::McpToolCatalogCache::default(),
        codex_apps_tools_cache_key: codex_connectors::ConnectorRuntimeContextKey::personal(
            None, None,
        ),
        client_mcp_extensions: ClientMcpExtensions::default(),
        auth: None,
        auth_manager: None,
        elicitation_reviewer: None,
        elicitation_lifecycle: None,
    }
}

async fn wait_for_publication_work_to_finish(live: &AtomicUsize) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while live.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("original driver must release finite publication work");
}

#[tokio::test]
async fn unpublished_eager_driver_releases_staged_original_without_starting_transport() {
    let live = Arc::new(AtomicUsize::new(1));
    let input = publication_input(Some(Box::new(PublicationWork(Arc::clone(&live)))));
    let (publish, gate) = McpPublicationGate::pending_for_test();
    let set = McpConnectionSet::new(
        None,
        None,
        None,
        gate,
        input,
        ElicitationRequestRouter::default(),
    )
    .await;
    let client = &set.servers["docs"].connection.client;
    assert_eq!(client.client.requirement, McpAttemptRequirement::Required);
    // Input is gone: driver and original each own one finite descendant.
    assert_eq!(live.load(Ordering::SeqCst), 2);
    assert!(client.client.peek().is_none());
    tokio::task::yield_now().await;
    assert!(client.client.peek().is_none());
    drop(publish);
    wait_for_publication_work_to_finish(&live).await;
    assert!(matches!(
        client.client().await,
        Err(StartupOutcomeError::Cancelled)
    ));
    set.shutdown().await;
}

#[tokio::test]
async fn missing_publication_work_refuses_eager_connection_before_driver_or_transport() {
    let mut input = publication_input(None);
    let mut server = crate::connection_pool::tests::config();
    server.required = true;
    input
        .mcp_servers
        .insert("docs".into(), EffectiveMcpServer::from_host_config(server));
    let set = McpConnectionSet::new(
        None,
        None,
        None,
        McpPublicationGate::already_published(),
        input,
        ElicitationRequestRouter::default(),
    )
    .await;
    assert!(set.servers.is_empty());
    assert!(
        set.validate_required_servers()
            .await
            .unwrap_err()
            .to_string()
            .contains("was not initialized")
    );
    set.shutdown().await;
}

#[tokio::test]
async fn empty_runtime_publication_does_not_keep_finite_input_work() {
    let live = Arc::new(AtomicUsize::new(1));
    let mut input = publication_input(Some(Box::new(PublicationWork(Arc::clone(&live)))));
    input.mcp_servers.clear();
    let runtime = crate::McpRuntime::new(input).await;
    assert_eq!(live.load(Ordering::SeqCst), 0);
    assert!(runtime.current_binding().await.is_some());
    runtime.shutdown().await;
}

#[tokio::test]
async fn closed_worker_registration_releases_unpolled_eager_driver_work() {
    let live = Arc::new(AtomicUsize::new(1));
    let input = publication_input(Some(Box::new(PublicationWork(Arc::clone(&live)))));
    input.runtime_context.workers.close_startup_admission();
    let set = McpConnectionSet::new(
        None,
        None,
        None,
        McpPublicationGate::already_published(),
        input,
        ElicitationRequestRouter::default(),
    )
    .await;
    wait_for_publication_work_to_finish(&live).await;
    assert!(matches!(
        set.servers["docs"].connection.client.client().await,
        Err(StartupOutcomeError::Cancelled)
    ));
    set.shutdown().await;
}

#[tokio::test]
async fn publication_reuse_cannot_cross_attempt_requirement() {
    for requirement in [
        McpAttemptRequirement::Ungated,
        McpAttemptRequirement::Required,
    ] {
        let live = Arc::new(AtomicUsize::new(1));
        let mut input = publication_input(Some(Box::new(PublicationWork(Arc::clone(&live)))));
        input.attempt_requirement = requirement;
        let config = input.mcp_servers["docs"].config().clone();
        let previous =
            tests::manager_with_reusable_ready_server(&config, &input.runtime_context, Vec::new())
                .await;
        let (publish, gate) = McpPublicationGate::pending_for_test();
        let set = McpConnectionSet::new(
            None,
            None,
            Some(PreviousMcpConnections {
                connections: &previous,
                same_owner: true,
            }),
            gate,
            input,
            ElicitationRequestRouter::default(),
        )
        .await;
        assert_eq!(
            Arc::ptr_eq(
                &previous.servers["docs"].connection,
                &set.servers["docs"].connection
            ),
            requirement == McpAttemptRequirement::Ungated
        );
        assert_eq!(
            set.servers["docs"].connection.client.client.requirement,
            requirement
        );
        drop(publish);
        wait_for_publication_work_to_finish(&live).await;
        set.shutdown().await;
        previous.shutdown().await;
    }
}
