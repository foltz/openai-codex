//! Tests cloud cache isolation, refresh retries, and late completion.

use super::*;
use crate::catalog::SkillCatalogEntry;
use crate::catalog::SkillResourceId;
use crate::catalog::SkillSearchResult;
use crate::provider::SkillProvider;
use crate::provider::SkillProviderFuture;
use crate::provider::SkillReadContext;
use crate::provider::SkillSearchRequest;
use codex_extension_api::CatalogRefreshInput;
use codex_extension_api::ExtensionData;
use codex_extension_api::ExtensionEventSink;
use codex_extension_api::ExtensionRegistryBuilder;
use codex_extension_api::ExtensionWarning;
use codex_mcp::McpRuntime;
use codex_protocol::protocol::Event;
use pretty_assertions::assert_eq;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Default)]
struct Provider {
    reads: AtomicUsize,
    lists: AtomicUsize,
    fail_list: AtomicBool,
    partial_list: AtomicBool,
    next_catalog: Mutex<Option<SkillCatalog>>,
    pause_read: AtomicBool,
    pause_list: AtomicBool,
    entered: Notify,
    resume: Notify,
}

impl SkillProvider for Provider {
    fn list<'a>(&'a self, _query: SkillListQuery<'a>) -> SkillProviderFuture<'a, SkillCatalog> {
        Box::pin(async move {
            self.lists.fetch_add(1, Ordering::SeqCst);
            let fail = self.fail_list.swap(false, Ordering::SeqCst);
            let catalog = self
                .next_catalog
                .lock()
                .expect("test provider catalog mutex should not be poisoned")
                .take();
            if self.pause_list.swap(false, Ordering::SeqCst) {
                self.entered.notify_one();
                self.resume.notified().await;
            }
            if fail {
                return Err(SkillProviderError::new("discovery failed"));
            }
            if let Some(catalog) = catalog {
                return Ok(catalog);
            }
            let is_partial = self.partial_list.swap(false, Ordering::SeqCst);
            Ok(SkillCatalog {
                is_partial,
                warnings: if is_partial {
                    Vec::new()
                } else {
                    vec!["account catalog".into()]
                },
                ..Default::default()
            })
        })
    }

    fn read<'a>(
        &'a self,
        request: SkillReadRequest<'a>,
    ) -> SkillProviderFuture<'a, SkillReadResult> {
        let revision = self.reads.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if self.pause_read.swap(false, Ordering::SeqCst) {
                self.entered.notify_one();
                self.resume.notified().await;
            }
            Ok(SkillReadResult {
                resource: request.resource,
                contents: format!("contents {revision}"),
            })
        })
    }

    fn search(&self, _request: SkillSearchRequest) -> SkillProviderFuture<'_, SkillSearchResult> {
        Box::pin(async { Ok(SkillSearchResult::default()) })
    }
}

fn state() -> SkillsThreadState {
    SkillsThreadState::new(
        SkillsExtensionConfig {
            include_instructions: true,
            catalog_deltas_enabled: false,
            omit_descriptions: false,
            max_context_tokens: None,
            bundled_skills_enabled: false,
            cloud_skill_enabled: true,
            shadow_selection_enabled: false,
        },
        /*cloud_skills_available*/ true,
        /*extension_metrics*/ None,
    )
}

fn client() -> Arc<McpResourceClient> {
    Arc::new(McpResourceClient::new(Arc::new(McpRuntime::empty(
        /*prefix_mcp_tool_names*/ false,
    ))))
}

fn query(client: Option<Arc<McpResourceClient>>) -> SkillListQuery<'static> {
    SkillListQuery {
        mcp_access: codex_mcp::McpAttemptAccess::Unscoped,
        turn_id: "turn".into(),
        executor_roots: Vec::new(),
        host_snapshot: None,
        include_host_skills: false,
        include_bundled_skills: false,
        include_cloud_skills: true,
        mcp_resources: client,
        executor_capability_discovery: None,
        executor_capability_discovery_v2: None,
    }
}

fn request(client: Option<Arc<McpResourceClient>>) -> SkillReadRequest<'static> {
    SkillReadRequest {
        mcp_access: Ok(codex_protocol::mcp_work::McpAttemptAccess::Unscoped),
        authority: SkillAuthority::new(SkillSourceKind::Cloud, "codex_apps"),
        package: SkillPackageId("skill://demo".into()),
        resource: SkillResourceId::new("skill://demo/SKILL.md"),
        context: SkillReadContext::Cloud {
            mcp_resources: client,
        },
    }
}

#[tokio::test]
async fn frozen_cloud_catalog_and_reads_ignore_live_mcp_changes() {
    let state = state();
    let provider = crate::FrozenCloudSkillProvider::from_json(
        serde_json::json!({"skills": [{
            "package": "skill://demo/work", "name": "demo", "description": "Frozen skill",
            "alias_root": "skill://demo", "main_resource": "skill://demo/work/SKILL.md",
            "resources": {"skill://demo/work/SKILL.md": "Frozen contents"}
        }]})
        .to_string()
        .as_bytes(),
    )
    .expect("frozen cloud skill fixture should be valid");
    let providers = SkillProviders::new().with_cloud_provider(Arc::new(provider));
    let turn_store = ExtensionData::new("turn");
    state
        .refresh_cloud_catalog(&providers, query(Some(client())), &turn_store)
        .await
        .expect("cloud catalog refresh should succeed");
    assert!(
        turn_store
            .get::<LatestCloudSkillsAuth>()
            .expect("successful discovery should record its auth scope")
            .0
            .is_none()
    );
    let catalog = state
        .available_cloud_catalog()
        .expect("cloud catalog should be available after refresh");
    let generation = state
        .catalog_state
        .lock()
        .expect("catalog state mutex should not be poisoned")
        .cloud_generation
        .clone()
        .expect("initial refresh should publish a cloud generation");
    for mcp_resources in [Some(client()), None, Some(client())] {
        let request = SkillReadRequest {
            mcp_access: Ok(codex_protocol::mcp_work::McpAttemptAccess::Unscoped),
            package: SkillPackageId("skill://demo/work".into()),
            resource: SkillResourceId::new("skill://demo/work/SKILL.md"),
            ..request(mcp_resources.clone())
        };
        assert_eq!(
            state
                .read_skill(&providers, request)
                .await
                .expect("cloud skill read should succeed"),
            SkillReadResult {
                resource: SkillResourceId::new("skill://demo/work/SKILL.md"),
                contents: "Frozen contents".into(),
            }
        );
        state
            .refresh_cloud_catalog(&providers, query(mcp_resources), &turn_store)
            .await
            .expect("cloud catalog refresh should succeed");
        assert_eq!(state.available_cloud_catalog(), Some(catalog.clone()));
        assert!(Arc::ptr_eq(
            &generation,
            state
                .catalog_state
                .lock()
                .expect("catalog state mutex should not be poisoned")
                .cloud_generation
                .as_ref()
                .expect("frozen cloud generation should remain available")
        ));
    }
}

#[tokio::test(start_paused = true)]
async fn step_cloud_refresh_serializes_waiters_and_retries() {
    #[derive(Default)]
    struct WarningCounter(AtomicUsize);

    impl ExtensionEventSink for WarningCounter {
        fn emit(&self, _event: Event) {}

        fn emit_warning(&self, _warning: ExtensionWarning) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let cloud = Arc::new(Provider::default());
    let providers = SkillProviders::new().with_cloud_provider(cloud.clone());
    let session_store = ExtensionData::new("session");
    let thread_store = ExtensionData::new("thread");
    thread_store.insert(state());
    let thread_state = thread_store
        .get::<SkillsThreadState>()
        .expect("skills thread state should be initialized");
    let turn_store = ExtensionData::new("turn");
    let config = thread_state.config();
    let warnings = Arc::new(WarningCounter::default());
    let mut builder = ExtensionRegistryBuilder::with_event_sink(warnings.clone());
    crate::install_with_providers(&mut builder, providers, move |_: &()| config.clone());
    let registry = builder.build();
    let contributor = registry
        .turn_lifecycle_contributors()
        .iter()
        .find(|contributor| contributor.requires_mcp_runtime(&thread_store))
        .expect("cloud skills contributor should be registered");
    let refresh_input = CatalogRefreshInput {
        mcp_access: Ok(codex_mcp::McpAttemptAccess::Unscoped),
        turn_id: "turn",
        session_store: &session_store,
        thread_store: &thread_store,
        turn_store: &turn_store,
    };

    assert!(
        !contributor
            .refresh_catalog_for_step(CatalogRefreshInput {
                mcp_access: Err(codex_mcp::McpAttemptRefused),
                ..refresh_input
            })
            .await
    );
    assert_eq!(cloud.lists.load(Ordering::SeqCst), 0);
    assert_eq!(warnings.0.load(Ordering::SeqCst), 0);

    for (index, delay) in [30, 60, 120, 240, 300, 300].into_iter().enumerate() {
        cloud.fail_list.store(true, Ordering::SeqCst);
        assert!(!contributor.refresh_catalog_for_step(refresh_input).await);
        assert_eq!(thread_state.available_cloud_catalog(), None);
        assert_eq!(cloud.lists.load(Ordering::SeqCst), index + 1);
        assert_eq!(warnings.0.load(Ordering::SeqCst), 1);
        tokio::time::advance(Duration::from_secs(delay - 1)).await;
        assert!(!contributor.refresh_catalog_for_step(refresh_input).await);
        assert_eq!(cloud.lists.load(Ordering::SeqCst), index + 1);
        tokio::time::advance(Duration::from_secs(1)).await;
    }
    cloud.pause_list.store(true, Ordering::SeqCst);
    let mut cancelled = contributor.refresh_catalog_for_step(refresh_input);
    assert!(futures::poll!(&mut cancelled).is_pending());
    drop(cancelled);
    *cloud
        .next_catalog
        .lock()
        .expect("test provider catalog mutex should not be poisoned") =
        Some(SkillCatalog::default());
    assert!(!contributor.refresh_catalog_for_step(refresh_input).await);
    assert!(!contributor.refresh_catalog_for_step(refresh_input).await);
    assert_eq!(cloud.lists.load(Ordering::SeqCst), 8);

    // Waiting callers reuse success and share failures' cooldowns; cancellation allows retry.
    for (fails, cancelled) in [(false, false), (true, false), (false, true)] {
        session_store.insert(SkillsSessionState {
            mcp_resources: Some(client()),
            extension_metrics: None,
        });
        let previous_calls = cloud.lists.load(Ordering::SeqCst);
        let prewarm_store = ExtensionData::new("prewarm");
        let first_catalog = SkillCatalog {
            warnings: vec!["catalog warning".into()],
            ..Default::default()
        };
        *cloud
            .next_catalog
            .lock()
            .expect("test provider catalog mutex should not be poisoned") =
            Some(first_catalog.clone());
        cloud.fail_list.store(fails, Ordering::SeqCst);
        cloud.pause_list.store(true, Ordering::SeqCst);
        let mut turn_start = contributor.refresh_catalog_for_step(refresh_input);
        assert!(futures::poll!(&mut turn_start).is_pending());
        let prewarm_input = CatalogRefreshInput {
            mcp_access: Ok(codex_mcp::McpAttemptAccess::Unscoped),
            turn_id: "prewarm",
            turn_store: &prewarm_store,
            ..refresh_input
        };
        let mut prewarm = contributor.refresh_catalog_for_step(prewarm_input);
        assert!(futures::poll!(&mut prewarm).is_pending());
        let mut cancelled_waiter = contributor.refresh_catalog_for_step(prewarm_input);
        assert!(futures::poll!(&mut cancelled_waiter).is_pending());
        drop(cancelled_waiter);
        assert_eq!(cloud.lists.load(Ordering::SeqCst), previous_calls + 1);
        *cloud
            .next_catalog
            .lock()
            .expect("test provider catalog mutex should not be poisoned") =
            Some(SkillCatalog::default());
        if cancelled {
            drop(turn_start);
        } else {
            cloud.resume.notify_one();
            assert!(!turn_start.await);
        }
        assert!(
            !tokio::time::timeout(std::time::Duration::from_secs(5), prewarm)
                .await
                .expect("waiting discovery should complete after its predecessor stops")
        );
        if fails {
            assert_eq!(thread_state.available_cloud_catalog(), None);
            assert_eq!(cloud.lists.load(Ordering::SeqCst), previous_calls + 1);
            tokio::time::advance(Duration::from_secs(30)).await;
            assert!(!contributor.refresh_catalog_for_step(prewarm_input).await);
        }
        assert_eq!(
            thread_state.available_cloud_catalog(),
            Some(if fails || cancelled {
                SkillCatalog::default()
            } else {
                first_catalog
            })
        );
        assert!(!contributor.refresh_catalog_for_step(prewarm_input).await);
        assert!(!contributor.refresh_catalog_for_step(refresh_input).await);
        assert_eq!(
            cloud.lists.load(Ordering::SeqCst),
            previous_calls + 1 + usize::from(fails || cancelled)
        );
        assert_eq!(warnings.0.load(Ordering::SeqCst), 1);
    }

    // New turns share the thread's cooldown and failure progression.
    session_store.insert(SkillsSessionState {
        mcp_resources: Some(client()),
        extension_metrics: None,
    });
    let next_turn = ExtensionData::new("next turn");
    let previous_calls = cloud.lists.load(Ordering::SeqCst);
    for turn_store in [&turn_store, &next_turn] {
        cloud.fail_list.store(true, Ordering::SeqCst);
        assert!(
            !contributor
                .refresh_catalog_for_step(CatalogRefreshInput {
                    mcp_access: Ok(codex_mcp::McpAttemptAccess::Unscoped),
                    turn_store,
                    ..refresh_input
                })
                .await
        );
        assert_eq!(cloud.lists.load(Ordering::SeqCst), previous_calls + 1);
    }
    assert_eq!(warnings.0.load(Ordering::SeqCst), 1);
    let next_input = CatalogRefreshInput {
        mcp_access: Ok(codex_mcp::McpAttemptAccess::Unscoped),
        turn_id: "next turn",
        turn_store: &next_turn,
        ..refresh_input
    };
    tokio::time::advance(Duration::from_secs(30)).await;
    assert!(!contributor.refresh_catalog_for_step(next_input).await);
    assert_eq!(cloud.lists.load(Ordering::SeqCst), previous_calls + 2);
    tokio::time::advance(Duration::from_secs(59)).await;
    assert!(!contributor.refresh_catalog_for_step(refresh_input).await);
    assert_eq!(cloud.lists.load(Ordering::SeqCst), previous_calls + 2);
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(!contributor.refresh_catalog_for_step(next_input).await);
    assert_eq!(cloud.lists.load(Ordering::SeqCst), previous_calls + 3);
    assert_eq!(warnings.0.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn late_read_cannot_return_or_populate_a_replaced_cache() {
    for replacement in [Some(client()), None] {
        let state = state();
        let provider = Arc::new(Provider::default());
        let providers = SkillProviders::new().with_cloud_provider(provider.clone());
        let original = Some(client());
        // None here exercises a successful per-turn refresh on the SAME generation.
        let replacement = replacement.or_else(|| original.clone());
        provider.pause_read.store(true, Ordering::SeqCst);
        let (late, ()) = tokio::join!(state.read_skill(&providers, request(original)), async {
            provider.entered.notified().await;
            state
                .refresh_cloud_catalog(
                    &providers,
                    query(replacement.clone()),
                    &ExtensionData::new("turn"),
                )
                .await
                .expect("cloud catalog refresh should succeed");
            provider.resume.notify_one();
        });
        assert!(late.is_err());
        assert_eq!(
            state
                .read_skill(&providers, request(replacement))
                .await
                .expect("cloud skill read should succeed")
                .contents,
            "contents 1"
        );
    }
}

#[tokio::test]
async fn late_discovery_cannot_publish_into_a_new_generation() {
    let state = state();
    let provider = Arc::new(Provider::default());
    let providers = SkillProviders::new().with_cloud_provider(provider.clone());
    provider.pause_list.store(true, Ordering::SeqCst);
    let turn_store = ExtensionData::new("turn");
    let (late, ()) = tokio::join!(
        state.refresh_cloud_catalog(&providers, query(Some(client())), &turn_store),
        async {
            provider.entered.notified().await;
            state
                .catalog_state
                .lock()
                .expect("catalog state mutex should not be poisoned")
                .prepare_cloud_generation(Some(&client()));
            provider.resume.notify_one();
        }
    );
    late.expect("superseded discovery should be discarded without an error");
    assert!(turn_store.get::<LatestCloudSkillsAuth>().is_none());
    assert!(
        state
            .catalog_state
            .lock()
            .expect("catalog state mutex should not be poisoned")
            .cloud_generation
            .as_ref()
            .expect("replacement cloud generation should exist")
            .catalog
            .is_none()
    );
}

#[tokio::test(start_paused = true)]
async fn partial_cloud_catalog_without_warnings_is_refreshed_next_turn() {
    let state = state();
    let provider = Arc::new(Provider::default());
    provider.partial_list.store(true, Ordering::SeqCst);
    let providers = SkillProviders::new().with_cloud_provider(provider.clone());
    let client = Some(client());
    let turn_store = ExtensionData::new("turn");
    state
        .refresh_cloud_catalog(&providers, query(client.clone()), &turn_store)
        .await
        .expect("cloud catalog refresh should succeed");
    // Reuse partial catalogs for the rest of the turn.
    state
        .refresh_cloud_catalog(&providers, query(client.clone()), &turn_store)
        .await
        .expect("cloud catalog refresh should succeed");
    assert_eq!(provider.lists.load(Ordering::SeqCst), 1);
    assert_eq!(
        state
            .available_cloud_catalog()
            .expect("cloud catalog should be available after refresh"),
        SkillCatalog {
            is_partial: true,
            ..Default::default()
        }
    );
    state
        .refresh_cloud_catalog(
            &providers,
            query(client.clone()),
            &ExtensionData::new("next turn"),
        )
        .await
        .expect("cloud catalog refresh should succeed");
    assert_eq!(
        state
            .available_cloud_catalog()
            .expect("cloud catalog should be available after refresh"),
        SkillCatalog {
            is_partial: false,
            warnings: vec!["account catalog".into()],
            ..Default::default()
        }
    );
    // Retry failures even when the previous catalog remains available.
    let retry_turn = ExtensionData::new("retry turn");
    provider.fail_list.store(true, Ordering::SeqCst);
    assert!(
        state
            .refresh_cloud_catalog(&providers, query(client.clone()), &retry_turn)
            .await
            .is_err()
    );
    assert!(state.available_cloud_catalog().is_some());
    assert!(retry_turn.get::<LatestCloudSkillsAuth>().is_none());
    tokio::time::advance(Duration::from_secs(30)).await;
    state
        .refresh_cloud_catalog(&providers, query(client), &retry_turn)
        .await
        .expect("cloud catalog refresh should succeed");
    assert_eq!(provider.lists.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn cloud_refresh_preserves_complete_catalogs_only_within_the_same_auth_scope() {
    let catalog = |name: &str, is_partial| SkillCatalog {
        entries: vec![SkillCatalogEntry::new(
            SkillPackageId("skill://demo".into()),
            SkillAuthority::new(SkillSourceKind::Cloud, "codex_apps"),
            name,
            "test catalog",
            SkillResourceId::new("skill://demo/SKILL.md"),
        )],
        // Complete listings can have diagnostics; these also keep discovery eligible for refresh.
        warnings: vec![format!("{name} catalog diagnostic")],
        is_partial,
    };
    for previous_partial in [false, true] {
        for incoming_partial in [false, true] {
            for changed_auth in [false, true] {
                let state = state();
                let provider = Arc::new(Provider::default());
                let providers = SkillProviders::new().with_cloud_provider(provider.clone());
                let original_client = client();
                let mut previous = catalog("previous", previous_partial);
                *provider
                    .next_catalog
                    .lock()
                    .expect("test provider catalog mutex should not be poisoned") =
                    Some(previous.clone());
                state
                    .refresh_cloud_catalog(
                        &providers,
                        query(Some(original_client.clone())),
                        &ExtensionData::new("turn"),
                    )
                    .await
                    .expect("cloud catalog refresh should succeed");
                let generation = state
                    .catalog_state
                    .lock()
                    .expect("catalog state mutex should not be poisoned")
                    .cloud_generation
                    .clone()
                    .expect("initial refresh should publish a cloud generation");
                let contents = state
                    .read_skill(&providers, request(Some(original_client.clone())))
                    .await
                    .expect("cloud skill read should succeed");

                let incoming = catalog("incoming", incoming_partial);
                *provider
                    .next_catalog
                    .lock()
                    .expect("test provider catalog mutex should not be poisoned") =
                    Some(incoming.clone());
                let current_client = if changed_auth {
                    client()
                } else {
                    original_client
                };
                state
                    .refresh_cloud_catalog(
                        &providers,
                        query(Some(current_client.clone())),
                        &ExtensionData::new("next turn"),
                    )
                    .await
                    .expect("cloud catalog refresh should succeed");
                let retained = !changed_auth && !previous_partial && incoming_partial;
                assert_eq!(
                    generation
                        .catalog
                        .as_ref()
                        .expect("published generation should contain a catalog")
                        .is_partial,
                    previous_partial
                );
                if retained {
                    previous.is_partial = true;
                }
                assert_eq!(
                    state.available_cloud_catalog(),
                    Some(if retained { previous } else { incoming })
                );
                assert!(!Arc::ptr_eq(
                    &generation,
                    state
                        .catalog_state
                        .lock()
                        .expect("catalog state mutex should not be poisoned")
                        .cloud_generation
                        .as_ref()
                        .expect("refresh should publish a cloud generation")
                ));
                assert_eq!(
                    state
                        .read_skill(&providers, request(Some(current_client)))
                        .await
                        .expect("cloud skill read should succeed")
                        .contents,
                    if retained {
                        contents.contents
                    } else {
                        "contents 1".to_string()
                    },
                );
            }
        }
    }
}

#[derive(Default)]
struct DiscoveryCounts {
    live: AtomicUsize,
    refuse: AtomicBool,
}
struct DiscoveryWork(Arc<DiscoveryCounts>);
impl DiscoveryWork {
    fn new(counts: &Arc<DiscoveryCounts>) -> Self {
        counts.live.fetch_add(1, Ordering::SeqCst);
        Self(counts.clone())
    }
}
impl Drop for DiscoveryWork {
    fn drop(&mut self) {
        self.0.live.fetch_sub(1, Ordering::SeqCst);
    }
}
impl codex_mcp::McpAttemptWork for DiscoveryWork {
    fn derive_attempt(
        &self,
    ) -> Result<Box<dyn codex_mcp::McpAttemptWork>, codex_mcp::McpAttemptRefused> {
        if self.0.refuse.load(Ordering::SeqCst) {
            return Err(codex_mcp::McpAttemptRefused);
        }
        Ok(Box::new(Self::new(&self.0)))
    }
}
#[derive(Default)]
struct AdmittedDiscoveryProvider {
    calls: AtomicUsize,
    pause: AtomicBool,
    resume: Notify,
}

impl SkillProvider for AdmittedDiscoveryProvider {
    fn list<'a>(&'a self, query: SkillListQuery<'a>) -> SkillProviderFuture<'a, SkillCatalog> {
        Box::pin(async move {
            let codex_mcp::McpAttemptAccess::Admitted(work) = query.mcp_access else {
                return Err(SkillProviderError::new("missing invocation work"));
            };
            let _rpc = work
                .derive_attempt()
                .map_err(|error| SkillProviderError::new(error.to_string()))?;
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.pause.swap(false, Ordering::SeqCst) {
                self.resume.notified().await;
            }
            Ok(SkillCatalog::default())
        })
    }
    fn read<'a>(
        &'a self,
        _request: SkillReadRequest<'a>,
    ) -> SkillProviderFuture<'a, SkillReadResult> {
        Box::pin(async { Err(SkillProviderError::new("not a read fixture")) })
    }
    fn search(&self, _request: SkillSearchRequest) -> SkillProviderFuture<'_, SkillSearchResult> {
        Box::pin(async { Err(SkillProviderError::new("not a search fixture")) })
    }
}

#[tokio::test]
async fn admitted_cloud_discovery_is_finite_and_refuses_before_serialization() {
    let counts = Arc::new(DiscoveryCounts::default());
    let root = DiscoveryWork::new(&counts);
    let provider = Arc::new(AdmittedDiscoveryProvider::default());
    let session = ExtensionData::new("session");
    let thread = ExtensionData::new("thread");
    let turn = ExtensionData::new("turn");
    thread.insert(state());
    let state = thread.get::<SkillsThreadState>().unwrap();
    let config = state.config();
    let mut builder = ExtensionRegistryBuilder::new();
    crate::install_with_providers(
        &mut builder,
        SkillProviders::new().with_cloud_provider(provider.clone()),
        move |_: &()| config.clone(),
    );
    let registry = builder.build();
    let contributor = registry
        .turn_lifecycle_contributors()
        .iter()
        .find(|contributor| contributor.requires_mcp_runtime(&thread))
        .unwrap();
    let input = CatalogRefreshInput {
        mcp_access: Ok(codex_mcp::McpAttemptAccess::Admitted(&root)),
        turn_id: "turn",
        session_store: &session,
        thread_store: &thread,
        turn_store: &turn,
    };
    counts.refuse.store(true, Ordering::SeqCst);
    assert!(!contributor.refresh_catalog_for_step(input).await);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    assert_eq!(state.available_cloud_catalog(), None);
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    counts.refuse.store(false, Ordering::SeqCst);
    provider.pause.store(true, Ordering::SeqCst);
    let mut first = contributor.refresh_catalog_for_step(input);
    assert!(futures::poll!(&mut first).is_pending());
    assert_eq!(counts.live.load(Ordering::SeqCst), 3);
    let mut waiter = contributor.refresh_catalog_for_step(input);
    assert!(futures::poll!(&mut waiter).is_pending());
    assert_eq!(counts.live.load(Ordering::SeqCst), 4);
    counts.refuse.store(true, Ordering::SeqCst);
    let mut refused = contributor.refresh_catalog_for_step(input);
    assert!(matches!(
        futures::poll!(&mut refused),
        std::task::Poll::Ready(false)
    ));
    drop(refused);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    drop(waiter);
    assert_eq!(counts.live.load(Ordering::SeqCst), 3);
    drop(first);
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    assert_eq!(state.available_cloud_catalog(), None);
    counts.refuse.store(false, Ordering::SeqCst);
    assert!(!contributor.refresh_catalog_for_step(input).await);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    assert!(!contributor.refresh_catalog_for_step(input).await);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    assert!(state.available_cloud_catalog().is_some());
    drop(root);
    assert_eq!(
        counts.live.load(Ordering::SeqCst),
        0,
        "retained catalog must not retain invocation work"
    );
}

#[derive(Default)]
struct AdmittedReadProvider {
    reads: AtomicUsize,
    pause: AtomicBool,
    resume: Notify,
}
impl SkillProvider for AdmittedReadProvider {
    fn list<'a>(&'a self, _query: SkillListQuery<'a>) -> SkillProviderFuture<'a, SkillCatalog> {
        Box::pin(async { Err(SkillProviderError::new("not a discovery fixture")) })
    }
    fn read<'a>(
        &'a self,
        request: SkillReadRequest<'a>,
    ) -> SkillProviderFuture<'a, SkillReadResult> {
        Box::pin(async move {
            let codex_mcp::McpAttemptAccess::Admitted(work) = request
                .mcp_access
                .map_err(|error| SkillProviderError::new(error.to_string()))?
            else {
                return Err(SkillProviderError::new("missing callback work"));
            };
            let _child = work
                .derive_attempt()
                .map_err(|error| SkillProviderError::new(error.to_string()))?;
            let revision = self.reads.fetch_add(1, Ordering::SeqCst);
            if self.pause.swap(false, Ordering::SeqCst) {
                self.resume.notified().await;
            }
            Ok(SkillReadResult {
                resource: request.resource,
                contents: format!("admitted contents {revision}"),
            })
        })
    }
    fn search(&self, _request: SkillSearchRequest) -> SkillProviderFuture<'_, SkillSearchResult> {
        Box::pin(async { Err(SkillProviderError::new("not a search fixture")) })
    }
}
#[tokio::test]
async fn cloud_reads_require_live_invocation_work_even_for_cached_contents() {
    let state = state();
    let counts = Arc::new(DiscoveryCounts::default());
    let root = DiscoveryWork::new(&counts);
    let provider = Arc::new(AdmittedReadProvider::default());
    let providers = SkillProviders::new().with_cloud_provider(provider.clone());
    let request = SkillReadRequest {
        mcp_access: Ok(codex_mcp::McpAttemptAccess::Admitted(&root)),
        ..request(None)
    };
    counts.refuse.store(true, Ordering::SeqCst);
    {
        let _catalog = state.catalog_state.lock().unwrap();
        let mut read = Box::pin(state.read_skill(&providers, request.clone()));
        assert!(matches!(
            futures::poll!(&mut read),
            std::task::Poll::Ready(Err(_))
        ));
        assert_eq!(provider.reads.load(Ordering::SeqCst), 0);
        assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    }
    counts.refuse.store(false, Ordering::SeqCst);
    provider.pause.store(true, Ordering::SeqCst);
    let mut read = Box::pin(state.read_skill(&providers, request.clone()));
    assert!(futures::poll!(&mut read).is_pending());
    assert_eq!(counts.live.load(Ordering::SeqCst), 3);
    drop(read);
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    let result = state.read_skill(&providers, request.clone()).await.unwrap();
    assert_eq!(result.contents, "admitted contents 1");
    assert_eq!(provider.reads.load(Ordering::SeqCst), 2);
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    assert_eq!(
        state.read_skill(&providers, request.clone()).await.unwrap(),
        result
    );
    assert_eq!(provider.reads.load(Ordering::SeqCst), 2);
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    counts.refuse.store(true, Ordering::SeqCst);
    assert!(state.read_skill(&providers, request.clone()).await.is_err());
    assert!(
        state
            .read_skill(
                &providers,
                SkillReadRequest {
                    mcp_access: Err(codex_mcp::McpAttemptRefused),
                    ..request
                }
            )
            .await
            .is_err()
    );
    assert_eq!(provider.reads.load(Ordering::SeqCst), 2);
    drop(root);
    assert_eq!(
        counts.live.load(Ordering::SeqCst),
        0,
        "cached contents retain no account work"
    );
}
