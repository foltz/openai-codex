//! Invocation carriers reach the real Runtime/connection-set startup boundary.
//! Wire fixtures are in process; these controls prove admission, not provider behavior.
use super::*;
use crate::connection_manager::tests::{create_ready_async_managed_client, create_test_tool};
use crate::{McpAttemptAccess, McpAttemptRefused, McpAttemptRequirement, McpAttemptWork};
use futures::FutureExt;
use pretty_assertions::assert_eq;
use std::sync::atomic::AtomicUsize;

struct Work {
    live: Arc<AtomicUsize>,
    refuse: Arc<AtomicBool>,
}
impl McpAttemptWork for Work {
    fn derive_attempt(&self) -> Result<Box<dyn McpAttemptWork>, McpAttemptRefused> {
        if self.refuse.load(Ordering::SeqCst) {
            return Err(McpAttemptRefused);
        }
        self.live.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(Self {
            live: Arc::clone(&self.live),
            refuse: Arc::clone(&self.refuse),
        }))
    }
}
impl Drop for Work {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

struct Fixture {
    runtime: Arc<McpRuntime>,
    client: crate::rmcp_client::AsyncManagedClient,
    finish: tokio::sync::oneshot::Sender<()>,
    starts: Arc<AtomicUsize>,
    work: Work,
    tool: ToolInfo,
    name: &'static str,
}
async fn fixture(apps: bool) -> Fixture {
    let name = if apps {
        CODEX_APPS_MCP_SERVER_NAME
    } else {
        "docs"
    };
    let tool = create_test_tool(name, "search");
    let mut client = create_ready_async_managed_client(vec![tool.clone()]).await;
    let ready = client.client().await.expect("in-process ready fixture");
    client.is_codex_apps_mcp_server = apps;
    client.startup_complete.store(false, Ordering::SeqCst);
    let (finish, receive) = tokio::sync::oneshot::channel();
    let starts = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&starts);
    client.client = crate::rmcp_client::ClientStartup::new_with_work(
        move |work| {
            async move {
                let _work = work;
                observed.fetch_add(1, Ordering::SeqCst);
                receive.await.expect("release original startup");
                Ok(ready)
            }
            .boxed()
            .shared()
        },
        McpAttemptRequirement::Required,
    );
    let mut set = McpConnectionSet::empty(true);
    set.insert_test_client(name, client.clone());
    let set = Arc::new(set);
    let mut config = crate::mcp::tests::test_mcp_config(std::env::temp_dir());
    config
        .server_permissions
        .insert(name.to_string(), crate::McpServerPermissions::Thread);
    if apps {
        let mut catalog = crate::ResolvedMcpCatalog::builder();
        catalog.register(crate::McpServerRegistration::from_hosted_apps(
            "authority-test",
            0,
            crate::connection_pool::tests::config(),
        ));
        config.mcp_server_catalog = catalog.build();
    }
    let runtime = Arc::new(McpRuntime::empty(true));
    runtime.current.store(Arc::new(PublishedMcpRuntime {
        connections: set,
        config: Some(Arc::new(config)),
        auth: None,
        auth_manager: None,
        auth_token: None,
        auth_owner: None,
        auth_generation: Arc::new(()),
        plugins_available: false,
        ready_selected_capability_roots: Vec::new(),
        environment_selections: Arc::default(),
        ready_environments: HashMap::new(),
        cached_binding: Mutex::new(None),
    }));
    Fixture {
        runtime,
        client,
        finish,
        starts,
        work: Work {
            live: Arc::new(AtomicUsize::new(1)),
            refuse: Arc::new(AtomicBool::new(false)),
        },
        tool,
        name,
    }
}

#[derive(Clone, Copy, Debug)]
enum Path {
    Wait,
    Ready,
    Binding,
    RequiredBinding,
    CallBinding,
    Prepare,
    Call,
    Unattributed,
    Read,
    AllResources,
    AllTemplates,
    Infos,
    Tools,
    Refresh,
    CapturedRefresh,
    BindingRead,
    BindingList,
    BindingTemplates,
    FacadeRead,
    FacadeList,
    AppsList,
    Events,
    OpenEvents,
}
impl Path {
    fn apps(self) -> bool {
        matches!(
            self,
            Self::Refresh
                | Self::CapturedRefresh
                | Self::AppsList
                | Self::Events
                | Self::OpenEvents
        )
    }
}
async fn invoke(path: Path, f: &Fixture, access: McpAttemptAccess<'_>) {
    let runtime = &f.runtime;
    let name = f.name;
    let set = runtime.latest_connections();
    match path {
        Path::Wait => {
            runtime
                .wait_for_server_startup_with_authority(name, access)
                .await;
        }
        Path::Ready => {
            let _ = runtime
                .latest_wait_for_server_ready_with_authority(name, Duration::from_secs(1), access)
                .await;
        }
        Path::Binding => {
            let _ = runtime.current_binding_with_authority(access).await;
        }
        Path::RequiredBinding => {
            let _ = runtime
                .current_binding_with_requirements_and_authority(
                    &[name.to_string()],
                    &HashSet::new(),
                    access,
                )
                .await;
        }
        Path::CallBinding => {
            let _ = runtime
                .current_binding_for_call_with_authority(name, access)
                .await;
        }
        Path::Prepare => {
            let _ = runtime.prepare_call_with_authority(&f.tool, access).await;
        }
        Path::Call => {
            let _ = runtime
                .latest_call_tool_with_authority(
                    name, "search", None, None, None, None, true, access,
                )
                .await;
        }
        Path::Unattributed => {
            let _ = runtime
                .latest_call_tool_without_attribution_with_authority(
                    name, "search", None, None, access,
                )
                .await;
        }
        Path::Read => {
            let _ = runtime
                .latest_read_resource_with_authority(
                    name,
                    ReadResourceRequestParams::new("test://proof"),
                    access,
                )
                .await;
        }
        Path::AllResources => {
            let _ = set
                .list_all_resources_with_authority(|_| true, access)
                .await;
        }
        Path::AllTemplates => {
            let _ = set
                .list_all_resource_templates_with_authority(|_| true, access)
                .await;
        }
        Path::Infos => {
            let _ = set
                .list_available_server_infos_with_authority(|_| true, access)
                .await;
        }
        Path::Tools => {
            let _ = runtime.latest_list_all_tools_with_authority(access).await;
        }
        Path::Refresh => {
            let _ = runtime
                .latest_hard_refresh_codex_apps_tools_cache_with_authority(access)
                .await;
        }
        Path::CapturedRefresh => {
            let _ = runtime
                .capture_apps_tool_refresh_with_authority(access)
                .await;
        }
        Path::BindingRead | Path::BindingList | Path::BindingTemplates => {
            let binding = McpBinding::new(
                set,
                Arc::default(),
                runtime.current_config().unwrap(),
                false,
                Vec::new(),
                HashMap::new(),
            );
            match path {
                Path::BindingRead => {
                    let _ = binding
                        .read_resource_with_authority(
                            name,
                            ReadResourceRequestParams::new("test://proof"),
                            access,
                        )
                        .await;
                }
                Path::BindingList => {
                    let _ = binding
                        .list_resources_with_authority(name, None, access)
                        .await;
                }
                Path::BindingTemplates => {
                    let _ = binding
                        .list_resource_templates_with_authority(name, None, access)
                        .await;
                }
                _ => unreachable!(),
            }
        }
        Path::FacadeRead | Path::FacadeList | Path::AppsList | Path::Events | Path::OpenEvents => {
            let facade = crate::McpResourceClient::new(Arc::clone(runtime));
            match path {
                Path::FacadeRead => {
                    let _ = facade
                        .read_resource_with_authority(name, "test://proof", access)
                        .await;
                }
                Path::FacadeList => {
                    let _ = facade
                        .list_resources_with_authority(name, None, access)
                        .await;
                }
                Path::AppsList => {
                    let _ = facade
                        .list_codex_apps_resources_with_authority(
                            crate::CodexAppsResourceListParams {
                                cursor: None,
                                mime_type: "mcp/plugin".into(),
                            },
                            access,
                        )
                        .await;
                }
                Path::Events => {
                    let _ = facade.list_events_with_authority(access).await;
                }
                Path::OpenEvents => {
                    let _ = facade
                        .open_event_stream_with_authority(
                            "proof",
                            &serde_json::json!({}),
                            None,
                            access,
                        )
                        .await;
                }
                _ => unreachable!(),
            }
        }
    }
}

async fn check_paths(paths: &[Path]) {
    for &path in paths {
        let f = fixture(path.apps()).await;
        tokio::time::timeout(
            Duration::from_secs(3),
            invoke(path, &f, McpAttemptAccess::Unscoped),
        )
        .await
        .expect("unscoped invocation must refuse promptly");
        assert_eq!(f.starts.load(Ordering::SeqCst), 0, "{path:?}");
        assert!(
            f.client.client.peek().is_none(),
            "refusal must not poison original: {path:?}"
        );
        f.work.refuse.store(true, Ordering::SeqCst);
        tokio::time::timeout(
            Duration::from_secs(3),
            invoke(path, &f, McpAttemptAccess::Admitted(&f.work)),
        )
        .await
        .expect("exhausted derivation must refuse promptly");
        assert_eq!(f.starts.load(Ordering::SeqCst), 0, "{path:?}");
        assert_eq!(f.work.live.load(Ordering::SeqCst), 1, "{path:?}");
        assert!(f.client.client.peek().is_none());
        f.work.refuse.store(false, Ordering::SeqCst);
        let mut admitted = Box::pin(invoke(path, &f, McpAttemptAccess::Admitted(&f.work)));
        assert!(
            futures::poll!(admitted.as_mut()).is_pending(),
            "authority must reach held original: {path:?}"
        );
        assert_eq!(f.starts.load(Ordering::SeqCst), 1, "{path:?}");
        assert_eq!(f.work.live.load(Ordering::SeqCst), 2, "{path:?}");
        // Drop the API observer while retaining the same shared startup original.
        drop(admitted);
        assert_eq!(
            f.work.live.load(Ordering::SeqCst),
            2,
            "original custody survives dropped API observer: {path:?}"
        );
        let Fixture {
            finish,
            runtime,
            client,
            starts,
            work,
            tool,
            name,
        } = f;
        finish.send(()).unwrap();
        // Continue polling the very same Shared original, not a second attempt.
        client
            .client()
            .await
            .expect("original completes after observer release");
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        assert_eq!(work.live.load(Ordering::SeqCst), 1);
        let live = Arc::clone(&work.live);
        drop(work);
        assert_eq!(
            live.load(Ordering::SeqCst),
            0,
            "idle runtime/client retain no work: {path:?}"
        );
        runtime.shutdown().await;
        drop((tool, name));
    }
}

#[tokio::test]
async fn runtime_request_paths_forward_only_invocation_authority() {
    check_paths(&[
        Path::Wait,
        Path::Ready,
        Path::Prepare,
        Path::Call,
        Path::Unattributed,
        Path::Read,
    ])
    .await;
}
#[tokio::test]
async fn binding_capture_and_deferred_reads_forward_invocation_authority() {
    check_paths(&[
        Path::Binding,
        Path::RequiredBinding,
        Path::CallBinding,
        Path::BindingRead,
        Path::BindingList,
        Path::BindingTemplates,
    ])
    .await;
}
#[tokio::test]
async fn catalog_refresh_and_metadata_forward_invocation_authority() {
    check_paths(&[
        Path::Tools,
        Path::Infos,
        Path::Refresh,
        Path::CapturedRefresh,
        Path::AllResources,
        Path::AllTemplates,
    ])
    .await;
}
#[tokio::test]
async fn resource_facade_and_event_startup_forward_invocation_authority() {
    check_paths(&[
        Path::FacadeRead,
        Path::FacadeList,
        Path::AppsList,
        Path::Events,
        Path::OpenEvents,
    ])
    .await;
}
#[tokio::test]
async fn nonwaiting_tool_call_never_acquires_new_startup_work() {
    let f = fixture(false).await;
    assert!(
        f.runtime
            .latest_call_tool_with_authority(
                f.name,
                "search",
                None,
                None,
                None,
                None,
                false,
                McpAttemptAccess::Admitted(&f.work)
            )
            .await
            .is_err()
    );
    assert_eq!(f.starts.load(Ordering::SeqCst), 0);
    assert_eq!(f.work.live.load(Ordering::SeqCst), 1);
    assert!(f.client.client.peek().is_none());
    f.runtime.shutdown().await;
}
