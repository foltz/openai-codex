use super::ConnectionSessionState;
use super::MessageProcessor;
use super::MessageProcessorArgs;
use crate::analytics_utils::analytics_events_client_from_config;
use crate::config_manager::ConfigManager;
use crate::outgoing_message::ConnectionId;
use crate::outgoing_message::OutgoingMessageSender;
use crate::transport::AppServerTransport;
use anyhow::Result;
use app_test_support::create_mock_responses_server_repeating_assistant;
use app_test_support::write_mock_responses_config_toml;
use codex_analytics::AppServerRpcTransport;
use codex_app_server_protocol::CancelManagedTransitionParams;
use codex_app_server_protocol::CancelManagedTransitionResponse;
use codex_app_server_protocol::ClientInfo;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::InitializeCapabilities;
use codex_app_server_protocol::InitializeParams;
use codex_app_server_protocol::InitializeResponse;
use codex_app_server_protocol::JSONRPCRequest;
use codex_app_server_protocol::MANAGED_AUTH_TRANSITION_CONTRACT_VERSION;
use codex_app_server_protocol::ManagedTransitionIntent;
use codex_app_server_protocol::ManagedTransitionRefusalKind;
use codex_app_server_protocol::ReadManagedTransitionParams;
use codex_app_server_protocol::ReadManagedTransitionResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::StartManagedTransitionParams;
use codex_app_server_protocol::StartManagedTransitionResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::UserInput;
use codex_arg0::Arg0DispatchPaths;
use codex_config::CloudConfigBundleLoader;
use codex_config::LoaderOverrides;
use codex_core::config::Config;
use codex_core::config::ConfigBuilder;
use codex_exec_server::EnvironmentManager;
use codex_feedback::CodexFeedback;
use codex_login::AuthManager;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::W3cTraceContext;
use opentelemetry::global;
use opentelemetry::trace::SpanId;
use opentelemetry::trace::SpanKind;
use opentelemetry::trace::TraceId;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::InMemorySpanExporter;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::trace::SpanData;
use pretty_assertions::assert_eq;
use serial_test::serial;
use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::sync::OnceLock;
use tempfile::TempDir;
use tokio::sync::mpsc;
use tracing_subscriber::layer::SubscriberExt;
use wiremock::MockServer;

const TEST_CONNECTION_ID: ConnectionId = ConnectionId(7);

struct TestTracing {
    exporter: InMemorySpanExporter,
    provider: SdkTracerProvider,
}

struct RemoteTrace {
    trace_id: TraceId,
    parent_span_id: SpanId,
    context: W3cTraceContext,
}

impl RemoteTrace {
    fn new(trace_id: &str, parent_span_id: &str) -> Self {
        let trace_id = TraceId::from_hex(trace_id).expect("trace id");
        let parent_span_id = SpanId::from_hex(parent_span_id).expect("parent span id");
        let context = W3cTraceContext {
            traceparent: Some(format!("00-{trace_id}-{parent_span_id}-01")),
            tracestate: Some("vendor=value".to_string()),
        };

        Self {
            trace_id,
            parent_span_id,
            context,
        }
    }
}

fn init_test_tracing() -> &'static TestTracing {
    static TEST_TRACING: OnceLock<TestTracing> = OnceLock::new();
    TEST_TRACING.get_or_init(|| {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("codex-app-server-message-processor-tests");
        global::set_text_map_propagator(TraceContextPropagator::new());
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        tracing::subscriber::set_global_default(subscriber)
            .expect("global tracing subscriber should only be installed once");
        TestTracing { exporter, provider }
    })
}

fn request_from_client_request(request: ClientRequest) -> JSONRPCRequest {
    serde_json::from_value(serde_json::to_value(request).expect("serialize client request"))
        .expect("client request should convert to JSON-RPC")
}

struct TracingHarness {
    _server: MockServer,
    _codex_home: TempDir,
    processor: Arc<MessageProcessor>,
    outgoing_rx: mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
    session: Arc<ConnectionSessionState>,
    tracing: &'static TestTracing,
    telemetry: Option<TestManagedTelemetry>,
}

enum TestTelemetry {
    Unavailable,
    Managed,
}

struct TestManagedTelemetry {
    cancel: tokio_util::sync::CancellationToken,
    task: tokio::task::JoinHandle<
        crate::otel_reloader::ManagedReloader<tracing_subscriber::Registry>,
    >,
    _dispatch: tracing::Dispatch,
}

impl TracingHarness {
    async fn new() -> Result<Self> {
        Self::with_codex_home(TempDir::new()?).await
    }

    async fn with_codex_home(codex_home: TempDir) -> Result<Self> {
        Self::with_telemetry(codex_home, TestTelemetry::Unavailable).await
    }

    async fn with_telemetry(codex_home: TempDir, telemetry: TestTelemetry) -> Result<Self> {
        let server = create_mock_responses_server_repeating_assistant("Done").await;
        let mut config = build_test_config(codex_home.path(), &server.uri()).await?;
        config.chatgpt_base_url = format!("{}/backend-api", server.uri());
        let config = Arc::new(config);
        let (processor, outgoing_rx, telemetry) = build_test_processor(config, telemetry).await;
        let tracing = init_test_tracing();
        tracing.exporter.reset();
        tracing::callsite::rebuild_interest_cache();
        let mut harness = Self {
            _server: server,
            _codex_home: codex_home,
            processor,
            outgoing_rx,
            session: Arc::new(ConnectionSessionState::new(
                crate::transport::ConnectionOrigin::Stdio,
            )),
            tracing,
            telemetry,
        };

        let _: InitializeResponse = harness
            .request(
                ClientRequest::Initialize {
                    request_id: RequestId::Integer(1),
                    params: InitializeParams {
                        client_info: ClientInfo {
                            name: "codex-app-server-tests".to_string(),
                            title: None,
                            version: "0.1.0".to_string(),
                        },
                        capabilities: Some(InitializeCapabilities {
                            experimental_api: true,
                            ..Default::default()
                        }),
                    },
                },
                /*trace*/ None,
            )
            .await;
        assert!(harness.session.initialized());

        Ok(harness)
    }

    fn reset_tracing(&self) {
        self.tracing.exporter.reset();
    }

    async fn shutdown(self) {
        self.processor.shutdown_threads().await;
        self.processor.drain_background_tasks().await;
        if let Some(telemetry) = self.telemetry {
            telemetry.cancel.cancel();
            let owner = tokio::time::timeout(std::time::Duration::from_secs(30), telemetry.task)
                .await
                .expect("telemetry owner shutdown deadline")
                .expect("telemetry owner join");
            assert_eq!(owner.shutdown_result, Some(Ok(())));
        }
    }

    async fn request<T>(&mut self, request: ClientRequest, trace: Option<W3cTraceContext>) -> T
    where
        T: serde::de::DeserializeOwned,
    {
        let request_id = match request.id() {
            RequestId::Integer(request_id) => *request_id,
            request_id => panic!("expected integer request id in test harness, got {request_id:?}"),
        };
        let mut request = request_from_client_request(request);
        request.trace = trace;

        self.processor
            .process_request(
                TEST_CONNECTION_ID,
                request,
                &AppServerTransport::Stdio,
                Arc::clone(&self.session),
            )
            .await;
        read_response(&mut self.outgoing_rx, request_id).await
    }

    async fn start_thread(
        &mut self,
        request_id: i64,
        trace: Option<W3cTraceContext>,
    ) -> ThreadStartResponse {
        let response = self
            .request(
                ClientRequest::ThreadStart {
                    request_id: RequestId::Integer(request_id),
                    params: ThreadStartParams {
                        ephemeral: Some(true),
                        ..ThreadStartParams::default()
                    },
                },
                trace,
            )
            .await;
        read_thread_started_notification(&mut self.outgoing_rx).await;
        response
    }
}

async fn build_test_config(codex_home: &Path, server_uri: &str) -> Result<Config> {
    write_mock_responses_config_toml(
        codex_home,
        server_uri,
        &BTreeMap::new(),
        /*auto_compact_limit*/ 8_192,
        Some(false),
        "mock_provider",
        "compact",
    )?;

    Ok(ConfigBuilder::default()
        .codex_home(codex_home.to_path_buf())
        .build()
        .await?)
}

async fn build_test_processor(
    config: Arc<Config>,
    telemetry: TestTelemetry,
) -> (
    Arc<MessageProcessor>,
    mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
    Option<TestManagedTelemetry>,
) {
    let (outgoing_tx, outgoing_rx) = mpsc::channel(16);
    let auth_manager =
        AuthManager::shared_from_config(config.as_ref(), /*enable_codex_api_key_env*/ false)
            .await
            .expect("test auth manager");
    let config_manager = ConfigManager::new(
        config.codex_home.to_path_buf(),
        Vec::new(),
        LoaderOverrides::default(),
        /*strict_config*/ false,
        CloudConfigBundleLoader::default(),
        Arg0DispatchPaths::default(),
        Arc::new(codex_config::NoopThreadConfigLoader),
    );
    let analytics_events_client =
        analytics_events_client_from_config(Arc::clone(&auth_manager), config.as_ref());
    let (telemetry_reset, telemetry) = match telemetry {
        TestTelemetry::Unavailable => (
            crate::otel_reset_control::TelemetryResetControl::default(),
            None,
        ),
        TestTelemetry::Managed => {
            let (layers, routes) = codex_otel::ManagedTelemetryRoutes::layers();
            let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layers));
            let mut candidate = Some(
                codex_core::otel_init::prepare_provider(
                    &config,
                    "test",
                    Some(crate::OTEL_SERVICE_NAME),
                    /*default_analytics_enabled*/ false,
                )
                .expect("prepare actual telemetry provider"),
            );
            let initial = routes
                .publish(&mut candidate)
                .expect("publish actual telemetry routes");
            let cancel = tokio_util::sync::CancellationToken::new();
            let (control, task) = crate::otel_reloader::spawn_managed(
                initial,
                routes,
                config_manager.clone(),
                Arc::clone(&auth_manager),
                /*default_analytics_enabled*/ false,
                cancel.clone(),
            );
            (
                control,
                Some(TestManagedTelemetry {
                    cancel,
                    task,
                    _dispatch: dispatch,
                }),
            )
        }
    };
    let outgoing = Arc::new(OutgoingMessageSender::new(
        outgoing_tx,
        analytics_events_client.clone(),
    ));
    let processor = Arc::new(MessageProcessor::new(MessageProcessorArgs {
        telemetry_reset,
        outgoing,
        analytics_events_client,
        arg0_paths: Arg0DispatchPaths::default(),
        config,
        config_manager,
        environment_manager: Arc::new(EnvironmentManager::default_for_tests()),
        feedback: CodexFeedback::new(),
        log_db: None,
        state_db: None,
        config_warnings: Vec::new(),
        session_source: SessionSource::VSCode,
        user_verification: Arc::new(crate::user_verification::Service::new(Arc::clone(
            &auth_manager,
        ))),
        auth_manager,
        installation_id: "11111111-1111-4111-8111-111111111111".to_string(),
        code_mode_session_provider: None,
        rpc_transport: AppServerRpcTransport::Stdio,
        remote_control_handle: None,
        // Keep this retirement fixture deterministic: startup plugin refresh
        // workers are exercised by their own ownership tests and would add an
        // unrelated admitted population to the background-drain assertion.
        plugin_startup_tasks: None,
        managed_transition_control_socket_endpoint: None,
        managed_transition_process_instance_id: None,
        control_endpoint: None,
    }));
    (processor, outgoing_rx, telemetry)
}

#[tokio::test]
async fn processor_login_shutdown_facade_binds_first_deadline() -> Result<()> {
    let home = TempDir::new()?;
    let config = Arc::new(build_test_config(home.path(), "http://127.0.0.1:1").await?);
    let (processor, _outgoing, _telemetry) =
        build_test_processor(config, TestTelemetry::Unavailable).await;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    drop(processor.begin_login_shutdown(deadline)?);
    let ticket = processor.begin_login_shutdown(deadline + std::time::Duration::from_secs(60))?;
    assert_eq!(ticket.deadline(), deadline);
    let report = ticket.wait().await;
    assert!(report.tasks.is_clean());
    assert_eq!(report.browsers, Ok(Vec::new()));
    assert!(!report.admission_unavailable);
    // Independently clean the fixture's other worker populations. The login
    // report is not a claim about them or a substitute for host retirement.
    processor.drain_background_tasks_until(deadline).await;
    Ok(())
}

#[tokio::test]
async fn processor_thread_shutdown_facade_retains_first_attempt() -> Result<()> {
    let home = TempDir::new()?;
    let config = Arc::new(build_test_config(home.path(), "http://127.0.0.1:1").await?);
    let (processor, _outgoing, _telemetry) =
        build_test_processor(config, TestTelemetry::Unavailable).await;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    drop(processor.begin_thread_shutdown(deadline).unwrap());
    let ticket = processor
        .begin_thread_shutdown(deadline + std::time::Duration::from_secs(20))
        .unwrap();
    assert_eq!(ticket.deadline(), deadline);
    let report = ticket.wait().await;
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(ticket.wait().await, report);
    // This proves only the thread limb. Independently stop fixture workers;
    // the thread receipt must not be mistaken for all-processor cleanup.
    processor.drain_background_tasks_until(deadline).await;
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::await_holding_invalid_type,
    reason = "the fixture exclusively observes worker shutdown under its owner lock before testing background custody"
)]
async fn background_drain_timeout_retains_work_and_reobserves_completion() -> Result<()> {
    use super::ProcessorBackgroundShutdown;
    use crate::models_refresh_worker::ModelsRefreshShutdown;
    use crate::processor_task_retirement::ProcessorTaskDrain as ThreadStartDrain;
    use crate::request_processors::AppsRuntimeDrain;
    use crate::request_processors::AppsShutdown;
    use tokio::time::Instant;

    let home = TempDir::new()?;
    let config = Arc::new(build_test_config(home.path(), "http://127.0.0.1:1").await?);
    let (processor, _outgoing, _telemetry) =
        build_test_processor(config, TestTelemetry::Unavailable).await;
    assert_eq!(
        processor
            .models_refresh_worker
            .lock()
            .await
            .shutdown_until(Instant::now() + std::time::Duration::from_secs(5))
            .await,
        ModelsRefreshShutdown::Joined
    );
    let resource = Arc::new(());
    let weak_resource = Arc::downgrade(&resource);
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let task = processor
        .thread_processor
        .background_tasks_for_test()
        .spawn(async move {
            let _resource = resource;
            entered_tx.send(()).unwrap();
            release_rx.await.unwrap();
        })
        .unwrap();
    entered_rx.await?;
    let mut observer = Box::pin(
        processor
            .drain_background_tasks_until(Instant::now() + std::time::Duration::from_millis(10)),
    );
    assert!(futures::poll!(observer.as_mut()).is_pending());
    drop(observer);
    assert_eq!(
        processor
            .drain_background_tasks_until(Instant::now() + std::time::Duration::from_millis(10))
            .await,
        ProcessorBackgroundShutdown {
            models: ModelsRefreshShutdown::Joined,
            thread_starts: ThreadStartDrain::default(),
            apps: AppsShutdown {
                tasks: ThreadStartDrain {
                    terminal: true,
                    ..Default::default()
                },
                runtimes: AppsRuntimeDrain {
                    unavailable: false,
                    reports: Vec::new()
                },
            },
            skills: crate::skills_watcher::SkillsWatcherShutdown::Joined,
            plugins: codex_core_plugins::PluginTaskDrain {
                terminal: true,
                ..Default::default()
            },
            auxiliary_tasks: ThreadStartDrain {
                terminal: true,
                ..Default::default()
            },
        }
    );
    assert!(weak_resource.upgrade().is_some());
    assert_eq!(
        processor.thread_processor.background_tasks_for_test().len(),
        1
    );
    release_tx.send(()).unwrap();
    // A later observer may normalize a completed join, but cannot extend the
    // expired attempt to wait for work. Settle the actual task first.
    let _ = task.await;
    let deadline = Instant::now() + std::time::Duration::from_secs(1);
    let (first, second) = tokio::join!(
        processor.drain_background_tasks_until(deadline),
        processor.drain_background_tasks_until(deadline),
    );
    let expected = ProcessorBackgroundShutdown {
        models: ModelsRefreshShutdown::Joined,
        thread_starts: ThreadStartDrain {
            terminal: true,
            ..Default::default()
        },
        apps: AppsShutdown {
            tasks: ThreadStartDrain {
                terminal: true,
                ..Default::default()
            },
            runtimes: AppsRuntimeDrain {
                unavailable: false,
                reports: Vec::new(),
            },
        },
        skills: crate::skills_watcher::SkillsWatcherShutdown::Joined,
        plugins: codex_core_plugins::PluginTaskDrain {
            terminal: true,
            ..Default::default()
        },
        auxiliary_tasks: ThreadStartDrain {
            terminal: true,
            ..Default::default()
        },
    };
    assert_eq!((first, second), (expected.clone(), expected));
    assert!(weak_resource.upgrade().is_none());
    processor.clear_runtime_references();
    processor.shutdown_threads().await;
    Ok(())
}

fn run_current_thread_test_with_stack<F>(name: &str, future: F) -> Result<()>
where
    F: Future<Output = Result<()>> + Send + 'static,
{
    const TEST_STACK_SIZE_BYTES: usize = 8 * 1024 * 1024;

    let handle = std::thread::Builder::new()
        .name(name.to_string())
        .stack_size(TEST_STACK_SIZE_BYTES)
        .spawn(move || -> Result<()> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(Box::pin(future))
        })?;

    match handle.join() {
        Ok(result) => result,
        Err(_) => Err(anyhow::anyhow!("{name} thread panicked")),
    }
}

fn span_attr<'a>(span: &'a SpanData, key: &str) -> Option<&'a str> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .and_then(|kv| match &kv.value {
            opentelemetry::Value::String(value) => Some(value.as_str()),
            _ => None,
        })
}

fn find_rpc_span_with_trace<'a>(
    spans: &'a [SpanData],
    kind: SpanKind,
    method: &str,
    trace_id: TraceId,
) -> &'a SpanData {
    spans
        .iter()
        .find(|span| {
            span.span_kind == kind
                && span_attr(span, "rpc.system") == Some("jsonrpc")
                && span_attr(span, "rpc.method") == Some(method)
                && span.span_context.trace_id() == trace_id
        })
        .unwrap_or_else(|| {
            panic!(
                "missing {kind:?} span for rpc.method={method} trace={trace_id}; exported spans:\n{}",
                format_spans(spans)
            )
        })
}

fn find_span_with_trace<'a, F>(
    spans: &'a [SpanData],
    trace_id: TraceId,
    description: &str,
    predicate: F,
) -> &'a SpanData
where
    F: Fn(&SpanData) -> bool,
{
    spans
        .iter()
        .find(|span| span.span_context.trace_id() == trace_id && predicate(span))
        .unwrap_or_else(|| {
            panic!(
                "missing span matching {description} for trace={trace_id}; exported spans:\n{}",
                format_spans(spans)
            )
        })
}

fn format_spans(spans: &[SpanData]) -> String {
    spans
        .iter()
        .map(|span| {
            let rpc_method = span_attr(span, "rpc.method").unwrap_or("-");
            format!(
                "name={} span_id={} kind={:?} parent={} trace={} rpc.method={}",
                span.name,
                span.span_context.span_id(),
                span.span_kind,
                span.parent_span_id,
                span.span_context.trace_id(),
                rpc_method
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn span_depth_from_ancestor(
    spans: &[SpanData],
    child: &SpanData,
    ancestor: &SpanData,
) -> Option<usize> {
    let ancestor_span_id = ancestor.span_context.span_id();
    let mut parent_span_id = child.parent_span_id;
    let mut depth = 1;
    while parent_span_id != SpanId::INVALID {
        if parent_span_id == ancestor_span_id {
            return Some(depth);
        }
        let Some(parent_span) = spans
            .iter()
            .find(|span| span.span_context.span_id() == parent_span_id)
        else {
            break;
        };
        parent_span_id = parent_span.parent_span_id;
        depth += 1;
    }

    None
}

fn assert_span_descends_from(spans: &[SpanData], child: &SpanData, ancestor: &SpanData) {
    if span_depth_from_ancestor(spans, child, ancestor).is_some() {
        return;
    }

    panic!(
        "span {} does not descend from {}; exported spans:\n{}",
        child.name,
        ancestor.name,
        format_spans(spans)
    );
}

fn assert_has_internal_descendant_at_min_depth(
    spans: &[SpanData],
    ancestor: &SpanData,
    min_depth: usize,
) {
    if spans.iter().any(|span| {
        span.span_kind == SpanKind::Internal
            && span.span_context.trace_id() == ancestor.span_context.trace_id()
            && span_depth_from_ancestor(spans, span, ancestor)
                .is_some_and(|depth| depth >= min_depth)
    }) {
        return;
    }

    panic!(
        "missing internal descendant at depth >= {min_depth} below {}; exported spans:\n{}",
        ancestor.name,
        format_spans(spans)
    );
}

async fn read_response<T: serde::de::DeserializeOwned>(
    outgoing_rx: &mut mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
    request_id: i64,
) -> T {
    loop {
        let envelope = tokio::time::timeout(std::time::Duration::from_secs(5), outgoing_rx.recv())
            .await
            .expect("timed out waiting for response")
            .expect("outgoing channel closed");
        let crate::outgoing_message::OutgoingEnvelope::ToConnection {
            connection_id,
            message,
            ..
        } = envelope
        else {
            continue;
        };
        if connection_id != TEST_CONNECTION_ID {
            continue;
        }
        let crate::outgoing_message::OutgoingMessage::Response(response) = message else {
            continue;
        };
        if response.id != RequestId::Integer(request_id) {
            continue;
        }
        return serde_json::from_value(
            serde_json::to_value(response.result).expect("response payload should serialize"),
        )
        .expect("response payload should deserialize");
    }
}

async fn read_thread_started_notification(
    outgoing_rx: &mut mpsc::Receiver<crate::outgoing_message::OutgoingEnvelope>,
) {
    loop {
        let envelope = tokio::time::timeout(std::time::Duration::from_secs(5), outgoing_rx.recv())
            .await
            .expect("timed out waiting for thread/started notification")
            .expect("outgoing channel closed");
        match envelope {
            crate::outgoing_message::OutgoingEnvelope::ToConnection {
                connection_id,
                message,
                ..
            } => {
                if connection_id != TEST_CONNECTION_ID {
                    continue;
                }
                let crate::outgoing_message::OutgoingMessage::AppServerNotification(notification) =
                    message
                else {
                    continue;
                };
                if matches!(
                    notification.notification,
                    codex_app_server_protocol::ServerNotification::ThreadStarted(_)
                ) {
                    return;
                }
            }
            crate::outgoing_message::OutgoingEnvelope::Broadcast { message } => {
                let crate::outgoing_message::OutgoingMessage::AppServerNotification(notification) =
                    message
                else {
                    continue;
                };
                if matches!(
                    notification.notification,
                    codex_app_server_protocol::ServerNotification::ThreadStarted(_)
                ) {
                    return;
                }
            }
        }
    }
}

async fn wait_for_exported_spans<F>(tracing: &TestTracing, predicate: F) -> Vec<SpanData>
where
    F: Fn(&[SpanData]) -> bool,
{
    let mut last_spans = Vec::new();
    for _ in 0..200 {
        tokio::task::yield_now().await;
        tracing
            .provider
            .force_flush()
            .expect("force flush should succeed");
        let spans = tracing.exporter.get_finished_spans().expect("span export");
        last_spans = spans.clone();
        if predicate(&spans) {
            return spans;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    panic!(
        "timed out waiting for expected exported spans:\n{}",
        format_spans(&last_spans)
    );
}

async fn wait_for_new_exported_spans<F>(
    tracing: &TestTracing,
    baseline_len: usize,
    predicate: F,
) -> Vec<SpanData>
where
    F: Fn(&[SpanData]) -> bool,
{
    let spans = wait_for_exported_spans(tracing, |spans| {
        spans.len() > baseline_len && predicate(&spans[baseline_len..])
    })
    .await;
    spans.into_iter().skip(baseline_len).collect()
}

#[test]
#[serial(app_server_tracing)]
fn thread_start_jsonrpc_span_exports_server_span_and_parents_children() -> Result<()> {
    run_current_thread_test_with_stack(
        "thread_start_jsonrpc_span_exports_server_span_and_parents_children",
        async {
            let mut harness = TracingHarness::new().await?;

            let RemoteTrace {
                trace_id: remote_trace_id,
                parent_span_id: remote_parent_span_id,
                context: remote_trace,
                ..
            } = RemoteTrace::new("00000000000000000000000000000011", "0000000000000022");

            let _: ThreadStartResponse = harness
                .start_thread(/*request_id*/ 20_002, /*trace*/ None)
                .await;
            let untraced_spans = wait_for_exported_spans(harness.tracing, |spans| {
                spans.iter().any(|span| {
                    span.span_kind == SpanKind::Server
                        && span_attr(span, "rpc.method") == Some("thread/start")
                })
            })
            .await;
            let untraced_server_span = find_rpc_span_with_trace(
                &untraced_spans,
                SpanKind::Server,
                "thread/start",
                untraced_spans
                    .iter()
                    .rev()
                    .find(|span| {
                        span.span_kind == SpanKind::Server
                            && span_attr(span, "rpc.system") == Some("jsonrpc")
                            && span_attr(span, "rpc.method") == Some("thread/start")
                    })
                    .unwrap_or_else(|| {
                        panic!(
                            "missing latest thread/start server span; exported spans:\n{}",
                            format_spans(&untraced_spans)
                        )
                    })
                    .span_context
                    .trace_id(),
            );
            assert_has_internal_descendant_at_min_depth(
                &untraced_spans,
                untraced_server_span,
                /*min_depth*/ 1,
            );

            let baseline_len = untraced_spans.len();
            let _: ThreadStartResponse = harness
                .start_thread(/*request_id*/ 20_003, Some(remote_trace))
                .await;
            let spans = wait_for_new_exported_spans(harness.tracing, baseline_len, |spans| {
                spans.iter().any(|span| {
                    span.span_kind == SpanKind::Server
                        && span_attr(span, "rpc.method") == Some("thread/start")
                        && span.span_context.trace_id() == remote_trace_id
                }) && spans.iter().any(|span| {
                    span.name.as_ref() == "app_server.thread_start.notify_started"
                        && span.span_context.trace_id() == remote_trace_id
                })
            })
            .await;

            let server_request_span =
                find_rpc_span_with_trace(&spans, SpanKind::Server, "thread/start", remote_trace_id);
            assert_eq!(server_request_span.name.as_ref(), "thread/start");
            assert_eq!(server_request_span.parent_span_id, remote_parent_span_id);
            assert!(server_request_span.parent_span_is_remote);
            assert_eq!(server_request_span.span_context.trace_id(), remote_trace_id);
            assert_ne!(server_request_span.span_context.span_id(), SpanId::INVALID);
            assert_has_internal_descendant_at_min_depth(
                &spans,
                server_request_span,
                /*min_depth*/ 1,
            );
            assert_has_internal_descendant_at_min_depth(
                &spans,
                server_request_span,
                /*min_depth*/ 2,
            );
            harness.shutdown().await;

            Ok(())
        },
    )
}

#[tokio::test(flavor = "current_thread")]
#[serial(app_server_tracing)]
async fn initialized_unproven_connection_has_no_retention_principal() -> Result<()> {
    let harness = TracingHarness::new().await?;

    assert!(harness.session.initialized());
    assert_eq!(
        harness.session.retention_principal(),
        None,
        "client initialization and experimental opt-in cannot establish retention eligibility"
    );

    harness.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
#[serial(app_server_tracing)]
async fn turn_start_jsonrpc_span_parents_core_turn_spans() -> Result<()> {
    let mut harness = TracingHarness::new().await?;
    let thread_start_response = harness.start_thread(/*request_id*/ 2, /*trace*/ None).await;
    let thread_id = thread_start_response.thread.id.clone();

    harness.reset_tracing();

    let RemoteTrace {
        trace_id: remote_trace_id,
        parent_span_id: remote_parent_span_id,
        context: remote_trace,
    } = RemoteTrace::new("00000000000000000000000000000077", "0000000000000088");
    let turn_start_response: TurnStartResponse = harness
        .request(
            ClientRequest::TurnStart {
                request_id: RequestId::Integer(3),
                params: TurnStartParams {
                    disabled_plugin_ids: None,
                    environments: None,
                    thread_id,
                    client_user_message_id: None,
                    input: vec![UserInput::Text {
                        text: "hello".to_string(),
                        text_elements: Vec::new(),
                    }],
                    turn_trigger: None,
                    tool_output: None,
                    responsesapi_client_metadata: None,
                    additional_context: None,
                    cwd: None,
                    runtime_workspace_roots: None,
                    approval_policy: None,
                    sandbox_policy: None,
                    permissions: None,
                    approvals_reviewer: None,
                    model: None,
                    service_tier: None,
                    service_tier_for_turn: None,
                    effort: None,
                    summary: None,
                    personality: None,
                    output_schema: None,
                    collaboration_mode: None,
                    multi_agent_mode: None,
                    cyber_access_program: None,
                },
            },
            Some(remote_trace),
        )
        .await;
    let spans = wait_for_exported_spans(harness.tracing, |spans| {
        spans.iter().any(|span| {
            span.span_kind == SpanKind::Server
                && span_attr(span, "rpc.method") == Some("turn/start")
                && span.span_context.trace_id() == remote_trace_id
        }) && spans.iter().any(|span| {
            span_attr(span, "codex.op") == Some("turn_input")
                && span.span_context.trace_id() == remote_trace_id
        })
    })
    .await;

    let server_request_span =
        find_rpc_span_with_trace(&spans, SpanKind::Server, "turn/start", remote_trace_id);
    let core_turn_span =
        find_span_with_trace(&spans, remote_trace_id, "codex.op=turn_input", |span| {
            span_attr(span, "codex.op") == Some("turn_input")
        });

    assert_eq!(server_request_span.parent_span_id, remote_parent_span_id);
    assert!(server_request_span.parent_span_is_remote);
    assert_eq!(server_request_span.span_context.trace_id(), remote_trace_id);
    assert_eq!(
        span_attr(server_request_span, "turn.id"),
        Some(turn_start_response.turn.id.as_str())
    );
    assert_span_descends_from(&spans, core_turn_span, server_request_span);
    harness.shutdown().await;

    Ok(())
}

#[test]
#[serial(app_server_tracing)]
fn managed_transition_dispatch_paths_refuse_before_authorization_without_reserving_state()
-> Result<()> {
    run_current_thread_test_with_stack(
        "managed_transition_dispatch_paths_refuse_before_authorization_without_reserving_state",
        async {
            let codex_home = TempDir::new()?;
            app_test_support::write_chatgpt_auth(
                codex_home.path(),
                app_test_support::ChatGptAuthFixture::new("prior-access-token")
                    .account_id("authorization-test-account"),
                codex_config::types::AuthCredentialsStoreMode::File,
            )?;
            let mut harness = TracingHarness::with_codex_home(codex_home).await?;
            let process_instance_id = harness
                .processor
                .managed_transition_coordinator
                .process_instance_id()
                .await;
            let start_params = StartManagedTransitionParams {
                contract_version: MANAGED_AUTH_TRANSITION_CONTRACT_VERSION,
                transition_id: "transition-a".to_owned(),
                process_instance_id: process_instance_id.clone(),
                intent: ManagedTransitionIntent::AdoptManagedAuth,
                expected_auth_revision: 0,
                expected_transition_revision: 0,
                expected_auth_fingerprint: Some(
                    codex_login::AuthManager::managed_account_fingerprint(
                        "authorization-test-account",
                    ),
                ),
                intended_result_auth_fingerprint: Some(
                    codex_login::AuthManager::managed_account_fingerprint(
                        "authorization-test-account",
                    ),
                ),
            };
            let start: StartManagedTransitionResponse = harness
                .request(
                    ClientRequest::ManagedTransitionStart {
                        request_id: RequestId::Integer(40_001),
                        params: start_params.clone(),
                    },
                    None,
                )
                .await;
            let read: ReadManagedTransitionResponse = harness
                .request(
                    ClientRequest::ManagedTransitionRead {
                        request_id: RequestId::Integer(40_002),
                        params: ReadManagedTransitionParams {
                            contract_version: MANAGED_AUTH_TRANSITION_CONTRACT_VERSION,
                            transition_id: start_params.transition_id.clone(),
                            process_instance_id: process_instance_id.clone(),
                        },
                    },
                    None,
                )
                .await;
            let cancel: CancelManagedTransitionResponse = harness
                .request(
                    ClientRequest::ManagedTransitionCancel {
                        request_id: RequestId::Integer(40_003),
                        params: CancelManagedTransitionParams {
                            contract_version: MANAGED_AUTH_TRANSITION_CONTRACT_VERSION,
                            transition_id: start_params.transition_id.clone(),
                            process_instance_id,
                        },
                    },
                    None,
                )
                .await;
            for refusal in [
                match start {
                    StartManagedTransitionResponse::Refused { refusal } => refusal,
                    StartManagedTransitionResponse::Accepted { .. } => panic!("start must refuse"),
                },
                match read {
                    ReadManagedTransitionResponse::Refused { refusal } => refusal,
                    ReadManagedTransitionResponse::Accepted { .. } => panic!("read must refuse"),
                },
                match cancel {
                    CancelManagedTransitionResponse::Refused { refusal } => refusal,
                    CancelManagedTransitionResponse::Accepted { .. } => {
                        panic!("cancel must refuse")
                    }
                },
            ] {
                assert_eq!(
                    refusal.kind,
                    ManagedTransitionRefusalKind::AuthorizationNotAdmitted
                );
            }
            assert!(
                harness
                    .processor
                    .managed_transition_coordinator
                    .admit(start_params)
                    .await
                    .is_ok(),
                "the three real MessageProcessor paths must not reserve the transition"
            );
            harness.shutdown().await;
            Ok(())
        },
    )
}

/// End-to-end proof that a real managed-auth adoption, driven through the
/// actual production-wired `MessageProcessor` (not a synthetic
/// `ResetInventory` double), completes the composed reset for the new account.
/// Upstream's PluginsManager reads the shared AuthManager directly instead of
/// retaining a separate auth-mode snapshot. The cloud request checks the new
/// account identity, while the terminal status checks the transition result.
/// Wire authorization is
/// bypassed the same way the sibling test above does, via
/// `managed_transition_coordinator.start_dispatch` directly -- that gate
/// is a separate, already-covered concern
/// (`managed_transition_dispatch_paths_refuse_before_authorization_without_reserving_state`).
#[test]
#[serial(app_server_tracing)]
fn managed_transition_adoption_completes_reset_with_shared_plugin_auth() -> Result<()> {
    run_current_thread_test_with_stack(
        "managed_transition_adoption_completes_reset_with_shared_plugin_auth",
        async {
            let codex_home = TempDir::new()?;
            app_test_support::write_chatgpt_auth(
                codex_home.path(),
                app_test_support::ChatGptAuthFixture::new("prior-access-token")
                    .account_id("prior-managed-account"),
                codex_config::types::AuthCredentialsStoreMode::File,
            )?;
            let harness =
                TracingHarness::with_telemetry(codex_home, TestTelemetry::Managed).await?;
            wiremock::Mock::given(wiremock::matchers::method("GET"))
                .and(wiremock::matchers::path("/backend-api/wham/config/bundle"))
                .and(wiremock::matchers::header(
                    "authorization",
                    "Bearer access-token",
                ))
                .and(wiremock::matchers::header(
                    "chatgpt-account-id",
                    "managed-adoption-account",
                ))
                .respond_with(
                    wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({})),
                )
                .expect(1)
                .mount(&harness._server)
                .await;
            wiremock::Mock::given(wiremock::matchers::method("GET"))
                .and(wiremock::matchers::path("/v1/models"))
                .respond_with(
                    wiremock::ResponseTemplate::new(200)
                        .set_body_json(serde_json::json!({"models": []})),
                )
                .mount(&harness._server)
                .await;
            assert_eq!(
                harness
                    .processor
                    .thread_manager_for_tests()
                    .plugins_manager()
                    .auth_mode(),
                Some(codex_protocol::auth::AuthMode::Chatgpt),
                "plugins must read the initially installed account through the shared manager"
            );

            let process_instance_id = harness
                .processor
                .managed_transition_coordinator
                .process_instance_id()
                .await;
            let start_params = StartManagedTransitionParams {
                contract_version: MANAGED_AUTH_TRANSITION_CONTRACT_VERSION,
                transition_id: "adopt-and-reset-plugins-auth-mode".to_owned(),
                process_instance_id,
                intent: ManagedTransitionIntent::AdoptManagedAuth,
                expected_auth_revision: 0,
                expected_transition_revision: 0,
                expected_auth_fingerprint: Some(
                    codex_login::AuthManager::managed_account_fingerprint("prior-managed-account"),
                ),
                intended_result_auth_fingerprint: Some(
                    codex_login::AuthManager::managed_account_fingerprint(
                        "managed-adoption-account",
                    ),
                ),
            };

            // A real managed account lands on disk before Adopting reads
            // it -- the same fixture builder the rest of this app-server
            // test suite already relies on for genuine ChatGPT auth.
            app_test_support::write_chatgpt_auth(
                harness._codex_home.path(),
                app_test_support::ChatGptAuthFixture::new("access-token")
                    .account_id("managed-adoption-account")
                    .plan_type("enterprise"),
                codex_config::types::AuthCredentialsStoreMode::File,
            )
            .expect("write real chatgpt auth.json for adoption");

            let status = harness
                .processor
                .managed_transition_coordinator
                .start_dispatch(start_params, /*caller_authorized*/ true)
                .await;
            let StartManagedTransitionResponse::Accepted { status } = status else {
                panic!("expected the real production coordinator to accept and complete adoption");
            };
            assert_eq!(
                status.phase,
                codex_app_server_protocol::ManagedTransitionPhase::Succeeded
            );
            assert_eq!(
                (
                    status.prior_auth_fingerprint,
                    status.result_auth_fingerprint
                ),
                (
                    Some(AuthManager::managed_account_fingerprint(
                        "prior-managed-account"
                    )),
                    Some(AuthManager::managed_account_fingerprint(
                        "managed-adoption-account"
                    )),
                )
            );

            assert_eq!(
                harness
                    .processor
                    .thread_manager_for_tests()
                    .plugins_manager()
                    .auth_mode(),
                Some(codex_protocol::auth::AuthMode::Chatgpt),
                "plugins must still read the adopted account's mode from the shared manager"
            );

            harness.shutdown().await;
            Ok(())
        },
    )
}
