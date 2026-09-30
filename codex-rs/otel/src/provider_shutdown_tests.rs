use super::Context;
use super::OTelSdkResult;
use super::OtelProvider;
use super::SdkTracerProvider;
use super::Span;
use super::SpanData;
use super::SpanProcessor;
use pretty_assertions::assert_eq;
use std::io::ErrorKind;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::process::Command;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
#[cfg(target_os = "macos")]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;

#[cfg(any(target_os = "linux", target_os = "macos"))]
const GUARD_PAGE_FAILURE_CHILD_TEST: &str =
    "provider::shutdown_tests::bounded_shutdown_survives_worker_guard_page_failure_child";

#[cfg(target_os = "macos")]
static GUARD_PAGE_INJECTION_ENABLED: AtomicBool = AtomicBool::new(false);
#[cfg(target_os = "macos")]
static GUARD_PAGE_INJECTION_ARMED: AtomicBool = AtomicBool::new(false);
#[cfg(target_os = "macos")]
static GUARD_PAGE_INJECTION_OBSERVED: AtomicBool = AtomicBool::new(false);

#[cfg(target_os = "macos")]
#[unsafe(export_name = "mprotect")]
unsafe extern "C" fn fault_injected_mprotect(
    address: *mut libc::c_void,
    length: usize,
    protection: libc::c_int,
) -> libc::c_int {
    let original_symbol = unsafe { libc::dlsym(libc::RTLD_NEXT, c"mprotect".as_ptr()) };
    let original_mprotect: unsafe extern "C" fn(
        *mut libc::c_void,
        usize,
        libc::c_int,
    ) -> libc::c_int = unsafe { std::mem::transmute(original_symbol) };

    if GUARD_PAGE_INJECTION_ENABLED.load(Ordering::Relaxed)
        && protection == libc::PROT_NONE
        && length == unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize
    {
        let mut thread_name = [0; 64];
        let named_shutdown_worker = unsafe {
            libc::pthread_getname_np(
                libc::pthread_self(),
                thread_name.as_mut_ptr(),
                thread_name.len(),
            )
        } == 0
            && unsafe { std::ffi::CStr::from_ptr(thread_name.as_ptr()) }
                .to_bytes()
                .starts_with(b"codex-otel-shut");

        if named_shutdown_worker {
            GUARD_PAGE_INJECTION_OBSERVED.store(/*val*/ true, Ordering::Relaxed);
            if GUARD_PAGE_INJECTION_ARMED.load(Ordering::Relaxed) {
                unsafe { *libc::__error() = libc::ENOMEM };
                return -1;
            }
        }
    }

    unsafe { original_mprotect(address, length, protection) }
}

#[test]
fn disabled_provider_removes_previous_global_tracer() {
    use opentelemetry::trace::Span as _;
    use opentelemetry::trace::Tracer as _;
    let previous = SdkTracerProvider::builder().build();
    opentelemetry::global::set_tracer_provider(previous.clone());
    let before = opentelemetry::global::tracer("before-disable").start("before");
    assert!(before.span_context().is_valid());
    let settings = crate::OtelSettings {
        http_client_factory: codex_http_client::HttpClientFactory::new(
            codex_http_client::OutboundProxyPolicy::ReqwestDefault,
        ),
        environment: "disabled".to_owned(),
        service_name: "disabled".to_owned(),
        service_version: "1".to_owned(),
        codex_home: std::path::PathBuf::from("."),
        exporter: crate::OtelExporter::None,
        trace_exporter: crate::OtelExporter::None,
        metrics_exporter: crate::OtelExporter::None,
        runtime_metrics: false,
        span_attributes: Default::default(),
        tracestate: Default::default(),
    };
    assert!(OtelProvider::try_new(&settings).unwrap().is_none());
    let after = opentelemetry::global::tracer("after-disable").start("after");
    assert!(!after.span_context().is_valid());
    drop(before);
    previous.shutdown().unwrap();
}

#[tokio::test]
async fn unpublished_provider_construction_preserves_global_metrics_and_settings() {
    use opentelemetry::trace::Span as _;
    use opentelemetry::trace::Tracer as _;
    let trace_exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let trace_provider = SdkTracerProvider::builder()
        .with_simple_exporter(trace_exporter.clone())
        .build();
    opentelemetry::global::set_tracer_provider(trace_provider.clone());
    let exporter = opentelemetry_sdk::metrics::InMemoryMetricExporter::default();
    let original_settings = crate::StatsigMetricsSettings {
        environment: "published-a".to_owned(),
    };
    let current = crate::metrics::install_global_with_settings(
        crate::MetricsClient::new(crate::MetricsConfig::in_memory(
            "test",
            "published-a",
            "1",
            exporter.clone(),
        ))
        .unwrap(),
        Some(original_settings.clone()),
    )
    .unwrap();
    let mut settings = crate::OtelSettings {
        http_client_factory: codex_http_client::HttpClientFactory::new(
            codex_http_client::OutboundProxyPolicy::ReqwestDefault,
        ),
        environment: "candidate-b".to_owned(),
        service_name: "candidate-b".to_owned(),
        service_version: "1".to_owned(),
        codex_home: std::path::PathBuf::from("."),
        exporter: crate::OtelExporter::None,
        trace_exporter: crate::OtelExporter::None,
        metrics_exporter: crate::OtelExporter::None,
        runtime_metrics: false,
        span_attributes: Default::default(),
        tracestate: Default::default(),
    };
    assert!(
        OtelProvider::prepare(&settings)
            .unwrap()
            .begin_retirement()
            .is_none()
    );
    crate::metrics::global()
        .unwrap()
        .counter("after.disabled.candidate", 1, &[])
        .unwrap();
    opentelemetry::global::tracer("published-a")
        .start("after_disabled_candidate")
        .end();
    settings.metrics_exporter = crate::OtelExporter::OtlpHttp {
        endpoint: "http://127.0.0.1:9/v1/metrics".to_owned(),
        headers: Default::default(),
        protocol: crate::OtelHttpProtocol::Binary,
        tls: None,
    };
    let candidate = OtelProvider::prepare(&settings).unwrap();
    crate::metrics::global()
        .unwrap()
        .counter("after.enabled.candidate", 1, &[])
        .unwrap();
    opentelemetry::global::tracer("published-a")
        .start("after_enabled_candidate")
        .end();
    let retirement = candidate.begin_retirement().unwrap();
    retirement
        .wait_until(tokio::time::Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    retirement
        .wait_until(tokio::time::Instant::now())
        .await
        .unwrap();
    assert_eq!(
        crate::metrics::global_statsig_settings(),
        Some(original_settings)
    );
    current.shutdown().unwrap();
    let exported = exporter.get_finished_metrics().unwrap();
    let mut names: Vec<_> = exported
        .iter()
        .flat_map(opentelemetry_sdk::metrics::data::ResourceMetrics::scope_metrics)
        .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
        .map(|metric| metric.name().to_owned())
        .collect();
    names.sort();
    names.dedup();
    assert_eq!(
        names,
        vec!["after.disabled.candidate", "after.enabled.candidate"]
    );
    assert_eq!(
        trace_exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .map(|span| span.name.to_string())
            .collect::<Vec<_>>(),
        vec!["after_disabled_candidate", "after_enabled_candidate"]
    );
    trace_provider.shutdown().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partial_provider_failure_retains_log_and_metrics_until_retirement() {
    let home =
        std::env::temp_dir().join(format!("codex-otel-missing-ca-test-{}", std::process::id()));
    assert!(!home.exists(), "the failure fixture must remain absent");
    let exporter = crate::OtelExporter::OtlpHttp {
        endpoint: "http://127.0.0.1:9/unused".to_owned(),
        headers: Default::default(),
        protocol: crate::OtelHttpProtocol::Json,
        tls: None,
    };
    let settings = crate::OtelSettings {
        http_client_factory: codex_http_client::HttpClientFactory::new(
            codex_http_client::OutboundProxyPolicy::ReqwestDefault,
        ),
        environment: "test".to_owned(),
        service_name: "partial-provider".to_owned(),
        service_version: "1".to_owned(),
        codex_home: home.clone(),
        exporter: exporter.clone(),
        metrics_exporter: exporter,
        trace_exporter: crate::OtelExporter::OtlpHttp {
            endpoint: "https://127.0.0.1:9/unused".to_owned(),
            headers: Default::default(),
            protocol: crate::OtelHttpProtocol::Json,
            tls: Some(crate::OtelTlsConfig {
                ca_certificate: Some(
                    codex_utils_absolute_path::AbsolutePathBuf::try_from(
                        home.join("private-missing-ca.pem"),
                    )
                    .unwrap(),
                ),
                ..Default::default()
            }),
        },
        runtime_metrics: false,
        span_attributes: Default::default(),
        tracestate: Default::default(),
    };
    let error = match OtelProvider::prepare(&settings) {
        Ok(_) => panic!("missing CA must fail the final constructor"),
        Err(error) => error,
    };
    assert_eq!(error.to_string(), "telemetry provider preparation failed");
    assert_eq!(format!("{error:?}"), "OtelPreparationError");
    let partial = error.provider.as_ref().unwrap();
    assert!(partial.shutdown_worker.is_some());
    assert!(partial.metrics.is_some());
    assert!(partial.logger.is_some());
    assert!(partial.tracer_provider.is_none());
    let receipt = partial.log_receipt.clone().unwrap();
    assert_eq!(
        receipt.wait_until(tokio::time::Instant::now()).await,
        Err(crate::trace_exporter_retirement::ExporterRetirementError::TimedOut)
    );
    let retirement = error.begin_retirement().unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    assert_eq!(retirement.wait_until(deadline).await, Ok(()));
    assert_eq!(receipt.wait_until(deadline).await, Ok(()));
    assert_eq!(
        retirement.wait_until(tokio::time::Instant::now()).await,
        Ok(())
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ShutdownBehavior {
    Complete,
    WaitForRelease,
    Fail,
    Panic,
}

#[derive(Debug, Default)]
struct ShutdownState {
    ended: AtomicUsize,
    force_flushes: AtomicUsize,
    shutdowns: AtomicUsize,
    processors_dropped: AtomicUsize,
    released: Mutex<bool>,
    release_notification: Condvar,
    started: Mutex<Option<mpsc::Sender<()>>>,
    completed: Mutex<Option<mpsc::Sender<()>>>,
}

#[derive(Debug)]
struct ControlledSpanProcessor {
    behavior: ShutdownBehavior,
    state: Arc<ShutdownState>,
}

impl Drop for ControlledSpanProcessor {
    fn drop(&mut self) {
        self.state
            .processors_dropped
            .fetch_add(1, Ordering::Relaxed);
    }
}

impl SpanProcessor for ControlledSpanProcessor {
    fn on_start(&self, _span: &mut Span, _context: &Context) {}

    fn on_end(&self, _span: SpanData) {
        self.state.ended.fetch_add(1, Ordering::Relaxed);
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.state
            .force_flushes
            .fetch_add(/*val*/ 1, Ordering::Relaxed);
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        self.state.shutdowns.fetch_add(/*val*/ 1, Ordering::Relaxed);
        if self.behavior == ShutdownBehavior::Panic {
            panic!("synthetic exporter panic");
        }
        if let Some(started) = self.state.started.lock().expect("started lock").take() {
            let _ = started.send(());
        }

        if self.behavior == ShutdownBehavior::WaitForRelease {
            drop(
                self.state
                    .release_notification
                    .wait_while(
                        self.state.released.lock().expect("release lock"),
                        |released| !*released,
                    )
                    .expect("release notification"),
            );
        }

        if let Some(completed) = self.state.completed.lock().expect("completed lock").take() {
            let _ = completed.send(());
        }
        if self.behavior == ShutdownBehavior::Fail {
            return Err(opentelemetry_sdk::error::OTelSdkError::InternalFailure(
                "synthetic shutdown failure".to_owned(),
            ));
        }
        Ok(())
    }
}

struct TestProvider {
    provider: OtelProvider,
    state: Arc<ShutdownState>,
    started: mpsc::Receiver<()>,
    completed: mpsc::Receiver<()>,
}

#[derive(Debug)]
struct FailingBatchExporter(Arc<ShutdownState>);

#[derive(Debug)]
struct GatedTraceExporter(Arc<ShutdownState>);

impl opentelemetry_sdk::trace::SpanExporter for GatedTraceExporter {
    async fn export(&self, _batch: Vec<SpanData>) -> OTelSdkResult {
        Ok(())
    }
    fn shutdown(&mut self) -> OTelSdkResult {
        self.0.shutdowns.fetch_add(1, Ordering::Relaxed);
        if let Some(started) = self.0.started.lock().unwrap().take() {
            let _ = started.send(());
        }
        drop(
            self.0
                .release_notification
                .wait_while(self.0.released.lock().unwrap(), |released| !*released)
                .unwrap(),
        );
        Ok(())
    }
}

impl Drop for GatedTraceExporter {
    fn drop(&mut self) {
        self.0.processors_dropped.fetch_add(1, Ordering::Relaxed);
    }
}

struct ReleaseExporterOnDrop(Arc<ShutdownState>);
impl Drop for ReleaseExporterOnDrop {
    fn drop(&mut self) {
        *self.0.released.lock().unwrap() = true;
        self.0.release_notification.notify_all();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_batch_early_response_cannot_complete_retirement_before_exporter_drop() {
    let (started_tx, started_rx) = mpsc::channel();
    let state = Arc::new(ShutdownState {
        started: Mutex::new(Some(started_tx)),
        ..Default::default()
    });
    let release = ReleaseExporterOnDrop(Arc::clone(&state));
    let (exporter, receipt) = crate::trace_exporter_retirement::AcknowledgedExporter::new(
        GatedTraceExporter(Arc::clone(&state)),
    );
    let processor =
        opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor::builder(
            exporter,
            opentelemetry_sdk::runtime::Tokio,
        )
        .build();
    let provider = OtelProvider {
        logger: None,
        tracer_provider: Some(
            SdkTracerProvider::builder()
                .with_span_processor(processor)
                .build(),
        ),
        tracer: None,
        metrics: None,
        trace_receipt: Some(receipt),
        log_receipt: None,
        shutdown_result: Mutex::new(None),
        shutdown_worker: None,
    };
    let mut provider = provider;
    provider
        .prepare_shutdown_worker()
        .expect("prepare fixture worker");
    let retirement = provider.begin_retirement();
    tokio::task::spawn_blocking(move || started_rx.recv_timeout(Duration::from_secs(2)))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        retirement
            .wait_until(tokio::time::Instant::now() + Duration::from_millis(20))
            .await,
        Err(crate::OtelRetirementError::TimedOut)
    );
    assert_eq!(state.processors_dropped.load(Ordering::Relaxed), 0);
    drop(release);
    assert_eq!(
        retirement
            .wait_until(tokio::time::Instant::now() + Duration::from_secs(2))
            .await,
        Ok(())
    );
    assert_eq!(
        retirement.wait_until(tokio::time::Instant::now()).await,
        Ok(())
    );
    assert_eq!(state.shutdowns.load(Ordering::Relaxed), 1);
    assert_eq!(state.processors_dropped.load(Ordering::Relaxed), 1);
}

impl Drop for FailingBatchExporter {
    fn drop(&mut self) {
        self.0.processors_dropped.fetch_add(1, Ordering::Relaxed);
    }
}

impl opentelemetry_sdk::trace::SpanExporter for FailingBatchExporter {
    async fn export(&self, _batch: Vec<SpanData>) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&mut self, _timeout: Duration) -> OTelSdkResult {
        self.0.shutdowns.fetch_add(1, Ordering::Relaxed);
        Err(opentelemetry_sdk::error::OTelSdkError::InternalFailure(
            "synthetic exporter refusal".to_owned(),
        ))
    }
}

impl opentelemetry_sdk::logs::LogExporter for FailingBatchExporter {
    async fn export(&self, _batch: opentelemetry_sdk::logs::LogBatch<'_>) -> OTelSdkResult {
        Ok(())
    }
    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        self.0.shutdowns.fetch_add(1, Ordering::Relaxed);
        Err(opentelemetry_sdk::error::OTelSdkError::InternalFailure(
            "synthetic log shutdown refusal".to_owned(),
        ))
    }
}

#[tokio::test]
async fn retained_provider_rejects_batch_hidden_log_failure() {
    let state = Arc::new(ShutdownState::default());
    let (exporter, receipt) = crate::trace_exporter_retirement::AcknowledgedExporter::new(
        FailingBatchExporter(Arc::clone(&state)),
    );
    let provider = OtelProvider {
        logger: Some(
            opentelemetry_sdk::logs::SdkLoggerProvider::builder()
                .with_batch_exporter(exporter)
                .build(),
        ),
        tracer_provider: None,
        tracer: None,
        metrics: None,
        trace_receipt: None,
        log_receipt: Some(receipt),
        shutdown_result: Mutex::new(None),
        shutdown_worker: None,
    };
    let mut provider = provider;
    provider
        .prepare_shutdown_worker()
        .expect("prepare fixture worker");
    let retirement = provider.begin_retirement();
    let failure = Err(crate::OtelRetirementError::Exporter(
        crate::OtelShutdownError::Logs,
    ));
    assert_eq!(
        retirement
            .wait_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await,
        failure
    );
    assert_eq!(
        retirement.wait_until(tokio::time::Instant::now()).await,
        failure
    );
    assert_eq!(state.shutdowns.load(Ordering::Relaxed), 1);
    assert_eq!(state.processors_dropped.load(Ordering::Relaxed), 1);
}

#[test]
fn real_batch_processor_join_drops_exporter_but_hides_shutdown_failure() {
    use opentelemetry::trace::Tracer as _;
    use opentelemetry::trace::TracerProvider as _;
    let state = Arc::new(ShutdownState::default());
    let processor = opentelemetry_sdk::trace::BatchSpanProcessor::builder(FailingBatchExporter(
        Arc::clone(&state),
    ))
    .build();
    let provider = SdkTracerProvider::builder()
        .with_span_processor(processor)
        .build();
    let retained = provider.tracer("batch-custody").start("held-span");
    // This pins the dependency's result-erasure behavior, not our acceptance
    // contract: managed retirement must additionally observe the exporter.
    provider.shutdown().unwrap();
    assert_eq!(state.shutdowns.load(Ordering::Relaxed), 1);
    assert_eq!(state.processors_dropped.load(Ordering::Relaxed), 1);
    drop(retained);
}

#[tokio::test]
async fn retained_provider_rejects_batch_hidden_exporter_failure() {
    let state = Arc::new(ShutdownState::default());
    let (exporter, receipt) = crate::trace_exporter_retirement::AcknowledgedExporter::new(
        FailingBatchExporter(Arc::clone(&state)),
    );
    let processor = opentelemetry_sdk::trace::BatchSpanProcessor::builder(exporter).build();
    let provider = OtelProvider {
        log_receipt: None,
        logger: None,
        tracer_provider: Some(
            SdkTracerProvider::builder()
                .with_span_processor(processor)
                .build(),
        ),
        tracer: None,
        metrics: None,
        trace_receipt: Some(receipt),
        shutdown_result: Mutex::new(None),
        shutdown_worker: None,
    };
    let mut provider = provider;
    provider
        .prepare_shutdown_worker()
        .expect("prepare fixture worker");
    let retirement = provider.begin_retirement();
    let failure = Err(crate::OtelRetirementError::Exporter(
        crate::OtelShutdownError::Traces,
    ));
    assert_eq!(
        retirement
            .wait_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await,
        failure
    );
    assert_eq!(
        retirement.wait_until(tokio::time::Instant::now()).await,
        failure
    );
    assert_eq!(state.shutdowns.load(Ordering::Relaxed), 1);
    assert_eq!(state.processors_dropped.load(Ordering::Relaxed), 1);
}

#[test]
fn sdk_shutdown_stops_late_export_but_span_still_retains_processor() {
    use opentelemetry::trace::Tracer as _;
    use opentelemetry::trace::TracerProvider as _;
    let TestProvider {
        provider, state, ..
    } = test_provider(ShutdownBehavior::Complete);
    let tracer = provider
        .tracer_provider
        .as_ref()
        .unwrap()
        .tracer("retained-span");
    drop(tracer.start("positive-control"));
    assert_eq!(state.ended.load(Ordering::Relaxed), 1);
    let retained = tracer.start("held-through-shutdown");
    drop(tracer);
    provider.shutdown_checked().unwrap();
    drop(provider);
    assert_eq!(state.shutdowns.load(Ordering::Relaxed), 1);
    // An SDK shutdown receipt is not proof that credential-bearing processor
    // custody has been released. This is the upstream contract, not the final
    // managed-reset acceptance condition.
    assert_eq!(state.processors_dropped.load(Ordering::Relaxed), 0);
    drop(retained);
    assert_eq!(state.ended.load(Ordering::Relaxed), 1);
    assert_eq!(state.processors_dropped.load(Ordering::Relaxed), 1);
}

fn test_provider(behavior: ShutdownBehavior) -> TestProvider {
    let (started_tx, started) = mpsc::channel();
    let (completed_tx, completed) = mpsc::channel();
    let state = Arc::new(ShutdownState {
        started: Mutex::new(Some(started_tx)),
        completed: Mutex::new(Some(completed_tx)),
        ..ShutdownState::default()
    });
    let processor = ControlledSpanProcessor {
        behavior,
        state: Arc::clone(&state),
    };
    let tracer_provider = SdkTracerProvider::builder()
        .with_span_processor(processor)
        .build();

    TestProvider {
        provider: OtelProvider {
            log_receipt: None,
            trace_receipt: None,
            logger: None,
            tracer_provider: Some(tracer_provider),
            tracer: None,
            metrics: None,
            shutdown_worker: None,
            shutdown_result: Mutex::new(None),
        },
        state,
        started,
        completed,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn bounded_shutdown_does_not_flush_when_worker_creation_fails() {
    let TestProvider {
        mut provider,
        state,
        started,
        completed,
    } = test_provider(ShutdownBehavior::Complete);

    let preparation = provider.prepare_shutdown_worker_with_spawner(|_startup| {
        Err(std::io::Error::new(
            ErrorKind::WouldBlock,
            "shutdown worker could not be created",
        ))
    });

    assert_eq!(
        preparation.as_ref().map_err(std::io::Error::kind),
        Err(ErrorKind::WouldBlock)
    );

    let result = provider
        .shutdown_with_timeout(Duration::from_secs(/*secs*/ 1))
        .await;
    assert_eq!(
        result.as_ref().map_err(std::io::Error::kind),
        Err(ErrorKind::NotConnected)
    );
    assert_eq!(state.shutdowns.load(Ordering::Relaxed), 0);
    assert_eq!(state.force_flushes.load(Ordering::Relaxed), 0);
    assert_eq!(started.try_recv(), Err(mpsc::TryRecvError::Empty));
    assert_eq!(completed.try_recv(), Err(mpsc::TryRecvError::Empty));
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn bounded_shutdown_survives_worker_guard_page_failure() {
    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("current time follows Unix epoch")
        .as_nanos();
    let temporary_directory = std::env::temp_dir().join(format!(
        "codex-otel-guard-page-{}-{unique_suffix}",
        std::process::id()
    ));
    std::fs::create_dir(&temporary_directory).expect("create fault injector directory");

    let observed_path = temporary_directory.join("guard_page_fault.observed");
    let mut subprocess = Command::new(std::env::current_exe().expect("current test binary"));
    subprocess
        .arg("--exact")
        .arg(GUARD_PAGE_FAILURE_CHILD_TEST)
        .arg("--ignored")
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env("CODEX_OTEL_GUARD_PAGE_FAILURE_CHILD", "1")
        .env("CODEX_OTEL_GUARD_PAGE_FAILURE_OBSERVED", &observed_path);

    let output = subprocess
        .output()
        .expect("run guard-page failure subprocess");
    let injection_was_observed = observed_path.is_file();

    let _ = std::fs::remove_dir_all(&temporary_directory);
    assert!(
        output.status.success(),
        "bounded telemetry shutdown crashed when its worker guard page could not be allocated\n\
         status: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        injection_was_observed,
        "guard-page fault injection never became active on {}-{}",
        std::env::consts::ARCH,
        std::env::consts::OS
    );
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
// The parent regression invokes this ignored test in a fresh subprocess with
// `--exact --ignored`, isolating fatal native-thread initialization failures.
#[ignore]
fn bounded_shutdown_survives_worker_guard_page_failure_child() {
    if std::env::var_os("CODEX_OTEL_GUARD_PAGE_FAILURE_CHILD").is_none() {
        return;
    }

    let TestProvider {
        mut provider,
        state,
        ..
    } = test_provider(ShutdownBehavior::Complete);

    #[cfg(target_os = "macos")]
    GUARD_PAGE_INJECTION_ENABLED.store(/*val*/ true, Ordering::Relaxed);

    provider
        .prepare_shutdown_worker()
        .expect("pre-initialize bounded shutdown worker");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("create current-thread runtime");

    #[cfg(target_os = "macos")]
    {
        assert!(
            GUARD_PAGE_INJECTION_OBSERVED.load(Ordering::Relaxed),
            "Rust mprotect interposer did not observe shutdown-worker guard-page setup"
        );
        GUARD_PAGE_INJECTION_ARMED.store(/*val*/ true, Ordering::Relaxed);
    }

    #[cfg(target_os = "linux")]
    {
        use seccompiler::BpfProgram;
        use seccompiler::SeccompAction;
        use seccompiler::SeccompCmpArgLen;
        use seccompiler::SeccompCmpOp;
        use seccompiler::SeccompCondition;
        use seccompiler::SeccompFilter;
        use seccompiler::SeccompRule;

        let page_size =
            usize::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) }).expect("valid page size");
        let mapped_page = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                page_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                /*fd*/ -1,
                /*offset*/ 0,
            )
        };
        assert_ne!(
            mapped_page,
            libc::MAP_FAILED,
            "map a page to verify guard-page fault injection: {}",
            std::io::Error::last_os_error()
        );

        let protection_is_none = SeccompCondition::new(
            /*arg_index*/ 2,
            SeccompCmpArgLen::Dword,
            SeccompCmpOp::Eq,
            libc::PROT_NONE as u64,
        )
        .expect("create guard-page seccomp condition");
        let rule =
            SeccompRule::new(vec![protection_is_none]).expect("create guard-page seccomp rule");
        let filter = SeccompFilter::new(
            std::collections::BTreeMap::from([(libc::SYS_mprotect, vec![rule])]),
            SeccompAction::Allow,
            SeccompAction::Errno(libc::ENOMEM as u32),
            std::env::consts::ARCH
                .try_into()
                .expect("supported seccomp architecture"),
        )
        .expect("create guard-page seccomp filter");
        let program: BpfProgram = filter
            .try_into()
            .expect("compile guard-page seccomp filter");
        seccompiler::apply_filter(&program).expect("install guard-page seccomp filter");

        let protection_result = unsafe { libc::mprotect(mapped_page, page_size, libc::PROT_NONE) };
        let protection_error = std::io::Error::last_os_error();
        assert_eq!(protection_result, -1);
        assert_eq!(protection_error.raw_os_error(), Some(libc::ENOMEM));
        assert_eq!(unsafe { libc::munmap(mapped_page, page_size) }, 0);
    }

    let observed_path = std::env::var_os("CODEX_OTEL_GUARD_PAGE_FAILURE_OBSERVED")
        .expect("guard-page fault observation path");
    std::fs::write(observed_path, "observed").expect("record guard-page fault injection");

    runtime
        .block_on(provider.shutdown_with_timeout(Duration::from_secs(/*secs*/ 1)))
        .expect("bounded telemetry shutdown should not create a new native thread");

    assert_eq!(state.shutdowns.load(Ordering::Relaxed), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn bounded_shutdown_times_out_without_blocking_the_runtime() {
    let TestProvider {
        mut provider,
        state,
        started,
        completed,
    } = test_provider(ShutdownBehavior::WaitForRelease);
    provider
        .prepare_shutdown_worker()
        .expect("pre-initialize bounded shutdown worker");

    let result = provider
        .shutdown_with_timeout(Duration::from_millis(/*millis*/ 50))
        .await;

    assert_eq!(
        result.as_ref().map_err(std::io::Error::kind),
        Err(ErrorKind::TimedOut)
    );
    started
        .recv_timeout(Duration::from_secs(/*secs*/ 1))
        .expect("shutdown worker started");
    assert_eq!(state.shutdowns.load(Ordering::Relaxed), 1);
    assert_eq!(state.force_flushes.load(Ordering::Relaxed), 0);

    *state.released.lock().expect("release lock") = true;
    state.release_notification.notify_one();
    completed
        .recv_timeout(Duration::from_secs(/*secs*/ 1))
        .expect("shutdown worker completed after release");
}

async fn assert_bounded_shutdown_completes() {
    let TestProvider {
        mut provider,
        state,
        started,
        completed,
    } = test_provider(ShutdownBehavior::Complete);
    provider
        .prepare_shutdown_worker()
        .expect("pre-initialize bounded shutdown worker");

    provider
        .shutdown_with_timeout(Duration::from_secs(/*secs*/ 1))
        .await
        .expect("healthy processor shuts down");

    started
        .recv_timeout(Duration::from_secs(/*secs*/ 1))
        .expect("shutdown worker started");
    completed
        .recv_timeout(Duration::from_secs(/*secs*/ 1))
        .expect("shutdown worker completed");
    assert_eq!(state.shutdowns.load(Ordering::Relaxed), 1);
    assert_eq!(state.force_flushes.load(Ordering::Relaxed), 0);
}

#[test]
fn checked_shutdown_failure_is_sticky_across_legacy_calls_and_drop() {
    let TestProvider {
        provider, state, ..
    } = test_provider(ShutdownBehavior::Fail);
    let expected = Err(super::OtelShutdownError::Traces);
    assert_eq!(provider.shutdown_checked(), expected);
    provider.shutdown();
    assert_eq!(provider.shutdown_checked(), expected);
    drop(provider);
    assert_eq!(state.shutdowns.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn bounded_shutdown_preserves_exporter_failure() {
    let TestProvider {
        provider, state, ..
    } = test_provider(ShutdownBehavior::Fail);
    let mut provider = provider;
    provider
        .prepare_shutdown_worker()
        .expect("prepare fixture worker");
    let result = provider.shutdown_with_timeout(Duration::from_secs(1)).await;
    assert_eq!(result.unwrap_err().kind(), ErrorKind::Other);
    assert_eq!(state.shutdowns.load(Ordering::Relaxed), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_retirement_timeout_reobserves_same_worker() {
    let TestProvider {
        provider,
        state,
        started,
        ..
    } = test_provider(ShutdownBehavior::WaitForRelease);
    let mut provider = provider;
    provider
        .prepare_shutdown_worker()
        .expect("prepare fixture worker");
    let retirement = provider.begin_retirement();
    started.recv_timeout(Duration::from_secs(1)).unwrap();
    let mut observer =
        Box::pin(retirement.wait_until(tokio::time::Instant::now() + Duration::from_secs(1)));
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(observer.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(observer);
    assert_eq!(state.processors_dropped.load(Ordering::Relaxed), 0);
    assert_eq!(
        retirement
            .wait_until(tokio::time::Instant::now() + Duration::from_millis(10))
            .await,
        Err(crate::OtelRetirementError::TimedOut)
    );
    assert_eq!(
        retirement.wait_until(tokio::time::Instant::now()).await,
        Err(crate::OtelRetirementError::TimedOut)
    );
    *state.released.lock().unwrap() = true;
    state.release_notification.notify_all();
    assert_eq!(
        retirement
            .wait_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await,
        Ok(())
    );
    assert_eq!(
        retirement.wait_until(tokio::time::Instant::now()).await,
        Ok(())
    );
    assert_eq!(state.processors_dropped.load(Ordering::Relaxed), 1);
    assert_eq!(state.shutdowns.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn retained_retirement_replays_exporter_failure() {
    let TestProvider {
        provider, state, ..
    } = test_provider(ShutdownBehavior::Fail);
    let mut provider = provider;
    provider
        .prepare_shutdown_worker()
        .expect("prepare fixture worker");
    let retirement = provider.begin_retirement();
    let expected = Err(crate::OtelRetirementError::Exporter(
        super::OtelShutdownError::Traces,
    ));
    assert_eq!(
        retirement
            .wait_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await,
        expected
    );
    assert_eq!(
        retirement.wait_until(tokio::time::Instant::now()).await,
        expected
    );
    assert_eq!(state.shutdowns.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn retained_retirement_without_worker_never_claims_cleanup_or_drops_on_caller() {
    let TestProvider {
        provider, state, ..
    } = test_provider(ShutdownBehavior::Complete);
    let retirement = provider.begin_retirement();
    for _ in 0..2 {
        assert_eq!(
            retirement
                .wait_until(tokio::time::Instant::now() + Duration::from_secs(1))
                .await,
            Err(crate::OtelRetirementError::WorkerFailed),
        );
    }
    drop(retirement);
    assert_eq!(state.shutdowns.load(Ordering::Relaxed), 0);
    assert_eq!(state.processors_dropped.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn retained_retirement_worker_panic_is_sticky() {
    let TestProvider {
        provider, state, ..
    } = test_provider(ShutdownBehavior::Panic);
    let mut provider = provider;
    provider
        .prepare_shutdown_worker()
        .expect("prepare fixture worker");
    let retirement = provider.begin_retirement();
    let expected = Err(crate::OtelRetirementError::WorkerFailed);
    assert_eq!(
        retirement
            .wait_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await,
        expected
    );
    assert_eq!(
        retirement.wait_until(tokio::time::Instant::now()).await,
        expected
    );
    assert_eq!(state.shutdowns.load(Ordering::Relaxed), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_retirement_long_observer_survives_short_observer_timeout() {
    let TestProvider {
        provider,
        state,
        started,
        ..
    } = test_provider(ShutdownBehavior::WaitForRelease);
    let mut provider = provider;
    provider
        .prepare_shutdown_worker()
        .expect("prepare fixture worker");
    let retirement = provider.begin_retirement();
    started.recv_timeout(Duration::from_secs(1)).unwrap();
    let mut short =
        Box::pin(retirement.wait_until(tokio::time::Instant::now() + Duration::from_millis(10)));
    let mut long =
        Box::pin(retirement.wait_until(tokio::time::Instant::now() + Duration::from_secs(1)));
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(short.as_mut(), cx).is_pending());
        assert!(std::future::Future::poll(long.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    assert_eq!(short.await, Err(crate::OtelRetirementError::TimedOut));
    *state.released.lock().unwrap() = true;
    state.release_notification.notify_all();
    assert_eq!(long.await, Ok(()));
    assert_eq!(state.shutdowns.load(Ordering::Relaxed), 1);
    assert_eq!(state.processors_dropped.load(Ordering::Relaxed), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn bounded_shutdown_completes_on_current_thread_runtime() {
    assert_bounded_shutdown_completes().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounded_shutdown_completes_on_multi_thread_runtime() {
    assert_bounded_shutdown_completes().await;
}

#[test]
fn explicit_shutdown_and_drop_shut_down_exporters_once_without_force_flush() {
    let TestProvider {
        provider,
        state,
        started,
        completed,
    } = test_provider(ShutdownBehavior::Complete);

    provider.shutdown();
    provider.shutdown();
    drop(provider);

    started
        .recv_timeout(Duration::from_secs(/*secs*/ 1))
        .expect("shutdown started");
    completed
        .recv_timeout(Duration::from_secs(/*secs*/ 1))
        .expect("shutdown completed");
    assert_eq!(state.shutdowns.load(Ordering::Relaxed), 1);
    assert_eq!(state.force_flushes.load(Ordering::Relaxed), 0);
}

#[test]
fn drop_shuts_down_exporters_without_force_flush() {
    let TestProvider {
        provider,
        state,
        started,
        completed,
    } = test_provider(ShutdownBehavior::Complete);

    drop(provider);

    started
        .recv_timeout(Duration::from_secs(/*secs*/ 1))
        .expect("shutdown started");
    completed
        .recv_timeout(Duration::from_secs(/*secs*/ 1))
        .expect("shutdown completed");
    assert_eq!(state.shutdowns.load(Ordering::Relaxed), 1);
    assert_eq!(state.force_flushes.load(Ordering::Relaxed), 0);
}
