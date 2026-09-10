use crate::config::OtelExporter;
use crate::config::OtelHttpProtocol;
use crate::config::OtelSettings;
use crate::metrics::MetricsClient;
use crate::metrics::MetricsConfig;
use crate::targets::is_log_export_target;
use crate::targets::is_trace_safe_target;
use gethostname::gethostname;
use opentelemetry::Context;
use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::trace::Span as _;
use opentelemetry::trace::SpanBuilder;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::LogExporter;
use opentelemetry_otlp::OTEL_EXPORTER_OTLP_LOGS_TIMEOUT;
use opentelemetry_otlp::OTEL_EXPORTER_OTLP_TRACES_TIMEOUT;
use opentelemetry_otlp::Protocol;
use opentelemetry_otlp::SpanExporter;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_otlp::WithHttpConfig;
use opentelemetry_otlp::WithTonicConfig;
use opentelemetry_otlp::tonic_types::metadata::MetadataMap;
use opentelemetry_otlp::tonic_types::transport::ClientTlsConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::runtime;
use opentelemetry_sdk::trace::BatchSpanProcessor;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::trace::Span;
use opentelemetry_sdk::trace::SpanData;
use opentelemetry_sdk::trace::SpanProcessor;
use opentelemetry_sdk::trace::Tracer;
use opentelemetry_sdk::trace::TracerProviderBuilder;
use opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor as TokioBatchSpanProcessor;
use opentelemetry_semantic_conventions as semconv;
use std::collections::BTreeMap;
use std::error::Error;
use std::io;
use std::mem::ManuallyDrop;
use std::sync::Mutex;
use std::time::Duration;
use tracing::debug;
use tracing_subscriber::Layer;
use tracing_subscriber::registry::LookupSpan;

const ENV_ATTRIBUTE: &str = "env";
const HOST_NAME_ATTRIBUTE: &str = "host.name";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResourceKind {
    Logs,
    Traces,
}

pub struct OtelProvider {
    pub logger: Option<SdkLoggerProvider>,
    pub tracer_provider: Option<SdkTracerProvider>,
    pub tracer: Option<Tracer>,
    pub metrics: Option<MetricsClient>,
    pub(crate) trace_receipt: Option<crate::trace_exporter_retirement::ExporterReceipt>,
    pub(crate) log_receipt: Option<crate::trace_exporter_retirement::ExporterReceipt>,
    shutdown_result: Mutex<Option<Result<(), OtelShutdownError>>>,
}

/// Fixed exporter-shutdown failure categories, without backend diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OtelShutdownError {
    #[error("telemetry shutdown state unavailable")]
    StateUnavailable,
    #[error("trace exporter shutdown failed")]
    Traces,
    #[error("metrics exporter shutdown failed")]
    Metrics,
    #[error("log exporter shutdown failed")]
    Logs,
}

struct ShutdownWorker {
    provider: ManuallyDrop<OtelProvider>,
    completed_tx: tokio::sync::oneshot::Sender<Result<(), OtelShutdownError>>,
}

#[derive(Debug)]
struct GlobalTracer {
    service_name: &'static str,
}

impl opentelemetry::trace::Tracer for GlobalTracer {
    type Span = global::BoxedSpan;

    fn build_with_context(&self, builder: SpanBuilder, parent: &Context) -> Self::Span {
        global::tracer(self.service_name).build_with_context(builder, parent)
    }
}

impl OtelProvider {
    /// Flushes and shuts down configured exporters at most once.
    pub fn shutdown(&self) {
        let _ = self.shutdown_checked();
    }

    /// Synchronously stop every configured exporter, retaining the first
    /// failure for all later observers. Run on a blocking worker, not an async
    /// executor thread. A returned error never skips the remaining exporters;
    /// an exporter panic interrupts shutdown and poisons its retained state.
    pub fn shutdown_checked(&self) -> Result<(), OtelShutdownError> {
        let mut completed = self
            .shutdown_result
            .lock()
            .map_err(|_| OtelShutdownError::StateUnavailable)?;
        if let Some(result) = *completed {
            return result;
        }
        let mut result = Ok(());
        if let Some(tracer_provider) = &self.tracer_provider {
            result = result.and(
                tracer_provider
                    .shutdown()
                    .map_err(|_| OtelShutdownError::Traces),
            );
        }
        if let Some(metrics) = &self.metrics {
            result = result.and(
                metrics
                    .shutdown_for_provider()
                    .map_err(|_| OtelShutdownError::Metrics),
            );
        }
        if let Some(logger) = &self.logger {
            result = result.and(logger.shutdown().map_err(|_| OtelShutdownError::Logs));
        }
        *completed = Some(result);
        result
    }

    /// Shuts down exporters on a detached thread within an external time budget.
    pub async fn shutdown_with_timeout(self, timeout: Duration) -> io::Result<()> {
        self.shutdown_with_timeout_and_spawner(timeout, |worker| {
            std::thread::Builder::new()
                .name("codex-otel-shutdown".to_string())
                .spawn(move || {
                    let provider = ManuallyDrop::into_inner(worker.provider);
                    let result = provider.shutdown_checked();
                    drop(provider);
                    let _ = worker.completed_tx.send(result);
                })
        })
        .await
    }

    async fn shutdown_with_timeout_and_spawner<F>(
        self,
        timeout: Duration,
        spawn: F,
    ) -> io::Result<()>
    where
        F: FnOnce(ShutdownWorker) -> io::Result<std::thread::JoinHandle<()>>,
    {
        let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();
        // A failed spawn drops its closure on the caller. Keep the provider
        // from synchronously running its potentially blocking destructor.
        let worker = ShutdownWorker {
            provider: ManuallyDrop::new(self),
            completed_tx,
        };
        let _shutdown_worker = spawn(worker)?;

        match tokio::time::timeout(timeout, completed_rx).await {
            Ok(Ok(result)) => result.map_err(io::Error::other),
            Ok(Err(_)) => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "telemetry shutdown worker stopped before completing",
            )),
            Err(_) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "telemetry shutdown exceeded its time budget",
            )),
        }
    }

    pub fn from(settings: &OtelSettings) -> Result<Option<Self>, Box<dyn Error>> {
        let crate::PreparedOtelProvider {
            mut provider,
            tracestate,
            statsig,
        } = Self::prepare(settings).map_err(crate::OtelPreparationError::into_legacy_error)?;
        crate::trace_context::set_tracestate_entries(tracestate)?;
        if let Some(tracer_provider) = provider.as_ref().and_then(|p| p.tracer_provider.clone()) {
            global::set_tracer_provider(tracer_provider);
            global::set_text_map_propagator(TraceContextPropagator::new());
        } else {
            // Disabled traces must not leave the preceding account's provider
            // installed for newly created global spans.
            global::set_tracer_provider(opentelemetry::trace::noop::NoopTracerProvider::new());
        }
        if let Some(metrics) = provider.as_mut().and_then(|p| p.metrics.as_mut()) {
            *metrics = crate::metrics::install_global_with_settings(metrics.clone(), statsig)?;
        } else {
            crate::metrics::disable_global()?;
        }
        Ok(provider)
    }

    // Construction must not publish any process-global route. The managed
    // reloader needs to retain a candidate before attempting logger publication.
    pub(crate) fn build_unpublished(
        settings: &OtelSettings,
    ) -> Result<Option<Self>, crate::OtelPreparationError> {
        let log_enabled = !matches!(settings.exporter, OtelExporter::None);
        let trace_enabled = !matches!(settings.trace_exporter, OtelExporter::None);
        let metric_exporter = crate::config::resolve_exporter(&settings.metrics_exporter);
        let metrics_enabled = !matches!(metric_exporter, OtelExporter::None);

        if !log_enabled && !trace_enabled && !metrics_enabled {
            debug!("No OTEL exporter enabled in settings.");
            return Ok(None);
        }

        // Validate before constructing exporters, which may start SDK workers.
        if trace_enabled {
            crate::config::validate_span_attributes(&settings.span_attributes)
                .map_err(crate::OtelPreparationError::before_resources)?;
        }
        crate::trace_context::validate_tracestate_entries(&settings.tracestate)
            .map_err(crate::OtelPreparationError::before_resources)?;

        // Install each successful sub-owner immediately. A later constructor
        // error transfers this partial value rather than dropping SDK workers
        // before the managed reloader can retain their terminal observations.
        let mut provider = Self {
            logger: None,
            tracer_provider: None,
            tracer: None,
            metrics: None,
            trace_receipt: None,
            log_receipt: None,
            shutdown_result: Mutex::new(None),
        };
        let construction = (|| -> Result<(), Box<dyn Error>> {
            provider.metrics = if matches!(metric_exporter, OtelExporter::None) {
                None
            } else {
                let mut config = MetricsConfig::otlp(
                    settings.environment.clone(),
                    settings.service_name.clone(),
                    settings.service_version.clone(),
                    settings.metrics_exporter.clone(),
                );
                if settings.runtime_metrics {
                    config = config.with_runtime_reader();
                }
                Some(MetricsClient::new(config)?)
            };

            let log_resource = make_resource(settings, ResourceKind::Logs);
            let trace_resource = make_resource(settings, ResourceKind::Traces);
            let (logger, log_receipt) = log_enabled
                .then(|| build_logger(&log_resource, &settings.exporter))
                .transpose()?
                .map(|(provider, receipt)| (Some(provider), receipt))
                .unwrap_or_default();
            provider.logger = logger;
            provider.log_receipt = log_receipt;

            let (tracer_provider, trace_receipt) = trace_enabled
                .then(|| {
                    build_tracer_provider(
                        &trace_resource,
                        &settings.trace_exporter,
                        settings.span_attributes.clone(),
                    )
                })
                .transpose()?
                .map(|(provider, receipt)| (Some(provider), receipt))
                .unwrap_or_default();

            provider.tracer = tracer_provider
                .as_ref()
                .map(|provider| provider.tracer(settings.service_name.clone()));

            provider.tracer_provider = tracer_provider;
            provider.trace_receipt = trace_receipt;
            Ok(())
        })();
        match construction {
            Ok(()) => Ok(Some(provider)),
            Err(source) => Err(crate::OtelPreparationError {
                source,
                provider: Some(provider),
            }),
        }
    }

    pub fn logger_layer<S>(&self) -> Option<impl Layer<S> + Send + Sync>
    where
        S: tracing::Subscriber + for<'span> LookupSpan<'span> + Send + Sync,
    {
        self.logger_export_layer().map(|layer| {
            layer.with_filter(tracing_subscriber::filter::filter_fn(
                OtelProvider::log_export_filter,
            ))
        })
    }

    /// Returns a log-export bridge that must be installed beneath the log export filter.
    pub fn logger_export_layer<S>(&self) -> Option<impl Layer<S> + Send + Sync>
    where
        S: tracing::Subscriber + for<'span> LookupSpan<'span> + Send + Sync,
    {
        self.logger.as_ref().map(OpenTelemetryTracingBridge::new)
    }

    pub fn tracing_layer<S>(&self) -> Option<impl Layer<S> + Send + Sync>
    where
        S: tracing::Subscriber + for<'span> LookupSpan<'span> + Send + Sync,
    {
        self.tracer.as_ref().map(|tracer| {
            tracing_opentelemetry::layer()
                .with_tracer(tracer.clone())
                .with_filter(tracing_subscriber::filter::filter_fn(
                    OtelProvider::trace_export_filter,
                ))
        })
    }

    /// Returns a permanent trace layer that follows the process-global tracer provider.
    pub fn reloadable_tracing_layer<S>(service_name: &'static str) -> impl Layer<S> + Send + Sync
    where
        S: tracing::Subscriber + for<'span> LookupSpan<'span> + Send + Sync,
    {
        tracing_opentelemetry::layer()
            .with_tracer(GlobalTracer { service_name })
            .with_filter(tracing_subscriber::filter::filter_fn(
                Self::trace_export_filter,
            ))
    }

    pub fn codex_export_filter(meta: &tracing::Metadata<'_>) -> bool {
        Self::log_export_filter(meta)
    }

    pub fn log_export_filter(meta: &tracing::Metadata<'_>) -> bool {
        is_log_export_target(meta.target())
    }

    pub fn trace_export_filter(meta: &tracing::Metadata<'_>) -> bool {
        meta.is_span() || is_trace_safe_target(meta.target())
    }

    pub fn metrics(&self) -> Option<&MetricsClient> {
        self.metrics.as_ref()
    }
}

impl Drop for OtelProvider {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn make_resource(settings: &OtelSettings, kind: ResourceKind) -> Resource {
    Resource::builder()
        .with_service_name(settings.service_name.clone())
        .with_attributes(resource_attributes(
            settings,
            detected_host_name().as_deref(),
            kind,
        ))
        .build()
}

fn resource_attributes(
    settings: &OtelSettings,
    host_name: Option<&str>,
    kind: ResourceKind,
) -> Vec<KeyValue> {
    let mut attributes = vec![
        KeyValue::new(
            semconv::attribute::SERVICE_VERSION,
            settings.service_version.clone(),
        ),
        KeyValue::new(ENV_ATTRIBUTE, settings.environment.clone()),
    ];
    if kind == ResourceKind::Logs
        && let Some(host_name) = host_name.and_then(normalize_host_name)
    {
        attributes.push(KeyValue::new(HOST_NAME_ATTRIBUTE, host_name));
    }
    attributes
}

fn detected_host_name() -> Option<String> {
    let host_name = gethostname();
    normalize_host_name(host_name.to_string_lossy().as_ref())
}

fn normalize_host_name(host_name: &str) -> Option<String> {
    let host_name = host_name.trim();
    (!host_name.is_empty()).then(|| host_name.to_owned())
}

fn tracer_provider_builder(
    resource: &Resource,
    span_attributes: BTreeMap<String, String>,
) -> TracerProviderBuilder {
    let builder = SdkTracerProvider::builder().with_resource(resource.clone());
    if span_attributes.is_empty() {
        builder
    } else {
        builder.with_span_processor(SpanAttributesProcessor {
            attributes: span_attributes,
        })
    }
}

/// Applies configured attributes when spans start.
///
/// Resource attributes describe the provider process. These attributes are
/// per-span metadata, so they need to be attached before each span is exported.
#[derive(Debug)]
struct SpanAttributesProcessor {
    attributes: BTreeMap<String, String>,
}

impl SpanProcessor for SpanAttributesProcessor {
    fn on_start(&self, span: &mut Span, _cx: &Context) {
        for (key, value) in self.attributes.iter() {
            span.set_attribute(KeyValue::new(key.clone(), value.clone()));
        }
    }

    fn on_end(&self, _span: SpanData) {}

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        Ok(())
    }
}

fn build_logger(
    resource: &Resource,
    exporter: &OtelExporter,
) -> Result<
    (
        SdkLoggerProvider,
        Option<crate::trace_exporter_retirement::ExporterReceipt>,
    ),
    Box<dyn Error>,
> {
    let mut builder = SdkLoggerProvider::builder().with_resource(resource.clone());
    let receipt;

    match crate::config::resolve_exporter(exporter) {
        OtelExporter::None => return Ok((builder.build(), None)),
        OtelExporter::Statsig => unreachable!("statsig exporter should be resolved"),
        OtelExporter::OtlpGrpc {
            endpoint,
            headers,
            tls,
        } => {
            debug!("Using OTLP Grpc exporter: {endpoint}");

            let header_map = crate::otlp::build_header_map(&headers);

            let base_tls_config = ClientTlsConfig::new()
                .with_enabled_roots()
                .assume_http2(true);

            let tls_config = match tls.as_ref() {
                Some(tls) => crate::otlp::build_grpc_tls_config(&endpoint, base_tls_config, tls)?,
                None => base_tls_config,
            };

            let exporter = LogExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .with_metadata(MetadataMap::from_headers(header_map))
                .with_tls_config(tls_config)
                .build()?;

            let (exporter, evidence) =
                crate::trace_exporter_retirement::AcknowledgedExporter::new(exporter);
            receipt = evidence;
            builder = builder.with_batch_exporter(exporter);
        }
        OtelExporter::OtlpHttp {
            endpoint,
            headers,
            protocol,
            tls,
        } => {
            debug!("Using OTLP Http exporter: {endpoint}");

            let protocol = match protocol {
                OtelHttpProtocol::Binary => Protocol::HttpBinary,
                OtelHttpProtocol::Json => Protocol::HttpJson,
            };

            let mut exporter_builder = LogExporter::builder()
                .with_http()
                .with_endpoint(endpoint)
                .with_protocol(protocol)
                .with_headers(headers);

            if let Some(tls) = tls.as_ref() {
                let client = crate::otlp::build_http_client(tls, OTEL_EXPORTER_OTLP_LOGS_TIMEOUT)?;
                exporter_builder = exporter_builder.with_http_client(client);
            }

            let exporter = exporter_builder.build()?;

            let (exporter, evidence) =
                crate::trace_exporter_retirement::AcknowledgedExporter::new(exporter);
            receipt = evidence;
            builder = builder.with_batch_exporter(exporter);
        }
    }

    Ok((builder.build(), Some(receipt)))
}

fn build_tracer_provider(
    resource: &Resource,
    exporter: &OtelExporter,
    span_attributes: BTreeMap<String, String>,
) -> Result<
    (
        SdkTracerProvider,
        Option<crate::trace_exporter_retirement::ExporterReceipt>,
    ),
    Box<dyn Error>,
> {
    let span_exporter = match crate::config::resolve_exporter(exporter) {
        OtelExporter::None => {
            return Ok((
                tracer_provider_builder(resource, span_attributes).build(),
                None,
            ));
        }
        OtelExporter::Statsig => unreachable!("statsig exporter should be resolved"),
        OtelExporter::OtlpGrpc {
            endpoint,
            headers,
            tls,
        } => {
            debug!("Using OTLP Grpc exporter for traces: {endpoint}");

            let header_map = crate::otlp::build_header_map(&headers);

            let base_tls_config = ClientTlsConfig::new()
                .with_enabled_roots()
                .assume_http2(true);

            let tls_config = match tls.as_ref() {
                Some(tls) => crate::otlp::build_grpc_tls_config(&endpoint, base_tls_config, tls)?,
                None => base_tls_config,
            };

            SpanExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .with_metadata(MetadataMap::from_headers(header_map))
                .with_tls_config(tls_config)
                .build()?
        }
        OtelExporter::OtlpHttp {
            endpoint,
            headers,
            protocol,
            tls,
        } => {
            debug!("Using OTLP Http exporter for traces: {endpoint}");

            if crate::otlp::current_tokio_runtime_is_multi_thread() {
                let protocol = match protocol {
                    OtelHttpProtocol::Binary => Protocol::HttpBinary,
                    OtelHttpProtocol::Json => Protocol::HttpJson,
                };

                let mut exporter_builder = SpanExporter::builder()
                    .with_http()
                    .with_endpoint(endpoint)
                    .with_protocol(protocol)
                    .with_headers(headers);

                let client = crate::otlp::build_async_http_client(
                    tls.as_ref(),
                    OTEL_EXPORTER_OTLP_TRACES_TIMEOUT,
                )?;
                exporter_builder = exporter_builder.with_http_client(client);

                let (exporter, receipt) =
                    crate::trace_exporter_retirement::AcknowledgedExporter::new(
                        exporter_builder.build()?,
                    );
                let processor = TokioBatchSpanProcessor::builder(exporter, runtime::Tokio).build();

                return Ok((
                    tracer_provider_builder(resource, span_attributes)
                        .with_span_processor(processor)
                        .build(),
                    Some(receipt),
                ));
            }

            let protocol = match protocol {
                OtelHttpProtocol::Binary => Protocol::HttpBinary,
                OtelHttpProtocol::Json => Protocol::HttpJson,
            };

            let mut exporter_builder = SpanExporter::builder()
                .with_http()
                .with_endpoint(endpoint)
                .with_protocol(protocol)
                .with_headers(headers);

            if let Some(tls) = tls.as_ref() {
                let client =
                    crate::otlp::build_http_client(tls, OTEL_EXPORTER_OTLP_TRACES_TIMEOUT)?;
                exporter_builder = exporter_builder.with_http_client(client);
            }

            exporter_builder.build()?
        }
    };

    let (span_exporter, receipt) =
        crate::trace_exporter_retirement::AcknowledgedExporter::new(span_exporter);
    let processor = BatchSpanProcessor::builder(span_exporter).build();

    Ok((
        tracer_provider_builder(resource, span_attributes)
            .with_span_processor(processor)
            .build(),
        Some(receipt),
    ))
}

#[cfg(test)]
#[path = "provider_shutdown_tests.rs"]
mod shutdown_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::API_CALL_COUNT_METRIC;
    use crate::metrics::API_CALL_DURATION_METRIC;
    use crate::metrics::MetricsExporter;
    use crate::metrics::TOOL_CALL_COUNT_METRIC;
    use crate::metrics::TOOL_CALL_DURATION_METRIC;
    use opentelemetry_sdk::metrics::InMemoryMetricExporter;
    use pretty_assertions::assert_eq;
    use std::path::PathBuf;

    #[test]
    fn resource_attributes_include_host_name_when_present() {
        let attrs = resource_attributes(
            &test_otel_settings(),
            Some("opentelemetry-test"),
            ResourceKind::Logs,
        );

        let host_name = attrs
            .iter()
            .find(|kv| kv.key.as_str() == HOST_NAME_ATTRIBUTE)
            .map(|kv| kv.value.as_str().to_string());

        assert_eq!(host_name, Some("opentelemetry-test".to_string()));
    }

    #[test]
    fn resource_attributes_omit_host_name_when_missing_or_empty() {
        let missing = resource_attributes(
            &test_otel_settings(),
            /*host_name*/ None,
            ResourceKind::Logs,
        );
        let empty = resource_attributes(&test_otel_settings(), Some("   "), ResourceKind::Logs);
        let trace_attrs = resource_attributes(
            &test_otel_settings(),
            Some("opentelemetry-test"),
            ResourceKind::Traces,
        );

        assert!(
            !missing
                .iter()
                .any(|kv| kv.key.as_str() == HOST_NAME_ATTRIBUTE)
        );
        assert!(
            !empty
                .iter()
                .any(|kv| kv.key.as_str() == HOST_NAME_ATTRIBUTE)
        );
        assert!(
            !trace_attrs
                .iter()
                .any(|kv| kv.key.as_str() == HOST_NAME_ATTRIBUTE)
        );
    }

    #[test]
    fn log_export_target_excludes_trace_safe_events() {
        assert!(is_log_export_target("codex_otel.log_only"));
        assert!(is_log_export_target("codex_otel.network_proxy"));
        assert!(!is_log_export_target("codex_otel.trace_safe"));
        assert!(!is_log_export_target("codex_otel.trace_safe.debug"));
    }

    #[test]
    fn trace_export_target_only_includes_trace_safe_prefix() {
        assert!(is_trace_safe_target("codex_otel.trace_safe"));
        assert!(is_trace_safe_target("codex_otel.trace_safe.summary"));
        assert!(!is_trace_safe_target("codex_otel.log_only"));
        assert!(!is_trace_safe_target("codex_otel.network_proxy"));
    }

    #[test]
    fn cached_global_metrics_follow_reinstalled_provider() -> Result<(), Box<dyn Error>> {
        let initial =
            crate::metrics::install_global(MetricsClient::new(MetricsConfig::in_memory(
                "test",
                "codex-test",
                env!("CARGO_PKG_VERSION"),
                InMemoryMetricExporter::default(),
            ))?)?;
        let cached = crate::metrics::global().expect("initial global metrics client");

        let exporter = InMemoryMetricExporter::default();
        let replacement =
            crate::metrics::install_global(MetricsClient::new(MetricsConfig::in_memory(
                "test",
                "codex-test",
                env!("CARGO_PKG_VERSION"),
                exporter.clone(),
            ))?)?;
        cached.counter("codex.after_transition", /*inc*/ 1, &[])?;
        initial.shutdown()?;
        replacement.shutdown()?;

        let exported_metrics = exporter.get_finished_metrics()?;
        let mut names: Vec<_> = exported_metrics
            .iter()
            .flat_map(opentelemetry_sdk::metrics::data::ResourceMetrics::scope_metrics)
            .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
            .map(opentelemetry_sdk::metrics::data::Metric::name)
            .collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names, vec!["codex.after_transition"]);

        Ok(())
    }

    #[test]
    fn statsig_runtime_only_metrics_are_not_exported() -> Result<(), Box<dyn Error>> {
        let exporter = InMemoryMetricExporter::default();
        let mut config = MetricsConfig::otlp(
            "test",
            "codex-cli",
            env!("CARGO_PKG_VERSION"),
            OtelExporter::Statsig,
        );
        config.exporter = MetricsExporter::InMemory(exporter.clone());
        let metrics = MetricsClient::new(config)?;

        metrics.counter(API_CALL_COUNT_METRIC, /*inc*/ 1, &[])?;
        metrics.record_duration(API_CALL_DURATION_METRIC, Duration::from_millis(100), &[])?;
        metrics.counter(TOOL_CALL_COUNT_METRIC, /*inc*/ 1, &[])?;
        metrics.record_duration(TOOL_CALL_DURATION_METRIC, Duration::from_millis(25), &[])?;
        metrics.counter("codex.turns", /*inc*/ 1, &[])?;
        metrics.shutdown()?;

        let exported_metrics = exporter.get_finished_metrics()?;
        let mut names: Vec<_> = exported_metrics
            .iter()
            .flat_map(opentelemetry_sdk::metrics::data::ResourceMetrics::scope_metrics)
            .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
            .map(opentelemetry_sdk::metrics::data::Metric::name)
            .collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names, vec!["codex.turns"]);

        Ok(())
    }

    fn test_otel_settings() -> OtelSettings {
        OtelSettings {
            environment: "test".to_string(),
            service_name: "codex-test".to_string(),
            service_version: "0.0.0".to_string(),
            codex_home: PathBuf::from("."),
            exporter: OtelExporter::None,
            trace_exporter: OtelExporter::None,
            metrics_exporter: OtelExporter::None,
            runtime_metrics: false,
            span_attributes: BTreeMap::new(),
            tracestate: BTreeMap::new(),
        }
    }
}
