use super::*;
use pretty_assertions::assert_eq;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

#[test]
fn worker_preparation_failure_precedes_every_exporter_and_publication() {
    let calls = AtomicUsize::new(0);
    let settings = OtelSettings {
        environment: "test".to_owned(),
        service_name: "birth-worker".to_owned(),
        service_version: "1".to_owned(),
        codex_home: std::path::PathBuf::from("."),
        exporter: OtelExporter::None,
        trace_exporter: OtelExporter::None,
        metrics_exporter: OtelExporter::OtlpHttp {
            endpoint: "http://127.0.0.1:9/metrics".to_owned(),
            headers: Default::default(),
            protocol: OtelHttpProtocol::Binary,
            tls: None,
        },
        runtime_metrics: false,
        span_attributes: Default::default(),
        tracestate: Default::default(),
    };
    let error =
        match OtelProvider::build_unpublished_with_worker_preparation(&settings, |provider| {
            calls.fetch_add(1, Ordering::SeqCst);
            assert!(provider.logger.is_none());
            assert!(provider.tracer_provider.is_none());
            assert!(provider.metrics.is_none());
            provider.prepare_shutdown_worker_with_spawner(|_| {
                Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "synthetic worker refusal",
                ))
            })
        }) {
            Ok(_) => panic!("worker refusal must fail preparation"),
            Err(error) => error,
        };
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(error.provider.is_none());
    assert!(error.begin_retirement().is_none());
}
