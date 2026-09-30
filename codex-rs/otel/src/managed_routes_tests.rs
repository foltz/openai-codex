use super::*;
use pretty_assertions::assert_eq;
use tracing_subscriber::prelude::*;

fn disabled() -> Option<PreparedOtelProvider> {
    Some(PreparedOtelProvider {
        provider: None,
        tracestate: Default::default(),
        statsig: None,
    })
}

#[tokio::test]
async fn live_subscriber_commits_disabled_routes_and_consumes_candidate_once() {
    let (layers, routes) = ManagedTelemetryRoutes::layers();
    let subscriber = tracing_subscriber::registry().with(layers);
    let dispatch = tracing::Dispatch::new(subscriber);
    let mut candidate = disabled();
    let publication =
        tracing::dispatcher::with_default(&dispatch, || routes.publish(&mut candidate).unwrap());
    assert!(candidate.is_none());
    assert!(publication.provider.is_none());
    assert_eq!(publication.drain_trace_until(Instant::now()).await, Ok(()));
    assert!(matches!(
        routes.publish(&mut candidate),
        Err(OtelShutdownError::StateUnavailable)
    ));
    assert!(crate::metrics::global().is_none());
}

#[test]
fn lost_subscriber_refuses_without_consuming_candidate_or_replacing_metrics() {
    let client = crate::MetricsClient::new(crate::MetricsConfig::in_memory(
        "test",
        "old",
        "1",
        opentelemetry_sdk::metrics::InMemoryMetricExporter::default(),
    ))
    .unwrap();
    let old = crate::metrics::install_global(client).unwrap();
    let (layers, routes) = ManagedTelemetryRoutes::layers();
    let subscriber = tracing_subscriber::registry().with(layers);
    drop(tracing::Dispatch::new(subscriber));
    let mut candidate = disabled();
    assert!(matches!(
        routes.publish(&mut candidate),
        Err(OtelShutdownError::StateUnavailable)
    ));
    assert!(candidate.is_some());
    crate::metrics::global()
        .unwrap()
        .counter("still.old", 1, &[])
        .unwrap();
    old.shutdown().unwrap();
}
