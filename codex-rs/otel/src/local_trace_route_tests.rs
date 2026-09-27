use super::*;
use opentelemetry_sdk::trace::InMemorySpanExporter;
use opentelemetry_sdk::trace::SdkTracerProvider;
use pretty_assertions::assert_eq;
use tracing_subscriber::prelude::*;

#[tokio::test]
async fn publication_guard_aborts_without_mutation_and_retains_exact_old_epoch() {
    let provider = SdkTracerProvider::builder().build();
    let route = LocalTraceRoute::default();
    let _initial = route.replace(Some(provider.tracer("a"))).unwrap();
    let old = Arc::downgrade(route.0.read().unwrap().as_ref().unwrap());
    let guard = route.publication_guard().unwrap();
    assert!(route.0.try_read().is_err());
    drop(guard);
    assert!(Arc::ptr_eq(
        route.0.read().unwrap().as_ref().unwrap(),
        &old.upgrade().unwrap()
    ));

    let mut guard = route.publication_guard().unwrap();
    let retired = guard.replace(None);
    assert!(old.upgrade().is_some());
    drop(guard);
    assert!(route.0.read().unwrap().is_none());
    assert!(old.upgrade().is_some());
    let ticket = retired.into_drain_ticket();
    assert!(old.upgrade().is_none());
    assert_eq!(ticket.wait_until(Instant::now()).await, Ok(()));
    provider.shutdown().unwrap();
}

#[test]
fn never_entered_a_span_cannot_be_exported_by_b_when_closed_after_swap() {
    let a = InMemorySpanExporter::default();
    let b = InMemorySpanExporter::default();
    let first = SdkTracerProvider::builder()
        .with_simple_exporter(a.clone())
        .build();
    let second = SdkTracerProvider::builder()
        .with_simple_exporter(b.clone())
        .build();
    let route = LocalTraceRoute::default();
    let _initial = route.replace(Some(first.tracer("a"))).unwrap();
    let subscriber = tracing_subscriber::registry().with(route.layer());
    tracing::subscriber::with_default(subscriber, || {
        let old = tracing::info_span!("opened_under_a");
        let _a = route.replace(Some(second.tracer("b"))).unwrap();
        drop(old);
        drop(tracing::info_span!("opened_under_b"));
        let _b = route.replace(None).unwrap();
        drop(tracing::info_span!("disabled"));
        let _disabled = route.replace(Some(second.tracer("b"))).unwrap();
        drop(tracing::info_span!("reenabled"));
    });
    let names = |exporter: &InMemorySpanExporter| {
        exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .map(|span| span.name.to_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(names(&a), vec!["opened_under_a"]);
    assert_eq!(names(&b), vec!["opened_under_b", "reenabled"]);
    first.shutdown().unwrap();
    second.shutdown().unwrap();
}

#[test]
fn incoming_parent_overrides_ambient_without_changing_creation_generation() {
    use opentelemetry::trace::TraceContextExt as _;
    let a = InMemorySpanExporter::default();
    let b = InMemorySpanExporter::default();
    let first = SdkTracerProvider::builder()
        .with_simple_exporter(a.clone())
        .build();
    let second = SdkTracerProvider::builder()
        .with_simple_exporter(b.clone())
        .build();
    let route = LocalTraceRoute::default();
    let _initial = route.replace(Some(first.tracer("a"))).unwrap();
    let parent =
        crate::context_from_w3c_trace_context(&codex_protocol::protocol::W3cTraceContext {
            traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into()),
            tracestate: None,
        })
        .unwrap();
    let expected = parent.span().span_context().clone();
    tracing::subscriber::with_default(tracing_subscriber::registry().with(route.layer()), || {
        let ambient = tracing::info_span!("ambient");
        let _entered = ambient.enter();
        let before = Context::current().span().span_context().clone();
        let old = crate::span_with_parent_context(Some(parent), || tracing::info_span!("incoming"));
        assert_eq!(Context::current().span().span_context(), &before);
        let _a = route.replace(Some(second.tracer("b"))).unwrap();
        old.record("unused", "no-op");
        drop(old);
        let _b = route.replace(None).unwrap();
    });
    let spans = a.get_finished_spans().unwrap();
    let incoming = spans.iter().find(|span| span.name == "incoming").unwrap();
    assert_eq!(incoming.span_context.trace_id(), expected.trace_id());
    assert_eq!(incoming.parent_span_id, expected.span_id());
    assert!(b.get_finished_spans().unwrap().is_empty());
    first.shutdown().unwrap();
    second.shutdown().unwrap();
}

#[test]
fn explicit_root_parent_does_not_retain_ambient_or_move_to_a_later_generation() {
    use codex_protocol::protocol::W3cTraceContext;
    use opentelemetry::trace::TraceContextExt as _;
    let a = InMemorySpanExporter::default();
    let b = InMemorySpanExporter::default();
    let first = SdkTracerProvider::builder().with_simple_exporter(a.clone()).build();
    let second = SdkTracerProvider::builder().with_simple_exporter(b.clone()).build();
    let route = LocalTraceRoute::default();
    let _initial = route.replace(Some(first.tracer("a"))).unwrap();
    let parent = W3cTraceContext {
        traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into()),
        tracestate: None,
    };
    let expected = crate::context_from_w3c_trace_context(&parent).unwrap();
    tracing::subscriber::with_default(tracing_subscriber::registry().with(route.layer()), || {
        let ambient = tracing::info_span!("ambient");
        let root = {
            let _entered = ambient.enter();
            crate::root_span_with_w3c_parent(Some(&parent), || {
                let span = tracing::info_span!(parent: None, "detached");
                // Publication before helper return must not change the selected epoch.
                let _old = route.replace(Some(second.tracer("b"))).unwrap();
                span
            })
        };
        drop(ambient);
        assert_eq!(a.get_finished_spans().unwrap().iter().map(|s| s.name.as_ref()).collect::<Vec<_>>(), vec!["ambient"]);
        let propagated = crate::span_w3c_trace_context(&root).unwrap();
        let actual = crate::context_from_w3c_trace_context(&propagated).unwrap();
        assert_eq!(actual.span().span_context().trace_id(), expected.span().span_context().trace_id());
        drop(root);
        drop(tracing::info_span!(parent: None, "next_generation"));
    });
    let spans = a.get_finished_spans().unwrap();
    let root = spans.iter().find(|span| span.name == "detached").unwrap();
    assert_eq!(root.span_context.trace_id(), expected.span().span_context().trace_id());
    assert_eq!(root.parent_span_id, expected.span().span_context().span_id());
    let later = b.get_finished_spans().unwrap();
    assert_eq!(later.len(), 1);
    assert_eq!(later[0].name, "next_generation");
    assert_ne!(later[0].span_context.trace_id(), root.span_context.trace_id());
    first.shutdown().unwrap();
    second.shutdown().unwrap();
}

#[test]
fn explicit_root_parent_works_with_a_plain_subscriber_without_retaining_ambient() {
    use codex_protocol::protocol::W3cTraceContext;
    use opentelemetry::trace::TraceContextExt as _;
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder().with_simple_exporter(exporter.clone()).build();
    let parent = W3cTraceContext {
        traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into()),
        tracestate: None,
    };
    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("plain")));
    tracing::subscriber::with_default(subscriber, || {
        let ambient = tracing::info_span!("ambient");
        let root = {
            let _entered = ambient.enter();
            crate::root_span_with_w3c_parent(Some(&parent), || tracing::info_span!(parent: None, "detached"))
        };
        drop(ambient);
        assert_eq!(exporter.get_finished_spans().unwrap().len(), 1);
        drop(root);
    });
    let spans = exporter.get_finished_spans().unwrap();
    let root = spans.iter().find(|span| span.name == "detached").unwrap();
    let expected = crate::context_from_w3c_trace_context(&parent).unwrap();
    assert_eq!(root.span_context.trace_id(), expected.span().span_context().trace_id());
    assert_eq!(root.parent_span_id, expected.span().span_context().span_id());
    provider.shutdown().unwrap();
}

#[test]
fn nested_root_parent_scope_restores_after_unwind_without_leaking_into_later_spans() {
    use codex_protocol::protocol::W3cTraceContext;
    use opentelemetry::trace::TraceContextExt as _;
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder().with_simple_exporter(exporter.clone()).build();
    let route = LocalTraceRoute::default();
    let _initial = route.replace(Some(provider.tracer("roots"))).unwrap();
    let outer = W3cTraceContext {
        traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into()),
        tracestate: None,
    };
    let inner = W3cTraceContext {
        traceparent: Some("00-11111111111111111111111111111111-2222222222222222-01".into()),
        tracestate: None,
    };
    tracing::subscriber::with_default(tracing_subscriber::registry().with(route.layer()), || {
        drop(crate::root_span_with_w3c_parent(Some(&outer), || {
            let interrupted = std::panic::catch_unwind(|| {
                crate::root_span_with_w3c_parent(Some(&inner), || panic!("interrupted creation"))
            });
            assert!(interrupted.is_err());
            drop(crate::root_span_with_w3c_parent(Some(&inner), || tracing::info_span!(parent: None, "inner")));
            tracing::info_span!(parent: None, "outer")
        }));
        drop(tracing::info_span!(parent: None, "unrelated"));
    });
    let spans = exporter.get_finished_spans().unwrap();
    let trace_id = |name: &str| spans.iter().find(|span| span.name == name).unwrap().span_context.trace_id();
    let outer_context = crate::context_from_w3c_trace_context(&outer).unwrap();
    let inner_context = crate::context_from_w3c_trace_context(&inner).unwrap();
    assert_eq!(trace_id("outer"), outer_context.span().span_context().trace_id());
    assert_eq!(trace_id("inner"), inner_context.span().span_context().trace_id());
    assert_ne!(trace_id("unrelated"), trace_id("outer"));
    assert_ne!(trace_id("unrelated"), trace_id("inner"));
    provider.shutdown().unwrap();
}

#[tokio::test(start_paused = true)]
async fn selected_a_build_drains_exactly_once_after_swap_timeout_and_observer_cancellation() {
    use opentelemetry::trace::Span as _;
    use opentelemetry::trace::Tracer as _;
    use std::time::Duration;

    let a = InMemorySpanExporter::default();
    let b = InMemorySpanExporter::default();
    let first = SdkTracerProvider::builder()
        .with_simple_exporter(a.clone())
        .build();
    let second = SdkTracerProvider::builder()
        .with_simple_exporter(b.clone())
        .build();
    let route = LocalTraceRoute::default();
    let _initial = route.replace(Some(first.tracer("a"))).unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let worker_route = route.clone();
    let worker = std::thread::spawn(move || {
        worker_route.with_tracer(|tracer| {
            entered_tx.send(()).unwrap();
            // Dropping the sender on assertion failure also releases the fixture.
            let _ = release_rx.recv();
            tracer.unwrap().start("selected_a").end();
        })
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let ticket = route.replace(Some(second.tracer("b"))).unwrap();
    route.start("new_b").end();
    let deadline = Instant::now() + Duration::from_secs(1);
    let mut observer = Box::pin(ticket.wait_until(deadline));
    assert!(
        std::future::Future::poll(
            observer.as_mut(),
            &mut std::task::Context::from_waker(std::task::Waker::noop())
        )
        .is_pending()
    );
    drop(observer);
    assert_eq!(
        ticket.wait_until(deadline).await,
        Err(crate::OtelRetirementError::TimedOut)
    );
    assert_eq!(Instant::now(), deadline);
    assert!(a.get_finished_spans().unwrap().is_empty());
    assert_eq!(b.get_finished_spans().unwrap()[0].name, "new_b");
    release_tx.send(()).unwrap();
    worker.join().unwrap();
    // Zero is durable evidence, including replay with an expired budget.
    assert_eq!(ticket.wait_until(deadline).await, Ok(()));
    assert_eq!(ticket.wait_until(deadline).await, Ok(()));
    assert_eq!(a.get_finished_spans().unwrap()[0].name, "selected_a");
    first.shutdown().unwrap();
    second.shutdown().unwrap();
}

#[tokio::test]
async fn panicking_build_releases_lease_without_poisoning_route() {
    let provider = SdkTracerProvider::builder().build();
    let route = LocalTraceRoute::default();
    let _initial = route.replace(Some(provider.tracer("a"))).unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        route.with_tracer(|_| panic!("build callback panic"));
    }));
    assert!(result.is_err());
    let ticket = route.replace(None).unwrap();
    assert_eq!(ticket.wait_until(Instant::now()).await, Ok(()));
    provider.shutdown().unwrap();
}

#[test]
fn combined_layer_filter_and_initially_disabled_route_preserve_selection() {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let route = LocalTraceRoute::default();
    let subscriber = tracing_subscriber::registry().with(route.layer().with_filter(
        tracing_subscriber::filter::filter_fn(|metadata| metadata.target() != "excluded"),
    ));
    tracing::subscriber::with_default(subscriber, || {
        let disabled = tracing::info_span!("initially_disabled");
        let _empty = route.replace(Some(provider.tracer("enabled"))).unwrap();
        drop(disabled);
        drop(tracing::info_span!(target: "excluded", "filtered"));
        drop(tracing::info_span!("included"));
    });
    assert_eq!(
        exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .map(|span| span.name.to_string())
            .collect::<Vec<_>>(),
        vec!["included"]
    );
    provider.shutdown().unwrap();
}
