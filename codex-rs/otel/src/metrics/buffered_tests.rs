use super::*;
use crate::MetricsConfig;
use opentelemetry_sdk::metrics::data::AggregatedMetrics;
use opentelemetry_sdk::metrics::data::MetricData;
use pretty_assertions::assert_eq;
use std::sync::Barrier;

fn client() -> MetricsClient {
    MetricsClient::new(
        MetricsConfig::in_memory("test", "test", "1", Default::default()).with_runtime_reader(),
    )
    .unwrap()
}

fn observe(buffer: &BufferedMetrics) {
    buffer
        .record(
            "test.operations",
            "test.duration",
            Duration::from_millis(/*millis*/ 7),
            &[("component", "test")],
        )
        .unwrap();
}

fn totals(metrics: &MetricsClient) -> (u64, u64, f64) {
    let snapshot = metrics.snapshot().unwrap();
    let mut totals = (0, 0, 0.0);
    for metric in snapshot
        .scope_metrics()
        .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
    {
        match metric.data() {
            AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                totals.0 += sum
                    .data_points()
                    .map(opentelemetry_sdk::metrics::data::SumDataPoint::value)
                    .sum::<u64>();
            }
            AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                for point in histogram.data_points() {
                    totals.1 += point.count();
                    totals.2 += point.sum();
                }
            }
            _ => panic!("unexpected metric"),
        }
    }
    totals
}

#[test]
fn startup_limits_and_validation_preserve_whole_pairs() {
    let buffer = BufferedMetrics::new();
    let oversized = "x".repeat(MAX_METADATA_BYTES + 1);
    assert!(matches!(
        buffer.record("count", "duration", Duration::ZERO, &[("tag", &oversized)]),
        Err(MetricsError::OperationMetadataTooLarge)
    ));
    assert!(matches!(
        buffer.record(
            "count",
            "duration",
            Duration::ZERO,
            &[("tag", "value"); MAX_TAGS + 1]
        ),
        Err(MetricsError::OperationMetadataTooLarge)
    ));
    for name in ["invalid duration", "1duration", &"d".repeat(256)] {
        assert!(buffer.record("count", name, Duration::ZERO, &[]).is_err());
        assert!(
            buffer
                .record(name, "duration", Duration::ZERO, &[])
                .is_err()
        );
    }
    assert!(
        buffer
            .record(
                "count",
                "duration",
                Duration::ZERO,
                &[("tag", "invalid value")]
            )
            .is_err()
    );
    for _ in 0..MAX_PENDING_OPERATIONS + 1 {
        observe(&buffer);
    }
    let metrics = client();
    buffer.enable(&metrics);
    assert_eq!(totals(&metrics), (256, 256, 1792.0));
    buffer.enable(&metrics);
    assert_eq!(totals(&metrics), (0, 0, 0.0));
    for name in ["invalid duration", "1duration", &"d".repeat(256)] {
        assert!(buffer.record("count", name, Duration::ZERO, &[]).is_err());
        assert!(
            buffer
                .record(name, "duration", Duration::ZERO, &[])
                .is_err()
        );
    }
    assert_eq!(totals(&metrics), (0, 0, 0.0));
}

#[test]
fn opt_out_discards_pending_and_live_observations_until_reenabled() {
    let buffer = BufferedMetrics::new();
    observe(&buffer);
    buffer.disable();
    observe(&buffer);
    let first = client();
    buffer.enable(&first);
    assert_eq!(totals(&first), (0, 0, 0.0));
    observe(&buffer);
    assert_eq!(totals(&first), (1, 1, 7.0));
    buffer.disable();
    observe(&buffer);
    assert_eq!(totals(&first), (0, 0, 0.0));
    let second = client();
    buffer.enable(&second);
    observe(&buffer);
    assert_eq!(totals(&second), (1, 1, 7.0));
    assert_eq!(totals(&first), (0, 0, 0.0));
}

#[test]
fn concurrent_installation_and_replacement_record_each_pair_once() {
    let buffer = BufferedMetrics::new();
    let first = client();
    let second = client();
    let barrier = Barrier::new(/*n*/ 5);
    observe(&buffer);
    std::thread::scope(|scope| {
        for _ in 0..4 {
            scope.spawn(|| {
                barrier.wait();
                for _ in 0..16 {
                    observe(&buffer);
                }
            });
        }
        barrier.wait();
        buffer.enable(&first);
        buffer.enable(&second);
    });
    let a = totals(&first);
    let b = totals(&second);
    assert_eq!((a.0 + b.0, a.1 + b.1, a.2 + b.2), (65, 65, 455.0));
    assert_eq!((a.0, b.0), (a.1, b.1));
    buffer.enable(&second);
    assert_eq!(totals(&second), (0, 0, 0.0));
}

#[test]
fn redirecting_a_global_handle_does_not_change_the_buffers_installation() {
    let buffer = BufferedMetrics::new();
    let first = client();
    let second = client();
    let active = Arc::new(crate::metrics::route::MetricsRoute::new(
        first.original_inner().unwrap(),
    ));
    let redirected = MetricsClient {
        inner: first.inner.clone(),
        active: Some(Arc::clone(&active)),
    };
    buffer.enable(&redirected);
    let _retirement = active.replace(second.original_inner().unwrap()).unwrap();
    observe(&buffer);
    assert_eq!(totals(&first), (1, 1, 7.0));
    assert_eq!(totals(&second), (0, 0, 0.0));
    buffer.enable(&second);
    observe(&buffer);
    assert_eq!(totals(&first), (0, 0, 0.0));
    assert_eq!(totals(&second), (1, 1, 7.0));
}

fn inner(metrics: &MetricsClient) -> Arc<MetricsClientInner> {
    metrics.original_inner().unwrap()
}

// Reads the original provider even after publication redirects the handle.
fn standalone(metrics: &MetricsClient) -> MetricsClient {
    MetricsClient {
        inner: super::super::client::MetricsOriginal::Standalone(inner(metrics)),
        active: None,
    }
}

#[test]
fn out_of_order_receipts_bind_latest_and_flush_pending_once() {
    let buffer = BufferedMetrics::new();
    let first = client();
    let second = client();
    observe(&buffer);
    observe(&buffer);
    let older = BufferedReceipt::enable(/*generation*/ 1, inner(&first));
    let newer = BufferedReceipt::enable(/*generation*/ 2, inner(&second));
    buffer.apply(newer);
    buffer.apply(older);
    observe(&buffer);
    assert_eq!(totals(&first), (0, 0, 0.0));
    assert_eq!(totals(&second), (3, 3, 21.0));
}

#[test]
fn racing_receipts_converge_on_highest_generation_and_flush_once() {
    let buffer = BufferedMetrics::new();
    let clients: Vec<_> = (0..8).map(|_| client()).collect();
    for _ in 0..3 {
        observe(&buffer);
    }
    let barrier = Barrier::new(clients.len());
    std::thread::scope(|scope| {
        for (generation, metrics) in (1u64..).zip(&clients) {
            let receipt = BufferedReceipt::enable(generation, inner(metrics));
            let (buffer, barrier) = (&buffer, &barrier);
            scope.spawn(move || {
                barrier.wait();
                buffer.apply(receipt);
            });
        }
    });
    observe(&buffer);
    let all: Vec<_> = clients.iter().map(totals).collect();
    let sum = all.iter().fold((0, 0, 0.0), |acc, t| {
        (acc.0 + t.0, acc.1 + t.1, acc.2 + t.2)
    });
    assert_eq!(sum, (4, 4, 28.0));
    assert!(
        all[7].0 >= 1,
        "the newest generation must own live recording"
    );
}

#[test]
fn stale_disable_is_ignored_and_newer_disable_wins() {
    let buffer = BufferedMetrics::new();
    let metrics = client();
    buffer.apply(BufferedReceipt::enable(
        /*generation*/ 2,
        inner(&metrics),
    ));
    buffer.apply(BufferedReceipt::disable(/*generation*/ 1));
    observe(&buffer);
    assert_eq!(totals(&metrics), (1, 1, 7.0));
    buffer.apply(BufferedReceipt::disable(/*generation*/ 3));
    buffer.apply(BufferedReceipt::enable(
        /*generation*/ 2,
        inner(&metrics),
    ));
    observe(&buffer);
    assert_eq!(totals(&metrics), (0, 0, 0.0));
}

#[test]
fn suspend_for_older_generation_does_not_detach_newer_binding() {
    let buffer = BufferedMetrics::new();
    let first = client();
    let second = client();
    buffer.apply(BufferedReceipt::enable(
        /*generation*/ 1,
        inner(&first),
    ));
    buffer.apply(BufferedReceipt::enable(
        /*generation*/ 2,
        inner(&second),
    ));
    buffer.suspend(/*generation*/ 1);
    observe(&buffer);
    assert_eq!(totals(&first), (0, 0, 0.0));
    assert_eq!(totals(&second), (1, 1, 7.0));
}

#[test]
fn suspend_then_late_newer_receipt_applies_and_flushes() {
    let buffer = BufferedMetrics::new();
    let first = client();
    let second = client();
    let newer = BufferedReceipt::enable(/*generation*/ 2, inner(&second));
    buffer.apply(BufferedReceipt::enable(
        /*generation*/ 1,
        inner(&first),
    ));
    buffer.suspend(/*generation*/ 1);
    observe(&buffer);
    assert_eq!(totals(&first), (0, 0, 0.0));
    buffer.apply(newer);
    assert_eq!(totals(&second), (1, 1, 7.0));
}

#[test]
fn newer_receipt_then_late_suspend_has_no_effect() {
    let buffer = BufferedMetrics::new();
    let second = client();
    buffer.apply(BufferedReceipt::enable(
        /*generation*/ 2,
        inner(&second),
    ));
    buffer.suspend(/*generation*/ 1);
    observe(&buffer);
    assert_eq!(totals(&second), (1, 1, 7.0));
}

#[test]
fn suspend_before_own_receipt_never_binds_the_shut_down_provider() {
    let buffer = BufferedMetrics::new();
    let first = client();
    let second = client();
    observe(&buffer);
    buffer.suspend(/*generation*/ 1);
    buffer.apply(BufferedReceipt::enable(
        /*generation*/ 1,
        inner(&first),
    ));
    observe(&buffer);
    assert_eq!(totals(&first), (0, 0, 0.0));
    buffer.apply(BufferedReceipt::enable(
        /*generation*/ 2,
        inner(&second),
    ));
    assert_eq!(totals(&second), (2, 2, 14.0));
}

#[test]
fn suspend_cannot_undo_opt_out() {
    let buffer = BufferedMetrics::new();
    let first = client();
    let second = client();
    buffer.apply(BufferedReceipt::enable(
        /*generation*/ 1,
        inner(&first),
    ));
    buffer.apply(BufferedReceipt::disable(/*generation*/ 2));
    buffer.suspend(/*generation*/ 1);
    buffer.suspend(/*generation*/ 2);
    observe(&buffer);
    buffer.apply(BufferedReceipt::enable(
        /*generation*/ 3,
        inner(&second),
    ));
    assert_eq!(totals(&first), (0, 0, 0.0));
    assert_eq!(totals(&second), (0, 0, 0.0));
}

#[test]
fn transitions_refuse_to_run_under_a_publication_guard() {
    let buffer = BufferedMetrics::new();
    let mark = super::super::PublicationMark::new();
    let refused = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        buffer.apply(BufferedReceipt::disable(/*generation*/ 1));
    }));
    drop(mark);
    assert!(refused.is_err());
    observe(&buffer);
    let metrics = client();
    buffer.apply(BufferedReceipt::enable(
        /*generation*/ 1,
        inner(&metrics),
    ));
    assert_eq!(totals(&metrics), (1, 1, 7.0));
}

#[test]
fn managed_publications_return_receipts_that_bind_latest_after_guards() {
    let slot = Mutex::new(super::super::GlobalMetrics::default());
    let buffer = BufferedMetrics::new();
    let (first, second) = (client(), client());
    let (first_view, second_view) = (standalone(&first), standalone(&second));
    observe(&buffer);
    let (older, newer) = {
        let mut publication = super::super::GlobalMetricsPublication::new(slot.lock().unwrap());
        assert_eq!(super::super::publication_guards_held(), 1);
        let mut candidate = Some(first);
        let older = publication
            .publish_with(&mut candidate, None, || {})
            .unwrap();
        let mut candidate = Some(second);
        let newer = publication
            .publish_with(&mut candidate, None, || {})
            .unwrap();
        (older, newer)
    };
    assert_eq!(super::super::publication_guards_held(), 0);
    assert!(newer.generation() > older.generation());
    // Nothing is exported until a receipt is applied.
    assert_eq!(totals(&first_view), (0, 0, 0.0));
    buffer.apply(newer);
    buffer.apply(older);
    observe(&buffer);
    assert_eq!(totals(&first_view), (0, 0, 0.0));
    assert_eq!(totals(&second_view), (2, 2, 14.0));

    let disabled = super::super::GlobalMetricsPublication::new(slot.lock().unwrap())
        .publish_with(&mut None, None, || {})
        .unwrap();
    buffer.apply(disabled);
    observe(&buffer);
    assert_eq!(totals(&second_view), (0, 0, 0.0));
}

#[test]
fn legacy_install_and_disable_use_generation_receipts() {
    let slot = Mutex::new(super::super::GlobalMetrics::default());
    let buffer = BufferedMetrics::new();
    let (first, second, third) = (client(), client(), client());
    let views = [standalone(&first), standalone(&second), standalone(&third)];
    observe(&buffer);
    let (_first, older) = super::super::install_in_with_receipt(&slot, first, None).unwrap();
    let (second, newer) = super::super::install_in_with_receipt(&slot, second, None).unwrap();
    assert!(
        slot.try_lock().is_ok(),
        "receipts are returned without the slot lock"
    );
    buffer.apply(newer);
    buffer.apply(older);
    observe(&buffer);
    assert_eq!(totals(&views[0]), (0, 0, 0.0));
    assert_eq!(totals(&views[1]), (2, 2, 14.0));

    // The published owner carries its generation, so its shutdown suspends only it.
    let super::super::client::MetricsOriginal::Routed(owner) = &second.inner else {
        panic!("routed owner");
    };
    buffer.suspend(owner.publication_generation().unwrap());
    observe(&buffer);
    assert_eq!(totals(&views[1]), (0, 0, 0.0));

    // A disable issued before a newer install is stale once that install binds.
    let stale_disable = super::super::disable_in_with_receipt(&slot).unwrap();
    let (_third, latest) = super::super::install_in_with_receipt(&slot, third, None).unwrap();
    buffer.apply(latest);
    buffer.apply(stale_disable);
    observe(&buffer);
    assert_eq!(totals(&views[2]), (2, 2, 14.0));

    buffer.apply(super::super::disable_in_with_receipt(&slot).unwrap());
    observe(&buffer);
    assert_eq!(totals(&views[2]), (0, 0, 0.0));
}
