use super::*;
use opentelemetry_sdk::error::OTelSdkError;
use opentelemetry_sdk::error::OTelSdkResult;
use pretty_assertions::assert_eq;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn route_replacement_waits_for_recording_operation() {
    let mut client = MetricsClient::new(
        MetricsConfig::in_memory(
            "test",
            "routing-fence",
            "1",
            opentelemetry_sdk::metrics::InMemoryMetricExporter::default(),
        )
        .with_runtime_reader(),
    )
    .unwrap();
    let next = MetricsClient::new(
        MetricsConfig::in_memory(
            "test",
            "routing-next",
            "1",
            opentelemetry_sdk::metrics::InMemoryMetricExporter::default(),
        )
        .with_runtime_reader(),
    )
    .unwrap();
    let route = Arc::new(crate::metrics::route::MetricsRoute::new(Arc::clone(
        &client.original_inner().unwrap(),
    )));
    client.active = Some(Arc::clone(&route));
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let recording = client.clone();
    let worker = std::thread::spawn(move || {
        recording.with_active_inner(|inner| {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            inner.counter("fenced.counter", None, 1, &[])
        })
    });
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let ticket = route
        .replace(Arc::clone(&next.original_inner().unwrap()))
        .unwrap();
    let mut drained = Box::pin(ticket.wait());
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(drained.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(drained);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), ticket.wait())
            .await
            .is_err()
    );
    client.counter("new.generation", 1, &[]).unwrap();
    let names = |snapshot: &ResourceMetrics| {
        snapshot
            .scope_metrics()
            .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
            .map(|metric| metric.name().to_owned())
            .collect::<Vec<_>>()
    };
    assert_eq!(names(&next.snapshot().unwrap()), vec!["new.generation"]);
    let old = MetricsClient {
        active: None,
        ..client.clone()
    };
    assert!(names(&old.snapshot().unwrap()).is_empty());
    release_tx.send(()).unwrap();
    worker.join().unwrap().unwrap();
    ticket.wait().await.unwrap();
    ticket.wait().await.unwrap();
    assert_eq!(names(&old.snapshot().unwrap()), vec!["fenced.counter"]);
    old.shutdown().unwrap();
    client.counter("after.old.shutdown", 1, &[]).unwrap();
    assert!(names(&next.snapshot().unwrap()).contains(&"after.old.shutdown".to_owned()));
    client
        .with_active_inner(|_| client.counter("reentrant.counter", 1, &[]))
        .unwrap();
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _: Result<()> = client.with_active_inner(|_| panic!("operation panic"));
        }))
        .is_err()
    );
    route
        .replace(Arc::clone(&next.original_inner().unwrap()))
        .unwrap()
        .wait()
        .await
        .unwrap();
    next.shutdown().unwrap();
}

#[tokio::test]
async fn snapshot_callback_reenters_new_generation_while_old_ticket_waits() {
    let make = |name| {
        MetricsClient::new(
            MetricsConfig::in_memory(
                "test",
                name,
                "1",
                opentelemetry_sdk::metrics::InMemoryMetricExporter::default(),
            )
            .with_runtime_reader(),
        )
        .unwrap()
    };
    let mut old = make("callback-old");
    let next = make("callback-next");
    let route = Arc::new(crate::metrics::route::MetricsRoute::new(Arc::clone(
        &old.original_inner().unwrap(),
    )));
    old.active = Some(Arc::clone(&route));
    let weak_route = Arc::downgrade(&route);
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let callbacks = Arc::new(AtomicUsize::new(0));
    let callback_count = Arc::clone(&callbacks);
    old.register_observable_gauge_with_description(
        "callback.gauge",
        "test",
        move || {
            if callback_count.fetch_add(1, Ordering::SeqCst) == 0 {
                entered_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
                weak_route
                    .upgrade()
                    .unwrap()
                    .with_inner(|inner| inner.counter("callback.new.generation", None, 1, &[]))
                    .unwrap();
            }
            1
        },
        &[],
    )
    .unwrap();
    let collecting = old.clone();
    let worker = std::thread::spawn(move || collecting.snapshot());
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let ticket = route
        .replace(Arc::clone(&next.original_inner().unwrap()))
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(10), ticket.wait())
            .await
            .is_err()
    );
    release_tx.send(()).unwrap();
    worker.join().unwrap().unwrap();
    ticket.wait().await.unwrap();
    let snapshot = next.snapshot().unwrap();
    assert!(
        snapshot
            .scope_metrics()
            .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
            .any(|metric| metric.name() == "callback.new.generation")
    );
    old.shutdown().unwrap();
    next.shutdown().unwrap();
}

#[tokio::test]
async fn completed_route_ticket_replays_without_retaining_old_exporter() {
    let make = |name| {
        MetricsClient::new(MetricsConfig::in_memory(
            "test",
            name,
            "1",
            opentelemetry_sdk::metrics::InMemoryMetricExporter::default(),
        ))
        .unwrap()
    };
    let old = make("released-old");
    let next = make("retained-next");
    let old_weak = Arc::downgrade(&old.original_inner().unwrap());
    let route =
        crate::metrics::route::MetricsRoute::new(Arc::clone(&old.original_inner().unwrap()));
    let ticket = route
        .replace(Arc::clone(&next.original_inner().unwrap()))
        .unwrap();
    old.shutdown().unwrap();
    drop(old);
    assert!(
        old_weak.upgrade().is_some(),
        "unobserved route still owns old exporter"
    );
    ticket.wait().await.unwrap();
    assert!(
        old_weak.upgrade().is_none(),
        "receipt must not pin old credentials"
    );
    ticket.clone().wait().await.unwrap();
    next.counter("still.active", 1, &[]).unwrap();
    next.shutdown().unwrap();
}

#[tokio::test]
async fn later_route_replacement_does_not_expand_earlier_drain_ticket() {
    let make = |name| {
        MetricsClient::new(MetricsConfig::in_memory(
            "test",
            name,
            "1",
            opentelemetry_sdk::metrics::InMemoryMetricExporter::default(),
        ))
        .unwrap()
    };
    let first = make("first");
    let second = make("second");
    let third = make("third");
    let route = Arc::new(crate::metrics::route::MetricsRoute::new(Arc::clone(
        &first.original_inner().unwrap(),
    )));
    let first_ticket = route
        .replace(Arc::clone(&second.original_inner().unwrap()))
        .unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let held = Arc::clone(&route);
    let worker = std::thread::spawn(move || {
        held.with_inner(|_| {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok(())
        })
    });
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let second_ticket = route
        .replace(Arc::clone(&third.original_inner().unwrap()))
        .unwrap();
    first_ticket.wait().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(10), second_ticket.wait())
            .await
            .is_err()
    );
    first_ticket.clone().wait().await.unwrap();
    release_tx.send(()).unwrap();
    worker.join().unwrap().unwrap();
    second_ticket.wait().await.unwrap();
    first.shutdown().unwrap();
    second.shutdown().unwrap();
    third.shutdown().unwrap();
}

#[derive(Debug)]
struct FailingFlushReader {
    shutdowns: Arc<AtomicUsize>,
}

#[tokio::test]
async fn metrics_retirement_timeout_preserves_drain_then_stops_exact_owner() {
    use crate::metrics::retirement::MetricsRetirementError;
    use crate::metrics::retirement::RetiredMetrics;
    use tokio::time::Instant;
    let make = |name| {
        MetricsClient::new(MetricsConfig::in_memory(
            "test",
            name,
            "1",
            opentelemetry_sdk::metrics::InMemoryMetricExporter::default(),
        ))
        .unwrap()
    };
    let old = make("retired-drain");
    let next = make("retired-next");
    let inner = old.original_inner().unwrap();
    let weak = Arc::downgrade(&inner);
    let owner = crate::metrics::owner::MetricsOwner::new(Arc::clone(&inner));
    let route = Arc::new(crate::metrics::route::MetricsRoute::new(inner));
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let held = Arc::clone(&route);
    let worker = std::thread::spawn(move || {
        held.with_inner(|inner| {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            inner.counter("drained.before.shutdown", None, 1, &[])
        })
    });
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let retirement = RetiredMetrics::new(
        owner,
        route.replace(next.original_inner().unwrap()).unwrap(),
    );
    drop(old);
    assert_eq!(
        retirement
            .wait_until(Instant::now() + Duration::from_millis(10))
            .await,
        Err(MetricsRetirementError::TimedOut)
    );
    assert!(weak.upgrade().is_some());
    release_tx.send(()).unwrap();
    worker.join().unwrap().unwrap();
    assert_eq!(
        retirement
            .wait_until(Instant::now() + Duration::from_secs(1))
            .await,
        Ok(())
    );
    assert!(weak.upgrade().is_none());
    assert_eq!(retirement.wait_until(Instant::now()).await, Ok(()));
    next.shutdown().unwrap();
}

#[tokio::test]
async fn disabled_route_refuses_new_operations_and_retains_old_drain() {
    let client = MetricsClient::new(MetricsConfig::in_memory(
        "test",
        "disabled",
        "1",
        opentelemetry_sdk::metrics::InMemoryMetricExporter::default(),
    ))
    .unwrap();
    let route = Arc::new(crate::metrics::route::MetricsRoute::new(Arc::clone(
        &client.original_inner().unwrap(),
    )));
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let held = Arc::clone(&route);
    let worker = std::thread::spawn(move || {
        held.with_inner(|inner| {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            inner.counter("admitted.before.disable", None, 1, &[])
        })
    });
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let ticket = route.disable().unwrap();
    assert!(matches!(
        route.with_inner::<()>(|_| panic!("disabled operation entered")),
        Err(MetricsError::ExporterDisabled)
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(10), ticket.wait())
            .await
            .is_err()
    );
    route.disable().unwrap().wait().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(10), ticket.wait())
            .await
            .is_err()
    );
    release_tx.send(()).unwrap();
    worker.join().unwrap().unwrap();
    ticket.wait().await.unwrap();
    route
        .replace(Arc::clone(&client.original_inner().unwrap()))
        .unwrap()
        .wait()
        .await
        .unwrap();
    route
        .with_inner(|inner| inner.counter("reenabled", None, 1, &[]))
        .unwrap();
    client.shutdown().unwrap();
}

struct CallbackReader(Box<dyn Fn() + Send + Sync>);

#[derive(Debug)]
struct FailFirstStopReader(Arc<AtomicUsize>);

impl MetricReader for FailFirstStopReader {
    fn register_pipeline(&self, _pipeline: Weak<Pipeline>) {}
    fn collect(&self, _metrics: &mut ResourceMetrics) -> OTelSdkResult {
        Ok(())
    }
    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }
    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(OTelSdkError::InternalFailure("first stop fails".to_owned()))
        } else {
            Ok(())
        }
    }
    fn temporality(&self, _kind: InstrumentKind) -> Temporality {
        Temporality::Cumulative
    }
}

#[test]
fn metrics_owner_failure_reobservation_preserves_sole_sdk_attempt() {
    use crate::metrics::owner::MetricsOwnerError;
    let stops = Arc::new(AtomicUsize::new(0));
    let provider = SdkMeterProvider::builder()
        .with_reader(FailFirstStopReader(Arc::clone(&stops)))
        .build();
    let inner = Arc::new(MetricsClientInner {
        meter: provider.meter("failed-first-stop"),
        meter_provider: provider,
        counters: Mutex::new(HashMap::new()),
        gauges: Mutex::new(HashMap::new()),
        histograms: Mutex::new(HashMap::new()),
        duration_histograms: Mutex::new(HashMap::new()),
        runtime_reader: None,
        runtime_only_metrics: &[],
        default_tags: BTreeMap::new(),
    });
    let weak = Arc::downgrade(&inner);
    let owner = crate::metrics::owner::MetricsOwner::new(inner);
    let first = owner.attempt().unwrap();
    assert_eq!(first.run(), Err(MetricsOwnerError::ExporterFailed));
    assert!(weak.upgrade().is_some());
    let retry = owner.attempt().unwrap();
    assert!(Arc::ptr_eq(&first, &retry));
    assert_eq!(retry.run(), Err(MetricsOwnerError::ExporterFailed));
    assert_eq!(
        stops.load(Ordering::SeqCst),
        1,
        "SDK never calls the otherwise-successful second stop"
    );
    assert_eq!(first.run(), Err(MetricsOwnerError::ExporterFailed));
    assert!(
        weak.upgrade().is_some(),
        "unproved shutdown retains custody"
    );
}

impl std::fmt::Debug for CallbackReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CallbackReader")
    }
}

impl MetricReader for CallbackReader {
    fn register_pipeline(&self, _pipeline: Weak<Pipeline>) {}
    fn collect(&self, _metrics: &mut ResourceMetrics) -> OTelSdkResult {
        Ok(())
    }
    fn force_flush(&self) -> OTelSdkResult {
        (self.0)();
        Ok(())
    }
    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        Ok(())
    }
    fn temporality(&self, _kind: InstrumentKind) -> Temporality {
        Temporality::Cumulative
    }
}

fn callback_owner(
    callback: impl Fn() + Send + Sync + 'static,
) -> Arc<crate::metrics::owner::MetricsOwner> {
    let provider = SdkMeterProvider::builder()
        .with_reader(CallbackReader(Box::new(callback)))
        .build();
    crate::metrics::owner::MetricsOwner::new(Arc::new(MetricsClientInner {
        meter: provider.meter("owner-callback"),
        meter_provider: provider,
        counters: Mutex::new(HashMap::new()),
        gauges: Mutex::new(HashMap::new()),
        histograms: Mutex::new(HashMap::new()),
        duration_histograms: Mutex::new(HashMap::new()),
        runtime_reader: None,
        runtime_only_metrics: &[],
        default_tags: BTreeMap::new(),
    }))
}

#[test]
fn metrics_owner_concurrent_and_sdk_reentrant_calls_refuse_until_completion() {
    use crate::metrics::owner::MetricsOwnerError;
    use crate::metrics::owner::MetricsShutdownAttempt;
    let receipt = Arc::new(Mutex::new(Weak::<MetricsShutdownAttempt>::new()));
    let callback_receipt = Arc::clone(&receipt);
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let owner = callback_owner(move || {
        let attempt = callback_receipt.lock().unwrap().upgrade().unwrap();
        assert_eq!(attempt.run(), Err(MetricsOwnerError::InProgress));
        assert_eq!(attempt.run_observing(), Err(MetricsOwnerError::InProgress));
        entered_tx.send(()).unwrap();
        release_rx.lock().unwrap().recv().unwrap();
    });
    let attempt = owner.attempt().unwrap();
    *receipt.lock().unwrap() = Arc::downgrade(&attempt);
    let executing = Arc::clone(&attempt);
    let worker = std::thread::spawn(move || executing.shutdown_public());
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(attempt.run(), Err(MetricsOwnerError::InProgress));
    let observing = Arc::clone(&attempt);
    let observer = std::thread::spawn(move || observing.run_observing());
    assert!(matches!(attempt.shutdown_public(),
        Err(MetricsError::ProviderShutdown { source: OTelSdkError::InternalFailure(message) })
            if message == "metrics shutdown is in progress"));
    assert_eq!(
        owner.attempt().unwrap().run(),
        Err(MetricsOwnerError::InProgress)
    );
    release_tx.send(()).unwrap();
    worker.join().unwrap().unwrap();
    assert_eq!(observer.join().unwrap(), Ok(()));
    assert_eq!(attempt.run(), Ok(()));
}

#[test]
fn metrics_owner_sdk_panic_is_sticky_across_reobservation() {
    use crate::metrics::owner::MetricsOwnerError;
    let owner = callback_owner(|| panic!("SDK callback panic"));
    let attempt = owner.attempt().unwrap();
    assert_eq!(attempt.run(), Err(MetricsOwnerError::Panicked));
    assert_eq!(attempt.run(), Err(MetricsOwnerError::Panicked));
    let retry = owner.attempt().unwrap();
    assert!(Arc::ptr_eq(&attempt, &retry));
    assert_eq!(attempt.run(), Err(MetricsOwnerError::Panicked));
}

#[test]
fn metrics_owner_public_and_managed_results_keep_distinct_replay_semantics() {
    use crate::metrics::owner::MetricsOwnerError;
    let public_first = callback_owner(|| {});
    let receipt = public_first.attempt().unwrap();
    receipt.shutdown_public().unwrap();
    assert_eq!(receipt.run(), Ok(()));
    assert!(matches!(
        receipt.shutdown_public(),
        Err(MetricsError::ProviderShutdown {
            source: OTelSdkError::AlreadyShutdown
        })
    ));

    let managed_first = callback_owner(|| {});
    let receipt = managed_first.attempt().unwrap();
    assert_eq!(receipt.run(), Ok(()));
    assert!(matches!(
        receipt.shutdown_public(),
        Err(MetricsError::ProviderShutdown {
            source: OTelSdkError::AlreadyShutdown
        })
    ));

    let failed = callback_owner(|| panic!("private failure sentinel"));
    let receipt = failed.attempt().unwrap();
    let error = receipt.shutdown_public().unwrap_err();
    assert!(!error.to_string().contains("private failure sentinel"));
    assert_eq!(receipt.run(), Err(MetricsOwnerError::Panicked));
    assert!(matches!(
        receipt.shutdown_public(),
        Err(MetricsError::ProviderShutdown {
            source: OTelSdkError::AlreadyShutdown
        })
    ));
}

#[test]
fn metrics_owner_success_releases_exporter_and_replays_exact_attempt() {
    let client = MetricsClient::new(MetricsConfig::in_memory(
        "test",
        "owner",
        "1",
        opentelemetry_sdk::metrics::InMemoryMetricExporter::default(),
    ))
    .unwrap();
    let weak = Arc::downgrade(&client.original_inner().unwrap());
    let owner =
        crate::metrics::owner::MetricsOwner::new(Arc::clone(&client.original_inner().unwrap()));
    drop(client);
    let attempt = owner.attempt().unwrap();
    assert_eq!(attempt.run(), Ok(()));
    assert!(weak.upgrade().is_none());
    assert!(Arc::ptr_eq(&attempt, &owner.attempt().unwrap()));
    drop(owner);
    assert_eq!(attempt.run(), Ok(()));
}

#[test]
fn standalone_metrics_shutdown_repeat_preserves_sdk_error() {
    let client = MetricsClient::new(MetricsConfig::in_memory(
        "test",
        "standalone-repeat",
        "1",
        opentelemetry_sdk::metrics::InMemoryMetricExporter::default(),
    ))
    .unwrap();
    let clone = client.clone();
    client.shutdown().unwrap();
    assert!(matches!(
        clone.shutdown(),
        Err(MetricsError::ProviderShutdown {
            source: OTelSdkError::AlreadyShutdown
        }),
    ));
}

#[tokio::test]
async fn global_retirement_requires_exact_removed_owner() {
    let make = |name| {
        MetricsClient::new(MetricsConfig::in_memory(
            "test",
            name,
            "1",
            opentelemetry_sdk::metrics::InMemoryMetricExporter::default(),
        ))
        .unwrap()
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let first = crate::metrics::install_global(make("exact-a")).unwrap();
    assert!(crate::metrics::retire_for(&first, deadline).await.is_err());
    first.counter("still.active", 1, &[]).unwrap();
    let second = crate::metrics::install_global(make("exact-b")).unwrap();
    crate::metrics::retire_for(&first, deadline).await.unwrap();
    crate::metrics::retire_for(&first, tokio::time::Instant::now())
        .await
        .unwrap();
    assert!(crate::metrics::retire_for(&second, deadline).await.is_err());
    second.counter("b.not.retired", 1, &[]).unwrap();
    assert!(matches!(
        first.shutdown(),
        Err(MetricsError::ProviderShutdown {
            source: OTelSdkError::AlreadyShutdown
        })
    ));
    second.shutdown().unwrap();
}

#[tokio::test]
async fn global_disable_retires_old_owner_and_cached_handles_follow_reenable() {
    let make = |name| {
        MetricsClient::new(
            MetricsConfig::in_memory(
                "test",
                name,
                "1",
                opentelemetry_sdk::metrics::InMemoryMetricExporter::default(),
            )
            .with_runtime_reader(),
        )
        .unwrap()
    };
    let first = make("disable-a");
    let old = Arc::downgrade(&first.original_inner().unwrap());
    let first = crate::metrics::install_global(first).unwrap();
    assert!(crate::metrics::install_global(first.clone()).is_err());
    first.counter("a.before.disable", 1, &[]).unwrap();
    crate::metrics::disable_global().unwrap();
    crate::metrics::disable_global().unwrap();
    assert!(crate::metrics::global().is_none());
    assert!(matches!(
        first.counter("a.after.disable", 1, &[]),
        Err(MetricsError::ExporterDisabled)
    ));
    crate::metrics::retire_for(&first, tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(old.upgrade().is_none());
    let second = crate::metrics::install_global(make("disable-b")).unwrap();
    first.counter("b.from.cached.a", 1, &[]).unwrap();
    assert!(
        second
            .snapshot()
            .unwrap()
            .scope_metrics()
            .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
            .any(|metric| metric.name() == "b.from.cached.a")
    );
    second.shutdown().unwrap();
}

#[tokio::test]
async fn cached_global_owner_shutdown_never_targets_replacement() {
    let make = |name| {
        MetricsClient::new(
            MetricsConfig::in_memory(
                "test",
                name,
                "1",
                opentelemetry_sdk::metrics::InMemoryMetricExporter::default(),
            )
            .with_runtime_reader(),
        )
        .unwrap()
    };
    let first = make("global-owner-a");
    let weak = Arc::downgrade(&first.original_inner().unwrap());
    let first = crate::metrics::install_global(first).unwrap();
    let cached = crate::metrics::global().unwrap();
    let second = crate::metrics::install_global(make("global-owner-b")).unwrap();
    let ticket = crate::metrics::GLOBAL_METRICS
        .lock()
        .unwrap()
        .retired
        .last()
        .unwrap()
        .ticket
        .clone();
    ticket.wait().await.unwrap();
    cached.shutdown().unwrap();
    cached.shutdown_for_provider().unwrap();
    assert!(
        weak.upgrade().is_none(),
        "cached handles must not pin retired exporter"
    );
    assert!(matches!(
        first.shutdown(),
        Err(MetricsError::ProviderShutdown {
            source: OTelSdkError::AlreadyShutdown
        })
    ));
    cached.counter("after.a.shutdown", 1, &[]).unwrap();
    let snapshot = second.snapshot().unwrap();
    assert!(
        snapshot
            .scope_metrics()
            .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
            .any(|metric| metric.name() == "after.a.shutdown")
    );
    second.shutdown().unwrap();
}

impl MetricReader for FailingFlushReader {
    fn register_pipeline(&self, _pipeline: Weak<Pipeline>) {}

    fn collect(&self, _metrics: &mut ResourceMetrics) -> OTelSdkResult {
        Ok(())
    }

    fn force_flush(&self) -> OTelSdkResult {
        Err(OTelSdkError::InternalFailure("flush sentinel".to_owned()))
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        self.shutdowns.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn temporality(&self, _kind: InstrumentKind) -> Temporality {
        Temporality::Cumulative
    }
}

#[test]
fn failed_flush_still_stops_reader_and_remains_failure() {
    let shutdowns = Arc::new(AtomicUsize::new(0));
    let provider = SdkMeterProvider::builder()
        .with_reader(FailingFlushReader {
            shutdowns: Arc::clone(&shutdowns),
        })
        .build();
    let client = MetricsClientInner {
        meter: provider.meter("retirement-test"),
        meter_provider: provider,
        counters: Mutex::new(HashMap::new()),
        gauges: Mutex::new(HashMap::new()),
        histograms: Mutex::new(HashMap::new()),
        duration_histograms: Mutex::new(HashMap::new()),
        runtime_reader: None,
        runtime_only_metrics: &[],
        default_tags: BTreeMap::new(),
    };
    let result = client.shutdown();
    assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
    let Err(MetricsError::ProviderShutdown { source }) = result else {
        panic!("flush failure must remain observable after shutdown");
    };
    assert!(source.to_string().contains("flush sentinel"));
    let inner = Arc::new(client);
    let weak = Arc::downgrade(&inner);
    let owner = crate::metrics::owner::MetricsOwner::new(inner);
    let first = owner.attempt().unwrap();
    assert_eq!(
        first.run(),
        Err(crate::metrics::owner::MetricsOwnerError::ExporterFailed)
    );
    assert!(weak.upgrade().is_some(), "failed proof must retain custody");
    let retry = owner.attempt().unwrap();
    assert!(Arc::ptr_eq(&first, &retry));
    assert_eq!(
        first.run(),
        Err(crate::metrics::owner::MetricsOwnerError::ExporterFailed)
    );
    assert_eq!(
        retry.run(),
        Err(crate::metrics::owner::MetricsOwnerError::ExporterFailed)
    );
    drop(owner);
    assert!(weak.upgrade().is_none(), "receipts must hold no exporter");
}
