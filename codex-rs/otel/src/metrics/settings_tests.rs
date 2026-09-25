use super::*;
use pretty_assertions::assert_eq;

fn metrics() -> MetricsClient {
    MetricsClient::new(MetricsConfig::in_memory(
        "test",
        "metadata",
        "1",
        opentelemetry_sdk::metrics::InMemoryMetricExporter::default(),
    ))
    .unwrap()
}

#[test]
fn legacy_replacement_releases_shutdown_original_without_managed_observer() {
    let slot = Mutex::new(GlobalMetrics::default());
    let first = metrics();
    let original = Arc::downgrade(&first.original_inner().unwrap());
    let first = install_in(&slot, first, None).unwrap();
    let second = install_in(&slot, metrics(), None).unwrap();
    first.shutdown_for_provider().unwrap();
    drop(first);
    assert!(
        original.upgrade().is_none(),
        "legacy replacement must not require a managed observer to release the old exporter"
    );
    second.counter("replacement.remains.live", 1, &[]).unwrap();
    second.shutdown().unwrap();
}

#[test]
fn legacy_replacement_keeps_inflight_lease_until_operation_returns() {
    let slot = Mutex::new(GlobalMetrics::default());
    let first = metrics();
    let original = Arc::downgrade(&first.original_inner().unwrap());
    let first = install_in(&slot, first, None).unwrap();
    let route = slot.lock().unwrap().route.clone().unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let operation = std::thread::spawn(move || {
        route.with_inner(|_| {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok(())
        })
    });
    entered_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    let second = install_in(&slot, metrics(), None).unwrap();
    first.shutdown_for_provider().unwrap();
    drop(first);
    let retained_during_operation = original.upgrade().is_some();
    release_tx.send(()).unwrap();
    operation.join().unwrap().unwrap();
    assert!(retained_during_operation);
    assert!(original.upgrade().is_none());
    second.counter("replacement.remains.live", 1, &[]).unwrap();
    second.shutdown().unwrap();
}

#[test]
fn statsig_metadata_replaces_with_the_metrics_owner() {
    let slot = Mutex::new(GlobalMetrics::default());
    for environment in [Some("account-a"), Some("account-b"), None] {
        let settings = environment.map(|environment| StatsigMetricsSettings {
            environment: environment.to_owned(),
        });
        let client = install_in(&slot, metrics(), settings.clone()).unwrap();
        let current = slot.lock().unwrap();
        assert_eq!(current.statsig, settings);
        let client::MetricsOriginal::Routed(expected) = &client.inner else {
            panic!("routed owner");
        };
        let client::MetricsOriginal::Routed(actual) = &current.current.as_ref().unwrap().inner
        else {
            panic!("routed owner");
        };
        assert!(Arc::ptr_eq(expected, actual));
        drop(current);
        client.shutdown().unwrap();
    }
}

#[tokio::test]
async fn coordinated_publication_preserves_failed_candidate_and_retires_exact_owner() {
    let slot = Mutex::new(GlobalMetrics::default());
    let mut candidate = Some(metrics());
    let mut publication = GlobalMetricsPublication(slot.lock().unwrap());
    publication
        .publish_with(&mut candidate, None, || assert!(slot.try_lock().is_err()))
        .unwrap();
    assert!(publication.0.retired.is_empty());
    let first = candidate.take().unwrap();
    let mut invalid = Some(first.clone());
    assert!(matches!(
        publication.publish_with(&mut invalid, None, || panic!("must not commit")),
        Err(MetricsError::RoutingUnavailable)
    ));
    assert!(invalid.is_some());
    assert!(publication.0.retired.is_empty());

    let mut second = Some(metrics());
    publication
        .publish_with(
            &mut second,
            Some(StatsigMetricsSettings {
                environment: "b".to_owned(),
            }),
            || assert!(slot.try_lock().is_err()),
        )
        .unwrap();
    assert_eq!(publication.0.retired.len(), 1);
    assert_eq!(publication.0.statsig.as_ref().unwrap().environment, "b");
    let client::MetricsOriginal::Routed(first_owner) = &first.inner else {
        panic!("routed owner");
    };
    let retired = Arc::clone(&publication.0.retired[0]);
    assert!(retired.belongs_to(first_owner));
    drop(publication);
    assert_eq!(
        retired
            .wait_until(tokio::time::Instant::now() + std::time::Duration::from_secs(2))
            .await,
        Ok(())
    );
    second.unwrap().shutdown().unwrap();
}

#[test]
fn poisoned_publication_preserves_prior_metadata_and_owner() {
    let slot = Mutex::new(GlobalMetrics::default());
    let prior = install_in(
        &slot,
        metrics(),
        Some(StatsigMetricsSettings {
            environment: "prior".to_owned(),
        }),
    )
    .unwrap();
    let _ = std::panic::catch_unwind(|| {
        let _guard = slot.lock().unwrap();
        panic!("poison fixture");
    });
    assert!(matches!(
        install_in(&slot, metrics(), None),
        Err(MetricsError::RoutingUnavailable)
    ));
    let current = match slot.lock() {
        Ok(_) => panic!("expected poison"),
        Err(error) => error.into_inner(),
    };
    assert_eq!(current.statsig.as_ref().unwrap().environment, "prior");
    let client::MetricsOriginal::Routed(expected) = &prior.inner else {
        panic!("routed owner");
    };
    let client::MetricsOriginal::Routed(actual) = &current.current.as_ref().unwrap().inner else {
        panic!("routed owner");
    };
    assert!(Arc::ptr_eq(expected, actual));
    drop(current);
    prior.shutdown().unwrap();
}
