use super::*;
use crate::MetricsClient;
use crate::MetricsConfig;
use pretty_assertions::assert_eq;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

struct DropProbe {
    route: Weak<MetricsRoute>,
    result: Arc<AtomicUsize>,
}

impl Drop for DropProbe {
    fn drop(&mut self) {
        let route = self.route.upgrade().unwrap();
        let unlocked = route.current.try_write().is_ok() && route.retired.try_lock().is_ok();
        self.result
            .store(if unlocked { 1 } else { 2 }, Ordering::SeqCst);
    }
}

fn client() -> MetricsClient {
    MetricsClient::new(
        MetricsConfig::in_memory(
            "test",
            "route-drop",
            "1",
            opentelemetry_sdk::metrics::InMemoryMetricExporter::default(),
        )
        .with_runtime_reader(),
    )
    .unwrap()
}

#[tokio::test]
async fn publication_guard_can_abort_without_mutation_and_commit_exact_epoch() {
    let old = client();
    let next = client();
    let old_inner = old.original_inner().unwrap();
    let next_inner = next.original_inner().unwrap();
    let route = MetricsRoute::new(Arc::clone(&old_inner));
    let guard = route.publication_guard().unwrap();
    assert!(route.current.try_read().is_err());
    assert!(route.retired.try_lock().is_err());
    drop(guard);
    assert!(Arc::ptr_eq(
        &route.current.read().unwrap().as_ref().unwrap().inner,
        &old_inner
    ));
    assert!(route.retired.lock().unwrap().is_empty());
    let ticket = {
        let mut guard = route.publication_guard().unwrap();
        let ticket = guard.replace(Some(Arc::clone(&next_inner)));
        assert!(Arc::ptr_eq(
            &guard.current.as_ref().unwrap().inner,
            &next_inner
        ));
        assert_eq!(guard.retired.len(), 1);
        assert!(Arc::ptr_eq(&guard.retired[0].inner, &old_inner));
        ticket
    };
    ticket.wait().await.unwrap();
    assert!(route.retired.lock().unwrap().is_empty());
    old.shutdown().unwrap();
    next.shutdown().unwrap();
}

#[tokio::test]
async fn retired_sdk_callback_drops_outside_route_locks() {
    for replace_again in [false, true] {
        let old = client();
        let next = client();
        let route = Arc::new(MetricsRoute::new(old.original_inner().unwrap()));
        let result = Arc::new(AtomicUsize::new(0));
        let probe = DropProbe {
            route: Arc::downgrade(&route),
            result: Arc::clone(&result),
        };
        old.register_observable_gauge_with_description(
            "drop.probe",
            "test",
            move || {
                // Keep the probe in the SDK-owned callback, not in this stack.
                let _ = &probe;
                1
            },
            &[],
        )
        .unwrap();
        let ticket = route.replace(next.original_inner().unwrap()).unwrap();
        drop(old);
        assert_eq!(result.load(Ordering::SeqCst), 0);
        if replace_again {
            let empty = route.disable().unwrap();
            empty.wait().await.unwrap();
            assert_eq!(
                result.load(Ordering::SeqCst),
                0,
                "later publication must not drop the old SDK under its caller's lock"
            );
        }
        ticket.wait().await.unwrap();
        assert_eq!(result.load(Ordering::SeqCst), 1);
        ticket.wait().await.unwrap();
        next.shutdown().unwrap();
    }
}
