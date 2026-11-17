use super::TracestateEntries;
use super::merge_tracestate_snapshot;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tracing::Subscriber;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;

struct ReentrantPublication {
    entries: Arc<RwLock<TracestateEntries>>,
    observed: Arc<AtomicBool>,
}

impl<S: Subscriber> Layer<S> for ReentrantPublication {
    fn on_event(&self, _: &tracing::Event<'_>, _: Context<'_, S>) {
        let mut entries = self
            .entries
            .try_write()
            .expect("merge must release read guard");
        entries.clear();
        self.observed.store(true, Ordering::Release);
    }
}

#[test]
fn propagation_warning_allows_reentrant_publication_without_changing_snapshot() {
    let entries = Arc::new(RwLock::new(TracestateEntries::from([(
        "vendor".to_owned(),
        [("account".to_owned(), "a".to_owned())].into(),
    )])));
    let expected = super::merge_tracestate_entries(None, &entries.read().unwrap());
    let observed = Arc::new(AtomicBool::new(false));
    let subscriber = tracing_subscriber::registry().with(ReentrantPublication {
        entries: Arc::clone(&entries),
        observed: Arc::clone(&observed),
    });
    let result = tracing::subscriber::with_default(subscriber, || {
        merge_tracestate_snapshot(Some("invalid member"), &entries)
    });
    pretty_assertions::assert_eq!(result, expected);
    assert!(observed.load(Ordering::Acquire));
    assert!(entries.read().unwrap().is_empty());
}
