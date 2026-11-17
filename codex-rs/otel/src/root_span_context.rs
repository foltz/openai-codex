use codex_protocol::protocol::W3cTraceContext;
use opentelemetry::Context;
use std::cell::Cell;
use std::thread::LocalKey;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

thread_local! {
    static ROOT_PARENT: Cell<Option<Context>> = const { Cell::new(None) };
    static BUILD_PARENT: Cell<Option<Context>> = const { Cell::new(None) };
}

struct ParentScope {
    slot: &'static LocalKey<Cell<Option<Context>>>,
    previous: Option<Context>,
}

impl ParentScope {
    fn install(slot: &'static LocalKey<Cell<Option<Context>>>, parent: Option<Context>) -> Self {
        Self {
            slot,
            previous: slot.with(|current| current.replace(parent)),
        }
    }
}

impl Drop for ParentScope {
    fn drop(&mut self) {
        self.slot.with(|current| current.set(self.previous.take()));
    }
}

/// Creates an explicit tracing root linked to a W3C parent, without retaining
/// the ambient tracing span. The closure must create and return one
/// `parent: None` span without entering it. Nested calls have independent scopes.
/// Managed telemetry selects its generation during span creation as usual;
/// plain OTEL layers receive the parent before their deferred builder starts.
/// This propagates W3C span identity, not arbitrary context baggage.
pub fn root_span_with_w3c_parent(
    parent: Option<&W3cTraceContext>,
    create: impl FnOnce() -> Span,
) -> Span {
    let parent = parent.and_then(crate::context_from_w3c_trace_context);
    let _scope = ParentScope::install(&ROOT_PARENT, parent);
    let span = create();
    // A plain layer (or a filtered span) never consumes the eager root token.
    if let Some(parent) = take_root_parent() {
        let _ = span.set_parent(parent);
    }
    span
}

pub(crate) fn take_root_parent() -> Option<Context> {
    ROOT_PARENT.with(Cell::take)
}

pub(crate) fn with_root_build_parent<T>(parent: Option<Context>, build: impl FnOnce() -> T) -> T {
    let scope = ParentScope::install(&BUILD_PARENT, parent);
    let result = build();
    let unconsumed = take_build_parent().is_some();
    drop(scope);
    if unconsumed {
        tracing::debug!("explicit root parent did not reach the eager trace build");
    }
    result
}

pub(crate) fn take_build_parent() -> Option<Context> {
    BUILD_PARENT.with(Cell::take)
}
