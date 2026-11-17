use opentelemetry::Context;
use opentelemetry::global::BoxedSpan;
use opentelemetry::global::BoxedTracer;
use opentelemetry::trace::SpanBuilder;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::Tracer;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::RwLock;
use std::sync::RwLockWriteGuard;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio::time::timeout_at;
use tracing::Subscriber;
use tracing_subscriber::Layer;
use tracing_subscriber::registry::LookupSpan;

struct TraceEpoch {
    tracer: Tracer,
    builds: Arc<watch::Sender<usize>>,
}

struct BuildLease(Arc<TraceEpoch>);

impl Drop for BuildLease {
    fn drop(&mut self) {
        self.0.builds.send_modify(|count| *count -= 1);
    }
}

/// Exact retired build population, without retaining a tracer or exporter.
#[derive(Clone)]
#[must_use = "drain selected builds before shutting down the retired provider"]
pub(crate) struct TraceDrainTicket(Arc<watch::Sender<usize>>);

impl TraceDrainTicket {
    pub(crate) async fn wait_until(
        &self,
        deadline: Instant,
    ) -> Result<(), crate::OtelRetirementError> {
        let mut builds = self.0.subscribe();
        loop {
            if *builds.borrow_and_update() == 0 {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(crate::OtelRetirementError::TimedOut);
            }
            timeout_at(deadline, builds.changed())
                .await
                .map_err(|_| crate::OtelRetirementError::TimedOut)?
                .map_err(|_| {
                    crate::OtelRetirementError::Exporter(crate::OtelShutdownError::StateUnavailable)
                })?;
        }
    }
}

/// Local publication authority; SDK globals are not part of this route.
#[derive(Clone, Default)]
pub(crate) struct LocalTraceRoute(Arc<RwLock<Option<Arc<TraceEpoch>>>>);

/// Acquire every publication guard before replacing any local route.
pub(crate) struct TracePublicationGuard<'a>(RwLockWriteGuard<'a, Option<Arc<TraceEpoch>>>);

/// Keeps SDK destruction outside the coordinated publication critical section.
#[must_use = "release publication guards before converting the retired epoch"]
pub(crate) struct RetiredTraceEpoch(Option<Arc<TraceEpoch>>);

impl RetiredTraceEpoch {
    pub(crate) fn into_drain_ticket(self) -> TraceDrainTicket {
        TraceDrainTicket(self.0.as_ref().map_or_else(
            || Arc::new(watch::channel(0).0),
            |epoch| Arc::clone(&epoch.builds),
        ))
    }
}

impl TracePublicationGuard<'_> {
    pub(crate) fn replace(&mut self, tracer: Option<Tracer>) -> RetiredTraceEpoch {
        let next = tracer.map(|tracer| {
            Arc::new(TraceEpoch {
                tracer,
                builds: Arc::new(watch::channel(0).0),
            })
        });
        RetiredTraceEpoch(std::mem::replace(&mut *self.0, next))
    }
}

impl LocalTraceRoute {
    pub(crate) fn publication_guard(
        &self,
    ) -> Result<TracePublicationGuard<'_>, crate::OtelShutdownError> {
        self.0
            .write()
            .map(TracePublicationGuard)
            .map_err(|_| crate::OtelShutdownError::StateUnavailable)
    }

    #[cfg(test)]
    pub(crate) fn replace(
        &self,
        tracer: Option<Tracer>,
    ) -> Result<TraceDrainTicket, crate::OtelShutdownError> {
        let previous = {
            let mut publication = self.publication_guard()?;
            publication.replace(tracer)
        };
        Ok(previous.into_drain_ticket())
    }

    fn with_tracer<T>(&self, operation: impl FnOnce(Option<&Tracer>) -> T) -> T {
        // Register under the selection guard, but never hold that guard through
        // SDK callbacks. Replacement cannot miss a selected build; reentrant
        // callbacks can select the new generation without recursive locking.
        let lease = self.0.read().ok().and_then(|current| {
            let epoch = current.as_ref()?;
            let mut admitted = false;
            epoch.builds.send_modify(|count| {
                if let Some(next) = count.checked_add(1) {
                    *count = next;
                    admitted = true;
                }
            });
            admitted.then(|| BuildLease(Arc::clone(epoch)))
        });
        operation(lease.as_ref().map(|lease| &lease.0.tracer))
    }

    pub(crate) fn layer<S>(&self) -> impl Layer<S> + Send + Sync
    where
        S: Subscriber + for<'span> LookupSpan<'span> + Send + Sync,
    {
        tracing_opentelemetry::layer()
            .with_tracer(self.clone())
            .and_then(EagerContext {
                dispatch: OnceLock::new(),
            })
    }
}

impl opentelemetry::trace::Tracer for LocalTraceRoute {
    type Span = BoxedSpan;
    fn build_with_context(&self, builder: SpanBuilder, parent: &Context) -> Self::Span {
        // Consume before SDK callbacks so a reentrant build cannot reuse it.
        let root_parent = crate::root_span_context::take_build_parent();
        let parent = root_parent.as_ref().unwrap_or(parent);
        self.with_tracer(|selected| {
            let tracer = match selected {
                Some(tracer) => BoxedTracer::new(Box::new(tracer.clone())),
                None => BoxedTracer::new(Box::new(
                    opentelemetry::trace::noop::NoopTracerProvider::new().tracer("disabled"),
                )),
            };
            tracer.build_with_context(builder, parent)
        })
    }
}

struct EagerContext {
    dispatch: OnceLock<tracing::dispatcher::WeakDispatch>,
}

impl<S> Layer<S> for EagerContext
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    fn on_register_dispatch(&self, dispatch: &tracing::Dispatch) {
        let _ = self.dispatch.set(dispatch.downgrade());
    }

    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        // and_then invokes the OTEL layer first. Force its deferred builder
        // now, before creation returns, so an A-open/B-close span stays on A.
        if let Some(span) = ctx.span(id)
            && let Some(dispatch) = self
                .dispatch
                .get()
                .and_then(tracing::dispatcher::WeakDispatch::upgrade)
        {
            let parent = if attrs.is_root() {
                crate::root_span_context::take_root_parent()
            } else {
                None
            };
            crate::root_span_context::with_root_build_parent(parent, || {
                let _ =
                    tracing_opentelemetry::get_otel_context(&mut span.extensions_mut(), &dispatch);
            });
        }
    }
}

#[cfg(test)]
#[path = "local_trace_route_tests.rs"]
mod tests;
