use crate::OtelProvider;
use crate::OtelRetirement;
use crate::OtelRetirementError;
use crate::OtelShutdownError;
use crate::PreparedOtelProvider;
use crate::local_trace_route::LocalTraceRoute;
use crate::local_trace_route::TraceDrainTicket;
use tokio::time::Instant;
use tracing::Subscriber;
use tracing_subscriber::Layer;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::reload;

type ExportLayer<S> = Option<Box<dyn Layer<S> + Send + Sync + 'static>>;
type Layers<S> = Box<dyn Layer<S> + Send + Sync + 'static>;

/// Permanent local routes for one managed app-server subscriber. SDK global
/// tracer publication is deliberately not part of this authority.
pub struct ManagedTelemetryRoutes<S> {
    trace: LocalTraceRoute,
    logger: reload::Handle<ExportLayer<S>, S>,
}

/// Successful route publication, not exporter-retirement acknowledgement.
#[must_use = "retain the provider and drain retired trace builds before exporter shutdown"]
pub struct TelemetryPublication {
    pub provider: Option<OtelProvider>,
    trace: TraceDrainTicket,
}

enum RetiredState {
    Draining(Option<OtelProvider>),
    Exporters(OtelRetirement),
    Complete(Result<(), OtelRetirementError>),
}

/// Owns one removed provider through trace selection, metrics, and exporter
/// retirement. A timeout drops only an observer, not any provider or worker.
#[must_use = "retain until exact retirement is observed"]
pub struct RetiredTelemetry {
    trace: TraceDrainTicket,
    state: tokio::sync::Mutex<RetiredState>,
}

impl RetiredTelemetry {
    /// Re-observe the same retirement under the caller's absolute deadline.
    #[expect(
        clippy::await_holding_invalid_type,
        reason = "exclusive observer poll right retains the provider across cancellation; workers never acquire this mutex"
    )]
    pub async fn wait_until(&self, deadline: Instant) -> Result<(), OtelRetirementError> {
        let mut state = tokio::time::timeout_at(deadline, self.state.lock())
            .await
            .map_err(|_| OtelRetirementError::TimedOut)?;
        if let RetiredState::Complete(result) = *state {
            return result;
        }
        if Instant::now() >= deadline {
            return Err(OtelRetirementError::TimedOut);
        }
        if let RetiredState::Draining(previous) = &mut *state {
            self.trace.wait_until(deadline).await?;
            if let Some(provider) = previous.as_ref() {
                provider.retire_metrics_until(deadline).await?;
            }
            if Instant::now() >= deadline {
                return Err(OtelRetirementError::TimedOut);
            }
            *state = match previous.take() {
                Some(provider) => RetiredState::Exporters(provider.begin_retirement()),
                None => RetiredState::Complete(Ok(())),
            };
        }
        if let RetiredState::Exporters(worker) = &*state {
            let result = worker.wait_until(deadline).await;
            if result == Err(OtelRetirementError::TimedOut) {
                return result;
            }
            *state = RetiredState::Complete(result);
        }
        match *state {
            RetiredState::Complete(result) => result,
            RetiredState::Draining(_) | RetiredState::Exporters(_) => {
                unreachable!("normalized above")
            }
        }
    }
}

impl TelemetryPublication {
    /// The serialized reloader supplies the provider displaced by this commit.
    /// Transfer it immediately, before any cancellable retirement observation.
    pub fn retire_previous(
        self,
        previous: Option<OtelProvider>,
    ) -> (Option<OtelProvider>, RetiredTelemetry) {
        (
            self.provider,
            RetiredTelemetry {
                trace: self.trace,
                state: tokio::sync::Mutex::new(RetiredState::Draining(previous)),
            },
        )
    }
    /// Observe only the exact trace-build population removed by this commit.
    /// The caller must separately observe old metrics and exporter retirement.
    pub async fn drain_trace_until(&self, deadline: Instant) -> Result<(), OtelRetirementError> {
        self.trace.wait_until(deadline).await
    }
}

impl<S> ManagedTelemetryRoutes<S>
where
    S: Subscriber + for<'span> LookupSpan<'span> + Send + Sync + 'static,
{
    /// Install both layers even when initially disabled. Publication is allowed
    /// only once this layer collection belongs to a live subscriber.
    pub fn layers() -> (Layers<S>, Self) {
        let trace = LocalTraceRoute::default();
        let (logger, handle) = reload::Layer::new(None::<Box<dyn Layer<S> + Send + Sync>>);
        // Vec<Layer> does not forward on_register_dispatch in the pinned
        // tracing-subscriber. Structural composition must deliver that hook
        // to EagerContext so it can activate spans at creation, not at close.
        let layers = logger
            .with_filter(tracing_subscriber::filter::filter_fn(
                OtelProvider::log_export_filter,
            ))
            .and_then(
                trace
                    .layer()
                    .with_filter(tracing_subscriber::filter::filter_fn(
                        OtelProvider::trace_export_filter,
                    )),
            )
            .boxed();
        (
            layers,
            Self {
                trace,
                logger: handle,
            },
        )
    }

    /// Atomically publish local routes under the logger write fence. Errors
    /// retain the unpublished candidate for explicit retirement by its owner.
    /// Serialize calls in the reloader's authoritative-generation transaction.
    pub fn publish(
        &self,
        candidate: &mut Option<PreparedOtelProvider>,
    ) -> Result<TelemetryPublication, OtelShutdownError> {
        let prepared = candidate
            .as_mut()
            .ok_or(OtelShutdownError::StateUnavailable)?;
        let next_logger = prepared
            .provider
            .as_ref()
            .and_then(OtelProvider::logger_export_layer)
            .map(Layer::boxed);
        let next_trace = prepared
            .provider
            .as_ref()
            .and_then(|provider| provider.tracer.clone());
        let mut next_logger = next_logger;
        let mut next_trace = next_trace;
        let mut old_logger = None;
        let mut old_trace = None;
        let mut result = Err(OtelShutdownError::StateUnavailable);
        self.logger
            .modify(|logger| {
                result = (|| {
                    let mut metrics = crate::metrics::publication_guard()
                        .map_err(|_| OtelShutdownError::StateUnavailable)?;
                    let mut trace = self.trace.publication_guard()?;
                    let mut propagation = crate::trace_context::tracestate_publication_guard()
                        .map_err(|_| OtelShutdownError::StateUnavailable)?;
                    let mut disabled_metrics = None;
                    let next_metrics = prepared
                        .provider
                        .as_mut()
                        .map_or(&mut disabled_metrics, |provider| &mut provider.metrics);
                    metrics
                        .publish_with(next_metrics, prepared.statsig.clone(), || {
                            old_trace = Some(trace.replace(next_trace.take()));
                            *propagation = std::mem::take(&mut prepared.tracestate);
                            old_logger = std::mem::replace(logger, next_logger.take());
                        })
                        .map_err(|_| OtelShutdownError::StateUnavailable)?;
                    // All guards leave scope before reload rebuilds interest caches.
                    Ok(())
                })();
            })
            .map_err(|_| OtelShutdownError::StateUnavailable)?;
        result?;
        // SDK-containing values are released only outside every route guard.
        drop(old_logger);
        let trace = old_trace
            .ok_or(OtelShutdownError::StateUnavailable)?
            .into_drain_ticket();
        let prepared = candidate
            .take()
            .ok_or(OtelShutdownError::StateUnavailable)?;
        Ok(TelemetryPublication {
            provider: prepared.provider,
            trace,
        })
    }

    /// Close all local telemetry routes while preserving exact prior receipts.
    pub fn disable(&self) -> Result<TelemetryPublication, OtelShutdownError> {
        self.publish(&mut Some(PreparedOtelProvider {
            provider: None,
            tracestate: Default::default(),
            statsig: None,
        }))
    }
}

#[cfg(test)]
#[path = "managed_routes_tests.rs"]
mod tests;
