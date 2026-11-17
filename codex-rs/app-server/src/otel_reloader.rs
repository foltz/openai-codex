use crate::OTEL_SERVICE_NAME;
use crate::config_manager::ConfigManager;
use codex_login::AuthManager;
use codex_otel::OtelProvider;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::Subscriber;
use tracing::info;
use tracing::warn;
use tracing_subscriber::Layer;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::reload;

type OtelExportLayer<S> = Option<Box<dyn Layer<S> + Send + Sync + 'static>>;
type OtelReloadLayers<S> = (
    Vec<Box<dyn Layer<S> + Send + Sync + 'static>>,
    reload::Handle<OtelExportLayer<S>, S>,
);

pub(crate) fn layers<S>(provider: Option<&OtelProvider>) -> OtelReloadLayers<S>
where
    S: Subscriber + for<'span> LookupSpan<'span> + Send + Sync + 'static,
{
    let logger_export_layer: OtelExportLayer<S> = provider
        .and_then(OtelProvider::logger_export_layer)
        .map(Layer::boxed);
    let (logger_layer, logger_handle) = reload::Layer::new(logger_export_layer);

    let layers: Vec<Box<dyn Layer<S> + Send + Sync + 'static>> = vec![
        logger_layer
            .with_filter(tracing_subscriber::filter::filter_fn(
                OtelProvider::log_export_filter,
            ))
            .boxed(),
        // The subscriber graph is fixed after startup. Keep the global tracer
        // bridge installed even when tracing starts disabled so an account
        // switch can install a trace exporter later.
        OtelProvider::reloadable_tracing_layer(OTEL_SERVICE_NAME).boxed(),
    ];
    (layers, logger_handle)
}

pub(crate) fn spawn<S>(
    mut provider: Option<OtelProvider>,
    logger_reload_handle: reload::Handle<OtelExportLayer<S>, S>,
    config_manager: ConfigManager,
    auth_manager: Arc<AuthManager>,
    default_analytics_enabled: bool,
    shutdown_token: CancellationToken,
) -> JoinHandle<()>
where
    S: Subscriber + for<'span> LookupSpan<'span> + Send + Sync + 'static,
{
    let mut auth_changes = auth_manager.auth_change_receiver();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => break,
                changed = auth_changes.changed() => {
                    if changed.is_err() {
                        break;
                    }

                    // Account handlers install the new cloud loader after publishing auth changes.
                    tokio::time::sleep(Duration::from_millis(/*millis*/ 50)).await;

                    let config = match config_manager.load_latest_config(/*fallback_cwd*/ None).await {
                        Ok(config) => config,
                        Err(error) => {
                            warn!(%error, "failed to reload telemetry config after account change");
                            continue;
                        }
                    };
                    let next_provider = match codex_core::otel_init::build_provider(
                        &config,
                        env!("CARGO_PKG_VERSION"),
                        Some(OTEL_SERVICE_NAME),
                        default_analytics_enabled,
                    ) {
                        Ok(provider) => provider,
                        Err(error) => {
                            warn!(%error, "failed to rebuild telemetry exporters after account change");
                            continue;
                        }
                    };
                    if let Err(error) = logger_reload_handle.reload(
                        next_provider
                            .as_ref()
                            .and_then(OtelProvider::logger_export_layer)
                            .map(Layer::boxed),
                    ) {
                        warn!(%error, "failed to install telemetry exporters after account change");
                        continue;
                    }
                    if let Some(previous_provider) = std::mem::replace(&mut provider, next_provider) {
                        drop(tokio::task::spawn_blocking(move || previous_provider.shutdown()));
                    }
                    info!(
                        event.name = "codex.app_server.otel_reloaded",
                        "reloaded telemetry exporters after account change"
                    );
                }
            }
        }

        if let Some(provider) = provider {
            let _ = tokio::task::spawn_blocking(move || provider.shutdown()).await;
        }
    })
}

// Managed publication and retained retirement. Standalone constructor wiring
// must supply the real owner deadline and inspect the returned shutdown result.
pub(crate) struct ManagedReloader<S> {
    provider: Option<OtelProvider>,
    routes: codex_otel::ManagedTelemetryRoutes<S>,
    retired: Vec<codex_otel::RetiredTelemetry>,
    rejected: Vec<codex_otel::OtelRetirement>,
    managed_generation: Option<u64>,
    #[cfg(test)]
    published_generations: Vec<u64>,
    pub(crate) shutdown_result: Option<Result<(), crate::otel_reset_control::TelemetryResetError>>,
}

impl<S> ManagedReloader<S>
where
    S: Subscriber + for<'span> LookupSpan<'span> + Send + Sync + 'static,
{
    async fn drain(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), crate::otel_reset_control::TelemetryResetError> {
        use crate::otel_reset_control::TelemetryResetError;
        let classify = |error| match error {
            codex_otel::OtelRetirementError::TimedOut => TelemetryResetError::TimedOut,
            codex_otel::OtelRetirementError::WorkerFailed
            | codex_otel::OtelRetirementError::Exporter(_) => TelemetryResetError::RetirementFailed,
        };
        while let Some(retired) = self.retired.first() {
            retired.wait_until(deadline).await.map_err(classify)?;
            drop(self.retired.remove(0));
        }
        while let Some(rejected) = self.rejected.first() {
            rejected.wait_until(deadline).await.map_err(classify)?;
            drop(self.rejected.remove(0));
        }
        Ok(())
    }

    async fn apply(
        &mut self,
        config: &codex_core::config::Config,
        generation: u64,
        auth: &AuthManager,
        analytics: bool,
        deadline: tokio::time::Instant,
    ) -> Result<(), crate::otel_reset_control::TelemetryResetError> {
        use crate::otel_reset_control::TelemetryResetError;
        self.drain(deadline).await?;
        if tokio::time::Instant::now() >= deadline {
            return Err(TelemetryResetError::TimedOut);
        }
        if *auth.auth_change_receiver().borrow() != generation {
            return Err(TelemetryResetError::AuthChanged);
        }
        let mut candidate = Some({
            let candidate = codex_core::otel_init::prepare_provider(
                config,
                env!("CARGO_PKG_VERSION"),
                Some(OTEL_SERVICE_NAME),
                analytics,
            );
            match candidate {
                Ok(candidate) => candidate,
                Err(error) => {
                    if let Some(retirement) = error.begin_retirement() {
                        self.rejected.push(retirement);
                    }
                    return Err(TelemetryResetError::Unavailable);
                }
            }
        });
        let refusal = if *auth.auth_change_receiver().borrow() != generation {
            Some(TelemetryResetError::AuthChanged)
        } else if tokio::time::Instant::now() >= deadline {
            Some(TelemetryResetError::TimedOut)
        } else {
            None
        };
        let publication = if let Some(error) = refusal {
            Err(error)
        } else {
            self.routes
                .publish(&mut candidate)
                .map_err(|_| TelemetryResetError::Unavailable)
        };
        let publication = match publication {
            Ok(publication) => publication,
            Err(error) => {
                if let Some(candidate) = candidate
                    && let Some(retirement) = candidate.begin_retirement()
                {
                    self.rejected.push(retirement);
                }
                return Err(error);
            }
        };
        let (provider, retired) = publication.retire_previous(self.provider.take());
        #[cfg(test)]
        self.published_generations.push(generation);
        self.provider = provider;
        self.retired.push(retired);
        codex_core::otel_init::install_sqlite_telemetry(self.provider.as_ref(), OTEL_SERVICE_NAME);
        self.drain(deadline).await?;
        if *auth.auth_change_receiver().borrow() != generation {
            return Err(TelemetryResetError::AuthChanged);
        }
        Ok(())
    }
}

/// Existing shutdown telemetry drain cap; standalone hosts clamp it to their owner.
pub(crate) const TELEMETRY_DRAIN_CAP: Duration = Duration::from_secs(30);

/// Serializes managed commands with the legacy auth watcher. The returned task
/// is the exporter owner and must remain owned through standalone shutdown.
#[cfg(test)]
pub(crate) fn spawn_managed<S>(
    initial: codex_otel::TelemetryPublication,
    routes: codex_otel::ManagedTelemetryRoutes<S>,
    config_manager: ConfigManager,
    auth_manager: Arc<AuthManager>,
    default_analytics_enabled: bool,
    shutdown_token: CancellationToken,
) -> (
    crate::otel_reset_control::TelemetryResetControl,
    JoinHandle<ManagedReloader<S>>,
)
where
    S: Subscriber + for<'span> LookupSpan<'span> + Send + Sync + 'static,
{
    spawn_managed_with_owner_deadline(
        initial,
        routes,
        config_manager,
        auth_manager,
        default_analytics_enabled,
        shutdown_token,
        tokio::sync::watch::channel(None).1,
    )
}

/// As `spawn_managed`; the shutdown drain is `min(TELEMETRY_DRAIN_CAP, owner)`
/// when the host has published its owner deadline before cancelling.
pub(crate) fn spawn_managed_with_owner_deadline<S>(
    initial: codex_otel::TelemetryPublication,
    routes: codex_otel::ManagedTelemetryRoutes<S>,
    config_manager: ConfigManager,
    auth_manager: Arc<AuthManager>,
    default_analytics_enabled: bool,
    shutdown_token: CancellationToken,
    owner_deadline: tokio::sync::watch::Receiver<Option<tokio::time::Instant>>,
) -> (
    crate::otel_reset_control::TelemetryResetControl,
    JoinHandle<ManagedReloader<S>>,
)
where
    S: Subscriber + for<'span> LookupSpan<'span> + Send + Sync + 'static,
{
    let (control, mut commands) = crate::otel_reset_control::TelemetryResetControl::channel();
    let (provider, initial_retirement) = initial.retire_previous(None);
    let mut state = ManagedReloader {
        provider,
        routes,
        retired: vec![initial_retirement],
        rejected: Vec::new(),
        managed_generation: None,
        #[cfg(test)]
        published_generations: Vec::new(),
        shutdown_result: None,
    };
    let mut changes = auth_manager.auth_change_receiver();
    let handle = tokio::spawn(async move {
        'actor: loop {
            let command = tokio::select! {
                biased;
                _ = shutdown_token.cancelled() => break,
                command = commands.recv() => {
                    let Some(command) = command else { break };
                    Some(command)
                },
                changed = changes.changed() => {
                    if changed.is_err() { break; }
                    let generation = *changes.borrow_and_update();
                    if auth_manager.is_managed_auth_change(generation) {
                        tracing::debug!(
                            event.name = "codex.app_server.otel_reload_deferred",
                            auth_generation = generation,
                            "managed auth generation awaits explicit telemetry reset"
                        );
                        continue;
                    }
                    if state.managed_generation.is_some_and(|managed| generation <= managed) { continue; }
                    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
                    // Keep every watcher await interruptible, including old-owner
                    // drain. Candidate construction/publication is synchronous;
                    // before the next await every owner is retained in state.
                    let watcher = async {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        if let Ok(Ok(config)) = tokio::time::timeout_at(deadline,
                            config_manager.load_latest_config(None)).await {
                            if auth_manager.is_managed_auth_change(generation) {
                                return;
                            }
                            match state.apply(&config, generation, &auth_manager,
                                default_analytics_enabled, deadline).await {
                                Ok(()) => info!(
                                    event.name = "codex.app_server.otel_reloaded",
                                    "reloaded telemetry exporters after account change"
                                ),
                                Err(outcome) => warn!(?outcome, "telemetry auth generation reload refused"),
                            }
                        }
                    };
                    tokio::pin!(watcher);
                    loop {
                        tokio::select! {
                            biased;
                            _ = shutdown_token.cancelled() => break 'actor,
                            command = commands.recv() => {
                                let Some(command) = command else { break 'actor };
                                if command.generation != *auth_manager.auth_change_receiver().borrow() {
                                    let _ = command.reply.send(Err(crate::otel_reset_control::TelemetryResetError::AuthChanged));
                                    continue;
                                }
                                break Some(command);
                            }
                            _ = &mut watcher => break None,
                        }
                    }
                }
            };
            if let Some(command) = command {
                // Only authoritative current commands suppress their watcher.
                if command.generation == *auth_manager.auth_change_receiver().borrow() {
                    state.managed_generation = Some(command.generation);
                }
                let result = state
                    .apply(
                        &command.config,
                        command.generation,
                        &auth_manager,
                        default_analytics_enabled,
                        command.deadline,
                    )
                    .await;
                let _ = command.reply.send(result);
            }
        }
        state.shutdown_result = Some(match state.routes.disable() {
            Ok(publication) => {
                let (provider, retired) = publication.retire_previous(state.provider.take());
                state.provider = provider;
                state.retired.push(retired);
                let cap = tokio::time::Instant::now() + TELEMETRY_DRAIN_CAP;
                let owner = *owner_deadline.borrow();
                let deadline = owner.map_or(cap, |owner| owner.min(cap));
                state.drain(deadline).await
            }
            Err(_) => Err(crate::otel_reset_control::TelemetryResetError::Unavailable),
        });
        // The join result retains incomplete owners for the standalone caller;
        // task completion alone is never a successful shutdown receipt.
        state
    });
    (control, handle)
}

#[cfg(test)]
#[path = "otel_reloader_tests.rs"]
mod tests;
