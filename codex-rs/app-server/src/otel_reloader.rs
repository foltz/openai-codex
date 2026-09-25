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
use tracing_subscriber::registry::LookupSpan;

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

/// Serializes managed commands with the legacy auth watcher. The returned task
/// is the exporter owner and must remain owned through standalone shutdown.
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
                state
                    .drain(tokio::time::Instant::now() + Duration::from_secs(30))
                    .await
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
