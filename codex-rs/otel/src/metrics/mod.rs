pub(crate) mod buffered;
mod client;
mod config;
mod error;
pub(crate) mod names;
mod owner;
mod process;
mod retirement;
mod route;
pub(crate) mod runtime_metrics;
pub(crate) mod tags;
pub(crate) mod timer;
pub(crate) mod validation;

use crate::config::StatsigMetricsSettings;
pub use crate::metrics::buffered::record_global_operation;
pub use crate::metrics::client::MetricsClient;
pub use crate::metrics::config::MetricsConfig;
pub use crate::metrics::config::MetricsExporter;
pub use crate::metrics::error::MetricsError;
pub use crate::metrics::error::Result;
pub use crate::metrics::process::record_process_start_once;
pub use names::*;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
pub use tags::ORIGINATOR_TAG;
pub use tags::SessionMetricTagValues;
pub use tags::bounded_originator_tag_value;

#[derive(Default)]
struct GlobalMetrics {
    current: Option<MetricsClient>,
    statsig: Option<StatsigMetricsSettings>,
    // Cached handles must follow re-enablement even while current is absent.
    route: Option<Arc<route::MetricsRoute>>,
    // Failed/unobserved retirement must outlive incidental public handle clones.
    retired: Vec<Arc<retirement::RetiredMetrics>>,
}

static GLOBAL_METRICS: Mutex<GlobalMetrics> = Mutex::new(GlobalMetrics {
    current: None,
    statsig: None,
    route: None,
    retired: Vec::new(),
});

/// Excludes global handle acquisition until a coordinated route commit ends.
pub(crate) struct GlobalMetricsPublication<'a>(MutexGuard<'a, GlobalMetrics>);

pub(crate) fn publication_guard() -> Result<GlobalMetricsPublication<'static>> {
    GLOBAL_METRICS
        .lock()
        .map(GlobalMetricsPublication)
        .map_err(|_| MetricsError::RoutingUnavailable)
}

impl GlobalMetricsPublication<'_> {
    /// Acquire the other publication guards before calling this method. The
    /// finalizer may only perform infallible route writes, never callbacks,
    /// logging, SDK destruction, or lock acquisition. It runs while metrics
    /// operations and global handle acquisition are still excluded.
    ///
    /// On an acquisition error the borrowed candidate remains unpublished and
    /// owned by the caller; no SDK owner is destroyed under publication locks.
    pub(crate) fn publish_with(
        &mut self,
        candidate: &mut Option<MetricsClient>,
        settings: Option<StatsigMetricsSettings>,
        finish: impl FnOnce(),
    ) -> Result<()> {
        let next = candidate
            .as_ref()
            .map(|client| match &client.inner {
                client::MetricsOriginal::Standalone(inner) => Ok(Arc::clone(inner)),
                client::MetricsOriginal::Routed(_) => Err(MetricsError::RoutingUnavailable),
            })
            .transpose()?;
        // An empty initial route is private until commit, and therefore cannot
        // create a spurious retired generation for the first installation.
        let active = self
            .0
            .route
            .clone()
            .unwrap_or_else(|| Arc::new(route::MetricsRoute::default()));
        let mut route = active.publication_guard()?;
        let next_owner = next
            .as_ref()
            .map(|inner| owner::MetricsOwner::new(Arc::clone(inner)));
        let retired = route.replace(next);
        if let Some(MetricsClient {
            inner: client::MetricsOriginal::Routed(previous),
            ..
        }) = self.0.current.take()
        {
            self.0
                .retired
                .push(Arc::new(retirement::RetiredMetrics::new(previous, retired)));
        }
        if let Some(client) = candidate
            && let Some(owner) = next_owner
        {
            client.inner = client::MetricsOriginal::Routed(owner);
            client.active = Some(Arc::clone(&active));
        }
        self.0.route = Some(Arc::clone(&active));
        self.0.current = candidate.clone();
        self.0.statsig = candidate.as_ref().and(settings);
        finish();
        Ok(())
    }
}

#[cfg(test)]
#[path = "settings_tests.rs"]
mod settings_tests;

pub(crate) fn install_global(metrics: MetricsClient) -> Result<MetricsClient> {
    install_global_with_settings(metrics, None)
}

pub(crate) fn install_global_with_settings(
    metrics: MetricsClient,
    settings: Option<StatsigMetricsSettings>,
) -> Result<MetricsClient> {
    install_in(&GLOBAL_METRICS, metrics, settings)
}

fn install_in(
    slot: &Mutex<GlobalMetrics>,
    mut metrics: MetricsClient,
    settings: Option<StatsigMetricsSettings>,
) -> Result<MetricsClient> {
    if matches!(&metrics.inner, client::MetricsOriginal::Routed(_)) {
        // Re-publishing a handle must not manufacture a second shutdown owner
        // for the same SDK provider.
        return Err(MetricsError::RoutingUnavailable);
    }
    let inner = metrics.original_inner()?;
    let owner = owner::MetricsOwner::new(Arc::clone(&inner));
    let mut global = slot.lock().map_err(|_| MetricsError::RoutingUnavailable)?;
    let mut release = None;
    let active = match global.route.clone() {
        Some(active) => {
            // Legacy callers do not drive managed retirement. Keep count proof
            // but release route custody outside locks; active leases and the
            // original owner independently retain resources while needed.
            let (retired, previous_epoch) = active.replace_legacy(Some(inner))?;
            release = Some(previous_epoch);
            if let Some(MetricsClient {
                inner: client::MetricsOriginal::Routed(previous),
                ..
            }) = &global.current
            {
                let previous = Arc::clone(previous);
                global
                    .retired
                    .push(Arc::new(retirement::RetiredMetrics::new(previous, retired)));
            }
            active
        }
        None => Arc::new(route::MetricsRoute::new(inner)),
    };
    global.route = Some(Arc::clone(&active));
    metrics.active = Some(active);
    metrics.inner = client::MetricsOriginal::Routed(owner);
    global.current = Some(metrics.clone());
    global.statsig = settings;
    buffered::GLOBAL.enable(&metrics);
    drop(global);
    drop(release);
    Ok(metrics)
}

pub(crate) fn disable_global() -> Result<()> {
    let mut global = GLOBAL_METRICS
        .lock()
        .map_err(|_| MetricsError::RoutingUnavailable)?;
    let mut release = None;
    if let Some(active) = &global.route {
        let (ticket, previous_epoch) = active.replace_legacy(None)?;
        release = Some(previous_epoch);
        if let Some(MetricsClient {
            inner: client::MetricsOriginal::Routed(owner),
            ..
        }) = global.current.take()
        {
            global
                .retired
                .push(Arc::new(retirement::RetiredMetrics::new(owner, ticket)));
        }
    }
    global.statsig = None;
    drop(global);
    drop(release);
    buffered::GLOBAL.disable();
    Ok(())
}

pub fn global() -> Option<MetricsClient> {
    GLOBAL_METRICS.lock().ok()?.current.clone()
}

pub(crate) async fn retire_for(
    metrics: &MetricsClient,
    deadline: tokio::time::Instant,
) -> std::result::Result<(), crate::OtelRetirementError> {
    use crate::OtelRetirementError;
    use crate::OtelShutdownError;
    let unavailable = OtelRetirementError::Exporter(OtelShutdownError::Metrics);
    let client::MetricsOriginal::Routed(owner) = &metrics.inner else {
        return Err(unavailable);
    };
    let retired = {
        let global = GLOBAL_METRICS.lock().map_err(|_| unavailable)?;
        global
            .retired
            .iter()
            .find(|entry| entry.belongs_to(owner))
            .cloned()
            .ok_or(unavailable)?
    };
    retired
        .wait_until(deadline)
        .await
        .map_err(|error| match error {
            retirement::MetricsRetirementError::TimedOut => OtelRetirementError::TimedOut,
            retirement::MetricsRetirementError::WorkerFailed => OtelRetirementError::WorkerFailed,
            retirement::MetricsRetirementError::DrainUnavailable
            | retirement::MetricsRetirementError::Owner(_) => unavailable,
        })
}

pub(crate) fn global_statsig_settings() -> Option<StatsigMetricsSettings> {
    let global = GLOBAL_METRICS.lock().ok()?;
    if global.current.as_ref().is_some_and(|metrics| {
        metrics
            .original_inner()
            .is_ok_and(|inner| inner.network_policy.is_managed())
    }) {
        return None;
    }
    global.statsig.clone()
}
