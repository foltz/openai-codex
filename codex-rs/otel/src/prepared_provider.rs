use crate::OtelExporter;
use crate::OtelProvider;
use crate::OtelRetirement;
use crate::OtelSettings;
use crate::StatsigMetricsSettings;
use std::collections::BTreeMap;

/// Failed construction retains any exporters created before the failure.
/// Diagnostics are deliberately fixed; this owner must never enter transition
/// state. Managed callers must transfer its retirement before returning failure.
#[must_use = "retain partial-provider retirement before retrying preparation"]
pub struct OtelPreparationError {
    pub(crate) source: Box<dyn std::error::Error>,
    pub(crate) provider: Option<OtelProvider>,
}

impl std::fmt::Debug for OtelPreparationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("OtelPreparationError")
    }
}

impl std::fmt::Display for OtelPreparationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("telemetry provider preparation failed")
    }
}

impl std::error::Error for OtelPreparationError {}

impl OtelPreparationError {
    pub(crate) fn before_resources(source: impl Into<Box<dyn std::error::Error>>) -> Self {
        Self {
            source: source.into(),
            provider: None,
        }
    }

    /// Transfer the exact partially built provider to the retained shutdown
    /// worker. No publication occurred, so no route drain is needed.
    pub fn begin_retirement(self) -> Option<OtelRetirement> {
        self.provider.map(OtelProvider::begin_retirement)
    }

    // Ordinary legacy construction retains its existing error interface and
    // best-effort Drop cleanup. Managed preparation must not use this adapter.
    pub(crate) fn into_legacy_error(self) -> Box<dyn std::error::Error> {
        self.source
    }
}

/// Unpublished exporters and their matching publication metadata.
///
/// This value owns SDK resources but changes no logger, trace, metrics, or
/// propagation route. Keep it in the telemetry owner, never transition state.
/// A rejected candidate must be retired and observed before claiming cleanup.
#[must_use = "publish the candidate or retain its retirement observation"]
pub struct PreparedOtelProvider {
    pub(crate) provider: Option<OtelProvider>,
    pub(crate) tracestate: BTreeMap<String, BTreeMap<String, String>>,
    pub(crate) statsig: Option<StatsigMetricsSettings>,
}

impl OtelProvider {
    /// Construct a candidate without publishing any process-global state.
    /// Disabled telemetry is an explicit empty candidate, not a no-op reload.
    pub fn prepare(settings: &OtelSettings) -> Result<PreparedOtelProvider, OtelPreparationError> {
        let provider = Self::build_unpublished(settings)?;
        let tracestate = if provider.is_some() {
            settings.tracestate.clone()
        } else {
            BTreeMap::new()
        };
        let statsig = (provider
            .as_ref()
            .is_some_and(|provider| provider.metrics.is_some())
            && matches!(settings.metrics_exporter, OtelExporter::Statsig))
        .then(|| StatsigMetricsSettings {
            environment: settings.environment.clone(),
        });
        Ok(PreparedOtelProvider {
            provider,
            tracestate,
            statsig,
        })
    }
}

impl PreparedOtelProvider {
    /// Transfer rejected, never-published exporters to the retained shutdown
    /// worker. `None` proves the candidate had no exporters to retire.
    pub fn begin_retirement(self) -> Option<OtelRetirement> {
        self.provider.map(OtelProvider::begin_retirement)
    }
}
