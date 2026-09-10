use crate::OtelProvider;
use crate::OtelShutdownError;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio::time::timeout_at;

/// An observation failure is not evidence that exporters stopped.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OtelRetirementError {
    #[error("telemetry retirement deadline elapsed")]
    TimedOut,
    #[error("telemetry retirement worker failed")]
    WorkerFailed,
    #[error("telemetry exporter shutdown failed")]
    Exporter(OtelShutdownError),
}

enum RetirementState {
    Pending(JoinHandle<Result<(), OtelShutdownError>>),
    Joined(Result<(), OtelRetirementError>),
    Complete(Result<(), OtelRetirementError>),
}

/// Owns the shutdown worker across cancelled or timed-out observations.
/// The surrounding reset owner must retain this handle until terminal evidence
/// is observed; dropping it while pending does not prove shutdown.
#[must_use = "retain and observe telemetry retirement before acknowledging reset"]
pub struct OtelRetirement {
    state: Mutex<RetirementState>,
    trace_receipt: Option<crate::trace_exporter_retirement::ExporterReceipt>,
    log_receipt: Option<crate::trace_exporter_retirement::ExporterReceipt>,
}

impl OtelProvider {
    /// Drain and retire this provider's exact removed metrics generation.
    /// Refuses if metrics are still published; it never retires the current
    /// replacement on behalf of an older provider. Other exporters are separate.
    pub async fn retire_metrics_until(&self, deadline: Instant) -> Result<(), OtelRetirementError> {
        match &self.metrics {
            Some(metrics) => crate::metrics::retire_for(metrics, deadline).await,
            None => Ok(()),
        }
    }

    /// Transfer this provider to one blocking shutdown worker. Call only after
    /// removing its publication routes; this does not clear process globals.
    pub fn begin_retirement(self) -> OtelRetirement {
        let trace_receipt = self.trace_receipt.clone();
        let log_receipt = self.log_receipt.clone();
        let worker = tokio::task::spawn_blocking(move || {
            let result = self.shutdown_checked();
            drop(self);
            result
        });
        OtelRetirement {
            state: Mutex::new(RetirementState::Pending(worker)),
            trace_receipt,
            log_receipt,
        }
    }
}

impl OtelRetirement {
    /// Observe the same worker under an absolute deadline. Cancellation drops
    /// only the borrow of its JoinHandle, never the stored handle itself.
    #[expect(
        clippy::await_holding_invalid_type,
        reason = "exclusive JoinHandle polling right; cancellation drops the guard, not the retained worker, and the worker never locks this mutex"
    )]
    pub async fn wait_until(&self, deadline: Instant) -> Result<(), OtelRetirementError> {
        let mut state = timeout_at(deadline, self.state.lock())
            .await
            .map_err(|_| OtelRetirementError::TimedOut)?;
        if let RetirementState::Complete(result) = *state {
            return result;
        }
        if let RetirementState::Pending(worker) = &mut *state {
            if Instant::now() >= deadline {
                return Err(OtelRetirementError::TimedOut);
            }
            let result = timeout_at(deadline, worker)
                .await
                .map_err(|_| OtelRetirementError::TimedOut)?
                .map_err(|_| OtelRetirementError::WorkerFailed)
                .and_then(|result| result.map_err(OtelRetirementError::Exporter));
            *state = RetirementState::Joined(result);
        }
        let RetirementState::Joined(mut result) = *state else {
            unreachable!("pending worker normalized above");
        };
        for (receipt, failure) in [
            (&self.trace_receipt, OtelShutdownError::Traces),
            (&self.log_receipt, OtelShutdownError::Logs),
        ] {
            if result.is_err() {
                break;
            }
            let Some(receipt) = receipt else {
                continue;
            };
            use crate::trace_exporter_retirement::ExporterRetirementError;
            result = match receipt.wait_until(deadline).await {
                Ok(()) => Ok(()),
                Err(ExporterRetirementError::TimedOut) => {
                    return Err(OtelRetirementError::TimedOut);
                }
                Err(
                    ExporterRetirementError::ShutdownFailed
                    | ExporterRetirementError::Panicked
                    | ExporterRetirementError::Unobserved,
                ) => Err(OtelRetirementError::Exporter(failure)),
            };
        }
        *state = RetirementState::Complete(result);
        result
    }
}
