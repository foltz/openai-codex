use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::SpanData;
use opentelemetry_sdk::trace::SpanExporter;
use std::fmt;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio::time::timeout_at;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExporterRetirementError {
    ShutdownFailed,
    Panicked,
    Unobserved,
    TimedOut,
}

#[derive(Clone, Copy, Debug, Default)]
struct Evidence {
    shutdown: Option<Result<(), ExporterRetirementError>>,
    dropped: bool,
}

/// Credential-free evidence; it never owns the exporter or its worker.
#[derive(Clone)]
pub(crate) struct ExporterReceipt(watch::Receiver<Evidence>);

impl ExporterReceipt {
    pub(crate) async fn wait_until(
        &self,
        deadline: Instant,
    ) -> Result<(), ExporterRetirementError> {
        let mut evidence = self.0.clone();
        loop {
            let current = *evidence.borrow_and_update();
            match current.shutdown {
                Some(Err(error)) => return Err(error),
                Some(Ok(())) if current.dropped => return Ok(()),
                None if current.dropped => return Err(ExporterRetirementError::Unobserved),
                Some(Ok(())) | None => {}
            }
            if Instant::now() >= deadline {
                return Err(ExporterRetirementError::TimedOut);
            }
            timeout_at(deadline, evidence.changed())
                .await
                .map_err(|_| ExporterRetirementError::TimedOut)?
                .map_err(|_| ExporterRetirementError::Unobserved)?;
        }
    }
}

/// Records what the SDK batch processor discards. Drop evidence is published
/// only after the actual exporter has been dropped, not at shutdown request.
pub(crate) struct AcknowledgedExporter<T> {
    inner: Option<T>,
    evidence: watch::Sender<Evidence>,
}

impl<T> fmt::Debug for AcknowledgedExporter<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AcknowledgedExporter")
    }
}

impl<T> AcknowledgedExporter<T> {
    pub(crate) fn new(inner: T) -> (Self, ExporterReceipt) {
        let (evidence, receiver) = watch::channel(Evidence::default());
        (
            Self {
                inner: Some(inner),
                evidence,
            },
            ExporterReceipt(receiver),
        )
    }

    #[expect(
        clippy::expect_used,
        reason = "inner is initialized in new and taken only in Drop, which cannot overlap a borrowed exporter method"
    )]
    fn observe_shutdown(
        &mut self,
        shutdown: impl FnOnce(&mut T) -> OTelSdkResult,
    ) -> OTelSdkResult {
        observe_shutdown(&self.evidence, || {
            shutdown(self.inner.as_mut().expect("exporter exists until Drop"))
        })
    }
}

fn observe_shutdown(
    evidence: &watch::Sender<Evidence>,
    shutdown: impl FnOnce() -> OTelSdkResult,
) -> OTelSdkResult {
    let result = catch_unwind(AssertUnwindSafe(shutdown));
    let safe = match &result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(ExporterRetirementError::ShutdownFailed),
        Err(_) => Err(ExporterRetirementError::Panicked),
    };
    evidence.send_modify(|evidence| {
        if evidence.shutdown.is_none() {
            evidence.shutdown = Some(safe);
        }
    });
    match result {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

#[expect(
    clippy::expect_used,
    reason = "all borrowed exporter operations precede Drop; only Drop can take inner, and absence must never become successful export/shutdown"
)]
impl<T: opentelemetry_sdk::logs::LogExporter> opentelemetry_sdk::logs::LogExporter
    for AcknowledgedExporter<T>
{
    async fn export(&self, batch: opentelemetry_sdk::logs::LogBatch<'_>) -> OTelSdkResult {
        self.inner
            .as_ref()
            .expect("exporter exists until Drop")
            .export(batch)
            .await
    }

    fn shutdown(&self) -> OTelSdkResult {
        observe_shutdown(&self.evidence, || {
            self.inner
                .as_ref()
                .expect("exporter exists until Drop")
                .shutdown()
        })
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        observe_shutdown(&self.evidence, || {
            self.inner
                .as_ref()
                .expect("exporter exists until Drop")
                .shutdown_with_timeout(timeout)
        })
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner
            .as_mut()
            .expect("exporter exists until Drop")
            .set_resource(resource);
    }
}

#[expect(
    clippy::expect_used,
    reason = "all borrowed exporter operations precede Drop; only Drop can take inner, and absence must never become successful export/shutdown"
)]
impl<T: SpanExporter> SpanExporter for AcknowledgedExporter<T> {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        self.inner
            .as_ref()
            .expect("exporter exists until Drop")
            .export(batch)
            .await
    }

    fn shutdown(&mut self) -> OTelSdkResult {
        self.observe_shutdown(SpanExporter::shutdown)
    }

    fn shutdown_with_timeout(&mut self, timeout: Duration) -> OTelSdkResult {
        self.observe_shutdown(|inner| inner.shutdown_with_timeout(timeout))
    }

    fn force_flush(&mut self) -> OTelSdkResult {
        self.inner
            .as_mut()
            .expect("exporter exists until Drop")
            .force_flush()
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner
            .as_mut()
            .expect("exporter exists until Drop")
            .set_resource(resource);
    }
}

impl<T> Drop for AcknowledgedExporter<T> {
    fn drop(&mut self) {
        drop(self.inner.take());
        self.evidence
            .send_modify(|evidence| evidence.dropped = true);
    }
}

#[cfg(test)]
#[path = "trace_exporter_retirement_tests.rs"]
mod tests;
