use super::*;
use opentelemetry_sdk::error::OTelSdkError;
use opentelemetry_sdk::trace::BatchSpanProcessor;
use opentelemetry_sdk::trace::SdkTracerProvider;
use pretty_assertions::assert_eq;

#[derive(Debug)]
struct Exporter {
    shutdown: OTelSdkResult,
}

impl SpanExporter for Exporter {
    async fn export(&self, _batch: Vec<SpanData>) -> OTelSdkResult {
        Ok(())
    }
    fn shutdown(&mut self) -> OTelSdkResult {
        std::mem::replace(&mut self.shutdown, Err(OTelSdkError::AlreadyShutdown))
    }
}

#[tokio::test]
async fn successful_shutdown_requires_drop_and_replays_after_deadline() {
    let (mut exporter, receipt) = AcknowledgedExporter::new(Exporter { shutdown: Ok(()) });
    exporter.shutdown().unwrap();
    assert_eq!(
        receipt.wait_until(Instant::now()).await,
        Err(ExporterRetirementError::TimedOut)
    );
    drop(exporter);
    assert_eq!(receipt.wait_until(Instant::now()).await, Ok(()));
    assert_eq!(receipt.wait_until(Instant::now()).await, Ok(()));
}

#[tokio::test]
async fn dropping_without_shutdown_never_proves_success() {
    let (exporter, receipt) = AcknowledgedExporter::new(Exporter { shutdown: Ok(()) });
    drop(exporter);
    assert_eq!(
        receipt.wait_until(Instant::now()).await,
        Err(ExporterRetirementError::Unobserved)
    );
}

#[tokio::test]
async fn real_batch_success_cannot_hide_exporter_failure() {
    let (exporter, receipt) = AcknowledgedExporter::new(Exporter {
        shutdown: Err(OTelSdkError::InternalFailure(
            "synthetic secret-shaped backend text".to_owned(),
        )),
    });
    let provider = SdkTracerProvider::builder()
        .with_span_processor(BatchSpanProcessor::builder(exporter).build())
        .build();
    tokio::task::spawn_blocking(move || provider.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        receipt.wait_until(Instant::now()).await,
        Err(ExporterRetirementError::ShutdownFailed)
    );
}

#[tokio::test(start_paused = true)]
async fn timed_out_observer_does_not_consume_later_evidence() {
    let (mut exporter, receipt) = AcknowledgedExporter::new(Exporter { shutdown: Ok(()) });
    let start = Instant::now();
    assert_eq!(
        receipt.wait_until(start + Duration::from_secs(1)).await,
        Err(ExporterRetirementError::TimedOut)
    );
    assert_eq!(start.elapsed(), Duration::from_secs(1));
    exporter.shutdown().unwrap();
    drop(exporter);
    assert_eq!(receipt.wait_until(start).await, Ok(()));
}
