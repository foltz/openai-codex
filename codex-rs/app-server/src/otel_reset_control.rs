use codex_core::config::Config;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tokio::time::timeout_at;

/// Fixed internal failures; no exporter endpoint, headers, or backend text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TelemetryResetError {
    Unavailable,
    TimedOut,
    AuthChanged,
    RetirementFailed,
}

pub(crate) struct TelemetryResetCommand {
    pub(crate) generation: u64,
    pub(crate) config: Arc<Config>,
    pub(crate) deadline: Instant,
    pub(crate) reply: oneshot::Sender<Result<(), TelemetryResetError>>,
}

/// A caller owns only a request/observation capability, never exporter state.
/// The reloader retains the admitted operation after an observer disappears.
#[derive(Clone, Default)]
pub(crate) struct TelemetryResetControl {
    sender: Option<mpsc::Sender<TelemetryResetCommand>>,
}

impl TelemetryResetControl {
    pub(crate) fn channel() -> (Self, mpsc::Receiver<TelemetryResetCommand>) {
        let (sender, receiver) = mpsc::channel(1);
        (
            Self {
                sender: Some(sender),
            },
            receiver,
        )
    }

    pub(crate) async fn reset_until(
        &self,
        generation: u64,
        config: Arc<Config>,
        deadline: Instant,
    ) -> Result<(), TelemetryResetError> {
        let sender = self
            .sender
            .as_ref()
            .ok_or(TelemetryResetError::Unavailable)?;
        if Instant::now() >= deadline {
            return Err(TelemetryResetError::TimedOut);
        }
        let (reply, response) = oneshot::channel();
        timeout_at(
            deadline,
            sender.send(TelemetryResetCommand {
                generation,
                config,
                deadline,
                reply,
            }),
        )
        .await
        .map_err(|_| TelemetryResetError::TimedOut)?
        .map_err(|_| TelemetryResetError::Unavailable)?;
        timeout_at(deadline, response)
            .await
            .map_err(|_| TelemetryResetError::TimedOut)?
            .map_err(|_| TelemetryResetError::Unavailable)?
    }
}

#[cfg(test)]
#[path = "otel_reset_control_tests.rs"]
mod tests;
