use super::owner::MetricsOwner;
use super::owner::MetricsOwnerError;
use super::route::MetricsDrainTicket;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio::time::timeout_at;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MetricsRetirementError {
    TimedOut,
    DrainUnavailable,
    WorkerFailed,
    Owner(MetricsOwnerError),
}

type Outcome = Result<(), MetricsRetirementError>;

enum State {
    Draining,
    Running(JoinHandle<Result<(), MetricsOwnerError>>),
    Complete(Outcome),
}

/// One exact retired generation. The registry retains this owner across
/// cancelled observers; no SDK shutdown starts before its operation drain.
pub(super) struct RetiredMetrics {
    owner: Arc<MetricsOwner>,
    pub(super) ticket: MetricsDrainTicket,
    state: Mutex<State>,
}

impl RetiredMetrics {
    pub(super) fn belongs_to(&self, owner: &Arc<MetricsOwner>) -> bool {
        Arc::ptr_eq(&self.owner, owner)
    }
    pub(super) fn new(owner: Arc<MetricsOwner>, ticket: MetricsDrainTicket) -> Self {
        Self {
            owner,
            ticket,
            state: Mutex::new(State::Draining),
        }
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "exclusive polling right retains the drain and worker across observer cancellation; worker does not acquire this mutex"
    )]
    pub(super) async fn wait_until(&self, deadline: Instant) -> Outcome {
        // This guard is the exclusive polling right, not a dependency lock.
        // Cancellation releases it but leaves the ticket/JoinHandle in self.
        let mut state = timeout_at(deadline, self.state.lock())
            .await
            .map_err(|_| MetricsRetirementError::TimedOut)?;
        if let State::Complete(result) = *state {
            return result;
        }
        if Instant::now() >= deadline {
            return Err(MetricsRetirementError::TimedOut);
        }
        if matches!(*state, State::Draining) {
            match timeout_at(deadline, self.ticket.wait()).await {
                Err(_) => return Err(MetricsRetirementError::TimedOut),
                Ok(Err(_)) => {
                    *state = State::Complete(Err(MetricsRetirementError::DrainUnavailable));
                    return Err(MetricsRetirementError::DrainUnavailable);
                }
                Ok(Ok(())) => {}
            }
            if Instant::now() >= deadline {
                return Err(MetricsRetirementError::TimedOut);
            }
            let owner = Arc::clone(&self.owner);
            *state = State::Running(tokio::task::spawn_blocking(move || {
                owner.attempt()?.run_observing()
            }));
        }
        let State::Running(worker) = &mut *state else {
            return Err(MetricsRetirementError::WorkerFailed);
        };
        let result = timeout_at(deadline, worker)
            .await
            .map_err(|_| MetricsRetirementError::TimedOut)?
            .map_err(|_| MetricsRetirementError::WorkerFailed)
            .and_then(|result| result.map_err(MetricsRetirementError::Owner));
        *state = State::Complete(result);
        result
    }
}
