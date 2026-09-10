use super::ShutdownHandle;
use super::callback_join::CallbackOutcome;
use std::io;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::watch;
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LoginHttpReport {
    pub acceptor: Option<LoginWorkerOutcome>,
    pub joined: u64,
    pub interrupted: u64,
    pub failed: u64,
    pub cancelled: u64,
    pub panicked: u64,
    pub unavailable: bool,
}

/// Fixed evidence for one Codex-owned login worker, not OAuth success.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LoginWorkerOutcome {
    Joined,
    Cancelled,
    Panicked,
    Failed,
}

/// Evidence for the three Codex-owned workers only. This report does not
/// establish termination of tiny_http's private accept/connection workers.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LoginRetirementReport {
    pub callback: Option<LoginWorkerOutcome>,
    pub response: Option<LoginWorkerOutcome>,
    pub receiver: Option<LoginWorkerOutcome>,
    pub http: Option<LoginHttpReport>,
    pub persistence: Option<LoginWorkerOutcome>,
    pub deadline_expired: bool,
}

#[derive(Debug)]
struct Attempt {
    deadline: Instant,
    progress: watch::Sender<LoginRetirementReport>,
}

#[derive(Debug, Default)]
pub(super) struct RetirementSlot(Mutex<Option<Arc<Attempt>>>);

/// Retains original login joins. The host must retain and drive this observer;
/// expiry never detaches, aborts, or resumes work beyond the first deadline.
#[derive(Clone)]
#[must_use]
pub struct LoginRetirement {
    handle: ShutdownHandle,
    attempt: Arc<Attempt>,
}

impl ShutdownHandle {
    /// Bind the first shutdown deadline synchronously, before a cancellable
    /// wait. Repeated callers observe that same attempt, never a fresh budget.
    pub fn begin_retirement(&self, deadline: Instant) -> io::Result<LoginRetirement> {
        let mut slot = self
            .retirement
            .0
            .lock()
            .map_err(|_| io::Error::other("login retirement ownership unavailable"))?;
        let attempt = slot.get_or_insert_with(|| {
            if Instant::now() < deadline {
                self.shutdown();
            }
            Arc::new(Attempt {
                deadline,
                progress: watch::channel(LoginRetirementReport::default()).0,
            })
        });
        Ok(LoginRetirement {
            handle: self.clone(),
            attempt: Arc::clone(attempt),
        })
    }
}

impl LoginRetirement {
    pub fn deadline(&self) -> Instant {
        self.attempt.deadline
    }

    pub async fn wait(&self) -> LoginRetirementReport {
        let report = *self.attempt.progress.borrow();
        if report.callback.is_some()
            && report.response.is_some()
            && report.receiver.is_some()
            && report.http.is_some()
            && report.persistence.is_some()
        {
            return report;
        }
        if Instant::now() >= self.deadline() {
            return LoginRetirementReport {
                deadline_expired: true,
                ..report
            };
        }
        let callback_and_response = async {
            let callback = match self.handle.callback.wait().await {
                CallbackOutcome::Returned => LoginWorkerOutcome::Joined,
                CallbackOutcome::Cancelled => LoginWorkerOutcome::Cancelled,
                CallbackOutcome::Panicked => LoginWorkerOutcome::Panicked,
            };
            self.attempt
                .progress
                .send_modify(|report| report.callback = Some(callback));
            let report = self.handle.http.wait().await;
            let http = LoginHttpReport {
                acceptor: Some(convert_outcome(report.acceptor)),
                joined: report.connections.joined,
                interrupted: report.connections.interrupted,
                failed: report.connections.failed,
                cancelled: report.connections.cancelled,
                panicked: report.connections.panicked,
                unavailable: report.unavailable,
            };
            let response = if http.unavailable
                || !matches!(http.acceptor, Some(LoginWorkerOutcome::Joined))
                || http.interrupted > 0
                || http.failed > 0
                || http.cancelled > 0
                || http.panicked > 0
            {
                LoginWorkerOutcome::Failed
            } else {
                LoginWorkerOutcome::Joined
            };
            self.attempt.progress.send_modify(|report| {
                report.response = Some(response);
                report.receiver = http.acceptor;
                report.http = Some(http);
            });
            let persistence = match self.handle.persistence.wait().await {
                super::PersistenceOutcome::Joined => LoginWorkerOutcome::Joined,
                super::PersistenceOutcome::Failed => LoginWorkerOutcome::Failed,
                super::PersistenceOutcome::Cancelled => LoginWorkerOutcome::Cancelled,
                super::PersistenceOutcome::Panicked => LoginWorkerOutcome::Panicked,
            };
            self.attempt
                .progress
                .send_modify(|report| report.persistence = Some(persistence));
        };
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(self.deadline()) => {
                LoginRetirementReport { deadline_expired: true, ..*self.attempt.progress.borrow() }
            }
            _ = callback_and_response => {
                *self.attempt.progress.borrow()
            }
        }
    }
}

impl LoginRetirementReport {
    pub fn is_complete(&self) -> bool {
        !self.deadline_expired
            && matches!(self.callback, Some(LoginWorkerOutcome::Joined))
            && matches!(self.response, Some(LoginWorkerOutcome::Joined))
            && matches!(self.receiver, Some(LoginWorkerOutcome::Joined))
            && self.http.is_some_and(|http| {
                !http.unavailable
                    && matches!(http.acceptor, Some(LoginWorkerOutcome::Joined))
                    && http.interrupted == 0
                    && http.failed == 0
                    && http.cancelled == 0
                    && http.panicked == 0
            })
            && matches!(self.persistence, Some(LoginWorkerOutcome::Joined))
    }
}

fn convert_outcome(outcome: super::http_server::WorkerOutcome) -> LoginWorkerOutcome {
    match outcome {
        super::http_server::WorkerOutcome::Joined => LoginWorkerOutcome::Joined,
        super::http_server::WorkerOutcome::Interrupted => LoginWorkerOutcome::Cancelled,
        super::http_server::WorkerOutcome::Failed => LoginWorkerOutcome::Failed,
        super::http_server::WorkerOutcome::Cancelled => LoginWorkerOutcome::Cancelled,
        super::http_server::WorkerOutcome::Panicked => LoginWorkerOutcome::Panicked,
    }
}

#[cfg(test)]
#[path = "retirement_tests.rs"]
mod tests;
