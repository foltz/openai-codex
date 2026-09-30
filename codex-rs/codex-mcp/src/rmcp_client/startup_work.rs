//! Admission belongs to one attempt, even when its Shared changes pollers.

use super::ManagedClient;
use super::ManagedClientFuture;
use super::StartupOutcomeError;
use crate::attempt_work::McpAttemptAccess;
use crate::attempt_work::McpAttemptRequirement;
use crate::attempt_work::McpAttemptWork;
use futures::FutureExt;
use std::sync::Arc;
use std::sync::Mutex;

enum Admission {
    Unstarted,
    Staged(Option<Box<dyn McpAttemptWork>>),
    Running,
    Cancelled,
}

/// The Shared is deliberately private: no observer may first-poll it without
/// checking admission. The lease is consumed by the effectful future itself,
/// not retained in the reusable startup factory or memoized result.
#[derive(Clone)]
pub(crate) struct ClientStartup {
    pub(crate) requirement: McpAttemptRequirement,
    admission: Arc<Mutex<Admission>>,
    completion: ManagedClientFuture,
}

impl ClientStartup {
    pub(crate) fn new(future: ManagedClientFuture, requirement: McpAttemptRequirement) -> Self {
        let admission = Arc::new(Mutex::new(Admission::Unstarted));
        let admission_for_future = Arc::clone(&admission);
        let completion = async move {
            let _work = {
                let mut admission = admission_for_future.lock().map_err(|_| {
                    StartupOutcomeError::from(anyhow::anyhow!("MCP startup admission is poisoned"))
                })?;
                match std::mem::replace(&mut *admission, Admission::Running) {
                    Admission::Staged(work) => work,
                    Admission::Cancelled => return Err(StartupOutcomeError::Cancelled),
                    Admission::Unstarted | Admission::Running => {
                        return Err(StartupOutcomeError::from(anyhow::anyhow!(
                            "MCP startup polled without admission"
                        )));
                    }
                }
            };
            future.await
        }
        .boxed()
        .shared();
        Self {
            requirement,
            admission,
            completion,
        }
    }

    /// Must precede waking a dormant driver. A refusal leaves the Shared
    /// unpolled and retryable; an observer joins an existing admission for free.
    /// The caller must not await between staging, waking the driver, and
    /// polling. Lock order is admission slot then host derivation; host code
    /// must not re-enter MCP while holding its own account-work locks.
    pub(crate) fn admit(&self, access: McpAttemptAccess<'_>) -> Result<(), StartupOutcomeError> {
        let mut admission = self.admission.lock().map_err(|_| {
            StartupOutcomeError::from(anyhow::anyhow!("MCP startup admission is poisoned"))
        })?;
        if matches!(*admission, Admission::Unstarted) {
            let work = self
                .requirement
                .derive(access)
                .map_err(StartupOutcomeError::Refused)?;
            *admission = Admission::Staged(work);
        }
        Ok(())
    }

    pub(crate) async fn observe(
        &self,
        access: McpAttemptAccess<'_>,
    ) -> Result<ManagedClient, StartupOutcomeError> {
        self.admit(access)?;
        self.completion.clone().await
    }

    /// Permanently cancel an attempt whose effectful future has not begun.
    /// Never reset Staged to Unstarted: a concurrent observer may already be
    /// about to poll. It must take either the staged work or Cancelled, never
    /// start without custody. Running work retains its lease until completion.
    pub(crate) fn cancel_unstarted(&self) {
        let mut admission = self
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if matches!(*admission, Admission::Unstarted | Admission::Staged(_)) {
            *admission = Admission::Cancelled;
        }
    }

    pub(crate) fn peek(&self) -> Option<&Result<ManagedClient, StartupOutcomeError>> {
        self.completion.peek()
    }
}

#[cfg(test)]
impl From<ManagedClientFuture> for ClientStartup {
    fn from(future: ManagedClientFuture) -> Self {
        Self::new(future, McpAttemptRequirement::Ungated)
    }
}

#[cfg(test)]
#[path = "startup_work_tests.rs"]
mod tests;
