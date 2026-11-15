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
    #[cfg(test)]
    pub(crate) fn new(future: ManagedClientFuture, requirement: McpAttemptRequirement) -> Self {
        Self::new_with_work(
            move |work| {
                async move {
                    let _work = work;
                    future.await
                }
                .boxed()
                .shared()
            },
            requirement,
        )
    }

    pub(crate) fn new_with_work(
        factory: impl FnOnce(Option<Box<dyn McpAttemptWork>>) -> ManagedClientFuture + Send + 'static,
        requirement: McpAttemptRequirement,
    ) -> Self {
        let admission = Arc::new(Mutex::new(Admission::Unstarted));
        let admission_for_future = Arc::clone(&admission);
        let completion = async move {
            let work = {
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
            factory(work).await
        }
        .boxed()
        .shared();
        Self {
            requirement,
            admission,
            completion,
        }
    }

    /// Extend the original attempt's result sequence before publication, preserving
    /// the same admission slot. No public Future implementation exposes an unchecked poll.
    pub(crate) fn map_completion(
        self,
        map: impl FnOnce(ManagedClientFuture) -> ManagedClientFuture,
    ) -> Self {
        Self {
            completion: map(self.completion),
            ..self
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

/// Derive before invoking a physical startup factory. The original future, not
/// a chat's connect observer or a reusable factory, retains this descendant.
#[cfg(test)]
pub(crate) fn physical_attempt(
    requirement: McpAttemptRequirement,
    access: McpAttemptAccess<'_>,
    factory: impl FnOnce() -> ManagedClientFuture,
) -> ManagedClientFuture {
    physical_attempt_with_work(requirement, access, |work| {
        let original = factory();
        async move {
            let _work = work;
            original.await
        }
        .boxed()
        .shared()
    })
}

#[cfg(test)]
pub(crate) fn physical_attempt_with_work(
    requirement: McpAttemptRequirement,
    access: McpAttemptAccess<'_>,
    factory: impl FnOnce(Option<Box<dyn McpAttemptWork>>) -> ManagedClientFuture,
) -> ManagedClientFuture {
    match requirement.derive(access) {
        Ok(work) => factory(work),
        Err(error) => futures::future::ready(Err(StartupOutcomeError::Refused(error)))
            .boxed()
            .shared(),
    }
}

/// Continuation custody for one already-admitted invocation. Cloning it is not
/// a new attempt or fresh authority. Each retained original must keep its clone
/// through effects; only their conjunctive release drops the counted lease.
pub(crate) fn physical_continuation(
    requirement: McpAttemptRequirement,
    work: Option<Arc<Box<dyn McpAttemptWork>>>,
    factory: impl FnOnce(Option<Arc<Box<dyn McpAttemptWork>>>) -> ManagedClientFuture,
) -> ManagedClientFuture {
    if requirement == McpAttemptRequirement::Required && work.is_none() {
        return futures::future::ready(Err(StartupOutcomeError::Refused(crate::McpAttemptRefused)))
            .boxed()
            .shared();
    }
    factory(work)
}
