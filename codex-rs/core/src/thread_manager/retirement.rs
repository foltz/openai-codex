//! Birth-time ownership of ThreadManager constructors, including failed starts.
//!
//! ThreadManager owns this registry; ThreadManagerState has only a weak ticket.
//! A retained constructor may therefore own State without cycling back to its
//! registry. Closing admission precedes draining and the final thread snapshot.

use super::CodexThread;
use super::NewThread;
use crate::session::startup_custody::SessionStartupCustody;
use crate::session::startup_custody::StartupCleanup;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CodexResult;
use futures::FutureExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use tokio::sync::oneshot;
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConstructionOutcome {
    Returned,
    Panicked,
}

struct Construction {
    completion: Shared<BoxFuture<'static, ConstructionOutcome>>,
    startup: Arc<SessionStartupCustody>,
}

#[derive(Default)]
struct State {
    closed: bool,
    deadline: Option<Instant>,
    constructions: Vec<Construction>,
    panicked: bool,
    terminal: Option<ConstructionDrain>,
    published: Vec<Arc<CodexThread>>,
    compacted: usize,
}

#[path = "population.rs"]
mod population;

#[derive(Clone, Default)]
pub(super) struct ThreadConstructions {
    state: Arc<Mutex<State>>,
}

#[derive(Clone)]
pub(super) struct ConstructionTicket(Weak<Mutex<State>>);

/// Caller cancellation and final map publication share one synchronous gate.
#[derive(Clone)]
pub(super) struct ConstructionPublication(Arc<Mutex<bool>>);

struct ObserverGuard(ConstructionPublication);

impl Drop for ObserverGuard {
    fn drop(&mut self) {
        *self
            .0
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = false;
    }
}

impl ConstructionPublication {
    pub(super) fn publish<T>(&self, publication: impl FnOnce() -> T) -> CodexResult<T> {
        let alive = self.0.lock().map_err(|_| CodexErr::InternalAgentDied)?;
        if !*alive {
            return Err(CodexErr::InternalAgentDied);
        }
        Ok(publication())
    }
}

/// This is constructor evidence only, never a whole-thread cleanup receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ConstructionDrain {
    pub(super) finished: bool,
    pub(super) panicked: bool,
    pub(super) unavailable: bool,
    pub(super) unpublished: usize,
    pub(super) sessions: Vec<StartupCleanup>,
}

impl ThreadConstructions {
    pub(super) fn close_until(&self, deadline: Instant) -> CodexResult<Instant> {
        let mut state = self.state.lock().map_err(|_| CodexErr::InternalAgentDied)?;
        state.closed = true;
        Ok(*state.deadline.get_or_insert(deadline))
    }

    pub(super) fn ticket(&self) -> ConstructionTicket {
        ConstructionTicket(Arc::downgrade(&self.state))
    }

    #[cfg(test)]
    pub(super) fn close(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closed = true;
    }

    pub(super) async fn drain_until(&self, deadline: Instant) -> ConstructionDrain {
        let (deadline, pending, unavailable) = {
            let unavailable = self.state.is_poisoned();
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.closed = true;
            if !unavailable && let Some(report) = &state.terminal {
                return report.clone();
            }
            let deadline = *state.deadline.get_or_insert(deadline);
            let pending = state
                .constructions
                .iter()
                .map(|entry| (entry.completion.clone(), Arc::clone(&entry.startup)))
                .collect::<Vec<_>>();
            (deadline, pending, unavailable)
        };
        let observed = futures::future::join_all(pending.into_iter().map(
            |(completion, startup)| async move {
                // Unlike a JoinHandle, polling this future can start real work.
                let outcome = if let Some(outcome) = completion.peek() {
                    Some(*outcome)
                } else if Instant::now() >= deadline {
                    None
                } else {
                    tokio::select! {
                        biased;
                        _ = tokio::time::sleep_until(deadline) => None,
                        outcome = completion => Some(outcome),
                    }
                };
                let cleanup = if outcome.is_some() {
                    match startup.shutdown_until(deadline).await {
                        StartupCleanup::NoUnpublishedSession => None,
                        cleanup => Some(cleanup),
                    }
                } else {
                    None
                };
                (outcome, cleanup)
            },
        ))
        .await;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.panicked |= observed
            .iter()
            .any(|(outcome, _)| *outcome == Some(ConstructionOutcome::Panicked));
        let report = ConstructionDrain {
            finished: observed.iter().all(|(outcome, _)| outcome.is_some()),
            panicked: state.panicked,
            unavailable: unavailable || self.state.is_poisoned(),
            unpublished: state
                .constructions
                .iter()
                .filter(|entry| !entry.startup.is_empty())
                .count(),
            sessions: observed
                .into_iter()
                .filter_map(|(_, cleanup)| cleanup)
                .collect(),
        };
        // Failed startup custody is deliberately NOT discarded by constructor
        // completion. Its separate session cleanup receipt is still required.
        if report.finished
            && !report.panicked
            && !report.unavailable
            && report.unpublished == 0
            && report.sessions.iter().all(|outcome| outcome.is_complete())
        {
            state.terminal = Some(report.clone());
            state.constructions.clear();
        }
        report
    }
}

impl ConstructionTicket {
    pub(super) fn register<F, Fut>(
        &self,
        factory: F,
    ) -> CodexResult<BoxFuture<'static, CodexResult<NewThread>>>
    where
        F: FnOnce(Arc<SessionStartupCustody>, ConstructionPublication) -> Fut + Send + 'static,
        Fut: Future<Output = CodexResult<NewThread>> + Send + 'static,
    {
        let registry = self.0.upgrade().ok_or(CodexErr::InternalAgentDied)?;
        population::compact(&registry);
        let mut state = registry.lock().map_err(|_| CodexErr::InternalAgentDied)?;
        if state.closed {
            return Err(CodexErr::InternalAgentDied);
        }
        state.panicked |= state
            .constructions
            .iter()
            .any(|entry| entry.completion.peek() == Some(&ConstructionOutcome::Panicked));
        state
            .constructions
            .retain(|entry| entry.completion.peek().is_none() || !entry.startup.is_empty());
        let startup = Arc::new(SessionStartupCustody::default());
        let construction_startup = Arc::clone(&startup);
        let publication = ConstructionPublication(Arc::new(Mutex::new(true)));
        let observer = ObserverGuard(publication.clone());
        let (sender, receiver) = oneshot::channel();
        let completion = async move {
            match AssertUnwindSafe(async move { factory(construction_startup, publication).await })
                .catch_unwind()
                .await
            {
                Ok(result) => {
                    // The map or unpublished-session cell, not this response,
                    // owns resources when the ordinary caller has gone away.
                    let _ = sender.send(result);
                    ConstructionOutcome::Returned
                }
                Err(_) => ConstructionOutcome::Panicked,
            }
        }
        .boxed()
        .shared();
        state.constructions.push(Construction {
            completion: completion.clone(),
            startup,
        });
        // The original is installed under the close gate before any polling.
        Ok(async move {
            let _observer = observer;
            if completion.await == ConstructionOutcome::Panicked {
                return Err(CodexErr::InternalAgentDied);
            }
            receiver.await.map_err(|_| CodexErr::InternalAgentDied)?
        }
        .boxed())
    }
}

#[cfg(test)]
#[path = "retirement_tests.rs"]
mod tests;
