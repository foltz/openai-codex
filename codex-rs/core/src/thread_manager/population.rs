//! Exact published runtimes outlive lookup-map removal. Neither map absence
//! nor a normal session-loop join is sufficient to discard this custody.

use super::CodexThread;
use super::ConstructionTicket;
use super::State;
use super::ThreadConstructions;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CodexResult;
use std::sync::Arc;
use std::sync::Mutex;

impl ConstructionTicket {
    /// The map insertion and custody registration share the admission gate.
    /// A constructor admitted earlier may finish after close, but cannot then
    /// publish; its separate startup cell continues to own its cleanup.
    pub(in crate::thread_manager) fn publish_thread<T>(
        &self,
        thread: Arc<CodexThread>,
        publish: impl FnOnce() -> T,
    ) -> CodexResult<T> {
        let registry = self.0.upgrade().ok_or(CodexErr::InternalAgentDied)?;
        let mut state = registry.lock().map_err(|_| CodexErr::InternalAgentDied)?;
        if state.closed {
            return Err(CodexErr::InternalAgentDied);
        }
        state.published.push(thread);
        // Caller holds the lookup-map write guard. No await, callback, or
        // fallible step belongs inside this publication closure.
        Ok(publish())
    }
}

impl ThreadConstructions {
    /// A population snapshot, not a completion report. The original entries
    /// remain owned here when an observer times out or a lookup is removed.
    pub(in crate::thread_manager) fn published(&self) -> Vec<Arc<CodexThread>> {
        compact(&self.state);
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .published
            .clone()
    }
}

pub(super) fn compact(registry: &Arc<Mutex<State>>) {
    let candidates = {
        let Ok(state) = registry.lock() else {
            return;
        };
        state.published.clone()
    };
    // Normalize actual joins outside the registry lock; cleanup observation
    // only peeks its already-recorded result. No shutdown work is started.
    let completed = candidates
        .into_iter()
        .filter(|thread| thread.observed_terminal_cleanup().is_some())
        .collect::<Vec<_>>();
    let removed = {
        let Ok(mut state) = registry.lock() else {
            return;
        };
        let mut removed = Vec::new();
        let mut retained = Vec::with_capacity(state.published.len());
        for thread in std::mem::take(&mut state.published) {
            if completed.iter().any(|done| Arc::ptr_eq(done, &thread)) {
                removed.push(thread);
            } else {
                retained.push(thread);
            }
        }
        state.compacted = state.compacted.saturating_add(removed.len());
        state.published = retained;
        removed
    };
    // Releasing the last runtime owner can run destructors. Never do that
    // while holding the registration/publication mutex.
    drop(removed);
}

#[cfg(test)]
#[path = "population_tests.rs"]
mod tests;
