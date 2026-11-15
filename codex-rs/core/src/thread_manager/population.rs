//! Exact published runtimes outlive lookup-map removal. Neither map absence
//! nor a normal session-loop join is sufficient to discard this custody.
//!
//! Release is evidence-driven: an entry leaves only once its exact loop join
//! is observable and its clean cleanup receipt is recorded. Every lookup
//! removal compacts after the lookup write guard is dropped, and a removal
//! that precedes cleanup leaves one passive release watcher (see
//! [`ConstructionTicket::release_removed`]).

use super::CodexThread;
use super::ConstructionTicket;
use super::State;
use super::ThreadConstructions;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CodexResult;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use tokio::time::Instant;

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
        // The host's shutdown-edge deadline is bound BEFORE the runtime is
        // exposed in the registry or the lookup map, outside the registry
        // lock (binding takes the session's cleanup mutex). If a tighter edge
        // latches meanwhile, bind that too before publishing: the first
        // visible runtime is always already bound, so a concurrent retirement
        // can never start its cleanup in Legacy (unbounded) mode.
        let mut bound: Option<Instant> = None;
        loop {
            let latched = {
                let state = registry.lock().map_err(|_| CodexErr::InternalAgentDied)?;
                if state.closed {
                    return Err(CodexErr::InternalAgentDied);
                }
                state.shutdown_deadline
            };
            if latched != bound {
                if let Some(deadline) = latched {
                    // The latched host/manager shutdown edge. A refusal (Legacy
                    // cleanup already began before publication) still
                    // engages that cleanup's live clamp.
                    let _ = thread.bind_shutdown_edge_deadline(deadline);
                }
                bound = latched;
            }
            let mut state = registry.lock().map_err(|_| CodexErr::InternalAgentDied)?;
            if state.closed {
                return Err(CodexErr::InternalAgentDied);
            }
            if state.shutdown_deadline != bound {
                continue;
            }
            state.published.push(Arc::clone(&thread));
            // Caller holds the lookup-map write guard. No await, callback, or
            // fallible step belongs inside this publication closure.
            let published = publish();
            #[cfg(test)]
            let probe = state.publication_probe.clone();
            drop(state);
            #[cfg(test)]
            if let Some(probe) = probe {
                probe(&thread);
            }
            return Ok(published);
        }
    }
}

impl ConstructionTicket {
    /// Lookup removal is not retirement, but after it this registry may be the
    /// last strong owner of the runtime. Callers must have dropped the lookup
    /// write guard: compaction can run runtime destructors, which must never
    /// execute under the lookup guard or the registry mutex.
    ///
    /// `removed` is the exact Arc the lookup returned (if any). When it is
    /// still retained after compaction (cleanup not yet proven), one passive
    /// watcher is left: it awaits only this loop's existing retained join and
    /// peeks the cleanup receipt, then compacts. It never drives, resumes or
    /// rebinds cleanup, binds no deadline, and reports no completion; an
    /// incomplete or abandoned cleanup simply stays retained here.
    pub(in crate::thread_manager) fn release_removed(&self, removed: Option<&Arc<CodexThread>>) {
        let Some(registry) = self.0.upgrade() else {
            return;
        };
        compact(&registry);
        let Some(removed) = removed else {
            return;
        };
        let retained = registry
            .lock()
            .map(|state| {
                state
                    .published
                    .iter()
                    .any(|thread| Arc::ptr_eq(thread, removed))
            })
            .unwrap_or(false);
        drop(registry);
        if retained {
            watch_release(Weak::clone(&self.0), removed);
        }
    }
}

/// Holds only a weak registry reference and the runtime's own termination
/// observer (never the CodexThread, Session or registry), so it forms no
/// cycle and does not extend the runtime's lifetime. It resolves only after
/// the session-loop JoinHandle has completed: a loop-end callback inside the
/// task could fire before the join is observable to `compact`.
fn watch_release(registry: Weak<Mutex<State>>, removed: &Arc<CodexThread>) {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        // No runtime: the entry stays counted until the next compaction point.
        return;
    };
    let released = removed.observe_resource_release();
    runtime.spawn(async move {
        if released.await
            && let Some(registry) = registry.upgrade()
        {
            compact(&registry);
        }
    });
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
    #[cfg(test)]
    {
        let probe = registry
            .lock()
            .ok()
            .and_then(|state| state.compaction_probe.clone());
        if let Some(probe) = probe {
            probe();
        }
    }
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
