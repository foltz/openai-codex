//! Host-owned logical turn permits, independent of app-server listeners.
//!
//! A session handle denotes one exact loop, not a reusable thread ID. Terminal
//! turn evidence releases bound work; loop termination is the fallback for
//! suspension, panic, and cancellation. The transition drain polls retained
//! receipts itself: this registry spawns no watcher and never requests shutdown.

use crate::managed_transition::AccountWorkPermitGuard;
use codex_protocol::host_turn_work::HostTurnWork;
use futures::FutureExt;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use futures::stream::FuturesUnordered;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::Notify;

#[derive(Clone, Default)]
pub(crate) struct AccountTurnWork {
    inner: Arc<Registry>,
}

#[derive(Default)]
struct Registry {
    sessions: Mutex<Vec<Arc<SessionWork>>>,
    changed: Notify,
}

struct SessionWork {
    termination: Shared<BoxFuture<'static, ()>>,
    state: Mutex<SessionState>,
}

#[derive(Default)]
struct SessionState {
    entries: Vec<Entry>,
    // Only retained while a submission is awaiting its turn ID.
    early_terminal: HashSet<String>,
    terminated: bool,
}

struct Entry {
    key: Arc<()>,
    turn_id: Option<String>,
    permit: AccountWorkPermitGuard,
}

/// Authority for one loop generation, including listenerless/internal threads.
#[derive(Clone)]
pub(crate) struct AccountTurnSession {
    registry: AccountTurnWork,
    work: Arc<SessionWork>,
}

/// A submission owns its pending slot until it binds to a logical turn.
/// Failed or cancelled submissions release just their own slot.
pub(crate) struct PendingAccountTurn {
    session: AccountTurnSession,
    key: Option<Arc<()>>,
}

impl AccountTurnWork {
    pub(crate) fn session(&self, termination: BoxFuture<'static, ()>) -> AccountTurnSession {
        AccountTurnSession {
            registry: self.clone(),
            work: Arc::new(SessionWork {
                termination: termination.shared(),
                state: Mutex::new(SessionState::default()),
            }),
        }
    }

    /// Observe one or more terminated loops. Cancellation only drops this
    /// observer; the registry retains every unresolved receipt and permit.
    /// Call concurrently with the permit-count notification under the drain's
    /// existing deadline. A live idle loop is never shut down to make progress.
    pub(crate) async fn observe_terminated(&self) {
        loop {
            let changed = self.inner.changed.notified();
            let sessions = self.inner.sessions.lock().expect("account turn registry poisoned").clone();
            let mut pending = FuturesUnordered::new();
            for session in sessions {
                pending.push(async move {
                    session.termination.clone().await;
                    session
                });
            }
            tokio::select! {
                Some(session) = pending.next(), if !pending.is_empty() => {
                    let entries = {
                        let mut state = session.state.lock().expect("account turn session poisoned");
                        state.terminated = true;
                        state.early_terminal.clear();
                        std::mem::take(&mut state.entries)
                    };
                    drop(entries);
                    self.compact();
                    return;
                }
                _ = changed => {}
            }
        }
    }

    fn compact(&self) {
        self.inner.sessions.lock().expect("account turn registry poisoned")
            .retain(|session| !session.state.lock().expect("account turn session poisoned").entries.is_empty());
        self.inner.changed.notify_waiters();
    }
}

impl AccountTurnSession {
    pub(crate) fn begin(&self, permit: AccountWorkPermitGuard) -> Option<PendingAccountTurn> {
        let key = Arc::new(());
        // Registry -> session is the only nested lock order. Registration and
        // compaction share it, so a new pending slot cannot be lost in between.
        let mut sessions = self.registry.inner.sessions.lock().expect("account turn registry poisoned");
        let mut state = self.work.state.lock().expect("account turn session poisoned");
        if state.terminated || self.work.termination.clone().now_or_never().is_some() {
            return None;
        }
        state.entries.push(Entry { key: Arc::clone(&key), turn_id: None, permit });
        if !sessions.iter().any(|session| Arc::ptr_eq(session, &self.work)) {
            sessions.push(Arc::clone(&self.work));
        }
        drop(state);
        drop(sessions);
        self.registry.inner.changed.notify_waiters();
        Some(PendingAccountTurn { session: self.clone(), key: Some(key) })
    }

    /// Delegate from the exact live parent turn, not merely a thread ID or a
    /// boolean claiming that some request once held a permit.
    pub(crate) fn derive(&self, turn_id: &str) -> Option<AccountWorkPermitGuard> {
        let state = self.work.state.lock().expect("account turn session poisoned");
        if state.terminated || self.work.termination.clone().now_or_never().is_some() {
            return None;
        }
        state.entries.iter()
            .find(|entry| entry.turn_id.as_deref() == Some(turn_id))?
            .permit.try_derive()
    }

    /// Logical stop/abort evidence, not a claim of physical task cleanup.
    pub(crate) fn terminal(&self, turn_id: &str) {
        let retired = {
            let mut state = self.work.state.lock().expect("account turn session poisoned");
            if state.entries.iter().any(|entry| entry.turn_id.is_none()) {
                state.early_terminal.insert(turn_id.to_owned());
            }
            let mut retired = Vec::new();
            let mut index = 0;
            while index < state.entries.len() {
                if state.entries[index].turn_id.as_deref() == Some(turn_id) {
                    retired.push(state.entries.swap_remove(index));
                } else {
                    index += 1;
                }
            }
            retired
        };
        drop(retired);
        self.registry.compact();
    }
}

#[cfg(test)]
impl PendingAccountTurn {
    pub(crate) fn bind(mut self, turn_id: String) {
        self.bind_submission(&turn_id);
        self.key = None;
    }
}

impl std::fmt::Debug for PendingAccountTurn {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PendingAccountTurn")
    }
}

impl HostTurnWork for PendingAccountTurn {
    fn bind_submission(&mut self, turn_id: &str) {
        let key = self.key.as_ref().expect("pending turn has submission authority");
        let retired = {
            let mut state = self.session.work.state.lock().expect("account turn session poisoned");
            let Some(index) = state.entries.iter().position(|entry| Arc::ptr_eq(&entry.key, key)) else {
                return;
            };
            let retired = if state.early_terminal.contains(turn_id) {
                Some(state.entries.swap_remove(index))
            } else {
                state.entries[index].turn_id = Some(turn_id.to_owned());
                None
            };
            if state.entries.iter().all(|entry| entry.turn_id.is_some()) {
                state.early_terminal.clear();
            }
            retired
        };
        drop(retired);
        self.session.registry.compact();
    }

    fn retain_until_terminal(mut self: Box<Self>) {
        self.key = None;
    }
}

impl Drop for PendingAccountTurn {
    fn drop(&mut self) {
        let Some(key) = self.key.take() else { return; };
        let retired = {
            let mut state = self.session.work.state.lock().expect("account turn session poisoned");
            let retired = state.entries.iter().position(|entry| Arc::ptr_eq(&entry.key, &key))
                .map(|index| state.entries.swap_remove(index));
            if state.entries.iter().all(|entry| entry.turn_id.is_some()) {
                state.early_terminal.clear();
            }
            retired
        };
        drop(retired);
        self.session.registry.compact();
    }
}

#[cfg(test)]
#[path = "account_turn_work_tests.rs"]
mod tests;
