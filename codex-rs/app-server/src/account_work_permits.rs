//! Credential-free admission kernel retained from the managed transition coordinator.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::sync::Notify;

/// Bounded, credential-free permit registry for account-dependent work
/// (`CODEX-I05-S03-R009`, `R012`). Holds no auth value and never will --
/// only a barrier flag and an admitted-work count. Deliberately not behind
/// `CoordinatorState`'s `Mutex`: every `Permit`-classified request calls
/// [`Self::try_acquire`] on the dispatch hot path
/// (request dispatch classification; coordinator wiring is ported separately), and acquiring the transition
/// state lock there would serialize unrelated ordinary work behind
/// managed-transition bookkeeping it has nothing to do with.
#[derive(Clone)]
pub(crate) struct AccountWorkPermits {
    inner: Arc<AccountWorkPermitsInner>,
}

struct AccountWorkPermitsInner {
    /// Closure and population share one modification order. Separate atomics
    /// would permit an acquirer to see open while the closer sees zero.
    state: AtomicU64,
    /// Shared by permit release and by cancellation of a draining
    /// transition -- both are "something the drain wait should recheck"
    /// events. A spurious wake from the other event class costs only one
    /// extra recheck of the two loop conditions.
    signal: Notify,
    #[cfg(test)]
    after_reopen: std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

const ACCOUNT_WORK_CLOSED: u64 = 1 << 63;
const ACCOUNT_WORK_COUNT_MASK: u64 = ACCOUNT_WORK_CLOSED - 1;

/// Held for the lifetime of one admitted account-dependent request's
/// handling. Releases exactly once, from every terminal path (success,
/// error, or task cancellation/drop), without the holder ever calling a
/// release method (`CODEX-I05-S03-R009`: "release permits exactly once
/// across every terminal path").
pub(crate) struct AccountWorkPermitGuard {
    permits: AccountWorkPermits,
}

impl AccountWorkPermitGuard {
    /// Count a child operation started by this already-admitted work.
    ///
    /// Unlike fresh admission, derivation remains valid after barrier closure:
    /// the borrowed parent keeps the count nonzero until the child is counted.
    /// The child has its own lifetime and must be retained until its terminal
    /// evidence, even if the parent request or turn finishes first. Deriving
    /// through the guard also prevents borrowing authority from another
    /// coordinator's registry. Exhaustion refuses without changing the barrier.
    pub(crate) fn try_derive(&self) -> Option<Self> {
        self.permits
            .inner
            .state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                ((state & ACCOUNT_WORK_COUNT_MASK) < ACCOUNT_WORK_COUNT_MASK).then(|| state + 1)
            })
            .ok()?;
        Some(Self {
            permits: self.permits.clone(),
        })
    }
}

impl Drop for AccountWorkPermitGuard {
    fn drop(&mut self) {
        self.permits.inner.state.fetch_sub(1, Ordering::AcqRel);
        self.permits.inner.signal.notify_waiters();
    }
}

impl AccountWorkPermits {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(AccountWorkPermitsInner {
                state: AtomicU64::new(0),
                signal: Notify::new(),
                #[cfg(test)]
                after_reopen: std::sync::Mutex::new(None),
            }),
        }
    }

    /// The successful CAS either precedes close in this one atomic's
    /// modification order (and is counted), or observes closure and refuses.
    /// Exhaustion also refuses rather than carrying into the closed bit.
    pub(crate) fn try_acquire(&self) -> Option<AccountWorkPermitGuard> {
        self.inner
            .state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                if state & ACCOUNT_WORK_CLOSED != 0 || state == ACCOUNT_WORK_COUNT_MASK {
                    None
                } else {
                    Some(state + 1)
                }
            })
            .ok()?;
        Some(AccountWorkPermitGuard {
            permits: self.clone(),
        })
    }

    pub(crate) fn close(&self) {
        self.inner
            .state
            .fetch_or(ACCOUNT_WORK_CLOSED, Ordering::AcqRel);
    }

    pub(crate) fn reopen(&self) {
        // The coordinator owns the terminal-state invariant authorizing this;
        // cancelling a drain must preserve permits that are still finishing.
        self.inner
            .state
            .fetch_and(ACCOUNT_WORK_COUNT_MASK, Ordering::AcqRel);
        self.inner.signal.notify_waiters();
        // Observe the actual admission-opening point, not terminal return.
        #[cfg(test)]
        if let Some(observe) = self.inner.after_reopen.lock().unwrap().take() {
            observe();
        }
    }

    pub(crate) fn admitted_count(&self) -> u64 {
        self.inner.state.load(Ordering::Acquire) & ACCOUNT_WORK_COUNT_MASK
    }

    /// A retry hint, not admission. Register before checking so reopening
    /// between refusal and this future's first poll cannot lose the wake.
    pub(crate) async fn wait_until_available(&self) {
        loop {
            let notified = self.inner.signal.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let state = self.inner.state.load(Ordering::Acquire);
            if state & ACCOUNT_WORK_CLOSED == 0 && state < ACCOUNT_WORK_COUNT_MASK {
                return;
            }
            notified.await;
        }
    }

    pub(crate) fn wake_waiters(&self) {
        self.inner.signal.notify_waiters();
    }

    /// Per `tokio::sync::Notify`'s documented race-free idiom: the listener
    /// is created before the caller checks any condition, so a release or a
    /// [`Self::wake_waiters`] call that happens between creation and the
    /// caller's `.await` is never lost.
    pub(crate) fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.inner.signal.notified()
    }
}

#[cfg(test)]
#[path = "account_work_permits_tests.rs"]
mod tests;
