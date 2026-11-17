//! Cooperative stop for host-owned shutdown work.
//!
//! The host latches one absolute owner deadline, arms it here with
//! [`OwnerExpiry::arm`], and cancels `OwnerExpiry`'s token when that deadline
//! passes. It never grants a fresh budget.
//!
//! - (b) Async work run through [`OwnerExpiry::run`] checks the clock against
//!   the armed deadline, and the token, before every poll. The token's timer
//!   task may run late; the clock check alone decides expiry, so work is never
//!   polled at or after the deadline. At expiry it stops scheduling work and
//!   hands its unpolled future back as a retained owner. It is never aborted
//!   or dropped early.
//! - Custody is split so retained work can never own its owner. The strong
//!   store of retained futures lives only in [`OwnerCustody`], which the
//!   external shutdown owner holds. Every [`OwnerExpiry`] handle, including
//!   the clones captured by the work itself, reaches that store through a
//!   `Weak` and holds strongly only the reporting state (deadline, counters,
//!   receipts, task names), which never contains retained work. Dropping the
//!   `OwnerCustody` therefore releases every retained future exactly once.
//! - (a) Blocking work that cannot be cancelled safely, such as an OS thread
//!   mid-syscall, is registered with [`OwnerExpiry::register_blocking`]. Its
//!   real join handle is kept and reported incomplete until it is joined
//!   with a normal (`Ok`) result. A thread that is still running, or that
//!   ended by panicking, is never treated as stopped.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::Weak;
use std::task::Poll;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// Owner of (b) work stopped at expiry: retained, never polled again.
pub struct ReturnedOwner {
    pub task: &'static str,
    _owner: Box<dyn Send>,
}

/// Output accepted for a peer and then dropped. This is the delivery
/// dimension, kept separate from thread custody. The counts are saturating
/// lower bounds.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AbandonedOutput {
    pub messages: u64,
    pub bytes: u64,
}

/// Real join for (a) blocking work.
pub struct BlockingReceipt {
    pub task: &'static str,
    state: BlockingState,
}

enum BlockingState {
    /// The thread may still run; its real join is kept.
    Running(std::thread::JoinHandle<()>),
    /// A finished original join is being observed outside the reporting lock.
    Joining,
    /// Joined with `Ok`: positive evidence of a normal end.
    Exited,
    /// Joined with `Err`: the thread panicked. Never proof of completion.
    Panicked,
}

impl BlockingReceipt {
    /// Whether the thread is still running (not joined yet).
    pub fn is_running(&self) -> bool {
        matches!(
            self.state,
            BlockingState::Running(_) | BlockingState::Joining
        )
    }

    /// Positive proof only: the thread was joined with a normal result.
    pub fn exited_normally(&self) -> bool {
        matches!(self.state, BlockingState::Exited)
    }
}

/// Refusal of bounded cleanup on a handle with no live `OwnerCustody`:
/// nothing could hold the work at its deadline, so it is neither armed nor
/// admitted. Returned explicitly and recorded (see `refused_tasks`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NoCustodyOwner;

impl std::fmt::Display for NoCustodyOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("no live OwnerCustody: bounded cleanup refused")
    }
}

impl std::error::Error for NoCustodyOwner {}

/// Host callback recording the first shutdown edge (see
/// [`OwnerExpiry::set_shutdown_edge`]).
type ShutdownEdge = Arc<dyn Fn() + Send + Sync>;

/// Strong store of retained (b) futures. Only `OwnerCustody` owns it.
type RetainedStore = Mutex<Vec<ReturnedOwner>>;

/// Reporting state shared by every handle. It never holds retained work, so
/// work that captures a handle cannot keep its own owner alive.
#[derive(Default)]
struct Custody {
    // Names of (b) tasks that handed back owners at expiry.
    returned: Vec<&'static str>,
    // Names of (b) tasks refused before admission: no live custody owner.
    refused: Vec<&'static str>,
    // Admitted (b) tasks stopped by the owner's release while pending.
    stopped_by_owner_release: Vec<&'static str>,
    blocking: Vec<BlockingReceipt>,
    // How a finished blocking task ended (e.g. "cancelled: output abandoned").
    notes: Vec<(&'static str, &'static str)>,
    abandoned: AbandonedOutput,
    // Output for a recipient that positively went away (EPIPE/POLLHUP on
    // stdout, a closed websocket). It is counted, but it does not make the
    // server incomplete.
    undeliverable: AbandonedOutput,
    recipient_closed: bool,
}

/// The explicit custody owner, held only by the external shutdown owner.
/// It is the sole strong owner of retained work; dropping it releases that
/// work (never aborted, never polled again). Not `Clone` on purpose.
pub struct OwnerCustody {
    expiry: OwnerExpiry,
    // Held only for ownership: dropping it releases the retained work.
    _retained: Arc<RetainedStore>,
}

impl Drop for OwnerCustody {
    /// Owner release is an expiry edge for every handle: pending drivers are
    /// woken through the same wake path as the deadline and stop before
    /// polling another unit (no new task, no budget). Retained work is then
    /// released exactly once when `_retained` drops after this.
    fn drop(&mut self) {
        self.expiry.released.cancel();
    }
}

impl OwnerCustody {
    /// `expired` must be cancelled exactly when the host's latched owner
    /// deadline passes.
    pub fn new(expired: CancellationToken) -> Self {
        let retained = Arc::new(RetainedStore::default());
        Self {
            expiry: OwnerExpiry {
                shutdown_edge: Arc::new(OnceLock::new()),
                expired,
                released: CancellationToken::new(),
                deadline: Arc::new(OnceLock::new()),
                custody: Arc::new(Mutex::new(Custody::default())),
                retained: Arc::downgrade(&retained),
            },
            _retained: retained,
        }
    }

    /// A non-owning handle for work, reporting and expiry checks.
    pub fn expiry(&self) -> OwnerExpiry {
        self.expiry.clone()
    }

    /// Names of the futures this owner currently retains.
    #[cfg(test)]
    pub(crate) fn retained_tasks(&self) -> Vec<&'static str> {
        self._retained
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|owner| owner.task)
            .collect()
    }
}

/// A non-owning handle: deadline, expiry checks, counters and receipts. It
/// reaches the retained-work store only through a `Weak`, so work that
/// captures it never keeps its own owner alive.
#[derive(Clone)]
pub struct OwnerExpiry {
    // The host's first-shutdown-edge hook (first set wins). Hosts install a
    // callback that holds only a weak path to their latch, so a handle
    // inside retained work never keeps the owner alive.
    shutdown_edge: Arc<OnceLock<ShutdownEdge>>,
    expired: CancellationToken,
    // Cancelled when the external `OwnerCustody` is dropped.
    released: CancellationToken,
    // The absolute latched owner deadline, set once by `arm`.
    deadline: Arc<OnceLock<Instant>>,
    custody: Arc<Mutex<Custody>>,
    retained: Weak<RetainedStore>,
}

impl OwnerExpiry {
    /// Carry the host's absolute latched owner deadline. The first call wins;
    /// a later deadline never replaces it (no fresh budget). Refused, before
    /// anything is armed, on a handle without a live `OwnerCustody`: a
    /// bounded deadline needs an owner to hold work stopped by it.
    pub fn arm(&self, deadline: Instant) -> Result<Instant, NoCustodyOwner> {
        if self.retained.upgrade().is_none() {
            return Err(NoCustodyOwner);
        }
        Ok(*self.deadline.get_or_init(|| deadline))
    }

    /// The armed absolute owner deadline, if the host has latched one.
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline.get().copied()
    }

    /// Reporting-only handle for callers without a host owner (tests,
    /// embedders): counters, receipts and notes. It is never bounded:
    /// `arm` and `run` refuse with `NoCustodyOwner` before arming or
    /// admitting anything, because no owner could hold stopped work.
    pub fn never() -> Self {
        Self {
            shutdown_edge: Arc::new(OnceLock::new()),
            expired: CancellationToken::new(),
            released: CancellationToken::new(),
            deadline: Arc::new(OnceLock::new()),
            custody: Arc::new(Mutex::new(Custody::default())),
            retained: Weak::new(),
        }
    }

    /// The clock decides: expired once `now >= deadline`, even before the
    /// token's timer task has run. A cancelled token is expired as well.
    pub fn is_expired(&self) -> bool {
        self.expired.is_cancelled()
            || self
                .deadline()
                .is_some_and(|deadline| Instant::now() >= deadline)
    }

    /// Install the host's first-shutdown-edge hook (first call wins). The
    /// hook must be idempotent (first edge wins, never extended) and must not
    /// hold its owner strongly; it is called from Tokio tasks.
    pub fn set_shutdown_edge(&self, edge: impl Fn() + Send + Sync + 'static) -> bool {
        self.shutdown_edge.set(Arc::new(edge)).is_ok()
    }

    /// Record a shutdown edge at independent intake (stdio EOF, a signal, a
    /// daemon shutdown request), before any queueing toward the processor.
    /// A no-op without a host hook. Must run inside a Tokio runtime.
    pub fn mark_shutdown_edge(&self) {
        if let Some(edge) = self.shutdown_edge.get() {
            edge();
        }
    }

    /// Whether the external `OwnerCustody` is gone: its drop cancelled the
    /// release token, or the store no longer upgrades. A run stops driving
    /// at this edge, like at expiry.
    pub fn owner_released(&self) -> bool {
        self.released.is_cancelled() || self.retained.upgrade().is_none()
    }

    pub fn expired_token(&self) -> CancellationToken {
        self.expired.clone()
    }

    /// Drive `work` until it completes or the owner expires, checking the
    /// clock against the armed deadline (and the token) before every poll.
    /// At expiry the unpolled future is handed to the owner's store and
    /// `Ok(None)` is returned.
    ///
    /// Admission requires a live `OwnerCustody`: without one the work is
    /// refused before it is polled (`Err(NoCustodyOwner)`, recorded in
    /// `refused_tasks`). A run never holds the owner's store strongly while
    /// it drives work: a run nested inside other retained work must not keep
    /// its own owner alive. Instead the owner's release is a wake edge: the
    /// driver checks that its owner still exists before every poll, so a
    /// pending run stops at the release with no further poll; with no owner
    /// to retain into, the work is released and recorded in
    /// `stopped_by_owner_release` (an incomplete outcome), never silently.
    pub async fn run<F>(
        &self,
        task: &'static str,
        work: F,
    ) -> Result<Option<F::Output>, NoCustodyOwner>
    where
        F: Future + Send + 'static,
    {
        if self.retained.upgrade().is_none() {
            self.with_custody(|custody| custody.refused.push(task));
            return Err(NoCustodyOwner);
        }
        let mut work = Box::pin(work);
        let output = self.poll_until_expired(work.as_mut()).await;
        if output.is_none() {
            // Handed to the external owner's store, never to this handle.
            match self.retained.upgrade() {
                Some(store) => {
                    self.with_custody(|custody| custody.returned.push(task));
                    store
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(ReturnedOwner {
                            task,
                            _owner: Box::new(work),
                        });
                }
                None => {
                    // Woken by the owner's release while pending: the driver
                    // stopped at that edge with no further poll. There is no
                    // owner to retain into, so the release covers this work.
                    // Not a deadline, not completion: reported incomplete.
                    self.with_custody(|custody| custody.stopped_by_owner_release.push(task));
                    tracing::warn!(
                        task,
                        "owner custody released while work was pending; work stopped and released (incomplete)"
                    );
                    drop(work);
                }
            }
        }
        Ok(output)
    }

    /// Names of (b) tasks refused before admission (no live custody owner).
    pub fn refused_tasks(&self) -> Vec<&'static str> {
        self.with_custody(|custody| custody.refused.clone())
    }

    /// Admitted (b) tasks stopped by the external owner's release while
    /// pending, and released with it. Always an incomplete outcome.
    pub fn stopped_by_owner_release(&self) -> Vec<&'static str> {
        self.with_custody(|custody| custody.stopped_by_owner_release.clone())
    }

    /// Resolves once the external `OwnerCustody` is released (immediately
    /// for a handle without a live owner). Admission loops stop on it.
    pub async fn owner_released_wait(&self) {
        if self.owner_released() {
            return;
        }
        self.released.cancelled().await;
    }

    /// Poll `work` only while not expired. Expiry is checked before every
    /// poll; the token and a deadline timer only wake this future, they never
    /// decide that the deadline has not passed.
    async fn poll_until_expired<F>(&self, mut work: Pin<&mut F>) -> Option<F::Output>
    where
        F: Future + ?Sized,
    {
        let mut cancelled = std::pin::pin!(self.expired.cancelled());
        let mut released = std::pin::pin!(self.released.cancelled());
        let mut timer: Option<Pin<Box<tokio::time::Sleep>>> = None;
        std::future::poll_fn(|cx| {
            // Expiry, or the owner's release, stops the driver before it
            // polls another unit of work.
            if self.is_expired() || self.owner_released() {
                return Poll::Ready(None);
            }
            // Wake-ups only; the stop itself was decided above.
            if cancelled.as_mut().poll(cx).is_ready() || released.as_mut().poll(cx).is_ready() {
                return Poll::Ready(None);
            }
            if let Some(deadline) = self.deadline() {
                let timer =
                    timer.get_or_insert_with(|| Box::pin(tokio::time::sleep_until(deadline)));
                if timer.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(None);
                }
            }
            work.as_mut().poll(cx).map(Some)
        })
        .await
    }

    /// Keep the real join of uncancellable blocking work.
    pub fn register_blocking(&self, task: &'static str, handle: std::thread::JoinHandle<()>) {
        self.with_custody(|custody| {
            custody.blocking.push(BlockingReceipt {
                task,
                state: BlockingState::Running(handle),
            });
        });
    }

    /// Records how a blocking task is ending. Its thread sets this before
    /// returning; the latest note wins.
    pub fn note_blocking(&self, task: &'static str, note: &'static str) {
        self.with_custody(|custody| custody.notes.push((task, note)));
    }

    /// Every blocking receipt with its state: `incomplete` while its thread
    /// still runs (never claimed cancelled), `panicked` when its join failed
    /// (whatever it noted), otherwise its last note, or `finished` if it left
    /// none.
    pub fn blocking_states(&self) -> Vec<(&'static str, &'static str)> {
        self.settle_finished_blocking();
        self.with_custody(|custody| {
            custody
                .blocking
                .iter()
                .map(|receipt| {
                    let state = match receipt.state {
                        BlockingState::Running(_) | BlockingState::Joining => "incomplete",
                        BlockingState::Panicked => "panicked",
                        BlockingState::Exited => custody
                            .notes
                            .iter()
                            .rev()
                            .find(|(task, _)| *task == receipt.task)
                            .map_or("finished", |(_, note)| *note),
                    };
                    (receipt.task, state)
                })
                .collect()
        })
    }

    /// Records output that was accepted for delivery and then dropped. Any
    /// nonzero count makes the host outcome incomplete, even when every
    /// thread joined cleanly.
    pub fn record_abandoned(&self, messages: u64, bytes: u64) {
        if messages == 0 && bytes == 0 {
            return;
        }
        self.with_custody(|custody| {
            custody.abandoned.messages = custody.abandoned.messages.saturating_add(messages);
            custody.abandoned.bytes = custody.abandoned.bytes.saturating_add(bytes);
        });
    }

    pub fn abandoned_output(&self) -> AbandonedOutput {
        self.with_custody(|custody| custody.abandoned)
    }

    /// Output dropped because its recipient positively closed.
    pub fn record_undeliverable_after_disconnect(&self, messages: u64, bytes: u64) {
        self.with_custody(|custody| {
            let undeliverable = &mut custody.undeliverable;
            undeliverable.messages = undeliverable.messages.saturating_add(messages);
            undeliverable.bytes = undeliverable.bytes.saturating_add(bytes);
        });
    }

    pub fn undeliverable_after_disconnect(&self) -> AbandonedOutput {
        self.with_custody(|custody| custody.undeliverable)
    }

    /// A positive signal that the stdio output recipient is gone (EPIPE, or
    /// POLLHUP/POLLERR followed by a failed write). Stdin EOF is NOT one: it
    /// only closes the inbound half.
    pub fn mark_recipient_closed(&self) {
        self.with_custody(|custody| custody.recipient_closed = true);
    }

    pub fn recipient_closed(&self) -> bool {
        self.with_custody(|custody| custody.recipient_closed)
    }

    /// Dropped stdio output: undeliverable if the recipient positively
    /// closed, otherwise abandoned (incomplete).
    pub fn record_dropped_output(&self, messages: u64, bytes: u64) {
        if self.recipient_closed() {
            self.record_undeliverable_after_disconnect(messages, bytes);
        } else {
            self.record_abandoned(messages, bytes);
        }
    }

    /// Names of (b) tasks that handed back owners at expiry.
    pub fn returned_tasks(&self) -> Vec<&'static str> {
        self.with_custody(|custody| custody.returned.clone())
    }

    /// (a) receipts without positive proof of a normal end: still running,
    /// or joined with a panic. Reported incomplete, never as stopped.
    pub fn incomplete_blocking(&self) -> Vec<&'static str> {
        self.settle_finished_blocking();
        self.with_custody(|custody| {
            custody
                .blocking
                .iter()
                .filter(|receipt| !receipt.exited_normally())
                .map(|receipt| receipt.task)
                .collect()
        })
    }

    /// (a) receipts whose thread may still run. Unlike `incomplete_blocking`
    /// this excludes panicked threads: there is nothing left to observe.
    pub fn running_blocking(&self) -> Vec<&'static str> {
        self.settle_finished_blocking();
        self.with_custody(|custody| {
            custody
                .blocking
                .iter()
                .filter(|receipt| receipt.is_running())
                .map(|receipt| receipt.task)
                .collect()
        })
    }

    /// Extract each finished original under the lock, then join it and release
    /// any arbitrary panic payload outside the lock. Concurrent observers see
    /// `Joining` as incomplete; they cannot take or replace the original join.
    fn settle_finished_blocking(&self) {
        let finished = self.with_custody(|custody| {
            custody
                .blocking
                .iter_mut()
                .enumerate()
                .filter_map(|(index, receipt)| {
                    let BlockingState::Running(handle) = &receipt.state else {
                        return None;
                    };
                    if !handle.is_finished() {
                        return None;
                    }
                    let BlockingState::Running(handle) =
                        std::mem::replace(&mut receipt.state, BlockingState::Joining)
                    else {
                        unreachable!();
                    };
                    Some((index, handle))
                })
                .collect::<Vec<_>>()
        });
        for (index, handle) in finished {
            let result = handle.join();
            self.with_custody(|custody| {
                custody.blocking[index].state = if result.is_ok() {
                    BlockingState::Exited
                } else {
                    BlockingState::Panicked
                };
            });
            // Payload destructors may re-enter reporting or register more work.
            drop(result);
        }
    }

    fn with_custody<T>(&self, f: impl FnOnce(&mut Custody) -> T) -> T {
        // A poisoned lock still holds the owners; keep using them.
        let mut custody = self
            .custody
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        f(&mut custody)
    }
}

#[cfg(test)]
#[path = "owner_expiry_tests.rs"]
mod tests;
