//! First shutdown-edge deadline and callback admission. Production host wiring
//! is separate; a signal cancellation alone does not prove task retirement.
use crate::in_process::SHUTDOWN_ACK_TIMEOUT;
use crate::standalone_shutdown::{StandaloneShutdownOwner, poll_before};
use codex_app_server_transport::{OwnerCustody, OwnerExpiry};
use std::future::Future;
use std::pin::Pin;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// The one owner deadline, latched at the first shutdown edge. Its expiry
/// token is cancelled exactly at that deadline; nothing extends it.
///
/// The latch never holds the explicit custody owner: `new` hands it to the
/// host, which keeps it outside every task (the processor task captures the
/// latch) and gives it to the shutdown owner. The latch keeps only the
/// deadline source and a non-owning `OwnerExpiry` handle, so neither a
/// retained processor join nor any retained work can keep the custody alive.
pub(crate) struct OwnerLatch {
    source: watch::Sender<Option<Instant>>,
    expiry: OwnerExpiry,
    // Run once, at the first edge, with the original latched deadline
    // (e.g. close turn admission, bind cleanup deadlines). Each holds only
    // weak handles to what it acts on.
    intake_actions: std::sync::Mutex<Vec<IntakeAction>>,
}

type IntakeAction = Box<dyn Fn(Instant) + Send + Sync>;

impl OwnerLatch {
    /// The latch and the explicit custody owner the host must hold.
    pub(crate) fn new() -> (Self, OwnerCustody) {
        let custody = OwnerCustody::new(CancellationToken::new());
        let expiry = custody.expiry();
        (
            Self {
                source: watch::channel(None).0,
                expiry,
                intake_actions: std::sync::Mutex::new(Vec::new()),
            },
            custody,
        )
    }

    /// Readers (the telemetry actor) clamp their own caps to the latch.
    pub(crate) fn subscribe(&self) -> watch::Receiver<Option<Instant>> {
        self.source.subscribe()
    }

    /// Non-owning handle for cooperative tasks and blocking receipts.
    pub(crate) fn expiry(&self) -> OwnerExpiry {
        self.expiry.clone()
    }

    /// First caller wins; later calls return the original deadline. The
    /// first latch also arms the expiry token (requires a Tokio runtime).
    pub(crate) fn latch(&self) -> Instant {
        let latched = self.source.send_if_modified(|current| {
            if current.is_some() {
                return false;
            }
            *current = Some(Instant::now() + SHUTDOWN_ACK_TIMEOUT);
            true
        });
        let deadline = (*self.source.borrow()).unwrap_or_else(Instant::now);
        // Custody carries the absolute deadline: cooperative work checks the
        // clock against it before every poll, so a late timer task below can
        // never let work run past it.
        if let Err(refused) = self.expiry.arm(deadline) {
            // Only after the shutdown owner released the custody; nothing
            // bounded can be admitted then (`run` refuses as well).
            warn!("owner deadline not armed: {refused}");
        }
        if latched {
            let expired = self.expiry.expired_token();
            tokio::spawn(async move {
                tokio::time::sleep_until(deadline).await;
                expired.cancel();
            });
            // The first edge, wherever it is recorded (intake or the
            // processor loop), applies the shutdown at once.
            let actions = std::mem::take(
                &mut *self
                    .intake_actions
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            );
            for action in actions {
                action(deadline);
            }
        }
        deadline
    }

    /// Register an action for the first shutdown edge; runs at once with the
    /// original deadline if the edge was already latched. It must hold only
    /// weak handles (the latch is captured by the processor task).
    pub(crate) fn on_edge(&self, action: impl Fn(Instant) + Send + Sync + 'static) {
        // Serialize registration with the latch's action drain before taking
        // the deadline snapshot. Otherwise a snapshot of None can be queued
        // after the first edge already drained the callbacks.
        let mut actions = self
            .intake_actions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let latched = *self.source.borrow();
        match latched {
            Some(deadline) => {
                drop(actions);
                action(deadline);
            }
            None => actions.push(Box::new(action)),
        }
    }

    /// Owner for a host whose processor task failed before handing back its
    /// own: the original latched deadline (latched here only if no edge fired
    /// earlier), never a fresh budget. Nothing proves the processor stages
    /// finished, so the report is incomplete.
    pub(crate) fn failed_processor_owner(&self) -> StandaloneShutdownOwner {
        let mut owner = StandaloneShutdownOwner::new(self.latch(), self.expiry());
        owner.record("processor", /*clean*/ false);
        owner
    }

    /// Await `join` until it completes, or, once the owner deadline is
    /// latched, until that ORIGINAL deadline (clock checked before every
    /// poll; no fresh cap). `None`: still pending at the deadline, unpolled
    /// since, and not aborted.
    pub(crate) async fn observe_until_deadline<J>(&self, join: &mut J) -> Option<J::Output>
    where
        J: Future + Unpin,
    {
        let mut latched = self.subscribe();
        loop {
            let deadline = *latched.borrow_and_update();
            if let Some(deadline) = deadline {
                return poll_before(deadline, Pin::new(join)).await;
            }
            tokio::select! {
                biased;
                output = &mut *join => return Some(output),
                changed = latched.changed() => {
                    if changed.is_err() {
                        // Unreachable while `self` lives; latch so the wait
                        // stays bounded rather than unbounded.
                        self.latch();
                    }
                }
            }
        }
    }
}

/// Wire independent intake (stdio EOF, SIGTERM, an accepted daemon
/// shutdown request, host signals) to `latch` so the first shutdown edge is
/// recorded where it happens, before anything is queued toward a possibly
/// stuck processor. The hook holds only a `Weak` latch: handles inside
/// retained work can never keep the latch (or its owner) alive. First edge
/// wins; later edges, including the processor loop's, are no-ops.
pub(crate) fn install_intake_latch(latch: &std::sync::Arc<OwnerLatch>) {
    let weak = std::sync::Arc::downgrade(latch);
    latch.expiry().set_shutdown_edge(move || {
        if let Some(latch) = weak.upgrade() {
            latch.latch();
        }
    });
}

/// Host signal intake for the restartable transports: the first shutdown
/// signal records the edge independently of the processor loop, which may be
/// stuck before it ever polls its own signal listener.
pub(crate) fn spawn_signal_intake<S>(
    expiry: OwnerExpiry,
    first_signal: S,
) -> tokio::task::JoinHandle<()>
where
    S: Future<Output = bool> + Send + 'static,
{
    tokio::spawn(async move {
        if first_signal.await {
            expiry.mark_shutdown_edge();
        }
    })
}

#[cfg(test)]
#[path = "shutdown_intake_tests.rs"]
mod tests;
