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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::sync::Notify;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConstructionOutcome {
    Returned,
    Panicked,
}

struct Construction {
    completion: Shared<BoxFuture<'static, ConstructionOutcome>>,
    account_dependent: bool,
    startup: Arc<SessionStartupCustody>,
    publication: ConstructionPublication,
}

#[cfg(test)]
type PublicationProbe = Arc<dyn Fn(&Arc<CodexThread>) + Send + Sync>;

#[derive(Default)]
struct State {
    closed: bool,
    deadline: Option<Instant>,
    // The host's shutdown-edge deadline (latched, only tightens). A runtime
    // published after it is bound at publication.
    shutdown_deadline: Option<Instant>,
    constructions: Vec<Construction>,
    panicked: bool,
    terminal: Option<ConstructionDrain>,
    published: Vec<Arc<CodexThread>>,
    compacted: usize,
    #[cfg(test)]
    pause_after_publish: Option<(Arc<Notify>, Arc<Notify>)>,
    /// Runs right after a runtime becomes visible in the registry and the
    /// lookup map, outside the registry mutex.
    #[cfg(test)]
    publication_probe: Option<PublicationProbe>,
    /// Runs at the start of every compaction, outside the registry mutex.
    #[cfg(test)]
    compaction_probe: Option<Arc<dyn Fn() + Send + Sync>>,
}

#[path = "population.rs"]
mod population;

#[derive(Clone, Default)]
pub(super) struct ThreadConstructions {
    state: Arc<Mutex<State>>,
    activity: Arc<Notify>,
}

#[derive(Clone)]
pub(super) struct ConstructionTicket(Weak<Mutex<State>>, Arc<Notify>);

/// Caller cancellation and final map publication share one synchronous gate.
/// Observer departure before the publication commit is also delivered to the
/// retained constructor as a cancellation request; after the commit it is an
/// ordinary departure and the published thread takes ordinary retirement.
#[derive(Clone)]
pub(super) struct ConstructionPublication {
    gate: Arc<Mutex<PublicationGate>>,
    cancel: CancellationToken,
    expired: CancellationToken,
    started: Arc<AtomicBool>,
    registry: Weak<Mutex<State>>,
    activity: Arc<Notify>,
}

struct PublicationGate {
    alive: bool,
    published: bool,
}

struct ObserverGuard {
    publication: ConstructionPublication,
    completion: Shared<BoxFuture<'static, ConstructionOutcome>>,
}

impl Drop for ObserverGuard {
    fn drop(&mut self) {
        // Check-and-refuse under the publication gate: either the commit has
        // happened (no request), or no commit can follow (alive is false).
        let published = {
            let mut gate = self
                .publication
                .gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            gate.alive = false;
            gate.published
        };
        // A published start takes ordinary retirement and a finished one needs
        // nothing; neither receives a constructor cancellation.
        if published || self.completion.peek().is_some() {
            return;
        }
        // Every other departure is an idempotent cancellation request to the
        // retained constructor, whether or not admission is still open and
        // whether or not it was ever polled. The request drives nothing by
        // itself: the constructor consumes it only if something else polls
        // it, its cancellation arm is gated by `unless_published`, and its
        // cleanup stays clamped to the registry deadline, so it can neither
        // extend nor bypass shutdown.
        self.publication.cancel.cancel();
        // The checks below only decide whether to spawn a progress observer.
        // A never-polled constructor owns no startup work and must not be
        // started by its departing observer. Once admission is closed the
        // manager drain owns driving this constructor to its deadline.
        if !self.publication.started.load(Ordering::Acquire) {
            return;
        }
        let open = self.publication.registry.upgrade().is_some_and(|registry| {
            let state = registry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            !state.closed
        });
        if !open {
            return;
        }
        // The registry keeps the original Shared and its recorded outcome. This
        // clone only supplies progress, and stops at the same clamped registry
        // deadline as the constructor's cleanup. Dropping it (e.g. runtime
        // shutdown) leaves the original, still counted, in the registry.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let mut completion = self.completion.clone();
            let clock = self.publication.clone();
            drop(handle.spawn(async move {
                let _ = observe_until_expiry(&mut completion, &clock).await;
            }));
        }
    }
}

impl ConstructionPublication {
    /// The commit is recorded under the same gate as the insert, and only if
    /// the insert succeeded.
    pub(super) fn publish<T>(
        &self,
        publication: impl FnOnce() -> CodexResult<T>,
    ) -> CodexResult<T> {
        let mut gate = self.gate.lock().map_err(|_| CodexErr::InternalAgentDied)?;
        if !gate.alive {
            return Err(CodexErr::InternalAgentDied);
        }
        let published = publication()?;
        gate.published = true;
        Ok(published)
    }

    /// Registry-expiry refusal through the same gate as the commit. Returns
    /// false (and does nothing) if this start already published: ordinary
    /// retirement applies. Otherwise publication can no longer commit and the
    /// constructor is asked to stop.
    pub(super) fn refuse_unpublished(&self) -> bool {
        {
            let mut gate = self
                .gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if gate.published {
                return false;
            }
            gate.alive = false;
        }
        self.expired.cancel();
        true
    }

    pub(super) fn expiry_cancellation(&self) -> CancellationToken {
        self.expired.clone()
    }

    /// Resolves after `signal`, unless this start has committed publication by
    /// then; a published start only leaves through ordinary retirement.
    pub(super) async fn unless_published(&self, signal: impl std::future::Future<Output = ()>) {
        signal.await;
        if self.is_published() {
            std::future::pending::<()>().await;
        }
    }

    pub(super) fn is_published(&self) -> bool {
        self.gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .published
    }

    /// Cancelled when the caller's observer goes away before completion and
    /// before the publication commit, including after admission has closed.
    pub(super) fn observer_cancellation(&self) -> CancellationToken {
        self.cancel.clone()
    }

    pub(in crate::thread_manager) fn registry_deadline(&self) -> Option<Instant> {
        let registry = self.registry.upgrade()?;
        let state = registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match (state.deadline, state.shutdown_deadline) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// Resolves at the earliest of `local` and the registry's latched
    /// deadline, re-reading when admission closes or a drain latches one.
    pub(super) async fn expiry(&self, local: Option<Instant>) {
        ConstructionTicket(self.registry.clone(), Arc::clone(&self.activity))
            .expiry(local)
            .await;
    }

    /// The constructor's own terminal custody cleanup. It is bound to the
    /// earliest applicable deadline and is never driven past a registry
    /// deadline latched later. At expiry it stops polling, tightens the
    /// custody bound and returns `None`; the retained cell then reports a
    /// sticky `TimedOut` to every drain.
    pub(super) async fn clamped_cleanup(
        &self,
        custody: &SessionStartupCustody,
        budget: std::time::Duration,
    ) -> Option<StartupCleanup> {
        let local = Instant::now() + budget;
        let deadline = self
            .registry_deadline()
            .map_or(local, |registry| registry.min(local));
        let cleanup = tokio::select! {
            biased;
            _ = self.expiry(Some(local)) => None,
            cleanup = custody.shutdown_until(deadline) => Some(cleanup),
        };
        if cleanup.is_none() {
            let expired = self
                .registry_deadline()
                .map_or(local, |registry| registry.min(local));
            custody.tighten_deadline(expired.min(Instant::now()));
        }
        cleanup
    }

    #[cfg(test)]
    pub(super) async fn wait_after_publish_for_test(&self) {
        let Some(registry) = self.registry.upgrade() else {
            return;
        };
        let pause = {
            let state = registry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.pause_after_publish.clone()
        };
        drop(registry);
        if let Some((entered, release)) = pause {
            entered.notify_one();
            release.notified().await;
        }
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
    #[cfg(test)]
    pub(in crate::thread_manager) fn retains_without_compaction(
        &self,
        thread: &Arc<CodexThread>,
    ) -> bool {
        self.state
            .lock()
            .unwrap()
            .published
            .iter()
            .any(|owned| Arc::ptr_eq(owned, thread))
    }

    /// Latch the host's shutdown-edge deadline (only tightens) and return the
    /// published population to bind now. Synchronous: one short std mutex.
    pub(super) fn latch_shutdown_deadline(&self, deadline: Instant) -> Vec<Arc<CodexThread>> {
        population::compact(&self.state);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let deadline = state
            .shutdown_deadline
            .map_or(deadline, |latched| latched.min(deadline));
        state.shutdown_deadline = Some(deadline);
        let published = state.published.clone();
        let startups = state
            .constructions
            .iter()
            .map(|entry| Arc::clone(&entry.startup))
            .collect::<Vec<_>>();
        drop(state);
        // Early resources need the same permanent clock as already-published Sessions.
        // Never take a startup/cleanup mutex under the registry mutex.
        for startup in startups {
            startup.tighten_shutdown_edge(deadline);
        }
        self.activity.notify_waiters();
        published
    }

    pub(super) fn close_until(&self, deadline: Instant) -> CodexResult<Instant> {
        let mut state = self.state.lock().map_err(|_| CodexErr::InternalAgentDied)?;
        state.closed = true;
        self.activity.notify_waiters();
        let deadline = state
            .shutdown_deadline
            .map_or(deadline, |edge| edge.min(deadline));
        let bound = state.deadline.map_or(deadline, |first| first.min(deadline));
        state.deadline = Some(bound);
        Ok(bound)
    }

    pub(super) fn ticket(&self) -> ConstructionTicket {
        ConstructionTicket(Arc::downgrade(&self.state), Arc::clone(&self.activity))
    }

    /// Progress only constructors that already carry account authority. This
    /// observer neither closes admission nor begins retirement. Dropping it
    /// leaves the original Shared futures in this registry.
    pub(super) async fn drive_account_work(&self) {
        loop {
            let changed = self.activity.notified();
            let pending = {
                let state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if state.closed {
                    return;
                }
                state
                    .constructions
                    .iter()
                    .filter(|entry| entry.account_dependent && entry.completion.peek().is_none())
                    .map(|entry| entry.completion.clone())
                    .collect::<Vec<_>>()
            };
            if pending.is_empty() {
                changed.await;
            } else {
                tokio::select! {
                    biased;
                    _ = changed => {},
                    _ = futures::future::join_all(pending) => {},
                }
            }
        }
    }

    pub(super) async fn retire_completed_startup_until(&self, deadline: Instant) -> bool {
        let (finished, startup) = {
            let Ok(state) = self.state.lock() else {
                return false;
            };
            // Reset concerns current resources, not the historical panic bit
            // retained for the permanent manager-retirement report.
            let finished = state
                .constructions
                .iter()
                .all(|entry| entry.completion.peek() == Some(&ConstructionOutcome::Returned));
            let startup = state
                .constructions
                .iter()
                .filter(|entry| entry.completion.peek().is_some())
                .map(|entry| Arc::clone(&entry.startup))
                .collect::<Vec<_>>();
            (finished, startup)
        };
        let results = futures::future::join_all(startup.into_iter().map(|startup| async move {
            // Like reusable bulk thread shutdown, bound this observation of
            // retained legacy cleanup. Do not latch an account-reset deadline
            // into the separate permanent-manager retirement transaction.
            tokio::time::timeout_at(deadline, startup.shutdown_legacy())
                .await
                .unwrap_or(false)
        }))
        .await;
        finished && results.into_iter().all(|complete| complete)
    }

    #[cfg(test)]
    pub(super) fn close(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closed = true;
        self.activity.notify_waiters();
    }

    pub(super) async fn drain_until(&self, deadline: Instant) -> ConstructionDrain {
        let (deadline, pending, unavailable) = {
            let unavailable = self.state.is_poisoned();
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.closed = true;
            self.activity.notify_waiters();
            if !unavailable && let Some(report) = &state.terminal {
                return report.clone();
            }
            let deadline = state
                .shutdown_deadline
                .map_or(deadline, |edge| edge.min(deadline));
            let deadline = state.deadline.map_or(deadline, |first| first.min(deadline));
            state.deadline = Some(deadline);
            let pending = state
                .constructions
                .iter()
                .map(|entry| {
                    (
                        entry.completion.clone(),
                        Arc::clone(&entry.startup),
                        entry.publication.clone(),
                    )
                })
                .collect::<Vec<_>>();
            (deadline, pending, unavailable)
        };
        let observed = futures::future::join_all(pending.into_iter().map(
            |(mut completion, startup, publication)| async move {
                // Unlike a JoinHandle, polling this future can start real work.
                let outcome = if let Some(outcome) = completion.peek() {
                    Some(*outcome)
                } else if Instant::now() >= deadline {
                    None
                } else {
                    tokio::select! {
                        biased;
                        _ = tokio::time::sleep_until(deadline) => None,
                        outcome = &mut completion => Some(outcome),
                    }
                };
                // A missing outcome means the deadline has passed. An
                // unpublished constructor is refused through its publication
                // gate; a started one gets a single termination poll in which
                // it drops its start and only inspects its elapsed custody. Its
                // cell is then inspected under the elapsed bound, so pending
                // custody reports a sticky TimedOut and an unfinished
                // constructor stays counted. A published one retires ordinarily.
                let refused = outcome.is_none() && publication.refuse_unpublished();
                if refused {
                    startup.tighten_deadline(deadline);
                }
                let outcome = match outcome {
                    None if refused && publication.started.load(Ordering::Acquire) => {
                        (&mut completion).now_or_never()
                    }
                    outcome => outcome,
                };
                let cleanup = if outcome.is_some() || refused {
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
    /// The registry's latched deadline, if it is still reachable.
    pub(super) fn deadline(&self) -> Option<Instant> {
        let registry = self.0.upgrade()?;
        let state = registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match (state.deadline, state.shutdown_deadline) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// Resolves at the earliest of `local` and the registry's latched
    /// deadline, re-reading when admission closes or a drain latches one.
    /// Holds only a weak registry reference.
    pub(super) async fn expiry(&self, local: Option<Instant>) {
        loop {
            let changed = self.1.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let deadline = match (local, self.deadline()) {
                (Some(local), Some(registry)) => Some(local.min(registry)),
                (local, registry) => local.or(registry),
            };
            match deadline {
                // An already-elapsed deadline resolves synchronously, without
                // waiting for the next timer tick, so a caller polling expiry
                // first can never poll new work after the deadline.
                Some(deadline) if Instant::now() >= deadline => return,
                Some(deadline) => tokio::select! {
                    biased;
                    _ = tokio::time::sleep_until(deadline) => return,
                    _ = &mut changed => {}
                },
                None => changed.await,
            }
        }
    }

    pub(super) fn register<F, Fut>(
        &self,
        account_work: Option<Box<dyn codex_extension_api::HostOperationWork>>,
        factory: F,
    ) -> CodexResult<BoxFuture<'static, CodexResult<NewThread>>>
    where
        F: FnOnce(Arc<SessionStartupCustody>, ConstructionPublication) -> Fut + Send + 'static,
        Fut: Future<Output = CodexResult<NewThread>> + Send + 'static,
    {
        let registry = self.0.upgrade().ok_or(CodexErr::InternalAgentDied)?;
        population::compact(&registry);
        let mut state = registry.lock().map_err(|_| CodexErr::InternalAgentDied)?;
        if state.closed
            || state
                .shutdown_deadline
                .is_some_and(|edge| Instant::now() >= edge)
        {
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
        if let Some(edge) = state.shutdown_deadline {
            startup.tighten_shutdown_edge(edge);
        }
        // Lets lifetime cleanup outside this registry prove registry expiry.
        let probe = self.clone();
        startup.set_registry_deadline_probe(Box::new(move || probe.deadline()));
        let construction_startup = Arc::clone(&startup);
        let publication = ConstructionPublication {
            gate: Arc::new(Mutex::new(PublicationGate {
                alive: true,
                published: false,
            })),
            cancel: CancellationToken::new(),
            expired: CancellationToken::new(),
            started: Arc::default(),
            registry: Arc::downgrade(&registry),
            activity: Arc::clone(&self.1),
        };
        let observer_publication = publication.clone();
        let (sender, receiver) = oneshot::channel();
        let account_dependent = account_work.is_some();
        let completion = async move {
            let _account_work = account_work;
            publication.started.store(true, Ordering::Release);
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
            account_dependent,
            startup,
            publication: observer_publication.clone(),
        });
        self.1.notify_waiters();
        drop(state);
        let observer = ObserverGuard {
            publication: observer_publication,
            completion: completion.clone(),
        };
        // The original is installed under the close gate before any polling.
        Ok(async move {
            let mut completion = completion;
            let clock = observer.publication.clone();
            let _observer = observer;
            // A live observer stops driving at registry expiry too, and is
            // refused instead of hanging. Its own published start still
            // finishes for the caller awaiting this response.
            match observe(&mut completion, &clock, PublishedAtExpiry::Finish).await {
                Some(ConstructionOutcome::Returned) => {
                    receiver.await.map_err(|_| CodexErr::InternalAgentDied)?
                }
                Some(ConstructionOutcome::Panicked) => Err(CodexErr::InternalAgentDied),
                None => Err(CodexErr::TurnAborted),
            }
        }
        .boxed())
    }
}

/// What an observer does when the registry deadline expires after its start
/// has already committed publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublishedAtExpiry {
    /// The live caller keeps receiving its own published start.
    Finish,
    /// A departed observer stops polling; the published thread takes
    /// ordinary retirement and the registry keeps the original Shared.
    Release,
}

/// Progress for a departed observer. It never polls the start at or after the
/// registry deadline: at expiry an unpublished start is refused with a single
/// termination poll, and a start that had already published is simply
/// released, so no owner-retained polling of its post-publication steps
/// outlives the deadline.
async fn observe_until_expiry(
    completion: &mut Shared<BoxFuture<'static, ConstructionOutcome>>,
    clock: &ConstructionPublication,
) -> Option<ConstructionOutcome> {
    observe(completion, clock, PublishedAtExpiry::Release).await
}

/// Drive a retained constructor until it completes or the registry deadline
/// expires. At expiry an unpublished start is refused through the publication
/// gate and receives one termination poll, in which its biased expiry arm
/// drops the start (e.g. pending MCP startup) and its clamped cleanup sees an
/// elapsed bound; nothing is driven after that. A published start either
/// keeps completing its post-publication steps for its live caller, or is
/// released by a departed observer.
async fn observe(
    completion: &mut Shared<BoxFuture<'static, ConstructionOutcome>>,
    clock: &ConstructionPublication,
    published_at_expiry: PublishedAtExpiry,
) -> Option<ConstructionOutcome> {
    if published_at_expiry == PublishedAtExpiry::Release {
        // Expiry is checked before every poll, synchronously against the
        // latched deadline as well as through the timer (which may not have
        // fired yet at the exact deadline instant). A departed observer thus
        // never polls the start at or after the deadline, except for the
        // single termination poll of a refused, unpublished start below.
        let expiry = clock.expiry(/*local*/ None);
        tokio::pin!(expiry);
        let outcome = std::future::poll_fn(|cx| {
            let elapsed = clock
                .registry_deadline()
                .is_some_and(|deadline| Instant::now() >= deadline);
            if elapsed || expiry.as_mut().poll(cx).is_ready() {
                return std::task::Poll::Ready(None);
            }
            completion.poll_unpin(cx).map(Some)
        })
        .await;
        if outcome.is_some() {
            return outcome;
        }
        if !clock.refuse_unpublished() {
            return None;
        }
        return (&mut *completion).now_or_never();
    }
    let mut expired = false;
    loop {
        tokio::select! {
            biased;
            outcome = &mut *completion => return Some(outcome),
            _ = clock.expiry(/*local*/ None), if !expired => {
                if !clock.refuse_unpublished() {
                    expired = true;
                    continue;
                }
                return (&mut *completion).now_or_never();
            }
        }
    }
}

#[cfg(test)]
#[path = "retirement_tests.rs"]
mod tests;
