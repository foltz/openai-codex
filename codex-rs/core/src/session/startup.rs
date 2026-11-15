//! Retains agent-tree membership and resources throughout managed startup.
//! Cleanup releases membership only after acquisition and partial session teardown finish;
//! abandoning that ownership marks tree shutdown as failed.

use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::MutexGuard;
use std::sync::OnceLock;
use std::sync::PoisonError;

use crate::agent::control::AgentTreeMembership;
use crate::agent::control::AgentTreeTeardownGuard;
use crate::thread_manager::thread_store_error_kind;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErrKind;
use codex_protocol::protocol::Op;
use codex_thread_store::LiveThreadInitGuard;
use futures::future::BoxFuture;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use super::SessionIo;
use super::session::Session;

#[derive(Default)]
pub(crate) struct SessionStartup {
    teardown: StdMutex<Option<AgentTreeTeardownGuard>>,
    // Cancellation must be consumed inside the retained constructor before cleanup.
    pub(crate) stop: CancellationToken,
    pub(crate) persistence: Arc<Mutex<LiveThreadInitGuard>>,
    pub(crate) custody: OnceLock<Arc<super::startup_custody::SessionStartupCustody>>,
    pub(crate) session: OnceLock<Arc<Session>>,
    pub(crate) io: OnceLock<SessionIo>,
}

impl SessionStartup {
    fn lock_teardown(&self) -> MutexGuard<'_, Option<AgentTreeTeardownGuard>> {
        self.teardown.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn hold_membership(&self, membership: AgentTreeMembership) {
        let teardown = membership.into_teardown_guard("session_startup", /*thread_id*/ None);
        let previous = self.lock_teardown().replace(teardown);
        assert!(previous.is_none(), "agent-tree membership already set");
    }

    pub(crate) fn set_session(&self, session: Arc<Session>) {
        let thread_id = session.thread_id;
        let mut teardown = self.lock_teardown();
        if self.session.set(session).is_ok()
            && let Some(teardown) = teardown.as_mut()
        {
            teardown.set_thread_id(thread_id);
        }
    }

    pub(crate) fn session_teardown(&self, thread_id: ThreadId) -> Option<AgentTreeTeardownGuard> {
        let teardown = self.lock_teardown();
        let teardown = teardown.as_ref()?;
        Some(teardown.clone_for_teardown("session_loop", Some(thread_id)))
    }

    /// Hand a failed manager-owned start's membership to its registry
    /// custody. The custody completes it only after its cleanup is terminal.
    pub(crate) fn take_teardown(&self) -> Option<AgentTreeTeardownGuard> {
        self.lock_teardown().take()
    }

    pub(crate) fn release_membership(&self) {
        if let Some(teardown) = self.lock_teardown().take() {
            teardown.complete();
        }
    }

    pub(crate) async fn cleanup(&self) {
        drop(self.cleanup_with_owner(Box::new(())).await);
    }

    /// Synchronous disposition for a cancelled start dropped where no Tokio
    /// runtime can drive `cleanup`. This polls nothing and starts no work, so
    /// it never progresses cleanup outside its bound. Ownership is never
    /// discarded silently: the membership records a sticky failure first.
    /// A registry custody, if present, then adopts the membership, so its
    /// existing drain or reset reports and settles it. Otherwise the
    /// uncompleted guard is dropped, which also records `GuardAbandoned`.
    pub(crate) fn abandon_without_runtime(&self) {
        let Some(mut teardown) = self.lock_teardown().take() else {
            return;
        };
        if let Some(session) = self.session.get() {
            teardown.set_thread_id(session.thread_id);
        }
        teardown.record_shutdown_failure("session_startup_guard", "dropped_without_runtime");
        tracing::warn!("cancelled session startup dropped without a Tokio runtime");
        match self.custody.get() {
            Some(custody) => custody.adopt_teardown(teardown),
            None => drop(teardown),
        }
    }

    /// Lifetime cleanup for a caller-supplied startup. `owner` is the caller's
    /// background-work token. It is handed back unless proven registry/custody
    /// expiry leaves an unpublished start without a complete receipt; then the
    /// custody retains it with the membership for the manager drain or reset.
    pub(crate) async fn cleanup_with_owner(&self, owner: Box<dyn Send>) -> Option<Box<dyn Send>> {
        if let Some(custody) = self.custody.get() {
            custody.latch_registry_expiry();
        }
        if let Some(custody) = self.custody.get()
            && !custody.is_empty()
            && let Some(deadline) = custody.elapsed_deadline()
        {
            let mut teardown = self.lock_teardown().take();
            if let Some(teardown) = teardown.as_mut()
                && let Some(session) = self.session.get()
            {
                teardown.set_thread_id(session.thread_id);
            }
            // Expiry is proven: inspect only. An elapsed bound starts no new
            // cleanup and replays an existing receipt or a sticky TimedOut.
            let receipt = custody.shutdown_until(deadline).await;
            if receipt.is_complete() {
                if let Some(teardown) = teardown {
                    teardown.complete();
                }
                return Some(owner);
            }
            if let Some(teardown) = teardown {
                custody.adopt_teardown(teardown);
            }
            custody.retain_owner(owner);
            tracing::warn!(
                ?receipt,
                "expired managed startup retained for manager drain or reset"
            );
            return None;
        }
        self.cleanup_unexpired().await;
        Some(owner)
    }

    async fn cleanup_unexpired(&self) {
        let mut teardown = self.lock_teardown().take();
        if let Some(teardown) = teardown.as_mut()
            && let Some(session) = self.session.get()
        {
            teardown.set_thread_id(session.thread_id);
        }
        if let Some(io) = self.io.get() {
            // The session loop owns persistence now. Preserve its shutdown semantics even
            // if registration or the caller's handoff was interrupted after the loop started.
            self.persistence.lock().await.commit();
            let _ = io.submit(Op::Interrupt).await;
            if let Err(error) = io.shutdown_and_wait().await {
                if let Some(teardown) = teardown.as_ref() {
                    teardown.record_shutdown_failure(
                        "stop_session_io",
                        CodexErrKind::from(&error).into(),
                    );
                }
                tracing::warn!("failed to stop cancelled session init: {error}");
            }
        } else if let Some(custody) = self.custody.get() {
            if let Some(session) = self.session.get() {
                session
                    .failed_initialization_persistence
                    .store(true, std::sync::atomic::Ordering::Release);
            }
            // The constructor is terminal. Its retained owner joins acquisition,
            // cleans any partial session, and records disposal once for both
            // this lifetime task and a later manager drain.
            if !custody.shutdown_legacy().await {
                if let Some(teardown) = teardown.as_ref() {
                    teardown.record_shutdown_failure("startup_custody", "cleanup_incomplete");
                }
                tracing::warn!("managed startup cleanup incomplete");
            }
        } else {
            if let Some(session) = self.session.get() {
                let cleanup = session.cleanup_owner().observe(Arc::clone(session)).await;
                if cleanup
                    != (super::retirement::CleanupExecution::Finished {
                        persistence_failed: false,
                    })
                {
                    if let Some(teardown) = teardown.as_ref() {
                        teardown.record_shutdown_failure(
                            "session_runtime_cleanup",
                            "cleanup_incomplete",
                        );
                    }
                    tracing::warn!(?cleanup, "managed startup runtime cleanup incomplete");
                }
            }
            let mut persistence = std::mem::take(&mut *self.persistence.lock().await);
            if let Err(error) = persistence.discard().await {
                if let Some(teardown) = teardown.as_ref() {
                    teardown.record_shutdown_failure(
                        "discard_persistence",
                        thread_store_error_kind(&error),
                    );
                }
                tracing::warn!(
                    "failed to discard thread persistence for failed session init: {error}"
                );
            }
        }
        if self.io.get().is_none()
            && let Some(session) = self.session.get()
        {
            session
                .services
                .rollout_thread_trace
                .record_ended(codex_rollout_trace::RolloutStatus::Aborted);
        }
        if let Some(teardown) = teardown {
            teardown.complete();
        }
    }
}

/// Cleans up a cancelled session start before releasing its agent-tree membership.
pub(crate) struct SessionStartupGuard {
    startup: Option<Arc<SessionStartup>>,
    /// The runtime that admitted this start. Captured at construction so that
    /// a drop on a runtime-less thread (for example the last `ThreadManager`
    /// owner released on a std thread) still hands cleanup to a driver.
    runtime: Option<tokio::runtime::Handle>,
}

impl SessionStartupGuard {
    pub(crate) fn new(startup: Arc<SessionStartup>) -> Self {
        Self {
            startup: Some(startup),
            runtime: tokio::runtime::Handle::try_current().ok(),
        }
    }

    pub(crate) fn disarm(mut self) {
        self.startup = None;
    }

    pub(crate) fn cleanup(mut self) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            if let Some(startup) = self.startup.take() {
                startup.cleanup().await;
            }
        })
    }
}

impl Drop for SessionStartupGuard {
    fn drop(&mut self) {
        let Some(startup) = self.startup.take() else {
            return;
        };
        // `tokio::spawn` panics without a runtime, and a panic here during an
        // unwind aborts the process. Cleanup ownership goes to the current
        // runtime, or else the runtime captured at construction. If that
        // runtime has shut down, it drops the task unpolled, and the sentinel
        // records the abandonment and hands membership to the registry
        // custody. Either way, cleanup is driven or reported, never lost.
        let Some(runtime) = tokio::runtime::Handle::try_current()
            .ok()
            .or_else(|| self.runtime.take())
        else {
            startup.abandon_without_runtime();
            return;
        };
        let sentinel = AbandonUnlessCleaned(Some(Arc::clone(&startup)));
        drop(runtime.spawn(async move {
            startup.cleanup().await;
            sentinel.disarm();
        }));
    }
}

/// Reports a startup whose cleanup task was dropped before it finished: for
/// example, one spawned onto a runtime that had already shut down. If cleanup
/// had already taken the membership, the guard it dropped recorded the failure
/// itself, and this is a no-op.
struct AbandonUnlessCleaned(Option<Arc<SessionStartup>>);

impl AbandonUnlessCleaned {
    fn disarm(mut self) {
        self.0 = None;
    }
}

impl Drop for AbandonUnlessCleaned {
    fn drop(&mut self) {
        if let Some(startup) = self.0.take() {
            startup.abandon_without_runtime();
        }
    }
}
