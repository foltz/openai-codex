//! Retains persistence acquisition and resources throughout managed startup.
//! The thread manager drops initialization, then joins acquisition and resource cleanup here.

use std::sync::Arc;
use std::sync::OnceLock;

use codex_protocol::protocol::Op;
use codex_thread_store::LiveThreadInitGuard;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use super::SessionIo;
use super::session::Session;

#[derive(Default)]
pub(crate) struct SessionStartup {
    // Cancellation must be consumed inside the retained constructor before cleanup.
    pub(crate) stop: CancellationToken,
    pub(crate) persistence: Arc<Mutex<LiveThreadInitGuard>>,
    pub(crate) custody: OnceLock<Arc<super::startup_custody::SessionStartupCustody>>,
    pub(crate) session: OnceLock<Arc<Session>>,
    pub(crate) io: OnceLock<SessionIo>,
}

impl SessionStartup {
    pub(crate) async fn cleanup(&self) {
        if let Some(io) = self.io.get() {
            // The session loop owns persistence now. Preserve its shutdown semantics even
            // if registration or the caller's handoff was interrupted after the loop started.
            self.persistence.lock().await.commit();
            let _ = io.submit(Op::Interrupt).await;
            let _ = io.shutdown_and_wait().await;
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
                    tracing::warn!(?cleanup, "managed startup runtime cleanup incomplete");
                }
            }
            let mut persistence = std::mem::take(&mut *self.persistence.lock().await);
            persistence.discard().await;
        }
    }
}
