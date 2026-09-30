//! Connection-scoped coordination for explicit interactive thread retention.
//! Observation never acquires custody. Per-thread workers settle cancelled
//! acquisitions and releases before a subsequent adoption may use that grant.

use codex_app_server_client::AppServerRequestHandle;
use codex_app_server_client::TypedRequestError;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ThreadRetentionAcquireParams;
use codex_app_server_protocol::ThreadRetentionAcquireResponse;
use codex_app_server_protocol::ThreadRetentionRefusalReason;
use codex_app_server_protocol::ThreadRetentionReleaseParams;
use codex_app_server_protocol::ThreadRetentionReleaseResponse;
use codex_app_server_protocol::WarningNotification;
use color_eyre::eyre::Result;
use color_eyre::eyre::WrapErr;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::timeout;
use uuid::Uuid;

pub(crate) const UNRETAINED_WARNING: &str =
    "This connection cannot retain idle sessions. Idle retirement may close the CLI.";
const SETTLEMENT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReleaseState {
    Settled,
    Indeterminate,
}

#[derive(Clone)]
pub(crate) struct RetentionClient {
    threads: Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<ReleaseState>>>>>,
    warnings: mpsc::UnboundedSender<WarningNotification>,
    warned_ineligible: Arc<AtomicBool>,
}

impl RetentionClient {
    pub(super) fn new() -> (Self, mpsc::UnboundedReceiver<WarningNotification>) {
        let (warnings, receiver) = mpsc::unbounded_channel();
        (
            Self {
                threads: Arc::new(Mutex::new(HashMap::new())),
                warnings,
                warned_ineligible: Arc::new(AtomicBool::new(false)),
            },
            receiver,
        )
    }

    pub(super) fn warn_clear(&self, thread_id: &str, error: &color_eyre::Report) {
        let _ = self.warnings.send(WarningNotification {
            thread_id: None,
            message: format!("Clear completed, but successor {thread_id} could not be retained: {error}. The successor may already be closed or may close while idle."),
        });
    }

    pub(super) async fn retain_thread(
        &self,
        request_handle: AppServerRequestHandle,
        thread_id: String,
    ) -> Result<PendingRetention> {
        let state = {
            let mut threads = self.threads.lock().map_err(|_| color_eyre::eyre::eyre!("retention coordinator is unavailable; restart the client to re-adopt this session"))?;
            // Keep unsettled/indeterminate entries; discard idle coordination
            // entries once no worker uses them. Grants themselves live on server.
            threads.retain(|_, state| {
                Arc::strong_count(state) > 1
                    || match state.try_lock() {
                        Ok(state) => *state == ReleaseState::Indeterminate,
                        Err(_) => true,
                    }
            });
            Arc::clone(
                threads
                    .entry(thread_id.clone())
                    .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(ReleaseState::Settled))),
            )
        };
        let (ready_tx, ready_rx) = oneshot::channel();
        let (commit_tx, commit_rx) = oneshot::channel();
        tokio::spawn(async move {
            let mut state = match timeout(SETTLEMENT_TIMEOUT, state.lock_owned()).await {
                Ok(state) if *state == ReleaseState::Settled => state,
                _ => {
                    let _ = ready_tx.send(Err(color_eyre::eyre::eyre!("prior retention cleanup has not settled for {thread_id}; restart the client to re-adopt this session")));
                    return;
                }
            };
            let response = request_handle
                .request_typed::<ThreadRetentionAcquireResponse>(
                    ClientRequest::ThreadRetentionAcquire {
                        request_id: RequestId::String(format!("tui-retain-{}", Uuid::new_v4())),
                        params: ThreadRetentionAcquireParams {
                            thread_id: thread_id.clone(),
                        },
                    },
                )
                .await;
            let (result, acquired_grant) = match response {
                Ok(ThreadRetentionAcquireResponse::Acquired { grant_id }) => {
                    (Ok(false), Some(grant_id))
                }
                Ok(ThreadRetentionAcquireResponse::AlreadyHeld { .. }) => (Ok(false), None),
                Ok(ThreadRetentionAcquireResponse::Refused {
                    reason: ThreadRetentionRefusalReason::IneligiblePrincipal,
                }) => (Ok(true), None),
                Ok(ThreadRetentionAcquireResponse::Refused { reason }) => (
                    Err(color_eyre::eyre::eyre!(
                        "thread retention refused for {thread_id}: {reason:?}"
                    )),
                    None,
                ),
                Err(error) => {
                    let recovery = if matches!(&error, TypedRequestError::Server { .. }) {
                        ""
                    } else {
                        *state = ReleaseState::Indeterminate;
                        "; restart the client to re-adopt this session"
                    };
                    let message =
                        format!("thread retention failed for {thread_id}: {error}{recovery}");
                    (
                        // Reconnect inspects the typed cause; do not erase it
                        // when adding the retention-specific recovery message.
                        Err(color_eyre::Report::new(error).wrap_err(message)),
                        None,
                    )
                }
            };
            let _ = ready_tx.send(result);
            if commit_rx.await.is_err()
                && let Some(grant_id) = acquired_grant
            {
                let response = timeout(
                    SETTLEMENT_TIMEOUT,
                    request_handle.request_typed::<ThreadRetentionReleaseResponse>(
                        ClientRequest::ThreadRetentionRelease {
                            request_id: RequestId::String(format!(
                                "tui-release-{}",
                                Uuid::new_v4()
                            )),
                            params: ThreadRetentionReleaseParams {
                                thread_id,
                                grant_id,
                            },
                        },
                    ),
                )
                .await;
                if !matches!(
                    response,
                    Ok(Ok(ThreadRetentionReleaseResponse::Released {}
                        | ThreadRetentionReleaseResponse::NotHeld {}))
                ) {
                    *state = ReleaseState::Indeterminate;
                    tracing::warn!(?response, "TUI retention release did not settle");
                }
            }
        });
        let ineligible = timeout(SETTLEMENT_TIMEOUT, ready_rx)
            .await
            .wrap_err("timed out waiting for thread retention")?
            .wrap_err("TUI retention acquisition stopped")??;
        Ok(PendingRetention {
            commit_tx,
            client: self.clone(),
            ineligible,
        })
    }
}

/// A new grant is released if preparation is abandoned. Existing grants are
/// never released by provisional adoption. The worker holds per-thread ordering
/// until commit or the cleanup response, including after caller cancellation.
pub(crate) struct PendingRetention {
    commit_tx: oneshot::Sender<()>,
    client: RetentionClient,
    ineligible: bool,
}

impl std::fmt::Debug for PendingRetention {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingRetention")
            .field("ineligible", &self.ineligible)
            .finish_non_exhaustive()
    }
}

impl PendingRetention {
    pub(crate) fn commit(self) {
        let _ = self.commit_tx.send(());
        if self.ineligible && !self.client.warned_ineligible.swap(true, Ordering::Relaxed) {
            let _ = self.client.warnings.send(WarningNotification {
                thread_id: None,
                message: UNRETAINED_WARNING.to_string(),
            });
        }
    }
}

#[cfg(test)]
#[path = "retention_tests.rs"]
mod tests;
