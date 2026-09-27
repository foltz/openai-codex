//! Best-effort MCP prewarming.
//!
//! A bounded channel coalesces refresh requests. The worker only prepares the
//! newest thread state; exact model steps remain the correctness path.

use super::*;

impl Session {
    pub(crate) fn request_mcp_runtime_refresh(&self) {
        // Plugin changes can reuse connections but still change their skill resources.
        self.services.mcp_runtime.invalidate_resource_caches();
        self.request_mcp_runtime_reprojection();
    }

    /// Reproject contributor state without invalidating cached MCP resources.
    pub(crate) fn request_mcp_runtime_reprojection(&self) {
        self.mark_mcp_runtime_dirty();
        self.schedule_mcp_prewarm();
    }

    pub(super) fn start_mcp_prewarm_worker(
        self: &Arc<Self>,
        requests: async_channel::Receiver<()>,
        mut auth_changes: tokio::sync::watch::Receiver<u64>,
    ) {
        let session = Arc::downgrade(self);
        let shutdown = self.mcp_prewarm_shutdown.clone();
        let worker = self.services.runtime_handle.spawn(async move {
            'worker: loop {
                let auth_changed = tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => break,
                    request = requests.recv() => {
                        if request.is_err() {
                            break;
                        }
                        false
                    },
                    auth_change = auth_changes.changed() => {
                        if auth_change.is_err() {
                            break;
                        }
                        true
                    },
                };
                let Some(mut current) = session.upgrade() else {
                    break;
                };
                if auth_changed {
                    current.mark_mcp_runtime_dirty();
                }
                // Background admission precedes the refresh semaphore and claim.
                // Waiting for reopen must not block admitted foreground work or
                // keep a session alive without any operation in progress.
                let account_work = loop {
                    let admission = current
                        .services
                        .host_admission
                        .as_ref()
                        .map_or(Ok(None), |host| host.admit_operation_work());
                    match admission {
                        Ok(work) => break work.map(super::mcp_work::McpOperationWork),
                        Err(codex_extension_api::TurnWorkRefused::Unavailable) => continue 'worker,
                        Err(codex_extension_api::TurnWorkRefused::RetryAfter(retry)) => {
                            drop(current);
                            tokio::select! {
                                biased;
                                _ = shutdown.cancelled() => break 'worker,
                                _ = retry => {},
                            }
                            let Some(resumed) = session.upgrade() else {
                                break 'worker;
                            };
                            current = resumed;
                        }
                    }
                };
                let access = account_work
                    .as_ref()
                    .map_or(codex_mcp::McpAttemptAccess::Unscoped, |work| {
                        codex_mcp::McpAttemptAccess::Admitted(work)
                    });
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => break,
                    result = current.refresh_mcp_if_dirty_with_authority(access) => {
                        if let Err(error) = result {
                            warn!("background MCP runtime refresh failed: {error:#}");
                        }
                    },
                }
            }
        });
        *self
            .mcp_prewarm_task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(session_loop_termination_from_handle(worker));
    }

    pub(super) fn schedule_mcp_prewarm(&self) {
        let _ = self.mcp_prewarm_tx.try_send(());
    }

    pub(super) async fn stop_mcp_prewarm_worker(&self) -> SessionLoopOutcome {
        self.mcp_prewarm_shutdown.cancel();
        let worker = self
            .mcp_prewarm_task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        match worker {
            Some(worker) => worker.await,
            None => SessionLoopOutcome::Normal,
        }
    }
}
