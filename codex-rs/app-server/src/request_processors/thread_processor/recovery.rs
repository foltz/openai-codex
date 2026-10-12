//! Recovery preserves the original failed transaction and every exact retired incarnation.
use super::ThreadRequestProcessor;
use super::invalid_request;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ThreadClosedNotification;
use codex_core::ThreadRecoveryOutcome;
use codex_protocol::ThreadId;
use std::time::Duration;
use tokio::time::Instant;

impl ThreadRequestProcessor {
    /// Claim bounded custody before shutdown can start. The request's owner keeps
    /// exclusion through successor publication, including cancellation of this wait.
    #[expect(
        clippy::await_holding_invalid_type,
        reason = "the pending reservation excludes idle claims while exact authority is checked"
    )]
    pub(super) async fn retire_cached_thread_for_resume(
        &self,
        id: ThreadId,
        old: &std::sync::Arc<codex_core::CodexThread>,
    ) -> Result<bool, JSONRPCErrorError> {
        use crate::thread_state::RetentionRetirementRefusal;

        let mut pending = self.pending_thread_unloads.lock().await;
        pending.reserve(id).map_err(invalid_request)?;
        let current =
            self.thread_manager.get_thread(id).await.map_err(|_| {
                invalid_request(format!("thread {id} changed during resume; retry"))
            })?;
        if !std::sync::Arc::ptr_eq(old, &current) {
            return Err(invalid_request(format!(
                "thread {id} runtime changed during resume; retry"
            )));
        }
        let retention = self
            .thread_state_manager
            .subscribe_to_retention(id)
            .await
            .ok_or_else(|| {
                invalid_request(format!("thread {id} retirement authority unavailable"))
            })?;
        let expected = *retention.borrow();
        if expected.retiring {
            drop(pending);
            return self.recover_retiring_thread(id).await;
        }
        let state = self.thread_state_manager.thread_state(id).await;
        let generation = state.try_lock().map(|state| state.listener_generation);
        let claim = match generation {
            Ok(generation) => {
                self.thread_state_manager
                    .claim_thread_retirement(
                        id,
                        expected,
                        old,
                        generation,
                        Instant::now() + Duration::from_secs(10),
                    )
                    .await
            }
            Err(_) => Err(RetentionRetirementRefusal::Busy),
        };
        match claim {
            Ok((claim, ticket)) => {
                drop(pending);
                let report = ticket.wait().await;
                if !self
                    .thread_state_manager
                    .record_retirement_report(claim, report)
                    .await
                {
                    return Err(invalid_request(format!(
                        "thread {id} retirement authority changed"
                    )));
                }
                if !report.is_complete() {
                    tracing::warn!(thread_id = %id, ?report, "resume override retirement remains incomplete");
                }
                // Both complete and incomplete observations retain the original
                // ticket and join the exact listener before removing this runtime.
                self.recover_retiring_thread(id).await
            }
            Err(RetentionRetirementRefusal::AlreadyRetiring) => {
                drop(pending);
                self.recover_retiring_thread(id).await
            }
            Err(
                reason @ (RetentionRetirementRefusal::Retained
                | RetentionRetirementRefusal::Busy
                | RetentionRetirementRefusal::ActiveWork
                | RetentionRetirementRefusal::Changed
                | RetentionRetirementRefusal::DeadlineExpired),
            ) => {
                // A refused claim supplies no shutdown proof. Rejoin only the
                // same still-open incarnation, revalidated under exclusion.
                let current = self.thread_manager.get_thread(id).await.ok();
                let snapshot = self.thread_state_manager.subscribe_to_retention(id).await;
                if current
                    .as_ref()
                    .is_some_and(|current| std::sync::Arc::ptr_eq(old, current))
                    && !old.is_closing()
                    && snapshot
                        .as_ref()
                        .is_some_and(|snapshot| !snapshot.borrow().retiring)
                {
                    // The existing listener handles a warm response outside
                    // this task's owner context. No shutdown was committed;
                    // restore ordinary final-attach admission, which rechecks
                    // pending claims and exact retiring authority itself.
                    if !pending.release_owned(&id) {
                        return Err(invalid_request(format!(
                            "thread {id} resume reservation owner changed"
                        )));
                    }
                    Ok(false)
                } else {
                    Err(invalid_request(format!(
                        "thread {id} resume claim refused: {reason:?}; runtime is not usable"
                    )))
                }
            }
            Err(
                reason @ (RetentionRetirementRefusal::UnknownThread
                | RetentionRetirementRefusal::AuthorityUnavailable),
            ) => Err(invalid_request(format!(
                "thread {id} resume retirement unavailable: {reason:?}"
            ))),
        }
    }

    pub(super) async fn recover_retiring_thread(
        &self,
        id: ThreadId,
    ) -> Result<bool, JSONRPCErrorError> {
        self.recover_retiring_thread_until(id, Instant::now() + Duration::from_secs(10))
            .await
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "the pending reservation lock excludes idle claims during exact lifecycle lookup"
    )]
    pub(crate) async fn recover_retiring_thread_until(
        &self,
        id: ThreadId,
        deadline: Instant,
    ) -> Result<bool, JSONRPCErrorError> {
        // Reserve before inspecting authority: the idle claim uses this same synchronization.
        let mut pending = self.pending_thread_unloads.lock().await;
        if pending.blocks(&id) {
            return Err(invalid_request(format!(
                "thread {id} is closing; retry after the transition completes"
            )));
        }
        let candidate = self
            .thread_state_manager
            .retirement_candidate(id)
            .await
            .map_err(|reason| {
                invalid_request(format!("thread {id} recovery unavailable: {reason}"))
            })?;
        let Some(candidate) = candidate else {
            if let Ok(current) = self.thread_manager.get_thread(id).await
                && current.is_closing()
            {
                return Err(invalid_request(format!(
                    "thread {id} recovery blocked: original retirement custody unavailable"
                )));
            }
            return Ok(false);
        };
        let claim = candidate.claim;
        let old = candidate.runtime;
        pending.reserve(id).map_err(invalid_request)?;
        drop(pending);

        // A cancelled request may leave the original retained observer unpolled.
        // Finish observing that exact transaction only within its existing bound.
        let report = tokio::time::timeout_at(deadline, candidate.ticket.wait())
            .await
            .map_err(|_| {
                invalid_request(format!(
                    "thread {id} recovery blocked: original retirement observation timed out"
                ))
            })?;
        self.thread_state_manager
            .record_retirement_report(claim, report)
            .await;

        if !old.retirement_is_quiescent() {
            return Err(invalid_request(format!(
                "thread {id} recovery blocked: task, loop or persistence cleanup is unproven"
            )));
        }
        if let Some(state) = candidate.listener {
            let mut state = state.lock().await;
            if let Some(cancel) = state.cancel_tx.take() {
                let _ = cancel.send(());
            }
        }
        let Some(completion) = candidate.completion else {
            return Err(invalid_request(format!(
                "thread {id} recovery blocked: listener join custody unavailable"
            )));
        };
        {
            if tokio::time::timeout_at(deadline, completion).await != Ok(true) {
                return Err(invalid_request(format!(
                    "thread {id} recovery blocked: old listener has not joined"
                )));
            }
        }
        let outcome = old.reconcile_retirement_until(deadline).await;
        if outcome == ThreadRecoveryOutcome::Ineligible {
            return Err(invalid_request(format!(
                "thread {id} recovery evidence changed"
            )));
        }
        if !self
            .thread_state_manager
            .archive_retirement(claim, &old)
            .await
        {
            return Err(invalid_request(format!(
                "thread {id} recovery authority changed"
            )));
        }
        // Archived custody persists even if this observer is cancelled after exact-map removal.
        if self
            .thread_manager
            .remove_thread_if_same(&id, &old)
            .await
            .is_none()
            && self.thread_manager.get_thread(id).await.is_ok()
        {
            return Err(invalid_request(format!(
                "thread {id} recovery runtime changed"
            )));
        }
        self.finalize_thread_teardown(id).await;
        self.outgoing
            .send_server_notification(ServerNotification::ThreadClosed(ThreadClosedNotification {
                thread_id: id.to_string(),
            }))
            .await;
        Ok(true)
    }
}

#[cfg(test)]
impl ThreadRequestProcessor {
    pub(crate) async fn pending_thread_unloads_for_test(&self, id: ThreadId) {
        self.pending_thread_unloads
            .lock()
            .await
            .reserve(id)
            .unwrap();
    }
}
