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

        if !crate::thread_state::recovery::resume_owner_available() {
            return Err(invalid_request("resume recovery has no request owner"));
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        let _resume = lock_resume_until(&self.thread_state_manager, id, deadline).await?;
        let reservation =
            UnloadReservation::acquire_until(self.pending_thread_unloads.clone(), id, deadline)
                .await?;
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
            return self
                .recover_reserved_retiring_thread(
                    id,
                    reservation,
                    Instant::now() + Duration::from_secs(10),
                )
                .await;
        }
        let state = self.thread_state_manager.thread_state(id).await;
        let generation = state.try_lock().map(|state| state.listener_generation);
        let claim = match generation {
            Ok(generation) => {
                self.thread_state_manager
                    .claim_thread_retirement(id, expected, old, generation, deadline)
                    .await
            }
            Err(_) => Err(RetentionRetirementRefusal::Busy),
        };
        match claim {
            Ok((claim, ticket)) => {
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
                self.recover_reserved_retiring_thread(
                    id,
                    reservation,
                    Instant::now() + Duration::from_secs(10),
                )
                .await
            }
            Err(RetentionRetirementRefusal::AlreadyRetiring) => {
                self.recover_reserved_retiring_thread(
                    id,
                    reservation,
                    Instant::now() + Duration::from_secs(10),
                )
                .await
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
                    reservation.release().await;
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
        if !crate::thread_state::recovery::resume_owner_available() {
            return Err(invalid_request("resume recovery has no request owner"));
        }
        let _resume = lock_resume_until(&self.thread_state_manager, id, deadline).await?;
        let reservation =
            UnloadReservation::acquire_until(self.pending_thread_unloads.clone(), id, deadline)
                .await?;
        self.recover_reserved_retiring_thread(id, reservation, deadline)
            .await
    }

    async fn recover_reserved_retiring_thread(
        &self,
        id: ThreadId,
        reservation: UnloadReservation,
        deadline: Instant,
    ) -> Result<bool, JSONRPCErrorError> {
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
            reservation.release().await;
            return Ok(false);
        };
        let claim = candidate.claim;
        let old = candidate.runtime;

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
        reservation
            .handoff()
            .map_err(|_| invalid_request("resume recovery has no request owner"))?;
        Ok(true)
    }
}

async fn lock_resume_until(
    state: &crate::thread_state::ThreadStateManager,
    id: ThreadId,
    deadline: Instant,
) -> Result<tokio::sync::OwnedMutexGuard<()>, JSONRPCErrorError> {
    if Instant::now() >= deadline {
        return Err(invalid_request("resume recovery deadline expired"));
    }
    let guard = tokio::time::timeout_at(deadline, state.lock_goal_resume(id))
        .await
        .map_err(|_| invalid_request("resume recovery lock deadline expired"))?;
    if Instant::now() >= deadline {
        return Err(invalid_request("resume recovery deadline expired"));
    }
    Ok(guard)
}

/// Owns exactly one 0.169 unload watch. Cancellation releases only this watch,
/// never a replacement. Until cleanup runs, resume/listener attach fail closed.
struct UnloadReservation {
    table: crate::thread_state::PendingThreadUnloads,
    id: ThreadId,
    sender: tokio::sync::watch::Sender<()>,
    released: bool,
}
impl UnloadReservation {
    async fn acquire_until(
        table: crate::thread_state::PendingThreadUnloads,
        id: ThreadId,
        deadline: Instant,
    ) -> Result<Self, JSONRPCErrorError> {
        if Instant::now() >= deadline {
            return Err(invalid_request("resume recovery deadline expired"));
        }
        let reservation = tokio::time::timeout_at(deadline, Self::acquire(table, id))
            .await
            .map_err(|_| invalid_request("resume recovery reservation deadline expired"))??;
        if Instant::now() >= deadline {
            return Err(invalid_request("resume recovery deadline expired"));
        }
        Ok(reservation)
    }

    async fn acquire(
        table: crate::thread_state::PendingThreadUnloads,
        id: ThreadId,
    ) -> Result<Self, JSONRPCErrorError> {
        let mut pending = table.lock().await;
        if let Some(sender) = pending.get(&id) {
            if crate::thread_state::recovery::owns_watch(id, sender) {
                let sender = sender.clone();
                drop(pending);
                return Ok(Self {
                    table,
                    id,
                    sender,
                    released: true,
                });
            }
            return Err(invalid_request(format!(
                "thread {id} is closing; retry after the transition completes"
            )));
        }
        let sender = tokio::sync::watch::channel(()).0;
        pending.insert(id, sender.clone());
        drop(pending);
        Ok(Self {
            table,
            id,
            sender,
            released: false,
        })
    }
    async fn release(mut self) {
        if !self.released {
            Self::remove_exact(&self.table, self.id, &self.sender).await;
            self.released = true;
        }
    }
    fn handoff(self) -> Result<(), Self> {
        if self.released {
            return Ok(());
        }
        crate::thread_state::recovery::retain_watch(self.id, self.sender.clone(), self)
    }
    async fn remove_exact(
        table: &crate::thread_state::PendingThreadUnloads,
        id: ThreadId,
        sender: &tokio::sync::watch::Sender<()>,
    ) {
        let mut pending = table.lock().await;
        if pending
            .get(&id)
            .is_some_and(|current| current.same_channel(sender))
        {
            pending.remove(&id);
        }
    }
}
impl Drop for UnloadReservation {
    fn drop(&mut self) {
        if !self.released {
            let table = self.table.clone();
            let id = self.id;
            let sender = self.sender.clone();
            tokio::spawn(async move {
                Self::remove_exact(&table, id, &sender).await;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn recovery_deadline_bounds_the_actual_goal_resume_lock() {
        let state = crate::thread_state::ThreadStateManager::new();
        let id = ThreadId::new();
        let held = state.lock_goal_resume(id).await;
        assert!(
            lock_resume_until(&state, id, Instant::now() + Duration::from_secs(10))
                .await
                .is_err()
        );
        drop(held);
        assert!(lock_resume_until(&state, id, Instant::now()).await.is_err());
        assert!(
            lock_resume_until(&state, id, Instant::now() + Duration::from_secs(10))
                .await
                .is_ok()
        );
    }
    #[tokio::test(start_paused = true)]
    async fn recovery_deadline_bounds_watch_reservation_without_inserting() {
        let table = std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));
        let id = ThreadId::new();
        let held = table.lock().await;
        assert!(
            UnloadReservation::acquire_until(
                table.clone(),
                id,
                Instant::now() + Duration::from_secs(10)
            )
            .await
            .is_err()
        );
        drop(held);
        assert!(!table.lock().await.contains_key(&id));
    }
    #[tokio::test]
    async fn watch_reservation_refuses_overlap_and_cannot_remove_replacement() {
        let table = std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));
        let id = ThreadId::new();
        let first = UnloadReservation::acquire(table.clone(), id).await.unwrap();
        assert!(UnloadReservation::acquire(table.clone(), id).await.is_err());
        let replacement = tokio::sync::watch::channel(()).0;
        table.lock().await.insert(id, replacement.clone());
        first.release().await;
        assert!(
            table
                .lock()
                .await
                .get(&id)
                .unwrap()
                .same_channel(&replacement)
        );
    }
    #[tokio::test]
    async fn recovered_watch_spans_publication_but_not_foreign_tasks_or_scope_exit() {
        let table = std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));
        let id = ThreadId::new();
        let (sender, mut completion) = tokio::sync::watch::channel(());
        crate::thread_state::recovery::resume_scope(async {
            let reservation = UnloadReservation::acquire(table.clone(), id).await.unwrap();
            completion = reservation.sender.subscribe();
            assert!(reservation.handoff().is_ok());
            assert!(!crate::thread_state::recovery::unload_blocks_resume(
                &*table.lock().await,
                id
            ));
            // A nested observation must not drop the outer publication fence.
            UnloadReservation::acquire(table.clone(), id)
                .await
                .unwrap()
                .release()
                .await;
            assert!(table.lock().await.contains_key(&id));
            let foreign = table.clone();
            assert!(
                tokio::spawn(async move {
                    crate::thread_state::recovery::unload_blocks_resume(&*foreign.lock().await, id)
                })
                .await
                .unwrap()
            );
            let replacement = tokio::sync::watch::channel(()).0;
            assert!(!crate::thread_state::recovery::owns_watch(id, &replacement));
        })
        .await;
        drop(sender);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), completion.changed())
                .await
                .unwrap()
                .is_err()
        );
        assert!(!table.lock().await.contains_key(&id));
    }

    #[tokio::test]
    async fn cancellation_releases_original_unload_watch() {
        let table = std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));
        let id = ThreadId::new();
        let reservation = UnloadReservation::acquire(table.clone(), id).await.unwrap();
        let mut completion = table.lock().await.get(&id).unwrap().subscribe();
        drop(reservation);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), completion.changed())
                .await
                .unwrap()
                .is_err()
        );
        assert!(!table.lock().await.contains_key(&id));
    }
}
