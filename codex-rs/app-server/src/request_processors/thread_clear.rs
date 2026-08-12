use super::thread_processor::build_thread_from_snapshot;
use super::*;
use crate::thread_state::ClearTransitionAuthorityError;
use codex_core::StartThreadOptions;
use codex_core::config::ThreadStoreConfig;
use codex_state::ClearTransitionEvidenceKind;
use codex_state::ClearTransitionEvidenceState;
use codex_state::ClearTransitionId;
use codex_state::ClearTransitionPhase;
use codex_state::ClearTransitionReserveOutcome;
use serde_json::json;

enum PersistedSuccessorLineage {
    Missing,
    Matching,
    Inconsistent,
}

impl ThreadRequestProcessor {
    pub(crate) async fn thread_clear(
        &self,
        request_id: ConnectionRequestId,
        params: ThreadClearParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        if !matches!(
            self.config.experimental_thread_store,
            ThreadStoreConfig::Local
        ) {
            return Err(clear_error(
                ThreadClearErrorCode::StateUnavailable,
                "thread/clear requires the durable local thread store",
            ));
        }
        let predecessor_thread_id = ThreadId::from_string(&params.thread_id).map_err(|err| {
            clear_error(
                ThreadClearErrorCode::UnknownPredecessor,
                format!("invalid predecessor thread id: {err}"),
            )
        })?;
        let predecessor = self
            .thread_manager
            .get_thread(predecessor_thread_id)
            .await
            .map_err(|_| {
                clear_error(
                    ThreadClearErrorCode::UnknownPredecessor,
                    format!("predecessor thread is not loaded: {predecessor_thread_id}"),
                )
            })?;
        let state_db = predecessor
            .state_db()
            .or_else(|| self.state_db.clone())
            .ok_or_else(|| {
                clear_error(
                    ThreadClearErrorCode::StateUnavailable,
                    "thread/clear requires durable state",
                )
            })?;

        if let Some(existing) = state_db
            .get_clear_transition_by_predecessor(predecessor_thread_id)
            .await
            .map_err(|err| {
                clear_error(
                    ThreadClearErrorCode::StateUnavailable,
                    format!("failed to read clear transition state: {err}"),
                )
            })?
        {
            let code = if existing.phase == ClearTransitionPhase::Completed {
                ThreadClearErrorCode::TransitionCompleted
            } else {
                ThreadClearErrorCode::TransitionConflict
            };
            return Err(clear_error(
                code,
                format!(
                    "predecessor already has clear transition {}",
                    existing.transition_id
                ),
            ));
        }

        self.thread_state_manager
            .reserve_clear_transition_authority(predecessor_thread_id, request_id.connection_id)
            .await
            .map_err(clear_authority_error)?;
        let result = self
            .thread_clear_authorized(
                request_id.connection_id,
                predecessor_thread_id,
                predecessor,
                state_db,
            )
            .await;
        self.thread_state_manager
            .release_clear_transition_authority(predecessor_thread_id)
            .await;
        result.map(|response| Some(response.into()))
    }

    /// Reconciles interrupted clear transitions without replaying requester-scoped evidence.
    ///
    /// Connection identities are process-local, so restart recovery can only converge durable
    /// state. Any evidence that was not confirmed before the interruption becomes terminally
    /// failed; it is never guessed, broadcast, or replayed to another connection.
    pub(crate) async fn reconcile_incomplete_clear_transitions(&self) {
        if !matches!(
            self.config.experimental_thread_store,
            ThreadStoreConfig::Local
        ) {
            return;
        }
        let Some(state_db) = self.state_db.as_ref() else {
            return;
        };
        let records = match state_db.list_incomplete_clear_transitions().await {
            Ok(records) => records,
            Err(err) => {
                warn!("failed to enumerate incomplete clear transitions: {err}");
                return;
            }
        };
        for record in records {
            let transition_id = record.transition_id;
            if let Err(err) = self.reconcile_clear_transition(state_db, record).await {
                warn!(
                    transition_id = %transition_id,
                    "failed to reconcile clear transition: {err}"
                );
            }
        }
    }

    async fn reconcile_clear_transition(
        &self,
        state_db: &codex_state::StateRuntime,
        mut record: codex_state::ClearTransitionRecord,
    ) -> anyhow::Result<()> {
        if record.phase == ClearTransitionPhase::Reserved {
            match self.persisted_successor_lineage(&record).await? {
                PersistedSuccessorLineage::Missing => {
                    if !state_db
                        .abandon_clear_transition(
                            record.transition_id,
                            ClearTransitionPhase::Reserved,
                        )
                        .await?
                    {
                        anyhow::bail!("reserved transition changed before it could be abandoned");
                    }
                    return Ok(());
                }
                PersistedSuccessorLineage::Matching => {
                    advance_recovery_phase(
                        state_db,
                        record.transition_id,
                        ClearTransitionPhase::Reserved,
                        ClearTransitionPhase::SuccessorCreated,
                    )
                    .await?;
                    record.phase = ClearTransitionPhase::SuccessorCreated;
                }
                PersistedSuccessorLineage::Inconsistent => {
                    anyhow::bail!("reserved successor has inconsistent immutable clear lineage");
                }
            }
        }

        if record.phase == ClearTransitionPhase::SuccessorCreated {
            if !matches!(
                self.persisted_successor_lineage(&record).await?,
                PersistedSuccessorLineage::Matching
            ) {
                anyhow::bail!("created successor is missing or has inconsistent clear lineage");
            }
            advance_recovery_phase(
                state_db,
                record.transition_id,
                ClearTransitionPhase::SuccessorCreated,
                ClearTransitionPhase::Committed,
            )
            .await?;
            record.phase = ClearTransitionPhase::Committed;
        }

        if record.phase == ClearTransitionPhase::Committed {
            advance_recovery_phase(
                state_db,
                record.transition_id,
                ClearTransitionPhase::Committed,
                ClearTransitionPhase::EvidenceClaimed,
            )
            .await?;
            record.phase = ClearTransitionPhase::EvidenceClaimed;
        }

        if record.phase == ClearTransitionPhase::EvidenceClaimed {
            terminalize_recovery_evidence(
                state_db,
                record.transition_id,
                ClearTransitionEvidenceKind::End,
                record.end_evidence_state,
            )
            .await?;
            terminalize_recovery_evidence(
                state_db,
                record.transition_id,
                ClearTransitionEvidenceKind::Start,
                record.start_evidence_state,
            )
            .await?;
            advance_recovery_phase(
                state_db,
                record.transition_id,
                ClearTransitionPhase::EvidenceClaimed,
                ClearTransitionPhase::Completed,
            )
            .await?;
        }
        Ok(())
    }

    async fn persisted_successor_lineage(
        &self,
        record: &codex_state::ClearTransitionRecord,
    ) -> anyhow::Result<PersistedSuccessorLineage> {
        let stored = match self
            .thread_store
            .read_thread(StoreReadThreadParams {
                thread_id: record.successor_thread_id,
                include_archived: true,
                include_history: true,
            })
            .await
        {
            Ok(stored) => stored,
            Err(ThreadStoreError::ThreadNotFound { .. }) => {
                return Ok(PersistedSuccessorLineage::Missing);
            }
            Err(ThreadStoreError::InvalidRequest { message })
                if message.starts_with("no rollout found for thread id ") =>
            {
                return Ok(PersistedSuccessorLineage::Missing);
            }
            Err(err) => return Err(err.into()),
        };
        let Some(meta) = stored.history.as_ref().and_then(|history| {
            history.items.iter().find_map(|item| match item {
                RolloutItem::SessionMeta(line) => Some(&line.meta),
                _ => None,
            })
        }) else {
            return Ok(PersistedSuccessorLineage::Inconsistent);
        };
        let transition_id = record.transition_id.to_string();
        Ok(
            if meta.clear_predecessor_thread_id == Some(record.predecessor_thread_id)
                && meta.clear_transition_id.as_deref() == Some(transition_id.as_str())
            {
                PersistedSuccessorLineage::Matching
            } else {
                PersistedSuccessorLineage::Inconsistent
            },
        )
    }

    async fn thread_clear_authorized(
        &self,
        connection_id: ConnectionId,
        predecessor_thread_id: ThreadId,
        predecessor: Arc<CodexThread>,
        state_db: StateDbHandle,
    ) -> Result<ThreadClearResponse, JSONRPCErrorError> {
        let transition_id = ClearTransitionId::new();
        let successor_thread_id = ThreadId::new();
        match state_db
            .reserve_clear_transition(transition_id, predecessor_thread_id, successor_thread_id)
            .await
            .map_err(|err| {
                clear_error(
                    ThreadClearErrorCode::StateUnavailable,
                    format!("failed to reserve clear transition: {err}"),
                )
            })? {
            ClearTransitionReserveOutcome::Reserved(_) => {}
            ClearTransitionReserveOutcome::PredecessorAlreadyReserved(existing) => {
                let code = if existing.phase == ClearTransitionPhase::Completed {
                    ThreadClearErrorCode::TransitionCompleted
                } else {
                    ThreadClearErrorCode::TransitionConflict
                };
                return Err(clear_error(
                    code,
                    format!(
                        "predecessor already has clear transition {}",
                        existing.transition_id
                    ),
                ));
            }
            ClearTransitionReserveOutcome::SuccessorAlreadyReserved(existing) => {
                return Err(clear_error(
                    ThreadClearErrorCode::TransitionConflict,
                    format!(
                        "successor identity is already reserved by {}",
                        existing.transition_id
                    ),
                ));
            }
        }

        let config = (*predecessor.config().await).clone();
        let predecessor_snapshot = predecessor.config_snapshot().await;
        let successor = match self
            .thread_manager
            .start_thread_for_clear(
                StartThreadOptions {
                    initial_history: InitialHistory::Cleared,
                    thread_source: predecessor_snapshot.thread_source.clone(),
                    environments: Some(predecessor_snapshot.environment_selections().to_vec()),
                    ..StartThreadOptions::new(config)
                },
                predecessor_thread_id,
                successor_thread_id,
                transition_id,
            )
            .await
        {
            Ok(successor) => successor,
            Err(err) => {
                let _ = state_db
                    .abandon_clear_transition(transition_id, ClearTransitionPhase::Reserved)
                    .await;
                return Err(clear_error(
                    ThreadClearErrorCode::SuccessorCreationFailed,
                    format!("failed to create clear successor: {err}"),
                ));
            }
        };

        if !self
            .thread_state_manager
            .reserve_clear_successor_attachment(predecessor_thread_id, successor_thread_id)
            .await
        {
            return Err(clear_error(
                ThreadClearErrorCode::TransitionConflict,
                "successor attachment is already reserved by another clear transition",
            ));
        }

        advance_phase(
            state_db.as_ref(),
            transition_id,
            ClearTransitionPhase::Reserved,
            ClearTransitionPhase::SuccessorCreated,
        )
        .await?;
        // Reserved now means no B exists. Once construction returns, record SuccessorCreated
        // before any fallible persistence work can yield an abandonment race. Publication still
        // waits for B's append-once SessionMeta lineage to become durable.
        successor
            .thread
            .try_ensure_rollout_materialized()
            .await
            .map_err(|err| {
                clear_error(
                    ThreadClearErrorCode::StateUnavailable,
                    format!("failed to persist clear successor lineage: {err}"),
                )
            })?;
        advance_phase(
            state_db.as_ref(),
            transition_id,
            ClearTransitionPhase::SuccessorCreated,
            ClearTransitionPhase::Committed,
        )
        .await?;

        let successor_config = successor.thread.config_snapshot().await;
        let mut successor_api = build_thread_from_snapshot(
            successor_thread_id,
            successor.session_configured.session_id.to_string(),
            successor.thread.multi_agent_version(),
            &successor_config,
            successor.session_configured.rollout_path.clone(),
        );
        self.thread_watch_manager
            .upsert_thread_silently(&successor_api.id)
            .await;
        successor_api.status = resolve_thread_status(
            self.thread_watch_manager
                .loaded_status_for_thread(&successor_api.id)
                .await,
            /*has_in_progress_turn*/ false,
        );

        advance_phase(
            state_db.as_ref(),
            transition_id,
            ClearTransitionPhase::Committed,
            ClearTransitionPhase::EvidenceClaimed,
        )
        .await?;
        claim_evidence(
            state_db.as_ref(),
            transition_id,
            ClearTransitionEvidenceKind::End,
        )
        .await?;
        predecessor.dispatch_clear_session_end(transition_id).await;
        let end_delivered = self
            .outgoing
            .try_send_server_notification_to_connection_and_wait(
                connection_id,
                ServerNotification::ThreadClearEnded(ThreadClearEndedNotification {
                    transition_id: transition_id.to_string(),
                    predecessor_thread_id: predecessor_thread_id.to_string(),
                    successor_thread_id: successor_thread_id.to_string(),
                    reason: ThreadClearEndReason::Clear,
                }),
            )
            .await;
        finish_evidence_with_outcome(
            state_db.as_ref(),
            transition_id,
            ClearTransitionEvidenceKind::End,
            end_delivered,
        )
        .await?;

        claim_evidence(
            state_db.as_ref(),
            transition_id,
            ClearTransitionEvidenceKind::Start,
        )
        .await?;
        let start_consumed = successor
            .thread
            .dispatch_deferred_clear_session_start()
            .await;
        let start_delivered = self
            .outgoing
            .try_send_server_notification_to_connection_and_wait(
                connection_id,
                ServerNotification::ThreadClearStarted(ThreadClearStartedNotification {
                    transition_id: transition_id.to_string(),
                    predecessor_thread_id: predecessor_thread_id.to_string(),
                    successor_thread: successor_api.clone(),
                    start_source: codex_app_server_protocol::ThreadStartSource::Clear,
                }),
            )
            .await;
        finish_evidence_with_outcome(
            state_db.as_ref(),
            transition_id,
            ClearTransitionEvidenceKind::Start,
            start_consumed && start_delivered,
        )
        .await?;

        // Start B's listener before the durable transition completes, but do
        // not subscribe it yet: the state manager moves A -> B atomically
        // after completion so no attachment notification can expose both.
        let successor_thread_state = self
            .thread_state_manager
            .thread_state(successor_thread_id)
            .await;
        self.ensure_listener_task_running(
            successor_thread_id,
            successor.thread.clone(),
            successor_thread_state,
        )
        .await?;
        advance_phase(
            state_db.as_ref(),
            transition_id,
            ClearTransitionPhase::EvidenceClaimed,
            ClearTransitionPhase::Completed,
        )
        .await?;
        if !self
            .thread_state_manager
            .move_connection_for_clear(predecessor_thread_id, successor_thread_id, connection_id)
            .await
        {
            // Completion is durable already. A connection close may win the
            // race after commit; it must not turn a completed transition into
            // a contradictory client error or manufacture an attachment for a
            // client that is no longer present.
            warn!(
                transition_id = %transition_id,
                "clear requester disconnected after durable completion; no attachment move was published"
            );
        }

        self.outgoing
            .send_server_notification(ServerNotification::ThreadStarted(
                thread_started_notification(
                    successor_api.clone(),
                    Some(codex_app_server_protocol::ThreadStartSource::Clear),
                    Some(predecessor_thread_id.to_string()),
                ),
            ))
            .await;

        Ok(ThreadClearResponse {
            transition_id: transition_id.to_string(),
            predecessor_thread_id: predecessor_thread_id.to_string(),
            successor_thread: successor_api,
        })
    }
}

async fn advance_recovery_phase(
    state_db: &codex_state::StateRuntime,
    transition_id: ClearTransitionId,
    expected: ClearTransitionPhase,
    next: ClearTransitionPhase,
) -> anyhow::Result<()> {
    if !state_db
        .advance_clear_transition_phase(transition_id, expected, next)
        .await?
    {
        anyhow::bail!("transition did not advance from {expected} to {next}");
    }
    Ok(())
}

async fn terminalize_recovery_evidence(
    state_db: &codex_state::StateRuntime,
    transition_id: ClearTransitionId,
    kind: ClearTransitionEvidenceKind,
    state: ClearTransitionEvidenceState,
) -> anyhow::Result<()> {
    match state {
        ClearTransitionEvidenceState::Pending => {
            if !state_db
                .advance_clear_transition_evidence(
                    transition_id,
                    kind,
                    ClearTransitionEvidenceState::Pending,
                    ClearTransitionEvidenceState::Claimed,
                )
                .await?
            {
                anyhow::bail!("pending evidence could not be claimed during recovery");
            }
            if !state_db
                .advance_clear_transition_evidence(
                    transition_id,
                    kind,
                    ClearTransitionEvidenceState::Claimed,
                    ClearTransitionEvidenceState::Failed,
                )
                .await?
            {
                anyhow::bail!("claimed evidence could not be failed during recovery");
            }
        }
        ClearTransitionEvidenceState::Claimed => {
            if !state_db
                .advance_clear_transition_evidence(
                    transition_id,
                    kind,
                    ClearTransitionEvidenceState::Claimed,
                    ClearTransitionEvidenceState::Failed,
                )
                .await?
            {
                anyhow::bail!("claimed evidence could not be failed during recovery");
            }
        }
        ClearTransitionEvidenceState::Delivered | ClearTransitionEvidenceState::Failed => {}
    }
    Ok(())
}

async fn advance_phase(
    state_db: &codex_state::StateRuntime,
    transition_id: ClearTransitionId,
    expected: ClearTransitionPhase,
    next: ClearTransitionPhase,
) -> Result<(), JSONRPCErrorError> {
    let advanced = state_db
        .advance_clear_transition_phase(transition_id, expected, next)
        .await
        .map_err(|err| {
            clear_error(
                ThreadClearErrorCode::StateUnavailable,
                format!("failed to advance clear transition: {err}"),
            )
        })?;
    if !advanced {
        return Err(clear_error(
            ThreadClearErrorCode::TransitionConflict,
            format!("clear transition {transition_id} did not advance from {expected} to {next}"),
        ));
    }
    Ok(())
}

async fn claim_evidence(
    state_db: &codex_state::StateRuntime,
    transition_id: ClearTransitionId,
    kind: ClearTransitionEvidenceKind,
) -> Result<(), JSONRPCErrorError> {
    let claimed = state_db
        .advance_clear_transition_evidence(
            transition_id,
            kind,
            ClearTransitionEvidenceState::Pending,
            ClearTransitionEvidenceState::Claimed,
        )
        .await
        .map_err(|err| {
            clear_error(
                ThreadClearErrorCode::StateUnavailable,
                format!("failed to claim clear evidence: {err}"),
            )
        })?;
    if !claimed {
        return Err(clear_error(
            ThreadClearErrorCode::TransitionConflict,
            format!("clear transition {transition_id} evidence was already claimed"),
        ));
    }
    Ok(())
}

async fn finish_evidence_with_outcome(
    state_db: &codex_state::StateRuntime,
    transition_id: ClearTransitionId,
    kind: ClearTransitionEvidenceKind,
    delivered: bool,
) -> Result<(), JSONRPCErrorError> {
    let next = if delivered {
        ClearTransitionEvidenceState::Delivered
    } else {
        ClearTransitionEvidenceState::Failed
    };
    let advanced = state_db
        .advance_clear_transition_evidence(
            transition_id,
            kind,
            ClearTransitionEvidenceState::Claimed,
            next,
        )
        .await
        .map_err(|err| {
            clear_error(
                ThreadClearErrorCode::StateUnavailable,
                format!("failed to record clear evidence outcome: {err}"),
            )
        })?;
    if !advanced {
        return Err(clear_error(
            ThreadClearErrorCode::TransitionConflict,
            format!("clear transition {transition_id} evidence did not reach {next}"),
        ));
    }
    Ok(())
}

fn clear_authority_error(err: ClearTransitionAuthorityError) -> JSONRPCErrorError {
    match err {
        ClearTransitionAuthorityError::UnknownPredecessor => clear_error(
            ThreadClearErrorCode::UnknownPredecessor,
            "predecessor thread is unknown",
        ),
        ClearTransitionAuthorityError::NotSubscribed => clear_error(
            ThreadClearErrorCode::NotSubscribed,
            "requester is not subscribed to predecessor thread",
        ),
        ClearTransitionAuthorityError::TransitionConflict => clear_error(
            ThreadClearErrorCode::TransitionConflict,
            "another clear transition is already in flight",
        ),
    }
}

fn clear_error(code: ThreadClearErrorCode, message: impl Into<String>) -> JSONRPCErrorError {
    let mut error = invalid_request(message);
    error.data = Some(json!({ "code": code }));
    error
}
