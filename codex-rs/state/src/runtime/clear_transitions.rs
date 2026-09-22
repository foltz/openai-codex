use crate::ClearTransitionEvidenceKind;
use crate::ClearTransitionEvidenceState;
use crate::ClearTransitionId;
use crate::ClearTransitionPhase;
use crate::ClearTransitionRecord;
use crate::ClearTransitionReserveOutcome;
use crate::StateRuntime;
use anyhow::Context;
use anyhow::bail;
use chrono::Utc;
use codex_protocol::ThreadId;
use sqlx::Row;
use std::str::FromStr;

impl StateRuntime {
    /// Atomically reserve the exact predecessor, successor, and transition identities.
    pub async fn reserve_clear_transition(
        &self,
        transition_id: ClearTransitionId,
        predecessor_thread_id: ThreadId,
        successor_thread_id: ThreadId,
    ) -> anyhow::Result<ClearTransitionReserveOutcome> {
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;

        if let Some(existing) =
            load_by_predecessor(&mut *transaction, predecessor_thread_id).await?
        {
            transaction.commit().await?;
            return Ok(ClearTransitionReserveOutcome::PredecessorAlreadyReserved(
                existing,
            ));
        }
        if let Some(existing) = load_by_successor(&mut *transaction, successor_thread_id).await? {
            transaction.commit().await?;
            return Ok(ClearTransitionReserveOutcome::SuccessorAlreadyReserved(
                existing,
            ));
        }

        let now = Utc::now().timestamp_millis();
        sqlx::query(
            r#"
INSERT INTO clear_transitions (
    transition_id,
    predecessor_thread_id,
    successor_thread_id,
    phase,
    end_evidence_state,
    start_evidence_state,
    created_at,
    updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(transition_id.to_string())
        .bind(predecessor_thread_id.to_string())
        .bind(successor_thread_id.to_string())
        .bind(ClearTransitionPhase::Reserved.as_ref())
        .bind(ClearTransitionEvidenceState::Pending.as_ref())
        .bind(ClearTransitionEvidenceState::Pending.as_ref())
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await?;

        let record = load_by_id(&mut *transaction, transition_id)
            .await?
            .context("reserved clear transition was not readable")?;
        transaction.commit().await?;
        Ok(ClearTransitionReserveOutcome::Reserved(record))
    }

    pub async fn get_clear_transition(
        &self,
        transition_id: ClearTransitionId,
    ) -> anyhow::Result<Option<ClearTransitionRecord>> {
        load_by_id(self.pool.as_ref(), transition_id).await
    }

    pub async fn get_clear_transition_by_predecessor(
        &self,
        predecessor_thread_id: ThreadId,
    ) -> anyhow::Result<Option<ClearTransitionRecord>> {
        load_by_predecessor(self.pool.as_ref(), predecessor_thread_id).await
    }

    /// Read exact successor lineage, including pending and completed records.
    /// Abandoned reservations do not establish succession and are excluded.
    pub async fn get_clear_transition_by_successor(
        &self,
        successor_thread_id: ThreadId,
    ) -> anyhow::Result<Option<ClearTransitionRecord>> {
        load_by_successor(self.pool.as_ref(), successor_thread_id).await
    }

    /// Advance one phase only when the current durable phase is the expected predecessor.
    pub async fn advance_clear_transition_phase(
        &self,
        transition_id: ClearTransitionId,
        expected: ClearTransitionPhase,
        next: ClearTransitionPhase,
    ) -> anyhow::Result<bool> {
        if !expected.can_advance_to(next) {
            bail!("invalid clear transition phase change: {expected} -> {next}");
        }
        let result = sqlx::query(
            "UPDATE clear_transitions SET phase = ?, updated_at = ? \
             WHERE transition_id = ? AND phase = ?",
        )
        .bind(next.as_ref())
        .bind(Utc::now().timestamp_millis())
        .bind(transition_id.to_string())
        .bind(expected.as_ref())
        .execute(self.pool.as_ref())
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Abandon a reservation only while no successor has been created.
    ///
    /// The retained row records the failed attempt, while reservation lookups
    /// exclude it so the predecessor and reserved successor identities can be
    /// used by a later authoritative attempt.
    pub async fn abandon_clear_transition(
        &self,
        transition_id: ClearTransitionId,
        expected: ClearTransitionPhase,
    ) -> anyhow::Result<bool> {
        if !expected.can_abandon() {
            bail!("cannot abandon clear transition from phase: {expected}");
        }
        let result = sqlx::query(
            "UPDATE clear_transitions SET phase = 'abandoned', updated_at = ? \
             WHERE transition_id = ? AND phase = ?",
        )
        .bind(Utc::now().timestamp_millis())
        .bind(transition_id.to_string())
        .bind(expected.as_ref())
        .execute(self.pool.as_ref())
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Advance one ordered evidence state without allowing claim replay or start-before-end.
    pub async fn advance_clear_transition_evidence(
        &self,
        transition_id: ClearTransitionId,
        kind: ClearTransitionEvidenceKind,
        expected: ClearTransitionEvidenceState,
        next: ClearTransitionEvidenceState,
    ) -> anyhow::Result<bool> {
        if !expected.can_advance_to(next) {
            bail!("invalid clear transition evidence change: {expected} -> {next}");
        }
        let now = Utc::now().timestamp_millis();
        let transition_id = transition_id.to_string();
        let result = match kind {
            ClearTransitionEvidenceKind::End => {
                sqlx::query(
                    "UPDATE clear_transitions SET end_evidence_state = ?, updated_at = ? \
                     WHERE transition_id = ? AND end_evidence_state = ?",
                )
                .bind(next.as_ref())
                .bind(now)
                .bind(&transition_id)
                .bind(expected.as_ref())
                .execute(self.pool.as_ref())
                .await?
            }
            ClearTransitionEvidenceKind::Start => {
                sqlx::query(
                    "UPDATE clear_transitions SET start_evidence_state = ?, updated_at = ? \
                     WHERE transition_id = ? AND start_evidence_state = ? \
                     AND end_evidence_state IN ('delivered', 'failed')",
                )
                .bind(next.as_ref())
                .bind(now)
                .bind(&transition_id)
                .bind(expected.as_ref())
                .execute(self.pool.as_ref())
                .await?
            }
        };
        Ok(result.rows_affected() == 1)
    }

    /// Load every non-completed transition for deterministic startup reconciliation.
    pub async fn list_incomplete_clear_transitions(
        &self,
    ) -> anyhow::Result<Vec<ClearTransitionRecord>> {
        let rows = sqlx::query(
            "SELECT * FROM clear_transitions WHERE phase NOT IN ('completed', 'abandoned') \
             ORDER BY created_at, transition_id",
        )
        .fetch_all(self.pool.as_ref())
        .await?;
        rows.iter().map(record_from_row).collect()
    }
}

async fn load_by_id<'e, E>(
    executor: E,
    transition_id: ClearTransitionId,
) -> anyhow::Result<Option<ClearTransitionRecord>>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let row = sqlx::query("SELECT * FROM clear_transitions WHERE transition_id = ?")
        .bind(transition_id.to_string())
        .fetch_optional(executor)
        .await?;
    row.as_ref().map(record_from_row).transpose()
}

async fn load_by_predecessor<'e, E>(
    executor: E,
    predecessor_thread_id: ThreadId,
) -> anyhow::Result<Option<ClearTransitionRecord>>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let row = sqlx::query(
        "SELECT * FROM clear_transitions \
         WHERE predecessor_thread_id = ? AND phase != 'abandoned'",
    )
    .bind(predecessor_thread_id.to_string())
    .fetch_optional(executor)
    .await?;
    row.as_ref().map(record_from_row).transpose()
}

async fn load_by_successor<'e, E>(
    executor: E,
    successor_thread_id: ThreadId,
) -> anyhow::Result<Option<ClearTransitionRecord>>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let row = sqlx::query(
        "SELECT * FROM clear_transitions \
         WHERE successor_thread_id = ? AND phase != 'abandoned'",
    )
    .bind(successor_thread_id.to_string())
    .fetch_optional(executor)
    .await?;
    row.as_ref().map(record_from_row).transpose()
}

fn record_from_row(row: &sqlx::sqlite::SqliteRow) -> anyhow::Result<ClearTransitionRecord> {
    let transition_id = ClearTransitionId::from_str(row.try_get("transition_id")?)?;
    let predecessor_thread_id: String = row.try_get("predecessor_thread_id")?;
    let successor_thread_id: String = row.try_get("successor_thread_id")?;
    let predecessor_thread_id = ThreadId::from_string(&predecessor_thread_id)?;
    let successor_thread_id = ThreadId::from_string(&successor_thread_id)?;
    let phase = ClearTransitionPhase::from_str(row.try_get("phase")?)?;
    let end_evidence_state =
        ClearTransitionEvidenceState::from_str(row.try_get("end_evidence_state")?)?;
    let start_evidence_state =
        ClearTransitionEvidenceState::from_str(row.try_get("start_evidence_state")?)?;
    Ok(ClearTransitionRecord {
        transition_id,
        predecessor_thread_id,
        successor_thread_id,
        phase,
        end_evidence_state,
        start_evidence_state,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

#[cfg(test)]
#[path = "clear_transitions_tests.rs"]
mod tests;
