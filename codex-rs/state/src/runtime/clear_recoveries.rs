use crate::StateRuntime;
use anyhow::bail;
use codex_protocol::ThreadId;
use sqlx::Row;

#[cfg(test)]
#[path = "clear_recoveries_tests.rs"]
mod tests;

/// Recovery describes creation only, never predecessor retirement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClearRecoveryPhase {
    Pending,
    Complete,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClearRecoveryRecord {
    pub successor_thread_id: ThreadId,
    pub predecessor_thread_id: Option<ThreadId>,
    pub phase: ClearRecoveryPhase,
}

impl StateRuntime {
    /// Establish provenance before starting any successor-owned resources.
    pub async fn reserve_clear_recovery(
        &self,
        successor: ThreadId,
        predecessor: Option<ThreadId>,
    ) -> anyhow::Result<()> {
        sqlx::query("INSERT INTO clear_recoveries VALUES (?, ?, 'pending')")
            .bind(successor.to_string())
            .bind(predecessor.map(|id| id.to_string()))
            .execute(self.pool.as_ref())
            .await?;
        Ok(())
    }

    /// Only a pending record may settle. Interrupted writes leave honest pending evidence.
    pub async fn finish_clear_recovery(
        &self,
        successor: ThreadId,
        phase: ClearRecoveryPhase,
    ) -> anyhow::Result<()> {
        let phase = match phase {
            ClearRecoveryPhase::Complete => "complete",
            ClearRecoveryPhase::Failed => "failed",
            ClearRecoveryPhase::Pending => bail!("cannot finish recovery as pending"),
        };
        let updated = sqlx::query(
            "UPDATE clear_recoveries SET phase = ? WHERE successor_thread_id = ? AND phase = 'pending'",
        )
        .bind(phase)
        .bind(successor.to_string())
        .execute(self.pool.as_ref())
        .await?;
        if updated.rows_affected() != 1 {
            bail!("recovery reservation is missing or already settled");
        }
        Ok(())
    }

    pub async fn get_clear_recovery(
        &self,
        successor: ThreadId,
    ) -> anyhow::Result<Option<ClearRecoveryRecord>> {
        let Some(row) = sqlx::query("SELECT * FROM clear_recoveries WHERE successor_thread_id = ?")
            .bind(successor.to_string())
            .fetch_optional(self.pool.as_ref())
            .await?
        else {
            return Ok(None);
        };
        let predecessor: Option<String> = row.try_get("predecessor_thread_id")?;
        let phase: &str = row.try_get("phase")?;
        Ok(Some(ClearRecoveryRecord {
            successor_thread_id: successor,
            predecessor_thread_id: predecessor
                .as_deref()
                .map(ThreadId::from_string)
                .transpose()?,
            phase: match phase {
                "pending" => ClearRecoveryPhase::Pending,
                "complete" => ClearRecoveryPhase::Complete,
                "failed" => ClearRecoveryPhase::Failed,
                _ => bail!("invalid recovery phase"),
            },
        }))
    }
}
