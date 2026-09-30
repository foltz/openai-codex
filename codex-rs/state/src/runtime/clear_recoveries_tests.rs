use super::*;
use crate::SqliteConfig;
use crate::runtime::test_support::unique_temp_dir;
use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn recovery_survives_restart_without_claiming_lineage_or_reusing_a_reservation()
-> anyhow::Result<()> {
    let home = unique_temp_dir();
    let config = SqliteConfig::new_for_testing(home.as_path().abs());
    let state = StateRuntime::init(config.clone(), "test".into()).await?;
    let a = ThreadId::new();
    let b = ThreadId::new();
    let c = ThreadId::new();
    assert_eq!(state.get_clear_recovery(b).await?, None);
    state.reserve_clear_recovery(b, Some(a)).await?;
    state.reserve_clear_recovery(c, Some(a)).await?;
    assert!(state.reserve_clear_recovery(b, None).await.is_err());
    let pending = ClearRecoveryRecord {
        successor_thread_id: b,
        predecessor_thread_id: Some(a),
        phase: ClearRecoveryPhase::Pending,
    };
    assert_eq!(state.get_clear_recovery(b).await?, Some(pending.clone()));
    assert_eq!(state.get_clear_transition_by_successor(b).await?, None);
    drop(state);
    let state = StateRuntime::init(config, "test".into()).await?;
    assert_eq!(state.get_clear_recovery(b).await?, Some(pending.clone()));
    state
        .finish_clear_recovery(b, ClearRecoveryPhase::Complete)
        .await?;
    state
        .finish_clear_recovery(c, ClearRecoveryPhase::Failed)
        .await?;
    assert_eq!(
        state.get_clear_recovery(b).await?,
        Some(ClearRecoveryRecord {
            phase: ClearRecoveryPhase::Complete,
            ..pending
        })
    );
    assert_eq!(
        state.get_clear_recovery(c).await?,
        Some(ClearRecoveryRecord {
            successor_thread_id: c,
            predecessor_thread_id: Some(a),
            phase: ClearRecoveryPhase::Failed,
        })
    );
    assert!(
        state
            .finish_clear_recovery(b, ClearRecoveryPhase::Failed)
            .await
            .is_err()
    );
    let d = ThreadId::new();
    state.reserve_clear_recovery(d, None).await?;
    assert_eq!(
        state.get_clear_recovery(d).await?,
        Some(ClearRecoveryRecord {
            successor_thread_id: d,
            predecessor_thread_id: None,
            phase: ClearRecoveryPhase::Pending,
        })
    );
    Ok(())
}
