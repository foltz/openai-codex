use super::KCF_STATE_MIGRATOR;
use crate::SqliteConfig;
use crate::StateRuntime;
use crate::migrations::STATE_MIGRATOR;
use crate::migrations::runtime_state_migrator;
use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;
use sqlx::SqlitePool;
use sqlx::migrate::MigrateError;
use sqlx::migrate::Migration;
use sqlx::migrate::MigrationType;
use std::borrow::Cow;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

struct FixtureDir(PathBuf);

impl FixtureDir {
    fn new() -> std::io::Result<Self> {
        let path = crate::runtime::test_support::unique_temp_dir();
        std::fs::create_dir_all(&path)?;
        Ok(Self(path))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for FixtureDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn history(pool: &SqlitePool) -> anyhow::Result<Vec<(i64, Vec<u8>, bool)>> {
    Ok(
        sqlx::query_as("SELECT version, checksum, success FROM _sqlx_migrations ORDER BY version")
            .fetch_all(pool)
            .await?,
    )
}

async fn kcf_history(pool: &SqlitePool) -> anyhow::Result<Vec<(i64, Vec<u8>, bool)>> {
    Ok(sqlx::query_as(
        "SELECT version, checksum, success FROM _kcf_state_migrations ORDER BY version",
    )
    .fetch_all(pool)
    .await?)
}

async fn table_exists(pool: &SqlitePool, name: &str) -> anyhow::Result<bool> {
    Ok(sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
    )
    .bind(name)
    .fetch_one(pool)
    .await?
        == 1)
}

async fn assert_complete(pool: &SqlitePool) -> anyhow::Result<()> {
    let expected_upstream = STATE_MIGRATOR
        .iter()
        .map(|m| (m.version, m.checksum.to_vec(), true))
        .collect::<Vec<_>>();
    let expected_kcf = KCF_STATE_MIGRATOR
        .iter()
        .map(|m| (m.version, m.checksum.to_vec(), true))
        .collect::<Vec<_>>();
    assert_eq!(history(pool).await?, expected_upstream);
    assert_eq!(kcf_history(pool).await?, expected_kcf);
    assert!(table_exists(pool, "clear_transitions").await?);
    let index_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM sqlite_master WHERE type='index' AND tbl_name='clear_transitions' AND name LIKE 'idx_clear_transitions_%'",
    )
    .fetch_one(pool)
    .await?;
    assert_eq!(index_count, 3);
    Ok(())
}

#[tokio::test]
async fn fresh_state_has_independent_histories_and_reopens() -> anyhow::Result<()> {
    let dir = FixtureDir::new()?;
    let sqlite = SqliteConfig::new_for_testing(dir.path().abs());
    let runtime = StateRuntime::init(sqlite.clone(), "test".into()).await?;
    let pool = sqlite.open_read_write_pool(&sqlite.state_db_path()).await?;
    assert_complete(&pool).await?;
    // Both version 1 rows exist, with different checksums and no collision.
    assert_ne!(history(&pool).await?[0].1, kcf_history(&pool).await?[0].1);
    let before = (history(&pool).await?, kcf_history(&pool).await?);
    for path in [
        sqlite.logs_db_path(),
        sqlite.goals_db_path(),
        sqlite.memories_db_path(),
        sqlite.queue_db_path(),
    ] {
        let other = sqlite.open_read_write_pool(&path).await?;
        assert!(!table_exists(&other, "_kcf_state_migrations").await?);
        assert!(!table_exists(&other, "clear_transitions").await?);
        other.close().await;
    }
    drop(runtime);
    let reopened = sqlite
        .open_state_db(&runtime_state_migrator(), /*telemetry_override*/ None)
        .await?;
    assert_eq!(
        (history(&reopened).await?, kcf_history(&reopened).await?),
        before
    );
    reopened.close().await;
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn upstream_adoption_preserves_history_and_upstream_can_reopen() -> anyhow::Result<()> {
    let dir = FixtureDir::new()?;
    let sqlite = SqliteConfig::new_for_testing(dir.path().abs());
    let pool = sqlite.open_read_write_pool(&sqlite.state_db_path()).await?;
    runtime_state_migrator().run(&pool).await?;
    let before = history(&pool).await?;
    assert!(!table_exists(&pool, "_kcf_state_migrations").await?);
    pool.close().await;
    let adopted = sqlite
        .open_state_db(&runtime_state_migrator(), /*telemetry_override*/ None)
        .await?;
    assert_eq!(history(&adopted).await?, before);
    assert_complete(&adopted).await?;
    let kcf_before = kcf_history(&adopted).await?;
    adopted.close().await;
    // This is precisely the unmodified upstream runtime migration path, not
    // the KCF-composed state initializer and not a claim about all stock APIs.
    let upstream = sqlite.open_read_write_pool(&sqlite.state_db_path()).await?;
    runtime_state_migrator().run(&upstream).await?;
    assert_eq!(history(&upstream).await?, before);
    assert_eq!(kcf_history(&upstream).await?, kcf_before);
    upstream.close().await;
    Ok(())
}

#[tokio::test]
async fn tampered_checksum_in_either_ledger_refuses_runtime() -> anyhow::Result<()> {
    for statement in [
        "UPDATE _sqlx_migrations SET checksum = X'00' WHERE version = 1",
        "UPDATE _kcf_state_migrations SET checksum = X'00' WHERE version = 1",
    ] {
        let dir = FixtureDir::new()?;
        let sqlite = SqliteConfig::new_for_testing(dir.path().abs());
        let pool = sqlite
            .open_state_db(&runtime_state_migrator(), /*telemetry_override*/ None)
            .await?;
        sqlx::query(statement).execute(&pool).await?;
        let before = (history(&pool).await?, kcf_history(&pool).await?);
        let err = StateRuntime::init(sqlite, "test".into())
            .await
            .err()
            .expect("refusal");
        assert!(
            matches!(
                err.chain()
                    .find_map(|cause| cause.downcast_ref::<MigrateError>()),
                Some(MigrateError::VersionMismatch(1))
            ),
            "{err:#}"
        );
        assert_eq!((history(&pool).await?, kcf_history(&pool).await?), before);
        pool.close().await;
    }
    Ok(())
}

#[tokio::test]
async fn legacy_kcf_and_unknown_48_refuse_without_adoption() -> anyhow::Result<()> {
    for unknown_checksum in [false, true] {
        let dir = FixtureDir::new()?;
        let sqlite = SqliteConfig::new_for_testing(dir.path().abs());
        let pool = sqlite.open_read_write_pool(&sqlite.state_db_path()).await?;
        let mut legacy = runtime_state_migrator();
        let mut migrations = STATE_MIGRATOR
            .iter()
            .filter(|m| m.version < 48)
            .cloned()
            .collect::<Vec<_>>();
        migrations.push(Migration::new(
            /*version*/ 48,
            Cow::Borrowed("clear transitions"),
            MigrationType::Simple,
            KCF_STATE_MIGRATOR
                .iter()
                .next()
                .expect("KCF migration")
                .sql
                .clone(),
            /*no_tx*/ false,
        ));
        legacy.migrations = Cow::Owned(migrations);
        legacy.run(&pool).await?;
        if unknown_checksum {
            sqlx::query("UPDATE _sqlx_migrations SET checksum = X'00' WHERE version = 48")
                .execute(&pool)
                .await?;
        }
        let before = history(&pool).await?;
        let err = StateRuntime::init(sqlite, "test".into())
            .await
            .err()
            .expect("legacy refusal");
        assert!(
            matches!(
                err.chain()
                    .find_map(|cause| cause.downcast_ref::<MigrateError>()),
                Some(MigrateError::VersionMismatch(48))
            ),
            "{err:#}"
        );
        assert_eq!(history(&pool).await?, before);
        assert!(!table_exists(&pool, "_kcf_state_migrations").await?);
        pool.close().await;
    }
    Ok(())
}

#[tokio::test]
async fn failed_kcf_script_rolls_back_and_retry_uses_committed_upstream() -> anyhow::Result<()> {
    let dir = FixtureDir::new()?;
    let sqlite = SqliteConfig::new_for_testing(dir.path().abs());
    let pool = sqlite.open_read_write_pool(&sqlite.state_db_path()).await?;
    // Collision occurs after CREATE TABLE in the same KCF script, proving
    // rollback of partial DDL rather than merely failure before any statement.
    sqlx::query("CREATE TABLE injected_blocker (id TEXT)")
        .execute(&pool)
        .await?;
    sqlx::query("CREATE INDEX idx_clear_transitions_phase ON injected_blocker(id)")
        .execute(&pool)
        .await?;
    let err = StateRuntime::init(sqlite.clone(), "test".into())
        .await
        .err()
        .expect("KCF failure must refuse initialization");
    assert!(format!("{err:#}").contains("failed to migrate KCF state schema"));
    assert_eq!(history(&pool).await?.len(), STATE_MIGRATOR.iter().count());
    assert!(!table_exists(&pool, "clear_transitions").await?);
    assert!(kcf_history(&pool).await?.is_empty());
    let upstream_before = history(&pool).await?;
    sqlx::query("DROP TABLE injected_blocker")
        .execute(&pool)
        .await?;
    let runtime = StateRuntime::init(sqlite, "test".into()).await?;
    assert_complete(&pool).await?;
    assert_eq!(history(&pool).await?, upstream_before);
    drop(runtime);
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn newer_kcf_history_refuses_without_relaxing_upstream_policy() -> anyhow::Result<()> {
    let dir = FixtureDir::new()?;
    let sqlite = SqliteConfig::new_for_testing(dir.path().abs());
    let pool = sqlite
        .open_state_db(&runtime_state_migrator(), /*telemetry_override*/ None)
        .await?;
    sqlx::query("INSERT INTO _kcf_state_migrations (version, description, success, checksum, execution_time) VALUES (2, 'future', 1, X'01', 0)").execute(&pool).await?;
    runtime_state_migrator().run(&pool).await?;
    let err = StateRuntime::init(sqlite, "test".into())
        .await
        .err()
        .expect("strict KCF history");
    assert!(
        matches!(
            err.chain()
                .find_map(|cause| cause.downcast_ref::<MigrateError>()),
            Some(MigrateError::VersionMissing(2))
        ),
        "{err:#}"
    );
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn concurrent_state_opens_never_publish_partial_schema_and_retry() -> anyhow::Result<()> {
    let dir = FixtureDir::new()?;
    let sqlite = SqliteConfig::new_for_testing(dir.path().abs());
    let first = runtime_state_migrator();
    let second = runtime_state_migrator();
    let (a, b) = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(
            sqlite.open_state_db(&first, /*telemetry_override*/ None),
            sqlite.open_state_db(&second, /*telemetry_override*/ None)
        )
    })
    .await?;
    // SQLx SQLite has no migrator lock. A concurrent opener may fail, but
    // every returned success must have the complete schema and both histories.
    for result in [a, b] {
        match result {
            Ok(pool) => {
                assert_complete(&pool).await?;
                pool.close().await;
            }
            Err(err) => assert!(
                err.downcast_ref::<crate::runtime::RuntimeDbInitError>()
                    .is_some(),
                "{err:#}"
            ),
        }
    }
    let retry = sqlite
        .open_state_db(&runtime_state_migrator(), /*telemetry_override*/ None)
        .await?;
    assert_complete(&retry).await?;
    retry.close().await;
    Ok(())
}
