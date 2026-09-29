//! KCF state schema has a separate, strict version history from upstream.

use sqlx::migrate::Migrator;
use std::sync::LazyLock;

pub(crate) static KCF_STATE_MIGRATOR: LazyLock<Migrator> = LazyLock::new(|| {
    let mut migrator = sqlx::migrate!("./kcf_migrations");
    // Only the new KCF migrator uses this fixed identifier. Never repoint the
    // upstream migrator: that would replay its SQL against an empty ledger.
    // Keep default strict history validation; KCF downgrade is not supported.
    migrator.dangerous_set_table_name("_kcf_state_migrations");
    migrator
});

#[cfg(test)]
#[path = "kcf_migrations_tests.rs"]
mod tests;
