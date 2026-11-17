use super::*;
use pretty_assertions::assert_eq;
use serde_json::json;
use tempfile::tempdir;

fn storage(home: &Path) -> Arc<dyn AuthStorageBackend> {
    create_auth_storage(
        home.to_path_buf(),
        AuthCredentialsStoreMode::File,
        AuthKeyringBackendKind::Direct,
    )
}

fn auth(label: &str) -> AuthDotJson {
    serde_json::from_value(json!({"OPENAI_API_KEY": label})).unwrap()
}

#[test]
fn command_guard_serializes_the_actual_native_writer_and_compare_commit() -> anyhow::Result<()> {
    let home = tempdir()?;
    let native = storage(home.path());
    let first = auth("synthetic-first");
    let next = auth("synthetic-next");
    native.save(&first)?;
    let guard = acquire_command_auth_source(home.path())?;
    assert_eq!(
        guard.lock_path,
        managed_lock_path(home.path(), AuthCredentialsStoreMode::File)?
    );
    assert!(!home.path().join(".managed-auth-source.lock").exists());
    assert_eq!(
        native.save(&next).unwrap_err().kind(),
        std::io::ErrorKind::TimedOut
    );
    assert_eq!(
        native.compare_and_save(&first, &next).unwrap_err().kind(),
        std::io::ErrorKind::TimedOut
    );
    assert_eq!(native.load()?, Some(first.clone()));
    guard.require_current_domain()?;
    drop(guard);
    native.compare_and_save(&first, &next)?;
    assert_eq!(native.load()?, Some(next));
    Ok(())
}

#[test]
fn command_and_managed_guards_share_immediate_contention_without_reading_auth() -> anyhow::Result<()>
{
    let home = tempdir()?;
    let native = storage(home.path());
    let managed = native.lock_managed_source()?;
    std::fs::write(get_auth_file(home.path()), "invalid-json-sentinel")?;
    assert_eq!(
        acquire_command_auth_source(home.path()).unwrap_err(),
        ManagedAuthStorageError::ReadFailed(ManagedAuthStorageFailure::CoordinationContended)
    );
    drop(managed);
    let command = acquire_command_auth_source(home.path())?;
    assert_eq!(
        native.load_managed().unwrap_err(),
        ManagedAuthStorageError::ReadFailed(ManagedAuthStorageFailure::CoordinationContended)
    );
    assert_eq!(
        acquire_command_auth_source(home.path()).unwrap_err(),
        ManagedAuthStorageError::ReadFailed(ManagedAuthStorageFailure::CoordinationContended)
    );
    command.require_current_domain()?;
    assert_eq!(
        std::fs::read_to_string(get_auth_file(home.path()))?,
        "invalid-json-sentinel"
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn command_guard_refuses_symlink_hardlink_and_nonprivate_lock_files() -> anyhow::Result<()> {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let home = tempdir()?;
    let target = home.path().join("unrelated");
    std::fs::write(&target, "unrelated-sentinel")?;
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o600))?;
    let path = managed_lock_path(home.path(), AuthCredentialsStoreMode::File)?;
    symlink(&target, &path)?;
    assert!(acquire_command_auth_source(home.path()).is_err());
    std::fs::remove_file(&path)?;
    std::fs::hard_link(&target, &path)?;
    assert!(acquire_command_auth_source(home.path()).is_err());
    std::fs::remove_file(&path)?;
    std::fs::write(&path, "lock-sentinel")?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))?;
    assert!(acquire_command_auth_source(home.path()).is_err());
    assert_eq!(std::fs::read_to_string(&target)?, "unrelated-sentinel");
    assert_eq!(std::fs::read_to_string(&path)?, "lock-sentinel");
    Ok(())
}

#[cfg(unix)]
#[test]
fn command_guard_detects_replaced_named_lock() -> anyhow::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let home = tempdir()?;
    let guard = acquire_command_auth_source(home.path())?;
    let path = guard.lock_path.clone();
    std::fs::remove_file(&path)?;
    drop(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)?,
    );
    assert!(guard.require_current_domain().is_err());
    Ok(())
}

#[cfg(unix)]
#[test]
fn command_guard_detects_auth_symlink_domain_change() -> anyhow::Result<()> {
    use std::os::unix::fs::symlink;
    let home = tempdir()?;
    let first = tempdir()?;
    let second = tempdir()?;
    std::fs::write(first.path().join("auth.json"), "first-sentinel")?;
    std::fs::write(second.path().join("auth.json"), "second-sentinel")?;
    let alias = get_auth_file(home.path());
    symlink(first.path().join("auth.json"), &alias)?;
    let guard = acquire_command_auth_source(home.path())?;
    guard.require_current_domain()?;
    std::fs::remove_file(&alias)?;
    symlink(second.path().join("auth.json"), &alias)?;
    assert!(guard.require_current_domain().is_err());
    assert_eq!(
        std::fs::read_to_string(first.path().join("auth.json"))?,
        "first-sentinel"
    );
    assert_eq!(
        std::fs::read_to_string(second.path().join("auth.json"))?,
        "second-sentinel"
    );
    Ok(())
}

#[test]
fn command_guard_does_not_create_a_missing_home() -> anyhow::Result<()> {
    let root = tempdir()?;
    let missing = root.path().join("missing");
    assert!(acquire_command_auth_source(&missing).is_err());
    assert!(!missing.exists());
    Ok(())
}
