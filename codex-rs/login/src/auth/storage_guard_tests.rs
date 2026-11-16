use super::*;
use codex_keyring_store::tests::MockKeyringStore;
use pretty_assertions::assert_eq;
use serde_json::json;
use tempfile::tempdir;

fn sample(label: &str) -> AuthDotJson {
    serde_json::from_value(json!({"OPENAI_API_KEY":label})).unwrap()
}

fn durable_case(
    mode: AuthCredentialsStoreMode,
    kind: AuthKeyringBackendKind,
) -> anyhow::Result<()> {
    let home = tempdir()?;
    let keyring = Arc::new(MockKeyringStore::default());
    let storage =
        create_auth_storage_with_store(home.path().to_path_buf(), mode, keyring.clone(), kind);
    assert_eq!(storage.load_managed()?.auth, None);
    let first = sample("first-synthetic");
    let next = sample("next-synthetic");
    storage.save(&first)?;
    let prepared = storage.load_managed()?;
    let guard = storage.lock_managed_source()?;
    assert!(guard.verify_managed_preimage(&prepared.preimage)?);
    let peer = create_auth_storage_with_store(home.path().to_path_buf(), mode, keyring, kind);
    // Exercise the actual ordinary writer's unchanged five-second refusal.
    assert_eq!(
        peer.save(&next).unwrap_err().kind(),
        std::io::ErrorKind::TimedOut
    );
    assert_eq!(
        peer.compare_and_save(&first, &next).unwrap_err().kind(),
        std::io::ErrorKind::TimedOut
    );
    assert_eq!(
        peer.delete().unwrap_err().kind(),
        std::io::ErrorKind::TimedOut
    );
    assert!(matches!(
        peer.load_managed(),
        Err(ManagedAuthStorageError::ReadFailed(
            ManagedAuthStorageFailure::CoordinationContended
        ))
    ));
    assert!(guard.verify_managed_preimage(&prepared.preimage)?);
    drop(guard);
    peer.compare_and_save(&first, &next)?;
    assert!(!storage.verify_managed_preimage(&prepared.preimage)?);
    assert_eq!(storage.load_managed()?.auth, Some(next));
    assert!(peer.delete()?);
    assert_eq!(storage.load_managed()?.auth, None);
    Ok(())
}

macro_rules! durable_test {
    ($name:ident, $mode:ident, $kind:ident) => {
        #[test]
        fn $name() -> anyhow::Result<()> {
            durable_case(
                AuthCredentialsStoreMode::$mode,
                AuthKeyringBackendKind::$kind,
            )
        }
    };
}
durable_test!(guarded_file_direct, File, Direct);
durable_test!(guarded_file_secrets, File, Secrets);
durable_test!(guarded_keyring_direct, Keyring, Direct);
durable_test!(guarded_keyring_secrets, Keyring, Secrets);
durable_test!(guarded_auto_direct, Auto, Direct);
durable_test!(guarded_auto_secrets, Auto, Secrets);

#[derive(Debug)]
struct PausedRead {
    file: FileAuthStorage,
    entered: Arc<std::sync::Barrier>,
    resume: Arc<std::sync::Barrier>,
}
impl AuthStorageBackend for PausedRead {
    fn load(&self) -> std::io::Result<Option<AuthDotJson>> {
        self.file.load()
    }
    fn read_managed_bytes(&self) -> Result<ManagedAuthStorageBytes, ManagedAuthStorageError> {
        self.entered.wait();
        self.resume.wait();
        self.file.read_managed_bytes()
    }
    fn save(&self, auth: &AuthDotJson) -> std::io::Result<()> {
        self.file.save(auth)
    }
    fn delete(&self) -> std::io::Result<bool> {
        self.file.delete()
    }
}

#[test]
fn guarded_prepare_holds_writer_lock_through_read_and_parse() -> anyhow::Result<()> {
    let home = tempdir()?;
    let writer = create_auth_storage(
        home.path().to_path_buf(),
        AuthCredentialsStoreMode::File,
        AuthKeyringBackendKind::Direct,
    );
    let first = sample("first-synthetic");
    writer.save(&first)?;
    let entered = Arc::new(std::sync::Barrier::new(2));
    let resume = Arc::new(std::sync::Barrier::new(2));
    let reader = CommittedStorage {
        inner: Arc::new(PausedRead {
            file: FileAuthStorage::new(home.path().to_path_buf()),
            entered: entered.clone(),
            resume: resume.clone(),
        }),
        home: home.path().to_path_buf(),
        mode: AuthCredentialsStoreMode::File,
    };
    std::thread::scope(|scope| {
        let read = scope.spawn(|| reader.load_managed());
        entered.wait();
        let result = writer.save(&sample("other-synthetic"));
        resume.wait();
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::TimedOut);
        assert_eq!(read.join().unwrap().unwrap().auth, Some(first));
    });
    writer.save(&sample("other-synthetic"))?;
    Ok(())
}

#[test]
fn guard_contention_refuses_without_reading_auth() -> anyhow::Result<()> {
    let home = tempdir()?;
    let storage = create_auth_storage(
        home.path().to_path_buf(),
        AuthCredentialsStoreMode::File,
        AuthKeyringBackendKind::Direct,
    );
    let guard = storage.lock_managed_source()?;
    // If the contender read before locking, this invalid payload would parse-fail.
    std::fs::write(get_auth_file(home.path()), "invalid-json-sentinel")?;
    assert!(matches!(
        storage.load_managed(),
        Err(ManagedAuthStorageError::ReadFailed(
            ManagedAuthStorageFailure::CoordinationContended
        ))
    ));
    drop(guard);
    assert_eq!(
        storage.load_managed().unwrap_err(),
        ManagedAuthStorageError::ReadFailed(ManagedAuthStorageFailure::Parse(
            ManagedAuthStorageSource::File
        ))
    );
    Ok(())
}

#[test]
fn guard_observes_lock_file_replacement_without_claiming_validity() -> anyhow::Result<()> {
    let home = tempdir()?;
    let storage = create_auth_storage(
        home.path().to_path_buf(),
        AuthCredentialsStoreMode::File,
        AuthKeyringBackendKind::Direct,
    );
    storage.save(&sample("first-synthetic"))?;
    let prepared = storage.load_managed()?;
    let guard = storage.lock_managed_source()?;
    let path = managed_lock_path(home.path(), AuthCredentialsStoreMode::File)?;
    // Deliberately uncooperative fixture mutation; not excluded by advisory locks.
    std::fs::remove_file(&path)?;
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    drop(options.open(&path)?);
    assert!(guard.verify_managed_preimage(&prepared.preimage).is_err());
    assert_eq!(storage.load()?, Some(sample("first-synthetic")));
    Ok(())
}

#[cfg(unix)]
#[test]
fn guard_refuses_symlink_hardlink_and_nonprivate_lock_files() -> anyhow::Result<()> {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let home = tempdir()?;
    let target = home.path().join("unrelated");
    std::fs::write(&target, "unrelated-sentinel")?;
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o600))?;
    let path = managed_lock_path(home.path(), AuthCredentialsStoreMode::File)?;
    let storage = create_auth_storage(
        home.path().to_path_buf(),
        AuthCredentialsStoreMode::File,
        AuthKeyringBackendKind::Direct,
    );
    symlink(&target, &path)?;
    assert!(storage.lock_managed_source().is_err());
    std::fs::remove_file(&path)?;
    std::fs::hard_link(&target, &path)?;
    assert!(storage.lock_managed_source().is_err());
    std::fs::remove_file(&path)?;
    std::fs::write(&path, "lock-sentinel")?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))?;
    assert!(storage.lock_managed_source().is_err());
    assert_eq!(std::fs::read_to_string(&target)?, "unrelated-sentinel");
    assert_eq!(std::fs::read_to_string(&path)?, "lock-sentinel");
    Ok(())
}

#[test]
fn guard_does_not_create_an_absent_home() -> anyhow::Result<()> {
    let root = tempdir()?;
    let home = root.path().join("absent");
    let storage = create_auth_storage(
        home.clone(),
        AuthCredentialsStoreMode::File,
        AuthKeyringBackendKind::Direct,
    );
    assert!(storage.load_managed().is_err());
    assert!(!home.exists());
    storage.save(&sample("first-synthetic"))?;
    assert_eq!(
        storage.load_managed()?.auth,
        Some(sample("first-synthetic"))
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn guard_does_not_create_an_unresolved_target_parent() -> anyhow::Result<()> {
    let home = tempdir()?;
    let target = home.path().join("absent/auth.json");
    std::os::unix::fs::symlink(&target, get_auth_file(home.path()))?;
    let storage = create_auth_storage(
        home.path().to_path_buf(),
        AuthCredentialsStoreMode::File,
        AuthKeyringBackendKind::Direct,
    );
    assert!(storage.load_managed().is_err());
    assert!(!target.parent().unwrap().exists());
    storage.save(&sample("first-synthetic"))?;
    assert_eq!(
        storage.load_managed()?.auth,
        Some(sample("first-synthetic"))
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn guard_and_writer_share_lock_through_alias_profiles() -> anyhow::Result<()> {
    let root = tempdir()?;
    let one = root.path().join("one");
    let two = root.path().join("two");
    std::fs::create_dir(&one)?;
    std::fs::create_dir(&two)?;
    let target = root.path().join("shared.json");
    std::os::unix::fs::symlink(&target, get_auth_file(&one))?;
    std::os::unix::fs::symlink(&target, get_auth_file(&two))?;
    let storage = create_auth_storage(
        one.clone(),
        AuthCredentialsStoreMode::File,
        AuthKeyringBackendKind::Direct,
    );
    let peer = create_auth_storage(
        two.clone(),
        AuthCredentialsStoreMode::File,
        AuthKeyringBackendKind::Direct,
    );
    storage.save(&sample("first-synthetic"))?;
    let prepared = storage.load_managed()?;
    let guard = storage.lock_managed_source()?;
    assert_eq!(
        managed_lock_path(&one, AuthCredentialsStoreMode::File)?,
        managed_lock_path(&two, AuthCredentialsStoreMode::File)?
    );
    assert_eq!(
        peer.save(&sample("other-synthetic")).unwrap_err().kind(),
        std::io::ErrorKind::TimedOut
    );
    assert!(guard.verify_managed_preimage(&prepared.preimage)?);
    drop(guard);
    peer.save(&sample("other-synthetic"))?;
    assert!(!storage.verify_managed_preimage(&prepared.preimage)?);
    Ok(())
}

#[cfg(unix)]
#[test]
fn guard_detects_source_alias_retarget_even_with_equal_auth_bytes() -> anyhow::Result<()> {
    let home = tempdir()?;
    let one = home.path().join("one.json");
    let two = home.path().join("two.json");
    let bytes = serde_json::to_vec(&sample("same-synthetic"))?;
    std::fs::write(&one, &bytes)?;
    std::fs::write(&two, &bytes)?;
    std::os::unix::fs::symlink(&one, get_auth_file(home.path()))?;
    let storage = create_auth_storage(
        home.path().to_path_buf(),
        AuthCredentialsStoreMode::File,
        AuthKeyringBackendKind::Direct,
    );
    let prepared = storage.load_managed()?;
    let guard = storage.lock_managed_source()?;
    std::fs::remove_file(get_auth_file(home.path()))?;
    std::os::unix::fs::symlink(&two, get_auth_file(home.path()))?;
    assert!(
        FileAuthStorage::new(home.path().to_path_buf())
            .verify_managed_preimage(&prepared.preimage)?
    );
    assert!(!guard.verify_managed_preimage(&prepared.preimage)?);
    Ok(())
}

#[test]
fn guard_verifies_changed_invalid_auth_without_parsing_replacement() -> anyhow::Result<()> {
    let home = tempdir()?;
    let storage = create_auth_storage(
        home.path().to_path_buf(),
        AuthCredentialsStoreMode::File,
        AuthKeyringBackendKind::Direct,
    );
    storage.save(&sample("first-synthetic"))?;
    let prepared = storage.load_managed()?;
    std::fs::write(get_auth_file(home.path()), "invalid-json-sentinel")?;
    let guard = storage.lock_managed_source()?;
    assert!(!guard.verify_managed_preimage(&prepared.preimage)?);
    assert_eq!(prepared.auth, Some(sample("first-synthetic")));
    assert_eq!(format!("{guard:?}"), "ManagedAuthSourceGuard([locked])");
    Ok(())
}

#[cfg(unix)]
#[test]
fn keyring_guard_uses_writer_fallback_domain_for_unusable_file_links() -> anyhow::Result<()> {
    for kind in [
        AuthKeyringBackendKind::Direct,
        AuthKeyringBackendKind::Secrets,
    ] {
        let home = tempdir()?;
        let keyring = Arc::new(MockKeyringStore::default());
        let storage = create_auth_storage_with_store(
            home.path().to_path_buf(),
            AuthCredentialsStoreMode::Keyring,
            keyring,
            kind,
        );
        storage.save(&sample("first-synthetic"))?;
        std::os::unix::fs::symlink("auth.json", get_auth_file(home.path()))?;
        let prepared = storage.load_managed()?;
        let guard = storage.lock_managed_source()?;
        assert!(matches!(
            storage.load_managed(),
            Err(ManagedAuthStorageError::ReadFailed(
                ManagedAuthStorageFailure::CoordinationContended
            ))
        ));
        assert!(guard.verify_managed_preimage(&prepared.preimage)?);
        drop(guard);
        storage.save(&sample("other-synthetic"))?;
        assert_eq!(
            std::fs::symlink_metadata(get_auth_file(home.path()))
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::NotFound
        );
        assert_eq!(
            storage.load_managed()?.auth,
            Some(sample("other-synthetic"))
        );
    }
    Ok(())
}
