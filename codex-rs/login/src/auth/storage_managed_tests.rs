use super::*;
use codex_keyring_store::tests::MockKeyringStore;
use keyring::Error as KeyringError;
use pretty_assertions::assert_eq;
use serde_json::json;
use tempfile::tempdir;

fn sample(value: &str) -> AuthDotJson {
    serde_json::from_value(json!({"OPENAI_API_KEY": value})).unwrap()
}

#[test]
fn raw_file_distinguishes_absence_parse_failure_and_io_failure() -> anyhow::Result<()> {
    let home = tempdir()?;
    let storage = FileAuthStorage::new(home.path().to_path_buf());
    let absent = storage.load_managed()?;
    assert_eq!(absent.source, ManagedAuthStorageSource::File);
    assert_eq!(absent.auth, None);
    std::fs::write(get_auth_file(home.path()), "invalid-json-sentinel")?;
    assert_eq!(
        storage.load_managed().unwrap_err(),
        ManagedAuthStorageError::ReadFailed(ManagedAuthStorageFailure::Parse(
            ManagedAuthStorageSource::File
        ))
    );
    std::fs::remove_file(get_auth_file(home.path()))?;
    std::fs::create_dir(get_auth_file(home.path()))?;
    assert!(matches!(
        storage.load_managed(),
        Err(ManagedAuthStorageError::ReadFailed(
            ManagedAuthStorageFailure::FileIo(_)
        ))
    ));
    Ok(())
}

#[test]
fn raw_preimage_detects_replacement_without_parsing_replacement_auth() -> anyhow::Result<()> {
    let home = tempdir()?;
    let storage = FileAuthStorage::new(home.path().to_path_buf());
    let first = sample("first-synthetic");
    storage.save(&first)?;
    let prepared = storage.load_managed()?;
    assert!(storage.verify_managed_preimage(&prepared.preimage)?);
    std::fs::write(get_auth_file(home.path()), "invalid-json-sentinel")?;
    assert!(!storage.verify_managed_preimage(&prepared.preimage)?);
    assert_eq!(prepared.auth, Some(first));
    Ok(())
}

#[test]
fn raw_file_preimage_binds_canonical_home_and_refuses_unresolved_home() -> anyhow::Result<()> {
    let first = tempdir()?;
    let second = tempdir()?;
    let one = FileAuthStorage::new(first.path().to_path_buf());
    let two = FileAuthStorage::new(second.path().to_path_buf());
    one.save(&sample("same-synthetic"))?;
    two.save(&sample("same-synthetic"))?;
    let prepared = one.load_managed()?;
    assert!(!two.verify_managed_preimage(&prepared.preimage)?);
    let missing = first.path().join("missing");
    assert!(
        FileAuthStorage::new(missing.clone())
            .load_managed()
            .is_err()
    );
    assert!(!missing.exists());
    Ok(())
}

#[test]
fn raw_direct_keyring_preserves_absence_parse_and_backend_error() -> anyhow::Result<()> {
    let home = tempdir()?;
    let keyring = Arc::new(MockKeyringStore::default());
    let storage = DirectKeyringAuthStorage::new(
        home.path().to_path_buf(),
        keyring.clone(),
        AuthCredentialsStoreMode::Keyring,
    );
    assert_eq!(storage.load_managed()?.auth, None);
    keyring.save(
        KEYRING_SERVICE,
        &compute_store_key(home.path())?,
        "invalid-json-sentinel",
    )?;
    assert_eq!(
        storage.load_managed().unwrap_err(),
        ManagedAuthStorageError::ReadFailed(ManagedAuthStorageFailure::Parse(
            ManagedAuthStorageSource::Keyring
        ))
    );
    keyring.set_error(
        &compute_store_key(home.path())?,
        KeyringError::Invalid("private-error-sentinel".into(), "load".into()),
    );
    let error = storage.load_managed().unwrap_err();
    assert_eq!(
        error,
        ManagedAuthStorageError::ReadFailed(ManagedAuthStorageFailure::Keyring)
    );
    assert!(!format!("{error:?} {error}").contains("private-error-sentinel"));
    Ok(())
}

#[test]
fn raw_auto_uses_fallback_only_after_verified_primary_absence() -> anyhow::Result<()> {
    let home = tempdir()?;
    let keyring = Arc::new(MockKeyringStore::default());
    let storage = AutoAuthStorage::new(
        home.path().to_path_buf(),
        keyring.clone(),
        AuthKeyringBackendKind::Direct,
    );
    storage.file_storage.save(&sample("fallback-synthetic"))?;
    let fallback = storage.load_managed()?;
    assert_eq!(
        fallback.source,
        ManagedAuthStorageSource::FileAfterKeyringAbsence
    );
    assert_eq!(fallback.auth, Some(sample("fallback-synthetic")));
    keyring.save(
        KEYRING_SERVICE,
        &compute_store_key(home.path())?,
        "invalid-json-sentinel",
    )?;
    assert_eq!(
        storage.load_managed().unwrap_err(),
        ManagedAuthStorageError::AutoFallbackRequired(ManagedAuthStorageFailure::Parse(
            ManagedAuthStorageSource::Keyring
        ))
    );
    keyring.set_error(
        &compute_store_key(home.path())?,
        KeyringError::Invalid("private-error-sentinel".into(), "load".into()),
    );
    assert_eq!(
        storage.load_managed().unwrap_err(),
        ManagedAuthStorageError::AutoFallbackRequired(ManagedAuthStorageFailure::Keyring)
    );
    assert_eq!(
        storage.file_storage.load()?,
        Some(sample("fallback-synthetic"))
    );
    Ok(())
}

#[test]
fn raw_auto_preimage_binds_both_primary_and_fallback_generation() -> anyhow::Result<()> {
    let home = tempdir()?;
    let keyring = Arc::new(MockKeyringStore::default());
    let storage = AutoAuthStorage::new(
        home.path().to_path_buf(),
        keyring.clone(),
        AuthKeyringBackendKind::Direct,
    );
    storage.file_storage.save(&sample("fallback-synthetic"))?;
    let fallback = storage.load_managed()?;
    storage.file_storage.save(&sample("changed-fallback"))?;
    assert!(!storage.verify_managed_preimage(&fallback.preimage)?);
    storage.file_storage.save(&sample("fallback-synthetic"))?;
    assert!(storage.verify_managed_preimage(&fallback.preimage)?);
    keyring.save(
        KEYRING_SERVICE,
        &compute_store_key(home.path())?,
        &serde_json::to_string(&sample("fallback-synthetic"))?,
    )?;
    assert_eq!(
        storage.load_managed()?.source,
        ManagedAuthStorageSource::Keyring
    );
    assert!(!storage.verify_managed_preimage(&fallback.preimage)?);
    Ok(())
}

#[test]
fn raw_secrets_preimage_binds_ciphertext_generation() -> anyhow::Result<()> {
    let home = tempdir()?;
    let keyring = Arc::new(MockKeyringStore::default());
    let storage = SecretsKeyringAuthStorage::new(
        home.path().to_path_buf(),
        keyring,
        AuthCredentialsStoreMode::Keyring,
    );
    let original = sample("encrypted-synthetic");
    storage.save(&original)?;
    let prepared = storage.load_managed()?;
    assert_eq!(prepared.source, ManagedAuthStorageSource::Secrets);
    assert!(storage.verify_managed_preimage(&prepared.preimage)?);
    storage.save(&original)?;
    assert_eq!(storage.load_managed()?.auth, Some(original));
    assert!(!storage.verify_managed_preimage(&prepared.preimage)?);
    Ok(())
}

#[test]
fn raw_auto_secrets_failure_is_not_fallback_or_logout() -> anyhow::Result<()> {
    let home = tempdir()?;
    let keyring = Arc::new(MockKeyringStore::default());
    let storage = AutoAuthStorage::new(
        home.path().to_path_buf(),
        keyring.clone(),
        AuthKeyringBackendKind::Secrets,
    );
    storage.file_storage.save(&sample("fallback-synthetic"))?;
    assert_eq!(
        storage.load_managed()?.source,
        ManagedAuthStorageSource::FileAfterKeyringAbsence
    );
    storage
        .keyring_storage
        .save(&sample("encrypted-synthetic"))?;
    storage.file_storage.save(&sample("fallback-synthetic"))?;
    keyring.set_error(
        &codex_secrets::compute_keyring_account(home.path(), LocalSecretsNamespace::CodexAuth),
        KeyringError::Invalid("private-error-sentinel".into(), "load".into()),
    );
    assert_eq!(
        storage.load_managed().unwrap_err(),
        ManagedAuthStorageError::AutoFallbackRequired(ManagedAuthStorageFailure::Secrets)
    );
    assert_eq!(
        storage.file_storage.load()?,
        Some(sample("fallback-synthetic"))
    );
    Ok(())
}

#[test]
fn raw_ephemeral_cannot_claim_durable_managed_authority() -> anyhow::Result<()> {
    let home = tempdir()?;
    let storage = EphemeralAuthStorage::new(home.path().to_path_buf());
    storage.save(&sample("ephemeral-synthetic"))?;
    assert_eq!(
        storage.load_managed().unwrap_err(),
        ManagedAuthStorageError::ReadFailed(ManagedAuthStorageFailure::UnsupportedStorage)
    );
    assert_eq!(storage.load()?, Some(sample("ephemeral-synthetic")));
    Ok(())
}

#[test]
fn raw_debug_hides_auth_and_internal_comparison_digest() -> anyhow::Result<()> {
    let home = tempdir()?;
    let storage = FileAuthStorage::new(home.path().to_path_buf());
    storage.save(&sample("private-value-sentinel"))?;
    let read = storage.load_managed()?;
    let debug = format!("{read:?} {:?}", read.preimage);
    assert!(!debug.contains("private-value-sentinel"));
    assert!(debug.contains("[redacted]"));
    assert!(!debug.contains(&format!("{:02x?}", read.preimage.0)));
    Ok(())
}
