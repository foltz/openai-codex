use super::*;
use crate::token_data::IdTokenInfo;
use anyhow::Context;
use base64::Engine;
use codex_secrets::LocalSecretsNamespace;
use codex_secrets::SecretScope;
use codex_secrets::SecretsBackendKind;
use codex_secrets::SecretsManager;
use codex_secrets::compute_keyring_account;
use pretty_assertions::assert_eq;
use serde_json::json;
use tempfile::tempdir;

use codex_keyring_store::tests::MockKeyringStore;
use keyring::Error as KeyringError;

#[test]
fn managed_source_guard_excludes_cooperating_writers_until_cache_commit() -> anyhow::Result<()> {
    for mode in [
        AuthCredentialsStoreMode::File,
        AuthCredentialsStoreMode::Keyring,
        AuthCredentialsStoreMode::Auto,
    ] {
        for backend in [
            AuthKeyringBackendKind::Direct,
            AuthKeyringBackendKind::Secrets,
        ] {
            let home = tempdir()?;
            let keyring = Arc::new(MockKeyringStore::default());
            let storage = create_auth_storage_with_store(
                home.path().to_path_buf(),
                mode,
                keyring.clone(),
                backend,
            );
            storage.save(&auth_with_prefix("prepared-B"))?;
            let prepared = storage.load_managed()?;
            let guard = storage.lock_managed_source()?;
            assert!(guard.verify_managed_preimage(&prepared.preimage)?);
            let writer =
                create_auth_storage_with_store(home.path().to_path_buf(), mode, keyring, backend);
            // This is the manager's verify -> cache-install interval. A separate
            // descriptor and worker must not mutate the source during it.
            std::thread::scope(|scope| {
                scope
                    .spawn(|| {
                        assert!(writer.save(&auth_with_prefix("replacement-C")).is_err());
                        assert!(writer.delete().is_err());
                        assert!(matches!(
                            writer.load_managed(),
                            Err(ManagedAuthStorageError::ReadFailed(
                                ManagedAuthStorageFailure::CoordinationContended
                            ))
                        ));
                    })
                    .join()
                    .unwrap();
            });
            assert!(guard.verify_managed_preimage(&prepared.preimage)?);
            drop(guard);
            writer.save(&auth_with_prefix("replacement-C"))?;
            assert!(!storage.verify_managed_preimage(&prepared.preimage)?);
        }
    }
    Ok(())
}

#[derive(Debug)]
struct PausedManagedRead {
    file: FileAuthStorage,
    entered: Arc<std::sync::Barrier>,
    resume: Arc<std::sync::Barrier>,
}

impl AuthStorageBackend for PausedManagedRead {
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
fn managed_prepare_holds_source_lock_across_read_and_parse() -> anyhow::Result<()> {
    let home = tempdir()?;
    let writer = create_auth_storage(
        home.path().to_path_buf(),
        AuthCredentialsStoreMode::File,
        AuthKeyringBackendKind::Direct,
    );
    let original = auth_with_prefix("prepared-B");
    writer.save(&original)?;
    let entered = Arc::new(std::sync::Barrier::new(2));
    let resume = Arc::new(std::sync::Barrier::new(2));
    let reader = LockedAuthStorage {
        codex_home: home.path().to_path_buf(),
        backend: Arc::new(PausedManagedRead {
            file: FileAuthStorage::new(home.path().to_path_buf()),
            entered: entered.clone(),
            resume: resume.clone(),
        }),
    };
    std::thread::scope(|scope| {
        let task = scope.spawn(|| reader.load_managed());
        entered.wait();
        let write_result = writer.save(&auth_with_prefix("replacement-C"));
        resume.wait();
        assert!(write_result.is_err());
        assert_eq!(task.join().unwrap().unwrap().auth, Some(original));
    });
    Ok(())
}

#[test]
fn managed_coordination_preserves_first_write_and_detects_noncooperating_precheck_change()
-> anyhow::Result<()> {
    let home = tempdir()?;
    let new_home = home.path().join("not-yet-created");
    let storage = create_auth_storage(
        new_home.clone(),
        AuthCredentialsStoreMode::File,
        AuthKeyringBackendKind::Direct,
    );
    assert!(storage.load_managed().is_err());
    assert!(
        !new_home.exists(),
        "managed prepare must not create an absent home"
    );
    storage.save(&auth_with_prefix("B"))?;
    let prepared = storage.load_managed()?;
    std::fs::write(
        get_auth_file(&new_home),
        serde_json::to_vec(&auth_with_prefix("C"))?,
    )?;
    let guard = storage.lock_managed_source()?;
    assert!(!guard.verify_managed_preimage(&prepared.preimage)?);
    Ok(())
}

#[cfg(unix)]
#[test]
fn managed_coordination_rejects_symlink_and_hardlink_lock_files() -> anyhow::Result<()> {
    let home = tempdir()?;
    let target = home.path().join("unrelated");
    std::fs::write(&target, "sentinel")?;
    let lock = home.path().join(MANAGED_AUTH_COORDINATION_FILE);
    std::os::unix::fs::symlink(&target, &lock)?;
    assert!(acquire_auth_source_lock(home.path()).is_err());
    std::fs::remove_file(&lock)?;
    std::fs::hard_link(&target, &lock)?;
    assert!(acquire_auth_source_lock(home.path()).is_err());
    assert_eq!(std::fs::read_to_string(&target)?, "sentinel");
    Ok(())
}

#[test]
fn managed_preimage_detects_replacement_without_parsing_replacement_auth() -> anyhow::Result<()> {
    for mode in [
        AuthCredentialsStoreMode::File,
        AuthCredentialsStoreMode::Keyring,
        AuthCredentialsStoreMode::Auto,
    ] {
        for backend in [
            AuthKeyringBackendKind::Direct,
            AuthKeyringBackendKind::Secrets,
        ] {
            let home = tempdir()?;
            let keyring = MockKeyringStore::default();
            let storage = create_auth_storage_with_store(
                home.path().to_path_buf(),
                mode,
                Arc::new(keyring.clone()),
                backend,
            );
            let absent = storage.load_managed()?;
            assert!(storage.verify_managed_preimage(&absent.preimage)?);
            let original = auth_with_prefix("prepared-B");
            storage.save(&original)?;
            assert!(!storage.verify_managed_preimage(&absent.preimage)?);
            let prepared = storage.load_managed()?;
            assert!(storage.verify_managed_preimage(&prepared.preimage)?);
            storage.save(&auth_with_prefix("replacement-C"))?;
            assert!(!storage.verify_managed_preimage(&prepared.preimage)?);
            assert_eq!(prepared.auth, Some(original));
            // Revalidation must report a changed preimage even if the new
            // candidate is not JSON; only prepare is allowed to parse auth.
            match (mode, backend) {
                (AuthCredentialsStoreMode::File, _) => {
                    std::fs::write(get_auth_file(home.path()), "{")?
                }
                (_, AuthKeyringBackendKind::Direct) => {
                    keyring.save(KEYRING_SERVICE, &compute_store_key(home.path())?, "{")?
                }
                (_, AuthKeyringBackendKind::Secrets) => {
                    SecretsManager::new_with_keyring_store_and_namespace(
                        home.path().to_path_buf(),
                        SecretsBackendKind::Local,
                        Arc::new(keyring.clone()),
                        LocalSecretsNamespace::CodexAuth,
                    )
                    .set(&SecretScope::Global, &CODEX_AUTH_SECRET_NAME, "{")?
                }
            }
            assert!(!storage.verify_managed_preimage(&prepared.preimage)?);
            assert!(storage.load_managed().is_err());
            assert_eq!(
                format!("{:?}", prepared.preimage),
                "ManagedAuthStoragePreimage([redacted])"
            );
        }
    }
    Ok(())
}

#[test]
fn managed_auto_preimage_binds_primary_absence_and_fallback_source_identity() -> anyhow::Result<()>
{
    let home = tempdir()?;
    let keyring = MockKeyringStore::default();
    let storage = AutoAuthStorage::new(
        home.path().to_path_buf(),
        Arc::new(keyring.clone()),
        AuthKeyringBackendKind::Direct,
    );
    let original = auth_with_prefix("B");
    storage.file_storage.save(&original)?;
    let prepared = storage.load_managed()?;
    assert!(storage.verify_managed_preimage(&prepared.preimage)?);
    // Even identical payload in a newly present primary is a source change.
    storage.keyring_storage.save(&original)?;
    assert!(!storage.verify_managed_preimage(&prepared.preimage)?);
    keyring.set_error(
        &compute_store_key(home.path())?,
        KeyringError::Invalid("error".into(), "read".into()),
    );
    assert!(matches!(
        storage.verify_managed_preimage(&prepared.preimage),
        Err(ManagedAuthStorageError::AutoFallbackRequired(
            ManagedAuthStorageFailure::Keyring
        ))
    ));

    let other_home = tempdir()?;
    let other = FileAuthStorage::new(other_home.path().to_path_buf());
    other.save(&original)?;
    let first = FileAuthStorage::new(home.path().to_path_buf());
    first.save(&original)?;
    assert!(!other.verify_managed_preimage(&first.load_managed()?.preimage)?);
    Ok(())
}

#[test]
fn managed_secrets_preimage_detects_reencrypted_identical_auth() -> anyhow::Result<()> {
    let home = tempdir()?;
    let storage = SecretsKeyringAuthStorage::new(
        home.path().to_path_buf(),
        Arc::new(MockKeyringStore::default()),
    );
    let auth = auth_with_prefix("B");
    storage.save(&auth)?;
    let prepared = storage.load_managed()?;
    storage.save(&auth)?;
    assert!(!storage.verify_managed_preimage(&prepared.preimage)?);
    Ok(())
}

#[test]
fn managed_file_load_distinguishes_absence_io_and_parse_failures() -> anyhow::Result<()> {
    let home = tempdir()?;
    let storage = FileAuthStorage::new(home.path().to_path_buf());
    let absent = storage.load_managed()?;
    assert!(absent.auth.is_none());
    assert_eq!(absent.source, ManagedAuthStorageSource::File);

    let auth_file = get_auth_file(home.path());
    std::fs::create_dir(&auth_file)?;
    assert!(matches!(
        storage.load_managed(),
        Err(ManagedAuthStorageError::ReadFailed(
            ManagedAuthStorageFailure::FileIo(_)
        ))
    ));
    std::fs::remove_dir(&auth_file)?;

    for invalid in [b"{".as_slice(), b"not-json", &[0xff]] {
        std::fs::write(&auth_file, invalid)?;
        assert_eq!(
            storage.load_managed().unwrap_err(),
            ManagedAuthStorageError::ReadFailed(ManagedAuthStorageFailure::Parse(
                ManagedAuthStorageSource::File
            ))
        );
    }
    Ok(())
}

#[test]
fn managed_load_preserves_exact_snapshot_and_redacts_debug() -> anyhow::Result<()> {
    let home = tempdir()?;
    let storage = FileAuthStorage::new(home.path().to_path_buf());
    let original = auth_with_prefix("managed-secret-sentinel");
    storage.save(&original)?;
    let read = storage.load_managed()?;
    storage.save(&auth_with_prefix("later-source"))?;
    assert_eq!(read.auth, Some(original));
    // Unsupported modes must remain present for the manager's explicit mode
    // refusal; storage must not turn them into authoritative logout absence.
    assert_eq!(
        read.auth.as_ref().and_then(|auth| auth.auth_mode),
        Some(AuthMode::ApiKey)
    );
    let debug = format!("{read:?}");
    assert!(debug.contains("auth_present: true"));
    assert!(!debug.contains("managed-secret-sentinel"));
    assert!(!debug.contains("api-key"));
    Ok(())
}

#[test]
fn managed_direct_keyring_distinguishes_absence_failure_and_parse() -> anyhow::Result<()> {
    let home = tempdir()?;
    let keyring = MockKeyringStore::default();
    let storage =
        DirectKeyringAuthStorage::new(home.path().to_path_buf(), Arc::new(keyring.clone()));
    assert!(storage.load_managed()?.auth.is_none());
    let key = compute_store_key(home.path())?;
    keyring.save(KEYRING_SERVICE, &key, "malformed-secret-sentinel")?;
    assert_eq!(
        storage.load_managed().unwrap_err(),
        ManagedAuthStorageError::ReadFailed(ManagedAuthStorageFailure::Parse(
            ManagedAuthStorageSource::Keyring
        ))
    );
    keyring.set_error(
        &key,
        KeyringError::Invalid("secret-error-sentinel".into(), "load".into()),
    );
    let error = storage.load_managed().unwrap_err();
    assert_eq!(
        error,
        ManagedAuthStorageError::ReadFailed(ManagedAuthStorageFailure::Keyring)
    );
    assert!(!format!("{error:?}: {error}").contains("secret-error-sentinel"));
    Ok(())
}

#[test]
fn managed_secrets_distinguishes_absence_backend_failure_and_auth_parse() -> anyhow::Result<()> {
    let home = tempdir()?;
    let keyring = MockKeyringStore::default();
    let storage =
        SecretsKeyringAuthStorage::new(home.path().to_path_buf(), Arc::new(keyring.clone()));
    assert!(storage.load_managed()?.auth.is_none());
    storage
        .secrets_manager
        .set(&SecretScope::Global, &CODEX_AUTH_SECRET_NAME, "{")?;
    assert_eq!(
        storage.load_managed().unwrap_err(),
        ManagedAuthStorageError::ReadFailed(ManagedAuthStorageFailure::Parse(
            ManagedAuthStorageSource::Secrets
        ))
    );
    let key = compute_keyring_account(home.path());
    keyring.set_error(
        &key,
        KeyringError::Invalid("secret-error-sentinel".into(), "load".into()),
    );
    assert_eq!(
        storage.load_managed().unwrap_err(),
        ManagedAuthStorageError::ReadFailed(ManagedAuthStorageFailure::Secrets)
    );
    Ok(())
}

#[test]
fn managed_auto_never_turns_failed_primary_into_fallback_auth_or_absence() -> anyhow::Result<()> {
    for backend in [
        AuthKeyringBackendKind::Direct,
        AuthKeyringBackendKind::Secrets,
    ] {
        let home = tempdir()?;
        let keyring = MockKeyringStore::default();
        let storage = AutoAuthStorage::new(
            home.path().to_path_buf(),
            Arc::new(keyring.clone()),
            backend,
        );
        let absent = storage.load_managed()?;
        assert!(absent.auth.is_none());
        assert_eq!(
            absent.source,
            ManagedAuthStorageSource::FileAfterKeyringAbsence
        );
        let file_auth = auth_with_prefix("fallback");
        storage.file_storage.save(&file_auth)?;
        let read = storage.load_managed()?;
        assert_eq!(read.auth, Some(file_auth));
        assert_eq!(
            read.source,
            ManagedAuthStorageSource::FileAfterKeyringAbsence
        );

        let (key, cause) = match backend {
            AuthKeyringBackendKind::Direct => (
                compute_store_key(home.path())?,
                ManagedAuthStorageFailure::Keyring,
            ),
            AuthKeyringBackendKind::Secrets => {
                seed_secrets_backend_with_auth(
                    &keyring,
                    home.path(),
                    &auth_with_prefix("primary"),
                )?;
                (
                    compute_keyring_account(home.path()),
                    ManagedAuthStorageFailure::Secrets,
                )
            }
        };
        for fallback_present in [true, false] {
            if !fallback_present {
                storage.file_storage.delete()?;
            }
            keyring.set_error(&key, KeyringError::Invalid("error".into(), "load".into()));
            assert_eq!(
                storage.load_managed().unwrap_err(),
                ManagedAuthStorageError::AutoFallbackRequired(cause)
            );
        }
    }
    Ok(())
}

#[test]
fn managed_auto_parse_failure_is_not_erased_by_valid_fallback() -> anyhow::Result<()> {
    let home = tempdir()?;
    let keyring = MockKeyringStore::default();
    let storage = AutoAuthStorage::new(
        home.path().to_path_buf(),
        Arc::new(keyring.clone()),
        AuthKeyringBackendKind::Direct,
    );
    storage
        .file_storage
        .save(&auth_with_prefix("valid-fallback"))?;
    keyring.save(KEYRING_SERVICE, &compute_store_key(home.path())?, "{")?;
    assert_eq!(
        storage.load_managed().unwrap_err(),
        ManagedAuthStorageError::AutoFallbackRequired(ManagedAuthStorageFailure::Parse(
            ManagedAuthStorageSource::Keyring
        ))
    );
    Ok(())
}

#[test]
fn managed_load_refuses_ephemeral_even_when_empty() -> anyhow::Result<()> {
    let home = tempdir()?;
    let storage = EphemeralAuthStorage::new(home.path().to_path_buf());
    assert_eq!(
        storage.load_managed().unwrap_err(),
        ManagedAuthStorageError::ReadFailed(ManagedAuthStorageFailure::UnsupportedStorage)
    );
    Ok(())
}

#[test]
fn managed_keyring_never_falls_back_to_an_unresolved_home_identity() -> anyhow::Result<()> {
    let home = tempdir()?;
    let missing = home.path().join("missing");
    let keyring = MockKeyringStore::default();
    let fallback_key = compute_store_key(&missing)?;
    keyring.save(
        KEYRING_SERVICE,
        &fallback_key,
        &serde_json::to_string(&auth_with_prefix("wrong-identity"))?,
    )?;
    let storage = DirectKeyringAuthStorage::new(missing, Arc::new(keyring));
    assert_eq!(
        storage.load_managed().unwrap_err(),
        ManagedAuthStorageError::ReadFailed(ManagedAuthStorageFailure::Keyring)
    );
    Ok(())
}

#[tokio::test]
async fn file_storage_load_returns_auth_dot_json() -> anyhow::Result<()> {
    let codex_home = tempdir()?;
    let storage = FileAuthStorage::new(codex_home.path().to_path_buf());
    let auth_dot_json = AuthDotJson {
        auth_mode: Some(AuthMode::ApiKey),
        openai_api_key: Some("test-key".to_string()),
        tokens: None,
        last_refresh: Some(Utc::now()),
        agent_identity: None,
        personal_access_token: None,
        bedrock_api_key: None,
    };

    storage
        .save(&auth_dot_json)
        .context("failed to save auth file")?;

    let loaded = storage.load().context("failed to load auth file")?;
    assert_eq!(Some(auth_dot_json), loaded);
    Ok(())
}

#[tokio::test]
async fn file_storage_save_persists_auth_dot_json() -> anyhow::Result<()> {
    let codex_home = tempdir()?;
    let storage = FileAuthStorage::new(codex_home.path().to_path_buf());
    let auth_dot_json = AuthDotJson {
        auth_mode: Some(AuthMode::ApiKey),
        openai_api_key: Some("test-key".to_string()),
        tokens: None,
        last_refresh: Some(Utc::now()),
        agent_identity: None,
        personal_access_token: None,
        bedrock_api_key: None,
    };

    let file = get_auth_file(codex_home.path());
    storage
        .save(&auth_dot_json)
        .context("failed to save auth file")?;

    let same_auth_dot_json = storage
        .try_read_auth_json(&file)
        .context("failed to read auth file after save")?;
    assert_eq!(auth_dot_json, same_auth_dot_json);
    Ok(())
}

#[tokio::test]
async fn file_storage_round_trips_agent_identity_auth() -> anyhow::Result<()> {
    let codex_home = tempdir()?;
    let storage = FileAuthStorage::new(codex_home.path().to_path_buf());
    let agent_identity = jwt_with_payload(json!({
        "agent_runtime_id": "agent-runtime-id",
        "agent_private_key": "private-key",
        "account_id": "account-id",
        "chatgpt_user_id": "user-id",
        "email": "user@example.com",
        "plan_type": "pro",
        "chatgpt_account_is_fedramp": false,
    }));
    let auth_dot_json = AuthDotJson {
        auth_mode: Some(AuthMode::AgentIdentity),
        openai_api_key: None,
        tokens: None,
        last_refresh: None,
        agent_identity: Some(AgentIdentityStorage::Jwt(agent_identity)),
        personal_access_token: None,
        bedrock_api_key: None,
    };

    storage.save(&auth_dot_json)?;

    let loaded = storage.load()?;
    assert_eq!(Some(auth_dot_json), loaded);
    Ok(())
}

#[tokio::test]
async fn file_storage_round_trips_registered_agent_identity_auth() -> anyhow::Result<()> {
    let codex_home = tempdir()?;
    let storage = FileAuthStorage::new(codex_home.path().to_path_buf());
    let record = AgentIdentityAuthRecord {
        agent_runtime_id: "agent-runtime-id".to_string(),
        agent_private_key: "private-key".to_string(),
        account_id: "account-id".to_string(),
        chatgpt_user_id: "user-id".to_string(),
        email: Some("user@example.com".to_string()),
        plan_type: AccountPlanType::Pro,
        chatgpt_account_is_fedramp: false,
        task_id: Some("task-id".to_string()),
    };
    let auth_dot_json = AuthDotJson {
        auth_mode: Some(AuthMode::Chatgpt),
        openai_api_key: None,
        tokens: None,
        last_refresh: None,
        agent_identity: Some(AgentIdentityStorage::Record(record)),
        personal_access_token: None,
        bedrock_api_key: None,
    };

    storage.save(&auth_dot_json)?;

    let loaded = storage.load()?;
    assert_eq!(Some(auth_dot_json), loaded);
    Ok(())
}

#[tokio::test]
async fn file_storage_loads_empty_agent_identity_email_as_none() -> anyhow::Result<()> {
    let codex_home = tempdir()?;
    let storage = FileAuthStorage::new(codex_home.path().to_path_buf());
    let auth_file = get_auth_file(codex_home.path());
    std::fs::write(
        &auth_file,
        serde_json::to_string_pretty(&json!({
            "auth_mode": "chatgpt",
            "agent_identity": {
                "agent_runtime_id": "agent-runtime-id",
                "agent_private_key": "private-key",
                "account_id": "account-id",
                "chatgpt_user_id": "user-id",
                "email": "",
                "plan_type": "pro",
                "chatgpt_account_is_fedramp": false,
            },
        }))?,
    )?;

    let loaded = storage.load()?;

    assert_eq!(
        loaded,
        Some(AuthDotJson {
            auth_mode: Some(AuthMode::Chatgpt),
            openai_api_key: None,
            tokens: None,
            last_refresh: None,
            agent_identity: Some(AgentIdentityStorage::Record(AgentIdentityAuthRecord {
                agent_runtime_id: "agent-runtime-id".to_string(),
                agent_private_key: "private-key".to_string(),
                account_id: "account-id".to_string(),
                chatgpt_user_id: "user-id".to_string(),
                email: None,
                plan_type: AccountPlanType::Pro,
                chatgpt_account_is_fedramp: false,
                task_id: None,
            })),
            personal_access_token: None,
            bedrock_api_key: None,
        })
    );
    Ok(())
}

#[tokio::test]
async fn file_storage_writes_missing_agent_identity_email_as_empty_string() -> anyhow::Result<()> {
    let codex_home = tempdir()?;
    let storage = FileAuthStorage::new(codex_home.path().to_path_buf());
    let auth_dot_json = AuthDotJson {
        auth_mode: Some(AuthMode::Chatgpt),
        openai_api_key: None,
        tokens: None,
        last_refresh: None,
        agent_identity: Some(AgentIdentityStorage::Record(AgentIdentityAuthRecord {
            agent_runtime_id: "agent-runtime-id".to_string(),
            agent_private_key: "private-key".to_string(),
            account_id: "account-id".to_string(),
            chatgpt_user_id: "user-id".to_string(),
            email: None,
            plan_type: AccountPlanType::Pro,
            chatgpt_account_is_fedramp: false,
            task_id: None,
        })),
        personal_access_token: None,
        bedrock_api_key: None,
    };

    storage.save(&auth_dot_json)?;

    let auth_file = get_auth_file(codex_home.path());
    let saved: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(auth_file)?)?;
    assert_eq!(saved["agent_identity"]["email"], "");
    assert_eq!(storage.load()?, Some(auth_dot_json));
    Ok(())
}

#[tokio::test]
async fn file_storage_round_trips_personal_access_token_auth() -> anyhow::Result<()> {
    let codex_home = tempdir()?;
    let storage = FileAuthStorage::new(codex_home.path().to_path_buf());
    let auth_dot_json = AuthDotJson {
        auth_mode: Some(AuthMode::PersonalAccessToken),
        openai_api_key: None,
        tokens: None,
        last_refresh: None,
        agent_identity: None,
        personal_access_token: Some("at-example".to_string()),
        bedrock_api_key: None,
    };

    storage.save(&auth_dot_json)?;

    let loaded = storage.load()?;
    assert_eq!(Some(auth_dot_json), loaded);
    Ok(())
}

#[tokio::test]
async fn file_storage_loads_agent_identity_as_jwt() -> anyhow::Result<()> {
    let codex_home = tempdir()?;
    let storage = FileAuthStorage::new(codex_home.path().to_path_buf());
    let agent_identity_jwt = jwt_with_payload(json!({
        "agent_runtime_id": "agent-runtime-id",
        "agent_private_key": "private-key",
        "account_id": "account-id",
        "chatgpt_user_id": "user-id",
        "email": "user@example.com",
        "plan_type": "pro",
        "chatgpt_account_is_fedramp": false,
    }));
    let auth_file = get_auth_file(codex_home.path());
    std::fs::write(
        &auth_file,
        serde_json::to_string_pretty(&json!({
            "auth_mode": "agentIdentity",
            "agent_identity": agent_identity_jwt,
        }))?,
    )?;

    let loaded = storage.load()?;

    assert_eq!(
        loaded.expect("auth should load").agent_identity,
        Some(AgentIdentityStorage::Jwt(agent_identity_jwt))
    );
    Ok(())
}

#[test]
fn file_storage_delete_removes_auth_file() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let auth_dot_json = AuthDotJson {
        auth_mode: Some(AuthMode::ApiKey),
        openai_api_key: Some("sk-test-key".to_string()),
        tokens: None,
        last_refresh: None,
        agent_identity: None,
        personal_access_token: None,
        bedrock_api_key: None,
    };
    let storage = create_auth_storage(
        dir.path().to_path_buf(),
        AuthCredentialsStoreMode::File,
        AuthKeyringBackendKind::default(),
    );
    storage.save(&auth_dot_json)?;
    assert!(dir.path().join("auth.json").exists());
    let storage = FileAuthStorage::new(dir.path().to_path_buf());
    let removed = storage.delete()?;
    assert!(removed);
    assert!(!dir.path().join("auth.json").exists());
    Ok(())
}

#[test]
fn ephemeral_storage_save_load_delete_is_in_memory_only() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let storage = create_auth_storage(
        dir.path().to_path_buf(),
        AuthCredentialsStoreMode::Ephemeral,
        AuthKeyringBackendKind::default(),
    );
    let auth_dot_json = AuthDotJson {
        auth_mode: Some(AuthMode::ApiKey),
        openai_api_key: Some("sk-ephemeral".to_string()),
        tokens: None,
        last_refresh: Some(Utc::now()),
        agent_identity: None,
        personal_access_token: None,
        bedrock_api_key: None,
    };

    storage.save(&auth_dot_json)?;
    let loaded = storage.load()?;
    assert_eq!(Some(auth_dot_json), loaded);

    let removed = storage.delete()?;
    assert!(removed);
    let loaded = storage.load()?;
    assert_eq!(None, loaded);
    assert!(!get_auth_file(dir.path()).exists());
    Ok(())
}

fn seed_secrets_backend_and_fallback_auth_file_for_delete(
    mock_keyring: &MockKeyringStore,
    codex_home: &Path,
    auth: &AuthDotJson,
) -> anyhow::Result<PathBuf> {
    let manager = SecretsManager::new_with_keyring_store_and_namespace(
        codex_home.to_path_buf(),
        SecretsBackendKind::Local,
        Arc::new(mock_keyring.clone()),
        LocalSecretsNamespace::CodexAuth,
    );
    manager.set(
        &SecretScope::Global,
        &CODEX_AUTH_SECRET_NAME,
        &serde_json::to_string(auth)?,
    )?;
    let auth_file = get_auth_file(codex_home);
    std::fs::write(&auth_file, "stale")?;
    Ok(auth_file)
}

fn seed_secrets_backend_with_auth(
    mock_keyring: &MockKeyringStore,
    codex_home: &Path,
    auth: &AuthDotJson,
) -> anyhow::Result<()> {
    let manager = SecretsManager::new_with_keyring_store_and_namespace(
        codex_home.to_path_buf(),
        SecretsBackendKind::Local,
        Arc::new(mock_keyring.clone()),
        LocalSecretsNamespace::CodexAuth,
    );
    manager.set(
        &SecretScope::Global,
        &CODEX_AUTH_SECRET_NAME,
        &serde_json::to_string(auth)?,
    )?;
    Ok(())
}

fn assert_keyring_saved_auth_and_removed_fallback(
    mock_keyring: &MockKeyringStore,
    codex_home: &Path,
    expected: &AuthDotJson,
) -> anyhow::Result<()> {
    let manager = SecretsManager::new_with_keyring_store_and_namespace(
        codex_home.to_path_buf(),
        SecretsBackendKind::Local,
        Arc::new(mock_keyring.clone()),
        LocalSecretsNamespace::CodexAuth,
    );
    let saved_value = manager
        .get(&SecretScope::Global, &CODEX_AUTH_SECRET_NAME)?
        .context("encrypted auth entry should exist")?;
    let expected_serialized = serde_json::to_string(expected)?;
    assert_eq!(saved_value, expected_serialized);
    let old_key = compute_store_key(codex_home)?;
    assert!(
        mock_keyring.saved_value(&old_key).is_none(),
        "legacy keyring auth entry should not be used"
    );
    let secrets_key = compute_keyring_account(codex_home);
    assert!(
        mock_keyring.saved_value(&secrets_key).is_some(),
        "secrets backend should persist an encryption passphrase in the keyring"
    );
    assert!(encrypted_auth_file(codex_home).exists());
    let auth_file = get_auth_file(codex_home);
    assert!(
        !auth_file.exists(),
        "fallback auth.json should be removed after keyring save"
    );
    Ok(())
}

fn encrypted_auth_file(codex_home: &Path) -> PathBuf {
    codex_home.join("secrets").join("codex_auth.age")
}

fn id_token_with_prefix(prefix: &str) -> IdTokenInfo {
    #[derive(Serialize)]
    struct Header {
        alg: &'static str,
        typ: &'static str,
    }

    let header = Header {
        alg: "none",
        typ: "JWT",
    };
    let payload = json!({
        "email": format!("{prefix}@example.com"),
        "https://api.openai.com/auth": {
            "chatgpt_account_id": format!("{prefix}-account"),
        },
    });
    let encode = |bytes: &[u8]| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
    let header_b64 = encode(&serde_json::to_vec(&header).expect("serialize header"));
    let payload_b64 = encode(&serde_json::to_vec(&payload).expect("serialize payload"));
    let signature_b64 = encode(b"sig");
    let fake_jwt = format!("{header_b64}.{payload_b64}.{signature_b64}");

    crate::token_data::parse_chatgpt_jwt_claims(&fake_jwt).expect("fake JWT should parse")
}

fn auth_with_prefix(prefix: &str) -> AuthDotJson {
    AuthDotJson {
        auth_mode: Some(AuthMode::ApiKey),
        openai_api_key: Some(format!("{prefix}-api-key")),
        tokens: Some(TokenData {
            id_token: id_token_with_prefix(prefix),
            access_token: format!("{prefix}-access"),
            refresh_token: format!("{prefix}-refresh"),
            account_id: Some(format!("{prefix}-account-id")),
        }),
        last_refresh: None,
        agent_identity: None,
        personal_access_token: None,
        bedrock_api_key: None,
    }
}

fn jwt_with_payload(payload: serde_json::Value) -> String {
    let encode = |bytes: &[u8]| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
    let header_b64 = encode(br#"{"alg":"EdDSA","typ":"JWT"}"#);
    let payload_b64 = encode(&serde_json::to_vec(&payload).expect("payload should serialize"));
    let signature_b64 = encode(b"sig");
    format!("{header_b64}.{payload_b64}.{signature_b64}")
}

#[test]
fn secrets_keyring_auth_storage_load_returns_deserialized_auth() -> anyhow::Result<()> {
    let codex_home = tempdir()?;
    let mock_keyring = MockKeyringStore::default();
    let storage = SecretsKeyringAuthStorage::new(
        codex_home.path().to_path_buf(),
        Arc::new(mock_keyring.clone()),
    );
    let expected = AuthDotJson {
        auth_mode: Some(AuthMode::ApiKey),
        openai_api_key: Some("sk-test".to_string()),
        tokens: None,
        last_refresh: None,
        agent_identity: None,
        personal_access_token: None,
        bedrock_api_key: None,
    };
    seed_secrets_backend_with_auth(&mock_keyring, codex_home.path(), &expected)?;

    let loaded = storage.load()?;
    assert_eq!(Some(expected), loaded);
    Ok(())
}

#[test]
fn keyring_auth_storage_compute_store_key_for_home_directory() -> anyhow::Result<()> {
    let codex_home = PathBuf::from("~/.codex");

    let key = compute_store_key(codex_home.as_path())?;

    assert_eq!(key, "cli|940db7b1d0e4eb40");
    Ok(())
}

#[test]
fn direct_keyring_auth_storage_saves_legacy_keyring_entry() -> anyhow::Result<()> {
    let codex_home = tempdir()?;
    let mock_keyring = MockKeyringStore::default();
    let storage = DirectKeyringAuthStorage::new(
        codex_home.path().to_path_buf(),
        Arc::new(mock_keyring.clone()),
    );
    let auth_file = get_auth_file(codex_home.path());
    std::fs::write(&auth_file, "stale")?;
    let auth = auth_with_prefix("direct");

    storage.save(&auth)?;

    let legacy_key = compute_store_key(codex_home.path())?;
    let saved_value = mock_keyring
        .saved_value(&legacy_key)
        .context("direct keyring auth entry should exist")?;
    assert_eq!(saved_value, serde_json::to_string(&auth)?);
    assert!(!encrypted_auth_file(codex_home.path()).exists());
    assert!(
        !auth_file.exists(),
        "fallback auth.json should be removed after keyring save"
    );
    assert_eq!(storage.load()?, Some(auth));
    Ok(())
}

#[test]
fn direct_keyring_auth_storage_delete_removes_keyring_and_file() -> anyhow::Result<()> {
    let codex_home = tempdir()?;
    let mock_keyring = MockKeyringStore::default();
    let storage = DirectKeyringAuthStorage::new(
        codex_home.path().to_path_buf(),
        Arc::new(mock_keyring.clone()),
    );
    let auth = auth_with_prefix("direct-delete");
    storage.save(&auth)?;
    let auth_file = get_auth_file(codex_home.path());
    std::fs::write(&auth_file, "stale")?;

    let removed = storage.delete()?;

    assert!(removed, "delete should report removal");
    assert_eq!(storage.load()?, None, "keyring auth should be removed");
    assert!(
        mock_keyring
            .saved_value(&compute_store_key(codex_home.path())?)
            .is_none(),
        "legacy keyring auth entry should be removed"
    );
    assert!(
        !auth_file.exists(),
        "fallback auth.json should be removed after keyring delete"
    );
    assert!(!encrypted_auth_file(codex_home.path()).exists());
    Ok(())
}

#[test]
fn factory_uses_secrets_backend_only_when_requested() -> anyhow::Result<()> {
    let direct_home = tempdir()?;
    let direct_keyring = MockKeyringStore::default();
    let direct_storage = create_auth_storage_with_store(
        direct_home.path().to_path_buf(),
        AuthCredentialsStoreMode::Keyring,
        Arc::new(direct_keyring.clone()),
        AuthKeyringBackendKind::Direct,
    );
    let direct_auth = auth_with_prefix("factory-direct");
    direct_storage.save(&direct_auth)?;
    assert!(
        direct_keyring
            .saved_value(&compute_store_key(direct_home.path())?)
            .is_some()
    );
    assert!(!encrypted_auth_file(direct_home.path()).exists());

    let secrets_home = tempdir()?;
    let secrets_keyring = MockKeyringStore::default();
    let secrets_storage = create_auth_storage_with_store(
        secrets_home.path().to_path_buf(),
        AuthCredentialsStoreMode::Keyring,
        Arc::new(secrets_keyring.clone()),
        AuthKeyringBackendKind::Secrets,
    );
    let secrets_auth = auth_with_prefix("factory-secrets");
    secrets_storage.save(&secrets_auth)?;
    assert!(
        secrets_keyring
            .saved_value(&compute_keyring_account(secrets_home.path()))
            .is_some()
    );
    assert!(encrypted_auth_file(secrets_home.path()).exists());
    Ok(())
}

#[test]
fn secrets_keyring_auth_storage_save_persists_and_removes_fallback_file() -> anyhow::Result<()> {
    let codex_home = tempdir()?;
    let mock_keyring = MockKeyringStore::default();
    let storage = SecretsKeyringAuthStorage::new(
        codex_home.path().to_path_buf(),
        Arc::new(mock_keyring.clone()),
    );
    let auth_file = get_auth_file(codex_home.path());
    std::fs::write(&auth_file, "stale")?;
    let auth = AuthDotJson {
        auth_mode: Some(AuthMode::Chatgpt),
        openai_api_key: None,
        tokens: Some(TokenData {
            id_token: Default::default(),
            access_token: "access".to_string(),
            refresh_token: "refresh".to_string(),
            account_id: Some("account".to_string()),
        }),
        last_refresh: Some(Utc::now()),
        agent_identity: None,
        personal_access_token: None,
        bedrock_api_key: None,
    };

    storage.save(&auth)?;

    assert_keyring_saved_auth_and_removed_fallback(&mock_keyring, codex_home.path(), &auth)?;
    Ok(())
}

#[test]
fn secrets_keyring_auth_storage_delete_removes_keyring_and_file() -> anyhow::Result<()> {
    let codex_home = tempdir()?;
    let mock_keyring = MockKeyringStore::default();
    let storage = SecretsKeyringAuthStorage::new(
        codex_home.path().to_path_buf(),
        Arc::new(mock_keyring.clone()),
    );
    let auth = auth_with_prefix("to-delete");
    let auth_file = seed_secrets_backend_and_fallback_auth_file_for_delete(
        &mock_keyring,
        codex_home.path(),
        &auth,
    )?;

    let removed = storage.delete()?;

    assert!(removed, "delete should report removal");
    assert_eq!(storage.load()?, None, "encrypted auth should be removed");
    assert!(
        !auth_file.exists(),
        "fallback auth.json should be removed after keyring delete"
    );
    Ok(())
}

#[test]
fn secrets_keyring_auth_storage_delete_removes_legacy_direct_keyring_entry() -> anyhow::Result<()> {
    let codex_home = tempdir()?;
    let mock_keyring = MockKeyringStore::default();
    let direct_storage = DirectKeyringAuthStorage::new(
        codex_home.path().to_path_buf(),
        Arc::new(mock_keyring.clone()),
    );
    direct_storage.save(&auth_with_prefix("legacy-direct"))?;
    let storage = SecretsKeyringAuthStorage::new(
        codex_home.path().to_path_buf(),
        Arc::new(mock_keyring.clone()),
    );
    let auth = auth_with_prefix("to-delete");
    let auth_file = seed_secrets_backend_and_fallback_auth_file_for_delete(
        &mock_keyring,
        codex_home.path(),
        &auth,
    )?;

    let removed = storage.delete()?;

    assert!(removed, "delete should report removal");
    assert_eq!(storage.load()?, None, "encrypted auth should be removed");
    assert_eq!(
        direct_storage.load()?,
        None,
        "legacy direct keyring auth should be removed"
    );
    assert!(
        !auth_file.exists(),
        "fallback auth.json should be removed after keyring delete"
    );
    Ok(())
}

#[test]
fn auto_auth_storage_load_prefers_keyring_value() -> anyhow::Result<()> {
    let codex_home = tempdir()?;
    let mock_keyring = MockKeyringStore::default();
    let storage = AutoAuthStorage::new(
        codex_home.path().to_path_buf(),
        Arc::new(mock_keyring.clone()),
        AuthKeyringBackendKind::Secrets,
    );
    let keyring_auth = auth_with_prefix("keyring");
    seed_secrets_backend_with_auth(&mock_keyring, codex_home.path(), &keyring_auth)?;

    let file_auth = auth_with_prefix("file");
    storage.file_storage.save(&file_auth)?;

    let loaded = storage.load()?;
    assert_eq!(loaded, Some(keyring_auth));
    Ok(())
}

#[test]
fn auto_auth_storage_load_uses_file_when_keyring_empty() -> anyhow::Result<()> {
    let codex_home = tempdir()?;
    let mock_keyring = MockKeyringStore::default();
    let storage = AutoAuthStorage::new(
        codex_home.path().to_path_buf(),
        Arc::new(mock_keyring),
        AuthKeyringBackendKind::Secrets,
    );

    let expected = auth_with_prefix("file-only");
    storage.file_storage.save(&expected)?;

    let loaded = storage.load()?;
    assert_eq!(loaded, Some(expected));
    Ok(())
}

#[test]
fn auto_auth_storage_load_falls_back_when_keyring_errors() -> anyhow::Result<()> {
    let codex_home = tempdir()?;
    let mock_keyring = MockKeyringStore::default();
    let storage = AutoAuthStorage::new(
        codex_home.path().to_path_buf(),
        Arc::new(mock_keyring.clone()),
        AuthKeyringBackendKind::Secrets,
    );
    let key = compute_keyring_account(codex_home.path());

    let encrypted = auth_with_prefix("encrypted");
    seed_secrets_backend_with_auth(&mock_keyring, codex_home.path(), &encrypted)?;
    mock_keyring.set_error(&key, KeyringError::Invalid("error".into(), "load".into()));

    let expected = auth_with_prefix("fallback");
    storage.file_storage.save(&expected)?;

    let loaded = storage.load()?;
    assert_eq!(loaded, Some(expected));
    Ok(())
}

#[test]
fn auto_auth_storage_save_prefers_keyring() -> anyhow::Result<()> {
    let codex_home = tempdir()?;
    let mock_keyring = MockKeyringStore::default();
    let storage = AutoAuthStorage::new(
        codex_home.path().to_path_buf(),
        Arc::new(mock_keyring.clone()),
        AuthKeyringBackendKind::Secrets,
    );
    let stale = auth_with_prefix("stale");
    storage.file_storage.save(&stale)?;

    let expected = auth_with_prefix("to-save");
    storage.save(&expected)?;

    assert_keyring_saved_auth_and_removed_fallback(&mock_keyring, codex_home.path(), &expected)?;
    Ok(())
}

#[test]
fn auto_auth_storage_save_falls_back_when_keyring_errors() -> anyhow::Result<()> {
    let codex_home = tempdir()?;
    let mock_keyring = MockKeyringStore::default();
    let storage = AutoAuthStorage::new(
        codex_home.path().to_path_buf(),
        Arc::new(mock_keyring.clone()),
        AuthKeyringBackendKind::Secrets,
    );
    let key = compute_keyring_account(codex_home.path());
    mock_keyring.set_error(&key, KeyringError::Invalid("error".into(), "save".into()));

    let auth = auth_with_prefix("fallback");
    storage.save(&auth)?;

    let auth_file = get_auth_file(codex_home.path());
    assert!(
        auth_file.exists(),
        "fallback auth.json should be created when keyring save fails"
    );
    let saved = storage
        .file_storage
        .load()?
        .context("fallback auth should exist")?;
    assert_eq!(saved, auth);
    assert!(
        mock_keyring.saved_value(&key).is_none(),
        "keyring should not contain value when save fails"
    );
    Ok(())
}

#[test]
fn auto_auth_storage_delete_removes_keyring_and_file() -> anyhow::Result<()> {
    let codex_home = tempdir()?;
    let mock_keyring = MockKeyringStore::default();
    let storage = AutoAuthStorage::new(
        codex_home.path().to_path_buf(),
        Arc::new(mock_keyring.clone()),
        AuthKeyringBackendKind::Secrets,
    );
    let auth = auth_with_prefix("to-delete");
    let auth_file = seed_secrets_backend_and_fallback_auth_file_for_delete(
        &mock_keyring,
        codex_home.path(),
        &auth,
    )?;

    let removed = storage.delete()?;

    assert!(removed, "delete should report removal");
    assert_eq!(storage.load()?, None, "encrypted auth should be removed");
    assert!(
        !auth_file.exists(),
        "fallback auth.json should be removed after delete"
    );
    Ok(())
}
