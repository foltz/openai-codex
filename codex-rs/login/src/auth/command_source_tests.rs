use super::*;
use base64::Engine;
use pretty_assertions::assert_eq;
use tempfile::TempDir;

fn envelope(generation: u64) -> Envelope {
    envelope_with_expiration(generation, chrono::Utc::now().timestamp() + 3600)
}

fn envelope_with_expiration(generation: u64, expiration: i64) -> Envelope {
    let body = serde_json::json!({"exp":expiration,"jti":generation,"https://api.openai.com/auth":{"chatgpt_account_id":"account","chatgpt_user_id":"principal"}});
    let access_token = format!(
        "e30.{}.signature",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(body.to_string())
    );
    Envelope {
        version: 1,
        source: "source".into(),
        binding: "fixture#example".into(),
        account: "account".into(),
        principal: "principal".into(),
        generation,
        fingerprint: format!("{:x}", sha2::Sha256::digest(access_token.as_bytes())),
        access_token,
        expires_at: expiration,
    }
}

fn session_with_state(home: &std::path::Path, record: Envelope) -> CommandSource {
    CommandSource {
        selection: Selection {
            home: home.to_path_buf(),
            command: home.join("unused-command"),
            binding: record.binding.clone(),
            source: record.source.clone(),
            account: record.account.clone(),
            principal: record.principal.clone(),
        },
        gate: Semaphore::new(1),
        state: Mutex::new(Some(record)),
        rejected: Mutex::new(HashSet::new()),
        retry_after: AtomicI64::new(chrono::Utc::now().timestamp() + 60),
        managers: Mutex::new(Vec::new()),
    }
}

#[cfg(unix)]
#[tokio::test]
async fn command_source_does_not_reuse_rejected_newer_disk_credential() {
    let home = TempDir::new().expect("home");
    let old = envelope(1);
    let newer = envelope(2);
    command_store::commit(home.path(), &newer, &newer.auth().expect("new auth"))
        .expect("another process published newer credential");
    let source = session_with_state(home.path(), old);
    let result = source.obtain(Some(newer.fingerprint)).await;
    assert!(
        result.is_err(),
        "a credential actually rejected by the request must not become a cache success"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn command_source_coalesces_old_rejection_onto_unrejected_newer_disk_credential() {
    let home = TempDir::new().expect("home");
    let old = envelope(1);
    let newer = envelope(2);
    command_store::commit(home.path(), &newer, &newer.auth().expect("new auth"))
        .expect("another process published newer credential");
    let source = session_with_state(home.path(), old.clone());
    let result = source.obtain(Some(old.fingerprint)).await;
    assert!(
        result.is_ok(),
        "an old request's rejection must not poison the new credential"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn command_source_rejection_capacity_cannot_leave_rejected_current_auth_usable() {
    let home = TempDir::new().expect("home");
    let current = envelope(2);
    let auth = current.auth().expect("current auth");
    command_store::commit(home.path(), &current, &auth).expect("commit");
    let source = session_with_state(home.path(), current.clone());
    source
        .rejected
        .lock()
        .expect("rejection state")
        .extend((0..16).map(|index| format!("stale-request-fingerprint-{index}")));
    assert!(source.obtain(Some(current.fingerprint)).await.is_err());
    assert!(
        !source.usable_auth(Some(&auth)),
        "capacity must not leave the actually rejected credential ready"
    );
    assert_eq!(source.receipt(Some(&auth))["coherent"], false);
    assert!(source.rejected.lock().expect("bounded rejections").len() <= 16);
}

#[test]
fn command_selection_requires_the_external_token_mode_before_source_io() {
    assert!(CommandSource::require_allowed_mode(None).is_ok());
    assert!(CommandSource::require_allowed_mode(Some(&[AuthMode::ChatgptAuthTokens])).is_ok());
    for modes in [&[][..], &[AuthMode::Chatgpt][..], &[AuthMode::ApiKey][..]] {
        assert_eq!(
            CommandSource::require_allowed_mode(Some(modes))
                .unwrap_err()
                .kind(),
            io::ErrorKind::PermissionDenied
        );
    }
}

#[cfg(unix)]
#[test]
fn command_source_accepts_only_its_selected_complete_principal() {
    let home = TempDir::new().unwrap();
    let record = envelope(1);
    let source = session_with_state(home.path(), record.clone());
    let auth = record.auth().unwrap();
    assert!(source.accepts(Some(&auth), &Some(record.clone())));
    assert!(!source.accepts(None, &Some(record.clone())));
    assert!(!source.accepts(
        Some(&CodexAuth::from_api_key("synthetic")),
        &Some(record.clone())
    ));
    let mut foreign = record.clone();
    foreign.source = "foreign-source".into();
    assert!(!source.accepts(Some(&auth), &Some(foreign)));
    let mut foreign = record.clone();
    foreign.principal = "foreign-principal".into();
    assert!(!source.accepts(Some(&auth), &Some(foreign)));
    assert!(!source.accepts(Some(&auth), &None));
}

#[cfg(unix)]
#[test]
fn cache_guard_retains_source_state_and_the_actual_native_writer_domain() {
    use super::super::storage::{
        ManagedAuthStorageError, ManagedAuthStorageFailure, acquire_command_auth_source,
    };
    let home = TempDir::new().unwrap();
    let record = envelope(1);
    let auth = record.auth().unwrap();
    command_store::commit(home.path(), &record, &auth).unwrap();
    let source = session_with_state(home.path(), record);
    let guard = source.cache_guard(Some(&auth)).unwrap();
    assert!(matches!(
        source.state.try_lock(),
        Err(std::sync::TryLockError::WouldBlock)
    ));
    assert!(matches!(
        acquire_command_auth_source(home.path()),
        Err(ManagedAuthStorageError::ReadFailed(
            ManagedAuthStorageFailure::CoordinationContended
        ))
    ));
    guard.require_current_domain().unwrap();
    drop(guard);
    assert!(source.state.try_lock().is_ok());
    assert!(acquire_command_auth_source(home.path()).is_ok());
}

#[cfg(unix)]
#[test]
fn cache_guard_refuses_a_newer_durable_generation_before_cache_install() {
    let home = TempDir::new().unwrap();
    let old = envelope(1);
    let auth = old.auth().unwrap();
    command_store::commit(home.path(), &old, &auth).unwrap();
    let source = session_with_state(home.path(), old);
    let newer = envelope(2);
    command_store::commit(home.path(), &newer, &newer.auth().unwrap()).unwrap();
    assert!(source.cache_guard(Some(&auth)).is_err());
    assert_eq!(source.state.lock().unwrap().as_ref().unwrap().generation, 1);
    assert_eq!(
        command_store::read(home.path())
            .unwrap()
            .unwrap()
            .generation,
        2
    );
}

#[cfg(unix)]
#[tokio::test]
async fn lost_live_ownership_is_not_cached_success_or_transient_fallback() {
    for remaining in [3600, 60] {
        let home = TempDir::new().unwrap();
        let record = envelope_with_expiration(1, chrono::Utc::now().timestamp() + remaining);
        command_store::commit(home.path(), &record, &record.auth().unwrap()).unwrap();
        let source = session_with_state(home.path(), record);
        let backoff = source.retry_after.load(Ordering::Acquire);
        std::fs::remove_file(home.path().join("auth.json")).unwrap();
        std::fs::remove_file(home.path().join(".external-auth-owner.json")).unwrap();
        assert!(source.obtain(None).await.is_err());
        assert_eq!(source.retry_after.load(Ordering::Acquire), backoff);
        assert!(!home.path().join("auth.json").exists());
        assert!(!home.path().join(".external-auth-owner.json").exists());
    }
}

#[cfg(unix)]
#[tokio::test]
async fn newer_expired_durable_ownership_is_imported_only_for_renewal() {
    use std::os::unix::fs::PermissionsExt;
    let home = TempDir::new().unwrap();
    let old = envelope(1);
    command_store::commit(home.path(), &old, &old.auth().unwrap()).unwrap();
    let source = session_with_state(home.path(), old);
    let expired = envelope_with_expiration(2, chrono::Utc::now().timestamp() - 60);
    expired.validate().unwrap();
    assert!(expired.auth().is_err());
    // Simulate a once-valid generation already expired on disk. The production
    // writer correctly refuses new expired commits; do not weaken that writer.
    let auth =
        CodexAuth::from_external_chatgpt_tokens(&expired.access_token, &expired.account, None)
            .unwrap();
    let mut json = serde_json::to_value(auth.get_current_auth_json().unwrap()).unwrap();
    json["external_auth_owner"] = serde_json::to_value(&expired).unwrap();
    let bytes = serde_json::to_vec(&json).unwrap();
    let staged = home.path().join("expired-fixture.json");
    std::fs::write(&staged, &bytes).unwrap();
    std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o400)).unwrap();
    std::fs::rename(&staged, home.path().join("auth.json")).unwrap();
    assert!(
        source.obtain(None).await.is_err(),
        "deferred renewal cannot return expired auth"
    );
    // Envelope intentionally has no Debug implementation: never print tokens
    // just to report an equality failure.
    assert!(source.state.lock().unwrap().as_ref() == Some(&expired));
    assert!(!source.usable_auth(Some(&auth)));
    assert_eq!(std::fs::read(home.path().join("auth.json")).unwrap(), bytes);
}

#[cfg(unix)]
#[test]
fn cache_guard_refuses_absent_mismatched_rejected_or_unprotected_credentials() {
    use std::os::unix::fs::PermissionsExt;
    let home = TempDir::new().unwrap();
    let record = envelope(1);
    let auth = record.auth().unwrap();
    command_store::commit(home.path(), &record, &auth).unwrap();
    let source = session_with_state(home.path(), record.clone());
    assert!(source.cache_guard(None).is_err());
    assert!(
        source
            .cache_guard(Some(&CodexAuth::from_api_key("synthetic")))
            .is_err()
    );
    source
        .rejected
        .lock()
        .unwrap()
        .insert(record.fingerprint.clone());
    assert!(source.cache_guard(Some(&auth)).is_err());
    source.rejected.lock().unwrap().clear();
    std::fs::set_permissions(
        home.path().join("auth.json"),
        std::fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    assert!(source.cache_guard(Some(&auth)).is_err());
    std::fs::set_permissions(
        home.path().join("auth.json"),
        std::fs::Permissions::from_mode(0o400),
    )
    .unwrap();
    assert!(source.cache_guard(Some(&auth)).is_ok());
}

#[cfg(unix)]
#[test]
fn poisoned_source_state_refuses_publication_without_repair() {
    let home = TempDir::new().unwrap();
    let record = envelope(1);
    let auth = record.auth().unwrap();
    command_store::commit(home.path(), &record, &auth).unwrap();
    let source = Arc::new(session_with_state(home.path(), record));
    let poison = source.clone();
    assert!(
        std::thread::spawn(move || {
            let _guard = poison.state.lock().unwrap();
            panic!("synthetic command state poison");
        })
        .join()
        .is_err()
    );
    assert!(source.cache_guard(Some(&auth)).is_err());
    assert!(source.state.is_poisoned());
}

#[cfg(unix)]
#[test]
fn retained_cache_guard_detects_replaced_native_lock_inode() {
    use std::os::unix::fs::OpenOptionsExt;
    let home = TempDir::new().unwrap();
    let record = envelope(1);
    let auth = record.auth().unwrap();
    command_store::commit(home.path(), &record, &auth).unwrap();
    let source = session_with_state(home.path(), record);
    let guard = source.cache_guard(Some(&auth)).unwrap();
    let path = home.path().join("auth.json.lock");
    assert!(path.is_file());
    std::fs::rename(&path, home.path().join("retired-lock")).unwrap();
    drop(
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .unwrap(),
    );
    assert!(guard.require_current_domain().is_err());
}

#[test]
fn expired_envelope_retains_valid_ownership_but_never_constructs_usable_auth() {
    let expired = envelope_with_expiration(2, chrono::Utc::now().timestamp() - 60);
    assert!(expired.validate().is_ok());
    assert!(expired.auth().is_err());
    let mut invalid = expired;
    invalid.principal = "different-principal".into();
    assert!(invalid.validate().is_err());
}
