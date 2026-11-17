use super::super::command_source::{CommandSource, Envelope};
use super::super::command_store;
use super::*;
use base64::Engine;

pub(super) fn record(generation: u64) -> Envelope {
    use sha2::Digest;
    let expires_at = chrono::Utc::now().timestamp() + 3600;
    let body = serde_json::json!({"exp": expires_at, "jti": generation, "https://api.openai.com/auth": {
        "chatgpt_account_id": "account", "chatgpt_user_id": "principal"
    }});
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
        expires_at,
    }
}

#[cfg(unix)]
pub(super) fn selected_fixture(
    home: &Path,
    record: Envelope,
) -> (Arc<CommandSource>, Arc<AuthManager>) {
    let auth = record.auth().unwrap();
    command_store::commit(home, &record, &auth).unwrap();
    let source = Arc::new(CommandSource::for_owner_tests(home, record));
    let mut manager = AuthManager::from_auth_for_testing_with_home(auth, home.to_path_buf());
    let mutable = Arc::get_mut(&mut manager).unwrap();
    mutable.command_source = Some(source.clone());
    *mutable.external_auth.get_mut().unwrap() = Some(Arc::new(source.clone()));
    source
        .managers
        .lock()
        .unwrap()
        .push(Arc::downgrade(&manager));
    (source, manager)
}

#[cfg(unix)]
#[test]
fn command_manager_accepts_unchanged_owned_auth_but_refuses_cache_clear_and_other_mode() {
    let home = tempfile::tempdir().unwrap();
    let (_, manager) = selected_fixture(home.path(), record(1));
    let auth = manager.auth_cached().unwrap();
    assert!(!manager.try_set_cached_auth(Some(auth.clone())).unwrap());
    assert!(manager.try_set_cached_auth(None).is_err());
    assert!(
        manager
            .try_set_cached_auth(Some(CodexAuth::from_api_key("synthetic")))
            .is_err()
    );
    assert_eq!(
        manager.auth_cached().unwrap().get_token().unwrap(),
        auth.get_token().unwrap()
    );
    assert_eq!(
        *manager.auth_change_state_receiver().borrow(),
        AuthChangeState::default()
    );
}

#[cfg(unix)]
#[test]
fn command_manager_cannot_publish_a_disk_superseded_credential() {
    let home = tempfile::tempdir().unwrap();
    let (_, manager) = selected_fixture(home.path(), record(1));
    let old = manager.auth_cached().unwrap();
    let newer = record(2);
    command_store::commit(home.path(), &newer, &newer.auth().unwrap()).unwrap();
    assert!(manager.try_set_cached_auth(Some(old.clone())).is_err());
    assert!(manager.publish_reloaded_auth(0, Some(old.clone())).is_err());
    assert_eq!(
        manager.auth_cached().unwrap().get_token().unwrap(),
        old.get_token().unwrap()
    );
    assert_eq!(
        *manager.auth_change_state_receiver().borrow(),
        AuthChangeState::default()
    );
}

#[cfg(unix)]
#[tokio::test]
async fn command_reload_propagates_lost_ownership_instead_of_spinning_or_returning_cached_auth() {
    let home = tempfile::tempdir().unwrap();
    let (_, manager) = selected_fixture(home.path(), record(1));
    let before = manager.auth_cached().unwrap().get_token().unwrap();
    std::fs::remove_file(home.path().join("auth.json")).unwrap();
    std::fs::remove_file(home.path().join(".external-auth-owner.json")).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), manager.reload_inner())
            .await
            .unwrap()
            .is_err()
    );
    assert!(
        tokio::time::timeout(
            Duration::from_secs(2),
            manager.reload_if_account_id_matches(Some("account"))
        )
        .await
        .unwrap()
        .is_err()
    );
    assert_eq!(manager.auth_cached().unwrap().get_token().unwrap(), before);
    assert_eq!(
        *manager.auth_change_state_receiver().borrow(),
        AuthChangeState::default()
    );
    assert!(!home.path().join("auth.json").exists());
}

#[cfg(unix)]
#[tokio::test]
async fn command_reload_propagates_poisoned_source_without_clearing_cached_owner() {
    let home = tempfile::tempdir().unwrap();
    let (source, manager) = selected_fixture(home.path(), record(1));
    let before = manager.auth_cached().unwrap().get_token().unwrap();
    assert!(
        std::thread::spawn(move || {
            let _guard = source.state.lock().unwrap();
            panic!("synthetic selected source poison");
        })
        .join()
        .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(2), manager.reload_inner())
            .await
            .unwrap()
            .is_err()
    );
    assert_eq!(manager.auth_cached().unwrap().get_token().unwrap(), before);
    assert_eq!(
        *manager.auth_change_state_receiver().borrow(),
        AuthChangeState::default()
    );
}

#[cfg(unix)]
#[test]
fn command_publication_checks_narrowed_mode_before_the_native_domain() {
    let home = tempfile::tempdir().unwrap();
    let (_, manager) = selected_fixture(home.path(), record(1));
    let auth = manager.auth_cached().unwrap();
    manager.restrict_auth_modes(vec![AuthMode::ApiKey]).unwrap();
    let lock = home.path().join("auth.json.lock");
    std::fs::remove_file(&lock).unwrap();
    std::fs::create_dir(&lock).unwrap();
    assert_eq!(
        manager.try_set_cached_auth(Some(auth)).unwrap_err().kind(),
        std::io::ErrorKind::PermissionDenied
    );
    assert_eq!(
        *manager.auth_change_state_receiver().borrow(),
        AuthChangeState::default()
    );
}

#[cfg(unix)]
#[test]
fn command_publication_revalidates_workspace_policy_under_the_cache_lock() {
    let home = tempfile::tempdir().unwrap();
    let (_, manager) = selected_fixture(home.path(), record(1));
    let auth = manager.auth_cached().unwrap();
    manager.set_forced_chatgpt_workspace_id(Some(vec!["foreign".into()]));
    let after_policy = *manager.auth_change_state_receiver().borrow();
    assert!(manager.try_set_cached_auth(Some(auth.clone())).is_err());
    assert!(
        manager
            .publish_reloaded_auth(after_policy.generation, Some(auth.clone()))
            .is_err()
    );
    assert_eq!(*manager.auth_change_state_receiver().borrow(), after_policy);
    assert_eq!(
        manager.auth_cached().unwrap().get_token().unwrap(),
        auth.get_token().unwrap()
    );
}

#[cfg(unix)]
#[test]
fn command_cache_poison_is_an_error_and_releases_source_and_disk_guards() {
    use super::super::storage::acquire_command_auth_source;
    let home = tempfile::tempdir().unwrap();
    let (source, manager) = selected_fixture(home.path(), record(1));
    let auth = manager.auth_cached().unwrap();
    let poison = manager.clone();
    assert!(
        std::thread::spawn(move || {
            let _guard = poison.inner.write().unwrap();
            panic!("synthetic selected cache poison");
        })
        .join()
        .is_err()
    );
    assert!(manager.try_set_cached_auth(Some(auth.clone())).is_err());
    assert!(manager.publish_reloaded_auth(0, Some(auth)).is_err());
    assert!(source.state.try_lock().is_ok());
    assert!(acquire_command_auth_source(home.path()).is_ok());
}

#[test]
fn ordinary_cache_poison_keeps_the_existing_reload_none_and_bool_false_contract() {
    let manager = AuthManager::from_optional_auth_for_testing(None);
    let poison = manager.clone();
    assert!(
        std::thread::spawn(move || {
            let _guard = poison.inner.write().unwrap();
            panic!("synthetic native cache poison");
        })
        .join()
        .is_err()
    );
    assert!(manager.publish_reloaded_auth(0, None).unwrap().is_none());
    assert!(!manager.set_cached_auth(None));
}

#[cfg(unix)]
#[tokio::test]
async fn command_factory_snapshot_preserves_actual_owned_credential() {
    let home = tempfile::tempdir().unwrap();
    let (_, manager) = selected_fixture(home.path(), record(1));
    let expected = manager.auth_cached().unwrap().get_token().unwrap();
    let (auth, _factory) = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        manager.auth_with_http_client_factory(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(auth.get_token().unwrap(), expected);
}

#[cfg(unix)]
#[test]
fn command_factory_snapshot_refuses_durable_supersession_after_resolution() {
    let home = tempfile::tempdir().unwrap();
    let (_, manager) = selected_fixture(home.path(), record(1));
    let resolved = manager.auth_cached().unwrap();
    let newer = record(2);
    command_store::commit(home.path(), &newer, &newer.auth().unwrap()).unwrap();
    assert!(manager.command_auth_with_factory(resolved).is_none());
}

#[cfg(unix)]
#[test]
fn command_factory_snapshot_refuses_policy_change_after_resolution() {
    let home = tempfile::tempdir().unwrap();
    let (_, manager) = selected_fixture(home.path(), record(1));
    let resolved = manager.auth_cached().unwrap();
    *manager.allowed_auth_modes.write().unwrap() = Some(vec![AuthMode::ApiKey]);
    assert!(manager.command_auth_with_factory(resolved).is_none());
}

#[cfg(unix)]
#[test]
fn command_factory_snapshot_refuses_cache_uncertainty_and_releases_source() {
    let home = tempfile::tempdir().unwrap();
    let (source, manager) = selected_fixture(home.path(), record(1));
    let resolved = manager.auth_cached().unwrap();
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = manager.inner.write().unwrap();
        panic!("synthetic cache poison");
    }));
    assert!(manager.command_auth_with_factory(resolved).is_none());
    assert!(source.state.try_lock().is_ok());
}
