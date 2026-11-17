use super::*;
use pretty_assertions::assert_eq;

#[test]
fn cache_observation_distinguishes_known_logout_failure_and_poison() {
    let manager = AuthManager::from_optional_auth_for_testing(None);
    assert_eq!(
        manager.auth_observation(),
        json!({
            "version": 1, "mode": null, "accountId": null, "principal": null,
            "provider": null, "stateKnown": true,
        })
    );
    manager.inner.write().unwrap().initial_load_failed = true;
    assert_eq!(manager.auth_observation()["stateKnown"], false);
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _lock = manager.inner.write().unwrap();
            panic!("controlled observation cache poison");
        }))
        .is_err()
    );
    let unknown = manager.auth_observation();
    assert_eq!(unknown["stateKnown"], false);
    assert_eq!(unknown["mode"], serde_json::Value::Null);
}

#[test]
fn cache_observation_is_token_free_and_does_not_reload_or_publish() -> anyhow::Result<()> {
    let home = tempdir()?;
    let auth = CodexAuth::from_api_key("synthetic-secret-never-observed");
    let manager = AuthManager::from_auth_for_testing_with_home(auth, home.path().to_path_buf());
    let before = *manager.auth_change_state_tx.borrow();
    let revision = *manager.auth_change_tx.borrow();
    std::fs::write(
        get_auth_file(home.path()),
        "invalid-json-not-a-cache-source",
    )?;
    let observation = manager.auth_observation();
    assert_eq!(observation["mode"], "apikey");
    assert_eq!(observation["stateKnown"], true);
    assert!(!observation.to_string().contains("synthetic-secret"));
    assert_eq!(*manager.auth_change_state_tx.borrow(), before);
    assert_eq!(*manager.auth_change_tx.borrow(), revision);
    assert_eq!(
        std::fs::read_to_string(get_auth_file(home.path()))?,
        "invalid-json-not-a-cache-source"
    );
    Ok(())
}

#[test]
fn cache_observation_preserves_cached_chatgpt_identity_with_unknown_marker() {
    let auth = CodexAuth::create_dummy_chatgpt_auth_for_testing();
    let account = auth.get_account_id();
    let principal = auth.get_chatgpt_user_id();
    let manager = AuthManager::from_auth_for_testing(auth);
    let observation = manager.auth_observation();
    assert_eq!(observation["accountId"], json!(account));
    assert_eq!(observation["principal"], json!(principal));
    assert_eq!(observation["mode"], "chatgpt");
    assert_eq!(observation["stateKnown"], true);
    manager.inner.write().unwrap().initial_load_failed = true;
    let uncertain = manager.auth_observation();
    assert_eq!(uncertain["stateKnown"], false);
    assert_eq!(uncertain["accountId"], observation["accountId"]);
}
