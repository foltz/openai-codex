use super::*;
use base64::Engine;

fn chatgpt(user: &str, workspace: &str) -> CodexAuth {
    let claims = serde_json::json!({"https://api.openai.com/auth": {"chatgpt_user_id": user}});
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(claims.to_string());
    CodexAuth::from_external_chatgpt_tokens(&format!("header.{payload}.signature"), workspace, None)
        .unwrap()
}

#[test]
fn fixed_principal_requires_both_nonblank_credential_identifiers() {
    for (user, workspace) in [
        ("", "workspace"),
        (" ", "workspace"),
        ("user", ""),
        ("user", " "),
    ] {
        assert_eq!(FixedPrincipal::carried_by(&chatgpt(user, workspace)), None);
    }
    assert_eq!(
        FixedPrincipal::carried_by(&chatgpt("user", "workspace")),
        Some(FixedPrincipal {
            chatgpt_user_id: "user".into(),
            account_id: "workspace".into(),
        })
    );
}

#[test]
fn api_key_and_identity_free_external_token_do_not_prove_a_fixed_principal() {
    assert_eq!(
        FixedPrincipal::carried_by(&CodexAuth::from_api_key("synthetic-key")),
        None
    );
    let opaque =
        CodexAuth::from_external_chatgpt_tokens("e30.e30.signature", "workspace", None).unwrap();
    assert_eq!(FixedPrincipal::carried_by(&opaque), None);
}

#[test]
fn existing_external_provider_defaults_to_no_fixed_principal_authority() {
    let provider = FixedApiKeyAuth(CodexAuth::from_api_key("synthetic-key"));
    assert_eq!(provider.fixed_principal(), None);
}

#[test]
fn fallible_cache_publication_distinguishes_changed_and_unchanged_without_revision_drift() {
    let manager = AuthManager::from_optional_auth_for_testing(None);
    assert!(!manager.try_set_cached_auth(None).unwrap());
    assert_eq!(
        *manager.auth_change_state_receiver().borrow(),
        AuthChangeState::default()
    );
    assert!(
        manager
            .try_set_cached_auth(Some(CodexAuth::from_api_key("synthetic-key")))
            .unwrap()
    );
    let after = *manager.auth_change_state_receiver().borrow();
    assert_eq!(
        after,
        AuthChangeState {
            generation: 1,
            owner_generation: 1,
            source_generation: 1,
            login_generation: 1,
        }
    );
    assert!(
        !manager
            .try_set_cached_auth(Some(CodexAuth::from_api_key("synthetic-key")))
            .unwrap()
    );
    assert_eq!(*manager.auth_change_state_receiver().borrow(), after);
}

#[test]
fn fallible_cache_publication_reports_poison_while_native_bool_contract_is_preserved() {
    let manager = AuthManager::from_optional_auth_for_testing(None);
    let poison = manager.clone();
    assert!(
        std::thread::spawn(move || {
            let _guard = poison.inner.write().unwrap();
            panic!("synthetic cache poison");
        })
        .join()
        .is_err()
    );
    assert!(
        manager
            .try_set_cached_auth(Some(CodexAuth::from_api_key("synthetic-key")))
            .is_err()
    );
    assert!(!manager.set_cached_auth(Some(CodexAuth::from_api_key("synthetic-key"))));
    assert_eq!(
        *manager.auth_change_state_receiver().borrow(),
        AuthChangeState::default()
    );
}
