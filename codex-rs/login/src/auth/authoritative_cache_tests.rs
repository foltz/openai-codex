use super::*;
use pretty_assertions::assert_eq;

#[test]
fn authoritative_cached_auth_distinguishes_initial_failure_from_logged_out() {
    let manager =
        AuthManager::from_auth_for_testing(CodexAuth::create_dummy_chatgpt_auth_for_testing());
    assert!(manager.authoritative_auth_cached().is_ok());
    manager
        .inner
        .write()
        .expect("test cache lock")
        .initial_load_failed = true;
    assert_eq!(
        manager.authoritative_auth_cached(),
        Err(AuthoritativeAuthUnavailable::InitialLoadFailed)
    );
}

#[test]
fn authoritative_cached_auth_reports_cache_lock_unavailable_when_poisoned() {
    let manager =
        AuthManager::from_auth_for_testing(CodexAuth::create_dummy_chatgpt_auth_for_testing());
    assert!(manager.authoritative_auth_cached().is_ok());

    let poisoning_manager = Arc::clone(&manager);
    let poisoned = std::thread::spawn(move || {
        let _guard = poisoning_manager.inner.write().expect("test cache lock");
        panic!("deliberately poison the cache lock for this test");
    })
    .join();
    assert!(poisoned.is_err(), "the spawned thread must have panicked");

    assert_eq!(
        manager.authoritative_auth_cached(),
        Err(AuthoritativeAuthUnavailable::CacheLockUnavailable),
        "a poisoned cache lock must fail closed as unavailable, never as logged-out"
    );
}

#[test]
fn managed_fingerprint_preserves_unavailable_and_logged_out_states() {
    let manager = AuthManager::from_optional_auth_for_testing(None);
    assert_eq!(manager.authoritative_managed_auth_fingerprint(), Ok(None));
    manager.inner.write().unwrap().initial_load_failed = true;
    assert_eq!(
        manager.authoritative_managed_auth_fingerprint(),
        Err(AuthoritativeAuthUnavailable::InitialLoadFailed)
    );
}

#[tokio::test]
#[serial_test::serial(codex_auth_env)]
async fn actual_startup_failure_is_not_confirmed_cached_absence() -> anyhow::Result<()> {
    let _access_token = remove_access_token_env_var();
    let absent_home = tempdir()?;
    let absent = AuthManager::new_from_auth_config(
        build_config(absent_home.path(), None, None).await,
        false,
        None,
    )
    .await;
    assert_eq!(
        absent
            .authoritative_auth_cached()
            .expect("available test authority"),
        None
    );
    assert_eq!(
        absent
            .authoritative_managed_auth_fingerprint()
            .expect("available test authority"),
        None
    );
    assert_eq!(
        absent
            .managed_transition_eligible()
            .expect("available test authority"),
        false
    );
    for directory in [false, true] {
        let home = tempdir()?;
        if directory {
            std::fs::create_dir(get_auth_file(home.path()))?;
        } else {
            std::fs::write(get_auth_file(home.path()), "invalid-json-sentinel")?;
        }
        let manager = AuthManager::new_from_auth_config(
            build_config(home.path(), None, None).await,
            false,
            None,
        )
        .await;
        assert_eq!(
            manager.auth_cached(),
            None,
            "compatibility getter is unchanged"
        );
        assert_eq!(
            manager.authoritative_auth_cached(),
            Err(AuthoritativeAuthUnavailable::InitialLoadFailed)
        );
        assert_eq!(
            manager.authoritative_managed_auth_fingerprint(),
            Err(AuthoritativeAuthUnavailable::InitialLoadFailed)
        );
        assert_eq!(
            manager.managed_transition_eligible(),
            Err(AuthoritativeAuthUnavailable::InitialLoadFailed)
        );
        if !directory {
            assert_eq!(
                std::fs::read_to_string(get_auth_file(home.path()))?,
                "invalid-json-sentinel"
            );
        }
    }
    Ok(())
}

#[tokio::test]
#[serial_test::serial(codex_auth_env)]
async fn startup_failure_stays_sticky_across_reload_but_not_manager_reconstruction()
-> anyhow::Result<()> {
    let _access_token = remove_access_token_env_var();
    let home = tempdir()?;
    std::fs::write(get_auth_file(home.path()), "invalid-json-sentinel")?;
    let manager =
        AuthManager::new_from_auth_config(build_config(home.path(), None, None).await, false, None)
            .await;
    assert_eq!(
        manager.authoritative_auth_cached(),
        Err(AuthoritativeAuthUnavailable::InitialLoadFailed)
    );
    let repaired = serde_json::from_value(json!({"OPENAI_API_KEY":"repaired-synthetic"}))?;
    FileAuthStorage::new(home.path().to_path_buf()).save(&repaired)?;
    assert!(
        manager.reload().await,
        "normal successful reload still publishes its changed value"
    );
    assert_eq!(
        manager.auth_cached(),
        Some(CodexAuth::from_api_key("repaired-synthetic"))
    );
    assert_eq!(
        manager.authoritative_auth_cached(),
        Err(AuthoritativeAuthUnavailable::InitialLoadFailed),
        "ordinary reload cannot mint a verified bootstrap baseline"
    );
    let restarted =
        AuthManager::new_from_auth_config(build_config(home.path(), None, None).await, false, None)
            .await;
    assert_eq!(
        restarted
            .authoritative_auth_cached()
            .expect("available test authority"),
        manager.auth_cached()
    );
    assert_eq!(
        restarted
            .managed_transition_eligible()
            .expect("available test authority"),
        false,
        "API keys are not managed ChatGPT authority"
    );
    Ok(())
}

#[test]
fn managed_eligibility_is_separate_from_cached_fingerprint() {
    let absent = AuthManager::from_optional_auth_for_testing(None);
    let api = AuthManager::from_auth_for_testing(CodexAuth::from_api_key("synthetic-key"));
    let headers =
        AuthManager::from_auth_for_testing(external_header_auth(Some(WORKSPACE_ID_ALLOWED)));
    let chatgpt =
        AuthManager::from_auth_for_testing(CodexAuth::create_dummy_chatgpt_auth_for_testing());
    assert_eq!(absent.managed_transition_eligible(), Ok(false));
    assert_eq!(api.managed_transition_eligible(), Ok(false));
    assert_eq!(headers.managed_transition_eligible(), Ok(false));
    assert!(
        headers
            .authoritative_managed_auth_fingerprint()
            .unwrap()
            .is_some()
    );
    assert_eq!(chatgpt.managed_transition_eligible(), Ok(true));
}

#[test]
fn external_authority_cannot_borrow_cached_chatgpt_eligibility() {
    let auth = CodexAuth::create_dummy_chatgpt_auth_for_testing();
    let manager = AuthManager::from_auth_for_testing(auth.clone());
    assert_eq!(manager.managed_transition_eligible(), Ok(true));
    *manager.external_auth.write().unwrap() = Some(Arc::new(StaticExternalAuth(auth)));
    assert_eq!(manager.managed_transition_eligible(), Ok(false));
    assert!(
        manager
            .authoritative_managed_auth_fingerprint()
            .unwrap()
            .is_some()
    );
}

#[test]
fn authoritative_queries_do_not_reload_source_or_publish_revisions() -> anyhow::Result<()> {
    let home = tempdir()?;
    let auth = CodexAuth::create_dummy_chatgpt_auth_for_testing();
    let expected =
        AuthManager::managed_account_fingerprint(auth.get_account_id().as_deref().unwrap());
    let manager =
        AuthManager::from_auth_for_testing_with_home(auth.clone(), home.path().to_path_buf());
    let before = *manager.auth_change_state_tx.borrow();
    let revision = *manager.auth_change_tx.borrow();
    std::fs::write(get_auth_file(home.path()), "invalid-json-sentinel")?;
    assert_eq!(
        manager
            .authoritative_auth_cached()
            .expect("available test authority"),
        Some(auth)
    );
    assert_eq!(
        manager
            .authoritative_managed_auth_fingerprint()
            .expect("available test authority"),
        Some(expected)
    );
    assert_eq!(
        manager
            .managed_transition_eligible()
            .expect("available test authority"),
        true
    );
    assert_eq!(*manager.auth_change_state_tx.borrow(), before);
    assert_eq!(*manager.auth_change_tx.borrow(), revision);
    assert_eq!(
        std::fs::read_to_string(get_auth_file(home.path()))?,
        "invalid-json-sentinel"
    );
    Ok(())
}

#[test]
fn managed_fingerprint_preserves_the_retained_domain_bytes() {
    let fingerprint = AuthManager::managed_account_fingerprint("account_id");
    assert_eq!(
        fingerprint,
        "7a33dc722b149b811189188aa30234c7e56e573337f7800dee380dc3359a4d4a"
    );
    assert_eq!(fingerprint.len(), 64);
    assert!(fingerprint.chars().all(|c| c.is_ascii_hexdigit()));
    assert_ne!(
        fingerprint,
        AuthManager::managed_account_fingerprint("other-account")
    );
}

#[test]
fn fingerprint_and_eligibility_refuse_a_poisoned_cache() {
    let manager =
        AuthManager::from_auth_for_testing(CodexAuth::create_dummy_chatgpt_auth_for_testing());
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = manager.inner.write().unwrap();
            panic!("controlled authority-cache poison");
        }))
        .is_err()
    );
    assert_eq!(
        manager.authoritative_managed_auth_fingerprint(),
        Err(AuthoritativeAuthUnavailable::CacheLockUnavailable)
    );
    assert_eq!(
        manager.managed_transition_eligible(),
        Err(AuthoritativeAuthUnavailable::CacheLockUnavailable)
    );
    assert_eq!(manager.auth_cached(), None);
}

#[test]
fn managed_eligibility_refuses_a_poisoned_external_slot() {
    let manager =
        AuthManager::from_auth_for_testing(CodexAuth::create_dummy_chatgpt_auth_for_testing());
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = manager.external_auth.write().unwrap();
            panic!("controlled external-authority poison");
        }))
        .is_err()
    );
    assert_eq!(
        manager.managed_transition_eligible(),
        Err(AuthoritativeAuthUnavailable::CacheLockUnavailable)
    );
    assert!(
        manager.authoritative_auth_cached().unwrap().is_some(),
        "a poisoned external slot is not a cache read failure"
    );
}
