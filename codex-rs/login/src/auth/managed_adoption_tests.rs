use super::*;
use pretty_assertions::assert_eq;

fn write_managed_auth_file(params: AuthFileParams, codex_home: &Path) -> std::io::Result<()> {
    let account_id = params.chatgpt_account_id.clone();
    write_auth_file(params, codex_home)?;
    let path = get_auth_file(codex_home);
    let mut value: serde_json::Value = serde_json::from_slice(&std::fs::read(&path)?)?;
    value["tokens"]["account_id"] = serde_json::json!(account_id);
    std::fs::write(path, serde_json::to_vec(&value)?)
}

fn write_api_key_auth_file(codex_home: &Path, api_key: &str) {
    let auth_dot_json = AuthDotJson {
        auth_mode: Some(AuthMode::ApiKey),
        openai_api_key: Some(api_key.to_owned()),
        tokens: None,
        last_refresh: None,
        agent_identity: None,
        personal_access_token: None,
        bedrock_api_key: None,
        bedrock_access_keys: None,
    };
    super::save_auth(
        codex_home,
        &auth_dot_json,
        AuthCredentialsStoreMode::File,
        AuthKeyringBackendKind::default(),
    )
    .expect("write api key auth.json");
}

/// R044/R045: external auth authority (agent identity, injected tokens) is
/// excluded from managed-auth-transition adoption regardless of what it
/// would resolve to -- the production source reader must refuse before any
/// resolution could be treated as an adoptable managed source.
#[tokio::test]
#[serial_test::serial(codex_auth_env)]
async fn managed_adoption_snapshot_excludes_external_auth_authority() {
    let codex_home = tempdir().unwrap();
    let manager = AuthManager::new(
        codex_home.path().to_path_buf(),
        /*enable_codex_api_key_env*/ false,
        AuthCredentialsStoreMode::Ephemeral,
        /*forced_chatgpt_workspace_id*/ None,
        /*chatgpt_base_url*/ None,
        AuthKeyringBackendKind::default(),
        crate::test_support::transport_default_auth_route_config(),
    )
    .await;
    manager
        .set_external_auth(Arc::new(StaticExternalAuth(CodexAuth::from_api_key(
            "sk-external",
        ))))
        .await
        .expect("external auth should install");

    match manager.read_managed_adoption_snapshot().await {
        Err(error) => assert_eq!(
            error,
            ManagedAdoptionVerificationError::IneligibleAuthMode,
            "external auth authority must never be treated as an adoptable managed source"
        ),
        Ok(_) => panic!("external auth authority was treated as an adoptable managed source"),
    }
}

/// R002: a persisted, non-ChatGPT mode (API key mode here) must refuse as
/// `IneligibleAuthMode`, rather than slipping through as an adoptable managed
/// source. This fixture is rejected by the serialized auth-mode gate before a
/// `CodexAuth` is constructed, so it deliberately does not model the
/// stable-identity handling for `AgentIdentity` or `PersonalAccessToken`.
#[tokio::test]
#[serial_test::serial(codex_auth_env)]
async fn managed_adoption_snapshot_excludes_persisted_non_chatgpt_mode() {
    let codex_home = tempdir().unwrap();
    write_api_key_auth_file(codex_home.path(), "sk-persisted");
    let manager = AuthManager::new(
        codex_home.path().to_path_buf(),
        /*enable_codex_api_key_env*/ false,
        AuthCredentialsStoreMode::File,
        /*forced_chatgpt_workspace_id*/ None,
        /*chatgpt_base_url*/ None,
        AuthKeyringBackendKind::default(),
        crate::test_support::transport_default_auth_route_config(),
    )
    .await;

    match manager.read_managed_adoption_snapshot().await {
        Err(error) => assert_eq!(
            error,
            ManagedAdoptionVerificationError::IneligibleAuthMode,
            "a persisted non-ChatGPT mode must refuse as IneligibleAuthMode"
        ),
        Ok(_) => panic!("a persisted non-ChatGPT mode was treated as adoptable"),
    }
}

/// R001: deliberate managed logout is a legitimate success, not a
/// failure -- `install_managed_logout` must clear the cache, publish the
/// auth-change signal, and never touch the durable store (no file is
/// written by this call).
#[tokio::test]
#[serial_test::serial(codex_auth_env)]
async fn install_managed_logout_clears_the_cache_and_publishes_a_revision() {
    let codex_home = tempdir().unwrap();
    write_managed_auth_file(
        AuthFileParams {
            openai_api_key: None,
            chatgpt_plan_type: Some("pro".to_owned()),
            chatgpt_account_id: Some("managed-before-logout".to_owned()),
        },
        codex_home.path(),
    )
    .expect("write managed prior auth");
    let manager = AuthManager::new(
        codex_home.path().to_path_buf(),
        /*enable_codex_api_key_env*/ false,
        AuthCredentialsStoreMode::File,
        /*forced_chatgpt_workspace_id*/ None,
        /*chatgpt_base_url*/ None,
        AuthKeyringBackendKind::default(),
        crate::test_support::transport_default_auth_route_config(),
    )
    .await;
    manager.reload().await;
    let previous = manager.auth_cached();
    assert!(previous.is_some(), "precondition: an auth was cached");
    let revisions = manager.auth_change_receiver();
    let owner_before = *manager.auth_change_state_receiver().borrow();
    let precondition = manager
        .capture_managed_adoption_precondition(Some(&AuthManager::managed_account_fingerprint(
            "managed-before-logout",
        )))
        .unwrap();

    let outcome = manager.install_managed_logout(previous, &precondition);
    assert!(
        matches!(outcome, ManagedAdoptionInstallOutcome::LoggedOut),
        "expected LoggedOut, got {outcome:?}"
    );
    assert_eq!(manager.auth_cached(), None);
    assert!(manager.is_managed_auth_change(*manager.auth_change_receiver().borrow()));
    assert_eq!(
        *manager.auth_change_state_receiver().borrow(),
        AuthChangeState {
            generation: owner_before.generation + 1,
            owner_generation: owner_before.owner_generation + 1,
            source_generation: owner_before.source_generation + 1,
            login_generation: owner_before.login_generation + 1,
        }
    );
    assert!(
        revisions.has_changed().unwrap_or(false),
        "logout must publish an auth-change signal like any other adoption"
    );
    assert!(
        get_auth_file(codex_home.path()).exists(),
        "managed logout must never touch the durable store -- the writer boundary is separately owned"
    );
}

/// R014's manager-owned CAS: installing must refuse rather than
/// overwrite when the cache no longer holds the snapshot observed
/// immediately before the read that produced the value being installed
/// -- an ordinary non-managed login/logout landing concurrently during
/// adoption must not be silently clobbered.
#[tokio::test]
#[serial_test::serial(codex_auth_env)]
async fn install_managed_adoption_refuses_when_the_cache_changed_concurrently() {
    let codex_home = tempdir().unwrap();
    write_managed_auth_file(
        AuthFileParams {
            openai_api_key: None,
            chatgpt_plan_type: Some("pro".to_owned()),
            chatgpt_account_id: Some("admission-account".to_owned()),
        },
        codex_home.path(),
    )
    .unwrap();
    let manager = AuthManager::new(
        codex_home.path().to_path_buf(),
        /*enable_codex_api_key_env*/ false,
        AuthCredentialsStoreMode::File,
        /*forced_chatgpt_workspace_id*/ None,
        /*chatgpt_base_url*/ None,
        AuthKeyringBackendKind::default(),
        crate::test_support::transport_default_auth_route_config(),
    )
    .await;
    let observed_previous = manager.auth_cached();
    assert!(matches!(observed_previous, Some(CodexAuth::Chatgpt(_))));
    let precondition = manager
        .capture_managed_adoption_precondition(Some(&AuthManager::managed_account_fingerprint(
            "admission-account",
        )))
        .unwrap();

    // Something else installs concurrently while the (simulated) read was
    // in flight.
    manager
        .set_external_auth(Arc::new(StaticExternalAuth(CodexAuth::from_api_key(
            "sk-concurrent-writer",
        ))))
        .await
        .expect("concurrent external auth should install");

    let new_auth = CodexAuth::from_api_key("sk-intended-adoption");
    let outcome = manager.install_managed_adoption(
        new_auth,
        observed_previous,
        "intended-account",
        &precondition,
    );
    assert!(
        matches!(
            outcome,
            ManagedAdoptionInstallOutcome::CacheChangedConcurrently
        ),
        "expected CacheChangedConcurrently, got {outcome:?}"
    );
    assert_eq!(
        manager.auth_cached().as_ref().and_then(CodexAuth::api_key),
        Some("sk-concurrent-writer"),
        "a refused install must never overwrite the concurrent value"
    );
}

#[tokio::test]
#[serial_test::serial(codex_auth_env)]
async fn managed_admission_and_logout_refuse_absent_or_api_key_current_auth() {
    let codex_home = tempdir().unwrap();
    let manager = AuthManager::new(
        codex_home.path().to_path_buf(),
        /*enable_codex_api_key_env*/ false,
        AuthCredentialsStoreMode::File,
        /*forced_chatgpt_workspace_id*/ None,
        /*chatgpt_base_url*/ None,
        AuthKeyringBackendKind::default(),
        crate::test_support::transport_default_auth_route_config(),
    )
    .await;
    for auth in [None, Some(CodexAuth::from_api_key("excluded-current-key"))] {
        manager.set_cached_auth(auth.clone());
        let revision = *manager.auth_change_receiver().borrow();
        assert_eq!(manager.managed_transition_eligible(), Ok(false));
        assert!(manager.capture_managed_adoption_precondition(None).is_err());
        assert_eq!(manager.auth_cached(), auth);
        assert_eq!(*manager.auth_change_receiver().borrow(), revision);
    }
}

#[tokio::test]
#[serial_test::serial(codex_auth_env)]
async fn managed_adoption_preserves_upstream_credential_and_owner_generations() {
    for (next_account, owner_delta) in [("owner-a", 0), ("owner-b", 1)] {
        let home = tempdir().unwrap();
        let write_account = |account: &str, plan: &str| {
            write_managed_auth_file(
                AuthFileParams {
                    openai_api_key: None,
                    chatgpt_plan_type: Some(plan.to_owned()),
                    chatgpt_account_id: Some(account.to_owned()),
                },
                home.path(),
            )
            .unwrap();
        };
        write_account("owner-a", "pro");
        let manager = AuthManager::new(
            home.path().to_path_buf(),
            /*enable_codex_api_key_env*/ false,
            AuthCredentialsStoreMode::File,
            /*forced_chatgpt_workspace_id*/ None,
            /*chatgpt_base_url*/ None,
            AuthKeyringBackendKind::default(),
            crate::test_support::transport_default_auth_route_config(),
        )
        .await;
        let before = *manager.auth_change_state_receiver().borrow();
        let precondition = manager
            .capture_managed_adoption_precondition(Some(&AuthManager::managed_account_fingerprint(
                "owner-a",
            )))
            .unwrap();
        // Changing the ID token forces a credential change even for the same owner.
        write_account(next_account, "plus");
        let prepared = manager
            .prepare_managed_adoption(
                Some(&AuthManager::managed_account_fingerprint(next_account)),
                &precondition,
            )
            .await
            .unwrap();
        assert!(matches!(
            manager.install_prepared_managed_adoption(&prepared, &precondition),
            ManagedAdoptionInstallOutcome::Installed { .. }
        ));
        let after = *manager.auth_change_state_receiver().borrow();
        let managed_revision = *manager.auth_change_receiver().borrow();
        assert!(manager.is_managed_auth_change(managed_revision));
        assert!(!manager.is_managed_auth_change(0));
        assert_eq!(
            after,
            AuthChangeState {
                generation: before.generation + 1,
                owner_generation: before.owner_generation + owner_delta,
                source_generation: before.source_generation + owner_delta,
                login_generation: before.login_generation + owner_delta,
            }
        );
        // A refused reuse must not publish another owner or credential change.
        assert!(matches!(
            manager.install_prepared_managed_adoption(&prepared, &precondition),
            ManagedAdoptionInstallOutcome::IntendedResultMismatch
        ));
        assert_eq!(*manager.auth_change_state_receiver().borrow(), after);
        manager.set_cached_auth(None);
        let ordinary_revision = *manager.auth_change_receiver().borrow();
        assert!(ordinary_revision > managed_revision);
        assert!(!manager.is_managed_auth_change(ordinary_revision));
    }
}

#[tokio::test]
#[serial_test::serial(codex_auth_env)]
async fn prepared_managed_candidate_refuses_changed_source_and_installs_only_once() {
    let home = tempdir().unwrap();
    let write_account = |account: &str| {
        write_managed_auth_file(
            AuthFileParams {
                openai_api_key: None,
                chatgpt_plan_type: Some("pro".to_owned()),
                chatgpt_account_id: Some(account.to_owned()),
            },
            home.path(),
        )
        .unwrap()
    };
    write_account("prepared-a");
    let manager = AuthManager::new(
        home.path().to_path_buf(),
        false,
        AuthCredentialsStoreMode::File,
        None,
        None,
        AuthKeyringBackendKind::default(),
        crate::test_support::transport_default_auth_route_config(),
    )
    .await;
    let precondition = manager
        .capture_managed_adoption_precondition(Some(&AuthManager::managed_account_fingerprint(
            "prepared-a",
        )))
        .unwrap();
    write_account("prepared-b");
    let original_b = std::fs::read(get_auth_file(home.path())).unwrap();
    let prepared = manager
        .prepare_managed_adoption(
            Some(&AuthManager::managed_account_fingerprint("prepared-b")),
            &precondition,
        )
        .await
        .unwrap();
    let revision = *manager.auth_change_receiver().borrow();
    write_account("unexpected-c");
    assert!(matches!(
        manager.install_prepared_managed_adoption(&prepared, &precondition),
        ManagedAdoptionInstallOutcome::SourceChanged
    ));
    assert_eq!(
        manager.auth_cached().unwrap().get_account_id().as_deref(),
        Some("prepared-a")
    );
    assert_eq!(*manager.auth_change_receiver().borrow(), revision);
    std::fs::write(get_auth_file(home.path()), &original_b).unwrap();
    assert!(matches!(
        manager.install_prepared_managed_adoption(&prepared, &precondition),
        ManagedAdoptionInstallOutcome::IntendedResultMismatch
    ));
    let malformed_candidate = manager
        .prepare_managed_adoption(
            Some(&AuthManager::managed_account_fingerprint("prepared-b")),
            &precondition,
        )
        .await
        .unwrap();
    // A malformed replacement is rejected by raw preimage comparison, not
    // parsed as a new candidate or collapsed into authoritative absence.
    std::fs::write(get_auth_file(home.path()), b"not auth JSON").unwrap();
    assert!(matches!(
        manager.install_prepared_managed_adoption(&malformed_candidate, &precondition),
        ManagedAdoptionInstallOutcome::SourceChanged
    ));
    std::fs::write(get_auth_file(home.path()), original_b).unwrap();
    assert!(matches!(
        manager.install_prepared_managed_adoption(&malformed_candidate, &precondition),
        ManagedAdoptionInstallOutcome::IntendedResultMismatch
    ));
    let prepared = manager
        .prepare_managed_adoption(
            Some(&AuthManager::managed_account_fingerprint("prepared-b")),
            &precondition,
        )
        .await
        .unwrap();
    assert!(matches!(
        manager.install_prepared_managed_adoption(&prepared, &precondition),
        ManagedAdoptionInstallOutcome::Installed { .. }
    ));
    assert_eq!(
        manager.auth_cached().unwrap().get_account_id().as_deref(),
        Some("prepared-b")
    );
    let installed_revision = *manager.auth_change_receiver().borrow();
    assert!(matches!(
        manager.install_prepared_managed_adoption(&prepared, &precondition),
        ManagedAdoptionInstallOutcome::IntendedResultMismatch
    ));
    assert_eq!(*manager.auth_change_receiver().borrow(), installed_revision);
}

#[tokio::test]
#[serial_test::serial(codex_auth_env)]
async fn managed_recovery_verifies_durable_and_cached_result_without_installing() {
    let codex_home = tempdir().unwrap();
    write_managed_auth_file(
        AuthFileParams {
            openai_api_key: None,
            chatgpt_plan_type: Some("pro".to_owned()),
            chatgpt_account_id: Some("recovery-account".to_owned()),
        },
        codex_home.path(),
    )
    .unwrap();
    let manager = AuthManager::new(
        codex_home.path().to_path_buf(),
        /*enable_codex_api_key_env*/ false,
        AuthCredentialsStoreMode::File,
        /*forced_chatgpt_workspace_id*/ None,
        /*chatgpt_base_url*/ None,
        AuthKeyringBackendKind::default(),
        crate::test_support::transport_default_auth_route_config(),
    )
    .await;
    let fingerprint = AuthManager::managed_account_fingerprint("recovery-account");
    let revision = *manager.auth_change_receiver().borrow();
    match manager
        .prepare_managed_terminal_commit(Some(&fingerprint))
        .await
    {
        Ok(_) => {}
        Err(error) => panic!("matching durable and cached result must verify: {error:?}"),
    }
    match manager
        .prepare_managed_terminal_commit(Some("wrong-result"))
        .await
    {
        Err(error) => assert_eq!(
            error,
            ManagedAdoptionVerificationError::IntendedResultMismatch,
            "a different intended result must be rejected"
        ),
        Ok(_) => panic!("a different intended result was accepted"),
    }
    assert_eq!(*manager.auth_change_receiver().borrow(), revision);

    // Durable absence alone is insufficient while the cache still holds A.
    std::fs::remove_file(get_auth_file(codex_home.path())).unwrap();
    match manager.prepare_managed_terminal_commit(None).await {
        Err(error) => assert_eq!(
            error,
            ManagedAdoptionVerificationError::CacheChangedConcurrently,
            "durable absence must not override a stale cached account"
        ),
        Ok(_) => {
            panic!("durable absence was treated as a verified logout while cache held an account")
        }
    }
    assert_eq!(*manager.auth_change_receiver().borrow(), revision);
    let precondition = manager
        .capture_managed_adoption_precondition(Some(&fingerprint))
        .unwrap();
    assert!(matches!(
        manager.install_managed_logout(manager.auth_cached(), &precondition),
        ManagedAdoptionInstallOutcome::LoggedOut
    ));
    let logout_revision = *manager.auth_change_receiver().borrow();
    match manager.prepare_managed_terminal_commit(None).await {
        Ok(_) => {}
        Err(error) => panic!("installed managed logout must verify: {error:?}"),
    }
    assert_eq!(*manager.auth_change_receiver().borrow(), logout_revision);
}

/// The credential-bearing install outcome must never render key/token
/// material through `{:?}`, matching the existing `CachedAuth` Debug
/// precedent in this same file.
#[tokio::test]
#[serial_test::serial(codex_auth_env)]
async fn managed_adoption_outcomes_never_debug_print_credential_material() {
    let fingerprint = AuthManager::managed_account_fingerprint("opaque-account");
    let install_outcome = ManagedAdoptionInstallOutcome::Installed {
        fingerprint: fingerprint.clone(),
    };
    let rendered = format!("{install_outcome:?}");
    assert_eq!(
        rendered,
        format!("Installed {{ fingerprint: {fingerprint:?} }}"),
        "debug output must expose only the opaque account fingerprint"
    );
    assert!(
        !rendered.contains("opaque-account"),
        "debug output must not expose the source account identifier"
    );
}

#[test]
fn prepared_handle_cannot_keep_credentials_alive_after_manager_drop() {
    let manager = AuthManager::from_auth_for_testing(CodexAuth::from_api_key("secret-sentinel"));
    let handle = PreparedManagedAdoption {
        id: 7,
        store: Arc::downgrade(&manager.prepared_managed_adoptions),
    };
    assert_eq!(Arc::strong_count(&manager.prepared_managed_adoptions), 1);
    drop(manager);
    assert!(handle.store.upgrade().is_none());
    assert!(!format!("{handle:?}").contains("secret-sentinel"));
}

#[test]
fn poisoned_prepared_store_is_terminal_and_never_rearmed() {
    let manager = AuthManager::from_auth_for_testing(CodexAuth::from_api_key("secret-sentinel"));
    let handle = PreparedManagedAdoption {
        id: 1,
        store: Arc::downgrade(&manager.prepared_managed_adoptions),
    };
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = manager.prepared_managed_adoptions.lock().unwrap();
        panic!("intentional prepared-store poison");
    }));
    let precondition = ManagedAdoptionPrecondition {
        revision: 0,
        generation: 0,
        fingerprint: String::new(),
    };
    for _ in 0..2 {
        assert!(matches!(
            manager.install_prepared_managed_adoption(&handle, &precondition),
            ManagedAdoptionInstallOutcome::CacheLockUnavailable
        ));
    }
    drop(handle);
    assert!(manager.prepared_managed_adoptions.is_poisoned());
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

#[test]
fn managed_projection_holds_authority_through_the_callback() {
    let manager =
        AuthManager::from_auth_for_testing(CodexAuth::create_dummy_chatgpt_auth_for_testing());
    let fingerprint = AuthManager::managed_account_fingerprint("account_id");
    assert_eq!(
        manager.authoritative_managed_auth_fingerprint(),
        Ok(Some(fingerprint.clone()))
    );
    let projection = manager.with_managed_account_projection(Some(&fingerprint), |mode, plan| {
        assert!(manager.inner.try_write().is_err());
        assert!(manager.external_auth.try_write().is_err());
        (mode, plan)
    });
    assert_eq!(
        projection,
        Ok((Some(AuthMode::Chatgpt), Some(AccountPlanType::Unknown)))
    );
    assert!(manager.inner.try_write().is_ok());
    assert!(manager.external_auth.try_write().is_ok());
}

#[test]
fn managed_projection_refuses_mismatch_without_running_the_callback() {
    let manager =
        AuthManager::from_auth_for_testing(CodexAuth::create_dummy_chatgpt_auth_for_testing());
    assert_eq!(
        manager.with_managed_account_projection(Some("different-account"), |_, _| {
            panic!("a mismatched account must not publish")
        }),
        Err::<(), _>(ManagedAdoptionVerificationError::IntendedResultMismatch)
    );
}

#[test]
fn managed_projection_distinguishes_logout_from_unavailable() {
    let manager = AuthManager::from_optional_auth_for_testing(None);
    assert_eq!(
        manager.with_managed_account_projection(None, |mode, plan| (mode, plan)),
        Ok((None, None))
    );
    manager.inner.write().unwrap().initial_load_failed = true;
    assert_eq!(
        manager.with_managed_account_projection(None, |_, _| {
            panic!("unavailable auth must not publish logout")
        }),
        Err::<(), _>(ManagedAdoptionVerificationError::CacheUnavailable)
    );
}

fn write_account(home: &Path, account: &str, plan: &str) {
    write_managed_auth_file(
        AuthFileParams {
            openai_api_key: None,
            chatgpt_plan_type: Some(plan.to_owned()),
            chatgpt_account_id: Some(account.to_owned()),
        },
        home,
    )
    .unwrap();
}

async fn manager_with_account(home: &Path, account: &str) -> AuthManager {
    write_account(home, account, "pro");
    AuthManager::new_from_auth_config(build_config(home, None, None).await, false, None).await
}

fn admission(manager: &AuthManager, account: &str) -> ManagedAdoptionPrecondition {
    manager
        .capture_managed_adoption_precondition(Some(&AuthManager::managed_account_fingerprint(
            account,
        )))
        .unwrap()
}

#[tokio::test]
#[serial_test::serial(codex_auth_env)]
async fn managed_tokens_reject_policy_generation_change_without_auth_revision_change() {
    let home = tempdir().unwrap();
    let manager = manager_with_account(home.path(), "before").await;
    let token = admission(&manager, "before");
    write_account(home.path(), "after", "plus");
    let candidate = manager
        .prepare_managed_adoption(
            Some(&AuthManager::managed_account_fingerprint("after")),
            &token,
        )
        .await
        .unwrap();
    let rev = *manager.auth_change_receiver().borrow();
    manager.set_forced_chatgpt_workspace_id(Some(vec!["different".into()]));
    assert_eq!(*manager.auth_change_receiver().borrow(), rev);
    assert_eq!(
        manager.validate_managed_adoption_precondition(&token),
        Err(ManagedAdoptionVerificationError::CacheChangedConcurrently)
    );
    assert!(matches!(
        manager.install_prepared_managed_adoption(&candidate, &token),
        ManagedAdoptionInstallOutcome::CacheChangedConcurrently
    ));
    assert_eq!(
        manager.auth_cached().unwrap().get_account_id().as_deref(),
        Some("before")
    );
    assert!(matches!(
        manager.install_prepared_managed_adoption(&candidate, &token),
        ManagedAdoptionInstallOutcome::IntendedResultMismatch
    ));
}

#[tokio::test]
#[serial_test::serial(codex_auth_env)]
async fn managed_terminal_refuses_policy_generation_change_and_never_calls_commit() {
    let home = tempdir().unwrap();
    let manager = manager_with_account(home.path(), "account").await;
    let fp = AuthManager::managed_account_fingerprint("account");
    let token = manager
        .prepare_managed_terminal_commit(Some(&fp))
        .await
        .unwrap();
    let rev = *manager.auth_change_receiver().borrow();
    manager.set_forced_chatgpt_workspace_id(Some(vec!["different".into()]));
    assert_eq!(*manager.auth_change_receiver().borrow(), rev);
    assert_eq!(
        manager.with_managed_terminal_projection(&token, |_, _| panic!("stale policy")),
        Err::<(), _>(ManagedTerminalVerificationError::Authority(
            ManagedAdoptionVerificationError::CacheChangedConcurrently
        ))
    );
}

#[tokio::test]
#[serial_test::serial(codex_auth_env)]
async fn managed_terminal_holds_all_three_real_guards_through_commit() {
    let home = tempdir().unwrap();
    let manager = manager_with_account(home.path(), "account").await;
    let fp = AuthManager::managed_account_fingerprint("account");
    let token = manager
        .prepare_managed_terminal_commit(Some(&fp))
        .await
        .unwrap();
    let storage = create_auth_storage(
        home.path().to_path_buf(),
        AuthCredentialsStoreMode::File,
        AuthKeyringBackendKind::Direct,
    );
    let before = *manager.auth_change_state_receiver().borrow();
    let rev = *manager.auth_change_receiver().borrow();
    assert_eq!(
        manager.with_managed_terminal_projection(&token, |mode, plan| {
            assert!(manager.inner.try_write().is_err());
            assert!(manager.external_auth.try_write().is_err());
            assert!(matches!(
                storage.lock_managed_source(),
                Err(ManagedAuthStorageError::ReadFailed(
                    ManagedAuthStorageFailure::CoordinationContended
                ))
            ));
            (mode, plan)
        }),
        Ok((Some(AuthMode::Chatgpt), Some(AccountPlanType::Pro)))
    );
    assert!(storage.lock_managed_source().is_ok());
    assert!(manager.inner.try_write().is_ok());
    assert!(manager.external_auth.try_write().is_ok());
    assert_eq!(*manager.auth_change_state_receiver().borrow(), before);
    assert_eq!(*manager.auth_change_receiver().borrow(), rev);
}

#[tokio::test]
#[serial_test::serial(codex_auth_env)]
async fn terminal_raw_replacement_never_parses_or_acknowledges_a_new_source() {
    for replacement in [b"invalid-json".as_slice(), b"{}".as_slice()] {
        let home = tempdir().unwrap();
        let manager = manager_with_account(home.path(), "account").await;
        let fp = AuthManager::managed_account_fingerprint("account");
        let token = manager
            .prepare_managed_terminal_commit(Some(&fp))
            .await
            .unwrap();
        let before = manager.auth_cached();
        std::fs::write(get_auth_file(home.path()), replacement).unwrap();
        assert_eq!(
            manager.with_managed_terminal_projection(&token, |_, _| panic!("changed source")),
            Err::<(), _>(ManagedTerminalVerificationError::SourceChanged)
        );
        assert_eq!(manager.auth_cached(), before);
    }
}

#[tokio::test]
#[serial_test::serial(codex_auth_env)]
async fn prepared_handles_and_terminal_tokens_are_bound_to_the_exact_manager() {
    let home = tempdir().unwrap();
    let first = manager_with_account(home.path(), "account").await;
    let second =
        AuthManager::new_from_auth_config(build_config(home.path(), None, None).await, false, None)
            .await;
    let fp = AuthManager::managed_account_fingerprint("account");
    let token = first
        .prepare_managed_terminal_commit(Some(&fp))
        .await
        .unwrap();
    assert_eq!(
        second.with_managed_terminal_projection(&token, |_, _| panic!("wrong manager")),
        Err::<(), _>(ManagedTerminalVerificationError::Authority(
            ManagedAdoptionVerificationError::CacheChangedConcurrently
        ))
    );
    let pre = admission(&first, "account");
    let prepared = first
        .prepare_managed_adoption(Some(&fp), &pre)
        .await
        .unwrap();
    let second_pre = admission(&second, "account");
    assert!(matches!(
        second.install_prepared_managed_adoption(&prepared, &second_pre),
        ManagedAdoptionInstallOutcome::IntendedResultMismatch
    ));
    assert!(matches!(
        first.install_prepared_managed_adoption(&prepared, &pre),
        ManagedAdoptionInstallOutcome::Installed { .. }
    ));
}

#[tokio::test]
#[serial_test::serial(codex_auth_env)]
async fn dropping_a_prepared_handle_releases_its_actual_candidate() {
    let home = tempdir().unwrap();
    let manager = manager_with_account(home.path(), "account").await;
    let pre = admission(&manager, "account");
    write_account(home.path(), "next", "plus");
    let prepared = manager
        .prepare_managed_adoption(
            Some(&AuthManager::managed_account_fingerprint("next")),
            &pre,
        )
        .await
        .unwrap();
    let weak_auth = {
        let store = manager.prepared_managed_adoptions.lock().unwrap();
        assert_eq!(store.candidates.len(), 1);
        let CodexAuth::Chatgpt(auth) = store.candidates[&prepared.id].candidate.as_ref().unwrap()
        else {
            panic!("ChatGPT candidate")
        };
        Arc::downgrade(&auth.state.auth_dot_json)
    };
    assert!(weak_auth.upgrade().is_some());
    drop(prepared);
    assert!(
        manager
            .prepared_managed_adoptions
            .lock()
            .unwrap()
            .candidates
            .is_empty()
    );
    assert!(
        weak_auth.upgrade().is_none(),
        "weak handle is not a credential owner"
    );
}

#[tokio::test]
#[serial_test::serial(codex_auth_env)]
async fn manager_drop_destroys_prepared_candidate_even_with_a_live_handle() {
    let home = tempdir().unwrap();
    let manager = manager_with_account(home.path(), "account").await;
    let pre = admission(&manager, "account");
    write_account(home.path(), "next", "plus");
    let prepared = manager
        .prepare_managed_adoption(
            Some(&AuthManager::managed_account_fingerprint("next")),
            &pre,
        )
        .await
        .unwrap();
    let weak_auth = {
        let store = manager.prepared_managed_adoptions.lock().unwrap();
        let CodexAuth::Chatgpt(auth) = store.candidates[&prepared.id].candidate.as_ref().unwrap()
        else {
            panic!("ChatGPT candidate")
        };
        Arc::downgrade(&auth.state.auth_dot_json)
    };
    drop(manager);
    assert!(prepared.store.upgrade().is_none());
    assert!(weak_auth.upgrade().is_none());
    drop(prepared);
    // This proves only private-store lifetime, not disposal of upstream global secrets caches.
}

#[tokio::test]
#[serial_test::serial(codex_auth_env)]
async fn prepared_id_overflow_refuses_without_inserting_or_installing() {
    let home = tempdir().unwrap();
    let manager = manager_with_account(home.path(), "account").await;
    let pre = admission(&manager, "account");
    manager.prepared_managed_adoptions.lock().unwrap().next_id = u64::MAX;
    let result = manager
        .prepare_managed_adoption(
            Some(&AuthManager::managed_account_fingerprint("account")),
            &pre,
        )
        .await;
    assert!(matches!(
        result,
        Err(ManagedAdoptionVerificationError::CacheUnavailable)
    ));
    assert!(
        manager
            .prepared_managed_adoptions
            .lock()
            .unwrap()
            .candidates
            .is_empty()
    );
    assert_eq!(*manager.auth_change_receiver().borrow(), 0);
}

#[tokio::test]
#[serial_test::serial(codex_auth_env)]
async fn per_manager_mode_restriction_is_not_discarded_by_the_strict_reader() {
    let home = tempdir().unwrap();
    write_account(home.path(), "account", "pro");
    let manager = AuthManager::new_from_auth_config(
        build_config(home.path(), None, None).await,
        false,
        Some(vec![AuthMode::ApiKey]),
    )
    .await;
    assert!(matches!(
        manager.read_managed_adoption_snapshot().await,
        Err(ManagedAdoptionVerificationError::IneligibleAuthMode)
    ));
    assert_eq!(manager.auth_cached(), None);
}

#[tokio::test]
#[serial_test::serial(codex_auth_env)]
async fn strict_managed_reader_rejects_malformed_and_restricted_without_logout() {
    let home = tempdir().unwrap();
    let manager = manager_with_account(home.path(), "account").await;
    let before = manager.auth_cached();
    std::fs::write(get_auth_file(home.path()), "broken-json").unwrap();
    assert!(matches!(
        manager.read_managed_adoption_snapshot().await,
        Err(ManagedAdoptionVerificationError::ParseFailed)
    ));
    assert_eq!(manager.auth_cached(), before);
    write_account(home.path(), "not-allowed", "pro");
    manager.set_forced_chatgpt_workspace_id(Some(vec!["account".into()]));
    assert!(matches!(
        manager.read_managed_adoption_snapshot().await,
        Err(ManagedAdoptionVerificationError::RestrictionRejected)
    ));
    assert_eq!(manager.auth_cached(), before);
}

#[tokio::test]
#[serial_test::serial(codex_auth_env)]
async fn managed_publication_preserves_source_and_login_fences_for_same_account() {
    for (access, login, expected_owner, expected_source, expected_login) in [
        ("new-access", None, 0, 1, 0),
        // Upstream same_owner treats a new non-null login ID as a new owner.
        ("test-access-token", Some("new-login"), 1, 0, 1),
    ] {
        let home = tempdir().unwrap();
        let manager = manager_with_account(home.path(), "account").await;
        let pre = admission(&manager, "account");
        let before = *manager.auth_change_state_receiver().borrow();
        let file = FileAuthStorage::new(home.path().to_path_buf());
        let mut auth = file.load().unwrap().unwrap();
        let tokens = auth.tokens.as_mut().unwrap();
        tokens.access_token = access.into();
        tokens.login_owner_id = login.map(str::to_owned);
        file.save(&auth).unwrap();
        let prepared = manager
            .prepare_managed_adoption(
                Some(&AuthManager::managed_account_fingerprint("account")),
                &pre,
            )
            .await
            .unwrap();
        assert!(matches!(
            manager.install_prepared_managed_adoption(&prepared, &pre),
            ManagedAdoptionInstallOutcome::Installed { .. }
        ));
        assert_eq!(
            *manager.auth_change_state_receiver().borrow(),
            AuthChangeState {
                generation: before.generation + 1,
                owner_generation: before.owner_generation + expected_owner,
                source_generation: before.source_generation + expected_source,
                login_generation: before.login_generation + expected_login,
            }
        );
    }
}

#[tokio::test]
#[serial_test::serial(codex_auth_env)]
async fn terminal_callback_unwind_releases_source_and_read_guards() {
    let home = tempdir().unwrap();
    let manager = manager_with_account(home.path(), "account").await;
    let fp = AuthManager::managed_account_fingerprint("account");
    let token = manager
        .prepare_managed_terminal_commit(Some(&fp))
        .await
        .unwrap();
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = manager.with_managed_terminal_projection(&token, |_, _| {
                panic!("controlled callback unwind")
            });
        }))
        .is_err()
    );
    assert!(manager.inner.try_write().is_ok());
    assert!(manager.external_auth.try_write().is_ok());
    let storage = create_auth_storage(
        home.path().to_path_buf(),
        AuthCredentialsStoreMode::File,
        AuthKeyringBackendKind::Direct,
    );
    assert!(storage.lock_managed_source().is_ok());
    assert_eq!(
        manager.with_managed_terminal_projection(&token, |_, _| 7),
        Ok(7)
    );
}

#[tokio::test]
#[serial_test::serial(codex_auth_env)]
async fn managed_owner_change_preserves_network_policy_ownership_boundary() {
    use codex_http_client::DestinationPolicy;
    use codex_http_client::NetworkPolicyController;
    for owns in [false, true] {
        let home = tempdir().unwrap();
        let mut manager = manager_with_account(home.path(), "before").await;
        let controller = NetworkPolicyController::default();
        let policy = controller.policy();
        manager.auth_route_config = AuthRouteConfig::from_http_client_factory(
            manager
                .http_client_factory()
                .with_network_policy(policy.clone()),
        );
        manager.owns_network_policy = owns;
        let before_factory = manager.http_client_factory();
        let pre = admission(&manager, "before");
        write_account(home.path(), "after", "plus");
        let prepared = manager
            .prepare_managed_adoption(
                Some(&AuthManager::managed_account_fingerprint("after")),
                &pre,
            )
            .await
            .unwrap();
        assert!(matches!(
            manager.install_prepared_managed_adoption(&prepared, &pre),
            ManagedAdoptionInstallOutcome::Installed { .. }
        ));
        assert!(controller.publish(policy.revision(), DestinationPolicy::Unrestricted));
        assert_eq!(
            before_factory
                .network_policy()
                .acquire(&"https://example.com/".parse().unwrap())
                .is_err(),
            owns
        );
    }
}
