use super::*;
use pretty_assertions::assert_eq;

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
