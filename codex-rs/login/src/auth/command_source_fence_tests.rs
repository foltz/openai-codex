#![cfg(unix)]
use super::command_cache_publication_tests::{record, selected_fixture};
use super::*;
use std::sync::atomic::AtomicUsize;

struct Probe(Arc<AtomicUsize>);
impl ExternalAuth for Probe {
    fn resolve(&self) -> ExternalAuthFuture<'_, CodexAuth> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(std::io::Error::other("unexpected external resolve")) })
    }
    fn refresh(&self, _: ExternalAuthRefreshContext) -> ExternalAuthFuture<'_, CodexAuth> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(std::io::Error::other("unexpected external refresh")) })
    }
}
fn files(home: &Path) -> (Vec<u8>, Vec<u8>) {
    (
        std::fs::read(home.join("auth.json")).unwrap(),
        std::fs::read(home.join(".external-auth-owner.json")).unwrap(),
    )
}

#[tokio::test]
async fn selected_command_source_cannot_be_replaced_or_cleared_through_public_auth_controls() {
    let home = tempfile::tempdir().unwrap();
    let (_, manager) = selected_fixture(home.path(), record(1));
    let before = files(home.path());
    let provider = manager.external_auth_provider().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    assert!(
        manager
            .set_external_auth(Arc::new(Probe(calls.clone())))
            .await
            .is_err()
    );
    manager.clear_external_auth();
    assert!(Arc::ptr_eq(
        &provider,
        &manager.external_auth_provider().unwrap()
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(files(home.path()), before);
    assert_eq!(
        *manager.auth_change_state_receiver().borrow(),
        AuthChangeState::default()
    );
}

#[tokio::test]
async fn selected_command_source_refuses_logout_before_deletion_or_revoke() {
    let home = tempfile::tempdir().unwrap();
    let (_, manager) = selected_fixture(home.path(), record(1));
    let before = files(home.path());
    assert_eq!(
        manager.logout().await.unwrap_err().kind(),
        std::io::ErrorKind::PermissionDenied
    );
    assert_eq!(
        manager.logout_with_revoke().await.unwrap_err().kind(),
        std::io::ErrorKind::PermissionDenied
    );
    assert_eq!(files(home.path()), before);
    assert!(manager.has_external_auth());
    assert_eq!(
        *manager.auth_change_state_receiver().borrow(),
        AuthChangeState::default()
    );
}

#[tokio::test]
async fn selected_command_load_does_not_use_generic_external_cached_fallback() {
    let home = tempfile::tempdir().unwrap();
    let (_, manager) = selected_fixture(home.path(), record(1));
    std::fs::remove_file(home.path().join("auth.json")).unwrap();
    std::fs::remove_file(home.path().join(".external-auth-owner.json")).unwrap();
    assert!(manager.load_auth(None).await.is_err());
    assert!(manager.auth().await.is_none());
    assert!(
        manager.auth_cached().is_some(),
        "uncertainty does not rewrite the last owner to logout"
    );
    assert_eq!(
        *manager.auth_change_state_receiver().borrow(),
        AuthChangeState::default()
    );
}

#[tokio::test]
async fn selected_command_mode_restriction_refuses_before_source_resolution() {
    let home = tempfile::tempdir().unwrap();
    let (source, manager) = selected_fixture(home.path(), record(1));
    assert!(
        std::thread::spawn(move || {
            let _guard = source.state.lock().unwrap();
            panic!("synthetic source poison to discriminate resolution order");
        })
        .join()
        .is_err()
    );
    manager.restrict_auth_modes(vec![AuthMode::ApiKey]).unwrap();
    let error = manager.load_auth(None).await.unwrap_err();
    assert!(error.to_string().contains("command auth mode forbidden"));
    assert!(manager.auth().await.is_none());
}

#[test]
fn selected_command_commit_does_not_create_an_ephemeral_native_copy() {
    let home = tempfile::tempdir().unwrap();
    let (_, manager) = selected_fixture(home.path(), record(1));
    let before = files(home.path());
    let ephemeral = create_auth_storage(
        home.path().to_path_buf(),
        AuthCredentialsStoreMode::Ephemeral,
        AuthKeyringBackendKind::default(),
    );
    assert!(ephemeral.load().unwrap().is_none());
    manager
        .commit_external_auth(manager.auth_cached().unwrap())
        .unwrap();
    assert!(ephemeral.load().unwrap().is_none());
    assert_eq!(files(home.path()), before);
    assert_eq!(
        *manager.auth_change_state_receiver().borrow(),
        AuthChangeState::default()
    );
}

#[test]
fn cache_only_provider_receipt_never_resolves_callbacks_or_returns_credentials() {
    let home = tempfile::tempdir().unwrap();
    let (_, manager) = selected_fixture(home.path(), record(1));
    let token = manager.auth_cached().unwrap().get_token().unwrap();
    let before = files(home.path());
    let calls = Arc::new(AtomicUsize::new(0));
    // Deliberately replace only the test fixture callback with a refusal probe:
    // an observation must not invoke any provider, even when resolution fails.
    *manager.external_auth.write().unwrap() = Some(Arc::new(Probe(calls.clone())));
    let observation = manager.auth_observation();
    assert_eq!(observation["stateKnown"], true);
    assert_eq!(observation["provider"]["source"], "source");
    assert_eq!(observation["provider"]["generation"], 1);
    assert_eq!(observation["provider"]["coherent"], true);
    assert!(!observation.to_string().contains(&token));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(files(home.path()), before);
    assert_eq!(
        *manager.auth_change_state_receiver().borrow(),
        AuthChangeState::default()
    );
}
