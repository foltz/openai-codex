#[cfg(unix)]
use super::command_cache_publication_tests::{record, selected_fixture};
use super::*;

#[test]
fn bearer_fingerprint_hashes_only_the_frozen_token_and_refuses_missing_bearer() {
    use sha2::Digest;
    for header in [
        "",
        "Bearer",
        "Bearer ",
        "Bearer   ",
        "Basic synthetic",
        "bearer synthetic",
    ] {
        assert!(AuthCredentialFingerprint::from_bearer_header(header).is_none());
    }
    let fingerprint = AuthCredentialFingerprint::from_bearer_header("Bearer synthetic").unwrap();
    assert_eq!(
        fingerprint.0,
        format!("{:x}", sha2::Sha256::digest(b"synthetic"))
    );
}

#[cfg(unix)]
#[tokio::test]
async fn command_recovery_never_guesses_the_rejected_token_from_mutable_cache() {
    let home = tempfile::tempdir().unwrap();
    let (source, manager) = selected_fixture(home.path(), record(1));
    let before = std::fs::read(home.path().join("auth.json")).unwrap();
    let mut recovery = manager.unauthorized_recovery();
    let error = recovery.next().await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("rejected command credential was not recorded")
    );
    assert!(!recovery.has_next());
    assert!(source.usable_auth(manager.auth_cached().as_ref()));
    assert_eq!(
        std::fs::read(home.path().join("auth.json")).unwrap(),
        before
    );
}

#[cfg(unix)]
#[tokio::test]
async fn rejected_old_auth_snapshot_does_not_poison_the_current_newer_credential() {
    let home = tempfile::tempdir().unwrap();
    let newer = record(2);
    let (source, manager) = selected_fixture(home.path(), newer.clone());
    let mut recovery = manager.unauthorized_recovery();
    recovery.record_failed_auth(Some(&record(1).auth().unwrap()));
    recovery.next().await.unwrap();
    assert!(!recovery.has_next());
    assert_eq!(
        manager.auth_cached().unwrap().get_token().unwrap(),
        newer.access_token
    );
    assert!(source.usable_auth(manager.auth_cached().as_ref()));
}

#[cfg(unix)]
#[tokio::test]
async fn rejected_new_header_is_not_imported_as_success_from_a_stale_local_cache() {
    let home = tempfile::tempdir().unwrap();
    let old = record(1);
    let (source, manager) = selected_fixture(home.path(), old.clone());
    let newer = record(2);
    super::super::command_store::commit(home.path(), &newer, &newer.auth().unwrap()).unwrap();
    let before = std::fs::read(home.path().join("auth.json")).unwrap();
    let mut recovery = manager.unauthorized_recovery();
    recovery.record_failed_credential(AuthCredentialFingerprint::from_bearer_header(&format!(
        "Bearer {}",
        newer.access_token
    )));
    assert!(recovery.next().await.is_err());
    assert_eq!(
        manager.auth_cached().unwrap().get_token().unwrap(),
        old.access_token
    );
    assert!(manager.auth().await.is_none());
    assert!(!source.usable_auth(Some(&newer.auth().unwrap())));
    assert_eq!(
        std::fs::read(home.path().join("auth.json")).unwrap(),
        before
    );
}

#[cfg(unix)]
#[tokio::test]
async fn missing_next_request_snapshot_clears_prior_rejection_proof() {
    let home = tempfile::tempdir().unwrap();
    let (source, manager) = selected_fixture(home.path(), record(2));
    let mut recovery = manager.unauthorized_recovery();
    recovery.record_failed_auth(Some(&record(1).auth().unwrap()));
    recovery.record_failed_credential(None);
    let error = recovery.next().await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("rejected command credential was not recorded")
    );
    assert!(source.usable_auth(manager.auth_cached().as_ref()));
}

#[cfg(unix)]
#[tokio::test]
async fn foreign_workspace_snapshot_cannot_poison_the_selected_source() {
    let home = tempfile::tempdir().unwrap();
    let current = record(2);
    let (source, manager) = selected_fixture(home.path(), current.clone());
    let foreign =
        CodexAuth::from_external_chatgpt_tokens(&current.access_token, "foreign-workspace", None)
            .unwrap();
    let mut recovery = manager.unauthorized_recovery();
    recovery.record_failed_auth(Some(&current.auth().unwrap()));
    recovery.record_failed_auth(Some(&foreign));
    assert!(recovery.expected_credential_fingerprint.is_none());
    assert!(recovery.next().await.is_err());
    assert!(source.usable_auth(manager.auth_cached().as_ref()));
}

#[test]
fn ordinary_recovery_does_not_adopt_command_rejection_state() {
    let manager = AuthManager::from_auth_for_testing(CodexAuth::from_api_key("synthetic"));
    let mut recovery = manager.unauthorized_recovery();
    recovery.record_failed_auth(manager.auth_cached().as_ref());
    recovery.record_failed_credential(AuthCredentialFingerprint::from_bearer_header(
        "Bearer synthetic",
    ));
    assert!(recovery.expected_credential_fingerprint.is_none());
    assert_eq!(recovery.mode_name(), "managed");
    assert!(!recovery.has_next());
}

#[cfg(unix)]
#[tokio::test]
async fn rejected_old_frozen_header_coalesces_to_the_current_newer_credential() {
    let home = tempfile::tempdir().unwrap();
    let newer = record(2);
    let (source, manager) = selected_fixture(home.path(), newer.clone());
    let mut recovery = manager.unauthorized_recovery();
    recovery.record_failed_credential(AuthCredentialFingerprint::from_bearer_header(&format!(
        "Bearer {}",
        record(1).access_token
    )));
    recovery.next().await.unwrap();
    assert_eq!(
        manager.auth_cached().unwrap().get_token().unwrap(),
        newer.access_token
    );
    assert!(source.usable_auth(manager.auth_cached().as_ref()));
}
