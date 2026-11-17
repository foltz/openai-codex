//! Actual exporter-process/cache boundaries with synthetic owners and owned files.
use super::*;
use codex_login::AuthManager;
use codex_login::CodexAuth;
use std::num::NonZeroU64;
use std::path::Path;
use std::time::Duration;

fn owner(home: &Path) -> Arc<AuthManager> {
    AuthManager::from_auth_for_testing_with_home(
        CodexAuth::from_api_key("synthetic-owner"),
        home.to_path_buf(),
    )
}
fn export(owner: &Arc<AuthManager>, counter: &Path) -> GatewayExport {
    GatewayExport::new(
        CredentialExportConfig {
            command: "/bin/sh".into(),
            args: vec![
                "-c".into(),
                "printf x >> \"$1\"; printf gateway-token".into(),
                "gateway-test".into(),
                counter.to_string_lossy().into_owned().into(),
            ],
            timeout_ms: NonZeroU64::new(2000).unwrap(),
        },
        NetworkPolicy::default(),
        owner,
    )
}

#[tokio::test]
async fn admitted_export_reuses_exact_cache_and_refuses_foreign_or_invalidated_source() {
    let home = tempfile::tempdir().unwrap();
    let owner = owner(home.path());
    let counter = home.path().join("counter");
    let first = export(&owner, &counter);
    assert!(first.observed_binding().is_none());
    let (token, binding) = first.token_observed().await.unwrap();
    assert_eq!(&*token, "gateway-token");
    assert_eq!(first.observed_binding(), Some(binding.clone()));
    assert_eq!(&*first.token_for(&binding).await.unwrap(), "gateway-token");
    assert_eq!(std::fs::read(&counter).unwrap(), b"x");
    let other = export(&owner, &counter);
    other.token_observed().await.unwrap();
    assert!(other.token_for(&binding).await.is_err());
    assert_eq!(std::fs::read(&counter).unwrap(), b"xx");
    first.invalidate().await;
    assert!(first.token_for(&binding).await.is_err());
    assert_eq!(
        std::fs::read(&counter).unwrap(),
        b"xx",
        "admitted use never re-executes"
    );
    let (_, replacement) = first.token_observed().await.unwrap();
    assert_ne!(
        binding, replacement,
        "equal token bytes do not revive an invalidated admission"
    );
    assert!(first.token_for(&binding).await.is_err());
    assert!(first.token_for(&replacement).await.is_ok());
    assert_eq!(std::fs::read(&counter).unwrap(), b"xxx");
    assert!(!format!("{binding:?}").contains("gateway-token"));
}

#[tokio::test]
async fn admitted_export_refuses_account_change_even_without_managed_network_policy() {
    let home = tempfile::tempdir().unwrap();
    let owner = owner(home.path());
    let counter = home.path().join("counter");
    let export = export(&owner, &counter);
    let (_, binding) = export.token_observed().await.unwrap();
    assert!(owner.reload_with_cached_auth().await.unwrap());
    assert!(export.observed_binding().is_none());
    assert!(export.token_for(&binding).await.is_err());
    assert_eq!(std::fs::read(&counter).unwrap(), b"x");
    let (_, next) = export.token_observed().await.unwrap();
    assert_ne!(binding, next);
    assert!(export.token_for(&binding).await.is_err());
    assert_eq!(std::fs::read(&counter).unwrap(), b"xx");
}

#[tokio::test]
async fn account_change_during_export_refuses_cache_publication() {
    let home = tempfile::tempdir().unwrap();
    let owner = owner(home.path());
    let started = home.path().join("started");
    let release = home.path().join("release");
    let export = Arc::new(GatewayExport::new(CredentialExportConfig {
        command: "/bin/sh".into(),
        args: vec!["-c".into(), "printf started > \"$1\"; while [ ! -f \"$2\" ]; do /bin/sleep 0.01; done; printf gateway-token".into(),
            "gateway-test".into(), started.to_string_lossy().into_owned().into(), release.to_string_lossy().into_owned().into()],
        timeout_ms: NonZeroU64::new(2000).unwrap(),
    }, NetworkPolicy::default(), &owner));
    let task = {
        let export = Arc::clone(&export);
        tokio::spawn(async move { export.token_observed().await })
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        while !started.exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(owner.reload_with_cached_auth().await.unwrap());
    std::fs::write(&release, b"release").unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert!(
        export.observed_binding().is_none(),
        "old-owner output must not enter the cache"
    );
}
