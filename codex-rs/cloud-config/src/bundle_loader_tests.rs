use super::*;
use codex_http_client::OutboundProxyPolicy;
use codex_login::CodexAuth;
use std::future::Future;
use std::task::Poll;
use tempfile::tempdir;

fn factory() -> HttpClientFactory {
    HttpClientFactory::new(OutboundProxyPolicy::ReqwestDefault)
}

fn auth() -> Arc<AuthManager> {
    AuthManager::from_auth_for_testing(CodexAuth::from_api_key("disposable-test-key"))
}

async fn reset(
    home: &std::path::Path,
) -> Result<CloudConfigBundleLoader, CloudConfigBundleLoadError> {
    managed_cloud_config_bundle_loader(
        auth(),
        "http://127.0.0.1:1".to_owned(),
        home.to_path_buf(),
        factory(),
    )
    .await
}

#[tokio::test(start_paused = true)]
#[expect(
    clippy::await_holding_invalid_type,
    reason = "test deliberately holds publication to prove reset timeout and retry ownership"
)]
async fn retirement_timeout_retains_ownership_and_retry_waits_for_publication() {
    let home = tempdir().expect("home");
    let lifecycle = home_lifecycle(home.path()).expect("lifecycle");
    let ordinary = cloud_config_bundle_loader(
        auth(),
        "http://127.0.0.1:1".to_owned(),
        home.path().to_path_buf(),
        factory(),
    );
    assert!(ordinary.get().await.is_ok());
    let lease = lifecycle.publication.lock().await;
    assert!(reset(home.path()).await.is_err());
    assert!(lifecycle.owners.lock().expect("owners").retiring);
    assert!(
        cloud_config_bundle_loader(
            auth(),
            "http://127.0.0.1:1".to_owned(),
            home.path().to_path_buf(),
            factory()
        )
        .get()
        .await
        .is_err()
    );
    drop(lease);
    assert!(
        reset(home.path())
            .await
            .expect("retry loader")
            .get()
            .await
            .is_ok()
    );
    assert!(!lifecycle.owners.lock().expect("owners").retiring);
}

#[tokio::test]
#[expect(
    clippy::await_holding_invalid_type,
    reason = "test deliberately blocks publication while cancelling a reset observer"
)]
async fn cancellation_keeps_retirement_gate_until_successful_retry() {
    let home = tempdir().expect("home");
    let lifecycle = home_lifecycle(home.path()).expect("lifecycle");
    let lease = lifecycle.publication.lock().await;
    let mut resetting = Box::pin(reset(home.path()));
    std::future::poll_fn(|cx| {
        assert!(resetting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(resetting);
    assert!(lifecycle.owners.lock().expect("owners").retiring);
    drop(lease);
    assert!(
        reset(home.path())
            .await
            .expect("retry loader")
            .get()
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn finished_owner_panic_is_reported_before_a_fresh_retry_can_succeed() {
    let home = tempdir().expect("home");
    let lifecycle = home_lifecycle(home.path()).expect("lifecycle");
    let task = tokio::spawn(async { panic!("injected cloud owner failure") });
    while !task.is_finished() {
        tokio::task::yield_now().await;
    }
    lifecycle.owners.lock().expect("owners").tasks.push(task);
    // Ordinary registration must not discard a completed panic unobserved.
    let ordinary = cloud_config_bundle_loader(
        auth(),
        "http://127.0.0.1:1".to_owned(),
        home.path().to_path_buf(),
        factory(),
    );
    assert!(ordinary.get().await.is_ok());
    assert!(reset(home.path()).await.is_err());
    assert!(lifecycle.owners.lock().expect("owners").retiring);
    // The failed owner is now proved terminal, with no surviving publication;
    // a distinct reset attempt may rebuild rather than remain permanently wedged.
    assert!(
        reset(home.path())
            .await
            .expect("fresh retry")
            .get()
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn exhausted_generation_never_wraps_or_reopens_ordinary_construction() {
    let home = tempdir().expect("home");
    let lifecycle = home_lifecycle(home.path()).expect("lifecycle");
    lifecycle.generation.store(u64::MAX, Ordering::Release);
    assert!(reset(home.path()).await.is_err());
    assert_eq!(lifecycle.generation.load(Ordering::Acquire), u64::MAX);
    assert!(lifecycle.owners.lock().expect("owners").retiring);
    assert!(reset(home.path()).await.is_err());
}

#[cfg(unix)]
#[test]
fn symlink_home_aliases_share_one_lifecycle_even_before_home_creation() {
    let home = tempdir().expect("home");
    let alias = home.path().join("alias");
    let actual = home.path().join("actual");
    std::fs::create_dir(&actual).expect("actual");
    std::os::unix::fs::symlink(&actual, &alias).expect("alias");
    assert!(Arc::ptr_eq(
        &home_lifecycle(&actual.join("new-home")).expect("actual lifecycle"),
        &home_lifecycle(&alias.join("new-home")).expect("alias lifecycle")
    ));
}
