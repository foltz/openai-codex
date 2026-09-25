use super::*;
use codex_core::test_support::auth_manager_from_auth_with_home;
use codex_login::CodexAuth;
use pretty_assertions::assert_eq;
use tempfile::TempDir;

#[tokio::test]
async fn reset_preserves_same_owner_and_observes_a_previously_replaced_owner() {
    let home = TempDir::new().expect("temporary auth home");
    let auth = auth_manager_from_auth_with_home(
        CodexAuth::create_dummy_chatgpt_auth_for_testing(),
        home.path().to_path_buf(),
    );
    let (events, _events_rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (name_tx, name_rx) = oneshot::channel();
    let shutdown = CancellationToken::new();
    let (task, handle) = start_remote_control(
        RemoteControlStartConfig {
            remote_control_url: "http://127.0.0.1:1".into(),
            installation_id: "reset-test".into(),
            policy: RemoteControlPolicy::Allowed,
        },
        /*state_db*/ None,
        auth.clone(),
        events,
        shutdown.clone(),
        Some(name_rx),
        RemoteControlStartupMode::DisabledEphemeral,
    )
    .await
    .expect("controller starts");
    let original = handle.inner.session();
    // Keep the name gate closed: neither the old nor the new session needs network I/O.
    original
        .desired_state_tx
        .send_replace(RemoteControlDesiredState::Enabled {
            persistence_preference: None,
        });
    handle
        .reset_auth_cycle()
        .await
        .expect("same owner needs no retirement");
    assert!(Arc::ptr_eq(&original, &handle.inner.session()));
    assert!(original.desired_state_tx.borrow().is_enabled());

    auth.logout()
        .await
        .expect("clear only the isolated fixture's auth");
    let replacement = handle.inner.session();
    assert!(!Arc::ptr_eq(&original, &replacement));
    assert_eq!(
        *replacement.desired_state_tx.borrow(),
        RemoteControlDesiredState::Disabled
    );
    assert!(original.shutdown_token.is_cancelled());
    // Replacement already occurred; reset must still join the old websocket and name waiter.
    handle
        .reset_auth_cycle()
        .await
        .expect("retired owner joined");
    assert!(!handle.inner.retired.snapshot().is_empty());
    assert!(
        handle
            .inner
            .retired
            .snapshot()
            .iter()
            .all(|receipt| receipt.peek() == Some(&true))
    );
    assert!(Arc::ptr_eq(&replacement, &handle.inner.session()));
    handle
        .reset_auth_cycle()
        .await
        .expect("repeated reset replays clean retirement");
    shutdown.cancel();
    task.await.expect("process tracker joins");
    drop(name_tx);
}

#[tokio::test]
async fn reset_timeout_retains_previously_retired_work_for_retry() {
    let home = TempDir::new().expect("temporary auth home");
    let auth = auth_manager_from_auth_with_home(
        CodexAuth::create_dummy_chatgpt_auth_for_testing(),
        home.path().to_path_buf(),
    );
    let (events, _events_rx) = mpsc::channel(CHANNEL_CAPACITY);
    let shutdown = CancellationToken::new();
    let (task, handle) = start_remote_control(
        RemoteControlStartConfig {
            remote_control_url: "http://127.0.0.1:1".into(),
            installation_id: "timeout-test".into(),
            policy: RemoteControlPolicy::Allowed,
        },
        /*state_db*/ None,
        auth,
        events,
        shutdown.clone(),
        /*app_server_client_name_rx*/ None,
        RemoteControlStartupMode::DisabledEphemeral,
    )
    .await
    .expect("controller starts");
    let (release, held) = oneshot::channel();
    drop(
        handle
            .inner
            .retired
            .track(tokio::spawn(async move { held.await.is_ok() })),
    );
    tokio::time::pause();
    assert_eq!(
        handle
            .reset_auth_cycle()
            .await
            .expect_err("held retirement")
            .kind(),
        io::ErrorKind::TimedOut
    );
    release.send(()).expect("timeout did not cancel work");
    handle
        .reset_auth_cycle()
        .await
        .expect("retry joins original work");
    drop(handle.inner.retired.track(tokio::spawn(async { false })));
    assert_eq!(
        handle
            .reset_auth_cycle()
            .await
            .expect_err("failed cleanup")
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(
        handle
            .reset_auth_cycle()
            .await
            .expect_err("failure remains")
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    shutdown.cancel();
    task.await.expect("process tracker joins");
}
