use super::*;
use codex_login::LoginWorkerOutcome;
use pretty_assertions::assert_eq;
use std::time::Duration;

fn options(home: &tempfile::TempDir) -> ServerOptions {
    let mut options = ServerOptions::new(
        home.path().to_path_buf(),
        codex_login::CLIENT_ID.to_owned(),
        /*forced_chatgpt_workspace_id*/ None,
        codex_login::AuthCredentialsStoreMode::Ephemeral,
        codex_login::AuthKeyringBackendKind::default(),
        codex_login::test_support::transport_default_auth_route_config(),
    );
    options.port = 0;
    options.open_browser = false;
    options
}

#[tokio::test]
async fn account_owner_retains_all_browser_servers_before_first_await() {
    let home = tempfile::tempdir().unwrap();
    let owner = BrowserLogins::default();
    drop(owner.start(options(&home)).unwrap());
    drop(owner.start(options(&home)).unwrap());
    let deadline = Instant::now() + Duration::from_secs(5);
    owner.close(deadline).unwrap();
    assert!(owner.start(options(&home)).is_err());
    owner.close(deadline + Duration::from_secs(60)).unwrap();
    assert_eq!(owner.0.lock().unwrap().deadline, Some(deadline));
    let expected = Some(LoginRetirementReport {
        callback: Some(LoginWorkerOutcome::Joined),
        response: Some(LoginWorkerOutcome::Joined),
        receiver: Some(LoginWorkerOutcome::Joined),
        http: Some(codex_login::LoginHttpReport {
            acceptor: Some(LoginWorkerOutcome::Joined),
            ..Default::default()
        }),
        persistence: Some(LoginWorkerOutcome::Joined),
        deadline_expired: false,
    });
    assert_eq!(owner.wait().await.unwrap(), vec![expected, expected]);
    assert_eq!(owner.wait().await.unwrap(), vec![expected, expected]);
}

#[tokio::test]
async fn unfinished_constructor_is_not_an_empty_successful_population() {
    let owner = BrowserLogins::default();
    let attempt = Arc::new(Attempt {
        state: Mutex::new(AttemptState {
            constructing: true,
            ..Default::default()
        }),
        changed: watch::channel(0).0,
    });
    owner.0.lock().unwrap().attempts.push(Arc::clone(&attempt));
    owner.close(Instant::now()).unwrap();
    assert!(owner.wait().await.is_err());
    assert!(attempt.state.lock().unwrap().constructing);
}

#[tokio::test]
async fn admitted_real_constructor_finishing_after_close_is_drained() {
    let home = tempfile::tempdir().unwrap();
    let owner = BrowserLogins::default();
    let constructor_owner = owner.clone();
    let options = options(&home);
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let constructor = tokio::task::spawn_blocking(move || {
        constructor_owner.start_with(options, |options| {
            entered_tx.send(()).unwrap();
            release_rx.blocking_recv().unwrap();
            codex_login::run_login_server(options)
        })
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    tokio::time::timeout_at(deadline, entered_rx)
        .await
        .unwrap()
        .unwrap();
    owner.close(deadline).unwrap();
    let mut first_observer = Box::pin(owner.wait());
    assert!(futures::poll!(first_observer.as_mut()).is_pending());
    drop(first_observer);

    release_tx.send(()).unwrap();
    assert!(
        tokio::time::timeout_at(deadline, constructor)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    let expected = vec![Some(LoginRetirementReport {
        callback: Some(LoginWorkerOutcome::Joined),
        response: Some(LoginWorkerOutcome::Joined),
        receiver: Some(LoginWorkerOutcome::Joined),
        http: Some(codex_login::LoginHttpReport {
            acceptor: Some(LoginWorkerOutcome::Joined),
            ..Default::default()
        }),
        persistence: Some(LoginWorkerOutcome::Joined),
        deadline_expired: false,
    })];
    assert_eq!(owner.wait().await.unwrap(), expected);
    assert_eq!(owner.wait().await.unwrap(), expected);
}

#[tokio::test]
async fn poisoned_custody_refuses_success_but_still_drains_every_known_browser() {
    enum PoisonTarget {
        Registry,
        FirstAttempt,
    }

    for target in [PoisonTarget::Registry, PoisonTarget::FirstAttempt] {
        let home = tempfile::tempdir().unwrap();
        let owner = BrowserLogins::default();
        drop(owner.start(options(&home)).unwrap());
        drop(owner.start(options(&home)).unwrap());
        let attempts = owner.0.lock().unwrap().attempts.clone();
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match target {
                PoisonTarget::Registry => {
                    let _guard = owner.0.lock().unwrap();
                    panic!("poison registry");
                }
                PoisonTarget::FirstAttempt => {
                    let _guard = attempts[0].state.lock().unwrap();
                    panic!("poison first attempt");
                }
            }))
            .is_err()
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        assert!(owner.close(deadline).is_err());
        assert!(owner.start(options(&home)).is_err());
        assert!(owner.wait().await.is_err());
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(5)).await;
        for attempt in attempts {
            let retirement = attempt
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .retirement
                .clone()
                .unwrap();
            assert_eq!(retirement.deadline(), deadline);
            // Elapse this observer's original budget before re-observation:
            // wait must replay proof recorded by the aggregate above, not
            // manufacture it by driving an omitted browser for the first time.
            assert_eq!(
                retirement.wait().await,
                LoginRetirementReport {
                    callback: Some(LoginWorkerOutcome::Joined),
                    response: Some(LoginWorkerOutcome::Joined),
                    receiver: Some(LoginWorkerOutcome::Joined),
                    http: Some(codex_login::LoginHttpReport {
                        acceptor: Some(LoginWorkerOutcome::Joined),
                        ..Default::default()
                    }),
                    persistence: Some(LoginWorkerOutcome::Joined),
                    deadline_expired: false,
                }
            );
        }
        tokio::time::resume();
    }
}
