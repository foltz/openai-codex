use super::*;
use codex_login::LoginWorkerOutcome;
use std::sync::Arc;
use std::time::Duration;

#[test]
fn missing_browser_resources_do_not_block_clean_account_shutdown() {
    let report = AccountLoginReport {
        tasks: ProcessorTaskDrain {
            terminal: true,
            ..Default::default()
        },
        browsers: Ok(vec![None]),
        admission_unavailable: false,
    };
    assert!(report.is_clean());
}

#[test]
fn unavailable_browser_admission_still_blocks_clean_account_shutdown() {
    let report = AccountLoginReport {
        tasks: ProcessorTaskDrain {
            terminal: true,
            ..Default::default()
        },
        browsers: Ok(vec![None]),
        admission_unavailable: true,
    };
    assert!(!report.is_clean());
}

#[tokio::test]
async fn held_completion_task_does_not_starve_real_browser_retirement() {
    let home = tempfile::tempdir().unwrap();
    let mut options = codex_login::ServerOptions::new(
        home.path().to_path_buf(),
        codex_login::CLIENT_ID.to_owned(),
        /*forced_chatgpt_workspace_id*/ None,
        codex_login::AuthCredentialsStoreMode::Ephemeral,
        codex_login::AuthKeyringBackendKind::default(),
        codex_login::test_support::transport_default_auth_route_config(),
    );
    options.port = 0;
    options.open_browser = false;
    let browsers = BrowserLogins::default();
    drop(browsers.start(options).unwrap());
    let tasks = ProcessorTasks::default();
    let resource = Arc::new(());
    let weak = Arc::downgrade(&resource);
    let (release, held) = tokio::sync::oneshot::channel();
    let receipt = tasks
        .spawn(async move {
            let _resource = resource;
            held.await.unwrap();
        })
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    browsers.close(deadline).unwrap();
    tasks.close_registration().unwrap();
    let owner = AccountLoginShutdown {
        deadline,
        tasks,
        browsers,
        admission_unavailable: false,
    };
    let report = owner.wait().await;
    assert!(!report.tasks.terminal);
    assert!(weak.upgrade().is_some());
    let browser = report.browsers.unwrap().pop().unwrap().unwrap();
    assert_eq!(browser.callback, Some(LoginWorkerOutcome::Joined));
    assert_eq!(browser.receiver, Some(LoginWorkerOutcome::Joined));
    assert_eq!(browser.response, Some(LoginWorkerOutcome::Joined));
    assert!(!browser.deadline_expired);
    release.send(()).unwrap();
    assert_eq!(
        receipt.await,
        crate::processor_task_retirement::ProcessorTaskJoin::Joined
    );
    assert!(weak.upgrade().is_none());
}
