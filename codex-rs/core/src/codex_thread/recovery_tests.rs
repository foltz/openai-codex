use super::*;
use crate::ThreadCleanupOutcome;
use crate::session::retirement::tests::ThreadLoopFixture;
use crate::session::retirement::tests::exact_thread_fixture;
use futures::future::FutureExt;
use std::time::Duration;

#[tokio::test]
async fn recovery_reconciliation_is_separate_from_the_original_failed_receipt() {
    let thread = exact_thread_fixture(ThreadLoopFixture::Ordinary).await;
    assert!(!thread.retirement_is_quiescent());
    let deadline = Instant::now() + Duration::from_secs(20);
    let ticket = thread.begin_retirement(deadline).unwrap();
    let mut original = ticket.wait().await;
    assert!(original.is_complete());
    // Inject the typed sticky aggregate classification. Independent real task/common
    // receipts still come from the normal cleanup handler; this isn't an MCP-cause test.
    original.cleanup = ThreadCleanupOutcome::McpFailed;
    let failed = futures::future::ready(original).boxed().shared();
    failed.clone().await;
    let original = {
        let mut slot = thread.retirement.lock().unwrap();
        slot.as_mut().unwrap().completion = failed;
        slot.as_ref().unwrap().clone()
    };
    assert_eq!(
        original.wait().await.cleanup,
        ThreadCleanupOutcome::McpFailed
    );
    crate::session::retirement::recovery_tests::inject_completed_mcp_failure(
        &thread.session.cleanup_owner(),
    );
    assert!(thread.observed_terminal_cleanup().is_none());
    assert!(thread.retirement_is_quiescent());
    assert!(!thread.retirement_reconciled());
    assert_eq!(
        thread.reconcile_retirement_until(Instant::now()).await,
        ThreadRecoveryOutcome::Quiescent
    );
    assert_eq!(
        thread
            .reconcile_retirement_until(Instant::now() + Duration::from_secs(5))
            .await,
        ThreadRecoveryOutcome::Reconciled
    );
    assert!(thread.retirement_reconciled());
    assert_eq!(original.deadline(), deadline);
    assert_eq!(
        original.wait().await.cleanup,
        ThreadCleanupOutcome::McpFailed
    );
    assert!(thread.observed_terminal_cleanup().is_some());
}

#[tokio::test]
async fn recovery_rejects_cancelled_or_panicked_session_loops() {
    for mode in [ThreadLoopFixture::Cancelled, ThreadLoopFixture::Panicked] {
        let thread = exact_thread_fixture(mode).await;
        let _ = thread
            .begin_retirement(Instant::now() + Duration::from_secs(1))
            .unwrap()
            .wait()
            .await;
        assert!(!thread.retirement_is_quiescent());
        assert_eq!(
            thread
                .reconcile_retirement_until(Instant::now() + Duration::from_secs(1))
                .await,
            ThreadRecoveryOutcome::Ineligible
        );
        assert!(!thread.retirement_reconciled());
    }
}

#[tokio::test]
async fn recovery_population_retains_handoff_until_separate_positive_mcp_proof() {
    use crate::StartThreadOptions;
    use crate::ThreadManager;
    use crate::config::ConfigBuilder;
    use std::sync::Arc;
    let home = tempfile::tempdir().unwrap();
    let mut config = ConfigBuilder::default()
        .codex_home(home.path().to_path_buf())
        .fallback_cwd(Some(home.path().to_path_buf()))
        .build()
        .await
        .unwrap();
    config.ephemeral = true;
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        codex_login::CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let started = manager
        .start_thread(StartThreadOptions::new(config.clone(), None))
        .await
        .unwrap();
    let ticket = started
        .thread
        .begin_retirement(Instant::now() + Duration::from_secs(20))
        .unwrap();
    ticket.wait().await;
    crate::session::retirement::recovery_tests::inject_completed_mcp_failure(
        &started.thread.session.cleanup_owner(),
    );
    let weak = Arc::downgrade(&started.thread);
    manager
        .remove_thread_if_same(&started.thread_id, &started.thread)
        .await;
    drop(started);
    let current = manager
        .start_thread(StartThreadOptions::new(config.clone(), None))
        .await
        .unwrap();
    let old = weak
        .upgrade()
        .expect("incomplete exact runtime stays in the core population");
    assert_eq!(
        old.reconcile_retirement_until(Instant::now()).await,
        ThreadRecoveryOutcome::Quiescent
    );
    assert!(old.observed_terminal_cleanup().is_none());
    assert_eq!(
        old.reconcile_retirement_until(Instant::now() + Duration::from_secs(5))
            .await,
        ThreadRecoveryOutcome::Reconciled
    );
    drop(old);
    let next = manager
        .start_thread(StartThreadOptions::new(config, None))
        .await
        .unwrap();
    assert!(
        weak.upgrade().is_none(),
        "positive proof enables population compaction"
    );
    let report = manager
        .begin_shutdown(Instant::now() + Duration::from_secs(20))
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(report.is_complete(), "{report:?}");
    drop((current, next));
}

#[tokio::test]
async fn recovery_complete_cleanup_accepts_historical_task_panic_but_incomplete_does_not() {
    let thread = exact_thread_fixture(ThreadLoopFixture::Ordinary).await;
    let task = thread.session.task_joins.register(tokio::spawn(async {
        panic!("historical task panic, actual registered join");
    }));
    assert!(!task.wait().await);
    let ticket = thread
        .begin_retirement(Instant::now() + Duration::from_secs(20))
        .unwrap();
    let complete = ticket.wait().await;
    assert!(complete.is_complete(), "{complete:?}");
    assert!(
        !thread.session.cleanup_owner().recovery_ready(),
        "the panic bit must remain recorded"
    );
    assert!(
        thread.retirement_is_quiescent(),
        "original complete proof is sufficient"
    );
    assert_eq!(
        thread.reconcile_retirement_until(Instant::now()).await,
        ThreadRecoveryOutcome::Quiescent
    );
    assert!(
        !thread.retirement_reconciled(),
        "no separate receipt is synthesized for original success"
    );
    assert_eq!(ticket.wait().await, complete);

    let mut incomplete = complete;
    incomplete.cleanup = ThreadCleanupOutcome::McpFailed;
    let failed = futures::future::ready(incomplete).boxed().shared();
    failed.clone().await;
    thread
        .retirement
        .lock()
        .unwrap()
        .as_mut()
        .unwrap()
        .completion = failed;
    crate::session::retirement::recovery_tests::inject_completed_mcp_failure(
        &thread.session.cleanup_owner(),
    );
    assert!(
        !thread.retirement_is_quiescent(),
        "incomplete cleanup still requires non-panicked task proof"
    );
    assert_eq!(
        thread.reconcile_retirement_until(Instant::now()).await,
        ThreadRecoveryOutcome::Ineligible
    );
    assert!(!thread.retirement_reconciled());
}
