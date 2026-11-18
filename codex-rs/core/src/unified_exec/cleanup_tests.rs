use super::*;
use codex_protocol::exec_output::SandboxCleanupState;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use tokio::sync::Notify;

fn complete() -> SandboxCleanupSummary {
    SandboxCleanupSummary {
        state: SandboxCleanupState::Complete,
        reaped: 0,
        preserved_unproven: 0,
        host_owned: 0,
        handed_off: 0,
        timed_out: 0,
        joined: true,
    }
}

#[test]
fn non_linux_sandbox_launch_has_no_custody_or_census_operation() {
    let census = Arc::new(SandboxCleanupCensus::default());
    let scope = CleanupDeadlineScope::new();
    assert!(
        SandboxCleanup::for_launch(SandboxType::None, Some(scope), Some(Arc::clone(&census)))
            .unwrap()
            .is_none()
    );
    assert_eq!(census.totals().operations, 0);
    assert_eq!(census.totals().pending_receipt, 0);
}

#[tokio::test]
async fn cancelled_observer_resumes_the_original_collection_and_records_once() {
    let census = Arc::new(SandboxCleanupCensus::default());
    let operation = Arc::new(Mutex::new(Some(SandboxCleanupOperation::new(
        Arc::clone(&census),
        true,
    ))));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let result = retained_collection(
        {
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            let calls = Arc::clone(&calls);
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                started.notify_one();
                release.notified().await;
                complete()
            }
        },
        Arc::clone(&operation),
    );
    let observer = tokio::spawn(result.clone());
    started.notified().await;
    observer.abort();
    assert!(observer.await.unwrap_err().is_cancelled());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(census.totals().has_unproven());
    release.notify_one();
    assert_eq!(result.clone().await, complete());
    assert_eq!(result.await, complete());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let totals = census.totals();
    assert_eq!(
        (totals.operations, totals.complete, totals.pending_receipt),
        (1, 1, 0)
    );
    assert!(!totals.has_unproven());
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn owner_drop_debt_cannot_be_erased_by_a_late_collector() {
    let census = Arc::new(SandboxCleanupCensus::default());
    let cleanup =
        SandboxCleanup::for_launch(SandboxType::LinuxSeccomp, None, Some(Arc::clone(&census)))
            .unwrap()
            .unwrap();
    let release = Arc::new(Notify::new());
    let result = retained_collection(
        {
            let release = Arc::clone(&release);
            async move {
                release.notified().await;
                complete()
            }
        },
        Arc::clone(&cleanup.operation),
    );
    {
        let mut state = cleanup.state.lock().unwrap();
        // Substitute only the collector: this cell tests the real owner's
        // Drop while an original collection can still produce a receipt.
        drop(state.custody.take());
        state.result = Some(result.clone());
    }
    drop(cleanup);
    assert_eq!(census.totals().missing_receipt, 1);
    release.notify_one();
    assert_eq!(result.await, complete());
    let totals = census.totals();
    assert_eq!(
        (
            totals.operations,
            totals.complete,
            totals.missing_receipt,
            totals.pending_receipt
        ),
        (1, 0, 1, 0)
    );
    assert!(totals.has_unproven());
}

#[cfg(target_os = "linux")]
#[test]
fn already_latched_scope_reaches_the_local_launch_argv_and_drop_census() {
    let census = Arc::new(SandboxCleanupCensus::default());
    let scope = CleanupDeadlineScope::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    scope.tighten(deadline);
    let mut cleanup = SandboxCleanup::for_launch(
        SandboxType::LinuxSeccomp,
        Some(scope),
        Some(Arc::clone(&census)),
    )
    .unwrap()
    .unwrap();
    let mut command = vec![
        "codex-linux-sandbox".to_string(),
        "--".to_string(),
        "true".to_string(),
    ];
    cleanup.attach(&mut command).unwrap();
    assert!(
        command
            .iter()
            .any(|arg| arg == "--cleanup-deadline-monotonic-ns")
    );
    assert_eq!(
        cleanup
            .state
            .lock()
            .unwrap()
            .custody
            .as_ref()
            .unwrap()
            .deadline(),
        Some(deadline)
    );
    assert_eq!(cleanup.inherited_fds().len(), 2);
    assert_eq!(census.totals().pending_receipt, 1);
    cleanup.terminate();
    assert_eq!(
        cleanup
            .state
            .lock()
            .unwrap()
            .custody
            .as_ref()
            .unwrap()
            .deadline(),
        Some(deadline)
    );
    drop(cleanup);
    assert_eq!(census.totals().missing_receipt, 1);
}

#[cfg(target_os = "linux")]
#[test]
fn reported_spawn_failure_records_no_launch_without_collecting_a_helper() {
    let census = Arc::new(SandboxCleanupCensus::default());
    let mut cleanup =
        SandboxCleanup::for_launch(SandboxType::LinuxSeccomp, None, Some(Arc::clone(&census)))
            .unwrap()
            .unwrap();
    cleanup.spawned(None);
    cleanup.not_launched();
    drop(cleanup);
    let totals = census.totals();
    assert_eq!(
        (totals.no_launch, totals.operations, totals.pending_receipt),
        (1, 0, 0)
    );
    assert!(!totals.has_unproven());
}
