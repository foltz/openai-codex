use super::*;
use pretty_assertions::assert_eq;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[tokio::test]
async fn live_constructor_cannot_be_reported_as_empty_or_clean() {
    let (resources, guard) = StartupResources::begin();
    assert_eq!(
        resources
            .observe_until(Instant::now() + Duration::from_secs(3))
            .await
            .unwrap(),
        CleanupExecution::AuthorityUnavailable
    );
    assert!(resources.state.lock().unwrap().completion.is_none());
    drop(guard);
    assert_eq!(
        resources
            .observe_until(Instant::now() + Duration::from_secs(3))
            .await
            .unwrap(),
        CleanupExecution::Finished {
            persistence_failed: false
        }
    );
}

#[tokio::test]
async fn cancelled_construction_keeps_real_mcp_root_after_runtime_drop() {
    let (resources, guard) = StartupResources::begin();
    let runtime = resources.create_mcp_runtime(false).unwrap();
    let root = runtime.retirement_owner();
    assert!(root.same_scope(&root.clone()));
    assert!(!root.same_scope(&codex_mcp::McpRuntimeRetirement::default()));
    drop(runtime);
    drop(guard);
    assert_eq!(
        resources
            .observe_until(Instant::now() + Duration::from_secs(3))
            .await
            .unwrap(),
        CleanupExecution::Finished {
            persistence_failed: false
        }
    );
    let report = resources
        .mcp_report
        .lock()
        .unwrap()
        .clone()
        .expect("actual root report");
    assert!(report.are_recorded_workers_drained());
    assert!(report.runtime_shutdowns > 0);
    assert!(!report.workers.is_empty());
}

#[tokio::test]
async fn unregistered_root_is_unproven_instead_of_no_runtime() {
    let (resources, guard) = StartupResources::begin();
    resources.state.lock().unwrap().root = Some(codex_mcp::McpRuntimeRetirement::default());
    drop(guard);
    assert_eq!(
        resources
            .observe_until(Instant::now() + Duration::from_secs(3))
            .await
            .unwrap(),
        CleanupExecution::McpFailed
    );
    let report = resources.mcp_report.lock().unwrap().clone().unwrap();
    assert!(!report.are_recorded_workers_drained());
    assert_eq!(report.runtime_shutdowns, 0);
}

#[tokio::test]
async fn missing_filesystem_cleanup_survives_constructor_cancellation() {
    let (resources, guard) = StartupResources::begin();
    resources.census().record(true, None);
    drop(guard);
    assert_eq!(
        resources
            .observe_until(Instant::now() + Duration::from_secs(3))
            .await
            .unwrap(),
        CleanupExecution::SandboxCleanupUnproven { timed_out: false }
    );
}

#[tokio::test(start_paused = true)]
async fn expired_bound_does_not_start_cleanup_or_create_a_runtime() {
    let (resources, guard) = StartupResources::begin();
    resources.bind_deadline(Instant::now()).unwrap();
    assert!(resources.create_mcp_runtime(false).is_err());
    drop(guard);
    assert_eq!(
        resources
            .observe_until(Instant::now() + Duration::from_secs(30))
            .await
            .unwrap(),
        CleanupExecution::TimedOut
    );
    assert!(resources.state.lock().unwrap().completion.is_none());
    assert!(resources.mcp_report.lock().unwrap().is_none());
}

#[tokio::test]
async fn runtime_birth_is_single_and_closed_after_constructor_scope() {
    let (resources, guard) = StartupResources::begin();
    let runtime = resources.create_mcp_runtime(false).unwrap();
    assert!(resources.create_mcp_runtime(false).is_err());
    drop(guard);
    assert!(resources.create_mcp_runtime(false).is_err());
    drop(runtime);
    assert_eq!(
        resources.observe_legacy().await,
        CleanupExecution::Finished {
            persistence_failed: false
        }
    );
    assert_eq!(
        resources.bind_deadline(Instant::now()),
        Err(DeadlineBindingError::LegacyCleanupStarted)
    );
}

#[tokio::test(start_paused = true)]
async fn ordinary_refusal_keeps_legacy_while_shutdown_edge_stops_its_driver() {
    let (resources, guard) = StartupResources::begin();
    drop(guard);
    let polls = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&polls);
    {
        let mut state = resources.state.lock().unwrap();
        state.phase = Some(Phase::Legacy);
        // Driver-only witness, not injected MCP cleanup evidence.
        state.completion = Some(
            std::future::poll_fn(move |_cx| {
                count.fetch_add(1, Ordering::SeqCst);
                std::task::Poll::Pending
            })
            .boxed()
            .shared(),
        );
    }
    let mut observer = Box::pin(resources.observe_legacy());
    assert!(futures::poll!(observer.as_mut()).is_pending());
    assert_eq!(
        resources.bind_deadline(Instant::now() + Duration::from_secs(1)),
        Err(DeadlineBindingError::LegacyCleanupStarted)
    );
    assert_eq!(*resources.deadline_changed.borrow(), None);
    resources.clamp_shutdown_edge(Instant::now() + Duration::from_secs(2));
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(observer.await, CleanupExecution::TimedOut);
    let stopped = polls.load(Ordering::SeqCst);
    tokio::time::advance(Duration::from_secs(30)).await;
    assert_eq!(resources.observe_legacy().await, CleanupExecution::TimedOut);
    assert_eq!(polls.load(Ordering::SeqCst), stopped);
    assert!(
        resources
            .state
            .lock()
            .unwrap()
            .completion
            .as_ref()
            .unwrap()
            .peek()
            .is_none()
    );
}
