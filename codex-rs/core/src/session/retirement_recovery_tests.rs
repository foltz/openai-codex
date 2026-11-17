use super::*;

#[tokio::test]
async fn recovery_cannot_infer_common_or_task_proof_from_an_mcp_failure() {
    let owner = SessionCleanupOwner::default();
    let common = futures::future::ready(CleanupExecution::Finished {
        persistence_failed: false,
    })
    .boxed()
    .shared();
    let tasks = futures::future::ready(crate::tasks::TaskJoinOutcome::Complete { panicked: false })
        .boxed()
        .shared();
    common.clone().await;
    tasks.clone().await;
    {
        let mut state = owner.state.lock().unwrap();
        state.common_completion = Some(common);
        state.task_completion = Some(tasks);
    }
    assert!(owner.recovery_ready());
    for result in [
        CleanupExecution::Finished {
            persistence_failed: true,
        },
        CleanupExecution::Panicked,
        CleanupExecution::CodeModeShutdownFailed,
        CleanupExecution::ConversationShutdownFailed,
    ] {
        let proof = futures::future::ready(result).boxed().shared();
        proof.clone().await;
        owner.state.lock().unwrap().common_completion = Some(proof);
        assert!(!owner.recovery_ready());
    }
    let common = futures::future::ready(CleanupExecution::Finished {
        persistence_failed: false,
    })
    .boxed()
    .shared();
    common.clone().await;
    owner.state.lock().unwrap().common_completion = Some(common);
    let tasks = futures::future::ready(crate::tasks::TaskJoinOutcome::Complete { panicked: true })
        .boxed()
        .shared();
    tasks.clone().await;
    owner.state.lock().unwrap().task_completion = Some(tasks);
    assert!(!owner.recovery_ready());
    let tasks = futures::future::pending::<crate::tasks::TaskJoinOutcome>()
        .boxed()
        .shared();
    owner.state.lock().unwrap().task_completion = Some(tasks);
    assert!(!owner.recovery_ready());
    assert!(
        !owner.record_reconciliation(codex_mcp::RuntimeWorkerDrainReport {
            connections: codex_mcp::RuntimeConnectionReport {
                registration_closed: false,
                registered_at_start: 0,
                registered_at_end: 0,
                connections: Vec::new(),
                generations_admitted: 0,
                generations_settled: 0,
                generations_published: 0,
                constructing_generations: Vec::new(),
                poisoned: true,
            },
            runtime_shutdowns: 0,
            runtime_shutdowns_joined: 0,
            workers: Vec::new(),
            ema: Vec::new(),
            poisoned: true,
        })
    );
    assert!(!owner.reconciled());
}

/// Test-local typed incomplete aggregate; independent handler receipts are unchanged.
pub(crate) fn inject_completed_mcp_failure(owner: &SessionCleanupOwner) {
    let completion = futures::future::ready(CleanupExecution::McpFailed)
        .boxed()
        .shared();
    assert_eq!(
        completion.clone().now_or_never(),
        Some(CleanupExecution::McpFailed)
    );
    owner.state.lock().unwrap().completion = Some(completion);
}

/// Test-local retained Legacy receipt: a completed cleanup recorded while no
/// deadline was bound (the retained-receipt injection of the receipt tests).
pub(crate) fn inject_completed_legacy_receipt(
    owner: &SessionCleanupOwner,
    receipt: CleanupExecution,
) {
    let completion = futures::future::ready(receipt).boxed().shared();
    assert_eq!(completion.clone().now_or_never(), Some(receipt));
    let mut state = owner.state.lock().unwrap();
    assert!(state.deadline.is_none(), "Legacy: no deadline bound");
    state.completion = Some(completion);
}
