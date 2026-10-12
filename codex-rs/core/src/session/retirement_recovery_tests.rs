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
        !owner.record_reconciliation(codex_mcp::RuntimeTerminationReport {
            connections: Vec::new(),
            tasks: vec![(0, codex_mcp::RuntimeTaskOutcome::Failed)],
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
