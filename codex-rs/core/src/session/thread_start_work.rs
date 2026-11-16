//! Constructor-time callback authority, independent of reusable extension stores.
use codex_extension_api::ThreadLifecycleContributor;
use codex_extension_api::ThreadStartInput;
use codex_mcp::McpAttemptAccess;
use codex_mcp::McpAttemptRefused;
use std::sync::Arc;

pub(super) async fn emit_thread_start_lifecycle<C: Send + Sync>(
    contributors: &[Arc<dyn ThreadLifecycleContributor<C>>],
    input: ThreadStartInput<'_, C>,
) {
    if contributors.is_empty() {
        return;
    }
    // One-shot notifications carry a plain refusal, never a retry future.
    let captured = match input.mcp_access {
        Ok(McpAttemptAccess::Admitted(work)) => work
            .derive_attempt()
            .map(Some)
            .map_err(|_| McpAttemptRefused),
        Ok(McpAttemptAccess::Unscoped) => Ok(None),
        Err(_) => Err(McpAttemptRefused),
    };
    let (work, refused) = match captured {
        Ok(work) => (work, false),
        Err(_) => (None, true),
    };
    for contributor in contributors {
        contributor
            .on_thread_start(ThreadStartInput {
                mcp_access: if refused {
                    Err(McpAttemptRefused)
                } else {
                    Ok(McpAttemptAccess::from_work(work.as_deref()))
                },
                config: input.config,
                session_source: input.session_source,
                persistent_thread_state_available: input.persistent_thread_state_available,
                environments: input.environments,
                mcp_resource_client: input.mcp_resource_client.clone(),
                extension_metrics: input.extension_metrics.clone(),
                session_store: input.session_store,
                thread_store: input.thread_store,
            })
            .await;
    }
}

#[cfg(test)]
#[path = "thread_start_work_tests.rs"]
mod tests;
