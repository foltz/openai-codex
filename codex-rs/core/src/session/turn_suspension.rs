use super::handlers;
use super::retirement::CleanupExecution;
use super::retirement::CleanupMode;
use super::session::Session;
use crate::state::TaskKind;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_protocol::turn_input::SuspendTurnOutcome;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

pub(super) async fn suspend_turn_and_shutdown(
    session: &Arc<Session>,
    submission_id: String,
) -> CodexResult<SuspendTurnOutcome> {
    {
        let active = session.active_turn.lock().await;
        let Some(task) = active.as_ref().and_then(|turn| turn.task.as_ref()) else {
            return Ok(SuspendTurnOutcome::NotActive);
        };
        if task.kind != TaskKind::Regular {
            return Ok(SuspendTurnOutcome::UnsupportedTask);
        }
    }

    // This is a snapshot of currently loaded descendants, not a spawn-admission seal.
    // Previously closed descendants and concurrent future spawns remain best effort.
    if session
        .services
        .agent_control
        .list_live_agent_subtree_thread_ids(session.thread_id)
        .await?
        .len()
        > 1
    {
        return Ok(SuspendTurnOutcome::HasLiveDescendants);
    }

    let live_thread = session
        .live_thread_for_persistence("suspend an unfinished root turn")
        .map_err(|error| CodexErr::Fatal(error.to_string()))?;
    // Flush before canceling execution so a persistence failure leaves the original turn running.
    live_thread.flush().await.map_err(|error| {
        CodexErr::Fatal(format!("flush before root turn suspension failed: {error}"))
    })?;

    // The flush can yield while the active turn completes or changes. Recheck its
    // kind under the same lock used to remove it.
    let mut turn = {
        let mut active = session.active_turn.lock().await;
        let Some(active_turn) = active.as_ref() else {
            return Ok(SuspendTurnOutcome::NotActive);
        };
        let Some(task) = active_turn.task.as_ref() else {
            return Ok(SuspendTurnOutcome::NotActive);
        };
        if task.kind != TaskKind::Regular {
            return Ok(SuspendTurnOutcome::UnsupportedTask);
        }
        active.take().ok_or_else(|| {
            CodexErr::Fatal("accepted root turn suspension had no running turn".to_string())
        })?
    };

    let task = turn.task.take().ok_or_else(|| {
        CodexErr::Fatal("accepted root turn suspension had no running task".to_string())
    })?;
    let turn_id = task.turn_context.sub_id.clone();
    // Normal shutdown records a terminal turn event, preventing another worker from
    // recovering this turn under its original ID. Cancel the task without that event.
    task.cancellation_token.cancel();
    task.turn_context
        .turn_metadata_state
        .cancel_git_enrichment_task();
    let task_handle = task.handle;
    match tokio::time::timeout(
        Duration::from_millis(crate::tasks::GRACEFULL_INTERRUPTION_TIMEOUT_MS),
        task_handle.wait(),
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => {
            warn!(thread_id = %session.thread_id, "suspended turn task exited abnormally");
        }
        Err(_) => {
            warn!(
                thread_id = %session.thread_id,
                "suspended turn task did not stop gracefully; aborting it"
            );
            task_handle.abort();
            let _ = task_handle.wait().await;
        }
    }
    task_handle.detach();
    // Pending accepted input and interactive waiters live only in this process. Handoff
    // intentionally drops that state; persisting or replaying it needs a separate protocol.
    session.input_queue.clear_pending(&turn).await;

    let cleanup = session
        .cleanup_owner()
        .observe_suspension(Arc::clone(session))
        .await;
    if cleanup
        != (CleanupExecution::Finished {
            persistence_failed: false,
        })
    {
        return Err(CodexErr::Fatal(format!(
            "cleanup after root turn suspension failed: {cleanup:?}"
        )));
    }
    // Announce completion only after extension cleanup and writer closure so a
    // replacement worker cannot write the same thread concurrently.
    session
        .deliver_event_raw(Event {
            id: submission_id,
            msg: EventMsg::ShutdownComplete,
        })
        .await;
    Ok(SuspendTurnOutcome::Suspended { turn_id })
}

/// Executed only by the retained cleanup owner, never as a second teardown.
pub(super) async fn cleanup_suspended_session(session: &Arc<Session>) -> CleanupExecution {
    // Preserve suspension's stricter ordering and stop-on-failure policy:
    // stop producers, flush their final history, then close the writer.
    // Even with a bound observer, suspension must join producers *inside* this
    // sequence before persistence. The owner's parallel deadline-bound join
    // alone does not establish that ordering. Keep the preexisting legacy join.
    if let Some(failure) = handlers::shutdown_session_runtime(session, CleanupMode::Legacy).await {
        return failure;
    }
    let result: anyhow::Result<()> = async {
        let live_thread = session.live_thread_for_persistence("close a suspended root turn")?;
        live_thread.flush().await?;
        live_thread.shutdown().await?;
        Ok(())
    }
    .await;
    if let Err(error) = &result {
        warn!(thread_id = %session.thread_id, %error, "suspended root turn persistence cleanup failed");
    }
    CleanupExecution::Finished {
        persistence_failed: result.is_err(),
    }
}
