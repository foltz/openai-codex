use crate::realtime_conversation::handle_audio as handle_realtime_conversation_audio;
use crate::realtime_conversation::handle_close as handle_realtime_conversation_close;
use crate::realtime_conversation::handle_speech as handle_realtime_conversation_speech;
use crate::realtime_conversation::handle_start as handle_realtime_conversation_start;
use crate::realtime_conversation::handle_text as handle_realtime_conversation_text;
use async_channel::Receiver;
use codex_otel::set_parent_from_w3c_trace_context;
use codex_protocol::protocol::Submission;
use tracing::Instrument;
use tracing::debug_span;
use tracing::info_span;

use crate::session::SessionLoopOutcome;
use crate::session::session::Session;
use crate::session::thread_settings;
use crate::session::turn_input;

use crate::config::Config;
use crate::context::ContextualUserFragment;
use crate::context::GuardianApprovedAction;
use crate::review_prompts::resolve_review_request;
use crate::session::spawn_review_thread;
use crate::tasks::CompactTask;
use crate::tasks::UserShellCommandMode;
use crate::tasks::UserShellCommandTask;
use crate::tasks::execute_user_shell_command;
use codex_protocol::protocol::CodexErrorInfo;
use codex_protocol::protocol::ErrorEvent;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::GuardianAssessmentEvent;
use codex_protocol::protocol::GuardianAssessmentStatus;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::RealtimeConversationListVoicesResponseEvent;
use codex_protocol::protocol::RealtimeVoicesList;
use codex_protocol::protocol::ReviewDecision;
use codex_protocol::protocol::ReviewRequest;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_protocol::protocol::TurnAbortReason;
use codex_protocol::protocol::WarningEvent;
use codex_protocol::request_permissions::RequestPermissionsResponse;
use codex_protocol::request_user_input::RequestUserInputResponse;
use codex_thread_store::PersistContext;

use crate::context_manager::is_user_turn_boundary;
use codex_protocol::dynamic_tools::DynamicToolResponse;
use codex_protocol::mcp::RequestId as ProtocolRequestId;
use codex_rmcp_client::ElicitationAction;
use codex_rmcp_client::ElicitationResponse;
use serde_json::Value;
use std::sync::Arc;
use tracing::debug;
use tracing::info;
use tracing::warn;

pub async fn interrupt(sess: &Arc<Session>) {
    sess.interrupt_task().await;
}

pub async fn clean_background_terminals(sess: &Arc<Session>) {
    sess.close_unified_exec_processes().await;
}

pub async fn realtime_conversation_list_voices(sess: &Session, sub_id: String) {
    sess.send_event_raw(Event {
        id: sub_id,
        msg: EventMsg::RealtimeConversationListVoicesResponse(
            RealtimeConversationListVoicesResponseEvent {
                voices: RealtimeVoicesList::builtin(),
            },
        ),
    })
    .await;
}

/// Queues an inter-agent message, then lets the shared pending-work scheduler
/// decide whether an idle session should start a regular turn.
pub async fn inter_agent_communication(
    sess: &Arc<Session>,
    sub_id: String,
    communication: InterAgentCommunication,
    start_options: codex_protocol::turn_input::TurnStartOptions,
    work: Option<Box<dyn codex_protocol::host_turn_work::HostTurnWork>>,
) {
    let trigger_turn = communication.trigger_turn;
    sess.input_queue
        .enqueue_mailbox_with_work(communication, start_options, work);
    crate::agent_communication::emit_agent_communication_receive(&sub_id);
    if trigger_turn || sess.has_outstanding_durable_sleep() {
        sess.maybe_start_turn_for_pending_work_with_sub_id(sub_id)
            .await;
    }
}

pub async fn run_user_shell_command(
    sess: &Arc<Session>,
    sub_id: String,
    command: String,
    timeout_ms: Option<u64>,
) {
    if let Some((turn_context, cancellation_token)) =
        sess.active_turn_context_and_cancellation_token().await
    {
        let session = Arc::clone(sess);
        tokio::spawn(async move {
            execute_user_shell_command(
                session,
                turn_context,
                command,
                timeout_ms,
                cancellation_token,
                UserShellCommandMode::ActiveTurnAuxiliary,
            )
            .await;
        });
        return;
    }

    let turn_context = sess
        .new_turn_with_default_settings(sub_id, Default::default())
        .await;
    sess.spawn_task(
        turn_context,
        Vec::new(),
        UserShellCommandTask::new(command, timeout_ms),
    )
    .await;
}

pub async fn resolve_elicitation(
    sess: &Arc<Session>,
    server_name: String,
    request_id: ProtocolRequestId,
    decision: codex_protocol::approvals::ElicitationAction,
    content: Option<Value>,
    meta: Option<Value>,
) {
    let action = match decision {
        codex_protocol::approvals::ElicitationAction::Accept => ElicitationAction::Accept,
        codex_protocol::approvals::ElicitationAction::Decline => ElicitationAction::Decline,
        codex_protocol::approvals::ElicitationAction::Cancel => ElicitationAction::Cancel,
    };
    let content = match action {
        // Preserve the legacy fallback for clients that only send an action.
        ElicitationAction::Accept => Some(content.unwrap_or_else(|| serde_json::json!({}))),
        ElicitationAction::Decline | ElicitationAction::Cancel => None,
        _ => None,
    };
    let response = ElicitationResponse {
        action,
        content,
        meta,
    };
    let request_id = match request_id {
        ProtocolRequestId::String(value) => {
            rmcp::model::NumberOrString::String(std::sync::Arc::from(value))
        }
        ProtocolRequestId::Integer(value) => rmcp::model::NumberOrString::Number(value),
    };
    if let Err(err) = sess
        .resolve_elicitation(server_name, request_id, response)
        .await
    {
        warn!(
            error = %err,
            "failed to resolve elicitation request in session"
        );
    }
}

/// Propagate a user's exec approval decision to the session.
/// Also optionally applies an execpolicy amendment.
pub async fn exec_approval(
    sess: &Arc<Session>,
    approval_id: String,
    turn_id: Option<String>,
    decision: ReviewDecision,
) {
    let event_turn_id = turn_id.unwrap_or_else(|| approval_id.clone());
    if let ReviewDecision::ApprovedExecpolicyAmendment {
        proposed_execpolicy_amendment,
    } = &decision
        && let Err(err) = sess
            .persist_execpolicy_amendment(proposed_execpolicy_amendment)
            .await
    {
        let message = format!("Failed to apply execpolicy amendment: {err}");
        tracing::warn!("{message}");
        let warning = EventMsg::Warning(WarningEvent { message });
        sess.send_event_raw(Event {
            id: event_turn_id.clone(),
            msg: warning,
        })
        .await;
    }
    match decision {
        ReviewDecision::Abort => {
            sess.interrupt_task().await;
        }
        other => sess.notify_approval(&approval_id, other).await,
    }
}

pub async fn patch_approval(sess: &Arc<Session>, id: String, decision: ReviewDecision) {
    match decision {
        ReviewDecision::Abort => {
            sess.interrupt_task().await;
        }
        other => sess.notify_approval(&id, other).await,
    }
}

pub async fn request_user_input_response(
    sess: &Arc<Session>,
    id: String,
    response: RequestUserInputResponse,
) {
    sess.notify_user_input_response(&id, response).await;
}

pub async fn request_permissions_response(
    sess: &Arc<Session>,
    id: String,
    response: RequestPermissionsResponse,
) {
    sess.notify_request_permissions_response(&id, response)
        .await;
}

pub async fn dynamic_tool_response(sess: &Arc<Session>, id: String, response: DynamicToolResponse) {
    sess.notify_dynamic_tool_response(&id, response).await;
}

pub fn refresh_mcp_servers(sess: &Session) {
    sess.services.mcp_runtime.reconnect_on_next_refresh();
    sess.request_mcp_runtime_refresh();
}

pub async fn reload_user_config(sess: &Arc<Session>) {
    sess.reload_user_config_layer().await;
}

pub async fn compact(sess: &Arc<Session>, sub_id: String) -> bool {
    // Stop the old turn before the compact task picks up the next turn's environments.
    sess.abort_all_tasks(TurnAbortReason::Replaced).await;
    let turn_context = sess
        .new_turn_with_default_settings(sub_id, Default::default())
        .await;

    sess.try_spawn_task(turn_context, Vec::new(), CompactTask)
        .await
        .is_ok()
}

pub(super) async fn persist_thread_memory_mode_update(
    sess: &Arc<Session>,
    mode: ThreadMemoryMode,
) -> anyhow::Result<()> {
    let live_thread = sess.live_thread_for_persistence("update thread memory mode")?;
    live_thread.persist(PersistContext::Standard).await?;
    live_thread.flush().await?;
    live_thread
        .update_memory_mode(mode, /*include_archived*/ false)
        .await?;
    live_thread.flush().await?;
    Ok(())
}

/// Persists thread-level memory mode metadata for the active session.
///
/// This does not involve the model and only affects whether the thread is
/// eligible for future memory generation.
pub async fn set_thread_memory_mode(sess: &Arc<Session>, sub_id: String, mode: ThreadMemoryMode) {
    if let Err(err) = persist_thread_memory_mode_update(sess, mode).await {
        warn!("Failed to persist thread memory mode update to rollout: {err}");
        let event = Event {
            id: sub_id,
            msg: EventMsg::Error(ErrorEvent {
                misalignment: None,
                message: err.to_string(),
                codex_error_info: Some(CodexErrorInfo::Other),
            }),
        };
        sess.send_event_raw(event).await;
    }
}

pub(super) async fn shutdown_session_runtime(
    sess: &Arc<Session>,
    mode: super::retirement::CleanupMode,
) -> Option<super::retirement::CleanupExecution> {
    let mut failure = None;
    sess.close_task_admission().await;
    if let Some(startup_prewarm) = sess.take_session_startup_prewarm().await {
        startup_prewarm.abort().await;
    }
    if let Err(error) = sess.conversation.shutdown().await {
        warn!("failed to shutdown realtime conversation: {error}");
        failure = Some(super::retirement::CleanupExecution::ConversationShutdownFailed);
    }
    sess.abort_all_tasks(TurnAbortReason::Interrupted).await;
    let shell_snapshot_prewarm = sess.state.lock().await.shell_snapshot_prewarm.take();
    if let Some(shell_snapshot_prewarm) = shell_snapshot_prewarm {
        shell_snapshot_prewarm.abort();
        let _ = shell_snapshot_prewarm.await;
    }
    if let super::retirement::CleanupMode::Legacy = mode {
        let task_result = sess
            .task_joins
            .shutdown_until(tokio::time::Instant::now() + std::time::Duration::from_secs(30))
            .await;
        if !matches!(task_result, crate::tasks::TaskJoinOutcome::Complete { .. }) {
            failure.get_or_insert(super::retirement::CleanupExecution::TaskJoinFailed);
            warn!(
                ?task_result,
                "session task joins were not observed during legacy cleanup"
            );
        }
    }
    sess.hooks().shutdown().await;
    sess.async_hook_results.close();
    while sess.async_hook_results.try_recv().is_ok() {}
    sess.services
        .unified_exec_manager
        .terminate_all_processes()
        .await;
    if let Err(err) = sess.services.code_mode_service.shutdown().await {
        warn!("failed to shutdown code mode session: {err}");
        failure.get_or_insert(super::retirement::CleanupExecution::CodeModeShutdownFailed);
    }
    let prewarm_outcome = sess.stop_mcp_prewarm_worker().await;
    if prewarm_outcome != SessionLoopOutcome::Normal {
        warn!(?prewarm_outcome, "MCP prewarm worker stopped unexpectedly");
        failure.get_or_insert(super::retirement::CleanupExecution::McpPrewarmFailed);
    }
    if let super::retirement::CleanupMode::Legacy = mode {
        let _refresh = sess.mcp_refresh.acquire().await;
        sess.mcp_refresh.close();
        let report = sess
            .services
            .mcp_runtime
            .shutdown_until(tokio::time::Instant::now() + std::time::Duration::from_secs(30))
            .await;
        if !report.is_complete() {
            warn!(
                ?report,
                "MCP runtime retirement was not acknowledged during legacy cleanup"
            );
            failure.get_or_insert(super::retirement::CleanupExecution::McpFailed);
        }
    }

    crate::hook_runtime::run_session_end_hooks(sess).await;
    emit_thread_stop_lifecycle(sess).await;
    failure
}

async fn emit_thread_stop_lifecycle(sess: &Session) {
    for contributor in sess.services.extensions.thread_lifecycle_contributors() {
        contributor
            .on_thread_stop(codex_extension_api::ThreadStopInput {
                session_store: &sess.services.session_extension_data,
                thread_store: &sess.services.thread_extension_data,
            })
            .await;
    }
}

pub(super) async fn cleanup_session(
    sess: &Arc<Session>,
    mode: super::retirement::CleanupMode,
) -> super::retirement::CleanupExecution {
    let failure = shutdown_session_runtime(sess, mode).await;
    info!("Shutting down Codex instance");
    let history = sess.clone_history().await;
    let turn_count = history
        .raw_items()
        .filter(|item| is_user_turn_boundary(item))
        .count();
    sess.services.session_telemetry.counter(
        "codex.conversation.turn.count",
        i64::try_from(turn_count).unwrap_or(0),
        &[],
    );
    let persistence_failed = if let Some(live_thread) = sess.live_thread() {
        let failed_initialization = sess
            .failed_initialization_persistence
            .load(std::sync::atomic::Ordering::Acquire);
        let result = if failed_initialization {
            live_thread.discard().await
        } else {
            live_thread.shutdown().await
        };
        if result.is_err() {
            let persistence_operation = if failed_initialization {
                "discard_failed_initialization"
            } else {
                "shutdown"
            };
            if failed_initialization {
                warn!(
                    persistence_operation,
                    thread_id = %sess.thread_id,
                    "failed initialization persistence disposal incomplete"
                );
            } else {
                warn!(
                    persistence_operation,
                    thread_id = %sess.thread_id,
                    "thread persistence disposal incomplete"
                );
            }
            true
        } else {
            false
        }
    } else {
        false
    };
    failure.unwrap_or(super::retirement::CleanupExecution::Finished { persistence_failed })
}

pub async fn shutdown(sess: &Arc<Session>, sub_id: String) -> bool {
    let cleanup = sess.cleanup_owner().observe(Arc::clone(sess)).await;
    let persistence_failed = match cleanup {
        super::retirement::CleanupExecution::Finished { persistence_failed } => persistence_failed,
        super::retirement::CleanupExecution::Panicked => {
            // Preserve panic disposition independently of the sticky receipt.
            std::panic::resume_unwind(Box::new("session cleanup panicked"));
        }
        super::retirement::CleanupExecution::TimedOut
        | super::retirement::CleanupExecution::McpFailed
        | super::retirement::CleanupExecution::McpPrewarmFailed
        | super::retirement::CleanupExecution::TaskJoinFailed
        | super::retirement::CleanupExecution::ConversationShutdownFailed
        | super::retirement::CleanupExecution::CodeModeShutdownFailed
        | super::retirement::CleanupExecution::AuthorityUnavailable => {
            warn!(
                ?cleanup,
                "session cleanup not observed; withholding ShutdownComplete"
            );
            return true;
        }
    };
    // Gracefully flush and shutdown thread persistence on session end so tests
    // that inspect durable state do not race with the background writer.
    if persistence_failed {
        let event = Event {
            id: sub_id.clone(),
            msg: EventMsg::Error(ErrorEvent {
                misalignment: None,
                message: "Failed to shutdown thread persistence".to_string(),
                codex_error_info: Some(CodexErrorInfo::Other),
            }),
        };
        sess.send_event_raw(event).await;
    }

    let event = Event {
        id: sub_id,
        msg: EventMsg::ShutdownComplete,
    };
    sess.services
        .rollout_thread_trace
        .record_protocol_event(&event.msg);
    sess.deliver_event_raw(event).await;
    sess.services
        .rollout_thread_trace
        .record_ended(codex_rollout_trace::RolloutStatus::Completed);
    true
}

pub async fn review(
    sess: &Arc<Session>,
    config: &Arc<Config>,
    sub_id: String,
    review_request: ReviewRequest,
) -> bool {
    let turn_context = sess
        .new_turn_with_default_settings(sub_id.clone(), Default::default())
        .await;
    sess.maybe_emit_model_warnings_for_turn(turn_context.as_ref())
        .await;
    #[allow(deprecated)]
    match resolve_review_request(review_request, &turn_context.cwd) {
        Ok(resolved) => {
            spawn_review_thread(
                Arc::clone(sess),
                Arc::clone(config),
                turn_context.clone(),
                sub_id,
                resolved,
            )
            .await
        }
        Err(err) => {
            let event = Event {
                id: sub_id,
                msg: EventMsg::Error(ErrorEvent {
                    misalignment: None,
                    message: err.to_string(),
                    codex_error_info: Some(CodexErrorInfo::Other),
                }),
            };
            sess.send_event(&turn_context, event.msg).await;
            false
        }
    }
}

pub(super) async fn submission_loop(
    sess: Arc<Session>,
    config: Arc<Config>,
    rx_sub: Receiver<Submission>,
    submissions: Option<super::submission::SubmissionDispatch>,
) {
    // To break out of this loop, send Op::Shutdown.
    let _cleanup_owner = sess.cleanup_owner();
    let mut shutdown_received = false;
    // As with the former finishing-task caller, birth can interleave with
    // handlers; install-time task admission remains the shutdown fence.
    let mut mailbox_start: Option<futures::future::BoxFuture<'_, ()>> = None;
    // Retain the retry observer across select iterations. Dropping a losing
    // select arm must not discard the host's pending reopen signal.
    let mut mailbox_retry = Box::pin(sess.input_queue.work_retry.wait());
    loop {
        if rx_sub.is_closed() && rx_sub.is_empty() {
            break;
        }
        let sub = tokio::select! {
            _ = &mut mailbox_retry => {
                mailbox_retry = Box::pin(sess.input_queue.work_retry.wait());
                sess.input_queue.completion_wake.notify_one();
                continue;
            }
            sub = rx_sub.recv() => match sub {
                Ok(sub) => sub,
                Err(_) => break,
            },
            _ = sess.input_queue.completion_wake.notified(), if mailbox_start.is_none() => {
                mailbox_start = Some(Box::pin(sess.maybe_start_turn_for_pending_work()));
                continue;
            }
            _ = async { mailbox_start.as_mut().expect("guarded mailbox start").await }, if mailbox_start.is_some() => {
                mailbox_start = None;
                continue;
            }
        };
        let _dispatch = submissions
            .as_ref()
            .map(super::submission::SubmissionDispatch::begin);
        if matches!(sub.op, Op::ResolveElicitation { .. }) {
            debug!(submission_id = %sub.id, operation = sub.op.kind(), "Submission");
        } else {
            debug!(?sub, "Submission");
        }
        let dispatch_span = submission_dispatch_span(&sub);
        let dispatch = async {
            match sub.op {
                Op::HostTurn { action, mut work } => {
                    work.bind_submission(&sub.id);
                    let started = match action {
                        codex_protocol::host_turn_work::HostTurnAction::Compact => {
                            compact(&sess, sub.id.clone()).await
                        }
                        codex_protocol::host_turn_work::HostTurnAction::Review(request) => {
                            review(&sess, &config, sub.id.clone(), request).await
                        }
                    };
                    if started {
                        work.retain_until_terminal();
                    }
                    false
                }
                Op::Interrupt => {
                    interrupt(&sess).await;
                    false
                }
                Op::CleanBackgroundTerminals => {
                    clean_background_terminals(&sess).await;
                    false
                }
                Op::RealtimeConversationStart(params) => {
                    if let Err(err) =
                        handle_realtime_conversation_start(&sess, sub.id.clone(), params).await
                    {
                        sess.send_event_raw(Event {
                            id: sub.id.clone(),
                            msg: EventMsg::Error(ErrorEvent {
                                misalignment: None,
                                message: err.to_string(),
                                codex_error_info: Some(CodexErrorInfo::Other),
                            }),
                        })
                        .await;
                    }
                    false
                }
                Op::RealtimeConversationAudio(params) => {
                    handle_realtime_conversation_audio(&sess, sub.id.clone(), params).await;
                    false
                }
                Op::RealtimeConversationText(params) => {
                    handle_realtime_conversation_text(&sess, sub.id.clone(), params).await;
                    false
                }
                Op::RealtimeConversationSpeech(params) => {
                    handle_realtime_conversation_speech(&sess, sub.id.clone(), params).await;
                    false
                }
                Op::RealtimeConversationClose => {
                    handle_realtime_conversation_close(&sess, sub.id.clone()).await;
                    false
                }
                Op::RealtimeConversationListVoices => {
                    realtime_conversation_list_voices(&sess, sub.id.clone()).await;
                    false
                }
                Op::TurnInput {
                    request,
                    mode,
                    mut host_work,
                    reply,
                } => {
                    if let Some(work) = &mut host_work {
                        work.bind_submission(&sub.id);
                    }
                    let result = turn_input::handle(&sess, *request, mode, sub.id.clone()).await;
                    if matches!(&result, Ok(codex_protocol::turn_input::TurnInputSubmission::Started { .. }))
                        && let Some(work) = host_work.take()
                    {
                        work.retain_until_terminal();
                    }
                    let _ = reply.send(result);
                    false
                }
                Op::RecoverTurn {
                    thread_settings,
                    start_options,
                    mut host_work,
                    reply,
                } => {
                    if let Some(work) = &mut host_work {
                        work.bind_submission(&sub.id);
                    }
                    let result = turn_input::handle_recovery(
                        &sess,
                        thread_settings,
                        start_options,
                        sub.id.clone(),
                    )
                    .await;
                    if matches!(&result, Ok(codex_protocol::turn_input::TurnInputSubmission::Started { .. }))
                        && let Some(work) = host_work.take()
                    {
                        work.retain_until_terminal();
                    }
                    let _ = reply.send(result);
                    false
                }
                Op::SuspendTurnAndShutdown { reply } => {
                    let result =
                        super::turn_suspension::suspend_turn_and_shutdown(&sess, sub.id.clone())
                            .await;
                    // Exit only after history is durable and its writer has closed; an error
                    // must leave responsibility for the thread with the current worker.
                    let should_exit = matches!(
                        &result,
                        Ok(codex_protocol::turn_input::SuspendTurnOutcome::Suspended { .. })
                    );
                    let _ = reply.send(result);
                    should_exit
                }
                Op::ThreadSettings { thread_settings } => {
                    thread_settings::update(&sess, sub.id.clone(), thread_settings).await;
                    false
                }
                Op::TurnSettings {
                    turn_id,
                    update,
                    reply,
                } => {
                    let outcome = sess.apply_turn_settings(&turn_id, update).await;
                    let _ = reply.send(outcome);
                    false
                }
                Op::InterAgentCommunication {
                    communication,
                    start_options,
                    work,
                } => {
                    inter_agent_communication(&sess, sub.id.clone(), communication, start_options, work)
                        .await;
                    false
                }
                Op::ExecApproval {
                    id: approval_id,
                    turn_id,
                    decision,
                } => {
                    exec_approval(&sess, approval_id, turn_id, decision).await;
                    false
                }
                Op::PatchApproval { id, decision } => {
                    patch_approval(&sess, id, decision).await;
                    false
                }
                Op::UserInputAnswer { id, response } => {
                    request_user_input_response(&sess, id, response).await;
                    false
                }
                Op::RequestPermissionsResponse { id, response } => {
                    request_permissions_response(&sess, id, response).await;
                    false
                }
                Op::DynamicToolResponse { id, response } => {
                    dynamic_tool_response(&sess, id, response).await;
                    false
                }
                Op::RefreshMcpServers => {
                    refresh_mcp_servers(&sess);
                    false
                }
                Op::ReloadUserConfig => {
                    reload_user_config(&sess).await;
                    false
                }
                Op::Compact => {
                    compact(&sess, sub.id.clone()).await;
                    false
                }
                Op::SetThreadMemoryMode { mode } => {
                    set_thread_memory_mode(&sess, sub.id.clone(), mode).await;
                    false
                }
                Op::RunUserShellCommand {
                    command,
                    timeout_ms,
                } => {
                    run_user_shell_command(&sess, sub.id.clone(), command, timeout_ms).await;
                    false
                }
                Op::ResolveElicitation {
                    server_name,
                    request_id,
                    decision,
                    content,
                    meta,
                } => {
                    resolve_elicitation(&sess, server_name, request_id, decision, content, meta)
                        .await;
                    false
                }
                Op::Shutdown => shutdown(&sess, sub.id.clone()).await,
                Op::Review { review_request } => {
                    review(&sess, &config, sub.id.clone(), review_request).await;
                    false
                }
                Op::ApproveGuardianDeniedAction { event } => {
                    approve_guardian_denied_action(&sess, event).await;
                    false
                }
                _ => false, // Ignore unknown ops; enum is non_exhaustive to allow extensions.
            }
        }
        .instrument(dispatch_span);
        tokio::pin!(dispatch);
        // Keep discovery/start progressing even when a control handler waits
        // on a lock it holds. Conversely, discovery never blocks dispatch.
        let should_exit = loop {
            tokio::select! {
                result = &mut dispatch => break result,
                _ = &mut mailbox_retry => {
                    mailbox_retry = Box::pin(sess.input_queue.work_retry.wait());
                    sess.input_queue.completion_wake.notify_one();
                }
                _ = async { mailbox_start.as_mut().expect("guarded mailbox start").await }, if mailbox_start.is_some() => {
                    mailbox_start = None;
                }
            }
        };
        if should_exit {
            shutdown_received = true;
            break;
        }
    }
    // Release any suspended preparation before cleanup acquires its locks.
    drop(mailbox_start);
    drop(mailbox_retry);
    // If the submission loop exits because the channel closed without an
    // explicit shutdown op, still run session teardown.
    if !shutdown_received {
        let cleanup = sess.cleanup_owner().observe(Arc::clone(&sess)).await;
        if cleanup == super::retirement::CleanupExecution::Panicked {
            std::panic::resume_unwind(Box::new("session cleanup panicked"));
        }
        if cleanup
            != (super::retirement::CleanupExecution::Finished {
                persistence_failed: false,
            })
        {
            warn!(
                ?cleanup,
                "session cleanup failed after submission channel closed"
            );
        }
    }
    debug!("Agent loop exited");
}

async fn approve_guardian_denied_action(sess: &Arc<Session>, event: GuardianAssessmentEvent) {
    if event.status != GuardianAssessmentStatus::Denied {
        warn!(
            review_id = event.id.as_str(),
            "ignoring approval for non-denied Guardian assessment"
        );
        return;
    }

    let approved_action = serde_json::json!({
        "action": &event.action,
        "outcome": "allowed",
    });
    let approved_action_json = match serde_json::to_string_pretty(&approved_action) {
        Ok(approved_action_json) => approved_action_json,
        Err(error) => {
            warn!(%error, review_id = event.id.as_str(), "failed to serialize approved Guardian action");
            return;
        }
    };
    let items = vec![ContextualUserFragment::into(GuardianApprovedAction::new(
        approved_action_json,
    ))];

    sess.inject_no_new_turn(items, /*current_turn_context*/ None)
        .await;
}

pub(super) fn submission_dispatch_span(sub: &Submission) -> tracing::Span {
    let op_name = sub.op.kind();
    let span_name = format!("op.dispatch.{op_name}");
    let dispatch_span = match &sub.op {
        Op::RealtimeConversationAudio(_) => {
            debug_span!(
                "submission_dispatch",
                otel.name = span_name.as_str(),
                submission.id = sub.id.as_str(),
                codex.op = op_name
            )
        }
        _ => info_span!(
            "submission_dispatch",
            otel.name = span_name.as_str(),
            submission.id = sub.id.as_str(),
            codex.op = op_name
        ),
    };
    if let Some(trace) = sub.trace.as_ref()
        && !set_parent_from_w3c_trace_context(&dispatch_span, trace)
    {
        warn!(
            submission.id = sub.id.as_str(),
            "ignoring invalid submission trace carrier"
        );
    }
    dispatch_span
}
