//! Background model warmup shared by startup and idle-thread resume.
//! One scheduled task owns the prepared client session until the next regular
//! turn consumes it; shutdown and turn cancellation use the same handoff.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use futures::FutureExt;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing::info;
use tracing::instrument;
use tracing::trace_span;
use tracing::warn;

use crate::client::ModelClientSession;
use crate::responses_metadata::CodexResponsesRequestKind;
use crate::session::INITIAL_SUBMIT_ID;
use crate::session::RequestEffortUsage;
use crate::session::session::Session;
use crate::session::turn::build_prompt;
use codex_features::Feature;
use codex_otel::STARTUP_PREWARM_AGE_AT_FIRST_TURN_METRIC;
use codex_otel::STARTUP_PREWARM_DURATION_METRIC;
use codex_otel::SessionTelemetry;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::models::BaseInstructions;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;

#[derive(Clone, Copy, Debug)]
pub(crate) enum PrewarmInput {
    Base,
    History,
}

impl PrewarmInput {
    fn as_str(self) -> &'static str {
        match self {
            Self::Base => "base",
            Self::History => "history",
        }
    }
}

pub(crate) struct SessionStartupPrewarmHandle {
    task: crate::tasks::TaskAbortHandle,
    result: tokio::sync::oneshot::Receiver<CodexResult<ModelClientSession>>,
    started_at: Instant,
    timeout: Duration,
}

pub(crate) enum SessionStartupPrewarmResolution {
    Cancelled,
    Ready(Box<ModelClientSession>),
    Unavailable {
        status: &'static str,
        prewarm_duration: Option<Duration>,
    },
}

impl SessionStartupPrewarmHandle {
    pub(crate) fn new(
        task: crate::tasks::TaskAbortHandle,
        result: tokio::sync::oneshot::Receiver<CodexResult<ModelClientSession>>,
        started_at: Instant,
        timeout: Duration,
    ) -> Self {
        Self {
            task,
            result,
            started_at,
            timeout,
        }
    }

    pub(crate) async fn abort(self) {
        self.task.abort();
        if !self.task.wait().await {
            warn!("startup websocket prewarm task panicked");
        }
    }

    #[instrument(name = "startup_prewarm.resolve", level = "trace", skip_all)]
    async fn resolve(
        self,
        session_telemetry: &SessionTelemetry,
        cancellation_token: &CancellationToken,
    ) -> SessionStartupPrewarmResolution {
        let resolve_started_at = Instant::now();
        let Self {
            task,
            mut result,
            started_at,
            timeout,
        } = self;
        let age_at_first_turn = started_at.elapsed();
        let remaining = timeout.saturating_sub(age_at_first_turn);

        let resolution = if task.is_finished() {
            Self::resolution_from_join_result(result.await, started_at)
        } else {
            match tokio::select! {
                _ = cancellation_token.cancelled() => None,
                result = tokio::time::timeout(remaining, &mut result) => Some(result),
            } {
                Some(Ok(result)) => Self::resolution_from_join_result(result, started_at),
                Some(Err(_elapsed)) => {
                    task.abort();
                    info!("startup websocket prewarm timed out before the first turn could use it");
                    SessionStartupPrewarmResolution::Unavailable {
                        status: "timed_out",
                        prewarm_duration: Some(started_at.elapsed()),
                    }
                }
                None => {
                    task.abort();
                    session_telemetry.record_startup_phase(
                        "startup_prewarm_resolve",
                        resolve_started_at.elapsed(),
                        Some("cancelled"),
                    );
                    session_telemetry.record_duration(
                        STARTUP_PREWARM_AGE_AT_FIRST_TURN_METRIC,
                        age_at_first_turn,
                        &[("status", "cancelled")],
                    );
                    session_telemetry.record_duration(
                        STARTUP_PREWARM_DURATION_METRIC,
                        started_at.elapsed(),
                        &[("status", "cancelled")],
                    );
                    return SessionStartupPrewarmResolution::Cancelled;
                }
            }
        };
        let status = match &resolution {
            SessionStartupPrewarmResolution::Cancelled => "cancelled",
            SessionStartupPrewarmResolution::Ready(_) => "ready",
            SessionStartupPrewarmResolution::Unavailable { status, .. } => status,
        };
        session_telemetry.record_startup_phase(
            "startup_prewarm_resolve",
            resolve_started_at.elapsed(),
            Some(status),
        );

        match resolution {
            SessionStartupPrewarmResolution::Cancelled => {
                SessionStartupPrewarmResolution::Cancelled
            }
            SessionStartupPrewarmResolution::Ready(prewarmed_session) => {
                session_telemetry.record_duration(
                    STARTUP_PREWARM_AGE_AT_FIRST_TURN_METRIC,
                    age_at_first_turn,
                    &[("status", "consumed")],
                );
                SessionStartupPrewarmResolution::Ready(prewarmed_session)
            }
            SessionStartupPrewarmResolution::Unavailable {
                status,
                prewarm_duration,
            } => {
                session_telemetry.record_duration(
                    STARTUP_PREWARM_AGE_AT_FIRST_TURN_METRIC,
                    age_at_first_turn,
                    &[("status", status)],
                );
                if let Some(prewarm_duration) = prewarm_duration {
                    session_telemetry.record_duration(
                        STARTUP_PREWARM_DURATION_METRIC,
                        prewarm_duration,
                        &[("status", status)],
                    );
                }
                SessionStartupPrewarmResolution::Unavailable {
                    status,
                    prewarm_duration,
                }
            }
        }
    }

    fn resolution_from_join_result(
        result: std::result::Result<
            CodexResult<ModelClientSession>,
            tokio::sync::oneshot::error::RecvError,
        >,
        started_at: Instant,
    ) -> SessionStartupPrewarmResolution {
        match result {
            Ok(Ok(prewarmed_session)) => {
                SessionStartupPrewarmResolution::Ready(Box::new(prewarmed_session))
            }
            Ok(Err(err)) => {
                warn!("startup websocket prewarm setup failed: {err:#}");
                SessionStartupPrewarmResolution::Unavailable {
                    status: "failed",
                    prewarm_duration: None,
                }
            }
            Err(err) => {
                warn!("startup websocket prewarm setup join failed: {err}");
                SessionStartupPrewarmResolution::Unavailable {
                    status: "join_failed",
                    prewarm_duration: Some(started_at.elapsed()),
                }
            }
        }
    }
}

impl Session {
    /// Register auxiliary startup work under the same admission lock as turn tasks.
    #[cfg(test)]
    pub(crate) async fn spawn_startup_auxiliary(
        &self,
        work: impl std::future::Future<Output = ()> + Send + 'static,
    ) {
        let active_turn = self.active_turn.lock().await;
        if self
            .task_admission_closed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            drop(active_turn);
            return;
        }
        self.task_joins.register(tokio::spawn(work)).detach();
    }

    pub(crate) async fn schedule_startup_prewarm(self: &Arc<Self>, input: PrewarmInput) {
        let websocket_connect_timeout = self.provider().await.websocket_connect_timeout();
        let mut state = self.state.lock().await;
        if state.shutting_down
            || self
                .task_admission_closed
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return;
        }
        // Publish the warmup before another turn can be admitted. No network work
        // or awaits occur while these guards are held.
        let Ok(active_turn) = self.active_turn.try_lock() else {
            return;
        };
        if active_turn.is_some() {
            return;
        }
        if let Some(prewarm) = state.startup_prewarm.as_mut() {
            let Some(completed) = (&mut prewarm.result).now_or_never() else {
                return;
            };
            // Return a completed warmup's client to the existing cache before
            // rechecking its socket: it may have closed since the last resume.
            drop(completed);
            state.startup_prewarm = None;
        }

        if self.features().enabled(Feature::CodeModePrewarm)
            && self.services.code_mode_service.is_available()
        {
            let session = Arc::clone(self);
            self.task_joins
                .register(tokio::spawn(async move {
                    if session.services.code_mode_service.session().await.is_err() {
                        warn!("code-mode host startup prewarm failed");
                    }
                }))
                .detach();
        }

        let account_work = match self.services.host_admission.as_ref() {
            Some(admission) => match admission.admit_operation_work() {
                Ok(work) => work,
                Err(_) => {
                    tracing::debug!("account admission refused optional startup prewarm");
                    return;
                }
            },
            None => None,
        };

        if !self.services.model_client.responses_websocket_enabled() {
            // Without websocket prewarm, resolve auth once so Agent Identity bootstrap can
            // register or engage this session's bearer fallback before the first user request.
            let model_client = self.services.model_client.clone();
            self.task_joins
                .register(tokio::spawn(async move {
                    let _account_work = account_work;
                    if let Err(err) = model_client.prewarm_auth().await {
                        warn!("startup auth prewarm failed: {err:#}");
                    }
                }))
                .detach();
            return;
        }

        let session_telemetry = self.services.session_telemetry.clone();
        let started_at = Instant::now();
        let startup_prewarm_session = Arc::clone(self);
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let startup_prewarm = tokio::spawn(
            async move {
                let work = account_work.map(|work| {
                    Box::new(crate::session::McpOperationWork(work))
                        as Box<dyn codex_mcp::McpAttemptWork>
                });
                let access = work.as_deref().map_or(
                    codex_mcp::McpAttemptAccess::Unscoped,
                    codex_mcp::McpAttemptAccess::Admitted,
                );
                let result =
                    schedule_startup_prewarm_inner(startup_prewarm_session, input, access).await;
                let status = if result.is_ok() { "ready" } else { "failed" };
                session_telemetry.record_startup_phase(
                    "startup_prewarm_total",
                    started_at.elapsed(),
                    Some(status),
                );
                session_telemetry.record_duration(
                    STARTUP_PREWARM_DURATION_METRIC,
                    started_at.elapsed(),
                    &[("status", status), ("input", input.as_str())],
                );
                let _ = result_tx.send(result);
            }
            .instrument(trace_span!(
                "startup_prewarm",
                otel.name = "startup_prewarm",
                thread.id = %self.thread_id(),
                prewarm.input = input.as_str(),
            )),
        );
        let task = self.task_joins.register(startup_prewarm);
        state.set_session_startup_prewarm(SessionStartupPrewarmHandle::new(
            task,
            result_rx,
            started_at,
            websocket_connect_timeout,
        ));
    }

    pub(crate) async fn consume_startup_prewarm_for_regular_turn(
        &self,
        cancellation_token: &CancellationToken,
    ) -> SessionStartupPrewarmResolution {
        let Some(startup_prewarm) = self.take_session_startup_prewarm().await else {
            return SessionStartupPrewarmResolution::Unavailable {
                status: "not_scheduled",
                prewarm_duration: None,
            };
        };
        startup_prewarm
            .resolve(&self.services.session_telemetry, cancellation_token)
            .await
    }
}

async fn schedule_startup_prewarm_inner(
    session: Arc<Session>,
    input: PrewarmInput,
    access: codex_mcp::McpAttemptAccess<'_>,
) -> CodexResult<ModelClientSession> {
    let prewarm_started_at = Instant::now();
    let mut client_session = session.services.model_client.new_session();
    let websocket_ready = client_session.is_websocket_prewarmed().await;
    // Count the decision before preparation can fail; fresh clients also need prewarm.
    session.services.session_telemetry.counter(
        "codex.startup_prewarm.websocket_check",
        /*inc*/ 1,
        &[
            (
                "outcome",
                if websocket_ready {
                    "ready"
                } else {
                    "needs_prewarm"
                },
            ),
            ("input", input.as_str()),
        ],
    );
    if websocket_ready {
        return Ok(client_session);
    }
    let base_instructions = session.get_prompt_base_instructions().await.text;
    let startup_turn_context = session
        .new_startup_prewarm_turn_with_sub_id(INITIAL_SUBMIT_ID.to_owned())
        .await;
    startup_turn_context.session_telemetry.record_startup_phase(
        "startup_prewarm_create_turn_context",
        prewarm_started_at.elapsed(),
        /*status*/ None,
    );
    let startup_cancellation_token = CancellationToken::new();
    let preconnect_model_info = Arc::clone(startup_turn_context.model_info());
    // Spawned subagents inherit the root's selection, with the same feature and model filtering
    // that capture applies to the actual request.
    let preconnect_service_tier = if matches!(
        startup_turn_context.session_source,
        SessionSource::SubAgent(SubAgentSource::ThreadSpawn { .. })
    ) {
        crate::session::get_service_tier(
            session.services.agent_control.service_tier(),
            session.features().enabled(Feature::FastMode),
            &preconnect_model_info,
        )
    } else {
        startup_turn_context.initial_settings.service_tier.clone()
    };
    // The handshake precedes tool capture; attach the finalized step metadata to the warmup.
    let (window_id, window_number, context_window_id) = session.current_window().await;
    let mut handshake_metadata = startup_turn_context
        .turn_metadata_state
        .to_responses_metadata(
            session.installation_id.clone(),
            window_id,
            CodexResponsesRequestKind::Prewarm,
        );
    crate::turn_metadata::ExecutionMetadata::from_settings(&startup_turn_context.initial_settings)
        .apply_to(&mut handshake_metadata);
    let handshake_metadata = session.with_window_and_fork_metadata(
        &startup_turn_context,
        handshake_metadata,
        window_number,
        context_window_id,
    );
    // Start the handshake with the expected route while capturing tools for generate=false.
    let (step_context, ()) = tokio::try_join!(
        async {
            let built_tools_started_at = Instant::now();
            let step_context = session
                .capture_step_context_with_authority(
                    Arc::clone(&startup_turn_context),
                    &startup_cancellation_token,
                    access,
                )
                .await?;
            startup_turn_context.session_telemetry.record_startup_phase(
                "startup_prewarm_build_tools",
                built_tools_started_at.elapsed(),
                /*status*/ None,
            );
            Ok(step_context)
        },
        client_session.preconnect_websocket(
            &preconnect_model_info,
            preconnect_service_tier,
            &startup_turn_context.session_telemetry,
            &handshake_metadata,
        ),
    )?;
    let build_prompt_started_at = Instant::now();
    let prompt_input = match input {
        PrewarmInput::Base => Vec::new(),
        PrewarmInput::History => {
            // Use the same history projection and tool metadata as a sampling request.
            // The real turn still checks this prefix before reusing the prepared response.
            let mut history = session
                .clone_history()
                .await
                .for_prompt(&step_context.settings.model_info.input_modalities);
            session
                .services
                .executed_tool_calls
                .attach_to_prompt(&mut history, &mut HashMap::new());
            history
        }
    };
    let startup_prompt = build_prompt(
        prompt_input,
        step_context.as_ref(),
        BaseInstructions {
            text: base_instructions,
            provenance: None,
        },
    );
    startup_turn_context.session_telemetry.record_startup_phase(
        "startup_prewarm_build_prompt",
        build_prompt_started_at.elapsed(),
        /*status*/ None,
    );
    // Tool discovery may have updated Responses Lite metadata since the eager handshake.
    let responses_metadata = session
        .responses_metadata(step_context.as_ref(), CodexResponsesRequestKind::Prewarm)
        .await;
    let websocket_warmup_started_at = Instant::now();
    // Prewarm establishes the request baseline before the first turn can change effort.
    client_session
        .prewarm_websocket(
            &startup_prompt,
            &step_context.settings.model_info,
            &step_context.session_telemetry,
            session
                .reasoning_effort_for_request(&step_context.settings, RequestEffortUsage::Sampling)
                .await,
            step_context.settings.reasoning_summary,
            step_context.settings.service_tier.clone(),
            &responses_metadata,
        )
        .await?;
    startup_turn_context.session_telemetry.record_startup_phase(
        "startup_prewarm_websocket_warmup",
        websocket_warmup_started_at.elapsed(),
        /*status*/ None,
    );
    Ok(client_session)
}
