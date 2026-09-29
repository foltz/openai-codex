use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

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
    /// Register auxiliary startup work under the same permanent admission gate
    /// as turn tasks. The join remains observable after its driver is dropped.
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

    pub(crate) async fn schedule_startup_prewarm(self: &Arc<Self>, base_instructions: String) {
        if self.features().enabled(Feature::CodeModePrewarm)
            && self.services.code_mode_service.is_available()
        {
            let session = Arc::clone(self);
            self.spawn_startup_auxiliary(async move {
                if session.services.code_mode_service.session().await.is_err() {
                    warn!("code-mode host startup prewarm failed");
                }
            })
            .await;
        }

        // Capture operation custody before spawning. It is deliberately not a
        // turn lease: construction precedes the loop receipt, and the operation
        // may outlive the loop. Refusal skips only this optional optimization.
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
            self.spawn_startup_auxiliary(async move {
                let _account_work = account_work;
                if let Err(err) = model_client.prewarm_auth().await {
                    warn!("startup auth prewarm failed: {err:#}");
                }
            })
            .await;
            return;
        }

        let session_telemetry = self.services.session_telemetry.clone();
        let websocket_connect_timeout = self.provider().await.websocket_connect_timeout();
        let started_at = Instant::now();
        let startup_prewarm_session = Arc::clone(self);
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let active_turn = self.active_turn.lock().await;
        if self
            .task_admission_closed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            drop(active_turn);
            return;
        }
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
                let result = schedule_startup_prewarm_inner(
                    startup_prewarm_session,
                    base_instructions,
                    access,
                )
                .await;
                let status = if result.is_ok() { "ready" } else { "failed" };
                session_telemetry.record_startup_phase(
                    "startup_prewarm_total",
                    started_at.elapsed(),
                    Some(status),
                );
                session_telemetry.record_duration(
                    STARTUP_PREWARM_DURATION_METRIC,
                    started_at.elapsed(),
                    &[("status", status)],
                );
                let _ = result_tx.send(result);
            }
            .instrument(trace_span!(
                "startup_prewarm",
                otel.name = "startup_prewarm",
                thread.id = %self.thread_id(),
            )),
        );
        let task = self.task_joins.register(startup_prewarm);
        drop(active_turn);
        self.set_session_startup_prewarm(SessionStartupPrewarmHandle::new(
            task,
            result_rx,
            started_at,
            websocket_connect_timeout,
        ))
        .await;
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
    base_instructions: String,
    access: codex_mcp::McpAttemptAccess<'_>,
) -> CodexResult<ModelClientSession> {
    let prewarm_started_at = Instant::now();
    let startup_turn_context = session
        .new_startup_prewarm_turn_with_sub_id(INITIAL_SUBMIT_ID.to_owned())
        .await;
    startup_turn_context.session_telemetry.record_startup_phase(
        "startup_prewarm_create_turn_context",
        prewarm_started_at.elapsed(),
        /*status*/ None,
    );
    let startup_cancellation_token = CancellationToken::new();
    let built_tools_started_at = Instant::now();
    // Startup prewarm runs before run_turn and needs its own tool-building snapshot.
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
    let build_prompt_started_at = Instant::now();
    let startup_prompt = build_prompt(
        Vec::new(),
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
    let responses_metadata = session
        .responses_metadata(step_context.as_ref(), CodexResponsesRequestKind::Prewarm)
        .await;
    let mut client_session = session.services.model_client.new_session();
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
