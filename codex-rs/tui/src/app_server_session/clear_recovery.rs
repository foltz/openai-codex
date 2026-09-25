use super::*;
use codex_app_server_protocol::ThreadClearRecoveryContext;
use codex_app_server_protocol::ThreadClearRecoveryObservation;
use codex_app_server_protocol::ThreadClearRecoveryReadParams;
use codex_app_server_protocol::ThreadClearRecoveryReadResponse;

impl AppServerSession {
    pub(crate) async fn start_clear_recovery(
        &mut self,
        local_settings: &LocalSettings,
        config: &Config,
        predecessor: Option<ThreadId>,
    ) -> Result<AppServerStartedThread> {
        // Use one connection-bound handle, never a cached capability across reconnect.
        let handle = self.request_handle();
        let support: ThreadClearRecoveryReadResponse = handle.request_typed(
            ClientRequest::ThreadClearRecoveryRead {
                request_id: self.next_request_id(),
                params: ThreadClearRecoveryReadParams::default(),
            },
        ).await.wrap_err(
            "Clear recovery is unsupported or unavailable; reconnect to a compatible server. No recovery was started",
        )?;
        if support.contract_version != 1
            || support.observation != ThreadClearRecoveryObservation::Support
        {
            color_eyre::eyre::bail!("Clear recovery contract mismatch; no recovery was started");
        }
        let context = ThreadClearRecoveryContext {
            contract_version: 1,
            predecessor_thread_id: predecessor.map(|id| id.to_string()),
        };
        let session_config = self.session_config_with_effective_service_tier(config);
        let mut params = thread_start_params_from_config(
            &session_config,
            self.thread_params_mode(),
            self.remote_cwd_override.as_deref(),
            Some(ThreadStartSource::Clear),
            /*clear_predecessor_thread_id*/ None,
        );
        params.clear_recovery = Some(context.clone());
        if self.history_support == ThreadHistorySupport::LegacyOnly {
            params.history_mode = None;
        }
        self.thread_tool_transport().configure(&mut params);
        let task_tools_available = params.dynamic_tools.is_some()
            || params
                .config
                .as_ref()
                .is_some_and(|config| config.contains_key("mcp_servers.codex_tui"));
        // Deliberately bypass the legacy history/tools creation retry helper.
        let response: ThreadStartResponse = handle.request_typed(ClientRequest::ThreadStart {
            request_id: self.next_request_id(), params,
        }).await.wrap_err(
            "Clear recovery outcome is unknown or refused; no automatic retry. A new attempt may create another thread",
        )?;
        if !response.clear_recovery.as_ref().is_some_and(|accepted| {
            accepted.context == context
                && accepted.successor_thread_id == response.thread.id
                && accepted.durability == support.durability
        }) {
            color_eyre::eyre::bail!(
                "Clear recovery outcome is unknown: server omitted or changed accepted context; do not automatically retry"
            );
        }
        let mut started = started_thread_from_start_response(
            response,
            local_settings,
            config,
            self.thread_params_mode(),
        )
        .await?;
        started.task_tools_available = task_tools_available;
        if task_tools_available {
            self.remember_task_tool_thread(started.session.thread_id);
        }
        Ok(started)
    }
}
