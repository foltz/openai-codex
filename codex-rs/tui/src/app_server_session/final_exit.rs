//! One original deadline across every final-exit backend and hosted endpoint.
//! Ordinary reconnect shutdown remains on AppServerSession::shutdown.
use super::*;
use crate::backend_id::BackendId;
use codex_app_server_client::FinalClose;

pub(crate) const FINAL_EXIT_BUDGET: Duration = Duration::from_secs(10);

/// Typed outcome of retiring the TUI-hosted tools endpoint at final exit.
/// It is kept distinct from the transport close result. Only `Complete` is
/// clean; an older daemon is `Unsupported`, never clean.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum HostedRetireOutcome {
    /// No hosted endpoint is in play (embedded app-server, or no server).
    NotHosted,
    Complete {
        census: codex_app_server_protocol::McpHostedEndpointRetireCensus,
    },
    Incomplete {
        census: codex_app_server_protocol::McpHostedEndpointRetireCensus,
    },
    TimedOut,
    Refused {
        reason: codex_app_server_protocol::McpHostedEndpointRefusalReason,
    },
    /// The daemon does not implement the experimental hosted-endpoint methods.
    Unsupported {
        message: String,
    },
    Failed {
        message: String,
    },
}

impl HostedRetireOutcome {
    pub(crate) fn is_clean(&self) -> bool {
        matches!(self, Self::NotHosted | Self::Complete { .. })
    }

    fn from_request_error(error: &codex_app_server_client::TypedRequestError) -> Self {
        match error {
            codex_app_server_client::TypedRequestError::Transport { source, .. }
                if source.kind() == std::io::ErrorKind::TimedOut =>
            {
                Self::TimedOut
            }
            codex_app_server_client::TypedRequestError::Server { source, .. }
                if source.code == -32601
                    || source.message.contains("unknown variant")
                    || source.message.contains("experimental") =>
            {
                Self::Unsupported {
                    message: error.to_string(),
                }
            }
            _ => Self::Failed {
                message: error.to_string(),
            },
        }
    }
}

#[derive(Debug)]
pub(crate) struct FinalExit {
    pub(crate) entries: Vec<(BackendId, HostedRetireOutcome, FinalClose)>,
}
impl FinalExit {
    pub(crate) fn is_clean(&self) -> bool {
        self.entries
            .iter()
            .all(|(_, hosted, close)| hosted.is_clean() && close.is_closed())
    }
    pub(crate) fn report(&self) {
        for (backend, hosted, close) in &self.entries {
            if !hosted.is_clean() || !close.is_closed() {
                tracing::warn!(backend = ?backend, hosted = ?hosted, close = ?close,
                    "final exit incomplete; original runtime/worker custody is not discharged by this observation");
            }
        }
    }
    pub(crate) fn into_result(self) -> std::io::Result<()> {
        for (_, hosted, close) in self.entries {
            match close {
                FinalClose::CloseFailed(error) | FinalClose::TimedOut(error) => return Err(error),
                FinalClose::JoinFailed(message) => return Err(std::io::Error::other(message)),
                FinalClose::Closed if !hosted.is_clean() => {
                    return Err(std::io::Error::other(format!(
                        "hosted endpoint retirement incomplete: {hosted:?}"
                    )));
                }
                FinalClose::Closed => {}
            }
        }
        Ok(())
    }
}

/// Latch every remote synchronously, then perform each session's teardown and close
/// concurrently with the other sessions. Earlier close results retain their shared server Arc
/// until every backend close is observed. No phase gets a fresh budget.
pub(crate) async fn final_exit_sessions(
    sessions: impl IntoIterator<Item = AppServerSession>,
    budget: Duration,
) -> FinalExit {
    let sessions = sessions.into_iter().collect::<Vec<_>>();
    let custody = sessions
        .iter()
        .filter_map(AppServerSession::worker_custody)
        .collect::<Vec<_>>();
    let deadline = tokio::time::Instant::now() + budget;
    for session in &sessions {
        session.begin_final_exit(deadline);
    }
    // 03's retention release extends each session's teardown, through the
    // same latched handles and deadline. One stalled retirement must not stop
    // an unrelated session from closing while its deadline is still live.
    let closes = futures::future::join_all(sessions.into_iter().map(|session| async move {
        let id = session.backend_id();
        let hosted = session.final_exit_teardown(deadline).await;
        let (close, server) = session.close_by(deadline).await;
        (id, hosted, close, server)
    }))
    .await;
    let mut servers = Vec::new();
    let mut entries = Vec::new();
    for (id, hosted, close, server) in closes {
        servers.push(server);
        entries.push((id, hosted, close));
    }
    // Proof and MCP DELETE routes stayed live through every close observation.
    drop(servers);
    drop(custody);
    FinalExit { entries }
}

fn remaining_ms(deadline: tokio::time::Instant) -> u64 {
    u64::try_from(
        deadline
            .saturating_duration_since(tokio::time::Instant::now())
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

impl AppServerSession {
    pub(crate) fn begin_final_exit(&self, deadline: tokio::time::Instant) {
        if let Some(client) = &self.connection_owner {
            client.begin_final_exit(deadline);
        }
    }

    pub(super) async fn close_by(
        self,
        deadline: tokio::time::Instant,
    ) -> (FinalClose, Option<Arc<DynamicToolMcpServer>>) {
        let Self {
            connection_owner,
            reported_disconnect,
            dynamic_tool_mcp,
            ..
        } = self;
        let remote = matches!(connection_owner, Some(AppServerClient::Remote(_)));
        let close = match connection_owner {
            Some(client) => client.shutdown_by(deadline).await,
            None => FinalClose::Closed,
        };
        let close = settle_final_close(remote, reported_disconnect.as_deref(), close);
        (close, dynamic_tool_mcp)
    }

    async fn declare_hosted_mcp_endpoint(
        &self,
        endpoint: &(String, String, String),
        deadline: tokio::time::Instant,
    ) -> Result<(), HostedRetireOutcome> {
        let (url, authorization, retirement_capability) = endpoint.clone();
        let request = ClientRequest::McpHostedEndpointDeclare {
            request_id: RequestId::String(format!("hosted-mcp-declare-{}", Uuid::new_v4())),
            params: codex_app_server_protocol::McpHostedEndpointDeclareParams {
                url,
                authorization,
                retirement_capability,
                timeout_ms: remaining_ms(deadline),
            },
        };
        let handle = self.request_handle();
        match tokio::time::timeout_at(
            deadline,
            handle.request_typed::<codex_app_server_protocol::McpHostedEndpointDeclareResponse>(
                request,
            ),
        )
        .await
        {
            Ok(Ok(codex_app_server_protocol::McpHostedEndpointDeclareResponse::Declared {
                ..
            })) => Ok(()),
            Ok(Ok(codex_app_server_protocol::McpHostedEndpointDeclareResponse::Refused {
                reason,
            })) => Err(HostedRetireOutcome::Refused { reason }),
            Ok(Err(error)) => Err(HostedRetireOutcome::from_request_error(&error)),
            Err(_) => Err(HostedRetireOutcome::TimedOut),
        }
    }

    pub(super) async fn final_exit_teardown(
        &self,
        deadline: tokio::time::Instant,
    ) -> HostedRetireOutcome {
        if !matches!(self.connection_owner, Some(AppServerClient::Remote(_))) {
            return HostedRetireOutcome::NotHosted;
        }
        let Some(server) = self.dynamic_tool_mcp.as_ref() else {
            return HostedRetireOutcome::NotHosted;
        };
        let Some(endpoint) = server.hosted_endpoint() else {
            return HostedRetireOutcome::NotHosted;
        };
        // No new task-tool calls are routed while their connections retire.
        server.suspend();
        if let Err(outcome) = self.declare_hosted_mcp_endpoint(&endpoint, deadline).await {
            return outcome;
        }
        let timeout_ms = remaining_ms(deadline);
        if timeout_ms == 0 {
            return HostedRetireOutcome::TimedOut;
        }
        let (url, authorization, retirement_capability) = endpoint;
        let request = ClientRequest::McpHostedEndpointRetire {
            request_id: RequestId::String(format!("hosted-mcp-retire-{}", Uuid::new_v4())),
            params: codex_app_server_protocol::McpHostedEndpointRetireParams {
                url,
                authorization,
                retirement_capability,
                timeout_ms,
            },
        };
        let handle = self.request_handle();
        match tokio::time::timeout_at(
            deadline,
            handle.request_typed::<codex_app_server_protocol::McpHostedEndpointRetireResponse>(
                request,
            ),
        )
        .await
        {
            Ok(Ok(codex_app_server_protocol::McpHostedEndpointRetireResponse::Retired {
                census,
                ..
            })) => HostedRetireOutcome::Complete { census },
            Ok(Ok(codex_app_server_protocol::McpHostedEndpointRetireResponse::Incomplete {
                census,
                ..
            })) => HostedRetireOutcome::Incomplete { census },
            Ok(Ok(codex_app_server_protocol::McpHostedEndpointRetireResponse::Refused {
                reason,
            })) => HostedRetireOutcome::Refused { reason },
            Ok(Err(error)) => HostedRetireOutcome::from_request_error(&error),
            Err(_) => HostedRetireOutcome::TimedOut,
        }
    }
}

fn settle_final_close(remote: bool, reported: Option<&str>, close: FinalClose) -> FinalClose {
    match close {
        FinalClose::CloseFailed(error) => {
            match settle_reported_disconnect(remote, reported, Err(error)) {
                Ok(()) => FinalClose::Closed,
                Err(error) => FinalClose::CloseFailed(error),
            }
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn final_close_acknowledges_only_the_identical_reported_remote_disconnect() {
        let error = || std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reported reset");
        assert!(
            settle_final_close(
                true,
                Some("reported reset"),
                FinalClose::CloseFailed(error())
            )
            .is_closed()
        );
        assert!(
            !settle_final_close(
                false,
                Some("reported reset"),
                FinalClose::CloseFailed(error())
            )
            .is_closed()
        );
        assert!(
            !settle_final_close(
                true,
                Some("different reset"),
                FinalClose::CloseFailed(error())
            )
            .is_closed()
        );
        assert!(matches!(
            settle_final_close(true, Some("reported reset"), FinalClose::TimedOut(error())),
            FinalClose::TimedOut(_)
        ));
    }
}
