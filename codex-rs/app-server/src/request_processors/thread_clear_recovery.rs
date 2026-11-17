//! Independent recovery evidence. No predecessor lookup, shutdown or authority claim.
use super::thread_processor::ThreadRequestProcessor;
use crate::error_code::internal_error;
use crate::error_code::invalid_request;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::ThreadClearRecovery;
use codex_app_server_protocol::ThreadClearRecoveryContext;
use codex_app_server_protocol::ThreadClearRecoveryDurability;
use codex_app_server_protocol::ThreadClearRecoveryObservation;
use codex_app_server_protocol::ThreadClearRecoveryReadParams;
use codex_app_server_protocol::ThreadClearRecoveryReadResponse;
use codex_app_server_protocol::ThreadStartSource;
use codex_protocol::ThreadId;
use codex_state::ClearRecoveryPhase;

pub(super) fn validate_recovery(
    context: &ThreadClearRecoveryContext,
    source: Option<ThreadStartSource>,
    legacy_predecessor: &Option<String>,
) -> Result<(), JSONRPCErrorError> {
    if context.contract_version != 1 {
        return Err(invalid_request("unsupported clearRecovery contractVersion"));
    }
    if source != Some(ThreadStartSource::Clear) || legacy_predecessor.is_some() {
        return Err(invalid_request(
            "clearRecovery requires source=clear and no clearPredecessorThreadId",
        ));
    }
    if let Some(id) = &context.predecessor_thread_id {
        ThreadId::from_string(id)
            .map_err(|err| invalid_request(format!("invalid recovery predecessor: {err}")))?;
    }
    Ok(())
}

impl ThreadRequestProcessor {
    pub(crate) async fn thread_clear_recovery_read(
        &self,
        params: ThreadClearRecoveryReadParams,
    ) -> Result<ThreadClearRecoveryReadResponse, JSONRPCErrorError> {
        read_recovery(self.state_db.as_deref(), params).await
    }
}

async fn read_recovery(
    state: Option<&codex_state::StateRuntime>,
    params: ThreadClearRecoveryReadParams,
) -> Result<ThreadClearRecoveryReadResponse, JSONRPCErrorError> {
    let durability = if state.is_some() {
        ThreadClearRecoveryDurability::Durable
    } else {
        ThreadClearRecoveryDurability::Unavailable
    };
    let observation = if let Some(successor) = params.successor_thread_id {
        let successor = ThreadId::from_string(&successor)
            .map_err(|err| invalid_request(format!("invalid recovery successor: {err}")))?;
        let state = state.ok_or_else(|| {
            internal_error("clear recovery durability unavailable; absence cannot be determined")
        })?;
        match state
            .get_clear_recovery(successor)
            .await
            .map_err(|err| internal_error(format!("failed to read clear recovery: {err}")))?
        {
            None => ThreadClearRecoveryObservation::None,
            Some(record) => {
                let recovery = ThreadClearRecovery {
                    successor_thread_id: successor.to_string(),
                    context: ThreadClearRecoveryContext {
                        contract_version: 1,
                        predecessor_thread_id: record
                            .predecessor_thread_id
                            .map(|id| id.to_string()),
                    },
                    durability,
                };
                match record.phase {
                    ClearRecoveryPhase::Pending => {
                        ThreadClearRecoveryObservation::Pending { recovery }
                    }
                    ClearRecoveryPhase::Complete => {
                        ThreadClearRecoveryObservation::Complete { recovery }
                    }
                    ClearRecoveryPhase::Failed => {
                        ThreadClearRecoveryObservation::Failed { recovery }
                    }
                }
            }
        }
    } else {
        ThreadClearRecoveryObservation::Support
    };
    Ok(ThreadClearRecoveryReadResponse {
        contract_version: 1,
        durability,
        observation,
    })
}

#[cfg(test)]
#[path = "thread_clear_recovery_tests.rs"]
mod tests;
