use super::thread_processor::ThreadRequestProcessor;
use crate::error_code::internal_error;
use crate::error_code::invalid_request;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::ThreadClearReadParams;
use codex_app_server_protocol::ThreadClearReadResponse;
use codex_app_server_protocol::ThreadClearTransition;
use codex_core::config::ThreadStoreConfig;
use codex_protocol::ThreadId;
use codex_state::ClearTransitionPhase;

impl ThreadRequestProcessor {
    pub(crate) async fn thread_clear_read(
        &self,
        params: ThreadClearReadParams,
    ) -> Result<ThreadClearReadResponse, JSONRPCErrorError> {
        let successor = ThreadId::from_string(&params.successor_thread_id)
            .map_err(|err| invalid_request(format!("invalid successor thread id: {err}")))?;
        if !matches!(
            self.config.experimental_thread_store,
            ThreadStoreConfig::Local
        ) {
            return Err(invalid_request(
                "thread/clear/read requires the durable local thread store",
            ));
        }
        let state = self
            .state_db
            .as_ref()
            .ok_or_else(|| internal_error("thread/clear/read durable state is unavailable"))?;
        let record = state
            .get_clear_transition_by_successor(successor)
            .await
            .map_err(|err| internal_error(format!("failed to read clear transition: {err}")))?;
        let Some(record) = record else {
            return Ok(ThreadClearReadResponse::None { transition: () });
        };
        let transition = ThreadClearTransition {
            transition_id: record.transition_id.to_string(),
            predecessor_thread_id: record.predecessor_thread_id.to_string(),
            successor_thread_id: record.successor_thread_id.to_string(),
        };
        match record.phase {
            ClearTransitionPhase::Reserved
            | ClearTransitionPhase::SuccessorCreated
            | ClearTransitionPhase::Committed
            | ClearTransitionPhase::EvidenceClaimed => {
                Ok(ThreadClearReadResponse::Pending { transition })
            }
            ClearTransitionPhase::Completed => Ok(ThreadClearReadResponse::Complete { transition }),
            // Excluded by the state query. Never manufacture a lineage if that
            // storage invariant is violated.
            ClearTransitionPhase::Abandoned => Err(internal_error(
                "clear successor lookup returned an abandoned reservation",
            )),
        }
    }
}
