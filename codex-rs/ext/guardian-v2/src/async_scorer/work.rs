//! Finite classifier invocation descendants, never idle/cache authority.
use super::sampler::LunaSamplerError;
use codex_protocol::mcp_work::McpAttemptWork;
pub(super) fn derive(
    required: bool,
    work: Option<&dyn McpAttemptWork>,
) -> Result<Option<Box<dyn McpAttemptWork>>, LunaSamplerError> {
    match work {
        Some(work) => work
            .derive_attempt()
            .map(Some)
            .map_err(|_| LunaSamplerError::AccountWorkRefused),
        None if required => Err(LunaSamplerError::AccountWorkRefused),
        None => Ok(None),
    }
}
