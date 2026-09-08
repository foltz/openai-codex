use codex_app_server_protocol::JSONRPCErrorError;

pub(crate) const INVALID_REQUEST_ERROR_CODE: i64 = -32600;
pub(crate) const METHOD_NOT_FOUND_ERROR_CODE: i64 = -32601;
pub const INVALID_PARAMS_ERROR_CODE: i64 = -32602;
pub(crate) const INTERNAL_ERROR_CODE: i64 = -32603;
pub(crate) const OVERLOADED_ERROR_CODE: i64 = -32001;
/// Stable, retryable refusal for a new account-dependent request submitted
/// while a managed-auth transition's barrier or quarantine is active
/// (`CODEX-I05-S03-R010`). Follows the same shape as
/// `OVERLOADED_ERROR_CODE`: a fixed implementation-defined code plus a
/// human-readable message, no `data` payload -- retryability is carried by
/// the stable code itself, not a structured field, matching this crate's
/// only other precedent for a typed-retryable JSON-RPC error.
pub(crate) const ACCOUNT_TRANSITION_IN_PROGRESS_ERROR_CODE: i64 = -32002;
pub const INPUT_TOO_LARGE_ERROR_CODE: &str = "input_too_large";

pub(crate) fn invalid_request(message: impl Into<String>) -> JSONRPCErrorError {
    error(INVALID_REQUEST_ERROR_CODE, message)
}

/// `CODEX-I05-S03-R010`: typed, stable, retryable, no credential or raw
/// stable-identity value. Returned before auth capture or provider effect
/// whenever the dispatch-time classifier
/// (`crate::account_dependency::classify`) finds the request permit-gated
/// and the barrier refuses admission.
pub(crate) fn account_transition_in_progress() -> JSONRPCErrorError {
    error(
        ACCOUNT_TRANSITION_IN_PROGRESS_ERROR_CODE,
        "a managed account transition is in progress; retry after it completes",
    )
}

pub(crate) fn method_not_found(message: impl Into<String>) -> JSONRPCErrorError {
    error(METHOD_NOT_FOUND_ERROR_CODE, message)
}

pub(crate) fn invalid_params(message: impl Into<String>) -> JSONRPCErrorError {
    error(INVALID_PARAMS_ERROR_CODE, message)
}

pub(crate) fn internal_error(message: impl Into<String>) -> JSONRPCErrorError {
    error(INTERNAL_ERROR_CODE, message)
}

fn error(code: i64, message: impl Into<String>) -> JSONRPCErrorError {
    JSONRPCErrorError {
        code,
        message: message.into(),
        data: None,
    }
}
