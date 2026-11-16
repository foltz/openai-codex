//! Auth-child scope derived from the caller's existing finite operation.
use codex_mcp::McpAttemptWork;
use codex_model_provider::{AuthTaskAccess, AuthTaskFailure, with_auth_task_scope};
use codex_protocol::error::Result;
use std::future::Future;

/// Capture before the first await. The factory owns derived finite work, never an idle client.
pub(crate) fn access(work: Option<&dyn McpAttemptWork>) -> Result<AuthTaskAccess> {
    match work {
        Some(work) => Ok(AuthTaskAccess::Admitted(
            work.auth_task_spawner()
                .map_err(|_| AuthTaskFailure::Refused.error())?,
        )),
        None => Ok(AuthTaskAccess::Ungated),
    }
}

/// Snapshot the exact current turn once; descendants do not look up a later turn or admit fresh work.
pub(crate) fn turn_access(
    session: &crate::session::session::Session,
    turn: &crate::session::turn_context::TurnContext,
) -> Result<AuthTaskAccess> {
    let work = session
        .turn_mcp_work(turn)
        .map_err(|_| AuthTaskFailure::Refused.error())?;
    access(work.as_deref())
}

pub(crate) async fn scope<T>(
    work: Option<&dyn McpAttemptWork>,
    future: impl Future<Output = T>,
) -> Result<T> {
    with_auth_task_scope(access(work)?, future).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    struct Unsupported;
    impl McpAttemptWork for Unsupported {
        fn derive_attempt(
            &self,
        ) -> std::result::Result<Box<dyn McpAttemptWork>, codex_mcp::McpAttemptRefused> {
            panic!("scope must not mint or derive substitute authority")
        }
    }
    #[tokio::test]
    async fn supplied_work_without_auth_owner_refuses_before_polling() {
        let polled = AtomicBool::new(false);
        assert!(
            scope(Some(&Unsupported), async {
                polled.store(true, Ordering::SeqCst);
            })
            .await
            .is_err()
        );
        assert!(!polled.load(Ordering::SeqCst));
    }
    #[tokio::test]
    async fn caller_validated_unmanaged_scope_preserves_its_result() {
        assert_eq!(scope(None, async { 37 }).await.unwrap(), 37);
    }
    #[derive(Debug)]
    struct Required;
    impl codex_extension_api::TurnStartAdmission for Required {
        fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
            panic!("no fresh fallback")
        }
        fn requires_account_work(&self) -> bool {
            true
        }
    }
    #[tokio::test]
    async fn turn_capture_refuses_missing_required_work_before_auth_scope() {
        let (mut session, turn) = crate::session::tests::make_session_and_context().await;
        session.services.host_admission = Some(std::sync::Arc::new(Required));
        assert!(turn_access(&session, &turn).is_err());
    }
}
