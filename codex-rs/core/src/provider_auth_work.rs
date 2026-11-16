//! Auth-child scope derived from the caller's existing finite operation.
use codex_mcp::McpAttemptWork;
use codex_model_provider::{AuthTaskAccess, AuthTaskFailure, with_auth_task_scope};
use codex_protocol::error::Result;
use std::future::Future;

pub(crate) async fn scope<T>(
    work: Option<&dyn McpAttemptWork>,
    future: impl Future<Output = T>,
) -> Result<T> {
    let access = match work {
        Some(work) => AuthTaskAccess::Admitted(
            work.auth_task_spawner()
                .map_err(|_| AuthTaskFailure::Refused.error())?,
        ),
        // Caller already validated that this host permits unmanaged work.
        None => AuthTaskAccess::Ungated,
    };
    with_auth_task_scope(access, future).await
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
}
