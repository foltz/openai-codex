//! Installation refuses before persistence or OAuth; no credentials or peer.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
struct Refused(AtomicUsize);
impl codex_mcp::McpAttemptWork for Refused {
    fn derive_attempt(
        &self,
    ) -> Result<Box<dyn codex_mcp::McpAttemptWork>, codex_mcp::McpAttemptRefused> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(codex_mcp::McpAttemptRefused)
    }
}
#[tokio::test]
async fn exhausted_installation_refuses_before_persistence_or_oauth() {
    let (session, mut turn) = crate::session::tests::make_session_and_context().await;
    let home = tempfile::tempdir().unwrap();
    let mut config = (*turn.config).clone();
    config.codex_home = codex_utils_absolute_path::AbsolutePathBuf::try_from(home.path()).unwrap();
    turn.config = Arc::new(config);
    let work = Refused(AtomicUsize::new(0));
    let missing = HashMap::from([(
        "not-installed".into(),
        serde_json::from_value(serde_json::json!({"url":"https://invalid.example/mcp"})).unwrap(),
    )]);
    let mut install = Box::pin(maybe_install_mcp_dependencies(
        &session,
        &turn,
        missing,
        None,
        codex_mcp::McpAttemptAccess::Admitted(&work),
    ));
    assert!(matches!(
        futures::poll!(&mut install),
        std::task::Poll::Ready(())
    ));
    assert_eq!(work.0.load(Ordering::SeqCst), 1);
    assert_eq!(std::fs::read_dir(home.path()).unwrap().count(), 0);
}
