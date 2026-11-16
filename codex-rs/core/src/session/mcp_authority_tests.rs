//! Invocation custody and refusal before Core MCP refresh side effects.
//! Real Session refresh/permit/invalidation paths, synthetic account authority.
use super::*;
use codex_extension_api::{
    ExtensionData, ExtensionFuture, HostOperationWork, TurnStartAdmission, TurnWorkRefused,
};
use codex_mcp::{McpAttemptAccess, McpAttemptRefused, McpAttemptWork};
use pretty_assertions::assert_eq;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Debug, Default)]
struct Counts {
    live: Arc<AtomicUsize>,
    derives: AtomicUsize,
    refuse: AtomicBool,
    turns: AtomicUsize,
}
#[derive(Debug)]
struct Work(Arc<Counts>);
impl Work {
    fn new(counts: &Arc<Counts>) -> Self {
        counts.live.fetch_add(1, Ordering::SeqCst);
        Self(counts.clone())
    }
}
impl Drop for Work {
    fn drop(&mut self) {
        self.0.live.fetch_sub(1, Ordering::SeqCst);
    }
}
impl McpAttemptWork for Work {
    fn derive_attempt(&self) -> Result<Box<dyn McpAttemptWork>, McpAttemptRefused> {
        if self.0.refuse.load(Ordering::SeqCst) {
            return Err(McpAttemptRefused);
        }
        self.0.derives.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(Self::new(&self.0)))
    }
}
impl HostOperationWork for Work {
    fn derive_operation(&self) -> Result<Box<dyn HostOperationWork>, TurnWorkRefused> {
        if self.0.refuse.load(Ordering::SeqCst) {
            return Err(TurnWorkRefused::Unavailable);
        }
        self.0.derives.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(Self::new(&self.0)))
    }
    fn derive_turn_work(
        &self,
        _: &ExtensionData,
        _: ExtensionFuture<'static, ()>,
    ) -> Result<Box<dyn codex_protocol::host_turn_work::HostTurnWork>, TurnWorkRefused> {
        Err(TurnWorkRefused::Unavailable)
    }
}
#[derive(Debug)]
struct Host {
    counts: Arc<Counts>,
    store: String,
    turn: String,
}
impl TurnStartAdmission for Host {
    fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
        panic!("no fresh admission fallback")
    }
    fn requires_account_work(&self) -> bool {
        true
    }
    fn derive_operation_work(
        &self,
        store: &ExtensionData,
        turn: &str,
    ) -> Result<Option<Box<dyn HostOperationWork>>, TurnWorkRefused> {
        self.counts.turns.fetch_add(1, Ordering::SeqCst);
        if self.counts.refuse.load(Ordering::SeqCst)
            || store.level_id() != self.store
            || turn != self.turn
        {
            return Err(TurnWorkRefused::Unavailable);
        }
        Ok(Some(Box::new(Work::new(&self.counts))))
    }
}
async fn fixture() -> (Arc<Session>, TurnContext, Arc<Counts>) {
    let (mut session, turn) = crate::session::tests::make_session_and_context().await;
    let counts = Arc::new(Counts::default());
    session.services.host_admission = Some(Arc::new(Host {
        counts: counts.clone(),
        store: session.services.thread_extension_data.level_id().to_owned(),
        turn: turn.sub_id.clone(),
    }));
    session.mark_mcp_runtime_dirty(McpRefreshReason::ExplicitInvalidation);
    (Arc::new(session), turn, counts)
}

#[tokio::test]
async fn required_unscoped_refresh_and_apps_refuse_before_the_held_permit() {
    let (session, _, counts) = fixture().await;
    let _permit = session.mcp_refresh.acquire().await.unwrap();
    let refresh = session.refresh_mcp_if_dirty_with_authority(McpAttemptAccess::Unscoped);
    futures::pin_mut!(refresh);
    assert!(matches!(
        futures::poll!(refresh.as_mut()),
        std::task::Poll::Ready(Err(_))
    ));
    let apps = session.refresh_codex_apps_tools(McpAttemptAccess::Unscoped);
    futures::pin_mut!(apps);
    assert!(matches!(
        futures::poll!(apps.as_mut()),
        std::task::Poll::Ready(Err(_))
    ));
    assert!(session.mcp_refresh.is_pending());
    assert_eq!(session.mcp_refresh.apps_refreshes.len(), 0);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn exhausted_explicit_refresh_refuses_before_the_held_permit_without_fallback() {
    let (session, _, counts) = fixture().await;
    let root = Work::new(&counts);
    counts.refuse.store(true, Ordering::SeqCst);
    let _permit = session.mcp_refresh.acquire().await.unwrap();
    let refresh = session.refresh_mcp_if_dirty_with_authority(McpAttemptAccess::Admitted(&root));
    futures::pin_mut!(refresh);
    assert!(matches!(
        futures::poll!(refresh.as_mut()),
        std::task::Poll::Ready(Err(_))
    ));
    assert!(session.mcp_refresh.is_pending());
    assert_eq!(counts.derives.load(Ordering::SeqCst), 0);
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelled_refresh_owns_work_and_restores_the_claimed_invalidation() {
    let (session, _, counts) = fixture().await;
    let root = Work::new(&counts);
    let _state = session.state.lock().await;
    {
        let mut refresh = Box::pin(
            session.refresh_mcp_if_dirty_with_authority(McpAttemptAccess::Admitted(&root)),
        );
        assert!(futures::poll!(&mut refresh).is_pending());
        assert!(
            !session.mcp_refresh.is_pending(),
            "refresh reached the claimed-state await"
        );
        assert_eq!(counts.live.load(Ordering::SeqCst), 2);
    }
    assert!(session.mcp_refresh.is_pending());
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelled_apps_refresh_releases_its_coalescing_token_and_work() {
    let (session, _, counts) = fixture().await;
    let root = Work::new(&counts);
    let _state = session.state.lock().await;
    {
        let mut refresh =
            Box::pin(session.refresh_codex_apps_tools(McpAttemptAccess::Admitted(&root)));
        assert!(futures::poll!(&mut refresh).is_pending());
        assert_eq!(session.mcp_refresh.apps_refreshes.len(), 1);
        assert_eq!(counts.live.load(Ordering::SeqCst), 2);
        assert!(!session.mcp_refresh.is_pending());
    }
    assert_eq!(session.mcp_refresh.apps_refreshes.len(), 0);
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    assert!(session.mcp_refresh.is_pending());
}

#[tokio::test]
async fn admitted_refresh_publishes_pending_state_without_retaining_idle_work() {
    let (session, _, counts) = fixture().await;
    let root = Work::new(&counts);
    session
        .refresh_mcp_if_dirty_with_authority(McpAttemptAccess::Admitted(&root))
        .await
        .unwrap();
    assert!(!session.mcp_refresh.is_pending());
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while counts.live.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        counts.derives.load(Ordering::SeqCst) >= 2,
        "refresh and publication own separate finite children"
    );
    drop(root);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn failed_publication_derivation_restores_pending_state_without_replacing_runtime() {
    let (session, _, counts) = fixture().await;
    let root = Work::new(&counts);
    let before =
        codex_mcp::McpResourceClient::new(session.services.mcp_runtime.clone()).cache_key();
    let state = session.state.lock().await;
    let mut refresh =
        Box::pin(session.refresh_mcp_if_dirty_with_authority(McpAttemptAccess::Admitted(&root)));
    assert!(futures::poll!(&mut refresh).is_pending());
    assert!(!session.mcp_refresh.is_pending());
    counts.refuse.store(true, Ordering::SeqCst);
    drop(state);
    assert!(refresh.await.is_err());
    assert!(session.mcp_refresh.is_pending());
    assert_eq!(
        before,
        codex_mcp::McpResourceClient::new(session.services.mcp_runtime.clone()).cache_key()
    );
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
}

fn advertised_tool() -> codex_mcp::ToolInfo {
    codex_mcp::ToolInfo {
        server_name: "test".into(),
        supports_parallel_tool_calls: false,
        server_origin: None,
        callable_name: "test".into(),
        callable_namespace: "test".into(),
        namespace_description: None,
        tool: rmcp::model::Tool::new("test", "test", Arc::new(Default::default())),
        openai_file_input_optional_fields: Default::default(),
        connector_id: None,
        connector_name: None,
        plugin_display_names: Vec::new(),
    }
}
#[tokio::test]
async fn readiness_prepare_and_hard_refresh_refuse_the_wrong_turn_before_the_permit() {
    let (session, mut turn, counts) = fixture().await;
    turn.sub_id = "wrong-turn".into();
    let _permit = session.mcp_refresh.acquire().await.unwrap();
    assert!(session.wait_for_mcp_server(&turn, "test").await.is_err());
    assert!(
        session
            .prepare_mcp_call(&turn, &advertised_tool())
            .await
            .is_err()
    );
    assert!(
        session
            .hard_refresh_latest_codex_apps_tools(&turn)
            .await
            .is_err()
    );
    assert_eq!(counts.turns.load(Ordering::SeqCst), 3);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    assert!(session.mcp_refresh.is_pending());
}
