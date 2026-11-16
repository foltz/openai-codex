//! Real turn-input callback boundary; synthetic host counts, no provider/model execution.
use super::*;
use codex_extension_api::{
    ExtensionData, ExtensionFuture, HostOperationWork, TurnStartAdmission, TurnWorkRefused,
};
use codex_mcp::{McpAttemptAccess, McpAttemptRefused, McpAttemptWork};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
#[derive(Default, Debug)]
struct Counts {
    live: AtomicUsize,
    refuse: AtomicBool,
    calls: AtomicUsize,
    refused: AtomicUsize,
    pause: AtomicBool,
}
struct Work(Arc<Counts>);
impl Work {
    fn new(c: &Arc<Counts>) -> Self {
        c.live.fetch_add(1, Ordering::SeqCst);
        Self(c.clone())
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
        Ok(Box::new(Self::new(&self.0)))
    }
}
impl HostOperationWork for Work {
    fn derive_operation(&self) -> Result<Box<dyn HostOperationWork>, TurnWorkRefused> {
        if self.0.refuse.load(Ordering::SeqCst) {
            return Err(TurnWorkRefused::Unavailable);
        }
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
    turn: String,
    store: String,
}
impl TurnStartAdmission for Host {
    fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
        panic!("no fresh admission")
    }
    fn requires_account_work(&self) -> bool {
        true
    }
    fn derive_operation_work(
        &self,
        store: &ExtensionData,
        turn: &str,
    ) -> Result<Option<Box<dyn HostOperationWork>>, TurnWorkRefused> {
        if self.counts.refuse.load(Ordering::SeqCst)
            || turn != self.turn
            || store.level_id() != self.store
        {
            return Err(TurnWorkRefused::Unavailable);
        }
        Ok(Some(Box::new(Work::new(&self.counts))))
    }
}
struct Contributor(Arc<Counts>);
impl codex_extension_api::TurnInputContributor for Contributor {
    fn contribute<'a>(
        &'a self,
        input: TurnInputContext<'a>,
        _: Option<Arc<dyn codex_extension_api::ExtensionMetrics>>,
        _: &'a ExtensionData,
        _: &'a ExtensionData,
        _: &'a ExtensionData,
    ) -> ExtensionFuture<'a, Vec<Box<dyn ContextualUserFragment + Send>>> {
        Box::pin(async move {
            let Ok(McpAttemptAccess::Admitted(work)) = input.mcp_access else {
                self.0.refused.fetch_add(1, Ordering::SeqCst);
                return Vec::new();
            };
            let _child = work
                .derive_attempt()
                .expect("callback gets live invocation work");
            self.0.calls.fetch_add(1, Ordering::SeqCst);
            if self.0.pause.swap(false, Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            Vec::new()
        })
    }
}
#[tokio::test]
async fn turn_input_callbacks_release_work_on_cancel_and_success_without_retaining_it_in_step() {
    let (mut session, turn) = crate::session::tests::make_session_and_context().await;
    let counts = Arc::new(Counts::default());
    session.services.host_admission = Some(Arc::new(Host {
        counts: counts.clone(),
        turn: turn.sub_id.clone(),
        store: session.services.thread_extension_data.level_id().into(),
    }));
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::new();
    builder.turn_input_contributor(Arc::new(Contributor(counts.clone())));
    session.services.extensions = Arc::new(builder.build());
    let session = Arc::new(session);
    let step = StepContext::for_test(Arc::new(turn));
    let cancel = CancellationToken::new();
    counts.pause.store(true, Ordering::SeqCst);
    let mut input = Box::pin(build_extension_turn_input_items(
        &session,
        &step,
        &[],
        &cancel,
    ));
    assert!(futures::poll!(&mut input).is_pending());
    assert_eq!(counts.live.load(Ordering::SeqCst), 2);
    drop(input);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    assert!(
        build_extension_turn_input_items(&session, &step, &[], &cancel)
            .await
            .is_some()
    );
    assert_eq!(counts.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        counts.live.load(Ordering::SeqCst),
        0,
        "retained step has no invocation lease"
    );
    counts.refuse.store(true, Ordering::SeqCst);
    assert!(
        build_extension_turn_input_items(&session, &step, &[], &cancel)
            .await
            .is_some()
    );
    assert_eq!(counts.refused.load(Ordering::SeqCst), 1);
    assert_eq!(counts.calls.load(Ordering::SeqCst), 2);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
}
