//! Phase-level authority forwarding; all account work is synthetic.
use super::*;
use crate::session::tests::make_session_and_context;
use codex_extension_api::{
    ExtensionData, ExtensionFuture, HostOperationWork, TurnStartAdmission, TurnWorkRefused,
};
use codex_protocol::host_turn_work::HostTurnWork;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Default)]
struct Gate {
    live: Arc<AtomicUsize>,
    turn_derives: AtomicUsize,
}
#[derive(Debug)]
struct Work(Arc<AtomicUsize>);
impl Drop for Work {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl HostOperationWork for Work {
    fn derive_operation(&self) -> Result<Box<dyn HostOperationWork>, TurnWorkRefused> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(Self(self.0.clone())))
    }
    fn derive_turn_work(
        &self,
        _: &ExtensionData,
        _: ExtensionFuture<'static, ()>,
    ) -> Result<Box<dyn HostTurnWork>, TurnWorkRefused> {
        Err(TurnWorkRefused::Unavailable)
    }
}
impl TurnStartAdmission for Gate {
    fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
        panic!("no fresh fallback")
    }
    fn requires_account_work(&self) -> bool {
        true
    }
    fn derive_operation_work(
        &self,
        _: &ExtensionData,
        _: &str,
    ) -> Result<Option<Box<dyn HostOperationWork>>, TurnWorkRefused> {
        self.turn_derives.fetch_add(1, Ordering::SeqCst);
        self.live.fetch_add(1, Ordering::SeqCst);
        Ok(Some(Box::new(Work(self.live.clone()))))
    }
}
struct Before(Arc<AtomicUsize>);
impl codex_extension_api::TurnLifecycleContributor for Before {
    fn on_turn_start<'a>(
        &'a self,
        input: codex_extension_api::TurnStartInput<'a>,
    ) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            assert!(
                input.mcp_access.is_err(),
                "pre-registration is no committed turn authority"
            );
            self.0.fetch_add(1, Ordering::SeqCst);
        })
    }
}
#[tokio::test]
async fn required_pre_registration_callbacks_get_explicit_refusal_without_turn_derivation() {
    let (mut session, turn) = make_session_and_context().await;
    let gate = Arc::new(Gate::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::new();
    builder.turn_start_admission(gate.clone());
    builder.turn_lifecycle_contributor(Arc::new(Before(calls.clone())));
    session.services.extensions = Arc::new(builder.build());
    session.services.host_admission = session.services.extensions.host_admission();
    Arc::new(session)
        .emit_turn_start_lifecycle(&turn, None, TurnStartPhase::BeforeTaskRegistration)
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(gate.turn_derives.load(Ordering::SeqCst), 0);
    assert_eq!(gate.live.load(Ordering::SeqCst), 0);
}
struct Held {
    entered: tokio::sync::Notify,
}
impl codex_extension_api::TurnLifecycleContributor for Held {
    fn turn_start_phase(&self, _: &ExtensionData) -> TurnStartPhase {
        TurnStartPhase::RegularTaskStart
    }
    fn requires_mcp_runtime(&self, _: &ExtensionData) -> bool {
        true
    }
    fn on_turn_start<'a>(
        &'a self,
        input: codex_extension_api::TurnStartInput<'a>,
    ) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            let access = input.mcp_access.expect("live turn authority");
            let codex_mcp::McpAttemptAccess::Admitted(work) = access else {
                panic!("required callback cannot be unscoped")
            };
            let child = work.derive_attempt().unwrap();
            self.entered.notify_one();
            std::future::pending::<()>().await;
            drop(child);
        })
    }
}
#[tokio::test]
async fn regular_callback_borrows_finite_work_and_cancellation_releases_it() {
    let (mut session, turn) = make_session_and_context().await;
    let gate = Arc::new(Gate::default());
    let held = Arc::new(Held {
        entered: Default::default(),
    });
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::new();
    builder.turn_start_admission(gate.clone());
    builder.turn_lifecycle_contributor(held.clone());
    session.services.extensions = Arc::new(builder.build());
    session.services.host_admission = session.services.extensions.host_admission();
    let session = Arc::new(session);
    let running = tokio::spawn(async move {
        session
            .emit_turn_start_lifecycle(&turn, None, TurnStartPhase::RegularTaskStart)
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), held.entered.notified())
        .await
        .unwrap();
    assert!(
        gate.live.load(Ordering::SeqCst) >= 2,
        "callback and independently derived child remain counted"
    );
    running.abort();
    assert!(matches!(running.await, Err(error) if error.is_cancelled()));
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while gate.live.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(gate.turn_derives.load(Ordering::SeqCst), 1);
}

struct LocalRegular(Arc<AtomicUsize>);
impl codex_extension_api::TurnLifecycleContributor for LocalRegular {
    fn turn_start_phase(&self, _: &ExtensionData) -> TurnStartPhase {
        TurnStartPhase::RegularTaskStart
    }
    fn on_turn_start<'a>(
        &'a self,
        input: codex_extension_api::TurnStartInput<'a>,
    ) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            assert!(input.mcp_access.is_err());
            self.0.fetch_add(1, Ordering::SeqCst);
        })
    }
}
#[tokio::test]
async fn required_local_regular_callbacks_cannot_mistake_absent_work_for_unscoped_authority() {
    let (mut session, turn) = make_session_and_context().await;
    let gate = Arc::new(Gate::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::new();
    builder.turn_start_admission(gate.clone());
    builder.turn_lifecycle_contributor(Arc::new(LocalRegular(calls.clone())));
    session.services.extensions = Arc::new(builder.build());
    session.services.host_admission = session.services.extensions.host_admission();
    Arc::new(session)
        .emit_turn_start_lifecycle(&turn, None, TurnStartPhase::RegularTaskStart)
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(gate.turn_derives.load(Ordering::SeqCst), 0);
    assert_eq!(gate.live.load(Ordering::SeqCst), 0);
}

#[derive(Debug)]
struct IdleGate {
    live: Arc<AtomicUsize>,
    calls: AtomicUsize,
    refusal: usize,
}
impl TurnStartAdmission for IdleGate {
    fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
        panic!("not turn admission")
    }
    fn requires_account_work(&self) -> bool {
        true
    }
    fn derive_request_operation_work(
        &self,
    ) -> Result<Option<Box<dyn HostOperationWork>>, TurnWorkRefused> {
        panic!("idle is not an ambient request")
    }
    fn derive_operation_work(
        &self,
        _: &ExtensionData,
        _: &str,
    ) -> Result<Option<Box<dyn HostOperationWork>>, TurnWorkRefused> {
        panic!("idle has no live turn")
    }
    fn admit_operation_work(&self) -> Result<Option<Box<dyn HostOperationWork>>, TurnWorkRefused> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match self.refusal {
            0 => {
                self.live.fetch_add(1, Ordering::SeqCst);
                Ok(Some(Box::new(Work(self.live.clone()))))
            }
            1 => Ok(None),
            2 => Err(TurnWorkRefused::Unavailable),
            _ => Err(TurnWorkRefused::RetryAfter(
                Box::pin(std::future::pending()),
            )),
        }
    }
}
struct IdleObserver {
    entered: AtomicUsize,
    refused: AtomicUsize,
    unscoped: AtomicUsize,
    hold: bool,
    detached: bool,
    join: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}
impl codex_extension_api::ThreadLifecycleContributor<crate::config::Config> for IdleObserver {
    fn on_thread_idle<'a>(
        &'a self,
        input: codex_extension_api::ThreadIdleInput<'a>,
    ) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            self.entered.fetch_add(1, Ordering::SeqCst);
            let child = match input.account_work {
                Ok(Some(work)) => work.derive_operation().unwrap(),
                Ok(None) => {
                    self.unscoped.fetch_add(1, Ordering::SeqCst);
                    return;
                }
                Err(_) => {
                    self.refused.fetch_add(1, Ordering::SeqCst);
                    return;
                }
            };
            if self.detached {
                *self.join.lock().unwrap() = Some(tokio::spawn(async move {
                    let _child = child;
                    std::future::pending::<()>().await;
                }));
            } else {
                let _child = child;
                if self.hold {
                    std::future::pending::<()>().await;
                }
            }
        })
    }
}
fn idle_fixture(
    session: &mut Session,
    refusal: usize,
    hold: bool,
    detached: bool,
) -> (Arc<IdleGate>, Arc<IdleObserver>) {
    let gate = Arc::new(IdleGate {
        live: Default::default(),
        calls: AtomicUsize::new(0),
        refusal,
    });
    let observer = Arc::new(IdleObserver {
        entered: AtomicUsize::new(0),
        refused: AtomicUsize::new(0),
        unscoped: AtomicUsize::new(0),
        hold,
        detached,
        join: Default::default(),
    });
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::new();
    builder.turn_start_admission(gate.clone());
    builder.thread_lifecycle_contributor(observer.clone());
    session.services.extensions = Arc::new(builder.build());
    session.services.host_admission = session.services.extensions.host_admission();
    (gate, observer)
}
#[tokio::test]
async fn idle_admission_precedes_active_turn_wait_and_cancel_releases_it() {
    let (mut session, _) = make_session_and_context().await;
    let (gate, observer) = idle_fixture(&mut session, 0, false, false);
    let held = session.active_turn.lock().await;
    let mut idle = Box::pin(session.emit_thread_idle_lifecycle_if_idle(ThreadIdleCause::Completed));
    assert!(futures::poll!(&mut idle).is_pending());
    assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
    assert_eq!(gate.live.load(Ordering::SeqCst), 1);
    assert_eq!(observer.entered.load(Ordering::SeqCst), 0);
    drop(idle);
    assert_eq!(gate.live.load(Ordering::SeqCst), 0);
    drop(held);
}
#[tokio::test]
async fn cancelled_idle_callback_releases_group_and_derived_work() {
    fn require_sync<T: Sync>() {}
    require_sync::<codex_extension_api::ThreadIdleInput<'static>>();
    let (mut session, _) = make_session_and_context().await;
    let (gate, observer) = idle_fixture(&mut session, 0, true, false);
    let mut idle = Box::pin(session.emit_thread_idle_lifecycle_if_idle(ThreadIdleCause::Completed));
    assert!(futures::poll!(&mut idle).is_pending());
    assert_eq!(observer.entered.load(Ordering::SeqCst), 1);
    assert_eq!(gate.live.load(Ordering::SeqCst), 2);
    drop(idle);
    assert_eq!(gate.live.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn idle_required_missing_unavailable_or_retry_is_refused_not_unscoped() {
    for refusal in [1, 2, 3] {
        let (mut session, _) = make_session_and_context().await;
        let (gate, observer) = idle_fixture(&mut session, refusal, false, false);
        session
            .emit_thread_idle_lifecycle_if_idle(ThreadIdleCause::Completed)
            .await;
        assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
        assert_eq!(observer.entered.load(Ordering::SeqCst), 1);
        assert_eq!(observer.refused.load(Ordering::SeqCst), 1);
        assert_eq!(observer.unscoped.load(Ordering::SeqCst), 0);
        assert_eq!(gate.live.load(Ordering::SeqCst), 0);
    }
}
#[tokio::test]
async fn idle_detached_child_retains_its_own_work_until_abort_is_joined() {
    let (mut session, _) = make_session_and_context().await;
    let (gate, observer) = idle_fixture(&mut session, 0, false, true);
    session
        .emit_thread_idle_lifecycle_if_idle(ThreadIdleCause::Completed)
        .await;
    assert_eq!(gate.live.load(Ordering::SeqCst), 1);
    let join = observer.join.lock().unwrap().take().unwrap();
    join.abort();
    assert!(join.await.unwrap_err().is_cancelled());
    assert_eq!(gate.live.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn hook_free_idle_does_not_admit_or_wait_and_ungated_metadata_still_runs() {
    let (mut session, _) = make_session_and_context().await;
    let (gate, observer) = idle_fixture(&mut session, 2, false, false);
    session.services.extensions =
        Arc::new(codex_extension_api::ExtensionRegistryBuilder::new().build());
    let held = session.active_turn.lock().await;
    let mut idle = Box::pin(session.emit_thread_idle_lifecycle_if_idle(ThreadIdleCause::Completed));
    assert!(futures::poll!(&mut idle).is_ready());
    assert_eq!(gate.calls.load(Ordering::SeqCst), 0);
    drop(idle);
    drop(held);
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::new();
    builder.thread_lifecycle_contributor(observer.clone());
    session.services.extensions = Arc::new(builder.build());
    session.services.host_admission = None;
    session
        .emit_thread_idle_lifecycle_if_idle(ThreadIdleCause::Completed)
        .await;
    assert_eq!(observer.unscoped.load(Ordering::SeqCst), 1);
}
