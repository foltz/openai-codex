use super::*;
use codex_extension_api::ExtensionData;
use codex_extension_api::ExtensionFuture;
use codex_extension_api::TurnStartAdmission;
use codex_extension_api::TurnWorkRefused;
use codex_protocol::host_turn_work::HostTurnWork;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

#[derive(Debug)]
struct ParentGate {
    parent_store: String,
    calls: AtomicUsize,
    events: Arc<Mutex<Vec<String>>>,
}

#[derive(Debug)]
struct ChildWork(Arc<Mutex<Vec<String>>>);

impl HostTurnWork for ChildWork {
    fn bind_submission(&mut self, turn_id: &str) {
        self.0.lock().unwrap().push(format!("bound:{turn_id}"));
    }
    fn retain_until_terminal(self: Box<Self>) {
        self.0.lock().unwrap().push("retained".into());
    }
}

impl Drop for ChildWork {
    fn drop(&mut self) { self.0.lock().unwrap().push("drop".into()); }
}

impl TurnStartAdmission for ParentGate {
    fn admit_turn_start(&self) -> Option<Box<dyn Send>> { None }
    fn admit_turn_work(&self, _store: &ExtensionData, _end: ExtensionFuture<'static, ()>)
        -> Result<Option<Box<dyn HostTurnWork>>, TurnWorkRefused> {
        Err(TurnWorkRefused::Unavailable)
    }
    fn derive_turn_work(&self, parent: &ExtensionData, turn: &str, _child: &ExtensionData,
        _end: ExtensionFuture<'static, ()>)
        -> Result<Option<Box<dyn HostTurnWork>>, TurnWorkRefused> {
        assert_eq!(parent.level_id(), self.parent_store);
        self.calls.fetch_add(1, Ordering::SeqCst);
        if turn != "live-parent" { return Err(TurnWorkRefused::Unavailable); }
        Ok(Some(Box::new(ChildWork(Arc::clone(&self.events)))))
    }
}

async fn parent() -> (Arc<crate::session::Session>, Arc<ParentGate>) {
    let (mut session, _) = crate::session::tests::make_session_and_context().await;
    let gate = Arc::new(ParentGate {
        parent_store: session.services.thread_extension_data.level_id().to_owned(),
        calls: AtomicUsize::new(0),
        events: Arc::new(Mutex::new(Vec::new())),
    });
    let mut extensions = codex_extension_api::ExtensionRegistryBuilder::<Config>::new();
    extensions.turn_start_admission(gate.clone());
    session.services.extensions = Arc::new(extensions.build());
    (Arc::new(session), gate)
}

#[tokio::test]
async fn trigger_operation_owns_derived_work_until_mail_is_consumed() {
    let harness = AgentControlHarness::new().await;
    let (id, child) = harness.start_thread().await;
    child.session.close_task_admission().await;
    let (parent, gate) = parent().await;
    let authority = crate::ParentTurnAuthority::capture(&parent, "live-parent");
    harness.control.send_inter_agent_communication(
        id,
        InterAgentCommunication::new(AgentPath::root(), AgentPath::root(), Vec::new(),
            "carried".into(), /*trigger_turn*/ true),
        AgentCommunicationContext::new(AgentCommunicationKind::Followup, parent.thread_id),
        Default::default(), Some(&authority),
    ).await.unwrap();
    timeout(Duration::from_secs(10), async {
        while !child.session.input_queue.has_pending_mailbox_items().await {
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
    assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
    assert!(gate.events.lock().unwrap().is_empty(), "enqueue is not turn binding");
    let batch = child.session.input_queue.reserve_mailbox();
    assert!(batch.has_turn_work());
    let _ = batch.into_input("consuming-turn");
    assert_eq!(*gate.events.lock().unwrap(), vec!["bound:consuming-turn", "retained", "drop"]);
    child.shutdown_and_wait().await.unwrap();
}

#[tokio::test]
async fn expired_parent_refuses_input_and_trigger_but_queue_only_has_no_lease() {
    let harness = AgentControlHarness::new().await;
    let (id, child) = harness.start_thread().await;
    let (parent, gate) = parent().await;
    let authority = crate::ParentTurnAuthority::capture(&parent, "ended-parent");
    assert!(harness.control.send_input(id, text_input("refused"), Default::default(),
        Some(&authority)).await.is_err());
    let mail = |trigger| InterAgentCommunication::new(AgentPath::root(), AgentPath::root(),
        Vec::new(), "mail".into(), trigger);
    let context = AgentCommunicationContext::new(AgentCommunicationKind::Followup, parent.thread_id);
    assert!(harness.control.send_inter_agent_communication(id, mail(true), context.clone(),
        Default::default(), Some(&authority)).await.is_err());
    harness.control.send_inter_agent_communication(id, mail(false), context,
        Default::default(), Some(&authority)).await.unwrap();
    timeout(Duration::from_secs(10), async {
        while !child.session.input_queue.has_pending_mailbox_items().await {
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
    assert_eq!(gate.calls.load(Ordering::SeqCst), 2);
    assert!(!child.session.input_queue.reserve_mailbox().has_turn_work());
    let weak = Arc::downgrade(&parent);
    drop(parent);
    assert!(weak.upgrade().is_none(), "provenance must not own the parent");
    assert!(authority.derive(&child).is_err());
    child.shutdown_and_wait().await.unwrap();
}
