use super::*;
use crate::session::tests::make_session_and_context;
use codex_extension_api::ExtensionFuture;
use codex_extension_api::TurnStartAdmission;
use codex_extension_api::TurnWorkRefused;
use codex_http_client::HttpClientFactory;
use codex_models_manager::manager::ModelsManager;
use codex_models_manager::manager::ModelsManagerFuture;
use codex_models_manager::manager::RefreshStrategy;
use codex_protocol::host_turn_work::HostTurnWork;
use codex_protocol::openai_models::ModelInfo;
use codex_protocol::openai_models::ModelsResponse;
use codex_protocol::protocol::InterAgentCommunication;
use futures::FutureExt;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

#[derive(Debug)]
struct Gate {
    closed: bool,
    events: Arc<Mutex<Vec<String>>>,
}

#[derive(Debug)]
struct Work {
    events: Arc<Mutex<Vec<String>>>,
    retained: bool,
}

impl HostTurnWork for Work {
    fn bind_submission(&mut self, turn_id: &str) {
        self.events.lock().unwrap().push(format!("bind:{turn_id}"));
    }
    fn retain_until_terminal(mut self: Box<Self>) {
        self.retained = true;
        self.events.lock().unwrap().push("retained".into());
    }
}

impl Drop for Work {
    fn drop(&mut self) {
        if !self.retained {
            self.events.lock().unwrap().push("dropped".into());
        }
    }
}

impl TurnStartAdmission for Gate {
    fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
        panic!("mailbox keeps the upstream shutdown-admission bypass");
    }
    fn admit_turn_work(
        &self,
        _store: &codex_extension_api::ExtensionData,
        _termination: ExtensionFuture<'static, ()>,
    ) -> Result<Option<Box<dyn HostTurnWork>>, TurnWorkRefused> {
        self.events.lock().unwrap().push("admit".into());
        if self.closed { return Err(TurnWorkRefused::Unavailable); }
        Ok(Some(Box::new(Work { events: Arc::clone(&self.events), retained: false })))
    }
}

#[derive(Debug)]
struct RefreshCounter {
    inner: Arc<dyn ModelsManager>,
    calls: AtomicUsize,
}

impl ModelsManager for RefreshCounter {
    fn raw_model_catalog(&self, strategy: RefreshStrategy, factory: HttpClientFactory)
        -> ModelsManagerFuture<'_, ModelsResponse> {
        self.inner.raw_model_catalog(strategy, factory)
    }
    fn get_remote_models(&self) -> ModelsManagerFuture<'_, Vec<ModelInfo>> {
        self.inner.get_remote_models()
    }
    fn try_get_remote_models(&self) -> Result<Vec<ModelInfo>, tokio::sync::TryLockError> {
        self.inner.try_get_remote_models()
    }
    fn auth_manager(&self) -> Option<&codex_login::AuthManager> { self.inner.auth_manager() }
    fn list_collaboration_modes(&self) -> Vec<codex_protocol::config_types::CollaborationModeMask> {
        self.inner.list_collaboration_modes()
    }
    fn refresh_if_new_etag(&self, etag: String, factory: HttpClientFactory) -> ModelsManagerFuture<'_, ()> {
        self.inner.refresh_if_new_etag(etag, factory)
    }
    fn refresh_after_auth_change(&self, _factory: HttpClientFactory) -> ModelsManagerFuture<'_, ()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(std::future::ready(()))
    }
}

struct HoldStart(tokio::sync::Notify);
impl codex_extension_api::TurnLifecycleContributor for HoldStart {
    fn on_turn_start<'a>(&'a self, _input: codex_extension_api::TurnStartInput<'a>) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            self.0.notify_one();
            std::future::pending().await
        })
    }
}

struct HoldRunningTurn;
impl codex_extension_api::TurnLifecycleContributor for HoldRunningTurn {
    fn turn_start_phase(&self, _store: &codex_extension_api::ExtensionData) -> codex_extension_api::TurnStartPhase {
        codex_extension_api::TurnStartPhase::RegularTaskStart
    }
    fn on_turn_start<'a>(&'a self, _input: codex_extension_api::TurnStartInput<'a>) -> ExtensionFuture<'a, ()> {
        Box::pin(std::future::pending())
    }
}

#[derive(Debug)]
struct ReopenGate {
    open: tokio::sync::watch::Receiver<bool>,
    refused: tokio::sync::Notify,
    attempts: AtomicUsize,
}

impl TurnStartAdmission for ReopenGate {
    fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
        Some(Box::new(()))
    }

    fn admit_turn_work(
        &self,
        _store: &codex_extension_api::ExtensionData,
        _termination: ExtensionFuture<'static, ()>,
    ) -> Result<Option<Box<dyn HostTurnWork>>, TurnWorkRefused> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        if *self.open.borrow() {
            return Ok(None);
        }
        self.refused.notify_one();
        let mut open = self.open.clone();
        Err(TurnWorkRefused::RetryAfter(Box::pin(async move {
            open.wait_for(|open| *open).await.unwrap();
        })))
    }
}

#[test_case::test_case(true; "reopen retries without unrelated input")]
#[test_case::test_case(false; "closed barrier does not prevent loop exit")]
#[tokio::test]
async fn mailbox_retries_after_reopen_without_an_unrelated_submission(reopen: bool) {
    let (mut session, context) = make_session_and_context().await;
    let (open, receiver) = tokio::sync::watch::channel(false);
    let gate = Arc::new(ReopenGate {
        open: receiver,
        refused: tokio::sync::Notify::new(),
        attempts: AtomicUsize::new(0),
    });
    let start = Arc::new(HoldStart(tokio::sync::Notify::new()));
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::<crate::config::Config>::new();
    builder.turn_start_admission(gate.clone());
    builder.turn_lifecycle_contributor(start.clone());
    session.services.extensions = Arc::new(builder.build());
    session.services.host_admission = session.services.extensions.host_admission();
    let models = Arc::new(RefreshCounter {
        inner: Arc::clone(&session.services.models_manager),
        calls: AtomicUsize::new(0),
    });
    session.services.models_manager = models.clone();
    let session = Arc::new(session);
    session.input_queue.enqueue_mailbox_communication(
        InterAgentCommunication::new(
            codex_protocol::AgentPath::root(), codex_protocol::AgentPath::root(),
            Vec::new(), "retry".into(), /*trigger_turn*/ true,
        ), Default::default(),
    ).await;
    session.input_queue.completion_wake.notify_one();
    let (sender, receiver) = async_channel::bounded(1);
    let loop_task = tokio::spawn(super::super::handlers::submission_loop(
        Arc::clone(&session), context.config.clone(), receiver, None,
    ));
    tokio::time::timeout(Duration::from_secs(10), gate.refused.notified()).await.unwrap();
    assert!(session.active_turn.lock().await.is_none());
    assert_eq!(models.calls.load(Ordering::SeqCst), 0);
    assert!(session.input_queue.has_pending_mailbox_items().await);
    if reopen {
        open.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(10), start.0.notified()).await.unwrap();
        assert_eq!(gate.attempts.load(Ordering::SeqCst), 2);
        assert_eq!(models.calls.load(Ordering::SeqCst), 1);
    }
    // A held preparation and the retry observer must not prevent loop exit.
    drop(sender);
    tokio::time::timeout(Duration::from_secs(10), loop_task).await.unwrap().unwrap();
    if !reopen {
        assert_eq!(gate.attempts.load(Ordering::SeqCst), 1);
        assert_eq!(models.calls.load(Ordering::SeqCst), 0);
    }
}

#[test_case::test_case(true; "trigger mail")]
#[test_case::test_case(false; "queue-only durable-sleep wake")]
#[tokio::test]
async fn refused_mailbox_start_preserves_mail_without_model_refresh(trigger: bool) {
    let (mut session, _) = make_session_and_context().await;
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::<crate::config::Config>::new();
    builder.turn_start_admission(Arc::new(Gate { closed: true, events: Arc::clone(&events) }));
    session.services.extensions = Arc::new(builder.build());
    session.services.host_admission = session.services.extensions.host_admission();
    let models = Arc::new(RefreshCounter { inner: Arc::clone(&session.services.models_manager), calls: AtomicUsize::new(0) });
    session.services.models_manager = models.clone();
    if !trigger {
        session.services.thread_extension_data.insert(codex_extension_items::sleep::SleepItem { id: "sleep".into(), duration_ms: 1000 });
    }
    let session = Arc::new(session);
    let mail = InterAgentCommunication::new(codex_protocol::AgentPath::root(), codex_protocol::AgentPath::root(), Vec::new(), "queued".into(), trigger);
    session.input_queue.enqueue_mailbox_communication(mail.clone(), Default::default()).await;
    session.maybe_start_turn_for_pending_work_with_sub_id("refused".into()).await;
    assert_eq!(*events.lock().unwrap(), vec!["admit"]);
    assert_eq!(models.calls.load(Ordering::SeqCst), 0);
    assert!(session.active_turn.lock().await.is_none());
    assert_eq!(session.input_queue.drain_mailbox_input_items().await.0,
        vec![crate::session::TurnInput::InterAgentCommunication(mail)]);
}

#[test_case::test_case(true; "carried authority survives closed barrier")]
#[test_case::test_case(false; "fresh work releases on cancelled preparation")]
#[tokio::test]
async fn mailbox_preparation_owns_its_admission(carried: bool) {
    let (mut session, _) = make_session_and_context().await;
    let events = Arc::new(Mutex::new(Vec::new()));
    let hold = Arc::new(HoldStart(tokio::sync::Notify::new()));
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::<crate::config::Config>::new();
    builder.turn_start_admission(Arc::new(Gate { closed: carried, events: Arc::clone(&events) }));
    builder.turn_lifecycle_contributor(hold.clone());
    session.services.extensions = Arc::new(builder.build());
    session.services.host_admission = session.services.extensions.host_admission();
    let session = Arc::new(session);
    let mail = InterAgentCommunication::new(codex_protocol::AgentPath::root(), codex_protocol::AgentPath::root(), Vec::new(), "queued".into(), /*trigger_turn*/ true);
    let work = carried.then(|| Box::new(Work { events: Arc::clone(&events), retained: false }) as Box<dyn HostTurnWork>);
    session.input_queue.enqueue_mailbox_with_work(mail, Default::default(), work);
    let mut start = Box::pin(session.maybe_start_turn_for_pending_work_with_sub_id("consumer".into()));
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::select! {
            _ = &mut start => panic!("preparation must be held"),
            _ = hold.0.notified() => (),
        }
    }).await.unwrap();
    drop(start);
    assert!(session.input_queue.has_pending_mailbox_items().await);
    assert_eq!(session.input_queue.reserve_mailbox().has_turn_work(), carried);
    let expected = if carried { Vec::<String>::new() } else { vec!["admit".into(), "bind:consumer".into(), "dropped".into()] };
    assert_eq!(*events.lock().unwrap(), expected);
}

#[tokio::test]
async fn installed_mailbox_turn_transfers_fresh_work_to_terminal_custody() {
    let (mut session, _) = make_session_and_context().await;
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::<crate::config::Config>::new();
    builder.turn_start_admission(Arc::new(Gate { closed: false, events: Arc::clone(&events) }));
    builder.turn_lifecycle_contributor(Arc::new(HoldRunningTurn));
    session.services.extensions = Arc::new(builder.build());
    session.services.host_admission = session.services.extensions.host_admission();
    let session = Arc::new(session);
    let mail = InterAgentCommunication::new(codex_protocol::AgentPath::root(), codex_protocol::AgentPath::root(), Vec::new(), "queued".into(), /*trigger_turn*/ true);
    session.input_queue.enqueue_mailbox_communication(mail, Default::default()).await;
    session.maybe_start_turn_for_pending_work_with_sub_id("installed".into()).await;
    assert_eq!(*events.lock().unwrap(), vec!["admit", "bind:installed", "retained"]);
    assert_eq!(session.active_turn.lock().await.as_ref().unwrap().task.as_ref().unwrap().turn_context.sub_id, "installed");
    session.close_task_admission().await;
    session.abort_all_tasks(codex_protocol::protocol::TurnAbortReason::Interrupted).await;
    let _ = session.task_joins.shutdown_until(tokio::time::Instant::now() + Duration::from_secs(5)).await;
}

#[test_case::test_case(false; "normal exit")]
#[test_case::test_case(true; "cancelled loop")]
#[tokio::test]
async fn session_work_receipt_observes_the_same_join_without_retaining_session(cancel: bool) {
    let (session, _) = make_session_and_context().await;
    let session = Arc::new(session);
    let weak = Arc::downgrade(&session);
    let (release, held) = tokio::sync::oneshot::channel();
    let termination = crate::session::session_loop_termination_from_handle(tokio::spawn(async move { let _ = held.await; }));
    session.services.thread_extension_data.insert(SessionLoopWorkReceipt(termination.completion.clone()));
    let first = session.turn_work_termination();
    let mut second = session.turn_work_termination();
    assert!(second.as_mut().now_or_never().is_none());
    drop(first);
    drop(session);
    assert!(weak.upgrade().is_none());
    let expected = if cancel {
        termination.request_abort();
        SessionLoopOutcome::Cancelled
    } else {
        release.send(()).unwrap();
        SessionLoopOutcome::Normal
    };
    tokio::time::timeout(Duration::from_secs(5), second).await.unwrap();
    assert_eq!(termination.observed(), Some(expected));
}

#[tokio::test]
async fn session_without_a_loop_never_reports_termination() {
    let (session, _) = make_session_and_context().await;
    assert!(session.turn_work_termination().now_or_never().is_none());
}

#[test_case::test_case(true; "closed handoff refuses before install")]
#[test_case::test_case(false; "open handoff retains only a started turn")]
#[tokio::test]
async fn realtime_handoff_owns_turn_admission(closed: bool) {
    let (mut session, _) = make_session_and_context().await;
    let events = Arc::new(Mutex::new(Vec::new()));
    // Keep the shutdown gate independent: this host only supplies account work.
    session.services.host_admission = Some(Arc::new(Gate {
        closed,
        events: Arc::clone(&events),
    }));
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::<crate::config::Config>::new();
    builder.turn_lifecycle_contributor(Arc::new(HoldRunningTurn));
    session.services.extensions = Arc::new(builder.build());
    let session = Arc::new(session);
    let result = session.route_realtime_text_input("handoff".into()).await;
    if closed {
        assert_eq!(result, Err("Server is draining; retry the turn after reconnecting"));
        assert_eq!(*events.lock().unwrap(), vec!["admit"]);
        assert!(session.active_turn.lock().await.is_none());
        assert_eq!(session.state.lock().await.last_started_turn_id, None);
        return;
    }
    assert_eq!(result, Ok(()));
    let turn_id = session.active_turn.lock().await.as_ref().unwrap()
        .task.as_ref().unwrap().turn_context.sub_id.clone();
    assert_eq!(*events.lock().unwrap(), vec![
        "admit".to_string(), format!("bind:{turn_id}"), "retained".to_string(),
    ]);
    events.lock().unwrap().clear();
    assert_eq!(session.route_realtime_text_input("steering".into()).await, Ok(()));
    let steering_events = events.lock().unwrap().clone();
    assert_eq!(steering_events.len(), 3);
    let steering_id = steering_events[1].strip_prefix("bind:").unwrap();
    assert_ne!(steering_id, turn_id);
    assert_eq!(steering_events, vec![
        "admit".to_string(), format!("bind:{steering_id}"), "dropped".to_string(),
    ]);
    assert_eq!(session.active_turn.lock().await.as_ref().unwrap()
        .task.as_ref().unwrap().turn_context.sub_id, turn_id);
    session.close_task_admission().await;
    session.abort_all_tasks(codex_protocol::protocol::TurnAbortReason::Interrupted).await;
    let _ = session.task_joins.shutdown_until(tokio::time::Instant::now() + Duration::from_secs(5)).await;
}
