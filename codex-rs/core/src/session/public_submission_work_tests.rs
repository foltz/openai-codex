//! Actual public submission and Core dispatch with synthetic finite account
//! owners. No provider turn: regular work parks before sampling and the model
//! catalog is offline. This is not application/real-account qualification.
use super::*;
use crate::CodexThread;
use crate::session::tests::make_session_and_context;
use codex_extension_api::ExtensionData;
use codex_extension_api::ExtensionFuture;
use codex_extension_api::TurnStartAdmission;
use codex_extension_api::TurnWorkRefused;
use codex_protocol::host_turn_work::HostTurnWork;
use codex_protocol::turn_input::NotSubmittedReason;
use codex_protocol::turn_input::StartIfIdleSubmission;
use codex_protocol::turn_input::SteerSubmission;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

#[derive(Debug)]
struct Lease(Arc<AtomicUsize>);
impl Drop for Lease {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
#[derive(Debug, Default)]
struct Account {
    closed: AtomicBool,
    calls: AtomicUsize,
    live: Arc<AtomicUsize>,
    bindings: Mutex<Vec<String>>,
    retained: Mutex<BTreeMap<String, Vec<Lease>>>,
}
#[derive(Debug)]
struct Work {
    account: Arc<Account>,
    lease: Option<Lease>,
    turn: Option<String>,
}
impl HostTurnWork for Work {
    fn bind_submission(&mut self, turn: &str) {
        assert!(self.turn.is_none());
        self.turn = Some(turn.to_owned());
        self.account.bindings.lock().unwrap().push(turn.to_owned());
    }
    fn retain_until_terminal(mut self: Box<Self>) {
        let turn = self
            .turn
            .take()
            .expect("retained work must bind to actual turn");
        let lease = self.lease.take().unwrap();
        self.account
            .retained
            .lock()
            .unwrap()
            .entry(turn)
            .or_default()
            .push(lease);
    }
}
#[derive(Debug, Clone)]
struct Host(Arc<Account>);
impl Host {
    fn terminal(&self, turn: &str) {
        let leases = self.0.retained.lock().unwrap().remove(turn);
        drop(leases);
    }
}
impl TurnStartAdmission for Host {
    fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
        Some(Box::new(()))
    }
    fn requires_account_work(&self) -> bool {
        true
    }
    fn admit_turn_work(
        &self,
        _: &ExtensionData,
        _: ExtensionFuture<'static, ()>,
    ) -> Result<Option<Box<dyn HostTurnWork>>, TurnWorkRefused> {
        self.0.calls.fetch_add(1, Ordering::SeqCst);
        if self.0.closed.load(Ordering::SeqCst) {
            return Err(TurnWorkRefused::Unavailable);
        }
        self.0.live.fetch_add(1, Ordering::SeqCst);
        Ok(Some(Box::new(Work {
            account: Arc::clone(&self.0),
            lease: Some(Lease(Arc::clone(&self.0.live))),
            turn: None,
        })))
    }
}
impl codex_extension_api::TurnLifecycleContributor for Host {
    fn on_turn_stop<'a>(
        &'a self,
        input: codex_extension_api::TurnStopInput<'a>,
    ) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            self.terminal(input.turn_store.level_id());
        })
    }
    fn on_turn_abort<'a>(
        &'a self,
        input: codex_extension_api::TurnAbortInput<'a>,
    ) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            self.terminal(input.turn_store.level_id());
        })
    }
}
struct ParkRegular;
impl codex_extension_api::TurnLifecycleContributor for ParkRegular {
    fn turn_start_phase(&self, _: &ExtensionData) -> codex_extension_api::TurnStartPhase {
        codex_extension_api::TurnStartPhase::RegularTaskStart
    }
    fn on_turn_start<'a>(
        &'a self,
        _: codex_extension_api::TurnStartInput<'a>,
    ) -> ExtensionFuture<'a, ()> {
        Box::pin(std::future::pending())
    }
}
#[derive(Debug)]
struct OfflineCatalog;
impl codex_models_manager::manager::ModelsManager for OfflineCatalog {
    fn ensure_initialized(
        &self,
        _: codex_http_client::HttpClientFactory,
    ) -> codex_models_manager::manager::ModelsManagerFuture<'_, ()> {
        Box::pin(std::future::ready(()))
    }
    fn model_catalog_with_origins(
        &self,
        _: RefreshStrategy,
        _: codex_http_client::HttpClientFactory,
    ) -> codex_models_manager::manager::ModelsManagerFuture<
        '_,
        Vec<codex_models_manager::CatalogModel>,
    > {
        Box::pin(async {
            codex_models_manager::bundled_models_response()
                .unwrap()
                .models
                .into_iter()
                .map(|model| codex_models_manager::CatalogModel {
                    model: Arc::new(model),
                    origin: codex_models_manager::ModelOrigin::Bundled,
                })
                .collect()
        })
    }
    fn get_model_info<'a>(
        &'a self,
        model: &'a str,
        config: &'a codex_models_manager::ModelsManagerConfig,
    ) -> codex_models_manager::manager::ModelsManagerFuture<'a, codex_models_manager::CatalogModel>
    {
        Box::pin(async move {
            codex_models_manager::test_support::construct_model_info_offline_for_tests(
                model, config,
            )
        })
    }
    fn get_remote_models(
        &self,
    ) -> codex_models_manager::manager::ModelsManagerFuture<
        '_,
        Vec<codex_protocol::openai_models::ModelInfo>,
    > {
        Box::pin(async {
            codex_models_manager::bundled_models_response()
                .unwrap()
                .models
        })
    }
    fn try_get_remote_models(
        &self,
    ) -> Result<Vec<codex_protocol::openai_models::ModelInfo>, tokio::sync::TryLockError> {
        Ok(codex_models_manager::bundled_models_response()
            .unwrap()
            .models)
    }
    fn auth_manager(&self) -> Option<&codex_login::AuthManager> {
        None
    }
    fn list_collaboration_modes(&self) -> Vec<codex_protocol::config_types::CollaborationModeMask> {
        Vec::new()
    }
    fn refresh_if_new_etag(
        &self,
        _: String,
        _: codex_http_client::HttpClientFactory,
    ) -> codex_models_manager::manager::ModelsManagerFuture<'_, ()> {
        Box::pin(std::future::ready(()))
    }
}
async fn fixture() -> (CodexThread, Arc<Session>, Arc<Account>) {
    let (mut session, context) = make_session_and_context().await;
    let account = Arc::new(Account::default());
    let host = Arc::new(Host(Arc::clone(&account)));
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::new();
    builder.turn_start_admission(host.clone());
    builder.turn_lifecycle_contributor(host);
    builder.turn_lifecycle_contributor(Arc::new(ParkRegular));
    session.services.extensions = Arc::new(builder.build());
    session.services.host_admission = session.services.extensions.host_admission();
    session.services.models_manager = Arc::new(OfflineCatalog);
    let configured = SessionConfiguredEvent {
        session_id: session.session_id(),
        thread_id: session.thread_id,
        forked_from_id: None,
        parent_thread_id: None,
        thread_source: None,
        thread_name: None,
        model: "fixture".into(),
        model_provider_id: "fixture".into(),
        service_tier: None,
        approval_policy: codex_protocol::protocol::AskForApproval::Never,
        approvals_reviewer: codex_protocol::config_types::ApprovalsReviewer::User,
        permission_profile: codex_protocol::models::PermissionProfile::Disabled,
        active_permission_profile: None,
        cwd: context.config.cwd.clone(),
        reasoning_effort: None,
        network_proxy: None,
        rollout_path: None,
    };
    let session = Arc::new(session);
    let (tx, rx) = async_channel::bounded(8);
    let handle = tokio::spawn(handlers::submission_loop_with_dispatch(
        Arc::clone(&session),
        context.config.clone(),
        rx,
        None,
    ));
    let termination = session_loop_termination_from_handle(handle);
    let (_, events) = async_channel::unbounded();
    let io = SessionIo {
        tx_sub: tx.into(),
        rx_event: events,
        agent_status: tokio::sync::watch::channel(crate::agent::AgentStatus::PendingInit).1,
        session_loop_termination: termination,
    };
    let thread = CodexThread::new(
        Arc::clone(&session),
        io,
        crate::thread_startup_metadata::ThreadStartupMetadata::from(&configured),
        None,
        SessionSource::Exec,
    );
    (thread, session, account)
}
fn request(text: &str) -> TurnInputRequest {
    TurnInputRequest::user_input(vec![UserInput::Text {
        text: text.into(),
        text_elements: Vec::new(),
    }])
}
async fn finish(thread: &CodexThread) {
    thread.submit(Op::Shutdown).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), thread.wait_until_terminated())
        .await
        .unwrap();
}

#[tokio::test]
async fn public_started_steered_and_refused_paths_retain_only_the_installed_work() {
    let (thread, session, account) = fixture().await;
    let started = tokio::time::timeout(
        Duration::from_secs(10),
        thread.start_or_steer_turn(request("first")),
    )
    .await
    .unwrap()
    .unwrap();
    let TurnInputSubmission::Started { turn_id, .. } = started else {
        panic!("actual task installs")
    };
    assert_eq!(account.live.load(Ordering::SeqCst), 1);
    assert_eq!(
        account
            .retained
            .lock()
            .unwrap()
            .get(&turn_id)
            .unwrap()
            .len(),
        1
    );
    account.closed.store(true, Ordering::SeqCst);
    assert_eq!(
        thread
            .steer_turn(request("pure steering"), turn_id.clone())
            .await
            .unwrap(),
        SteerSubmission::Steered {
            turn_id: turn_id.clone()
        }
    );
    assert_eq!(account.calls.load(Ordering::SeqCst), 1);
    account.closed.store(false, Ordering::SeqCst);
    assert!(matches!(
        thread
            .start_or_steer_turn(request("may start or steer"))
            .await
            .unwrap(),
        TurnInputSubmission::Steered { .. }
    ));
    assert_eq!(
        thread
            .start_turn_if_idle(request("must refuse active"))
            .await
            .unwrap(),
        StartIfIdleSubmission::NotSubmitted {
            reason: NotSubmittedReason::NotIdle
        }
    );
    assert_eq!(account.calls.load(Ordering::SeqCst), 3);
    assert_eq!(account.live.load(Ordering::SeqCst), 1);
    assert_eq!(
        session.state.lock().await.last_started_turn_id.as_deref(),
        Some(turn_id.as_str())
    );
    finish(&thread).await;
    assert_eq!(account.live.load(Ordering::SeqCst), 0);
    assert!(account.retained.lock().unwrap().is_empty());
}

#[tokio::test]
async fn public_agent_acceptance_keeps_work_unbound_until_exact_worker_consumes_it() {
    let (thread, session, account) = fixture().await;
    let TurnInputSubmission::Started { turn_id, .. } =
        thread.start_or_steer_turn(request("first")).await.unwrap()
    else {
        panic!("task installs")
    };
    let message = codex_protocol::protocol::InterAgentCommunication::new(
        codex_protocol::AgentPath::root(),
        codex_protocol::AgentPath::root(),
        Vec::new(),
        "accepted agent input".into(),
        false,
    );
    assert_eq!(
        thread
            .start_or_steer_turn(TurnInputRequest::new(
                codex_protocol::turn_input::TurnInput::InterAgentCommunication(message)
            ))
            .await
            .unwrap(),
        TurnInputSubmission::Accepted
    );
    assert_eq!(account.calls.load(Ordering::SeqCst), 2);
    assert_eq!(account.live.load(Ordering::SeqCst), 2);
    assert_eq!(*account.bindings.lock().unwrap(), [turn_id.clone()]);
    assert_eq!(
        account
            .retained
            .lock()
            .unwrap()
            .get(&turn_id)
            .unwrap()
            .len(),
        1
    );
    let (items, _) = session
        .input_queue
        .get_pending_input(&session.active_turn, &turn_id)
        .await;
    assert_eq!(items.len(), 1);
    assert!(
        matches!(&items[0], crate::session::TurnInput::InterAgentCommunication(mail) if mail.trigger_turn)
    );
    assert_eq!(
        *account.bindings.lock().unwrap(),
        [turn_id.clone(), turn_id.clone()]
    );
    assert_eq!(
        account
            .retained
            .lock()
            .unwrap()
            .get(&turn_id)
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        account.calls.load(Ordering::SeqCst),
        2,
        "consumption derives no fresh admission"
    );
    finish(&thread).await;
    assert_eq!(account.live.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn closed_task_gate_drops_legacy_compact_and_review_work_without_fake_commit() {
    let (thread, session, account) = fixture().await;
    session.close_task_admission().await;
    let compact = thread.submit(Op::Compact).await.unwrap();
    let review = thread
        .submit(Op::Review {
            review_request: codex_protocol::protocol::ReviewRequest {
                target: codex_protocol::protocol::ReviewTarget::Custom {
                    instructions: "source test only".into(),
                },
                user_facing_hint: None,
            },
        })
        .await
        .unwrap();
    finish(&thread).await;
    assert_eq!(account.calls.load(Ordering::SeqCst), 2);
    assert_eq!(*account.bindings.lock().unwrap(), [compact, review]);
    assert_eq!(account.live.load(Ordering::SeqCst), 0);
    assert!(account.retained.lock().unwrap().is_empty());
    assert!(session.state.lock().await.last_started_turn_id.is_none());
}
