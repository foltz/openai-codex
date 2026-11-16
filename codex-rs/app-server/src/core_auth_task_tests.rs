//! Actual Core startup -> provider -> counted original auth join, not a counter-only substitute.
use crate::account_turn_admission::{AccountTurnAdmission, within_request};
use crate::account_turn_work::AccountTurnWork;
use crate::account_work_permits::AccountWorkPermits;
use crate::managed_transition::DRAIN_DEADLINE;
use crate::processor_task_retirement::{ProcessorTaskTicket, ProcessorTasks};
use codex_extension_api::{ExtensionRegistryBuilder, TurnStartAdmission};
use codex_login::{AuthManager, CodexAuth, ExternalAuth};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use tokio::sync::Semaphore;
use tokio::time::Instant;

#[derive(Debug)]
struct HeldAuth {
    tasks: Weak<ProcessorTasks>,
    held: AtomicBool,
    entered: Semaphore,
    release: Semaphore,
    finished: AtomicBool,
    nested: bool,
}
impl ExternalAuth for HeldAuth {
    fn refresh(
        &self,
        _: codex_login::ExternalAuthRefreshContext,
    ) -> codex_login::ExternalAuthFuture<'_, CodexAuth> {
        Box::pin(async { panic!("synthetic startup must not refresh external credentials") })
    }
    fn resolve(&self) -> codex_login::ExternalAuthFuture<'_, CodexAuth> {
        Box::pin(async move {
            // The initial constructor's unscoped auth is a separate open producer.
            // Hold only a real child already registered in this strict owner.
            let owned = self.tasks.upgrade().is_some_and(|tasks| tasks.len() > 0);
            if owned && !self.held.swap(true, Ordering::SeqCst) {
                self.entered.add_permits(1);
                self.release.acquire().await.unwrap().forget();
                if self.nested {
                    let mut info =
                        codex_model_provider_info::ModelProviderInfo::create_openai_provider(None);
                    info.experimental_bearer_token = Some("synthetic-core-nested".into());
                    codex_model_provider::create_model_provider(info, None)
                        .request_auth(None)
                        .await
                        .expect("nested descendant derives during closed fresh admission");
                }
                self.finished.store(true, Ordering::SeqCst);
            }
            Ok(CodexAuth::create_dummy_chatgpt_auth_for_testing())
        })
    }
}
fn host(permits: &AccountWorkPermits, ticket: Option<ProcessorTaskTicket>) -> AccountTurnAdmission {
    AccountTurnAdmission {
        shutdown: Default::default(),
        permits: permits.clone(),
        work: AccountTurnWork::default(),
        auth_tasks: ticket,
    }
}
async fn held_auth(
    tasks: &Arc<ProcessorTasks>,
    nested: bool,
) -> (tempfile::TempDir, Arc<AuthManager>, Arc<HeldAuth>) {
    let home = tempfile::tempdir().unwrap();
    let auth = AuthManager::from_auth_for_testing_with_home(
        CodexAuth::create_dummy_chatgpt_auth_for_testing(),
        home.path().to_path_buf(),
    );
    let held = Arc::new(HeldAuth {
        tasks: Arc::downgrade(tasks),
        held: AtomicBool::new(false),
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
        finished: AtomicBool::new(false),
        nested,
    });
    auth.set_external_auth(held.clone()).await.unwrap();
    (home, auth, held)
}

#[test_case::test_case(false; "auth_only")]
#[test_case::test_case(true; "websocket")]
#[tokio::test]
async fn actual_core_prewarm_auth_child_outlives_constructor_and_parent_shutdown(
    websocket: bool,
) -> anyhow::Result<()> {
    let tasks = Arc::new(ProcessorTasks::default());
    let permits = AccountWorkPermits::new();
    let (_home, auth, held) = held_auth(&tasks, false).await;
    let mut extensions = ExtensionRegistryBuilder::new();
    extensions.turn_start_admission(Arc::new(host(&permits, Some(tasks.ticket()))));
    extensions.turn_lifecycle_contributor(Arc::new(
        crate::account_turn_admission::AccountTurnLifecycle,
    ));
    let server = core_test_support::responses::start_mock_server().await;
    let test = within_request(Some(permits.try_acquire().unwrap()), async {
        core_test_support::test_codex::test_codex()
            .with_extensions(Arc::new(extensions.build()))
            .with_auth_manager(auth)
            .with_config(move |config| config.model_provider.supports_websockets = websocket)
            .build_with_auto_env(&server)
            .await
    })
    .await?;
    assert_eq!(test.config.model_provider.supports_websockets, websocket);
    tokio::time::timeout(DRAIN_DEADLINE, held.entered.acquire())
        .await??
        .forget();
    permits.close();
    assert!(!held.finished.load(Ordering::SeqCst));
    let deadline = Instant::now() + DRAIN_DEADLINE;
    let mut observer = Box::pin(tasks.observe_until(deadline));
    assert!(futures::poll!(observer.as_mut()).is_pending());
    drop(observer);
    assert!(
        tokio::time::timeout(DRAIN_DEADLINE, test.codex.shutdown_and_wait_with_cleanup())
            .await?
            .is_complete()
    );
    assert!(
        permits.admitted_count() > 0,
        "auth child remains counted after constructor request and Core parent end"
    );
    assert!(
        !tasks.observe_until(Instant::now()).await.terminal,
        "timeout must retain the original join"
    );
    assert!(!held.finished.load(Ordering::SeqCst));
    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| !request.url.path().contains("responses")),
        "no provider turn or websocket request before auth release"
    );
    held.release.add_permits(1);
    assert!(tasks.observe_until(deadline).await.is_clean());
    assert!(held.finished.load(Ordering::SeqCst));
    assert_eq!(permits.admitted_count(), 0);
    Ok(())
}

#[tokio::test]
async fn actual_host_auth_descendant_derives_during_drain_after_request_cancellation() {
    let tasks = Arc::new(ProcessorTasks::default());
    let permits = AccountWorkPermits::new();
    let (_home, auth, held) = held_auth(&tasks, true).await;
    let admission = host(&permits, Some(tasks.ticket()));
    let operation = admission.admit_operation_work().unwrap().unwrap();
    let spawner = operation.auth_task_spawner().unwrap();
    drop(operation);
    let info = codex_model_provider_info::ModelProviderInfo::create_openai_provider(None);
    let provider = codex_model_provider::create_model_provider(info, Some(auth));
    let job = tokio::spawn(async move {
        codex_model_provider::with_auth_task_scope(
            codex_model_provider::AuthTaskAccess::Admitted(spawner),
            provider.request_auth(None),
        )
        .await
    });
    tokio::time::timeout(DRAIN_DEADLINE, held.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    permits.close();
    job.abort();
    assert!(matches!(job.await, Err(error) if error.is_cancelled()));
    assert!(permits.admitted_count() > 0);
    assert!(!tasks.observe_until(Instant::now()).await.terminal);
    held.release.add_permits(1);
    assert!(
        tasks
            .observe_until(Instant::now() + DRAIN_DEADLINE)
            .await
            .is_clean()
    );
    assert!(held.finished.load(Ordering::SeqCst));
    assert_eq!(permits.admitted_count(), 0);
}

#[tokio::test]
async fn unsupported_host_auth_capability_refuses_without_deriving_or_polling() {
    let permits = AccountWorkPermits::new();
    let operation = host(&permits, None)
        .admit_operation_work()
        .unwrap()
        .unwrap();
    assert!(operation.auth_task_spawner().is_err());
    assert_eq!(permits.admitted_count(), 1);
    drop(operation);
    assert_eq!(permits.admitted_count(), 0);
}

#[tokio::test]
async fn auth_capability_cannot_keep_its_task_owner_alive_or_spawn_after_retirement() {
    let permits = AccountWorkPermits::new();
    let tasks = ProcessorTasks::default();
    let operation = host(&permits, Some(tasks.ticket()))
        .admit_operation_work()
        .unwrap()
        .unwrap();
    let spawner = operation.auth_task_spawner().unwrap();
    drop(operation);
    drop(tasks);
    permits.close();
    let polled = Arc::new(AtomicBool::new(false));
    let child_polled = polled.clone();
    assert!(matches!(
        spawner.spawn(Box::pin(async move {
            child_polled.store(true, Ordering::SeqCst);
        })),
        Err(codex_model_provider::AuthTaskFailure::Unavailable)
    ));
    assert!(!polled.load(Ordering::SeqCst));
    assert_eq!(
        permits.admitted_count(),
        1,
        "only the finite factory root remains"
    );
    drop(spawner);
    assert_eq!(permits.admitted_count(), 0);
}
