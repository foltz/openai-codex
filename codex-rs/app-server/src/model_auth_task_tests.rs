use super::*;
use crate::account_work_permits::AccountWorkPermits;
use crate::processor_task_retirement::ProcessorTasks;
use codex_extension_api::{
    ExtensionRegistryBuilder, HostOperationWork, TurnStartAdmission, TurnWorkRefused,
};
use codex_login::{AuthManager, CodexAuth, ExternalAuth};
use codex_models_manager::manager::RefreshStrategy;
use pretty_assertions::assert_eq;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::Instant;

#[derive(Debug)]
struct HeldAuth {
    calls: AtomicUsize,
    hold_at: AtomicUsize,
    entered: Semaphore,
    release: Semaphore,
    finished: AtomicBool,
    nested: bool,
    panic: bool,
}
impl ExternalAuth for HeldAuth {
    fn refresh(
        &self,
        _: codex_login::ExternalAuthRefreshContext,
    ) -> codex_login::ExternalAuthFuture<'_, CodexAuth> {
        Box::pin(async { panic!("fixture must not perform external credential refresh") })
    }
    fn resolve(&self) -> codex_login::ExternalAuthFuture<'_, CodexAuth> {
        Box::pin(async move {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == self.hold_at.load(Ordering::SeqCst) {
                self.entered.add_permits(1);
                self.release.acquire().await.unwrap().forget();
                assert!(!self.panic, "synthetic auth child panic");
                if self.nested {
                    let mut info =
                        codex_model_provider_info::ModelProviderInfo::create_openai_provider(None);
                    info.experimental_bearer_token = Some("synthetic-nested-token".into());
                    codex_model_provider::create_model_provider(info, None)
                        .request_auth(None)
                        .await
                        .expect("finite auth descendant derives while admission closed");
                }
                self.finished.store(true, Ordering::SeqCst);
            }
            Ok(CodexAuth::create_dummy_chatgpt_auth_for_testing())
        })
    }
}
struct Fixture {
    _home: tempfile::TempDir,
    server: wiremock::MockServer,
    catalog: Arc<ModelCatalog>,
    config: Arc<Config>,
    models: SharedModelsManager,
    provider: codex_model_provider::SharedModelProvider,
    held: Arc<HeldAuth>,
    permits: AccountWorkPermits,
}
#[derive(Debug)]
struct MissingWork;
impl TurnStartAdmission for MissingWork {
    fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
        Some(Box::new(()))
    }
    fn requires_account_work(&self) -> bool {
        true
    }
}
async fn fixture(nested: bool, panic: bool, missing_work: bool) -> Fixture {
    let home = tempfile::tempdir().unwrap();
    let server = wiremock::MockServer::start().await;
    let config_manager = ConfigManager::without_managed_config_for_tests(home.path().to_path_buf());
    let mut config = config_manager.load_non_project_config().await.unwrap();
    config.model_provider.base_url = Some(server.uri());
    let config = Arc::new(config);
    let auth = AuthManager::from_auth_for_testing_with_home(
        CodexAuth::create_dummy_chatgpt_auth_for_testing(),
        home.path().to_path_buf(),
    );
    let held = Arc::new(HeldAuth {
        calls: AtomicUsize::new(0),
        hold_at: AtomicUsize::new(1),
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
        finished: AtomicBool::new(false),
        nested,
        panic,
    });
    auth.set_external_auth(held.clone()).await.unwrap();
    assert_eq!(held.calls.load(Ordering::SeqCst), 1);
    let provider = codex_model_provider::create_model_provider(
        config.model_provider.clone(),
        Some(auth.clone()),
    );
    let models = provider.models_manager_without_cache(None);
    let startup = codex_core::build_startup_models(
        &config,
        auth,
        uuid::Uuid::new_v4().to_string(),
        codex_core::test_support::fixed_model_catalog(models.clone()),
    );
    let permits = AccountWorkPermits::new();
    let mut extensions = ExtensionRegistryBuilder::new();
    let host: Arc<dyn TurnStartAdmission> = if missing_work {
        Arc::new(MissingWork)
    } else {
        Arc::new(crate::account_turn_admission::AccountTurnAdmission {
            shutdown: crate::turn_admission::TurnAdmission::default(),
            permits: permits.clone(),
            work: crate::account_turn_work::AccountTurnWork::default(),
            auth_tasks: None,
        })
    };
    extensions.turn_start_admission(host);
    let manager = Arc::new(ThreadManager::new(
        &config,
        startup,
        codex_core::CodexAppsToolsCache::default(),
        codex_protocol::protocol::SessionSource::Exec,
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        Arc::new(extensions.build()),
        Arc::new(codex_core::test_support::EmptyUserInstructionsProvider),
        None,
        codex_core::passthrough_image_store(),
        codex_core::ThreadStoreBuilder::from_config(&config, None).build(),
        None,
        None,
        None,
    ));
    let catalog = Arc::new(ModelCatalog::new(
        config_manager,
        None,
        config.clone(),
        manager,
    ));
    Fixture {
        _home: home,
        server,
        catalog,
        config,
        models,
        provider,
        held,
        permits,
    }
}
async fn entered(f: &Fixture) {
    tokio::time::timeout(Duration::from_secs(1), f.held.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}
async fn prove_cancelled_catalog_child(nested: bool, request: bool) {
    let f = fixture(nested, false, false).await;
    let catalog = f.catalog.clone();
    let config = f.config.clone();
    let models = f.models.clone();
    let job = if request {
        let request = f.permits.try_acquire().unwrap();
        tokio::spawn(crate::account_turn_admission::within_request(
            Some(request),
            async move {
                catalog
                    .list_models_from_snapshot(config, models, RefreshStrategy::Online)
                    .await
            },
        ))
    } else {
        tokio::spawn(async move { catalog.list_models(RefreshStrategy::Online).await })
    };
    entered(&f).await;
    f.permits.close();
    job.abort();
    assert!(matches!(job.await, Err(error) if error.is_cancelled()));
    assert!(
        f.permits.admitted_count() > 0,
        "the real auth child retains finite work after parent end"
    );
    assert!(!f.held.finished.load(Ordering::SeqCst));
    let deadline = Instant::now() + Duration::from_secs(1);
    let mut observer = Box::pin(f.catalog.observe_auth_tasks_until(deadline));
    assert!(futures::poll!(observer.as_mut()).is_pending());
    drop(observer);
    assert!(
        !f.catalog
            .observe_auth_tasks_until(Instant::now() + Duration::from_millis(10))
            .await
            .terminal
    );
    assert!(f.permits.admitted_count() > 0);
    f.held.release.add_permits(1);
    let report = f.catalog.observe_auth_tasks_until(deadline).await;
    assert!(report.is_clean(), "{report:?}");
    assert!(f.held.finished.load(Ordering::SeqCst));
    assert_eq!(f.permits.admitted_count(), 0);
    assert!(
        f.catalog
            .observe_auth_tasks_until(Instant::now())
            .await
            .is_clean()
    );
    assert!(
        f.server.received_requests().await.unwrap().is_empty(),
        "cancelled catalog did not proceed to remote fetch"
    );
    // Reusable observation must not permanently close this task owner.
    let receipt = f.catalog.auth_tasks.spawn(async {}).unwrap();
    assert_eq!(
        receipt.await,
        crate::processor_task_retirement::ProcessorTaskJoin::Joined
    );
    assert!(
        f.catalog
            .observe_auth_tasks_until(Instant::now())
            .await
            .is_clean()
    );
}
#[tokio::test]
async fn catalog_auth_child_survives_cancelled_refresh_and_observers() {
    prove_cancelled_catalog_child(false, false).await;
}
#[tokio::test]
async fn catalog_nested_auth_child_derives_after_admission_closes() {
    prove_cancelled_catalog_child(true, false).await;
}
#[tokio::test]
async fn request_catalog_auth_child_outlives_parent_request_during_drain() {
    prove_cancelled_catalog_child(false, true).await;
}

#[tokio::test]
async fn request_setup_auth_child_survives_cancelled_caller() {
    let f = fixture(false, false, false).await;
    let tasks = ProcessorTasks::default();
    let work = Box::new(crate::account_operation_work::AccountOperationWork {
        permit: f.permits.try_acquire().unwrap(),
        turns: Default::default(),
        auth_tasks: None,
    }) as Box<dyn HostOperationWork>;
    let provider = f.provider.clone();
    let owner = tasks.clone();
    let job = tokio::spawn(async move {
        crate::auth_task_work::scope(&owner, Some(work), async move {
            provider.request_auth(None).await
        })
        .await
    });
    entered(&f).await;
    f.permits.close();
    job.abort();
    assert!(matches!(job.await, Err(error) if error.is_cancelled()));
    assert!(f.permits.admitted_count() > 0);
    f.held.release.add_permits(1);
    assert!(
        tasks
            .observe_until(Instant::now() + Duration::from_secs(1))
            .await
            .is_clean()
    );
    assert!(f.held.finished.load(Ordering::SeqCst));
    assert_eq!(f.permits.admitted_count(), 0);
    assert!(f.server.received_requests().await.unwrap().is_empty());
}
#[tokio::test]
async fn closed_background_admission_refuses_before_auth_effects() {
    let f = fixture(false, false, false).await;
    f.permits.close();
    assert!(
        f.catalog
            .list_models(RefreshStrategy::Online)
            .await
            .is_err()
    );
    assert_eq!(f.held.calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.permits.admitted_count(), 0);
    assert_eq!(f.catalog.auth_tasks.len(), 0);
}
#[tokio::test]
async fn missing_required_background_work_refuses_before_auth_effects() {
    let f = fixture(false, false, true).await;
    assert!(
        f.catalog
            .list_models(RefreshStrategy::Online)
            .await
            .is_err()
    );
    assert_eq!(f.held.calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.catalog.auth_tasks.len(), 0);
}
#[tokio::test]
async fn missing_required_request_scope_refuses_before_auth_effects() {
    let f = fixture(false, false, false).await;
    assert!(
        f.catalog
            .list_models_from_snapshot(f.config.clone(), f.models.clone(), RefreshStrategy::Online)
            .await
            .is_err()
    );
    assert_eq!(f.held.calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.catalog.auth_tasks.len(), 0);
}
#[tokio::test]
async fn auth_panic_cannot_be_hidden_by_catalog_bool_fallback() {
    let f = fixture(false, true, false).await;
    let catalog = f.catalog.clone();
    let job = tokio::spawn(async move { catalog.list_models(RefreshStrategy::Online).await });
    entered(&f).await;
    f.held.release.add_permits(1);
    assert!(job.await.unwrap().is_err());
    let report = f.catalog.observe_auth_tasks_until(Instant::now()).await;
    assert!(report.terminal && report.panicked && !report.is_clean());
    assert_eq!(
        f.catalog.observe_auth_tasks_until(Instant::now()).await,
        report
    );
    assert_eq!(f.permits.admitted_count(), 0);
}
#[tokio::test]
async fn closed_join_registration_cannot_be_hidden_as_logged_out_catalog() {
    let f = fixture(false, false, false).await;
    f.catalog.auth_tasks.close_registration().unwrap();
    assert!(
        f.catalog
            .list_models(RefreshStrategy::Online)
            .await
            .is_err()
    );
    assert_eq!(f.held.calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.permits.admitted_count(), 0);
}
#[tokio::test]
async fn explicit_required_scope_never_polls_an_unrooted_operation() {
    let entered = AtomicBool::new(false);
    let result = codex_model_provider::with_auth_task_scope(
        codex_model_provider::AuthTaskAccess::Required,
        async { entered.store(true, Ordering::SeqCst) },
    )
    .await;
    assert!(result.is_err());
    assert!(!entered.load(Ordering::SeqCst));
}
struct RefusedWork {
    _work: Box<dyn HostOperationWork>,
}
impl std::fmt::Debug for RefusedWork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RefusedWork")
    }
}
impl HostOperationWork for RefusedWork {
    fn derive_operation(&self) -> Result<Box<dyn HostOperationWork>, TurnWorkRefused> {
        Err(TurnWorkRefused::Unavailable)
    }
    fn derive_turn_work(
        &self,
        _: &codex_extension_api::ExtensionData,
        _: codex_extension_api::ExtensionFuture<'static, ()>,
    ) -> Result<Box<dyn codex_protocol::host_turn_work::HostTurnWork>, TurnWorkRefused> {
        panic!("not a turn")
    }
}
#[tokio::test]
async fn refused_child_derivation_has_no_fresh_fallback_or_auth_effect() {
    let f = fixture(false, false, false).await;
    let tasks = ProcessorTasks::default();
    let work = Box::new(RefusedWork {
        _work: Box::new(crate::account_operation_work::AccountOperationWork {
            permit: f.permits.try_acquire().unwrap(),
            turns: Default::default(),
            auth_tasks: None,
        }),
    }) as Box<dyn HostOperationWork>;
    assert!(
        crate::auth_task_work::scope(&tasks, Some(work), f.provider.request_auth(None))
            .await
            .is_err()
    );
    assert_eq!(f.held.calls.load(Ordering::SeqCst), 1);
    assert_eq!(tasks.len(), 0);
    assert_eq!(f.permits.admitted_count(), 0);
}

#[tokio::test]
async fn actual_endpoint_timeout_keeps_original_auth_child_counted_until_join() {
    let f = fixture(false, false, false).await;
    // Let the backend-eligibility auth finish. Hold request setup under the real
    // unchanged five-second endpoint timeout, before any transport can send.
    f.held.hold_at.store(2, Ordering::SeqCst);
    let catalog = f.catalog.clone();
    let job = tokio::spawn(async move { catalog.list_models(RefreshStrategy::Online).await });
    entered(&f).await;
    job.await
        .unwrap()
        .expect("ordinary discovery retains its upstream fallback policy");
    assert!(f.permits.admitted_count() > 0);
    assert!(!f.held.finished.load(Ordering::SeqCst));
    assert!(
        !f.catalog
            .observe_auth_tasks_until(Instant::now() + Duration::from_millis(10))
            .await
            .terminal
    );
    f.held.release.add_permits(1);
    assert!(
        f.catalog
            .observe_auth_tasks_until(Instant::now() + Duration::from_secs(1))
            .await
            .is_clean()
    );
    assert_eq!(f.permits.admitted_count(), 0);
    assert!(f.held.finished.load(Ordering::SeqCst));
    assert!(f.server.received_requests().await.unwrap().is_empty());
}
