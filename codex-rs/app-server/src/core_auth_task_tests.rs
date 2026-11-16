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
    credential: CodexAuth,
    held: AtomicBool,
    armed: AtomicBool,
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
            if owned && self.armed.load(Ordering::SeqCst) && !self.held.swap(true, Ordering::SeqCst)
            {
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
            Ok(self.credential.clone())
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
    let credential = CodexAuth::create_dummy_chatgpt_auth_for_testing();
    let auth =
        AuthManager::from_auth_for_testing_with_home(credential.clone(), home.path().to_path_buf());
    let held = Arc::new(HeldAuth {
        tasks: Arc::downgrade(tasks),
        credential,
        held: AtomicBool::new(false),
        armed: AtomicBool::new(true),
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

/// Wait for the constructor's actual finite work, not merely its publication reply.
async fn idle_constructor(permits: &AccountWorkPermits, tasks: &ProcessorTasks) {
    tokio::time::timeout(DRAIN_DEADLINE, async {
        loop {
            let released = permits.notified();
            if permits.admitted_count() == 0 {
                break;
            }
            released.await;
        }
    })
    .await
    .unwrap();
    assert!(
        tasks
            .observe_until(Instant::now() + DRAIN_DEADLINE)
            .await
            .is_clean()
    );
}
fn input() -> codex_core::TurnInputRequest {
    codex_core::TurnInputRequest::user_input(vec![codex_protocol::user_input::UserInput::Text {
        text: "synthetic inference custody".into(),
        text_elements: Vec::new(),
    }])
}

#[test_case::test_case(false, false; "http_complete")]
#[test_case::test_case(false, true; "http_cancel")]
#[test_case::test_case(true, true; "websocket_cancel_before_transport")]
#[tokio::test]
async fn actual_sampling_auth_has_finite_original_custody(
    websocket: bool,
    cancel: bool,
) -> anyhow::Result<()> {
    let tasks = Arc::new(ProcessorTasks::default());
    let permits = AccountWorkPermits::new();
    let (_home, auth, held) = held_auth(&tasks, false).await;
    held.armed.store(false, Ordering::SeqCst);
    let mut extensions = ExtensionRegistryBuilder::new();
    extensions.turn_start_admission(Arc::new(host(&permits, Some(tasks.ticket()))));
    extensions.turn_lifecycle_contributor(Arc::new(
        crate::account_turn_admission::AccountTurnLifecycle,
    ));
    let server = core_test_support::responses::start_mock_server().await;
    let response = core_test_support::responses::mount_sse_once(
        &server,
        core_test_support::responses::sse_completed("sampled"),
    )
    .await;
    let test = within_request(Some(permits.try_acquire().unwrap()), async {
        core_test_support::test_codex::test_codex()
            .with_auth_manager(auth)
            .with_extensions(Arc::new(extensions.build()))
            .with_config(move |config| config.model_provider.supports_websockets = websocket)
            .build_with_auto_env(&server)
            .await
    })
    .await?;
    idle_constructor(&permits, &tasks).await;
    held.armed.store(true, Ordering::SeqCst);
    assert!(matches!(
        within_request(
            permits.try_acquire(),
            test.codex.start_turn_if_idle(input())
        )
        .await?,
        codex_core::StartIfIdleSubmission::Started { .. }
    ));
    tokio::time::timeout(DRAIN_DEADLINE, held.entered.acquire())
        .await??
        .forget();
    permits.close();
    assert!(
        response.requests().is_empty(),
        "auth is held before actual inference transport"
    );
    if cancel {
        test.codex
            .submit(codex_protocol::protocol::Op::Interrupt)
            .await?;
        core_test_support::wait_for_event(&test.codex, |e| {
            matches!(e, codex_protocol::protocol::EventMsg::TurnAborted(_))
        })
        .await;
        assert!(
            test.codex
                .shutdown_and_wait_with_cleanup()
                .await
                .is_complete()
        );
        assert!(!tasks.observe_until(Instant::now()).await.terminal);
        assert!(
            permits.admitted_count() > 0,
            "auth survives originating turn and parent shutdown"
        );
        assert!(!held.finished.load(Ordering::SeqCst));
        held.release.add_permits(1);
    } else {
        held.release.add_permits(1);
        core_test_support::wait_for_successful_turn(&test.codex).await;
        assert_eq!(response.requests().len(), 1);
        assert!(
            test.codex
                .shutdown_and_wait_with_cleanup()
                .await
                .is_complete()
        );
    }
    assert!(
        tasks
            .observe_until(Instant::now() + DRAIN_DEADLINE)
            .await
            .is_clean()
    );
    assert!(held.finished.load(Ordering::SeqCst));
    assert_eq!(permits.admitted_count(), 0);
    Ok(())
}

#[test_case::test_case(false; "local")]
#[test_case::test_case(true; "remote_v2")]
#[tokio::test]
async fn actual_manual_compaction_auth_outlives_cancelled_parent(
    remote: bool,
) -> anyhow::Result<()> {
    let tasks = Arc::new(ProcessorTasks::default());
    let permits = AccountWorkPermits::new();
    let (_home, auth, held) = held_auth(&tasks, false).await;
    held.armed.store(false, Ordering::SeqCst);
    let mut extensions = ExtensionRegistryBuilder::new();
    extensions.turn_start_admission(Arc::new(host(&permits, Some(tasks.ticket()))));
    extensions.turn_lifecycle_contributor(Arc::new(
        crate::account_turn_admission::AccountTurnLifecycle,
    ));
    let server = core_test_support::responses::start_mock_server().await;
    let test = within_request(Some(permits.try_acquire().unwrap()), async {
        core_test_support::test_codex::test_codex()
            .with_auth_manager(auth)
            .with_extensions(Arc::new(extensions.build()))
            .with_config(move |config| {
                config
                    .features
                    .disable(codex_features::Feature::TokenBudget)
                    .unwrap();
                config.model_provider.capabilities =
                    Some(codex_model_provider_info::ModelProviderCapabilities {
                        remote_compaction: Some(if remote {
                            codex_model_provider_info::RemoteCompactionSupport::V2
                        } else {
                            codex_model_provider_info::RemoteCompactionSupport::Unsupported
                        }),
                        ..Default::default()
                    });
            })
            .build_with_auto_env(&server)
            .await
    })
    .await?;
    idle_constructor(&permits, &tasks).await;
    held.armed.store(true, Ordering::SeqCst);
    within_request(
        permits.try_acquire(),
        test.codex.submit(codex_protocol::protocol::Op::Compact),
    )
    .await?;
    tokio::time::timeout(DRAIN_DEADLINE, held.entered.acquire())
        .await??
        .forget();
    permits.close();
    test.codex
        .submit(codex_protocol::protocol::Op::Interrupt)
        .await?;
    core_test_support::wait_for_event(&test.codex, |e| {
        matches!(e, codex_protocol::protocol::EventMsg::TurnAborted(_))
    })
    .await;
    assert!(
        test.codex
            .shutdown_and_wait_with_cleanup()
            .await
            .is_complete()
    );
    assert!(permits.admitted_count() > 0);
    assert!(!tasks.observe_until(Instant::now()).await.terminal);
    assert!(!held.finished.load(Ordering::SeqCst));
    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|r| !r.url.path().contains("responses"))
    );
    held.release.add_permits(1);
    assert!(
        tasks
            .observe_until(Instant::now() + DRAIN_DEADLINE)
            .await
            .is_clean()
    );
    assert!(held.finished.load(Ordering::SeqCst));
    assert_eq!(permits.admitted_count(), 0);
    Ok(())
}

#[tokio::test]
async fn actual_background_compaction_captures_work_before_originating_turn_ends()
-> anyhow::Result<()> {
    let tasks = Arc::new(ProcessorTasks::default());
    let permits = AccountWorkPermits::new();
    let (_home, auth, held) = held_auth(&tasks, false).await;
    held.armed.store(false, Ordering::SeqCst);
    let mut extensions = ExtensionRegistryBuilder::new();
    extensions.turn_start_admission(Arc::new(host(&permits, Some(tasks.ticket()))));
    extensions.turn_lifecycle_contributor(Arc::new(
        crate::account_turn_admission::AccountTurnLifecycle,
    ));
    let server = core_test_support::responses::start_mock_server().await;
    let arm = held.clone();
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path("/v1/responses"))
        .respond_with(move |_: &wiremock::Request| {
            // Foreground auth has finished. Only the genuine post-turn background resolution is held.
            arm.armed.store(true, Ordering::SeqCst);
            core_test_support::responses::sse_response(core_test_support::responses::sse(vec![
                core_test_support::responses::ev_completed_with_tokens("launch-background", 60_000),
            ]))
        })
        .up_to_n_times(1)
        .mount(&server)
        .await;
    let test = within_request(Some(permits.try_acquire().unwrap()), async {
        core_test_support::test_codex::test_codex()
            .with_auth_manager(auth)
            .with_extensions(Arc::new(extensions.build()))
            .with_config(|config| {
                config.model = Some("gpt-5.2".into());
                config.model_context_window = Some(100_000);
                config.model_auto_compact_token_limit = Some(90_000);
                config.async_compaction_threshold_percent = 50;
                config
                    .features
                    .enable(codex_features::Feature::AsyncCompaction)
                    .unwrap();
                config
                    .features
                    .disable(codex_features::Feature::TokenBudget)
                    .unwrap();
            })
            .build_with_auto_env(&server)
            .await
    })
    .await?;
    idle_constructor(&permits, &tasks).await;
    assert!(matches!(
        within_request(
            permits.try_acquire(),
            test.codex.start_turn_if_idle(input())
        )
        .await?,
        codex_core::StartIfIdleSubmission::Started { .. }
    ));
    core_test_support::wait_for_successful_turn(&test.codex).await;
    tokio::time::timeout(DRAIN_DEADLINE, held.entered.acquire())
        .await??
        .forget();
    permits.close();
    assert!(
        permits.admitted_count() > 0,
        "post-turn background work has no live originating turn to rely on"
    );
    assert!(
        test.codex
            .shutdown_and_wait_with_cleanup()
            .await
            .is_complete()
    );
    assert!(!tasks.observe_until(Instant::now()).await.terminal);
    assert!(!held.finished.load(Ordering::SeqCst));
    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|r| r.url.path().contains("responses"))
            .count(),
        1,
        "only foreground reached transport; background is held in real auth"
    );
    held.release.add_permits(1);
    assert!(
        tasks
            .observe_until(Instant::now() + DRAIN_DEADLINE)
            .await
            .is_clean()
    );
    assert!(held.finished.load(Ordering::SeqCst));
    assert_eq!(permits.admitted_count(), 0);
    Ok(())
}

#[tokio::test]
async fn fixed_external_fixture_preserves_owner_and_generation_across_resolutions() {
    let tasks = Arc::new(ProcessorTasks::default());
    let (_home, auth, held) = held_auth(&tasks, false).await;
    let owner = auth.auth_owner();
    let changes = auth.auth_change_state_receiver();
    let before = *changes.borrow();
    for _ in 0..2 {
        assert!(auth.auth().await.is_some());
        assert!(
            owner.is_current(),
            "unchanged synthetic credentials must not impersonate an account switch"
        );
        assert_eq!(*changes.borrow(), before);
    }
    assert!(!held.held.load(Ordering::SeqCst));
    assert_eq!(tasks.len(), 0);
}

#[tokio::test]
async fn provider_parent_keeps_original_and_nested_auth_after_observer_drop() {
    let tasks = Arc::new(ProcessorTasks::default());
    let permits = AccountWorkPermits::new();
    let (_home, auth, held) = held_auth(&tasks, true).await;
    let operation = host(&permits, Some(tasks.ticket()))
        .admit_operation_work()
        .unwrap()
        .unwrap();
    let spawner = operation.auth_task_spawner().unwrap();
    drop(operation);
    let provider = codex_model_provider::create_model_provider(
        codex_model_provider_info::ModelProviderInfo::create_openai_provider(None),
        Some(auth),
    );
    let observation = codex_model_provider::with_auth_task_scope(
        codex_model_provider::AuthTaskAccess::Admitted(spawner),
        async move {
            // Ungated must preserve the inherited managed context, not detach.
            codex_model_provider::spawn_provider_task_with_access(
                codex_model_provider::AuthTaskAccess::Ungated,
                Box::pin(async move {
                    provider.request_auth(None).await.unwrap();
                }),
            )
            .unwrap()
            .expect("managed original observation")
        },
    )
    .await
    .unwrap();
    tokio::time::timeout(DRAIN_DEADLINE, held.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    permits.close();
    assert_eq!(tasks.len(), 2, "original parent and original auth child");
    assert_eq!(
        permits.admitted_count(),
        3,
        "finite factory, parent and auth child"
    );
    drop(observation);
    assert!(!tasks.observe_until(Instant::now()).await.terminal);
    assert!(!held.finished.load(Ordering::SeqCst));
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
async fn provider_parent_required_and_retired_owner_refuse_before_polling() {
    let polled = Arc::new(AtomicBool::new(false));
    let child_polled = polled.clone();
    assert!(matches!(
        codex_model_provider::spawn_provider_task_with_access(
            codex_model_provider::AuthTaskAccess::Required,
            Box::pin(async move {
                child_polled.store(true, Ordering::SeqCst);
            }),
        ),
        Err(codex_model_provider::AuthTaskFailure::Refused)
    ));
    assert!(!polled.load(Ordering::SeqCst));
    let tasks = ProcessorTasks::default();
    let permits = AccountWorkPermits::new();
    let operation = host(&permits, Some(tasks.ticket()))
        .admit_operation_work()
        .unwrap()
        .unwrap();
    let spawner = operation.auth_task_spawner().unwrap();
    drop(operation);
    drop(tasks);
    permits.close();
    let child_polled = polled.clone();
    assert!(matches!(
        codex_model_provider::spawn_provider_task_with_access(
            codex_model_provider::AuthTaskAccess::Admitted(spawner),
            Box::pin(async move {
                child_polled.store(true, Ordering::SeqCst);
            }),
        ),
        Err(codex_model_provider::AuthTaskFailure::Unavailable)
    ));
    assert!(!polled.load(Ordering::SeqCst));
    assert_eq!(permits.admitted_count(), 0);
}

#[tokio::test]
async fn provider_parent_closed_registration_is_sticky_without_polling() {
    let tasks = ProcessorTasks::default();
    let permits = AccountWorkPermits::new();
    let operation = host(&permits, Some(tasks.ticket()))
        .admit_operation_work()
        .unwrap()
        .unwrap();
    let spawner = operation.auth_task_spawner().unwrap();
    drop(operation);
    tasks.close_registration().unwrap();
    let polled = Arc::new(AtomicBool::new(false));
    let child_polled = polled.clone();
    let result = codex_model_provider::with_auth_task_scope(
        codex_model_provider::AuthTaskAccess::Admitted(spawner),
        async move {
            // Deliberately discard the dispatch error: the enclosing scope must refuse.
            let _ = codex_model_provider::spawn_provider_task(Box::pin(async move {
                child_polled.store(true, Ordering::SeqCst);
            }));
        },
    )
    .await;
    assert!(result.is_err());
    assert!(!polled.load(Ordering::SeqCst));
    assert!(tasks.observe_until(Instant::now()).await.is_clean());
    assert_eq!(permits.admitted_count(), 0);
}

#[tokio::test]
async fn provider_parent_panic_is_original_sticky_failure() {
    let tasks = ProcessorTasks::default();
    let permits = AccountWorkPermits::new();
    let operation = host(&permits, Some(tasks.ticket()))
        .admit_operation_work()
        .unwrap()
        .unwrap();
    let spawner = operation.auth_task_spawner().unwrap();
    drop(operation);
    let observation = codex_model_provider::spawn_provider_task_with_access(
        codex_model_provider::AuthTaskAccess::Admitted(spawner),
        Box::pin(async {
            panic!("synthetic provider parent panic");
        }),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        observation.await,
        Err(codex_model_provider::AuthTaskFailure::Panicked)
    );
    let receipt = tasks.observe_until(Instant::now() + DRAIN_DEADLINE).await;
    assert!(receipt.terminal && receipt.panicked && !receipt.is_clean());
    assert!(tasks.observe_until(Instant::now()).await.panicked);
    assert_eq!(permits.admitted_count(), 0);
}

#[tokio::test]
async fn unmanaged_provider_parent_preserves_detached_execution() {
    let (send, receive) = tokio::sync::oneshot::channel();
    assert!(
        codex_model_provider::spawn_provider_task_with_access(
            codex_model_provider::AuthTaskAccess::Ungated,
            Box::pin(async move {
                send.send(()).unwrap();
            }),
        )
        .unwrap()
        .is_none()
    );
    tokio::time::timeout(DRAIN_DEADLINE, receive)
        .await
        .unwrap()
        .unwrap();
}
