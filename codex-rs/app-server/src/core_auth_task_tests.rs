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
    skip_owned: std::sync::atomic::AtomicUsize,
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
            // Callback installation itself precedes Core and has no task owner.
            // Hold only a real child already registered in this strict owner.
            let owned = self.tasks.upgrade().is_some_and(|tasks| tasks.len() > 0);
            let armed = owned && self.armed.load(Ordering::SeqCst);
            let skip = armed
                && self
                    .skip_owned
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                        remaining.checked_sub(1)
                    })
                    .is_ok();
            if armed && !skip && !self.held.swap(true, Ordering::SeqCst) {
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
        skip_owned: std::sync::atomic::AtomicUsize::new(0),
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
    // Shared persistence/MCP auth and latest-auth are separate tests below.
    // This fixture still holds the actual subsequent prewarm resolution.
    held.skip_owned.store(2, Ordering::SeqCst);
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
    assert_eq!(
        tasks.len(),
        2,
        "original background parent and held auth child"
    );
    assert!(
        permits.admitted_count() >= 3,
        "shared finite factory, parent and auth child"
    );
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

#[tokio::test]
async fn provider_parent_observer_preserves_caught_nested_registration_failure() {
    let tasks = ProcessorTasks::default();
    let permits = AccountWorkPermits::new();
    let operation = host(&permits, Some(tasks.ticket()))
        .admit_operation_work()
        .unwrap()
        .unwrap();
    let spawner = operation.auth_task_spawner().unwrap();
    drop(operation);
    let (started_send, started_receive) = tokio::sync::oneshot::channel();
    let (continue_send, continue_receive) = tokio::sync::oneshot::channel();
    let polled = Arc::new(AtomicBool::new(false));
    let child_polled = polled.clone();
    let observation = codex_model_provider::spawn_provider_task_with_access(
        codex_model_provider::AuthTaskAccess::Admitted(spawner),
        Box::pin(async move {
            started_send.send(()).unwrap();
            continue_receive.await.unwrap();
            // Deliberately catch it: the original parent's observer must retain it.
            let _ = codex_model_provider::spawn_provider_task(Box::pin(async move {
                child_polled.store(true, Ordering::SeqCst);
            }));
        }),
    )
    .unwrap()
    .unwrap();
    tokio::time::timeout(DRAIN_DEADLINE, started_receive)
        .await
        .unwrap()
        .unwrap();
    tasks.close_registration().unwrap();
    permits.close();
    continue_send.send(()).unwrap();
    assert_eq!(
        tokio::time::timeout(DRAIN_DEADLINE, observation)
            .await
            .unwrap(),
        Err(codex_model_provider::AuthTaskFailure::Unavailable),
    );
    assert!(!polled.load(Ordering::SeqCst));
    // The parent joined normally; that is distinct from the operation's refusal.
    assert!(
        tasks
            .observe_until(Instant::now() + DRAIN_DEADLINE)
            .await
            .is_clean()
    );
    assert_eq!(permits.admitted_count(), 0);
}

struct PumpAuthContributor {
    auth: Arc<AuthManager>,
    held: Arc<HeldAuth>,
    permits: AccountWorkPermits,
}
impl std::fmt::Debug for PumpAuthContributor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PumpAuthContributor")
    }
}
struct PumpAuthInterceptor {
    auth: Arc<AuthManager>,
    held: Arc<HeldAuth>,
    permits: AccountWorkPermits,
}
impl codex_extension_api::ModelRequestContributor for PumpAuthContributor {
    fn request(
        &self,
        input: codex_extension_api::ModelRequestInput<'_>,
    ) -> Option<Box<dyn codex_extension_api::ModelResponseInterceptor>> {
        if input.kind != codex_extension_api::ModelRequestKind::Generation {
            return None;
        }
        Some(Box::new(PumpAuthInterceptor {
            auth: self.auth.clone(),
            held: self.held.clone(),
            permits: self.permits.clone(),
        }))
    }
}
impl codex_extension_api::ModelResponseInterceptor for PumpAuthInterceptor {
    fn intercept(
        self: Box<Self>,
        stream: codex_extension_api::ModelResponseStream,
    ) -> codex_extension_api::ModelResponseStream {
        use futures::StreamExt;
        let seen = Arc::new(AtomicBool::new(false));
        Box::pin(stream.then(move |event| {
            let seen = seen.clone();
            let auth = self.auth.clone();
            let held = self.held.clone();
            let permits = self.permits.clone();
            async move {
                if !seen.swap(true, Ordering::SeqCst) {
                    permits.close();
                    held.armed.store(true, Ordering::SeqCst);
                    codex_model_provider::create_model_provider(
                        codex_model_provider_info::ModelProviderInfo::create_openai_provider(None),
                        Some(auth),
                    )
                    .request_auth(None)
                    .await
                    .expect("real pump nested auth derives during drain");
                }
                event
            }
        }))
    }
}

#[test_case::test_case(false; "normal_completion")]
#[test_case::test_case(true; "cancelled_consumer")]
#[tokio::test]
async fn actual_stream_pump_owns_nested_auth_and_original_join(cancel: bool) -> anyhow::Result<()> {
    let tasks = Arc::new(ProcessorTasks::default());
    let permits = AccountWorkPermits::new();
    let (_home, auth, held) = held_auth(&tasks, true).await;
    held.armed.store(false, Ordering::SeqCst);
    let mut extensions = ExtensionRegistryBuilder::new();
    extensions.turn_start_admission(Arc::new(host(&permits, Some(tasks.ticket()))));
    extensions.turn_lifecycle_contributor(Arc::new(
        crate::account_turn_admission::AccountTurnLifecycle,
    ));
    extensions.model_request_contributor(Arc::new(PumpAuthContributor {
        auth: auth.clone(),
        held: held.clone(),
        permits: permits.clone(),
    }));
    let server = core_test_support::responses::start_mock_server().await;
    let response = core_test_support::responses::mount_sse_once(
        &server,
        core_test_support::responses::sse_completed("pump-completed"),
    )
    .await;
    let test = within_request(Some(permits.try_acquire().unwrap()), async {
        core_test_support::test_codex::test_codex()
            .with_auth_manager(auth)
            .with_extensions(Arc::new(extensions.build()))
            .with_config(|config| config.model_provider.supports_websockets = false)
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
    tokio::time::timeout(DRAIN_DEADLINE, held.entered.acquire())
        .await??
        .forget();
    assert_eq!(
        response.requests().len(),
        1,
        "real event interceptor after HTTP transport"
    );
    assert_eq!(tasks.len(), 2, "original stream pump and nested auth child");
    assert!(
        permits.admitted_count() >= 4,
        "turn, finite factory, pump and auth child remain counted"
    );
    assert!(!tasks.observe_until(Instant::now()).await.terminal);
    if cancel {
        test.codex
            .submit(codex_protocol::protocol::Op::Interrupt)
            .await?;
        core_test_support::wait_for_event(&test.codex, |event| {
            matches!(event, codex_protocol::protocol::EventMsg::TurnAborted(_))
        })
        .await;
        assert!(
            test.codex
                .shutdown_and_wait_with_cleanup()
                .await
                .is_complete()
        );
        assert!(!held.finished.load(Ordering::SeqCst));
        assert!(!tasks.observe_until(Instant::now()).await.terminal);
        assert!(
            permits.admitted_count() > 0,
            "credential child outlives cancelled consumer and Core parent"
        );
        held.release.add_permits(1);
    } else {
        held.release.add_permits(1);
        core_test_support::wait_for_successful_turn(&test.codex).await;
        assert!(
            tasks.observe_until(Instant::now()).await.is_clean(),
            "original pump joined before successful turn"
        );
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

#[derive(Debug)]
struct PanicInBackgroundRequest {
    generations: std::sync::atomic::AtomicUsize,
    entered: Semaphore,
}
impl codex_extension_api::ModelRequestContributor for PanicInBackgroundRequest {
    fn request(
        &self,
        input: codex_extension_api::ModelRequestInput<'_>,
    ) -> Option<Box<dyn codex_extension_api::ModelResponseInterceptor>> {
        if input.kind == codex_extension_api::ModelRequestKind::Generation
            && self.generations.fetch_add(1, Ordering::SeqCst) == 1
        {
            self.entered.add_permits(1);
            panic!("synthetic actual background parent panic before transport");
        }
        None
    }
}

#[tokio::test]
async fn actual_background_parent_panic_is_not_clean_tracker_completion() -> anyhow::Result<()> {
    let tasks = Arc::new(ProcessorTasks::default());
    let permits = AccountWorkPermits::new();
    let (_home, auth, held) = held_auth(&tasks, false).await;
    held.armed.store(false, Ordering::SeqCst);
    let panic = Arc::new(PanicInBackgroundRequest {
        generations: std::sync::atomic::AtomicUsize::new(0),
        entered: Semaphore::new(0),
    });
    let mut extensions = ExtensionRegistryBuilder::new();
    extensions.turn_start_admission(Arc::new(host(&permits, Some(tasks.ticket()))));
    extensions.turn_lifecycle_contributor(Arc::new(
        crate::account_turn_admission::AccountTurnLifecycle,
    ));
    extensions.model_request_contributor(panic.clone());
    let server = core_test_support::responses::start_mock_server().await;
    let response = core_test_support::responses::mount_sse_once(
        &server,
        core_test_support::responses::sse(vec![
            core_test_support::responses::ev_completed_with_tokens(
                "launch-panicking-background",
                60_000,
            ),
        ]),
    )
    .await;
    let test = within_request(Some(permits.try_acquire().unwrap()), async {
        core_test_support::test_codex::test_codex()
            .with_auth_manager(auth)
            .with_extensions(Arc::new(extensions.build()))
            .with_config(|config| {
                config.model = Some("gpt-5.2".into());
                config.model_provider.supports_websockets = false;
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
    tokio::time::timeout(DRAIN_DEADLINE, panic.entered.acquire())
        .await??
        .forget();
    permits.close();
    tokio::time::timeout(
        DRAIN_DEADLINE,
        test.codex.wait_for_background_compaction_for_test(),
    )
    .await?;
    let original = tasks.observe_until(Instant::now() + DRAIN_DEADLINE).await;
    assert!(
        original.terminal && original.panicked && !original.is_clean(),
        "TaskTracker drained, but the actual original parent panicked"
    );
    assert!(
        tasks.observe_until(Instant::now()).await.panicked,
        "panic survives compaction and repeated observation"
    );
    assert_eq!(panic.generations.load(Ordering::SeqCst), 2);
    assert_eq!(
        response.requests().len(),
        1,
        "only foreground reached HTTP transport"
    );
    assert_eq!(
        permits.admitted_count(),
        0,
        "no factory or parent account-work cycle"
    );
    assert!(
        test.codex
            .shutdown_and_wait_with_cleanup()
            .await
            .is_complete()
    );
    assert!(
        !tasks.observe_until(Instant::now()).await.is_clean(),
        "Core parent cleanup cannot erase host-owned failure"
    );
    Ok(())
}

#[tokio::test]
async fn direct_auth_call_preserves_inline_unmanaged_and_required_refusal() {
    tokio::task_local! { static INLINE_CALLER: u8; }
    let observed = INLINE_CALLER
        .scope(
            17,
            codex_model_provider::run_auth_call(
                codex_model_provider::AuthTaskAccess::Ungated,
                async { INLINE_CALLER.with(|value| *value) },
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        observed, 17,
        "unmanaged direct auth must not introduce a Tokio hop"
    );
    let polled = Arc::new(AtomicBool::new(false));
    let child_polled = polled.clone();
    assert_eq!(
        codex_model_provider::run_auth_call(
            codex_model_provider::AuthTaskAccess::Required,
            async move {
                child_polled.store(true, Ordering::SeqCst);
            },
        )
        .await,
        Err(codex_model_provider::AuthTaskFailure::Refused)
    );
    assert!(!polled.load(Ordering::SeqCst));
}

#[test_case::test_case(0; "shared_persistence_mcp")]
#[test_case::test_case(1; "latest_auth_after_configured")]
#[tokio::test]
async fn actual_constructor_auth_original_survives_cancelled_start_caller(
    skip: usize,
) -> anyhow::Result<()> {
    let tasks = Arc::new(ProcessorTasks::default());
    let permits = AccountWorkPermits::new();
    let (_home, auth, held) = held_auth(&tasks, true).await;
    held.armed.store(false, Ordering::SeqCst);
    let mut extensions = ExtensionRegistryBuilder::new();
    extensions.turn_start_admission(Arc::new(host(&permits, Some(tasks.ticket()))));
    extensions.turn_lifecycle_contributor(Arc::new(
        crate::account_turn_admission::AccountTurnLifecycle,
    ));
    let server = core_test_support::responses::start_mock_server().await;
    // Keep genuine manager, home and environment ownership outside the cancelled caller.
    let seed = within_request(Some(permits.try_acquire().unwrap()), async {
        core_test_support::test_codex::test_codex()
            .with_auth_manager(auth)
            .with_extensions(Arc::new(extensions.build()))
            .with_config(|config| config.model_provider.supports_websockets = false)
            .build_with_auto_env(&server)
            .await
    })
    .await?;
    idle_constructor(&permits, &tasks).await;
    let options = seed.start_thread_options().await;
    held.skip_owned.store(skip, Ordering::SeqCst);
    held.armed.store(true, Ordering::SeqCst);
    let manager = seed.thread_manager.clone();
    let root = permits.try_acquire().unwrap();
    let caller = tokio::spawn(within_request(Some(root), async move {
        manager.start_thread(options).await
    }));
    tokio::time::timeout(DRAIN_DEADLINE, held.entered.acquire())
        .await??
        .forget();
    assert_eq!(held.skip_owned.load(Ordering::SeqCst), 0);
    assert_eq!(
        tasks.len(),
        1,
        "actual constructor auth original, not fixture callback installation"
    );
    permits.close();
    caller.abort();
    assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
    assert!(!tasks.observe_until(Instant::now()).await.terminal);
    assert!(
        permits.admitted_count() > 0,
        "credential work outlives original start caller"
    );
    assert!(!held.finished.load(Ordering::SeqCst));
    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| !request.url.path().contains("responses"))
    );
    held.release.add_permits(1);
    assert!(
        tasks
            .observe_until(Instant::now() + DRAIN_DEADLINE)
            .await
            .is_clean()
    );
    assert!(held.finished.load(Ordering::SeqCst));
    let retirement = seed
        .thread_manager
        .begin_shutdown(Instant::now() + DRAIN_DEADLINE)
        .unwrap();
    let report = retirement.wait().await.unwrap();
    assert!(
        report.is_complete(),
        "seed and original cancelled constructor cleanup: {report:?}"
    );
    idle_constructor(&permits, &tasks).await;
    assert_eq!(permits.admitted_count(), 0);
    Ok(())
}

/// Owned loopback IO only. Unknown routes cannot impersonate a recommendation
/// response, and every socket task is joined before the fixture is retired.
struct HeldRecommendation {
    base: String,
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
    requests: Arc<std::sync::atomic::AtomicUsize>,
    stop: tokio_util::sync::CancellationToken,
    task: Option<tokio::task::JoinHandle<()>>,
}
impl HeldRecommendation {
    async fn start() -> anyhow::Result<Self> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let base = format!("http://{}", listener.local_addr()?);
        let entered = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let stop = tokio_util::sync::CancellationToken::new();
        let (child_entered, child_release, child_requests, child_stop) = (
            entered.clone(),
            release.clone(),
            requests.clone(),
            stop.clone(),
        );
        let task = tokio::spawn(async move {
            let mut sockets = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    biased;
                    _ = child_stop.cancelled() => break,
                    done = sockets.join_next(), if !sockets.is_empty() => {
                        done.unwrap().expect("recommendation fixture socket panicked");
                    }
                    accepted = listener.accept() => {
                        let (mut socket, _) = accepted.unwrap();
                        let (entered, release, requests, stop) =
                            (child_entered.clone(), child_release.clone(), child_requests.clone(), child_stop.clone());
                        sockets.spawn(async move {
                            let serve = async {
                                let mut header = Vec::new();
                                loop {
                                    let byte = match socket.read_u8().await {
                                        Ok(byte) => byte,
                                        Err(error) if header.is_empty() && error.kind() == std::io::ErrorKind::UnexpectedEof => return,
                                        Err(error) => panic!("recommendation fixture truncated request header: {error}"),
                                    };
                                    header.push(byte);
                                    assert!(header.len() <= 1024 * 1024, "fixture request headers oversized");
                                    if header.ends_with(b"\r\n\r\n") { break; }
                                }
                                let header = String::from_utf8(header).unwrap();
                                let is_recommendation = header.lines().next().unwrap()
                                    == "GET /ps/plugins/suggested/codex?scope=GLOBAL HTTP/1.1";
                                let (status, body) = if is_recommendation {
                                    let lower = header.to_ascii_lowercase();
                                    assert!(lower.contains("authorization: bearer access token\r\n"), "missing synthetic auth");
                                    assert!(lower.contains("chatgpt-account-id: account_id\r\n"), "missing synthetic account");
                                    requests.fetch_add(1, Ordering::SeqCst);
                                    entered.add_permits(1);
                                    release.acquire().await.unwrap().forget();
                                    ("200 OK", r#"{"enabled":true,"plugins":[]}"#)
                                } else {
                                    ("404 Not Found", "{}")
                                };
                                let reply = format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                                socket.write_all(reply.as_bytes()).await.unwrap();
                                socket.shutdown().await.unwrap();
                            };
                            tokio::select! {
                                biased;
                                _ = stop.cancelled() => {},
                                result = tokio::time::timeout(DRAIN_DEADLINE, serve) => result.expect("fixture IO timeout"),
                            }
                        });
                    }
                }
            }
            while let Some(result) = sockets.join_next().await {
                result.expect("recommendation fixture socket panicked during retirement");
            }
        });
        Ok(Self {
            base,
            entered,
            release,
            requests,
            stop,
            task: Some(task),
        })
    }
    async fn finish(mut self) {
        self.stop.cancel();
        tokio::time::timeout(DRAIN_DEADLINE, self.task.take().unwrap())
            .await
            .unwrap()
            .unwrap();
    }
}
impl Drop for HeldRecommendation {
    fn drop(&mut self) {
        // Only fixture sockets are interrupted on a failed assertion. This is
        // never an abort handle for the production recommendation/auth original.
        self.stop.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[test_case::test_case(false; "published_constructor")]
#[test_case::test_case(true; "cancelled_unpublished_constructor")]
#[tokio::test]
async fn actual_recommendation_original_survives_constructor_retirement(
    cancel_unpublished: bool,
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
    let model_server = core_test_support::responses::start_mock_server().await;
    let seed = within_request(Some(permits.try_acquire().unwrap()), async {
        core_test_support::test_codex::test_codex()
            .with_auth_manager(auth)
            .with_extensions(Arc::new(extensions.build()))
            .with_config(|config| config.model_provider.supports_websockets = false)
            .build_with_auto_env(&model_server)
            .await
    })
    .await?;
    idle_constructor(&permits, &tasks).await;
    let recommendation = HeldRecommendation::start().await?;
    let mut options = seed.start_thread_options().await;
    options.config.chatgpt_base_url = recommendation.base.clone();
    for feature in [
        codex_features::Feature::Apps,
        codex_features::Feature::Plugins,
        codex_features::Feature::RemotePlugin,
        codex_features::Feature::RecommendedPlugins,
    ] {
        options
            .config
            .features
            .enable(feature)
            .expect("fixture feature must be enabled");
    }
    assert!(options.config.features.plugin_recommendations_enabled());
    let plugins = seed.thread_manager.plugins_manager();
    let plugins_config = options.config.plugins_config_input();
    let mcp_server = core_test_support::responses::start_mock_server().await;
    let (mcp_control, mcp_release) = if cancel_unpublished {
        let (apps, control) =
            core_test_support::apps_test_server::AppsTestServer::mount_with_startup_control(
                &mcp_server,
            )
            .await?;
        let release = control.hold_next_successful_initialize();
        let mut servers = options.config.mcp_servers.get().clone();
        servers.insert(
            "stalled".into(),
            serde_json::from_value(serde_json::json!({
                "url": format!("{}/api/codex/ps/mcp", apps.chatgpt_base_url),
                "http_headers": {"Authorization":"Bearer synthetic-test-token"},
                "required":true, "startup_timeout_sec":120
            }))?,
        );
        options.config.mcp_servers.set(servers)?;
        (Some(control), Some(release))
    } else {
        (None, None)
    };
    let manager = seed.thread_manager.clone();
    let root = permits.try_acquire().unwrap();
    let caller = tokio::spawn(within_request(Some(root), async move {
        manager.start_thread(options).await
    }));
    tokio::time::timeout(DRAIN_DEADLINE, recommendation.entered.acquire())
        .await??
        .forget();
    assert_eq!(recommendation.requests.load(Ordering::SeqCst), 1);
    if let Some(control) = mcp_control.as_ref() {
        tokio::time::timeout(DRAIN_DEADLINE, async {
            while control.initialize_attempts() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        permits.close();
        caller.abort();
        assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
    } else {
        let created = tokio::time::timeout(DRAIN_DEADLINE, caller).await???;
        permits.close();
        assert!(
            created
                .thread
                .shutdown_and_wait_with_cleanup()
                .await
                .is_complete()
        );
    }
    // Poll actual completed original joins once; stale auth-read receipts cannot
    // impersonate the sole request which the loopback fixture still holds.
    assert_eq!(
        tasks.pending_len_for_test(),
        1,
        "exact pending recommendation original"
    );
    assert!(!tasks.observe_until(Instant::now()).await.terminal);
    assert!(permits.admitted_count() > 0);
    recommendation.release.add_permits(1);
    assert!(
        tasks
            .observe_until(Instant::now() + DRAIN_DEADLINE)
            .await
            .is_clean()
    );
    let retirement = seed
        .thread_manager
        .begin_shutdown(Instant::now() + DRAIN_DEADLINE)
        .unwrap();
    let report = retirement.wait().await.unwrap();
    assert!(
        report.is_complete(),
        "original constructor cleanup: {report:?}"
    );
    drop(mcp_release);
    idle_constructor(&permits, &tasks).await;
    assert_eq!(
        permits.admitted_count(),
        0,
        "retained plugin manager cannot retain finite authority"
    );
    // Stop the endpoint before asking the same manager: only a successful cache
    // publication can return Endpoint without another network response.
    recommendation.finish().await;
    let credential = CodexAuth::create_dummy_chatgpt_auth_for_testing();
    assert!(matches!(
        plugins
            .recommended_plugins_mode_for_config(&plugins_config, Some(&credential))
            .await,
        codex_core_plugins::RecommendedPluginsMode::Endpoint { .. }
    ));
    assert_eq!(permits.admitted_count(), 0);
    Ok(())
}

#[tokio::test]
async fn actual_blocking_work_remains_counted_after_caller_abort_and_owner_close() {
    let tasks = Arc::new(ProcessorTasks::default());
    let permits = AccountWorkPermits::new();
    let operation = host(&permits, Some(tasks.ticket()))
        .admit_operation_work()
        .unwrap()
        .unwrap();
    let spawner = operation.auth_task_spawner().unwrap();
    drop(operation);
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let finished = Arc::new(AtomicBool::new(false));
    let done = finished.clone();
    let caller = tokio::spawn(codex_model_provider::with_auth_task_scope(
        codex_model_provider::AuthTaskAccess::Admitted(spawner),
        async move {
            codex_model_provider::run_blocking_call(move || {
                entered.send(()).unwrap();
                released.recv().unwrap();
                done.store(true, Ordering::SeqCst);
                9
            })
            .await
        },
    ));
    tokio::time::timeout(DRAIN_DEADLINE, started)
        .await
        .unwrap()
        .unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    permits.close();
    assert!(permits.admitted_count() > 0);
    assert_eq!(tasks.pending_len_for_test(), 1);
    let first = tasks.shutdown_until(Instant::now()).await;
    assert!(!first.terminal && !finished.load(Ordering::SeqCst));
    release.send(()).unwrap();
    tokio::time::timeout(DRAIN_DEADLINE, async {
        loop {
            let report = tasks.shutdown_until(Instant::now()).await;
            if report.terminal {
                assert!(report.is_clean());
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(finished.load(Ordering::SeqCst));
    assert_eq!(permits.admitted_count(), 0);
}

#[tokio::test]
async fn actual_blocking_scope_transfers_nested_auth_original_after_parent_returns() {
    let tasks = ProcessorTasks::default();
    let permits = AccountWorkPermits::new();
    let operation = host(&permits, Some(tasks.ticket()))
        .admit_operation_work()
        .unwrap()
        .unwrap();
    let spawner = operation.auth_task_spawner().unwrap();
    drop(operation);
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let value = codex_model_provider::with_auth_task_scope(
        codex_model_provider::AuthTaskAccess::Admitted(spawner),
        async move {
            codex_model_provider::run_blocking_call(move || {
                let observation = codex_model_provider::spawn_provider_task(Box::pin(async move {
                    entered.send(()).unwrap();
                    released.await.unwrap();
                }))
                .unwrap();
                assert!(observation.is_some());
                drop(observation);
                21
            })
            .await
        },
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(value, 21);
    tokio::time::timeout(DRAIN_DEADLINE, started)
        .await
        .unwrap()
        .unwrap();
    permits.close();
    assert!(permits.admitted_count() > 0);
    assert_eq!(tasks.pending_len_for_test(), 1);
    assert!(!tasks.observe_until(Instant::now()).await.terminal);
    release.send(()).unwrap();
    assert!(
        tasks
            .shutdown_until(Instant::now() + DRAIN_DEADLINE)
            .await
            .is_clean()
    );
    assert_eq!(permits.admitted_count(), 0);
}

#[tokio::test]
async fn actual_blocking_closed_registration_refuses_body_without_fresh_work() {
    let tasks = ProcessorTasks::default();
    let permits = AccountWorkPermits::new();
    let operation = host(&permits, Some(tasks.ticket()))
        .admit_operation_work()
        .unwrap()
        .unwrap();
    let spawner = operation.auth_task_spawner().unwrap();
    drop(operation);
    tasks.close_registration().unwrap();
    permits.close();
    let polled = Arc::new(AtomicBool::new(false));
    let invoked = polled.clone();
    let result = codex_model_provider::with_auth_task_scope(
        codex_model_provider::AuthTaskAccess::Admitted(spawner),
        async move {
            let _ = codex_model_provider::run_blocking_call(move || {
                invoked.store(true, Ordering::SeqCst)
            })
            .await;
        },
    )
    .await;
    assert!(result.is_err());
    assert!(!polled.load(Ordering::SeqCst));
    assert_eq!(permits.admitted_count(), 0);
    assert!(tasks.shutdown_until(Instant::now()).await.is_clean());
}

#[tokio::test]
async fn actual_blocking_panic_releases_work_but_never_reports_clean_retirement() {
    let tasks = ProcessorTasks::default();
    let permits = AccountWorkPermits::new();
    let operation = host(&permits, Some(tasks.ticket()))
        .admit_operation_work()
        .unwrap()
        .unwrap();
    let spawner = operation.auth_task_spawner().unwrap();
    drop(operation);
    let result = codex_model_provider::with_auth_task_scope(
        codex_model_provider::AuthTaskAccess::Admitted(spawner),
        async {
            codex_model_provider::run_blocking_call(|| -> () {
                panic!("synthetic actual counted blocking panic");
            })
            .await
        },
    )
    .await;
    assert!(result.is_err());
    assert_eq!(permits.admitted_count(), 0);
    let report = tasks.shutdown_until(Instant::now()).await;
    assert!(report.terminal && report.panicked && !report.is_clean());
}

#[tokio::test]
async fn actual_blocking_nonsend_scope_drop_stays_inside_finite_auth_context() {
    struct ScopeExit {
        dropped: Arc<AtomicBool>,
        entered: Option<tokio::sync::oneshot::Sender<()>>,
        release: Option<tokio::sync::oneshot::Receiver<()>>,
        _not_send: std::rc::Rc<()>,
    }
    impl Drop for ScopeExit {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
            let entered = self.entered.take().unwrap();
            let release = self.release.take().unwrap();
            assert!(tokio::runtime::Handle::try_current().is_ok());
            assert!(
                codex_model_provider::spawn_provider_task(Box::pin(async move {
                    entered.send(()).unwrap();
                    release.await.unwrap();
                }))
                .unwrap()
                .is_some()
            );
        }
    }
    let tasks = ProcessorTasks::default();
    let permits = AccountWorkPermits::new();
    let operation = host(&permits, Some(tasks.ticket()))
        .admit_operation_work()
        .unwrap()
        .unwrap();
    let spawner = operation.auth_task_spawner().unwrap();
    drop(operation);
    let dropped = Arc::new(AtomicBool::new(false));
    let scoped = dropped.clone();
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let value = codex_model_provider::with_auth_task_scope(
        codex_model_provider::AuthTaskAccess::Admitted(spawner),
        async move {
            codex_model_provider::run_blocking_call_with_scope(
                move || ScopeExit {
                    dropped: scoped,
                    entered: Some(entered),
                    release: Some(released),
                    _not_send: std::rc::Rc::new(()),
                },
                || 33,
            )
            .await
        },
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(value, 33);
    assert!(dropped.load(Ordering::SeqCst));
    tokio::time::timeout(DRAIN_DEADLINE, started)
        .await
        .unwrap()
        .unwrap();
    permits.close();
    assert!(permits.admitted_count() > 0);
    release.send(()).unwrap();
    assert!(
        tasks
            .shutdown_until(Instant::now() + DRAIN_DEADLINE)
            .await
            .is_clean()
    );
    assert_eq!(permits.admitted_count(), 0);
}

#[test]
fn actual_blocking_original_keeps_work_after_runtime_shutdown() {
    let tasks = Arc::new(ProcessorTasks::default());
    let permits = AccountWorkPermits::new();
    let operation = host(&permits, Some(tasks.ticket()))
        .admit_operation_work()
        .unwrap()
        .unwrap();
    let spawner = operation.auth_task_spawner().unwrap();
    drop(operation);
    let (release, released) = std::sync::mpsc::channel();
    let (shutdown, shutdown_seen) = std::sync::mpsc::channel();
    let (finished, finished_seen) = std::sync::mpsc::channel();
    let runtime_thread = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let (entered, started) = tokio::sync::oneshot::channel();
            let caller = tokio::spawn(codex_model_provider::with_auth_task_scope(
                codex_model_provider::AuthTaskAccess::Admitted(spawner),
                async move {
                    codex_model_provider::run_blocking_call(move || {
                        entered.send(()).unwrap();
                        released.recv().unwrap();
                        finished.send(()).unwrap();
                    })
                    .await
                },
            ));
            started.await.unwrap();
            caller.abort();
            assert!(caller.await.unwrap_err().is_cancelled());
        });
        // This new fixture ends the async runtime while the confirmed-running
        // physical body is held. Product/runtime/test-profile settings unchanged.
        runtime.shutdown_timeout(std::time::Duration::ZERO);
        shutdown.send(()).unwrap();
    });
    shutdown_seen
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    permits.close();
    let count_while_held = permits.admitted_count();
    let originals_while_held = tasks.pending_len_for_test();
    let unfinished_while_held = finished_seen.try_recv().is_err();
    // Always release the fixture before reporting assertions.
    release.send(()).unwrap();
    runtime_thread.join().unwrap();
    finished_seen
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    let start = std::time::Instant::now();
    while tasks.pending_len_for_test() != 0 {
        assert!(start.elapsed() < std::time::Duration::from_secs(2));
        std::thread::yield_now();
    }
    assert!(count_while_held > 0);
    assert_eq!(originals_while_held, 1);
    assert!(unfinished_while_held);
    assert_eq!(permits.admitted_count(), 0);
    let observer = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert!(
        observer
            .block_on(tasks.shutdown_until(Instant::now()))
            .is_clean()
    );
}

#[tokio::test]
async fn actual_retained_operation_runs_its_tail_after_blocking_caller_cancellation() {
    let tasks = ProcessorTasks::default();
    let permits = AccountWorkPermits::new();
    let operation = host(&permits, Some(tasks.ticket()))
        .admit_operation_work()
        .unwrap()
        .unwrap();
    let spawner = operation.auth_task_spawner().unwrap();
    drop(operation);
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let tail = Arc::new(AtomicBool::new(false));
    let tail_done = tail.clone();
    let caller = tokio::spawn(codex_model_provider::run_auth_call(
        codex_model_provider::AuthTaskAccess::Admitted(spawner),
        async move {
            codex_model_provider::run_blocking_call(move || {
                entered.send(()).unwrap();
                released.recv().unwrap();
            })
            .await
            .unwrap();
            tail_done.store(true, Ordering::SeqCst);
        },
    ));
    tokio::time::timeout(DRAIN_DEADLINE, started)
        .await
        .unwrap()
        .unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    permits.close();
    assert!(!tail.load(Ordering::SeqCst));
    assert!(permits.admitted_count() > 0);
    assert_eq!(
        tasks.pending_len_for_test(),
        2,
        "retained whole operation plus physical blocking original"
    );
    let report = tasks.shutdown_until(Instant::now()).await;
    release.send(()).unwrap();
    assert!(!report.terminal);
    tokio::time::timeout(DRAIN_DEADLINE, async {
        loop {
            let report = tasks.shutdown_until(Instant::now()).await;
            if report.terminal {
                assert!(report.is_clean());
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(tail.load(Ordering::SeqCst));
    assert_eq!(permits.admitted_count(), 0);
}

#[tokio::test]
async fn recommendation_fixture_idle_connection_cannot_drop_held_request() -> anyhow::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let fixture = HeldRecommendation::start().await?;
    let address = fixture.base.strip_prefix("http://").unwrap();
    let idle = tokio::net::TcpStream::connect(address).await?;
    drop(idle);
    let mut client = tokio::net::TcpStream::connect(address).await?;
    client.write_all(b"GET /ps/plugins/suggested/codex?scope=GLOBAL HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer access token\r\nChatGPT-Account-ID: account_id\r\n\r\n").await?;
    tokio::time::timeout(DRAIN_DEADLINE, fixture.entered.acquire())
        .await??
        .forget();
    assert_eq!(
        fixture.requests.load(Ordering::SeqCst),
        1,
        "idle socket is not a request"
    );
    assert!(!fixture.task.as_ref().unwrap().is_finished());
    fixture.release.add_permits(1);
    let mut response = String::new();
    tokio::time::timeout(DRAIN_DEADLINE, client.read_to_string(&mut response)).await??;
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(response.ends_with(r#"{"enabled":true,"plugins":[]}"#));
    assert_eq!(fixture.requests.load(Ordering::SeqCst), 1);
    fixture.finish().await;
    Ok(())
}

#[tokio::test]
async fn recommendation_fixture_partial_header_eof_still_fails() -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut fixture = HeldRecommendation::start().await?;
    let address = fixture.base.strip_prefix("http://").unwrap();
    let mut client = tokio::net::TcpStream::connect(address).await?;
    client.write_all(b"GET /ps/plugins/suggested/").await?;
    client.shutdown().await?;
    let result = tokio::time::timeout(DRAIN_DEADLINE, fixture.task.take().unwrap()).await?;
    assert!(
        matches!(result, Err(error) if error.is_panic()),
        "partial request must not be accepted as idle EOF"
    );
    assert_eq!(fixture.requests.load(Ordering::SeqCst), 0);
    Ok(())
}

fn queued_auth_access(
    tasks: &ProcessorTasks,
    permits: &AccountWorkPermits,
) -> codex_model_provider::AuthTaskAccess {
    let operation = host(permits, Some(tasks.ticket()))
        .admit_operation_work()
        .unwrap()
        .unwrap();
    let spawner = operation.auth_task_spawner().unwrap();
    drop(operation);
    codex_model_provider::AuthTaskAccess::Admitted(spawner)
}

#[tokio::test]
async fn queued_auth_request_is_counted_before_dispatch_and_ends_after_last_endpoint() {
    let tasks = ProcessorTasks::default();
    let permits = AccountWorkPermits::new();
    let family = codex_core_plugins::PluginTaskRegistry::default();
    let request =
        codex_model_provider::with_auth_task_scope(queued_auth_access(&tasks, &permits), async {
            family.prepare_auth_request()
        })
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    permits.close();
    assert!(permits.try_acquire().is_none());
    assert!(permits.admitted_count() > 0);
    assert_eq!(tasks.pending_len_for_test(), 1);
    assert!(!tasks.observe_until(Instant::now()).await.terminal);
    assert_eq!(request.run(async { 42 }).await.unwrap(), 42);
    assert!(
        permits.admitted_count() > 0,
        "endpoint retains only this finite request"
    );
    drop(request);
    assert!(
        family
            .shutdown_until(Instant::now() + DRAIN_DEADLINE)
            .await
            .is_clean()
    );
    assert!(
        tasks
            .shutdown_until(Instant::now() + DRAIN_DEADLINE)
            .await
            .is_clean()
    );
    assert_eq!(permits.admitted_count(), 0);
}

#[tokio::test]
async fn queued_auth_command_survives_result_observer_abort_and_family_deadline() {
    let tasks = ProcessorTasks::default();
    let permits = AccountWorkPermits::new();
    let family = codex_core_plugins::PluginTaskRegistry::default();
    let request =
        codex_model_provider::with_auth_task_scope(queued_auth_access(&tasks, &permits), async {
            family.prepare_auth_request()
        })
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let finished = Arc::new(AtomicBool::new(false));
    let done = finished.clone();
    let observer = tokio::spawn(async move {
        request
            .run(async move {
                entered.send(()).unwrap();
                released.await.unwrap();
                done.store(true, Ordering::SeqCst);
            })
            .await
    });
    tokio::time::timeout(DRAIN_DEADLINE, started)
        .await
        .unwrap()
        .unwrap();
    permits.close();
    observer.abort();
    assert!(observer.await.unwrap_err().is_cancelled());
    assert!(!finished.load(Ordering::SeqCst));
    assert!(permits.admitted_count() > 0);
    assert!(!family.shutdown_until(Instant::now()).await.terminal);
    assert!(!tasks.observe_until(Instant::now()).await.terminal);
    release.send(()).unwrap();
    assert!(
        tasks
            .shutdown_until(Instant::now() + DRAIN_DEADLINE)
            .await
            .is_clean()
    );
    assert!(family.shutdown_until(Instant::now()).await.is_clean());
    assert!(finished.load(Ordering::SeqCst));
    assert_eq!(permits.admitted_count(), 0);
}

#[tokio::test]
async fn queued_auth_endpoint_aliases_never_exchange_separate_invoking_roots() {
    let a = ProcessorTasks::default();
    let b = ProcessorTasks::default();
    let pa = AccountWorkPermits::new();
    let pb = AccountWorkPermits::new();
    let family = codex_core_plugins::PluginTaskRegistry::default();
    let ra = codex_model_provider::with_auth_task_scope(queued_auth_access(&a, &pa), async {
        family.prepare_auth_request()
    })
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    let rb = codex_model_provider::with_auth_task_scope(queued_auth_access(&b, &pb), async {
        family.prepare_auth_request()
    })
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    let selected_callback_a = ra.clone();
    pa.close();
    pb.close();
    drop(ra);
    assert!(pa.admitted_count() > 0 && pb.admitted_count() > 0);
    rb.run(async {}).await.unwrap();
    drop(rb);
    assert!(
        b.shutdown_until(Instant::now() + DRAIN_DEADLINE)
            .await
            .is_clean()
    );
    assert_eq!(pb.admitted_count(), 0);
    assert!(pa.admitted_count() > 0);
    selected_callback_a.run(async {}).await.unwrap();
    drop(selected_callback_a);
    assert!(
        a.shutdown_until(Instant::now() + DRAIN_DEADLINE)
            .await
            .is_clean()
    );
    assert!(
        family
            .shutdown_until(Instant::now() + DRAIN_DEADLINE)
            .await
            .is_clean()
    );
    assert_eq!(pa.admitted_count(), 0);
}

#[tokio::test]
async fn queued_auth_closed_family_refuses_before_external_host_birth() {
    let tasks = ProcessorTasks::default();
    let permits = AccountWorkPermits::new();
    let family = codex_core_plugins::PluginTaskRegistry::default();
    family.close();
    let result =
        codex_model_provider::with_auth_task_scope(queued_auth_access(&tasks, &permits), async {
            family.prepare_auth_request()
        })
        .await
        .unwrap();
    assert!(result.is_err());
    assert_eq!(tasks.len(), 0);
    assert_eq!(permits.admitted_count(), 0);
    assert!(family.shutdown_until(Instant::now()).await.is_clean());
}

#[tokio::test]
async fn queued_auth_closed_host_attaches_unavailable_without_fallback() {
    let tasks = ProcessorTasks::default();
    let permits = AccountWorkPermits::new();
    let access = queued_auth_access(&tasks, &permits);
    tasks.close_registration().unwrap();
    permits.close();
    let family = codex_core_plugins::PluginTaskRegistry::default();
    let result = codex_model_provider::with_auth_task_scope(access, async {
        assert!(family.prepare_auth_request().is_err());
    })
    .await;
    assert!(result.is_err());
    assert_eq!(tasks.len(), 0);
    assert_eq!(permits.admitted_count(), 0);
    let report = family.shutdown_until(Instant::now() + DRAIN_DEADLINE).await;
    assert!(report.terminal && report.unavailable && !report.is_clean());
}

#[tokio::test]
async fn queued_auth_command_panic_is_sticky_in_both_original_ledgers() {
    let tasks = ProcessorTasks::default();
    let permits = AccountWorkPermits::new();
    let family = codex_core_plugins::PluginTaskRegistry::default();
    let request =
        codex_model_provider::with_auth_task_scope(queued_auth_access(&tasks, &permits), async {
            family.prepare_auth_request()
        })
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    request
        .dispatch(Box::pin(async {
            panic!("synthetic deferred command panic");
        }))
        .unwrap();
    let report = tasks.shutdown_until(Instant::now() + DRAIN_DEADLINE).await;
    assert!(report.terminal && report.panicked && !report.is_clean());
    let invoked = Arc::new(AtomicBool::new(false));
    let child = invoked.clone();
    assert!(
        request
            .dispatch(Box::pin(async move {
                child.store(true, Ordering::SeqCst);
            }))
            .is_err()
    );
    assert!(!invoked.load(Ordering::SeqCst));
    drop(request);
    let report = family.shutdown_until(Instant::now() + DRAIN_DEADLINE).await;
    assert!(report.terminal && report.panicked && !report.is_clean());
    assert!(family.shutdown_until(Instant::now()).await.panicked);
    assert_eq!(permits.admitted_count(), 0);
}

struct ReentrantQueuedAuthSpawner {
    family: codex_core_plugins::PluginTaskRegistry,
    inner: Arc<dyn codex_model_provider::AuthTaskSpawner>,
    panic_before_birth: bool,
}
impl std::fmt::Debug for ReentrantQueuedAuthSpawner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReentrantQueuedAuthSpawner")
    }
}
impl codex_model_provider::AuthTaskSpawner for ReentrantQueuedAuthSpawner {
    fn spawn(
        &self,
        future: codex_model_provider::AuthTaskFuture,
    ) -> Result<codex_model_provider::AuthTaskObservation, codex_model_provider::AuthTaskFailure>
    {
        self.family.close(); // Takes the family lock; reserve must have released it.
        assert!(
            !self.panic_before_birth,
            "synthetic reservation attachment panic"
        );
        self.inner.spawn(future)
    }
}

#[tokio::test]
async fn queued_auth_family_close_during_external_birth_retains_prior_reservation() {
    let tasks = ProcessorTasks::default();
    let permits = AccountWorkPermits::new();
    let family = codex_core_plugins::PluginTaskRegistry::default();
    let codex_model_provider::AuthTaskAccess::Admitted(inner) =
        queued_auth_access(&tasks, &permits)
    else {
        panic!("admitted fixture");
    };
    let access =
        codex_model_provider::AuthTaskAccess::Admitted(Arc::new(ReentrantQueuedAuthSpawner {
            family: family.clone(),
            inner,
            panic_before_birth: false,
        }));
    let request =
        codex_model_provider::with_auth_task_scope(access, async { family.prepare_auth_request() })
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    assert!(
        family.prepare_auth_request().is_err(),
        "no later admission after reentrant close"
    );
    assert!(permits.admitted_count() > 0);
    request.run(async {}).await.unwrap();
    drop(request);
    assert!(
        family
            .shutdown_until(Instant::now() + DRAIN_DEADLINE)
            .await
            .is_clean()
    );
    assert!(
        tasks
            .shutdown_until(Instant::now() + DRAIN_DEADLINE)
            .await
            .is_clean()
    );
    assert_eq!(permits.admitted_count(), 0);
}

#[tokio::test]
async fn queued_auth_unattached_reservation_panic_is_unavailable_not_clean() {
    use futures::FutureExt;
    let tasks = ProcessorTasks::default();
    let permits = AccountWorkPermits::new();
    let family = codex_core_plugins::PluginTaskRegistry::default();
    let codex_model_provider::AuthTaskAccess::Admitted(inner) =
        queued_auth_access(&tasks, &permits)
    else {
        panic!("admitted fixture");
    };
    let access =
        codex_model_provider::AuthTaskAccess::Admitted(Arc::new(ReentrantQueuedAuthSpawner {
            family: family.clone(),
            inner,
            panic_before_birth: true,
        }));
    assert!(
        std::panic::AssertUnwindSafe(codex_model_provider::with_auth_task_scope(access, async {
            let _ = family.prepare_auth_request();
        }))
        .catch_unwind()
        .await
        .is_err()
    );
    assert_eq!(tasks.len(), 0);
    assert_eq!(permits.admitted_count(), 0);
    let report = family.shutdown_until(Instant::now() + DRAIN_DEADLINE).await;
    assert!(report.terminal && report.unavailable && !report.is_clean());
}

#[tokio::test]
async fn queued_auth_unmanaged_preparation_creates_no_managed_endpoint() {
    let family = codex_core_plugins::PluginTaskRegistry::default();
    assert!(family.prepare_auth_request().unwrap().is_none());
    assert!(
        family
            .shutdown_until(Instant::now() + DRAIN_DEADLINE)
            .await
            .is_clean()
    );
}

struct CloseFamilyOnObservationDrop(codex_core_plugins::PluginTaskRegistry);
impl Drop for CloseFamilyOnObservationDrop {
    fn drop(&mut self) {
        self.0.close();
    }
}
struct ReentrantObservationSpawner {
    family: codex_core_plugins::PluginTaskRegistry,
    inner: Arc<dyn codex_model_provider::AuthTaskSpawner>,
    panic_on_poll: bool,
}
impl std::fmt::Debug for ReentrantObservationSpawner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReentrantObservationSpawner")
    }
}
impl codex_model_provider::AuthTaskSpawner for ReentrantObservationSpawner {
    fn spawn(
        &self,
        future: codex_model_provider::AuthTaskFuture,
    ) -> Result<codex_model_provider::AuthTaskObservation, codex_model_provider::AuthTaskFailure>
    {
        let original = self.inner.spawn(future)?;
        let family = self.family.clone();
        let panic_on_poll = self.panic_on_poll;
        Ok(Box::pin(async move {
            let _close = CloseFamilyOnObservationDrop(family);
            assert!(!panic_on_poll, "synthetic external observation panic");
            original.await
        }))
    }
}

#[tokio::test]
async fn queued_auth_observation_completion_reenters_family_without_locked_poll() {
    let tasks = ProcessorTasks::default();
    let permits = AccountWorkPermits::new();
    let family = codex_core_plugins::PluginTaskRegistry::default();
    let codex_model_provider::AuthTaskAccess::Admitted(inner) =
        queued_auth_access(&tasks, &permits)
    else {
        panic!("admitted fixture");
    };
    let access =
        codex_model_provider::AuthTaskAccess::Admitted(Arc::new(ReentrantObservationSpawner {
            family: family.clone(),
            inner,
            panic_on_poll: false,
        }));
    let request =
        codex_model_provider::with_auth_task_scope(access, async { family.prepare_auth_request() })
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    drop(request);
    assert!(
        tasks
            .shutdown_until(Instant::now() + DRAIN_DEADLINE)
            .await
            .is_clean()
    );
    // Completing the external observation closes this SAME family. Polling it
    // under the state lock would deadlock rather than reach this refusal.
    assert!(family.prepare_auth_request().is_err());
    assert!(
        family
            .shutdown_until(Instant::now() + DRAIN_DEADLINE)
            .await
            .is_clean()
    );
    assert_eq!(permits.admitted_count(), 0);
}

#[tokio::test]
async fn queued_auth_observation_panic_is_unavailable_while_actual_original_stays_owned() {
    let tasks = ProcessorTasks::default();
    let permits = AccountWorkPermits::new();
    let family = codex_core_plugins::PluginTaskRegistry::default();
    let codex_model_provider::AuthTaskAccess::Admitted(inner) =
        queued_auth_access(&tasks, &permits)
    else {
        panic!("admitted fixture");
    };
    let access =
        codex_model_provider::AuthTaskAccess::Admitted(Arc::new(ReentrantObservationSpawner {
            family: family.clone(),
            inner,
            panic_on_poll: true,
        }));
    let request =
        codex_model_provider::with_auth_task_scope(access, async { family.prepare_auth_request() })
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    let report = family.shutdown_until(Instant::now()).await;
    assert!(
        report.terminal && report.unavailable && !report.is_clean(),
        "lost observation is never clean completion"
    );
    assert!(
        !tasks.observe_until(Instant::now()).await.terminal,
        "actual original is still independently retained"
    );
    assert!(permits.admitted_count() > 0);
    drop(request);
    assert!(
        tasks
            .shutdown_until(Instant::now() + DRAIN_DEADLINE)
            .await
            .is_clean()
    );
    assert_eq!(permits.admitted_count(), 0);
    assert!(family.shutdown_until(Instant::now()).await.unavailable);
}
