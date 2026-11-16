//! Exercises Guardian registration and denial cleanup through real turns.

use super::*;
use codex_core::TurnInputRequest;
use codex_protocol::openai_models::ModelInfo;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::TurnAbortReason;
use codex_protocol::user_input::UserInput;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::sse;
use core_test_support::responses::start_mock_server;
use core_test_support::streaming_sse::StreamingSseChunk;
use core_test_support::streaming_sse::start_streaming_sse_server;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use core_test_support::wait_for_event_match;
use pretty_assertions::assert_eq;
use std::time::Duration;

struct StaleDenials(ModelInfo);

impl TurnLifecycleContributor for StaleDenials {
    fn on_turn_start<'a>(&'a self, input: TurnStartInput<'a>) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            let denials = ReviewDenials::for_thread(input.thread_store);
            for _ in 0..2 {
                assert_eq!(denials.record_denial(input.turn_id, &self.0).await, None);
            }
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_turn_clears_extension_owned_denials() -> anyhow::Result<()> {
    let home = tempfile::tempdir()?;
    let config = core_test_support::load_default_config_for_test(&home).await;
    let model = codex_core::test_support::construct_model_info_offline("gpt-5", &config);
    let mut extensions = ExtensionRegistryBuilder::<Config>::new();
    // Seed stale state before the production Guardian contributor runs its start hook.
    extensions.turn_lifecycle_contributor(Arc::new(StaleDenials(model.clone())));
    let mut releases = Vec::new();
    let streams = (0..3)
        .map(|_| {
            let (release, gate) = tokio::sync::oneshot::channel();
            releases.push(release);
            vec![
                StreamingSseChunk {
                    gate: None,
                    body: sse(vec![ev_response_created("response")]),
                },
                StreamingSseChunk {
                    gate: Some(gate),
                    body: sse(vec![ev_completed("response")]),
                },
            ]
        })
        .collect();
    let (streaming, _) = start_streaming_sse_server(streams).await;
    let server = start_mock_server().await;
    let base_url = format!("{}/v1", streaming.uri());
    let test = test_codex()
        .with_extensions(Arc::new(extensions.build()))
        .with_config(move |config| {
            config.model_provider.base_url = Some(base_url);
            config.model_provider.supports_websockets = false;
        })
        .build_with_auto_env(&server)
        .await?;
    let denials = ReviewDenials::for_thread(test.codex.thread_extension_data());

    let mut previous_turn = None;
    // The third turn also checks that an interrupted turn leaves the next turn usable.
    for (index, release) in releases.into_iter().enumerate() {
        test.codex
            .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
                text: "Continue.".into(),
                text_elements: Vec::new(),
            }]))
            .await?;
        let turn_id = wait_for_event_match(&test.codex, |event| match event {
            EventMsg::TurnStarted(event) => Some(event.turn_id.clone()),
            _ => None,
        })
        .await;
        // An interruption event can precede its cleanup. The next turn starts after it.
        if let Some(previous) = previous_turn.replace(turn_id.clone()) {
            assert_eq!(denials.record_denial(&previous, &model).await, None);
        }
        tokio::time::timeout(
            Duration::from_secs(/*secs*/ 10),
            streaming.wait_for_request_count(index + 1),
        )
        .await?;
        // Without start cleanup, these denials reach the three-denial limit.
        for _ in 0..2 {
            assert_eq!(denials.record_denial(&turn_id, &model).await, None);
        }
        if index == 1 {
            test.codex.submit(Op::Interrupt).await?;
            wait_for_event(&test.codex, |event| {
                matches!(event, EventMsg::TurnAborted(event)
                    if event.reason == TurnAbortReason::Interrupted
                        && event.turn_id.as_deref() == Some(turn_id.as_str()))
            })
            .await;
            drop(release);
        } else {
            release.send(()).unwrap();
            wait_for_event(
                &test.codex,
                |event| matches!(event, EventMsg::TurnComplete(event) if event.turn_id == turn_id),
            )
            .await;
        }
    }
    test.codex.shutdown_and_wait().await?;
    assert_eq!(
        denials.record_denial(&previous_turn.unwrap(), &model).await,
        None
    );
    streaming.shutdown().await;
    Ok(())
}

#[derive(Debug, Default)]
struct ReadyCounts {
    live: std::sync::atomic::AtomicUsize,
    derives: std::sync::atomic::AtomicUsize,
    refuse: std::sync::atomic::AtomicBool,
}
#[derive(Debug)]
struct ReadyWork(Arc<ReadyCounts>);
impl Drop for ReadyWork {
    fn drop(&mut self) {
        self.0
            .live
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}
impl codex_extension_api::HostOperationWork for ReadyWork {
    fn derive_operation(
        &self,
    ) -> Result<Box<dyn codex_extension_api::HostOperationWork>, codex_extension_api::TurnWorkRefused>
    {
        self.0
            .derives
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.0.refuse.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(codex_extension_api::TurnWorkRefused::Unavailable);
        }
        self.0
            .live
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Box::new(Self(Arc::clone(&self.0))))
    }
    fn derive_turn_work(
        &self,
        _: &codex_extension_api::ExtensionData,
        _: ExtensionFuture<'static, ()>,
    ) -> Result<
        Box<dyn codex_protocol::host_turn_work::HostTurnWork>,
        codex_extension_api::TurnWorkRefused,
    > {
        panic!("ready callback must not create a turn")
    }
}
#[tokio::test]
async fn ready_hook_derives_before_runtime_lookup_and_releases_unused_child() {
    let home = tempfile::tempdir().unwrap();
    let mut config = core_test_support::load_default_config_for_test(&home).await;
    config
        .permissions
        .approval_policy
        .set(AskForApproval::OnRequest)
        .unwrap();
    config.approvals_reviewer = ApprovalsReviewer::AutoReview;
    let counts = Arc::new(ReadyCounts::default());
    counts.live.store(1, std::sync::atomic::Ordering::SeqCst);
    let work = ReadyWork(Arc::clone(&counts));
    let extension = GuardianExtension {
        thread_manager: Weak::new(),
    };
    let session = codex_extension_api::ExtensionData::new("session");
    let thread = codex_extension_api::ExtensionData::new("thread-without-runtime");
    let mut ready = extension.on_thread_ready(ThreadReadyInput {
        account_work: Ok(Some(&work)),
        config: &config,
        session_source: &SessionSource::Exec,
        history_initialization: codex_extension_api::HistoryInitialization::New,
        history_mode: codex_protocol::protocol::ThreadHistoryMode::Legacy,
        session_store: &session,
        thread_store: &thread,
    });
    assert!(matches!(
        std::future::Future::poll(
            ready.as_mut(),
            &mut std::task::Context::from_waker(std::task::Waker::noop()),
        ),
        std::task::Poll::Ready(())
    ));
    drop(ready);
    assert_eq!(counts.derives.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(counts.live.load(std::sync::atomic::Ordering::SeqCst), 1);
    drop(work);
    assert_eq!(counts.live.load(std::sync::atomic::Ordering::SeqCst), 0);
}
#[tokio::test]
async fn ready_hook_refuses_expired_or_missing_constructor_work() {
    let home = tempfile::tempdir().unwrap();
    let mut config = core_test_support::load_default_config_for_test(&home).await;
    config
        .permissions
        .approval_policy
        .set(AskForApproval::OnRequest)
        .unwrap();
    config.approvals_reviewer = ApprovalsReviewer::AutoReview;
    let counts = Arc::new(ReadyCounts::default());
    counts.live.store(1, std::sync::atomic::Ordering::SeqCst);
    counts
        .refuse
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let work = ReadyWork(Arc::clone(&counts));
    let extension = GuardianExtension {
        thread_manager: Weak::new(),
    };
    let session = codex_extension_api::ExtensionData::new("session");
    let thread = codex_extension_api::ExtensionData::new("thread-without-runtime");
    for access in [
        Err(codex_extension_api::TurnWorkRefused::Unavailable),
        Ok(Some(&work as &dyn codex_extension_api::HostOperationWork)),
    ] {
        extension
            .on_thread_ready(ThreadReadyInput {
                account_work: access,
                config: &config,
                session_source: &SessionSource::Exec,
                history_initialization: codex_extension_api::HistoryInitialization::New,
                history_mode: codex_protocol::protocol::ThreadHistoryMode::Legacy,
                session_store: &session,
                thread_store: &thread,
            })
            .await;
    }
    assert_eq!(counts.derives.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(counts.live.load(std::sync::atomic::Ordering::SeqCst), 1);
    drop(work);
    assert_eq!(counts.live.load(std::sync::atomic::Ordering::SeqCst), 0);
}
