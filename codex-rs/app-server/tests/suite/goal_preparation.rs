//! Exercises the real host→Goal handoff, including the refused Review caller.

use anyhow::Result;
use codex_analytics::AnalyticsEventsClient;
use codex_core::TurnInputRequest;
use codex_extension_api::ExtensionData;
use codex_extension_api::ExtensionEventSink;
use codex_extension_api::ExtensionFuture;
use codex_extension_api::ExtensionRegistryBuilder;
use codex_extension_api::ExtensionWarning;
use codex_extension_api::TurnLifecycleContributor;
use codex_extension_api::TurnStartInput;
use codex_extension_api::TurnStartPhase;
use codex_goal_extension::GoalExtensionConfig;
use codex_goal_extension::GoalRuntimeHandle;
use codex_goal_extension::GoalService;
use codex_protocol::items::TurnItem;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::ReviewRequest;
use codex_protocol::protocol::ReviewTarget;
use codex_protocol::protocol::SessionSource;
use codex_protocol::user_input::UserInput;
use codex_utils_absolute_path::test_support::PathExt;
use core_test_support::responses;
use core_test_support::test_codex::test_codex;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::Weak;
use tempfile::TempDir;
use tokio::sync::Semaphore;
use tokio::time::Duration;
use tokio::time::timeout;

#[derive(Default)]
struct GoalEvents(Mutex<Vec<Event>>);

impl ExtensionEventSink for GoalEvents {
    fn emit(&self, event: Event) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(event);
    }
    fn emit_warning(&self, _warning: ExtensionWarning) {}
}

struct Pause {
    phase: TurnStartPhase,
    entered: Semaphore,
    release: Semaphore,
    review_item: Semaphore,
}

impl TurnLifecycleContributor for Pause {
    fn turn_start_phase(&self, _store: &ExtensionData) -> TurnStartPhase {
        self.phase
    }
    fn on_turn_start<'a>(&'a self, _input: TurnStartInput<'a>) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            self.entered.add_permits(1);
            self.release
                .acquire()
                .await
                .expect("release preparation")
                .forget();
        })
    }
    fn on_item_completed<'a>(
        &'a self,
        _thread: &'a ExtensionData,
        _turn: &'a ExtensionData,
        item: &'a TurnItem,
    ) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            if matches!(item, TurnItem::EnteredReviewMode(_)) {
                self.review_item.add_permits(1);
            }
        })
    }
}

#[tokio::test]
async fn real_goal_handoff_excludes_refused_review_and_precedes_first_regular_item() -> Result<()> {
    for refused_review in [true, false] {
        let db_home = TempDir::new()?;
        let state = codex_state::StateRuntime::init(
            codex_state::SqliteConfig::new_for_testing(db_home.path().abs()),
            "test-provider".into(),
        )
        .await?;
        let sink = Arc::new(GoalEvents::default());
        let mut extensions =
            ExtensionRegistryBuilder::<codex_core::config::Config>::with_event_sink(sink.clone());
        let service = Arc::new(GoalService::new());
        codex_goal_extension::install_with_backend(
            &mut extensions,
            Arc::clone(&state),
            AnalyticsEventsClient::disabled(),
            /*metrics_client*/ None,
            Weak::new(),
            Arc::clone(&service),
            |_| GoalExtensionConfig {
                enabled: true,
                max_goal_token_budget: None,
            },
        );
        let before = Arc::new(Pause {
            phase: TurnStartPhase::BeforeTaskRegistration,
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            review_item: Semaphore::new(0),
        });
        let installed = Arc::new(Pause {
            phase: TurnStartPhase::RegularTaskStart,
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            review_item: Semaphore::new(0),
        });
        extensions.turn_lifecycle_contributor(before.clone());
        extensions.turn_lifecycle_contributor(installed.clone());
        let server = responses::start_mock_server().await;
        let test = test_codex()
            .with_extensions(Arc::new(extensions.build()))
            .build_with_auto_env(&server)
            .await?;
        let thread_id = test.session_configured.thread_id;
        let metadata = codex_state::ThreadMetadataBuilder::new(
            thread_id,
            state.sqlite().home().join("rollout.jsonl"),
            chrono::Utc::now(),
            SessionSource::Cli,
        );
        state
            .upsert_thread(&metadata.build("test-provider"))
            .await?;
        state
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "finish work",
                codex_state::ThreadGoalStatus::Active,
                /*token_budget*/ None,
            )
            .await?;
        let goal_runtime = test
            .codex
            .thread_extension_data()
            .get::<GoalRuntimeHandle>()
            .expect("real Goal contributor");
        service
            .restore_thread_runtime_after_resume(thread_id)
            .await?;
        if refused_review {
            test.codex
                .submit(Op::Review {
                    review_request: ReviewRequest {
                        target: ReviewTarget::Custom {
                            instructions: "review work".into(),
                        },
                        user_facing_hint: None,
                    },
                })
                .await?;
            timeout(Duration::from_secs(5), before.entered.acquire())
                .await??
                .forget();
            codex_core::test_support::close_task_admission(&test.codex).await;
            before.release.add_permits(1);
            timeout(Duration::from_secs(5), before.review_item.acquire())
                .await??
                .forget();
        } else {
            let thread = Arc::clone(&test.codex);
            let start = tokio::spawn(async move {
                thread
                    .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
                        text: "work".into(),
                        text_elements: Vec::new(),
                    }]))
                    .await
            });
            timeout(Duration::from_secs(5), before.entered.acquire())
                .await??
                .forget();
            before.release.add_permits(1);
            start.await??;
            timeout(Duration::from_secs(5), installed.entered.acquire())
                .await??
                .forget();
        }
        // The task is either refused or parked before any optional item/token
        // callback. Charge nonzero elapsed progress and inspect its attribution.
        tokio::time::sleep(Duration::from_millis(1100)).await;
        sink.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        goal_runtime
            .prepare_external_goal_mutation()
            .await
            .map_err(anyhow::Error::msg)?;
        {
            let events = sink.0.lock().unwrap_or_else(PoisonError::into_inner);
            let progress = events
                .iter()
                .find_map(|event| match &event.msg {
                    EventMsg::ThreadGoalUpdated(updated) => Some(updated),
                    _ => None,
                })
                .expect("nonzero progress was accounted");
            assert_eq!(!refused_review, progress.turn_id.is_some());
        }
        test.codex.shutdown_and_wait().await?;
    }
    Ok(())
}
