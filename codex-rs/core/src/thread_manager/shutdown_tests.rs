use super::*;
use crate::ThreadCleanupOutcome;
use crate::ThreadLoopOutcome;
use crate::ThreadShutdownOutcome;
use crate::config::Config;
use crate::config::ConfigBuilder;
use crate::thread_manager::StartThreadOptions;
use pretty_assertions::assert_eq;
use std::time::Duration;

#[test]
fn previously_completed_runtime_requires_normal_session_loop() {
    assert!(RuntimeOutcome::PreviouslyCompleted(SessionLoopOutcome::Normal).is_complete());
    assert!(!RuntimeOutcome::PreviouslyCompleted(SessionLoopOutcome::Cancelled).is_complete());
    assert!(!RuntimeOutcome::PreviouslyCompleted(SessionLoopOutcome::Panicked).is_complete());
}

#[test]
fn runtime_retirement_requires_all_three_positive_receipts() {
    for ordinary in [
        ThreadShutdownOutcome::Complete,
        ThreadShutdownOutcome::SubmitFailed,
        ThreadShutdownOutcome::TimedOut,
    ] {
        for session_loop in [
            ThreadLoopOutcome::Normal,
            ThreadLoopOutcome::Cancelled,
            ThreadLoopOutcome::Panicked,
            ThreadLoopOutcome::TimedOut,
        ] {
            for cleanup in [
                ThreadCleanupOutcome::Finished {
                    persistence_failed: false,
                },
                ThreadCleanupOutcome::Finished {
                    persistence_failed: true,
                },
                ThreadCleanupOutcome::Panicked,
                ThreadCleanupOutcome::TimedOut,
                ThreadCleanupOutcome::AuthorityUnavailable,
                ThreadCleanupOutcome::McpFailed,
                ThreadCleanupOutcome::McpPrewarmFailed,
                ThreadCleanupOutcome::TaskJoinFailed,
                ThreadCleanupOutcome::ConversationShutdownFailed,
                ThreadCleanupOutcome::CodeModeShutdownFailed,
                // Unproven or timed-out sandbox cleanup never makes the
                // manager's retirement report complete.
                ThreadCleanupOutcome::SandboxCleanupUnproven { timed_out: false },
                ThreadCleanupOutcome::SandboxCleanupUnproven { timed_out: true },
            ] {
                let outcome = RuntimeOutcome::Retirement(ThreadRetirementReport {
                    ordinary,
                    session_loop,
                    cleanup,
                });
                let expected = ordinary == ThreadShutdownOutcome::Complete
                    && session_loop == ThreadLoopOutcome::Normal
                    && cleanup
                        == ThreadCleanupOutcome::Finished {
                            persistence_failed: false,
                        };
                assert_eq!(outcome.is_complete(), expected, "{outcome:?}");
            }
        }
    }
}

async fn manager() -> (tempfile::TempDir, ThreadManager, Config) {
    let home = tempfile::tempdir().unwrap();
    let mut config = ConfigBuilder::default()
        .codex_home(home.path().to_path_buf())
        .fallback_cwd(Some(home.path().to_path_buf()))
        .build()
        .await
        .unwrap();
    config.ephemeral = true;
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        codex_login::CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    (home, manager, config)
}

#[tokio::test]
async fn manager_shutdown_owns_removed_and_loaded_threads_before_first_poll() {
    let (_home, manager, config) = manager().await;
    let first = manager
        .start_thread(StartThreadOptions::new(config.clone(), None))
        .await
        .unwrap();
    let second = manager
        .start_thread(StartThreadOptions::new(config.clone(), None))
        .await
        .unwrap();
    let first_weak = Arc::downgrade(&first.thread);
    let second_weak = Arc::downgrade(&second.thread);
    drop(manager.remove_thread(&first.thread_id).await);
    drop(first);
    drop(second);
    let deadline = Instant::now() + Duration::from_secs(20);
    let observer = manager.begin_shutdown(deadline).unwrap();
    drop(observer);
    // The gate is closed by begin_shutdown itself, not by the lazy wait.
    assert!(
        manager
            .start_thread(StartThreadOptions::new(config, None))
            .await
            .is_err()
    );
    let observer = manager
        .begin_shutdown(deadline + Duration::from_secs(60))
        .unwrap();
    assert_eq!(observer.deadline(), deadline);
    let report = observer.wait().await.unwrap();
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(report.runtimes.len(), 2);
    assert!(manager.list_thread_ids().await.is_empty());
    assert!(first_weak.upgrade().is_none());
    assert!(second_weak.upgrade().is_none());
    assert_eq!(observer.wait().await.unwrap(), report);
    let state = Arc::downgrade(&manager.state);
    drop(manager);
    assert!(
        state.upgrade().is_none(),
        "positive receipt must not pin manager resources"
    );
    assert_eq!(observer.wait().await.unwrap(), report);
}

#[tokio::test]
#[expect(
    clippy::await_holding_invalid_type,
    reason = "holding the lookup lock is the discriminator proving independent runtime cleanup"
)]
async fn manager_shutdown_does_not_let_lookup_lock_starve_runtime_cleanup() {
    let (_home, manager, config) = manager().await;
    let started = manager
        .start_thread(StartThreadOptions::new(config, None))
        .await
        .unwrap();
    let lookup = manager.state.threads.write().await;
    let ticket = manager
        .begin_shutdown(Instant::now() + Duration::from_secs(20))
        .unwrap();
    let observer = ticket.clone();
    let waiting = tokio::spawn(async move { observer.wait().await });
    tokio::time::timeout(Duration::from_secs(10), async {
        while started.thread.observed_terminal_cleanup().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual runtime cleanup while lookup remains locked");
    assert!(
        !waiting.is_finished(),
        "map removal is a separate required limb"
    );
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());
    drop(lookup);
    let report = ticket.wait().await.unwrap();
    assert!(report.is_complete(), "{report:?}");
    assert!(manager.list_thread_ids().await.is_empty());
}

#[tokio::test]
async fn manager_shutdown_expired_observer_never_restarts_original_work() {
    let (_home, manager, config) = manager().await;
    let started = manager
        .start_thread(StartThreadOptions::new(config, None))
        .await
        .unwrap();
    let deadline = Instant::now();
    let ticket = manager.begin_shutdown(deadline).unwrap();
    assert_eq!(
        ticket.wait().await,
        Err(ThreadManagerRetirementError::TimedOut)
    );
    let repeated = manager
        .begin_shutdown(deadline + Duration::from_secs(60))
        .unwrap();
    assert_eq!(repeated.deadline(), deadline);
    assert_eq!(
        repeated.wait().await,
        Err(ThreadManagerRetirementError::TimedOut)
    );
    assert!(started.thread.is_running());
    assert!(started.thread.observed_terminal_cleanup().is_none());
    assert_eq!(manager.constructions.published().len(), 1);
    // The permanent manager shutdown edge bound this runtime to the original
    // (already expired) deadline. A later retirement is not a retry: it can
    // only tighten, never grant a fresh budget, so the original deadline
    // stays the effective one.
    let expired = ThreadRetirementReport {
        ordinary: ThreadShutdownOutcome::TimedOut,
        session_loop: ThreadLoopOutcome::TimedOut,
        cleanup: ThreadCleanupOutcome::TimedOut,
    };
    let later = started
        .thread
        .begin_retirement(Instant::now() + Duration::from_secs(20))
        .unwrap();
    assert_eq!(later.deadline(), deadline, "no fresh budget");
    assert_eq!(later.wait().await, expired);
    // Sticky: replaying it, or asking again with a looser deadline, keeps the
    // original deadline and the same timed-out report.
    assert_eq!(later.wait().await, expired);
    let again = started
        .thread
        .begin_retirement(Instant::now() + Duration::from_secs(60))
        .unwrap();
    assert_eq!(again.deadline(), deadline, "no fresh budget");
    assert_eq!(again.wait().await, expired);
    // Nothing ran after expiry: no Shutdown was submitted (the loop still
    // runs), the cleanup sequence never started (no receipt), and there is no
    // physical terminal proof.
    assert!(started.thread.is_running());
    assert_eq!(started.thread.session.cleanup_owner().completed(), None);
    assert!(started.thread.observed_terminal_cleanup().is_none());
    assert_eq!(manager.constructions.published().len(), 1);
    assert_eq!(
        ticket.wait().await,
        Err(ThreadManagerRetirementError::TimedOut)
    );
}

#[tokio::test]
async fn manager_shutdown_replays_prior_cleanup_without_new_ordinary_classification() {
    let (_home, manager, config) = manager().await;
    let started = manager
        .start_thread(StartThreadOptions::new(config, None))
        .await
        .unwrap();
    started.thread.shutdown_and_wait().await.unwrap();
    let report = manager
        .begin_shutdown(Instant::now() + Duration::from_secs(20))
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(
        report.runtimes,
        vec![(
            started.thread_id,
            RuntimeOutcome::PreviouslyCompleted(SessionLoopOutcome::Normal)
        )]
    );
    assert!(manager.list_thread_ids().await.is_empty());
}

#[tokio::test]
async fn manager_incomplete_report_keeps_failed_start_custody_after_future_returns() {
    let (home, manager, mut config) = manager().await;
    let server: codex_config::McpServerConfig = toml::from_str(&format!(
        "command = {}\nrequired = true\nstartup_timeout_sec = 2\n",
        toml::Value::String(
            home.path()
                .join("missing-mcp-server")
                .to_string_lossy()
                .into_owned()
        ),
    ))
    .unwrap();
    config
        .mcp_servers
        .set(std::collections::HashMap::from([(
            "required".to_owned(),
            server,
        )]))
        .unwrap();
    assert!(
        manager
            .start_thread(StartThreadOptions::new(config, None))
            .await
            .is_err()
    );
    assert!(manager.list_thread_ids().await.is_empty());
    let state = Arc::downgrade(&manager.state);
    let ticket = manager
        .begin_shutdown(Instant::now() + Duration::from_secs(20))
        .unwrap();
    let report = ticket.wait().await.unwrap();
    assert!(!report.is_complete());
    assert_eq!(report.constructors.unpublished, 1);
    assert_eq!(
        report.constructors.sessions,
        vec![crate::session::startup_custody::StartupCleanup::BeforeLoop(
            crate::session::retirement::CleanupExecution::McpFailed,
        )]
    );
    drop(manager);
    assert!(
        state.upgrade().is_some(),
        "a returned incomplete report is not custody release"
    );
    assert_eq!(ticket.wait().await.unwrap(), report);
    // Destroying the final owner is outside supported completion. This check
    // detects a cycle; it does not claim that abandonment completed cleanup.
    drop(ticket);
    assert!(state.upgrade().is_none());
}

/// Cancelling an admitted constructor before Session birth must still finish
/// it and positively settle its actual early scope without inventing Session/MCP birth.
#[tokio::test]
async fn manager_shutdown_completes_cancelled_admitted_constructor_without_a_session() {
    struct HeldInstructions {
        held: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
        entered: Arc<tokio::sync::Notify>,
    }
    impl codex_extension_api::UserInstructionsProvider for HeldInstructions {
        fn load_user_instructions(&self) -> codex_extension_api::LoadInstructionsFuture<'_> {
            let held = self.held.lock().unwrap().take().unwrap();
            self.entered.notify_one();
            Box::pin(async move {
                held.await.unwrap();
                codex_extension_api::LoadedUserInstructions::default()
            })
        }
    }
    let (_home, mut manager, config) = manager().await;
    let (release, held) = tokio::sync::oneshot::channel();
    let entered = Arc::new(tokio::sync::Notify::new());
    Arc::get_mut(&mut manager.state)
        .unwrap()
        .user_instructions_provider = Arc::new(HeldInstructions {
        held: Mutex::new(Some(held)),
        entered: Arc::clone(&entered),
    });
    let mut constructor = Box::pin(manager.start_thread(StartThreadOptions::new(config, None)));
    tokio::time::timeout(Duration::from_secs(20), async {
        tokio::select! {
            _ = entered.notified() => {},
            _ = constructor.as_mut() => panic!("constructor escaped held instructions"),
        }
    })
    .await
    .expect("real constructor reaches held instruction refresh");
    let startup = Arc::clone(&manager.constructions.state.lock().unwrap().constructions[0].startup);
    assert_eq!(
        startup.constructor_stage_for_test(),
        Some((false, false, false)),
        "early resource scope exists, but Session/MCP runtime have not been born"
    );
    let deadline = Instant::now() + Duration::from_secs(20);
    let ticket = manager.begin_shutdown(deadline).unwrap();
    let mut observer = Box::pin(ticket.wait());
    assert!(futures::poll!(observer.as_mut()).is_pending());
    // Both caller cancellations leave the manager's admitted construction and
    // original aggregate intact. Semantic correction (startup decision #1):
    // the constructor's departing observer now requests cancellation even
    // though admission is closed, so the drain delivers the start to its
    // cancellation arm before the released instructions are consumed and no
    // Session is born. The actual early scope still requires its positive receipt.
    drop(observer);
    drop(constructor);
    release.send(()).unwrap();
    let report = ticket.wait().await.unwrap();
    assert!(report.is_complete(), "{report:?}");
    assert!(report.constructors.finished);
    assert_eq!(report.constructors.unpublished, 0);
    assert_eq!(
        report.constructors.sessions,
        vec![crate::session::startup_custody::StartupCleanup::BeforeLoop(
            crate::session::retirement::CleanupExecution::Finished {
                persistence_failed: false
            }
        )],
        "cancelled before Session birth: the actual early scope was positively cleaned"
    );
    assert_eq!(
        startup.constructor_stage_for_test(),
        Some((false, false, true))
    );
    assert!(startup.is_empty());
    assert!(
        report.runtimes.is_empty(),
        "post-close publication is forbidden"
    );
    assert!(manager.list_thread_ids().await.is_empty());
    assert_eq!(ticket.deadline(), deadline);
    assert_eq!(ticket.wait().await.unwrap(), report);
}

#[tokio::test]
async fn manager_shutdown_observes_both_exact_runtimes_when_thread_id_is_reused() {
    let (_home, manager, config) = manager().await;
    let id = ThreadId::new();
    let manager = manager.with_thread_id_generator(move || id);
    let old = manager
        .start_thread(StartThreadOptions::new(config.clone(), None))
        .await
        .unwrap();
    drop(manager.remove_thread(&id).await);
    let replacement = manager
        .start_thread(StartThreadOptions::new(config, None))
        .await
        .unwrap();
    assert_eq!(old.thread_id, replacement.thread_id);
    assert!(!Arc::ptr_eq(&old.thread, &replacement.thread));
    assert!(
        manager
            .remove_thread_if_same(&id, &old.thread)
            .await
            .is_none()
    );
    assert!(Arc::ptr_eq(
        &manager.get_thread(id).await.unwrap(),
        &replacement.thread
    ));
    let deadline = Instant::now() + Duration::from_secs(20);
    let old_ticket = old.thread.begin_retirement(deadline).unwrap();
    let report = manager
        .begin_shutdown(deadline)
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(report.runtimes.len(), 2);
    assert!(
        report
            .runtimes
            .iter()
            .all(|(thread_id, outcome)| *thread_id == id && outcome.is_complete())
    );
    assert_eq!(
        old_ticket.wait().await.cleanup,
        ThreadCleanupOutcome::Finished {
            persistence_failed: false
        }
    );
    assert!(old.thread.observed_terminal_cleanup().is_some());
    assert!(replacement.thread.observed_terminal_cleanup().is_some());
    assert!(manager.list_thread_ids().await.is_empty());
}
