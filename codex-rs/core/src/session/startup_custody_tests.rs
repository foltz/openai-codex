use super::*;
use crate::codex_thread::ThreadCleanupOutcome;
use crate::codex_thread::ThreadLoopOutcome;
use crate::codex_thread::ThreadShutdownOutcome;

#[tokio::test]
async fn cancelled_acquisition_remains_owned_until_its_writer_is_disposed() {
    let (mut session, _) = crate::session::tests::make_session_and_context().await;
    crate::session::tests::open_thread_persistence(&mut session).await;
    let live_thread = session.live_thread().expect("writer").clone();
    let release = Arc::new(tokio::sync::Notify::new());
    let guard = Arc::new(tokio::sync::Mutex::new(LiveThreadInitGuard::default()));
    let custody = SessionStartupCustody::default();
    custody.retain_persistence_guard(session.thread_id, Arc::clone(&guard));
    let mut acquisition = Box::pin(async {
        let release = Arc::clone(&release);
        guard
            .lock()
            .await
            .acquire(async move {
                release.notified().await;
                Ok(live_thread)
            })
            .await
    });
    assert!(futures::poll!(acquisition.as_mut()).is_pending());
    drop(acquisition);
    assert!(!custody.is_empty(), "pending acquisition is still custody");

    let mut cleanup =
        Box::pin(custody.shutdown_until(Instant::now() + std::time::Duration::from_secs(3)));
    assert!(futures::poll!(cleanup.as_mut()).is_pending());
    release.notify_one();
    assert_eq!(
        cleanup.await,
        StartupCleanup::BeforeLoop(CleanupExecution::Finished {
            persistence_failed: false,
        }),
    );
    assert!(custody.is_empty());
    assert!(guard.lock().await.as_ref().is_none());
    assert!(
        session
            .live_thread()
            .expect("same writer")
            .discard()
            .await
            .is_err(),
        "the retained acquisition's writer was actually disposed",
    );
}

#[test]
fn loop_cleanup_requires_the_canonical_complete_report() {
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
                ThreadCleanupOutcome::McpPrewarmFailed,
                ThreadCleanupOutcome::TaskJoinFailed,
            ] {
                let observed = StartupCleanup::Loop(ThreadRetirementReport {
                    ordinary,
                    session_loop,
                    cleanup,
                });
                let expected = ordinary == ThreadShutdownOutcome::Complete
                    && session_loop == ThreadLoopOutcome::Normal
                    && cleanup
                        == (ThreadCleanupOutcome::Finished {
                            persistence_failed: false,
                        });
                assert_eq!(observed.is_complete(), expected);
            }
        }
    }
}

#[tokio::test]
async fn pre_session_custody_distinguishes_absent_disposed_and_failed_resources() {
    let absent = SessionStartupCustody::default();
    assert_eq!(
        absent
            .shutdown_until(Instant::now() + std::time::Duration::from_secs(3))
            .await,
        StartupCleanup::NoUnpublishedSession
    );

    let (mut session, _) = crate::session::tests::make_session_and_context().await;
    crate::session::tests::open_thread_persistence(&mut session).await;
    let live_thread = session.live_thread().expect("live thread").clone();
    let disposed = SessionStartupCustody::default();
    disposed.retain_persistence(session.thread_id, live_thread.clone());
    assert_eq!(
        disposed
            .shutdown_until(Instant::now() + std::time::Duration::from_secs(3))
            .await,
        StartupCleanup::BeforeLoop(CleanupExecution::Finished {
            persistence_failed: false,
        })
    );
    assert!(disposed.is_empty());

    let failed = SessionStartupCustody::default();
    failed.retain_persistence(session.thread_id, live_thread);
    let first = failed
        .shutdown_until(Instant::now() + std::time::Duration::from_secs(3))
        .await;
    assert_eq!(
        first,
        StartupCleanup::BeforeLoop(CleanupExecution::Finished {
            persistence_failed: true,
        })
    );
    assert_eq!(
        failed.shutdown_until(Instant::now()).await,
        first,
        "cached failed disposal must replay before the elapsed-deadline check"
    );
    assert!(!failed.is_empty());
}

#[tokio::test]
async fn pre_session_transfer_is_atomic_and_poison_is_fail_closed() {
    let (mut session, _) = crate::session::tests::make_session_and_context().await;
    crate::session::tests::open_thread_persistence(&mut session).await;
    let session = Arc::new(session);
    let live_thread = session.live_thread().expect("live thread").clone();
    let transfer = SessionStartupCustody::default();
    transfer.retain_persistence(session.thread_id, live_thread.clone());
    assert!(transfer.retain(&session));
    let state = transfer.state.lock().expect("startup state");
    assert!(state.persistence.is_none());
    assert!(state.session.is_some());
    drop(state);

    let poisoned = SessionStartupCustody::default();
    poisoned.retain_persistence(session.thread_id, live_thread);
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _state = poisoned.state.lock().expect("startup state");
        panic!("poison startup custody");
    }));
    assert!(!poisoned.is_empty());
    assert_eq!(
        poisoned
            .shutdown_until(Instant::now() + std::time::Duration::from_secs(3))
            .await,
        StartupCleanup::Refused(ThreadRetirementError::AuthorityUnavailable)
    );
    let poisoned_state = match poisoned.state.lock() {
        Ok(_) => panic!("poison must remain fail closed"),
        Err(error) => error.into_inner(),
    };
    assert!(poisoned_state.persistence.is_some());
}

#[tokio::test]
async fn concurrent_disposal_observers_replay_the_same_success() {
    let (mut session, _) = crate::session::tests::make_session_and_context().await;
    crate::session::tests::open_thread_persistence(&mut session).await;
    let live_thread = session.live_thread().expect("live thread").clone();
    let custody = Arc::new(SessionStartupCustody::default());
    custody.retain_persistence(session.thread_id, live_thread);
    let release = Arc::new(tokio::sync::Notify::new());
    {
        let mut state = custody.state.lock().expect("startup state");
        let release = Arc::clone(&release);
        let guard = state
            .persistence
            .as_ref()
            .expect("retained persistence")
            .guard
            .clone();
        state
            .persistence
            .as_mut()
            .expect("retained persistence")
            .disposal = Some(PersistenceDisposal {
            receipt: async move {
                release.notified().await;
                guard.lock().await.discard_with_result().await.is_ok()
            }
            .boxed()
            .shared(),
            phase: PersistenceDisposalPhase::DeadlineBound,
        });
    }
    let deadline = Instant::now() + std::time::Duration::from_secs(3);
    let mut first = Box::pin(custody.shutdown_until(deadline));
    let mut second = Box::pin(custody.shutdown_until(deadline));
    assert!(futures::poll!(first.as_mut()).is_pending());
    assert!(futures::poll!(second.as_mut()).is_pending());
    release.notify_one();

    let expected = StartupCleanup::BeforeLoop(CleanupExecution::Finished {
        persistence_failed: false,
    });
    let (first, second) = tokio::join!(first, second);
    assert_eq!(first, expected);
    assert_eq!(second, expected);
    assert!(custody.is_empty());
}

#[tokio::test]
async fn expired_bounded_observation_cannot_start_disposal_through_legacy() {
    let (mut session, _) = crate::session::tests::make_session_and_context().await;
    crate::session::tests::open_thread_persistence(&mut session).await;
    let custody = SessionStartupCustody::default();
    custody.retain_persistence(
        session.thread_id,
        session.live_thread().expect("live thread").clone(),
    );

    assert_eq!(
        custody.shutdown_until(Instant::now()).await,
        StartupCleanup::TimedOut
    );
    assert!(
        custody
            .state
            .lock()
            .expect("startup state")
            .persistence
            .as_ref()
            .expect("retained persistence")
            .disposal
            .is_none()
    );
    assert!(!custody.shutdown_legacy().await);
    assert!(
        custody
            .state
            .lock()
            .expect("startup state")
            .persistence
            .as_ref()
            .expect("retained persistence")
            .disposal
            .is_none(),
        "legacy observation must not start work after the first bound expires"
    );
    session
        .live_thread()
        .expect("live thread")
        .discard()
        .await
        .expect("test-owned persistence cleanup");
}

#[tokio::test]
async fn legacy_disposal_remains_legacy_after_bounded_refusal() {
    let (mut session, _) = crate::session::tests::make_session_and_context().await;
    crate::session::tests::open_thread_persistence(&mut session).await;
    let custody = SessionStartupCustody::default();
    custody.retain_persistence(
        session.thread_id,
        session.live_thread().expect("live thread").clone(),
    );
    let release = Arc::new(tokio::sync::Notify::new());
    {
        let mut state = custody.state.lock().expect("startup state");
        let persistence = state.persistence.as_mut().expect("retained persistence");
        let guard = Arc::clone(&persistence.guard);
        let release = Arc::clone(&release);
        persistence.disposal = Some(PersistenceDisposal {
            receipt: async move {
                release.notified().await;
                guard.lock().await.discard_with_result().await.is_ok()
            }
            .boxed()
            .shared(),
            phase: PersistenceDisposalPhase::Legacy,
        });
    }
    let mut legacy = Box::pin(custody.shutdown_legacy());
    assert!(futures::poll!(legacy.as_mut()).is_pending());
    assert_eq!(
        custody
            .shutdown_until(Instant::now() + std::time::Duration::from_secs(3))
            .await,
        StartupCleanup::Refused(ThreadRetirementError::LegacyCleanupStarted)
    );
    let mut replay = Box::pin(custody.shutdown_legacy());
    assert!(
        futures::poll!(replay.as_mut()).is_pending(),
        "a later legacy observer must retain the original legacy phase"
    );

    release.notify_one();
    let (legacy, replay) = tokio::join!(legacy, replay);
    assert!(legacy, "the original legacy observer remains its driver");
    assert!(replay, "later legacy observers replay the original driver");
    assert!(custody.is_empty());
}
