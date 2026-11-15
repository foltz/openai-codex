use super::*;
use crate::McpAttemptRefused;
use pretty_assertions::assert_eq;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use tokio::sync::oneshot;

struct Work {
    live: Arc<AtomicUsize>,
    refused: Arc<AtomicBool>,
}

impl McpAttemptWork for Work {
    fn derive_attempt(&self) -> Result<Box<dyn McpAttemptWork>, McpAttemptRefused> {
        if self.refused.load(Ordering::SeqCst) {
            return Err(McpAttemptRefused);
        }
        self.live.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(Self {
            live: Arc::clone(&self.live),
            refused: Arc::clone(&self.refused),
        }))
    }
}

impl Drop for Work {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn refused_start_stays_retryable_and_one_attempt_keeps_work_across_observers() {
    let live = Arc::new(AtomicUsize::new(1));
    let refused = Arc::new(AtomicBool::new(true));
    let work = Work {
        live: Arc::clone(&live),
        refused: Arc::clone(&refused),
    };
    let effects = Arc::new(AtomicUsize::new(0));
    let effects_for_start = Arc::clone(&effects);
    let (finish, finished) = oneshot::channel();
    let startup = ClientStartup::new(
        async move {
            effects_for_start.fetch_add(1, Ordering::SeqCst);
            finished.await.expect("finish attempt");
            Err(StartupOutcomeError::Cancelled)
        }
        .boxed()
        .shared(),
        McpAttemptRequirement::Required,
    );

    for access in [
        McpAttemptAccess::Unscoped,
        McpAttemptAccess::Admitted(&work),
    ] {
        assert!(matches!(
            startup.observe(access).await,
            Err(StartupOutcomeError::Refused(_))
        ));
        assert!(startup.peek().is_none());
    }
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    refused.store(false, Ordering::SeqCst);
    let mut first = Box::pin(startup.observe(McpAttemptAccess::Admitted(&work)));
    assert!(futures::poll!(&mut first).is_pending());
    assert_eq!(live.load(Ordering::SeqCst), 2);
    drop(first);
    // Admission is now exhausted, but observers join the already-counted
    // attempt without deriving another lease or poisoning its shared result.
    refused.store(true, Ordering::SeqCst);
    let mut second = Box::pin(startup.observe(McpAttemptAccess::Admitted(&work)));
    assert!(futures::poll!(&mut second).is_pending());
    startup.cancel_unstarted();
    assert_eq!(live.load(Ordering::SeqCst), 2);
    finish.send(()).expect("release attempt");
    assert!(matches!(second.await, Err(StartupOutcomeError::Cancelled)));
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(live.load(Ordering::SeqCst), 1);
    assert!(startup.peek().is_some());
    assert!(matches!(
        startup.observe(McpAttemptAccess::Unscoped).await,
        Err(StartupOutcomeError::Cancelled)
    ));
    assert_eq!(live.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancellation_before_first_poll_releases_staged_work_without_transport_effects() {
    let live = Arc::new(AtomicUsize::new(1));
    let work = Work {
        live: Arc::clone(&live),
        refused: Arc::new(AtomicBool::new(false)),
    };
    let effects = Arc::new(AtomicUsize::new(0));
    let effects_for_start = Arc::clone(&effects);
    let startup = ClientStartup::new(
        async move {
            effects_for_start.fetch_add(1, Ordering::SeqCst);
            Err(StartupOutcomeError::Cancelled)
        }
        .boxed()
        .shared(),
        McpAttemptRequirement::Required,
    );
    startup
        .admit(McpAttemptAccess::Admitted(&work))
        .expect("stage work before activation");
    assert_eq!(live.load(Ordering::SeqCst), 2);
    startup.cancel_unstarted();
    assert_eq!(live.load(Ordering::SeqCst), 1);
    assert!(matches!(
        startup.observe(McpAttemptAccess::Unscoped).await,
        Err(StartupOutcomeError::Cancelled)
    ));
    assert_eq!(effects.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn reconnect_requires_per_trigger_work_and_does_not_keep_it_in_the_factory() {
    let live = Arc::new(AtomicUsize::new(1));
    let work = Work {
        live: Arc::clone(&live),
        refused: Arc::new(AtomicBool::new(false)),
    };
    let (started, mut start) = oneshot::channel();
    let (finish, finished) = oneshot::channel();
    let attempt = async move {
        started.send(()).expect("signal reconnect");
        finished.await.expect("finish reconnect");
        Err(StartupOutcomeError::Cancelled)
    }
    .boxed()
    .shared();
    let reconnect = Arc::new(super::super::CodexAppsStartupReconnect::with_requirement(
        Arc::new(move || attempt.clone()),
        crate::McpWorkerOwner::default(),
        McpAttemptRequirement::Required,
    ));
    reconnect.reconnect_with_authority(McpAttemptAccess::Unscoped);
    assert!(matches!(
        start.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(
        reconnect
            .state
            .lock()
            .expect("reconnect state")
            .reconnect_in_flight
            .is_none()
    );
    reconnect.reconnect_with_authority(McpAttemptAccess::Admitted(&work));
    start.await.expect("attempt admitted");
    assert_eq!(live.load(Ordering::SeqCst), 2);
    finish.send(()).expect("release reconnect");
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while reconnect
            .state
            .lock()
            .expect("reconnect state")
            .reconnect_in_flight
            .is_some()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reconnect completed");
    assert_eq!(live.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn result_mapping_keeps_one_original_admission_and_releases_attempt_work() {
    let live = Arc::new(AtomicUsize::new(1));
    let work = Work {
        live: Arc::clone(&live),
        refused: Arc::new(AtomicBool::new(false)),
    };
    let mapped = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&mapped);
    let (send, receive) = oneshot::channel();
    let startup = ClientStartup::new(
        async move {
            receive.await.unwrap();
            Err(StartupOutcomeError::Cancelled)
        }
        .boxed()
        .shared(),
        McpAttemptRequirement::Required,
    )
    .map_completion(move |original| {
        async move {
            let result = original.await;
            counted.fetch_add(1, Ordering::SeqCst);
            result
        }
        .boxed()
        .shared()
    });
    assert!(matches!(
        startup.observe(McpAttemptAccess::Unscoped).await,
        Err(StartupOutcomeError::Refused(_))
    ));
    assert_eq!(mapped.load(Ordering::SeqCst), 0);
    let mut first = Box::pin(startup.observe(McpAttemptAccess::Admitted(&work)));
    assert!(futures::poll!(first.as_mut()).is_pending());
    assert_eq!(live.load(Ordering::SeqCst), 2);
    drop(first);
    send.send(()).unwrap();
    assert!(matches!(
        startup.observe(McpAttemptAccess::Unscoped).await,
        Err(StartupOutcomeError::Cancelled)
    ));
    assert_eq!(mapped.load(Ordering::SeqCst), 1);
    assert_eq!(live.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn poisoned_admission_never_constructs_or_polls_the_effectful_factory() {
    let effects = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&effects);
    let startup = ClientStartup::new_with_work(
        move |_| {
            count.fetch_add(1, Ordering::SeqCst);
            futures::future::ready(Err(StartupOutcomeError::Cancelled))
                .boxed()
                .shared()
        },
        McpAttemptRequirement::Ungated,
    );
    let admission = Arc::clone(&startup.admission);
    assert!(
        std::thread::spawn(move || {
            let _held = admission.lock().unwrap();
            panic!("poison witness");
        })
        .join()
        .is_err()
    );
    assert!(matches!(
        startup.observe(McpAttemptAccess::Unscoped).await,
        Err(StartupOutcomeError::Failed { .. })
    ));
    assert!(startup.peek().is_none());
    assert_eq!(effects.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn physical_attempt_refuses_before_factory_construction_and_releases_on_completion() {
    let live = Arc::new(AtomicUsize::new(1));
    let refused = Arc::new(AtomicBool::new(true));
    let work = Work {
        live: Arc::clone(&live),
        refused: Arc::clone(&refused),
    };
    let effects = Arc::new(AtomicUsize::new(0));
    for access in [
        McpAttemptAccess::Unscoped,
        McpAttemptAccess::Admitted(&work),
    ] {
        let count = Arc::clone(&effects);
        assert!(matches!(
            physical_attempt(McpAttemptRequirement::Required, access, move || {
                count.fetch_add(1, Ordering::SeqCst);
                futures::future::ready(Err(StartupOutcomeError::Cancelled))
                    .boxed()
                    .shared()
            })
            .await,
            Err(StartupOutcomeError::Refused(_))
        ));
    }
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert_eq!(live.load(Ordering::SeqCst), 1);
    refused.store(false, Ordering::SeqCst);
    let original = physical_attempt(
        McpAttemptRequirement::Required,
        McpAttemptAccess::Admitted(&work),
        || {
            effects.fetch_add(1, Ordering::SeqCst);
            futures::future::ready(Err(StartupOutcomeError::Cancelled))
                .boxed()
                .shared()
        },
    );
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(live.load(Ordering::SeqCst), 2);
    assert!(matches!(
        original.await,
        Err(StartupOutcomeError::Cancelled)
    ));
    assert_eq!(live.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn private_original_keeps_account_work_after_every_client_observer_is_dropped() {
    let live = Arc::new(AtomicUsize::new(1));
    let work = Work {
        live: Arc::clone(&live),
        refused: Arc::new(AtomicBool::new(false)),
    };
    let owner = crate::rmcp_client::PrivateClientRetirement::default();
    let retained_owner = owner.clone();
    let (finish, receive) = oneshot::channel();
    let startup = ClientStartup::new_with_work(
        move |work| {
            retained_owner.track_startup(tokio_util::sync::CancellationToken::new(), move |_| {
                async move {
                    let _work = work;
                    receive.await.unwrap();
                    Err(StartupOutcomeError::Cancelled)
                }
                .boxed()
                .shared()
            })
        },
        McpAttemptRequirement::Required,
    );
    let mut observer = Box::pin(startup.observe(McpAttemptAccess::Admitted(&work)));
    assert!(futures::poll!(observer.as_mut()).is_pending());
    assert_eq!(live.load(Ordering::SeqCst), 2);
    drop(observer);
    drop(startup);
    assert_eq!(
        live.load(Ordering::SeqCst),
        2,
        "actual private registry retains the admitted original"
    );
    finish.send(()).unwrap();
    let report = owner
        .shutdown_until(tokio::time::Instant::now() + std::time::Duration::from_secs(3))
        .await;
    assert!(
        report
            .tasks
            .iter()
            .any(|(_, task)| *task == crate::PrivateTaskOutcome::Complete)
    );
    assert_eq!(live.load(Ordering::SeqCst), 1);
    // No transport was created by this fixture; task/lease proof is not physical retirement proof.
}

#[tokio::test]
async fn missing_required_continuation_custody_refuses_before_factory_effects() {
    let calls = AtomicUsize::new(0);
    assert!(matches!(
        physical_continuation(McpAttemptRequirement::Required, None, |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            futures::future::ready(Err(StartupOutcomeError::Cancelled))
                .boxed()
                .shared()
        })
        .await,
        Err(StartupOutcomeError::Refused(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
