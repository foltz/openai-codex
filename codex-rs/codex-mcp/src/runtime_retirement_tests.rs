use super::*;
use pretty_assertions::assert_eq;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::Semaphore;

#[tokio::test]
async fn external_control_retains_original_task_and_requires_positive_proof() {
    let control = McpRuntimeRetirement::default();
    let registry = control.registry.clone();
    let resource = Arc::new(());
    let weak = Arc::downgrade(&resource);
    let task = registry
        .task_ticket()
        .register(move || async move {
            let _resource = resource;
            RuntimeTaskOutcome::Failed
        })
        .unwrap();
    assert_eq!(task.await, RuntimeTaskOutcome::Failed);
    drop(registry);
    assert!(weak.upgrade().is_none());
    let report = control
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert!(!report.is_complete());
    assert!(!control.is_retired());
    assert_eq!(report.tasks, vec![(0, RuntimeTaskOutcome::Failed)]);
    assert_eq!(control.shutdown_until(Instant::now()).await, report);
}

struct PausedConnectionFactory {
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
    stream: Arc<Mutex<Option<tokio::io::DuplexStream>>>,
}

impl codex_rmcp_client::InProcessTransportFactory for PausedConnectionFactory {
    fn open(&self) -> BoxFuture<'static, std::io::Result<tokio::io::DuplexStream>> {
        let entered = Arc::clone(&self.entered);
        let release = Arc::clone(&self.release);
        let stream = Arc::clone(&self.stream);
        async move {
            entered.add_permits(1);
            release
                .acquire()
                .await
                .expect("release admitted creation")
                .forget();
            stream
                .lock()
                .expect("stream owner")
                .take()
                .ok_or_else(|| std::io::Error::other("already consumed"))
        }
        .boxed()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admitted_physical_creation_finishing_after_upper_freeze_is_actually_closed() {
    use tokio::io::AsyncReadExt;
    let registry = RuntimeRetirementRegistry::default();
    let owner = registry
        .register_connection(CancellationToken::new())
        .expect("reserve upper owner");
    let (client, mut peer) = tokio::io::duplex(64);
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let factory = Arc::new(PausedConnectionFactory {
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
        stream: Arc::new(Mutex::new(Some(client))),
    });
    // The upper task is admitted and its lower physical creation has begun,
    // but no pending transport can yet be returned to the constructor.
    let lower = owner.lower();
    let task = owner
        .task_ticket()
        .register(move || async move {
            match codex_rmcp_client::RmcpClient::new_in_process_client_in_retirement(factory, lower)
                .await
            {
                Ok(client) => {
                    drop(client);
                    RuntimeTaskOutcome::Complete
                }
                Err(_) => RuntimeTaskOutcome::Failed,
            }
        })
        .expect("register startup");
    let caller = tokio::spawn(task);
    entered
        .acquire()
        .await
        .expect("real factory entered")
        .forget();
    let deadline = Instant::now() + Duration::from_secs(3);
    registry.close_registration();
    let retiring = registry.clone();
    let shutdown = tokio::spawn(async move { retiring.shutdown_until(deadline).await });
    release.add_permits(1);
    let report = shutdown.await.expect("retirement joined");
    assert_eq!(
        caller.await.expect("startup joined"),
        RuntimeTaskOutcome::Complete
    );
    assert!(
        report.is_complete(),
        "late attachment must be retired, not omitted: {report:?}"
    );
    assert_eq!(report.connections.len(), 1);
    assert_eq!(
        report.connections[0].1.attempts.len(),
        1,
        "census includes physical creation before freeze"
    );
    assert_eq!(
        tokio::time::timeout_at(deadline, peer.read(&mut [0]))
            .await
            .expect("EOF under original deadline")
            .expect("read peer"),
        0
    );
}

#[tokio::test]
async fn dormant_tasks_are_skipped_and_connection_identity_is_stable() {
    let registry = RuntimeRetirementRegistry::default();
    let cancel = CancellationToken::new();
    let owner = registry
        .register_connection(cancel.clone())
        .expect("reserve");
    let reused = Arc::clone(&owner);
    assert_eq!(owner.id(), reused.id());
    let count = Arc::new(AtomicUsize::new(0));
    let launched = Arc::clone(&count);
    let task = owner
        .task_ticket()
        .register(move || async move {
            launched.fetch_add(1, Ordering::SeqCst);
            RuntimeTaskOutcome::Complete
        })
        .expect("register");
    let report = registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert!(report.is_complete());
    assert_eq!(report.connections.len(), 1);
    assert_eq!(task.await, RuntimeTaskOutcome::Skipped);
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert!(cancel.is_cancelled());
    assert!(
        registry
            .register_connection(CancellationToken::new())
            .is_err()
    );
}

#[tokio::test(start_paused = true)]
async fn expired_budget_does_not_resume_admitted_task_and_terminal_report_is_replayable() {
    let registry = RuntimeRetirementRegistry::default();
    let gate = Arc::new(Semaphore::new(0));
    let resumed = Arc::clone(&gate);
    let count = Arc::new(AtomicUsize::new(0));
    let completed = Arc::clone(&count);
    let task = registry
        .task_ticket()
        .register(move || async move {
            resumed.acquire().await.expect("resume").forget();
            completed.fetch_add(1, Ordering::SeqCst);
            RuntimeTaskOutcome::Complete
        })
        .expect("register task");
    assert!(task.now_or_never().is_none());
    gate.add_permits(1);
    let expired = Instant::now();
    let report = registry.shutdown_until(expired).await;
    assert_eq!(report.tasks, vec![(0, RuntimeTaskOutcome::TimedOut)]);
    assert_eq!(count.load(Ordering::SeqCst), 0);
    let report = registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert!(report.is_complete());
    assert_eq!(registry.shutdown_until(expired).await, report);
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn timed_out_task_is_owned_and_can_be_drained_by_later_observer() {
    let registry = RuntimeRetirementRegistry::default();
    let gate = Arc::new(Semaphore::new(0));
    let resumed = Arc::clone(&gate);
    let task = registry
        .task_ticket()
        .register(move || async move {
            resumed.acquire().await.expect("resume").forget();
            RuntimeTaskOutcome::Complete
        })
        .expect("register summary");
    assert!(task.now_or_never().is_none());
    let report = registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(report.tasks, vec![(0, RuntimeTaskOutcome::TimedOut)]);
    gate.add_permits(1);
    let complete = registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(complete.tasks, vec![(0, RuntimeTaskOutcome::Complete)]);
    assert!(complete.is_complete());
}

#[tokio::test(start_paused = true)]
async fn all_lower_gates_close_before_cancelled_task_is_polled() {
    let registry = RuntimeRetirementRegistry::default();
    let cancellation = CancellationToken::new();
    let owner = registry
        .register_connection(cancellation.clone())
        .expect("reserve");
    let lower = owner.lower();
    let task = owner
        .task_ticket()
        .register(move || async move {
            cancellation.cancelled().await;
            // A public lower retirement observation must see a closed, empty census.
            assert!(
                lower
                    .shutdown_until(Instant::now() + Duration::from_secs(1))
                    .await
                    .is_complete()
            );
            RuntimeTaskOutcome::Complete
        })
        .expect("register");
    assert!(task.now_or_never().is_none());
    assert!(
        registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .is_complete()
    );
}

#[tokio::test]
async fn successful_compaction_drops_runtime_owners_and_retains_result() {
    let registry = RuntimeRetirementRegistry::default();
    let owner = registry
        .register_connection(CancellationToken::new())
        .expect("reserve");
    let weak = Arc::downgrade(&owner);
    drop(owner);
    let report = registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert!(report.is_complete());
    assert!(weak.upgrade().is_none());
    assert_eq!(registry.shutdown_until(Instant::now()).await, report);
}

#[tokio::test]
async fn stored_future_has_no_strong_registry_cycle() {
    let registry = RuntimeRetirementRegistry::default();
    let probe = Arc::new(());
    let capture = Arc::clone(&probe);
    let task = registry
        .task_ticket()
        .register(move || async move {
            let _capture = capture;
            std::future::pending().await
        })
        .expect("register");
    assert!(task.now_or_never().is_none());
    assert_eq!(Arc::strong_count(&probe), 2);
    drop(registry);
    assert_eq!(Arc::strong_count(&probe), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_first_poll_and_gate_close_record_exact_admission() {
    let registry = RuntimeRetirementRegistry::default();
    let invoked = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&invoked);
    let task = registry
        .task_ticket()
        .register(move || {
            count.fetch_add(1, Ordering::SeqCst);
            async { RuntimeTaskOutcome::Complete }
        })
        .expect("reserve");
    let admission = Arc::clone(&registry.state.lock().expect("registry").tasks[0].admission);
    let gate = Arc::new(tokio::sync::Barrier::new(2));
    let started = Arc::clone(&gate);
    let caller = tokio::spawn(async move {
        started.wait().await;
        task.await
    });
    gate.wait().await;
    registry.close_registration();
    let result = caller.await.expect("caller joined");
    let recorded = *admission.lock().expect("admission");
    match recorded {
        TaskAdmission::Admitted => {
            assert_eq!(result, RuntimeTaskOutcome::Complete);
            assert_eq!(invoked.load(Ordering::SeqCst), 1);
        }
        TaskAdmission::Skipped => {
            assert_eq!(result, RuntimeTaskOutcome::Skipped);
            assert_eq!(invoked.load(Ordering::SeqCst), 0);
        }
        TaskAdmission::Dormant => panic!("close must classify every reserved task"),
    }
    assert!(
        registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .is_complete()
    );
}

#[tokio::test]
async fn poisoned_registry_refuses_registration_and_success_but_cancels_owners() {
    let registry = RuntimeRetirementRegistry::default();
    let cancellation = CancellationToken::new();
    let _owner = registry
        .register_connection(cancellation.clone())
        .expect("reserve");
    let state = Arc::clone(&registry.state);
    assert!(
        std::thread::spawn(move || {
            let _guard = state.lock().expect("registry");
            panic!("injected interrupted mutation");
        })
        .join()
        .is_err()
    );
    assert!(
        registry
            .register_connection(CancellationToken::new())
            .is_err()
    );
    assert!(
        registry
            .task_ticket()
            .register(|| async { RuntimeTaskOutcome::Complete })
            .is_err()
    );
    let report = registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert!(!report.is_complete());
    assert_eq!(report.tasks, vec![(0, RuntimeTaskOutcome::Failed)]);
    assert!(cancellation.is_cancelled());
}
