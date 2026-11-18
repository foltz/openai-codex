use super::process::OutputDrainPolicy;
use super::process::UnifiedExecProcess;
use crate::unified_exec::UnifiedExecError;
use codex_exec_server::ExecProcess;
use codex_exec_server::ExecProcessEvent;
use codex_exec_server::ExecProcessEventReceiver;
use codex_exec_server::ExecProcessFuture;
use codex_exec_server::ExecServerError;
use codex_exec_server::ProcessId;
use codex_exec_server::ProcessSignal;
use codex_exec_server::ReadResponse;
use codex_exec_server::StartedExecProcess;
use codex_exec_server::WriteResponse;
use codex_exec_server::WriteStatus;
use pretty_assertions::assert_eq;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::watch;

struct MockExecProcess {
    process_id: ProcessId,
    write_response: WriteResponse,
    read_responses: Mutex<VecDeque<ReadResponse>>,
    terminate_error: Option<String>,
    wake_tx: watch::Sender<u64>,
    events: Vec<ExecProcessEvent>,
    deadlines: Arc<std::sync::Mutex<Vec<std::time::Instant>>>,
}

impl MockExecProcess {
    async fn read(&self) -> Result<ReadResponse, ExecServerError> {
        Ok(self
            .read_responses
            .lock()
            .await
            .pop_front()
            .unwrap_or(ReadResponse {
                sandbox_cleanup: None,
                chunks: Vec::new(),
                next_seq: 1,
                exited: false,
                exit_code: None,
                closed: false,
                failure: None,
                sandbox_denied: false,
            }))
    }

    async fn terminate(&self) -> Result<(), ExecServerError> {
        if let Some(message) = &self.terminate_error {
            return Err(ExecServerError::Protocol(message.clone()));
        }
        Ok(())
    }
}

impl ExecProcess for MockExecProcess {
    fn process_id(&self) -> &ProcessId {
        &self.process_id
    }

    fn subscribe_wake(&self) -> watch::Receiver<u64> {
        self.wake_tx.subscribe()
    }

    fn subscribe_events(&self) -> ExecProcessEventReceiver {
        ExecProcessEventReceiver::from_replay_for_tests(self.events.clone())
    }

    fn read(
        &self,
        _after_seq: Option<u64>,
        _max_bytes: Option<usize>,
        _wait_ms: Option<u64>,
    ) -> ExecProcessFuture<'_, ReadResponse> {
        Box::pin(MockExecProcess::read(self))
    }

    fn write(&self, _chunk: Vec<u8>) -> ExecProcessFuture<'_, WriteResponse> {
        Box::pin(async { Ok(self.write_response.clone()) })
    }

    fn signal(&self, _signal: ProcessSignal) -> ExecProcessFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn terminate(&self) -> ExecProcessFuture<'_, ()> {
        Box::pin(MockExecProcess::terminate(self))
    }

    fn terminate_with_cleanup_deadline(
        &self,
        deadline: std::time::Instant,
    ) -> ExecProcessFuture<'_, ()> {
        self.deadlines.lock().expect("deadlines").push(deadline);
        Box::pin(MockExecProcess::terminate(self))
    }
}

pub(super) async fn remote_process(
    write_status: WriteStatus,
    terminate_error: Option<String>,
    sandbox_type: impl Into<Option<codex_sandboxing::SandboxType>>,
) -> UnifiedExecProcess {
    let (wake_tx, _wake_rx) = watch::channel(0);
    let started = StartedExecProcess {
        process: Arc::new(MockExecProcess {
            process_id: "test-process".to_string().into(),
            write_response: WriteResponse {
                status: write_status,
            },
            read_responses: Mutex::new(VecDeque::new()),
            terminate_error,
            wake_tx,
            events: Vec::new(),
            deadlines: Arc::new(std::sync::Mutex::new(Vec::new())),
        }),
        sandbox_type: sandbox_type.into(),
    };

    UnifiedExecProcess::from_exec_server_started(started, OutputDrainPolicy::WaitForOutputClosure)
        .await
        .expect("remote process should start")
}

#[tokio::test]
async fn remote_write_unknown_process_marks_process_exited() {
    let process = remote_process(
        WriteStatus::UnknownProcess,
        /*terminate_error*/ None,
        codex_sandboxing::SandboxType::None,
    )
    .await;

    let err = process
        .write(b"hello")
        .await
        .expect_err("expected write failure");

    assert!(matches!(err, UnifiedExecError::WriteToStdin));
    assert!(process.has_exited());
}

#[tokio::test]
async fn remote_write_closed_stdin_marks_process_exited() {
    let process = remote_process(
        WriteStatus::StdinClosed,
        /*terminate_error*/ None,
        codex_sandboxing::SandboxType::None,
    )
    .await;

    let err = process
        .write(b"hello")
        .await
        .expect_err("expected write failure");

    assert!(matches!(err, UnifiedExecError::WriteToStdin));
    assert!(process.has_exited());
}

#[tokio::test]
async fn fail_and_terminate_preserves_failure_message() {
    let process = remote_process(
        WriteStatus::Accepted,
        /*terminate_error*/ None,
        codex_sandboxing::SandboxType::None,
    )
    .await;

    process.fail_and_terminate("network denied".to_string());
    process.fail_and_terminate("second failure".to_string());

    assert!(process.has_exited());
    assert_eq!(
        process.failure_message(),
        Some("network denied".to_string())
    );
}

#[tokio::test]
async fn remote_terminate_confirmed_updates_state_on_success_only() {
    let process = remote_process(
        WriteStatus::Accepted,
        Some("terminate unavailable".to_string()),
        codex_sandboxing::SandboxType::None,
    )
    .await;

    let err = process
        .terminate_confirmed()
        .await
        .expect_err("expected terminate failure");

    assert!(matches!(err, UnifiedExecError::ProcessFailed { .. }));
    assert!(!process.has_exited());

    let process = remote_process(
        WriteStatus::Accepted,
        /*terminate_error*/ None,
        codex_sandboxing::SandboxType::None,
    )
    .await;

    process
        .terminate_confirmed()
        .await
        .expect("terminate should succeed");

    assert!(process.has_exited());
}

#[tokio::test]
async fn remote_process_preserves_executor_sandbox_type() {
    use codex_sandboxing::SandboxType;
    for sandbox_type in [
        None,
        Some(SandboxType::None),
        Some(SandboxType::LinuxSeccomp),
    ] {
        let process = remote_process(
            WriteStatus::Accepted,
            /*terminate_error*/ None,
            sandbox_type,
        )
        .await;
        assert_eq!(process.sandbox_type(), sandbox_type);
    }
}

// These fixtures exercise core's real event/read owner and census using inert
// replayed events. They do not qualify native sandbox or remote transport.
async fn remote_custody_fixture(
    sandbox_type: Option<codex_sandboxing::SandboxType>,
    events: Vec<ExecProcessEvent>,
    scope: codex_sandboxing::CleanupDeadlineScope,
) -> (
    UnifiedExecProcess,
    Arc<crate::sandboxing::cleanup_census::SandboxCleanupCensus>,
    Arc<std::sync::Mutex<Vec<std::time::Instant>>>,
) {
    let census = Arc::new(crate::sandboxing::cleanup_census::SandboxCleanupCensus::default());
    let operation =
        crate::sandboxing::cleanup_census::SandboxCleanupOperation::new(census.clone(), true);
    let deadlines = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (wake_tx, _) = watch::channel(0);
    let started = StartedExecProcess {
        sandbox_type,
        process: Arc::new(MockExecProcess {
            process_id: "custody-fixture".into(),
            write_response: WriteResponse {
                status: WriteStatus::Accepted,
            },
            read_responses: Mutex::new(VecDeque::new()),
            terminate_error: None,
            wake_tx,
            events,
            deadlines: deadlines.clone(),
        }),
    };
    let process = UnifiedExecProcess::from_exec_server_started_with_cleanup(
        started,
        OutputDrainPolicy::WaitForOutputClosure,
        Some(scope),
        Some(operation),
    )
    .await
    .expect("start inert fixture");
    (process, census, deadlines)
}

fn cleanup_events(
    summary: Option<codex_protocol::exec_output::SandboxCleanupSummary>,
) -> Vec<ExecProcessEvent> {
    vec![
        ExecProcessEvent::Exited {
            seq: 1,
            exit_code: 0,
            sandbox_denied: Some(false),
        },
        ExecProcessEvent::Closed {
            seq: 2,
            sandbox_cleanup: summary,
        },
    ]
}

#[tokio::test]
async fn remote_cleanup_receipt_is_recorded_in_the_original_census_once() {
    let summary = codex_protocol::exec_output::SandboxCleanupSummary {
        state: codex_protocol::exec_output::SandboxCleanupState::Complete,
        reaped: 1,
        preserved_unproven: 0,
        host_owned: 2,
        handed_off: 0,
        timed_out: 0,
        joined: true,
    };
    let (process, census, _) = remote_custody_fixture(
        Some(codex_sandboxing::SandboxType::LinuxSeccomp),
        cleanup_events(Some(summary)),
        codex_sandboxing::CleanupDeadlineScope::new(),
    )
    .await;
    process.collect_sandbox_cleanup().await;
    process.collect_sandbox_cleanup().await;
    let totals = census.totals();
    assert_eq!(
        (
            totals.operations,
            totals.complete,
            totals.missing_receipt,
            totals.pending_receipt,
            totals.reaped_targets
        ),
        (1, 1, 0, 0, 1)
    );
    assert!(!totals.has_unproven());
}

#[tokio::test]
async fn remote_missing_receipt_is_never_a_no_launch_claim() {
    for sandbox in [Some(codex_sandboxing::SandboxType::LinuxSeccomp), None] {
        let (process, census, _) = remote_custody_fixture(
            sandbox,
            cleanup_events(None),
            codex_sandboxing::CleanupDeadlineScope::new(),
        )
        .await;
        process.collect_sandbox_cleanup().await;
        let totals = census.totals();
        assert_eq!(
            (totals.operations, totals.missing_receipt, totals.no_launch),
            (1, 1, 0)
        );
        assert!(totals.has_unproven());
    }
}

#[tokio::test]
async fn remote_positive_non_linux_backend_owes_no_linux_receipt() {
    let (process, census, _) = remote_custody_fixture(
        Some(codex_sandboxing::SandboxType::MacosSeatbelt),
        cleanup_events(None),
        codex_sandboxing::CleanupDeadlineScope::new(),
    )
    .await;
    process.collect_sandbox_cleanup().await;
    assert_eq!(
        (
            census.totals().no_launch,
            census.totals().operations,
            census.totals().pending_receipt
        ),
        (1, 0, 0)
    );
}

#[tokio::test]
async fn remote_retirement_passes_the_earliest_instant_to_terminate() {
    let (process, _, requests) = remote_custody_fixture(
        Some(codex_sandboxing::SandboxType::LinuxSeccomp),
        Vec::new(),
        codex_sandboxing::CleanupDeadlineScope::new(),
    )
    .await;
    let deadline = std::time::Instant::now();
    process.retire(deadline);
    process.retire(deadline + std::time::Duration::from_secs(100));
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while requests.lock().expect("requests").len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("termination requests");
    assert!(
        requests
            .lock()
            .expect("requests")
            .iter()
            .all(|actual| *actual == deadline)
    );
    process.collect_sandbox_cleanup().await;
}

#[tokio::test]
async fn scope_tightening_wakes_a_remote_collector_without_a_fresh_grace() {
    let scope = codex_sandboxing::CleanupDeadlineScope::new();
    let (process, census, _) = remote_custody_fixture(
        Some(codex_sandboxing::SandboxType::LinuxSeccomp),
        Vec::new(),
        scope.clone(),
    )
    .await;
    let collect = process.collect_sandbox_cleanup();
    tokio::pin!(collect);
    assert!(futures::poll!(&mut collect).is_pending());
    scope.tighten(std::time::Instant::now());
    tokio::time::timeout(std::time::Duration::from_secs(1), &mut collect)
        .await
        .expect("scope woke original observer");
    assert_eq!(
        (
            census.totals().missing_receipt,
            census.totals().pending_receipt
        ),
        (1, 0)
    );
}

#[tokio::test]
async fn dropping_remote_owner_preserves_missing_debt() {
    let (process, census, _) = remote_custody_fixture(
        Some(codex_sandboxing::SandboxType::LinuxSeccomp),
        Vec::new(),
        codex_sandboxing::CleanupDeadlineScope::new(),
    )
    .await;
    assert_eq!(census.totals().pending_receipt, 1);
    drop(process);
    tokio::task::yield_now().await;
    assert_eq!(
        (
            census.totals().operations,
            census.totals().missing_receipt,
            census.totals().pending_receipt
        ),
        (1, 1, 0)
    );
}
