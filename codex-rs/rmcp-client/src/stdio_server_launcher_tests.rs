use super::*;
use codex_exec_server::Environment;
use codex_exec_server::ExecProcessFuture;
use codex_exec_server::ProcessId;
use codex_exec_server::ProcessSignal;
use codex_exec_server::ReadResponse;
use codex_exec_server::WriteResponse;
use pretty_assertions::assert_eq;
use std::io::Write;
#[cfg(unix)]
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;

const READY: &str = "executor-retirement-fixture-ready";
const FIXTURE_TEST: &str = "stdio_server_launcher::executor_tests::executor_stdio_fixture";

#[test]
#[ignore = "subprocess fixture launched only by the executor retirement tests"]
fn executor_stdio_fixture() {
    println!("{READY}");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    std::io::stdin().read_line(&mut line).unwrap();
}

async fn start_live_executor_process()
-> anyhow::Result<(Arc<dyn ExecProcess>, ExecProcessEventReceiver)> {
    let backend = Environment::default_for_tests().get_exec_backend();
    let started = backend
        .start(ExecParams {
            process_id: ExecutorProcessTransport::next_process_id(),
            argv: vec![
                std::env::current_exe()?.to_string_lossy().into_owned(),
                "--exact".to_owned(),
                FIXTURE_TEST.to_owned(),
                "--ignored".to_owned(),
                "--nocapture".to_owned(),
            ],
            cwd: PathUri::from_host_native_path(std::env::current_dir()?)?,
            env_policy: None,
            env: HashMap::new(),
            tty: false,
            pipe_stdin: true,
            arg0: None,
            sandbox: None,
            enforce_managed_network: false,
            managed_network: None,
            network_proxy: None,
        })
        .await?;
    let mut events = started.process.subscribe_events();
    let ready = tokio::time::timeout(Duration::from_secs(10), async {
        let mut output = Vec::new();
        loop {
            match events.recv().await? {
                ExecProcessEvent::Output(chunk) => {
                    output.extend_from_slice(&chunk.chunk.0);
                    if output
                        .windows(READY.len())
                        .any(|part| part == READY.as_bytes())
                    {
                        return Ok::<_, anyhow::Error>(());
                    }
                }
                event => anyhow::bail!("fixture ended before readiness: {event:?}"),
            }
        }
    })
    .await;
    if !matches!(ready, Ok(Ok(()))) {
        // Clean up the real fixture even when a constructor/name regression
        // prevents the readiness proof.
        let _ = started.process.terminate().await;
    }
    ready??;
    Ok((started.process, events))
}

async fn terminal_sequence(events: &mut ExecProcessEventReceiver) -> anyhow::Result<u64> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match events.recv().await? {
                ExecProcessEvent::Closed { seq } => return Ok(seq),
                ExecProcessEvent::Exited { .. } | ExecProcessEvent::Output(_) => {}
                ExecProcessEvent::Failed(message) => anyhow::bail!("executor failed: {message}"),
            }
        }
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_executor_subscribe_before_and_after_termination_observe_same_closed()
-> anyhow::Result<()> {
    let (process, mut before) = start_live_executor_process().await?;
    let mut classifier_before = process.subscribe_events();
    process.terminate().await?;
    let before_seq = terminal_sequence(&mut before).await?;
    // This receiver is created only after Closed was independently consumed.
    let mut after = process.subscribe_events();
    assert_eq!(terminal_sequence(&mut after).await?, before_seq);
    tokio::time::timeout(
        Duration::from_secs(10),
        await_executor_process_close(&mut classifier_before),
    )
    .await??;
    let mut classifier_after = process.subscribe_events();
    tokio::time::timeout(
        Duration::from_secs(10),
        await_executor_process_close(&mut classifier_after),
    )
    .await??;
    Ok(())
}

struct CountedRealProcess {
    process: Arc<dyn ExecProcess>,
    subscriptions: AtomicUsize,
    terminations: AtomicUsize,
    observations_unavailable: AtomicBool,
}

impl ExecProcess for CountedRealProcess {
    fn process_id(&self) -> &ProcessId {
        self.process.process_id()
    }
    fn subscribe_wake(&self) -> tokio::sync::watch::Receiver<u64> {
        assert!(!self.observations_unavailable.load(Ordering::Acquire));
        self.process.subscribe_wake()
    }
    fn subscribe_events(&self) -> ExecProcessEventReceiver {
        assert!(
            !self.observations_unavailable.load(Ordering::Acquire),
            "retained terminal proof must not request replay"
        );
        self.subscriptions.fetch_add(1, Ordering::Relaxed);
        self.process.subscribe_events()
    }
    fn read(
        &self,
        after_seq: Option<u64>,
        max_bytes: Option<usize>,
        wait_ms: Option<u64>,
    ) -> ExecProcessFuture<'_, ReadResponse> {
        assert!(!self.observations_unavailable.load(Ordering::Acquire));
        self.process.read(after_seq, max_bytes, wait_ms)
    }
    fn write(&self, chunk: Vec<u8>) -> ExecProcessFuture<'_, WriteResponse> {
        assert!(!self.observations_unavailable.load(Ordering::Acquire));
        self.process.write(chunk)
    }
    fn signal(&self, signal: ProcessSignal) -> ExecProcessFuture<'_, ()> {
        assert!(!self.observations_unavailable.load(Ordering::Acquire));
        self.process.signal(signal)
    }
    fn terminate(&self) -> ExecProcessFuture<'_, ()> {
        assert!(
            !self.observations_unavailable.load(Ordering::Acquire),
            "retained terminal proof must not resend terminate"
        );
        self.terminations.fetch_add(1, Ordering::Relaxed);
        self.process.terminate()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_executor_concurrent_and_repeated_retirement_retains_terminal_proof()
-> anyhow::Result<()> {
    let (process, mut evidence) = start_live_executor_process().await?;
    let counted = Arc::new(CountedRealProcess {
        process,
        subscriptions: AtomicUsize::new(0),
        terminations: AtomicUsize::new(0),
        observations_unavailable: AtomicBool::new(false),
    });
    let handle =
        StdioServerProcessHandle::executor("real-executor-fixture".to_owned(), counted.clone());
    let other = handle.clone();
    let (first, concurrent) = tokio::join!(handle.terminate(), other.terminate());
    first?;
    concurrent?;
    terminal_sequence(&mut evidence).await?;
    assert!(handle.inner.terminal_observed.load(Ordering::Acquire));
    assert_eq!(
        (
            counted.subscriptions.load(Ordering::Relaxed),
            counted.terminations.load(Ordering::Relaxed)
        ),
        (1, 1)
    );

    // Unlike a synthetic Closed event, the proof above came from the real
    // backend. Now make any attempted reacquisition fail: replay availability
    // cannot be the reason the repeated/concurrent calls continue to succeed.
    counted
        .observations_unavailable
        .store(true, Ordering::Release);
    let (repeat, repeat_concurrent) = tokio::join!(handle.terminate(), other.terminate());
    repeat?;
    repeat_concurrent?;
    assert_eq!(
        (
            counted.subscriptions.load(Ordering::Relaxed),
            counted.terminations.load(Ordering::Relaxed)
        ),
        (1, 1)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn direct_executor_transport_close_waits_for_closed_proof() -> anyhow::Result<()> {
    let (process, mut evidence) = start_live_executor_process().await?;
    let counted = Arc::new(CountedRealProcess {
        process,
        subscriptions: AtomicUsize::new(0),
        terminations: AtomicUsize::new(0),
        observations_unavailable: AtomicBool::new(false),
    });
    let mut transport =
        ExecutorProcessTransport::new(counted.clone(), "real-executor-fixture".to_owned());

    transport.close().await?;
    terminal_sequence(&mut evidence).await?;
    drop(transport);

    assert_eq!(counted.subscriptions.load(Ordering::Relaxed), 2);
    assert_eq!(counted.terminations.load(Ordering::Relaxed), 1);
    Ok(())
}

#[cfg(unix)]
struct GatedLocalLauncher {
    launcher: LocalStdioServerLauncher,
    entered: Mutex<Option<tokio::sync::oneshot::Sender<StdioServerProcessHandle>>>,
    gate: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    launches: AtomicUsize,
}

#[cfg(unix)]
impl private::Sealed for GatedLocalLauncher {}

#[cfg(unix)]
impl StdioServerLauncher for GatedLocalLauncher {
    fn launch(
        &self,
        command: StdioServerCommand,
    ) -> BoxFuture<'static, io::Result<StdioServerTransport>> {
        self.launches.fetch_add(1, Ordering::Relaxed);
        let launched = self.launcher.launch(command);
        let entered = self.entered.lock().unwrap().take();
        let gate = self.gate.lock().unwrap().take();
        async move {
            let transport = launched.await?;
            if let Some(entered) = entered {
                entered
                    .send(transport.process_handle())
                    .map_err(|_| io::Error::other("launch observer gone"))?;
            }
            if let Some(gate) = gate {
                gate.await
                    .map_err(|_| io::Error::other("launch gate dropped"))?;
            }
            Ok(transport)
        }
        .boxed()
    }
}

#[cfg(unix)]
fn local_group(handle: &StdioServerProcessHandle) -> u32 {
    let StdioServerProcessKind::Local {
        terminator: Some(terminator),
        ..
    } = &handle.inner.kind
    else {
        panic!("real local launcher must own its process group")
    };
    terminator.process_group_id
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_stdio_constructor_keeps_launched_child_in_external_retirement_registry()
-> anyhow::Result<()> {
    use crate::retirement::PhysicalRetirementOutcome;
    use crate::retirement::PhysicalRetirementReport;
    use crate::retirement::RmcpClientRetirement;
    use crate::rmcp_client::RmcpClient;
    for mode in [McpProtocolMode::Legacy, McpProtocolMode::V20260728] {
        let home = tempfile::tempdir()?;
        let registry = RmcpClientRetirement::default();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let launcher = Arc::new(GatedLocalLauncher {
            launcher: LocalStdioServerLauncher::new(home.path().to_path_buf()),
            entered: Mutex::new(Some(entered_tx)),
            gate: Mutex::new(Some(release_rx)),
            launches: AtomicUsize::new(0),
        });
        let launch = launcher.clone();
        let owner = registry.clone();
        let env = (mode == McpProtocolMode::V20260728).then(|| {
            HashMap::from([(
                OsString::from("CODEX_MCP_PROTOCOL_VERSION"),
                OsString::from("2026-07-28"),
            )])
        });
        let observer = tokio::spawn(async move {
            RmcpClient::new_stdio_client_in_retirement(
                "sh".into(),
                vec!["-c".into(), "exec sleep 30".into()],
                env,
                &[],
                /*cwd*/ None,
                launch,
                mode,
                owner,
            )
            .await
        });
        let process = tokio::time::timeout(Duration::from_secs(5), entered_rx).await??;
        let group = local_group(&process);
        assert!(codex_utils_pty::process_group::process_group_exists(group)?);
        observer.abort();
        assert!(
            observer
                .await
                .err()
                .is_some_and(|error| error.is_cancelled())
        );
        release_tx.send(()).unwrap();
        let report = registry
            .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(10))
            .await;
        let group_gone_before_cleanup =
            !codex_utils_pty::process_group::process_group_exists(group)?;
        let terminal_before_cleanup = process.inner.terminal_observed.load(Ordering::Acquire);
        // Cleanup is independent of the assertion, so a regression cannot
        // strand the fixture process merely because the report is wrong.
        let cleanup = process.terminate().await;
        assert_eq!(
            report,
            PhysicalRetirementReport {
                attempts: vec![(0, PhysicalRetirementOutcome::Complete)]
            }
        );
        cleanup?;
        assert!(
            group_gone_before_cleanup && terminal_before_cleanup,
            "registry must prove group exit before independent cleanup"
        );
        assert!(!codex_utils_pty::process_group::process_group_exists(
            group
        )?);
        assert_eq!(launcher.launches.load(Ordering::Relaxed), 1);
        assert_eq!(
            registry
                .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(1))
                .await,
            report
        );
    }
    Ok(())
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_real_stdio_handshake_has_one_attempt_and_retires_child_group() -> anyhow::Result<()>
{
    use crate::retirement::PhysicalRetirementOutcome;
    use crate::retirement::PhysicalRetirementReport;
    use crate::retirement::RmcpClientRetirement;
    use crate::rmcp_client::RmcpClient;
    use rmcp::model::ClientCapabilities;
    use rmcp::model::Implementation;
    use rmcp::model::InitializeRequestParams;
    for mode in [McpProtocolMode::Legacy, McpProtocolMode::V20260728] {
        let home = tempfile::tempdir()?;
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let launcher = Arc::new(GatedLocalLauncher {
            launcher: LocalStdioServerLauncher::new(home.path().to_path_buf()),
            entered: Mutex::new(Some(entered_tx)),
            gate: Mutex::new(None),
            launches: AtomicUsize::new(0),
        });
        let env = (mode == McpProtocolMode::V20260728).then(|| {
            HashMap::from([(
                OsString::from("CODEX_MCP_PROTOCOL_VERSION"),
                OsString::from("2026-07-28"),
            )])
        });
        let client = RmcpClient::new_stdio_client_in_retirement(
            "sh".into(),
            vec!["-c".into(), "IFS= read -r request; exit 7".into()],
            env,
            &[],
            /*cwd*/ None,
            launcher.clone(),
            mode,
            RmcpClientRetirement::default(),
        )
        .await?;
        let process = entered_rx.await?;
        let group = local_group(&process);
        assert!(codex_utils_pty::process_group::process_group_exists(group)?);
        let initialized = client
            .initialize(
                InitializeRequestParams::new(
                    ClientCapabilities::default(),
                    Implementation::new("retirement-test", "1"),
                ),
                Some(Duration::from_secs(5)),
                Box::new(|_, _| async { anyhow::bail!("unexpected elicitation") }.boxed()),
            )
            .await;
        let report = client
            .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(10))
            .await;
        let group_gone_before_cleanup =
            !codex_utils_pty::process_group::process_group_exists(group)?;
        let terminal_before_cleanup = process.inner.terminal_observed.load(Ordering::Acquire);
        let cleanup = process.terminate().await;
        assert!(initialized.is_err());
        assert!(
            group_gone_before_cleanup && terminal_before_cleanup,
            "client retirement must prove group exit before independent cleanup"
        );
        assert_eq!(
            report,
            PhysicalRetirementReport {
                attempts: vec![(0, PhysicalRetirementOutcome::Complete)]
            }
        );
        cleanup?;
        assert!(!codex_utils_pty::process_group::process_group_exists(
            group
        )?);
        assert_eq!(
            launcher.launches.load(Ordering::Relaxed),
            1,
            "stdio handshake failure must not relaunch"
        );
    }
    Ok(())
}
