use super::*;
use codex_exec_server::Environment;
use codex_exec_server::ExecProcessFuture;
use codex_exec_server::ProcessId;
use codex_exec_server::ProcessSignal;
use codex_exec_server::ReadResponse;
use codex_exec_server::WriteResponse;
use pretty_assertions::assert_eq;
use std::io::Write;
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
            metadata: Default::default(),
            process_id: ExecutorProcessTransport::next_process_id(),
            argv: vec![
                std::env::current_exe()?.to_string_lossy().into_owned(),
                "--exact".to_owned(),
                FIXTURE_TEST.to_owned(),
                "--ignored".to_owned(),
                "--nocapture".to_owned(),
            ],
            cwd: PathUri::from_host_native_path(std::env::current_dir()?)?,
            shell_snapshot: None,
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
