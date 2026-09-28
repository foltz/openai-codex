//! Real process/Unix-acceptor proofs. The child runs this integration binary's
//! production server entry point, so the parent has the same executable identity
//! without replacing or injecting the server's peer-authorization decision.

use anyhow::Context;
use anyhow::Result;
use anyhow::ensure;
use app_test_support::ChatGptAuthFixture;
use app_test_support::MockResponsesConfig;
use app_test_support::create_mock_responses_server_repeating_assistant;
use app_test_support::write_chatgpt_auth;
use codex_config::types::AuthCredentialsStoreMode;
use codex_http_client::HttpClientBuilder;
use futures::SinkExt;
use futures::StreamExt;
use serde_json::Value;
use serde_json::json;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;
use tokio::process::Child;
use tokio::process::Command;
use tokio::time::sleep;
use tokio::time::timeout;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::client_async;
use tokio_tungstenite::tungstenite::Message;

const WAIT: Duration = Duration::from_secs(30);
const CHILD_SOCKET: &str = "CODEX_TEST_MANAGED_TRANSITION_SOCKET";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "subprocess entry point, invoked only with a disposable socket"]
async fn managed_transition_server_process_helper() -> Result<()> {
    let socket = std::env::var(CHILD_SOCKET).context("missing test socket")?;
    codex_app_server::run_main_with_transport_options(
        codex_arg0::Arg0DispatchPaths {
            codex_self_exe: Some(codex_utils_cargo_bin::cargo_bin("codex-app-server")?),
            ..Default::default()
        },
        Default::default(),
        codex_config::LoaderOverrides::without_managed_config_for_tests(),
        false,
        false,
        format!("unix://{socket}").parse()?,
        codex_protocol::protocol::SessionSource::VSCode,
        Default::default(),
        codex_app_server::AppServerRuntimeOptions {
            plugin_startup_tasks: codex_app_server::PluginStartupTasks::Skip,
            remote_control_startup_mode:
                codex_app_server::RemoteControlStartupMode::DisabledEphemeral,
            ..Default::default()
        },
    )
    .await?;
    Ok(())
}

struct Server {
    process: Child,
    peer: Peer,
    _executable_directory: TempDir,
}

struct Peer {
    socket: WebSocketStream<tokio::net::UnixStream>,
    next_id: i64,
    account_events_before_response: Vec<Value>,
}

impl std::ops::Deref for Server {
    type Target = Peer;
    fn deref(&self) -> &Peer {
        &self.peer
    }
}

impl std::ops::DerefMut for Server {
    fn deref_mut(&mut self) -> &mut Peer {
        &mut self.peer
    }
}

impl Server {
    async fn start(home: &Path, socket_path: &Path) -> Result<Self> {
        let started = std::time::Instant::now();
        eprintln!("managed-process: spawn begin");
        // Keep the real signed executable and inode, but avoid asking macOS
        // code discovery to enumerate the entire Cargo dependency directory.
        // Same-filesystem placement makes hard-link identity mandatory; there
        // is no copy or weaker provenance fallback.
        let executable = std::env::current_exe()?;
        let executable_directory = tempfile::Builder::new()
            .prefix("managed-process-")
            .tempdir_in(executable.parent().context("test executable parent")?)?;
        let helper = executable_directory.path().join("managed-process-helper");
        std::fs::hard_link(&executable, &helper)?;
        use std::os::unix::fs::MetadataExt;
        let original = std::fs::metadata(&executable)?;
        let linked = std::fs::metadata(&helper)?;
        ensure!(
            (original.dev(), original.ino()) == (linked.dev(), linked.ino()),
            "helper must retain the exact executable file identity"
        );
        let mut process = Command::new(helper)
            .args(["--ignored", "--exact", "suite::v2::managed_transition::process_proofs::managed_transition_server_process_helper", "--nocapture"])
            .env("CODEX_HOME", home)
            .env(CHILD_SOCKET, socket_path)
            .env("KESTREL_CODEX_MANAGED_PROFILE", "disposable-regression")
            // Keep fixture metrics for explicit reset flushes; do not inherit
            // an ambient short interval that consumes them before the proof.
            .env("OTEL_METRIC_EXPORT_INTERVAL", "3600000")
            .env_remove("OPENAI_API_KEY")
            .env_remove("CODEX_API_KEY")
            .stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::inherit())
            .kill_on_drop(true).spawn()?;
        let stream = timeout(WAIT, async {
            loop {
                if let Ok(stream) = tokio::net::UnixStream::connect(socket_path).await {
                    break Ok(stream);
                }
                if let Some(status) = process.try_wait()? {
                    anyhow::bail!("test server exited before listening: {status}");
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await??;
        eprintln!(
            "managed-process: socket ready after {:?}",
            started.elapsed()
        );
        let peer = Peer::initialize(stream).await?;
        eprintln!("managed-process: initialized after {:?}", started.elapsed());
        Ok(Self {
            process,
            peer,
            _executable_directory: executable_directory,
        })
    }

    async fn kill(mut self) -> Result<()> {
        let started = std::time::Instant::now();
        eprintln!("managed-process: kill/join begin");
        self.process.kill().await?;
        self.process.wait().await?;
        eprintln!(
            "managed-process: kill/join complete after {:?}",
            started.elapsed()
        );
        Ok(())
    }
}

impl Peer {
    async fn connect(path: &Path) -> Result<Self> {
        Self::initialize(tokio::net::UnixStream::connect(path).await?).await
    }

    async fn initialize(stream: tokio::net::UnixStream) -> Result<Self> {
        let (socket, _) = client_async("ws://localhost/rpc", stream).await?;
        let mut server = Self {
            socket,
            next_id: 1,
            account_events_before_response: Vec::new(),
        };
        server.request("initialize", json!({"clientInfo":{"name":"codex-tui","version":"test"},"capabilities":{"experimentalApi":true,"interactiveClient":true}})).await?;
        server
            .socket
            .send(Message::Text(
                json!({"jsonrpc":"2.0","method":"initialized"})
                    .to_string()
                    .into(),
            ))
            .await?;
        Ok(server)
    }

    async fn request(&mut self, method: &str, params: Value) -> Result<Value> {
        let started = std::time::Instant::now();
        eprintln!("managed-process: request {method} begin");
        self.account_events_before_response.clear();
        let id = self.next_id;
        self.next_id += 1;
        self.socket
            .send(Message::Text(
                json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
                    .to_string()
                    .into(),
            ))
            .await?;
        timeout(WAIT, async {
            while let Some(message) = self.socket.next().await {
                if let Message::Text(text) = message? {
                    let value: Value = serde_json::from_str(&text)?;
                    if matches!(
                        value["method"].as_str(),
                        Some("account/updated" | "account/login/completed")
                    ) {
                        self.account_events_before_response.push(value.clone());
                    }
                    if value["id"] == id {
                        eprintln!(
                            "managed-process: request {method} response after {:?}",
                            started.elapsed()
                        );
                        return value
                            .get("result")
                            .cloned()
                            .with_context(|| format!("{method} failed: {value}"));
                    }
                }
            }
            anyhow::bail!("server disconnected during {method}")
        })
        .await?
    }

    async fn evidence(&mut self) -> Result<Value> {
        let response = self.request("account/managedAuthTransition/read", json!({"contractVersion":1,"transitionId":"probe","processInstanceId":"unknown-instance"})).await?;
        ensure!(
            response["refusal"]["kind"] == "processMismatch",
            "real same-image Unix peer must pass authorization: {response}"
        );
        Ok(response["refusal"].clone())
    }

    async fn adopt(&mut self, evidence: &Value, id: &str, account: &str) -> Result<Value> {
        self.request("account/managedAuthTransition/start", json!({
            "contractVersion":1,"transitionId":id,"processInstanceId":evidence["processInstanceId"],
            "intent":"adoptManagedAuth","expectedAuthRevision":evidence["authRevision"],
            "expectedTransitionRevision":evidence["transitionRevision"],
            "expectedAuthFingerprint":evidence["authFingerprint"],
            "intendedResultAuthFingerprint":codex_login::AuthManager::managed_account_fingerprint(account)
        })).await
    }

    async fn turn(&mut self, thread: &Value) -> Result<()> {
        self.request("turn/start", json!({"threadId":thread,"input":[{"type":"text","text":"Reply Done","text_elements":[]}]})).await?;
        timeout(WAIT, async {
            while let Some(message) = self.socket.next().await {
                if let Message::Text(text) = message? {
                    let event: Value = serde_json::from_str(&text)?;
                    if event["method"] == "turn/completed" && &event["params"]["threadId"] == thread
                    {
                        ensure!(
                            event["params"]["turn"]["status"] == "completed",
                            "provider turn failed: {event}"
                        );
                        return Ok(());
                    }
                }
            }
            anyhow::bail!("server disconnected before provider completion")
        })
        .await?
    }
}

fn write_account(home: &Path, account: &str, token: &str) -> Result<()> {
    write_chatgpt_auth(
        home,
        ChatGptAuthFixture::new(token).account_id(account),
        AuthCredentialsStoreMode::File,
    )?;
    Ok(())
}

fn mock_config(uri: &str) -> MockResponsesConfig {
    MockResponsesConfig::new(uri)
        .with_root_config(&format!("chatgpt_base_url = \"{uri}/backend-api\""))
        .with_provider_config("requires_openai_auth = true\nsupports_websockets = false")
}

async fn mock_backend() -> wiremock::MockServer {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path_regex(".*/models$"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(json!({"models":[]})))
        .mount(&server)
        .await;
    server
}

async fn assert_models_backend_restored(server: &wiremock::MockServer) -> Result<()> {
    let response = HttpClientBuilder::new()
        .build_direct()?
        .get(format!("{}/v1/models?client_version=0.0.0", server.uri()))
        .bearer_auth("fixture-token-b")
        .timeout(WAIT)
        .send()
        .await?;
    ensure!(
        response.status().is_success(),
        "scoped failure must be removed before retry"
    );
    assert_eq!(response.json::<Value>().await?, json!({"models":[]}));
    Ok(())
}

async fn interpose_b_models(server: &wiremock::MockServer, initial_mode: u8) -> Arc<AtomicU8> {
    let mode = Arc::new(AtomicU8::new(initial_mode));
    let response_mode = Arc::clone(&mode);
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path_regex(".*/models$"))
        .and(wiremock::matchers::header(
            "authorization",
            "Bearer fixture-token-b",
        ))
        .respond_with(move |_: &wiremock::Request| {
            let response = wiremock::ResponseTemplate::new(200).set_body_json(json!({"models":[]}));
            match response_mode.load(Ordering::SeqCst) {
                1 => response.set_delay(Duration::from_secs(120)),
                2 => wiremock::ResponseTemplate::new(400),
                _ => response,
            }
        })
        .with_priority(1)
        .mount(server)
        .await;
    // Use responder state, not a priority-sorted scoped mock's insertion ID,
    // to restore I/O. The same mounted responder serves the successful retry.
    mode
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_during_adopting_revalidates_the_real_persisted_source() -> Result<()> {
    let temp = TempDir::new()?;
    let home = temp.path().join("home");
    std::fs::create_dir(&home)?;
    let responses = mock_backend().await;
    mock_config(&responses.uri()).write(&home)?;
    write_account(&home, "account-a", "fixture-token-a")?;
    let mut server = Server::start(&home, &temp.path().join("first.sock")).await?;
    let prior = server.evidence().await?;
    let mut observer = Peer::connect(&temp.path().join("first.sock")).await?;
    let auth_file = home.join("auth.json");
    // Interpose on real storage I/O, after the process has loaded managed A.
    // No credential or coordinator state is injected into the running process.
    write_account(&home, "account-b", "fixture-token-b")?;
    let prepared_bytes = std::fs::read(&auth_file)?;
    std::fs::remove_file(&auth_file)?;
    let mut make_fifo = Command::new("mkfifo")
        .arg(&auth_file)
        .kill_on_drop(true)
        .spawn()?;
    ensure!(timeout(WAIT, make_fifo.wait()).await??.success());
    server.socket.send(Message::Text(json!({
        "jsonrpc":"2.0","id":10000,"method":"account/managedAuthTransition/start",
        "params":{"contractVersion":1,"transitionId":"interrupted-adoption",
            "processInstanceId":prior["processInstanceId"],"intent":"adoptManagedAuth",
            "expectedAuthRevision":prior["authRevision"],"expectedTransitionRevision":prior["transitionRevision"],
            "expectedAuthFingerprint":prior["authFingerprint"],
            "intendedResultAuthFingerprint":codex_login::AuthManager::managed_account_fingerprint("account-b")}
    }).to_string().into())).await?;
    // Tokio opens FIFO writers with O_NONBLOCK. ENXIO means the real reader
    // has not arrived; neither open nor an abandoned blocking task can hang
    // this parent. The first read receives a complete valid B object for
    // pre-admission preparation. Closing then parks the distinct raw-preimage
    // verification open, after the coordinator has advanced to Adopting.
    let mut writer = timeout(WAIT, async {
        loop {
            if let Ok(writer) = tokio::net::unix::pipe::OpenOptions::new().open_sender(&auth_file) {
                break writer;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("real adoption source never opened the FIFO")?;
    timeout(WAIT, writer.write_all(&prepared_bytes)).await??;
    drop(writer);
    let observed = timeout(WAIT, async {
        loop {
            let observed = observer.request("account/managedAuthTransition/read", json!({
                "contractVersion":1,"transitionId":"interrupted-adoption","processInstanceId":prior["processInstanceId"]
            })).await.context("second-connection read must remain responsive while adoption I/O is parked")?;
            if observed["status"]["phase"] == "adopting" { break Ok::<_, anyhow::Error>(observed); }
            sleep(Duration::from_millis(10)).await;
        }
    }).await??;
    assert_eq!(
        observed["status"]["phase"], "adopting",
        "the process must actually reach the material boundary: {observed}"
    );
    let late = observer.request("account/managedAuthTransition/cancel", json!({
        "contractVersion":1,"transitionId":"interrupted-adoption","processInstanceId":prior["processInstanceId"]
    })).await?;
    assert_eq!(late["refusal"]["kind"], "lateCancellation");
    server.kill().await?;
    std::fs::remove_file(&auth_file)?;
    write_account(&home, "account-b", "fixture-token-b")?;
    let mut restarted = Server::start(&home, &temp.path().join("second.sock")).await?;
    let current = restarted.evidence().await?;
    ensure!(current["processInstanceId"] != prior["processInstanceId"]);
    assert_eq!(
        current["authFingerprint"],
        codex_login::AuthManager::managed_account_fingerprint("account-b")
    );
    let stale = restarted
        .adopt(&prior, "interrupted-adoption", "account-b")
        .await?;
    assert_eq!(stale["refusal"]["kind"], "processMismatch");
    let retry = restarted
        .adopt(&current, "retry-after-adopting", "account-b")
        .await?;
    assert_eq!(
        retry["status"]["phase"], "succeeded",
        "retry must pass actual adoption and reset: {retry}"
    );
    restarted.kill().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistent_server_adopts_b_and_restart_refuses_prior_instance() -> Result<()> {
    let temp = TempDir::new()?;
    let home = temp.path().join("home");
    std::fs::create_dir(&home)?;
    let responses = mock_backend().await;
    mock_config(&responses.uri()).write(&home)?;
    write_account(&home, "account-a", "fixture-token-a")?;
    let mut server = Server::start(&home, &temp.path().join("first.sock")).await?;
    let pid = server.process.id().context("server pid")?;
    let prior = server.evidence().await?;
    let started = server.request("thread/start", json!({})).await?;
    let thread_id = started["thread"]["id"].clone();
    server.turn(&thread_id).await?;
    write_chatgpt_auth(
        &home,
        ChatGptAuthFixture::new("fixture-token-b")
            .account_id("account-b")
            .plan_type("pro"),
        AuthCredentialsStoreMode::File,
    )?;
    let result = server.adopt(&prior, "adopt-b", "account-b").await?;
    assert_eq!(
        server.account_events_before_response.len(),
        1,
        "exactly one account event must precede the Start response"
    );
    assert_eq!(
        server.account_events_before_response[0]["method"],
        "account/updated"
    );
    assert_eq!(
        server.account_events_before_response[0]["params"],
        json!({"authMode":"chatgpt","planType":"pro"})
    );
    ensure!(
        result["status"]["phase"] == "succeeded",
        "adoption must succeed: {result}"
    );
    assert_eq!(
        result["status"]["resultAuthFingerprint"],
        codex_login::AuthManager::managed_account_fingerprint("account-b")
    );
    ensure!(result["status"]["authRevision"].as_u64() > prior["authRevision"].as_u64());
    assert_eq!(server.process.id(), Some(pid));
    // Reuse the original logical thread: starting a different thread would
    // observe B even if reset left A's existing ModelClient usable.
    let resumed = server
        .request("thread/resume", json!({"threadId":thread_id}))
        .await?;
    assert_eq!(resumed["thread"]["id"], thread_id);
    ensure!(
        resumed["thread"]["turns"]
            .as_array()
            .is_some_and(|turns| !turns.is_empty()),
        "account reset must preserve the original thread history: {resumed}"
    );
    server.turn(&thread_id).await?;
    let requests = responses
        .received_requests()
        .await
        .context("request recording enabled")?;
    let auth: Vec<_> = requests
        .iter()
        .filter(|r| r.url.path().ends_with("/responses"))
        .map(|r| {
            r.headers
                .get("authorization")
                .and_then(|h| h.to_str().ok())
                .unwrap_or("")
        })
        .collect();
    assert_eq!(
        auth,
        ["Bearer fixture-token-a", "Bearer fixture-token-b"],
        "post-transition provider work must use the new auth transport"
    );
    let accounts: Vec<_> = requests
        .iter()
        .filter(|r| r.url.path().ends_with("/responses"))
        .map(|r| {
            r.headers
                .get("chatgpt-account-id")
                .and_then(|h| h.to_str().ok())
                .unwrap_or("")
        })
        .collect();
    assert_eq!(accounts, ["account-a", "account-b"]);
    server.kill().await?;

    let mut restarted = Server::start(&home, &temp.path().join("second.sock")).await?;
    ensure!(restarted.process.id() != Some(pid));
    let current = restarted.evidence().await?;
    ensure!(current["processInstanceId"] != prior["processInstanceId"]);
    assert_eq!(
        current["authFingerprint"],
        codex_login::AuthManager::managed_account_fingerprint("account-b")
    );
    let stale = restarted.adopt(&prior, "adopt-b", "account-b").await?;
    assert_eq!(stale["refusal"]["kind"], "processMismatch");
    let retry = restarted.adopt(&current, "retry-b", "account-b").await?;
    ensure!(
        retry["status"]["phase"] == "succeeded",
        "current-state retry must run adoption/reset: {retry}"
    );
    restarted.kill().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_after_unintended_result_refusal_reconstructs_persisted_c() -> Result<()> {
    let temp = TempDir::new()?;
    let home = temp.path().join("home");
    std::fs::create_dir(&home)?;
    let responses = mock_backend().await;
    mock_config(&responses.uri()).write(&home)?;
    write_account(&home, "account-a", "fixture-token-a")?;
    let mut server = Server::start(&home, &temp.path().join("first.sock")).await?;
    let prior = server.evidence().await?;
    write_account(&home, "account-c", "fixture-token-c")?;
    let rejected = server.adopt(&prior, "intended-b", "account-b").await?;
    assert_eq!(
        rejected["type"], "refused",
        "unintended C must refuse before admission: {rejected}"
    );
    assert_eq!(
        rejected["refusal"]["authFingerprint"],
        prior["authFingerprint"]
    );
    assert_eq!(
        rejected["refusal"]["transitionRevision"],
        prior["transitionRevision"]
    );
    server.kill().await?;

    let mut restarted = Server::start(&home, &temp.path().join("second.sock")).await?;
    let current = restarted.evidence().await?;
    ensure!(current["processInstanceId"] != prior["processInstanceId"]);
    assert_eq!(
        current["authFingerprint"],
        codex_login::AuthManager::managed_account_fingerprint("account-c")
    );
    let old = restarted.request("account/managedAuthTransition/read", json!({"contractVersion":1,"transitionId":"intended-b","processInstanceId":prior["processInstanceId"]})).await?;
    assert_eq!(old["refusal"]["kind"], "processMismatch");
    let forgotten = restarted.request("account/managedAuthTransition/read", json!({"contractVersion":1,"transitionId":"intended-b","processInstanceId":current["processInstanceId"]})).await?;
    assert_eq!(
        forgotten["type"], "refused",
        "restart cannot manufacture the old outcome"
    );
    write_account(&home, "account-b", "fixture-token-b")?;
    let retry = restarted
        .adopt(&current, "new-intended-b", "account-b")
        .await?;
    assert_eq!(
        retry["status"]["phase"], "succeeded",
        "retry must use current C as its prestate: {retry}"
    );
    assert_eq!(
        retry["status"]["resultAuthFingerprint"],
        codex_login::AuthManager::managed_account_fingerprint("account-b")
    );
    restarted.kill().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_during_real_models_reset_discards_prior_completion_authority() -> Result<()> {
    let temp = TempDir::new()?;
    let home = temp.path().join("home");
    std::fs::create_dir(&home)?;
    let responses = mock_backend().await;
    mock_config(&responses.uri()).write(&home)?;
    write_account(&home, "account-a", "fixture-token-a")?;
    let mut server = Server::start(&home, &temp.path().join("first.sock")).await?;
    let prior = server.evidence().await?;
    let mut observer = Peer::connect(&temp.path().join("first.sock")).await?;
    // Only the new account's real models I/O stalls. A startup response is
    // still successful, and no mock controls coordinator phase transitions.
    let held_models = interpose_b_models(&responses, 1).await;
    write_account(&home, "account-b", "fixture-token-b")?;
    server.socket.send(Message::Text(json!({
        "jsonrpc":"2.0","id":10000,"method":"account/managedAuthTransition/start",
        "params":{"contractVersion":1,"transitionId":"interrupted-reset",
            "processInstanceId":prior["processInstanceId"],"intent":"adoptManagedAuth",
            "expectedAuthRevision":prior["authRevision"],"expectedTransitionRevision":prior["transitionRevision"],
            "expectedAuthFingerprint":prior["authFingerprint"],
            "intendedResultAuthFingerprint":codex_login::AuthManager::managed_account_fingerprint("account-b")}
    }).to_string().into())).await?;
    timeout(WAIT, async {
        loop {
            let observed = observer.request("account/managedAuthTransition/read", json!({
                "contractVersion":1,"transitionId":"interrupted-reset","processInstanceId":prior["processInstanceId"]
            })).await?;
            if observed["status"]["phase"] == "resetting" { break Ok::<(), anyhow::Error>(()); }
            ensure!(observed["status"]["phase"] != "succeeded" && observed["status"]["phase"] != "quarantined", "reset must remain blocked on actual models I/O: {observed}");
            sleep(Duration::from_millis(10)).await;
        }
    }).await??;
    let late = observer.request("account/managedAuthTransition/cancel", json!({
        "contractVersion":1,"transitionId":"interrupted-reset","processInstanceId":prior["processInstanceId"]
    })).await?;
    assert_eq!(late["refusal"]["kind"], "lateCancellation");
    timeout(WAIT, async {
        loop {
            if responses
                .received_requests()
                .await
                .unwrap_or_default()
                .iter()
                .any(|request| {
                    request.url.path().ends_with("/models")
                        && request
                            .headers
                            .get("authorization")
                            .and_then(|h| h.to_str().ok())
                            == Some("Bearer fixture-token-b")
                })
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    server.kill().await?;
    held_models.store(0, Ordering::SeqCst);
    assert_models_backend_restored(&responses).await?;
    let mut restarted = Server::start(&home, &temp.path().join("second.sock")).await?;
    let current = restarted.evidence().await?;
    ensure!(current["processInstanceId"] != prior["processInstanceId"]);
    assert_eq!(
        current["authFingerprint"],
        codex_login::AuthManager::managed_account_fingerprint("account-b")
    );
    let stale = restarted
        .adopt(&prior, "interrupted-reset", "account-b")
        .await?;
    assert_eq!(stale["refusal"]["kind"], "processMismatch");
    let retry = restarted
        .adopt(&current, "retry-after-reset", "account-b")
        .await?;
    assert_eq!(
        retry["status"]["phase"], "succeeded",
        "new process must revalidate and complete reset: {retry}"
    );
    restarted.kill().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_after_failed_models_reset_reconstructs_installed_b_without_old_success()
-> Result<()> {
    let temp = TempDir::new()?;
    let home = temp.path().join("home");
    std::fs::create_dir(&home)?;
    let responses = mock_backend().await;
    mock_config(&responses.uri()).write(&home)?;
    write_account(&home, "account-a", "fixture-token-a")?;
    let mut server = Server::start(&home, &temp.path().join("first.sock")).await?;
    let prior = server.evidence().await?;
    let failed_models = interpose_b_models(&responses, 2).await;
    write_account(&home, "account-b", "fixture-token-b")?;
    let failed = server
        .adopt(&prior, "failed-model-reset", "account-b")
        .await?;
    assert_eq!(
        failed["status"]["phase"], "quarantined",
        "required reset failure cannot acknowledge success: {failed}"
    );
    assert_eq!(
        failed["status"]["resultAuthFingerprint"],
        codex_login::AuthManager::managed_account_fingerprint("account-b")
    );
    server.kill().await?;
    failed_models.store(0, Ordering::SeqCst);
    assert_models_backend_restored(&responses).await?;
    let mut restarted = Server::start(&home, &temp.path().join("second.sock")).await?;
    let current = restarted.evidence().await?;
    ensure!(current["processInstanceId"] != prior["processInstanceId"]);
    assert_eq!(
        current["authFingerprint"],
        codex_login::AuthManager::managed_account_fingerprint("account-b")
    );
    let stale = restarted
        .adopt(&prior, "failed-model-reset", "account-b")
        .await?;
    assert_eq!(stale["refusal"]["kind"], "processMismatch");
    let retry = restarted
        .adopt(&current, "retry-after-failed-reset", "account-b")
        .await?;
    assert_eq!(
        retry["status"]["phase"],
        "succeeded",
        "retry must prove reset succeeds on current B: {retry}; observed model URLs: {:?}",
        responses
            .received_requests()
            .await
            .unwrap_or_default()
            .iter()
            .filter(|r| r.url.path().contains("models"))
            .map(|r| r.url.to_string())
            .collect::<Vec<_>>()
    );
    restarted.kill().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn telemetry_preparation_failure_quarantines_and_restart_reconstructs_installed_b()
-> Result<()> {
    let temp = TempDir::new()?;
    let home = temp.path().join("home");
    std::fs::create_dir(&home)?;
    let responses = mock_backend().await;
    mock_config(&responses.uri()).write(&home)?;
    let config_path = home.join("config.toml");
    let working_config = std::fs::read_to_string(&config_path)?;
    write_account(&home, "account-a", "fixture-token-a")?;
    let mut server = Server::start(&home, &temp.path().join("first.sock")).await?;
    let prior = server.evidence().await?;

    // Valid config syntax, but actual exporter construction must read this
    // missing certificate. This is preparation failure, not an HTTP export
    // error masquerading as terminal exporter-shutdown evidence.
    let missing_ca = home.join("missing-telemetry-ca.pem");
    std::fs::write(
        &config_path,
        format!(
            "{working_config}\n[otel]\ntrace_exporter = {{ otlp-http = {{ endpoint = \"https://127.0.0.1:1/traces\", protocol = \"json\", tls = {{ ca-certificate = {} }} }} }}\n",
            serde_json::to_string(&missing_ca)?
        ),
    )?;
    write_account(&home, "account-b", "fixture-token-b")?;
    let failed = server
        .adopt(&prior, "failed-telemetry-prepare", "account-b")
        .await?;
    assert_eq!(failed["status"]["phase"], "quarantined");
    assert_eq!(failed["status"]["refusal"], "resetFailed");
    assert_eq!(
        failed["status"]["resultAuthFingerprint"],
        codex_login::AuthManager::managed_account_fingerprint("account-b")
    );
    let replay = server
        .adopt(&prior, "failed-telemetry-prepare", "account-b")
        .await?;
    assert_eq!(replay["refusal"]["kind"], "completedReplay");
    let recorded = server
        .request(
            "account/managedAuthTransition/read",
            json!({
                "contractVersion": 1,
                "transitionId": "failed-telemetry-prepare",
                "processInstanceId": prior["processInstanceId"]
            }),
        )
        .await?;
    assert_eq!(recorded["status"], failed["status"]);
    for sentinel in ["fixture-token-b", "account-b", "missing-telemetry-ca.pem"] {
        ensure!(!failed.to_string().contains(sentinel));
    }
    server.kill().await?;

    std::fs::write(&config_path, working_config)?;
    let mut restarted = Server::start(&home, &temp.path().join("second.sock")).await?;
    let current = restarted.evidence().await?;
    ensure!(current["processInstanceId"] != prior["processInstanceId"]);
    assert_eq!(
        current["authFingerprint"],
        codex_login::AuthManager::managed_account_fingerprint("account-b")
    );
    let stale = restarted
        .adopt(&prior, "failed-telemetry-prepare", "account-b")
        .await?;
    assert_eq!(stale["refusal"]["kind"], "processMismatch");
    let recovered = restarted
        .adopt(&current, "retry-after-telemetry-prepare", "account-b")
        .await?;
    assert_eq!(recovered["status"]["phase"], "succeeded");
    restarted.kill().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn telemetry_flush_failure_stays_quarantined_until_process_replacement() -> Result<()> {
    let temp = TempDir::new()?;
    let home = temp.path().join("home");
    std::fs::create_dir(&home)?;
    let responses = mock_backend().await;
    let collector = wiremock::MockServer::start().await;
    let restored = Arc::new(AtomicU8::new(0));
    let response_mode = Arc::clone(&restored);
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path("/metrics"))
        .respond_with(move |_: &wiremock::Request| {
            if response_mode.load(Ordering::SeqCst) == 0 {
                wiremock::ResponseTemplate::new(400).set_body_string("private-exporter-diagnostic")
            } else {
                wiremock::ResponseTemplate::new(200).set_body_json(json!({}))
            }
        })
        .mount(&collector)
        .await;
    mock_config(&responses.uri())
        .with_extra_config(&format!(
            "[analytics]\nenabled = true\n[otel]\nmetrics_exporter = {{ otlp-http = {{ endpoint = \"{}/metrics\", protocol = \"json\" }} }}",
            collector.uri()
        ))
        .write(&home)?;
    write_account(&home, "account-a", "fixture-token-a")?;
    let mut server = Server::start(&home, &temp.path().join("first.sock")).await?;
    let prior = server.evidence().await?;
    write_account(&home, "account-b", "fixture-token-b")?;
    let failed = server
        .adopt(&prior, "failed-metrics-flush", "account-b")
        .await?;
    assert_eq!(failed["status"]["phase"], "quarantined");
    assert_eq!(failed["status"]["refusal"], "resetFailed");
    assert_eq!(
        failed["status"]["resultAuthFingerprint"],
        codex_login::AuthManager::managed_account_fingerprint("account-b")
    );
    let rejected_requests = collector.received_requests().await.unwrap_or_default();
    ensure!(
        !rejected_requests.is_empty(),
        "real metrics export must have failed"
    );
    ensure!(!failed.to_string().contains("private-exporter-diagnostic"));
    // Restoring the endpoint cannot undo the SDK's consumed shutdown attempt.
    // A fresh, fully revalidated transition must replay that sticky failure.
    restored.store(1, Ordering::SeqCst);
    let current = server.evidence().await?;
    let still_failed = server
        .adopt(&current, "retry-sticky-metrics", "account-b")
        .await?;
    assert_eq!(still_failed["status"]["phase"], "quarantined");
    assert_eq!(still_failed["status"]["refusal"], "resetFailed");
    server.kill().await?;

    let before_restart = collector
        .received_requests()
        .await
        .unwrap_or_default()
        .len();
    let mut restarted = Server::start(&home, &temp.path().join("second.sock")).await?;
    let current = restarted.evidence().await?;
    ensure!(current["processInstanceId"] != prior["processInstanceId"]);
    assert_eq!(
        current["authFingerprint"],
        codex_login::AuthManager::managed_account_fingerprint("account-b")
    );
    let stale = restarted
        .adopt(&prior, "failed-metrics-flush", "account-b")
        .await?;
    assert_eq!(stale["refusal"]["kind"], "processMismatch");
    let recovered = restarted
        .adopt(&current, "fresh-metrics-owner", "account-b")
        .await?;
    assert_eq!(recovered["status"]["phase"], "succeeded");
    ensure!(
        collector
            .received_requests()
            .await
            .unwrap_or_default()
            .len()
            > before_restart
    );
    restarted.kill().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn independent_connection_cancels_draining_and_source_change_quarantines_prepared_adoption()
-> Result<()> {
    // Admitted and Draining have identical restart authority: both own only
    // process-local prepared work, with the installed cache unchanged. The
    // production start path moves from admit() to advance(Draining) without
    // intervening external I/O. A competing reader may observe Admitted while
    // its mutex acquisition delays advance, so it is not claimed unreachable;
    // the held Draining crash below exercises the same persisted/cache/owner
    // reconstruction state after that purely local phase/revision increment.
    for boundary in ["cancelled", "draining", "source-changed"] {
        let temp = TempDir::new()?;
        let home = temp.path().join("home");
        std::fs::create_dir(&home)?;
        let responses = mock_backend().await;
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let handler_entered = Arc::clone(&entered);
        let handler_release = Arc::clone(&release);
        let plugin_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let plugin_address = plugin_listener.local_addr()?;
        let app = axum::Router::new().route(
            "/backend-api/ps/plugins/workspace/created",
            axum::routing::get(move || {
                let entered = Arc::clone(&handler_entered);
                let release = Arc::clone(&handler_release);
                async move {
                    entered.notify_one();
                    release.notified().await;
                    axum::Json(json!({"plugins":[],"pagination":{}}))
                }
            }),
        );
        let _plugin_server = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
            axum::serve(plugin_listener, app).await
        }));
        MockResponsesConfig::new(&responses.uri())
            .with_root_config(&format!(
                "chatgpt_base_url = \"http://{plugin_address}/backend-api\""
            ))
            .with_provider_config("requires_openai_auth = true\nsupports_websockets = false")
            .enable_feature(codex_features::Feature::Plugins)
            .write(&home)?;
        write_account(&home, "account-a", "fixture-token-a")?;
        let mut server = Server::start(&home, &temp.path().join("first.sock")).await?;
        let prior = server.evidence().await?;
        let mut observer = Peer::connect(&temp.path().join("first.sock")).await?;
        server
            .socket
            .send(Message::Text(
                json!({"jsonrpc":"2.0","id":10001,"method":"plugin/share/list","params":{}})
                    .to_string()
                    .into(),
            ))
            .await?;
        timeout(WAIT, entered.notified())
            .await
            .context("real account-work permit never reached plugin I/O")?;
        write_account(&home, "account-b", "fixture-token-b")?;
        server.socket.send(Message::Text(json!({
            "jsonrpc":"2.0","id":10000,"method":"account/managedAuthTransition/start",
            "params":{"contractVersion":1,"transitionId":"held-drain",
                "processInstanceId":prior["processInstanceId"],"intent":"adoptManagedAuth",
                "expectedAuthRevision":prior["authRevision"],"expectedTransitionRevision":prior["transitionRevision"],
                "expectedAuthFingerprint":prior["authFingerprint"],
                "intendedResultAuthFingerprint":codex_login::AuthManager::managed_account_fingerprint("account-b")}
        }).to_string().into())).await?;
        timeout(WAIT, async {
            loop {
                let status = observer.request("account/managedAuthTransition/read", json!({"contractVersion":1,"transitionId":"held-drain","processInstanceId":prior["processInstanceId"]})).await?;
                if status["status"]["phase"] == "draining" { break Ok::<(),anyhow::Error>(()); }
                ensure!(status["status"]["phase"] != "succeeded", "held permit must prevent adoption");
                sleep(Duration::from_millis(10)).await;
            }
        }).await??;
        if boundary == "cancelled" || boundary == "draining" {
            if boundary == "cancelled" {
                let cancelled = observer.request("account/managedAuthTransition/cancel", json!({"contractVersion":1,"transitionId":"held-drain","processInstanceId":prior["processInstanceId"]})).await?;
                assert_eq!(
                    cancelled["status"]["phase"], "cancelled",
                    "second-connection cancel must execute while Start is pending: {cancelled}"
                );
                assert_eq!(cancelled["status"]["authRevision"], prior["authRevision"]);
            }
            // In the Draining row the permit remains held until after death,
            // proving the restart occurred before zero-drain/adoption.
            server.kill().await?;
            release.notify_one();
            let mut restarted = Server::start(&home, &temp.path().join("second.sock")).await?;
            let current = restarted.evidence().await?;
            ensure!(current["processInstanceId"] != prior["processInstanceId"]);
            assert_eq!(
                current["authFingerprint"],
                codex_login::AuthManager::managed_account_fingerprint("account-b")
            );
            let stale = restarted.adopt(&prior, "held-drain", "account-b").await?;
            assert_eq!(stale["refusal"]["kind"], "processMismatch");
            let forgotten = restarted.request("account/managedAuthTransition/read", json!({"contractVersion":1,"transitionId":"held-drain","processInstanceId":current["processInstanceId"]})).await?;
            assert_eq!(
                forgotten["type"], "refused",
                "{boundary} must not survive as fabricated history"
            );
            let retry = restarted
                .adopt(&current, "retry-current-b", "account-b")
                .await?;
            assert_eq!(
                retry["status"]["phase"], "succeeded",
                "truthful retry after {boundary}: {retry}"
            );
            restarted.kill().await?;
        } else {
            // B was already parsed and bound before admission. Changing bytes
            // while real account work drains must invalidate its later install.
            write_account(&home, "account-c", "fixture-token-c")?;
            release.notify_one();
            let terminal = timeout(WAIT, async {
                loop {
                    let status = observer.request("account/managedAuthTransition/read", json!({"contractVersion":1,"transitionId":"held-drain","processInstanceId":prior["processInstanceId"]})).await?;
                    if status["status"]["phase"] == "quarantined" { break Ok::<_,anyhow::Error>(status); }
                    ensure!(status["status"]["phase"] != "succeeded", "changed C cannot complete prepared B");
                    sleep(Duration::from_millis(10)).await;
                }
            }).await??;
            assert_eq!(
                terminal["status"]["resultAuthFingerprint"],
                prior["authFingerprint"]
            );
            server.kill().await?;
            let mut restarted = Server::start(&home, &temp.path().join("second.sock")).await?;
            let current = restarted.evidence().await?;
            ensure!(current["processInstanceId"] != prior["processInstanceId"]);
            assert_eq!(
                current["authFingerprint"],
                codex_login::AuthManager::managed_account_fingerprint("account-c")
            );
            let stale = restarted.adopt(&prior, "held-drain", "account-b").await?;
            assert_eq!(stale["refusal"]["kind"], "processMismatch");
            write_account(&home, "account-b", "fixture-token-b")?;
            let retry = restarted
                .adopt(&current, "retry-after-source-change", "account-b")
                .await?;
            assert_eq!(
                retry["status"]["phase"], "succeeded",
                "fresh retry must use current persisted/prepared truth: {retry}"
            );
            restarted.kill().await?;
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_logout_restart_reconstructs_absence_and_refuses_ineligible_retry() -> Result<()> {
    let temp = TempDir::new()?;
    let home = temp.path().join("home");
    std::fs::create_dir(&home)?;
    let responses = mock_backend().await;
    mock_config(&responses.uri()).write(&home)?;
    write_account(&home, "account-a", "fixture-token-a")?;
    let mut server = Server::start(&home, &temp.path().join("first.sock")).await?;
    let prior = server.evidence().await?;
    std::fs::remove_file(home.join("auth.json"))?;
    let logged_out = server.request("account/managedAuthTransition/start", json!({
        "contractVersion":1,"transitionId":"managed-logout","processInstanceId":prior["processInstanceId"],
        "intent":"adoptManagedLogout","expectedAuthRevision":prior["authRevision"],
        "expectedTransitionRevision":prior["transitionRevision"],"expectedAuthFingerprint":prior["authFingerprint"],
        "intendedResultAuthFingerprint":null
    })).await?;
    assert_eq!(
        logged_out["status"]["phase"], "succeeded",
        "deliberate eligible managed logout must complete: {logged_out}"
    );
    assert_eq!(logged_out["status"]["resultAuthFingerprint"], Value::Null);
    assert_eq!(
        server.account_events_before_response.len(),
        1,
        "logout projection must precede its Start response, without login-completed"
    );
    assert_eq!(
        server.account_events_before_response[0]["method"],
        "account/updated"
    );
    assert_eq!(
        server.account_events_before_response[0]["params"],
        json!({"authMode":null,"planType":null})
    );
    ensure!(logged_out["status"]["authRevision"].as_u64() > prior["authRevision"].as_u64());
    server.kill().await?;

    let mut restarted = Server::start(&home, &temp.path().join("second.sock")).await?;
    let absent = restarted.evidence().await?;
    ensure!(absent["processInstanceId"] != prior["processInstanceId"]);
    assert_eq!(absent["authFingerprint"], Value::Null);
    let stale = restarted.request("account/managedAuthTransition/read", json!({"contractVersion":1,"transitionId":"managed-logout","processInstanceId":prior["processInstanceId"]})).await?;
    assert_eq!(stale["refusal"]["kind"], "processMismatch");
    let forgotten = restarted.request("account/managedAuthTransition/read", json!({"contractVersion":1,"transitionId":"managed-logout","processInstanceId":absent["processInstanceId"]})).await?;
    assert_eq!(
        forgotten["type"], "refused",
        "restart must not manufacture the prior logout acknowledgement"
    );
    // A new ordinary transition from logged-out current state is ineligible
    // under R002 even when a separate writer subsequently persists managed B.
    write_account(&home, "account-b", "fixture-token-b")?;
    let ineligible = restarted
        .adopt(&absent, "retry-from-absent", "account-b")
        .await?;
    assert_eq!(ineligible["type"], "refused");
    assert_eq!(ineligible["refusal"]["authFingerprint"], Value::Null);
    assert_eq!(
        ineligible["refusal"]["authRevision"],
        absent["authRevision"]
    );
    assert_eq!(
        ineligible["refusal"]["transitionRevision"],
        absent["transitionRevision"]
    );
    restarted.kill().await?;

    // Re-establish an eligible current state through the normal startup path;
    // this is an explicit fresh process, not a privileged recovery shortcut.
    write_account(&home, "account-a", "fixture-token-a")?;
    let mut eligible = Server::start(&home, &temp.path().join("third.sock")).await?;
    let current = eligible.evidence().await?;
    assert_eq!(
        current["authFingerprint"],
        codex_login::AuthManager::managed_account_fingerprint("account-a")
    );
    write_account(&home, "account-b", "fixture-token-b")?;
    let retry = eligible
        .adopt(&current, "fresh-eligible-retry", "account-b")
        .await?;
    assert_eq!(
        retry["status"]["phase"], "succeeded",
        "eligible retry must run real adoption/reset: {retry}"
    );
    assert_eq!(
        retry["status"]["resultAuthFingerprint"],
        codex_login::AuthManager::managed_account_fingerprint("account-b")
    );
    eligible.kill().await?;
    Ok(())
}
