//! Hidden, same-image managed-auth protocol client.
//!
//! Repository-owned profile wrappers use this command only after they have
//! performed their independent target-evidence and credential-writer steps.
//! The helper receives opaque revisions/fingerprints and control metadata; it
//! never accepts or reads credential bytes.

use anyhow::Context;
use anyhow::Result;
use clap::Args;
use clap::ValueEnum;
use codex_app_server_client::RemoteAppServerClient;
use codex_app_server_client::RemoteAppServerConnectArgs;
use codex_app_server_client::RemoteAppServerEndpoint;
use codex_app_server_protocol::CancelManagedTransitionParams;
use codex_app_server_protocol::CancelManagedTransitionResponse;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::MANAGED_AUTH_TRANSITION_CONTRACT_VERSION;
use codex_app_server_protocol::ManagedTransitionIntent;
use codex_app_server_protocol::ReadManagedTransitionParams;
use codex_app_server_protocol::ReadManagedTransitionResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::StartManagedTransitionParams;
use codex_app_server_protocol::StartManagedTransitionResponse;
use codex_utils_absolute_path::AbsolutePathBuf;
use serde::Serialize;
use std::time::SystemTime;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ManagedAuthAction {
    Read,
    Start,
    Cancel,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ManagedAuthIntent {
    Login,
    Logout,
}

#[derive(Debug, Args)]
pub(crate) struct ManagedAuthCommand {
    /// Exact Unix app-server socket selected by the profile wrapper.
    #[arg(long = "socket")]
    socket_path: String,
    #[arg(long = "action", value_enum)]
    action: ManagedAuthAction,
    #[arg(long = "transition-id")]
    transition_id: String,
    #[arg(long = "process-instance-id")]
    process_instance_id: String,
    #[arg(long = "intent", value_enum)]
    intent: Option<ManagedAuthIntent>,
    #[arg(long = "expected-auth-revision")]
    expected_auth_revision: Option<u64>,
    #[arg(long = "expected-transition-revision")]
    expected_transition_revision: Option<u64>,
    #[arg(long = "expected-auth-fingerprint")]
    expected_auth_fingerprint: Option<String>,
    #[arg(long = "intended-result-auth-fingerprint")]
    intended_result_auth_fingerprint: Option<String>,
}

fn request_id(action: ManagedAuthAction) -> RequestId {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    RequestId::String(format!(
        "managed-auth-{}-{}-{}",
        std::process::id(),
        nanos,
        match action {
            ManagedAuthAction::Read => "read",
            ManagedAuthAction::Start => "start",
            ManagedAuthAction::Cancel => "cancel",
        }
    ))
}

fn to_intent(value: ManagedAuthIntent) -> ManagedTransitionIntent {
    match value {
        ManagedAuthIntent::Login => ManagedTransitionIntent::AdoptManagedAuth,
        ManagedAuthIntent::Logout => ManagedTransitionIntent::AdoptManagedLogout,
    }
}

async fn connect(socket_path: String) -> Result<RemoteAppServerClient> {
    let socket_path = AbsolutePathBuf::from_absolute_path_checked(&socket_path)
        .with_context(|| format!("--socket must be an absolute path: {socket_path}"))?;
    RemoteAppServerClient::connect(RemoteAppServerConnectArgs {
        endpoint: RemoteAppServerEndpoint::UnixSocket { socket_path },
        client_name: "codex-managed-auth".to_owned(),
        client_version: env!("CARGO_PKG_VERSION").to_owned(),
        experimental_api: true,
        mcp_server_openai_form_elicitation: false,
        interactive_client: true,
        opt_out_notification_methods: Vec::new(),
        channel_capacity: 8,
    })
    .await
    .context("connect to the selected app-server Unix socket")
}

async fn print_response<T: Serialize>(response: T) -> Result<()> {
    println!("{}", serde_json::to_string(&response)?);
    Ok(())
}

#[expect(
    clippy::expect_used,
    reason = "start arguments are validated before connecting and matched by the same action"
)]
pub(crate) async fn run(command: ManagedAuthCommand) -> Result<()> {
    let ManagedAuthCommand {
        socket_path,
        action,
        transition_id,
        process_instance_id,
        intent: requested_intent,
        expected_auth_revision,
        expected_transition_revision,
        expected_auth_fingerprint,
        intended_result_auth_fingerprint,
    } = command;
    anyhow::ensure!(
        !matches!(requested_intent, Some(ManagedAuthIntent::Logout))
            || intended_result_auth_fingerprint.is_none(),
        "logout does not accept --intended-result-auth-fingerprint"
    );
    let start_args = match action {
        ManagedAuthAction::Start => Some((
            requested_intent.context("--intent is required for --action start")?,
            expected_auth_revision
                .context("--expected-auth-revision is required for --action start")?,
            expected_transition_revision
                .context("--expected-transition-revision is required for --action start")?,
        )),
        ManagedAuthAction::Read | ManagedAuthAction::Cancel => None,
    };
    let client = connect(socket_path).await?;
    // Keep request errors inside this future so they cannot bypass shutdown.
    let result = async {
        match action {
            ManagedAuthAction::Read => {
                let response = client
                    .request_typed::<ReadManagedTransitionResponse>(
                        ClientRequest::ManagedTransitionRead {
                            request_id: request_id(action),
                            params: ReadManagedTransitionParams {
                                contract_version: MANAGED_AUTH_TRANSITION_CONTRACT_VERSION,
                                transition_id,
                                process_instance_id,
                            },
                        },
                    )
                    .await
                    .context("read managed-auth transition status")?;
                print_response(response).await
            }
            ManagedAuthAction::Start => {
                let (requested_intent, expected_auth_revision, expected_transition_revision) =
                    start_args.expect("start arguments were validated before connecting");
                let response = client
                    .request_typed::<StartManagedTransitionResponse>(
                        ClientRequest::ManagedTransitionStart {
                            request_id: request_id(action),
                            params: StartManagedTransitionParams {
                                contract_version: MANAGED_AUTH_TRANSITION_CONTRACT_VERSION,
                                transition_id,
                                process_instance_id,
                                intent: to_intent(requested_intent),
                                expected_auth_revision,
                                expected_transition_revision,
                                expected_auth_fingerprint,
                                intended_result_auth_fingerprint,
                            },
                        },
                    )
                    .await
                    .context("start managed-auth transition")?;
                print_response(response).await
            }
            ManagedAuthAction::Cancel => {
                let response = client
                    .request_typed::<CancelManagedTransitionResponse>(
                        ClientRequest::ManagedTransitionCancel {
                            request_id: request_id(action),
                            params: CancelManagedTransitionParams {
                                contract_version: MANAGED_AUTH_TRANSITION_CONTRACT_VERSION,
                                transition_id,
                                process_instance_id,
                            },
                        },
                    )
                    .await
                    .context("cancel managed-auth transition")?;
                print_response(response).await
            }
        }
    }
    .await;
    let shutdown = client
        .shutdown()
        .await
        .context("shutdown managed-auth protocol client");
    // A secondary shutdown failure must not replace the request's failure.
    result.and(shutdown)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn logout_fingerprint_is_rejected_before_connect() {
        let error = run(ManagedAuthCommand {
            socket_path: "not-an-absolute-socket".to_owned(),
            action: ManagedAuthAction::Start,
            transition_id: "test-transition".to_owned(),
            process_instance_id: "test-process".to_owned(),
            intent: Some(ManagedAuthIntent::Logout),
            expected_auth_revision: Some(0),
            expected_transition_revision: Some(0),
            expected_auth_fingerprint: None,
            intended_result_auth_fingerprint: Some("synthetic-fingerprint".to_owned()),
        })
        .await
        .expect_err("logout fingerprint must refuse before socket validation");
        assert_eq!(
            error.to_string(),
            "logout does not accept --intended-result-auth-fingerprint"
        );
    }

    #[tokio::test]
    async fn required_start_arguments_are_rejected_before_connect() {
        for (intent, expected_auth_revision, expected_transition_revision, expected_error) in [
            (
                None,
                Some(0),
                Some(0),
                "--intent is required for --action start",
            ),
            (
                Some(ManagedAuthIntent::Login),
                None,
                Some(0),
                "--expected-auth-revision is required for --action start",
            ),
            (
                Some(ManagedAuthIntent::Login),
                Some(0),
                None,
                "--expected-transition-revision is required for --action start",
            ),
        ] {
            let error = run(ManagedAuthCommand {
                socket_path: "not-an-absolute-socket".to_owned(),
                action: ManagedAuthAction::Start,
                transition_id: "test-transition".to_owned(),
                process_instance_id: "test-process".to_owned(),
                intent,
                expected_auth_revision,
                expected_transition_revision,
                expected_auth_fingerprint: None,
                intended_result_auth_fingerprint: Some("synthetic-fingerprint".to_owned()),
            })
            .await
            .expect_err("required start arguments must refuse before socket validation");
            assert_eq!(error.to_string(), expected_error);
        }
    }

    #[test]
    fn managed_intents_are_token_free_and_explicit() {
        assert_eq!(
            to_intent(ManagedAuthIntent::Login),
            ManagedTransitionIntent::AdoptManagedAuth
        );
        assert_eq!(
            to_intent(ManagedAuthIntent::Logout),
            ManagedTransitionIntent::AdoptManagedLogout
        );
    }

    #[test]
    fn request_ids_include_action_without_credentials() {
        let read = request_id(ManagedAuthAction::Read).to_string();
        let cancel = request_id(ManagedAuthAction::Cancel).to_string();
        assert!(read.contains("-read"));
        assert!(cancel.contains("-cancel"));
        assert!(!read.contains("token"));
        assert!(!cancel.contains("token"));
    }
}
