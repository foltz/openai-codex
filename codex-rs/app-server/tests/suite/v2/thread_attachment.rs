use anyhow::Result;
use app_test_support::DEFAULT_CLIENT_NAME;
use app_test_support::TestAppServer;
use codex_app_server_protocol::ClientInfo;
use codex_app_server_protocol::InitializeCapabilities;
use codex_app_server_protocol::JSONRPCMessage;
use codex_app_server_protocol::ThreadAttachmentListParams;
use codex_app_server_protocol::ThreadAttachmentListResponse;
use pretty_assertions::assert_eq;
use std::time::Duration;
use tokio::time::timeout;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test]
async fn thread_attachment_list_is_empty_without_server_entitlement() -> Result<()> {
    let mut app_server = TestAppServer::builder().without_auto_env().build().await?;
    let initialized = app_server
        .initialize_with_capabilities(
            ClientInfo {
                name: DEFAULT_CLIENT_NAME.to_string(),
                title: None,
                version: "0.1.0".to_string(),
            },
            Some(InitializeCapabilities {
                experimental_api: true,
                request_attestation: false,
                interactive_client: true,
                opt_out_notification_methods: None,
                mcp_server_openai_form_elicitation: false,
                extensions: None,
            }),
        )
        .await?;
    assert!(matches!(initialized, JSONRPCMessage::Response(_)));

    let request_id = app_server
        .send_thread_attachment_list_request(ThreadAttachmentListParams::default())
        .await?;
    let response: ThreadAttachmentListResponse =
        timeout(DEFAULT_TIMEOUT, app_server.read_response(request_id)).await??;

    assert!(!response.generation.is_empty());
    assert_eq!(response.revision, 0);
    assert!(response.entries.is_empty());
    Ok(())
}
