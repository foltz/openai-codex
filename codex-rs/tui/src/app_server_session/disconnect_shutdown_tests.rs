//! Ordinary remote shutdown acknowledges only the exact disconnect it already reported.

use super::*;
use futures::SinkExt;
use futures::StreamExt;
use tokio_tungstenite::tungstenite::Message;

/// A peer that completes the initialize handshake and then drops the
/// connection without a closing handshake. The task ends only after the drop.
async fn session_with_resetting_peer()
-> color_eyre::Result<(AppServerSession, tokio::task::JoinHandle<()>)> {
    session_with_resetting_peer_in_host(None).await
}

pub(crate) async fn session_with_resetting_peer_in_host(
    host: Option<std::sync::Arc<codex_app_server_client::InProcessHost>>,
) -> color_eyre::Result<(AppServerSession, tokio::task::JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = crate::resolve_remote_addr(&format!("ws://{}", listener.local_addr()?))?;
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut socket = tokio_tungstenite::accept_async(stream)
            .await
            .expect("websocket handshake");
        let Some(Ok(Message::Text(text))) = socket.next().await else {
            panic!("expected the initialize request");
        };
        let initialize: serde_json::Value = serde_json::from_str(&text).expect("json");
        assert_eq!(initialize["method"], "initialize");
        let reply = serde_json::json!({
            "jsonrpc": "2.0",
            "id": initialize["id"],
            "result": {"userAgent": "reset-test/0.0.0", "codexHome": "/tmp/test"},
        });
        socket
            .send(Message::Text(reply.to_string().into()))
            .await
            .expect("initialize reply");
        let initialized = socket.next().await;
        assert!(
            matches!(initialized, Some(Ok(Message::Text(_)))),
            "expected the initialized notification: {initialized:?}"
        );
        drop(socket);
    });
    let client = crate::connect_remote_app_server(endpoint).await?;
    let client = match (client, host) {
        (AppServerClient::Remote(client), Some(host)) => {
            AppServerClient::Remote(client.with_worker_custody(host)?)
        }
        (client, _) => client,
    };
    let session = AppServerSession::new(
        crate::backend_id::BackendId::new(),
        client,
        ThreadParamsMode::Remote,
    );
    Ok((session, peer))
}

async fn next_disconnect(session: &mut AppServerSession) -> String {
    let event = tokio::time::timeout(Duration::from_secs(/*secs*/ 5), session.next_event())
        .await
        .expect("the session reports the reset");
    match event.map(|event| event.event) {
        Some(AppServerEvent::Disconnected { message }) => message,
        other => panic!("expected a disconnect: {other:?}"),
    }
}

#[tokio::test]
async fn reported_disconnect_releases_the_session_close() -> color_eyre::Result<()> {
    let (mut session, peer) = session_with_resetting_peer().await?;
    peer.await?;
    let message = next_disconnect(&mut session).await;
    assert!(message.contains("without closing handshake"), "{message}");

    session.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn unreported_reset_fails_the_session_close() -> color_eyre::Result<()> {
    let (session, peer) = session_with_resetting_peer().await?;
    peer.await?;
    wait_for_unreported_peer_reset(&session).await;

    let error = session
        .shutdown()
        .await
        .expect_err("a reset the session never reported is not a clean close");

    assert!(
        matches!(
            error.kind(),
            std::io::ErrorKind::InvalidData | std::io::ErrorKind::BrokenPipe
        ),
        "{error}"
    );
    assert!(
        error.to_string().starts_with("remote app server at"),
        "{error}"
    );
    Ok(())
}

/// Wait for the actual client worker's failure without reporting its event.
/// A peer task ending alone is not evidence that the client has read the reset.
pub(crate) async fn wait_for_unreported_peer_reset(session: &AppServerSession) {
    // Positive gate without consuming events: a request fails only once the
    // worker has stopped, after it recorded its terminal failure.
    let handle = session.request_handle();
    let request = tokio::time::timeout(
        Duration::from_secs(5),
        handle.request_typed::<codex_app_server_protocol::UserVerificationStatusResponse>(
            ClientRequest::UserVerificationStatus {
                request_id: RequestId::String("after-reset".to_string()),
                params: codex_app_server_protocol::UserVerificationStatusParams {},
            },
        ),
    )
    .await
    .expect("the worker observes the already closed peer");
    assert!(
        matches!(request, Err(TypedRequestError::Transport { .. })),
        "{request:?}"
    );
}
