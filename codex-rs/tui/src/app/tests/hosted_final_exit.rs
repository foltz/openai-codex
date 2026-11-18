//! Final-exit consumer witness: the real `AppServerSession` final-exit path
//! against a proxied app-server daemon (a real embedded app-server behind a
//! loopback WebSocket), with the TUI's real hosted tools server.

use super::session_lifecycle_requests::HistoryCapabilities;
use super::session_lifecycle_requests::RealtimeRequestBehavior;
use super::session_lifecycle_requests::RecordedRequests;
use super::session_lifecycle_requests::recorded_params;
use super::session_lifecycle_requests::start_recording_app_server_with_realtime_speech;
use super::*;
use crate::app_server_session::HostedRetireOutcome;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::McpHostedEndpointDeclareResponse;
use codex_config::LoaderOverrides;
use pretty_assertions::assert_eq;
use std::time::Duration;
use std::time::Instant;
use tokio::task::JoinHandle;

const DECLARE: &str = "kcf/mcpServer/hostedEndpoint/declare";
const RETIRE: &str = "kcf/mcpServer/hostedEndpoint/retire";
/// Scheduling slack only; the budget itself is never extended.
const EPSILON: Duration = Duration::from_millis(500);

struct Hosted {
    app_server: AppServerSession,
    requests: RecordedRequests,
    proxy: JoinHandle<Result<()>>,
    _keep_alive: Box<dyn std::any::Any>,
}

/// A remote session with the TUI's hosted tools server and one thread whose
/// overrides name it, so the daemon attributes the endpoint to this connection.
async fn hosted_session(behavior: RealtimeRequestBehavior) -> Result<Hosted> {
    let (mut app, events, ops) = Box::pin(make_test_app_with_channels()).await;
    let codex_home = tempdir()?;
    app.config.codex_home = codex_home.path().to_path_buf().abs();
    app.config
        .features
        .set_enabled(codex_features::Feature::Plugins, false)?;
    app.config.sqlite = codex_state::SqliteConfig::new_for_testing(codex_home.path().abs());
    let (mut app_server, requests, proxy) =
        Box::pin(start_recording_app_server_with_realtime_speech(
            &app.config,
            HistoryCapabilities::Current,
            /*blocked_thread_list*/ None,
            /*failed_thread_name*/ None,
            crate::app_server_session::ThreadParamsMode::Embedded,
            behavior,
            LoaderOverrides::without_managed_config_for_tests(),
        ))
        .await?;
    Box::pin(app_server.start_dynamic_tool_mcp(
        app.config.clone(),
        app.app_event_tx.clone(),
        app.dynamic_tool_status_updates.clone(),
    ))
    .await?;
    let started = Box::pin(app_server.start_thread(&app.config)).await?;
    assert!(
        started.task_tools_available,
        "the daemon path hosts task tools"
    );
    // The proxy's embedded threads retire after remote client close. Keep
    // their hosted MCP endpoint alive until that shutdown is observed.
    let tool_transport = app_server.thread_tool_transport();
    Ok(Hosted {
        app_server,
        requests,
        proxy,
        _keep_alive: Box::new((app, events, ops, codex_home, tool_transport)),
    })
}

#[tokio::test]
async fn final_exit_retires_the_hosted_endpoint_within_one_budget() -> Result<()> {
    let Hosted {
        app_server,
        requests,
        proxy,
        _keep_alive,
    } = hosted_session(RealtimeRequestBehavior::Forward).await?;
    let budget = Duration::from_secs(10);
    let started = Instant::now();

    let exit = crate::app_server_session::final_exit_sessions([app_server], budget).await;

    assert!(started.elapsed() <= budget, "{:?}", started.elapsed());
    assert!(
        matches!(exit.entries[0].1, HostedRetireOutcome::Complete { .. }),
        "{:?}",
        exit.entries[0].1
    );
    assert!(exit.entries[0].1.is_clean());
    assert!(exit.entries[0].2.is_closed(), "{:?}", exit.entries[0].2);
    assert_eq!(recorded_params(&requests, DECLARE).len(), 1);
    assert_eq!(recorded_params(&requests, RETIRE).len(), 1);
    proxy.await??;
    Ok(())
}

#[tokio::test]
async fn hung_retirement_times_out_at_the_single_original_deadline() -> Result<()> {
    let Hosted {
        app_server,
        requests,
        proxy,
        _keep_alive,
    } = hosted_session(RealtimeRequestBehavior::StallHostedRetire).await?;
    let budget = Duration::from_secs(1);
    let started = Instant::now();

    let exit = crate::app_server_session::final_exit_sessions([app_server], budget).await;

    // Declare, retire, client close and worker join all share one deadline.
    let elapsed = started.elapsed();
    assert!(elapsed <= budget + EPSILON, "final exit ran {elapsed:?}");
    assert_eq!(exit.entries[0].1, HostedRetireOutcome::TimedOut);
    assert!(!exit.entries[0].1.is_clean());
    assert_eq!(recorded_params(&requests, RETIRE).len(), 1);
    assert!(
        !exit.entries[0].2.is_closed(),
        "a close never requested before the deadline is not claimed: {:?}",
        exit.entries[0].2
    );
    let _ = proxy.await;
    Ok(())
}

#[tokio::test]
async fn retained_handles_cannot_extend_exit_and_only_teardown_is_admitted() -> Result<()> {
    let Hosted {
        app_server,
        requests,
        proxy,
        _keep_alive,
    } = hosted_session(RealtimeRequestBehavior::StallHostedRetire).await?;
    let retained = app_server.request_handle();
    let budget = Duration::from_secs(2);
    let started = Instant::now();

    let (exit, (ordinary, teardown)) = tokio::join!(
        crate::app_server_session::final_exit_sessions([app_server], budget),
        async {
            // The retire is sent only after the final-exit edge.
            while recorded_params(&requests, RETIRE).is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let ordinary = retained
                .request_typed::<serde_json::Value>(ClientRequest::ThreadLoadedList {
                    request_id: codex_app_server_protocol::RequestId::String(
                        "ordinary".to_string(),
                    ),
                    params: codex_app_server_protocol::ThreadLoadedListParams::default(),
                })
                .await;
            let teardown = retained
                .request_typed::<McpHostedEndpointDeclareResponse>(
                    ClientRequest::McpHostedEndpointDeclare {
                        request_id: codex_app_server_protocol::RequestId::String(
                            "teardown".to_string(),
                        ),
                        params: codex_app_server_protocol::McpHostedEndpointDeclareParams {
                            url: "http://127.0.0.1:1/mcp".to_string(),
                            authorization: "Bearer unrelated".to_string(),
                            retirement_capability: "unrelated".to_string(),
                            timeout_ms: 500,
                        },
                    },
                )
                .await;
            (ordinary, teardown)
        }
    );

    let elapsed = started.elapsed();
    assert!(elapsed <= budget + EPSILON, "final exit ran {elapsed:?}");
    assert_eq!(exit.entries[0].1, HostedRetireOutcome::TimedOut);
    assert!(
        ordinary.is_err(),
        "ordinary requests are refused at the edge"
    );
    assert!(
        recorded_params(&requests, "thread/loaded/list").is_empty(),
        "the refused request never reached the daemon"
    );
    assert!(
        matches!(
            teardown,
            Ok(McpHostedEndpointDeclareResponse::Refused { .. })
        ),
        "owned teardown is still admitted: {teardown:?}"
    );
    assert_eq!(recorded_params(&requests, DECLARE).len(), 2);
    // The retained handle outlives the exit without keeping the worker alive.
    drop(retained);
    let _ = proxy.await;
    Ok(())
}

#[tokio::test]
async fn older_daemon_without_hosted_methods_is_unsupported_never_clean() -> Result<()> {
    let Hosted {
        app_server,
        requests,
        proxy,
        _keep_alive,
    } = hosted_session(RealtimeRequestBehavior::HostedUnsupported).await?;

    let exit =
        crate::app_server_session::final_exit_sessions([app_server], Duration::from_secs(10)).await;

    assert!(
        matches!(exit.entries[0].1, HostedRetireOutcome::Unsupported { .. }),
        "{:?}",
        exit.entries[0].1
    );
    assert!(!exit.entries[0].1.is_clean());
    assert!(exit.entries[0].2.is_closed(), "{:?}", exit.entries[0].2);
    assert!(recorded_params(&requests, RETIRE).is_empty());
    proxy.await??;
    Ok(())
}

#[tokio::test]
async fn all_backends_are_latched_before_any_retire_wait_and_share_one_deadline() -> Result<()> {
    let a = hosted_session(RealtimeRequestBehavior::StallHostedRetire).await?;
    let b = hosted_session(RealtimeRequestBehavior::StallHostedRetire).await?;
    let retained_b = b.app_server.request_handle();
    let budget = Duration::from_secs(2);
    let started = Instant::now();
    let (exit, ordinary) = tokio::join!(
        crate::app_server_session::final_exit_sessions([a.app_server, b.app_server], budget),
        async {
            tokio::time::timeout(Duration::from_secs(5), async {
                while recorded_params(&a.requests, RETIRE).is_empty() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("first backend reached its retirement wait");
            retained_b
                .request_typed::<serde_json::Value>(ClientRequest::ThreadLoadedList {
                    request_id: codex_app_server_protocol::RequestId::String(
                        "post-edge-b".to_owned(),
                    ),
                    params: codex_app_server_protocol::ThreadLoadedListParams::default(),
                })
                .await
        }
    );
    assert!(
        ordinary.is_err(),
        "backend B must be latched before waiting on A"
    );
    assert!(recorded_params(&b.requests, "thread/loaded/list").is_empty());
    assert_eq!(exit.entries.len(), 2);
    assert!(
        exit.entries
            .iter()
            .all(|(_, hosted, _)| *hosted == HostedRetireOutcome::TimedOut)
    );
    assert!(!exit.is_clean());
    assert!(
        started.elapsed() <= budget + EPSILON,
        "no per-backend budget restart"
    );
    for requests in [&a.requests, &b.requests] {
        assert_eq!(recorded_params(requests, DECLARE).len(), 1);
        assert_eq!(recorded_params(requests, RETIRE).len(), 1);
    }
    drop(retained_b);
    let _ = a.proxy.await;
    let _ = b.proxy.await;
    Ok(())
}
