#![cfg(test)]

//! Unit tests for the crate and its related test suites.

use super::LogFormat;
use super::ShutdownAction;
use super::ShutdownSignal;
use super::ShutdownState;
#[cfg(debug_assertions)]
use super::loader_overrides_with_test_user_config_file;
use super::turn_admission::TurnAdmission;
#[cfg(debug_assertions)]
use codex_config::LoaderOverrides;
#[cfg(debug_assertions)]
use codex_utils_absolute_path::AbsolutePathBuf;
use pretty_assertions::assert_eq;

#[test]
fn shutdown_waits_for_admitted_requests_but_force_remains_available() {
    let admission = TurnAdmission::default();
    let active = admission.subscribe_active();
    let in_flight = admission.admit().expect("request admitted");
    let mut shutdown = ShutdownState::default();
    let connection_count = 1;
    let running_turn_count = 0;
    shutdown.on_signal(
        ShutdownSignal::Forceable,
        connection_count,
        running_turn_count,
        &admission,
    );
    assert!(matches!(
        shutdown.update(running_turn_count, *active.borrow(), connection_count),
        ShutdownAction::Noop
    ));
    shutdown.on_signal(
        ShutdownSignal::Forceable,
        connection_count,
        running_turn_count,
        &admission,
    );
    assert!(shutdown.forced());
    assert!(matches!(
        shutdown.update(running_turn_count, *active.borrow(), connection_count),
        ShutdownAction::Finish
    ));
    drop(in_flight);
}

#[test]
fn log_format_from_env_value_matches_json_values_case_insensitively() {
    assert_eq!(LogFormat::from_env_value(Some("json")), LogFormat::Json);
    assert_eq!(LogFormat::from_env_value(Some("JSON")), LogFormat::Json);
    assert_eq!(LogFormat::from_env_value(Some("  Json  ")), LogFormat::Json);
}

#[test]
fn log_format_from_env_value_defaults_for_non_json_values() {
    assert_eq!(
        LogFormat::from_env_value(/*value*/ None),
        LogFormat::Default
    );
    assert_eq!(LogFormat::from_env_value(Some("")), LogFormat::Default);
    assert_eq!(LogFormat::from_env_value(Some("text")), LogFormat::Default);
    assert_eq!(LogFormat::from_env_value(Some("jsonl")), LogFormat::Default);
}

#[cfg(debug_assertions)]
#[test]
fn debug_test_user_config_file_overrides_loader_path() {
    let path = std::env::temp_dir().join("codex-app-server-test-config.toml");
    let loader_overrides =
        loader_overrides_with_test_user_config_file(LoaderOverrides::default(), Some(path.clone()))
            .expect("test config path should be valid");

    assert_eq!(
        loader_overrides.user_config_path,
        Some(AbsolutePathBuf::from_absolute_path(path).expect("absolute test path"))
    );
}

#[path = "stderr_logging_tests.rs"]
mod stderr_logging_tests;

#[cfg(unix)]
#[test]
fn hosting_control_endpoint_uses_the_bound_absolute_listener_path() {
    use super::AppServerTransport;
    use super::mcp_control_endpoint_uri;
    use codex_utils_absolute_path::AbsolutePathBuf;

    assert_eq!(mcp_control_endpoint_uri(&AppServerTransport::Stdio), None);
    assert_eq!(mcp_control_endpoint_uri(&AppServerTransport::Off), None);
    assert_eq!(
        mcp_control_endpoint_uri(&AppServerTransport::WebSocket {
            bind_address: "127.0.0.1:0".parse().unwrap(),
        }),
        None
    );
    assert!(AbsolutePathBuf::from_absolute_path_checked("relative.sock").is_err());
    assert_eq!(
        mcp_control_endpoint_uri(&AppServerTransport::UnixSocket {
            socket_path: AbsolutePathBuf::from_absolute_path("/tmp/host socket?#.sock").unwrap(),
        }),
        Some("unix:///tmp/host%20socket%3F%23.sock".to_string()),
    );
    // 0.169 resolves CLI-relative listen arguments before the transport binds.
    // Disclosure uses that resolved listener, never the raw relative argument.
    let resolved = AppServerTransport::from_listen_url("unix://relative.sock").unwrap();
    let AppServerTransport::UnixSocket { socket_path } = &resolved else {
        panic!("Unix listener");
    };
    assert!(socket_path.is_absolute());
    let disclosed = url::Url::parse(&mcp_control_endpoint_uri(&resolved).unwrap()).unwrap();
    assert_eq!(disclosed.scheme(), "unix");
    let file_uri = format!("file:{}", disclosed.as_str().strip_prefix("unix:").unwrap());
    assert_eq!(
        url::Url::parse(&file_uri).unwrap().to_file_path().unwrap(),
        socket_path.to_path_buf()
    );
}

#[test]
fn router_classifies_dropped_replies_by_positive_close() {
    use super::account_reply_drop;
    use crate::transport::ReplyDrop;
    use codex_app_server_transport::AbandonedOutput;
    use codex_app_server_transport::OwnerExpiry;
    let expiry = OwnerExpiry::never();
    let mut refused = std::collections::HashSet::new();
    let stdio = super::ConnectionId(1);
    let websocket = super::ConnectionId(2);

    // A closed websocket (positive close) and an unknown recipient are
    // undeliverable, not abandoned.
    account_reply_drop(
        &expiry,
        &mut refused,
        ReplyDrop::WriterRefused {
            connection_id: websocket,
            disconnectable: true,
        },
    );
    account_reply_drop(&expiry, &mut refused, ReplyDrop::Unregistered(websocket));
    assert_eq!(expiry.abandoned_output(), AbandonedOutput::default());
    assert_eq!(expiry.undeliverable_after_disconnect().messages, 2);

    // A stdio writer gone without a positive close: abandoned, now and later.
    account_reply_drop(
        &expiry,
        &mut refused,
        ReplyDrop::WriterRefused {
            connection_id: stdio,
            disconnectable: false,
        },
    );
    account_reply_drop(&expiry, &mut refused, ReplyDrop::Unregistered(stdio));
    assert_eq!(expiry.abandoned_output().messages, 2);
}
