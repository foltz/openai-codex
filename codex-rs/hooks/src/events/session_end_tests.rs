use std::collections::HashMap;

use codex_protocol::ThreadId;
use codex_protocol::protocol::HookEventName;
use codex_protocol::protocol::HookOutputEntryKind;
use codex_protocol::protocol::HookRunStatus;
use codex_protocol::protocol::HookSource;
use codex_utils_absolute_path::test_support::PathBufExt;
use codex_utils_absolute_path::test_support::test_path_buf;
use pretty_assertions::assert_eq;

use super::SessionEndReason;
use super::SessionEndRequest;
use super::parse_completed;
use super::preview;
use super::run;
use super::run_with_serializer;
use super::select_handlers;
use crate::engine::ClaudeHooksEngine;
use crate::engine::CommandShell;
use crate::engine::ConfiguredHandler;
use crate::engine::HandlerRunResult;
use crate::engine::command_runner::CommandHookRuntime;

#[test]
fn session_end_matches_other_reason() {
    let selected = preview(
        &[
            ConfiguredHandler {
                display_order: 0,
                ..handler(Some("clear"))
            },
            ConfiguredHandler {
                display_order: 1,
                ..handler(Some("other"))
            },
            ConfiguredHandler {
                display_order: 2,
                ..handler(/*matcher*/ None)
            },
        ],
        SessionEndReason::Other,
    );

    assert_eq!(
        selected
            .iter()
            .map(|run| run.display_order)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[test]
fn clear_session_end_requires_an_exact_clear_token() {
    let handlers = [
        handler(Some("clear")),
        handler(Some("other|clear")),
        handler(/*matcher*/ None),
        handler(Some("")),
        handler(Some("*")),
        handler(Some("^clear$")),
        handler(Some(".*")),
        handler(Some("other")),
    ]
    .into_iter()
    .enumerate()
    .map(|(display_order, mut handler)| {
        handler.display_order = display_order.try_into().unwrap();
        handler
    })
    .collect::<Vec<_>>();

    let preview_ids = preview(&handlers, SessionEndReason::Clear)
        .into_iter()
        .map(|run| run.display_order)
        .collect::<Vec<_>>();
    let run_ids = select_handlers(&handlers, SessionEndReason::Clear)
        .into_iter()
        .map(|handler| handler.display_order)
        .collect::<Vec<_>>();

    assert_eq!(vec![0, 1], preview_ids);
    assert_eq!(preview_ids, run_ids);
}

#[test]
fn session_end_ignores_successful_output() {
    let completed = parse_completed(
        &handler(/*matcher*/ None),
        HandlerRunResult {
            started_at: 1,
            completed_at: 2,
            duration_ms: 1,
            exit_code: Some(0),
            stdout: r#"{"continue":false,"decision":"block","reason":"ignored"}"#.to_string(),
            stderr: String::new(),
            error: None,
        },
        /*turn_id*/ None,
    );

    assert_eq!(completed.completed.run.status, HookRunStatus::Completed);
    assert_eq!(completed.completed.run.entries, Vec::new());
}

#[tokio::test]
async fn clear_session_end_reports_timeout() {
    let mut timed_out = handler(Some("clear"));
    timed_out.timeout_sec = 0;
    set_command(&mut timed_out, "echo unreachable");
    let outcome = run(&engine(timed_out, default_command_shell()), request()).await;

    assert_failed_with(&outcome.hook_events, "hook timed out after 0s");
}

#[tokio::test]
async fn clear_session_end_reports_nonzero_exit() {
    let mut nonzero = handler(Some("clear"));
    #[cfg(windows)]
    set_command(&mut nonzero, "echo nonzero detail 1>&2 & exit /b 7");
    #[cfg(not(windows))]
    set_command(&mut nonzero, "printf 'nonzero detail' >&2; exit 7");
    let outcome = run(&engine(nonzero, default_command_shell()), request()).await;

    assert_failed_with(&outcome.hook_events, "nonzero detail");
}

#[tokio::test]
async fn clear_session_end_reports_launch_failure() {
    let launch_failure = handler(Some("clear"));
    let outcome = run(
        &engine(
            launch_failure,
            CommandShell {
                program: "/definitely/missing/codex-hook-shell".to_string(),
                args: Vec::new(),
            },
        ),
        request(),
    )
    .await;

    assert_eq!(outcome.hook_events.len(), 1);
    let event = &outcome.hook_events[0];
    assert_eq!(event.run.status, HookRunStatus::Failed);
    assert_eq!(event.run.entries.len(), 1);
    assert_eq!(event.run.entries[0].kind, HookOutputEntryKind::Error);
    assert!(!event.run.entries[0].text.is_empty());
}

#[tokio::test]
async fn clear_session_end_reports_serialization_failure() {
    let outcome = run_with_serializer(
        &engine(handler(Some("clear")), default_command_shell()),
        request(),
        |_input| Err("injected session end serialization failure".to_string()),
    )
    .await;

    assert_failed_with(
        &outcome.hook_events,
        "injected session end serialization failure",
    );
}

fn request() -> SessionEndRequest {
    SessionEndRequest {
        session_id: ThreadId::new(),
        turn_id: "clear-hook-test".to_string(),
        cwd: test_path_buf("/tmp").abs(),
        transcript_path: None,
        reason: SessionEndReason::Clear,
        clear_transition_id: Some("transition".to_string()),
    }
}

fn engine(handler: ConfiguredHandler, shell: CommandShell) -> ClaudeHooksEngine {
    let (result_sender, _result_receiver) = async_channel::unbounded();
    let runtime = CommandHookRuntime::new(shell, ThreadId::new(), result_sender);
    let mut engine = ClaudeHooksEngine::new(
        /*enabled*/ true,
        /*bypass_hook_trust*/ false,
        /*config_layer_stack*/ None,
        Vec::new(),
        Vec::new(),
        runtime,
    );
    engine.handlers = vec![handler];
    engine
}

fn default_command_shell() -> CommandShell {
    CommandShell {
        program: String::new(),
        args: Vec::new(),
    }
}

fn set_command(handler: &mut ConfiguredHandler, command: &str) {
    let crate::engine::ConfiguredHandlerKind::Command {
        command: configured,
        ..
    } = &mut handler.kind;
    *configured = command.to_string();
}

fn assert_failed_with(events: &[codex_protocol::protocol::HookCompletedEvent], text: &str) {
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].run.status, HookRunStatus::Failed);
    assert!(
        events[0]
            .run
            .entries
            .iter()
            .any(|entry| entry.kind == HookOutputEntryKind::Error && entry.text.contains(text)),
        "missing failure text {text:?}: {:?}",
        events[0].run.entries
    );
}

fn handler(matcher: Option<&str>) -> ConfiguredHandler {
    ConfiguredHandler {
        builtin: false,
        event_name: HookEventName::SessionEnd,
        matcher: matcher.map(str::to_string),
        timeout_sec: 2,
        status_message: None,
        additional_context_limit: Default::default(),
        source_path: test_path_buf("/tmp/hooks.json").abs().into(),
        source: HookSource::User,
        display_order: 0,
        kind: crate::engine::ConfiguredHandlerKind::Command {
            command: "echo hook".to_string(),
            r#async: false,
            env: HashMap::new(),
        },
    }
}
