//! Integration test for the text encoding fix for issue #6178.
//!
//! These tests simulate VSCode's shell preview on Windows/WSL where the output
//! may be encoded with a legacy code page before it reaches Codex.

use super::StreamOutput;
use pretty_assertions::assert_eq;

#[test]
fn test_utf8_shell_output() {
    // Baseline: UTF-8 output should bypass the detector and remain unchanged.
    assert_eq!(decode_shell_output("пример".as_bytes()), "пример");
}

#[test]
fn test_cp1251_shell_output() {
    // VS Code shells on Windows frequently surface CP1251 bytes for Cyrillic text.
    assert_eq!(decode_shell_output(b"\xEF\xF0\xE8\xEC\xE5\xF0"), "пример");
}

#[test]
fn test_cp866_shell_output() {
    // Native cmd.exe still defaults to CP866; make sure we recognize that too.
    assert_eq!(decode_shell_output(b"\xAF\xE0\xA8\xAC\xA5\xE0"), "пример");
}

#[test]
fn test_windows_1252_smart_decoding() {
    // Smart detection should turn fancy quotes/dashes into the proper Unicode glyphs.
    assert_eq!(
        decode_shell_output(b"\x93\x94 test \x96 dash"),
        "\u{201C}\u{201D} test \u{2013} dash"
    );
}

#[test]
fn test_smart_decoding_improves_over_lossy_utf8() {
    // Regression guard: String::from_utf8_lossy() alone used to emit replacement chars here.
    let bytes = b"\x93\x94 test \x96 dash";
    assert!(
        String::from_utf8_lossy(bytes).contains('\u{FFFD}'),
        "lossy UTF-8 should inject replacement chars"
    );
    assert_eq!(
        decode_shell_output(bytes),
        "\u{201C}\u{201D} test \u{2013} dash",
        "smart decoding should keep curly quotes intact"
    );
}

#[test]
fn test_mixed_ascii_and_legacy_encoding() {
    // Commands tend to mix ASCII status text with Latin-1 bytes (e.g. café).
    assert_eq!(decode_shell_output(b"Output: caf\xE9"), "Output: café"); // codespell:ignore caf
}

#[test]
fn test_pure_latin1_shell_output() {
    // Latin-1 by itself should still decode correctly (regression coverage for the older tests).
    assert_eq!(decode_shell_output(b"caf\xE9"), "café"); // codespell:ignore caf
}

#[test]
fn test_invalid_bytes_still_fall_back_to_lossy() {
    // If detection fails, we still want the user to see replacement characters.
    let bytes = b"\xFF\xFE\xFD";
    assert_eq!(decode_shell_output(bytes), String::from_utf8_lossy(bytes));
}

fn decode_shell_output(bytes: &[u8]) -> String {
    StreamOutput {
        text: bytes.to_vec(),
        truncated_after_lines: None,
    }
    .from_utf8_lossy()
    .text
}

#[test]
fn sandbox_cleanup_summary_roundtrips_only_typed_counts_and_join_evidence() {
    use super::SandboxCleanupState;
    use super::SandboxCleanupSummary;
    for (state, wire) in [
        (SandboxCleanupState::Complete, "complete"),
        (SandboxCleanupState::Incomplete, "incomplete"),
        (SandboxCleanupState::TimedOut, "timed_out"),
        (SandboxCleanupState::Unreported, "unreported"),
    ] {
        let summary = SandboxCleanupSummary {
            state,
            reaped: 1,
            preserved_unproven: 2,
            host_owned: 3,
            handed_off: 4,
            timed_out: 5,
            joined: false,
        };
        let value = serde_json::to_value(summary).unwrap();
        assert_eq!(value["state"], serde_json::json!(wire));
        assert_eq!(value["joined"], serde_json::json!(false));
        let actual: std::collections::BTreeSet<_> = value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        let expected = [
            "state",
            "reaped",
            "preservedUnproven",
            "hostOwned",
            "handedOff",
            "timedOut",
            "joined",
        ]
        .into_iter()
        .collect();
        assert_eq!(actual, expected);
        assert_eq!(
            serde_json::from_value::<SandboxCleanupSummary>(value).unwrap(),
            summary
        );
    }
}

#[test]
fn sandbox_cleanup_summary_never_defaults_missing_join_or_invalid_state() {
    use super::SandboxCleanupSummary;
    let value = serde_json::json!({"state":"complete", "reaped":0, "preservedUnproven":0, "hostOwned":0, "handedOff":0, "timedOut":0, "joined":false});
    let mut missing = value.clone();
    missing.as_object_mut().unwrap().remove("joined");
    assert!(serde_json::from_value::<SandboxCleanupSummary>(missing).is_err());
    for state in [
        serde_json::json!("unknown"),
        serde_json::json!(true),
        serde_json::Value::Null,
    ] {
        let mut invalid = value.clone();
        invalid["state"] = state;
        assert!(serde_json::from_value::<SandboxCleanupSummary>(invalid).is_err());
    }
    for count in [
        serde_json::json!(-1),
        serde_json::json!(false),
        serde_json::json!(4294967296u64),
    ] {
        let mut invalid = value.clone();
        invalid["reaped"] = count;
        assert!(serde_json::from_value::<SandboxCleanupSummary>(invalid).is_err());
    }
}
