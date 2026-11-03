use super::*;
use pretty_assertions::assert_eq;
use serde_json::json;

#[cfg(unix)]
fn context() -> (tempfile::TempDir, PathBuf) {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().expect("private scratch");
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))
        .expect("private mode");
    let path = dir.path().join("launch.context.json");
    let value = json!({"formatVersion":1,"contextId":uuid::Uuid::new_v4().to_string(),"bindingDigest":"a".repeat(64),"provider":"codex"});
    std::fs::write(&path, serde_json::to_vec(&value).expect("context JSON"))
        .expect("write context");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("context mode");
    (dir, path)
}

#[cfg(unix)]
#[test]
fn checkpoint_tracks_client_transitions_and_explicit_stop() {
    let (_dir, path) = context();
    let mut observer = RecoveryObserver::open(&path, "%42".to_string()).expect("open context");
    let first = ThreadId::new();
    observer.observed(Some(first)).expect("first identity");
    let destination = path.with_extension("checkpoint.json");
    let read = || -> serde_json::Value {
        serde_json::from_slice(&std::fs::read(&destination).expect("checkpoint bytes"))
            .expect("checkpoint JSON")
    };
    let first_record = read();
    assert_eq!(first_record["conversationId"], first.to_string());
    assert_eq!(first_record["paneId"], "%42");
    assert_eq!(first_record["pid"], std::process::id());
    assert_eq!(first_record["phase"], "observed");
    observer.transition().expect("invalidate before changing");
    let changing = read();
    assert_eq!(changing["conversationId"], serde_json::Value::Null);
    assert_eq!(changing["phase"], "transition");
    assert_eq!(changing["intent"], "unknown");
    let second = ThreadId::new();
    observer.observed(Some(second)).expect("new identity");
    observer.stopped(Some(second)).expect("stop intent");
    let stopped = read();
    assert_eq!(stopped["conversationId"], second.to_string());
    assert_eq!(stopped["intent"], "stopped");
    assert_eq!(stopped["contextId"], first_record["contextId"]);
    assert_eq!(
        std::fs::read_dir(path.parent().expect("context parent"))
            .expect("listing")
            .count(),
        2
    );
}

#[cfg(unix)]
#[test]
fn unsafe_or_unbound_context_cannot_assert_native_identity() {
    use std::os::unix::fs::PermissionsExt;
    let (dir, path) = context();
    assert!(RecoveryObserver::open(&path, "not-a-pane".to_string()).is_err());
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o666))
        .expect("fixture writable mode");
    assert!(RecoveryObserver::open(&path, "%1".to_string()).is_err());
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("restore mode");
    let link = dir.path().join("link.json");
    std::os::unix::fs::symlink(&path, &link).expect("fixture symlink");
    assert!(RecoveryObserver::open(&link, "%1".to_string()).is_err());
}
