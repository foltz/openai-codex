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
    let value = json!({"formatVersion":2,"contextId":uuid::Uuid::new_v4().to_string(),"bindingDigest":"a".repeat(64),"provider":"codex"});
    std::fs::write(&path, serde_json::to_vec(&value).expect("context JSON"))
        .expect("write context");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("context mode");
    (dir, path)
}

#[cfg(unix)]
#[test]
fn checkpoint_keeps_initial_assignment_across_navigation_and_explicit_stop() {
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
    observer
        .interrupted()
        .expect("interrupted terminal retains assignment");
    assert_eq!(read()["assignmentId"], first.to_string());
    let second = ThreadId::new();
    observer.observed(Some(second)).expect("new identity");
    observer.stopped().expect("stop intent");
    let stopped = read();
    assert_eq!(stopped["conversationId"], first.to_string());
    assert_eq!(stopped["assignmentId"], first.to_string());
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

#[cfg(unix)]
#[test]
fn resume_verifies_assigned_identity_once_without_adopting_later_navigation() {
    let (_dir, path) = context();
    let assigned = ThreadId::new();
    let mut value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).expect("context")).expect("JSON");
    value["conversationId"] = json!(assigned.to_string());
    std::fs::write(&path, serde_json::to_vec(&value).expect("JSON")).expect("write");
    let mut observer = RecoveryObserver::open(&path, "%1".to_string()).expect("open");
    assert!(observer.observed(Some(ThreadId::new())).is_err());
    assert!(!path.with_extension("checkpoint.json").exists());
    observer.observed(Some(assigned)).expect("exact resume");
    observer
        .observed(Some(ThreadId::new()))
        .expect("navigation is not adopted");
    let checkpoint: serde_json::Value = serde_json::from_slice(
        &std::fs::read(path.with_extension("checkpoint.json")).expect("checkpoint"),
    )
    .expect("JSON");
    assert_eq!(checkpoint["assignmentId"], assigned.to_string());
    assert_eq!(checkpoint["conversationId"], assigned.to_string());
}
