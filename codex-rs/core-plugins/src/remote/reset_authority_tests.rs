use super::*;
use pretty_assertions::assert_eq;

#[test]
fn reset_rejects_retired_commits_and_tracks_them_across_retries() {
    let home = tempfile::tempdir().expect("create codex home");
    let generation = RemotePluginBundleSyncGeneration::capture(home.path());
    let lease = generation.begin_commit().expect("admit old commit");
    let first = retire_remote_plugin_bundle_sync(home.path());
    assert_eq!(*first.borrow(), 1);
    assert!(generation.begin_commit().is_none());
    let retry = retire_remote_plugin_bundle_sync(home.path());
    assert_eq!(*retry.borrow(), 1);
    drop(lease);
    assert_eq!((*first.borrow(), *retry.borrow()), (0, 0));
    let current = RemotePluginBundleSyncGeneration::capture(home.path());
    assert!(current.begin_commit().is_some());
}

#[test]
fn commit_keeps_custody_when_all_admission_handles_are_dropped() {
    let home = tempfile::tempdir().expect("create codex home");
    let generation = RemotePluginBundleSyncGeneration::capture(home.path());
    let lease = generation.begin_commit().expect("admit commit");
    drop(generation);
    let active = retire_remote_plugin_bundle_sync(home.path());
    assert_eq!(*active.borrow(), 1);
    drop(lease);
    assert_eq!(*active.borrow(), 0);
}

#[tokio::test]
async fn queued_work_cannot_recapture_authority_after_reset() {
    let home = tempfile::tempdir().expect("create codex home");
    let admitted = RemotePluginBundleSyncGeneration::capture(home.path());
    let continuation = admitted.clone();
    let (release, ready) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        ready.await.expect("release queued work");
        (
            admitted.begin_commit().is_some(),
            continuation.begin_commit().is_some(),
        )
    });
    let active = retire_remote_plugin_bundle_sync(home.path());
    assert_eq!(*active.borrow(), 0);
    release.send(()).expect("release task");
    assert_eq!(task.await.expect("join task"), (false, false));
}
