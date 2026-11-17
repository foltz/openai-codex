use super::*;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn retired_generation_cannot_commit_after_successful_replacement() {
    let home = tempfile::tempdir().unwrap();
    let old = RemotePluginBundleSyncGeneration::capture(home.path()).unwrap();
    let retirement = retire_remote_plugin_bundle_sync(home.path()).unwrap();
    assert!(old.begin_commit().is_err());
    assert!(RemotePluginBundleSyncGeneration::capture(home.path()).is_err());
    retirement
        .wait_until(Instant::now() + std::time::Duration::from_secs(1))
        .await
        .unwrap();
    assert!(old.begin_commit().is_err());
    let fresh = RemotePluginBundleSyncGeneration::capture(home.path()).unwrap();
    drop(fresh.begin_commit().unwrap());
}

#[tokio::test]
async fn dropping_retirement_observer_cannot_forget_closed_authority() {
    let home = tempfile::tempdir().unwrap();
    let generation = RemotePluginBundleSyncGeneration::capture(home.path()).unwrap();
    drop(retire_remote_plugin_bundle_sync(home.path()).unwrap());
    drop(generation);
    assert!(RemotePluginBundleSyncGeneration::capture(home.path()).is_err());
    retire_remote_plugin_bundle_sync(home.path())
        .unwrap()
        .wait_until(Instant::now() + std::time::Duration::from_secs(1))
        .await
        .unwrap();
    assert!(RemotePluginBundleSyncGeneration::capture(home.path()).is_ok());
}

#[tokio::test]
async fn retry_still_waits_for_an_actual_blocking_commit_after_observer_drop() {
    let home = tempfile::tempdir().unwrap();
    let generation = RemotePluginBundleSyncGeneration::capture(home.path()).unwrap();
    let lease = generation.begin_commit().unwrap();
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, held) = std::sync::mpsc::channel();
    let task = tokio::task::spawn_blocking(move || {
        let _lease = lease;
        entered.send(()).unwrap();
        held.recv().unwrap();
    });
    started.await.unwrap();
    let first = retire_remote_plugin_bundle_sync(home.path()).unwrap();
    let error = first.wait_until(Instant::now()).await.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    drop(task); // The physical writer outlives its async join observer.
    drop(generation);
    assert!(RemotePluginBundleSyncGeneration::capture(home.path()).is_err());
    let retry = retire_remote_plugin_bundle_sync(home.path()).unwrap();
    let waiting = retry.wait_until(Instant::now() + std::time::Duration::from_secs(1));
    tokio::pin!(waiting);
    assert!(futures::poll!(waiting.as_mut()).is_pending());
    release.send(()).unwrap();
    waiting.await.unwrap();
    assert!(RemotePluginBundleSyncGeneration::capture(home.path()).is_ok());
}

#[tokio::test]
async fn superseded_reset_observer_cannot_reopen_the_newer_fence() {
    let home = tempfile::tempdir().unwrap();
    let first = retire_remote_plugin_bundle_sync(home.path()).unwrap();
    let latest = retire_remote_plugin_bundle_sync(home.path()).unwrap();
    assert!(
        first
            .wait_until(Instant::now() + std::time::Duration::from_secs(1))
            .await
            .is_err()
    );
    assert!(RemotePluginBundleSyncGeneration::capture(home.path()).is_err());
    latest
        .wait_until(Instant::now() + std::time::Duration::from_secs(1))
        .await
        .unwrap();
    assert!(RemotePluginBundleSyncGeneration::capture(home.path()).is_ok());
}

#[tokio::test]
async fn counter_exhaustion_never_wraps_or_acknowledges_reset() {
    let home = tempfile::tempdir().unwrap();
    let generation = RemotePluginBundleSyncGeneration::capture(home.path()).unwrap();
    generation
        .authority
        .lock()
        .unwrap()
        .active
        .send_replace(usize::MAX);
    assert!(generation.begin_commit().is_err());
    assert_eq!(
        *generation.authority.lock().unwrap().active.borrow(),
        usize::MAX
    );
    assert!(RemotePluginBundleSyncGeneration::capture(home.path()).is_err());
    assert!(retire_remote_plugin_bundle_sync(home.path()).is_err());
}

#[test]
fn poisoned_authority_refuses_new_commit_capture_and_reset() {
    let home = tempfile::tempdir().unwrap();
    let generation = RemotePluginBundleSyncGeneration::capture(home.path()).unwrap();
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _state = generation.authority.lock().unwrap();
            panic!("poison only this synthetic authority");
        }))
        .is_err()
    );
    assert!(generation.begin_commit().is_err());
    assert!(RemotePluginBundleSyncGeneration::capture(home.path()).is_err());
    assert!(retire_remote_plugin_bundle_sync(home.path()).is_err());
}

#[cfg(unix)]
#[tokio::test]
async fn uncreated_home_aliases_share_the_physical_commit_fence() {
    let parent = tempfile::tempdir().unwrap();
    let physical = parent.path().join("physical");
    std::fs::create_dir(&physical).unwrap();
    let alias = parent.path().join("alias");
    std::os::unix::fs::symlink(&physical, &alias).unwrap();
    let generation = RemotePluginBundleSyncGeneration::capture(&alias.join("uncreated")).unwrap();
    let lease = generation.begin_commit().unwrap();
    let retirement = retire_remote_plugin_bundle_sync(&physical.join("uncreated")).unwrap();
    assert!(generation.begin_commit().is_err());
    let waiting = retirement.wait_until(Instant::now() + std::time::Duration::from_secs(1));
    tokio::pin!(waiting);
    assert!(futures::poll!(waiting.as_mut()).is_pending());
    drop(lease);
    waiting.await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn distinct_homes_with_an_aliased_cache_share_retirement() {
    let parent = tempfile::tempdir().unwrap();
    let physical = parent.path().join("physical-cache");
    std::fs::create_dir(&physical).unwrap();
    let left = parent.path().join("left");
    let right = parent.path().join("right");
    for home in [&left, &right] {
        let cache = home.join(PLUGINS_CACHE_DIR);
        std::fs::create_dir_all(cache.parent().unwrap()).unwrap();
        std::os::unix::fs::symlink(&physical, &cache).unwrap();
    }
    let generation = RemotePluginBundleSyncGeneration::capture(&left).unwrap();
    let lease = generation.begin_commit().unwrap();
    let waiting = retire_remote_plugin_bundle_sync(&right)
        .unwrap()
        .wait_until(Instant::now() + std::time::Duration::from_secs(1));
    tokio::pin!(waiting);
    assert!(futures::poll!(waiting.as_mut()).is_pending());
    assert!(generation.begin_commit().is_err());
    drop(lease);
    waiting.await.unwrap();
}

#[tokio::test]
async fn blocking_result_transfers_custody_through_final_publication() {
    let home = tempfile::tempdir().unwrap();
    let generation = RemotePluginBundleSyncGeneration::capture(home.path()).unwrap();
    let lease = generation.begin_commit().unwrap();
    let (result, publication_lease) = lease.run_blocking(|| 42).await.unwrap();
    assert_eq!(result, 42);
    let waiting = retire_remote_plugin_bundle_sync(home.path())
        .unwrap()
        .wait_until(Instant::now() + std::time::Duration::from_secs(1));
    tokio::pin!(waiting);
    assert!(futures::poll!(waiting.as_mut()).is_pending());
    assert!(generation.begin_commit().is_err());
    // The real metadata/rollback caller owns this lease after extraction;
    // extraction completion alone must not acknowledge retirement.
    drop(publication_lease);
    waiting.await.unwrap();
}

#[tokio::test]
async fn abandoned_blocking_observer_does_not_release_the_original_writer() {
    let home = tempfile::tempdir().unwrap();
    let generation = RemotePluginBundleSyncGeneration::capture(home.path()).unwrap();
    let lease = generation.begin_commit().unwrap();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let observer = tokio::spawn(lease.run_blocking(move || {
        entered_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    }));
    entered_rx.await.unwrap();
    observer.abort();
    let Err(error) = observer.await else {
        panic!("aborted observer unexpectedly completed");
    };
    assert!(error.is_cancelled());
    let waiting = retire_remote_plugin_bundle_sync(home.path())
        .unwrap()
        .wait_until(Instant::now() + std::time::Duration::from_secs(1));
    tokio::pin!(waiting);
    assert!(futures::poll!(waiting.as_mut()).is_pending());
    release_tx.send(()).unwrap();
    waiting.await.unwrap();
}
