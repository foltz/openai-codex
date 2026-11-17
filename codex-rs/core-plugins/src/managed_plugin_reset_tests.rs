use super::*;
use crate::remote::reset_authority::RemotePluginBundleSyncGeneration;
use crate::test_support::test_plugins_manager;

#[tokio::test]
async fn strict_reset_retires_original_epoch_and_both_cache_generations() {
    let home = tempfile::tempdir().unwrap();
    let manager = test_plugins_manager(home.path().to_path_buf());
    let old = RemotePluginBundleSyncGeneration::capture(home.path()).unwrap();
    let metadata_before = manager
        .remote_installed_plugins_cache
        .read()
        .unwrap()
        .generation;
    let loaded_before = manager.loaded_plugins_cache.lock().unwrap().generation;
    manager.reset_remote_installed_plugins().await.unwrap();
    assert!(old.begin_commit().is_err());
    assert!(RemotePluginBundleSyncGeneration::capture(home.path()).is_ok());
    assert_eq!(
        manager
            .remote_installed_plugins_cache
            .read()
            .unwrap()
            .generation,
        metadata_before + 1
    );
    assert_eq!(
        manager.loaded_plugins_cache.lock().unwrap().generation,
        loaded_before + 1
    );
}

#[tokio::test]
async fn cancelled_reset_and_retry_keep_the_original_physical_writer() {
    let home = tempfile::tempdir().unwrap();
    let manager = test_plugins_manager(home.path().to_path_buf());
    let old = RemotePluginBundleSyncGeneration::capture(home.path()).unwrap();
    let writer = old.begin_commit().unwrap();
    let mut first = Box::pin(manager.reset_remote_installed_plugins());
    assert!(futures::poll!(first.as_mut()).is_pending());
    drop(first);
    assert!(RemotePluginBundleSyncGeneration::capture(home.path()).is_err());
    let mut retry = Box::pin(manager.reset_remote_installed_plugins());
    assert!(futures::poll!(retry.as_mut()).is_pending());
    drop(writer);
    retry.await.unwrap();
    assert!(old.begin_commit().is_err());
    assert!(RemotePluginBundleSyncGeneration::capture(home.path()).is_ok());
}

#[tokio::test]
async fn exhausted_metadata_generation_refuses_without_wrap_or_reopen() {
    let home = tempfile::tempdir().unwrap();
    let manager = test_plugins_manager(home.path().to_path_buf());
    manager
        .remote_installed_plugins_cache
        .write()
        .unwrap()
        .generation = u64::MAX;
    let loaded_before = manager.loaded_plugins_cache.lock().unwrap().generation;
    assert!(manager.reset_remote_installed_plugins().await.is_err());
    assert_eq!(
        manager
            .remote_installed_plugins_cache
            .read()
            .unwrap()
            .generation,
        u64::MAX
    );
    assert_eq!(
        manager.loaded_plugins_cache.lock().unwrap().generation,
        loaded_before
    );
    assert!(RemotePluginBundleSyncGeneration::capture(home.path()).is_err());
}

#[tokio::test]
async fn exhausted_loaded_generation_refuses_before_metadata_mutation() {
    let home = tempfile::tempdir().unwrap();
    let manager = test_plugins_manager(home.path().to_path_buf());
    manager.loaded_plugins_cache.lock().unwrap().generation = u64::MAX;
    let metadata_before = manager
        .remote_installed_plugins_cache
        .read()
        .unwrap()
        .generation;
    assert!(manager.reset_remote_installed_plugins().await.is_err());
    assert_eq!(
        manager
            .remote_installed_plugins_cache
            .read()
            .unwrap()
            .generation,
        metadata_before
    );
    assert_eq!(
        manager.loaded_plugins_cache.lock().unwrap().generation,
        u64::MAX
    );
    assert!(RemotePluginBundleSyncGeneration::capture(home.path()).is_err());
}

#[tokio::test]
async fn poisoned_metadata_refuses_and_leaves_physical_admission_closed() {
    let home = tempfile::tempdir().unwrap();
    let manager = test_plugins_manager(home.path().to_path_buf());
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _cache = manager.remote_installed_plugins_cache.write().unwrap();
            panic!("poison synthetic metadata publication lock");
        }))
        .is_err()
    );
    assert!(manager.reset_remote_installed_plugins().await.is_err());
    assert!(RemotePluginBundleSyncGeneration::capture(home.path()).is_err());
}
