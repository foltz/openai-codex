use super::REMOTE_CREATED_BY_ME_MARKETPLACE_NAME;
use super::REMOTE_GLOBAL_MARKETPLACE_NAME;
use super::REMOTE_WORKSPACE_MARKETPLACE_NAME;
use super::REMOTE_WORKSPACE_SHARED_WITH_ME_MARKETPLACE_NAME;
use super::REMOTE_WORKSPACE_SHARED_WITH_ME_PRIVATE_MARKETPLACE_NAME;
use super::REMOTE_WORKSPACE_SHARED_WITH_ME_UNLISTED_MARKETPLACE_NAME;
use super::RemoteInstalledPluginScope;
use super::RemotePluginCatalogError;
use super::RemotePluginScope;
use super::RemotePluginServiceConfig;
use super::RemotePluginShareDiscoverability;
use super::ensure_chatgpt_auth;
use super::fetch_installed_plugins;
use super::remote_plugin_canonical_marketplace_name;
use crate::store::PLUGINS_CACHE_DIR;
use crate::store::PluginStore;
use crate::store::PluginStoreError;
use codex_login::CodexAuth;
use codex_plugin::PluginId;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use tokio::sync::watch;
use tracing::info;
use tracing::warn;

static REMOTE_INSTALLED_PLUGIN_BUNDLE_SYNC_AUTHORITIES: OnceLock<
    Mutex<HashMap<RemoteInstalledPluginBundleSyncKey, Arc<Mutex<BundleSyncState>>>>,
> = OnceLock::new();
static REMOTE_PLUGIN_CACHE_MUTATIONS_IN_FLIGHT: OnceLock<
    Mutex<HashMap<RemotePluginCacheMutationKey, usize>>,
> = OnceLock::new();

/// A remote plugin bundle newly installed or updated from an authenticated snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemotePluginMaterialization {
    pub plugin_id: PluginId,
    pub scope: RemotePluginScope,
    pub discoverability: Option<RemotePluginShareDiscoverability>,
    pub authenticated_account_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RemoteInstalledPluginBundleSyncOutcome {
    pub materialized_remote_plugins: Vec<RemotePluginMaterialization>,
    pub removed_cache_plugin_ids: Vec<String>,
    pub failed_remote_plugin_ids: Vec<String>,
}

impl RemoteInstalledPluginBundleSyncOutcome {
    pub fn changed_local_cache(&self) -> bool {
        !self.materialized_remote_plugins.is_empty() || !self.removed_cache_plugin_ids.is_empty()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RemoteInstalledPluginBundleSyncError {
    #[error("{0}")]
    Catalog(#[from] RemotePluginCatalogError),

    #[error("{0}")]
    Store(#[from] PluginStoreError),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RemoteInstalledPluginBundleSyncKey {
    plugin_cache_root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RemotePluginCacheMutationKey {
    plugin_cache_root: PathBuf,
    marketplace_name: String,
    plugin_name: String,
}

pub struct RemotePluginCacheMutationGuard {
    key: RemotePluginCacheMutationKey,
}

struct BundleSyncState {
    generation: Arc<watch::Sender<bool>>,
    active_commits: Arc<watch::Sender<usize>>,
    requested: Option<BundleSyncRequest>,
    running: bool,
}

struct BundleSyncRequest {
    codex_home: PathBuf,
    config: RemotePluginServiceConfig,
    auth: CodexAuth,
    generation: RemotePluginBundleSyncGeneration,
    on_changed: Option<Arc<dyn Fn(RemoteInstalledPluginBundleSyncOutcome) + Send + Sync>>,
}

/// Carries the filesystem publication authority of the authenticated snapshot.
#[derive(Clone)]
pub(crate) struct RemotePluginBundleSyncGeneration {
    state: Arc<Mutex<BundleSyncState>>,
    generation: Arc<watch::Sender<bool>>,
}

pub(crate) struct RemotePluginBundleCommitLease(Arc<watch::Sender<usize>>);

impl RemotePluginBundleSyncGeneration {
    async fn cancelled(&self) {
        let mut retired = self.generation.subscribe();
        while !*retired.borrow_and_update() {
            if retired.changed().await.is_err() {
                return;
            }
        }
    }
    pub(crate) fn capture(codex_home: &Path) -> Self {
        let authority = bundle_sync_authority(codex_home);
        let state = authority.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        Self {
            state: Arc::clone(&authority),
            generation: Arc::clone(&state.generation),
        }
    }
    pub(crate) fn begin_commit(&self) -> Option<RemotePluginBundleCommitLease> {
        let state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if !Arc::ptr_eq(&state.generation, &self.generation) {
            return None;
        }
        state.active_commits.send_modify(|active| *active += 1);
        Some(RemotePluginBundleCommitLease(Arc::clone(
            &state.active_commits,
        )))
    }
}

impl Drop for RemotePluginBundleCommitLease {
    fn drop(&mut self) {
        self.0.send_modify(|active| *active -= 1);
    }
}

fn bundle_sync_authority(codex_home: &Path) -> Arc<Mutex<BundleSyncState>> {
    let authorities =
        REMOTE_INSTALLED_PLUGIN_BUNDLE_SYNC_AUTHORITIES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut authorities = authorities.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    Arc::clone(
        authorities
            .entry(RemoteInstalledPluginBundleSyncKey {
                plugin_cache_root: remote_plugin_cache_root(codex_home),
            })
            .or_insert_with(|| {
                Arc::new(Mutex::new(BundleSyncState {
                    generation: Arc::new(watch::channel(false).0),
                    active_commits: Arc::new(watch::channel(0).0),
                    requested: None,
                    running: false,
                }))
            }),
    )
}

/// Invalidates downloads and queued work immediately. The returned receiver
/// tracks filesystem commits across generations, including earlier failed
/// reset attempts, so retry cannot forget a still-running retired commit.
pub(crate) fn retire_remote_plugin_bundle_sync(codex_home: &Path) -> watch::Receiver<usize> {
    let authority = bundle_sync_authority(codex_home);
    let mut state = authority.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let retired = state.active_commits.subscribe();
    state.generation.send_replace(true);
    state.generation = Arc::new(watch::channel(false).0);
    state.requested = None;
    retired
}

pub(crate) fn maybe_start_remote_installed_plugin_bundle_sync(
    codex_home: PathBuf,
    config: RemotePluginServiceConfig,
    auth: Option<CodexAuth>,
    on_local_cache_changed: Option<
        Arc<dyn Fn(RemoteInstalledPluginBundleSyncOutcome) + Send + Sync + 'static>,
    >,
) {
    let Some(auth) = auth else {
        return;
    };
    let authority = bundle_sync_authority(&codex_home);
    {
        let mut state = authority.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.requested = Some(BundleSyncRequest {
            codex_home,
            config,
            auth,
            generation: RemotePluginBundleSyncGeneration {
                state: Arc::clone(&authority),
                generation: Arc::clone(&state.generation),
            },
            on_changed: on_local_cache_changed,
        });
        if state.running {
            return;
        }
        state.running = true;
    }
    tokio::spawn(async move {
        loop {
            let request = {
                let mut state = authority.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                let Some(request) = state.requested.take() else {
                    state.running = false;
                    return;
                };
                request
            };
            let result = tokio::select! {
              biased;
              _ = request.generation.cancelled() => continue,
              result = sync_remote_installed_plugin_bundles_for_generation(
                request.codex_home,
                &request.config,
                Some(&request.auth),
                request.generation.clone(),
              ) => result,
            };
            match result {
                Ok(outcome) => {
                    info!(
                        materialized_remote_plugins = ?outcome.materialized_remote_plugins,
                        removed_cache_plugin_ids = ?outcome.removed_cache_plugin_ids,
                        failed_remote_plugin_ids = ?outcome.failed_remote_plugin_ids,
                        "completed remote installed plugin bundle sync"
                    );
                    if outcome.changed_local_cache()
                        && let Some(on_local_cache_changed) = request.on_changed
                        && let Some(_lease) = request.generation.begin_commit()
                    {
                        on_local_cache_changed(outcome);
                    }
                }
                Err(err) => {
                    warn!(
                        error = %err,
                        "remote installed plugin bundle sync failed"
                    );
                }
            }
        }
    });
}

pub async fn sync_remote_installed_plugin_bundles_once(
    codex_home: PathBuf,
    config: &RemotePluginServiceConfig,
    auth: Option<&CodexAuth>,
) -> Result<RemoteInstalledPluginBundleSyncOutcome, RemoteInstalledPluginBundleSyncError> {
    let generation = RemotePluginBundleSyncGeneration::capture(&codex_home);
    sync_remote_installed_plugin_bundles_for_generation(codex_home, config, auth, generation).await
}

async fn sync_remote_installed_plugin_bundles_for_generation(
    codex_home: PathBuf,
    config: &RemotePluginServiceConfig,
    auth: Option<&CodexAuth>,
    generation: RemotePluginBundleSyncGeneration,
) -> Result<RemoteInstalledPluginBundleSyncOutcome, RemoteInstalledPluginBundleSyncError> {
    let auth = ensure_chatgpt_auth(auth)?;
    let authenticated_account_id = auth.get_account_id();
    let installed_plugins = fetch_installed_plugins(
        config,
        auth,
        RemoteInstalledPluginScope::All,
        /*include_download_urls*/ true,
    )
    .await?;
    let store = PluginStore::try_new(codex_home.clone())?;
    let mut installed_plugin_names_by_marketplace =
        BTreeMap::<String, BTreeSet<String>>::from_iter([
            (REMOTE_GLOBAL_MARKETPLACE_NAME.to_string(), BTreeSet::new()),
            (
                REMOTE_CREATED_BY_ME_MARKETPLACE_NAME.to_string(),
                BTreeSet::new(),
            ),
            (
                REMOTE_WORKSPACE_MARKETPLACE_NAME.to_string(),
                BTreeSet::new(),
            ),
            (
                REMOTE_WORKSPACE_SHARED_WITH_ME_MARKETPLACE_NAME.to_string(),
                BTreeSet::new(),
            ),
            (
                REMOTE_WORKSPACE_SHARED_WITH_ME_PRIVATE_MARKETPLACE_NAME.to_string(),
                BTreeSet::new(),
            ),
            (
                REMOTE_WORKSPACE_SHARED_WITH_ME_UNLISTED_MARKETPLACE_NAME.to_string(),
                BTreeSet::new(),
            ),
        ]);
    let mut materialized_remote_plugins = BTreeMap::new();
    let mut failed_remote_plugin_ids = BTreeSet::new();

    for installed_plugin in installed_plugins {
        let plugin = installed_plugin.plugin;
        let scope = plugin.scope;
        let discoverability = plugin.discoverability;
        let marketplace_name = remote_plugin_canonical_marketplace_name(&plugin)?.to_string();
        installed_plugin_names_by_marketplace
            .entry(marketplace_name.clone())
            .or_default()
            .insert(plugin.name.clone());
        let plugin_id = match PluginId::new(plugin.name.clone(), marketplace_name.clone()) {
            Ok(plugin_id) => plugin_id,
            Err(err) => {
                warn!(
                    remote_plugin_id = %plugin.id,
                    plugin = %plugin.name,
                    marketplace = %marketplace_name,
                    error = %err,
                    "skipping remote installed plugin with invalid local cache id"
                );
                failed_remote_plugin_ids.insert(plugin.id);
                continue;
            }
        };
        let release_version = plugin
            .release
            .version
            .as_deref()
            .map(str::trim)
            .filter(|version| !version.is_empty());
        if store.active_plugin_version(&plugin_id).as_deref() == release_version {
            let _lease = generation
                .begin_commit()
                .ok_or(RemotePluginCatalogError::AuthChanged)?;
            if let Err(err) = store.write_remote_plugin_id(&plugin_id, &plugin.id) {
                warn!(
                    remote_plugin_id = %plugin.id,
                    plugin = %plugin.name,
                    marketplace = %marketplace_name,
                    error = %err,
                    "failed to persist identity for cached remote installed plugin"
                );
                failed_remote_plugin_ids.insert(plugin.id);
            }
            continue;
        }

        let bundle = match crate::remote_bundle::validate_remote_plugin_bundle(
            &plugin.id,
            &marketplace_name,
            &plugin.name,
            release_version,
            plugin.release.bundle_download_url.as_deref(),
            plugin.release.app_manifest.clone(),
        ) {
            Ok(bundle) => bundle,
            Err(err) => {
                warn!(
                    remote_plugin_id = %plugin.id,
                    plugin = %plugin.name,
                    marketplace = %marketplace_name,
                    error = %err,
                    "skipping remote installed plugin bundle download"
                );
                failed_remote_plugin_ids.insert(plugin.id);
                continue;
            }
        };

        match crate::remote_bundle::download_and_install_remote_plugin_bundle_for_generation(
            config,
            codex_home.clone(),
            bundle,
            generation.clone(),
        )
        .await
        {
            Ok(Some(result)) => {
                let plugin_id = result.plugin_id;
                materialized_remote_plugins.insert(
                    plugin_id.as_key(),
                    RemotePluginMaterialization {
                        plugin_id,
                        scope,
                        discoverability,
                        authenticated_account_id: authenticated_account_id.clone(),
                    },
                );
            }
            Ok(None) => return Err(RemotePluginCatalogError::AuthChanged.into()),
            Err(err) => {
                warn!(
                    remote_plugin_id = %plugin.id,
                    plugin = %plugin.name,
                    marketplace = %marketplace_name,
                    error = %err,
                    "failed to download remote installed plugin bundle"
                );
                failed_remote_plugin_ids.insert(plugin.id);
            }
        }
    }

    let stale_cache_cleanup = tokio::task::spawn_blocking(move || {
        remove_stale_remote_plugin_caches_for_generation(
            codex_home.as_path(),
            &installed_plugin_names_by_marketplace,
            &generation,
        )
    })
    .await;
    let removed_cache_plugin_ids = match stale_cache_cleanup {
        Ok(Ok(removed_cache_plugin_ids)) => removed_cache_plugin_ids,
        Ok(Err(err)) => {
            return Err(err.into());
        }
        Err(err) => {
            return Err(RemotePluginCatalogError::CacheRemove(format!(
                "failed to join stale remote plugin cache cleanup task: {err}"
            ))
            .into());
        }
    };

    Ok(RemoteInstalledPluginBundleSyncOutcome {
        materialized_remote_plugins: materialized_remote_plugins.into_values().collect(),
        removed_cache_plugin_ids,
        failed_remote_plugin_ids: failed_remote_plugin_ids.into_iter().collect(),
    })
}

fn remove_stale_remote_plugin_caches_for_generation(
    codex_home: &Path,
    installed: &BTreeMap<String, BTreeSet<String>>,
    generation: &RemotePluginBundleSyncGeneration,
) -> Result<Vec<String>, RemotePluginCatalogError> {
    let _lease = generation
        .begin_commit()
        .ok_or(RemotePluginCatalogError::AuthChanged)?;
    remove_stale_remote_plugin_caches(codex_home, installed)
        .map_err(RemotePluginCatalogError::CacheRemove)
}

pub fn mark_remote_plugin_cache_mutation_in_flight(
    codex_home: &Path,
    marketplace_name: &str,
    plugin_name: &str,
) -> RemotePluginCacheMutationGuard {
    let key = RemotePluginCacheMutationKey {
        plugin_cache_root: remote_plugin_cache_root(codex_home),
        marketplace_name: marketplace_name.to_string(),
        plugin_name: plugin_name.to_string(),
    };
    let mutations =
        REMOTE_PLUGIN_CACHE_MUTATIONS_IN_FLIGHT.get_or_init(|| Mutex::new(HashMap::new()));
    let mut mutations = match mutations.lock() {
        Ok(mutations) => mutations,
        Err(err) => err.into_inner(),
    };
    *mutations.entry(key.clone()).or_default() += 1;
    RemotePluginCacheMutationGuard { key }
}

impl Drop for RemotePluginCacheMutationGuard {
    fn drop(&mut self) {
        let Some(mutations) = REMOTE_PLUGIN_CACHE_MUTATIONS_IN_FLIGHT.get() else {
            return;
        };
        let mut mutations = match mutations.lock() {
            Ok(mutations) => mutations,
            Err(err) => err.into_inner(),
        };
        if let Some(count) = mutations.get_mut(&self.key) {
            *count -= 1;
            if *count == 0 {
                mutations.remove(&self.key);
            }
        }
    }
}

fn remove_stale_remote_plugin_caches(
    codex_home: &Path,
    installed_plugin_names_by_marketplace: &BTreeMap<String, BTreeSet<String>>,
) -> Result<Vec<String>, String> {
    let mut removed_cache_plugin_ids = Vec::new();
    for marketplace_name in [
        REMOTE_GLOBAL_MARKETPLACE_NAME,
        REMOTE_CREATED_BY_ME_MARKETPLACE_NAME,
        REMOTE_WORKSPACE_MARKETPLACE_NAME,
        REMOTE_WORKSPACE_SHARED_WITH_ME_MARKETPLACE_NAME,
        REMOTE_WORKSPACE_SHARED_WITH_ME_PRIVATE_MARKETPLACE_NAME,
        REMOTE_WORKSPACE_SHARED_WITH_ME_UNLISTED_MARKETPLACE_NAME,
    ] {
        let marketplace_root = codex_home.join(PLUGINS_CACHE_DIR).join(marketplace_name);
        if !marketplace_root.exists() {
            continue;
        }
        let installed_plugin_names = installed_plugin_names_by_marketplace
            .get(marketplace_name)
            .cloned()
            .unwrap_or_default();
        for entry in fs::read_dir(&marketplace_root).map_err(|err| {
            format!(
                "failed to read remote plugin cache directory {}: {err}",
                marketplace_root.display()
            )
        })? {
            let entry = entry.map_err(|err| {
                format!(
                    "failed to enumerate remote plugin cache directory {}: {err}",
                    marketplace_root.display()
                )
            })?;
            let plugin_name = entry.file_name().into_string().map_err(|file_name| {
                format!(
                    "remote plugin cache entry under {} is not valid UTF-8: {:?}",
                    marketplace_root.display(),
                    file_name
                )
            })?;
            if installed_plugin_names.contains(&plugin_name) {
                continue;
            }
            if is_remote_plugin_cache_mutation_in_flight(codex_home, marketplace_name, &plugin_name)
            {
                continue;
            }

            let cache_path = entry.path();
            if cache_path.is_dir() {
                fs::remove_dir_all(&cache_path).map_err(|err| {
                    format!(
                        "failed to remove stale remote plugin cache entry {}: {err}",
                        cache_path.display()
                    )
                })?;
            } else {
                fs::remove_file(&cache_path).map_err(|err| {
                    format!(
                        "failed to remove stale remote plugin cache entry {}: {err}",
                        cache_path.display()
                    )
                })?;
            }
            let plugin_key = PluginId::new(plugin_name.clone(), marketplace_name.to_string())
                .map(|plugin_id| plugin_id.as_key())
                .unwrap_or_else(|_| format!("{plugin_name}@{marketplace_name}"));
            removed_cache_plugin_ids.push(plugin_key);
        }
    }

    removed_cache_plugin_ids.sort();
    Ok(removed_cache_plugin_ids)
}

fn remote_plugin_cache_root(codex_home: &Path) -> PathBuf {
    codex_home.join(PLUGINS_CACHE_DIR)
}

fn is_remote_plugin_cache_mutation_in_flight(
    codex_home: &Path,
    marketplace_name: &str,
    plugin_name: &str,
) -> bool {
    let Some(mutations) = REMOTE_PLUGIN_CACHE_MUTATIONS_IN_FLIGHT.get() else {
        return false;
    };
    let mutations = match mutations.lock() {
        Ok(mutations) => mutations,
        Err(err) => err.into_inner(),
    };
    mutations.contains_key(&RemotePluginCacheMutationKey {
        plugin_cache_root: remote_plugin_cache_root(codex_home),
        marketplace_name: marketplace_name.to_string(),
        plugin_name: plugin_name.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use wiremock::Mock;
    use wiremock::MockServer;
    use wiremock::ResponseTemplate;
    use wiremock::matchers::method;
    use wiremock::matchers::path;
    use wiremock::matchers::query_param;
    use wiremock::matchers::query_param_is_missing;

    #[test]
    fn reset_rejects_retired_commits_and_tracks_them_across_retries() {
        let codex_home = tempfile::tempdir().expect("create codex home");
        let authority = bundle_sync_authority(codex_home.path());
        let generation = RemotePluginBundleSyncGeneration {
            state: Arc::clone(&authority),
            generation: Arc::clone(&authority.lock().unwrap().generation),
        };
        let lease = generation.begin_commit().unwrap();
        let first = retire_remote_plugin_bundle_sync(codex_home.path());
        assert_eq!(*first.borrow(), 1);
        assert!(generation.begin_commit().is_none());
        let retry = retire_remote_plugin_bundle_sync(codex_home.path());
        assert_eq!(*retry.borrow(), 1);
        drop(lease);
        assert_eq!(*retry.borrow(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reset_fences_paused_cleanup_before_it_can_delete_new_account_files() {
        let codex_home = tempfile::tempdir().unwrap();
        let generation = RemotePluginBundleSyncGeneration::capture(codex_home.path());
        let path = codex_home.path().to_path_buf();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
        let cleanup = tokio::spawn(async move {
            ready_tx.send(()).unwrap();
            resume_rx.await.unwrap();
            tokio::task::spawn_blocking(move || {
                remove_stale_remote_plugin_caches_for_generation(
                    &path,
                    &BTreeMap::new(),
                    &generation,
                )
            })
            .await
            .unwrap()
        });
        ready_rx.await.unwrap();
        let active = retire_remote_plugin_bundle_sync(codex_home.path());
        assert_eq!(*active.borrow(), 0);
        let b = remote_plugin_cache_root(codex_home.path())
            .join(REMOTE_GLOBAL_MARKETPLACE_NAME)
            .join("account-b");
        fs::create_dir_all(&b).unwrap();
        fs::write(b.join("sentinel"), "account-b").unwrap();
        resume_tx.send(()).unwrap();
        assert!(matches!(
            cleanup.await.unwrap(),
            Err(RemotePluginCatalogError::AuthChanged)
        ));
        assert_eq!(fs::read_to_string(b.join("sentinel")).unwrap(), "account-b");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reset_preserves_new_account_sync_queued_behind_old_http_request() {
        use tokio::io::AsyncReadExt;
        use tokio::io::AsyncWriteExt;
        let codex_home = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
        let a_server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buf = [0; 1024];
            while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                let n = stream.read(&mut buf).await.unwrap();
                assert_ne!(n, 0);
                request.extend_from_slice(&buf[..n]);
            }
            ready_tx.send(()).unwrap();
            resume_rx.await.unwrap();
            let body = r#"{"plugins":[],"pagination":{"next_page_token":null}}"#;
            // Retirement may already have closed A's socket.
            let _ = stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await;
        });
        maybe_start_remote_installed_plugin_bundle_sync(
            codex_home.path().to_path_buf(),
            RemotePluginServiceConfig::new(
                format!("http://{address}/backend-api"),
                crate::test_support::test_http_client_factory(),
            ),
            Some(CodexAuth::create_dummy_chatgpt_auth_for_testing()),
            /*on_local_cache_changed*/ None,
        );
        tokio::time::timeout(std::time::Duration::from_secs(5), ready_rx)
            .await
            .unwrap()
            .unwrap();
        retire_remote_plugin_bundle_sync(codex_home.path());
        let stale = remote_plugin_cache_root(codex_home.path())
            .join(REMOTE_GLOBAL_MARKETPLACE_NAME)
            .join("stale");
        fs::create_dir_all(&stale).unwrap();
        let b_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/backend-api/ps/plugins/installed"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"plugins":[],"pagination":{"next_page_token":null}})),
            )
            .expect(1)
            .mount(&b_server)
            .await;
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let done_tx = Mutex::new(Some(done_tx));
        maybe_start_remote_installed_plugin_bundle_sync(
            codex_home.path().to_path_buf(),
            RemotePluginServiceConfig::new(
                format!("{}/backend-api", b_server.uri()),
                crate::test_support::test_http_client_factory(),
            ),
            Some(CodexAuth::create_dummy_chatgpt_auth_for_testing()),
            Some(Arc::new(move |_| {
                done_tx.lock().unwrap().take().unwrap().send(()).unwrap();
            })),
        );
        tokio::time::timeout(std::time::Duration::from_secs(5), done_rx)
            .await
            .unwrap()
            .unwrap();
        resume_tx.send(()).unwrap();
        a_server.await.unwrap();
        assert!(!stale.exists());
    }

    #[tokio::test]
    async fn sync_same_version_backfills_metadata_without_materialization() {
        let server = MockServer::start().await;
        let codex_home = tempfile::tempdir().expect("create codex home");
        let cached_manifest = codex_home
            .path()
            .join(PLUGINS_CACHE_DIR)
            .join(REMOTE_GLOBAL_MARKETPLACE_NAME)
            .join("linear")
            .join("1.2.3")
            .join(".codex-plugin")
            .join("plugin.json");
        std::fs::create_dir_all(cached_manifest.parent().expect("manifest parent"))
            .expect("create cached plugin manifest parent");
        std::fs::write(&cached_manifest, r#"{"name":"linear","version":"1.2.3"}"#)
            .expect("write cached plugin manifest");
        let remote_plugin_id = "plugins~Plugin_linear";
        Mock::given(method("GET"))
            .and(path("/backend-api/ps/plugins/installed"))
            .and(query_param_is_missing("scope"))
            .and(query_param("limit", "200"))
            .and(query_param("includeDownloadUrls", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "plugins": [{
                    "id": remote_plugin_id,
                    "name": "linear",
                    "scope": "GLOBAL",
                    "installation_policy": "AVAILABLE",
                    "authentication_policy": "ON_USE",
                    "status": "ENABLED",
                    "release": {
                        "version": "1.2.3",
                        "display_name": "Linear",
                        "description": "Track work",
                        "interface": {},
                    },
                    "enabled": true,
                }],
                "pagination": {"next_page_token": null},
            })))
            .expect(1)
            .mount(&server)
            .await;
        let config = RemotePluginServiceConfig::new(
            format!("{}/backend-api", server.uri()),
            crate::test_support::test_http_client_factory(),
        );
        let auth = CodexAuth::create_dummy_chatgpt_auth_for_testing();

        let outcome = sync_remote_installed_plugin_bundles_once(
            codex_home.path().to_path_buf(),
            &config,
            Some(&auth),
        )
        .await
        .expect("sync current remote plugin bundle");

        assert_eq!(outcome, RemoteInstalledPluginBundleSyncOutcome::default());
        let plugin_id = PluginId::new(
            "linear".to_string(),
            REMOTE_GLOBAL_MARKETPLACE_NAME.to_string(),
        )
        .expect("valid plugin id");
        let metadata_path = PluginStore::new(codex_home.path().to_path_buf())
            .plugin_base_root(&plugin_id)
            .join(".codex-remote-plugin-install.json");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                &std::fs::read_to_string(metadata_path.as_path())
                    .expect("read remote plugin install metadata")
            )
            .expect("parse remote plugin install metadata"),
            json!({
                "schema_version": 1,
                "remote_plugin_id": remote_plugin_id,
            })
        );
    }

    #[tokio::test]
    async fn sync_all_scopes_paginates_and_reconciles_each_marketplace() {
        let server = MockServer::start().await;
        let codex_home = tempfile::tempdir().expect("create codex home");
        let cached_plugins = [
            (
                REMOTE_GLOBAL_MARKETPLACE_NAME,
                "global-plugin",
                "GLOBAL",
                None,
            ),
            (
                REMOTE_CREATED_BY_ME_MARKETPLACE_NAME,
                "user-plugin",
                "USER",
                None,
            ),
            (
                REMOTE_WORKSPACE_MARKETPLACE_NAME,
                "workspace-plugin",
                "WORKSPACE",
                Some("LISTED"),
            ),
            (
                REMOTE_WORKSPACE_SHARED_WITH_ME_MARKETPLACE_NAME,
                "shared-plugin",
                "WORKSPACE",
                Some("PRIVATE"),
            ),
        ];
        for (marketplace_name, plugin_name, _, _) in cached_plugins {
            for cached_plugin_name in [plugin_name, "stale"] {
                let manifest = codex_home
                    .path()
                    .join(PLUGINS_CACHE_DIR)
                    .join(marketplace_name)
                    .join(cached_plugin_name)
                    .join("1.2.3")
                    .join(".codex-plugin")
                    .join("plugin.json");
                std::fs::create_dir_all(manifest.parent().expect("manifest parent"))
                    .expect("create cached plugin manifest parent");
                std::fs::write(
                    &manifest,
                    format!(r#"{{"name":"{cached_plugin_name}","version":"1.2.3"}}"#),
                )
                .expect("write cached plugin manifest");
            }
        }
        let installed_plugins = cached_plugins
            .iter()
            .map(|(_, plugin_name, scope, discoverability)| {
                let mut plugin = json!({
                    "id": format!("plugins~Plugin_{plugin_name}"),
                    "name": plugin_name,
                    "scope": scope,
                    "installation_policy": "AVAILABLE",
                    "authentication_policy": "ON_USE",
                    "status": "ENABLED",
                    "release": {
                        "version": "1.2.3",
                        "display_name": plugin_name,
                        "description": "Installed plugin",
                        "interface": {},
                    },
                    "enabled": true,
                });
                if let Some(discoverability) = discoverability {
                    plugin["discoverability"] = json!(discoverability);
                }
                plugin
            })
            .collect::<Vec<_>>();
        Mock::given(method("GET"))
            .and(path("/backend-api/ps/plugins/installed"))
            .and(query_param_is_missing("scope"))
            .and(query_param("limit", "200"))
            .and(query_param("includeDownloadUrls", "true"))
            .and(query_param_is_missing("pageToken"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "plugins": &installed_plugins[..2],
                "pagination": {"next_page_token": "page-2"},
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/backend-api/ps/plugins/installed"))
            .and(query_param_is_missing("scope"))
            .and(query_param("limit", "200"))
            .and(query_param("includeDownloadUrls", "true"))
            .and(query_param("pageToken", "page-2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "plugins": &installed_plugins[2..],
                "pagination": {"next_page_token": null},
            })))
            .expect(1)
            .mount(&server)
            .await;
        let (config, selected_urls) = crate::test_support::recording_remote_plugin_service_config(
            format!("{}/backend-api", server.uri()),
        );
        let auth = CodexAuth::create_dummy_chatgpt_auth_for_testing();

        let outcome = sync_remote_installed_plugin_bundles_once(
            codex_home.path().to_path_buf(),
            &config,
            Some(&auth),
        )
        .await
        .expect("sync installed plugins across every marketplace");
        let mut removed_cache_plugin_ids = cached_plugins
            .iter()
            .map(|(marketplace_name, _, _, _)| format!("stale@{marketplace_name}"))
            .collect::<Vec<_>>();
        removed_cache_plugin_ids.sort();

        assert_eq!(
            outcome,
            RemoteInstalledPluginBundleSyncOutcome {
                materialized_remote_plugins: Vec::new(),
                removed_cache_plugin_ids,
                failed_remote_plugin_ids: Vec::new(),
            }
        );
        assert_eq!(
            crate::test_support::recorded_http_client_urls(&selected_urls),
            vec![
                format!(
                    "{}/backend-api/ps/plugins/installed?limit=200&includeDownloadUrls=true",
                    server.uri()
                ),
                format!(
                    "{}/backend-api/ps/plugins/installed?limit=200&includeDownloadUrls=true&pageToken=page-2",
                    server.uri()
                ),
            ]
        );
        for (marketplace_name, plugin_name, _, _) in cached_plugins {
            let plugin_root = codex_home
                .path()
                .join(PLUGINS_CACHE_DIR)
                .join(marketplace_name)
                .join(plugin_name);
            assert!(
                plugin_root
                    .join("1.2.3/.codex-plugin/plugin.json")
                    .is_file()
            );
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(
                    &std::fs::read_to_string(plugin_root.join(".codex-remote-plugin-install.json"))
                        .expect("read remote plugin install metadata")
                )
                .expect("parse remote plugin install metadata"),
                json!({
                    "schema_version": 1,
                    "remote_plugin_id": format!("plugins~Plugin_{plugin_name}"),
                })
            );
            assert!(
                !codex_home
                    .path()
                    .join(PLUGINS_CACHE_DIR)
                    .join(marketplace_name)
                    .join("stale")
                    .exists()
            );
        }
    }

    #[test]
    fn stale_remote_plugin_cleanup_skips_cache_mutations_in_progress() {
        let codex_home = tempfile::tempdir().expect("create codex home");
        let cached_manifest = codex_home
            .path()
            .join(PLUGINS_CACHE_DIR)
            .join(REMOTE_GLOBAL_MARKETPLACE_NAME)
            .join("linear")
            .join("1.2.3")
            .join(".codex-plugin")
            .join("plugin.json");
        std::fs::create_dir_all(cached_manifest.parent().expect("manifest parent"))
            .expect("create cached plugin manifest parent");
        std::fs::write(&cached_manifest, r#"{"name":"linear"}"#)
            .expect("write cached plugin manifest");
        let installed_plugin_names_by_marketplace =
            BTreeMap::<String, BTreeSet<String>>::from_iter([
                (REMOTE_GLOBAL_MARKETPLACE_NAME.to_string(), BTreeSet::new()),
                (
                    REMOTE_WORKSPACE_MARKETPLACE_NAME.to_string(),
                    BTreeSet::new(),
                ),
                (
                    REMOTE_WORKSPACE_SHARED_WITH_ME_PRIVATE_MARKETPLACE_NAME.to_string(),
                    BTreeSet::new(),
                ),
                (
                    REMOTE_WORKSPACE_SHARED_WITH_ME_UNLISTED_MARKETPLACE_NAME.to_string(),
                    BTreeSet::new(),
                ),
            ]);

        let guard = mark_remote_plugin_cache_mutation_in_flight(
            codex_home.path(),
            REMOTE_GLOBAL_MARKETPLACE_NAME,
            "linear",
        );
        let second_guard = mark_remote_plugin_cache_mutation_in_flight(
            codex_home.path(),
            REMOTE_GLOBAL_MARKETPLACE_NAME,
            "linear",
        );
        let removed = remove_stale_remote_plugin_caches(
            codex_home.path(),
            &installed_plugin_names_by_marketplace,
        )
        .expect("cleanup while install is guarded");
        assert_eq!(removed, Vec::<String>::new());
        assert!(cached_manifest.is_file());

        drop(guard);
        let removed = remove_stale_remote_plugin_caches(
            codex_home.path(),
            &installed_plugin_names_by_marketplace,
        )
        .expect("cleanup while second install guard is still active");
        assert_eq!(removed, Vec::<String>::new());
        assert!(cached_manifest.is_file());

        drop(second_guard);
        let removed = remove_stale_remote_plugin_caches(
            codex_home.path(),
            &installed_plugin_names_by_marketplace,
        )
        .expect("cleanup after install guard is dropped");
        assert_eq!(removed, vec!["linear@openai-curated-remote".to_string()]);
        assert!(!cached_manifest.exists());
    }

    #[test]
    fn stale_remote_plugin_cleanup_removes_stale_marketplace_caches_and_keeps_canonical_cache() {
        let codex_home = tempfile::tempdir().expect("create codex home");
        let created_by_me_cached_manifest = codex_home
            .path()
            .join(PLUGINS_CACHE_DIR)
            .join(REMOTE_CREATED_BY_ME_MARKETPLACE_NAME)
            .join("created-by-me-plugin")
            .join("1.2.3")
            .join(".codex-plugin")
            .join("plugin.json");
        std::fs::create_dir_all(
            created_by_me_cached_manifest
                .parent()
                .expect("manifest parent"),
        )
        .expect("create cached plugin manifest parent");
        std::fs::write(
            &created_by_me_cached_manifest,
            r#"{"name":"created-by-me-plugin"}"#,
        )
        .expect("write cached plugin manifest");
        let cached_manifest = codex_home
            .path()
            .join(PLUGINS_CACHE_DIR)
            .join(REMOTE_WORKSPACE_SHARED_WITH_ME_PRIVATE_MARKETPLACE_NAME)
            .join("private-plugin")
            .join("1.2.3")
            .join(".codex-plugin")
            .join("plugin.json");
        std::fs::create_dir_all(cached_manifest.parent().expect("manifest parent"))
            .expect("create cached plugin manifest parent");
        std::fs::write(&cached_manifest, r#"{"name":"private-plugin"}"#)
            .expect("write cached plugin manifest");
        let canonical_cached_manifest = codex_home
            .path()
            .join(PLUGINS_CACHE_DIR)
            .join(REMOTE_WORKSPACE_SHARED_WITH_ME_MARKETPLACE_NAME)
            .join("shared-plugin")
            .join("1.2.3")
            .join(".codex-plugin")
            .join("plugin.json");
        std::fs::create_dir_all(canonical_cached_manifest.parent().expect("manifest parent"))
            .expect("create canonical cached plugin manifest parent");
        std::fs::write(&canonical_cached_manifest, r#"{"name":"shared-plugin"}"#)
            .expect("write canonical cached plugin manifest");
        let installed_plugin_names_by_marketplace =
            BTreeMap::<String, BTreeSet<String>>::from_iter([
                (REMOTE_GLOBAL_MARKETPLACE_NAME.to_string(), BTreeSet::new()),
                (
                    REMOTE_CREATED_BY_ME_MARKETPLACE_NAME.to_string(),
                    BTreeSet::new(),
                ),
                (
                    REMOTE_WORKSPACE_MARKETPLACE_NAME.to_string(),
                    BTreeSet::new(),
                ),
                (
                    REMOTE_WORKSPACE_SHARED_WITH_ME_MARKETPLACE_NAME.to_string(),
                    BTreeSet::from(["shared-plugin".to_string()]),
                ),
                (
                    REMOTE_WORKSPACE_SHARED_WITH_ME_PRIVATE_MARKETPLACE_NAME.to_string(),
                    BTreeSet::new(),
                ),
                (
                    REMOTE_WORKSPACE_SHARED_WITH_ME_UNLISTED_MARKETPLACE_NAME.to_string(),
                    BTreeSet::new(),
                ),
            ]);

        let removed = remove_stale_remote_plugin_caches(
            codex_home.path(),
            &installed_plugin_names_by_marketplace,
        )
        .expect("cleanup private shared-with-me cache");

        assert_eq!(
            removed,
            vec![
                "created-by-me-plugin@created-by-me-remote".to_string(),
                "private-plugin@workspace-shared-with-me-private".to_string(),
            ]
        );
        assert!(!created_by_me_cached_manifest.exists());
        assert!(!cached_manifest.exists());
        assert!(canonical_cached_manifest.is_file());
    }
}
