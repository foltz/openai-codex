use crate::backend::BackendBundleClient;
use crate::backend::BundleClient;
use crate::home_lifecycle::HomeLifecycle;
use crate::home_lifecycle::HomeOwners;
use crate::home_lifecycle::home_lifecycle;
use crate::home_lifecycle::lifecycle_error;
use crate::service::CLOUD_CONFIG_BUNDLE_TIMEOUT;
use crate::service::CloudConfigBundleService;
use codex_config::CloudConfigBundleLoadError;
use codex_config::CloudConfigBundleLoader;
use codex_http_client::HttpClientFactory;
use codex_login::AuthConfig;
use codex_login::AuthManager;
use std::future::Future;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::Ordering;
use tokio::task::AbortHandle;

fn refresher_task_slot() -> &'static Mutex<Option<AbortHandle>> {
    static REFRESHER_TASK: OnceLock<Mutex<Option<AbortHandle>>> = OnceLock::new();
    REFRESHER_TASK.get_or_init(|| Mutex::new(None))
}

pub(crate) fn replace_refresh_task(slot: &Mutex<Option<AbortHandle>>, next: AbortHandle) {
    let mut guard = slot.lock().unwrap_or_else(|err| {
        tracing::warn!("cloud config bundle refresher task slot was poisoned");
        err.into_inner()
    });
    if let Some(previous) = guard.replace(next) {
        previous.abort();
    }
}

struct CloudConfigBundleLoaderLifetime<C> {
    service: Arc<CloudConfigBundleService<C>>,
    refresh_task: AbortHandle,
}

impl<C> Drop for CloudConfigBundleLoaderLifetime<C> {
    fn drop(&mut self) {
        self.refresh_task.abort();
    }
}

pub fn cloud_config_bundle_loader(
    auth_manager: Arc<AuthManager>,
    chatgpt_base_url: String,
    codex_home: PathBuf,
    http_client_factory: HttpClientFactory,
) -> CloudConfigBundleLoader {
    let service = CloudConfigBundleService::new(
        auth_manager,
        Arc::new(BackendBundleClient::new(
            chatgpt_base_url,
            http_client_factory,
        )),
        codex_home,
        CLOUD_CONFIG_BUNDLE_TIMEOUT,
    );
    match registered_loader(service, Some(refresher_task_slot())) {
        Ok((loader, _)) => loader,
        Err(error) => CloudConfigBundleLoader::new(async move { Err(error) }),
    }
}

#[cfg(test)]
pub(crate) fn cloud_config_bundle_loader_for_service<C>(
    service: CloudConfigBundleService<C>,
) -> Result<(CloudConfigBundleLoader, AbortHandle), CloudConfigBundleLoadError>
where
    C: BundleClient + 'static,
{
    registered_loader(service, /*replacement_slot*/ None)
}

fn registered_loader<C: BundleClient + 'static>(
    service: CloudConfigBundleService<C>,
    replacement_slot: Option<&Mutex<Option<AbortHandle>>>,
) -> Result<(CloudConfigBundleLoader, AbortHandle), CloudConfigBundleLoadError> {
    let birth = LoaderBirth::capture(&service.codex_home)?;
    registered_loader_at_birth(service, replacement_slot, birth)
}

struct LoaderBirth {
    lifecycle: Arc<HomeLifecycle>,
    generation: u64,
}

impl LoaderBirth {
    fn capture(home: &Path) -> Result<Self, CloudConfigBundleLoadError> {
        let lifecycle = home_lifecycle(home)?;
        let owners = lifecycle
            .owners
            .lock()
            .map_err(|_| lifecycle_error("cloud owner lock unavailable"))?;
        if owners.retiring {
            return Err(lifecycle_error("cloud reset is still retiring prior work"));
        }
        let generation = lifecycle.generation.load(Ordering::Acquire);
        drop(owners);
        Ok(Self {
            lifecycle,
            generation,
        })
    }
}

fn registered_loader_at_birth<C: BundleClient + 'static>(
    service: CloudConfigBundleService<C>,
    replacement_slot: Option<&Mutex<Option<AbortHandle>>>,
    birth: LoaderBirth,
) -> Result<(CloudConfigBundleLoader, AbortHandle), CloudConfigBundleLoadError> {
    let lifecycle = birth.lifecycle;
    let mut owners = lifecycle
        .owners
        .lock()
        .map_err(|_| lifecycle_error("cloud owner lock unavailable"))?;
    if owners.retiring || lifecycle.generation.load(Ordering::Acquire) != birth.generation {
        return Err(lifecycle_error("cloud reset is still retiring prior work"));
    }
    let result = start_loader(service, &lifecycle, &mut owners);
    // Keep registration and global replacement in one admission interval, so
    // a delayed old constructor cannot replace the managed reset's refresher.
    if let Some(slot) = replacement_slot {
        replace_refresh_task(slot, result.1.clone());
    }
    Ok(result)
}

fn start_loader<C: BundleClient + 'static>(
    mut service: CloudConfigBundleService<C>,
    lifecycle: &Arc<HomeLifecycle>,
    owners: &mut HomeOwners,
) -> (CloudConfigBundleLoader, AbortHandle) {
    service.publication = Arc::clone(&lifecycle.publication);
    service.generation = Arc::clone(&lifecycle.generation);
    service.expected_generation = lifecycle.generation.load(Ordering::Acquire);
    let expected_generation = service.expected_generation;
    let service = Arc::new(service);
    let background_service = Arc::clone(&service);
    let refresh_task = tokio::spawn(async move {
        let _ = background_service.get_latest().await;
        background_service.refresh_cache_in_background().await;
    });
    let abort_handle = refresh_task.abort_handle();
    owners.reap_finished();
    owners.tasks.push(refresh_task);
    let lifetime = Arc::new(CloudConfigBundleLoaderLifetime {
        service,
        refresh_task: abort_handle.clone(),
    });

    let lifecycle = Arc::clone(lifecycle);
    let loader = CloudConfigBundleLoader::from_getter(move || {
        let lifetime = Arc::clone(&lifetime);
        let lifecycle = Arc::clone(&lifecycle);
        async move {
            lifecycle
                .load(expected_generation, lifetime.service.get_latest())
                .await
        }
    });
    (loader, abort_handle)
}

pub async fn cloud_config_bundle_loader_for_storage(
    auth_config: AuthConfig,
    enable_codex_api_key_env: bool,
) -> std::io::Result<CloudConfigBundleLoader> {
    storage_loader(
        auth_config.codex_home.clone(),
        cloud_config_bundle_service_for_storage(auth_config, enable_codex_api_key_env),
        StorageMode::Cached,
    )
    .await
}

/// Fetches directly from the network on each load, without reading or writing
/// the disk cache or starting a background refresher.
pub async fn cloud_config_bundle_loader_for_storage_without_cache(
    auth_config: AuthConfig,
    enable_codex_api_key_env: bool,
) -> std::io::Result<CloudConfigBundleLoader> {
    storage_loader(
        auth_config.codex_home.clone(),
        cloud_config_bundle_service_for_storage(auth_config, enable_codex_api_key_env),
        StorageMode::Uncached,
    )
    .await
}

enum StorageMode {
    Cached,
    Uncached,
}

async fn storage_loader<C: BundleClient + 'static>(
    home: PathBuf,
    construct: impl Future<Output = std::io::Result<CloudConfigBundleService<C>>>,
    mode: StorageMode,
) -> std::io::Result<CloudConfigBundleLoader> {
    // Capture before AuthManager construction can read an old identity and
    // suspend. Never stamp that snapshot with a later reset's generation.
    let birth = LoaderBirth::capture(&home).map_err(std::io::Error::other)?;
    let service = construct.await?;
    match mode {
        StorageMode::Cached => {
            registered_loader_at_birth(service, Some(refresher_task_slot()), birth)
                .map(|(loader, _)| loader)
        }
        StorageMode::Uncached => uncached_loader_at_birth(service, birth),
    }
    .map_err(std::io::Error::other)
}

#[cfg(test)]
pub(crate) fn uncached_loader<C: BundleClient + 'static>(
    service: CloudConfigBundleService<C>,
) -> Result<CloudConfigBundleLoader, CloudConfigBundleLoadError> {
    let birth = LoaderBirth::capture(&service.codex_home)?;
    uncached_loader_at_birth(service, birth)
}

fn uncached_loader_at_birth<C: BundleClient + 'static>(
    service: CloudConfigBundleService<C>,
    birth: LoaderBirth,
) -> Result<CloudConfigBundleLoader, CloudConfigBundleLoadError> {
    let mut service = service.without_cache();
    let lifecycle = birth.lifecycle;
    let owners = lifecycle
        .owners
        .lock()
        .map_err(|_| lifecycle_error("cloud owner lock unavailable"))?;
    if owners.retiring || lifecycle.generation.load(Ordering::Acquire) != birth.generation {
        return Err(lifecycle_error("cloud reset is still retiring prior work"));
    }
    service.publication = Arc::clone(&lifecycle.publication);
    service.generation = Arc::clone(&lifecycle.generation);
    service.expected_generation = lifecycle.generation.load(Ordering::Acquire);
    let expected_generation = service.expected_generation;
    let service = Arc::new(service);
    drop(owners);
    Ok(CloudConfigBundleLoader::from_getter(move || {
        let service = Arc::clone(&service);
        let lifecycle = Arc::clone(&lifecycle);
        async move {
            lifecycle
                .load(
                    expected_generation,
                    service.load_startup_bundle_with_timeout(),
                )
                .await
        }
    }))
}

/// Retires this process's prior same-home cloud owners and cache publications.
/// The caller must successfully await `get()` before publishing this strict
/// loader as an acknowledged configuration reset.
#[expect(
    clippy::await_holding_invalid_type,
    reason = "serializes same-home reset and installation; getters and writers never take the reset lock"
)]
pub async fn managed_cloud_config_bundle_loader(
    auth_manager: Arc<AuthManager>,
    chatgpt_base_url: String,
    codex_home: PathBuf,
    http_client_factory: HttpClientFactory,
) -> Result<CloudConfigBundleLoader, CloudConfigBundleLoadError> {
    let lifecycle = home_lifecycle(&codex_home)?;
    let _reset = tokio::time::timeout(CLOUD_CONFIG_BUNDLE_TIMEOUT, lifecycle.reset.lock())
        .await
        .map_err(|_| lifecycle_error("cloud reset ownership timed out"))?;
    lifecycle.retire_owners().await?;
    let mut service = CloudConfigBundleService::new(
        auth_manager,
        Arc::new(BackendBundleClient::new(
            chatgpt_base_url,
            http_client_factory,
        )),
        codex_home,
        CLOUD_CONFIG_BUNDLE_TIMEOUT,
    );
    service.strict_cache_publication = true;
    let mut owners = lifecycle
        .owners
        .lock()
        .map_err(|_| lifecycle_error("cloud owner lock unavailable"))?;
    let (loader, refresh_task) = start_loader(service, &lifecycle, &mut owners);
    replace_refresh_task(refresher_task_slot(), refresh_task);
    owners.retiring = false;
    Ok(loader)
}

async fn cloud_config_bundle_service_for_storage(
    auth_config: AuthConfig,
    enable_codex_api_key_env: bool,
) -> std::io::Result<CloudConfigBundleService<BackendBundleClient>> {
    let auth_manager =
        AuthManager::shared_from_auth_config(auth_config.clone(), enable_codex_api_key_env).await?;
    Ok(CloudConfigBundleService::new(
        auth_manager,
        Arc::new(BackendBundleClient::new(
            auth_config
                .chatgpt_base_url
                .unwrap_or_else(|| "https://chatgpt.com/backend-api/".to_string()),
            auth_config.auth_route_config.http_client_factory().clone(),
        )),
        auth_config.codex_home,
        CLOUD_CONFIG_BUNDLE_TIMEOUT,
    ))
}

#[cfg(test)]
#[path = "bundle_loader_tests.rs"]
mod tests;
