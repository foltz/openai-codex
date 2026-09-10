use crate::backend::BackendBundleClient;
use crate::service::CLOUD_CONFIG_BUNDLE_TIMEOUT;
use crate::service::CloudConfigBundleService;
use codex_config::CloudConfigBundleLoadError;
use codex_config::CloudConfigBundleLoadErrorCode;
use codex_config::CloudConfigBundleLoader;
use codex_http_client::HttpClientFactory;
use codex_login::AuthConfig;
use codex_login::AuthManager;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::task::JoinHandle;

#[derive(Default)]
struct HomeOwners {
    tasks: Vec<JoinHandle<()>>,
    refresh_abort: Option<tokio::task::AbortHandle>,
    retiring: bool,
    failed_owner: bool,
}

impl HomeOwners {
    fn reap_finished(&mut self) {
        use std::future::Future;
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        self.tasks.retain_mut(|task| {
            if !task.is_finished() {
                return true;
            }
            match std::pin::Pin::new(task).poll(&mut context) {
                std::task::Poll::Ready(Ok(())) => false,
                std::task::Poll::Ready(Err(error)) => {
                    // Requested cancellation is terminal proof; panic is not a
                    // successful retirement and must be surfaced once observed.
                    self.failed_owner |= !error.is_cancelled();
                    false
                }
                std::task::Poll::Pending => true,
            }
        });
    }
}

#[derive(Default)]
struct HomeLifecycle {
    owners: Mutex<HomeOwners>,
    publication: Arc<tokio::sync::Mutex<()>>,
    generation: Arc<AtomicU64>,
    reset: tokio::sync::Mutex<()>,
}

enum LoadMode {
    Ordinary,
    Managed,
}

fn home_lifecycle(
    home: &std::path::Path,
) -> Result<Arc<HomeLifecycle>, CloudConfigBundleLoadError> {
    let home = codex_config::AbsolutePathBuf::resolve_path_against_base(home, "/");
    // Canonicalize the existing ancestor even when the home has not yet been
    // created. Symlink aliases must never get independent publication fences.
    let mut ancestor = home.to_path_buf();
    let mut suffix = Vec::new();
    let canonical = loop {
        match std::fs::canonicalize(&ancestor) {
            Ok(path) => break path,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                suffix.push(
                    ancestor
                        .file_name()
                        .ok_or_else(|| lifecycle_error("cloud home identity unavailable"))?
                        .to_owned(),
                );
                if !ancestor.pop() {
                    return Err(lifecycle_error("cloud home identity unavailable"));
                }
            }
            Err(_) => return Err(lifecycle_error("cloud home identity unavailable")),
        }
    };
    let mut identity = canonical;
    for part in suffix.into_iter().rev() {
        identity.push(part);
    }
    static HOMES: OnceLock<Mutex<HashMap<PathBuf, Arc<HomeLifecycle>>>> = OnceLock::new();
    let mut homes = HOMES
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Ok(homes.entry(identity).or_default().clone())
}

fn lifecycle_error(message: &'static str) -> CloudConfigBundleLoadError {
    CloudConfigBundleLoadError::new(CloudConfigBundleLoadErrorCode::Internal, None, message)
}

pub fn cloud_config_bundle_loader(
    auth_manager: Arc<AuthManager>,
    chatgpt_base_url: String,
    codex_home: PathBuf,
    http_client_factory: HttpClientFactory,
) -> CloudConfigBundleLoader {
    let lifecycle = match home_lifecycle(&codex_home) {
        Ok(lifecycle) => lifecycle,
        Err(error) => return CloudConfigBundleLoader::new(async move { Err(error) }),
    };
    let mut owners = lifecycle
        .owners
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if owners.retiring {
        return CloudConfigBundleLoader::new(async {
            Err(lifecycle_error("cloud reset is still retiring prior work"))
        });
    }
    start_loader(
        auth_manager,
        chatgpt_base_url,
        codex_home,
        http_client_factory,
        &lifecycle,
        &mut owners,
        LoadMode::Ordinary,
    )
}

fn start_loader(
    auth_manager: Arc<AuthManager>,
    chatgpt_base_url: String,
    codex_home: PathBuf,
    http_client_factory: HttpClientFactory,
    lifecycle: &HomeLifecycle,
    owners: &mut HomeOwners,
    mode: LoadMode,
) -> CloudConfigBundleLoader {
    let mut service = CloudConfigBundleService::new(
        auth_manager,
        Arc::new(BackendBundleClient::new(
            chatgpt_base_url,
            http_client_factory,
        )),
        codex_home,
        CLOUD_CONFIG_BUNDLE_TIMEOUT,
    );
    service.publication = lifecycle.publication.clone();
    service.generation = lifecycle.generation.clone();
    service.expected_generation = lifecycle.generation.load(Ordering::Acquire);
    service.strict_cache_publication = matches!(mode, LoadMode::Managed);
    let refresh_service = service.clone();
    let (result_tx, result_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let _ = result_tx.send(service.load_startup_bundle_with_timeout().await);
    });
    let refresh_task =
        tokio::spawn(async move { refresh_service.refresh_cache_in_background().await });
    if let Some(existing) = owners.refresh_abort.replace(refresh_task.abort_handle()) {
        existing.abort();
    }
    owners.reap_finished();
    owners.tasks.push(task);
    owners.tasks.push(refresh_task);
    CloudConfigBundleLoader::new(async move {
        result_rx
            .await
            .map_err(|_| lifecycle_error("cloud config bundle owner stopped"))?
    })
}

/// Retires prior same-home startup and refresh owners, including detached cache
/// writes, before constructing a new loader. Callers must await `get()` before
/// publishing the returned loader as an acknowledged configuration reset.
#[expect(
    clippy::await_holding_invalid_type,
    reason = "serializes same-home reset observers while retained owners and publication leases drain; those workers never acquire the reset mutex"
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
    {
        let mut owners = lifecycle
            .owners
            .lock()
            .map_err(|_| lifecycle_error("cloud owner lock unavailable"))?;
        owners.retiring = true;
        lifecycle
            .generation
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .map_err(|_| lifecycle_error("cloud generation exhausted"))?;
        for task in &owners.tasks {
            task.abort();
        }
    }
    // Keep handles registered until completion. A timeout/cancel leaves retiring
    // set, so ordinary calls cannot bypass this barrier; a managed retry can join.
    tokio::time::timeout(CLOUD_CONFIG_BUNDLE_TIMEOUT, async {
        loop {
            let done = {
                let mut owners = lifecycle
                    .owners
                    .lock()
                    .map_err(|_| lifecycle_error("cloud owner lock unavailable"))?;
                owners.reap_finished();
                owners.tasks.is_empty()
            };
            if done {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        let _publication = lifecycle.publication.lock().await;
        Ok::<(), CloudConfigBundleLoadError>(())
    })
    .await
    .map_err(|_| lifecycle_error("cloud retirement timed out"))??;
    let mut owners = lifecycle
        .owners
        .lock()
        .map_err(|_| lifecycle_error("cloud owner lock unavailable"))?;
    if std::mem::take(&mut owners.failed_owner) {
        return Err(lifecycle_error(
            "cloud prior owner failed during retirement",
        ));
    }
    owners.tasks.clear();
    owners.refresh_abort = None;
    owners.retiring = false;
    Ok(start_loader(
        auth_manager,
        chatgpt_base_url,
        codex_home,
        http_client_factory,
        &lifecycle,
        &mut owners,
        LoadMode::Managed,
    ))
}

pub async fn cloud_config_bundle_loader_for_storage(
    auth_config: AuthConfig,
    enable_codex_api_key_env: bool,
) -> CloudConfigBundleLoader {
    let codex_home = auth_config.codex_home.clone();
    let chatgpt_base_url = auth_config
        .chatgpt_base_url
        .clone()
        .unwrap_or_else(|| "https://chatgpt.com/backend-api/".to_string());
    let http_client_factory = auth_config.auth_route_config.http_client_factory().clone();
    let auth_manager =
        AuthManager::shared_from_auth_config(auth_config, enable_codex_api_key_env).await;
    cloud_config_bundle_loader(
        auth_manager,
        chatgpt_base_url,
        codex_home,
        http_client_factory,
    )
}

#[cfg(test)]
#[path = "bundle_loader_tests.rs"]
mod tests;
