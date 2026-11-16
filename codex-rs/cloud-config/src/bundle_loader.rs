use crate::backend::BackendBundleClient;
use crate::chatgpt::ChatGptSource;
use crate::chatgpt::plan_requires_managed_config;
use crate::config_server::config_server_http_client_factory;
use crate::config_source::BundleSource;
use crate::home_lifecycle::{HomeLifecycle, HomeOwners, home_lifecycle, lifecycle_error};
use crate::refresh_task::CloudRefreshTask;
use crate::service::CLOUD_CONFIG_BUNDLE_TIMEOUT;
use crate::service::CloudConfigBundleService;
use codex_config::CloudConfigBundleLoadError;
use codex_config::CloudConfigBundleLoader;
use codex_config::ConfigRequirementsToml;
use codex_http_client::HttpClientFactory;
use codex_login::AuthConfig;
use codex_login::AuthManager;
use codex_login::CodexAuth;
use codex_protocol::account::PlanType;
use futures::FutureExt;
use std::future::Future;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;

pub fn managed_config_auth_manager(
    auth: AuthConfig,
    requirements: &ConfigRequirementsToml,
) -> std::io::Result<Option<codex_login::OAuthSessionManager>> {
    let Some(service_config) = requirements.config_server.as_ref() else {
        return Ok(None);
    };
    let factory = config_server_http_client_factory(service_config, &auth.auth_route_config);
    crate::config_server_auth_manager(service_config, auth.codex_home, &factory)
}

/// Existing grants are left to the loader's normal cache and refresh handling.
pub async fn ensure_config_server_login(
    auth: AuthConfig,
    requirements: &ConfigRequirementsToml,
) -> std::io::Result<()> {
    let Some(manager) = managed_config_auth_manager(auth, requirements)? else {
        return Ok(());
    };
    if let Err(error) = manager.authorization_id().await {
        if error
            .get_ref()
            .and_then(|source| source.downcast_ref::<codex_login::OAuthSessionError>())
            != Some(&codex_login::OAuthSessionError::LoginRequired)
        {
            return Err(error);
        }
        manager.login().await?;
    }
    Ok(())
}

pub fn managed_config_bundle_loader(
    auth_manager: Arc<AuthManager>,
    chatgpt_base_url: String,
    codex_home: PathBuf,
    factory: HttpClientFactory,
    requirements: &ConfigRequirementsToml,
) -> CloudConfigBundleLoader {
    if let Some(service_config) = requirements.config_server.as_ref() {
        let factory = config_server_http_client_factory(
            service_config,
            &codex_login::AuthRouteConfig::from_http_client_factory(factory),
        );
        return crate::config_server_bundle_loader(service_config.clone(), codex_home, factory);
    }
    let endpoint = codex_backend_client::Client::new(chatgpt_base_url.clone(), factory.clone())
        .config_bundle_url();
    let policy = factory
        .network_policy()
        .clone()
        .restrict_to_endpoints(endpoint.parse().into_iter().collect());
    chatgpt_config_bundle_loader(
        auth_manager,
        chatgpt_base_url,
        codex_home,
        factory.with_network_policy(policy),
    )
}

pub async fn managed_config_bundle_loader_for_storage(
    auth_config: AuthConfig,
    requirements: &ConfigRequirementsToml,
    enable_codex_api_key_env: bool,
) -> std::io::Result<CloudConfigBundleLoader> {
    if let Some(service_config) = requirements.config_server.as_ref() {
        let factory =
            config_server_http_client_factory(service_config, &auth_config.auth_route_config);
        return Ok(crate::config_server_bundle_loader(
            service_config.clone(),
            auth_config.codex_home,
            factory,
        ));
    }
    chatgpt_config_bundle_loader_for_storage(auth_config, enable_codex_api_key_env).await
}

struct CloudConfigBundleLoaderLifetime<C: BundleSource> {
    service: Arc<CloudConfigBundleService<C>>,
    refresh_task: CloudRefreshTask,
}

impl<C: BundleSource> Drop for CloudConfigBundleLoaderLifetime<C> {
    fn drop(&mut self) {
        self.refresh_task.abort();
    }
}

pub fn chatgpt_config_bundle_loader(
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
    match managed_config_bundle_loader_for_service(service) {
        Ok((loader, _)) => loader,
        Err(error) => CloudConfigBundleLoader::new(async move { Err(error) }),
    }
}

pub(crate) fn managed_config_bundle_loader_for_service<C>(
    service: CloudConfigBundleService<C>,
) -> Result<(CloudConfigBundleLoader, CloudRefreshTask), CloudConfigBundleLoadError>
where
    C: BundleSource + 'static,
{
    let birth = LoaderBirth::capture(service.codex_home.as_path())?;
    registered_loader_at_birth(service, birth)
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

fn fence_loader(loader: CloudConfigBundleLoader, birth: LoaderBirth) -> CloudConfigBundleLoader {
    let snapshot_lifecycle = Arc::clone(&birth.lifecycle);
    let expected = birth.generation;
    loader.with_load_scope(
        move |work| {
            let lifecycle = Arc::clone(&birth.lifecycle);
            async move { lifecycle.load(expected, work).await }.boxed()
        },
        move |work| {
            let lifecycle = Arc::clone(&snapshot_lifecycle);
            async move { lifecycle.load_snapshot(expected, work).await }.boxed()
        },
    )
}

fn registered_loader_at_birth<C: BundleSource + 'static>(
    service: CloudConfigBundleService<C>,
    birth: LoaderBirth,
) -> Result<(CloudConfigBundleLoader, CloudRefreshTask), CloudConfigBundleLoadError> {
    birth.lifecycle.reap_finished()?;
    let mut owners = birth
        .lifecycle
        .owners
        .lock()
        .map_err(|_| lifecycle_error("cloud owner lock unavailable"))?;
    if owners.retiring || birth.lifecycle.generation.load(Ordering::Acquire) != birth.generation {
        return Err(lifecycle_error("cloud reset is still retiring prior work"));
    }
    Ok(start_loader(service, &birth.lifecycle, &mut owners))
}

fn start_loader<C: BundleSource + 'static>(
    mut service: CloudConfigBundleService<C>,
    lifecycle: &Arc<HomeLifecycle>,
    owners: &mut HomeOwners,
) -> (CloudConfigBundleLoader, CloudRefreshTask) {
    service.publication = Arc::clone(&lifecycle.publication);
    service.generation = Arc::clone(&lifecycle.generation);
    service.expected_generation = lifecycle.generation.load(Ordering::Acquire);
    let expected = service.expected_generation;
    let service = Arc::new(service);
    let background_service = Arc::clone(&service);
    let original = tokio::spawn(async move {
        let _ = background_service.get_latest().await;
        background_service.refresh_cache_in_background().await;
    });
    let refresh_task = CloudRefreshTask::new(original);
    owners.tasks.push(refresh_task.clone());
    let lifetime = Arc::new(CloudConfigBundleLoaderLifetime {
        service,
        refresh_task: refresh_task.clone(),
    });
    let snapshots = Arc::clone(&lifetime);
    let policy = lifetime.service.policy.clone();
    let loader = CloudConfigBundleLoader::from_getter(move || {
        let lifetime = Arc::clone(&lifetime);
        async move { lifetime.service.get_latest().await }
    })
    .with_ema_policy_snapshots(policy, move || {
        let lifetime = Arc::clone(&snapshots);
        async move { lifetime.service.get_latest_snapshot().await }
    });
    (
        fence_loader(
            loader,
            LoaderBirth {
                lifecycle: Arc::clone(lifecycle),
                generation: expected,
            },
        ),
        refresh_task,
    )
}

pub async fn chatgpt_config_bundle_loader_for_storage(
    auth_config: AuthConfig,
    enable_codex_api_key_env: bool,
) -> std::io::Result<CloudConfigBundleLoader> {
    storage_loader(
        auth_config.codex_home.clone(),
        chatgpt_config_bundle_service_for_storage(auth_config, enable_codex_api_key_env),
        StorageMode::Cached,
    )
    .await
}

/// Use the configured policy server or the supplied account's ChatGPT policy when eligible.
pub async fn managed_config_bundle_loader_for_auth(
    config: AuthConfig,
    auth: &CodexAuth,
    requirements: &ConfigRequirementsToml,
) -> std::io::Result<CloudConfigBundleLoader> {
    let birth = LoaderBirth::capture(&config.codex_home).map_err(std::io::Error::other)?;
    let loader = birth
        .lifecycle
        .load_inner(birth.generation, async {
            Ok(managed_config_bundle_loader_for_auth_inner(config, auth, requirements).await)
        })
        .await
        .map_err(std::io::Error::other)??;
    Ok(fence_loader(loader, birth))
}

async fn managed_config_bundle_loader_for_auth_inner(
    config: AuthConfig,
    auth: &CodexAuth,
    requirements: &ConfigRequirementsToml,
) -> std::io::Result<CloudConfigBundleLoader> {
    if requirements.config_server.is_some() {
        return managed_config_bundle_loader_for_storage(
            config,
            requirements,
            /*enable_codex_api_key_env*/ false,
        )
        .await;
    }
    // Only skip the policy lookup when the supplied account's plan is known.
    if auth
        .account_plan_type()
        .is_some_and(|plan| plan != PlanType::Unknown && !plan_requires_managed_config(plan))
    {
        return Ok(CloudConfigBundleLoader::default());
    }

    let base_url = config
        .chatgpt_base_url
        .as_deref()
        .unwrap_or("https://chatgpt.com/backend-api/");
    codex_http_client::validate_https_endpoint_url(base_url, "ChatGPT policy endpoint")
        .map_err(std::io::Error::other)?;
    let mut client = codex_backend_client::Client::new_without_redirects(
        base_url,
        config.auth_route_config.http_client_factory().clone(),
    )
    .with_user_agent(codex_login::default_client::get_codex_user_agent())
    .with_bearer_token(auth.get_token()?)
    .map_err(std::io::Error::other)?
    .with_chatgpt_account_id(
        auth.get_account_id()
            .ok_or_else(|| std::io::Error::other("missing ChatGPT account ID"))?,
    );
    if auth.is_fedramp_account() {
        client = client.with_fedramp_routing_header();
    }
    let response = tokio::time::timeout(CLOUD_CONFIG_BUNDLE_TIMEOUT, client.get_config_bundle())
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "timed out loading ChatGPT account policy",
            )
        })?
        .map_err(|_| std::io::Error::other("could not load ChatGPT account policy"))?;
    let bundle = crate::backend::bundle_from_response(response);
    Ok(CloudConfigBundleLoader::new(
        async move { Ok(Some(bundle)) },
    ))
}

/// Fetches directly from the network on each load, without reading or writing
/// the disk cache or starting a background refresher.
pub async fn chatgpt_config_bundle_loader_for_storage_without_cache(
    auth_config: AuthConfig,
    enable_codex_api_key_env: bool,
) -> std::io::Result<CloudConfigBundleLoader> {
    storage_loader(
        auth_config.codex_home.clone(),
        chatgpt_config_bundle_service_for_storage(auth_config, enable_codex_api_key_env),
        StorageMode::Uncached,
    )
    .await
}

enum StorageMode {
    Cached,
    Uncached,
}
async fn storage_loader<C: BundleSource + 'static>(
    home: PathBuf,
    construct: impl Future<Output = std::io::Result<CloudConfigBundleService<C>>>,
    mode: StorageMode,
) -> std::io::Result<CloudConfigBundleLoader> {
    let birth = LoaderBirth::capture(&home).map_err(std::io::Error::other)?;
    let service = construct.await?;
    match mode {
        StorageMode::Cached => registered_loader_at_birth(service, birth).map(|(loader, _)| loader),
        StorageMode::Uncached => uncached_loader_at_birth(service, birth),
    }
    .map_err(std::io::Error::other)
}
fn uncached_loader_at_birth<C: BundleSource + 'static>(
    mut service: CloudConfigBundleService<C>,
    birth: LoaderBirth,
) -> Result<CloudConfigBundleLoader, CloudConfigBundleLoadError> {
    let owners = birth
        .lifecycle
        .owners
        .lock()
        .map_err(|_| lifecycle_error("cloud owner lock unavailable"))?;
    if owners.retiring || birth.lifecycle.generation.load(Ordering::Acquire) != birth.generation {
        return Err(lifecycle_error("cloud reset is still retiring prior work"));
    }
    service = service.without_cache();
    service.publication = Arc::clone(&birth.lifecycle.publication);
    service.generation = Arc::clone(&birth.lifecycle.generation);
    service.expected_generation = birth.generation;
    let service = Arc::new(service);
    drop(owners);
    let loader = CloudConfigBundleLoader::from_getter(move || {
        let service = Arc::clone(&service);
        async move { service.load_startup_bundle_with_timeout().await }
    });
    Ok(fence_loader(loader, birth))
}

async fn chatgpt_config_bundle_service_for_storage(
    auth_config: AuthConfig,
    enable_codex_api_key_env: bool,
) -> std::io::Result<CloudConfigBundleService<ChatGptSource<BackendBundleClient>>> {
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

/// Retire this process's same-home cloud generation and install a strict
/// replacement of the selected source family. The caller must successfully
/// await get() before publishing a managed-reset acknowledgement.
pub async fn managed_cloud_config_bundle_loader(
    auth_manager: Arc<AuthManager>,
    chatgpt_base_url: String,
    codex_home: PathBuf,
    factory: HttpClientFactory,
    requirements: &ConfigRequirementsToml,
) -> Result<CloudConfigBundleLoader, CloudConfigBundleLoadError> {
    if let Some(config) = requirements.config_server.as_ref() {
        let selected = config.clone();
        let constructor_config = selected.clone();
        let home = codex_home.clone();
        return strict_loader_from_constructor(codex_home, move || {
            let factory = config_server_http_client_factory(
                &constructor_config,
                &codex_login::AuthRouteConfig::from_http_client_factory(factory),
            );
            let source = crate::config_server::ConfigServerSource::from_config_server(
                constructor_config,
                home.clone(),
                factory,
            )?;
            Ok(CloudConfigBundleService::from_source(
                source,
                home,
                CLOUD_CONFIG_BUNDLE_TIMEOUT,
            ))
        })
        .await
        .map(|loader| loader.with_config_server(selected));
    }
    let home = codex_home.clone();
    strict_loader_from_constructor(codex_home, move || {
        let endpoint = codex_backend_client::Client::new(chatgpt_base_url.clone(), factory.clone())
            .config_bundle_url();
        let policy = factory
            .network_policy()
            .clone()
            .restrict_to_endpoints(endpoint.parse().into_iter().collect());
        Ok(CloudConfigBundleService::new(
            auth_manager,
            Arc::new(BackendBundleClient::new(
                chatgpt_base_url,
                factory.with_network_policy(policy),
            )),
            home,
            CLOUD_CONFIG_BUNDLE_TIMEOUT,
        ))
    })
    .await
}

#[cfg(test)]
pub(crate) async fn strict_loader_for_service<C: BundleSource + 'static>(
    service: CloudConfigBundleService<C>,
) -> Result<CloudConfigBundleLoader, CloudConfigBundleLoadError> {
    strict_loader_from_constructor(service.codex_home.to_path_buf(), || Ok(service)).await
}

#[expect(
    clippy::await_holding_invalid_type,
    reason = "serializes same-home reset and installation; getters never take this reset lock"
)]
async fn strict_loader_from_constructor<C: BundleSource + 'static>(
    home: PathBuf,
    construct: impl FnOnce() -> Result<CloudConfigBundleService<C>, CloudConfigBundleLoadError>,
) -> Result<CloudConfigBundleLoader, CloudConfigBundleLoadError> {
    let lifecycle = home_lifecycle(&home)?;
    let _reset = tokio::time::timeout(CLOUD_CONFIG_BUNDLE_TIMEOUT, lifecycle.reset.lock())
        .await
        .map_err(|_| lifecycle_error("cloud reset ownership timed out"))?;
    // Refusal to construct the replacement must not leave the old generation
    // running after a caller has already adopted a new auth identity.
    lifecycle.retire_owners().await?;
    let mut service = construct()?;
    service.strict_cache_publication = true;
    let mut owners = lifecycle
        .owners
        .lock()
        .map_err(|_| lifecycle_error("cloud owner lock unavailable"))?;
    let (loader, _) = start_loader(service, &lifecycle, &mut owners);
    owners.retiring = false;
    Ok(loader)
}

#[cfg(test)]
#[path = "bundle_loader_tests.rs"]
mod tests;
