//! Plugin mutation errors and their stable telemetry classifications.

use crate::marketplace::MarketplaceError;
use crate::remote_legacy::RemotePluginMutationError;
use crate::store::PluginStoreError;
use crate::store::error_context_sub_error_type;
use codex_plugin::PluginIdError;

#[derive(Debug, thiserror::Error)]
pub enum PluginInstallError {
    #[error("{0}")]
    Marketplace(#[from] MarketplaceError),

    #[error("{0}")]
    Remote(#[from] RemotePluginMutationError),

    #[error("{0}")]
    Store(#[from] PluginStoreError),

    #[error("{0}")]
    Config(#[from] anyhow::Error),

    #[error("failed to join plugin install task: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("plugin operation task custody failed: {0:?}")]
    Custody(codex_model_provider::AuthTaskFailure),
}

impl PluginInstallError {
    pub(super) fn blocking(error: codex_model_provider::AuthBlockingCallError) -> Self {
        match error {
            codex_model_provider::AuthBlockingCallError::Join(error) => Self::join(error),
            codex_model_provider::AuthBlockingCallError::Custody(failure) => Self::Custody(failure),
        }
    }

    pub(super) fn join(source: tokio::task::JoinError) -> Self {
        Self::Join(source)
    }

    pub fn is_invalid_request(&self) -> bool {
        matches!(
            self,
            Self::Marketplace(
                MarketplaceError::MarketplaceNotFound { .. }
                    | MarketplaceError::InvalidMarketplaceFile { .. }
                    | MarketplaceError::PluginNotFound { .. }
                    | MarketplaceError::PluginNotAvailable { .. }
                    | MarketplaceError::InvalidPlugin(_)
            ) | Self::Store(PluginStoreError::Invalid(_))
        )
    }

    pub fn sub_error_type(&self) -> Option<String> {
        match self {
            Self::Marketplace(err) => marketplace_error_sub_error_type(err),
            Self::Remote(err) => err.sub_error_type(),
            Self::Store(err) => err.sub_error_type(),
            Self::Config(_) => Some("failed_to_enable_plugin".to_string()),
            Self::Join(_) => Some("plugin_install_task_failed".to_string()),
            Self::Custody(_) => Some("plugin_install_task_custody_failed".to_string()),
        }
    }
}

pub(super) fn plugin_install_error_type(err: &PluginInstallError) -> &'static str {
    match err {
        PluginInstallError::Marketplace(err) => marketplace_error_type(err),
        PluginInstallError::Remote(err) => remote_plugin_mutation_error_type(err),
        PluginInstallError::Store(err) => plugin_store_error_type(err),
        PluginInstallError::Config(_) => "config",
        PluginInstallError::Join(_) => "join",
        PluginInstallError::Custody(_) => "custody",
    }
}

pub(super) fn marketplace_error_type(err: &MarketplaceError) -> &'static str {
    match err {
        MarketplaceError::Io { .. } => "marketplace_io",
        MarketplaceError::MarketplaceNotFound { .. } => "marketplace_not_found",
        MarketplaceError::InvalidMarketplaceFile { .. } => "invalid_marketplace_file",
        MarketplaceError::PluginNotFound { .. } => "plugin_not_found",
        MarketplaceError::PluginNotAvailable { .. } => "plugin_not_available",
        MarketplaceError::PluginsDisabled => "plugins_disabled",
        MarketplaceError::InvalidPlugin(_) => "invalid_plugin",
    }
}

pub(super) fn marketplace_error_sub_error_type(err: &MarketplaceError) -> Option<String> {
    match err {
        MarketplaceError::Io { context, .. } => Some(error_context_sub_error_type(context)),
        MarketplaceError::MarketplaceNotFound { .. }
        | MarketplaceError::InvalidMarketplaceFile { .. }
        | MarketplaceError::PluginNotFound { .. }
        | MarketplaceError::PluginNotAvailable { .. }
        | MarketplaceError::PluginsDisabled
        | MarketplaceError::InvalidPlugin(_) => None,
    }
}

fn remote_plugin_mutation_error_type(err: &RemotePluginMutationError) -> &'static str {
    match err {
        RemotePluginMutationError::AuthRequired => "remote_mutation_auth_required",
        RemotePluginMutationError::UnsupportedAuthMode => "remote_mutation_unsupported_auth_mode",
        RemotePluginMutationError::AuthToken(_) => "remote_mutation_auth_token",
        RemotePluginMutationError::InvalidBaseUrl(_) => "remote_mutation_invalid_base_url",
        RemotePluginMutationError::InvalidBaseUrlPath => "remote_mutation_invalid_base_url_path",
        RemotePluginMutationError::Request { .. } => "remote_mutation_request",
        RemotePluginMutationError::UnexpectedStatus { .. } => "remote_mutation_unexpected_status",
        RemotePluginMutationError::Decode { .. } => "remote_mutation_decode",
        RemotePluginMutationError::UnexpectedPluginId { .. } => {
            "remote_mutation_unexpected_plugin_id"
        }
        RemotePluginMutationError::UnexpectedEnabledState { .. } => {
            "remote_mutation_unexpected_enabled_state"
        }
    }
}

fn plugin_store_error_type(err: &PluginStoreError) -> &'static str {
    match err {
        PluginStoreError::Io { .. } => "store_io",
        PluginStoreError::Invalid(_) => "store_invalid",
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PluginUninstallError {
    #[error("{0}")]
    InvalidPluginId(#[from] PluginIdError),

    #[error("{0}")]
    Remote(#[from] RemotePluginMutationError),

    #[error("{0}")]
    Store(#[from] PluginStoreError),

    #[error("{0}")]
    Config(#[from] anyhow::Error),

    #[error("failed to join plugin uninstall task: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("plugin operation task custody failed: {0:?}")]
    Custody(codex_model_provider::AuthTaskFailure),
}

impl PluginUninstallError {
    pub(super) fn blocking(error: codex_model_provider::AuthBlockingCallError) -> Self {
        match error {
            codex_model_provider::AuthBlockingCallError::Join(error) => Self::join(error),
            codex_model_provider::AuthBlockingCallError::Custody(failure) => Self::Custody(failure),
        }
    }

    pub(super) fn join(source: tokio::task::JoinError) -> Self {
        Self::Join(source)
    }

    pub fn is_invalid_request(&self) -> bool {
        matches!(self, Self::InvalidPluginId(_))
    }
}

#[cfg(test)]
mod custody_tests {
    use super::*;
    #[test]
    fn custody_failures_are_internal_not_invalid_plugin_requests() {
        for failure in [
            codex_model_provider::AuthTaskFailure::Refused,
            codex_model_provider::AuthTaskFailure::Unavailable,
            codex_model_provider::AuthTaskFailure::Cancelled,
            codex_model_provider::AuthTaskFailure::Panicked,
        ] {
            let install = PluginInstallError::blocking(
                codex_model_provider::AuthBlockingCallError::Custody(failure),
            );
            assert!(!install.is_invalid_request());
            assert_eq!(plugin_install_error_type(&install), "custody");
            assert_eq!(
                install.sub_error_type().as_deref(),
                Some("plugin_install_task_custody_failed")
            );
            let uninstall = PluginUninstallError::blocking(
                codex_model_provider::AuthBlockingCallError::Custody(failure),
            );
            assert!(!uninstall.is_invalid_request());
        }
    }
}
