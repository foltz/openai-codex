use codex_config::ConfigLayerSource;
use codex_core::config::Config;
use codex_utils_absolute_path::AbsolutePathBuf;
use std::sync::Arc;
use std::sync::RwLock;

/// The selected user configuration layer that supplied the app-server's MCP
/// configuration.
///
/// This deliberately records the layer snapshot rather than reading the file
/// again, so it remains a truthful identity of configuration that was actually
/// accepted by the process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct McpConfigIdentity {
    pub(crate) file_path: AbsolutePathBuf,
    pub(crate) version: String,
}

impl McpConfigIdentity {
    pub(crate) fn from_config(config: &Config) -> Option<Self> {
        let layer = config.config_layer_stack.get_active_user_layer()?;
        let ConfigLayerSource::User { file, .. } = &layer.name else {
            return None;
        };
        Some(Self {
            file_path: file.clone(),
            version: layer.version.clone(),
        })
    }
}

/// Process-scoped ownership of the last selected user configuration identity
/// accepted by a strict MCP refresh.
///
/// Reads and replacements are synchronous and intentionally short-lived; no
/// caller may hold this lock while doing configuration or thread work.
#[derive(Clone, Debug, Default)]
pub(crate) struct AppliedMcpConfigIdentity {
    identity: Arc<RwLock<Option<McpConfigIdentity>>>,
}

impl AppliedMcpConfigIdentity {
    pub(crate) fn from_startup_config(config: &Config) -> Self {
        Self {
            identity: Arc::new(RwLock::new(McpConfigIdentity::from_config(config))),
        }
    }

    pub(crate) fn replace(&self, identity: Option<McpConfigIdentity>) {
        if let Ok(mut retained_identity) = self.identity.write() {
            *retained_identity = identity;
        }
    }

    pub(crate) fn current(&self) -> Option<McpConfigIdentity> {
        self.identity
            .read()
            .ok()
            .and_then(|retained_identity| retained_identity.clone())
    }
}
