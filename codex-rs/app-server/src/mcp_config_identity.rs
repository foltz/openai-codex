use codex_core::config::Config;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::Mutex;

/// One selected user configuration layer that can contribute to the effective
/// MCP configuration.
///
/// The path is canonicalized while the accepted configuration snapshot is
/// still held. This makes the wire representation a filesystem identity rather
/// than a spelling supplied through `CODEX_HOME` or a profile override.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct McpConfigIdentityLayer {
    pub(crate) file_path: String,
    pub(crate) version: String,
}

/// The ordered selected user configuration inputs that supplied the
/// app-server's effective MCP configuration.
///
/// This deliberately records the accepted layer snapshots rather than reading
/// files again, so it remains a truthful identity of configuration that was
/// actually accepted by the process. Profile-v2 uses both the base user layer
/// and the selected profile layer; omitting either would make inherited MCP
/// configuration invisible to consumers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct McpConfigIdentity {
    pub(crate) layers: Vec<McpConfigIdentityLayer>,
}

impl McpConfigIdentity {
    pub(crate) fn from_config(config: &Config) -> std::io::Result<Option<Self>> {
        let mut layers = Vec::new();
        for layer in config.config_layer_stack.layers_low_to_high() {
            let codex_config::ConfigLayerSource::User { file, .. } = &layer.name else {
                continue;
            };
            let file_path = file
                .canonicalize()?
                .into_path_buf()
                .into_os_string()
                .into_string()
                .map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "selected MCP configuration path is not valid UTF-8",
                    )
                })?;
            layers.push(McpConfigIdentityLayer {
                file_path,
                version: layer.version.clone(),
            });
        }
        Ok((!layers.is_empty()).then_some(Self { layers }))
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
    apply_lock: Arc<Mutex<()>>,
}

impl AppliedMcpConfigIdentity {
    pub(crate) fn from_startup_config(config: &Config) -> Self {
        Self {
            // MessageProcessor construction is intentionally infallible. An
            // unavailable canonical wire identity is therefore retained as
            // unavailable and the read/reload request returns an ordinary
            // error instead of reporting a lossy or non-canonical identity.
            identity: Arc::new(RwLock::new(
                McpConfigIdentity::from_config(config).ok().flatten(),
            )),
            apply_lock: Arc::new(Mutex::new(())),
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

    /// A best-effort runtime reload can refresh only a subset of threads, so
    /// it must never continue to advertise a process-wide applied identity.
    pub(crate) fn invalidate(&self) {
        self.replace(None);
    }

    /// Serializes every operation that can change thread MCP runtime state with
    /// the corresponding retained applied identity. The asynchronous guard is
    /// intentionally held across configuration and thread work; the separate
    /// synchronous identity lock remains short-lived.
    pub(crate) async fn lock_apply(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.apply_lock.lock().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_manager::ConfigManager;
    use codex_config::CloudConfigBundleLoader;
    use codex_config::LoaderOverrides;
    use codex_utils_absolute_path::AbsolutePathBuf;
    use pretty_assertions::assert_eq;
    use tempfile::tempdir;

    #[tokio::test]
    async fn identity_captures_inherited_base_and_selected_profile_layers() -> anyhow::Result<()> {
        let home = tempdir()?;
        let base_path = home.path().join(codex_config::CONFIG_TOML_FILE);
        let profile_path = home.path().join("work.config.toml");
        std::fs::write(&base_path, "[mcp_servers.shared]\ncommand = \"base\"\n")?;
        std::fs::write(
            &profile_path,
            "[mcp_servers.profile]\ncommand = \"profile\"\n",
        )?;

        let mut overrides =
            LoaderOverrides::with_managed_config_path_for_tests(home.path().join("managed.toml"));
        overrides.user_config_path = Some(AbsolutePathBuf::from_absolute_path(&profile_path)?);
        overrides.user_config_profile = Some("work".parse()?);
        let manager = ConfigManager::new_for_tests(
            home.path().to_path_buf(),
            Vec::new(),
            overrides,
            CloudConfigBundleLoader::default(),
        );

        let startup = McpConfigIdentity::from_config(
            &manager.load_latest_config(/*fallback_cwd*/ None).await?,
        )?
        .expect("profile-v2 has selected user layers");
        assert_eq!(startup.layers.len(), 2);
        assert_eq!(
            startup.layers[0].file_path,
            std::fs::canonicalize(&base_path)?.display().to_string()
        );
        assert_eq!(
            startup.layers[1].file_path,
            std::fs::canonicalize(&profile_path)?.display().to_string()
        );

        std::fs::write(
            &base_path,
            "[mcp_servers.shared]\ncommand = \"changed-base\"\n",
        )?;
        let changed = McpConfigIdentity::from_config(
            &manager.load_latest_config(/*fallback_cwd*/ None).await?,
        )?
        .expect("profile-v2 has selected user layers");
        assert_ne!(changed, startup);
        assert_ne!(changed.layers[0], startup.layers[0]);
        assert_eq!(changed.layers[1], startup.layers[1]);
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::await_holding_invalid_type,
        reason = "the test intentionally proves the shared transition lock blocks a competing async task"
    )]
    async fn apply_lock_serializes_runtime_identity_transitions() {
        let identity = AppliedMcpConfigIdentity::default();
        let (first_entered_tx, first_entered_rx) = tokio::sync::oneshot::channel();
        let (release_first_tx, release_first_rx) = tokio::sync::oneshot::channel();
        let (second_entered_tx, mut second_entered_rx) = tokio::sync::oneshot::channel();

        let first_identity = identity.clone();
        let first = tokio::spawn(async move {
            let _guard = first_identity.lock_apply().await;
            first_entered_tx
                .send(())
                .expect("test receives first entry");
            release_first_rx
                .await
                .expect("test releases first transition");
        });
        first_entered_rx.await.expect("first transition enters");

        let second_identity = identity.clone();
        let second = tokio::spawn(async move {
            let _guard = second_identity.lock_apply().await;
            second_entered_tx
                .send(())
                .expect("test receives second entry");
        });

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut second_entered_rx,)
                .await
                .is_err(),
            "a second runtime transition must not enter while the first holds the shared apply lock"
        );
        release_first_tx.send(()).expect("release first transition");
        first.await.expect("first task succeeds");
        second_entered_rx.await.expect("second transition enters");
        second.await.expect("second task succeeds");
    }
}
