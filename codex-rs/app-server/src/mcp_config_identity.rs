use codex_core::config::Config;

/// One selected user configuration layer that can contribute to the effective
/// MCP configuration.
///
/// The normalized absolute path spelling selected by the accepted configuration
/// snapshot.
///
/// This is deliberately not a canonical filesystem identity: selected user
/// layers may be absent (and therefore load as an empty table), and resolving
/// symlinks after loading could pair a layer's accepted version with a later
/// filesystem object. The path and version are instead both taken from the
/// same accepted configuration snapshot.
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
                .as_path()
                .to_path_buf()
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
        assert_eq!(startup.layers[0].file_path, base_path.display().to_string());
        assert_eq!(
            startup.layers[1].file_path,
            profile_path.display().to_string()
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

    #[cfg(unix)]
    #[tokio::test]
    async fn identity_preserves_the_accepted_selected_symlink_path_spelling() -> anyhow::Result<()>
    {
        use std::os::unix::fs::symlink;

        let home = tempdir()?;
        let real_path = home.path().join("real-config.toml");
        let selected_path = home.path().join("selected-config.toml");
        std::fs::write(&real_path, "[mcp_servers.shared]\ncommand = \"real\"\n")?;
        symlink(&real_path, &selected_path)?;

        let mut overrides =
            LoaderOverrides::with_managed_config_path_for_tests(home.path().join("managed.toml"));
        overrides.user_config_path = Some(AbsolutePathBuf::from_absolute_path(&selected_path)?);
        let manager = ConfigManager::new_for_tests(
            home.path().to_path_buf(),
            Vec::new(),
            overrides,
            CloudConfigBundleLoader::default(),
        );

        let identity = McpConfigIdentity::from_config(
            &manager.load_latest_config(/*fallback_cwd*/ None).await?,
        )?
        .expect("selected user config identity");
        let selected_layer = identity.layers.last().expect("explicit selected layer");
        assert_eq!(
            selected_layer.file_path,
            selected_path.display().to_string()
        );
        assert_ne!(
            selected_layer.file_path,
            std::fs::canonicalize(real_path)?.display().to_string()
        );
        Ok(())
    }
}
