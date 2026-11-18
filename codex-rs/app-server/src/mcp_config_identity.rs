use codex_core::config::Config;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::RwLock;

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
        Self::from_layers(&config.config_layer_stack)
    }

    pub(crate) fn from_layers(
        stack: &codex_config::ConfigLayerStack,
    ) -> std::io::Result<Option<Self>> {
        let mut layers = Vec::new();
        for layer in stack.layers_low_to_high() {
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

/// Process-scoped ownership of the last selected user configuration identity
/// accepted by startup or a complete host-owned runtime transition.
/// Live session membership, including delegates outside ThreadManager, must
/// agree with that baseline before it can be reported.
///
/// Reads and replacements are synchronous and intentionally short-lived; no
/// caller may hold this lock while doing configuration or thread work.
#[derive(Clone, Debug, Default)]
pub(crate) struct AppliedMcpConfigIdentity {
    state: Arc<RwLock<AppliedIdentityState>>,
    apply_gate: codex_core::RuntimeConfigChangeGate,
}

#[derive(Debug, Default)]
struct AppliedIdentityState {
    baseline: Option<McpConfigIdentity>,
    next_member: u64,
    untrackable: bool,
    // None means a rejected or otherwise unidentifiable runtime publication.
    members: BTreeMap<u64, Option<McpConfigIdentity>>,
}

struct RuntimeIdentityMember {
    state: Arc<RwLock<AppliedIdentityState>>,
    id: u64,
}

impl codex_core::RuntimeConfigMembership for RuntimeIdentityMember {
    fn before_publication(&self, config: &Config, outcome: codex_core::ConfigRefreshOutcome) {
        if let Ok(mut state) = self.state.write()
            && let Some(member) = state.members.get_mut(&self.id)
        {
            *member = match outcome {
                codex_core::ConfigRefreshOutcome::Published => {
                    McpConfigIdentity::from_config(config).ok().flatten()
                }
                // Rejected swaps can carry old layers over a changed ordinary
                // map. Those layers do not prove a known runtime identity.
                codex_core::ConfigRefreshOutcome::Rejected => None,
                codex_core::ConfigRefreshOutcome::Stale => return,
            };
        }
    }
}

impl Drop for RuntimeIdentityMember {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.write() {
            state.members.remove(&self.id);
        }
    }
}

impl AppliedMcpConfigIdentity {
    pub(crate) fn from_startup_config(config: &Config) -> Self {
        Self {
            // MessageProcessor construction is intentionally infallible. An
            // unavailable UTF-8 wire identity is therefore retained as
            // unavailable and the read/reload request returns an ordinary
            // error instead of reporting a lossy identity.
            state: Arc::new(RwLock::new(AppliedIdentityState {
                baseline: McpConfigIdentity::from_config(config).ok().flatten(),
                ..AppliedIdentityState::default()
            })),
            apply_gate: codex_core::RuntimeConfigChangeGate::default(),
        }
    }

    pub(crate) fn replace(&self, identity: Option<McpConfigIdentity>) {
        if let Ok(mut state) = self.state.write() {
            state.baseline = identity;
        }
    }

    pub(crate) fn current(&self) -> Option<McpConfigIdentity> {
        let state = self.state.read().ok()?;
        if state.untrackable {
            return None;
        }
        let baseline = state.baseline.as_ref()?;
        state
            .members
            .values()
            .all(|member| member.as_ref() == Some(baseline))
            .then(|| baseline.clone())
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
    pub(crate) async fn lock_apply(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.apply_gate.lock().await
    }

    pub(crate) fn apply_gate(&self) -> codex_core::RuntimeConfigChangeGate {
        self.apply_gate.clone()
    }
}

impl codex_core::RuntimeConfigChangeListener for AppliedMcpConfigIdentity {
    fn before_runtime_config_change(&self) {
        // Legacy core reloads can only report that a selected-user runtime
        // mutation is about to begin; they cannot construct this app-server
        // protocol identity. Fail closed until an app-server-owned complete
        // transition replaces it.
        self.invalidate();
    }

    fn register_runtime_config(
        &self,
        config: &Config,
    ) -> Option<Arc<dyn codex_core::RuntimeConfigMembership>> {
        let mut state = self.state.write().ok()?;
        let id = state.next_member;
        let Some(next_member) = id.checked_add(1) else {
            state.baseline = None;
            state.untrackable = true;
            return None;
        };
        state.next_member = next_member;
        state
            .members
            .insert(id, McpConfigIdentity::from_config(config).ok().flatten());
        Some(Arc::new(RuntimeIdentityMember {
            state: Arc::clone(&self.state),
            id,
        }))
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
    #[tokio::test]
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

    #[tokio::test]
    async fn live_delegate_with_old_layers_withholds_a_new_complete_baseline() -> anyhow::Result<()>
    {
        use codex_core::RuntimeConfigChangeListener;
        let home = tempdir()?;
        let file = home.path().join(codex_config::CONFIG_TOML_FILE);
        std::fs::write(&file, "[features]\nplugins = false\n")?;
        let manager = ConfigManager::without_managed_config_for_tests(home.path().to_path_buf());
        let old = manager.load_latest_config(None).await?;
        let applied = AppliedMcpConfigIdentity::from_startup_config(&old);
        let parent = applied
            .register_runtime_config(&old)
            .expect("parent member");
        let delegate = applied
            .register_runtime_config(&old)
            .expect("delegate member");
        std::fs::write(
            &file,
            "[features]\nplugins = false\nsecret_auth_storage = true\n",
        )?;
        let next = manager.load_latest_config(None).await?;
        let next_identity = McpConfigIdentity::from_config(&next)?;
        let guard = applied.lock_apply().await;
        applied.invalidate();
        parent.before_publication(&next, codex_core::ConfigRefreshOutcome::Published);
        applied.replace(next_identity.clone());
        assert!(
            applied.current().is_none(),
            "the unenumerated delegate still uses old layers"
        );
        drop(guard);
        // Dropping one observer must not retire a runtime still owned elsewhere.
        let retained_delegate = Arc::clone(&delegate);
        drop(delegate);
        assert!(applied.current().is_none());
        drop(retained_delegate);
        assert!(
            applied.current().is_none(),
            "a retained MCP runtime still owns its provenance"
        );
        drop(retained_runtime);
        assert_eq!(applied.current(), next_identity);
        drop(parent);
        Ok(())
    }

    #[tokio::test]
    async fn rejected_member_cannot_reuse_its_old_layer_identity() -> anyhow::Result<()> {
        use codex_core::RuntimeConfigChangeListener;
        let home = tempdir()?;
        let manager = ConfigManager::without_managed_config_for_tests(home.path().to_path_buf());
        let config = manager.load_latest_config(None).await?;
        let applied = AppliedMcpConfigIdentity::from_startup_config(&config);
        let baseline = applied.current();
        assert!(baseline.is_some());
        let member = applied
            .register_runtime_config(&config)
            .expect("runtime member");
        member.before_publication(&config, codex_core::ConfigRefreshOutcome::Rejected);
        applied.replace(baseline.clone());
        assert!(
            applied.current().is_none(),
            "old layer bytes do not identify a mixed rejected runtime"
        );
        member.before_publication(&config, codex_core::ConfigRefreshOutcome::Stale);
        assert!(
            applied.current().is_none(),
            "a stale refusal cannot repair unknown provenance"
        );
        member.before_publication(&config, codex_core::ConfigRefreshOutcome::Published);
        assert_eq!(applied.current(), baseline);
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_start_membership_is_removed_without_advancing_the_baseline()
    -> anyhow::Result<()> {
        use codex_core::RuntimeConfigChangeListener;
        let home = tempdir()?;
        let file = home.path().join(codex_config::CONFIG_TOML_FILE);
        std::fs::write(&file, "[features]\nplugins = false\n")?;
        let manager = ConfigManager::without_managed_config_for_tests(home.path().to_path_buf());
        let old = manager.load_latest_config(None).await?;
        let applied = AppliedMcpConfigIdentity::from_startup_config(&old);
        let baseline = applied.current();
        std::fs::write(
            &file,
            "[features]\nplugins = false\nsecret_auth_storage = true\n",
        )?;
        let next = manager.load_latest_config(None).await?;
        let starting = applied
            .register_runtime_config(&next)
            .expect("starting runtime member");
        assert!(applied.current().is_none());
        drop(starting);
        assert_eq!(applied.current(), baseline);
        applied.invalidate();
        let matching = applied
            .register_runtime_config(&old)
            .expect("matching runtime member");
        drop(matching);
        assert!(
            applied.current().is_none(),
            "member retirement cannot invent a completed reload"
        );
        Ok(())
    }
}
