use std::cell::Cell;
use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

use codex_exec_server_protocol::JSONRPCErrorError;
use codex_file_system::SandboxLaunch;
use codex_file_system::merge_optional_sandbox_cleanup;
use codex_protocol::exec_output::SandboxCleanupState;
use codex_protocol::exec_output::SandboxCleanupSummary;
use codex_protocol::models::PermissionProfile;
use codex_protocol::permissions::FileSystemAccessMode;
use codex_protocol::permissions::FileSystemPath;
use codex_protocol::permissions::FileSystemSandboxEntry;
use codex_protocol::permissions::FileSystemSandboxPolicy;
use codex_protocol::permissions::FileSystemSpecialPath;
use codex_protocol::permissions::NetworkSandboxPolicy;
use codex_sandboxing::SandboxCleanupCustody;
use codex_sandboxing::SandboxCommand;
use codex_sandboxing::SandboxDirectSpawnTransformRequest;
use codex_sandboxing::SandboxExecRequest;
use codex_sandboxing::SandboxManager;
use codex_sandboxing::SandboxTransformRequest;
use codex_utils_absolute_path::AbsolutePathBuf;
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
use codex_utils_absolute_path::canonicalize_preserving_symlinks;
#[cfg(any(windows, test))]
use codex_utils_path_uri::LegacyAppPathString;
#[cfg(any(windows, test))]
use codex_utils_path_uri::PathConvention;
use codex_utils_path_uri::PathUri;
use codex_utils_pty::Child;
use codex_utils_pty::ChildStdin;
use codex_utils_pty::Command;
#[cfg(target_os = "macos")]
use codex_utils_pty::DescriptorPolicy;
use codex_utils_pty::SpawnFallback;
#[cfg(any(windows, test))]
use tokio::io::AsyncBufReadExt;
#[cfg(any(windows, test))]
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

use crate::ExecServerRuntimeOptions;
use crate::FileSystemSandboxContext;
use crate::fs_helper::CODEX_FS_HELPER_ARG1;
use crate::fs_helper::FsHelperPayload;
use crate::fs_helper::FsHelperRequest;
use crate::fs_helper::FsHelperResponse;
use crate::rpc::internal_error;
use crate::rpc::invalid_request;

const FS_HELPER_ENV_ALLOWLIST: &[&str] = &["PATH", "TMPDIR", "TMP", "TEMP"];
/// Bounds a helper's exit after its response, and its sandbox launcher's
/// cleanup once the executor retires the helper launch.
const FS_HELPER_EXIT_TIMEOUT: Duration = Duration::from_secs(/*secs*/ 2);
#[cfg(any(windows, test))]
const MAX_FS_HELPER_STDERR_BYTES: u64 = 4096;
#[cfg(debug_assertions)]
const FS_HELPER_BAZEL_BWRAP_ENV_ALLOWLIST: &[&str] = &[
    "CARGO_BIN_EXE_bwrap",
    "RUNFILES_DIR",
    "RUNFILES_MANIFEST_FILE",
    "RUNFILES_MANIFEST_ONLY",
    "TEST_SRCDIR",
    "TEST_WORKSPACE",
];

/// Receipt state of one filesystem request: the merged cleanup summary and
/// whether any Linux sandbox launch was made.
#[derive(Clone, Copy, Default)]
struct FsHelperReceipt {
    sandbox_cleanup: Option<SandboxCleanupSummary>,
    launched: usize,
    collected: usize,
}

tokio::task_local! {
    /// Merged cleanup receipt of the helper launches made while serving one
    /// filesystem request. `ExecutorFileSystem` results cannot carry it, so the
    /// helper owner records it here for the RPC layer that builds the response.
    static FS_HELPER_CLEANUP_RECEIPT: Cell<FsHelperReceipt>;
}

/// Runs one filesystem request and returns, next to its output, the merged
/// cleanup summary of every sandboxed helper launch it made and the launch
/// evidence. Every Linux sandbox launch of this filesystem layer goes through
/// [`spawn_command`], which registers it, so a request that registered none
/// positively launched nothing (`SandboxLaunch::NotLaunched`).
pub(crate) async fn with_fs_helper_cleanup_receipt<F: Future>(
    operation: F,
) -> (F::Output, Option<SandboxCleanupSummary>, SandboxLaunch) {
    FS_HELPER_CLEANUP_RECEIPT
        .scope(Cell::new(FsHelperReceipt::default()), async move {
            let output = operation.await;
            let receipt = FS_HELPER_CLEANUP_RECEIPT.with(Cell::get);
            let launch = if receipt.launched != 0 {
                SandboxLaunch::Launched
            } else {
                SandboxLaunch::NotLaunched
            };
            // A failed spawn or cancelled collection still owes its own receipt.
            let cleanup = if receipt.launched == receipt.collected {
                receipt.sandbox_cleanup
            } else {
                None
            };
            (output, cleanup, launch)
        })
        .await
}

/// Registers a Linux sandbox launch with the current request, if a request
/// is collecting receipts.
fn record_fs_helper_launch() {
    let _ = FS_HELPER_CLEANUP_RECEIPT.try_with(|receipt| {
        let current = receipt.get();
        receipt.set(FsHelperReceipt {
            launched: current
                .launched
                .checked_add(1)
                .expect("filesystem launch count overflow"),
            ..current
        });
    });
}

/// Adds one helper launch's summary to the current request's receipt, if a
/// request is collecting one.
fn record_fs_helper_cleanup(summary: SandboxCleanupSummary) {
    let _ = FS_HELPER_CLEANUP_RECEIPT.try_with(|receipt| {
        let current = receipt.get();
        receipt.set(FsHelperReceipt {
            sandbox_cleanup: merge_optional_sandbox_cleanup(current.sandbox_cleanup, Some(summary)),
            collected: current
                .collected
                .checked_add(1)
                .expect("filesystem receipt count overflow"),
            ..current
        });
    });
}

/// The executor's custody of one filesystem helper's sandbox launch. A helper
/// has no original retirement deadline: it is retired when it exits.
#[must_use = "collect the helper's sandbox cleanup after the helper exits"]
pub(crate) struct FsHelperSandboxCleanup(Option<SandboxCleanupCustody>);

impl FsHelperSandboxCleanup {
    /// Retires the launch once the helper has exited (or was killed): the
    /// launcher gets [`FS_HELPER_EXIT_TIMEOUT`] to clean up, and its report is
    /// collected off the async runtime.
    pub(crate) async fn collect(self) -> Option<SandboxCleanupSummary> {
        let custody = self.0?;
        custody.tighten(std::time::Instant::now() + FS_HELPER_EXIT_TIMEOUT);
        let sandbox_cleanup = custody.collect(FS_HELPER_EXIT_TIMEOUT).await;
        if sandbox_cleanup.state == SandboxCleanupState::Complete {
            tracing::debug!(sandbox_cleanup = ?sandbox_cleanup, "fs sandbox helper cleanup");
        } else {
            tracing::warn!(sandbox_cleanup = ?sandbox_cleanup, "fs sandbox helper cleanup");
        }
        record_fs_helper_cleanup(sandbox_cleanup);
        Some(sandbox_cleanup)
    }
}

#[derive(Debug, PartialEq, Eq)]
struct SandboxCwd {
    uri: PathUri,
    native: AbsolutePathBuf,
}

#[derive(Clone, Debug)]
pub(crate) struct FileSystemSandboxRunner {
    runtime_paths: ExecServerRuntimeOptions,
    helper_env: HashMap<String, String>,
}

impl FileSystemSandboxRunner {
    pub(crate) fn new(runtime_paths: ExecServerRuntimeOptions) -> Self {
        Self {
            runtime_paths,
            helper_env: helper_env(),
        }
    }

    #[tracing::instrument(name = "fs.sandbox_request", skip_all)]
    pub(crate) async fn run(
        &self,
        sandbox: &FileSystemSandboxContext,
        request: FsHelperRequest,
    ) -> Result<FsHelperPayload, JSONRPCErrorError> {
        let command = self.sandbox_command(sandbox)?;
        crate::run_integrity_checks(&command).await;
        let request_json = serde_json::to_vec(&request).map_err(json_error)?;
        run_command(command, request_json).await
    }

    #[tracing::instrument(
        name = "fs.sandbox_prepare",
        skip_all,
        fields(permission_entries = tracing::field::Empty)
    )]
    pub(crate) fn sandbox_command(
        &self,
        sandbox: &FileSystemSandboxContext,
    ) -> Result<SandboxExecRequest, JSONRPCErrorError> {
        let cwd = sandbox_cwd(sandbox)?;
        let native_workspace_roots = sandbox
            .workspace_roots
            .iter()
            .map(native_workspace_root)
            .collect::<Result<Vec<_>, _>>()?;
        let workspace_roots = native_workspace_roots.as_slice();
        sandbox
            .validate_file_system_paths_for_current_host()
            .map_err(|err| invalid_request(err.to_string()))?;
        let native_permissions = sandbox
            .permissions
            .clone()
            .materialize_project_roots_with_workspace_roots(workspace_roots);
        let mut file_system_policy = native_permissions.file_system_sandbox_policy();
        tracing::Span::current().record("permission_entries", file_system_policy.entries.len());
        let helper_read_roots = if sandbox.use_legacy_landlock {
            Vec::new()
        } else {
            helper_read_roots(&self.runtime_paths)
        };
        add_helper_runtime_permissions(
            &mut file_system_policy,
            &helper_read_roots,
            cwd.native.as_path(),
        );
        // Linux resolves aliases in the sandbox helper. Doing it here also probes
        // unrelated permission roots synchronously on the executor's runtime thread.
        #[cfg(not(target_os = "linux"))]
        normalize_file_system_policy_root_aliases(&mut file_system_policy)?;
        #[cfg(windows)]
        bind_windows_cwd_relative_deny_read_globs(&mut file_system_policy, &cwd.uri)?;
        let network_policy = NetworkSandboxPolicy::Restricted;
        let permission_profile = PermissionProfile::from_runtime_permissions_with_enforcement(
            native_permissions.enforcement(),
            &file_system_policy,
            network_policy,
        );
        self.sandbox_exec_request(&permission_profile, &cwd, workspace_roots, sandbox)
    }

    fn sandbox_exec_request(
        &self,
        permission_profile: &PermissionProfile,
        cwd: &SandboxCwd,
        workspace_roots: &[AbsolutePathBuf],
        sandbox_context: &FileSystemSandboxContext,
    ) -> Result<SandboxExecRequest, JSONRPCErrorError> {
        let helper = &self.runtime_paths.codex_self_exe;
        let sandbox_manager = SandboxManager::for_file_system_helpers()
            .with_linux_sandbox_pid_namespace(self.runtime_paths.linux_sandbox_pid_namespace);
        #[cfg(target_os = "macos")]
        let sandbox_manager = sandbox_manager.with_allowed_symlinked_codex_home(
            self.runtime_paths.allowed_symlinked_codex_home.clone(),
        );
        let (selection, windows_sandbox_level) =
            crate::sandbox_selection::sandbox_selection(sandbox_context);
        // Requests use absolute paths; the helper can start at the filesystem root even if
        // the policy cwd was removed. Keep its drive or share for Windows `:root` rules.
        let helper_cwd = cwd
            .native
            .ancestors()
            .last()
            .ok_or_else(|| invalid_request("filesystem sandbox cwd has no root".to_string()))?;
        let command = SandboxCommand {
            program: helper.as_path().as_os_str().to_owned(),
            args: vec![CODEX_FS_HELPER_ARG1.to_string()],
            cwd: PathUri::from_abs_path(&helper_cwd),
            env: self.helper_env.clone(),
            managed_network: None,
            additional_permissions: None,
        };
        let mut request = sandbox_manager
            .transform_for_direct_spawn(SandboxDirectSpawnTransformRequest {
                workspace_roots,
                windows_sandbox_proxy_settings_mode:
                    codex_sandboxing::WindowsSandboxProxySettingsMode::Preserve,
                transform: SandboxTransformRequest {
                    command,
                    permissions: permission_profile,
                    workspace_roots: None,
                    runtime_readable_paths: &[],
                    selection,
                    enforce_managed_network: false,
                    environment_id: None,
                    network: None,
                    sandbox_policy_cwd: &cwd.uri,
                    sandbox_exe: if cfg!(windows) {
                        Some(self.runtime_paths.codex_self_exe.as_path())
                    } else {
                        self.runtime_paths.codex_linux_sandbox_exe.as_deref()
                    },
                    use_legacy_landlock: sandbox_context.use_legacy_landlock,
                    windows_sandbox_level,
                },
            })
            .map_err(|err| match err {
                codex_sandboxing::SandboxTransformError::SandboxUnavailable => invalid_request(
                    "filesystem sandbox cannot be enforced on this executor".to_string(),
                ),
                err => invalid_request(format!("failed to prepare fs sandbox: {err}")),
            })?;
        request.sandbox_override = sandbox_context.sandbox_override;
        Ok(request)
    }
}

fn sandbox_cwd(sandbox: &FileSystemSandboxContext) -> Result<SandboxCwd, JSONRPCErrorError> {
    Ok(SandboxCwd {
        native: native_sandbox_cwd(&sandbox.cwd)?,
        uri: sandbox.cwd.clone(),
    })
}

fn native_sandbox_cwd(cwd: &PathUri) -> Result<AbsolutePathBuf, JSONRPCErrorError> {
    cwd.to_abs_path()
        .map_err(|err| invalid_request(err.to_string()))
}

fn native_workspace_root(root: &PathUri) -> Result<AbsolutePathBuf, JSONRPCErrorError> {
    root.to_abs_path().map_err(|err| {
        invalid_request(format!(
            "file system sandbox workspace root is not native to this exec-server host: {err}"
        ))
    })
}

fn helper_read_roots(runtime_paths: &ExecServerRuntimeOptions) -> Vec<AbsolutePathBuf> {
    let mut roots = vec![runtime_paths.codex_self_exe.clone()];
    if let Some(path) = &runtime_paths.codex_linux_sandbox_exe
        && !roots.contains(path)
    {
        roots.push(path.clone());
    }
    roots
}

fn add_helper_runtime_permissions(
    file_system_policy: &mut FileSystemSandboxPolicy,
    helper_read_roots: &[AbsolutePathBuf],
    cwd: &std::path::Path,
) {
    if !file_system_policy.has_full_disk_read_access() {
        let minimal_read_entry = FileSystemSandboxEntry::new(
            FileSystemPath::Special {
                value: FileSystemSpecialPath::Minimal,
            },
            FileSystemAccessMode::Read,
        );
        if !file_system_policy.entries.contains(&minimal_read_entry) {
            file_system_policy.entries.push(minimal_read_entry);
        }
    }

    for helper_read_root in helper_read_roots {
        if file_system_policy.can_read_local_path_with_cwd(helper_read_root.as_path(), cwd) {
            continue;
        }

        file_system_policy.entries.push(FileSystemSandboxEntry::new(
            helper_read_root.clone().into(),
            FileSystemAccessMode::Read,
        ));
    }
}

#[cfg(any(windows, test))]
fn bind_windows_cwd_relative_deny_read_globs(
    file_system_policy: &mut FileSystemSandboxPolicy,
    cwd: &PathUri,
) -> Result<(), JSONRPCErrorError> {
    // The Windows direct-spawn wrapper reevaluates the profile using the helper cwd.
    // Bind cwd-relative denials before moving the helper to the filesystem root.
    for entry in &mut file_system_policy.entries {
        if let FileSystemPath::GlobPattern { pattern } = &mut entry.path
            && entry.access == FileSystemAccessMode::Deny
            && PathConvention::Windows
                .home_relative_suffix(pattern)
                .is_none()
            && LegacyAppPathString::from_string(pattern.as_str())
                .to_path_uri(PathConvention::Windows)
                .is_err()
        {
            cwd.validate_glob_directory(PathConvention::Windows)
                .map_err(|err| invalid_request(err.to_string()))?;
            *pattern = cwd
                .join(pattern.as_str())
                .map_err(|err| invalid_request(err.to_string()))?
                .inferred_native_path_string();
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn normalize_file_system_policy_root_aliases(
    file_system_policy: &mut FileSystemSandboxPolicy,
) -> Result<(), JSONRPCErrorError> {
    for entry in &mut file_system_policy.entries {
        // Alias normalization uses this executor's filesystem; leave foreign
        // or opaque PathUris unchanged.
        if let FileSystemPath::Path { path } = &mut entry.path
            && let Ok(native_path) = path.to_abs_path()
        {
            #[cfg(target_os = "macos")]
            {
                *path = native_path
                    .normalize_system_aliases()
                    .map_err(|error| {
                        invalid_request(format!("failed to normalize {path}: {error}"))
                    })?
                    .into();
            }
            #[cfg(not(target_os = "macos"))]
            {
                *path = normalize_top_level_alias(native_path).into();
            }
        }
    }
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn normalize_top_level_alias(path: AbsolutePathBuf) -> AbsolutePathBuf {
    let raw_path = path.to_path_buf();
    for ancestor in raw_path.ancestors() {
        if std::fs::symlink_metadata(ancestor).is_err() {
            continue;
        }
        let Ok(normalized_ancestor) = canonicalize_preserving_symlinks(ancestor) else {
            continue;
        };
        if normalized_ancestor == ancestor {
            continue;
        }
        let Ok(suffix) = raw_path.strip_prefix(ancestor) else {
            continue;
        };
        if let Ok(normalized_path) =
            AbsolutePathBuf::from_absolute_path(normalized_ancestor.join(suffix))
        {
            return normalized_path;
        }
    }
    path
}

fn helper_env() -> HashMap<String, String> {
    helper_env_from_vars(std::env::vars_os())
}

fn helper_env_from_vars(
    vars: impl IntoIterator<Item = (std::ffi::OsString, std::ffi::OsString)>,
) -> HashMap<String, String> {
    vars.into_iter()
        .filter_map(|(key, value)| {
            let key = key.to_string_lossy();
            helper_env_key_is_allowed(&key)
                .then(|| (key.into_owned(), value.to_string_lossy().into_owned()))
        })
        .collect()
}

fn helper_env_key_is_allowed(key: &str) -> bool {
    FS_HELPER_ENV_ALLOWLIST.contains(&key)
        // CoreFoundation consults this before falling back to user lookup during helper startup.
        || (cfg!(target_os = "macos") && key == "__CF_USER_TEXT_ENCODING")
        || bazel_bwrap_env_key_is_allowed(key)
        // MXC needs SystemDrive to resolve platform directories, and SystemRoot and
        // LOCALAPPDATA to create the sandboxed helper process.
        || (cfg!(windows)
            && ["PATH", "SystemDrive", "SystemRoot", "LOCALAPPDATA"]
                .iter()
                .any(|allowed| key.eq_ignore_ascii_case(allowed)))
}

#[cfg(debug_assertions)]
fn bazel_bwrap_env_key_is_allowed(key: &str) -> bool {
    option_env!("BAZEL_PACKAGE").is_some() && FS_HELPER_BAZEL_BWRAP_ENV_ALLOWLIST.contains(&key)
}

#[cfg(not(debug_assertions))]
fn bazel_bwrap_env_key_is_allowed(_key: &str) -> bool {
    false
}

#[tracing::instrument(
    name = "fs.sandbox_execute",
    skip_all,
    fields(sandbox_cleanup = tracing::field::Empty)
)]
async fn run_command(
    command: SandboxExecRequest,
    request_json: Vec<u8>,
) -> Result<FsHelperPayload, JSONRPCErrorError> {
    let (child, sandbox_cleanup) = spawn_command(command, ChildStdin::Piped)?;
    // The exchange consumes the helper (reaped, or killed on drop) before the
    // launch is retired and its cleanup collected.
    let result = exchange_with_helper(child, request_json).await;
    let sandbox_cleanup = sandbox_cleanup.collect().await;
    if let Some(sandbox_cleanup) = &sandbox_cleanup {
        tracing::Span::current().record("sandbox_cleanup", tracing::field::debug(sandbox_cleanup));
    }
    result
}

async fn exchange_with_helper(
    mut child: Child,
    request_json: Vec<u8>,
) -> Result<FsHelperPayload, JSONRPCErrorError> {
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| internal_error("failed to open fs sandbox helper stdin".to_string()))?;

    #[cfg(windows)]
    let mut request_json = request_json;
    #[cfg(windows)]
    request_json.push(b'\n');
    stdin.write_all(&request_json).await.map_err(io_error)?;

    #[cfg(windows)]
    let response = {
        stdin.flush().await.map_err(io_error)?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| internal_error("failed to open fs sandbox helper stdout".to_string()))?;
        let stderr = drain_helper_stderr(&mut child);
        let response = read_helper_response(stdout).await;
        drop(stdin);
        reap_helper_after_response(child, stderr).await?;
        response?
    };

    #[cfg(not(windows))]
    let response = {
        stdin.shutdown().await.map_err(io_error)?;
        drop(stdin);
        wait_for_helper_output(child).await?.stdout
    };

    let response = serde_json::from_slice(&response).map_err(json_error)?;
    match response {
        FsHelperResponse::Ok(payload) => Ok(payload),
        FsHelperResponse::Error(error) => Err(error),
    }
}

#[cfg(any(windows, test))]
pub(crate) async fn read_helper_response(
    stdout: impl tokio::io::AsyncRead + Unpin,
) -> Result<Vec<u8>, JSONRPCErrorError> {
    let mut response = Vec::new();
    let bytes_read = tokio::io::BufReader::new(stdout)
        .read_until(b'\n', &mut response)
        .await
        .map_err(io_error)?;
    if bytes_read == 0 {
        return Err(internal_error(
            "fs sandbox helper closed stdout without responding".to_string(),
        ));
    }
    Ok(response)
}

#[cfg(any(windows, test))]
pub(crate) fn drain_helper_stderr(
    child: &mut Child,
) -> tokio::task::JoinHandle<Result<Vec<u8>, std::io::Error>> {
    let stderr_pipe = child.stderr.take();
    tokio::spawn(async move {
        let mut stderr = Vec::new();
        if let Some(mut stderr_pipe) = stderr_pipe {
            (&mut stderr_pipe)
                .take(MAX_FS_HELPER_STDERR_BYTES)
                .read_to_end(&mut stderr)
                .await?;
            tokio::io::copy(&mut stderr_pipe, &mut tokio::io::sink()).await?;
        }
        Ok::<_, std::io::Error>(stderr)
    })
}

#[cfg(any(windows, test))]
pub(crate) async fn reap_helper_after_response(
    mut child: Child,
    stderr: tokio::task::JoinHandle<Result<Vec<u8>, std::io::Error>>,
) -> Result<(), JSONRPCErrorError> {
    let (status, stderr) = match tokio::time::timeout(FS_HELPER_EXIT_TIMEOUT, async {
        tokio::try_join!(child.wait(), async {
            stderr.await.map_err(std::io::Error::other)?
        })
    })
    .await
    {
        Ok(result) => result.map_err(io_error)?,
        Err(_) => {
            tokio::time::timeout(FS_HELPER_EXIT_TIMEOUT, child.kill())
                .await
                .map_err(|_| {
                    internal_error("fs sandbox helper did not stop after its response".to_string())
                })?
                .map_err(io_error)?;
            return Ok(());
        }
    };
    if status.success() {
        return Ok(());
    }

    Err(internal_error(format!(
        "fs sandbox helper failed with status {status}: {stderr}",
        stderr = String::from_utf8_lossy(&stderr).trim()
    )))
}

#[cfg(not(windows))]
pub(crate) async fn wait_for_helper_output(
    child: Child,
) -> Result<std::process::Output, JSONRPCErrorError> {
    let output = child.wait_with_output().await.map_err(io_error)?;
    if !output.status.success() {
        return Err(internal_error(format!(
            "fs sandbox helper failed with status {status}: {stderr}",
            status = output.status,
            stderr = String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(output)
}

/// Spawns a filesystem helper and takes the executor's custody of its
/// sandbox launch. The caller collects the cleanup after the helper exits.
pub(crate) fn spawn_command(
    SandboxExecRequest {
        command: mut argv,
        cwd,
        mut env,
        arg0,
        sandbox,
        ..
    }: SandboxExecRequest,
    stdin: ChildStdin,
) -> Result<(Child, FsHelperSandboxCleanup), JSONRPCErrorError> {
    if argv.is_empty() {
        return Err(invalid_request("fs sandbox command was empty".to_string()));
    }
    // No original deadline: the helper launch is retired when the helper exits.
    let mut custody = SandboxCleanupCustody::for_launch(
        sandbox, /*cleanup_deadline*/ None, /*scope*/ None,
    )
    .map_err(io_error)?;
    if let Some(custody) = &custody {
        custody
            .attach_to_launcher_argv(&mut argv)
            .map_err(io_error)?;
        // From here on the request owes a receipt, even if the spawn fails.
        record_fs_helper_launch();
    }
    let Some((program, args)) = argv.split_first() else {
        return Err(invalid_request("fs sandbox command was empty".to_string()));
    };
    let mut command = Command::new(program);
    #[cfg(unix)]
    if let Some(arg0) = arg0 {
        command.arg0(arg0);
    }
    #[cfg(not(unix))]
    let _ = arg0;
    command.args(args);
    // TODO(anp): Keep PathUri through the filesystem helper launch boundary.
    let cwd = cwd.to_abs_path().map_err(io_error)?;
    command.current_dir(cwd.as_path());
    env.retain(|name, _| !codex_protocol::shell_environment::is_non_inheritable_env_var(name));
    command.envs(env);
    command.stdin(stdin);
    // A helper is a known executable: native launch errors must not retry through fork.
    command.fallback(SpawnFallback::ReturnError);
    // macOS cannot receive passed fds with close-on-exec set atomically.
    #[cfg(target_os = "macos")]
    command.descriptor_policy(DescriptorPolicy::Explicit);
    // The custody's launcher ends are CLOEXEC; only the helper's copies are
    // made inheritable. The custody keeps them open until `spawned`.
    #[cfg(unix)]
    if let Some(custody) = &custody {
        command.inherit_fds(&custody.inherited_fds());
    }
    let child = command.spawn();
    if let Some(custody) = &mut custody {
        // Pin the launcher (our own, not yet reaped child) for the join.
        custody.spawned(child.as_ref().ok().and_then(Child::id));
    }
    Ok((child.map_err(io_error)?, FsHelperSandboxCleanup(custody)))
}

pub(crate) fn io_error(err: std::io::Error) -> JSONRPCErrorError {
    internal_error(err.to_string())
}

fn json_error(err: serde_json::Error) -> JSONRPCErrorError {
    internal_error(format!(
        "failed to encode or decode fs sandbox helper message: {err}"
    ))
}

#[cfg(test)]
#[path = "fs_sandbox_windows_tests.rs"]
mod windows_tests;

#[cfg(test)]
mod tests {
    use codex_protocol::sandbox::SandboxOverride;
    use std::collections::HashMap;
    use std::ffi::OsString;

    use codex_protocol::models::PermissionProfile;
    use codex_protocol::permissions::FileSystemAccessMode;
    use codex_protocol::permissions::FileSystemPath;
    use codex_protocol::permissions::FileSystemSandboxEntry;
    use codex_protocol::permissions::FileSystemSandboxPolicy;
    use codex_protocol::permissions::FileSystemSpecialPath;
    use codex_protocol::permissions::NetworkSandboxPolicy;
    use codex_utils_absolute_path::AbsolutePathBuf;
    use codex_utils_path_uri::PathUri;
    use pretty_assertions::assert_eq;

    use crate::ExecServerRuntimeOptions;

    use super::FileSystemSandboxRunner;
    use super::SandboxCwd;
    use super::add_helper_runtime_permissions;
    use super::bind_windows_cwd_relative_deny_read_globs;
    use super::helper_env;
    use super::helper_env_from_vars;
    use super::helper_env_key_is_allowed;
    use super::helper_read_roots;
    use super::sandbox_cwd;

    use super::SandboxCleanupState;
    use super::SandboxCleanupSummary;
    use super::SandboxLaunch;
    use super::record_fs_helper_cleanup;
    use super::record_fs_helper_launch;
    use super::with_fs_helper_cleanup_receipt;

    fn cleanup_complete() -> SandboxCleanupSummary {
        SandboxCleanupSummary {
            state: SandboxCleanupState::Complete,
            reaped: 1,
            preserved_unproven: 0,
            host_owned: 0,
            handed_off: 0,
            timed_out: 0,
            joined: true,
        }
    }

    #[tokio::test]
    async fn request_receipt_missing_launch_cannot_be_hidden() {
        for missing_first in [true, false] {
            let ((), receipt, launch) = with_fs_helper_cleanup_receipt(async {
                if missing_first {
                    record_fs_helper_launch();
                }
                record_fs_helper_launch();
                record_fs_helper_cleanup(cleanup_complete());
                if !missing_first {
                    record_fs_helper_launch();
                }
            })
            .await;
            assert_eq!(launch, SandboxLaunch::Launched);
            assert_eq!(receipt, None);
        }
    }

    #[tokio::test]
    async fn request_receipt_requires_one_collection_per_launch() {
        let ((), receipt, launch) = with_fs_helper_cleanup_receipt(async {
            record_fs_helper_launch();
            record_fs_helper_launch();
            record_fs_helper_cleanup(cleanup_complete());
            record_fs_helper_cleanup(cleanup_complete());
        })
        .await;
        assert_eq!(launch, SandboxLaunch::Launched);
        let receipt = receipt.unwrap();
        assert_eq!(receipt.reaped, 2);
        assert_eq!(receipt.state, SandboxCleanupState::Complete);
    }

    #[tokio::test]
    async fn request_receipt_no_launch_is_positive_absence() {
        let ((), receipt, launch) = with_fs_helper_cleanup_receipt(async {}).await;
        assert_eq!((receipt, launch), (None, SandboxLaunch::NotLaunched));
    }

    #[test]
    fn helper_permissions_enable_minimal_reads_for_restricted_profile() {
        let cwd = AbsolutePathBuf::from_absolute_path(std::env::temp_dir().as_path())
            .expect("absolute cwd");
        let mut policy = restricted_policy(Vec::new());

        add_helper_runtime_permissions(&mut policy, /*helper_read_roots*/ &[], cwd.as_path());

        assert!(policy.include_platform_defaults());
    }

    #[test]
    fn helper_permissions_enable_minimal_reads_for_restricted_profile_with_writes() {
        let cwd = AbsolutePathBuf::from_absolute_path(std::env::temp_dir().as_path())
            .expect("absolute cwd");
        let mut policy = restricted_policy(vec![path_entry(
            cwd.join("writable"),
            FileSystemAccessMode::Write,
        )]);

        add_helper_runtime_permissions(&mut policy, /*helper_read_roots*/ &[], cwd.as_path());

        assert!(policy.include_platform_defaults());
    }

    #[test]
    fn helper_permissions_preserve_existing_writes() {
        let codex_self_exe = std::env::current_exe().expect("current exe");
        let runtime_paths =
            ExecServerRuntimeOptions::new(codex_self_exe, /*codex_linux_sandbox_exe*/ None)
                .expect("runtime paths");
        let cwd = AbsolutePathBuf::from_absolute_path(std::env::temp_dir().as_path())
            .expect("absolute cwd");
        let writable = cwd.join("writable");
        let mut policy = restricted_policy(vec![path_entry(
            writable.clone(),
            FileSystemAccessMode::Write,
        )]);
        let readable = runtime_paths.codex_self_exe.clone();

        add_helper_runtime_permissions(
            &mut policy,
            &helper_read_roots(&runtime_paths),
            cwd.as_path(),
        );

        assert!(policy.can_read_local_path_with_cwd(readable.as_path(), cwd.as_path()));
        assert!(policy.can_write_local_path_with_cwd(writable.as_path(), cwd.as_path()));
    }

    #[test]
    fn helper_env_carries_only_allowlisted_runtime_vars() {
        let env = helper_env();

        let expected = std::env::vars_os()
            .filter_map(|(key, value)| {
                let key = key.to_string_lossy();
                helper_env_key_is_allowed(&key)
                    .then(|| (key.into_owned(), value.to_string_lossy().into_owned()))
            })
            .collect::<HashMap<_, _>>();

        assert_eq!(env, expected);
    }

    #[test]
    fn helper_env_preserves_path_for_system_bwrap_discovery_without_leaking_secrets() {
        let env = helper_env_from_vars(
            [
                ("PATH", "/usr/bin:/bin"),
                ("TMPDIR", "/tmp/codex"),
                ("TMP", "/tmp"),
                ("TEMP", "/tmp"),
                ("HOME", "/home/user"),
                ("OPENAI_API_KEY", "secret"),
                ("HTTPS_PROXY", "http://proxy.example"),
            ]
            .map(|(key, value)| (OsString::from(key), OsString::from(value))),
        );

        assert_eq!(
            env,
            HashMap::from([
                ("PATH".to_string(), "/usr/bin:/bin".to_string()),
                ("TMPDIR".to_string(), "/tmp/codex".to_string()),
                ("TMP".to_string(), "/tmp".to_string()),
                ("TEMP".to_string(), "/tmp".to_string()),
            ])
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn helper_env_preserves_corefoundation_text_encoding() {
        let env = helper_env_from_vars(
            [
                ("__CF_USER_TEXT_ENCODING", "0x1F6:0x0:0x0"),
                ("HOME", "/Users/test"),
            ]
            .map(|(key, value)| (OsString::from(key), OsString::from(value))),
        );

        assert_eq!(
            env,
            HashMap::from([(
                "__CF_USER_TEXT_ENCODING".to_string(),
                "0x1F6:0x0:0x0".to_string(),
            )])
        );
    }

    #[cfg(windows)]
    #[test]
    fn helper_env_preserves_windows_runtime_variables_without_leaking_secrets() {
        let env = helper_env_from_vars(
            [
                ("Path", r"C:\Windows\System32"),
                ("LocalAppData", r"C:\Users\test\AppData\Local"),
                ("SystemDrive", "C:"),
                ("systemROOT", r"C:\Windows"),
                ("PATH_INJECTION", "bad"),
                ("OPENAI_API_KEY", "secret"),
            ]
            .map(|(key, value)| (OsString::from(key), OsString::from(value))),
        );

        assert_eq!(
            env,
            HashMap::from([
                ("Path".to_string(), r"C:\Windows\System32".to_string()),
                (
                    "LocalAppData".to_string(),
                    r"C:\Users\test\AppData\Local".to_string()
                ),
                ("SystemDrive".to_string(), "C:".to_string()),
                ("systemROOT".to_string(), r"C:\Windows".to_string()),
            ])
        );
    }

    #[test]
    fn sandbox_exec_request_carries_helper_env() {
        let Some((path_key, path)) = std::env::vars_os().find(|(key, _)| {
            let key = key.to_string_lossy();
            key == "PATH" || (cfg!(windows) && key.eq_ignore_ascii_case("PATH"))
        }) else {
            return;
        };
        let path_key = path_key.to_string_lossy().into_owned();
        let path = path.to_string_lossy().into_owned();
        let codex_self_exe = std::env::current_exe().expect("current exe");
        let runtime_paths =
            ExecServerRuntimeOptions::new(codex_self_exe.clone(), Some(codex_self_exe))
                .expect("runtime paths");
        let runner = FileSystemSandboxRunner::new(runtime_paths);
        let native_cwd = AbsolutePathBuf::current_dir().expect("cwd");
        let cwd = PathUri::from_abs_path(&native_cwd);
        let file_system_policy = restricted_policy(vec![
            #[cfg(windows)]
            special_entry(FileSystemSpecialPath::Root, FileSystemAccessMode::Read),
            path_entry(native_cwd.clone(), FileSystemAccessMode::Write),
        ]);
        let network_policy = NetworkSandboxPolicy::Restricted;
        let permission_profile =
            PermissionProfile::from_runtime_permissions(&file_system_policy, network_policy);
        let sandbox_context = sandbox_context_with_cwd(&file_system_policy, cwd.clone());
        let sandbox_cwd = SandboxCwd {
            uri: cwd,
            native: native_cwd,
        };
        #[cfg(windows)]
        let sandbox_context = {
            let error = runner
                .sandbox_exec_request(
                    &permission_profile,
                    &sandbox_cwd,
                    std::slice::from_ref(&sandbox_cwd.native),
                    &sandbox_context,
                )
                .expect_err("disabled Windows sandbox must not run the helper unsandboxed");
            assert_eq!(
                error.message,
                "filesystem sandbox cannot be enforced on this executor"
            );
            crate::FileSystemSandboxContext {
                windows_sandbox_selection:
                    codex_file_system::WindowsSandboxSelection::RestrictedToken,
                ..sandbox_context
            }
        };

        let request = runner
            .sandbox_exec_request(
                &permission_profile,
                &sandbox_cwd,
                std::slice::from_ref(&sandbox_cwd.native),
                &sandbox_context,
            )
            .expect("sandbox exec request");

        assert_eq!(request.env.get(&path_key), Some(&path));
    }

    /// Removing the selected directory must not change permission anchoring or prevent launch.
    #[test]
    fn sandbox_exec_request_uses_filesystem_root_and_preserves_policy_cwd() {
        let codex_self_exe = std::env::current_exe().expect("current exe");
        let runtime_paths =
            ExecServerRuntimeOptions::new(codex_self_exe.clone(), Some(codex_self_exe))
                .expect("runtime paths");
        let runner = FileSystemSandboxRunner::new(runtime_paths);
        let selected = tempfile::tempdir().expect("selected directory");
        let selected_cwd = AbsolutePathBuf::from_absolute_path(selected.path()).expect("cwd");
        let root = selected_cwd.ancestors().last().expect("filesystem root");
        let policy = restricted_policy(vec![special_entry(
            FileSystemSpecialPath::Root,
            FileSystemAccessMode::Read,
        )]);
        let mut sandbox_context =
            sandbox_context_with_cwd(&policy, PathUri::from_abs_path(&selected_cwd));
        sandbox_context.sandbox_override = SandboxOverride::EscalatedSandboxWithRestrictions;
        #[cfg(windows)]
        let sandbox_context = crate::FileSystemSandboxContext {
            windows_sandbox_selection: codex_file_system::WindowsSandboxSelection::RestrictedToken,
            ..sandbox_context
        };
        selected.close().expect("remove selected directory");

        let request = runner
            .sandbox_command(&sandbox_context)
            .expect("sandbox command");

        assert_eq!(
            (
                request.cwd,
                request.sandbox_policy_cwd,
                request.sandbox_override
            ),
            (
                PathUri::from_abs_path(&root),
                PathUri::from_abs_path(&selected_cwd),
                SandboxOverride::EscalatedSandboxWithRestrictions,
            )
        );
    }

    /// Tilde patterns stay home-relative when Windows sandbox commands start at another cwd.
    #[test]
    fn windows_cwd_relative_denial_binding_keeps_tilde_patterns_home_relative() {
        let cwd = PathUri::parse("file:///C:/policy/checkout").expect("Windows policy cwd");
        let mut policy = restricted_policy(
            [
                "~",
                "~/",
                r"~\",
                "~/private/*.env",
                r"~\private\*.env",
                "hidden/*.env",
                "~someone/private",
                "D:/absolute/*.env",
            ]
            .into_iter()
            .map(|pattern| {
                FileSystemSandboxEntry::new(
                    FileSystemPath::GlobPattern {
                        pattern: pattern.to_string(),
                    },
                    FileSystemAccessMode::Deny,
                )
            })
            .collect(),
        );

        bind_windows_cwd_relative_deny_read_globs(&mut policy, &cwd).expect("bind Windows policy");

        let patterns = policy
            .entries
            .into_iter()
            .filter_map(|entry| match entry.path {
                FileSystemPath::GlobPattern { pattern } => Some(pattern),
                FileSystemPath::Path { .. } | FileSystemPath::Special { .. } => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            patterns,
            [
                "~",
                "~/",
                r"~\",
                "~/private/*.env",
                r"~\private\*.env",
                r"C:\policy\checkout\hidden\*.env",
                r"C:\policy\checkout\~someone\private",
                "D:/absolute/*.env",
            ]
        );
    }

    /// Windows wrapper policy checks must keep cwd-relative denials anchored to the selection.
    #[cfg(windows)]
    #[test]
    fn sandbox_exec_request_binds_windows_relative_globs_to_policy_cwd() {
        let codex_self_exe = std::env::current_exe().expect("current exe");
        let runtime_paths =
            ExecServerRuntimeOptions::new(codex_self_exe, /*codex_linux_sandbox_exe*/ None)
                .expect("runtime paths");
        let runner = FileSystemSandboxRunner::new(runtime_paths);
        let selected = tempfile::tempdir().expect("selected directory");
        let selected_cwd = AbsolutePathBuf::from_absolute_path(selected.path()).expect("cwd");
        let cwd_uri = PathUri::from_abs_path(&selected_cwd);
        let pattern = "hidden/*.env";
        let absolute_pattern = cwd_uri
            .join(pattern)
            .expect("absolute glob")
            .inferred_native_path_string();
        let policy = restricted_policy(vec![
            special_entry(FileSystemSpecialPath::Root, FileSystemAccessMode::Read),
            FileSystemSandboxEntry::new(
                FileSystemPath::GlobPattern {
                    pattern: pattern.to_string(),
                },
                FileSystemAccessMode::Deny,
            ),
        ]);
        let sandbox_context = crate::FileSystemSandboxContext {
            windows_sandbox_selection: codex_file_system::WindowsSandboxSelection::RestrictedToken,
            ..sandbox_context_with_cwd(&policy, cwd_uri)
        };
        selected.close().expect("remove selected directory");

        let request = runner
            .sandbox_command(&sandbox_context)
            .expect("sandbox command");

        assert_eq!(
            request
                .permission_profile
                .file_system_sandbox_policy()
                .entries
                .into_iter()
                .find(|entry| { matches!(entry.path, FileSystemPath::GlobPattern { .. }) }),
            Some(FileSystemSandboxEntry::new(
                FileSystemPath::GlobPattern {
                    pattern: absolute_pattern
                },
                FileSystemAccessMode::Deny,
            ))
        );
    }

    #[test]
    fn sandbox_cwd_uses_context_cwd() {
        let native_cwd = AbsolutePathBuf::from_absolute_path(std::env::temp_dir().as_path())
            .expect("absolute cwd");
        let cwd = PathUri::from_abs_path(&native_cwd);
        let policy = restricted_policy(vec![special_entry(
            FileSystemSpecialPath::project_roots(/*subpath*/ None),
            FileSystemAccessMode::Write,
        )]);
        let sandbox_context = sandbox_context_with_cwd(&policy, cwd.clone());

        assert_eq!(
            sandbox_cwd(&sandbox_context).expect("sandbox cwd"),
            SandboxCwd {
                uri: cwd,
                native: native_cwd
            }
        );
    }

    #[test]
    fn sandbox_cwd_rejects_non_native_context_cwd_without_fallback() {
        let cwd = non_native_cwd();
        let policy = restricted_policy(vec![special_entry(
            FileSystemSpecialPath::project_roots(/*subpath*/ None),
            FileSystemAccessMode::Write,
        )]);
        let sandbox_context = sandbox_context_with_cwd(&policy, cwd.clone());

        let err = sandbox_cwd(&sandbox_context).expect_err("non-native cwd should be rejected");

        assert_eq!(
            err,
            crate::rpc::invalid_request(format!(
                "'{cwd}' is invalid on '{}'",
                std::env::consts::OS
            ))
        );
    }

    #[test]
    fn helper_permissions_include_only_the_helper_executable() {
        let codex_self_exe = std::env::current_exe().expect("current exe");
        let runtime_paths =
            ExecServerRuntimeOptions::new(codex_self_exe, /*codex_linux_sandbox_exe*/ None)
                .expect("runtime paths");
        let cwd = AbsolutePathBuf::from_absolute_path(std::env::temp_dir().as_path())
            .expect("absolute cwd");
        let mut policy = restricted_policy(Vec::new());
        let parent = runtime_paths
            .codex_self_exe
            .parent()
            .expect("current exe parent");
        let sibling = parent.join("credentials.json");

        add_helper_runtime_permissions(
            &mut policy,
            &helper_read_roots(&runtime_paths),
            cwd.as_path(),
        );

        assert!(
            policy.can_read_local_path_with_cwd(
                runtime_paths.codex_self_exe.as_path(),
                cwd.as_path(),
            )
        );
        assert!(!policy.can_read_local_path_with_cwd(parent.as_path(), cwd.as_path()));
        assert!(!policy.can_read_local_path_with_cwd(sibling.as_path(), cwd.as_path()));
    }

    #[test]
    fn helper_permissions_include_only_linux_sandbox_alias_executable() {
        let root = tempfile::tempdir().expect("temp dir");
        let codex_self_exe = root.path().join("bin").join("codex");
        let codex_linux_sandbox_exe = root.path().join("aliases").join("codex-linux-sandbox");
        let runtime_paths =
            ExecServerRuntimeOptions::new(codex_self_exe, Some(codex_linux_sandbox_exe))
                .expect("runtime paths");
        let cwd = AbsolutePathBuf::from_absolute_path(std::env::temp_dir().as_path())
            .expect("absolute cwd");
        let mut policy = restricted_policy(Vec::new());
        let codex_parent = runtime_paths.codex_self_exe.parent().expect("codex parent");
        let alias = runtime_paths
            .codex_linux_sandbox_exe
            .as_ref()
            .expect("linux sandbox alias");
        let alias_parent = alias.parent().expect("alias parent");

        add_helper_runtime_permissions(
            &mut policy,
            &helper_read_roots(&runtime_paths),
            cwd.as_path(),
        );

        assert!(
            policy.can_read_local_path_with_cwd(
                runtime_paths.codex_self_exe.as_path(),
                cwd.as_path(),
            )
        );
        assert!(policy.can_read_local_path_with_cwd(alias.as_path(), cwd.as_path()));
        assert!(!policy.can_read_local_path_with_cwd(codex_parent.as_path(), cwd.as_path()));
        assert!(!policy.can_read_local_path_with_cwd(alias_parent.as_path(), cwd.as_path()));
    }

    fn restricted_policy(entries: Vec<FileSystemSandboxEntry>) -> FileSystemSandboxPolicy {
        FileSystemSandboxPolicy::restricted(entries)
    }

    fn sandbox_context_with_cwd(
        policy: &FileSystemSandboxPolicy,
        cwd: PathUri,
    ) -> crate::FileSystemSandboxContext {
        codex_file_system::FileSystemSandboxContext::from_permission_profile(
            PermissionProfile::from_runtime_permissions(policy, NetworkSandboxPolicy::Restricted),
            cwd,
        )
    }

    fn non_native_cwd() -> PathUri {
        #[cfg(unix)]
        let uri = "file://server/share/checkout";
        #[cfg(windows)]
        let uri = "file:///usr/local/checkout";

        PathUri::parse(uri).expect("non-native cwd URI")
    }

    fn path_entry(path: AbsolutePathBuf, access: FileSystemAccessMode) -> FileSystemSandboxEntry {
        FileSystemSandboxEntry {
            path: path.into(),
            access,
            missing_path_behavior: None,
        }
    }

    fn special_entry(
        value: FileSystemSpecialPath,
        access: FileSystemAccessMode,
    ) -> FileSystemSandboxEntry {
        FileSystemSandboxEntry {
            path: FileSystemPath::Special { value },
            access,
            missing_path_behavior: None,
        }
    }
}
