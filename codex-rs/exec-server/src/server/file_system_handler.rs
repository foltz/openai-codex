use std::io;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use codex_exec_server_protocol::JSONRPCErrorError;
use codex_exec_server_protocol::SandboxCleanupSummary;
use codex_exec_server_protocol::attach_sandbox_cleanup_to_error;
use codex_exec_server_protocol::mark_no_sandbox_launch_in_error;
use codex_file_system::FileSystemOutcome;
use codex_file_system::SandboxLaunch;

use crate::CapabilityRootsDiscoverParams;
use crate::CapabilityRootsDiscoverResponse;
use crate::CopyOptions;
use crate::CreateDirectoryOptions;
use crate::ExecServerRuntimeOptions;
use crate::ExecutorFileSystem;
use crate::GetMetadataOptions;
use crate::ReadFileOptions;
use crate::RemoveOptions;
use crate::WriteFileOptions;
use crate::file_handle::FileHandleManager;
use crate::fs_sandbox::with_fs_helper_cleanup_receipt;
use crate::local_file_system::LocalFileSystem;
use crate::protocol::FS_READ_DIRECTORY_METHOD;
use crate::protocol::FS_WRITE_FILE_METHOD;
use crate::protocol::FsCanonicalizeParams;
use crate::protocol::FsCanonicalizeResponse;
use crate::protocol::FsCloseParams;
use crate::protocol::FsCloseResponse;
use crate::protocol::FsCopyParams;
use crate::protocol::FsCopyResponse;
use crate::protocol::FsCreateDirectoryParams;
use crate::protocol::FsCreateDirectoryResponse;
use crate::protocol::FsGetMetadataParams;
use crate::protocol::FsGetMetadataResponse;
use crate::protocol::FsOpenParams;
use crate::protocol::FsOpenResponse;
use crate::protocol::FsReadBlockParams;
use crate::protocol::FsReadBlockResponse;
use crate::protocol::FsReadDirectoryEntry;
use crate::protocol::FsReadDirectoryParams;
use crate::protocol::FsReadDirectoryResponse;
use crate::protocol::FsReadFileParams;
use crate::protocol::FsReadFileResponse;
use crate::protocol::FsRemoveParams;
use crate::protocol::FsRemoveResponse;
use crate::protocol::FsWalkParams;
use crate::protocol::FsWalkResponse;
use crate::protocol::FsWriteBlockParams;
use crate::protocol::FsWriteBlockResponse;
use crate::protocol::FsWriteFileParams;
use crate::protocol::FsWriteFileResponse;
use crate::rpc::internal_error;
use crate::rpc::invalid_request;
use crate::rpc::not_found;

const MAX_FILE_HANDLE_ID_BYTES: usize = 32;
// Each read-directory entry needs four JSON values. Keep same-version
// producers comfortably below the shared 256K-value decoder budget.
const MAX_READ_DIRECTORY_ENTRIES: usize = 50_000;

#[derive(Clone)]
pub(crate) struct FileSystemHandler {
    file_system: LocalFileSystem,
    file_handles: FileHandleManager,
}

impl FileSystemHandler {
    pub(crate) fn new(runtime_paths: ExecServerRuntimeOptions) -> Self {
        Self {
            file_system: LocalFileSystem::with_runtime_paths(runtime_paths),
            file_handles: FileHandleManager::default(),
        }
    }

    pub(crate) async fn shutdown(&self) {
        self.file_handles.close_all();
    }

    pub(crate) async fn discover_capability_roots(
        &self,
        params: CapabilityRootsDiscoverParams,
    ) -> Result<CapabilityRootsDiscoverResponse, JSONRPCErrorError> {
        // Discovery reads through sandboxed helpers (batched or per root), which
        // are launches this executor owns: report their merged cleanup.
        let (result, sandbox_cleanup, launch) =
            with_fs_helper_cleanup_receipt(self.discover_capability_roots_inner(params)).await;
        match result {
            // Never forward a receipt or launch claim from the helper's output.
            Ok(response) => Ok(CapabilityRootsDiscoverResponse {
                sandbox_cleanup,
                no_sandbox_launch: launch == SandboxLaunch::NotLaunched,
                ..response
            }),
            Err(error) => Err(attach_receipt_to_error(error, sandbox_cleanup, launch)),
        }
    }

    async fn discover_capability_roots_inner(
        &self,
        params: CapabilityRootsDiscoverParams,
    ) -> Result<CapabilityRootsDiscoverResponse, JSONRPCErrorError> {
        let sandbox = params
            .roots
            .first()
            .and_then(|root| root.sandbox.as_ref())
            .filter(|sandbox| {
                sandbox
                    .validate_file_system_paths_for_current_host()
                    .is_ok()
                    && sandbox.should_read_from_sandbox()
                    && (!cfg!(target_os = "windows") || sandbox.windows_sandbox_is_requested())
                    && params
                        .roots
                        .iter()
                        .all(|root| root.sandbox.as_ref() == Some(*sandbox))
            })
            .cloned();

        if let Some(sandbox) = sandbox {
            let mut batched_params = params.clone();
            for root in &mut batched_params.roots {
                root.sandbox = None;
            }
            let result = match self.file_system.sandboxed() {
                Ok(file_system) => {
                    file_system
                        .discover_capability_roots(batched_params, &sandbox)
                        .await
                }
                Err(error) => Err(error),
            };
            match result {
                Ok(response) => return Ok(response),
                Err(error) => {
                    tracing::warn!(%error, "batched capability discovery failed; retrying roots separately");
                }
            }
        }

        crate::discover_capability_roots(&self.file_system, params)
            .await
            .map_err(|error| invalid_request(error.to_string()))
    }

    pub(crate) async fn open(
        &self,
        params: FsOpenParams,
    ) -> Result<FsOpenResponse, JSONRPCErrorError> {
        validate_file_handle_id(&params.handle_id)?;
        let (handle_id, sandbox_cleanup, launch) = with_fs_helper_cleanup_receipt(
            self.file_handles.open(
                params.handle_id,
                self.file_system
                    .open_file(&params.path, params.mode, params.sandbox.as_ref()),
            ),
        )
        .await;
        let handle_id = handle_id.map_err(|error| {
            attach_receipt_to_error(map_fs_error(error), sandbox_cleanup, launch)
        })?;
        Ok(FsOpenResponse {
            handle_id,
            sandbox_cleanup,
            no_sandbox_launch: launch == SandboxLaunch::NotLaunched,
        })
    }

    pub(crate) async fn read_block(
        &self,
        params: FsReadBlockParams,
    ) -> Result<FsReadBlockResponse, JSONRPCErrorError> {
        validate_file_handle_id(&params.handle_id)?;
        let block = self
            .file_handles
            .read_block(&params.handle_id, params.offset, params.len)
            .await
            .map_err(map_fs_error)?;
        Ok(FsReadBlockResponse {
            chunk: block.bytes.into(),
            eof: block.eof,
        })
    }

    pub(crate) async fn write_block(
        &self,
        params: FsWriteBlockParams,
    ) -> Result<FsWriteBlockResponse, JSONRPCErrorError> {
        validate_file_handle_id(&params.handle_id)?;
        self.file_handles
            .write_block(&params.handle_id, params.offset, params.chunk.into_inner())
            .await
            .map_err(map_fs_error)?;
        Ok(FsWriteBlockResponse {})
    }

    pub(crate) async fn close(
        &self,
        params: FsCloseParams,
    ) -> Result<FsCloseResponse, JSONRPCErrorError> {
        validate_file_handle_id(&params.handle_id)?;
        self.file_handles.close(&params.handle_id);
        Ok(FsCloseResponse {})
    }

    pub(crate) async fn read_file(
        &self,
        params: FsReadFileParams,
    ) -> Result<FsReadFileResponse, JSONRPCErrorError> {
        let (bytes, sandbox_cleanup, no_sandbox_launch) = fs_outcome(
            self.file_system
                .read_file_with_receipt(
                    &params.path,
                    ReadFileOptions {
                        follow_symlinks: params.follow_symlinks.unwrap_or(true),
                    },
                    params.sandbox.as_ref(),
                )
                .await,
        )?;
        Ok(FsReadFileResponse {
            data_base64: STANDARD.encode(bytes),
            sandbox_cleanup,
            no_sandbox_launch,
        })
    }

    pub(crate) async fn write_file(
        &self,
        params: FsWriteFileParams,
    ) -> Result<FsWriteFileResponse, JSONRPCErrorError> {
        let bytes = STANDARD.decode(params.data_base64).map_err(|err| {
            invalid_request(format!(
                "{FS_WRITE_FILE_METHOD} requires valid base64 dataBase64: {err}"
            ))
        })?;
        let ((), sandbox_cleanup, no_sandbox_launch) = fs_outcome(
            self.file_system
                .write_file_with_receipt(
                    &params.path,
                    bytes,
                    WriteFileOptions {
                        follow_symlinks: params.follow_symlinks.unwrap_or(true),
                    },
                    params.sandbox.as_ref(),
                )
                .await,
        )?;
        Ok(FsWriteFileResponse {
            sandbox_cleanup,
            no_sandbox_launch,
        })
    }

    pub(crate) async fn create_directory(
        &self,
        params: FsCreateDirectoryParams,
    ) -> Result<FsCreateDirectoryResponse, JSONRPCErrorError> {
        let recursive = params.recursive.unwrap_or(true);
        let ((), sandbox_cleanup, no_sandbox_launch) = fs_outcome(
            self.file_system
                .create_directory_with_receipt(
                    &params.path,
                    CreateDirectoryOptions {
                        recursive,
                        follow_symlinks: params.follow_symlinks.unwrap_or(true),
                    },
                    params.sandbox.as_ref(),
                )
                .await,
        )?;
        Ok(FsCreateDirectoryResponse {
            sandbox_cleanup,
            no_sandbox_launch,
        })
    }

    pub(crate) async fn get_metadata(
        &self,
        params: FsGetMetadataParams,
    ) -> Result<FsGetMetadataResponse, JSONRPCErrorError> {
        let (metadata, sandbox_cleanup, no_sandbox_launch) = fs_outcome(
            self.file_system
                .get_metadata_with_receipt(
                    &params.path,
                    GetMetadataOptions {
                        follow_symlinks: params.follow_symlinks.unwrap_or(true),
                    },
                    params.sandbox.as_ref(),
                )
                .await,
        )?;
        Ok(FsGetMetadataResponse {
            is_directory: metadata.is_directory,
            is_file: metadata.is_file,
            is_symlink: metadata.is_symlink,
            size: metadata.size,
            created_at_ms: metadata.created_at_ms,
            modified_at_ms: metadata.modified_at_ms,
            sandbox_cleanup,
            no_sandbox_launch,
        })
    }

    pub(crate) async fn canonicalize(
        &self,
        params: FsCanonicalizeParams,
    ) -> Result<FsCanonicalizeResponse, JSONRPCErrorError> {
        let (path, sandbox_cleanup, no_sandbox_launch) = fs_outcome(
            self.file_system
                .canonicalize_with_receipt(&params.path, params.sandbox.as_ref())
                .await,
        )?;
        Ok(FsCanonicalizeResponse {
            path,
            sandbox_cleanup,
            no_sandbox_launch,
        })
    }

    pub(crate) async fn read_directory(
        &self,
        params: FsReadDirectoryParams,
    ) -> Result<FsReadDirectoryResponse, JSONRPCErrorError> {
        let (entries, sandbox_cleanup, no_sandbox_launch) = fs_outcome(
            self.file_system
                .read_directory_with_receipt(&params.path, params.sandbox.as_ref())
                .await,
        )?;
        let entry_count = entries.len();
        if entry_count > MAX_READ_DIRECTORY_ENTRIES {
            let error = attach_sandbox_cleanup_to_error(
                internal_error(format!(
                    "{FS_READ_DIRECTORY_METHOD} returned {entry_count} entries; limit is {MAX_READ_DIRECTORY_ENTRIES}"
                )),
                sandbox_cleanup,
            );
            return Err(if no_sandbox_launch {
                mark_no_sandbox_launch_in_error(error)
            } else {
                error
            });
        }
        let entries = entries
            .into_iter()
            .map(|entry| FsReadDirectoryEntry {
                file_name: entry.file_name,
                is_directory: entry.is_directory,
                is_file: entry.is_file,
            })
            .collect();
        Ok(FsReadDirectoryResponse {
            entries,
            sandbox_cleanup,
            no_sandbox_launch,
        })
    }

    pub(crate) async fn walk(
        &self,
        params: FsWalkParams,
    ) -> Result<FsWalkResponse, JSONRPCErrorError> {
        let (outcome, sandbox_cleanup, no_sandbox_launch) = fs_outcome(
            self.file_system
                .walk_with_receipt(&params.path, params.options, params.sandbox.as_ref())
                .await,
        )?;
        Ok(FsWalkResponse {
            outcome,
            sandbox_cleanup,
            no_sandbox_launch,
        })
    }

    pub(crate) async fn remove(
        &self,
        params: FsRemoveParams,
    ) -> Result<FsRemoveResponse, JSONRPCErrorError> {
        let recursive = params.recursive.unwrap_or(true);
        let force = params.force.unwrap_or(true);
        let ((), sandbox_cleanup, no_sandbox_launch) = fs_outcome(
            self.file_system
                .remove_with_receipt(
                    &params.path,
                    RemoveOptions {
                        recursive,
                        force,
                        follow_symlinks: params.follow_symlinks.unwrap_or(true),
                    },
                    params.sandbox.as_ref(),
                )
                .await,
        )?;
        Ok(FsRemoveResponse {
            sandbox_cleanup,
            no_sandbox_launch,
        })
    }

    pub(crate) async fn copy(
        &self,
        params: FsCopyParams,
    ) -> Result<FsCopyResponse, JSONRPCErrorError> {
        let ((), sandbox_cleanup, no_sandbox_launch) = fs_outcome(
            self.file_system
                .copy_with_receipt(
                    &params.source_path,
                    &params.destination_path,
                    CopyOptions {
                        recursive: params.recursive,
                    },
                    params.sandbox.as_ref(),
                )
                .await,
        )?;
        Ok(FsCopyResponse {
            sandbox_cleanup,
            no_sandbox_launch,
        })
    }
}

/// Splits a filesystem outcome for the wire: the cleanup receipt and the
/// positive "no launch" evidence go in the response, or in the error's `data`
/// when the operation failed. The failure and the cleanup state stay distinct.
fn fs_outcome<T>(
    outcome: FileSystemOutcome<T>,
) -> Result<(T, Option<SandboxCleanupSummary>, bool), JSONRPCErrorError> {
    let FileSystemOutcome {
        result,
        sandbox_cleanup,
        launch,
    } = outcome;
    match result {
        Ok(value) => Ok((value, sandbox_cleanup, launch == SandboxLaunch::NotLaunched)),
        Err(error) => Err(attach_receipt_to_error(
            map_fs_error(error),
            sandbox_cleanup,
            launch,
        )),
    }
}

/// Puts a failed request's receipt and launch evidence in the error's data.
fn attach_receipt_to_error(
    error: JSONRPCErrorError,
    sandbox_cleanup: Option<SandboxCleanupSummary>,
    launch: SandboxLaunch,
) -> JSONRPCErrorError {
    let error = attach_sandbox_cleanup_to_error(error, sandbox_cleanup);
    if launch == SandboxLaunch::NotLaunched {
        mark_no_sandbox_launch_in_error(error)
    } else {
        error
    }
}

fn validate_file_handle_id(handle_id: &str) -> Result<(), JSONRPCErrorError> {
    if handle_id.len() > MAX_FILE_HANDLE_ID_BYTES {
        return Err(invalid_request(format!(
            "file handle ID must not exceed {MAX_FILE_HANDLE_ID_BYTES} bytes"
        )));
    }
    Ok(())
}

fn map_fs_error(err: io::Error) -> JSONRPCErrorError {
    match err.kind() {
        io::ErrorKind::NotFound => not_found(err.to_string()),
        io::ErrorKind::InvalidInput | io::ErrorKind::PermissionDenied => {
            invalid_request(err.to_string())
        }
        _ => internal_error(err.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use codex_protocol::models::PermissionProfile;
    use codex_protocol::permissions::NetworkSandboxPolicy;
    use codex_utils_path_uri::PathUri;
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::FileSystemSandboxContext;
    use crate::protocol::FsReadFileParams;
    use crate::protocol::FsWriteFileParams;

    /// A failed sandboxed operation still reports its launches' cleanup, in the
    /// error's data; the failure itself is unchanged.
    #[test]
    fn failed_operation_reports_cleanup_receipt_in_error_data() {
        let receipt = SandboxCleanupSummary {
            state: codex_exec_server_protocol::SandboxCleanupState::Complete,
            reaped: 1,
            preserved_unproven: 0,
            host_owned: 0,
            handed_off: 0,
            timed_out: 0,
            joined: true,
        };

        let error = fs_outcome::<()>(FileSystemOutcome {
            result: Err(io::Error::new(io::ErrorKind::NotFound, "gone")),
            sandbox_cleanup: Some(receipt),
            launch: SandboxLaunch::Launched,
        })
        .expect_err("the operation failed");

        assert_eq!(
            (
                error.code,
                error.message.clone(),
                codex_exec_server_protocol::sandbox_cleanup_from_error(&error)
            ),
            (
                not_found("gone".to_string()).code,
                "gone".to_string(),
                Some(receipt)
            )
        );
    }

    #[tokio::test]
    async fn no_platform_sandbox_policies_do_not_require_configured_sandbox_helper() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let runtime_paths = ExecServerRuntimeOptions::new(
            std::env::current_exe().expect("current exe"),
            /*codex_linux_sandbox_exe*/ None,
        )
        .expect("runtime paths");
        let handler = FileSystemHandler::new(runtime_paths);
        let sandbox_cwd = PathUri::from_host_native_path(temp_dir.path()).expect("tempdir URI");
        let sandbox_context = |permissions| {
            FileSystemSandboxContext::from_permission_profile(permissions, sandbox_cwd.clone())
        };

        for (file_name, permission_profile) in [
            ("danger.txt", PermissionProfile::Disabled),
            (
                "external.txt",
                PermissionProfile::External {
                    network: NetworkSandboxPolicy::Restricted,
                },
            ),
        ] {
            let path =
                PathUri::from_host_native_path(temp_dir.path().join(file_name)).expect("path URI");

            handler
                .write_file(FsWriteFileParams {
                    path: path.clone(),
                    follow_symlinks: None,
                    data_base64: STANDARD.encode("ok"),
                    sandbox: Some(sandbox_context(permission_profile.clone())),
                })
                .await
                .expect("write file");

            let canonicalized = handler
                .canonicalize(FsCanonicalizeParams {
                    path: path.clone(),
                    sandbox: Some(sandbox_context(permission_profile.clone())),
                })
                .await
                .expect("canonicalize file");
            assert_eq!(
                canonicalized.path,
                PathUri::from_host_native_path(
                    std::fs::canonicalize(temp_dir.path().join(file_name)).expect("canonical path"),
                )
                .expect("canonical path URI"),
            );

            let response = handler
                .read_file(FsReadFileParams {
                    path,
                    follow_symlinks: None,
                    sandbox: Some(sandbox_context(permission_profile)),
                })
                .await
                .expect("read file");

            // No helper (and so no sandbox launcher) ran: no receipt is owed,
            // and the response says so positively.
            assert_eq!(
                (
                    response.data_base64,
                    response.sandbox_cleanup,
                    response.no_sandbox_launch
                ),
                (STANDARD.encode("ok"), None, true)
            );
        }
    }
}
