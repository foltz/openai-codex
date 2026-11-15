use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use codex_file_system::ExecutorFileSystemOutcomeFuture;
use codex_file_system::ExecutorFileSystemReadStreamFuture;
use codex_file_system::FILE_WRITE_CHUNK_SIZE;
use codex_file_system::FileSystemOutcome;
use codex_file_system::ReadStreamOutcome;
use codex_file_system::SandboxLaunch;
use codex_protocol::models::ManagedFileSystemPermissions;
use codex_protocol::models::PermissionProfile;
use codex_protocol::permissions::FileSystemPath;
use codex_utils_path_uri::PathUri;
use futures::StreamExt;
use futures::stream::BoxStream;
use tokio::io;
use tokio::sync::OnceCell;
use tracing::Instrument;
use tracing::instrument::WithSubscriber;
use tracing::trace;
use uuid::Uuid;

use crate::CopyOptions;
use crate::CreateDirectoryOptions;
use crate::ExecServerClient;
use crate::ExecServerError;
use crate::ExecutorFileSystem;
use crate::ExecutorFileSystemFuture;
use crate::FileMetadata;
use crate::FileSystemReadStream;
use crate::FileSystemResult;
use crate::FileSystemSandboxContext;
use crate::GetMetadataOptions;
use crate::ReadDirectoryEntry;
use crate::ReadFileOptions;
use crate::RemoveOptions;
use crate::WalkOptions;
use crate::WalkOutcome;
use crate::WriteFileOptions;
use crate::client::LazyRemoteExecServerClient;
use crate::protocol::FS_CANONICALIZE_METHOD;
use crate::protocol::FS_COPY_METHOD;
use crate::protocol::FS_CREATE_DIRECTORY_METHOD;
use crate::protocol::FS_GET_METADATA_METHOD;
use crate::protocol::FS_READ_DIRECTORY_METHOD;
use crate::protocol::FS_READ_FILE_METHOD;
use crate::protocol::FS_REMOVE_METHOD;
use crate::protocol::FS_WALK_METHOD;
use crate::protocol::FS_WRITE_FILE_METHOD;
use crate::protocol::FsCanonicalizeParams;
use crate::protocol::FsCanonicalizeResponse;
use crate::protocol::FsCloseParams;
use crate::protocol::FsCopyParams;
use crate::protocol::FsCopyResponse;
use crate::protocol::FsCreateDirectoryParams;
use crate::protocol::FsCreateDirectoryResponse;
use crate::protocol::FsGetMetadataParams;
use crate::protocol::FsGetMetadataResponse;
use crate::protocol::FsOpenMode;
use crate::protocol::FsOpenParams;
use crate::protocol::FsReadDirectoryParams;
use crate::protocol::FsReadDirectoryResponse;
use crate::protocol::FsReadFileParams;
use crate::protocol::FsReadFileResponse;
use crate::protocol::FsRemoveParams;
use crate::protocol::FsRemoveResponse;
use crate::protocol::FsWalkParams;
use crate::protocol::FsWalkResponse;
use crate::protocol::FsWriteBlockParams;
use crate::protocol::FsWriteFileParams;
use crate::protocol::FsWriteFileResponse;
use crate::protocol::SandboxCleanupSummary;
use crate::protocol::WireFsCanonicalizeParams;
use crate::protocol::WireFsCopyParams;
use crate::protocol::WireFsCreateDirectoryParams;
use crate::protocol::WireFsGetMetadataParams;
use crate::protocol::WireFsReadDirectoryParams;
use crate::protocol::WireFsReadFileParams;
use crate::protocol::WireFsRemoveParams;
use crate::protocol::WireFsWalkParams;
use crate::protocol::WireFsWriteFileParams;

const INVALID_REQUEST_ERROR_CODE: i64 = -32600;
const NOT_FOUND_ERROR_CODE: i64 = -32004;

#[path = "remote_file_stream.rs"]
mod file_stream;

type InFlightMetadataRequest = OnceCell<Result<FileMetadata, Arc<io::Error>>>;

pub(crate) struct RemoteFileSystem {
    client: LazyRemoteExecServerClient,
    // Only the in-flight request map is locked; metadata I/O happens outside this lock.
    metadata_requests: Mutex<HashMap<PathUri, Arc<InFlightMetadataRequest>>>,
}

impl RemoteFileSystem {
    pub(crate) fn new(client: LazyRemoteExecServerClient) -> Self {
        trace!("remote fs new");
        Self {
            client,
            metadata_requests: Mutex::new(HashMap::new()),
        }
    }

    fn lock_metadata_requests(
        &self,
    ) -> MutexGuard<'_, HashMap<PathUri, Arc<InFlightMetadataRequest>>> {
        self.metadata_requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Sends one filesystem request and keeps the executor's cleanup receipt
    /// and launch evidence: from the response's own fields on success, or from
    /// the error's data on failure. The receipt never changes the result.
    async fn call<P, T>(
        &self,
        method: &str,
        params: &P,
        sandbox_requested: bool,
        response_receipt: impl FnOnce(&T) -> (Option<SandboxCleanupSummary>, bool),
    ) -> FileSystemOutcome<T>
    where
        P: serde::Serialize,
        T: serde::de::DeserializeOwned,
    {
        let client = match self.client.get().await {
            Ok(client) => client,
            // No request reached the executor, so nothing was launched.
            Err(error) => return FileSystemOutcome::not_launched(Err(map_remote_error(error))),
        };
        Self::call_with_client(&client, method, params, sandbox_requested, response_receipt).await
    }

    async fn call_with_client<P, T>(
        client: &ExecServerClient,
        method: &str,
        params: &P,
        sandbox_requested: bool,
        response_receipt: impl FnOnce(&T) -> (Option<SandboxCleanupSummary>, bool),
    ) -> FileSystemOutcome<T>
    where
        P: serde::Serialize,
        T: serde::de::DeserializeOwned,
    {
        let (result, error_receipt, error_no_launch) =
            client.call_fs_with_error_receipt(method, params).await;
        let (result, sandbox_cleanup, no_sandbox_launch) = match result {
            Ok(response) => {
                let (sandbox_cleanup, no_sandbox_launch) = response_receipt(&response);
                (Ok(response), sandbox_cleanup, no_sandbox_launch)
            }
            Err(error) => (Err(map_remote_error(error)), error_receipt, error_no_launch),
        };
        FileSystemOutcome {
            result,
            sandbox_cleanup,
            launch: launch_evidence(sandbox_requested, sandbox_cleanup, no_sandbox_launch),
        }
    }

    /// Keep 0.169 read normalization and MXC validation on the client used for
    /// the filesystem request. Failure here sends no filesystem operation.
    async fn prepare_read<'a>(
        &self,
        path: &PathUri,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> FileSystemResult<(ExecServerClient, Option<&'a FileSystemSandboxContext>)> {
        let client = self.client.get().await.map_err(map_remote_error)?;
        let sandbox = read_sandbox(&client, path, sandbox).await?;
        Ok((client, sandbox))
    }

    async fn write_file_stream_outcome<'a>(
        &'a self,
        path: &'a PathUri,
        contents: BoxStream<'a, FileSystemResult<Bytes>>,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> FileSystemOutcome<u64> {
        let mut sandbox_cleanup = None;
        let mut launch = SandboxLaunch::NotLaunched;
        let result = async {
            let client = self.client.get().await.map_err(map_remote_error)?;
            let _invalidation = MetadataInvalidation(self);
            let mut contents = contents.map(|chunk| {
                let chunk = chunk?;
                if !(1..=FILE_WRITE_CHUNK_SIZE).contains(&chunk.len()) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid file stream block length",
                    ));
                }
                Ok(chunk)
            });
            // Choose the legacy write before opening a stream, so an open or write failure
            // can never cause us to replay a replacement that may have already modified the file.
            if !client
                .supports_file_write_streaming()
                .await
                .map_err(map_remote_error)?
            {
                let mut bytes = Vec::new();
                while let Some(chunk) = contents.next().await {
                    bytes.extend_from_slice(&chunk?);
                }
                let len = bytes.len() as u64;
                let outcome = self
                    .write_file_outcome(path, bytes, WriteFileOptions::default(), sandbox)
                    .await;
                sandbox_cleanup = outcome.sandbox_cleanup;
                launch = outcome.launch;
                outcome.result?;
                return Ok(len);
            }

            // Dropping the JoinHandle lets the open finish. If it succeeds after cancellation,
            // Tokio drops the unclaimed registration, which closes that exact handle.
            let params = crate::protocol::WireFsOpenParams::from(FsOpenParams {
                handle_id: Uuid::new_v4().simple().to_string(),
                path: path.clone(),
                mode: FsOpenMode::Replace,
                sandbox: sandbox.cloned(),
            });
            let sandbox_requested = sandbox.is_some();
            let open = async move {
                let (opened, error_receipt, error_no_launch) = client
                    .call_fs_with_error_receipt::<_, crate::protocol::FsOpenResponse>(
                        crate::protocol::FS_OPEN_METHOD,
                        &params,
                    )
                    .await;
                match opened {
                    Ok(opened) => FileSystemOutcome {
                        launch: launch_evidence(
                            sandbox_requested,
                            opened.sandbox_cleanup,
                            opened.no_sandbox_launch,
                        ),
                        sandbox_cleanup: opened.sandbox_cleanup,
                        result: Ok(FileWriteRegistration {
                            client,
                            handle_id: opened.handle_id,
                            active: true,
                            runtime: tokio::runtime::Handle::current(),
                        }),
                    },
                    Err(error) => FileSystemOutcome {
                        launch: launch_evidence(sandbox_requested, error_receipt, error_no_launch),
                        sandbox_cleanup: error_receipt,
                        result: Err(map_remote_error(error)),
                    },
                }
            };
            // The same detached-open ownership as upstream: an unclaimed
            // registration still drops and closes its exact remote handle.
            launch = SandboxLaunch::Unknown;
            let open = tokio::spawn(open.in_current_span().with_current_subscriber());
            let opened = open.await.map_err(io::Error::other)?;
            sandbox_cleanup = opened.sandbox_cleanup;
            launch = opened.launch;
            let mut registration = opened.result?;

            let result: io::Result<u64> = async {
                let mut written = 0_u64;
                while let Some(chunk) = contents.next().await {
                    let chunk = chunk?;
                    let next_offset = written.checked_add(chunk.len() as u64).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "file stream offset overflow")
                    })?;
                    registration
                        .client
                        .fs_write_block(FsWriteBlockParams {
                            handle_id: registration.handle_id.clone(),
                            offset: written,
                            chunk: chunk.to_vec().into(),
                        })
                        .await
                        .map_err(map_remote_error)?;
                    written = next_offset;
                }
                Ok(written)
            }
            .await;
            let close_result = registration
                .client
                .fs_close(FsCloseParams {
                    handle_id: registration.handle_id.clone(),
                })
                .await
                .map_err(map_remote_error);
            registration.active = close_result.is_err();
            let written = result?;
            close_result?;
            Ok(written)
        }
        .await;
        FileSystemOutcome {
            result,
            sandbox_cleanup,
            launch,
        }
    }

    async fn canonicalize_outcome(
        &self,
        path: &PathUri,
        sandbox: Option<&FileSystemSandboxContext>,
    ) -> FileSystemOutcome<PathUri> {
        trace!("remote fs canonicalize");
        let (client, sandbox) = match self.prepare_read(path, sandbox).await {
            Ok(prepared) => prepared,
            Err(error) => return FileSystemOutcome::not_launched(Err(error)),
        };
        let outcome = Self::call_with_client(
            &client,
            FS_CANONICALIZE_METHOD,
            &WireFsCanonicalizeParams::from(FsCanonicalizeParams {
                path: path.clone(),
                sandbox: sandbox.cloned(),
            }),
            sandbox.is_some(),
            |response: &FsCanonicalizeResponse| {
                (response.sandbox_cleanup, response.no_sandbox_launch)
            },
        )
        .await;
        map_outcome(outcome, |response| Ok(response.path))
    }

    async fn read_file_outcome(
        &self,
        path: &PathUri,
        options: ReadFileOptions,
        sandbox: Option<&FileSystemSandboxContext>,
    ) -> FileSystemOutcome<Vec<u8>> {
        trace!("remote fs read_file");
        let (client, sandbox) = match self.prepare_read(path, sandbox).await {
            Ok(prepared) => prepared,
            Err(error) => return FileSystemOutcome::not_launched(Err(error)),
        };
        let outcome = Self::call_with_client(
            &client,
            FS_READ_FILE_METHOD,
            &WireFsReadFileParams::from(FsReadFileParams {
                path: path.clone(),
                follow_symlinks: (!options.follow_symlinks).then_some(false),
                sandbox: sandbox.cloned(),
            }),
            sandbox.is_some(),
            |response: &FsReadFileResponse| (response.sandbox_cleanup, response.no_sandbox_launch),
        )
        .await;
        map_outcome(outcome, |response| {
            STANDARD.decode(response.data_base64).map_err(|err| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("remote fs/readFile returned invalid base64 dataBase64: {err}"),
                )
            })
        })
    }

    async fn read_file_stream_outcome(
        &self,
        path: &PathUri,
        sandbox: Option<&FileSystemSandboxContext>,
    ) -> FileSystemOutcome<FileSystemReadStream> {
        trace!("remote fs read_file_stream");
        let (client, sandbox) = match self.prepare_read(path, sandbox).await {
            Ok(prepared) => prepared,
            Err(error) => return FileSystemOutcome::not_launched(Err(error)),
        };
        file_stream::open(client, path.clone(), sandbox).await
    }

    async fn write_file_outcome(
        &self,
        path: &PathUri,
        contents: Vec<u8>,
        options: WriteFileOptions,
        sandbox: Option<&FileSystemSandboxContext>,
    ) -> FileSystemOutcome<()> {
        trace!("remote fs write_file");
        let outcome = self
            .call(
                FS_WRITE_FILE_METHOD,
                &WireFsWriteFileParams::from(FsWriteFileParams {
                    path: path.clone(),
                    data_base64: STANDARD.encode(contents),
                    follow_symlinks: (!options.follow_symlinks).then_some(false),
                    sandbox: sandbox.cloned(),
                }),
                sandbox.is_some(),
                |response: &FsWriteFileResponse| {
                    (response.sandbox_cleanup, response.no_sandbox_launch)
                },
            )
            .await;
        self.lock_metadata_requests().clear();
        map_outcome(outcome, |_| Ok(()))
    }

    async fn create_directory_outcome(
        &self,
        path: &PathUri,
        options: CreateDirectoryOptions,
        sandbox: Option<&FileSystemSandboxContext>,
    ) -> FileSystemOutcome<()> {
        trace!("remote fs create_directory");
        let outcome = self
            .call(
                FS_CREATE_DIRECTORY_METHOD,
                &WireFsCreateDirectoryParams::from(FsCreateDirectoryParams {
                    path: path.clone(),
                    recursive: Some(options.recursive),
                    follow_symlinks: (!options.follow_symlinks).then_some(false),
                    sandbox: sandbox.cloned(),
                }),
                sandbox.is_some(),
                |response: &FsCreateDirectoryResponse| {
                    (response.sandbox_cleanup, response.no_sandbox_launch)
                },
            )
            .await;
        self.lock_metadata_requests().clear();
        map_outcome(outcome, |_| Ok(()))
    }

    /// Shares identical unsandboxed metadata requests only while their RPC remains in flight.
    /// Shared requests run no sandboxed helper, so they carry no receipt.
    async fn get_metadata_outcome(
        &self,
        path: &PathUri,
        options: GetMetadataOptions,
        sandbox: Option<&FileSystemSandboxContext>,
    ) -> FileSystemOutcome<FileMetadata> {
        if sandbox.is_some() || !options.follow_symlinks {
            return self.get_metadata_uncached(path, options, sandbox).await;
        }

        let request = {
            let mut requests = self.lock_metadata_requests();
            requests.retain(|_, in_flight| Arc::strong_count(in_flight) > 1);
            Arc::clone(requests.entry(path.clone()).or_default())
        };
        let result = match request
            .get_or_init(|| async {
                self.get_metadata_uncached(path, options, /*sandbox*/ None)
                    .await
                    .result
                    .map_err(Arc::new)
            })
            .await
        {
            Ok(metadata) => Ok(metadata.clone()),
            Err(error) => Err(io::Error::new(error.kind(), error.to_string())),
        };

        let mut requests = self.lock_metadata_requests();
        if requests
            .get(path)
            .is_some_and(|in_flight| Arc::ptr_eq(in_flight, &request))
        {
            requests.remove(path);
        }

        // Shared requests carry no sandbox context, so they launch nothing.
        FileSystemOutcome::not_launched(result)
    }

    /// Sends a fresh metadata request without sharing another caller's sandbox or result.
    async fn get_metadata_uncached(
        &self,
        path: &PathUri,
        options: GetMetadataOptions,
        sandbox: Option<&FileSystemSandboxContext>,
    ) -> FileSystemOutcome<FileMetadata> {
        trace!("remote fs get_metadata");
        let (client, sandbox) = match self.prepare_read(path, sandbox).await {
            Ok(prepared) => prepared,
            Err(error) => return FileSystemOutcome::not_launched(Err(error)),
        };
        let outcome = Self::call_with_client(
            &client,
            FS_GET_METADATA_METHOD,
            &WireFsGetMetadataParams::from(FsGetMetadataParams {
                path: path.clone(),
                follow_symlinks: (!options.follow_symlinks).then_some(false),
                sandbox: sandbox.cloned(),
            }),
            sandbox.is_some(),
            |response: &FsGetMetadataResponse| {
                (response.sandbox_cleanup, response.no_sandbox_launch)
            },
        )
        .await;
        map_outcome(outcome, |response| {
            Ok(FileMetadata {
                is_directory: response.is_directory,
                is_file: response.is_file,
                is_symlink: response.is_symlink,
                size: response.size,
                created_at_ms: response.created_at_ms,
                modified_at_ms: response.modified_at_ms,
            })
        })
    }

    async fn read_directory_outcome(
        &self,
        path: &PathUri,
        sandbox: Option<&FileSystemSandboxContext>,
    ) -> FileSystemOutcome<Vec<ReadDirectoryEntry>> {
        trace!("remote fs read_directory");
        let (client, sandbox) = match self.prepare_read(path, sandbox).await {
            Ok(prepared) => prepared,
            Err(error) => return FileSystemOutcome::not_launched(Err(error)),
        };
        let outcome = Self::call_with_client(
            &client,
            FS_READ_DIRECTORY_METHOD,
            &WireFsReadDirectoryParams::from(FsReadDirectoryParams {
                path: path.clone(),
                sandbox: sandbox.cloned(),
            }),
            sandbox.is_some(),
            |response: &FsReadDirectoryResponse| {
                (response.sandbox_cleanup, response.no_sandbox_launch)
            },
        )
        .await;
        map_outcome(outcome, |response| {
            Ok(response
                .entries
                .into_iter()
                .map(|entry| ReadDirectoryEntry {
                    file_name: entry.file_name,
                    is_directory: entry.is_directory,
                    is_file: entry.is_file,
                })
                .collect())
        })
    }

    async fn walk_outcome(
        &self,
        path: &PathUri,
        options: WalkOptions,
        sandbox: Option<&FileSystemSandboxContext>,
    ) -> FileSystemOutcome<WalkOutcome> {
        trace!("remote fs walk");
        let (client, sandbox) = match self.prepare_read(path, sandbox).await {
            Ok(prepared) => prepared,
            Err(error) => return FileSystemOutcome::not_launched(Err(error)),
        };
        let outcome = Self::call_with_client(
            &client,
            FS_WALK_METHOD,
            &WireFsWalkParams::from(FsWalkParams {
                path: path.clone(),
                options,
                sandbox: sandbox.cloned(),
            }),
            sandbox.is_some(),
            |response: &FsWalkResponse| (response.sandbox_cleanup, response.no_sandbox_launch),
        )
        .await;
        map_outcome(outcome, |response| Ok(response.outcome))
    }

    async fn remove_outcome(
        &self,
        path: &PathUri,
        options: RemoveOptions,
        sandbox: Option<&FileSystemSandboxContext>,
    ) -> FileSystemOutcome<()> {
        trace!("remote fs remove");
        let outcome = self
            .call(
                FS_REMOVE_METHOD,
                &WireFsRemoveParams::from(FsRemoveParams {
                    path: path.clone(),
                    recursive: Some(options.recursive),
                    force: Some(options.force),
                    follow_symlinks: (!options.follow_symlinks).then_some(false),
                    sandbox: sandbox.cloned(),
                }),
                sandbox.is_some(),
                |response: &FsRemoveResponse| {
                    (response.sandbox_cleanup, response.no_sandbox_launch)
                },
            )
            .await;
        self.lock_metadata_requests().clear();
        map_outcome(outcome, |_| Ok(()))
    }

    async fn copy_outcome(
        &self,
        source_path: &PathUri,
        destination_path: &PathUri,
        options: CopyOptions,
        sandbox: Option<&FileSystemSandboxContext>,
    ) -> FileSystemOutcome<()> {
        trace!("remote fs copy");
        let outcome = self
            .call(
                FS_COPY_METHOD,
                &WireFsCopyParams::from(FsCopyParams {
                    source_path: source_path.clone(),
                    destination_path: destination_path.clone(),
                    recursive: options.recursive,
                    sandbox: sandbox.cloned(),
                }),
                sandbox.is_some(),
                |response: &FsCopyResponse| (response.sandbox_cleanup, response.no_sandbox_launch),
            )
            .await;
        self.lock_metadata_requests().clear();
        map_outcome(outcome, |_| Ok(()))
    }
}

/// Launch evidence for one remote request. The executor runs a sandboxed
/// helper only for a request that carries a sandbox context (its
/// `LocalFileSystem::file_system_for_reads`/`_for_writes`/`open_file`), so a
/// request without one positively launched nothing, whatever the executor's
/// version. Otherwise a receipt proves a launch, the executor's
/// `noSandboxLaunch` proves none, and silence (older executor) is unknown.
pub(crate) fn launch_evidence(
    sandbox_requested: bool,
    sandbox_cleanup: Option<SandboxCleanupSummary>,
    no_sandbox_launch: bool,
) -> SandboxLaunch {
    if !sandbox_requested {
        SandboxLaunch::NotLaunched
    } else if sandbox_cleanup.is_some() {
        SandboxLaunch::Launched
    } else if no_sandbox_launch {
        SandboxLaunch::NotLaunched
    } else {
        SandboxLaunch::Unknown
    }
}

/// Converts a wire response into the operation's value, keeping the receipt.
fn map_outcome<R, T>(
    outcome: FileSystemOutcome<R>,
    convert: impl FnOnce(R) -> FileSystemResult<T>,
) -> FileSystemOutcome<T> {
    FileSystemOutcome {
        result: outcome.result.and_then(convert),
        sandbox_cleanup: outcome.sandbox_cleanup,
        launch: outcome.launch,
    }
}

impl ExecutorFileSystem for RemoteFileSystem {
    fn watch_missing(
        &self,
        missing: Vec<PathUri>,
        present: Option<PathUri>,
    ) -> ExecutorFileSystemFuture<'_, Option<tokio::sync::oneshot::Receiver<()>>> {
        Box::pin(async move {
            self.client
                .get()
                .await
                .map_err(map_remote_error)?
                .fs_watch_missing(missing, present)
                .await
                .map_err(map_remote_error)
        })
    }

    fn write_file_stream<'a>(
        &'a self,
        path: &'a PathUri,
        contents: BoxStream<'a, FileSystemResult<Bytes>>,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, u64> {
        Box::pin(async move {
            self.write_file_stream_outcome(path, contents, sandbox)
                .await
                .result
        })
    }

    fn write_file_stream_with_receipt<'a>(
        &'a self,
        path: &'a PathUri,
        contents: BoxStream<'a, FileSystemResult<Bytes>>,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemOutcomeFuture<'a, u64> {
        Box::pin(self.write_file_stream_outcome(path, contents, sandbox))
    }

    fn canonicalize<'a>(
        &'a self,
        path: &'a PathUri,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, PathUri> {
        Box::pin(async move {
            RemoteFileSystem::canonicalize_outcome(self, path, sandbox)
                .await
                .result
        })
    }

    fn read_file<'a>(
        &'a self,
        path: &'a PathUri,
        options: ReadFileOptions,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, Vec<u8>> {
        Box::pin(async move {
            RemoteFileSystem::read_file_outcome(self, path, options, sandbox)
                .await
                .result
        })
    }

    fn read_file_stream<'a>(
        &'a self,
        path: &'a PathUri,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, FileSystemReadStream> {
        Box::pin(async move {
            RemoteFileSystem::read_file_stream_outcome(self, path, sandbox)
                .await
                .result
        })
    }

    fn write_file<'a>(
        &'a self,
        path: &'a PathUri,
        contents: Vec<u8>,
        options: WriteFileOptions,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, ()> {
        Box::pin(async move {
            RemoteFileSystem::write_file_outcome(self, path, contents, options, sandbox)
                .await
                .result
        })
    }

    fn create_directory<'a>(
        &'a self,
        path: &'a PathUri,
        options: CreateDirectoryOptions,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, ()> {
        Box::pin(async move {
            RemoteFileSystem::create_directory_outcome(self, path, options, sandbox)
                .await
                .result
        })
    }

    fn get_metadata<'a>(
        &'a self,
        path: &'a PathUri,
        options: GetMetadataOptions,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, FileMetadata> {
        Box::pin(async move {
            RemoteFileSystem::get_metadata_outcome(self, path, options, sandbox)
                .await
                .result
        })
    }

    fn read_directory<'a>(
        &'a self,
        path: &'a PathUri,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, Vec<ReadDirectoryEntry>> {
        Box::pin(async move {
            RemoteFileSystem::read_directory_outcome(self, path, sandbox)
                .await
                .result
        })
    }

    fn walk<'a>(
        &'a self,
        path: &'a PathUri,
        options: WalkOptions,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, WalkOutcome> {
        Box::pin(async move {
            RemoteFileSystem::walk_outcome(self, path, options, sandbox)
                .await
                .result
        })
    }

    fn remove<'a>(
        &'a self,
        path: &'a PathUri,
        options: RemoveOptions,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, ()> {
        Box::pin(async move {
            RemoteFileSystem::remove_outcome(self, path, options, sandbox)
                .await
                .result
        })
    }

    fn copy<'a>(
        &'a self,
        source_path: &'a PathUri,
        destination_path: &'a PathUri,
        options: CopyOptions,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, ()> {
        Box::pin(async move {
            RemoteFileSystem::copy_outcome(self, source_path, destination_path, options, sandbox)
                .await
                .result
        })
    }

    // The executor reports each request's sandbox cleanup receipt on the wire.

    fn read_file_stream_with_receipt<'a>(
        &'a self,
        path: &'a PathUri,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemReadStreamFuture<'a> {
        Box::pin(async move {
            let FileSystemOutcome {
                result,
                sandbox_cleanup,
                launch,
            } = RemoteFileSystem::read_file_stream_outcome(self, path, sandbox).await;
            ReadStreamOutcome::from_open(result, sandbox_cleanup, launch)
        })
    }

    fn canonicalize_with_receipt<'a>(
        &'a self,
        path: &'a PathUri,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemOutcomeFuture<'a, PathUri> {
        Box::pin(RemoteFileSystem::canonicalize_outcome(self, path, sandbox))
    }

    fn read_file_with_receipt<'a>(
        &'a self,
        path: &'a PathUri,
        options: ReadFileOptions,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemOutcomeFuture<'a, Vec<u8>> {
        Box::pin(RemoteFileSystem::read_file_outcome(
            self, path, options, sandbox,
        ))
    }

    fn write_file_with_receipt<'a>(
        &'a self,
        path: &'a PathUri,
        contents: Vec<u8>,
        options: WriteFileOptions,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemOutcomeFuture<'a, ()> {
        Box::pin(RemoteFileSystem::write_file_outcome(
            self, path, contents, options, sandbox,
        ))
    }

    fn create_directory_with_receipt<'a>(
        &'a self,
        path: &'a PathUri,
        options: CreateDirectoryOptions,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemOutcomeFuture<'a, ()> {
        Box::pin(RemoteFileSystem::create_directory_outcome(
            self, path, options, sandbox,
        ))
    }

    fn get_metadata_with_receipt<'a>(
        &'a self,
        path: &'a PathUri,
        options: GetMetadataOptions,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemOutcomeFuture<'a, FileMetadata> {
        Box::pin(RemoteFileSystem::get_metadata_outcome(
            self, path, options, sandbox,
        ))
    }

    fn read_directory_with_receipt<'a>(
        &'a self,
        path: &'a PathUri,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemOutcomeFuture<'a, Vec<ReadDirectoryEntry>> {
        Box::pin(RemoteFileSystem::read_directory_outcome(
            self, path, sandbox,
        ))
    }

    fn walk_with_receipt<'a>(
        &'a self,
        path: &'a PathUri,
        options: WalkOptions,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemOutcomeFuture<'a, WalkOutcome> {
        Box::pin(RemoteFileSystem::walk_outcome(self, path, options, sandbox))
    }

    fn remove_with_receipt<'a>(
        &'a self,
        path: &'a PathUri,
        options: RemoveOptions,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemOutcomeFuture<'a, ()> {
        Box::pin(RemoteFileSystem::remove_outcome(
            self, path, options, sandbox,
        ))
    }

    fn copy_with_receipt<'a>(
        &'a self,
        source_path: &'a PathUri,
        destination_path: &'a PathUri,
        options: CopyOptions,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemOutcomeFuture<'a, ()> {
        Box::pin(RemoteFileSystem::copy_outcome(
            self,
            source_path,
            destination_path,
            options,
            sandbox,
        ))
    }
}

/// A cancelled write must stop later lookups from joining pre-cancellation metadata requests.
struct MetadataInvalidation<'a>(&'a RemoteFileSystem);

impl Drop for MetadataInvalidation<'_> {
    fn drop(&mut self) {
        self.0.lock_metadata_requests().clear();
    }
}

/// Best-effort remote handle cleanup may outlive a cancelled transfer.
struct FileWriteRegistration {
    client: ExecServerClient,
    handle_id: String,
    active: bool,
    runtime: tokio::runtime::Handle,
}

impl Drop for FileWriteRegistration {
    fn drop(&mut self) {
        if self.active {
            let client = self.client.clone();
            let handle_id = self.handle_id.clone();
            self.runtime.spawn(async move {
                let _ = client.fs_close(FsCloseParams { handle_id }).await;
            });
        }
    }
}

// Older executors sandbox any supplied policy. Omit unrestricted read policies only when
// their explicit paths are valid for the read target's convention. The executor rejects foreign
// targets itself; keep uncertain policies intact so it also validates their explicit paths.
async fn read_sandbox<'a>(
    client: &ExecServerClient,
    path: &PathUri,
    sandbox: Option<&'a FileSystemSandboxContext>,
) -> FileSystemResult<Option<&'a FileSystemSandboxContext>> {
    let convention = path.infer_path_convention();
    let sandbox = sandbox.filter(|sandbox| {
        if sandbox.should_read_from_sandbox() {
            return true;
        }
        let PermissionProfile::Managed {
            file_system: ManagedFileSystemPermissions::Restricted { entries, .. },
            ..
        } = &sandbox.permissions
        else {
            return false;
        };
        entries.iter().any(|entry| {
            let FileSystemPath::Path { path } = &entry.path else {
                return false;
            };
            let Some(convention) = convention else {
                return true;
            };
            path.validate_config_path(convention).is_err()
        })
    });
    if sandbox.is_some_and(|sandbox| {
        sandbox.windows_sandbox_selection == crate::WindowsSandboxSelection::Mxc
    }) {
        let info = client.environment_info().await.map_err(map_remote_error)?;
        crate::sandbox_selection::validate_remote_mxc(&info)?;
    }
    Ok(sandbox)
}

pub(crate) fn map_remote_error(error: ExecServerError) -> io::Error {
    match error {
        ExecServerError::Server { code, message } if code == NOT_FOUND_ERROR_CODE => {
            io::Error::new(io::ErrorKind::NotFound, message)
        }
        ExecServerError::Server { code, message } if code == INVALID_REQUEST_ERROR_CODE => {
            io::Error::new(io::ErrorKind::InvalidInput, message)
        }
        ExecServerError::Closed | ExecServerError::Disconnected(_) => {
            io::Error::new(io::ErrorKind::BrokenPipe, "exec-server transport closed")
        }
        error if error.is_retryable_preparation_error() => {
            io::Error::new(io::ErrorKind::BrokenPipe, error.to_string())
        }
        ExecServerError::Server { message, .. } => io::Error::other(message),
        _ => io::Error::other(error.to_string()),
    }
}

#[cfg(all(test, any(unix, windows)))]
#[path = "remote_file_system_path_uri_tests.rs"]
mod path_uri_tests;

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    /// A response that fails to convert still keeps the executor's receipt:
    /// failure and cleanup state stay distinct.
    #[test]
    fn conversion_failure_keeps_cleanup_receipt() {
        let receipt = SandboxCleanupSummary {
            state: crate::protocol::SandboxCleanupState::Complete,
            reaped: 1,
            preserved_unproven: 0,
            host_owned: 0,
            handed_off: 0,
            timed_out: 0,
            joined: true,
        };

        let outcome = map_outcome(
            FileSystemOutcome {
                result: Ok("not base64!"),
                sandbox_cleanup: Some(receipt),
                launch: SandboxLaunch::Launched,
            },
            |data: &str| {
                STANDARD
                    .decode(data)
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))
            },
        );

        assert_eq!(
            (
                outcome.result.map_err(|error| error.kind()),
                outcome.sandbox_cleanup,
                outcome.launch
            ),
            (
                Err(io::ErrorKind::InvalidData),
                Some(receipt),
                SandboxLaunch::Launched
            )
        );
    }

    /// Positive "not launched" evidence is told apart from a missing receipt.
    #[test]
    fn launch_evidence_distinguishes_not_launched_from_unknown() {
        let receipt = SandboxCleanupSummary {
            state: crate::protocol::SandboxCleanupState::Complete,
            reaped: 0,
            preserved_unproven: 0,
            host_owned: 0,
            handed_off: 0,
            timed_out: 0,
            joined: true,
        };

        assert_eq!(
            [
                launch_evidence(/*sandbox_requested*/ false, None, false),
                launch_evidence(/*sandbox_requested*/ true, Some(receipt), false),
                launch_evidence(/*sandbox_requested*/ true, None, true),
                launch_evidence(/*sandbox_requested*/ true, None, false),
            ],
            [
                SandboxLaunch::NotLaunched,
                SandboxLaunch::Launched,
                SandboxLaunch::NotLaunched,
                SandboxLaunch::Unknown,
            ]
        );
    }

    #[test]
    fn transport_errors_map_to_broken_pipe() {
        let errors = [
            ExecServerError::Closed,
            ExecServerError::Disconnected("exec-server transport disconnected".to_string()),
        ];

        let mapped_errors = errors
            .into_iter()
            .map(|error| {
                let error = map_remote_error(error);
                (error.kind(), error.to_string())
            })
            .collect::<Vec<_>>();

        assert_eq!(
            mapped_errors,
            vec![
                (
                    io::ErrorKind::BrokenPipe,
                    "exec-server transport closed".to_string()
                ),
                (
                    io::ErrorKind::BrokenPipe,
                    "exec-server transport closed".to_string()
                ),
            ]
        );
    }

    #[test]
    fn preparation_retryability_controls_filesystem_error_kind() {
        let errors = [
            ExecServerError::ConnectionAttempt(Arc::new(
                ExecServerError::EnvironmentRegistryHttp {
                    status: http::StatusCode::SERVICE_UNAVAILABLE,
                    code: None,
                    message: "registry unavailable".to_string(),
                },
            )),
            ExecServerError::Protocol("exec-server transport closed".to_string()),
        ];

        let kinds = errors
            .into_iter()
            .map(|error| map_remote_error(error).kind())
            .collect::<Vec<_>>();

        assert_eq!(kinds, vec![io::ErrorKind::BrokenPipe, io::ErrorKind::Other]);
    }
}
