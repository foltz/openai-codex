//! Aggregates terminal receipts without hiding missing or cancelled operations.
use super::*;
use std::sync::Mutex;

pub struct SandboxCleanupReceiptCollector<'a> {
    inner: &'a dyn ExecutorFileSystem,
    state: Mutex<ReceiptState>,
}

struct ReceiptState {
    receipt: Option<SandboxCleanupSummary>,
    launch: SandboxLaunch,
    pending: usize,
    unreported: bool,
}

impl<'a> SandboxCleanupReceiptCollector<'a> {
    pub fn new(inner: &'a dyn ExecutorFileSystem) -> Self {
        Self {
            inner,
            state: Mutex::new(ReceiptState {
                receipt: None,
                launch: SandboxLaunch::NotLaunched,
                pending: 0,
                unreported: false,
            }),
        }
    }

    /// No partial receipt is exposed while any started operation lacks proof.
    pub fn receipt(&self) -> Option<SandboxCleanupSummary> {
        let state = self.state.lock().ok()?;
        if state.pending != 0 || state.unreported {
            None
        } else {
            state.receipt
        }
    }

    pub fn launch(&self) -> SandboxLaunch {
        match self.state.lock() {
            Ok(state) if state.pending == 0 => state.launch,
            _ => SandboxLaunch::Unknown,
        }
    }

    // Called inside async bodies: an unpolled operation did not launch work.
    fn begin(&self) -> PendingOperation<'_> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.pending = state
            .pending
            .checked_add(1)
            .expect("filesystem operation count overflow");
        PendingOperation {
            state: &self.state,
            completed: false,
        }
    }
}

struct PendingOperation<'a> {
    state: &'a Mutex<ReceiptState>,
    completed: bool,
}

impl PendingOperation<'_> {
    fn finish<T>(mut self, outcome: FileSystemOutcome<T>) -> FileSystemResult<T> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.pending -= 1;
        state.unreported |=
            outcome.launch != SandboxLaunch::NotLaunched && outcome.sandbox_cleanup.is_none();
        state.receipt = merge_optional_sandbox_cleanup(state.receipt, outcome.sandbox_cleanup);
        state.launch = state.launch.merge(outcome.launch);
        self.completed = true;
        outcome.result
    }
}

impl Drop for PendingOperation<'_> {
    fn drop(&mut self) {
        if !self.completed {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.pending -= 1;
            state.unreported = true;
            state.launch = state.launch.merge(SandboxLaunch::Unknown);
        }
    }
}

impl ExecutorFileSystem for SandboxCleanupReceiptCollector<'_> {
    fn watch_missing(
        &self,
        missing: Vec<PathUri>,
        present: Option<PathUri>,
    ) -> ExecutorFileSystemFuture<'_, Option<tokio::sync::oneshot::Receiver<()>>> {
        self.inner.watch_missing(missing, present)
    }

    fn write_file_stream<'a>(
        &'a self,
        path: &'a PathUri,
        contents: BoxStream<'a, FileSystemResult<Bytes>>,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, u64> {
        Box::pin(async move {
            let guard = self.begin();
            guard.finish(
                self.inner
                    .write_file_stream_with_receipt(path, contents, sandbox)
                    .await,
            )
        })
    }

    fn canonicalize<'a>(
        &'a self,
        path: &'a PathUri,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, PathUri> {
        Box::pin(async move {
            let guard = self.begin();
            guard.finish(self.inner.canonicalize_with_receipt(path, sandbox).await)
        })
    }

    fn read_file<'a>(
        &'a self,
        path: &'a PathUri,
        options: ReadFileOptions,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, Vec<u8>> {
        Box::pin(async move {
            let guard = self.begin();
            guard.finish(
                self.inner
                    .read_file_with_receipt(path, options, sandbox)
                    .await,
            )
        })
    }

    // Plain streams outlive this call, so their terminal evidence is unknown.
    fn read_file_stream<'a>(
        &'a self,
        path: &'a PathUri,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, FileSystemReadStream> {
        Box::pin(async move {
            let guard = self.begin();
            guard.finish(FileSystemOutcome::without_receipt(
                self.inner.read_file_stream(path, sandbox).await,
            ))
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
            let guard = self.begin();
            guard.finish(
                self.inner
                    .write_file_with_receipt(path, contents, options, sandbox)
                    .await,
            )
        })
    }

    fn create_directory<'a>(
        &'a self,
        path: &'a PathUri,
        create_directory_options: CreateDirectoryOptions,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, ()> {
        Box::pin(async move {
            let guard = self.begin();
            guard.finish(
                self.inner
                    .create_directory_with_receipt(path, create_directory_options, sandbox)
                    .await,
            )
        })
    }

    fn get_metadata<'a>(
        &'a self,
        path: &'a PathUri,
        options: GetMetadataOptions,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, FileMetadata> {
        Box::pin(async move {
            let guard = self.begin();
            guard.finish(
                self.inner
                    .get_metadata_with_receipt(path, options, sandbox)
                    .await,
            )
        })
    }

    fn read_directory<'a>(
        &'a self,
        path: &'a PathUri,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, Vec<ReadDirectoryEntry>> {
        Box::pin(async move {
            let guard = self.begin();
            guard.finish(self.inner.read_directory_with_receipt(path, sandbox).await)
        })
    }

    fn walk<'a>(
        &'a self,
        path: &'a PathUri,
        options: WalkOptions,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, WalkOutcome> {
        Box::pin(async move {
            let guard = self.begin();
            guard.finish(self.inner.walk_with_receipt(path, options, sandbox).await)
        })
    }

    fn remove<'a>(
        &'a self,
        path: &'a PathUri,
        remove_options: RemoveOptions,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, ()> {
        Box::pin(async move {
            let guard = self.begin();
            guard.finish(
                self.inner
                    .remove_with_receipt(path, remove_options, sandbox)
                    .await,
            )
        })
    }

    fn copy<'a>(
        &'a self,
        source_path: &'a PathUri,
        destination_path: &'a PathUri,
        copy_options: CopyOptions,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, ()> {
        Box::pin(async move {
            let guard = self.begin();
            guard.finish(
                self.inner
                    .copy_with_receipt(source_path, destination_path, copy_options, sandbox)
                    .await,
            )
        })
    }
}
