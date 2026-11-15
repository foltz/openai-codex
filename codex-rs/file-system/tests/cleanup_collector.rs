use bytes::Bytes;
use codex_file_system::*;
use codex_protocol::exec_output::SandboxCleanupState;
use codex_utils_path_uri::PathUri;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use std::collections::VecDeque;
use std::sync::Mutex;

#[derive(Clone, Copy)]
enum Reply {
    Complete,
    Missing,
    NotLaunched,
    Pending,
    ErrorComplete,
}
struct Backend {
    replies: Mutex<VecDeque<Reply>>,
    observed_sandbox: Mutex<Vec<Option<FileSystemSandboxContext>>>,
}
impl Backend {
    fn new(replies: impl IntoIterator<Item = Reply>) -> Self {
        Self {
            replies: Mutex::new(replies.into_iter().collect()),
            observed_sandbox: Mutex::new(Vec::new()),
        }
    }
}
fn complete() -> SandboxCleanupSummary {
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
impl ExecutorFileSystem for Backend {
    fn read_file<'a>(
        &'a self,
        path: &'a PathUri,
        options: ReadFileOptions,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, Vec<u8>> {
        Box::pin(async move {
            self.read_file_with_receipt(path, options, sandbox)
                .await
                .result
        })
    }
    fn read_file_with_receipt<'a>(
        &'a self,
        _path: &'a PathUri,
        _options: ReadFileOptions,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemOutcomeFuture<'a, Vec<u8>> {
        Box::pin(async move {
            self.observed_sandbox.lock().unwrap().push(sandbox.cloned());
            let reply = self.replies.lock().unwrap().pop_front().unwrap();
            match reply {
                Reply::Complete => FileSystemOutcome::launched(Ok(vec![1]), Some(complete())),
                Reply::Missing => FileSystemOutcome::launched(Ok(vec![1]), None),
                Reply::NotLaunched => FileSystemOutcome::not_launched(Ok(vec![1])),
                Reply::ErrorComplete => FileSystemOutcome::launched(
                    Err(std::io::Error::other("operation failed")),
                    Some(complete()),
                ),
                Reply::Pending => futures::future::pending().await,
            }
        })
    }
    fn read_file_stream<'a>(
        &'a self,
        _path: &'a PathUri,
        _sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, FileSystemReadStream> {
        Box::pin(async { Ok(FileSystemReadStream::new(futures::stream::empty())) })
    }
    fn watch_missing(
        &self,
        _missing: Vec<PathUri>,
        _present: Option<PathUri>,
    ) -> ExecutorFileSystemFuture<'_, Option<tokio::sync::oneshot::Receiver<()>>> {
        Box::pin(async {
            let (sender, receiver) = tokio::sync::oneshot::channel();
            sender.send(()).unwrap();
            Ok(Some(receiver))
        })
    }
    fn write_file_stream<'a>(
        &'a self,
        _path: &'a PathUri,
        mut contents: BoxStream<'a, FileSystemResult<Bytes>>,
        _sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, u64> {
        Box::pin(async move {
            let mut size = 0;
            while let Some(chunk) = contents.next().await {
                size += chunk?.len() as u64;
            }
            Ok(size)
        })
    }
    fn canonicalize<'a>(
        &'a self,
        path: &'a PathUri,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, PathUri> {
        let _ = (path, sandbox);
        Box::pin(async { Err(std::io::Error::other("unused fixture method")) })
    }
    fn write_file<'a>(
        &'a self,
        path: &'a PathUri,
        contents: Vec<u8>,
        options: WriteFileOptions,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, ()> {
        let _ = (path, contents, options, sandbox);
        Box::pin(async { Err(std::io::Error::other("unused fixture method")) })
    }
    fn create_directory<'a>(
        &'a self,
        path: &'a PathUri,
        create_directory_options: CreateDirectoryOptions,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, ()> {
        let _ = (path, create_directory_options, sandbox);
        Box::pin(async { Err(std::io::Error::other("unused fixture method")) })
    }
    fn get_metadata<'a>(
        &'a self,
        path: &'a PathUri,
        options: GetMetadataOptions,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, FileMetadata> {
        let _ = (path, options, sandbox);
        Box::pin(async { Err(std::io::Error::other("unused fixture method")) })
    }
    fn read_directory<'a>(
        &'a self,
        path: &'a PathUri,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, Vec<ReadDirectoryEntry>> {
        let _ = (path, sandbox);
        Box::pin(async { Err(std::io::Error::other("unused fixture method")) })
    }
    fn walk<'a>(
        &'a self,
        path: &'a PathUri,
        options: WalkOptions,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, WalkOutcome> {
        let _ = (path, options, sandbox);
        Box::pin(async { Err(std::io::Error::other("unused fixture method")) })
    }
    fn remove<'a>(
        &'a self,
        path: &'a PathUri,
        remove_options: RemoveOptions,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, ()> {
        let _ = (path, remove_options, sandbox);
        Box::pin(async { Err(std::io::Error::other("unused fixture method")) })
    }
    fn copy<'a>(
        &'a self,
        source_path: &'a PathUri,
        destination_path: &'a PathUri,
        copy_options: CopyOptions,
        sandbox: Option<&'a FileSystemSandboxContext>,
    ) -> ExecutorFileSystemFuture<'a, ()> {
        let _ = (source_path, destination_path, copy_options, sandbox);
        Box::pin(async { Err(std::io::Error::other("unused fixture method")) })
    }
}
fn path() -> PathUri {
    PathUri::parse("file:///fixture").unwrap()
}

#[test]
fn missing_receipts_cannot_be_hidden_in_either_order() {
    for replies in [
        [Reply::Missing, Reply::Complete],
        [Reply::Complete, Reply::Missing],
    ] {
        let backend = Backend::new(replies);
        let collector = SandboxCleanupReceiptCollector::new(&backend);
        let path = path();
        for _ in 0..2 {
            futures::executor::block_on(collector.read_file(
                &path,
                ReadFileOptions::default(),
                None,
            ))
            .unwrap();
        }
        assert_eq!(collector.receipt(), None);
        assert_eq!(collector.launch(), SandboxLaunch::Launched);
    }
}
#[test]
fn dormant_future_is_not_a_launch_but_started_cancellation_is_missing() {
    let backend = Backend::new([Reply::Pending, Reply::Complete]);
    let collector = SandboxCleanupReceiptCollector::new(&backend);
    let path = path();
    drop(collector.read_file(&path, ReadFileOptions::default(), None));
    assert_eq!(collector.launch(), SandboxLaunch::NotLaunched);
    let mut pending = collector.read_file(&path, ReadFileOptions::default(), None);
    assert!(pending.as_mut().now_or_never().is_none());
    assert_eq!(collector.launch(), SandboxLaunch::Unknown);
    drop(pending);
    futures::executor::block_on(collector.read_file(&path, ReadFileOptions::default(), None))
        .unwrap();
    assert_eq!(collector.receipt(), None);
}
#[test]
fn a_non_launch_does_not_poison_a_proven_launch() {
    let backend = Backend::new([Reply::NotLaunched, Reply::Complete]);
    let collector = SandboxCleanupReceiptCollector::new(&backend);
    let path = path();
    for _ in 0..2 {
        futures::executor::block_on(collector.read_file(&path, ReadFileOptions::default(), None))
            .unwrap();
    }
    assert_eq!(collector.receipt(), Some(complete()));
}
#[test]
fn failed_operations_keep_independent_cleanup_evidence() {
    let backend = Backend::new([Reply::ErrorComplete]);
    let collector = SandboxCleanupReceiptCollector::new(&backend);
    assert!(
        futures::executor::block_on(collector.read_file(&path(), ReadFileOptions::default(), None))
            .is_err()
    );
    assert_eq!(collector.receipt(), Some(complete()));
}
#[test]
fn plain_stream_cannot_be_hidden_by_another_complete_operation() {
    let backend = Backend::new([Reply::Complete]);
    let collector = SandboxCleanupReceiptCollector::new(&backend);
    let path = path();
    futures::executor::block_on(collector.read_file(&path, ReadFileOptions::default(), None))
        .unwrap();
    drop(futures::executor::block_on(collector.read_file_stream(&path, None)).unwrap());
    assert_eq!(collector.receipt(), None);
}
#[test]
fn new_upstream_watch_and_streaming_write_are_forwarded() {
    let backend = Backend::new([]);
    let collector = SandboxCleanupReceiptCollector::new(&backend);
    futures::executor::block_on(async {
        collector
            .watch_missing(vec![path()], None)
            .await
            .unwrap()
            .unwrap()
            .await
            .unwrap();
        assert_eq!(collector.launch(), SandboxLaunch::NotLaunched);
        let contents = futures::stream::iter([Ok(Bytes::from_static(b"hello"))]).boxed();
        assert_eq!(
            collector
                .write_file_stream(&path(), contents, None)
                .await
                .unwrap(),
            5
        );
    });
    assert_eq!(collector.launch(), SandboxLaunch::Unknown);
    assert_eq!(collector.receipt(), None);
}

#[test]
fn environment_accessor_preserves_reported_cleanup_and_operation_error() {
    let backend: std::sync::Arc<dyn ExecutorFileSystem> =
        std::sync::Arc::new(Backend::new([Reply::ErrorComplete]));
    let accessor = FileSystemEnvironmentAccessor::unrestricted(&backend);
    let outcome = futures::executor::block_on(
        accessor.read_file_with_receipt(&path(), ReadFileOptions::default()),
    );
    assert!(outcome.result.is_err());
    assert_eq!(outcome.launch, SandboxLaunch::Launched);
    assert_eq!(outcome.sandbox_cleanup, Some(complete()));
}

#[test]
fn environment_accessor_streaming_write_keeps_default_unknown_and_byte_count() {
    let backend: std::sync::Arc<dyn ExecutorFileSystem> = std::sync::Arc::new(Backend::new([]));
    let accessor = FileSystemEnvironmentAccessor::unrestricted(&backend);
    let contents = futures::stream::iter([Ok(Bytes::from_static(b"hello"))]).boxed();
    let outcome =
        futures::executor::block_on(accessor.write_file_stream_with_receipt(&path(), contents));
    assert_eq!(outcome.result.unwrap(), 5);
    assert_eq!(outcome.launch, SandboxLaunch::Unknown);
    assert_eq!(outcome.sandbox_cleanup, None);
}

#[test]
fn cancelled_streaming_write_cannot_be_hidden_by_a_complete_operation() {
    let backend = Backend::new([Reply::Complete]);
    let collector = SandboxCleanupReceiptCollector::new(&backend);
    let path = path();
    let mut write = collector.write_file_stream(&path, futures::stream::pending().boxed(), None);
    assert_eq!(collector.launch(), SandboxLaunch::NotLaunched);
    assert!(write.as_mut().now_or_never().is_none());
    assert_eq!(collector.launch(), SandboxLaunch::Unknown);
    drop(write);
    futures::executor::block_on(collector.read_file(&path, ReadFileOptions::default(), None))
        .unwrap();
    assert_eq!(collector.receipt(), None);
    assert_eq!(collector.launch(), SandboxLaunch::Unknown);
}

#[test]
fn accessor_receipt_uses_the_captured_sandbox_override() {
    use codex_protocol::models::PermissionProfile;
    use codex_protocol::sandbox::SandboxOverride;
    let backend = std::sync::Arc::new(Backend::new([Reply::Complete]));
    let erased: std::sync::Arc<dyn ExecutorFileSystem> = backend.clone();
    let mut sandbox =
        FileSystemSandboxContext::from_permission_profile(PermissionProfile::Disabled, path());
    sandbox.sandbox_override = SandboxOverride::EscalatedSandboxWithRestrictions;
    let accessor = FileSystemEnvironmentAccessor::new(&erased, sandbox.clone());
    let outcome = futures::executor::block_on(
        accessor.read_file_with_receipt(&path(), ReadFileOptions::default()),
    );
    assert_eq!(outcome.launch, SandboxLaunch::Launched);
    assert_eq!(outcome.sandbox_cleanup, Some(complete()));
    assert_eq!(
        *backend.observed_sandbox.lock().unwrap(),
        vec![Some(sandbox)]
    );
}
