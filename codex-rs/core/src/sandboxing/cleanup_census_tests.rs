use super::*;
use pretty_assertions::assert_eq;

fn receipt(state: SandboxCleanupState, joined: bool) -> SandboxCleanupSummary {
    SandboxCleanupSummary {
        state,
        reaped: 2,
        preserved_unproven: 0,
        host_owned: 1,
        handed_off: 0,
        timed_out: 0,
        joined,
    }
}

#[test]
fn positively_no_launch_is_nothing_to_clean() {
    let census = SandboxCleanupCensus::default();
    census.record(/*may_have_launched*/ false, None);
    assert_eq!(
        census.totals(),
        SandboxCleanupCensusTotals {
            no_launch: 1,
            ..SandboxCleanupCensusTotals::default()
        }
    );
    assert!(
        !census.totals().has_unproven(),
        "a no-launch session stays clean"
    );
}

#[test]
fn may_have_launched_missing_receipt_stays_unproven() {
    let census = SandboxCleanupCensus::default();
    census.record(/*may_have_launched*/ false, None);
    census.record(/*may_have_launched*/ true, None);
    let totals = census.totals();
    assert_eq!((totals.no_launch, totals.missing_receipt), (1, 1));
    assert!(totals.has_unproven());
}

#[tokio::test]
async fn apply_patch_predicate_covers_sandboxed_writes() {
    let local = codex_exec_server::Environment::default_for_tests();
    let cwd = codex_utils_path_uri::PathUri::parse("file:///workspace").expect("valid path uri");
    // Full-disk reads but restricted writes: a read cannot launch a helper,
    // a write (apply_patch) can.
    let read_only = FileSystemSandboxContext::from_permission_profile(
        codex_protocol::models::PermissionProfile::read_only(),
        cwd,
    );
    assert!(!read_only.should_read_from_sandbox());
    assert!(read_only.should_write_into_sandbox());
    assert!(!may_launch_sandboxed_read_helper(Some(&read_only), &local));
    assert_eq!(
        may_launch_sandboxed_helper(Some(&read_only), &local),
        cfg!(target_os = "linux")
    );
    assert!(!may_launch_sandboxed_helper(None, &local));
}

#[tokio::test]
async fn reads_without_a_sandboxed_read_are_positively_no_launch() {
    // No sandbox context: the executor serves the read itself, on any host.
    let local = codex_exec_server::Environment::default_for_tests();
    assert!(!may_launch_sandboxed_read_helper(None, &local));
}

#[test]
fn missing_receipt_is_unproven_never_clean() {
    let census = SandboxCleanupCensus::default();
    census.record(/*may_have_launched*/ true, None);
    let totals = census.totals();
    assert_eq!((totals.operations, totals.missing_receipt), (1, 1));
    assert!(totals.has_unproven());
    assert!(!totals.has_timed_out());
}

#[test]
fn complete_joined_receipts_are_proven() {
    let census = SandboxCleanupCensus::default();
    census.record(true, Some(receipt(SandboxCleanupState::Complete, true)));
    census.record(true, Some(receipt(SandboxCleanupState::Complete, true)));
    let totals = census.totals();
    assert_eq!(
        (
            totals.operations,
            totals.complete,
            totals.reaped_targets,
            totals.host_owned_targets
        ),
        (2, 2, 4, 2)
    );
    assert!(!totals.has_unproven());
}

#[test]
fn timed_out_and_unjoined_receipts_stay_visible() {
    let census = SandboxCleanupCensus::default();
    census.record(true, Some(receipt(SandboxCleanupState::Complete, true)));
    census.record(true, Some(receipt(SandboxCleanupState::TimedOut, false)));
    let totals = census.totals();
    assert_eq!((totals.timed_out, totals.unjoined), (1, 1));
    assert!(totals.has_timed_out());
    assert!(totals.has_unproven());
}

#[test]
fn record_outcome_keeps_the_operation_error_separate_from_its_receipt() {
    let census = SandboxCleanupCensus::default();
    // A failed operation whose helper still cleaned up completely.
    let failed: FileSystemResult<()> = census.record_outcome(
        /*may_have_launched*/ true,
        FileSystemOutcome {
            result: Err(std::io::Error::other("denied")),
            sandbox_cleanup: Some(receipt(SandboxCleanupState::Complete, true)),
            launch: codex_file_system::SandboxLaunch::Launched,
        },
    );
    assert_eq!(
        failed.map_err(|err| err.to_string()),
        Err("denied".to_string())
    );
    // A successful operation without any receipt: unproven.
    let succeeded = census.record_outcome(true, FileSystemOutcome::without_receipt(Ok(7)));
    assert_eq!(succeeded.ok(), Some(7));
    let totals = census.totals();
    assert_eq!(
        (totals.operations, totals.complete, totals.missing_receipt),
        (2, 1, 1)
    );
    assert!(totals.has_unproven());
}

mod receipt_backend {
    //! The mechanism view_image, the Codex Apps upload, AGENTS.md discovery
    //! and apply_patch verification share: operations through a receipt-
    //! bearing filesystem, receipts into the census, errors untouched.
    use super::*;
    use codex_file_system::CopyOptions;
    use codex_file_system::CreateDirectoryOptions;
    use codex_file_system::ExecutorFileSystem;
    use codex_file_system::ExecutorFileSystemFuture;
    use codex_file_system::ExecutorFileSystemOutcomeFuture;
    use codex_file_system::FileMetadata;
    use codex_file_system::FileSystemReadStream;
    use codex_file_system::GetMetadataOptions;
    use codex_file_system::ReadDirectoryEntry;
    use codex_file_system::ReadFileOptions;
    use codex_file_system::RemoveOptions;
    use codex_file_system::SandboxCleanupReceiptCollector;
    use codex_file_system::WalkOptions;
    use codex_file_system::WalkOutcome;
    use codex_file_system::WriteFileOptions;
    use codex_utils_path_uri::PathUri;
    use pretty_assertions::assert_eq;

    /// Serves reads through a "helper" whose receipt is fixed per test.
    struct ReceiptFileSystem {
        read_result: Result<Vec<u8>, String>,
        receipt: Option<SandboxCleanupSummary>,
    }

    fn unused<'a, T: Send + 'a>() -> ExecutorFileSystemFuture<'a, T> {
        Box::pin(async { Err(std::io::Error::other("unused in this test")) })
    }

    impl ExecutorFileSystem for ReceiptFileSystem {
        fn canonicalize<'a>(
            &'a self,
            _path: &'a PathUri,
            _sandbox: Option<&'a FileSystemSandboxContext>,
        ) -> ExecutorFileSystemFuture<'a, PathUri> {
            unused()
        }

        fn read_file<'a>(
            &'a self,
            _path: &'a PathUri,
            _options: ReadFileOptions,
            _sandbox: Option<&'a FileSystemSandboxContext>,
        ) -> ExecutorFileSystemFuture<'a, Vec<u8>> {
            unused()
        }

        fn read_file_stream<'a>(
            &'a self,
            _path: &'a PathUri,
            _sandbox: Option<&'a FileSystemSandboxContext>,
        ) -> ExecutorFileSystemFuture<'a, FileSystemReadStream> {
            unused()
        }

        fn write_file<'a>(
            &'a self,
            _path: &'a PathUri,
            _contents: Vec<u8>,
            _options: WriteFileOptions,
            _sandbox: Option<&'a FileSystemSandboxContext>,
        ) -> ExecutorFileSystemFuture<'a, ()> {
            unused()
        }

        fn create_directory<'a>(
            &'a self,
            _path: &'a PathUri,
            _options: CreateDirectoryOptions,
            _sandbox: Option<&'a FileSystemSandboxContext>,
        ) -> ExecutorFileSystemFuture<'a, ()> {
            unused()
        }

        fn get_metadata<'a>(
            &'a self,
            _path: &'a PathUri,
            _options: GetMetadataOptions,
            _sandbox: Option<&'a FileSystemSandboxContext>,
        ) -> ExecutorFileSystemFuture<'a, FileMetadata> {
            unused()
        }

        fn read_directory<'a>(
            &'a self,
            _path: &'a PathUri,
            _sandbox: Option<&'a FileSystemSandboxContext>,
        ) -> ExecutorFileSystemFuture<'a, Vec<ReadDirectoryEntry>> {
            unused()
        }

        fn walk<'a>(
            &'a self,
            _path: &'a PathUri,
            _options: WalkOptions,
            _sandbox: Option<&'a FileSystemSandboxContext>,
        ) -> ExecutorFileSystemFuture<'a, WalkOutcome> {
            unused()
        }

        fn remove<'a>(
            &'a self,
            _path: &'a PathUri,
            _options: RemoveOptions,
            _sandbox: Option<&'a FileSystemSandboxContext>,
        ) -> ExecutorFileSystemFuture<'a, ()> {
            unused()
        }

        fn copy<'a>(
            &'a self,
            _source_path: &'a PathUri,
            _destination_path: &'a PathUri,
            _options: CopyOptions,
            _sandbox: Option<&'a FileSystemSandboxContext>,
        ) -> ExecutorFileSystemFuture<'a, ()> {
            unused()
        }

        fn read_file_with_receipt<'a>(
            &'a self,
            _path: &'a PathUri,
            _options: ReadFileOptions,
            _sandbox: Option<&'a FileSystemSandboxContext>,
        ) -> ExecutorFileSystemOutcomeFuture<'a, Vec<u8>> {
            let result = self.read_result.clone().map_err(std::io::Error::other);
            let receipt = self.receipt;
            Box::pin(async move {
                FileSystemOutcome {
                    result,
                    sandbox_cleanup: receipt,
                    // A reported receipt proves the launch; none stays unproven.
                    launch: if receipt.is_some() {
                        codex_file_system::SandboxLaunch::Launched
                    } else {
                        codex_file_system::SandboxLaunch::Unknown
                    },
                }
            })
        }
    }

    fn path() -> PathUri {
        PathUri::parse("file:///workspace/AGENTS.md").expect("valid path uri")
    }

    /// The collector path (AGENTS.md discovery, apply_patch verification).
    async fn read_through_collector(fs: &ReceiptFileSystem, census: &SandboxCleanupCensus) -> bool {
        let collector = SandboxCleanupReceiptCollector::new(fs);
        let result = collector
            .read_file(&path(), ReadFileOptions::default(), /*sandbox*/ None)
            .await;
        census.record_launch(
            resolve_launch(collector.launch(), /*may_have_launched*/ true),
            collector.receipt(),
        );
        result.is_ok()
    }

    #[tokio::test]
    async fn collector_consumers_record_on_success_and_on_error() {
        let census = SandboxCleanupCensus::default();
        let complete = receipt(SandboxCleanupState::Complete, true);
        let ok = ReceiptFileSystem {
            read_result: Ok(b"docs".to_vec()),
            receipt: Some(complete),
        };
        let failed = ReceiptFileSystem {
            read_result: Err("permission denied".to_string()),
            receipt: Some(receipt(SandboxCleanupState::TimedOut, false)),
        };
        assert!(read_through_collector(&ok, &census).await);
        assert!(!read_through_collector(&failed, &census).await);
        let totals = census.totals();
        assert_eq!(
            (totals.operations, totals.complete, totals.timed_out),
            (2, 1, 1)
        );
        assert!(totals.has_timed_out());
    }

    #[tokio::test]
    async fn direct_receipt_consumers_record_on_success_and_on_error() {
        // view_image and the Codex Apps upload call the `_with_receipt`
        // variants directly and record through `record_outcome`.
        let census = SandboxCleanupCensus::default();
        let legacy = ReceiptFileSystem {
            read_result: Err("not found".to_string()),
            receipt: None,
        };
        let outcome = legacy
            .read_file_with_receipt(&path(), ReadFileOptions::default(), None)
            .await;
        assert!(census.record_outcome(true, outcome).is_err());
        let totals = census.totals();
        assert_eq!((totals.operations, totals.missing_receipt), (1, 1));
        assert!(totals.has_unproven(), "a missing receipt is never clean");
    }
}

mod stream_receipts {
    //! The Codex Apps upload's read streams: each receipt resolves when its
    //! stream ends and is settled into the census, bounded, never driven.
    use super::*;
    use codex_file_system::FileSystemReadStream;
    use codex_file_system::ReadStreamOutcome;
    use codex_file_system::SandboxLaunch;
    use futures::StreamExt;
    use pretty_assertions::assert_eq;

    fn launched_open(
        stream: FileSystemReadStream,
        receipt: Option<SandboxCleanupSummary>,
    ) -> ReadStreamOutcome {
        ReadStreamOutcome::from_open(Ok(stream), receipt, SandboxLaunch::Launched)
    }

    #[tokio::test]
    async fn consumed_stream_receipt_is_recorded_as_reported() {
        let census = Arc::new(SandboxCleanupCensus::default());
        let receipts =
            PendingStreamReceipts::new(Arc::clone(&census), /*may_have_launched*/ true);
        let mut stream = receipts
            .track(launched_open(
                FileSystemReadStream::new(futures::stream::empty()),
                Some(receipt(SandboxCleanupState::Complete, true)),
            ))
            .expect("open succeeded");
        assert!(stream.next().await.is_none());
        drop(stream);
        receipts.settle(STREAM_RECEIPT_SETTLE_GRACE).await;
        let totals = census.totals();
        assert_eq!((totals.operations, totals.complete), (1, 1));
        assert!(!totals.has_unproven());
    }

    #[tokio::test]
    async fn dropped_stream_still_delivers_its_receipt() {
        let census = Arc::new(SandboxCleanupCensus::default());
        let receipts = PendingStreamReceipts::new(Arc::clone(&census), true);
        let stream = receipts
            .track(launched_open(
                FileSystemReadStream::new(futures::stream::pending()),
                Some(receipt(SandboxCleanupState::TimedOut, false)),
            ))
            .expect("open succeeded");
        // The upload failed before reading: the stream is only dropped.
        drop(stream);
        receipts.settle(STREAM_RECEIPT_SETTLE_GRACE).await;
        assert_eq!(census.totals().timed_out, 1);
    }

    #[tokio::test]
    async fn receipt_that_never_arrives_is_unproven_after_the_bound() {
        let census = Arc::new(SandboxCleanupCensus::default());
        let receipts = PendingStreamReceipts::new(Arc::clone(&census), true);
        let _still_open = receipts
            .track(launched_open(
                FileSystemReadStream::new(futures::stream::pending()),
                Some(receipt(SandboxCleanupState::Complete, true)),
            ))
            .expect("open succeeded");
        receipts.settle(std::time::Duration::from_millis(10)).await;
        let totals = census.totals();
        assert_eq!((totals.operations, totals.missing_receipt), (1, 1));
        assert!(totals.has_unproven());
    }

    #[test]
    fn abandoned_receipts_count_as_missing() {
        let census = Arc::new(SandboxCleanupCensus::default());
        let receipts = PendingStreamReceipts::new(Arc::clone(&census), true);
        let failed_open = ReadStreamOutcome::from_open(
            Err(std::io::Error::other("denied")),
            Some(receipt(SandboxCleanupState::Complete, true)),
            SandboxLaunch::Launched,
        );
        assert!(receipts.track(failed_open).is_err());
        // The owner was cancelled before settling.
        drop(receipts);
        assert_eq!(census.totals().missing_receipt, 1);
    }

    #[tokio::test]
    async fn not_launched_stream_is_nothing_to_clean() {
        let census = Arc::new(SandboxCleanupCensus::default());
        // Even if the caller could not rule it out, the executor's positive
        // evidence wins.
        let receipts = PendingStreamReceipts::new(Arc::clone(&census), true);
        let stream = receipts
            .track(ReadStreamOutcome::from_open(
                Ok(FileSystemReadStream::new(futures::stream::empty())),
                None,
                SandboxLaunch::NotLaunched,
            ))
            .expect("open succeeded");
        drop(stream);
        receipts.settle(STREAM_RECEIPT_SETTLE_GRACE).await;
        let totals = census.totals();
        assert_eq!((totals.no_launch, totals.operations), (1, 0));
        assert!(!totals.has_unproven());
    }
}

#[test]
fn launch_evidence_decides_whether_a_missing_receipt_is_owed() {
    let census = SandboxCleanupCensus::default();
    // Positively nothing launched: nothing owed.
    let _ = census.record_outcome(
        /*may_have_launched*/ true,
        FileSystemOutcome::not_launched(Ok(())),
    );
    assert!(!census.totals().has_unproven());
    // Unknown evidence for a request that cannot launch (unsandboxed read):
    // the predicate fallback makes it a positive no-launch.
    let _ = census.record_outcome(false, FileSystemOutcome::without_receipt(Ok(())));
    assert_eq!(census.totals().no_launch, 2);
    assert!(!census.totals().has_unproven());
    // Launched without a receipt, or Unknown where a launch was possible:
    // unproven.
    let _ = census.record_outcome(true, FileSystemOutcome::launched(Ok(()), None));
    let _ = census.record_outcome(true, FileSystemOutcome::without_receipt(Ok(())));
    let totals = census.totals();
    assert_eq!((totals.no_launch, totals.missing_receipt), (2, 2));
    assert!(totals.has_unproven());
}

#[test]
fn reported_launch_evidence_is_never_overridden_by_the_predicate() {
    assert_eq!(
        resolve_launch(SandboxLaunch::Launched, /*may_have_launched*/ false),
        SandboxLaunch::Launched
    );
    assert_eq!(
        resolve_launch(SandboxLaunch::NotLaunched, true),
        SandboxLaunch::NotLaunched
    );
    assert_eq!(
        resolve_launch(SandboxLaunch::Unknown, true),
        SandboxLaunch::Unknown
    );
    assert_eq!(
        resolve_launch(SandboxLaunch::Unknown, false),
        SandboxLaunch::NotLaunched
    );
}

#[test]
fn cancelled_owner_operation_records_missing_or_positive_no_launch() {
    for may_launch in [true, false] {
        let census = Arc::new(SandboxCleanupCensus::default());
        let operation = SandboxCleanupOperation::new(Arc::clone(&census), may_launch);
        drop(operation);
        let totals = census.totals();
        assert_eq!(totals.missing_receipt, u64::from(may_launch));
        assert_eq!(totals.no_launch, u64::from(!may_launch));
    }
}

#[test]
fn owner_attempts_keep_timeout_missing_and_retry_separate() {
    let census = Arc::new(SandboxCleanupCensus::default());
    let first = SandboxCleanupOperation::new(Arc::clone(&census), true);
    first.finish(
        SandboxLaunch::Launched,
        Some(receipt(SandboxCleanupState::TimedOut, false)),
    );
    let second = SandboxCleanupOperation::new(Arc::clone(&census), true);
    second
        .finish_outcome(FileSystemOutcome::launched(Ok(()), None))
        .unwrap();
    let totals = census.totals();
    assert_eq!(
        (totals.operations, totals.timed_out, totals.missing_receipt),
        (2, 1, 1)
    );
    assert!(totals.has_unproven());
}

#[tokio::test]
async fn cancelled_stream_settlement_records_every_unresolved_receipt() {
    use futures::FutureExt;
    let census = Arc::new(SandboxCleanupCensus::default());
    let receipts = PendingStreamReceipts::new(Arc::clone(&census), true);
    let mut streams = Vec::new();
    for _ in 0..2 {
        streams.push(
            receipts
                .track(ReadStreamOutcome::from_open(
                    Ok(FileSystemReadStream::new(futures::stream::pending())),
                    None,
                    SandboxLaunch::Launched,
                ))
                .unwrap(),
        );
    }
    let mut settlement = Box::pin(receipts.settle(STREAM_RECEIPT_SETTLE_GRACE));
    assert!(settlement.as_mut().now_or_never().is_none());
    drop(settlement);
    drop(receipts);
    assert_eq!(census.totals().missing_receipt, 2);
    drop(streams);
    assert_eq!(census.totals().operations, 2);
}

#[tokio::test]
async fn cancelled_stream_settlement_keeps_already_recorded_proof() {
    use futures::FutureExt;
    let census = Arc::new(SandboxCleanupCensus::default());
    let receipts = PendingStreamReceipts::new(Arc::clone(&census), true);
    let stream = receipts
        .track(ReadStreamOutcome::from_open(
            Ok(FileSystemReadStream::new(futures::stream::pending())),
            None,
            SandboxLaunch::Launched,
        ))
        .unwrap();
    // LIFO settlement records this already delivered receipt first.
    assert!(
        receipts
            .track(ReadStreamOutcome::from_open(
                Err(std::io::Error::other("open failed")),
                Some(receipt(SandboxCleanupState::Complete, true)),
                SandboxLaunch::Launched
            ))
            .is_err()
    );
    let mut settlement = Box::pin(receipts.settle(STREAM_RECEIPT_SETTLE_GRACE));
    assert!(settlement.as_mut().now_or_never().is_none());
    assert_eq!(census.totals().complete, 1);
    drop(settlement);
    drop(receipts);
    drop(stream);
    assert_eq!(
        (census.totals().complete, census.totals().missing_receipt),
        (1, 1)
    );
}

#[tokio::test]
async fn complete_stream_settlement_is_exactly_once() {
    let census = Arc::new(SandboxCleanupCensus::default());
    let receipts = PendingStreamReceipts::new(Arc::clone(&census), true);
    for _ in 0..2 {
        assert!(
            receipts
                .track(ReadStreamOutcome::from_open(
                    Err(std::io::Error::other("open failed")),
                    Some(receipt(SandboxCleanupState::Complete, true)),
                    SandboxLaunch::Launched
                ))
                .is_err()
        );
    }
    receipts.settle(STREAM_RECEIPT_SETTLE_GRACE).await;
    drop(receipts);
    assert_eq!(
        (census.totals().operations, census.totals().complete),
        (2, 2)
    );
    assert_eq!(census.totals().missing_receipt, 0);
}

#[test]
fn remote_full_disk_context_cancellation_requires_actual_no_launch_evidence() {
    let remote =
        Environment::create_for_tests(Some("ws://127.0.0.1:1".to_string())).expect("lazy remote");
    assert!(remote.is_remote());
    let sandbox = FileSystemSandboxContext::from_permission_profile(
        codex_protocol::models::PermissionProfile::Disabled,
        codex_utils_path_uri::PathUri::parse("file:///workspace").expect("executor cwd"),
    );
    assert!(!sandbox.should_read_from_sandbox());
    assert!(!sandbox.should_write_into_sandbox());
    assert!(may_launch_sandboxed_read_helper(Some(&sandbox), &remote));
    assert!(may_launch_sandboxed_helper(Some(&sandbox), &remote));
    assert!(!may_launch_sandboxed_read_helper(None, &remote));
    let census = Arc::new(SandboxCleanupCensus::default());
    drop(SandboxCleanupOperation::new(
        Arc::clone(&census),
        may_launch_sandboxed_read_helper(Some(&sandbox), &remote),
    ));
    SandboxCleanupOperation::new(Arc::clone(&census), true)
        .finish(SandboxLaunch::NotLaunched, None);
    let totals = census.totals();
    assert_eq!(
        (totals.operations, totals.missing_receipt, totals.no_launch),
        (1, 1, 1)
    );
    assert!(totals.has_unproven());
}

#[test]
fn remote_unknown_stream_cancellation_cannot_be_relabelled_as_local_absence() {
    let remote =
        Environment::create_for_tests(Some("ws://127.0.0.1:1".to_string())).expect("lazy remote");
    let sandbox = FileSystemSandboxContext::from_permission_profile(
        codex_protocol::models::PermissionProfile::Disabled,
        codex_utils_path_uri::PathUri::parse("file:///workspace").expect("executor cwd"),
    );
    let census = Arc::new(SandboxCleanupCensus::default());
    let pending = PendingStreamReceipts::new(
        Arc::clone(&census),
        may_launch_sandboxed_read_helper(Some(&sandbox), &remote),
    );
    let stream = pending
        .track(ReadStreamOutcome::from_open(
            Ok(FileSystemReadStream::new(futures::stream::pending())),
            None,
            SandboxLaunch::Unknown,
        ))
        .expect("stream");
    drop(pending);
    drop(stream);
    let totals = census.totals();
    assert_eq!(
        (totals.operations, totals.missing_receipt, totals.no_launch),
        (1, 1, 0)
    );
    assert!(totals.has_unproven());
}

#[tokio::test]
async fn cancelled_stream_open_cannot_erase_launch_debt() {
    use futures::FutureExt;
    let census = Arc::new(SandboxCleanupCensus::default());
    let receipts = PendingStreamReceipts::new(Arc::clone(&census), true);
    let mut opening = Box::pin(receipts.open(futures::future::pending));
    assert!(opening.as_mut().now_or_never().is_none());
    drop(opening);
    drop(receipts);
    assert_eq!(
        (census.totals().operations, census.totals().missing_receipt),
        (1, 1)
    );
}

#[tokio::test]
async fn completed_stream_open_transfers_debt_without_double_recording() {
    let census = Arc::new(SandboxCleanupCensus::default());
    let receipts = PendingStreamReceipts::new(Arc::clone(&census), true);
    assert!(
        receipts
            .open(|| async {
                ReadStreamOutcome::from_open(
                    Err(std::io::Error::other("open failed")),
                    Some(receipt(SandboxCleanupState::Complete, true)),
                    SandboxLaunch::Launched,
                )
            })
            .await
            .is_err()
    );
    assert_eq!(census.totals().operations, 0);
    receipts.settle(STREAM_RECEIPT_SETTLE_GRACE).await;
    drop(receipts);
    assert_eq!(
        (census.totals().operations, census.totals().complete),
        (1, 1)
    );
}

#[tokio::test]
async fn stream_open_guard_precedes_backend_future_construction() {
    use futures::FutureExt;
    let census = Arc::new(SandboxCleanupCensus::default());
    let receipts = PendingStreamReceipts::new(Arc::clone(&census), true);
    let result =
        std::panic::AssertUnwindSafe(receipts.open(|| -> std::future::Ready<ReadStreamOutcome> {
            panic!("backend refused before returning its future");
        }))
        .catch_unwind()
        .await;
    assert!(result.is_err());
    drop(receipts);
    let totals = census.totals();
    assert_eq!((totals.operations, totals.missing_receipt), (1, 1));
}
