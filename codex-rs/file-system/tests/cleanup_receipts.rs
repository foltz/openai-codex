use bytes::Bytes;
use codex_file_system::FileSystemReadStream;
use codex_file_system::ReadStreamOutcome;
use codex_file_system::SandboxLaunch;
use codex_protocol::exec_output::SandboxCleanupState;
use codex_protocol::exec_output::SandboxCleanupSummary;
use futures::FutureExt;
use futures::StreamExt;

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

#[test]
fn stream_receipt_waits_for_natural_end() {
    let stream =
        FileSystemReadStream::new(futures::stream::iter([Ok(Bytes::from_static(b"data"))]));
    let (mut stream, receipt) = stream.with_cleanup_receipt(Some(complete()));
    let mut receipt = Box::pin(receipt.wait());
    assert!(receipt.as_mut().now_or_never().is_none());
    futures::executor::block_on(async {
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            Bytes::from_static(b"data")
        );
        assert!(receipt.as_mut().now_or_never().is_none());
        assert!(stream.next().await.is_none());
        assert_eq!(receipt.await, Some(complete()));
    });
}

#[test]
fn dropping_an_unconsumed_stream_delivers_its_receipt() {
    let stream = FileSystemReadStream::new(futures::stream::pending());
    let (stream, receipt) = stream.with_cleanup_receipt(Some(complete()));
    let mut receipt = Box::pin(receipt.wait());
    assert!(receipt.as_mut().now_or_never().is_none());
    drop(stream);
    assert_eq!(futures::executor::block_on(receipt), Some(complete()));
}

#[test]
fn stream_error_keeps_cleanup_evidence_independent() {
    let stream = FileSystemReadStream::new(futures::stream::iter([Err(std::io::Error::other(
        "read failed",
    ))]));
    let (mut stream, receipt) = stream.with_cleanup_receipt(Some(complete()));
    futures::executor::block_on(async {
        assert!(stream.next().await.unwrap().is_err());
        assert_eq!(receipt.wait().await, Some(complete()));
    });
}

#[test]
fn failed_open_preserves_the_reported_cleanup() {
    let outcome = ReadStreamOutcome::from_open(
        Err(std::io::Error::other("open failed")),
        Some(complete()),
        SandboxLaunch::Launched,
    );
    assert!(outcome.result.is_err());
    assert_eq!(outcome.launch, SandboxLaunch::Launched);
    assert_eq!(
        futures::executor::block_on(outcome.sandbox_cleanup.wait()),
        Some(complete())
    );
}

#[test]
fn missing_receipt_is_not_synthesized_when_a_stream_drops() {
    let stream = FileSystemReadStream::new(futures::stream::pending());
    let (stream, receipt) = stream.with_cleanup_receipt(None);
    drop(stream);
    assert_eq!(futures::executor::block_on(receipt.wait()), None);
}

#[test]
fn merging_preserves_worst_state_saturating_counts_and_unjoined_evidence() {
    use codex_file_system::merge_sandbox_cleanup_summaries;
    let states = [
        SandboxCleanupState::Complete,
        SandboxCleanupState::Incomplete,
        SandboxCleanupState::Unreported,
        SandboxCleanupState::TimedOut,
    ];
    for (a, first) in states.into_iter().enumerate() {
        for (b, second) in states.into_iter().enumerate() {
            let merged = merge_sandbox_cleanup_summaries(
                SandboxCleanupSummary {
                    state: first,
                    reaped: u32::MAX,
                    preserved_unproven: u32::MAX,
                    host_owned: u32::MAX,
                    handed_off: u32::MAX,
                    timed_out: u32::MAX,
                    joined: true,
                },
                SandboxCleanupSummary {
                    state: second,
                    reaped: 1,
                    preserved_unproven: 1,
                    host_owned: 1,
                    handed_off: 1,
                    timed_out: 1,
                    joined: false,
                },
            );
            assert_eq!(merged.state, states[a.max(b)]);
            assert!(!merged.joined);
            assert_eq!(
                (
                    merged.reaped,
                    merged.preserved_unproven,
                    merged.host_owned,
                    merged.handed_off,
                    merged.timed_out
                ),
                (u32::MAX, u32::MAX, u32::MAX, u32::MAX, u32::MAX)
            );
        }
    }
}

#[test]
fn launch_aggregation_never_erases_unknown_evidence() {
    let states = [
        SandboxLaunch::NotLaunched,
        SandboxLaunch::Launched,
        SandboxLaunch::Unknown,
    ];
    for (a, first) in states.into_iter().enumerate() {
        for (b, second) in states.into_iter().enumerate() {
            assert_eq!(first.merge(second), states[a.max(b)]);
        }
    }
}
