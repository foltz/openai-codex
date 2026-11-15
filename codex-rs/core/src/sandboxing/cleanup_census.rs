//! In-memory census of sandbox cleanup receipts for session operations that
//! have no user-facing place to report them (filesystem helpers serving
//! view_image, Codex Apps file uploads, AGENTS.md discovery and apply_patch
//! verification). Owner wiring and session retirement consume this census;
//! constructing it alone does not attach any operation or prove retirement.
//! Nothing here is journaled or shown to the model or the user.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use codex_exec_server::Environment;
use codex_file_system::FileSystemOutcome;
use codex_file_system::FileSystemReadStream;
use codex_file_system::FileSystemResult;
use codex_file_system::FileSystemSandboxContext;
use codex_file_system::ReadStreamOutcome;
use codex_file_system::SandboxCleanupReceiptHandle;
use codex_file_system::SandboxLaunch;
use codex_protocol::exec_output::SandboxCleanupState;
use codex_protocol::exec_output::SandboxCleanupSummary;

/// Per-category totals of every recorded operation's cleanup.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SandboxCleanupCensusTotals {
    /// Operations positively known to launch no sandboxed helper: nothing
    /// to clean. Never unproven.
    pub(crate) no_launch: u64,
    /// Operations that may have launched a sandboxed helper.
    pub(crate) operations: u64,
    /// Receipts by state.
    pub(crate) complete: u64,
    pub(crate) incomplete: u64,
    pub(crate) unreported: u64,
    pub(crate) timed_out: u64,
    /// Operations that may have launched a helper but returned no receipt
    /// (a legacy or receipt-less backend): unproven, never clean by default.
    pub(crate) missing_receipt: u64,
    /// Receipts whose helpers were not all observed to exit.
    pub(crate) unjoined: u64,
    /// Summed per-target counts across receipts.
    pub(crate) reaped_targets: u64,
    pub(crate) preserved_unproven_targets: u64,
    pub(crate) host_owned_targets: u64,
    pub(crate) handed_off_targets: u64,
    pub(crate) timed_out_targets: u64,
}

impl SandboxCleanupCensusTotals {
    /// Some cleanup passed its deadline (sticky).
    pub(crate) fn has_timed_out(&self) -> bool {
        self.timed_out > 0 || self.timed_out_targets > 0
    }

    /// Some cleanup is not proven complete: a missing receipt, an
    /// incomplete or unreported one, an unjoined helper, or a target
    /// preserved as unproven.
    pub(crate) fn has_unproven(&self) -> bool {
        self.missing_receipt > 0
            || self.incomplete > 0
            || self.unreported > 0
            || self.unjoined > 0
            || self.preserved_unproven_targets > 0
            || self.has_timed_out()
    }
}

/// The session's census. Operations record whether they succeeded or not:
/// the operation's error is the caller's, the receipt is cleanup's.
#[derive(Debug, Default)]
pub(crate) struct SandboxCleanupCensus {
    totals: Mutex<SandboxCleanupCensusTotals>,
}

impl SandboxCleanupCensus {
    /// Records one operation from the executor's launch evidence:
    /// - `NotLaunched` without a receipt: "no launch: nothing to clean";
    /// - `Launched` or `Unknown` without a receipt: missing, i.e. unproven;
    /// - any receipt: counted as reported (evidence wins).
    pub(crate) fn record_launch(
        &self,
        launch: SandboxLaunch,
        receipt: Option<SandboxCleanupSummary>,
    ) {
        let mut totals = self
            .totals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if launch == SandboxLaunch::NotLaunched && receipt.is_none() {
            totals.no_launch = totals.no_launch.saturating_add(1);
            return;
        }
        totals.operations = totals.operations.saturating_add(1);
        let Some(receipt) = receipt else {
            totals.missing_receipt = totals.missing_receipt.saturating_add(1);
            return;
        };
        let state = match receipt.state {
            SandboxCleanupState::Complete => &mut totals.complete,
            SandboxCleanupState::Incomplete => &mut totals.incomplete,
            SandboxCleanupState::Unreported => &mut totals.unreported,
            SandboxCleanupState::TimedOut => &mut totals.timed_out,
        };
        *state = state.saturating_add(1);
        if !receipt.joined {
            totals.unjoined = totals.unjoined.saturating_add(1);
        }
        let add = |total: &mut u64, count: u32| *total = total.saturating_add(u64::from(count));
        add(&mut totals.reaped_targets, receipt.reaped);
        add(
            &mut totals.preserved_unproven_targets,
            receipt.preserved_unproven,
        );
        add(&mut totals.host_owned_targets, receipt.host_owned);
        add(&mut totals.handed_off_targets, receipt.handed_off);
        add(&mut totals.timed_out_targets, receipt.timed_out);
    }

    /// Fallback for operations that carry no launch evidence at all: the
    /// caller's [`may_launch_sandboxed_read_helper`] verdict stands in for it.
    /// Callers with executor evidence use `record_outcome`; this boolean-only
    /// fallback is test-only.
    #[cfg(test)]
    pub(crate) fn record(&self, may_have_launched: bool, receipt: Option<SandboxCleanupSummary>) {
        let launch = if may_have_launched {
            SandboxLaunch::Unknown
        } else {
            SandboxLaunch::NotLaunched
        };
        self.record_launch(launch, receipt);
    }

    /// Records a receipt-bearing operation's cleanup and hands back its
    /// result unchanged: an error is the operation's, the receipt cleanup's.
    pub(crate) fn record_outcome<T>(
        &self,
        may_have_launched: bool,
        outcome: FileSystemOutcome<T>,
    ) -> FileSystemResult<T> {
        self.record_launch(
            resolve_launch(outcome.launch, may_have_launched),
            outcome.sandbox_cleanup,
        );
        outcome.result
    }

    pub(crate) fn totals(&self) -> SandboxCleanupCensusTotals {
        *self
            .totals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// One started owner operation. Cancellation cannot erase its launch debt.
/// Construct this inside the owner's async body before awaiting filesystem work.
/// Session retirement joins/cancels owner tasks before reading the final census.
pub(crate) struct SandboxCleanupOperation {
    census: Arc<SandboxCleanupCensus>,
    fallback_launch: SandboxLaunch,
    recorded: bool,
}

impl SandboxCleanupOperation {
    pub(crate) fn new(census: Arc<SandboxCleanupCensus>, may_have_launched: bool) -> Self {
        Self::with_launch(
            census,
            resolve_launch(SandboxLaunch::Unknown, may_have_launched),
        )
    }

    fn with_launch(census: Arc<SandboxCleanupCensus>, fallback_launch: SandboxLaunch) -> Self {
        Self {
            census,
            fallback_launch,
            recorded: false,
        }
    }

    pub(crate) fn finish(mut self, launch: SandboxLaunch, receipt: Option<SandboxCleanupSummary>) {
        let launch = resolve_launch(launch, self.fallback_launch != SandboxLaunch::NotLaunched);
        self.census.record_launch(launch, receipt);
        self.recorded = true;
    }

    pub(crate) fn finish_outcome<T>(self, outcome: FileSystemOutcome<T>) -> FileSystemResult<T> {
        self.finish(outcome.launch, outcome.sandbox_cleanup);
        outcome.result
    }

    fn finish_receipt(self, receipt: Option<SandboxCleanupSummary>) {
        let launch = self.fallback_launch;
        self.finish(launch, receipt);
    }
}

impl Drop for SandboxCleanupOperation {
    fn drop(&mut self) {
        if !self.recorded {
            self.census.record_launch(self.fallback_launch, None);
        }
    }
}

/// How long [`PendingStreamReceipts::settle`] waits, after the owner is done
/// with its streams, for receipts still in flight. Waiting drives nothing:
/// a stream delivers its receipt when it ends or is dropped.
pub(crate) const STREAM_RECEIPT_SETTLE_GRACE: Duration = Duration::from_secs(2);

/// Cleanup receipts of read streams, each resolving when its stream ends.
/// The owner tracks every open, settles once it has consumed or dropped the
/// streams, and every receipt lands in the census exactly once: resolved
/// ones as reported, ones that never arrive as missing (unproven).
pub(crate) struct PendingStreamReceipts {
    census: Arc<SandboxCleanupCensus>,
    may_have_launched: bool,
    handles: Mutex<Vec<(SandboxCleanupReceiptHandle, SandboxLaunch)>>,
}

impl PendingStreamReceipts {
    pub(crate) fn new(census: Arc<SandboxCleanupCensus>, may_have_launched: bool) -> Self {
        Self {
            census,
            may_have_launched,
            handles: Mutex::new(Vec::new()),
        }
    }

    /// Registers launch debt before invoking the backend, not only before
    /// polling its future. Transfer it to the retained receipt without an await gap.
    pub(crate) async fn open<F>(
        &self,
        open: impl FnOnce() -> F,
    ) -> FileSystemResult<FileSystemReadStream>
    where
        F: std::future::Future<Output = ReadStreamOutcome>,
    {
        let mut operation =
            SandboxCleanupOperation::new(Arc::clone(&self.census), self.may_have_launched);
        let outcome = open().await;
        let result = self.track(outcome);
        operation.recorded = true;
        result
    }

    /// Tracks an open's receipt and hands back its result.
    pub(crate) fn track(&self, open: ReadStreamOutcome) -> FileSystemResult<FileSystemReadStream> {
        let launch = resolve_launch(open.launch, self.may_have_launched);
        self.handles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((open.sandbox_cleanup, launch));
        open.result
    }

    fn take_handles(&self) -> Vec<(SandboxCleanupReceiptHandle, SandboxLaunch)> {
        std::mem::take(
            &mut *self
                .handles
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    fn pop_handle(&self) -> Option<(SandboxCleanupReceiptHandle, SandboxLaunch)> {
        self.handles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop()
    }

    /// Records every tracked receipt, waiting at most `grace` in total for
    /// those not yet delivered. Call only after the streams were consumed or
    /// dropped; a receipt still missing at the deadline counts as unproven.
    pub(crate) async fn settle(&self, grace: Duration) {
        let deadline = tokio::time::Instant::now() + grace;
        // Only move one receipt out at a time. The guard accounts for an
        // in-flight cancellation; unpopped entries stay owned by self's Drop.
        while let Some((handle, launch)) = self.pop_handle() {
            let operation = SandboxCleanupOperation::with_launch(Arc::clone(&self.census), launch);
            let receipt = tokio::time::timeout_at(deadline, handle.wait())
                .await
                .unwrap_or(None);
            operation.finish_receipt(receipt);
        }
    }
}

impl Drop for PendingStreamReceipts {
    /// Abandoned before settling (the owner was cancelled): nothing proves
    /// those launches clean, so each counts as a missing receipt.
    fn drop(&mut self) {
        for (_, launch) in self.take_handles() {
            self.census.record_launch(launch, None);
        }
    }
}

/// The launch evidence to record. `Unknown` (a backend that cannot tell)
/// stays unproven unless the request itself could not launch a helper: a
/// read the executor serves unsandboxed (see
/// [`may_launch_sandboxed_read_helper`]) is positively no launch. Reported
/// `NotLaunched`/`Launched` evidence is never overridden.
pub(crate) fn resolve_launch(launch: SandboxLaunch, may_have_launched: bool) -> SandboxLaunch {
    if launch == SandboxLaunch::Unknown && !may_have_launched {
        SandboxLaunch::NotLaunched
    } else {
        launch
    }
}

/// Whether a filesystem read could owe a Linux helper cleanup receipt.
/// No context proves no launch. A local non-Linux executor has no Linux
/// custody; local Linux additionally checks whether the read needs a sandbox.
/// A remote executor may be older and sandbox any supplied context, including
/// a full-disk policy retained for explicit-path validation. Until it reports
/// otherwise, cancellation or Unknown with a context therefore stays unproven.
pub(crate) fn may_launch_sandboxed_read_helper(
    sandbox: Option<&FileSystemSandboxContext>,
    environment: &Environment,
) -> bool {
    sandbox.is_some_and(|sandbox| {
        environment.is_remote() || (cfg!(target_os = "linux") && sandbox.should_read_from_sandbox())
    })
}

/// Like the read predicate for operations that may also write. Positive
/// executor launch evidence still takes precedence over this fallback.
pub(crate) fn may_launch_sandboxed_helper(
    sandbox: Option<&FileSystemSandboxContext>,
    environment: &Environment,
) -> bool {
    sandbox.is_some_and(|sandbox| {
        environment.is_remote()
            || (cfg!(target_os = "linux")
                && (sandbox.should_read_from_sandbox() || sandbox.should_write_into_sandbox()))
    })
}

#[cfg(test)]
#[path = "cleanup_census_tests.rs"]
mod tests;
