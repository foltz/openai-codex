//! Local launch custody retained through process and observer lifetimes.

use std::future::Future;
use std::io;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use codex_file_system::SandboxLaunch;
use codex_sandboxing::CleanupDeadlineScope;
use codex_sandboxing::SandboxCleanupCustody;
use codex_sandboxing::SandboxCleanupSummary;
use codex_sandboxing::SandboxCleanupTightener;
use codex_sandboxing::SandboxType;
use codex_utils_pty::ProcessPin;
use futures::FutureExt;
use futures::future::BoxFuture;
use futures::future::Shared;

use crate::sandboxing::cleanup_census::SandboxCleanupCensus;
use crate::sandboxing::cleanup_census::SandboxCleanupOperation;

const RETIRE_GRACE: Duration = Duration::from_secs(2);
type Collection = Shared<BoxFuture<'static, SandboxCleanupSummary>>;
type Operation = Arc<Mutex<Option<SandboxCleanupOperation>>>;

struct CollectionState {
    custody: Option<SandboxCleanupCustody>,
    result: Option<Collection>,
}

/// Exists only for a local LinuxSeccomp launch. The original collection
/// future stays here when an observer stops waiting; a later observer polls
/// that same future, rather than taking custody a second time.
pub(super) struct SandboxCleanup {
    state: Mutex<CollectionState>,
    tightener: SandboxCleanupTightener,
    scope: Option<CleanupDeadlineScope>,
    operation: Operation,
}

fn retained_collection(
    collect: impl Future<Output = SandboxCleanupSummary> + Send + 'static,
    operation: Operation,
) -> Collection {
    async move {
        let summary = collect.await;
        if let Some(operation) = operation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            operation.finish(SandboxLaunch::Launched, Some(summary));
        }
        summary
    }
    .boxed()
    .shared()
}

impl SandboxCleanup {
    pub(super) fn for_launch(
        sandbox: SandboxType,
        scope: Option<CleanupDeadlineScope>,
        census: Option<Arc<SandboxCleanupCensus>>,
    ) -> io::Result<Option<Self>> {
        let Some(custody) = SandboxCleanupCustody::for_launch(sandbox, None, scope.as_ref())?
        else {
            return Ok(None);
        };
        let tightener = custody.tightener();
        Ok(Some(Self {
            state: Mutex::new(CollectionState {
                custody: Some(custody),
                result: None,
            }),
            tightener,
            scope,
            operation: Arc::new(Mutex::new(
                census.map(|census| SandboxCleanupOperation::new(census, true)),
            )),
        }))
    }

    pub(super) fn attach(&mut self, command: &mut Vec<String>) -> io::Result<()> {
        self.state
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .custody
            .as_ref()
            .expect("custody precedes spawn")
            .attach_to_launcher_argv(command)
    }

    pub(super) fn inherited_fds(&self) -> Vec<i32> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .custody
            .as_ref()
            .expect("custody precedes spawn")
            .inherited_fds()
    }

    pub(super) fn spawned(&mut self, pin: Option<ProcessPin>) {
        self.state
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .custody
            .as_mut()
            .expect("custody precedes collection")
            .spawned_with_pin(pin);
    }

    /// An actual argv/spawn refusal is not an unobserved successful launch.
    pub(super) fn not_launched(&mut self) {
        if let Some(operation) = self
            .operation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            operation.finish(SandboxLaunch::NotLaunched, None);
        }
    }

    pub(super) fn tighten(&self, deadline: Instant) {
        self.tightener.tighten(deadline);
    }

    pub(super) fn terminate(&self) {
        // During retirement use exactly the already latched bound, never a
        // fresh ordinary termination grace.
        self.tighten(
            self.scope
                .as_ref()
                .and_then(CleanupDeadlineScope::latched)
                .unwrap_or_else(|| Instant::now() + RETIRE_GRACE),
        );
    }

    pub(super) async fn collect(&self) -> SandboxCleanupSummary {
        let result = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.result.is_none() {
                let custody = state.custody.take().expect("one original custody");
                state.result = Some(retained_collection(
                    custody.collect(RETIRE_GRACE),
                    Arc::clone(&self.operation),
                ));
            }
            state
                .result
                .as_ref()
                .expect("initialized original collection")
                .clone()
        };
        result.await
    }
}

impl Drop for SandboxCleanup {
    fn drop(&mut self) {
        self.terminate();
        // Do not leave the guard only in a detached blocking collector. The
        // owning session may read its census immediately after this drop.
        // A collector that outlives us cannot later erase this missing debt.
        drop(
            self.operation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take(),
        );
    }
}

#[cfg(test)]
#[path = "cleanup_tests.rs"]
mod tests;
