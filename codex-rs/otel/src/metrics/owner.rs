use super::client::MetricsClientInner;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::Weak;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MetricsOwnerError {
    Unavailable,
    InProgress,
    ExporterFailed,
    Panicked,
}

type Outcome = Result<(), MetricsOwnerError>;

enum AttemptState {
    Pending,
    Running(std::thread::ThreadId),
    Complete(Outcome),
}

/// A receipt contains no exporter and never changes identity across retries.
pub(super) struct MetricsShutdownAttempt {
    owner: Weak<MetricsOwner>,
    state: Mutex<AttemptState>,
    completed: Condvar,
}

struct OwnerState {
    inner: Option<Arc<MetricsClientInner>>,
    attempt: Arc<MetricsShutdownAttempt>,
}

/// Central custody of one original exporter, separate from routed operations.
/// Failed attempts retain custody; successful shutdown releases it. The caller
/// must first close and drain the corresponding route generation.
pub(super) struct MetricsOwner {
    state: Mutex<OwnerState>,
}

impl std::fmt::Debug for MetricsOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MetricsOwner")
    }
}

impl MetricsOwner {
    pub(super) fn inner(&self) -> Result<Arc<MetricsClientInner>, MetricsOwnerError> {
        self.state
            .lock()
            .map_err(|_| MetricsOwnerError::Unavailable)?
            .inner
            .clone()
            .ok_or(MetricsOwnerError::Unavailable)
    }
    pub(super) fn new(inner: Arc<MetricsClientInner>) -> Arc<Self> {
        Arc::new_cyclic(|owner| Self {
            state: Mutex::new(OwnerState {
                inner: Some(inner),
                attempt: Arc::new(MetricsShutdownAttempt {
                    owner: owner.clone(),
                    state: Mutex::new(AttemptState::Pending),
                    completed: Condvar::new(),
                }),
            }),
        })
    }

    /// Re-observe the sole SDK attempt. A returned failure or panic is sticky:
    /// SDK shutdown is consumed even on failure, so same-process recovery must
    /// refuse this row rather than invent a second successful shutdown.
    pub(super) fn attempt(&self) -> Result<Arc<MetricsShutdownAttempt>, MetricsOwnerError> {
        Ok(Arc::clone(
            &self
                .state
                .lock()
                .map_err(|_| MetricsOwnerError::Unavailable)?
                .attempt,
        ))
    }
}

impl MetricsShutdownAttempt {
    /// Blocking observation used only on an owned shutdown worker. Waits for a
    /// concurrent public winner without re-running SDK shutdown. Callback
    /// re-entry on that same thread refuses instead of waiting on itself.
    pub(super) fn run_observing(&self) -> Outcome {
        match self.claim() {
            Ok(Some(result)) => result,
            Ok(None) => self.execute_claimed(),
            Err(MetricsOwnerError::InProgress) => {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| MetricsOwnerError::Unavailable)?;
                loop {
                    match *state {
                        AttemptState::Complete(result) => return result,
                        AttemptState::Running(thread) if thread == std::thread::current().id() => {
                            return Err(MetricsOwnerError::InProgress);
                        }
                        AttemptState::Running(_) => {
                            state = self
                                .completed
                                .wait(state)
                                .map_err(|_| MetricsOwnerError::Unavailable)?;
                        }
                        AttemptState::Pending => return Err(MetricsOwnerError::Unavailable),
                    }
                }
            }
            Err(error) => Err(error),
        }
    }
    /// Run on a blocking worker. Concurrent or callback-reentrant callers
    /// receive InProgress, never wait on their own SDK call or claim success.
    pub(super) fn run(&self) -> Outcome {
        match self.claim()? {
            Some(result) => result,
            None => self.execute_claimed(),
        }
    }

    /// Public shutdown preserves SDK one-shot result semantics, while managed
    /// observations keep the first result. Both compete through the same claim.
    pub(super) fn shutdown_public(&self) -> super::Result<()> {
        use opentelemetry_sdk::error::OTelSdkError;
        let result = match self.claim() {
            Ok(Some(_)) => {
                return Err(super::MetricsError::ProviderShutdown {
                    source: OTelSdkError::AlreadyShutdown,
                });
            }
            Ok(None) => self.execute_claimed(),
            Err(error) => Err(error),
        };
        result.map_err(|error| super::MetricsError::ProviderShutdown {
            source: OTelSdkError::InternalFailure(
                match error {
                    MetricsOwnerError::Unavailable => "metrics shutdown state unavailable",
                    MetricsOwnerError::InProgress => "metrics shutdown is in progress",
                    MetricsOwnerError::ExporterFailed => "metrics exporter shutdown failed",
                    MetricsOwnerError::Panicked => "metrics shutdown worker panicked",
                }
                .to_owned(),
            ),
        })
    }

    fn claim(&self) -> Result<Option<Outcome>, MetricsOwnerError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| MetricsOwnerError::Unavailable)?;
        match *state {
            AttemptState::Complete(result) => return Ok(Some(result)),
            AttemptState::Running(_) => return Err(MetricsOwnerError::InProgress),
            AttemptState::Pending => *state = AttemptState::Running(std::thread::current().id()),
        }
        Ok(None)
    }

    fn execute_claimed(&self) -> Outcome {
        // Catch unwinding outside both mutexes. Even exporter panic becomes
        // a sticky failed attempt rather than leaving Running permanently.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let owner = self.owner.upgrade().ok_or(MetricsOwnerError::Unavailable)?;
            let inner = owner
                .state
                .lock()
                .map_err(|_| MetricsOwnerError::Unavailable)?
                .inner
                .clone()
                .ok_or(MetricsOwnerError::Unavailable)?;
            inner
                .shutdown()
                .map_err(|_| MetricsOwnerError::ExporterFailed)?;
            let retired = owner
                .state
                .lock()
                .map_err(|_| MetricsOwnerError::Unavailable)?
                .inner
                .take();
            drop(inner);
            drop(retired);
            Ok(())
        }))
        .unwrap_or(Err(MetricsOwnerError::Panicked));
        *self
            .state
            .lock()
            .map_err(|_| MetricsOwnerError::Unavailable)? = AttemptState::Complete(result);
        self.completed.notify_all();
        result
    }
}
