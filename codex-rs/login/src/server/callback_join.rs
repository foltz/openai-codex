use crate::LoginCallbackResult;
use std::io;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

/// Retains the callback join while a caller borrows its exclusive poll right.
/// The callback never accesses this mutex, so cancellation only drops a borrow.
pub(super) struct CallbackJoin(Mutex<State>);

impl std::fmt::Debug for CallbackJoin {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The single-use legacy result may contain backend error detail. The
        // cloneable shutdown handle must not expose it through Debug.
        formatter
            .debug_struct("CallbackJoin")
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
enum State {
    Running(JoinHandle<io::Result<LoginCallbackResult>>),
    Finished {
        outcome: CallbackOutcome,
        result: Option<io::Result<LoginCallbackResult>>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CallbackOutcome {
    Returned,
    Cancelled,
    Panicked,
}

impl CallbackJoin {
    pub(super) fn new(handle: JoinHandle<io::Result<LoginCallbackResult>>) -> Self {
        Self(Mutex::new(State::Running(handle)))
    }

    pub(super) async fn take_result(&self) -> io::Result<LoginCallbackResult> {
        let mut state = self.0.lock().await;
        Self::observe(&mut state).await;
        let State::Finished { result, .. } = &mut *state else {
            unreachable!("callback join was observed under its exclusive poll right")
        };
        result
            .take()
            .unwrap_or_else(|| Err(io::Error::other("login callback result already consumed")))
    }

    pub(super) async fn wait(&self) -> CallbackOutcome {
        let mut state = self.0.lock().await;
        Self::observe(&mut state).await;
        let State::Finished { outcome, .. } = &*state else {
            unreachable!("callback join was observed under its exclusive poll right")
        };
        *outcome
    }

    async fn observe(state: &mut State) {
        if let State::Running(handle) = &mut *state {
            let (outcome, result) = match handle.await {
                Ok(result) => (CallbackOutcome::Returned, result),
                Err(error) if error.is_cancelled() => (
                    CallbackOutcome::Cancelled,
                    Err(io::Error::other("login callback task cancelled")),
                ),
                Err(_) => (
                    CallbackOutcome::Panicked,
                    Err(io::Error::other("login callback task panicked")),
                ),
            };
            *state = State::Finished {
                outcome,
                result: Some(result),
            };
        }
    }
}

#[cfg(test)]
#[path = "callback_join_tests.rs"]
mod tests;
