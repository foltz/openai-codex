use codex_login::LoginRetirement;
use codex_login::LoginRetirementReport;
use codex_login::LoginServer;
use codex_login::ServerOptions;
use codex_login::ShutdownHandle;
use std::io;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::watch;
use tokio::time::Instant;

#[derive(Clone, Default)]
pub(super) struct BrowserLogins(Arc<Mutex<State>>);

#[derive(Default)]
struct State {
    deadline: Option<Instant>,
    attempts: Vec<Arc<Attempt>>,
}

struct Attempt {
    state: Mutex<AttemptState>,
    changed: watch::Sender<u64>,
}

#[derive(Default)]
struct AttemptState {
    constructing: bool,
    deadline: Option<Instant>,
    handle: Option<ShutdownHandle>,
    retirement: Option<LoginRetirement>,
    unavailable: bool,
}

/// Keeps an admitted construction represented if it unwinds before returning.
struct Construction(Arc<Attempt>);

impl Drop for Construction {
    fn drop(&mut self) {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.constructing = false;
        self.0
            .changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }
}

impl BrowserLogins {
    pub(super) fn start(&self, options: ServerOptions) -> io::Result<LoginServer> {
        self.start_with(options, codex_login::run_login_server)
    }

    fn start_with(
        &self,
        options: ServerOptions,
        construct: impl FnOnce(ServerOptions) -> io::Result<LoginServer>,
    ) -> io::Result<LoginServer> {
        let construction = {
            let mut state = self
                .0
                .lock()
                .map_err(|_| io::Error::other("login custody unavailable"))?;
            if state.deadline.is_some() {
                return Err(io::Error::other("login admission closed"));
            }
            let attempt = Arc::new(Attempt {
                state: Mutex::new(AttemptState {
                    constructing: true,
                    unavailable: true,
                    ..Default::default()
                }),
                changed: watch::channel(0).0,
            });
            state.attempts.push(Arc::clone(&attempt));
            Construction(attempt)
        };
        // Binding/listener construction must not hold the admission mutex.
        // The reservation, not a later returned value, owns this interval.
        let server = match construct(options) {
            Ok(server) => server,
            Err(error) => {
                if let Ok(mut state) = construction.0.state.lock() {
                    state.unavailable = false;
                }
                return Err(error);
            }
        };
        let mut state = construction
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.unavailable = construction.0.state.is_poisoned();
        let handle = server.cancel_handle();
        if let Some(deadline) = state.deadline {
            match handle.begin_retirement(deadline) {
                Ok(retirement) => state.retirement = Some(retirement),
                Err(_) => state.unavailable = true,
            }
        }
        state.handle = Some(handle);
        let closed = state.deadline.is_some();
        drop(state);
        drop(construction);
        if closed {
            Err(io::Error::other(
                "login admission closed during construction",
            ))
        } else {
            Ok(server)
        }
    }

    /// Freeze births synchronously; every existing or in-flight constructor
    /// receives the same original deadline before any observer can be dropped.
    pub(super) fn close(&self, deadline: Instant) -> io::Result<()> {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut unavailable = self.0.is_poisoned();
        let deadline = *state.deadline.get_or_insert(deadline);
        for attempt in &state.attempts {
            let mut state = attempt
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.unavailable |= attempt.state.is_poisoned();
            state.deadline.get_or_insert(deadline);
            if state.retirement.is_none()
                && let Some(handle) = &state.handle
            {
                match handle.begin_retirement(deadline) {
                    Ok(retirement) => state.retirement = Some(retirement),
                    Err(_) => state.unavailable = true,
                }
            }
            // A constructing entry starts unavailable until it either installs
            // its owners or returns an error. That is pending, not a failed
            // terminal fact. Poison still refuses admission and success, but
            // must not prevent the other known owners from receiving shutdown.
            unavailable |=
                attempt.state.is_poisoned() || (!state.constructing && state.unavailable);
        }
        if unavailable {
            Err(io::Error::other("login custody unavailable"))
        } else {
            Ok(())
        }
    }

    pub(super) async fn wait(&self) -> io::Result<Vec<Option<LoginRetirementReport>>> {
        let (deadline, attempts, unavailable) = {
            let state = self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (
                state
                    .deadline
                    .ok_or_else(|| io::Error::other("login admission still open"))?,
                state.attempts.clone(),
                self.0.is_poisoned(),
            )
        };
        let reports = futures::future::join_all(attempts.into_iter().map(|attempt| async move {
            let mut changed = attempt.changed.subscribe();
            loop {
                let (constructing, retirement, unavailable) = {
                    let state = attempt.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                    (state.constructing, state.retirement.clone(), state.unavailable || attempt.state.is_poisoned())
                };
                if !constructing {
                    let report = match retirement {
                        Some(retirement) => Some(retirement.wait().await),
                        None => None, // Constructor returned no server/resources.
                    };
                    // Failure cannot suppress observation of any retained
                    // joins, including those attached to this failed entry.
                    return if unavailable {
                        Err(io::Error::other("login retirement unavailable"))
                    } else {
                        Ok(report)
                    };
                }
                if Instant::now() >= deadline {
                    return Err(io::Error::other("login construction not observed within deadline"));
                }
                tokio::select! {
                    biased;
                    _ = tokio::time::sleep_until(deadline) => return Err(io::Error::other("login construction not observed within deadline")),
                    _ = changed.changed() => {},
                }
            }
        })).await;
        if unavailable {
            Err(io::Error::other("login custody unavailable"))
        } else {
            reports.into_iter().collect()
        }
    }
}

#[cfg(test)]
#[path = "browser_logins_tests.rs"]
mod tests;
