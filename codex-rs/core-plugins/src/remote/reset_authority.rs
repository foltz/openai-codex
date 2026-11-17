//! Same-home publication retirement, separate from cache reconciliation and
//! actual task joins. Observer drop never forgets admitted physical commits.

use crate::store::PLUGINS_CACHE_DIR;
use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio::time::timeout_at;

type Authority = Arc<Mutex<ResetAuthority>>;
static AUTHORITIES: OnceLock<Mutex<HashMap<PathBuf, Authority>>> = OnceLock::new();

struct ResetAuthority {
    epoch: Arc<()>,
    active: watch::Sender<usize>,
    closed: bool,
    failed: bool,
}

#[derive(Clone)]
pub(crate) struct RemotePluginBundleSyncGeneration {
    authority: Authority,
    epoch: Arc<()>,
}

/// This lease must be moved into the actual blocking writer, not retained by
/// only its async observer. It proves physical completion, not a clean join.
pub(crate) struct RemotePluginBundleCommitLease {
    authority: Authority,
}

pub(crate) struct RemotePluginRetirement {
    authority: Authority,
    epoch: Arc<()>,
    active: watch::Receiver<usize>,
}

fn unavailable() -> io::Error {
    io::Error::other("remote plugin publication authority unavailable")
}

fn identity(home: &Path) -> io::Result<PathBuf> {
    let cache = home.join(PLUGINS_CACHE_DIR);
    let mut ancestor = if cache.is_absolute() {
        cache
    } else {
        std::env::current_dir()
            .map_err(|_| unavailable())?
            .join(cache)
    };
    let mut missing = Vec::new();
    let mut canonical = loop {
        match std::fs::canonicalize(&ancestor) {
            Ok(path) => break path,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                missing.push(ancestor.file_name().ok_or_else(unavailable)?.to_owned());
                if !ancestor.pop() {
                    return Err(unavailable());
                }
            }
            Err(_) => return Err(unavailable()),
        }
    };
    for part in missing.into_iter().rev() {
        canonical.push(part);
    }
    Ok(canonical)
}

fn authority(home: &Path) -> io::Result<Authority> {
    let key = identity(home)?;
    let mut all = AUTHORITIES
        .get_or_init(Mutex::default)
        .lock()
        .map_err(|_| unavailable())?;
    // Keep failed/closed custody even when the last manager/observer is dropped.
    Ok(Arc::clone(all.entry(key).or_insert_with(|| {
        Arc::new(Mutex::new(ResetAuthority {
            epoch: Arc::new(()),
            active: watch::channel(0).0,
            closed: false,
            failed: false,
        }))
    })))
}

impl RemotePluginBundleSyncGeneration {
    /// Capture at operation admission, before auth/download/queue awaits.
    pub(crate) fn capture(home: &Path) -> io::Result<Self> {
        let authority = authority(home)?;
        let state = authority.lock().map_err(|_| unavailable())?;
        if state.closed || state.failed {
            return Err(unavailable());
        }
        Ok(Self {
            authority: authority.clone(),
            epoch: state.epoch.clone(),
        })
    }

    pub(crate) fn begin_commit(&self) -> io::Result<RemotePluginBundleCommitLease> {
        let mut state = self.authority.lock().map_err(|_| unavailable())?;
        if state.closed || state.failed || !Arc::ptr_eq(&self.epoch, &state.epoch) {
            return Err(unavailable());
        }
        let count = *state.active.borrow();
        let Some(next) = count.checked_add(1) else {
            state.failed = true;
            state.closed = true;
            return Err(unavailable());
        };
        state.active.send_replace(next);
        Ok(RemotePluginBundleCommitLease {
            authority: self.authority.clone(),
        })
    }
}

impl RemotePluginBundleCommitLease {
    /// Keep physical custody in the queued/running closure, then transfer it
    /// with the result for subsequent synchronous publication or rollback.
    /// Dropping the async observer cannot release a running writer's lease.
    pub(crate) async fn run_blocking<T, F>(self, write: F) -> io::Result<(T, Self)>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        tokio::task::spawn_blocking(move || {
            let result = write();
            (result, self)
        })
        .await
        .map_err(|_| unavailable())
    }
}

impl Drop for RemotePluginBundleCommitLease {
    fn drop(&mut self) {
        let mut state = match self.authority.lock() {
            Ok(state) => state,
            Err(error) => {
                let mut state = error.into_inner();
                state.failed = true;
                state.closed = true;
                state
            }
        };
        let count = *state.active.borrow();
        if let Some(next) = count.checked_sub(1) {
            state.active.send_replace(next);
        } else {
            state.failed = true;
            state.closed = true;
        }
    }
}

pub(crate) fn retire_remote_plugin_bundle_sync(home: &Path) -> io::Result<RemotePluginRetirement> {
    let authority = authority(home)?;
    let mut state = authority.lock().map_err(|_| unavailable())?;
    state.closed = true;
    if state.failed {
        return Err(unavailable());
    }
    state.epoch = Arc::new(());
    Ok(RemotePluginRetirement {
        authority: authority.clone(),
        epoch: state.epoch.clone(),
        active: state.active.subscribe(),
    })
}

impl RemotePluginRetirement {
    /// A cancelled/timed-out observer leaves admission closed. Retry advances
    /// the epoch but observes the same physical count, including older commits.
    pub(crate) async fn wait_until(mut self, deadline: Instant) -> io::Result<()> {
        loop {
            drop(self.active.borrow_and_update());
            {
                let mut state = self.authority.lock().map_err(|_| unavailable())?;
                if state.failed || !Arc::ptr_eq(&self.epoch, &state.epoch) {
                    return Err(unavailable());
                }
                if Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "remote plugin publication retirement timed out",
                    ));
                }
                let count = *state.active.borrow();
                if count == 0 {
                    state.closed = false;
                    return Ok(());
                }
            }
            timeout_at(deadline, self.active.changed())
                .await
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::TimedOut,
                        "remote plugin publication retirement timed out",
                    )
                })?
                .map_err(|_| unavailable())?;
        }
    }
}

#[cfg(test)]
#[path = "reset_authority_tests.rs"]
mod tests;
