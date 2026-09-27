//! Reset-only authority for remote plugin publication, independent of cache
//! reconciliation generations and network scheduling. A lease owns an admitted
//! mutation through completion; dropping a waiting observer cannot erase it.

use crate::store::PLUGINS_CACHE_DIR;
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::Weak;
use tokio::sync::watch;

static AUTHORITIES: OnceLock<Mutex<HashMap<PathBuf, Weak<Mutex<ResetAuthority>>>>> =
    OnceLock::new();

struct ResetAuthority {
    epoch: Arc<()>,
    active_commits: watch::Sender<usize>,
}

/// Captured when work is admitted, never recaptured by its continuation or retry.
/// Normal cache generation changes do not invalidate this reset epoch.
#[derive(Clone)]
pub(crate) struct RemotePluginBundleSyncGeneration {
    authority: Arc<Mutex<ResetAuthority>>,
    epoch: Arc<()>,
}

/// Retains the authority as well as the count, including if the originating
/// manager or download future is dropped while a blocking commit is running.
pub(crate) struct RemotePluginBundleCommitLease {
    authority: Arc<Mutex<ResetAuthority>>,
}

impl RemotePluginBundleSyncGeneration {
    pub(crate) fn capture(codex_home: &Path) -> Self {
        let authority = authority(codex_home);
        let state = authority
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Self {
            authority: Arc::clone(&authority),
            epoch: Arc::clone(&state.epoch),
        }
    }

    pub(crate) fn begin_commit(&self) -> Option<RemotePluginBundleCommitLease> {
        let state = self
            .authority
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !Arc::ptr_eq(&self.epoch, &state.epoch) {
            return None;
        }
        state.active_commits.send_modify(|active| *active += 1);
        Some(RemotePluginBundleCommitLease {
            authority: Arc::clone(&self.authority),
        })
    }
}

impl Drop for RemotePluginBundleCommitLease {
    fn drop(&mut self) {
        let state = self
            .authority
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active_commits.send_modify(|active| *active -= 1);
    }
}

fn authority(codex_home: &Path) -> Arc<Mutex<ResetAuthority>> {
    let authorities = AUTHORITIES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut authorities = authorities
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let key = codex_home.join(PLUGINS_CACHE_DIR);
    if let Some(authority) = authorities.get(&key).and_then(Weak::upgrade) {
        return authority;
    }
    let authority = Arc::new(Mutex::new(ResetAuthority {
        epoch: Arc::new(()),
        active_commits: watch::channel(0).0,
    }));
    authorities.insert(key, Arc::downgrade(&authority));
    authority
}

/// Refuses old admissions immediately. The counter spans epochs, so retrying a
/// timed-out reset cannot forget a commit admitted before the first reset.
pub(crate) fn retire_remote_plugin_bundle_sync(codex_home: &Path) -> watch::Receiver<usize> {
    let authority = authority(codex_home);
    let mut state = authority
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    state.epoch = Arc::new(());
    state.active_commits.subscribe()
}

#[cfg(test)]
#[path = "reset_authority_tests.rs"]
mod tests;
