//! Process-local, canonical-home custody for managed cloud-config reset.
//! Publication tasks retain their lease on a live runtime even if their caller
//! disappears. This is not a process-exit or cross-process write guarantee.

use crate::service::CLOUD_CONFIG_BUNDLE_TIMEOUT;
use codex_config::CloudConfigBundle;
use codex_config::CloudConfigBundleLoadError;
use codex_config::CloudConfigBundleLoadErrorCode;
use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

#[derive(Default)]
pub(crate) struct HomeOwners {
    pub(crate) tasks: Vec<JoinHandle<()>>,
    pub(crate) retiring: bool,
    pub(crate) failed_owner: bool,
    active_getters: usize,
}

impl HomeOwners {
    pub(crate) fn reap_finished(&mut self) {
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        self.tasks.retain_mut(|task| {
            if !task.is_finished() {
                return true;
            }
            match std::pin::Pin::new(task).poll(&mut context) {
                std::task::Poll::Ready(Ok(())) => false,
                std::task::Poll::Ready(Err(error)) => {
                    // Lifetime drop, global replacement and reset all cancel
                    // refreshers. A panic, unlike cancellation, fails reset.
                    self.failed_owner |= !error.is_cancelled();
                    false
                }
                std::task::Poll::Pending => true,
            }
        });
    }
}

#[derive(Default)]
pub(crate) struct HomeLifecycle {
    pub(crate) owners: Mutex<HomeOwners>,
    pub(crate) publication: Arc<tokio::sync::Mutex<()>>,
    pub(crate) generation: Arc<AtomicU64>,
    pub(crate) reset: tokio::sync::Mutex<()>,
    retired: Notify,
}

struct GetterGuard<'a>(&'a HomeLifecycle);

impl Drop for GetterGuard<'_> {
    fn drop(&mut self) {
        let mut owners = self
            .0
            .owners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        owners.active_getters -= 1;
    }
}

impl HomeLifecycle {
    /// Admission happens when this future is first polled, never when a caller
    /// merely constructs a getter. The service future remains caller-polled.
    pub(crate) async fn load(
        &self,
        expected_generation: u64,
        work: impl Future<Output = Result<Option<CloudConfigBundle>, CloudConfigBundleLoadError>>,
    ) -> Result<Option<CloudConfigBundle>, CloudConfigBundleLoadError> {
        let retired = self.retired.notified();
        tokio::pin!(retired);
        retired.as_mut().enable();
        let _getter = {
            let mut owners = self
                .owners
                .lock()
                .map_err(|_| lifecycle_error("cloud owner lock unavailable"))?;
            if owners.retiring || self.generation.load(Ordering::Acquire) != expected_generation {
                return Err(lifecycle_error("cloud loader generation retired"));
            }
            owners.active_getters = owners
                .active_getters
                .checked_add(1)
                .ok_or_else(|| lifecycle_error("cloud getter count exhausted"))?;
            GetterGuard(self)
        };
        // Both branch futures are dropped before the admission guard. In
        // particular, reset cannot observe a released getter with live I/O.
        let result = tokio::select! {
            biased;
            () = &mut retired => Err(lifecycle_error("cloud loader generation retired")),
            result = work => result,
        };
        let owners = self
            .owners
            .lock()
            .map_err(|_| lifecycle_error("cloud owner lock unavailable"))?;
        if owners.retiring || self.generation.load(Ordering::Acquire) != expected_generation {
            return Err(lifecycle_error("cloud loader generation retired"));
        }
        result
    }

    /// Caller holds `reset` through this drain and fresh-loader installation.
    /// Failure leaves admission closed; a later managed retry can finish it.
    pub(crate) async fn retire_owners(&self) -> Result<(), CloudConfigBundleLoadError> {
        {
            let mut owners = self
                .owners
                .lock()
                .map_err(|_| lifecycle_error("cloud owner lock unavailable"))?;
            owners.retiring = true;
            self.generation
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                    value.checked_add(1)
                })
                .map_err(|_| lifecycle_error("cloud generation exhausted"))?;
            self.retired.notify_waiters();
            for task in &owners.tasks {
                task.abort();
            }
        }
        tokio::time::timeout(CLOUD_CONFIG_BUNDLE_TIMEOUT, async {
            loop {
                let done = {
                    let mut owners = self
                        .owners
                        .lock()
                        .map_err(|_| lifecycle_error("cloud owner lock unavailable"))?;
                    owners.reap_finished();
                    owners.tasks.is_empty() && owners.active_getters == 0
                };
                if done {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            // Generation was advanced first: queued old writers refuse, and
            // any writer already inside this lease must finish before ack.
            let _publication = self.publication.lock().await;
            Ok::<(), CloudConfigBundleLoadError>(())
        })
        .await
        .map_err(|_| lifecycle_error("cloud retirement timed out"))??;
        let mut owners = self
            .owners
            .lock()
            .map_err(|_| lifecycle_error("cloud owner lock unavailable"))?;
        if std::mem::take(&mut owners.failed_owner) {
            return Err(lifecycle_error(
                "cloud prior owner failed during retirement",
            ));
        }
        Ok(())
    }
}

pub(crate) fn lifecycle_error(message: &'static str) -> CloudConfigBundleLoadError {
    CloudConfigBundleLoadError::new(
        CloudConfigBundleLoadErrorCode::Internal,
        /*status_code*/ None,
        message,
    )
}

pub(crate) fn home_lifecycle(
    home: &Path,
) -> Result<Arc<HomeLifecycle>, CloudConfigBundleLoadError> {
    let home = codex_config::AbsolutePathBuf::resolve_path_against_base(home, "/");
    // Resolve the existing ancestor too: two aliases of an uncreated home
    // must not mint independent write fences.
    let mut ancestor = home.to_path_buf();
    let mut suffix = Vec::new();
    let canonical = loop {
        match std::fs::canonicalize(&ancestor) {
            Ok(path) => break path,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                suffix.push(
                    ancestor
                        .file_name()
                        .ok_or_else(|| lifecycle_error("cloud home identity unavailable"))?
                        .to_owned(),
                );
                if !ancestor.pop() {
                    return Err(lifecycle_error("cloud home identity unavailable"));
                }
            }
            Err(_) => return Err(lifecycle_error("cloud home identity unavailable")),
        }
    };
    let mut identity = canonical;
    for part in suffix.into_iter().rev() {
        identity.push(part);
    }
    static HOMES: OnceLock<Mutex<HashMap<PathBuf, Arc<HomeLifecycle>>>> = OnceLock::new();
    let mut homes = HOMES
        .get_or_init(Mutex::default)
        .lock()
        .map_err(|_| lifecycle_error("cloud home registry unavailable"))?;
    Ok(Arc::clone(homes.entry(identity).or_default()))
}

#[cfg(test)]
#[path = "home_lifecycle_tests.rs"]
mod tests;
