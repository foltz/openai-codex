use super::MetricsError;
use super::Result;
use super::client::MetricsClientInner;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::RwLock;
use std::sync::RwLockWriteGuard;
use std::sync::Weak;
use tokio::sync::watch;

#[derive(Debug)]
struct Epoch {
    inner: Arc<MetricsClientInner>,
    operations: Arc<watch::Sender<usize>>,
}

struct Lease(Arc<Epoch>);

/// Legacy publication releases its route custody outside publication locks.
/// Admitted operations retain their own epoch leases; this is not a shutdown
/// acknowledgement and does not replace a managed drain ticket.
#[must_use = "drop outside every publication lock"]
pub(super) struct LegacyEpochRelease {
    _epoch: Option<Arc<Epoch>>,
}

/// Owns observation of precisely the epoch retired by one replacement.
/// Dropping a wait future does not drop this ticket or change its population.
#[derive(Clone, Debug)]
#[must_use = "retired operations must drain before their exporter is stopped"]
pub(super) struct MetricsDrainTicket {
    operations: Arc<watch::Sender<usize>>,
    retired: Weak<Mutex<Vec<Arc<Epoch>>>>,
}

impl MetricsDrainTicket {
    pub(super) async fn wait(&self) -> Result<()> {
        let mut operations = self.operations.subscribe();
        loop {
            if *operations.borrow_and_update() == 0 {
                // Keep replayable count evidence, not the retired exporter.
                // No operation can enter this epoch after its route was swapped.
                if let Some(retired) = self.retired.upgrade() {
                    let released = {
                        let mut retired = retired
                            .lock()
                            .map_err(|_| MetricsError::RoutingUnavailable)?;
                        let (released, remaining): (Vec<_>, Vec<_>) = retired
                            .drain(..)
                            .partition(|epoch| Arc::ptr_eq(&epoch.operations, &self.operations));
                        *retired = remaining;
                        released
                    };
                    // SDK-owned callbacks/destructors must never run under a
                    // route lock, including when this is the last inner Arc.
                    drop(released);
                }
                return Ok(());
            }
            operations
                .changed()
                .await
                .map_err(|_| MetricsError::RoutingUnavailable)?;
        }
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.0.operations.send_modify(|count| *count -= 1);
    }
}

/// Selection and registration are atomic against replacement; user callbacks
/// execute without the route lock, including callbacks that re-enter metrics.
#[derive(Debug, Default)]
pub(super) struct MetricsRoute {
    current: RwLock<Option<Arc<Epoch>>>,
    retired: Arc<Mutex<Vec<Arc<Epoch>>>>,
}

/// Acquires every fallible route lock before a coordinated publication starts.
/// Keep this guard through all route mutations; release before callbacks or
/// exporter retirement. Dropping it without replacement has no state effect.
pub(super) struct MetricsPublicationGuard<'a> {
    current: RwLockWriteGuard<'a, Option<Arc<Epoch>>>,
    retired: MutexGuard<'a, Vec<Arc<Epoch>>>,
    retired_owner: Weak<Mutex<Vec<Arc<Epoch>>>>,
}

impl MetricsRoute {
    pub(super) fn new(inner: Arc<MetricsClientInner>) -> Self {
        Self {
            current: RwLock::new(Some(Arc::new(Epoch {
                inner,
                operations: Arc::new(watch::channel(0).0),
            }))),
            retired: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub(super) fn with_inner<T>(
        &self,
        operation: impl FnOnce(&MetricsClientInner) -> Result<T>,
    ) -> Result<T> {
        let lease = {
            let epoch = self
                .current
                .read()
                .map_err(|_| MetricsError::RoutingUnavailable)?;
            let epoch = epoch.as_ref().ok_or(MetricsError::ExporterDisabled)?;
            let mut admitted = false;
            epoch.operations.send_modify(|count| {
                if let Some(next) = count.checked_add(1) {
                    *count = next;
                    admitted = true;
                }
            });
            if !admitted {
                return Err(MetricsError::RoutingUnavailable);
            }
            Lease(Arc::clone(epoch))
        };
        operation(&lease.0.inner)
    }

    pub(super) fn replace(&self, inner: Arc<MetricsClientInner>) -> Result<MetricsDrainTicket> {
        Ok(self.publication_guard()?.replace(Some(inner)))
    }

    /// Close new operation admission without abandoning already-selected work.
    pub(super) fn disable(&self) -> Result<MetricsDrainTicket> {
        Ok(self.publication_guard()?.replace(None))
    }

    pub(super) fn replace_legacy(
        &self,
        inner: Option<Arc<MetricsClientInner>>,
    ) -> Result<(MetricsDrainTicket, LegacyEpochRelease)> {
        let mut current = self
            .current
            .write()
            .map_err(|_| MetricsError::RoutingUnavailable)?;
        let next = inner.map(|inner| {
            Arc::new(Epoch {
                inner,
                operations: Arc::new(watch::channel(0).0),
            })
        });
        let epoch = std::mem::replace(&mut *current, next);
        let operations = epoch
            .as_ref()
            .map(|epoch| Arc::clone(&epoch.operations))
            .unwrap_or_else(|| Arc::new(watch::channel(0).0));
        Ok((
            MetricsDrainTicket {
                operations,
                retired: Weak::new(),
            },
            LegacyEpochRelease { _epoch: epoch },
        ))
    }

    pub(super) fn publication_guard(&self) -> Result<MetricsPublicationGuard<'_>> {
        let current = self
            .current
            .write()
            .map_err(|_| MetricsError::RoutingUnavailable)?;
        let retired = self
            .retired
            .lock()
            .map_err(|_| MetricsError::RoutingUnavailable)?;
        Ok(MetricsPublicationGuard {
            current,
            retired,
            retired_owner: Arc::downgrade(&self.retired),
        })
    }
}

impl MetricsPublicationGuard<'_> {
    pub(super) fn replace(&mut self, inner: Option<Arc<MetricsClientInner>>) -> MetricsDrainTicket {
        let next = inner.map(|inner| {
            Arc::new(Epoch {
                inner,
                operations: Arc::new(watch::channel(0).0),
            })
        });
        // Do not compact here: callers can hold the global publication lock.
        // Only the exact ticket's drain releases old SDK references, outside
        // publication. Each retired owner must retain and observe its ticket.
        let Some(epoch) = std::mem::replace(&mut *self.current, next) else {
            // No active generation existed. This receipt certifies the empty
            // population only, not any separately retained failed retirement.
            return MetricsDrainTicket {
                operations: Arc::new(watch::channel(0).0),
                retired: Weak::new(),
            };
        };
        let ticket = MetricsDrainTicket {
            operations: Arc::clone(&epoch.operations),
            retired: self.retired_owner.clone(),
        };
        self.retired.push(epoch);
        ticket
    }
}

#[cfg(test)]
#[path = "route_tests.rs"]
mod tests;
