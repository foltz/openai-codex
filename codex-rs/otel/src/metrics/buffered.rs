//! Buffers bounded operation counts and durations until process metrics are configured.
//! Each pair is admitted together; lifecycle transitions serialize with local SDK recording.
//! Shutting down the active installation resumes buffering until its replacement is ready.
//!
//! Every publication (managed or legacy) allocates a strictly increasing generation while
//! it holds the global metrics lock, and returns a `BufferedReceipt` carrying it. Receipts
//! are applied only after every publication guard is released. The buffer accepts a
//! receipt only if its generation exceeds the bound generation, so a late transition
//! for an older publication can never bind, flush, or disable over a newer one.
//! Lock order: no publication lock (metrics slot, route, trace, tracestate, logger) is
//! ever held while acquiring the buffer lock, and the buffer lock never acquires one;
//! beneath it only a standalone client's SDK-internal instrument locks are taken.

use super::MetricsClient;
use super::MetricsError;
use super::Result;
use super::client::MetricsClientInner;
use super::validation::validate_metric_name;
use super::validation::validate_tag_key;
use super::validation::validate_tag_value;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

const MAX_PENDING_OPERATIONS: usize = 256;
const MAX_METADATA_BYTES: usize = 1024;
const MAX_TAGS: usize = 16;

struct PendingOperation {
    count_name: Box<str>,
    duration_name: Box<str>,
    duration: Duration,
    tags: Vec<(Box<str>, Box<str>)>,
}

enum State {
    Startup(Vec<PendingOperation>),
    Ready(MetricsClient),
    Disabled,
}

struct Binding {
    // Generation of the last accepted receipt; 0 before any publication.
    generation: u64,
    // Highest generation whose provider shut down before its receipt applied.
    // A receipt for such a generation binds as Startup, never to the dead provider.
    suspended: u64,
    state: State,
}

pub(crate) struct BufferedMetrics {
    state: Mutex<Binding>,
}

enum Transition {
    Enable(Arc<MetricsClientInner>),
    Disable,
}

/// Outcome of one publication for buffered operation metrics. It holds no lock
/// and performs no SDK work until applied, after all publication guards drop.
#[must_use = "apply the receipt after releasing every publication guard"]
pub(crate) struct BufferedReceipt {
    generation: u64,
    transition: Transition,
}

impl BufferedReceipt {
    pub(super) fn enable(generation: u64, inner: Arc<MetricsClientInner>) -> Self {
        Self {
            generation,
            transition: Transition::Enable(inner),
        }
    }

    pub(super) fn disable(generation: u64) -> Self {
        Self {
            generation,
            transition: Transition::Disable,
        }
    }

    #[cfg(test)]
    pub(super) fn generation(&self) -> u64 {
        self.generation
    }

    /// Apply to the process buffer. Call with no publication guard held.
    pub(crate) fn apply(self) {
        GLOBAL.apply(self);
    }
}

pub(crate) static GLOBAL: BufferedMetrics = BufferedMetrics::new();

/// Records one operation count and its duration in milliseconds through `MetricsClient`.
/// While awaiting an active provider, retains at most 256 pairs, each with at most 16 tags and
/// 1024 bytes of metric names, tag keys, and tag values. Overflow drops the entire pair.
/// Supply bounded classifications, never credentials or user-provided identifiers.
/// Disabling metrics discards pending pairs and suppresses subsequent recording.
pub fn record_global_operation(
    count_name: &str,
    duration_name: &str,
    duration: Duration,
    tags: &[(&str, &str)],
) -> Result<()> {
    GLOBAL.record(count_name, duration_name, duration, tags)
}

impl BufferedMetrics {
    const fn new() -> Self {
        Self {
            state: Mutex::new(Binding {
                generation: 0,
                suspended: 0,
                state: State::Startup(Vec::new()),
            }),
        }
    }

    fn record(
        &self,
        count_name: &str,
        duration_name: &str,
        duration: Duration,
        tags: &[(&str, &str)],
    ) -> Result<()> {
        if tags.len() > MAX_TAGS
            || tags.iter().fold(
                count_name.len().saturating_add(duration_name.len()),
                |bytes, (key, value)| bytes.saturating_add(key.len()).saturating_add(value.len()),
            ) > MAX_METADATA_BYTES
        {
            return Err(MetricsError::OperationMetadataTooLarge);
        }
        // Validate both instruments before accepting either half of the pair.
        for name in [count_name, duration_name] {
            validate_metric_name(name)?;
            // The SDK otherwise silently builds a no-op instrument for these names.
            if name.len() > 255 || !name.as_bytes()[0].is_ascii_alphabetic() {
                return Err(MetricsError::InvalidMetricName {
                    name: name.to_owned(),
                });
            }
        }
        for (key, value) in tags {
            validate_tag_key(key)?;
            validate_tag_value(value)?;
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &mut state.state {
            State::Startup(pending) => {
                if pending.len() < MAX_PENDING_OPERATIONS {
                    pending.push(PendingOperation {
                        count_name: count_name.into(),
                        duration_name: duration_name.into(),
                        duration,
                        tags: tags
                            .iter()
                            .map(|(key, value)| ((*key).into(), (*value).into()))
                            .collect(),
                    });
                }
                Ok(())
            }
            State::Ready(metrics) => record(metrics, count_name, duration_name, duration, tags),
            State::Disabled => Ok(()),
        }
    }

    /// Accept the receipt only if it is newer than the bound generation. The
    /// winning enable flushes pending pairs exactly once; a stale receipt
    /// changes nothing and is released after the buffer lock drops.
    pub(super) fn apply(&self, receipt: BufferedReceipt) {
        #[cfg(test)]
        assert_eq!(
            super::publication_guards_held(),
            0,
            "buffered transitions must run outside every publication guard"
        );
        let BufferedReceipt {
            generation,
            transition,
        } = receipt;
        let mut binding = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if generation <= binding.generation {
            drop(binding);
            drop(transition);
            return;
        }
        binding.generation = generation;
        let mut shut_down = None;
        let next = match transition {
            Transition::Enable(inner) if generation <= binding.suspended => {
                // Its provider already shut down: keep buffering for a successor.
                shut_down = Some(inner);
                match &mut binding.state {
                    State::Startup(pending) => State::Startup(std::mem::take(pending)),
                    State::Ready(_) | State::Disabled => State::Startup(Vec::new()),
                }
            }
            Transition::Enable(inner) => {
                // Bind to this installation so a provider replacement cannot split a
                // pair between exporters through the redirectable global handle.
                let metrics = MetricsClient {
                    inner: super::client::MetricsOriginal::Standalone(inner),
                    active: None,
                };
                if let State::Startup(pending) = &mut binding.state {
                    for operation in pending.drain(..) {
                        let tags: Vec<_> = operation
                            .tags
                            .iter()
                            .map(|(key, value)| (key.as_ref(), value.as_ref()))
                            .collect();
                        let _ = record(
                            &metrics,
                            &operation.count_name,
                            &operation.duration_name,
                            operation.duration,
                            &tags,
                        );
                    }
                }
                State::Ready(metrics)
            }
            Transition::Disable => State::Disabled,
        };
        let previous = std::mem::replace(&mut binding.state, next);
        drop(binding);
        // A replaced binding may hold the last provider reference.
        drop(previous);
        drop(shut_down);
    }

    /// A provider published as `generation` is shutting down. Only that exact
    /// bound generation returns to buffering; the bound generation is never
    /// raised, so any already-issued receipt with a higher generation still
    /// applies afterwards. A suspend for an older generation, or after
    /// opt-out, has no effect.
    pub(super) fn suspend(&self, generation: u64) {
        let mut binding = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if generation > binding.generation {
            // Shut down before its own receipt applied: that receipt must not
            // bind a dead provider. Newer receipts are unaffected.
            binding.suspended = binding.suspended.max(generation);
            return;
        }
        if generation == binding.generation && matches!(binding.state, State::Ready(_)) {
            let previous = std::mem::replace(&mut binding.state, State::Startup(Vec::new()));
            drop(binding);
            drop(previous);
        }
    }

    #[cfg(test)]
    fn enable(&self, metrics: &MetricsClient) {
        let Ok(inner) = metrics.original_inner() else {
            return;
        };
        self.apply(BufferedReceipt::enable(
            super::next_publication_generation(),
            inner,
        ));
    }

    #[cfg(test)]
    fn disable(&self) {
        self.apply(BufferedReceipt::disable(
            super::next_publication_generation(),
        ));
    }
}

fn record(
    metrics: &MetricsClient,
    count_name: &str,
    duration_name: &str,
    duration: Duration,
    tags: &[(&str, &str)],
) -> Result<()> {
    metrics.counter(count_name, /*inc*/ 1, tags)?;
    metrics.record_duration(duration_name, duration, tags)
}

#[cfg(test)]
#[path = "buffered_tests.rs"]
mod tests;
