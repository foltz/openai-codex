//! Owner-side cleanup custody for sandbox launches.
//!
//! Every process that spawns the Linux sandbox launcher (core exec, unified
//! exec, exec-server and its filesystem helper, app-server `command/exec`,
//! the debug-sandbox CLI) is the launch's owner and must hold its cleanup
//! custody: create a [`SandboxCleanupCustody`] for the launch, attach it to the
//! launcher argv, let the spawn inherit its descriptors, tighten its deadline
//! when the owner terminates or retires the launch, and finally collect the
//! structured [`SandboxCleanupSummary`] in its own process.
//!
//! The cleanup deadline is separate from any command timeout. It starts as
//! the owner's original deadline (or none) and only ever moves earlier:
//! directly through [`SandboxCleanupTightener`], or through a
//! [`CleanupDeadlineScope`] (session or thread retirement) that every live
//! custody of that scope is registered with. Cancellation never extends it.
//!
//! Off Linux, and for launches that are not the Linux sandbox launcher, no
//! custody exists ([`SandboxCleanupCustody::for_launch`] returns `None`).

use std::io;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::time::Duration;
use std::time::Instant;

pub use codex_protocol::exec_output::SandboxCleanupState;
pub use codex_protocol::exec_output::SandboxCleanupSummary;

use crate::SandboxType;

/// Something whose cleanup deadline can be tightened.
pub trait CleanupDeadlineSink: Send + Sync + std::fmt::Debug {
    fn tighten(&self, deadline: Instant);
}

#[derive(Debug, Default)]
struct ScopeState {
    latched: Option<Instant>,
    sinks: Vec<Weak<dyn CleanupDeadlineSink>>,
}

/// A retirement scope (a session, or a thread through its session): its
/// deadline only tightens, and every custody registered with it, now or
/// later, is tightened to it.
#[derive(Clone, Debug, Default)]
pub struct CleanupDeadlineScope {
    state: Arc<Mutex<ScopeState>>,
}

impl CleanupDeadlineScope {
    pub fn new() -> Self {
        Self::default()
    }

    /// Latches `deadline` if it is earlier and tightens every live custody.
    pub fn tighten(&self, deadline: Instant) {
        let (deadline, sinks) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let deadline = state
                .latched
                .map_or(deadline, |latched| latched.min(deadline));
            state.latched = Some(deadline);
            state.sinks.retain(|sink| sink.strong_count() > 0);
            let sinks: Vec<_> = state.sinks.iter().filter_map(Weak::upgrade).collect();
            (deadline, sinks)
        };
        for sink in sinks {
            sink.tighten(deadline);
        }
    }

    pub fn latched(&self) -> Option<Instant> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .latched
    }

    /// Registers a custody; an already latched deadline applies at once.
    pub fn register(&self, sink: &Arc<dyn CleanupDeadlineSink>) {
        let latched = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.sinks.retain(|sink| sink.strong_count() > 0);
            state.sinks.push(Arc::downgrade(sink));
            state.latched
        };
        if let Some(latched) = latched {
            sink.tighten(latched);
        }
    }
}

/// Tightens one launch's cleanup deadline; cloneable into terminate paths.
#[derive(Clone, Debug)]
pub struct SandboxCleanupTightener {
    sink: Arc<dyn CleanupDeadlineSink>,
}

impl SandboxCleanupTightener {
    pub fn tighten(&self, deadline: Instant) {
        self.sink.tighten(deadline);
    }
}

/// The owner's custody of one Linux sandbox launch.
#[derive(Debug)]
pub struct SandboxCleanupCustody {
    #[cfg(target_os = "linux")]
    inner: crate::linux_cleanup_custody::LinuxSandboxCleanupCustody,
    #[cfg(target_os = "linux")]
    sink: Arc<dyn CleanupDeadlineSink>,
}

impl SandboxCleanupCustody {
    /// Custody for a launch through the Linux sandbox launcher; `None` for
    /// any other launch. `cleanup_deadline` is the owner's original cleanup
    /// deadline (independent of the command timeout); `scope` is the
    /// retirement scope whose tightenings must reach this launch.
    #[cfg_attr(not(target_os = "linux"), allow(unused_variables))]
    pub fn for_launch(
        sandbox: SandboxType,
        cleanup_deadline: Option<Instant>,
        scope: Option<&CleanupDeadlineScope>,
    ) -> io::Result<Option<Self>> {
        #[cfg(target_os = "linux")]
        if sandbox == SandboxType::LinuxSeccomp {
            let inner =
                crate::linux_cleanup_custody::LinuxSandboxCleanupCustody::new(cleanup_deadline)?;
            let sink: Arc<dyn CleanupDeadlineSink> = inner.tightener();
            if let Some(scope) = scope {
                scope.register(&sink);
            }
            return Ok(Some(Self { inner, sink }));
        }
        Ok(None)
    }

    /// Inserts the custody arguments before the launcher argv's `--`.
    #[cfg_attr(not(target_os = "linux"), allow(unused_variables))]
    pub fn attach_to_launcher_argv(&self, argv: &mut Vec<String>) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            self.inner.attach_to_launcher_argv(argv)
        }
        #[cfg(not(target_os = "linux"))]
        {
            Ok(())
        }
    }

    /// Descriptors the spawn must let the launcher inherit.
    pub fn inherited_fds(&self) -> Vec<i32> {
        #[cfg(target_os = "linux")]
        {
            self.inner.inherited_fds()
        }
        #[cfg(not(target_os = "linux"))]
        {
            Vec::new()
        }
    }

    /// Call right after the spawn returns: `Some(pid)` of the launcher (the
    /// owner's own child, not yet reaped) on success, `None` if the spawn
    /// failed. The join of the launch requires the launcher's exit, observed
    /// through a pidfd taken here.
    #[cfg_attr(not(target_os = "linux"), allow(unused_variables))]
    pub fn spawned(&mut self, launcher_pid: Option<u32>) {
        #[cfg(target_os = "linux")]
        self.inner.spawned(launcher_pid);
    }

    /// Like [`Self::spawned`], with the launcher already pinned by the spawn
    /// itself (`codex_utils_pty` opens a pidfd before its wait task can reap
    /// the child), so a fast-exiting launcher cannot race the pin.
    pub fn spawned_with_pin(&mut self, launcher: Option<codex_utils_pty::ProcessPin>) {
        #[cfg(target_os = "linux")]
        match launcher {
            Some(pin) => self.inner.spawned_pinned(pin.into_pidfd()),
            None => self.inner.spawned(None),
        }
        #[cfg(not(target_os = "linux"))]
        drop(launcher);
    }

    pub fn tightener(&self) -> SandboxCleanupTightener {
        #[cfg(target_os = "linux")]
        {
            SandboxCleanupTightener {
                sink: Arc::clone(&self.sink),
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            unreachable!("no cleanup custody exists off Linux")
        }
    }

    pub fn tighten(&self, deadline: Instant) {
        #[cfg(target_os = "linux")]
        self.sink.tighten(deadline);
        #[cfg(not(target_os = "linux"))]
        let _ = deadline;
    }

    pub fn deadline(&self) -> Option<Instant> {
        #[cfg(target_os = "linux")]
        {
            self.inner.deadline()
        }
        #[cfg(not(target_os = "linux"))]
        {
            None
        }
    }

    /// Collects the launch report in the owner's process until every cleanup
    /// helper is proven exited (launcher and reaper pidfds) or the cleanup
    /// deadline passes. A launch that still has no deadline is retired now:
    /// the owner grants
    /// `retire_grace`. Blocking.
    #[cfg_attr(not(target_os = "linux"), allow(unused_variables))]
    pub fn collect_blocking(self, retire_grace: Duration) -> SandboxCleanupSummary {
        #[cfg(target_os = "linux")]
        {
            if self.inner.deadline().is_none() {
                self.sink.tighten(Instant::now() + retire_grace);
            }
            self.inner.collect_blocking().summary()
        }
        #[cfg(not(target_os = "linux"))]
        {
            unreachable!("no cleanup custody exists off Linux")
        }
    }

    /// [`Self::collect_blocking`] on a blocking thread.
    pub async fn collect(self, retire_grace: Duration) -> SandboxCleanupSummary {
        tokio::task::spawn_blocking(move || self.collect_blocking(retire_grace))
            .await
            .unwrap_or(SandboxCleanupSummary {
                state: SandboxCleanupState::Unreported,
                reaped: 0,
                preserved_unproven: 0,
                host_owned: 0,
                handed_off: 0,
                timed_out: 0,
                joined: false,
            })
    }
}

#[cfg(test)]
#[path = "cleanup_custody_tests.rs"]
mod tests;
