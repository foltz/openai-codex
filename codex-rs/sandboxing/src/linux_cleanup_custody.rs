//! Owner side of the Linux sandbox launcher's cleanup custody.
//!
//! The process that spawns `codex-linux-sandbox` owns the launch's lifetime.
//! With [`LinuxSandboxCleanupCustody`] it hands the launcher its original
//! cleanup deadline (absolute `CLOCK_MONOTONIC` nanoseconds: the clock behind
//! `std::time::Instant` on Linux), keeps a channel to tighten that deadline
//! later, and receives the launch's final cleanup receipt. The join is proven
//! only by terminal evidence of every cleanup helper: the launcher's pidfd
//! (the owner's own child) and the pidfd of every reaper a record names
//! (identified by boot, pid and start time) must report exit, or the launcher
//! must have declared before the command started that it created no reaper.
//! EOF on the report pipe only proves that its descriptors closed and is never
//! taken as evidence of anything else.
//!
//! The owner never waits past its own deadline for the report: an unfinished
//! observation is `TimedOut`, and that result is never rewritten. Detailed
//! receipts (paths, outcomes per target) stay in this module; owners get a
//! [`SandboxCleanupSummary`].

use std::io;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;
use std::os::fd::OwnedFd;
use std::os::fd::RawFd;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use codex_protocol::exec_output::SandboxCleanupState;
use codex_protocol::exec_output::SandboxCleanupSummary;

use crate::cleanup_custody::CleanupDeadlineSink;

pub const CLEANUP_DEADLINE_ARG: &str = "--cleanup-deadline-monotonic-ns";
pub const CLEANUP_DEADLINE_FD_ARG: &str = "--cleanup-deadline-fd";
pub const CLEANUP_REPORT_FD_ARG: &str = "--cleanup-report-fd";

/// The full launch receipt, internal to the sandboxing crate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LinuxSandboxCleanupReport {
    pub(crate) state: SandboxCleanupState,
    // Kept for diagnostics and tests; owners only see the summary.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) finisher: Option<String>,
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) reaper_join: Option<String>,
    /// Per-target census: (outcome, path).
    pub(crate) targets: Vec<(String, PathBuf)>,
    /// Whether every cleanup helper of the launch was observed to exit (EOF)
    /// before the owner's deadline.
    pub(crate) joined: bool,
}

impl LinuxSandboxCleanupReport {
    /// The owner-facing summary: state and counts only.
    pub(crate) fn summary(&self) -> SandboxCleanupSummary {
        let mut summary = SandboxCleanupSummary {
            state: self.state,
            reaped: 0,
            preserved_unproven: 0,
            host_owned: 0,
            handed_off: 0,
            timed_out: 0,
            joined: self.joined,
        };
        for (outcome, _) in &self.targets {
            let counter = match outcome.as_str() {
                "reaped" | "absent" => &mut summary.reaped,
                "host-owned" => &mut summary.host_owned,
                "handed-off" => &mut summary.handed_off,
                "timed-out" => &mut summary.timed_out,
                // `pending`, every `incomplete:<reason>`, and anything
                // unknown: preserved, never counted as cleaned.
                _ => &mut summary.preserved_unproven,
            };
            *counter = counter.saturating_add(1);
        }
        summary
    }
}

pub fn monotonic_ns(instant: Instant) -> u64 {
    // SAFETY: zeroed timespec is a valid out-parameter.
    let mut now: libc::timespec = unsafe { std::mem::zeroed() };
    // SAFETY: CLOCK_MONOTONIC is always available on Linux.
    unsafe {
        libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut now);
    }
    let now_ns = (now.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(now.tv_nsec as u64);
    let remaining = instant.saturating_duration_since(Instant::now());
    now_ns.saturating_add(u64::try_from(remaining.as_nanos()).unwrap_or(u64::MAX))
}

/// The tightening end, shareable with deadline scopes (session or thread
/// retirement) for as long as the custody lives.
#[derive(Debug)]
pub(crate) struct LinuxTightener {
    channel: Mutex<std::fs::File>,
    deadline: Mutex<Option<Instant>>,
    /// Self-pipe that wakes a blocked collection when the deadline tightens.
    wake_read: OwnedFd,
    wake_write: Mutex<std::fs::File>,
}

impl LinuxTightener {
    fn deadline(&self) -> Option<Instant> {
        *self
            .deadline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl CleanupDeadlineSink for LinuxTightener {
    /// Only an earlier deadline is applied. An 8-byte pipe write is atomic and
    /// never blocks here; a launcher that has exited makes it fail harmlessly.
    fn tighten(&self, deadline: Instant) {
        let mut current = self
            .deadline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if current.is_some_and(|current| current <= deadline) {
            return;
        }
        *current = Some(deadline);
        let mut channel = self
            .channel
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = channel.write_all(&monotonic_ns(deadline).to_le_bytes());
        drop(channel);
        // Non-blocking: a full wake pipe already guarantees a wake-up.
        let mut wake = self
            .wake_write
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = wake.write(&[1]);
    }
}

/// The owner's end of one launch's cleanup custody.
#[derive(Debug)]
pub(crate) struct LinuxSandboxCleanupCustody {
    tightener: Arc<LinuxTightener>,
    report: OwnedFd,
    child_tighten: Option<OwnedFd>,
    child_report: Option<OwnedFd>,
    launcher: LauncherState,
}

/// What the owner knows about the launcher it spawned.
#[derive(Debug)]
enum LauncherState {
    /// Not spawned (yet, or the spawn failed): nothing to join.
    NotSpawned,
    /// The owner's child, pinned by a pidfd.
    Pinned(OwnedFd),
    /// Spawned but could not be pinned: never joined.
    Unverifiable,
}

fn cloexec_pipe() -> io::Result<(OwnedFd, OwnedFd)> {
    let mut fds = [-1; 2];
    // SAFETY: valid two-element array.
    if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) } < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: pipe2 returned two new owned descriptors.
    Ok(unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) })
}

impl LinuxSandboxCleanupCustody {
    /// `deadline` is the original cleanup deadline; `None` when the owner has
    /// none and will tighten one (termination, retirement, collection).
    pub(crate) fn new(deadline: Option<Instant>) -> io::Result<Self> {
        let (child_tighten, tighten) = cloexec_pipe()?;
        let (report, child_report) = cloexec_pipe()?;
        let (wake_read, wake_write) = cloexec_pipe()?;
        set_nonblocking(&report)?;
        set_nonblocking(&wake_read)?;
        set_nonblocking(&wake_write)?;
        Ok(Self {
            tightener: Arc::new(LinuxTightener {
                channel: Mutex::new(std::fs::File::from(tighten)),
                deadline: Mutex::new(deadline),
                wake_read,
                wake_write: Mutex::new(std::fs::File::from(wake_write)),
            }),
            report,
            child_tighten: Some(child_tighten),
            child_report: Some(child_report),
            launcher: LauncherState::NotSpawned,
        })
    }

    pub(crate) fn tightener(&self) -> Arc<LinuxTightener> {
        Arc::clone(&self.tightener)
    }

    /// Inserts the custody arguments into a launcher argv, before its `--`.
    pub(crate) fn attach_to_launcher_argv(&self, argv: &mut Vec<String>) -> io::Result<()> {
        let (Some(tighten), Some(report)) = (&self.child_tighten, &self.child_report) else {
            return Err(io::Error::other("cleanup custody was already handed over"));
        };
        let separator = argv
            .iter()
            .position(|arg| arg == "--")
            .ok_or_else(|| io::Error::other("launcher argv has no `--`"))?;
        let mut custody = Vec::with_capacity(6);
        if let Some(deadline) = self.tightener.deadline() {
            custody.push(CLEANUP_DEADLINE_ARG.to_string());
            custody.push(monotonic_ns(deadline).to_string());
        }
        custody.push(CLEANUP_DEADLINE_FD_ARG.to_string());
        custody.push(tighten.as_raw_fd().to_string());
        custody.push(CLEANUP_REPORT_FD_ARG.to_string());
        custody.push(report.as_raw_fd().to_string());
        argv.splice(separator..separator, custody);
        Ok(())
    }

    /// Descriptors the spawn must make inheritable (only in the child).
    pub(crate) fn inherited_fds(&self) -> Vec<RawFd> {
        [&self.child_tighten, &self.child_report]
            .into_iter()
            .flatten()
            .map(AsRawFd::as_raw_fd)
            .collect()
    }

    /// Drops this process's copies of the launcher's ends after spawning and
    /// pins the launcher (the owner's child, not yet reaped) with a pidfd.
    /// `None` when the spawn failed: no launcher exists to join.
    pub(crate) fn spawned(&mut self, launcher_pid: Option<u32>) {
        self.child_tighten = None;
        self.child_report = None;
        self.launcher = match launcher_pid {
            Some(pid) => match libc::pid_t::try_from(pid).map(pidfd_open) {
                Ok(Ok(pidfd)) => LauncherState::Pinned(pidfd),
                _ => LauncherState::Unverifiable,
            },
            None => LauncherState::NotSpawned,
        };
    }

    /// [`Self::spawned`] with a pidfd the spawn opened itself.
    /// `None` means the spawn could not pin the launcher: never joined.
    pub(crate) fn spawned_pinned(&mut self, pidfd: Option<OwnedFd>) {
        self.child_tighten = None;
        self.child_report = None;
        self.launcher = match pidfd {
            Some(pidfd) => LauncherState::Pinned(pidfd),
            None => LauncherState::Unverifiable,
        };
    }

    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.tightener.deadline()
    }

    /// Reads the launch report and joins every cleanup helper, never past
    /// the current deadline. The deadline (and any tightening) is checked
    /// before every report read, `/proc` lookup and poll: at or after expiry
    /// nothing more is read or polled and the result is a timeout with custody
    /// retained. A tightening wakes the wait through the self-pipe.
    ///
    /// Joined requires positive terminal evidence for every helper:
    /// - the launcher: its pidfd (the owner's own child) reports exit;
    /// - the reaper: either the launcher's pre-command declaration that no
    ///   reaper exists (`kind=helpers`, `reaper=none`), or every reaper any
    ///   record names (`reaper=<boot>.<pid>.<start>`) pinned and observed to
    ///   exit (pidfd readable, or its pid gone or reused).
    ///
    /// Report EOF is never evidence of anything but a closed descriptor; an
    /// unnamed helper stays unproven. Blocking.
    pub(crate) fn collect_blocking(self) -> LinuxSandboxCleanupReport {
        let expired = || {
            self.tightener
                .deadline()
                .is_none_or(|deadline| Instant::now() >= deadline)
        };
        let mut text = Vec::new();
        let mut parsed_blocks = 0;
        let mut report_open = true;
        let mut launcher_exited = matches!(self.launcher, LauncherState::NotSpawned);
        let mut helpers = Helpers::default();
        let joined = loop {
            if expired() {
                break false;
            }
            if report_open {
                match drain_report(&self.report, &mut text, &expired) {
                    Drain::Open => {}
                    Drain::Closed => report_open = false,
                    Drain::Expired => break false,
                }
            }
            let blocks = complete_blocks(&String::from_utf8_lossy(&text));
            for block in blocks.iter().skip(parsed_blocks) {
                if expired() {
                    break;
                }
                helpers.record(block);
            }
            parsed_blocks = blocks.len();
            if expired() {
                break false;
            }
            if !launcher_exited && let LauncherState::Pinned(launcher) = &self.launcher {
                launcher_exited = poll_readable(launcher.as_raw_fd());
            }
            for (_, state) in &mut helpers.named {
                if expired() {
                    break;
                }
                if let HelperState::Pending(pidfd) = state
                    && poll_readable(pidfd.as_raw_fd())
                {
                    *state = HelperState::Exited;
                }
            }
            if expired() {
                break false;
            }
            if launcher_exited && helpers.all_proven_exited() && !report_open {
                break true;
            }
            let Some(deadline) = self.tightener.deadline() else {
                break false;
            };
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break false;
            }
            let mut fds = vec![self.tightener.wake_read.as_raw_fd()];
            if report_open {
                fds.push(self.report.as_raw_fd());
            }
            if !launcher_exited && let LauncherState::Pinned(launcher) = &self.launcher {
                fds.push(launcher.as_raw_fd());
            }
            for (_, state) in &helpers.named {
                if let HelperState::Pending(pidfd) = state {
                    fds.push(pidfd.as_raw_fd());
                }
            }
            let timeout =
                libc::c_int::try_from(remaining.as_millis().max(1)).unwrap_or(libc::c_int::MAX);
            wait_any(&fds, timeout);
            drain_wake(&self.tightener.wake_read);
        };
        parse_report(&String::from_utf8_lossy(&text), joined)
    }
}

/// Positive evidence about the launch's reaper(s).
#[derive(Debug, Default)]
struct Helpers {
    /// The launcher declared before the command started that it created no
    /// reaper.
    declared_none: bool,
    /// Every reaper any record names, by token.
    named: Vec<(String, HelperState)>,
}

impl Helpers {
    fn record(&mut self, block: &[String]) {
        let declaration = block.iter().any(|line| line == "kind=helpers");
        let Some(token) = block.iter().find_map(|line| line.strip_prefix("reaper=")) else {
            return;
        };
        if token == "none" {
            // Only a pre-command declaration proves that no reaper exists; a
            // final receipt's `none` does not.
            self.declared_none |= declaration;
            return;
        }
        if !self.named.iter().any(|(named, _)| named == token) {
            self.named.push((token.to_string(), resolve_reaper(token)));
        }
    }

    fn all_proven_exited(&self) -> bool {
        if self.named.is_empty() {
            return self.declared_none;
        }
        self.named
            .iter()
            .all(|(_, state)| matches!(state, HelperState::Exited))
    }
}

/// What the owner knows about one named reaper.
#[derive(Debug)]
enum HelperState {
    /// Pinned by a pidfd; not exited yet.
    Pending(OwnedFd),
    /// Terminal evidence: pidfd readable, or its pid gone or reused.
    Exited,
    /// Neither pinnable nor provably gone: never joined.
    Unverifiable,
}

fn complete_blocks(text: &str) -> Vec<Vec<String>> {
    let mut blocks = Vec::new();
    let mut current = Vec::new();
    for line in text.lines() {
        if line == "end" {
            blocks.push(std::mem::take(&mut current));
        } else {
            current.push(line.to_string());
        }
    }
    blocks
}

/// Pins the reaper a record names (`<boot>.<pid>.<start>`).
fn resolve_reaper(token: &str) -> HelperState {
    let mut parts = token.splitn(3, '.');
    let (Some(boot), Some(pid), Some(start)) = (parts.next(), parts.next(), parts.next()) else {
        return HelperState::Unverifiable;
    };
    let (Ok(pid), Ok(start)) = (pid.parse::<libc::pid_t>(), start.parse::<u64>()) else {
        return HelperState::Unverifiable;
    };
    match std::fs::read_to_string("/proc/sys/kernel/random/boot_id") {
        Ok(current) if current.trim() != boot => return HelperState::Exited,
        Ok(_) => {}
        Err(_) => return HelperState::Unverifiable,
    }
    let pidfd = match pidfd_open(pid) {
        Ok(pidfd) => pidfd,
        Err(err) if err.raw_os_error() == Some(libc::ESRCH) => return HelperState::Exited,
        Err(_) => return HelperState::Unverifiable,
    };
    // The pidfd pins whatever holds the pid now; it is the reaper only if
    // its start time matches the record.
    match process_start_ticks(pid) {
        Ok(ticks) if ticks == start => HelperState::Pending(pidfd),
        Ok(_) => HelperState::Exited,
        Err(err) if err.kind() == io::ErrorKind::NotFound => HelperState::Exited,
        Err(_) => HelperState::Unverifiable,
    }
}

fn process_start_ticks(pid: libc::pid_t) -> io::Result<u64> {
    let stat = std::fs::read(format!("/proc/{pid}/stat"))?;
    let close = stat
        .iter()
        .rposition(|byte| *byte == b')')
        .ok_or_else(|| io::Error::other("unparseable stat"))?;
    std::str::from_utf8(&stat[close + 1..])
        .ok()
        .and_then(|fields| fields.split_ascii_whitespace().nth(19)?.parse().ok())
        .ok_or_else(|| io::Error::other("unparseable stat"))
}

enum Drain {
    Open,
    Closed,
    Expired,
}

#[cfg(test)]
thread_local! {
    /// Report reads and pidfd polls performed by this thread (tests only).
    pub(crate) static IO_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn count_io_call() {
    #[cfg(test)]
    IO_CALLS.with(|calls| calls.set(calls.get() + 1));
}

/// Reads what is available, checking expiry before every read. EOF means
/// only that every writer closed its descriptor.
fn drain_report(report: &OwnedFd, text: &mut Vec<u8>, expired: &dyn Fn() -> bool) -> Drain {
    loop {
        if expired() {
            return Drain::Expired;
        }
        let mut chunk = [0_u8; 4096];
        count_io_call();
        // SAFETY: valid descriptor and buffer.
        let read =
            unsafe { libc::read(report.as_raw_fd(), chunk.as_mut_ptr().cast(), chunk.len()) };
        if read > 0 {
            text.extend_from_slice(&chunk[..read as usize]);
            continue;
        }
        if read == 0 {
            return Drain::Closed;
        }
        let err = io::Error::last_os_error();
        match err.kind() {
            io::ErrorKind::Interrupted => continue,
            io::ErrorKind::WouldBlock => return Drain::Open,
            // An unreadable report proves nothing; stop reading it.
            _ => return Drain::Closed,
        }
    }
}

fn drain_wake(wake: &OwnedFd) {
    let mut chunk = [0_u8; 64];
    // SAFETY: valid non-blocking descriptor and buffer.
    while unsafe { libc::read(wake.as_raw_fd(), chunk.as_mut_ptr().cast(), chunk.len()) } > 0 {}
}

/// A zero-timeout readiness check (a pidfd: the process exited).
fn poll_readable(fd: RawFd) -> bool {
    count_io_call();
    let mut poll = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: one valid pollfd.
    unsafe { libc::poll(&mut poll, 1, 0) > 0 }
}

fn wait_any(fds: &[RawFd], timeout: libc::c_int) {
    count_io_call();
    let mut polls: Vec<libc::pollfd> = fds
        .iter()
        .map(|fd| libc::pollfd {
            fd: *fd,
            events: libc::POLLIN,
            revents: 0,
        })
        .collect();
    // SAFETY: valid pollfd array; EINTR simply re-evaluates in the caller.
    unsafe {
        libc::poll(polls.as_mut_ptr(), polls.len() as libc::nfds_t, timeout);
    }
}

fn pidfd_open(pid: libc::pid_t) -> io::Result<OwnedFd> {
    // SAFETY: pidfd_open takes a pid and flags and returns a new descriptor.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the kernel returned a new owned descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(fd as RawFd) })
}

fn set_nonblocking(fd: &impl AsRawFd) -> io::Result<()> {
    // SAFETY: plain fcntl on a valid descriptor.
    let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
    if flags < 0
        || unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Parses the last complete receipt block. Without the join (terminal
/// evidence of every helper) before the deadline the observation is
/// `TimedOut`, whatever the receipt says.
pub(crate) fn parse_report(text: &str, joined: bool) -> LinuxSandboxCleanupReport {
    // Complete blocks end with a line that is exactly `end`; helper
    // declarations are not receipts.
    let mut last = None;
    let mut current = Vec::new();
    for line in text.lines() {
        if line == "end" {
            let block = std::mem::take(&mut current);
            if !block.contains(&"kind=helpers") {
                last = Some(block);
            }
        } else {
            current.push(line);
        }
    }
    let mut report = LinuxSandboxCleanupReport {
        state: SandboxCleanupState::Unreported,
        finisher: None,
        reaper_join: None,
        targets: Vec::new(),
        joined,
    };
    if let Some(block) = last {
        for line in block {
            if let Some(state) = line.strip_prefix("state=") {
                report.state = match state {
                    "complete" => SandboxCleanupState::Complete,
                    "timed-out" => SandboxCleanupState::TimedOut,
                    _ => SandboxCleanupState::Incomplete,
                };
            } else if let Some(finisher) = line.strip_prefix("finisher=") {
                report.finisher = Some(finisher.to_string());
            } else if let Some(join) = line.strip_prefix("reaper_join=") {
                report.reaper_join = Some(join.to_string());
            } else if let Some(target) = line.strip_prefix("target=")
                && let Some((outcome, path)) = target.split_once(' ')
            {
                report
                    .targets
                    .push((outcome.to_string(), PathBuf::from(unhex(path))));
            }
        }
    }
    if !joined {
        report.state = SandboxCleanupState::TimedOut;
    }
    report
}

fn unhex(text: &str) -> std::ffi::OsString {
    use std::os::unix::ffi::OsStringExt;
    let bytes = (0..text.len() / 2)
        .filter_map(|index| u8::from_str_radix(text.get(index * 2..index * 2 + 2)?, 16).ok())
        .collect();
    std::ffi::OsString::from_vec(bytes)
}

#[cfg(test)]
#[path = "linux_cleanup_custody_tests.rs"]
mod tests;
