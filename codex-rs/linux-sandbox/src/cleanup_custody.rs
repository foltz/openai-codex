//! Launcher side of the owner's cleanup custody for synthetic mount targets.
//!
//! The process that spawns the launcher (core exec, and later exec-server,
//! unified exec and app-server) owns the launch's lifetime and its deadline.
//! It hands the launcher:
//!
//! - `--cleanup-deadline-monotonic-ns <ns>`: the original absolute retirement
//!   deadline on `CLOCK_MONOTONIC`, the clock behind `std::time::Instant`
//!   and `tokio::time::Instant` on Linux, so both sides compare the same
//!   instants;
//! - `--cleanup-deadline-fd <fd>`: an inherited pipe on which the owner writes
//!   later tightenings as 8-byte little-endian `CLOCK_MONOTONIC` nanoseconds.
//!   Only earlier deadlines are applied. EOF (the owner died or stopped
//!   holding the channel) changes nothing when a deadline was already issued.
//!   A launch that never received one is then "owner gone": nothing may bound
//!   further cleanup, so it stops driving at once, preserves every record and
//!   reports its targets as unproven (`incomplete:owner-gone-without-deadline`).
//!   No death-time budget is ever invented;
//! - `--cleanup-report-fd <fd>`: an inherited pipe on which the launcher, or
//!   its reaper, writes the final launch receipt. EOF on it only proves that
//!   its descriptors closed; the owner joins the launch on the helpers' pidfds
//!   (or a declaration that no reaper exists), never on EOF.
//!
//! The launcher and its reaper both read the channel. Whichever reads a
//! tightening publishes it in a shared anonymous mapping created before the
//! reaper is forked, so neither can miss one the other consumed.
//!
//! All waits are `poll` on pidfds plus the deadline channel, with the timeout
//! set to the current deadline: no timers, no polling past it. Once the
//! deadline passes, no cleanup happens and the outcome is a sticky
//! `timed-out`.
//!
//! A launcher started without any of these runs in owner-less mode: it never
//! spawns a reaper, never waits for the sandbox, and cleans only what is
//! already provably finished when bubblewrap exits.

use std::io;
use std::io::Write;
use std::os::fd::AsFd;
use std::os::fd::AsRawFd;
use std::os::fd::BorrowedFd;
use std::os::fd::FromRawFd;
use std::os::fd::OwnedFd;
use std::os::fd::RawFd;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

/// `at_ns` value meaning "no deadline yet".
const NO_DEADLINE: u64 = u64::MAX;

pub(crate) const DEADLINE_ARG: &str = "cleanup-deadline-monotonic-ns";
pub(crate) const DEADLINE_FD_ARG: &str = "cleanup-deadline-fd";
pub(crate) const REPORT_FD_ARG: &str = "cleanup-report-fd";

pub(crate) fn monotonic_now_ns() -> u64 {
    // SAFETY: zeroed timespec is a valid out-parameter.
    let mut now: libc::timespec = unsafe { std::mem::zeroed() };
    // SAFETY: CLOCK_MONOTONIC is always available on Linux.
    unsafe {
        libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut now);
    }
    (now.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(now.tv_nsec as u64)
}

/// Result of a bounded wait.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Wait {
    Ready,
    /// The deadline passed first.
    Expired,
    /// Owner-less mode: not ready right now, and nothing may be waited for.
    NotYet,
}

/// Why cleanup must stop driving.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StopReason {
    /// An issued deadline passed: a sticky timeout.
    DeadlinePassed,
    /// The owner is gone and never issued a deadline: unproven, not a timeout.
    OwnerGone,
}

/// Two `u64`s (deadline, owner-gone flag) in a `MAP_SHARED|MAP_ANONYMOUS`
/// page: shared with every process forked after it was created (the
/// reaper), never with others.
#[derive(Debug)]
struct SharedDeadline {
    page: std::ptr::NonNull<[AtomicU64; 2]>,
}

impl SharedDeadline {
    fn new(initial: u64) -> io::Result<Self> {
        // SAFETY: anonymous shared mapping of one page; checked below.
        let page = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                std::mem::size_of::<[AtomicU64; 2]>(),
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if page == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let page = std::ptr::NonNull::new(page.cast::<[AtomicU64; 2]>())
            .ok_or_else(|| io::Error::other("mmap returned null"))?;
        // SAFETY: the page is writable, aligned and exclusively ours here.
        unsafe {
            page.as_ptr()
                .write([AtomicU64::new(initial), AtomicU64::new(0)])
        };
        Ok(Self { page })
    }

    fn get(&self) -> &AtomicU64 {
        // SAFETY: the mapping lives as long as this process (never unmapped).
        unsafe { &self.page.as_ref()[0] }
    }

    fn owner_gone(&self) -> &AtomicU64 {
        // SAFETY: as above.
        unsafe { &self.page.as_ref()[1] }
    }
}

/// The current cleanup deadline: the original one, tightened by the owner.
#[derive(Debug)]
pub(crate) struct CleanupDeadline {
    shared: Option<SharedDeadline>,
    channel: Option<OwnedFd>,
    buffer: Vec<u8>,
}

impl CleanupDeadline {
    /// Owner-less mode.
    pub(crate) fn none() -> Self {
        Self {
            shared: None,
            channel: None,
            buffer: Vec::new(),
        }
    }

    pub(crate) fn new(at_ns: Option<u64>, channel: Option<OwnedFd>) -> io::Result<Self> {
        if at_ns.is_none() && channel.is_none() {
            return Ok(Self::none());
        }
        if let Some(channel) = &channel {
            set_nonblocking(channel.as_fd())?;
        }
        Ok(Self {
            shared: Some(SharedDeadline::new(at_ns.unwrap_or(NO_DEADLINE))?),
            channel,
            buffer: Vec::new(),
        })
    }

    #[cfg(test)]
    pub(crate) fn at_for_tests(at_ns: u64) -> Self {
        Self::new(Some(at_ns), None).expect("shared deadline")
    }

    fn at_ns(&self) -> Option<u64> {
        let at = self.shared.as_ref()?.get().load(Ordering::SeqCst);
        (at != NO_DEADLINE).then_some(at)
    }

    fn tighten_to(&self, at_ns: u64) {
        if let Some(shared) = &self.shared {
            shared.get().fetch_min(at_ns, Ordering::SeqCst);
        }
    }

    /// Whether an owner bounds this launch (a reaper may only exist then).
    pub(crate) fn is_owned(&self) -> bool {
        self.shared.is_some()
    }

    pub(crate) fn channel_fd(&self) -> Option<RawFd> {
        self.channel.as_ref().map(AsRawFd::as_raw_fd)
    }

    /// Applies every tightening already written; non-blocking.
    fn refresh(&mut self) {
        let Some(channel) = &self.channel else {
            return;
        };
        let mut closed = false;
        loop {
            let mut chunk = [0_u8; 64];
            // SAFETY: valid descriptor and buffer.
            let read =
                unsafe { libc::read(channel.as_raw_fd(), chunk.as_mut_ptr().cast(), chunk.len()) };
            if read > 0 {
                self.buffer.extend_from_slice(&chunk[..read as usize]);
                continue;
            }
            if read == 0 {
                closed = true;
            }
            break;
        }
        while self.buffer.len() >= 8 {
            let mut value = [0_u8; 8];
            value.copy_from_slice(&self.buffer[..8]);
            self.buffer.drain(..8);
            self.tighten_to(u64::from_le_bytes(value).min(NO_DEADLINE - 1));
        }
        if closed {
            self.channel = None;
            if self.at_ns().is_none()
                && let Some(shared) = &self.shared
            {
                // The owner is gone without ever issuing a deadline: stop
                // driving. This is not a timeout and no budget is invented.
                shared.owner_gone().store(1, Ordering::SeqCst);
            }
        }
    }

    /// Why cleanup must stop, if it must. An issued deadline that passed is a
    /// sticky timeout; an owner gone without any deadline is unproven.
    pub(crate) fn stop_reason(&mut self) -> Option<StopReason> {
        self.refresh();
        if self.at_ns().is_some_and(|at| monotonic_now_ns() >= at) {
            return Some(StopReason::DeadlinePassed);
        }
        let owner_gone = self
            .shared
            .as_ref()
            .is_some_and(|shared| shared.owner_gone().load(Ordering::SeqCst) != 0);
        owner_gone.then_some(StopReason::OwnerGone)
    }

    /// What is left of an issued deadline (zero once it passed), after
    /// applying every tightening already written; `None` while no deadline
    /// has been issued (or in owner-less mode).
    pub(crate) fn remaining(&mut self) -> Option<std::time::Duration> {
        self.refresh();
        let at = self.at_ns()?;
        Some(std::time::Duration::from_nanos(
            at.saturating_sub(monotonic_now_ns()),
        ))
    }

    /// Sticky once true: a deadline can only move earlier, and an owner
    /// that is gone never comes back.
    pub(crate) fn expired(&mut self) -> bool {
        self.stop_reason().is_some()
    }

    /// Waits for `fd` to become readable (a pidfd: the process exited), never
    /// past the deadline. Tightenings arriving meanwhile shorten the wait.
    /// Expiry first: at or after the deadline (or once the owner is gone
    /// without one) nothing is polled, not even once, and the result is
    /// `Expired` whatever `fd`'s state.
    pub(crate) fn wait_readable(&mut self, fd: BorrowedFd<'_>) -> Wait {
        loop {
            if self.expired() {
                return Wait::Expired;
            }
            if !self.is_owned() {
                return if poll_now(fd) {
                    Wait::Ready
                } else {
                    Wait::NotYet
                };
            }
            let timeout = match self.at_ns() {
                Some(at) => {
                    let remaining_ms = at.saturating_sub(monotonic_now_ns()).div_ceil(1_000_000);
                    libc::c_int::try_from(remaining_ms).unwrap_or(libc::c_int::MAX)
                }
                None => -1,
            };
            let mut fds = vec![libc::pollfd {
                fd: fd.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            }];
            if let Some(channel) = &self.channel {
                fds.push(libc::pollfd {
                    fd: channel.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                });
            }
            #[cfg(test)]
            count_poll_for_tests();
            // SAFETY: valid pollfd array.
            let rc = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, timeout) };
            if rc < 0 {
                if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Wait::Expired;
            }
            if fds[0].revents != 0 {
                return Wait::Ready;
            }
            // Timeout or channel activity: re-evaluate the deadline.
        }
    }
}

#[cfg(test)]
thread_local! {
    /// Unit-test-only count of the polls this thread's waits issued.
    static POLLS_FOR_TESTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn count_poll_for_tests() {
    POLLS_FOR_TESTS.with(|polls| polls.set(polls.get() + 1));
}

/// The number of polls this thread's waits have issued (unit tests only).
#[cfg(test)]
pub(crate) fn polls_for_tests() -> usize {
    POLLS_FOR_TESTS.with(std::cell::Cell::get)
}

fn poll_now(fd: BorrowedFd<'_>) -> bool {
    #[cfg(test)]
    count_poll_for_tests();
    let mut poll = libc::pollfd {
        fd: fd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: one valid pollfd, zero timeout.
    unsafe { libc::poll(&mut poll, 1, 0) > 0 }
}

fn set_nonblocking(fd: BorrowedFd<'_>) -> io::Result<()> {
    // SAFETY: plain fcntl on a valid descriptor.
    let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
    if flags < 0
        || unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Adopts an inherited pipe descriptor named on the command line: it must be
/// open and a FIFO, and is made close-on-exec so neither bubblewrap nor the
/// sandboxed command can inherit it.
pub(crate) fn adopt_inherited_pipe(fd: RawFd, what: &str) -> OwnedFd {
    // SAFETY: probing flags has no side effects.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if fd <= 2 || flags < 0 {
        panic!("refusing --{what} {fd}: not an inherited open descriptor");
    }
    // SAFETY: the owner transferred this open descriptor to the launcher.
    let owned = unsafe { OwnedFd::from_raw_fd(fd) };
    // SAFETY: valid descriptor.
    if unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
        panic!(
            "refusing --{what} {fd}: cannot mark close-on-exec: {}",
            io::Error::last_os_error()
        );
    }
    // SAFETY: zeroed stat is a valid out-parameter.
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: valid descriptor and out-parameter.
    if unsafe { libc::fstat(fd, &mut stat) } < 0 || stat.st_mode & libc::S_IFMT != libc::S_IFIFO {
        panic!("refusing --{what} {fd}: not a pipe");
    }
    owned
}

pub(crate) fn parse_inherited_fd(value: &str) -> Result<RawFd, String> {
    let fd: RawFd = value
        .parse()
        .map_err(|err| format!("invalid descriptor {value:?}: {err}"))?;
    if fd <= 2 {
        return Err(format!("descriptor {fd} is a standard stream"));
    }
    Ok(fd)
}

/// Writes one receipt block to the owner's report pipe. A receipt is far
/// below `PIPE_BUF`, so the write is atomic; an owner that stopped reading
/// makes it fail with `EPIPE` (SIGPIPE is ignored by the Rust runtime).
pub(crate) fn write_report(report: Option<&OwnedFd>, receipt: &str) {
    let Some(report) = report else {
        return;
    };
    let block = format!("{receipt}end\n");
    let mut file = std::mem::ManuallyDrop::new(unsafe {
        // SAFETY: borrowed for one write; the OwnedFd keeps ownership.
        std::fs::File::from_raw_fd(report.as_raw_fd())
    });
    let _ = file.write_all(block.as_bytes());
}

#[cfg(test)]
#[path = "cleanup_custody_tests.rs"]
mod tests;
