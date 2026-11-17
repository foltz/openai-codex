//! Cancellable stdio threads for the standalone stdio transport.
//!
//! Framing, EOF handling and backpressure are the same as before:
//! - line-delimited input split like `BufRead::lines` (strip `\n`, then one
//!   `\r`; a final unterminated line is yielded as-is; invalid UTF-8 yields an
//!   `InvalidData` error and stops);
//! - EOF ends the reader;
//! - one line in flight through a capacity-1 channel, waited on without
//!   busy polling.
//!
//! On Unix both threads wait in poll(2) on the stdio fd plus a private
//! self-pipe. Shutdown or the host's owner deadline writes the self-pipe, so a
//! thread waiting for readiness wakes promptly.
//!
//! O_NONBLOCK is deliberately NOT set on fd 0 or fd 1. The flag lives on the
//! open file description, which is shared with the parent and any sibling
//! holding the same pipe or TTY; `dup` shares it too. Setting it would change
//! their I/O. Only our private self-pipe is non-blocking.
//!
//! Writes go out in chunks, re-polling between them. How strong the deadline
//! bound is depends on what stdout is:
//! - pipe (FIFO): after POLLOUT a pipe accepts at least `PIPE_BUF` bytes
//!   (POSIX minimum 512) without blocking. With 512-byte chunks, the writer
//!   can only wait in poll(2), and that wait is cancellable. The bound is
//!   guaranteed.
//! - TTY/PTY, socket, other character device: POLLOUT promises some room, not
//!   512 bytes, and TTY flow control (XOFF, an unread PTY master) can still
//!   leave a write(2) blocked partway. Small 64-byte chunks narrow the window
//!   but cannot close it. The bound is best-effort.
//! - regular file: always "ready"; a write can wait on storage. Not cancellable.
//!
//! A writer still inside write(2) at the deadline is never claimed cancelled:
//! its real join stays unfinished and the host reports it `incomplete`.
//! Process exit does not wait for this thread.
//!
//! Elsewhere (Windows) the stdio read and write stay blocking: cancelling a
//! synchronous console or pipe read is not reliably safe there. Channel waits
//! are still cancellable. A thread left blocked is reported incomplete through
//! its real join; it is never treated as stopped.

use std::io;
use std::thread::JoinHandle;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::error;

pub(super) type StdinLine = io::Result<String>;
pub(super) type StdoutItem = (String, oneshot::Sender<()>);

/// Idempotent stop for both stdio threads.
#[derive(Clone)]
pub(super) struct StdioCancel {
    token: CancellationToken,
    #[cfg(unix)]
    wake: std::sync::Arc<unix::SelfPipe>,
}

impl StdioCancel {
    pub(super) fn new() -> io::Result<Self> {
        Ok(Self {
            token: CancellationToken::new(),
            #[cfg(unix)]
            wake: std::sync::Arc::new(unix::SelfPipe::new()?),
        })
    }

    pub(super) fn cancel(&self) {
        self.token.cancel();
        #[cfg(unix)]
        self.wake.wake();
    }

    #[cfg(unix)]
    fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }
}

/// Send with backpressure; gives up only when cancelled or the receiver is gone.
fn send_line(
    runtime: &Handle,
    tx: &mpsc::Sender<StdinLine>,
    line: StdinLine,
    cancel: &StdioCancel,
) -> bool {
    runtime.block_on(async {
        tokio::select! {
            biased;
            () = cancel.token.cancelled() => false,
            permit = tx.reserve() => match permit {
                Ok(permit) => {
                    permit.send(line);
                    true
                }
                Err(_) => false,
            },
        }
    })
}

/// Splits complete lines the way `BufRead::lines` does.
#[derive(Default)]
pub(super) struct LineFramer {
    pending: Vec<u8>,
}

impl LineFramer {
    pub(super) fn push(&mut self, bytes: &[u8]) {
        self.pending.extend_from_slice(bytes);
    }

    /// The next complete line, if any.
    pub(super) fn next_line(&mut self) -> Option<StdinLine> {
        let end = self.pending.iter().position(|byte| *byte == b'\n')?;
        let mut line: Vec<u8> = self.pending.drain(..=end).collect();
        line.pop();
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        Some(utf8(line))
    }

    /// At EOF: an unterminated final line, unchanged, like `lines()`.
    pub(super) fn finish(&mut self) -> Option<StdinLine> {
        if self.pending.is_empty() {
            return None;
        }
        Some(utf8(std::mem::take(&mut self.pending)))
    }
}

fn utf8(bytes: Vec<u8>) -> StdinLine {
    String::from_utf8(bytes).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "stream did not contain valid UTF-8",
        )
    })
}

/// Deliver each framed line; false stops the reader.
fn deliver(
    runtime: &Handle,
    tx: &mpsc::Sender<StdinLine>,
    line: StdinLine,
    cancel: &StdioCancel,
) -> bool {
    let failed = line.is_err();
    send_line(runtime, tx, line, cancel) && !failed
}

/// Task names in the owner report; each thread notes how it ended.
pub(super) const READER_TASK: &str = "stdin_reader";
pub(super) const WRITER_TASK: &str = "stdout_writer";
pub(super) const HUP_MONITOR_TASK: &str = "stdin_hangup_monitor";

/// Waits, without reading, until `fd` reports hangup (POLLHUP; on Linux also
/// POLLRDHUP), an error, or until `stop` wakes it. POLLIN is never requested,
/// so pending unread input neither wakes nor is consumed: framing and
/// backpressure are untouched, and fd 0 keeps its blocking mode. Returns
/// `true` when a hangup (or error) was observed.
///
/// Which stdin kinds report hangup this way:
/// - pipe/FIFO: POLLHUP once every writer has closed, even while unread data
///   remains;
/// - PTY slave / TTY: POLLHUP when the master side (or terminal) hangs up;
/// - stream socket: POLLRDHUP (Linux) when the peer shuts down writing,
///   POLLHUP when both directions are shut;
/// - regular file, `/dev/null` and other always-ready character devices:
///   never (poll reports only POLLIN/POLLOUT). Their EOF is observable only
///   by reading, so with a stuck processor and a full input queue no stdin
///   EOF edge is recorded for them; their shutdown edge then comes only from
///   SIGTERM (stdio signal intake) or another edge, and is otherwise
///   unbounded.
#[cfg(unix)]
pub(super) fn watch_for_hangup(fd: libc::c_int, stop: &StdioCancel) -> io::Result<bool> {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let events = libc::POLLRDHUP;
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    let events = 0;
    Ok(matches!(stop.wake.wait(fd, events)?, unix::Wait::Ready))
}

/// Owned stdin hangup monitor: records the first shutdown edge at hangup,
/// independently of the reader (which may be blocked on a full queue behind
/// a stuck processor) and of the processor. It is a blocking thread whose
/// only wait is one poll(2) on `fd` plus `stop`'s self-pipe, so `stop` (fired
/// at reader exit, SIGTERM, owner expiry or owner release) is a bounded
/// wakeup and its provable exit; its real join is a receipt in the owner.
#[cfg(unix)]
pub(super) fn spawn_hup_monitor(
    fd: libc::c_int,
    stop: StdioCancel,
    runtime: Handle,
    expiry: crate::OwnerExpiry,
) -> io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name("app-server-stdin-hup".to_string())
        .spawn(move || {
            let end = match watch_for_hangup(fd, &stop) {
                Ok(true) => {
                    // The latch callback spawns its timer on the runtime.
                    let _runtime = runtime.enter();
                    expiry.mark_shutdown_edge();
                    "hangup"
                }
                Ok(false) => "stopped",
                Err(err) => {
                    debug!("stdin hangup monitor stopped: {err}");
                    "error"
                }
            };
            expiry.note_blocking(HUP_MONITOR_TASK, end);
        })
}

pub(super) fn spawn_stdin_reader(
    tx: mpsc::Sender<StdinLine>,
    cancel: StdioCancel,
    runtime: Handle,
    expiry: crate::OwnerExpiry,
) -> io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name("app-server-stdin".to_string())
        .spawn(move || {
            #[cfg(unix)]
            let end = read_stdin(libc::STDIN_FILENO, &tx, &cancel, &runtime);
            #[cfg(not(unix))]
            let end = read_stdin(&tx, &cancel, &runtime);
            expiry.note_blocking(READER_TASK, end);
        })
}

pub(super) fn spawn_stdout_writer(
    mut rx: mpsc::Receiver<StdoutItem>,
    cancel: StdioCancel,
    runtime: Handle,
    expiry: crate::OwnerExpiry,
) -> io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name("app-server-stdout".to_string())
        .spawn(move || {
            // Exclude other Rust stdout writers for the thread's lifetime.
            let mut stdout = io::stdout().lock();
            let end = loop {
                let next = runtime.block_on(async {
                    tokio::select! {
                        biased;
                        () = cancel.token.cancelled() => None,
                        item = rx.recv() => item,
                    }
                });
                let Some((json, written_tx)) = next else {
                    if !cancel.token.is_cancelled() {
                        break "finished";
                    }
                    // Cancelled while idle: anything still queued is abandoned.
                    let queued = abandon_queued(&mut rx, &expiry);
                    break if queued {
                        "cancelled: output abandoned"
                    } else {
                        "cancelled"
                    };
                };
                match write_stdout(&mut stdout, json.as_bytes(), &cancel) {
                    Ok(WriteOutcome::Complete) => {
                        let _ = written_tx.send(());
                    }
                    Ok(WriteOutcome::Cancelled { unwritten }) => {
                        debug!("stdout write abandoned at shutdown; peer is not reading");
                        let unwritten = u64::try_from(unwritten).unwrap_or(u64::MAX);
                        expiry.record_dropped_output(1, unwritten);
                        abandon_queued(&mut rx, &expiry);
                        break "cancelled: output abandoned";
                    }
                    Err(err) if err.kind() == io::ErrorKind::BrokenPipe => {
                        // Positive signal: the output recipient closed (EPIPE,
                        // including after POLLHUP). Stdin EOF never gets here.
                        debug!("stdout recipient closed: {err}");
                        expiry.mark_recipient_closed();
                        expiry.record_undeliverable_after_disconnect(1, 0);
                        abandon_queued(&mut rx, &expiry);
                        break "recipient closed";
                    }
                    Err(err) => {
                        error!("Failed to write to stdout: {err}");
                        break "error";
                    }
                }
            };
            expiry.note_blocking(WRITER_TASK, end);
        })
}

/// Returns how the reader ended, for the owner report.
#[cfg(unix)]
fn read_stdin(
    fd: libc::c_int,
    tx: &mpsc::Sender<StdinLine>,
    cancel: &StdioCancel,
    runtime: &Handle,
) -> &'static str {
    let mut framer = LineFramer::default();
    let mut chunk = [0u8; 8 * 1024];
    loop {
        match cancel.wake.wait(fd, libc::POLLIN) {
            Ok(unix::Wait::Cancelled) => return "cancelled",
            Ok(unix::Wait::Ready) => {}
            Err(err) => {
                let _ = send_line(runtime, tx, Err(err), cancel);
                return "error";
            }
        }
        if cancel.is_cancelled() {
            return "cancelled";
        }
        // SAFETY: `chunk` is a valid, writable buffer of `chunk.len()` bytes,
        // and `fd` (fd 0 in production) stays open for the reader's life.
        let read = unsafe { libc::read(fd, chunk.as_mut_ptr().cast(), chunk.len()) };
        if read < 0 {
            let err = io::Error::last_os_error();
            if matches!(
                err.kind(),
                io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
            ) {
                continue;
            }
            let _ = send_line(runtime, tx, Err(err), cancel);
            return "error";
        }
        if read == 0 {
            if let Some(line) = framer.finish() {
                let _ = deliver(runtime, tx, line, cancel);
            }
            return "eof";
        }
        framer.push(&chunk[..read.unsigned_abs()]);
        while let Some(line) = framer.next_line() {
            if !deliver(runtime, tx, line, cancel) {
                return if cancel.is_cancelled() {
                    "cancelled"
                } else {
                    "stopped"
                };
            }
        }
    }
}

#[cfg(not(unix))]
fn read_stdin(
    tx: &mpsc::Sender<StdinLine>,
    cancel: &StdioCancel,
    runtime: &Handle,
) -> &'static str {
    use std::io::BufRead;
    // Blocking read: no safe cancellation here; reported incomplete if left.
    for line in io::stdin().lock().lines() {
        if !deliver(runtime, tx, line, cancel) {
            return "stopped";
        }
    }
    "eof"
}

/// Counts (and drops) messages still queued for the writer thread.
fn abandon_queued(rx: &mut mpsc::Receiver<StdoutItem>, expiry: &crate::OwnerExpiry) -> bool {
    let (mut messages, mut bytes) = (0u64, 0u64);
    while let Ok((json, _written_tx)) = rx.try_recv() {
        messages = messages.saturating_add(1);
        bytes = bytes.saturating_add(u64::try_from(json.len()).unwrap_or(u64::MAX));
    }
    expiry.record_dropped_output(messages, bytes);
    messages > 0
}

/// Whether a write finished or was cancelled with bytes still unwritten.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WriteOutcome {
    Complete,
    Cancelled { unwritten: usize },
}

/// `Cancelled` when cancelled while the peer was not accepting output.
#[cfg(unix)]
fn write_stdout(
    _stdout: &mut io::StdoutLock<'_>,
    bytes: &[u8],
    cancel: &StdioCancel,
) -> io::Result<WriteOutcome> {
    write_fd(
        libc::STDOUT_FILENO,
        bytes,
        cancel,
        unix::chunk_size(libc::STDOUT_FILENO),
    )
}

/// Chunked, poll-gated write to `fd`; `Cancelled` when cancelled while waiting.
#[cfg(unix)]
fn write_fd(
    fd: std::os::fd::RawFd,
    bytes: &[u8],
    cancel: &StdioCancel,
    chunk: usize,
) -> io::Result<WriteOutcome> {
    let mut rest = bytes;
    while !rest.is_empty() {
        match cancel.wake.wait(fd, libc::POLLOUT)? {
            unix::Wait::Cancelled => {
                return Ok(WriteOutcome::Cancelled {
                    unwritten: rest.len(),
                });
            }
            unix::Wait::Ready => {}
        }
        let len = rest.len().min(chunk);
        // SAFETY: `rest[..len]` is a valid readable buffer, and the caller
        // keeps `fd` open for the duration of the call.
        let written = unsafe { libc::write(fd, rest.as_ptr().cast(), len) };
        if written < 0 {
            let err = io::Error::last_os_error();
            if matches!(
                err.kind(),
                io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
            ) {
                continue;
            }
            return Err(err);
        }
        if written == 0 {
            // No progress is an error, never a silent retry loop.
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "stdout accepted zero bytes",
            ));
        }
        rest = &rest[written.unsigned_abs()..];
    }
    Ok(WriteOutcome::Complete)
}

#[cfg(not(unix))]
fn write_stdout(
    stdout: &mut io::StdoutLock<'_>,
    bytes: &[u8],
    _cancel: &StdioCancel,
) -> io::Result<WriteOutcome> {
    use std::io::Write;
    stdout.write_all(bytes)?;
    stdout.flush()?;
    Ok(WriteOutcome::Complete)
}

#[cfg(unix)]
mod unix {
    use std::io;
    use std::os::fd::AsRawFd;
    use std::os::fd::FromRawFd;
    use std::os::fd::OwnedFd;
    use std::os::fd::RawFd;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    pub(super) enum Wait {
        Ready,
        Cancelled,
    }

    /// POSIX minimum `PIPE_BUF`: after POLLOUT a pipe accepts this much.
    const PIPE_CHUNK: usize = 512;
    /// Best-effort size for TTY/PTY and other character devices.
    const DEVICE_CHUNK: usize = 64;
    const FILE_CHUNK: usize = 64 * 1024;

    /// Chunk size for `fd` by file type (see the module docs for the bound).
    pub(super) fn chunk_size(fd: RawFd) -> usize {
        // SAFETY: an all-zero `stat` is a valid out-buffer for fstat(2).
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        // SAFETY: fstat on an open fd with a valid out pointer.
        if unsafe { libc::fstat(fd, &mut stat) } != 0 {
            return DEVICE_CHUNK;
        }
        match stat.st_mode & libc::S_IFMT {
            libc::S_IFIFO => PIPE_CHUNK,
            libc::S_IFREG => FILE_CHUNK,
            libc::S_IFSOCK => PIPE_CHUNK,
            _ => DEVICE_CHUNK,
        }
    }

    /// A private, non-blocking, close-on-exec self-pipe. Only these two fds
    /// get O_NONBLOCK; the shared stdio descriptions are never modified.
    pub(super) struct SelfPipe {
        read: OwnedFd,
        write: OwnedFd,
        woken: AtomicBool,
    }

    fn add_flag(
        fd: RawFd,
        get: libc::c_int,
        set: libc::c_int,
        flag: libc::c_int,
    ) -> io::Result<()> {
        // SAFETY: fcntl on a valid fd that this process owns.
        let current = unsafe { libc::fcntl(fd, get) };
        if current < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: as above; only adds `flag`.
        if unsafe { libc::fcntl(fd, set, current | flag) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    impl SelfPipe {
        pub(super) fn new() -> io::Result<Self> {
            let mut fds = [0 as libc::c_int; 2];
            // SAFETY: `fds` is a valid two-element array for pipe(2).
            if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: pipe(2) just returned two new fds that nothing else owns.
            let (read, write) =
                unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
            for fd in [read.as_raw_fd(), write.as_raw_fd()] {
                add_flag(fd, libc::F_GETFD, libc::F_SETFD, libc::FD_CLOEXEC)?;
                add_flag(fd, libc::F_GETFL, libc::F_SETFL, libc::O_NONBLOCK)?;
            }
            Ok(Self {
                read,
                write,
                woken: AtomicBool::new(false),
            })
        }

        pub(super) fn wake(&self) {
            if !self.woken.swap(true, Ordering::SeqCst) {
                let byte = [1u8];
                // SAFETY: writes one byte from a valid buffer to our own pipe.
                // A full pipe (EAGAIN) is already readable, so it is ignored.
                let _ = unsafe { libc::write(self.write.as_raw_fd(), byte.as_ptr().cast(), 1) };
            }
        }

        /// Wait for `events` on `fd` or for the wake, whichever comes first.
        /// The wake byte is never drained, so the wake stays sticky.
        pub(super) fn wait(&self, fd: RawFd, events: libc::c_short) -> io::Result<Wait> {
            loop {
                if self.woken.load(Ordering::SeqCst) {
                    return Ok(Wait::Cancelled);
                }
                let mut fds = [
                    libc::pollfd {
                        fd,
                        events,
                        revents: 0,
                    },
                    libc::pollfd {
                        fd: self.read.as_raw_fd(),
                        events: libc::POLLIN,
                        revents: 0,
                    },
                ];
                // SAFETY: `fds` is a valid array of two pollfd entries.
                let ready = unsafe { libc::poll(fds.as_mut_ptr(), 2, -1) };
                if ready < 0 {
                    let err = io::Error::last_os_error();
                    if err.kind() == io::ErrorKind::Interrupted {
                        continue;
                    }
                    return Err(err);
                }
                if fds[1].revents != 0 || self.woken.load(Ordering::SeqCst) {
                    return Ok(Wait::Cancelled);
                }
                // HUP/ERR/NVAL also report Ready: the next read or write then
                // returns EOF or the error itself, as a blocking call would.
                if fds[0].revents != 0 {
                    return Ok(Wait::Ready);
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "stdio_threads_tests.rs"]
mod tests;
