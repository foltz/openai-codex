use super::*;
use pretty_assertions::assert_eq;
use std::io::BufRead;
use std::sync::mpsc as std_mpsc;
use std::time::Duration;

/// How long a blocked thread must stay blocked to count as "still waiting".
const STILL_BLOCKED: Duration = Duration::from_millis(200);
/// Upper bound for a cancelled wait to report back.
const RELEASE_BOUND: Duration = Duration::from_secs(5);

/// Cancels on drop, so a failed assertion still releases the helper thread.
struct CancelOnDrop(StdioCancel);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// Frame `input` delivered in `chunk`-sized reads; compare with `BufRead::lines`.
fn framed(input: &[u8], chunk: usize) -> Vec<Result<String, io::ErrorKind>> {
    let mut framer = LineFramer::default();
    let mut out = Vec::new();
    for piece in input.chunks(chunk.max(1)) {
        framer.push(piece);
        while let Some(line) = framer.next_line() {
            let failed = line.is_err();
            out.push(line.map_err(|err| err.kind()));
            if failed {
                return out;
            }
        }
    }
    if let Some(line) = framer.finish() {
        out.push(line.map_err(|err| err.kind()));
    }
    out
}

fn std_lines(input: &[u8]) -> Vec<Result<String, io::ErrorKind>> {
    let mut out = Vec::new();
    for line in io::Cursor::new(input).lines() {
        let failed = line.is_err();
        out.push(line.map_err(|err| err.kind()));
        if failed {
            break;
        }
    }
    out
}

#[test]
fn framing_matches_bufread_lines() {
    let cases: [&[u8]; 7] = [
        b"{\"id\":1}\n{\"id\":2}\n",
        b"crlf\r\nnext\n",
        b"final line without newline",
        b"trailing cr at eof\r",
        b"\n\n",
        b"",
        b"ok\n\xff\xfe\nnever reached\n",
    ];
    for input in cases {
        for chunk in [1, 2, 3, 7, 8 * 1024] {
            assert_eq!(
                framed(input, chunk),
                std_lines(input),
                "{input:?} by {chunk}"
            );
        }
    }
}

#[test]
fn backpressured_send_waits_then_stops_only_on_cancel() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let (tx, mut rx) = mpsc::channel::<StdinLine>(1);
    let cancel = StdioCancel::new().expect("cancel");
    let release = CancelOnDrop(cancel.clone());
    let handle = runtime.handle().clone();
    assert!(send_line(&handle, &tx, Ok("first".to_string()), &cancel));

    let (done_tx, done_rx) = std_mpsc::channel();
    let sender = std::thread::spawn({
        let cancel = cancel.clone();
        move || {
            let sent = send_line(&handle, &tx, Ok("second".to_string()), &cancel);
            let _ = done_tx.send(sent);
        }
    });

    // Backpressure: the full capacity-1 channel keeps the second send waiting.
    assert_eq!(
        done_rx.recv_timeout(STILL_BLOCKED),
        Err(std_mpsc::RecvTimeoutError::Timeout),
        "a full channel must apply backpressure"
    );
    cancel.cancel();
    assert_eq!(
        done_rx.recv_timeout(RELEASE_BOUND),
        Ok(false),
        "cancel stops the wait"
    );
    sender.join().expect("sender thread already reported done");
    drop(release);
    assert_eq!(
        runtime.block_on(rx.recv()).map(Result::ok),
        Some(Some("first".to_string()))
    );
}

#[cfg(unix)]
fn owned_pipe() -> (std::os::fd::OwnedFd, std::os::fd::OwnedFd) {
    use std::os::fd::FromRawFd;
    let mut fds = [0 as libc::c_int; 2];
    // SAFETY: valid two-element array for pipe(2).
    assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
    // SAFETY: freshly created fds owned only by this test.
    unsafe {
        (
            std::os::fd::OwnedFd::from_raw_fd(fds[0]),
            std::os::fd::OwnedFd::from_raw_fd(fds[1]),
        )
    }
}

#[cfg(unix)]
#[test]
fn poll_wait_returns_promptly_when_cancelled() {
    use std::os::fd::AsRawFd;
    // The write end stays open, so the read end is idle: like an open stdin.
    let (read, _write) = owned_pipe();
    let cancel = StdioCancel::new().expect("cancel");
    let release = CancelOnDrop(cancel.clone());
    let (done_tx, done_rx) = std_mpsc::channel();
    let waiter = std::thread::spawn({
        let cancel = cancel.clone();
        let fd = read.as_raw_fd();
        move || {
            let cancelled = matches!(
                cancel.wake.wait(fd, libc::POLLIN),
                Ok(unix::Wait::Cancelled)
            );
            let _ = done_tx.send(cancelled);
        }
    });

    assert_eq!(
        done_rx.recv_timeout(STILL_BLOCKED),
        Err(std_mpsc::RecvTimeoutError::Timeout),
        "idle input keeps the reader waiting"
    );
    cancel.cancel();
    assert_eq!(done_rx.recv_timeout(RELEASE_BOUND), Ok(true));
    waiter.join().expect("waiter already reported done");
    drop(release);
}

/// Saturated pipe (the guaranteed case): the writer can only wait in poll(2),
/// so cancellation abandons the output and the thread returns.
#[cfg(unix)]
#[test]
fn saturated_pipe_write_is_cancelled_and_reports_abandoned_output() {
    use std::os::fd::AsRawFd;
    let (_read, write) = owned_pipe();
    let cancel = StdioCancel::new().expect("cancel");
    let release = CancelOnDrop(cancel.clone());
    let fd = write.as_raw_fd();
    assert_eq!(unix::chunk_size(fd), 512);
    let (done_tx, done_rx) = std_mpsc::channel();
    let writer = std::thread::spawn({
        let cancel = cancel.clone();
        move || {
            // Far more than any pipe buffer; nobody reads `_read`.
            let payload = vec![b'x'; 4 * 1024 * 1024];
            let _ = done_tx.send(write_fd(fd, &payload, &cancel, 512).ok());
        }
    });

    assert_eq!(
        done_rx.recv_timeout(STILL_BLOCKED),
        Err(std_mpsc::RecvTimeoutError::Timeout),
        "a full pipe keeps the writer waiting"
    );
    cancel.cancel();
    let outcome = done_rx.recv_timeout(RELEASE_BOUND);
    assert!(
        matches!(outcome, Ok(Some(WriteOutcome::Cancelled { unwritten })) if unwritten > 0),
        "{outcome:?}"
    );
    writer.join().expect("writer already reported done");
    drop(release);
}

/// PTY flow control (best-effort case): with the master unread, the writer
/// either waits in poll(2), where cancellation releases it, or blocks inside
/// write(2). In the second case it must be reported `incomplete`, never
/// claimed cancelled. Reading the master afterwards releases the thread.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn stalled_pty_writer_is_reported_truthfully_at_the_deadline() {
    use std::io::Read;
    use std::os::fd::AsRawFd;
    use std::os::fd::FromRawFd;
    use std::os::fd::OwnedFd;

    // SAFETY: posix_openpt returns a new master fd or -1.
    let master = unsafe { libc::posix_openpt(libc::O_RDWR | libc::O_NOCTTY) };
    assert!(master >= 0, "posix_openpt failed");
    // SAFETY: `master` is a valid, newly opened fd owned by this test.
    let master = unsafe { OwnedFd::from_raw_fd(master) };
    // SAFETY: grantpt/unlockpt/ptsname on our own master fd.
    let slave_path = unsafe {
        assert_eq!(libc::grantpt(master.as_raw_fd()), 0);
        assert_eq!(libc::unlockpt(master.as_raw_fd()), 0);
        let name = libc::ptsname(master.as_raw_fd());
        assert!(!name.is_null());
        std::ffi::CStr::from_ptr(name).to_owned()
    };
    // SAFETY: opening the slave path returned by ptsname.
    let slave = unsafe { libc::open(slave_path.as_ptr(), libc::O_RDWR | libc::O_NOCTTY) };
    assert!(slave >= 0, "open pty slave failed");
    // SAFETY: `slave` is a valid, newly opened fd owned by this test.
    let slave = unsafe { OwnedFd::from_raw_fd(slave) };
    let slave_fd = slave.as_raw_fd();
    assert_eq!(
        unix::chunk_size(slave_fd),
        64,
        "character devices are best-effort"
    );

    let expiry = crate::OwnerExpiry::never();
    let cancel = StdioCancel::new().expect("cancel");
    let release = CancelOnDrop(cancel.clone());
    let (done_tx, done_rx) = std_mpsc::channel();
    let writer = std::thread::spawn({
        let cancel = cancel.clone();
        let expiry = expiry.clone();
        move || {
            let payload = vec![b'x'; 4 * 1024 * 1024];
            let note = match write_fd(slave_fd, &payload, &cancel, 64) {
                Ok(WriteOutcome::Complete) => "finished",
                Ok(WriteOutcome::Cancelled { .. }) => "cancelled: output abandoned",
                Err(_) => "error",
            };
            expiry.note_blocking(WRITER_TASK, note);
            let _ = done_tx.send(note);
        }
    });
    expiry.register_blocking(WRITER_TASK, writer);

    // The master is not read, so flow control stalls the writer.
    assert_eq!(
        done_rx.recv_timeout(STILL_BLOCKED),
        Err(std_mpsc::RecvTimeoutError::Timeout),
        "an unread PTY master stalls the writer"
    );
    // Deadline: cancel, then give it the same bounded window to report back.
    cancel.cancel();
    match done_rx.recv_timeout(RELEASE_BOUND) {
        Ok(note) => {
            assert_eq!(note, "cancelled: output abandoned");
            // The thread noted, then returns; wait (bounded) for its real join.
            let joined_by = std::time::Instant::now() + RELEASE_BOUND;
            while !expiry.incomplete_blocking().is_empty() && std::time::Instant::now() < joined_by
            {
                std::thread::sleep(Duration::from_millis(5));
            }
            assert_eq!(
                expiry.blocking_states(),
                vec![(WRITER_TASK, "cancelled: output abandoned")]
            );
        }
        Err(_) => {
            // Blocked inside write(2): truthful report, not a claimed cancel.
            assert_eq!(expiry.blocking_states(), vec![(WRITER_TASK, "incomplete")]);
            // Release it by draining the master, then confirm it ends.
            let mut master_file = std::fs::File::from(master.try_clone().expect("dup master"));
            let (drained_tx, drained_rx) = std_mpsc::channel();
            std::thread::spawn(move || {
                let mut buffer = [0u8; 64 * 1024];
                while drained_rx.try_recv().is_err() {
                    if master_file.read(&mut buffer).unwrap_or(0) == 0 {
                        break;
                    }
                }
            });
            assert!(
                done_rx.recv_timeout(RELEASE_BOUND).is_ok(),
                "writer never released"
            );
            let _ = drained_tx.send(());
        }
    }
    drop(release);
    drop(slave);
}

#[cfg(unix)]
fn test_pipe() -> (std::os::fd::OwnedFd, std::os::fd::OwnedFd) {
    use std::os::fd::FromRawFd;
    let mut fds = [0 as libc::c_int; 2];
    // SAFETY: `fds` is a valid two-element array for pipe(2).
    assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0, "pipe");
    // SAFETY: pipe(2) just returned two new fds that nothing else owns.
    unsafe {
        (
            std::os::fd::OwnedFd::from_raw_fd(fds[0]),
            std::os::fd::OwnedFd::from_raw_fd(fds[1]),
        )
    }
}

/// An owner whose first-shutdown-edge hook reports each edge on a channel.
#[cfg(unix)]
fn edge_reporting_owner() -> (
    crate::OwnerCustody,
    crate::OwnerExpiry,
    std_mpsc::Receiver<()>,
) {
    let custody = crate::OwnerCustody::new(CancellationToken::new());
    let owner = custody.expiry();
    let (edge_tx, edge_rx) = std_mpsc::channel();
    let edge_tx = std::sync::Mutex::new(edge_tx);
    assert!(owner.set_shutdown_edge(move || {
        let _ = edge_tx.lock().expect("edge").send(());
    }));
    (custody, owner, edge_rx)
}

/// Bytes written to `fd` (a pipe read end) that nobody has read yet.
#[cfg(unix)]
fn unread_pipe_bytes(fd: libc::c_int) -> usize {
    let mut pending: libc::c_int = 0;
    // SAFETY: FIONREAD on our own open pipe writes one c_int to `pending`.
    let status = unsafe { libc::ioctl(fd, libc::FIONREAD, &raw mut pending) };
    assert_eq!(status, 0, "FIONREAD on the test pipe");
    usize::try_from(pending).expect("pending byte count")
}

/// A full input queue behind a stuck processor keeps the reader from ever
/// reading EOF. The writer then closes the pipe: the owned hangup monitor
/// latches the shutdown edge from POLLHUP (unread data still pending, nothing
/// read ahead), while the reader is still blocked on the queue.
#[cfg(unix)]
#[test]
fn pipe_hangup_latches_the_edge_while_the_reader_is_blocked_by_a_stuck_processor() {
    use std::os::fd::AsRawFd;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let (_custody, owner, edge_rx) = edge_reporting_owner();
    let (read_end, write_end) = test_pipe();
    // A stuck processor: a capacity-1 queue that is already full and is never
    // drained, so the reader's first delivery can only block.
    let (tx, mut stuck_processor_rx) = mpsc::channel::<StdinLine>(1);
    tx.try_send(Ok("queued before the reader".to_string()))
        .expect("fill the queue");
    let framed = b"{\"first\":1}\n";
    // SAFETY: writes a valid buffer to our own pipe's write end.
    let written =
        unsafe { libc::write(write_end.as_raw_fd(), framed.as_ptr().cast(), framed.len()) };
    assert_eq!(written, isize::try_from(framed.len()).expect("len"));

    let cancel = StdioCancel::new().expect("cancel");
    let release_reader = CancelOnDrop(cancel.clone());
    let reader = std::thread::spawn({
        let cancel = cancel.clone();
        let handle = runtime.handle().clone();
        let fd = read_end.as_raw_fd();
        move || read_stdin(fd, &tx, &cancel, &handle)
    });
    // Handshake: the reader has read the complete line. Its next step is to
    // deliver it, and the queue is full, so from here it is blocked on the
    // queue and does not read again.
    let read_by = std::time::Instant::now() + RELEASE_BOUND;
    while unread_pipe_bytes(read_end.as_raw_fd()) != 0 {
        assert!(
            std::time::Instant::now() < read_by,
            "the reader reads the framed line"
        );
        std::thread::yield_now();
    }
    // Input the blocked reader never reads.
    let unread = b"{\"unread\":2}\n";
    // SAFETY: writes a valid buffer to our own pipe's write end.
    let written =
        unsafe { libc::write(write_end.as_raw_fd(), unread.as_ptr().cast(), unread.len()) };
    assert_eq!(written, isize::try_from(unread.len()).expect("len"));

    let hup_stop = StdioCancel::new().expect("hangup stop");
    let release_monitor = CancelOnDrop(hup_stop.clone());
    let monitor = spawn_hup_monitor(
        read_end.as_raw_fd(),
        hup_stop,
        runtime.handle().clone(),
        owner,
    )
    .expect("monitor");

    // The client closes stdin: only the writer side, unread data remains.
    drop(write_end);
    edge_rx
        .recv_timeout(RELEASE_BOUND)
        .expect("the edge latches from hangup, before any queue drains");
    monitor.join().expect("the monitor exits after the hangup");
    assert!(
        !reader.is_finished(),
        "the reader is still blocked on the full queue"
    );
    assert_eq!(
        unread_pipe_bytes(read_end.as_raw_fd()),
        unread.len(),
        "the monitor read nothing ahead"
    );
    assert_eq!(
        stuck_processor_rx.len(),
        1,
        "nothing beyond the queue was delivered"
    );

    // Owner expiry stops the blocked reader (the existing cancel path).
    cancel.cancel();
    assert_eq!(reader.join().expect("reader"), "cancelled");
    assert_eq!(
        stuck_processor_rx.try_recv().ok().and_then(Result::ok),
        Some("queued before the reader".to_string()),
        "the queue still holds only what was there before the reader"
    );
    assert!(
        stuck_processor_rx.try_recv().is_err(),
        "the cancelled reader delivered nothing"
    );
    drop(release_monitor);
    drop(release_reader);
    drop(read_end);
}

/// No hangup: the monitor is stopped at owner expiry/release, exits (its
/// join is the receipt), and observes nothing afterwards.
#[cfg(unix)]
#[test]
fn hangup_monitor_stops_at_expiry_and_observes_nothing_after() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let (_custody, owner, edge_rx) = edge_reporting_owner();
    let (read_end, write_end) = test_pipe();
    let hup_stop = StdioCancel::new().expect("hangup stop");
    let monitor = {
        use std::os::fd::AsRawFd;
        spawn_hup_monitor(
            read_end.as_raw_fd(),
            hup_stop.clone(),
            runtime.handle().clone(),
            owner,
        )
        .expect("monitor")
    };
    // Expiry (or release, or reader exit) stops it.
    hup_stop.cancel();
    monitor.join().expect("the monitor exits at the stop");
    // A hangup after the stop is never observed: the monitor is gone.
    drop(write_end);
    assert!(
        edge_rx.recv_timeout(STILL_BLOCKED).is_err(),
        "no edge after the monitor stopped"
    );
    drop(read_end);
}
