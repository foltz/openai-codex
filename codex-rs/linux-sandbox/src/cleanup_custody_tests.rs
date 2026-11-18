use super::*;
use pretty_assertions::assert_eq;
use std::io::{Read, Write};
use std::os::fd::FromRawFd;
use std::time::Duration;
use std::time::Instant;

fn pipe() -> (OwnedFd, std::fs::File) {
    let mut fds = [-1; 2];
    // SAFETY: valid two-element array.
    assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
    // SAFETY: pipe2 returned two new owned descriptors.
    unsafe {
        (
            OwnedFd::from_raw_fd(fds[0]),
            std::fs::File::from_raw_fd(fds[1]),
        )
    }
}

fn ns_from_now(offset: Duration) -> u64 {
    monotonic_now_ns() + offset.as_nanos() as u64
}

#[test]
fn tightening_only_moves_the_deadline_earlier() {
    let (read, mut write) = pipe();
    let mut deadline = CleanupDeadline::new(Some(ns_from_now(Duration::from_secs(60))), Some(read))
        .expect("deadline");
    assert!(!deadline.expired());

    // A later value is ignored.
    write
        .write_all(&ns_from_now(Duration::from_secs(600)).to_le_bytes())
        .expect("write later");
    assert!(!deadline.expired());

    // An earlier one applies, and stays applied after the owner closes.
    write
        .write_all(&monotonic_now_ns().to_le_bytes())
        .expect("write earlier");
    assert!(deadline.expired());
    drop(write);
    assert!(deadline.expired());
}

#[test]
fn an_owner_gone_without_any_deadline_stops_without_inventing_one() {
    let (read, write) = pipe();
    let mut deadline = CleanupDeadline::new(None, Some(read)).expect("deadline");
    assert!(!deadline.expired());
    assert_eq!(deadline.stop_reason(), None);
    drop(write);
    // Unproven, not a timeout, and no deadline was made up for it.
    assert_eq!(deadline.stop_reason(), Some(StopReason::OwnerGone));
    assert!(deadline.expired());
    assert_eq!(deadline.at_ns(), None);
}

#[test]
fn a_passed_issued_deadline_stays_a_timeout_after_the_owner_is_gone() {
    let (read, mut write) = pipe();
    let mut deadline = CleanupDeadline::new(Some(ns_from_now(Duration::from_secs(60))), Some(read))
        .expect("deadline");
    write
        .write_all(&monotonic_now_ns().to_le_bytes())
        .expect("tighten to now");
    drop(write);
    assert_eq!(deadline.stop_reason(), Some(StopReason::DeadlinePassed));
}

#[test]
fn a_closed_channel_keeps_the_original_deadline() {
    let (read, write) = pipe();
    let mut deadline = CleanupDeadline::new(Some(ns_from_now(Duration::from_secs(60))), Some(read))
        .expect("deadline");
    drop(write);
    assert!(!deadline.expired());
}

#[test]
fn waits_end_at_the_deadline_and_are_woken_by_tightening() {
    let (never_ready, _keep) = pipe();
    let (read, mut write) = pipe();
    let mut deadline = CleanupDeadline::new(Some(ns_from_now(Duration::from_secs(60))), Some(read))
        .expect("deadline");
    let tightener = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(50));
        write
            .write_all(&ns_from_now(Duration::from_millis(50)).to_le_bytes())
            .expect("tighten");
        write
    });
    let start = Instant::now();
    assert_eq!(deadline.wait_readable(never_ready.as_fd()), Wait::Expired);
    assert!(start.elapsed() < Duration::from_secs(10));
    drop(tightener.join().expect("tightener"));
}

#[test]
fn owner_less_waits_never_block() {
    let (never_ready, _keep) = pipe();
    let mut deadline = CleanupDeadline::none();
    assert!(!deadline.is_owned());
    assert_eq!(deadline.wait_readable(never_ready.as_fd()), Wait::NotYet);
    assert!(!deadline.expired());
}

#[test]
fn an_expired_deadline_polls_nothing_even_for_a_ready_descriptor() {
    let (ready, mut write) = pipe();
    write.write_all(b"x").expect("make readable");
    let mut deadline = CleanupDeadline::at_for_tests(0);
    let polls = polls_for_tests();
    assert_eq!(deadline.wait_readable(ready.as_fd()), Wait::Expired);
    assert_eq!(polls_for_tests(), polls, "no poll at or after the deadline");
    assert!(deadline.expired());

    // The same for an owner gone without a deadline.
    let (channel, close) = pipe();
    let mut owner_gone = CleanupDeadline::new(None, Some(channel)).expect("deadline");
    drop(close);
    let polls = polls_for_tests();
    assert_eq!(owner_gone.wait_readable(ready.as_fd()), Wait::Expired);
    assert_eq!(polls_for_tests(), polls);
}

#[test]
fn custody_pair_requires_distinct_pipes_in_the_correct_directions() {
    let (deadline_read, deadline_write) = pipe();
    let (report_read, report_write) = pipe();
    assert!(
        validate_pipe_pair(
            Some(deadline_read.as_raw_fd()),
            Some(report_write.as_raw_fd())
        )
        .is_ok()
    );
    assert!(
        validate_pipe_pair(
            Some(deadline_write.as_raw_fd()),
            Some(report_write.as_raw_fd())
        )
        .is_err()
    );
    assert!(
        validate_pipe_pair(
            Some(deadline_read.as_raw_fd()),
            Some(report_read.as_raw_fd())
        )
        .is_err()
    );
    assert!(
        validate_pipe_pair(
            Some(deadline_read.as_raw_fd()),
            Some(deadline_write.as_raw_fd())
        )
        .is_err()
    );
    assert!(
        validate_pipe_pair(
            Some(deadline_read.as_raw_fd()),
            Some(deadline_read.as_raw_fd())
        )
        .is_err()
    );
    assert!(validate_pipe_pair(Some(0), None).is_err());
    assert!(unsafe { libc::fcntl(deadline_read.as_raw_fd(), libc::F_GETFD) } >= 0);
}

#[test]
fn report_frame_is_atomic_and_complete() {
    let (read, write) = pipe();
    let write = OwnedFd::from(write);
    write_report(Some(&write), "v=1\nstate=complete\n").expect("delivered frame");
    drop(write);
    let mut text = String::new();
    std::fs::File::from(read)
        .read_to_string(&mut text)
        .expect("read frame");
    assert_eq!(text, "v=1\nstate=complete\nend\n");
}

#[test]
fn oversized_report_delivers_only_explicit_incomplete_summary() {
    let (read, write) = pipe();
    let write = OwnedFd::from(write);
    let bound = unsafe { libc::fpathconf(write.as_raw_fd(), libc::_PC_PIPE_BUF) };
    assert!(bound > 0);
    let receipt = format!("v=1\nstate=complete\n{}", "x".repeat(bound as usize));
    assert!(write_report(Some(&write), &receipt).is_err());
    drop(write);
    let mut text = String::new();
    std::fs::File::from(read)
        .read_to_string(&mut text)
        .expect("read summary");
    assert_eq!(
        text,
        "v=1\nstate=incomplete\nreport=exceeds-atomic-bound\nend\n"
    );
}

#[test]
fn saturated_report_pipe_returns_failure_without_waiting_or_partial_frame() {
    let (read, write) = pipe();
    let write = OwnedFd::from(write);
    let flags = unsafe { libc::fcntl(write.as_raw_fd(), libc::F_GETFL) };
    assert!(flags >= 0);
    assert_eq!(
        unsafe { libc::fcntl(write.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let capacity = unsafe { libc::fcntl(write.as_raw_fd(), libc::F_GETPIPE_SZ) };
    assert!(capacity > 0);
    let fill = vec![b'x'; capacity as usize];
    assert_eq!(
        unsafe { libc::write(write.as_raw_fd(), fill.as_ptr().cast(), fill.len()) },
        capacity as isize
    );
    let start = Instant::now();
    assert_eq!(
        write_report(Some(&write), "v=1\nstate=complete\n")
            .expect_err("full pipe")
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(start.elapsed() < Duration::from_secs(1));
    drop(write);
    let mut observed = Vec::new();
    std::fs::File::from(read)
        .read_to_end(&mut observed)
        .expect("drain");
    assert_eq!(observed, fill, "no receipt prefix was partially written");
}

#[test]
fn closed_report_reader_is_failure_and_no_owner_is_a_noop() {
    let (read, write) = pipe();
    let write = OwnedFd::from(write);
    drop(read);
    assert_eq!(
        write_report(Some(&write), "v=1\nstate=complete\n")
            .expect_err("closed owner")
            .raw_os_error(),
        Some(libc::EPIPE)
    );
    assert!(write_report(None, "ignored").is_ok());
}
