use super::*;
use pretty_assertions::assert_eq;
use std::os::fd::AsRawFd;
use std::os::fd::IntoRawFd;
use std::os::fd::OwnedFd;

fn identity(fd: RawFd) -> (u64, u64) {
    let stat = stat_of(fd).expect("open descriptor");
    (stat.st_dev, stat.st_ino)
}

fn named(fd: RawFd, role: &str) -> String {
    let (dev, ino) = identity(fd);
    format!("{fd}:{role}:{dev}:{ino}")
}

fn socket_pair(kind: libc::c_int) -> (OwnedFd, OwnedFd) {
    let mut fds = [-1; 2];
    // SAFETY: valid two-element array.
    assert_eq!(
        unsafe {
            libc::socketpair(
                libc::AF_UNIX,
                kind | libc::SOCK_CLOEXEC,
                0,
                fds.as_mut_ptr(),
            )
        },
        0
    );
    // SAFETY: socketpair returned two new owned descriptors.
    unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) }
}

fn pipe() -> (OwnedFd, OwnedFd) {
    let mut fds = [-1; 2];
    // SAFETY: valid two-element array.
    assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
    // SAFETY: pipe2 returned two new owned descriptors.
    unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) }
}

fn read_only_file() -> File {
    let file = tempfile::NamedTempFile::new().expect("tempfile");
    let reader = File::open(file.path()).expect("open read-only");
    let fd = unsafe { libc::fcntl(reader.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 10) };
    assert!(fd >= 10);
    unsafe { File::from_raw_fd(fd) }
}

fn refusal(texts: &[String], custody: &[RawFd]) -> String {
    validate_forwarded_fds(texts, custody).expect_err("refused")
}

#[test]
fn every_role_is_accepted_with_its_own_type_and_identity() {
    let source = read_only_file();
    let probe = read_only_file();
    let texts = vec![
        named(source.as_raw_fd(), "snapshot-source"),
        named(probe.as_raw_fd(), "snapshot-probe"),
    ];
    let specs = validate_forwarded_fds(&texts, &[]).expect("valid");
    assert_eq!(
        specs
            .iter()
            .map(|spec| (spec.fd, spec.role))
            .collect::<Vec<_>>(),
        vec![
            (source.as_raw_fd(), ForwardedFdRole::SnapshotSource),
            (probe.as_raw_fd(), ForwardedFdRole::SnapshotProbe),
        ]
    );
}

#[test]
fn a_mismatched_type_is_refused_never_dropped() {
    let (stream, _peer) = socket_pair(libc::SOCK_STREAM);
    let (read_end, _write_end) = pipe();
    let (dgram, _dgram_peer) = socket_pair(libc::SOCK_DGRAM);
    let writable = tempfile::tempfile().expect("read-write tempfile");
    for text in [
        named(stream.as_raw_fd(), "snapshot-source"),
        named(dgram.as_raw_fd(), "snapshot-source"),
        named(read_end.as_raw_fd(), "snapshot-probe"),
        named(writable.as_raw_fd(), "snapshot-source"),
    ] {
        let message = refusal(std::slice::from_ref(&text), &[]);
        assert!(
            message.contains("is not an unlinked read-only snapshot carrier at fd >= 10"),
            "{text}: {message}"
        );
    }
}

#[test]
fn malformed_unknown_closed_stdio_and_misidentified_entries_are_refused() {
    let file = read_only_file();
    let fd = file.as_raw_fd();
    let (dev, ino) = identity(fd);
    let other = read_only_file();
    let (other_dev, other_ino) = identity(other.as_raw_fd());
    let closed = read_only_file().into_raw_fd();
    let closed_identity = identity(closed);
    // SAFETY: closes a descriptor this test owns.
    unsafe { libc::close(closed) };
    // Open all fixtures before closing this descriptor, so no later open
    // can reuse its number and invalidate the closed-descriptor case.
    for (text, expected) in [
        (
            format!("{fd}:snapshot-source:{dev}"),
            "expected <fd>:<role>",
        ),
        (
            format!("{fd}:snapshot-source:{dev}:{ino}:x"),
            "expected <fd>:<role>",
        ),
        (
            format!("x:snapshot-source:{dev}:{ino}"),
            "expected <fd>:<role>",
        ),
        (
            format!("-1:snapshot-source:{dev}:{ino}"),
            "collides with standard I/O",
        ),
        (format!("{fd}:stdin:{dev}:{ino}"), "unknown role"),
        (format!("{fd}:escalate-socket:{dev}:{ino}"), "unknown role"),
        (
            format!("1:snapshot-source:{dev}:{ino}"),
            "collides with standard I/O",
        ),
        (
            format!(
                "{closed}:snapshot-source:{}:{}",
                closed_identity.0, closed_identity.1
            ),
            "is not an open descriptor",
        ),
        (
            format!("{fd}:snapshot-source:{other_dev}:{other_ino}"),
            "is not the descriptor it was named as",
        ),
    ] {
        let message = refusal(std::slice::from_ref(&text), &[]);
        assert!(message.contains(expected), "{text}: {message}");
    }
}

#[test]
fn custody_descriptors_and_their_aliases_are_refused() {
    let (report_read, report_write) = pipe();
    let (deadline_read, _deadline_write) = pipe();
    let custody = [report_write.as_raw_fd(), deadline_read.as_raw_fd()];
    // A dup of the report pipe at another number: same type, dev and ino.
    // SAFETY: F_DUPFD_CLOEXEC returns a new descriptor owned here.
    let alias = unsafe {
        OwnedFd::from_raw_fd(libc::fcntl(
            report_write.as_raw_fd(),
            libc::F_DUPFD_CLOEXEC,
            10,
        ))
    };
    for fd in [
        report_write.as_raw_fd(),
        deadline_read.as_raw_fd(),
        alias.as_raw_fd(),
    ] {
        let message = refusal(&[named(fd, "snapshot-source")], &custody);
        assert!(
            message.contains("is a cleanup custody descriptor"),
            "{fd}: {message}"
        );
    }
    drop(report_read);
}

#[test]
fn a_descriptor_or_identity_named_twice_is_refused() {
    let file = read_only_file();
    // SAFETY: F_DUPFD_CLOEXEC returns a new descriptor owned here.
    let alias =
        unsafe { OwnedFd::from_raw_fd(libc::fcntl(file.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 10)) };
    let once = named(file.as_raw_fd(), "snapshot-source");
    for texts in [
        vec![once.clone(), once.clone()],
        vec![once, named(alias.as_raw_fd(), "snapshot-probe")],
    ] {
        assert!(refusal(&texts, &[]).contains("is named twice"));
    }
}

#[test]
fn nothing_is_owned_unless_the_whole_list_is_valid() {
    let file = read_only_file();
    let fd = file.as_raw_fd();
    // SAFETY: F_GETFD only reads the descriptor's flags.
    let before = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    let texts = vec![
        named(fd, "snapshot-source"),
        "9999:snapshot-source:1:1".to_string(),
    ];
    assert!(adopt_forwarded_fds(&texts, &[]).is_err());
    // Untouched: neither owned (still open after the refusal) nor re-flagged.
    // SAFETY: F_GETFD only reads the descriptor's flags.
    assert_eq!(unsafe { libc::fcntl(fd, libc::F_GETFD) }, before);
}

#[test]
fn adoption_keeps_the_number_and_marks_it_close_on_exec() {
    let source = read_only_file();
    let fd = source.as_raw_fd();
    // Inheritable, as the spawner's pre_exec leaves it in the launcher.
    // SAFETY: F_SETFD only changes this descriptor's flags.
    assert_eq!(unsafe { libc::fcntl(fd, libc::F_SETFD, 0) }, 0);
    let texts = vec![named(fd, "snapshot-source")];
    // The launcher takes ownership: release the test's.
    let raw = source.into_raw_fd();
    let adopted = adopt_forwarded_fds(&texts, &[]).expect("adopted");
    assert_eq!(adopted.len(), 1);
    assert_eq!(adopted[0].as_raw_fd(), raw);
    // SAFETY: F_GETFD only reads the descriptor's flags.
    let flags = unsafe { libc::fcntl(raw, libc::F_GETFD) };
    assert_eq!(flags & libc::FD_CLOEXEC, libc::FD_CLOEXEC);
}
