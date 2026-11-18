use super::*;
use pretty_assertions::assert_eq;
#[cfg(unix)]
use std::os::fd::AsRawFd;

fn launcher_argv() -> Vec<String> {
    [
        "codex-linux-sandbox",
        "--sandbox-policy-cwd",
        "/w",
        "--",
        "sh",
        "-c",
        "--",
    ]
    .map(String::from)
    .to_vec()
}

#[cfg(unix)]
#[test]
fn forwarded_fds_are_named_before_the_launcher_separator_with_their_identity() {
    let file = tempfile::tempfile().expect("tempfile");
    let fd = file.as_raw_fd();
    // SAFETY: zeroed stat is a valid out-parameter.
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: valid descriptor and out-parameter.
    assert_eq!(unsafe { libc::fstat(fd, &mut stat) }, 0);
    let mut argv = launcher_argv();
    attach_forwarded_fds(
        SandboxType::LinuxSeccomp,
        &mut argv,
        &[ForwardedFd::new(fd, ForwardedFdRole::SnapshotSource)],
    )
    .expect("attach");
    let mut expected = launcher_argv();
    expected.splice(
        3..3,
        [
            FORWARD_FD_ARG.to_string(),
            format!("{fd}:snapshot-source:{}:{}", stat.st_dev, stat.st_ino),
        ],
    );
    assert_eq!(argv, expected);
}

#[test]
fn other_sandboxes_and_empty_lists_leave_the_argv_unchanged() {
    let mut argv = launcher_argv();
    attach_forwarded_fds(
        SandboxType::MacosSeatbelt,
        &mut argv,
        &[ForwardedFd::new(3, ForwardedFdRole::SnapshotSource)],
    )
    .expect("attach");
    attach_forwarded_fds(SandboxType::LinuxSeccomp, &mut argv, &[]).expect("attach");
    assert_eq!(argv, launcher_argv());
}

#[cfg(unix)]
#[test]
fn a_closed_descriptor_cannot_be_named() {
    let file = tempfile::tempfile().expect("tempfile");
    let fd = file.as_raw_fd();
    drop(file);
    let mut argv = launcher_argv();
    assert!(
        attach_forwarded_fds(
            SandboxType::LinuxSeccomp,
            &mut argv,
            &[ForwardedFd::new(fd, ForwardedFdRole::SnapshotProbe)],
        )
        .is_err()
    );
    assert_eq!(argv, launcher_argv());
}

#[test]
fn roles_round_trip() {
    for role in [
        ForwardedFdRole::SnapshotSource,
        ForwardedFdRole::SnapshotProbe,
    ] {
        assert_eq!(ForwardedFdRole::parse(role.as_str()), Some(role));
    }
    assert_eq!(ForwardedFdRole::parse("anything"), None);
    assert_eq!(ForwardedFdRole::parse("escalate-socket"), None);
}

#[cfg(not(unix))]
#[test]
fn linux_forwarding_without_unix_descriptors_is_refused() {
    let mut argv = launcher_argv();
    let error = attach_forwarded_fds(
        SandboxType::LinuxSeccomp,
        &mut argv,
        &[ForwardedFd::new(42, ForwardedFdRole::SnapshotSource)],
    )
    .expect_err("unsupported forwarding must not silently drop descriptors");
    assert_eq!(error.kind(), io::ErrorKind::Unsupported);
    assert_eq!(argv, launcher_argv());
}
