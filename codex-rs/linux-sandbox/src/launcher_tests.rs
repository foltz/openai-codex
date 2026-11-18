//! Verify system bubblewrap selection and legacy fd mount translation.

use super::*;
use pretty_assertions::assert_eq;
use tempfile::NamedTempFile;

#[test]
fn prefers_system_bwrap_when_help_lists_argv0() {
    let fake_bwrap = NamedTempFile::new().expect("temp file");
    let fake_bwrap_path = fake_bwrap.path();
    let expected = AbsolutePathBuf::from_absolute_path(fake_bwrap_path).expect("absolute");

    assert_eq!(
        system_bwrap_launcher_for_path_with_probe(fake_bwrap_path, |_| {
            Some(SystemBwrapCapabilities {
                supports_argv0: true,
                supports_perms: true,
                supports_ro_bind_fd: true,
            })
        }),
        Some(SystemBwrapLauncher {
            program: expected,
            supports_argv0: true,
            supports_ro_bind_fd: true,
        })
    );
}

#[test]
fn prefers_system_bwrap_when_system_bwrap_lacks_argv0() {
    let fake_bwrap = NamedTempFile::new().expect("temp file");
    let fake_bwrap_path = fake_bwrap.path();

    assert_eq!(
        system_bwrap_launcher_for_path_with_probe(fake_bwrap_path, |_| {
            Some(SystemBwrapCapabilities {
                supports_argv0: false,
                supports_perms: true,
                supports_ro_bind_fd: false,
            })
        }),
        Some(SystemBwrapLauncher {
            program: AbsolutePathBuf::from_absolute_path(fake_bwrap_path).expect("absolute"),
            supports_argv0: false,
            supports_ro_bind_fd: false,
        })
    );
}

#[test]
fn ignores_system_bwrap_when_system_bwrap_lacks_perms() {
    let fake_bwrap = NamedTempFile::new().expect("temp file");

    assert_eq!(
        system_bwrap_launcher_for_path_with_probe(fake_bwrap.path(), |_| {
            Some(SystemBwrapCapabilities {
                supports_argv0: false,
                supports_perms: false,
                supports_ro_bind_fd: false,
            })
        }),
        None
    );
}

#[test]
fn detects_fd_backed_read_only_mount_support_in_system_bwrap_help() {
    let temp_dir = tempfile::tempdir().expect("temp directory");
    let fake_bwrap_path = temp_dir.path().join("bwrap");
    codex_utils_cargo_bin::write_executable(
        &fake_bwrap_path,
        "#!/bin/sh\nprintf '%s\\n' '--as-pid-1' '--perms' '--argv0' '--ro-bind-fd'\n",
    )
    .expect("write fake bubblewrap");

    assert_eq!(
        system_bwrap_capabilities(&fake_bwrap_path),
        Some(SystemBwrapCapabilities {
            supports_argv0: true,
            supports_perms: true,
            supports_ro_bind_fd: true,
        })
    );
}

#[test]
fn translates_fd_mounts_for_legacy_system_bubblewrap() {
    let mut argv = vec![
        "bwrap".to_string(),
        "--ro-bind-fd".to_string(),
        "7".to_string(),
        "/tmp/socket-root".to_string(),
        "--".to_string(),
        "/usr/bin/codex-linux-sandbox".to_string(),
        "--apply-seccomp-then-exec".to_string(),
        "--".to_string(),
        "echo".to_string(),
        "--ro-bind-fd".to_string(),
    ];

    translate_legacy_bwrap_fd_mounts(&mut argv).expect("fd mount should translate");

    assert_eq!(
        argv,
        vec![
            "bwrap".to_string(),
            "--ro-bind".to_string(),
            "/proc/self/fd/7".to_string(),
            "/tmp/socket-root".to_string(),
            "--".to_string(),
            "/usr/bin/codex-linux-sandbox".to_string(),
            "--verify-fd-mount".to_string(),
            "7:/tmp/socket-root".to_string(),
            "--apply-seccomp-then-exec".to_string(),
            "--".to_string(),
            "echo".to_string(),
            "--ro-bind-fd".to_string(),
        ]
    );
}

#[test]
fn translates_multiple_fd_mounts_for_legacy_system_bubblewrap() {
    let mut argv = vec![
        "bwrap".to_string(),
        "--ro-bind-fd".to_string(),
        "7".to_string(),
        "/tmp/first".to_string(),
        "--ro-bind-fd".to_string(),
        "9".to_string(),
        "/tmp/second:with-colon".to_string(),
        "--".to_string(),
        "codex-linux-sandbox".to_string(),
        "--apply-seccomp-then-exec".to_string(),
        "--".to_string(),
        "true".to_string(),
    ];

    translate_legacy_bwrap_fd_mounts(&mut argv).expect("fd mounts should translate");

    assert_eq!(
        argv,
        vec![
            "bwrap".to_string(),
            "--ro-bind".to_string(),
            "/proc/self/fd/7".to_string(),
            "/tmp/first".to_string(),
            "--ro-bind".to_string(),
            "/proc/self/fd/9".to_string(),
            "/tmp/second:with-colon".to_string(),
            "--".to_string(),
            "codex-linux-sandbox".to_string(),
            "--verify-fd-mount".to_string(),
            "7:/tmp/first".to_string(),
            "--verify-fd-mount".to_string(),
            "9:/tmp/second:with-colon".to_string(),
            "--apply-seccomp-then-exec".to_string(),
            "--".to_string(),
            "true".to_string(),
        ]
    );
}

#[test]
fn rejects_malformed_legacy_bubblewrap_fd_mounts() {
    for (fd, destination) in [
        ("invalid", "/tmp/socket-root"),
        ("2", "/tmp/socket-root"),
        ("7", "relative/socket-root"),
    ] {
        let mut argv = vec![
            "bwrap".to_string(),
            "--ro-bind-fd".to_string(),
            fd.to_string(),
            destination.to_string(),
            "--".to_string(),
            "codex-linux-sandbox".to_string(),
        ];

        assert!(translate_legacy_bwrap_fd_mounts(&mut argv).is_err());
    }
}

#[test]
fn rejects_fd_mounts_without_the_trusted_inner_sandbox_stage() {
    let mut argv = vec![
        "bwrap".to_string(),
        "--ro-bind-fd".to_string(),
        "7".to_string(),
        "/tmp/socket-root".to_string(),
        "--".to_string(),
        "/bin/true".to_string(),
        "--".to_string(),
        "--apply-seccomp-then-exec".to_string(),
    ];

    assert!(translate_legacy_bwrap_fd_mounts(&mut argv).is_err());
}

#[test]
fn rejects_duplicate_legacy_bubblewrap_fd_mounts() {
    let mut argv = vec![
        "bwrap".to_string(),
        "--ro-bind-fd".to_string(),
        "7".to_string(),
        "/tmp/first".to_string(),
        "--ro-bind-fd".to_string(),
        "7".to_string(),
        "/tmp/second".to_string(),
        "--".to_string(),
        "codex-linux-sandbox".to_string(),
    ];

    assert!(translate_legacy_bwrap_fd_mounts(&mut argv).is_err());
}

#[test]
fn ignores_system_bwrap_when_system_bwrap_is_missing() {
    assert_eq!(
        system_bwrap_launcher_for_path(Path::new("/definitely/not/a/bwrap")),
        None
    );
}

fn inner_stage_argv() -> Vec<String> {
    vec![
        "bwrap".to_string(),
        "--die-with-parent".to_string(),
        "--".to_string(),
        "/usr/bin/codex-linux-sandbox".to_string(),
        "--apply-seccomp-then-exec".to_string(),
        "--".to_string(),
        "echo".to_string(),
    ]
}

#[test]
fn attaches_parent_pidfd_to_the_inner_sandbox_stage() {
    let mut argv = inner_stage_argv();
    let mut preserved_files = Vec::new();

    attach_bwrap_parent_pidfd(&mut argv, &mut preserved_files, open_own_pidfd)
        .expect("pidfd should attach");

    assert_eq!(preserved_files.len(), 1);
    let pidfd = preserved_files[0].as_raw_fd();
    assert_eq!(
        argv,
        vec![
            "bwrap".to_string(),
            "--die-with-parent".to_string(),
            "--".to_string(),
            "/usr/bin/codex-linux-sandbox".to_string(),
            BWRAP_PARENT_PIDFD_ARG.to_string(),
            pidfd.to_string(),
            "--apply-seccomp-then-exec".to_string(),
            "--".to_string(),
            "echo".to_string(),
        ]
    );
    // SAFETY: querying descriptor flags has no side effects.
    let flags = unsafe { libc::fcntl(pidfd, libc::F_GETFD) };
    assert_eq!(
        flags,
        libc::FD_CLOEXEC,
        "the pidfd must stay close-on-exec until the exec into bubblewrap"
    );
    crate::exec_util::make_files_inheritable(&preserved_files);
    // SAFETY: querying descriptor flags has no side effects.
    let flags = unsafe { libc::fcntl(pidfd, libc::F_GETFD) };
    assert_eq!(flags, 0, "the exec into bubblewrap must inherit the pidfd");
    let mut poll_fd = libc::pollfd {
        fd: pidfd,
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: `poll_fd` is valid for the duration of the call.
    let ready = unsafe { libc::poll(&mut poll_fd, 1, 0) };
    assert_eq!(ready, 0, "the live current process must not poll as exited");
}

#[test]
fn refuses_launch_when_pidfd_open_fails() {
    for errno in [
        libc::EPERM,
        libc::ENOSYS,
        libc::EMFILE,
        libc::ENFILE,
        libc::ESRCH,
        libc::EINVAL,
    ] {
        let mut argv = inner_stage_argv();
        let mut preserved_files = Vec::new();

        let err = attach_bwrap_parent_pidfd(&mut argv, &mut preserved_files, || {
            Err(std::io::Error::from_raw_os_error(errno))
        })
        .expect_err("a pidfd_open failure must refuse the launch");

        assert!(
            err.to_string()
                .contains("failed to open a pidfd for the bubblewrap parent"),
            "unexpected error for errno {errno}: {err}"
        );
        assert_eq!(argv, inner_stage_argv());
        assert!(preserved_files.is_empty());
    }
}

#[test]
fn does_not_attach_parent_pidfd_without_the_inner_sandbox_stage() {
    let original = vec![
        "bwrap".to_string(),
        "--".to_string(),
        "/usr/bin/true".to_string(),
        "--".to_string(),
        "--apply-seccomp-then-exec".to_string(),
    ];
    let mut argv = original.clone();
    let mut preserved_files = Vec::new();

    attach_bwrap_parent_pidfd(&mut argv, &mut preserved_files, || {
        panic!("no pidfd is needed without the inner sandbox stage")
    })
    .expect("argv without the inner stage needs no pidfd");

    assert_eq!(argv, original);
    assert!(preserved_files.is_empty());
}
