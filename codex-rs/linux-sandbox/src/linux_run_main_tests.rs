#[cfg(test)]
use super::*;
#[cfg(test)]
use crate::linux_run_main::install_bwrap_signal_forwarders;
#[cfg(test)]
use crate::linux_run_main::wait_for_bwrap_child;
#[cfg(test)]
use codex_network_proxy::ManagedNetworkSandboxContext;
#[cfg(test)]
use codex_protocol::models::PermissionProfile;
#[cfg(test)]
use codex_protocol::protocol::FileSystemSandboxPolicy;
#[cfg(test)]
use codex_protocol::protocol::NetworkSandboxPolicy;
#[cfg(test)]
use codex_utils_absolute_path::AbsolutePathBuf;
#[cfg(test)]
use pretty_assertions::assert_eq;
#[cfg(test)]
use std::os::fd::IntoRawFd;
use std::os::unix::ffi::OsStrExt;
#[cfg(test)]
use std::os::unix::fs::PermissionsExt;

fn read_only_permission_profile() -> PermissionProfile {
    PermissionProfile::read_only()
}

fn read_only_file_system_policy() -> FileSystemSandboxPolicy {
    read_only_permission_profile().file_system_sandbox_policy()
}

#[test]
fn detects_proc_mount_invalid_argument_failure() {
    let stderr = "bwrap: Can't mount proc on /newroot/proc: Invalid argument";
    assert!(is_proc_mount_failure(stderr));
}

#[test]
fn detects_proc_mount_operation_not_permitted_failure() {
    let stderr = "bwrap: Can't mount proc on /newroot/proc: Operation not permitted";
    assert!(is_proc_mount_failure(stderr));
}

#[test]
fn detects_proc_mount_permission_denied_failure() {
    let stderr = "bwrap: Can't mount proc on /newroot/proc: Permission denied";
    assert!(is_proc_mount_failure(stderr));
}

#[test]
fn ignores_non_proc_mount_errors() {
    let stderr = "bwrap: Can't bind mount /dev/null: Operation not permitted";
    assert!(!is_proc_mount_failure(stderr));
}

#[test]
fn inserts_bwrap_argv0_before_command_separator() {
    let file_system_sandbox_policy = read_only_file_system_policy();
    let mut argv = build_bwrap_argv(
        vec!["/bin/true".to_string()],
        &file_system_sandbox_policy,
        Path::new("/"),
        Path::new("/"),
        BwrapOptions {
            mount_proc: true,
            network_mode: BwrapNetworkMode::FullAccess,
            ..Default::default()
        },
    )
    .expect("build bwrap argv")
    .args;
    apply_inner_command_argv0_for_launcher(
        &mut argv,
        /*supports_argv0*/ true,
        "/tmp/codex-arg0-session/codex-linux-sandbox".to_string(),
    );
    let separator = argv.iter().position(|arg| arg == "--").unwrap();
    assert_eq!(
        &argv[separator - 2..],
        ["--argv0", "codex-linux-sandbox", "--", "/bin/true"]
    );
}

#[test]
fn rewrites_inner_command_path_when_bwrap_lacks_argv0() {
    let file_system_sandbox_policy = read_only_file_system_policy();
    let mut argv = build_bwrap_argv(
        vec!["/bin/true".to_string()],
        &file_system_sandbox_policy,
        Path::new("/"),
        Path::new("/"),
        BwrapOptions {
            mount_proc: true,
            network_mode: BwrapNetworkMode::FullAccess,
            ..Default::default()
        },
    )
    .expect("build bwrap argv")
    .args;
    apply_inner_command_argv0_for_launcher(
        &mut argv,
        /*supports_argv0*/ false,
        "/tmp/codex-arg0-session/codex-linux-sandbox".to_string(),
    );

    assert!(!argv.iter().any(|arg| arg == "--argv0"));
    assert!(
        argv.windows(2)
            .any(|window| { window == ["--", "/tmp/codex-arg0-session/codex-linux-sandbox"] })
    );
}

#[test]
fn rewrites_bwrap_helper_command_not_nested_user_command_when_current_exe_appears_later() {
    let nested_current_exe = std::env::current_exe()
        .expect("current exe")
        .to_string_lossy()
        .into_owned();
    let mut argv = vec![
        "bwrap".to_string(),
        "--".to_string(),
        "/tmp/helper-symlink".to_string(),
        "--sandbox-policy-cwd".to_string(),
        "/tmp/cwd".to_string(),
        "--".to_string(),
        nested_current_exe.clone(),
        "--codex-run-as-apply-patch".to_string(),
        "patch".to_string(),
    ];

    apply_inner_command_argv0_for_launcher(
        &mut argv,
        /*supports_argv0*/ false,
        "/tmp/argv0-fallback-helper".to_string(),
    );

    assert_eq!(
        argv,
        vec![
            "bwrap".to_string(),
            "--".to_string(),
            "/tmp/argv0-fallback-helper".to_string(),
            "--sandbox-policy-cwd".to_string(),
            "/tmp/cwd".to_string(),
            "--".to_string(),
            nested_current_exe,
            "--codex-run-as-apply-patch".to_string(),
            "patch".to_string(),
        ]
    );
}

#[test]
fn inserts_unshare_net_when_network_isolation_requested() {
    let file_system_sandbox_policy = read_only_file_system_policy();
    let argv = build_bwrap_argv(
        vec!["/bin/true".to_string()],
        &file_system_sandbox_policy,
        Path::new("/"),
        Path::new("/"),
        BwrapOptions {
            mount_proc: true,
            network_mode: BwrapNetworkMode::Isolated,
            ..Default::default()
        },
    )
    .expect("build bwrap argv")
    .args;
    assert!(argv.contains(&"--unshare-net".to_string()));
}

#[test]
fn inserts_unshare_net_when_proxy_only_network_mode_requested() {
    let file_system_sandbox_policy = read_only_file_system_policy();
    let argv = build_bwrap_argv(
        vec!["/bin/true".to_string()],
        &file_system_sandbox_policy,
        Path::new("/"),
        Path::new("/"),
        BwrapOptions {
            mount_proc: true,
            network_mode: BwrapNetworkMode::ProxyOnly,
            ..Default::default()
        },
    )
    .expect("build bwrap argv")
    .args;
    assert!(argv.contains(&"--unshare-net".to_string()));
}

#[test]
fn masks_wsl_interop_with_full_network_and_restricted_filesystem() {
    let argv = build_bwrap_argv(
        vec!["/bin/true".to_string()],
        &read_only_file_system_policy(),
        Path::new("/"),
        Path::new("/"),
        BwrapOptions {
            network_mode: BwrapNetworkMode::FullAccess,
            mask_wsl_interop: true,
            ..Default::default()
        },
    )
    .expect("build bwrap argv")
    .args;

    assert!(argv.windows(2).any(|args| args == ["--tmpfs", "/run/WSL"]));
    assert!(!argv.iter().any(|arg| arg == "--unshare-net"));
}

#[test]
fn masks_inherited_procfs_when_wsl_interop_is_masked_without_fresh_proc() {
    let argv = build_bwrap_argv(
        vec!["/bin/true".to_string()],
        &read_only_file_system_policy(),
        Path::new("/"),
        Path::new("/"),
        BwrapOptions {
            mount_proc: false,
            network_mode: BwrapNetworkMode::FullAccess,
            mask_wsl_interop: true,
            ..Default::default()
        },
    )
    .expect("build bwrap argv")
    .args;

    assert!(argv.windows(2).any(|args| args == ["--tmpfs", "/run/WSL"]));
    assert!(argv.windows(2).any(|args| args == ["--tmpfs", "/proc"]));
    assert!(!argv.windows(2).any(|args| args == ["--proc", "/proc"]));
}

#[test]
fn proxy_only_mode_takes_precedence_over_full_network_policy() {
    let mode = bwrap_network_mode(
        NetworkSandboxPolicy::Enabled,
        /*allow_network_for_proxy*/ true,
    );
    assert_eq!(mode, BwrapNetworkMode::ProxyOnly);
}

#[test]
fn split_only_filesystem_policy_requires_direct_runtime_enforcement() {
    let temp_dir = tempfile::TempDir::new().expect("tempdir");
    let docs = temp_dir.path().join("docs");
    std::fs::create_dir_all(&docs).expect("create docs");
    let docs = AbsolutePathBuf::from_absolute_path(&docs).expect("absolute docs");
    let policy = FileSystemSandboxPolicy::restricted(vec![
        codex_protocol::permissions::FileSystemSandboxEntry {
            path: codex_protocol::permissions::FileSystemPath::Special {
                value: codex_protocol::permissions::FileSystemSpecialPath::project_roots(
                    /*subpath*/ None,
                ),
            },
            access: codex_protocol::permissions::FileSystemAccessMode::Write,
            missing_path_behavior: None,
        },
        codex_protocol::permissions::FileSystemSandboxEntry {
            path: docs.into(),
            access: codex_protocol::permissions::FileSystemAccessMode::Read,
            missing_path_behavior: None,
        },
    ]);

    assert!(
        policy.needs_direct_runtime_enforcement(NetworkSandboxPolicy::Restricted, temp_dir.path(),)
    );
}

#[test]
fn root_write_read_only_carveout_requires_direct_runtime_enforcement() {
    let temp_dir = tempfile::TempDir::new().expect("tempdir");
    let docs = temp_dir.path().join("docs");
    std::fs::create_dir_all(&docs).expect("create docs");
    let docs = AbsolutePathBuf::from_absolute_path(&docs).expect("absolute docs");
    let policy = FileSystemSandboxPolicy::restricted(vec![
        codex_protocol::permissions::FileSystemSandboxEntry {
            path: codex_protocol::permissions::FileSystemPath::Special {
                value: codex_protocol::permissions::FileSystemSpecialPath::Root,
            },
            access: codex_protocol::permissions::FileSystemAccessMode::Write,
            missing_path_behavior: None,
        },
        codex_protocol::permissions::FileSystemSandboxEntry {
            path: docs.into(),
            access: codex_protocol::permissions::FileSystemAccessMode::Read,
            missing_path_behavior: None,
        },
    ]);

    assert!(
        policy.needs_direct_runtime_enforcement(NetworkSandboxPolicy::Restricted, temp_dir.path(),)
    );
}

#[test]
fn managed_proxy_preflight_argv_unshares_network() {
    let mode = bwrap_network_mode(
        NetworkSandboxPolicy::Enabled,
        /*allow_network_for_proxy*/ true,
    );
    let argv = build_preflight_bwrap_argv(BwrapOptions {
        network_mode: mode,
        ..Default::default()
    })
    .expect("build preflight argv")
    .args;
    assert!(argv.iter().any(|arg| arg == "--"));
    assert!(argv.iter().any(|arg| arg == "--unshare-net"));
}

#[test]
fn proc_mount_preflight_does_not_bind_the_full_filesystem() {
    let argv = build_preflight_bwrap_argv(BwrapOptions::default())
        .expect("build preflight argv")
        .args;

    assert!(argv.windows(2).any(|window| window == ["--tmpfs", "/"]));
    assert!(argv.windows(2).any(|window| window == ["--proc", "/proc"]));
    assert!(
        !argv
            .windows(3)
            .any(|window| window == ["--ro-bind", "/", "/"])
    );
    assert!(!argv.windows(3).any(|window| window == ["--bind", "/", "/"]));
}

#[test]
fn proc_mount_preflight_preserves_wsl_masks() {
    let argv = build_preflight_bwrap_argv(BwrapOptions {
        mask_wsl_interop: true,
        mask_wslg_distro: true,
        ..Default::default()
    })
    .expect("build WSL preflight argv")
    .args;

    assert!(argv.windows(2).any(|window| window == ["--proc", "/proc"]));
    assert!(
        argv.windows(2)
            .any(|window| window == ["--tmpfs", WSL_INTEROP_DIR])
    );
    assert!(argv.windows(6).any(|window| {
        window
            == [
                "--perms",
                "000",
                "--tmpfs",
                WSLG_DISTRO_ROOT,
                "--remount-ro",
                WSLG_DISTRO_ROOT,
            ]
    }));
}

#[test]
fn synthetic_mount_registry_root_is_the_shared_trusted_registry() {
    // Needs a trusted per-uid base (`/run/user/<uid>` or the passwd home).
    let Ok(expected) = crate::synthetic_targets::shared_registry_path() else {
        return;
    };
    if crate::synthetic_targets::open_shared_registry().is_err() {
        return;
    }
    assert_eq!(synthetic_mount_registry_root(), expected);
}

#[test]
fn bwrap_signal_forwarder_terminates_child_and_keeps_parent_alive() {
    let supervisor_pid = unsafe { libc::fork() };
    assert!(supervisor_pid >= 0, "failed to fork supervisor");

    if supervisor_pid == 0 {
        run_bwrap_signal_forwarder_test_supervisor();
    }

    let status = wait_for_bwrap_child(supervisor_pid);
    assert!(libc::WIFEXITED(status), "supervisor status: {status}");
    assert_eq!(libc::WEXITSTATUS(status), 0);
}

#[cfg(test)]
fn run_bwrap_signal_forwarder_test_supervisor() -> ! {
    let child_pid = unsafe { libc::fork() };
    if child_pid < 0 {
        unsafe {
            libc::_exit(2);
        }
    }

    if child_pid == 0 {
        loop {
            unsafe {
                libc::pause();
            }
        }
    }

    install_bwrap_signal_forwarders(child_pid);
    unsafe {
        libc::raise(libc::SIGTERM);
    }

    let status = wait_for_bwrap_child(child_pid);
    let child_terminated_by_sigterm =
        libc::WIFSIGNALED(status) && libc::WTERMSIG(status) == libc::SIGTERM;
    unsafe {
        libc::_exit(if child_terminated_by_sigterm { 0 } else { 1 });
    }
}

#[test]
fn managed_proxy_inner_command_includes_route_spec() {
    let permission_profile = read_only_permission_profile();
    for managed_network in [
        ManagedNetworkSandboxContext::default(),
        ManagedNetworkSandboxContext {
            loopback_ports: vec![8080, 9090],
            allow_local_binding: true,
            allow_unix_sockets: vec!["/tmp/daemon.sock".to_string()],
            dangerously_allow_all_unix_sockets: true,
        },
    ] {
        let args = build_inner_seccomp_command(InnerSeccompCommandArgs {
            sandbox_policy_cwd: Path::new("/tmp"),
            command_cwd: Some(Path::new("/tmp/link")),
            permission_profile: &permission_profile,
            managed_network: Some(managed_network.clone()),
            proxy_route_spec: Some("{\"routes\":[]}".to_string()),
            command: vec!["/bin/true".to_string()],
        });

        assert!(args.iter().any(|arg| arg == "--proxy-route-spec"));
        assert!(args.iter().any(|arg| arg == "{\"routes\":[]}"));
        let parsed = LinuxSandboxCommand::try_parse_from(args)
            .expect("inner command should preserve the managed network policy");
        assert_eq!(parsed.managed_network, Some(managed_network));
    }
}

#[test]
fn sender_built_custody_argv_parses_into_the_receiver_fields() {
    let custody = codex_sandboxing::SandboxCleanupCustody::for_launch(
        codex_sandboxing::SandboxType::LinuxSeccomp,
        Some(std::time::Instant::now() + std::time::Duration::from_secs(5)),
        None,
    )
    .expect("create sender custody")
    .expect("Linux launch has custody");
    let mut argv = [
        "codex-linux-sandbox",
        "--sandbox-policy-cwd",
        "/tmp",
        "--",
        "/bin/true",
    ]
    .map(str::to_owned)
    .to_vec();
    custody
        .attach_to_launcher_argv(&mut argv)
        .expect("attach sender custody arguments");
    let deadline_index = argv
        .iter()
        .position(|arg| arg == "--cleanup-deadline-monotonic-ns")
        .expect("sender publishes the original deadline");
    let deadline = argv[deadline_index + 1]
        .parse::<u64>()
        .expect("sender deadline is numeric");
    assert!(deadline > 0);
    let parsed = LinuxSandboxCommand::try_parse_from(argv.clone())
        .expect("receiver accepts sender-built custody arguments");
    assert_eq!(parsed.cleanup_deadline_monotonic_ns, Some(deadline));
    let inherited = custody.inherited_fds();
    assert_eq!(inherited.len(), 2);
    assert_eq!(parsed.cleanup_deadline_fd, Some(inherited[0]));
    assert_eq!(parsed.cleanup_report_fd, Some(inherited[1]));
    crate::cleanup_custody::validate_pipe_pair(
        parsed.cleanup_deadline_fd,
        parsed.cleanup_report_fd,
    )
    .expect("sender descriptors satisfy the receiver contract");
    assert_eq!(parsed.command, vec!["/bin/true".to_owned()]);

    argv[deadline_index + 1] = "not-a-deadline".to_owned();
    assert!(LinuxSandboxCommand::try_parse_from(argv).is_err());
}

#[test]
fn managed_network_policy_alone_enables_proxy_mode() {
    let parsed = LinuxSandboxCommand::try_parse_from([
        "codex-linux-sandbox",
        "--sandbox-policy-cwd",
        "/tmp",
        "--managed-network",
        "{}",
        "--",
        "/bin/true",
    ])
    .expect("managed network context should enable proxy mode on its own");
    assert_eq!(
        parsed.managed_network,
        Some(ManagedNetworkSandboxContext::default())
    );
}

#[test]
fn malformed_managed_network_policy_is_rejected() {
    let error = LinuxSandboxCommand::try_parse_from([
        "codex-linux-sandbox",
        "--sandbox-policy-cwd",
        "/tmp",
        "--managed-network",
        "{\"dangerouslyAllowAllUnixSockets\":\"true\"}",
        "--",
        "/bin/true",
    ])
    .expect_err("managed network policy should reject a non-boolean grant");
    assert_eq!(error.kind(), clap::error::ErrorKind::ValueValidation);
}

#[test]
fn inner_command_includes_permission_profile_flag() {
    let permission_profile = read_only_permission_profile();
    let args = build_inner_seccomp_command(InnerSeccompCommandArgs {
        sandbox_policy_cwd: Path::new("/tmp"),
        command_cwd: Some(Path::new("/tmp/link")),
        permission_profile: &permission_profile,
        managed_network: None,
        proxy_route_spec: None,
        command: vec!["/bin/true".to_string()],
    });

    assert!(args.iter().any(|arg| arg == "--permission-profile"));
    assert!(
        args.windows(2)
            .any(|window| { window == ["--command-cwd", "/tmp/link"] })
    );
}

#[test]
fn non_managed_inner_command_omits_route_spec() {
    let permission_profile = read_only_permission_profile();
    let args = build_inner_seccomp_command(InnerSeccompCommandArgs {
        sandbox_policy_cwd: Path::new("/tmp"),
        command_cwd: Some(Path::new("/tmp/link")),
        permission_profile: &permission_profile,
        managed_network: None,
        proxy_route_spec: None,
        command: vec!["/bin/true".to_string()],
    });

    assert!(!args.iter().any(|arg| arg == "--proxy-route-spec"));
    let parsed = LinuxSandboxCommand::try_parse_from(args)
        .expect("unmanaged inner command should preserve ordinary sandbox mode");
    assert_eq!(parsed.managed_network, None);
}

#[test]
fn managed_proxy_inner_command_requires_route_spec() {
    let result = std::panic::catch_unwind(|| {
        let permission_profile = read_only_permission_profile();
        build_inner_seccomp_command(InnerSeccompCommandArgs {
            sandbox_policy_cwd: Path::new("/tmp"),
            command_cwd: Some(Path::new("/tmp/link")),
            permission_profile: &permission_profile,
            managed_network: Some(ManagedNetworkSandboxContext::default()),
            proxy_route_spec: None,
            command: vec!["/bin/true".to_string()],
        })
    });
    assert!(result.is_err());
}

#[test]
fn resolve_permission_profile_derives_runtime_policies() {
    let permission_profile = read_only_permission_profile();
    let resolved = resolve_permission_profile(Some(permission_profile.clone()))
        .expect("profile should resolve");

    assert_eq!(resolved.permission_profile, permission_profile);
    assert_eq!(
        resolved.file_system_sandbox_policy,
        read_only_file_system_policy()
    );
    assert_eq!(
        resolved.network_sandbox_policy,
        NetworkSandboxPolicy::Restricted
    );
}

#[test]
fn resolve_permission_profile_preserves_direct_runtime_profile() {
    let temp_dir = tempfile::TempDir::new().expect("tempdir");
    let docs = temp_dir.path().join("docs");
    std::fs::create_dir_all(&docs).expect("create docs");
    let docs = AbsolutePathBuf::from_absolute_path(&docs).expect("absolute docs");
    let file_system_sandbox_policy = FileSystemSandboxPolicy::restricted(vec![
        codex_protocol::permissions::FileSystemSandboxEntry {
            path: codex_protocol::permissions::FileSystemPath::Special {
                value: codex_protocol::permissions::FileSystemSpecialPath::Root,
            },
            access: codex_protocol::permissions::FileSystemAccessMode::Read,
            missing_path_behavior: None,
        },
        codex_protocol::permissions::FileSystemSandboxEntry {
            path: docs.into(),
            access: codex_protocol::permissions::FileSystemAccessMode::Write,
            missing_path_behavior: None,
        },
    ]);
    let permission_profile = PermissionProfile::from_runtime_permissions(
        &file_system_sandbox_policy,
        NetworkSandboxPolicy::Restricted,
    );
    let resolved = resolve_permission_profile(Some(permission_profile.clone()))
        .expect("profile should resolve");

    assert_eq!(resolved.permission_profile, permission_profile);
    assert_eq!(
        resolved.file_system_sandbox_policy,
        file_system_sandbox_policy
    );
    assert_eq!(
        resolved.network_sandbox_policy,
        NetworkSandboxPolicy::Restricted
    );
}

#[test]
fn resolve_permission_profile_rejects_missing_configuration() {
    let err = resolve_permission_profile(/*permission_profile*/ None)
        .expect_err("missing profile should fail");

    assert_eq!(err, ResolvePermissionProfileError::MissingConfiguration);
}

#[test]
fn apply_seccomp_then_exec_with_legacy_landlock_panics() {
    let result = std::panic::catch_unwind(|| {
        ensure_inner_stage_mode_is_valid(
            /*apply_seccomp_then_exec*/ true, /*use_legacy_landlock*/ true,
        )
    });
    assert!(result.is_err());
}

#[test]
#[should_panic(expected = "filesystem-restricted execution requires bubblewrap")]
fn legacy_landlock_cannot_bypass_daemon_socket_isolation() {
    ensure_legacy_landlock_mode_supports_policy(
        /*use_legacy_landlock*/ true,
        &read_only_file_system_policy(),
    );
}

#[test]
fn valid_inner_stage_modes_do_not_panic() {
    ensure_inner_stage_mode_is_valid(
        /*apply_seccomp_then_exec*/ false, /*use_legacy_landlock*/ false,
    );
    ensure_inner_stage_mode_is_valid(
        /*apply_seccomp_then_exec*/ false, /*use_legacy_landlock*/ true,
    );
    ensure_inner_stage_mode_is_valid(
        /*apply_seccomp_then_exec*/ true, /*use_legacy_landlock*/ false,
    );
}

#[test]
fn accepts_only_pidfds_as_the_bubblewrap_parent_pidfd() {
    // SAFETY: pidfd_open on our own pid returns a new descriptor or -1.
    let raw_pidfd = unsafe { libc::syscall(libc::SYS_pidfd_open, libc::getpid(), 0) };
    assert!(raw_pidfd >= 0, "pidfd_open should be supported");
    // SAFETY: the descriptor was just created and is owned here.
    let pidfd = unsafe { OwnedFd::from_raw_fd(raw_pidfd as libc::c_int) };
    assert_eq!(ensure_is_pidfd(pidfd.as_fd(), pidfd_send_signal_0), Ok(()));

    // SAFETY: eventfd returns a new descriptor or -1.
    let raw_eventfd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC) };
    assert!(raw_eventfd >= 0);
    // SAFETY: the descriptor was just created and is owned here.
    let eventfd = unsafe { OwnedFd::from_raw_fd(raw_eventfd) };
    let proc_dir = File::open(format!("/proc/{}", std::process::id())).expect("open proc dir");
    let regular_file = tempfile::tempfile().expect("create temp file");
    for (name, fd) in [
        ("eventfd", eventfd.as_fd()),
        ("/proc/<pid> directory", proc_dir.as_fd()),
        ("regular file", regular_file.as_fd()),
    ] {
        let err = ensure_is_pidfd(fd, pidfd_send_signal_0).expect_err(name);
        assert!(err.contains("is not a pidfd"), "{name}: {err}");
    }
}

fn own_pidfd() -> OwnedFd {
    // SAFETY: pidfd_open on our own pid returns a new descriptor or -1.
    let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, libc::getpid(), 0) };
    assert!(raw >= 0, "pidfd_open: {}", std::io::Error::last_os_error());
    // SAFETY: the descriptor was just created and is owned here.
    unsafe { OwnedFd::from_raw_fd(raw as libc::c_int) }
}

/// `pidfd_send_signal` stand-in that fails with `errno`.
fn errno_probe(errno: libc::c_int) -> impl FnOnce(BorrowedFd<'_>) -> std::io::Result<()> {
    move |_| Err(std::io::Error::from_raw_os_error(errno))
}

#[test]
fn accepts_documented_pidfd_send_signal_outcomes() {
    let pidfd = own_pidfd();
    assert_eq!(ensure_is_pidfd(pidfd.as_fd(), |_| Ok(())), Ok(()));
    for errno in [libc::ESRCH, libc::EPERM, libc::EINVAL] {
        assert_eq!(
            ensure_is_pidfd(pidfd.as_fd(), errno_probe(errno)),
            Ok(()),
            "errno {errno}"
        );
    }
}

#[test]
fn accepts_a_pidfd_for_a_reaped_process() {
    // SAFETY: the child only calls `_exit`.
    let child = unsafe { libc::fork() };
    assert!(child >= 0);
    if child == 0 {
        unsafe { libc::_exit(0) };
    }
    // SAFETY: pidfd_open returns a new descriptor or -1; the unreaped child
    // still exists.
    let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, child, 0) };
    assert!(raw >= 0, "pidfd_open: {}", std::io::Error::last_os_error());
    // SAFETY: the descriptor was just created and is owned here.
    let pidfd = unsafe { OwnedFd::from_raw_fd(raw as libc::c_int) };
    let mut status = 0;
    assert_eq!(unsafe { libc::waitpid(child, &mut status, 0) }, child);

    let observed = pidfd_send_signal_0(pidfd.as_fd()).expect_err("reaped");
    assert_eq!(observed.raw_os_error(), Some(libc::ESRCH));
    assert_eq!(ensure_is_pidfd(pidfd.as_fd(), pidfd_send_signal_0), Ok(()));
}

#[test]
fn refuses_pidfd_validation_that_proves_nothing() {
    let pidfd = own_pidfd();
    for errno in [
        libc::ENOSYS,
        libc::EFAULT,
        libc::EACCES,
        libc::EAGAIN,
        libc::ENOMEM,
    ] {
        let err = ensure_is_pidfd(pidfd.as_fd(), errno_probe(errno))
            .expect_err("unexpected errno must refuse");
        assert!(
            err.contains("validation unobservable"),
            "errno {errno}: {err}"
        );
    }
    let err = ensure_is_pidfd(pidfd.as_fd(), |_| Err(std::io::Error::other("no errno")))
        .expect_err("error without errno must refuse");
    assert!(err.contains("validation unobservable"), "{err}");

    let err =
        ensure_is_pidfd(pidfd.as_fd(), errno_probe(libc::EBADF)).expect_err("EBADF must refuse");
    assert!(err.contains("is not a pidfd"), "{err}");
}

#[test]
fn filesystem_magic_is_required_even_when_the_signal_probe_succeeds() {
    let proc_dir = File::open(format!("/proc/{}", std::process::id())).expect("open proc dir");
    let regular_file = tempfile::tempfile().expect("create temp file");
    for (name, fd) in [
        ("/proc/<pid> directory", proc_dir.as_fd()),
        ("regular file", regular_file.as_fd()),
    ] {
        let mut probed = false;
        let err = ensure_is_pidfd(fd, |_| {
            probed = true;
            Ok(())
        })
        .expect_err(name);
        assert!(err.contains("filesystem magic"), "{name}: {err}");
        assert!(!probed, "{name}: signal probe must not run");
    }
}

/// A pidfd for a child that stays an unreaped zombie until [`reap`], so no
/// descriptor opened by another test refers to the same pid (or, on pidfs, the
/// same inode).
fn zombie_child_pidfd() -> (libc::pid_t, OwnedFd) {
    // SAFETY: the child only calls `_exit`.
    let child = unsafe { libc::fork() };
    assert!(child >= 0);
    if child == 0 {
        unsafe { libc::_exit(0) };
    }
    // SAFETY: pidfd_open returns a new descriptor or -1; the unreaped child
    // still exists.
    let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, child, 0) };
    assert!(raw >= 0, "pidfd_open: {}", std::io::Error::last_os_error());
    // SAFETY: the descriptor was just created and is owned here.
    (child, unsafe { OwnedFd::from_raw_fd(raw as libc::c_int) })
}

fn reap(child: libc::pid_t) {
    let mut status = 0;
    assert_eq!(unsafe { libc::waitpid(child, &mut status, 0) }, child);
}

/// `(st_dev, st_ino, fdinfo Pid)`. All pidfds share one inode on kernels
/// without pidfs, so the pidfd's `Pid` is part of the identity.
type FdIdentity = (libc::dev_t, libc::ino_t, Option<String>);

/// Identity of the object behind `fd`, or `None` when `fd` is closed.
fn fd_identity(fd: libc::c_int) -> Option<FdIdentity> {
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: `stat` is valid writable storage for the call.
    if unsafe { libc::fstat(fd, &mut stat) } < 0 {
        let err = std::io::Error::last_os_error();
        assert_eq!(err.raw_os_error(), Some(libc::EBADF), "fstat {fd}: {err}");
        return None;
    }
    let pid = fs::read_to_string(format!("/proc/self/fdinfo/{fd}"))
        .ok()
        .and_then(|info| {
            info.lines()
                .find_map(|line| line.strip_prefix("Pid:"))
                .map(|pid| pid.trim().to_string())
        });
    Some((stat.st_dev, stat.st_ino, pid))
}

/// The refused descriptor must be gone. Other test threads may already have
/// reused its number, so a different object behind it also counts as closed;
/// only the original identity means the descriptor was kept.
fn assert_released(fd: libc::c_int, identity: &FdIdentity) {
    assert_ne!(
        fd_identity(fd).as_ref(),
        Some(identity),
        "descriptor {fd} was kept open"
    );
}

#[test]
fn refused_bubblewrap_parent_pidfd_is_closed() {
    for (errno, expected) in [
        (libc::ENOSYS, "validation unobservable"),
        (libc::EFAULT, "validation unobservable"),
        (libc::EBADF, "is not a pidfd"),
    ] {
        let (child, pidfd) = zombie_child_pidfd();
        let fd = pidfd.into_raw_fd();
        let identity = fd_identity(fd).expect("open pidfd");
        let err = adopt_bwrap_parent_pidfd_with(fd, errno_probe(errno)).expect_err("must refuse");
        assert!(err.contains(expected), "errno {errno}: {err}");
        assert_released(fd, &identity);
        reap(child);
    }

    let fd = tempfile::tempfile()
        .expect("create temp file")
        .into_raw_fd();
    let identity = fd_identity(fd).expect("open file");
    let err = adopt_bwrap_parent_pidfd_with(fd, |_| Ok(())).expect_err("regular file");
    assert!(err.contains("filesystem magic"), "{err}");
    assert_released(fd, &identity);

    // Above any descriptor table limit, so no other test can own it.
    let err =
        adopt_bwrap_parent_pidfd_with(libc::c_int::MAX, |_| Ok(())).expect_err("closed descriptor");
    assert!(err.contains("is not open"), "{err}");
}

#[test]
fn adopted_bubblewrap_parent_pidfd_is_owned_and_close_on_exec() {
    let (child, pidfd) = zombie_child_pidfd();
    let fd = pidfd.into_raw_fd();
    let identity = fd_identity(fd).expect("open pidfd");
    // The launcher hands the descriptor over inheritable.
    assert_eq!(unsafe { libc::fcntl(fd, libc::F_SETFD, 0) }, 0);
    let pidfd = adopt_bwrap_parent_pidfd_with(fd, pidfd_send_signal_0).expect("child pidfd");
    assert_eq!(pidfd.as_raw_fd(), fd);
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    assert_eq!(flags & libc::FD_CLOEXEC, libc::FD_CLOEXEC);
    drop(pidfd);
    assert_released(fd, &identity);
    reap(child);
}

#[test]
fn rejects_standard_descriptors_as_the_bubblewrap_parent_pidfd() {
    for value in ["0", "1", "2", "-1", "x", ""] {
        assert!(parse_bwrap_parent_pidfd(value).is_err(), "{value:?}");
    }
    assert_eq!(parse_bwrap_parent_pidfd("7"), Ok(7));
}

fn cloexec_pipe() -> (libc::c_int, libc::c_int) {
    let mut fds = [0; 2];
    // SAFETY: `fds` provides storage for the two descriptors.
    assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
    (fds[0], fds[1])
}

/// Read everything the command child reported, failing if it stays alive.
/// Every read polls first, all within one [`TEST_WAIT`] deadline.
fn read_child_report(report_read: libc::c_int) -> Vec<u8> {
    let mut report = Vec::new();
    let deadline = std::time::Instant::now() + TEST_WAIT;
    loop {
        // SAFETY: the caller owns `report_read` for the duration of the call.
        let fd = unsafe { BorrowedFd::borrow_raw(report_read) };
        assert!(
            wait_readable(
                fd,
                deadline.saturating_duration_since(std::time::Instant::now())
            ),
            "command child should exit: {report:?}"
        );
        let mut byte = [0_u8; 1];
        // SAFETY: `byte` is valid writable storage for one byte.
        let read = unsafe { libc::read(report_read, byte.as_mut_ptr().cast(), 1) };
        assert!(read >= 0);
        if read == 0 {
            return report;
        }
        report.push(byte[0]);
    }
}

#[test]
fn command_child_exits_when_parent_dies_before_death_signal_is_armed() {
    let (barrier_read, barrier_write) = cloexec_pipe();
    let (report_read, report_write) = cloexec_pipe();
    // SAFETY: the forked processes only call async-signal-safe libc functions.
    let parent = unsafe { libc::fork() };
    assert!(parent >= 0);
    if parent == 0 {
        unsafe {
            let parent_pid = libc::getpid();
            if libc::fork() == 0 {
                libc::close(barrier_write);
                libc::close(report_read);
                kill_with_parent_after(parent_pid, || {
                    // Barrier: the test releases it only after the parent died.
                    let mut byte = [0_u8; 1];
                    libc::read(barrier_read, byte.as_mut_ptr().cast(), 1);
                });
                libc::write(report_write, b"survived".as_ptr().cast(), 8);
                libc::_exit(0);
            }
            libc::_exit(0);
        }
    }
    unsafe {
        libc::close(barrier_read);
        libc::close(report_write);
        let mut status = 0;
        assert_eq!(libc::waitpid(parent, &mut status, 0), parent);
        assert_eq!(libc::write(barrier_write, b"x".as_ptr().cast(), 1), 1);
        libc::close(barrier_write);
    }

    assert_eq!(read_child_report(report_read), Vec::<u8>::new());
    unsafe { libc::close(report_read) };
}

#[test]
fn command_child_dies_with_its_parent_after_death_signal_is_armed() {
    let (barrier_read, barrier_write) = cloexec_pipe();
    let (report_read, report_write) = cloexec_pipe();
    // SAFETY: the forked processes only call async-signal-safe libc functions.
    let parent = unsafe { libc::fork() };
    assert!(parent >= 0);
    if parent == 0 {
        unsafe {
            let parent_pid = libc::getpid();
            let child = libc::fork();
            if child == 0 {
                libc::close(barrier_write);
                libc::close(report_read);
                kill_with_parent(parent_pid);
                libc::write(report_write, b"armed".as_ptr().cast(), 5);
                // Block until the parent-death signal kills this process.
                let mut byte = [0_u8; 1];
                libc::read(barrier_read, byte.as_mut_ptr().cast(), 1);
                libc::write(report_write, b"survived".as_ptr().cast(), 8);
                libc::_exit(0);
            }
            let mut status = 0;
            libc::waitpid(child, &mut status, 0);
            libc::_exit(0);
        }
    }
    unsafe {
        libc::close(barrier_read);
        libc::close(report_write);
        let mut armed = [0_u8; 5];
        assert!(
            wait_readable(BorrowedFd::borrow_raw(report_read), TEST_WAIT),
            "the command child never reported it was armed"
        );
        let read = libc::read(report_read, armed.as_mut_ptr().cast(), armed.len());
        assert_eq!(read, 5);
        assert_eq!(&armed, b"armed");
        assert_eq!(libc::kill(parent, libc::SIGKILL), 0);
        let mut status = 0;
        assert_eq!(libc::waitpid(parent, &mut status, 0), parent);
    }

    assert_eq!(read_child_report(report_read), Vec::<u8>::new());
    unsafe {
        libc::close(barrier_write);
        libc::close(report_read);
    }
}

/// A symlinked registry, injected as this thread's registry base (never the
/// real per-uid registry), is refused by both launch entry points: argv
/// construction refuses when a writable root exposes the registry, and
/// registration refuses before any synthetic target is installed. Nothing is
/// written through the symlink and no target is created.
#[test]
fn a_symlinked_registry_is_refused_before_argv_or_registration() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let elsewhere = temp.path().join("elsewhere");
    let base = temp.path().join("base");
    let real_base = temp.path().join("real-base");
    let workspace = temp.path().join("workspace");
    for dir in [&elsewhere, &base, &real_base, &workspace] {
        std::fs::create_dir(dir).expect("create fixture dir");
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
            .expect("chmod fixture dir");
    }
    // The registry itself is a symlink to an otherwise acceptable directory.
    std::os::unix::fs::symlink(&elsewhere, base.join("codex-sandbox-registry"))
        .expect("symlink registry");
    // The registry base is a symlink to a base holding a valid registry.
    let linked_base = temp.path().join("linked-base");
    std::fs::create_dir(real_base.join("codex-sandbox-registry")).expect("real registry");
    std::fs::set_permissions(
        real_base.join("codex-sandbox-registry"),
        std::fs::Permissions::from_mode(0o700),
    )
    .expect("chmod real registry");
    std::os::unix::fs::symlink(&real_base, &linked_base).expect("symlink base");

    let workspace_root = AbsolutePathBuf::try_from(temp.path()).expect("absolute temp root");
    let policy = PermissionProfile::workspace_write_with(
        std::slice::from_ref(&workspace_root),
        NetworkSandboxPolicy::Restricted,
        /*exclude_tmpdir_env_var*/ true,
        /*exclude_slash_tmp*/ true,
    )
    .file_system_sandbox_policy();
    let launch = crate::synthetic_targets::ProcessToken::current().expect("launch token");
    let target =
        crate::bwrap::SyntheticMountTarget::missing_empty_directory(&workspace.join(".git"));

    for registry_base in [&base, &linked_base] {
        crate::synthetic_targets::with_test_registry_base(registry_base, || {
            let registry = crate::synthetic_targets::shared_registry_path().expect("path");
            assert!(registry.starts_with(temp.path()), "{}", registry.display());
            assert!(
                crate::synthetic_targets::open_shared_registry().is_err(),
                "a symlinked registry must not verify"
            );
            build_bwrap_argv(
                vec!["/bin/true".to_string()],
                &policy,
                &workspace,
                &workspace,
                BwrapOptions::default(),
            )
            .expect_err("argv construction must refuse an exposed symlinked registry");
            let refused = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                install_synthetic_mount_targets(
                    std::slice::from_ref(&target),
                    &crate::synthetic_targets::LaunchIdentity::single(launch.clone()),
                    /*pid_namespace*/ true,
                )
            }))
            .expect_err("registration must refuse a symlinked registry");
            let message = refused
                .downcast_ref::<String>()
                .map(String::as_str)
                .unwrap_or_default();
            assert!(
                message.contains("synthetic mount targets need the trusted sandbox registry"),
                "{message}"
            );
        });
    }

    let written = |dir: &Path| {
        std::fs::read_dir(dir)
            .expect("read fixture dir")
            .map(|entry| entry.expect("entry").file_name())
            .collect::<Vec<_>>()
    };
    assert_eq!(written(&elsewhere), Vec::<std::ffi::OsString>::new());
    assert_eq!(
        written(&real_base.join("codex-sandbox-registry")),
        Vec::<std::ffi::OsString>::new()
    );
    assert!(!workspace.join(".git").exists());
}

/// A launch without synthetic targets never opens or
/// creates the registry: with an injected registry base that does not exist,
/// registering and cleaning up nothing succeeds and creates nothing.
#[test]
fn a_launch_without_targets_needs_no_registry() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let absent_base = temp.path().join("absent");
    crate::synthetic_targets::with_test_registry_base(&absent_base, || {
        assert!(crate::synthetic_targets::open_shared_registry().is_err());
        let mut custody = crate::synthetic_targets::LaunchCustody {
            deadline: crate::cleanup_custody::CleanupDeadline::none(),
            report: None,
        };
        report_no_targets(&mut custody);
    });
    assert!(!absent_base.exists());
}

// ---------------------------------------------------------------------------
// Keeper launches: a forked launcher, real bubblewrap, a private registry
// (never the real one), and the test-only gap hook.
// ---------------------------------------------------------------------------

/// The keeper tests share the process-wide gap hook.
static KEEPER_TESTS: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// The cap on any single wait in the keeper fixtures. Their passing runs
/// finish in 0.02-0.2 s, so 10 s is a margin of 50x or more on the slowest.
const TEST_WAIT: Duration = Duration::from_secs(10);
/// The whole budget of one keeper launch's waits, shared from its start:
/// every launch-bound wait takes the smaller of `TEST_WAIT` and what is left
/// of it. With the failure guards' own bounded waits (`GUARD_WAIT` each),
/// one test stays well under nextest's 60 s outer timeout (30 s period,
/// terminated after 2), so a failure surfaces as an assertion and the guards
/// run.
const KEEPER_TEST_BUDGET: Duration = Duration::from_secs(40);
/// The failure guards' bounded wait for what they killed to exit.
const GUARD_WAIT: Duration = Duration::from_secs(5);

struct KeeperLaunch {
    launcher: libc::pid_t,
    launcher_token: crate::synthetic_targets::ProcessToken,
    gaps: OwnedFd,
    resume: OwnedFd,
    stdin: Option<File>,
    stdout: File,
    stderr: File,
    report: File,
    targets: Vec<PathBuf>,
    registry_base: tempfile::TempDir,
    _workspace: tempfile::TempDir,
    /// The owner's tightening channel, for owner-bounded launches.
    deadline_write: Option<File>,
    /// The original owner deadline (`CLOCK_MONOTONIC` ns), if owned.
    owner_deadline_ns: Option<u64>,
    /// The end of this launch's shared wait budget ([`KEEPER_TEST_BUDGET`]).
    budget_ends: std::time::Instant,
    /// The launcher (this test's own child), pinned at its fork.
    launcher_pidfd: OwnedFd,
}

/// Sends `signal` through a pidfd: never to a pid that could have been
/// reused.
fn pidfd_kill(pidfd: BorrowedFd<'_>, signal: libc::c_int) {
    // SAFETY: a null siginfo with flags 0 is the kill(2)-like form.
    unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            pidfd.as_raw_fd(),
            signal,
            std::ptr::null::<libc::siginfo_t>(),
            0,
        );
    }
}

/// Failure cleanup for a launch the test did not see through. Runs after
/// [`Pinned`]'s cleanup (the pinned processes are declared later, so they
/// are dropped first), and after every assertion on the success path: it
/// only acts on what is still alive. It closes the test's stdin writer, kills
/// the launcher if it still lives (through its pidfd), and reaps it within a
/// bounded wait (a launcher the test already reaped reports `ECHILD`).
impl Drop for KeeperLaunch {
    fn drop(&mut self) {
        drop(self.stdin.take());
        if !wait_readable(self.launcher_pidfd.as_fd(), Duration::ZERO) {
            pidfd_kill(self.launcher_pidfd.as_fd(), libc::SIGKILL);
            eprintln!(
                "keeper test cleanup: SIGKILLed the still-live launcher {}",
                self.launcher
            );
        }
        if wait_readable(self.launcher_pidfd.as_fd(), GUARD_WAIT) {
            // SAFETY: zeroed siginfo is a valid out-parameter.
            let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
            // SAFETY: reaps exactly the pinned child, never blocking.
            unsafe {
                libc::waitid(
                    libc::P_PIDFD,
                    self.launcher_pidfd.as_raw_fd() as libc::id_t,
                    &mut info,
                    libc::WEXITED | libc::WNOHANG,
                );
            }
        }
    }
}

fn cloexec_pipe_pair() -> (OwnedFd, OwnedFd) {
    let mut fds = [-1; 2];
    // SAFETY: valid two-element array.
    assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
    // SAFETY: pipe2 returned two new owned descriptors.
    unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) }
}

/// Why the keeper tests that run real bubblewrap are ignored by default:
/// they are run explicitly on the qualification host, where an unusable
/// bubblewrap fails them; they never pass by skipping.
const NEEDS_BWRAP: &str = "needs a usable bubblewrap";

/// Starts a keeper launch of `/bin/sh -c script` in a forked launcher, with
/// an owner deadline 120 s away (and its tightening channel) when `owned`,
/// and the gap hook blocking at `stop_at`. Panics when bubblewrap is unusable
/// on this host: such a run is a failure, not a pass.
fn start_keeper_launch(
    script: &str,
    owned: bool,
    stop_at: Option<keeper_gaps::Gap>,
) -> KeeperLaunch {
    start_keeper_launch_with(
        &KeeperCommand::Shell(script.to_string()),
        owned.then_some(Duration::from_secs(120)),
        stop_at,
    )
}

/// What the sandbox runs as its PID 1.
enum KeeperCommand {
    /// `/bin/sh -c script` directly (the lib custody fixtures).
    Shell(String),
    /// The real inner-stage helper (`--apply-seccomp-then-exec`, as
    /// production passes it) at `binary`, wrapping `command`.
    RealInnerStage {
        binary: PathBuf,
        command: Vec<String>,
    },
}

/// [`start_keeper_launch`] with an explicit sandbox command and, if owned,
/// the owner's budget: the original deadline is set to that much after the
/// moment the launcher is forked (as an owner sets it at spawn), and
/// recorded in [`KeeperLaunch::owner_deadline_ns`].
fn start_keeper_launch_with(
    command: &KeeperCommand,
    owner_budget: Option<Duration>,
    stop_at: Option<keeper_gaps::Gap>,
) -> KeeperLaunch {
    start_keeper_launch_owned_by(
        command,
        owner_budget.map_or(TestOwner::None, TestOwner::Deadline),
        stop_at,
    )
}

/// How the test owns a launch.
enum TestOwner {
    /// No owner: owner-less custody.
    None,
    /// An owner channel and an original deadline this long after the fork.
    Deadline(Duration),
    /// An owner channel with no deadline ever issued (dropping
    /// [`KeeperLaunch::deadline_write`] is the owner's death).
    ChannelWithoutDeadline,
}

fn start_keeper_launch_owned_by(
    command: &KeeperCommand,
    owner: TestOwner,
    stop_at: Option<keeper_gaps::Gap>,
) -> KeeperLaunch {
    let budget_ends = std::time::Instant::now() + KEEPER_TEST_BUDGET;
    let registry_base = tempfile::TempDir::new().expect("registry base");
    fs::set_permissions(registry_base.path(), fs::Permissions::from_mode(0o700))
        .expect("chmod registry base");
    let workspace = tempfile::TempDir::new().expect("workspace");
    let workspace_root = AbsolutePathBuf::try_from(workspace.path()).expect("absolute workspace");
    let profile = PermissionProfile::workspace_write_with(
        std::slice::from_ref(&workspace_root),
        NetworkSandboxPolicy::Restricted,
        /*exclude_tmpdir_env_var*/ true,
        /*exclude_slash_tmp*/ true,
    );
    let policy = profile.file_system_sandbox_policy();
    initialize_bwrap_launcher(&policy, workspace.path());
    let base = registry_base.path().to_path_buf();
    let probe = crate::synthetic_targets::with_test_registry_base(&base, || {
        run_bwrap_in_child_capture_stderr(
            build_preflight_bwrap_argv(BwrapOptions::default()).expect("probe argv"),
        )
    });
    let mount_proc = !is_proc_mount_failure(&probe);
    assert!(
        !mount_proc || probe.trim().is_empty(),
        "{NEEDS_BWRAP}, but bubblewrap is unusable here: {probe}"
    );
    let inner = match command {
        KeeperCommand::Shell(script) => {
            vec!["/bin/sh".to_string(), "-c".to_string(), script.clone()]
        }
        KeeperCommand::RealInnerStage { binary, command } => {
            // Exactly the inner argv production builds, with the freshly
            // built helper in place of this test binary.
            let mut inner = build_inner_seccomp_command(InnerSeccompCommandArgs {
                sandbox_policy_cwd: workspace.path(),
                command_cwd: None,
                permission_profile: &profile,
                managed_network: None,
                proxy_route_spec: None,
                command: command.clone(),
            });
            inner[0] = binary.to_string_lossy().into_owned();
            inner
        }
    };
    let bwrap_args = crate::synthetic_targets::with_test_registry_base(&base, || {
        build_bwrap_argv(
            inner,
            &policy,
            workspace.path(),
            workspace.path(),
            BwrapOptions {
                mount_proc,
                network_mode: BwrapNetworkMode::Isolated,
                ..Default::default()
            },
        )
        .expect("bubblewrap argv")
    });
    assert!(
        !bwrap_args.synthetic_mount_targets.is_empty(),
        "the workspace must need synthetic mount targets"
    );
    let targets = bwrap_args
        .synthetic_mount_targets
        .iter()
        .map(|target| target.path().to_path_buf())
        .collect::<Vec<_>>();
    for target in &targets {
        assert!(
            !target.exists(),
            "{} exists before the launch",
            target.display()
        );
    }

    let (stdin_read, stdin_write) = cloexec_pipe_pair();
    let (stdout_read, stdout_write) = cloexec_pipe_pair();
    let (stderr_read, stderr_write) = cloexec_pipe_pair();
    let (gaps_read, gaps_write) = cloexec_pipe_pair();
    let (resume_read, resume_write) = cloexec_pipe_pair();
    let (report_read, report_write) = cloexec_pipe_pair();
    keeper_gaps::REPORT_FD.store(gaps_write.as_raw_fd(), Ordering::SeqCst);
    keeper_gaps::RESUME_FD.store(resume_read.as_raw_fd(), Ordering::SeqCst);
    keeper_gaps::STOP_AT.store(stop_at.map_or(0, |gap| gap as i32), Ordering::SeqCst);
    let owner_deadline_ns = match owner {
        TestOwner::Deadline(budget) => Some(
            crate::cleanup_custody::monotonic_now_ns()
                + u64::try_from(budget.as_nanos()).unwrap_or(u64::MAX),
        ),
        TestOwner::None | TestOwner::ChannelWithoutDeadline => None,
    };
    let (deadline, deadline_write) = match owner {
        TestOwner::None => (crate::cleanup_custody::CleanupDeadline::none(), None),
        TestOwner::Deadline(_) | TestOwner::ChannelWithoutDeadline => {
            let (channel, write) = cloexec_pipe_pair();
            let deadline =
                crate::cleanup_custody::CleanupDeadline::new(owner_deadline_ns, Some(channel))
                    .expect("owner deadline");
            (deadline, Some(File::from(write)))
        }
    };
    let custody = crate::synthetic_targets::LaunchCustody {
        deadline,
        report: Some(report_write),
    };

    // SAFETY: the child becomes the launcher and never returns.
    let launcher = unsafe { libc::fork() };
    assert!(launcher >= 0, "fork the launcher");
    if launcher == 0 {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            // A raw fork does not honour close-on-exec: the launcher drops
            // every end that belongs to the test (the owner's deadline write
            // end, the stdin writer, the output and report readers, the gap
            // reader and the resume writer), so none of them is pinned.
            drop((
                stdin_write,
                stdout_read,
                stderr_read,
                gaps_read,
                resume_write,
                report_read,
                deadline_write,
            ));
            // SAFETY: plain dup2 onto the standard descriptors.
            unsafe {
                libc::dup2(stdin_read.as_raw_fd(), 0);
                libc::dup2(stdout_write.as_raw_fd(), 1);
                libc::dup2(stderr_write.as_raw_fd(), 2);
            }
            drop((stdin_read, stdout_write, stderr_write));
            crate::synthetic_targets::with_test_registry_base(&base, || {
                run_bwrap_through_keeper(bwrap_args, custody);
            });
        }));
        // SAFETY: never return into the test harness.
        unsafe { libc::_exit(101) }
    }
    keeper_gaps::REPORT_FD.store(-1, Ordering::SeqCst);
    keeper_gaps::RESUME_FD.store(-1, Ordering::SeqCst);
    keeper_gaps::STOP_AT.store(0, Ordering::SeqCst);
    let launcher_token =
        crate::synthetic_targets::ProcessToken::for_pid(launcher).expect("launcher token");
    drop((
        stdin_read,
        stdout_write,
        stderr_write,
        gaps_write,
        resume_read,
    ));
    let launcher_pidfd = pin(launcher);
    KeeperLaunch {
        launcher,
        launcher_pidfd,
        launcher_token,
        gaps: gaps_read,
        resume: resume_write,
        stdin: Some(File::from(stdin_write)),
        stdout: File::from(stdout_read),
        stderr: File::from(stderr_read),
        report: File::from(report_read),
        targets,
        registry_base,
        _workspace: workspace,
        deadline_write,
        owner_deadline_ns,
        budget_ends,
    }
}

/// Reads `file` (a test-side read end) through EOF, polling before each
/// single read, all by `deadline`; a read after a readable poll never
/// blocks. At the deadline it fails with what arrived so far.
fn read_to_eof_by(file: &mut File, deadline: std::time::Instant, what: &str) -> String {
    let mut bytes = Vec::new();
    loop {
        assert!(
            wait_readable(
                file.as_fd(),
                deadline.saturating_duration_since(std::time::Instant::now())
            ),
            "{what} did not reach EOF in time; so far: {}",
            String::from_utf8_lossy(&bytes)
        );
        let mut chunk = [0_u8; 4096];
        let read = file.read(&mut chunk).expect(what);
        if read == 0 {
            return String::from_utf8(bytes).expect(what);
        }
        bytes.extend_from_slice(&chunk[..read]);
    }
}

fn wait_readable(fd: BorrowedFd<'_>, timeout: Duration) -> bool {
    let mut poll_fd = libc::pollfd {
        fd: fd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    let millis = libc::c_int::try_from(timeout.as_millis()).unwrap_or(libc::c_int::MAX);
    // SAFETY: one valid pollfd.
    unsafe { libc::poll(&mut poll_fd, 1, millis) > 0 }
}

fn pin(pid: libc::pid_t) -> OwnedFd {
    // SAFETY: pidfd_open returns a new descriptor or fails.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    assert!(
        fd >= 0,
        "pidfd_open({pid}): {}",
        std::io::Error::last_os_error()
    );
    // SAFETY: a new owned descriptor.
    unsafe { OwnedFd::from_raw_fd(fd as libc::c_int) }
}

/// The processes a keeper test pinned for the physical join, each as the
/// gap that reported it, its pid and its pidfd, in pinning order (the keeper,
/// bubblewrap, the init, then the command).
///
/// It is also the failure cleanup: when dropped, after the test's assertions
/// or while unwinding from a failed one, it SIGKILLs, through each pidfd,
/// every pinned process that is still alive, newest first (the command, the
/// init, bubblewrap, the keeper), then confirms within a bounded wait that
/// each has exited and reports any survivor. On a passing run every process
/// already exited, so it does nothing.
#[derive(Default)]
struct Pinned(Vec<(i32, libc::pid_t, OwnedFd)>);

impl std::ops::Deref for Pinned {
    type Target = Vec<(i32, libc::pid_t, OwnedFd)>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Pinned {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'a> IntoIterator for &'a Pinned {
    type Item = &'a (i32, libc::pid_t, OwnedFd);
    type IntoIter = std::slice::Iter<'a, (i32, libc::pid_t, OwnedFd)>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl Drop for Pinned {
    fn drop(&mut self) {
        let mut killed = Vec::new();
        for (_, pid, pidfd) in self.0.iter().rev() {
            if !wait_readable(pidfd.as_fd(), Duration::ZERO) {
                pidfd_kill(pidfd.as_fd(), libc::SIGKILL);
                killed.push(*pid);
            }
        }
        if killed.is_empty() {
            return;
        }
        // One shared bound for all of them.
        let exits_by = std::time::Instant::now() + GUARD_WAIT;
        let survivors: Vec<libc::pid_t> = self
            .0
            .iter()
            .filter(|(_, _, pidfd)| {
                !wait_readable(
                    pidfd.as_fd(),
                    exits_by.saturating_duration_since(std::time::Instant::now()),
                )
            })
            .map(|(_, pid, _)| *pid)
            .collect();
        eprintln!(
            "keeper test cleanup: SIGKILLed still-live pinned processes {killed:?} (command, init, bubblewrap, keeper order); still alive afterwards: {survivors:?}"
        );
    }
}

impl KeeperLaunch {
    /// The next gap report `(gap, pid)`; `None` at EOF (every writer exited)
    /// or when nothing arrives within `timeout`.
    fn try_next_gap(&self, timeout: Duration) -> Option<(i32, libc::pid_t)> {
        if !wait_readable(self.gaps.as_fd(), timeout) {
            return None;
        }
        let mut record = [0_i32; 2];
        // SAFETY: a valid buffer of two i32s.
        let read = unsafe {
            libc::read(
                self.gaps.as_raw_fd(),
                record.as_mut_ptr().cast(),
                std::mem::size_of_val(&record),
            )
        };
        (read == std::mem::size_of_val(&record) as isize).then_some((record[0], record[1]))
    }

    fn pin_reported(pinned: &mut Pinned, reached: i32, pid: libc::pid_t) {
        if pid > 0 && !pinned.iter().any(|(_, known, _)| *known == pid) {
            pinned.push((reached, pid, pin(pid)));
        }
    }

    /// Reads gap reports up to and including `gap`, pinning every reported
    /// process for the physical join.
    fn pin_through(&self, gap: keeper_gaps::Gap, pinned: &mut Pinned) {
        loop {
            let (reached, pid) = self
                .try_next_gap(self.bounded(TEST_WAIT))
                .unwrap_or_else(|| panic!("no keeper gap report up to {gap:?}"));
            Self::pin_reported(pinned, reached, pid);
            if reached == gap as i32 {
                return;
            }
        }
    }

    /// Pins whatever is still reported until every writer is gone.
    fn pin_rest(&self, pinned: &mut Pinned) {
        while let Some((reached, pid)) = self.try_next_gap(self.bounded(Duration::from_secs(5))) {
            Self::pin_reported(pinned, reached, pid);
        }
    }

    fn resume(&self) {
        // SAFETY: one byte from a valid buffer.
        assert_eq!(
            unsafe { libc::write(self.resume.as_raw_fd(), [1_u8].as_ptr().cast(), 1) },
            1
        );
    }

    fn kill_launcher(&self) {
        // SAFETY: our own child.
        assert_eq!(unsafe { libc::kill(self.launcher, libc::SIGKILL) }, 0);
        assert_eq!(self.launcher_status(), Some(libc::SIGKILL));
    }

    fn registry(&self) -> crate::synthetic_targets::Registry {
        crate::synthetic_targets::with_test_registry_base(self.registry_base.path(), || {
            crate::synthetic_targets::open_shared_registry()
        })
        .expect("private registry")
    }

    fn registry_dir(&self) -> PathBuf {
        self.registry_base.path().join("codex-sandbox-registry")
    }

    /// Every registry key directory that is left.
    fn keys(&self) -> Vec<PathBuf> {
        fs::read_dir(self.registry_dir())
            .expect("registry dir")
            .map(|entry| entry.expect("entry").path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("t-"))
            })
            .collect()
    }

    /// The text of the launch receipt named by the launcher.
    fn receipt_text(&self) -> String {
        let _ = self.registry();
        fs::read_to_string(
            self.registry_dir()
                .join("receipts")
                .join(format!("launch.{}", self.launcher_token.encode())),
        )
        .expect("launch receipt")
    }

    /// The first complete receipt block on the owner's report pipe that is
    /// not the helper declaration (the report stays open while the keeper
    /// lives).
    fn owner_report_block(&mut self) -> String {
        let mut text = String::new();
        let deadline = std::time::Instant::now() + self.bounded(TEST_WAIT);
        loop {
            let finished = text
                .split("end\n")
                .any(|block| block.contains("state=") && !block.contains("kind=helpers"));
            if finished {
                return text;
            }
            assert!(
                wait_readable(
                    self.report.as_fd(),
                    deadline.saturating_duration_since(std::time::Instant::now())
                ),
                "no receipt on the owner report: {text}"
            );
            let mut chunk = [0_u8; 4096];
            let read = self.report.read(&mut chunk).expect("owner report");
            assert!(read > 0, "the owner report closed early: {text}");
            text.push_str(&String::from_utf8_lossy(&chunk[..read]));
        }
    }

    fn owner_report(&mut self) -> String {
        let deadline = std::time::Instant::now() + self.bounded(TEST_WAIT);
        read_to_eof_by(&mut self.report, deadline, "the owner report")
    }

    /// `cap`, or less: what is left of this launch's shared wait budget.
    fn bounded(&self, cap: Duration) -> Duration {
        cap.min(
            self.budget_ends
                .saturating_duration_since(std::time::Instant::now()),
        )
    }

    /// Physical join: every pinned process (keeper, bubblewrap, init, and the
    /// command where pinned) exited, all by one bounded deadline.
    fn assert_all_joined(&self, pinned: &Pinned) {
        let joined_by = std::time::Instant::now() + self.bounded(TEST_WAIT);
        for (gap, _, pidfd) in pinned {
            assert!(
                wait_readable(
                    pidfd.as_fd(),
                    joined_by.saturating_duration_since(std::time::Instant::now())
                ),
                "the process reported at gap {gap} did not exit within {TEST_WAIT:?}"
            );
        }
    }

    /// The launcher's wait status, waited for within the launch's budget
    /// (never a blocking `waitpid` on a launcher that does not exit).
    fn wait_launcher(&self) -> libc::c_int {
        assert!(
            wait_readable(self.launcher_pidfd.as_fd(), self.bounded(TEST_WAIT)),
            "the launcher did not exit within {TEST_WAIT:?}"
        );
        wait_for_bwrap_child(self.launcher)
    }

    /// The launcher's exit, within the launch's budget: `Some(signal)` when it
    /// was killed by one, else `None` with the exit status asserted by the
    /// caller through `waitpid`.
    fn launcher_status(&self) -> Option<libc::c_int> {
        let status = self.wait_launcher();
        libc::WIFSIGNALED(status).then(|| libc::WTERMSIG(status))
    }

    /// Waits, within the launch's budget, until `pid` is reaped by whoever
    /// became its parent: a zombie is still alive for recovery's liveness
    /// check.
    fn wait_reaped(&self, pid: libc::pid_t) {
        let deadline = std::time::Instant::now() + self.bounded(TEST_WAIT);
        while Path::new(&format!("/proc/{pid}")).exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "{pid} was not reaped within {TEST_WAIT:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

#[test]
#[ignore = "needs a usable bubblewrap; run with --ignored on the qualification host"]
fn a_keeper_launch_relays_stdio_and_status_and_joins_every_process() {
    let _guard = KEEPER_TESTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut launch = start_keeper_launch(
        "read line; echo \"out:$line\"; echo err:$line >&2; exit 7",
        /*owned*/ true,
        /*stop_at*/ None,
    );
    let mut pinned = Pinned::default();
    launch.pin_through(keeper_gaps::Gap::KeeperRecordedInit, &mut pinned);
    assert_eq!(pinned.len(), 3, "keeper, bubblewrap and the sandbox init");

    let mut stdin = launch.stdin.take().expect("stdin");
    std::io::Write::write_all(&mut stdin, b"hello\n").expect("write stdin");
    drop(stdin);
    let read_by = std::time::Instant::now() + launch.bounded(TEST_WAIT);
    let stdout = read_to_eof_by(&mut launch.stdout, read_by, "stdout");
    let stderr = read_to_eof_by(&mut launch.stderr, read_by, "stderr");
    assert_eq!(stdout, "out:hello\n");
    assert!(stderr.contains("err:hello"), "{stderr}");

    let status = launch.wait_launcher();
    assert!(libc::WIFEXITED(status), "launcher status {status}");
    assert_eq!(libc::WEXITSTATUS(status), 7);
    launch.assert_all_joined(&pinned);

    let report = launch.owner_report();
    assert!(report.contains("kind=helpers\nreaper="), "{report}");
    assert!(report.contains("state=complete\n"), "{report}");
    assert!(report.contains("finisher=reaper\n"), "{report}");
    assert!(report.contains("reaper_join=pending\n"), "{report}");
    let receipt = launch.receipt_text();
    assert!(receipt.contains("state=complete\n"), "{receipt}");
    assert!(!receipt.contains("cancelled-by-launcher"), "{receipt}");
    for target in &launch.targets {
        assert!(!target.exists(), "{} was not reaped", target.display());
    }
    assert_eq!(launch.keys(), Vec::<PathBuf>::new());
}

#[test]
#[ignore = "needs a usable bubblewrap; run with --ignored on the qualification host"]
fn a_forwarded_sigterm_reaches_bubblewrap_and_its_death_is_re_raised() {
    let _guard = KEEPER_TESTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // Owner-less, so the keeper never waits for the init after bubblewrap.
    let launch = start_keeper_launch("exec sleep 5", /*owned*/ false, None);
    let mut pinned = Pinned::default();
    launch.pin_through(keeper_gaps::Gap::KeeperReleasedBwrap, &mut pinned);
    // SAFETY: our own child.
    assert_eq!(unsafe { libc::kill(launch.launcher, libc::SIGTERM) }, 0);
    assert_eq!(launch.launcher_status(), Some(libc::SIGTERM));
    // The keeper and bubblewrap; the init exits by itself.
    launch.assert_all_joined(&pinned);
}

/// Kills the launcher at `gap` of an owner-bounded launch, resumes the keeper
/// and asserts that the keeper alone finishes cleanup within the deadline.
fn launcher_killed_at(gap: keeper_gaps::Gap) {
    let _guard = KEEPER_TESTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut launch = start_keeper_launch("exec sleep 3", /*owned*/ true, Some(gap));
    let mut pinned = Pinned::default();
    launch.pin_through(gap, &mut pinned);
    launch.kill_launcher();
    launch.resume();
    // Pin whatever the keeper still reports (bubblewrap, the init).
    launch.pin_rest(&mut pinned);
    launch.assert_all_joined(&pinned);

    let report = launch.owner_report();
    assert!(report.contains("finisher=reaper\n"), "{report}");
    assert!(
        report.contains("reaper_join=finished-by-reaper\n"),
        "{report}"
    );
    assert!(report.contains("state=complete\n"), "{report}");
    let receipt = launch.receipt_text();
    assert!(receipt.contains("state=complete\n"), "{receipt}");
    assert!(!receipt.contains("target=incomplete"), "{receipt}");
    for target in &launch.targets {
        assert!(!target.exists(), "{} was not reaped", target.display());
    }
    assert_eq!(launch.keys(), Vec::<PathBuf>::new());
}

#[test]
#[ignore = "needs a usable bubblewrap; run with --ignored on the qualification host"]
fn killing_the_launcher_before_admission_leaves_nothing() {
    launcher_killed_at(keeper_gaps::Gap::LauncherForkedKeeper);
}

#[test]
#[ignore = "needs a usable bubblewrap; run with --ignored on the qualification host"]
fn killing_the_launcher_after_admission_hands_cleanup_to_the_keeper() {
    launcher_killed_at(keeper_gaps::Gap::LauncherAdmittedKeeper);
}

#[test]
#[ignore = "needs a usable bubblewrap; run with --ignored on the qualification host"]
fn killing_the_launcher_after_registration_reaps_never_started_targets() {
    launcher_killed_at(keeper_gaps::Gap::KeeperRegistered);
}

#[test]
#[ignore = "needs a usable bubblewrap; run with --ignored on the qualification host"]
fn killing_the_launcher_while_bubblewrap_is_parked_never_starts_it() {
    launcher_killed_at(keeper_gaps::Gap::KeeperForkedBwrap);
}

#[test]
#[ignore = "needs a usable bubblewrap; run with --ignored on the qualification host"]
fn killing_the_launcher_after_the_bubblewrap_token_reaps_within_the_deadline() {
    launcher_killed_at(keeper_gaps::Gap::KeeperRecordedBwrap);
}

#[test]
#[ignore = "needs a usable bubblewrap; run with --ignored on the qualification host"]
fn killing_the_launcher_after_bubblewrap_started_reaps_within_the_deadline() {
    launcher_killed_at(keeper_gaps::Gap::KeeperReleasedBwrap);
}

#[test]
#[ignore = "needs a usable bubblewrap; run with --ignored on the qualification host"]
fn killing_the_launcher_of_an_owner_less_launch_keeps_everything_for_recovery() {
    let _guard = KEEPER_TESTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut launch = start_keeper_launch(
        "exec sleep 3",
        /*owned*/ false,
        Some(keeper_gaps::Gap::KeeperRegistered),
    );
    let mut pinned = Pinned::default();
    launch.pin_through(keeper_gaps::Gap::KeeperRegistered, &mut pinned);
    launch.kill_launcher();
    launch.resume();
    launch.pin_rest(&mut pinned);
    launch.assert_all_joined(&pinned);
    for (_, pid, _) in &pinned {
        launch.wait_reaped(*pid);
    }

    // Stopped and recorded, nothing removed.
    let report = launch.owner_report();
    assert!(
        report.contains("target=incomplete:owner-gone-without-deadline "),
        "{report}"
    );
    for target in &launch.targets {
        assert!(target.exists(), "{} was removed", target.display());
    }
    let keys = launch.keys();
    assert!(!keys.is_empty());
    for key in &keys {
        let holders = fs::read_dir(key)
            .expect("key dir")
            .map(|entry| entry.expect("entry").path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("holder."))
            })
            .collect::<Vec<_>>();
        for holder in holders {
            let text = fs::read_to_string(&holder).expect("holder");
            assert!(text.contains("sandbox=never-started\n"), "{text}");
        }
    }

    // A later, separate recovery acts on the statement once the keeper is
    // proven terminated (it is joined above).
    let by = crate::synthetic_targets::ProcessToken::current().expect("token");
    let registry = launch.registry();
    {
        let _lock = registry.lock().expect("lock");
        crate::synthetic_targets::recover_registry(&registry, &by);
    }
    let recovery = fs::read_to_string(
        launch
            .registry_dir()
            .join("receipts")
            .join(format!("recovery.{}", by.encode())),
    )
    .expect("recovery receipt");
    if recovery.contains("identity-unprovable") {
        eprintln!("no kernel file handles here: recovery preserved the targets: {recovery}");
        return;
    }
    assert!(recovery.contains("key=reaped "), "{recovery}");
    assert!(!recovery.contains("holder-unproven"), "{recovery}");
    for target in &launch.targets {
        assert!(!target.exists(), "{} was not recovered", target.display());
    }
    let receipt = launch.receipt_text();
    assert!(
        receipt.contains("incomplete:owner-gone-without-deadline"),
        "the launch receipt is never rewritten: {receipt}"
    );
}

#[test]
#[ignore = "needs a usable bubblewrap; run with --ignored on the qualification host"]
fn the_bubblewrap_and_init_evidence_are_recorded_under_separate_locks() {
    let _guard = KEEPER_TESTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for (gap, expect_init) in [
        (keeper_gaps::Gap::KeeperRecordedBwrap, false),
        (keeper_gaps::Gap::KeeperRecordedInit, true),
    ] {
        let mut launch = start_keeper_launch("read line; exit 0", true, Some(gap));
        let mut pinned = Pinned::default();
        launch.pin_through(gap, &mut pinned);
        // Stopped between the two writes (or after the second): read every
        // holder this launch wrote.
        for key in launch.keys() {
            for entry in fs::read_dir(&key).expect("key dir") {
                let path = entry.expect("entry").path();
                if !path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("holder."))
                {
                    continue;
                }
                let text = fs::read_to_string(&path).expect("holder");
                assert!(text.contains("\nbwrap="), "{text}");
                assert_eq!(text.contains("\ninit="), expect_init, "{text}");
            }
        }
        launch.resume();
        drop(launch.stdin.take());
        let status = launch.wait_launcher();
        assert!(libc::WIFEXITED(status), "launcher status {status}");
        assert_eq!(libc::WEXITSTATUS(status), 0);
        launch.assert_all_joined(&pinned);
        let _ = launch.owner_report();
    }
}

#[test]
#[ignore = "needs a usable bubblewrap; run with --ignored on the qualification host"]
fn a_deadline_that_passes_after_admission_is_recorded_at_once_and_cleans_nothing() {
    let _guard = KEEPER_TESTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut launch = start_keeper_launch("read line; exit 0", /*owned*/ true, None);
    let mut pinned = Pinned::default();
    launch.pin_through(keeper_gaps::Gap::KeeperRecordedInit, &mut pinned);
    // The owner tightens its original deadline into the past while the
    // command runs: the keeper records the sticky timeout at once.
    let mut tighten = launch.deadline_write.take().expect("owner channel");
    std::io::Write::write_all(
        &mut tighten,
        &crate::cleanup_custody::monotonic_now_ns().to_le_bytes(),
    )
    .expect("tighten");
    let report = launch.owner_report_block();
    assert!(report.contains("state=timed-out\n"), "{report}");
    assert!(report.contains("reaper_join=pending\n"), "{report}");
    assert!(!report.contains("persist=failed"), "{report}");
    for target in &launch.targets {
        assert!(
            target.exists(),
            "{} was cleaned after expiry",
            target.display()
        );
    }

    // The command still ends normally, and nothing is cleaned afterwards.
    drop(launch.stdin.take());
    let status = launch.wait_launcher();
    assert!(libc::WIFEXITED(status), "launcher status {status}");
    assert_eq!(libc::WEXITSTATUS(status), 0);
    launch.assert_all_joined(&pinned);
    let receipt = launch.receipt_text();
    assert!(receipt.contains("state=timed-out\n"), "{receipt}");
    for target in &launch.targets {
        assert!(
            target.exists(),
            "{} was cleaned after expiry",
            target.display()
        );
    }
    drop(tighten);
}

/// The keeper's receipt frame in an owner report, split strictly into the
/// receipt as the registry would store it and the optional trailing
/// `persist=failed`. A frame is one `write_report` write: the receipt, the
/// optional flag, and the `end` line `write_report` appends. Helper
/// declarations are skipped; only `end`-terminated frames count.
fn keeper_receipt_frame(report: &str) -> (String, bool) {
    let mut frames = Vec::new();
    let mut current = String::new();
    for line in report.split_inclusive('\n') {
        if line == "end\n" {
            frames.push(std::mem::take(&mut current));
        } else {
            current.push_str(line);
        }
    }
    assert!(current.is_empty(), "truncated report: {report:?}");
    let receipts: Vec<String> = frames
        .into_iter()
        .filter(|frame| !frame.contains("\nkind=helpers\n"))
        .collect();
    assert_eq!(receipts.len(), 1, "one receipt frame: {report:?}");
    let frame = &receipts[0];
    match frame.strip_suffix("persist=failed\n") {
        Some(receipt) => {
            assert!(!receipt.contains("persist="), "{report:?}");
            (receipt.to_string(), true)
        }
        None => {
            assert!(!frame.contains("persist="), "{report:?}");
            (frame.clone(), false)
        }
    }
}

fn hex_path(path: &Path) -> String {
    path.as_os_str()
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// A deadline tightened after admission, with the private registry's lock
/// either free or held by the test across the keeper's finish: the keeper's
/// single post-stop try either persists the timed-out receipt (on disk
/// exactly as reported) or fails, and then the owner report says
/// `persist=failed` with the same exact outcome while the registry keeps the
/// pre-command registration byte for byte. Nothing is cleaned in either.
fn assert_tightened_finish_is_reported_exactly(hold_registry_lock: bool) {
    let _guard = KEEPER_TESTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut launch = start_keeper_launch("read line; exit 0", /*owned*/ true, None);
    let mut pinned = Pinned::default();
    launch.pin_through(keeper_gaps::Gap::KeeperRecordedInit, &mut pinned);
    let registration = launch.receipt_text();
    let keeper = pinned
        .iter()
        .find(|(gap, _, _)| *gap == keeper_gaps::Gap::LauncherForkedKeeper as i32)
        .map(|(_, pid, _)| *pid)
        .expect("the keeper was pinned");
    let registry = launch.registry();
    let held = hold_registry_lock.then(|| {
        registry
            .try_lock()
            .expect("lock")
            .expect("the private registry lock is free before the stop")
    });
    let mut tighten = launch.deadline_write.take().expect("owner channel");
    std::io::Write::write_all(
        &mut tighten,
        &crate::cleanup_custody::monotonic_now_ns().to_le_bytes(),
    )
    .expect("tighten");
    let report = launch.owner_report_block();
    drop(held);
    let (receipt, persist_failed) = keeper_receipt_frame(&report);
    assert_eq!(persist_failed, hold_registry_lock, "{report}");
    let mut expected = format!(
        "v=1\nlaunch={}\nstate=timed-out\nfinisher=reaper\nreaper=",
        launch.launcher_token.encode()
    );
    assert!(receipt.starts_with(&expected), "{report}");
    let reaper_line = receipt[expected.len()..]
        .lines()
        .next()
        .expect("reaper token");
    assert_eq!(
        reaper_line.split('.').nth(1),
        Some(keeper.to_string().as_str()),
        "{report}"
    );
    expected.push_str(reaper_line);
    expected.push_str("\nreaper_join=pending\n");
    let mut target_lines: Vec<String> = receipt
        .strip_prefix(&expected)
        .unwrap_or_else(|| panic!("exact receipt header: {report}"))
        .lines()
        .map(str::to_string)
        .collect();
    target_lines.sort();
    let mut expected_targets: Vec<String> = launch
        .targets
        .iter()
        .map(|target| format!("target=timed-out {}", hex_path(target)))
        .collect();
    expected_targets.sort();
    assert_eq!(target_lines, expected_targets, "{report}");
    let on_disk = launch.receipt_text();
    if persist_failed {
        assert_eq!(
            on_disk, registration,
            "a failed persist leaves the registration untouched"
        );
    } else {
        assert_eq!(
            on_disk, receipt,
            "the persisted receipt is the reported one"
        );
    }
    for target in &launch.targets {
        assert!(target.exists(), "{} was cleaned", target.display());
    }

    // The command still ends normally; the receipt never changes again.
    drop(launch.stdin.take());
    let status = launch.wait_launcher();
    assert!(libc::WIFEXITED(status), "launcher status {status}");
    assert_eq!(libc::WEXITSTATUS(status), 0);
    launch.assert_all_joined(&pinned);
    assert_eq!(launch.receipt_text(), on_disk);
    for target in &launch.targets {
        assert!(target.exists(), "{} was cleaned", target.display());
    }
    drop(tighten);
}

#[test]
#[ignore = "needs a usable bubblewrap; run with --ignored on the qualification host"]
fn a_tightened_finish_that_persists_is_reported_exactly() {
    assert_tightened_finish_is_reported_exactly(/*hold_registry_lock*/ false);
}

#[test]
#[ignore = "needs a usable bubblewrap; run with --ignored on the qualification host"]
fn a_tightened_finish_that_cannot_persist_is_reported_as_failed() {
    assert_tightened_finish_is_reported_exactly(/*hold_registry_lock*/ true);
}

/// Runs `body` in a forked, single-threaded child and returns its wait
/// status.
fn in_forked_child_status(body: impl FnOnce() -> i32) -> libc::c_int {
    // SAFETY: the child runs `body` and `_exit`s.
    let child = unsafe { libc::fork() };
    assert!(child >= 0, "fork");
    if child == 0 {
        let code = std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)).unwrap_or(101);
        // SAFETY: never return into the test harness.
        unsafe { libc::_exit(code) }
    }
    wait_forked_child(child)
}

#[test]
fn the_keepers_forwarder_is_armed_before_the_unmask() {
    // A forwarded signal that is already pending when the keeper unmasks
    // must reach its recording forwarder, never the default action.
    let status = in_forked_child_status(|| {
        let mask = ForwardedSignalMask::block();
        PENDING_FORWARDED_SIGNAL.store(0, Ordering::SeqCst);
        // SAFETY: raising a blocked signal leaves it pending.
        unsafe { libc::raise(libc::SIGTERM) };
        if keeper_prologue(/*pid_namespace*/ false, &mask).is_err() {
            return 2;
        }
        if PENDING_FORWARDED_SIGNAL.load(Ordering::SeqCst) != libc::SIGTERM {
            return 3;
        }
        0
    });
    assert!(libc::WIFEXITED(status), "the keeper died: status {status}");
    assert_eq!(libc::WEXITSTATUS(status), 0);
}

#[test]
fn the_launchers_relay_is_armed_before_the_unmask() {
    // A forwarded signal pending at the launcher's unmask reaches the keeper
    // (here a stand-in child), and the launcher survives it.
    let status = in_forked_child_status(|| {
        // SAFETY: the stand-in only waits to be signalled.
        let keeper = unsafe { libc::fork() };
        if keeper == 0 {
            loop {
                // SAFETY: plain pause.
                unsafe { libc::pause() };
            }
        }
        let keeper_pidfd = pin(keeper);
        let mask = ForwardedSignalMask::block();
        // SAFETY: raising a blocked signal leaves it pending.
        unsafe { libc::raise(libc::SIGTERM) };
        let _forwarders = arm_launcher_relay(keeper, &mask);
        if !wait_readable(keeper_pidfd.as_fd(), TEST_WAIT) {
            // SAFETY: our own child.
            unsafe { libc::kill(keeper, libc::SIGKILL) };
            return 2;
        }
        let keeper_status = wait_for_bwrap_child(keeper);
        if !(libc::WIFSIGNALED(keeper_status) && libc::WTERMSIG(keeper_status) == libc::SIGTERM) {
            return 3;
        }
        0
    });
    assert!(
        libc::WIFEXITED(status),
        "the launcher died: status {status}"
    );
    assert_eq!(libc::WEXITSTATUS(status), 0);
}

#[test]
fn the_info_report_is_read_within_the_owners_deadline_only() {
    use crate::cleanup_custody::CleanupDeadline;
    let info = crate::synthetic_targets::INFO_DEADLINE;
    assert_eq!(
        keeper_info_bound(&mut CleanupDeadline::at_for_tests(0)),
        None,
        "expiry first: nothing is read after the deadline"
    );
    assert_eq!(keeper_info_bound(&mut CleanupDeadline::none()), Some(info));
    let far = crate::cleanup_custody::monotonic_now_ns() + 3_600_000_000_000;
    assert_eq!(
        keeper_info_bound(&mut CleanupDeadline::at_for_tests(far)),
        Some(info)
    );
    let near = crate::cleanup_custody::monotonic_now_ns() + 1_000_000_000;
    let bound = keeper_info_bound(&mut CleanupDeadline::at_for_tests(near)).expect("bound");
    assert!(bound <= Duration::from_secs(1), "{bound:?}");
}

#[test]
fn the_keeper_keeps_exactly_its_whitelisted_descriptors() {
    // In a forked, single-threaded child, like the keeper: the keeper's own
    // descriptors next to unrelated inherited ones, among them a socket pair,
    // an owner's deadline-channel write end and a stdin writer.
    let status = in_forked_child_status(|| {
        let identity = |fd: libc::c_int| {
            // SAFETY: zeroed stat is a valid out-parameter.
            let mut stat: libc::stat = unsafe { std::mem::zeroed() };
            // SAFETY: fstat on a descriptor number only reads.
            (unsafe { libc::fstat(fd, &mut stat) } == 0).then_some((stat.st_dev, stat.st_ino))
        };
        let open_table = || {
            (0..4096)
                .filter_map(|fd| identity(fd).map(|id| (fd, id)))
                .collect::<std::collections::BTreeSet<_>>()
        };
        let Ok((unrelated_socket, unrelated_peer)) = keeper_channel_pair() else {
            return 10;
        };
        let (deadline_read, deadline_write) = cloexec_pipe_pair();
        let (report_read, report_write) = cloexec_pipe_pair();
        let (stdin_read, stdin_write) = cloexec_pipe_pair();
        let (preserved_read, preserved_write) = cloexec_pipe_pair();
        let (gap_read, gap_write) = cloexec_pipe_pair();
        let Ok((launcher_channel, keeper_channel)) = keeper_channel_pair() else {
            return 11;
        };
        let Ok(launcher_pidfd) = open_own_pidfd() else {
            return 12;
        };
        let Ok(launcher) = crate::synthetic_targets::ProcessToken::current() else {
            return 13;
        };
        keeper_gaps::REPORT_FD.store(gap_write.as_raw_fd(), Ordering::SeqCst);
        keeper_gaps::RESUME_FD.store(gap_read.as_raw_fd(), Ordering::SeqCst);
        let Ok(deadline) = crate::cleanup_custody::CleanupDeadline::new(
            Some(crate::cleanup_custody::monotonic_now_ns() + 3_600_000_000_000),
            Some(deadline_read),
        ) else {
            return 14;
        };
        let start = KeeperStart {
            args: Vec::new(),
            preserved_files: vec![File::from(preserved_read)],
            synthetic_mount_targets: Vec::new(),
            pid_namespace: false,
            launcher,
            launcher_pidfd,
            channel: keeper_channel,
            custody: crate::synthetic_targets::LaunchCustody {
                deadline,
                report: Some(report_write),
            },
            setup_signal_mask: ForwardedSignalMask::block(),
        };
        // Everything not whitelisted is the keeper's to close: owned here
        // only as raw numbers.
        let unrelated = [
            unrelated_socket.into_raw_fd(),
            unrelated_peer.into_raw_fd(),
            deadline_write.into_raw_fd(),
            report_read.into_raw_fd(),
            stdin_read.into_raw_fd(),
            stdin_write.into_raw_fd(),
            preserved_write.into_raw_fd(),
            launcher_channel.into_raw_fd(),
        ];
        let expected = keeper_descriptor_whitelist(&start)
            .into_iter()
            .filter_map(|fd| identity(fd).map(|id| (fd, id)))
            .collect::<std::collections::BTreeSet<_>>();
        if expected.len() != 10 {
            // 0-2, channel, launcher pidfd, deadline channel, report,
            // preserved file and the two gap pipes.
            return 15;
        }

        if close_keeper_inherited_descriptors(&start).is_err() {
            return 16;
        }

        if open_table() != expected {
            return 2;
        }
        if unrelated.iter().any(|fd| identity(*fd).is_some()) {
            return 3;
        }
        // The whitelisted descriptors are still owned by `start` and the gap
        // pipes; nothing is closed twice.
        keeper_gaps::REPORT_FD.store(-1, Ordering::SeqCst);
        keeper_gaps::RESUME_FD.store(-1, Ordering::SeqCst);
        drop((start, gap_read, gap_write));
        0
    });
    assert!(libc::WIFEXITED(status), "status {status}");
    assert_eq!(libc::WEXITSTATUS(status), 0);
}

/// A pipe end moved to a descriptor number above the soft `RLIMIT_NOFILE`
/// in force afterwards: the limit is raised, the end is placed high, and the
/// limit is lowered again (descriptors opened before a lowering may exceed
/// it). Returns the high number, or `None` when the hard limit is too low.
fn descriptor_above_a_lowered_limit() -> Option<libc::c_int> {
    // SAFETY: zeroed rlimit is a valid out-parameter.
    let mut limit: libc::rlimit = unsafe { std::mem::zeroed() };
    // SAFETY: valid out-parameter.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 || limit.rlim_max < 4096 {
        return None;
    }
    let raised = libc::rlimit {
        rlim_cur: limit.rlim_max,
        rlim_max: limit.rlim_max,
    };
    // SAFETY: valid rlimit.
    if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raised) } != 0 {
        return None;
    }
    let high = libc::c_int::try_from(limit.rlim_max.min(1 << 20) - 1).ok()?;
    let (read, _write) = cloexec_pipe_pair();
    // SAFETY: dup2 onto a free number below the raised limit.
    if unsafe { libc::dup2(read.as_raw_fd(), high) } != high {
        return None;
    }
    let lowered = libc::rlimit {
        rlim_cur: 256,
        rlim_max: limit.rlim_max,
    };
    // SAFETY: valid rlimit.
    if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &lowered) } != 0 {
        return None;
    }
    Some(high)
}

#[test]
fn close_range_closes_a_descriptor_above_a_lowered_limit() {
    let status = in_forked_child_status(|| {
        let Some(high) = descriptor_above_a_lowered_limit() else {
            return 10;
        };
        let (kept, kept_write) = cloexec_pipe_pair();
        if close_descriptors_except(&[0, 1, 2, kept.as_raw_fd()]).is_err() {
            return 2;
        }
        if descriptor_is_open(high) {
            return 3;
        }
        if !descriptor_is_open(kept.as_raw_fd()) {
            return 4;
        }
        // `kept_write` was closed by the call: never closed twice.
        std::mem::forget(kept_write);
        0
    });
    assert!(libc::WIFEXITED(status), "status {status}");
    assert_eq!(libc::WEXITSTATUS(status), 0);
}

#[test]
fn without_close_range_the_listing_closes_every_unknown_descriptor() {
    let status = in_forked_child_status(|| {
        FORCE_CLOSE_RANGE_UNSUPPORTED.with(|force| force.set(true));
        let Some(high) = descriptor_above_a_lowered_limit() else {
            return 10;
        };
        let (kept, unrelated) = cloexec_pipe_pair();
        let unrelated = std::os::fd::IntoRawFd::into_raw_fd(unrelated);
        if close_descriptors_except(&[0, 1, 2, kept.as_raw_fd()]).is_err() {
            return 2;
        }
        if descriptor_is_open(high) || descriptor_is_open(unrelated) {
            return 3;
        }
        if !descriptor_is_open(kept.as_raw_fd()) {
            return 4;
        }
        0
    });
    assert!(libc::WIFEXITED(status), "status {status}");
    assert_eq!(libc::WEXITSTATUS(status), 0);
}

#[test]
fn a_keeper_that_cannot_prove_its_descriptors_closed_refuses_to_start() {
    // Neither close_range nor /proc: the keeper reports the failure on its
    // channel and exits 1 before anything else (it never reads its
    // admission, so it cannot reach registration).
    let Ok((launcher_channel, keeper_channel)) = keeper_channel_pair() else {
        panic!("keeper channel");
    };
    let registry_base = tempfile::TempDir::new().expect("registry base");
    // SAFETY: the child runs only the keeper entry, which `_exit`s.
    let child = unsafe { libc::fork() };
    assert!(child >= 0, "fork");
    if child == 0 {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            drop(launcher_channel);
            FORCE_CLOSE_RANGE_UNSUPPORTED.with(|force| force.set(true));
            FORCE_PROC_FD_UNAVAILABLE.with(|force| force.set(true));
            let start = KeeperStart {
                args: Vec::new(),
                preserved_files: Vec::new(),
                synthetic_mount_targets: Vec::new(),
                pid_namespace: false,
                launcher: crate::synthetic_targets::ProcessToken::current().expect("token"),
                launcher_pidfd: open_own_pidfd().expect("pidfd"),
                channel: keeper_channel,
                custody: crate::synthetic_targets::LaunchCustody {
                    deadline: crate::cleanup_custody::CleanupDeadline::none(),
                    report: None,
                },
                setup_signal_mask: ForwardedSignalMask::block(),
            };
            crate::synthetic_targets::with_test_registry_base(registry_base.path(), || {
                run_keeper(start);
            });
        }));
        // SAFETY: never return into the test harness.
        unsafe { libc::_exit(101) }
    }
    drop(keeper_channel);
    let status = wait_forked_child(child);
    assert!(libc::WIFEXITED(status), "status {status}");
    assert_eq!(libc::WEXITSTATUS(status), 1, "the keeper fails closed");
    let message = match receive_keeper_outcome(&launcher_channel) {
        // The typed pre-side-effect refusal, not a generic error.
        Some(KeeperOutcome::Refused(message)) => message,
        _ => panic!("the keeper must report its typed refusal"),
    };
    assert!(
        message.contains("cannot prove that every inherited descriptor is closed"),
        "{message}"
    );
    assert_eq!(
        fs::read_dir(registry_base.path())
            .expect("registry base")
            .count(),
        0,
        "no side effect: not even the registry was created"
    );
}

#[test]
fn only_the_typed_refusal_is_a_refusal() {
    let (launcher_channel, keeper_channel) = keeper_channel_pair().expect("channel");
    // The same words as an error are an error: no text is interpreted.
    send_keeper_error(
        &keeper_channel,
        "codex-linux-sandbox: the sandbox keeper refuses to start: it cannot prove that every inherited descriptor is closed",
    );
    assert!(matches!(
        receive_keeper_outcome(&launcher_channel),
        Some(KeeperOutcome::Error(_))
    ));
    send_keeper_refusal(&keeper_channel, "any reason");
    assert!(matches!(
        receive_keeper_outcome(&launcher_channel),
        Some(KeeperOutcome::Refused(reason)) if reason == "any reason"
    ));
}

#[test]
fn a_refusal_survives_an_admission_the_keeper_never_read() {
    // The launcher admits first; the keeper, without reading the admission,
    // refuses and closes. Closing with an unread message makes AF_UNIX
    // report ECONNRESET on the launcher's end before its queue.
    let (launcher_channel, keeper_channel) = keeper_channel_pair().expect("channel");
    send_keeper_message(&launcher_channel, &[KEEPER_ADMIT]);
    send_keeper_refusal(&keeper_channel, "refused after admission");
    drop(keeper_channel);
    assert!(matches!(
        receive_keeper_outcome(&launcher_channel),
        Some(KeeperOutcome::Refused(reason)) if reason == "refused after admission"
    ));
    assert!(
        receive_keeper_message(&launcher_channel).is_none(),
        "then EOF"
    );
}

#[test]
fn a_refusal_survives_an_admission_sent_after_the_keeper_closed() {
    // The keeper refuses and closes first; the launcher's admission then
    // meets a closed peer (EPIPE, ignored), and the refusal is still read.
    let (launcher_channel, keeper_channel) = keeper_channel_pair().expect("channel");
    send_keeper_refusal(&keeper_channel, "refused before admission");
    drop(keeper_channel);
    send_keeper_message(&launcher_channel, &[KEEPER_ADMIT]);
    assert!(matches!(
        receive_keeper_outcome(&launcher_channel),
        Some(KeeperOutcome::Refused(reason)) if reason == "refused before admission"
    ));
}

/// Runs the real launcher in a forked child with its keeper forced to refuse
/// before any side effect, in the given order against the launcher's
/// declaration and admission (`keeper_gaps::REFUSAL_ORDER`), and asserts the
/// typed refusal's whole outcome: exit 1, the owner told `incomplete` and
/// `refused`, and the declared receipt finished, never left running.
/// A forked test child's wait status, within [`TEST_WAIT`]: a child that
/// does not exit is killed and reaped before the failure is reported.
fn wait_forked_child(child: libc::pid_t) -> libc::c_int {
    let child_pidfd = pin(child);
    if !wait_readable(child_pidfd.as_fd(), TEST_WAIT) {
        pidfd_kill(child_pidfd.as_fd(), libc::SIGKILL);
        wait_for_bwrap_child(child);
        panic!("the forked child {child} did not exit within {TEST_WAIT:?}");
    }
    wait_for_bwrap_child(child)
}

fn assert_refused_keeper_launch_is_finished(order: i32) {
    let registry_base = tempfile::TempDir::new().expect("registry base");
    fs::set_permissions(registry_base.path(), fs::Permissions::from_mode(0o700))
        .expect("chmod registry base");
    let workspace = tempfile::TempDir::new().expect("workspace");
    let target = workspace.path().join(".git");
    let (report_read, report_write) = cloexec_pipe_pair();
    let (stderr_read, stderr_write) = cloexec_pipe_pair();
    let base = registry_base.path().to_path_buf();
    let target_for_launch = target.clone();
    // SAFETY: the child becomes the launcher and never returns.
    let launcher = unsafe { libc::fork() };
    assert!(launcher >= 0, "fork the launcher");
    if launcher == 0 {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            drop((report_read, stderr_read));
            // SAFETY: plain dup2 onto stderr.
            unsafe { libc::dup2(stderr_write.as_raw_fd(), libc::STDERR_FILENO) };
            drop(stderr_write);
            // Inherited by the keeper's fork, in this thread.
            FORCE_CLOSE_RANGE_UNSUPPORTED.with(|force| force.set(true));
            FORCE_PROC_FD_UNAVAILABLE.with(|force| force.set(true));
            keeper_gaps::REFUSAL_ORDER.store(order, Ordering::SeqCst);
            let bwrap_args = crate::bwrap::BwrapArgs {
                args: vec!["bwrap".to_string(), "--".to_string(), "true".to_string()],
                preserved_files: Vec::new(),
                synthetic_mount_targets: vec![
                    crate::bwrap::SyntheticMountTarget::missing_empty_directory(&target_for_launch),
                ],
            };
            let custody = crate::synthetic_targets::LaunchCustody {
                deadline: crate::cleanup_custody::CleanupDeadline::at_for_tests(
                    crate::cleanup_custody::monotonic_now_ns() + 3_600_000_000_000,
                ),
                report: Some(report_write),
            };
            crate::synthetic_targets::with_test_registry_base(&base, || {
                run_bwrap_through_keeper(bwrap_args, custody);
            });
        }));
        // SAFETY: never return into the test harness.
        unsafe { libc::_exit(101) }
    }
    drop((report_write, stderr_write));
    let launcher_token =
        crate::synthetic_targets::ProcessToken::for_pid(launcher).expect("launcher token");
    let status = wait_forked_child(launcher);
    let stderr = read_to_eof_by(
        &mut File::from(stderr_read),
        std::time::Instant::now() + TEST_WAIT,
        "launcher stderr",
    );
    assert!(
        libc::WIFEXITED(status),
        "launcher status {status}: {stderr}"
    );
    assert_eq!(libc::WEXITSTATUS(status), 1, "{stderr}");
    assert!(
        !stderr.contains("exited without reporting"),
        "the typed refusal was lost: {stderr}"
    );
    assert!(
        stderr.contains("refuses to start"),
        "the refusal's reason is printed: {stderr}"
    );

    let report = read_to_eof_by(
        &mut File::from(report_read),
        std::time::Instant::now() + TEST_WAIT,
        "owner report",
    );
    assert!(report.contains("kind=helpers\nreaper="), "{report}");
    assert!(report.contains("state=incomplete\n"), "{report}");
    assert!(report.contains("finisher=launcher\n"), "{report}");
    assert!(report.contains("keeper=refused\n"), "{report}");
    assert!(!report.contains("persist=failed"), "{report}");
    let receipt = fs::read_to_string(
        registry_base
            .path()
            .join("codex-sandbox-registry")
            .join("receipts")
            .join(format!("launch.{}", launcher_token.encode())),
    )
    .expect("launch receipt");
    assert!(receipt.contains("state=incomplete\n"), "{receipt}");
    assert!(!receipt.contains("state=running"), "{receipt}");
    assert!(!target.exists(), "no side effect: nothing was installed");
}

#[test]
fn a_keeper_refusing_before_its_declaration_is_finished_as_refused() {
    assert_refused_keeper_launch_is_finished(keeper_gaps::REFUSE_BEFORE_DECLARATION);
}

#[test]
fn a_keeper_refusing_with_its_admission_unread_is_finished_as_refused() {
    assert_refused_keeper_launch_is_finished(keeper_gaps::REFUSE_AFTER_ADMISSION);
}

#[test]
#[ignore = "needs a usable bubblewrap; run with --ignored on the qualification host"]
fn an_owner_gone_before_any_release_keeps_every_target() {
    let _guard = KEEPER_TESTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // The sandbox's PID 1 is the real inner-stage helper, as in production:
    // only its re-armed parent-death signal ends the init once bubblewrap
    // dies (the keeper signals nothing but bubblewrap). The command never
    // exits on its own: it reads a stdin the test holds and never writes to.
    let binary = codex_utils_cargo_bin::cargo_bin("codex-linux-sandbox")
        .unwrap_or_else(|err| panic!("the real inner-stage helper is required: {err}"));
    // Owned, with no deadline ever issued.
    let mut launch = start_keeper_launch_owned_by(
        &KeeperCommand::RealInnerStage {
            binary,
            command: vec!["/bin/cat".to_string()],
        },
        TestOwner::ChannelWithoutDeadline,
        Some(keeper_gaps::Gap::KeeperRecordedInit),
    );
    let mut pinned = Pinned::default();
    // Positive gate: the keeper is held before any release.
    launch.pin_through(keeper_gaps::Gap::KeeperRecordedInit, &mut pinned);
    let init = pinned
        .iter()
        .find(|(gap, _, _)| *gap == keeper_gaps::Gap::KeeperRecordedInit as i32)
        .map(|(_, pid, _)| *pid)
        .expect("the init was pinned");
    // The gated command (the inner stage's child) is joined too.
    let joins_by = std::time::Instant::now() + launch.bounded(TEST_WAIT);
    let command = loop {
        if let Some(child) = std::fs::read_to_string(format!("/proc/{init}/task/{init}/children"))
            .ok()
            .and_then(|children| children.split_whitespace().next()?.parse().ok())
        {
            break child;
        }
        assert!(
            std::time::Instant::now() < joins_by,
            "the inner stage never started the command"
        );
        std::thread::yield_now();
    };
    pinned.push((0, command, pin(command)));
    // The owner goes away: the test holds its only writer, so the channel is
    // at EOF as soon as it is dropped.
    drop(launch.deadline_write.take());
    // The launcher's parent-death SIGTERM, as a dying owner delivers it.
    pidfd_kill(launch.launcher_pidfd.as_fd(), libc::SIGTERM);
    launch.resume();
    launch.pin_rest(&mut pinned);
    let status = launch.wait_launcher();
    assert!(
        libc::WIFSIGNALED(status) && libc::WTERMSIG(status) == libc::SIGTERM,
        "the forwarded SIGTERM is re-raised: status {status}"
    );
    launch.assert_all_joined(&pinned);

    let report = launch.owner_report();
    assert!(report.contains("state=incomplete\n"), "{report}");
    let target_lines = report
        .lines()
        .filter(|line| line.starts_with("target="))
        .collect::<Vec<_>>();
    assert!(!target_lines.is_empty(), "{report}");
    assert_eq!(target_lines.len(), launch.targets.len(), "{report}");
    assert!(
        target_lines
            .iter()
            .all(|line| { line.starts_with("target=incomplete:owner-gone-without-deadline ") }),
        "every installed target is owner-gone, none reaped: {report}"
    );
    for target in &launch.targets {
        assert!(
            target.exists(),
            "{} was released after the owner was gone",
            target.display()
        );
    }
}

#[test]
fn a_keeper_refused_before_any_side_effect_never_leaves_a_running_receipt() {
    // The launcher runs for real in a forked child; its keeper is forced to
    // refuse (neither close_range nor /proc), before any side effect.
    let registry_base = tempfile::TempDir::new().expect("registry base");
    fs::set_permissions(registry_base.path(), fs::Permissions::from_mode(0o700))
        .expect("chmod registry base");
    let workspace = tempfile::TempDir::new().expect("workspace");
    let target = workspace.path().join(".git");
    let (report_read, report_write) = cloexec_pipe_pair();
    let base = registry_base.path().to_path_buf();
    let target_for_launch = target.clone();
    // SAFETY: the child becomes the launcher and never returns.
    let launcher = unsafe { libc::fork() };
    assert!(launcher >= 0, "fork the launcher");
    if launcher == 0 {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            drop(report_read);
            // Inherited by the keeper's fork, in this thread.
            FORCE_CLOSE_RANGE_UNSUPPORTED.with(|force| force.set(true));
            FORCE_PROC_FD_UNAVAILABLE.with(|force| force.set(true));
            let bwrap_args = crate::bwrap::BwrapArgs {
                args: vec!["bwrap".to_string(), "--".to_string(), "true".to_string()],
                preserved_files: Vec::new(),
                synthetic_mount_targets: vec![
                    crate::bwrap::SyntheticMountTarget::missing_empty_directory(&target_for_launch),
                ],
            };
            let custody = crate::synthetic_targets::LaunchCustody {
                deadline: crate::cleanup_custody::CleanupDeadline::at_for_tests(
                    crate::cleanup_custody::monotonic_now_ns() + 3_600_000_000_000,
                ),
                report: Some(report_write),
            };
            crate::synthetic_targets::with_test_registry_base(&base, || {
                run_bwrap_through_keeper(bwrap_args, custody);
            });
        }));
        // SAFETY: never return into the test harness.
        unsafe { libc::_exit(101) }
    }
    drop(report_write);
    let launcher_token =
        crate::synthetic_targets::ProcessToken::for_pid(launcher).expect("launcher token");
    let status = wait_forked_child(launcher);
    assert!(libc::WIFEXITED(status), "launcher status {status}");
    assert_eq!(
        libc::WEXITSTATUS(status),
        1,
        "a refused keeper fails the launch"
    );

    let report = read_to_eof_by(
        &mut File::from(report_read),
        std::time::Instant::now() + TEST_WAIT,
        "owner report",
    );
    assert!(report.contains("kind=helpers\nreaper="), "{report}");
    assert!(report.contains("state=incomplete\n"), "{report}");
    assert!(report.contains("finisher=launcher\n"), "{report}");
    assert!(report.contains("keeper=refused\n"), "{report}");
    assert!(!report.contains("persist=failed"), "{report}");
    let receipt = fs::read_to_string(
        registry_base
            .path()
            .join("codex-sandbox-registry")
            .join("receipts")
            .join(format!("launch.{}", launcher_token.encode())),
    )
    .expect("launch receipt");
    assert!(receipt.contains("state=incomplete\n"), "{receipt}");
    assert!(!receipt.contains("state=running"), "{receipt}");
    assert!(!target.exists(), "no side effect: nothing was installed");
}

/// The observed case: a command timed out after 50 ms whose owner gives the
/// launch about 2 s to drain. The launcher is SIGKILLed early, the sandbox's
/// PID 1 is the real inner-stage helper, and the command never exits on its
/// own (it reads a pipe the test never writes to). Only the keeper's cancel
/// (SIGTERM to bubblewrap) together with the inner stage's re-armed
/// parent-death signal can end the sandbox, and only then can the keeper
/// prove it gone and reap the targets, all within the original deadline.
#[test]
#[ignore = "needs a usable bubblewrap and the built codex-linux-sandbox binary; run with --ignored on the qualification host"]
fn an_early_killed_launch_with_the_real_inner_stage_is_reaped_within_its_original_deadline() {
    let _guard = KEEPER_TESTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // The helper the qualification run built (and hash-verified):
    // `CARGO_BIN_EXE_codex-linux-sandbox` when exported, otherwise the cargo
    // target directory. Missing is a failure, never a skip.
    let binary = codex_utils_cargo_bin::cargo_bin("codex-linux-sandbox")
        .unwrap_or_else(|err| panic!("the real inner-stage helper is required: {err}"));
    // The observed owner budget: a 50 ms timeout plus a 2 s drain.
    let mut launch = start_keeper_launch_with(
        &KeeperCommand::RealInnerStage {
            binary,
            // Blocks forever: stdin is a pipe the test holds and never
            // writes to.
            command: vec!["/bin/cat".to_string()],
        },
        Some(Duration::from_millis(50 + 2_000)),
        Some(keeper_gaps::Gap::KeeperRecordedInit),
    );
    let original_deadline_ns = launch.owner_deadline_ns.expect("owned launch");
    let mut pinned = Pinned::default();
    launch.pin_through(keeper_gaps::Gap::KeeperRecordedInit, &mut pinned);
    let init = pinned
        .iter()
        .find(|(gap, _, _)| *gap == keeper_gaps::Gap::KeeperRecordedInit as i32)
        .map(|(_, pid, _)| *pid)
        .expect("the init was pinned");
    // The gated command: the inner stage's child.
    let command = loop {
        if let Some(child) = std::fs::read_to_string(format!("/proc/{init}/task/{init}/children"))
            .ok()
            .and_then(|children| children.split_whitespace().next()?.parse().ok())
        {
            break child;
        }
        assert!(
            crate::cleanup_custody::monotonic_now_ns() < original_deadline_ns,
            "the inner stage never started the command"
        );
        std::thread::yield_now();
    };
    pinned.push((0, command, pin(command)));

    // SIGKILL the launcher (as the owner's killpg did), then let the keeper go.
    launch.kill_launcher();
    launch.resume();
    launch.pin_rest(&mut pinned);

    // Physical joins (keeper, bubblewrap, init, command; the launcher was
    // joined by `kill_launcher`), all by the original deadline.
    for (gap, pid, pidfd) in &pinned {
        let remaining = Duration::from_nanos(
            original_deadline_ns.saturating_sub(crate::cleanup_custody::monotonic_now_ns()),
        );
        assert!(
            !remaining.is_zero() && wait_readable(pidfd.as_fd(), remaining),
            "process {pid} (reported at gap {gap}) outlived the original deadline"
        );
    }
    // Reaped means proven within the deadline: after it, release is a
    // sticky timeout (expiry first).
    let report = launch.owner_report();
    assert!(report.contains("finisher=reaper\n"), "{report}");
    assert!(
        report.contains("reaper_join=finished-by-reaper\n"),
        "{report}"
    );
    assert!(report.contains("state=complete\n"), "{report}");
    assert!(!report.contains("target=timed-out"), "{report}");
    for target in &launch.targets {
        assert!(!target.exists(), "{} was not reaped", target.display());
    }
    assert_eq!(launch.keys(), Vec::<PathBuf>::new());
    drop(launch.stdin.take());
}
