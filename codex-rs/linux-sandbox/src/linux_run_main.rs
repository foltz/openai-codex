use clap::Parser;
use std::ffi::CString;
use std::fmt;
use std::fs;
use std::fs::File;
use std::io::Read;
use std::os::fd::AsFd;
use std::os::fd::AsRawFd;
use std::os::fd::BorrowedFd;
use std::os::fd::FromRawFd;
use std::os::fd::OwnedFd;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::bwrap::BwrapNetworkMode;
use crate::bwrap::BwrapOptions;
use crate::bwrap::WSL_INTEROP_DIR;
use crate::bwrap::WSLG_DISTRO_ROOT;
use crate::bwrap::create_bwrap_command_args;
use crate::launcher::exec_bwrap;
use crate::launcher::initialize_bwrap_launcher;
use crate::launcher::preferred_bwrap_supports_argv0;
use crate::proxy_routing::activate_proxy_routes_in_netns;
use crate::proxy_routing::prepare_host_proxy_route_spec;
use crate::seccomp::apply_sandbox_policies_to_current_thread;
use codex_network_proxy::ManagedNetworkSandboxContext;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::models::PermissionProfile;
use codex_protocol::protocol::FileSystemAccessMode;
use codex_protocol::protocol::FileSystemPath;
use codex_protocol::protocol::FileSystemSandboxEntry;
use codex_protocol::protocol::FileSystemSandboxPolicy;
use codex_protocol::protocol::FileSystemSpecialPath;
use codex_protocol::protocol::NetworkSandboxPolicy;
use codex_sandboxing::landlock::CODEX_LINUX_SANDBOX_ARG0;

static BWRAP_CHILD_PID: AtomicI32 = AtomicI32::new(0);
static PENDING_FORWARDED_SIGNAL: AtomicI32 = AtomicI32::new(0);

const FORWARDED_SIGNALS: &[libc::c_int] =
    &[libc::SIGHUP, libc::SIGINT, libc::SIGQUIT, libc::SIGTERM];
const LINUX_CAPABILITY_VERSION_3: u32 = 0x2008_0522;
#[derive(Debug, Parser)]
/// CLI surface for the Linux sandbox helper.
///
/// Bubblewrap is the default filesystem sandbox; Landlock is the explicit
/// legacy mode.
pub struct LinuxSandboxCommand {
    /// It is possible that the cwd used in the context of the sandbox policy
    /// is different from the cwd of the process to spawn.
    #[arg(long = "sandbox-policy-cwd")]
    pub sandbox_policy_cwd: PathBuf,

    /// The logical working directory for the command being sandboxed.
    ///
    /// This can intentionally differ from `sandbox_policy_cwd` when the
    /// command runs from a symlinked alias of the policy workspace. Keep it
    /// explicit so bubblewrap can preserve the caller's logical cwd when that
    /// alias would otherwise disappear inside the sandbox namespace.
    #[arg(long = "command-cwd", hide = true)]
    pub command_cwd: Option<PathBuf>,

    /// Canonical runtime permissions for the command.
    #[arg(
        long = "permission-profile",
        hide = true,
        value_parser = parse_permission_profile
    )]
    pub permission_profile: Option<PermissionProfile>,

    /// Opt-in: use the legacy Landlock Linux sandbox fallback.
    ///
    /// When not set, the helper uses the default bubblewrap pipeline.
    #[arg(long = "use-legacy-landlock", hide = true, default_value_t = false)]
    pub use_legacy_landlock: bool,

    /// Internal: apply seccomp and `no_new_privs` in the already-sandboxed
    /// process, then exec the user command.
    ///
    /// This exists so we can run bubblewrap first (which may rely on setuid)
    /// and only tighten with seccomp after the filesystem view is established.
    #[arg(long = "apply-seccomp-then-exec", hide = true, default_value_t = false)]
    pub apply_seccomp_then_exec: bool,

    /// Effective managed-network policy prepared for this command launch.
    #[arg(
        long,
        hide = true,
        value_parser = parse_managed_network
    )]
    pub managed_network: Option<ManagedNetworkSandboxContext>,

    /// Internal route spec used for managed proxy routing in bwrap mode.
    #[arg(long = "proxy-route-spec", hide = true)]
    pub proxy_route_spec: Option<String>,

    /// Inherited fallback mounts that must be authenticated before sandboxed code runs.
    #[arg(long = "verify-fd-mount", hide = true)]
    pub verify_fd_mounts: Vec<String>,

    /// Internal: pidfd for the bubblewrap process that launched the inner
    /// sandbox stage. Required by `--apply-seccomp-then-exec`.
    #[arg(
        long = "bwrap-parent-pidfd",
        hide = true,
        value_parser = parse_bwrap_parent_pidfd
    )]
    pub bwrap_parent_pidfd: Option<libc::c_int>,

    /// When set, skip mounting a fresh `/proc` even though PID isolation is
    /// still enabled. This is primarily intended for restrictive container
    /// environments that deny `--proc /proc`.
    #[arg(long = "no-proc", default_value_t = false)]
    pub no_proc: bool,

    /// Reuse the caller's PID namespace and `/proc` together.
    /// Only trusted executor startup may select this mode.
    #[arg(long = "inherit-pid-namespace", hide = true)]
    pub inherit_pid_namespace: bool,

    /// Owner's original cleanup deadline, absolute `CLOCK_MONOTONIC`
    /// nanoseconds. See `crate::cleanup_custody`.
    #[arg(long = crate::cleanup_custody::DEADLINE_ARG, hide = true)]
    pub cleanup_deadline_monotonic_ns: Option<u64>,

    /// Inherited pipe carrying the owner's deadline tightenings.
    #[arg(
        long = crate::cleanup_custody::DEADLINE_FD_ARG,
        hide = true,
        value_parser = crate::cleanup_custody::parse_inherited_fd
    )]
    pub cleanup_deadline_fd: Option<libc::c_int>,

    /// Inherited pipe receiving the launch's final cleanup receipt.
    #[arg(
        long = crate::cleanup_custody::REPORT_FD_ARG,
        hide = true,
        value_parser = crate::cleanup_custody::parse_inherited_fd
    )]
    pub cleanup_report_fd: Option<libc::c_int>,

    /// Inherited descriptors the sandboxed command must inherit at the same
    /// number, each `<fd>:<role>:<dev>:<ino>` (see
    /// `codex_sandboxing::FORWARD_FD_ARG`). Only these are forwarded.
    #[arg(long = "forward-fd", hide = true)]
    pub forward_fds: Vec<String>,

    /// Full command args to run under the Linux sandbox helper.
    #[arg(trailing_var_arg = true)]
    pub command: Vec<String>,
}

/// Entry point for the Linux sandbox helper.
///
/// The sequence is:
/// 1. When needed, wrap the command with bubblewrap to construct the
///    filesystem view.
/// 2. Apply in-process restrictions (no_new_privs + seccomp).
/// 3. `execvp` into the final command.
pub fn run_main() -> ! {
    let LinuxSandboxCommand {
        sandbox_policy_cwd,
        command_cwd,
        permission_profile,
        use_legacy_landlock,
        apply_seccomp_then_exec,
        managed_network,
        proxy_route_spec,
        verify_fd_mounts,
        bwrap_parent_pidfd,
        no_proc,
        inherit_pid_namespace,
        cleanup_deadline_monotonic_ns,
        cleanup_deadline_fd,
        cleanup_report_fd,
        forward_fds,
        command,
    } = LinuxSandboxCommand::parse();
    // Own (and validate) the inherited pidfd before anything else can run so
    // that every exit path, including panics, closes it.
    let bwrap_parent_pidfd = bwrap_parent_pidfd.map(adopt_bwrap_parent_pidfd);
    // The same for the owner's custody pipes; they become close-on-exec, so
    // neither bubblewrap nor the sandboxed command inherits them.
    crate::cleanup_custody::validate_pipe_pair(cleanup_deadline_fd, cleanup_report_fd)
        .unwrap_or_else(|err| panic!("refusing cleanup custody: {err}"));
    let cleanup_deadline_fd = cleanup_deadline_fd.map(|fd| {
        crate::cleanup_custody::adopt_inherited_pipe(fd, crate::cleanup_custody::DEADLINE_FD_ARG)
    });
    let cleanup_report_fd = cleanup_report_fd.map(|fd| {
        crate::cleanup_custody::adopt_inherited_pipe(fd, crate::cleanup_custody::REPORT_FD_ARG)
    });
    if apply_seccomp_then_exec
        && (cleanup_deadline_monotonic_ns.is_some()
            || cleanup_deadline_fd.is_some()
            || cleanup_report_fd.is_some())
    {
        panic!("cleanup custody is only supported in the outer launcher");
    }
    let mut custody = crate::synthetic_targets::LaunchCustody {
        deadline: crate::cleanup_custody::CleanupDeadline::new(
            cleanup_deadline_monotonic_ns,
            cleanup_deadline_fd,
        )
        .unwrap_or_else(|err| panic!("failed to adopt the cleanup deadline: {err}")),
        report: cleanup_report_fd,
    };
    if apply_seccomp_then_exec && !forward_fds.is_empty() {
        panic!("--forward-fd is only supported in the outer launcher");
    }
    // The command's forwarded descriptors: the whole list is validated
    // before any is owned, then each is marked close-on-exec at its number.
    // A refusal comes before anything of this launch is forked, installed
    // or registered, so the no-reaper declaration and the empty receipt are
    // true, and the owner joins at once.
    let custody_fds: Vec<libc::c_int> = custody
        .deadline
        .channel_fd()
        .into_iter()
        .chain(custody.report.as_ref().map(AsRawFd::as_raw_fd))
        .collect();
    let forwarded = crate::forwarded_fds::adopt_forwarded_fds(&forward_fds, &custody_fds)
        .unwrap_or_else(|err| {
            report_no_targets(&mut custody);
            eprintln!("refusing to launch the Linux sandbox: {err}");
            std::process::exit(1);
        });
    let allow_network_for_proxy = managed_network.is_some();

    if command.is_empty() {
        panic!("No command specified to execute.");
    }
    if !apply_seccomp_then_exec && !verify_fd_mounts.is_empty() {
        panic!("--verify-fd-mount is only supported in the inner sandbox stage");
    }
    if !apply_seccomp_then_exec && bwrap_parent_pidfd.is_some() {
        panic!("--bwrap-parent-pidfd is only supported in the inner sandbox stage");
    }
    ensure_inner_stage_mode_is_valid(apply_seccomp_then_exec, use_legacy_landlock);
    let EffectivePermissions {
        permission_profile,
        file_system_sandbox_policy,
        network_sandbox_policy,
    } = resolve_permission_profile(permission_profile).unwrap_or_else(|err| panic!("{err}"));
    ensure_legacy_landlock_mode_supports_policy(use_legacy_landlock, &file_system_sandbox_policy);

    // Inner stage: apply seccomp/no_new_privs after bubblewrap has already
    // established the filesystem view.
    if apply_seccomp_then_exec {
        if let Err(err) = crate::fd_mount::verify_fd_mounts(&verify_fd_mounts) {
            panic!("failed to verify descriptor-backed bubblewrap mount: {err}");
        }

        let mut capability_header = [LINUX_CAPABILITY_VERSION_3, 0];
        let mut capability_sets = [[0_u32; 3]; 2];
        // SAFETY: capability ABI version 3 uses a [version, pid] header and
        // two [effective, permitted, inheritable] capability-set entries.
        let result = unsafe {
            libc::syscall(
                libc::SYS_capget,
                capability_header.as_mut_ptr(),
                capability_sets.as_mut_ptr(),
            )
        };
        if result < 0 {
            panic!(
                "failed to verify Linux sandbox capabilities: {}",
                std::io::Error::last_os_error()
            );
        }
        if capability_sets
            .into_iter()
            .any(|[effective, permitted, _]| effective != 0 || permitted != 0)
        {
            panic!("Linux sandbox retained effective or permitted capabilities");
        }

        // Must run before proxy routing, Landlock, seccomp and the user exec:
        // it consumes and closes the pidfd.
        die_with_bwrap_parent(bwrap_parent_pidfd.unwrap_or_else(|| {
            panic!(
                "refusing to run the inner sandbox stage without --bwrap-parent-pidfd: \
                 its lifetime cannot be bound to bubblewrap"
            )
        }));

        if allow_network_for_proxy {
            let spec = proxy_route_spec
                .as_deref()
                .unwrap_or_else(|| panic!("managed proxy mode requires --proxy-route-spec"));
            if let Err(err) = activate_proxy_routes_in_netns(spec) {
                panic!("error activating Linux proxy routing bridge: {err}");
            }
        }
        let proxy_routing_active = allow_network_for_proxy;
        if let Err(e) = apply_sandbox_policies_to_current_thread(
            &file_system_sandbox_policy,
            network_sandbox_policy,
            &sandbox_policy_cwd,
            /*apply_landlock_fs*/ false,
            managed_network.as_ref(),
            proxy_routing_active,
        ) {
            panic!("error applying Linux sandbox restrictions: {e:?}");
        }

        let signal_mask = ForwardedSignalMask::block();
        let inner_stage_pid = unsafe { libc::getpid() };
        let command_pid = unsafe { libc::fork() };
        if command_pid < 0 {
            let err = std::io::Error::last_os_error();
            panic!("failed to fork sandboxed command: {err}");
        }

        if command_pid == 0 {
            reset_forwarded_signal_handlers_to_default();
            signal_mask.restore();
            // Without a private PID namespace the inner stage is not a
            // namespace init, so its death would not take the command down.
            kill_with_parent(inner_stage_pid);
            exec_or_panic(command);
        }

        let signal_forwarders = install_bwrap_signal_forwarders(command_pid);
        signal_mask.restore();
        loop {
            let mut status = 0;
            let reaped_pid = unsafe { libc::waitpid(-1, &mut status, 0) };
            if reaped_pid == command_pid {
                let exit_signal_mask = ForwardedSignalMask::block();
                signal_forwarders.restore();
                exit_signal_mask.restore();
                exit_with_wait_status(status);
            }
            if reaped_pid >= 0 {
                continue;
            }

            let err = std::io::Error::last_os_error();
            if err.raw_os_error() != Some(libc::EINTR) {
                panic!("failed to reap sandboxed child: {err}");
            }
        }
    }

    if file_system_sandbox_policy.has_full_disk_write_access() && !allow_network_for_proxy {
        if let Err(e) = apply_sandbox_policies_to_current_thread(
            &file_system_sandbox_policy,
            network_sandbox_policy,
            &sandbox_policy_cwd,
            /*apply_landlock_fs*/ false,
            managed_network.as_ref(),
            /*proxy_routing_active*/ false,
        ) {
            panic!("error applying Linux sandbox restrictions: {e:?}");
        }
        // Inheritable only for this exec of the command itself.
        report_no_targets(&mut custody);
        crate::exec_util::make_files_inheritable(&forwarded);
        exec_or_panic(command);
    }

    if !use_legacy_landlock {
        // Outer stage: bubblewrap first, then re-enter this binary in the
        // sandboxed environment to apply seccomp. This path never falls back
        // to legacy Landlock on failure.
        //
        // Refuse deny rules that no mount can enforce before anything is
        // prepared or spawned for this launch. The check has no side effects,
        // and its verdict does not depend on the bubblewrap options.
        if let Err(err) =
            crate::bwrap::validate_proc_deny_rules(&file_system_sandbox_policy, &sandbox_policy_cwd)
        {
            // Nothing of this launch was forked, installed or registered, and
            // nothing of it is alive: the only child the check can start (a
            // glob-expanding `rg`) was waited for, and never held the
            // close-on-exec custody pipes. So the declaration that no reaper
            // will exist, and the empty receipt, are true; they let the owner
            // join at once instead of waiting out its deadline.
            report_no_targets(&mut custody);
            exit_with_bwrap_build_error(err);
        }
        let (proxy_route_spec, proxy_controls) = if let Some(context) = managed_network.as_ref() {
            let (proxy_route_spec, controls) = prepare_host_proxy_route_spec(
                (!context.loopback_ports.is_empty()).then_some(context.loopback_ports.as_slice()),
            )
            .unwrap_or_else(|err| panic!("failed to prepare host proxy routing bridge: {err}"));
            (Some(proxy_route_spec), controls)
        } else {
            (None, Vec::new())
        };
        let options = BwrapOptions {
            mount_proc: !no_proc && !inherit_pid_namespace,
            inherit_pid_namespace,
            network_mode: bwrap_network_mode(network_sandbox_policy, allow_network_for_proxy),
            mask_wsl_interop: !file_system_sandbox_policy.has_full_disk_write_access()
                && Path::new(WSL_INTEROP_DIR).is_dir(),
            mask_wslg_distro: (!file_system_sandbox_policy.has_full_disk_write_access()
                || !file_system_sandbox_policy
                    .get_unreadable_globs_with_cwd(&sandbox_policy_cwd)
                    .is_empty())
                && crate::wslg::is_duplicate_root(Path::new(WSLG_DISTRO_ROOT))
                    .unwrap_or_else(|err| exit_with_bwrap_build_error(err.into())),
            ..Default::default()
        };
        if options.mask_wslg_distro
            && let Some(executable) = command
                .first()
                .filter(|executable| executable.contains('/'))
        {
            let executable = command_cwd
                .as_deref()
                .unwrap_or(&sandbox_policy_cwd)
                .join(executable);
            crate::wslg::ensure_supported_path(&executable)
                .unwrap_or_else(|err| exit_with_bwrap_build_error(err));
        }
        let inner = build_inner_seccomp_command(InnerSeccompCommandArgs {
            sandbox_policy_cwd: &sandbox_policy_cwd,
            command_cwd: command_cwd.as_deref(),
            permission_profile: &permission_profile,
            managed_network,
            proxy_route_spec,
            command,
        });
        let mut preserved_files = proxy_controls;
        preserved_files.extend(forwarded);
        run_bwrap_with_proc_fallback(
            &sandbox_policy_cwd,
            command_cwd.as_deref(),
            &file_system_sandbox_policy,
            options,
            inner,
            preserved_files,
            custody,
        );
    }

    // Legacy path: Landlock enforcement only, when bwrap sandboxing is not enabled.
    if let Err(e) = apply_sandbox_policies_to_current_thread(
        &file_system_sandbox_policy,
        network_sandbox_policy,
        &sandbox_policy_cwd,
        /*apply_landlock_fs*/ true,
        managed_network.as_ref(),
        /*proxy_routing_active*/ false,
    ) {
        panic!("error applying legacy Linux sandbox restrictions: {e:?}");
    }
    report_no_targets(&mut custody);
    // Inheritable only for this exec of the command itself.
    crate::exec_util::make_files_inheritable(&forwarded);
    exec_or_panic(command);
}

/// Reports a launch that installs no synthetic targets before exec, so the
/// owner's join does not wait on a report that will never come.
fn report_no_targets(custody: &mut crate::synthetic_targets::LaunchCustody) {
    if custody.report.is_none() {
        return;
    }
    // Before bubblewrap (and so the command) starts: no reaper will exist.
    crate::synthetic_targets::declare_reaper(custody.report.as_ref(), /*reaper*/ None);
    if let Ok(launch) = crate::synthetic_targets::ProcessToken::current() {
        crate::cleanup_custody::write_report(
            custody.report.as_ref(),
            &crate::synthetic_targets::no_target_receipt(&launch),
        );
    }
}

#[derive(Debug, Clone)]
struct EffectivePermissions {
    permission_profile: PermissionProfile,
    file_system_sandbox_policy: FileSystemSandboxPolicy,
    network_sandbox_policy: NetworkSandboxPolicy,
}

#[derive(Debug, PartialEq, Eq)]
enum ResolvePermissionProfileError {
    MissingConfiguration,
}

impl fmt::Display for ResolvePermissionProfileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingConfiguration => write!(f, "missing permission profile configuration"),
        }
    }
}

fn parse_permission_profile(value: &str) -> std::result::Result<PermissionProfile, String> {
    serde_json::from_str(value).map_err(|err| format!("invalid permission profile JSON: {err}"))
}

fn parse_bwrap_parent_pidfd(value: &str) -> std::result::Result<libc::c_int, String> {
    value
        .parse::<libc::c_int>()
        .ok()
        .filter(|fd| *fd > libc::STDERR_FILENO)
        .ok_or_else(|| format!("invalid bubblewrap parent pidfd: {value:?}"))
}

/// Take ownership of the pidfd passed by the trusted launcher, refusing to
/// run unless it is an open pidfd.
fn adopt_bwrap_parent_pidfd(fd: libc::c_int) -> OwnedFd {
    match adopt_bwrap_parent_pidfd_with(fd, pidfd_send_signal_0) {
        Ok(pidfd) => pidfd,
        Err(err) => {
            panic!("refusing to run the inner sandbox stage: --bwrap-parent-pidfd {fd} {err}")
        }
    }
}

/// [`adopt_bwrap_parent_pidfd`] with the `pidfd_send_signal` probe injected.
/// Every `Err` is returned after the adopted descriptor has been closed.
fn adopt_bwrap_parent_pidfd_with(
    fd: libc::c_int,
    send_signal: impl FnOnce(BorrowedFd<'_>) -> std::io::Result<()>,
) -> std::result::Result<OwnedFd, String> {
    // Only adopt a descriptor that is actually open so `OwnedFd` never closes
    // a descriptor number it does not own.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 {
        let err = std::io::Error::last_os_error();
        return Err(format!("is not open: {err}"));
    }
    // SAFETY: the launcher transfers ownership of this open descriptor to the
    // inner stage; nothing else in this process refers to it.
    let pidfd = unsafe { OwnedFd::from_raw_fd(fd) };
    // Re-set FD_CLOEXEC so nothing exec'd from here can inherit it. On every
    // refusal below `pidfd` is dropped, which closes it.
    if unsafe { libc::fcntl(pidfd.as_raw_fd(), libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
        let err = std::io::Error::last_os_error();
        return Err(format!("could not be marked close-on-exec: {err}"));
    }
    ensure_is_pidfd(pidfd.as_fd(), send_signal)?;
    Ok(pidfd)
}

// `i128` holds every `statfs::f_type` value losslessly; see `ensure_is_pidfd`.
const PIDFS_MAGIC: i128 = 0x5049_4446;
const ANON_INODE_FS_MAGIC: i128 = 0x0904_1934;

/// `pidfd_send_signal(fd, 0, NULL, 0)`: existence and permission checks only.
fn pidfd_send_signal_0(fd: BorrowedFd<'_>) -> std::io::Result<()> {
    // SAFETY: a null siginfo with flags 0 is the documented kill(2)-like form.
    let res = unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            fd.as_raw_fd(),
            0,
            std::ptr::null::<libc::siginfo_t>(),
            0,
        )
    };
    if res < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// Check that `fd` is a pidfd without relying on `/proc`, which may be absent
/// or masked inside the sandbox. Both checks must pass.
///
/// 1. pidfds live on pidfs (Linux 6.9+) or the anonymous inode filesystem.
///    This also refuses `/proc/<pid>` directory fds, which
///    `pidfd_send_signal` accepts as legacy pidfds.
/// 2. `send_signal` (`pidfd_send_signal(fd, 0, NULL, 0)` in production) must
///    end in an outcome the kernel only produces after it resolved `fd` to a
///    pid (Linux `kernel/signal.c`, `sys_pidfd_send_signal`): success,
///    `ESRCH` (the process is gone), `EPERM` (no permission to signal it), or
///    `EINVAL`, which with signal 0, a null siginfo and flags 0 can only come
///    from `access_pidfd_pidns`, i.e. a target outside our PID namespace. That
///    is the normal case for bubblewrap seen from the sandbox. Any other file
///    fails with `EBADF`. Every other result, including `ENOSYS` (Linux < 5.1
///    or a seccomp filter) and `EFAULT`, proves nothing and is refused.
fn ensure_is_pidfd(
    fd: BorrowedFd<'_>,
    send_signal: impl FnOnce(BorrowedFd<'_>) -> std::io::Result<()>,
) -> std::result::Result<(), String> {
    let mut statfs: libc::statfs = unsafe { std::mem::zeroed() };
    // SAFETY: `statfs` is valid writable storage for the call.
    if unsafe { libc::fstatfs(fd.as_raw_fd(), &mut statfs) } < 0 {
        let err = std::io::Error::last_os_error();
        return Err(format!("cannot be inspected: {err}"));
    }
    // `f_type` is `i64` on 64-bit glibc, `i32` on 32-bit glibc, `u64` on musl
    // and `u32` on s390x. `i128::from` widens each of them losslessly without
    // a cast, so no target needs a lint waiver.
    let filesystem = statfs.f_type;
    let magic = i128::from(filesystem);
    if magic != PIDFS_MAGIC && magic != ANON_INODE_FS_MAGIC {
        return Err(format!("is not a pidfd (filesystem magic {filesystem:#x})"));
    }
    let Err(err) = send_signal(fd) else {
        return Ok(());
    };
    match err.raw_os_error() {
        Some(libc::ESRCH | libc::EPERM | libc::EINVAL) => Ok(()),
        Some(libc::EBADF) => Err(format!("is not a pidfd (pidfd_send_signal: {err})")),
        _ => Err(format!(
            "could not be validated as a pidfd (validation unobservable): pidfd_send_signal: {err}"
        )),
    }
}

fn parse_managed_network(value: &str) -> std::result::Result<ManagedNetworkSandboxContext, String> {
    serde_json::from_str(value)
        .map_err(|err| format!("invalid managed network context JSON: {err}"))
}

fn resolve_permission_profile(
    permission_profile: Option<PermissionProfile>,
) -> Result<EffectivePermissions, ResolvePermissionProfileError> {
    let permission_profile =
        permission_profile.ok_or(ResolvePermissionProfileError::MissingConfiguration)?;
    let (file_system_sandbox_policy, network_sandbox_policy) =
        permission_profile.to_runtime_permissions();
    Ok(EffectivePermissions {
        permission_profile,
        file_system_sandbox_policy,
        network_sandbox_policy,
    })
}

fn ensure_inner_stage_mode_is_valid(apply_seccomp_then_exec: bool, use_legacy_landlock: bool) {
    if apply_seccomp_then_exec && use_legacy_landlock {
        panic!("--apply-seccomp-then-exec is incompatible with --use-legacy-landlock");
    }
}

fn ensure_legacy_landlock_mode_supports_policy(
    use_legacy_landlock: bool,
    file_system_sandbox_policy: &FileSystemSandboxPolicy,
) {
    if use_legacy_landlock && !file_system_sandbox_policy.has_full_disk_write_access() {
        panic!("filesystem-restricted execution requires bubblewrap to isolate app-server sockets");
    }
}

fn run_bwrap_with_proc_fallback(
    sandbox_policy_cwd: &Path,
    command_cwd: Option<&Path>,
    file_system_sandbox_policy: &FileSystemSandboxPolicy,
    mut options: BwrapOptions,
    inner: Vec<String>,
    preserved_files: Vec<File>,
    custody: crate::synthetic_targets::LaunchCustody,
) -> ! {
    let command_cwd = command_cwd.unwrap_or(sandbox_policy_cwd);

    // Select before the preflight, whose synthetic policy omits writable roots.
    initialize_bwrap_launcher(file_system_sandbox_policy, sandbox_policy_cwd);

    if options.mount_proc
        && !preflight_proc_mount_support(options)
            .unwrap_or_else(|err| exit_with_bwrap_build_error(err))
    {
        // Keep the retry silent so sandbox-internal diagnostics do not leak into the
        // child process stderr stream.
        options.mount_proc = false;
    }

    let mut bwrap_args = build_bwrap_argv(
        inner,
        file_system_sandbox_policy,
        sandbox_policy_cwd,
        command_cwd,
        options,
    )
    .unwrap_or_else(|err| exit_with_bwrap_build_error(err));
    // Explicitly forwarded command descriptors: preserved (so kept by the
    // keeper's whitelist) and made inheritable only in the process that
    // execs bubblewrap, at the same numbers.
    bwrap_args.preserved_files.extend(preserved_files);
    apply_inner_command_argv0(&mut bwrap_args.args);
    run_or_exec_bwrap(bwrap_args, custody);
}

fn bwrap_network_mode(
    network_sandbox_policy: NetworkSandboxPolicy,
    allow_network_for_proxy: bool,
) -> BwrapNetworkMode {
    if allow_network_for_proxy {
        BwrapNetworkMode::ProxyOnly
    } else if network_sandbox_policy.is_enabled() {
        BwrapNetworkMode::FullAccess
    } else {
        BwrapNetworkMode::Isolated
    }
}

fn build_bwrap_argv(
    inner: Vec<String>,
    file_system_sandbox_policy: &FileSystemSandboxPolicy,
    sandbox_policy_cwd: &Path,
    command_cwd: &Path,
    options: BwrapOptions,
) -> CodexResult<crate::bwrap::BwrapArgs> {
    let bwrap_args = create_bwrap_command_args(
        inner,
        file_system_sandbox_policy,
        sandbox_policy_cwd,
        command_cwd,
        options,
    )?;

    let mut argv = vec!["bwrap".to_string()];
    argv.extend(bwrap_args.args);
    Ok(crate::bwrap::BwrapArgs {
        args: argv,
        preserved_files: bwrap_args.preserved_files,
        synthetic_mount_targets: bwrap_args.synthetic_mount_targets,
    })
}

fn exit_with_bwrap_build_error(err: codex_protocol::error::CodexErr) -> ! {
    eprintln!("error building bubblewrap command: {err}");
    std::process::exit(1);
}

fn apply_inner_command_argv0(argv: &mut Vec<String>) {
    apply_inner_command_argv0_for_launcher(
        argv,
        preferred_bwrap_supports_argv0(),
        current_process_argv0(),
    );
}

fn apply_inner_command_argv0_for_launcher(
    argv: &mut Vec<String>,
    supports_argv0: bool,
    argv0_fallback_command: String,
) {
    let command_separator_index = argv
        .iter()
        .position(|arg| arg == "--")
        .unwrap_or_else(|| panic!("bubblewrap argv is missing command separator '--'"));

    if supports_argv0 {
        argv.splice(
            command_separator_index..command_separator_index,
            ["--argv0".to_string(), CODEX_LINUX_SANDBOX_ARG0.to_string()],
        );
        return;
    }

    let command_index = command_separator_index + 1;
    let Some(command) = argv.get_mut(command_index) else {
        panic!("bubblewrap argv is missing inner command after '--'");
    };
    *command = argv0_fallback_command;
}

fn current_process_argv0() -> String {
    match std::env::args_os().next() {
        Some(argv0) => argv0.to_string_lossy().into_owned(),
        None => panic!("failed to resolve current process argv[0]"),
    }
}

fn preflight_proc_mount_support(options: BwrapOptions) -> CodexResult<bool> {
    let preflight_argv = build_preflight_bwrap_argv(options)?;
    let stderr = run_bwrap_in_child_capture_stderr(preflight_argv);
    Ok(!is_proc_mount_failure(stderr.as_str()))
}

fn build_preflight_bwrap_argv(options: BwrapOptions) -> CodexResult<crate::bwrap::BwrapArgs> {
    let file_system_sandbox_policy =
        FileSystemSandboxPolicy::restricted(vec![FileSystemSandboxEntry {
            path: FileSystemPath::Special {
                value: FileSystemSpecialPath::Minimal,
            },
            access: FileSystemAccessMode::Read,
            missing_path_behavior: None,
        }]);
    let preflight_command = vec![resolve_true_command()];
    build_bwrap_argv(
        preflight_command,
        &file_system_sandbox_policy,
        Path::new("/"),
        Path::new("/"),
        BwrapOptions {
            mount_proc: true,
            // The alias check must see the same WSL masks as the main sandbox.
            ..options
        },
    )
}

fn resolve_true_command() -> String {
    for candidate in ["/usr/bin/true", "/bin/true"] {
        if Path::new(candidate).exists() {
            return candidate.to_string();
        }
    }
    "true".to_string()
}

fn run_or_exec_bwrap(
    bwrap_args: crate::bwrap::BwrapArgs,
    mut custody: crate::synthetic_targets::LaunchCustody,
) -> ! {
    if bwrap_args.synthetic_mount_targets.is_empty() {
        report_no_targets(&mut custody);
        exec_bwrap(bwrap_args.args, bwrap_args.preserved_files);
    }
    run_bwrap_through_keeper(bwrap_args, custody);
}

// ---------------------------------------------------------------------------
// Keeper launches
// ---------------------------------------------------------------------------
//
// Process tree: owner -> launcher (L, in the owner's kill group) -> keeper (K,
// its own process group, same session, no parent-death signal, a child
// subreaper exactly when the sandbox has a private PID namespace) ->
// bubblewrap (B, its own process group, parent-death SIGTERM aimed at K) ->
// sandbox init.
//
// L forks K before anything exists. L then writes the `running` receipt that
// names K as the launch's reaper, declares K to the owner, and only then
// admits K over a private channel. K creates and holds every target (by its
// `O_PATH` descriptors), forks B, records its evidence (bubblewrap, then the
// init, in two separate registry locks), waits for B, finishes the receipt
// as the launch's reaper, and sends B's wait status to L, which exits with
// it. If L is killed, K is unaffected: it delivers the cancel L's death used
// to deliver (SIGTERM to B) and finishes cleanup within the owner's original
// deadline, or, with no owner deadline, records everything as incomplete and
// removes nothing.

/// Launcher to keeper: the launcher declared the keeper; it may act.
const KEEPER_ADMIT: u8 = b'A';
/// Launcher to keeper: the launcher could not declare the keeper; it must
/// exit without any side effect.
const KEEPER_REFUSE: u8 = b'R';
/// Keeper to launcher: bubblewrap's raw wait status follows (`i32`, native
/// endianness).
const KEEPER_STATUS: u8 = b'S';
/// Keeper to launcher: a refusal or failure message for the launcher's
/// stderr follows (UTF-8).
const KEEPER_ERROR: u8 = b'E';
/// Keeper to launcher: a warning for the launcher's stderr follows (UTF-8);
/// the final message is still to come.
const KEEPER_WARNING: u8 = b'W';
/// Keeper to launcher: the keeper refused to start **before any side
/// effect** (it installed, created and forked nothing); the reason follows
/// (UTF-8). The launcher then finishes the receipt it declared itself.
const KEEPER_REFUSED: u8 = b'X';

/// Test-only gap hook: reports each startup step with the pid it concerns
/// and, at the selected step, blocks until the test resumes it. Compiled out
/// of every non-test build.
#[cfg(test)]
pub(crate) mod keeper_gaps {
    use std::sync::atomic::AtomicI32;
    use std::sync::atomic::Ordering;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    #[repr(i32)]
    pub(crate) enum Gap {
        /// L forked K; nothing is declared (pid = K).
        LauncherForkedKeeper = 1,
        /// L wrote the receipt and the owner declaration and admitted K (pid = K).
        LauncherAdmittedKeeper = 2,
        /// K registered (created or joined) every target; B not forked (pid = K).
        KeeperRegistered = 3,
        /// K forked B, which is parked before exec; nothing recorded (pid = B).
        KeeperForkedBwrap = 4,
        /// K recorded the bubblewrap token (its own lock); B still parked (pid = B).
        KeeperRecordedBwrap = 5,
        /// K released B's exec; the init is not recorded yet (pid = B).
        KeeperReleasedBwrap = 6,
        /// K recorded the init token (a second, separate lock) (pid = init).
        KeeperRecordedInit = 7,
    }

    /// Write end of the test's report pipe (`-1`: no hook).
    pub(crate) static REPORT_FD: AtomicI32 = AtomicI32::new(-1);
    /// Read end of the test's resume pipe.
    pub(crate) static RESUME_FD: AtomicI32 = AtomicI32::new(-1);
    /// The step at which to block (`0`: never).
    pub(crate) static STOP_AT: AtomicI32 = AtomicI32::new(0);
    /// The forced order of a keeper refusal against the launcher's
    /// admission (`0`: no ordering forced).
    pub(crate) static REFUSAL_ORDER: AtomicI32 = AtomicI32::new(0);
    /// The keeper refuses and exits before the launcher declares it.
    pub(crate) const REFUSE_BEFORE_DECLARATION: i32 = 1;
    /// The launcher's admission is queued, unread, when the keeper refuses
    /// and exits, and the launcher reads only after that exit.
    pub(crate) const REFUSE_AFTER_ADMISSION: i32 = 2;

    pub(crate) fn refusal_order() -> i32 {
        REFUSAL_ORDER.load(Ordering::SeqCst)
    }

    /// Blocks until `pid` (the caller's unreaped child) has exited.
    pub(crate) fn wait_for_exit(pid: libc::pid_t) {
        // SAFETY: pidfd_open on our own unreaped child.
        let pidfd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
        if pidfd < 0 {
            return;
        }
        let mut poll_fd = libc::pollfd {
            fd: pidfd as libc::c_int,
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: one valid pollfd; the descriptor is closed below.
        while unsafe { libc::poll(&mut poll_fd, 1, -1) } < 0 {}
        // SAFETY: closing the pidfd opened above.
        unsafe { libc::close(pidfd as libc::c_int) };
    }

    /// Blocks until `fd` has something to read, without reading it.
    pub(crate) fn wait_until_readable(fd: libc::c_int) {
        let mut poll_fd = libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: one valid pollfd.
        while unsafe { libc::poll(&mut poll_fd, 1, -1) } < 0 {}
    }

    pub(crate) fn reached(gap: Gap, pid: libc::pid_t) {
        let report = REPORT_FD.load(Ordering::SeqCst);
        if report < 0 {
            return;
        }
        let record = [gap as i32, pid];
        // SAFETY: a valid buffer of two i32s.
        unsafe {
            libc::write(
                report,
                record.as_ptr().cast(),
                std::mem::size_of_val(&record),
            );
        }
        if STOP_AT.load(Ordering::SeqCst) == gap as i32 {
            let mut byte = 0_u8;
            // SAFETY: a valid one-byte buffer.
            unsafe {
                libc::read(
                    RESUME_FD.load(Ordering::SeqCst),
                    (&mut byte as *mut u8).cast(),
                    1,
                );
            }
        }
    }
}

macro_rules! keeper_gap {
    ($gap:ident, $pid:expr) => {
        #[cfg(test)]
        keeper_gaps::reached(keeper_gaps::Gap::$gap, $pid);
    };
}

/// What the keeper inherits from the launcher at the fork. Fork duplicates
/// the descriptor table in one step; the launcher then drops its copies of
/// everything here except standard I/O.
struct KeeperStart {
    args: Vec<String>,
    preserved_files: Vec<File>,
    synthetic_mount_targets: Vec<crate::bwrap::SyntheticMountTarget>,
    pid_namespace: bool,
    launcher: crate::synthetic_targets::ProcessToken,
    launcher_pidfd: OwnedFd,
    channel: OwnedFd,
    custody: crate::synthetic_targets::LaunchCustody,
    setup_signal_mask: ForwardedSignalMask,
}

enum KeeperOutcome {
    Status(libc::c_int),
    Error(String),
    /// A typed pre-side-effect refusal (`KEEPER_REFUSED`).
    Refused(String),
}

fn run_bwrap_through_keeper(
    bwrap_args: crate::bwrap::BwrapArgs,
    mut custody: crate::synthetic_targets::LaunchCustody,
) -> ! {
    let crate::bwrap::BwrapArgs {
        args,
        preserved_files,
        synthetic_mount_targets,
    } = bwrap_args;
    let setup_signal_mask = ForwardedSignalMask::block();
    let pid_namespace = args
        .iter()
        .take_while(|arg| arg.as_str() != "--")
        .any(|arg| arg == "--unshare-pid");
    let launcher = crate::synthetic_targets::ProcessToken::current()
        .unwrap_or_else(|err| panic!("failed to identify the sandbox launcher: {err}"));
    let (launcher_channel, keeper_channel) = keeper_channel_pair()
        .unwrap_or_else(|err| panic!("failed to create the sandbox keeper channel: {err}"));
    let launcher_pidfd = open_own_pidfd()
        .unwrap_or_else(|err| panic!("refusing to launch the Linux sandbox: {err}"));
    let keeper_pid = unsafe { libc::fork() };
    if keeper_pid < 0 {
        let err = std::io::Error::last_os_error();
        panic!("failed to fork the sandbox keeper: {err}");
    }
    if keeper_pid == 0 {
        drop(launcher_channel);
        run_keeper(KeeperStart {
            args,
            preserved_files,
            synthetic_mount_targets,
            pid_namespace,
            launcher,
            launcher_pidfd,
            channel: keeper_channel,
            custody,
            setup_signal_mask,
        });
    }
    drop(keeper_channel);
    drop(launcher_pidfd);
    drop(preserved_files);
    // Both sides move the keeper out of the launcher's group (the owner's
    // kill target), so neither order of execution leaves a window. A keeper
    // that already exited is reported by the relay below.
    unsafe {
        libc::setpgid(keeper_pid, keeper_pid);
    }
    // Forwarded signals stay blocked until the forwarder to the keeper is
    // armed; only then are they unmasked, before the keeper is admitted.
    let signal_forwarders = arm_launcher_relay(keeper_pid, &setup_signal_mask);
    keeper_gap!(LauncherForkedKeeper, keeper_pid);
    #[cfg(test)]
    if keeper_gaps::refusal_order() == keeper_gaps::REFUSE_BEFORE_DECLARATION {
        keeper_gaps::wait_for_exit(keeper_pid);
    }
    let (declared, refusal) = match declare_keeper(keeper_pid, launcher, &mut custody) {
        Ok(ids) => {
            send_keeper_message(&launcher_channel, &[KEEPER_ADMIT]);
            keeper_gap!(LauncherAdmittedKeeper, keeper_pid);
            #[cfg(test)]
            if keeper_gaps::refusal_order() == keeper_gaps::REFUSE_AFTER_ADMISSION {
                keeper_gaps::wait_for_exit(keeper_pid);
            }
            (Some(ids), None)
        }
        Err(message) => {
            send_keeper_message(&launcher_channel, &[KEEPER_REFUSE]);
            (None, Some(message))
        }
    };
    let outcome = receive_keeper_outcome(&launcher_channel);
    // Only a typed pre-side-effect refusal leaves the launcher's declared,
    // empty receipt to the launcher: it finishes it (bounded by the owner's
    // original deadline; one non-blocking try after a stop). Otherwise the
    // keeper owns the deadline channel and the report pipe.
    if let (Some(KeeperOutcome::Refused(_)), Some(ids)) = (&outcome, &declared) {
        let finished = crate::synthetic_targets::finish_refused_keeper(ids, &mut custody);
        if let Some(error) = &finished.persist_error {
            eprintln!(
                "codex-linux-sandbox: the refused launch's receipt could not be persisted ({error}); the registry still records it as running, and the owner was told it is {}",
                finished.state_name()
            );
        }
    }
    drop(custody);
    let keeper_status = wait_for_bwrap_child(keeper_pid);
    let cleanup_signal_mask = ForwardedSignalMask::block();
    BWRAP_CHILD_PID.store(0, Ordering::SeqCst);
    signal_forwarders.restore();
    cleanup_signal_mask.restore();
    if let Some(message) = refusal {
        eprintln!("{message}");
        std::process::exit(1);
    }
    match outcome {
        Some(KeeperOutcome::Status(status)) => exit_with_wait_status(status),
        Some(KeeperOutcome::Error(message)) | Some(KeeperOutcome::Refused(message)) => {
            eprintln!("{message}");
            std::process::exit(1);
        }
        None => {
            eprintln!(
                "codex-linux-sandbox: the sandbox keeper exited without reporting the sandboxed command's status ({})",
                describe_wait_status(keeper_status)
            );
            std::process::exit(1);
        }
    }
}

/// The launcher's declaration of its keeper, before the keeper may act: the
/// `running` receipt (registry) naming the keeper as reaper, then the owner's
/// helper declaration. Both are bounded by the owner's original deadline.
fn declare_keeper(
    keeper_pid: libc::pid_t,
    launcher: crate::synthetic_targets::ProcessToken,
    custody: &mut crate::synthetic_targets::LaunchCustody,
) -> Result<crate::synthetic_targets::LaunchIdentity, String> {
    // The keeper is this process's unreaped child: its pid cannot be reused,
    // so its token is exact.
    let keeper = crate::synthetic_targets::ProcessToken::for_pid(keeper_pid).map_err(|err| {
        format!("refusing to launch the Linux sandbox: cannot identify the sandbox keeper: {err}")
    })?;
    let ids = crate::synthetic_targets::LaunchIdentity::keeper(launcher, keeper.clone());
    let registry = crate::synthetic_targets::open_shared_registry().map_err(|err| {
        format!(
            "refusing to launch the Linux sandbox: synthetic mount targets need the trusted sandbox registry, which is unavailable: {err}"
        )
    })?;
    let Some(_lock) = registry.lock_within(&mut custody.deadline).map_err(|err| {
        format!(
            "refusing to launch the Linux sandbox: synthetic mount targets need the trusted sandbox registry, which is unavailable: {err}"
        )
    })?
    else {
        return Err(
            "refusing to launch the Linux sandbox: the owner's cleanup deadline passed, or the owner is gone, before the sandbox keeper could be declared"
                .to_string(),
        );
    };
    crate::synthetic_targets::declare_keeper_receipt(&registry, &ids).map_err(|err| {
        format!("refusing to launch the Linux sandbox: cannot declare the sandbox keeper: {err}")
    })?;
    crate::synthetic_targets::declare_reaper(custody.report.as_ref(), Some(&keeper));
    Ok(ids)
}

fn describe_wait_status(status: libc::c_int) -> String {
    if libc::WIFEXITED(status) {
        format!("exit status {}", libc::WEXITSTATUS(status))
    } else if libc::WIFSIGNALED(status) {
        format!("killed by signal {}", libc::WTERMSIG(status))
    } else {
        format!("wait status {status}")
    }
}

fn keeper_channel_pair() -> std::io::Result<(OwnedFd, OwnedFd)> {
    let mut fds = [-1; 2];
    // SAFETY: valid two-element array.
    if unsafe {
        libc::socketpair(
            libc::AF_UNIX,
            libc::SOCK_SEQPACKET | libc::SOCK_CLOEXEC,
            0,
            fds.as_mut_ptr(),
        )
    } < 0
    {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: socketpair returned two new owned descriptors.
    Ok(unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) })
}

fn open_own_pidfd() -> std::io::Result<OwnedFd> {
    // SAFETY: pidfd_open takes a pid and flags and returns a new descriptor.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, libc::getpid(), 0) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: the kernel returned a new owned descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(fd as libc::c_int) })
}

/// One message on the keeper channel; a peer that is gone is not an error
/// here (the receiver reports it), and never raises SIGPIPE.
fn send_keeper_message(channel: &OwnedFd, message: &[u8]) {
    loop {
        // SAFETY: valid descriptor and buffer.
        let sent = unsafe {
            libc::send(
                channel.as_raw_fd(),
                message.as_ptr().cast(),
                message.len(),
                libc::MSG_NOSIGNAL,
            )
        };
        if sent >= 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
            return;
        }
    }
}

/// Reads one message; `None` at EOF (the peer is gone) or on error.
///
/// `ECONNRESET` is not the end of the channel. When a peer closes its end
/// while its own receive queue still holds a message it never read (a keeper
/// that refuses after the launcher's admission is queued, but before reading
/// it), AF_UNIX sets `ECONNRESET` as a pending error on this end. The datagram
/// receive path reports a pending error *before* looking at the queue
/// (`__skb_try_recv_datagram`: `sock_error()` first) and clears it as it does
/// so, so the message the peer sent before closing is still queued: the next
/// `recv` returns it, and only then end-of-file. Retrying once per reported
/// reset terminates, because the error is cleared when reported.
fn receive_keeper_message(channel: &OwnedFd) -> Option<Vec<u8>> {
    let mut buffer = vec![0_u8; 4096];
    loop {
        // SAFETY: valid descriptor and buffer.
        let received = unsafe {
            libc::recv(
                channel.as_raw_fd(),
                buffer.as_mut_ptr().cast(),
                buffer.len(),
                0,
            )
        };
        if received > 0 {
            buffer.truncate(received as usize);
            return Some(buffer);
        }
        if received == 0 {
            return None;
        }
        match std::io::Error::last_os_error().raw_os_error() {
            Some(libc::EINTR) | Some(libc::ECONNRESET) => {}
            _ => return None,
        }
    }
}

/// The keeper's final message. Warnings before it (a failed persist) are
/// printed to the launcher's stderr as they arrive.
fn receive_keeper_outcome(channel: &OwnedFd) -> Option<KeeperOutcome> {
    loop {
        let message = receive_keeper_message(channel)?;
        match message.split_first()? {
            (&KEEPER_STATUS, status) => {
                return Some(KeeperOutcome::Status(libc::c_int::from_ne_bytes(
                    status.try_into().ok()?,
                )));
            }
            (&KEEPER_ERROR, text) => {
                return Some(KeeperOutcome::Error(
                    String::from_utf8_lossy(text).into_owned(),
                ));
            }
            (&KEEPER_REFUSED, text) => {
                return Some(KeeperOutcome::Refused(
                    String::from_utf8_lossy(text).into_owned(),
                ));
            }
            (&KEEPER_WARNING, text) => eprintln!("{}", String::from_utf8_lossy(text)),
            _ => return None,
        }
    }
}

fn send_keeper_warning(channel: &OwnedFd, text: &str) {
    let mut message = vec![KEEPER_WARNING];
    message.extend_from_slice(text.as_bytes());
    send_keeper_message(channel, &message);
}

/// The launcher's relay to its keeper: the forwarder is armed first, and only
/// then are the forwarded signals (blocked since before the fork) unmasked,
/// so a signal that arrived meanwhile is forwarded, never lost or acted on.
fn arm_launcher_relay(
    keeper_pid: libc::pid_t,
    setup_signal_mask: &ForwardedSignalMask,
) -> ForwardedSignalHandlers {
    let forwarders = install_bwrap_signal_forwarders(keeper_pid);
    setup_signal_mask.restore();
    forwarders
}

fn send_keeper_status(channel: &OwnedFd, status: libc::c_int) {
    let mut message = vec![KEEPER_STATUS];
    message.extend_from_slice(&status.to_ne_bytes());
    send_keeper_message(channel, &message);
}

/// The keeper's refusal before any side effect: typed, so the launcher never
/// has to interpret a message's text.
fn send_keeper_refusal(channel: &OwnedFd, text: &str) {
    let mut message = vec![KEEPER_REFUSED];
    message.extend_from_slice(text.as_bytes());
    send_keeper_message(channel, &message);
}

fn send_keeper_error(channel: &OwnedFd, text: &str) {
    let mut message = vec![KEEPER_ERROR];
    message.extend_from_slice(text.as_bytes());
    send_keeper_message(channel, &message);
}

enum KeeperExit {
    /// The keeper finished (or exited without any side effect) and has
    /// reported what the launcher must report.
    Done,
    /// The launcher must exit 1 with this message.
    Failed(String),
    /// Refused before any side effect: the launcher finishes its declared,
    /// empty receipt, then exits 1 with this message.
    Refused(String),
}

/// The keeper process: never returns. Its result reaches the launcher over
/// the channel; a panic is reported the same way, as a failure.
fn run_keeper(start: KeeperStart) -> ! {
    // First, before any side effect: the keeper was forked without an exec,
    // so close-on-exec never applied. Keep exactly its own descriptors, or,
    // when that cannot be proven, refuse to run at all: the launcher reports
    // the keeper's failure and exits 1.
    if let Err(message) = close_keeper_inherited_descriptors(&start) {
        #[cfg(test)]
        if keeper_gaps::refusal_order() == keeper_gaps::REFUSE_AFTER_ADMISSION {
            // The admission is queued here, unread, when the keeper closes.
            keeper_gaps::wait_until_readable(start.channel.as_raw_fd());
        }
        send_keeper_refusal(
            &start.channel,
            &format!(
                "codex-linux-sandbox: the sandbox keeper refuses to start: it cannot prove that every inherited descriptor is closed ({message})"
            ),
        );
        // SAFETY: terminate the forked keeper without running the launcher's
        // destructors or exit handlers.
        unsafe { libc::_exit(1) }
    }
    // The launcher prints the keeper's failures; keep them out of the
    // command's stderr.
    std::panic::set_hook(Box::new(|_| {}));
    let report = start.channel.try_clone().ok();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| keeper_main(start)));
    let code = match result {
        Ok(KeeperExit::Done) => 0,
        Ok(KeeperExit::Failed(message)) => {
            if let Some(report) = &report {
                send_keeper_error(report, &message);
            }
            1
        }
        Ok(KeeperExit::Refused(message)) => {
            if let Some(report) = &report {
                send_keeper_refusal(report, &message);
            }
            1
        }
        Err(panic) => {
            let detail = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("unknown failure");
            if let Some(report) = &report {
                send_keeper_error(
                    report,
                    &format!("codex-linux-sandbox: the sandbox keeper failed: {detail}"),
                );
            }
            1
        }
    };
    // SAFETY: terminate the forked keeper without running the launcher's
    // destructors or exit handlers.
    unsafe { libc::_exit(code) }
}

/// Every descriptor the keeper may keep, exactly: standard I/O (bubblewrap's,
/// until it is forked), its channel to the launcher and the launcher's pidfd,
/// the owner's deadline channel and report pipe, bubblewrap's preserved
/// files, and (unit tests only) the gap hook's two pipes.
fn keeper_descriptor_whitelist(start: &KeeperStart) -> Vec<libc::c_int> {
    let mut keep = vec![
        libc::STDIN_FILENO,
        libc::STDOUT_FILENO,
        libc::STDERR_FILENO,
        start.channel.as_raw_fd(),
        start.launcher_pidfd.as_raw_fd(),
    ];
    keep.extend(start.custody.deadline.channel_fd());
    keep.extend(start.custody.report.as_ref().map(AsRawFd::as_raw_fd));
    keep.extend(start.preserved_files.iter().map(AsRawFd::as_raw_fd));
    #[cfg(test)]
    keep.extend(
        [
            keeper_gaps::REPORT_FD.load(Ordering::SeqCst),
            keeper_gaps::RESUME_FD.load(Ordering::SeqCst),
        ]
        .into_iter()
        .filter(|fd| *fd >= 0),
    );
    keep
}

/// Closes every descriptor the keeper inherited except its whitelist. A fork
/// without an exec does not honour close-on-exec, so anything else the
/// launcher held (another launch's pipes, a test harness's writers, the
/// launcher's own inheritance) would otherwise stay open for the keeper's
/// whole life, pinning unrelated EOFs and the owner's release. None of those
/// descriptors is owned by anything the keeper ever drops: it never returns
/// to the launcher's frames.
fn close_keeper_inherited_descriptors(start: &KeeperStart) -> Result<(), String> {
    close_descriptors_except(&keeper_descriptor_whitelist(start))
}

#[cfg(test)]
thread_local! {
    /// Unit-test-only: `close_range` reports `ENOSYS`, as on an old kernel.
    static FORCE_CLOSE_RANGE_UNSUPPORTED: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
    /// Unit-test-only: `/proc/self/fd` cannot be enumerated.
    static FORCE_PROC_FD_UNAVAILABLE: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

/// `close_range(2)` over `first..=last`, without flags.
fn close_range(first: libc::c_uint, last: libc::c_uint) -> std::io::Result<()> {
    #[cfg(test)]
    if FORCE_CLOSE_RANGE_UNSUPPORTED.with(std::cell::Cell::get) {
        return Err(std::io::Error::from_raw_os_error(libc::ENOSYS));
    }
    let flags: libc::c_uint = 0;
    // SAFETY: closes descriptor numbers only.
    let rc = unsafe { libc::syscall(libc::SYS_close_range, first, last, flags) };
    if rc < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// The open descriptor numbers, from `/proc/self/fd`.
fn open_descriptors() -> std::io::Result<Vec<libc::c_int>> {
    #[cfg(test)]
    if FORCE_PROC_FD_UNAVAILABLE.with(std::cell::Cell::get) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "/proc/self/fd is unavailable (test)",
        ));
    }
    let mut open = Vec::new();
    for entry in fs::read_dir("/proc/self/fd")? {
        let name = entry?.file_name();
        let fd = name
            .to_str()
            .and_then(|name| name.parse().ok())
            .ok_or_else(|| {
                std::io::Error::other(format!("unexpected /proc/self/fd entry {name:?}"))
            })?;
        open.push(fd);
    }
    Ok(open)
}

fn descriptor_is_open(fd: libc::c_int) -> bool {
    // SAFETY: F_GETFD only reads the descriptor's flags.
    unsafe { libc::fcntl(fd, libc::F_GETFD) >= 0 }
}

/// Closes every descriptor not in `keep`, and returns `Ok` only when that is
/// proven for every possible descriptor number:
///
/// 1. `close_range(2)` over each gap between the sorted whitelist, up to
///    `~0U`: one call per gap, whatever the descriptor limit (a descriptor
///    opened above a since-lowered limit is closed too);
/// 2. on `ENOSYS`/`EINVAL` (a kernel without it), every descriptor listed in
///    `/proc/self/fd` that is not kept is closed, and the listing is checked
///    again;
/// 3. without either, nothing proves that every unknown descriptor is closed:
///    an error, never an arbitrary bound.
fn close_descriptors_except(keep: &[libc::c_int]) -> Result<(), String> {
    let mut kept: Vec<libc::c_uint> = keep
        .iter()
        .filter_map(|fd| libc::c_uint::try_from(*fd).ok())
        .collect();
    kept.sort_unstable();
    kept.dedup();
    let mut first: libc::c_uint = 0;
    let mut ranges = Vec::with_capacity(kept.len() + 1);
    for fd in &kept {
        if *fd > first {
            ranges.push((first, fd - 1));
        }
        first = fd.saturating_add(1);
    }
    ranges.push((first, libc::c_uint::MAX));
    let mut unsupported = None;
    for (low, high) in ranges {
        if let Err(err) = close_range(low, high) {
            match err.raw_os_error() {
                Some(libc::ENOSYS) | Some(libc::EINVAL) => {
                    unsupported = Some(err);
                    break;
                }
                _ => return Err(format!("close_range({low}, {high}): {err}")),
            }
        }
    }
    let Some(unsupported) = unsupported else {
        return Ok(());
    };
    let not_kept = |fd: &libc::c_int| {
        !libc::c_uint::try_from(*fd).is_ok_and(|fd| kept.binary_search(&fd).is_ok())
    };
    let open = open_descriptors().map_err(|err| {
        format!(
            "close_range is unsupported ({unsupported}) and /proc/self/fd cannot be read ({err})"
        )
    })?;
    for fd in open.iter().filter(|fd| not_kept(fd)) {
        // SAFETY: closing an inherited descriptor nothing here owns; EBADF
        // (the listing's own directory, already closed) is harmless.
        unsafe {
            libc::close(*fd);
        }
    }
    // Proven only if a fresh listing shows nothing else open.
    let remaining =
        open_descriptors().map_err(|err| format!("/proc/self/fd cannot be re-read ({err})"))?;
    match remaining
        .into_iter()
        .filter(not_kept)
        .find(|fd| descriptor_is_open(*fd))
    {
        Some(fd) => Err(format!("descriptor {fd} is still open after closing")),
        None => Ok(()),
    }
}

/// Why the keeper may not act (yet).
enum Admission {
    Admitted,
    /// The launcher could not declare the keeper; nothing was declared.
    Refused,
    /// The launcher died before admitting the keeper.
    LauncherGone,
    Stopped(crate::cleanup_custody::StopReason),
}

/// Waits for the launcher's admission, never past the owner's original
/// deadline (expiry first). Without an owner there is no deadline to bound
/// it; the launcher answers at once or dies, which closes the channel.
fn wait_for_admission(
    channel: &OwnedFd,
    deadline: &mut crate::cleanup_custody::CleanupDeadline,
) -> Admission {
    if let Some(reason) = deadline.stop_reason() {
        return Admission::Stopped(reason);
    }
    if deadline.is_owned()
        && deadline.wait_readable(channel.as_fd()) != crate::cleanup_custody::Wait::Ready
    {
        return deadline
            .stop_reason()
            .map_or(Admission::LauncherGone, Admission::Stopped);
    }
    match receive_keeper_message(channel).as_deref() {
        Some([KEEPER_ADMIT]) => {}
        Some([KEEPER_REFUSE]) => return Admission::Refused,
        _ => return Admission::LauncherGone,
    }
    // Expiry first: an admission that arrives late is not permission to act.
    match deadline.stop_reason() {
        Some(reason) => Admission::Stopped(reason),
        None => Admission::Admitted,
    }
}

fn launcher_gone(launcher_pidfd: &OwnedFd) -> bool {
    let mut poll_fd = libc::pollfd {
        fd: launcher_pidfd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: one valid pollfd, zero timeout.
    unsafe { libc::poll(&mut poll_fd, 1, 0) > 0 }
}

/// The keeper's honest `reaper_join` when it stops before the sandbox starts.
fn stop_join(
    launcher_alive: bool,
    stop: Option<crate::cleanup_custody::StopReason>,
) -> crate::synthetic_targets::ReaperJoin {
    use crate::cleanup_custody::StopReason;
    use crate::synthetic_targets::ReaperJoin;
    if !launcher_alive {
        return ReaperJoin::FinishedByReaper;
    }
    match stop {
        Some(StopReason::DeadlinePassed) => ReaperJoin::ExitedAtDeadline,
        Some(StopReason::OwnerGone) => ReaperJoin::ExitedOwnerGone,
        None => ReaperJoin::Pending,
    }
}

/// The keeper's first steps, before it can be admitted: out of the
/// launcher's (the owner's kill) group, no parent-death signal, a child
/// subreaper with a private PID namespace, and its forwarder (which only
/// records signals until bubblewrap exists) installed **before** the
/// forwarded signals blocked at the fork are unmasked.
fn keeper_prologue(
    pid_namespace: bool,
    setup_signal_mask: &ForwardedSignalMask,
) -> Result<ForwardedSignalHandlers, String> {
    if unsafe { libc::setpgid(0, 0) } < 0 {
        let err = std::io::Error::last_os_error();
        return Err(format!(
            "codex-linux-sandbox: the sandbox keeper cannot leave the launcher's process group: {err}"
        ));
    }
    // With a private PID namespace, an init orphaned by bubblewrap's death
    // is reparented here, so the keeper can join it.
    if pid_namespace && unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) } < 0 {
        let err = std::io::Error::last_os_error();
        return Err(format!(
            "codex-linux-sandbox: the sandbox keeper cannot become a child subreaper: {err}"
        ));
    }
    let forwarders = install_pending_signal_forwarders();
    setup_signal_mask.restore();
    Ok(forwarders)
}

/// How long the keeper may read bubblewrap's `--info-fd` report: at most
/// `INFO_DEADLINE`, never past the owner's original deadline, and not at all
/// once custody stopped (expiry first).
fn keeper_info_bound(deadline: &mut crate::cleanup_custody::CleanupDeadline) -> Option<Duration> {
    if deadline.stop_reason().is_some() {
        return None;
    }
    Some(
        deadline
            .remaining()
            .map_or(crate::synthetic_targets::INFO_DEADLINE, |remaining| {
                remaining.min(crate::synthetic_targets::INFO_DEADLINE)
            }),
    )
}

/// What the keeper knows about itself and its launcher.
struct KeeperSession {
    ids: crate::synthetic_targets::LaunchIdentity,
    channel: OwnedFd,
    launcher_pidfd: OwnedFd,
}

impl KeeperSession {
    fn launcher_alive(&self) -> bool {
        !launcher_gone(&self.launcher_pidfd)
    }

    /// The keeper's finish (see `finish_keeper`). A failed persist is in the
    /// owner report; the launcher, if it is still there, also prints it.
    fn finish(
        &self,
        custody: &mut crate::synthetic_targets::LaunchCustody,
        registrations: &[crate::synthetic_targets::SyntheticTargetRegistration],
        known: Option<Vec<(PathBuf, crate::synthetic_targets::TargetOutcome)>>,
        proof: crate::synthetic_targets::SandboxProof,
        reaper_join: crate::synthetic_targets::ReaperJoin,
        never_started: bool,
    ) -> crate::synthetic_targets::KeeperFinish {
        let finished = crate::synthetic_targets::finish_keeper(
            registrations,
            known,
            &self.ids,
            proof,
            custody,
            reaper_join,
            never_started,
        );
        if let Some(error) = &finished.persist_error {
            send_keeper_warning(
                &self.channel,
                &format!(
                    "codex-linux-sandbox: the sandbox keeper could not persist the launch receipt ({error}); the registry still records the launch as running, and the owner was told it is {}",
                    finished.state_name()
                ),
            );
        }
        finished
    }
}

fn keeper_main(start: KeeperStart) -> KeeperExit {
    use crate::synthetic_targets::LaunchIdentity;
    use crate::synthetic_targets::ReaperJoin;
    use crate::synthetic_targets::SandboxProof;
    let KeeperStart {
        mut args,
        mut preserved_files,
        synthetic_mount_targets,
        pid_namespace,
        launcher,
        launcher_pidfd,
        channel,
        mut custody,
        setup_signal_mask,
    } = start;

    // Both refusals below happen before any side effect: nothing is
    // installed, created or forked yet, and admission was not even read.
    let _pending_forwarders = match keeper_prologue(pid_namespace, &setup_signal_mask) {
        Ok(forwarders) => forwarders,
        Err(message) => return KeeperExit::Refused(message),
    };
    let keeper = match crate::synthetic_targets::ProcessToken::current() {
        Ok(keeper) => keeper,
        Err(err) => {
            return KeeperExit::Refused(format!(
                "codex-linux-sandbox: the sandbox keeper cannot identify itself: {err}"
            ));
        }
    };
    let session = KeeperSession {
        ids: LaunchIdentity::keeper(launcher, keeper),
        channel,
        launcher_pidfd,
    };

    // Admission: the launcher declared this keeper (receipt, then owner).
    match wait_for_admission(&session.channel, &mut custody.deadline) {
        Admission::Admitted => {}
        Admission::Refused => return KeeperExit::Done,
        Admission::LauncherGone => {
            // Nothing was created: finish the receipt the launcher may have
            // written, with no target.
            session.finish(
                &mut custody,
                &[],
                Some(Vec::new()),
                SandboxProof::NeverStarted,
                ReaperJoin::FinishedByReaper,
                /*never_started*/ false,
            );
            return KeeperExit::Done;
        }
        Admission::Stopped(reason) => {
            session.finish(
                &mut custody,
                &[],
                Some(Vec::new()),
                crate::synthetic_targets::stopped_proof(reason),
                stop_join(session.launcher_alive(), Some(reason)),
                /*never_started*/ false,
            );
            return KeeperExit::Failed(
                "refusing to launch the Linux sandbox: the owner's cleanup deadline passed, or the owner is gone, before the sandbox keeper was admitted"
                    .to_string(),
            );
        }
    }

    // Registration: the first side effect, after the declaration, bounded by
    // the owner's original deadline throughout (lock, recovery, every target
    // operation and evidence write, any retraction).
    let unavailable = |err: std::io::Error| {
        format!(
            "refusing to launch the Linux sandbox: synthetic mount targets need the trusted sandbox registry, which is unavailable: {err}"
        )
    };
    let locked = crate::synthetic_targets::open_shared_registry().and_then(|registry| {
        let lock = registry.lock_within(&mut custody.deadline)?;
        Ok(lock.map(|lock| (registry, lock)))
    });
    let (registry, lock) = match locked {
        Ok(Some(locked)) => locked,
        Ok(None) => {
            let stop = custody.deadline.stop_reason();
            session.finish(
                &mut custody,
                &[],
                Some(Vec::new()),
                stop.map_or(
                    SandboxProof::NeverStarted,
                    crate::synthetic_targets::stopped_proof,
                ),
                stop_join(session.launcher_alive(), stop),
                /*never_started*/ false,
            );
            return KeeperExit::Failed(
                "refusing to launch the Linux sandbox: the owner's cleanup deadline passed, or the owner is gone, before any target was installed"
                    .to_string(),
            );
        }
        Err(err) => {
            // Nothing was installed. The finish cannot reach the registry
            // either, so the owner is told the receipt was not persisted.
            session.finish(
                &mut custody,
                &[],
                Some(Vec::new()),
                SandboxProof::NeverStarted,
                stop_join(session.launcher_alive(), None),
                /*never_started*/ false,
            );
            return KeeperExit::Failed(unavailable(err));
        }
    };
    let crate::synthetic_targets::Registered {
        mut registrations,
        failure,
    } = crate::synthetic_targets::register_synthetic_targets(
        &registry,
        &synthetic_mount_targets,
        &session.ids,
        pid_namespace,
        &mut custody.deadline,
    );
    drop(lock);
    // The keeper keeps no registry descriptor while the sandbox runs.
    drop(registry);
    if let Some(failure) = failure {
        // Finish truthfully with the partial census; the registrations and
        // any interrupted staging descriptor stay held until it is written.
        let proof = failure.proof();
        let crate::synthetic_targets::RegistrationFailure {
            cause,
            census,
            retained,
        } = failure;
        let stop = match &cause {
            crate::synthetic_targets::RegistrationCause::Stopped(reason) => Some(*reason),
            crate::synthetic_targets::RegistrationCause::Failed(_) => None,
        };
        let launcher_alive = session.launcher_alive();
        session.finish(
            &mut custody,
            &registrations,
            Some(census),
            proof,
            stop_join(launcher_alive, stop),
            /*never_started*/ false,
        );
        drop(retained);
        drop(registrations);
        return if launcher_alive {
            KeeperExit::Failed(format!("refusing to launch the Linux sandbox: {cause}"))
        } else {
            KeeperExit::Done
        };
    }
    keeper_gap!(KeeperRegistered, unsafe { libc::getpid() });

    // Admission of the sandbox: the launcher is alive and custody has not
    // stopped. Otherwise bubblewrap is never forked.
    if !session.launcher_alive() || custody.deadline.stop_reason().is_some() {
        return keeper_never_started(&session, &registrations, &mut custody);
    }

    // Fork bubblewrap, parked on the exec-start pipe until its token is
    // recorded. The parked child exits, never executing bubblewrap, if the
    // pipe closes without the start byte.
    let mut info_read = registrations
        .iter()
        .any(crate::synthetic_targets::SyntheticTargetRegistration::is_registry_owned)
        .then(|| attach_bwrap_info_fd(&mut args, &mut preserved_files));
    let exec_start_pipe = create_exec_start_pipe(/*enabled*/ true);
    let fork_signal_mask = ForwardedSignalMask::block();
    let keeper_pid = unsafe { libc::getpid() };
    let pid = unsafe { libc::fork() };
    if pid < 0 {
        let err = std::io::Error::last_os_error();
        fork_signal_mask.restore();
        close_child_exec_start_read(exec_start_pipe[0]);
        close_child_exec_start_read(exec_start_pipe[1]);
        let _ = keeper_never_started(&session, &registrations, &mut custody);
        return KeeperExit::Failed(format!("failed to fork for bubblewrap: {err}"));
    }
    if pid == 0 {
        // bubblewrap's child: its panics are its own (printed, exit 1), never
        // reported as the keeper's.
        let _ = std::panic::take_hook();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            reset_forwarded_signal_handlers_to_default();
            fork_signal_mask.restore();
            let setpgid_res = unsafe { libc::setpgid(0, 0) };
            if setpgid_res < 0 {
                let err = std::io::Error::last_os_error();
                panic!("failed to place bubblewrap child in its own process group: {err}");
            }
            terminate_with_parent(keeper_pid);
            wait_for_parent_exec_start(exec_start_pipe[0], exec_start_pipe[1]);
            exec_bwrap(args, preserved_files);
        }));
        // SAFETY: terminate the forked child without the keeper's destructors.
        unsafe { libc::_exit(1) }
    }
    drop(preserved_files);
    close_child_exec_start_read(exec_start_pipe[0]);
    forward_pending_signals_to(pid);
    fork_signal_mask.restore();
    // bubblewrap holds the command's standard I/O now; the keeper must not
    // keep the owner's pipes open after the launcher is gone.
    redirect_standard_io_to_dev_null();
    keeper_gap!(KeeperForkedBwrap, pid);

    // Evidence 1 (its own lock): bubblewrap's token, before it may exec.
    let mut sandbox = crate::synthetic_targets::SandboxProcesses::pin_bwrap(pid, pid_namespace);
    let admitted = sandbox.bwrap_pidfd().is_some()
        && session.launcher_alive()
        && record_sandbox_evidence(&mut registrations, &session.ids, &sandbox, &mut custody);
    if !admitted {
        // Never released: the parked child exits without ever executing
        // bubblewrap, so no sandbox of this launch can exist.
        close_child_exec_start_read(exec_start_pipe[1]);
        BWRAP_CHILD_PID.store(0, Ordering::SeqCst);
        if let Some(pidfd) = sandbox.bwrap_pidfd()
            && custody.deadline.wait_readable(pidfd) == crate::cleanup_custody::Wait::Ready
        {
            reap_child(pid);
        }
        return keeper_never_started(&session, &registrations, &mut custody);
    }
    keeper_gap!(KeeperRecordedBwrap, pid);
    release_child_exec_start(exec_start_pipe[1]);
    keeper_gap!(KeeperReleasedBwrap, pid);

    // Evidence 2 (a second, separate lock): the init, observed for at most
    // INFO_DEADLINE and never past the owner's live deadline (expiry first,
    // re-evaluated before every poll, read, lookup and pin).
    if let Some(reading) = &info_read
        && let Some(bound) = keeper_info_bound(&mut custody.deadline)
    {
        let observed = sandbox.observe_init(
            pid,
            reading,
            std::time::Instant::now() + bound,
            &mut custody.deadline,
        );
        let recorded_init = observed == crate::synthetic_targets::InitObservation::Pinned
            && record_sandbox_evidence(&mut registrations, &session.ids, &sandbox, &mut custody);
        if sandbox.init_pinned() {
            // The report was read to EOF: bubblewrap closed its end, so the
            // read end can no longer cause an EPIPE and is not kept.
            info_read = None;
        }
        if recorded_init {
            keeper_gap!(KeeperRecordedInit, sandbox.tokens().init_pid().unwrap_or(0));
        }
    }

    // The command runs: wait for bubblewrap or the launcher's death, as the
    // launcher's own wait did (this is not a cleanup wait). If the owner's
    // deadline passes (or the owner is gone without one) meanwhile, the
    // outcome is recorded at once and is final, exactly as the reaper did:
    // nothing is cleaned afterwards and the receipt is never rewritten.
    let mut recorded_early = false;
    let launcher_died = loop {
        let custody_deadline = (!recorded_early).then_some(&mut custody.deadline);
        match wait_for_sandbox_event(&sandbox, &session.launcher_pidfd, custody_deadline) {
            SandboxEvent::BwrapExited => break false,
            SandboxEvent::LauncherDied => break true,
            SandboxEvent::CustodyStopped(reason) => {
                session.finish(
                    &mut custody,
                    &registrations,
                    None,
                    crate::synthetic_targets::stopped_proof(reason),
                    ReaperJoin::Pending,
                    /*never_started*/ false,
                );
                recorded_early = true;
            }
        }
    };
    let cleanup_signal_mask = ForwardedSignalMask::block();
    BWRAP_CHILD_PID.store(0, Ordering::SeqCst);
    if !launcher_died {
        let status = wait_for_bwrap_child(pid);
        if !recorded_early {
            let proof = sandbox.keeper_proof(&mut custody.deadline);
            let join = if session.launcher_alive() {
                ReaperJoin::Pending
            } else {
                ReaperJoin::FinishedByReaper
            };
            session.finish(
                &mut custody,
                &registrations,
                None,
                proof,
                join,
                /*never_started*/ false,
            );
        }
        drop(info_read);
        cleanup_signal_mask.restore();
        send_keeper_status(&session.channel, status);
        return KeeperExit::Done;
    }

    // The launcher was killed while the sandbox ran. Its death used to reach
    // bubblewrap as its parent-death SIGTERM; bubblewrap's parent is now the
    // keeper, which delivers exactly that, through the pinned pidfd only.
    sandbox.signal_bwrap(libc::SIGTERM);
    if !recorded_early {
        let proof = match custody.deadline.stop_reason() {
            Some(reason) => crate::synthetic_targets::stopped_proof(reason),
            // No owner deadline: stop all cleanup now and keep everything.
            None if !custody.deadline.is_owned() => SandboxProof::OwnerGone,
            None => sandbox.keeper_proof(&mut custody.deadline),
        };
        session.finish(
            &mut custody,
            &registrations,
            None,
            proof,
            ReaperJoin::FinishedByReaper,
            /*never_started*/ false,
        );
    }
    drop(info_read);
    cleanup_signal_mask.restore();
    KeeperExit::Done
}

/// The keeper decided never to fork (or never to release) bubblewrap: no
/// sandbox of this launch exists or can exist. Its finish records that
/// statement for a later recovery (bookkeeping: after a stop only under a
/// lock taken without waiting), then removes its targets (by their held
/// descriptors) only while an owner deadline still bounds the launch. With
/// no owner deadline, or once custody stopped, everything is kept.
fn keeper_never_started(
    session: &KeeperSession,
    registrations: &[crate::synthetic_targets::SyntheticTargetRegistration],
    custody: &mut crate::synthetic_targets::LaunchCustody,
) -> KeeperExit {
    use crate::synthetic_targets::SandboxProof;
    let launcher_alive = session.launcher_alive();
    let stop = custody.deadline.stop_reason();
    let proof = match stop {
        Some(reason) => crate::synthetic_targets::stopped_proof(reason),
        // The launcher is gone and no owner deadline bounds the launch: stop
        // all cleanup now and keep every target as incomplete.
        None if !launcher_alive && !custody.deadline.is_owned() => SandboxProof::OwnerGone,
        None => SandboxProof::NeverStarted,
    };
    let finished = session.finish(
        custody,
        registrations,
        None,
        proof,
        stop_join(launcher_alive, stop),
        /*never_started*/ true,
    );
    if !launcher_alive {
        return KeeperExit::Done;
    }
    KeeperExit::Failed(format!(
        "refusing to launch the Linux sandbox: the sandbox was not started (cleanup {})",
        finished.state_name()
    ))
}

/// Records the tokens pinned so far in every holder of this launch, under
/// its own registry lock, never awaited past the owner's deadline.
fn record_sandbox_evidence(
    registrations: &mut [crate::synthetic_targets::SyntheticTargetRegistration],
    ids: &crate::synthetic_targets::LaunchIdentity,
    sandbox: &crate::synthetic_targets::SandboxProcesses,
    custody: &mut crate::synthetic_targets::LaunchCustody,
) -> bool {
    let Ok(registry) = crate::synthetic_targets::open_shared_registry() else {
        return false;
    };
    let Ok(Some(_lock)) = registry.lock_within(&mut custody.deadline) else {
        return false;
    };
    // Every holder must be written; a stop part-way is not admission.
    matches!(
        crate::synthetic_targets::record_sandbox_tokens(
            &registry,
            registrations,
            ids,
            sandbox.tokens(),
            &mut custody.deadline,
        ),
        Ok(true)
    )
}

enum SandboxEvent {
    BwrapExited,
    LauncherDied,
    /// The owner's deadline passed, or the owner is gone without one.
    CustodyStopped(crate::cleanup_custody::StopReason),
}

/// Waits for bubblewrap to exit or the launcher to die, for as long as the
/// command runs, as the launcher's own wait did. With `deadline`, it also
/// wakes for the owner's deadline (and its tightenings) so that custody can
/// stop on time; that never ends the wait for the command itself.
fn wait_for_sandbox_event(
    sandbox: &crate::synthetic_targets::SandboxProcesses,
    launcher_pidfd: &OwnedFd,
    mut deadline: Option<&mut crate::cleanup_custody::CleanupDeadline>,
) -> SandboxEvent {
    let Some(bwrap) = sandbox.bwrap_pidfd() else {
        return if launcher_gone(launcher_pidfd) {
            SandboxEvent::LauncherDied
        } else {
            SandboxEvent::BwrapExited
        };
    };
    loop {
        let mut timeout = -1;
        let mut channel = None;
        if let Some(deadline) = deadline.as_deref_mut() {
            if let Some(reason) = deadline.stop_reason() {
                return SandboxEvent::CustodyStopped(reason);
            }
            if let Some(remaining) = deadline.remaining() {
                let millis = remaining.as_nanos().div_ceil(1_000_000);
                timeout = libc::c_int::try_from(millis).unwrap_or(libc::c_int::MAX);
            }
            channel = deadline.channel_fd();
        }
        let mut fds = vec![
            libc::pollfd {
                fd: bwrap.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: launcher_pidfd.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        if let Some(channel) = channel {
            fds.push(libc::pollfd {
                fd: channel,
                events: libc::POLLIN,
                revents: 0,
            });
        }
        // SAFETY: valid pollfd array.
        let rc = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, timeout) };
        if rc < 0 {
            if std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            return if launcher_gone(launcher_pidfd) {
                SandboxEvent::LauncherDied
            } else {
                SandboxEvent::BwrapExited
            };
        }
        if fds[0].revents != 0 {
            return SandboxEvent::BwrapExited;
        }
        if fds[1].revents != 0 {
            return SandboxEvent::LauncherDied;
        }
        // The deadline, or channel activity: re-evaluated at the loop head.
    }
}

fn reap_child(pid: libc::pid_t) {
    loop {
        let mut status = 0;
        // SAFETY: non-blocking reap of one child.
        let rc = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
        if rc < 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        return;
    }
}

fn redirect_standard_io_to_dev_null() {
    // SAFETY: open/dup2/close on descriptors owned by this process.
    unsafe {
        let null = libc::open(c"/dev/null".as_ptr(), libc::O_RDWR | libc::O_CLOEXEC);
        if null < 0 {
            return;
        }
        for fd in 0..=2 {
            libc::dup2(null, fd);
        }
        if null > 2 {
            libc::close(null);
        }
    }
}

fn create_exec_start_pipe(enabled: bool) -> [libc::c_int; 2] {
    if !enabled {
        return [-1, -1];
    }
    let mut pipe = [-1, -1];
    if unsafe { libc::pipe2(pipe.as_mut_ptr(), libc::O_CLOEXEC) } < 0 {
        let err = std::io::Error::last_os_error();
        panic!("failed to create bubblewrap exec start pipe: {err}");
    }
    pipe
}

fn wait_for_parent_exec_start(read_fd: libc::c_int, write_fd: libc::c_int) {
    if write_fd >= 0 {
        unsafe {
            libc::close(write_fd);
        }
    }
    if read_fd < 0 {
        return;
    }

    let mut byte = [0_u8; 1];
    let released = loop {
        let read = unsafe { libc::read(read_fd, byte.as_mut_ptr().cast(), byte.len()) };
        if read >= 0 {
            break read == 1;
        }
        let err = std::io::Error::last_os_error();
        if err.kind() != std::io::ErrorKind::Interrupted {
            break false;
        }
    };
    unsafe {
        libc::close(read_fd);
    }
    // Fail closed: only the start byte releases the exec. EOF means the
    // parent closed the pipe without releasing it (it decided never to start
    // bubblewrap, or it died), so this child exits without executing it.
    if !released {
        unsafe { libc::_exit(1) };
    }
}

fn close_child_exec_start_read(read_fd: libc::c_int) {
    if read_fd >= 0 {
        unsafe {
            libc::close(read_fd);
        }
    }
}

fn release_child_exec_start(write_fd: libc::c_int) {
    if write_fd < 0 {
        return;
    }
    let byte = [0_u8; 1];
    unsafe {
        libc::write(write_fd, byte.as_ptr().cast(), byte.len());
        libc::close(write_fd);
    }
}

struct ForwardedSignalMask {
    previous: libc::sigset_t,
}

struct ForwardedSignalHandlers {
    previous: Vec<(libc::c_int, libc::sigaction)>,
}

impl ForwardedSignalMask {
    fn block() -> Self {
        let mut blocked: libc::sigset_t = unsafe { std::mem::zeroed() };
        let mut previous: libc::sigset_t = unsafe { std::mem::zeroed() };
        unsafe {
            libc::sigemptyset(&mut blocked);
            for signal in FORWARDED_SIGNALS {
                libc::sigaddset(&mut blocked, *signal);
            }
            if libc::sigprocmask(libc::SIG_BLOCK, &blocked, &mut previous) < 0 {
                let err = std::io::Error::last_os_error();
                panic!("failed to block bubblewrap forwarded signals: {err}");
            }
        }
        Self { previous }
    }

    fn restore(&self) {
        let mut restored = self.previous;
        unsafe {
            for signal in FORWARDED_SIGNALS {
                libc::sigdelset(&mut restored, *signal);
            }
            if libc::sigprocmask(libc::SIG_SETMASK, &restored, std::ptr::null_mut()) < 0 {
                let err = std::io::Error::last_os_error();
                panic!("failed to restore bubblewrap forwarded signals: {err}");
            }
        }
    }
}

/// Re-establish bubblewrap's `--die-with-parent` contract for the inner stage.
///
/// bubblewrap arms `PR_SET_PDEATHSIG` before it execs this stage, but the
/// kernel discards it when that exec changes the LSM confinement label (for
/// example AppArmor's `bwrap-userns-restrict` profile). This stage runs in its
/// own session and usually as PID 1 of the sandbox PID namespace, so it must
/// not outlive bubblewrap: when it dies, the kernel tears down every process
/// in the namespace.
///
/// The parent-death signal fires when the parent *thread* exits. Our parent is
/// bubblewrap's monitor: bubblewrap is single-threaded and `raw_clone`s this
/// stage from its only thread, and the pidfd was opened by the launcher for
/// the process that exec'd into that same bubblewrap. So the pidfd and the
/// parent-death signal watch the same thread.
///
/// Arm the signal first, then poll the pidfd. If bubblewrap was already gone
/// the signal can never arrive (and `getppid()` is 0 across the namespace
/// boundary, so it proves nothing), so exit. The pidfd is closed on every path
/// before this function returns or exits.
fn die_with_bwrap_parent(parent_pidfd: OwnedFd) {
    let res = unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) };
    if res < 0 {
        let err = std::io::Error::last_os_error();
        drop(parent_pidfd);
        panic!("failed to set inner sandbox stage parent-death signal: {err}");
    }
    let mut poll_fd = libc::pollfd {
        fd: parent_pidfd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    let poll_result = loop {
        // SAFETY: `poll_fd` is a valid pollfd for the duration of the call.
        let res = unsafe { libc::poll(&mut poll_fd, 1, 0) };
        if res >= 0 {
            break Ok(res > 0);
        }
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() != Some(libc::EINTR) {
            break Err(err);
        }
    };
    drop(parent_pidfd);
    match poll_result {
        Ok(false) => {}
        Ok(true) => {
            // A PID namespace init cannot deliver SIGKILL to itself, so exit
            // with the status a parent-death SIGKILL would have produced.
            unsafe { libc::_exit(128 + libc::SIGKILL) };
        }
        Err(err) => panic!("failed to poll bubblewrap parent pidfd: {err}"),
    }
}

/// Bind the forked user command to the inner stage. Used for
/// `--inherit-pid-namespace`, where the inner stage is not a namespace init.
fn kill_with_parent(parent_pid: libc::pid_t) {
    kill_with_parent_after(parent_pid, || {});
}

fn kill_with_parent_after(parent_pid: libc::pid_t, before_arming: impl FnOnce()) {
    before_arming();
    let res = unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) };
    if res < 0 {
        let err = std::io::Error::last_os_error();
        panic!("failed to set sandboxed command parent-death signal: {err}");
    }
    // Checked only after arming: a parent that died before prctl has already
    // reparented this process, and its death signal will never be sent.
    if unsafe { libc::getppid() } != parent_pid {
        unsafe { libc::_exit(128 + libc::SIGKILL) };
    }
}

fn terminate_with_parent(parent_pid: libc::pid_t) {
    let res = unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) };
    if res < 0 {
        let err = std::io::Error::last_os_error();
        panic!("failed to set bubblewrap child parent-death signal: {err}");
    }
    if unsafe { libc::getppid() } != parent_pid {
        unsafe {
            libc::raise(libc::SIGTERM);
        }
    }
}

impl ForwardedSignalHandlers {
    fn restore(self) {
        BWRAP_CHILD_PID.store(0, Ordering::SeqCst);
        PENDING_FORWARDED_SIGNAL.store(0, Ordering::SeqCst);
        for (signal, previous_action) in self.previous {
            unsafe {
                if libc::sigaction(signal, &previous_action, std::ptr::null_mut()) < 0 {
                    let err = std::io::Error::last_os_error();
                    panic!("failed to restore bubblewrap signal handler for {signal}: {err}");
                }
            }
        }
    }
}

fn install_bwrap_signal_forwarders(pid: libc::pid_t) -> ForwardedSignalHandlers {
    BWRAP_CHILD_PID.store(pid, Ordering::SeqCst);
    let handlers = install_forwarding_handlers();
    replay_pending_forwarded_signal(pid);
    handlers
}

/// The keeper's forwarder before bubblewrap exists: it only records a
/// forwarded signal; [`forward_pending_signals_to`] replays it to bubblewrap.
fn install_pending_signal_forwarders() -> ForwardedSignalHandlers {
    BWRAP_CHILD_PID.store(0, Ordering::SeqCst);
    install_forwarding_handlers()
}

/// Arms an installed forwarder for `pid` and replays a recorded signal.
fn forward_pending_signals_to(pid: libc::pid_t) {
    BWRAP_CHILD_PID.store(pid, Ordering::SeqCst);
    replay_pending_forwarded_signal(pid);
}

fn install_forwarding_handlers() -> ForwardedSignalHandlers {
    let mut previous = Vec::with_capacity(FORWARDED_SIGNALS.len());
    for signal in FORWARDED_SIGNALS {
        let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
        let mut previous_action: libc::sigaction = unsafe { std::mem::zeroed() };
        action.sa_sigaction = forward_signal_to_bwrap_child as *const () as libc::sighandler_t;
        unsafe {
            libc::sigemptyset(&mut action.sa_mask);
            if libc::sigaction(*signal, &action, &mut previous_action) < 0 {
                let err = std::io::Error::last_os_error();
                panic!("failed to install bubblewrap signal forwarder for {signal}: {err}");
            }
        }
        previous.push((*signal, previous_action));
    }
    ForwardedSignalHandlers { previous }
}

extern "C" fn forward_signal_to_bwrap_child(signal: libc::c_int) {
    PENDING_FORWARDED_SIGNAL.store(signal, Ordering::SeqCst);
    let pid = BWRAP_CHILD_PID.load(Ordering::SeqCst);
    if pid > 0 {
        send_signal_to_bwrap_child(pid, signal);
    }
}

fn replay_pending_forwarded_signal(pid: libc::pid_t) {
    // Never `kill(0, ..)`/`kill(-0, ..)`: without a target the signal stays
    // recorded.
    if pid <= 0 {
        return;
    }
    let signal = PENDING_FORWARDED_SIGNAL.swap(0, Ordering::SeqCst);
    if signal > 0 {
        send_signal_to_bwrap_child(pid, signal);
    }
}

fn send_signal_to_bwrap_child(pid: libc::pid_t, signal: libc::c_int) {
    unsafe {
        libc::kill(-pid, signal);
        libc::kill(pid, signal);
    }
}

fn reset_forwarded_signal_handlers_to_default() {
    for signal in FORWARDED_SIGNALS {
        unsafe {
            if libc::signal(*signal, libc::SIG_DFL) == libc::SIG_ERR {
                let err = std::io::Error::last_os_error();
                panic!("failed to reset bubblewrap signal handler for {signal}: {err}");
            }
        }
    }
}

fn wait_for_bwrap_child(pid: libc::pid_t) -> libc::c_int {
    loop {
        let mut status: libc::c_int = 0;
        let wait_res = unsafe { libc::waitpid(pid, &mut status as *mut libc::c_int, 0) };
        if wait_res >= 0 {
            return status;
        }
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        panic!("waitpid failed for bubblewrap child: {err}");
    }
}

/// Installs (or joins) every synthetic mount target and publishes creation
/// evidence and the `running` launch receipt before bubblewrap is forked.
/// Any failure refuses the launch.
fn install_synthetic_mount_targets(
    targets: &[crate::bwrap::SyntheticMountTarget],
    ids: &crate::synthetic_targets::LaunchIdentity,
    pid_namespace: bool,
) -> Vec<crate::synthetic_targets::SyntheticTargetRegistration> {
    // Synthetic targets are only ever installed with custody in the trusted
    // registry. Without one (no `/run/user/<uid>` and no usable home) the
    // launch is refused before anything is created and before the command
    // can run; there is no fallback location.
    let registered = crate::synthetic_targets::try_with_shared_registry_lock(|registry| {
        crate::synthetic_targets::register_synthetic_targets(
            registry,
            targets,
            ids,
            pid_namespace,
            &mut crate::cleanup_custody::CleanupDeadline::none(),
        )
        .into_result()
    })
    .unwrap_or_else(|err| {
        panic!(
            "refusing to launch the Linux sandbox: synthetic mount targets need the trusted sandbox registry, which is unavailable: {err}"
        )
    });
    registered.unwrap_or_else(|err| panic!("refusing to launch the Linux sandbox: {err}"))
}

/// Adds `--info-fd` before the command separator and keeps the write end in
/// the descriptors bubblewrap inherits.
fn attach_bwrap_info_fd(args: &mut Vec<String>, preserved_files: &mut Vec<File>) -> OwnedFd {
    let (read, write) = crate::synthetic_targets::bwrap_info_pipe()
        .unwrap_or_else(|err| panic!("failed to create bubblewrap info pipe: {err}"));
    let separator = args
        .iter()
        .position(|arg| arg == "--")
        .unwrap_or_else(|| panic!("bubblewrap argv is missing command separator '--'"));
    args.splice(
        separator..separator,
        ["--info-fd".to_string(), write.as_raw_fd().to_string()],
    );
    preserved_files.push(write);
    read
}

/// Trusted per-uid registry, shared independently of TMPDIR.
pub(crate) fn synthetic_mount_registry_root() -> PathBuf {
    crate::synthetic_targets::shared_registry_root()
}

fn exit_with_wait_status(status: libc::c_int) -> ! {
    if libc::WIFEXITED(status) {
        std::process::exit(libc::WEXITSTATUS(status));
    }

    if libc::WIFSIGNALED(status) {
        let signal = libc::WTERMSIG(status);
        unsafe {
            libc::signal(signal, libc::SIG_DFL);
            libc::kill(libc::getpid(), signal);
        }
        std::process::exit(128 + signal);
    }

    std::process::exit(1);
}

/// Run a short-lived bubblewrap preflight in a child process and capture stderr.
///
/// Strategy:
/// - This is used only by `preflight_proc_mount_support`, which runs `/bin/true`
///   under bubblewrap with `--proc /proc`.
/// - The goal is to detect environments where mounting `/proc` fails (for
///   example, restricted containers), so we can retry the real run with
///   `--no-proc`.
/// - We capture stderr from that preflight to match known mount-failure text.
///   We do not stream it because this is a one-shot probe with a trivial
///   command, and reads are bounded to a fixed max size.
fn run_bwrap_in_child_capture_stderr(bwrap_args: crate::bwrap::BwrapArgs) -> String {
    const MAX_PREFLIGHT_STDERR_BYTES: u64 = 64 * 1024;
    let crate::bwrap::BwrapArgs {
        args,
        preserved_files,
        synthetic_mount_targets,
    } = bwrap_args;
    let setup_signal_mask = ForwardedSignalMask::block();
    let launch = (!synthetic_mount_targets.is_empty()).then(|| {
        crate::synthetic_targets::LaunchIdentity::single(
            crate::synthetic_targets::ProcessToken::current()
                .unwrap_or_else(|err| panic!("failed to identify the sandbox launcher: {err}")),
        )
    });
    let synthetic_target_registrations = launch.as_ref().map_or_else(Vec::new, |launch| {
        install_synthetic_mount_targets(
            &synthetic_mount_targets,
            launch,
            /*pid_namespace*/ true,
        )
    });

    let mut pipe_fds = [0; 2];
    let pipe_res = unsafe { libc::pipe2(pipe_fds.as_mut_ptr(), libc::O_CLOEXEC) };
    if pipe_res < 0 {
        let err = std::io::Error::last_os_error();
        panic!("failed to create stderr pipe for bubblewrap: {err}");
    }
    let read_fd = pipe_fds[0];
    let write_fd = pipe_fds[1];

    let pid = unsafe { libc::fork() };
    if pid < 0 {
        let err = std::io::Error::last_os_error();
        panic!("failed to fork for bubblewrap: {err}");
    }

    if pid == 0 {
        reset_forwarded_signal_handlers_to_default();
        setup_signal_mask.restore();
        // Child: redirect stderr to the pipe, then run bubblewrap.
        unsafe {
            close_fd_or_panic(read_fd, "close read end in bubblewrap child");
            if libc::dup2(write_fd, libc::STDERR_FILENO) < 0 {
                let err = std::io::Error::last_os_error();
                panic!("failed to redirect stderr for bubblewrap: {err}");
            }
            close_fd_or_panic(write_fd, "close write end in bubblewrap child");
        }

        exec_bwrap(args, preserved_files);
    }

    let signal_forwarders = install_bwrap_signal_forwarders(pid);
    setup_signal_mask.restore();
    // Parent: close the write end and read stderr while the child runs.
    close_fd_or_panic(write_fd, "close write end in bubblewrap parent");

    // SAFETY: `read_fd` is a valid owned fd in the parent.
    let mut read_file = unsafe { File::from_raw_fd(read_fd) };
    let mut stderr_bytes = Vec::new();
    let mut limited_reader = (&mut read_file).take(MAX_PREFLIGHT_STDERR_BYTES);
    if let Err(err) = limited_reader.read_to_end(&mut stderr_bytes) {
        panic!("failed to read bubblewrap stderr: {err}");
    }

    let status = wait_for_bwrap_child(pid);
    let cleanup_signal_mask = ForwardedSignalMask::block();
    BWRAP_CHILD_PID.store(0, Ordering::SeqCst);
    if let Some(launch) = &launch {
        // No bubblewrap observation here: the proof falls back to an
        // exhaustive namespace inspection, and anything unproven stays
        // recorded as incomplete.
        crate::synthetic_targets::finish_launch(
            &synthetic_target_registrations,
            launch,
            /*sandbox*/ None,
            &mut crate::synthetic_targets::LaunchCustody {
                deadline: crate::cleanup_custody::CleanupDeadline::none(),
                report: None,
            },
        )
        .unwrap_or_else(|err| {
            panic!("failed to release synthetic bubblewrap mount targets: {err}")
        });
    }
    signal_forwarders.restore();
    cleanup_signal_mask.restore();
    if libc::WIFSIGNALED(status) {
        exit_with_wait_status(status);
    }

    String::from_utf8_lossy(&stderr_bytes).into_owned()
}

/// Close an owned file descriptor and panic with context on failure.
///
/// We use explicit close() checks here (instead of ignoring return codes)
/// because this code runs in low-level sandbox setup paths where fd leaks or
/// close errors can mask the root cause of later failures.
fn close_fd_or_panic(fd: libc::c_int, context: &str) {
    let close_res = unsafe { libc::close(fd) };
    if close_res < 0 {
        let err = std::io::Error::last_os_error();
        panic!("{context}: {err}");
    }
}

fn is_proc_mount_failure(stderr: &str) -> bool {
    stderr.contains("Can't mount proc")
        && stderr.contains("/newroot/proc")
        && (stderr.contains("Invalid argument")
            || stderr.contains("Operation not permitted")
            || stderr.contains("Permission denied"))
}

struct InnerSeccompCommandArgs<'a> {
    sandbox_policy_cwd: &'a Path,
    command_cwd: Option<&'a Path>,
    permission_profile: &'a PermissionProfile,
    managed_network: Option<ManagedNetworkSandboxContext>,
    proxy_route_spec: Option<String>,
    command: Vec<String>,
}

/// Build the inner command that applies seccomp after bubblewrap.
fn build_inner_seccomp_command(args: InnerSeccompCommandArgs<'_>) -> Vec<String> {
    let InnerSeccompCommandArgs {
        sandbox_policy_cwd,
        command_cwd,
        permission_profile,
        managed_network,
        proxy_route_spec,
        command,
    } = args;
    let current_exe = match std::env::current_exe() {
        Ok(path) => path,
        Err(err) => panic!("failed to resolve current executable path: {err}"),
    };
    let permission_profile_json = match serde_json::to_string(permission_profile) {
        Ok(json) => json,
        Err(err) => panic!("failed to serialize permission profile: {err}"),
    };

    let mut inner = vec![
        current_exe.to_string_lossy().to_string(),
        "--sandbox-policy-cwd".to_string(),
        sandbox_policy_cwd.to_string_lossy().to_string(),
    ];
    if let Some(command_cwd) = command_cwd {
        inner.push("--command-cwd".to_string());
        inner.push(command_cwd.to_string_lossy().to_string());
    }
    inner.extend([
        "--permission-profile".to_string(),
        permission_profile_json,
        "--apply-seccomp-then-exec".to_string(),
    ]);
    if let Some(managed_network) = managed_network {
        inner.push("--managed-network".to_string());
        inner.push(
            serde_json::to_string(&managed_network)
                .unwrap_or_else(|err| panic!("failed to serialize managed network context: {err}")),
        );
        let proxy_route_spec = proxy_route_spec
            .unwrap_or_else(|| panic!("managed proxy mode requires a proxy route spec"));
        inner.push("--proxy-route-spec".to_string());
        inner.push(proxy_route_spec);
    }
    inner.push("--".to_string());
    inner.extend(command);
    inner
}

/// Exec the provided argv, panicking with context if it fails.
fn exec_or_panic(command: Vec<String>) -> ! {
    #[expect(clippy::expect_used)]
    let c_command =
        CString::new(command[0].as_str()).expect("Failed to convert command to CString");
    #[expect(clippy::expect_used)]
    let c_args: Vec<CString> = command
        .iter()
        .map(|arg| CString::new(arg.as_str()).expect("Failed to convert arg to CString"))
        .collect();

    let mut c_args_ptrs: Vec<*const libc::c_char> = c_args.iter().map(|arg| arg.as_ptr()).collect();
    c_args_ptrs.push(std::ptr::null());

    unsafe {
        libc::execvp(c_command.as_ptr(), c_args_ptrs.as_ptr());
    }

    // If execvp returns, there was an error.
    let err = std::io::Error::last_os_error();
    panic!("Failed to execvp {}: {err}", command[0].as_str());
}

#[cfg(test)]
#[path = "linux_run_main_tests.rs"]
mod tests;
