//! Deny masks under `/proc` must hold over the fresh procfs from `--proc /proc`.

use super::*;
use codex_protocol::protocol::FileSystemSandboxEntry;
use pretty_assertions::assert_eq;

fn root_read_policy_denying(denied: &[&str]) -> FileSystemSandboxPolicy {
    let mut entries = vec![FileSystemSandboxEntry {
        path: FileSystemPath::Special {
            value: FileSystemSpecialPath::Root,
        },
        access: FileSystemAccessMode::Read,
        missing_path_behavior: None,
    }];
    entries.extend(denied.iter().map(|path| {
        FileSystemSandboxEntry::new(
            AbsolutePathBuf::try_from(*path)
                .expect("absolute deny path")
                .into(),
            FileSystemAccessMode::Deny,
        )
    }));
    FileSystemSandboxPolicy::restricted(entries)
}

fn bwrap_args(denied: &[&str], mount_proc: bool) -> Result<BwrapArgs> {
    create_bwrap_command_args(
        vec!["/bin/true".to_string()],
        &root_read_policy_denying(denied),
        Path::new("/"),
        Path::new("/"),
        BwrapOptions {
            mount_proc,
            ..Default::default()
        },
    )
}

fn position(args: &[String], window: &[&str]) -> Option<usize> {
    args.windows(window.len())
        .position(|candidate| candidate == window)
}

fn directory_mask(path: &str) -> [&str; 6] {
    ["--perms", "000", "--tmpfs", path, "--remount-ro", path]
}

#[test]
fn proc_denies_are_masked_on_top_of_the_fresh_procfs() {
    let bwrap_args =
        bwrap_args(&["/proc/sys", "/proc/version"], /*mount_proc*/ true).expect("bwrap args");
    let args = &bwrap_args.args;
    let proc_mount = position(args, &["--proc", "/proc"]).expect("fresh procfs");
    let directory = position(args, &directory_mask("/proc/sys")).expect("/proc/sys mask");
    let file = args
        .windows(5)
        .position(|window| {
            window[..3] == ["--perms", "000", "--ro-bind-data"] && window[4] == "/proc/version"
        })
        .expect("/proc/version mask");

    assert!(proc_mount < directory && proc_mount < file, "{args:#?}");
    assert!(
        !args[..proc_mount]
            .iter()
            .any(|arg| arg.starts_with(PROC_ROOT)),
        "a mask applied before --proc is covered by the fresh procfs: {args:#?}"
    );
    assert_eq!(bwrap_args.preserved_files.len(), 1);
}

#[test]
fn proc_root_deny_hides_the_procfs_with_and_without_a_fresh_mount() {
    for mount_proc in [true, false] {
        let args = bwrap_args(&["/proc", "/proc/self/fd", "/proc/sys"], mount_proc)
            .expect("bwrap args")
            .args;
        let mask = position(&args, &directory_mask(PROC_ROOT)).expect("whole /proc mask");
        if mount_proc {
            assert_eq!(position(&args, &["--proc", "/proc"]), Some(mask - 2));
        } else {
            assert!(!args.iter().any(|arg| arg == "--proc"));
            let unshare_user = position(&args, &["--unshare-user"]).expect("user namespace");
            assert!(mask < unshare_user, "{args:#?}");
        }
        // Child denies are already hidden and cannot hold a mount point.
        assert!(
            !args.iter().any(|arg| arg.starts_with("/proc/")),
            "{args:#?}"
        );
    }
}

#[test]
fn proc_denies_stay_inline_over_the_host_procfs() {
    let args = bwrap_args(&["/proc/sys"], /*mount_proc*/ false)
        .expect("bwrap args")
        .args;
    let mask = position(&args, &directory_mask("/proc/sys")).expect("/proc/sys mask");
    let unshare_user = position(&args, &["--unshare-user"]).expect("user namespace");

    assert!(!args.iter().any(|arg| arg == "--proc"));
    assert!(mask < unshare_user, "{args:#?}");
}

fn assert_refused(denied: &str, mount_proc: bool, reason: &str) {
    let message = bwrap_args(&[denied], mount_proc)
        .expect_err("an unenforceable deny must be refused")
        .to_string();
    assert!(
        message.contains(&format!(
            "cannot enforce sandbox deny-read path {denied} because"
        )) && message.contains(reason),
        "{denied} (mount_proc={mount_proc}): {message}"
    );
}

#[test]
fn pid_one_denies_are_masked_at_their_literal_path() {
    for mount_proc in [true, false] {
        let masks = bwrap_args(
            &["/proc/1/fd", "/proc/1/status", "/proc/1/cmdline"],
            mount_proc,
        )
        .expect("bwrap args");
        let args = &masks.args;
        // With a fresh procfs the masks follow it; otherwise they precede the
        // namespace flags with the other filesystem mounts.
        let anchor = if mount_proc {
            position(args, &["--proc", "/proc"]).expect("fresh procfs")
        } else {
            assert!(!args.iter().any(|arg| arg == "--proc"));
            0
        };
        let unshare_user = position(args, &["--unshare-user"]).expect("user namespace");
        let fd = position(args, &directory_mask("/proc/1/fd")).expect("/proc/1/fd mask");
        assert!(
            anchor < fd && (mount_proc || fd < unshare_user),
            "{args:#?}"
        );
        for file in ["/proc/1/status", "/proc/1/cmdline"] {
            let mask = args
                .windows(5)
                .position(|window| {
                    window[..3] == ["--perms", "000", "--ro-bind-data"] && window[4] == file
                })
                .unwrap_or_else(|| panic!("{file} mask: {args:#?}"));
            assert!(
                anchor < mask && (mount_proc || mask < unshare_user),
                "{args:#?}"
            );
        }
        assert_eq!(masks.preserved_files.len(), 2);
    }

    let args = bwrap_args(&["/proc/1", "/proc/1/fd"], /*mount_proc*/ true)
        .expect("bwrap args")
        .args;
    let proc_mount = position(&args, &["--proc", "/proc"]).expect("fresh procfs");
    assert_eq!(
        args[proc_mount + 2..proc_mount + 8],
        directory_mask("/proc/1")
    );
    assert!(!args.iter().any(|arg| arg == "/proc/1/fd"), "{args:#?}");
}

#[test]
fn pid_denies_other_than_pid_one_fail_closed() {
    let own = format!("/proc/{}", std::process::id());
    for denied in [
        "/proc/2",
        "/proc/10",
        "/proc/57/status",
        "/proc/4194303",
        own.as_str(),
    ] {
        for mount_proc in [true, false] {
            assert_refused(denied, mount_proc, "another process can receive that PID");
        }
    }
}

#[test]
fn procfs_links_under_pid_one_fail_closed() {
    for denied in [
        "/proc/1/root",
        "/proc/1/cwd/x",
        "/proc/1/exe",
        "/proc/1/ns/net",
    ] {
        for mount_proc in [true, false] {
            assert_refused(denied, mount_proc, "is a procfs link");
        }
    }
}

#[test]
fn dynamic_pid_one_entries_fail_closed() {
    for denied in [
        "/proc/1/fd/0",
        "/proc/1/fd/87",
        "/proc/1/fdinfo/0",
        "/proc/1/map_files/x",
        "/proc/1/task/1",
        "/proc/1/task/1/status",
        "/proc/1/task/7/fd/3",
    ] {
        for mount_proc in [true, false] {
            assert_refused(denied, mount_proc, "can appear after the sandbox starts");
        }
    }
}

#[test]
fn absent_pid_one_entries_fail_closed() {
    for denied in ["/proc/1/no-such-entry", "/proc/1/status/x"] {
        for mount_proc in [true, false] {
            assert_refused(denied, mount_proc, "does not exist");
        }
    }
}

#[test]
fn absent_shared_proc_entries_fail_closed() {
    for denied in [
        "/proc/kcf-no-such-entry",
        "/proc/sys/kernel/kcf-no-such-entry",
        "/proc/version/x",
    ] {
        for mount_proc in [true, false] {
            assert_refused(denied, mount_proc, "does not exist");
        }
    }
    // The refusal names the first missing component, as for `/proc/1`.
    let message = bwrap_args(&["/proc/kcf-no-such-entry/child"], /*mount_proc*/ true)
        .expect_err("an absent shared /proc entry must be refused")
        .to_string();
    assert!(
        message.contains("because /proc/kcf-no-such-entry does not exist"),
        "{message}"
    );
}

#[test]
fn present_shared_proc_entries_are_still_masked() {
    for mount_proc in [true, false] {
        let masks = bwrap_args(&["/proc/sys/kernel", "/proc/cpuinfo"], mount_proc)
            .expect("present shared entries are enforceable");
        assert!(
            position(&masks.args, &directory_mask("/proc/sys/kernel")).is_some(),
            "{:#?}",
            masks.args
        );
        assert!(
            masks.args.windows(5).any(|window| {
                window[..3] == ["--perms", "000", "--ro-bind-data"] && window[4] == "/proc/cpuinfo"
            }),
            "{:#?}",
            masks.args
        );
        assert_eq!(masks.preserved_files.len(), 1);
    }
}

#[test]
fn denied_ancestor_covers_an_absent_shared_entry() {
    let denied = ["/proc/sys/kernel", "/proc/sys/kernel/kcf-no-such-entry"];
    validate_proc_deny_rules(&root_read_policy_denying(&denied), Path::new("/"))
        .expect("the denied ancestor's mask already hides the absent entry");
    for mount_proc in [true, false] {
        let args = bwrap_args(&denied, mount_proc)
            .expect("the denied ancestor's mask already hides the absent entry")
            .args;
        assert!(
            position(&args, &directory_mask("/proc/sys/kernel")).is_some(),
            "{args:#?}"
        );
        assert!(!args.iter().any(|arg| arg == denied[1]), "{args:#?}");
    }
}

#[test]
fn static_pid_one_directories_cover_their_dynamic_entries() {
    for mount_proc in [true, false] {
        let args = bwrap_args(
            &[
                "/proc/1/fd",
                "/proc/1/fd/3",
                "/proc/1/task",
                "/proc/1/task/9/status",
            ],
            mount_proc,
        )
        .expect("a denied static directory covers its dynamic entries")
        .args;
        for directory in ["/proc/1/fd", "/proc/1/task"] {
            assert!(
                position(&args, &directory_mask(directory)).is_some(),
                "{directory}: {args:#?}"
            );
        }
        assert!(
            !args
                .iter()
                .any(|arg| arg == "/proc/1/fd/3" || arg == "/proc/1/task/9/status"),
            "{args:#?}"
        );
    }
}

#[test]
fn refusal_precedes_every_side_effect() {
    for denied in ["/proc/2", "/proc/kcf-no-such-entry"] {
        assert_refusal_precedes_every_side_effect(denied);
    }
}

fn assert_refusal_precedes_every_side_effect(denied: &str) {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let workspace = AbsolutePathBuf::from_absolute_path(temp_dir.path()).expect("workspace");
    // A writable root without `.git`, `.agents` or `.codex` would get
    // synthetic mount targets if the argv were built.
    let policy = FileSystemSandboxPolicy::restricted(vec![
        FileSystemSandboxEntry {
            path: FileSystemPath::Special {
                value: FileSystemSpecialPath::Root,
            },
            access: FileSystemAccessMode::Read,
            missing_path_behavior: None,
        },
        FileSystemSandboxEntry::new(workspace.into(), FileSystemAccessMode::Write),
        FileSystemSandboxEntry::new(
            AbsolutePathBuf::try_from(denied)
                .expect("absolute deny path")
                .into(),
            FileSystemAccessMode::Deny,
        ),
    ]);
    let validation = validate_proc_deny_rules(&policy, temp_dir.path())
        .expect_err("the pure pass refuses")
        .to_string();
    assert!(
        validation.contains(&format!(
            "cannot enforce sandbox deny-read path {denied} because"
        )),
        "{validation}"
    );
    let build = create_bwrap_command_args(
        vec!["/bin/true".to_string()],
        &policy,
        temp_dir.path(),
        temp_dir.path(),
        BwrapOptions::default(),
    )
    .expect_err("the argv build refuses as well")
    .to_string();

    assert_eq!(validation, build);
    let entries = fs::read_dir(temp_dir.path())
        .expect("read workspace")
        .count();
    assert_eq!(
        entries, 0,
        "a refused policy ({denied}) must not touch the workspace"
    );
}

#[test]
fn proc_deny_covers_unenforceable_children() {
    for mount_proc in [true, false] {
        let args = bwrap_args(&["/proc", "/proc/57", "/proc/1/root"], mount_proc)
            .expect("the /proc mask already hides the children")
            .args;
        assert!(
            position(&args, &directory_mask(PROC_ROOT)).is_some(),
            "{args:#?}"
        );
    }
}

#[test]
fn procfs_link_targets_of_deny_matches_are_not_masked() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let file = temp_dir.path().join("file");
    fs::write(&file, "fixture").expect("write fixture");
    let file_link = temp_dir.path().join("file-link");
    std::os::unix::fs::symlink(&file, &file_link).expect("file link");
    let fd_link = temp_dir.path().join("fd-link");
    std::os::unix::fs::symlink("/proc/self/fd", &fd_link).expect("procfs link");

    assert_eq!(
        canonical_deny_target(&file_link),
        Some(file.canonicalize().expect("canonical fixture"))
    );
    assert_eq!(canonical_deny_target(&fd_link), None);
}

#[test]
fn per_reader_proc_denies_fail_closed() {
    for denied in [
        "/proc/self",
        "/proc/self/fd",
        "/proc/thread-self/fd",
        "/proc/mounts",
    ] {
        for mount_proc in [true, false] {
            let message = bwrap_args(&[denied], mount_proc)
                .expect_err("a per-reader /proc deny cannot be enforced")
                .to_string();
            assert!(
                message.contains(&format!("cannot enforce sandbox deny-read path {denied}")),
                "{denied} (mount_proc={mount_proc}): {message}"
            );
        }
    }
}

#[test]
fn argv_without_proc_denies_is_unchanged() {
    let args = bwrap_args(&[], /*mount_proc*/ true)
        .expect("bwrap args")
        .args;
    let proc_mount = position(&args, &["--proc", "/proc"]).expect("fresh procfs");

    assert_eq!(
        args[proc_mount + 2..],
        ["--cap-drop", "ALL", "--", "/bin/true"]
    );
}

#[test]
fn proc_entry_kinds() {
    for (path, kind) in [
        ("/proc", ProcEntryKind::Shared),
        ("/proc/sys/kernel", ProcEntryKind::Shared),
        ("/procfs/self", ProcEntryKind::Shared),
        ("/proc/1/fd", ProcEntryKind::Pid),
        ("/proc/self/fd", ProcEntryKind::PerReader("self")),
        ("/proc/thread-self", ProcEntryKind::PerReader("thread-self")),
        ("/proc/net/tcp", ProcEntryKind::PerReader("net")),
    ] {
        assert_eq!(proc_entry_kind(Path::new(path)), kind, "{path}");
    }
}

#[test]
fn dev_denies_are_never_refused() {
    let denied = [
        "/dev/fd",
        "/dev/fd/3",
        "/dev/stdin",
        "/dev/stdout",
        "/dev/stderr",
        "/dev/core",
        "/dev/pts",
        "/dev/null",
    ];
    for mount_proc in [true, false] {
        let args = bwrap_args(&denied, mount_proc)
            .expect("/dev denies are not procfs denies")
            .args;
        assert!(
            !args.iter().any(|arg| arg.starts_with("/proc/")),
            "{args:#?}"
        );
    }
}
