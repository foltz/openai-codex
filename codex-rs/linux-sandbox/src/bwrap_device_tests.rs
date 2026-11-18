use super::*;
use codex_protocol::protocol::FileSystemSandboxEntry;
use pretty_assertions::assert_eq;
fn without_bind_data_fds(args: &[String]) -> Vec<String> {
    let mut normalized = args.to_vec();
    for index in 1..normalized.len() {
        if normalized[index - 1] == "--ro-bind-data" {
            normalized[index] = "<fd>".to_string();
        }
    }
    normalized
}

fn read_policy_denying(read: FileSystemSpecialPath, denied: &[&str]) -> FileSystemSandboxPolicy {
    let mut entries = vec![FileSystemSandboxEntry {
        path: FileSystemPath::Special { value: read },
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

fn explicit_device_tree(denied: &[&str]) -> Vec<String> {
    let mut args = vec!["--tmpfs".to_string(), "/dev".to_string()];
    for node in ["null", "zero", "full", "random", "urandom", "tty"] {
        let path = format!("/dev/{node}");
        args.extend(["--dev-bind".to_string(), path.clone(), path]);
    }
    for (name, target) in [
        ("stdin", "/proc/self/fd/0"),
        ("stdout", "/proc/self/fd/1"),
        ("stderr", "/proc/self/fd/2"),
        ("fd", "/proc/self/fd"),
        ("core", "/proc/kcore"),
    ] {
        if !denied.contains(&name) {
            args.extend([
                "--symlink".to_string(),
                target.to_string(),
                format!("/dev/{name}"),
            ]);
        }
    }
    args.extend(
        [
            "--dev",
            "/dev/.codex-dev",
            "--symlink",
            ".codex-dev/pts",
            "/dev/pts",
            "--symlink",
            "pts/ptmx",
            "/dev/ptmx",
            "--symlink",
            ".codex-dev/console",
            "/dev/console",
        ]
        .map(String::from),
    );
    for node in ["null", "zero", "full", "random", "urandom", "tty"] {
        args.extend([
            "--perms".to_string(),
            "000".to_string(),
            "--ro-bind-data".to_string(),
            "<fd>".to_string(),
            format!("/dev/.codex-dev/{node}"),
        ]);
    }
    args.extend(["--dir".to_string(), "/dev/shm".to_string()]);
    args
}

fn device_tree(denied: &[&str]) -> (Vec<String>, usize) {
    let roots = denied.iter().map(PathBuf::from).collect::<Vec<_>>();
    let mut args = Vec::new();
    let mut preserved_files = Vec::new();
    append_device_tree_args(&mut args, &mut preserved_files, &roots).expect("device tree");
    (without_bind_data_fds(&args), preserved_files.len())
}

#[test]
fn device_tree_is_plain_dev_without_proc_symlink_denies() {
    assert_eq!(
        device_tree(&[
            "/dev/pts",
            "/dev/ptmx",
            "/dev/console",
            "/dev/zero",
            "/dev/fdx",
        ]),
        (vec!["--dev".to_string(), "/dev".to_string()], 0)
    );
}

#[test]
fn device_tree_omits_denied_proc_symlinks_and_masks_staged_nodes() {
    assert_eq!(
        device_tree(&["/dev/fd"]),
        (explicit_device_tree(&["fd"]), 6)
    );
    assert_eq!(
        device_tree(&["/dev/stdin", "/dev/core"]),
        (explicit_device_tree(&["stdin", "core"]), 6)
    );
    assert_eq!(
        device_tree(&["/dev/fd/3"]),
        (explicit_device_tree(&["fd"]), 6)
    );
}

#[test]
fn denied_dev_fd_is_masked_on_explicit_device_tree() {
    for (read, root_prefix) in [
        (FileSystemSpecialPath::Root, &["--ro-bind", "/", "/"][..]),
        (FileSystemSpecialPath::Minimal, &["--tmpfs", "/"][..]),
    ] {
        let bwrap_args = create_filesystem_args(
            &read_policy_denying(read, &["/dev/fd"]),
            Path::new("/"),
            BwrapOptions::default(),
        )
        .expect("bwrap args");
        assert_eq!(bwrap_args.preserved_files.len(), 6);
        let args = without_bind_data_fds(&bwrap_args.args);
        let mut expected_prefix: Vec<String> =
            root_prefix.iter().map(ToString::to_string).collect();
        expected_prefix.extend(explicit_device_tree(&["fd"]));
        assert_eq!(args[..expected_prefix.len()], expected_prefix);
        assert!(!args.windows(2).any(|window| window == ["--dev", "/dev"]));
        assert!(!args.iter().any(|arg| arg == "/proc/self/fd"));
        let mask = [
            "--perms",
            "000",
            "--tmpfs",
            "/dev/fd",
            "--remount-ro",
            "/dev/fd",
        ];
        assert!(
            args.windows(mask.len()).any(|window| window == mask),
            "denied /dev/fd mask: {args:#?}"
        );
    }
}

#[test]
fn related_dev_denies_follow_relative_links_into_the_stage() {
    for (related, mask) in [
        ("/dev/pts", &["--perms", "000", "--tmpfs", "/dev/pts"][..]),
        (
            "/dev/ptmx",
            &["--perms", "000", "--ro-bind-data", "<fd>", "/dev/ptmx"][..],
        ),
    ] {
        let bwrap_args = create_filesystem_args(
            &read_policy_denying(FileSystemSpecialPath::Root, &["/dev/fd", related]),
            Path::new("/"),
            BwrapOptions::default(),
        )
        .expect("bwrap args");
        let args = without_bind_data_fds(&bwrap_args.args);
        // The canonical names are relative links, so bubblewrap mounts the
        // deny mask on the staged devpts object they point to.
        for link in [
            ["--symlink", ".codex-dev/pts", "/dev/pts"],
            ["--symlink", "pts/ptmx", "/dev/ptmx"],
        ] {
            assert!(args.windows(3).any(|window| window == link), "{args:#?}");
        }
        let stage = args
            .windows(2)
            .position(|window| window == ["--dev", "/dev/.codex-dev"])
            .expect("staged devpts");
        let related_mask = args
            .windows(mask.len())
            .rposition(|window| window == mask)
            .unwrap_or_else(|| panic!("{related}: missing deny mask {mask:?} in {args:#?}"));
        assert!(stage < related_mask, "{related}: {args:#?}");
    }
}
