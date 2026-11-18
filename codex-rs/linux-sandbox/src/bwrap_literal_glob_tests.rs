//! A deny glob without glob syntax is one exact path, not a pattern to scan.

use super::*;
use codex_protocol::protocol::FileSystemSandboxEntry;
use pretty_assertions::assert_eq;
use tempfile::TempDir;

/// Root readable, project roots writable, `denied` denied: as a glob pattern
/// entry (`glob: true`) or as a literal path entry.
fn policy_denying(denied: &str, glob: bool) -> FileSystemSandboxPolicy {
    let deny = if glob {
        FileSystemSandboxEntry {
            path: FileSystemPath::GlobPattern {
                pattern: denied.to_string(),
            },
            access: FileSystemAccessMode::Deny,
            missing_path_behavior: None,
        }
    } else {
        FileSystemSandboxEntry::new(
            AbsolutePathBuf::try_from(denied)
                .expect("absolute deny path")
                .into(),
            FileSystemAccessMode::Deny,
        )
    };
    FileSystemSandboxPolicy::restricted(vec![
        FileSystemSandboxEntry {
            path: FileSystemPath::Special {
                value: FileSystemSpecialPath::Root,
            },
            access: FileSystemAccessMode::Read,
            missing_path_behavior: None,
        },
        FileSystemSandboxEntry {
            path: FileSystemPath::Special {
                value: FileSystemSpecialPath::project_roots(/*subpath*/ None),
            },
            access: FileSystemAccessMode::Write,
            missing_path_behavior: None,
        },
        deny,
    ])
}

fn unreadable_roots(
    policy: &FileSystemSandboxPolicy,
    cwd: &Path,
    depth: Option<usize>,
) -> Result<Vec<PathBuf>> {
    let globs = policy.get_unreadable_globs_with_cwd(cwd);
    let mut roots = collect_unreadable_roots(
        policy,
        cwd,
        &globs,
        BwrapOptions {
            glob_scan_max_depth: depth,
            ..Default::default()
        },
    )?;
    roots.sort();
    roots.dedup();
    Ok(roots)
}

fn display(path: &Path) -> String {
    path.to_str().expect("utf-8 test path").to_string()
}

#[test]
fn only_patterns_without_glob_syntax_are_literal() {
    let cwd = Path::new("/work");
    assert_eq!(
        literal_deny_pattern("/tmp/.tmpAb_9.x/secret.env", cwd),
        Some(AbsolutePathBuf::try_from("/tmp/.tmpAb_9.x/secret.env").expect("absolute"))
    );
    assert_eq!(
        literal_deny_pattern("secret.env", cwd),
        Some(AbsolutePathBuf::try_from("/work/secret.env").expect("absolute"))
    );
    for pattern in [
        "/tmp/*.env",
        "/tmp/secret.en?",
        "/tmp/[s]ecret.env",
        "/tmp/secret]",
        "/tmp/{a,b}.env",
        r"/tmp/secret\*.env",
        "/**/*.env",
    ] {
        assert_eq!(literal_deny_pattern(pattern, cwd), None, "{pattern}");
    }
}

/// Literal denies are never scanned, so no scan depth, including zero, can
/// drop them.
const DEPTHS: [Option<usize>; 3] = [None, Some(0), Some(2)];

/// The literal as a glob entry collects exactly what the same literal path
/// entry collects, at every scan depth, and includes `expected`.
fn assert_literal_collected_like_exact_path(
    denied: &Path,
    cwd: &Path,
    expected: &[PathBuf],
) -> Result<()> {
    let denied = display(denied);
    for depth in DEPTHS {
        let as_glob = unreadable_roots(&policy_denying(&denied, true), cwd, depth)?;
        let as_path = unreadable_roots(&policy_denying(&denied, false), cwd, None)?;
        for path in expected {
            assert!(
                as_glob.contains(path),
                "depth {depth:?}: {} missing from {as_glob:#?}",
                path.display()
            );
        }
        for path in &as_path {
            assert!(
                as_glob.contains(path),
                "depth {depth:?}: exact-path root {} missing from {as_glob:#?}",
                path.display()
            );
        }
        validate_proc_deny_rules(&policy_denying(&denied, true), cwd)?;
    }
    Ok(())
}

#[test]
fn literal_deny_glob_file_is_an_exact_deny_path() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let secret = temp_dir.path().join("secret.env");
    std::fs::write(&secret, "secret")?;
    assert_literal_collected_like_exact_path(
        &secret,
        temp_dir.path(),
        std::slice::from_ref(&secret),
    )
}

#[test]
fn literal_deny_glob_directory_is_an_exact_deny_path() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let private = temp_dir.path().join("original-environment-private");
    std::fs::create_dir(&private)?;
    std::fs::write(private.join("inside"), "secret")?;
    assert_literal_collected_like_exact_path(
        &private,
        temp_dir.path(),
        std::slice::from_ref(&private),
    )
}

#[test]
fn absent_literal_deny_glob_follows_the_exact_path_absence_policy() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let missing_parent = temp_dir.path().join("not-created-yet");
    let missing = missing_parent.join("secret.env");
    assert_literal_collected_like_exact_path(
        &missing,
        temp_dir.path(),
        std::slice::from_ref(&missing),
    )?;

    // Collected like a literal path, it reaches the same mask builder, which
    // owns the absence policy: under a writable root the first missing
    // component is masked, elsewhere nothing is mounted.
    let empty = || BwrapArgs {
        args: Vec::new(),
        preserved_files: Vec::new(),
        synthetic_mount_targets: Vec::new(),
    };
    let mut under_writable = empty();
    append_unreadable_root_args(
        &mut under_writable,
        &missing,
        &[temp_dir.path().to_path_buf()],
    )?;
    assert!(
        under_writable.args.contains(&display(&missing_parent)),
        "{:#?}",
        under_writable.args
    );
    let mut elsewhere = empty();
    append_unreadable_root_args(&mut elsewhere, &missing, &[])?;
    assert_eq!(Vec::<String>::new(), elsewhere.args);
    Ok(())
}

#[test]
fn symlinked_literal_deny_glob_also_masks_the_canonical_target() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let real = temp_dir.path().join("real.env");
    let link = temp_dir.path().join("link.env");
    std::fs::write(&real, "secret")?;
    std::os::unix::fs::symlink(&real, &link)?;
    assert_literal_collected_like_exact_path(
        &link,
        temp_dir.path(),
        &[link.clone(), std::fs::canonicalize(&real)?],
    )
}

#[test]
fn literal_deny_glob_under_proc_gets_the_exact_path_proc_checks() {
    // The shared `/proc` validation decides both: an existing entry is
    // accepted, an absent shared entry refused, the same for either form.
    for denied in ["/proc/sys", "/proc/sys/kernel/kcf-no-such-entry"] {
        let outcome = |glob: bool| {
            validate_proc_deny_rules(&policy_denying(denied, glob), Path::new("/"))
                .map_err(|err| err.to_string())
        };
        assert_eq!(outcome(false), outcome(true), "{denied}");
    }
    assert!(
        validate_proc_deny_rules(
            &policy_denying("/proc/sys/kernel/kcf-no-such-entry", true),
            Path::new("/")
        )
        .is_err(),
        "an absent shared /proc entry is refused for a literal deny glob too"
    );
}

#[test]
fn root_prefix_glob_is_still_refused() {
    let policy = policy_denying("/**/*.env", true);
    let error = unreadable_roots(&policy, Path::new("/tmp"), None)
        .expect_err("a root-prefix deny glob must still fail closed");
    assert_eq!(
        error.to_string(),
        "Fatal error: unreadable glob `/**/*.env` cannot be safely expanded; use a pattern with a non-root directory prefix"
    );
}

#[test]
fn brace_pattern_splits_before_the_directory_alternation() {
    let (root, pattern) = split_glob_pattern("/work/{first,second}/secret.env", Path::new("/"))
        .expect("a brace alternation has a static non-root prefix");
    assert_eq!(root.as_path(), Path::new("/work"));
    assert_eq!(pattern, "{first,second}/secret.env");
}
