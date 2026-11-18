use super::*;
use crate::bwrap::SyntheticMountTarget;
use pretty_assertions::assert_eq;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;
use tempfile::TempDir;

const PRINT_REGISTRY_ENV: &str = "CODEX_LINUX_SANDBOX_TEST_PRINT_REGISTRY_PATH";

struct Fixture {
    _base: TempDir,
    registry: Registry,
    work: TempDir,
}

fn fixture() -> Fixture {
    let base = TempDir::new().expect("registry base");
    // A trusted base must not be group- or other-writable; do not depend on
    // the process umask for that.
    fs::set_permissions(base.path(), fs::Permissions::from_mode(0o700)).expect("chmod base");
    let registry = Registry::open_trusted(base.path(), false).expect("trusted test registry");
    Fixture {
        _base: base,
        registry,
        work: TempDir::new().expect("work dir"),
    }
}

/// Recovery and the creation evidence need kernel file handles; filesystems
/// without them (overlayfs without `nfs_export`) are covered separately.
fn file_handles_supported(fixture: &Fixture) -> bool {
    let dir = File::open(fixture.work.path()).expect("open work dir");
    identity_of(dir.as_fd()).expect("identity").handle.is_some()
}

fn reaped_child_pid() -> libc::pid_t {
    let mut child = std::process::Command::new("true")
        .spawn()
        .expect("spawn short-lived child");
    let pid = libc::pid_t::try_from(child.id()).expect("pid fits pid_t");
    child.wait().expect("reap short-lived child");
    pid
}

/// A process of this boot that has exited.
fn terminated_token() -> ProcessToken {
    ProcessToken::for_tests(read_boot_id().expect("boot id"), reaped_child_pid(), 1)
}

fn own_token() -> ProcessToken {
    ProcessToken::current().expect("own token")
}

fn dir_target(path: &Path) -> SyntheticMountTarget {
    SyntheticMountTarget::missing_empty_directory(path)
}

fn register(
    fixture: &Fixture,
    targets: &[SyntheticMountTarget],
    launch: &ProcessToken,
) -> Vec<SyntheticTargetRegistration> {
    let _lock = fixture.registry.lock().expect("lock");
    register_synthetic_targets(
        &fixture.registry,
        targets,
        &LaunchIdentity::single(launch.clone()),
        /*pid_namespace*/ true,
        &mut CleanupDeadline::none(),
    )
    .into_result()
    .expect("register")
}

fn release(
    fixture: &Fixture,
    registrations: &[SyntheticTargetRegistration],
    launch: &ProcessToken,
    proof: SandboxProof,
    scan: &mut NamespaceScan,
) -> Vec<(PathBuf, TargetOutcome)> {
    let _lock = fixture.registry.lock().expect("lock");
    release_synthetic_targets_with(
        &fixture.registry,
        registrations,
        &LaunchIdentity::single(launch.clone()),
        proof,
        &mut CleanupDeadline::none(),
        scan,
    )
    .expect("release")
}

fn recover(fixture: &Fixture, key: &str, scan: &mut NamespaceScan) -> Option<TargetOutcome> {
    let _lock = fixture.registry.lock().expect("lock");
    recover_key(&fixture.registry, key, scan).expect("recover")
}

fn only_record(fixture: &Fixture, key: &str) -> CreationRecord {
    let records = fixture.registry.records(key).expect("records");
    assert_eq!(records.len(), 1, "exactly one creation record");
    records[0].1.clone().expect("decodable record")
}

/// A launch whose launcher and sandbox init were killed before cleanup: its
/// records, holder and target are left behind with proven-terminated tokens.
fn killed_launch_leftover(fixture: &Fixture, path: &Path) -> String {
    let launch = terminated_token();
    let mut registrations = register(fixture, &[dir_target(path)], &launch);
    {
        let _lock = fixture.registry.lock().expect("lock");
        record_sandbox_tokens(
            &fixture.registry,
            &mut registrations,
            &LaunchIdentity::single(launch),
            &SandboxTokens::for_tests(terminated_token()),
            &mut CleanupDeadline::none(),
        )
        .expect("record sandbox tokens");
    }
    let key = registrations[0].key.clone();
    drop(registrations);
    key
}

#[test]
fn creation_evidence_and_running_receipt_precede_bubblewrap() {
    let fixture = fixture();
    if !file_handles_supported(&fixture) {
        return;
    }
    let dot_git = fixture.work.path().join(".git");
    let launch = own_token();

    let registrations = register(&fixture, &[dir_target(&dot_git)], &launch);

    // Nothing has been forked, yet the installed target already has its
    // exact identity, creator and a `running` launch receipt published.
    let installed = File::open(&dot_git).expect("installed target");
    let record = only_record(&fixture, &registrations[0].key);
    assert_eq!(
        record.identity,
        identity_of(installed.as_fd()).expect("identity")
    );
    assert!(record.identity.handle.is_some());
    assert_eq!(record.creator, launch);
    let receipt = fixture
        .registry
        .launch_receipt(&launch)
        .expect("read receipt")
        .expect("running receipt");
    assert_eq!(receipt.state, CleanupState::Running);
    assert_eq!(
        receipt.targets,
        vec![(dot_git.clone(), TargetOutcome::Pending)]
    );
    assert_eq!(
        fs::read_dir(fixture.work.path()).expect("work dir").count(),
        1,
        "no staging entry is left"
    );

    let census = release(
        &fixture,
        &registrations,
        &launch,
        SandboxProof::Gone,
        &mut NamespaceScan::default(),
    );
    assert_eq!(census, vec![(dot_git.clone(), TargetOutcome::Reaped)]);
    assert!(!dot_git.exists());
    assert!(!fixture.registry.at(&registrations[0].key).exists());
}

#[test]
fn recovery_removes_exact_empty_leftover_of_terminated_launch() {
    let fixture = fixture();
    if !file_handles_supported(&fixture) {
        return;
    }
    let dot_git = fixture.work.path().join(".git");
    let key = killed_launch_leftover(&fixture, &dot_git);

    let outcome = recover(&fixture, &key, &mut NamespaceScan::default());

    assert_eq!(outcome, Some(TargetOutcome::Reaped));
    assert!(!dot_git.exists());
    assert!(!fixture.registry.at(&key).exists());
}

/// The second cleanup route, deterministically: a key recovery did not reach
/// (deferred by its budget, or not yet reached in its order) is joined by
/// the next launch's registration, which proves the leftover is codex's by
/// its creation record (file handle), and that launch's own release reaps
/// it once the terminated launch's holder is proven gone. No recovery
/// receipt is written.
#[test]
fn a_leftover_recovery_did_not_reach_is_joined_and_reaped_by_the_next_launch() {
    let fixture = fixture();
    if !file_handles_supported(&fixture) {
        return;
    }
    let dot_git = fixture.work.path().join(".git");
    let key = killed_launch_leftover(&fixture, &dot_git);
    assert!(dot_git.is_dir());

    // The next launch's registration of this target, without recovery.
    let joiner = own_token();
    let registration = {
        let _lock = fixture.registry.lock().expect("lock");
        match register_target(
            &fixture.registry,
            &dir_target(&dot_git),
            &joiner,
            /*pid_namespace*/ true,
            /*index*/ 0,
            &mut CleanupDeadline::none(),
        ) {
            Ok(registration) => registration,
            Err(_) => panic!("the leftover is joined"),
        }
    };
    assert_eq!(registration.key, key);
    assert_eq!(
        registration.holder.as_ref().map(|holder| holder.role),
        Some(HolderRole::Join)
    );
    assert!(registration.held.is_some(), "the joined target is held");

    let census = release(
        &fixture,
        &[registration],
        &joiner,
        SandboxProof::Gone,
        &mut NamespaceScan::default(),
    );

    assert_eq!(census, vec![(dot_git.clone(), TargetOutcome::Reaped)]);
    assert!(!dot_git.exists());
    assert!(!fixture.registry.at(&key).exists(), "the key is retired");
    let recovery_receipts = fs::read_dir(fixture.registry.at(RECEIPTS_DIR))
        .into_iter()
        .flatten()
        .flatten()
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with(RECOVERY_RECEIPT_PREFIX))
        })
        .count();
    assert_eq!(recovery_receipts, 0, "no recovery receipt on this route");
}

#[test]
fn recovery_never_relabels_a_timed_out_launch_receipt() {
    let fixture = fixture();
    if !file_handles_supported(&fixture) {
        return;
    }
    let dot_git = fixture.work.path().join(".git");
    let launch = terminated_token();
    let mut registrations = register(&fixture, &[dir_target(&dot_git)], &launch);
    {
        let _lock = fixture.registry.lock().expect("lock");
        record_sandbox_tokens(
            &fixture.registry,
            &mut registrations,
            &LaunchIdentity::single(launch.clone()),
            &SandboxTokens::for_tests(terminated_token()),
            &mut CleanupDeadline::none(),
        )
        .expect("record");
        // The reaper hit its deadline before the sandbox exited.
        let census = release_synthetic_targets(
            &fixture.registry,
            &registrations,
            &LaunchIdentity::single(launch.clone()),
            SandboxProof::TimedOut,
            &mut CleanupDeadline::none(),
        )
        .expect("release");
        assert_eq!(census, vec![(dot_git.clone(), TargetOutcome::TimedOut)]);
        fixture
            .registry
            .write_launch_receipt(&LaunchReceipt {
                launch: launch.clone(),
                state: CleanupState::of(&census),
                finisher: Some("reaper"),
                reaper: None,
                reaper_join: ReaperJoin::FinishedByReaper,
                targets: census,
            })
            .expect("timed-out receipt");
    }
    drop(registrations);
    assert!(dot_git.is_dir(), "a timed-out target is preserved");

    let by = own_token();
    {
        let _lock = fixture.registry.lock().expect("lock");
        recover_registry(&fixture.registry, &by);
    }

    assert!(!dot_git.exists(), "recovery removes the proven leftover");
    let receipt = fixture
        .registry
        .launch_receipt(&launch)
        .expect("read")
        .expect("launch receipt");
    assert_eq!(receipt.state, CleanupState::TimedOut);
    let recovery = fs::read_to_string(fixture.registry.at(&format!(
        "{RECEIPTS_DIR}/{RECOVERY_RECEIPT_PREFIX}{}",
        by.encode()
    )))
    .expect("recovery receipt");
    assert!(recovery.contains("kind=recovery"), "{recovery}");
    assert!(recovery.contains("key=reaped "), "{recovery}");
}

#[test]
fn nothing_is_cleaned_after_the_deadline() {
    let fixture = fixture();
    let dot_git = fixture.work.path().join(".git");
    let launch = own_token();
    let registrations = register(&fixture, &[dir_target(&dot_git)], &launch);

    // The sandbox is proven gone, but the owner's deadline already passed.
    let census = {
        let _lock = fixture.registry.lock().expect("lock");
        release_synthetic_targets(
            &fixture.registry,
            &registrations,
            &LaunchIdentity::single(launch.clone()),
            SandboxProof::Gone,
            &mut CleanupDeadline::at_for_tests(0),
        )
        .expect("release")
    };

    assert_eq!(census, vec![(dot_git.clone(), TargetOutcome::TimedOut)]);
    assert_eq!(CleanupState::of(&census), CleanupState::TimedOut);
    assert!(dot_git.is_dir(), "zero cleanup after expiry");
    assert!(
        fixture
            .registry
            .key_file(&registrations[0].key, &holder_file(&launch))
            .exists(),
        "the holder is retained for recovery"
    );
}

/// A deadline whose owner is gone without ever having issued one.
fn owner_gone_deadline() -> CleanupDeadline {
    let mut fds = [-1; 2];
    // SAFETY: valid two-element array.
    assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
    // SAFETY: pipe2 returned two new owned descriptors.
    let (read, write) = unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
    let deadline = CleanupDeadline::new(None, Some(read)).expect("deadline");
    drop(write);
    deadline
}

#[test]
fn an_owner_gone_without_a_deadline_preserves_everything_as_unproven() {
    let fixture = fixture();
    let dot_git = fixture.work.path().join(".git");
    let launch = own_token();
    let registrations = register(&fixture, &[dir_target(&dot_git)], &launch);

    // Even with the sandbox proven gone, no cleanup is driven and the result
    // is unproven rather than a timeout.
    let census = {
        let _lock = fixture.registry.lock().expect("lock");
        release_synthetic_targets(
            &fixture.registry,
            &registrations,
            &LaunchIdentity::single(launch.clone()),
            SandboxProof::Gone,
            &mut owner_gone_deadline(),
        )
        .expect("release")
    };

    assert_eq!(
        census,
        vec![(
            dot_git.clone(),
            TargetOutcome::Incomplete(IncompleteReason::OwnerGoneWithoutDeadline)
        )]
    );
    assert_eq!(CleanupState::of(&census), CleanupState::Incomplete);
    assert!(dot_git.is_dir());
    assert_eq!(only_record(&fixture, &registrations[0].key).creator, launch);
    assert!(
        fixture
            .registry
            .key_file(&registrations[0].key, &holder_file(&launch))
            .exists()
    );
}

#[test]
fn foreign_replacement_at_the_same_path_is_preserved() {
    let fixture = fixture();
    if !file_handles_supported(&fixture) {
        return;
    }
    let dot_git = fixture.work.path().join(".git");
    let key = killed_launch_leftover(&fixture, &dot_git);
    fs::remove_dir(&dot_git).expect("remove leftover");
    fs::create_dir(&dot_git).expect("foreign replacement");

    let outcome = recover(&fixture, &key, &mut NamespaceScan::default());

    assert_eq!(
        outcome,
        Some(TargetOutcome::Incomplete(IncompleteReason::Replaced))
    );
    assert!(dot_git.is_dir(), "replacement must survive");
    assert_eq!(fixture.registry.records(&key).expect("records").len(), 1);
}

#[test]
fn non_empty_leftover_is_preserved() {
    let fixture = fixture();
    if !file_handles_supported(&fixture) {
        return;
    }
    let dot_git = fixture.work.path().join(".git");
    let key = killed_launch_leftover(&fixture, &dot_git);
    fs::write(dot_git.join("HEAD"), "ref: refs/heads/main\n").expect("populate");

    let outcome = recover(&fixture, &key, &mut NamespaceScan::default());

    assert_eq!(
        outcome,
        Some(TargetOutcome::Incomplete(IncompleteReason::NotEmpty))
    );
    assert!(dot_git.join("HEAD").exists());
    assert_eq!(fixture.registry.records(&key).expect("records").len(), 1);
}

#[test]
fn uncertain_records_are_preserved() {
    let fixture = fixture();

    // A launch that is still running.
    let live = fixture.work.path().join(".agents");
    let registrations = register(&fixture, &[dir_target(&live)], &own_token());
    let live_key = registrations[0].key.clone();
    assert_eq!(
        recover(&fixture, &live_key, &mut NamespaceScan::default()),
        None
    );
    assert!(live.is_dir());
    std::mem::forget(registrations);

    // A record that cannot be decoded.
    let dot_git = fixture.work.path().join(".git");
    let key = killed_launch_leftover(&fixture, &dot_git);
    let (name, _) = fixture.registry.records(&key).expect("records").remove(0);
    fs::write(fixture.registry.key_file(&key, &name), "garbage").expect("corrupt record");
    assert_eq!(recover(&fixture, &key, &mut NamespaceScan::default()), None);
    assert!(dot_git.is_dir());
    assert!(fixture.registry.key_file(&key, &name).exists());
}

#[test]
fn uninspectable_namespaces_keep_targets_and_records_incomplete() {
    let fixture = fixture();

    // The launch's own release cannot prove its sandbox gone.
    let dot_codex = fixture.work.path().join(".codex");
    let launch = own_token();
    let registrations = register(&fixture, &[dir_target(&dot_codex)], &launch);
    let census = release(
        &fixture,
        &registrations,
        &launch,
        SandboxProof::Unproven,
        &mut NamespaceScan::uninspectable_for_tests(),
    );
    assert_eq!(
        census,
        vec![(
            dot_codex.clone(),
            TargetOutcome::Incomplete(IncompleteReason::SandboxNotProvenGone)
        )]
    );
    assert!(dot_codex.is_dir());
    assert!(
        fixture
            .registry
            .key_file(&registrations[0].key, &holder_file(&launch))
            .exists(),
        "the holder with its termination proof is kept"
    );

    // Recovery of a launch without a PID-namespace init proof.
    let dot_git = fixture.work.path().join(".git");
    let dead = terminated_token();
    let registrations = register(&fixture, &[dir_target(&dot_git)], &dead);
    let key = registrations[0].key.clone();
    drop(registrations);
    assert_eq!(
        recover(
            &fixture,
            &key,
            &mut NamespaceScan::uninspectable_for_tests()
        ),
        Some(TargetOutcome::Incomplete(IncompleteReason::HolderUnproven))
    );
    assert!(dot_git.is_dir());
    assert_eq!(fixture.registry.records(&key).expect("records").len(), 1);
}

/// Simulates a filesystem without file handles (overlayfs without
/// `nfs_export`): the published records carry no generation.
fn strip_file_handles(fixture: &Fixture, key: &str) {
    let _lock = fixture.registry.lock().expect("lock");
    for (name, record) in fixture.registry.records(key).expect("records") {
        let mut record = record.expect("decodable record");
        record.identity.handle = None;
        fixture
            .registry
            .write_key_file(key, &name, record.encode().as_bytes())
            .expect("rewrite record");
    }
}

#[test]
fn without_file_handles_only_the_held_target_descriptor_proves_identity() {
    let fixture = fixture();

    // The live launch still holds its O_PATH target descriptor: it may clean.
    let dot_git = fixture.work.path().join(".git");
    let launch = own_token();
    let registrations = register(&fixture, &[dir_target(&dot_git)], &launch);
    strip_file_handles(&fixture, &registrations[0].key);
    let census = release(
        &fixture,
        &registrations,
        &launch,
        SandboxProof::Gone,
        &mut NamespaceScan::default(),
    );
    assert_eq!(census, vec![(dot_git.clone(), TargetOutcome::Reaped)]);
    assert!(!dot_git.exists());

    // A killed launch's descriptor is gone: recovery preserves as unproven.
    let dot_codex = fixture.work.path().join(".codex");
    let key = killed_launch_leftover(&fixture, &dot_codex);
    strip_file_handles(&fixture, &key);
    assert_eq!(
        recover(&fixture, &key, &mut NamespaceScan::default()),
        Some(TargetOutcome::Incomplete(
            IncompleteReason::IdentityUnprovable
        ))
    );
    assert!(dot_codex.is_dir());
    assert_eq!(fixture.registry.records(&key).expect("records").len(), 1);
}

#[test]
fn pre_existing_host_directory_is_never_removed() {
    let fixture = fixture();
    let dot_git = fixture.work.path().join(".git");
    fs::create_dir(&dot_git).expect("host-owned empty directory");
    let launch = own_token();

    let registrations = register(&fixture, &[dir_target(&dot_git)], &launch);
    assert!(!registrations[0].is_registry_owned());
    let census = release(
        &fixture,
        &registrations,
        &launch,
        SandboxProof::Gone,
        &mut NamespaceScan::default(),
    );

    assert_eq!(census, vec![(dot_git.clone(), TargetOutcome::HostOwned)]);
    assert!(dot_git.is_dir());
}

#[test]
fn existing_target_of_unexpected_kind_is_refused() {
    let fixture = fixture();
    let dot_git = fixture.work.path().join(".git");
    std::os::unix::fs::symlink(fixture.work.path(), &dot_git).expect("symlink");

    let _lock = fixture.registry.lock().expect("lock");
    let result = register_synthetic_targets(
        &fixture.registry,
        &[dir_target(&dot_git)],
        &LaunchIdentity::single(own_token()),
        /*pid_namespace*/ true,
        &mut CleanupDeadline::none(),
    )
    .into_result();

    assert!(result.is_err());
    assert!(dot_git.is_symlink());
}

#[test]
fn launch_receipts_round_trip() {
    let receipt = LaunchReceipt {
        launch: own_token(),
        state: CleanupState::TimedOut,
        finisher: Some("reaper"),
        reaper: Some(terminated_token()),
        reaper_join: ReaperJoin::FinishedByReaper,
        targets: vec![
            (PathBuf::from("/tmp/a b/.git"), TargetOutcome::TimedOut),
            (
                PathBuf::from("/w/.codex"),
                TargetOutcome::Incomplete(IncompleteReason::SandboxNotProvenGone),
            ),
        ],
    };
    assert_eq!(LaunchReceipt::decode(&receipt.encode()), Some(receipt));
}

#[test]
fn shared_registry_location_does_not_follow_tmpdir() {
    if std::env::var_os(PRINT_REGISTRY_ENV).is_some() {
        match shared_registry_path() {
            Ok(path) => println!("REGISTRY={}", path.display()),
            Err(_) => println!("REGISTRY-UNAVAILABLE"),
        }
        return;
    }
    // Needs a per-uid base (`/run/user/<uid>` or a passwd home).
    if shared_registry_path().is_err() {
        return;
    }
    let run = |tmpdir: &Path| {
        let output = std::process::Command::new(std::env::current_exe().expect("test binary"))
            .args([
                "--exact",
                "synthetic_targets::tests::shared_registry_location_does_not_follow_tmpdir",
                "--nocapture",
            ])
            .env(PRINT_REGISTRY_ENV, "1")
            .env("TMPDIR", tmpdir)
            .output()
            .expect("run test binary");
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .find_map(|line| line.strip_prefix("REGISTRY=").map(PathBuf::from))
            .expect("registry path printed")
    };
    let first = TempDir::new().expect("first TMPDIR");
    let second = TempDir::new().expect("second TMPDIR");

    let first_registry = run(first.path());
    let second_registry = run(second.path());

    assert_eq!(first_registry, second_registry);
    assert!(!first_registry.starts_with(first.path()));
    assert!(!second_registry.starts_with(second.path()));
}

#[test]
fn hostile_registry_locations_are_refused() {
    // Writable by others.
    let open_base = TempDir::new().expect("base");
    fs::set_permissions(open_base.path(), fs::Permissions::from_mode(0o777)).expect("chmod");
    assert!(Registry::open_trusted(open_base.path(), false).is_err());

    // Registry directory with a permissive mode.
    let base = TempDir::new().expect("base");
    fs::create_dir(base.path().join(REGISTRY_NAME)).expect("registry dir");
    fs::set_permissions(
        base.path().join(REGISTRY_NAME),
        fs::Permissions::from_mode(0o755),
    )
    .expect("chmod");
    assert!(Registry::open_trusted(base.path(), false).is_err());

    // Registry replaced by a symlink to an otherwise acceptable directory.
    let base = TempDir::new().expect("base");
    let elsewhere = TempDir::new().expect("elsewhere");
    std::os::unix::fs::symlink(elsewhere.path(), base.path().join(REGISTRY_NAME)).expect("symlink");
    assert!(Registry::open_trusted(base.path(), false).is_err());

    // Owned by another user.
    if unsafe { libc::geteuid() } != 0 {
        assert!(Registry::open_trusted(Path::new("/"), false).is_err());
    }
}

const UMASK_WITNESS_BASE_ENV: &str = "CODEX_LINUX_SANDBOX_TEST_UMASK_REGISTRY_BASE";
const UMASK_WITNESS_WORK_ENV: &str = "CODEX_LINUX_SANDBOX_TEST_UMASK_WORK";

/// Every registry directory and file a registration creates is 0700/0600
/// regardless of the umask. The umask is process-global, so the registration
/// runs in a child process (this test binary, re-executed) under umask 002,
/// against an injected scratch registry base; never the real registry.
#[test]
fn registry_modes_do_not_follow_the_umask() {
    if let (Some(base), Some(work)) = (
        std::env::var_os(UMASK_WITNESS_BASE_ENV),
        std::env::var_os(UMASK_WITNESS_WORK_ENV),
    ) {
        // SAFETY: umask only changes this child process's creation mask.
        unsafe { libc::umask(0o002) };
        let work = PathBuf::from(work);
        // A plain directory shows the umask in effect (0775).
        fs::create_dir(work.join("umask-probe")).expect("umask probe");
        with_test_registry_base(Path::new(&base), || {
            let registry = open_shared_registry().expect("scratch registry");
            let _lock = registry.lock().expect("lock");
            register_synthetic_targets(
                &registry,
                &[dir_target(&work.join(".git"))],
                &LaunchIdentity::single(own_token()),
                /*pid_namespace*/ true,
                &mut CleanupDeadline::none(),
            )
            .into_result()
            .expect("register");
            registry
                .write_receipt("umask-probe", "v=1\n")
                .expect("write receipt");
        });
        return;
    }

    let scratch = TempDir::new().expect("scratch");
    let base = scratch.path().join("base");
    let work = scratch.path().join("work");
    for dir in [&base, &work] {
        fs::create_dir(dir).expect("create fixture dir");
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).expect("chmod fixture dir");
    }
    let output = std::process::Command::new(std::env::current_exe().expect("test binary"))
        .args([
            "--exact",
            "synthetic_targets::tests::registry_modes_do_not_follow_the_umask",
            "--nocapture",
        ])
        .env(UMASK_WITNESS_BASE_ENV, &base)
        .env(UMASK_WITNESS_WORK_ENV, &work)
        .output()
        .expect("run test binary");
    assert!(output.status.success(), "{output:?}");

    // The child really ran under umask 002.
    let probe_mode = fs::metadata(work.join("umask-probe"))
        .expect("umask probe")
        .permissions()
        .mode();
    assert_eq!(probe_mode & 0o777, 0o775, "the child ran under umask 002");

    let registry = base.join(REGISTRY_NAME);
    let mut checked = Vec::new();
    let mut pending = vec![registry.clone()];
    while let Some(dir) = pending.pop() {
        for entry in fs::read_dir(&dir).expect("read registry dir") {
            let entry = entry.expect("entry");
            let metadata = fs::symlink_metadata(entry.path()).expect("metadata");
            let expected = if metadata.is_dir() {
                pending.push(entry.path());
                0o700
            } else {
                assert!(metadata.is_file(), "{}", entry.path().display());
                0o600
            };
            checked.push((
                entry.path().strip_prefix(&registry).unwrap().to_path_buf(),
                metadata.permissions().mode() & 0o7777,
                expected,
            ));
        }
    }
    let wrong: Vec<_> = checked
        .iter()
        .filter(|(_, mode, expected)| mode != expected)
        .collect();
    assert_eq!(wrong, Vec::<&(PathBuf, u32, u32)>::new(), "{checked:?}");
    let names: Vec<String> = checked
        .iter()
        .map(|(path, _, _)| path.to_string_lossy().into_owned())
        .collect();
    assert!(names.iter().any(|name| name == RECEIPTS_DIR), "{names:?}");
    let key = names
        .iter()
        .find(|name| name.starts_with(KEY_PREFIX) && !name.contains('/'))
        .expect("key directory")
        .clone();

    // A key directory or record that does not verify is refused, not used.
    let opened = Registry::open_trusted(&base, false).expect("registry");
    fs::set_permissions(registry.join(&key), fs::Permissions::from_mode(0o775)).expect("chmod key");
    let refused = opened
        .ensure_key(&key, &work.join(".git"))
        .expect_err("a 0775 key directory is refused");
    assert_eq!(refused.kind(), io::ErrorKind::PermissionDenied, "{refused}");
    assert!(opened.key_path(&key).is_err());
    fs::set_permissions(registry.join(&key), fs::Permissions::from_mode(0o700)).expect("chmod key");
    fs::set_permissions(
        registry.join(&key).join(PATH_FILE),
        fs::Permissions::from_mode(0o644),
    )
    .expect("chmod path file");
    let refused = opened.key_path(&key).expect_err("a 0644 record is refused");
    assert_eq!(refused.kind(), io::ErrorKind::PermissionDenied, "{refused}");
}

/// What an entry is, without following it: its type, mode and contents (or
/// symlink target), to show a refused operation left it unchanged.
fn entry_snapshot(path: &Path) -> (u32, Vec<u8>) {
    use std::os::unix::ffi::OsStringExt;
    use std::os::unix::fs::MetadataExt;

    let metadata = fs::symlink_metadata(path).expect("entry metadata");
    let contents = if metadata.file_type().is_symlink() {
        fs::read_link(path)
            .expect("symlink target")
            .into_os_string()
            .into_vec()
    } else if metadata.is_file() {
        fs::read(path).expect("file contents")
    } else {
        Vec::new()
    };
    (metadata.mode(), contents)
}

/// Existing registry entries that do not verify (wrong mode, FIFO, symlink,
/// a permissive lock or key directory) are refused before anything changes
/// them, and are left exactly as found. Runs against an injected scratch
/// registry root, never the real registry.
#[test]
fn untrusted_existing_registry_entries_are_refused_and_left_unchanged() {
    use std::os::unix::fs::MetadataExt;

    let scratch = TempDir::new().expect("scratch");
    let base = scratch.path().join("base");
    fs::create_dir(&base).expect("base");
    fs::set_permissions(&base, fs::Permissions::from_mode(0o700)).expect("chmod base");
    let registry = with_test_registry_base(&base, open_shared_registry).expect("scratch registry");
    let root = base.join(REGISTRY_NAME);
    let key = "t-witness";
    registry
        .ensure_key(key, Path::new("/witness/.git"))
        .expect("key");

    let bad_mode = root.join(key).join("created.bad-mode");
    fs::write(&bad_mode, b"original").expect("bad-mode record");
    fs::set_permissions(&bad_mode, fs::Permissions::from_mode(0o644)).expect("chmod record");
    let fifo = root.join(key).join("created.fifo");
    let fifo_c = path_cstring(&fifo).expect("fifo path");
    // SAFETY: valid NUL-terminated path.
    assert_eq!(unsafe { libc::mkfifo(fifo_c.as_ptr(), 0o600) }, 0, "mkfifo");
    let outside = scratch.path().join("outside");
    fs::write(&outside, b"outside").expect("outside file");
    fs::set_permissions(&outside, fs::Permissions::from_mode(0o600)).expect("chmod outside");
    let link = root.join(key).join("created.link");
    std::os::unix::fs::symlink(&outside, &link).expect("symlink record");

    for path in [&bad_mode, &fifo, &link] {
        let name = path.file_name().unwrap().to_str().unwrap();
        let before = entry_snapshot(path);
        let outside_before = entry_snapshot(&outside);
        let refused = registry
            .write_key_file(key, name, b"replacement")
            .expect_err("an untrusted record is not replaced");
        assert_eq!(
            refused.kind(),
            io::ErrorKind::PermissionDenied,
            "{name}: {refused}"
        );
        let refused = registry
            .remove_key_file(key, name)
            .expect_err("an untrusted record is not removed");
        assert_eq!(
            refused.kind(),
            io::ErrorKind::PermissionDenied,
            "{name}: {refused}"
        );
        assert!(
            registry.records(key).is_err(),
            "{name}: records are not trusted"
        );
        assert_eq!(entry_snapshot(path), before, "{name} changed");
        assert_eq!(entry_snapshot(&outside), outside_before, "{name} followed");
        fs::remove_file(path).expect("drop the witness entry");
    }

    // A leftover temp entry from an older layout is never opened or reused:
    // the write goes through a fresh, exclusively created temp file.
    let leftover = root.join(key).join(format!("{TEMP_PREFIX}created.ok"));
    fs::write(&leftover, b"leftover").expect("leftover temp");
    fs::set_permissions(&leftover, fs::Permissions::from_mode(0o644)).expect("chmod leftover");
    let leftover_before = entry_snapshot(&leftover);
    registry
        .write_key_file(key, "created.ok", b"fresh")
        .expect("write through a fresh temp");
    assert_eq!(entry_snapshot(&leftover), leftover_before);
    assert_eq!(
        entry_snapshot(&root.join(key).join("created.ok")),
        (libc::S_IFREG as u32 | 0o600, b"fresh".to_vec())
    );
    // Exclusive creation never opens an existing entry.
    let refused = create_new_private_file(&leftover).expect_err("existing entry");
    assert_eq!(refused.kind(), io::ErrorKind::AlreadyExists);
    assert_eq!(entry_snapshot(&leftover), leftover_before);

    // An existing permissive lock is refused, not repaired.
    let lock = root.join(LOCK_NAME);
    fs::write(&lock, b"").expect("lock");
    fs::set_permissions(&lock, fs::Permissions::from_mode(0o644)).expect("chmod lock");
    let lock_before = entry_snapshot(&lock);
    let Err(refused) = registry.lock() else {
        panic!("a 0644 lock is refused");
    };
    assert_eq!(refused.kind(), io::ErrorKind::PermissionDenied, "{refused}");
    assert_eq!(entry_snapshot(&lock), lock_before);

    // An existing permissive key directory is refused, not repaired.
    let loose = root.join("t-loose");
    fs::create_dir(&loose).expect("loose key");
    fs::set_permissions(&loose, fs::Permissions::from_mode(0o775)).expect("chmod key");
    let refused = registry
        .ensure_key("t-loose", Path::new("/loose/.git"))
        .expect_err("a 0775 key directory is refused");
    assert_eq!(refused.kind(), io::ErrorKind::PermissionDenied, "{refused}");
    assert_eq!(
        fs::symlink_metadata(&loose).expect("key").mode() & 0o7777,
        0o775
    );
    assert_eq!(fs::read_dir(&loose).expect("key entries").count(), 0);
}

/// Whether thread `tid` of this process is asleep inside `flock`. The kernel
/// reports a syscall number only for a task that is blocked in a syscall; a
/// running or runnable task (for example one executing a signal handler)
/// reads as `running`.
fn blocked_in_flock(tid: libc::pid_t) -> bool {
    fs::read_to_string(format!("/proc/self/task/{tid}/syscall"))
        .ok()
        .and_then(|text| text.split_whitespace().next()?.parse::<libc::c_long>().ok())
        == Some(libc::SYS_flock)
}

/// Restores the saved SIGUSR2 disposition, also when the test panics.
struct RestoreSigusr2(libc::sigaction);

impl Drop for RestoreSigusr2 {
    fn drop(&mut self) {
        // SAFETY: restores a disposition previously returned by sigaction.
        unsafe {
            libc::sigaction(libc::SIGUSR2, &self.0, std::ptr::null_mut());
        }
    }
}

/// Holds the registry lock until taken or dropped. Declared inside the
/// thread scope, so a failing assertion releases the lock while unwinding,
/// before the scope joins the waiter: the waiter is never left blocked.
struct HeldLock(Option<RegistryLock>);

impl HeldLock {
    fn release(&mut self) {
        self.0.take();
    }
}

impl Drop for HeldLock {
    fn drop(&mut self) {
        self.release();
    }
}

/// A handler without `SA_RESTART` running while the registry lock is
/// contended must not fail the lock. The launcher's forwarded parent-death
/// SIGTERM arrives exactly like this; a failed lock panicked the launcher
/// before it finished cleanup, and its reaper finished in its place.
///
/// The gate is deterministic: while this test holds the lock, the waiter can
/// leave `flock` only through the signal. The test sees the waiter asleep in
/// `flock`, signals it, waits for the handler to have run, and then requires
/// the waiter to be asleep in `flock` again with the lock still held. That is
/// positive evidence that the call was interrupted mid-wait and retried.
/// Without the retry the waiter returns `Err(Interrupted)` instead and never
/// re-enters `flock`, so the gate fails before the lock is released.
#[test]
fn registry_lock_survives_a_handled_signal_while_contended() {
    static HANDLED: AtomicU64 = AtomicU64::new(0);
    extern "C" fn count(_signal: libc::c_int) {
        HANDLED.fetch_add(1, Ordering::SeqCst);
    }
    let fixture = fixture();
    let registry = &fixture.registry;
    // A counting SIGUSR2 handler without SA_RESTART, like the launcher's
    // signal forwarders; the previous disposition is restored on every path.
    // SAFETY: zeroed sigaction values are valid in- and out-parameters.
    let _restore = unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        let mut previous: libc::sigaction = std::mem::zeroed();
        action.sa_sigaction = count as *const () as libc::sighandler_t;
        libc::sigemptyset(&mut action.sa_mask);
        assert_eq!(libc::sigaction(libc::SIGUSR2, &action, &mut previous), 0);
        RestoreSigusr2(previous)
    };
    let deadline = Instant::now() + Duration::from_secs(/*secs*/ 10);
    let remaining = || deadline.saturating_duration_since(Instant::now());
    let result = std::thread::scope(|scope| {
        let mut held = HeldLock(Some(registry.lock().expect("hold the registry lock")));
        let (sender, receiver) = std::sync::mpsc::channel();
        let waiter = scope.spawn(move || {
            // SAFETY: pthread_self and gettid have no preconditions.
            let ids = unsafe {
                (
                    libc::pthread_self(),
                    libc::syscall(libc::SYS_gettid) as libc::pid_t,
                )
            };
            sender.send(ids).expect("send waiter ids");
            registry.lock().map(drop)
        });
        let (thread, tid) = receiver
            .recv_timeout(remaining())
            .expect("waiter ids within the bound");
        // Gate 1: the waiter is asleep in the contended flock.
        while !blocked_in_flock(tid) {
            assert!(
                !remaining().is_zero() && !waiter.is_finished(),
                "the waiter should block in flock"
            );
            std::thread::yield_now();
        }
        let handled_before = HANDLED.load(Ordering::SeqCst);
        // SAFETY: `thread` is alive: it is blocked in flock on a lock held here.
        assert_eq!(unsafe { libc::pthread_kill(thread, libc::SIGUSR2) }, 0);
        // Gate 2: the handler ran on the waiter, which could only have left
        // flock through this signal because the lock is still held.
        while HANDLED.load(Ordering::SeqCst) == handled_before {
            assert!(!remaining().is_zero(), "the handler should run");
            std::thread::yield_now();
        }
        // Gate 3: with the lock still held, the waiter is asleep in flock
        // again (retried) rather than returned (refused).
        while !blocked_in_flock(tid) {
            assert!(
                !waiter.is_finished(),
                "the interrupted waiter returned instead of retrying flock"
            );
            assert!(!remaining().is_zero(), "the waiter should re-enter flock");
            std::thread::yield_now();
        }
        held.release();
        while !waiter.is_finished() {
            assert!(
                !remaining().is_zero(),
                "the waiter should take the released lock"
            );
            std::thread::yield_now();
        }
        waiter.join().expect("waiter thread")
    });
    assert!(
        result.is_ok(),
        "an interrupted contended lock must be retried: {result:?}"
    );
}

// ---------------------------------------------------------------------------
// Keeper launches
// ---------------------------------------------------------------------------

fn register_ids(
    fixture: &Fixture,
    targets: &[SyntheticMountTarget],
    ids: &LaunchIdentity,
) -> Vec<SyntheticTargetRegistration> {
    let _lock = fixture.registry.lock().expect("lock");
    register_synthetic_targets(
        &fixture.registry,
        targets,
        ids,
        /*pid_namespace*/ true,
        &mut CleanupDeadline::none(),
    )
    .into_result()
    .expect("register")
}

fn holder_text(fixture: &Fixture, key: &str, holder: &ProcessToken) -> String {
    String::from_utf8(
        read_private_file(&fixture.registry.key_file(key, &holder_file(holder)))
            .expect("holder record"),
    )
    .expect("utf-8 holder record")
}

fn no_custody() -> LaunchCustody {
    LaunchCustody {
        deadline: CleanupDeadline::none(),
        report: None,
    }
}

#[test]
fn a_keeper_launch_attributes_targets_to_the_keeper_and_the_receipt_to_the_launcher() {
    let fixture = fixture();
    let dot_git = fixture.work.path().join(".git");
    let launcher = terminated_token();
    let keeper = own_token();
    assert_ne!(launcher, keeper);
    let ids = LaunchIdentity::keeper(launcher.clone(), keeper.clone());
    let registrations = register_ids(&fixture, &[dir_target(&dot_git)], &ids);
    let key = registrations[0].key.clone();

    let record = only_record(&fixture, &key);
    assert_eq!(record.creator, keeper, "the keeper creates the target");
    assert!(
        String::from_utf8_lossy(&record.staging).contains(&keeper.encode()),
        "the staging entry is the keeper's"
    );
    assert!(
        fixture
            .registry
            .key_file(&key, &holder_file(&keeper))
            .exists()
    );
    assert!(
        !fixture
            .registry
            .key_file(&key, &holder_file(&launcher))
            .exists(),
        "the launcher never holds a target"
    );
    let receipt = fixture
        .registry
        .launch_receipt(&launcher)
        .expect("read")
        .expect("receipt named by the launcher");
    assert_eq!(receipt.state, CleanupState::Running);
    assert_eq!(receipt.reaper, Some(keeper.clone()));
    assert_eq!(receipt.reaper_join, ReaperJoin::Pending);
    assert!(
        fixture
            .registry
            .launch_receipt(&keeper)
            .expect("read")
            .is_none(),
        "no receipt is named by the keeper"
    );

    // Release is performed for the keeper's hold only.
    let census = {
        let _lock = fixture.registry.lock().expect("lock");
        release_synthetic_targets(
            &fixture.registry,
            &registrations,
            &ids,
            SandboxProof::Gone,
            &mut CleanupDeadline::none(),
        )
        .expect("release")
    };
    assert_eq!(census, vec![(dot_git.clone(), TargetOutcome::Reaped)]);
    assert!(!dot_git.exists());
    assert!(!fixture.registry.at(&key).exists());
}

#[test]
fn the_never_started_statement_round_trips_and_unknown_statements_fail_closed() {
    let holder = HolderRecord {
        role: HolderRole::Join,
        staging: None,
        sandbox: SandboxTokens::default(),
        never_started: true,
    };
    let text = holder.encode();
    assert!(text.contains("sandbox=never-started\n"), "{text}");
    assert_eq!(HolderRecord::decode(text.as_bytes()), Some(holder));
    assert_eq!(
        HolderRecord::decode(b"v=1\nrole=join\npidns=1\nsandbox=joined\n"),
        None,
        "an unknown statement is not understood"
    );
    let without = HolderRecord::decode(b"v=1\nrole=join\npidns=1\n").expect("v1 holder");
    assert!(!without.never_started);
}

#[test]
fn recovery_trusts_the_never_started_statement_of_a_terminated_keeper() {
    let fixture = fixture();
    if !file_handles_supported(&fixture) {
        return;
    }
    let dot_git = fixture.work.path().join(".git");
    let keeper = terminated_token();
    let ids = LaunchIdentity::keeper(own_token(), keeper.clone());
    let registrations = register_ids(&fixture, &[dir_target(&dot_git)], &ids);
    let key = registrations[0].key.clone();

    // Token-less, with an uninspectable namespace scan: unproven.
    assert_eq!(
        recover(
            &fixture,
            &key,
            &mut NamespaceScan::uninspectable_for_tests()
        ),
        Some(TargetOutcome::Incomplete(IncompleteReason::HolderUnproven))
    );
    assert!(dot_git.is_dir());

    {
        let _lock = fixture.registry.lock().expect("lock");
        record_never_started(&fixture.registry, &registrations, &ids).expect("record");
    }
    assert!(holder_text(&fixture, &key, &keeper).contains("sandbox=never-started\n"));
    drop(registrations);

    assert_eq!(
        recover(
            &fixture,
            &key,
            &mut NamespaceScan::uninspectable_for_tests()
        ),
        Some(TargetOutcome::Reaped)
    );
    assert!(!dot_git.exists());
    assert!(!fixture.registry.at(&key).exists());
}

#[test]
fn a_live_keepers_never_started_statement_is_not_acted_on() {
    let fixture = fixture();
    let dot_git = fixture.work.path().join(".git");
    let ids = LaunchIdentity::keeper(terminated_token(), own_token());
    let registrations = register_ids(&fixture, &[dir_target(&dot_git)], &ids);
    let key = registrations[0].key.clone();
    {
        let _lock = fixture.registry.lock().expect("lock");
        record_never_started(&fixture.registry, &registrations, &ids).expect("record");
    }

    assert_eq!(
        recover(
            &fixture,
            &key,
            &mut NamespaceScan::uninspectable_for_tests()
        ),
        None,
        "a live keeper still holds its targets"
    );
    assert!(dot_git.is_dir());
    drop(registrations);
}

#[test]
fn the_keeper_releases_handle_less_targets_by_their_held_descriptors() {
    let fixture = fixture();
    let dot_git = fixture.work.path().join(".git");
    let ids = LaunchIdentity::keeper(terminated_token(), own_token());
    let registrations = register_ids(&fixture, &[dir_target(&dot_git)], &ids);
    // As on overlayfs without `nfs_export`: no kernel file handle recorded.
    strip_file_handles(&fixture, &registrations[0].key);

    let base = fixture._base.path().to_path_buf();
    let finished = with_test_registry_base(&base, || {
        finish_keeper(
            &registrations,
            None,
            &ids,
            SandboxProof::NeverStarted,
            &mut no_custody(),
            ReaperJoin::FinishedByReaper,
            /*never_started*/ false,
        )
    });

    assert_eq!(finished.state, CleanupState::Complete);
    assert_eq!(finished.persist_error, None);
    assert!(!dot_git.exists(), "the held descriptor proves identity");
    let receipt = fixture
        .registry
        .launch_receipt(ids.receipt())
        .expect("read")
        .expect("receipt");
    assert_eq!(receipt.finisher, Some("reaper"));
    assert_eq!(receipt.reaper, Some(ids.holder().clone()));
    assert_eq!(receipt.reaper_join, ReaperJoin::FinishedByReaper);
    assert_eq!(receipt.targets, vec![(dot_git, TargetOutcome::Reaped)]);
}

#[test]
fn with_no_owner_deadline_the_keeper_records_never_started_and_keeps_everything() {
    let fixture = fixture();
    let dot_git = fixture.work.path().join(".git");
    let keeper = own_token();
    let ids = LaunchIdentity::keeper(terminated_token(), keeper.clone());
    let registrations = register_ids(&fixture, &[dir_target(&dot_git)], &ids);
    let key = registrations[0].key.clone();

    let base = fixture._base.path().to_path_buf();
    let finished = with_test_registry_base(&base, || {
        finish_keeper(
            &registrations,
            None,
            &ids,
            SandboxProof::OwnerGone,
            &mut LaunchCustody {
                deadline: owner_gone_deadline(),
                report: None,
            },
            ReaperJoin::FinishedByReaper,
            /*never_started*/ true,
        )
    });

    assert_eq!(finished.state, CleanupState::Incomplete);
    assert_eq!(
        finished.persist_error, None,
        "the lock was free: a trylock persists"
    );
    assert!(dot_git.is_dir(), "nothing is removed without a deadline");
    assert!(holder_text(&fixture, &key, &keeper).contains("sandbox=never-started\n"));
    assert_eq!(only_record(&fixture, &key).creator, keeper);
    let receipt = fixture
        .registry
        .launch_receipt(ids.receipt())
        .expect("read")
        .expect("receipt");
    assert_eq!(
        receipt.targets,
        vec![(
            dot_git,
            TargetOutcome::Incomplete(IncompleteReason::OwnerGoneWithoutDeadline)
        )]
    );
}

#[test]
fn the_keepers_registry_lock_is_never_awaited_past_the_deadline() {
    let fixture = fixture();
    assert!(
        fixture
            .registry
            .lock_within(&mut CleanupDeadline::at_for_tests(0))
            .expect("lock")
            .is_none(),
        "an expired deadline locks nothing"
    );
    assert!(
        fixture
            .registry
            .lock_within(&mut owner_gone_deadline())
            .expect("lock")
            .is_none(),
        "an owner gone without a deadline locks nothing"
    );
    let far = crate::cleanup_custody::monotonic_now_ns() + 60_000_000_000;
    let held = fixture
        .registry
        .lock_within(&mut CleanupDeadline::at_for_tests(far))
        .expect("lock")
        .expect("a free lock is taken");

    // Contended by another open file description: given up at the deadline.
    let started = Instant::now();
    let near = crate::cleanup_custody::monotonic_now_ns() + 200_000_000;
    assert!(
        fixture
            .registry
            .lock_within(&mut CleanupDeadline::at_for_tests(near))
            .expect("lock")
            .is_none()
    );
    let waited = started.elapsed();
    assert!(waited >= Duration::from_millis(150), "{waited:?}");
    assert!(waited < Duration::from_secs(5), "{waited:?}");
    drop(held);
}

/// Runs `body` in a forked, single-threaded child (like the keeper) and
/// returns its exit code.
fn in_forked_child(body: impl FnOnce() -> i32) -> i32 {
    // SAFETY: the child runs `body` and `_exit`s.
    let child = unsafe { libc::fork() };
    assert!(child >= 0, "fork");
    if child == 0 {
        let code = std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)).unwrap_or(101);
        // SAFETY: terminate the forked child without the test's destructors.
        unsafe { libc::_exit(code) }
    }
    let mut status = 0;
    // SAFETY: waiting for our own child.
    assert_eq!(unsafe { libc::waitpid(child, &mut status, 0) }, child);
    assert!(libc::WIFEXITED(status), "child status {status}");
    libc::WEXITSTATUS(status)
}

/// The cap on any single fixture wait. Passing runs take milliseconds.
const CHILD_GATE_WAIT: Duration = Duration::from_secs(10);
/// One shared budget for every fixture wait of a test, from its start: each
/// wait takes the smaller of its cap and what is left of it.
const FIXTURE_BUDGET: Duration = Duration::from_secs(30);
/// The guard's own bound for observing a SIGKILLed child's exit. It is not
/// taken from the budget, so cleanup always gets its chance.
const KILL_OBSERVE_WAIT: Duration = Duration::from_secs(5);

/// `cap`, or what is left of the budget ending at `ends`.
fn within_budget(ends: Instant, cap: Duration) -> Duration {
    cap.min(ends.saturating_duration_since(Instant::now()))
}

/// Points this forked fixture child's standard I/O at `/dev/null`, so it
/// never holds the test harness's output pipes.
fn detach_fixture_stdio() {
    // SAFETY: plain open/dup2/close in a forked child.
    unsafe {
        let null = libc::open(c"/dev/null".as_ptr(), libc::O_RDWR);
        if null < 0 {
            libc::_exit(120);
        }
        for fd in [libc::STDIN_FILENO, libc::STDOUT_FILENO, libc::STDERR_FILENO] {
            if libc::dup2(null, fd) < 0 {
                libc::_exit(121);
            }
        }
        if null > libc::STDERR_FILENO {
            libc::close(null);
        }
    }
}

/// Whether `pidfd` reports its process's exit within `timeout`.
fn pidfd_exits_within(pidfd: BorrowedFd<'_>, timeout: Duration) -> bool {
    let mut poll_fd = libc::pollfd {
        fd: pidfd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    let millis = libc::c_int::try_from(timeout.as_millis()).unwrap_or(libc::c_int::MAX);
    // SAFETY: one valid pollfd.
    unsafe { libc::poll(&mut poll_fd, 1, millis) > 0 }
}

/// Reaps `pid` (our own child) without ever blocking: `true` once reaped.
fn reap_without_blocking(pid: libc::pid_t) -> bool {
    loop {
        let mut status = 0;
        // SAFETY: non-blocking reap of our own child.
        let rc = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
        if rc == pid {
            return true;
        }
        if rc < 0 && io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        return false;
    }
}

/// A gated fixture child, pinned by a pidfd. [`GatedChild::finish`] (and,
/// on early-return paths, `Drop`) releases the gate, waits for the exit
/// within the budget, SIGKILLs the child through its pidfd if it is still
/// alive (reported on stderr), observes the exit within
/// [`KILL_OBSERVE_WAIT`], and reaps it without blocking. Nothing here
/// waits without a bound; an exit that cannot be proven is an error.
struct GatedChild {
    pid: libc::pid_t,
    pidfd: OwnedFd,
    release: Option<OwnedFd>,
    budget_ends: Instant,
    finished: bool,
}

impl GatedChild {
    fn finish(&mut self) -> Result<(), String> {
        self.finished = true;
        drop(self.release.take());
        let mut exited = pidfd_exits_within(
            self.pidfd.as_fd(),
            within_budget(self.budget_ends, CHILD_GATE_WAIT),
        );
        if !exited {
            let sent = pidfd_signal(self.pidfd.as_fd(), libc::SIGKILL);
            eprintln!(
                "fixture cleanup: SIGKILL to gated child {} ({sent:?})",
                self.pid
            );
            exited = pidfd_exits_within(self.pidfd.as_fd(), KILL_OBSERVE_WAIT);
        }
        if !exited {
            return Err(format!(
                "gated child {} did not exit within {KILL_OBSERVE_WAIT:?} of SIGKILL",
                self.pid
            ));
        }
        if !reap_without_blocking(self.pid) {
            return Err(format!(
                "gated child {} exited but was not reaped",
                self.pid
            ));
        }
        Ok(())
    }
}

impl Drop for GatedChild {
    fn drop(&mut self) {
        if !self.finished
            && let Err(err) = self.finish()
        {
            eprintln!("fixture cleanup: {err}");
        }
    }
}

/// Forks a child that detaches its standard I/O and blocks reading a gate
/// only this process can release: deterministic, never a timed sleep.
fn fork_gated_child(budget_ends: Instant) -> Option<GatedChild> {
    let mut gate = [-1; 2];
    // SAFETY: valid two-element array.
    if unsafe { libc::pipe2(gate.as_mut_ptr(), libc::O_CLOEXEC) } < 0 {
        return None;
    }
    // SAFETY: pipe2 returned two new descriptors, owned here.
    let (gate_read, gate_write) =
        unsafe { (OwnedFd::from_raw_fd(gate[0]), OwnedFd::from_raw_fd(gate[1])) };
    // SAFETY: the child only uses async-signal-safe calls and `_exit`s.
    let pid = unsafe { libc::fork() };
    if pid < 0 {
        return None;
    }
    if pid == 0 {
        detach_fixture_stdio();
        // SAFETY: plain close and read on descriptors this child holds; the
        // owned wrappers are never dropped here (the child `_exit`s).
        unsafe {
            libc::close(gate_write.as_raw_fd());
            let mut byte = [0_u8; 1];
            libc::read(gate_read.as_raw_fd(), byte.as_mut_ptr().cast(), 1);
            libc::_exit(0)
        }
    }
    drop(gate_read);
    let Ok(pidfd) = pidfd_open(pid) else {
        // No pidfd: released at once, then reaped by bounded non-blocking
        // polls; SIGKILLed (our own unreaped child, so the pid cannot be
        // reused) if it does not exit in time. Never a blocking wait.
        drop(gate_write);
        let reap_by = Instant::now() + within_budget(budget_ends, CHILD_GATE_WAIT);
        while !reap_without_blocking(pid) {
            if Instant::now() >= reap_by {
                // SAFETY: signals our own unreaped child.
                let sent = unsafe { libc::kill(pid, libc::SIGKILL) };
                eprintln!("fixture cleanup: SIGKILL to gated child {pid} (rc {sent})");
                let killed_by = Instant::now() + KILL_OBSERVE_WAIT;
                while !reap_without_blocking(pid) && Instant::now() < killed_by {
                    std::thread::yield_now();
                }
                break;
            }
            std::thread::yield_now();
        }
        return None;
    };
    Some(GatedChild {
        pid,
        pidfd,
        release: Some(gate_write),
        budget_ends,
        finished: false,
    })
}

#[test]
fn the_keeper_joins_its_reparented_children_only_while_single_threaded() {
    let code = in_forked_child(|| {
        let budget_ends = Instant::now() + FIXTURE_BUDGET;
        // SAFETY: plain prctl in this child.
        if unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) } < 0 {
            return 10;
        }
        // SAFETY: getpid has no preconditions.
        let subreaper = unsafe { libc::getpid() };
        // An intermediate exits at once, orphaning a grandchild that is
        // reparented to this subreaper. The grandchild exits only once it
        // has observed that reparenting (bounded), never on a timer.
        let reparented_by = Instant::now() + within_budget(budget_ends, CHILD_GATE_WAIT);
        // SAFETY: the children only detach stdio, poll getppid and `_exit`.
        let intermediate = unsafe { libc::fork() };
        if intermediate == 0 {
            detach_fixture_stdio();
            if unsafe { libc::fork() } == 0 {
                // SAFETY: getppid has no preconditions.
                while unsafe { libc::getppid() } != subreaper {
                    if Instant::now() >= reparented_by {
                        unsafe { libc::_exit(1) }
                    }
                    std::thread::yield_now();
                }
                unsafe { libc::_exit(0) }
            }
            unsafe { libc::_exit(0) }
        }
        // A deadline far enough for the reparented grandchild, clamped to
        // the fixture's budget.
        let join_bound = within_budget(budget_ends, FIXTURE_BUDGET);
        let far = crate::cleanup_custody::monotonic_now_ns()
            + u64::try_from(join_bound.as_nanos()).unwrap_or(u64::MAX);
        if join_owned_children(&mut CleanupDeadline::at_for_tests(far)) != SandboxProof::Gone {
            return 2;
        }
        // Owner-less: nothing is waited for, so a live child is unproven.
        // The sleeper is gated (it blocks until released) and guarded.
        let Some(mut sleeper) = fork_gated_child(budget_ends) else {
            return 11;
        };
        if join_owned_children(&mut CleanupDeadline::none()) != SandboxProof::Unproven {
            return 3;
        }
        // Another thread: ECHILD is no longer trusted. The thread signals
        // that it runs, and blocks until released; it is joined below.
        let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let thread = std::thread::spawn(move || {
            let _ = started_tx.send(());
            let _ = release_rx.recv();
        });
        if started_rx
            .recv_timeout(within_budget(budget_ends, CHILD_GATE_WAIT))
            .is_err()
        {
            drop(release_tx);
            let _ = thread.join();
            return 12;
        }
        let multithreaded =
            join_owned_children(&mut CleanupDeadline::at_for_tests(far)) == SandboxProof::Unproven;
        // Released: the thread's `recv` returns at once, so the join is not
        // a wait on anything else.
        drop(release_tx);
        if thread.join().is_err() {
            return 13;
        }
        if !multithreaded {
            return 4;
        }
        // Released, observed to exit within its bounds, and reaped.
        if let Err(err) = sleeper.finish() {
            eprintln!("fixture cleanup: {err}");
            return 6;
        }
        drop(sleeper);
        // No child of this fixture is left (every other one was reaped by
        // the join above).
        let mut status = 0;
        // SAFETY: non-blocking probe for any remaining child.
        if unsafe { libc::waitpid(-1, &mut status, libc::WNOHANG) } != -1
            || io::Error::last_os_error().raw_os_error() != Some(libc::ECHILD)
        {
            return 5;
        }
        0
    });
    assert_eq!(code, 0);
}

// ---------------------------------------------------------------------------
// rev2: bounded registration and bookkeeping
// ---------------------------------------------------------------------------

fn report_pipe() -> (File, OwnedFd) {
    let mut fds = [-1; 2];
    // SAFETY: valid two-element array.
    assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
    // SAFETY: pipe2 returned two new owned descriptors.
    unsafe { (File::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) }
}

/// The report through EOF, polling before each single read, all within
/// 10 s; at the deadline it fails with what arrived so far.
fn read_report(mut report: File) -> String {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut bytes = Vec::new();
    loop {
        let mut poll_fd = libc::pollfd {
            fd: report.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let millis = deadline
            .saturating_duration_since(std::time::Instant::now())
            .as_millis();
        // SAFETY: one valid pollfd.
        let ready = unsafe {
            libc::poll(
                &mut poll_fd,
                1,
                libc::c_int::try_from(millis).unwrap_or(libc::c_int::MAX),
            )
        };
        assert!(
            ready > 0,
            "the report did not reach EOF in time; so far: {}",
            String::from_utf8_lossy(&bytes)
        );
        let mut chunk = [0_u8; 4096];
        let read = report.read(&mut chunk).expect("report");
        if read == 0 {
            return String::from_utf8(bytes).expect("UTF-8 report");
        }
        bytes.extend_from_slice(&chunk[..read]);
    }
}

fn far_deadline() -> CleanupDeadline {
    CleanupDeadline::at_for_tests(crate::cleanup_custody::monotonic_now_ns() + 60_000_000_000)
}

#[test]
fn a_contended_lock_at_expiry_blocks_nothing_and_cleans_nothing() {
    let fixture = fixture();
    let dot_git = fixture.work.path().join(".git");
    let launcher = terminated_token();
    let ids = LaunchIdentity::keeper(launcher.clone(), own_token());
    let registrations = register_ids(&fixture, &[dir_target(&dot_git)], &ids);
    // Another launch holds the registry lock. A blocking wait would never
    // return; a trylock fails at once.
    let other = fixture.registry.lock().expect("lock");
    let (report, report_write) = report_pipe();
    let base = fixture._base.path().to_path_buf();
    let finished = with_test_registry_base(&base, || {
        finish_keeper(
            &registrations,
            None,
            &ids,
            SandboxProof::Gone,
            &mut LaunchCustody {
                deadline: CleanupDeadline::at_for_tests(0),
                report: Some(report_write),
            },
            ReaperJoin::FinishedByReaper,
            /*never_started*/ false,
        )
    });
    drop(other);

    assert_eq!(finished.state, CleanupState::TimedOut);
    assert!(finished.persist_error.is_some());
    assert!(dot_git.is_dir(), "zero cleanup after the deadline");
    let receipt = fixture
        .registry
        .launch_receipt(&launcher)
        .expect("read")
        .expect("receipt");
    assert_eq!(
        receipt.state,
        CleanupState::Running,
        "the registry record is preserved, not rewritten"
    );
    let report = read_report(report);
    assert!(report.contains("state=timed-out\n"), "{report}");
    assert!(report.contains("target=timed-out "), "{report}");
    assert!(report.contains("persist=failed\n"), "{report}");
}

#[test]
fn bookkeeping_after_a_stop_never_waits_and_never_cleans() {
    for deadline in [owner_gone_deadline(), CleanupDeadline::none()] {
        let fixture = fixture();
        let dot_git = fixture.work.path().join(".git");
        let keeper = own_token();
        let ids = LaunchIdentity::keeper(terminated_token(), keeper.clone());
        let registrations = register_ids(&fixture, &[dir_target(&dot_git)], &ids);
        let key = registrations[0].key.clone();
        let other = fixture.registry.lock().expect("lock");
        let (report, report_write) = report_pipe();
        let base = fixture._base.path().to_path_buf();
        let finished = with_test_registry_base(&base, || {
            finish_keeper(
                &registrations,
                None,
                &ids,
                SandboxProof::OwnerGone,
                &mut LaunchCustody {
                    deadline,
                    report: Some(report_write),
                },
                ReaperJoin::FinishedByReaper,
                /*never_started*/ true,
            )
        });
        drop(other);

        assert_eq!(finished.state, CleanupState::Incomplete);
        assert!(finished.persist_error.is_some());
        assert!(dot_git.is_dir());
        assert!(
            !holder_text(&fixture, &key, &keeper).contains("sandbox=never-started"),
            "nothing was written without the lock"
        );
        let report = read_report(report);
        assert!(report.contains("state=incomplete\n"), "{report}");
        assert!(
            report.contains("target=incomplete:owner-gone-without-deadline "),
            "{report}"
        );
        assert!(report.contains("persist=failed\n"), "{report}");
    }
}

#[test]
fn a_stop_without_targets_is_never_reported_complete() {
    assert_eq!(
        finished_state(&[], SandboxProof::TimedOut),
        CleanupState::TimedOut
    );
    assert_eq!(
        finished_state(&[], SandboxProof::OwnerGone),
        CleanupState::Incomplete
    );
    assert_eq!(
        finished_state(&[], SandboxProof::NeverStarted),
        CleanupState::Complete
    );

    let fixture = fixture();
    let ids = LaunchIdentity::keeper(terminated_token(), own_token());
    let base = fixture._base.path().to_path_buf();
    let finished = with_test_registry_base(&base, || {
        finish_keeper(
            &[],
            Some(Vec::new()),
            &ids,
            SandboxProof::TimedOut,
            &mut LaunchCustody {
                deadline: CleanupDeadline::at_for_tests(0),
                report: None,
            },
            ReaperJoin::ExitedAtDeadline,
            /*never_started*/ false,
        )
    });
    assert_eq!(finished.state, CleanupState::TimedOut);
    let receipt = fixture
        .registry
        .launch_receipt(ids.receipt())
        .expect("read")
        .expect("receipt");
    assert_eq!(receipt.state, CleanupState::TimedOut);
}

#[test]
fn a_failed_registration_retracts_in_time_and_keeps_its_partial_census() {
    let fixture = fixture();
    let dot_git = fixture.work.path().join(".git");
    let dot_codex = fixture.work.path().join(".codex");
    std::os::unix::fs::symlink(fixture.work.path(), &dot_codex).expect("symlink");
    let ids = LaunchIdentity::keeper(terminated_token(), own_token());

    let registered = {
        let _lock = fixture.registry.lock().expect("lock");
        register_synthetic_targets(
            &fixture.registry,
            &[dir_target(&dot_git), dir_target(&dot_codex)],
            &ids,
            /*pid_namespace*/ true,
            &mut far_deadline(),
        )
    };

    assert_eq!(registered.registrations.len(), 1);
    assert!(
        registered.registrations[0].is_registry_owned(),
        "the partial registration keeps its held descriptor"
    );
    let failure = registered.failure.expect("failure");
    assert!(matches!(failure.cause, RegistrationCause::Failed(_)));
    assert_eq!(
        failure.census,
        vec![
            (dot_git.clone(), TargetOutcome::Reaped),
            (dot_codex.clone(), TargetOutcome::HostOwned),
        ]
    );
    assert!(!dot_git.exists(), "retracted while custody ran");
    assert!(dot_codex.is_symlink(), "the foreign object is untouched");
}

#[test]
fn a_registration_stopped_part_way_cleans_nothing_and_keeps_its_descriptors() {
    let fixture = fixture();
    let dot_git = fixture.work.path().join(".git");
    let dot_codex = fixture.work.path().join(".codex");
    let ids = LaunchIdentity::keeper(terminated_token(), own_token());
    // The owner's channel closes, with no deadline ever issued, exactly
    // between the two targets.
    let mut fds = [-1; 2];
    // SAFETY: valid two-element array.
    assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
    // SAFETY: pipe2 returned two new owned descriptors.
    let (channel, close) = unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
    let mut deadline = CleanupDeadline::new(None, Some(channel)).expect("deadline");
    let mut close = Some(close);

    let registered = with_registration_hook(
        move |index| {
            if index == 1 {
                drop(close.take());
            }
        },
        || {
            let _lock = fixture.registry.lock().expect("lock");
            register_synthetic_targets(
                &fixture.registry,
                &[dir_target(&dot_git), dir_target(&dot_codex)],
                &ids,
                /*pid_namespace*/ true,
                &mut deadline,
            )
        },
    );

    assert_eq!(registered.registrations.len(), 1);
    assert!(registered.registrations[0].is_registry_owned());
    let failure = registered.failure.expect("failure");
    assert!(matches!(
        failure.cause,
        RegistrationCause::Stopped(StopReason::OwnerGone)
    ));
    assert_eq!(
        failure.census,
        vec![(
            dot_git.clone(),
            TargetOutcome::Incomplete(IncompleteReason::OwnerGoneWithoutDeadline)
        )]
    );
    assert!(dot_git.is_dir(), "zero cleanup after the stop");
    assert!(!dot_codex.exists(), "no target operation after the stop");
}

#[test]
fn recovery_does_no_work_past_the_owners_deadline() {
    assert_eq!(
        recovery_budget(&mut CleanupDeadline::at_for_tests(0)),
        Duration::ZERO
    );
    assert_eq!(recovery_budget(&mut far_deadline()), RECOVERY_BUDGET);
    assert_eq!(
        recovery_budget(&mut CleanupDeadline::none()),
        RECOVERY_BUDGET
    );
    let mut near =
        CleanupDeadline::at_for_tests(crate::cleanup_custody::monotonic_now_ns() + 50_000_000);
    assert!(recovery_budget(&mut near) <= Duration::from_millis(50));

    let fixture = fixture();
    if !file_handles_supported(&fixture) {
        return;
    }
    let dot_git = fixture.work.path().join(".git");
    killed_launch_leftover(&fixture, &dot_git);
    let by = own_token();
    {
        let _lock = fixture.registry.lock().expect("lock");
        recover_registry_within(
            &fixture.registry,
            &by,
            &mut CleanupDeadline::at_for_tests(0),
        );
    }
    assert!(
        dot_git.is_dir(),
        "an expired owner deadline defers every key"
    );
    let recovery = fs::read_to_string(fixture.registry.at(&format!(
        "{RECEIPTS_DIR}/{RECOVERY_RECEIPT_PREFIX}{}",
        by.encode()
    )))
    .expect("recovery receipt");
    assert!(recovery.contains("deferred=1\n"), "{recovery}");
}

// ---------------------------------------------------------------------------
// rev2-inc1: the info report is read against the live deadline
// ---------------------------------------------------------------------------

fn info_pipe() -> (OwnedFd, File) {
    let mut fds = [-1; 2];
    // SAFETY: valid two-element array.
    assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
    // SAFETY: pipe2 returned two new owned descriptors.
    unsafe { (OwnedFd::from_raw_fd(fds[0]), File::from_raw_fd(fds[1])) }
}

fn far_instant() -> Instant {
    Instant::now() + Duration::from_secs(3600)
}

#[test]
fn a_tightening_while_the_info_report_is_pending_ends_the_wait_at_once() {
    let (info, _writer) = info_pipe();
    let (channel, tighten) = info_pipe();
    let mut deadline = CleanupDeadline::new(
        Some(crate::cleanup_custody::monotonic_now_ns() + 3_600_000_000_000),
        Some(channel),
    )
    .expect("deadline");
    let mut tighten = Some(tighten);
    let before = info_io_for_tests();
    let started = Instant::now();
    // The tightening arrives while the first poll is in flight.
    let report = with_info_poll_hook(
        move || {
            if let Some(mut tighten) = tighten.take() {
                std::io::Write::write_all(
                    &mut tighten,
                    &crate::cleanup_custody::monotonic_now_ns().to_le_bytes(),
                )
                .expect("tighten");
            }
        },
        || read_info_child_pid(&info, far_instant(), &mut deadline),
    );
    assert_eq!(report, InfoReport::Expired);
    let after = info_io_for_tests();
    assert_eq!(after.0 - before.0, 1, "only the poll the tightening woke");
    assert_eq!(after.1, before.1, "nothing read after expiry");
    assert!(
        started.elapsed() < INFO_DEADLINE / 2,
        "woken, not timed out"
    );
}

#[test]
fn the_owners_release_while_the_info_report_is_pending_ends_the_wait_at_once() {
    let (info, _writer) = info_pipe();
    let (channel, release) = info_pipe();
    let mut deadline = CleanupDeadline::new(None, Some(channel)).expect("deadline");
    let mut release = Some(release);
    let before = info_io_for_tests();
    let started = Instant::now();
    let report = with_info_poll_hook(
        move || drop(release.take()),
        || read_info_child_pid(&info, far_instant(), &mut deadline),
    );
    assert_eq!(report, InfoReport::Expired);
    let after = info_io_for_tests();
    assert_eq!(after.0 - before.0, 1);
    assert_eq!(after.1, before.1);
    assert!(started.elapsed() < INFO_DEADLINE / 2);
}

#[test]
fn a_report_ready_at_or_after_expiry_is_neither_polled_nor_read_nor_pinned() {
    let (info, mut writer) = info_pipe();
    let own = std::process::id();
    std::io::Write::write_all(&mut writer, format!("{{\"child-pid\": {own}}}").as_bytes())
        .expect("report");
    drop(writer);
    let before = info_io_for_tests();

    // Exactly at the deadline (remaining zero), and past it.
    for at in [crate::cleanup_custody::monotonic_now_ns(), 0] {
        let mut deadline = CleanupDeadline::at_for_tests(at);
        assert_eq!(
            read_info_child_pid(&info, far_instant(), &mut deadline),
            InfoReport::Expired
        );
        let mut sandbox = SandboxProcesses::pin_bwrap(own as libc::pid_t, true);
        assert_eq!(
            sandbox.observe_init(own as libc::pid_t, &info, far_instant(), &mut deadline),
            InitObservation::Expired
        );
        assert!(!sandbox.init_pinned());
    }
    // The info cap reached is expiry too.
    assert_eq!(
        read_info_child_pid(&info, Instant::now(), &mut CleanupDeadline::none()),
        InfoReport::Expired
    );
    assert_eq!(info_io_for_tests(), before, "zero polls, reads and lookups");
}

// ---------------------------------------------------------------------------
// rev2-inc4: the launcher finishes a refused keeper's declared receipt
// ---------------------------------------------------------------------------

#[test]
fn a_refused_keepers_declared_receipt_is_finished_as_incomplete() {
    let fixture = fixture();
    let launcher = terminated_token();
    let keeper = own_token();
    let ids = LaunchIdentity::keeper(launcher.clone(), keeper.clone());
    {
        let _lock = fixture.registry.lock().expect("lock");
        declare_keeper_receipt(&fixture.registry, &ids).expect("declare");
    }
    let (report, report_write) = report_pipe();
    let base = fixture._base.path().to_path_buf();
    let finished = with_test_registry_base(&base, || {
        finish_refused_keeper(
            &ids,
            &mut LaunchCustody {
                deadline: far_deadline(),
                report: Some(report_write),
            },
        )
    });

    assert_eq!(finished.state, CleanupState::Incomplete);
    assert_eq!(finished.persist_error, None);
    let receipt = fixture
        .registry
        .launch_receipt(&launcher)
        .expect("read")
        .expect("receipt");
    assert_eq!(
        receipt.state,
        CleanupState::Incomplete,
        "never left running"
    );
    assert_eq!(receipt.finisher, Some("launcher"));
    assert_eq!(receipt.reaper, Some(keeper));
    assert_eq!(receipt.targets, Vec::new());
    let report = read_report(report);
    assert!(report.contains("state=incomplete\n"), "{report}");
    assert!(report.contains("keeper=refused\n"), "{report}");
    assert!(!report.contains("persist=failed"), "{report}");
}

#[test]
fn a_refusal_with_the_lock_busy_after_expiry_never_waits_and_tells_the_owner() {
    let fixture = fixture();
    let launcher = terminated_token();
    let ids = LaunchIdentity::keeper(launcher.clone(), own_token());
    {
        let _lock = fixture.registry.lock().expect("lock");
        declare_keeper_receipt(&fixture.registry, &ids).expect("declare");
    }
    // Another launch holds the lock and the deadline has passed: a blocking
    // wait would never return.
    let other = fixture.registry.lock().expect("lock");
    let (report, report_write) = report_pipe();
    let base = fixture._base.path().to_path_buf();
    let finished = with_test_registry_base(&base, || {
        finish_refused_keeper(
            &ids,
            &mut LaunchCustody {
                deadline: CleanupDeadline::at_for_tests(0),
                report: Some(report_write),
            },
        )
    });
    drop(other);

    assert_eq!(finished.state, CleanupState::Incomplete);
    assert!(finished.persist_error.is_some());
    let receipt = fixture
        .registry
        .launch_receipt(&launcher)
        .expect("read")
        .expect("receipt");
    assert_eq!(
        receipt.state,
        CleanupState::Running,
        "the registry record is preserved, not rewritten"
    );
    let report = read_report(report);
    assert!(report.contains("state=incomplete\n"), "{report}");
    assert!(report.contains("keeper=refused\n"), "{report}");
    assert!(report.contains("persist=failed\n"), "{report}");
}

// ---------------------------------------------------------------------------
// rev2-inc8: the owner closing between target releases
// ---------------------------------------------------------------------------

#[test]
fn an_owner_closing_between_releases_leaves_a_reaped_prefix_and_an_owner_gone_rest() {
    let fixture = fixture();
    let dot_git = fixture.work.path().join(".git");
    let dot_codex = fixture.work.path().join(".codex");
    let ids = LaunchIdentity::keeper(terminated_token(), own_token());
    // Released in reverse registration order: `.codex`, then `.git`.
    let registrations = register_ids(
        &fixture,
        &[dir_target(&dot_git), dir_target(&dot_codex)],
        &ids,
    );
    // Owned, no deadline ever issued; the test holds the owner's only writer.
    let mut fds = [-1; 2];
    // SAFETY: valid two-element array.
    assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
    // SAFETY: pipe2 returned two new owned descriptors.
    let (channel, writer) = unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
    let mut deadline = CleanupDeadline::new(None, Some(channel)).expect("deadline");
    let mut writer = Some(writer);

    // The owner goes away exactly between the first and second release.
    let census = with_release_hook(
        move |position| {
            if position == 1 {
                drop(writer.take());
            }
        },
        || {
            let _lock = fixture.registry.lock().expect("lock");
            release_synthetic_targets(
                &fixture.registry,
                &registrations,
                &ids,
                SandboxProof::Gone,
                &mut deadline,
            )
            .expect("release")
        },
    );

    // Truthful per target: the release made while the owner lived stands;
    // nothing is released after the owner is gone.
    assert_eq!(
        census,
        vec![
            (
                dot_git.clone(),
                TargetOutcome::Incomplete(IncompleteReason::OwnerGoneWithoutDeadline)
            ),
            (dot_codex.clone(), TargetOutcome::Reaped),
        ]
    );
    assert_eq!(CleanupState::of(&census), CleanupState::Incomplete);
    assert!(!dot_codex.exists(), "released while the owner lived");
    assert!(dot_git.is_dir(), "kept after the owner is gone");
}

#[test]
fn cleanup_synthetic_mount_targets_preserves_real_pre_existing_empty_file() {
    let fixture = fixture();
    let file = fixture.work.path().join(".git");
    fs::write(&file, "").expect("pre-existing empty metadata file");
    let before = fs::symlink_metadata(&file).expect("metadata");
    let target = SyntheticMountTarget::existing_empty_file(&file, &before);
    let owner = own_token();
    let first = register(&fixture, std::slice::from_ref(&target), &owner);
    let second = register(&fixture, &[target], &owner);
    for registrations in [&first, &second] {
        let outcomes = release(
            &fixture,
            registrations,
            &owner,
            SandboxProof::NeverStarted,
            &mut NamespaceScan::default(),
        );
        assert!(
            outcomes
                .iter()
                .all(|(_, outcome)| matches!(outcome, TargetOutcome::HostOwned))
        );
    }
    let after = fs::symlink_metadata(&file).expect("file is preserved");
    assert_eq!((before.dev(), before.ino()), (after.dev(), after.ino()));
    assert_eq!(fs::read(&file).expect("read preserved file"), b"");
}

/// Observe a real descriptor, not only bookkeeping. The pin must retain the
/// unlinked inode even after all registration-local descriptors have dropped.
fn has_open_inode(dev: u64, ino: u64) -> bool {
    fs::read_dir("/proc/self/fd")
        .expect("fd table")
        .flatten()
        .filter_map(|entry| fs::metadata(entry.path()).ok())
        .any(|metadata| metadata.dev() == dev && metadata.ino() == ino)
}

#[test]
fn creation_pin_survives_dropped_registrations_and_refuses_exec() {
    assert_eq!(
        in_forked_child(|| {
            let fixture = fixture();
            let target = fixture.work.path().join(".git");
            let launch = own_token();
            let registrations = register(&fixture, &[dir_target(&target)], &launch);
            let metadata = fs::metadata(&target).expect("created target");
            fs::remove_dir(&target).expect("simulate external unlink");
            drop(registrations);
            assert!(
                has_open_inode(metadata.dev(), metadata.ino()),
                "creator still pins the unlinked inode after registration drop"
            );
            assert!(
                ensure_no_creation_pins().is_err(),
                "exec must not invalidate a live creation record's pin"
            );
            // A forked child has a different creator identity. Its exec cannot
            // invalidate this parent's descriptor and must remain available.
            assert_eq!(
                in_forked_child(|| if ensure_no_creation_pins().is_ok() {
                    0
                } else {
                    1
                }),
                0
            );
            0
        }),
        0
    );
}

#[test]
fn creation_pin_survives_registration_unwind() {
    assert_eq!(
        in_forked_child(|| {
            let fixture = fixture();
            let target = fixture.work.path().join(".git");
            let mut identity = None;
            let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let registrations = register(&fixture, &[dir_target(&target)], &own_token());
                let metadata = fs::metadata(&target).expect("created target");
                identity = Some((metadata.dev(), metadata.ino()));
                fs::remove_dir(&target).expect("unlink target");
                assert!(!registrations.is_empty());
                panic!("keeper failure after creation");
            }));
            assert!(panic.is_err());
            let (dev, ino) = identity.expect("registered before panic");
            assert!(
                has_open_inode(dev, ino),
                "unwind must not release the creation pin"
            );
            assert!(ensure_no_creation_pins().is_err());
            0
        }),
        0
    );
}

#[test]
fn successful_retirement_releases_creation_pin_and_exec_guard() {
    assert_eq!(
        in_forked_child(|| {
            let fixture = fixture();
            let target = fixture.work.path().join(".git");
            let launch = own_token();
            let registrations = register(&fixture, &[dir_target(&target)], &launch);
            let metadata = fs::metadata(&target).expect("created target");
            assert!(ensure_no_creation_pins().is_err());
            assert_eq!(
                release(
                    &fixture,
                    &registrations,
                    &launch,
                    SandboxProof::Gone,
                    &mut NamespaceScan::default()
                ),
                vec![(target, TargetOutcome::Reaped)]
            );
            drop(registrations);
            assert!(!has_open_inode(metadata.dev(), metadata.ino()));
            assert!(ensure_no_creation_pins().is_ok());
            0
        }),
        0
    );
}

#[test]
fn unreaped_zombie_cannot_prove_handle_less_creation() {
    let fixture = fixture();
    let target = fixture.work.path().join(".git");
    let registrations = register(&fixture, &[dir_target(&target)], &own_token());
    let key = &registrations[0].key;
    let mut child =
        fork_gated_child(Instant::now() + FIXTURE_BUDGET).expect("pidfd-backed child required");
    let token = ProcessToken::for_pid(child.pid).expect("live child token");
    let identity =
        identity_of(registrations[0].held.as_ref().expect("held").as_fd()).expect("identity");
    {
        let _lock = fixture.registry.lock().expect("lock");
        for (name, record) in fixture.registry.records(key).expect("records") {
            let mut record = record.expect("record");
            record.identity.handle = None;
            record.creator = token.clone();
            fixture
                .registry
                .write_key_file(key, &name, record.encode().as_bytes())
                .expect("bind zombie creator");
        }
    }
    drop(child.release.take());
    assert!(
        pidfd_exits_within(child.pidfd.as_fd(), CHILD_GATE_WAIT),
        "child exits without reaping"
    );
    assert!(matches!(
        read_process_stat(child.pid).expect("unreaped stat").state,
        b'Z' | b'X' | b'x'
    ));
    assert_eq!(liveness(&token), Liveness::Terminated);
    assert!(
        !fixture
            .registry
            .proves_created(key, SyntheticMountTargetKind::EmptyDirectory, &identity)
            .expect("proof")
    );
    child.finish().expect("reap child");
}
