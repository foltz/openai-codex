//! Host-side lifecycle of synthetic bubblewrap mount targets.
//!
//! A read-only carveout for a missing path under a writable root (for example
//! `.git`, `.agents`, `.codex` or `.aws`) needs a host mount point. bubblewrap
//! would create a missing one itself (`ensure_dir`/`ensure_file`), leaving no
//! evidence of who created it. Instead the launcher installs every missing
//! target before bubblewrap starts, and publishes exact creation evidence in a
//! trusted per-uid registry first:
//!
//! 1. record intent (a holder file naming a staging entry);
//! 2. create that staging entry exclusively, as a sibling of the target (same
//!    directory, so the same filesystem: no `EXDEV`; in a sticky directory
//!    only the owner can rename or remove it);
//! 3. open it with `O_PATH|O_NOFOLLOW` (kept open for the whole launch) and
//!    record its identity: device, inode and, where the filesystem has one,
//!    the kernel file handle (inode plus generation);
//! 4. install it with `renameat2(RENAME_NOREPLACE)`. A target that appeared
//!    meanwhile is never adopted or replaced, and filesystems without
//!    no-replace renames fail closed.
//!
//! Cleanup custody and receipts:
//!
//! - Every launch with targets has a launch receipt (`receipts/launch.<token>`)
//!   written before bubblewrap starts (`running`) and finished with a
//!   per-target census by exactly one finisher: the launcher, or its reaper
//!   when the launcher was killed. A finished receipt is never rewritten by
//!   recovery.
//! - Every wait and every removal is bounded by the owner's cleanup deadline
//!   (`crate::cleanup_custody`): the original retirement deadline passed by
//!   the process that spawned the launcher, tightened over an inherited
//!   channel. After it passes nothing is removed and the outcome is a sticky
//!   `timed-out`.
//! - The reaper exists only when an owner bounds the launch. It is the
//!   launcher's child, recorded in the receipt when it is spawned, and exits
//!   at the deadline at the latest. On a normal exit the launcher cancels and
//!   reaps it and records the join. If the launcher dies first, the reaper
//!   waits event-driven on pidfds for bubblewrap and the sandbox init, finishes
//!   the receipt, and exits.
//! - The final receipt is also written to the owner's report pipe. EOF on
//!   that pipe only proves that its descriptors closed: the owner joins the
//!   launch on terminal evidence alone (the launcher's pidfd, and the pidfd of
//!   every reaper a record names, or the launcher's pre-command declaration
//!   that it created none).
//! - Next-launch recovery is a separate, bounded maintenance action with its
//!   own receipt (`receipts/recovery.<token>`), acting only on records whose
//!   creator is proven terminated.
//!
//! Identity used for removal:
//!
//! - by the launch itself (launcher or its reaper): the open `O_PATH` target
//!   descriptor, which pins the inode, compared by device and inode with what
//!   the verified parent descriptor names now. This works on every
//!   filesystem, including overlayfs without file handles;
//! - by next-launch recovery (no descriptor survives): the recorded file
//!   handle. Where the filesystem has none, recovery preserves the path and
//!   its records as unproven and never claims cleanup.
//!
//! Removal requires exact identity, terminated creator and holders, a sandbox
//! proven gone (PID-namespace init exited, or every same-uid mount namespace
//! inspected), an empty object, and `unlinkat` on the verified parent
//! descriptor. Anything else is preserved, and records that cannot be resolved
//! are kept.
//!
//! Keeper launches (`crate::linux_run_main`): the launcher forks a keeper
//! before anything exists, writes the `running` receipt naming the keeper as
//! the launch's reaper, declares it to the owner, and only then admits it. The
//! keeper, outside the launcher's process group, does everything above that
//! the launcher or its reaper used to do: it creates and holds the targets
//! (it is their creator and holder; the launcher only names the receipt, see
//! [`LaunchIdentity`]), forks bubblewrap, records its evidence, and finishes
//! the receipt, including after the launcher was killed. No separate reaper
//! is forked. A keeper that decides never to fork bubblewrap records that
//! (`sandbox=never-started`) for a later recovery; with no owner deadline it
//! removes nothing itself.

// `libc::stat` field types differ between targets; the casts keep the
// identities `u64`/`u32` everywhere.
#![allow(clippy::unnecessary_cast)]
use std::collections::HashSet;
use std::ffi::CStr;
use std::ffi::CString;
use std::ffi::OsStr;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Read;
use std::io::Write;
use std::os::fd::AsFd;
use std::os::fd::AsRawFd;
use std::os::fd::BorrowedFd;
use std::os::fd::FromRawFd;
use std::os::fd::OwnedFd;
use std::os::fd::RawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

use crate::bwrap::SyntheticMountTarget;
use crate::bwrap::SyntheticMountTargetKind;
use crate::cleanup_custody::CleanupDeadline;
use crate::cleanup_custody::StopReason;
use crate::cleanup_custody::Wait;
use crate::cleanup_custody::write_report;

const REGISTRY_NAME: &str = "codex-sandbox-registry";
const LOCK_NAME: &str = "lock";
const RECEIPTS_DIR: &str = "receipts";
const KEY_PREFIX: &str = "t-";
const PATH_FILE: &str = "path";
const HOLDER_PREFIX: &str = "holder.";
const CREATED_PREFIX: &str = "created.";
const TEMP_PREFIX: &str = "tmp.";
const LAUNCH_RECEIPT_PREFIX: &str = "launch.";
const RECOVERY_RECEIPT_PREFIX: &str = "recovery.";
const STAGING_PREFIX: &str = ".codex-sandbox-staging.";
const MAX_HANDLE_SZ: usize = 128;
const BOOT_ID_PATH: &str = "/proc/sys/kernel/random/boot_id";

/// Bound on reading bubblewrap's `--info-fd` report (written right after
/// bubblewrap clones the sandbox init).
pub(crate) const INFO_DEADLINE: Duration = Duration::from_secs(/*secs*/ 5);
/// Bound on next-launch recovery work per registration.
pub(crate) const RECOVERY_BUDGET: Duration = Duration::from_millis(/*millis*/ 250);
/// Granularity of the keeper's non-blocking registry-lock retries. It is not
/// a budget: the retries stop at the owner's original deadline.
const LOCK_RETRY_INTERVAL: Duration = Duration::from_millis(/*millis*/ 5);
const RECEIPT_RETENTION: Duration = Duration::from_secs(/*secs*/ 7 * 24 * 60 * 60);
const MAX_RECEIPTS: usize = 512;

// ---------------------------------------------------------------------------
// Trusted per-uid registry
// ---------------------------------------------------------------------------

/// A registry directory verified to be owned by the effective uid, mode 0700,
/// and opened without following a final symlink. All record I/O goes through
/// the verified descriptor (`/proc/self/fd/N/...`), never through the path.
#[derive(Debug)]
pub(crate) struct Registry {
    dir: OwnedFd,
    path: PathBuf,
}

/// Where the shared registry lives. It is derived from the effective uid only
/// (never from `TMPDIR`, `XDG_RUNTIME_DIR` or `CODEX_HOME`, which differ
/// between launchers of the same user): `/run/user/<uid>` when it exists,
/// otherwise `<passwd home>/.codex`. A runtime directory that exists but does
/// not verify is refused rather than silently skipped.
fn shared_registry_base() -> io::Result<(PathBuf, bool)> {
    #[cfg(test)]
    if let Some(base) = TEST_REGISTRY_BASE.with(|base| base.borrow().clone()) {
        return Ok((base, false));
    }
    let uid = unsafe { libc::geteuid() };
    let runtime = PathBuf::from(format!("/run/user/{uid}"));
    match fs::symlink_metadata(&runtime) {
        Ok(_) => Ok((runtime, false)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            Ok((passwd_home(uid)?.join(".codex"), true))
        }
        Err(err) => Err(err),
    }
}

#[cfg(test)]
thread_local! {
    /// Unit-test-only registry base of this thread. It lets tests drive the
    /// shared entry points against a fixture instead of the real per-uid
    /// registry; it does not exist outside `cfg(test)` builds.
    static TEST_REGISTRY_BASE: std::cell::RefCell<Option<PathBuf>> =
        const { std::cell::RefCell::new(None) };
}

/// Runs `f` with `base` as this thread's registry base (unit tests only).
/// The base is used as given: it is verified like any other base and never
/// created.
#[cfg(test)]
pub(crate) fn with_test_registry_base<T>(base: &Path, f: impl FnOnce() -> T) -> T {
    struct Restore(Option<PathBuf>);
    impl Drop for Restore {
        fn drop(&mut self) {
            let previous = self.0.take();
            TEST_REGISTRY_BASE.with(|base| *base.borrow_mut() = previous);
        }
    }
    let previous =
        TEST_REGISTRY_BASE.with(|current| current.borrow_mut().replace(base.to_path_buf()));
    let _restore = Restore(previous);
    f()
}

/// Registry path, without creating or verifying anything (argv construction
/// must not have side effects).
pub(crate) fn shared_registry_path() -> io::Result<PathBuf> {
    Ok(shared_registry_base()?.0.join(REGISTRY_NAME))
}

/// Opens (creating on first use) and verifies the shared registry.
pub(crate) fn open_shared_registry() -> io::Result<Registry> {
    let (base, create_base) = shared_registry_base()?;
    Registry::open_trusted(&base, create_base)
}

/// Cached registry path for callers that only need its location.
pub(crate) fn shared_registry_root() -> PathBuf {
    static ROOT: OnceLock<PathBuf> = OnceLock::new();
    ROOT.get_or_init(|| {
        open_shared_registry()
            .map(|registry| registry.path)
            .unwrap_or_else(|err| panic!("refusing to use the sandbox registry: {err}"))
    })
    .clone()
}

/// Runs `f` under the registry lock. The registry is re-opened and re-verified
/// on every call; an untrusted registry refuses the operation.
pub(crate) fn with_shared_registry_lock<T>(f: impl FnOnce(&Registry) -> T) -> T {
    try_with_shared_registry_lock(f)
        .unwrap_or_else(|err| panic!("refusing to use the sandbox registry: {err}"))
}

/// [`with_shared_registry_lock`] that reports an unavailable or untrusted
/// registry (or a failed lock) to the caller instead of panicking.
pub(crate) fn try_with_shared_registry_lock<T>(f: impl FnOnce(&Registry) -> T) -> io::Result<T> {
    let registry = open_shared_registry()?;
    let _lock = registry.lock()?;
    Ok(f(&registry))
}

pub(crate) struct RegistryLock {
    _file: File,
}

impl Registry {
    pub(crate) fn open_trusted(base: &Path, create_base: bool) -> io::Result<Self> {
        let uid = unsafe { libc::geteuid() };
        let base_c = path_cstring(base)?;
        let mut created_base = false;
        if create_base {
            // SAFETY: valid NUL-terminated path.
            if unsafe { libc::mkdir(base_c.as_ptr(), 0o700) } == 0 {
                created_base = true;
            } else {
                let err = io::Error::last_os_error();
                if err.raw_os_error() != Some(libc::EEXIST) {
                    return Err(err);
                }
            }
        }
        let base_fd = openat(libc::AT_FDCWD, &base_c, dir_flags(), 0)?;
        // Only a base this call created is normalized (the umask may have
        // narrowed it); an existing one is verified as found.
        // SAFETY: valid owned descriptor.
        if created_base && unsafe { libc::fchmod(base_fd.as_raw_fd(), 0o700) } < 0 {
            return Err(io::Error::last_os_error());
        }
        verify_directory(base_fd.as_fd(), uid, base, |mode| mode & 0o022 == 0)?;

        let name = CString::new(REGISTRY_NAME).map_err(io::Error::other)?;
        // SAFETY: valid descriptor and NUL-terminated name.
        let created = unsafe { libc::mkdirat(base_fd.as_raw_fd(), name.as_ptr(), 0o700) } == 0;
        if !created {
            let err = io::Error::last_os_error();
            if err.raw_os_error() != Some(libc::EEXIST) {
                return Err(err);
            }
        }
        let dir = openat(base_fd.as_raw_fd(), &name, dir_flags(), 0)?;
        if created {
            // Only a directory this call created is normalized; an existing
            // one must already verify.
            // SAFETY: valid owned descriptor.
            if unsafe { libc::fchmod(dir.as_raw_fd(), 0o700) } < 0 {
                return Err(io::Error::last_os_error());
            }
        }
        let path = base.join(REGISTRY_NAME);
        verify_directory(dir.as_fd(), uid, &path, |mode| mode & 0o7777 == 0o700)?;
        Ok(Self { dir, path })
    }

    pub(crate) fn lock(&self) -> io::Result<RegistryLock> {
        let path = self.at(LOCK_NAME);
        // Created here (exclusively), or an existing lock that already
        // verifies; an existing lock is never repaired.
        let file = match create_new_private_file(&path) {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                open_existing_private_file(&path, /*write*/ false)?
            }
            Err(err) => return Err(err),
        };
        // A contended `flock` fails with `EINTR` whenever a handler without
        // `SA_RESTART` runs, as the launcher's forwarder of its parent-death
        // SIGTERM does. Refusing the registry then would panic a healthy
        // launcher before it finishes cleanup, so retry.
        loop {
            // SAFETY: valid owned descriptor.
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } == 0 {
                return Ok(RegistryLock { _file: file });
            }
            let err = io::Error::last_os_error();
            if err.kind() != io::ErrorKind::Interrupted {
                return Err(err);
            }
        }
    }

    /// [`Registry::lock`] for the keeper, clamped to the owner's original
    /// deadline: `None` (and nothing locked) once cleanup custody has stopped,
    /// checked before the first attempt and again once the lock is held. With
    /// an issued deadline the lock is retried without blocking, so the wait
    /// never extends past it. Without one it waits as [`Registry::lock`]
    /// does: no budget exists to bound it, and nothing is driven meanwhile.
    pub(crate) fn lock_within(
        &self,
        deadline: &mut CleanupDeadline,
    ) -> io::Result<Option<RegistryLock>> {
        if deadline.stop_reason().is_some() {
            return Ok(None);
        }
        if deadline.remaining().is_none() {
            let lock = self.lock()?;
            return Ok(deadline.stop_reason().is_none().then_some(lock));
        }
        let path = self.at(LOCK_NAME);
        let file = match create_new_private_file(&path) {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                open_existing_private_file(&path, /*write*/ false)?
            }
            Err(err) => return Err(err),
        };
        loop {
            // SAFETY: valid owned descriptor.
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
                break;
            }
            let err = io::Error::last_os_error();
            match err.raw_os_error() {
                Some(libc::EINTR) => continue,
                Some(libc::EWOULDBLOCK) => {}
                _ => return Err(err),
            }
            let Some(remaining) = deadline.remaining() else {
                return Ok(None);
            };
            if remaining.is_zero() || deadline.stop_reason().is_some() {
                return Ok(None);
            }
            let pause = remaining.min(LOCK_RETRY_INTERVAL);
            let millis = libc::c_int::try_from(pause.as_millis().max(1)).unwrap_or(1);
            // SAFETY: a poll on no descriptors only sleeps.
            unsafe { libc::poll(std::ptr::null_mut(), 0, millis) };
        }
        // Expiry first: holding the lock is not permission to act late.
        if deadline.stop_reason().is_some() {
            return Ok(None);
        }
        Ok(Some(RegistryLock { _file: file }))
    }

    /// One non-blocking attempt at the registry lock: `None` when another
    /// holder has it. The only way the keeper locks after custody stopped.
    pub(crate) fn try_lock(&self) -> io::Result<Option<RegistryLock>> {
        let path = self.at(LOCK_NAME);
        let file = match create_new_private_file(&path) {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                open_existing_private_file(&path, /*write*/ false)?
            }
            Err(err) => return Err(err),
        };
        loop {
            // SAFETY: valid owned descriptor.
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
                return Ok(Some(RegistryLock { _file: file }));
            }
            let err = io::Error::last_os_error();
            match err.raw_os_error() {
                Some(libc::EINTR) => continue,
                Some(libc::EWOULDBLOCK) => return Ok(None),
                _ => return Err(err),
            }
        }
    }

    fn at(&self, relative: &str) -> PathBuf {
        PathBuf::from(format!("/proc/self/fd/{}", self.dir.as_raw_fd())).join(relative)
    }

    fn key_file(&self, key: &str, name: &str) -> PathBuf {
        self.at(key).join(name)
    }

    /// Creates (if missing) and verifies a private subdirectory: owned by the
    /// effective uid, mode exactly 0700, never reached through a symlink. A
    /// directory this call created is normalized with `fchmod`, so its mode
    /// never depends on the process umask; an existing one must already
    /// verify, otherwise the operation is refused.
    fn private_dir(&self, name: &str) -> io::Result<OwnedFd> {
        let c_name = CString::new(name).map_err(io::Error::other)?;
        // SAFETY: valid descriptor and NUL-terminated name.
        let created = unsafe { libc::mkdirat(self.dir.as_raw_fd(), c_name.as_ptr(), 0o700) } == 0;
        if !created {
            let err = io::Error::last_os_error();
            if err.raw_os_error() != Some(libc::EEXIST) {
                return Err(err);
            }
        }
        let dir = openat(self.dir.as_raw_fd(), &c_name, dir_flags(), 0)?;
        if created {
            // SAFETY: valid owned descriptor.
            if unsafe { libc::fchmod(dir.as_raw_fd(), 0o700) } < 0 {
                return Err(io::Error::last_os_error());
            }
        }
        self.verify_private_dir(&dir, name)?;
        Ok(dir)
    }

    /// Opens and verifies an existing private subdirectory; `None` when it
    /// does not exist.
    fn existing_private_dir(&self, name: &str) -> io::Result<Option<OwnedFd>> {
        let c_name = CString::new(name).map_err(io::Error::other)?;
        let dir = match openat(self.dir.as_raw_fd(), &c_name, dir_flags(), 0) {
            Ok(dir) => dir,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err),
        };
        self.verify_private_dir(&dir, name)?;
        Ok(Some(dir))
    }

    fn verify_private_dir(&self, dir: &OwnedFd, name: &str) -> io::Result<()> {
        let uid = unsafe { libc::geteuid() };
        verify_directory(dir.as_fd(), uid, &self.path.join(name), |mode| {
            mode & 0o7777 == 0o700
        })
    }

    /// Replaces `dir/name` atomically. An existing destination must already
    /// verify (it is refused, never touched, otherwise). The contents go to a
    /// fresh, exclusively created temp file with a unique name, so a leftover
    /// or planted temp entry is never opened or reused.
    fn write_atomic(&self, dir: &str, name: &str, contents: &[u8]) -> io::Result<()> {
        let destination = self.at(dir).join(name);
        match open_existing_private_file(&destination, /*write*/ false) {
            Ok(_) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
        static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let unique = format!(
            "{TEMP_PREFIX}{name}.{}.{}.{}",
            std::process::id(),
            TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let temp = self.at(dir).join(unique);
        let mut file = create_new_private_file(&temp)?;
        let written = file.write_all(contents).and_then(|()| {
            drop(file);
            fs::rename(&temp, &destination)
        });
        if written.is_err() {
            let _ = fs::remove_file(&temp);
        }
        written
    }

    fn write_key_file(&self, key: &str, name: &str, contents: &[u8]) -> io::Result<()> {
        self.write_atomic(key, name, contents)
    }

    /// Removes a record only after it verifies: an untrusted entry is
    /// refused and left in place.
    fn remove_key_file(&self, key: &str, name: &str) -> io::Result<()> {
        match open_existing_private_file(&self.key_file(key, name), /*write*/ false) {
            Ok(_) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err),
        }
        match fs::remove_file(self.key_file(key, name)) {
            Err(err) if err.kind() != io::ErrorKind::NotFound => Err(err),
            _ => Ok(()),
        }
    }

    fn key_entries(&self, key: &str) -> io::Result<Vec<String>> {
        let mut names = Vec::new();
        // A key retired by another holder has no entries.
        let Some(dir) = self.existing_private_dir(key)? else {
            return Ok(names);
        };
        let entries = match fs::read_dir(format!("/proc/self/fd/{}", dir.as_raw_fd())) {
            Ok(entries) => entries,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(names),
            Err(err) => return Err(err),
        };
        for entry in entries {
            let name = entry?.file_name();
            if let Some(name) = name.to_str() {
                names.push(name.to_string());
            }
        }
        Ok(names)
    }

    /// Creates the key directory and binds it to the exact target path. A
    /// different path under the same key is a collision and fails closed.
    fn ensure_key(&self, key: &str, path: &Path) -> io::Result<()> {
        self.private_dir(key)?;
        let bytes = path.as_os_str().as_bytes();
        match create_new_private_file(&self.key_file(key, PATH_FILE)) {
            Ok(mut file) => file.write_all(bytes),
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                if read_private_file(&self.key_file(key, PATH_FILE))? == bytes {
                    Ok(())
                } else {
                    Err(io::Error::other(format!(
                        "sandbox registry key {key} is bound to another path than {}",
                        path.display()
                    )))
                }
            }
            Err(err) => Err(err),
        }
    }

    fn key_path(&self, key: &str) -> io::Result<Option<PathBuf>> {
        if self.existing_private_dir(key)?.is_none() {
            return Ok(None);
        }
        match read_private_file(&self.key_file(key, PATH_FILE)) {
            Ok(bytes) => Ok(Some(PathBuf::from(OsStr::from_bytes(&bytes)))),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Removes the key once nothing but its path binding is left.
    fn remove_key_if_idle(&self, key: &str) -> io::Result<()> {
        let entries = self.key_entries(key)?;
        if entries.iter().any(|name| name != PATH_FILE) {
            return Ok(());
        }
        self.remove_key_file(key, PATH_FILE)?;
        match fs::remove_dir(self.at(key)) {
            Err(err)
                if !matches!(
                    err.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::DirectoryNotEmpty
                ) =>
            {
                Err(err)
            }
            _ => Ok(()),
        }
    }

    fn records(&self, key: &str) -> io::Result<Vec<(String, Option<CreationRecord>)>> {
        let mut records = Vec::new();
        for name in self.key_entries(key)? {
            if !name.starts_with(CREATED_PREFIX) {
                continue;
            }
            let record = match read_private_file(&self.key_file(key, &name)) {
                Ok(bytes) => std::str::from_utf8(&bytes)
                    .ok()
                    .and_then(CreationRecord::decode),
                Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err),
            };
            records.push((name, record));
        }
        Ok(records)
    }

    /// Whether `identity` is an object this registry proves codex created:
    /// an exact match including the file handle, or, on a filesystem without
    /// handles, a device and inode match while the creator is alive (its open
    /// target descriptor pins the inode, so the numbers cannot be reused).
    fn proves_created(
        &self,
        key: &str,
        kind: SyntheticMountTargetKind,
        identity: &ObjectIdentity,
    ) -> io::Result<bool> {
        Ok(self.records(key)?.into_iter().any(|(_, record)| {
            record.is_some_and(|record| {
                record.kind == kind
                    && match &record.identity.handle {
                        Some(_) => record.identity == *identity,
                        None => {
                            (record.identity.dev, record.identity.ino)
                                == (identity.dev, identity.ino)
                                && liveness(&record.creator) == Liveness::Alive
                        }
                    }
            })
        }))
    }

    /// Drops the creation records for an object this process still holds
    /// open (so its device and inode numbers cannot have been reused).
    fn retire_records(&self, key: &str, dev: u64, ino: u64) -> io::Result<()> {
        for (name, record) in self.records(key)? {
            if record.is_some_and(|record| (record.identity.dev, record.identity.ino) == (dev, ino))
            {
                self.remove_key_file(key, &name)?;
            }
        }
        // Records are gone before the independent live-creator pins close.
        retire_creation_pins(self, key, dev, ino);
        Ok(())
    }

    /// Classifies every holder except `own`. Holders whose launcher and
    /// sandbox are proven terminated are removed once nothing depends on them;
    /// anything alive or unprovable blocks removal.
    fn holder_block(
        &self,
        key: &str,
        target: &Path,
        parent: BorrowedFd<'_>,
        own: Option<&ProcessToken>,
        scan: &mut NamespaceScan,
    ) -> io::Result<HolderBlock> {
        let mut block = HolderBlock::None;
        for name in self.key_entries(key)? {
            let Some(encoded) = name.strip_prefix(HOLDER_PREFIX) else {
                continue;
            };
            let Some(token) = ProcessToken::decode(encoded) else {
                block = block.max(HolderBlock::Unproven);
                continue;
            };
            if own == Some(&token) {
                continue;
            }
            match liveness(&token) {
                Liveness::Alive => {
                    block = block.max(HolderBlock::Live);
                    continue;
                }
                Liveness::Uncertain => {
                    block = block.max(HolderBlock::Unproven);
                    continue;
                }
                Liveness::Terminated => {}
            }
            let contents = match read_private_file(&self.key_file(key, &name)) {
                Ok(contents) => contents,
                Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err),
            };
            let Some(holder) = HolderRecord::decode(&contents) else {
                block = block.max(HolderBlock::Unproven);
                continue;
            };
            // A terminated keeper that recorded, before exiting, that it never
            // forked bubblewrap proves that no sandbox of this holder exists.
            if !holder.never_started && !holder.sandbox.proven_gone(target, scan) {
                block = block.max(HolderBlock::Unproven);
                continue;
            }
            let resolved = match holder.role {
                HolderRole::Join => true,
                // Identity was published: the creation record now covers the
                // staging name, including a staging entry never installed.
                HolderRole::Intent
                    if self
                        .key_file(key, &format!("{CREATED_PREFIX}{encoded}"))
                        .exists() =>
                {
                    true
                }
                // Killed before publishing identity: only an absent staging
                // entry resolves it; an unidentified one is preserved (it
                // never carried the target's name, so it does not block).
                HolderRole::Intent => match &holder.staging {
                    Some(staging) => {
                        let staging = CString::new(staging.clone()).map_err(io::Error::other)?;
                        is_absent(fstatat_nofollow(parent, &staging))?
                    }
                    None => false,
                },
            };
            if resolved {
                self.remove_key_file(key, &name)?;
            }
        }
        Ok(block)
    }

    #[cfg(test)]
    fn launch_receipt(&self, launch: &ProcessToken) -> io::Result<Option<LaunchReceipt>> {
        let name = format!("{RECEIPTS_DIR}/{LAUNCH_RECEIPT_PREFIX}{}", launch.encode());
        if self.existing_private_dir(RECEIPTS_DIR)?.is_none() {
            return Ok(None);
        }
        match read_private_file(&self.at(&name)) {
            Ok(bytes) => Ok(std::str::from_utf8(&bytes)
                .ok()
                .and_then(LaunchReceipt::decode)),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err),
        }
    }

    fn write_receipt(&self, name: &str, contents: &str) -> io::Result<()> {
        self.private_dir(RECEIPTS_DIR)?;
        self.write_atomic(RECEIPTS_DIR, name, contents.as_bytes())
    }

    fn write_launch_receipt(&self, receipt: &LaunchReceipt) -> io::Result<()> {
        self.write_receipt(
            &format!("{LAUNCH_RECEIPT_PREFIX}{}", receipt.launch.encode()),
            &receipt.encode(),
        )
    }

    /// Bounded retention: receipts older than the retention period, then the
    /// oldest beyond the count limit. Unfinished launch receipts are kept.
    fn prune_receipts(&self) -> io::Result<()> {
        if self.existing_private_dir(RECEIPTS_DIR)?.is_none() {
            return Ok(());
        }
        let entries = match fs::read_dir(self.at(RECEIPTS_DIR)) {
            Ok(entries) => entries,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err),
        };
        let now = SystemTime::now();
        let mut receipts = Vec::new();
        for entry in entries {
            let entry = entry?;
            let modified = entry.metadata()?.modified()?;
            let path = entry.path();
            // An entry that does not verify is not ours to prune: it is left
            // untouched.
            let Ok(text) = read_private_file(&path) else {
                continue;
            };
            let running = text
                .split(|byte| *byte == b'\n')
                .any(|line| line == b"state=running");
            if running {
                continue;
            }
            if now.duration_since(modified).unwrap_or_default() > RECEIPT_RETENTION {
                let _ = fs::remove_file(&path);
            } else {
                receipts.push((modified, path));
            }
        }
        if receipts.len() > MAX_RECEIPTS {
            receipts.sort();
            for (_, path) in receipts.iter().take(receipts.len() - MAX_RECEIPTS) {
                let _ = fs::remove_file(path);
            }
        }
        Ok(())
    }
}

fn verify_directory(
    fd: BorrowedFd<'_>,
    uid: libc::uid_t,
    path: &Path,
    mode_ok: impl Fn(u32) -> bool,
) -> io::Result<()> {
    let stat = fstat(fd)?;
    let mode = stat.st_mode as u32;
    if mode & libc::S_IFMT as u32 != libc::S_IFDIR as u32 || stat.st_uid != uid || !mode_ok(mode) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "untrusted sandbox registry location {} (owner {}, mode {:o})",
                path.display(),
                stat.st_uid,
                mode & 0o7777
            ),
        ));
    }
    Ok(())
}

/// Verifies a registry file descriptor: a regular file owned by the
/// effective uid with mode exactly 0600.
fn verify_private_file(file: &File, path: &Path) -> io::Result<()> {
    let stat = fstat(file.as_fd())?;
    let mode = stat.st_mode as u32;
    if mode & libc::S_IFMT as u32 != libc::S_IFREG as u32
        || stat.st_uid != unsafe { libc::geteuid() }
        || mode & 0o7777 != 0o600
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "untrusted sandbox registry file {} (owner {}, mode {:o})",
                path.display(),
                stat.st_uid,
                mode & 0o7777
            ),
        ));
    }
    Ok(())
}

/// Creates a registry file exclusively (O_CREAT|O_EXCL, never through a
/// symlink), sets mode 0600 on the descriptor so the umask cannot matter, and
/// verifies it. An existing entry of any kind fails with `AlreadyExists` and
/// is not opened.
fn create_new_private_file(path: &Path) -> io::Result<File> {
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    // SAFETY: valid descriptor of the file this call just created.
    if unsafe { libc::fchmod(file.as_raw_fd(), 0o600) } < 0 {
        return Err(io::Error::last_os_error());
    }
    verify_private_file(&file, path)?;
    Ok(file)
}

/// Opens an existing registry file without creating, truncating or changing
/// it. The entry is first pinned with `O_PATH|O_NOFOLLOW` (which opens no
/// FIFO, device or symlink target) and verified on that descriptor: a regular
/// file owned by the effective uid with mode exactly 0600. Only then is it
/// reopened through the pinned descriptor. A mismatch is refused, never
/// repaired.
fn open_existing_private_file(path: &Path, write: bool) -> io::Result<File> {
    let pinned = File::from(openat(
        libc::AT_FDCWD,
        &path_cstring(path)?,
        libc::O_PATH | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        0,
    )?);
    verify_private_file(&pinned, path)?;
    let file = OpenOptions::new()
        .read(true)
        .write(write)
        .custom_flags(libc::O_CLOEXEC)
        .open(format!("/proc/self/fd/{}", pinned.as_raw_fd()))?;
    verify_private_file(&file, path)?;
    Ok(file)
}

/// Reads a verified existing registry file (see
/// [`open_existing_private_file`]).
fn read_private_file(path: &Path) -> io::Result<Vec<u8>> {
    let mut file = open_existing_private_file(path, /*write*/ false)?;
    let mut contents = Vec::new();
    file.read_to_end(&mut contents)?;
    Ok(contents)
}

fn passwd_home(uid: libc::uid_t) -> io::Result<PathBuf> {
    // SAFETY: zeroed passwd is a valid out-parameter for getpwuid_r.
    let mut passwd: libc::passwd = unsafe { std::mem::zeroed() };
    let mut buffer = vec![0 as libc::c_char; 16 * 1024];
    let mut result: *mut libc::passwd = std::ptr::null_mut();
    // SAFETY: all pointers are valid for the call.
    let rc = unsafe {
        libc::getpwuid_r(
            uid,
            &mut passwd,
            buffer.as_mut_ptr(),
            buffer.len(),
            &mut result,
        )
    };
    if rc != 0 {
        return Err(io::Error::from_raw_os_error(rc));
    }
    if result.is_null() || passwd.pw_dir.is_null() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("no passwd home for uid {uid}"),
        ));
    }
    // SAFETY: getpwuid_r returned a NUL-terminated string inside `buffer`.
    let home = unsafe { CStr::from_ptr(passwd.pw_dir) };
    let home = PathBuf::from(OsStr::from_bytes(home.to_bytes()));
    if !home.is_absolute() {
        return Err(io::Error::other(format!(
            "passwd home for uid {uid} is not absolute"
        )));
    }
    Ok(home)
}

// ---------------------------------------------------------------------------
// Owner identity and liveness
// ---------------------------------------------------------------------------

/// A process identity that survives pid reuse: boot, pid and start time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProcessToken {
    boot_id: String,
    pid: libc::pid_t,
    start_ticks: u64,
}

impl ProcessToken {
    pub(crate) fn current() -> io::Result<Self> {
        Self::for_pid(unsafe { libc::getpid() })
    }

    /// Reads `pid`'s start time; the caller pins the process with a pidfd and
    /// re-checks liveness afterwards when the pid could be recycled.
    pub(crate) fn for_pid(pid: libc::pid_t) -> io::Result<Self> {
        Ok(Self {
            boot_id: read_boot_id()?,
            pid,
            start_ticks: read_process_stat(pid)?.start_ticks,
        })
    }

    #[cfg(test)]
    pub(crate) fn for_tests(boot_id: String, pid: libc::pid_t, start_ticks: u64) -> Self {
        Self {
            boot_id,
            pid,
            start_ticks,
        }
    }

    pub(crate) fn encode(&self) -> String {
        format!("{}.{}.{}", self.boot_id, self.pid, self.start_ticks)
    }

    fn decode(value: &str) -> Option<Self> {
        let mut parts = value.splitn(3, '.');
        let boot_id = parts.next()?;
        if boot_id.is_empty()
            || !boot_id
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
        {
            return None;
        }
        Some(Self {
            boot_id: boot_id.to_string(),
            pid: parts.next()?.parse().ok()?,
            start_ticks: parts.next()?.parse().ok()?,
        })
    }
}

/// The two identities of one launch. They are never interchangeable:
///
/// - `launcher`: names the launch receipt (`receipts/launch.<launcher>`)
///   and nothing else;
/// - `holder`: the process that creates, holds (by `O_PATH` descriptor) and
///   releases the targets. It is the `creator` of every creation record, the
///   name of every holder record and of every staging entry, and the owner of
///   every sandbox process.
///
/// In a keeper launch the holder is the keeper, the launcher's child, which
/// is also the launch's declared reaper. Only the legacy single-process path
/// (the proc preflight, which installs no targets) uses one process for both.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LaunchIdentity {
    launcher: ProcessToken,
    holder: ProcessToken,
    keeper: bool,
}

impl LaunchIdentity {
    /// A keeper launch: `keeper` holds the targets and is the declared reaper.
    pub(crate) fn keeper(launcher: ProcessToken, keeper: ProcessToken) -> Self {
        Self {
            launcher,
            holder: keeper,
            keeper: true,
        }
    }

    /// A launch in which one process both names the receipt and holds the
    /// targets, without any reaper.
    pub(crate) fn single(launcher: ProcessToken) -> Self {
        Self {
            holder: launcher.clone(),
            launcher,
            keeper: false,
        }
    }

    /// The identity naming the launch receipt.
    pub(crate) fn receipt(&self) -> &ProcessToken {
        &self.launcher
    }

    /// The identity that creates and holds the targets.
    pub(crate) fn holder(&self) -> &ProcessToken {
        &self.holder
    }

    /// The reaper declared for this launch: the keeper, if any.
    fn declared_reaper(&self) -> Option<&ProcessToken> {
        self.keeper.then_some(&self.holder)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Liveness {
    Alive,
    Terminated,
    Uncertain,
}

pub(crate) fn read_boot_id() -> io::Result<String> {
    let boot_id = fs::read_to_string(BOOT_ID_PATH)?.trim().to_string();
    if ProcessToken::decode(&format!("{boot_id}.0.0")).is_none() {
        return Err(io::Error::other("unexpected boot id format"));
    }
    Ok(boot_id)
}

struct ProcessStat {
    state: u8,
    parent: libc::pid_t,
    start_ticks: u64,
}

fn read_process_stat(pid: libc::pid_t) -> io::Result<ProcessStat> {
    let stat = fs::read(format!("/proc/{pid}/stat"))?;
    let invalid = || io::Error::other(format!("unparseable /proc/{pid}/stat"));
    // The command name may contain anything, including ')'.
    let close = stat
        .iter()
        .rposition(|byte| *byte == b')')
        .ok_or_else(invalid)?;
    let fields = std::str::from_utf8(&stat[close + 1..]).map_err(|_| invalid())?;
    let fields: Vec<&str> = fields.split_ascii_whitespace().collect();
    // Field 3 (state) is index 0: ppid is field 4, starttime field 22.
    Ok(ProcessStat {
        state: fields
            .first()
            .filter(|state| state.len() == 1)
            .ok_or_else(invalid)?
            .as_bytes()[0],
        parent: fields
            .get(1)
            .and_then(|v| v.parse().ok())
            .ok_or_else(invalid)?,
        start_ticks: fields
            .get(19)
            .and_then(|v| v.parse().ok())
            .ok_or_else(invalid)?,
    })
}

fn pidfd_open(pid: libc::pid_t) -> io::Result<OwnedFd> {
    // SAFETY: pidfd_open takes a pid and flags and returns a new descriptor.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the kernel returned a new owned descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(fd as RawFd) })
}

fn pidfd_signal(fd: BorrowedFd<'_>, signal: libc::c_int) -> io::Result<()> {
    // SAFETY: a null siginfo with flags 0 is the documented kill(2)-like form.
    let rc = unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            fd.as_raw_fd(),
            signal,
            std::ptr::null::<libc::siginfo_t>(),
            0,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Classifies a recorded owner. Only a different boot, a vanished pid, or a
/// pid now held by a process with another start time, or a dead/zombie task
/// prove termination. Zombies have already released their descriptors and
/// namespaces. A pidfd pins the process while its state/start time is read.
pub(crate) fn liveness(token: &ProcessToken) -> Liveness {
    let Ok(boot_id) = read_boot_id() else {
        return Liveness::Uncertain;
    };
    if boot_id != token.boot_id {
        return Liveness::Terminated;
    }
    let pidfd = match pidfd_open(token.pid) {
        Ok(pidfd) => pidfd,
        Err(err) if err.raw_os_error() == Some(libc::ESRCH) => return Liveness::Terminated,
        Err(_) => return Liveness::Uncertain,
    };
    match read_process_stat(token.pid) {
        Ok(stat)
            if stat.start_ticks != token.start_ticks
                || matches!(stat.state, b'Z' | b'X' | b'x') =>
        {
            return Liveness::Terminated;
        }
        Ok(_) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Liveness::Terminated,
        Err(_) => return Liveness::Uncertain,
    }
    match pidfd_signal(pidfd.as_fd(), 0) {
        Ok(()) => Liveness::Alive,
        Err(err) if err.raw_os_error() == Some(libc::ESRCH) => Liveness::Terminated,
        Err(_) => Liveness::Uncertain,
    }
}

// ---------------------------------------------------------------------------
// Object identity and records
// ---------------------------------------------------------------------------

/// Device, inode and kernel file handle. The handle encodes the inode
/// generation on ext4, xfs, btrfs and tmpfs, which changes when an inode
/// number is reused, unlike a coarse-grained birth timestamp.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ObjectIdentity {
    dev: u64,
    ino: u64,
    handle: Option<(i32, Vec<u8>)>,
}

pub(crate) fn identity_of(fd: BorrowedFd<'_>) -> io::Result<ObjectIdentity> {
    let stat = fstat(fd)?;
    Ok(ObjectIdentity {
        dev: stat.st_dev as u64,
        ino: stat.st_ino as u64,
        handle: file_handle(fd)?,
    })
}

/// The kernel file handle of `fd`, or `None` where the filesystem has none
/// (for example overlayfs without `nfs_export`).
fn file_handle(fd: BorrowedFd<'_>) -> io::Result<Option<(i32, Vec<u8>)>> {
    // struct file_handle { u32 handle_bytes; int handle_type; u8 f_handle[]; }
    let mut buffer = vec![0_u32; (8 + MAX_HANDLE_SZ) / 4];
    buffer[0] = MAX_HANDLE_SZ as u32;
    let mut mount_id: libc::c_int = 0;
    // SAFETY: the buffer holds a file_handle header plus MAX_HANDLE_SZ bytes.
    let rc = unsafe {
        libc::syscall(
            libc::SYS_name_to_handle_at,
            fd.as_raw_fd(),
            c"".as_ptr(),
            buffer.as_mut_ptr(),
            &mut mount_id,
            libc::AT_EMPTY_PATH,
        )
    };
    if rc < 0 {
        let err = io::Error::last_os_error();
        return match err.raw_os_error() {
            Some(libc::EOPNOTSUPP) | Some(libc::EOVERFLOW) | Some(libc::ENOSYS) => Ok(None),
            _ => Err(err),
        };
    }
    let length = (buffer[0] as usize).min(MAX_HANDLE_SZ);
    let handle_type = buffer[1] as i32;
    // SAFETY: the kernel filled `length` bytes after the 8-byte header.
    let bytes = unsafe { std::slice::from_raw_parts(buffer.as_ptr().cast::<u8>().add(8), length) };
    Ok(Some((handle_type, bytes.to_vec())))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CreationRecord {
    kind: SyntheticMountTargetKind,
    identity: ObjectIdentity,
    parent_dev: u64,
    parent_ino: u64,
    staging: Vec<u8>,
    creator: ProcessToken,
}

impl CreationRecord {
    fn encode(&self) -> String {
        let kind = match self.kind {
            SyntheticMountTargetKind::EmptyDirectory => "dir",
            SyntheticMountTargetKind::EmptyFile => "file",
        };
        let handle = match &self.identity.handle {
            Some((handle_type, bytes)) => format!("{handle_type}:{}", hex(bytes)),
            None => "none".to_string(),
        };
        format!(
            "v=1\nkind={kind}\ndev={}\nino={}\nhandle={handle}\nparent={}:{}\nstaging={}\ncreator={}\n",
            self.identity.dev,
            self.identity.ino,
            self.parent_dev,
            self.parent_ino,
            hex(&self.staging),
            self.creator.encode()
        )
    }

    fn decode(text: &str) -> Option<Self> {
        let fields = parse_fields(text)?;
        if fields.get("v") != Some(&"1") {
            return None;
        }
        let kind = match *fields.get("kind")? {
            "dir" => SyntheticMountTargetKind::EmptyDirectory,
            "file" => SyntheticMountTargetKind::EmptyFile,
            _ => return None,
        };
        let handle = match *fields.get("handle")? {
            "none" => None,
            value => {
                let (handle_type, bytes) = value.split_once(':')?;
                Some((handle_type.parse().ok()?, unhex(bytes)?))
            }
        };
        let (parent_dev, parent_ino) = fields.get("parent")?.split_once(':')?;
        Some(Self {
            kind,
            identity: ObjectIdentity {
                dev: fields.get("dev")?.parse().ok()?,
                ino: fields.get("ino")?.parse().ok()?,
                handle,
            },
            parent_dev: parent_dev.parse().ok()?,
            parent_ino: parent_ino.parse().ok()?,
            staging: unhex(fields.get("staging")?)?,
            creator: ProcessToken::decode(fields.get("creator")?)?,
        })
    }
}

fn parse_fields(text: &str) -> Option<std::collections::HashMap<&str, &str>> {
    let mut fields = std::collections::HashMap::new();
    for line in text.lines() {
        let (name, value) = line.split_once('=')?;
        fields.insert(name, value);
    }
    Some(fields)
}

/// The processes whose termination proves a launch's sandbox is gone.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct SandboxTokens {
    pid_namespace: bool,
    bwrap: Option<ProcessToken>,
    init: Option<ProcessToken>,
}

impl SandboxTokens {
    #[cfg(test)]
    pub(crate) fn for_tests(init: ProcessToken) -> Self {
        Self {
            pid_namespace: true,
            bwrap: None,
            init: Some(init),
        }
    }

    /// The recorded init's pid (test hooks only).
    #[cfg(test)]
    pub(crate) fn init_pid(&self) -> Option<libc::pid_t> {
        self.init.as_ref().map(|init| init.pid)
    }

    /// With a private PID namespace, the init's exit implies every process in
    /// the sandbox has exited. Otherwise (or without a recorded init) only an
    /// exhaustive inspection of same-uid mount namespaces proves it, and an
    /// uninspectable namespace never counts as absent.
    fn proven_gone(&self, target: &Path, scan: &mut NamespaceScan) -> bool {
        if self.pid_namespace
            && let Some(init) = &self.init
        {
            return liveness(init) == Liveness::Terminated;
        }
        if let Some(bwrap) = &self.bwrap
            && liveness(bwrap) != Liveness::Terminated
        {
            return false;
        }
        scan.proves_unused(target)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HolderRole {
    Intent,
    Join,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct HolderRecord {
    role: HolderRole,
    staging: Option<Vec<u8>>,
    sandbox: SandboxTokens,
    /// Written only by the keeper (the holder), and only after it has
    /// decided never to fork bubblewrap for this launch: no sandbox process
    /// of this holder ever existed. It is evidence for a later, separate
    /// recovery once the holder itself is proven terminated; it never removes
    /// anything by itself. Older decoders ignore the line and see a token-less
    /// holder, so they fail closed.
    never_started: bool,
}

impl HolderRecord {
    fn encode(&self) -> String {
        let mut text = format!(
            "v=1\nrole={}\npidns={}\n",
            match self.role {
                HolderRole::Intent => "intent",
                HolderRole::Join => "join",
            },
            u8::from(self.sandbox.pid_namespace)
        );
        if let Some(staging) = &self.staging {
            text.push_str(&format!("staging={}\n", hex(staging)));
        }
        if let Some(bwrap) = &self.sandbox.bwrap {
            text.push_str(&format!("bwrap={}\n", bwrap.encode()));
        }
        if let Some(init) = &self.sandbox.init {
            text.push_str(&format!("init={}\n", init.encode()));
        }
        if self.never_started {
            text.push_str("sandbox=never-started\n");
        }
        text
    }

    fn decode(contents: &[u8]) -> Option<Self> {
        let fields = parse_fields(std::str::from_utf8(contents).ok()?)?;
        if fields.get("v") != Some(&"1") {
            return None;
        }
        let token = |name: &str| match fields.get(name) {
            Some(value) => ProcessToken::decode(value).map(Some),
            None => Some(None),
        };
        Some(Self {
            role: match *fields.get("role")? {
                "intent" => HolderRole::Intent,
                "join" => HolderRole::Join,
                _ => return None,
            },
            staging: match fields.get("staging") {
                Some(value) => Some(unhex(value)?),
                None => None,
            },
            sandbox: SandboxTokens {
                pid_namespace: match *fields.get("pidns")? {
                    "0" => false,
                    "1" => true,
                    _ => return None,
                },
                bwrap: token("bwrap")?,
                init: token("init")?,
            },
            never_started: match fields.get("sandbox") {
                None => false,
                Some(&"never-started") => true,
                // An unknown statement is not understood: fail closed.
                Some(_) => return None,
            },
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum HolderBlock {
    None,
    Live,
    Unproven,
}

// ---------------------------------------------------------------------------
// Outcomes and receipts
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IncompleteReason {
    SandboxNotProvenGone,
    HolderUnproven,
    Replaced,
    NotEmpty,
    ParentChanged,
    OtherBoot,
    IdentityUnprovable,
    /// The owner died (or dropped custody) without ever issuing a cleanup
    /// deadline: cleanup stopped without a budget, records preserved.
    OwnerGoneWithoutDeadline,
    /// A registry key directory or record failed the ownership and mode
    /// checks: nothing in it is trusted or acted on.
    UntrustedRecord,
}

/// Per-target census entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TargetOutcome {
    /// Installed by this launch; cleanup not finished yet.
    Pending,
    /// The exact object this launch (or the recovered creator) installed was
    /// removed.
    Reaped,
    /// The installed object was already gone.
    Absent,
    /// Never codex's: used as a mount point, untouched.
    HostOwned,
    /// Another live launch holds the object and will release it.
    HandedOff,
    Incomplete(IncompleteReason),
    TimedOut,
}

impl TargetOutcome {
    fn encode(self) -> String {
        match self {
            Self::Pending => "pending".to_string(),
            Self::Reaped => "reaped".to_string(),
            Self::Absent => "absent".to_string(),
            Self::HostOwned => "host-owned".to_string(),
            Self::HandedOff => "handed-off".to_string(),
            Self::TimedOut => "timed-out".to_string(),
            Self::Incomplete(reason) => format!(
                "incomplete:{}",
                match reason {
                    IncompleteReason::SandboxNotProvenGone => "sandbox-not-proven-gone",
                    IncompleteReason::HolderUnproven => "holder-unproven",
                    IncompleteReason::Replaced => "replaced",
                    IncompleteReason::NotEmpty => "not-empty",
                    IncompleteReason::ParentChanged => "parent-changed",
                    IncompleteReason::OtherBoot => "other-boot",
                    IncompleteReason::IdentityUnprovable => "identity-unprovable",
                    IncompleteReason::OwnerGoneWithoutDeadline => "owner-gone-without-deadline",
                    IncompleteReason::UntrustedRecord => "untrusted-record",
                }
            ),
        }
    }

    #[cfg(test)]
    fn decode(value: &str) -> Option<Self> {
        Some(match value {
            "pending" => Self::Pending,
            "reaped" => Self::Reaped,
            "absent" => Self::Absent,
            "host-owned" => Self::HostOwned,
            "handed-off" => Self::HandedOff,
            "timed-out" => Self::TimedOut,
            other => Self::Incomplete(match other.strip_prefix("incomplete:")? {
                "sandbox-not-proven-gone" => IncompleteReason::SandboxNotProvenGone,
                "holder-unproven" => IncompleteReason::HolderUnproven,
                "replaced" => IncompleteReason::Replaced,
                "not-empty" => IncompleteReason::NotEmpty,
                "parent-changed" => IncompleteReason::ParentChanged,
                "other-boot" => IncompleteReason::OtherBoot,
                "identity-unprovable" => IncompleteReason::IdentityUnprovable,
                "owner-gone-without-deadline" => IncompleteReason::OwnerGoneWithoutDeadline,
                "untrusted-record" => IncompleteReason::UntrustedRecord,
                _ => return None,
            }),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CleanupState {
    Running,
    Complete,
    Incomplete,
    TimedOut,
}

impl CleanupState {
    fn of(outcomes: &[(PathBuf, TargetOutcome)]) -> Self {
        if outcomes
            .iter()
            .any(|(_, outcome)| *outcome == TargetOutcome::TimedOut)
        {
            Self::TimedOut
        } else if outcomes.iter().any(|(_, outcome)| {
            matches!(
                outcome,
                TargetOutcome::Incomplete(_) | TargetOutcome::Pending
            )
        }) {
            Self::Incomplete
        } else {
            Self::Complete
        }
    }

    fn encode(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Complete => "complete",
            Self::Incomplete => "incomplete",
            Self::TimedOut => "timed-out",
        }
    }

    #[cfg(test)]
    fn decode(value: &str) -> Option<Self> {
        Some(match value {
            "running" => Self::Running,
            "complete" => Self::Complete,
            "incomplete" => Self::Incomplete,
            "timed-out" => Self::TimedOut,
            _ => return None,
        })
    }
}

/// Who finished a launch receipt, and how the reaper was joined.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReaperJoin {
    /// No reaper was spawned.
    None,
    /// Spawned and not joined yet.
    Pending,
    /// The launcher finished cleanup, then killed and reaped the reaper.
    #[cfg(test)]
    CancelledByLauncher,
    /// The launcher died; the reaper finished cleanup and exited.
    FinishedByReaper,
    /// The launcher finished cleanup but died before joining the reaper.
    #[cfg(test)]
    ExitedAfterLauncherFinished,
    /// The deadline passed while the launcher was alive; the reaper exited
    /// without doing anything and the launcher reaped it.
    ExitedAtDeadline,
    /// The owner died without issuing a deadline while the launcher was
    /// alive; the reaper recorded the unproven state and exited.
    ExitedOwnerGone,
    /// The launcher could not observe the reaper's exit before the deadline.
    #[cfg(test)]
    NotJoinedByDeadline,
}

impl ReaperJoin {
    fn encode(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Pending => "pending",
            #[cfg(test)]
            Self::CancelledByLauncher => "cancelled-by-launcher",
            Self::FinishedByReaper => "finished-by-reaper",
            #[cfg(test)]
            Self::ExitedAfterLauncherFinished => "exited-after-launcher-finished",
            Self::ExitedAtDeadline => "exited-at-deadline",
            Self::ExitedOwnerGone => "exited-owner-gone",
            #[cfg(test)]
            Self::NotJoinedByDeadline => "not-joined-by-deadline",
        }
    }

    #[cfg(test)]
    fn decode(value: &str) -> Option<Self> {
        Some(match value {
            "none" => Self::None,
            "pending" => Self::Pending,
            "cancelled-by-launcher" => Self::CancelledByLauncher,
            "finished-by-reaper" => Self::FinishedByReaper,
            "exited-after-launcher-finished" => Self::ExitedAfterLauncherFinished,
            "exited-at-deadline" => Self::ExitedAtDeadline,
            "exited-owner-gone" => Self::ExitedOwnerGone,
            "not-joined-by-deadline" => Self::NotJoinedByDeadline,
            _ => return None,
        })
    }
}

/// The launch report: one per launch with synthetic targets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LaunchReceipt {
    launch: ProcessToken,
    state: CleanupState,
    finisher: Option<&'static str>,
    reaper: Option<ProcessToken>,
    reaper_join: ReaperJoin,
    targets: Vec<(PathBuf, TargetOutcome)>,
}

impl LaunchReceipt {
    fn encode(&self) -> String {
        let mut text = format!(
            "v=1\nlaunch={}\nstate={}\nfinisher={}\nreaper={}\nreaper_join={}\n",
            self.launch.encode(),
            self.state.encode(),
            self.finisher.unwrap_or("none"),
            self.reaper
                .as_ref()
                .map_or_else(|| "none".to_string(), ProcessToken::encode),
            self.reaper_join.encode()
        );
        for (path, outcome) in &self.targets {
            text.push_str(&format!(
                "target={} {}\n",
                outcome.encode(),
                hex(path.as_os_str().as_bytes())
            ));
        }
        text
    }

    #[cfg(test)]
    fn decode(text: &str) -> Option<Self> {
        let mut targets = Vec::new();
        let mut header = String::new();
        for line in text.lines() {
            match line.strip_prefix("target=") {
                Some(target) => {
                    let (outcome, path) = target.split_once(' ')?;
                    targets.push((
                        PathBuf::from(OsStr::from_bytes(&unhex(path)?)),
                        TargetOutcome::decode(outcome)?,
                    ));
                }
                None => {
                    header.push_str(line);
                    header.push('\n');
                }
            }
        }
        let fields = parse_fields(&header)?;
        if fields.get("v") != Some(&"1") {
            return None;
        }
        Some(Self {
            launch: ProcessToken::decode(fields.get("launch")?)?,
            state: CleanupState::decode(fields.get("state")?)?,
            finisher: match *fields.get("finisher")? {
                "none" => None,
                "launcher" => Some("launcher"),
                "reaper" => Some("reaper"),
                _ => return None,
            },
            reaper: match *fields.get("reaper")? {
                "none" => None,
                value => Some(ProcessToken::decode(value)?),
            },
            reaper_join: ReaperJoin::decode(fields.get("reaper_join")?)?,
            targets,
        })
    }
}

// A handle-less CreationRecord may be trusted by another launch while its
// creator is Alive. Keep an independent O_PATH pin until the record is retired
// or the creator exits, including registration drops, early returns and unwind.
// Nodes are process-lifetime bookkeeping: they are never freed. Only their fd
// is closed after retirement. This avoids a mutex inherited locked across fork
// (the helper forks after registration); child copies never release a parent's
// pins. Helper lifetime and its finite target list bound this storage.
struct CreationPin {
    creator_pid: libc::pid_t,
    registry: PathBuf,
    key: String,
    dev: u64,
    ino: u64,
    fd: AtomicI32,
    next: *mut CreationPin,
}

static CREATION_PINS: AtomicPtr<CreationPin> = AtomicPtr::new(std::ptr::null_mut());

fn pin_creation(
    registry: &Registry,
    key: &str,
    identity: &ObjectIdentity,
    held: BorrowedFd<'_>,
) -> io::Result<()> {
    use std::os::fd::IntoRawFd;
    let fd = held.try_clone_to_owned()?;
    let node = Box::into_raw(Box::new(CreationPin {
        creator_pid: unsafe { libc::getpid() },
        registry: registry.path.clone(),
        key: key.to_string(),
        dev: identity.dev,
        ino: identity.ino,
        fd: AtomicI32::new(fd.into_raw_fd()),
        next: std::ptr::null_mut(),
    }));
    let mut head = CREATION_PINS.load(Ordering::Acquire);
    loop {
        // SAFETY: this unpublished node is owned exclusively by this call.
        unsafe { (*node).next = head };
        match CREATION_PINS.compare_exchange_weak(head, node, Ordering::Release, Ordering::Acquire)
        {
            Ok(_) => return Ok(()),
            Err(actual) => head = actual,
        }
    }
}

fn retire_creation_pins(registry: &Registry, key: &str, dev: u64, ino: u64) {
    let pid = unsafe { libc::getpid() };
    let mut node = CREATION_PINS.load(Ordering::Acquire);
    while !node.is_null() {
        // SAFETY: published nodes are immutable except the atomic fd and are
        // never freed, including after retirement. Acquire pairs publication.
        let pin = unsafe { &*node };
        if pin.creator_pid == pid
            && pin.registry == registry.path
            && pin.key == key
            && pin.dev == dev
            && pin.ino == ino
        {
            let fd = pin.fd.swap(-1, Ordering::AcqRel);
            if fd >= 0 {
                // SAFETY: swap gives this caller exclusive ownership; no retry
                // on EINTR, which could otherwise close a reused descriptor.
                unsafe { libc::close(fd) };
            }
        }
        node = pin.next;
    }
}

/// Exec preserves pid/start time but drops CLOEXEC pins. Refuse that transition
/// while this process still supplies a CreationRecord's live-inode premise.
/// Forked bwrap children own no records of their parent and may exec normally.
pub(crate) fn ensure_no_creation_pins() -> io::Result<()> {
    let pid = unsafe { libc::getpid() };
    let mut node = CREATION_PINS.load(Ordering::Acquire);
    while !node.is_null() {
        // SAFETY: same process-lifetime publication invariant as above.
        let pin = unsafe { &*node };
        if pin.creator_pid == pid && pin.fd.load(Ordering::Acquire) >= 0 {
            return Err(io::Error::other(
                "refusing exec while synthetic target creation records remain pinned",
            ));
        }
        node = pin.next;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct SyntheticTargetRegistration {
    path: PathBuf,
    kind: SyntheticMountTargetKind,
    key: String,
    parent: OwnedFd,
    name: CString,
    /// Open descriptor on an object the registry proves codex created. It pins
    /// the inode, so comparing device and inode numbers against it is exact.
    held: Option<OwnedFd>,
    holder: Option<HolderRecord>,
}

impl SyntheticTargetRegistration {
    pub(crate) fn is_registry_owned(&self) -> bool {
        self.held.is_some()
    }
}

/// Why registration stopped before every target was registered.
#[derive(Debug)]
pub(crate) enum RegistrationCause {
    /// The owner's deadline passed, or the owner is gone without one: no
    /// further target operation, evidence write or retraction was made.
    Stopped(StopReason),
    /// An operation failed while custody still ran; what this launch had
    /// installed was retracted, within the deadline, by its held descriptors.
    Failed(io::Error),
}

impl std::fmt::Display for RegistrationCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stopped(StopReason::DeadlinePassed) => {
                write!(f, "the owner's cleanup deadline passed during registration")
            }
            Self::Stopped(StopReason::OwnerGone) => {
                write!(
                    f,
                    "the owner is gone, without a cleanup deadline, during registration"
                )
            }
            Self::Failed(err) => write!(f, "{err}"),
        }
    }
}

/// A registration that did not complete. Nothing in it is dropped before
/// the launch's bounded finish: `census` covers every target on which this
/// launch made a side effect, and `retained` keeps the staging descriptor of
/// an installation interrupted part-way.
#[derive(Debug)]
pub(crate) struct RegistrationFailure {
    pub(crate) cause: RegistrationCause,
    pub(crate) census: Vec<(PathBuf, TargetOutcome)>,
    pub(crate) retained: Vec<OwnedFd>,
}

impl RegistrationFailure {
    /// The proof the finished receipt's state derives from.
    pub(crate) fn proof(&self) -> SandboxProof {
        match &self.cause {
            RegistrationCause::Stopped(reason) => stopped_proof(*reason),
            RegistrationCause::Failed(_) => SandboxProof::NeverStarted,
        }
    }
}

/// The result of [`register_synthetic_targets`]: every target registered so
/// far (descriptors held), and why registration stopped, if it did.
#[derive(Debug)]
pub(crate) struct Registered {
    pub(crate) registrations: Vec<SyntheticTargetRegistration>,
    pub(crate) failure: Option<RegistrationFailure>,
}

impl Registered {
    /// For the single-process launch and unit tests: the registrations, or
    /// the failure as an error (dropping the partial registration).
    pub(crate) fn into_result(self) -> io::Result<Vec<SyntheticTargetRegistration>> {
        match self.failure {
            None => Ok(self.registrations),
            Some(RegistrationFailure {
                cause: RegistrationCause::Failed(err),
                ..
            }) => Err(err),
            Some(failure) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                failure.cause.to_string(),
            )),
        }
    }
}

/// One target's installation that stopped: why, what it left (if any side
/// effect was made), and its staging descriptor (if one was opened).
struct InstallInterrupted {
    cause: RegistrationCause,
    outcome: Option<TargetOutcome>,
    held: Option<OwnedFd>,
}

impl InstallInterrupted {
    fn stopped(reason: StopReason, after_side_effect: bool, held: Option<OwnedFd>) -> Self {
        Self {
            cause: RegistrationCause::Stopped(reason),
            outcome: after_side_effect.then(|| stopped_outcome(reason)),
            held,
        }
    }

    fn failed(err: io::Error, outcome: Option<TargetOutcome>, held: Option<OwnedFd>) -> Self {
        Self {
            cause: RegistrationCause::Failed(err),
            outcome,
            held,
        }
    }
}

#[cfg(test)]
type RegistrationHook = Box<dyn FnMut(usize)>;

#[cfg(test)]
thread_local! {
    /// Unit-test-only hook run before each target's registration (with its
    /// index), on this thread.
    static REGISTRATION_HOOK: std::cell::RefCell<Option<RegistrationHook>> =
        const { std::cell::RefCell::new(None) };
}

/// Runs `f` with `hook` called before each target's registration (unit
/// tests only).
#[cfg(test)]
pub(crate) fn with_registration_hook<T>(
    hook: impl FnMut(usize) + 'static,
    f: impl FnOnce() -> T,
) -> T {
    REGISTRATION_HOOK.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
    let result = f();
    REGISTRATION_HOOK.with(|slot| *slot.borrow_mut() = None);
    result
}

/// Installs or joins every target, then writes the `running` launch receipt.
/// Runs under the registry lock, after argv construction succeeded and before
/// bubblewrap is forked, so creation evidence is published before any
/// sandboxed command can run.
///
/// Every creation record, holder and staging entry is attributed to
/// `ids.holder()`; the receipt is named by `ids.receipt()` and keeps the
/// launch's declared reaper. The owner's original `deadline` bounds
/// everything: next-launch recovery first (within what is left of it), then
/// every target operation and evidence write, each checked for expiry before
/// it is made, and a retraction after a failure. After expiry nothing more is
/// done: the partial registration and its census are returned, descriptors
/// held, for the launch's bounded finish.
pub(crate) fn register_synthetic_targets(
    registry: &Registry,
    targets: &[SyntheticMountTarget],
    ids: &LaunchIdentity,
    pid_namespace: bool,
    deadline: &mut CleanupDeadline,
) -> Registered {
    if deadline.stop_reason().is_none() {
        recover_registry_within(registry, ids.holder(), deadline);
    }
    let mut registrations: Vec<SyntheticTargetRegistration> = Vec::with_capacity(targets.len());
    for (index, target) in targets.iter().enumerate() {
        #[cfg(test)]
        REGISTRATION_HOOK.with(|slot| {
            if let Some(hook) = slot.borrow_mut().as_mut() {
                hook(index);
            }
        });
        if registrations
            .iter()
            .any(|registration| registration.path == target.path())
        {
            continue;
        }
        // Expiry first, before every target.
        let registered = match deadline.stop_reason() {
            Some(reason) => Err(InstallInterrupted::stopped(
                reason, /*after_side_effect*/ false, None,
            )),
            None => register_target(
                registry,
                target,
                ids.holder(),
                pid_namespace,
                index,
                deadline,
            ),
        };
        match registered {
            Ok(registration) => registrations.push(registration),
            Err(interrupted) => {
                return registration_failed(
                    registry,
                    registrations,
                    ids,
                    deadline,
                    Some(target.path()),
                    interrupted,
                );
            }
        }
    }
    // The `running` receipt is evidence too.
    if let Some(reason) = deadline.stop_reason() {
        return registration_failed(
            registry,
            registrations,
            ids,
            deadline,
            None,
            InstallInterrupted::stopped(reason, /*after_side_effect*/ false, None),
        );
    }
    let reaper = ids.declared_reaper().cloned();
    let reaper_join = if reaper.is_some() {
        ReaperJoin::Pending
    } else {
        ReaperJoin::None
    };
    let written = registry.write_launch_receipt(&LaunchReceipt {
        launch: ids.receipt().clone(),
        state: CleanupState::Running,
        finisher: None,
        reaper,
        reaper_join,
        targets: registrations
            .iter()
            .map(|registration| {
                let outcome = if registration.is_registry_owned() {
                    TargetOutcome::Pending
                } else {
                    TargetOutcome::HostOwned
                };
                (registration.path.clone(), outcome)
            })
            .collect(),
    });
    match written {
        Ok(()) => Registered {
            registrations,
            failure: None,
        },
        Err(err) => registration_failed(
            registry,
            registrations,
            ids,
            deadline,
            None,
            InstallInterrupted::failed(err, None, None),
        ),
    }
}

/// Finishes a registration that stopped: retracts what this launch installed
/// (only while custody runs, by the held descriptors, each removal checked
/// for expiry first), and returns the partial registrations with a census of
/// every target this launch touched. Nothing is dropped here.
fn registration_failed(
    registry: &Registry,
    registrations: Vec<SyntheticTargetRegistration>,
    ids: &LaunchIdentity,
    deadline: &mut CleanupDeadline,
    path: Option<&Path>,
    interrupted: InstallInterrupted,
) -> Registered {
    let InstallInterrupted {
        cause,
        outcome,
        held,
    } = interrupted;
    let proof = match &cause {
        RegistrationCause::Stopped(reason) => stopped_proof(*reason),
        // Nothing has been forked yet: the launch never started.
        RegistrationCause::Failed(_) => SandboxProof::NeverStarted,
    };
    let mut census = release_synthetic_targets(registry, &registrations, ids, proof, deadline)
        .unwrap_or_else(|_| unreleased_census(&registrations, deadline));
    if let (Some(path), Some(outcome)) = (path, outcome) {
        census.push((path.to_path_buf(), outcome));
    }
    Registered {
        registrations,
        failure: Some(RegistrationFailure {
            cause,
            census,
            retained: held.into_iter().collect(),
        }),
    }
}

/// The census of registrations that could not be released: what custody
/// stopped with, or `pending` (not cleaned, records kept).
fn unreleased_census(
    registrations: &[SyntheticTargetRegistration],
    deadline: &mut CleanupDeadline,
) -> Vec<(PathBuf, TargetOutcome)> {
    let stop = deadline.stop_reason();
    registrations
        .iter()
        .map(|registration| {
            let outcome = match (&registration.held, stop) {
                (None, _) => TargetOutcome::HostOwned,
                (Some(_), Some(reason)) => stopped_outcome(reason),
                (Some(_), None) => TargetOutcome::Pending,
            };
            (registration.path.clone(), outcome)
        })
        .collect()
}

/// The holder record's name: always the holder identity (the keeper in a
/// keeper launch), never the launcher that names the receipt.
fn holder_file(holder: &ProcessToken) -> String {
    format!("{HOLDER_PREFIX}{}", holder.encode())
}

fn register_target(
    registry: &Registry,
    target: &SyntheticMountTarget,
    holder_id: &ProcessToken,
    pid_namespace: bool,
    index: usize,
    deadline: &mut CleanupDeadline,
) -> Result<SyntheticTargetRegistration, InstallInterrupted> {
    let failed = |err: io::Error| InstallInterrupted::failed(err, None, None);
    let path = target.path().to_path_buf();
    let kind = target.kind();
    let (parent_path, name) = split_target(&path).map_err(failed)?;
    let parent = open_directory(&parent_path).map_err(failed)?;
    let parent_stat = fstat(parent.as_fd()).map_err(failed)?;
    let key = key_name(&path, parent_stat.st_dev as u64);
    // `recover_registry` already ran (and may have retired this key).
    if let Some(reason) = deadline.stop_reason() {
        return Err(InstallInterrupted::stopped(reason, false, None));
    }
    registry.ensure_key(&key, &path).map_err(failed)?;
    let sandbox = SandboxTokens {
        pid_namespace,
        bwrap: None,
        init: None,
    };

    let existing = match fstatat_nofollow(parent.as_fd(), &name) {
        Ok(stat) => Some(stat),
        Err(err) if err.raw_os_error() == Some(libc::ENOENT) => None,
        Err(err) => return Err(failed(err)),
    };
    let Some(existing) = existing else {
        let (held, holder) = install_target(
            registry,
            &key,
            parent.as_fd(),
            &parent_stat,
            &name,
            kind,
            holder_id,
            sandbox,
            index,
            deadline,
        )
        .map_err(|interrupted| {
            let InstallInterrupted {
                cause,
                outcome,
                held,
            } = interrupted;
            let cause = match cause {
                RegistrationCause::Failed(err) => RegistrationCause::Failed(io::Error::new(
                    err.kind(),
                    format!(
                        "failed to install synthetic mount target {}: {err}",
                        path.display()
                    ),
                )),
                stopped @ RegistrationCause::Stopped(_) => stopped,
            };
            InstallInterrupted {
                cause,
                outcome,
                held,
            }
        })?;
        return Ok(SyntheticTargetRegistration {
            path,
            kind,
            key,
            parent,
            name,
            held: Some(held),
            holder: Some(holder),
        });
    };

    // bubblewrap follows symlinks when it mounts over a destination; refuse
    // anything but the expected kind of object. It is not codex's: untouched.
    if existing.st_mode & libc::S_IFMT != kind_mode(kind) {
        return Err(InstallInterrupted::failed(
            io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "refusing synthetic mount target {}: an unexpected object is present",
                    path.display()
                ),
            ),
            Some(TargetOutcome::HostOwned),
            None,
        ));
    }
    let held = open_entry(parent.as_fd(), &name, kind).map_err(failed)?;
    let identity = identity_of(held.as_fd()).map_err(failed)?;
    if registry
        .proves_created(&key, kind, &identity)
        .map_err(failed)?
    {
        let holder = HolderRecord {
            role: HolderRole::Join,
            staging: None,
            sandbox,
            never_started: false,
        };
        if let Some(reason) = deadline.stop_reason() {
            return Err(InstallInterrupted::stopped(reason, false, Some(held)));
        }
        if let Err(err) =
            registry.write_key_file(&key, &holder_file(holder_id), holder.encode().as_bytes())
        {
            return Err(InstallInterrupted::failed(err, None, Some(held)));
        }
        return Ok(SyntheticTargetRegistration {
            path,
            kind,
            key,
            parent,
            name,
            held: Some(held),
            holder: Some(holder),
        });
    }
    // Host-owned or unprovable: used as a mount point, never removed.
    Ok(SyntheticTargetRegistration {
        path,
        kind,
        key,
        parent,
        name,
        held: None,
        holder: None,
    })
}

/// Installs one missing target in four steps, each checked for expiry first:
/// intent, exclusive staging creation, published identity, no-replace
/// rename. A failure while custody runs retracts this target's own steps;
/// after expiry nothing is retracted and the staging descriptor is returned
/// with the interruption.
#[allow(clippy::too_many_arguments)]
fn install_target(
    registry: &Registry,
    key: &str,
    parent: BorrowedFd<'_>,
    parent_stat: &libc::stat,
    name: &CStr,
    kind: SyntheticMountTargetKind,
    holder_id: &ProcessToken,
    sandbox: SandboxTokens,
    index: usize,
    deadline: &mut CleanupDeadline,
) -> Result<(OwnedFd, HolderRecord), InstallInterrupted> {
    let staging = CString::new(format!("{STAGING_PREFIX}{}.{index}", holder_id.encode()))
        .map_err(|err| InstallInterrupted::failed(io::Error::other(err), None, None))?;
    let holder = HolderRecord {
        role: HolderRole::Intent,
        staging: Some(staging.as_bytes().to_vec()),
        sandbox,
        never_started: false,
    };
    let holder_name = holder_file(holder_id);
    // 1. Intent, naming the staging entry, before anything exists on the host.
    if let Some(reason) = deadline.stop_reason() {
        return Err(InstallInterrupted::stopped(reason, false, None));
    }
    registry
        .write_key_file(key, &holder_name, holder.encode().as_bytes())
        .map_err(|err| InstallInterrupted::failed(err, None, None))?;
    // 2. Exclusive creation: EEXIST means the name is not ours.
    if let Some(reason) = deadline.stop_reason() {
        return Err(InstallInterrupted::stopped(reason, true, None));
    }
    let held = match create_staging(parent, &staging, kind) {
        Ok(held) => held,
        Err(err) => {
            // Retract the intent, only while custody runs.
            if let Some(reason) = deadline.stop_reason() {
                return Err(InstallInterrupted::failed(
                    err,
                    Some(stopped_outcome(reason)),
                    None,
                ));
            }
            let outcome = registry
                .remove_key_file(key, &holder_name)
                .err()
                .map(|_| TargetOutcome::Incomplete(IncompleteReason::HolderUnproven));
            return Err(InstallInterrupted::failed(err, outcome, None));
        }
    };
    // 3. Exact identity, published before the object gets its real name.
    //    Without a file handle (for example overlayfs) only the held
    //    descriptor can later prove it; recovery will preserve it.
    if let Some(reason) = deadline.stop_reason() {
        return Err(InstallInterrupted::stopped(reason, true, Some(held)));
    }
    let created = format!("{CREATED_PREFIX}{}", holder_id.encode());
    let published = identity_of(held.as_fd()).and_then(|identity| {
        // Pin before publication, including a failure or panic after the
        // record is visible. Retire only after all matching records disappear.
        pin_creation(registry, key, &identity, held.as_fd())?;
        let record = CreationRecord {
            kind,
            identity,
            parent_dev: parent_stat.st_dev as u64,
            parent_ino: parent_stat.st_ino as u64,
            staging: staging.as_bytes().to_vec(),
            creator: holder_id.clone(),
        };
        registry.write_key_file(key, &created, record.encode().as_bytes())
    });
    // 4. Install without ever replacing an existing target.
    let installed = match published {
        Ok(()) => match deadline.stop_reason() {
            Some(reason) => return Err(InstallInterrupted::stopped(reason, true, Some(held))),
            None => rename_noreplace(parent, &staging, name),
        },
        Err(err) => Err(err),
    };
    let Err(err) = installed else {
        return Ok((held, holder));
    };
    // Retract this target's own steps, only while custody runs.
    if let Some(reason) = deadline.stop_reason() {
        return Err(InstallInterrupted::failed(
            err,
            Some(stopped_outcome(reason)),
            Some(held),
        ));
    }
    let unproven = Some(TargetOutcome::Incomplete(
        IncompleteReason::IdentityUnprovable,
    ));
    let retracted = fstat(held.as_fd()).and_then(|stat| {
        Ok(same_entry(parent, &staging, &stat)? && remove_entry(parent, &staging, kind, &stat)?)
    });
    let outcome = match retracted {
        Ok(true) => {
            let records = registry
                .remove_key_file(key, &created)
                .and_then(|()| registry.remove_key_file(key, &holder_name));
            if records.is_ok() {
                if let Ok(stat) = fstat(held.as_fd()) {
                    retire_creation_pins(registry, key, stat.st_dev as u64, stat.st_ino as u64);
                }
            }
            records
                .err()
                .map(|_| TargetOutcome::Incomplete(IncompleteReason::HolderUnproven))
        }
        // The staging entry was not ours to remove, or could not be: kept.
        Ok(false) | Err(_) => unproven,
    };
    Err(InstallInterrupted::failed(err, outcome, Some(held)))
}

/// Records which processes must terminate before this launch's sandbox is
/// proven gone, in every holder of this launch (named by `ids.holder()`).
/// Each holder write is checked for expiry first: `Ok(false)` when custody
/// stopped before every holder was written (the evidence is then partial
/// and the caller must not admit the sandbox).
pub(crate) fn record_sandbox_tokens(
    registry: &Registry,
    registrations: &mut [SyntheticTargetRegistration],
    ids: &LaunchIdentity,
    tokens: &SandboxTokens,
    deadline: &mut CleanupDeadline,
) -> io::Result<bool> {
    for registration in registrations.iter_mut() {
        let Some(holder) = &mut registration.holder else {
            continue;
        };
        if deadline.stop_reason().is_some() {
            return Ok(false);
        }
        let mut updated = holder.clone();
        updated.sandbox = tokens.clone();
        registry.write_key_file(
            &registration.key,
            &holder_file(ids.holder()),
            updated.encode().as_bytes(),
        )?;
        *holder = updated;
    }
    Ok(true)
}

/// Records, in every holder of this launch, that its holder never forked
/// bubblewrap and never will. Only the keeper calls this, as part of its
/// finish, and only on a path on which it exits without forking (or after
/// closing the exec-start pipe of a parked child, which then exits without
/// ever executing bubblewrap). It is bookkeeping, not cleanup: after a stop
/// it runs only under a lock taken without waiting. It removes nothing: a
/// later, separate recovery may use it once the keeper is proven terminated.
pub(crate) fn record_never_started(
    registry: &Registry,
    registrations: &[SyntheticTargetRegistration],
    ids: &LaunchIdentity,
) -> io::Result<()> {
    for registration in registrations {
        let Some(holder) = &registration.holder else {
            continue;
        };
        let mut statement = holder.clone();
        statement.never_started = true;
        registry.write_key_file(
            &registration.key,
            &holder_file(ids.holder()),
            statement.encode().as_bytes(),
        )?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Release (launcher or reaper)
// ---------------------------------------------------------------------------

/// What is known about the launch's sandbox when its targets are released.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SandboxProof {
    /// bubblewrap was never forked.
    NeverStarted,
    /// The PID-namespace init exited: no sandbox process remains.
    Gone,
    /// Nothing proves it; fall back to an exhaustive namespace inspection.
    Unproven,
    /// The cleanup deadline passed before the sandbox exited.
    TimedOut,
    /// The owner is gone without ever issuing a deadline.
    OwnerGone,
}

/// The outcome for a target whose cleanup had to stop.
fn stopped_outcome(reason: StopReason) -> TargetOutcome {
    match reason {
        StopReason::DeadlinePassed => TargetOutcome::TimedOut,
        StopReason::OwnerGone => {
            TargetOutcome::Incomplete(IncompleteReason::OwnerGoneWithoutDeadline)
        }
    }
}

pub(crate) fn stopped_proof(reason: StopReason) -> SandboxProof {
    match reason {
        StopReason::DeadlinePassed => SandboxProof::TimedOut,
        StopReason::OwnerGone => SandboxProof::OwnerGone,
    }
}

/// Releases this launch's hold and returns the census. Idempotent. Holders
/// are kept (with their sandbox tokens) for every target left incomplete or
/// timed out, so next-launch recovery can still prove termination.
pub(crate) fn release_synthetic_targets(
    registry: &Registry,
    registrations: &[SyntheticTargetRegistration],
    ids: &LaunchIdentity,
    proof: SandboxProof,
    deadline: &mut CleanupDeadline,
) -> io::Result<Vec<(PathBuf, TargetOutcome)>> {
    release_synthetic_targets_with(
        registry,
        registrations,
        ids,
        proof,
        deadline,
        &mut NamespaceScan::default(),
    )
}

/// A unit-test-only hook run before each target's release, with its position
/// in release order.
#[cfg(test)]
type ReleaseHook = Box<dyn FnMut(usize)>;

#[cfg(test)]
thread_local! {
    static RELEASE_HOOK: std::cell::RefCell<Option<ReleaseHook>> =
        const { std::cell::RefCell::new(None) };
}

/// Runs `f` with `hook` called before each target's release (unit tests
/// only).
#[cfg(test)]
pub(crate) fn with_release_hook<T>(hook: impl FnMut(usize) + 'static, f: impl FnOnce() -> T) -> T {
    RELEASE_HOOK.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
    let result = f();
    RELEASE_HOOK.with(|slot| *slot.borrow_mut() = None);
    result
}

pub(crate) fn release_synthetic_targets_with(
    registry: &Registry,
    registrations: &[SyntheticTargetRegistration],
    ids: &LaunchIdentity,
    proof: SandboxProof,
    deadline: &mut CleanupDeadline,
    scan: &mut NamespaceScan,
) -> io::Result<Vec<(PathBuf, TargetOutcome)>> {
    let mut census = Vec::with_capacity(registrations.len());
    let registrations = registrations.iter().rev();
    #[cfg(test)]
    let registrations = registrations.enumerate();
    for entry in registrations {
        #[cfg(test)]
        let (position, registration) = entry;
        #[cfg(not(test))]
        let registration = entry;
        #[cfg(test)]
        {
            RELEASE_HOOK.with(|slot| {
                if let Some(hook) = slot.borrow_mut().as_mut() {
                    hook(position);
                }
            });
        }
        // Zero cleanup after the deadline: the rest is a sticky timeout.
        let stop = if registration.held.is_some() {
            deadline.stop_reason()
        } else {
            None
        };
        let outcome = if let Some(reason) = stop {
            stopped_outcome(reason)
        } else {
            release_one(registry, registration, ids, proof, deadline, scan)?
        };
        census.push((registration.path.clone(), outcome));
    }
    census.reverse();
    Ok(census)
}

fn release_one(
    registry: &Registry,
    registration: &SyntheticTargetRegistration,
    ids: &LaunchIdentity,
    proof: SandboxProof,
    deadline: &mut CleanupDeadline,
    scan: &mut NamespaceScan,
) -> io::Result<TargetOutcome> {
    let Some(held) = &registration.held else {
        return Ok(TargetOutcome::HostOwned);
    };
    let key = &registration.key;
    let sandbox_gone = match proof {
        SandboxProof::NeverStarted | SandboxProof::Gone => true,
        SandboxProof::Unproven => scan.proves_unused(&registration.path),
        SandboxProof::TimedOut => return Ok(TargetOutcome::TimedOut),
        SandboxProof::OwnerGone => {
            return Ok(TargetOutcome::Incomplete(
                IncompleteReason::OwnerGoneWithoutDeadline,
            ));
        }
    };
    if !sandbox_gone {
        return Ok(TargetOutcome::Incomplete(
            IncompleteReason::SandboxNotProvenGone,
        ));
    }
    // This launch's sandbox is gone: its hold (always the holder identity,
    // never the launcher's) no longer protects anything. Expiry first.
    if let Some(reason) = deadline.stop_reason() {
        return Ok(stopped_outcome(reason));
    }
    registry.remove_key_file(key, &holder_file(ids.holder()))?;
    match registry.holder_block(
        key,
        &registration.path,
        registration.parent.as_fd(),
        Some(ids.holder()),
        scan,
    )? {
        HolderBlock::None => {}
        HolderBlock::Live => return Ok(TargetOutcome::HandedOff),
        HolderBlock::Unproven => {
            return Ok(TargetOutcome::Incomplete(IncompleteReason::HolderUnproven));
        }
    }
    let held_stat = fstat(held.as_fd())?;
    let outcome = match fstatat_nofollow(registration.parent.as_fd(), &registration.name) {
        Err(err) if err.raw_os_error() == Some(libc::ENOENT) => TargetOutcome::Absent,
        Err(err) => return Err(err),
        Ok(stat) if (stat.st_dev, stat.st_ino) != (held_stat.st_dev, held_stat.st_ino) => {
            return Ok(TargetOutcome::Incomplete(IncompleteReason::Replaced));
        }
        Ok(_) => {
            // Expiry first, before the removal itself.
            if let Some(reason) = deadline.stop_reason() {
                return Ok(stopped_outcome(reason));
            }
            if !remove_entry(
                registration.parent.as_fd(),
                &registration.name,
                registration.kind,
                &held_stat,
            )? {
                return Ok(TargetOutcome::Incomplete(IncompleteReason::NotEmpty));
            }
            TargetOutcome::Reaped
        }
    };
    registry.retire_records(key, held_stat.st_dev as u64, held_stat.st_ino as u64)?;
    registry.remove_key_if_idle(key)?;
    Ok(outcome)
}

// ---------------------------------------------------------------------------
// Next-launch recovery (separate, bounded maintenance with its own receipt)
// ---------------------------------------------------------------------------

/// Recovers keys within [`RECOVERY_BUDGET`] and writes a recovery receipt
/// when anything was attempted. It never touches launch receipts: a launch
/// that ended `timed-out` stays `timed-out`. Failures leave keys as they are.
#[cfg(test)]
pub(crate) fn recover_registry(registry: &Registry, by: &ProcessToken) {
    recover_registry_within(registry, by, &mut CleanupDeadline::none());
}

/// Recovery's budget: [`RECOVERY_BUDGET`], or what is left of the owner's
/// original deadline when that is smaller (zero once it passed or the owner
/// is gone without one).
pub(crate) fn recovery_budget(deadline: &mut CleanupDeadline) -> Duration {
    if deadline.stop_reason().is_some() {
        return Duration::ZERO;
    }
    deadline
        .remaining()
        .map_or(RECOVERY_BUDGET, |remaining| remaining.min(RECOVERY_BUDGET))
}

/// [`recover_registry`] bounded by the owner's original `deadline` as well:
/// within [`recovery_budget`], and checked for expiry before every key. A
/// key not reached is counted as deferred.
pub(crate) fn recover_registry_within(
    registry: &Registry,
    by: &ProcessToken,
    owner_deadline: &mut CleanupDeadline,
) {
    let budget = recovery_budget(owner_deadline);
    let _ = registry.prune_receipts();
    let Ok(entries) = fs::read_dir(registry.at("")) else {
        return;
    };
    let keys: Vec<String> = entries
        .flatten()
        .filter_map(|entry| entry.file_name().to_str().map(str::to_string))
        .filter(|name| name.starts_with(KEY_PREFIX))
        .collect();
    let deadline = Instant::now() + budget;
    let mut scan = NamespaceScan::default();
    let mut outcomes = Vec::new();
    let mut deferred = 0;
    for key in &keys {
        if Instant::now() >= deadline || owner_deadline.stop_reason().is_some() {
            deferred += 1;
            continue;
        }
        let path = registry.key_path(key).ok().flatten();
        match recover_key(registry, key, &mut scan) {
            Ok(Some(outcome)) => outcomes.push((path.unwrap_or_default(), outcome)),
            Ok(None) => {}
            // An untrusted key is left untouched and reported, never skipped
            // silently.
            Err(err) if err.kind() == io::ErrorKind::PermissionDenied => outcomes.push((
                path.unwrap_or_default(),
                TargetOutcome::Incomplete(IncompleteReason::UntrustedRecord),
            )),
            Err(_) => {}
        }
    }
    if outcomes.is_empty() && deferred == 0 {
        return;
    }
    let state = if deferred > 0 {
        "partial"
    } else {
        CleanupState::of(&outcomes).encode()
    };
    let mut text = format!(
        "v=1\nkind=recovery\nby={}\nstate={state}\ndeferred={deferred}\n",
        by.encode()
    );
    for (path, outcome) in &outcomes {
        text.push_str(&format!(
            "key={} {}\n",
            outcome.encode(),
            hex(path.as_os_str().as_bytes())
        ));
    }
    let _ = registry.write_receipt(&format!("{RECOVERY_RECEIPT_PREFIX}{}", by.encode()), &text);
}

/// Removes leftovers of terminated creators that the records identify
/// exactly. Returns `None` when there was nothing of a terminated creator to
/// act on, otherwise the outcome for this key.
pub(crate) fn recover_key(
    registry: &Registry,
    key: &str,
    scan: &mut NamespaceScan,
) -> io::Result<Option<TargetOutcome>> {
    let Some(path) = registry.key_path(key)? else {
        registry.remove_key_if_idle(key)?;
        return Ok(None);
    };
    let candidates: Vec<(String, CreationRecord)> = registry
        .records(key)?
        .into_iter()
        .filter_map(|(file, record)| Some((file, record?)))
        .filter(|(_, record)| liveness(&record.creator) == Liveness::Terminated)
        .collect();
    if candidates.is_empty() {
        return Ok(None);
    }
    let (parent_path, name) = split_target(&path)?;
    let Ok(parent) = open_directory(&parent_path) else {
        // The parent may be an absent mount: keep everything.
        return Ok(Some(TargetOutcome::Incomplete(
            IncompleteReason::ParentChanged,
        )));
    };
    match registry.holder_block(key, &path, parent.as_fd(), None, scan)? {
        HolderBlock::None => {}
        // A live launch holds it and will release it itself.
        HolderBlock::Live => return Ok(None),
        HolderBlock::Unproven => {
            return Ok(Some(TargetOutcome::Incomplete(
                IncompleteReason::HolderUnproven,
            )));
        }
    }
    let parent_stat = fstat(parent.as_fd())?;
    let boot_id = read_boot_id()?;
    let mut summary = TargetOutcome::Absent;
    for (file, record) in candidates {
        if (record.parent_dev, record.parent_ino)
            != (parent_stat.st_dev as u64, parent_stat.st_ino as u64)
        {
            summary = TargetOutcome::Incomplete(IncompleteReason::ParentChanged);
            continue;
        }
        let staging = CString::new(record.staging.clone()).map_err(io::Error::other)?;
        let mut resolved = true;
        for entry in [&name, &staging] {
            let fd = match open_entry(parent.as_fd(), entry, record.kind) {
                Ok(fd) => fd,
                Err(err) if err.raw_os_error() == Some(libc::ENOENT) => continue,
                // A symlink, wrong type or unreadable entry is not ours.
                Err(_) => {
                    resolved = false;
                    summary = TargetOutcome::Incomplete(IncompleteReason::Replaced);
                    continue;
                }
            };
            // Inode evidence is only compared within the boot that recorded
            // it; a reboot can renumber devices and reuse inodes.
            if record.creator.boot_id != boot_id {
                resolved = false;
                summary = TargetOutcome::Incomplete(IncompleteReason::OtherBoot);
                continue;
            }
            // Without a generation-bearing handle, and with the original
            // target descriptor gone with its holder, nothing proves the inode
            // is still the recorded one: preserve, never guess.
            if record.identity.handle.is_none() {
                resolved = false;
                summary = TargetOutcome::Incomplete(IncompleteReason::IdentityUnprovable);
                continue;
            }
            if identity_of(fd.as_fd())? != record.identity {
                resolved = false;
                summary = TargetOutcome::Incomplete(IncompleteReason::Replaced);
                continue;
            }
            let stat = fstat(fd.as_fd())?;
            if remove_entry(parent.as_fd(), entry, record.kind, &stat)? {
                if !matches!(summary, TargetOutcome::Incomplete(_)) {
                    summary = TargetOutcome::Reaped;
                }
            } else {
                resolved = false;
                summary = TargetOutcome::Incomplete(IncompleteReason::NotEmpty);
            }
        }
        if resolved {
            registry.remove_key_file(key, &file)?;
        }
    }
    registry.remove_key_if_idle(key)?;
    Ok(Some(summary))
}

// ---------------------------------------------------------------------------
// Sandbox observation, launcher finish, and the reaper
// ---------------------------------------------------------------------------

/// Creates the `--info-fd` pipe through which bubblewrap reports the sandbox
/// init pid. The launcher keeps the read end open until bubblewrap exits, so
/// the report never fails with `EPIPE`.
pub(crate) fn bwrap_info_pipe() -> io::Result<(OwnedFd, File)> {
    let mut fds = [-1; 2];
    // SAFETY: valid two-element array.
    if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) } < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: pipe2 returned two new owned descriptors.
    Ok(unsafe { (OwnedFd::from_raw_fd(fds[0]), File::from_raw_fd(fds[1])) })
}

/// Pidfds and tokens for bubblewrap and the sandbox init.
#[derive(Debug)]
pub(crate) struct SandboxProcesses {
    tokens: SandboxTokens,
    bwrap: Option<OwnedFd>,
    init: Option<OwnedFd>,
}

impl SandboxProcesses {
    /// Pins bubblewrap, the keeper's own unreaped child (its pid cannot be
    /// reused before the keeper reaps it). Anything that cannot be pinned is
    /// left out, which only weakens the termination proof (fail closed); the
    /// keeper never admits a bubblewrap it could not pin.
    pub(crate) fn pin_bwrap(bwrap_pid: libc::pid_t, pid_namespace: bool) -> Self {
        let mut processes = Self {
            tokens: SandboxTokens {
                pid_namespace,
                bwrap: None,
                init: None,
            },
            bwrap: None,
            init: None,
        };
        if let Ok(pidfd) = pidfd_open(bwrap_pid)
            && let Ok(token) = ProcessToken::for_pid(bwrap_pid)
            && pidfd_signal(pidfd.as_fd(), 0).is_ok()
        {
            processes.tokens.bwrap = Some(token);
            processes.bwrap = Some(pidfd);
        }
        processes
    }

    /// Pins the sandbox init from bubblewrap's `--info-fd` report, read until
    /// `until` at the latest. The caller clamps `until` to [`INFO_DEADLINE`]
    /// and to what is left of the owner's original deadline.
    ///
    /// The bound is live: the earlier of `info_cap` and the owner's current
    /// deadline, re-evaluated (expiry first) before every poll, read, pidfd
    /// lookup and pin. A tightening or the owner's release wakes the wait.
    /// Nothing is polled, read, looked up or pinned at or after expiry.
    pub(crate) fn observe_init(
        &mut self,
        bwrap_pid: libc::pid_t,
        info_read: &OwnedFd,
        info_cap: Instant,
        deadline: &mut CleanupDeadline,
    ) -> InitObservation {
        let init_pid = match read_info_child_pid(info_read, info_cap, deadline) {
            InfoReport::ChildPid(pid) => pid,
            InfoReport::Unavailable => return InitObservation::NotPinned,
            InfoReport::Expired => return InitObservation::Expired,
        };
        let expired = |deadline: &mut CleanupDeadline| {
            deadline.stop_reason().is_some() || Instant::now() >= info_cap
        };
        if expired(deadline) {
            return InitObservation::Expired;
        }
        #[cfg(test)]
        count_info_io_for_tests(InfoIo::PidfdLookup);
        let Ok(pidfd) = pidfd_open(init_pid) else {
            return InitObservation::NotPinned;
        };
        if expired(deadline) {
            return InitObservation::Expired;
        }
        if !read_process_stat(init_pid).is_ok_and(|stat| stat.parent == bwrap_pid) {
            return InitObservation::NotPinned;
        }
        if expired(deadline) {
            return InitObservation::Expired;
        }
        let Ok(token) = ProcessToken::for_pid(init_pid) else {
            return InitObservation::NotPinned;
        };
        if expired(deadline) {
            return InitObservation::Expired;
        }
        if pidfd_signal(pidfd.as_fd(), 0).is_err() {
            return InitObservation::NotPinned;
        }
        if expired(deadline) {
            return InitObservation::Expired;
        }
        self.tokens.init = Some(token);
        self.init = Some(pidfd);
        InitObservation::Pinned
    }

    pub(crate) fn tokens(&self) -> &SandboxTokens {
        &self.tokens
    }

    /// The pidfd of bubblewrap, when it is pinned.
    pub(crate) fn bwrap_pidfd(&self) -> Option<BorrowedFd<'_>> {
        self.bwrap.as_ref().map(AsFd::as_fd)
    }

    /// Whether the sandbox init is pinned.
    pub(crate) fn init_pinned(&self) -> bool {
        self.init.is_some()
    }

    /// Delivers `signal` to bubblewrap through its pidfd only: never to a pid
    /// that could have been reused, and never to anything else.
    pub(crate) fn signal_bwrap(&self, signal: libc::c_int) {
        if let Some(pidfd) = &self.bwrap {
            let _ = pidfd_signal(pidfd.as_fd(), signal);
        }
    }

    /// Waits, event-driven on pidfds and never past the owner's deadline,
    /// for bubblewrap and then the init to exit. Without an owner it only
    /// checks.
    fn wait_gone(&self, deadline: &mut CleanupDeadline) -> SandboxProof {
        for pidfd in [&self.bwrap, &self.init].into_iter().flatten() {
            match deadline.wait_readable(pidfd.as_fd()) {
                Wait::Ready => {}
                Wait::Expired => {
                    return deadline
                        .stop_reason()
                        .map_or(SandboxProof::TimedOut, stopped_proof);
                }
                Wait::NotYet => return SandboxProof::Unproven,
            }
        }
        if let Some(reason) = deadline.stop_reason() {
            return stopped_proof(reason);
        }
        if self.tokens.pid_namespace && self.init.is_some() {
            SandboxProof::Gone
        } else {
            SandboxProof::Unproven
        }
    }

    /// The keeper's termination proof. It is the existing pidfd proof; only
    /// with a private PID namespace whose init could not be pinned does the
    /// keeper join its own children instead (see [`join_owned_children`]).
    /// Without a private PID namespace the result stays `Unproven`, so release
    /// falls back to the exhaustive namespace scan, exactly as before.
    pub(crate) fn keeper_proof(&self, deadline: &mut CleanupDeadline) -> SandboxProof {
        let proof = self.wait_gone(deadline);
        if proof == SandboxProof::Unproven && self.tokens.pid_namespace && self.init.is_none() {
            return join_owned_children(deadline);
        }
        proof
    }
}

/// Proves, in the keeper only, that every descendant of the keeper has exited
/// and been reaped: bubblewrap, and the sandbox init if bubblewrap died first
/// (the keeper is a child subreaper exactly when the launch uses a private
/// PID namespace, and the init's exit empties that namespace before it can be
/// reaped).
///
/// `ECHILD` is trusted only because the keeper is single-purpose: it forks
/// nothing but bubblewrap and must run no other thread, so every child it has
/// is a sandbox process of this launch. Every child is enumerated from
/// `/proc/self/task/<tid>/children` and pinned by a pidfd (exact: an unreaped
/// child's pid cannot be reused) before it is waited for, never past the
/// owner's deadline, and reaped. Fails closed (`Unproven`) when another
/// thread exists, when the children cannot be enumerated, or when `waitpid`
/// finds a child the enumeration did not list. Never signals anything.
fn join_owned_children(deadline: &mut CleanupDeadline) -> SandboxProof {
    if !single_threaded() {
        return SandboxProof::Unproven;
    }
    loop {
        if let Some(reason) = deadline.stop_reason() {
            return stopped_proof(reason);
        }
        let Ok(children) = own_children() else {
            return SandboxProof::Unproven;
        };
        if children.is_empty() {
            let mut status = 0;
            // SAFETY: non-blocking reap; `status` is a valid out-parameter.
            let rc = unsafe { libc::waitpid(-1, &mut status, libc::WNOHANG) };
            if rc > 0 {
                continue;
            }
            if rc == 0 {
                // A child exists that the enumeration did not list.
                return SandboxProof::Unproven;
            }
            return match io::Error::last_os_error().raw_os_error() {
                Some(libc::ECHILD) => SandboxProof::Gone,
                Some(libc::EINTR) => continue,
                _ => SandboxProof::Unproven,
            };
        }
        for child in children {
            let pidfd = match pidfd_open(child) {
                Ok(pidfd) => pidfd,
                // Already reaped: only the keeper reaps its children.
                Err(err) if err.raw_os_error() == Some(libc::ESRCH) => continue,
                Err(_) => return SandboxProof::Unproven,
            };
            match deadline.wait_readable(pidfd.as_fd()) {
                Wait::Ready => reap_exited_child(child),
                Wait::Expired => {
                    return deadline
                        .stop_reason()
                        .map_or(SandboxProof::TimedOut, stopped_proof);
                }
                Wait::NotYet => return SandboxProof::Unproven,
            }
        }
    }
}

/// Whether this process runs exactly one thread (`/proc/self/status`).
fn single_threaded() -> bool {
    fs::read_to_string("/proc/self/status").is_ok_and(|status| {
        status
            .lines()
            .find_map(|line| line.strip_prefix("Threads:"))
            .is_some_and(|threads| threads.trim() == "1")
    })
}

/// The children of this (single-threaded) process, from
/// `/proc/self/task/<tid>/children` (`CONFIG_PROC_CHILDREN`).
fn own_children() -> io::Result<Vec<libc::pid_t>> {
    // SAFETY: gettid has no preconditions.
    let tid = unsafe { libc::syscall(libc::SYS_gettid) };
    let text = fs::read_to_string(format!("/proc/self/task/{tid}/children"))?;
    text.split_ascii_whitespace()
        .map(|pid| {
            pid.parse()
                .map_err(|_| io::Error::other(format!("unparseable child pid {pid:?}")))
        })
        .collect()
}

/// Reaps one child that is known to have exited; never blocks.
fn reap_exited_child(pid: libc::pid_t) {
    loop {
        let mut status = 0;
        // SAFETY: non-blocking reap of one child.
        let rc = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
        if rc < 0 && io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        return;
    }
}

/// The owner's custody of one launch: its cleanup deadline and report pipe.
#[derive(Debug)]
pub(crate) struct LaunchCustody {
    pub(crate) deadline: CleanupDeadline,
    pub(crate) report: Option<OwnedFd>,
}

/// Writes the keeper launch's `running` receipt before the keeper may act:
/// named by the launcher, it declares the keeper as the launch's reaper and
/// lists no target, because nothing exists yet. The launcher writes it, then
/// declares the keeper to the owner, and only then admits the keeper.
pub(crate) fn declare_keeper_receipt(registry: &Registry, ids: &LaunchIdentity) -> io::Result<()> {
    registry.write_launch_receipt(&LaunchReceipt {
        launch: ids.receipt().clone(),
        state: CleanupState::Running,
        finisher: None,
        reaper: ids.declared_reaper().cloned(),
        reaper_join: ReaperJoin::Pending,
        targets: Vec::new(),
    })
}

/// A finished launch's state. A stopped proof (deadline passed, or owner
/// gone without one) is kept whatever the census holds, so an empty or
/// all-host-owned census is never reported `complete` after a stop.
fn finished_state(census: &[(PathBuf, TargetOutcome)], proof: SandboxProof) -> CleanupState {
    let state = CleanupState::of(census);
    match proof {
        SandboxProof::TimedOut => CleanupState::TimedOut,
        SandboxProof::OwnerGone if state != CleanupState::TimedOut => CleanupState::Incomplete,
        _ => state,
    }
}

fn effective_keeper_proof(proof: SandboxProof, deadline: &mut CleanupDeadline) -> SandboxProof {
    match (proof, deadline.stop_reason()) {
        (SandboxProof::TimedOut, _) | (_, Some(StopReason::DeadlinePassed)) => {
            SandboxProof::TimedOut
        }
        (_, Some(StopReason::OwnerGone)) => SandboxProof::OwnerGone,
        _ => proof,
    }
}

/// The registry lock for the keeper's bookkeeping. While custody runs it is
/// awaited, never past the owner's deadline; once custody stopped (or the
/// wait reached the deadline) it is only tried once, without waiting.
fn keeper_bookkeeping_lock(
    deadline: &mut CleanupDeadline,
    stopped: bool,
) -> Result<(Registry, RegistryLock), String> {
    let registry = open_shared_registry()
        .map_err(|err| format!("the sandbox registry is unavailable: {err}"))?;
    let lock = if stopped || deadline.stop_reason().is_some() {
        registry.try_lock()
    } else {
        match registry.lock_within(deadline) {
            Ok(Some(lock)) => Ok(Some(lock)),
            Ok(None) => registry.try_lock(),
            Err(err) => Err(err),
        }
    };
    match lock {
        Ok(Some(lock)) => Ok((registry, lock)),
        Ok(None) => Err(
            "the sandbox registry lock was held by another launch after cleanup custody stopped"
                .to_string(),
        ),
        Err(err) => Err(format!("cannot lock the sandbox registry: {err}")),
    }
}

/// What the keeper's finish reported.
#[derive(Debug)]
pub(crate) struct KeeperFinish {
    pub(crate) state: CleanupState,
    /// Why the receipt (or the never-started statement) was not persisted:
    /// the registry still says `running`, and the owner report says so.
    pub(crate) persist_error: Option<String>,
}

impl KeeperFinish {
    /// The reported state, as the receipt spells it.
    pub(crate) fn state_name(&self) -> &'static str {
        self.state.encode()
    }
}

fn note_persist_error(slot: &mut Option<String>, error: String) {
    if slot.is_none() {
        *slot = Some(error);
    }
}

/// The keeper's only way to finish a launch, bounded throughout by the
/// owner's original deadline:
///
/// - the registry lock is awaited only while custody runs, and only until
///   the deadline; after a stop it is tried once, without waiting;
/// - with the lock, `never_started` first records that statement, then the
///   targets are released (by their held descriptors; every removal checked
///   for expiry first) with `proof`, unless `known` already is the census
///   (a registration that stopped or retracted itself);
/// - without it nothing is changed: the census is computed in memory, the
///   registry keeps its `running` receipt, and the failed persist is part of
///   the owner report (`persist=failed`).
///
/// The receipt is finished as the launch's reaper. `reaper_join` must be what
/// is true when it is written: `pending` while the launcher is alive (it joins
/// the keeper afterwards), `finished-by-reaper` once the launcher is gone, and
/// `exited-at-deadline` or `exited-owner-gone` when the keeper stops before
/// the sandbox starts while the launcher is alive.
pub(crate) fn finish_keeper(
    registrations: &[SyntheticTargetRegistration],
    known: Option<Vec<(PathBuf, TargetOutcome)>>,
    ids: &LaunchIdentity,
    proof: SandboxProof,
    custody: &mut LaunchCustody,
    reaper_join: ReaperJoin,
    never_started: bool,
) -> KeeperFinish {
    let mut persist_error = None;
    let proof = effective_keeper_proof(proof, &mut custody.deadline);
    let stopped = matches!(proof, SandboxProof::TimedOut | SandboxProof::OwnerGone);
    let locked = keeper_bookkeeping_lock(&mut custody.deadline, stopped);
    let proof = effective_keeper_proof(proof, &mut custody.deadline);
    let mut census = match &locked {
        Ok((registry, _lock)) => {
            if never_started && let Err(err) = record_never_started(registry, registrations, ids) {
                persist_error = Some(format!(
                    "cannot record that bubblewrap never started: {err}"
                ));
            }
            match known {
                Some(census) => census,
                None => release_synthetic_targets(
                    registry,
                    registrations,
                    ids,
                    proof,
                    &mut custody.deadline,
                )
                .unwrap_or_else(|err| {
                    note_persist_error(
                        &mut persist_error,
                        format!("cannot release the targets: {err}"),
                    );
                    unreleased_census(registrations, &mut custody.deadline)
                }),
            }
        }
        Err(err) => {
            persist_error = Some(err.clone());
            known.unwrap_or_else(|| unreleased_census(registrations, &mut custody.deadline))
        }
    };
    let proof = effective_keeper_proof(proof, &mut custody.deadline);
    let reason = match proof {
        SandboxProof::TimedOut => Some(StopReason::DeadlinePassed),
        SandboxProof::OwnerGone => Some(StopReason::OwnerGone),
        _ => None,
    };
    if let Some(reason) = reason {
        for (_, outcome) in &mut census {
            if *outcome == TargetOutcome::Pending {
                *outcome = stopped_outcome(reason);
            }
        }
    }
    let mut receipt = LaunchReceipt {
        launch: ids.receipt().clone(),
        state: finished_state(&census, proof),
        finisher: Some("reaper"),
        reaper: Some(ids.holder().clone()),
        reaper_join,
        targets: census,
    };
    receipt.state = finished_state(
        &receipt.targets,
        effective_keeper_proof(proof, &mut custody.deadline),
    );
    if persist_error.is_some() && receipt.state == CleanupState::Complete {
        receipt.state = CleanupState::Incomplete;
    }
    if let Ok((registry, _lock)) = &locked
        && let Err(err) = registry.write_launch_receipt(&receipt)
    {
        note_persist_error(
            &mut persist_error,
            format!("cannot write the launch receipt: {err}"),
        );
        if receipt.state == CleanupState::Complete {
            receipt.state = CleanupState::Incomplete;
        }
    }
    drop(locked);
    let mut report = receipt.encode();
    if persist_error.is_some() {
        // Visible to the owner, not only logged: the registry still holds
        // the `running` receipt (or lacks the never-started statement).
        report.push_str("persist=failed\n");
    }
    write_report(custody.report.as_ref(), &report);
    KeeperFinish {
        state: receipt.state,
        persist_error,
    }
}

/// The launcher's finish of a declared keeper launch whose keeper refused,
/// by its typed refusal, before any side effect: nothing was installed, so
/// the receipt keeps no target, and it is finished as `incomplete` (the
/// launch never ran; nothing here is a positive no-launch claim), named by the
/// launcher, which finished it, with the declared keeper as its reaper, not
/// joined yet. Bounded like every keeper bookkeeping step: the lock is awaited
/// only within the owner's original deadline, and tried once, without
/// waiting, after a stop. A failed persist leaves the registry's `running`
/// receipt and is visible in the owner report (`persist=failed`).
pub(crate) fn finish_refused_keeper(
    ids: &LaunchIdentity,
    custody: &mut LaunchCustody,
) -> KeeperFinish {
    let receipt = LaunchReceipt {
        launch: ids.receipt().clone(),
        state: CleanupState::Incomplete,
        finisher: Some("launcher"),
        reaper: ids.declared_reaper().cloned(),
        reaper_join: ReaperJoin::Pending,
        targets: Vec::new(),
    };
    let stopped = custody.deadline.stop_reason().is_some();
    let persist_error = match keeper_bookkeeping_lock(&mut custody.deadline, stopped) {
        Ok((registry, _lock)) => registry
            .write_launch_receipt(&receipt)
            .err()
            .map(|err| format!("cannot write the launch receipt: {err}")),
        Err(err) => Some(err),
    };
    let mut report = receipt.encode();
    report.push_str("keeper=refused\n");
    if persist_error.is_some() {
        report.push_str("persist=failed\n");
    }
    write_report(custody.report.as_ref(), &report);
    KeeperFinish {
        state: receipt.state,
        persist_error,
    }
}

/// The single-process launch's normal-exit path (the proc preflight): waits
/// for the sandbox to be gone (never past the owner's deadline), releases,
/// finishes the launch receipt and reports it to the owner.
pub(crate) fn finish_launch(
    registrations: &[SyntheticTargetRegistration],
    ids: &LaunchIdentity,
    sandbox: Option<&SandboxProcesses>,
    custody: &mut LaunchCustody,
) -> io::Result<CleanupState> {
    let proof = match sandbox {
        Some(sandbox) => sandbox.wait_gone(&mut custody.deadline),
        None => match custody.deadline.stop_reason() {
            Some(reason) => stopped_proof(reason),
            None => SandboxProof::Unproven,
        },
    };
    let receipt = with_shared_registry_lock(|registry| -> io::Result<LaunchReceipt> {
        let census =
            release_synthetic_targets(registry, registrations, ids, proof, &mut custody.deadline)?;
        let receipt = LaunchReceipt {
            launch: ids.receipt().clone(),
            state: finished_state(&census, proof),
            finisher: Some("launcher"),
            reaper: None,
            reaper_join: ReaperJoin::None,
            targets: census,
        };
        registry.write_launch_receipt(&receipt)?;
        Ok(receipt)
    })?;
    write_report(custody.report.as_ref(), &receipt.encode());
    Ok(receipt.state)
}

/// Declares to the owner which reaper exists for this launch: `None` (written
/// before the sandboxed command can start) positively proves that no reaper
/// was created; `Some` names the exact reaper so the owner can pin and join
/// it. Nothing is declared when a reaper may exist but cannot be named.
pub(crate) fn declare_reaper(report: Option<&OwnedFd>, reaper: Option<&ProcessToken>) {
    let reaper = reaper.map_or_else(|| "none".to_string(), ProcessToken::encode);
    write_report(report, &format!("v=1\nkind=helpers\nreaper={reaper}\n"));
}

/// The receipt for a launch that installed nothing: written to the owner's
/// report pipe before the launcher execs bubblewrap.
pub(crate) fn no_target_receipt(launch: &ProcessToken) -> String {
    LaunchReceipt {
        launch: launch.clone(),
        state: CleanupState::Complete,
        finisher: Some("launcher"),
        reaper: None,
        reaper_join: ReaperJoin::None,
        targets: Vec::new(),
    }
    .encode()
}

/// The result of observing the sandbox init.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InitObservation {
    Pinned,
    /// No report, or a report naming no pinnable child of bubblewrap.
    NotPinned,
    /// The live bound (info cap or owner deadline) passed first.
    Expired,
}

/// The result of reading bubblewrap's `--info-fd` report.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InfoReport {
    ChildPid(libc::pid_t),
    Unavailable,
    Expired,
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum InfoIo {
    Poll,
    Read,
    PidfdLookup,
}

/// A unit-test-only hook run right before each poll of the info report.
#[cfg(test)]
type InfoPollHook = Option<Box<dyn FnMut()>>;

#[cfg(test)]
thread_local! {
    /// Unit-test-only counts of the info report's polls, reads and pidfd
    /// lookups on this thread, and the hook run right before each poll.
    static INFO_IO_FOR_TESTS: std::cell::Cell<[usize; 3]> = const { std::cell::Cell::new([0; 3]) };
    static INFO_BEFORE_POLL_HOOK: std::cell::RefCell<InfoPollHook> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn count_info_io_for_tests(io: InfoIo) {
    INFO_IO_FOR_TESTS.with(|counts| {
        let mut current = counts.get();
        current[io as usize] += 1;
        counts.set(current);
    });
}

/// `(polls, reads, pidfd lookups)` of the info report on this thread (unit
/// tests only).
#[cfg(test)]
pub(crate) fn info_io_for_tests() -> (usize, usize, usize) {
    let [polls, reads, lookups] = INFO_IO_FOR_TESTS.with(std::cell::Cell::get);
    (polls, reads, lookups)
}

/// Runs `f` with `hook` called right before each poll of the info report
/// (unit tests only).
#[cfg(test)]
pub(crate) fn with_info_poll_hook<T>(hook: impl FnMut() + 'static, f: impl FnOnce() -> T) -> T {
    INFO_BEFORE_POLL_HOOK.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
    let result = f();
    INFO_BEFORE_POLL_HOOK.with(|slot| *slot.borrow_mut() = None);
    result
}

/// Reads bubblewrap's `--info-fd` report until EOF, never at or past the live
/// bound: the earlier of `info_cap` and the owner's current deadline. Expiry
/// is checked first before every poll and every read. The owner's deadline
/// channel is polled alongside the report, so a tightening, or the owner's
/// release, wakes the wait and is applied at once.
fn read_info_child_pid(
    info_read: &OwnedFd,
    info_cap: Instant,
    deadline: &mut CleanupDeadline,
) -> InfoReport {
    let mut report = Vec::new();
    loop {
        // Expiry first, against the live bound.
        if deadline.stop_reason().is_some() {
            return InfoReport::Expired;
        }
        let now = Instant::now();
        let mut remaining = info_cap.saturating_duration_since(now);
        if let Some(owner) = deadline.remaining() {
            remaining = remaining.min(owner);
        }
        if remaining.is_zero() {
            return InfoReport::Expired;
        }
        let timeout = libc::c_int::try_from(remaining.as_nanos().div_ceil(1_000_000))
            .unwrap_or(libc::c_int::MAX);
        let mut fds = vec![libc::pollfd {
            fd: info_read.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        }];
        if let Some(channel) = deadline.channel_fd() {
            fds.push(libc::pollfd {
                fd: channel,
                events: libc::POLLIN,
                revents: 0,
            });
        }
        #[cfg(test)]
        {
            INFO_BEFORE_POLL_HOOK.with(|slot| {
                if let Some(hook) = slot.borrow_mut().as_mut() {
                    hook();
                }
            });
            count_info_io_for_tests(InfoIo::Poll);
        }
        // SAFETY: valid pollfd array.
        let rc = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, timeout) };
        if rc < 0 {
            if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return InfoReport::Unavailable;
        }
        if fds[0].revents == 0 {
            // The bound, a tightening or the owner's release: re-evaluated.
            continue;
        }
        // Expiry first, again: readiness at or after the bound is not read.
        if deadline.stop_reason().is_some() || Instant::now() >= info_cap {
            return InfoReport::Expired;
        }
        let mut buffer = [0_u8; 512];
        #[cfg(test)]
        count_info_io_for_tests(InfoIo::Read);
        // SAFETY: valid descriptor and buffer.
        let read = unsafe {
            libc::read(
                info_read.as_raw_fd(),
                buffer.as_mut_ptr().cast(),
                buffer.len(),
            )
        };
        match read {
            0 => break,
            read if read > 0 => report.extend_from_slice(&buffer[..read as usize]),
            _ if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted => {}
            _ => return InfoReport::Unavailable,
        }
    }
    let Ok(report) = String::from_utf8(report) else {
        return InfoReport::Unavailable;
    };
    let digits: String = report
        .split("\"child-pid\":")
        .nth(1)
        .unwrap_or_default()
        .trim_start()
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits
        .parse()
        .map_or(InfoReport::Unavailable, InfoReport::ChildPid)
}

// ---------------------------------------------------------------------------
// Mount-namespace inspection
// ---------------------------------------------------------------------------

/// Exhaustive inspection of the mount namespaces of every process running as
/// this uid. It proves a path unused only when every such namespace could be
/// read; an uninspectable one (for example a non-dumpable process) means
/// "possibly still mounted", never "absent".
#[derive(Debug, Default)]
pub(crate) struct NamespaceScan {
    result: Option<Option<HashSet<Vec<u8>>>>,
}

impl NamespaceScan {
    /// A scan that met an uninspectable namespace.
    #[cfg(test)]
    pub(crate) fn uninspectable_for_tests() -> Self {
        Self { result: Some(None) }
    }

    fn proves_unused(&mut self, path: &Path) -> bool {
        let points = self.result.get_or_insert_with(scan_own_namespaces);
        match points {
            Some(points) => !points.contains(path.as_os_str().as_bytes()),
            None => false,
        }
    }
}

fn scan_own_namespaces() -> Option<HashSet<Vec<u8>>> {
    let mut namespaces = HashSet::new();
    let mut points = HashSet::new();
    for entry in fs::read_dir("/proc").ok()?.flatten() {
        let name = entry.file_name();
        if name.to_str().and_then(|n| n.parse::<u32>().ok()).is_none() {
            continue;
        }
        let process = Path::new("/proc").join(&name);
        let namespace = match fs::read_link(process.join("ns/mnt")) {
            Ok(namespace) => namespace,
            Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
            Err(_) => match process_real_uid(&process) {
                ProcessOwner::Exited | ProcessOwner::Other => continue,
                ProcessOwner::Own | ProcessOwner::Unknown => return None,
            },
        };
        if !namespaces.insert(namespace) {
            continue;
        }
        let mountinfo = match fs::read(process.join("mountinfo")) {
            Ok(mountinfo) => mountinfo,
            Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
            Err(_) => return None,
        };
        for line in mountinfo.split(|byte| *byte == b'\n') {
            if let Some(point) = line.split(|byte| *byte == b' ').nth(4) {
                points.insert(unescape_mountinfo(point));
            }
        }
    }
    Some(points)
}

enum ProcessOwner {
    Own,
    Other,
    Exited,
    Unknown,
}

fn process_real_uid(process: &Path) -> ProcessOwner {
    let status = match fs::read_to_string(process.join("status")) {
        Ok(status) => status,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return ProcessOwner::Exited,
        Err(_) => return ProcessOwner::Unknown,
    };
    let uid: Option<libc::uid_t> = status
        .lines()
        .find_map(|line| line.strip_prefix("Uid:"))
        .and_then(|uids| uids.split_ascii_whitespace().next())
        .and_then(|uid| uid.parse().ok());
    match uid {
        Some(uid) if uid == unsafe { libc::geteuid() } => ProcessOwner::Own,
        Some(_) => ProcessOwner::Other,
        None => ProcessOwner::Unknown,
    }
}

fn unescape_mountinfo(field: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(field.len());
    let mut index = 0;
    while index < field.len() {
        if field[index] == b'\\'
            && index + 3 < field.len()
            && field[index + 1..=index + 3]
                .iter()
                .all(|byte| (b'0'..=b'7').contains(byte))
        {
            let value = field[index + 1..=index + 3]
                .iter()
                .fold(0_u32, |acc, byte| acc * 8 + u32::from(byte - b'0'));
            out.push(value as u8);
            index += 4;
        } else {
            out.push(field[index]);
            index += 1;
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Low-level helpers
// ---------------------------------------------------------------------------

fn key_name(path: &Path, parent_dev: u64) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in path.as_os_str().as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{KEY_PREFIX}{hash:016x}-{parent_dev:x}")
}

fn split_target(path: &Path) -> io::Result<(PathBuf, CString)> {
    let invalid = || {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid synthetic mount target {}", path.display()),
        )
    };
    let parent = path.parent().ok_or_else(invalid)?;
    let name = path.file_name().ok_or_else(invalid)?;
    if !path.is_absolute() || name == ".." || name == "." {
        return Err(invalid());
    }
    Ok((
        parent.to_path_buf(),
        CString::new(name.as_bytes()).map_err(|_| invalid())?,
    ))
}

fn path_cstring(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes()).map_err(io::Error::other)
}

fn dir_flags() -> libc::c_int {
    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC
}

fn kind_mode(kind: SyntheticMountTargetKind) -> libc::mode_t {
    match kind {
        SyntheticMountTargetKind::EmptyDirectory => libc::S_IFDIR,
        SyntheticMountTargetKind::EmptyFile => libc::S_IFREG,
    }
}

fn openat(dir: RawFd, name: &CStr, flags: libc::c_int, mode: libc::mode_t) -> io::Result<OwnedFd> {
    // SAFETY: valid directory descriptor (or AT_FDCWD) and NUL-terminated name.
    let fd = unsafe { libc::openat(dir, name.as_ptr(), flags, mode as libc::c_uint) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: openat returned a new owned descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Opens a target's parent. Symlinks in the parent path were validated by the
/// planner; the final entry itself is always opened relative to this fd.
fn open_directory(path: &Path) -> io::Result<OwnedFd> {
    let path = path_cstring(path)?;
    openat(
        libc::AT_FDCWD,
        &path,
        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        0,
    )
}

/// Opens a target as an `O_PATH` descriptor: it pins the inode without read
/// access, never follows a final symlink, and works with `fstat` and
/// `name_to_handle_at(AT_EMPTY_PATH)`.
fn open_entry(
    parent: BorrowedFd<'_>,
    name: &CStr,
    kind: SyntheticMountTargetKind,
) -> io::Result<OwnedFd> {
    let flags = match kind {
        SyntheticMountTargetKind::EmptyDirectory => {
            libc::O_PATH | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC
        }
        SyntheticMountTargetKind::EmptyFile => libc::O_PATH | libc::O_NOFOLLOW | libc::O_CLOEXEC,
    };
    let fd = openat(parent.as_raw_fd(), name, flags, 0)?;
    if fstat(fd.as_fd())?.st_mode & libc::S_IFMT != kind_mode(kind) {
        return Err(io::Error::from_raw_os_error(libc::ENOTDIR));
    }
    Ok(fd)
}

fn create_staging(
    parent: BorrowedFd<'_>,
    name: &CStr,
    kind: SyntheticMountTargetKind,
) -> io::Result<OwnedFd> {
    match kind {
        SyntheticMountTargetKind::EmptyDirectory => {
            // Same mode bubblewrap's ensure_dir would have used.
            // SAFETY: valid descriptor and name.
            if unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o755) } < 0 {
                return Err(io::Error::last_os_error());
            }
            open_entry(parent, name, kind)
        }
        SyntheticMountTargetKind::EmptyFile => {
            // Same mode bubblewrap's ensure_file would have used.
            let created = openat(
                parent.as_raw_fd(),
                name,
                libc::O_RDONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                0o444,
            )?;
            let held = open_entry(parent, name, kind)?;
            let (created, reopened) = (fstat(created.as_fd())?, fstat(held.as_fd())?);
            if (created.st_dev, created.st_ino) != (reopened.st_dev, reopened.st_ino) {
                return Err(io::Error::other(
                    "staging entry was replaced while opening it",
                ));
            }
            Ok(held)
        }
    }
}

fn rename_noreplace(parent: BorrowedFd<'_>, from: &CStr, to: &CStr) -> io::Result<()> {
    // SAFETY: valid descriptors and names. EINVAL (no RENAME_NOREPLACE support)
    // fails closed; there is no fallback to a replacing rename.
    let rc = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            parent.as_raw_fd(),
            from.as_ptr(),
            parent.as_raw_fd(),
            to.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn fstat(fd: BorrowedFd<'_>) -> io::Result<libc::stat> {
    // SAFETY: zeroed stat is a valid out-parameter.
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: valid descriptor and out-parameter.
    if unsafe { libc::fstat(fd.as_raw_fd(), &mut stat) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(stat)
}

fn fstatat_nofollow(parent: BorrowedFd<'_>, name: &CStr) -> io::Result<libc::stat> {
    // SAFETY: zeroed stat is a valid out-parameter.
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: valid descriptor, name and out-parameter.
    if unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            name.as_ptr(),
            &mut stat,
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } < 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(stat)
}

fn is_absent(result: io::Result<libc::stat>) -> io::Result<bool> {
    match result {
        Ok(_) => Ok(false),
        Err(err) if err.raw_os_error() == Some(libc::ENOENT) => Ok(true),
        Err(err) => Err(err),
    }
}

fn same_entry(parent: BorrowedFd<'_>, name: &CStr, held: &libc::stat) -> io::Result<bool> {
    match fstatat_nofollow(parent, name) {
        Ok(stat) => Ok((stat.st_dev, stat.st_ino) == (held.st_dev, held.st_ino)),
        Err(err) if err.raw_os_error() == Some(libc::ENOENT) => Ok(false),
        Err(err) => Err(err),
    }
}

/// Removes one verified entry from its verified parent, never recursively.
/// Returns false when the object is preserved (non-empty or busy).
fn remove_entry(
    parent: BorrowedFd<'_>,
    name: &CStr,
    kind: SyntheticMountTargetKind,
    held: &libc::stat,
) -> io::Result<bool> {
    let flags = match kind {
        // rmdir is atomic about emptiness.
        SyntheticMountTargetKind::EmptyDirectory => libc::AT_REMOVEDIR,
        SyntheticMountTargetKind::EmptyFile => {
            if held.st_size != 0 {
                return Ok(false);
            }
            0
        }
    };
    // SAFETY: valid descriptor and name.
    if unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), flags) } == 0 {
        return Ok(true);
    }
    let err = io::Error::last_os_error();
    match err.raw_os_error() {
        Some(libc::ENOENT) => Ok(true),
        Some(libc::ENOTEMPTY) | Some(libc::EEXIST) | Some(libc::EBUSY) => Ok(false),
        _ => Err(err),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn unhex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(text.get(index..index + 2)?, 16).ok())
        .collect()
}

#[cfg(test)]
#[path = "synthetic_targets_tests.rs"]
mod tests;
