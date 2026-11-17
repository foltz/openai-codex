//! Serializes cooperating processes' credential commits, including compare-and-save.
//! The stable sidecar is never renamed or removed; network requests never hold it.

use super::*;
use std::fs::OpenOptions;
use std::fs::TryLockError;
use std::time::Duration;
use std::time::Instant;

pub(super) fn wrap(
    inner: Arc<dyn AuthStorageBackend>,
    home: PathBuf,
    mode: AuthCredentialsStoreMode,
) -> Arc<dyn AuthStorageBackend> {
    if mode == AuthCredentialsStoreMode::Ephemeral {
        return inner;
    }
    Arc::new(CommittedStorage { inner, home, mode })
}

#[derive(Debug)]
struct CommittedStorage {
    inner: Arc<dyn AuthStorageBackend>,
    home: PathBuf,
    mode: AuthCredentialsStoreMode,
}

impl CommittedStorage {
    fn refuse_external_owner(&self) -> std::io::Result<()> {
        if self.mode == AuthCredentialsStoreMode::Keyring {
            super::super::command_store::refuse_keyring_native_write(&self.home)
        } else {
            super::super::command_store::refuse_native_write(&self.home)
        }
    }

    fn lock(&self) -> std::io::Result<File> {
        let auth_file = get_auth_file(&self.home);
        // Keyring commits can unlink an intermediate symlink used by another
        // home, so resolvable chains must share the target's commit lock.
        let path = match storage_file::resolve_auth_file(auth_file.clone()) {
            Ok(path) => path,
            // Unresolvable fallback links can still be removed by keyring commits.
            Err(_) if self.mode == AuthCredentialsStoreMode::Keyring => auth_file,
            Err(error) => return Err(error),
        }
        .with_extension("json.lock");
        std::fs::create_dir_all(
            path.parent()
                .ok_or_else(|| std::io::Error::other("credential lock has no parent"))?,
        )?;
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(path)?;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match file.try_lock() {
                Ok(()) => return Ok(file),
                Err(TryLockError::WouldBlock) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10))
                }
                Err(TryLockError::WouldBlock) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "credential commit lock timed out",
                    ));
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
}

// The ordinary writer above remains unchanged. Managed reads must use exactly
// that writer's resolved lock domain, with an immediate contention refusal.
pub(super) fn managed_lock_path(
    home: &Path,
    mode: AuthCredentialsStoreMode,
) -> std::io::Result<PathBuf> {
    let auth_file = get_auth_file(home);
    let path = match storage_file::resolve_auth_file(auth_file.clone()) {
        Ok(path) => path,
        Err(_) if mode == AuthCredentialsStoreMode::Keyring => auth_file,
        Err(error) => return Err(error),
    }
    .with_extension("json.lock");
    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other("credential lock has no parent"))?;
    Ok(parent.canonicalize()?.join(
        path.file_name()
            .ok_or_else(|| std::io::Error::other("credential lock has no name"))?,
    ))
}

fn open_managed_lock(path: &Path, home: &Path) -> Result<File, ManagedAuthStorageError> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata = std::fs::symlink_metadata(path).map_err(managed_coordination_error)?;
            validate_managed_lock_metadata(&metadata, home)?;
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .map_err(managed_coordination_error)?
        }
        Err(error) => return Err(managed_coordination_error(error)),
    };
    validate_managed_lock(path, &file, home)?;
    Ok(file)
}

/// Owned synchronous guard for command-provider durable/cache commits.
/// Uses the ordinary File writer's exact resolved lock, never a second domain.
/// Advisory locking does not exclude uncooperative filesystem mutations.
#[must_use]
pub(in crate::auth) struct CommandAuthSourceGuard {
    file: File,
    home: PathBuf,
    lock_path: PathBuf,
}

impl Debug for CommandAuthSourceGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CommandAuthSourceGuard([locked])")
    }
}

impl CommandAuthSourceGuard {
    /// Revalidate the selected physical domain and the held descriptor before
    /// each synchronous publication. No credential or provider is resolved here.
    pub(in crate::auth) fn require_current_domain(&self) -> Result<(), ManagedAuthStorageError> {
        let current = managed_lock_path(&self.home, AuthCredentialsStoreMode::File)
            .map_err(managed_coordination_error)?;
        if current != self.lock_path {
            return Err(managed_coordination_error(
                std::io::ErrorKind::InvalidData.into(),
            ));
        }
        validate_managed_lock(&self.lock_path, &self.file, &self.home)
    }
}

/// An immediate contention refusal. A bounded caller may retry acquisition;
/// it must never hold this synchronous guard across external command work.
pub(in crate::auth) fn acquire_command_auth_source(
    home: &Path,
) -> Result<CommandAuthSourceGuard, ManagedAuthStorageError> {
    // Do not turn an unavailable home into a new empty auth authority.
    home.canonicalize().map_err(managed_coordination_error)?;
    let path = managed_lock_path(home, AuthCredentialsStoreMode::File)
        .map_err(managed_coordination_error)?;
    let file = open_managed_lock(&path, home)?;
    file.try_lock().map_err(|error| match error {
        TryLockError::WouldBlock => {
            ManagedAuthStorageError::ReadFailed(ManagedAuthStorageFailure::CoordinationContended)
        }
        TryLockError::Error(error) => managed_coordination_error(error),
    })?;
    let guard = CommandAuthSourceGuard {
        file,
        home: home.to_path_buf(),
        lock_path: path,
    };
    guard.require_current_domain()?;
    Ok(guard)
}

impl AuthStorageBackend for CommittedStorage {
    fn persistent_home(&self) -> Option<&Path> {
        self.inner.persistent_home()
    }

    fn lock_managed_source(&self) -> Result<ManagedAuthSourceGuard<'_>, ManagedAuthStorageError> {
        // A failed lookup is not an absent authority and must not create a home.
        self.home
            .canonicalize()
            .map_err(managed_coordination_error)?;
        let path = managed_lock_path(&self.home, self.mode).map_err(managed_coordination_error)?;
        let file = open_managed_lock(&path, &self.home)?;
        file.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => ManagedAuthStorageError::ReadFailed(
                ManagedAuthStorageFailure::CoordinationContended,
            ),
            TryLockError::Error(error) => managed_coordination_error(error),
        })?;
        let guard = ManagedAuthSourceGuard {
            file,
            backend: self.inner.as_ref(),
            home: self.home.clone(),
            mode: self.mode,
            lock_path: path,
        };
        guard.require_current_domain()?;
        Ok(guard)
    }

    fn load_managed(&self) -> Result<ManagedAuthStorageRead, ManagedAuthStorageError> {
        let guard = self.lock_managed_source()?;
        let read = self.inner.load_managed()?;
        guard.require_current_domain()?;
        Ok(read)
    }

    fn read_managed_bytes(&self) -> Result<ManagedAuthStorageBytes, ManagedAuthStorageError> {
        let guard = self.lock_managed_source()?;
        let read = self.inner.read_managed_bytes()?;
        guard.require_current_domain()?;
        Ok(read)
    }

    fn verify_managed_preimage(
        &self,
        expected: &ManagedAuthStoragePreimage,
    ) -> Result<bool, ManagedAuthStorageError> {
        self.lock_managed_source()?
            .verify_managed_preimage(expected)
    }

    fn load(&self) -> std::io::Result<Option<AuthDotJson>> {
        self.inner.load()
    }
    fn load_account_grant(&self) -> std::io::Result<Option<AuthDotJson>> {
        self.inner.load_account_grant()
    }
    fn save(&self, auth: &AuthDotJson) -> std::io::Result<()> {
        let _guard = self.lock()?;
        self.refuse_external_owner()?;
        self.inner.save(auth)
    }
    fn compare_and_save(&self, expected: &AuthDotJson, auth: &AuthDotJson) -> std::io::Result<()> {
        let _guard = self.lock()?;
        self.refuse_external_owner()?;
        self.inner.compare_and_save(expected, auth)
    }
    fn delete(&self) -> std::io::Result<bool> {
        // A missing file-store directory contains no credentials to delete.
        // Linearize this no-op before any concurrent creator, without creating
        // a directory just to leave a lock behind. Secure stores may still have
        // credentials even when the directory is absent and must take the lock.
        if self.mode == AuthCredentialsStoreMode::File && !self.home.try_exists()? {
            return Ok(false);
        }
        let _guard = self.lock()?;
        self.inner.delete()
    }
}

#[cfg(test)]
#[path = "storage_commit_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "storage_guard_tests.rs"]
mod guard_tests;

#[cfg(test)]
#[path = "command_source_lock_tests.rs"]
mod command_source_lock_tests;
