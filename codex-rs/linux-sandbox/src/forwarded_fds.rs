//! The launcher's side of the explicit forwarding map
//! (`codex_sandboxing::FORWARD_FD_ARG`): every descriptor the sandboxed
//! command must inherit is named by number, role and identity. The whole list
//! is validated before anything is owned; then each descriptor is marked
//! close-on-exec and owned exactly once, at the same number. Only the process
//! that execs the command onward (bubblewrap's child, through its preserved
//! files, or the launcher right before a direct exec) makes it inheritable
//! again, so no other child of the launcher (the proc preflight, `bwrap
//! --help`) inherits it. Every other inherited descriptor stays unlisted and
//! is closed by the keeper.

use std::collections::HashSet;
use std::fs::File;
use std::os::fd::FromRawFd;
use std::os::fd::RawFd;

use codex_sandboxing::ForwardedFdRole;

/// One validated `<fd>:<role>:<dev>:<ino>` entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ForwardSpec {
    pub(crate) fd: RawFd,
    pub(crate) role: ForwardedFdRole,
    dev: u64,
    ino: u64,
}

fn parse_forward_spec(text: &str) -> Result<ForwardSpec, String> {
    let invalid = || format!("invalid --forward-fd {text:?}: expected <fd>:<role>:<dev>:<ino>");
    let mut parts = text.split(':');
    let (Some(fd), Some(role), Some(dev), Some(ino), None) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) else {
        return Err(invalid());
    };
    let fd: RawFd = fd.parse().map_err(|_| invalid())?;
    let role = ForwardedFdRole::parse(role)
        .ok_or_else(|| format!("invalid --forward-fd {text:?}: unknown role {role:?}"))?;
    let dev: u64 = dev.parse().map_err(|_| invalid())?;
    let ino: u64 = ino.parse().map_err(|_| invalid())?;
    Ok(ForwardSpec { fd, role, dev, ino })
}

/// `fstat` of an open descriptor; `None` when it is not open.
fn stat_of(fd: RawFd) -> Option<libc::stat> {
    // SAFETY: F_GETFD only reads the descriptor's flags.
    if unsafe { libc::fcntl(fd, libc::F_GETFD) } < 0 {
        return None;
    }
    // SAFETY: zeroed stat is a valid out-parameter.
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: valid descriptor and out-parameter.
    (unsafe { libc::fstat(fd, &mut stat) } == 0).then_some(stat)
}

/// The type each role requires, checked on the open descriptor.
fn check_role(spec: &ForwardSpec, stat: &libc::stat) -> Result<(), String> {
    let kind = stat.st_mode & libc::S_IFMT;
    match spec.role {
        ForwardedFdRole::SnapshotSource | ForwardedFdRole::SnapshotProbe => {
            // SAFETY: F_GETFL only reads the descriptor's status flags.
            let flags = unsafe { libc::fcntl(spec.fd, libc::F_GETFL) };
            if kind != libc::S_IFREG
                || stat.st_nlink != 0
                || spec.fd < 10
                || flags < 0
                || flags & libc::O_ACCMODE != libc::O_RDONLY
            {
                return Err(format!(
                    "--forward-fd {} ({}) is not an unlinked read-only snapshot carrier at fd >= 10",
                    spec.fd,
                    spec.role.as_str()
                ));
            }
        }
    }
    Ok(())
}

/// Validates the whole list, without taking ownership of anything:
///
/// - every entry parses, names an open descriptor above stdio, and has the
///   identity (`dev:ino`) it was named with;
/// - its type is the one its role requires;
/// - it is no custody descriptor, by number or as an alias of one (same
///   type, device and inode, e.g. a `dup` of the report pipe);
/// - no number and no identity is named twice.
pub(crate) fn validate_forwarded_fds(
    texts: &[String],
    custody_fds: &[RawFd],
) -> Result<Vec<ForwardSpec>, String> {
    let custody: Vec<(RawFd, libc::mode_t, u64, u64)> = custody_fds
        .iter()
        .filter_map(|fd| {
            stat_of(*fd).map(|stat| (*fd, stat.st_mode & libc::S_IFMT, stat.st_dev, stat.st_ino))
        })
        .collect();
    let mut numbers = HashSet::new();
    let mut identities = HashSet::new();
    let mut specs = Vec::with_capacity(texts.len());
    for text in texts {
        let spec = parse_forward_spec(text)?;
        if spec.fd <= libc::STDERR_FILENO {
            return Err(format!(
                "--forward-fd {} collides with standard I/O",
                spec.fd
            ));
        }
        let stat = stat_of(spec.fd)
            .ok_or_else(|| format!("--forward-fd {} is not an open descriptor", spec.fd))?;
        if stat.st_dev != spec.dev || stat.st_ino != spec.ino {
            return Err(format!(
                "--forward-fd {} is not the descriptor it was named as ({}:{})",
                spec.fd, spec.dev, spec.ino
            ));
        }
        let kind = stat.st_mode & libc::S_IFMT;
        if custody.iter().any(|(fd, custody_kind, dev, ino)| {
            *fd == spec.fd || (*custody_kind == kind && *dev == spec.dev && *ino == spec.ino)
        }) {
            return Err(format!(
                "--forward-fd {} is a cleanup custody descriptor",
                spec.fd
            ));
        }
        if !numbers.insert(spec.fd) || !identities.insert((spec.dev, spec.ino)) {
            return Err(format!("--forward-fd {} is named twice", spec.fd));
        }
        check_role(&spec, &stat)?;
        specs.push(spec);
    }
    Ok(specs)
}

/// Validates the whole list, then marks every entry close-on-exec and takes
/// ownership of each exactly once. Nothing is owned when validation fails.
pub(crate) fn adopt_forwarded_fds(
    texts: &[String],
    custody_fds: &[RawFd],
) -> Result<Vec<File>, String> {
    let specs = validate_forwarded_fds(texts, custody_fds)?;
    for spec in &specs {
        // SAFETY: F_GETFD/F_SETFD only change this descriptor's flags.
        let flags = unsafe { libc::fcntl(spec.fd, libc::F_GETFD) };
        if flags < 0 || unsafe { libc::fcntl(spec.fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0
        {
            return Err(format!(
                "--forward-fd {} cannot be marked close-on-exec: {}",
                spec.fd,
                std::io::Error::last_os_error()
            ));
        }
    }
    Ok(specs
        .iter()
        // SAFETY: validated open, named once and owned by nothing else in
        // this process (the spawner passed it to this launcher).
        .map(|spec| unsafe { File::from_raw_fd(spec.fd) })
        .collect())
}

#[cfg(test)]
#[path = "forwarded_fds_tests.rs"]
mod tests;
