//! The explicit forwarding map for descriptors a sandboxed command must
//! inherit through the Linux sandbox launcher.
//!
//! A spawner names each such descriptor, before it is merged with the
//! launch's cleanup-custody descriptors, by number, role and identity
//! (`dev:ino`). The launcher validates the whole list before it takes
//! ownership of anything and forwards exactly these descriptors, at the same
//! numbers, into the sandboxed command. On the keeper path, unlisted
//! inherited descriptors are closed. Nothing is forwarded on the strength of
//! an environment variable or an argv mention alone.

use std::io;

use crate::SandboxType;

/// The launcher flag, repeated once per forwarded descriptor:
/// `--forward-fd <fd>:<role>:<dev>:<ino>`.
pub const FORWARD_FD_ARG: &str = "--forward-fd";

/// Why a descriptor is forwarded. The launcher checks the descriptor's type
/// against its role and refuses a mismatch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForwardedFdRole {
    /// A protected shell-snapshot replay source (`. /dev/fd/<fd>`): a
    /// read-only regular file.
    SnapshotSource,
    /// A shell-snapshot descriptor-path probe (`</dev/fd/<fd>`): a read-only
    /// regular file.
    SnapshotProbe,
}

impl ForwardedFdRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SnapshotSource => "snapshot-source",
            Self::SnapshotProbe => "snapshot-probe",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "snapshot-source" => Some(Self::SnapshotSource),
            "snapshot-probe" => Some(Self::SnapshotProbe),
            _ => None,
        }
    }
}

/// One descriptor the sandboxed command must inherit, at this number.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ForwardedFd {
    /// The descriptor number (`RawFd` on Unix).
    pub fd: i32,
    pub role: ForwardedFdRole,
}

impl ForwardedFd {
    pub fn new(fd: i32, role: ForwardedFdRole) -> Self {
        Self { fd, role }
    }
}

/// Inserts one [`FORWARD_FD_ARG`] per descriptor before the launcher argv's
/// `--`, with the identity each descriptor has now. Only a Linux launcher
/// argv (`SandboxType::LinuxSeccomp`) gets them, whether or not the launch
/// has cleanup custody; any other argv is left unchanged.
pub fn attach_forwarded_fds(
    sandbox: SandboxType,
    argv: &mut Vec<String>,
    fds: &[ForwardedFd],
) -> io::Result<()> {
    if sandbox != SandboxType::LinuxSeccomp || fds.is_empty() {
        return Ok(());
    }
    attach_forwarded_fds_to_launcher_argv(argv, fds)
}

#[cfg(unix)]
fn attach_forwarded_fds_to_launcher_argv(
    argv: &mut Vec<String>,
    fds: &[ForwardedFd],
) -> io::Result<()> {
    let separator = argv
        .iter()
        .position(|arg| arg == "--")
        .ok_or_else(|| io::Error::other("launcher argv has no `--`"))?;
    let mut forwarded = Vec::with_capacity(fds.len() * 2);
    for forward in fds {
        // SAFETY: zeroed stat is a valid out-parameter.
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        // SAFETY: fstat only reads the descriptor's metadata.
        if unsafe { libc::fstat(forward.fd, &mut stat) } < 0 {
            return Err(io::Error::other(format!(
                "cannot forward descriptor {} ({}): {}",
                forward.fd,
                forward.role.as_str(),
                io::Error::last_os_error()
            )));
        }
        forwarded.push(FORWARD_FD_ARG.to_string());
        forwarded.push(format!(
            "{}:{}:{}:{}",
            forward.fd,
            forward.role.as_str(),
            stat.st_dev,
            stat.st_ino
        ));
    }
    argv.splice(separator..separator, forwarded);
    Ok(())
}

#[cfg(not(unix))]
fn attach_forwarded_fds_to_launcher_argv(
    argv: &mut Vec<String>,
    fds: &[ForwardedFd],
) -> io::Result<()> {
    let _ = (argv, fds);
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "Linux command descriptor forwarding requires Unix descriptors",
    ))
}

#[cfg(test)]
#[path = "forwarded_fds_tests.rs"]
mod tests;
