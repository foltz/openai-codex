//! Server-observed Unix peer executable identity.
//!
//! This deliberately resolves and compares a file identity while the accepted
//! connection is still current. It never preserves a PID as entitlement.

use codex_uds::UnixStream;
use std::fs::Metadata;
use std::io;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PeerExecutableIdentity {
    device: u64,
    inode: u64,
}

impl PeerExecutableIdentity {
    /// Captures the daemon image before its listener is published.
    pub(crate) fn capture_running_process() -> io::Result<Self> {
        platform::running_process_identity()
    }

    #[cfg(test)]
    pub fn current_process() -> io::Result<Self> {
        Self::capture_running_process()
    }

    #[cfg(unix)]
    pub(crate) fn from_unix_stream(stream: &UnixStream) -> io::Result<Option<Self>> {
        platform::peer_executable_identity(stream)
    }

    #[cfg(not(unix))]
    pub(crate) fn from_unix_stream(_stream: &UnixStream) -> io::Result<Option<Self>> {
        Ok(None)
    }

    #[cfg(unix)]
    pub(crate) fn from_metadata(metadata: Metadata) -> io::Result<Self> {
        use std::os::unix::fs::MetadataExt;

        Ok(Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }

    #[cfg(not(unix))]
    pub(crate) fn from_metadata(_metadata: Metadata) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "executable identity is unsupported on this platform",
        ))
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
mod platform {
    use super::PeerExecutableIdentity;
    use codex_uds::UnixStream;
    use std::io;
    use std::os::fd::AsRawFd;

    pub(super) fn running_process_identity() -> io::Result<PeerExecutableIdentity> {
        PeerExecutableIdentity::from_metadata(std::fs::metadata("/proc/self/exe")?)
    }

    pub(super) fn peer_executable_identity(
        stream: &UnixStream,
    ) -> io::Result<Option<PeerExecutableIdentity>> {
        let mut credentials = unsafe { std::mem::zeroed::<libc::ucred>() };
        let mut credentials_len: libc::socklen_t =
            std::mem::size_of::<libc::ucred>().try_into().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid peer credential length",
                )
            })?;
        let result = unsafe {
            libc::getsockopt(
                stream.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                &mut credentials as *mut _ as *mut libc::c_void,
                &mut credentials_len,
            )
        };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }

        // Open the procfs executable before inspecting it. The file handle
        // pins the object we compare, instead of resolving a pathname and
        // subsequently statting whatever has appeared there.
        let executable = std::fs::File::open(format!("/proc/{}/exe", credentials.pid))?;
        PeerExecutableIdentity::from_metadata(executable.metadata()?).map(Some)
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use super::PeerExecutableIdentity;
    use codex_uds::UnixStream;
    use std::io;

    pub(super) fn running_process_identity() -> io::Result<PeerExecutableIdentity> {
        let executable = std::env::current_exe()?;
        let image = std::fs::File::open(executable)?;
        PeerExecutableIdentity::from_metadata(image.metadata()?)
    }

    pub(super) fn peer_executable_identity(
        _stream: &UnixStream,
    ) -> io::Result<Option<PeerExecutableIdentity>> {
        // Darwin exposes a peer PID but no kernel-bound handle for its mapped
        // executable. `proc_pidpath` plus `stat` can be substituted between
        // lookup and use, so it is not entitlement evidence. Keep Darwin
        // daemon peers unproven until the transport gains a capability whose
        // possession is bound to the connecting process.
        Ok(None)
    }
}

#[cfg(all(
    unix,
    not(any(target_os = "linux", target_os = "android", target_os = "macos"))
))]
mod platform {
    use super::PeerExecutableIdentity;
    use codex_uds::UnixStream;
    use std::io;

    pub(super) fn running_process_identity() -> io::Result<PeerExecutableIdentity> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "running executable identity is unsupported on this platform",
        ))
    }

    pub(super) fn peer_executable_identity(
        _stream: &UnixStream,
    ) -> io::Result<Option<PeerExecutableIdentity>> {
        Ok(None)
    }
}

#[cfg(not(unix))]
mod platform {
    use super::PeerExecutableIdentity;
    use std::io;

    pub(super) fn running_process_identity() -> io::Result<PeerExecutableIdentity> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "running executable identity is unsupported on this platform",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::PeerExecutableIdentity;

    #[test]
    fn current_process_identity_is_available_on_supported_unix() {
        #[cfg(unix)]
        assert!(PeerExecutableIdentity::current_process().is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn different_executable_has_a_different_identity() {
        let current =
            PeerExecutableIdentity::current_process().expect("current executable identity");
        let other = ["/bin/sh", "/bin/ls"]
            .into_iter()
            .find_map(|path| std::fs::metadata(path).ok())
            .and_then(|metadata| PeerExecutableIdentity::from_metadata(metadata).ok())
            .expect("shell executable identity");
        assert_ne!(current, other);
    }
}
