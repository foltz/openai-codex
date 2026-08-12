//! Server-observed Unix peer executable identity.
//!
//! Linux compares an opened executable's file identity while the accepted
//! connection is still current. Darwin validates the audit-token-bound peer
//! process against the daemon's code-signing requirement; neither path keeps
//! a PID as entitlement.

use codex_uds::UnixStream;
#[cfg(any(target_os = "linux", target_os = "android"))]
use std::fs::Metadata;
use std::io;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PeerExecutableIdentity {
    FileIdentity {
        device: u64,
        inode: u64,
    },
    /// Darwin's kernel binds this code-directory hash to the process described
    /// by an audit token. Unlike `proc_pidpath`, it never resolves a mutable
    /// filesystem pathname after accepting the peer.
    #[cfg(target_os = "macos")]
    CodeSignedProcess,
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

    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub(crate) fn from_metadata(metadata: Metadata) -> io::Result<Self> {
        use std::os::unix::fs::MetadataExt;

        Ok(Self::FileIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
mod platform {
    use super::PeerExecutableIdentity;
    use codex_uds::UnixStream;
    use std::fs::Metadata;
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
    use std::os::fd::AsRawFd;

    const AUDIT_TOKEN_WORDS: usize = 8;
    type AuditToken = [u32; AUDIT_TOKEN_WORDS];
    type CfRef = *const libc::c_void;
    type Status = i32;

    #[link(name = "Security", kind = "framework")]
    unsafe extern "C" {
        static kSecGuestAttributeAudit: CfRef;
        fn SecCodeCopySelf(flags: u32, code: *mut CfRef) -> Status;
        fn SecCodeCopyGuestWithAttributes(
            host: CfRef,
            attributes: CfRef,
            flags: u32,
            code: *mut CfRef,
        ) -> Status;
        fn SecCodeCopyDesignatedRequirement(
            code: CfRef,
            flags: u32,
            requirement: *mut CfRef,
        ) -> Status;
        fn SecCodeCheckValidity(code: CfRef, flags: u32, requirement: CfRef) -> Status;
    }
    #[link(name = "CoreFoundation", kind = "framework")]
    unsafe extern "C" {
        fn CFDataCreate(allocator: CfRef, bytes: *const u8, length: isize) -> CfRef;
        fn CFDictionaryCreateMutable(
            allocator: CfRef,
            capacity: isize,
            keys: *const libc::c_void,
            values: *const libc::c_void,
        ) -> CfRef;
        fn CFDictionarySetValue(dictionary: CfRef, key: CfRef, value: CfRef);
        fn CFRelease(value: CfRef);
    }

    pub(super) fn running_process_identity() -> io::Result<PeerExecutableIdentity> {
        let mut code = std::ptr::null();
        let status = unsafe { SecCodeCopySelf(0, &mut code) };
        if status != 0 {
            return Err(io::Error::from_raw_os_error(status));
        }
        unsafe { CFRelease(code) };
        Ok(PeerExecutableIdentity::CodeSignedProcess)
    }

    pub(super) fn peer_executable_identity(
        stream: &UnixStream,
    ) -> io::Result<Option<PeerExecutableIdentity>> {
        let mut token = [0_u32; AUDIT_TOKEN_WORDS];
        let mut token_len: libc::socklen_t =
            std::mem::size_of::<AuditToken>().try_into().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "invalid audit token length")
            })?;
        let result = unsafe {
            libc::getsockopt(
                stream.as_raw_fd(),
                libc::SOL_LOCAL,
                libc::LOCAL_PEERTOKEN,
                token.as_mut_ptr().cast(),
                &mut token_len,
            )
        };
        if result != 0 || token_len as usize != std::mem::size_of::<AuditToken>() {
            return Err(io::Error::last_os_error());
        }
        peer_matches_running_code(&token)
            .map(|matches| matches.then_some(PeerExecutableIdentity::CodeSignedProcess))
    }

    fn peer_matches_running_code(token: &AuditToken) -> io::Result<bool> {
        let data = unsafe {
            CFDataCreate(
                std::ptr::null(),
                token.as_ptr().cast(),
                std::mem::size_of::<AuditToken>() as isize,
            )
        };
        let attributes = unsafe {
            CFDictionaryCreateMutable(std::ptr::null(), 1, std::ptr::null(), std::ptr::null())
        };
        if data.is_null() || attributes.is_null() {
            if !data.is_null() {
                unsafe { CFRelease(data) };
            }
            if !attributes.is_null() {
                unsafe { CFRelease(attributes) };
            }
            return Err(io::Error::other(
                "could not construct audit-token attributes",
            ));
        }
        unsafe { CFDictionarySetValue(attributes, kSecGuestAttributeAudit, data) };
        let (mut self_code, mut peer_code, mut requirement) =
            (std::ptr::null(), std::ptr::null(), std::ptr::null());
        let status = unsafe { SecCodeCopySelf(0, &mut self_code) };
        let status = if status == 0 {
            unsafe {
                SecCodeCopyGuestWithAttributes(std::ptr::null(), attributes, 0, &mut peer_code)
            }
        } else {
            status
        };
        let status = if status == 0 {
            unsafe { SecCodeCopyDesignatedRequirement(self_code, 0, &mut requirement) }
        } else {
            status
        };
        let status = if status == 0 {
            unsafe { SecCodeCheckValidity(peer_code, 0, requirement) }
        } else {
            status
        };
        for value in [requirement, peer_code, self_code, attributes, data] {
            if !value.is_null() {
                unsafe { CFRelease(value) }
            }
        }
        if status == 0 {
            Ok(true)
        } else {
            Err(io::Error::from_raw_os_error(status))
        }
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

    #[cfg(any(target_os = "linux", target_os = "android"))]
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
