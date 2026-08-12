//! Server-observed Unix peer executable identity.
//!
//! Linux compares an opened executable's file identity while the accepted
//! connection is still current. Darwin compares the audit-token-bound peer's
//! exact static-code identifier with the daemon's captured code-directory
//! hash; neither path keeps a PID as entitlement.

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
    /// Darwin's Security framework derives this exact static-code identifier
    /// from the process described by an audit token. It is not a designated
    /// requirement, so a different release signed by the same identity does
    /// not match. Unlike `proc_pidpath`, it never resolves a mutable pathname
    /// after accepting the peer.
    #[cfg(target_os = "macos")]
    ExactCodeDirectoryHash {
        bytes: [u8; MAX_CODE_DIRECTORY_HASH_BYTES],
        length: u8,
    },
}

#[cfg(target_os = "macos")]
const MAX_CODE_DIRECTORY_HASH_BYTES: usize = 64;

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
    use super::MAX_CODE_DIRECTORY_HASH_BYTES;
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
        fn SecCodeCheckValidity(code: CfRef, flags: u32, requirement: CfRef) -> Status;
        fn SecCodeCopySigningInformation(
            code: CfRef,
            flags: u32,
            information: *mut CfRef,
        ) -> Status;
        static kSecCodeInfoUnique: CfRef;
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
        fn CFDictionaryGetValue(dictionary: CfRef, key: CfRef) -> CfRef;
        fn CFDataGetLength(data: CfRef) -> isize;
        fn CFDataGetBytePtr(data: CfRef) -> *const u8;
        fn CFRelease(value: CfRef);
    }

    pub(super) fn running_process_identity() -> io::Result<PeerExecutableIdentity> {
        let mut code = std::ptr::null();
        let status = unsafe { SecCodeCopySelf(0, &mut code) };
        if status != 0 {
            return Err(io::Error::from_raw_os_error(status));
        }
        let identity = exact_static_code_identity(code);
        unsafe { CFRelease(code) };
        identity
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
        peer_exact_static_code_identity(&token).map(Some)
    }

    fn peer_exact_static_code_identity(token: &AuditToken) -> io::Result<PeerExecutableIdentity> {
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
        let mut peer_code = std::ptr::null();
        let status = unsafe {
            SecCodeCopyGuestWithAttributes(std::ptr::null(), attributes, 0, &mut peer_code)
        };
        let identity = if status == 0 {
            exact_static_code_identity(peer_code)
        } else {
            Err(io::Error::from_raw_os_error(status))
        };
        for value in [peer_code, attributes, data] {
            if !value.is_null() {
                unsafe { CFRelease(value) }
            }
        }
        identity
    }

    /// `kSecCodeInfoUnique` is Security.framework's stable binary identifier
    /// for this exact static code, rather than its cross-version signing
    /// identity. Validating the dynamic `SecCode` first binds the read to the
    /// process represented by `SecCodeCopySelf` or `LOCAL_PEERTOKEN`.
    fn exact_static_code_identity(code: CfRef) -> io::Result<PeerExecutableIdentity> {
        let status = unsafe { SecCodeCheckValidity(code, 0, std::ptr::null()) };
        if status != 0 {
            return Err(io::Error::from_raw_os_error(status));
        }
        let mut information = std::ptr::null();
        let status = unsafe { SecCodeCopySigningInformation(code, 0, &mut information) };
        if status != 0 {
            return Err(io::Error::from_raw_os_error(status));
        }
        let identity = (|| {
            let unique = unsafe { CFDictionaryGetValue(information, kSecCodeInfoUnique) };
            if unique.is_null() {
                return Err(io::Error::other(
                    "signed code has no static-code identifier",
                ));
            }
            let length = unsafe { CFDataGetLength(unique) };
            if length <= 0 || length as usize > MAX_CODE_DIRECTORY_HASH_BYTES {
                return Err(io::Error::other("invalid static-code identifier length"));
            }
            let source = unsafe { CFDataGetBytePtr(unique) };
            if source.is_null() {
                return Err(io::Error::other("static-code identifier bytes unavailable"));
            }
            let mut bytes = [0; MAX_CODE_DIRECTORY_HASH_BYTES];
            unsafe {
                std::ptr::copy_nonoverlapping(source, bytes.as_mut_ptr(), length as usize);
            }
            Ok(PeerExecutableIdentity::ExactCodeDirectoryHash {
                bytes,
                length: length as u8,
            })
        })();
        unsafe { CFRelease(information) };
        identity
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
