//! Spawn identity custody; pidfd claims are Linux-only.
#[cfg(unix)]
use crate::{SpawnedProcess, TerminalSize};
#[cfg(unix)]
use std::collections::HashMap;
#[cfg(unix)]
use std::path::Path;
#[cfg(unix)]
use std::time::Duration;

#[cfg(unix)]
async fn assert_single_take_pin(spawned: SpawnedProcess) -> anyhow::Result<()> {
    assert!(spawned.session.pid().is_some());
    let pin = spawned
        .session
        .take_launcher_pin()
        .expect("local spawn records its pin attempt");
    assert!(spawned.session.take_launcher_pin().is_none());
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), spawned.exit_rx).await??,
        0
    );
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let fd = pin.into_pidfd().expect("Linux pin must predate reaping");
        let mut poll = libc::pollfd {
            fd: fd.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        assert_eq!(unsafe { libc::poll(&mut poll, 1, 0) }, 1);
    }
    #[cfg(not(target_os = "linux"))]
    drop(pin); // An opaque stub off Linux, not native Linux exit proof.
    Ok(())
}

#[cfg(unix)]
fn environment() -> HashMap<String, String> {
    HashMap::from([("PATH".into(), "/usr/bin:/bin".into())])
}

#[cfg(unix)]
#[tokio::test]
async fn pipe_spawn_exposes_one_pin_before_and_after_reap() -> anyhow::Result<()> {
    let spawned = crate::spawn_pipe_process_no_stdin(
        "/bin/sh",
        &["-c".into(), "exit 0".into()],
        Path::new("/"),
        &environment(),
        &None,
        &[],
    )
    .await?;
    assert_single_take_pin(spawned).await
}

#[cfg(unix)]
#[tokio::test]
async fn portable_pty_spawn_exposes_one_pin_without_changing_output_contract() -> anyhow::Result<()>
{
    let spawned = crate::spawn_pty_process(
        "/bin/sh",
        &["-c".into(), "exit 0".into()],
        Path::new("/"),
        &environment(),
        &None,
        TerminalSize::default(),
        crate::pty::ChildFds::Inherited(&[]),
    )
    .await?;
    assert_single_take_pin(spawned).await
}

#[cfg(unix)]
#[tokio::test]
async fn attached_pty_spawn_preserves_parent_descriptor_flags_and_one_pin() -> anyhow::Result<()> {
    use std::os::fd::AsRawFd;
    let attached = std::fs::File::open("/dev/null")?;
    let flags = unsafe { libc::fcntl(attached.as_raw_fd(), libc::F_GETFD) };
    assert!(flags >= 0 && flags & libc::FD_CLOEXEC != 0);
    let descriptors = [attached.as_raw_fd()];
    let spawned = crate::spawn_pty_process(
        "/bin/sh",
        &["-c".into(), "exit 0".into()],
        Path::new("/"),
        &environment(),
        &None,
        TerminalSize::default(),
        crate::pty::ChildFds::Attached(&descriptors),
    )
    .await?;
    assert_eq!(
        unsafe { libc::fcntl(attached.as_raw_fd(), libc::F_GETFD) },
        flags
    );
    assert_single_take_pin(spawned).await
}
