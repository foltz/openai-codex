//! Synchronous managed-worker command ownership. The unreaped direct child is
//! the process-group identity lease: never signal a numeric group after wait.
//! This covers the command's group and pipes, not descendants that call setsid.

use crate::WorkerStop;
use std::process::Command;
use std::process::Output;
use std::time::Duration;

pub(crate) fn run_output(
    command: &mut Command,
    context: &str,
    cap: Duration,
    stop: WorkerStop,
) -> Result<Output, String> {
    if !stop.is_managed() {
        return Err(format!("{context} requires a managed process owner"));
    }
    if cap.is_zero() || stop.is_stopped() {
        return Err(format!("{context} stopped before process creation"));
    }
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    {
        supported::run_output(command, context, cap, stop)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = command;
        stop.mark_unavailable();
        Err(format!(
            "{context} managed process custody is unavailable on this platform"
        ))
    }
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
mod supported {
    use super::*;
    use std::io;
    use std::io::Read;
    use std::os::fd::AsRawFd;
    use std::os::unix::process::CommandExt;
    use std::process::Child;
    use std::process::ExitStatus;
    use std::process::Stdio;
    use std::time::Instant;

    // Cleanup stays on the same original. An expired observer may report pending
    // while this drains killed writers; this never extends an operation budget.
    const PIPE_CLEANUP_CAP: Duration = Duration::from_millis(100);
    const POLL_INTERVAL: Duration = Duration::from_millis(5);

    pub(super) struct OwnedChild {
        pub(super) child: Child,
        pub(super) stop: WorkerStop,
        pinned: bool,
    }

    impl OwnedChild {
        pub(super) fn new(child: Child, stop: WorkerStop) -> Self {
            Self {
                child,
                stop,
                pinned: true,
            }
        }

        fn exited_without_reap(&mut self) -> io::Result<bool> {
            if !self.pinned {
                return Err(io::Error::other("child identity lease already released"));
            }
            let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
            let result = unsafe {
                libc::waitid(
                    libc::P_PID,
                    self.child.id() as libc::id_t,
                    &mut info,
                    libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
                )
            };
            if result == -1 {
                let error = io::Error::last_os_error();
                if error.raw_os_error() == Some(libc::ECHILD) {
                    // An external reaper/ignored SIGCHLD invalidates custody.
                    // Never signal an identity which may already be recycled.
                    self.pinned = false;
                }
                return Err(error);
            }
            Ok(unsafe { info.si_pid() } != 0)
        }

        fn signal_group(&mut self) -> io::Result<()> {
            // Validate the still-unreaped original, not a getpgid lookup. This
            // private std Child has no Tokio reaper or concurrent wait owner.
            loop {
                match self.exited_without_reap() {
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    result => {
                        result?;
                        break;
                    }
                }
            }
            let pid = i32::try_from(self.child.id()).map_err(io::Error::other)?;
            if pid <= 0 {
                return Err(io::Error::other("invalid process group leader"));
            }
            if unsafe { libc::killpg(pid, libc::SIGKILL) } == -1 {
                let error = io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::ESRCH) {
                    return Err(error);
                }
            }
            Ok(())
        }

        fn reap(&mut self) -> io::Result<ExitStatus> {
            loop {
                match self.child.wait() {
                    Ok(status) => {
                        self.pinned = false;
                        return Ok(status);
                    }
                    Err(error) if error.raw_os_error() == Some(libc::ECHILD) => {
                        self.pinned = false;
                        self.stop.mark_unavailable();
                        return Err(error);
                    }
                    Err(_) => {
                        // Retain the actual original on unusual kernel errors;
                        // do not detach a live child and manufacture completion.
                        self.stop.mark_unavailable();
                        std::thread::sleep(POLL_INTERVAL);
                    }
                }
            }
        }
    }

    impl Drop for OwnedChild {
        fn drop(&mut self) {
            if self.pinned {
                if self.signal_group().is_err() {
                    self.stop.mark_unavailable();
                }
                if self.pinned && self.reap().is_err() {
                    self.stop.mark_unavailable();
                }
            }
        }
    }

    fn nonblocking(pipe: &impl AsRawFd) -> io::Result<()> {
        let fd = pipe.as_raw_fd();
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if flags == -1 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1
        {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn drain(pipe: &mut impl Read, bytes: &mut Vec<u8>, eof: &mut bool) -> io::Result<()> {
        if *eof {
            return Ok(());
        }
        // Fairness: output on either pipe cannot starve the other pipe, child
        // status or a newly latched owner deadline.
        let mut buffer = [0; 8192];
        for _ in 0..16 {
            match pipe.read(&mut buffer) {
                Ok(0) => {
                    *eof = true;
                    break;
                }
                Ok(size) => bytes.extend_from_slice(&buffer[..size]),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    pub(super) fn run_output(
        command: &mut Command,
        context: &str,
        cap: Duration,
        stop: WorkerStop,
    ) -> Result<Output, String> {
        command
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let child = command
            .spawn()
            .map_err(|error| format!("failed to run {context}: {error}"))?;
        let mut owned = OwnedChild::new(child, stop.clone());
        let result = (|| -> io::Result<Output> {
            let mut stdout = owned
                .child
                .stdout
                .take()
                .ok_or_else(|| io::Error::other("missing stdout pipe"))?;
            let mut stderr = owned
                .child
                .stderr
                .take()
                .ok_or_else(|| io::Error::other("missing stderr pipe"))?;
            nonblocking(&stdout)?;
            nonblocking(&stderr)?;
            let mut output = Vec::new();
            let mut diagnostic = Vec::new();
            let (mut output_eof, mut diagnostic_eof) = (false, false);
            let start = Instant::now();
            let mut stopped = false;
            let status = loop {
                drain(&mut stdout, &mut output, &mut output_eof)?;
                drain(&mut stderr, &mut diagnostic, &mut diagnostic_eof)?;
                let exited = match owned.exited_without_reap() {
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    result => result?,
                };
                stopped |= stop.is_stopped() || start.elapsed() >= cap;
                if exited || stopped {
                    // Also close any same-group pipe writers on ordinary exit,
                    // while the direct child still pins the group's identity.
                    owned.signal_group()?;
                    break owned.reap()?;
                }
                std::thread::sleep(POLL_INTERVAL);
            };
            let cleanup_start = Instant::now();
            while !output_eof || !diagnostic_eof {
                drain(&mut stdout, &mut output, &mut output_eof)?;
                drain(&mut stderr, &mut diagnostic, &mut diagnostic_eof)?;
                if output_eof && diagnostic_eof {
                    break;
                }
                if cleanup_start.elapsed() >= PIPE_CLEANUP_CAP {
                    return Err(io::Error::other(
                        "process exited but output ownership is incomplete",
                    ));
                }
                std::thread::sleep(POLL_INTERVAL);
            }
            if stopped {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "{context} stopped by its step budget or the shutdown deadline: {}",
                        String::from_utf8_lossy(&diagnostic).trim()
                    ),
                ));
            }
            Ok(Output {
                status,
                stdout: output,
                stderr: diagnostic,
            })
        })();
        match result {
            Ok(output) => Ok(output),
            Err(error) => {
                if error.kind() != io::ErrorKind::TimedOut {
                    stop.mark_unavailable();
                }
                // Drop kills/reaps any still-pinned original before returning.
                drop(owned);
                Err(format!("failed to complete {context}: {error}"))
            }
        }
    }
}

#[cfg(all(test, any(target_os = "macos", target_os = "linux")))]
mod tests {
    use super::*;
    use crate::PluginTaskRegistry;
    use crate::retirement::JoinOutcome;
    use std::io::Write;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;
    use std::sync::atomic::Ordering;

    fn probe_command(name: &str, mode: &str) -> Command {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", name, "--nocapture"])
            .env("K169_OWNED_PROCESS_PROBE", mode);
        command
    }

    #[test]
    fn owned_output_drains_both_large_pipes_without_wait_deadlock() {
        if std::env::var("K169_OWNED_PROCESS_PROBE").as_deref() == Ok("flood") {
            std::io::stdout()
                .write_all(&vec![0x93; 512 * 1024])
                .unwrap();
            std::io::stderr()
                .write_all(&vec![0x94; 512 * 1024])
                .unwrap();
            return;
        }
        let registry = PluginTaskRegistry::default();
        let output = run_output(
            &mut probe_command(
                "owned_process::tests::owned_output_drains_both_large_pipes_without_wait_deadlock",
                "flood",
            ),
            "flood",
            Duration::from_secs(2),
            registry.worker_stop(),
        )
        .unwrap();
        assert!(output.status.success());
        assert_eq!(
            output.stdout.iter().filter(|byte| **byte == 0x93).count(),
            512 * 1024
        );
        assert_eq!(
            output.stderr.iter().filter(|byte| **byte == 0x94).count(),
            512 * 1024
        );
    }

    #[tokio::test]
    async fn owned_output_expired_owner_refuses_before_child_birth() {
        let home = tempfile::tempdir().unwrap();
        let marker = home.path().join("started");
        let registry = PluginTaskRegistry::default();
        assert!(
            registry
                .shutdown_until(tokio::time::Instant::now())
                .await
                .is_clean()
        );
        let mut command = Command::new("/bin/sh");
        command
            .args(["-c", "printf started > \"$1\"", "probe"])
            .arg(&marker);
        assert!(
            run_output(
                &mut command,
                "expired",
                Duration::from_secs(2),
                registry.worker_stop()
            )
            .unwrap_err()
            .contains("before process creation")
        );
        assert!(!marker.exists());
    }

    #[test]
    fn owned_output_unmanaged_scope_refuses_before_child_birth() {
        let mut command = Command::new("/definitely/not/a/real/program");
        assert!(
            run_output(
                &mut command,
                "unmanaged",
                Duration::from_secs(2),
                WorkerStop::never()
            )
            .unwrap_err()
            .contains("requires a managed process owner")
        );
    }

    #[tokio::test]
    async fn owned_output_missing_program_has_no_child_to_retire() {
        let registry = PluginTaskRegistry::default();
        let mut command = Command::new("/definitely/not/a/real/program");
        assert!(
            run_output(
                &mut command,
                "missing",
                Duration::from_secs(2),
                registry.worker_stop()
            )
            .unwrap_err()
            .contains("failed to run missing")
        );
        assert!(
            registry
                .shutdown_until(tokio::time::Instant::now())
                .await
                .is_clean()
        );
    }

    #[tokio::test]
    async fn owned_output_late_deadline_reaps_actual_child_despite_cancelled_observer() {
        if std::env::var("K169_OWNED_PROCESS_PROBE").as_deref() == Ok("hold") {
            std::fs::write(
                std::env::var_os("K169_OWNED_PROCESS_READY").unwrap(),
                std::process::id().to_string(),
            )
            .unwrap();
            std::thread::sleep(Duration::from_secs(30));
            return;
        }
        let home = tempfile::tempdir().unwrap();
        let ready = home.path().join("ready");
        let child_ready = ready.clone();
        let registry = PluginTaskRegistry::default();
        let stop = registry.worker_stop();
        let receipt = registry.spawn_thread("owned-process-deadline-fixture", move || {
            let mut command = probe_command("owned_process::tests::owned_output_late_deadline_reaps_actual_child_despite_cancelled_observer", "hold");
            command.env("K169_OWNED_PROCESS_READY", child_ready);
            assert!(run_output(&mut command, "held", Duration::from_secs(30), stop).unwrap_err().contains("shutdown deadline"));
        }).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !ready.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let pid: i32 = std::fs::read_to_string(&ready).unwrap().parse().unwrap();
        let observer = tokio::spawn(receipt.clone());
        observer.abort();
        assert!(observer.await.unwrap_err().is_cancelled());
        let _ = registry.shutdown_until(tokio::time::Instant::now()).await;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), receipt)
                .await
                .unwrap(),
            JoinOutcome::Joined
        );
        assert!(
            registry
                .shutdown_until(tokio::time::Instant::now())
                .await
                .is_clean()
        );
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH)
        );
    }

    #[test]
    fn owned_output_step_cap_reaps_child() {
        let registry = PluginTaskRegistry::default();
        let mut command = Command::new("/bin/sleep");
        command.arg("30");
        assert!(
            run_output(
                &mut command,
                "step cap",
                Duration::from_millis(30),
                registry.worker_stop()
            )
            .unwrap_err()
            .contains("step budget")
        );
    }

    #[test]
    fn owned_output_parent_exit_cannot_leave_same_group_pipe_writer_running() {
        let registry = PluginTaskRegistry::default();
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "/bin/sleep 30 & printf parent-done; exit 0"]);
        let output = run_output(
            &mut command,
            "group writer",
            Duration::from_secs(2),
            registry.worker_stop(),
        )
        .unwrap();
        assert!(output.status.success());
        assert_eq!(output.stdout, b"parent-done");
        assert!(output.stderr.is_empty());
    }

    #[tokio::test]
    async fn owned_output_panic_still_owns_the_actual_child_join() {
        let registry = PluginTaskRegistry::default();
        let stop = registry.worker_stop();
        let pid = Arc::new(AtomicU32::new(0));
        let recorded = pid.clone();
        let receipt = registry
            .spawn_thread("owned-process-panic-fixture", move || {
                use std::os::unix::process::CommandExt;
                let mut command = Command::new("/bin/sleep");
                command.arg("30").process_group(0);
                let child = command.spawn().unwrap();
                recorded.store(child.id(), Ordering::SeqCst);
                let _original = supported::OwnedChild::new(child, stop);
                panic!("synthetic panic after actual child birth");
            })
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), receipt)
                .await
                .unwrap(),
            JoinOutcome::Panicked
        );
        let report = registry.shutdown_until(tokio::time::Instant::now()).await;
        assert!(report.panicked && !report.is_clean());
        let pid = i32::try_from(pid.load(Ordering::SeqCst)).unwrap();
        assert!(pid > 0);
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH)
        );
    }
    #[tokio::test]
    async fn owned_output_incomplete_custody_is_sticky_after_original_join() {
        let registry = PluginTaskRegistry::default();
        let stop = registry.worker_stop();
        let receipt = registry
            .spawn_thread("owned-process-unavailable-fixture", move || {
                stop.mark_unavailable()
            })
            .unwrap();
        assert_eq!(receipt.await, JoinOutcome::Joined);
        for _ in 0..2 {
            let report = registry.shutdown_until(tokio::time::Instant::now()).await;
            assert!(report.terminal && report.unavailable && !report.is_clean());
        }
    }
}
