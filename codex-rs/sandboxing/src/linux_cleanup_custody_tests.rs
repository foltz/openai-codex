use super::*;
use pretty_assertions::assert_eq;
use std::io::Read;
use std::io::Write;
use std::time::Duration;

fn launcher_argv() -> Vec<String> {
    vec![
        "/codex-linux-sandbox".to_string(),
        "--sandbox-policy-cwd".to_string(),
        "/w".to_string(),
        "--".to_string(),
        "true".to_string(),
    ]
}

#[test]
fn custody_arguments_precede_the_command_separator() {
    let custody = LinuxSandboxCleanupCustody::new(Some(Instant::now() + Duration::from_secs(5)))
        .expect("custody");
    let mut argv = launcher_argv();
    custody.attach_to_launcher_argv(&mut argv).expect("attach");

    let separator = argv.iter().position(|arg| arg == "--").expect("separator");
    let fds = custody.inherited_fds();
    assert_eq!(fds.len(), 2);
    assert_eq!(argv[separator - 6], CLEANUP_DEADLINE_ARG);
    assert_eq!(argv[separator - 4], CLEANUP_DEADLINE_FD_ARG);
    assert_eq!(argv[separator - 3], fds[0].to_string());
    assert_eq!(argv[separator - 2], CLEANUP_REPORT_FD_ARG);
    assert_eq!(argv[separator - 1], fds[1].to_string());
    assert_eq!(argv[separator + 1], "true");
}

#[test]
fn tightening_writes_only_earlier_deadlines() {
    let custody = LinuxSandboxCleanupCustody::new(Some(Instant::now() + Duration::from_secs(60)))
        .expect("custody");
    let tighten_fd = custody.inherited_fds()[0];
    let tightener = custody.tightener();
    tightener.tighten(Instant::now() + Duration::from_secs(600));
    let earlier = Instant::now() + Duration::from_millis(10);
    tightener.tighten(earlier);
    assert_eq!(custody.deadline(), Some(earlier));

    // SAFETY: borrowed for one read; the custody owns it.
    let mut reader = std::mem::ManuallyDrop::new(unsafe { std::fs::File::from_raw_fd(tighten_fd) });
    let mut value = [0_u8; 8];
    reader.read_exact(&mut value).expect("one tightening");
    // `monotonic_ns` samples two clocks; allow a millisecond of skew.
    assert!(u64::from_le_bytes(value) <= monotonic_ns(earlier) + 1_000_000);
}

/// Capacity sizes only the saturation fixture; delivery never depends on it.
#[test]
fn a_full_channel_coalesces_without_blocking_and_retries_the_minimum() {
    let custody = LinuxSandboxCleanupCustody::new(None).expect("custody");
    let fd = custody.inherited_fds()[0];
    let mut state = custody.tightener.state.lock().expect("state");
    let writer = state.channel.as_raw_fd();
    // Refuse a blocking implementation before attempting to fill its pipe.
    assert_ne!(
        unsafe { libc::fcntl(writer, libc::F_GETFL) } & libc::O_NONBLOCK,
        0
    );
    let capacity = unsafe { libc::fcntl(writer, libc::F_GETPIPE_SZ) };
    assert!(capacity >= 8);
    let mut filled = 0;
    loop {
        match state.channel.write(&u64::MAX.to_le_bytes()) {
            Ok(8) => filled += 8,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
            result => panic!("unexpected atomic fill result: {result:?}"),
        }
        assert!(filled <= capacity as usize);
    }
    drop(state);
    let earlier = Instant::now() + Duration::from_secs(30);
    custody.tightener.tighten(earlier + Duration::from_secs(10));
    custody.tightener.tighten(earlier);
    assert_eq!(custody.deadline(), Some(earlier));
    assert_eq!(
        custody.tightener.delivery_failure(),
        Some("deadline-pending")
    );
    let frame = custody
        .tightener
        .state
        .lock()
        .expect("state")
        .frame_ns
        .expect("frame");
    let mut argv = launcher_argv();
    custody.attach_to_launcher_argv(&mut argv).expect("attach");
    let index = argv
        .iter()
        .position(|arg| arg == CLEANUP_DEADLINE_ARG)
        .expect("arg");
    assert_eq!(argv[index + 1], frame.to_string());
    // Drain only the bytes inserted by this fixture, without closing the reader.
    let mut reader = std::mem::ManuallyDrop::new(unsafe { std::fs::File::from_raw_fd(fd) });
    let mut contents = vec![0; filled];
    reader.read_exact(&mut contents).expect("drain fill");
    // A later, non-tightening call must retry the pending earlier frame.
    custody.tightener.tighten(earlier + Duration::from_secs(20));
    let mut delivered = [0; 8];
    reader.read_exact(&mut delivered).expect("pending frame");
    assert_eq!(u64::from_le_bytes(delivered), frame);
    assert_eq!(custody.tightener.delivery_failure(), None);
    assert_eq!(custody.deadline(), Some(earlier));
}

#[test]
fn an_unflushed_minimum_cannot_report_clean_completion() {
    let mut custody = LinuxSandboxCleanupCustody::new(None).expect("custody");
    let receiver = custody.child_tighten.take().expect("receiver");
    {
        let mut state = custody.tightener.state.lock().expect("state");
        let flags = unsafe { libc::fcntl(state.channel.as_raw_fd(), libc::F_GETFL) };
        assert_ne!(flags & libc::O_NONBLOCK, 0);
        loop {
            match state.channel.write(&u64::MAX.to_le_bytes()) {
                Ok(8) => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                result => panic!("unexpected fill result: {result:?}"),
            }
        }
    }
    custody
        .tightener
        .tighten(Instant::now() + Duration::from_secs(5));
    write_and_close_report(
        &mut custody,
        "v=1\nkind=helpers\nreaper=none\nend\nv=1\nstate=complete\nend\n",
    );
    custody.spawned(None);
    let report = custody.collect_blocking();
    assert!(report.joined);
    assert_eq!(report.state, SandboxCleanupState::Incomplete);
    drop(receiver);
}

#[test]
fn a_closed_deadline_receiver_cannot_report_clean_completion() {
    let mut custody = LinuxSandboxCleanupCustody::new(None).expect("custody");
    drop(custody.child_tighten.take());
    custody
        .tightener
        .tighten(Instant::now() + Duration::from_secs(5));
    assert_eq!(custody.tightener.delivery_failure(), Some("receiver-gone"));
    write_and_close_report(
        &mut custody,
        "v=1\nkind=helpers\nreaper=none\nend\nv=1\nstate=complete\nend\n",
    );
    custody.spawned(None);
    let report = custody.collect_blocking();
    assert!(report.joined);
    assert_eq!(report.state, SandboxCleanupState::Incomplete);
}

/// Without a spawned launcher, the helper declaration (no reaper) and the
/// closed report leave nothing to join; the census is summarized from the
/// receipt. This does not exercise pidfd exit evidence (see
/// `the_launcher_pidfd_exit_completes_the_join`).
#[test]
fn a_launch_without_launcher_or_reaper_joins_and_summarizes_the_census() {
    let mut custody =
        LinuxSandboxCleanupCustody::new(Some(Instant::now() + Duration::from_secs(5)))
            .expect("custody");
    let report_fd = custody.inherited_fds()[1];
    {
        // SAFETY: borrowed for one write; the custody owns it until `spawned`.
        let mut writer =
            std::mem::ManuallyDrop::new(unsafe { std::fs::File::from_raw_fd(report_fd) });
        writer
            .write_all(
                b"v=1\nkind=helpers\nreaper=none\nend\n\
                  v=1\nstate=incomplete\nfinisher=reaper\nreaper=none\n\
                  reaper_join=finished-by-reaper\n\
                  target=reaped 2f772f2e676974\ntarget=host-owned 2f772f2e636f646578\n\
                  target=incomplete:replaced 2f772f2e6177730a\nend\n",
            )
            .expect("write receipt");
    }
    custody.spawned(/*launcher_pid*/ None);

    let report = custody.collect_blocking();
    // No launcher was spawned and the launcher's declaration records no
    // reaper: nothing remains to be joined.
    assert!(report.joined);
    assert_eq!(report.finisher.as_deref(), Some("reaper"));
    assert_eq!(
        report.summary(),
        SandboxCleanupSummary {
            state: SandboxCleanupState::Incomplete,
            reaped: 1,
            preserved_unproven: 1,
            host_owned: 1,
            handed_off: 0,
            timed_out: 0,
            joined: true,
        }
    );
}

/// The join needs exit evidence from the launcher's pidfd. With the same
/// complete receipt and no-reaper declaration, a live launcher is not joined
/// (`a_closed_report_is_not_a_join_while_the_launcher_lives`); once it has
/// exited, its pidfd reports exit and the launch is joined.
#[test]
fn the_launcher_pidfd_exit_completes_the_join() {
    let mut launcher = exiting_child();
    let mut custody =
        LinuxSandboxCleanupCustody::new(Some(Instant::now() + Duration::from_secs(5)))
            .expect("custody");
    write_and_close_report(
        &mut custody,
        "v=1\nkind=helpers\nreaper=none\nend\n\
         v=1\nstate=complete\nfinisher=launcher\nreaper=none\nreaper_join=none\nend\n",
    );
    custody.spawned(Some(launcher.id()));
    launcher.wait();

    let report = custody.collect_blocking();
    assert!(
        report.joined,
        "the exited launcher's pidfd completes the join"
    );
    assert_eq!(report.summary().state, SandboxCleanupState::Complete);
    assert!(report.summary().joined);
}

#[test]
fn an_unjoined_launch_is_timed_out_at_the_owner_deadline() {
    let mut custody =
        LinuxSandboxCleanupCustody::new(Some(Instant::now() + Duration::from_millis(50)))
            .expect("custody");
    // Keep a writer open, as a reaper still running past the deadline would.
    let writer = std::fs::File::from(custody.child_report.take().expect("report end"));
    custody.spawned(/*launcher_pid*/ None);

    let started = Instant::now();
    let report = custody.collect_blocking();
    assert_eq!(report.state, SandboxCleanupState::TimedOut);
    assert!(!report.joined);
    assert!(started.elapsed() < Duration::from_secs(5));
    drop(writer);
}

#[test]
fn eof_without_any_record_is_never_a_join() {
    let mut custody =
        LinuxSandboxCleanupCustody::new(Some(Instant::now() + Duration::from_millis(100)))
            .expect("custody");
    custody.spawned(/*launcher_pid*/ None);
    let report = custody.collect_blocking();
    // A closed report proves only that its descriptors closed.
    assert!(!report.joined);
    assert_eq!(report.state, SandboxCleanupState::TimedOut);
}

/// A stand-in process owned by one test. It is pinned by a pidfd at spawn,
/// and dropping the guard (including while a failed assertion unwinds)
/// SIGKILLs it through that pidfd and reaps it, so no test leaks a process.
struct ChildGuard {
    child: std::process::Child,
    pidfd: OwnedFd,
}

impl ChildGuard {
    fn spawn(program: &str, args: &[&str]) -> Self {
        let child = std::process::Command::new(program)
            .args(args)
            .spawn()
            .expect("spawn stand-in process");
        // The child is unreaped, so its pid cannot be reused before the pin.
        let pidfd = libc::pid_t::try_from(child.id())
            .map_err(io::Error::other)
            .and_then(pidfd_open);
        let pidfd = match pidfd {
            Ok(pidfd) => pidfd,
            Err(error) => {
                let mut child = child;
                let _ = child.kill();
                let _ = child.wait();
                panic!("pin stand-in process: {error}");
            }
        };
        Self { child, pidfd }
    }

    fn id(&self) -> u32 {
        self.child.id()
    }

    /// Waits for a process expected to exit on its own.
    fn wait(&mut self) {
        self.child.wait().expect("reap stand-in process");
    }

    /// SIGKILLs the process through its pidfd and reaps it.
    fn kill_and_reap(&mut self) -> io::Result<()> {
        // SAFETY: pidfd_send_signal with a pidfd this guard owns and no siginfo.
        let sent = unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                self.pidfd.as_raw_fd(),
                libc::SIGKILL,
                std::ptr::null::<libc::siginfo_t>(),
                0,
            )
        };
        let signal_error = (sent < 0).then(io::Error::last_os_error);
        self.child.wait()?;
        // ESRCH only means the process had already exited before the reap.
        match signal_error {
            Some(error) if error.raw_os_error() != Some(libc::ESRCH) => Err(error),
            _ => Ok(()),
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.kill_and_reap();
    }
}

fn long_lived_child() -> ChildGuard {
    ChildGuard::spawn("sleep", &["30"])
}

fn exiting_child() -> ChildGuard {
    ChildGuard::spawn("true", &[])
}

fn write_and_close_report(custody: &mut LinuxSandboxCleanupCustody, receipt: &str) {
    let writer = custody.child_report.take().expect("report end");
    let mut writer = std::fs::File::from(writer);
    writer.write_all(receipt.as_bytes()).expect("write receipt");
    // Closing the report pipe proves only that this descriptor closed.
    drop(writer);
}

#[test]
fn a_closed_report_is_not_a_join_while_the_launcher_lives() {
    let mut launcher = long_lived_child();
    let mut custody =
        LinuxSandboxCleanupCustody::new(Some(Instant::now() + Duration::from_millis(200)))
            .expect("custody");
    write_and_close_report(
        &mut custody,
        "v=1\nstate=complete\nfinisher=launcher\nreaper=none\nreaper_join=none\nend\n",
    );
    custody.spawned(Some(launcher.id()));

    let report = custody.collect_blocking();
    assert!(!report.joined, "EOF with a live launcher must not join");
    assert_eq!(report.state, SandboxCleanupState::TimedOut);
    assert_eq!(report.summary().state, SandboxCleanupState::TimedOut);

    launcher.kill_and_reap().expect("kill stand-in launcher");
}

#[test]
fn a_named_reaper_must_exit_before_the_join() {
    let mut reaper = long_lived_child();
    let pid = reaper.id();
    let boot = std::fs::read_to_string("/proc/sys/kernel/random/boot_id").expect("boot id");
    let start = process_start_ticks(pid as libc::pid_t).expect("start ticks");
    let receipt = format!(
        "v=1\nstate=complete\nfinisher=reaper\nreaper={}.{pid}.{start}\n\
         reaper_join=finished-by-reaper\nend\n",
        boot.trim()
    );

    // The reaper is alive: the closed report is not a join.
    let mut custody =
        LinuxSandboxCleanupCustody::new(Some(Instant::now() + Duration::from_millis(200)))
            .expect("custody");
    write_and_close_report(&mut custody, &receipt);
    custody.spawned(/*launcher_pid*/ None);
    let report = custody.collect_blocking();
    assert!(!report.joined);

    // Once it has exited (pidfd readable), the same receipt joins.
    reaper.kill_and_reap().expect("kill stand-in reaper");
    let mut custody =
        LinuxSandboxCleanupCustody::new(Some(Instant::now() + Duration::from_secs(5)))
            .expect("custody");
    write_and_close_report(&mut custody, &receipt);
    custody.spawned(/*launcher_pid*/ None);
    let report = custody.collect_blocking();
    assert!(report.joined);
    assert_eq!(report.state, SandboxCleanupState::Complete);
}

/// A keeper launch's normal finish reports `finisher=reaper` and
/// `reaper_join=pending` (the launcher joins the keeper afterwards). That
/// receipt, complete or not, is never the owner's join: only the keeper's own
/// pidfd reporting exit is.
#[test]
fn a_keepers_complete_receipt_is_joined_only_by_its_exit() {
    let mut keeper = long_lived_child();
    let pid = keeper.id();
    let boot = std::fs::read_to_string("/proc/sys/kernel/random/boot_id").expect("boot id");
    let start = process_start_ticks(pid as libc::pid_t).expect("start ticks");
    let token = format!("{}.{pid}.{start}", boot.trim());
    let report = format!(
        "v=1\nkind=helpers\nreaper={token}\nend\n\
         v=1\nstate=complete\nfinisher=reaper\nreaper={token}\nreaper_join=pending\nend\n"
    );

    // The keeper is alive: a complete receipt and a closed report are not
    // a join.
    let mut custody =
        LinuxSandboxCleanupCustody::new(Some(Instant::now() + Duration::from_millis(200)))
            .expect("custody");
    write_and_close_report(&mut custody, &report);
    custody.spawned(/*launcher_pid*/ None);
    let collected = custody.collect_blocking();
    assert!(!collected.joined);

    // Its pidfd reports exit: the same report joins.
    keeper.kill_and_reap().expect("kill stand-in keeper");
    let mut custody =
        LinuxSandboxCleanupCustody::new(Some(Instant::now() + Duration::from_secs(5)))
            .expect("custody");
    write_and_close_report(&mut custody, &report);
    custody.spawned(/*launcher_pid*/ None);
    let collected = custody.collect_blocking();
    assert!(collected.joined);
    assert_eq!(collected.state, SandboxCleanupState::Complete);
    assert_eq!(collected.reaper_join.as_deref(), Some("pending"));
}

#[test]
fn a_tightening_wakes_a_blocked_collection() {
    let mut custody =
        LinuxSandboxCleanupCustody::new(Some(Instant::now() + Duration::from_secs(60)))
            .expect("custody");
    // Keep the report open so nothing but the deadline can end the wait.
    let writer = std::fs::File::from(custody.child_report.take().expect("report end"));
    custody.spawned(/*launcher_pid*/ None);
    let tightener = custody.tightener();
    let started = Instant::now();
    let collector = std::thread::spawn(move || custody.collect_blocking());
    std::thread::sleep(Duration::from_millis(50));
    tightener.tighten(Instant::now());

    let report = collector.join().expect("collector");
    assert!(!report.joined);
    assert_eq!(report.state, SandboxCleanupState::TimedOut);
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "woken by the tightening"
    );
    drop(writer);
}

#[test]
fn an_already_expired_deadline_is_checked_before_waiting() {
    let mut custody = LinuxSandboxCleanupCustody::new(Some(Instant::now())).expect("custody");
    let writer = std::fs::File::from(custody.child_report.take().expect("report end"));
    custody.spawned(/*launcher_pid*/ None);
    let started = Instant::now();
    let report = custody.collect_blocking();
    assert!(!report.joined);
    assert!(started.elapsed() < Duration::from_secs(1));
    drop(writer);
}

#[test]
fn an_unnamed_live_helper_is_never_joined_after_the_launcher_exits() {
    // The launcher exited (pinned before it was reaped).
    let mut launcher = exiting_child();
    let mut custody =
        LinuxSandboxCleanupCustody::new(Some(Instant::now() + Duration::from_millis(200)))
            .expect("custody");
    // A helper is alive, the report is closed, and no record names it or
    // declares that no reaper exists.
    let mut helper = long_lived_child();
    write_and_close_report(
        &mut custody,
        "v=1\nstate=complete\nfinisher=launcher\nreaper_join=none\nend\n",
    );
    custody.spawned(Some(launcher.id()));
    launcher.wait();

    let report = custody.collect_blocking();
    assert!(!report.joined, "nothing proves the helper exited");
    assert_eq!(report.state, SandboxCleanupState::TimedOut);
    assert_ne!(report.summary().state, SandboxCleanupState::Complete);

    helper.kill_and_reap().expect("kill helper");
}

#[test]
fn an_expired_deadline_reads_and_polls_nothing() {
    // Everything is ready: report data is available and the launcher's
    // pidfd would report exit. The deadline has already passed.
    let mut launcher = exiting_child();
    let mut custody = LinuxSandboxCleanupCustody::new(Some(Instant::now())).expect("custody");
    write_and_close_report(
        &mut custody,
        "v=1\nkind=helpers\nreaper=none\nend\nv=1\nstate=complete\nreaper=none\nend\n",
    );
    custody.spawned(Some(launcher.id()));
    launcher.wait();

    IO_CALLS.with(|calls| calls.set(0));
    let report = custody.collect_blocking();
    assert_eq!(
        IO_CALLS.with(std::cell::Cell::get),
        0,
        "no read or poll after expiry"
    );
    assert!(!report.joined);
    assert_eq!(report.state, SandboxCleanupState::TimedOut);
}

#[test]
fn oversized_final_summary_is_incomplete_even_after_positive_join() {
    let text = "v=1\nkind=helpers\nreaper=none\nend\nv=1\nstate=incomplete\nreport=exceeds-atomic-bound\nend\n";
    let joined = parse_report(text, true);
    assert!(joined.joined);
    assert_eq!(joined.state, SandboxCleanupState::Incomplete);
    assert_eq!(
        parse_report(text, false).state,
        SandboxCleanupState::TimedOut
    );
}
