#![cfg(unix)]

use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use anyhow::Result;
use anyhow::ensure;
use pretty_assertions::assert_eq;
use serde_json::Value;
use tempfile::TempDir;

struct TestDaemon {
    home: TempDir,
    codex: PathBuf,
    unmanaged: Option<Child>,
}

impl TestDaemon {
    fn new() -> Result<Self> {
        let home = tempfile::Builder::new().tempdir_in("/tmp")?;
        let codex = codex_utils_cargo_bin::cargo_bin("codex")?;
        let codex_source = std::fs::canonicalize(&codex)?;
        let target = if cfg!(target_os = "macos") {
            format!("{}-apple-darwin", std::env::consts::ARCH)
        } else {
            format!("{}-unknown-linux-musl", std::env::consts::ARCH)
        };
        let standalone = home.path().join("packages/standalone");
        let release_name = format!("0.0.0-{target}");
        let managed = standalone
            .join("releases")
            .join(&release_name)
            .join("bin/codex");
        std::fs::create_dir_all(managed.parent().context("managed bin parent")?)?;
        std::fs::hard_link(&codex_source, &managed)
            .or_else(|_| std::fs::copy(&codex_source, managed).map(|_| ()))?;
        std::fs::write(standalone.join("auto-update-version"), &release_name)?;
        std::os::unix::fs::symlink(
            PathBuf::from("releases").join(release_name),
            standalone.join("current"),
        )?;
        // Model a daemon that was previously launched and is currently stopped.
        let state = home.path().join("app-server-daemon");
        std::fs::create_dir(&state)?;
        std::fs::write(state.join("app-server.stderr.log"), b"")?;
        Ok(Self {
            home,
            codex,
            unmanaged: None,
        })
    }

    fn command(&self) -> Command {
        let mut command = Command::new(&self.codex);
        command.env("CODEX_HOME", self.home.path());
        command
    }

    fn lifecycle(&self, action: &str) -> Result<Value> {
        let output = self
            .command()
            .args(["app-server", "daemon", action])
            .output()?;
        ensure!(
            output.status.success(),
            "daemon {action} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(serde_json::from_slice(&output.stdout)?)
    }

    fn pid(&self, name: &str) -> Result<u32> {
        let record = std::fs::read(self.home.path().join("app-server-daemon").join(name))
            .with_context(|| format!("failed to read {name}"))?;
        Ok(serde_json::from_slice::<Value>(&record)?["pid"]
            .as_u64()
            .context("pid missing")? as u32)
    }

    fn assert_no_updater(&self) {
        let state = self.home.path().join("app-server-daemon");
        assert!(!state.join("app-server-updater.pid").exists());
        assert!(!state.join("daemon-updater.pid").exists());
    }
}

fn assert_update_disabled(output: &std::process::Output) {
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert_eq!(
        String::from_utf8_lossy(&output.stderr).trim(),
        "Error: codex self-update is disabled in this managed distribution; use the managed release and promotion workflow instead"
    );
}

impl Drop for TestDaemon {
    fn drop(&mut self) {
        if let Some(mut child) = self.unmanaged.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = self.lifecycle("stop");
        if let Ok(pid) = self.pid("app-server-updater.pid") {
            let _ = signal(pid, libc::SIGTERM);
        }
    }
}

fn signal(pid: u32, signal: libc::c_int) -> Result<()> {
    let raw_pid = libc::pid_t::try_from(pid).context("pid out of range")?;
    ensure!(
        unsafe { libc::kill(raw_pid, signal) } == 0,
        "failed to signal pid {pid}: {}",
        std::io::Error::last_os_error()
    );
    Ok(())
}

fn wait_for_exit(pid: u32) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let output = Command::new("/bin/ps")
            .args(["-p", &pid.to_string(), "-o", "stat="])
            .output()
            .context("failed to invoke ps")?;
        let state = String::from_utf8_lossy(&output.stdout);
        if !output.status.success() || state.trim().starts_with('Z') {
            return Ok(());
        }
        ensure!(Instant::now() < deadline, "pid {pid} did not exit: {state}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn managed_identity_survives_locale_and_timezone_changes() -> Result<()> {
    let daemon = TestDaemon::new()?;
    let mut original = Vec::new();
    let pid_file = daemon.home.path().join("app-server-daemon/app-server.pid");
    for (action, locale, timezone, expected_status) in [
        ("start", "C", "UTC0", "started"),
        ("version", "en_AU.UTF-8", "PST8PDT", "running"),
        ("restart", "en_AU.UTF-8", "PST8PDT", "restarted"),
        ("stop", "C", "UTC0", "stopped"),
    ] {
        let output = daemon
            .command()
            .args(["app-server", "daemon", action])
            .env("LC_ALL", locale)
            .env("TZ", timezone)
            .output()?;
        ensure!(
            output.status.success(),
            "daemon {action} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let output: Value = serde_json::from_slice(&output.stdout)?;
        assert_eq!(output["status"], expected_status);
        if action == "start" {
            daemon.assert_no_updater();
            original = std::fs::read(&pid_file)?;
            let record: Value = serde_json::from_slice(&original)?;
            assert!(record["processIdentity"].is_object());
            // Older clients still receive the original, unnormalized ps value.
            let legacy = Command::new("ps")
                .args([
                    "-p",
                    &daemon.pid("app-server.pid")?.to_string(),
                    "-o",
                    "lstart=",
                ])
                .env("LC_ALL", locale)
                .env("TZ", timezone)
                .output()?;
            ensure!(legacy.status.success(), "failed to read legacy start time");
            assert_eq!(
                record["processStartTime"],
                String::from_utf8(legacy.stdout)?.trim()
            );
        } else if action == "version" {
            assert_eq!(output["backend"], "pid");
            daemon.assert_no_updater();
            assert_eq!(std::fs::read(&pid_file)?, original);

            // Managed builds have no updater; deliberately restore a legacy record
            // after observing the running daemon itself.
            let mut legacy: Value = serde_json::from_slice(&original)?;
            legacy
                .as_object_mut()
                .context("PID record object")?
                .remove("processIdentity");
            let legacy_bytes = serde_json::to_vec(&legacy)?;
            std::fs::write(&pid_file, &legacy_bytes)?;
            let result = daemon
                .command()
                .args(["app-server", "daemon", "version"])
                .env("LC_ALL", locale)
                .env("TZ", timezone)
                .output();
            let preserved = std::fs::read(&pid_file);
            // Restore the native identity before assertions so Drop can stop the daemon.
            std::fs::write(&pid_file, &original)?;
            let result = result?;
            assert!(!result.status.success());
            assert_eq!(preserved?, legacy_bytes);
            let stderr =
                String::from_utf8(result.stderr)?.replace(&legacy["pid"].to_string(), "[PID]");
            insta::assert_snapshot!(stderr, @"Error: cannot verify pid-managed process [PID]: legacy start time changed; PID record retained. Retry with the locale and timezone used to start the daemon. If the system clock changed, stop the original process before restarting the daemon");
        }
        if action == "restart" {
            daemon.assert_no_updater();
        }
    }
    assert!(!pid_file.exists());
    Ok(())
}

#[test]
fn package_ownership_check_does_not_start_an_updater() -> Result<()> {
    let daemon = TestDaemon::new()?;
    let output = daemon
        .command()
        .args([
            "app-server",
            "daemon",
            "pid-update-loop",
            "--check-package-ownership",
        ])
        .output()?;
    assert_update_disabled(&output);
    daemon.assert_no_updater();
    Ok(())
}

#[test]
fn managed_starts_and_restarts_do_not_create_an_updater() -> Result<()> {
    let daemon = TestDaemon::new()?;
    assert_eq!(daemon.lifecycle("start")?["status"], "started");
    let backend_pid = daemon.pid("app-server.pid")?;
    daemon.assert_no_updater();

    assert_eq!(daemon.lifecycle("start")?["status"], "alreadyRunning");
    assert_eq!(daemon.pid("app-server.pid")?, backend_pid);
    daemon.assert_no_updater();
    assert_eq!(daemon.lifecycle("restart")?["status"], "restarted");
    assert_ne!(daemon.pid("app-server.pid")?, backend_pid);
    wait_for_exit(backend_pid)?;
    daemon.assert_no_updater();
    daemon.lifecycle("stop")?;
    assert!(
        !daemon
            .home
            .path()
            .join("app-server-daemon/app-server.pid")
            .exists()
    );
    assert_eq!(daemon.lifecycle("start")?["status"], "started");
    daemon.assert_no_updater();
    assert!(
        !daemon
            .home
            .path()
            .join("packages/app-server-daemon")
            .exists()
    );
    Ok(())
}

#[test]
fn managed_start_refuses_when_updater_record_is_invalid() -> Result<()> {
    let daemon = TestDaemon::new()?;
    let state_dir = daemon.home.path().join("app-server-daemon");
    std::fs::create_dir_all(&state_dir)?;
    std::fs::write(state_dir.join("app-server-updater.pid"), "not a PID record")?;

    let output = daemon
        .command()
        .args(["app-server", "daemon", "start"])
        .output()?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid pid file contents"));
    assert_eq!(
        std::fs::read_to_string(state_dir.join("app-server-updater.pid"))?,
        "not a PID record"
    );
    assert!(!state_dir.join("app-server.pid").exists());
    assert!(!state_dir.join("daemon.pid").exists());
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn wall_clock_shift_keeps_server_managed_without_updater() -> Result<()> {
    let daemon = TestDaemon::new()?;
    assert_eq!(daemon.lifecycle("start")?["status"], "started");
    let server_pid = daemon.pid("app-server.pid")?;
    daemon.assert_no_updater();
    let path = daemon.home.path().join("app-server-daemon/app-server.pid");
    let mut record: Value = serde_json::from_slice(&std::fs::read(&path)?)?;
    record["processStartTime"] = "historical wall-clock start time".into();
    let replacement = path.with_extension("replacement");
    std::fs::write(&replacement, serde_json::to_vec(&record)?)?;
    std::fs::rename(replacement, path)?;

    let version = daemon.lifecycle("version")?;
    assert_eq!(version["status"], "running");
    assert_eq!(version["backend"], "pid");
    assert_eq!(daemon.lifecycle("start")?["status"], "alreadyRunning");
    assert_eq!(daemon.pid("app-server.pid")?, server_pid);
    daemon.assert_no_updater();
    assert_eq!(daemon.lifecycle("restart")?["status"], "restarted");
    wait_for_exit(server_pid)?;
    assert_ne!(daemon.pid("app-server.pid")?, server_pid);
    daemon.assert_no_updater();
    Ok(())
}

#[test]
fn managed_start_never_creates_updater_on_marker_mismatch_or_pin() -> Result<()> {
    let daemon = TestDaemon::new()?;
    assert_eq!(daemon.lifecycle("start")?["status"], "started");
    let server_pid = daemon.pid("app-server.pid")?;
    daemon.assert_no_updater();
    let marker = daemon
        .home
        .path()
        .join("packages/standalone/auto-update-version");
    std::fs::write(&marker, "0.1.0-other-target")?;
    assert_eq!(daemon.lifecycle("start")?["status"], "alreadyRunning");
    assert_eq!(daemon.pid("app-server.pid")?, server_pid);
    daemon.assert_no_updater();

    std::fs::remove_file(marker)?;

    assert_eq!(daemon.lifecycle("start")?["status"], "alreadyRunning");
    assert_eq!(daemon.pid("app-server.pid")?, server_pid);
    daemon.assert_no_updater();
    Ok(())
}

#[test]
fn restart_keeps_updater_disabled_despite_saved_preference() -> Result<()> {
    let daemon = TestDaemon::new()?;
    assert_eq!(daemon.lifecycle("start")?["status"], "started");
    daemon.assert_no_updater();
    let settings = daemon.home.path().join("app-server-daemon/settings.json");
    std::fs::write(
        &settings,
        serde_json::to_vec(&serde_json::json!({
            "updater": {"autoUpdateEnabled": false, "updateIntervalMinutes": 2},
        }))?,
    )?;
    assert_eq!(daemon.lifecycle("restart")?["status"], "restarted");
    daemon.assert_no_updater();
    assert_eq!(daemon.lifecycle("bootstrap")?["autoUpdateEnabled"], false);
    daemon.assert_no_updater();

    std::fs::write(
        &settings,
        serde_json::to_vec(&serde_json::json!({
            "updater": {"autoUpdateEnabled": true, "updateIntervalMinutes": 2},
        }))?,
    )?;
    assert_eq!(daemon.lifecycle("restart")?["status"], "restarted");
    daemon.assert_no_updater();
    assert_eq!(daemon.lifecycle("bootstrap")?["autoUpdateEnabled"], false);
    daemon.assert_no_updater();

    std::fs::write(&settings, "{malformed")?;
    assert_eq!(daemon.lifecycle("stop")?["status"], "stopped");
    Ok(())
}

#[test]
fn unmanaged_app_server_does_not_launch_updater() -> Result<()> {
    let mut daemon = TestDaemon::new()?;
    daemon.unmanaged = Some(
        daemon
            .command()
            .args(["app-server", "--listen", "unix://"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    while !daemon
        .command()
        .args(["app-server", "daemon", "version"])
        .output()?
        .status
        .success()
    {
        ensure!(Instant::now() < deadline, "app server did not become ready");
        std::thread::sleep(Duration::from_millis(50));
    }

    let output = daemon.lifecycle("start")?;
    assert_eq!(output["status"], "alreadyRunning");
    assert_eq!(output["backend"], Value::Null);
    assert_update_disabled(
        &daemon
            .command()
            .args(["app-server", "daemon", "update"])
            .output()?,
    );
    assert!(
        !daemon
            .home
            .path()
            .join("app-server-daemon/app-server-updater.pid")
            .exists()
    );
    Ok(())
}

#[test]
fn manual_update_rejects_an_unowned_installation() -> Result<()> {
    let daemon = TestDaemon::new()?;
    std::fs::remove_file(
        daemon
            .home
            .path()
            .join("packages/standalone/current/bin/codex"),
    )?;

    assert_update_disabled(
        &daemon
            .command()
            .args(["app-server", "daemon", "update"])
            .output()?,
    );
    assert!(daemon.pid("app-server.pid").is_err());
    assert!(daemon.pid("app-server-updater.pid").is_err());
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum InitialDaemon {
    Missing,
    Legacy,
}

fn packaged_daemon_launch(action: &str, initial: InitialDaemon) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut daemon = TestDaemon::new()?;
    let standalone = daemon.home.path().join("packages/standalone");
    let package = if action == "start" && initial == InitialDaemon::Missing {
        standalone.join("releases/caller")
    } else {
        daemon.home.path().join("cli-package")
    };
    for directory in ["bin", "codex-path", "codex-resources"] {
        std::fs::create_dir_all(package.join(directory))?;
    }
    std::fs::copy(&daemon.codex, package.join("bin/codex"))?;
    daemon.codex = package.join("bin/codex");
    for helper in [
        "bin/codex-code-mode-host",
        "codex-path/rg",
        "codex-resources/bwrap",
    ] {
        std::fs::write(package.join(helper), b"runtime fixture")?;
        std::fs::set_permissions(package.join(helper), std::fs::Permissions::from_mode(0o755))?;
    }
    let target = format!(
        "{}-{}",
        std::env::consts::ARCH,
        if cfg!(target_os = "macos") {
            "apple-darwin"
        } else if cfg!(target_env = "gnu") {
            "unknown-linux-gnu"
        } else {
            "unknown-linux-musl"
        }
    );
    std::fs::write(
        package.join("codex-package.json"),
        serde_json::to_vec(&serde_json::json!({
            "version": env!("CARGO_PKG_VERSION"), "target": target, "entrypoint": "bin/codex"
        }))?,
    )?;
    if action == "start" && initial == InitialDaemon::Missing {
        std::fs::remove_file(standalone.join("current"))?;
        std::os::unix::fs::symlink(&package, standalone.join("current"))?;
    }
    let cli_selection = standalone.join("current").canonicalize()?;
    let state = daemon.home.path().join("app-server-daemon");
    if initial == InitialDaemon::Missing {
        std::fs::remove_file(state.join("app-server.stderr.log"))?;
    }
    std::fs::write(
        state.join("settings.json"),
        br#"{"shutdownGraceSeconds":0}"#,
    )?;
    let settings_before = std::fs::read(state.join("settings.json"))?;
    let marker_before = std::fs::read(standalone.join("auto-update-version"))?;
    let cli_before = daemon.codex.canonicalize()?;
    let mut command = daemon.command();
    command.args(["app-server", "daemon", action]);
    // A freshly copied executable can briefly remain busy on Linux CI workers.
    let mut retries = 0;
    let result = loop {
        let result = command.output();
        if !result
            .as_ref()
            .is_err_and(|error| error.kind() == std::io::ErrorKind::ExecutableFileBusy)
            || retries == 2
        {
            break result?;
        }
        retries += 1;
        std::thread::sleep(Duration::from_millis(/*millis*/ 10));
    };
    let dedicated = daemon
        .home
        .path()
        .canonicalize()?
        .join("packages/app-server-daemon");
    assert_eq!(daemon.codex.canonicalize()?, cli_before);
    assert_eq!(standalone.join("current").canonicalize()?, cli_selection);
    daemon.assert_no_updater();
    if initial == InitialDaemon::Missing {
        // A CLI package is not authority to install a daemon implicitly.
        assert!(!result.status.success());
        let stderr = String::from_utf8_lossy(&result.stderr);
        assert!(stderr.contains("daemon executable not found at"));
        assert!(!stderr.contains("Installing daemon from CLI version"));
        assert!(!dedicated.exists());
        assert!(!state.join("daemon.pid").exists());
        assert!(!state.join("app-server.pid").exists());
        assert_eq!(std::fs::read(state.join("settings.json"))?, settings_before);
        assert_eq!(
            std::fs::read(standalone.join("auto-update-version"))?,
            marker_before
        );
        return Ok(());
    }

    ensure!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let output: Value = serde_json::from_slice(&result.stdout)?;
    assert_eq!(output["status"], "started");
    assert_eq!(
        output["managedCodexPath"],
        standalone
            .canonicalize()?
            .join("current/bin/codex")
            .to_str()
            .context("managed path is not UTF-8")?
    );
    let original_pid = daemon.pid("app-server.pid")?;
    // Neither a confirmation flag nor a complete local package grants update
    // authority. Preserve the running daemon, CLI selection and settings.
    for args in [
        vec!["app-server", "daemon", "update", "--from-cli"],
        vec!["app-server", "daemon", "update", "--from-cli", "--yes"],
    ] {
        assert_update_disabled(&daemon.command().args(args).output()?);
        assert_eq!(daemon.pid("app-server.pid")?, original_pid);
        assert!(!dedicated.exists());
        daemon.assert_no_updater();
        assert_eq!(std::fs::read(state.join("settings.json"))?, settings_before);
        assert_eq!(standalone.join("current").canonicalize()?, cli_selection);
        assert_eq!(
            std::fs::read(standalone.join("auto-update-version"))?,
            marker_before
        );
        assert_eq!(
            std::fs::read(package.join("bin/codex-code-mode-host"))?,
            b"runtime fixture"
        );
    }
    assert_eq!(daemon.lifecycle("start")?["status"], "alreadyRunning");
    assert_eq!(daemon.lifecycle("stop")?["status"], "stopped");
    wait_for_exit(original_pid)?;
    Ok(())
}

#[test]
fn packaged_daemon_start_does_not_install_missing_package() -> Result<()> {
    packaged_daemon_launch("start", InitialDaemon::Missing)
}

#[test]
fn packaged_daemon_restart_does_not_install_missing_package() -> Result<()> {
    packaged_daemon_launch("restart", InitialDaemon::Missing)
}

#[test]
fn packaged_daemon_bootstrap_does_not_install_missing_package() -> Result<()> {
    packaged_daemon_launch("bootstrap", InitialDaemon::Missing)
}

#[test]
fn packaged_daemon_explicit_update_preserves_running_legacy() -> Result<()> {
    packaged_daemon_launch("start", InitialDaemon::Legacy)
}
