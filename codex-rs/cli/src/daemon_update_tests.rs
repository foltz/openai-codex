#![cfg(test)]

use super::*;
use codex_tui::DaemonUpdateSource;

#[test]
fn managed_daemon_handoff_is_refused_before_executing_the_selected_cli() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let missing_executable = dir.path().join("must not execute");
    for source in [
        DaemonUpdateSource::PublicStable,
        DaemonUpdateSource::ThisCli,
    ] {
        let error = run_update_action(UpdateAction::Daemon(source), Some(&missing_executable))
            .expect_err("managed update must refuse before spawning a process");
        assert_eq!(
            error.to_string(),
            codex_app_server_daemon::MANAGED_UPDATE_DISABLED_MESSAGE
        );
    }
    assert_eq!(std::fs::read_dir(dir.path())?.count(), 0);
    Ok(())
}

#[test]
fn package_update_is_refused_before_project_or_installer_execution() -> anyhow::Result<()> {
    const CHILD: &str = "CODEX_TEST_MANAGED_PACKAGE_UPDATE_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let error = run_update_action(UpdateAction::PnpmGlobalLatest, None)
            .expect_err("managed distribution refuses before invoking an installer");
        assert_eq!(
            error.to_string(),
            codex_app_server_daemon::MANAGED_UPDATE_DISABLED_MESSAGE
        );
        return Ok(());
    }
    let project = tempfile::tempdir()?;
    let bin = tempfile::tempdir()?;
    codex_utils_cargo_bin::write_executable(
        &bin.path().join("pnpm"),
        "#!/bin/sh\npwd -P > \"$0.cwd\"\n",
    )?;
    assert_cmd::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "daemon_update_tests::package_update_is_refused_before_project_or_installer_execution",
        ])
        .current_dir(project.path())
        .env("PATH", bin.path())
        .env(CHILD, "1")
        .timeout(std::time::Duration::from_secs(10))
        .assert()
        .success();
    assert!(!bin.path().join("pnpm.cwd").exists());
    assert_eq!(std::fs::read_dir(project.path())?.count(), 0);
    Ok(())
}
