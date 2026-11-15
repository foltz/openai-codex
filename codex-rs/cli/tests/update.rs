use anyhow::Result;
use predicates::str::contains;
use std::path::Path;
use tempfile::TempDir;

fn codex_command(codex_home: &Path) -> Result<assert_cmd::Command> {
    let mut cmd = assert_cmd::Command::new(codex_utils_cargo_bin::cargo_bin("codex")?);
    cmd.env("CODEX_HOME", codex_home);
    Ok(cmd)
}

#[tokio::test]
async fn update_is_refused_with_managed_release_promotion_guidance() -> Result<()> {
    let codex_home = TempDir::new()?;

    codex_command(codex_home.path())?
        .arg("update")
        .assert()
        .failure()
        .stderr(contains(
            "codex self-update is disabled in this managed distribution",
        ))
        .stderr(contains("managed release and promotion workflow"));

    Ok(())
}

#[tokio::test]
async fn hidden_pid_update_loop_is_refused_before_updater_setup() -> Result<()> {
    let codex_home = TempDir::new()?;

    codex_command(codex_home.path())?
        .args(["app-server", "daemon", "pid-update-loop"])
        .assert()
        .failure()
        .stderr(contains(
            "codex self-update is disabled in this managed distribution",
        ))
        .stderr(contains("managed release and promotion workflow"));

    Ok(())
}
