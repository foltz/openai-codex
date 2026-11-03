//! Optional per-client recovery checkpoints. They are written by the TUI, never
//! by the shared app-server. A transition is durable before its effects begin.
use std::path::Path;
use std::path::PathBuf;

use codex_protocol::ThreadId;
use serde::Deserialize;
use serde::Serialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Context {
    format_version: u32,
    context_id: String,
    binding_digest: String,
    provider: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProcessBirth {
    pid: u32,
    start: String,
    boot_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Checkpoint<'a> {
    format_version: u32,
    context_id: &'a str,
    binding_digest: &'a str,
    provider: &'a str,
    pane_id: &'a str,
    pid: u32,
    process: &'a ProcessBirth,
    conversation_id: Option<String>,
    phase: &'a str,
    intent: &'a str,
    observed_at: u64,
}

pub(crate) struct RecoveryObserver {
    context: Context,
    path: PathBuf,
    pane: String,
    process: ProcessBirth,
    last: Option<(Option<ThreadId>, &'static str, &'static str)>,
}

impl RecoveryObserver {
    pub(crate) fn from_env() -> std::io::Result<Option<Self>> {
        let Some(path) = std::env::var_os("WORKGROUPS_RECOVERY_CONTEXT") else {
            return Ok(None);
        };
        let pane = std::env::var("TMUX_PANE").unwrap_or_default();
        Self::open(Path::new(&path), pane).map(Some)
    }

    fn open(path: &Path, pane: String) -> std::io::Result<Self> {
        use std::io::Error;
        if !path.is_absolute()
            || std::fs::canonicalize(path)? != path
            || !pane.starts_with('%')
            || pane.len() < 2
            || !pane[1..].bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(Error::other("invalid Workgroups recovery context/pane"));
        }
        check_custody(path, /*directory*/ false)?;
        check_custody(
            path.parent()
                .ok_or_else(|| Error::other("missing context parent"))?,
            /*directory*/ true,
        )?;
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW);
        }
        let mut file = options.open(path)?;
        let before = file.metadata()?;
        if before.len() > 4096 {
            return Err(Error::other("recovery context exceeds bound"));
        }
        use std::io::Read;
        let mut bytes = Vec::new();
        file.by_ref().take(4097).read_to_end(&mut bytes)?;
        if bytes.len() > 4096
            || before.len() != file.metadata()?.len()
            || before.modified()? != file.metadata()?.modified()?
        {
            return Err(Error::other("recovery context exceeds bound"));
        }
        let context: Context = serde_json::from_slice(&bytes)?;
        if context.format_version != 1
            || context.provider != "codex"
            || uuid::Uuid::parse_str(&context.context_id).is_err()
            || context.binding_digest.len() != 64
            || !context
                .binding_digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(Error::other("unsupported Workgroups recovery context"));
        }
        Ok(Self {
            context,
            path: path.with_extension("checkpoint.json"),
            pane,
            process: process_birth()?,
            last: None,
        })
    }

    pub(crate) fn transition(&mut self) -> std::io::Result<()> {
        self.write(/*thread_id*/ None, "transition", "unknown")
    }

    pub(crate) fn observed(&mut self, thread_id: Option<ThreadId>) -> std::io::Result<()> {
        self.write(thread_id, "observed", "recover")
    }

    pub(crate) fn stopped(&mut self, thread_id: Option<ThreadId>) -> std::io::Result<()> {
        self.write(thread_id, "observed", "stopped")
    }

    fn write(
        &mut self,
        thread_id: Option<ThreadId>,
        phase: &'static str,
        intent: &'static str,
    ) -> std::io::Result<()> {
        let key = (thread_id, phase, intent);
        if self.last == Some(key) {
            return Ok(());
        }
        let value = Checkpoint {
            format_version: 1,
            context_id: &self.context.context_id,
            binding_digest: &self.context.binding_digest,
            provider: "codex",
            pane_id: &self.pane,
            pid: std::process::id(),
            process: &self.process,
            conversation_id: thread_id.map(|id| id.to_string()),
            phase,
            intent,
            observed_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(std::io::Error::other)?
                .as_secs(),
        };
        durable_write(&self.path, &serde_json::to_vec(&value)?)?;
        self.last = Some(key);
        Ok(())
    }
}

#[cfg(unix)]
fn check_custody(path: &Path, directory: bool) -> std::io::Result<()> {
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::symlink_metadata(path)?;
    // SAFETY: geteuid has no preconditions and does not modify process state.
    let uid = unsafe { libc::geteuid() };
    if metadata.uid() != uid
        || metadata.mode() & 0o077 != 0
        || metadata.file_type().is_symlink()
        || (directory && !metadata.is_dir())
        || (!directory && !metadata.is_file())
    {
        return Err(std::io::Error::other("unsafe Workgroups recovery custody"));
    }
    Ok(())
}
#[cfg(not(unix))]
fn check_custody(_path: &Path, _directory: bool) -> std::io::Result<()> {
    Err(std::io::Error::other(
        "Workgroups recovery checkpoints require Unix custody",
    ))
}

fn durable_write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other("missing checkpoint parent"))?;
    check_custody(parent, /*directory*/ true)?;
    if path.try_exists()? {
        check_custody(path, /*directory*/ false)?;
    }
    let temporary = parent.join(format!(".{}.pending", uuid::Uuid::new_v4()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let result = (|| {
        let mut file = options.open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)?;
        std::fs::File::open(parent)?.sync_all()
    })();
    if temporary.exists() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

#[cfg(test)]
#[path = "workgroups_recovery_tests.rs"]
mod tests;

#[cfg(target_os = "linux")]
fn process_birth() -> std::io::Result<ProcessBirth> {
    let stat = std::fs::read_to_string("/proc/self/stat")?;
    let after_name = stat
        .rsplit_once(')')
        .ok_or_else(|| std::io::Error::other("invalid process birth"))?
        .1;
    let start = after_name
        .split_whitespace()
        .nth(19)
        .filter(|value| value.bytes().all(|byte| byte.is_ascii_digit()))
        .ok_or_else(|| std::io::Error::other("missing process start time"))?
        .to_string();
    Ok(ProcessBirth {
        pid: std::process::id(),
        start,
        boot_id: std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?
            .trim()
            .to_string(),
    })
}

#[cfg(target_os = "macos")]
fn process_birth() -> std::io::Result<ProcessBirth> {
    let start = std::process::Command::new("/bin/ps")
        .args(["-p", &std::process::id().to_string(), "-o", "lstart="])
        .env("LC_ALL", "C")
        .output()?;
    let boot = std::process::Command::new("/usr/sbin/sysctl")
        .args(["-n", "kern.boottime"])
        .env("LC_ALL", "C")
        .output()?;
    if !start.status.success() || !boot.status.success() {
        return Err(std::io::Error::other("process birth unavailable"));
    }
    Ok(ProcessBirth {
        pid: std::process::id(),
        start: String::from_utf8_lossy(&start.stdout).trim().to_string(),
        boot_id: String::from_utf8_lossy(&boot.stdout).trim().to_string(),
    })
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn process_birth() -> std::io::Result<ProcessBirth> {
    Err(std::io::Error::other(
        "Workgroups recovery process birth unsupported",
    ))
}
