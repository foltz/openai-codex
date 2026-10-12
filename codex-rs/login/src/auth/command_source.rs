//! Process-selected ChatGPT command source. Credentials travel over private IPC.
use super::AuthConfig;
use super::CodexAuth;
use super::ExternalAuth;
use super::ExternalAuthFuture;
use super::ExternalAuthRefreshContext;
use super::command_store;
use codex_config::types::AuthCredentialsStoreMode;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::Weak;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::Semaphore;

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Envelope {
    pub version: u32,
    pub source: String,
    pub binding: String,
    pub account: String,
    pub principal: String,
    pub generation: u64,
    pub access_token: String,
    pub expires_at: i64,
    pub fingerprint: String,
}

impl Envelope {
    fn usable(&self) -> bool {
        self.expires_at > chrono::Utc::now().timestamp()
    }

    fn auth(&self) -> io::Result<CodexAuth> {
        let fingerprint = format!("{:x}", sha2::Sha256::digest(self.access_token.as_bytes()));
        if self.version != 1
            || self.generation == 0
            || self.fingerprint != fingerprint
            || !self.usable()
        {
            return Err(io::Error::other("invalid or expired command credentials"));
        }
        let expiration = crate::token_data::parse_jwt_expiration(&self.access_token)
            .map_err(|_| io::Error::other("invalid credential expiry"))?;
        if expiration.map(|value| value.timestamp()) != Some(self.expires_at) {
            return Err(io::Error::other("command expiry mismatch"));
        }
        let claims = crate::token_data::parse_chatgpt_jwt_claims(&self.access_token)
            .map_err(|_| io::Error::other("invalid credential claims"))?;
        if claims.chatgpt_account_id.as_deref() != Some(self.account.as_str())
            || claims.chatgpt_user_id.as_deref() != Some(self.principal.as_str())
        {
            return Err(io::Error::other("command principal mismatch"));
        }
        CodexAuth::from_external_chatgpt_tokens(
            &self.access_token,
            &self.account,
            /*chatgpt_plan_type*/ None,
        )
        .map_err(|_| io::Error::other("invalid external credential"))
    }
}

#[derive(Clone, PartialEq, Eq)]
struct Selection {
    home: PathBuf,
    command: PathBuf,
    binding: String,
    source: String,
    account: String,
    principal: String,
}

pub(super) struct CommandSource {
    selection: Selection,
    gate: Semaphore,
    pub state: Mutex<Option<Envelope>>,
    rejected: Mutex<HashSet<String>>,
    retry_after: AtomicI64,
    pub managers: Mutex<Vec<Weak<super::AuthManager>>>,
}

type Sessions = HashMap<PathBuf, Weak<CommandSource>>;
static SESSIONS: OnceLock<Mutex<Sessions>> = OnceLock::new();

impl CommandSource {
    pub fn selected(config: &AuthConfig) -> io::Result<Option<Arc<Self>>> {
        let Some(command) =
            std::env::var_os("CODEX_CHATGPT_AUTH_COMMAND").filter(|value| !value.is_empty())
        else {
            return Ok(None);
        };
        if !cfg!(target_os = "macos")
            || config.auth_credentials_store_mode != AuthCredentialsStoreMode::File
        {
            return Err(io::Error::other("command auth requires macOS File storage"));
        }
        let required = |key| {
            std::env::var(key)
                .ok()
                .filter(|v| !v.is_empty())
                .ok_or_else(|| io::Error::other("incomplete command auth selection"))
        };
        let selection = Selection {
            home: config.codex_home.canonicalize()?,
            command: PathBuf::from(command),
            binding: required("CODEX_CHATGPT_AUTH_BINDING")?,
            source: required("CODEX_CHATGPT_AUTH_SOURCE")?,
            account: required("CODEX_CHATGPT_AUTH_ACCOUNT")?,
            principal: required("CODEX_CHATGPT_AUTH_PRINCIPAL")?,
        };
        if !config.is_login_method_allowed(codex_protocol::config_types::ForcedLoginMethod::Chatgpt)
            || config
                .effective_chatgpt_workspaces()
                .is_some_and(|accounts| !accounts.contains(&selection.account))
        {
            return Err(io::Error::other(
                "command auth is forbidden by login policy",
            ));
        }
        if !selection.command.is_absolute() {
            return Err(io::Error::other("absolute command required"));
        }
        let mut sessions = SESSIONS
            .get_or_init(Mutex::default)
            .lock()
            .map_err(|_| io::Error::other("command registry unavailable"))?;
        if let Some(session) = sessions.get(&selection.home).and_then(Weak::upgrade) {
            if session.selection != selection {
                return Err(io::Error::other("conflicting command source"));
            }
            return Ok(Some(session));
        }
        let current = command_store::read(&selection.home)?;
        if current.is_some() && !command_store::is_protected(&selection.home.join("auth.json")) {
            return Err(io::Error::other("owned auth protection unsupported"));
        }
        if let Some(record) = &current
            && (record.source != selection.source
                || record.binding != selection.binding
                || record.account != selection.account
                || record.principal != selection.principal)
        {
            return Err(io::Error::other("account conflict"));
        }
        if let Some(record) = &current {
            command_store::recover_marker(&selection.home, record)?;
        }
        let session = Arc::new(Self {
            selection,
            gate: Semaphore::new(1),
            state: Mutex::new(current),
            rejected: Mutex::new(HashSet::new()),
            retry_after: AtomicI64::new(0),
            managers: Mutex::new(Vec::new()),
        });
        sessions.insert(session.selection.home.clone(), Arc::downgrade(&session));
        Ok(Some(session))
    }

    pub fn matches(&self, envelope: &Envelope) -> bool {
        envelope.source == self.selection.source
            && envelope.binding == self.selection.binding
            && envelope.account == self.selection.account
            && envelope.principal == self.selection.principal
    }

    pub fn accepts(&self, auth: Option<&CodexAuth>, state: &Option<Envelope>) -> bool {
        state.as_ref().is_some_and(|record| {
            auth.and_then(|auth| auth.get_token_data().ok())
                .is_some_and(|tokens| tokens.access_token == record.access_token)
        })
    }

    pub fn usable_auth(&self, auth: Option<&CodexAuth>) -> bool {
        let Ok(state) = self.state.lock() else {
            return false;
        };
        let Ok(rejected) = self.rejected.lock() else {
            return false;
        };
        state.as_ref().is_some_and(|record| {
            record.usable()
                && !rejected.contains(&record.fingerprint)
                && self.accepts(auth, &state)
                && command_store::is_protected(&self.selection.home.join("auth.json"))
                && command_store::read(&self.selection.home)
                    .ok()
                    .flatten()
                    .as_ref()
                    == Some(record)
        })
    }

    pub fn receipt(&self, auth: Option<&CodexAuth>) -> serde_json::Value {
        let Ok(state) = self.state.lock() else {
            return serde_json::Value::Null;
        };
        let Some(current) = state.as_ref() else {
            return serde_json::Value::Null;
        };
        let durable = command_store::read(&self.selection.home).ok().flatten();
        let managers_coherent = self.managers.lock().is_ok_and(|managers| {
            managers
                .iter()
                .filter_map(Weak::upgrade)
                .all(|manager| self.accepts(manager.auth_cached().as_ref(), &state))
        });
        let mode_valid = command_store::is_protected(&self.selection.home.join("auth.json"));
        let not_rejected = self
            .rejected
            .lock()
            .is_ok_and(|rejected| !rejected.contains(&current.fingerprint));
        let coherent = mode_valid
            && not_rejected
            && current.usable()
            && managers_coherent
            && self.accepts(auth, &state)
            && durable
                .as_ref()
                .is_some_and(|record| record == current && self.matches(record));
        serde_json::json!({"source":current.source,"generation":current.generation,"expiresAt":current.expires_at,"coherent":coherent,"protectionMode":if mode_valid { "0400" } else { "unknown" }})
    }

    fn publish(&self, record: Envelope) -> io::Result<CodexAuth> {
        let auth = record.auth()?;
        let replaced = self
            .state
            .lock()
            .map_err(|_| io::Error::other("command state unavailable"))?
            .as_ref()
            .is_some_and(|old| old.fingerprint != record.fingerprint);
        let fingerprint = record.fingerprint.clone();
        *self
            .state
            .lock()
            .map_err(|_| io::Error::other("command state unavailable"))? = Some(record);
        if replaced {
            // Another process may have published the credential rejected by
            // this request. Importing it must preserve that exact rejection.
            self.rejected
                .lock()
                .map_err(|_| io::Error::other("rejection state unavailable"))?
                .retain(|rejected| rejected == &fingerprint);
        }
        let managers: Vec<_> = self
            .managers
            .lock()
            .map_err(|_| io::Error::other("manager registry unavailable"))?
            .iter()
            .filter_map(Weak::upgrade)
            .collect();
        for manager in managers {
            manager.set_cached_auth(Some(auth.clone()));
        }
        Ok(auth)
    }

    pub(super) async fn obtain(&self, failed: Option<String>) -> io::Result<CodexAuth> {
        let _gate = tokio::time::timeout(Duration::from_secs(10), self.gate.acquire())
            .await
            .map_err(|_| io::Error::other("command gate timeout"))?
            .map_err(|_| io::Error::other("command gate closed"))?;
        let current_fingerprint = self
            .state
            .lock()
            .map_err(|_| io::Error::other("command state unavailable"))?
            .as_ref()
            .map(|record| record.fingerprint.clone());
        {
            let mut rejected = self
                .rejected
                .lock()
                .map_err(|_| io::Error::other("command rejection state unavailable"))?;
            if let Some(fingerprint) = failed.as_ref() {
                if rejected.len() >= 16 && !rejected.contains(fingerprint) {
                    // Stale request fingerprints must not prevent marking the
                    // actually rejected credential. Keep current rejection,
                    // then record this request within the same bounded set.
                    rejected.retain(|stored| current_fingerprint.as_ref() == Some(stored));
                }
                rejected.insert(fingerprint.clone());
            }
        }
        if let Some(durable) = command_store::read(&self.selection.home)? {
            if !self.matches(&durable) {
                return Err(io::Error::other("durable account conflict"));
            }
            let newer = self
                .state
                .lock()
                .map_err(|_| io::Error::other("command state unavailable"))?
                .as_ref()
                .is_none_or(|current| durable.generation > current.generation);
            if newer && durable.usable() {
                self.publish(durable)?;
            }
        }
        let current = self
            .state
            .lock()
            .map_err(|_| io::Error::other("command state unavailable"))?
            .clone();
        // Determine rejection against the adopted record, after any disk import.
        let failed = {
            let rejected = self
                .rejected
                .lock()
                .map_err(|_| io::Error::other("command rejection state unavailable"))?;
            current
                .as_ref()
                .filter(|record| rejected.contains(&record.fingerprint))
                .map(|record| record.fingerprint.clone())
        };
        if let Some(record) = &current
            && record.expires_at > chrono::Utc::now().timestamp() + 300
            && failed.as_deref() != Some(record.fingerprint.as_str())
        {
            return record.auth();
        }
        let result = if self.retry_after.load(Ordering::Acquire) > chrono::Utc::now().timestamp() {
            Err(io::Error::other("provider retry pending"))
        } else {
            self.invoke(failed.as_deref()).await
        };
        let record = match result {
            Ok(record) => record,
            Err(error) => {
                // Do not extend a pending backoff on every hot-path read.
                let now = chrono::Utc::now().timestamp();
                if self.retry_after.load(Ordering::Acquire) <= now {
                    self.retry_after.store(now + 5, Ordering::Release);
                }
                if let Some(record) = current
                    && record.usable()
                    && failed.as_deref() != Some(record.fingerprint.as_str())
                {
                    return record.auth();
                }
                return Err(error);
            }
        };
        self.retry_after.store(0, Ordering::Release);
        if !self.matches(&record) || failed.as_deref() == Some(record.fingerprint.as_str()) {
            return Err(io::Error::other("command credential conflict"));
        }
        let auth = record.auth()?;
        let home = self.selection.home.clone();
        let durable_record = record.clone();
        let durable_auth = auth.clone();
        tokio::task::spawn_blocking(move || {
            command_store::commit(&home, &durable_record, &durable_auth)
        })
        .await
        .map_err(|_| io::Error::other("credential commit unavailable"))??;
        self.publish(record)
    }

    #[cfg(unix)]
    async fn invoke(&self, failed: Option<&str>) -> io::Result<Envelope> {
        use std::os::fd::AsRawFd;
        use std::os::unix::process::CommandExt;
        use tokio::io::AsyncBufReadExt;
        use tokio::io::AsyncWriteExt;
        // Both endpoints are born in this process: the child's endpoint exposes
        // this worker's audit-token identity to the supplier.
        let (ours, theirs) = std::os::unix::net::UnixStream::pair()?;
        ours.set_nonblocking(true)?;
        let descriptor = theirs.as_raw_fd();
        let mut command = std::process::Command::new(&self.selection.command);
        command.args([
            "token",
            "--binding",
            &self.selection.binding,
            "--fd",
            &descriptor.to_string(),
        ]);
        command.process_group(0);
        command
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        for (key, _) in std::env::vars_os() {
            let name = key.to_string_lossy();
            if name.starts_with("CODEX_")
                || name.starts_with("KCF_")
                || name.starts_with("CSR_")
                || name.starts_with("OPENAI_")
            {
                command.env_remove(key);
            }
        }
        unsafe {
            command.pre_exec(move || {
                unsafe extern "C" {
                    fn fcntl(fd: i32, command: i32, ...) -> i32;
                }
                // F_SETFD=2, flags=0: private endpoint survives this one exec.
                if fcntl(descriptor, 2, 0) == -1 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = OwnedCommand(
            tokio::process::Command::from(command)
                .kill_on_drop(true)
                .spawn()?,
        );
        drop(theirs);
        let stream = tokio::net::UnixStream::from_std(ours)?;
        let (reader, mut writer) = stream.into_split();
        let request = serde_json::json!({"version":1,"source":self.selection.source,"account":self.selection.account,"principal":self.selection.principal,"failedFingerprint":failed});
        let exchange = tokio::time::timeout(Duration::from_secs(10), async {
            writer.write_all(format!("{request}\n").as_bytes()).await?;
            let reader = tokio::io::BufReader::new(reader);
            let mut bytes = Vec::new();
            // Bound input before parsing; access tokens never enter stdout/logs.
            use tokio::io::AsyncReadExt;
            reader.take(65537).read_until(b'\n', &mut bytes).await?;
            if bytes.len() > 65536 {
                return Err(io::Error::other("command output limit"));
            }
            let response: serde_json::Value = serde_json::from_slice(&bytes)
                .map_err(|_| io::Error::other("invalid command response"))?;
            serde_json::from_value(
                response
                    .get("ok")
                    .cloned()
                    .ok_or_else(|| io::Error::other("provider unavailable"))?,
            )
            .map_err(|_| io::Error::other("invalid command envelope"))
        })
        .await;
        // The owned child has not been reaped, so its process group cannot be
        // confused with an arbitrary service PID. Its upstream owner/helpers
        // share this group and must not outlive a cancelled command.
        if let Some(id) = child.0.id() {
            unsafe extern "C" {
                fn kill(pid: i32, signal: i32) -> i32;
            }
            unsafe {
                kill(-(id as i32), 15);
            }
            if tokio::time::timeout(Duration::from_secs(1), child.0.wait())
                .await
                .is_err()
            {
                unsafe {
                    kill(-(id as i32), 9);
                }
                let _ = child.0.wait().await;
            }
        }
        exchange.map_err(|_| io::Error::other("provider timeout"))?
    }

    #[cfg(not(unix))]
    async fn invoke(&self, _failed: Option<&str>) -> io::Result<Envelope> {
        Err(io::Error::other("command auth unsupported"))
    }
}

#[cfg(unix)]
struct OwnedCommand(tokio::process::Child);

#[cfg(unix)]
impl Drop for OwnedCommand {
    fn drop(&mut self) {
        if let Some(id) = self.0.id() {
            unsafe extern "C" {
                fn kill(pid: i32, signal: i32) -> i32;
            }
            // Cancel only the private group born with this still-owned child.
            unsafe {
                kill(-(id as i32), 9);
            }
        }
    }
}

impl ExternalAuth for Arc<CommandSource> {
    fn resolve(&self) -> ExternalAuthFuture<'_, CodexAuth> {
        Box::pin(self.obtain(None))
    }
    fn refresh(&self, _context: ExternalAuthRefreshContext) -> ExternalAuthFuture<'_, CodexAuth> {
        Box::pin(async move {
            let failed = self
                .state
                .lock()
                .map_err(|_| io::Error::other("command state unavailable"))?
                .as_ref()
                .map(|record| record.fingerprint.clone());
            self.obtain(failed).await
        })
    }
}

#[cfg(test)]
#[path = "command_source_tests.rs"]
mod tests;
