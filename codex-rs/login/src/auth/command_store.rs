//! Atomic durable external access ownership, under the native auth lock domain.
use super::CodexAuth;
use super::command_source::Envelope;
use super::storage::CommandAuthSourceGuard;
use super::storage::ManagedAuthStorageError;
use super::storage::ManagedAuthStorageFailure;
use super::storage::acquire_command_auth_source;
use serde_json::Value;
#[cfg(unix)]
use std::fs::OpenOptions;
use std::io;
#[cfg(unix)]
use std::io::Write;
use std::path::Path;
use std::time::Duration;
use std::time::Instant;

fn marker_present(home: &Path) -> io::Result<bool> {
    match std::fs::symlink_metadata(home.join(".external-auth-owner.json")) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

pub(super) fn read(home: &Path) -> io::Result<Option<Envelope>> {
    let value: Value = match std::fs::read(home.join("auth.json")) {
        Ok(bytes) => {
            serde_json::from_slice(&bytes).map_err(|_| io::Error::other("invalid auth state"))?
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if marker_present(home)? {
                return Err(io::Error::other("owned auth record is missing"));
            }
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    let Some(owner) = value.get("external_auth_owner") else {
        if marker_present(home)? {
            return Err(io::Error::other("owned auth record was replaced"));
        }
        return Ok(None);
    };
    let record: Envelope = serde_json::from_value(owner.clone())
        .map_err(|_| io::Error::other("invalid external owner"))?;
    let stored: super::AuthDotJson = serde_json::from_value(value.clone())
        .map_err(|_| io::Error::other("invalid native auth record"))?;
    let tokens = stored
        .tokens
        .as_ref()
        .ok_or_else(|| io::Error::other("missing native credentials"))?;
    if value.get("auth_mode").and_then(Value::as_str) != Some("chatgptAuthTokens")
        || tokens.access_token != record.access_token
        || !tokens.refresh_token.is_empty()
        || tokens.account_id.as_deref() != Some(record.account.as_str())
        || tokens.id_token.chatgpt_account_id.as_deref() != Some(record.account.as_str())
        || tokens.id_token.chatgpt_user_id.as_deref() != Some(record.principal.as_str())
    {
        return Err(io::Error::other(
            "external owner and credential state diverge",
        ));
    }
    Ok(Some(record))
}

fn lock(home: &Path) -> io::Result<CommandAuthSourceGuard> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match acquire_command_auth_source(home) {
            Ok(lock) => return Ok(lock),
            Err(ManagedAuthStorageError::ReadFailed(
                ManagedAuthStorageFailure::CoordinationContended,
            )) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            Err(_) => return Err(io::Error::other("auth coordination unavailable")),
        }
    }
}

pub(super) fn locked_current(
    home: &Path,
    expected: &Envelope,
) -> io::Result<CommandAuthSourceGuard> {
    let guard = lock(home)?;
    if read(home)?.as_ref() != Some(expected) {
        return Err(io::Error::other(
            "durable generation changed before cache install",
        ));
    }
    guard.require_current_domain().map_err(io::Error::other)?;
    Ok(guard)
}

pub(super) fn commit(home: &Path, envelope: &Envelope, auth: &CodexAuth) -> io::Result<()> {
    // Validate the exact access-only credential before any durable mutation.
    let expected_auth = envelope.auth()?;
    let mut expected_state = expected_auth
        .get_current_auth_json()
        .ok_or_else(|| io::Error::other("missing validated external auth"))?;
    let supplied_state = auth
        .get_current_auth_json()
        .ok_or_else(|| io::Error::other("missing external auth"))?;
    expected_state.last_refresh = supplied_state.last_refresh;
    if auth.api_auth_mode() != expected_auth.api_auth_mode() || supplied_state != expected_state {
        return Err(io::Error::other("external credential and envelope diverge"));
    }
    let guard = lock(home)?;
    if marker_present(home)? && read(home)?.is_none() {
        return Err(io::Error::other("owned authority is unavailable"));
    }
    if marker_present(home)? {
        owner_marker(home, envelope)?; // Validate existing ownership before mutation.
    }
    if std::fs::symlink_metadata(home.join("auth.json"))
        .is_ok_and(|info| info.file_type().is_symlink())
    {
        return Err(io::Error::other("auth authority must not be a symlink"));
    }
    let old: Option<Value> = match std::fs::read(home.join("auth.json")) {
        Ok(bytes) => Some(
            serde_json::from_slice(&bytes).map_err(|_| io::Error::other("invalid auth state"))?,
        ),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };
    if let Some(value) = old {
        if let Some(owned) = value.get("external_auth_owner") {
            let previous: Envelope = serde_json::from_value(owned.clone())
                .map_err(|_| io::Error::other("invalid external owner"))?;
            if previous.source != envelope.source
                || previous.binding != envelope.binding
                || previous.account != envelope.account
                || previous.principal != envelope.principal
                || previous.generation > envelope.generation
                || (previous.generation == envelope.generation && previous != *envelope)
            {
                return Err(io::Error::other(
                    "obsolete or conflicting external credentials",
                ));
            }
        } else {
            let mode = value
                .get("auth_mode")
                .and_then(Value::as_str)
                .unwrap_or("chatgpt");
            let tokens = value
                .get("tokens")
                .ok_or_else(|| io::Error::other("account conflict"))?;
            let account = tokens.get("account_id").and_then(Value::as_str);
            let id = tokens
                .get("id_token")
                .and_then(Value::as_str)
                .ok_or_else(|| io::Error::other("unprovable account"))?;
            let id = crate::token_data::parse_chatgpt_jwt_claims(id)
                .map_err(|_| io::Error::other("unprovable account"))?;
            if mode != "chatgpt"
                || account != Some(envelope.account.as_str())
                || id.chatgpt_user_id.as_deref() != Some(envelope.principal.as_str())
            {
                return Err(io::Error::other("account conflict"));
            }
        }
    }
    // Publish the validated immutable snapshot, never a second read of a
    // potentially shared auth object after waiting for the native lock.
    let mut value =
        serde_json::to_value(expected_state).map_err(|_| io::Error::other("invalid auth"))?;
    value["external_auth_owner"] =
        serde_json::to_value(envelope).map_err(|_| io::Error::other("invalid owner"))?;
    // Native external-auth format retains an empty refresh field for older readers;
    // no refresh credential is distributed.
    guard.require_current_domain().map_err(io::Error::other)?;
    if envelope.expires_at <= chrono::Utc::now().timestamp() {
        return Err(io::Error::other("command credential expired before commit"));
    }
    atomic_record(
        home,
        "auth.json",
        &serde_json::to_vec(&value).map_err(|_| io::Error::other("invalid record"))?,
    )?;
    guard.require_current_domain().map_err(io::Error::other)?;
    owner_marker(home, envelope)?;
    guard.require_current_domain().map_err(io::Error::other)
}

#[cfg(unix)]
fn atomic_record(home: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::fs::PermissionsExt;
    let staged = home.join(format!(
        ".auth-{}-{}.tmp",
        std::process::id(),
        rand::random::<u64>()
    ));
    let result = (|| {
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&staged)?;
        output.write_all(bytes)?;
        output.set_permissions(std::fs::Permissions::from_mode(0o400))?;
        if !is_protected(&staged) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "read-only auth protection unsupported",
            ));
        }
        output.sync_all()?;
        std::fs::rename(&staged, home.join(name))?;
        std::fs::File::open(home)?.sync_all()
    })();
    if staged.exists() {
        let _ = std::fs::remove_file(staged);
    }
    result
}

#[cfg(not(unix))]
fn atomic_record(_home: &Path, _name: &str, _bytes: &[u8]) -> io::Result<()> {
    Err(io::Error::other("command auth requires macOS File storage"))
}

fn owner_marker(home: &Path, envelope: &Envelope) -> io::Result<()> {
    let expected = serde_json::json!({"version":1,"source":envelope.source,"binding":envelope.binding,"account":envelope.account,"principal":envelope.principal});
    let path = home.join(".external-auth-owner.json");
    if marker_present(home)? {
        let metadata = std::fs::symlink_metadata(&path)?;
        if !metadata.file_type().is_file() {
            return Err(io::Error::other("invalid ownership marker type"));
        }
        let current: Value = serde_json::from_slice(&std::fs::read(path)?)
            .map_err(|_| io::Error::other("invalid ownership marker"))?;
        if current != expected {
            return Err(io::Error::other("ownership marker conflict"));
        }
        return Ok(());
    }
    // This marker is not a second credential/generation record. It makes loss
    // of an acknowledged owned auth file distinguishable from a fresh home.
    atomic_record(
        home,
        ".external-auth-owner.json",
        &serde_json::to_vec(&expected).map_err(|_| io::Error::other("invalid ownership marker"))?,
    )
}

pub(super) fn recover_marker(home: &Path, expected: &Envelope) -> io::Result<()> {
    let guard = locked_current(home, expected)?;
    guard.require_current_domain().map_err(io::Error::other)?;
    owner_marker(home, expected)?;
    guard.require_current_domain().map_err(io::Error::other)
}

/// Opening for write without truncation is a non-mutating permission probe. It
/// catches root/ACL bypass of POSIX0400 before takeover and at receipt time.
#[cfg(unix)]
pub(super) fn is_protected(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::symlink_metadata(path)
        .is_ok_and(|info| info.file_type().is_file() && info.permissions().mode() & 0o777 == 0o400)
        && OpenOptions::new()
            .write(true)
            .open(path)
            .is_err_and(|error| error.kind() == io::ErrorKind::PermissionDenied)
}

#[cfg(not(unix))]
pub(super) fn is_protected(_path: &Path) -> bool {
    false
}

pub(super) fn refuse_native_write(home: &Path) -> io::Result<()> {
    // Ownership evidence, not validity of ordinary managed credentials, fences
    // the native writer. An unenrolled home retains native repair semantics.
    let marker = match std::fs::symlink_metadata(home.join(".external-auth-owner.json")) {
        Ok(_) => true,
        Err(error) if error.kind() == io::ErrorKind::NotFound => false,
        Err(error) => return Err(error),
    };
    let owned_record = match std::fs::read(home.join("auth.json")) {
        Ok(bytes) => serde_json::from_slice::<Value>(&bytes)
            .is_ok_and(|value| value.get("external_auth_owner").is_some()),
        // A directory cannot carry an ownership record; leave its failure to
        // the backend's own write/cleanup path (upstream secure-save semantics).
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::IsADirectory
            ) =>
        {
            false
        }
        Err(error) => return Err(error),
    };
    if marker || owned_record {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "external auth is owned by the configured source",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "command_store_tests.rs"]
mod tests;
