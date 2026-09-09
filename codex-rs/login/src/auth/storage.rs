use chrono::DateTime;
use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use tracing::warn;

use super::BedrockApiKeyAuth;
use crate::token_data::TokenData;
use codex_agent_identity::AgentIdentityJwtClaims;
use codex_agent_identity::decode_agent_identity_jwt;
use codex_config::types::AuthCredentialsStoreMode;
pub use codex_config::types::AuthKeyringBackendKind;
use codex_keyring_store::DefaultKeyringStore;
use codex_keyring_store::KeyringStore;
use codex_protocol::account::PlanType as AccountPlanType;
use codex_protocol::auth::AuthMode;
use codex_secrets::LocalSecretsBackend;
use codex_secrets::LocalSecretsNamespace;
use codex_secrets::SecretName;
use codex_secrets::SecretScope;
use codex_secrets::SecretsBackendKind;
use codex_secrets::SecretsManager;
use once_cell::sync::Lazy;

/// Expected structure for $CODEX_HOME/auth.json.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct AuthDotJson {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_mode: Option<AuthMode>,

    #[serde(rename = "OPENAI_API_KEY")]
    pub openai_api_key: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokens: Option<TokenData>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_refresh: Option<DateTime<Utc>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_identity: Option<AgentIdentityStorage>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub personal_access_token: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bedrock_api_key: Option<BedrockApiKeyAuth>,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(untagged)]
pub enum AgentIdentityStorage {
    Jwt(String),
    Record(AgentIdentityAuthRecord),
}

impl AgentIdentityStorage {
    pub fn has_auth_material(&self) -> bool {
        match self {
            Self::Jwt(jwt) => !jwt.trim().is_empty(),
            Self::Record(record) => {
                !record.agent_runtime_id.trim().is_empty()
                    && !record.agent_private_key.trim().is_empty()
            }
        }
    }

    pub(crate) fn as_record(&self) -> Option<&AgentIdentityAuthRecord> {
        match self {
            Self::Jwt(_) => None,
            Self::Record(record) => Some(record),
        }
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
pub struct AgentIdentityAuthRecord {
    pub agent_runtime_id: String,
    pub agent_private_key: String,
    pub account_id: String,
    pub chatgpt_user_id: String,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_empty_string",
        serialize_with = "serialize_optional_string_as_empty"
    )]
    pub email: Option<String>,
    pub plan_type: AccountPlanType,
    pub chatgpt_account_is_fedramp: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
}

fn deserialize_optional_non_empty_string<'de, D>(
    deserializer: D,
) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer).map(|value| value.filter(|value| !value.is_empty()))
}

fn serialize_optional_string_as_empty<S>(
    value: &Option<String>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    value.as_deref().unwrap_or_default().serialize(serializer)
}

impl AgentIdentityAuthRecord {
    pub(crate) fn from_agent_identity_jwt(jwt: &str) -> std::io::Result<Self> {
        let claims =
            decode_agent_identity_jwt(jwt, /*jwks*/ None).map_err(std::io::Error::other)?;

        Ok(claims.into())
    }
}

impl From<AgentIdentityJwtClaims> for AgentIdentityAuthRecord {
    fn from(claims: AgentIdentityJwtClaims) -> Self {
        Self {
            agent_runtime_id: claims.agent_runtime_id,
            agent_private_key: claims.agent_private_key,
            account_id: claims.account_id,
            chatgpt_user_id: claims.chatgpt_user_id,
            email: claims.email,
            plan_type: claims.plan_type.into(),
            chatgpt_account_is_fedramp: claims.chatgpt_account_is_fedramp,
            task_id: None,
        }
    }
}

pub(super) fn get_auth_file(codex_home: &Path) -> PathBuf {
    codex_home.join("auth.json")
}

pub(super) fn delete_file_if_exists(codex_home: &Path) -> std::io::Result<bool> {
    let auth_file = get_auth_file(codex_home);
    match std::fs::remove_file(&auth_file) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

pub(super) trait AuthStorageBackend: Debug + Send + Sync {
    fn load(&self) -> std::io::Result<Option<AuthDotJson>>;
    /// Reads the managed transition's persisted source without lossy fallback.
    fn load_managed(&self) -> Result<ManagedAuthStorageRead, ManagedAuthStorageError> {
        self.read_managed_bytes()?.into_parsed()
    }
    /// Revalidates only source bytes, never parses or adopts a replacement auth.
    fn verify_managed_preimage(
        &self,
        expected: &ManagedAuthStoragePreimage,
    ) -> Result<bool, ManagedAuthStorageError> {
        Ok(self.read_managed_bytes()?.preimage == *expected)
    }
    fn read_managed_bytes(&self) -> Result<ManagedAuthStorageBytes, ManagedAuthStorageError>;
    fn lock_managed_source(&self) -> Result<ManagedAuthSourceGuard<'_>, ManagedAuthStorageError> {
        Err(ManagedAuthStorageError::ReadFailed(
            ManagedAuthStorageFailure::UnsupportedStorage,
        ))
    }
    fn save(&self, auth: &AuthDotJson) -> std::io::Result<()>;
    fn delete(&self) -> std::io::Result<bool>;
}

/// Provenance of the single parsed snapshot used by managed adoption.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManagedAuthStorageSource {
    File,
    Keyring,
    Secrets,
    FileAfterKeyringAbsence,
}

/// Classified failures contain no credential, path, or backend error text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManagedAuthStorageFailure {
    FileIo(std::io::ErrorKind),
    Parse(ManagedAuthStorageSource),
    Keyring,
    Secrets,
    UnsupportedStorage,
    CoordinationUnavailable(std::io::ErrorKind),
    CoordinationContended,
}

/// Holds repository-owned durable writers out until cache installation finishes.
/// The coordination file is permanent: unlinking it would split the lock domain.
pub(super) struct ManagedAuthSourceGuard<'a> {
    _file: File,
    backend: &'a dyn AuthStorageBackend,
}

impl Debug for ManagedAuthSourceGuard<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ManagedAuthSourceGuard([locked])")
    }
}

impl ManagedAuthSourceGuard<'_> {
    pub(super) fn verify_managed_preimage(
        &self,
        expected: &ManagedAuthStoragePreimage,
    ) -> Result<bool, ManagedAuthStorageError> {
        self.backend.verify_managed_preimage(expected)
    }
}

#[derive(Debug)]
struct LockedAuthStorage {
    codex_home: PathBuf,
    backend: Arc<dyn AuthStorageBackend>,
}

const MANAGED_AUTH_COORDINATION_FILE: &str = ".managed-auth-source.lock";

fn coordination_error(error: std::io::Error) -> ManagedAuthStorageError {
    ManagedAuthStorageError::ReadFailed(ManagedAuthStorageFailure::CoordinationUnavailable(
        error.kind(),
    ))
}

fn acquire_auth_source_lock(codex_home: &Path) -> Result<File, ManagedAuthStorageError> {
    let canonical_home = codex_home.canonicalize().map_err(coordination_error)?;
    let path = canonical_home.join(MANAGED_AUTH_COORDINATION_FILE);
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let file = match options.open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata = std::fs::symlink_metadata(&path).map_err(coordination_error)?;
            if !metadata.file_type().is_file() {
                return Err(coordination_error(std::io::Error::from(
                    std::io::ErrorKind::PermissionDenied,
                )));
            }
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .map_err(coordination_error)?
        }
        Err(error) => return Err(coordination_error(error)),
    };
    let path_metadata = std::fs::symlink_metadata(&path).map_err(coordination_error)?;
    let file_metadata = file.metadata().map_err(coordination_error)?;
    if !path_metadata.file_type().is_file() || !file_metadata.file_type().is_file() {
        return Err(coordination_error(std::io::Error::from(
            std::io::ErrorKind::PermissionDenied,
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let home_metadata = std::fs::metadata(&canonical_home).map_err(coordination_error)?;
        if path_metadata.dev() != file_metadata.dev()
            || path_metadata.ino() != file_metadata.ino()
            || file_metadata.nlink() != 1
            || file_metadata.uid() != home_metadata.uid()
            || file_metadata.mode() & 0o077 != 0
        {
            return Err(coordination_error(std::io::Error::from(
                std::io::ErrorKind::PermissionDenied,
            )));
        }
    }
    file.try_lock().map_err(|error| match error {
        std::fs::TryLockError::WouldBlock => {
            ManagedAuthStorageError::ReadFailed(ManagedAuthStorageFailure::CoordinationContended)
        }
        std::fs::TryLockError::Error(error) => coordination_error(error),
    })?;
    Ok(file)
}

impl AuthStorageBackend for LockedAuthStorage {
    fn load(&self) -> std::io::Result<Option<AuthDotJson>> {
        self.backend.load()
    }

    fn load_managed(&self) -> Result<ManagedAuthStorageRead, ManagedAuthStorageError> {
        let guard = self.lock_managed_source()?;
        guard.backend.load_managed()
    }

    fn lock_managed_source(&self) -> Result<ManagedAuthSourceGuard<'_>, ManagedAuthStorageError> {
        Ok(ManagedAuthSourceGuard {
            _file: acquire_auth_source_lock(&self.codex_home)?,
            backend: self.backend.as_ref(),
        })
    }

    fn verify_managed_preimage(
        &self,
        expected: &ManagedAuthStoragePreimage,
    ) -> Result<bool, ManagedAuthStorageError> {
        self.lock_managed_source()?
            .verify_managed_preimage(expected)
    }

    fn read_managed_bytes(&self) -> Result<ManagedAuthStorageBytes, ManagedAuthStorageError> {
        let guard = self.lock_managed_source()?;
        guard.backend.read_managed_bytes()
    }

    fn save(&self, auth: &AuthDotJson) -> std::io::Result<()> {
        // Match the ordinary file writer's ability to initialize CODEX_HOME.
        std::fs::create_dir_all(&self.codex_home)?;
        let _guard = self.lock_managed_source().map_err(std::io::Error::other)?;
        self.backend.save(auth)
    }

    fn delete(&self) -> std::io::Result<bool> {
        std::fs::create_dir_all(&self.codex_home)?;
        let _guard = self.lock_managed_source().map_err(std::io::Error::other)?;
        self.backend.delete()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManagedAuthStorageError {
    ReadFailed(ManagedAuthStorageFailure),
    /// Ordinary Auto loading would hide this failure by trying another source.
    /// A transition must instead refuse; fallback absence is not logout proof.
    AutoFallbackRequired(ManagedAuthStorageFailure),
}

impl std::fmt::Display for ManagedAuthStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "managed auth storage unavailable: {self:?}")
    }
}

impl std::error::Error for ManagedAuthStorageError {}

pub(super) struct ManagedAuthStorageRead {
    pub auth: Option<AuthDotJson>,
    pub source: ManagedAuthStorageSource,
    pub preimage: ManagedAuthStoragePreimage,
}

/// Process-local comparison evidence; never formatted as a credential digest.
#[derive(Clone, PartialEq, Eq)]
pub(super) struct ManagedAuthStoragePreimage([u8; 32]);

impl Debug for ManagedAuthStoragePreimage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ManagedAuthStoragePreimage([redacted])")
    }
}

pub(super) struct ManagedAuthStorageBytes {
    bytes: Option<Vec<u8>>,
    source: ManagedAuthStorageSource,
    preimage: ManagedAuthStoragePreimage,
}

impl ManagedAuthStorageBytes {
    fn into_parsed(self) -> Result<ManagedAuthStorageRead, ManagedAuthStorageError> {
        let auth = self
            .bytes
            .as_deref()
            .map(serde_json::from_slice)
            .transpose()
            .map_err(|_| {
                ManagedAuthStorageError::ReadFailed(ManagedAuthStorageFailure::Parse(self.source))
            })?;
        Ok(ManagedAuthStorageRead {
            auth,
            source: self.source,
            preimage: self.preimage,
        })
    }
    fn new(
        bytes: Option<Vec<u8>>,
        source: ManagedAuthStorageSource,
        identity: &[u8],
        backend_evidence: &[u8],
    ) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"codex-managed-auth/storage-preimage/v1\0");
        hasher.update([match source {
            ManagedAuthStorageSource::File => 0,
            ManagedAuthStorageSource::Keyring => 1,
            ManagedAuthStorageSource::Secrets => 2,
            ManagedAuthStorageSource::FileAfterKeyringAbsence => 3,
        }]);
        hasher.update((identity.len() as u64).to_le_bytes());
        hasher.update(identity);
        hasher.update((backend_evidence.len() as u64).to_le_bytes());
        hasher.update(backend_evidence);
        hasher.update([u8::from(bytes.is_some())]);
        if let Some(bytes) = &bytes {
            hasher.update(bytes);
        }
        Self {
            bytes,
            source,
            preimage: ManagedAuthStoragePreimage(hasher.finalize().into()),
        }
    }
}

impl Debug for ManagedAuthStorageRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagedAuthStorageRead")
            .field("source", &self.source)
            .field("auth_present", &self.auth.is_some())
            .finish()
    }
}

#[derive(Clone, Debug)]
pub(super) struct FileAuthStorage {
    codex_home: PathBuf,
}

impl FileAuthStorage {
    pub(super) fn new(codex_home: PathBuf) -> Self {
        Self { codex_home }
    }

    /// Attempt to read and parse the `auth.json` file in the given `CODEX_HOME` directory.
    /// Returns the full AuthDotJson structure.
    pub(super) fn try_read_auth_json(&self, auth_file: &Path) -> std::io::Result<AuthDotJson> {
        let mut file = File::open(auth_file)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let auth_dot_json: AuthDotJson = serde_json::from_str(&contents)?;

        Ok(auth_dot_json)
    }
}

impl AuthStorageBackend for FileAuthStorage {
    fn read_managed_bytes(&self) -> Result<ManagedAuthStorageBytes, ManagedAuthStorageError> {
        let canonical_home = self.codex_home.canonicalize().map_err(|error| {
            ManagedAuthStorageError::ReadFailed(ManagedAuthStorageFailure::FileIo(error.kind()))
        })?;
        // Only opening a genuinely absent source establishes absence. A read
        // error after opening (including NotFound) never becomes logout proof.
        let mut file = match File::open(get_auth_file(&self.codex_home)) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(ManagedAuthStorageBytes::new(
                    None,
                    ManagedAuthStorageSource::File,
                    canonical_home.as_os_str().as_encoded_bytes(),
                    &[],
                ));
            }
            Err(error) => {
                return Err(ManagedAuthStorageError::ReadFailed(
                    ManagedAuthStorageFailure::FileIo(error.kind()),
                ));
            }
        };
        let mut contents = Vec::new();
        file.read_to_end(&mut contents).map_err(|error| {
            ManagedAuthStorageError::ReadFailed(ManagedAuthStorageFailure::FileIo(error.kind()))
        })?;
        Ok(ManagedAuthStorageBytes::new(
            Some(contents),
            ManagedAuthStorageSource::File,
            canonical_home.as_os_str().as_encoded_bytes(),
            &[],
        ))
    }

    fn load(&self) -> std::io::Result<Option<AuthDotJson>> {
        let auth_file = get_auth_file(&self.codex_home);
        let auth_dot_json = match self.try_read_auth_json(&auth_file) {
            Ok(auth) => auth,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err),
        };
        Ok(Some(auth_dot_json))
    }

    fn save(&self, auth_dot_json: &AuthDotJson) -> std::io::Result<()> {
        let auth_file = get_auth_file(&self.codex_home);

        if let Some(parent) = auth_file.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let json_data = serde_json::to_string_pretty(auth_dot_json)?;
        let mut options = OpenOptions::new();
        options.truncate(true).write(true).create(true);
        #[cfg(unix)]
        {
            options.mode(0o600);
        }
        let mut file = options.open(auth_file)?;
        file.write_all(json_data.as_bytes())?;
        file.flush()?;
        Ok(())
    }

    fn delete(&self) -> std::io::Result<bool> {
        delete_file_if_exists(&self.codex_home)
    }
}

static CODEX_AUTH_SECRET_NAME: Lazy<SecretName> =
    Lazy::new(|| match SecretName::new("CODEX_AUTH") {
        Ok(name) => name,
        Err(err) => unreachable!("CODEX_AUTH should be a valid secret name: {err}"),
    });
const KEYRING_SERVICE: &str = "Codex Auth";

// turns codex_home path into a stable, short key string
fn compute_store_key(codex_home: &Path) -> std::io::Result<String> {
    let canonical = codex_home
        .canonicalize()
        .unwrap_or_else(|_| codex_home.to_path_buf());
    let path_str = canonical.to_string_lossy();
    let mut hasher = Sha256::new();
    hasher.update(path_str.as_bytes());
    let digest = hasher.finalize();
    let hex = format!("{digest:x}");
    let truncated = hex.get(..16).unwrap_or(&hex);
    Ok(format!("cli|{truncated}"))
}

fn compute_managed_store_key(codex_home: &Path) -> Result<String, ManagedAuthStorageError> {
    let canonical = codex_home
        .canonicalize()
        .map_err(|_| ManagedAuthStorageError::ReadFailed(ManagedAuthStorageFailure::Keyring))?;
    let mut hasher = Sha256::new();
    hasher.update(canonical.to_string_lossy().as_bytes());
    let hex = format!("{:x}", hasher.finalize());
    let truncated = hex.get(..16).unwrap_or(&hex);
    Ok(format!("cli|{truncated}"))
}

#[derive(Clone, Debug)]
struct DirectKeyringAuthStorage {
    codex_home: PathBuf,
    keyring_store: Arc<dyn KeyringStore>,
}

impl DirectKeyringAuthStorage {
    fn new(codex_home: PathBuf, keyring_store: Arc<dyn KeyringStore>) -> Self {
        Self {
            codex_home,
            keyring_store,
        }
    }

    fn load_from_keyring(&self, key: &str) -> std::io::Result<Option<AuthDotJson>> {
        match self.keyring_store.load(KEYRING_SERVICE, key) {
            Ok(Some(serialized)) => serde_json::from_str(&serialized).map(Some).map_err(|err| {
                std::io::Error::other(format!(
                    "failed to deserialize CLI auth from keyring: {err}"
                ))
            }),
            Ok(None) => Ok(None),
            Err(error) => Err(std::io::Error::other(format!(
                "failed to load CLI auth from keyring: {}",
                error.message()
            ))),
        }
    }

    fn save_to_keyring(&self, key: &str, value: &str) -> std::io::Result<()> {
        match self.keyring_store.save(KEYRING_SERVICE, key, value) {
            Ok(()) => Ok(()),
            Err(error) => {
                let message = format!(
                    "failed to write OAuth tokens to keyring: {}",
                    error.message()
                );
                warn!("{message}");
                Err(std::io::Error::other(message))
            }
        }
    }
}

impl AuthStorageBackend for DirectKeyringAuthStorage {
    fn read_managed_bytes(&self) -> Result<ManagedAuthStorageBytes, ManagedAuthStorageError> {
        let key = compute_managed_store_key(&self.codex_home)?;
        match self.keyring_store.load(KEYRING_SERVICE, &key) {
            Ok(Some(serialized)) => Ok(ManagedAuthStorageBytes::new(
                Some(serialized.into_bytes()),
                ManagedAuthStorageSource::Keyring,
                key.as_bytes(),
                &[],
            )),
            Ok(None) => Ok(ManagedAuthStorageBytes::new(
                None,
                ManagedAuthStorageSource::Keyring,
                key.as_bytes(),
                &[],
            )),
            Err(_) => Err(ManagedAuthStorageError::ReadFailed(
                ManagedAuthStorageFailure::Keyring,
            )),
        }
    }

    fn load(&self) -> std::io::Result<Option<AuthDotJson>> {
        let key = compute_store_key(&self.codex_home)?;
        self.load_from_keyring(&key)
    }

    fn save(&self, auth: &AuthDotJson) -> std::io::Result<()> {
        let key = compute_store_key(&self.codex_home)?;
        // Simpler error mapping per style: prefer method reference over closure
        let serialized = serde_json::to_string(auth).map_err(std::io::Error::other)?;
        self.save_to_keyring(&key, &serialized)?;
        if let Err(err) = delete_file_if_exists(&self.codex_home) {
            warn!("failed to remove CLI auth fallback file: {err}");
        }
        Ok(())
    }

    fn delete(&self) -> std::io::Result<bool> {
        let key = compute_store_key(&self.codex_home)?;
        let keyring_removed = self
            .keyring_store
            .delete(KEYRING_SERVICE, &key)
            .map_err(|err| {
                std::io::Error::other(format!("failed to delete auth from keyring: {err}"))
            })?;
        let file_removed = delete_file_if_exists(&self.codex_home)?;
        Ok(keyring_removed || file_removed)
    }
}

#[derive(Clone)]
struct SecretsKeyringAuthStorage {
    codex_home: PathBuf,
    direct_storage: DirectKeyringAuthStorage,
    secrets_manager: SecretsManager,
}

impl Debug for SecretsKeyringAuthStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecretsKeyringAuthStorage")
            .field("codex_home", &self.codex_home)
            .finish_non_exhaustive()
    }
}

impl SecretsKeyringAuthStorage {
    fn new(codex_home: PathBuf, keyring_store: Arc<dyn KeyringStore>) -> Self {
        let direct_storage =
            DirectKeyringAuthStorage::new(codex_home.clone(), Arc::clone(&keyring_store));
        let secrets_manager = SecretsManager::new_with_keyring_store_and_namespace(
            codex_home.clone(),
            SecretsBackendKind::Local,
            keyring_store,
            LocalSecretsNamespace::CodexAuth,
        );
        Self {
            codex_home,
            direct_storage,
            secrets_manager,
        }
    }
}

impl AuthStorageBackend for SecretsKeyringAuthStorage {
    fn read_managed_bytes(&self) -> Result<ManagedAuthStorageBytes, ManagedAuthStorageError> {
        let canonical_home = self
            .codex_home
            .canonicalize()
            .map_err(|_| ManagedAuthStorageError::ReadFailed(ManagedAuthStorageFailure::Secrets))?;
        let backend = LocalSecretsBackend::new_with_namespace(
            self.codex_home.clone(),
            Arc::clone(&self.direct_storage.keyring_store),
            LocalSecretsNamespace::CodexAuth,
        );
        let (serialized, ciphertext_preimage) = backend
            .get_existing_with_preimage(&SecretScope::Global, &CODEX_AUTH_SECRET_NAME)
            .map_err(|_| ManagedAuthStorageError::ReadFailed(ManagedAuthStorageFailure::Secrets))?;
        Ok(ManagedAuthStorageBytes::new(
            serialized.map(String::into_bytes),
            ManagedAuthStorageSource::Secrets,
            canonical_home.as_os_str().as_encoded_bytes(),
            &ciphertext_preimage,
        ))
    }

    fn load(&self) -> std::io::Result<Option<AuthDotJson>> {
        match self
            .secrets_manager
            .get(&SecretScope::Global, &CODEX_AUTH_SECRET_NAME)
            .map_err(|err| {
                std::io::Error::other(format!(
                    "failed to load CLI auth from encrypted auth storage: {err}"
                ))
            })? {
            Some(serialized) => serde_json::from_str(&serialized).map(Some).map_err(|err| {
                std::io::Error::other(format!(
                    "failed to deserialize CLI auth from encrypted auth storage: {err}"
                ))
            }),
            None => Ok(None),
        }
    }

    fn save(&self, auth: &AuthDotJson) -> std::io::Result<()> {
        let serialized = serde_json::to_string(auth).map_err(std::io::Error::other)?;
        self.secrets_manager
            .set(&SecretScope::Global, &CODEX_AUTH_SECRET_NAME, &serialized)
            .map_err(|err| {
                let message =
                    format!("failed to write OAuth tokens to encrypted auth storage: {err}");
                warn!("{message}");
                std::io::Error::other(message)
            })?;
        if let Err(err) = delete_file_if_exists(&self.codex_home) {
            warn!("failed to remove CLI auth fallback file: {err}");
        }
        Ok(())
    }

    fn delete(&self) -> std::io::Result<bool> {
        let keyring_removed = self
            .secrets_manager
            .delete(&SecretScope::Global, &CODEX_AUTH_SECRET_NAME)
            .map_err(|err| {
                std::io::Error::other(format!(
                    "failed to delete auth from encrypted auth storage: {err}"
                ))
            })?;
        let file_removed = delete_file_if_exists(&self.codex_home)?;
        let direct_removed = self.direct_storage.delete()?;
        Ok(keyring_removed || file_removed || direct_removed)
    }
}

#[derive(Clone, Debug)]
struct AutoAuthStorage {
    keyring_storage: Arc<dyn AuthStorageBackend>,
    file_storage: Arc<FileAuthStorage>,
}

impl AutoAuthStorage {
    fn new(
        codex_home: PathBuf,
        keyring_store: Arc<dyn KeyringStore>,
        keyring_backend_kind: AuthKeyringBackendKind,
    ) -> Self {
        Self {
            keyring_storage: create_keyring_auth_storage(
                codex_home.clone(),
                keyring_store,
                keyring_backend_kind,
            ),
            file_storage: Arc::new(FileAuthStorage::new(codex_home)),
        }
    }
}

impl AuthStorageBackend for AutoAuthStorage {
    fn load_managed(&self) -> Result<ManagedAuthStorageRead, ManagedAuthStorageError> {
        let read = self.read_managed_bytes()?;
        let primary = matches!(
            read.source,
            ManagedAuthStorageSource::Keyring | ManagedAuthStorageSource::Secrets
        );
        read.into_parsed().map_err(|error| match error {
            ManagedAuthStorageError::ReadFailed(cause) if primary => {
                ManagedAuthStorageError::AutoFallbackRequired(cause)
            }
            error => error,
        })
    }
    fn read_managed_bytes(&self) -> Result<ManagedAuthStorageBytes, ManagedAuthStorageError> {
        match self.keyring_storage.read_managed_bytes() {
            Ok(read) if read.bytes.is_some() => Ok(read),
            Ok(primary) => {
                let read = self.file_storage.read_managed_bytes()?;
                Ok(ManagedAuthStorageBytes::new(
                    read.bytes,
                    ManagedAuthStorageSource::FileAfterKeyringAbsence,
                    &primary.preimage.0,
                    &read.preimage.0,
                ))
            }
            Err(ManagedAuthStorageError::ReadFailed(cause))
            | Err(ManagedAuthStorageError::AutoFallbackRequired(cause)) => {
                Err(ManagedAuthStorageError::AutoFallbackRequired(cause))
            }
        }
    }

    fn load(&self) -> std::io::Result<Option<AuthDotJson>> {
        match self.keyring_storage.load() {
            Ok(Some(auth)) => Ok(Some(auth)),
            Ok(None) => self.file_storage.load(),
            Err(err) => {
                warn!("failed to load CLI auth from keyring, falling back to file storage: {err}");
                self.file_storage.load()
            }
        }
    }

    fn save(&self, auth: &AuthDotJson) -> std::io::Result<()> {
        match self.keyring_storage.save(auth) {
            Ok(()) => Ok(()),
            Err(err) => {
                warn!("failed to save auth to keyring, falling back to file storage: {err}");
                self.file_storage.save(auth)
            }
        }
    }

    fn delete(&self) -> std::io::Result<bool> {
        // Keyring storage will delete from disk as well
        self.keyring_storage.delete()
    }
}

// A global in-memory store for mapping codex_home -> AuthDotJson.
static EPHEMERAL_AUTH_STORE: Lazy<Mutex<HashMap<String, AuthDotJson>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[derive(Clone, Debug)]
struct EphemeralAuthStorage {
    codex_home: PathBuf,
}

impl EphemeralAuthStorage {
    fn new(codex_home: PathBuf) -> Self {
        Self { codex_home }
    }

    fn with_store<F, T>(&self, action: F) -> std::io::Result<T>
    where
        F: FnOnce(&mut HashMap<String, AuthDotJson>, String) -> std::io::Result<T>,
    {
        let key = compute_store_key(&self.codex_home)?;
        let mut store = EPHEMERAL_AUTH_STORE
            .lock()
            .map_err(|_| std::io::Error::other("failed to lock ephemeral auth storage"))?;
        action(&mut store, key)
    }
}

impl AuthStorageBackend for EphemeralAuthStorage {
    fn read_managed_bytes(&self) -> Result<ManagedAuthStorageBytes, ManagedAuthStorageError> {
        Err(ManagedAuthStorageError::ReadFailed(
            ManagedAuthStorageFailure::UnsupportedStorage,
        ))
    }

    fn load(&self) -> std::io::Result<Option<AuthDotJson>> {
        self.with_store(|store, key| Ok(store.get(&key).cloned()))
    }

    fn save(&self, auth: &AuthDotJson) -> std::io::Result<()> {
        self.with_store(|store, key| {
            store.insert(key, auth.clone());
            Ok(())
        })
    }

    fn delete(&self) -> std::io::Result<bool> {
        self.with_store(|store, key| Ok(store.remove(&key).is_some()))
    }
}

pub(super) fn create_auth_storage(
    codex_home: PathBuf,
    mode: AuthCredentialsStoreMode,
    keyring_backend_kind: AuthKeyringBackendKind,
) -> Arc<dyn AuthStorageBackend> {
    let keyring_store: Arc<dyn KeyringStore> = Arc::new(DefaultKeyringStore);
    create_auth_storage_with_store(codex_home, mode, keyring_store, keyring_backend_kind)
}

fn create_auth_storage_with_store(
    codex_home: PathBuf,
    mode: AuthCredentialsStoreMode,
    keyring_store: Arc<dyn KeyringStore>,
    keyring_backend_kind: AuthKeyringBackendKind,
) -> Arc<dyn AuthStorageBackend> {
    if mode == AuthCredentialsStoreMode::Ephemeral {
        return Arc::new(EphemeralAuthStorage::new(codex_home));
    }
    let backend: Arc<dyn AuthStorageBackend> = match mode {
        AuthCredentialsStoreMode::File => Arc::new(FileAuthStorage::new(codex_home.clone())),
        AuthCredentialsStoreMode::Keyring => {
            create_keyring_auth_storage(codex_home.clone(), keyring_store, keyring_backend_kind)
        }
        AuthCredentialsStoreMode::Auto => Arc::new(AutoAuthStorage::new(
            codex_home.clone(),
            keyring_store,
            keyring_backend_kind,
        )),
        AuthCredentialsStoreMode::Ephemeral => {
            Arc::new(EphemeralAuthStorage::new(codex_home.clone()))
        }
    };
    Arc::new(LockedAuthStorage {
        codex_home,
        backend,
    })
}

fn create_keyring_auth_storage(
    codex_home: PathBuf,
    keyring_store: Arc<dyn KeyringStore>,
    keyring_backend_kind: AuthKeyringBackendKind,
) -> Arc<dyn AuthStorageBackend> {
    match keyring_backend_kind {
        AuthKeyringBackendKind::Direct => {
            Arc::new(DirectKeyringAuthStorage::new(codex_home, keyring_store))
        }
        AuthKeyringBackendKind::Secrets => {
            Arc::new(SecretsKeyringAuthStorage::new(codex_home, keyring_store))
        }
    }
}

#[cfg(test)]
#[path = "storage_tests.rs"]
mod tests;
