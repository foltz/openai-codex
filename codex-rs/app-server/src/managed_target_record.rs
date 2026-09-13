use crate::managed_transition::MANAGED_PROFILE_ENV_VAR;
use codex_app_server_transport::AppServerTransport;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;
use std::ffi::OsString;
use std::fs::File;
use std::io;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use tempfile::NamedTempFile;
use uuid::Uuid;

pub(crate) const MANAGED_TARGET_RECORD_ENV_VAR: &str = "KESTREL_CODEX_MANAGED_TARGET_RECORD";

/// Startup-only target publication selected explicitly by a disposable
/// qualification harness. Absence of the opt-in environment variable creates
/// no file and leaves ordinary app-server startup unchanged.
pub(crate) struct ManagedTargetRecordSetup {
    pub(crate) process_instance_id: Option<String>,
    pub(crate) control_endpoint: Option<String>,
    publication: Option<ManagedTargetRecordPublication>,
}

impl ManagedTargetRecordSetup {
    pub(crate) fn from_env(transport: &AppServerTransport) -> io::Result<Self> {
        let control_endpoint = match transport {
            AppServerTransport::UnixSocket { socket_path } => {
                Some(socket_path.display().to_string())
            }
            _ => None,
        };
        let Some(record_path) = std::env::var_os(MANAGED_TARGET_RECORD_ENV_VAR) else {
            return Ok(Self {
                process_instance_id: None,
                control_endpoint,
                publication: None,
            });
        };
        let AppServerTransport::UnixSocket { socket_path } = transport else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "managed target record requires a Unix socket transport",
            ));
        };
        let record_path = validated_record_path(record_path)?;
        let profile = required_profile()?;
        let artifact = std::env::current_exe()?.canonicalize()?;
        let process_instance_id = Uuid::now_v7().to_string();
        // This identity names one immutable publication for the process. It
        // is distinct from the process instance because a record publication
        // is independently selected and immutable.
        let record_identity = Uuid::now_v7().to_string();
        let record = ManagedTargetRecord {
            profile,
            artifact: path_text(&artifact)?,
            artifact_sha256: sha256_file(&artifact)?,
            socket: path_text(socket_path.as_path())?,
            pid: std::process::id(),
            process_instance_id: process_instance_id.clone(),
            record_identity,
        };
        Ok(Self {
            process_instance_id: Some(process_instance_id),
            control_endpoint,
            publication: Some(ManagedTargetRecordPublication {
                path: record_path,
                record,
            }),
        })
    }

    pub(crate) fn publish_after_socket_bound(&mut self) -> io::Result<()> {
        let Some(publication) = self.publication.take() else {
            return Ok(());
        };
        publication.publish()
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ManagedTargetRecord {
    profile: String,
    artifact: String,
    artifact_sha256: String,
    socket: String,
    pid: u32,
    process_instance_id: String,
    record_identity: String,
}

struct ManagedTargetRecordPublication {
    path: PathBuf,
    record: ManagedTargetRecord,
}

impl ManagedTargetRecordPublication {
    fn publish(self) -> io::Result<()> {
        let parent = self.path.parent().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "target record has no parent")
        })?;
        let mut temporary = NamedTempFile::new_in(parent)?;
        serde_json::to_writer(temporary.as_file_mut(), &self.record).map_err(io::Error::other)?;
        temporary.as_file_mut().write_all(b"\n")?;
        temporary.as_file_mut().sync_all()?;

        // Linking a complete same-directory temporary inode publishes the
        // record atomically and refuses if the selected path already exists.
        // Dropping `temporary` removes only its private link.
        std::fs::hard_link(temporary.path(), &self.path)
    }
}

fn validated_record_path(value: OsString) -> io::Result<PathBuf> {
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "managed target record path must be absolute",
        ));
    }
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "managed target record path has no parent",
        )
    })?;
    if !parent.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "managed target record parent is unavailable",
        ));
    }
    Ok(path)
}

fn required_profile() -> io::Result<String> {
    let profile = std::env::var(MANAGED_PROFILE_ENV_VAR).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "managed target record requires a managed profile",
        )
    })?;
    if profile.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "managed target record requires a managed profile",
        ));
    }
    Ok(profile)
}

fn path_text(path: &Path) -> io::Result<String> {
    path.to_str().map(str::to_owned).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "managed target record path is not UTF-8",
        )
    })
}

fn sha256_file(path: &Path) -> io::Result<String> {
    let mut source = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = source.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    fn publication(path: PathBuf, artifact: &Path) -> ManagedTargetRecordPublication {
        ManagedTargetRecordPublication {
            path,
            record: ManagedTargetRecord {
                profile: "synthetic".to_owned(),
                artifact: path_text(artifact).unwrap(),
                artifact_sha256: sha256_file(artifact).unwrap(),
                socket: "/tmp/disposable.sock".to_owned(),
                pid: 4242,
                process_instance_id: "process-instance".to_owned(),
                record_identity: "publication-record".to_owned(),
            },
        }
    }

    #[test]
    fn publication_writes_complete_expected_record_without_overwrite() {
        let directory = tempfile::tempdir().unwrap();
        let artifact = directory.path().join("codex");
        std::fs::write(&artifact, b"synthetic artifact").unwrap();
        let record_path = directory.path().join("target.json");

        publication(record_path.clone(), &artifact)
            .publish()
            .unwrap();
        let record: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&record_path).unwrap()).unwrap();
        assert_eq!(record["profile"], "synthetic");
        assert_eq!(record["artifact"], path_text(&artifact).unwrap());
        assert_eq!(
            record["artifactSha256"],
            "169e73adeeb9ee2cbf78b5878c95ebef88d580774a6005a367c8e35458658027"
        );
        assert_eq!(record["socket"], "/tmp/disposable.sock");
        assert_eq!(record["pid"], 4242);
        assert_eq!(record["processInstanceId"], "process-instance");
        assert_eq!(record["recordIdentity"], "publication-record");
        assert_ne!(record["recordIdentity"], record["processInstanceId"]);

        let error = publication(record_path.clone(), &artifact)
            .publish()
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&std::fs::read(record_path).unwrap())
                .unwrap(),
            record
        );
    }
}
