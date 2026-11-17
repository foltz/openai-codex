use super::super::command_source::Envelope;
use super::super::command_store;
use super::*;
use base64::Engine;
use pretty_assertions::assert_eq;
use sha2::Digest;
use tempfile::TempDir;

fn envelope(generation: u64) -> Envelope {
    let expiration = chrono::Utc::now().timestamp() + 3600;
    let body = serde_json::json!({"exp":expiration,"jti":generation,"https://api.openai.com/auth":{"chatgpt_account_id":"account","chatgpt_user_id":"principal"}});
    let access_token = format!(
        "e30.{}.signature",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(body.to_string())
    );
    Envelope {
        version: 1,
        source: "source".into(),
        binding: "fixture#example".into(),
        account: "account".into(),
        principal: "principal".into(),
        generation,
        fingerprint: format!("{:x}", sha2::Sha256::digest(access_token.as_bytes())),
        access_token,
        expires_at: expiration,
    }
}

#[test]
fn command_envelope_rejects_wrong_identity_expiry_and_fingerprint() {
    let good = envelope(1);
    assert!(good.auth().is_ok());
    let mut bad = good.clone();
    bad.account = "other".into();
    assert!(bad.auth().is_err());
    bad = good.clone();
    bad.expires_at += 100;
    assert!(bad.auth().is_err());
    bad = good;
    bad.fingerprint = "incorrect".into();
    assert!(bad.auth().is_err());
}

#[cfg(unix)]
#[test]
fn command_store_is_atomic_read_only_and_rejects_native_overwrite() {
    use std::os::unix::fs::PermissionsExt;
    let home = TempDir::new().expect("home");
    let record = envelope(2);
    let auth = record.auth().expect("auth");
    command_store::commit(home.path(), &record, &auth).expect("commit");
    let before = std::fs::read(home.path().join("auth.json")).expect("state");
    let json: serde_json::Value = serde_json::from_slice(&before).expect("JSON");
    assert_eq!(json["tokens"]["refresh_token"], "");
    assert_eq!(
        std::fs::metadata(home.path().join("auth.json"))
            .expect("mode")
            .permissions()
            .mode()
            & 0o777,
        0o400
    );
    let old = envelope(1);
    assert!(command_store::commit(home.path(), &old, &old.auth().expect("old auth")).is_err());
    assert!(command_store::refuse_native_write(home.path()).is_err());
    assert_eq!(
        std::fs::read(home.path().join("auth.json")).expect("unchanged"),
        before
    );
    let newer = envelope(3);
    command_store::commit(home.path(), &newer, &newer.auth().expect("new auth"))
        .expect("replace readonly");
    assert_eq!(
        command_store::read(home.path())
            .expect("read")
            .expect("record")
            .generation,
        3
    );
}

#[cfg(unix)]
#[test]
fn loss_of_owned_auth_is_not_a_fresh_logged_out_home() {
    let home = TempDir::new().expect("home");
    let record = envelope(2);
    command_store::commit(home.path(), &record, &record.auth().expect("auth")).expect("commit");
    std::fs::remove_file(home.path().join("auth.json")).expect("simulate loss");
    assert!(command_store::read(home.path()).is_err());
    assert!(command_store::commit(home.path(), &record, &record.auth().expect("auth")).is_err());
}

#[cfg(unix)]
#[test]
fn command_owned_record_rejects_native_account_and_principal_divergence() {
    let home = TempDir::new().expect("home");
    let record = envelope(2);
    command_store::commit(home.path(), &record, &record.auth().expect("auth")).expect("commit");
    let original: serde_json::Value =
        serde_json::from_slice(&std::fs::read(home.path().join("auth.json")).expect("record"))
            .expect("JSON");
    let foreign_id = format!("e30.{}.signature", base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
        serde_json::json!({"https://api.openai.com/auth":{"chatgpt_account_id":"account","chatgpt_user_id":"other"}}).to_string(),
    ));
    for (field, value) in [
        ("account_id", "other".to_string()),
        ("id_token", foreign_id),
    ] {
        let mut corrupt = original.clone();
        corrupt["tokens"][field] = serde_json::Value::String(value);
        let staged = home.path().join("corrupt-fixture.json");
        std::fs::write(&staged, serde_json::to_vec(&corrupt).expect("fixture JSON"))
            .expect("stage corruption fixture");
        std::fs::rename(&staged, home.path().join("auth.json")).expect("replace fixture");
        assert!(
            command_store::read(home.path()).is_err(),
            "native account/principal must match the owned envelope"
        );
    }
}

#[test]
fn command_native_guard_preserves_unenrolled_repair_but_fences_owned_corruption() {
    let home = TempDir::new().expect("home");
    let path = home.path().join("auth.json");
    std::fs::write(&path, "not valid json").expect("ordinary corrupt native state");
    assert!(command_store::refuse_native_write(home.path()).is_ok());
    std::fs::write(&path, r#"{"external_auth_owner":null}"#).expect("corrupt owned record");
    assert!(command_store::refuse_native_write(home.path()).is_err());
    std::fs::write(
        home.path().join(".external-auth-owner.json"),
        "corrupt marker",
    )
    .expect("ownership evidence");
    std::fs::write(&path, "not valid json").expect("lost owned record");
    assert!(command_store::refuse_native_write(home.path()).is_err());
    std::fs::remove_file(&path).expect("lost auth file");
    assert!(command_store::refuse_native_write(home.path()).is_err());
}

#[cfg(unix)]
#[test]
fn directory_auth_state_does_not_bypass_external_owner_marker() {
    let home = TempDir::new().expect("home");
    let auth = home.path().join("auth.json");
    std::fs::create_dir(&auth).expect("invalid native file state");
    assert!(command_store::refuse_native_write(home.path()).is_ok());
    std::fs::write(
        home.path().join(".external-auth-owner.json"),
        "corrupt marker",
    )
    .expect("ownership evidence");
    let error = command_store::refuse_native_write(home.path()).expect_err("owned home is fenced");
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    assert!(
        auth.is_dir(),
        "the ownership probe must not mutate native state"
    );
}

#[cfg(unix)]
#[test]
fn command_store_rejects_equal_generation_changes_and_source_epoch_changes() {
    let home = TempDir::new().expect("home");
    let original = envelope(2);
    command_store::commit(home.path(), &original, &original.auth().expect("auth")).expect("commit");
    let before = std::fs::read(home.path().join("auth.json")).expect("owned bytes");
    let mut altered = envelope(3);
    altered.generation = original.generation;
    let mut foreign_source = original.clone();
    foreign_source.source = "other-source-epoch".into();
    let mut foreign_binding = original;
    foreign_binding.binding = "other-profile-binding".into();
    for candidate in [altered, foreign_source, foreign_binding] {
        assert!(
            command_store::commit(
                home.path(),
                &candidate,
                &candidate.auth().expect("valid token")
            )
            .is_err()
        );
        assert_eq!(
            std::fs::read(home.path().join("auth.json")).expect("unchanged"),
            before
        );
    }
}

#[cfg(unix)]
#[test]
fn real_native_factory_save_and_compare_save_refuse_owned_state() -> anyhow::Result<()> {
    use super::super::AuthKeyringBackendKind;
    use super::super::storage::create_auth_storage;
    use codex_config::types::AuthCredentialsStoreMode;
    let home = TempDir::new()?;
    let record = envelope(1);
    let external = record.auth()?;
    commit(home.path(), &record, &external)?;
    let before = std::fs::read(home.path().join("auth.json"))?;
    let native = create_auth_storage(
        home.path().to_path_buf(),
        AuthCredentialsStoreMode::File,
        AuthKeyringBackendKind::Direct,
    );
    let existing = native.load()?.expect("owned native-format record");
    let replacement =
        serde_json::from_value(serde_json::json!({"OPENAI_API_KEY":"synthetic-replacement"}))?;
    assert_eq!(
        native.save(&replacement).unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(
        native
            .compare_and_save(&existing, &replacement)
            .unwrap_err()
            .kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(std::fs::read(home.path().join("auth.json"))?, before);
    Ok(())
}

#[cfg(unix)]
#[test]
fn delayed_cache_guard_refuses_newer_durable_generation() -> anyhow::Result<()> {
    let home = TempDir::new()?;
    let first = envelope(1);
    commit(home.path(), &first, &first.auth()?)?;
    drop(locked_current(home.path(), &first)?);
    let next = envelope(2);
    commit(home.path(), &next, &next.auth()?)?;
    assert!(locked_current(home.path(), &first).is_err());
    locked_current(home.path(), &next)?.require_current_domain()?;
    Ok(())
}

#[cfg(unix)]
#[test]
fn mismatched_envelope_and_auth_cannot_create_durable_state() -> anyhow::Result<()> {
    let home = TempDir::new()?;
    let first = envelope(1);
    let other = envelope(2);
    assert!(commit(home.path(), &first, &other.auth()?).is_err());
    assert!(!home.path().join("auth.json").exists());
    assert!(!home.path().join(".external-auth-owner.json").exists());
    assert!(
        commit(
            home.path(),
            &first,
            &CodexAuth::from_api_key("synthetic-key")
        )
        .is_err()
    );
    assert!(!home.path().join("auth.json").exists());
    Ok(())
}

#[cfg(unix)]
#[test]
fn dangling_owner_marker_is_not_a_fresh_home_or_native_write_permission() -> anyhow::Result<()> {
    use std::os::unix::fs::symlink;
    let home = TempDir::new()?;
    symlink(
        home.path().join("missing-marker"),
        home.path().join(".external-auth-owner.json"),
    )?;
    assert!(read(home.path()).is_err());
    assert_eq!(
        refuse_native_write(home.path()).unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    let record = envelope(1);
    assert!(commit(home.path(), &record, &record.auth()?).is_err());
    assert!(
        std::fs::symlink_metadata(home.path().join(".external-auth-owner.json"))?
            .file_type()
            .is_symlink()
    );
    assert!(!home.path().join("missing-marker").exists());
    Ok(())
}

#[test]
fn command_envelope_refuses_invalid_version_generation_and_empty_owner() {
    let original = envelope(1);
    for field in [
        "version",
        "generation",
        "source",
        "binding",
        "account",
        "principal",
    ] {
        let mut value = serde_json::to_value(&original).unwrap();
        value[field] = match field {
            "version" | "generation" => serde_json::json!(0),
            _ => serde_json::json!(""),
        };
        let altered: Envelope = serde_json::from_value(value).unwrap();
        assert!(altered.auth().is_err(), "invalid envelope field: {field}");
    }
    let mut extra = serde_json::to_value(&original).unwrap();
    extra["refreshToken"] = serde_json::json!("synthetic-forbidden");
    assert!(serde_json::from_value::<Envelope>(extra).is_err());
}

#[cfg(unix)]
#[test]
fn missing_owner_marker_recovers_only_for_the_exact_locked_generation() -> anyhow::Result<()> {
    let home = TempDir::new()?;
    let record = envelope(2);
    commit(home.path(), &record, &record.auth()?)?;
    std::fs::remove_file(home.path().join(".external-auth-owner.json"))?;
    assert!(
        refuse_native_write(home.path()).is_err(),
        "owned payload alone fences native saves"
    );
    let wrong = envelope(1);
    assert!(recover_marker(home.path(), &wrong).is_err());
    assert!(!home.path().join(".external-auth-owner.json").exists());
    recover_marker(home.path(), &record)?;
    assert!(home.path().join(".external-auth-owner.json").is_file());
    assert!(read(home.path())? == Some(record));
    Ok(())
}

#[cfg(unix)]
#[test]
fn present_corrupt_or_foreign_marker_refuses_read_and_cache_install() -> anyhow::Result<()> {
    let home = TempDir::new()?;
    let record = envelope(1);
    commit(home.path(), &record, &record.auth()?)?;
    let auth_before = std::fs::read(home.path().join("auth.json"))?;
    for bytes in [
        b"invalid-marker".as_slice(),
        br#"{"version":1,"source":"foreign"}"#.as_slice(),
    ] {
        // Deliberate uncooperative fixture mutation of an existing read-only
        // marker, without invoking any production repair path.
        let staged = home.path().join("fixture-marker");
        std::fs::write(&staged, bytes)?;
        std::fs::rename(staged, home.path().join(".external-auth-owner.json"))?;
        assert!(read(home.path()).is_err());
        assert!(locked_current(home.path(), &record).is_err());
        assert!(recover_marker(home.path(), &record).is_err());
        assert_eq!(std::fs::read(home.path().join("auth.json"))?, auth_before);
        assert_eq!(
            std::fs::read(home.path().join(".external-auth-owner.json"))?,
            bytes
        );
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn reading_owned_payload_does_not_recreate_a_missing_marker() -> anyhow::Result<()> {
    let home = TempDir::new()?;
    let record = envelope(1);
    commit(home.path(), &record, &record.auth()?)?;
    let marker = home.path().join(".external-auth-owner.json");
    std::fs::remove_file(&marker)?;
    let before = std::fs::read(home.path().join("auth.json"))?;
    assert!(read(home.path())? == Some(record.clone()));
    assert!(!marker.exists(), "observation must not repair ownership");
    assert_eq!(std::fs::read(home.path().join("auth.json"))?, before);
    recover_marker(home.path(), &record)?;
    assert!(marker.is_file());
    Ok(())
}
