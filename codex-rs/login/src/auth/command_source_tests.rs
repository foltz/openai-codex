use super::*;
use base64::Engine;
use pretty_assertions::assert_eq;
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

fn session_with_state(home: &std::path::Path, record: Envelope) -> CommandSource {
    CommandSource {
        selection: Selection {
            home: home.to_path_buf(),
            command: home.join("unused-command"),
            binding: record.binding.clone(),
            source: record.source.clone(),
            account: record.account.clone(),
            principal: record.principal.clone(),
        },
        gate: Semaphore::new(1),
        state: Mutex::new(Some(record)),
        rejected: Mutex::new(HashSet::new()),
        retry_after: AtomicI64::new(chrono::Utc::now().timestamp() + 60),
        managers: Mutex::new(Vec::new()),
    }
}

#[cfg(unix)]
#[tokio::test]
async fn command_source_does_not_reuse_rejected_newer_disk_credential() {
    let home = TempDir::new().expect("home");
    let old = envelope(1);
    let newer = envelope(2);
    command_store::commit(home.path(), &newer, &newer.auth().expect("new auth"))
        .expect("another process published newer credential");
    let source = session_with_state(home.path(), old);
    let result = source.obtain(Some(newer.fingerprint)).await;
    assert!(
        result.is_err(),
        "a credential actually rejected by the request must not become a cache success"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn command_source_coalesces_old_rejection_onto_unrejected_newer_disk_credential() {
    let home = TempDir::new().expect("home");
    let old = envelope(1);
    let newer = envelope(2);
    command_store::commit(home.path(), &newer, &newer.auth().expect("new auth"))
        .expect("another process published newer credential");
    let source = session_with_state(home.path(), old.clone());
    let result = source.obtain(Some(old.fingerprint)).await;
    assert!(
        result.is_ok(),
        "an old request's rejection must not poison the new credential"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn command_source_rejection_capacity_cannot_leave_rejected_current_auth_usable() {
    let home = TempDir::new().expect("home");
    let current = envelope(2);
    let auth = current.auth().expect("current auth");
    command_store::commit(home.path(), &current, &auth).expect("commit");
    let source = session_with_state(home.path(), current.clone());
    source
        .rejected
        .lock()
        .expect("rejection state")
        .extend((0..16).map(|index| format!("stale-request-fingerprint-{index}")));
    assert!(source.obtain(Some(current.fingerprint)).await.is_err());
    assert!(
        !source.usable_auth(Some(&auth)),
        "capacity must not leave the actually rejected credential ready"
    );
    assert_eq!(source.receipt(Some(&auth))["coherent"], false);
    assert!(source.rejected.lock().expect("bounded rejections").len() <= 16);
}

#[test]
fn command_selection_requires_the_external_token_mode_before_source_io() {
    assert!(CommandSource::require_allowed_mode(None).is_ok());
    assert!(CommandSource::require_allowed_mode(Some(&[AuthMode::ChatgptAuthTokens])).is_ok());
    for modes in [&[][..], &[AuthMode::Chatgpt][..], &[AuthMode::ApiKey][..]] {
        assert_eq!(
            CommandSource::require_allowed_mode(Some(modes))
                .unwrap_err()
                .kind(),
            io::ErrorKind::PermissionDenied
        );
    }
}

#[cfg(unix)]
#[test]
fn command_source_accepts_only_its_selected_complete_principal() {
    let home = TempDir::new().unwrap();
    let record = envelope(1);
    let source = session_with_state(home.path(), record.clone());
    let auth = record.auth().unwrap();
    assert!(source.accepts(Some(&auth), &Some(record.clone())));
    assert!(!source.accepts(None, &Some(record.clone())));
    assert!(!source.accepts(
        Some(&CodexAuth::from_api_key("synthetic")),
        &Some(record.clone())
    ));
    let mut foreign = record.clone();
    foreign.source = "foreign-source".into();
    assert!(!source.accepts(Some(&auth), &Some(foreign)));
    let mut foreign = record.clone();
    foreign.principal = "foreign-principal".into();
    assert!(!source.accepts(Some(&auth), &Some(foreign)));
    assert!(!source.accepts(Some(&auth), &None));
}
