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

// Hold the real native OAuth request after its guarded reload. The command
// takeover then runs under the production lock before the response is released.
#[cfg(unix)]
async fn late_native_refresh_cannot_replace_owned_record(proactive: bool) {
    use super::super::AuthKeyringBackendKind;
    use super::super::AuthManager;
    use codex_config::types::AuthCredentialsStoreMode;
    use std::os::unix::fs::PermissionsExt;
    use std::time::Duration;

    let home = TempDir::new().expect("home");
    let record = envelope(2);
    let auth = record.auth().expect("auth");
    let mut native = auth.get_current_auth_json().expect("native JSON");
    native.auth_mode = Some(codex_protocol::auth::AuthMode::Chatgpt);
    native.tokens.as_mut().expect("tokens").refresh_token = "synthetic-native-only".into();
    super::super::save_auth(
        home.path(),
        &native,
        AuthCredentialsStoreMode::File,
        AuthKeyringBackendKind::default(),
    )
    .expect("seed managed client");
    let manager = AuthManager::shared(
        home.path().to_path_buf(),
        false,
        AuthCredentialsStoreMode::File,
        None,
        None,
        AuthKeyringBackendKind::default(),
        crate::test_support::transport_default_auth_route_config(),
    )
    .await;
    assert!(
        manager
            .auth_cached()
            .expect("cached managed auth")
            .is_chatgpt_auth()
    );

    let server = tiny_http::Server::http("127.0.0.1:0").expect("loopback issuer");
    let endpoint = format!("http://{}/oauth/token", server.server_addr());
    let prior = std::env::var_os(crate::REFRESH_TOKEN_URL_OVERRIDE_ENV_VAR);
    // SAFETY: both cases are serialized with the auth_env test group.
    unsafe {
        std::env::set_var(crate::REFRESH_TOKEN_URL_OVERRIDE_ENV_VAR, endpoint);
    }
    struct RestoreEnv(Option<std::ffi::OsString>);
    impl Drop for RestoreEnv {
        fn drop(&mut self) {
            // SAFETY: restoration occurs before the serial test guard releases.
            unsafe {
                if let Some(value) = &self.0 {
                    std::env::set_var(crate::REFRESH_TOKEN_URL_OVERRIDE_ENV_VAR, value);
                } else {
                    std::env::remove_var(crate::REFRESH_TOKEN_URL_OVERRIDE_ENV_VAR);
                }
            }
        }
    }
    let _environment = RestoreEnv(prior);
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let issuer = std::thread::spawn(move || {
        let mut request = server
            .recv_timeout(Duration::from_secs(10))
            .expect("issuer")
            .expect("refresh request");
        assert_eq!(request.url(), "/oauth/token");
        let mut body = String::new();
        request.as_reader().read_to_string(&mut body).expect("body");
        let body: serde_json::Value = serde_json::from_str(&body).expect("JSON");
        assert_eq!(body["grant_type"], "refresh_token");
        assert_eq!(body["refresh_token"], "synthetic-native-only");
        entered_tx.send(()).expect("entered barrier");
        release_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("takeover barrier");
        request.respond(tiny_http::Response::from_string(
            r#"{"access_token":"late-native-access","refresh_token":"late-native-refresh"}"#)
            .with_header(tiny_http::Header::from_bytes(b"Content-Type", b"application/json").expect("header")))
            .expect("response");
    });
    let refresh_manager = Arc::clone(&manager);
    let refresh = tokio::spawn(async move {
        if proactive {
            refresh_manager.refresh_token().await
        } else {
            let mut recovery = refresh_manager.unauthorized_recovery();
            recovery.next().await.expect("pre-takeover native reload");
            recovery.next().await.map(|_| ())
        }
    });
    tokio::time::timeout(Duration::from_secs(10), entered_rx)
        .await
        .expect("native request started")
        .expect("barrier");
    command_store::commit(home.path(), &record, &auth).expect("provider takeover");
    let before = std::fs::read(home.path().join("auth.json")).expect("owned bytes");
    release_tx.send(()).expect("release late OAuth result");
    assert!(
        tokio::time::timeout(Duration::from_secs(10), refresh)
            .await
            .expect("bounded refresh")
            .expect("task")
            .is_err(),
        "late native writer must report failure"
    );
    issuer.join().expect("issuer finished");
    assert_eq!(
        std::fs::read(home.path().join("auth.json")).expect("owned bytes"),
        before
    );
    assert_eq!(
        std::fs::metadata(home.path().join("auth.json"))
            .expect("mode")
            .permissions()
            .mode()
            & 0o777,
        0o400
    );
    assert!(command_store::read(home.path()).expect("owned record") == Some(record.clone()));
    let source = session_with_state(home.path(), record);
    assert!(
        source.obtain(None).await.is_ok(),
        "worker resolve remains usable"
    );
    // An unselected reader created after takeover has external access credentials;
    // native 401 recovery must not write a refresh credential back into that store.
    let reader = AuthManager::shared(
        home.path().to_path_buf(),
        false,
        AuthCredentialsStoreMode::File,
        None,
        None,
        AuthKeyringBackendKind::default(),
        crate::test_support::transport_default_auth_route_config(),
    )
    .await;
    reader
        .refresh_token_from_authority()
        .await
        .expect("external reader no-op");
    assert_eq!(
        std::fs::read(home.path().join("auth.json")).expect("unchanged reader state"),
        before
    );
}

#[cfg(unix)]
#[serial_test::serial(auth_env)]
#[tokio::test]
async fn command_takeover_fences_in_flight_native_proactive_refresh() {
    late_native_refresh_cannot_replace_owned_record(true).await;
}

#[cfg(unix)]
#[serial_test::serial(auth_env)]
#[tokio::test]
async fn command_takeover_fences_in_flight_native_unauthorized_refresh() {
    late_native_refresh_cannot_replace_owned_record(false).await;
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

#[cfg(target_os = "macos")]
#[serial_test::serial(auth_env)]
#[tokio::test]
async fn command_source_shared_managers_reject_delayed_old_cache_install() {
    use super::super::AuthConfig;
    use super::super::AuthKeyringBackendKind;
    use super::super::AuthManager;
    use codex_config::types::AuthCredentialsStoreMode;
    let home = TempDir::new().expect("home");
    let first_record = envelope(1);
    command_store::commit(
        home.path(),
        &first_record,
        &first_record.auth().expect("first auth"),
    )
    .expect("saved first generation");
    let values = [
        (
            "CODEX_CHATGPT_AUTH_COMMAND",
            home.path().join("command-must-not-run").into_os_string(),
        ),
        ("CODEX_CHATGPT_AUTH_BINDING", "fixture#example".into()),
        ("CODEX_CHATGPT_AUTH_SOURCE", "source".into()),
        ("CODEX_CHATGPT_AUTH_ACCOUNT", "account".into()),
        ("CODEX_CHATGPT_AUTH_PRINCIPAL", "principal".into()),
    ];
    struct Restore(Vec<(&'static str, Option<std::ffi::OsString>)>);
    impl Drop for Restore {
        fn drop(&mut self) {
            for (key, value) in &self.0 {
                // SAFETY: this isolated nextest case holds the auth_env serial guard.
                unsafe {
                    match value {
                        Some(value) => std::env::set_var(key, value),
                        None => std::env::remove_var(key),
                    }
                }
            }
        }
    }
    let prior = Restore(
        values
            .iter()
            .map(|(key, _)| (*key, std::env::var_os(key)))
            .collect(),
    );
    for (key, value) in values {
        // SAFETY: this isolated nextest case holds the auth_env serial guard.
        unsafe {
            std::env::set_var(key, value);
        }
    }
    let config = AuthConfig {
        codex_home: home.path().to_path_buf(),
        auth_credentials_store_mode: AuthCredentialsStoreMode::File,
        keyring_backend_kind: AuthKeyringBackendKind::default(),
        forced_login_method: None,
        chatgpt_base_url: None,
        forced_chatgpt_workspace_id: None,
        managed_auth_policy: Default::default(),
        auth_route_config: crate::test_support::transport_default_auth_route_config(),
    };
    let first = AuthManager::shared_from_auth_config(config.clone(), false)
        .await
        .expect("first manager");
    let second = AuthManager::shared_from_auth_config(config.clone(), false)
        .await
        .expect("second manager");
    let source = CommandSource::selected(&config)
        .expect("source selection")
        .expect("selected");
    assert_eq!(source.managers.lock().expect("manager registry").len(), 2);
    let stale_resolve = source.obtain(None).await.expect("completed old resolve");
    let (release, ready) = tokio::sync::oneshot::channel();
    let delayed_manager = Arc::clone(&first);
    let delayed_install = tokio::spawn(async move {
        ready.await.expect("newer publication barrier");
        delayed_manager.set_cached_auth(Some(stale_resolve))
    });
    let newer = envelope(2);
    command_store::commit(home.path(), &newer, &newer.auth().expect("new auth"))
        .expect("another publisher committed newer generation");
    source
        .obtain(None)
        .await
        .expect("import and publish newer disk generation");
    release.send(()).expect("release old cache install");
    assert!(!delayed_install.await.expect("delayed setter"));
    for manager in [&first, &second] {
        let cached = manager.auth_cached().expect("cache remains installed");
        assert_eq!(
            cached.get_token_data().expect("tokens").access_token,
            newer.access_token
        );
        assert_eq!(source.receipt(Some(&cached))["generation"], 2);
        assert_eq!(source.receipt(Some(&cached))["coherent"], true);
    }
    assert!(command_store::read(home.path()).expect("saved record") == Some(newer));
    drop(prior);
}
