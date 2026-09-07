use anyhow::Result;
use app_test_support::ChatGptAuthFixture;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use app_test_support::create_mock_responses_server_repeating_assistant;
use app_test_support::write_chatgpt_auth;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::MANAGED_AUTH_TRANSITION_CONTRACT_VERSION;
use codex_app_server_protocol::ManagedTransitionIntent;
use codex_app_server_protocol::ManagedTransitionRefusalKind;
use codex_app_server_protocol::StartManagedTransitionParams;
use codex_app_server_protocol::StartManagedTransitionResponse;
use codex_config::types::AuthCredentialsStoreMode;
use tempfile::TempDir;

fn start_params(process_instance_id: String) -> StartManagedTransitionParams {
    StartManagedTransitionParams {
        contract_version: MANAGED_AUTH_TRANSITION_CONTRACT_VERSION,
        transition_id: "restart-transition".to_owned(),
        process_instance_id,
        intent: ManagedTransitionIntent::AdoptManagedAuth,
        expected_auth_revision: 0,
        expected_transition_revision: 0,
        expected_auth_fingerprint: None,
    }
}

async fn start_refusal(
    server: &mut TestAppServer,
    process_instance_id: String,
) -> Result<StartManagedTransitionResponse> {
    server
        .request(|request_id| ClientRequest::ManagedTransitionStart {
            request_id,
            params: start_params(process_instance_id),
        })
        .await
}

/// Exercises every persisted-auth-source boundary from `CODEX-I05-S01-R04`
/// through the real production mapper (`AuthManager::new_from_auth_config`
/// -> `AuthoritativeAuthState::from_auth_manager`), by restarting a real
/// app-server/`MessageProcessor` against the same `codex_home` across each
/// state change. `CacheLockUnavailable` has no restart analogue -- a freshly
/// started process always begins with an unpoisoned lock -- so it is proven
/// separately, at the `AuthManager` unit level (same crate, so it can poison
/// the private lock directly), by
/// `codex_login::auth::auth_tests::authoritative_cached_auth_reports_cache_lock_unavailable_when_poisoned`.
#[tokio::test]
async fn restart_reconstructs_every_persisted_auth_source_boundary_through_the_production_mapper()
-> Result<()> {
    let codex_home = TempDir::new()?;
    let responses = create_mock_responses_server_repeating_assistant("Done").await;
    MockResponsesConfig::new(&responses.uri()).write(codex_home.path())?;

    // Boundary 1: confirmed absent (no `auth.json` at all) is a genuine
    // logged-out current state, not an authority failure.
    let mut initial = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let initial_response = start_refusal(&mut initial, "first-process".to_owned()).await?;
    let StartManagedTransitionResponse::Refused { refusal } = initial_response else {
        panic!("Slice 1 must not admit a wire caller");
    };
    assert_eq!(
        refusal.kind,
        ManagedTransitionRefusalKind::AuthorizationNotAdmitted,
        "a confirmed missing auth record is a logged-out current state, not an authority failure"
    );
    let initial_process_id = refusal.process_instance_id;
    initial.shutdown_gracefully().await?;

    // Boundary 2: initial-load failure (an unreadable/unparseable persisted
    // source) must fail closed as `AuthoritativeAuthUnavailable`, never as a
    // manufactured logout.
    std::fs::write(codex_home.path().join("auth.json"), "not valid json")?;
    let mut unreadable = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let unreadable_response = start_refusal(&mut unreadable, "second-process".to_owned()).await?;
    let StartManagedTransitionResponse::Refused { refusal } = unreadable_response else {
        panic!("unreadable persisted auth must not admit a caller");
    };
    assert_eq!(
        refusal.kind,
        ManagedTransitionRefusalKind::AuthoritativeAuthUnavailable,
        "the restarted app-server must not turn a failed persisted-state read into logout"
    );
    assert_ne!(
        refusal.process_instance_id, initial_process_id,
        "each restart must reconstruct a new process identity"
    );
    let unreadable_process_id = refusal.process_instance_id;
    unreadable.shutdown_gracefully().await?;

    // Boundary 3: confirmed present (a genuine, readable persisted account)
    // must resolve the authority as available, distinct from an unavailable
    // read failure -- and, following directly after the unreadable-source
    // restart above with no other change but a valid write, this also proves
    // the source is safely retried rather than latched into a poisoned state.
    write_chatgpt_auth(
        codex_home.path(),
        ChatGptAuthFixture::new("chatgpt-token").account_id("restart-account"),
        AuthCredentialsStoreMode::File,
    )?;
    let mut present = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let present_response = start_refusal(&mut present, "third-process".to_owned()).await?;
    let StartManagedTransitionResponse::Refused { refusal } = present_response else {
        panic!("Slice 1 must not admit a wire caller even once auth is present");
    };
    assert_eq!(
        refusal.kind,
        ManagedTransitionRefusalKind::AuthorizationNotAdmitted,
        "a confirmed present account is a logged-in current state, not an authority failure"
    );
    assert_ne!(
        refusal.process_instance_id, unreadable_process_id,
        "each restart must reconstruct a new process identity"
    );
    let present_process_id = refusal.process_instance_id;

    // No manufactured old outcome or reservation: Slice 1's wire boundary
    // never admits or reserves any transition regardless of which prior
    // process's identity a caller presents -- a restart cannot resurrect an
    // admission that was never granted in the first place. (Slice 2 attaches
    // process-instance validation to this same call path; until then, every
    // wire read is gated solely on authoritative-auth availability, proven
    // above, never on `process_instance_id`.)
    let read_as_prior_process = present
        .request(|request_id| ClientRequest::ManagedTransitionRead {
            request_id,
            params: codex_app_server_protocol::ReadManagedTransitionParams {
                contract_version: MANAGED_AUTH_TRANSITION_CONTRACT_VERSION,
                transition_id: "restart-transition".to_owned(),
                process_instance_id: unreadable_process_id,
            },
        })
        .await?;
    let codex_app_server_protocol::ReadManagedTransitionResponse::Refused { refusal } =
        read_as_prior_process
    else {
        panic!("a prior process identity must never resolve a live transition after restart");
    };
    assert_eq!(
        refusal.kind,
        ManagedTransitionRefusalKind::AuthorizationNotAdmitted,
        "restart must not manufacture or resurrect a reservation from a previous process"
    );
    assert_eq!(
        refusal.process_instance_id, present_process_id,
        "the refusal must report the current process identity, not the stale caller's"
    );

    Ok(())
}
