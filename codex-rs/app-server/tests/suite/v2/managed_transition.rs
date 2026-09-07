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

/// Proves Slice 1's temporary public start/read/cancel gates stay
/// unconditionally `AuthorizationNotAdmitted` across a restart, regardless of
/// the real persisted-auth source's health, per `CODEX-I05-S01-R07-001`. An
/// unauthorized wire caller must never observe whether the server's own
/// auth backend is readable -- that would leak health signal before Slice 2's
/// own authorization decision exists. The production auth mapper's own
/// per-source behavior (confirmed absent/present/unreadable/cache-lock) is
/// proven separately, through an internal test-only seam that never crosses
/// this wire boundary, by
/// `managed_transition::tests::restart_reconstructs_every_material_phase_through_the_real_persisted_auth_mapper`
/// and, for the lock-poisoning case with no restart analogue, by
/// `codex_login::auth::auth_tests::authoritative_cached_auth_reports_cache_lock_unavailable_when_poisoned`.
#[tokio::test]
async fn wire_gates_stay_unconditionally_not_admitted_across_every_persisted_auth_source_boundary()
-> Result<()> {
    let codex_home = TempDir::new()?;
    let responses = create_mock_responses_server_repeating_assistant("Done").await;
    MockResponsesConfig::new(&responses.uri()).write(codex_home.path())?;

    // Boundary 1: confirmed absent (no `auth.json` at all).
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
        "confirmed-absent auth must not change the temporary gate's refusal kind"
    );
    let initial_process_id = refusal.process_instance_id;
    initial.shutdown_gracefully().await?;

    // Boundary 2: initial-load failure (an unreadable/unparseable persisted
    // source). The gate must stay unconditional here too -- this is exactly
    // the case `CODEX-I05-S01-R07-001` found leaking a distinct refusal kind.
    std::fs::write(codex_home.path().join("auth.json"), "not valid json")?;
    let mut unreadable = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let unreadable_start = start_refusal(&mut unreadable, "second-process".to_owned()).await?;
    let StartManagedTransitionResponse::Refused { refusal } = unreadable_start else {
        panic!("unreadable persisted auth must not admit a caller");
    };
    assert_eq!(
        refusal.kind,
        ManagedTransitionRefusalKind::AuthorizationNotAdmitted,
        "an unreadable persisted-auth source must not be observable through the temporary wire gate"
    );
    assert_ne!(
        refusal.process_instance_id, initial_process_id,
        "each restart must reconstruct a new process identity"
    );
    let unreadable_process_id = refusal.process_instance_id;

    // Same boundary, read and cancel gates: the review's required correction
    // asks for all three real gates in the unavailable-source population, not
    // only start.
    let unreadable_read = unreadable
        .request(|request_id| ClientRequest::ManagedTransitionRead {
            request_id,
            params: codex_app_server_protocol::ReadManagedTransitionParams {
                contract_version: MANAGED_AUTH_TRANSITION_CONTRACT_VERSION,
                transition_id: "restart-transition".to_owned(),
                process_instance_id: unreadable_process_id.clone(),
            },
        })
        .await?;
    let codex_app_server_protocol::ReadManagedTransitionResponse::Refused { refusal } =
        unreadable_read
    else {
        panic!("unreadable persisted auth must not admit a read caller");
    };
    assert_eq!(
        refusal.kind,
        ManagedTransitionRefusalKind::AuthorizationNotAdmitted,
        "the read gate must not leak persisted-auth health either"
    );

    let unreadable_cancel = unreadable
        .request(|request_id| ClientRequest::ManagedTransitionCancel {
            request_id,
            params: codex_app_server_protocol::CancelManagedTransitionParams {
                contract_version: MANAGED_AUTH_TRANSITION_CONTRACT_VERSION,
                transition_id: "restart-transition".to_owned(),
                process_instance_id: unreadable_process_id.clone(),
            },
        })
        .await?;
    let codex_app_server_protocol::CancelManagedTransitionResponse::Refused { refusal } =
        unreadable_cancel
    else {
        panic!("unreadable persisted auth must not admit a cancel caller");
    };
    assert_eq!(
        refusal.kind,
        ManagedTransitionRefusalKind::AuthorizationNotAdmitted,
        "the cancel gate must not leak persisted-auth health either"
    );
    unreadable.shutdown_gracefully().await?;

    // Boundary 3: confirmed present (a genuine, readable persisted account),
    // following directly after the unreadable-source restart above with no
    // other change but a valid write -- the gate must still be exactly the
    // same unconditional refusal, proving no coordinator mutation or
    // reservation happened at any boundary above.
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
        "confirmed-present auth must not change the temporary gate's refusal kind"
    );
    assert_ne!(
        refusal.process_instance_id, unreadable_process_id,
        "each restart must reconstruct a new process identity"
    );
    let present_process_id = refusal.process_instance_id;

    // No manufactured old outcome or reservation: a prior process's own
    // transition id resolves through the same unconditional refusal after
    // restart, never through a resurrected admission from an earlier process.
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
