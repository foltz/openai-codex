use super::*;
use pretty_assertions::assert_eq;
use std::time::Duration;

fn key(name: &str) -> EndpointKey {
    EndpointKey::for_streamable_http(
        &format!("http://127.0.0.1:1/{name}/mcp"),
        &format!("Bearer {name}"),
    )
}

fn capability(name: &str) -> CapabilityDigest {
    CapabilityDigest::of(name)
}

fn soon() -> Instant {
    Instant::now() + Duration::from_secs(1)
}

/// Intake tracking by `connection`, then a declaration (after the caller's
/// challenge succeeded).
fn use_and_declare(custody: &HostedEndpointCustody, name: &str, connection: u64) {
    custody
        .track_override(key(name), connection)
        .expect("track");
    custody
        .declare_for_test(key(name), capability(&format!("cap-{name}")), connection)
        .expect("declare");
}

fn admit(custody: &HostedEndpointCustody, name: &str, thread: &str, owner: &RmcpClientRetirement) {
    custody
        .admit(key(name), Some(thread.to_owned()), owner, true)
        .expect("admit");
}

#[tokio::test]
async fn retire_reports_exact_owners_and_leaves_other_endpoints_untouched() {
    let custody = HostedEndpointCustody::default();
    use_and_declare(&custody, "a", 1);
    use_and_declare(&custody, "b", 2);
    let parent = RmcpClientRetirement::default();
    let subagent = RmcpClientRetirement::default();
    let other = RmcpClientRetirement::default();
    admit(&custody, "a", "parent", &parent);
    admit(&custody, "a", "subagent", &subagent);
    admit(&custody, "b", "other", &other);

    let census = custody
        .retire(key("a"), capability("cap-a"), 1, soon())
        .await
        .expect("owner retires");

    assert_eq!(
        census
            .receipts
            .iter()
            .map(|receipt| (receipt.thread_id.as_deref(), receipt.report.is_complete()))
            .collect::<Vec<_>>(),
        vec![(Some("parent"), true), (Some("subagent"), true)]
    );
    assert!(census.is_complete());
    assert_eq!(
        census.custody,
        HostedEndpointCustodyCensus {
            declared: 1,
            provisional: 0,
            tombstones: 1,
            capacity: HOSTED_ENDPOINT_CAPACITY,
        }
    );
    assert!(!custody.is_retired(&key("b")));
    assert_eq!(
        custody.admit(key("a"), None, &RmcpClientRetirement::default(), true),
        Err(HostedEndpointRefusal::Retired)
    );
    assert_eq!(
        custody.track_override(key("a"), 3),
        Err(HostedEndpointRefusal::Retired)
    );
    assert_eq!(
        custody.admit(key("b"), None, &RmcpClientRetirement::default(), true),
        Ok(())
    );
}

#[tokio::test]
async fn only_a_connection_that_used_the_endpoint_may_declare_or_retire() {
    let custody = HostedEndpointCustody::default();
    // Never used by any connection: no blind declaration.
    assert_eq!(
        custody.declare_for_test(key("c"), capability("cap-c"), 1),
        Err(HostedEndpointRefusal::NotOwner)
    );
    use_and_declare(&custody, "c", 1);
    // Another connection that never named the endpoint cannot declare it.
    assert_eq!(
        custody.declare_for_test(key("c"), capability("cap-c"), 2),
        Err(HostedEndpointRefusal::NotOwner)
    );
    // One that did, but with a different capability, cannot either.
    custody.track_override(key("c"), 2).expect("track");
    assert_eq!(
        custody.declare_for_test(key("c"), capability("forged"), 2),
        Err(HostedEndpointRefusal::NotOwner)
    );
    assert_eq!(
        custody
            .retire(key("c"), capability("forged"), 1, soon())
            .await,
        Err(HostedEndpointRefusal::NotOwner)
    );
    assert_eq!(
        custody
            .retire(key("c"), capability("cap-c"), 2, soon())
            .await,
        Err(HostedEndpointRefusal::NotOwner)
    );
    custody.release_connection(1);
    assert_eq!(
        custody
            .retire(key("c"), capability("cap-c"), 1, soon())
            .await,
        Err(HostedEndpointRefusal::NotOwner)
    );
    assert!(!custody.is_retired(&key("c")));
}

#[tokio::test]
async fn vanished_owners_without_proof_are_unknown_and_incomplete() {
    let custody = HostedEndpointCustody::default();
    use_and_declare(&custody, "d", 1);
    let proven = RmcpClientRetirement::default();
    let unproven = RmcpClientRetirement::default();
    admit(&custody, "d", "proven", &proven);
    admit(&custody, "d", "unproven", &unproven);
    assert!(proven.shutdown_until(soon()).await.is_complete());
    drop(proven);
    drop(unproven);

    let census = custody
        .retire(key("d"), capability("cap-d"), 1, soon())
        .await
        .expect("owner retires");

    assert!(census.receipts.is_empty());
    assert_eq!(census.proven_before_retirement, 1);
    assert_eq!(census.unknown, vec![Some("unproven".to_owned())]);
    assert!(!census.is_complete());
    let again = custody
        .retire(key("d"), capability("cap-d"), 1, soon())
        .await
        .expect("owner re-observes");
    assert_eq!(again.unknown, vec![Some("unproven".to_owned())]);
    assert!(!again.is_complete());
}

#[tokio::test]
async fn owners_admitted_before_tracking_make_the_census_incomplete() {
    let custody = HostedEndpointCustody::default();
    let early = RmcpClientRetirement::default();
    custody
        .admit(key("e"), Some("early".to_owned()), &early, true)
        .expect("untracked admission");
    use_and_declare(&custody, "e", 1);
    let census = custody
        .retire(key("e"), capability("cap-e"), 1, soon())
        .await
        .expect("owner retires");
    assert!(census.possibly_untracked);
    assert!(!census.is_complete());
}

#[tokio::test]
async fn expired_latched_owner_deadline_is_retained_by_retirement() {
    let custody = HostedEndpointCustody::default();
    use_and_declare(&custody, "l", 1);
    let owner = RmcpClientRetirement::default();
    admit(&custody, "l", "t", &owner);
    let expired = Instant::now();
    owner.shutdown_until(expired).await;
    let census = custody
        .retire(key("l"), capability("cap-l"), 1, soon())
        .await
        .expect("owner retires");
    // An empty registry is complete at any deadline; the latch is retained.
    assert_eq!(owner.latched_deadline(), Some(expired));
    assert_eq!(census.receipts.len(), 1);
}

#[tokio::test]
async fn capacities_are_fixed_and_tombstones_are_never_evicted() {
    let custody = HostedEndpointCustody::with_capacity(1);
    use_and_declare(&custody, "f", 1);
    custody.track_override(key("g"), 2).expect("provisional");
    assert_eq!(
        custody.declare_for_test(key("g"), capability("cap-g"), 2),
        Err(HostedEndpointRefusal::CapacityExhausted)
    );
    custody
        .retire(key("f"), capability("cap-f"), 1, soon())
        .await
        .expect("owner retires");
    custody.release_connection(1);
    assert_eq!(
        custody.declare_for_test(key("g"), capability("cap-g"), 2),
        Err(HostedEndpointRefusal::CapacityExhausted)
    );
    assert!(custody.is_retired(&key("f")));

    let provisional = HostedEndpointCustody::default();
    for index in 0..HOSTED_ENDPOINT_PROVISIONAL_PER_CONNECTION {
        provisional
            .track_override(key(&format!("p{index}")), 9)
            .expect("within the per-connection bound");
    }
    assert_eq!(
        provisional.track_override(key("one-too-many"), 9),
        Err(HostedEndpointRefusal::CapacityExhausted)
    );
    assert_eq!(
        provisional.track_override(key("other-connection"), 10),
        Ok(())
    );
}

#[tokio::test]
async fn released_entries_are_reclaimed_only_without_live_or_unproven_owners() {
    let custody = HostedEndpointCustody::default();
    use_and_declare(&custody, "h", 1);
    let owner = RmcpClientRetirement::default();
    admit(&custody, "h", "t", &owner);
    custody.release_connection(1);
    assert_eq!(custody.census().declared, 1, "a live owner keeps the entry");
    assert!(owner.shutdown_until(soon()).await.is_complete());
    drop(owner);
    custody.release_connection(1);
    assert_eq!(custody.census().declared, 0);

    custody.track_override(key("q"), 5).expect("provisional");
    custody.release_connection(5);
    assert_eq!(custody.census().provisional, 0, "nothing left to retire");
}

#[tokio::test]
async fn repeated_admission_of_one_physical_lower_has_one_receipt() {
    let custody = HostedEndpointCustody::default();
    use_and_declare(&custody, "dedup", 1);
    let lower = RmcpClientRetirement::default();
    admit(&custody, "dedup", "creator", &lower);
    admit(&custody, "dedup", "creator", &lower);
    let result = custody
        .retire(key("dedup"), capability("cap-dedup"), 1, soon())
        .await
        .expect("retire");
    assert!(result.is_complete());
    assert_eq!(result.receipts.len(), 1);
}

#[tokio::test]
async fn physical_constructor_refuses_tombstone_before_auth_or_environment_work() {
    use crate::rmcp_client::StartupOutcomeError;
    use crate::server::EffectiveMcpServer;
    use codex_config::McpServerConfig;
    use codex_config::types::AuthKeyringBackendKind;
    use codex_config::types::McpServerAuth;
    use codex_config::types::OAuthCredentialsStoreMode;
    use codex_rmcp_client::McpOAuthRefreshMode;

    let custody = HostedEndpointCustody::default();
    use_and_declare(&custody, "constructor", 1);
    assert!(
        custody
            .retire(key("constructor"), capability("cap-constructor"), 1, soon())
            .await
            .expect("tombstone")
            .is_complete()
    );
    for enterprise_auth in [false, true] {
        let mut config: McpServerConfig = serde_json::from_value(serde_json::json!({
            "url": "http://127.0.0.1:1/constructor/mcp",
            "http_headers": {"Authorization": "Bearer constructor"}
        }))
        .expect("server config");
        if enterprise_auth {
            config.auth = McpServerAuth::EmaAuth;
        }
        let lower = RmcpClientRetirement::default();
        let result = crate::rmcp_client::make_rmcp_client_in_retirement(
            None,
            "hosted-constructor",
            EffectiveMcpServer::from_host_config(config),
            OAuthCredentialsStoreMode::default(),
            AuthKeyringBackendKind::default(),
            McpOAuthRefreshMode::Legacy,
            crate::connection_pool::tests::context()
                .with_hosted_endpoint_custody(custody.clone())
                .with_canonical_thread_id("creator".to_owned()),
            Err("environment work must not be reached".to_owned()),
            None,
            crate::McpProtocolMode::Legacy,
            lower.clone(),
        )
        .await;
        assert!(matches!(result, Err(StartupOutcomeError::Cancelled)));
    }
}

#[tokio::test]
async fn same_url_other_authorization_is_incomplete_in_both_admission_orders() {
    let url = "http://127.0.0.1:4000/shared/mcp";
    let tracked = EndpointKey::for_streamable_http(url, "Bearer tracked");
    let other = EndpointKey::for_streamable_http(url, "Bearer other");
    for other_first in [false, true] {
        let custody = HostedEndpointCustody::default();
        let lower = RmcpClientRetirement::default();
        if other_first {
            custody
                .admit(other, Some("other".to_owned()), &lower, true)
                .expect("other admission");
        }
        custody.track_override(tracked, 1).expect("track");
        custody
            .declare_for_test(tracked, capability("tracked"), 1)
            .expect("declare");
        if !other_first {
            custody
                .admit(other, Some("other".to_owned()), &lower, true)
                .expect("other admission");
        }
        let result = custody
            .retire(tracked, capability("tracked"), 1, soon())
            .await
            .expect("retire");
        assert!(result.possibly_untracked);
        assert!(!result.is_complete());
        assert!(result.receipts.is_empty());
        assert!(!lower.retirement_proof().is_proven());
        // No authority over the foreign key is minted by the conservative flag.
        assert!(!custody.is_retired(&other));
    }
}

#[test]
fn reset_invalidates_pending_challenge_even_after_same_connection_reattributes() {
    let custody = HostedEndpointCustody::default();
    custody.track_override(key("reset"), 1).expect("track");
    let old = custody
        .begin_declare(key("reset"), 1)
        .expect("old challenge");
    custody.reset_for_account_transition();
    custody
        .track_override(key("reset"), 1)
        .expect("fresh attribution");
    assert_eq!(
        custody.declare(old, capability("old")),
        Err(HostedEndpointRefusal::NotOwner)
    );
    let fresh = custody
        .begin_declare(key("reset"), 1)
        .expect("fresh challenge");
    assert!(custody.declare(fresh, capability("new")).is_ok());
}

#[test]
fn challenge_ticket_cannot_cross_runtime_owners_and_generation_exhaustion_fails_closed() {
    let a = HostedEndpointCustody::default();
    let b = HostedEndpointCustody::default();
    for custody in [&a, &b] {
        custody.track_override(key("ticket"), 1).expect("track");
    }
    let ticket = a.begin_declare(key("ticket"), 1).expect("ticket");
    assert_eq!(
        b.declare(ticket, capability("a")),
        Err(HostedEndpointRefusal::NotOwner)
    );
    a.lock().generation = Some(u64::MAX);
    let last = a.begin_declare(key("ticket"), 1).expect("last generation");
    a.reset_for_account_transition();
    a.track_override(key("ticket"), 1)
        .expect("track after exhaustion");
    assert_eq!(
        a.declare(last, capability("a")),
        Err(HostedEndpointRefusal::NotOwner)
    );
    assert!(matches!(
        a.begin_declare(key("ticket"), 1),
        Err(HostedEndpointRefusal::NotOwner)
    ));
    a.reset_for_account_transition();
    assert_eq!(a.lock().generation, None);
}

#[tokio::test]
async fn account_reset_revokes_authority_but_keeps_unproven_physical_evidence() {
    let custody = HostedEndpointCustody::default();
    use_and_declare(&custody, "reset-owner", 1);
    let lower = RmcpClientRetirement::default();
    admit(&custody, "reset-owner", "old-owner", &lower);
    drop(lower); // No positive retirement proof.
    custody.reset_for_account_transition();
    custody.release_connection(1);
    assert!(!custody.is_used_by(&key("reset-owner"), 1));
    assert_eq!(custody.census().provisional, 1);
    assert_eq!(
        custody
            .retire(key("reset-owner"), capability("cap-reset-owner"), 1, soon())
            .await,
        Err(HostedEndpointRefusal::NotOwner)
    );
    custody
        .track_override(key("reset-owner"), 2)
        .expect("new attribution");
    custody
        .declare_for_test(key("reset-owner"), capability("new-owner"), 2)
        .expect("new proof");
    let result = custody
        .retire(key("reset-owner"), capability("new-owner"), 2, soon())
        .await
        .expect("retire");
    assert_eq!(result.unknown, vec![Some("old-owner".to_owned())]);
    assert!(!result.is_complete());
}

#[tokio::test]
async fn account_resets_do_not_refund_permanent_tombstone_capacity() {
    let custody = HostedEndpointCustody::with_capacity(1);
    use_and_declare(&custody, "permanent", 1);
    assert!(
        custody
            .retire(key("permanent"), capability("cap-permanent"), 1, soon())
            .await
            .expect("retire")
            .is_complete()
    );
    for _ in 0..3 {
        custody.reset_for_account_transition();
        assert_eq!(custody.census().tombstones, 1);
        assert!(custody.is_retired(&key("permanent")));
    }
    custody
        .track_override(key("replacement"), 2)
        .expect("new provisional");
    assert_eq!(
        custody.declare_for_test(key("replacement"), capability("new"), 2),
        Err(HostedEndpointRefusal::CapacityExhausted)
    );
}

#[test]
fn reset_provisional_overflow_keeps_old_owners_and_refuses_new_tracking() {
    let custody = HostedEndpointCustody::default();
    let mut owners = Vec::new();
    for index in 0..=HOSTED_ENDPOINT_PROVISIONAL_CAPACITY {
        let name = format!("reset-{index}");
        use_and_declare(&custody, &name, 1);
        let lower = RmcpClientRetirement::default();
        admit(&custody, &name, &name, &lower);
        owners.push(lower);
    }
    custody.reset_for_account_transition();
    assert_eq!(
        custody.census().provisional,
        HOSTED_ENDPOINT_PROVISIONAL_CAPACITY + 1
    );
    assert_eq!(
        custody.track_override(key("new-after-reset"), 2),
        Err(HostedEndpointRefusal::CapacityExhausted)
    );
    assert_eq!(owners.len(), HOSTED_ENDPOINT_PROVISIONAL_CAPACITY + 1);
}
