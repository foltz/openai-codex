//! Exact, opt-in physical-connection disclosure gates for KCF010/KCF013.
//! Caller wiring must supply owning-thread identity and host endpoint, and must
//! run the initial gate before provider output or Ready publication. A cached
//! declaration is never an input authority for a new physical connection.

use std::collections::HashSet;
use std::time::Duration;

use anyhow::Result;
use anyhow::bail;
use codex_rmcp_client::PostReconnectHook;
use codex_rmcp_client::RmcpClient;
use futures::FutureExt;
use rmcp::model::CustomResult;
use rmcp::model::ServerPeerInfo;
use rmcp::model::ServerResult;
use serde_json::Value;
use serde_json::json;

const THREAD_IDENTITY: &str = "codex/thread-identity";
const CONTROL_ENDPOINT: &str = "codex/control-endpoint";

/// Includes eligibility in connection identity even when no value is present.
/// No serialization or Debug implementation: these values are not log metadata.
#[derive(Clone, Default, PartialEq, Eq)]
pub(crate) struct ConnectionBindings {
    thread_eligible: bool,
    canonical_thread_id: Option<String>,
    endpoint_eligible: bool,
    control_endpoint: Option<String>,
}

impl ConnectionBindings {
    pub(crate) fn new(
        thread_eligible: bool,
        canonical_thread_id: Option<String>,
        endpoint_eligible: bool,
        control_endpoint: Option<String>,
    ) -> Self {
        Self {
            thread_eligible,
            canonical_thread_id: canonical_thread_id.filter(|_| thread_eligible),
            endpoint_eligible,
            control_endpoint: control_endpoint.filter(|_| endpoint_eligible),
        }
    }

    fn requests(&self, direct_peer_info: &ServerPeerInfo) -> Vec<(&'static str, Value)> {
        let mut requests = Vec::new();
        if let Some(thread_id) = &self.canonical_thread_id
            && compatible(direct_peer_info, THREAD_IDENTITY)
        {
            requests.push((
                THREAD_IDENTITY,
                json!({"version": 1, "threadId": thread_id}),
            ));
        }
        if let Some(endpoint) = &self.control_endpoint
            && compatible(direct_peer_info, CONTROL_ENDPOINT)
        {
            requests.push((
                CONTROL_ENDPOINT,
                json!({"version": 1, "endpoint": endpoint}),
            ));
        }
        requests
    }

    pub(crate) async fn bind_initial(
        &self,
        client: &RmcpClient,
        direct_peer_info: &ServerPeerInfo,
        timeout: Option<Duration>,
    ) -> Result<()> {
        for (method, params) in self.requests(direct_peer_info) {
            let response = client
                .send_custom_request_with_timeout(method, Some(params), timeout)
                .await?;
            require_ack(method, &response)?;
        }
        Ok(())
    }

    pub(crate) fn reconnect_hook(&self, timeout: Option<Duration>) -> PostReconnectHook {
        let bindings = self.clone();
        Box::new(move |context, direct_peer_info| {
            let requests = bindings.requests(direct_peer_info);
            async move {
                for (method, params) in requests {
                    let response = context
                        .send_custom_request(method, Some(params), timeout)
                        .await?;
                    require_ack(method, &response)?;
                }
                Ok(())
            }
            .boxed()
        })
    }
}

fn compatible(peer_info: &ServerPeerInfo, key: &str) -> bool {
    peer_info
        .capabilities
        .experimental
        .as_ref()
        .and_then(|experimental| experimental.get(key))
        .and_then(|capability| capability.get("versions"))
        .is_some_and(compatible_versions)
}

fn compatible_versions(versions: &Value) -> bool {
    let Some(entries) = versions.as_array() else {
        return false;
    };
    if entries.is_empty() {
        return false;
    }
    let mut seen = HashSet::new();
    for entry in entries {
        let Some(version) = entry.as_i64() else {
            return false;
        };
        if version <= 0 || !seen.insert(version) {
            return false;
        }
    }
    seen.contains(&1)
}

fn require_ack(method: &str, response: &ServerResult) -> Result<()> {
    if matches!(response, ServerResult::CustomResult(CustomResult(value)) if value == &json!({"version": 1, "accepted": true}))
    {
        Ok(())
    } else {
        bail!("MCP server declared {method} but rejected or malformed the acknowledgement")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(experimental: Value) -> ServerPeerInfo {
        serde_json::from_value(json!({
            "protocolVersion": "2025-06-18",
            "capabilities": {"experimental": experimental},
            "serverInfo": {"name": "negotiation-fixture", "version": "1"},
        }))
        .expect("valid direct-handshake fixture")
    }

    #[test]
    fn version_grammar_refuses_partially_valid_declarations() {
        for value in [
            json!(null),
            json!(1),
            json!([]),
            json!([0, 1]),
            json!([-1, 1]),
            json!([1.0]),
            json!([true, 1]),
            json!(["1"]),
            json!([1, 1]),
            json!([2, 2, 1]),
            json!([2, 3]),
        ] {
            assert!(!compatible_versions(&value), "invalid declaration: {value}");
        }
        for value in [json!([1]), json!([2, 1]), json!([1, 2, 3])] {
            assert!(compatible_versions(&value));
        }
    }

    #[test]
    fn default_and_ineligible_connections_never_disclose_values() {
        let peer =
            peer(json!({THREAD_IDENTITY: {"versions": [1]}, CONTROL_ENDPOINT: {"versions": [1]}}));
        assert!(ConnectionBindings::default().requests(&peer).is_empty());
        assert!(
            ConnectionBindings::new(
                false,
                Some("thread-a".into()),
                false,
                Some("unix:///host".into())
            )
            .requests(&peer)
            .is_empty()
        );
        assert!(
            ConnectionBindings::new(true, None, true, None)
                .requests(&peer)
                .is_empty()
        );
    }

    #[test]
    fn each_capability_discloses_only_its_exact_explicitly_eligible_value() {
        let bindings = ConnectionBindings::new(
            true,
            Some("thread-a".into()),
            true,
            Some("unix:///host".into()),
        );
        assert_eq!(
            bindings.requests(&peer(json!({THREAD_IDENTITY: {"versions": [1]}}))),
            vec![(
                THREAD_IDENTITY,
                json!({"version": 1, "threadId": "thread-a"})
            )]
        );
        assert_eq!(
            bindings.requests(&peer(json!({CONTROL_ENDPOINT: {"versions": [1]}}))),
            vec![(
                CONTROL_ENDPOINT,
                json!({"version": 1, "endpoint": "unix:///host"})
            )]
        );
        assert_eq!(
            bindings
                .requests(&peer(
                    json!({THREAD_IDENTITY: {"versions": [1]}, CONTROL_ENDPOINT: {"versions": [1]}})
                ))
                .iter()
                .map(|(method, _)| *method)
                .collect::<Vec<_>>(),
            vec![THREAD_IDENTITY, CONTROL_ENDPOINT]
        );
    }

    #[test]
    fn unsupported_missing_and_malformed_objects_remain_ordinary() {
        let bindings = ConnectionBindings::new(
            true,
            Some("thread-a".into()),
            true,
            Some("unix:///host".into()),
        );
        for declaration in [
            json!({}),
            json!({"versions": []}),
            json!({"versions": [2]}),
            json!({"versions": [1, 1]}),
            json!({"version": 1}),
        ] {
            assert!(
                bindings
                    .requests(&peer(
                        json!({THREAD_IDENTITY: declaration.clone(), CONTROL_ENDPOINT: declaration})
                    ))
                    .is_empty()
            );
        }
        assert!(bindings.requests(&peer(json!({}))).is_empty());
    }

    #[test]
    fn acknowledgement_requires_exact_version_acceptance_and_no_extra_fields() {
        for value in [
            json!({}),
            json!({"version": 1, "accepted": false}),
            json!({"version": 2, "accepted": true}),
            json!({"version": 1.0, "accepted": true}),
            json!({"version": 1, "accepted": true, "threadId": "echo"}),
            json!({"version": 1, "accepted": "true"}),
        ] {
            assert!(
                require_ack(
                    THREAD_IDENTITY,
                    &ServerResult::CustomResult(CustomResult(value))
                )
                .is_err()
            );
        }
        assert!(
            require_ack(
                THREAD_IDENTITY,
                &ServerResult::CustomResult(CustomResult(json!({"accepted": true, "version": 1})))
            )
            .is_ok()
        );
    }

    #[test]
    fn non_object_experimental_entry_fails_peer_info_deserialization() {
        for value in [json!("not-an-object"), json!([1]), json!(true), json!(null)] {
            let parsed = serde_json::from_value::<ServerPeerInfo>(json!({
                "protocolVersion": "2025-06-18",
                "capabilities": {"experimental": {(THREAD_IDENTITY): value}},
                "serverInfo": {"name": "malformed-fixture", "version": "1"},
            }));
            assert!(parsed.is_err());
        }
    }

    #[test]
    fn reuse_identity_changes_for_thread_endpoint_and_eligibility() {
        let first = ConnectionBindings::new(
            true,
            Some("thread-a".into()),
            true,
            Some("unix:///host-a".into()),
        );
        for other in [
            ConnectionBindings::new(
                true,
                Some("thread-b".into()),
                true,
                Some("unix:///host-a".into()),
            ),
            ConnectionBindings::new(
                true,
                Some("thread-a".into()),
                true,
                Some("unix:///host-b".into()),
            ),
            ConnectionBindings::new(
                false,
                Some("thread-a".into()),
                true,
                Some("unix:///host-a".into()),
            ),
        ] {
            assert!(first != other);
        }
        assert!(ConnectionBindings::new(true, None, false, None) != ConnectionBindings::default());
    }
}
