use super::*;
use pretty_assertions::assert_eq;

#[test]
fn hosted_endpoint_credentials_serialize_but_never_appear_in_debug() {
    let value = serde_json::json!({
        "url": "http://127.0.0.1:41000/mcp",
        "authorization": "Bearer fixture-authorization",
        "retirementCapability": "fixture-retirement-capability",
        "timeoutMs": 19
    });
    let declare: McpHostedEndpointDeclareParams =
        serde_json::from_value(value.clone()).expect("declare params");
    let retire: McpHostedEndpointRetireParams =
        serde_json::from_value(value.clone()).expect("retire params");
    assert_eq!(serde_json::to_value(&declare).expect("wire declare"), value);
    assert_eq!(serde_json::to_value(&retire).expect("wire retire"), value);
    for debug in [format!("{declare:?}"), format!("{retire:?}")] {
        assert!(!debug.contains("fixture-authorization"));
        assert!(!debug.contains("fixture-retirement-capability"));
        assert!(debug.contains("<redacted>"));
    }
}

#[test]
fn hosted_endpoint_requests_require_separate_retirement_authority_and_budget() {
    let valid = serde_json::json!({
        "url": "http://127.0.0.1:41000/mcp",
        "authorization": "Bearer fixture-authorization",
        "retirementCapability": "fixture-retirement-capability",
        "timeoutMs": 0
    });
    for missing in ["authorization", "retirementCapability", "timeoutMs"] {
        let mut value = valid.clone();
        value
            .as_object_mut()
            .expect("params object")
            .remove(missing);
        assert!(serde_json::from_value::<McpHostedEndpointDeclareParams>(value.clone()).is_err());
        assert!(serde_json::from_value::<McpHostedEndpointRetireParams>(value).is_err());
    }
    // Zero is explicit expired-budget input, not omission selecting a fresh budget.
    assert_eq!(
        serde_json::from_value::<McpHostedEndpointRetireParams>(valid)
            .expect("explicit zero")
            .timeout_ms,
        0
    );
}
