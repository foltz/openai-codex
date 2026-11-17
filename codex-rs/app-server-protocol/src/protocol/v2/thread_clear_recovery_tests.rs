use super::*;
use pretty_assertions::assert_eq;
use serde_json::json;

#[test]
fn accepted_context_roundtrips_without_legacy_lineage_fields() {
    let value = json!({
        "contractVersion": 1, "durability": "durable", "observation": {
            "type": "pending", "recovery": {
                "successorThreadId": "B",
                "context": {"contractVersion": 1, "predecessorThreadId": "A"},
                "durability": "durable"
            }
        }
    });
    let response: ThreadClearRecoveryReadResponse = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(serde_json::to_value(response).unwrap(), value);
    // Malformed observation must not degrade to absence.
    for invalid in [
        json!({"contractVersion": 1, "durability": "durable"}),
        json!({"contractVersion": 1, "durability": "durable", "observation": {"type": "pending"}}),
        json!({"contractVersion": 1, "durability": "durable", "observation": {"type": "unknown"}}),
    ] {
        assert!(serde_json::from_value::<ThreadClearRecoveryReadResponse>(invalid).is_err());
    }
}
