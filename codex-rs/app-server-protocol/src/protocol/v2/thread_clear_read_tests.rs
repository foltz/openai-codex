use super::*;
use pretty_assertions::assert_eq;
use serde_json::json;

#[test]
fn clear_read_round_trips_closed_wire_variants() {
    let transition = ThreadClearTransition {
        transition_id: "transition".into(),
        predecessor_thread_id: "predecessor".into(),
        successor_thread_id: "successor".into(),
    };
    for (response, disposition) in [
        (ThreadClearReadResponse::None { transition: () }, "none"),
        (
            ThreadClearReadResponse::Pending {
                transition: transition.clone(),
            },
            "pending",
        ),
        (
            ThreadClearReadResponse::Complete {
                transition: transition.clone(),
            },
            "complete",
        ),
    ] {
        let expected = json!({
            "disposition": disposition,
            "transition": if disposition == "none" { serde_json::Value::Null } else { serde_json::to_value(&transition).unwrap() }
        });
        assert_eq!(serde_json::to_value(&response).unwrap(), expected);
        assert_eq!(
            serde_json::from_value::<ThreadClearReadResponse>(expected).unwrap(),
            response
        );
    }
    for invalid in [
        json!({"disposition":"none", "transition":transition}),
        json!({"disposition":"pending", "transition":null}),
        json!({"disposition":"complete", "transition":null}),
        json!({"disposition":"unknown", "transition":null}),
    ] {
        assert!(serde_json::from_value::<ThreadClearReadResponse>(invalid).is_err());
    }
}
