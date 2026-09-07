use crate::JsonSchema;
use crate::TS;
use serde::Deserialize;
use serde::Serialize;

/// A token-free intent for a managed transition. The credential writer and the
/// authoritative auth operation intentionally live behind later server-side
/// admission stages; neither is represented on this wire surface.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase", export_to = "v2/")]
pub enum ManagedTransitionIntent {
    AdoptManagedAuth,
    AdoptManagedLogout,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase", export_to = "v2/")]
pub enum ManagedTransitionPhase {
    Idle,
    Admitted,
    Draining,
    Adopting,
    Resetting,
    Succeeded,
    Cancelled,
    Quarantined,
}

impl ManagedTransitionPhase {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Cancelled | Self::Quarantined)
    }
}

/// Typed, secret-safe reasons for a transition request to be refused.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase", export_to = "v2/")]
pub enum ManagedTransitionRefusalKind {
    InvalidRequest,
    ProcessMismatch,
    StaleAuthRevision,
    StaleTransitionRevision,
    StaleAuthFingerprint,
    ConcurrentTransition,
    CompletedReplay,
    TransitionIdConflict,
    LateCancellation,
    AuthorizationNotAdmitted,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ManagedTransitionRefusal {
    pub kind: ManagedTransitionRefusalKind,
    pub retryable: bool,
    pub process_instance_id: String,
    pub transition_id: String,
    pub auth_revision: u64,
    pub transition_revision: u64,
    pub auth_fingerprint: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ManagedTransitionStatus {
    pub process_instance_id: String,
    pub transition_id: Option<String>,
    pub intent: Option<ManagedTransitionIntent>,
    pub phase: ManagedTransitionPhase,
    pub retryable: bool,
    pub auth_revision: u64,
    pub transition_revision: u64,
    pub prior_auth_fingerprint: Option<String>,
    pub result_auth_fingerprint: Option<String>,
    pub refusal: Option<ManagedTransitionRefusalKind>,
}

/// Token-free initiation envelope. The expected revision and opaque
/// fingerprint intentionally remain separate values: a version CAS is not an
/// account-identity equality proof.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct StartManagedTransitionParams {
    pub transition_id: String,
    pub process_instance_id: String,
    pub intent: ManagedTransitionIntent,
    pub expected_auth_revision: u64,
    pub expected_transition_revision: u64,
    pub expected_auth_fingerprint: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(tag = "type", rename_all = "camelCase")]
#[ts(tag = "type", export_to = "v2/")]
pub enum StartManagedTransitionResponse {
    Accepted { status: ManagedTransitionStatus },
    Refused { refusal: ManagedTransitionRefusal },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ReadManagedTransitionParams {
    pub transition_id: String,
    pub process_instance_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(tag = "type", rename_all = "camelCase")]
#[ts(tag = "type", export_to = "v2/")]
pub enum ReadManagedTransitionResponse {
    Accepted { status: ManagedTransitionStatus },
    Refused { refusal: ManagedTransitionRefusal },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct CancelManagedTransitionParams {
    pub transition_id: String,
    pub process_instance_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(tag = "type", rename_all = "camelCase")]
#[ts(tag = "type", export_to = "v2/")]
pub enum CancelManagedTransitionResponse {
    Accepted { status: ManagedTransitionStatus },
    Refused { refusal: ManagedTransitionRefusal },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ManagedTransitionStatusNotification {
    pub status: ManagedTransitionStatus,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TS;

    #[test]
    fn start_params_round_trip_and_export_without_sensitive_fields() {
        let params = StartManagedTransitionParams {
            transition_id: "transition-a".to_owned(),
            process_instance_id: "process-a".to_owned(),
            intent: ManagedTransitionIntent::AdoptManagedAuth,
            expected_auth_revision: 4,
            expected_transition_revision: 7,
            expected_auth_fingerprint: Some("opaque-fingerprint".to_owned()),
        };
        let json = serde_json::to_string(&params).unwrap();
        assert_eq!(
            serde_json::from_str::<StartManagedTransitionParams>(&json).unwrap(),
            params
        );

        let projection = StartManagedTransitionParams::export_to_string().unwrap();
        for forbidden in ["accessToken", "apiKey", "email", "accountId", "workspaceId"] {
            assert!(
                !projection.contains(forbidden),
                "projection leaked {forbidden}"
            );
        }
    }
}
