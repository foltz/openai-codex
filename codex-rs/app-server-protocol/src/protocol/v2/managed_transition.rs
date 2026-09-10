use crate::JsonSchema;
use crate::TS;
use codex_experimental_api_macros::ExperimentalApi;
use serde::Deserialize;
use serde::Serialize;
use serde::de::Error as _;

/// The only accepted wire version for the managed-auth transition family.
pub const MANAGED_AUTH_TRANSITION_CONTRACT_VERSION: u8 = 1;

fn deserialize_contract_version<'de, D>(deserializer: D) -> Result<u8, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let version = u8::deserialize(deserializer)?;
    if version == MANAGED_AUTH_TRANSITION_CONTRACT_VERSION {
        Ok(version)
    } else {
        Err(D::Error::custom(format!(
            "unsupported managed-auth transition contract version {version}"
        )))
    }
}

/// Deserialize a nullable fingerprint while still requiring the field to be
/// present. `null` is the explicit logged-out value; omission is malformed.
fn deserialize_required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[cfg(test)]
fn nullable_string_schema(
    generator: &mut schemars::r#gen::SchemaGenerator,
) -> schemars::schema::Schema {
    generator.subschema_for::<Option<String>>()
}

#[cfg(test)]
fn nullable_intent_schema(
    generator: &mut schemars::r#gen::SchemaGenerator,
) -> schemars::schema::Schema {
    generator.subschema_for::<Option<ManagedTransitionIntent>>()
}

#[cfg(test)]
fn nullable_refusal_kind_schema(
    generator: &mut schemars::r#gen::SchemaGenerator,
) -> schemars::schema::Schema {
    generator.subschema_for::<Option<ManagedTransitionRefusalKind>>()
}

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
// The same vocabulary carries terminal quarantine causes in status.refusal.
// New causes are experimental; existing stable type exports remain unchanged.
#[derive(
    Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, JsonSchema, TS, ExperimentalApi,
)]
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
    AuthoritativeAuthUnavailable,
    #[experimental("managedAccountTransitionTerminalCauses")]
    DrainTimedOut,
    #[experimental("managedAccountTransitionTerminalCauses")]
    TargetChanged,
    #[experimental("managedAccountTransitionTerminalCauses")]
    AuthSourceChanged,
    #[experimental("managedAccountTransitionTerminalCauses")]
    ResetFailed,
    #[experimental("managedAccountTransitionTerminalCauses")]
    AuthInstallFailed,
    #[experimental("managedAccountTransitionTerminalCauses")]
    IntendedResultMismatch,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[ts(export_to = "v2/")]
pub struct ManagedTransitionRefusal {
    pub kind: ManagedTransitionRefusalKind,
    pub retryable: bool,
    pub process_instance_id: String,
    pub transition_id: String,
    pub auth_revision: u64,
    pub transition_revision: u64,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    #[schemars(required, schema_with = "nullable_string_schema")]
    pub auth_fingerprint: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[ts(export_to = "v2/")]
pub struct ManagedTransitionStatus {
    pub process_instance_id: String,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    #[schemars(required, schema_with = "nullable_string_schema")]
    pub transition_id: Option<String>,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    #[schemars(required, schema_with = "nullable_intent_schema")]
    pub intent: Option<ManagedTransitionIntent>,
    pub phase: ManagedTransitionPhase,
    pub retryable: bool,
    pub prior_auth_revision: u64,
    pub auth_revision: u64,
    pub prior_transition_revision: u64,
    pub transition_revision: u64,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    #[schemars(required, schema_with = "nullable_string_schema")]
    pub prior_auth_fingerprint: Option<String>,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    #[schemars(required, schema_with = "nullable_string_schema")]
    pub result_auth_fingerprint: Option<String>,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    #[schemars(required, schema_with = "nullable_refusal_kind_schema")]
    pub refusal: Option<ManagedTransitionRefusalKind>,
}

/// Token-free initiation envelope. The expected revision and opaque
/// fingerprint intentionally remain separate values: a version CAS is not an
/// account-identity equality proof.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[ts(export_to = "v2/")]
pub struct StartManagedTransitionParams {
    #[serde(deserialize_with = "deserialize_contract_version")]
    pub contract_version: u8,
    pub transition_id: String,
    pub process_instance_id: String,
    pub intent: ManagedTransitionIntent,
    pub expected_auth_revision: u64,
    pub expected_transition_revision: u64,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    #[schemars(required, schema_with = "nullable_string_schema")]
    pub expected_auth_fingerprint: Option<String>,
    /// Independently binds the intended result. Explicit null means managed
    /// logout; account adoption requires a nonempty opaque fingerprint.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    #[schemars(required, schema_with = "nullable_string_schema")]
    pub intended_result_auth_fingerprint: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
#[ts(tag = "type", export_to = "v2/")]
pub enum StartManagedTransitionResponse {
    Accepted { status: ManagedTransitionStatus },
    Refused { refusal: ManagedTransitionRefusal },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[ts(export_to = "v2/")]
pub struct ReadManagedTransitionParams {
    #[serde(deserialize_with = "deserialize_contract_version")]
    pub contract_version: u8,
    pub transition_id: String,
    pub process_instance_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
#[ts(tag = "type", export_to = "v2/")]
pub enum ReadManagedTransitionResponse {
    Accepted { status: ManagedTransitionStatus },
    Refused { refusal: ManagedTransitionRefusal },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[ts(export_to = "v2/")]
pub struct CancelManagedTransitionParams {
    #[serde(deserialize_with = "deserialize_contract_version")]
    pub contract_version: u8,
    pub transition_id: String,
    pub process_instance_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
#[ts(tag = "type", export_to = "v2/")]
pub enum CancelManagedTransitionResponse {
    Accepted { status: ManagedTransitionStatus },
    Refused { refusal: ManagedTransitionRefusal },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
            contract_version: MANAGED_AUTH_TRANSITION_CONTRACT_VERSION,
            transition_id: "transition-a".to_owned(),
            process_instance_id: "process-a".to_owned(),
            intent: ManagedTransitionIntent::AdoptManagedAuth,
            expected_auth_revision: 4,
            expected_transition_revision: 7,
            expected_auth_fingerprint: Some("opaque-fingerprint".to_owned()),
            intended_result_auth_fingerprint: Some("intended-fingerprint".to_owned()),
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

    #[test]
    fn request_decoders_are_closed_versioned_and_require_nullable_fingerprint() {
        let start = StartManagedTransitionParams {
            contract_version: MANAGED_AUTH_TRANSITION_CONTRACT_VERSION,
            transition_id: "transition-a".to_owned(),
            process_instance_id: "process-a".to_owned(),
            intent: ManagedTransitionIntent::AdoptManagedAuth,
            expected_auth_revision: 4,
            expected_transition_revision: 7,
            expected_auth_fingerprint: None,
            intended_result_auth_fingerprint: None,
        };
        let mut unknown = serde_json::to_value(&start).unwrap();
        unknown["accessToken"] = serde_json::json!("secret");
        assert!(serde_json::from_value::<StartManagedTransitionParams>(unknown).is_err());

        let mut wrong_version = serde_json::to_value(&start).unwrap();
        wrong_version["contractVersion"] = serde_json::json!(2);
        assert!(serde_json::from_value::<StartManagedTransitionParams>(wrong_version).is_err());

        let mut missing_fingerprint = serde_json::to_value(&start).unwrap();
        missing_fingerprint
            .as_object_mut()
            .unwrap()
            .remove("expectedAuthFingerprint");
        assert!(
            serde_json::from_value::<StartManagedTransitionParams>(missing_fingerprint).is_err()
        );
        let mut missing_intended = serde_json::to_value(&start).unwrap();
        missing_intended
            .as_object_mut()
            .unwrap()
            .remove("intendedResultAuthFingerprint");
        assert!(serde_json::from_value::<StartManagedTransitionParams>(missing_intended).is_err());

        for value in [
            serde_json::json!({
                "contractVersion": 1,
                "transitionId": "transition-a",
                "processInstanceId": "process-a",
                "unexpected": true,
            }),
            serde_json::json!({
                "contractVersion": 2,
                "transitionId": "transition-a",
                "processInstanceId": "process-a",
            }),
        ] {
            assert!(serde_json::from_value::<ReadManagedTransitionParams>(value.clone()).is_err());
            assert!(serde_json::from_value::<CancelManagedTransitionParams>(value).is_err());
        }
    }

    #[test]
    fn responses_and_notifications_are_closed_and_secret_safe() {
        let status = ManagedTransitionStatus {
            process_instance_id: "process-a".to_owned(),
            transition_id: Some("transition-a".to_owned()),
            intent: Some(ManagedTransitionIntent::AdoptManagedAuth),
            phase: ManagedTransitionPhase::Admitted,
            retryable: false,
            prior_auth_revision: 3,
            auth_revision: 4,
            prior_transition_revision: 6,
            transition_revision: 7,
            prior_auth_fingerprint: None,
            result_auth_fingerprint: None,
            refusal: None,
        };
        let response = StartManagedTransitionResponse::Accepted {
            status: status.clone(),
        };
        let mut response_json = serde_json::to_value(&response).unwrap();
        response_json["accessToken"] = serde_json::json!("secret");
        assert!(serde_json::from_value::<StartManagedTransitionResponse>(response_json).is_err());

        let notification = ManagedTransitionStatusNotification { status };
        let json = serde_json::to_string(&notification).unwrap();
        for forbidden in ["accessToken", "apiKey", "email", "accountId", "workspaceId"] {
            assert!(!json.contains(forbidden), "notification leaked {forbidden}");
        }
        for projection in [
            ManagedTransitionStatus::export_to_string().unwrap(),
            ManagedTransitionRefusal::export_to_string().unwrap(),
            ManagedTransitionStatusNotification::export_to_string().unwrap(),
        ] {
            for forbidden in ["accessToken", "apiKey", "email", "accountId", "workspaceId"] {
                assert!(
                    !projection.contains(forbidden),
                    "projection leaked {forbidden}"
                );
            }
        }
    }

    #[test]
    fn required_nullable_status_and_refusal_fields_reject_omission_in_runtime_and_schema() {
        let status = ManagedTransitionStatus {
            process_instance_id: "process-a".to_owned(),
            transition_id: None,
            intent: None,
            phase: ManagedTransitionPhase::Idle,
            retryable: false,
            prior_auth_revision: 0,
            auth_revision: 0,
            prior_transition_revision: 0,
            transition_revision: 0,
            prior_auth_fingerprint: None,
            result_auth_fingerprint: None,
            refusal: None,
        };
        for key in [
            "transitionId",
            "intent",
            "priorAuthFingerprint",
            "resultAuthFingerprint",
            "refusal",
        ] {
            let mut value = serde_json::to_value(&status).unwrap();
            value.as_object_mut().unwrap().remove(key);
            assert!(serde_json::from_value::<ManagedTransitionStatus>(value).is_err());
        }
        let refusal = ManagedTransitionRefusal {
            kind: ManagedTransitionRefusalKind::InvalidRequest,
            retryable: false,
            process_instance_id: "process-a".to_owned(),
            transition_id: "transition-a".to_owned(),
            auth_revision: 0,
            transition_revision: 0,
            auth_fingerprint: None,
        };
        let mut value = serde_json::to_value(&refusal).unwrap();
        value.as_object_mut().unwrap().remove("authFingerprint");
        assert!(serde_json::from_value::<ManagedTransitionRefusal>(value).is_err());

        let start_schema = schemars::schema_for!(StartManagedTransitionParams);
        let object = start_schema.schema.object.unwrap();
        let required = object.required;
        assert!(required.contains("expectedAuthFingerprint"));
        let fingerprint = object
            .properties
            .get("expectedAuthFingerprint")
            .expect("fingerprint schema");
        let fingerprint = serde_json::to_value(fingerprint).unwrap();
        assert_eq!(fingerprint["type"], serde_json::json!(["string", "null"]));
    }
}
