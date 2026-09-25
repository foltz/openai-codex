use crate::JsonSchema;
use crate::TS;
use serde::Deserialize;
use serde::Serialize;

/// Caller provenance, never an authoritative clear edge or a shutdown receipt.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[ts(export_to = "v2/")]
pub struct ThreadClearRecoveryContext {
    /// Must match the recovery-read contract on this connection before creation.
    pub contract_version: u32,
    /// Null means genuinely no displayed predecessor, not an unavailable known ID.
    pub predecessor_thread_id: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub enum ThreadClearRecoveryDurability {
    Durable,
    Unavailable,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ThreadClearRecovery {
    pub successor_thread_id: String,
    pub context: ThreadClearRecoveryContext,
    pub durability: ThreadClearRecoveryDurability,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ThreadClearRecoveryReadParams {
    /// Omit to probe read AND thread/start.clearRecovery revision support.
    #[ts(optional = nullable)]
    pub successor_thread_id: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(tag = "type", rename_all = "camelCase")]
#[ts(tag = "type", rename_all = "camelCase", export_to = "v2/")]
pub enum ThreadClearRecoveryObservation {
    Support,
    /// Absence is not proof of freshness or permission to register an identity.
    None,
    /// May remain pending after a crash; consumers must bound their wait.
    Pending {
        recovery: ThreadClearRecovery,
    },
    Complete {
        recovery: ThreadClearRecovery,
    },
    Failed {
        recovery: ThreadClearRecovery,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ThreadClearRecoveryReadResponse {
    /// Revision 1 binds this read to thread/start.clearRecovery revision 1.
    pub contract_version: u32,
    pub durability: ThreadClearRecoveryDurability,
    pub observation: ThreadClearRecoveryObservation,
}

#[cfg(test)]
#[path = "thread_clear_recovery_tests.rs"]
mod tests;
