use crate::JsonSchema;
use crate::TS;
use serde::Deserialize;
use serde::Serialize;

/// Read this server's durable clear record for an exact successor.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ThreadClearReadParams {
    pub successor_thread_id: String,
}

/// Server-recorded succession, not proof of current interactive attachment.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ThreadClearTransition {
    pub transition_id: String,
    pub predecessor_thread_id: String,
    pub successor_thread_id: String,
}

/// `none` means no non-abandoned row in the available store, not a fresh-thread
/// authorization. Pending can remain unresolved or become none after abandonment.
/// Complete does not imply hook delivery or a current attachment.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(tag = "disposition", rename_all = "camelCase")]
#[ts(tag = "disposition", rename_all = "camelCase", export_to = "v2/")]
pub enum ThreadClearReadResponse {
    None { transition: () },
    Pending { transition: ThreadClearTransition },
    Complete { transition: ThreadClearTransition },
}

#[cfg(test)]
#[path = "thread_clear_read_tests.rs"]
mod tests;
