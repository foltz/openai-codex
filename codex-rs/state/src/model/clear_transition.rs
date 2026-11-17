use codex_protocol::ThreadId;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use std::str::FromStr;
use strum::AsRefStr;
use strum::Display;
use strum::EnumString;
use uuid::Uuid;

/// Stable identity shared by every durable and externally visible part of one clear transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ClearTransitionId(Uuid);

impl ClearTransitionId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

impl Default for ClearTransitionId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ClearTransitionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for ClearTransitionId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self)
    }
}

/// Durable orchestration phase for a clear transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr, Display, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum ClearTransitionPhase {
    Reserved,
    SuccessorCreated,
    Committed,
    EvidenceClaimed,
    Completed,
    Abandoned,
}

impl ClearTransitionPhase {
    pub fn can_advance_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Reserved, Self::SuccessorCreated)
                | (Self::SuccessorCreated, Self::Committed)
                | (Self::Committed, Self::EvidenceClaimed)
                | (Self::EvidenceClaimed, Self::Completed)
        )
    }

    pub fn can_abandon(self) -> bool {
        self == Self::Reserved
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Abandoned)
    }
}

/// Durable claim and delivery state for one side of the clear lifecycle pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr, Display, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum ClearTransitionEvidenceState {
    Pending,
    Claimed,
    Delivered,
    Failed,
}

impl ClearTransitionEvidenceState {
    pub fn can_advance_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Pending, Self::Claimed)
                | (Self::Claimed, Self::Delivered)
                | (Self::Claimed, Self::Failed)
        )
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Delivered | Self::Failed)
    }
}

/// Which ordered lifecycle observation an evidence state belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClearTransitionEvidenceKind {
    End,
    Start,
}

/// Complete durable state for one server-authorized clear transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClearTransitionRecord {
    pub transition_id: ClearTransitionId,
    pub predecessor_thread_id: ThreadId,
    pub successor_thread_id: ThreadId,
    pub phase: ClearTransitionPhase,
    pub end_evidence_state: ClearTransitionEvidenceState,
    pub start_evidence_state: ClearTransitionEvidenceState,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Result of atomically reserving predecessor, successor, and transition identities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClearTransitionReserveOutcome {
    Reserved(ClearTransitionRecord),
    PredecessorAlreadyReserved(ClearTransitionRecord),
    SuccessorAlreadyReserved(ClearTransitionRecord),
}
