//! Parent identity captured at a tool boundary, never reconstructed from IDs.

use crate::CodexThread;
use crate::session::Session;
use codex_extension_api::TurnWorkRefused;
use codex_protocol::host_turn_work::HostTurnWork;
use std::sync::Arc;
use std::sync::Weak;

/// Opaque provenance for deriving child work from an exact live parent turn.
/// Holding this value does not itself admit work or extend the parent's lease.
#[derive(Clone)]
pub struct ParentTurnAuthority {
    session: Weak<Session>,
    turn_id: String,
}

impl std::fmt::Debug for ParentTurnAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ParentTurnAuthority")
    }
}

impl ParentTurnAuthority {
    pub(crate) fn capture(session: &Arc<Session>, turn_id: &str) -> Self {
        Self { session: Arc::downgrade(session), turn_id: turn_id.to_owned() }
    }

    pub(crate) fn derive(
        &self,
        child: &CodexThread,
    ) -> Result<Option<Box<dyn HostTurnWork>>, TurnWorkRefused> {
        let parent = self.session.upgrade().ok_or(TurnWorkRefused::Unavailable)?;
        parent.services.extensions.derive_turn_work(
            &parent.services.thread_extension_data,
            &self.turn_id,
            &child.session.services.thread_extension_data,
            Box::pin(child.termination_receipt()),
        )
    }
}
