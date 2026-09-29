//! Exclusive mailbox batches survive cancellation without an async Drop task.
//!
//! A reservation keeps complete mail records private until consumption. Drop
//! restores them by enqueue order, including when reservations return in a
//! different order. The mutex protects only in-memory queue operations; it is
//! never held across an await or caller-supplied code.

use super::input_queue::InputQueueActivity;
use super::input_queue::TurnInput;
use codex_diagnostics::Gauge;
use codex_diagnostics::GaugeGuard;
use codex_protocol::host_turn_work::HostTurnWork;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::turn_input::TurnStartOptions;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::watch;

static PENDING_MAILBOX_MESSAGES: Gauge = Gauge::new("core.mailbox.pending");

struct PendingMailboxCommunication {
    communication: InterAgentCommunication,
    start_options: TurnStartOptions,
    work: Option<Box<dyn HostTurnWork>>,
    _diagnostics_guard: GaugeGuard,
}

#[derive(Default)]
struct MailboxState {
    next_sequence: u64,
    pending: BTreeMap<u64, PendingMailboxCommunication>,
}

pub(super) struct Mailbox {
    state: Arc<Mutex<MailboxState>>,
    activity: watch::Sender<InputQueueActivity>,
}

/// Owns mail removed from the deliverable queue. Until `into_input`, dropping
/// the owner returns the complete records, not reconstructed input fragments.
#[must_use]
pub(crate) struct MailboxReservation {
    state: Arc<Mutex<MailboxState>>,
    activity: watch::Sender<InputQueueActivity>,
    pending: BTreeMap<u64, PendingMailboxCommunication>,
}

impl Mailbox {
    pub(super) fn new(activity: watch::Sender<InputQueueActivity>) -> Self {
        Self {
            state: Arc::new(Mutex::new(MailboxState::default())),
            activity,
        }
    }

    pub(super) fn enqueue(
        &self,
        communication: InterAgentCommunication,
        start_options: TurnStartOptions,
    ) {
        self.enqueue_with_work(communication, start_options, /*work*/ None);
    }

    #[expect(
        clippy::expect_used,
        reason = "mail custody fails closed on poison or enqueue sequence exhaustion"
    )]
    pub(super) fn enqueue_with_work(
        &self,
        communication: InterAgentCommunication,
        start_options: TurnStartOptions,
        work: Option<Box<dyn HostTurnWork>>,
    ) {
        assert!(
            communication.trigger_turn || work.is_none(),
            "queue-only mail must not carry turn work"
        );
        let mut state = self.state.lock().expect("mailbox poisoned");
        let sequence = state.next_sequence;
        state.next_sequence = sequence.checked_add(1).expect("mailbox sequence exhausted");
        state.pending.insert(
            sequence,
            PendingMailboxCommunication {
                communication,
                start_options,
                work,
                _diagnostics_guard: PENDING_MAILBOX_MESSAGES.track(),
            },
        );
        drop(state);
        self.activity.send_replace(InputQueueActivity::Mailbox);
    }

    /// Reports only deliverable mail; another attempt's private reservation
    /// cannot authorize or feed a competing start.
    #[expect(
        clippy::expect_used,
        reason = "poisoned queue state cannot establish deliverable mail"
    )]
    pub(super) fn has_pending(&self) -> bool {
        !self
            .state
            .lock()
            .expect("mailbox poisoned")
            .pending
            .is_empty()
    }

    #[expect(
        clippy::expect_used,
        reason = "poisoned queue state cannot authorize a trigger turn"
    )]
    pub(super) fn has_trigger(&self) -> bool {
        self.state
            .lock()
            .expect("mailbox poisoned")
            .pending
            .values()
            .any(|mail| mail.communication.trigger_turn)
    }

    #[expect(
        clippy::expect_used,
        reason = "mail custody cannot transfer out of poisoned queue state"
    )]
    pub(super) fn reserve(&self) -> MailboxReservation {
        let pending = std::mem::take(&mut self.state.lock().expect("mailbox poisoned").pending);
        MailboxReservation {
            state: Arc::clone(&self.state),
            activity: self.activity.clone(),
            pending,
        }
    }
}

impl MailboxReservation {
    pub(crate) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Only this owned batch can authorize its start; peeking at deliverable
    /// mail would allow a competing consumer to take the same authority.
    pub(crate) fn has_turn_work(&self) -> bool {
        self.pending.values().any(|mail| mail.work.is_some())
    }

    pub(crate) fn append(&mut self, mut other: Self) {
        assert!(
            Arc::ptr_eq(&self.state, &other.state),
            "mailbox reservation owner mismatch"
        );
        self.pending.append(&mut other.pending);
    }

    pub(crate) fn start_metadata(&self) -> (TurnStartOptions, Option<codex_protocol::AgentPath>) {
        // Preserve upstream's settings selection: the latest trigger wins,
        // while parent identity requires agreement across every trigger.
        let mut start_options = self
            .pending
            .values()
            .rev()
            .find(|mail| mail.communication.trigger_turn)
            .map(|mail| mail.start_options.clone())
            .unwrap_or_default();
        start_options.parent_turn_id = self
            .pending
            .values()
            .filter(|mail| mail.communication.trigger_turn)
            .map(|mail| mail.start_options.parent_turn_id.as_deref())
            .reduce(|expected, candidate| expected.filter(|id| candidate == Some(*id)))
            .and_then(|id| id.filter(|id| !id.trim().is_empty()).map(str::to_string));
        start_options.root_turn_id = self
            .pending
            .values()
            .find(|mail| mail.communication.trigger_turn)
            .and_then(|mail| {
                mail.start_options
                    .parent_turn_id
                    .as_deref()
                    .filter(|id| !id.trim().is_empty())
                    .and(mail.start_options.root_turn_id.as_deref())
                    .filter(|id| !id.trim().is_empty())
            })
            .map(str::to_string);
        let author = self
            .pending
            .values()
            .find(|mail| mail.communication.trigger_turn)
            .map(|mail| mail.communication.author.clone());
        (start_options, author)
    }

    pub(super) fn into_input(mut self, turn_id: &str) -> (Vec<TurnInput>, TurnStartOptions) {
        let (start_options, _) = self.start_metadata();
        let items = std::mem::take(&mut self.pending)
            .into_values()
            .map(|mail| {
                // Binding and transfer are synchronous. No cancellation point
                // can split consumption from host terminal-evidence custody.
                if let Some(mut work) = mail.work {
                    work.bind_submission(turn_id);
                    work.retain_until_terminal();
                }
                TurnInput::InterAgentCommunication(mail.communication)
            })
            .collect();
        (items, start_options)
    }
}

impl Drop for MailboxReservation {
    fn drop(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        // Rollback also runs during unwinding. Preserve custody without a
        // second panic; ordinary queue access still refuses poisoned state.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pending
            .append(&mut self.pending);
        // Wake a later consumer after rollback, including cancellation while
        // other input was enqueued. No task or runtime is needed for restoration.
        self.activity.send_replace(InputQueueActivity::Mailbox);
    }
}

#[cfg(test)]
#[path = "mailbox_tests.rs"]
mod tests;
