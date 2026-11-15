//! Exclusive mailbox batches survive cancellation without an async Drop task.
//!
//! A reservation keeps complete mail records private until consumption. Drop
//! restores them by enqueue order, including when reservations return in a
//! different order. The mutex protects only in-memory queue operations; it is
//! never held across an await. The existing tree-controller mailbox handoff
//! remains atomic with local enqueue; attachment destruction happens after unlock.

use super::input_queue::InputQueueActivity;
use super::input_queue::TurnInput;
use crate::agent::api::AgentControl;
use crate::agent_communication::PENDING_MAILBOX_MESSAGES;
use codex_diagnostics::GaugeGuard;
use codex_protocol::ThreadId;
use codex_protocol::host_turn_work::HostTurnWork;
use codex_protocol::turn_input::TurnStartOptions;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::watch;

pub(crate) struct PendingMailboxCommunication {
    pub(crate) communication: crate::PendingAgentMessage,
    pub(super) start_options: TurnStartOptions,
    work: Option<Box<dyn HostTurnWork>>,
    _diagnostics_guard: GaugeGuard,
}

#[derive(Default)]
struct MailboxState {
    next_sequence: u64,
    open_reservations: usize,
    eviction_fenced: bool,
    pending: BTreeMap<u64, PendingMailboxCommunication>,
}

pub(super) struct Mailbox {
    state: Arc<Mutex<MailboxState>>,
    activity: watch::Sender<InputQueueActivity>,
    controller: Option<(ThreadId, Arc<dyn AgentControl>, watch::Receiver<bool>)>,
}

/// Owns mail removed from the deliverable queue. Until `into_input`, dropping
/// the owner returns the complete records, not reconstructed input fragments.
#[must_use]
pub(crate) struct MailboxReservation {
    state: Arc<Mutex<MailboxState>>,
    activity: watch::Sender<InputQueueActivity>,
    pending: BTreeMap<u64, PendingMailboxCommunication>,
    counted: bool,
}

pub(crate) struct MailboxEvictionFence {
    state: Arc<Mutex<MailboxState>>,
    activity: watch::Sender<InputQueueActivity>,
    wake: Option<Arc<tokio::sync::Notify>>,
    committed: bool,
}

impl MailboxEvictionFence {
    pub(crate) fn with_wake(mut self, wake: Arc<tokio::sync::Notify>) -> Self {
        self.wake = Some(wake);
        self
    }
    pub(crate) fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for MailboxEvictionFence {
    fn drop(&mut self) {
        if !self.committed {
            self.state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .eviction_fenced = false;
            self.activity.send_replace(InputQueueActivity::Mailbox);
            if let Some(wake) = &self.wake {
                wake.notify_one();
            }
        }
    }
}

impl Mailbox {
    pub(super) fn new(activity: watch::Sender<InputQueueActivity>) -> Self {
        Self {
            state: Arc::new(Mutex::new(MailboxState::default())),
            activity,
            controller: None,
        }
    }

    pub(super) fn set_controller(&mut self, thread_id: ThreadId, control: Arc<dyn AgentControl>) {
        let updates = control.watch_mailbox(thread_id);
        self.controller = Some((thread_id, control, updates));
    }

    pub(super) fn updates(&self) -> Option<watch::Receiver<bool>> {
        self.controller
            .as_ref()
            .map(|(_, _, updates)| updates.clone())
    }

    #[expect(clippy::expect_used)]
    fn read_retained_mail(&self, state: &mut MailboxState) -> bool {
        let mut read = false;
        if let Some((thread_id, control, _)) = &self.controller {
            for communication in control.take_mailbox(*thread_id) {
                read = true;
                let sequence = state.next_sequence;
                state.next_sequence = sequence.checked_add(1).expect("mailbox sequence exhausted");
                state.pending.insert(
                    sequence,
                    PendingMailboxCommunication {
                        communication,
                        start_options: TurnStartOptions::default(),
                        work: None,
                        _diagnostics_guard: PENDING_MAILBOX_MESSAGES.track(),
                    },
                );
            }
        }
        read
    }

    #[expect(clippy::expect_used)]
    pub(super) fn try_fence_eviction(&self) -> Option<MailboxEvictionFence> {
        let mut state = self.state.lock().expect("mailbox poisoned");
        let read = self.read_retained_mail(&mut state);
        let eligible = state.open_reservations == 0
            && !state.eviction_fenced
            && !state
                .pending
                .values()
                .any(|mail| mail.communication.trigger_turn || mail.work.is_some());
        if eligible {
            state.eviction_fenced = true;
        }
        drop(state);
        if read {
            self.activity.send_replace(InputQueueActivity::Mailbox);
        }
        eligible.then(|| MailboxEvictionFence {
            state: Arc::clone(&self.state),
            activity: self.activity.clone(),
            wake: None,
            committed: false,
        })
    }

    #[expect(clippy::expect_used)]
    pub(super) fn eviction_fenced(&self) -> bool {
        self.state.lock().expect("mailbox poisoned").eviction_fenced
    }

    pub(super) fn drain_for_eviction(
        &self,
    ) -> Result<Vec<PendingMailboxCommunication>, (Vec<PendingMailboxCommunication>, &'static str)>
    {
        let (mut state, poisoned) = match self.state.lock() {
            Ok(state) => (state, false),
            Err(error) => (error.into_inner(), true),
        };
        self.read_retained_mail(&mut state);
        let refused = poisoned
            || state.open_reservations != 0
            || state
                .pending
                .values()
                .any(|mail| mail.work.is_some() || mail.communication.trigger_turn);
        let pending = std::mem::take(&mut state.pending).into_values().collect();
        if refused {
            Err((pending, "mailbox custody remains active"))
        } else {
            Ok(pending)
        }
    }

    /// Temporary compatibility for current unaccounted consumers. This never
    /// releases or transfers a private batch or counted turn work. Remove it
    /// when the task-installation transaction owns all production reservations.
    #[expect(clippy::expect_used)]
    pub(super) fn drain_unaccounted(&self) -> Vec<PendingMailboxCommunication> {
        let mut state = self.state.lock().expect("mailbox poisoned");
        self.read_retained_mail(&mut state);
        if state.open_reservations != 0 || state.pending.values().any(|mail| mail.work.is_some()) {
            drop(state);
            panic!("unaccounted mailbox drain cannot consume reserved or accounted mail");
        }
        std::mem::take(&mut state.pending).into_values().collect()
    }

    #[cfg(test)]
    pub(super) fn enqueue(
        &self,
        communication: impl Into<crate::PendingAgentMessage>,
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
        communication: impl Into<crate::PendingAgentMessage>,
        start_options: TurnStartOptions,
        work: Option<Box<dyn HostTurnWork>>,
    ) {
        let communication = communication.into();
        assert!(
            communication.trigger_turn || work.is_none(),
            "queue-only mail must not carry turn work"
        );
        let mut state = self.state.lock().expect("mailbox poisoned");
        // Previously retained controller mail is admitted before new submissions.
        self.read_retained_mail(&mut state);
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
            || self
                .controller
                .as_ref()
                .is_some_and(|(_, _, updates)| *updates.borrow())
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
        let mut state = self.state.lock().expect("mailbox poisoned");
        let read = !state.eviction_fenced && self.read_retained_mail(&mut state);
        let pending = if state.eviction_fenced {
            BTreeMap::new()
        } else {
            std::mem::take(&mut state.pending)
        };
        let counted = !pending.is_empty();
        if counted {
            state.open_reservations += 1;
        }
        drop(state);
        if read {
            self.activity.send_replace(InputQueueActivity::Mailbox);
        }
        MailboxReservation {
            state: Arc::clone(&self.state),
            activity: self.activity.clone(),
            pending,
            counted,
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
        if other.counted && !self.counted {
            self.counted = true;
            other.counted = false;
        }
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
        let restored = !self.pending.is_empty();
        if !self.counted && !restored {
            return;
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.counted {
            state.open_reservations -= 1;
        }
        state.pending.append(&mut self.pending);
        drop(state);
        if restored {
            self.activity.send_replace(InputQueueActivity::Mailbox);
        }
    }
}

#[cfg(test)]
#[path = "mailbox_tests.rs"]
mod tests;
