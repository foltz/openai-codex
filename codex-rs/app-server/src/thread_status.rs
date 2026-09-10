use crate::managed_transition::AccountWorkPermitGuard;
#[cfg(test)]
use crate::outgoing_message::OutgoingEnvelope;
#[cfg(test)]
use crate::outgoing_message::OutgoingMessage;
use crate::outgoing_message::OutgoingMessageSender;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ThreadActiveFlag;
use codex_app_server_protocol::ThreadStatus;
use codex_app_server_protocol::ThreadStatusChangedNotification;
use codex_protocol::ThreadId;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::AtomicU64;
use tokio::sync::Mutex;
#[cfg(test)]
use tokio::sync::mpsc;
use tokio::sync::watch;

#[derive(Clone)]
pub(crate) struct ThreadWatchManager {
    state: Arc<Mutex<ThreadWatchState>>,
    outgoing: Option<Arc<OutgoingMessageSender>>,
    running_turn_count_tx: watch::Sender<usize>,
    /// Account-work permits transferred from an RPC handler into the turn it
    /// submitted.  `turn/start` only acknowledges submission; retaining the
    /// permit here keeps the managed-transition drain closed until the core
    /// turn emits a terminal event.
    account_work_permits: Arc<StdMutex<HashMap<String, AccountWorkPermitThreadState>>>,
    listener_generations: Arc<StdMutex<HashMap<String, u64>>>,
    next_account_work_permit_key: Arc<AtomicU64>,
}

struct AccountWorkPermitThreadState {
    entries: Vec<AccountWorkPermitEntry>,
    /// A terminal event can be delivered before the submission future binds
    /// its generated turn id. Keep those ids only while a pending handoff
    /// exists so the subsequent bind can release the right permit without
    /// letting a stale prior-turn event release a newer turn.
    terminal_turn_ids: HashSet<String>,
}

struct AccountWorkPermitEntry {
    key: u64,
    turn_id: Option<String>,
    permit: AccountWorkPermitGuard,
}

pub(crate) struct ThreadWatchActiveGuard {
    manager: ThreadWatchManager,
    thread_id: String,
    guard_type: ThreadWatchActiveGuardType,
    handle: tokio::runtime::Handle,
}

impl ThreadWatchActiveGuard {
    fn new(
        manager: ThreadWatchManager,
        thread_id: String,
        guard_type: ThreadWatchActiveGuardType,
    ) -> Self {
        Self {
            manager,
            thread_id,
            guard_type,
            handle: tokio::runtime::Handle::current(),
        }
    }
}

impl Drop for ThreadWatchActiveGuard {
    fn drop(&mut self) {
        let manager = self.manager.clone();
        let thread_id = self.thread_id.clone();
        let guard_type = self.guard_type;
        self.handle.spawn(async move {
            manager
                .note_active_guard_released(thread_id, guard_type)
                .await;
        });
    }
}

#[derive(Clone, Copy)]
enum ThreadWatchActiveGuardType {
    Permission,
    UserInput,
}

impl Default for ThreadWatchManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ThreadWatchManager {
    pub(crate) fn new() -> Self {
        let (running_turn_count_tx, _running_turn_count_rx) = watch::channel(0);
        Self {
            state: Arc::new(Mutex::new(ThreadWatchState::default())),
            outgoing: None,
            running_turn_count_tx,
            account_work_permits: Arc::new(StdMutex::new(HashMap::new())),
            listener_generations: Arc::new(StdMutex::new(HashMap::new())),
            next_account_work_permit_key: Arc::new(AtomicU64::new(1)),
        }
    }

    pub(crate) fn new_with_outgoing(outgoing: Arc<OutgoingMessageSender>) -> Self {
        let (running_turn_count_tx, _running_turn_count_rx) = watch::channel(0);
        Self {
            state: Arc::new(Mutex::new(ThreadWatchState::default())),
            outgoing: Some(outgoing),
            running_turn_count_tx,
            account_work_permits: Arc::new(StdMutex::new(HashMap::new())),
            listener_generations: Arc::new(StdMutex::new(HashMap::new())),
            next_account_work_permit_key: Arc::new(AtomicU64::new(1)),
        }
    }

    pub(crate) async fn upsert_thread(&self, thread_id: &str) {
        let thread_id = thread_id.to_string();
        self.mutate_and_publish(move |state| {
            state.upsert_thread(thread_id, /*emit_notification*/ true)
        })
        .await;
    }

    pub(crate) async fn upsert_thread_silently(&self, thread_id: &str) {
        let thread_id = thread_id.to_string();
        self.mutate_and_publish(move |state| {
            state.upsert_thread(thread_id, /*emit_notification*/ false)
        })
        .await;
    }

    pub(crate) async fn remove_thread(&self, thread_id: &str) {
        let thread_id = thread_id.to_string();
        self.mutate_and_publish(|state| state.remove_thread(&thread_id))
            .await;
        self.listener_generations
            .lock()
            .expect("listener generation registry lock poisoned")
            .remove(&thread_id);
        self.release_account_work_permits(&thread_id);
    }

    /// Record which listener task currently owns the thread-level fallback
    /// release. The caller holds the ThreadState lock while publishing this
    /// generation and signalling the predecessor, so the predecessor cannot
    /// run its post-loop guard until this registry update is visible.
    pub(crate) fn register_listener_generation(&self, thread_id: &str, generation: u64) {
        self.listener_generations
            .lock()
            .expect("listener generation registry lock poisoned")
            .insert(thread_id.to_string(), generation);
    }

    /// Release permits when the listener task that still owns this generation
    /// ends. A superseded listener must not release permits transferred to its
    /// replacement.
    pub(crate) fn release_account_work_permits_for_listener_generation(
        &self,
        thread_id: &str,
        generation: u64,
    ) {
        let owns_generation = {
            let mut generations = self
                .listener_generations
                .lock()
                .expect("listener generation registry lock poisoned");
            if generations.get(thread_id).copied() == Some(generation) {
                generations.remove(thread_id);
                true
            } else {
                false
            }
        };
        if !owns_generation {
            return;
        }
        self.release_account_work_permits(thread_id);
    }

    pub(crate) async fn loaded_status_for_thread(&self, thread_id: &str) -> ThreadStatus {
        self.state.lock().await.loaded_status_for_thread(thread_id)
    }

    pub(crate) async fn loaded_statuses_for_threads(
        &self,
        thread_ids: impl IntoIterator<Item = String>,
    ) -> HashMap<String, ThreadStatus> {
        let state = self.state.lock().await;
        thread_ids
            .into_iter()
            .filter_map(|thread_id| {
                state
                    .status_for(&thread_id)
                    .map(|status| (thread_id, status))
            })
            .collect()
    }

    #[cfg(test)]
    pub(crate) async fn running_turn_count(&self) -> usize {
        self.state
            .lock()
            .await
            .runtime_by_thread_id
            .values()
            .filter(|runtime| runtime.running)
            .count()
    }

    pub(crate) fn subscribe_running_turn_count(&self) -> watch::Receiver<usize> {
        self.running_turn_count_tx.subscribe()
    }

    pub(crate) async fn note_turn_started(&self, thread_id: &str) {
        self.update_runtime_for_thread(thread_id, |runtime| {
            runtime.is_loaded = true;
            runtime.running = true;
            runtime.has_system_error = false;
        })
        .await;
    }

    pub(crate) async fn note_turn_completed(&self, thread_id: &str, turn_id: &str, _failed: bool) {
        self.clear_active_state(thread_id, turn_id).await;
    }

    pub(crate) async fn note_turn_interrupted(&self, thread_id: &str, turn_id: &str) {
        self.clear_active_state(thread_id, turn_id).await;
    }

    /// Transfer a dispatch permit into the submitted core turn.  This must be
    /// called before submission so a very fast terminal event cannot race the
    /// handoff and release the permit before it is visible here.
    pub(crate) fn retain_account_work_permit(
        &self,
        thread_id: &str,
        permit: AccountWorkPermitGuard,
    ) -> u64 {
        let key = self
            .next_account_work_permit_key
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.account_work_permits
            .lock()
            .expect("account-work permit registry lock poisoned")
            .entry(thread_id.to_string())
            .or_insert_with(|| AccountWorkPermitThreadState {
                entries: Vec::new(),
                terminal_turn_ids: HashSet::new(),
            })
            .entries
            .push(AccountWorkPermitEntry {
                key,
                turn_id: None,
                permit,
            });
        key
    }

    pub(crate) fn bind_account_work_permit(&self, thread_id: &str, key: u64, turn_id: String) {
        let permit = {
            let mut permits = self
                .account_work_permits
                .lock()
                .expect("account-work permit registry lock poisoned");
            let Some(thread_permits) = permits.get_mut(thread_id) else {
                return;
            };
            let Some(index) = thread_permits
                .entries
                .iter()
                .position(|entry| entry.key == key)
            else {
                return;
            };
            if thread_permits.terminal_turn_ids.remove(&turn_id) {
                Some(thread_permits.entries.swap_remove(index).permit)
            } else {
                thread_permits.entries[index].turn_id = Some(turn_id);
                if thread_permits
                    .entries
                    .iter()
                    .all(|entry| entry.turn_id.is_some())
                {
                    thread_permits.terminal_turn_ids.clear();
                }
                None
            }
        };
        drop(permit);
        self.remove_empty_account_work_permit_state(thread_id);
    }

    /// Undo a handoff when submission itself fails. The key, rather than a
    /// thread-level "last" entry, prevents an unrelated terminal event or
    /// concurrent request from releasing this permit.
    pub(crate) fn release_pending_account_work_permit(&self, thread_id: &str, key: u64) {
        let permit = {
            let mut permits = self
                .account_work_permits
                .lock()
                .expect("account-work permit registry lock poisoned");
            let Some(thread_permits) = permits.get_mut(thread_id) else {
                return;
            };
            let Some(index) = thread_permits
                .entries
                .iter()
                .position(|entry| entry.key == key)
            else {
                return;
            };
            thread_permits.entries.swap_remove(index).permit
        };
        drop(permit);
        self.remove_empty_account_work_permit_state(thread_id);
    }

    pub(crate) async fn note_thread_shutdown(&self, thread_id: &str) {
        self.update_runtime_for_thread(thread_id, |runtime| {
            runtime.running = false;
            runtime.pending_permission_requests = 0;
            runtime.pending_user_input_requests = 0;
            runtime.is_loaded = false;
        })
        .await;
        self.release_account_work_permits(thread_id);
    }

    /// Release work transferred to a thread when its core event stream closes
    /// before a terminal event can be observed.  The listener owns this
    /// fallback because a dead session cannot emit `ShutdownComplete` or
    /// `TurnComplete` anymore.
    pub(crate) fn note_thread_event_stream_closed(&self, thread_id: &str) {
        self.release_account_work_permits(thread_id);
    }

    pub(crate) async fn note_system_error(&self, thread_id: &str, turn_id: &str) {
        self.update_runtime_for_thread(thread_id, |runtime| {
            runtime.running = false;
            runtime.pending_permission_requests = 0;
            runtime.pending_user_input_requests = 0;
            runtime.has_system_error = true;
        })
        .await;
        self.release_account_work_permit_for_turn(thread_id, turn_id);
    }

    async fn clear_active_state(&self, thread_id: &str, turn_id: &str) {
        self.update_runtime_for_thread(thread_id, move |runtime| {
            runtime.running = false;
            runtime.pending_permission_requests = 0;
            runtime.pending_user_input_requests = 0;
        })
        .await;
        self.release_account_work_permit_for_turn(thread_id, turn_id);
    }

    fn release_account_work_permit_for_turn(&self, thread_id: &str, turn_id: &str) {
        let permit = {
            let mut permits = self
                .account_work_permits
                .lock()
                .expect("account-work permit registry lock poisoned");
            let Some(thread_permits) = permits.get_mut(thread_id) else {
                return;
            };
            let Some(index) = thread_permits
                .entries
                .iter()
                .position(|entry| entry.turn_id.as_deref() == Some(turn_id))
            else {
                if thread_permits
                    .entries
                    .iter()
                    .any(|entry| entry.turn_id.is_none())
                {
                    thread_permits.terminal_turn_ids.insert(turn_id.to_string());
                }
                return;
            };
            thread_permits.entries.swap_remove(index).permit
        };
        drop(permit);
        self.remove_empty_account_work_permit_state(thread_id);
    }

    fn release_account_work_permits(&self, thread_id: &str) {
        // Drop the guards after releasing the mutex; their Drop notifies the
        // transition drain and must never run while this bookkeeping lock is
        // held.
        let permits = self
            .account_work_permits
            .lock()
            .expect("account-work permit registry lock poisoned")
            .remove(thread_id);
        drop(permits);
    }

    fn remove_empty_account_work_permit_state(&self, thread_id: &str) {
        let mut permits = self
            .account_work_permits
            .lock()
            .expect("account-work permit registry lock poisoned");
        if permits
            .get(thread_id)
            .is_some_and(|thread_permits| thread_permits.entries.is_empty())
        {
            permits.remove(thread_id);
        }
    }

    pub(crate) async fn note_permission_requested(
        &self,
        thread_id: &str,
    ) -> ThreadWatchActiveGuard {
        self.note_pending_request(thread_id, ThreadWatchActiveGuardType::Permission)
            .await
    }

    pub(crate) async fn note_user_input_requested(
        &self,
        thread_id: &str,
    ) -> ThreadWatchActiveGuard {
        self.note_pending_request(thread_id, ThreadWatchActiveGuardType::UserInput)
            .await
    }

    async fn note_pending_request(
        &self,
        thread_id: &str,
        guard_type: ThreadWatchActiveGuardType,
    ) -> ThreadWatchActiveGuard {
        self.update_runtime_for_thread(thread_id, move |runtime| {
            runtime.is_loaded = true;
            let counter = Self::pending_counter(runtime, guard_type);
            *counter = counter.saturating_add(1);
        })
        .await;
        ThreadWatchActiveGuard::new(self.clone(), thread_id.to_string(), guard_type)
    }

    async fn mutate_and_publish<F>(&self, mutate: F)
    where
        F: FnOnce(&mut ThreadWatchState) -> Option<ThreadStatusChangedNotification>,
    {
        let (notification, running_turn_count) = {
            let mut state = self.state.lock().await;
            let notification = mutate(&mut state);
            let running_turn_count = state
                .runtime_by_thread_id
                .values()
                .filter(|runtime| runtime.running)
                .count();
            (notification, running_turn_count)
        };
        let _ = self.running_turn_count_tx.send(running_turn_count);

        if let Some(notification) = notification
            && let Some(outgoing) = &self.outgoing
        {
            outgoing
                .send_server_notification(ServerNotification::ThreadStatusChanged(notification))
                .await;
        }
    }

    pub(crate) async fn subscribe(
        &self,
        thread_id: ThreadId,
    ) -> Option<watch::Receiver<ThreadStatus>> {
        Some(self.state.lock().await.subscribe(thread_id.to_string()))
    }

    async fn note_active_guard_released(
        &self,
        thread_id: String,
        guard_type: ThreadWatchActiveGuardType,
    ) {
        self.update_runtime_for_thread(&thread_id, move |runtime| {
            let counter = Self::pending_counter(runtime, guard_type);
            *counter = counter.saturating_sub(1);
        })
        .await;
    }

    async fn update_runtime_for_thread<F>(&self, thread_id: &str, update: F)
    where
        F: FnOnce(&mut RuntimeFacts),
    {
        let thread_id = thread_id.to_string();
        self.mutate_and_publish(move |state| state.update_runtime(&thread_id, update))
            .await;
    }

    fn pending_counter(
        runtime: &mut RuntimeFacts,
        guard_type: ThreadWatchActiveGuardType,
    ) -> &mut u32 {
        match guard_type {
            ThreadWatchActiveGuardType::Permission => &mut runtime.pending_permission_requests,
            ThreadWatchActiveGuardType::UserInput => &mut runtime.pending_user_input_requests,
        }
    }
}

pub(crate) fn resolve_thread_status(
    status: ThreadStatus,
    has_in_progress_turn: bool,
) -> ThreadStatus {
    // Running-turn events can arrive before the watch runtime state is observed by
    // the listener loop. In that window we prefer to reflect a real active turn as
    // `Active` instead of `Idle`/`NotLoaded`.
    if has_in_progress_turn && matches!(status, ThreadStatus::Idle | ThreadStatus::NotLoaded) {
        return ThreadStatus::Active {
            active_flags: Vec::new(),
        };
    }

    status
}

#[derive(Default)]
struct ThreadWatchState {
    runtime_by_thread_id: HashMap<String, RuntimeFacts>,
    status_watcher_by_thread_id: HashMap<String, watch::Sender<ThreadStatus>>,
}

impl ThreadWatchState {
    fn upsert_thread(
        &mut self,
        thread_id: String,
        emit_notification: bool,
    ) -> Option<ThreadStatusChangedNotification> {
        let previous_status = self.status_for(&thread_id);
        let runtime = self
            .runtime_by_thread_id
            .entry(thread_id.clone())
            .or_default();
        runtime.is_loaded = true;
        self.update_status_watcher_for_thread(&thread_id);
        if emit_notification {
            self.status_changed_notification(thread_id, previous_status)
        } else {
            None
        }
    }

    fn remove_thread(&mut self, thread_id: &str) -> Option<ThreadStatusChangedNotification> {
        let previous_status = self.status_for(thread_id);
        self.runtime_by_thread_id.remove(thread_id);
        self.update_status_watcher(thread_id, &ThreadStatus::NotLoaded);
        if previous_status.is_some() && previous_status != Some(ThreadStatus::NotLoaded) {
            Some(ThreadStatusChangedNotification {
                thread_id: thread_id.to_string(),
                status: ThreadStatus::NotLoaded,
            })
        } else {
            None
        }
    }

    fn update_runtime<F>(
        &mut self,
        thread_id: &str,
        mutate: F,
    ) -> Option<ThreadStatusChangedNotification>
    where
        F: FnOnce(&mut RuntimeFacts),
    {
        let previous_status = self.status_for(thread_id);
        let runtime = self
            .runtime_by_thread_id
            .entry(thread_id.to_string())
            .or_default();
        runtime.is_loaded = true;
        mutate(runtime);
        self.update_status_watcher_for_thread(thread_id);
        self.status_changed_notification(thread_id.to_string(), previous_status)
    }

    fn status_for(&self, thread_id: &str) -> Option<ThreadStatus> {
        self.runtime_by_thread_id
            .get(thread_id)
            .map(loaded_thread_status)
    }

    fn loaded_status_for_thread(&self, thread_id: &str) -> ThreadStatus {
        self.status_for(thread_id)
            .unwrap_or(ThreadStatus::NotLoaded)
    }

    fn subscribe(&mut self, thread_id: String) -> watch::Receiver<ThreadStatus> {
        let status = self.loaded_status_for_thread(&thread_id);
        let sender = self
            .status_watcher_by_thread_id
            .entry(thread_id)
            .or_insert_with(|| watch::channel(status.clone()).0);
        sender.subscribe()
    }

    fn update_status_watcher_for_thread(&mut self, thread_id: &str) {
        let status = self.loaded_status_for_thread(thread_id);
        self.update_status_watcher(thread_id, &status);
    }

    fn update_status_watcher(&mut self, thread_id: &str, status: &ThreadStatus) {
        let remove_watcher = if let Some(sender) = self.status_watcher_by_thread_id.get(thread_id) {
            let status = status.clone();
            let _ = sender.send_if_modified(|current| {
                if *current == status {
                    false
                } else {
                    *current = status;
                    true
                }
            });
            sender.receiver_count() == 0
        } else {
            false
        };
        if remove_watcher {
            self.status_watcher_by_thread_id.remove(thread_id);
        }
    }

    fn status_changed_notification(
        &self,
        thread_id: String,
        previous_status: Option<ThreadStatus>,
    ) -> Option<ThreadStatusChangedNotification> {
        let status = self.status_for(&thread_id)?;

        if previous_status.as_ref() == Some(&status) {
            return None;
        }

        Some(ThreadStatusChangedNotification { thread_id, status })
    }
}

#[derive(Clone, Default)]
struct RuntimeFacts {
    is_loaded: bool,
    running: bool,
    pending_permission_requests: u32,
    pending_user_input_requests: u32,
    has_system_error: bool,
}

fn loaded_thread_status(runtime: &RuntimeFacts) -> ThreadStatus {
    if !runtime.is_loaded {
        return ThreadStatus::NotLoaded;
    }

    let mut active_flags = Vec::new();
    if runtime.pending_permission_requests > 0 {
        active_flags.push(ThreadActiveFlag::WaitingOnApproval);
    }
    if runtime.pending_user_input_requests > 0 {
        active_flags.push(ThreadActiveFlag::WaitingOnUserInput);
    }

    if runtime.running || !active_flags.is_empty() {
        return ThreadStatus::Active { active_flags };
    }

    if runtime.has_system_error {
        return ThreadStatus::SystemError;
    }

    ThreadStatus::Idle
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use tokio::time::Duration;
    use tokio::time::timeout;

    const INTERACTIVE_THREAD_ID: &str = "00000000-0000-0000-0000-000000000001";
    const NON_INTERACTIVE_THREAD_ID: &str = "00000000-0000-0000-0000-000000000002";

    #[tokio::test]
    async fn loaded_status_defaults_to_not_loaded_for_untracked_threads() {
        let manager = ThreadWatchManager::new();

        assert_eq!(
            manager
                .loaded_status_for_thread("00000000-0000-0000-0000-000000000003")
                .await,
            ThreadStatus::NotLoaded,
        );
    }

    #[tokio::test]
    async fn tracks_non_interactive_thread_status() {
        let manager = ThreadWatchManager::new();
        manager.upsert_thread(NON_INTERACTIVE_THREAD_ID).await;

        manager.note_turn_started(NON_INTERACTIVE_THREAD_ID).await;

        assert_eq!(
            manager
                .loaded_status_for_thread(NON_INTERACTIVE_THREAD_ID)
                .await,
            ThreadStatus::Active {
                active_flags: vec![],
            },
        );
    }

    #[tokio::test]
    async fn status_updates_track_single_thread() {
        let manager = ThreadWatchManager::new();
        manager.upsert_thread(INTERACTIVE_THREAD_ID).await;

        manager.note_turn_started(INTERACTIVE_THREAD_ID).await;
        assert_eq!(
            manager
                .loaded_status_for_thread(INTERACTIVE_THREAD_ID)
                .await,
            ThreadStatus::Active {
                active_flags: vec![],
            },
        );

        let permission_guard = manager
            .note_permission_requested(INTERACTIVE_THREAD_ID)
            .await;
        assert_eq!(
            manager
                .loaded_status_for_thread(INTERACTIVE_THREAD_ID)
                .await,
            ThreadStatus::Active {
                active_flags: vec![ThreadActiveFlag::WaitingOnApproval],
            },
        );

        let user_input_guard = manager
            .note_user_input_requested(INTERACTIVE_THREAD_ID)
            .await;
        assert_eq!(
            manager
                .loaded_status_for_thread(INTERACTIVE_THREAD_ID)
                .await,
            ThreadStatus::Active {
                active_flags: vec![
                    ThreadActiveFlag::WaitingOnApproval,
                    ThreadActiveFlag::WaitingOnUserInput,
                ],
            },
        );

        drop(permission_guard);
        wait_for_status(
            &manager,
            INTERACTIVE_THREAD_ID,
            ThreadStatus::Active {
                active_flags: vec![ThreadActiveFlag::WaitingOnUserInput],
            },
        )
        .await;

        drop(user_input_guard);
        wait_for_status(
            &manager,
            INTERACTIVE_THREAD_ID,
            ThreadStatus::Active {
                active_flags: vec![],
            },
        )
        .await;

        manager
            .note_turn_completed(INTERACTIVE_THREAD_ID, "turn-1", false)
            .await;
        assert_eq!(
            manager
                .loaded_status_for_thread(INTERACTIVE_THREAD_ID)
                .await,
            ThreadStatus::Idle,
        );
    }

    #[test]
    fn resolves_in_progress_turn_to_active_status() {
        let status = resolve_thread_status(ThreadStatus::Idle, /*has_in_progress_turn*/ true);
        assert_eq!(
            status,
            ThreadStatus::Active {
                active_flags: Vec::new(),
            }
        );

        let status =
            resolve_thread_status(ThreadStatus::NotLoaded, /*has_in_progress_turn*/ true);
        assert_eq!(
            status,
            ThreadStatus::Active {
                active_flags: Vec::new(),
            }
        );
    }

    #[test]
    fn keeps_status_when_no_in_progress_turn() {
        assert_eq!(
            resolve_thread_status(ThreadStatus::Idle, /*has_in_progress_turn*/ false),
            ThreadStatus::Idle
        );
        assert_eq!(
            resolve_thread_status(
                ThreadStatus::SystemError,
                /*has_in_progress_turn*/ false
            ),
            ThreadStatus::SystemError
        );
    }

    #[tokio::test]
    async fn system_error_sets_idle_flag_until_next_turn() {
        let manager = ThreadWatchManager::new();
        manager.upsert_thread(INTERACTIVE_THREAD_ID).await;

        manager.note_turn_started(INTERACTIVE_THREAD_ID).await;
        manager
            .note_system_error(INTERACTIVE_THREAD_ID, "turn-1")
            .await;

        assert_eq!(
            manager
                .loaded_status_for_thread(INTERACTIVE_THREAD_ID)
                .await,
            ThreadStatus::SystemError,
        );

        manager.note_turn_started(INTERACTIVE_THREAD_ID).await;
        assert_eq!(
            manager
                .loaded_status_for_thread(INTERACTIVE_THREAD_ID)
                .await,
            ThreadStatus::Active {
                active_flags: vec![],
            },
        );
    }

    #[tokio::test]
    async fn shutdown_marks_thread_not_loaded() {
        let manager = ThreadWatchManager::new();
        manager.upsert_thread(INTERACTIVE_THREAD_ID).await;

        manager.note_turn_started(INTERACTIVE_THREAD_ID).await;
        manager.note_thread_shutdown(INTERACTIVE_THREAD_ID).await;

        assert_eq!(
            manager
                .loaded_status_for_thread(INTERACTIVE_THREAD_ID)
                .await,
            ThreadStatus::NotLoaded,
        );
    }

    #[tokio::test]
    async fn loaded_statuses_distinguish_shutdown_from_untracked_threads() {
        const UNTRACKED_THREAD_ID: &str = "00000000-0000-0000-0000-000000000003";

        let manager = ThreadWatchManager::new();
        manager.upsert_thread(INTERACTIVE_THREAD_ID).await;
        manager.note_turn_started(INTERACTIVE_THREAD_ID).await;
        manager.upsert_thread(NON_INTERACTIVE_THREAD_ID).await;
        manager
            .note_thread_shutdown(NON_INTERACTIVE_THREAD_ID)
            .await;

        let statuses = manager
            .loaded_statuses_for_threads(vec![
                INTERACTIVE_THREAD_ID.to_string(),
                NON_INTERACTIVE_THREAD_ID.to_string(),
                UNTRACKED_THREAD_ID.to_string(),
            ])
            .await;

        assert_eq!(
            statuses.get(INTERACTIVE_THREAD_ID),
            Some(&ThreadStatus::Active {
                active_flags: vec![],
            }),
        );
        assert_eq!(
            statuses.get(NON_INTERACTIVE_THREAD_ID),
            Some(&ThreadStatus::NotLoaded),
        );
        assert_eq!(statuses.get(UNTRACKED_THREAD_ID), None);
    }

    #[tokio::test]
    async fn has_running_turns_tracks_runtime_running_flag_only() {
        let manager = ThreadWatchManager::new();
        manager.upsert_thread(INTERACTIVE_THREAD_ID).await;

        assert_eq!(manager.running_turn_count().await, 0);

        let _permission_guard = manager
            .note_permission_requested(INTERACTIVE_THREAD_ID)
            .await;
        assert_eq!(manager.running_turn_count().await, 0);

        manager.note_turn_started(INTERACTIVE_THREAD_ID).await;
        assert_eq!(manager.running_turn_count().await, 1);

        manager
            .note_turn_completed(INTERACTIVE_THREAD_ID, "turn-1", false)
            .await;
        assert_eq!(manager.running_turn_count().await, 0);
    }

    #[tokio::test]
    async fn transferred_account_work_permit_lives_until_turn_terminal_event() {
        let coordinator = crate::managed_transition::ManagedTransitionCoordinator::new();
        let permit = coordinator
            .try_acquire_account_work_permit()
            .expect("the barrier starts open");
        let manager = ThreadWatchManager::new();
        let key = manager.retain_account_work_permit(INTERACTIVE_THREAD_ID, permit);
        manager.bind_account_work_permit(INTERACTIVE_THREAD_ID, key, "turn-1".to_string());

        assert_eq!(
            coordinator.account_work_permits().admitted_count(),
            1,
            "submission acknowledgement must not release the transferred permit"
        );
        manager
            .note_turn_completed(INTERACTIVE_THREAD_ID, "turn-1", false)
            .await;
        assert_eq!(
            coordinator.account_work_permits().admitted_count(),
            0,
            "the terminal turn event releases the account-work permit"
        );
    }

    #[tokio::test]
    async fn event_stream_closure_releases_transferred_account_work_permit() {
        let coordinator = crate::managed_transition::ManagedTransitionCoordinator::new();
        let permit = coordinator
            .try_acquire_account_work_permit()
            .expect("the barrier starts open");
        let manager = ThreadWatchManager::new();
        manager.retain_account_work_permit(INTERACTIVE_THREAD_ID, permit);

        manager.note_thread_event_stream_closed(INTERACTIVE_THREAD_ID);

        assert_eq!(
            coordinator.account_work_permits().admitted_count(),
            0,
            "a dead core event stream must not strand the account-work permit"
        );
    }

    #[tokio::test]
    async fn superseded_listener_does_not_release_replacement_permit() {
        let coordinator = crate::managed_transition::ManagedTransitionCoordinator::new();
        let first_permit = coordinator
            .try_acquire_account_work_permit()
            .expect("the barrier starts open");
        let second_permit = coordinator
            .try_acquire_account_work_permit()
            .expect("the barrier admits the replacement turn");
        let manager = ThreadWatchManager::new();
        manager.retain_account_work_permit(INTERACTIVE_THREAD_ID, first_permit);
        manager.retain_account_work_permit(INTERACTIVE_THREAD_ID, second_permit);

        manager.register_listener_generation(INTERACTIVE_THREAD_ID, 1);
        manager.register_listener_generation(INTERACTIVE_THREAD_ID, 2);
        manager.release_account_work_permits_for_listener_generation(INTERACTIVE_THREAD_ID, 1);
        assert_eq!(
            coordinator.account_work_permits().admitted_count(),
            2,
            "a superseded listener must leave replacement-owned permits intact"
        );

        manager.release_account_work_permits_for_listener_generation(INTERACTIVE_THREAD_ID, 2);
        assert_eq!(coordinator.account_work_permits().admitted_count(), 0);
    }

    #[tokio::test]
    async fn stale_terminal_event_does_not_release_a_newer_turn_permit() {
        let coordinator = crate::managed_transition::ManagedTransitionCoordinator::new();
        let first_permit = coordinator
            .try_acquire_account_work_permit()
            .expect("the barrier starts open");
        let second_permit = coordinator
            .try_acquire_account_work_permit()
            .expect("the barrier admits the second live turn");
        let manager = ThreadWatchManager::new();
        let first_key = manager.retain_account_work_permit(INTERACTIVE_THREAD_ID, first_permit);
        let second_key = manager.retain_account_work_permit(INTERACTIVE_THREAD_ID, second_permit);
        manager.bind_account_work_permit(INTERACTIVE_THREAD_ID, first_key, "turn-1".to_string());
        manager.bind_account_work_permit(INTERACTIVE_THREAD_ID, second_key, "turn-2".to_string());

        manager
            .note_turn_completed(INTERACTIVE_THREAD_ID, "turn-1", false)
            .await;
        assert_eq!(
            coordinator.account_work_permits().admitted_count(),
            1,
            "a prior turn's terminal event must not release the newer turn"
        );
        manager
            .note_turn_completed(INTERACTIVE_THREAD_ID, "turn-2", false)
            .await;
        assert_eq!(coordinator.account_work_permits().admitted_count(), 0);
    }

    #[tokio::test]
    async fn unrelated_system_error_does_not_release_turn_permit() {
        let coordinator = crate::managed_transition::ManagedTransitionCoordinator::new();
        let permit = coordinator
            .try_acquire_account_work_permit()
            .expect("the barrier starts open");
        let manager = ThreadWatchManager::new();
        let key = manager.retain_account_work_permit(INTERACTIVE_THREAD_ID, permit);
        manager.bind_account_work_permit(INTERACTIVE_THREAD_ID, key, "turn-1".to_string());

        manager
            .note_system_error(INTERACTIVE_THREAD_ID, "other-turn")
            .await;
        assert_eq!(
            coordinator.account_work_permits().admitted_count(),
            1,
            "an unrelated error event must not release the active turn"
        );
        manager
            .note_system_error(INTERACTIVE_THREAD_ID, "turn-1")
            .await;
        assert_eq!(coordinator.account_work_permits().admitted_count(), 0);
    }

    #[tokio::test]
    async fn status_change_emits_notification() {
        let (outgoing_tx, mut outgoing_rx) = mpsc::channel(8);
        let manager = ThreadWatchManager::new_with_outgoing(Arc::new(OutgoingMessageSender::new(
            outgoing_tx,
            codex_analytics::AnalyticsEventsClient::disabled(),
        )));

        manager.upsert_thread(INTERACTIVE_THREAD_ID).await;
        assert_eq!(
            recv_status_changed_notification(&mut outgoing_rx).await,
            ThreadStatusChangedNotification {
                thread_id: INTERACTIVE_THREAD_ID.to_string(),
                status: ThreadStatus::Idle,
            },
        );

        manager.note_turn_started(INTERACTIVE_THREAD_ID).await;
        assert_eq!(
            recv_status_changed_notification(&mut outgoing_rx).await,
            ThreadStatusChangedNotification {
                thread_id: INTERACTIVE_THREAD_ID.to_string(),
                status: ThreadStatus::Active {
                    active_flags: vec![],
                },
            },
        );

        manager.remove_thread(INTERACTIVE_THREAD_ID).await;
        assert_eq!(
            recv_status_changed_notification(&mut outgoing_rx).await,
            ThreadStatusChangedNotification {
                thread_id: INTERACTIVE_THREAD_ID.to_string(),
                status: ThreadStatus::NotLoaded,
            },
        );
    }

    #[tokio::test]
    async fn silent_upsert_skips_initial_notification() {
        let (outgoing_tx, mut outgoing_rx) = mpsc::channel(8);
        let manager = ThreadWatchManager::new_with_outgoing(Arc::new(OutgoingMessageSender::new(
            outgoing_tx,
            codex_analytics::AnalyticsEventsClient::disabled(),
        )));

        manager.upsert_thread_silently(INTERACTIVE_THREAD_ID).await;

        assert_eq!(
            manager
                .loaded_status_for_thread(INTERACTIVE_THREAD_ID)
                .await,
            ThreadStatus::Idle,
        );
        assert!(
            timeout(Duration::from_millis(100), outgoing_rx.recv())
                .await
                .is_err(),
            "silent upsert should not emit thread/status/changed"
        );

        manager.note_turn_started(INTERACTIVE_THREAD_ID).await;
        assert_eq!(
            recv_status_changed_notification(&mut outgoing_rx).await,
            ThreadStatusChangedNotification {
                thread_id: INTERACTIVE_THREAD_ID.to_string(),
                status: ThreadStatus::Active {
                    active_flags: vec![],
                },
            },
        );
    }

    #[tokio::test]
    async fn status_watchers_receive_only_their_thread_updates() {
        let manager = ThreadWatchManager::new();
        manager.upsert_thread(INTERACTIVE_THREAD_ID).await;
        manager.upsert_thread(NON_INTERACTIVE_THREAD_ID).await;
        let interactive_thread_id = ThreadId::from_string(INTERACTIVE_THREAD_ID)
            .expect("interactive thread id should parse");
        let non_interactive_thread_id = ThreadId::from_string(NON_INTERACTIVE_THREAD_ID)
            .expect("non-interactive thread id should parse");
        let mut interactive_rx = manager
            .subscribe(interactive_thread_id)
            .await
            .expect("interactive status watcher should subscribe");
        let mut non_interactive_rx = manager
            .subscribe(non_interactive_thread_id)
            .await
            .expect("non-interactive status watcher should subscribe");

        manager.note_turn_started(INTERACTIVE_THREAD_ID).await;

        timeout(Duration::from_secs(1), interactive_rx.changed())
            .await
            .expect("timed out waiting for interactive status update")
            .expect("interactive status watcher should remain open");
        assert_eq!(
            *interactive_rx.borrow(),
            ThreadStatus::Active {
                active_flags: vec![],
            },
        );
        assert!(
            timeout(Duration::from_millis(100), non_interactive_rx.changed())
                .await
                .is_err(),
            "unrelated thread watcher should not receive an update"
        );
        assert_eq!(*non_interactive_rx.borrow(), ThreadStatus::Idle);
    }

    async fn wait_for_status(
        manager: &ThreadWatchManager,
        thread_id: &str,
        expected_status: ThreadStatus,
    ) {
        timeout(Duration::from_secs(1), async {
            loop {
                let status = manager.loaded_status_for_thread(thread_id).await;
                if status == expected_status {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timed out waiting for status");
    }

    async fn recv_status_changed_notification(
        outgoing_rx: &mut mpsc::Receiver<OutgoingEnvelope>,
    ) -> ThreadStatusChangedNotification {
        let envelope = timeout(Duration::from_secs(1), outgoing_rx.recv())
            .await
            .expect("timed out waiting for outgoing notification")
            .expect("outgoing channel closed unexpectedly");
        let OutgoingEnvelope::Broadcast { message } = envelope else {
            panic!("expected broadcast notification");
        };
        let OutgoingMessage::AppServerNotification(envelope) = message else {
            panic!("expected thread/status/changed notification");
        };
        let ServerNotification::ThreadStatusChanged(notification) = envelope.notification else {
            panic!("expected thread/status/changed notification");
        };
        notification
    }
}
