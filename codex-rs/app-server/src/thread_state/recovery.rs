//! Owner-aware pending transitions. Dropping a resume marks its reservations inactive synchronously.
use codex_protocol::ThreadId;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use uuid::Uuid;

tokio::task_local! { static OWNER: ResumeOwner; }
struct ResumeOwner {
    id: Uuid,
    reservations: Mutex<Vec<Arc<AtomicBool>>>,
}
impl Drop for ResumeOwner {
    fn drop(&mut self) {
        for active in self
            .reservations
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            active.store(false, Ordering::Release);
        }
    }
}

pub(crate) async fn resume_scope<T>(future: impl Future<Output = T>) -> T {
    OWNER
        .scope(
            ResumeOwner {
                id: Uuid::new_v4(),
                reservations: Mutex::new(Vec::new()),
            },
            future,
        )
        .await
}

#[derive(Default)]
pub(crate) struct PendingThreadUnloads {
    entries: HashMap<ThreadId, (Option<Uuid>, Arc<AtomicBool>)>,
}
impl PendingThreadUnloads {
    pub(crate) fn contains(&self, id: &ThreadId) -> bool {
        self.entries
            .get(id)
            .is_some_and(|(_, active)| active.load(Ordering::Acquire))
    }
    pub(crate) fn blocks(&self, id: &ThreadId) -> bool {
        self.entries.get(id).is_some_and(|(owner, active)| {
            active.load(Ordering::Acquire)
                && (owner.is_none()
                    || OWNER
                        .try_with(|current| Some(current.id) != *owner)
                        .unwrap_or(true))
        })
    }
    pub(crate) fn insert(&mut self, id: ThreadId) {
        self.entries
            .retain(|_, (_, active)| active.load(Ordering::Acquire));
        self.entries
            .insert(id, (None, Arc::new(AtomicBool::new(true))));
    }
    pub(crate) fn remove(&mut self, id: &ThreadId) {
        // Ordinary teardown cannot release another operation's owned reservation.
        if self
            .entries
            .get(id)
            .is_some_and(|(owner, active)| owner.is_none() || !active.load(Ordering::Acquire))
        {
            self.entries.remove(id);
        }
    }
    /// A benign refused claim may return to ordinary listener-driven rejoin.
    /// Only its owner can release this reservation; no retirement was committed.
    pub(crate) fn release_owned(&mut self, id: &ThreadId) -> bool {
        let Some((Some(owner), active)) = self.entries.get(id) else {
            return false;
        };
        if !OWNER
            .try_with(|current| current.id == *owner)
            .unwrap_or(false)
        {
            return false;
        }
        active.store(false, Ordering::Release);
        self.entries.remove(id);
        true
    }

    pub(crate) fn reserve(&mut self, id: ThreadId) -> Result<(), &'static str> {
        if self.blocks(&id) {
            return Err("thread is closing; retry after the transition completes");
        }
        if self.contains(&id) {
            return Ok(());
        }
        let active = Arc::new(AtomicBool::new(true));
        let owner = OWNER
            .try_with(|current| {
                current
                    .reservations
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(Arc::clone(&active));
                current.id
            })
            .map_err(|_| "resume transition has no owner")?;
        self.entries
            .retain(|_, (_, active)| active.load(Ordering::Acquire));
        self.entries.insert(id, (Some(owner), active));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn recovery_owner_admits_only_itself_and_releases_on_cancellation() {
        let table = Arc::new(tokio::sync::Mutex::new(PendingThreadUnloads::default()));
        let id = ThreadId::new();
        let (reserved, reservation) = tokio::sync::oneshot::channel();
        let owned = Arc::clone(&table);
        let task = tokio::spawn(resume_scope(async move {
            {
                let mut table = owned.lock().await;
                table.reserve(id).unwrap();
                assert!(table.contains(&id));
                assert!(!table.blocks(&id));
                table.remove(&id);
                assert!(table.contains(&id), "teardown cannot release the owner");
                reserved.send(()).unwrap();
            }
            std::future::pending::<()>().await;
        }));
        reservation.await.unwrap();
        assert!(table.lock().await.blocks(&id));
        resume_scope(async {
            assert!(table.lock().await.reserve(id).is_err());
        })
        .await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(!table.lock().await.contains(&id));
        resume_scope(async {
            table.lock().await.reserve(id).unwrap();
        })
        .await;
        assert!(!table.lock().await.blocks(&id));
    }
    #[tokio::test]
    async fn recovery_idle_claim_blocks_even_the_resume_owner() {
        let id = ThreadId::new();
        let mut table = PendingThreadUnloads::default();
        table.insert(id);
        resume_scope(async {
            assert!(table.reserve(id).is_err());
        })
        .await;
        table.remove(&id);
        assert!(!table.contains(&id));
    }
}
