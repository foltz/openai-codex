//! Synchronous resources retained by the host's cancellable preparation future.

use std::any::Any;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;

struct State {
    open: bool,
    resources: Vec<Box<dyn Any + Send + Sync>>,
}

/// The host owns this value until committed publication finishes or preparation
/// is dropped. It is intentionally not cloneable and is never stored in a turn.
pub struct TurnPreparationOwner {
    turn_id: Arc<str>,
    state: Arc<Mutex<State>>,
}

/// Weak access cannot extend preparation lifetime when a turn store is retained.
#[derive(Clone)]
pub struct TurnPreparationAccess {
    turn_id: Arc<str>,
    state: Weak<Mutex<State>>,
}

impl TurnPreparationOwner {
    pub fn new(turn_id: &str) -> Self {
        Self {
            turn_id: Arc::from(turn_id),
            state: Arc::new(Mutex::new(State {
                open: true,
                resources: Vec::new(),
            })),
        }
    }

    pub fn access(&self) -> TurnPreparationAccess {
        TurnPreparationAccess {
            turn_id: Arc::clone(&self.turn_id),
            state: Arc::downgrade(&self.state),
        }
    }
}

impl TurnPreparationAccess {
    /// Retain a resource only for this exact live preparation. Refusal returns
    /// ownership, without dropping a resource while the registry lock is held.
    pub fn retain<T: Any + Send + Sync>(&self, turn_id: &str, resource: T) -> Result<(), T> {
        if self.turn_id.as_ref() != turn_id {
            return Err(resource);
        }
        let Some(state) = self.state.upgrade() else {
            return Err(resource);
        };
        let Ok(mut state) = state.lock() else {
            return Err(resource);
        };
        if !state.open {
            return Err(resource);
        }
        state.resources.push(Box::new(resource));
        Ok(())
    }
}

impl Drop for TurnPreparationOwner {
    fn drop(&mut self) {
        let resources = {
            // Poison cannot grant another admission. Still release resources
            // outside the lock rather than strand an already-retained permit.
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.open = false;
            std::mem::take(&mut state.resources)
        };
        drop(resources);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    struct Resource(Arc<AtomicUsize>);
    impl Drop for Resource {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn retained_access_cannot_keep_resources_alive_or_accept_after_owner_drop() {
        let owner = TurnPreparationOwner::new("exact");
        let access = owner.access();
        let dropped = Arc::new(AtomicUsize::new(0));
        assert!(
            access
                .retain("exact", Resource(Arc::clone(&dropped)))
                .is_ok()
        );
        assert_eq!(0, dropped.load(Ordering::SeqCst));
        drop(owner);
        assert_eq!(1, dropped.load(Ordering::SeqCst));
        let returned = access.retain("exact", Resource(Arc::clone(&dropped)));
        assert!(returned.is_err());
        assert_eq!(1, dropped.load(Ordering::SeqCst));
        drop(returned);
        assert_eq!(2, dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn wrong_turn_and_poisoned_registry_refuse_without_consuming_resource() {
        let owner = TurnPreparationOwner::new("exact");
        let access = owner.access();
        let dropped = Arc::new(AtomicUsize::new(0));
        let returned = access.retain("other", Resource(Arc::clone(&dropped)));
        assert!(returned.is_err());
        assert_eq!(0, dropped.load(Ordering::SeqCst));
        drop(returned);
        let _ = std::panic::catch_unwind(|| {
            let _guard = owner.state.lock().unwrap();
            panic!("poison witness");
        });
        let returned = access.retain("exact", Resource(Arc::clone(&dropped)));
        assert!(returned.is_err());
        assert_eq!(1, dropped.load(Ordering::SeqCst));
        drop(returned);
        drop(owner);
        assert_eq!(2, dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn destructor_runs_outside_registry_lock() {
        struct Locks(Weak<Mutex<State>>);
        impl Drop for Locks {
            fn drop(&mut self) {
                let state = self.0.upgrade().unwrap();
                assert!(!state.try_lock().unwrap().open);
            }
        }
        let owner = TurnPreparationOwner::new("exact");
        assert!(
            owner
                .access()
                .retain("exact", Locks(Arc::downgrade(&owner.state)))
                .is_ok()
        );
        drop(owner);
    }
}
