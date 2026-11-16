//! Parent identity captured at a tool boundary, never reconstructed from IDs.

use crate::CodexThread;
use crate::session::session::Session;
use codex_extension_api::HostOperationWork;
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
        Self {
            session: Arc::downgrade(session),
            turn_id: turn_id.to_owned(),
        }
    }

    /// Capture finite constructor work while the exact parent turn is live.
    /// The returned owner can cross retained polling and task boundaries;
    /// this weak provenance itself remains uncounted.
    pub(crate) fn capture_construction(
        authority: Option<&Self>,
    ) -> Result<Option<crate::thread_manager::CapturedConstructionWork>, TurnWorkRefused> {
        authority
            .map(|authority| {
                authority
                    .derive_operation()
                    .map(crate::thread_manager::CapturedConstructionWork)
            })
            .transpose()
    }

    pub(crate) fn derive_operation(
        &self,
    ) -> Result<Option<Box<dyn HostOperationWork>>, TurnWorkRefused> {
        let parent = self.session.upgrade().ok_or(TurnWorkRefused::Unavailable)?;
        let Some(host) = &parent.services.host_admission else {
            return Ok(None);
        };
        host.derive_operation_work(&parent.services.thread_extension_data, &self.turn_id)
    }

    pub(crate) fn derive(
        &self,
        child: &CodexThread,
    ) -> Result<Option<Box<dyn HostTurnWork>>, TurnWorkRefused> {
        let parent = self.session.upgrade().ok_or(TurnWorkRefused::Unavailable)?;
        let child_requires = child
            .session
            .services
            .host_admission
            .as_ref()
            .is_some_and(|host| host.requires_account_work());
        let Some(host) = &parent.services.host_admission else {
            return if child_requires {
                Err(TurnWorkRefused::Unavailable)
            } else {
                Ok(None)
            };
        };
        let work = host.derive_turn_work(
            &parent.services.thread_extension_data,
            &self.turn_id,
            &child.session.services.thread_extension_data,
            Box::pin(child.termination_receipt()),
        )?;
        if work.is_none() && (host.requires_account_work() || child_requires) {
            return Err(TurnWorkRefused::Unavailable);
        }
        Ok(work)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[derive(Debug)]
    pub(crate) struct ParentGate {
        store: String,
        pub(crate) active: AtomicBool,
        pub(crate) calls: AtomicUsize,
        pub(crate) live: Arc<AtomicUsize>,
        pub(crate) refuse_descendants: Arc<AtomicBool>,
    }

    #[derive(Debug)]
    struct Work {
        live: Arc<AtomicUsize>,
        refuse: Arc<AtomicBool>,
    }

    impl Drop for Work {
        fn drop(&mut self) {
            self.live.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl HostOperationWork for Work {
        fn derive_operation(&self) -> Result<Box<dyn HostOperationWork>, TurnWorkRefused> {
            if self.refuse.load(Ordering::SeqCst) {
                return Err(TurnWorkRefused::Unavailable);
            }
            self.live.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(Self {
                live: Arc::clone(&self.live),
                refuse: Arc::clone(&self.refuse),
            }))
        }
        fn derive_turn_work(
            &self,
            _: &codex_extension_api::ExtensionData,
            _: codex_extension_api::ExtensionFuture<'static, ()>,
        ) -> Result<Box<dyn HostTurnWork>, TurnWorkRefused> {
            Err(TurnWorkRefused::Unavailable)
        }
    }

    impl codex_extension_api::TurnStartAdmission for ParentGate {
        fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
            None
        }
        fn requires_account_work(&self) -> bool {
            true
        }
        fn admit_operation_work(
            &self,
        ) -> Result<Option<Box<dyn HostOperationWork>>, TurnWorkRefused> {
            panic!("parent provenance cannot acquire fresh admission")
        }
        fn derive_request_operation_work(
            &self,
        ) -> Result<Option<Box<dyn HostOperationWork>>, TurnWorkRefused> {
            panic!("explicit parent work cannot rediscover ambient request authority")
        }
        fn derive_operation_work(
            &self,
            store: &codex_extension_api::ExtensionData,
            turn: &str,
        ) -> Result<Option<Box<dyn HostOperationWork>>, TurnWorkRefused> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(store.level_id(), self.store);
            if turn != "live-parent" || !self.active.load(Ordering::SeqCst) {
                return Err(TurnWorkRefused::Unavailable);
            }
            self.live.fetch_add(1, Ordering::SeqCst);
            Ok(Some(Box::new(Work {
                live: Arc::clone(&self.live),
                refuse: Arc::clone(&self.refuse_descendants),
            })))
        }
    }

    pub(crate) async fn parent() -> (Arc<Session>, Arc<ParentGate>) {
        let (mut session, _) = crate::session::tests::make_session_and_context().await;
        let gate = Arc::new(ParentGate {
            store: session.services.thread_extension_data.level_id().to_owned(),
            active: AtomicBool::new(true),
            calls: AtomicUsize::new(0),
            live: Arc::new(AtomicUsize::new(0)),
            refuse_descendants: Arc::new(AtomicBool::new(false)),
        });
        session.services.host_admission = Some(gate.clone());
        (Arc::new(session), gate)
    }

    #[tokio::test]
    async fn constructor_provenance_is_weak_and_work_outlives_ended_parent_turn() {
        let (parent, gate) = parent().await;
        let authority = ParentTurnAuthority::capture(&parent, "live-parent");
        assert_eq!(gate.live.load(Ordering::SeqCst), 0);
        let work = ParentTurnAuthority::capture_construction(Some(&authority))
            .unwrap()
            .unwrap();
        assert_eq!(gate.live.load(Ordering::SeqCst), 1);
        gate.active.store(false, Ordering::SeqCst);
        let descendant = work.derive().unwrap();
        assert_eq!(gate.live.load(Ordering::SeqCst), 2);
        assert!(authority.derive_operation().is_err());
        assert_eq!(gate.calls.load(Ordering::SeqCst), 2);
        let weak = Arc::downgrade(&parent);
        drop(parent);
        assert!(weak.upgrade().is_none());
        assert!(authority.derive_operation().is_err());
        drop(descendant);
        drop(work);
        assert_eq!(gate.live.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn exhausted_descendant_and_wrong_turn_refuse_without_fresh_admission() {
        let (parent, gate) = parent().await;
        let wrong = ParentTurnAuthority::capture(&parent, "different-turn");
        assert!(wrong.derive_operation().is_err());
        assert_eq!(gate.live.load(Ordering::SeqCst), 0);
        let valid = ParentTurnAuthority::capture(&parent, "live-parent");
        let work = ParentTurnAuthority::capture_construction(Some(&valid))
            .unwrap()
            .unwrap();
        gate.refuse_descendants.store(true, Ordering::SeqCst);
        assert!(work.derive().is_err());
        assert_eq!(gate.live.load(Ordering::SeqCst), 1);
        drop(work);
        assert_eq!(gate.live.load(Ordering::SeqCst), 0);
        assert_eq!(gate.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn explicit_ungated_parent_observation_survives_independent_derivation() {
        let (mut parent, _) = crate::session::tests::make_session_and_context().await;
        parent.services.host_admission = None;
        let parent = Arc::new(parent);
        let authority = ParentTurnAuthority::capture(&parent, "ungated");
        let captured = ParentTurnAuthority::capture_construction(Some(&authority))
            .unwrap()
            .unwrap();
        assert!(captured.0.is_none());
        assert!(captured.derive().unwrap().0.is_none());
        assert!(
            ParentTurnAuthority::capture_construction(None)
                .unwrap()
                .is_none()
        );
    }
}
