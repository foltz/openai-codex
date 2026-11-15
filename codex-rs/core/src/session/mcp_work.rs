//! Explicit finite account authority at the Core/MCP boundary.
//!
//! This adapter belongs to an invocation, never an idle runtime or binding.

use super::session::Session;
use super::turn_context::TurnContext;
use codex_extension_api::HostOperationWork;
use codex_mcp::McpAttemptRefused;
use codex_mcp::McpAttemptWork;

pub(crate) struct McpOperationWork(pub(crate) Box<dyn HostOperationWork>);

impl McpAttemptWork for McpOperationWork {
    fn derive_attempt(&self) -> Result<Box<dyn McpAttemptWork>, McpAttemptRefused> {
        self.0
            .derive_operation()
            .map(|work| Box::new(Self(work)) as Box<dyn McpAttemptWork>)
            .map_err(|_| McpAttemptRefused)
    }
}

impl Session {
    /// Capture the exact live turn's authority, including from spawned tool
    /// tasks. A missing turn entry refuses; it never becomes fresh admission.
    pub(crate) fn turn_mcp_work(
        &self,
        turn: &TurnContext,
    ) -> anyhow::Result<Option<Box<dyn McpAttemptWork>>> {
        let Some(host) = &self.services.host_admission else {
            return Ok(None);
        };
        host.derive_operation_work(&self.services.thread_extension_data, &turn.sub_id)
            .map(|work| {
                work.map(|work| Box::new(McpOperationWork(work)) as Box<dyn McpAttemptWork>)
            })
            .map_err(|_| anyhow::anyhow!("MCP turn account work is unavailable"))
    }

    /// Capture before the first await in a host-request entry point. Absence
    /// remains ungated; exhausted derivation refuses without fresh admission.
    pub(crate) fn request_mcp_work(&self) -> anyhow::Result<Option<Box<dyn McpAttemptWork>>> {
        let Some(host) = &self.services.host_admission else {
            return Ok(None);
        };
        host.derive_request_operation_work()
            .map(|work| {
                work.map(|work| Box::new(McpOperationWork(work)) as Box<dyn McpAttemptWork>)
            })
            .map_err(|_| anyhow::anyhow!("MCP request account work is unavailable"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

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
        fn derive_operation(
            &self,
        ) -> Result<Box<dyn HostOperationWork>, codex_extension_api::TurnWorkRefused> {
            if self.refuse.load(Ordering::SeqCst) {
                return Err(codex_extension_api::TurnWorkRefused::Unavailable);
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
        ) -> Result<
            Box<dyn codex_protocol::host_turn_work::HostTurnWork>,
            codex_extension_api::TurnWorkRefused,
        > {
            Err(codex_extension_api::TurnWorkRefused::Unavailable)
        }
    }
    #[derive(Debug)]
    struct Marker(Arc<()>);
    #[derive(Debug)]
    struct Host {
        live: Arc<AtomicUsize>,
        refuse: Arc<AtomicBool>,
        marker: Arc<()>,
        turn: String,
        requests: AtomicUsize,
        turns: AtomicUsize,
    }
    impl Host {
        fn work(
            &self,
        ) -> Result<Option<Box<dyn HostOperationWork>>, codex_extension_api::TurnWorkRefused>
        {
            if self.refuse.load(Ordering::SeqCst) {
                return Err(codex_extension_api::TurnWorkRefused::Unavailable);
            }
            self.live.fetch_add(1, Ordering::SeqCst);
            Ok(Some(Box::new(Work {
                live: Arc::clone(&self.live),
                refuse: Arc::clone(&self.refuse),
            })))
        }
    }
    impl codex_extension_api::TurnStartAdmission for Host {
        fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
            Some(Box::new(()))
        }
        fn admit_operation_work(
            &self,
        ) -> Result<Option<Box<dyn HostOperationWork>>, codex_extension_api::TurnWorkRefused>
        {
            panic!("request/turn derivation must never fall back to fresh admission");
        }
        fn derive_request_operation_work(
            &self,
        ) -> Result<Option<Box<dyn HostOperationWork>>, codex_extension_api::TurnWorkRefused>
        {
            self.requests.fetch_add(1, Ordering::SeqCst);
            self.work()
        }
        fn derive_operation_work(
            &self,
            store: &codex_extension_api::ExtensionData,
            turn: &str,
        ) -> Result<Option<Box<dyn HostOperationWork>>, codex_extension_api::TurnWorkRefused>
        {
            self.turns.fetch_add(1, Ordering::SeqCst);
            assert!(Arc::ptr_eq(
                &store.get::<Marker>().expect("exact thread store").0,
                &self.marker
            ));
            assert_eq!(turn, self.turn);
            self.work()
        }
    }

    #[tokio::test]
    async fn request_and_turn_capture_derive_from_the_exact_host_without_fresh_admission() {
        let (mut session, turn) = crate::session::tests::make_session_and_context().await;
        assert!(session.request_mcp_work().unwrap().is_none());
        assert!(session.turn_mcp_work(&turn).unwrap().is_none());
        let marker = Arc::new(());
        session
            .services
            .thread_extension_data
            .insert(Marker(Arc::clone(&marker)));
        let host = Arc::new(Host {
            live: Arc::new(AtomicUsize::new(0)),
            refuse: Arc::new(AtomicBool::new(false)),
            marker,
            turn: turn.sub_id.clone(),
            requests: AtomicUsize::new(0),
            turns: AtomicUsize::new(0),
        });
        session.services.host_admission = Some(host.clone());
        let request = session.request_mcp_work().unwrap().unwrap();
        let turn = session.turn_mcp_work(&turn).unwrap().unwrap();
        let child = request.derive_attempt().unwrap();
        assert_eq!(host.requests.load(Ordering::SeqCst), 1);
        assert_eq!(host.turns.load(Ordering::SeqCst), 1);
        assert_eq!(host.live.load(Ordering::SeqCst), 3);
        drop(request);
        assert_eq!(host.live.load(Ordering::SeqCst), 2);
        drop((turn, child));
        assert_eq!(
            host.live.load(Ordering::SeqCst),
            0,
            "idle session has only uncounted host policy"
        );
    }

    #[tokio::test]
    async fn exhausted_request_turn_and_attempt_derivation_refuse_without_fallback() {
        let (mut session, turn) = crate::session::tests::make_session_and_context().await;
        let marker = Arc::new(());
        session
            .services
            .thread_extension_data
            .insert(Marker(Arc::clone(&marker)));
        let host = Arc::new(Host {
            live: Arc::new(AtomicUsize::new(0)),
            refuse: Arc::new(AtomicBool::new(false)),
            marker,
            turn: turn.sub_id.clone(),
            requests: AtomicUsize::new(0),
            turns: AtomicUsize::new(0),
        });
        session.services.host_admission = Some(host.clone());
        let existing = session.request_mcp_work().unwrap().unwrap();
        host.refuse.store(true, Ordering::SeqCst);
        assert!(existing.derive_attempt().is_err());
        assert!(session.request_mcp_work().is_err());
        assert!(session.turn_mcp_work(&turn).is_err());
        assert_eq!(host.requests.load(Ordering::SeqCst), 2);
        assert_eq!(host.turns.load(Ordering::SeqCst), 1);
        assert_eq!(host.live.load(Ordering::SeqCst), 1);
        drop(existing);
        assert_eq!(host.live.load(Ordering::SeqCst), 0);
    }
}
