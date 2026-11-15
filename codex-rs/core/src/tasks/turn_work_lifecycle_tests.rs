//! Logical terminal forwarding without widening private lifecycle methods.
use crate::session::tests::make_session_and_context;
use codex_extension_api::{ExtensionFuture, TurnStartAdmission};
use futures::FutureExt;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
struct TerminalHost(Arc<Mutex<Vec<String>>>);
impl TurnStartAdmission for TerminalHost {
    fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
        Some(Box::new(()))
    }
    fn turn_work_terminal(&self, _: &codex_extension_api::ExtensionData, turn: &str) {
        self.0.lock().unwrap().push(format!("isolated:{turn}"));
    }
}
impl codex_extension_api::TurnLifecycleContributor for TerminalHost {
    fn on_turn_stop<'a>(
        &'a self,
        input: codex_extension_api::TurnStopInput<'a>,
    ) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            self.0
                .lock()
                .unwrap()
                .push(format!("contributor:{}", input.turn_store.level_id()));
        })
    }
    fn on_turn_abort<'a>(
        &'a self,
        input: codex_extension_api::TurnAbortInput<'a>,
    ) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            self.0
                .lock()
                .unwrap()
                .push(format!("contributor:{}", input.turn_store.level_id()));
        })
    }
}

#[test_case::test_case(true; "isolated host gets logical terminal")]
#[test_case::test_case(false; "normal contributor is not duplicated")]
#[tokio::test]
async fn terminal_account_bridge_observes_exact_store_and_only_isolated_host(isolated: bool) {
    let (mut session, context) = make_session_and_context().await;
    let events = Arc::new(Mutex::new(Vec::new()));
    let gate = Arc::new(TerminalHost(Arc::clone(&events)));
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::new();
    if !isolated {
        builder.turn_start_admission(gate.clone());
        builder.turn_lifecycle_contributor(gate.clone());
    }
    session.services.extensions = Arc::new(builder.build());
    session.services.host_admission = Some(gate);
    session
        .emit_turn_stop_lifecycle(&context.extension_data)
        .await;
    session
        .emit_turn_abort_lifecycle(
            codex_protocol::protocol::TurnAbortReason::Interrupted,
            &context.extension_data,
        )
        .await;
    let prefix = if isolated { "isolated" } else { "contributor" };
    let expected = format!("{prefix}:{}", context.extension_data.level_id());
    assert_eq!(*events.lock().unwrap(), [expected.clone(), expected]);
    // This observes logical callbacks, never physical cleanup or a fabricated
    // SessionLoop/retirement receipt.
    assert!(session.turn_work_termination().now_or_never().is_none());
}
