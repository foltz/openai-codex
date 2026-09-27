//! The queued operation, not the request waiter, owns host admission custody.

use codex_core::NotSubmittedReason;
use codex_core::StartIfIdleSubmission;
use codex_core::TurnInputRequest;
use codex_extension_api::ExtensionData;
use codex_extension_api::ExtensionFuture;
use codex_extension_api::ExtensionRegistryBuilder;
use codex_extension_api::TurnLifecycleContributor;
use codex_extension_api::TurnStartAdmission;
use codex_extension_api::TurnStartInput;
use codex_extension_api::TurnWorkRefused;
use codex_protocol::host_turn_work::HostTurnWork;
use codex_protocol::protocol::EventMsg;
use codex_protocol::user_input::UserInput;
use core_test_support::responses;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::sync::Notify;

#[derive(Debug, Default, PartialEq, Eq)]
struct Evidence {
    bound: Option<String>,
    retained: usize,
    cancelled: usize,
}

#[derive(Debug)]
struct Lease {
    evidence: Arc<Mutex<Evidence>>,
    retained: bool,
}

impl HostTurnWork for Lease {
    fn bind_submission(&mut self, turn_id: &str) {
        self.evidence.lock().unwrap().bound = Some(turn_id.to_owned());
    }

    fn retain_until_terminal(mut self: Box<Self>) {
        self.evidence.lock().unwrap().retained += 1;
        self.retained = true;
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        if !self.retained {
            self.evidence.lock().unwrap().cancelled += 1;
        }
    }
}

#[derive(Debug, Default)]
struct Gate {
    refused: AtomicBool,
    evidence: Arc<Mutex<Evidence>>,
    entered: Notify,
    release: Notify,
}

impl TurnStartAdmission for Gate {
    fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
        Some(Box::new(()))
    }

    fn admit_turn_work(
        &self,
        _store: &ExtensionData,
        _termination: ExtensionFuture<'static, ()>,
    ) -> Result<Option<Box<dyn HostTurnWork>>, TurnWorkRefused> {
        if self.refused.load(Ordering::Acquire) {
            return Err(TurnWorkRefused::Unavailable);
        }
        Ok(Some(Box::new(Lease {
            evidence: Arc::clone(&self.evidence),
            retained: false,
        })))
    }
}

impl TurnLifecycleContributor for Gate {
    fn on_turn_start<'a>(&'a self, input: TurnStartInput<'a>) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            assert_eq!(
                self.evidence.lock().unwrap().bound.as_deref(),
                Some(input.turn_id)
            );
            self.entered.notify_one();
            self.release.notified().await;
        })
    }
}

fn input() -> TurnInputRequest {
    TurnInputRequest::user_input(vec![UserInput::Text {
        text: "hello".to_owned(),
        text_elements: Vec::new(),
    }])
}

#[tokio::test]
async fn cancelled_caller_does_not_release_a_queued_turn_lease() -> anyhow::Result<()> {
    let server = responses::start_mock_server().await;
    let response = responses::mount_sse_once(&server, responses::sse_completed("done")).await;
    let gate = Arc::new(Gate::default());
    let mut extensions = ExtensionRegistryBuilder::new();
    extensions.turn_start_admission(gate.clone());
    extensions.turn_lifecycle_contributor(gate.clone());
    let test = test_codex()
        .with_extensions(Arc::new(extensions.build()))
        .build_with_auto_env(&server)
        .await?;
    let mut call = Box::pin(test.codex.start_turn_if_idle(input()));
    tokio::select! {
        _ = gate.entered.notified() => {}
        result = &mut call => panic!("submission replied before the held start: {result:?}"),
    }
    drop(call);
    assert_eq!(gate.evidence.lock().unwrap().cancelled, 0);
    gate.release.notify_one();
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
    let evidence = gate.evidence.lock().unwrap();
    assert!(evidence.bound.is_some());
    assert_eq!((evidence.retained, evidence.cancelled), (1, 0));
    drop(evidence);
    assert_eq!(response.requests().len(), 1);
    test.codex.shutdown_and_wait().await?;
    Ok(())
}

#[tokio::test]
async fn account_refusal_precedes_submission_even_when_shutdown_gate_is_open() -> anyhow::Result<()>
{
    let server = responses::start_mock_server().await;
    let response = responses::mount_sse_once(&server, responses::sse_completed("unused")).await;
    let gate = Arc::new(Gate::default());
    gate.refused.store(true, Ordering::Release);
    let mut extensions = ExtensionRegistryBuilder::new();
    extensions.turn_start_admission(gate.clone());
    let test = test_codex()
        .with_extensions(Arc::new(extensions.build()))
        .build_with_auto_env(&server)
        .await?;
    assert_eq!(
        test.codex.start_turn_if_idle(input()).await?,
        StartIfIdleSubmission::NotSubmitted {
            reason: NotSubmittedReason::ServerDraining,
        }
    );
    assert_eq!(*gate.evidence.lock().unwrap(), Evidence::default());
    assert!(response.requests().is_empty());
    gate.refused.store(false, Ordering::Release);
    assert!(matches!(
        test.codex.start_turn_if_idle(input()).await?,
        StartIfIdleSubmission::Started { .. }
    ));
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
    assert_eq!(response.requests().len(), 1);
    test.codex.shutdown_and_wait().await?;
    Ok(())
}

#[tokio::test]
async fn carried_trigger_mail_starts_with_its_own_work_when_fresh_admission_is_closed()
-> anyhow::Result<()> {
    let server = responses::start_mock_server().await;
    let response = responses::mount_sse_once(&server, responses::sse_completed("mail-done")).await;
    let gate = Arc::new(Gate::default());
    gate.refused.store(true, Ordering::Release);
    let mut extensions = ExtensionRegistryBuilder::new();
    extensions.turn_start_admission(gate.clone());
    let test = test_codex()
        .with_extensions(Arc::new(extensions.build()))
        .build_with_auto_env(&server)
        .await?;
    test.codex
        .submit(codex_protocol::protocol::Op::InterAgentCommunication {
            communication: codex_protocol::protocol::InterAgentCommunication::new(
                codex_protocol::AgentPath::root(),
                codex_protocol::AgentPath::root(),
                Vec::new(),
                "admitted trigger".into(),
                /*trigger_turn*/ true,
            ),
            start_options: Default::default(),
            work: Some(Box::new(Lease {
                evidence: Arc::clone(&gate.evidence),
                retained: false,
            })),
        })
        .await?;
    let event = wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
    let EventMsg::TurnComplete(completed) = event else {
        unreachable!()
    };
    assert_eq!(
        *gate.evidence.lock().unwrap(),
        Evidence {
            bound: Some(completed.turn_id),
            retained: 1,
            cancelled: 0,
        }
    );
    assert_eq!(response.requests().len(), 1);
    test.codex.shutdown_and_wait().await?;
    Ok(())
}
