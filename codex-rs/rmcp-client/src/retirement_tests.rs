use super::*;
use std::io;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use pretty_assertions::assert_eq;
use rmcp::service::ClientServiceExt;
use rmcp::service::serve_directly;
use tokio::sync::Semaphore;

#[tokio::test]
async fn closing_gate_waits_for_cancelled_operation_lease_without_losing_join() {
    let transport = controlled_transport(false);
    let sent = Arc::clone(&transport.handshake_sent);
    transport.release.add_permits(1);
    let (transport, receipt) = AcknowledgedTransport::new(transport);
    let owner = ManagedRunningService::new(serve_directly((), transport, None), receipt);
    let deadline = Instant::now() + Duration::from_secs(5);
    {
        let operation = owner.call_tool(rmcp::model::CallToolRequestParams::new("held"));
        tokio::pin!(operation);
        tokio::select! {
            result = &mut operation => panic!("request should await response: {result:?}"),
            permit = sent.acquire() => permit.expect("request sent").forget(),
        }
        owner.request_close();
        assert!(matches!(
            owner
                .call_tool(rmcp::model::CallToolRequestParams::new("late"))
                .await,
            Err(rmcp::service::ServiceError::TransportClosed)
        ));
        assert!(
            owner.wait_until(deadline).now_or_never().is_none(),
            "held operation prevents exclusive join transfer"
        );
        // Caller cancellation drops its lease even without polling the request
        // again after service cancellation. The owner retains the actual join.
    }
    assert_eq!(owner.wait_until(deadline).await, Ok(LogicalCloseAck));
}

#[tokio::test]
async fn registry_closure_skips_dormant_launch_and_rejects_new_registration() {
    let registry = RmcpClientRetirement::default();
    let launched = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&launched);
    let startup = registry
        .start_attempt(move |ticket| async move {
            observed.fetch_add(1, Ordering::SeqCst);
            ticket.finish_no_resource();
        })
        .expect("reserve");
    let report = registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(
        report.attempts,
        vec![(0, PhysicalRetirementOutcome::Complete)]
    );
    assert_eq!(startup.await, None);
    assert_eq!(launched.load(Ordering::SeqCst), 0);
    assert!(registry.start_attempt(|_| async {}).is_err());
}

#[tokio::test(start_paused = true)]
async fn expired_registry_budget_does_not_start_cleanup_but_replays_terminal_proof() {
    let registry = RmcpClientRetirement::default();
    let transport = controlled_transport(false);
    transport.release.add_permits(1);
    let closes = Arc::clone(&transport.closes);
    let startup = registry
        .start_attempt(move |ticket| async move {
            let (transport, receipt) = AcknowledgedTransport::new(transport);
            ticket.attach_transport(receipt, None);
            drop(transport);
        })
        .expect("reserve");
    assert_eq!(startup.await, Some(()));
    let expired = Instant::now();
    assert_eq!(
        registry.shutdown_until(expired).await.attempts,
        vec![(0, PhysicalRetirementOutcome::TimedOut)]
    );
    assert_eq!(closes.load(Ordering::SeqCst), 0);
    let report = registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(
        report.attempts,
        vec![(0, PhysicalRetirementOutcome::Complete)]
    );
    assert_eq!(registry.shutdown_until(expired).await, report);
    assert_eq!(closes.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn expired_returned_transport_budget_preserves_unstarted_close_for_retry() {
    let transport = controlled_transport(false);
    transport.release.add_permits(1);
    let closes = Arc::clone(&transport.closes);
    let (transport, receipt) = AcknowledgedTransport::new(transport);
    drop(transport);
    let expired = Instant::now();
    assert_eq!(
        receipt.close_returned_until(expired).await,
        Err(LogicalCloseError::TimedOut)
    );
    assert_eq!(closes.load(Ordering::SeqCst), 0);
    assert_eq!(
        receipt
            .close_returned_until(Instant::now() + Duration::from_secs(1))
            .await,
        Ok(())
    );
    assert_eq!(receipt.close_returned_until(expired).await, Ok(()));
    assert_eq!(closes.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn registry_retains_started_launch_after_observer_drop_and_timeout() {
    let registry = RmcpClientRetirement::default();
    let gate = Arc::new(Semaphore::new(0));
    let open = Arc::clone(&gate);
    let startup = registry
        .start_attempt(move |ticket| async move {
            open.acquire().await.expect("continue").forget();
            ticket.finish_no_resource();
        })
        .expect("reserve");
    assert!(startup.clone().now_or_never().is_none());
    drop(startup);
    let report = registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(
        report.attempts,
        vec![(0, PhysicalRetirementOutcome::TimedOut)]
    );
    gate.add_permits(1);
    let report = registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(
        report.attempts,
        vec![(0, PhysicalRetirementOutcome::Complete)]
    );
}

#[tokio::test(start_paused = true)]
async fn hung_launch_does_not_starve_other_transport_cleanup() {
    let registry = RmcpClientRetirement::default();
    let hung = registry
        .start_attempt(|_| std::future::pending::<()>())
        .expect("reserve");
    assert!(hung.now_or_never().is_none());
    let transport = controlled_transport(false);
    transport.release.add_permits(1);
    let closes = Arc::clone(&transport.closes);
    let ready = registry
        .start_attempt(move |ticket| async move {
            let (transport, receipt) = AcknowledgedTransport::new(transport);
            ticket.attach_transport(receipt, None);
            drop(transport);
        })
        .expect("reserve");
    assert_eq!(ready.await, Some(()));
    let report = registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(
        report.attempts,
        vec![
            (0, PhysicalRetirementOutcome::TimedOut),
            (1, PhysicalRetirementOutcome::Complete)
        ]
    );
    assert_eq!(closes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn pending_registry_has_no_strong_slot_future_cycle() {
    let registry = RmcpClientRetirement::default();
    let probe = Arc::new(());
    let captured = Arc::clone(&probe);
    let startup = registry
        .start_attempt(move |_| async move {
            let _captured = captured;
            std::future::pending::<()>().await;
        })
        .expect("reserve");
    assert!(startup.now_or_never().is_none());
    assert_eq!(Arc::strong_count(&probe), 2);
    drop(registry);
    assert_eq!(Arc::strong_count(&probe), 1);
}

#[tokio::test]
async fn physical_ticket_accepts_creation_then_handshake_but_not_after_close() {
    let registry = RmcpClientRetirement::default();
    let ticket = registry.reserve_attempt().expect("reserve");
    let created = ticket.start_phase(|_| async { 1usize }).expect("creation");
    assert_eq!(created.await, Some(1));
    let handshake = ticket
        .start_phase(|ticket| async move {
            ticket.finish_no_resource();
            2usize
        })
        .expect("handshake");
    assert_eq!(handshake.await, Some(2));
    let report = registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(
        report.attempts,
        vec![(0, PhysicalRetirementOutcome::Complete)]
    );
    assert!(ticket.start_phase(|_| async {}).is_err());
}

#[tokio::test]
async fn draining_unit_phase_releases_pending_output_when_typed_observers_are_gone() {
    let registry = RmcpClientRetirement::default();
    let transport = controlled_transport(false);
    transport.release.add_permits(1);
    let closes = Arc::clone(&transport.closes);
    let startup = registry
        .start_attempt(move |ticket| async move {
            let (transport, receipt) = AcknowledgedTransport::new(transport);
            ticket.attach_transport(receipt, None);
            Arc::new(Mutex::new(Some(transport)))
        })
        .expect("reserve");
    // Complete creation without retaining its returned Arc. Only the registry's
    // mapped-to-unit drain future now retains the typed Shared result.
    drop(startup.clone().now_or_never().expect("creation ready"));
    drop(startup);
    let report = registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(
        report.attempts,
        vec![(0, PhysicalRetirementOutcome::Complete)]
    );
    assert_eq!(closes.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn attached_returned_transport_is_closed_while_same_attempt_startup_hangs() {
    let registry = RmcpClientRetirement::default();
    let transport = controlled_transport(false);
    transport.release.add_permits(1);
    let closes = Arc::clone(&transport.closes);
    let startup = registry
        .start_attempt(move |ticket| async move {
            let (transport, receipt) = AcknowledgedTransport::new(transport);
            ticket.attach_transport(receipt, None);
            drop(transport);
            std::future::pending::<()>().await;
        })
        .expect("reserve");
    assert!(startup.now_or_never().is_none());
    let report = registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(
        report.attempts,
        vec![(0, PhysicalRetirementOutcome::TimedOut)]
    );
    assert_eq!(
        closes.load(Ordering::SeqCst),
        1,
        "a hung phase must not starve its attached resource"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_poll_and_registration_close_choose_one_authoritative_gate_outcome() {
    let registry = RmcpClientRetirement::default();
    let launched = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&launched);
    let startup = registry
        .start_attempt(move |ticket| async move {
            count.fetch_add(1, Ordering::SeqCst);
            ticket.finish_no_resource();
        })
        .expect("reserve");
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let start_gate = Arc::clone(&barrier);
    let starter = tokio::spawn(async move {
        start_gate.wait().await;
        startup.await
    });
    barrier.wait().await;
    let report = registry
        .shutdown_until(Instant::now() + Duration::from_secs(2))
        .await;
    let outcome = starter.await.expect("starter joined");
    assert_eq!(
        launched.load(Ordering::SeqCst),
        usize::from(outcome.is_some())
    );
    assert_eq!(
        report.attempts,
        vec![(0, PhysicalRetirementOutcome::Complete)]
    );
    assert!(registry.reserve_attempt().is_err());
}

#[cfg(unix)]
#[tokio::test]
async fn observed_local_process_exit_does_not_replace_failed_logical_close() {
    use crate::stdio_server_launcher::StdioServerCommand;
    use crate::stdio_server_launcher::StdioServerLauncher;
    let directory = tempfile::tempdir().expect("temporary cwd");
    let launcher = crate::LocalStdioServerLauncher::new(directory.path().to_path_buf());
    let process_transport = launcher
        .launch(StdioServerCommand::new(
            "sleep".into(),
            vec!["30".into()],
            None,
            Vec::new(),
            None,
            crate::McpProtocolMode::Legacy,
        ))
        .await
        .expect("launch child");
    let process = process_transport.process_handle();
    let observed = process.clone();
    let registry = RmcpClientRetirement::default();
    let transport = controlled_transport(true);
    transport.release.add_permits(1);
    let started = registry
        .start_attempt(move |ticket| async move {
            let (transport, receipt) = AcknowledgedTransport::new(transport);
            ticket.attach_transport(receipt.clone(), Some(process));
            let service = Arc::new(ManagedRunningService::new(
                serve_directly((), transport, None),
                receipt,
            ));
            ticket.attach_service(service);
        })
        .expect("reserve");
    assert_eq!(started.await, Some(()));
    let report = registry
        .shutdown_until(Instant::now() + Duration::from_secs(5))
        .await;
    assert_eq!(
        report.attempts,
        vec![(0, PhysicalRetirementOutcome::LogicalFailed)]
    );
    observed
        .terminate()
        .await
        .expect("process proof was independently completed");
    drop(process_transport);
}

struct ControlledTransport {
    _drop_probe: Option<Arc<()>>,
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
    closes: Arc<AtomicUsize>,
    fail_close: bool,
    receive_eof: bool,
    handshake_sent: Arc<Semaphore>,
    drops: Arc<AtomicUsize>,
}

impl Drop for ControlledTransport {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl Transport<RoleClient> for ControlledTransport {
    type Error = io::Error;

    fn send(
        &mut self,
        _item: TxJsonRpcMessage<RoleClient>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        let sent = Arc::clone(&self.handshake_sent);
        async move {
            sent.add_permits(1);
            Ok(())
        }
    }

    async fn receive(&mut self) -> Option<RxJsonRpcMessage<RoleClient>> {
        if self.receive_eof {
            None
        } else {
            std::future::pending().await
        }
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        self.entered.add_permits(1);
        self.release
            .acquire()
            .await
            .expect("release close")
            .forget();
        if self.fail_close {
            Err(io::Error::other("private transport error"))
        } else {
            Ok(())
        }
    }
}

fn controlled_transport(fail_close: bool) -> ControlledTransport {
    ControlledTransport {
        _drop_probe: None,
        entered: Arc::new(Semaphore::new(0)),
        release: Arc::new(Semaphore::new(0)),
        closes: Arc::new(AtomicUsize::new(0)),
        fail_close,
        receive_eof: false,
        handshake_sent: Arc::new(Semaphore::new(0)),
        drops: Arc::new(AtomicUsize::new(0)),
    }
}

#[tokio::test]
async fn terminal_compaction_releases_service_transport_and_phase_captures() {
    let registry = RmcpClientRetirement::default();
    let probe = Arc::new(());
    let mut transport = controlled_transport(false);
    transport._drop_probe = Some(Arc::clone(&probe));
    transport.release.add_permits(1);
    let startup = registry
        .start_attempt(move |ticket| async move {
            let (transport, receipt) = AcknowledgedTransport::new(transport);
            ticket.attach_transport(receipt.clone(), None);
            let owner = Arc::new(ManagedRunningService::new(
                serve_directly((), transport, None),
                receipt,
            ));
            let weak_owner = Arc::downgrade(&owner);
            ticket.attach_service(owner);
            weak_owner
        })
        .expect("reserve");
    let weak_owner = startup.await.expect("started");
    assert!(weak_owner.upgrade().is_some());
    assert_eq!(Arc::strong_count(&probe), 2);
    let deadline = Instant::now() + Duration::from_secs(5);
    let (first, concurrent) = tokio::join!(
        registry.shutdown_until(deadline),
        registry.shutdown_until(deadline)
    );
    assert_eq!(
        first.attempts,
        vec![(0, PhysicalRetirementOutcome::Complete)]
    );
    assert_eq!(concurrent, first);
    assert!(
        weak_owner.upgrade().is_none(),
        "terminal identity must not retain service"
    );
    assert_eq!(
        Arc::strong_count(&probe),
        1,
        "terminal identity must not retain transport"
    );
    assert_eq!(registry.shutdown_until(deadline).await, first);
}

#[tokio::test]
async fn failed_retirement_keeps_actual_transport_owned_until_registry_drops() {
    let registry = RmcpClientRetirement::default();
    let probe = Arc::new(());
    let mut transport = controlled_transport(true);
    transport._drop_probe = Some(Arc::clone(&probe));
    transport.release.add_permits(1);
    let startup = registry
        .start_attempt(move |ticket| async move {
            let (transport, receipt) = AcknowledgedTransport::new(transport);
            ticket.attach_transport(receipt, None);
            drop(transport);
        })
        .expect("reserve");
    assert_eq!(startup.await, Some(()));
    let deadline = Instant::now() + Duration::from_secs(5);
    let report = registry.shutdown_until(deadline).await;
    assert_eq!(
        report.attempts,
        vec![(0, PhysicalRetirementOutcome::LogicalFailed)]
    );
    assert_eq!(Arc::strong_count(&probe), 2);
    assert_eq!(registry.shutdown_until(deadline).await, report);
    assert_eq!(
        Arc::strong_count(&probe),
        2,
        "failed proof retains actual resource"
    );
    drop(registry);
    assert_eq!(Arc::strong_count(&probe), 1);
}

#[tokio::test]
async fn cancelled_observer_retains_original_join_for_retry_and_idempotence() {
    let transport = controlled_transport(false);
    let entered = Arc::clone(&transport.entered);
    let release = Arc::clone(&transport.release);
    let closes = Arc::clone(&transport.closes);
    let (transport, receipt) = AcknowledgedTransport::new(transport);
    let owner = ManagedRunningService::new(serve_directly((), transport, None), receipt);
    owner.request_close();
    assert!(
        owner.is_closed(),
        "requested cancellation is diagnostic only"
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    {
        let observation = owner.wait_until(deadline);
        tokio::pin!(observation);
        tokio::select! {
            outcome = &mut observation => panic!("close is still parked: {outcome:?}"),
            permit = entered.acquire() => permit.expect("close entered").forget(),
        }
        // Dropping this polled observer must not detach the original join.
    }
    assert!(owner.completion.peek().is_none());
    release.add_permits(1);
    assert_eq!(owner.wait_until(deadline).await, Ok(LogicalCloseAck));
    owner.request_close();
    assert_eq!(owner.wait_until(deadline).await, Ok(LogicalCloseAck));
    assert_eq!(closes.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn timed_out_observer_does_not_consume_completion_ownership() {
    let transport = controlled_transport(false);
    let release = Arc::clone(&transport.release);
    let (transport, receipt) = AcknowledgedTransport::new(transport);
    let owner = ManagedRunningService::new(serve_directly((), transport, None), receipt);
    owner.request_close();
    assert_eq!(
        owner
            .wait_until(Instant::now() + Duration::from_secs(1))
            .await,
        Err(LogicalCloseError::TimedOut)
    );
    release.add_permits(1);
    assert_eq!(
        owner
            .wait_until(Instant::now() + Duration::from_secs(1))
            .await,
        Ok(LogicalCloseAck)
    );
}

#[tokio::test]
async fn joined_service_does_not_hide_transport_close_failure() {
    let transport = controlled_transport(true);
    transport.release.add_permits(1);
    let (transport, receipt) = AcknowledgedTransport::new(transport);
    let owner = ManagedRunningService::new(serve_directly((), transport, None), receipt);
    owner.request_close();
    assert_eq!(
        owner
            .wait_until(Instant::now() + Duration::from_secs(5))
            .await,
        Err(LogicalCloseError::TransportCloseFailed)
    );
}

#[tokio::test]
async fn joined_service_without_exact_close_receipt_refuses_success() {
    let (unused_transport, receipt) = AcknowledgedTransport::new(controlled_transport(false));
    drop(unused_transport);
    let transport = controlled_transport(false);
    transport.release.add_permits(1);
    let owner = ManagedRunningService::new(serve_directly((), transport, None), receipt.clone());
    owner.request_close();
    let deadline = Instant::now() + Duration::from_secs(5);
    assert_eq!(
        owner.wait_until(deadline).await,
        Err(LogicalCloseError::TransportCloseUnobserved)
    );
    assert_eq!(
        receipt.outcome(),
        Err(LogicalCloseError::TransportCloseUnobserved)
    );
}

#[tokio::test]
async fn failed_real_handshake_returns_actual_transport_for_close() {
    let mut transport = controlled_transport(false);
    transport.receive_eof = true;
    transport.release.add_permits(1);
    let closes = Arc::clone(&transport.closes);
    let (transport, receipt) = AcknowledgedTransport::new(transport);
    let result =
        ().serve_with_lifecycle(transport, crate::McpProtocolMode::Legacy.client_lifecycle())
            .await;
    assert!(
        result.is_err(),
        "EOF must fail the real initialize handshake"
    );
    assert_eq!(
        closes.load(Ordering::SeqCst),
        0,
        "rmcp does not close failed startup"
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    assert_eq!(receipt.close_returned_until(deadline).await, Ok(()));
    assert_eq!(receipt.wait_until(deadline).await, Ok(()));
    assert_eq!(receipt.close_returned_until(deadline).await, Ok(()));
    assert_eq!(closes.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn cancelled_real_handshake_and_timed_out_cleanup_retain_actual_transport() {
    let transport = controlled_transport(false);
    let sent = Arc::clone(&transport.handshake_sent);
    let release = Arc::clone(&transport.release);
    let closes = Arc::clone(&transport.closes);
    let (transport, receipt) = AcknowledgedTransport::new(transport);
    {
        let handshake =
            ().serve_with_lifecycle(transport, crate::McpProtocolMode::Legacy.client_lifecycle());
        tokio::pin!(handshake);
        tokio::select! {
            result = &mut handshake => panic!("handshake should await response: {}", result.is_ok()),
            permit = sent.acquire() => permit.expect("initialize sent").forget(),
        }
        assert_eq!(
            receipt
                .close_returned_until(Instant::now() + Duration::from_secs(1))
                .await,
            Err(LogicalCloseError::TransportStillLeased)
        );
    }
    assert_eq!(closes.load(Ordering::SeqCst), 0);
    assert_eq!(
        receipt
            .close_returned_until(Instant::now() + Duration::from_secs(1))
            .await,
        Err(LogicalCloseError::TimedOut)
    );
    release.add_permits(1);
    assert_eq!(
        receipt
            .close_returned_until(Instant::now() + Duration::from_secs(1))
            .await,
        Ok(())
    );
    assert_eq!(
        closes.load(Ordering::SeqCst),
        1,
        "retry resumes the original close future"
    );
}

#[tokio::test]
async fn returned_transport_close_failure_remains_sticky_and_owned() {
    let transport = controlled_transport(true);
    transport.release.add_permits(1);
    let closes = Arc::clone(&transport.closes);
    let drops = Arc::clone(&transport.drops);
    let (transport, receipt) = AcknowledgedTransport::new(transport);
    drop(transport);
    let deadline = Instant::now() + Duration::from_secs(5);
    assert_eq!(
        receipt.close_returned_until(deadline).await,
        Err(LogicalCloseError::TransportCloseFailed)
    );
    assert_eq!(
        receipt.close_returned_until(deadline).await,
        Err(LogicalCloseError::TransportCloseFailed)
    );
    assert_eq!(closes.load(Ordering::SeqCst), 1);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "failed close must retain the actual transport"
    );
    drop(receipt);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "the final owner releases the transport exactly once"
    );
}
