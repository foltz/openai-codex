//! Retained logical-service termination evidence, independent of process exit.
//!
//! rmcp logs transport-close failures instead of returning them from its service
//! task. Joining that task therefore needs a separate transport-close receipt.

use std::ops::Deref;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering as AtomicOrdering;

use crate::stdio_server_launcher::StdioProcessRef;
use crate::stdio_server_launcher::StdioTermination;
use futures::FutureExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use rmcp::Service;
use rmcp::service::Peer;
use rmcp::service::QuitReason;
use rmcp::service::RoleClient;
use rmcp::service::RunningService;
use rmcp::service::RunningServiceCancellationToken;
use rmcp::service::RxJsonRpcMessage;
use rmcp::service::TxJsonRpcMessage;
use rmcp::transport::Transport;
use rmcp::transport::worker::CloseDeadline;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// Credential-free outcome for one reserved physical connection attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PhysicalRetirementOutcome {
    Complete,
    TimedOut,
    IncompleteOwnership,
    LogicalFailed,
    ProcessFailed,
}

/// Complete census, including failed startup and superseded physical services.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalRetirementReport {
    pub attempts: Vec<(usize, PhysicalRetirementOutcome)>,
}

impl PhysicalRetirementReport {
    /// True only when every registered attempt has positive terminal evidence.
    pub fn is_complete(&self) -> bool {
        self.attempts
            .iter()
            .all(|(_, outcome)| *outcome == PhysicalRetirementOutcome::Complete)
    }
}

/// Reservation failure occurs before any launch side effect.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("physical connection registration is closed")]
pub struct RegistrationClosed;

#[derive(Default)]
struct RegistryState {
    closed: bool,
    shutdown: CancellationToken,
    attempts: Vec<Arc<Attempt>>,
    /// Shared with every rmcp worker this registry's transports spawn, so their
    /// exit cleanup ends by the earliest retirement deadline. Its own mutex is
    /// a leaf: it is taken after this registry lock or alone, never before it.
    close_deadline: CloseDeadline,
    /// Set only by a complete `shutdown_until` census of a closed registry.
    /// Shared with observers so the proof outlives the registry itself.
    proven: Arc<AtomicBool>,
}

/// Allocate before constructing the first transport and retain through retirement.
#[derive(Clone, Default)]
pub struct RmcpClientRetirement {
    state: Arc<Mutex<RegistryState>>,
}

/// Observation handle that never extends a registry's lifetime. Custody stays
/// with the owning runtime; this only lets an exact-endpoint retirement find it.
#[derive(Clone)]
pub struct WeakRmcpClientRetirement {
    state: Weak<Mutex<RegistryState>>,
}

impl WeakRmcpClientRetirement {
    pub fn upgrade(&self) -> Option<RmcpClientRetirement> {
        self.state
            .upgrade()
            .map(|state| RmcpClientRetirement { state })
    }

    pub fn is_dropped(&self) -> bool {
        self.state.strong_count() == 0
    }

    /// Identity of the exact registry, not of an equal-looking one.
    pub fn same_registry(&self, other: &Self) -> bool {
        Weak::ptr_eq(&self.state, &other.state)
    }
}

/// Cached terminal evidence: true only after a complete physical retirement
/// census was observed for the whole, closed registry. Never set otherwise.
#[derive(Clone)]
pub struct RetirementProof(Arc<AtomicBool>);

impl RetirementProof {
    pub fn is_proven(&self) -> bool {
        self.0.load(AtomicOrdering::Acquire)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum LaunchState {
    Dormant,
    Started,
    Finished,
    Skipped,
}

struct Attempt {
    state: Mutex<AttemptState>,
}

struct AttemptState {
    terminal: bool,
    launch: LaunchState,
    startup: Vec<Shared<BoxFuture<'static, ()>>>,
    active: usize,
    receipt: Option<TransportCloseReceipt>,
    service: Option<Arc<dyn RetainedService>>,
    process: Option<StdioProcessRef>,
    no_resource: bool,
}

/// Weak attachment authority prevents stored startup futures owning their slot.
#[derive(Clone)]
pub(crate) struct PhysicalAttemptTicket {
    attempt: Weak<Attempt>,
    registry: Weak<Mutex<RegistryState>>,
    pub(crate) shutdown: CancellationToken,
    /// Cloned from the registry at reservation, so reading it never takes the
    /// registry lock. It holds no registry reference, so no ownership cycle.
    close_deadline: CloseDeadline,
}

impl PhysicalAttemptTicket {
    /// Reserve a creation or handshake phase before its first poll. Stored
    /// factories must capture only weak tickets, never their owning client.
    pub(crate) fn start_phase<T, F, Fut>(
        &self,
        factory: F,
    ) -> Result<Shared<BoxFuture<'static, Option<T>>>, RegistrationClosed>
    where
        T: Clone + Send + Sync + 'static,
        F: FnOnce(PhysicalAttemptTicket) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let registry = self.registry.upgrade().ok_or(RegistrationClosed)?;
        let registry = registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if registry.closed {
            return Err(RegistrationClosed);
        }
        let attempt = self.attempt.upgrade().ok_or(RegistrationClosed)?;
        let ticket = self.clone();
        let startup = async move {
            let allowed = ticket.registry.upgrade().is_some_and(|registry| {
                let registry = registry
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let Some(attempt) = ticket.attempt.upgrade() else {
                    return false;
                };
                let mut state = attempt
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if registry.closed {
                    if state.launch == LaunchState::Dormant {
                        state.launch = LaunchState::Skipped;
                    }
                    false
                } else {
                    state.launch = LaunchState::Started;
                    state.active += 1;
                    true
                }
            });
            if !allowed {
                return None;
            }
            let result = factory(ticket.clone()).await;
            if let Some(attempt) = ticket.attempt.upgrade() {
                let mut state = attempt
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.active -= 1;
                if state.active == 0 {
                    state.launch = LaunchState::Finished;
                }
            }
            Some(result)
        }
        .boxed()
        .shared();
        attempt
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .startup
            .push(startup.clone().map(|_| ()).boxed().shared());
        Ok(startup)
    }
    pub(crate) fn attach_transport(
        &self,
        receipt: TransportCloseReceipt,
        process: Option<StdioProcessRef>,
    ) {
        if let Some(attempt) = self.attempt.upgrade() {
            let mut state = attempt
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(
                state.receipt.is_none(),
                "one transport per physical attempt"
            );
            state.receipt = Some(receipt);
            state.process = process;
        }
    }

    /// The registry's retirement deadline, for an rmcp worker to read when its
    /// exit cleanup starts. Takes no lock.
    pub(crate) fn close_deadline(&self) -> CloseDeadline {
        self.close_deadline.clone()
    }

    pub(crate) fn attach_service<S: Service<RoleClient> + 'static>(
        &self,
        service: Arc<ManagedRunningService<S>>,
    ) {
        if let Some(attempt) = self.attempt.upgrade() {
            let mut state = attempt
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(state.service.is_none(), "one service per physical attempt");
            state.service = Some(service);
        }
    }

    /// Only a launch path proving no transport/process was created may call this.
    #[cfg(test)]
    pub(crate) fn finish_no_resource(&self) {
        if let Some(attempt) = self.attempt.upgrade() {
            let mut state = attempt
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(state.receipt.is_none() && state.process.is_none());
            state.no_resource = true;
        }
    }
}

impl RmcpClientRetirement {
    pub fn downgrade(&self) -> WeakRmcpClientRetirement {
        WeakRmcpClientRetirement {
            state: Arc::downgrade(&self.state),
        }
    }

    /// Earliest deadline any owner has latched for this registry's workers.
    pub fn latched_deadline(&self) -> Option<Instant> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .close_deadline
            .get()
    }

    /// Observes under `min(deadline, latched)`, retaining a latched deadline
    /// even when it has already expired: an expired latch yields no cleanup
    /// work at all. Plain `shutdown_until` keeps its retry-with-fresh-budget
    /// semantics for owners that explicitly retry.
    pub async fn shutdown_within_latched(&self, deadline: Instant) -> PhysicalRetirementReport {
        let effective = self
            .latched_deadline()
            .map_or(deadline, |latched| latched.min(deadline));
        self.shutdown_until(effective).await
    }

    pub fn retirement_proof(&self) -> RetirementProof {
        RetirementProof(Arc::clone(
            &self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .proven,
        ))
    }

    pub(crate) fn close_registration_until(&self, deadline: Instant) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .close_deadline
            .latch(deadline);
        self.close_registration();
    }

    /// Close launch admission before the upper owner cancels/drains its tasks.
    /// Already-started phases may still attach their owned resources; dormant
    /// phases are atomically skipped and can never begin a transport launch.
    pub fn close_registration(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.closed = true;
        state.shutdown.cancel();
        for attempt in &state.attempts {
            let mut attempt = attempt
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if attempt.launch == LaunchState::Dormant {
                attempt.launch = LaunchState::Skipped;
            }
        }
    }

    pub(crate) fn reserve_attempt(&self) -> Result<PhysicalAttemptTicket, RegistrationClosed> {
        let mut registry = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if registry.closed {
            return Err(RegistrationClosed);
        }
        let attempt = Arc::new(Attempt {
            state: Mutex::new(AttemptState {
                terminal: false,
                launch: LaunchState::Dormant,
                startup: Vec::new(),
                active: 0,
                receipt: None,
                service: None,
                process: None,
                no_resource: false,
            }),
        });
        let ticket = PhysicalAttemptTicket {
            attempt: Arc::downgrade(&attempt),
            registry: Arc::downgrade(&self.state),
            shutdown: registry.shutdown.clone(),
            close_deadline: registry.close_deadline.clone(),
        };
        registry.attempts.push(attempt);
        Ok(ticket)
    }
    /// The factory must not capture a strong registry or a client owning it.
    /// Only the weak ticket belongs in the stored future. Output is shared so
    /// cancellation of a constructor observer cannot discard admitted work.
    #[cfg(test)]
    pub(crate) fn start_attempt<T, F, Fut>(
        &self,
        factory: F,
    ) -> Result<Shared<BoxFuture<'static, Option<T>>>, RegistrationClosed>
    where
        T: Clone + Send + Sync + 'static,
        F: FnOnce(PhysicalAttemptTicket) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        self.reserve_attempt()?.start_phase(factory)
    }

    /// Cancelled callers leave every original launch/close future retained.
    pub async fn shutdown_until(&self, deadline: Instant) -> PhysicalRetirementReport {
        self.shutdown_for_until(StdioTermination::Shutdown, deadline)
            .await
    }

    pub(crate) async fn shutdown_for_until(
        &self,
        reason: StdioTermination,
        deadline: Instant,
    ) -> PhysicalRetirementReport {
        // Latch before closing registration, which can already start a close.
        // Latching keeps the earliest deadline, so a later call never extends it.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .close_deadline
            .latch(deadline);
        self.close_registration();
        let attempts = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.attempts.clone()
        };
        let outcomes = futures::future::join_all(attempts.into_iter().enumerate().map(
            |(id, attempt)| async move { (id, retire_attempt(attempt, reason, deadline).await) },
        ))
        .await;
        let report = PhysicalRetirementReport { attempts: outcomes };
        if report.is_complete() {
            // Registration is closed, so this census covers every attempt
            // that can ever exist for this registry.
            self.state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .proven
                .store(true, AtomicOrdering::Release);
        }
        report
    }
}

async fn retire_attempt(
    attempt: Arc<Attempt>,
    reason: StdioTermination,
    deadline: Instant,
) -> PhysicalRetirementOutcome {
    let startup = {
        let state = attempt
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.terminal {
            return PhysicalRetirementOutcome::Complete;
        }
        // timeout_at polls its inner future first. Do not use that poll to
        // resume startup or initiate cleanup after this attempt's budget.
        // Previously recorded terminal proof above remains replayable.
        if Instant::now() >= deadline {
            return PhysicalRetirementOutcome::TimedOut;
        }
        state.startup.clone()
    };
    let drain = async {
        futures::future::join_all(startup).await;
    };
    let cleanup = async {
        loop {
            let (launch, receipt, service, process, no_resource) = {
                let state = attempt
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if state.terminal {
                    return PhysicalRetirementOutcome::Complete;
                }
                if Instant::now() >= deadline {
                    return PhysicalRetirementOutcome::TimedOut;
                }
                (
                    state.launch,
                    state.receipt.clone(),
                    state.service.clone(),
                    state.process.clone(),
                    state.no_resource,
                )
            };
            if launch == LaunchState::Skipped {
                return PhysicalRetirementOutcome::Complete;
            }
            // Process termination must progress even while admitted startup is
            // stuck. The independent ticket is safe to retry after cancellation.
            if let Some(service) = &service {
                service.request_close();
            }
            let process_close = async {
                match process {
                    Some(process) => process
                        .terminate_until(reason, deadline)
                        .await
                        .map_err(|_| ()),
                    None => Ok(()),
                }
            };
            let logical_close = async {
                if let Some(service) = service {
                    service.request_close();
                    return service.wait_until(deadline).await.map(|_| ());
                }
                if let Some(receipt) = receipt {
                    return receipt.close_returned_until(deadline).await;
                }
                Err(LogicalCloseError::TransportCloseUnobserved)
            };
            let (process_result, logical_result) = tokio::join!(process_close, logical_close);
            if process_result.is_err() {
                return PhysicalRetirementOutcome::ProcessFailed;
            }
            if launch == LaunchState::Finished {
                return match logical_result {
                    Ok(()) => PhysicalRetirementOutcome::Complete,
                    Err(LogicalCloseError::TransportCloseUnobserved) if no_resource => {
                        PhysicalRetirementOutcome::Complete
                    }
                    Err(
                        LogicalCloseError::TransportCloseUnobserved
                        | LogicalCloseError::TransportStillLeased,
                    ) => PhysicalRetirementOutcome::IncompleteOwnership,
                    Err(LogicalCloseError::TimedOut) => PhysicalRetirementOutcome::TimedOut,
                    Err(_) => PhysicalRetirementOutcome::LogicalFailed,
                };
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    };
    match tokio::time::timeout_at(deadline, async {
        let (_, outcome) = tokio::join!(drain, cleanup);
        outcome
    })
    .await
    {
        Ok(PhysicalRetirementOutcome::Complete) => {
            // Both branches above finished: every admitted phase was drained
            // and logical/process acknowledgement was observed. Keep identity
            // plus the positive fact, not entire historic connection owners.
            // Another concurrent observer may still hold its own clones; it
            // sees terminal on its next sample and cannot erase this proof.
            let retired = {
                let mut state = attempt
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.terminal = true;
                (
                    std::mem::take(&mut state.startup),
                    state.receipt.take(),
                    state.service.take(),
                    state.process.take(),
                )
            };
            // Resource destructors do not run under the registry state guard.
            drop(retired);
            PhysicalRetirementOutcome::Complete
        }
        Ok(outcome) => outcome,
        Err(_) => PhysicalRetirementOutcome::TimedOut,
    }
}

/// Both the service task and its transport have finished successfully.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LogicalCloseAck;

/// Credential-free retirement classifications; underlying errors stay private.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum LogicalCloseError {
    #[error("service join failed")]
    ServiceJoinFailed,
    #[error("service task failed")]
    ServiceTaskFailed,
    #[error("service quit reason is unsupported")]
    UnsupportedQuitReason,
    #[error("transport close failed")]
    TransportCloseFailed,
    #[error("transport close was not observed")]
    TransportCloseUnobserved,
    #[error("transport is still leased to a handshake or service")]
    TransportStillLeased,
    #[error("logical retirement timed out")]
    TimedOut,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CloseState {
    Pending,
    Closed,
    Failed,
}

/// A retained receipt for the exact transport, including failed initialization.
#[derive(Clone)]
pub(crate) struct TransportCloseReceipt {
    state: watch::Receiver<CloseState>,
    owner: Arc<dyn ReturnedTransportOwner>,
}

impl TransportCloseReceipt {
    /// Close the actual transport returned by a failed/cancelled handshake.
    /// The registry must first drain that handshake. A timeout drops only this
    /// observer, leaving the original close future and transport in the ticket.
    pub(crate) async fn close_returned_until(
        &self,
        deadline: Instant,
    ) -> Result<(), LogicalCloseError> {
        match *self.state.borrow() {
            CloseState::Closed => return Ok(()),
            CloseState::Failed => return Err(LogicalCloseError::TransportCloseFailed),
            CloseState::Pending => {}
        }
        if Instant::now() >= deadline {
            return Err(LogicalCloseError::TimedOut);
        }
        let closing = self.owner.begin_close()?;
        tokio::time::timeout_at(deadline, closing)
            .await
            .map_err(|_| LogicalCloseError::TimedOut)?
    }

    fn outcome(&self) -> Result<(), LogicalCloseError> {
        match *self.state.borrow() {
            CloseState::Closed => Ok(()),
            CloseState::Failed => Err(LogicalCloseError::TransportCloseFailed),
            CloseState::Pending => Err(LogicalCloseError::TransportCloseUnobserved),
        }
    }

    /// Observe a close without consuming the receipt or manufacturing success
    /// when the transport disappears. This does not initiate transport cleanup.
    #[cfg(test)]
    pub(crate) async fn wait_until(&self, deadline: Instant) -> Result<(), LogicalCloseError> {
        let mut state = self.state.clone();
        tokio::time::timeout_at(deadline, async move {
            loop {
                let current = *state.borrow_and_update();
                match current {
                    CloseState::Closed => return Ok(()),
                    CloseState::Failed => return Err(LogicalCloseError::TransportCloseFailed),
                    CloseState::Pending => {}
                }
                state
                    .changed()
                    .await
                    .map_err(|_| LogicalCloseError::TransportCloseUnobserved)?;
            }
        })
        .await
        .map_err(|_| LogicalCloseError::TimedOut)?
    }
}

type TransportCompletion = Shared<BoxFuture<'static, Result<(), LogicalCloseError>>>;

/// Erases the concrete transport without erasing its retained cleanup ownership.
trait ReturnedTransportOwner: Send + Sync {
    fn begin_close(&self) -> Result<TransportCompletion, LogicalCloseError>;
}

enum TransportLeaseState<T> {
    Leased,
    Returned(T),
    Closing {
        completion: TransportCompletion,
        // Retain the actual transport even if close fails. Completed failure
        // remains sticky; another call cannot reinterpret an empty close as OK.
        _transport: Arc<Mutex<Option<T>>>,
    },
}

struct TransportLease<T> {
    state: Mutex<TransportLeaseState<T>>,
    closed: watch::Sender<CloseState>,
}

impl<T: Transport<RoleClient> + 'static> ReturnedTransportOwner for TransportLease<T> {
    fn begin_close(&self) -> Result<TransportCompletion, LogicalCloseError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &*state {
            TransportLeaseState::Leased => return Err(LogicalCloseError::TransportStillLeased),
            TransportLeaseState::Closing { completion, .. } => return Ok(completion.clone()),
            TransportLeaseState::Returned(_) => {}
        }
        let TransportLeaseState::Returned(mut transport) =
            std::mem::replace(&mut *state, TransportLeaseState::Leased)
        else {
            unreachable!("returned transport checked under the same guard");
        };
        let retained = Arc::new(Mutex::new(None));
        let completed_transport = Arc::clone(&retained);
        let closed = self.closed.clone();
        let completion = async move {
            let result = transport
                .close()
                .await
                .map_err(|_| LogicalCloseError::TransportCloseFailed);
            *completed_transport
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(transport);
            closed.send_replace(if result.is_ok() {
                CloseState::Closed
            } else {
                CloseState::Failed
            });
            result
        }
        .boxed()
        .shared();
        *state = TransportLeaseState::Closing {
            completion: completion.clone(),
            _transport: retained,
        };
        Ok(completion)
    }
}

/// Records the real close result before rmcp can discard it.
pub(crate) struct AcknowledgedTransport<T> {
    inner: Option<T>,
    lease: Arc<TransportLease<T>>,
}

impl<T: Transport<RoleClient> + 'static> AcknowledgedTransport<T> {
    #[expect(
        clippy::expect_used,
        reason = "inner is taken only by Drop, after these borrows end"
    )]
    pub(crate) fn inner_mut(&mut self) -> &mut T {
        self.inner.as_mut().expect("live transport")
    }
    #[expect(
        clippy::expect_used,
        reason = "inner is taken only by Drop, after these borrows end"
    )]
    pub(crate) fn inner(&self) -> &T {
        self.inner.as_ref().expect("live transport")
    }
    pub(crate) fn new(inner: T) -> (Self, TransportCloseReceipt) {
        let (closed, state) = watch::channel(CloseState::Pending);
        let lease = Arc::new(TransportLease {
            state: Mutex::new(TransportLeaseState::Leased),
            closed,
        });
        (
            Self {
                inner: Some(inner),
                lease: Arc::clone(&lease),
            },
            TransportCloseReceipt {
                state,
                owner: lease,
            },
        )
    }
}

impl<T> Drop for AcknowledgedTransport<T> {
    fn drop(&mut self) {
        if let Some(transport) = self.inner.take() {
            *self
                .lease
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                TransportLeaseState::Returned(transport);
        }
    }
}

impl<T: Transport<RoleClient>> Transport<RoleClient> for AcknowledgedTransport<T> {
    type Error = T::Error;

    #[expect(
        clippy::expect_used,
        reason = "inner is initialized by new and taken only by Drop; these &mut methods cannot run afterwards"
    )]
    fn send(
        &mut self,
        item: TxJsonRpcMessage<RoleClient>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        self.inner
            .as_mut()
            .expect("transport is returned only on drop")
            .send(item)
    }

    #[expect(
        clippy::expect_used,
        reason = "inner is initialized by new and taken only by Drop; these &mut methods cannot run afterwards"
    )]
    fn receive(&mut self) -> impl Future<Output = Option<RxJsonRpcMessage<RoleClient>>> + Send {
        self.inner
            .as_mut()
            .expect("transport is returned only on drop")
            .receive()
    }

    #[expect(
        clippy::expect_used,
        reason = "inner is initialized by new and taken only by Drop; these &mut methods cannot run afterwards"
    )]
    async fn close(&mut self) -> Result<(), Self::Error> {
        let result = self
            .inner
            .as_mut()
            .expect("transport is returned only on drop")
            .close()
            .await;
        // Preserve the first complete close result. A repeated close must not
        // erase a failure merely because the underlying transport became empty.
        self.lease.closed.send_if_modified(|state| {
            if *state != CloseState::Pending {
                return false;
            }
            *state = if result.is_ok() {
                CloseState::Closed
            } else {
                CloseState::Failed
            };
            true
        });
        result
    }
}

type Completion = Shared<BoxFuture<'static, Result<LogicalCloseAck, LogicalCloseError>>>;

struct ServiceLeaseState<S: Service<RoleClient>> {
    service: Option<Arc<RunningService<RoleClient, S>>>,
    closing: bool,
    leases: usize,
}

struct ServiceLeaseOwner<S: Service<RoleClient>> {
    state: Mutex<ServiceLeaseState<S>>,
    changed: watch::Sender<usize>,
}

pub(crate) struct ServiceOperationLease<S: Service<RoleClient>> {
    service: Option<Arc<RunningService<RoleClient, S>>>,
    owner: Arc<ServiceLeaseOwner<S>>,
}

impl<S: Service<RoleClient>> Drop for ServiceOperationLease<S> {
    fn drop(&mut self) {
        drop(self.service.take());
        let mut state = self
            .owner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.leases -= 1;
        self.owner.changed.send_replace(state.leases);
    }
}

impl<S: Service<RoleClient>> ServiceLeaseOwner<S> {
    fn acquire(self: &Arc<Self>) -> Result<ServiceOperationLease<S>, rmcp::service::ServiceError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closing {
            return Err(rmcp::service::ServiceError::TransportClosed);
        }
        let service = state
            .service
            .clone()
            .ok_or(rmcp::service::ServiceError::TransportClosed)?;
        state.leases += 1;
        Ok(ServiceOperationLease {
            service: Some(service),
            owner: Arc::clone(self),
        })
    }
}

impl<S: Service<RoleClient>> ServiceOperationLease<S> {
    pub(crate) fn reacquire(&self) -> Result<Arc<Self>, rmcp::service::ServiceError> {
        self.owner.acquire().map(Arc::new)
    }
}

impl<S: Service<RoleClient>> Deref for ServiceOperationLease<S> {
    type Target = RunningService<RoleClient, S>;
    fn deref(&self) -> &Self::Target {
        self.service
            .as_ref()
            .expect("lease retains its service until drop")
    }
}

/// The registry erases only terminal observation, never request execution.
trait RetainedService: Send + Sync {
    fn request_close(&self);
    fn wait_until(
        &self,
        deadline: Instant,
    ) -> BoxFuture<'_, Result<LogicalCloseAck, LogicalCloseError>>;
}

/// Owns the original service join future, while requests use its cloned peer.
///
/// This is not a spawned supervisor: completion is driven only while an observer
/// polls it. The registry must strongly retain this owner until terminal success.
/// Cancelling or timing out an observer drops only a shared clone, preserving the
/// original join future for another attempt. Never wrap rmcp's `close()` itself
/// in a timeout: that method takes and can then detach its private join handle.
pub(crate) struct ManagedRunningService<S: Service<RoleClient>> {
    peer: Peer<RoleClient>,
    cancellation: Mutex<Option<RunningServiceCancellationToken>>,
    owner: Arc<ServiceLeaseOwner<S>>,
    completion: Completion,
    // Completion futures release their captures once resolved. Keep the
    // returned transport owned even when the cached result is failure.
    _receipt: TransportCloseReceipt,
}

impl<S: Service<RoleClient> + 'static> ManagedRunningService<S> {
    pub(crate) fn new(
        service: RunningService<RoleClient, S>,
        receipt: TransportCloseReceipt,
    ) -> Self
    where
        S: Service<RoleClient> + 'static,
    {
        let peer = service.peer().clone();
        let cancellation = Mutex::new(Some(service.cancellation_token()));
        let retained_receipt = receipt.clone();
        let (changed, mut changes) = watch::channel(0usize);
        let owner = Arc::new(ServiceLeaseOwner {
            state: Mutex::new(ServiceLeaseState {
                service: Some(Arc::new(service)),
                closing: false,
                leases: 0,
            }),
            changed,
        });
        let retained_owner = Arc::clone(&owner);
        let completion = async move {
            let service = loop {
                changes.borrow_and_update();
                let service = {
                    let mut state = owner
                        .state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    state.closing = true;
                    if state.leases == 0 {
                        state.service.take()
                    } else {
                        None
                    }
                };
                if let Some(service) = service {
                    break service;
                }
                changes
                    .changed()
                    .await
                    .map_err(|_| LogicalCloseError::ServiceJoinFailed)?;
            };
            let service =
                Arc::try_unwrap(service).map_err(|_| LogicalCloseError::ServiceJoinFailed)?;
            match service.waiting().await {
                Ok(QuitReason::Closed | QuitReason::Cancelled) => {}
                Ok(QuitReason::JoinError(_)) => return Err(LogicalCloseError::ServiceTaskFailed),
                Ok(_) => return Err(LogicalCloseError::UnsupportedQuitReason),
                Err(_) => return Err(LogicalCloseError::ServiceJoinFailed),
            }
            // The service join occurs after transport.close; pending here is
            // missing evidence, not something to infer from cancellation.
            receipt.outcome()?;
            Ok(LogicalCloseAck)
        }
        .boxed()
        .shared();
        Self {
            peer,
            cancellation,
            owner: retained_owner,
            completion,
            _receipt: retained_receipt,
        }
    }

    /// A request keeps the original typed service alive through every upstream
    /// continuation, cancellation and request-scoped input path.
    pub(crate) fn lease(
        &self,
    ) -> Result<Arc<ServiceOperationLease<S>>, rmcp::service::ServiceError> {
        self.owner.acquire().map(Arc::new)
    }

    pub(crate) fn owns(&self, lease: &ServiceOperationLease<S>) -> bool {
        Arc::ptr_eq(&self.owner, &lease.owner)
    }

    pub(crate) fn peer(&self) -> &Peer<RoleClient> {
        &self.peer
    }

    /// Request cancellation synchronously; this is not a terminal receipt.
    pub(crate) fn request_close(&self) {
        self.owner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closing = true;
        let token = self
            .cancellation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(token) = token {
            token.cancel();
        }
    }

    #[cfg(test)]
    pub(crate) async fn call_tool(
        &self,
        params: rmcp::model::CallToolRequestParams,
    ) -> Result<rmcp::model::CallToolResult, rmcp::service::ServiceError> {
        self.lease()?.call_tool(params).await
    }

    /// Diagnostic compatibility only; retirement must use `wait_until`.
    pub(crate) fn is_closed(&self) -> bool {
        self.peer.is_transport_closed()
            || self.completion.peek().is_some()
            || self
                .cancellation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_none()
    }

    pub(crate) async fn wait_until(
        &self,
        deadline: Instant,
    ) -> Result<LogicalCloseAck, LogicalCloseError> {
        if let Some(outcome) = self.completion.peek() {
            return *outcome;
        }
        if Instant::now() >= deadline {
            return Err(LogicalCloseError::TimedOut);
        }
        // A direct observer has the same cancellation semantics as the
        // registry path. Expired observers above still initiate no work.
        self.request_close();
        tokio::time::timeout_at(deadline, self.completion.clone())
            .await
            .map_err(|_| LogicalCloseError::TimedOut)?
    }
}

impl<S: Service<RoleClient> + 'static> Deref for ManagedRunningService<S> {
    type Target = Peer<RoleClient>;

    fn deref(&self) -> &Self::Target {
        self.peer()
    }
}

impl<S: Service<RoleClient> + 'static> RetainedService for ManagedRunningService<S> {
    fn request_close(&self) {
        ManagedRunningService::request_close(self);
    }
    fn wait_until(
        &self,
        deadline: Instant,
    ) -> BoxFuture<'_, Result<LogicalCloseAck, LogicalCloseError>> {
        ManagedRunningService::wait_until(self, deadline).boxed()
    }
}

#[cfg(test)]
#[path = "retirement_tests.rs"]
mod tests;

/// Default observation budget shared by client, process and early retirement.
pub const DEFAULT_RETIREMENT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
