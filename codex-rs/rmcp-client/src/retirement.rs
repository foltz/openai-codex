//! Retained logical-service termination evidence, independent of process exit.
//!
//! rmcp logs transport-close failures instead of returning them from its service
//! task. Joining that task therefore needs a separate transport-close receipt.

use std::ops::Deref;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;

use crate::stdio_server_launcher::StdioServerProcessHandle;
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
}

/// Allocate before constructing the first transport and retain through retirement.
#[derive(Clone, Default)]
pub struct RmcpClientRetirement {
    state: Arc<Mutex<RegistryState>>,
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
    service: Option<Arc<ManagedRunningService>>,
    process: Option<StdioServerProcessHandle>,
    no_resource: bool,
}

/// Weak attachment authority prevents stored startup futures owning their slot.
#[derive(Clone)]
pub(crate) struct PhysicalAttemptTicket {
    attempt: Weak<Attempt>,
    registry: Weak<Mutex<RegistryState>>,
    pub(crate) shutdown: CancellationToken,
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
        process: Option<StdioServerProcessHandle>,
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

    pub(crate) fn attach_service(&self, service: Arc<ManagedRunningService>) {
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
        self.close_registration();
        let attempts = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.attempts.clone()
        };
        let outcomes = futures::future::join_all(
            attempts
                .into_iter()
                .enumerate()
                .map(|(id, attempt)| async move { (id, retire_attempt(attempt, deadline).await) }),
        )
        .await;
        PhysicalRetirementReport { attempts: outcomes }
    }
}

async fn retire_attempt(attempt: Arc<Attempt>, deadline: Instant) -> PhysicalRetirementOutcome {
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
            let process_close = async {
                match process {
                    Some(process) => process.terminate_until(deadline).await.map_err(|_| ()),
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

struct ServiceOperationLease<S: Service<RoleClient>> {
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

/// Erases the local handler type while retaining a lease across tool continuations.
trait ServiceOperations: Send + Sync {
    fn close_gate(&self);
    fn call_tool(
        &self,
        params: rmcp::model::CallToolRequestParams,
    ) -> BoxFuture<'_, Result<rmcp::model::CallToolResult, rmcp::service::ServiceError>>;
    fn read_resource(
        &self,
        params: rmcp::model::ReadResourceRequestParams,
    ) -> BoxFuture<'_, Result<rmcp::model::ReadResourceResult, rmcp::service::ServiceError>>;
}

impl<S: Service<RoleClient>> ServiceOperations for Arc<ServiceLeaseOwner<S>> {
    fn close_gate(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closing = true;
    }
    #[expect(
        clippy::expect_used,
        reason = "acquire initializes the lease service, which is taken only when the lease drops"
    )]
    fn call_tool(
        &self,
        params: rmcp::model::CallToolRequestParams,
    ) -> BoxFuture<'_, Result<rmcp::model::CallToolResult, rmcp::service::ServiceError>> {
        async move {
            let lease = self.acquire()?;
            crate::tool_input::call_tool(
                lease.service.as_ref().expect("live operation lease"),
                params,
            )
            .await
        }
        .boxed()
    }
    #[expect(
        clippy::expect_used,
        reason = "acquire initializes the lease service, which is taken only when the lease drops"
    )]
    fn read_resource(
        &self,
        params: rmcp::model::ReadResourceRequestParams,
    ) -> BoxFuture<'_, Result<rmcp::model::ReadResourceResult, rmcp::service::ServiceError>> {
        async move {
            let lease = self.acquire()?;
            lease
                .service
                .as_ref()
                .expect("live operation lease")
                .read_resource(params)
                .await
        }
        .boxed()
    }
}

/// Owns the original service join future, while requests use its cloned peer.
///
/// This is not a spawned supervisor: completion is driven only while an observer
/// polls it. The registry must strongly retain this owner until terminal success.
/// Cancelling or timing out an observer drops only a shared clone, preserving the
/// original join future for another attempt. Never wrap rmcp's `close()` itself
/// in a timeout: that method takes and can then detach its private join handle.
pub(crate) struct ManagedRunningService {
    peer: Peer<RoleClient>,
    cancellation: Mutex<Option<RunningServiceCancellationToken>>,
    operations: Box<dyn ServiceOperations>,
    completion: Completion,
    // Completion futures release their captures once resolved. Keep the
    // returned transport owned even when the cached result is failure.
    _receipt: TransportCloseReceipt,
}

impl ManagedRunningService {
    pub(crate) fn new<S>(
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
        let operations: Box<dyn ServiceOperations> = Box::new(Arc::clone(&owner));
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
            operations,
            completion,
            _receipt: retained_receipt,
        }
    }

    pub(crate) fn peer(&self) -> &Peer<RoleClient> {
        &self.peer
    }

    /// Request cancellation synchronously; this is not a terminal receipt.
    pub(crate) fn request_close(&self) {
        self.operations.close_gate();
        let token = self
            .cancellation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(token) = token {
            token.cancel();
        }
    }

    pub(crate) async fn call_tool(
        &self,
        params: rmcp::model::CallToolRequestParams,
    ) -> Result<rmcp::model::CallToolResult, rmcp::service::ServiceError> {
        self.operations.call_tool(params).await
    }

    pub(crate) async fn read_resource(
        &self,
        params: rmcp::model::ReadResourceRequestParams,
    ) -> Result<rmcp::model::ReadResourceResult, rmcp::service::ServiceError> {
        self.operations.read_resource(params).await
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
        tokio::time::timeout_at(deadline, self.completion.clone())
            .await
            .map_err(|_| LogicalCloseError::TimedOut)?
    }
}

impl Deref for ManagedRunningService {
    type Target = Peer<RoleClient>;

    fn deref(&self) -> &Self::Target {
        self.peer()
    }
}

#[cfg(test)]
#[path = "retirement_tests.rs"]
mod tests;
