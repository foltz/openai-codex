//! Custody of physical connections to MCP endpoints hosted by an app-server
//! client, scoped to one `McpManager` (one app-server runtime owner and every
//! thread, sub-agent, and isolated session it creates).
//!
//! Lifecycle of one endpoint key:
//! 1. **Provisional.** A connection's own thread request names a literal
//!    loopback endpoint in its overrides. Tracking starts at request intake,
//!    before that thread's runtime can admit a connection. It carries no
//!    authority.
//! 2. **Declared.** The same connection later proves (through the app-server's
//!    challenge) that the endpoint holds its retirement capability. Only a
//!    connection that used the endpoint may declare it.
//! 3. **Tombstoned.** A declarer retires it. This is permanent.
//!
//! The index holds weak handles plus cached terminal proofs, so it never
//! extends a runtime's lifetime. Every kind of entry is bounded, and the
//! bounds fail closed. Tombstones are never evicted.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use std::sync::Weak;

use codex_rmcp_client::CapabilityDigest;
use codex_rmcp_client::EndpointKey;
use codex_rmcp_client::PhysicalRetirementReport;
use codex_rmcp_client::RetirementProof;
use codex_rmcp_client::RmcpClientRetirement;
use codex_rmcp_client::WeakRmcpClientRetirement;
use tokio::time::Instant;

/// Declared plus tombstoned endpoints. When this is full, declarations are
/// refused and tombstones are kept.
pub const HOSTED_ENDPOINT_CAPACITY: usize = 1024;
/// Provisional (used but undeclared) endpoints across all connections.
pub const HOSTED_ENDPOINT_PROVISIONAL_CAPACITY: usize = 256;
/// Provisional endpoints introduced by one connection.
pub const HOSTED_ENDPOINT_PROVISIONAL_PER_CONNECTION: usize = 16;
/// Connection owners recorded for one endpoint; further admissions fail closed.
pub const HOSTED_ENDPOINT_OWNER_CAPACITY: usize = 4096;
/// Loopback endpoints admitted with no tracking entry. Overflow is sticky and
/// marks every later entry as possibly missing owners.
const UNTRACKED_SIGHTING_CAPACITY: usize = 4096;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostedEndpointRefusal {
    /// Not used, not declared, or wrong capability for this connection.
    NotOwner,
    /// The endpoint's owner retired it; the tombstone is permanent.
    Retired,
    /// A fixed bound is in use. Nothing is evicted.
    CapacityExhausted,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HostedEndpointCustodyCensus {
    pub declared: usize,
    pub provisional: usize,
    pub tombstones: usize,
    pub capacity: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostedEndpointConnectionReceipt {
    /// Canonical thread of the owning runtime; `None` for threadless runtimes.
    pub thread_id: Option<String>,
    pub report: PhysicalRetirementReport,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostedEndpointRetirementCensus {
    /// Live owners observed under the caller's deadline.
    pub receipts: Vec<HostedEndpointConnectionReceipt>,
    /// Owners already dropped with a cached complete retirement proof.
    pub proven_before_retirement: usize,
    /// Threads whose owners vanished without terminal proof: unknown evidence.
    pub unknown: Vec<Option<String>>,
    /// Owners may have been admitted before tracking began.
    pub possibly_untracked: bool,
    pub custody: HostedEndpointCustodyCensus,
}

impl HostedEndpointRetirementCensus {
    /// Unknown or possibly untracked evidence is never treated as retired.
    pub fn is_complete(&self) -> bool {
        self.receipts
            .iter()
            .all(|receipt| receipt.report.is_complete())
            && self.unknown.is_empty()
            && !self.possibly_untracked
    }
}

struct Owner {
    thread_id: Option<String>,
    lower: WeakRmcpClientRetirement,
    proof: RetirementProof,
}

impl Owner {
    fn released_with_proof(&self) -> bool {
        self.lower.is_dropped() && self.proof.is_proven()
    }
}

struct Endpoint {
    ever_declared: bool,
    /// `None` while provisional.
    capability: Option<CapabilityDigest>,
    /// Connections whose own thread requests named this endpoint.
    users: HashSet<u64>,
    declarers: HashSet<u64>,
    owners: Vec<Owner>,
    retired: bool,
    possibly_untracked: bool,
}

impl Endpoint {
    fn is_provisional(&self) -> bool {
        self.capability.is_none() && !self.retired
    }
}

struct CustodyState {
    generation: Option<u64>,
    capacity: usize,
    endpoints: HashMap<EndpointKey, Endpoint>,
    untracked_sightings: HashSet<EndpointKey>,
    sightings_overflowed: bool,
}

impl CustodyState {
    fn census(&self) -> HostedEndpointCustodyCensus {
        let mut census = HostedEndpointCustodyCensus {
            capacity: self.capacity,
            ..Default::default()
        };
        for endpoint in self.endpoints.values() {
            if endpoint.retired {
                census.tombstones += 1;
            } else if endpoint.capability.is_some() {
                census.declared += 1;
            } else {
                census.provisional += 1;
            }
        }
        census
    }
}

/// Attribution captured before a proof challenge. It grants no declaration
/// authority on its own and is bound to one original custody generation.
#[derive(Clone)]
pub struct HostedEndpointDeclaration {
    owner: Weak<Mutex<CustodyState>>,
    generation: u64,
    key: EndpointKey,
    connection: u64,
}

#[derive(Clone)]
pub struct HostedEndpointCustody {
    state: Arc<Mutex<CustodyState>>,
}

impl Default for HostedEndpointCustody {
    fn default() -> Self {
        Self::with_capacity(HOSTED_ENDPOINT_CAPACITY)
    }
}

impl HostedEndpointCustody {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(CustodyState {
                capacity,
                generation: Some(0),
                endpoints: HashMap::new(),
                untracked_sightings: HashSet::new(),
                sightings_overflowed: false,
            })),
        }
    }

    fn lock(&self) -> MutexGuard<'_, CustodyState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn census(&self) -> HostedEndpointCustodyCensus {
        self.lock().census()
    }

    pub fn is_retired(&self, key: &EndpointKey) -> bool {
        self.lock()
            .endpoints
            .get(key)
            .is_some_and(|endpoint| endpoint.retired)
    }

    /// Whether `connection`'s own thread requests named this live endpoint.
    pub fn is_used_by(&self, key: &EndpointKey, connection: u64) -> bool {
        self.lock()
            .endpoints
            .get(key)
            .is_some_and(|endpoint| !endpoint.retired && endpoint.users.contains(&connection))
    }

    /// Records that `connection` names a literal loopback endpoint in a thread
    /// request. Called at request intake, before the thread can connect.
    /// `Retired` must refuse the request. `CapacityExhausted` leaves the
    /// endpoint untracked, so it can never be declared: honest, not silent.
    pub fn track_override(
        &self,
        key: EndpointKey,
        connection: u64,
    ) -> Result<(), HostedEndpointRefusal> {
        let mut state = self.lock();
        if let Some(endpoint) = state.endpoints.get_mut(&key) {
            if endpoint.retired {
                return Err(HostedEndpointRefusal::Retired);
            }
            endpoint.users.insert(connection);
            return Ok(());
        }
        let provisional = state
            .endpoints
            .values()
            .filter(|endpoint| endpoint.is_provisional())
            .count();
        let introduced_here = state
            .endpoints
            .values()
            .filter(|endpoint| endpoint.is_provisional() && endpoint.users.contains(&connection))
            .count();
        if provisional >= HOSTED_ENDPOINT_PROVISIONAL_CAPACITY
            || introduced_here >= HOSTED_ENDPOINT_PROVISIONAL_PER_CONNECTION
        {
            return Err(HostedEndpointRefusal::CapacityExhausted);
        }
        let possibly_untracked = state.sightings_overflowed
            || state
                .untracked_sightings
                .iter()
                .any(|seen| seen.same_url(&key))
            || state.endpoints.iter().any(|(other_key, endpoint)| {
                other_key.same_url(&key)
                    && endpoint
                        .owners
                        .iter()
                        .any(|owner| !owner.released_with_proof())
            });
        state.untracked_sightings.remove(&key);
        state.endpoints.insert(
            key,
            Endpoint {
                ever_declared: false,
                capability: None,
                users: HashSet::from([connection]),
                declarers: HashSet::new(),
                owners: Vec::new(),
                retired: false,
                possibly_untracked,
            },
        );
        Ok(())
    }

    /// Records one exact connection owner before its first launch, atomically
    /// with retirement. A refused owner must close without launching.
    pub(crate) fn admit(
        &self,
        key: EndpointKey,
        thread_id: Option<String>,
        lower: &RmcpClientRetirement,
        loopback: bool,
    ) -> Result<(), HostedEndpointRefusal> {
        let mut state = self.lock();
        if state
            .endpoints
            .get(&key)
            .is_some_and(|endpoint| endpoint.retired)
        {
            return Err(HostedEndpointRefusal::Retired);
        }
        // Another Authorization identity on the same URL is not ours to close.
        // Keep that missing scope explicit instead of reporting a false clean
        // endpoint census. This does not grant authority over the other lower.
        if loopback {
            for (other_key, endpoint) in &mut state.endpoints {
                if *other_key != key && other_key.same_url(&key) {
                    endpoint.possibly_untracked = true;
                }
            }
        }
        if let Some(endpoint) = state.endpoints.get_mut(&key) {
            if endpoint.retired {
                return Err(HostedEndpointRefusal::Retired);
            }
            endpoint.owners.retain(|owner| !owner.released_with_proof());
            let weak = lower.downgrade();
            if endpoint
                .owners
                .iter()
                .any(|owner| owner.lower.same_registry(&weak))
            {
                return Ok(());
            }
            if endpoint.owners.len() >= HOSTED_ENDPOINT_OWNER_CAPACITY {
                return Err(HostedEndpointRefusal::CapacityExhausted);
            }
            endpoint.owners.push(Owner {
                thread_id,
                lower: lower.downgrade(),
                proof: lower.retirement_proof(),
            });
        } else if loopback {
            if state.untracked_sightings.len() < UNTRACKED_SIGHTING_CAPACITY {
                state.untracked_sightings.insert(key);
            } else if !state.untracked_sightings.contains(&key) {
                state.sightings_overflowed = true;
            }
        }
        Ok(())
    }

    /// Capture attribution before starting a challenge. Reset revokes all
    /// outstanding tickets, even if the same connection later reattributes.
    pub fn begin_declare(
        &self,
        key: EndpointKey,
        connection: u64,
    ) -> Result<HostedEndpointDeclaration, HostedEndpointRefusal> {
        let state = self.lock();
        let generation = state.generation.ok_or(HostedEndpointRefusal::NotOwner)?;
        let endpoint = state
            .endpoints
            .get(&key)
            .ok_or(HostedEndpointRefusal::NotOwner)?;
        if endpoint.retired {
            return Err(HostedEndpointRefusal::Retired);
        }
        if !endpoint.users.contains(&connection) {
            return Err(HostedEndpointRefusal::NotOwner);
        }
        Ok(HostedEndpointDeclaration {
            owner: Arc::downgrade(&self.state),
            generation,
            key,
            connection,
        })
    }

    /// Commit only after the endpoint proved possession of the capability.
    /// Ticket validation and publication share the reset/tombstone mutex.
    pub fn declare(
        &self,
        ticket: HostedEndpointDeclaration,
        capability: CapabilityDigest,
    ) -> Result<HostedEndpointCustodyCensus, HostedEndpointRefusal> {
        if !ticket.owner.ptr_eq(&Arc::downgrade(&self.state)) {
            return Err(HostedEndpointRefusal::NotOwner);
        }
        let mut state = self.lock();
        if state.generation != Some(ticket.generation) {
            return Err(HostedEndpointRefusal::NotOwner);
        }
        let HostedEndpointDeclaration {
            key, connection, ..
        } = ticket;
        let committed = state
            .endpoints
            .values()
            .filter(|endpoint| endpoint.retired || endpoint.capability.is_some())
            .count();
        let capacity = state.capacity;
        let Some(endpoint) = state.endpoints.get_mut(&key) else {
            return Err(HostedEndpointRefusal::NotOwner);
        };
        if endpoint.retired {
            return Err(HostedEndpointRefusal::Retired);
        }
        if !endpoint.users.contains(&connection) {
            return Err(HostedEndpointRefusal::NotOwner);
        }
        match endpoint.capability {
            Some(existing) if !existing.matches(&capability) => {
                return Err(HostedEndpointRefusal::NotOwner);
            }
            Some(_) => {}
            None if committed >= capacity => {
                return Err(HostedEndpointRefusal::CapacityExhausted);
            }
            None => endpoint.capability = Some(capability),
        }
        endpoint.ever_declared = true;
        endpoint.declarers.insert(connection);
        Ok(state.census())
    }

    #[cfg(test)]
    pub(crate) fn declare_for_test(
        &self,
        key: EndpointKey,
        capability: CapabilityDigest,
        connection: u64,
    ) -> Result<HostedEndpointCustodyCensus, HostedEndpointRefusal> {
        let ticket = self.begin_declare(key, connection)?;
        self.declare(ticket, capability)
    }

    /// Called only after account work and thread construction have drained,
    /// before account-work admission reopens. Keep tombstones and unknown
    /// physical owners; no old user, declarer or capability carries forward.
    pub fn reset_for_account_transition(&self) {
        let mut state = self.lock();
        state.generation = state.generation.and_then(|value| value.checked_add(1));
        state.endpoints.retain(|_, endpoint| {
            endpoint.users.clear();
            endpoint.declarers.clear();
            endpoint.capability = None;
            endpoint.retired
                || endpoint
                    .owners
                    .iter()
                    .any(|owner| !owner.released_with_proof())
        });
    }

    /// Drops `connection`'s use and declarations. Tombstones are kept forever.
    /// A declared endpoint is forgotten only when nobody uses or declared it and
    /// every owner left with terminal proof. A provisional endpoint is forgotten
    /// once nobody uses it and no owner is still alive. A formerly declared
    /// endpoint retains unknown evidence across account reset as well.
    pub fn release_connection(&self, connection: u64) {
        let mut state = self.lock();
        state.endpoints.retain(|_, endpoint| {
            endpoint.users.remove(&connection);
            endpoint.declarers.remove(&connection);
            if endpoint.retired || !endpoint.users.is_empty() || !endpoint.declarers.is_empty() {
                return true;
            }
            if endpoint.ever_declared {
                !endpoint.owners.iter().all(Owner::released_with_proof)
            } else {
                endpoint
                    .owners
                    .iter()
                    .any(|owner| !owner.lower.is_dropped())
            }
        });
    }

    /// Tombstones `key`, then observes every recorded owner under
    /// `min(deadline, the owner's latched deadline)`. A latched deadline is
    /// retained even when it has expired, which then drives no cleanup.
    /// Unproven owners stay recorded, so a later call reports them again.
    pub async fn retire(
        &self,
        key: EndpointKey,
        capability: CapabilityDigest,
        connection: u64,
        deadline: Instant,
    ) -> Result<HostedEndpointRetirementCensus, HostedEndpointRefusal> {
        let (targets, proven_before_retirement, unknown, possibly_untracked) = {
            let mut state = self.lock();
            let Some(endpoint) = state.endpoints.get_mut(&key) else {
                return Err(HostedEndpointRefusal::NotOwner);
            };
            let owns = endpoint.declarers.contains(&connection)
                && endpoint
                    .capability
                    .is_some_and(|declared| declared.matches(&capability));
            if !owns {
                return Err(HostedEndpointRefusal::NotOwner);
            }
            endpoint.retired = true;
            let mut targets = Vec::new();
            let mut proven = 0;
            let mut unknown = Vec::new();
            for owner in &endpoint.owners {
                match owner.lower.upgrade() {
                    Some(lower) => {
                        targets.push((owner.thread_id.clone(), owner.lower.clone(), lower));
                    }
                    None if owner.proof.is_proven() => proven += 1,
                    None => unknown.push(owner.thread_id.clone()),
                }
            }
            (targets, proven, unknown, endpoint.possibly_untracked)
        };
        let reports = futures::future::join_all(
            targets
                .iter()
                .map(|(_, _, lower)| lower.shutdown_within_latched(deadline)),
        )
        .await;
        let mut receipts = Vec::with_capacity(targets.len());
        let mut proven_now = Vec::new();
        for ((thread_id, weak, _lower), report) in targets.into_iter().zip(reports) {
            if report.is_complete() {
                proven_now.push(weak);
            }
            receipts.push(HostedEndpointConnectionReceipt { thread_id, report });
        }
        let mut state = self.lock();
        if let Some(endpoint) = state.endpoints.get_mut(&key) {
            endpoint.owners.retain(|owner| {
                !owner.released_with_proof()
                    && !proven_now
                        .iter()
                        .any(|done| done.same_registry(&owner.lower))
            });
        }
        Ok(HostedEndpointRetirementCensus {
            receipts,
            proven_before_retirement,
            unknown,
            possibly_untracked,
            custody: state.census(),
        })
    }
}

#[cfg(test)]
#[path = "hosted_endpoint_custody_tests.rs"]
mod tests;
