//! Connection-owned authority over MCP endpoints hosted by an app-server
//! client (for example the TUI's task-tools server).
//!
//! Flow:
//! 1. **Attribution at intake.** When a connection's own `thread/start`,
//!    `thread/resume`, or `thread/fork` names a literal loopback endpoint in its
//!    overrides, custody starts tracking that endpoint for this connection. This
//!    happens synchronously at dispatch, before the thread can connect.
//! 2. **Declaration.** The same connection declares the endpoint with a
//!    separate retirement capability. The server never probes an endpoint this
//!    connection has not named. Declaration is new outbound work, so it needs:
//!    - the account permit and finite turn admission (dispatcher);
//!    - an open connection;
//!    - network-policy authorization of the exact challenge URL;
//!    - per-connection and global rate and concurrency limits.
//!
//!    It then sends one bounded, proxy-free, redirect-free POST to the endpoint's
//!    own literal loopback origin. The endpoint must answer with an HMAC over a
//!    fresh nonce, keyed by the capability.
//! 3. **Retirement** requires the same capability on a declaring connection.
//!    It is teardown and runs under the caller's remaining budget.
//!
//! The configured `Authorization` value only identifies connections, because it
//! may be shared, inherited, or used on others' behalf.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;

use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::McpHostedEndpointCustodyCensus;
use codex_app_server_protocol::McpHostedEndpointDeclareParams;
use codex_app_server_protocol::McpHostedEndpointDeclareResponse;
use codex_app_server_protocol::McpHostedEndpointRefusalReason;
use codex_app_server_protocol::McpHostedEndpointRetireCensus;
use codex_app_server_protocol::McpHostedEndpointRetireParams;
use codex_app_server_protocol::McpHostedEndpointRetireResponse;
use codex_app_server_protocol::McpHostedEndpointRetirementOutcome;
use codex_app_server_protocol::McpHostedEndpointThreadReceipt;
use codex_exec_server::EnvironmentManager;
use codex_mcp::HostedEndpointCustody;
use codex_mcp::HostedEndpointCustodyCensus;
use codex_mcp::HostedEndpointRefusal;
use codex_mcp::HostedEndpointRetirementCensus;
use codex_rmcp_client::CapabilityDigest;
use codex_rmcp_client::EndpointKey;
use codex_rmcp_client::LoopbackEndpoint;
use codex_rmcp_client::PhysicalRetirementOutcome;
use codex_rmcp_client::PhysicalRetirementReport;
use serde_json::Value;
use tokio::sync::Semaphore;
use tokio::time::Instant;
use uuid::Uuid;

use crate::error_code::invalid_request;
use crate::outgoing_message::ConnectionId;

/// Upper clamp for a caller-supplied retirement budget; it can only shorten it.
const MAX_RETIRE_BUDGET: Duration = Duration::from_secs(60);
/// Fixed bound on one ownership challenge, further clamped to the caller's budget.
const PROOF_CHALLENGE_TIMEOUT: Duration = Duration::from_secs(2);
/// A proof is 64 hex characters; anything larger is refused before decoding.
const PROOF_BODY_LIMIT: usize = 128;
const CHALLENGE_WINDOW: Duration = Duration::from_secs(60);
const CHALLENGES_PER_CONNECTION_PER_WINDOW: u32 = 4;
const CHALLENGES_PER_WINDOW: u32 = 64;
const CONCURRENT_CHALLENGES: usize = 4;

#[derive(Default)]
struct WindowCount {
    started: Option<Instant>,
    count: u32,
}

impl WindowCount {
    fn try_take(&mut self, limit: u32, now: Instant) -> bool {
        if self
            .started
            .is_none_or(|started| now.duration_since(started) >= CHALLENGE_WINDOW)
        {
            self.started = Some(now);
            self.count = 0;
        }
        if self.count >= limit {
            return false;
        }
        self.count += 1;
        true
    }
}

#[derive(Default)]
struct ChallengeLimits {
    global: WindowCount,
    connections: HashMap<u64, (WindowCount, bool)>,
}

pub(crate) struct HostedMcpEndpoints {
    custody: HostedEndpointCustody,
    environment_manager: Arc<EnvironmentManager>,
    concurrent_challenges: Arc<Semaphore>,
    limits: Mutex<ChallengeLimits>,
}

/// Releases a connection's single in-flight challenge slot, even on cancellation.
struct InFlight<'a> {
    limits: &'a Mutex<ChallengeLimits>,
    connection: u64,
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        if let Some((_, in_flight)) = self
            .limits
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .connections
            .get_mut(&self.connection)
        {
            *in_flight = false;
        }
    }
}

impl HostedMcpEndpoints {
    pub(crate) fn new(
        custody: HostedEndpointCustody,
        environment_manager: Arc<EnvironmentManager>,
    ) -> Self {
        Self {
            custody,
            environment_manager,
            concurrent_challenges: Arc::new(Semaphore::new(CONCURRENT_CHALLENGES)),
            limits: Mutex::new(ChallengeLimits::default()),
        }
    }

    fn take_challenge_slot(&self, connection: u64) -> Option<InFlight<'_>> {
        let now = Instant::now();
        let mut limits = self.limits.lock().unwrap_or_else(PoisonError::into_inner);
        let (window, in_flight) = limits.connections.entry(connection).or_default();
        if *in_flight || !window.try_take(CHALLENGES_PER_CONNECTION_PER_WINDOW, now) {
            return None;
        }
        *in_flight = true;
        if !limits.global.try_take(CHALLENGES_PER_WINDOW, now) {
            if let Some((_, in_flight)) = limits.connections.get_mut(&connection) {
                *in_flight = false;
            }
            return None;
        }
        Some(InFlight {
            limits: &self.limits,
            connection,
        })
    }

    /// `connection_closed` is sampled before any network work, and again before
    /// recording, so a lost connection is refused without a challenge.
    pub(crate) async fn declare(
        &self,
        connection_id: ConnectionId,
        params: McpHostedEndpointDeclareParams,
        connection_closed: impl Fn() -> bool,
    ) -> McpHostedEndpointDeclareResponse {
        let refused = |reason| McpHostedEndpointDeclareResponse::Refused { reason };
        if connection_closed() {
            return refused(McpHostedEndpointRefusalReason::ConnectionClosed);
        }
        if params.timeout_ms == 0 {
            return refused(McpHostedEndpointRefusalReason::Expired);
        }
        let Ok(endpoint) = LoopbackEndpoint::parse(&params.url) else {
            return refused(McpHostedEndpointRefusalReason::NotLoopback);
        };
        let key = EndpointKey::for_streamable_http(&params.url, &params.authorization);
        if self.custody.is_retired(&key) {
            return refused(McpHostedEndpointRefusalReason::Retired);
        }
        // Bind the challenge to this runtime and account-reset generation,
        // before network work. Reattribution after reset cannot revive it.
        let ticket = match self.custody.begin_declare(key, connection_id.0) {
            Ok(ticket) => ticket,
            Err(HostedEndpointRefusal::NotOwner) => {
                return refused(McpHostedEndpointRefusalReason::NotUsed);
            }
            Err(reason) => return refused(refusal_reason(reason)),
        };
        let Some(_slot) = self.take_challenge_slot(connection_id.0) else {
            return refused(McpHostedEndpointRefusalReason::RateLimited);
        };
        let Ok(_concurrency) = Arc::clone(&self.concurrent_challenges).try_acquire_owned() else {
            return refused(McpHostedEndpointRefusalReason::RateLimited);
        };
        let proof_url = endpoint.proof_url();
        let Ok(permit) = self
            .environment_manager
            .http_client_factory()
            .network_policy()
            .acquire(&proof_url)
        else {
            return refused(McpHostedEndpointRefusalReason::NetworkDenied);
        };
        let budget = Duration::from_millis(params.timeout_ms).min(PROOF_CHALLENGE_TIMEOUT);
        let nonce = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        let returned = tokio::time::timeout(budget, request_proof(proof_url, &nonce, budget))
            .await
            .ok()
            .flatten();
        let proven = permit.check().is_ok()
            && returned.is_some_and(|returned| {
                codex_rmcp_client::hosted_endpoint_proof_matches(
                    &params.retirement_capability,
                    &nonce,
                    &params.url,
                    &returned,
                )
            });
        if !proven {
            return refused(McpHostedEndpointRefusalReason::ProofFailed);
        }
        if connection_closed() {
            return refused(McpHostedEndpointRefusalReason::ConnectionClosed);
        }
        match self
            .custody
            .declare(ticket, CapabilityDigest::of(&params.retirement_capability))
        {
            Ok(custody) => McpHostedEndpointDeclareResponse::Declared {
                custody: custody_census(custody),
            },
            Err(refusal) => refused(refusal_reason(refusal)),
        }
    }

    /// Attributes literal loopback overrides to `connection_id` and refuses
    /// overrides that name a retired endpoint. Connection admission in
    /// `codex-mcp` remains authoritative for every effective configuration.
    pub(crate) fn admit_request(
        &self,
        connection_id: ConnectionId,
        request: &ClientRequest,
    ) -> Result<(), JSONRPCErrorError> {
        let config = match request {
            ClientRequest::ThreadStart { params, .. } => params.config.as_ref(),
            ClientRequest::ThreadResume { params, .. } => params.config.as_ref(),
            ClientRequest::ThreadFork { params, .. } => params.config.as_ref(),
            _ => None,
        };
        let Some(config) = config else {
            return Ok(());
        };
        let endpoints = override_endpoints(config);
        if endpoints
            .iter()
            .any(|(key, _)| self.custody.is_retired(key))
        {
            return Err(invalid_request(
                "an MCP server override names a client-hosted endpoint that its owner retired",
            ));
        }
        for (key, url) in endpoints {
            if LoopbackEndpoint::parse(&url).is_ok() {
                // Capacity refusals leave the endpoint undeclarable, which is
                // reported honestly at declaration; they never fail the thread.
                let _ = self.custody.track_override(key, connection_id.0);
            }
        }
        Ok(())
    }

    pub(crate) async fn retire(
        &self,
        connection_id: ConnectionId,
        params: McpHostedEndpointRetireParams,
    ) -> McpHostedEndpointRetireResponse {
        // An expired budget is refused before any cleanup is driven.
        if params.timeout_ms == 0 {
            return McpHostedEndpointRetireResponse::Refused {
                reason: McpHostedEndpointRefusalReason::Expired,
            };
        }
        // Server-local reconstruction of the caller's deadline: arrival plus
        // the remaining budget, clamped, never extended.
        let deadline =
            Instant::now() + Duration::from_millis(params.timeout_ms).min(MAX_RETIRE_BUDGET);
        let key = EndpointKey::for_streamable_http(&params.url, &params.authorization);
        match self
            .custody
            .retire(
                key,
                CapabilityDigest::of(&params.retirement_capability),
                connection_id.0,
                deadline,
            )
            .await
        {
            Ok(census) => retire_response(census),
            Err(refusal) => McpHostedEndpointRetireResponse::Refused {
                reason: refusal_reason(refusal),
            },
        }
    }

    pub(crate) fn connection_closed(&self, connection_id: ConnectionId) {
        self.custody.release_connection(connection_id.0);
        let mut limits = self.limits.lock().unwrap_or_else(PoisonError::into_inner);
        let in_flight = limits
            .connections
            .get(&connection_id.0)
            .is_some_and(|(_, in_flight)| *in_flight);
        // An in-flight challenge clears its own slot when it ends.
        if !in_flight {
            limits.connections.remove(&connection_id.0);
        }
    }
}

/// One bounded POST: direct (no proxy), no redirects, exactly 200, and at most
/// `PROOF_BODY_LIMIT` body bytes read before decoding.
async fn request_proof(proof_url: url::Url, nonce: &str, budget: Duration) -> Option<String> {
    let client = codex_http_client::HttpClientBuilder::new()
        .without_redirects()
        .build_direct()
        .ok()?;
    let mut response = client
        .post(proof_url)
        .header("content-type", "text/plain")
        .timeout(budget)
        .body(nonce.to_owned())
        .send()
        .await
        .ok()?;
    if response.status().as_u16() != 200 {
        return None;
    }
    if response
        .content_length()
        .is_some_and(|length| length > PROOF_BODY_LIMIT as u64)
    {
        return None;
    }
    let mut body = Vec::with_capacity(PROOF_BODY_LIMIT);
    while let Some(chunk) = response.chunk().await.ok()? {
        if body.len() + chunk.len() > PROOF_BODY_LIMIT {
            return None;
        }
        body.extend_from_slice(&chunk);
    }
    String::from_utf8(body).ok()
}

fn refusal_reason(refusal: HostedEndpointRefusal) -> McpHostedEndpointRefusalReason {
    match refusal {
        HostedEndpointRefusal::NotOwner => McpHostedEndpointRefusalReason::NotOwner,
        HostedEndpointRefusal::Retired => McpHostedEndpointRefusalReason::Retired,
        HostedEndpointRefusal::CapacityExhausted => {
            McpHostedEndpointRefusalReason::CapacityExhausted
        }
    }
}

fn saturating_u32(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn custody_census(census: HostedEndpointCustodyCensus) -> McpHostedEndpointCustodyCensus {
    McpHostedEndpointCustodyCensus {
        declared: saturating_u32(census.declared),
        provisional: saturating_u32(census.provisional),
        tombstones: saturating_u32(census.tombstones),
        capacity: saturating_u32(census.capacity),
    }
}

/// Endpoints (key and raw URL) named by `mcp_servers` overrides, in the nested
/// (`mcp_servers` / `mcp_servers.<name>`) or the leaf-dotted form.
fn override_endpoints(config: &HashMap<String, Value>) -> Vec<(EndpointKey, String)> {
    // These are authority attributions, not a second configuration loader.
    // Conflicting nested/dotted sources yield no attribution, rather than
    // choosing a winner based on HashMap iteration order. Physical admission
    // still enforces tombstones and records untracked connections.
    let mut servers: std::collections::BTreeMap<String, (Value, bool)> =
        std::collections::BTreeMap::new();
    for (path, value) in config {
        let segments = path.split('.').collect::<Vec<_>>();
        if segments.first() != Some(&"mcp_servers") {
            continue;
        }
        match &segments[1..] {
            [] => {
                if let Some(all) = value.as_object() {
                    for (name, server) in all {
                        merge_override(&mut servers, name, &[], server);
                    }
                }
            }
            [name, rest @ ..] => merge_override(&mut servers, name, rest, value),
        }
    }
    servers
        .into_values()
        .filter_map(|(server, ambiguous)| {
            if ambiguous {
                return None;
            }
            let url = server.get("url")?.as_str()?;
            let headers = server.get("http_headers")?.as_object()?;
            let headers = headers
                .iter()
                .map(|(name, value)| Some((name.as_str(), value.as_str()?)))
                .collect::<Option<Vec<_>>>()?;
            Some((
                EndpointKey::from_http_headers(url, headers)?,
                url.to_owned(),
            ))
        })
        .collect()
}

fn merge_override(
    servers: &mut std::collections::BTreeMap<String, (Value, bool)>,
    name: &str,
    path: &[&str],
    value: &Value,
) {
    let (server, ambiguous) = servers
        .entry(name.to_owned())
        .or_insert_with(|| (Value::Object(Default::default()), false));
    let mut contribution = value.clone();
    for field in path.iter().rev() {
        contribution = Value::Object(serde_json::Map::from_iter([(
            (*field).to_owned(),
            contribution,
        )]));
    }
    *ambiguous |= !merge_unambiguous(server, &contribution);
}

fn merge_unambiguous(target: &mut Value, source: &Value) -> bool {
    match (target, source) {
        (Value::Object(target), Value::Object(source)) => {
            let mut unambiguous = true;
            for (field, value) in source {
                if let Some(previous) = target.get_mut(field) {
                    unambiguous &= merge_unambiguous(previous, value);
                } else {
                    target.insert(field.clone(), value.clone());
                }
            }
            unambiguous
        }
        (target, source) => target == source,
    }
}

fn classify(report: &PhysicalRetirementReport) -> McpHostedEndpointRetirementOutcome {
    if report.is_complete() {
        return McpHostedEndpointRetirementOutcome::Retired;
    }
    let outcomes = || report.attempts.iter().map(|(_, outcome)| *outcome);
    if outcomes().any(|outcome| {
        matches!(
            outcome,
            PhysicalRetirementOutcome::LogicalFailed | PhysicalRetirementOutcome::ProcessFailed
        )
    }) {
        McpHostedEndpointRetirementOutcome::Failed
    } else if outcomes().any(|outcome| outcome == PhysicalRetirementOutcome::TimedOut) {
        McpHostedEndpointRetirementOutcome::TimedOut
    } else {
        McpHostedEndpointRetirementOutcome::Incomplete
    }
}

fn severity(outcome: McpHostedEndpointRetirementOutcome) -> u8 {
    match outcome {
        McpHostedEndpointRetirementOutcome::Retired => 0,
        McpHostedEndpointRetirementOutcome::Incomplete => 1,
        McpHostedEndpointRetirementOutcome::TimedOut => 2,
        McpHostedEndpointRetirementOutcome::Failed => 3,
    }
}

fn add_receipt(
    receipts: &mut Vec<McpHostedEndpointThreadReceipt>,
    thread_id: &Option<String>,
    outcome: McpHostedEndpointRetirementOutcome,
) {
    match receipts
        .iter_mut()
        .find(|receipt| &receipt.thread_id == thread_id)
    {
        Some(receipt) => {
            receipt.connections = receipt.connections.saturating_add(1);
            if severity(outcome) > severity(receipt.outcome) {
                receipt.outcome = outcome;
            }
        }
        None => receipts.push(McpHostedEndpointThreadReceipt {
            thread_id: thread_id.clone(),
            connections: 1,
            outcome,
        }),
    }
}

/// One receipt per thread with its worst evidence. An owner that vanished
/// without terminal proof is unknown evidence and counts as incomplete.
fn retire_response(census: HostedEndpointRetirementCensus) -> McpHostedEndpointRetireResponse {
    let mut receipts = Vec::new();
    for connection in &census.receipts {
        add_receipt(
            &mut receipts,
            &connection.thread_id,
            classify(&connection.report),
        );
    }
    for thread_id in &census.unknown {
        add_receipt(
            &mut receipts,
            thread_id,
            McpHostedEndpointRetirementOutcome::Incomplete,
        );
    }
    let mut summary = McpHostedEndpointRetireCensus {
        proven_before_retirement: saturating_u32(census.proven_before_retirement),
        unknown: saturating_u32(census.unknown.len()),
        possibly_untracked: census.possibly_untracked,
        custody: custody_census(census.custody),
        ..Default::default()
    };
    for receipt in &receipts {
        let counter = match receipt.outcome {
            McpHostedEndpointRetirementOutcome::Retired => &mut summary.retired,
            McpHostedEndpointRetirementOutcome::Incomplete => &mut summary.incomplete,
            McpHostedEndpointRetirementOutcome::TimedOut => &mut summary.timed_out,
            McpHostedEndpointRetirementOutcome::Failed => &mut summary.failed,
        };
        *counter = counter.saturating_add(1);
    }
    if census.is_complete() {
        McpHostedEndpointRetireResponse::Retired {
            receipts,
            census: summary,
        }
    } else {
        McpHostedEndpointRetireResponse::Incomplete {
            receipts,
            census: summary,
        }
    }
}

#[cfg(test)]
#[path = "hosted_mcp_endpoints_tests.rs"]
mod tests;
