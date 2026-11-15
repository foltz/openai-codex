# Local rmcp cleanup correction

Base: modelcontextprotocol/rust-sdk `98a54789b80110ad210495c4ad845c89e1980471`
(rmcp and rmcp-macros 3.5.1). The application manifest retains that exact pin
and overrides only these local crates. All other SDK source remains at this base.

The three-file correction is carried from KCF source `360b7705` against SDK
base `3e636cab`: transport/worker.rs, transport/streamable_http_client.rs and
transport/common/reqwest/streamable_http_client.rs. It preserves cancellable
startup/HTTP initialization, joined stream tasks, session release on exit,
tightening close deadlines and truthful terminal close/retry evidence. A failed
join or timeout is incomplete cleanup. HTTP 405 proves local release only;
404 absence is valid only for the established session identity.

The port preserves 3.5.1's body-preview, JSON-RPC error classification and
ReinitializationFailed behavior. Failed or cancelled reinitialization leaves sticky incomplete-cleanup evidence,
even after a later successful recovery. Known recovered session custody moves
before fallible response draining. Unknown sessions are not claimed deleted.
Source and integration tests remain required before acceptance.

Only the two SDK crates are copied. Inherited manifest fields are inlined from
this base (not 3.3.0); the macro dependency is explicit for path-crate tooling.
The nested workspace keeps upstream resolver 3. Upstream's two LICENSE links
resolve only to this vendor root's copied LICENSE. Use its own rustfmt config;
application formatting must not rewrite unmodified SDK source. Remove the
Cargo overrides, workspace exclusion and vendor directory together only after
equivalent upstream behavior and consumer qualification are established.
