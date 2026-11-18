//! Identity and ownership proofs for MCP endpoints hosted by an app-server client.
//!
//! Two different secrets are involved:
//! - The endpoint's configured `Authorization` value identifies its connections.
//!   It is part of thread configuration, so it can be shared with, or seen by,
//!   other parties.
//! - The retirement capability is minted separately by the hosting client. It
//!   never appears in configuration, lives only in that client's memory, and is
//!   sent only on the connection that declares the endpoint. The hosted server
//!   proves possession of it by answering a daemon nonce with an HMAC.

use sha2::Digest;
use sha2::Sha256;
use std::fmt;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use url::Host;
use url::Url;

/// Path, on the hosted endpoint's own origin, that answers ownership challenges.
pub const HOSTED_ENDPOINT_PROOF_PATH: &str = "/kcf/hosted-endpoint/proof";

const PROOF_CONTEXT: &[u8] = b"kcf-hosted-endpoint-proof-v1\0";

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum LoopbackEndpointError {
    #[error("not a valid URL")]
    Invalid,
    #[error("the scheme must be http")]
    Scheme,
    #[error("userinfo is not allowed")]
    UserInfo,
    #[error("the host must be the literal 127.0.0.1 or [::1]")]
    Host,
    #[error("an explicit, non-default port is required")]
    Port,
    #[error("a query or fragment is not allowed")]
    QueryOrFragment,
    #[error("the URL must already be in canonical form")]
    NonCanonical,
}

/// A client-hosted endpoint URL that can only ever reach this machine's
/// loopback interface: `http`, no userinfo, a literal `127.0.0.1` or `[::1]`
/// host (never a hostname, including `localhost`), an explicit port, and a
/// canonical spelling (so alternate IPv4/IPv6 encodings are refused).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoopbackEndpoint {
    url: Url,
}

impl LoopbackEndpoint {
    pub fn parse(raw: &str) -> Result<Self, LoopbackEndpointError> {
        let url = Url::parse(raw).map_err(|_| LoopbackEndpointError::Invalid)?;
        if url.scheme() != "http" {
            return Err(LoopbackEndpointError::Scheme);
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(LoopbackEndpointError::UserInfo);
        }
        match url.host() {
            Some(Host::Ipv4(ip)) if ip == Ipv4Addr::LOCALHOST => {}
            Some(Host::Ipv6(ip)) if ip == Ipv6Addr::LOCALHOST => {}
            _ => return Err(LoopbackEndpointError::Host),
        }
        if url.port().is_none() {
            return Err(LoopbackEndpointError::Port);
        }
        if url.query().is_some() || url.fragment().is_some() {
            return Err(LoopbackEndpointError::QueryOrFragment);
        }
        if url.as_str() != raw {
            return Err(LoopbackEndpointError::NonCanonical);
        }
        Ok(Self { url })
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    /// The challenge route on the same literal loopback origin.
    pub fn proof_url(&self) -> Url {
        let mut url = self.url.clone();
        url.set_path(HOSTED_ENDPOINT_PROOF_PATH);
        url
    }
}

/// Digest of one exact endpoint URL and its configured `Authorization` value.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct EndpointKey([u8; 32]);

impl EndpointKey {
    /// Extracts exactly one case-insensitive configured Authorization header.
    /// Ambiguous case-duplicate fields cannot mint hosted ownership identity.
    pub fn from_http_headers<'a>(
        url: &str,
        headers: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Option<Self> {
        let mut authorization = None;
        for (name, value) in headers {
            if name.eq_ignore_ascii_case("authorization") {
                if authorization.is_some() {
                    return None;
                }
                authorization = Some(value);
            }
        }
        authorization.map(|value| Self::for_streamable_http(url, value))
    }

    pub fn for_streamable_http(url: &str, authorization: &str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"kcf-hosted-mcp-endpoint-v1\0");
        hasher.update((url.len() as u64).to_be_bytes());
        hasher.update(url.as_bytes());
        hasher.update((authorization.len() as u64).to_be_bytes());
        hasher.update(authorization.as_bytes());
        Self(digest_bytes(hasher))
    }
}

impl fmt::Debug for EndpointKey {
    // A short prefix correlates logs without disclosing anything usable.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "EndpointKey({:02x}{:02x}{:02x}{:02x}..)",
            self.0[0], self.0[1], self.0[2], self.0[3]
        )
    }
}

/// Digest under which a retirement capability is retained. The capability
/// itself is never stored by the daemon.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct CapabilityDigest([u8; 32]);

impl CapabilityDigest {
    pub fn of(capability: &str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"kcf-hosted-endpoint-capability-v1\0");
        hasher.update(capability.as_bytes());
        Self(digest_bytes(hasher))
    }

    /// Constant-time comparison.
    pub fn matches(&self, other: &Self) -> bool {
        constant_time_eq(&self.0, &other.0)
    }
}

impl fmt::Debug for CapabilityDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CapabilityDigest(..)")
    }
}

/// Lowercase-hex HMAC-SHA256 that the hosted server returns for `nonce`.
/// Binding the declared URL prevents a proof for one endpoint from being
/// replayed for another.
pub fn hosted_endpoint_proof(capability: &str, nonce: &str, url: &str) -> String {
    let mut message = Vec::with_capacity(PROOF_CONTEXT.len() + nonce.len() + url.len() + 1);
    message.extend_from_slice(PROOF_CONTEXT);
    message.extend_from_slice(nonce.as_bytes());
    message.push(0);
    message.extend_from_slice(url.as_bytes());
    hex(&hmac_sha256(capability.as_bytes(), &message))
}

/// Constant-time check of a returned proof against the expected one.
pub fn hosted_endpoint_proof_matches(
    capability: &str,
    nonce: &str,
    url: &str,
    returned: &str,
) -> bool {
    let expected = hosted_endpoint_proof(capability, nonce, url);
    constant_time_eq(expected.as_bytes(), returned.trim().as_bytes())
}

/// RFC 2104 HMAC over SHA-256.
pub fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    const BLOCK: usize = 64;
    let mut block_key = [0_u8; BLOCK];
    if key.len() > BLOCK {
        block_key[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block_key[..key.len()].copy_from_slice(key);
    }
    let mut inner = Sha256::new();
    inner.update(block_key.map(|byte| byte ^ 0x36));
    inner.update(message);
    let inner = digest_bytes(inner);
    let mut outer = Sha256::new();
    outer.update(block_key.map(|byte| byte ^ 0x5c));
    outer.update(inner);
    digest_bytes(outer)
}

fn digest_bytes(hasher: Sha256) -> [u8; 32] {
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 32];
    bytes.copy_from_slice(&digest);
    bytes
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

#[cfg(test)]
#[path = "hosted_endpoint_tests.rs"]
mod tests;
