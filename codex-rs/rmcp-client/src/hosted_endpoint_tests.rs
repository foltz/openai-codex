use super::*;
use pretty_assertions::assert_eq;

#[test]
fn hmac_matches_rfc_4231_test_case_2() {
    assert_eq!(
        hex(&hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
        "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
    );
}

#[test]
fn hmac_hashes_keys_longer_than_one_block() {
    // RFC 4231 test case 6.
    let key = [0xaa_u8; 131];
    assert_eq!(
        hex(&hmac_sha256(
            &key,
            b"Test Using Larger Than Block-Size Key - Hash Key First"
        )),
        "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
    );
}

#[test]
fn endpoint_key_binds_url_and_authorization() {
    let url = "http://127.0.0.1:4000/mcp";
    assert_eq!(
        EndpointKey::for_streamable_http(url, "Bearer a"),
        EndpointKey::for_streamable_http(url, "Bearer a")
    );
    assert_ne!(
        EndpointKey::for_streamable_http(url, "Bearer a"),
        EndpointKey::for_streamable_http(url, "Bearer b")
    );
    assert_ne!(
        EndpointKey::for_streamable_http(url, "Bearer a"),
        EndpointKey::for_streamable_http("http://127.0.0.1:4001/mcp", "Bearer a")
    );
}

#[test]
fn proof_binds_capability_nonce_and_url() {
    let url = "http://127.0.0.1:4000/mcp";
    let proof = hosted_endpoint_proof("capability", "nonce", url);
    assert!(hosted_endpoint_proof_matches(
        "capability",
        "nonce",
        url,
        &proof
    ));
    assert!(!hosted_endpoint_proof_matches(
        "other", "nonce", url, &proof
    ));
    assert!(!hosted_endpoint_proof_matches(
        "capability",
        "replayed",
        url,
        &proof
    ));
    assert!(!hosted_endpoint_proof_matches(
        "capability",
        "nonce",
        "http://127.0.0.1:4001/mcp",
        &proof
    ));
    assert!(CapabilityDigest::of("capability").matches(&CapabilityDigest::of("capability")));
    assert!(!CapabilityDigest::of("capability").matches(&CapabilityDigest::of("other")));
}

#[test]
fn loopback_endpoint_accepts_only_literal_canonical_loopback_http() {
    let parsed = LoopbackEndpoint::parse("http://127.0.0.1:41000/mcp").expect("loopback");
    assert_eq!(
        parsed.proof_url().as_str(),
        "http://127.0.0.1:41000/kcf/hosted-endpoint/proof"
    );
    assert_eq!(
        LoopbackEndpoint::parse("http://[::1]:41000/mcp")
            .expect("ipv6 loopback")
            .proof_url()
            .as_str(),
        "http://[::1]:41000/kcf/hosted-endpoint/proof"
    );
    for (raw, error) in [
        // Userinfo must never redirect the challenge to another host.
        (
            "http://127.0.0.1:80@evil.example:8000/mcp",
            LoopbackEndpointError::UserInfo,
        ),
        (
            "http://user@127.0.0.1:41000/mcp",
            LoopbackEndpointError::UserInfo,
        ),
        (
            "http://:secret@127.0.0.1:41000/mcp",
            LoopbackEndpointError::UserInfo,
        ),
        ("http://evil.example:41000/mcp", LoopbackEndpointError::Host),
        ("http://localhost:41000/mcp", LoopbackEndpointError::Host),
        (
            "http://127.0.0.1.nip.io:41000/mcp",
            LoopbackEndpointError::Host,
        ),
        ("http://10.0.0.1:41000/mcp", LoopbackEndpointError::Host),
        ("http://127.0.0.2:41000/mcp", LoopbackEndpointError::Host),
        (
            "http://[::ffff:127.0.0.1]:41000/mcp",
            LoopbackEndpointError::Host,
        ),
        ("http://[fe80::1]:41000/mcp", LoopbackEndpointError::Host),
        (
            "http://[0:0:0:0:0:0:0:1]:41000/mcp",
            LoopbackEndpointError::NonCanonical,
        ),
        (
            "http://2130706433:41000/mcp",
            LoopbackEndpointError::NonCanonical,
        ),
        (
            "http://0x7f.0.0.1:41000/mcp",
            LoopbackEndpointError::NonCanonical,
        ),
        ("https://127.0.0.1:41000/mcp", LoopbackEndpointError::Scheme),
        ("http://127.0.0.1/mcp", LoopbackEndpointError::Port),
        ("http://127.0.0.1:80/mcp", LoopbackEndpointError::Port),
        (
            "http://127.0.0.1:41000/mcp?x=1",
            LoopbackEndpointError::QueryOrFragment,
        ),
        (
            "http://127.0.0.1:41000/mcp#x",
            LoopbackEndpointError::QueryOrFragment,
        ),
        ("not a url", LoopbackEndpointError::Invalid),
    ] {
        assert_eq!(LoopbackEndpoint::parse(raw), Err(error), "{raw}");
    }
}

#[test]
fn configured_header_identity_refuses_ambiguous_authorization() {
    let url = "http://127.0.0.1:4000/mcp";
    assert_eq!(
        EndpointKey::from_http_headers(
            url,
            [
                ("Authorization", "Bearer a"),
                ("Accept", "application/json")
            ]
        ),
        Some(EndpointKey::for_streamable_http(url, "Bearer a"))
    );
    for headers in [
        [("Authorization", "Bearer a"), ("authorization", "Bearer b")],
        [("authorization", "Bearer b"), ("Authorization", "Bearer a")],
        [("Authorization", "Bearer a"), ("authorization", "Bearer a")],
    ] {
        assert_eq!(EndpointKey::from_http_headers(url, headers), None);
    }
    assert_eq!(
        EndpointKey::from_http_headers(url, [("Accept", "application/json")]),
        None
    );
}
