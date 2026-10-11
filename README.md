# icanact-remote

`icanact_remote` is a Rust crate for remote actor discovery, peer connectivity, and tell/ask messaging over authenticated transports.

The canonical remote-transport contract is
[`docs/architecture/REMOTE_INVARIANTS.md`](docs/architecture/REMOTE_INVARIANTS.md).

The current admission and session-provenance API migrations are documented in
[REMEDIATION_API_MIGRATIONS.md](docs/REMEDIATION_API_MIGRATIONS.md). These changes
do not establish architecture or performance acceptance; the contract remains
UNPROVED pending the full host matrix and required measurements.

Deferred replies now use scoped responders or bounded, physical-session reply
endpoints; forwarding binds its destination once and queues opaque session tokens.
A replacement connection requires a new bound forwarder rather than retargeting
old jobs. See [reply ownership](docs/REPLY_OWNER_MIGRATION.md) and
[forwarding admission](docs/FORWARDER_ADMISSION.md) before migrating callers.

## What the crate provides

- `GossipRegistryHandle` for node lifecycle, peer registration, gossip, and shutdown.
- `RemoteActorRef` for route lookup and `session()` acquisition; `RemoteActorSession` for exact-session sends, scoped `ask_deferred`, and typed helpers.
- Built-in TLS bootstrap through `BuilderTlsBootstrap`.
- DNS refresh hooks for peer addresses that may change over time.

## Quick start

The default transport bootstrap shipped in this crate is `BuilderTlsBootstrap`.

```rust
use icanact_remote::{
    BuilderTlsBootstrap, GossipConfig, GossipRegistryHandle, SecretKey,
};

#[tokio::main]
async fn main() -> icanact_remote::Result<()> {
    let secret = SecretKey::generate();
    let bind_addr = "127.0.0.1:0".parse().unwrap();

    let handle = GossipRegistryHandle::new_with_transport_stack(
        bind_addr,
        secret,
        Some(GossipConfig::default()),
        BuilderTlsBootstrap,
    )
    .await?;

    println!("listening on {}", handle.registry.bind_addr);
    handle.shutdown().await?;
    Ok(())
}
```

## Lookup model

- `lookup("actor_name")` returns `Option<RemoteActorRef>`.
- `lookup_peer(&PeerId)` returns `Result<RemoteActorRef>` and is the preferred path when peer identity matters.
- Low-level connection APIs are intentionally not public; callers should work through lookup methods.

## Correlation exhaustion

A shared correlation tracker issues each nonzero `u32` wire ID at most once.
After `u32::MAX`, new asks return the existing typed correlation-capacity error
instead of wrapping and allowing a delayed reply to complete an unrelated ask.
Existing in-flight requests may still complete. The tracker stays exhausted;
releasing a slot or replacing its transport does not reset the namespace.
Creating a new registry establishes a fresh namespace. No application timeout
or namespace exhaustion is used as peer-liveness evidence.

## DNS refresh on reconnect

If a peer is configured with a DNS name, reconnect paths can re-resolve that name after dial failure.

Tests can inject a custom resolver:

```rust
use std::sync::Arc;
use icanact_remote::{DnsResolver, GossipRegistryHandle, TokioDnsResolver};

// Default behavior:
// handle.registry.set_dns_resolver(Arc::new(TokioDnsResolver::default())).await;
//
// Custom test resolvers can implement `DnsResolver`.
```

You can also associate a DNS name with a peer address through `handle.set_peer_dns_name(...)`.

## Client-only peers (push to NAT/VPN clients)

Both features below are opt-in; defaults are unchanged.

**Client-only role.** A laptop/CLI that only makes outbound connections sets
`GossipConfig { client_only: true, .. }`. It advertises no dialable address and
declares the role in its mTLS-authenticated Hello (`Feature::ClientOnly`). A
peer that learns this never dials, supervises, retries, or gossips that
identity as a dial target (any fresh dial toward it fails with
`GossipError::ClientOnlyPeer`), so a LAN server does not dial back toward
laptops or log "required peer unreachable" for them. While the client is
connected the server can still `tell`/`ask` its actors and send replies over
the client-initiated session (`client().lookup_connected_peer(&peer_id)`). When
the session ends the server drops the peer's dial state, fenced by that exact
session. The client keeps its configured-peer supervisor (and DNS refresh) and
re-subscribes from the existing `PeerConnectHandler` / `PeerDisconnectHandler`
callbacks. The role is bound to the authenticated `PeerId`, never an address.
`Feature::ClientOnly` is a new Hello variant: enable `client_only` only against
servers that have this version.

**Inbound allowlist.** `GossipConfig { inbound_peer_allowlist: Some(list), .. }`
rejects, during the TLS handshake, any client whose authenticated `PeerId` is
not in the `PeerAllowlist` (typed `tls::allowlist::PeerNotAllowed`, recoverable
from the accept error with `tls::allowlist::peer_not_allowed`). Rejections log
at most one line per identity per 60 s. Diagnostic dedupe retains up to 1024
recent identities; while all windows are recent, new identities are rejected
silently until capacity expires. Churn cannot reset existing suppression, and
this diagnostic policy never changes handshake rejection. Keep a clone of the list and call
`list.replace(new_peer_ids)` to swap the whole set atomically at runtime; it
applies to new handshakes. Existing sessions are not torn down by a swap.

See `examples/client_only_push_{server,client}.rs` (run the server first), and
the `tests/client_only_peer_e2e.rs` / `tests/peer_allowlist_e2e.rs` suites.

## Key types

- `SecretKey` is the private Ed25519 signing key used for TLS identity.
- `PublicKey` and `NodeId` refer to the same public key type.
- `PeerId` is the wire-facing peer identity and can be converted to and from `NodeId`.
- `KeyPair` remains available, especially in tests and examples, and can be converted to `SecretKey`.

## Feature flags

- `strict-zero-copy`: reserved feature flag present in `Cargo.toml`.
- `test-helpers`: exports the `test_helpers` module.

The crate uses the Tokio-based writer path.

## Examples and tests

- Runnable examples live under [`examples/`](./examples).
- TLS-focused walkthroughs are in [`examples/TLS_EXAMPLES.md`](./examples/TLS_EXAMPLES.md).
- API behavior around `lookup()` and `RemoteActorRef` is covered by integration tests such as `tests/test_new_lookup_api.rs`.

## Security notes

- TLS identity verification is tied to Ed25519-derived node identities.
- When the client connects with an expected node identity, the TLS verifier rejects mismatches with a `NodeId mismatch` error.
- Address-only flows are still possible in the API, so prefer peer-id-driven flows such as `add_peer(...)` plus `lookup_peer(...)` when identity pinning matters.
