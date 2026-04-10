# icanact-remote

`icanact_remote` is a Rust crate for remote actor discovery, peer connectivity, and tell/ask messaging over authenticated transports.

## What the crate provides

- `GossipRegistryHandle` for node lifecycle, peer registration, gossip, and shutdown.
- `RemoteActorRef` for cached remote sends via `tell`, `ask`, `ask_deferred`, and typed helpers.
- Built-in TLS bootstrap through `BuilderTlsBootstrap`.
- DNS refresh hooks for peer addresses that may change over time.
- Optional Linux `io_uring` writer support behind the `io_uring` feature flag.

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
    handle.shutdown().await;
    Ok(())
}
```

## Lookup model

- `lookup("actor_name")` returns `Option<RemoteActorRef>`.
- `lookup_peer(&PeerId)` returns `Result<RemoteActorRef>` and is the preferred path when peer identity matters.
- Low-level connection APIs are intentionally not public; callers should work through lookup methods.

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

## Key types

- `SecretKey` is the private Ed25519 signing key used for TLS identity.
- `PublicKey` and `NodeId` refer to the same public key type.
- `PeerId` is the wire-facing peer identity and can be converted to and from `NodeId`.
- `KeyPair` remains available, especially in tests and examples, and can be converted to `SecretKey`.

## Feature flags

- `io_uring`: enables the Linux-only `io_uring` stream writer path when supported by the target.
- `strict-zero-copy`: reserved feature flag present in `Cargo.toml`.
- `test-helpers`: exports the `test_helpers` module.

Build with `io_uring`:

```bash
cargo build --features io_uring
```

On non-Linux platforms, or without that feature, the crate uses the standard Tokio-based writer path.

## Examples and tests

- Runnable examples live under [`examples/`](./examples).
- TLS-focused walkthroughs are in [`examples/TLS_EXAMPLES.md`](./examples/TLS_EXAMPLES.md).
- API behavior around `lookup()` and `RemoteActorRef` is covered by integration tests such as `tests/test_new_lookup_api.rs`.

## Security notes

- TLS identity verification is tied to Ed25519-derived node identities.
- When the client connects with an expected node identity, the TLS verifier rejects mismatches with a `NodeId mismatch` error.
- Address-only flows are still possible in the API, so prefer peer-id-driven flows such as `add_peer(...)` plus `lookup_peer(...)` when identity pinning matters.
