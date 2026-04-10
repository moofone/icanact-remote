# Design Notes

This file documents implementation constraints that are visible in the current repository. It is not a normative replacement for reading the code.

## Current architectural themes

- The public API is centered on `GossipRegistryHandle`, `GossipClient`, `Peer`, and `RemoteActorRef`.
- Remote sends are intended to flow through `lookup(...)`, `lookup_peer(...)`, or `lookup_address(...)`, not through public connection-pool APIs.
- TLS bootstrap is built into this crate through `BuilderTlsBootstrap`.
- Peer identity is based on Ed25519 keys exposed as `SecretKey`, `PublicKey`/`NodeId`, `PeerId`, and `KeyPair`.

## Connection management

- `GossipRegistryHandle::get_connection(...)` is `pub(crate)`.
- `GossipRegistry::get_connection(...)` is `pub(crate)`.
- External callers are expected to use lookup methods that return `RemoteActorRef`.

That matches the integration tests in `tests/test_new_lookup_api.rs`.

## Concurrency and locking

- The codebase does use synchronization primitives internally.
- `src/registry.rs` currently includes `tokio::sync::Mutex` for registry-owned state.
- Hot-path messaging code still aims to avoid unnecessary lookup work by caching a `RemoteConnection` inside `RemoteActorRef`.

Because the implementation is mixed, broad claims such as "fully lock-free everywhere in tell/ask/streaming" would not be accurate for the repository as it stands.

## Payload and copy behavior

- The crate exposes zero-copy-friendly APIs such as `tell_bytes`, `try_tell_bytes`, aligned buffers, and typed payload helpers.
- The receive path contains alignment-aware deserialization logic.
- The repository also contains explicit copy guards and targeted tests around copy-sensitive paths.

At the same time, convenience APIs still exist, and not every path can be described as strict total zero-copy without qualification.

## Validation surface

The current validation script is `scripts/full_validation.sh`. It performs:

- workspace tests, with retries for flaky socket-heavy cases
- `rkyv::from_bytes` guard checks
- forbidden copy-pattern checks
- focused pointer-identity tests
- focused streaming tests
- optional coverage gates when a plan path is supplied

Coverage gating scripts still accept a default legacy sprint path internally, but callers should pass an explicit plan path if they want coverage checks to run against a real artifact.
