# Integration Guide

This guide covers the crate surface that exists in this repository today: built-in TLS bootstrap, peer-aware lookup, and DNS-aware reconnect behavior.

## 1. Bootstrap a node

Use `GossipRegistryHandle::new_with_transport_stack(...)` with the built-in `BuilderTlsBootstrap`.

```toml
[dependencies]
icanact-remote = "*"
bytes = "1"
tokio = { version = "1", features = ["full"] }
```

```rust
use icanact_remote::{
    BuilderTlsBootstrap, GossipConfig, GossipRegistryHandle, SecretKey,
};

async fn make_node() -> icanact_remote::Result<GossipRegistryHandle> {
    GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        SecretKey::generate(),
        Some(GossipConfig::default()),
        BuilderTlsBootstrap,
    )
    .await
}
```

If you need deterministic identities in tests, use `KeyPair::new_for_testing(...)` and pass `keypair.to_secret_key()`.

## 2. Connect peers and discover an actor

```rust
use bytes::Bytes;
use icanact_remote::{BuilderTlsBootstrap, GossipConfig, GossipRegistryHandle, KeyPair, RegistrationPriority};
use tokio::time::{sleep, Duration};

async fn remote_actor_flow() -> icanact_remote::Result<()> {
    let key_a = KeyPair::new_for_testing("node-a");
    let key_b = KeyPair::new_for_testing("node-b");
    let peer_b_id = key_b.peer_id();

    let a = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        key_a.to_secret_key(),
        Some(GossipConfig::default()),
        BuilderTlsBootstrap,
    )
    .await?;

    let b = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        key_b.to_secret_key(),
        Some(GossipConfig::default()),
        BuilderTlsBootstrap,
    )
    .await?;

    let peer_b = a.add_peer(&peer_b_id).await;
    peer_b.connect(&b.registry.bind_addr).await?;

    b.register_urgent(
        "echo_service".to_string(),
        b.registry.bind_addr,
        RegistrationPriority::Immediate,
    )
    .await?;

    sleep(Duration::from_millis(200)).await;

    let peer_ref = a.lookup_peer(&peer_b_id).await?;
    peer_ref.tell(Bytes::from_static(b"ping")).await?;

    let actor_ref = a
        .lookup("echo_service")
        .await
        .ok_or_else(|| icanact_remote::GossipError::ActorNotFound("echo_service".into()))?;

    actor_ref.tell(Bytes::from_static(b"hello")).await?;
    let _response = actor_ref.ask(Bytes::from_static(b"request")).await?;

    a.shutdown().await;
    b.shutdown().await;
    Ok(())
}
```

## 3. Implementing a custom bootstrap

If you need a different transport bootstrap, implement `RegistryTransportBootstrap`.

Required methods:

- `stack_name()`
- `prepare_config(secret_key, config)`
- `configure_registry(registry, secret_key)`

The built-in TLS bootstrap is implemented by `BuilderTlsBootstrap` in `src/handle_builder.rs` and is the best reference for matching the current code.

## 4. DNS refresh for moving peers

The registry supports swapping in a custom `DnsResolver`, and reconnect paths can refresh DNS after dial failures.

Relevant APIs:

- `registry.set_dns_resolver(...)`
- `handle.set_peer_dns_name(peer_addr, dns_name)`
- `registry.connect_to_peer(&peer_id)`

The test file `tests/dns_refresh_on_dial_failure.rs` shows the current end-to-end behavior.

## 5. Rules for generated code

- Prefer `lookup_peer(&PeerId)` when identity matters.
- Use `lookup(...)` when actor-name discovery is the right abstraction.
- Treat `RemoteActorRef` as the send surface: `tell`, `tell_bytes`, `ask`, `ask_deferred`, typed variants, and streaming helpers.
- Keep `GossipConfig.key_pair` aligned with the `SecretKey` used to bootstrap the node.
- Use `shutdown()` for clean teardown in examples and tests.
