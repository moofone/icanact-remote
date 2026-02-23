# LLM Integration Guide: Remote Actor Handle + TLS Transport

This is the minimum you need to wire remote actor calls and implement a transport bootstrap.

## 1. Use the right integration point

Use `GossipRegistryHandle::new_with_transport_stack(...)` with a concrete transport stack.

For TLS, use `icanact_remote_transports::TcpTlsStack`.

```toml
[dependencies]
icanact-remote = "*"
icanact-remote-transports = "*"
bytes = "1"
```

## 2. Bootstrap two TLS nodes

```rust
use icanact_remote::{GossipConfig, GossipRegistryHandle, KeyPair, RegistrationPriority};
use icanact_remote_transports::{tls, TcpTlsStack};
use std::time::Duration;

async fn make_node(seed: &str) -> icanact_remote::Result<
    (icanact_remote::KeyPair, GossipRegistryHandle<TcpTlsStack>)
> {
    tls::ensure_crypto_provider();

    let keypair = KeyPair::new_for_testing(seed);
    let mut cfg = GossipConfig::default();
    cfg.key_pair = Some(keypair.clone());
    cfg.gossip_interval = Duration::from_millis(100);

    let handle = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse().unwrap(),
        keypair.to_secret_key(),
        Some(cfg),
        TcpTlsStack,
    )
    .await?;

    Ok((keypair, handle))
}
```

## 3. Connect peers, then create a remote actor handle

```rust
use bytes::Bytes;

async fn remote_actor_flow() -> icanact_remote::Result<()> {
    let (_kp_a, a) = make_node("node-a").await?;
    let (kp_b, b) = make_node("node-b").await?;

    let peer_b_id = kp_b.peer_id();

    // Register a remote actor on node B.
    b.register_urgent(
        "echo_service".to_string(),
        b.registry.bind_addr,
        RegistrationPriority::Immediate,
    )
    .await?;

    // Connect A -> B with expected peer identity.
    let peer_b = a.add_peer(&peer_b_id).await;
    peer_b.connect(&b.registry.bind_addr).await?;

    // Preferred for secure direct peer traffic.
    let peer_ref = a.lookup_peer(&peer_b_id).await?;
    peer_ref.tell(Bytes::from_static(b"ping")).await?;

    // Actor-name lookup path (returns RemoteActorRef with cached connection).
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

## 4. Transport trait implementation you actually need

Implement `RegistryTransportBootstrap` for your transport stack type.

Required methods:
- `stack_name()`
- `prepare_config(secret_key, config)`
- `configure_registry(registry, secret_key)`

TLS example:

```rust
use icanact_remote::{
    GossipConfig, GossipError, Result, SecretKey,
    registry::GossipRegistry,
    transport::RegistryTransportBootstrap,
};

#[derive(Debug, Clone, Copy, Default)]
pub struct MyTlsStack;

impl RegistryTransportBootstrap for MyTlsStack {
    fn stack_name(&self) -> &'static str {
        "my-tcp+tls"
    }

    fn prepare_config(&self, secret_key: &SecretKey, config: &mut GossipConfig) -> Result<()> {
        let derived = secret_key.to_keypair();
        match config.key_pair.as_ref() {
            Some(existing) if existing.peer_id() != derived.peer_id() => {
                Err(GossipError::InvalidKeyPair(
                    "GossipConfig.key_pair does not match transport secret key".into(),
                ))
            }
            Some(_) => Ok(()),
            None => {
                config.key_pair = Some(derived);
                Ok(())
            }
        }
    }

    fn configure_registry(&self, registry: &mut GossipRegistry, secret_key: SecretKey) -> Result<()> {
        registry.enable_tls(secret_key)
    }
}
```

## 5. Rules to keep your LLM-generated code correct

- Prefer `lookup_peer(&PeerId)` over `lookup_address(...)` when identity matters.
- Do not call private connection APIs; use `lookup(...)`/`lookup_peer(...)` to get `RemoteActorRef`.
- `RemoteActorRef` is the send surface: `tell(...)`, `ask(...)`, `ask_deferred(...)`, typed variants.
- Always set or derive `config.key_pair` from the same `SecretKey` used for transport.
- Prefer concrete stacks from `icanact_remote_transports` (for TLS: `TcpTlsStack`) over local test/bootstrap shims.
- Call `shutdown()` for clean task teardown.
