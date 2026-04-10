# Key Management Guide

This guide describes the key types that exist in this repository and how they are used with the built-in TLS bootstrap.

## Key types

- `SecretKey`: Ed25519 private key used for node identity and TLS bootstrap.
- `PublicKey`: Ed25519 public key wrapper.
- `NodeId`: type alias for `PublicKey`.
- `PeerId`: wire-facing peer identity derived from the same public key material.
- `KeyPair`: convenience type used heavily in tests and examples; convertible to `SecretKey`.

## Generate a new key

```rust
use icanact_remote::SecretKey;

let secret_key = SecretKey::generate();
let node_id = secret_key.public();

println!("NodeId: {}", node_id.fmt_short());
println!("NodeId (hex): {}", hex::encode(node_id.as_bytes()));
```

## Persist a secret key

```rust
use icanact_remote::SecretKey;
use std::fs;

let secret_key = SecretKey::generate();
fs::write("node.key", hex::encode(secret_key.to_bytes()))?;
```

On Unix-like systems, tighten permissions after writing the file.

## Load a secret key

```rust
use icanact_remote::SecretKey;

let key_hex = std::fs::read_to_string("node.key")?;
let key_bytes = hex::decode(key_hex.trim())?;
let secret_key = SecretKey::from_bytes(&key_bytes)?;
```

`SecretKey::from_bytes` expects exactly 32 bytes after decoding.

## Convert between identity types

```rust
use icanact_remote::{KeyPair, NodeId, PeerId};

let keypair = KeyPair::new_for_testing("demo");
let peer_id: PeerId = keypair.peer_id();
let node_id: NodeId = peer_id.to_node_id();

assert_eq!(peer_id.to_bytes(), *node_id.as_bytes());
```

## Start a node with a persisted key

```rust
use icanact_remote::{BuilderTlsBootstrap, GossipConfig, GossipRegistryHandle, SecretKey};
use std::net::SocketAddr;

async fn start_node(key_path: &str) -> Result<GossipRegistryHandle, Box<dyn std::error::Error>> {
    let secret_key = if std::path::Path::new(key_path).exists() {
        let key_hex = std::fs::read_to_string(key_path)?;
        let key_bytes = hex::decode(key_hex.trim())?;
        SecretKey::from_bytes(&key_bytes)?
    } else {
        let key = SecretKey::generate();
        std::fs::write(key_path, hex::encode(key.to_bytes()))?;
        key
    };

    let bind_addr: SocketAddr = "0.0.0.0:9000".parse()?;

    let handle = GossipRegistryHandle::new_with_transport_stack(
        bind_addr,
        secret_key,
        Some(GossipConfig::default()),
        BuilderTlsBootstrap,
    )
    .await?;

    Ok(handle)
}
```

## TLS verification behavior

- When the client knows the expected node identity, TLS verification can fail with `NodeId mismatch: expected X, got Y`.
- The examples under `examples/` demonstrate this behavior.
- Identity-aware peer setup is stronger than raw address-only dialing.

## Operational guidance

- Do not log private keys.
- Store secret keys with restrictive filesystem permissions.
- Use different keys for development and production.
- Treat key rotation as an identity change; peers will observe the rotated node as a different peer unless the surrounding system updates its expected identity.
