# Key Management Guide

This document describes how to generate, persist, and manage Ed25519 keypairs for node identity in icanact_remote.

## Overview

Each node in a gossip network has a unique identity derived from an Ed25519 keypair:

- **SecretKey**: The private signing key (32 bytes). Keep this secure and never share.
- **NodeId (PublicKey)**: The public verification key (32 bytes). This is your node's identity.

## Generating Keypairs

### New Node Identity

Generate a new random keypair for a node:

```rust
use icanact_remote::SecretKey;

// Generate a new random secret key
let secret_key = SecretKey::generate();

// Get the corresponding public key (NodeId)
let node_id = secret_key.public();

println!("Node ID: {}", node_id); // Prints abbreviated form
println!("Node ID (full): {}", hex::encode(node_id.as_bytes()));
```

## Persisting Keys

### Saving to File

Save the secret key to a file for later use:

```rust
use icanact_remote::SecretKey;
use std::fs;

let secret_key = SecretKey::generate();

// Get raw bytes and encode as hex
let key_hex = hex::encode(secret_key.to_bytes());

// Save to file with restrictive permissions
fs::write("node.key", &key_hex).expect("Failed to write key file");

// On Unix, set permissions to owner-only (chmod 600)
#[cfg(unix)]
{
    use std::os::unix::fs::PermissionsExt;
    let mut perms = fs::metadata("node.key").unwrap().permissions();
    perms.set_mode(0o600);
    fs::set_permissions("node.key", perms).expect("Failed to set permissions");
}
```

### Loading from File

Load a previously saved secret key:

```rust
use icanact_remote::SecretKey;
use std::fs;

// Read hex-encoded key from file
let key_hex = fs::read_to_string("node.key").expect("Failed to read key file");
let key_hex = key_hex.trim(); // Remove any whitespace

// Decode hex to bytes
let key_bytes = hex::decode(key_hex).expect("Invalid hex encoding");

// Create SecretKey from bytes
let secret_key = SecretKey::from_bytes(&key_bytes).expect("Invalid key bytes");

println!("Loaded node: {}", secret_key.public());
```

## Sharing Node Identity

### NodeId Format

NodeId (PublicKey) can be displayed and shared in several formats:

```rust
use icanact_remote::NodeId;

let node_id: NodeId = secret_key.public();

// Short form for logging (first 5 bytes as hex)
println!("Short: {}", node_id.fmt_short()); // e.g., "3a7bc9d1f2"

// Full hex encoding
println!("Full hex: {}", hex::encode(node_id.as_bytes()));

// Base32 encoding (used in human-readable serialization)
let base32 = data_encoding::BASE32_NOPAD.encode(node_id.as_bytes());
println!("Base32: {}", base32);
```

### Configuration File

When configuring peer addresses in config files, use the full hex or base32 encoding:

```toml
# config.toml
[[bootstrap_nodes]]
node_id = "3A7BC9D1F2E4A8B6C3D5E7F9A0B1C2D3E4F5A6B7C8D9E0F1A2B3C4D5E6F7A8B9"
address = "192.168.1.100:9000"
```

## Using with TLS

To create a TLS-enabled gossip registry with persistent keys:

```rust
use icanact_remote::{GossipConfig, GossipRegistryHandle, SecretKey};
use std::net::SocketAddr;

async fn start_node(key_path: &str) -> Result<GossipRegistryHandle, Box<dyn std::error::Error>> {
    // Load or generate key
    let secret_key = if std::path::Path::new(key_path).exists() {
        let key_hex = std::fs::read_to_string(key_path)?;
        let key_bytes = hex::decode(key_hex.trim())?;
        SecretKey::from_bytes(&key_bytes)?
    } else {
        let key = SecretKey::generate();
        std::fs::write(key_path, hex::encode(key.to_bytes()))?;
        key
    };

    println!("Node ID: {}", secret_key.public());

    let config = GossipConfig::default();
    let bind_addr: SocketAddr = "0.0.0.0:9000".parse()?;

    // Create registry with a concrete transport stack (from icanact-remote-transports)
    let handle = GossipRegistryHandle::new_with_transport_stack(
        bind_addr,
        secret_key,
        Some(config),
        icanact_remote_transports::TcpTlsStack::default(),
    )
    .await?;

    Ok(handle)
}
```

## Security Best Practices

### Key Storage

1. **File Permissions**: Always set restrictive permissions on key files (chmod 600 on Unix)
2. **Encryption at Rest**: Consider encrypting key files with a passphrase for additional security
3. **Secure Directories**: Store keys in directories with restricted access
4. **Backup**: Keep secure backups of keys; losing the key means losing the node identity

### Key Handling

1. **Never Log Keys**: Don't log full secret keys; use NodeId for identification
2. **Memory Security**: SecretKey uses `zeroize` crate to clear memory on drop
3. **No Hardcoding**: Never hardcode keys in source code
4. **Separate Environments**: Use different keys for development, staging, and production

### Test vs Production Keys

```rust
// WRONG: Don't use in production
let test_key = SecretKey::generate(); // Random key each restart

// CORRECT: Load persistent key
let prod_key = load_key_from_file("/secure/path/node.key")?;
```

### Environment Variables

For containerized deployments, keys can be passed via environment variables:

```rust
use std::env;

fn load_key_from_env() -> Result<SecretKey, Box<dyn std::error::Error>> {
    let key_hex = env::var("NODE_SECRET_KEY")?;
    let key_bytes = hex::decode(&key_hex)?;
    Ok(SecretKey::from_bytes(&key_bytes)?)
}
```

```bash
# Set key in environment (secure method)
export NODE_SECRET_KEY=$(cat /secure/path/node.key)
```

## Key Rotation

To rotate a node's identity:

1. Generate a new keypair
2. Update configuration to use new key
3. Inform peers of the new NodeId
4. Restart the node

Note: Key rotation changes the node's identity. Other nodes will see it as a new peer.

## Troubleshooting

### Invalid Key Length

```
Error: Invalid secret key length: expected 32, got N
```

Ensure the key is exactly 32 bytes. If hex-encoded, it should be 64 characters.

### Invalid Key Format

```
Error: Invalid hex: ...
```

Check that the key file contains only hex characters (0-9, a-f, A-F).

### TLS Verification Failed

```
Error: NodeId mismatch: expected X, got Y
```

The node's certificate doesn't match the expected NodeId. Verify you're connecting to the correct node.

## Example: Full Setup

```rust
use icanact_remote::{GossipConfig, GossipRegistryHandle, SecretKey};
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configuration
    let key_file = "node.key";
    let bind_addr: SocketAddr = "0.0.0.0:9000".parse()?;
    let bootstrap: Vec<SocketAddr> = vec!["192.168.1.100:9000".parse()?];

    // Load or create key
    let secret_key = if std::path::Path::new(key_file).exists() {
        println!("Loading existing key...");
        let key_hex = std::fs::read_to_string(key_file)?;
        let key_bytes = hex::decode(key_hex.trim())?;
        SecretKey::from_bytes(&key_bytes)?
    } else {
        println!("Generating new key...");
        let key = SecretKey::generate();

        // Save with secure permissions
        std::fs::write(key_file, hex::encode(key.to_bytes()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(0o600);
            std::fs::set_permissions(key_file, perms)?;
        }

        key
    };

    println!("Node ID: {}", secret_key.public());

    // Configure gossip
    let mut config = GossipConfig::default();
    config.enable_peer_discovery = true;

    // Start registry with a concrete transport stack
    let handle = GossipRegistryHandle::new_with_transport_stack(
        bind_addr,
        secret_key,
        Some(config),
        icanact_remote_transports::TcpTlsStack::default(),
    )
    .await?;

    // Bootstrap connections
    handle.bootstrap_non_blocking(bootstrap).await;

    println!("Node started on {}", bind_addr);

    // Keep running...
    tokio::signal::ctrl_c().await?;

    handle.shutdown().await;
    Ok(())
}
```

## See Also

- [GossipConfig](../src/config.rs) - Configuration options
- [TLS Module](../src/tls/mod.rs) - TLS implementation details
- [Peer Discovery](../src/peer_discovery.rs) - Automatic peer discovery
