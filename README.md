# icanact-remote

`icanact_remote` is a high-throughput, transport-agnostic core for remote actor discovery and request/response (tell/ask) style messaging.

## Highlights

- High-performance ring buffer: Lock-free ring buffer for tell/ask operations with future io_uring support for Linux 5.1+.
- Compile-time transport bootstrap via `new_with_transport_stack(...)`.
- Ask/response correlation tracking and streaming response protocol for large payloads.
- Zero-copy friendly payload paths (`bytes::Bytes`, aligned buffers, pooled typed payloads).

## DNS Refresh (Kubernetes IP Churn)

If a peer is configured with a `dns_name` (for example, a StatefulSet pod DNS name),
`icanact_remote` will re-resolve DNS on dial/reconnect failure before the next attempt.

For deterministic tests, you can inject a resolver:

```rust
use std::sync::Arc;
use icanact_remote::{DnsResolver, GossipRegistryHandle, TokioDnsResolver};

// Default behavior uses Tokio DNS:
// handle.registry.set_dns_resolver(Arc::new(TokioDnsResolver::default())).await;
//
// Tests can provide a scripted resolver implementing `DnsResolver`.
```

## Transport Bootstrap

`icanact-remote` core does not ship concrete transport stacks. Use a stack from
`icanact-remote-transports` and bootstrap with `new_with_transport_stack(...)`.

```rust
use icanact_remote::{GossipConfig, GossipRegistryHandle, SecretKey};
use icanact_remote_transports::TcpTlsStack;

#[tokio::main]
async fn main() -> icanact_remote::Result<()> {
    let secret = SecretKey::generate();
    let bind_addr = "127.0.0.1:0".parse().unwrap();

    let handle = GossipRegistryHandle::new_with_transport_stack(
        bind_addr,
        secret,
        Some(GossipConfig::default()),
        TcpTlsStack::default(),
    ).await?;
    println!("listening on {}", handle.registry.bind_addr);

    handle.shutdown().await;
    Ok(())
}
```

## io_uring (Linux)

There is an `io_uring` feature flag intended for Linux 5.1+:

```bash
cargo build --features io_uring
```

This is currently a platform-specific optimization path; non-Linux platforms use the default Tokio networking path.

## Security Note

The old note about “not using proper cryptographic PeerId keys” is no longer accurate: `PeerId` is an Ed25519 public key, and TLS identities are derived from Ed25519 keys.

However, peer authentication is still intentionally permissive in places:

- Certificate parsing/identity extraction uses a simplified placeholder approach (not full X.509 parsing).
- Unless you encode an expected NodeId in SNI, the client verifier does not strictly pin the peer identity when connecting via IP.

If you need production-grade node identity verification, you should implement strict identity pinning (expected `PeerId`/`NodeId` per address) and replace the placeholder certificate parsing with proper X.509/SPKI extraction.
