# TLS Examples

This repository currently exposes these runnable TLS examples through Cargo:

- `tls_server`
- `tls_client`
- `test_tls_basic`
- `test_tls_gossip`
- `test_tls_with_node_id`
- `test_bidirectional_tls`
- `test_keypair_auth`
- `test_invalid_keypair`
- `test_valid_keypair`

Files under `examples/remote/` are useful reference code, but they are not registered as Cargo example targets in this checkout.

## Quick start

Run the basic TLS server:

```bash
cargo run --example tls_server 9001
```

Then connect with the client:

```bash
cargo run --example tls_client 9001 /tmp/icanact_tls_server.pub
```

The server example writes its public key to `/tmp/icanact_tls_server.pub` by default.

## Authentication failure check

You can intentionally force an identity mismatch:

```bash
cargo run --example tls_client 9001 /tmp/icanact_tls_server.pub --wrong-key
```

That path is expected to fail with a `NodeId mismatch` error.

## Related runnable examples

- `cargo run --example test_tls_basic`
- `cargo run --example test_tls_gossip`
- `cargo run --example test_tls_with_node_id`
- `cargo run --example test_bidirectional_tls`
- `cargo run --example test_keypair_auth`
- `cargo run --example test_invalid_keypair`
- `cargo run --example test_valid_keypair`

## Identity model

- `NodeId` is the public-key identity used by the TLS verifier.
- `PeerId` is the peer-facing identity used elsewhere in the crate and can be converted to `NodeId`.
- When the client supplies an expected node identity, the verifier rejects a peer whose certificate encodes a different Ed25519 public key.

## Debugging

```bash
RUST_LOG=icanact_remote=debug cargo run --example tls_server 9001
RUST_LOG=icanact_remote=trace cargo run --example tls_client 9001 /tmp/icanact_tls_server.pub
```

Common failure cases:

- Missing public-key file because the server has not been run yet.
- `NodeId mismatch` when testing with `--wrong-key`.
- `Connection refused` when the server is not listening.
