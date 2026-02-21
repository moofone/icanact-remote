# TLS Examples for Icanact Remote

This directory contains examples demonstrating TLS-enabled secure communication with Ed25519 authentication.

## Quick Start: Basic TLS Connection

### 1. Run the TLS Server
```bash
# Terminal 1
cargo run --example tls_server 9001
```

### 2. Connect with TLS Client
```bash
# Terminal 2
cargo run --example tls_client 9001 /tmp/icanact_tls_server.pub
```

## Secure Tell Examples (Production-Ready)

These examples demonstrate production-ready TLS with persistent keys and service discovery.

### 1. Run the Secure TLS Server
```bash
# Terminal 1
cargo run --example tell_secure_tls_server
```
The server will:
- Generate or load Ed25519 keypair from `/tmp/icanact_tls_keys/secure_server.key`
- Save public key to `/tmp/icanact_tls_keys/secure_server.pub`
- Start TLS-enabled gossip registry on port 29001
- Register secure services (chat, auth, data)
- Display connection statistics

### 2. Connect with Secure TLS Client
```bash
# Terminal 2
cargo run --example tell_secure_tls_client /tmp/icanact_tls_keys/secure_server.pub
```
The client will:
- Load server's public key for verification
- Generate or load its own keypair
- Connect via TLS with mutual authentication
- Discover server's services through encrypted gossip
- Display connection status

### 3. Test Authentication Failure
```bash
# Terminal 2 (with server still running)
cargo run --example tell_secure_tls_client /tmp/icanact_tls_keys/secure_server.pub --wrong-key
```
This will:
- Attempt connection with wrong expected public key
- Connection should FAIL with "NodeId mismatch" error
- Demonstrates that TLS prevents impersonation attacks

## Key Files and Locations

### Default Key Locations
- **Basic Examples**: `/tmp/icanact_tls_*.key` and `/tmp/icanact_tls_*.pub`
- **Secure Examples**: `/tmp/icanact_tls_keys/*.key` and `/tmp/icanact_tls_keys/*.pub`

### Pre-generating Keys
You can pre-generate keys for testing:

```bash
# Generate a keypair using the keypair_generation example
cargo run --example keypair_generation

# Or use OpenSSL to generate Ed25519 keys
openssl genpkey -algorithm ED25519 -out server.key
openssl pkey -in server.key -pubout -out server.pub
```

## Understanding NodeId vs PublicKey

In the TLS implementation:
- **NodeId = PublicKey** - They are the same type
- The Ed25519 public key IS the node's identity
- When we say "NodeId mismatch", we mean the public key doesn't match
- This provides cryptographic proof of identity

## Security Properties

### What's Protected
✅ **Authentication**: Only nodes with valid Ed25519 keypairs can connect
✅ **Encryption**: All gossip traffic encrypted with TLS 1.3
✅ **Integrity**: Messages cannot be tampered with
✅ **No MITM**: Can't impersonate without the private key

### What Should Fail
❌ Connection with wrong NodeId/PublicKey
❌ Connection without private key
❌ Man-in-the-middle attempts
❌ Certificate substitution

## Debugging TLS Issues

### Enable Debug Logging
```bash
# Detailed TLS logging
RUST_LOG=icanact_remote=debug cargo run --example tls_server 9001

# Trace-level logging
RUST_LOG=icanact_remote=trace cargo run --example tls_client 9001
```

### Common Issues

1. **"No such file or directory"** - Server hasn't been run yet to generate keys
2. **"NodeId mismatch"** - This is expected when testing with `--wrong-key`
3. **"Connection refused"** - Make sure server is running first
4. **Services not discovered** - Wait a few seconds for gossip propagation

## Example Output

### Successful Connection
```
🔐 TLS-Enabled Secure Icanact Remote Client
==========================================

1. Loading server information:
   Server NodeId: 428cf775b7
   Server public key from: /tmp/icanact_tls_keys/secure_server.pub
   Server address: 127.0.0.1:29001

2. Setting up client TLS identity:
   Client NodeId: 8a3b291dc4
   Client key: /tmp/icanact_tls_keys/secure_client.key

3. Starting client with TLS encryption...
   ✅ Client listening on: 127.0.0.1:54321
   ✅ Client NodeId: 8a3b291dc4
   ✅ TLS encryption: ENABLED
   ✅ Ed25519 authentication: ENABLED

6. Discovered secure services from server:
   ✅ secure_chat_service -> ActorLocation { ... }
   ✅ secure_auth_service -> ActorLocation { ... }
   ✅ secure_data_service -> ActorLocation { ... }

✅ SUCCESS: All secure services discovered via TLS gossip!
   - Mutual TLS authentication successful
   - Ed25519 certificates verified
   - Encrypted gossip propagation working
```

### Failed Authentication (Expected)
```
⏳ Attempting TLS connection (should fail due to NodeId mismatch)...
✅ Connection correctly rejected: TLS error: TLS handshake failed: unexpected error: NodeId mismatch: expected 138d80fed1, got 428cf775b7

✅ TEST PASSED: TLS authentication working correctly!

Explanation:
- NodeId IS the Ed25519 public key
- Server's certificate contains its public key: 428cf775b7
- We expected a different public key: 138d80fed1
- TLS handshake detected the mismatch and rejected the connection

This prevents man-in-the-middle attacks where an attacker
tries to impersonate the server with their own key.
```