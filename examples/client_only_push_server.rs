#[path = "support/error.rs"]
mod example_error;

use bytes::Bytes;
use example_error::{Error, Result};
use icanact_remote::tls::allowlist::PeerAllowlist;
use icanact_remote::{GossipConfig, GossipRegistryHandle, PeerId, SecretKey};
use std::fs;
use std::path::Path;
use std::time::Duration;

const ACTOR_ID: u64 = 0xC0FF_EE01;
const TYPE_HASH: u32 = 0xC0FF_EE01;
const SERVER_ADDR: &str = "127.0.0.1:29300";
const SERVER_KEY: &str = "/tmp/icanact_tls/client_only_push_server.key";
const CLIENT_PUB: &str = "/tmp/icanact_tls/client_only_push_client.pub";

/// Server that pushes events to a client-only node (a laptop/CLI that can
/// only make outbound connections), admitting only allowlisted client keys.
///
/// Terminal 1: `cargo run --example client_only_push_server`
/// Terminal 2: `cargo run --example client_only_push_client`
///
/// The server never dials the client: it `tell`s over the session the client
/// opened. The allowlist is re-read from `CLIENT_PUB` on every push tick, so
/// editing that file swaps the allowed set at runtime (REMOTE-3: by identity).
#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    tracing_subscriber::fmt().init();

    let secret = load_or_generate_key(SERVER_KEY)?;
    fs::write(
        SERVER_KEY.replace(".key", ".pub"),
        hex::encode(secret.public().as_bytes()),
    )?;

    // Start closed: nobody is allowed until the client's public key exists.
    let allowlist = PeerAllowlist::default();
    let config = GossipConfig {
        inbound_peer_allowlist: Some(allowlist.clone()),
        ..Default::default()
    };
    let server = GossipRegistryHandle::new_with_transport_stack(
        SERVER_ADDR.parse()?,
        secret,
        Some(config),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await?;
    println!("server listening on {SERVER_ADDR}; allowlist file: {CLIENT_PUB}");

    let mut tick = tokio::time::interval(Duration::from_secs(2));
    let mut seq = 0u64;
    let mut allowed: Option<PeerId> = None;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = tick.tick() => {}
        }
        // Reload the allowlist; a changed file replaces the whole set atomically.
        let wanted = fs::read_to_string(CLIENT_PUB)
            .ok()
            .and_then(|hex_key| hex::decode(hex_key.trim()).ok())
            .and_then(|bytes| PeerId::from_bytes(&bytes).ok());
        if wanted != allowed {
            allowlist.replace(wanted.clone());
            println!("allowlist now: {wanted:?}");
            allowed = wanted;
        }
        let Some(client_id) = allowed.as_ref() else {
            continue;
        };
        match server.client().lookup_connected_peer(client_id) {
            Some(client) => {
                seq += 1;
                let payload = Bytes::from(format!("event #{seq}"));
                let conn = client
                    .connection_ref()
                    .ok_or(icanact_remote::GossipError::Shutdown)?;
                conn.tell_actor_frame(ACTOR_ID, TYPE_HASH, payload).await?;
                println!("pushed event #{seq}");
            }
            None => println!("client not connected (the server never dials it)"),
        }
    }
    server.shutdown_and_wait().await;
    Ok(())
}

fn load_or_generate_key(path: &str) -> Result<SecretKey> {
    let key_path = Path::new(path);
    if key_path.exists() {
        let bytes = hex::decode(fs::read_to_string(key_path)?.trim())?;
        let arr: [u8; 32] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| Error::InvalidKeyLength {
                kind: "secret key",
                actual: bytes.len(),
            })?;
        Ok(SecretKey::from_bytes(&arr)?)
    } else {
        if let Some(parent) = key_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let secret = SecretKey::generate();
        fs::write(key_path, hex::encode(secret.to_bytes()))?;
        Ok(secret)
    }
}
