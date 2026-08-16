#[path = "support/error.rs"]
mod example_error;

use example_error::{Error, Result};
use futures::future::BoxFuture;
use icanact_remote::registry::{
    ActorMessageFuture, ActorMessageHandler, ActorMessageHandlerSync, PeerDisconnectHandler,
};
use icanact_remote::{GossipConfig, GossipRegistryHandle, SecretKey};
use std::fs;
use std::path::Path;
use std::sync::Arc;

const ACTOR_ID: u64 = 0xC0FF_EE00;

/// Console tell/ask server (TLS).
///
/// Usage:
///   cargo run --example console_tell_ask_server
///
/// Then in another terminal:
///   cargo run --example console_tell_ask_client -- /tmp/icanact_tls/console_tell_ask_server.pub
#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    tracing_subscriber::fmt().init();

    println!("🔐 Console Tell/Ask Server (TLS)");
    println!("================================\n");

    // Load or generate server TLS keypair
    let key_path = "/tmp/icanact_tls/console_tell_ask_server.key";
    let secret_key = load_or_generate_key(key_path)?;
    let node_id = secret_key.public();

    let pub_path = key_path.replace(".key", ".pub");
    fs::write(&pub_path, hex::encode(node_id.as_bytes()))?;

    println!("Server GossipNodeId: {}", node_id.fmt_short());
    println!("Public key: {}\n", pub_path);

    let server_addr = "127.0.0.1:29200".parse()?;
    let config = GossipConfig {
        // Raise ask inflight limit to avoid throttling direct responses under high concurrency.
        ask_window: 4096,
        ..Default::default()
    };
    let registry = GossipRegistryHandle::new_with_transport_stack(
        server_addr,
        secret_key,
        Some(config),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await?;

    registry
        .registry
        .set_actor_message_handler_sync(Arc::new(ConsoleActorHandler))
        .await;
    registry
        .registry
        .set_peer_disconnect_handler(Arc::new(ConsoleDisconnectHandler))
        .await;

    println!("✅ Listening on: {}", server_addr);
    println!(
        "✅ Actor handler ready (accepts any actor_id; client uses 0x{:016x})\n",
        ACTOR_ID
    );
    println!("Run the client in another terminal:");
    println!(
        "  cargo run --example console_tell_ask_client -- {}",
        pub_path
    );
    println!("Press Ctrl+C to stop\n");

    let _ = tokio::signal::ctrl_c().await;
    println!("🛑 [SERVER] Ctrl+C received, shutting down...");
    registry.shutdown_and_wait().await;
    println!("🛑 [SERVER] Shutdown complete.");
    Ok(())
}

struct ConsoleActorHandler;
struct ConsoleDisconnectHandler;

impl PeerDisconnectHandler for ConsoleDisconnectHandler {
    fn handle_peer_disconnect(
        &self,
        peer_addr: std::net::SocketAddr,
        peer_id: Option<icanact_remote::PeerId>,
    ) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            println!(
                "🔌 [SERVER] Peer disconnected: addr={} peer_id={:?}",
                peer_addr, peer_id
            );
        })
    }
}

impl ActorMessageHandler for ConsoleActorHandler {
    fn handle_actor_message(
        &self,
        _actor_id: u64,
        _type_hash: u32,
        payload: icanact_remote::AlignedBytes,
        correlation_id: Option<u32>,
    ) -> ActorMessageFuture<'_> {
        Box::pin(async move {
            if correlation_id.is_some() {
                Ok(Some(payload.into()))
            } else {
                Ok(None)
            }
        })
    }
}

impl ActorMessageHandlerSync for ConsoleActorHandler {
    fn handle_actor_message_sync(
        &self,
        _actor_id: u64,
        _type_hash: u32,
        payload: icanact_remote::AlignedBytes,
        correlation_id: Option<u32>,
    ) -> icanact_remote::Result<Option<icanact_remote::registry::ActorResponse>> {
        if correlation_id.is_some() {
            Ok(Some(payload.into()))
        } else {
            Ok(None)
        }
    }
}

fn load_or_generate_key(path: &str) -> Result<SecretKey> {
    let key_path = Path::new(path);

    if key_path.exists() {
        let key_hex = fs::read_to_string(key_path)?;
        let key_bytes = hex::decode(key_hex.trim())?;

        if key_bytes.len() != 32 {
            return Err(Error::InvalidKeyLength {
                kind: "secret key",
                actual: key_bytes.len(),
            });
        }

        let mut arr = [0u8; 32];
        arr.copy_from_slice(&key_bytes);
        Ok(SecretKey::from_bytes(&arr)?)
    } else {
        if let Some(parent) = key_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let secret_key = SecretKey::generate();
        fs::write(key_path, hex::encode(secret_key.to_bytes()))?;
        Ok(secret_key)
    }
}
