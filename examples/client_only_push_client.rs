#[path = "support/error.rs"]
mod example_error;

use example_error::{Error, Result};
use futures::future::BoxFuture;
use icanact_remote::registry::{
    ActorMessageHandlerSync, ActorResponse, PeerConnectHandler, PeerDisconnectHandler,
};
use icanact_remote::{
    AlignedBytes, GossipConfig, GossipNodeId, GossipRegistryHandle, PeerId, SecretKey,
};
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

const SERVER_ADDR: &str = "127.0.0.1:29300";
const SERVER_PUB: &str = "/tmp/icanact_tls/client_only_push_server.pub";
const CLIENT_KEY: &str = "/tmp/icanact_tls/client_only_push_client.key";

/// Client-only node: makes only outbound connections, advertises no dialable
/// address, and receives pushed events on an actor over its own session.
///
/// Run the server first, then: `cargo run --example client_only_push_client`
/// Stop and restart the server: the configured-peer supervisor reconnects and
/// the connect/disconnect callbacks below fire so the app can re-subscribe.
#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    tracing_subscriber::fmt().init();

    let secret = load_or_generate_key(CLIENT_KEY)?;
    fs::write(
        CLIENT_KEY.replace(".key", ".pub"),
        hex::encode(secret.public().as_bytes()),
    )?;
    let server_bytes = hex::decode(fs::read_to_string(SERVER_PUB)?.trim())?;
    let server_id = PeerId::from(&GossipNodeId::from_bytes(&server_bytes)?);

    let config = GossipConfig {
        client_only: true,
        ..Default::default()
    };
    let client = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse()?,
        secret,
        Some(config),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await?;
    client
        .registry
        .set_actor_message_handler_sync(Arc::new(Printer))
        .await;
    client
        .registry
        .set_peer_connect_handler(Arc::new(Events))
        .await;
    client
        .registry
        .set_peer_disconnect_handler(Arc::new(Events))
        .await;

    // A configured peer is supervised: the client redials the server forever.
    let server = client.add_peer(&server_id).await;
    match server.connect(&SERVER_ADDR.parse()?).await {
        Ok(_) => println!("connected to {SERVER_ADDR}"),
        Err(err) => println!("initial connect failed ({err}); the supervisor keeps retrying"),
    }
    println!("waiting for pushes (Ctrl+C to quit)");

    let _ = tokio::signal::ctrl_c().await;
    client.shutdown_and_wait().await;
    Ok(())
}

struct Printer;

impl ActorMessageHandlerSync for Printer {
    fn handle_actor_message_sync(
        &self,
        _actor_id: u64,
        _type_hash: u32,
        payload: AlignedBytes,
        correlation_id: Option<u32>,
    ) -> icanact_remote::Result<Option<ActorResponse>> {
        println!("received: {}", String::from_utf8_lossy(payload.as_ref()));
        Ok(correlation_id.map(|_| ActorResponse::from(b"ack".to_vec())))
    }
}

struct Events;

impl PeerConnectHandler for Events {
    fn handle_peer_connect(&self, addr: SocketAddr, _peer: Option<PeerId>) -> BoxFuture<'_, ()> {
        Box::pin(async move { println!("connected: {addr} (re-subscribe here)") })
    }
}

impl PeerDisconnectHandler for Events {
    fn handle_peer_disconnect(&self, addr: SocketAddr, _peer: Option<PeerId>) -> BoxFuture<'_, ()> {
        Box::pin(async move { println!("disconnected: {addr}") })
    }
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
