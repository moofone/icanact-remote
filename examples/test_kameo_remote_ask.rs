use icanact_remote::{GossipConfig, GossipRegistryHandle, KeyPair};
use std::net::SocketAddr;
use tokio::time::{Duration, sleep};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("icanact_remote=info")
        .init();

    // Create two nodes
    let addr_a: SocketAddr = "127.0.0.1:9001".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:9002".parse().unwrap();

    let key_pair_a = KeyPair::new_for_testing("node_a");
    let key_pair_b = KeyPair::new_for_testing("node_b");
    let peer_id_a = key_pair_a.peer_id();
    let peer_id_b = key_pair_b.peer_id();

    let config_a = GossipConfig {
        gossip_interval: Duration::from_secs(300),
        ..Default::default()
    };

    let config_b = GossipConfig {
        gossip_interval: Duration::from_secs(300),
        ..Default::default()
    };

    // Start nodes
    let handle_a = GossipRegistryHandle::new_with_transport_stack(addr_a, key_pair_a.to_secret_key(), Some(config_a), icanact_remote::BuilderTlsBootstrap)
        .await
        .unwrap();

    let handle_b = GossipRegistryHandle::new_with_transport_stack(addr_b, key_pair_b.to_secret_key(), Some(config_b), icanact_remote::BuilderTlsBootstrap)
        .await
        .unwrap();

    // Connect nodes - both directions
    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&addr_b).await.unwrap();

    let peer_a = handle_b.add_peer(&peer_id_a).await;
    peer_a.connect(&addr_a).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    println!("Test: Basic ask with correlation tracking");

    // Get connection and test ask
    let conn = handle_a.lookup_address(addr_b).await.unwrap();

    println!("Sending ask request...");
    let request = b"ECHO:Hello from Node A";
    let result: icanact_remote::Result<bytes::Bytes> = conn
        .ask_with_timeout(
            bytes::Bytes::copy_from_slice(request),
            Duration::from_secs(2),
        )
        .await;
    match result {
        Ok(response) => {
            println!("✅ Got response: {:?}", String::from_utf8_lossy(&response));
            assert_eq!(response.as_ref(), b"ECHOED:Hello from Node A");
        }
        Err(e) => {
            println!("❌ Ask failed: {:?}", e);
        }
    }

    // Shutdown
    handle_a.shutdown().await;
    handle_b.shutdown().await;
}
