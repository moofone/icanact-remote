// examples/multi_node.rs
// Run with: cargo run --example multi_node

use anyhow::Result;
use icanact_remote::{GossipConfig, GossipRegistryHandle, KeyPair};
use tokio::time::{Duration, sleep};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("info"))
        .init();

    println!("=== TCP Gossip Registry Multi-Node Demo ===\n");

    let config = GossipConfig::default();

    // Start bootstrap node (node1)
    println!("🚀 Starting bootstrap node (node1) on 127.0.0.1:8000");
    let key_pair1 = KeyPair::new_for_testing("node1");
    let peer_id1 = key_pair1.peer_id();
    let node1 = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:8000".parse().unwrap(),
        key_pair1.to_secret_key(),
        Some(config.clone()),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await?;

    // Register some services on node1
    node1
        .register(
            "user_service".to_string(),
            "127.0.0.1:9001".parse().unwrap(),
        )
        .await?;

    node1
        .register(
            "auth_service".to_string(),
            "127.0.0.1:9002".parse().unwrap(),
        )
        .await?;

    println!("✅ Registered user_service and auth_service on node1\n");

    // Wait a moment then start node2
    sleep(Duration::from_millis(500)).await;

    println!("🚀 Starting node2 on 127.0.0.1:8001 (connecting to node1)");
    let key_pair2 = KeyPair::new_for_testing("node2");
    let node2 = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:8001".parse().unwrap(),
        key_pair2.to_secret_key(),
        Some(config.clone()),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await?;
    let peer1 = node2.add_peer(&peer_id1).await;
    peer1.connect(&"127.0.0.1:8000".parse().unwrap()).await?;

    // Register different services on node2
    node2
        .register(
            "payment_service".to_string(),
            "127.0.0.1:9003".parse().unwrap(),
        )
        .await?;

    node2
        .register(
            "notification_service".to_string(),
            "127.0.0.1:9004".parse().unwrap(),
        )
        .await?;

    println!("✅ Registered payment_service and notification_service on node2\n");

    // Wait a moment then start node3
    sleep(Duration::from_millis(500)).await;

    println!("🚀 Starting node3 on 127.0.0.1:8002 (connecting to node1)");
    let key_pair3 = KeyPair::new_for_testing("node3");
    let node3 = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:8002".parse().unwrap(),
        key_pair3.to_secret_key(),
        Some(config.clone()),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await?;
    let peer1_from_3 = node3.add_peer(&peer_id1).await;
    peer1_from_3
        .connect(&"127.0.0.1:8000".parse().unwrap())
        .await?;

    // Register services on node3
    node3
        .register(
            "order_service".to_string(),
            "127.0.0.1:9005".parse().unwrap(),
        )
        .await?;

    node3
        .register(
            "inventory_service".to_string(),
            "127.0.0.1:9006".parse().unwrap(),
        )
        .await?;

    println!("✅ Registered order_service and inventory_service on node3\n");

    // Monitor gossip propagation
    println!("📡 Monitoring gossip propagation...\n");

    for _ in 1..=8 {
        let stats1 = node1.stats().await;
        let stats2 = node2.stats().await;
        let stats3 = node3.stats().await;

        println!(
            "🔍 === Gossip Round {}/{}/{} ===",
            stats1.total_gossip_rounds, stats2.total_gossip_rounds, stats3.total_gossip_rounds
        );

        // Show stats for each node

        println!(
            "Node1 - Local: {}, Known: {}, Peers: {}, Gossip rounds: {}",
            stats1.local_actors,
            stats1.known_actors,
            stats1.active_peers,
            stats1.total_gossip_rounds
        );
        println!(
            "Node2 - Local: {}, Known: {}, Peers: {}, Gossip rounds: {}",
            stats2.local_actors,
            stats2.known_actors,
            stats2.active_peers,
            stats2.total_gossip_rounds
        );
        println!(
            "Node3 - Local: {}, Known: {}, Peers: {}, Gossip rounds: {}",
            stats3.local_actors,
            stats3.known_actors,
            stats3.active_peers,
            stats3.total_gossip_rounds
        );

        // Test cross-node lookups
        println!("\n🔎 Testing cross-node service discovery:");

        async fn perform_lookup(
            node_id: u8,
            node: &GossipRegistryHandle,
            name: &'static str,
            peer_node: u8,
        ) {
            if let Some(location) = node.lookup(name).await {
                println!(
                    "✅ Node{node_id} found {name:<20} (node{peer_node}) at: {}",
                    location.location.address
                );
            } else {
                println!("❌ Node1 cannot yet find {name:<20}");
            }
        }

        // Node1 looking for services on other nodes
        perform_lookup(1, &node1, "payment_service", 2).await;
        perform_lookup(1, &node1, "order_service", 3).await;

        // Node2 looking for services on other nodes
        perform_lookup(2, &node2, "user_service", 1).await;
        perform_lookup(2, &node2, "inventory_service", 3).await;

        // Node3 looking for services on other nodes
        perform_lookup(3, &node3, "auth_service", 1).await;
        perform_lookup(3, &node3, "notification_service", 2).await;

        // Check if all services are discovered
        let all_discovered = [
            node1.lookup("payment_service").await.is_some(),
            node1.lookup("order_service").await.is_some(),
            node2.lookup("user_service").await.is_some(),
            node2.lookup("inventory_service").await.is_some(),
            node3.lookup("auth_service").await.is_some(),
            node3.lookup("notification_service").await.is_some(),
        ];

        if all_discovered.iter().all(|&x| x) {
            println!("\n🎉 All services discovered! Gossip convergence complete.");
            break;
        }

        println!();
        sleep(config.gossip_interval / 2).await;
    }

    // Keep running so you can observe continued gossip
    println!("\n⏰ Keeping nodes running for continued observation...");
    println!("   Press Ctrl+C to exit");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!("\n👋 Shutting down gracefully...");
        }
        _ = sleep(Duration::from_secs(120)) => {
            println!("\n⏰ Demo timeout reached");
        }
    }

    Ok(())
}
