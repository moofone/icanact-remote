use icanact_remote::{GossipConfig, GossipRegistryHandle, SecretKey};

#[tokio::test]
async fn test_manual_peer_configuration_enables_lookup() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Setup a registry (client side)
    let secret = SecretKey::generate();
    let registry = GossipRegistryHandle::new_with_transport_stack(
        "0.0.0.0:0".parse()?,
        secret,
        Some(GossipConfig::default()),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await?;

    // 2. Define a target server (doesn't need to actually exist for this test,
    //    we are testing the internal mapping logic)
    let server_addr = "127.0.0.1:9999".parse()?;
    let server_node_id = SecretKey::generate().public(); // Random ID

    // 3. Manually configure the peer
    // This calls configure_peer internally
    registry
        .registry
        .add_peer_with_node_id(server_addr, Some(server_node_id))
        .await;

    // 4. Verification: Immediate lookup by address should work
    // Before the fix, this would fail/return None because addr_to_peer_id wasn't populated
    // We expect a connection attempt (or at least a valid handle construction), not an "ActorNotFound" / "Unknown Peer" error logic

    // The previous bug caused lookup_address to return Err("No peer ID found for ...") inside the implementation
    // or return None equivalent if it was option-based (lookup_address returns Result<ConnectionHandle>)

    let result = registry.lookup_address(server_addr).await;

    // We expect success (getting a handle) or at least NOT "No peer ID found"
    // Since the server doesn't exist, it might fail to CONNECT, but it shouldn't fail to IDENTIFY the peer ID.
    // However, lookup_address in the current codebase tries to Get or Create a connection.
    // If mapping exists, it proceeds.

    match result {
        Ok(_) => {
            // Success! We found the mapping and tried to connect (even if connection fails later, the mapping worked)
            println!("✅ Lookup succeeded (handle returned)");
        }
        Err(e) => {
            let msg = e.to_string();
            // If the bug is present, it returns "No peer ID found for 127.0.0.1:9999"
            if msg.contains("No peer ID found") {
                panic!(
                    "❌ Regression: Address mapping was not created! Error: {}",
                    msg
                );
            } else {
                // Other errors (like connection refused) are expected since 127.0.0.1:9999 isn't listening
                println!(
                    "✅ Lookup attempted (mapping exists), failed with expected network error: {}",
                    msg
                );
            }
        }
    }

    Ok(())
}
