use std::future::Future;
use std::net::SocketAddr;
use tokio::time::{Duration, sleep};
use tracing::info;
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};

use icanact_remote::{GossipConfig, GossipRegistryHandle, KeyPair};

fn maybe_init_tracing(level: &'static str) {
    // In this sandboxed environment, enabling tracing can trigger EPERM ("Operation not
    // permitted") on subsequent networking syscalls. Only enable when explicitly requested.
    if std::env::var("ICANACT_TEST_LOG").ok().as_deref() == Some("1") {
        let directive = format!("icanact_remote={level}").parse().unwrap();
        let _ = tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_filter(EnvFilter::from_default_env().add_directive(directive)),
            )
            .try_init();
    }
}

fn run_async_test<F>(name: &str, fut: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    let handle = std::thread::Builder::new()
        .name(name.to_string())
        .stack_size(8 * 1024 * 1024)
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(4 * 1024 * 1024)
                .enable_all()
                .build()
                .expect("failed to build runtime");
            rt.block_on(fut);
        })
        .expect("failed to spawn lifecycle test thread");
    handle.join().expect("lifecycle test panicked");
}

fn key_pair_ordered_for_outbound_a(seed_a: &str, seed_b: &str) -> (KeyPair, KeyPair) {
    let first = KeyPair::new_for_testing(seed_a);
    let second = KeyPair::new_for_testing(seed_b);
    if first
        .peer_id()
        .to_node_id()
        .as_bytes()
        .cmp(second.peer_id().to_node_id().as_bytes())
        .is_lt()
    {
        (first, second)
    } else {
        (second, first)
    }
}

fn key_pair_greater_than(seed_prefix: &str, lower: &KeyPair) -> KeyPair {
    (0..100)
        .map(|idx| KeyPair::new_for_testing(format!("{seed_prefix}_{idx}")))
        .find(|candidate| {
            lower
                .peer_id()
                .to_node_id()
                .as_bytes()
                .cmp(candidate.peer_id().to_node_id().as_bytes())
                .is_lt()
        })
        .expect("find higher peer id")
}

/// Test that connection mappings remain valid after multiple gossip rounds.
///
/// This test verifies that the fix for the FullSync/FullSyncResponse addr_to_peer_id
/// removal bug works correctly. Without the fix, address mappings would get corrupted
/// over time as gossip rounds remove the ephemeral address entries.
///
/// The key aspect tested is that get_connection() continues to work after many gossip
/// rounds, which requires the address mappings to remain consistent.
#[test]
fn test_connection_survives_multiple_gossip_rounds() {
    run_async_test("connection-lifecycle-gossip", async {
        maybe_init_tracing("info");

        // Create two nodes with SHORT gossip interval to trigger many rounds quickly
        let addr_a: SocketAddr = "127.0.0.1:7921".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:7922".parse().unwrap();

        let (key_pair_a, key_pair_b) =
            key_pair_ordered_for_outbound_a("lifecycle_node_a", "lifecycle_node_b");

        let peer_id_b = key_pair_b.peer_id();

        // Use short gossip interval to trigger multiple rounds
        let config_a = GossipConfig {
            gossip_interval: Duration::from_millis(500), // Fast gossip to trigger bug
            ..Default::default()
        };

        let config_b = GossipConfig {
            gossip_interval: Duration::from_millis(500), // Fast gossip to trigger bug
            ..Default::default()
        };

        // Start nodes
        let handle_a = GossipRegistryHandle::new_with_transport_stack(
            addr_a,
            key_pair_a.to_secret_key(),
            Some(config_a),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .expect("Failed to create node A");

        let handle_b = GossipRegistryHandle::new_with_transport_stack(
            addr_b,
            key_pair_b.to_secret_key(),
            Some(config_b),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .expect("Failed to create node B");

        // Connect A -> B (single direction is sufficient for this test)
        let peer_b = handle_a.add_peer(&peer_id_b).await;
        peer_b
            .connect(&addr_b)
            .await
            .expect("Failed to connect A -> B");

        // Wait for connection to stabilize
        sleep(Duration::from_millis(200)).await;

        // Initial verification - connection should be available
        handle_a
            .lookup_address(addr_b)
            .await
            .expect("Initial connection failed");
        info!("Initial connection established");

        // Wait for multiple gossip rounds (which trigger FullSync/FullSyncResponse)
        // With 500ms interval, 5 seconds = ~10 gossip rounds
        info!("Waiting for multiple gossip rounds...");
        sleep(Duration::from_secs(5)).await;
        info!("Multiple gossip rounds completed");

        // After many gossip rounds, connection should STILL be available
        // This is the critical test - without the fix, get_connection would fail
        // because the address mappings would be corrupted
        handle_a
            .lookup_address(addr_b)
            .await
            .expect("Connection should still be available after gossip rounds - fix verified!");

        info!("Connection still available after gossip rounds - PASS");

        // Cleanup
        handle_a.shutdown().await;
        handle_b.shutdown().await;
    });
}

/// Test that addr_to_peer_id mappings are preserved after reindexing
///
/// This specifically tests the fix for the bug where FullSync removes
/// the ephemeral address mapping before reindex, causing orphaned entries.
#[test]
fn test_addr_mappings_preserved_after_fullsync() {
    run_async_test("connection-lifecycle-fullsync", async {
        maybe_init_tracing("debug");

        let addr_a: SocketAddr = "127.0.0.1:7930".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:7924".parse().unwrap();

        let (key_pair_a, key_pair_b) =
            key_pair_ordered_for_outbound_a("mapping_node_a", "mapping_node_b");

        let peer_id_b = key_pair_b.peer_id();

        let config_a = GossipConfig {
            gossip_interval: Duration::from_millis(200), // Very fast gossip
            ..Default::default()
        };

        let config_b = GossipConfig {
            gossip_interval: Duration::from_millis(200),
            ..Default::default()
        };

        let handle_a = GossipRegistryHandle::new_with_transport_stack(
            addr_a,
            key_pair_a.to_secret_key(),
            Some(config_a),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .unwrap();

        let handle_b = GossipRegistryHandle::new_with_transport_stack(
            addr_b,
            key_pair_b.to_secret_key(),
            Some(config_b),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .unwrap();

        // Connect
        let peer_b = handle_a.add_peer(&peer_id_b).await;
        peer_b.connect(&addr_b).await.unwrap();

        // Wait for connection to stabilize
        sleep(Duration::from_millis(500)).await;

        // Verify initial state
        let conn = handle_a
            .lookup_address(addr_b)
            .await
            .expect("Initial connection");
        let response = conn
            .ask(bytes::Bytes::from_static(b"ECHO:test"))
            .await
            .expect("Initial ask");
        assert_eq!(response.as_ref(), b"ECHOED:test");

        // Now let many gossip rounds happen
        for round in 0..20 {
            sleep(Duration::from_millis(250)).await;

            // Try to send a message each round
            match handle_a.lookup_address(addr_b).await {
                Ok(conn) => {
                    let request = format!("ECHO:round{}", round);
                    match conn
                        .ask(bytes::Bytes::copy_from_slice(request.as_bytes()))
                        .await
                    {
                        Ok(response) => {
                            let expected = format!("ECHOED:round{}", round);
                            assert_eq!(response, expected.as_bytes(), "Round {} mismatch", round);
                            info!("Round {} message delivered successfully", round);
                        }
                        Err(e) => {
                            panic!(
                                "Round {} ask failed: {} - address mappings likely corrupted!",
                                round, e
                            );
                        }
                    }
                }
                Err(e) => {
                    panic!(
                        "Round {} connection lost: {} - address mappings corrupted!",
                        round, e
                    );
                }
            }
        }

        info!("All 20 rounds passed - address mappings are correctly preserved");

        handle_a.shutdown().await;
        handle_b.shutdown().await;
    });
}

/// Test rapid reconnection doesn't leave orphaned address entries.
///
/// This test verifies that after a node disconnects and a new node binds to the
/// same address, the connection mappings are correctly updated without orphaned
/// entries that would prevent proper routing.
#[test]
fn test_reconnect_cleanup() {
    run_async_test("connection-lifecycle-reconnect", async {
        maybe_init_tracing("info");

        let addr_a: SocketAddr = "127.0.0.1:7935".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:7936".parse().unwrap();

        let (key_pair_a, key_pair_b) =
            key_pair_ordered_for_outbound_a("reconnect_node_a", "reconnect_node_b");

        let peer_id_b = key_pair_b.peer_id();

        let config = GossipConfig {
            gossip_interval: Duration::from_secs(300), // Long interval - we control timing
            ..Default::default()
        };

        let handle_a = GossipRegistryHandle::new_with_transport_stack(
            addr_a,
            key_pair_a.to_secret_key(),
            Some(config.clone()),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .unwrap();

        let handle_b = GossipRegistryHandle::new_with_transport_stack(
            addr_b,
            key_pair_b.to_secret_key(),
            Some(config),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .unwrap();

        // Initial connect
        let peer_b = handle_a.add_peer(&peer_id_b).await;
        peer_b.connect(&addr_b).await.unwrap();

        sleep(Duration::from_millis(200)).await;

        // Verify initial connection available
        handle_a
            .lookup_address(addr_b)
            .await
            .expect("Initial connection should work");
        info!("Initial connection established");

        // Disconnect by shutting down B
        info!("Shutting down node B to force disconnect");
        handle_b.shutdown().await;

        // Wait longer for peer cleanup to avoid consensus query race condition
        // (old peer failure handling needs time to complete before we reconnect with new peer)
        sleep(Duration::from_secs(2)).await;

        // Restart B with same address but NEW identity
        info!("Restarting node B with new identity");
        let key_pair_b2 = key_pair_greater_than("reconnect_node_b2", &key_pair_a);
        let peer_id_b2 = key_pair_b2.peer_id();
        let handle_b2 = GossipRegistryHandle::new_with_transport_stack(
            addr_b,
            key_pair_b2.to_secret_key(),
            Some(GossipConfig {
                gossip_interval: Duration::from_secs(300),
                ..Default::default()
            }),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .unwrap();

        // Reconnect to the new peer
        let peer_b2 = handle_a.add_peer(&peer_id_b2).await;
        peer_b2.connect(&addr_b).await.unwrap();

        sleep(Duration::from_millis(500)).await;

        // The critical test: verify get_connection works for the NEW peer
        // This would fail if old address mappings weren't cleaned up properly
        handle_a
            .lookup_address(addr_b)
            .await
            .expect("Reconnection should work - address mappings correctly updated");

        info!("Reconnection successful - no orphaned address entries");

        handle_a.shutdown().await;
        handle_b2.shutdown().await;
    });
}

/// A clean session teardown must release its address ownership promptly --
/// not merely "eventually, once someone else times it out". This exercises
/// the exact production path that arms and releases a connection-scoped
/// claim (the outbound dial's `add_connection_scoped_peer_claim` in
/// `transport_stream.rs`, and the IO task's `ExitGuard::drop` in
/// `stream_writer.rs`, which is the sole production constructor of that
/// guard and is what threads `peer_id`/`session_source` through to
/// `release_connection_scoped_claims`), end to end over a real TLS
/// connection. If any of those hops ever dropped `peer_id` or
/// `session_source`, this test would hang the same way
/// `test_reconnect_cleanup` did before the fix -- just with a much shorter,
/// deliberately tight wait instead of a multi-second workaround.
#[test]
fn test_clean_disconnect_releases_ownership_promptly() {
    run_async_test("connection-lifecycle-prompt-release", async {
        maybe_init_tracing("info");

        let addr_a: SocketAddr = "127.0.0.1:7940".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:7941".parse().unwrap();

        let (key_pair_a, key_pair_b) =
            key_pair_ordered_for_outbound_a("prompt_release_node_a", "prompt_release_node_b");

        let peer_id_b = key_pair_b.peer_id();

        let config = GossipConfig {
            gossip_interval: Duration::from_secs(300), // Long interval - we control timing
            ..Default::default()
        };

        let handle_a = GossipRegistryHandle::new_with_transport_stack(
            addr_a,
            key_pair_a.to_secret_key(),
            Some(config.clone()),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .unwrap();

        let handle_b = GossipRegistryHandle::new_with_transport_stack(
            addr_b,
            key_pair_b.to_secret_key(),
            Some(config),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .unwrap();

        let peer_b = handle_a.add_peer(&peer_id_b).await;
        peer_b.connect(&addr_b).await.unwrap();

        sleep(Duration::from_millis(200)).await;

        handle_a
            .lookup_address(addr_b)
            .await
            .expect("Initial connection should work");
        info!("Initial connection established");

        info!("Shutting down node B to force a clean disconnect");
        handle_b.shutdown().await;

        // Deliberately short: a clean teardown must release its claim
        // promptly, not merely within `test_reconnect_cleanup`'s much
        // longer 2s workaround wait.
        sleep(Duration::from_millis(500)).await;

        info!("Starting a DIFFERENT identity on B's old address");
        let key_pair_c = key_pair_greater_than("prompt_release_node_c", &key_pair_a);
        let peer_id_c = key_pair_c.peer_id();
        let handle_c = GossipRegistryHandle::new_with_transport_stack(
            addr_b,
            key_pair_c.to_secret_key(),
            Some(GossipConfig {
                gossip_interval: Duration::from_secs(300),
                ..Default::default()
            }),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .unwrap();

        let peer_c = handle_a.add_peer(&peer_id_c).await;
        peer_c.connect(&addr_b).await.expect(
            "a different identity must be able to claim the address promptly \
                     after the previous owner's clean disconnect",
        );

        sleep(Duration::from_millis(500)).await;

        handle_a
            .lookup_address(addr_b)
            .await
            .expect("Reconnection should work - ownership released promptly");

        info!("Prompt reclaim successful");

        handle_a.shutdown().await;
        handle_c.shutdown().await;
    });
}
