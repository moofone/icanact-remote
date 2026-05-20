mod common;

use std::time::Duration;

use common::{
    create_ordered_tls_pair, create_tls_node_with_keypair, fast_gossip_config,
    register_probe_and_wait_visible, seed_peer, wait_for_pair_connection,
};
use icanact_remote::KeyPair;

async fn assert_probe_visible(source: &common::TlsHandle, sink: &common::TlsHandle, name: &str) {
    assert!(
        wait_for_pair_connection(source, sink, Duration::from_secs(5)).await,
        "peer pair never reached a usable connection"
    );
    assert!(
        register_probe_and_wait_visible(source, sink, name, Duration::from_secs(5)).await,
        "probe actor {name} never became visible across the gossip pair"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lower_id_seed_only_converges_to_high_id_actor_visibility() {
    let (high, low) = create_ordered_tls_pair("ownership-lower-only-a", "ownership-lower-only-b")
        .await
        .expect("ordered pair");

    seed_peer(&low, &high)
        .await
        .expect("lower should dial high");

    assert_probe_visible(&high, &low, "ownership.lower_only.high_probe").await;
    assert_probe_visible(&low, &high, "ownership.lower_only.low_probe").await;

    high.shutdown().await;
    low.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn higher_id_seed_only_converges_instead_of_waiting_forever_for_inbound() {
    let (high, low) = create_ordered_tls_pair("ownership-higher-only-a", "ownership-higher-only-b")
        .await
        .expect("ordered pair");

    seed_peer(&high, &low)
        .await
        .expect("higher-only seed must converge by dialing instead of suppressing outbound");

    assert_probe_visible(&high, &low, "ownership.higher_only.high_probe").await;
    assert_probe_visible(&low, &high, "ownership.higher_only.low_probe").await;

    high.shutdown().await;
    low.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mutual_seed_converges_to_single_usable_pair_without_blocking_actor_visibility() {
    let (high, low) = create_ordered_tls_pair("ownership-mutual-a", "ownership-mutual-b")
        .await
        .expect("ordered pair");

    let (low_result, high_result) = tokio::join!(seed_peer(&low, &high), seed_peer(&high, &low));
    low_result.expect("low->high seed must converge");
    high_result.expect("high->low seed must converge");

    assert_probe_visible(&high, &low, "ownership.mutual.high_probe").await;
    assert_probe_visible(&low, &high, "ownership.mutual.low_probe").await;

    high.shutdown().await;
    low.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn random_non_empty_seed_subset_chaos_converges_for_actor_visibility() {
    for idx in 0..12 {
        let (high, low) = create_ordered_tls_pair(
            &format!("ownership-chaos-{idx}-a"),
            &format!("ownership-chaos-{idx}-b"),
        )
        .await
        .expect("ordered pair");

        match idx % 3 {
            0 => seed_peer(&high, &low)
                .await
                .expect("higher-only seed must converge"),
            1 => seed_peer(&low, &high)
                .await
                .expect("lower-only seed must converge"),
            _ => {
                let (low_result, high_result) =
                    tokio::join!(seed_peer(&low, &high), seed_peer(&high, &low));
                low_result.expect("chaos low->high seed must converge");
                high_result.expect("chaos high->low seed must converge");
            }
        }

        assert_probe_visible(&high, &low, &format!("ownership.chaos.{idx}.high_probe")).await;
        assert_probe_visible(&low, &high, &format!("ownership.chaos.{idx}.low_probe")).await;

        high.shutdown().await;
        low.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn configured_peer_id_mismatch_is_reported_as_tls_identity_failure() {
    let client = create_tls_node_with_keypair(
        KeyPair::new_for_testing("ownership-tls-client"),
        fast_gossip_config(),
    )
    .await
    .expect("client node");
    let server = create_tls_node_with_keypair(
        KeyPair::new_for_testing("ownership-tls-server"),
        fast_gossip_config(),
    )
    .await
    .expect("server node");
    let wrong_peer = KeyPair::new_for_testing("ownership-tls-wrong-peer").peer_id();

    assert_ne!(wrong_peer, server.registry.peer_id);

    let err = client
        .add_peer(&wrong_peer)
        .await
        .connect(&server.registry.bind_addr)
        .await
        .expect_err("server certificate NodeId must match the configured peer id");
    let err_text = err.to_string();
    assert!(
        err_text.contains("TLS handshake failed") && err_text.contains("NodeId mismatch"),
        "identity mismatch must be visible as TLS identity failure, got: {err_text}"
    );
    assert!(
        !client
            .registry
            .has_connection_to_peer(&server.registry.peer_id)
            .await,
        "client must not publish a session to the real server id after a configured-id mismatch"
    );
    assert!(
        !client.registry.has_connection_to_peer(&wrong_peer).await,
        "client must not publish a session to the configured wrong peer id"
    );

    client.shutdown().await;
    server.shutdown().await;
}
