mod common;

use common::{
    DynError, connect_bidirectional, create_ordered_tls_pair, register_probe_and_wait_visible,
};
use icanact_remote::{GossipRegistryHandle, PeerClockSnapshot};
use std::time::{Duration, Instant};
use tokio::time::sleep;

async fn wait_for_clock_snapshot(
    handle: &GossipRegistryHandle<icanact_remote::BuilderTlsBootstrap>,
    peer_addr: std::net::SocketAddr,
    timeout: Duration,
) -> PeerClockSnapshot {
    let started = Instant::now();
    while started.elapsed() < timeout {
        if let Some(snapshot) = handle.peer_clock_snapshot(&peer_addr) {
            return snapshot;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "timed out waiting for clock calibration snapshot for peer {}; current snapshots: {:?}",
        peer_addr,
        handle.peer_clock_snapshots()
    );
}

fn print_snapshot(node: &str, snapshot: PeerClockSnapshot) {
    println!(
        "clock_calibration_e2e node={} peer={} sample_id={} offset_ns={} offset_ms={:.6} rtt_ns={} rtt_ms={:.6} error_bound_ns={} sample_count={}",
        node,
        snapshot.peer_addr,
        snapshot.sample_id,
        snapshot.offset_ns,
        snapshot.offset_ns as f64 / 1_000_000.0,
        snapshot.rtt_ns,
        snapshot.rtt_ns as f64 / 1_000_000.0,
        snapshot.error_bound_ns,
        snapshot.sample_count,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_gossip_clock_calibration_exposes_peer_timing_snapshots() -> Result<(), DynError> {
    let (node_a, node_b) =
        create_ordered_tls_pair("clock_calibration_e2e_a", "clock_calibration_e2e_b").await?;
    connect_bidirectional(&node_a, &node_b).await?;

    assert!(
        register_probe_and_wait_visible(
            &node_a,
            &node_b,
            "clock-calibration-e2e-probe",
            Duration::from_secs(5),
        )
        .await,
        "actor registration should propagate over real TLS/gossip wiring"
    );

    let a_view_of_b =
        wait_for_clock_snapshot(&node_a, node_b.registry.bind_addr, Duration::from_secs(5)).await;
    let b_view_of_a =
        wait_for_clock_snapshot(&node_b, node_a.registry.bind_addr, Duration::from_secs(5)).await;

    print_snapshot("a", a_view_of_b);
    print_snapshot("b", b_view_of_a);

    for snapshot in [a_view_of_b, b_view_of_a] {
        assert!(snapshot.sample_count >= 1);
        assert_eq!(snapshot.error_bound_ns, snapshot.rtt_ns / 2);
        assert!(!snapshot.is_stale_at(snapshot.sampled_at_wall_ns));
        assert!(
            snapshot.offset_ns.unsigned_abs() < 1_000_000_000,
            "loopback nodes on one host should not report >1s clock offset: {:?}",
            snapshot
        );
    }

    node_a.shutdown().await;
    node_b.shutdown().await;
    Ok(())
}
