//! Phase 3B hard-cut guard: transport failures never become membership or
//! ownership truth inside `icanact-remote`.

use std::fs::read_to_string;
use std::path::Path;

#[test]
fn peer_health_consensus_surface_is_deleted() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let files = [
        "src/config.rs",
        "src/connection_pool/pool_connect.rs",
        "src/handle.rs",
        "src/lib.rs",
        "src/protocol.rs",
        "src/registry.rs",
    ];
    let forbidden = [
        "PeerHealthMode",
        "LegacyConsensus",
        "peer_health_consensus_enabled",
        "query_peer_health_consensus",
        "check_peer_consensus",
        "peer_health_reports",
        "pending_peer_failures",
        "PeerHealthStatus",
        "PendingFailure",
    ];

    let mut found = Vec::new();
    for file in files {
        let source = read_to_string(root.join(file)).expect("tracked source must be readable");
        for token in forbidden {
            if source.contains(token) {
                found.push(format!("{file}: {token}"));
            }
        }
    }

    assert!(
        found.is_empty(),
        "transport-only hard cut still exposes peer-health consensus:\n{}",
        found.join("\n"),
    );
}
