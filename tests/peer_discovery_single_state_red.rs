#[test]
fn peer_discovery_has_one_state_map_and_no_shadow_ledgers() {
    let source = include_str!("../src/peer_discovery.rs");
    for retired in [
        "connected_peers:",
        "pending_peers:",
        "failed_peers:",
        "connected_count_unified",
        "pending_count_unified",
        "failed_count_unified",
        "Also update legacy",
        "backward compatibility",
    ] {
        assert!(
            !source.contains(retired),
            "peer discovery still contains retired overlapping state `{retired}`"
        );
    }
}
