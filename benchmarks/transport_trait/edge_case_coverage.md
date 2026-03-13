# Transport Trait Edge-Case Coverage (Sprint 7)

This matrix maps mandatory edge cases to automated tests and current status.

| Area | Edge case | Automated test(s) | Status |
|---|---|---|---|
| Transport handshake/selection | `GossipConfig.key_pair` mismatches transport identity secret | `/Users/greg/dev/icanact-remote/tests/transport_edge_cases.rs` (`tdd_transport_stack_rejects_mismatched_keypair`) | PASS |
| Transport handshake/selection | Missing `key_pair` is auto-derived from stack identity secret | `/Users/greg/dev/icanact-remote/tests/transport_edge_cases.rs` (`tdd_transport_stack_populates_missing_keypair_and_enables_tls`) | PASS |
| Identity/auth/session binding | Peer identity mismatch rejected on TLS handshake | `/Users/greg/dev/icanact-remote/tests/tls_integration.rs` (`test_impersonation_prevention`) | PASS |
| Identity/auth/session binding | Peer identity mismatch rejected on signed Noise auth handshake | `/Users/greg/dev/icanact-remote/src/noise_auth.rs` (`noise_auth_rejects_wrong_expected_peer`) | PASS |
| Identity/auth/session binding | Mutual signed Noise auth establishes peer identities on both ends | `/Users/greg/dev/icanact-remote/src/noise_auth.rs` (`noise_auth_mutual_success`) | PASS |
| Transport handshake/selection | `NativeQuicStack` rejects `GossipConfig.key_pair` that does not match transport identity secret | `/Users/greg/dev/icanact-remote/tests/transport_edge_cases.rs` (`tdd_native_quic_stack_rejects_mismatched_keypair`) | PASS |
| Link termination detection | Forced peer failure emits disconnect callback with peer identity | `/Users/greg/dev/icanact-remote/tests/transport_edge_cases.rs` (`tdd_link_detection_invokes_disconnect_handler_with_peer_id`) | PASS |
| Link termination detection | Forced peer failure emits disconnect callback with peer identity on Noise transport | `/Users/greg/dev/icanact-remote/tests/noise_integration.rs` (`noise_disconnect_handler_includes_peer_id`) | PASS |
| Link termination detection | UDP detector terminates silent peer and emits disconnect callback with peer identity | `/Users/greg/dev/icanact-remote/tests/transport_edge_cases.rs` (`tdd_udp_detector_terminates_silent_peer_and_emits_disconnect`) | PASS |
| Mixed routing/fan-out | TLS+QUIC line topology converges actor routing across heterogeneous links | `/Users/greg/dev/icanact-remote/tests/mixed_transport_routing_e2e.rs` (`mixed_tls_quic_line_topology_routes_actor_state_end_to_end`) | PASS |
| Mixed routing/fan-out | Fan-out to healthy TLS segment continues after QUIC-side segment failure | `/Users/greg/dev/icanact-remote/tests/mixed_transport_fanout_e2e.rs` (`mixed_tls_quic_fanout_remains_available_when_one_segment_fails`) | PASS |
| Mixed routing/fan-out | Failed mixed segment is isolated while healthy segment still delivers | `/Users/greg/dev/icanact-remote/tests/mixed_transport_failure_isolation_e2e.rs` (`mixed_tls_quic_segment_failure_isolated_from_healthy_peers`) | PASS |
| Mixed routing/fan-out | TLS+NativeQUIC line topology converges actor routing across heterogeneous links | `/Users/greg/dev/icanact-remote/tests/mixed_transport_native_quic_routing_e2e.rs` (`mixed_tls_native_quic_line_topology_routes_actor_state_end_to_end`) | PASS |
| Mixed routing/fan-out | Fan-out to healthy TLS segment continues after NativeQUIC-side segment failure | `/Users/greg/dev/icanact-remote/tests/mixed_transport_native_quic_fanout_e2e.rs` (`mixed_tls_native_quic_fanout_remains_available_when_one_segment_fails`) | PASS |
| Mixed routing/fan-out | Failed mixed NativeQUIC segment is isolated while healthy segment still delivers | `/Users/greg/dev/icanact-remote/tests/mixed_transport_native_quic_failure_isolation_e2e.rs` (`mixed_tls_native_quic_segment_failure_isolated_from_healthy_peers`) | PASS |
| Mixed routing/fan-out | UDP line topology converges actor routing end-to-end | `/Users/greg/dev/icanact-remote/tests/mixed_transport_udp_routing_e2e.rs` (`udp_line_topology_routes_actor_state_end_to_end`) | PASS |
| Mixed routing/fan-out | Fan-out to healthy segment continues after one UDP segment failure | `/Users/greg/dev/icanact-remote/tests/mixed_transport_udp_fanout_e2e.rs` (`udp_fanout_remains_available_when_one_segment_fails`) | PASS |
| Mixed routing/fan-out | Failed UDP segment is isolated while healthy segment still delivers | `/Users/greg/dev/icanact-remote/tests/mixed_transport_udp_failure_isolation_e2e.rs` (`udp_segment_failure_isolated_from_healthy_peers`) | PASS |

## Deferred Rows (Later Sprints)

| Area | Deferred to sprint |
|---|---|
| Mixed-topology partition + convergence edge cases | Sprint 5-6 |
