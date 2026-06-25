use std::time::Duration;

/// Default retry interval for failed peer connections
pub const DEFAULT_PEER_RETRY_SECONDS: u64 = 5;
pub const DEFAULT_PEER_SUPERVISOR_SECONDS: u64 = 1;

/// Default maximum failed connection attempts before marking peer as failed
pub const DEFAULT_MAX_PEER_FAILURES: usize = 2;

/// Default gossip interval in seconds
pub const DEFAULT_GOSSIP_INTERVAL_SECS: u64 = 5;

/// Default cleanup interval in seconds  
pub const DEFAULT_CLEANUP_INTERVAL_SECS: u64 = 60;

/// Default dead peer timeout in seconds (15 minutes)
pub const DEFAULT_DEAD_PEER_TIMEOUT_SECS: u64 = 900;

/// Default max concurrent ask inflight
pub const DEFAULT_ASK_WINDOW: usize = 128;

/// Default cap on simultaneous in-flight (post-accept, pre-identified) inbound
/// handshakes. Bounds half-open inbound tasks.
pub const DEFAULT_MAX_INFLIGHT_INBOUND_HANDSHAKES: usize = 256;

/// Default small cluster threshold - clusters with this many nodes or fewer use full sync
/// Set to 0 to always use delta sync when possible
pub const DEFAULT_SMALL_CLUSTER_THRESHOLD: usize = 5;

/// Default: keep NAT role-based reconnect suppression disabled in production.
pub const DEFAULT_NAT_ROLE_RECONNECT_ENABLED: bool = false;

/// Default TCP keepalive idle time (seconds)
pub const DEFAULT_TCP_KEEPALIVE_IDLE_SECS: u64 = 1;

/// Default TCP keepalive interval (seconds)
pub const DEFAULT_TCP_KEEPALIVE_INTERVAL_SECS: u64 = 1;

/// Default TCP keepalive probe retries
pub const DEFAULT_TCP_KEEPALIVE_RETRIES: u32 = 3;

/// TCP keepalive configuration for fast disconnect detection during idle periods
#[derive(Debug, Clone)]
pub struct TcpKeepaliveConfig {
    /// Idle time before sending keepalive probes
    pub idle: Duration,
    /// Interval between keepalive probes
    pub interval: Duration,
    /// Number of failed probes before declaring the socket dead (platform dependent)
    pub retries: Option<u32>,
}

/// Domain-specific recovery behavior for cached peer connections.
///
/// Defaults are intentionally conservative for general actor traffic: a slow actor handler should
/// not normally cause a transport session to be torn down. Latency-sensitive domains can opt in
/// through their `RegistryTransportBootstrap`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ConnectionRecoveryPolicy {
    /// On actor-ask timeout, evict the cached transport session for the target peer.
    pub evict_peer_on_ask_timeout: bool,
    /// On actor-ask cancellation, evict the cached transport session for the target peer.
    pub evict_peer_on_ask_cancel: bool,
    /// After evicting on timeout, reconnect and retry the actor ask once.
    pub retry_actor_ask_once_after_timeout: bool,
    /// Number of *consecutive* ask outcomes a consumer classifies as
    /// streak-timeouts (via the `note_peer_ask_*` mechanism) before the cached
    /// transport session is evicted. `0` disables the streak mechanism.
    ///
    /// This is the single, generic eviction mechanism: the consumer supplies
    /// the domain classification (which errors count, which RPCs participate)
    /// and icanact-remote owns the per-peer counter, the threshold, the
    /// reset-on-success, and the instance-guarded teardown. It lets
    /// latency-sensitive consumers ride over transient transport blips
    /// instead of evicting on the first timeout.
    pub consecutive_timeout_threshold: u8,
}

impl ConnectionRecoveryPolicy {
    pub const fn aggressive_ask_timeout_recovery() -> Self {
        Self {
            evict_peer_on_ask_timeout: true,
            evict_peer_on_ask_cancel: true,
            retry_actor_ask_once_after_timeout: true,
            consecutive_timeout_threshold: 0,
        }
    }

    /// Streak-based recovery: evict only after `threshold` consecutive
    /// consumer-classified streak-timeouts (hard transport faults still evict
    /// immediately). Rides over transient blips shorter than the streak. Used
    /// by latency-sensitive consumers such as consensus layers.
    pub const fn streak_ask_timeout_recovery(threshold: u8) -> Self {
        Self {
            evict_peer_on_ask_timeout: false,
            evict_peer_on_ask_cancel: false,
            retry_actor_ask_once_after_timeout: false,
            consecutive_timeout_threshold: threshold,
        }
    }
}

/// Configuration for the gossip registry
#[derive(Debug, Clone)]
pub struct GossipConfig {
    /// Node's key pair for identification (must be unique in the cluster)
    pub key_pair: Option<crate::KeyPair>,
    /// Interval between gossip rounds
    pub gossip_interval: Duration,
    /// Maximum number of peers to gossip to in each round
    pub max_gossip_peers: usize,
    /// Time-to-live for actor entries
    pub actor_ttl: Duration,
    /// Cleanup interval for stale entries
    pub cleanup_interval: Duration,
    /// Connection timeout for outbound connections
    pub connection_timeout: Duration,
    /// Response timeout for gossip exchanges
    pub response_timeout: Duration,
    /// Maximum message size in bytes
    pub max_message_size: usize,
    /// Optional schema/version hash for protocol guardrails (v3 header).
    pub schema_hash: Option<u64>,
    /// Maximum number of failed connection attempts before marking peer as failed
    pub max_peer_failures: usize,
    /// Time to wait before retrying failed peers
    pub peer_retry_interval: Duration,
    /// How often the p2p configured-peer supervisor runs: for every
    /// `configure_peer`d (required) peer it ensures a direct point-to-point
    /// connection (dials only when down, no-op when already connected) and
    /// surfaces a liveness signal. Point-to-point only — no gossip, no
    /// broadcast. Defaults to 1s.
    pub peer_supervisor_interval: Duration,
    /// Maximum number of deltas to keep in history
    pub max_delta_history: usize,
    /// Force full sync after this many delta exchanges
    pub full_sync_interval: u64,
    /// Maximum number of pooled connections
    pub max_pooled_connections: usize,
    /// Idle connection timeout for pool
    pub idle_connection_timeout: Duration,
    /// Timeout for connections checked out too long
    pub checkout_timeout: Duration,
    /// How often to run vector clock garbage collection  
    pub vector_clock_gc_frequency: Duration,
    /// How long to retain node entries in vector clocks after last seen
    pub vector_clock_retention_period: Duration,
    /// Maximum number of entries in a vector clock before compaction
    pub max_vector_clock_size: usize,
    /// Threshold for small clusters
    pub small_cluster_threshold: usize,
    /// Maximum time to wait for server to become ready before bootstrap
    pub bootstrap_readiness_timeout: Duration,
    /// Interval between readiness checks
    pub bootstrap_readiness_check_interval: Duration,
    /// Maximum bootstrap retry attempts
    pub bootstrap_max_retries: usize,
    /// Delay between bootstrap retry attempts
    pub bootstrap_retry_delay: Duration,
    /// Enable immediate propagation for urgent changes
    pub immediate_propagation_enabled: bool,
    /// Gossip fanout multiplier for urgent changes
    pub urgent_gossip_fanout: usize,
    /// Maximum retries for immediate propagation
    pub max_immediate_retries: usize,
    /// Timeout for causal consistency operations
    pub causal_consistency_timeout: Duration,
    /// Target in-flight ask window per connection (used for queue/pool sizing)
    pub ask_window: usize,
    /// How long to keep disconnected peers before removing them (default: 15 minutes)
    pub dead_peer_timeout: Duration,
    /// Enable peer role-aware NAT reconnect suppression for inbound-only, undialable peers.
    /// This is an internal retry-policy toggle and does not change wire behavior.
    pub nat_role_reconnect_enabled: bool,
    /// TCP keepalive settings for faster idle disconnect detection
    pub tcp_keepalive: Option<TcpKeepaliveConfig>,
    /// Optional domain-specific recovery policy for cached transport sessions.
    pub connection_recovery: ConnectionRecoveryPolicy,

    // =================== Peer Discovery Configuration ===================
    /// Advertised address for peer discovery (what we tell others to connect to)
    /// If None, uses the listening address
    pub advertise_address: Option<std::net::SocketAddr>,
    /// Enable automatic peer discovery via gossip (default: false for safe rollout)
    pub enable_peer_discovery: bool,
    /// Maximum number of peers to maintain via discovery (soft cap, default: 100)
    pub max_peers: usize,
    /// Maximum consecutive peer connection failures before removal (default: 10)
    pub max_peer_discovery_failures: usize,
    /// Interval between peer list gossip (default: 30s)
    pub peer_gossip_interval: Option<Duration>,
    /// Maximum number of peers to send peer list gossip to (default: 3)
    pub max_peer_gossip_targets: usize,
    /// Allow discovery of private IP addresses (default: true)
    pub allow_private_discovery: bool,
    /// Allow discovery of loopback addresses (default: false)
    pub allow_loopback_discovery: bool,
    /// Allow discovery of link-local addresses (default: false)
    pub allow_link_local_discovery: bool,
    /// Time-to-live for failed peers before eviction (default: 6 hours)
    pub fail_ttl: Duration,
    /// Time-to-live for pending peers before eviction (default: 1 hour)
    pub pending_ttl: Duration,
    /// Time-to-live for stale peers before eviction (default: 24 hours)
    pub stale_ttl: Duration,
    /// Maximum capacity for known_peers LRU cache (default: 10_000)
    pub known_peers_capacity: usize,
    /// Number of connected peers required before recording mesh_formation_time_ms
    /// Set to 0 to disable metric tracking.
    pub mesh_formation_target: usize,
    /// DNS name to advertise in gossip (e.g., "data-feeder.default.svc.cluster.local:9000")
    /// When set, this DNS name is included in gossip messages so peers can re-resolve
    /// the address if the underlying IP changes (e.g., Kubernetes pod restarts)
    pub advertise_dns: Option<String>,
    /// How long to keep writing to a peer that never sends responses
    /// back before treating subsequent rounds as failures.
    ///
    /// `apply_gossip_results` records a round as `Ok(_)` whenever the
    /// outbound write returned successfully — but on a persistent
    /// connection that may just mean the kernel buffered the bytes for
    /// a peer that has since stopped reading. The response-asymmetry
    /// detector compares `current_time - last_response_received_ms` to
    /// this window: if we've been writing into a black hole for longer
    /// than this window without ever seeing an inbound response, the
    /// next no-response round increments `failures`, eventually
    /// tripping `max_peer_failures` and firing the dead-peer cleanup
    /// hook in `registry::handle_peer_death`.
    ///
    /// Default: 10 s. Set very small (e.g., 500 ms) in tests for
    /// determinism.
    pub peer_liveness_window: Duration,
    /// Maximum number of inbound connections allowed to be in the
    /// post-accept/pre-identified handshake stage simultaneously. Caps half-open
    /// inbound tasks so a flood of TCP connects that complete the TLS handshake
    /// but never send a first frame cannot spawn unbounded handshake tasks.
    /// Acquired at accept, released when the handshake task finishes.
    /// Default: 256.
    pub max_inflight_inbound_handshakes: usize,
}

impl Default for GossipConfig {
    fn default() -> Self {
        // Read gossip interval from environment variable for testing flexibility
        // Example: ICANACT_GOSSIP_INTERVAL_MS=100 for fast testing (100ms)
        //          ICANACT_GOSSIP_INTERVAL_MS=5000 for production (5s default)
        let gossip_interval = if let Ok(ms_str) = std::env::var("ICANACT_GOSSIP_INTERVAL_MS") {
            if let Ok(ms) = ms_str.parse::<u64>() {
                Duration::from_millis(ms)
            } else {
                tracing::warn!(
                    "Invalid ICANACT_GOSSIP_INTERVAL_MS value '{}', using default {}ms",
                    ms_str,
                    DEFAULT_GOSSIP_INTERVAL_SECS * 1000
                );
                Duration::from_secs(DEFAULT_GOSSIP_INTERVAL_SECS)
            }
        } else {
            Duration::from_secs(DEFAULT_GOSSIP_INTERVAL_SECS)
        };

        Self {
            key_pair: None,
            gossip_interval,
            max_gossip_peers: 3,
            // Increase default actor TTL to avoid premature expiry of distributed actor discovery
            // This prevents cases where peers stop discovering actors after a few minutes of idle time.
            // If needed, this can still be overridden by callers providing a custom GossipConfig.
            actor_ttl: Duration::from_secs(86_400),
            cleanup_interval: Duration::from_secs(DEFAULT_CLEANUP_INTERVAL_SECS),
            connection_timeout: Duration::from_secs(10),
            response_timeout: Duration::from_secs(5),
            max_message_size: 10 * 1024 * 1024, // 10MB
            schema_hash: None,
            max_peer_failures: DEFAULT_MAX_PEER_FAILURES,
            peer_retry_interval: Duration::from_secs(DEFAULT_PEER_RETRY_SECONDS),
            peer_supervisor_interval: Duration::from_secs(DEFAULT_PEER_SUPERVISOR_SECONDS),
            max_delta_history: 100,
            full_sync_interval: 50,     // Force full sync every 50 deltas
            max_pooled_connections: 20, // Allow up to 20 pooled connections
            idle_connection_timeout: Duration::from_secs(300),
            checkout_timeout: Duration::from_secs(60),
            vector_clock_gc_frequency: Duration::from_secs(300), // 5 minutes
            vector_clock_retention_period: Duration::from_secs(7200), // 2 hours (was 1 hour)
            max_vector_clock_size: 1000,                         // Compact after 1000 entries
            small_cluster_threshold: DEFAULT_SMALL_CLUSTER_THRESHOLD,
            bootstrap_readiness_timeout: Duration::from_secs(30),
            bootstrap_readiness_check_interval: Duration::from_millis(100),
            bootstrap_max_retries: 5, // Increased from 3 to handle startup race conditions
            bootstrap_retry_delay: Duration::from_secs(5),
            immediate_propagation_enabled: true,
            urgent_gossip_fanout: 5,
            max_immediate_retries: 3,
            causal_consistency_timeout: Duration::from_millis(500),
            ask_window: DEFAULT_ASK_WINDOW,
            dead_peer_timeout: Duration::from_secs(DEFAULT_DEAD_PEER_TIMEOUT_SECS),
            nat_role_reconnect_enabled: DEFAULT_NAT_ROLE_RECONNECT_ENABLED,
            tcp_keepalive: Some(TcpKeepaliveConfig {
                idle: Duration::from_secs(DEFAULT_TCP_KEEPALIVE_IDLE_SECS),
                interval: Duration::from_secs(DEFAULT_TCP_KEEPALIVE_INTERVAL_SECS),
                retries: Some(DEFAULT_TCP_KEEPALIVE_RETRIES),
            }),
            connection_recovery: ConnectionRecoveryPolicy::default(),
            // Peer discovery defaults
            advertise_address: None,
            enable_peer_discovery: false, // Safe rollout: disabled by default
            max_peers: 100,
            max_peer_discovery_failures: 10,
            peer_gossip_interval: Some(Duration::from_secs(5)),
            max_peer_gossip_targets: 3,
            allow_private_discovery: true,
            allow_loopback_discovery: false,
            allow_link_local_discovery: false,
            fail_ttl: Duration::from_secs(6 * 60 * 60), // 6 hours
            pending_ttl: Duration::from_secs(60 * 60),  // 1 hour
            stale_ttl: Duration::from_secs(24 * 60 * 60), // 24 hours
            known_peers_capacity: 10_000,
            mesh_formation_target: 2,
            // Read advertise_dns from environment for Kubernetes-style deployments
            // Example: ICANACT_ADVERTISE_DNS=data-feeder.default.svc.cluster.local:9400
            advertise_dns: std::env::var("ICANACT_ADVERTISE_DNS").ok(),
            peer_liveness_window: Duration::from_secs(10),
            max_inflight_inbound_handshakes: DEFAULT_MAX_INFLIGHT_INBOUND_HANDSHAKES,
        }
    }
}

impl GossipConfig {
    /// Enforce runtime invariants on a consumer-supplied config, clamping
    /// unsafe values to safe ones with a warning rather than silently honoring
    /// them. Called once when the config enters the registry; never on the hot
    /// path.
    ///
    /// Invariant: `peer_liveness_window >= peer_gossip_interval * 2`. The
    /// response-asymmetry detector compares elapsed time-since-last-response to
    /// `peer_liveness_window`. If that window is shorter than two gossip
    /// intervals, a single delayed inbound peer-gossip payload can false-fail an
    /// otherwise healthy (non-required) peer. A consumer that sets a too-small
    /// window would silently lose this protection; clamp it up instead.
    pub fn validate_and_normalize(&mut self) {
        if let Some(interval) = self.peer_gossip_interval {
            let min_window = interval.saturating_mul(2);
            if self.peer_liveness_window < min_window {
                tracing::warn!(
                    peer_liveness_window_ms = self.peer_liveness_window.as_millis(),
                    peer_gossip_interval_ms = interval.as_millis(),
                    clamped_to_ms = min_window.as_millis(),
                    "peer_liveness_window < peer_gossip_interval*2; clamping up to avoid \
false-failing healthy peers on a single delayed inbound peer-gossip payload"
                );
                self.peer_liveness_window = min_window;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = GossipConfig::default();

        assert_eq!(config.gossip_interval, Duration::from_secs(5));
        assert_eq!(config.max_gossip_peers, 3);
        assert_eq!(config.actor_ttl, Duration::from_secs(86_400)); // 24 hours
        assert_eq!(config.cleanup_interval, Duration::from_secs(60));
        assert_eq!(config.connection_timeout, Duration::from_secs(10));
        assert_eq!(config.response_timeout, Duration::from_secs(5));
        assert_eq!(config.max_message_size, 10 * 1024 * 1024);
        assert_eq!(config.max_peer_failures, 2);
        assert_eq!(
            config.peer_retry_interval,
            Duration::from_secs(DEFAULT_PEER_RETRY_SECONDS)
        );
        assert_eq!(config.max_delta_history, 100);
        assert_eq!(config.full_sync_interval, 50);
        assert_eq!(config.max_pooled_connections, 20);
        assert_eq!(config.idle_connection_timeout, Duration::from_secs(300));
        assert_eq!(config.checkout_timeout, Duration::from_secs(60));
        assert_eq!(config.vector_clock_gc_frequency, Duration::from_secs(300));
        assert_eq!(
            config.vector_clock_retention_period,
            Duration::from_secs(7200)
        );
        assert_eq!(config.small_cluster_threshold, 5);
        assert_eq!(config.bootstrap_readiness_timeout, Duration::from_secs(30));
        assert_eq!(
            config.bootstrap_readiness_check_interval,
            Duration::from_millis(100)
        );
        assert_eq!(config.bootstrap_max_retries, 5);
        assert_eq!(config.bootstrap_retry_delay, Duration::from_secs(5));
        assert!(config.immediate_propagation_enabled);
        assert_eq!(config.urgent_gossip_fanout, 5);
        assert_eq!(config.max_immediate_retries, 3);
        assert_eq!(
            config.causal_consistency_timeout,
            Duration::from_millis(500)
        );
        assert_eq!(config.ask_window, DEFAULT_ASK_WINDOW);
        assert_eq!(config.dead_peer_timeout, Duration::from_secs(900));
        assert!(!config.nat_role_reconnect_enabled);
        assert_eq!(
            config.tcp_keepalive.as_ref().and_then(|ka| ka.retries),
            Some(DEFAULT_TCP_KEEPALIVE_RETRIES)
        );
        assert_eq!(
            config.connection_recovery,
            ConnectionRecoveryPolicy::default()
        );
        // Peer discovery defaults
        assert!(config.advertise_address.is_none());
        assert!(!config.enable_peer_discovery); // Disabled by default for safe rollout
        assert_eq!(config.max_peers, 100);
        assert_eq!(config.max_peer_discovery_failures, 10);
        assert_eq!(config.peer_gossip_interval, Some(Duration::from_secs(5)));
        assert_eq!(config.max_peer_gossip_targets, 3);
        assert!(config.allow_private_discovery);
        assert!(!config.allow_loopback_discovery);
        assert!(!config.allow_link_local_discovery);
        assert!(
            config.peer_liveness_window >= config.peer_gossip_interval.unwrap() * 2,
            "peer_liveness_window must allow at least two peer-gossip intervals; \
otherwise healthy peers can be false-failed by one delayed inbound peer-gossip payload"
        );
        assert_eq!(config.fail_ttl, Duration::from_secs(6 * 60 * 60));
        assert_eq!(config.pending_ttl, Duration::from_secs(60 * 60));
        assert_eq!(config.stale_ttl, Duration::from_secs(24 * 60 * 60));
        assert_eq!(config.known_peers_capacity, 10_000);
        assert_eq!(config.mesh_formation_target, 2);
    }

    #[test]
    fn validate_and_normalize_clamps_too_small_liveness_window() {
        let mut config = GossipConfig::default();
        config.peer_gossip_interval = Some(Duration::from_secs(5));
        // Violate the invariant: window < interval*2 (10s).
        config.peer_liveness_window = Duration::from_secs(3);

        config.validate_and_normalize();

        assert_eq!(
            config.peer_liveness_window,
            Duration::from_secs(10),
            "window must be clamped up to peer_gossip_interval*2"
        );
    }

    #[test]
    fn validate_and_normalize_leaves_conforming_config_unchanged() {
        let mut config = GossipConfig::default();
        config.peer_gossip_interval = Some(Duration::from_secs(5));
        config.peer_liveness_window = Duration::from_secs(30); // >= 10s, conforming.

        config.validate_and_normalize();

        assert_eq!(
            config.peer_liveness_window,
            Duration::from_secs(30),
            "conforming window must be left unchanged"
        );
    }

    #[test]
    fn validate_and_normalize_noop_when_gossip_interval_disabled() {
        let mut config = GossipConfig::default();
        config.peer_gossip_interval = None;
        config.peer_liveness_window = Duration::from_secs(1);

        config.validate_and_normalize();

        assert_eq!(
            config.peer_liveness_window,
            Duration::from_secs(1),
            "no interval means no invariant to enforce"
        );
    }

    #[test]
    fn test_config_clone() {
        let config = GossipConfig::default();
        let cloned = config.clone();

        assert_eq!(config.gossip_interval, cloned.gossip_interval);
        assert_eq!(config.max_gossip_peers, cloned.max_gossip_peers);
        assert_eq!(config.dead_peer_timeout, cloned.dead_peer_timeout);
    }

    #[test]
    fn test_aggressive_connection_recovery_policy() {
        let policy = ConnectionRecoveryPolicy::aggressive_ask_timeout_recovery();

        assert!(policy.evict_peer_on_ask_timeout);
        assert!(policy.evict_peer_on_ask_cancel);
        assert!(policy.retry_actor_ask_once_after_timeout);
    }

    #[test]
    fn test_config_debug() {
        let config = GossipConfig::default();
        let debug_str = format!("{:?}", config);

        assert!(debug_str.contains("GossipConfig"));
        assert!(debug_str.contains("gossip_interval"));
        assert!(debug_str.contains("max_gossip_peers"));
    }

    #[test]
    fn test_custom_config() {
        let config = GossipConfig {
            gossip_interval: Duration::from_secs(10),
            max_gossip_peers: 5,
            peer_retry_interval: Duration::from_secs(2),
            ..Default::default()
        };

        assert_eq!(config.gossip_interval, Duration::from_secs(10));
        assert_eq!(config.max_gossip_peers, 5);
        assert_eq!(config.peer_retry_interval, Duration::from_secs(2));
        // Other fields should have default values
        assert_eq!(config.actor_ttl, Duration::from_secs(86_400)); // 24 hours
    }

    #[test]
    fn test_peer_retry_constant() {
        assert_eq!(DEFAULT_PEER_RETRY_SECONDS, 5);
    }
}
