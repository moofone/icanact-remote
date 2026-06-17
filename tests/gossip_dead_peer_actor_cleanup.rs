//! Reproduces and regression-tests the transport-failure path for actors
//! owned by peers that have crossed `max_peer_failures`.
//!
//! ## What this guards
//!
//! Gossip detects transport failures by incrementing `peer_info.failures`.
//! Once a peer reaches `config.max_peer_failures`, it is filtered out of
//! subsequent gossip selection, but that transport-local verdict must not
//! immediately remove actors or publish `ActorRemoved` tombstones. Short
//! disconnects are expected during reconnect/failover, so actors stay
//! routable until the consensus/timeout cleanup path decides they should
//! be reclaimed.
//!
//! ## Coverage
//!
//! 1. **drop case**: peer's listener closes (process exits / network
//!    drops). Gossip rounds fail to dial it. `failures` crosses
//!    threshold. Assert its actor entries are retained until timeout.
//!
//! 2. **stale case**: peer's TCP listener stays alive but it does NOT
//!    process incoming gossip frames (deadlocked / paused). Gossip
//!    RPCs time out. Same `failures++` path → same retained-actor
//!    assertion.

use std::future::Future;
use std::sync::Once;
use std::time::{Duration, Instant};

use icanact_remote::{
    GossipConfig, GossipRegistryHandle, PeerId, RegistrationPriority, RemoteActorLocation,
    SecretKey,
};
use tokio::net::TcpListener;
use tokio::runtime::Builder;
use tokio::time::sleep;

const TEST_THREAD_STACK_SIZE: usize = 32 * 1024 * 1024;
const TEST_WORKER_STACK_SIZE: usize = 8 * 1024 * 1024;
const TEST_WORKER_THREADS: usize = 4;

type DynError = Box<dyn std::error::Error + Send + Sync>;

static CRYPTO_INIT: Once = Once::new();

fn init_crypto() {
    CRYPTO_INIT.call_once(|| {
        icanact_remote::tls::ensure_crypto_provider();
    });
}

fn run_gossip_test<F, R>(future: F) -> R
where
    F: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    std::thread::Builder::new()
        .name("gossip-dead-peer-cleanup".into())
        .stack_size(TEST_THREAD_STACK_SIZE)
        .spawn(move || {
            let rt = Builder::new_multi_thread()
                .worker_threads(TEST_WORKER_THREADS)
                .thread_stack_size(TEST_WORKER_STACK_SIZE)
                .enable_all()
                .build()
                .expect("failed to build runtime");
            rt.block_on(future)
        })
        .expect("failed to spawn test thread")
        .join()
        .expect("test thread panicked")
}

async fn create_node(config: GossipConfig) -> Result<GossipRegistryHandle, DynError> {
    init_crypto();
    let secret_key = SecretKey::generate();
    let node = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse()?,
        secret_key,
        Some(config),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await?;
    Ok(node)
}

async fn connect_pair(a: &GossipRegistryHandle, b: &GossipRegistryHandle) {
    let addr_a = a.registry.bind_addr;
    let addr_b = b.registry.bind_addr;

    a.registry
        .add_peer_with_node_id(addr_b, Some(b.registry.peer_id.to_node_id()))
        .await;
    b.registry
        .add_peer_with_node_id(addr_a, Some(a.registry.peer_id.to_node_id()))
        .await;

    a.bootstrap_non_blocking(vec![addr_b]).await;
    b.bootstrap_non_blocking(vec![addr_a]).await;
}

async fn wait_for_actor(
    node: &GossipRegistryHandle,
    name: &str,
    timeout: Duration,
) -> Option<RemoteActorLocation> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Some(loc) = node.registry.lookup_actor(name).await {
            return Some(loc);
        }
        sleep(Duration::from_millis(50)).await;
    }
    None
}

async fn peer_failures(node: &GossipRegistryHandle, addr: std::net::SocketAddr) -> usize {
    let state = node.registry.gossip_state.lock().await;
    state.peers.get(&addr).map(|p| p.failures).unwrap_or(0)
}

async fn seed_known_actor_for_synthetic_peer(
    node: &GossipRegistryHandle,
    actor_name: &str,
) -> Result<(std::net::SocketAddr, PeerId), DynError> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let peer_addr = listener.local_addr()?;
    drop(listener);

    let peer_id = SecretKey::generate().to_keypair().peer_id();
    node.registry
        .add_peer_with_node_id(peer_addr, Some(peer_id.to_node_id()))
        .await;
    node.registry.actor_state.known_actors.upsert_sync(
        actor_name.to_string(),
        RemoteActorLocation::new_with_peer(peer_addr, peer_id.clone()),
    );
    {
        let mut state = node.registry.gossip_state.lock().await;
        state
            .peer_to_actors
            .entry(peer_addr)
            .or_default()
            .insert(actor_name.to_string());
    }

    Ok((peer_addr, peer_id))
}

async fn diagnose_peer(node: &GossipRegistryHandle, addr: std::net::SocketAddr) -> String {
    let state = node.registry.gossip_state.lock().await;
    match state.peers.get(&addr) {
        Some(p) => format!(
            "failures={} last_attempt={} last_success={} last_response_received_ms={} peer_count={}",
            p.failures,
            p.last_attempt,
            p.last_success,
            p.last_response_received_ms,
            state.peers.len()
        ),
        None => format!(
            "peer ABSENT from gossip_state; peer_count={}",
            state.peers.len()
        ),
    }
}

async fn wait_for_peer_dead(
    node: &GossipRegistryHandle,
    addr: std::net::SocketAddr,
    max_failures: usize,
    timeout: Duration,
) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if peer_failures(node, addr).await >= max_failures {
            return true;
        }
        sleep(Duration::from_millis(100)).await;
    }
    false
}

const DEAD_ACTOR_NAME: &str = "icanact/test/dead-peer-owned-actor/v1";

async fn assert_transport_failure_retains_actor(
    node: &GossipRegistryHandle,
    peer_addr: std::net::SocketAddr,
    actor_name: &str,
) {
    assert!(
        node.registry.lookup_actor(actor_name).await.is_some(),
        "transport failure must retain the remote actor until consensus/timeout cleanup"
    );

    let state = node.registry.gossip_state.lock().await;
    assert!(
        state
            .peer_to_actors
            .get(&peer_addr)
            .is_some_and(|actors| actors.contains(actor_name)),
        "transport failure must retain peer_to_actors attribution for timeout cleanup"
    );
    let queued_actor_removed = state
        .urgent_changes
        .iter()
        .chain(state.pending_changes.iter())
        .any(|change| {
            matches!(
                change,
                icanact_remote::registry::RegistryChange::ActorRemoved { name, .. }
                    if name == actor_name
            )
        });
    assert!(
        !queued_actor_removed,
        "transport failure must not queue ActorRemoved before consensus/timeout cleanup"
    );
}

/// Case 1: peer drops its TCP listener (process exit / crash).
///
/// Gossip rounds attempt to write into the publisher's cached
/// persistent connection to the dead peer. On a real network /
/// kernel, this eventually surfaces as a hard transport error —
/// either via TCP keepalive (`TcpKeepaliveConfig`) or via a write
/// returning `BrokenPipe` once kernel buffers fill — at which point
/// `handle_peer_connection_failure` fires and the retained-actor
/// transport-failure contract is exercised.
///
/// In an in-process loopback test on a clean shutdown, the kernel
/// keeps accepting bytes into its send buffer for many seconds, so
/// the failure signal never reaches our gossip layer within the test
/// window. The stale-peer test below exercises the same failure accounting
/// via a deterministic gossip-RPC result path.
///
/// Ignored by default. To exercise this case end-to-end, run with
/// `--ignored` and a long timeout — production stratum at
/// `stratum-devnet-a` reproduces the death-detection naturally after
/// the gossip layer's keepalive trips (observed at `failures=3` in
/// real logs).
#[ignore = "requires real-network TCP keepalive timing; covered behaviorally by the stale-peer test below"]
#[test]
fn known_actors_owned_by_dropped_peer_are_retained() -> Result<(), DynError> {
    run_gossip_test(async {
        let config = GossipConfig {
            gossip_interval: Duration::from_millis(100),
            // Default retry_interval is 5 s, which makes "three failed
            // rounds" take ~10 s. Shrink so the test finishes quickly.
            peer_retry_interval: Duration::from_millis(200),
            connection_timeout: Duration::from_millis(250),
            response_timeout: Duration::from_millis(250),
            // Default liveness window is 10 s; tighten for deterministic
            // response-asymmetry detection in CI.
            peer_liveness_window: Duration::from_millis(500),
            max_peer_failures: 3,
            ..Default::default()
        };

        let publisher = create_node(config.clone()).await?;
        let subscriber = create_node(config.clone()).await?;
        let sub_addr = subscriber.registry.bind_addr;

        connect_pair(&publisher, &subscriber).await;

        subscriber
            .registry
            .register_actor_with_priority(
                DEAD_ACTOR_NAME.to_string(),
                RemoteActorLocation::new_with_peer(sub_addr, subscriber.registry.peer_id.clone()),
                RegistrationPriority::Immediate,
            )
            .await?;

        let loc = wait_for_actor(&publisher, DEAD_ACTOR_NAME, Duration::from_secs(3))
            .await
            .ok_or_else::<DynError, _>(|| "subscriber's actor never propagated".into())?;
        assert_eq!(loc.peer_id, subscriber.registry.peer_id);

        // Subscriber goes offline ungracefully (port becomes refused).
        subscriber.shutdown().await;

        // Wait for publisher's gossip to mark subscriber as dead via
        // failure threshold. With 100 ms gossip interval and threshold
        // 3, this should happen within ~1 s.
        let dead_in_time =
            wait_for_peer_dead(&publisher, sub_addr, 3, Duration::from_secs(10)).await;
        let diag = diagnose_peer(&publisher, sub_addr).await;
        assert!(
            dead_in_time,
            "publisher's gossip should mark subscriber dead after 3 failed rounds; {diag}"
        );

        assert_transport_failure_retains_actor(&publisher, sub_addr, DEAD_ACTOR_NAME).await;

        publisher.shutdown().await;
        Ok(())
    })
}

/// Case 2: peer's TCP stays accepting but it never processes incoming
/// gossip frames (deadlocked, paused, app-level hang).
///
/// We simulate this by applying successful send results with no gossip
/// response while `last_response_received_ms` is already outside the liveness
/// window. Gossip rounds on the publisher side see "kernel accepted write,
/// but the peer gave no app-level response" and the same `failures++` path
/// fires. Same assertion as case 1.
#[test]
fn known_actors_owned_by_stale_peer_are_retained() -> Result<(), DynError> {
    run_gossip_test(async {
        let config = GossipConfig {
            gossip_interval: Duration::from_millis(100),
            // Default retry_interval is 5 s, which makes "three failed
            // rounds" take ~10 s. Shrink so the test finishes quickly.
            peer_retry_interval: Duration::from_millis(200),
            // Default liveness window is 10 s; tighten for deterministic
            // response-asymmetry detection in CI.
            peer_liveness_window: Duration::from_millis(500),
            // Default dial timeout is 10 s; tighten so each failed
            // redial to the accept-and-drop dummy listener completes
            // within a gossip interval. Under multi-binary parallel test
            // runs the 15-s assertion window otherwise catches only one
            // or two rounds.
            connection_timeout: Duration::from_millis(300),
            max_peer_failures: 3,
            ..Default::default()
        };

        let publisher = create_node(config.clone()).await?;
        let (sub_addr, _) =
            seed_known_actor_for_synthetic_peer(&publisher, DEAD_ACTOR_NAME).await?;

        {
            let mut state = publisher.registry.gossip_state.lock().await;
            let stale_cutoff = 60;
            state
                .peers
                .get_mut(&sub_addr)
                .expect("synthetic peer must be present")
                .last_response_received_ms =
                icanact_remote::current_timestamp_millis().saturating_sub(stale_cutoff * 1000);
        }

        for sequence in 0..config.max_peer_failures {
            publisher
                .registry
                .apply_gossip_results(vec![icanact_remote::registry::GossipResult {
                    peer_addr: sub_addr,
                    sent_sequence: sequence as u64,
                    outcome: Ok(None),
                }])
                .await;
        }

        assert_eq!(
            peer_failures(&publisher, sub_addr).await,
            config.max_peer_failures,
            "publisher's gossip should mark stale subscriber dead after \
             max_peer_failures no-response rounds"
        );

        assert_transport_failure_retains_actor(&publisher, sub_addr, DEAD_ACTOR_NAME).await;

        publisher.shutdown().await;
        Ok(())
    })
}

#[test]
fn subsecond_liveness_window_counts_no_response() -> Result<(), DynError> {
    run_gossip_test(async {
        let config = GossipConfig {
            gossip_interval: Duration::from_millis(100),
            peer_retry_interval: Duration::from_millis(200),
            peer_liveness_window: Duration::from_millis(500),
            max_peer_failures: 3,
            ..Default::default()
        };

        let publisher = create_node(config.clone()).await?;
        let (sub_addr, _) =
            seed_known_actor_for_synthetic_peer(&publisher, DEAD_ACTOR_NAME).await?;

        {
            let mut state = publisher.registry.gossip_state.lock().await;
            state
                .peers
                .get_mut(&sub_addr)
                .expect("synthetic peer must be present")
                .last_response_received_ms =
                icanact_remote::current_timestamp_millis().saturating_sub(600);
        }

        publisher
            .registry
            .apply_gossip_results(vec![icanact_remote::registry::GossipResult {
                peer_addr: sub_addr,
                sent_sequence: 0,
                outcome: Ok(None),
            }])
            .await;

        assert_eq!(
            peer_failures(&publisher, sub_addr).await,
            1,
            "a 500ms peer_liveness_window must be evaluated in milliseconds, \
             not rounded down to a whole-second boundary"
        );

        publisher.shutdown().await;
        Ok(())
    })
}

async fn wait_for_lookup_peer(from: &GossipRegistryHandle, to: &PeerId, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if from.lookup_peer(to).await.is_ok() {
            return true;
        }
        sleep(Duration::from_millis(25)).await;
    }
    false
}

#[test]
fn required_peer_connects_within_one_second_without_waiting_for_discovery_gossip()
-> Result<(), DynError> {
    run_gossip_test(async {
        let config = GossipConfig {
            gossip_interval: Duration::from_secs(3600),
            peer_gossip_interval: Some(Duration::from_secs(3600)),
            peer_supervisor_interval: Duration::from_millis(100),
            peer_retry_interval: Duration::from_millis(100),
            connection_timeout: Duration::from_millis(250),
            response_timeout: Duration::from_millis(250),
            ..Default::default()
        };

        let peer_a = create_node(config.clone()).await?;
        let peer_b = create_node(config.clone()).await?;
        let addr_a = peer_a.registry.bind_addr;
        let addr_b = peer_b.registry.bind_addr;
        let id_a = peer_a.registry.peer_id.clone();
        let id_b = peer_b.registry.peer_id.clone();

        peer_a
            .registry
            .add_peer_with_node_id(addr_b, Some(id_b.to_node_id()))
            .await;
        peer_a.registry.configure_peer(id_b.clone(), addr_b).await;
        peer_b
            .registry
            .add_peer_with_node_id(addr_a, Some(id_a.to_node_id()))
            .await;
        peer_b.registry.configure_peer(id_a.clone(), addr_a).await;

        assert!(
            wait_for_lookup_peer(&peer_a, &id_b, Duration::from_secs(1)).await,
            "configured peer A->B must establish a direct lookup_peer route within 1s \
             without waiting for periodic peer-discovery gossip"
        );
        assert!(
            wait_for_lookup_peer(&peer_b, &id_a, Duration::from_secs(1)).await,
            "configured peer B->A must establish a direct lookup_peer route within 1s \
             without waiting for periodic peer-discovery gossip"
        );

        peer_a.shutdown().await;
        peer_b.shutdown().await;
        Ok(())
    })
}

#[test]
fn configured_peer_live_connection_is_not_failed_by_peer_gossip_cadence_gap() -> Result<(), DynError>
{
    run_gossip_test(async {
        let config = GossipConfig {
            gossip_interval: Duration::from_secs(3600),
            peer_gossip_interval: Some(Duration::from_millis(1500)),
            peer_liveness_window: Duration::from_millis(500),
            peer_supervisor_interval: Duration::from_secs(3600),
            peer_retry_interval: Duration::from_secs(3600),
            connection_timeout: Duration::from_millis(250),
            response_timeout: Duration::from_millis(250),
            max_peer_failures: 3,
            ..Default::default()
        };

        let peer_a = create_node(config.clone()).await?;
        let peer_b = create_node(config.clone()).await?;
        let addr_b = peer_b.registry.bind_addr;
        let id_b = peer_b.registry.peer_id.clone();

        peer_a
            .registry
            .add_peer_with_node_id(addr_b, Some(id_b.to_node_id()))
            .await;
        peer_a.registry.configure_peer(id_b.clone(), addr_b).await;
        peer_a.registry.connect_to_peer(&id_b).await?;
        assert!(
            peer_a.lookup_peer(&id_b).await.is_ok(),
            "test precondition: configured direct peer must be routable before silence simulation"
        );

        {
            let mut state = peer_a.registry.gossip_state.lock().await;
            state
                .peers
                .get_mut(&addr_b)
                .expect("configured peer must be present")
                .last_response_received_ms =
                icanact_remote::current_timestamp_millis().saturating_sub(600);
        }

        for sequence in 0..config.max_peer_failures {
            peer_a
                .registry
                .apply_gossip_results(vec![icanact_remote::registry::GossipResult {
                    peer_addr: addr_b,
                    sent_sequence: sequence as u64,
                    outcome: Ok(None),
                }])
                .await;
        }

        let failures = peer_failures(&peer_a, addr_b).await;
        let lookup_result = peer_a.lookup_peer(&id_b).await;
        let lookup_error = lookup_result.as_ref().err().map(ToString::to_string);
        assert!(
            failures == 0 && lookup_result.is_ok(),
            "a configured peer with a live direct connection must not be failed solely \
             because inbound peer-gossip silence exceeds peer_liveness_window while still \
             below the peer_gossip_interval cadence; failures={failures}, \
             lookup_peer_error={lookup_error:?}"
        );
        peer_a.shutdown().await;
        peer_b.shutdown().await;
        Ok(())
    })
}

#[test]
fn discovered_non_required_peer_still_uses_response_asymmetry_liveness() -> Result<(), DynError> {
    run_gossip_test(async {
        let config = GossipConfig {
            gossip_interval: Duration::from_millis(100),
            peer_gossip_interval: Some(Duration::from_millis(1500)),
            peer_liveness_window: Duration::from_millis(500),
            max_peer_failures: 3,
            ..Default::default()
        };

        let publisher = create_node(config.clone()).await?;
        let (peer_addr, _) =
            seed_known_actor_for_synthetic_peer(&publisher, DEAD_ACTOR_NAME).await?;

        {
            let mut state = publisher.registry.gossip_state.lock().await;
            state
                .peers
                .get_mut(&peer_addr)
                .expect("discovered peer must be present")
                .last_response_received_ms =
                icanact_remote::current_timestamp_millis().saturating_sub(600);
        }

        for sequence in 0..config.max_peer_failures {
            publisher
                .registry
                .apply_gossip_results(vec![icanact_remote::registry::GossipResult {
                    peer_addr,
                    sent_sequence: sequence as u64,
                    outcome: Ok(None),
                }])
                .await;
        }

        assert_eq!(
            peer_failures(&publisher, peer_addr).await,
            config.max_peer_failures,
            "non-required discovered peers should still use response-asymmetry liveness; \
             the configured-peer exception must not disable discovery failure detection"
        );

        publisher.shutdown().await;
        Ok(())
    })
}

// =============================================================================
// Direct exercise of the drop-side detection paths.
//
// The end-to-end drop case (`known_actors_owned_by_dropped_peer_are_retained`
// above) is `#[ignore]`d because in-process loopback TCP keepalive doesn't
// surface a hard-socket-error within a CI window. The two tests below close
// that coverage gap by driving the production code paths directly:
//
//   * `disconnect_handler_retains_actors_after_transport_failure` — calls
//     `handle_peer_connection_failure` (the function the connection-pool
//     read-loop invokes when it observes a socket close) and asserts the
//     transport failure verdict does not prune actors immediately.
//
//   * `hard_socket_error_in_apply_gossip_results_triggers_cleanup` —
//     constructs a `GossipResult` with `Err(BrokenPipe)` and feeds it
//     directly to `apply_gossip_results`, exercising the hard-error
//     fast-path classification.
//
// These cover the same retained-actor contract as the stale-peer test,
// but enter it via the production drop paths.
// =============================================================================

#[test]
fn disconnect_handler_retains_actors_after_transport_failure() -> Result<(), DynError> {
    run_gossip_test(async {
        let config = GossipConfig {
            gossip_interval: Duration::from_millis(100),
            peer_retry_interval: Duration::from_millis(200),
            peer_liveness_window: Duration::from_millis(500),
            connection_timeout: Duration::from_millis(300),
            max_peer_failures: 3,
            ..Default::default()
        };

        let publisher = create_node(config.clone()).await?;
        let (sub_addr, _) =
            seed_known_actor_for_synthetic_peer(&publisher, DEAD_ACTOR_NAME).await?;

        assert!(
            publisher
                .registry
                .lookup_actor(DEAD_ACTOR_NAME)
                .await
                .is_some(),
            "test precondition: synthetic peer actor should be present"
        );

        // Sanity: peer is not failed yet.
        assert_eq!(peer_failures(&publisher, sub_addr).await, 0);

        // Call the disconnect handler directly. In production this is
        // fired by the transport's read-loop / ExitGuard when it
        // observes a socket close.
        publisher
            .registry
            .handle_peer_connection_failure(sub_addr)
            .await?;

        // Failures should be jammed to `max_peer_failures`, but actor
        // removal remains deferred to consensus/timeout cleanup.
        assert_eq!(
            peer_failures(&publisher, sub_addr).await,
            3,
            "handle_peer_connection_failure should jump failures to max_peer_failures"
        );
        assert_transport_failure_retains_actor(&publisher, sub_addr, DEAD_ACTOR_NAME).await;

        publisher.shutdown().await;
        Ok(())
    })
}

#[test]
fn disconnect_handler_canonicalizes_ephemeral_source_addr_before_cleanup() -> Result<(), DynError> {
    run_gossip_test(async {
        let config = GossipConfig {
            gossip_interval: Duration::from_millis(100),
            peer_retry_interval: Duration::from_millis(200),
            peer_liveness_window: Duration::from_millis(500),
            max_peer_failures: 3,
            ..Default::default()
        };

        let publisher = create_node(config.clone()).await?;
        let (sub_addr, sub_peer_id) =
            seed_known_actor_for_synthetic_peer(&publisher, DEAD_ACTOR_NAME).await?;
        let ephemeral_source_addr: std::net::SocketAddr = "127.0.0.1:49152".parse()?;

        publisher
            .registry
            .connection_pool
            .add_addr_to_peer_id(ephemeral_source_addr, sub_peer_id);
        {
            let mut state = publisher.registry.gossip_state.lock().await;
            let peer = state
                .peers
                .get_mut(&sub_addr)
                .expect("synthetic peer must be present");
            peer.node_id = None;
            peer.peer_address = Some(ephemeral_source_addr);
        }

        publisher
            .registry
            .handle_peer_connection_failure(ephemeral_source_addr)
            .await?;

        assert_eq!(
            peer_failures(&publisher, sub_addr).await,
            config.max_peer_failures,
            "disconnects observed on an inbound TCP source alias must mark the \
             configured peer address dead"
        );
        assert_transport_failure_retains_actor(&publisher, sub_addr, DEAD_ACTOR_NAME).await;

        publisher.shutdown().await;
        Ok(())
    })
}

#[test]
fn hard_socket_error_in_apply_gossip_results_triggers_cleanup() -> Result<(), DynError> {
    run_gossip_test(async {
        let config = GossipConfig {
            gossip_interval: Duration::from_millis(100),
            peer_retry_interval: Duration::from_millis(200),
            peer_liveness_window: Duration::from_millis(500),
            max_peer_failures: 3,
            ..Default::default()
        };

        let publisher = create_node(config.clone()).await?;
        let (sub_addr, _) =
            seed_known_actor_for_synthetic_peer(&publisher, DEAD_ACTOR_NAME).await?;
        assert!(
            publisher
                .registry
                .lookup_actor(DEAD_ACTOR_NAME)
                .await
                .is_some(),
            "test precondition: synthetic peer actor should be present"
        );
        assert_eq!(peer_failures(&publisher, sub_addr).await, 0);

        // Construct a single fake gossip-round result indicating a
        // hard transport failure on the subscriber's address. In
        // production this is what the gossip send task produces when
        // the underlying TCP write returns `BrokenPipe`/`ConnectionReset`
        // (kernel observed the FIN and the next write errored out).
        // The `apply_gossip_results` hard-error fast path should jump
        // failures straight to `max_peer_failures` on the same call,
        // but it should not publish an actor-removal verdict.
        let hard_err = icanact_remote::registry::GossipResult {
            peer_addr: sub_addr,
            sent_sequence: 0,
            outcome: Err(icanact_remote::GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "simulated peer socket termination",
            ))),
        };
        publisher
            .registry
            .apply_gossip_results(vec![hard_err])
            .await;

        assert_eq!(
            peer_failures(&publisher, sub_addr).await,
            3,
            "hard-socket-error in a single gossip-round outcome should jump \
             failures to max_peer_failures on the same call"
        );
        assert_transport_failure_retains_actor(&publisher, sub_addr, DEAD_ACTOR_NAME).await;

        publisher.shutdown().await;
        Ok(())
    })
}

/// A hard socket error is only transport evidence. It must not create
/// an `ActorRemoved` delta that would tell indirect peers the actor is
/// gone before the consensus/timeout path reaches that conclusion.
#[test]
fn hard_socket_error_does_not_enqueue_actor_removed_gossip() -> Result<(), DynError> {
    run_gossip_test(async {
        let config = GossipConfig {
            gossip_interval: Duration::from_millis(100),
            peer_retry_interval: Duration::from_millis(200),
            peer_liveness_window: Duration::from_millis(500),
            max_peer_failures: 3,
            ..Default::default()
        };

        let publisher = create_node(config.clone()).await?;
        let (sub_addr, _) =
            seed_known_actor_for_synthetic_peer(&publisher, DEAD_ACTOR_NAME).await?;

        // Drive the apply_gossip_results hard-error fast path.
        let hard_err = icanact_remote::registry::GossipResult {
            peer_addr: sub_addr,
            sent_sequence: 0,
            outcome: Err(icanact_remote::GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "simulated peer socket termination",
            ))),
        };
        publisher
            .registry
            .apply_gossip_results(vec![hard_err])
            .await;

        assert_transport_failure_retains_actor(&publisher, sub_addr, DEAD_ACTOR_NAME).await;

        // A later periodic gossip round must not synthesize an
        // ActorRemoved from transport failure state.
        let _tasks = publisher.registry.prepare_gossip_round().await?;

        let state = publisher.registry.gossip_state.lock().await;
        assert!(
            state.urgent_changes.is_empty(),
            "transport failure must not populate urgent_changes (len={})",
            state.urgent_changes.len()
        );
        let history_has_removed = state
            .delta_history
            .iter()
            .flat_map(|d| d.changes.iter())
            .any(|c| {
                matches!(
                    c,
                    icanact_remote::registry::RegistryChange::ActorRemoved { name, .. }
                        if name == DEAD_ACTOR_NAME
                )
            });
        assert!(
            !history_has_removed,
            "transport failure must not emit ActorRemoved into delta_history"
        );
        drop(state);

        publisher.shutdown().await;
        Ok(())
    })
}

/// Socket close must start failure consensus without broadcasting an
/// immediate actor-removal verdict.
#[test]
fn socket_close_does_not_trigger_actor_removed_broadcast() -> Result<(), DynError> {
    run_gossip_test(async {
        let config = GossipConfig {
            gossip_interval: Duration::from_millis(100),
            peer_retry_interval: Duration::from_millis(200),
            peer_liveness_window: Duration::from_millis(500),
            max_peer_failures: 3,
            ..Default::default()
        };

        let publisher = create_node(config.clone()).await?;
        let (sub_addr, _) =
            seed_known_actor_for_synthetic_peer(&publisher, DEAD_ACTOR_NAME).await?;
        assert_eq!(peer_failures(&publisher, sub_addr).await, 0);

        publisher
            .registry
            .handle_peer_connection_failure(sub_addr)
            .await?;

        assert_transport_failure_retains_actor(&publisher, sub_addr, DEAD_ACTOR_NAME).await;

        let state = publisher.registry.gossip_state.lock().await;
        let queued_or_historical_actor_removed = state
            .pending_changes
            .iter()
            .chain(state.urgent_changes.iter())
            .any(|c| match c {
                icanact_remote::registry::RegistryChange::ActorRemoved { name, .. } => {
                    name == DEAD_ACTOR_NAME
                }
                _ => false,
            })
            || state
                .delta_history
                .iter()
                .flat_map(|d| d.changes.iter())
                .any(|c| match c {
                    icanact_remote::registry::RegistryChange::ActorRemoved { name, .. } => {
                        name == DEAD_ACTOR_NAME
                    }
                    _ => false,
                });
        assert!(
            !queued_or_historical_actor_removed,
            "socket close must not enqueue or publish ActorRemoved before timeout \
             (pending_changes_len={}, urgent_changes_len={}, delta_history_len={})",
            state.pending_changes.len(),
            state.urgent_changes.len(),
            state.delta_history.len()
        );
        drop(state);

        publisher.shutdown().await;
        Ok(())
    })
}

/// Stale-connection teardown over a *real, still-open* pooled connection — the
/// UDP black-hole regression guard.
///
/// The synthetic-peer tests above prove the failure-consensus *accounting*
/// (failures++, actors retained, no premature tombstone) but they never
/// establish a real transport connection, so they cannot observe the other half
/// of the contract: when a connected peer crosses `max_peer_failures` via
/// **response-asymmetry** (we keep sending, it stops answering) the now-stale
/// pooled connection must be **torn down** so the next send/connect
/// re-establishes a fresh one (self-correcting).
///
/// Why response-asymmetry specifically — and why the peer must stay *up*: over
/// TCP a dead peer sends a FIN, the read-loop observes the close, and
/// `handle_peer_connection_failure` tears the connection down on its own (the
/// `#[ignore]`d drop test). **Over UDP there is no FIN** — the socket stays
/// "usable" forever, the read-loop never fires, and the *only* signal that the
/// peer is gone is that it stopped answering gossip. Before the fix that left
/// the dead connection lingering in the pool for 90 s+, so `has_connection*`
/// reported a dead peer as connected and jobs silently black-holed. To isolate
/// that exact path this test keeps the subscriber **alive** (socket open, no
/// FIN, connection stays usable) and quiets background gossip, so the
/// response-asymmetry verdict in `apply_gossip_results` is the sole thing that
/// can remove the connection.
#[test]
fn stale_peer_failure_tears_down_connection_but_retains_actors() -> Result<(), DynError> {
    run_gossip_test(async {
        let config = GossipConfig {
            // Quiet background gossip on BOTH nodes: no automatic round can
            // reset `last_response_received_ms` (which would defeat the
            // no-response simulation) or redial. The connection is established
            // by an explicit bootstrap dial below, independent of this.
            gossip_interval: Duration::from_secs(3600),
            peer_gossip_interval: Some(Duration::from_millis(250)),
            peer_retry_interval: Duration::from_secs(3600),
            peer_liveness_window: Duration::from_millis(500),
            connection_timeout: Duration::from_millis(500),
            max_peer_failures: 3,
            ..Default::default()
        };

        let publisher = create_node(config.clone()).await?;
        let subscriber = create_node(config.clone()).await?;
        let sub_addr = subscriber.registry.bind_addr;
        let sub_peer_id = subscriber.registry.peer_id.clone();

        // Establish a REAL connection by an explicit *blocking* dial (not
        // gossip-driven, so it works even with gossip quiesced; and blocking, so
        // it is deterministic under parallel-test handshake contention). The
        // subscriber stays up for the whole test, so this connection never
        // receives a FIN and the read-loop never tears it down — exactly the UDP
        // "no close signal" condition.
        publisher
            .registry
            .add_peer_with_node_id(sub_addr, Some(sub_peer_id.to_node_id()))
            .await;
        publisher
            .registry
            .configure_peer(sub_peer_id.clone(), sub_addr)
            .await;
        let pool = &publisher.registry.connection_pool;
        let connected_before = {
            let start = Instant::now();
            loop {
                let _ = publisher.registry.connect_to_peer(&sub_peer_id).await;
                if pool.has_connection(&sub_addr) || pool.has_connection_by_peer_id(&sub_peer_id) {
                    break true;
                }
                if start.elapsed() > Duration::from_secs(20) {
                    break false;
                }
                sleep(Duration::from_millis(100)).await;
            }
        };
        assert!(
            connected_before,
            "test precondition: publisher must hold a real, usable pooled connection to the \
             (still-running) subscriber"
        );

        // Attribute a known actor to the real subscriber's addr/peer-id so the
        // retained-actor half of the contract can be checked after teardown.
        publisher.registry.actor_state.known_actors.upsert_sync(
            DEAD_ACTOR_NAME.to_string(),
            RemoteActorLocation::new_with_peer(sub_addr, sub_peer_id.clone()),
        );
        {
            let mut state = publisher.registry.gossip_state.lock().await;
            state
                .peer_to_actors
                .entry(sub_addr)
                .or_default()
                .insert(DEAD_ACTOR_NAME.to_string());
            // Make the last response look stale so the no-response rounds below
            // trip the response-asymmetry detector.
            if let Some(peer) = state.peers.get_mut(&sub_addr) {
                peer.last_response_received_ms =
                    icanact_remote::current_timestamp_millis().saturating_sub(60 * 1000);
            }
        }

        // Drive the verdict deterministically: the subscriber is alive at the
        // socket level (connection stays "usable") but is treated as having
        // stopped answering at the app level — the UDP black-hole shape.
        for sequence in 0..config.max_peer_failures {
            publisher
                .registry
                .apply_gossip_results(vec![icanact_remote::registry::GossipResult {
                    peer_addr: sub_addr,
                    sent_sequence: sequence as u64,
                    outcome: Ok(None),
                }])
                .await;
        }

        assert!(
            peer_failures(&publisher, sub_addr).await >= config.max_peer_failures,
            "publisher should mark the silent subscriber failed after no-response rounds"
        );

        // First half of the contract: the stale connection is torn down even
        // though the socket never closed. This is what fails on the pre-fix
        // code (the connection lingered as "usable").
        assert!(
            !pool.has_connection(&sub_addr) && !pool.has_connection_by_peer_id(&sub_peer_id),
            "stale connection to a peer past max_peer_failures must be torn down so the \
             next send/connect self-corrects (UDP black-hole regression)"
        );

        // Second half (must still hold): tearing down the transport connection
        // must NOT evict the peer's actors.
        assert_transport_failure_retains_actor(&publisher, sub_addr, DEAD_ACTOR_NAME).await;

        publisher.shutdown().await;
        subscriber.shutdown().await;
        Ok(())
    })
}

/// Transport-level peer death should not queue `ActorRemoved` for
/// gossip. Actor removal is a higher-level consensus/timeout decision.
#[test]
fn transport_peer_death_retains_actor_without_gossip_tombstone() -> Result<(), DynError> {
    run_gossip_test(async {
        let config = GossipConfig {
            gossip_interval: Duration::from_millis(100),
            peer_retry_interval: Duration::from_millis(200),
            peer_liveness_window: Duration::from_millis(500),
            max_peer_failures: 3,
            ..Default::default()
        };

        let publisher = create_node(config.clone()).await?;
        let (sub_addr, _) =
            seed_known_actor_for_synthetic_peer(&publisher, DEAD_ACTOR_NAME).await?;
        assert_eq!(peer_failures(&publisher, sub_addr).await, 0);

        let hard_err = icanact_remote::registry::GossipResult {
            peer_addr: sub_addr,
            sent_sequence: 0,
            outcome: Err(icanact_remote::GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "simulated peer socket termination",
            ))),
        };
        publisher
            .registry
            .apply_gossip_results(vec![hard_err])
            .await;

        assert_transport_failure_retains_actor(&publisher, sub_addr, DEAD_ACTOR_NAME).await;

        let state = publisher.registry.gossip_state.lock().await;
        let queued_actor_removed = state
            .pending_changes
            .iter()
            .chain(state.urgent_changes.iter())
            .any(|c| match c {
                icanact_remote::registry::RegistryChange::ActorRemoved { name, .. } => {
                    name == DEAD_ACTOR_NAME
                }
                _ => false,
            });
        assert!(
            !queued_actor_removed,
            "transport peer death must not enqueue ActorRemoved gossip \
             (urgent_changes_len={}, pending_changes_len={})",
            state.urgent_changes.len(),
            state.pending_changes.len()
        );
        drop(state);

        publisher.shutdown().await;
        Ok(())
    })
}
