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
//! 2. **fire-and-forget contract**: `Ok(None)` means no response was
//!    requested. It never increments failures or tears down a live session.
//!    SWIM owns application-level peer liveness.
//!
//! 3. **address/instance boundary**: an address-scoped send result may update
//!    retry accounting, but it cannot peer-wide disconnect a possibly newer
//!    session. The failed stream's IO owner performs exact-instance teardown.

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
        .add_peer_with_node_id(
            addr_b,
            Some(b.registry.peer_id.to_node_id()),
            icanact_remote::addr_ownership::ClaimKind::Verified,
        )
        .await;
    b.registry
        .add_peer_with_node_id(
            addr_a,
            Some(a.registry.peer_id.to_node_id()),
            icanact_remote::addr_ownership::ClaimKind::Verified,
        )
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
        .add_peer_with_node_id(
            peer_addr,
            Some(peer_id.to_node_id()),
            icanact_remote::addr_ownership::ClaimKind::Verified,
        )
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
/// window. The direct hard-socket-error and disconnect-handler tests below
/// exercise the same failure accounting deterministically.
///
/// Ignored by default. To exercise this case end-to-end, run with
/// `--ignored` and a long timeout — production stratum at
/// `stratum-devnet-a` reproduces the death-detection naturally after
/// the gossip layer's keepalive trips (observed at `failures=3` in
/// real logs).
#[ignore = "requires real-network TCP keepalive timing; covered by direct hard-error and disconnect-handler tests"]
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
            // Short side-table retention horizon for the test fixture.
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

/// A successful periodic gossip send is not a liveness probe result.
///
/// The production sender uses `tell()`, so every successful wire send reaches
/// this boundary as `Ok(None)`: the `None` means "no response was requested",
/// not "a requested response timed out". SWIM owns application-level peer
/// liveness above this transport; only an actual send/socket error may add a
/// transport failure here.
#[test]
fn fire_and_forget_success_is_neutral_even_when_response_timestamp_is_stale() -> Result<(), DynError>
{
    run_gossip_test(async {
        let config = GossipConfig {
            gossip_interval: Duration::from_millis(100),
            // Default retry_interval is 5 s, which makes "three failed
            // rounds" take ~10 s. Shrink so the test finishes quickly.
            peer_retry_interval: Duration::from_millis(200),
            // Short side-table retention horizon for the test fixture.
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
            0,
            "Ok(None) means a fire-and-forget send completed without requesting a \
             response; it must not be reinterpreted as a failed liveness probe"
        );

        assert_transport_failure_retains_actor(&publisher, sub_addr, DEAD_ACTOR_NAME).await;

        publisher.shutdown().await;
        Ok(())
    })
}

/// Highest-level regression: repeated successful fire-and-forget results must
/// not tear down a real, healthy pooled TLS connection, even if the legacy
/// registry-response timestamp is arbitrarily stale. SWIM traffic and actor
/// traffic use this same authenticated transport and own their own response
/// semantics; registry gossip must not compete with them as a second failure
/// detector.
#[test]
fn fire_and_forget_success_does_not_disconnect_a_healthy_pooled_peer() -> Result<(), DynError> {
    run_gossip_test(async {
        let config = GossipConfig {
            // Keep background rounds out of this deterministic boundary test.
            gossip_interval: Duration::from_secs(3_600),
            peer_gossip_interval: None,
            cleanup_interval: Duration::from_secs(3_600),
            peer_retry_interval: Duration::from_secs(3_600),
            peer_supervisor_interval: Duration::from_secs(3_600),
            max_peer_failures: 2,
            ..Default::default()
        };

        let observer = create_node(config.clone()).await?;
        let peer = create_node(config.clone()).await?;
        let peer_addr = peer.registry.bind_addr;
        let peer_id = peer.registry.peer_id.clone();

        observer
            .registry
            .add_peer_with_node_id(
                peer_addr,
                Some(peer_id.to_node_id()),
                icanact_remote::addr_ownership::ClaimKind::Verified,
            )
            .await;
        observer
            .registry
            .configure_peer(peer_id.clone(), peer_addr)
            .await;
        observer.registry.connect_to_peer(&peer_id).await?;
        assert!(
            observer.lookup_peer(&peer_id).await.is_ok(),
            "test precondition: the healthy peer must be connected"
        );

        {
            let mut state = observer.registry.gossip_state.lock().await;
            state
                .peers
                .get_mut(&peer_addr)
                .expect("connected peer must be tracked")
                .last_response_received_ms = 0;
        }

        for sequence in 0..config.max_peer_failures {
            observer
                .registry
                .apply_gossip_results(vec![icanact_remote::registry::GossipResult {
                    peer_addr,
                    sent_sequence: sequence as u64,
                    outcome: Ok(None),
                }])
                .await;
        }

        assert_eq!(
            peer_failures(&observer, peer_addr).await,
            0,
            "successful fire-and-forget sends must not accrue transport failures"
        );
        assert!(
            observer.lookup_peer(&peer_id).await.is_ok(),
            "registry gossip must not tear down a healthy TLS session; SWIM owns \
             application-level liveness"
        );

        observer.shutdown().await;
        peer.shutdown().await;
        Ok(())
    })
}

/// A gossip result is address-scoped and carries no connection-instance
/// identity. It may record a hard transport failure for retry accounting, but
/// it cannot safely tear down whatever session is current for that identity:
/// the failed write's IO owner performs the exact-instance teardown, while a
/// replacement may already have been published by the time this batch applies.
#[test]
fn address_scoped_hard_error_does_not_disconnect_the_current_peer_session() -> Result<(), DynError>
{
    run_gossip_test(async {
        let config = GossipConfig {
            gossip_interval: Duration::from_secs(3_600),
            peer_gossip_interval: None,
            cleanup_interval: Duration::from_secs(3_600),
            peer_retry_interval: Duration::from_secs(3_600),
            peer_supervisor_interval: Duration::from_secs(3_600),
            max_peer_failures: 2,
            ..Default::default()
        };
        let observer = create_node(config.clone()).await?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let peer_addr = listener.local_addr()?;
        drop(listener);
        let peer_id = SecretKey::generate().to_keypair().peer_id();

        observer
            .registry
            .add_peer_with_node_id(
                peer_addr,
                Some(peer_id.to_node_id()),
                icanact_remote::addr_ownership::ClaimKind::Verified,
            )
            .await;
        let _current_session = icanact_remote::test_helpers::install_silent_pooled_connection(
            &observer,
            peer_id.clone(),
            peer_addr,
        );
        assert!(
            observer
                .registry
                .connection_pool
                .has_connection_by_peer_id(&peer_id),
            "test precondition: a current peer session must be published"
        );

        observer
            .registry
            .apply_gossip_results(vec![icanact_remote::registry::GossipResult {
                peer_addr,
                sent_sequence: 0,
                outcome: Err(icanact_remote::GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "simulated stale write result",
                ))),
            }])
            .await;

        assert_eq!(
            peer_failures(&observer, peer_addr).await,
            config.max_peer_failures,
            "hard send errors must still update transport retry accounting"
        );
        assert!(
            observer
                .registry
                .connection_pool
                .has_connection_by_peer_id(&peer_id),
            "an address-scoped batch result must not peer-wide disconnect the current \
             session; the failed instance's IO owner performs exact-instance teardown"
        );

        observer.shutdown().await;
        Ok(())
    })
}

#[test]
fn liveness_window_does_not_turn_fire_and_forget_success_into_a_probe() -> Result<(), DynError> {
    run_gossip_test(async {
        let config = GossipConfig {
            gossip_interval: Duration::from_millis(100),
            peer_gossip_interval: None,
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
            0,
            "peer_liveness_window must not reinterpret Ok(None) as a timed-out \
             probe: periodic gossip never requested a response"
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
            .add_peer_with_node_id(
                addr_b,
                Some(id_b.to_node_id()),
                icanact_remote::addr_ownership::ClaimKind::Verified,
            )
            .await;
        peer_a.registry.configure_peer(id_b.clone(), addr_b).await;
        peer_b
            .registry
            .add_peer_with_node_id(
                addr_a,
                Some(id_a.to_node_id()),
                icanact_remote::addr_ownership::ClaimKind::Verified,
            )
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
            .add_peer_with_node_id(
                addr_b,
                Some(id_b.to_node_id()),
                icanact_remote::addr_ownership::ClaimKind::Verified,
            )
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
fn discovered_peer_fire_and_forget_success_is_not_liveness_evidence() -> Result<(), DynError> {
    run_gossip_test(async {
        let config = GossipConfig {
            gossip_interval: Duration::from_millis(100),
            peer_gossip_interval: None,
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
            0,
            "discovered peers use the same fire-and-forget contract: Ok(None) means \
             no response was requested, not that a liveness probe failed"
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
            .handle_peer_connection_failure(sub_addr, None)
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
            .handle_peer_connection_failure(ephemeral_source_addr, None)
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
            .handle_peer_connection_failure(sub_addr, None)
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

/// A fire-and-forget result over a real, still-open pooled stream remains
/// neutral. The silent duplex endpoint deliberately supplies no inbound
/// registry message, proving that an absent response to a request that was
/// never made cannot become a transport or membership verdict.
#[test]
fn no_response_result_does_not_tear_down_a_silent_pooled_connection() -> Result<(), DynError> {
    run_gossip_test(async {
        let config = GossipConfig {
            // Quiet background gossip: the liveness results below are the
            // only activity associated with the silent fixture.
            gossip_interval: Duration::from_secs(3600),
            peer_gossip_interval: None,
            peer_retry_interval: Duration::from_secs(3600),
            peer_supervisor_interval: Duration::from_secs(3600),
            peer_liveness_window: Duration::from_millis(500),
            connection_timeout: Duration::from_millis(500),
            max_peer_failures: 3,
            ..Default::default()
        };
        let publisher = create_node(config.clone()).await?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let sub_addr = listener.local_addr()?;
        drop(listener);
        let sub_peer_id = SecretKey::generate().to_keypair().peer_id();

        publisher
            .registry
            .add_peer_with_node_id(
                sub_addr,
                Some(sub_peer_id.to_node_id()),
                icanact_remote::addr_ownership::ClaimKind::Verified,
            )
            .await;
        let _silent_peer = icanact_remote::test_helpers::install_silent_pooled_connection(
            &publisher,
            sub_peer_id.clone(),
            sub_addr,
        );
        let pool = &publisher.registry.connection_pool;
        assert!(
            pool.has_connection(&sub_addr) && pool.has_connection_by_peer_id(&sub_peer_id),
            "test precondition: publisher must hold the silent pooled connection"
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
        }

        {
            let mut state = publisher.registry.gossip_state.lock().await;
            state
                .peers
                .get_mut(&sub_addr)
                .expect("silent peer must be present")
                .last_response_received_ms = 0;
        }

        // The duplex endpoint remains open, but no task refreshes the legacy
        // registry-response timestamp.
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
            0,
            "a silent fixture does not turn a fire-and-forget send into a failed probe"
        );

        assert!(
            pool.has_connection(&sub_addr) && pool.has_connection_by_peer_id(&sub_peer_id),
            "address-scoped Ok(None) results must not tear down the current identity-scoped \
             session; SWIM owns application-level liveness"
        );

        // Neutral registry results also leave actor routing untouched.
        assert_transport_failure_retains_actor(&publisher, sub_addr, DEAD_ACTOR_NAME).await;

        publisher.shutdown().await;
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
