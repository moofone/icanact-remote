//! Reproduces and regression-tests gossip-driven cleanup of actors owned
//! by peers that have crossed `max_peer_failures`.
//!
//! ## What this guards
//!
//! Gossip already detects dead peers: each failed gossip round increments
//! `peer_info.failures`, and once it hits `config.max_peer_failures` the
//! peer is filtered out of subsequent gossip selection
//! (`registry.rs:3437` increment; line ~3444 logs `peer reached max
//! failures`). But the *actors registered against that peer* in our
//! `known_actors` map are NOT pruned at the same moment — they linger
//! until `actor_ttl` (5 minutes default).
//!
//! That gap is the root cause of behavior observed on
//! `stratum-devnet-a` 2026-05-11:
//!
//! - Backend peer `538a99…` (10.77.0.52:9101) is unreachable for ~66
//!   minutes. `failures=3` (== `max_peer_failures`) so the peer is
//!   considered dead.
//! - A third peer (`f4061522…`) keeps re-announcing the dead peer's
//!   pubsub-interest actors via gossip every ~5 s.
//! - This stratum keeps treating the dead peer's interest entries as
//!   live, drives publish + lookup paths against them, and pays the
//!   cost (the warn-spam stopped after the pool-level log demote in
//!   PR #28, but the wasted-work / wrong-destination behavior remains).
//!
//! ## Coverage
//!
//! 1. **drop case**: peer's listener closes (process exits / network
//!    drops). Gossip rounds fail to dial it. `failures` crosses
//!    threshold. Assert its actor entries are gone from our
//!    `known_actors`.
//!
//! 2. **stale case**: peer's TCP listener stays alive but it does NOT
//!    process incoming gossip frames (deadlocked / paused). Gossip
//!    RPCs time out. Same `failures++` path → same threshold → same
//!    assertion.
//!
//! Both cases funnel through the single increment site at
//! `registry.rs:3437`. The fix point is one hook at the
//! `peer_info.failures == max_peer_failures` transition: enumerate
//! `known_actors` and remove entries whose `location.peer_id` matches
//! the now-dead peer.
//!
//! ## Status
//!
//! Both tests are expected to FAIL on current `main`. They will pass
//! once the cleanup hook is wired in.

use std::future::Future;
use std::sync::Once;
use std::time::{Duration, Instant};

use icanact_remote::{
    GossipConfig, GossipRegistryHandle, RegistrationPriority, RemoteActorLocation, SecretKey,
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

async fn wait_for_actor_gone(
    node: &GossipRegistryHandle,
    name: &str,
    timeout: Duration,
) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if node.registry.lookup_actor(name).await.is_none() {
            return true;
        }
        sleep(Duration::from_millis(50)).await;
    }
    false
}

async fn peer_failures(node: &GossipRegistryHandle, addr: std::net::SocketAddr) -> usize {
    let state = node.registry.gossip_state.lock().await;
    state.peers.get(&addr).map(|p| p.failures).unwrap_or(0)
}

async fn diagnose_peer(node: &GossipRegistryHandle, addr: std::net::SocketAddr) -> String {
    let state = node.registry.gossip_state.lock().await;
    match state.peers.get(&addr) {
        Some(p) => format!(
            "failures={} last_attempt={} last_success={} last_response_received={} peer_count={}",
            p.failures,
            p.last_attempt,
            p.last_success,
            p.last_response_received,
            state.peers.len()
        ),
        None => format!("peer ABSENT from gossip_state; peer_count={}", state.peers.len()),
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

/// Case 1: peer drops its TCP listener (process exit / crash).
///
/// Gossip rounds attempt to write into the publisher's cached
/// persistent connection to the dead peer. On a real network /
/// kernel, this eventually surfaces as a hard transport error —
/// either via TCP keepalive (`TcpKeepaliveConfig`) or via a write
/// returning `BrokenPipe` once kernel buffers fill — at which point
/// `handle_peer_connection_failure` fires and the cleanup hook
/// installed by this PR runs.
///
/// In an in-process loopback test on a clean shutdown, the kernel
/// keeps accepting bytes into its send buffer for many seconds, so
/// the failure signal never reaches our gossip layer within the test
/// window. The stale-peer test below exercises the same cleanup hook
/// via a deterministic gossip-RPC failure path, so the cleanup logic
/// is covered.
///
/// Ignored by default. To exercise this case end-to-end, run with
/// `--ignored` and a long timeout — production stratum at
/// `stratum-devnet-a` reproduces the death-detection naturally after
/// the gossip layer's keepalive trips (observed at `failures=3` in
/// real logs).
#[ignore = "requires real-network TCP keepalive timing; covered behaviorally by the stale-peer test below"]
#[test]
fn known_actors_owned_by_dropped_peer_get_pruned() -> Result<(), DynError> {
    run_gossip_test(async {
        let config = GossipConfig {
            gossip_interval: Duration::from_millis(100),
            // Default retry_interval is 5 s, which makes "three failed
            // rounds" take ~10 s. Shrink so the test finishes quickly.
            peer_retry_interval: Duration::from_millis(200),
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

        let loc =
            wait_for_actor(&publisher, DEAD_ACTOR_NAME, Duration::from_secs(3))
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

        // ASSERT THE FIX: subscriber's actor is no longer in publisher's
        // known_actors. Pre-fix: this assertion FAILS — the entry stays
        // around until `actor_ttl` (5 minutes default).
        assert!(
            wait_for_actor_gone(&publisher, DEAD_ACTOR_NAME, Duration::from_secs(5)).await,
            "subscriber's actor should be pruned from publisher's known_actors \
             after peer crossed max_peer_failures"
        );

        publisher.shutdown().await;
        Ok(())
    })
}

/// Case 2: peer's TCP stays accepting but it never processes incoming
/// gossip frames (deadlocked, paused, app-level hang).
///
/// We simulate this by shutting down the subscriber's gossip server
/// while *keeping its bind addr alive* via a bare `TcpListener` that
/// accepts and immediately drops connections. Gossip rounds on the
/// publisher side see "connection accepted but no response" / timeout
/// behavior and the same `failures++` path fires. Same assertion as
/// case 1.
#[test]
fn known_actors_owned_by_stale_peer_get_pruned() -> Result<(), DynError> {
    run_gossip_test(async {
        let config = GossipConfig {
            gossip_interval: Duration::from_millis(100),
            // Default retry_interval is 5 s, which makes "three failed
            // rounds" take ~10 s. Shrink so the test finishes quickly.
            peer_retry_interval: Duration::from_millis(200),
            // Default liveness window is 10 s; tighten for deterministic
            // response-asymmetry detection in CI.
            peer_liveness_window: Duration::from_millis(500),
            max_peer_failures: 3,
            ..Default::default()
        };

        let publisher = create_node(config.clone()).await?;
        let subscriber = create_node(config.clone()).await?;
        let sub_addr = subscriber.registry.bind_addr;
        let sub_peer_id = subscriber.registry.peer_id.clone();

        connect_pair(&publisher, &subscriber).await;

        subscriber
            .registry
            .register_actor_with_priority(
                DEAD_ACTOR_NAME.to_string(),
                RemoteActorLocation::new_with_peer(sub_addr, sub_peer_id.clone()),
                RegistrationPriority::Immediate,
            )
            .await?;

        let _ = wait_for_actor(&publisher, DEAD_ACTOR_NAME, Duration::from_secs(3))
            .await
            .ok_or_else::<DynError, _>(|| "subscriber's actor never propagated".into())?;

        // Stop the subscriber's gossip server but keep the port from
        // being immediately rebindable. Replace it with a passive
        // listener that accepts and drops, simulating an app-level
        // hang.
        subscriber.shutdown().await;
        let dummy_listener = TcpListener::bind(sub_addr).await.map_err(|e| -> DynError {
            format!("failed to rebind sub_addr as dummy listener: {e}").into()
        })?;
        let _accept_task = tokio::spawn(async move {
            loop {
                match dummy_listener.accept().await {
                    Ok((stream, _)) => drop(stream),
                    Err(_) => break,
                }
            }
        });

        assert!(
            wait_for_peer_dead(&publisher, sub_addr, 3, Duration::from_secs(15)).await,
            "publisher's gossip should mark stale subscriber dead after \
             3 failed rounds (TCP accept-then-drop should count as failed exchange)"
        );

        assert!(
            wait_for_actor_gone(&publisher, DEAD_ACTOR_NAME, Duration::from_secs(5)).await,
            "stale subscriber's actor should be pruned from publisher's \
             known_actors after peer crossed max_peer_failures"
        );

        publisher.shutdown().await;
        Ok(())
    })
}

// =============================================================================
// Direct exercise of the drop-side detection paths.
//
// The end-to-end drop case (`known_actors_owned_by_dropped_peer_get_pruned`
// above) is `#[ignore]`d because in-process loopback TCP keepalive doesn't
// surface a hard-socket-error within a CI window. The two tests below close
// that coverage gap by driving the production code paths directly:
//
//   * `disconnect_handler_invokes_peer_death_cleanup` — calls
//     `handle_peer_connection_failure` (the function the connection-pool
//     read-loop invokes when it observes a socket close) and asserts the
//     cleanup chain runs end-to-end.
//
//   * `hard_socket_error_in_apply_gossip_results_triggers_cleanup` —
//     constructs a `GossipResult` with `Err(BrokenPipe)` and feeds it
//     directly to `apply_gossip_results`, exercising the hard-error
//     fast-path classification + `handle_peer_death` invocation.
//
// These cover the same `handle_peer_death` hook the stale-peer test
// exercises, but enter it via the production drop paths.
// =============================================================================

#[test]
fn disconnect_handler_invokes_peer_death_cleanup() -> Result<(), DynError> {
    run_gossip_test(async {
        let config = GossipConfig {
            gossip_interval: Duration::from_millis(100),
            peer_retry_interval: Duration::from_millis(200),
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

        wait_for_actor(&publisher, DEAD_ACTOR_NAME, Duration::from_secs(3))
            .await
            .ok_or_else::<DynError, _>(|| "subscriber's actor never propagated".into())?;

        // Sanity: peer is not failed yet.
        assert_eq!(peer_failures(&publisher, sub_addr).await, 0);

        // Call the disconnect handler directly. In production this is
        // fired by the transport's read-loop / ExitGuard when it
        // observes a socket close (`handle_peer_connection_failure` in
        // registry.rs:~4296). We're testing that THIS path triggers
        // the cleanup hook regardless of whether the gossip-round
        // failure-detection path also fires.
        publisher
            .registry
            .handle_peer_connection_failure(sub_addr)
            .await?;

        // Failures should be jammed to `max_peer_failures` and cleanup
        // should have run synchronously.
        assert_eq!(
            peer_failures(&publisher, sub_addr).await,
            3,
            "handle_peer_connection_failure should jump failures to max_peer_failures"
        );
        assert!(
            publisher
                .registry
                .lookup_actor(DEAD_ACTOR_NAME)
                .await
                .is_none(),
            "subscriber's actor should be pruned synchronously by handle_peer_death \
             when the disconnect handler fires"
        );

        publisher.shutdown().await;
        subscriber.shutdown().await;
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

        wait_for_actor(&publisher, DEAD_ACTOR_NAME, Duration::from_secs(3))
            .await
            .ok_or_else::<DynError, _>(|| "subscriber's actor never propagated".into())?;
        assert_eq!(peer_failures(&publisher, sub_addr).await, 0);

        // Construct a single fake gossip-round result indicating a
        // hard transport failure on the subscriber's address. In
        // production this is what the gossip send task produces when
        // the underlying TCP write returns `BrokenPipe`/`ConnectionReset`
        // (kernel observed the FIN and the next write errored out).
        // The `apply_gossip_results` hard-error fast path (added in
        // moofone/icanact-remote#29) should jump failures straight to
        // `max_peer_failures` and fire `handle_peer_death` on the same
        // call — not wait for `max_peer_failures` separate rounds.
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
        assert!(
            publisher
                .registry
                .lookup_actor(DEAD_ACTOR_NAME)
                .await
                .is_none(),
            "subscriber's actor should be pruned by handle_peer_death \
             when the hard-error fast path fires"
        );

        publisher.shutdown().await;
        subscriber.shutdown().await;
        Ok(())
    })
}

/// C3 regression: when `handle_peer_death` prunes a dead peer's
/// entries from `known_actors`, it MUST also queue an
/// `ActorRemoved` urgent gossip change for every pruned entry so
/// downstream peers in the cluster learn about the death within one
/// gossip tick instead of having to re-derive it from their own
/// timeout path.
///
/// Pre-fix: `handle_peer_death` only mutates `known_actors`
/// locally; `gossip_state.urgent_changes` / `pending_changes` see
/// no entry. Indirect peers continue routing to the dead peer's
/// actors until their own gossip rounds time out.
///
/// Post-fix: the same call enqueues an `ActorRemoved` per pruned
/// actor and the next gossip round fans it out.
#[test]
fn peer_death_queues_actor_removed_for_gossip() -> Result<(), DynError> {
    run_gossip_test(async {
        let config = GossipConfig {
            gossip_interval: Duration::from_millis(100),
            peer_retry_interval: Duration::from_millis(200),
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

        wait_for_actor(&publisher, DEAD_ACTOR_NAME, Duration::from_secs(3))
            .await
            .ok_or_else::<DynError, _>(|| "subscriber's actor never propagated".into())?;
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

        // Local prune already works (covered by the test above).
        assert!(
            publisher
                .registry
                .lookup_actor(DEAD_ACTOR_NAME)
                .await
                .is_none(),
            "local prune precondition: subscriber's actor should be gone from publisher's \
             known_actors"
        );

        // The C3 assertion: an ActorRemoved for the dead peer's
        // actor must now be queued for gossip broadcast. The urgent
        // queue may have been drained already by an interleaving
        // `trigger_immediate_gossip` call; the pending queue is the
        // durable record of the change, so we accept either.
        let state = publisher.registry.gossip_state.lock().await;
        let pending_has_actor_removed = state.pending_changes.iter().any(|c| match c {
            icanact_remote::registry::RegistryChange::ActorRemoved { name, .. } => {
                name == DEAD_ACTOR_NAME
            }
            _ => false,
        });
        let urgent_has_actor_removed = state.urgent_changes.iter().any(|c| match c {
            icanact_remote::registry::RegistryChange::ActorRemoved { name, .. } => {
                name == DEAD_ACTOR_NAME
            }
            _ => false,
        });
        assert!(
            urgent_has_actor_removed || pending_has_actor_removed,
            "handle_peer_death should enqueue an ActorRemoved gossip change for each \
             pruned dead-peer actor (urgent_changes_len={}, \
             urgent_has_actor_removed={}, pending_has_actor_removed={})",
            state.urgent_changes.len(),
            urgent_has_actor_removed,
            pending_has_actor_removed
        );
        drop(state);

        publisher.shutdown().await;
        subscriber.shutdown().await;
        Ok(())
    })
}
