//! `RemoteActorRef` self-healing after transport failure / actor-ask timeout.
//!
//! The doc block on `RemoteActorRef` (src/remote_actor_ref.rs) promises that a
//! held ref auto-reconnects after the peer's TCP connection dies (e.g. pod
//! restart) with "no manual re-lookup needed". Before this fix, `connection`
//! was set once at construction and never reassigned anywhere except a local
//! variable inside `ask_actor_frame`'s Timeout-retry branch, which was never
//! written back. These tests hold this claim to its word.

mod common;

use common::wait_for_condition;
use icanact_remote::registry::{ActorMessageFuture, ActorMessageHandler};
use icanact_remote::{ConnectionRecoveryPolicy, GossipConfig, GossipRegistryHandle, KeyPair};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::{Duration, sleep};

const TEST_THREAD_STACK: usize = 8 * 1024 * 1024;
const TEST_ACTOR_ID: u64 = 0x5E1F_4EA1;
const TEST_TYPE_HASH: u32 = 0xC0DE_CAFE;

fn run_async_test<F>(name: &str, fut: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    std::thread::Builder::new()
        .name(name.to_string())
        .stack_size(TEST_THREAD_STACK)
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()
                .expect("failed to build runtime");
            rt.block_on(fut);
        })
        .expect("failed to spawn self-heal test thread")
        .join()
        .expect("self-heal test panicked");
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

/// A failed ask must repair a held ref after the peer process restarts (same
/// identity, same address - e.g. a Kubernetes pod restart onto the same
/// Service IP), without replaying that ambiguous request or requiring a
/// manual `lookup()` call.
#[test]
fn held_ref_recovers_after_peer_restart() {
    run_async_test("held-ref-recovers-after-restart", async {
        let addr_a: SocketAddr = "127.0.0.1:28471".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:28472".parse().unwrap();

        let (key_pair_a, key_pair_b) =
            key_pair_ordered_for_outbound_a("selfheal_restart_a", "selfheal_restart_b");
        let peer_id_b = key_pair_b.peer_id();

        let config = GossipConfig {
            gossip_interval: Duration::from_secs(300),
            peer_supervisor_interval: Duration::from_secs(300),
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
            Some(config.clone()),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .unwrap();

        let peer_b = handle_a.add_peer(&peer_id_b).await;
        peer_b.connect(&addr_b).await.unwrap();
        sleep(Duration::from_millis(300)).await;

        // Step 1: lookup does ALL the work - finds the peer AND caches a connection.
        let remote_actor = handle_a
            .lookup_peer(&peer_id_b)
            .await
            .expect("initial lookup_peer should succeed");

        // Sanity: works before the restart.
        let before = remote_actor
            .ask(bytes::Bytes::from_static(b"ECHO:before"))
            .await
            .expect("ask() should work before restart");
        assert_eq!(before.as_ref(), b"ECHOED:before");
        remote_actor
            .tell(bytes::Bytes::from_static(b"before"))
            .await
            .expect("tell() should work before restart");

        // Kill B - this is the "old TCP connection dies" scenario the doc block describes.
        handle_b.shutdown().await;
        sleep(Duration::from_secs(2)).await;

        // Restart B with the SAME identity at the SAME address (pod restart onto the
        // same Service IP). Deliberately do NOT touch `handle_a` here - no add_peer,
        // no connect, no re-lookup. The held `remote_actor` must heal itself.
        let handle_b2 = GossipRegistryHandle::new_with_transport_stack(
            addr_b,
            key_pair_b.to_secret_key(),
            Some(config),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .unwrap();
        sleep(Duration::from_millis(300)).await;

        let ambiguous = remote_actor
            .ask(bytes::Bytes::from_static(b"ECHO:after"))
            .await;
        assert!(
            ambiguous.is_err(),
            "the failed ask must be returned instead of replayed, got: {:?}",
            ambiguous
        );

        let healed = remote_actor
            .ask(bytes::Bytes::from_static(b"ECHO:after-heal"))
            .await
            .expect("the next ask should use the healed connection");
        assert_eq!(healed.as_ref(), b"ECHOED:after-heal");

        let tell_result = remote_actor.tell(bytes::Bytes::from_static(b"after")).await;
        assert!(
            tell_result.is_ok(),
            "tell() should self-heal after peer restart with no manual re-lookup, got: {:?}",
            tell_result
        );

        handle_a.shutdown().await;
        handle_b2.shutdown().await;
    });
}

/// Actor handler that always answers immediately with `pong`.
struct PongHandler;

impl ActorMessageHandler for PongHandler {
    fn handle_actor_message(
        &self,
        _actor_id: u64,
        _type_hash: u32,
        _payload: icanact_remote::AlignedBytes,
        correlation_id: Option<u32>,
    ) -> ActorMessageFuture<'_> {
        Box::pin(async move {
            if correlation_id.is_some() {
                Ok(Some(b"pong".to_vec().into()))
            } else {
                Ok(None)
            }
        })
    }
}

/// `ask_actor_frame` must persist a repaired connection after an ambiguous
/// failure, while returning that failure instead of replaying the request.
///
/// This deliberately reuses the `held_ref_recovers_after_peer_restart`
/// topology (kill + restart the peer with the same identity/address) rather
/// than trying to provoke the Timeout branch against a peer that stays
/// continuously alive: the connection pool intentionally maintains a
/// reciprocal session in both directions once two live registries connect,
/// so forcing *this* ref to evict-and-redial while the peer is still up
/// races against the framework's own duplicate-connection tie-break for that
/// reciprocal session - a pre-existing, independent behavior unrelated to
/// `RemoteActorRef` self-healing. A genuine peer restart sidesteps that
/// entirely (the restarted peer has no reciprocal session yet), which is
/// also the realistic production scenario the self-healing doc block on
/// `RemoteActorRef` describes.
#[test]
fn timeout_retry_persists_recovered_connection() {
    run_async_test("timeout-retry-persists-connection", async {
        let addr_a: SocketAddr = "127.0.0.1:28475".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:28476".parse().unwrap();

        let (key_pair_a, key_pair_b) =
            key_pair_ordered_for_outbound_a("selfheal_timeout_a", "selfheal_timeout_b");
        let peer_id_b = key_pair_b.peer_id();

        let config = GossipConfig {
            gossip_interval: Duration::from_secs(300),
            peer_supervisor_interval: Duration::from_secs(300),
            connection_recovery: ConnectionRecoveryPolicy::aggressive_ask_timeout_recovery(),
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
            Some(config.clone()),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .unwrap();
        handle_b
            .registry
            .set_actor_message_handler(Arc::new(PongHandler))
            .await;

        let peer_b = handle_a.add_peer(&peer_id_b).await;
        peer_b.connect(&addr_b).await.unwrap();
        sleep(Duration::from_millis(300)).await;

        let remote_actor = handle_a
            .lookup_peer(&peer_id_b)
            .await
            .expect("initial lookup_peer should succeed");

        // Sanity: ask_actor_frame works before the restart.
        let before = remote_actor
            .ask_actor_frame(
                TEST_ACTOR_ID,
                TEST_TYPE_HASH,
                bytes::Bytes::from_static(b"before"),
                Duration::from_secs(2),
            )
            .await
            .expect("ask_actor_frame() should work before restart");
        assert_eq!(before.as_ref(), b"pong");

        // Keep our own handle to the pre-restart connection so we can prove,
        // via its *public* `is_closed()`, that it was actually evicted - and
        // that the ref itself no longer points at it afterwards.
        let before_conn = remote_actor
            .connection_ref()
            .expect("connection should be cached");

        // Kill B - genuinely, no reciprocal session survives this.
        handle_b.shutdown().await;
        sleep(Duration::from_secs(2)).await;

        // Restart B with the SAME identity at the SAME address. Do NOT touch
        // `handle_a` here - no add_peer, no connect, no re-lookup.
        let handle_b2 = GossipRegistryHandle::new_with_transport_stack(
            addr_b,
            key_pair_b.to_secret_key(),
            Some(config),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .unwrap();
        handle_b2
            .registry
            .set_actor_message_handler(Arc::new(PongHandler))
            .await;
        sleep(Duration::from_millis(300)).await;

        let ambiguous = remote_actor
            .ask_actor_frame(
                TEST_ACTOR_ID,
                TEST_TYPE_HASH,
                bytes::Bytes::from_static(b"after"),
                Duration::from_secs(2),
            )
            .await;
        assert!(
            ambiguous.is_err(),
            "the ambiguous actor ask must not be replayed, got: {:?}",
            ambiguous
        );

        let after = remote_actor
            .ask_actor_frame(
                TEST_ACTOR_ID,
                TEST_TYPE_HASH,
                bytes::Bytes::from_static(b"after-heal"),
                Duration::from_secs(2),
            )
            .await
            .expect("the next actor ask should use the healed connection");
        assert_eq!(after.as_ref(), b"pong");

        assert!(
            wait_for_condition(Duration::from_secs(2), || async { before_conn.is_closed() }).await,
            "the pre-restart connection instance should have been evicted/closed"
        );
        let after_conn = remote_actor
            .connection_ref()
            .expect("connection should still be cached");
        assert!(
            !after_conn.is_closed(),
            "the connection recovered by ask_actor_frame's own recovery path must be persisted \
             into the ref (found the ref still pointing at the closed pre-restart connection)"
        );

        // The real assertion: a subsequent, unrelated call on the SAME ref
        // must use the healed connection, not the original (now-evicted)
        // instance.
        let follow_up = remote_actor
            .ask(bytes::Bytes::from_static(b"ECHO:after-timeout-retry"))
            .await;
        assert!(
            follow_up.is_ok(),
            "a subsequent ask() must not use the dead pre-restart connection, got: {:?}",
            follow_up
        );
        assert_eq!(follow_up.unwrap().as_ref(), b"ECHOED:after-timeout-retry");

        let tell_result = remote_actor
            .tell(bytes::Bytes::from_static(b"after-timeout-retry"))
            .await;
        assert!(
            tell_result.is_ok(),
            "a subsequent tell() must not use the dead pre-restart connection, got: {:?}",
            tell_result
        );

        handle_a.shutdown().await;
        handle_b2.shutdown().await;
    });
}

/// Concurrent ambiguous asks may each fail, but must leave one coherent,
/// usable repaired connection without replaying any request.
#[test]
fn concurrent_asks_survive_connection_swap_race() {
    run_async_test("concurrent-asks-survive-swap-race", async {
        let addr_a: SocketAddr = "127.0.0.1:28473".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:28474".parse().unwrap();

        let (key_pair_a, key_pair_b) =
            key_pair_ordered_for_outbound_a("selfheal_race_a", "selfheal_race_b");
        let peer_id_b = key_pair_b.peer_id();

        let config = GossipConfig {
            gossip_interval: Duration::from_secs(300),
            peer_supervisor_interval: Duration::from_secs(300),
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
            Some(config.clone()),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .unwrap();

        let peer_b = handle_a.add_peer(&peer_id_b).await;
        peer_b.connect(&addr_b).await.unwrap();
        sleep(Duration::from_millis(300)).await;

        let remote_actor = Arc::new(
            handle_a
                .lookup_peer(&peer_id_b)
                .await
                .expect("initial lookup_peer should succeed"),
        );

        handle_b.shutdown().await;
        sleep(Duration::from_secs(2)).await;

        let handle_b2 = GossipRegistryHandle::new_with_transport_stack(
            addr_b,
            key_pair_b.to_secret_key(),
            Some(config),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .unwrap();
        sleep(Duration::from_millis(300)).await;

        // Fire a burst of concurrent asks right as the ref needs to heal.
        let mut tasks = Vec::new();
        for i in 0..24 {
            let remote_actor = remote_actor.clone();
            tasks.push(tokio::spawn(async move {
                remote_actor
                    .ask(bytes::Bytes::from(format!("ECHO:race-{i}")))
                    .await
                    .map(|resp| resp.to_vec())
            }));
        }

        for (i, task) in tasks.into_iter().enumerate() {
            let result = task.await.expect("task should not panic");
            if let Ok(resp) = result {
                assert_eq!(
                    resp,
                    format!("ECHOED:race-{i}").into_bytes(),
                    "response must not be torn/mixed with another racer's payload"
                );
            }
        }

        // The slot itself must have settled on a single, coherent connection -
        // never left in an unusable/half-swapped state.
        let final_conn = remote_actor
            .connection_ref()
            .expect("connection slot must not be left empty after the race");
        assert!(
            final_conn
                .ask(bytes::Bytes::from_static(b"ECHO:final-check"))
                .await
                .is_ok(),
            "the connection left in the slot after the race must be usable"
        );

        handle_a.shutdown().await;
        handle_b2.shutdown().await;
    });
}
