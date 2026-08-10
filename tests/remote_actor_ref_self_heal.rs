//! `RemoteActorRef` self-healing after transport failure / actor-ask timeout.
//!
//! The doc block on `RemoteActorRef` (src/remote_actor_ref.rs) promises that a
//! held ref auto-reconnects after the peer's TCP connection dies (e.g. pod
//! restart) with "no manual re-lookup needed". Before this fix, `connection`
//! was set once at construction and never reassigned anywhere except a local
//! variable inside `ask_actor_frame`'s Timeout-retry branch, which was never
//! written back. These tests hold this claim to its word.

use icanact_remote::registry::{ActorMessageFuture, ActorMessageHandler};
use icanact_remote::{ConnectionRecoveryPolicy, GossipConfig, GossipRegistryHandle, KeyPair};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
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

/// Actor handler whose first invocation deliberately outlasts the caller's
/// ask timeout, and every invocation after that answers immediately. Keyed
/// on invocation count rather than payload content, since a retried ask
/// resends the identical bytes.
struct DelayFirstThenFastHandler {
    calls: Arc<AtomicU64>,
    first_call_delay: Duration,
}

impl ActorMessageHandler for DelayFirstThenFastHandler {
    fn handle_actor_message(
        &self,
        _actor_id: u64,
        _type_hash: u32,
        _payload: icanact_remote::AlignedBytes,
        correlation_id: Option<u32>,
    ) -> ActorMessageFuture<'_> {
        let calls = self.calls.clone();
        let first_call_delay = self.first_call_delay;
        Box::pin(async move {
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                sleep(first_call_delay).await;
            }
            if correlation_id.is_some() {
                Ok(Some(b"pong".to_vec().into()))
            } else {
                Ok(None)
            }
        })
    }
}

/// `ask_actor_frame`'s Timeout branch must actually retry a timed-out ask on
/// the connection `recover_connection_after_actor_ask_timeout` just healed,
/// and that healed connection must then be the one persisted into the ref -
/// not merely reachable in principle. This exercises
/// `evict_peer_on_ask_timeout = true` together with
/// `retry_actor_ask_once_after_timeout = true`
/// (`aggressive_ask_timeout_recovery()`) against a peer that stays
/// continuously alive (never restarted): a peer-restart topology reliably
/// produces a transport-class failure on the stale connection instead
/// (`ConnectionClosed`/`ConnectionReset`), which is caught by
/// `is_transport_failure` and routed through the ambiguous-failure path
/// (`preserve_ambiguous_ask_error`, which never replays), so it can never
/// enter the `Err(GossipError::Timeout)` arm this test is named for. Forcing
/// a genuine `Timeout` instead requires the connection itself to stay
/// healthy while only the response is late, which is exactly what
/// `DelayFirstThenFastHandler` is for.
#[test]
fn timeout_retry_persists_recovered_connection() {
    run_async_test("timeout-retry-persists-connection", async {
        let addr_a: SocketAddr = "127.0.0.1:28475".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:28476".parse().unwrap();

        let (key_pair_a, key_pair_b) =
            key_pair_ordered_for_outbound_a("selfheal_timeout_a", "selfheal_timeout_b");
        let peer_id_a = key_pair_a.peer_id();
        let peer_id_b = key_pair_b.peer_id();
        assert_ne!(
            peer_id_a, peer_id_b,
            "distinct test seeds must not collapse to the same PeerId"
        );

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
            Some(config),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .unwrap();
        let calls = Arc::new(AtomicU64::new(0));
        handle_b
            .registry
            .set_actor_message_handler(Arc::new(DelayFirstThenFastHandler {
                calls: calls.clone(),
                first_call_delay: Duration::from_millis(1_500),
            }))
            .await;

        let peer_b = handle_a.add_peer(&peer_id_b).await;
        peer_b.connect(&addr_b).await.unwrap();
        sleep(Duration::from_millis(300)).await;

        let remote_actor = handle_a
            .lookup_peer(&peer_id_b)
            .await
            .expect("initial lookup_peer should succeed");

        // Keep our own handle to the pre-timeout connection so we can prove,
        // via its *public* `is_closed()`, that it was actually evicted - and
        // that the ref itself no longer points at it afterwards.
        let before_conn = remote_actor
            .connection_ref()
            .expect("connection should be cached");

        // B is still fully alive throughout - only its first response is
        // late. The ask's own budget (600ms) is comfortably shorter than
        // the handler's first-call delay (1.5s), so this must time out
        // locally rather than ever observe the eventual reply.
        let retried = remote_actor
            .ask_actor_frame(
                TEST_ACTOR_ID,
                TEST_TYPE_HASH,
                bytes::Bytes::from_static(b"slow-then-fast"),
                Duration::from_millis(600),
            )
            .await;
        assert_eq!(
            retried.as_deref().ok(),
            Some(b"pong".as_slice()),
            "with retry_actor_ask_once_after_timeout enabled, the timed-out ask must be \
             retried on the recovered connection and succeed, got: {:?}",
            retried
        );
        assert!(
            calls.load(Ordering::SeqCst) >= 2,
            "the handler must have observed both the original attempt and the retry, got {} calls",
            calls.load(Ordering::SeqCst)
        );

        assert!(
            before_conn.is_closed(),
            "the timed-out connection instance should have been evicted/closed"
        );
        let after_conn = remote_actor
            .connection_ref()
            .expect("connection should still be cached");
        assert!(
            !after_conn.is_closed(),
            "the connection the retry actually succeeded on must be persisted into the ref \
             (found the ref still pointing at the closed pre-timeout connection)"
        );

        // The real assertion: a subsequent, unrelated call on the SAME ref
        // must use the healed connection, not the original (now-evicted)
        // instance.
        let follow_up = remote_actor
            .ask(bytes::Bytes::from_static(b"ECHO:after-timeout-retry"))
            .await;
        assert!(
            follow_up.is_ok(),
            "a subsequent ask() must not use the dead pre-timeout connection, got: {:?}",
            follow_up
        );
        assert_eq!(follow_up.unwrap().as_ref(), b"ECHOED:after-timeout-retry");

        let tell_result = remote_actor
            .tell(bytes::Bytes::from_static(b"after-timeout-retry"))
            .await;
        assert!(
            tell_result.is_ok(),
            "a subsequent tell() must not use the dead pre-timeout connection, got: {:?}",
            tell_result
        );

        handle_a.shutdown().await;
        handle_b.shutdown().await;
    });
}

/// Actor handler that never answers within any ask timeout a test here
/// uses, so the caller's `ask_actor_frame` always observes a genuine
/// `GossipError::Timeout` against a connection that is otherwise perfectly
/// healthy - never a transport-class failure.
struct NeverRespondsHandler;

impl ActorMessageHandler for NeverRespondsHandler {
    fn handle_actor_message(
        &self,
        _actor_id: u64,
        _type_hash: u32,
        _payload: icanact_remote::AlignedBytes,
        _correlation_id: Option<u32>,
    ) -> ActorMessageFuture<'_> {
        Box::pin(async move {
            sleep(Duration::from_secs(30)).await;
            Ok(None)
        })
    }
}

/// Evicting on ask timeout and persisting a healed replacement are two
/// independent decisions from replaying the timed-out request onto it - the
/// policy combination that evicts but never replays
/// (`evict_peer_on_ask_timeout = true`, `retry_actor_ask_once_after_timeout
/// = false`) is exactly the one nothing else here covers, and exactly the
/// one where conflating "dial a replacement" with "replay onto it" used to
/// leave the ref's own slot stuck holding the connection instance that was
/// just evicted underneath it.
#[test]
fn timeout_without_replay_still_heals_the_cached_slot() {
    run_async_test("timeout-without-replay-heals-slot", async {
        let addr_a: SocketAddr = "127.0.0.1:28477".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:28478".parse().unwrap();

        let (key_pair_a, key_pair_b) =
            key_pair_ordered_for_outbound_a("selfheal_noretry_a", "selfheal_noretry_b");
        let peer_id_a = key_pair_a.peer_id();
        let peer_id_b = key_pair_b.peer_id();
        assert_ne!(
            peer_id_a, peer_id_b,
            "distinct test seeds must not collapse to the same PeerId"
        );

        let config = GossipConfig {
            gossip_interval: Duration::from_secs(300),
            peer_supervisor_interval: Duration::from_secs(300),
            connection_recovery: ConnectionRecoveryPolicy {
                evict_peer_on_ask_timeout: true,
                evict_peer_on_ask_cancel: false,
                retry_actor_ask_once_after_timeout: false,
                consecutive_timeout_threshold: 0,
            },
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
            Some(config),
            icanact_remote::BuilderTlsBootstrap,
        )
        .await
        .unwrap();
        handle_b
            .registry
            .set_actor_message_handler(Arc::new(NeverRespondsHandler))
            .await;

        let peer_b = handle_a.add_peer(&peer_id_b).await;
        peer_b.connect(&addr_b).await.unwrap();
        sleep(Duration::from_millis(300)).await;

        let remote_actor = handle_a
            .lookup_peer(&peer_id_b)
            .await
            .expect("initial lookup_peer should succeed");

        let before_conn = remote_actor
            .connection_ref()
            .expect("connection should be cached");

        let timed_out = remote_actor
            .ask_actor_frame(
                TEST_ACTOR_ID,
                TEST_TYPE_HASH,
                bytes::Bytes::from_static(b"slow"),
                Duration::from_millis(300),
            )
            .await;
        assert!(
            matches!(timed_out, Err(icanact_remote::GossipError::Timeout)),
            "the ask must time out against a handler that never responds, got: {:?}",
            timed_out
        );

        assert!(
            before_conn.is_closed(),
            "the timed-out connection instance should have been evicted/closed \
             synchronously before ask_actor_frame returned"
        );

        // The real assertion: even with replay disabled (this specific
        // timed-out ask is never retried), the ref's own cached slot must
        // not be left pointing at the connection instance that was just
        // evicted underneath it - a follow-up call must not inherit the
        // dead handle.
        let after_conn = remote_actor
            .connection_ref()
            .expect("connection should still be cached after the timeout");
        assert!(
            !after_conn.is_closed(),
            "the slot must be healed even when replay is disabled, found the ref still \
             pointing at the closed evicted connection"
        );

        let follow_up = remote_actor
            .ask(bytes::Bytes::from_static(b"ECHO:no-replay-heal"))
            .await;
        assert!(
            follow_up.is_ok(),
            "a subsequent ask() must not use the dead evicted connection, got: {:?}",
            follow_up
        );
        assert_eq!(follow_up.unwrap().as_ref(), b"ECHOED:no-replay-heal");

        handle_a.shutdown().await;
        handle_b.shutdown().await;
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
