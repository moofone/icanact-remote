//! `RemoteActorRef` self-healing after transport failure / actor-ask timeout.
//!
//! The doc block on `RemoteActorRef` (src/remote_actor_ref.rs) promises that a
//! held ref auto-reconnects after the peer's TCP connection dies (e.g. pod
//! restart) with "no manual re-lookup needed". Before this fix, `connection`
//! was set once at construction and never reassigned anywhere except a local
//! variable inside `ask_actor_frame`'s Timeout-retry branch, which was never
//! written back. These tests hold this claim to its word.

use icanact_remote::registry::{ActorAskHandlerSync, ActorResponse, AskDisposition};
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
        handle_b.shutdown().await.unwrap();
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

        handle_a.shutdown().await.unwrap();
        handle_b2.shutdown().await.unwrap();
    });
}

/// Actor handler whose first invocation deliberately outlasts the caller's
/// ask timeout, and every invocation after that answers immediately. Keyed
/// on invocation count rather than payload content, since a retried ask
/// resends the identical bytes.
struct DelayFirstThenFastHandler {
    calls: Arc<AtomicU64>,
    first_call_delay: Duration,
    first_reply: std::sync::Mutex<
        Option<(
            icanact_remote::ReplySession,
            icanact_remote::ForwardReplyToken,
            tokio::time::Instant,
        )>,
    >,
}

impl ActorAskHandlerSync for DelayFirstThenFastHandler {
    fn handle_actor_ask_sync(
        &self,
        _actor_id: u64,
        _type_hash: u32,
        _payload: icanact_remote::AlignedBytes,
        context: icanact_remote::AskContext<'_>,
    ) -> icanact_remote::Result<AskDisposition> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            // Exactly one bounded deferred reply; do not sleep inside the
            // transport's inline callback and block its retry reader.
            let session = context.reply_session()?;
            let token = context.into_forward_reply(&session)?;
            *self.first_reply.lock().unwrap() = Some((
                session,
                token,
                tokio::time::Instant::now() + self.first_call_delay,
            ));
            Ok(AskDisposition::Deferred)
        } else {
            Ok(AskDisposition::Immediate(ActorResponse::Bytes(
                icanact_remote::ForwardPayload::from_static(b"pong"),
            )))
        }
    }
}

/// Explicit timeout replay may re-send an idempotent request, but cannot evict
/// the healthy session. The first actor response is late while the transport
/// remains live; a second request and unrelated subsequent traffic still work.
#[test]
fn timeout_retry_preserves_healthy_connection() {
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
        let handler = Arc::new(DelayFirstThenFastHandler {
            calls: calls.clone(),
            first_call_delay: Duration::from_millis(1_500),
            first_reply: std::sync::Mutex::new(None),
        });
        handle_b
            .registry
            .set_actor_ask_handler_sync(handler.clone())
            .await;

        let peer_b = handle_a.add_peer(&peer_id_b).await;
        peer_b.connect(&addr_b).await.unwrap();
        sleep(Duration::from_millis(300)).await;

        let remote_actor = handle_a
            .lookup_peer(&peer_id_b)
            .await
            .expect("initial lookup_peer should succeed");

        // Retain the original handle: timeout must leave this exact transport
        // open, not merely install some other usable replacement.
        let before_conn = remote_actor
            .connection_ref()
            .expect("connection should be cached");
        let before_physical = handle_a
            .registry
            .connection_pool
            .get_lock_free_connection(addr_b)
            .unwrap();

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
             retried on the healthy connection and succeed, got: {:?}",
            retried
        );
        assert!(
            calls.load(Ordering::SeqCst) >= 2,
            "the handler must have observed both the original attempt and the retry, got {} calls",
            calls.load(Ordering::SeqCst)
        );

        assert!(
            !before_conn.is_closed(),
            "an actor timeout must not evict the healthy transport"
        );
        let after_conn = remote_actor
            .connection_ref()
            .expect("connection should still be cached");
        assert!(
            !after_conn.is_closed(),
            "the original healthy transport must remain available after replay"
        );

        assert!(
            Arc::ptr_eq(
                &before_physical,
                &handle_a
                    .registry
                    .connection_pool
                    .get_lock_free_connection(addr_b)
                    .unwrap()
            ),
            "timeout replay must preserve the exact physical generation"
        );

        // Drive the original response late, without spawning a task. Its
        // distinguishable body must not satisfy the retry or a later ask.
        let (session, token, ready_at) = handler.first_reply.lock().unwrap().take().unwrap();
        tokio::time::sleep_until(ready_at).await;
        session
            .try_reply_owned(
                token,
                icanact_remote::ForwardPayload::from_static(b"late-original"),
            )
            .unwrap();

        // Subsequent, unrelated calls on the same ref remain usable.
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

        handle_a.shutdown().await.unwrap();
        handle_b.shutdown().await.unwrap();
    });
}

/// Actor handler that never answers within any ask timeout a test here
/// uses, so the caller's `ask_actor_frame` always observes a genuine
/// `GossipError::Timeout` against a connection that is otherwise perfectly
/// healthy - never a transport-class failure.
struct NeverRespondsHandler;

impl ActorAskHandlerSync for NeverRespondsHandler {
    fn handle_actor_ask_sync(
        &self,
        _actor_id: u64,
        _type_hash: u32,
        _payload: icanact_remote::AlignedBytes,
        _context: icanact_remote::AskContext<'_>,
    ) -> icanact_remote::Result<AskDisposition> {
        Ok(AskDisposition::Deferred)
    }
}

/// Legacy eviction flags are inert. Without explicit replay the caller gets
/// Timeout, while the same healthy cached transport remains usable.
#[test]
fn timeout_without_replay_preserves_healthy_connection() {
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
            .set_actor_ask_handler_sync(Arc::new(NeverRespondsHandler))
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
        let before_physical = handle_a
            .registry
            .connection_pool
            .get_lock_free_connection(addr_b)
            .unwrap();

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
            !before_conn.is_closed(),
            "an actor timeout must not evict the healthy transport"
        );

        // Replay disabled still preserves the healthy cached slot.
        let after_conn = remote_actor
            .connection_ref()
            .expect("connection should still be cached after the timeout");
        assert!(
            !after_conn.is_closed(),
            "the original healthy slot remains available with replay disabled"
        );

        assert!(
            Arc::ptr_eq(
                &before_physical,
                &handle_a
                    .registry
                    .connection_pool
                    .get_lock_free_connection(addr_b)
                    .unwrap()
            ),
            "a timeout without replay must preserve the exact physical generation"
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

        handle_a.shutdown().await.unwrap();
        handle_b.shutdown().await.unwrap();
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

        handle_b.shutdown().await.unwrap();
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

        handle_a.shutdown().await.unwrap();
        handle_b2.shutdown().await.unwrap();
    });
}
