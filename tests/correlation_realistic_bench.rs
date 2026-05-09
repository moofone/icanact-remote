//! Realistic end-to-end bench for the CorrelationTracker hot path.
//!
//! Unlike the in-process throughput micro-bench in `connection_pool/tests/mod.rs`,
//! this exercises the full production path:
//!   - Real TCP + TLS loopback transport between two `GossipRegistryHandle`s.
//!   - Real `ConnectionHandle::ask` → `ask_with_timeout_bytes` → SlotGuard
//!     allocate → frame write → wait_for_response (parks future) → read
//!     pipeline decodes response → `complete()` wakes the waiter → response
//!     consumed and returned.
//!   - Built-in ECHO responder ("ECHO:..." → "ECHOED:...") so each
//!     iteration is a real wire round-trip with payload echo, not a
//!     synthetic in-memory completion.
//!
//! Two scenarios are reported:
//!   - `sequential` — exactly 1 ask in flight at any moment. Models a raft
//!     leader's per-peer heartbeat path (one append_entries pending per
//!     follower at a time). Reports per-op latency and effective throughput.
//!   - `concurrent` — `INFLIGHT` asks pipelined via `tokio::spawn`. Models
//!     burst load. Reports max sustained throughput.
//!
//! Each scenario reports BOTH:
//!   - **ops/sec**  = completed request/response cycles per second
//!     (one ask call returning successfully = one op).
//!   - **msgs/sec** = wire-level frames per second
//!     (= 2 × ops/sec for an ask/reply protocol: 1 request frame + 1
//!     response frame on the network per cycle).
//!
//! Run with:
//!   cargo test --release --test correlation_realistic_bench -- \
//!       --ignored --nocapture
//!
//! Both benches are gated behind `#[ignore]` to keep `cargo test` fast in CI.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use icanact_remote::{GossipConfig, GossipRegistryHandle, KeyPair, PeerId};
use tokio::time::sleep;

/// Number of full request/response cycles per bench. High enough that the
/// per-op number is dominated by steady-state behaviour rather than warmup,
/// low enough that the bench finishes in a few seconds.
const ITERS_SEQUENTIAL: usize = 5_000;
const ITERS_CONCURRENT: usize = 20_000;
/// Concurrency for the pipelined bench. Far below the 8192-slot ring so we
/// measure happy-path throughput, not exhaustion behaviour.
const INFLIGHT: usize = 128;

#[test]
#[ignore = "benchmark; cargo test --release --test correlation_realistic_bench -- --ignored --nocapture"]
fn correlation_realistic_sequential_bench() {
    spawn_runtime("realistic-seq", run_sequential);
}

#[test]
#[ignore = "benchmark; cargo test --release --test correlation_realistic_bench -- --ignored --nocapture"]
fn correlation_realistic_concurrent_bench() {
    spawn_runtime("realistic-conc", run_concurrent);
}

fn spawn_runtime<F, Fut>(name: &'static str, body: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let handle = std::thread::Builder::new()
        .name(name.into())
        .stack_size(8 * 1024 * 1024)
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(4 * 1024 * 1024)
                .enable_all()
                .build()
                .expect("build runtime");
            rt.block_on(body());
        })
        .expect("spawn bench thread");
    handle.join().expect("bench panicked");
}

async fn run_sequential() {
    let (handle_a, handle_b, conn) = setup("seq", 7920, 7921).await;

    // Warmup: a few asks to amortise TLS handshake / TCP slow-start before
    // we start the timer. This keeps the steady-state number out of the
    // setup noise.
    for i in 0..32 {
        let req = format!("ECHO:warmup-{i}").into_bytes();
        let _ = conn.ask(bytes::Bytes::from(req)).await.unwrap();
    }

    let start = Instant::now();
    for i in 0..ITERS_SEQUENTIAL {
        // Real wire ask: TCP write of request frame, server-side ECHO,
        // TCP read of response frame, CorrelationTracker park/wake.
        let req = format!("ECHO:seq-{i}").into_bytes();
        let resp = conn.ask(bytes::Bytes::from(req)).await.unwrap();
        // Cheap correctness check that the right response routed back to
        // the right SlotGuard. Cost is dominated by `==` on a small Bytes.
        debug_assert!(resp.starts_with(b"ECHOED:seq-"));
        std::hint::black_box(resp);
    }
    let elapsed = start.elapsed();

    report("sequential", ITERS_SEQUENTIAL, elapsed);

    handle_a.shutdown().await;
    handle_b.shutdown().await;
}

async fn run_concurrent() {
    let (handle_a, handle_b, conn) = setup("conc", 7922, 7923).await;

    // Warmup
    for i in 0..64 {
        let req = format!("ECHO:warmup-{i}").into_bytes();
        let _ = conn.ask(bytes::Bytes::from(req)).await.unwrap();
    }

    // Pipelined producer: keep INFLIGHT asks in flight at all times until
    // ITERS_CONCURRENT cycles have completed. Models burst traffic where
    // the application has many requests queued.
    let start = Instant::now();
    let mut pending = futures::stream::FuturesUnordered::new();
    let mut launched = 0usize;
    let mut completed = 0usize;
    while completed < ITERS_CONCURRENT {
        while pending.len() < INFLIGHT && launched < ITERS_CONCURRENT {
            let conn = conn.clone();
            let i = launched;
            pending.push(async move {
                let req = format!("ECHO:conc-{i}").into_bytes();
                conn.ask(bytes::Bytes::from(req)).await.unwrap()
            });
            launched += 1;
        }
        use futures::stream::StreamExt;
        if let Some(resp) = pending.next().await {
            debug_assert!(resp.starts_with(b"ECHOED:conc-"));
            std::hint::black_box(resp);
            completed += 1;
        }
    }
    let elapsed = start.elapsed();

    report("concurrent", ITERS_CONCURRENT, elapsed);

    handle_a.shutdown().await;
    handle_b.shutdown().await;
}

fn report(label: &str, ops: usize, elapsed: Duration) {
    let secs = elapsed.as_secs_f64();
    let ops_per_sec = ops as f64 / secs;
    // Wire messages per op = 1 request frame + 1 response frame for an
    // ask/reply protocol. If you ever extend to multi-frame asks (e.g.
    // streaming chunks), update the multiplier here.
    let msgs_per_op = 2.0;
    let msgs_per_sec = ops_per_sec * msgs_per_op;
    let ns_per_op = elapsed.as_nanos() as f64 / ops as f64;

    println!(
        "[realistic_{label}] ops={ops} elapsed_s={secs:.6} \
         ns_per_op={ns_per_op:.1} \
         ops_per_sec={ops_per_sec:.0} \
         msgs_per_sec={msgs_per_sec:.0} \
         (1 op = 1 ask cycle = 2 wire msgs: request + response)"
    );
}

async fn setup(
    tag: &'static str,
    port_a: u16,
    port_b: u16,
) -> (
    GossipRegistryHandle<icanact_remote::BuilderTlsBootstrap>,
    GossipRegistryHandle<icanact_remote::BuilderTlsBootstrap>,
    icanact_remote::RemoteActorRef,
) {
    let addr_a: SocketAddr = format!("127.0.0.1:{port_a}").parse().unwrap();
    let addr_b: SocketAddr = format!("127.0.0.1:{port_b}").parse().unwrap();

    let key_pair_a = KeyPair::new_for_testing(&format!("realistic_{tag}_a"));
    let key_pair_b = KeyPair::new_for_testing(&format!("realistic_{tag}_b"));
    let peer_id_a = key_pair_a.peer_id();
    let peer_id_b = key_pair_b.peer_id();

    // Long gossip interval keeps gossip traffic out of the bench window.
    let cfg = || GossipConfig {
        gossip_interval: Duration::from_secs(600),
        ..Default::default()
    };

    let handle_a = GossipRegistryHandle::new_with_transport_stack(
        addr_a,
        key_pair_a.to_secret_key(),
        Some(cfg()),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await
    .unwrap();

    let handle_b = GossipRegistryHandle::new_with_transport_stack(
        addr_b,
        key_pair_b.to_secret_key(),
        Some(cfg()),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await
    .unwrap();

    connect_preferred_direction(&handle_a, &peer_id_a, addr_a, &handle_b, &peer_id_b, addr_b).await;
    sleep(Duration::from_millis(500)).await;

    let conn = handle_a.lookup_peer(&peer_id_b).await.unwrap();
    (handle_a, handle_b, conn)
}

async fn connect_preferred_direction(
    handle_a: &GossipRegistryHandle<icanact_remote::BuilderTlsBootstrap>,
    peer_id_a: &PeerId,
    addr_a: SocketAddr,
    handle_b: &GossipRegistryHandle<icanact_remote::BuilderTlsBootstrap>,
    peer_id_b: &PeerId,
    addr_b: SocketAddr,
) {
    if handle_a.registry.should_keep_connection(peer_id_b, true) {
        handle_a
            .add_peer(peer_id_b)
            .await
            .connect(&addr_b)
            .await
            .unwrap();
    } else {
        handle_b
            .add_peer(peer_id_a)
            .await
            .connect(&addr_a)
            .await
            .unwrap();
    }
}
