use anyhow::Result;
use bytes::Bytes;
use futures::future::BoxFuture;
use icanact_remote::registry::PeerDisconnectHandler;
use icanact_remote::{GossipConfig, GossipNodeId, GossipRegistryHandle, SecretKey, wire_type};
use std::alloc::{GlobalAlloc, Layout, System};
use std::env;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

struct CountingAlloc;
struct ConsoleDisconnectHandler;

impl PeerDisconnectHandler for ConsoleDisconnectHandler {
    fn handle_peer_disconnect(
        &self,
        peer_addr: std::net::SocketAddr,
        peer_id: Option<icanact_remote::PeerId>,
    ) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            println!(
                "🔌 [CLIENT] Peer disconnected: addr={} peer_id={:?}",
                peer_addr, peer_id
            );
        })
    }
}

static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static DEALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static DEALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
struct EchoRequest {
    payload: Vec<u8>,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
struct EchoResponse {
    payload: Vec<u8>,
}

wire_type!(EchoRequest, "icanact.remote.EchoRequest");
wire_type!(EchoResponse, "icanact.remote.EchoResponse");

const ACTOR_ID: u64 = 0xC0FF_EE00;

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        DEALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        DEALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

/// Console tell/ask client (TLS).
///
/// Usage:
///   cargo run --example console_tell_ask_client -- /tmp/icanact_tls/console_tell_ask_server.pub [tell_count] [ask_count] [ask_concurrency] [--typed] [--direct] [--direct-timeout] [--no-timeout]
///
/// Notes:
/// - Counts accept `k/m/g` suffixes (e.g. `1k`, `1m`), and may include `_` separators.
/// - Flags can appear in any order.
#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    tracing_subscriber::fmt().init();

    let (
        server_pub_path,
        tell_count,
        ask_count,
        ask_concurrency,
        run_typed,
        use_direct,
        use_direct_timeout,
        use_no_timeout,
    ) = parse_args(env::args().skip(1))?;

    println!("🔐 Console Tell/Ask Client (TLS)");
    println!("================================\n");

    let server_node_id = load_node_id(&server_pub_path)?;
    println!("Server GossipNodeId: {}", server_node_id.fmt_short());
    println!("Server key: {}\n", server_pub_path);

    let client_key_path = "/tmp/icanact_tls/console_tell_ask_client.key";
    let client_secret = load_or_generate_key(client_key_path)?;
    let client_node_id = client_secret.public();

    println!("Client GossipNodeId: {}", client_node_id.fmt_short());
    println!("Client key: {}\n", client_key_path);
    println!("Actor Id: 0x{:016x}\n", ACTOR_ID);

    let config = GossipConfig {
        ask_window: ask_concurrency.max(1),
        // Disable per-request timeouts to avoid timer allocations in the hot path.
        response_timeout: if use_direct_timeout {
            GossipConfig::default().response_timeout
        } else {
            Duration::ZERO
        },
        ..Default::default()
    };
    // Bind to loopback by default to keep the 2-terminal benchmark self-contained.
    // (Some sandboxed environments disallow binding 0.0.0.0.)
    let registry = GossipRegistryHandle::new_with_transport_stack(
        "127.0.0.1:0".parse()?,
        client_secret,
        Some(config),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await?;
    registry
        .registry
        .set_peer_disconnect_handler(Arc::new(ConsoleDisconnectHandler))
        .await;
    let ask_timeout = registry.registry.config.response_timeout;
    let effective_no_timeout = use_no_timeout || ask_timeout.is_zero();

    let server_addr = "127.0.0.1:29200".parse()?;
    registry
        .registry
        .add_peer_with_node_id(server_addr, Some(server_node_id))
        .await;

    // Establish TLS connection to the server.
    let conn = registry.lookup_address(server_addr).await?;
    let conn_handle = conn
        .connection_ref()
        .ok_or_else(|| anyhow::anyhow!("No connection handle for {}", server_addr))?;

    println!("✅ Connected to {}", server_addr);
    println!("Sending tell + ask via Actor frames...\n");

    let actor_id = ACTOR_ID;
    let type_hash = 0xC0FFEE00;

    // Tell (fire-and-forget) via ActorTell frame
    conn_handle
        .tell_actor_frame(actor_id, type_hash, Bytes::from_static(b"tell:hello"))
        .await?;
    println!("✅ Tell sent");

    // Ask (request-response) via ActorAsk frame
    let response: Bytes = if use_direct {
        // DirectAsk fast path: bypass ActorAsk envelope
        if use_direct_timeout {
            conn_handle
                .ask_direct(Bytes::from_static(b"ask:ping"), ask_timeout)
                .await?
        } else {
            conn_handle
                .ask_direct_no_timeout(Bytes::from_static(b"ask:ping"))
                .await?
        }
    } else {
        // Regular ask through ActorAsk frames
        if effective_no_timeout {
            conn_handle
                .ask_actor_frame_no_timeout_aligned(
                    actor_id,
                    type_hash,
                    Bytes::from_static(b"ask:ping"),
                )
                .await?
                .into_bytes()
        } else {
            conn_handle
                .ask_actor_frame_aligned(
                    actor_id,
                    type_hash,
                    Bytes::from_static(b"ask:ping"),
                    ask_timeout,
                )
                .await?
                .into_bytes()
        }
    };

    println!(
        "✅ Ask response: {:?}",
        String::from_utf8_lossy(response.as_ref())
    );

    if run_typed {
        #[cfg(debug_assertions)]
        {
            // Typed ask (debug-only type hash verification)
            let typed_response: EchoResponse = conn
                .ask_typed::<EchoRequest, EchoResponse>(&EchoRequest {
                    payload: b"ask:typed".to_vec(),
                })
                .await?;
            println!(
                "✅ Typed ask response: {:?}",
                String::from_utf8_lossy(&typed_response.payload)
            );
        }
        #[cfg(not(debug_assertions))]
        {
            println!(
                "⚠️  Typed ask is disabled in release builds; use a debug build to verify types."
            );
        }
    }

    let run = async {
        if tell_count > 0 {
            println!("\n🔸 Tell benchmark (count = {})", tell_count);
            let before = alloc_snapshot();
            let mut tell_latencies = Vec::with_capacity(tell_count);
            let tell_start = Instant::now();
            for _ in 0..tell_count {
                let start = Instant::now();
                conn_handle
                    .tell_actor_frame(actor_id, type_hash, Bytes::from_static(b"tell:hello"))
                    .await?;
                tell_latencies.push(start.elapsed());
            }
            let tell_total = tell_start.elapsed();
            let tell_rps = tell_count as f64 / tell_total.as_secs_f64();
            let after = alloc_snapshot();
            print_latency_stats("tell()", &tell_latencies);
            println!(
                "tell() throughput: {:.0} msgs/sec (total {:.3}s)",
                tell_rps,
                tell_total.as_secs_f64()
            );
            print_alloc_delta("tell()", before, after);
        }

        if ask_count > 0 {
            let ask_label = if use_direct {
                if use_direct_timeout {
                    " [DirectAsk TIMEOUT]"
                } else {
                    " [DirectAsk FAST PATH]"
                }
            } else if effective_no_timeout {
                " [ActorAsk NO TIMEOUT]"
            } else {
                " [ActorAsk FRAME]"
            };
            println!(
                "\n🔸 Ask benchmark (count = {}, workers = {}){}",
                ask_count, ask_concurrency, ask_label
            );
            let ask_payload = b"ask:ping";

            // Warm up to let TLS and buffers settle before measuring allocations.
            let warmup = ask_count.min(64);
            for _ in 0..warmup {
                if use_direct {
                    if use_direct_timeout {
                        let _ = conn
                            .connection_ref()
                            .unwrap()
                            .ask_direct(Bytes::from_static(ask_payload), ask_timeout)
                            .await?;
                    } else {
                        let _ = conn
                            .connection_ref()
                            .unwrap()
                            .ask_direct_no_timeout(Bytes::from_static(ask_payload))
                            .await?;
                    }
                } else {
                    let conn_handle = conn.connection_ref().unwrap();
                    if effective_no_timeout {
                        let _ = conn_handle
                            .ask_actor_frame_no_timeout_aligned(
                                actor_id,
                                type_hash,
                                Bytes::from_static(ask_payload),
                            )
                            .await?;
                    } else {
                        let _ = conn_handle
                            .ask_actor_frame_aligned(
                                actor_id,
                                type_hash,
                                Bytes::from_static(ask_payload),
                                ask_timeout,
                            )
                            .await?;
                    }
                }
            }
            tokio::task::yield_now().await;

            let worker_count = ask_concurrency.min(ask_count).max(1);
            let conn_handle = conn.connection_ref().unwrap().clone();
            let remaining = Arc::new(AtomicUsize::new(0));
            let warmup_done = Arc::new(AtomicUsize::new(0));
            let warmup_complete = Arc::new(Notify::new());
            let warmup_error = Arc::new(std::sync::Mutex::new(None::<String>));
            let (warmup_tx, warmup_rx) = tokio::sync::watch::channel(false);
            let (measure_tx, measure_rx) = tokio::sync::watch::channel(false);
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            let done_count = Arc::new(AtomicUsize::new(0));
            let done_notify = Arc::new(Notify::new());
            let latencies = Arc::new(
                (0..ask_count)
                    .map(|_| AtomicU64::new(0))
                    .collect::<Vec<_>>(),
            );
            let ask_payload = Bytes::from_static(ask_payload);

            let mut tasks = Vec::with_capacity(worker_count);

            for _ in 0..worker_count {
                let conn_handle = conn_handle.clone();
                let remaining = remaining.clone();
                let latencies = latencies.clone();
                let ask_payload = ask_payload.clone();
                let warmup_done = warmup_done.clone();
                let warmup_complete = warmup_complete.clone();
                let warmup_error = warmup_error.clone();
                let mut warmup_rx = warmup_rx.clone();
                let mut measure_rx = measure_rx.clone();
                let mut shutdown_rx = shutdown_rx.clone();
                let done_count = done_count.clone();
                let done_notify = done_notify.clone();
                tasks.push(tokio::spawn(async move {
                    if !*warmup_rx.borrow() {
                        warmup_rx
                            .changed()
                            .await
                            .map_err(|_| anyhow::anyhow!("warmup start dropped"))?;
                    }
                    let warmup_result = if use_direct {
                        if use_direct_timeout {
                            conn_handle
                                .ask_direct(ask_payload.clone(), ask_timeout)
                                .await
                                .map(|_| ())
                        } else {
                            conn_handle
                                .ask_direct_no_timeout(ask_payload.clone())
                                .await
                                .map(|_| ())
                        }
                    } else if effective_no_timeout {
                        conn_handle
                            .ask_actor_frame_no_timeout_aligned(
                                actor_id,
                                type_hash,
                                ask_payload.clone(),
                            )
                            .await
                            .map(|_| ())
                    } else {
                        conn_handle
                            .ask_actor_frame_aligned(
                                actor_id,
                                type_hash,
                                ask_payload.clone(),
                                ask_timeout,
                            )
                            .await
                            .map(|_| ())
                    };

                    if let Err(err) = warmup_result {
                        let mut guard = warmup_error.lock().unwrap();
                        if guard.is_none() {
                            *guard = Some(format!("{err}"));
                        }
                    }
                    if warmup_done.fetch_add(1, Ordering::Relaxed) + 1 == worker_count {
                        warmup_complete.notify_waiters();
                    }
                    if !*measure_rx.borrow() {
                        measure_rx
                            .changed()
                            .await
                            .map_err(|_| anyhow::anyhow!("measure start dropped"))?;
                    }
                    loop {
                        let idx = remaining.fetch_add(1, Ordering::Relaxed);
                        if idx >= ask_count {
                            break;
                        }
                        let start = Instant::now();
                        if use_direct {
                            if use_direct_timeout {
                                let _ = conn_handle
                                    .ask_direct(ask_payload.clone(), ask_timeout)
                                    .await?;
                            } else {
                                let _ = conn_handle
                                    .ask_direct_no_timeout(ask_payload.clone())
                                    .await?;
                            }
                        } else if effective_no_timeout {
                            let _ = conn_handle
                                .ask_actor_frame_no_timeout_aligned(
                                    actor_id,
                                    type_hash,
                                    ask_payload.clone(),
                                )
                                .await?;
                        } else {
                            let _ = conn_handle
                                .ask_actor_frame_aligned(
                                    actor_id,
                                    type_hash,
                                    ask_payload.clone(),
                                    ask_timeout,
                                )
                                .await?;
                        }
                        let nanos = start.elapsed().as_nanos() as u64;
                        latencies[idx].store(nanos, Ordering::Relaxed);
                    }
                    if done_count.fetch_add(1, Ordering::Relaxed) + 1 == worker_count {
                        done_notify.notify_waiters();
                    }
                    if !*shutdown_rx.borrow() {
                        let _ = shutdown_rx.changed().await;
                    }
                    Ok::<(), anyhow::Error>(())
                }));
            }

            let _ = warmup_tx.send(true);
            while warmup_done.load(Ordering::Relaxed) < worker_count {
                if tokio::time::timeout(Duration::from_secs(5), warmup_complete.notified())
                    .await
                    .is_err()
                {
                    return Err(anyhow::anyhow!(
                        "warmup timed out after 5s (done {}/{})",
                        warmup_done.load(Ordering::Relaxed),
                        worker_count
                    ));
                }
            }
            if let Some(err) = warmup_error.lock().unwrap().take() {
                return Err(anyhow::anyhow!("warmup failed: {err}"));
            }

            remaining.store(0, Ordering::Relaxed);
            let before = alloc_snapshot();
            let ask_start = Instant::now();
            let _ = measure_tx.send(true);

            let bench_timeout = Duration::from_secs(10);
            let wait_done = async {
                while done_count.load(Ordering::Relaxed) < worker_count {
                    done_notify.notified().await;
                }
                Ok::<(), anyhow::Error>(())
            };

            match tokio::time::timeout(bench_timeout, wait_done).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    return Err(anyhow::anyhow!(
                        "ask benchmark timed out after {}s",
                        bench_timeout.as_secs()
                    ));
                }
            }

            let ask_total = ask_start.elapsed();
            let ask_rps = ask_count as f64 / ask_total.as_secs_f64();
            let after = alloc_snapshot();

            let mut ask_latencies = Vec::with_capacity(ask_count);
            for entry in latencies.iter() {
                let nanos = entry.load(Ordering::Relaxed);
                ask_latencies.push(Duration::from_nanos(nanos));
            }
            print_latency_stats("ask()", &ask_latencies);
            println!(
                "ask() throughput: {:.0} req/sec (total {:.3}s)",
                ask_rps,
                ask_total.as_secs_f64()
            );
            print_alloc_delta("ask()", before, after);

            let _ = shutdown_tx.send(true);
            for task in tasks {
                task.await??;
            }
        }

        // Keep the client alive so the server doesn't mark the peer as failed.
        println!("\nPress Enter to exit...");
        let _ = tokio::task::spawn_blocking(|| {
            let mut line = String::new();
            let _ = std::io::stdin().read_line(&mut line);
        })
        .await;
        println!("🛑 [CLIENT] Enter received, shutting down registry...");
        registry.shutdown().await;
        Ok::<(), anyhow::Error>(())
    };

    tokio::select! {
        res = run => res?,
        _ = tokio::signal::ctrl_c() => {
            println!("🛑 [CLIENT] Ctrl+C received, shutting down...");
            registry.shutdown_and_wait().await;
        }
    }
    Ok(())
}

type ParsedArgs = (String, usize, usize, usize, bool, bool, bool, bool);

fn parse_args<I>(args: I) -> Result<ParsedArgs>
where
    I: IntoIterator<Item = String>,
{
    let mut server_pub_path: Option<String> = None;
    let mut tell_count: Option<usize> = None;
    let mut ask_count: Option<usize> = None;
    let mut ask_concurrency: Option<usize> = None;
    let mut run_typed = false;
    let mut use_direct = false;
    let mut use_direct_timeout = false;
    let mut use_no_timeout = false;

    // Positional, non-flag numeric args are interpreted as:
    //   [tell_count] [ask_count] [ask_concurrency]
    let mut numeric_args = Vec::new();

    for arg in args {
        match arg.as_str() {
            "--typed" => run_typed = true,
            "--direct" => use_direct = true,
            "--direct-timeout" => use_direct_timeout = true,
            "--no-timeout" => use_no_timeout = true,
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            _ if arg.starts_with('-') => {
                return Err(anyhow::anyhow!("Unknown flag: {arg}"));
            }
            _ => {
                if let Some(count) = parse_human_count(&arg) {
                    numeric_args.push(count);
                } else if server_pub_path.is_none() {
                    server_pub_path = Some(arg);
                } else {
                    return Err(anyhow::anyhow!(
                        "Unexpected extra argument (already have server pub path): {arg}"
                    ));
                }
            }
        }
    }

    if !numeric_args.is_empty() {
        tell_count = numeric_args.first().copied();
        ask_count = numeric_args.get(1).copied();
        ask_concurrency = numeric_args.get(2).copied();
        if numeric_args.len() > 3 {
            return Err(anyhow::anyhow!(
                "Too many numeric args: expected [tell_count] [ask_count] [ask_concurrency]"
            ));
        }
    }

    Ok((
        server_pub_path
            .unwrap_or_else(|| "/tmp/icanact_tls/console_tell_ask_server.pub".to_string()),
        tell_count.unwrap_or(1_000_000),
        ask_count.unwrap_or(100),
        ask_concurrency.unwrap_or(50),
        run_typed,
        use_direct,
        use_direct_timeout,
        use_no_timeout,
    ))
}

fn print_usage() {
    println!("Console tell/ask client (TLS).");
    println!();
    println!("Usage:");
    println!(
        "  cargo run --example console_tell_ask_client -- [server_pub_path] [tell_count] [ask_count] [ask_concurrency] [--typed] [--direct] [--direct-timeout] [--no-timeout]"
    );
    println!();
    println!("Examples:");
    println!(
        "  cargo run --example console_tell_ask_client -- /tmp/icanact_tls/console_tell_ask_server.pub 1m 0"
    );
    println!("  cargo run --example console_tell_ask_client -- --direct 1m 0 1");
    println!();
    println!("Notes:");
    println!("  - Counts accept k/m/g suffixes (e.g. 1k, 1m).");
    println!("  - Counts may include _ separators (e.g. 1_000_000).");
    println!("  - Flags can appear in any order.");
}

fn parse_human_count(raw: &str) -> Option<usize> {
    let mut s = raw.trim().to_ascii_lowercase();
    if s.is_empty() {
        return None;
    }

    // Allow `_` separators for readability.
    s.retain(|c| c != '_');

    let (multiplier, digits) = match s.as_bytes().last().copied()? {
        b'k' => (1_000u128, &s[..s.len() - 1]),
        b'm' => (1_000_000u128, &s[..s.len() - 1]),
        b'g' => (1_000_000_000u128, &s[..s.len() - 1]),
        _ => (1u128, s.as_str()),
    };

    if digits.is_empty() || !digits.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }

    let base: u128 = digits.parse().ok()?;
    let value = base.saturating_mul(multiplier);
    if value > usize::MAX as u128 {
        return None;
    }
    Some(value as usize)
}

fn load_node_id(path: &str) -> Result<GossipNodeId> {
    let pub_key_hex = fs::read_to_string(path)?;
    let pub_key_bytes = hex::decode(pub_key_hex.trim())?;

    if pub_key_bytes.len() != 32 {
        return Err(anyhow::anyhow!(
            "Invalid public key length: expected 32, got {}",
            pub_key_bytes.len()
        ));
    }

    GossipNodeId::from_bytes(&pub_key_bytes)
        .map_err(|e| anyhow::anyhow!("Invalid GossipNodeId: {}", e))
}

fn load_or_generate_key(path: &str) -> Result<SecretKey> {
    let key_path = Path::new(path);

    if key_path.exists() {
        let key_hex = fs::read_to_string(key_path)?;
        let key_bytes = hex::decode(key_hex.trim())?;

        if key_bytes.len() != 32 {
            return Err(anyhow::anyhow!(
                "Invalid key length: expected 32, got {}",
                key_bytes.len()
            ));
        }

        let mut arr = [0u8; 32];
        arr.copy_from_slice(&key_bytes);
        Ok(SecretKey::from_bytes(&arr)?)
    } else {
        if let Some(parent) = key_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let secret_key = SecretKey::generate();
        fs::write(key_path, hex::encode(secret_key.to_bytes()))?;
        Ok(secret_key)
    }
}

#[derive(Clone, Copy)]
struct AllocSnapshot {
    allocs: u64,
    deallocs: u64,
    alloc_bytes: u64,
    dealloc_bytes: u64,
}

fn alloc_snapshot() -> AllocSnapshot {
    AllocSnapshot {
        allocs: ALLOC_COUNT.load(Ordering::Relaxed),
        deallocs: DEALLOC_COUNT.load(Ordering::Relaxed),
        alloc_bytes: ALLOC_BYTES.load(Ordering::Relaxed),
        dealloc_bytes: DEALLOC_BYTES.load(Ordering::Relaxed),
    }
}

fn print_alloc_delta(label: &str, before: AllocSnapshot, after: AllocSnapshot) {
    let allocs = after.allocs.saturating_sub(before.allocs);
    let deallocs = after.deallocs.saturating_sub(before.deallocs);
    let alloc_bytes = after.alloc_bytes.saturating_sub(before.alloc_bytes);
    let dealloc_bytes = after.dealloc_bytes.saturating_sub(before.dealloc_bytes);
    let live_bytes = alloc_bytes.saturating_sub(dealloc_bytes);

    println!(
        "{} allocations: allocs {} ({} bytes) | deallocs {} ({} bytes) | net {} bytes",
        label, allocs, alloc_bytes, deallocs, dealloc_bytes, live_bytes
    );
}

fn print_latency_stats(label: &str, latencies: &[Duration]) {
    if latencies.is_empty() {
        return;
    }

    let mut nanos: Vec<u128> = latencies.iter().map(|d| d.as_nanos()).collect();
    nanos.sort_unstable();

    let len = nanos.len() as u128;
    let sum: u128 = nanos.iter().sum();
    let mean = sum / len;

    let p50 = percentile(&nanos, 50);
    let p95 = percentile(&nanos, 95);
    let p99 = percentile(&nanos, 99);
    let min = *nanos.first().unwrap();
    let max = *nanos.last().unwrap();

    println!(
        "{} latency (µs): min {:.2} | p50 {:.2} | p95 {:.2} | p99 {:.2} | max {:.2} | avg {:.2}",
        label,
        nanos_to_micros(min),
        nanos_to_micros(p50),
        nanos_to_micros(p95),
        nanos_to_micros(p99),
        nanos_to_micros(max),
        nanos_to_micros(mean)
    );
}

fn percentile(sorted_nanos: &[u128], pct: u128) -> u128 {
    if sorted_nanos.is_empty() {
        return 0;
    }
    let idx = ((pct * (sorted_nanos.len() as u128 - 1)) / 100) as usize;
    sorted_nanos[idx]
}

fn nanos_to_micros(nanos: u128) -> f64 {
    nanos as f64 / 1000.0
}
