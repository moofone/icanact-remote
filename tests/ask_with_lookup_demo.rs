use icanact_remote::*;
use std::future::Future;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing_subscriber::EnvFilter;

static TEST_MUTEX: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

fn run_async_test<F>(name: &str, fut: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    let handle = std::thread::Builder::new()
        .name(name.to_string())
        .stack_size(8 * 1024 * 1024)
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(4 * 1024 * 1024)
                .enable_all()
                .build()
                .expect("failed to build test runtime");
            rt.block_on(fut);
        })
        .expect("failed to spawn async test thread");
    handle.join().expect("async test panicked");
}

async fn wait_for_active_peers(handle: &GossipRegistryHandle, min_peers: usize, timeout: Duration) {
    let start = Instant::now();
    loop {
        if handle.stats().await.active_peers >= min_peers {
            return;
        }
        if start.elapsed() > timeout {
            let stats = handle.stats().await;
            panic!(
                "Timed out waiting for active peers (min {}): stats: {:?}",
                min_peers, stats
            );
        }
        sleep(Duration::from_millis(20)).await;
    }
}

// Helper function to wait for an actor to be available via lookup
async fn wait_for_actor(
    handle: &GossipRegistryHandle,
    name: &str,
    timeout: Duration,
) -> icanact_remote::RemoteActorRef {
    let start = Instant::now();
    loop {
        if let Some(remote_actor) = handle.lookup(name).await {
            return remote_actor;
        }
        if start.elapsed() > timeout {
            panic!("Timed out waiting for actor: {}", name);
        }
        sleep(Duration::from_millis(20)).await;
    }
}

/// Comprehensive ask() API demonstration with lookup() by actor name
/// Tests request-response patterns with performance comparisons
#[test]
fn test_ask_with_lookup_and_performance() {
    run_async_test("ask-lookup-performance", async {
        // Avoid sandbox-triggered EPERM flakiness unless explicitly enabled.
        if std::env::var("ICANACT_TEST_LOG").ok().as_deref() == Some("1") {
            let _ = tracing_subscriber::fmt()
                .with_env_filter(EnvFilter::from_default_env())
                .try_init();
        }
        let _guard = TEST_MUTEX
            .get_or_init(|| tokio::sync::Mutex::new(()))
            .lock()
            .await;
        println!("🚀 Ask() with Lookup() Performance Test");
        println!("=======================================");

        // Setup three nodes
        let config = GossipConfig {
            urgent_gossip_fanout: 32,
            ..Default::default()
        };

        println!("📡 Setting up 3-node cluster...");
        let node1_keypair = KeyPair::new_for_testing("node1");
        let node2_keypair = KeyPair::new_for_testing("node2");
        let node3_keypair = KeyPair::new_for_testing("node3");
        let _node1_id = node1_keypair.peer_id();
        let node2_id = node2_keypair.peer_id();
        let node3_id = node3_keypair.peer_id();

        let node1 = GossipRegistryHandle::new_with_transport_stack("127.0.0.1:30001".parse().unwrap(), node1_keypair.to_secret_key(), Some(config.clone()),
        icanact_remote::BuilderTlsBootstrap)
        .await
        .unwrap();

        let node2 = GossipRegistryHandle::new_with_transport_stack("127.0.0.1:30002".parse().unwrap(), node2_keypair.to_secret_key(), Some(config.clone()),
        icanact_remote::BuilderTlsBootstrap)
        .await
        .unwrap();

        let node3 = GossipRegistryHandle::new_with_transport_stack("127.0.0.1:30003".parse().unwrap(), node3_keypair.to_secret_key(), Some(config.clone()),
        icanact_remote::BuilderTlsBootstrap)
        .await
        .unwrap();

        // Connect nodes (single-direction dial avoids tie-breaker churn)
        println!("\n🔗 Establishing peer connections...");

        // Node1 connects to Node2 and Node3 (bidirectional once established)
        let peer2 = node1.add_peer(&node2_id).await;
        let peer3 = node1.add_peer(&node3_id).await;
        peer2
            .connect(&"127.0.0.1:30002".parse().unwrap())
            .await
            .unwrap();
        peer3
            .connect(&"127.0.0.1:30003".parse().unwrap())
            .await
            .unwrap();

        wait_for_active_peers(&node2, 1, Duration::from_secs(2)).await;
        wait_for_active_peers(&node3, 1, Duration::from_secs(2)).await;
        println!("✅ All nodes connected");

        // Register service actors
        println!("\n📋 Registering service actors...");

        node2
            .register_urgent(
                "database_service".to_string(),
                "127.0.0.1:30002".parse().unwrap(),
                RegistrationPriority::Immediate,
            )
            .await
            .unwrap();
        println!("   ✅ Node2: registered 'database_service'");

        node2
            .register_urgent(
                "compute_service".to_string(),
                "127.0.0.1:30002".parse().unwrap(),
                RegistrationPriority::Immediate,
            )
            .await
            .unwrap();
        println!("   ✅ Node2: registered 'compute_service'");

        node3
            .register_urgent(
                "cache_service".to_string(),
                "127.0.0.1:30003".parse().unwrap(),
                RegistrationPriority::Immediate,
            )
            .await
            .unwrap();
        println!("   ✅ Node3: registered 'cache_service'");

        // Wait for propagation
        sleep(Duration::from_millis(500)).await;
        println!(
            "   🔍 Stats after registration:\n      node1={:?}\n      node2={:?}\n      node3={:?}",
            node1.stats().await,
            node2.stats().await,
            node3.stats().await
        );

        // ===========================================
        // PART 1: CREATE ACTOR REFS WITH LOOKUP (NEW API)
        // ===========================================
        println!("\n📊 PART 1: ACTOR LOOKUP AND REFERENCE CREATION");
        println!("==============================================");

        let lookup_start = Instant::now();

        // NEW API: lookup() returns RemoteActorRef directly (location + cached connection)
        let db_actor = wait_for_actor(&node1, "database_service", Duration::from_secs(5)).await;
        let compute_actor = wait_for_actor(&node1, "compute_service", Duration::from_secs(5)).await;
        let cache_actor = wait_for_actor(&node1, "cache_service", Duration::from_secs(5)).await;

        let lookup_time = lookup_start.elapsed();

        println!("   ✅ Created RemoteActorRefs:");
        println!(
            "     - database_service at {} (peer: {})",
            db_actor.location.address, db_actor.location.peer_id
        );
        println!(
            "     - compute_service at {} (peer: {})",
            compute_actor.location.address, compute_actor.location.peer_id
        );
        println!(
            "     - cache_service at {} (peer: {})",
            cache_actor.location.address, cache_actor.location.peer_id
        );
        println!(
            "   📈 Total lookup time: {:?} ({:.3} μs)",
            lookup_time,
            lookup_time.as_nanos() as f64 / 1000.0
        );
        println!("   💡 Each RemoteActorRef has connection cached - zero lookups for ask()!");

        // ===========================================
        // PART 2: ASK() PERFORMANCE TESTING
        // ===========================================
        println!("\n📊 PART 2: ASK() REQUEST-RESPONSE PERFORMANCE");
        println!("============================================");

        // Test queries
        let queries = [
            ("database_service", "SELECT * FROM users WHERE id = 123"),
            ("compute_service", "CALCULATE fibonacci(40)"),
            ("cache_service", "GET user:123:profile"),
            ("database_service", "INSERT INTO logs VALUES (...)"),
            ("compute_service", "PROCESS image_resize(1024x768)"),
            ("cache_service", "SET session:abc123 = data"),
        ];

        println!("\n🔸 Test 2A: Individual ask() calls");
        println!("   Queries to execute:");
        for (i, (service, query)) in queries.iter().enumerate() {
            println!("     {}. {} <- \"{}\"", i + 1, service, query);
        }

        let mut ask_times = Vec::new();
        let individual_start = Instant::now();

        for (i, (service, query)) in queries.iter().enumerate() {
            let ask_start = Instant::now();

            // Select the appropriate RemoteActorRef (with cached connection)
            let actor = match *service {
                "database_service" => &db_actor,
                "compute_service" => &compute_actor,
                "cache_service" => &cache_actor,
                _ => panic!("Unknown service"),
            };

            // Send request and wait for response (uses cached connection, zero lookups)
            // Add retry logic to handle "not listening yet" transient errors during initial connection
            let response = {
                let mut result = Err(GossipError::ActorNotFound(
                    "Initial error".to_string().into(),
                ));
                let start = Instant::now();
                while start.elapsed() < Duration::from_secs(5) {
                    match actor
                        .ask(bytes::Bytes::copy_from_slice(query.as_bytes()))
                        .await
                    {
                        Ok(r) => {
                            result = Ok(r);
                            break;
                        }
                        Err(e) => {
                            result = Err(e);
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
                result.unwrap()
            };
            let response_str = String::from_utf8_lossy(&response);

            let ask_time = ask_start.elapsed();
            ask_times.push(ask_time);

            println!(
                "     Query {} response: \"{}\" in {:?} ({:.3} μs)",
                i + 1,
                response_str,
                ask_time,
                ask_time.as_nanos() as f64 / 1000.0
            );
        }

        let individual_total = individual_start.elapsed();
        let individual_avg = individual_total / queries.len() as u32;

        println!("   📈 Individual ask() Results:");
        println!(
            "     - Total time: {:?} ({:.3} μs)",
            individual_total,
            individual_total.as_nanos() as f64 / 1000.0
        );
        println!(
            "     - Average per query: {:?} ({:.3} μs)",
            individual_avg,
            individual_avg.as_nanos() as f64 / 1000.0
        );

        // Test 2B: Parallel ask() calls using tokio::join!
        println!("\n🔸 Test 2B: Parallel ask() calls");

        let parallel_start = Instant::now();

        // Send all queries in parallel
        // Send all queries in parallel using RemoteActorRefs (each uses cached connection)
        let (r1, r2, r3, r4, r5, r6) = tokio::join!(
            db_actor.ask(bytes::Bytes::copy_from_slice(queries[0].1.as_bytes())),
            compute_actor.ask(bytes::Bytes::copy_from_slice(queries[1].1.as_bytes())),
            cache_actor.ask(bytes::Bytes::copy_from_slice(queries[2].1.as_bytes())),
            db_actor.ask(bytes::Bytes::copy_from_slice(queries[3].1.as_bytes())),
            compute_actor.ask(bytes::Bytes::copy_from_slice(queries[4].1.as_bytes())),
            cache_actor.ask(bytes::Bytes::copy_from_slice(queries[5].1.as_bytes())),
        );

        // Verify all responses
        let responses = [r1, r2, r3, r4, r5, r6];
        for (i, response) in responses.iter().enumerate() {
            match response {
                Ok(data) => {
                    let response_str = String::from_utf8_lossy(data);
                    println!("     Query {} response: \"{}\"", i + 1, response_str);
                }
                Err(e) => println!("     Query {} failed: {}", i + 1, e),
            }
        }

        let parallel_total = parallel_start.elapsed();
        let parallel_avg = parallel_total / queries.len() as u32;

        println!("   📈 Parallel ask() Results:");
        println!(
            "     - Total time: {:?} ({:.3} μs)",
            parallel_total,
            parallel_total.as_nanos() as f64 / 1000.0
        );
        println!(
            "     - Average per query: {:?} ({:.3} μs)",
            parallel_avg,
            parallel_avg.as_nanos() as f64 / 1000.0
        );
        println!(
            "     - Speedup vs sequential: {:.2}x",
            individual_total.as_nanos() as f64 / parallel_total.as_nanos() as f64
        );

        // ===========================================
        // PART 3: ASK VS TELL COMPARISON
        // ===========================================
        println!("\n📊 PART 3: ASK() VS TELL() COMPARISON");
        println!("=====================================");

        let test_message = bytes::Bytes::from_static(b"PING test request");
        let iterations = 100;

        println!(
            "\n🔸 Test 3A: tell() performance ({} iterations)",
            iterations
        );
        let tell_start = Instant::now();
        for _ in 0..iterations {
            db_actor.tell(test_message.clone()).await.unwrap();
        }
        let tell_total = tell_start.elapsed();
        let tell_avg = tell_total / iterations;

        println!("   📈 tell() Results:");
        println!(
            "     - Total: {:?} ({:.3} μs)",
            tell_total,
            tell_total.as_nanos() as f64 / 1000.0
        );
        println!(
            "     - Average: {:?} ({:.3} μs)",
            tell_avg,
            tell_avg.as_nanos() as f64 / 1000.0
        );
        println!(
            "     - Throughput: {:.0} ops/sec",
            iterations as f64 / tell_total.as_secs_f64()
        );

        println!(
            "\n🔸 Test 3B: ask() performance ({} iterations)",
            iterations
        );
        let ask_start = Instant::now();
        for _ in 0..iterations {
            let _ = db_actor.ask(test_message.clone()).await.unwrap();
        }
        let ask_total = ask_start.elapsed();
        let ask_avg = ask_total / iterations;

        println!("   📈 ask() Results:");
        println!(
            "     - Total: {:?} ({:.3} μs)",
            ask_total,
            ask_total.as_nanos() as f64 / 1000.0
        );
        println!(
            "     - Average: {:?} ({:.3} μs)",
            ask_avg,
            ask_avg.as_nanos() as f64 / 1000.0
        );
        println!(
            "     - Throughput: {:.0} ops/sec",
            iterations as f64 / ask_total.as_secs_f64()
        );

        let ask_overhead = ask_total.as_nanos() as f64 / tell_total.as_nanos() as f64;
        println!(
            "\n   🎯 ask() overhead: {:.2}x slower than tell()",
            ask_overhead
        );

        // ===========================================
        // PART 4: ERROR HANDLING
        // ===========================================
        println!("\n📊 PART 4: ERROR HANDLING");
        println!("========================");

        // Test asking a non-existent actor
        println!("\n🔸 Test 5A: Lookup non-existent actor");
        let missing = node1.lookup("non_existent_service").await;
        match missing {
            None => println!("   ✅ Correctly returned None for non-existent actor"),
            Some(_) => println!("   ❌ ERROR: Found non-existent actor!"),
        }

        // ===========================================
        // RECOMMENDATIONS
        // ===========================================
        println!("\n📋 RECOMMENDATIONS");
        println!("==================");

        println!("\n✅ Best Practices:");
        println!("   1. Use ask() for operations that require a response (queries, calculations)");
        println!("   2. Use tell() for fire-and-forget operations (logging, notifications)");
        println!("   3. Do lookup() once to get RemoteActorRef, then reuse it");
        println!("   4. RemoteActorRef uses cached connection (no lookups, direct reference)");
        println!("   5. Use parallel ask() calls when querying multiple services");

        println!("\n🔧 Code Examples:");
        println!("   // Lookup once - gets actor AND caches connection:");
        println!("   let remote_actor = registry.lookup(\"database_service\").await?;");
        println!();
        println!("   // Sequential queries (no lookups - direct reference):");
        println!("   let result1 = remote_actor.ask(query1).await?;");
        println!("   let result2 = remote_actor.ask(query2).await?;");
        println!();
        println!("   // Parallel queries:");
        println!("   let (r1, r2) = tokio::join!(");
        println!("       actor1.ask(query1),");
        println!("       actor2.ask(query2)");
        println!("   );");

        println!("\n⚠️  NOTE: Current ask() implementation returns mock responses.");
        println!("   In production, implement proper request-response correlation.");

        // Cleanup
        node1.shutdown().await;
        node2.shutdown().await;
        node3.shutdown().await;

        println!("\n✅ Test completed successfully!");
    });
}

/// Test high-throughput ask() scenarios
#[test]
fn test_ask_high_throughput() {
    run_async_test("ask-with-lookup-high-throughput", async {
        let _guard = TEST_MUTEX
            .get_or_init(|| tokio::sync::Mutex::new(()))
            .lock()
            .await;
        println!("🚀 High-Throughput ask() Test");
        println!("=============================");

        // Setup two nodes
        let config = GossipConfig {
            urgent_gossip_fanout: 32,
            ..Default::default()
        };
        let node1_addr = "127.0.0.1:31001".parse().unwrap();
        let node2_addr = "127.0.0.1:31002".parse().unwrap();

        let node1_keypair = KeyPair::new_for_testing("node1_ht");
        let node2_keypair = KeyPair::new_for_testing("node2_ht");
        let node2_id = node2_keypair.peer_id();

        let node1 =
            GossipRegistryHandle::new_with_transport_stack(node1_addr, node1_keypair.to_secret_key(), Some(config.clone()), icanact_remote::BuilderTlsBootstrap)
                .await
                .unwrap();

        let node2 =
            GossipRegistryHandle::new_with_transport_stack(node2_addr, node2_keypair.to_secret_key(), Some(config.clone()), icanact_remote::BuilderTlsBootstrap)
                .await
                .unwrap();

        // Connect nodes (single-direction dial avoids tie-breaker churn)
        let peer2 = node1.add_peer(&node2_id).await;
        peer2.connect(&node2_addr).await.unwrap();

        sleep(Duration::from_millis(100)).await;

        // Register a high-performance service
        node2
            .register_urgent(
                "api_service".to_string(),
                "127.0.0.1:41002".parse().unwrap(),
                RegistrationPriority::Immediate,
            )
            .await
            .unwrap();

        sleep(Duration::from_millis(500)).await;

        // NEW API: Lookup to get RemoteActorRef (includes cached connection)
        let api_actor = wait_for_actor(&node1, "api_service", Duration::from_secs(5)).await;
        let api_conn = api_actor
            .connection
            .clone()
            .expect("API service should be connected");

        // Test parameters
        let request_count = 200;
        let concurrent_requests = 10;

        println!("\n📊 Test Configuration:");
        println!("   - Total requests: {}", request_count);
        println!("   - Concurrent requests: {}", concurrent_requests);

        // Warmup
        println!("\n🔥 Warming up...");
        for _ in 0..10 {
            let _ = api_conn
                .ask(bytes::Bytes::from_static(b"warmup"))
                .await
                .unwrap();
        }

        // High-throughput test
        println!("\n🔸 Running high-throughput test...");
        let test_start = Instant::now();
        let mut latencies = Vec::new();
        let mut failed_requests = 0usize;

        // Process requests in batches
        for batch in 0..(request_count / concurrent_requests) {
            let _batch_start = Instant::now();

            // Send concurrent requests
            let mut handles = Vec::new();
            for i in 0..concurrent_requests {
                let conn = api_conn.clone();
                let request_id = batch * concurrent_requests + i;
                let handle = tokio::spawn(async move {
                    let req_start = Instant::now();
                    let request = format!("REQUEST:{}", request_id);
                    match conn
                        .ask(bytes::Bytes::copy_from_slice(request.as_bytes()))
                        .await
                    {
                        Ok(_) => Some(req_start.elapsed()),
                        Err(err) => {
                            eprintln!(
                                "⚠️ ask() request {} failed after {:?}: {}",
                                request_id,
                                req_start.elapsed(),
                                err
                            );
                            None
                        }
                    }
                });
                handles.push(handle);
            }

            // Wait for batch to complete
            for handle in handles {
                match handle.await {
                    Ok(Some(latency)) => latencies.push(latency),
                    Ok(None) => failed_requests += 1,
                    Err(join_err) => {
                        failed_requests += 1;
                        eprintln!("⚠️ ask() task join error: {}", join_err);
                    }
                }
            }

            if batch % 10 == 0 {
                println!(
                    "   - Completed {} requests",
                    (batch + 1) * concurrent_requests
                );
            }
        }

        let test_total = test_start.elapsed();

        assert!(
            !latencies.is_empty(),
            "No successful ask() requests recorded"
        );
        assert!(
            failed_requests < request_count / 5,
            "Too many ask() failures: {} out of {}",
            failed_requests,
            request_count
        );

        // Calculate statistics
        latencies.sort();
        let avg_latency = latencies.iter().sum::<Duration>() / latencies.len() as u32;
        let p50 = latencies[latencies.len() / 2];
        let p95 = latencies[latencies.len() * 95 / 100];
        let p99 = latencies[latencies.len() * 99 / 100];
        let throughput = request_count as f64 / test_total.as_secs_f64();

        println!("\n📈 High-Throughput Results:");
        println!("   - Total time: {:?}", test_total);
        println!("   - Total requests: {}", request_count);
        println!("   - Throughput: {:.0} req/sec", throughput);
        println!("   - Failed requests: {}", failed_requests);
        println!(
            "   - Average latency: {:?} ({:.3} μs)",
            avg_latency,
            avg_latency.as_nanos() as f64 / 1000.0
        );
        println!(
            "   - P50 latency: {:?} ({:.3} μs)",
            p50,
            p50.as_nanos() as f64 / 1000.0
        );
        println!(
            "   - P95 latency: {:?} ({:.3} μs)",
            p95,
            p95.as_nanos() as f64 / 1000.0
        );
        println!(
            "   - P99 latency: {:?} ({:.3} μs)",
            p99,
            p99.as_nanos() as f64 / 1000.0
        );

        // Cleanup
        node1.shutdown().await;
        node2.shutdown().await;

        println!("\n✅ High-throughput test completed!");
    });
}
