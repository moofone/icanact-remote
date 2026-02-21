use icanact_remote::*;
use std::future::Future;
use std::time::Duration;
use tokio::{runtime::Builder, time::sleep};

const TIMING_DEBUG_THREAD_STACK_SIZE: usize = 16 * 1024 * 1024;
const TIMING_DEBUG_WORKER_STACK_SIZE: usize = 4 * 1024 * 1024;
const TIMING_DEBUG_WORKERS: usize = 2;

fn run_timing_debug_test<F, Fut>(test: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let handle = std::thread::Builder::new()
        .name("timing-debug-test".into())
        .stack_size(TIMING_DEBUG_THREAD_STACK_SIZE)
        .spawn(move || {
            let rt = Builder::new_multi_thread()
                .worker_threads(TIMING_DEBUG_WORKERS)
                .thread_stack_size(TIMING_DEBUG_WORKER_STACK_SIZE)
                .enable_all()
                .build()
                .expect("failed to build timing_debug runtime");
            rt.block_on(test());
        })
        .expect("failed to spawn timing_debug test thread");
    handle
        .join()
        .expect("timing_debug test thread panicked unexpectedly");
}

#[test]
fn debug_timing_variations() {
    run_timing_debug_test(|| async {
        println!("🔍 Debugging Timing Variations");

        // Setup two nodes with bootstrap completion
        let config = GossipConfig::default();
        let node1_addr = "127.0.0.1:25101".parse().unwrap();
        let node2_addr = "127.0.0.1:25102".parse().unwrap();

        let node1_keypair = KeyPair::new_for_testing("timing_node1");
        let node2_keypair = KeyPair::new_for_testing("timing_node2");
        let node1_id = node1_keypair.peer_id();
        let node2_id = node2_keypair.peer_id();

        let node1 =
            GossipRegistryHandle::new_with_keypair(node1_addr, node1_keypair, Some(config.clone()))
                .await
                .unwrap();
        let node2 =
            GossipRegistryHandle::new_with_keypair(node2_addr, node2_keypair, Some(config.clone()))
                .await
                .unwrap();

        let peer2 = node1.add_peer(&node2_id).await;
        peer2.connect(&node2_addr).await.unwrap();
        let peer1 = node2.add_peer(&node1_id).await;
        peer1.connect(&node1_addr).await.unwrap();

        // Give a moment for connections to stabilize
        sleep(Duration::from_millis(50)).await;

        println!("📊 Testing 10 sequential registrations to capture timing variations:");

        for i in 0..10 {
            let actor_name = format!("test_actor_{}", i);
            let start_time = std::time::Instant::now();

            node1
                .register_urgent(
                    actor_name.clone(),
                    format!("127.0.0.1:2610{}", i).parse().unwrap(),
                    RegistrationPriority::Immediate,
                )
                .await
                .unwrap();

            // Wait for propagation
            let mut propagated = false;
            while !propagated && start_time.elapsed() < Duration::from_secs(2) {
                if node2.lookup(&actor_name).await.is_some() {
                    propagated = true;
                    let total_time = start_time.elapsed();
                    println!("  Registration {}: {:?}", i, total_time);
                } else {
                    sleep(Duration::from_micros(10)).await;
                }
            }

            if !propagated {
                println!("  Registration {}: TIMEOUT", i);
            }

            // Small delay between registrations
            sleep(Duration::from_millis(10)).await;
        }

        node1.shutdown().await;
        node2.shutdown().await;
    });
}
