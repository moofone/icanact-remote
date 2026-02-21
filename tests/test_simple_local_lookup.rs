use icanact_remote::*;
use std::future::Future;
use std::time::Duration;
use tokio::runtime::Builder;

const SIMPLE_LOOKUP_THREAD_STACK_SIZE: usize = 16 * 1024 * 1024;
const SIMPLE_LOOKUP_WORKER_STACK_SIZE: usize = 4 * 1024 * 1024;
const SIMPLE_LOOKUP_WORKERS: usize = 2;

fn run_simple_lookup_test<F, Fut>(test: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let handle = std::thread::Builder::new()
        .name("simple-local-lookup".into())
        .stack_size(SIMPLE_LOOKUP_THREAD_STACK_SIZE)
        .spawn(move || {
            let rt = Builder::new_multi_thread()
                .worker_threads(SIMPLE_LOOKUP_WORKERS)
                .thread_stack_size(SIMPLE_LOOKUP_WORKER_STACK_SIZE)
                .enable_all()
                .build()
                .expect("failed to build simple lookup runtime");
            rt.block_on(test());
        })
        .expect("failed to spawn simple lookup test thread");
    handle
        .join()
        .expect("simple local lookup test panicked unexpectedly");
}

#[test]
fn test_simple_local_lookup() {
    run_simple_lookup_test(|| async {
        let config = GossipConfig {
            gossip_interval: Duration::from_secs(300),
            ..Default::default()
        };

        let key_pair = KeyPair::new_for_testing("test");
        let handle = GossipRegistryHandle::new_with_keypair(
            "127.0.0.1:0".parse().unwrap(),
            key_pair,
            Some(config),
        )
        .await
        .unwrap();

        let addr = handle.registry.bind_addr;

        // Register a local actor
        handle
            .register_urgent(
                "test_actor".to_string(),
                addr,
                RegistrationPriority::Immediate,
            )
            .await
            .unwrap();

        // Try to look it up
        let result = handle.lookup("test_actor").await;

        match result {
            Some(actor) => println!("✅ Found actor at {}", actor.location.address),
            None => println!("❌ Actor not found!"),
        }

        handle.shutdown().await;
    });
}
