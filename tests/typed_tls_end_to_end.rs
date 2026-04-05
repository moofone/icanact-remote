use icanact_remote::{GossipConfig, GossipRegistryHandle, KeyPair, wire_type};
use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::time::sleep;

const TYPED_TLS_THREAD_STACK_SIZE: usize = 32 * 1024 * 1024;
const TYPED_TLS_WORKER_STACK_SIZE: usize = 8 * 1024 * 1024;
const TYPED_TLS_WORKERS: usize = 4;

fn run_typed_tls_test<F, Fut>(name: &'static str, test: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let handle = std::thread::Builder::new()
        .name(format!("typed-tls-test-{}", name))
        .stack_size(TYPED_TLS_THREAD_STACK_SIZE)
        .spawn(move || {
            let runtime = Builder::new_multi_thread()
                .worker_threads(TYPED_TLS_WORKERS)
                .thread_stack_size(TYPED_TLS_WORKER_STACK_SIZE)
                .enable_all()
                .build()
                .expect("failed to build typed TLS test runtime");
            runtime.block_on(test());
        })
        .expect("failed to spawn typed TLS test thread");

    handle.join().expect("typed TLS test panicked");
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, PartialEq)]
struct Ping {
    id: u64,
}

wire_type!(Ping, "icanact.remote.PingTLS");

#[test]
fn test_typed_ask_over_tls_with_pooled_path() {
    run_typed_tls_test("typed-ask-pooled", || async {
        unsafe {
            std::env::set_var("ICANACT_REMOTE_TYPED_ECHO", "1");
        }

        let addr_a: SocketAddr = "127.0.0.1:9011".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:9012".parse().unwrap();

        let key_pair_a = KeyPair::new_for_testing("typed_tls_a");
        let key_pair_b = KeyPair::new_for_testing("typed_tls_b");
        let peer_id_b = key_pair_b.peer_id();

        let config = GossipConfig {
            gossip_interval: Duration::from_secs(300),
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

        let peer_b = handle_a.add_peer(&peer_id_b).await;
        peer_b.connect(&addr_b).await.unwrap();

        sleep(Duration::from_millis(200)).await;

        let conn = handle_a.lookup_address(addr_b).await.unwrap();
        let request = Ping { id: 42 };
        let response: Ping = conn.ask_typed(&request).await.unwrap();
        assert_eq!(response, request);

        handle_a.shutdown().await;
        handle_b.shutdown().await;

        unsafe {
            std::env::remove_var("ICANACT_REMOTE_TYPED_ECHO");
        }
    });
}

#[test]
fn test_typed_tell_over_tls_with_pooled_path() {
    run_typed_tls_test("typed-tell-pooled", || async {
        use tokio::time::{Duration, Instant};

        unsafe {
            std::env::set_var("ICANACT_REMOTE_TYPED_TELL_CAPTURE", "1");
        }
        icanact_remote::test_helpers::drain_raw_payloads();

        let addr_a: SocketAddr = "127.0.0.1:9013".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:9014".parse().unwrap();

        let key_pair_a = KeyPair::new_for_testing("typed_tls_tell_a");
        let key_pair_b = KeyPair::new_for_testing("typed_tls_tell_b");
        let peer_id_b = key_pair_b.peer_id();

        let config = GossipConfig {
            gossip_interval: Duration::from_secs(300),
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

        let peer_b = handle_a.add_peer(&peer_id_b).await;
        peer_b.connect(&addr_b).await.unwrap();

        sleep(Duration::from_millis(200)).await;
        icanact_remote::test_helpers::drain_raw_payloads();

        let conn = handle_a.lookup_address(addr_b).await.unwrap();
        let request = Ping { id: 7 };
        conn.tell_typed(&request).await.unwrap();

        let deadline = Instant::now() + Duration::from_secs(10);
        let mut decoded: Option<Ping> = None;
        while Instant::now() < deadline {
            if let Some(payload) =
                icanact_remote::test_helpers::wait_for_raw_payload(Duration::from_millis(200)).await
            {
                // Force alignment by copying to a fresh Vec
                // record_raw_payload captures a slice offset by 4 bytes (len prefix), causing misalignment
                // for rkyv if accessed directly.
                let aligned = payload.to_vec();
                if let Ok(msg) = icanact_remote::decode_typed::<Ping>(&aligned) {
                    decoded = Some(msg);
                    break;
                }
            }
        }

        if decoded.is_none() {
            let payloads = icanact_remote::test_helpers::drain_raw_payloads();
            let lengths: Vec<usize> = payloads.iter().map(|p| p.len()).collect();
            panic!(
                "typed tell payload not captured; saw {} raw payloads with lengths {:?}",
                lengths.len(),
                lengths
            );
        }

        assert_eq!(decoded, Some(request));

        handle_a.shutdown().await;
        handle_b.shutdown().await;

        unsafe {
            std::env::remove_var("ICANACT_REMOTE_TYPED_TELL_CAPTURE");
        }
    });
}
