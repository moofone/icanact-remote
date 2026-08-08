use std::{
    collections::HashMap,
    io,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures::future::BoxFuture;

use icanact_remote::{DnsResolver, GossipConfig, GossipRegistryHandle, KeyPair};

#[derive(Default)]
struct ScriptedResolver {
    // dns_name -> sequence of results returned on successive lookups
    scripted: Mutex<HashMap<String, Vec<Vec<SocketAddr>>>>,
    calls: AtomicUsize,
}

impl ScriptedResolver {
    fn with_script(dns: &str, script: Vec<Vec<SocketAddr>>) -> Self {
        let mut scripted = HashMap::new();
        scripted.insert(dns.to_string(), script);
        Self {
            scripted: Mutex::new(scripted),
            calls: AtomicUsize::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::Acquire)
    }
}

impl DnsResolver for ScriptedResolver {
    fn lookup<'a>(&'a self, dns: &'a str) -> BoxFuture<'a, io::Result<Vec<SocketAddr>>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::AcqRel);
            let mut map = self.scripted.lock().unwrap();
            let Some(seq) = map.get_mut(dns) else {
                return Ok(vec![]);
            };
            if seq.is_empty() {
                return Ok(vec![]);
            }
            Ok(seq.remove(0))
        })
    }
}

async fn new_registry_with_keypair(
    bind: SocketAddr,
    keypair: KeyPair,
) -> icanact_remote::Result<GossipRegistryHandle> {
    let cfg = GossipConfig {
        key_pair: Some(keypair.clone()),
        allow_loopback_discovery: true, // tests use 127.0.0.1
        connection_timeout: Duration::from_millis(150),
        ..Default::default()
    };
    GossipRegistryHandle::new_with_transport_stack(
        bind,
        keypair.to_secret_key(),
        Some(cfg),
        icanact_remote::BuilderTlsBootstrap,
    )
    .await
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

fn unused_local_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dial_failure_triggers_dns_refresh_and_reconnect_succeeds() -> icanact_remote::Result<()> {
    let (a_keypair, b_keypair) = key_pair_ordered_for_outbound_a("dns-a", "dns-b");
    let b = new_registry_with_keypair("127.0.0.1:0".parse().unwrap(), b_keypair).await?;
    let b_addr = b.registry.bind_addr;
    let b_peer_id = b.registry.peer_id.clone();

    let a = new_registry_with_keypair("127.0.0.1:0".parse().unwrap(), a_keypair).await?;

    let stale_addr = loop {
        let candidate = unused_local_addr();
        if candidate.port() != b_addr.port() {
            break candidate;
        }
    };

    let dns_name = format!("b.service.invalid:{}", b_addr.port());
    let resolver = Arc::new(ScriptedResolver::with_script(&dns_name, vec![vec![b_addr]]));
    a.registry.set_dns_resolver(resolver.clone()).await;

    // Configure a peer using a stale address + a DNS name that resolves to the live address.
    a.registry
        .add_peer_with_node_id(
            stale_addr,
            Some(b_peer_id.to_node_id()),
            icanact_remote::addr_ownership::ClaimKind::Verified,
        )
        .await;
    let _ = a.registry
        .configure_peer(b_peer_id.clone(), stale_addr)
        .await;
    a.registry
        .set_peer_dns_name(stale_addr, dns_name.clone())
        .await;

    // connect_to_peer will dial stale_addr first, fail, refresh DNS, then retry the new address.
    a.registry.connect_to_peer(&b_peer_id).await?;

    let mapped = a
        .registry
        .connection_pool
        .peer_id_to_addr
        .read_sync(&b_peer_id, |_, v| *v)
        .unwrap();
    assert_eq!(mapped, b_addr);
    assert!(
        resolver.calls() >= 1,
        "expected resolver to be consulted on failure"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dial_failure_with_empty_resolution_does_not_update_mapping() -> icanact_remote::Result<()>
{
    let (a_keypair, b_keypair) = key_pair_ordered_for_outbound_a("dns-a2", "dns-b2");
    let b = new_registry_with_keypair("127.0.0.1:0".parse().unwrap(), b_keypair).await?;
    let b_addr = b.registry.bind_addr;
    let b_peer_id = b.registry.peer_id.clone();

    let a = new_registry_with_keypair("127.0.0.1:0".parse().unwrap(), a_keypair).await?;

    let stale_addr = loop {
        let candidate = unused_local_addr();
        if candidate.port() != b_addr.port() {
            break candidate;
        }
    };

    let dns_name = format!("b2.service.invalid:{}", b_addr.port());
    let resolver = Arc::new(ScriptedResolver::with_script(&dns_name, vec![vec![]]));
    a.registry.set_dns_resolver(resolver.clone()).await;

    a.registry
        .add_peer_with_node_id(
            stale_addr,
            Some(b_peer_id.to_node_id()),
            icanact_remote::addr_ownership::ClaimKind::Verified,
        )
        .await;
    let _ = a.registry
        .configure_peer(b_peer_id.clone(), stale_addr)
        .await;
    a.registry
        .set_peer_dns_name(stale_addr, dns_name.clone())
        .await;

    let err = a.registry.connect_to_peer(&b_peer_id).await.unwrap_err();
    // We don't care about the exact failure kind here, only that mapping stays stale.
    let mapped = a
        .registry
        .connection_pool
        .peer_id_to_addr
        .read_sync(&b_peer_id, |_, v| *v)
        .unwrap();
    assert_eq!(
        mapped, stale_addr,
        "mapping should not change on empty resolution"
    );
    assert!(
        resolver.calls() >= 1,
        "expected resolver to be consulted on failure"
    );
    drop(err);

    Ok(())
}
