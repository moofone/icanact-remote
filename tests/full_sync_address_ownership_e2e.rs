//! Authenticated address-ownership security boundary.
//!
//! A peer's TLS identity authenticates *who sent* a FullSync frame. It does
//! not prove that the peer owns an arbitrary `sender_bind_addr` carried in
//! that frame. These tests exercise the real mutually-authenticated transport
//! and assert that a rejected address claim changes none of the victim's
//! routing, session, actor-directory, admission, capability, or extension
//! projections.

use bytes::Bytes;
use icanact_remote::handshake::{Hello, PeerCapabilities};
use icanact_remote::registry::{GossipRegistry, RegistryMessage};
use icanact_remote::{
    BuilderTlsBootstrap, ClockProbeV1, GossipConfig, GossipExtensionsV1, GossipNodeId,
    GossipRegistryHandle, KeyPair, PeerId, RemoteActorLocation,
};
use std::collections::HashSet;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, sleep};

const VICTIM_ACTOR: &str = "ownership/victim/actor";
const POISON_ACTOR: &str = "ownership/attacker/poison";
const BARRIER_ACTOR: &str = "ownership/attacker/barrier";

#[derive(Clone, Copy)]
enum FullSyncKind {
    Request,
    Response,
}

fn reserve_free_port() -> u16 {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().expect("local_addr").port();
    drop(listener);
    port
}

fn loopback_addr() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], reserve_free_port()))
}

fn quiet_config() -> GossipConfig {
    GossipConfig {
        gossip_interval: Duration::from_secs(3_600),
        cleanup_interval: Duration::from_secs(3_600),
        peer_retry_interval: Duration::from_secs(3_600),
        peer_supervisor_interval: Duration::from_secs(3_600),
        immediate_propagation_enabled: false,
        enable_peer_discovery: false,
        ..Default::default()
    }
}

async fn start_node(
    addr: SocketAddr,
    keypair: &KeyPair,
) -> icanact_remote::Result<GossipRegistryHandle<BuilderTlsBootstrap>> {
    icanact_remote::tls::ensure_crypto_provider();
    GossipRegistryHandle::new_with_transport_stack(
        addr,
        keypair.to_secret_key(),
        Some(quiet_config()),
        BuilderTlsBootstrap,
    )
    .await
}

async fn wait_for_connection(node: &GossipRegistryHandle<BuilderTlsBootstrap>, peer: &PeerId) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if node.registry.has_connection_to_peer(peer).await {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for authenticated connection to {peer}"
        );
        sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_actor(registry: &Arc<GossipRegistry>, name: &str) -> RemoteActorLocation {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(location) = registry.lookup_actor(name).await {
            return location;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for actor {name}; the FIFO barrier was not processed"
        );
        sleep(Duration::from_millis(20)).await;
    }
}

fn send_registry_message(
    sender: &GossipRegistryHandle<BuilderTlsBootstrap>,
    recipient: &PeerId,
    message: RegistryMessage,
) {
    let payload =
        rkyv::to_bytes::<rkyv::rancor::Error>(&message).expect("serialize registry message");
    let payload = Bytes::from_owner(payload);
    let header = Bytes::copy_from_slice(&icanact_remote::framing::write_gossip_frame_prefix(
        payload.len(),
    ));
    sender
        .registry
        .connection_pool
        .send_to_peer_id_parts(recipient, header, payload)
        .expect("enqueue registry message on authenticated connection");
}

fn full_sync_message(
    kind: FullSyncKind,
    sender_peer_id: PeerId,
    sender_bind_addr: SocketAddr,
    actor_name: &str,
    actor_addr: SocketAddr,
    sequence: u64,
    extensions: Option<GossipExtensionsV1>,
) -> RegistryMessage {
    let local_actors = vec![(
        actor_name.to_string(),
        RemoteActorLocation::new_with_peer(actor_addr, sender_peer_id.clone()),
    )];
    match kind {
        FullSyncKind::Request => RegistryMessage::FullSync {
            local_actors,
            known_actors: Vec::new(),
            sender_peer_id,
            sender_bind_addr: Some(sender_bind_addr.to_string()),
            sequence,
            wall_clock_time: icanact_remote::current_timestamp(),
            extensions,
        },
        FullSyncKind::Response => RegistryMessage::FullSyncResponse {
            local_actors,
            known_actors: Vec::new(),
            sender_peer_id,
            sender_bind_addr: Some(sender_bind_addr.to_string()),
            sequence,
            wall_clock_time: icanact_remote::current_timestamp(),
            extensions,
        },
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProtectedProjection {
    addr_owner: Option<PeerId>,
    victim_route: Option<SocketAddr>,
    peer_node_id: Option<GossipNodeId>,
    peer_source: Option<SocketAddr>,
    peer_session_epoch: Option<u64>,
    peer_last_sequence: Option<u64>,
    peer_to_actors: Option<HashSet<String>>,
    victim_actor: Option<(PeerId, String)>,
    victim_admissions: Option<HashSet<String>>,
    capability_by_addr: Option<icanact_remote::handshake::PeerCapabilities>,
    capability_by_node: Option<icanact_remote::handshake::PeerCapabilities>,
    capability_addr_node: Option<GossipNodeId>,
    clock_snapshot: Option<icanact_remote::PeerClockSnapshot>,
}

async fn protected_projection(
    registry: &Arc<GossipRegistry>,
    victim_addr: SocketAddr,
    victim_peer_id: &PeerId,
) -> ProtectedProjection {
    let addr_owner = registry
        .connection_pool
        .addr_to_peer_id
        .read_sync(&victim_addr, |_, peer| peer.clone());
    let victim_route = registry
        .connection_pool
        .peer_id_to_addr
        .read_sync(victim_peer_id, |_, addr| *addr);
    let victim_actor = registry
        .lookup_actor(VICTIM_ACTOR)
        .await
        .map(|location| (location.peer_id, location.address));
    let victim_node_id = victim_peer_id.to_node_id();
    let capability_by_addr = registry
        .peer_capabilities
        .read_sync(&victim_addr, |_, caps| *caps);
    let capability_by_node = registry
        .peer_capabilities_by_node
        .read_sync(&victim_node_id, |_, caps| *caps);
    let capability_addr_node = registry
        .peer_capability_addr_to_node
        .read_sync(&victim_addr, |_, node| *node);
    let clock_snapshot = registry.peer_clock_snapshot(&victim_addr);

    let state = registry.gossip_state.lock().await;
    let peer = state.peers.get(&victim_addr);
    ProtectedProjection {
        addr_owner,
        victim_route,
        peer_node_id: peer.and_then(|info| info.node_id),
        peer_source: peer.and_then(|info| info.current_session_source),
        peer_session_epoch: peer.map(|info| info.current_session_epoch),
        peer_last_sequence: peer.map(|info| info.last_sequence),
        peer_to_actors: state.peer_to_actors.get(&victim_addr).cloned(),
        victim_actor,
        victim_admissions: state.actor_admissions_by_peer.get(victim_peer_id).cloned(),
        capability_by_addr,
        capability_by_node,
        capability_addr_node,
        clock_snapshot,
    }
}

async fn run_live_victim_claim(kind: FullSyncKind) -> icanact_remote::Result<()> {
    let observer_addr = loopback_addr();
    let victim_addr = loopback_addr();
    let attacker_addr = loopback_addr();
    let observer_key =
        KeyPair::new_for_testing(format!("ownership-observer-{}", observer_addr.port()));
    let victim_key = KeyPair::new_for_testing(format!("ownership-victim-{}", victim_addr.port()));
    let attacker_key =
        KeyPair::new_for_testing(format!("ownership-attacker-{}", attacker_addr.port()));

    let observer = start_node(observer_addr, &observer_key).await?;
    let victim = start_node(victim_addr, &victim_key).await?;
    let attacker = start_node(attacker_addr, &attacker_key).await?;

    victim
        .register(VICTIM_ACTOR.to_string(), victim_addr)
        .await?;
    // Operator configuration is independent evidence that the victim owns
    // this listening address. The victim's later inbound self-report may
    // refresh it, but cannot be what creates exclusive ownership.
    let _ = observer
        .registry
        .configure_peer(victim.registry.peer_id.clone(), victim_addr)
        .await;
    victim
        .add_peer(&observer.registry.peer_id)
        .await
        .connect(&observer_addr)
        .await?;
    attacker
        .add_peer(&observer.registry.peer_id)
        .await
        .connect(&observer_addr)
        .await?;
    wait_for_connection(&observer, &victim.registry.peer_id).await;
    wait_for_connection(&observer, &attacker.registry.peer_id).await;
    let _ = wait_for_actor(&observer.registry, VICTIM_ACTOR).await;

    // Capability publication is normally keyed first by the observed TCP
    // source. Publish the equivalent negotiated V5 capability under the
    // advertised victim address as an explicit clock-extension precondition;
    // the ownership test then proves a rejected claim cannot consume or
    // project extension state under that address.
    observer.registry.set_peer_capabilities(
        victim_addr,
        PeerCapabilities::from_hello_exchange(&Hello::new(), &Hello::new()),
    );
    // Drain any clock echo the connection's own bootstrap exchange legitimately
    // owes the victim before establishing the security-invariant baseline
    // below, so a later `Some` can only be attributed to the attack.
    //
    // This is NOT a race against an in-flight background computation: the
    // exchange that could populate `pending_clock_echoes` under `victim_addr`
    // is `record_inbound_gossip_extensions`, called by whichever of
    // `handle_incoming_message`'s `FullSync`/`FullSyncResponse` arms actually
    // processes the victim's side of this connection's bootstrap — and in
    // BOTH arms that call unconditionally precedes, in the same task and the
    // same lock, the actor-state merge that makes `VICTIM_ACTOR` visible.
    // `wait_for_actor` above therefore already proves — by program order, not
    // by inference from an absent poll result — that any legitimate probe the
    // victim sent has already been recorded, regardless of which side's
    // initial `FullSync` happened to reach the other first (that direction is
    // itself racy, but irrelevant here: either arm's recording step still
    // precedes the merge `wait_for_actor` waits on).
    //
    // What is NOT guaranteed by that ordering is whether anything has since
    // drained it: `gossip_extensions_for_outbound` only drains `pending_
    // clock_echoes` once clock calibration is recognized for the peer, so if
    // the recording above happened before this test's own `set_peer_
    // capabilities` call just above, nothing else will ever drain it — no
    // further legitimate traffic exists in this quiesced config. Draining it
    // explicitly here, once, discarding the result, is therefore this test's
    // own responsibility, not something to wait for. A concurrent drain by
    // this registry's own auto-reply computation (if the `FullSync` arm is
    // what fired) racing this exact call is harmless either way: `SccHashMap::
    // remove_sync` is atomic, so at most one of the two observes the pending
    // echo and whichever does not simply finds it already gone.
    let _ = observer
        .registry
        .gossip_extensions_for_outbound(victim_addr, icanact_remote::current_timestamp_nanos())
        .await;

    let before =
        protected_projection(&observer.registry, victim_addr, &victim.registry.peer_id).await;
    assert_eq!(
        before.addr_owner.as_ref(),
        Some(&victim.registry.peer_id),
        "precondition: the live victim owns its advertised address"
    );
    assert!(observer.registry.lookup_actor(POISON_ACTOR).await.is_none());
    assert!(before.clock_snapshot.is_none());
    let victim_connection_before = observer
        .registry
        .connection_pool
        .connections_by_addr
        .read_sync(&victim_addr, |_, connection| Arc::clone(connection))
        .expect("victim address must route to its live connection");
    let full_sync_count_before = observer.registry.get_stats().await.full_sync_exchanges;

    let forged_extensions = GossipExtensionsV1 {
        clock_probe: Some(ClockProbeV1 {
            sample_id: 0xCA11_AB1E,
            sender_wall_ns: icanact_remote::current_timestamp_nanos(),
        }),
        clock_echo: None,
    };
    let malicious = full_sync_message(
        kind,
        attacker.registry.peer_id.clone(),
        victim_addr,
        POISON_ACTOR,
        victim_addr,
        9_001,
        Some(forged_extensions),
    );
    send_registry_message(&attacker, &observer.registry.peer_id, malicious);

    // FIFO barrier on the same authenticated connection. Seeing this actor
    // proves the malicious frame before it was fully processed; absence is
    // therefore a rejection, not a timing assumption.
    let barrier = full_sync_message(
        kind,
        attacker.registry.peer_id.clone(),
        attacker_addr,
        BARRIER_ACTOR,
        attacker_addr,
        9_002,
        None,
    );
    send_registry_message(&attacker, &observer.registry.peer_id, barrier);
    let barrier_location = wait_for_actor(&observer.registry, BARRIER_ACTOR).await;
    assert_eq!(barrier_location.peer_id, attacker.registry.peer_id);

    let leaked_extension = observer
        .registry
        .gossip_extensions_for_outbound(victim_addr, icanact_remote::current_timestamp_nanos())
        .await
        .and_then(|extensions| extensions.clock_echo);
    assert!(
        leaked_extension.is_none(),
        "rejected claim must not project the attacker's clock extension under the victim address"
    );

    let after =
        protected_projection(&observer.registry, victim_addr, &victim.registry.peer_id).await;
    assert_eq!(
        after, before,
        "a rejected authenticated claim must mutate none of the victim's projections"
    );
    if let Some(poison) = observer.registry.lookup_actor(POISON_ACTOR).await {
        assert_eq!(poison.peer_id, attacker.registry.peer_id);
        assert_ne!(
            poison.address,
            victim_addr.to_string(),
            "the victim address must not survive verified-source actor repair"
        );
    }
    let victim_connection_after = observer
        .registry
        .connection_pool
        .connections_by_addr
        .read_sync(&victim_addr, |_, connection| Arc::clone(connection))
        .expect("victim address must retain its live connection");
    assert!(
        Arc::ptr_eq(&victim_connection_before, &victim_connection_after),
        "rejected claim must not replace the victim's connection route"
    );
    assert_eq!(
        observer.registry.get_stats().await.full_sync_exchanges,
        full_sync_count_before + 2,
        "both authenticated frames apply, rebound to the attacker's verified transport source"
    );

    attacker.shutdown().await;
    victim.shutdown().await;
    observer.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authenticated_full_sync_cannot_claim_live_victim_address() -> icanact_remote::Result<()> {
    run_live_victim_claim(FullSyncKind::Request).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authenticated_full_sync_response_cannot_claim_live_victim_address()
-> icanact_remote::Result<()> {
    run_live_victim_claim(FullSyncKind::Response).await
}

async fn run_local_address_claim(kind: FullSyncKind) -> icanact_remote::Result<()> {
    let observer_addr = loopback_addr();
    let attacker_addr = loopback_addr();
    let observer_key =
        KeyPair::new_for_testing(format!("ownership-local-observer-{}", observer_addr.port()));
    let attacker_key =
        KeyPair::new_for_testing(format!("ownership-local-attacker-{}", attacker_addr.port()));
    let observer = start_node(observer_addr, &observer_key).await?;
    let attacker = start_node(attacker_addr, &attacker_key).await?;

    attacker
        .add_peer(&observer.registry.peer_id)
        .await
        .connect(&observer_addr)
        .await?;
    wait_for_connection(&observer, &attacker.registry.peer_id).await;

    let local_addr = observer_addr;
    let owner_before = observer
        .registry
        .connection_pool
        .addr_to_peer_id
        .read_sync(&local_addr, |_, peer| peer.clone());
    let connection_before = observer
        .registry
        .connection_pool
        .connections_by_addr
        .read_sync(&local_addr, |_, connection| Arc::clone(connection));

    let malicious = full_sync_message(
        kind,
        attacker.registry.peer_id.clone(),
        local_addr,
        POISON_ACTOR,
        local_addr,
        10_001,
        None,
    );
    send_registry_message(&attacker, &observer.registry.peer_id, malicious);
    let barrier = full_sync_message(
        kind,
        attacker.registry.peer_id.clone(),
        attacker_addr,
        BARRIER_ACTOR,
        attacker_addr,
        10_002,
        None,
    );
    send_registry_message(&attacker, &observer.registry.peer_id, barrier);
    let _ = wait_for_actor(&observer.registry, BARRIER_ACTOR).await;

    assert!(observer.registry.lookup_actor(POISON_ACTOR).await.is_none());
    assert_eq!(
        observer
            .registry
            .connection_pool
            .addr_to_peer_id
            .read_sync(&local_addr, |_, peer| peer.clone()),
        owner_before,
        "a remote peer must never own this registry's local address"
    );
    let connection_after = observer
        .registry
        .connection_pool
        .connections_by_addr
        .read_sync(&local_addr, |_, connection| Arc::clone(connection));
    match (connection_before, connection_after) {
        (None, None) => {}
        (Some(before), Some(after)) => assert!(Arc::ptr_eq(&before, &after)),
        _ => panic!("a rejected local-address claim changed the local connection index"),
    }

    attacker.shutdown().await;
    observer.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authenticated_full_sync_variants_cannot_claim_local_address() -> icanact_remote::Result<()>
{
    run_local_address_claim(FullSyncKind::Request).await?;
    run_local_address_claim(FullSyncKind::Response).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn provisional_self_reports_never_publish_exclusive_aliases() -> icanact_remote::Result<()> {
    let observer_addr = loopback_addr();
    let first_addr = loopback_addr();
    let second_addr = loopback_addr();
    let claimed_addr = loopback_addr();
    let observer_key =
        KeyPair::new_for_testing(format!("ownership-race-observer-{}", observer_addr.port()));
    let first_key = KeyPair::new_for_testing(format!("ownership-race-first-{}", first_addr.port()));
    let second_key =
        KeyPair::new_for_testing(format!("ownership-race-second-{}", second_addr.port()));
    let observer = start_node(observer_addr, &observer_key).await?;
    let first = start_node(first_addr, &first_key).await?;
    let second = start_node(second_addr, &second_key).await?;

    first
        .add_peer(&observer.registry.peer_id)
        .await
        .connect(&observer_addr)
        .await?;
    second
        .add_peer(&observer.registry.peer_id)
        .await
        .connect(&observer_addr)
        .await?;
    wait_for_connection(&observer, &first.registry.peer_id).await;
    wait_for_connection(&observer, &second.registry.peer_id).await;

    let first_actor = "ownership/first/provisional";
    send_registry_message(
        &first,
        &observer.registry.peer_id,
        full_sync_message(
            FullSyncKind::Request,
            first.registry.peer_id.clone(),
            claimed_addr,
            first_actor,
            claimed_addr,
            11_001,
            None,
        ),
    );
    // First peer's actual-address message is a FIFO barrier.
    send_registry_message(
        &first,
        &observer.registry.peer_id,
        full_sync_message(
            FullSyncKind::Request,
            first.registry.peer_id.clone(),
            first_addr,
            "ownership/first/barrier",
            first_addr,
            11_002,
            None,
        ),
    );
    let _ = wait_for_actor(&observer.registry, "ownership/first/barrier").await;
    assert_eq!(
        observer
            .registry
            .connection_pool
            .addr_to_peer_id
            .read_sync(&claimed_addr, |_, peer| peer.clone()),
        None,
        "a first provisional self-report must not own a previously-unowned alias"
    );

    send_registry_message(
        &second,
        &observer.registry.peer_id,
        full_sync_message(
            FullSyncKind::Response,
            second.registry.peer_id.clone(),
            claimed_addr,
            POISON_ACTOR,
            claimed_addr,
            12_001,
            None,
        ),
    );
    send_registry_message(
        &second,
        &observer.registry.peer_id,
        full_sync_message(
            FullSyncKind::Response,
            second.registry.peer_id.clone(),
            second_addr,
            BARRIER_ACTOR,
            second_addr,
            12_002,
            None,
        ),
    );
    let _ = wait_for_actor(&observer.registry, BARRIER_ACTOR).await;

    assert_eq!(
        observer
            .registry
            .connection_pool
            .addr_to_peer_id
            .read_sync(&claimed_addr, |_, peer| peer.clone()),
        None,
        "no provisional claimant may publish the alias"
    );
    // The FIFO barrier is itself a complete snapshot and may prune the prior
    // actor, so actor presence is intentionally not used as evidence here.
    // The address route is the durable security boundary under test.

    second.shutdown().await;
    first.shutdown().await;
    observer.shutdown().await;
    Ok(())
}

/// A normal unconfigured inbound peer's advertised listening address differs
/// from its raw TCP source port. That self-report must remain non-exclusive;
/// identity and actor state bind to the authenticated transport source.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nat_inbound_full_sync_binds_identity_to_observed_transport() -> icanact_remote::Result<()>
{
    let observer_addr = loopback_addr();
    let sender_addr = loopback_addr();
    let observer_key =
        KeyPair::new_for_testing(format!("ownership-nat-observer-{}", observer_addr.port()));
    let sender_key =
        KeyPair::new_for_testing(format!("ownership-nat-sender-{}", sender_addr.port()));
    let observer = start_node(observer_addr, &observer_key).await?;
    let sender = start_node(sender_addr, &sender_key).await?;

    sender
        .add_peer(&observer.registry.peer_id)
        .await
        .connect(&observer_addr)
        .await?;
    wait_for_connection(&observer, &sender.registry.peer_id).await;

    let observed_addr = {
        let state = observer.registry.gossip_state.lock().await;
        assert!(
            state
                .peers
                .get(&sender_addr)
                .and_then(|peer| peer.node_id)
                .is_none(),
            "an unconfigured advertised address is only a self-report"
        );
        state
            .peers
            .iter()
            .find_map(|(addr, peer)| {
                (peer.node_id == Some(sender.registry.peer_id.to_node_id())).then_some(*addr)
            })
            .expect("authenticated transport source must carry the peer identity")
    };
    assert_ne!(observed_addr, sender_addr);

    let actor = "ownership/nat/identity-barrier";
    send_registry_message(
        &sender,
        &observer.registry.peer_id,
        full_sync_message(
            FullSyncKind::Request,
            sender.registry.peer_id.clone(),
            sender_addr,
            actor,
            sender_addr,
            13_001,
            None,
        ),
    );
    let _ = wait_for_actor(&observer.registry, actor).await;

    let state = observer.registry.gossip_state.lock().await;
    assert!(
        state
            .peers
            .get(&sender_addr)
            .and_then(|peer| peer.node_id)
            .is_none(),
        "FullSync must not promote its advertised self-report"
    );
    let observed = state
        .peers
        .get(&observed_addr)
        .expect("observed transport peer entry must survive FullSync");
    assert_eq!(
        observed.node_id,
        Some(sender.registry.peer_id.to_node_id()),
        "FullSync must retain identity at the authenticated transport source"
    );
    drop(state);
    let location = observer
        .registry
        .lookup_actor(actor)
        .await
        .expect("FullSync actor must be applied");
    assert_eq!(location.peer_id, sender.registry.peer_id);
    assert_eq!(
        location.address,
        sender_addr.to_string(),
        "owned actor repair must preserve the advertised service port while anchoring the verified IP"
    );

    sender.shutdown().await;
    observer.shutdown().await;
    Ok(())
}
