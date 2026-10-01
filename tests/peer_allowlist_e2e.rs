//! Inbound peer allowlist enforced in the TLS client-certificate verifier.
//!
//! REMOTE-3: only the authenticated identity is consulted, never an address.

use icanact_remote::tls::TlsConfig;
use icanact_remote::tls::allowlist::{PeerAllowlist, peer_not_allowed};
use icanact_remote::{PeerId, SecretKey};
use rustls::pki_types::ServerName;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn peer_id(key: &SecretKey) -> PeerId {
    PeerId::from_public_key(&key.public())
}

/// Outcome of one real loopback mTLS handshake, from both ends.
struct Handshake {
    server: std::io::Result<()>,
    server_err_peer: Option<PeerId>,
    client_saw_failure: bool,
}

async fn handshake(server: &TlsConfig, client: &TlsConfig) -> Handshake {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let acceptor = server.acceptor();
    let connector = client.connector();
    let server_name = ServerName::try_from("peer.icanact.invalid").unwrap();

    let server_task = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        match acceptor.accept(sock).await {
            Ok(mut tls) => {
                let mut b = [0u8; 1];
                let _ = tls.read_exact(&mut b).await;
                let _ = tls.write_all(&b).await;
                let _ = tls.flush().await;
                (Ok(()), None)
            }
            Err(e) => {
                let typed = peer_not_allowed(&e).map(|r| r.peer_id.clone());
                (Err(e), typed)
            }
        }
    });

    let sock = TcpStream::connect(addr).await.unwrap();
    // TLS 1.3: the client finishes its side before the server has judged the
    // client certificate, so rejection surfaces on the first read/write.
    let client_saw_failure = match connector.connect(server_name, sock).await {
        Err(_) => true,
        Ok(mut tls) => {
            let _ = tls.write_all(&[7]).await;
            let _ = tls.flush().await;
            let mut b = [0u8; 1];
            !matches!(
                tokio::time::timeout(Duration::from_secs(5), tls.read_exact(&mut b)).await,
                Ok(Ok(_))
            )
        }
    };
    let (server, server_err_peer) = server_task.await.unwrap();
    Handshake {
        server: server.map(|_| ()),
        server_err_peer,
        client_saw_failure,
    }
}

fn server_cfg(key: &SecretKey, allow: Option<PeerAllowlist>) -> TlsConfig {
    TlsConfig::with_options(key.clone(), false, allow).unwrap()
}

#[tokio::test]
async fn allowlisted_client_completes_handshake() {
    let server_key = SecretKey::generate();
    let client_key = SecretKey::generate();
    let allow = PeerAllowlist::new([peer_id(&client_key)]);
    let h = handshake(
        &server_cfg(&server_key, Some(allow)),
        &TlsConfig::new(client_key).unwrap(),
    )
    .await;
    assert!(
        h.server.is_ok(),
        "allowlisted client must connect: {:?}",
        h.server
    );
    assert!(!h.client_saw_failure);
}

#[tokio::test]
async fn non_allowlisted_client_with_valid_key_is_rejected_with_typed_error() {
    let server_key = SecretKey::generate();
    let client_key = SecretKey::generate();
    let other = SecretKey::generate();
    let allow = PeerAllowlist::new([peer_id(&other)]);
    let h = handshake(
        &server_cfg(&server_key, Some(allow)),
        &TlsConfig::new(client_key.clone()).unwrap(),
    )
    .await;
    assert!(h.server.is_err(), "non-allowlisted client must be rejected");
    assert_eq!(
        h.server_err_peer,
        Some(peer_id(&client_key)),
        "rejection must carry the typed PeerNotAllowed identity"
    );
    assert!(h.client_saw_failure, "client must observe the rejection");
}

#[tokio::test]
async fn runtime_allowlist_swap_applies_to_new_handshakes() {
    let server_key = SecretKey::generate();
    let client_key = SecretKey::generate();
    let allow = PeerAllowlist::new([]);
    let server = server_cfg(&server_key, Some(allow.clone()));
    let client = TlsConfig::new(client_key.clone()).unwrap();

    let before = handshake(&server, &client).await;
    assert!(before.server.is_err(), "empty allowlist rejects everyone");

    allow.replace([peer_id(&client_key)]);
    let granted = handshake(&server, &client).await;
    assert!(
        granted.server.is_ok(),
        "swap must admit the client: {:?}",
        granted.server
    );

    allow.replace([]);
    let revoked = handshake(&server, &client).await;
    assert!(revoked.server.is_err(), "swap must revoke the client");
}

#[tokio::test]
async fn no_allowlist_keeps_default_open_behaviour() {
    let h = handshake(
        &server_cfg(&SecretKey::generate(), None),
        &TlsConfig::new(SecretKey::generate()).unwrap(),
    )
    .await;
    assert!(h.server.is_ok());
}

mod registry_level {
    use super::*;
    use icanact_remote::{BuilderTlsBootstrap, GossipConfig, GossipRegistryHandle};
    use std::time::Instant;

    fn cfg(allow: Option<PeerAllowlist>) -> GossipConfig {
        GossipConfig {
            connection_timeout: Duration::from_secs(2),
            inbound_peer_allowlist: allow,
            ..Default::default()
        }
    }

    async fn start(key: SecretKey, allow: Option<PeerAllowlist>) -> GossipRegistryHandle {
        GossipRegistryHandle::new_with_transport_stack(
            "127.0.0.1:0".parse().unwrap(),
            key,
            Some(cfg(allow)),
            BuilderTlsBootstrap,
        )
        .await
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn server_registry_enforces_allowlist_and_runtime_swap() {
        let server_key = SecretKey::generate();
        let good_key = SecretKey::generate();
        let bad_key = SecretKey::generate();
        let allow = PeerAllowlist::new([peer_id(&good_key)]);
        let server = start(server_key.clone(), Some(allow.clone())).await;
        let server_id = server.registry.peer_id.clone();

        let bad = start(bad_key.clone(), None).await;
        let bad_peer = bad.add_peer(&server_id).await;
        let rejected = bad_peer.connect(&server.registry.bind_addr).await;
        // The rejection can surface at connect (hello read fails) or just after.
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline && server.stats().await.active_peers == 0 {
            tokio::time::sleep(Duration::from_millis(25)).await;
            if rejected.is_err() {
                break;
            }
        }
        assert_eq!(
            server.stats().await.active_peers,
            0,
            "non-allowlisted peer must never become an active peer"
        );

        let good = start(good_key, None).await;
        let good_peer = good.add_peer(&server_id).await;
        good_peer
            .connect(&server.registry.bind_addr)
            .await
            .expect("allowlisted client connects");
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline && server.stats().await.active_peers == 0 {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(server.stats().await.active_peers, 1);

        // Swap: admit the previously rejected identity for NEW handshakes.
        allow.replace([peer_id(&bad_key)]);
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            let retry = bad.add_peer(&server_id).await;
            match retry.connect(&server.registry.bind_addr).await {
                Ok(_) => break,
                // The pool's per-peer dial retry floor spaces attempts out.
                Err(e) if Instant::now() < deadline => {
                    let _ = e;
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Err(e) => panic!("swapped-in client never connected: {e}"),
            }
        }
    }
}
