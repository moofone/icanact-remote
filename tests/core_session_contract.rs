#![cfg(feature = "test-helpers")]
mod common;
use common::{DynError, TlsHandle, connect_bidirectional, create_ordered_tls_pair};
use icanact_remote::{AuthenticatedSessionReceipt, RemoteConnection, SessionMutationOutcome};
use std::time::Duration;

async fn observed(source: &TlsHandle, peer: &TlsHandle) -> RemoteConnection {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(connection) = source
                .client()
                .lookup_connected_peer(&peer.registry.peer_id)
                .and_then(|remote| remote.connection_ref())
                .filter(|connection| !connection.is_closed())
            {
                return connection;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual source physical session, not just aggregate active-peer stats")
}

fn retire_observed(
    source: &TlsHandle,
    receipt: &AuthenticatedSessionReceipt,
) -> SessionMutationOutcome {
    source.client().disconnect_session(receipt)
}

async fn reconnect(
    source: &TlsHandle,
    peer: &TlsHandle,
    old: &RemoteConnection,
) -> Result<RemoteConnection, DynError> {
    // Explicit operator fixture shutdown is the sole peer-wide exception.
    source.disconnect_peer_connection(&peer.registry.peer_id);
    peer.disconnect_peer_connection(&source.registry.peer_id);
    tokio::time::timeout(Duration::from_secs(5), async {
        while !old.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    connect_bidirectional(source, peer).await?;
    Ok(observed(source, peer).await)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_bootstrap_cleanup_cannot_disconnect_replacement_session() -> Result<(), DynError> {
    let (source, peer) = create_ordered_tls_pair("receipt-source", "receipt-peer").await?;
    connect_bidirectional(&source, &peer).await?;
    let old = observed(&source, &peer).await;
    let receipt = source.client().capture_session(&old)?;
    assert_eq!(receipt.peer_id(), &peer.registry.peer_id);
    let old_instance = source
        .client()
        .current_peer_connection_instance(&peer.registry.peer_id)
        .unwrap();
    let new = reconnect(&source, &peer, &old).await?;
    let new_instance = source
        .client()
        .current_peer_connection_instance(&peer.registry.peer_id)
        .unwrap();
    assert_ne!(
        old_instance, new_instance,
        "positive control: real physical replacement occurred"
    );
    assert!(!new.is_closed());
    assert_eq!(
        retire_observed(&source, &receipt),
        SessionMutationOutcome::Stale
    );
    let successor_survived = !new.is_closed();
    let selected = source
        .client()
        .current_peer_connection_instance(&peer.registry.peer_id);
    source.shutdown_and_wait().await;
    peer.shutdown_and_wait().await;
    assert!(
        successor_survived,
        "old cleanup must not close a successor physical session"
    );
    assert_eq!(selected, Some(new_instance));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_bootstrap_dns_commit_cannot_overwrite_successor_hint() -> Result<(), DynError> {
    let (source, peer) = create_ordered_tls_pair("dns-receipt-source", "dns-receipt-peer").await?;
    connect_bidirectional(&source, &peer).await?;
    let old = observed(&source, &peer).await;
    let old_receipt = source.client().capture_session(&old)?;
    let new = reconnect(&source, &peer, &old).await?;
    let address = peer.registry.bind_addr;
    let new_receipt = source.client().capture_session(&new)?;
    assert_eq!(
        source
            .client()
            .set_peer_dns_for_session(&new_receipt, address, "successor.example:9001".into())
            .await,
        SessionMutationOutcome::Applied,
        "positive control: current exact-session DNS commits"
    );
    assert_eq!(
        source
            .client()
            .set_peer_dns_for_session(&old_receipt, address, "obsolete.example:9000".into())
            .await,
        SessionMutationOutcome::Stale
    );
    let dns = source
        .registry
        .gossip_state
        .lock()
        .await
        .peers
        .get(&address)
        .unwrap()
        .dns_name
        .clone();
    assert!(!new.is_closed());
    source.shutdown_and_wait().await;
    peer.shutdown_and_wait().await;
    assert_eq!(
        dns.as_deref(),
        Some("successor.example:9001"),
        "old DNS work cannot commit into the successor's state"
    );
    Ok(())
}
