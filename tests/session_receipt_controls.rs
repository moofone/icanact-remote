#![cfg(feature = "test-helpers")]
mod common;
use common::{DynError, connect_bidirectional, create_ordered_tls_pair};
use icanact_remote::{SessionCaptureError, SessionMutationOutcome};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn current_foreign_address_and_terminal_receipt_controls() -> Result<(), DynError> {
    let (source, peer) =
        create_ordered_tls_pair("scope-controls-source", "scope-controls-peer").await?;
    connect_bidirectional(&source, &peer).await?;
    let connection = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(c) = source
                .client()
                .lookup_connected_connection(&peer.registry.peer_id)
                .filter(|c| !c.is_closed())
            {
                break c;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let receipt = source.client().capture_session(&connection)?;
    assert_eq!(receipt.peer_id(), &peer.registry.peer_id);
    assert!(!receipt.is_closed());
    // Public address hints are mutable; they cannot widen the captured physical scope.
    let mut altered = connection.clone();
    altered.addr = source.registry.bind_addr;
    assert_eq!(
        source.client().capture_session(&altered)?.peer_id(),
        &peer.registry.peer_id
    );
    assert_eq!(
        peer.client().disconnect_session(&receipt),
        SessionMutationOutcome::ForeignOwner
    );
    assert_eq!(
        peer.client()
            .set_peer_dns_for_session(
                &receipt,
                source.registry.bind_addr,
                "foreign.example:9".into()
            )
            .await,
        SessionMutationOutcome::ForeignOwner
    );
    assert!(!connection.is_closed());
    assert!(matches!(
        source
            .client()
            .capture_session_at_address("127.0.0.1:1".parse()?),
        Err(SessionCaptureError::Unavailable)
    ));
    assert_eq!(
        source
            .client()
            .set_peer_dns_for_session(
                &receipt,
                source.registry.bind_addr,
                "wrong-address.example:9".into()
            )
            .await,
        SessionMutationOutcome::Stale
    );
    let after_wrong_address = !connection.is_closed();
    // Cancellation of a borrowed closure wait cannot terminate or mutate the session.
    assert!(
        tokio::time::timeout(Duration::from_millis(10), receipt.closed())
            .await
            .is_err()
    );
    assert!(!receipt.is_closed());
    // Register before teardown and verify both pending and already-terminal waits.
    let pending = tokio::spawn({
        let r = receipt.clone();
        async move {
            r.closed().await;
        }
    });
    tokio::task::yield_now().await;
    assert_eq!(
        source.client().disconnect_session(&receipt),
        SessionMutationOutcome::Applied
    );
    tokio::time::timeout(Duration::from_secs(5), pending).await??;
    tokio::time::timeout(Duration::from_secs(1), receipt.closed()).await?;
    assert!(receipt.is_closed());
    assert!(connection.is_closed());
    assert!(after_wrong_address);
    source.shutdown_and_wait().await;
    peer.shutdown_and_wait().await;
    Ok(())
}
