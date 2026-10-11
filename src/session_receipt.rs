//! Authenticated physical-session control evidence; never liveness or application authority.
use crate::connection_pool::{ConnectionPool, LockFreeConnection};
use std::net::SocketAddr;
use std::sync::{
    Arc, Weak,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::Notify;

/// Opaque evidence of one certificate-authenticated physical stream.
/// Clones retain only weak connection/pool identities and small closure-signal
/// state, not queued payloads or IO tasks. Dropping a receipt does not disconnect.
/// `closed` means transport admission is closed, not that IO retirement completed.
#[derive(Clone)]
pub struct AuthenticatedSessionReceipt {
    pub(crate) pool: Weak<ConnectionPool>,
    pub(crate) connection: Weak<LockFreeConnection>,
    peer: crate::PeerId,
    address: SocketAddr,
    exited: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl std::fmt::Debug for AuthenticatedSessionReceipt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthenticatedSessionReceipt")
            .field("peer", &self.peer)
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

/// Failure to capture exact authenticated evidence; never guess from an address map.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionCaptureError {
    /// The indexed physical connection no longer matches the observed handle.
    Stale,
    /// This physical connection has no certificate-owned identity proof.
    Unauthenticated,
    /// No usable physical stream is currently indexed.
    Unavailable,
}
impl std::fmt::Display for SessionCaptureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cannot capture authenticated physical session: {self:?}")
    }
}
impl std::error::Error for SessionCaptureError {}

/// Compare-and-apply outcome. Applied teardown requests retirement; it is not
/// proof that all stream tasks, externally held handles, or storage terminated.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionMutationOutcome {
    /// Only the observed session/record was changed.
    Applied,
    /// The observation is retired, replaced, or no longer owns this record.
    Stale,
    /// The receipt was minted by another connection-pool owner.
    ForeignOwner,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cached_or_embedded_identity_is_not_certificate_proof() {
        let pool = Arc::new(ConnectionPool::new(4, std::time::Duration::from_secs(1)));
        let peer = crate::KeyPair::new_for_testing("unauthenticated-cache-claim").peer_id();
        let address = "127.0.0.1:45003".parse().unwrap();
        let mut target = LockFreeConnection::new(
            address,
            crate::connection_pool::ConnectionDirection::Outbound,
        );
        target.embedded_peer_id = Some(peer.clone());
        pool.addr_to_peer_id.insert_sync(address, peer).unwrap();
        let target = Arc::new(target);
        assert!(matches!(
            AuthenticatedSessionReceipt::capture(&pool, &target),
            Err(SessionCaptureError::Unauthenticated)
        ));
    }
}

impl AuthenticatedSessionReceipt {
    pub(crate) fn capture(
        pool: &Arc<ConnectionPool>,
        target: &Arc<LockFreeConnection>,
    ) -> Result<Self, SessionCaptureError> {
        let peer = target
            .authenticated_peer_id
            .clone()
            .ok_or(SessionCaptureError::Unauthenticated)?;
        let handle = target
            .stream_handle
            .as_ref()
            .ok_or(SessionCaptureError::Unavailable)?;
        if !target.has_live_stream() {
            return Err(SessionCaptureError::Unavailable);
        }
        let (exited, notify) = handle.closed_signal();
        Ok(Self {
            pool: Arc::downgrade(pool),
            connection: Arc::downgrade(target),
            peer,
            address: target.addr,
            exited,
            notify,
        })
    }
    /// Certificate identity of the actual stream, not an address-index claim.
    pub fn peer_id(&self) -> &crate::PeerId {
        &self.peer
    }
    /// Observed physical endpoint; informational, never teardown authority.
    pub fn address(&self) -> SocketAddr {
        self.address
    }
    /// The IO owner closed admission on this exact stream.
    pub fn is_closed(&self) -> bool {
        self.exited.load(Ordering::Acquire)
    }
    /// Await this exact stream's terminal admission signal. Subscribe before
    /// checking, including an already-closed stream; no peer callbacks or polls.
    /// Caller owns cancellation/deadline and bounded waiter admission.
    pub async fn closed(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_closed() {
                return;
            }
            notified.await;
        }
    }
    pub(crate) fn belongs_to(&self, pool: &Arc<ConnectionPool>) -> bool {
        self.pool.as_ptr() == Arc::as_ptr(pool)
    }
}
