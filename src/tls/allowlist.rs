//! Runtime-swappable inbound peer allowlist.

use crate::PeerId;
use arc_swap::ArcSwap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Set of authenticated [`PeerId`]s allowed to complete an inbound TLS
/// handshake. Cloning shares the same underlying set, so a daemon can keep a
/// clone and [`replace`](Self::replace) the whole set at runtime.
///
/// REMOTE-3: membership is evaluated against the authenticated Ed25519
/// identity only, never an address.
#[derive(Clone, Default)]
pub struct PeerAllowlist {
    inner: Arc<ArcSwap<HashSet<PeerId>>>,
}

impl PeerAllowlist {
    pub fn new(peers: impl IntoIterator<Item = PeerId>) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(peers.into_iter().collect())),
        }
    }

    /// Atomically replace the entire allowed set. In-flight handshakes see
    /// either the old or the new set, never a mixture.
    pub fn replace(&self, peers: impl IntoIterator<Item = PeerId>) {
        self.inner.store(Arc::new(peers.into_iter().collect()));
    }

    pub fn contains(&self, peer: &PeerId) -> bool {
        self.inner.load().contains(peer)
    }

    pub fn len(&self) -> usize {
        self.inner.load().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.load().is_empty()
    }
}

impl std::fmt::Debug for PeerAllowlist {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerAllowlist")
            .field("len", &self.len())
            .finish()
    }
}

/// Typed reason a client handshake was rejected by the allowlist.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("peer {peer_id} is not on the inbound peer allowlist")]
pub struct PeerNotAllowed {
    pub peer_id: PeerId,
}

/// Recover the typed allowlist rejection from the `io::Error` returned by a
/// failed server-side `TlsAcceptor::accept`.
pub fn peer_not_allowed(err: &std::io::Error) -> Option<&PeerNotAllowed> {
    let rustls::Error::InvalidCertificate(rustls::CertificateError::Other(other)) =
        err.get_ref()?.downcast_ref::<rustls::Error>()?
    else {
        return None;
    };
    other.0.downcast_ref::<PeerNotAllowed>()
}

/// Minimum spacing between rejection log lines for one identity.
const REJECT_LOG_INTERVAL: Duration = Duration::from_secs(60);
/// Upper bound on distinct identities tracked for log dedupe (REMOTE-5).
const REJECT_LOG_CAPACITY: usize = 1024;

/// Bounded per-identity dedupe so repeated attempts cannot storm the log.
#[derive(Default)]
pub(super) struct RejectLogLimiter {
    last_logged: scc::HashMap<PeerId, Instant>,
}

impl RejectLogLimiter {
    /// True when a log line for `peer` is due now (and records that it was).
    pub(super) fn should_log(&self, peer: &PeerId) -> bool {
        let now = Instant::now();
        if self.last_logged.len() >= REJECT_LOG_CAPACITY && !self.last_logged.contains_sync(peer) {
            self.last_logged.clear_sync();
        }
        let mut due = false;
        self.last_logged
            .entry_sync(peer.clone())
            .and_modify(|last| {
                if now.duration_since(*last) >= REJECT_LOG_INTERVAL {
                    *last = now;
                    due = true;
                }
            })
            .or_insert_with(|| {
                due = true;
                now
            });
        due
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reject_log_is_deduped_per_identity_and_bounded() {
        let limiter = RejectLogLimiter::default();
        let a = PeerId::from_public_key(&crate::SecretKey::generate().public());
        let b = PeerId::from_public_key(&crate::SecretKey::generate().public());
        assert!(limiter.should_log(&a), "first rejection logs");
        assert!(
            !limiter.should_log(&a),
            "repeat within the window is silent"
        );
        assert!(limiter.should_log(&b), "other identities still log once");
        for _ in 0..(REJECT_LOG_CAPACITY * 2) {
            let p = PeerId::from_public_key(&crate::SecretKey::generate().public());
            assert!(limiter.should_log(&p));
        }
        assert!(limiter.last_logged.len() <= REJECT_LOG_CAPACITY);
    }
}
