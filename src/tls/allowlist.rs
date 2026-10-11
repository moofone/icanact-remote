//! Runtime-swappable inbound peer allowlist.

use crate::PeerId;
use arc_swap::ArcSwap;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
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
/// At capacity, new identities are silent until an existing 60-second window
/// expires. Rejection itself remains unconditional; this bounds diagnostics
/// without allowing identity churn to erase recent suppression evidence.
#[derive(Default)]
pub(super) struct RejectLogLimiter {
    last_logged: Mutex<std::collections::HashMap<PeerId, Instant>>,
}

impl RejectLogLimiter {
    /// True when a log line for `peer` is due now (and records that it was).
    pub(super) fn should_log(&self, peer: &PeerId) -> bool {
        self.should_log_with_admission_observer(peer, || {})
    }

    fn should_log_with_admission_observer(&self, peer: &PeerId, observe: impl FnOnce()) -> bool {
        // REMOTE-5: admission and expiry are one critical section. This
        // runs only on rejected TLS handshakes, never on an established
        // messaging path. Do not erase recent windows under identity churn.
        let mut last_logged = self
            .last_logged
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let now = Instant::now();
        if last_logged.len() >= REJECT_LOG_CAPACITY && !last_logged.contains_key(peer) {
            last_logged.retain(|_, last| now.duration_since(*last) < REJECT_LOG_INTERVAL);
            if last_logged.len() >= REJECT_LOG_CAPACITY {
                return false;
            }
        }
        // This no-op in production provides a deterministic admission seam.
        observe();
        let mut due = false;
        last_logged
            .entry(peer.clone())
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
            let _ = limiter.should_log(&p);
            assert!(limiter.last_logged.lock().unwrap().len() <= REJECT_LOG_CAPACITY);
        }
        assert!(!limiter.should_log(&a), "churn retains recent suppression");
        assert!(!limiter.should_log(&b));
        assert_eq!(
            limiter.last_logged.lock().unwrap().len(),
            REJECT_LOG_CAPACITY
        );
    }

    /// REMOTE-5: identity churn cannot reset a hot peer's suppression window.
    #[test]
    fn qa_pool_007_full_log_table_preserves_recent_suppression_and_reclaims_expiry() {
        let limiter = RejectLogLimiter::default();
        let hot = crate::KeyPair::new_for_testing("qa-reject-hot").peer_id();
        assert!(limiter.should_log(&hot));
        let mut expired = None;
        for index in 1..REJECT_LOG_CAPACITY {
            let peer = crate::KeyPair::new_for_testing(format!("qa-reject-fill-{index}")).peer_id();
            assert!(limiter.should_log(&peer));
            expired = Some(peer);
        }
        let newcomer = crate::KeyPair::new_for_testing("qa-reject-newcomer").peer_id();
        assert!(
            !limiter.should_log(&newcomer),
            "full recent table rejects new diagnostics"
        );
        assert!(
            !limiter.should_log(&hot),
            "churn must not erase hot-peer evidence"
        );
        assert_eq!(
            limiter.last_logged.lock().unwrap().len(),
            REJECT_LOG_CAPACITY
        );
        let expired = expired.unwrap();
        limiter.last_logged.lock().unwrap().insert(
            expired,
            Instant::now() - REJECT_LOG_INTERVAL - Duration::from_secs(1),
        );
        assert!(
            limiter.should_log(&newcomer),
            "expired capacity admits a new identity"
        );
        assert!(!limiter.should_log(&newcomer));
        assert!(!limiter.should_log(&hot));
        assert_eq!(
            limiter.last_logged.lock().unwrap().len(),
            REJECT_LOG_CAPACITY
        );
    }

    #[test]
    fn admission_holds_exclusive_guard_across_check_and_insert() {
        let limiter = RejectLogLimiter::default();
        let first = crate::KeyPair::new_for_testing("qa-reject-first").peer_id();
        let second = crate::KeyPair::new_for_testing("qa-reject-second").peer_id();
        assert!(limiter.should_log(&first));
        assert!(limiter.should_log_with_admission_observer(&second, || {
            assert!(matches!(
                limiter.last_logged.try_lock(),
                Err(std::sync::TryLockError::WouldBlock)
            ));
        }));
        let entries = limiter.last_logged.lock().unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries.contains_key(&first));
        assert!(entries.contains_key(&second));
        drop(entries);
        assert!(!limiter.should_log(&second));
    }

    #[test]
    fn rejection_admission_is_bounded_under_concurrent_identity_churn() {
        let limiter = RejectLogLimiter::default();
        let start = std::sync::Barrier::new(8);
        let peer = crate::KeyPair::new_for_testing("qa-reject-control").peer_id();
        assert!(limiter.should_log(&peer));
        std::thread::scope(|scope| {
            for worker in 0..8 {
                let limiter = &limiter;
                let start = &start;
                scope.spawn(move || {
                    start.wait();
                    for index in 0..REJECT_LOG_CAPACITY {
                        let key = format!("qa-reject-{worker}-{index}");
                        let peer = crate::KeyPair::new_for_testing(&key).peer_id();
                        let _ = limiter.should_log(&peer);
                        assert!(limiter.last_logged.lock().unwrap().len() <= REJECT_LOG_CAPACITY);
                    }
                });
            }
        });
        assert!(
            !limiter.should_log(&peer),
            "recent identity survives concurrent churn"
        );
        assert_eq!(
            limiter.last_logged.lock().unwrap().len(),
            REJECT_LOG_CAPACITY
        );
    }
}
