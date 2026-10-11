//! Identity-keyed record of peers that declared themselves client-only.
//!
//! REMOTE-3: keyed by the authenticated `PeerId` and fed only from the
//! authenticated Hello, never from an address or a third party's claim.
//! REMOTE-4: each declaration carries an opaque session token so cleanup that
//! outlives a session cannot touch a successor session's state.
//! REMOTE-5: the table is bounded.

use crate::PeerId;
use scc::HashMap as SccHashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

/// Upper bound on remembered client-only identities.
const MAX_CLIENT_ONLY_PEERS: usize = 4096;
/// Retain ended declarations for five minutes. Live streams never expire.
const DISCONNECTED_ROLE_TTL: Duration = Duration::from_secs(300);

struct DeclaredRole {
    token: u64,
    connection: Option<Weak<crate::connection_pool::LockFreeConnection>>,
    ended_at: Mutex<Option<Instant>>,
}

impl DeclaredRole {
    fn expired(&self) -> bool {
        let mut ended = self
            .ended_at
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if ended.is_none()
            && self.connection.as_ref().is_some_and(|connection| {
                connection
                    .upgrade()
                    .is_none_or(|connection| !connection.has_live_stream())
            })
        {
            *ended = Some(Instant::now());
        }
        ended.is_some_and(|ended| ended.elapsed() >= DISCONNECTED_ROLE_TTL)
    }
}

/// Opaque receipt for one client-only declaration (one authenticated session).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ClientOnlySession(u64);

#[derive(Default)]
pub(crate) struct ClientOnlyPeers {
    sessions: SccHashMap<PeerId, DeclaredRole>,
    next_token: AtomicU64,
    admission: Mutex<()>,
}

impl ClientOnlyPeers {
    pub(crate) fn contains(&self, peer: &PeerId) -> bool {
        self.sessions.contains_sync(peer)
    }

    /// Record the role the peer declared in its latest authenticated Hello.
    /// A client-only declaration returns the receipt for that session; a
    /// regular declaration forgets any earlier client-only record.
    #[cfg(test)]
    pub(crate) fn note_declared_role(
        &self,
        peer: &PeerId,
        client_only: bool,
    ) -> crate::Result<Option<ClientOnlySession>> {
        self.note_declared_role_inner(peer, client_only, None, || {})
    }

    pub(crate) fn note_declared_role_for_connection(
        &self,
        peer: &PeerId,
        client_only: bool,
        connection: &Arc<crate::connection_pool::LockFreeConnection>,
    ) -> crate::Result<Option<ClientOnlySession>> {
        self.note_declared_role_inner(peer, client_only, Some(Arc::downgrade(connection)), || {})
    }

    pub(crate) fn mark_ended(&self, peer: &PeerId, session: ClientOnlySession) {
        let _admission = self
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.sessions.read_sync(peer, |_, role| {
            if role.token == session.0 {
                let mut ended = role
                    .ended_at
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                ended.get_or_insert_with(Instant::now);
            }
        });
    }

    #[cfg(test)]
    fn note_declared_role_with_observer(
        &self,
        peer: &PeerId,
        client_only: bool,
        after_capacity_check: impl FnOnce(),
    ) -> crate::Result<Option<ClientOnlySession>> {
        self.note_declared_role_inner(peer, client_only, None, after_capacity_check)
    }

    fn note_declared_role_inner(
        &self,
        peer: &PeerId,
        client_only: bool,
        connection: Option<Weak<crate::connection_pool::LockFreeConnection>>,
        after_capacity_check: impl FnOnce(),
    ) -> crate::Result<Option<ClientOnlySession>> {
        // REMOTE-5: serialize the count check and mutation across Hello handlers.
        // This is control-plane admission only; role reads never take this lock.
        let _admission = self
            .admission
            .lock()
            .expect("client-only admission poisoned");
        if !client_only {
            let _ = self.sessions.remove_sync(peer);
            return Ok(None);
        }
        if self.sessions.len() >= MAX_CLIENT_ONLY_PEERS {
            let mut expired = Vec::new();
            self.sessions.iter_sync(|peer, role| {
                if role.expired() {
                    expired.push(peer.clone());
                }
                true
            });
            for peer in expired {
                let _ = self.sessions.remove_if_sync(&peer, |role| role.expired());
            }
        }
        let at_capacity =
            self.sessions.len() >= MAX_CLIENT_ONLY_PEERS && !self.sessions.contains_sync(peer);
        after_capacity_check();
        if at_capacity {
            // REMOTE-3/5: saturation cannot erase another live declaration.
            // Reject this Hello; existing authority and receipts remain intact.
            return Err(crate::GossipError::WriteQueueFull);
        }
        let token = self
            .next_token
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                next.checked_add(1)
            })
            .map_err(|_| crate::GossipError::WriteQueueFull)?;
        let _ = self.sessions.upsert_sync(
            peer.clone(),
            DeclaredRole {
                token,
                connection,
                ended_at: Mutex::new(None),
            },
        );
        Ok(Some(ClientOnlySession(token)))
    }

    /// True while `session` is still the peer's most recent declaration.
    pub(crate) fn is_current(&self, peer: &PeerId, session: ClientOnlySession) -> bool {
        self.sessions
            .read_sync(peer, |_, role| role.token == session.0)
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer() -> PeerId {
        PeerId::from_public_key(&crate::SecretKey::generate().public())
    }

    #[test]
    fn newer_session_fences_older_receipt() {
        let peers = ClientOnlyPeers::default();
        let p = peer();
        let first = peers
            .note_declared_role(&p, true)
            .expect("admitted")
            .expect("receipt");
        assert!(peers.is_current(&p, first));
        let second = peers
            .note_declared_role(&p, true)
            .expect("admitted")
            .expect("receipt");
        assert!(!peers.is_current(&p, first), "stale receipt must be fenced");
        assert!(peers.is_current(&p, second));
        assert!(peers.contains(&p));
    }

    #[test]
    fn regular_declaration_clears_the_role() {
        let peers = ClientOnlyPeers::default();
        let p = peer();
        let receipt = peers
            .note_declared_role(&p, true)
            .expect("admitted")
            .expect("receipt");
        assert!(
            peers
                .note_declared_role(&p, false)
                .expect("regular")
                .is_none()
        );
        assert!(!peers.contains(&p));
        assert!(!peers.is_current(&p, receipt));
    }

    #[test]
    fn concurrent_admission_keeps_capacity_check_and_insert_serialized() {
        let peers = ClientOnlyPeers::default();
        for _ in 0..MAX_CLIENT_ONLY_PEERS - 1 {
            let _ = peers.note_declared_role(&peer(), true);
        }
        let start = std::sync::Barrier::new(3);
        std::thread::scope(|scope| {
            for _ in 0..2 {
                let peers = &peers;
                let start = &start;
                scope.spawn(move || {
                    let p = peer();
                    start.wait();
                    let result = peers.note_declared_role_with_observer(&p, true, || {
                        assert!(matches!(
                            peers.admission.try_lock(),
                            Err(std::sync::TryLockError::WouldBlock)
                        ));
                        assert!(peers.sessions.len() <= MAX_CLIENT_ONLY_PEERS);
                    });
                    if let Ok(Some(receipt)) = result {
                        assert!(peers.is_current(&p, receipt));
                    }
                });
            }
            start.wait();
        });
        assert_eq!(peers.sessions.len(), MAX_CLIENT_ONLY_PEERS);
        let p = peer();
        assert!(matches!(
            peers.note_declared_role(&p, true),
            Err(crate::GossipError::WriteQueueFull)
        ));
        assert_eq!(peers.sessions.len(), MAX_CLIENT_ONLY_PEERS);
    }

    #[test]
    fn capacity_plus_one_preserves_live_client_only_authority() {
        let peers = ClientOnlyPeers::default();
        let first = peer();
        let receipt = peers.note_declared_role(&first, true).unwrap().unwrap();
        for _ in 1..MAX_CLIENT_ONLY_PEERS {
            peers.note_declared_role(&peer(), true).unwrap().unwrap();
        }
        let overflow = peer();
        assert!(matches!(
            peers.note_declared_role(&overflow, true),
            Err(crate::GossipError::WriteQueueFull)
        ));
        assert!(peers.contains(&first));
        assert!(peers.is_current(&first, receipt));
        assert!(!peers.contains(&overflow));
        let successor = peers.note_declared_role(&first, true).unwrap().unwrap();
        assert!(!peers.is_current(&first, receipt));
        assert!(peers.is_current(&first, successor));
        peers.note_declared_role(&first, false).unwrap();
        assert!(peers.note_declared_role(&overflow, true).unwrap().is_some());
        assert_eq!(peers.sessions.len(), MAX_CLIENT_ONLY_PEERS);
    }

    #[test]
    fn expired_disconnected_role_reclaims_capacity_but_live_successor_survives() {
        let peers = ClientOnlyPeers::default();
        let departed = peer();
        let ended = peers.note_declared_role(&departed, true).unwrap().unwrap();
        peers.mark_ended(&departed, ended);
        peers.sessions.read_sync(&departed, |_, role| {
            *role.ended_at.lock().unwrap() = Some(Instant::now() - DISCONNECTED_ROLE_TTL);
        });
        let live = peer();
        let old = peers.note_declared_role(&live, true).unwrap().unwrap();
        let successor = peers.note_declared_role(&live, true).unwrap().unwrap();
        peers.mark_ended(&live, old);
        assert!(
            peers
                .sessions
                .read_sync(&live, |_, role| { role.ended_at.lock().unwrap().is_none() })
                .unwrap()
        );
        for _ in 2..MAX_CLIENT_ONLY_PEERS {
            peers.note_declared_role(&peer(), true).unwrap().unwrap();
        }
        let newcomer = peer();
        assert!(peers.note_declared_role(&newcomer, true).unwrap().is_some());
        assert!(!peers.contains(&departed));
        assert!(peers.contains(&live));
        assert!(peers.is_current(&live, successor));
        assert_eq!(peers.sessions.len(), MAX_CLIENT_ONLY_PEERS);
    }

    #[test]
    fn table_is_bounded() {
        let peers = ClientOnlyPeers::default();
        for _ in 0..(MAX_CLIENT_ONLY_PEERS + 50) {
            let _ = peers.note_declared_role(&peer(), true);
        }
        assert!(peers.sessions.len() <= MAX_CLIENT_ONLY_PEERS);
    }
}
