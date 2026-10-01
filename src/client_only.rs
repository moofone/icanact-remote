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

/// Upper bound on remembered client-only identities.
const MAX_CLIENT_ONLY_PEERS: usize = 4096;

/// Opaque receipt for one client-only declaration (one authenticated session).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ClientOnlySession(u64);

#[derive(Default)]
pub(crate) struct ClientOnlyPeers {
    sessions: SccHashMap<PeerId, u64>,
    next_token: AtomicU64,
}

impl ClientOnlyPeers {
    pub(crate) fn contains(&self, peer: &PeerId) -> bool {
        self.sessions.contains_sync(peer)
    }

    /// Record the role the peer declared in its latest authenticated Hello.
    /// A client-only declaration returns the receipt for that session; a
    /// regular declaration forgets any earlier client-only record.
    pub(crate) fn note_declared_role(
        &self,
        peer: &PeerId,
        client_only: bool,
    ) -> Option<ClientOnlySession> {
        if !client_only {
            let _ = self.sessions.remove_sync(peer);
            return None;
        }
        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        if self.sessions.len() >= MAX_CLIENT_ONLY_PEERS && !self.sessions.contains_sync(peer) {
            // Bounded: forget one arbitrary identity to admit the new one.
            if let Some(entry) = self.sessions.begin_sync() {
                let _ = entry.remove();
            }
        }
        let _ = self.sessions.upsert_sync(peer.clone(), token);
        Some(ClientOnlySession(token))
    }

    /// True while `session` is still the peer's most recent declaration.
    pub(crate) fn is_current(&self, peer: &PeerId, session: ClientOnlySession) -> bool {
        self.sessions
            .read_sync(peer, |_, token| *token == session.0)
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
        let first = peers.note_declared_role(&p, true).expect("receipt");
        assert!(peers.is_current(&p, first));
        let second = peers.note_declared_role(&p, true).expect("receipt");
        assert!(!peers.is_current(&p, first), "stale receipt must be fenced");
        assert!(peers.is_current(&p, second));
        assert!(peers.contains(&p));
    }

    #[test]
    fn regular_declaration_clears_the_role() {
        let peers = ClientOnlyPeers::default();
        let p = peer();
        let receipt = peers.note_declared_role(&p, true).expect("receipt");
        assert!(peers.note_declared_role(&p, false).is_none());
        assert!(!peers.contains(&p));
        assert!(!peers.is_current(&p, receipt));
    }

    #[test]
    fn table_is_bounded() {
        let peers = ClientOnlyPeers::default();
        for _ in 0..(MAX_CLIENT_ONLY_PEERS + 50) {
            peers.note_declared_role(&peer(), true);
        }
        assert!(peers.sessions.len() <= MAX_CLIENT_ONLY_PEERS);
    }
}
