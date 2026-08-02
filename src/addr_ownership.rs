//! Pure arbitration core for "who owns address A" decisions.
//!
//! This module holds ONLY the decision logic: given the currently recorded
//! owner of an address (if any) and an incoming claim, decide whether the
//! claim may be adopted. It is deliberately synchronous, allocation-free on
//! the hot path, and has no knowledge of `GossipState`, the connection pool,
//! or any lock — callers own all of that and are responsible for performing
//! the actual read-then-mutate around this decision.
//!
//! Stage 1 of a staged refactor: this is the truth table the future
//! single-owner registry actor will use to serialize ownership decisions.
//! Wiring it in now, ahead of the actor, gives every existing call site a
//! fail-closed early return the moment a conflicting claim is detected, even
//! though the check and the eventual state mutation are not yet atomic
//! across the `gossip_state` mutex and the lock-free `addr_to_peer_id` map.
//! Full check-then-commit atomicity is Stage 2's job.

use crate::PeerId;

/// How strongly an address claim (current or incoming) is backed by
/// evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimKind {
    /// Backed by an observed connection: the raw TCP source of the
    /// connection matches the claimed address, we ourselves dialed the
    /// address and completed a TLS handshake with it, or a local operator
    /// explicitly pinned the address via `configure_peer`.
    Verified,
    /// Inferred only from a peer's self-report: a `sender_bind_addr` that
    /// does not match the connection's raw TCP source, or a third-party
    /// address learned via peer-list gossip.
    Provisional,
}

/// The node currently recorded as owning an address, and how that record
/// was established.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Owner {
    pub node_id: PeerId,
    pub kind: ClaimKind,
}

/// An incoming claim on an address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claim {
    pub node_id: PeerId,
    pub kind: ClaimKind,
}

/// Why a claim was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    /// The claimed address is this node's own bind/advertised address; no
    /// remote node may ever claim it.
    LocalAddress,
    /// An identity was authenticated, but the address itself was only a
    /// self-report or third-party discovery hint. Unverified evidence may
    /// not create an exclusive address owner.
    UnverifiedAddress,
    /// The address already has a verified owner and a verified owner is
    /// never displaced by a later claim, verified or not.
    VerifiedOwnerPresent,
    /// The address already has a provisional owner and the incoming claim
    /// is also merely provisional; first-come wins among unverified claims.
    ProvisionalFirstCome,
}

/// The outcome of arbitrating a claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    Accept,
    Reject(RejectReason),
}

/// Outcome of a caller's attempt to claim an address for an identity (e.g.
/// via `GossipRegistry::add_peer_with_node_id`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddrClaimOutcome {
    /// The address is now attributed to the claimed identity, or already
    /// was. Safe to perform further address-keyed work for this address.
    Accepted,
    /// The claim was refused — an ownership conflict, an invalid address,
    /// or a self-identity/self-address filter. Callers must not perform any
    /// further address-keyed mutation for this address; either stop, or
    /// retry the claim against a different, unambiguous address (e.g. the
    /// raw observed TCP source of the connection).
    Rejected,
}

/// Decide whether `claim` may take (or refresh) ownership of an address
/// currently recorded as owned by `current` (`None` if unowned).
///
/// `is_local_addr` must be `true` when the address being claimed is this
/// node's own bind/advertised address; that check takes priority over
/// everything else and always rejects a remote claimant, regardless of
/// `current` or `claim`.
pub fn arbitrate(current: Option<Owner>, claim: Claim, is_local_addr: bool) -> Decision {
    if is_local_addr {
        return Decision::Reject(RejectReason::LocalAddress);
    }

    match current {
        // A TLS identity authenticates who sent a claim, not that the sender
        // owns an arbitrary address carried in the payload. Publishing an
        // exclusive first-owner route from that evidence lets any connected
        // peer reserve unlimited victim addresses. First ownership therefore
        // requires independently verified address evidence; callers may keep
        // provisional data as a non-authoritative discovery hint instead.
        None if claim.kind == ClaimKind::Provisional => {
            Decision::Reject(RejectReason::UnverifiedAddress)
        }
        None => Decision::Accept,
        Some(owner) if owner.node_id == claim.node_id => {
            // Same-node refresh: kind only ever upgrades (Provisional ->
            // Verified), never downgrades. The caller is responsible for
            // persisting `max(owner.kind, claim.kind)` on accept; this
            // function only needs to know the refresh itself is allowed.
            Decision::Accept
        }
        Some(owner) if owner.kind == ClaimKind::Verified => {
            Decision::Reject(RejectReason::VerifiedOwnerPresent)
        }
        Some(_provisional_other) => match claim.kind {
            // A genuinely authenticated owner displaces a wire-only
            // provisional alias. Without this, an attacker could
            // pre-claim an unowned victim address with a cheap
            // provisional claim and permanently lock out the real peer.
            ClaimKind::Verified => Decision::Accept,
            ClaimKind::Provisional => Decision::Reject(RejectReason::ProvisionalFirstCome),
        },
    }
}

/// Resolve the effective `ClaimKind` for an accepted claim, applying the
/// never-downgrade rule for same-node refreshes.
///
/// Callers persist this value (not `claim.kind` directly) as the new
/// owner's kind after an `Accept` decision.
pub fn resolved_kind(current: Option<&Owner>, claim: &Claim) -> ClaimKind {
    match current {
        Some(owner) if owner.node_id == claim.node_id && owner.kind == ClaimKind::Verified => {
            ClaimKind::Verified
        }
        _ => claim.kind,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(byte: u8) -> PeerId {
        // Arbitrary bytes rarely decode as a valid Ed25519 verifying key, so
        // build test PeerIds through the crate's testing constructor, which
        // derives one from a deterministic keypair seed instead.
        crate::KeyPair::new_for_testing(format!("addr-ownership-test-{byte}")).peer_id()
    }

    fn node_a() -> PeerId {
        peer(1)
    }
    fn node_b() -> PeerId {
        peer(2)
    }

    /// T-ARB: exhaustive table over
    /// {unowned, provisional-self, provisional-other, verified-self, verified-other}
    /// x {Provisional claim, Verified claim}, plus the is_local_addr=true cases.
    #[test]
    fn t_arb_unowned_rejects_provisional_claim() {
        let d = arbitrate(
            None,
            Claim {
                node_id: node_a(),
                kind: ClaimKind::Provisional,
            },
            false,
        );
        assert_eq!(d, Decision::Reject(RejectReason::UnverifiedAddress));
    }

    #[test]
    fn t_arb_unowned_accepts_verified_claim() {
        let d = arbitrate(
            None,
            Claim {
                node_id: node_a(),
                kind: ClaimKind::Verified,
            },
            false,
        );
        assert_eq!(d, Decision::Accept);
    }

    #[test]
    fn t_arb_provisional_self_accepts_provisional_refresh() {
        let current = Owner {
            node_id: node_a(),
            kind: ClaimKind::Provisional,
        };
        let claim = Claim {
            node_id: node_a(),
            kind: ClaimKind::Provisional,
        };
        assert_eq!(arbitrate(Some(current), claim, false), Decision::Accept);
    }

    #[test]
    fn t_arb_provisional_self_accepts_verified_refresh() {
        let current = Owner {
            node_id: node_a(),
            kind: ClaimKind::Provisional,
        };
        let claim = Claim {
            node_id: node_a(),
            kind: ClaimKind::Verified,
        };
        assert_eq!(arbitrate(Some(current), claim, false), Decision::Accept);
    }

    #[test]
    fn t_arb_verified_self_accepts_provisional_refresh() {
        let current = Owner {
            node_id: node_a(),
            kind: ClaimKind::Verified,
        };
        let claim = Claim {
            node_id: node_a(),
            kind: ClaimKind::Provisional,
        };
        assert_eq!(arbitrate(Some(current), claim, false), Decision::Accept);
    }

    #[test]
    fn t_arb_verified_self_accepts_verified_refresh() {
        let current = Owner {
            node_id: node_a(),
            kind: ClaimKind::Verified,
        };
        let claim = Claim {
            node_id: node_a(),
            kind: ClaimKind::Verified,
        };
        assert_eq!(arbitrate(Some(current), claim, false), Decision::Accept);
    }

    #[test]
    fn t_arb_provisional_other_rejects_provisional_claim() {
        let current = Owner {
            node_id: node_a(),
            kind: ClaimKind::Provisional,
        };
        let claim = Claim {
            node_id: node_b(),
            kind: ClaimKind::Provisional,
        };
        assert_eq!(
            arbitrate(Some(current), claim, false),
            Decision::Reject(RejectReason::ProvisionalFirstCome)
        );
    }

    #[test]
    fn t_arb_provisional_other_accepts_verified_claim() {
        let current = Owner {
            node_id: node_a(),
            kind: ClaimKind::Provisional,
        };
        let claim = Claim {
            node_id: node_b(),
            kind: ClaimKind::Verified,
        };
        assert_eq!(arbitrate(Some(current), claim, false), Decision::Accept);
    }

    #[test]
    fn t_arb_verified_other_rejects_provisional_claim() {
        let current = Owner {
            node_id: node_a(),
            kind: ClaimKind::Verified,
        };
        let claim = Claim {
            node_id: node_b(),
            kind: ClaimKind::Provisional,
        };
        assert_eq!(
            arbitrate(Some(current), claim, false),
            Decision::Reject(RejectReason::VerifiedOwnerPresent)
        );
    }

    #[test]
    fn t_arb_verified_other_rejects_verified_claim() {
        let current = Owner {
            node_id: node_a(),
            kind: ClaimKind::Verified,
        };
        let claim = Claim {
            node_id: node_b(),
            kind: ClaimKind::Verified,
        };
        assert_eq!(
            arbitrate(Some(current), claim, false),
            Decision::Reject(RejectReason::VerifiedOwnerPresent)
        );
    }

    #[test]
    fn t_arb_local_addr_rejects_unowned() {
        let d = arbitrate(
            None,
            Claim {
                node_id: node_a(),
                kind: ClaimKind::Verified,
            },
            true,
        );
        assert_eq!(d, Decision::Reject(RejectReason::LocalAddress));
    }

    #[test]
    fn t_arb_local_addr_rejects_provisional_claim() {
        let d = arbitrate(
            None,
            Claim {
                node_id: node_a(),
                kind: ClaimKind::Provisional,
            },
            true,
        );
        assert_eq!(d, Decision::Reject(RejectReason::LocalAddress));
    }

    #[test]
    fn t_arb_local_addr_rejects_even_over_provisional_other_owner() {
        let current = Owner {
            node_id: node_a(),
            kind: ClaimKind::Provisional,
        };
        let claim = Claim {
            node_id: node_b(),
            kind: ClaimKind::Verified,
        };
        assert_eq!(
            arbitrate(Some(current), claim, true),
            Decision::Reject(RejectReason::LocalAddress)
        );
    }

    #[test]
    fn t_arb_local_addr_rejects_even_over_verified_other_owner() {
        let current = Owner {
            node_id: node_a(),
            kind: ClaimKind::Verified,
        };
        let claim = Claim {
            node_id: node_b(),
            kind: ClaimKind::Verified,
        };
        assert_eq!(
            arbitrate(Some(current), claim, true),
            Decision::Reject(RejectReason::LocalAddress)
        );
    }

    #[test]
    fn t_arb_local_addr_rejects_even_for_same_node_refresh() {
        // Even a "refresh" from the node that legitimately owns the local
        // address must never be routed as a remote claim in the first
        // place; is_local_addr short-circuits regardless of node identity.
        let current = Owner {
            node_id: node_a(),
            kind: ClaimKind::Verified,
        };
        let claim = Claim {
            node_id: node_a(),
            kind: ClaimKind::Verified,
        };
        assert_eq!(
            arbitrate(Some(current), claim, true),
            Decision::Reject(RejectReason::LocalAddress)
        );
    }

    // Kind upgrade/never-downgrade rule, checked explicitly via
    // `resolved_kind` (the value callers persist on Accept).
    #[test]
    fn t_arb_resolved_kind_same_node_provisional_refresh_stays_provisional() {
        let current = Owner {
            node_id: node_a(),
            kind: ClaimKind::Provisional,
        };
        let claim = Claim {
            node_id: node_a(),
            kind: ClaimKind::Provisional,
        };
        assert_eq!(
            resolved_kind(Some(&current), &claim),
            ClaimKind::Provisional
        );
    }

    #[test]
    fn t_arb_resolved_kind_same_node_upgrades_to_verified() {
        let current = Owner {
            node_id: node_a(),
            kind: ClaimKind::Provisional,
        };
        let claim = Claim {
            node_id: node_a(),
            kind: ClaimKind::Verified,
        };
        assert_eq!(resolved_kind(Some(&current), &claim), ClaimKind::Verified);
    }

    #[test]
    fn t_arb_resolved_kind_same_node_never_downgrades() {
        let current = Owner {
            node_id: node_a(),
            kind: ClaimKind::Verified,
        };
        let claim = Claim {
            node_id: node_a(),
            kind: ClaimKind::Provisional,
        };
        assert_eq!(resolved_kind(Some(&current), &claim), ClaimKind::Verified);
    }

    #[test]
    fn t_arb_resolved_kind_unowned_takes_claim_kind() {
        let claim = Claim {
            node_id: node_a(),
            kind: ClaimKind::Provisional,
        };
        assert_eq!(resolved_kind(None, &claim), ClaimKind::Provisional);
    }

    #[test]
    fn t_arb_resolved_kind_other_node_takes_claim_kind() {
        // current is a Verified OTHER node, claim is Provisional: if
        // resolved_kind wrongly consulted the old owner's kind instead of
        // recognizing the node id differs, it would wrongly return Verified
        // here. Only reached in practice when arbitrate() already returned
        // Accept for a differing-node case (not this exact combination —
        // this test exercises resolved_kind in isolation to prove it keys
        // off node identity, not merely "was there a prior owner").
        let current = Owner {
            node_id: node_a(),
            kind: ClaimKind::Verified,
        };
        let claim = Claim {
            node_id: node_b(),
            kind: ClaimKind::Provisional,
        };
        assert_eq!(
            resolved_kind(Some(&current), &claim),
            ClaimKind::Provisional
        );
    }
}
