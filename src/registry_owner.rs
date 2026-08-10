//! Single-owner registry actor: the sole authority for "who owns address A".
//!
//! Stage 1 ([`crate::addr_ownership`]) extracted the pure arbitration truth
//! table but left every call site performing its own read-decide-write around
//! it, across two independently synchronized structures: the `gossip_state`
//! mutex (`peers`) and the lock-free `addr_to_peer_id` routing map. No single
//! critical section can span both, so two concurrent claims for the same
//! address could each read pre-claim state, each pass arbitration, and each
//! commit — the arbitration verdict was advisory rather than binding.
//!
//! Stage 2 (this module) removes the race by construction rather than by
//! protocol. Exactly one task owns the ownership table, and that same task
//! performs routing publication. A claim is therefore decided AND published
//! inside one serialized command with no `.await` in between and no lock at
//! all: there is no interleaving point for a second claimant to observe stale
//! state. Lifecycle callbacks do carry the accepted claim's generation back
//! to this task: peer identity alone cannot distinguish an old connection
//! from a newer reconnect by the same authenticated peer.
//!
//! Reads never enter the mailbox. After every committed mutation the owner
//! task publishes an immutable [`RoutingSnapshot`] through an
//! [`arc_swap::ArcSwap`]; lookups load that snapshot lock-free.
//!
//! Deliberately NOT owned here: `peers` sequence/session/failure fields,
//! `peer_to_actors`, and the admission tables. Those remain under
//! `gossip_state` and are updated as derived projections of the decision this
//! actor returns.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Weak};

use arc_swap::ArcSwap;
use crossbeam_queue::ArrayQueue;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, trace, warn};

use crate::PeerId;
use crate::addr_ownership::{
    AddrClaimOutcome, Claim, ClaimKind, Decision, Owner, RejectReason, arbitrate, resolved_kind,
};

/// Mailbox depth. Claims occur on connection establishment and gossip
/// full-syncs, not on the message hot path, so a modest bound is ample; a
/// full mailbox applies backpressure to the claimant rather than dropping a
/// decision.
const OWNER_MAILBOX_CAPACITY: usize = 512;

/// Legacy/unit-test callers that only have a socket discriminator still get a
/// deterministic receipt identity. Production transport paths always pass a
/// `LockFreeStreamHandle` instance id; this fallback keeps the small owner API
/// usable for synthetic callers without making a socket address the live
/// connection identity again.
pub(crate) fn legacy_connection_instance_id(session_source: SocketAddr) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    session_source.hash(&mut hasher);
    hasher.finish() | (1_u64 << 63)
}

const REAP_PENDING: u8 = 0;
const REAP_COMMITTED: u8 = 1;
const REAP_INVALIDATED: u8 = 2;

/// Exact authority held by one ownership generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnershipToken {
    owner: PeerId,
    generation: CommitSeq,
}

impl OwnershipToken {
    /// Build a token for `owner` at `generation`.
    pub fn new(owner: PeerId, generation: CommitSeq) -> Self {
        Self { owner, generation }
    }

    /// Authenticated identity holding this generation.
    pub fn owner(&self) -> &PeerId {
        &self.owner
    }

    /// Owner-actor commit that created/refreshed this generation.
    pub fn generation(&self) -> CommitSeq {
        self.generation
    }
}

/// Source state a migration is conditional on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceExpectation {
    /// The source must still be unowned.
    Unowned,
    /// The source must still be held by this exact owner generation.
    Owned(OwnershipToken),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PublishedOwner {
    owner: Owner,
    generation: CommitSeq,
}

/// Immutable, lock-free-readable publication of the address ownership table.
///
/// A mutation clones only one bounded shard (two for an address migration),
/// while every untouched shard remains structurally shared with the previous
/// snapshot. This keeps adversarial address churn from turning publication
/// into quadratic whole-map copying. Reads remain lock-free and never enter
/// the owner mailbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingSnapshot {
    owner_shards: [Arc<HashMap<SocketAddr, PublishedOwner>>; ROUTING_SNAPSHOT_SHARDS],
    /// Operator-pin identity, published separately from ownership: a pin is
    /// a DIFFERENT fact than "who owns this address" (see `operator_pinned`'s
    /// doc comment), decided and moved by its own owner commands
    /// (`configure_peer`'s atomic transaction, and `migrate`'s pin carry),
    /// not by `claim`. Neither `ConnectionPool`'s derived `required_addr`
    /// (updated by any `.connect()` call, configured or not) nor the
    /// ownership generation above (advanced by every accepted claim,
    /// including unrelated gossip/discovery chatter for the same identity)
    /// answers "is this peer still the one I pinned here" -- only the
    /// owner's own pin decision does, so it gets its own publication.
    pin_shards: [Arc<HashMap<SocketAddr, PeerId>>; ROUTING_SNAPSHOT_SHARDS],
    /// Reverse of `pin_shards`: the address, if any, `peer_id` is CURRENTLY
    /// operator-pinned to. Kept in the SAME `with_pin` step as the
    /// addr-keyed side, so the two can never disagree. Not sharded by
    /// address (there is nothing to shard on for a peer-keyed lookup);
    /// operator pins are expected to be orders of magnitude fewer than
    /// gossiped addresses, so one `Arc<HashMap>` clone-on-write is fine at
    /// this scale.
    ///
    /// Exists so a non-owner caller can cheaply, lock-freely check "is this
    /// peer pinned to some OTHER address" before writing an address-keyed
    /// field it shares with the owner's own pin publication -- see
    /// `Peer::connect_with_route_mode`'s use of `pinned_addr_for`.
    pinned_by_peer: Arc<HashMap<PeerId, SocketAddr>>,
}

const ROUTING_SNAPSHOT_SHARDS: usize = 64;

impl Default for RoutingSnapshot {
    fn default() -> Self {
        Self {
            owner_shards: std::array::from_fn(|_| Arc::new(HashMap::new())),
            pin_shards: std::array::from_fn(|_| Arc::new(HashMap::new())),
            pinned_by_peer: Arc::new(HashMap::new()),
        }
    }
}

impl RoutingSnapshot {
    fn shard_index(addr: &SocketAddr) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        addr.hash(&mut hasher);
        hasher.finish() as usize % ROUTING_SNAPSHOT_SHARDS
    }

    /// The recorded owner of `addr`, if any.
    pub fn owner(&self, addr: &SocketAddr) -> Option<&Owner> {
        self.owner_shards[Self::shard_index(addr)]
            .get(addr)
            .map(|published| &published.owner)
    }

    /// The identity `addr` currently routes to, if any.
    pub fn peer_id(&self, addr: &SocketAddr) -> Option<&PeerId> {
        self.owner(addr).map(|owner| &owner.node_id)
    }

    /// Exact owner generation currently published for `addr`.
    pub fn ownership_token(&self, addr: &SocketAddr) -> Option<OwnershipToken> {
        self.owner_shards[Self::shard_index(addr)]
            .get(addr)
            .map(|published| {
                OwnershipToken::new(published.owner.node_id.clone(), published.generation)
            })
    }

    /// Whether `owner` and `generation` are still the authoritative claim.
    pub fn claim_is_current(
        &self,
        addr: &SocketAddr,
        owner: &PeerId,
        generation: CommitSeq,
    ) -> bool {
        self.owner_shards[Self::shard_index(addr)]
            .get(addr)
            .is_some_and(|published| {
                published.owner.node_id == *owner && published.generation == generation
            })
    }

    /// Number of addresses with a recorded owner.
    pub fn len(&self) -> usize {
        self.owner_shards.iter().map(|shard| shard.len()).sum()
    }

    /// Whether no address has a recorded owner.
    pub fn is_empty(&self) -> bool {
        self.owner_shards.iter().all(|shard| shard.is_empty())
    }

    fn with_owner(&self, addr: SocketAddr, owner: Option<(Owner, CommitSeq)>) -> Self {
        let mut next = self.clone();
        let shard_index = Self::shard_index(&addr);
        let mut shard = (*next.owner_shards[shard_index]).clone();
        match owner {
            Some((owner, generation)) => {
                shard.insert(addr, PublishedOwner { owner, generation });
            }
            None => {
                shard.remove(&addr);
            }
        }
        next.owner_shards[shard_index] = Arc::new(shard);
        next
    }

    /// The peer `addr` is currently operator-pinned for, if any.
    pub fn pin_owner(&self, addr: &SocketAddr) -> Option<&PeerId> {
        self.pin_shards[Self::shard_index(addr)].get(addr)
    }

    /// Whether `peer_id` is still the exact identity `addr` is pinned for.
    ///
    /// This is the authoritative "did I lose the configuration" check: it
    /// reads the SAME pin decision `configure_peer`'s atomic transaction
    /// (or `migrate`'s pin carry) itself just published, not a value some
    /// unrelated path can move independently -- `ConnectionPool`'s
    /// `required_addr` is written by every `.connect()` call, configured or
    /// not, and the ownership generation advances on every accepted claim,
    /// including unrelated gossip/discovery chatter for the SAME identity.
    /// Neither answers this question; only the owner's own pin state does.
    pub fn pin_is_current(&self, addr: &SocketAddr, peer_id: &PeerId) -> bool {
        self.pin_owner(addr) == Some(peer_id)
    }

    /// The address, if any, `peer_id` is CURRENTLY operator-pinned to. See
    /// `pinned_by_peer`'s doc comment.
    pub fn pinned_addr_for(&self, peer_id: &PeerId) -> Option<SocketAddr> {
        self.pinned_by_peer.get(peer_id).copied()
    }

    fn with_pin(&self, addr: SocketAddr, pin: Option<PeerId>) -> Self {
        let mut next = self.clone();
        let shard_index = Self::shard_index(&addr);
        let mut shard = (*next.pin_shards[shard_index]).clone();
        let mut pinned_by_peer = (*next.pinned_by_peer).clone();
        match pin {
            Some(peer_id) => {
                // If a DIFFERENT peer was previously pinned at `addr`, drop
                // its own reverse entry too -- mirrors `install_pin`'s
                // addr-keyed eviction semantics on this peer-keyed side.
                if let Some(previous_peer) = shard.get(&addr)
                    && *previous_peer != peer_id
                {
                    pinned_by_peer.remove(previous_peer);
                }
                shard.insert(addr, peer_id.clone());
                pinned_by_peer.insert(peer_id, addr);
            }
            None => {
                if let Some(previous_peer) = shard.remove(&addr) {
                    // Only clear the reverse entry if it still points at
                    // THIS address: within one snapshot construction (e.g.
                    // `install_pin` calling `with_pin(new, Some(p))` then
                    // `with_pin(evicted, None)` for the SAME peer, or
                    // `migrate` doing the reverse order), the peer may
                    // already have a NEWER pin recorded here by the time
                    // this stale clear runs, which must not be clobbered.
                    if pinned_by_peer.get(&previous_peer) == Some(&addr) {
                        pinned_by_peer.remove(&previous_peer);
                    }
                }
            }
        }
        next.pin_shards[shard_index] = Arc::new(shard);
        next.pinned_by_peer = Arc::new(pinned_by_peer);
        next
    }
}

/// Why a claim did not take ownership.
///
/// `#[non_exhaustive]` (added review round against `c111380`,
/// `registry.rs:4538`): this enum already exists on `main`, and this PR's
/// `ReapInProgress` variant is a genuine, unavoidable break for any
/// exhaustive external match on it -- adding a variant to a
/// non-`#[non_exhaustive]` public enum always is. That break cannot be
/// undone (the variant is real, load-bearing information callers need),
/// but marking it `#[non_exhaustive]` now, in the same round that adds it,
/// costs nothing further for THIS round's consumers (they already must
/// handle the new variant one way or another) while preventing this exact
/// category of silent break from recurring the next time this enum grows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ClaimRejection {
    /// The arbitration truth table refused the claim.
    Arbitration(RejectReason),
    /// The owner task is not reachable (shutting down, or its mailbox side
    /// was dropped). Fail closed: no address-keyed mutation may proceed on a
    /// decision that was never actually made.
    OwnerUnavailable,
    /// A `cleanup_dead_peers` sweep currently holds a reap reservation for
    /// this address (see `OwnerCommand::ReserveForReap`). Refused
    /// unconditionally, before `arbitrate` is even consulted: the sweep's
    /// destructive, non-owner work (actor removal, tombstone emission) is
    /// about to run, or is running, on the assumption that nothing can
    /// commit ownership of this address out from under it while the
    /// reservation is held. The caller is expected to retry -- the
    /// reservation is released promptly once the sweep finishes with this
    /// address, successfully or not.
    ReapInProgress,
    /// P1 finding (review round against `ded8495`, `registry.rs:4982`): a
    /// `configure_peer` retry presented an `expected_generation` older
    /// than the current value `configure_peer_generation` records for
    /// this peer -- a LATER `configure_peer` call for the SAME peer has
    /// already been made, atomically, at the owner, since this retry's
    /// own generation was established. Refused unconditionally, before
    /// `arbitrate` is even consulted and before touching anything: retrying
    /// again would not help (a newer request already superseded this one,
    /// permanently, by construction -- generations only increase), unlike
    /// `ReapInProgress`, which IS worth retrying.
    SupersededByNewerConfiguration,
}

/// Monotonic position of a committed mutation in the owner task's total
/// order. Issued by the owner task alone, so it is a true sequence number and
/// not a timestamp: `a < b` means `a` was committed strictly before `b`.
pub type CommitSeq = u64;

/// `OwnerCommand::ReleaseDeadPeer`'s full outcome. A plain
/// `Option<CommitSeq>` (as this used to be) collapses two, very
/// different, refusal reasons into one bit: "this candidate has been
/// PROVEN ALIVE since the failure evidence it was selected on" and "there
/// was never any ownership here to release in the first place (operator
/// pin, or this identity never actually held it)". A caller that destroys
/// unrelated, transient state (capabilities, clock calibration) only when
/// this call actually released ownership needs to tell those apart: the
/// former must block that destruction too, the latter must not -- the
/// address genuinely has nothing left to protect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadPeerReleaseOutcome {
    /// Ownership was retracted at this commit.
    Released(CommitSeq),
    /// Refused: `addr` has direct or ordinary liveness evidence causally
    /// AFTER the failure this reap is acting on (`claim_committed_at` or
    /// `liveness_evidence_at`) -- this peer has been proven alive. Callers
    /// must treat this exactly like a live peer: no destructive cleanup
    /// of ANY kind for this candidate, not just ownership. Also the
    /// fail-CLOSED default when the owner itself is unreachable: unable
    /// to prove anything, so assumed unsafe, the same "cannot prove it is
    /// safe, so don't" direction every other command in this module
    /// takes.
    ProvenAlive,
    /// Refused for a reason unrelated to liveness: `addr` is
    /// operator-pinned, or `peer_id` never actually held ownership of it
    /// at the owner at all (a `GossipState`-only entry, e.g. discovered
    /// but never connection-verified). There is no ownership here for a
    /// caller to have accidentally destroyed, so transient, non-ownership
    /// cleanup may still proceed.
    NotApplicable,
}

/// Lifecycle receipt for one accepted claim command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClaimReceipt {
    generation: CommitSeq,
    created_ownership: bool,
}

impl ClaimReceipt {
    /// Owner-actor generation accepted for this command.
    pub fn generation(self) -> CommitSeq {
        self.generation
    }

    /// Whether this command created ownership from an unowned address.
    pub fn created_ownership(self) -> bool {
        self.created_ownership
    }
}

/// The committed result of a claim. An `Accepted` value is only ever produced
/// after the ownership entry AND the routing publication have already been
/// applied, so any state the caller derives from it cannot precede it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimCommit {
    Accepted {
        /// The kind persisted for the new owner, after the never-downgrade
        /// rule for same-node refreshes.
        kind: ClaimKind,
        /// The previous owner's identity when this claim genuinely CHANGED
        /// the owner (a different node id). `None` for a first claim or a
        /// same-node refresh. Drives identity-scoped rekey at the caller.
        displaced: Option<PeerId>,
        /// Whether this exact command created ownership from an unowned
        /// address. A same-identity refresh is accepted but returns `false`.
        created_ownership: bool,
        /// This claim's position in the owner task's commit order.
        ///
        /// The decision is atomic inside the task, but a caller then projects
        /// it into address-keyed state it does not hold any lock over while
        /// awaiting the reply (`peers[].node_id`, connection indexing). Two
        /// callers can therefore reach their projection step out of commit
        /// order and the loser would overwrite the winner. Carrying the
        /// commit position lets a caller discard a projection that is older
        /// than one already applied for the same address.
        commit_seq: CommitSeq,
    },
    Rejected(ClaimRejection),
}

impl ClaimCommit {
    /// Caller-facing accept/reject verdict.
    pub fn outcome(&self) -> AddrClaimOutcome {
        match self {
            Self::Accepted { .. } => AddrClaimOutcome::Accepted,
            Self::Rejected(_) => AddrClaimOutcome::Rejected,
        }
    }

    /// Whether the claim was committed.
    pub fn is_accepted(&self) -> bool {
        matches!(self, Self::Accepted { .. })
    }

    /// This claim's position in the owner's commit order, if it committed.
    pub fn commit_seq(&self) -> Option<CommitSeq> {
        match self {
            Self::Accepted { commit_seq, .. } => Some(*commit_seq),
            Self::Rejected(_) => None,
        }
    }

    /// Lifecycle receipt for a committed claim.
    pub fn receipt(&self) -> Option<ClaimReceipt> {
        match self {
            Self::Accepted {
                commit_seq,
                created_ownership,
                ..
            } => Some(ClaimReceipt {
                generation: *commit_seq,
                created_ownership: *created_ownership,
            }),
            Self::Rejected(_) => None,
        }
    }

    /// Whether the accepted claim displaced a DIFFERENT identity, which
    /// requires the caller to rekey every identity-scoped side table for the
    /// address.
    pub fn owner_changed(&self) -> bool {
        matches!(
            self,
            Self::Accepted {
                displaced: Some(_),
                ..
            }
        )
    }

    /// Whether the committed owner is verified.
    pub fn is_verified(&self) -> bool {
        matches!(
            self,
            Self::Accepted {
                kind: ClaimKind::Verified,
                ..
            }
        )
    }
}

/// The committed result of an ownership move between two addresses.
///
/// Distinguishing "there was nothing to move" from "the destination belongs
/// to someone else" matters to the caller: only the latter means a competing
/// identity now owns the destination, and only the latter must stop the
/// caller from publishing the address change into its own address-keyed
/// state. An address that was never claimed (a seed configured by host name
/// before any handshake) has no ownership record to move and no conflict.
///
/// `#[non_exhaustive]` (added review round against `c111380`,
/// `registry.rs:4538`) for the same reason, and at the same time, as
/// `ClaimRejection`'s own: this PR's `ReapInProgress` variant is a real,
/// unavoidable break for an exhaustive external match on an enum that
/// already existed on `main`; marking it `#[non_exhaustive]` now costs
/// this round's consumers nothing further and closes off the category for
/// future growth.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MigrateOutcome {
    /// Ownership (and its routing publication) moved from `from` to `to`.
    Migrated {
        /// This move's position in the owner task's commit order. Callers
        /// use it to fence their own address-keyed state: both the vacated
        /// and the occupied address advance to this position, so a claim
        /// that committed before the move can no longer project onto either.
        commit_seq: CommitSeq,
    },
    /// `from` had no recorded owner: nothing to move, nothing in conflict.
    /// Only reported once the destination has been checked and found free
    /// (or held by the source's own identity) — see the single owner's
    /// migration rules in this module.
    SourceUnowned,
    /// `to` is already owned by a DIFFERENT identity. Ownership stays where
    /// it is and the caller must not move address-keyed state onto `to`.
    TargetOwnedByOther,
    /// `to` names this registry's own bind or advertised address. A remote
    /// peer can never own it, regardless of what DNS returned.
    TargetIsLocal,
    /// The caller named an expected owner for `from` and that is no longer
    /// the identity holding it.
    ///
    /// A caller that re-keys its own identity-scoped state alongside the move
    /// must resolve the identity to re-key BEFORE issuing the command, and
    /// that resolution is not part of the command. Between the two, another
    /// claimant can displace the source's owner. Naming the expected owner
    /// makes the move conditional on the caller's resolution still holding,
    /// so a displaced caller re-keys nothing instead of re-keying the wrong
    /// identity onto the destination.
    SourceOwnerMismatch,
    /// A `cleanup_dead_peers` sweep currently holds a reap reservation for
    /// `from`, `to`, or both -- see `reap_reserved`'s doc comment and
    /// `OwnerCommand::ReserveForReap`. Refused unconditionally, before any
    /// ownership state is even inspected: `migrate` mutates
    /// `addr_ownership`/`claim_committed_at` for both addresses exactly as
    /// `claim`/`claim_connection_scoped` do, and is the one owner command
    /// that used to reach those tables without going through `claim`'s own
    /// `reap_reserved` check. Refusing it here closes that gap: nothing may
    /// move fresh (or existing) ownership onto a reserved destination, and
    /// nothing may move ownership away from a reserved source, while a sweep
    /// is relying on `reap_reserved` to keep both fixed for the duration of
    /// its non-owner destructive work. The caller is expected to retry, same
    /// as any other refused migration -- the reservation is released
    /// promptly once the sweep finishes with that address.
    ReapInProgress,
}

impl MigrateOutcome {
    /// Whether the caller is forbidden from re-keying its own address-keyed
    /// state onto the destination: either a competing identity owns the
    /// destination, the source is no longer held by the identity the caller
    /// resolved, or a reap reservation currently holds one of the two
    /// addresses.
    pub fn is_blocked(&self) -> bool {
        matches!(
            self,
            Self::TargetOwnedByOther
                | Self::TargetIsLocal
                | Self::SourceOwnerMismatch
                | Self::ReapInProgress
        )
    }

    /// Whether ownership actually moved.
    pub fn moved(&self) -> bool {
        matches!(self, Self::Migrated { .. })
    }

    /// The move's position in the commit order, if it moved.
    pub fn commit_seq(&self) -> Option<CommitSeq> {
        match self {
            Self::Migrated { commit_seq } => Some(*commit_seq),
            Self::SourceUnowned
            | Self::TargetOwnedByOther
            | Self::TargetIsLocal
            | Self::SourceOwnerMismatch
            | Self::ReapInProgress => None,
        }
    }
}

/// Routing publication performed by the owner task, in the same serialized
/// step as the ownership decision.
///
/// Every method is synchronous and lock-free by contract: the owner task must
/// never block, and must never await, between deciding and publishing.
pub trait RoutingPublisher: Send + Sync + 'static {
    /// Route `addr` to `peer_id`.
    fn publish_owner(&self, addr: SocketAddr, peer_id: &PeerId);
    /// Withdraw `addr`'s route, but only if it still points at `peer_id`.
    fn retract_owner(&self, addr: SocketAddr, peer_id: &PeerId);
    /// Record `addr` as `peer_id`'s configured/required dial target, AND
    /// reindex any live connection for `peer_id` under `addr`
    /// (`ConnectionPool::reindex_connection_addr`), in this SAME call.
    ///
    /// Called synchronously from `PeerRegistryOwner::pin`, in the SAME
    /// serialized command as the operator-pin decision, so the two can
    /// never be observed disagreeing: without this, `configure_peer` would
    /// have to make this `ConnectionPool` write itself, afterward and
    /// outside the owner, and two concurrent `configure_peer` calls for the
    /// same peer could then have their pin decided in one order by the
    /// owner but this write land in the other order on `ConnectionPool` --
    /// two independently-atomic operations that are not atomic WITH each
    /// other. Bringing the write inside the same command the pin is
    /// decided in removes the second ordering domain entirely.
    ///
    /// The reindex is folded in here for the exact same reason, and this is
    /// the ONLY place it may happen: a caller that instead reads the pin
    /// (however it is published) and THEN calls a reindex-equivalent
    /// mutation itself is never truly atomic with the owner's own commands,
    /// no matter how tightly the read and the mutation are held together on
    /// the caller's side -- the owner runs as its own independently
    /// scheduled task, and `ConnectionPool`'s underlying maps are not
    /// protected by one lock spanning a whole owner command, so a caller's
    /// unsynchronized read/mutate pair can still straddle the exact instant
    /// a DIFFERENT owner command changes the pin, publishing a losing alias
    /// that no later check can retract. Three prior attempts in this same
    /// spot -- fencing on the ownership generation, on `ConnectionPool`'s
    /// derived `required_addr`, and finally on a dedicated but still
    /// separately-read `pinned_addr` mirror compared just before the
    /// mutation -- were all instances of *observing* the pin from outside
    /// the owner rather than performing the mutation *inside* it, and each
    /// left the same class of gap open to some degree. Doing the write
    /// here, synchronously, as part of the command that decides the pin, is
    /// the only way for the comparison and the mutation to share the
    /// owner's own serialization instead of a lock or snapshot copied from
    /// it.
    ///
    /// `evicted_addr`, `Some` whenever this SAME command's pin decision
    /// evicted a DIFFERENT address from `peer_id`'s pin (see `install_pin`/
    /// `migrate`, the two callers), is what P1 review (round against
    /// `ba2bff2`, `registry_owner.rs:615`) found missing: `connections_by_
    /// addr` aliases used to be "never un-published just because a pin
    /// moved" (this doc comment's own prior wording) -- but that is not a
    /// property to preserve, it is the bug. `reindex_connection_addr`
    /// installs `addr` as a NEW alias for `peer_id`'s connection in this
    /// same call; without also being told which address to evict, nothing
    /// ever removes the address this peer's pin just moved AWAY from,
    /// leaving `connections_by_addr[evicted_addr]` pointing at this peer's
    /// connection indefinitely. Once a DIFFERENT identity legitimately
    /// claims `evicted_addr`, `ConnectionPool::get_connection_by_peer_id`'s
    /// own address-fallback (checked whenever the new identity's own
    /// peer-indexed session has no connection yet -- the common case for a
    /// just-claimed, not-yet-directly-connected address) reads that stale
    /// alias, finds it `is_usable_connection` (a liveness check only, not
    /// an identity check), and publishes the OLD peer's live connection as
    /// the NEW peer's current connection -- traffic addressed to the new
    /// identity is delivered over the old identity's actual TCP stream.
    /// Not lost state: misdelivery.
    ///
    /// P1 review, second pass (round against `aea7772`): the first
    /// implementation of the eviction this triggers
    /// (`ConnectionPool::evict_pin_alias`) reintroduced the exact
    /// misdelivery above for the common case of an OUTBOUND connection
    /// (`connection.addr == evicted_addr`, since an outbound connection's
    /// own address IS its dial target, normally the same as its pin). See
    /// that function's own doc comment for the corrected invariant: an
    /// address-keyed lookup must never resolve `peer_id`'s connection once
    /// `evicted_addr` has changed hands, independent of whether that
    /// address also happens to be the connection's own.
    fn set_configured_peer_addr(
        &self,
        addr: SocketAddr,
        peer_id: &PeerId,
        evicted_addr: Option<SocketAddr>,
    );
}

impl<T: 'static> RoutingPublisher for crate::connection_pool::ConnectionPool<T> {
    fn publish_owner(&self, addr: SocketAddr, peer_id: &PeerId) {
        let _ = self.addr_to_peer_id.upsert_sync(addr, peer_id.clone());
    }

    fn retract_owner(&self, addr: SocketAddr, peer_id: &PeerId) {
        let _ = self
            .addr_to_peer_id
            .remove_if_sync(&addr, |current| current == peer_id);
    }

    fn set_configured_peer_addr(
        &self,
        addr: SocketAddr,
        peer_id: &PeerId,
        evicted_addr: Option<SocketAddr>,
    ) {
        crate::connection_pool::ConnectionPool::set_configured_peer_addr(self, peer_id, addr);
        crate::connection_pool::ConnectionPool::reindex_connection_addr(self, peer_id, addr);
        if let Some(evicted_addr) = evicted_addr {
            crate::connection_pool::ConnectionPool::evict_pin_alias(self, peer_id, evicted_addr);
        }
    }
}

/// The committed result of an atomic `configure_peer` transaction: a
/// `ClaimKind::Verified` claim and its operator pin, decided and published
/// in one serialized owner step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurePeerCommit {
    claim: ClaimCommit,
    /// The address (if any, and if different from the newly pinned one)
    /// this peer was pinned at immediately beforehand.
    evicted_pin: Option<SocketAddr>,
    /// If the eviction above also released that address's ownership --
    /// because this SAME peer still genuinely owned it -- the position of
    /// that release in the owner's commit order. Released in this SAME
    /// synchronous step as the eviction, never as a separate, later command
    /// a concurrent claim or migrate could land in front of.
    evicted_release_seq: Option<CommitSeq>,
    /// This peer's CURRENT `configure_peer_generation` value as of this
    /// SAME atomic transaction -- see that field's own doc comment. The
    /// value a first call must capture and later present back as `expected_
    /// generation` for a retry to be validated against, atomically, at the
    /// owner. Present regardless of `claim`'s own outcome (including
    /// `SupersededByNewerConfiguration` itself, whose caller needs to know
    /// it lost, not just that it did).
    generation: u64,
}

impl ConfigurePeerCommit {
    /// The underlying claim decision.
    pub fn claim(&self) -> &ClaimCommit {
        &self.claim
    }

    /// This peer's `configure_peer_generation` value as of this
    /// transaction -- see [`ConfigurePeerCommit`]'s own doc comment.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// The address evicted from this peer's previous pin, if any.
    pub fn evicted_pin(&self) -> Option<SocketAddr> {
        self.evicted_pin
    }

    /// The evicted address's release position, if this transaction also
    /// released its ownership.
    pub fn evicted_release_seq(&self) -> Option<CommitSeq> {
        self.evicted_release_seq
    }
}

/// Commands accepted by the owner task. Every variant that mutates carries a
/// reply channel so the caller observes the committed state, never a promise.
enum OwnerCommand {
    Claim {
        addr: SocketAddr,
        claim: Claim,
        is_local_addr: bool,
        reply: oneshot::Sender<ClaimCommit>,
    },
    Release {
        addr: SocketAddr,
        /// Only release the exact identity AND claim generation accepted by
        /// the lifecycle callback. A peer identity can reconnect before an
        /// old callback runs, so identity alone is not a sufficient fence.
        expected_owner: PeerId,
        expected_generation: CommitSeq,
        reply: oneshot::Sender<Option<CommitSeq>>,
    },
    Migrate {
        from: SocketAddr,
        to: SocketAddr,
        /// Whether `to` names this registry itself. DNS is external input, so
        /// the serialized owner command must enforce this just like claims do.
        is_local_addr: bool,
        /// Exact source state observed by the caller before submitting the
        /// command. Checked inside the serialized owner task.
        expected_source: SourceExpectation,
        reply: oneshot::Sender<MigrateOutcome>,
    },
    /// A claim tied to one authenticated transport session. Identical to
    /// `Claim` except the receipt bookkeeping for `session_source` -- transfer
    /// of every other still-live session's receipt for this peer+address to
    /// the new generation, then recording this session's own receipt -- is
    /// performed in the SAME synchronous step as the ownership commit, so it
    /// can never be observed or produced half-applied by a concurrent claim
    /// or a concurrent session teardown.
    ClaimConnectionScoped {
        addr: SocketAddr,
        claim: Claim,
        session_source: SocketAddr,
        /// Stable identity of the physical transport instance. The socket
        /// tuple is useful for diagnostics, but it can be reused by a later
        /// connection, so it is never the receipt key on its own.
        connection_instance_id: u64,
        /// Monotonic instant at which the authenticated connection supplied
        /// this evidence, captured before the command entered the owner
        /// mailbox. Sampling in the owner handler would make queued older
        /// evidence look newer than a failure recorded while it waited.
        evidence_at: std::time::Instant,
        reply: oneshot::Sender<ClaimCommit>,
    },
    /// Atomically take every connection-scoped receipt `peer_id` holds for
    /// `session_source`, AND release the ownership of every address no
    /// OTHER live session still covers -- in this SAME command, not as a
    /// set of candidates for a separately-ordered `Release` command to act
    /// on afterward (see `PeerRegistryOwner::release_session`'s doc comment
    /// for why splitting it that way stranded addresses permanently).
    /// Deciding "covered by another session" in this same step, against the
    /// map as it exists after this session's own entries are removed, is
    /// what makes a session exit racing a fresh claim for the same
    /// peer+address resolve consistently rather than stranding a receipt
    /// for the exiting session.
    ReleaseSession {
        peer_id: PeerId,
        session_source: SocketAddr,
        /// The exact physical transport instance being torn down.
        connection_instance_id: u64,
        reply: oneshot::Sender<Vec<(SocketAddr, CommitSeq)>>,
    },
    /// Release everything a peer whose `GossipState` failure evidence looks
    /// dead still holds at `addr`: every connection-scoped receipt recorded
    /// for `peer_id` at `addr` under any session (a missed or
    /// still-in-flight teardown must not leave a ghost behind for a peer
    /// that is never coming back), and the address ownership itself if
    /// `peer_id` still holds it and `addr` is not operator-pinned.
    ///
    /// Refused entirely (no receipts touched, no ownership cleared) if
    /// `addr` has DIRECT evidence of a live owner -- the owner's OWN
    /// `claim_committed_at` record, not any liveness snapshot the caller
    /// took -- that is causally NEWER than `evidence_before`: the instant,
    /// on this same process's monotonic clock, that the failure evidence
    /// the caller's selection is acting on was itself recorded. This is a
    /// causal fence, not a temporal one -- "did direct evidence of life
    /// happen after the evidence of death I'm acting on", not "has enough
    /// time passed since the last commit". A purely elapsed-time check
    /// (whether measured against the commit, or against a generation
    /// snapshot's own submission) is a LEASE: it can be made to expire
    /// simply by the command sitting queued long enough -- behind lock
    /// contention, earlier peers in the same sweep, or actor-table cleanup
    /// work -- even though a reconnect landed, and was itself proven live,
    /// before that queueing delay ever started. A claim causally after the
    /// failure it is being reaped for invalidates the reap permanently,
    /// regardless of how much wall-clock time elapses before this command
    /// actually runs; a claim causally before it (or no direct evidence at
    /// all) never protects the address, no matter how recently the caller's
    /// selection happened to run.
    ReleaseDeadPeer {
        peer_id: PeerId,
        addr: SocketAddr,
        /// The Instant-equivalent of when the `GossipState` failure
        /// evidence this reap is acting on was recorded, computed by the
        /// caller from `PeerInfo::last_failure_time`'s wall-clock age as of
        /// selection time. Fixed at submission time and never re-derived
        /// from "now" inside the owner, so this fence cannot be satisfied
        /// merely by elapsed wall-clock delay before the command runs.
        evidence_before: std::time::Instant,
        reply: oneshot::Sender<DeadPeerReleaseOutcome>,
    },
    /// P1 finding (review round against `7c05e40`, `registry.rs:8824`): a
    /// PURE READ, answering exactly the same causal-fence question
    /// `ReleaseDeadPeer` checks FIRST -- does `addr` have direct evidence
    /// of life (`claim_committed_at`) or ordinary liveness
    /// (`liveness_evidence_at`) causally NEWER than `evidence_before` --
    /// but with NONE of that command's side effects: no
    /// `connection_scoped_claims` purge, no ownership retraction. Brought
    /// back (this shape previously existed, then got fused into
    /// `ReleaseDeadPeer` itself) specifically so `reap_reserved_candidates`
    /// can obtain the "is this candidate still worth destroying at all"
    /// verdict WITHOUT that verdict itself performing the FIRST
    /// destructive step (ownership retraction used to happen inside the
    /// same call that decided whether to destroy, ahead of every
    /// `ReapReservation::is_still_valid()` check the destructive phase
    /// runs) -- see `reap_reserved_candidates`'s own doc comment for the
    /// full ordering this enables. `ReleaseDeadPeer` remains the sole
    /// place ownership is EVER actually retracted, called LAST, after
    /// every other destructive step, behind its own final validity check;
    /// this command exists only to gate ENTRY into that whole sequence
    /// cheaply, before any of it runs.
    HasNewerLivenessEvidence {
        addr: SocketAddr,
        /// Same fence as `ReleaseDeadPeer::evidence_before`.
        evidence_before: std::time::Instant,
        reply: oneshot::Sender<bool>,
    },
    /// NOT an authorization or a fence -- a best-effort MITIGATION. Read
    /// this variant's name literally: it detects whether activity
    /// (liveness evidence, or an operator's own `configure_peer` call) has
    /// committed for this peer since a baseline the caller captured
    /// earlier. It narrows the window in which a stale reap can destroy
    /// actors for a peer that just proved itself alive or was just
    /// reconfigured; it does NOT close that window, because this is a
    /// plain read taken as close as practical to the caller's own
    /// mutation, not a step inside the same serialized commit as that
    /// mutation. See `reap_reserved_candidates`'s own doc comment (Gap A)
    /// for why closing it for real requires moving the mutation itself
    /// into this owner's serialized command stream -- structural work,
    /// tracked separately, not something another read here can achieve.
    ///
    /// P1 finding (review round against `ded8495`, `registry.rs:9508`):
    /// `HasNewerLivenessEvidence` alone answers "has this peer proven
    /// itself alive", but `reap_reserved_candidates`'s fresh pre-
    /// destruction re-check (added the previous round to close a
    /// `DeadPeerReleaseOutcome::ProvenAlive` contract violation) also needs
    /// to catch an OPERATOR reconfiguring this SAME peer (to `addr` or to
    /// anywhere else), entirely independent of liveness -- `configure_peer`
    /// atomically releases a PIN's evicted address's ownership as part of
    /// installing a new one, with no liveness evidence involved at all
    /// (the peer may still genuinely be dead; the operator is simply
    /// repointing it). `try_consume`'s own reservation flag cannot
    /// observe this either, once already consumed (round 5's own,
    /// deliberate, unchanged design) -- so this, like
    /// `HasNewerLivenessEvidence`, is a SEPARATE, additional, pure read
    /// the caller must take fresh, immediately before the irreversible
    /// step it guards, not a substitute for anything already in place, and
    /// not a way to make that step atomic either.
    ///
    /// P1 finding, second pass (review round against `ded8495` itself,
    /// this exact command): the FIRST version of this check answered "is
    /// `addr` currently owned by `peer_id`" -- which produced false
    /// positives for the overwhelmingly common case of a candidate that
    /// was NEVER owner-claimed in the first place (a `GossipState`-only
    /// entry `node_id` merely resolves; ordinary `cleanup_dead_peers`
    /// selection does not require an owner-level claim to exist at all).
    /// `addr_ownership.get(&addr)` reads `None` for such a candidate
    /// regardless of whether anything actually changed, so this used to
    /// treat "never owned to begin with" identically to "was owned, now
    /// isn't" -- aborting perfectly ordinary reaps.
    ///
    /// Fixed by checking `configure_peer_generation` instead of ownership
    /// directly: `baseline_configure_peer_generation`, captured by the
    /// caller BEFORE `try_consume` runs (see `reap_reserved_candidates`'s
    /// own doc comment for the P1 finding on that exact ordering -- an
    /// earlier version of this doc comment said "immediately after
    /// `try_consume` succeeds", which was already stale by the time it was
    /// written), is `peer_id`'s `configure_peer_generation` value AT THAT
    /// INSTANT (see `RegistryOwnerHandle::configure_peer_generation_of`).
    /// If it has since advanced, SOME `configure_peer` call for this SAME
    /// peer_id committed in the window this check exists to shrink --
    /// regardless of whether the candidate was ever pinned, ever owned at
    /// `addr`, or owned anywhere at all before. This is precise where the
    /// ownership check was not: it answers "did an operator reconfigure
    /// THIS peer during this exact window", not "does `addr` currently
    /// look unowned", which can be true for entirely unrelated, benign
    /// reasons.
    ///
    /// PURE READ, no mutation: answers "has liveness evidence newer than
    /// `evidence_before` committed for `addr`, OR has `peer_id`'s
    /// `configure_peer_generation` advanced past `baseline_configure_
    /// peer_generation`" -- either one independently means activity has
    /// been detected since this reap's baseline was captured, and the
    /// caller should abandon this candidate. A `false` reply means no
    /// activity was detected AS OF THIS READ -- it is not, and cannot be,
    /// a guarantee that none commits in the remaining gap between this
    /// reply and the caller's own subsequent mutation.
    ReapBaselineActivityDetected {
        addr: SocketAddr,
        peer_id: PeerId,
        evidence_before: std::time::Instant,
        baseline_configure_peer_generation: u64,
        reply: oneshot::Sender<bool>,
    },
    /// PURE READ, no mutation: `peer_id`'s CURRENT `configure_peer_
    /// generation` value (`0` if this peer has never had a `configure_peer`
    /// call at all). Exists so `reap_reserved_candidates` can capture a
    /// baseline BEFORE `try_consume` runs (see that function's own doc
    /// comment for why that ordering, not "immediately after", is what
    /// this must be captured against), to later present back as
    /// `ReapBaselineActivityDetected`'s own `baseline_configure_peer_
    /// generation` -- see that variant's own doc comment.
    ConfigurePeerGenerationOf {
        peer_id: PeerId,
        reply: oneshot::Sender<u64>,
    },
    /// Atomically check the causal fence `ReleaseDeadPeer` also checks
    /// (does `addr` have DIRECT evidence of a live owner -- a
    /// connection-scoped claim -- causally NEWER than `evidence_before`,
    /// the failure this candidate was selected on?) AND revalidate the
    /// FULL identity the caller's selection observed for `addr` --
    /// ownership (peer id + generation) and operator pin state -- against
    /// the owner's OWN current state, and, only if EVERY check passes,
    /// mark `addr` as reserved for reaping -- see `reap_reserved`'s doc
    /// comment. Returns whether the reservation was granted.
    ///
    /// This supersedes a plain "check, then let the caller act later"
    /// query: a read that only ANSWERS "is it safe right now" and lets
    /// the caller act afterward is stale the instant a concurrent claim
    /// commits in the gap between the read and whatever the caller does
    /// next -- the same class of race this PR keeps finding in every
    /// shape of "observe a fact, then act on it later" it has tried. A
    /// RESERVATION instead gives the caller a fact the owner itself
    /// continues to enforce (via `claim`'s own check) for as long as the
    /// caller holds it, so the caller's later, non-owner destructive work
    /// (actor removal, tombstone emission/gossip, capability/clock state
    /// clearing) can safely run OUTSIDE the owner's critical path without
    /// re-racing a concurrent reconnect. `ReleaseDeadPeer`'s own check at
    /// the end of a sweep is necessary but not sufficient on its own: it
    /// protects ONLY the address-ownership mutation it performs, by which
    /// point a stale candidate's actors, capabilities, clock state, and
    /// their gossiped removal would already be irrecoverable -- unlike a
    /// purely local ownership refusal.
    ///
    /// The causal fence and the identity-match check are BOTH required --
    /// neither subsumes the other, because they protect two DIFFERENT
    /// windows against two DIFFERENT kinds of evidence:
    /// - The causal fence protects the (possibly long) window between the
    ///   FAILURE this candidate was selected on and this command actually
    ///   running, against DIRECT evidence of life: a connection-scoped
    ///   claim can commit well before selection ever runs, while
    ///   `GossipState` still shows the old "failed" verdict (its own
    ///   liveness update only happens AFTER the owner has already
    ///   committed the claim) -- selection would then capture that
    ///   ALREADY-reconnected state as the new "expected" baseline, and an
    ///   identity-match check alone would see nothing has moved SINCE
    ///   selection and wrongly grant the reservation anyway.
    /// - The identity-match check protects the (much narrower) window
    ///   between SELECTION itself and this command running, against ANY
    ///   claim for a DIFFERENT identity: a plain gossip/discovery claim or
    ///   an operator `configure_peer` claiming `addr` for someone else
    ///   deliberately does NOT refresh `claim_committed_at` (see `claim`'s
    ///   own doc comment), so the causal fence alone would not notice a
    ///   new owner has taken the address in that window, and the
    ///   reservation would authorize destructive work against the NEW
    ///   owner's actors, capabilities, and clock state instead of the
    ///   dead peer's.
    ///
    /// If EITHER check fails, the reservation is refused and the sweep
    /// simply skips this candidate, reconsidering it against fresh state
    /// next cycle.
    ///
    /// P1 finding (review round against `a147603`, `registry.rs:8653`):
    /// `expected_ownership`/`expected_pin` prove "this address's owner-side
    /// identity has not moved since selection" -- but the destructive
    /// phase does not act on an `OwnershipToken`, it acts on a `PeerId`
    /// (`node_id`, threaded through separately, sourced from `GossipState`'s
    /// OWN, independently-updated `PeerInfo::node_id` rather than from
    /// either of the values validated here). Nothing tied that `PeerId` to
    /// the identity `expected_ownership`/`expected_pin` describe: if a
    /// NEW claim for `addr` committed while selection's `GossipState` read
    /// of `node_id` and its SEPARATE, lock-free reads of
    /// `ownership_token`/`pin_owner` straddled the change, selection could
    /// capture the OLD failed peer's `node_id` alongside the NEW owner's
    /// (now current, validated-below) token -- and this command would
    /// grant the reservation, since nothing here ever looked at `node_id`
    /// at all.
    ///
    /// `expected_node_id` closes that: checked here, atomically, in the
    /// SAME step that just reconfirmed `expected_ownership`/`expected_pin`
    /// are current -- not as a separate, earlier read, which is the shape
    /// that keeps failing here. See `PeerRegistryOwner::reserve_for_reap`'s
    /// handler for the exact comparison.
    ReserveForReap {
        addr: SocketAddr,
        /// The Instant-equivalent of when the `GossipState` failure
        /// evidence this candidate was selected on was recorded -- see
        /// `ReleaseDeadPeer::evidence_before`'s doc comment, which this
        /// mirrors exactly.
        evidence_before: std::time::Instant,
        /// Ownership (peer id + generation) the caller's selection
        /// observed for `addr`, lock-free, via `RegistryOwnerHandle::
        /// ownership_token`. `None` means `addr` was unowned at
        /// selection -- and must still be unowned now for the
        /// reservation to be granted.
        expected_ownership: Option<OwnershipToken>,
        /// The operator pin owner the caller's selection observed for
        /// `addr`, lock-free, via `RegistryOwnerHandle::pin_owner`.
        /// `None` means `addr` was unpinned at selection -- and must
        /// still be unpinned now.
        expected_pin: Option<PeerId>,
        /// The `PeerId` the caller's destructive phase will act against --
        /// sourced independently of `expected_ownership`/`expected_pin`
        /// (typically `GossipState::PeerInfo::node_id`), and validated
        /// here against them for exactly that reason: whenever
        /// `expected_ownership`/`expected_pin` name a CONCRETE identity
        /// (`Some`), this must name the SAME one, or the reservation would
        /// authorize destructive work keyed to a `PeerId` no longer
        /// connected to this address at all. When ownership and pin are
        /// BOTH `None` (unowned, unpinned), this is unconstrained: `Gossip
        /// State` routinely knows a `node_id` for an address with no
        /// owner-level claim behind it at all (gossip/discovery chatter
        /// about a peer never itself claimed, or an address whose
        /// ownership was independently released while `GossipState`'s own
        /// record lingers) -- legitimate, not evidence of a race, and
        /// there is no ownership-level identity there to be wrong about.
        expected_node_id: Option<PeerId>,
        /// `Some(valid)` when granted -- `valid` is the SAME `Arc<AtomicU8>`
        /// the owner-internal `reap_reserved` map stores for this address,
        /// so the caller's `ReapReservation` guard and the owner's own
        /// entry share one flag. `None` when refused. See
        /// `PeerRegistryOwner::reap_reserved`'s doc comment for why this
        /// exists: a one-time grant/refuse answer is not enough once the
        /// destructive phase needs to keep re-checking validity long after
        /// this reply was sent.
        reply: oneshot::Sender<Option<Arc<AtomicU8>>>,
    },
    /// Release a reservation `ReserveForReap` granted, whether the sweep
    /// used it to reap the address or is abandoning the candidate for
    /// some other reason. Always succeeds (removing an absent key is a
    /// no-op) -- there is nothing to fail closed against.
    ReleaseReapReservation {
        addr: SocketAddr,
        reply: oneshot::Sender<()>,
    },
    /// Atomically install `peer_id`'s operator pin at `addr`, replacing
    /// whatever address (if any) the owner's own peer -> address reverse
    /// map currently shows this peer pinned at -- not merely the address a
    /// caller last observed in `ConnectionPool`, which can be stale by the
    /// time this command runs. See `PeerRegistryOwner::pin`.
    #[cfg(test)]
    Pin {
        addr: SocketAddr,
        peer_id: PeerId,
        reply: oneshot::Sender<Option<SocketAddr>>,
    },
    /// Atomically claim `addr` for `peer_id` with `ClaimKind::Verified` and,
    /// if accepted, install it as `peer_id`'s operator pin -- evicting
    /// whatever address this SAME peer was pinned at beforehand and, in
    /// this SAME synchronous step, releasing that evicted address's
    /// ownership if `peer_id` still holds it.
    ///
    /// This is the atomic transaction `GossipRegistry::configure_peer`
    /// submits in place of separately-ordered claim, pin, and release
    /// commands. Folding the three into one `&mut self` step closes the
    /// interleaving window a concurrent `configure_peer`/claim/migrate
    /// could otherwise exploit between the claim taking effect and the pin
    /// (with its eviction and release) landing -- see
    /// `PeerRegistryOwner::configure_peer`.
    ConfigurePeer {
        addr: SocketAddr,
        peer_id: PeerId,
        /// See `RegistryOwnerHandle::configure_peer`'s own doc comment and
        /// `configure_peer_generation`'s.
        expected_generation: Option<u64>,
        reply: oneshot::Sender<ConfigurePeerCommit>,
    },
    /// `Peer::connect`'s ordinary (non-`configure_peer`) route update,
    /// submitted as an owner command instead of writing `ConnectionPool`
    /// directly from the caller's own task.
    ///
    /// An ordinary connect writes the SAME `ConnectionPool` fields
    /// `RoutingPublisher::set_configured_peer_addr` writes from inside
    /// `install_pin`/`migrate` -- if it wrote them directly, a caller-side
    /// "is this peer pinned elsewhere" read (however published, however
    /// tightly held next to the write) could still be invalidated by a
    /// pin decision the owner commits in the gap, since the two are not
    /// on the same serialization. Submitting this as an owner command
    /// instead means the pin check and the route write are the SAME
    /// serialized step no other owner command can land inside of -- see
    /// `PeerRegistryOwner::set_ordinary_connect_route`.
    ///
    /// `reply` carries whether the write actually happened: `false` means
    /// `peer_id` is operator-pinned to a DIFFERENT address and the write
    /// was declined. The caller MUST consult this -- see
    /// `RegistryOwnerHandle::set_ordinary_connect_route`'s doc comment for
    /// the bug that shipped when an earlier version of this command's
    /// caller discarded it.
    SetOrdinaryConnectRoute {
        peer_id: PeerId,
        addr: SocketAddr,
        reply: oneshot::Sender<bool>,
    },
    #[cfg(test)]
    InspectGeneration {
        addr: SocketAddr,
        reply: oneshot::Sender<Option<CommitSeq>>,
    },
    /// Pure, side-effect-free read of whether `addr` currently has a live
    /// reap reservation held for it -- for deterministically polling, in
    /// tests, exactly when a background sweep's reservation for a
    /// candidate has been granted, without submitting a real (and
    /// therefore side-effecting) claim to probe it indirectly.
    #[cfg(test)]
    IsReapReserved {
        addr: SocketAddr,
        reply: oneshot::Sender<bool>,
    },
    /// Record DIRECT liveness evidence for `addr`, observed at `at` -- see
    /// `RegistryOwnerHandle::note_liveness_evidence`'s doc comment for the
    /// exact standard (an inbound, application-level response actually
    /// received from the peer; never indirect chatter). Routed through
    /// this SAME serialized command stream, not a side channel a caller
    /// writes directly: a lock-free structure the owner merely CONSULTS
    /// has the same check-then-act gap as every other mirror this PR has
    /// found and closed on this project -- `claim_generation`,
    /// `get_required_peer_addr`, the pin token, `pinned_addr`,
    /// `pinned_addr_for`. Routing the WRITE through the owner, not just
    /// the read, is what makes `release_dead_peer`'s check atomic with
    /// this update: whichever of the two commands the owner processes
    /// first is fully committed before the other is even looked at,
    /// because both run inside the same single-threaded `handle()`.
    /// No reply: the caller does not need confirmation the owner
    /// processed it, only that the send is durably enqueued before this
    /// call returns (`mpsc::Sender::send` itself is the ordering
    /// guarantee -- see this variant's caller).
    NoteLivenessEvidence {
        addr: SocketAddr,
        at: std::time::Instant,
    },
}

/// Shared state behind every [`RegistryOwnerHandle`] clone.
struct OwnerShared {
    tx: mpsc::Sender<OwnerCommand>,
    /// Dedicated, UNBOUNDED channel carrying `OwnerCommand::
    /// ReleaseReapReservation` exclusively -- never routed through the
    /// bounded `tx` mailbox above. See `ReapReservation`'s doc comment for
    /// the failure this exists to close: "failing to TAKE a reservation is
    /// safe (the sweep just skips the candidate); failing to RELEASE one is
    /// not (every later claim for that address is refused forever)." A
    /// bounded send can suspend waiting for mailbox capacity, and a task
    /// aborted while suspended there drops the release with it. An
    /// unbounded sender's `send` is synchronous -- it enqueues or reports
    /// the owner gone immediately, with no `.await` point in between for an
    /// abort to land inside -- so by the time it returns, the release is
    /// either irrevocably queued or there is no owner left to leak a
    /// reservation against. This is deliberately NOT unbounded for every
    /// command, only this one: an unbounded queue for ordinary claim/
    /// mutation traffic would let a caller flooding requests grow the
    /// owner's backlog without limit, which the bounded `tx` mailbox's
    /// backpressure exists to prevent. Releases are different: they can
    /// only ever be in flight once per outstanding reservation, so their
    /// worst-case queue depth is already bounded by how many reservations
    /// are concurrently held, not by caller behavior.
    ///
    /// `NoteLivenessEvidence` goes through the ORDINARY bounded `tx`
    /// mailbox below, deliberately, even though it is genuinely
    /// higher-frequency than the claim/ownership traffic that mailbox was
    /// originally sized for: correctness requires it to be serialized
    /// with `ReleaseDeadPeer` on the SAME queue (see
    /// `OwnerCommand::NoteLivenessEvidence`'s own doc comment), and
    /// backpressure on a per-response signal is a feature here, not a
    /// bug -- it naturally sheds load rather than growing an unbounded
    /// backlog the way this comment's own reasoning rejects for the
    /// release channel.
    release_tx: mpsc::UnboundedSender<OwnerCommand>,
    snapshot: Arc<ArcSwap<RoutingSnapshot>>,
    /// Exactly-once start latch. The receiving half plus the publisher live
    /// here until the first command, at which point whichever caller wins the
    /// single-slot pop spawns the task. Registry construction is synchronous
    /// and may run outside a Tokio runtime, so the spawn cannot happen there.
    pending_start: ArrayQueue<StartKit>,
}

struct StartKit {
    rx: mpsc::Receiver<OwnerCommand>,
    release_rx: mpsc::UnboundedReceiver<OwnerCommand>,
    routing: Weak<dyn RoutingPublisher>,
}

/// Cheap, cloneable handle to the single-owner registry actor.
#[derive(Clone)]
pub struct RegistryOwnerHandle {
    shared: Arc<OwnerShared>,
}

impl std::fmt::Debug for RegistryOwnerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistryOwnerHandle")
            .field("owned_addrs", &self.snapshot().len())
            .finish()
    }
}

impl RegistryOwnerHandle {
    /// Create a handle whose task publishes routing through `routing`.
    ///
    /// A `Weak` reference deliberately: the publisher (the connection pool)
    /// transitively owns this handle, and a strong reference here would keep
    /// the pool alive for as long as the owner task runs.
    pub fn new(routing: Weak<dyn RoutingPublisher>) -> Self {
        let (tx, rx) = mpsc::channel(OWNER_MAILBOX_CAPACITY);
        let (release_tx, release_rx) = mpsc::unbounded_channel();
        let pending_start = ArrayQueue::new(1);
        // Cannot fail: the queue was just created with capacity 1.
        let _ = pending_start.push(StartKit {
            rx,
            release_rx,
            routing,
        });
        Self {
            shared: Arc::new(OwnerShared {
                tx,
                release_tx,
                snapshot: Arc::new(ArcSwap::from_pointee(RoutingSnapshot::default())),
                pending_start,
            }),
        }
    }

    /// Record DIRECT liveness evidence for `addr`, observed at `at`: an
    /// inbound, application-level response actually received from the
    /// peer occupying it -- see `mark_response_received`'s own doc
    /// comment for the exact source.
    ///
    /// Submitted through the SAME serialized command stream
    /// `release_dead_peer` reads from -- see `OwnerCommand::
    /// NoteLivenessEvidence`'s own doc comment for why a lock-free side
    /// table the owner merely consulted (an earlier version of this) is
    /// not enough: `release_dead_peer`'s read of it was not atomic with
    /// its own decision, so a response could land between the check and
    /// the ownership removal it was meant to prevent. Routing the WRITE
    /// through the owner closes that -- by the time `release_dead_peer`
    /// runs, either this command already committed (and the release
    /// correctly refuses) or it has not been submitted yet at all (and
    /// there is genuinely nothing to protect against yet); there is no
    /// third possibility where it is "in flight" relative to the check.
    ///
    /// This is deliberately narrow in the SAME way
    /// `PeerRegistryOwner::claim_committed_at` is: only genuinely direct
    /// evidence from the peer itself may advance it. A caller that bumps
    /// this for indirect chatter about a peer (third-party relay,
    /// repeated discovery claims, DNS refresh attempts) would let that
    /// chatter keep a dead peer's address permanently unreapable, exactly
    /// the failure mode `claim_committed_at`'s own doc comment already
    /// rejects for claims.
    ///
    /// No reply is needed: `mpsc::Sender::send` returning is itself the
    /// ordering guarantee (a bounded, single-consumer FIFO channel), and
    /// the caller does not need to know whether the owner has processed
    /// it yet, only that it is durably enqueued before this call
    /// returns.
    pub async fn note_liveness_evidence(&self, addr: SocketAddr, at: std::time::Instant) {
        self.ensure_started();
        let _ = self
            .shared
            .tx
            .send(OwnerCommand::NoteLivenessEvidence { addr, at })
            .await;
    }

    /// Current lock-free ownership/routing snapshot.
    pub fn snapshot(&self) -> Arc<RoutingSnapshot> {
        self.shared.snapshot.load_full()
    }

    /// Lock-free ownership lookup. Never enters the mailbox.
    pub fn owner_of(&self, addr: &SocketAddr) -> Option<Owner> {
        self.shared.snapshot.load().owner(addr).cloned()
    }

    /// Lock-free routing lookup. Never enters the mailbox.
    pub fn routes_to(&self, addr: &SocketAddr) -> Option<PeerId> {
        self.shared.snapshot.load().peer_id(addr).cloned()
    }

    /// Lock-free exact owner-generation lookup.
    pub fn ownership_token(&self, addr: &SocketAddr) -> Option<OwnershipToken> {
        self.shared.snapshot.load().ownership_token(addr)
    }

    /// Lock-free revalidation of one accepted claim generation.
    pub fn claim_is_current(
        &self,
        addr: &SocketAddr,
        owner: &PeerId,
        generation: CommitSeq,
    ) -> bool {
        self.shared
            .snapshot
            .load()
            .claim_is_current(addr, owner, generation)
    }

    /// Lock-free revalidation of an operator pin: is `peer_id` still the
    /// exact identity `addr` is pinned for, per the owner's own pin
    /// decision -- not `ConnectionPool`'s derived `required_addr` (moved by
    /// any `.connect()` call) and not the ownership generation (advanced by
    /// every accepted claim, including unrelated same-identity chatter).
    /// See `RoutingSnapshot::pin_is_current`.
    pub fn pin_is_current(&self, addr: &SocketAddr, peer_id: &PeerId) -> bool {
        self.shared.snapshot.load().pin_is_current(addr, peer_id)
    }

    /// Lock-free reverse lookup: the address, if any, `peer_id` is
    /// CURRENTLY operator-pinned to. See `RoutingSnapshot::pinned_addr_for`.
    pub fn pinned_addr_for(&self, peer_id: &PeerId) -> Option<SocketAddr> {
        self.shared.snapshot.load().pinned_addr_for(peer_id)
    }

    /// Lock-free forward pin lookup: the peer, if any, `addr` is currently
    /// operator-pinned for. See `RoutingSnapshot::pin_owner`. Used by
    /// `cleanup_dead_peers`'s selection step to capture pin state
    /// alongside `ownership_token`, for `reserve_for_reap` to revalidate
    /// atomically -- see `OwnerCommand::ReserveForReap`'s doc comment.
    pub fn pin_owner(&self, addr: &SocketAddr) -> Option<PeerId> {
        self.shared.snapshot.load().pin_owner(addr).cloned()
    }

    /// Submit an address claim and wait for the committed decision.
    ///
    /// CALLERS MUST NOT hold the `gossip_state` guard across this await: the
    /// owner task is a separate task and the guard would serialize every
    /// other registry path behind it for the duration of the round trip.
    /// (The task itself never touches `gossip_state`, so this is a latency
    /// and fairness rule rather than a lock-order cycle — but the rule is
    /// absolute so no future publisher can turn it into a cycle.)
    pub async fn claim(&self, addr: SocketAddr, claim: Claim, is_local_addr: bool) -> ClaimCommit {
        self.ensure_started();
        let (reply, response) = oneshot::channel();
        let command = OwnerCommand::Claim {
            addr,
            claim,
            is_local_addr,
            reply,
        };
        if self.shared.tx.send(command).await.is_err() {
            warn!(addr = %addr, "registry owner unavailable; failing address claim closed");
            return ClaimCommit::Rejected(ClaimRejection::OwnerUnavailable);
        }
        match response.await {
            Ok(commit) => commit,
            Err(_) => {
                warn!(addr = %addr, "registry owner dropped an in-flight claim; failing closed");
                ClaimCommit::Rejected(ClaimRejection::OwnerUnavailable)
            }
        }
    }

    /// Submit a connection-scoped address claim and wait for the committed
    /// decision. Identical contract to [`Self::claim`], except the receipt
    /// bookkeeping for `session_source` commits atomically with the
    /// ownership decision -- see `OwnerCommand::ClaimConnectionScoped`.
    pub async fn claim_connection_scoped(
        &self,
        addr: SocketAddr,
        claim: Claim,
        session_source: SocketAddr,
    ) -> ClaimCommit {
        self.claim_connection_scoped_at(
            addr,
            claim,
            session_source,
            legacy_connection_instance_id(session_source),
            std::time::Instant::now(),
        )
        .await
    }

    /// Submit a connection-scoped claim with the monotonic instant at which
    /// the authenticated connection supplied its liveness evidence.
    /// Production callers capture this before waiting on the owner mailbox;
    /// tests and internal callers that do not have a more precise source may
    /// use [`Self::claim_connection_scoped`].
    pub(crate) async fn claim_connection_scoped_at(
        &self,
        addr: SocketAddr,
        claim: Claim,
        session_source: SocketAddr,
        connection_instance_id: u64,
        evidence_at: std::time::Instant,
    ) -> ClaimCommit {
        self.ensure_started();
        let (reply, response) = oneshot::channel();
        let command = OwnerCommand::ClaimConnectionScoped {
            addr,
            claim,
            session_source,
            connection_instance_id,
            evidence_at,
            reply,
        };
        if self.shared.tx.send(command).await.is_err() {
            warn!(addr = %addr, "registry owner unavailable; failing address claim closed");
            return ClaimCommit::Rejected(ClaimRejection::OwnerUnavailable);
        }
        match response.await {
            Ok(commit) => commit,
            Err(_) => {
                warn!(addr = %addr, "registry owner dropped an in-flight claim; failing closed");
                ClaimCommit::Rejected(ClaimRejection::OwnerUnavailable)
            }
        }
    }

    /// Atomically release every connection-scoped receipt `peer_id` holds
    /// for `session_source` AND retract the ownership of every address no
    /// other live session still covers, in the SAME owner command -- see
    /// `OwnerCommand::ReleaseSession` and `PeerRegistryOwner::release_session`
    /// for why this must not be split into "find candidates" plus a
    /// separately-ordered `release` call. Returns the addresses actually
    /// released, paired with the resulting commit sequence -- callers
    /// tombstone their own `gossip_state` projection at that sequence, the
    /// same as `release_dead_peer`'s callers do. An unreachable owner
    /// reports nothing released: fail closed, the same as every other
    /// command here.
    pub async fn release_session(
        &self,
        peer_id: PeerId,
        session_source: SocketAddr,
    ) -> Vec<(SocketAddr, CommitSeq)> {
        self.release_session_for_instance(
            peer_id,
            session_source,
            legacy_connection_instance_id(session_source),
        )
        .await
    }

    /// Release receipts for one exact physical transport instance.
    pub(crate) async fn release_session_for_instance(
        &self,
        peer_id: PeerId,
        session_source: SocketAddr,
        connection_instance_id: u64,
    ) -> Vec<(SocketAddr, CommitSeq)> {
        self.ensure_started();
        let (reply, response) = oneshot::channel();
        let command = OwnerCommand::ReleaseSession {
            peer_id,
            session_source,
            connection_instance_id,
            reply,
        };
        if self.shared.tx.send(command).await.is_err() {
            return Vec::new();
        }
        response.await.unwrap_or_default()
    }

    /// Release everything `peer_id` still holds at `addr` -- every
    /// connection-scoped receipt recorded for it under any session, and the
    /// address ownership itself if `peer_id` still holds it and `addr` is not
    /// operator-pinned -- but ONLY if `addr` has no direct OR ordinary
    /// liveness evidence causally newer than `evidence_before`; otherwise a
    /// no-op. See `OwnerCommand::ReleaseDeadPeer` and
    /// `DeadPeerReleaseOutcome`.
    pub async fn release_dead_peer(
        &self,
        peer_id: PeerId,
        addr: SocketAddr,
        evidence_before: std::time::Instant,
    ) -> DeadPeerReleaseOutcome {
        self.ensure_started();
        let (reply, response) = oneshot::channel();
        let command = OwnerCommand::ReleaseDeadPeer {
            peer_id,
            addr,
            evidence_before,
            reply,
        };
        if self.shared.tx.send(command).await.is_err() {
            // Fail-closed: see `DeadPeerReleaseOutcome::ProvenAlive`'s own
            // doc comment.
            return DeadPeerReleaseOutcome::ProvenAlive;
        }
        response
            .await
            .unwrap_or(DeadPeerReleaseOutcome::ProvenAlive)
    }

    /// A PURE READ of the same causal fence `release_dead_peer` checks
    /// first -- `true` means `addr` has direct or ordinary liveness
    /// evidence causally newer than `evidence_before` (this candidate is
    /// proven alive). Unlike `release_dead_peer`, this performs no
    /// mutation whatsoever: no receipt purge, no ownership change. See
    /// `OwnerCommand::HasNewerLivenessEvidence`'s own doc comment for why
    /// this exists separately.
    ///
    /// Fail-CLOSED like every other command here: an unreachable owner
    /// reports `true` (behave as if proven alive -- "cannot prove it is
    /// safe [to destroy], so don't"), the same direction `release_dead_peer`
    /// itself takes when it cannot be reached.
    pub async fn has_newer_liveness_evidence(
        &self,
        addr: SocketAddr,
        evidence_before: std::time::Instant,
    ) -> bool {
        self.ensure_started();
        let (reply, response) = oneshot::channel();
        let command = OwnerCommand::HasNewerLivenessEvidence {
            addr,
            evidence_before,
            reply,
        };
        if self.shared.tx.send(command).await.is_err() {
            return true;
        }
        response.await.unwrap_or(true)
    }

    /// A PURE READ combining `has_newer_liveness_evidence` with an
    /// independent, fresh check that `peer_id`'s `configure_peer_
    /// generation` has not advanced past `baseline_configure_peer_
    /// generation` -- `true` means activity (liveness evidence, or an
    /// operator's own `configure_peer` call) has been detected for this
    /// exact (addr, peer_id) pair since the caller's baseline was
    /// captured, for EITHER reason.
    ///
    /// NOT an authorization check and NOT a fence -- a best-effort
    /// MITIGATION, same as `OwnerCommand::ReapBaselineActivityDetected`'s
    /// own doc comment says. It is a plain read taken as close as
    /// practical to the caller's own subsequent mutation, not a step
    /// inside the same serialized commit as that mutation, so it narrows
    /// the destructive window without closing it: activity that commits
    /// after this call returns but before the caller's mutation runs is
    /// invisible to it. See `GossipRegistry::reap_reserved_candidates`'s
    /// own doc comment (Gap A) for why closing this for real requires
    /// moving the mutation itself into this owner's serialized command
    /// stream, and why that is tracked as separate structural work rather
    /// than attempted here.
    ///
    /// See `OwnerCommand::ReapBaselineActivityDetected`'s own doc comment
    /// (and its second-pass history) for the P1 finding this mitigates:
    /// liveness evidence alone does not catch an operator's own
    /// `configure_peer` reconfiguring this peer, since that can commit
    /// with no liveness evidence involved at all -- and a raw ownership
    /// check produced false positives for the common case of a candidate
    /// never owner-claimed in the first place, which the generation
    /// counter does not.
    ///
    /// `baseline_configure_peer_generation`: capture via [`Self::
    /// configure_peer_generation_of`] BEFORE `try_consume` runs, not after
    /// -- see `GossipRegistry::reap_reserved_candidates`'s own doc comment
    /// for the P1 finding on exactly that ordering.
    ///
    /// Fail-CLOSED like every other command here: an unreachable owner
    /// reports `true` (behave as if activity was detected -- "cannot
    /// prove nothing has changed, so don't proceed"), the same direction
    /// `has_newer_liveness_evidence` and `release_dead_peer` both take.
    pub async fn reap_baseline_activity_detected(
        &self,
        addr: SocketAddr,
        peer_id: PeerId,
        evidence_before: std::time::Instant,
        baseline_configure_peer_generation: u64,
    ) -> bool {
        self.ensure_started();
        let (reply, response) = oneshot::channel();
        let command = OwnerCommand::ReapBaselineActivityDetected {
            addr,
            peer_id,
            evidence_before,
            baseline_configure_peer_generation,
            reply,
        };
        if self.shared.tx.send(command).await.is_err() {
            return true;
        }
        response.await.unwrap_or(true)
    }

    /// PURE READ: `peer_id`'s CURRENT `configure_peer_generation` value
    /// (`0` if it has never had a `configure_peer` call at all). Intended
    /// use is capturing a baseline BEFORE a `reap_reserved_candidates`
    /// candidate's `try_consume` runs (see that function's own doc comment
    /// for the P1 finding on that ordering), to later present back as
    /// [`Self::reap_baseline_activity_detected`]'s own `baseline_
    /// configure_peer_generation` -- see that method's own doc comment for
    /// why this is a mitigation, not a closing check.
    ///
    /// Fail-CLOSED like every other command here, in spirit if not in the
    /// literal sense: an unreachable owner reports `u64::MAX`, a baseline
    /// no REAL generation could ever be captured as `<=` -- so the later
    /// `reap_baseline_activity_detected` call this feeds is guaranteed to
    /// see `current_generation > baseline` as false regardless of what the
    /// owner (if it recovers) later reports, deferring entirely to that
    /// call's OWN fail-closed default (`true`, activity detected) rather
    /// than this one accidentally manufacturing a false-positive verdict
    /// from an owner that was merely unreachable for one specific query.
    pub async fn configure_peer_generation_of(&self, peer_id: PeerId) -> u64 {
        self.ensure_started();
        let (reply, response) = oneshot::channel();
        let command = OwnerCommand::ConfigurePeerGenerationOf { peer_id, reply };
        if self.shared.tx.send(command).await.is_err() {
            return u64::MAX;
        }
        response.await.unwrap_or(u64::MAX)
    }

    /// Atomically check the causal fence against `evidence_before` AND
    /// revalidate the identity the caller's selection observed for `addr`
    /// -- `expected_ownership`, `expected_pin`, and that `expected_node_id`
    /// corresponds to them -- against the owner's own current state, and,
    /// only if EVERY check still passes, reserve `addr` for reaping -- see
    /// `OwnerCommand::ReserveForReap`'s doc comment for why the causal
    /// fence and the identity checks are all required; none alone closes
    /// every window. Returns a [`ReapReservation`] guard when granted,
    /// `None` when refused. Fail-CLOSED like every other command here: an
    /// unreachable owner reports `None` (not granted / don't proceed), the
    /// same "cannot prove it is safe, so don't" direction `release_dead_peer`
    /// itself takes when it cannot be reached.
    ///
    /// `expected_node_id` must be the `PeerId` the caller's destructive
    /// phase will act against once granted (typically `GossipState::
    /// PeerInfo::node_id`, sourced independently of `expected_ownership`/
    /// `expected_pin`) -- see `OwnerCommand::ReserveForReap`'s own doc
    /// comment for the finding this closes: without this check, a
    /// reservation could be granted for a candidate whose `node_id` no
    /// longer corresponds to the address's actual current owner, and the
    /// destructive phase would run against whichever peer that stale
    /// `node_id` happens to still resolve to instead of the dead one.
    ///
    /// The returned guard is what makes a granted reservation impossible to
    /// leak: releasing it is normally an explicit, awaited
    /// `ReapReservation::release()` call once the sweep's destructive work
    /// for `addr` has actually finished, but if the guard is instead
    /// dropped without that call -- because the task holding it was hard-
    /// aborted mid-sweep, not merely raced past in a `select!` -- its `Drop`
    /// impl still releases the reservation, best-effort. See
    /// `ReapReservation`'s doc comment for why a plain `bool` (as this
    /// method returned before) cannot provide that guarantee: nothing forces
    /// a caller holding a bare `true` to ever call the matching release, and
    /// nothing runs on its behalf if the caller's task ends without doing
    /// so.
    pub async fn reserve_for_reap(
        &self,
        addr: SocketAddr,
        evidence_before: std::time::Instant,
        expected_ownership: Option<OwnershipToken>,
        expected_pin: Option<PeerId>,
        expected_node_id: Option<PeerId>,
    ) -> Option<ReapReservation> {
        self.ensure_started();
        let (reply, response) = oneshot::channel();
        let command = OwnerCommand::ReserveForReap {
            addr,
            evidence_before,
            expected_ownership,
            expected_pin,
            expected_node_id,
            reply,
        };
        if self.shared.tx.send(command).await.is_err() {
            return None;
        }
        let valid = response.await.ok().flatten()?;
        Some(ReapReservation {
            owner: self.clone(),
            addr,
            released: false,
            valid,
        })
    }

    /// Enqueue an `OwnerCommand::ReleaseReapReservation` for `addr` on the
    /// dedicated, unbounded release channel -- see `OwnerShared::
    /// release_tx`'s doc comment. Deliberately synchronous, not `async`:
    /// `UnboundedSender::send` cannot suspend on capacity, and has no
    /// `.await` point inside it for a task abort to land in the middle of,
    /// so by the time this call returns, the release is either irrevocably
    /// queued -- `Some`, carrying the reply receiver for a caller that can
    /// afford to await confirmation the owner actually processed it -- or
    /// the owner task is already gone (`None`), in which case there is
    /// nothing left to release against. Callable from both the async
    /// `ReapReservation::release` and its synchronous `Drop` impl for
    /// exactly this reason.
    fn enqueue_reap_release(&self, addr: SocketAddr) -> Option<oneshot::Receiver<()>> {
        self.ensure_started();
        let (reply, response) = oneshot::channel();
        self.shared
            .release_tx
            .send(OwnerCommand::ReleaseReapReservation { addr, reply })
            .ok()?;
        Some(response)
    }

    /// Reserve `addr` for `peer_id` independently of any connection,
    /// atomically replacing any address this peer was previously pinned at.
    /// Returns the evicted address, if this peer held a DIFFERENT pin
    /// beforehand -- the caller's cue to also release that address's
    /// ownership.
    ///
    /// The lower-level pin-bookkeeping primitive `configure_peer` (below)
    /// is now built on: it does NOT itself verify that `peer_id` actually
    /// owns `addr` (or, for the evicted address, release its ownership) --
    /// only `pinned_by_peer`/`operator_pinned`/the `ConnectionPool` route
    /// are touched here. `GossipRegistry::configure_peer` no longer calls
    /// this directly for that reason: claiming and pinning as two
    /// separately-ordered commands left a window for another command to
    /// land in between, observing (or acting on) a pin with no matching
    /// claim. Kept as its own command for the reverse-map invariant it
    /// guarantees in isolation (see the concurrent-pin tests below); any
    /// future caller must claim ownership first in the SAME atomic step --
    /// i.e. use `configure_peer` -- rather than calling this directly.
    ///
    /// Two concurrent callers for the same peer can each observe the same
    /// stale "previous address" from `ConnectionPool` before either has
    /// applied its own change; if each then independently pinned its own
    /// target, both addresses would end up pinned for one peer, and since a
    /// pinned address can never be reclaimed by `release`/`release_dead_peer`,
    /// the loser would stay reserved forever. Routing the replacement
    /// through the owner's own reverse map instead -- looked up here, at
    /// the moment this command actually runs, rather than trusted from the
    /// caller -- means whichever `pin` command the owner serializes LAST
    /// always wins outright, and there is never a window in which two
    /// addresses are simultaneously pinned for the same peer.
    #[cfg(test)]
    pub(crate) async fn pin(&self, addr: SocketAddr, peer_id: PeerId) -> Option<SocketAddr> {
        self.ensure_started();
        let (reply, response) = oneshot::channel();
        let command = OwnerCommand::Pin {
            addr,
            peer_id,
            reply,
        };
        if self.shared.tx.send(command).await.is_err() {
            return None;
        }
        response.await.unwrap_or(None)
    }

    /// Atomically claim `addr` for `peer_id` (`ClaimKind::Verified`) and
    /// install it as `peer_id`'s operator pin, evicting and releasing any
    /// previous pin for this SAME peer in the same synchronous step. See
    /// `OwnerCommand::ConfigurePeer` and `PeerRegistryOwner::configure_peer`.
    ///
    /// `expected_generation`: `None` for a peer's FIRST `configure_peer`
    /// call (always applies, and bumps `configure_peer_generation` to a
    /// new value, returned via [`ConfigurePeerCommit::generation`]);
    /// `Some(generation)` for a retry presenting a value a PRIOR call
    /// already established -- rejected outright, atomically, with
    /// `ClaimRejection::SupersededByNewerConfiguration`, if a NEWER call
    /// for the same peer has bumped the generation further in the
    /// meantime. See `configure_peer_generation`'s own doc comment for
    /// the P1 finding this closes.
    ///
    /// An owner-unavailable send failure reports a rejected claim with no
    /// eviction, the same fail-closed shape as every other command here;
    /// `generation` in that case is a placeholder (`0`), never meaningful
    /// to present back as `expected_generation` since `OwnerUnavailable`
    /// is not retried by any caller.
    pub async fn configure_peer(
        &self,
        addr: SocketAddr,
        peer_id: PeerId,
        expected_generation: Option<u64>,
    ) -> ConfigurePeerCommit {
        self.ensure_started();
        let (reply, response) = oneshot::channel();
        let command = OwnerCommand::ConfigurePeer {
            addr,
            peer_id,
            expected_generation,
            reply,
        };
        if self.shared.tx.send(command).await.is_err() {
            warn!(addr = %addr, "registry owner unavailable; failing configure_peer closed");
            return ConfigurePeerCommit {
                claim: ClaimCommit::Rejected(ClaimRejection::OwnerUnavailable),
                evicted_pin: None,
                evicted_release_seq: None,
                generation: 0,
            };
        }
        response.await.unwrap_or(ConfigurePeerCommit {
            claim: ClaimCommit::Rejected(ClaimRejection::OwnerUnavailable),
            evicted_pin: None,
            evicted_release_seq: None,
            generation: 0,
        })
    }

    /// `Peer::connect`'s ordinary route update, submitted as an owner
    /// command so the pin-conflict check and the `ConnectionPool` write
    /// share the owner's own serialization instead of racing it -- see
    /// `OwnerCommand::SetOrdinaryConnectRoute` and
    /// `PeerRegistryOwner::set_ordinary_connect_route`.
    ///
    /// Returns whether `addr` actually became (or already was) the
    /// effective route: `false` when the owner declined it -- `peer_id` is
    /// operator-pinned to a DIFFERENT address -- or when the owner is
    /// unreachable (fail-closed: cannot prove the write happened, so
    /// treat it as not having happened).
    ///
    /// CALLERS MUST CONSULT THIS. An earlier version returned `()` and
    /// `connect_with_route_mode` discarded the result entirely: on a
    /// decline, it still unconditionally inserted the requested address
    /// into `gossip_state`, still dialed (`connect_to_peer` uses
    /// `ConnectionPool`'s required/configured route, which the decline
    /// left pointing at the PIN's address, not the requested one), and on
    /// that dial's success still marked the REQUESTED address healthy and
    /// gossiped it -- advertising a route this node never actually
    /// connected to, reachable any time an ordinary `.connect()` named an
    /// address other than a peer's current pin.
    pub async fn set_ordinary_connect_route(&self, peer_id: PeerId, addr: SocketAddr) -> bool {
        self.ensure_started();
        let (reply, response) = oneshot::channel();
        let command = OwnerCommand::SetOrdinaryConnectRoute {
            peer_id,
            addr,
            reply,
        };
        if self.shared.tx.send(command).await.is_err() {
            return false;
        }
        response.await.unwrap_or(false)
    }

    /// Drop the recorded ownership of `addr` only when both `expected_owner`
    /// and `expected_generation` still match its latest accepted claim.
    ///
    /// Returns the release's position in the commit order when an entry was
    /// actually removed, so the caller can fence its own address-keyed state
    /// at that position: a claim that committed BEFORE the release must not
    /// be able to project peer or connection state back onto an address the
    /// release has since vacated.
    pub async fn release(
        &self,
        addr: SocketAddr,
        expected_owner: PeerId,
        expected_generation: CommitSeq,
    ) -> Option<CommitSeq> {
        self.ensure_started();
        let (reply, response) = oneshot::channel();
        let command = OwnerCommand::Release {
            addr,
            expected_owner,
            expected_generation,
            reply,
        };
        if self.shared.tx.send(command).await.is_err() {
            return None;
        }
        response.await.unwrap_or(None)
    }

    /// Move ownership of `from` onto `to` (address re-resolution).
    ///
    /// Callers that also re-key their own address-keyed state must issue this
    /// FIRST and act on the outcome, because only the owner task can tell
    /// whether the destination has meanwhile been claimed by someone else. An
    /// unreachable owner is reported as blocked: without a committed decision
    /// no address-keyed move may proceed.
    ///
    /// `expected_source` pins either exact owner+generation or exact unowned
    /// state. A competing claim/refresh between the caller's observation and
    /// this command therefore refuses the move rather than carrying the wrong
    /// lifecycle generation onto the destination.
    pub async fn migrate(
        &self,
        from: SocketAddr,
        to: SocketAddr,
        expected_source: SourceExpectation,
        is_local_addr: bool,
    ) -> MigrateOutcome {
        self.ensure_started();
        let (reply, response) = oneshot::channel();
        let command = OwnerCommand::Migrate {
            from,
            to,
            expected_source,
            is_local_addr,
            reply,
        };
        if self.shared.tx.send(command).await.is_err() {
            warn!(from = %from, to = %to, "registry owner unavailable; failing address migration closed");
            return MigrateOutcome::TargetOwnedByOther;
        }
        response.await.unwrap_or(MigrateOutcome::TargetOwnedByOther)
    }

    /// Spawn the owner task on first use. The single-slot queue makes this
    /// exactly-once without a lock: losers of the race simply proceed and
    /// their command waits in the mailbox until the winner's task drains it.
    fn ensure_started(&self) {
        if let Some(StartKit {
            rx,
            release_rx,
            routing,
        }) = self.shared.pending_start.pop()
        {
            let owner = PeerRegistryOwner {
                addr_ownership: HashMap::new(),
                claim_generation: HashMap::new(),
                claim_committed_at: HashMap::new(),
                liveness_evidence_at: HashMap::new(),
                configure_peer_generation: HashMap::new(),
                connection_scoped_claims: HashMap::new(),
                operator_pinned: HashMap::new(),
                pinned_by_peer: HashMap::new(),
                reap_reserved: HashMap::new(),
                snapshot: Arc::clone(&self.shared.snapshot),
                routing,
                commit_seq: 0,
            };
            tokio::spawn(owner.run(rx, release_rx));
        }
    }

    /// Take the receiving half before the task is ever spawned, simulating an
    /// owner that is gone so the fail-closed path can be exercised.
    #[cfg(test)]
    fn simulate_owner_gone(&self) {
        let _ = self.shared.pending_start.pop();
    }

    /// Observe the authoritative generation in deterministic race tests.
    /// Production lifecycle paths retain the generation returned by their
    /// own claim and never perform a racy read-back.
    #[cfg(test)]
    pub(crate) async fn claim_generation_for_test(&self, addr: SocketAddr) -> Option<CommitSeq> {
        self.ensure_started();
        let (reply, response) = oneshot::channel();
        self.shared
            .tx
            .send(OwnerCommand::InspectGeneration { addr, reply })
            .await
            .ok()?;
        response.await.ok().flatten()
    }

    /// Pure, side-effect-free read of whether `addr` currently has a live
    /// reap reservation held for it. See `OwnerCommand::IsReapReserved`'s
    /// own doc comment for why: deterministically polling for exactly
    /// when a background sweep's reservation for a candidate has been
    /// granted, without submitting a real, side-effecting claim to probe
    /// it indirectly.
    #[cfg(test)]
    pub(crate) async fn is_reap_reserved_for_test(&self, addr: SocketAddr) -> bool {
        self.ensure_started();
        let (reply, response) = oneshot::channel();
        if self
            .shared
            .tx
            .send(OwnerCommand::IsReapReserved { addr, reply })
            .await
            .is_err()
        {
            return false;
        }
        response.await.unwrap_or(false)
    }
}

/// RAII guard for one reservation `RegistryOwnerHandle::reserve_for_reap`
/// granted. This is what upgrades "the owner enforces this fact for as long
/// as the caller holds it" from a documented obligation (the previous
/// `bool`-returning shape) into something the type system makes hard to get
/// wrong: nothing about a bare `true` stops a caller from losing track of
/// it, forgetting the matching release, or having its task end -- panic,
/// early return, or a hard `JoinHandle::abort()` -- before reaching it.
///
/// The intended path is `release()`: an explicit, awaited owner round trip
/// once the sweep's destructive work for the reserved address has actually
/// finished, giving the same guarantee a direct release command always
/// gave -- the release is fully committed, in order, before the caller
/// proceeds. `cleanup_dead_peers` is `select!`-cancellation-safe (a chosen
/// arm's body, including this one, runs to completion; only sibling arms
/// are dropped -- see its own doc comment), so under ordinary operation
/// `release()` is the only path this guard's release ever takes.
///
/// The `Drop` impl exists for the one case that is NOT
/// cancellation-through-`select!`: a genuine hard abort of the task holding
/// this guard mid-sweep. `GossipRegistryHandle::shutdown` and
/// `shutdown_and_wait` both `JoinHandle::abort()` the exact task that runs
/// `cleanup_dead_peers` (the periodic timer loop), and `Drop for
/// GossipRegistryHandle` does the same if a caller drops the handle without
/// an explicit shutdown -- all three reachable in ordinary operation, not
/// exotic edge cases. An abort drops the task's future in place, the same
/// as dropping any other value on the stack, which runs this guard's `Drop`
/// impl synchronously.
///
/// Both paths, `release()` and `Drop`, enqueue through
/// `RegistryOwnerHandle::enqueue_reap_release` -- the dedicated, UNBOUNDED
/// release channel (`OwnerShared::release_tx`), never the bounded `tx`
/// mailbox ordinary commands use. This is what closes a real bug an
/// earlier version of this guard had: `release()` used to disarm
/// (`released = true`) and only THEN `.await` a send on the bounded
/// mailbox. If the task was aborted while that send was suspended waiting
/// for mailbox capacity, the future -- `released` already `true` -- was
/// dropped mid-await, `Drop` saw `released == true` and did nothing, and
/// the `try_send` fallback it would otherwise have attempted could itself
/// fail on that same full mailbox. The exact leak this guard exists to
/// prevent, reappearing through the guard's own failure path. The
/// governing asymmetry: failing to TAKE a reservation is safe --
/// `reserve_for_reap` returning `None` just means the sweep skips that
/// candidate this round -- but failing to RELEASE one is not, since every
/// later claim for that address is refused forever. An unbounded sender's
/// `send` cannot suspend on capacity and has no `.await` point inside it
/// for an abort to land in the middle of, so by the time it returns, the
/// release is either irrevocably queued or the owner task is already gone
/// -- and in the latter case there is nothing left to leak a reservation
/// against, since a fresh owner task starts with an empty `reap_reserved`.
/// `released` is now set only strictly AFTER that synchronous enqueue
/// succeeds, never before it -- see `release()` below.
///
/// P1 finding (review round against `d4000e2`, `registry.rs:8750`): a
/// reservation blocks new ownership CLAIMS, never ordinary liveness
/// evidence on an already-established connection -- and the one-time
/// verdict `release_dead_peer_ownership` returns is a snapshot, obtained
/// through an `.await`, stale the instant it returns. Evidence proving the
/// candidate alive can still commit in the window between that verdict
/// returning and the destructive phase's own irreversible steps -- actor
/// removal, `ActorRemoved` tombstone emission -- actually running.
/// `valid` (below) is a shared, lock-free `Arc<AtomicU8>`, the same state
/// stored by `PeerRegistryOwner::reap_reserved` for this address. It starts
/// as `REAP_PENDING`; owner-side liveness evidence can transition it to
/// `REAP_INVALIDATED` before the sweep commits. `try_consume` performs the
/// single `REAP_PENDING -> REAP_COMMITTED` compare-and-swap that linearizes
/// the entire reap. The caller then performs all actor, tombstone, side-table
/// and ownership work for that candidate under that committed decision.
/// Evidence processed after the CAS is ordered after the reap and cannot make
/// ownership return `ProvenAlive` after actors were already removed.
///
/// `is_still_valid` remains a cheap diagnostic peek. It is useful before the
/// commit point, but it is not a second authorization: once the CAS commits,
/// the decision is final; once owner evidence invalidates the reservation, the
/// CAS fails. Pin-changing configuration is rejected while the previous pin
/// is reserved, so it cannot invalidate a reap after the destructive decision
/// has crossed its linearization point.
pub struct ReapReservation {
    owner: RegistryOwnerHandle,
    addr: SocketAddr,
    released: bool,
    valid: Arc<AtomicU8>,
}

impl ReapReservation {
    /// Cheap, synchronous, no `.await`: `false` while DIRECT liveness
    /// evidence for this reservation's address has invalidated the pending
    /// reservation through the owner's serialized command stream
    /// (`note_liveness_evidence`). This is a diagnostic view of the pending
    /// state; `try_consume` is the only authorization for the destructive
    /// sequence.
    pub fn is_still_valid(&self) -> bool {
        self.valid.load(Ordering::Acquire) == REAP_PENDING
    }

    /// One-shot, race-free authorization to perform this reservation's
    /// complete destructive sequence: side tables, actors, tombstones and
    /// ownership. It is a compare-and-swap from `REAP_PENDING` to
    /// `REAP_COMMITTED` on the same `Arc<AtomicU8>` that
    /// [`Self::is_still_valid`] reads, not a second read of it. `true` means
    /// this call crossed the reap's linearization point; later owner evidence
    /// is ordered after it and cannot produce an inconsistent ownership
    /// verdict. `false` means owner-side evidence invalidated the pending
    /// reservation first, so the caller must abandon this candidate.
    ///
    /// Call this exactly once per candidate as the single gate for its whole
    /// sequence; do not treat a later diagnostic peek as a second
    /// authorization.
    pub fn try_consume(&self) -> bool {
        self.valid
            .compare_exchange(
                REAP_PENDING,
                REAP_COMMITTED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Release this reservation. The normal path: call once the sweep's
    /// destructive work for this address has actually finished.
    pub async fn release(mut self) {
        match self.owner.enqueue_reap_release(self.addr) {
            Some(response) => {
                // Durably enqueued the instant `enqueue_reap_release`
                // returned, above -- see this type's doc comment. Disarm
                // NOW, before awaiting the reply below: even if this task
                // is aborted while awaiting it, the release has already
                // happened (or will, regardless of this task's fate), so
                // `Drop` firing anyway would only be a redundant, harmless
                // resend -- but disarming here avoids even that.
                self.released = true;
                let _ = response.await;
            }
            None => {
                warn!(
                    addr = %self.addr,
                    "reap reservation release found the owner task already gone; nothing to \
                     release"
                );
            }
        }
    }
}

impl Drop for ReapReservation {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        // Best-effort fallback for a hard task abort -- see this type's
        // doc comment. Synchronous, like `enqueue_reap_release` itself:
        // `Drop::drop` cannot `.await`, but nothing here needs to.
        if self.owner.enqueue_reap_release(self.addr).is_none() {
            warn!(
                addr = %self.addr,
                "reap reservation guard dropped without releasing -- owner task already gone; \
                 nothing to release"
            );
        }
    }
}

/// The single writer. Owns `addr_ownership` outright — no `Arc`, no mutex, no
/// interior mutability — so `&mut self` alone proves exclusivity.
struct PeerRegistryOwner {
    addr_ownership: HashMap<SocketAddr, Owner>,
    /// Latest accepted claim generation for each owned address. This is
    /// lifecycle fencing metadata, not part of the arbitration truth table.
    claim_generation: HashMap<SocketAddr, CommitSeq>,
    /// When each owned address last had DIRECT evidence of a live owner --
    /// an outbound dial this node completed, or an authenticated inbound
    /// session (refreshed only by `claim_connection_scoped`, carried
    /// unchanged rather than refreshed by `migrate`, and never touched by
    /// the plain `claim` command gossip/discovery claims also go through).
    /// This is the owner's OWN, self-contained notion of "how recently was
    /// this address touched by something that actually proves liveness",
    /// independent of `GossipState`'s `failures`/`last_failure_time`
    /// bookkeeping -- which a reconnect only updates AFTER the owner has
    /// already committed the fresh claim that proves the peer alive, and
    /// which lives behind a different lock entirely. `release_dead_peer`
    /// checks this instead of trusting any liveness snapshot a caller took
    /// from that other domain, so it can never be fooled by a reconnect
    /// whose claim commit and whose `GossipState` update straddle the
    /// caller's own observation in either order -- and, because only direct
    /// evidence refreshes it, it also cannot be kept perpetually "fresh" by
    /// indirect chatter (repeated gossip/discovery claims, or DNS refresh
    /// attempts) about a peer nothing has actually reconnected to.
    ///
    /// Deliberately narrow to NEW-CONNECTION/claim evidence, not ongoing
    /// liveness on a connection already claimed: see `liveness_evidence_at`
    /// below for the complementary fence `release_dead_peer` ALSO checks,
    /// covering exactly the gap this field alone leaves -- an
    /// already-claimed connection that keeps delivering ordinary responses
    /// never commits another claim, so `claim_committed_at` never advances
    /// for it again.
    claim_committed_at: HashMap<SocketAddr, std::time::Instant>,
    /// Connection-scoped ownership receipts: which live authenticated
    /// sessions currently back a peer's claim on an address, and at what
    /// owner generation. Keyed by `(peer, session_source, addr)` --
    /// `connection_instance_id` is the exact physical connection's stable
    /// discriminator. `session_source` remains in the command/API for
    /// diagnostics and the legacy synthetic wrapper, but is deliberately not
    /// part of the authoritative identity: sequential sockets can reuse the
    /// same tuple.
    ///
    /// Lives here, alongside `claim_generation`, rather than in a
    /// separately-synchronized map the way PR #178 first shipped it: every
    /// mutation below happens from `&mut self` in the same synchronous
    /// command as the ownership commit or release it corresponds to, so a
    /// receipt can never be observed (or left behind) at a generation the
    /// owner authority does not simultaneously agree is current.
    connection_scoped_claims: HashMap<(PeerId, u64, SocketAddr), CommitSeq>,
    /// Addresses reserved by an explicit `GossipRegistry::configure_peer`
    /// call, independent of any connection. A pinned address is invisible to
    /// `claim_connection_scoped`'s receipt bookkeeping (no receipt is ever
    /// recorded for it) and refused by `release`/`release_dead_peer`
    /// (checked directly, not merely inferred from the absence of a
    /// receipt): a session that happens to authenticate the same identity
    /// at a pinned address must not be able to make the reservation
    /// releasable merely by connecting and later disconnecting. Distinct
    /// from `ConnectionPool`'s `required_addr` (the supervisor's
    /// keep-retrying-this-dial-target bookkeeping, set by every `.connect()`
    /// call, configured or not): conflating the two was what let an
    /// ordinary, non-configured dial's address become permanently
    /// undisplaceable once its peer's session ended.
    operator_pinned: HashMap<SocketAddr, PeerId>,
    /// Reverse index of `operator_pinned`: the address (if any) each peer is
    /// currently pinned at. `pin` looks a peer up here -- never trusts a
    /// caller-supplied "previous address" -- so installing a new pin always
    /// atomically replaces whatever this SAME peer was pinned at a moment
    /// ago, even if that address was itself the product of a different,
    /// concurrently-running `configure_peer`/`migrate` command this task
    /// already serialized ahead of this one. This is what keeps "at most one
    /// pinned address per peer" true at every instant, not merely eventually.
    pinned_by_peer: HashMap<PeerId, SocketAddr>,
    /// Addresses a `cleanup_dead_peers` sweep has RESERVED for reaping --
    /// see `OwnerCommand::ReserveForReap`. Keyed purely by address (no
    /// `PeerId` is needed to check or hold membership), but the DECISION
    /// to grant a reservation is not: it is only made after revalidating
    /// the full identity (ownership + pin state) the caller's selection
    /// observed for the address still matches exactly -- see
    /// `ReserveForReap`'s doc comment for why an address-only or
    /// `claim_committed_at`-only check is not enough. While an address is
    /// a member here, `claim`/`claim_connection_scoped` refuse EVERY claim
    /// for it outright, regardless of claimant identity or what
    /// `arbitrate` would otherwise decide. This is what makes the sweep's
    /// later, non-owner destructive work (actor removal, tombstone
    /// emission) safe to run OUTSIDE the owner's critical path without
    /// racing a concurrent reconnect, instead of the sweep merely
    /// re-checking a snapshot that could go stale again before it finishes
    /// acting on it. Released by `OwnerCommand::ReleaseReapReservation`
    /// once the sweep is done with the address, successfully or not.
    ///
    /// EXCLUSIVE, not merely present/absent: `reserve_for_reap` grants a
    /// reservation only when THIS call is the one that actually inserts
    /// the address (checked via `contains_key` immediately before
    /// inserting, both inside the same synchronous call -- no other
    /// command can interleave between them). Two concurrent sweeps racing
    /// to reserve the SAME address must never both receive a guard backed
    /// by one shared entry: whichever released first would remove the
    /// entry while the OTHER sweep's destructive work was still relying
    /// on it staying held, reopening the exact race this mechanism exists
    /// to close, one level up. The second sweep instead finds the address
    /// already reserved and skips the candidate entirely -- there is no
    /// legitimate reason for two sweeps to reap the same address at once,
    /// so refusal (not reference-counted sharing) keeps "at most one
    /// destructive pass per address" true by construction.
    ///
    /// The VALUE is what makes a reservation a live authority rather than a
    /// one-time admission ticket: a shared, lock-free `Arc<AtomicU8>`,
    /// initialized to `REAP_PENDING` when granted, with a clone handed to
    /// the matching [`ReapReservation`] guard. Owner-side liveness evidence
    /// can transition it to `REAP_INVALIDATED`; the guard's single
    /// `REAP_PENDING -> REAP_COMMITTED` CAS is the reap's linearization
    /// point. Pin-changing configuration is rejected while a previous pin
    /// is reserved, so no operator eviction can race a committed sweep.
    reap_reserved: HashMap<SocketAddr, Arc<AtomicU8>>,
    /// Updated ONLY via `OwnerCommand::NoteLivenessEvidence`, from within
    /// this task's own `&mut self` command handling -- see that variant's
    /// doc comment, and `claim_committed_at`'s just above, for why this
    /// must NOT be a lock-free structure some other task writes directly:
    /// `release_dead_peer`'s read of it must be atomic with the release
    /// decision it feeds, and the only way to guarantee that is for both
    /// the write and the read to happen inside this task's own
    /// serialized command stream, exactly like every other piece of
    /// owner-authoritative state in this struct.
    liveness_evidence_at: HashMap<SocketAddr, std::time::Instant>,
    /// P1 finding (review round against `ded8495`, `registry.rs:4982`):
    /// `GossipRegistry`'s own, caller-side generation fence for
    /// `configure_peer`'s queued retry (`should_abandon`, checked before
    /// submitting the async owner command) was not atomic with the pin
    /// update it guarded -- a later, genuinely newer call could bump the
    /// caller-side counter and commit its OWN pin AFTER the check passed
    /// but BEFORE the stale retry's own command reached the owner, which
    /// would then still install the stale pin, evicting the newer one,
    /// with no way for the (already-passed) caller-side check to catch it.
    ///
    /// Moved here, owner-side, so `configure_peer`'s own atomic transaction
    /// (`PeerRegistryOwner::configure_peer`) can validate a retry's
    /// generation in the SAME serialized step that installs the pin,
    /// rather than the caller validating a snapshot of it beforehand and
    /// hoping nothing changes before its own separate command lands. The
    /// FIRST `configure_peer` call for a peer (no `expected_generation`)
    /// bumps this monotonically and reports the new value back to the
    /// caller; every retry presents that value back as `expected_
    /// generation`, and is rejected outright -- atomically, before
    /// touching anything else -- if a NEWER call has since bumped it
    /// further.
    configure_peer_generation: HashMap<PeerId, u64>,
    snapshot: Arc<ArcSwap<RoutingSnapshot>>,
    routing: Weak<dyn RoutingPublisher>,
    /// Position of the last committed mutation. A plain `u64` rather than an
    /// atomic: the field is reached only through `&mut self` from the single
    /// owner task, so the type system already provides the exclusivity an
    /// atomic would be buying at a cost. Advanced by EVERY committed mutation
    /// — accepted claim, release, migrate — so a claim's position orders it
    /// against all of them, not just against other claims.
    commit_seq: CommitSeq,
}

impl PeerRegistryOwner {
    /// Run until every sender is dropped. Drains TWO channels: the main
    /// bounded mailbox, and the dedicated unbounded release channel (see
    /// `OwnerShared::release_tx`'s doc comment) -- `biased` so a ready
    /// release is always handled before a ready ordinary command, on the
    /// theory that a granted reservation should be held no longer than
    /// necessary once its release is already queued.
    ///
    /// The priority is re-checked on EVERY drained command, not just once
    /// per outer wakeup: an earlier version drained `release_rx` fully,
    /// then drained `rx` fully, each in its own separate `while let`
    /// loop. `self.handle` never awaits, so once that second loop started
    /// it ran to completion as one uninterruptible synchronous burst --
    /// any release that became queued only after the first loop's single
    /// check (e.g. a concurrent `ReleaseReapReservation` landing while a
    /// burst of ordinary commands was already draining) was invisible to
    /// this task until the ENTIRE ordinary backlog was exhausted, no
    /// matter how large that backlog was. That starves exactly the
    /// priority this function's own `biased` select exists to provide:
    /// `reap_reserved`'s own doc comment is explicit that a reservation
    /// should be "held no longer than necessary once its release is
    /// already queued", and a caller retrying against a supposedly
    /// temporary `ClaimRejection::ReapInProgress` (`configure_peer`'s
    /// bounded reap-retry among them) could exhaust its retry budget
    /// entirely behind an ordinary-command backlog even though the
    /// reservation the retries are waiting on was released long before
    /// the backlog finished. A single combined loop that re-checks
    /// `release_rx` before every single item -- ordinary or release --
    /// keeps the priority intact for the whole drain, not just its first
    /// instant.
    async fn run(
        mut self,
        mut rx: mpsc::Receiver<OwnerCommand>,
        mut release_rx: mpsc::UnboundedReceiver<OwnerCommand>,
    ) {
        loop {
            tokio::select! {
                biased;
                Some(command) = release_rx.recv() => {
                    self.handle(command);
                }
                command = rx.recv() => {
                    let Some(command) = command else {
                        break;
                    };
                    self.handle(command);
                }
            }
            // Drain whatever else is already queued on EITHER channel
            // without re-suspending -- release commands first, on EVERY
            // iteration, for the same priority reason as the select
            // above (see this function's own doc comment). Publication
            // still happens per command inside `handle` rather than once
            // per batch: a reply must never be observable before the
            // snapshot that justifies it.
            loop {
                if let Ok(command) = release_rx.try_recv() {
                    self.handle(command);
                    continue;
                }
                if let Ok(command) = rx.try_recv() {
                    self.handle(command);
                    continue;
                }
                break;
            }
        }
        debug!("registry owner task stopped: all senders dropped");
    }

    fn handle(&mut self, command: OwnerCommand) {
        match command {
            OwnerCommand::Claim {
                addr,
                claim,
                is_local_addr,
                reply,
            } => {
                let commit = self.claim(addr, claim, is_local_addr);
                // Reply AFTER the commit + publish above, never before.
                let _ = reply.send(commit);
            }
            OwnerCommand::Release {
                addr,
                expected_owner,
                expected_generation,
                reply,
            } => {
                let released = self.release(addr, &expected_owner, expected_generation);
                let _ = reply.send(released);
            }
            OwnerCommand::Migrate {
                from,
                to,
                expected_source,
                is_local_addr,
                reply,
            } => {
                let migrated = self.migrate(from, to, &expected_source, is_local_addr);
                let _ = reply.send(migrated);
            }
            OwnerCommand::ClaimConnectionScoped {
                addr,
                claim,
                session_source,
                connection_instance_id,
                evidence_at,
                reply,
            } => {
                let commit = self.claim_connection_scoped(
                    addr,
                    claim,
                    session_source,
                    connection_instance_id,
                    evidence_at,
                );
                let _ = reply.send(commit);
            }
            OwnerCommand::ReleaseSession {
                peer_id,
                session_source,
                connection_instance_id,
                reply,
            } => {
                let candidates =
                    self.release_session(&peer_id, session_source, connection_instance_id);
                let _ = reply.send(candidates);
            }
            OwnerCommand::ReleaseDeadPeer {
                peer_id,
                addr,
                evidence_before,
                reply,
            } => {
                let released = self.release_dead_peer(&peer_id, addr, evidence_before);
                let _ = reply.send(released);
            }
            OwnerCommand::HasNewerLivenessEvidence {
                addr,
                evidence_before,
                reply,
            } => {
                let has_newer = self.has_newer_liveness_evidence(addr, evidence_before);
                let _ = reply.send(has_newer);
            }
            OwnerCommand::ReapBaselineActivityDetected {
                addr,
                peer_id,
                evidence_before,
                baseline_configure_peer_generation,
                reply,
            } => {
                let activity_detected = self.reap_baseline_activity_detected(
                    addr,
                    &peer_id,
                    evidence_before,
                    baseline_configure_peer_generation,
                );
                let _ = reply.send(activity_detected);
            }
            OwnerCommand::ConfigurePeerGenerationOf { peer_id, reply } => {
                let generation = self
                    .configure_peer_generation
                    .get(&peer_id)
                    .copied()
                    .unwrap_or(0);
                let _ = reply.send(generation);
            }
            OwnerCommand::ReserveForReap {
                addr,
                evidence_before,
                expected_ownership,
                expected_pin,
                expected_node_id,
                reply,
            } => {
                let granted = self.reserve_for_reap(
                    addr,
                    evidence_before,
                    expected_ownership,
                    expected_pin,
                    expected_node_id,
                );
                let _ = reply.send(granted);
            }
            OwnerCommand::ReleaseReapReservation { addr, reply } => {
                self.reap_reserved.remove(&addr);
                let _ = reply.send(());
            }
            #[cfg(test)]
            OwnerCommand::Pin {
                addr,
                peer_id,
                reply,
            } => {
                let evicted = self.pin(addr, peer_id);
                let _ = reply.send(evicted);
            }
            OwnerCommand::ConfigurePeer {
                addr,
                peer_id,
                expected_generation,
                reply,
            } => {
                let commit = self.configure_peer(addr, peer_id, expected_generation);
                let _ = reply.send(commit);
            }
            OwnerCommand::SetOrdinaryConnectRoute {
                peer_id,
                addr,
                reply,
            } => {
                let accepted = self.set_ordinary_connect_route(&peer_id, addr);
                let _ = reply.send(accepted);
            }
            #[cfg(test)]
            OwnerCommand::InspectGeneration { addr, reply } => {
                let _ = reply.send(self.claim_generation.get(&addr).copied());
            }
            #[cfg(test)]
            OwnerCommand::IsReapReserved { addr, reply } => {
                let _ = reply.send(self.reap_reserved.contains_key(&addr));
            }
            OwnerCommand::NoteLivenessEvidence { addr, at } => {
                self.note_liveness_evidence(addr, at);
            }
        }
    }

    fn claim(&mut self, addr: SocketAddr, claim: Claim, is_local_addr: bool) -> ClaimCommit {
        // A `cleanup_dead_peers` sweep is currently reaping this address --
        // see `reap_reserved`'s doc comment and `OwnerCommand::
        // ReserveForReap`. Refused unconditionally, before `arbitrate` is
        // even consulted, regardless of what it would otherwise decide:
        // the sweep's non-owner destructive work is safe to run outside
        // this task's critical path ONLY because nothing can commit
        // ownership of a reserved address while the reservation is held.
        // `claim_connection_scoped` calls this same function, so this one
        // check point covers both paths.
        if self.reap_reserved.contains_key(&addr) {
            trace!(
                addr = %addr,
                claimant = %claim.node_id,
                "address claim rejected: a dead-peer reap reservation is held for it"
            );
            return ClaimCommit::Rejected(ClaimRejection::ReapInProgress);
        }
        let current = self.addr_ownership.get(&addr).cloned();
        match arbitrate(current.clone(), claim.clone(), is_local_addr) {
            Decision::Reject(reason) => {
                // Rejected claims mutate nothing and publish nothing.
                trace!(addr = %addr, claimant = %claim.node_id, ?reason, "address claim rejected");
                ClaimCommit::Rejected(ClaimRejection::Arbitration(reason))
            }
            Decision::Accept => {
                let created_ownership = current.is_none();
                let kind = resolved_kind(current.as_ref(), &claim);
                let displaced = current
                    .as_ref()
                    .filter(|owner| owner.node_id != claim.node_id)
                    .map(|owner| owner.node_id.clone());
                let node_id = claim.node_id;
                let next_owner = Owner {
                    node_id: node_id.clone(),
                    kind,
                };
                let ownership_changed = current.as_ref() != Some(&next_owner);
                if ownership_changed {
                    self.addr_ownership.insert(addr, next_owner.clone());
                }
                let commit_seq = self.advance();
                // Every accepted refresh is a new lifecycle generation even
                // when peer identity and claim kind are unchanged.
                self.claim_generation.insert(addr, commit_seq);
                // Keep every still-live connection-scoped receipt for this
                // SAME owner+address in sync with the generation this claim
                // just advanced to -- regardless of whether THIS claim is
                // itself connection-scoped. `claim_generation` is the CAS
                // fencing token `release`'s `expected_generation` check
                // compares a receipt's stored generation against, and it
                // advances on every accepted claim for the reasons above
                // (`ownership_token`/`claim_is_current`, and `migrate`'s own
                // CAS check, all need "any accepted claim is a new,
                // distinguishable state" -- not only a connection-scoped
                // one). A receipt that does not move with it is not stale
                // evidence, it is a stale CACHE of a token that already
                // moved: `release_session` finds it, hands its old
                // generation to `release`, and `release`'s CAS rejects it
                // against the newer one -- and because `release_session`
                // already removed the receipt before that CAS ever ran,
                // there is no retry, and the address's ownership is
                // stranded forever on a later, genuinely correct teardown.
                //
                // This is deliberately NOT "stop advancing `claim_generation`
                // for indirect refreshes" instead: `claim_generation` and
                // `claim_committed_at` already answer two DIFFERENT
                // questions correctly-scoped to two different kinds of
                // evidence -- "is this the current CAS-fenced state"
                // (any accepted claim, on purpose) vs. "when did this
                // address last have DIRECT evidence of a live owner"
                // (connection-scoped only, on purpose, see
                // `claim_committed_at` below). Splitting `claim_generation`
                // itself into a second, indirect-claims-excluded counter
                // would re-fold those two already-cleanly-separated
                // concerns back together and require threading a THIRD
                // generation concept through `release`'s CAS and every
                // `connection_scoped_claims` entry, for no benefit over
                // just keeping the existing receipt cache in sync with the
                // token it is meant to track.
                for (key, generation) in self.connection_scoped_claims.iter_mut() {
                    if key.0 == node_id && key.2 == addr {
                        *generation = commit_seq;
                    }
                }
                // `claim_committed_at` is deliberately NOT touched here.
                // This method also serves the gossip/discovery path (any
                // caller of the plain, non-connection-scoped `claim`
                // command) -- third-party address announcements this
                // registry never directly verified, which can be repeated
                // indefinitely (benign chatter or a deliberate replay)
                // regardless of whether the claimed peer is actually
                // reachable. Only `claim_connection_scoped` -- backed by an
                // outbound dial this node completed or an authenticated
                // inbound session -- is direct evidence the peer is alive
                // right now, so only it refreshes this timestamp. Refreshing
                // it here would let indirect chatter about an offline peer
                // keep `release_dead_peer`'s freshness fence perpetually
                // satisfied, answering "when did we last hear a claim
                // mentioning this address" instead of "when did this
                // address last have a directly-evidenced live owner".
                // The lock-free snapshot is also the authoritative
                // generation fence. Refresh it for every accepted command;
                // route publication itself remains identity/kind-change only.
                self.publish_owner_snapshot(addr, Some((next_owner, commit_seq)));
                if ownership_changed {
                    if let Some(routing) = self.routing.upgrade() {
                        routing.publish_owner(addr, &node_id);
                    }
                }
                ClaimCommit::Accepted {
                    kind,
                    displaced,
                    created_ownership,
                    commit_seq,
                }
            }
        }
    }

    /// `claim`, plus the connection-scoped receipt bookkeeping for
    /// `session_source`, committed in the same synchronous step.
    ///
    /// This is the structural fix for two races PR #178's separately
    /// synchronized receipt map could hit: with the transfer folded into the
    /// same `&mut self` call as the commit itself (inside `claim` -- see its
    /// doc comment), no second command can ever be handled in between, so
    /// - two concurrent claims for the same peer+address can no longer
    ///   finish their receipt transfer out of commit order (whichever claim
    ///   this method processes SECOND always sees the first one's receipts
    ///   already installed, and transfers them again to its own, later,
    ///   generation), and
    /// - a session exit racing a fresh claim for the same peer+address can no
    ///   longer strand a ghost receipt for the exiting session (`release_session`
    ///   either runs first and removes it before this transfer would touch
    ///   it, or this transfer runs first and carries it forward to the new
    ///   generation like any other still-live receipt, for `release_session`
    ///   to remove correctly afterward).
    ///
    /// The transfer itself lives in `claim`, not here, and runs for EVERY
    /// accepted same-owner claim, connection-scoped or not: a plain
    /// gossip/discovery refresh through the shared `claim` path advances
    /// `claim_generation` exactly the same way this method's own commit
    /// does, so it must keep live receipts in sync exactly the same way too,
    /// or a plain refresh between a connection's claim and its later
    /// teardown leaves that session's receipt holding a generation
    /// `release`'s CAS will reject -- permanently stranding the address once
    /// `release_session` has already removed the now-useless receipt.
    fn claim_connection_scoped(
        &mut self,
        addr: SocketAddr,
        claim: Claim,
        _session_source: SocketAddr,
        connection_instance_id: u64,
        evidence_at: std::time::Instant,
    ) -> ClaimCommit {
        let peer_id = claim.node_id.clone();
        let commit = self.claim(addr, claim, /* is_local_addr */ false);
        if commit.is_accepted() {
            // Unlike the plain `claim` command, every call into this method
            // is backed by an actual connection -- an outbound dial this
            // node completed, or an authenticated inbound session (see
            // `GossipRegistry::add_connection_scoped_peer_claim`'s only two
            // production callers). That is direct evidence the peer is
            // alive right now, so this is the one place `claim_committed_at`
            // is refreshed to "now" -- regardless of whether the address
            // ends up pinned below, so a currently-connected pinned peer's
            // address is never mistaken for one that has been untouched.
            self.claim_committed_at.insert(addr, evidence_at);
        }
        // An operator-pinned address (`pin`, set only by `configure_peer`) is
        // a reservation that exists independently of any one connection: no
        // receipt is recorded for it, so nothing here can later mistake a
        // stale session teardown for permission to retract the pin.
        if self.operator_pinned.get(&addr) == Some(&peer_id) {
            return commit;
        }
        if let ClaimCommit::Accepted { commit_seq, .. } = &commit {
            let commit_seq = *commit_seq;
            // Every still-live session receipt for this peer+address was
            // already transferred to `commit_seq` inside `self.claim(...)`
            // above (it syncs every accepted claim's receipts, connection-
            // scoped or not -- see its doc comment). Only THIS session's own
            // new receipt remains to be added.
            self.connection_scoped_claims
                .insert((peer_id, connection_instance_id, addr), commit_seq);
        }
        commit
    }

    fn release(
        &mut self,
        addr: SocketAddr,
        expected_owner: &PeerId,
        expected_generation: CommitSeq,
    ) -> Option<CommitSeq> {
        // A pinned address can only ever be retracted through `set_pin`
        // clearing the pin first (see `configure_peer`'s reconfigure path,
        // the sole legitimate mover of a pin) -- never through this generic
        // path, which every connection-scoped and dead-peer release routes
        // through and which a stale/racing caller cannot be trusted to have
        // already unpinned.
        if self.operator_pinned.contains_key(&addr) {
            return None;
        }
        let matches_expectation = self
            .addr_ownership
            .get(&addr)
            .is_some_and(|owner| owner.node_id == *expected_owner)
            && self.claim_generation.get(&addr) == Some(&expected_generation);
        if !matches_expectation {
            return None;
        }
        let owner = self.addr_ownership.remove(&addr)?;
        Some(self.retract_owner(addr, owner))
    }

    /// Atomically release every connection-scoped receipt `peer_id` holds
    /// for `session_source`, AND retract this session's address ownership
    /// for every address no OTHER live session still covers -- in the SAME
    /// synchronous step, not as two separately-ordered owner commands.
    ///
    /// Folding both into one command, rather than reporting release
    /// candidates for a caller to release with a follow-up `release` call,
    /// is what closes a real stranding window: `release` fences a CAS
    /// against `claim_generation`, and a plain, same-identity claim landing
    /// between "find the last receipt here" and "release using its
    /// generation" advances that generation with no receipt left to update
    /// (this session's own receipt was already removed by the first
    /// command) -- so the follow-up `release` rejects its now-stale
    /// generation, and because the receipt is already gone there is no
    /// retry. An unpinned address with no receipt left that could ever
    /// release it is stranded permanently: the exact failure this PR
    /// exists to fix, reached through the ordinary teardown path instead
    /// of the dead-peer sweep. Folded, this needs no CAS at all -- nothing
    /// can move `claim_generation` in the middle of one synchronous call,
    /// so checking "is `peer_id` still `addr`'s owner right now" (the same
    /// check `release_dead_peer` uses, for the same reason) is sufficient;
    /// it also naturally covers the case where a DIFFERENT identity has
    /// since taken the address (a displacing claim, a migration), which
    /// this session's exit must never retract regardless of receipts.
    ///
    /// An address is only released when NO other live session still holds
    /// a receipt for the same peer+address at this exact moment -- checked
    /// against the map after this session's own entries are already
    /// removed, in the same synchronous step, so a concurrent claim or a
    /// concurrent second session's own exit can never be interleaved into
    /// the middle of this decision.
    ///
    /// Returns the addresses actually released, paired with the resulting
    /// commit sequence, for the caller to tombstone its own `gossip_state`
    /// ownership projection at -- the same shape `release_dead_peer`
    /// returns, and for the same purpose.
    fn release_session(
        &mut self,
        peer_id: &PeerId,
        _session_source: SocketAddr,
        connection_instance_id: u64,
    ) -> Vec<(SocketAddr, CommitSeq)> {
        let mut own_entries = Vec::new();
        self.connection_scoped_claims.retain(|key, generation| {
            if &key.0 == peer_id && key.1 == connection_instance_id {
                own_entries.push((key.2, *generation));
                false
            } else {
                true
            }
        });
        let no_longer_covered: Vec<SocketAddr> = own_entries
            .into_iter()
            .filter(|(addr, _)| {
                !self
                    .connection_scoped_claims
                    .keys()
                    .any(|key| &key.0 == peer_id && key.2 == *addr)
            })
            .map(|(addr, _)| addr)
            .collect();

        let mut released = Vec::new();
        for addr in no_longer_covered {
            // A pinned address can only ever be retracted through `set_pin`
            // clearing the pin first -- see `release`'s matching check.
            if self.operator_pinned.contains_key(&addr) {
                continue;
            }
            let still_owned = self
                .addr_ownership
                .get(&addr)
                .is_some_and(|owner| &owner.node_id == peer_id);
            if !still_owned {
                continue;
            }
            if let Some(owner) = self.addr_ownership.remove(&addr) {
                let release_seq = self.retract_owner(addr, owner);
                released.push((addr, release_seq));
            }
        }
        released
    }

    /// Invalidate a currently-held reap reservation for `addr`, if one
    /// exists -- a no-op otherwise. Cheap and synchronous: transitions the
    /// SAME `Arc<AtomicU8>` `reap_reserved` shares with the matching
    /// `ReapReservation` guard, from within this task's own serialized
    /// command stream (see `reap_reserved`'s own doc comment for why the
    /// write must happen here, never from outside).
    ///
    /// Every owner command that commits a fact making `addr` no longer
    /// genuinely worth reaping -- currently direct liveness evidence
    /// (`note_liveness_evidence`) -- calls this as part of that SAME atomic
    /// commit, so a pending destructive phase's CAS fails. Operator pin
    /// changes are rejected while a reservation is held, before they can
    /// need this invalidation path.
    fn invalidate_reap_reservation(&self, addr: SocketAddr) {
        if let Some(valid) = self.reap_reserved.get(&addr) {
            // Invalidation wins only before the reap's commit point. Once
            // `try_consume` has linearized the destructive phase, later
            // evidence is ordered after that decision and must not turn the
            // final ownership verdict into an inconsistent ProvenAlive.
            let _ = valid.compare_exchange(
                REAP_PENDING,
                REAP_INVALIDATED,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }

    /// `OwnerCommand::NoteLivenessEvidence`'s handler: record `at` as the
    /// latest DIRECT liveness evidence for `addr`, taking the max so a
    /// command that happens to be processed out of SEND order (never out
    /// of PROCESSING order -- this queue is FIFO -- but a caller could in
    /// principle construct two `Instant`s and submit the later-timestamped
    /// one first) never rolls the recorded evidence backwards.
    ///
    /// P1 finding (review round against `d4000e2`, `registry.rs:8750`):
    /// ALSO invalidates a currently-held reap reservation for `addr`, if
    /// one exists, the instant this evidence commits. `release_dead_peer_
    /// ownership`'s own verdict (checked against `liveness_evidence_at`
    /// exactly like `release_dead_peer` below does) is obtained through an
    /// `.await` before the destructive phase's irreversible steps run --
    /// stale the moment it returns, since nothing blocks THIS evidence from
    /// committing while the reservation is pending. Transitioning the
    /// reservation here, synchronously, from within this task's serialized
    /// command stream, makes the pending CAS fail; evidence processed after
    /// a committed CAS is ordered after the reap by `release_dead_peer`.
    fn note_liveness_evidence(&mut self, addr: SocketAddr, at: std::time::Instant) {
        self.liveness_evidence_at
            .entry(addr)
            .and_modify(|existing| {
                if at > *existing {
                    *existing = at;
                }
            })
            .or_insert(at);
        self.invalidate_reap_reservation(addr);
    }

    /// `OwnerCommand::HasNewerLivenessEvidence`'s handler: the SAME causal
    /// fence `release_dead_peer` checks first (`claim_committed_at` OR
    /// `liveness_evidence_at` causally newer than `evidence_before`), as a
    /// PURE READ -- `&self`, not `&mut self`, and no mutation of any kind.
    /// See that command's own doc comment for why this exists separately
    /// from `release_dead_peer` rather than the caller trying to infer the
    /// same answer from its outcome: this is what lets
    /// `reap_reserved_candidates` decide whether a candidate is worth
    /// destroying at all WITHOUT that decision itself performing the
    /// first destructive step.
    fn has_newer_liveness_evidence(
        &self,
        addr: SocketAddr,
        evidence_before: std::time::Instant,
    ) -> bool {
        self.claim_committed_at
            .get(&addr)
            .is_some_and(|committed_at| *committed_at > evidence_before)
            || self
                .liveness_evidence_at
                .get(&addr)
                .is_some_and(|seen_at| *seen_at > evidence_before)
    }

    /// `OwnerCommand::ReapBaselineActivityDetected`'s handler -- see that
    /// variant's own doc comment for why this is a best-effort MITIGATION
    /// that narrows, not an authorization that closes, plus the P1 finding
    /// it exists to shrink and its SECOND pass (both: review round against
    /// `ded8495`, `registry.rs:9508`). PURE READ, no mutation: reuses
    /// `has_newer_liveness_evidence` verbatim for the evidence half, and
    /// independently checks whether `peer_id`'s OWN `configure_peer_
    /// generation` has advanced past `baseline_configure_peer_generation`
    /// -- i.e. whether some `configure_peer` call for this SAME peer
    /// committed since the caller captured that baseline (BEFORE
    /// `try_consume` ran -- see `GossipRegistry::reap_reserved_
    /// candidates`'s own doc comment for the P1 finding on that exact
    /// ordering). Deliberately NOT an ownership check
    /// (`addr_ownership.get(&addr)`): the first version of this function
    /// tried that and produced false positives for every candidate that
    /// was never owner-claimed at all (a `GossipState`-only entry, the
    /// common case for ordinary dead-peer selection) -- reading `None`
    /// there regardless of whether anything actually changed. The
    /// generation counter has no such ambiguity: it only ever advances on
    /// an actual `configure_peer` call for this exact peer, so "advanced
    /// past the baseline" means precisely "an operator reconfigured this
    /// peer during this window", never "this candidate happens to look
    /// unowned for an unrelated reason".
    fn reap_baseline_activity_detected(
        &self,
        addr: SocketAddr,
        peer_id: &PeerId,
        evidence_before: std::time::Instant,
        baseline_configure_peer_generation: u64,
    ) -> bool {
        if self.has_newer_liveness_evidence(addr, evidence_before) {
            return true;
        }
        let current_generation = self
            .configure_peer_generation
            .get(peer_id)
            .copied()
            .unwrap_or(0);
        current_generation > baseline_configure_peer_generation
    }

    /// Release everything `peer_id` still holds at `addr`: every
    /// connection-scoped receipt recorded for it there under any session
    /// (ghost cleanup for a teardown that never ran), and the ownership
    /// record itself if `peer_id` is still its owner and `addr` is not
    /// operator-pinned.
    ///
    /// `evidence_before` is a CAUSAL fence, checked against
    /// `claim_committed_at` -- owner-internal, exclusively owner-written
    /// state -- rather than against anything the caller observed or against
    /// elapsed wall-clock time. It answers "did this address get direct
    /// evidence of a live owner AFTER the failure evidence this reap is
    /// acting on was itself recorded", not "how long ago was the last
    /// commit" and not "does the generation still match what the caller
    /// saw". Both of those alternatives are temporal/snapshot LEASES: each
    /// can be satisfied merely by enough wall-clock time passing --
    /// elapsed-time-since-commit expires as soon as the command sits queued
    /// past the timeout regardless of when the reconnect actually happened,
    /// and a generation snapshot only fences the window between selection
    /// and this command running, not the window before selection where a
    /// reconnect can land while `GossipState`'s own failure bookkeeping
    /// (cleared only by `mark_peer_connected*`, itself running AFTER this
    /// owner already committed the claim) has not caught up yet.
    ///
    /// A causal comparison between two FIXED instants has neither problem:
    /// `claim_committed_at` is fixed the moment a connection-scoped claim
    /// commits, and `evidence_before` is fixed by the caller at selection
    /// time from the failure's own recorded age. Neither moves as more time
    /// elapses before this command actually runs, so a claim causally after
    /// the failure invalidates the reap permanently -- it cannot expire by
    /// waiting -- and a claim causally before it (or no direct evidence at
    /// all) never protects the address, no matter how promptly the sweep
    /// reaches this command.
    ///
    /// `claim_committed_at` alone only ever advances on a NEW claim/session
    /// event -- it says nothing about ongoing traffic on a connection
    /// ALREADY claimed, which never commits another claim no matter how
    /// many responses it delivers. `liveness_evidence_at` (the same causal
    /// comparison, against the same `evidence_before`) is the
    /// complementary fence for exactly that case -- see its own doc
    /// comment. Checking both, in the SAME synchronous step that decides
    /// the release, is what finally closes this family of finding: every
    /// earlier fence in this file answered "did a NEW claim land in some
    /// window", and gossip delivering ordinary liveness on an
    /// already-established connection was never a claim at all, so it
    /// could slip through any fence built only from claim events, no
    /// matter how many windows those fences closed or how early they
    /// closed them.
    fn release_dead_peer(
        &mut self,
        peer_id: &PeerId,
        addr: SocketAddr,
        evidence_before: std::time::Instant,
    ) -> DeadPeerReleaseOutcome {
        // A reservation's successful `try_consume` is the single
        // linearization point shared with actor/tombstone destruction. Any
        // evidence observed after that point is ordered after this reap and
        // must not make the final ownership result disagree with the work
        // already committed. Unreserved callers retain the normal causal
        // liveness fence.
        let reap_committed = self
            .reap_reserved
            .get(&addr)
            .is_some_and(|state| state.load(Ordering::Acquire) == REAP_COMMITTED);
        if !reap_committed
            && self
                .claim_committed_at
                .get(&addr)
                .is_some_and(|committed_at| *committed_at > evidence_before)
        {
            trace!(
                addr = %addr,
                peer = %peer_id,
                "dead-peer release refused: address has direct evidence of life after the \
                 failure this reap is acting on"
            );
            return DeadPeerReleaseOutcome::ProvenAlive;
        }
        if !reap_committed
            && self
                .liveness_evidence_at
                .get(&addr)
                .is_some_and(|seen_at| *seen_at > evidence_before)
        {
            trace!(
                addr = %addr,
                peer = %peer_id,
                "dead-peer release refused: address has ordinary liveness evidence (a response \
                 on an already-claimed connection) after the failure this reap is acting on"
            );
            return DeadPeerReleaseOutcome::ProvenAlive;
        }
        self.connection_scoped_claims
            .retain(|key, _| !(&key.0 == peer_id && key.2 == addr));
        if self.operator_pinned.contains_key(&addr) {
            return DeadPeerReleaseOutcome::NotApplicable;
        }
        let still_owned = self
            .addr_ownership
            .get(&addr)
            .is_some_and(|owner| owner.node_id == *peer_id);
        if !still_owned {
            return DeadPeerReleaseOutcome::NotApplicable;
        }
        let Some(owner) = self.addr_ownership.remove(&addr) else {
            return DeadPeerReleaseOutcome::NotApplicable;
        };
        DeadPeerReleaseOutcome::Released(self.retract_owner(addr, owner))
    }

    /// This address's current ownership, as an `OwnershipToken`,
    /// constructed directly from this task's own authoritative
    /// `addr_ownership`/`claim_generation` -- not the published
    /// `RoutingSnapshot`, which is only ever a projection OF this state,
    /// committed in the same step. `claim_generation` is guaranteed
    /// present whenever `addr_ownership` is: every accepted `claim`
    /// inserts both together, and `retract_owner` (the sole remover of
    /// either) removes both together.
    fn current_ownership_token(&self, addr: &SocketAddr) -> Option<OwnershipToken> {
        let owner = self.addr_ownership.get(addr)?;
        let generation = self
            .claim_generation
            .get(addr)
            .copied()
            .expect("claim_generation must be present whenever addr_ownership is");
        Some(OwnershipToken::new(owner.node_id.clone(), generation))
    }

    /// `OwnerCommand::ReserveForReap`'s handler: checks the causal fence
    /// against `evidence_before` (the SAME one `release_dead_peer` checks
    /// first) AND revalidates the FULL identity the caller's selection
    /// observed for `addr` -- ownership, pin state, AND that the
    /// destructive phase's own `node_id` corresponds to them -- against
    /// this task's own current state, and, only if EVERY check passes,
    /// marks `addr` reserved (see `reap_reserved`'s doc comment) instead
    /// of releasing anything outright. Nothing else is touched: no
    /// receipt purge, no ownership change. See `OwnerCommand::
    /// ReserveForReap`'s own doc comment for why the causal fence and the
    /// ownership/pin checks are both required -- they protect two
    /// different windows against two different kinds of evidence, and
    /// neither alone closes both. The reservation alone is what makes
    /// `claim`'s own refusal of every claim for a reserved address the
    /// thing that keeps a concurrent reconnect from committing while the
    /// caller's later, non-owner destructive work runs.
    ///
    /// The `expected_node_id` check runs LAST, after ownership/pin are
    /// already reconfirmed current -- deliberately, not incidentally: by
    /// that point, `expected_ownership`/`expected_pin` are not merely
    /// what the caller observed at selection, they are what THIS atomic
    /// step has just, freshly, reconfirmed IS the current state. Checking
    /// `expected_node_id` against them here, rather than at selection
    /// time (or against them at selection time, which is the same
    /// mistake one step earlier), is what makes the comparison meaningful
    /// against the actual race: a `node_id` that agreed with
    /// `expected_ownership`/`expected_pin` back at selection tells us
    /// nothing about whether it still does once this command actually
    /// runs, since all three could have gone stale together in the
    /// interim. Borrowing the freshness the ownership/pin checks above
    /// just established is what closes that gap.
    fn reserve_for_reap(
        &mut self,
        addr: SocketAddr,
        evidence_before: std::time::Instant,
        expected_ownership: Option<OwnershipToken>,
        expected_pin: Option<PeerId>,
        expected_node_id: Option<PeerId>,
    ) -> Option<Arc<AtomicU8>> {
        if self
            .claim_committed_at
            .get(&addr)
            .is_some_and(|committed_at| *committed_at > evidence_before)
        {
            trace!(
                addr = %addr,
                "reap reservation refused: address has direct evidence of life after the \
                 failure this reap is acting on"
            );
            return None;
        }
        if self.current_ownership_token(&addr) != expected_ownership {
            trace!(
                addr = %addr,
                "reap reservation refused: ownership (identity or generation) moved since \
                 selection"
            );
            return None;
        }
        if self.operator_pinned.get(&addr) != expected_pin.as_ref() {
            trace!(
                addr = %addr,
                "reap reservation refused: operator pin state moved since selection"
            );
            return None;
        }
        // `expected_ownership`/`expected_pin` are, as of the two checks
        // just above, confirmed current -- so the identity they name IS
        // this address's current identity, right now, atomically. Ownership
        // is authoritative when present (a claim always exists before a
        // pin can be installed on top of it -- see `configure_peer`); pin
        // alone covers the (should-be-unreachable in steady state, but
        // checked anyway) case of a pin surviving without a corresponding
        // claim.
        //
        // Checked ONLY when this names a concrete identity -- `Some`.
        // `None` means unowned AND unpinned, which is not the same thing
        // as "no identity to protect": `GossipState` routinely knows a
        // `node_id` for an address with no owner-level claim behind it at
        // all (gossip/discovery chatter about a peer this node has never
        // itself claimed, or an address whose ownership was independently
        // released elsewhere while `GossipState`'s own record of who it
        // last belonged to lingers) -- a real, common, entirely legitimate
        // state, not evidence of a race. There is no ownership-level
        // identity there to be wrong about, so `expected_node_id` is not
        // constrained by this check in that case; it is exactly what the
        // destructive phase will act on regardless, and nothing here
        // authorizes any ownership-affecting step against it. The
        // adversarial case this closes is the opposite direction: a
        // CONCRETE, just-reconfirmed identity (`Some`) that `expected_
        // node_id` disagrees with, or is entirely silent about (`None`) --
        // fail-closed, exactly like every other check in this function.
        if let Some(current_identity) = expected_ownership
            .as_ref()
            .map(|token| token.owner().clone())
            .or_else(|| expected_pin.clone())
        {
            if expected_node_id.as_ref() != Some(&current_identity) {
                trace!(
                    addr = %addr,
                    "reap reservation refused: candidate's node_id does not correspond to the \
                     ownership/pin identity just reconfirmed current -- selection captured a \
                     mismatched identity, most likely a stale GossipState node_id paired with a \
                     newer owner"
                );
                return None;
            }
        }
        // Exclusive: checked via `contains_key` immediately before
        // inserting, both inside this same synchronous call -- `Some(_)`
        // here means `addr` was ALREADY reserved (a concurrent sweep, or
        // this same sweep re-entering), and must be refused rather than
        // treated as a fresh grant. See `reap_reserved`'s doc comment for
        // why sharing one entry (and its validity flag) between two
        // guards is unsafe.
        if self.reap_reserved.contains_key(&addr) {
            return None;
        }
        let valid = Arc::new(AtomicU8::new(REAP_PENDING));
        self.reap_reserved.insert(addr, valid.clone());
        Some(valid)
    }

    /// Atomically install `peer_id`'s operator pin at `addr`, evicting
    /// whatever address `pinned_by_peer` shows this SAME peer pinned at
    /// beforehand (if different).
    ///
    /// The eviction is keyed off `pinned_by_peer`, not off any address the
    /// caller believes was previously configured: that belief can be stale
    /// by the time this command runs (read from `ConnectionPool` before a
    /// concurrent `configure_peer`/`migrate` command for the same peer was
    /// serialized ahead of this one here). Consulting the owner's own
    /// authoritative reverse map instead is what guarantees at most one
    /// pinned address per peer at every instant, regardless of how many
    /// pin installs for that peer are in flight or in what order the owner
    /// task actually processes them.
    ///
    /// Returns the evicted address, if any and if different from `addr` --
    /// the caller's cue to also release that address's ownership. This
    /// helper only touches pin bookkeeping and the `ConnectionPool` route;
    /// releasing the evicted address's ownership (when applicable) is the
    /// caller's responsibility -- `configure_peer` below does so in the
    /// SAME synchronous step, atomically; the standalone `pin` command does
    /// not, by design (see its doc comment).
    ///
    /// Also publishes `addr` as `peer_id`'s configured/required
    /// `ConnectionPool` dial target, in this SAME step, via
    /// `RoutingPublisher::set_configured_peer_addr` -- so the pin decision
    /// and the connection-pool route caller code elsewhere reads
    /// (`get_required_peer_addr`) can never disagree about which address is
    /// current for this peer, the way two independently-atomic writes
    /// (owner pin, then a separate later `ConnectionPool` write) could
    /// under two concurrent `configure_peer` calls for the same peer.
    ///
    /// Also publishes the pin identity itself into the lock-free
    /// `RoutingSnapshot`, in this SAME step -- the authoritative answer to
    /// "is this peer still the one I pinned at this address" a caller
    /// revalidates via `RegistryOwnerHandle::pin_is_current` after this
    /// command returns. Deliberately a SEPARATE publication from both
    /// `ConnectionPool`'s route (moved by any `.connect()` call, configured
    /// or not) and the ownership generation (advanced by every accepted
    /// claim, including unrelated same-identity chatter): neither answers
    /// the pin question, only this does.
    fn install_pin(&mut self, addr: SocketAddr, peer_id: PeerId) -> Option<SocketAddr> {
        // Replacing an address pin must clear the displaced peer's reverse
        // entry too. Leaving the old `(peer -> addr)` entry behind lets a
        // later pin for that peer evict this address even though the peer no
        // longer owns its pin.
        if let Some(previous_peer) = self.operator_pinned.get(&addr)
            && previous_peer != &peer_id
            && self.pinned_by_peer.get(previous_peer) == Some(&addr)
        {
            self.pinned_by_peer.remove(previous_peer);
        }
        let previous = self.pinned_by_peer.insert(peer_id.clone(), addr);
        let evicted = previous.filter(|previous_addr| *previous_addr != addr);
        if let Some(evicted_addr) = evicted {
            self.operator_pinned.remove(&evicted_addr);
        }
        if let Some(routing) = self.routing.upgrade() {
            routing.set_configured_peer_addr(addr, &peer_id, evicted);
        }
        self.operator_pinned.insert(addr, peer_id.clone());
        let snapshot = self.snapshot.load_full();
        let mut snapshot = snapshot.with_pin(addr, Some(peer_id));
        if let Some(evicted_addr) = evicted {
            snapshot = snapshot.with_pin(evicted_addr, None);
        }
        self.snapshot.store(Arc::new(snapshot));
        evicted
    }

    /// `OwnerCommand::Pin`'s handler: see `install_pin`. Kept as its own,
    /// narrower command (never called from `GossipRegistry::configure_peer`,
    /// which uses the atomic `configure_peer` below instead) for the
    /// reverse-map invariant it guarantees on its own -- see
    /// `RegistryOwnerHandle::pin`'s doc comment for why a caller needing an
    /// ownership-backed pin must use `configure_peer` instead.
    #[cfg(test)]
    fn pin(&mut self, addr: SocketAddr, peer_id: PeerId) -> Option<SocketAddr> {
        self.install_pin(addr, peer_id)
    }

    /// `OwnerCommand::SetOrdinaryConnectRoute`'s handler: an ordinary
    /// `.connect()` call's route update, performed HERE -- inside the
    /// owner's own serialized command processing -- instead of by the
    /// caller writing `ConnectionPool` directly.
    ///
    /// Checked against `self.pinned_by_peer` directly: the owner's OWN,
    /// exclusively-owner-written reverse map, not the lock-free
    /// `RoutingSnapshot` mirror a caller would otherwise have to read
    /// (and could only ever read as a SEPARATE step from the write,
    /// leaving the same class of gap `install_pin`'s own doc comment
    /// describes -- reading a published copy and then acting on it is
    /// never atomic with a DIFFERENT owner command that changes the pin
    /// in between, no matter how tightly the read and the write are held
    /// together on the caller's side). Running as part of the owner's own
    /// single-threaded command processing, with no other command able to
    /// run until this one returns, is what makes the check and the write
    /// here one indivisible step instead of two.
    ///
    /// Declines (no mutation) when `peer_id` is operator-pinned to a
    /// DIFFERENT address: an ordinary connect must never undo a pin's
    /// synchronized route. Reuses `RoutingPublisher::
    /// set_configured_peer_addr` for the actual write -- the SAME method
    /// `install_pin`/`migrate` call -- so the write (and its own
    /// reindex) is identical in shape to the pin-driven case; this
    /// command only adds the conflict check in front of it, and does NOT
    /// install a pin itself.
    ///
    /// Returns whether `addr` is now (or already was) the effective
    /// route -- see `RegistryOwnerHandle::set_ordinary_connect_route`'s
    /// doc comment for why the caller MUST consult this rather than
    /// assume the write happened.
    fn set_ordinary_connect_route(&self, peer_id: &PeerId, addr: SocketAddr) -> bool {
        if let Some(pinned) = self.pinned_by_peer.get(peer_id)
            && *pinned != addr
        {
            trace!(
                peer_id = %peer_id,
                addr = %addr,
                pinned_addr = %pinned,
                "ordinary connect route update declined: peer is operator-pinned to a \
                 different address"
            );
            return false;
        }
        if let Some(routing) = self.routing.upgrade() {
            // Never a pin eviction: this command only ever runs for an
            // ordinary connect, and declines outright above whenever the
            // peer is pinned anywhere else -- there is nothing to evict.
            routing.set_configured_peer_addr(addr, peer_id, None);
        }
        true
    }

    /// `OwnerCommand::ConfigurePeer`'s handler: the atomic transaction
    /// behind `GossipRegistry::configure_peer`. Claims `addr` for `peer_id`
    /// with `ClaimKind::Verified` and, only if that claim is accepted,
    /// installs the operator pin in the SAME synchronous step -- so no other
    /// owner command can ever be processed between the claim taking effect
    /// and the pin landing, and by the time `install_pin` runs, `peer_id`
    /// claiming `addr` is not merely believed but a fact this exact call
    /// itself just committed.
    ///
    /// If installing the pin evicts a DIFFERENT address this same peer was
    /// previously pinned at, that address's ownership is released in this
    /// SAME step too, when `peer_id` still holds it -- not left for a
    /// caller to reclaim afterward through a separately-ordered `release`
    /// call a concurrent `migrate` could race ahead of. This is what closes
    /// the window in which the evicted address is unpinned but still
    /// "owned" by a peer that has already moved its configuration
    /// elsewhere.
    ///
    /// P1 finding (review round against `3e260a9`, `registry_owner.rs:2716`):
    /// the evicted address's own `ReapReservation`, if a `cleanup_dead_peers`
    /// sweep currently holds one for it, used to stay valid regardless --
    /// this claim/pin transaction never touched `reap_reserved` at all, only
    /// `addr_ownership`. A sweep already mid-destruction for the evicted
    /// address (side tables and actors gated on `is_still_valid()`, which
    /// this left untouched) would carry on deleting the peer's capabilities
    /// and actors and emitting `ActorRemoved` tombstones for a peer the
    /// operator is, at this exact moment, actively reconfiguring elsewhere.
    ///
    /// `migrate` already refuses outright when either endpoint is
    /// reap-reserved (`MigrateOutcome::ReapInProgress`). A pin-changing
    /// configure follows the same rule for the peer's previous pin: the
    /// owner checks it before the destination claim and returns the same
    /// temporary rejection. The public registry path retries that rejection
    /// after the sweep releases its reservation, so no caller-side follow-up
    /// can evict an address while destructive cleanup still has authority to
    /// act on it. This is stricter than invalidating the reservation after
    /// the eviction, because a consumed reap has already crossed its own
    /// destructive linearization point.
    ///
    /// P1 finding (review round against `ded8495`, `registry.rs:4982`):
    /// `expected_generation` is validated FIRST, atomically, before the
    /// claim below is even attempted -- see `configure_peer_generation`'s
    /// own doc comment for the caller-side race this closes. `None` (the
    /// FIRST call for this peer) always proceeds and bumps the counter to
    /// a new value; `Some(g)` proceeds only if `g` is still current (no
    /// LATER call has bumped it further), otherwise this returns
    /// `SupersededByNewerConfiguration` immediately, having touched
    /// nothing else at all -- not even a rejected claim attempt.
    fn configure_peer(
        &mut self,
        addr: SocketAddr,
        peer_id: PeerId,
        expected_generation: Option<u64>,
    ) -> ConfigurePeerCommit {
        let current_generation = self
            .configure_peer_generation
            .get(&peer_id)
            .copied()
            .unwrap_or(0);
        // A reconfiguration also mutates the peer's previous pin: the
        // eviction/release below must not race a reap already committed for
        // that address. Treat the collision as the same temporary refusal as
        // a reserved destination, before bumping the request generation or
        // attempting the new claim.
        if self
            .pinned_by_peer
            .get(&peer_id)
            .is_some_and(|previous| *previous != addr && self.reap_reserved.contains_key(previous))
        {
            return ConfigurePeerCommit {
                claim: ClaimCommit::Rejected(ClaimRejection::ReapInProgress),
                evicted_pin: None,
                evicted_release_seq: None,
                generation: current_generation,
            };
        }
        let generation = match expected_generation {
            Some(expected) if expected < current_generation => {
                return ConfigurePeerCommit {
                    claim: ClaimCommit::Rejected(ClaimRejection::SupersededByNewerConfiguration),
                    evicted_pin: None,
                    evicted_release_seq: None,
                    generation: current_generation,
                };
            }
            Some(expected) => expected,
            None => {
                let bumped = current_generation + 1;
                self.configure_peer_generation
                    .insert(peer_id.clone(), bumped);
                bumped
            }
        };
        let claim = Claim {
            node_id: peer_id.clone(),
            kind: ClaimKind::Verified,
        };
        let commit = self.claim(addr, claim, /* is_local_addr */ false);
        if !commit.is_accepted() {
            return ConfigurePeerCommit {
                claim: commit,
                evicted_pin: None,
                evicted_release_seq: None,
                generation,
            };
        }
        let evicted_pin = self.install_pin(addr, peer_id.clone());
        let evicted_release_seq = evicted_pin.and_then(|evicted_addr| {
            // Ghost connection-scoped receipts for the evicted address must
            // not survive its release either -- the same cleanup `release`
            // itself performs, just folded into this atomic step instead of
            // a separately-ordered call.
            self.connection_scoped_claims
                .retain(|key, _| !(key.0 == peer_id && key.2 == evicted_addr));
            let still_owned = self
                .addr_ownership
                .get(&evicted_addr)
                .is_some_and(|owner| owner.node_id == peer_id);
            still_owned.then(|| {
                let owner = self
                    .addr_ownership
                    .remove(&evicted_addr)
                    .expect("still_owned just confirmed this entry exists");
                self.retract_owner(evicted_addr, owner)
            })
        });
        ConfigurePeerCommit {
            claim: commit,
            evicted_pin,
            evicted_release_seq,
            generation,
        }
    }

    /// Shared tail of every path that drops a recorded owner: clear its
    /// generation, purge any connection-scoped receipts still recorded
    /// against it, advance the commit order, publish the vacancy, and
    /// retract the routing publication. Callers are responsible for removing
    /// `owner` from `addr_ownership` (and for whatever ownership-match check
    /// justified doing so) before calling this.
    ///
    /// The receipt purge is unconditional on identity, not scoped to
    /// `owner` alone: `addr` is being fully vacated, so ANY receipt still
    /// keyed to it -- under any identity -- refers to a lifecycle
    /// generation that no longer exists. This is what makes receipt
    /// reconciliation a property of every ownership retraction rather than
    /// something each call site must remember to do itself. A caller that
    /// already purged its own identity's receipts before calling this (e.g.
    /// `release_dead_peer`'s ghost-receipt cleanup for a specific peer) is
    /// unaffected -- this is a second, harmless pass over an already-empty
    /// set for that identity. A caller that does NOT purge first (e.g. a
    /// generic peer-table eviction going straight through `release`) is
    /// exactly the gap this closes: left behind, such a receipt is later
    /// silently updated to a NEW generation by the same identity's next
    /// reconnect (see `claim_connection_scoped`'s same-peer transfer), and
    /// that reconnect's own eventual teardown then finds an apparently
    /// still-live second session that in fact tore down long ago --
    /// permanently stranding the address.
    fn retract_owner(&mut self, addr: SocketAddr, owner: Owner) -> CommitSeq {
        self.claim_generation.remove(&addr);
        self.claim_committed_at.remove(&addr);
        // Mirrors `claim_committed_at`'s own cleanup above -- prevents
        // unbounded growth over a long-running process's lifetime as
        // addresses churn (peer reconnects elsewhere, DNS migration,
        // dead-peer reap). A fresh claim for this address afterward starts
        // with no stale liveness evidence to accidentally protect it.
        self.liveness_evidence_at.remove(&addr);
        self.connection_scoped_claims.retain(|key, _| key.2 != addr);
        let commit_seq = self.advance();
        self.publish_owner_snapshot(addr, None);
        if let Some(routing) = self.routing.upgrade() {
            routing.retract_owner(addr, &owner.node_id);
        }
        commit_seq
    }

    /// Move ownership of `from` onto `to`.
    ///
    /// The DESTINATION is inspected first, and a destination held by another
    /// identity blocks the move even when the source has no owner at all. An
    /// unowned source is the normal state of a seed configured by host name
    /// before any handshake; reporting "nothing to move" for it without
    /// looking at the destination would tell the caller it is free to re-key
    /// its peer entry and connection index onto an address that another
    /// identity legitimately owns, silently stealing that identity's routing.
    /// "Nothing to move" is therefore only reported once the destination is
    /// known to be free (or already held by the source's own identity).
    ///
    /// `expected_source` pins the exact source state observed by the caller.
    /// It is checked here, inside the serialized command, so a later owner or
    /// same-identity generation cannot be carried by a stale migration.
    fn migrate(
        &mut self,
        from: SocketAddr,
        to: SocketAddr,
        expected_source: &SourceExpectation,
        is_local_addr: bool,
    ) -> MigrateOutcome {
        // See `MigrateOutcome::ReapInProgress`'s doc comment: this command
        // mutates `addr_ownership`/`claim_committed_at` for BOTH addresses
        // directly, without going through `claim`'s own `reap_reserved`
        // check, so it is checked here instead -- before `is_local_addr`,
        // before either address's current state is even read. A sweep
        // holding a reservation for either end is relying on both staying
        // fixed for the duration of its destructive work; this refusal is
        // what makes that true regardless of which owner command an
        // in-flight mutation happens to arrive through.
        if self.reap_reserved.contains_key(&from) || self.reap_reserved.contains_key(&to) {
            trace!(
                from = %from,
                to = %to,
                "address migration refused: a dead-peer reap reservation is held for the \
                 source, the destination, or both"
            );
            return MigrateOutcome::ReapInProgress;
        }
        if is_local_addr {
            trace!(
                from = %from,
                to = %to,
                "address migration refused: destination is local"
            );
            return MigrateOutcome::TargetIsLocal;
        }
        let source = self.addr_ownership.get(&from).cloned();
        let source_matches = match expected_source {
            SourceExpectation::Unowned => source.is_none(),
            SourceExpectation::Owned(expected) => {
                source
                    .as_ref()
                    .is_some_and(|owner| owner.node_id == *expected.owner())
                    && self.claim_generation.get(&from) == Some(&expected.generation())
            }
        };
        if !source_matches {
            trace!(
                from = %from,
                to = %to,
                expected = ?expected_source,
                "address migration refused: source owner generation changed"
            );
            return MigrateOutcome::SourceOwnerMismatch;
        }
        let destination = self.addr_ownership.get(&to).cloned();
        if let Some(existing) = destination.as_ref() {
            let same_identity = source
                .as_ref()
                .is_some_and(|owner| owner.node_id == existing.node_id);
            if !same_identity {
                return MigrateOutcome::TargetOwnedByOther;
            }
        }
        let Some(owner) = source else {
            return MigrateOutcome::SourceUnowned;
        };
        // Merging onto an address the same identity already holds keeps the
        // stronger of the two kinds. Carrying the source's kind across
        // unconditionally could turn a destination that is already backed by
        // an observed connection back into a self-reported one, making it
        // displaceable again by a claim it had already earned the right to
        // refuse. The never-downgrade rule is the arbitration core's, reused
        // here rather than restated.
        let kind = resolved_kind(
            destination.as_ref(),
            &Claim {
                node_id: owner.node_id.clone(),
                kind: owner.kind,
            },
        );
        let owner = Owner {
            node_id: owner.node_id,
            kind,
        };
        self.addr_ownership.remove(&from);
        self.addr_ownership.insert(to, owner.clone());
        // An operator pin travels with its address's re-resolution rather
        // than being silently dropped: an offline configured peer whose DNS
        // entry moves must stay protected at its new address, not become
        // momentarily displaceable until the operator reconfigures it again.
        // `pinned_by_peer` is updated in the same step so a concurrent `pin`
        // command for this same peer -- whichever side of this migrate the
        // owner task happens to serialize it on -- always evicts from
        // wherever the pin ACTUALLY is, never leaving two addresses pinned
        // for one peer (see `pin`'s doc comment).
        let migrated_pin = self.operator_pinned.remove(&from);
        if let Some(pinned_peer) = migrated_pin.clone() {
            self.operator_pinned.insert(to, pinned_peer.clone());
            self.pinned_by_peer.insert(pinned_peer.clone(), to);
            // The pin's `ConnectionPool` configured/required route must move
            // with it in this SAME command -- exactly the ordering-domain
            // unification `pin` already applies for `configure_peer`.
            // Leaving this publish to some later, separately-ordered step
            // would let a DNS migration reintroduce the pin/route
            // divergence through a different door: the owner would protect
            // `to`, but `ConnectionPool::get_required_peer_addr` would keep
            // reporting the stale `from` until the operator reconfigured
            // the peer again.
            //
            // `from` is passed as the evicted address too, for the exact
            // same reason `install_pin` passes its own evicted address --
            // see `RoutingPublisher::set_configured_peer_addr`'s doc
            // comment: this pin's `connections_by_addr[from]` alias must
            // not survive the move, or a later, different identity
            // claiming `from` inherits this peer's still-live connection
            // via `get_connection_by_peer_id`'s address fallback.
            if let Some(routing) = self.routing.upgrade() {
                routing.set_configured_peer_addr(to, &pinned_peer, Some(from));
            }
        }
        let commit_seq = self.advance();
        self.claim_generation.remove(&from);
        self.claim_generation.insert(to, commit_seq);
        // Connection-scoped receipts move with the ownership they back, in
        // this SAME step. `from` is now unowned in `addr_ownership` --
        // exactly the same "this address's owner just changed" event
        // `retract_owner` purges receipts for -- so any receipt still keyed
        // to it, under an identity OTHER than the one migrating, no longer
        // refers to a live generation and is dropped rather than carried
        // anywhere. The receipts that belonged to the identity actually
        // migrating are re-homed at `to` instead, carrying the new
        // generation forward; any receipt already at `to` for that same
        // identity (the same-identity merge case) is bumped to the same new
        // generation too, since `to`'s own `claim_generation` just advanced
        // regardless of whether anything moved onto it. Left alone, either
        // shape strands a later, genuinely correct teardown: it can never
        // find a receipt at the CURRENT generation to release, and the
        // address becomes unreleasable through the connection-scoped path
        // for good -- the same failure mode `retract_owner`'s purge closes
        // for a plain release, just reached through a move instead.
        let mut migrated_receipts = Vec::new();
        self.connection_scoped_claims.retain(|key, generation| {
            if key.2 != from {
                return true;
            }
            if key.0 == owner.node_id {
                migrated_receipts.push((key.1, *generation));
            }
            false
        });
        for (session_source, _stale_generation) in migrated_receipts {
            self.connection_scoped_claims
                .insert((owner.node_id.clone(), session_source, to), commit_seq);
        }
        for (key, generation) in self.connection_scoped_claims.iter_mut() {
            if key.0 == owner.node_id && key.2 == to {
                *generation = commit_seq;
            }
        }
        // Carried over, never reset to "now": `migrate` is exclusively
        // DNS-refresh-triggered in production (see `refresh_peer_dns`,
        // itself run as part of a RETRY for a peer that is already
        // failing), not direct evidence of a live connection. Resetting
        // this here would let repeated DNS lookups for a peer that never
        // actually reconnects keep `release_dead_peer`'s freshness fence
        // perpetually satisfied, the same failure mode a gossip/discovery
        // claim refreshing it would cause (see `claim`'s doc comment).
        //
        // `to` may already be owned by the same identity (the merge case
        // above), and therefore may already have its OWN, strictly newer
        // direct-evidence timestamp than `from`'s -- e.g. the peer
        // independently (re)connected at `to` before this migration ever
        // ran. Take the newer of the two rather than unconditionally
        // overwriting: a migration must never make an address look LESS
        // fresh than it actually is, the same "measuring the wrong event"
        // shape as a gossip/discovery claim making one look MORE fresh than
        // it actually is. If `from` never had direct evidence, `to`'s
        // existing entry (if any) is left untouched; if `to` never had one,
        // `from`'s is carried forward unchanged.
        if let Some(from_committed_at) = self.claim_committed_at.remove(&from) {
            self.claim_committed_at
                .entry(to)
                .and_modify(|to_committed_at| {
                    if from_committed_at > *to_committed_at {
                        *to_committed_at = from_committed_at;
                    }
                })
                .or_insert(from_committed_at);
        }
        // Same carry-forward, same reasoning, for the complementary
        // ordinary-liveness fence (`liveness_evidence_at`) `release_dead_peer`
        // also checks -- see `claim_committed_at`'s handling just above.
        if let Some(from_seen_at) = self.liveness_evidence_at.remove(&from) {
            self.liveness_evidence_at
                .entry(to)
                .and_modify(|to_seen_at| {
                    if from_seen_at > *to_seen_at {
                        *to_seen_at = from_seen_at;
                    }
                })
                .or_insert(from_seen_at);
        }
        let snapshot = self.snapshot.load_full();
        let mut snapshot = snapshot
            .with_owner(from, None)
            .with_owner(to, Some((owner.clone(), commit_seq)));
        // The pin's own publication moves with it, in this SAME snapshot
        // construction, for the same reason its `ConnectionPool` route
        // does above: a caller revalidating "is my pin still current" (see
        // `RoutingSnapshot::pin_is_current`) must never observe a window
        // where the owner's own pin bookkeeping has already moved to `to`
        // but the published snapshot still shows `from`, or worse, shows
        // neither.
        if let Some(pinned_peer) = migrated_pin {
            snapshot = snapshot
                .with_pin(from, None)
                .with_pin(to, Some(pinned_peer));
        }
        self.snapshot.store(Arc::new(snapshot));
        if let Some(routing) = self.routing.upgrade() {
            routing.retract_owner(from, &owner.node_id);
            routing.publish_owner(to, &owner.node_id);
        }
        MigrateOutcome::Migrated { commit_seq }
    }

    /// Take the next position in the commit order. Called exactly once per
    /// committed mutation, before the snapshot that mutation publishes.
    fn advance(&mut self) -> CommitSeq {
        self.commit_seq += 1;
        self.commit_seq
    }

    /// Publish one address update while structurally sharing every untouched
    /// shard with the previous immutable snapshot.
    fn publish_owner_snapshot(&self, addr: SocketAddr, owner: Option<(Owner, CommitSeq)>) {
        let snapshot = self.snapshot.load_full();
        self.snapshot
            .store(Arc::new(snapshot.with_owner(addr, owner)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::Duration;

    /// Records every routing publication so tests can assert that a rejected
    /// claim published NOTHING.
    #[derive(Default)]
    struct RecordingPublisher {
        events: Mutex<Vec<(SocketAddr, Option<PeerId>)>>,
        configured_routes: Mutex<Vec<(SocketAddr, PeerId, Option<SocketAddr>)>>,
    }

    impl RecordingPublisher {
        fn events(&self) -> Vec<(SocketAddr, Option<PeerId>)> {
            self.events.lock().expect("publisher mutex").clone()
        }

        fn configured_routes(&self) -> Vec<(SocketAddr, PeerId, Option<SocketAddr>)> {
            self.configured_routes
                .lock()
                .expect("publisher mutex")
                .clone()
        }
    }

    impl RoutingPublisher for RecordingPublisher {
        fn publish_owner(&self, addr: SocketAddr, peer_id: &PeerId) {
            self.events
                .lock()
                .expect("publisher mutex")
                .push((addr, Some(peer_id.clone())));
        }

        fn retract_owner(&self, addr: SocketAddr, _peer_id: &PeerId) {
            self.events
                .lock()
                .expect("publisher mutex")
                .push((addr, None));
        }

        fn set_configured_peer_addr(
            &self,
            addr: SocketAddr,
            peer_id: &PeerId,
            evicted_addr: Option<SocketAddr>,
        ) {
            self.configured_routes
                .lock()
                .expect("publisher mutex")
                .push((addr, peer_id.clone(), evicted_addr));
        }
    }

    fn owner_handle() -> (RegistryOwnerHandle, Arc<RecordingPublisher>) {
        let publisher = Arc::new(RecordingPublisher::default());
        let weak: Weak<dyn RoutingPublisher> = Arc::downgrade(&publisher) as _;
        (RegistryOwnerHandle::new(weak), publisher)
    }

    fn peer(tag: &str) -> PeerId {
        crate::KeyPair::new_for_testing(format!("registry-owner-{tag}")).peer_id()
    }

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    fn claim_of(node_id: PeerId, kind: ClaimKind) -> Claim {
        Claim { node_id, kind }
    }

    fn current_source(owner: &RegistryOwnerHandle, addr: SocketAddr) -> SourceExpectation {
        owner
            .ownership_token(&addr)
            .map(SourceExpectation::Owned)
            .unwrap_or(SourceExpectation::Unowned)
    }

    #[test]
    fn routing_retract_is_conditional_on_the_current_owner() {
        let pool = crate::connection_pool::ConnectionPool::<()>::new(8, Duration::from_secs(1));
        let addr = addr(30_000);
        let old_owner = peer("retract-old");
        let current_owner = peer("retract-current");

        let _ = pool
            .addr_to_peer_id
            .upsert_sync(addr, current_owner.clone());
        <crate::connection_pool::ConnectionPool as RoutingPublisher>::retract_owner(
            &pool, addr, &old_owner,
        );
        assert_eq!(
            pool.addr_to_peer_id
                .read_sync(&addr, |_, owner| owner.clone()),
            Some(current_owner.clone()),
            "a stale retract must not remove a newer route"
        );

        <crate::connection_pool::ConnectionPool as RoutingPublisher>::retract_owner(
            &pool,
            addr,
            &current_owner,
        );
        assert!(pool.addr_to_peer_id.read_sync(&addr, |_, _| ()).is_none());
    }

    /// Verified first, then a competing Provisional: the truth table's
    /// `VerifiedOwnerPresent` rule survives the move into the actor, and the
    /// loser publishes no routing change.
    #[tokio::test]
    async fn verified_then_provisional_keeps_the_verified_owner() {
        let (owner, publisher) = owner_handle();
        let a = peer("a");
        let b = peer("b");
        let target = addr(30_001);

        let first = owner
            .claim(target, claim_of(a.clone(), ClaimKind::Verified), false)
            .await;
        assert!(first.is_accepted());
        let published_after_first = publisher.events();

        let second = owner
            .claim(target, claim_of(b.clone(), ClaimKind::Provisional), false)
            .await;
        assert_eq!(
            second,
            ClaimCommit::Rejected(ClaimRejection::Arbitration(
                RejectReason::VerifiedOwnerPresent
            ))
        );
        assert_eq!(
            publisher.events(),
            published_after_first,
            "a rejected claim must publish no routing change"
        );
        assert_eq!(owner.routes_to(&target), Some(a));
    }

    /// A self-reported address cannot become a first owner. The subsequent
    /// genuinely verified claim becomes the first published owner without
    /// inheriting or displacing any squatter state.
    #[tokio::test]
    async fn provisional_first_claim_publishes_nothing_then_verified_claim_owns() {
        let (owner, publisher) = owner_handle();
        let squatter = peer("squatter");
        let real = peer("real");
        let target = addr(30_002);

        let first = owner
            .claim(
                target,
                claim_of(squatter.clone(), ClaimKind::Provisional),
                false,
            )
            .await;
        assert_eq!(
            first,
            ClaimCommit::Rejected(ClaimRejection::Arbitration(RejectReason::UnverifiedAddress))
        );
        assert!(owner.routes_to(&target).is_none());
        assert!(publisher.events().is_empty());

        let second = owner
            .claim(target, claim_of(real.clone(), ClaimKind::Verified), false)
            .await;
        assert_eq!(
            second,
            ClaimCommit::Accepted {
                kind: ClaimKind::Verified,
                displaced: None,
                created_ownership: true,
                commit_seq: 1,
            }
        );
        assert!(
            !second.owner_changed(),
            "a refused self-report never became an owner to displace"
        );
        assert_eq!(owner.routes_to(&target), Some(real.clone()));
        assert_eq!(
            publisher.events(),
            vec![(target, Some(real))],
            "only the verified claim may publish routing"
        );
    }

    /// A same-node refresh never downgrades the recorded kind, going through
    /// the actor exactly as the pure function does.
    #[tokio::test]
    async fn same_node_refresh_never_downgrades_kind() {
        let (owner, _publisher) = owner_handle();
        let node = peer("refresh");
        let target = addr(30_003);

        owner
            .claim(target, claim_of(node.clone(), ClaimKind::Verified), false)
            .await;
        let refresh = owner
            .claim(
                target,
                claim_of(node.clone(), ClaimKind::Provisional),
                false,
            )
            .await;
        assert_eq!(
            refresh,
            ClaimCommit::Accepted {
                kind: ClaimKind::Verified,
                displaced: None,
                created_ownership: false,
                commit_seq: 2,
            }
        );
        assert_eq!(
            owner.owner_of(&target).map(|owner| owner.kind),
            Some(ClaimKind::Verified)
        );
    }

    /// Routine FullSync refreshes publish their new lifecycle generation but
    /// do not republish an identical address route.
    #[tokio::test]
    async fn unchanged_same_owner_refresh_publishes_generation_not_route() {
        let (owner, publisher) = owner_handle();
        let node = peer("unchanged-refresh");
        let target = addr(30_023);

        owner
            .claim(target, claim_of(node.clone(), ClaimKind::Verified), false)
            .await;
        let first_generation = owner
            .ownership_token(&target)
            .expect("first ownership token")
            .generation();
        let published = publisher.events();

        let refresh = owner
            .claim(target, claim_of(node, ClaimKind::Provisional), false)
            .await;

        assert_eq!(
            refresh.commit_seq(),
            Some(2),
            "the projection fence advances"
        );
        assert_eq!(
            owner
                .ownership_token(&target)
                .expect("refreshed ownership token")
                .generation(),
            2,
            "the lock-free authority snapshot must publish the refresh generation"
        );
        assert!(first_generation < 2);
        assert_eq!(
            publisher.events(),
            published,
            "an unchanged refresh must not republish an identical address route"
        );
    }

    /// A distinct accepted claim must copy only its bounded routing shard,
    /// not every ownership entry accumulated so far. Keeping the prior
    /// snapshot alive makes allocation reuse impossible, so pointer identity
    /// of an owner in another shard proves that storage was structurally
    /// shared across the publication.
    #[tokio::test]
    async fn distinct_claim_reuses_untouched_routing_snapshot_shard() {
        use std::hash::{Hash, Hasher};

        fn intended_shard(addr: &SocketAddr) -> usize {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            addr.hash(&mut hasher);
            hasher.finish() as usize % 64
        }

        let (owner, _publisher) = owner_handle();
        let retained_addr = addr(30_024);
        let retained_peer = peer("structural-share-retained");
        owner
            .claim(
                retained_addr,
                claim_of(retained_peer, ClaimKind::Verified),
                false,
            )
            .await;
        let before = owner.snapshot();
        let retained_ptr = before.owner(&retained_addr).expect("retained owner") as *const Owner;

        let inserted_addr = (30_025..=u16::MAX)
            .map(addr)
            .find(|candidate| intended_shard(candidate) != intended_shard(&retained_addr))
            .expect("an address in another shard");
        owner
            .claim(
                inserted_addr,
                claim_of(peer("structural-share-inserted"), ClaimKind::Verified),
                false,
            )
            .await;

        let after = owner.snapshot();
        let retained_after_ptr = after
            .owner(&retained_addr)
            .expect("retained owner after update") as *const Owner;
        assert_eq!(
            retained_ptr, retained_after_ptr,
            "an accepted claim must reuse every untouched snapshot shard"
        );
    }

    /// The local address is reserved: no remote claim is ever adopted and
    /// nothing is published.
    #[tokio::test]
    async fn local_address_claim_commits_nothing() {
        let (owner, publisher) = owner_handle();
        let target = addr(30_004);

        let commit = owner
            .claim(target, claim_of(peer("local"), ClaimKind::Verified), true)
            .await;
        assert_eq!(
            commit,
            ClaimCommit::Rejected(ClaimRejection::Arbitration(RejectReason::LocalAddress))
        );
        assert!(owner.snapshot().is_empty());
        assert!(publisher.events().is_empty());
    }

    /// Snapshot correctness: a committed claim is visible to a lock-free read
    /// the instant the reply is observed, and a rejected one leaves the
    /// previously published snapshot pointer-identical.
    #[tokio::test]
    async fn snapshot_reflects_commits_and_is_untouched_by_rejections() {
        let (owner, _publisher) = owner_handle();
        let a = peer("snap-a");
        let b = peer("snap-b");
        let target = addr(30_005);

        assert!(owner.snapshot().is_empty());
        owner
            .claim(target, claim_of(a.clone(), ClaimKind::Verified), false)
            .await;
        let after_accept = owner.snapshot();
        assert_eq!(
            after_accept.owner(&target),
            Some(&Owner {
                node_id: a,
                kind: ClaimKind::Verified,
            })
        );

        owner
            .claim(target, claim_of(b, ClaimKind::Verified), false)
            .await;
        assert!(
            Arc::ptr_eq(&after_accept, &owner.snapshot()),
            "a rejected claim must not republish the snapshot at all"
        );
    }

    /// The case that was unfixable before the actor: two conflicting claims
    /// for the same address issued concurrently. Whatever the interleaving,
    /// they cannot both observe pre-claim state and both commit — exactly one
    /// accepts, and the published routing agrees with it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_conflicting_claims_cannot_both_commit() {
        let (owner, publisher) = owner_handle();
        let a = peer("race-a");
        let b = peer("race-b");
        let target = addr(30_006);

        let left = {
            let owner = owner.clone();
            let a = a.clone();
            tokio::spawn(async move {
                owner
                    .claim(target, claim_of(a, ClaimKind::Verified), false)
                    .await
            })
        };
        let right = {
            let owner = owner.clone();
            let b = b.clone();
            tokio::spawn(async move {
                owner
                    .claim(target, claim_of(b, ClaimKind::Verified), false)
                    .await
            })
        };

        let left = left.await.expect("left claim task");
        let right = right.await.expect("right claim task");

        let accepted = [&left, &right]
            .into_iter()
            .filter(|commit| commit.is_accepted())
            .count();
        assert_eq!(accepted, 1, "exactly one of two verified claims may commit");

        let routed = owner
            .routes_to(&target)
            .expect("address must have an owner");
        assert!(routed == a || routed == b);
        assert_eq!(
            publisher.events(),
            vec![(target, Some(routed.clone()))],
            "routing must be published exactly once, for the winner"
        );
        assert_eq!(owner.snapshot().len(), 1);
    }

    /// Liveness under contention: many concurrent claimants for one address
    /// resolve to a single owner, with routing agreeing, and no hang.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn many_concurrent_claims_settle_on_one_owner() {
        const CLAIMANTS: usize = 64;
        let (owner, publisher) = owner_handle();
        let target = addr(30_007);

        let mut tasks = Vec::with_capacity(CLAIMANTS);
        for index in 0..CLAIMANTS {
            let owner = owner.clone();
            let claimant = peer(&format!("storm-{index}"));
            tasks.push(tokio::spawn(async move {
                owner
                    .claim(
                        target,
                        claim_of(claimant.clone(), ClaimKind::Verified),
                        false,
                    )
                    .await
                    .is_accepted()
            }));
        }

        let results =
            tokio::time::timeout(Duration::from_secs(10), futures::future::join_all(tasks))
                .await
                .expect("claim storm must not hang");
        let accepted = results
            .into_iter()
            .filter(|result| *result.as_ref().expect("claim task"))
            .count();

        assert_eq!(accepted, 1, "exactly one verified owner may result");
        let routed = owner
            .routes_to(&target)
            .expect("address must have an owner");
        assert_eq!(publisher.events(), vec![(target, Some(routed))]);
        assert_eq!(owner.snapshot().len(), 1);
    }

    /// Release drops ownership only for the identity that still holds it, and
    /// retracts the routing publication with it.
    #[tokio::test]
    async fn release_is_owner_scoped_and_retracts_routing() {
        let (owner, publisher) = owner_handle();
        let holder = peer("release-holder");
        let other = peer("release-other");
        let target = addr(30_008);

        let claim = owner
            .claim(target, claim_of(holder.clone(), ClaimKind::Verified), false)
            .await;
        let generation = claim.commit_seq().expect("holder claim commits");
        assert!(
            owner.release(target, other, generation).await.is_none(),
            "a non-owner must not be able to release the address"
        );
        assert_eq!(owner.routes_to(&target), Some(holder.clone()));

        assert!(owner.release(target, holder, generation).await.is_some());
        assert!(owner.snapshot().is_empty());
        assert_eq!(
            publisher.events().last().map(|event| event.1.clone()),
            Some(None)
        );

        // Released addresses are reclaimable by a different identity.
        let reclaim = owner
            .claim(
                target,
                claim_of(peer("reclaim"), ClaimKind::Verified),
                false,
            )
            .await;
        assert!(reclaim.is_accepted());
    }

    /// P2 regression: `release` -- the generic retraction path every
    /// non-connection-scoped eviction routes through, not just
    /// `release_session`'s own teardown -- used to clear ownership state
    /// but leave any connection-scoped receipt still recorded for the
    /// address behind. If the same peer later reclaims the address,
    /// `claim_connection_scoped`'s same-peer receipt transfer silently
    /// carries that ghost forward to the new generation, and the new
    /// session's own, entirely legitimate teardown then sees an apparently
    /// still-live second session -- one that in fact tore down before the
    /// reclaim ever happened, through a path that never called
    /// `release_session` -- and can never release. Asserts the ghost-
    /// revival consequence directly (a later teardown CAN release), not
    /// merely that the receipt map happens to be empty somewhere, which is
    /// reachable for uninteresting reasons too.
    #[tokio::test]
    async fn generic_release_purges_receipts_so_a_later_reclaim_can_still_be_released() {
        let (owner, _publisher) = owner_handle();
        let node = peer("generic-release-receipt-purge");
        let target = addr(30_015);
        let old_session = addr(30_115);
        let new_session = addr(30_215);

        let claim = owner
            .claim_connection_scoped(
                target,
                claim_of(node.clone(), ClaimKind::Verified),
                old_session,
            )
            .await;
        let generation = claim.commit_seq().expect("initial claim commits");

        // A generic retraction -- e.g. peer-table eviction -- releases the
        // address WITHOUT ever going through the connection-scoped,
        // receipt-aware teardown path (`release_session`).
        assert!(
            owner
                .release(target, node.clone(), generation)
                .await
                .is_some(),
            "the generic release itself must succeed"
        );

        // The peer reclaims the address with a brand-new session.
        let reclaim = owner
            .claim_connection_scoped(
                target,
                claim_of(node.clone(), ClaimKind::Verified),
                new_session,
            )
            .await;
        assert!(reclaim.is_accepted());

        // The new session's own, entirely legitimate teardown must be
        // recognized as the SOLE remaining session, not shadowed by a
        // ghost receipt the generic release above should have purged --
        // and must actually release the address itself, atomically, as
        // part of this same call (see `release_session`'s doc comment).
        let released = owner.release_session(node.clone(), new_session).await;
        assert_eq!(
            released
                .iter()
                .map(|(candidate_addr, _)| *candidate_addr)
                .collect::<Vec<_>>(),
            vec![target],
            "the new session must be the sole session covering `target`, not shadowed by \
             a ghost receipt the generic release should have purged, and its own teardown \
             must actually release the address -- the ghost-revival consequence, not \
             merely an empty receipt map"
        );
        assert_eq!(
            owner.routes_to(&target),
            None,
            "the address must actually be released, not merely reported as a candidate"
        );
    }

    /// A connection-scoped claim's liveness timestamp is evidence from the
    /// transport, not from the instant the owner happens to dequeue it. If
    /// the command waits behind owner work, sampling in the handler can make
    /// evidence that predates a recorded failure look newer and permanently
    /// protect a dead address from cleanup.
    #[tokio::test]
    async fn connection_claim_preserves_evidence_time_across_owner_queueing() {
        let (owner, _publisher) = owner_handle();
        let node = peer("connection-claim-evidence-time");
        let target = addr(30_216);
        let session = addr(30_217);
        let evidence_at = std::time::Instant::now()
            .checked_sub(Duration::from_secs(1))
            .expect("test clock supports a one-second history");

        let claim = owner
            .claim_connection_scoped_at(
                target,
                claim_of(node.clone(), ClaimKind::Verified),
                session,
                legacy_connection_instance_id(session),
                evidence_at,
            )
            .await;
        assert!(claim.is_accepted());

        let failure_at = std::time::Instant::now();
        assert!(
            matches!(
                owner.release_dead_peer(node, target, failure_at).await,
                DeadPeerReleaseOutcome::Released(_)
            ),
            "evidence recorded before the failure must not be refreshed to the owner dequeue time"
        );
    }

    /// A socket tuple is diagnostic metadata, not a transport-session key.
    /// When a replacement reuses the same tuple, tearing down the predecessor
    /// must remove only the predecessor's receipt and leave the replacement's
    /// ownership live until its own instance exits.
    #[tokio::test]
    async fn sequential_connection_instances_do_not_release_a_replacement() {
        let (owner, _publisher) = owner_handle();
        let node = peer("sequential-connection-instance");
        let target = addr(30_218);
        let session_source = addr(30_219);

        let first = owner
            .claim_connection_scoped_at(
                target,
                claim_of(node.clone(), ClaimKind::Verified),
                session_source,
                11,
                std::time::Instant::now(),
            )
            .await;
        assert!(first.is_accepted());

        let second = owner
            .claim_connection_scoped_at(
                target,
                claim_of(node.clone(), ClaimKind::Verified),
                session_source,
                12,
                std::time::Instant::now(),
            )
            .await;
        assert!(second.is_accepted());

        assert!(
            owner
                .release_session_for_instance(node.clone(), session_source, 11)
                .await
                .is_empty(),
            "the predecessor's teardown must not retract while the replacement receipt exists"
        );
        assert_eq!(owner.routes_to(&target), Some(node.clone()));

        let released = owner
            .release_session_for_instance(node.clone(), session_source, 12)
            .await;
        assert_eq!(released.len(), 1);
        assert_eq!(released[0].0, target);
        assert_eq!(owner.routes_to(&target), None);
    }

    /// A disconnect/rejection callback belongs to the exact claim generation
    /// it accepted, not merely to the peer identity. The same authenticated
    /// peer can reconnect and refresh ownership before the old callback runs;
    /// that stale callback must not withdraw the newer session's route.
    #[tokio::test]
    async fn stale_same_identity_release_cannot_clear_newer_claim_generation() {
        let (owner, publisher) = owner_handle();
        let node = peer("same-identity-reconnect");
        let target = addr(30_035);

        let old_claim = owner
            .claim(target, claim_of(node.clone(), ClaimKind::Verified), false)
            .await;
        let old_generation = old_claim.commit_seq().expect("old claim commits");
        let new_claim = owner
            .claim(target, claim_of(node.clone(), ClaimKind::Verified), false)
            .await;
        let new_generation = new_claim.commit_seq().expect("new claim commits");
        assert!(new_generation > old_generation);

        let events_before_stale_release = publisher.events();
        let stale_release = owner.release(target, node.clone(), old_generation).await;

        assert_eq!(
            stale_release, None,
            "a release for the old claim generation must be refused after the same peer reclaims"
        );
        assert_eq!(owner.routes_to(&target), Some(node));
        assert_eq!(
            publisher.events(),
            events_before_stale_release,
            "a stale release must publish no route retraction"
        );
    }

    /// P1 follow-on regression: elapsed time alone (whether measured from
    /// the last commit, or from when a caller-supplied generation snapshot
    /// was taken) is a LEASE, not a fence -- it can "become" valid purely by
    /// the release command sitting queued long enough (lock contention, or
    /// earlier peers in the same sweep each doing their own owner round
    /// trip first), even though a reconnect landed -- and was itself proven
    /// live -- while it waited. This reproduces exactly that: the failure
    /// evidence a sweep would act on is fixed BEFORE a reconnect, the
    /// reconnect then commits a fresh claim, and enough wall time passes
    /// that any elapsed-time check (measured from the RECONNECT's own
    /// genuinely fresh commit) would no longer protect it. The release must
    /// still be refused, because the reconnect's direct evidence is
    /// causally AFTER the fixed failure evidence -- a fact elapsed wall
    /// time can never undo.
    #[tokio::test]
    async fn release_dead_peer_is_fenced_against_evidence_causally_before_a_late_reconnect() {
        let (owner, _publisher) = owner_handle();
        let node = peer("late-reconnect-causal-fence");
        let target = addr(30_040);
        let old_session = addr(30_041);
        let new_session = addr(30_042);

        owner
            .claim_connection_scoped(
                target,
                claim_of(node.clone(), ClaimKind::Verified),
                old_session,
            )
            .await;
        // What a dead-peer sweep would have fixed as the failure evidence's
        // Instant-equivalent at selection time -- BEFORE the reconnect
        // below, e.g. because `old_session` had already gone quiet and
        // `gossip_state` looked dead at that exact moment.
        let evidence_before = std::time::Instant::now();

        // The reconnect: a fresh, genuinely live claim for the SAME
        // identity, committed strictly AFTER the fixed failure evidence but
        // well before its (delayed) release actually runs.
        owner
            .claim_connection_scoped(
                target,
                claim_of(node.clone(), ClaimKind::Verified),
                new_session,
            )
            .await;

        // Enough wall time now passes that any elapsed-time check, measured
        // from the reconnect's own commit, would no longer protect it. The
        // causal fence does not care: neither operand it compares moves as
        // more time passes.
        tokio::time::sleep(Duration::from_millis(40)).await;

        let released = owner
            .release_dead_peer(node.clone(), target, evidence_before)
            .await;

        assert_eq!(
            released,
            DeadPeerReleaseOutcome::ProvenAlive,
            "a dead-peer release must refuse when direct evidence of life is causally after \
             the failure evidence being acted on, regardless of how much wall-clock time has \
             since elapsed"
        );
        assert_eq!(
            owner.routes_to(&target),
            Some(node),
            "the reconnect's ownership must survive a stale sweep's delayed release"
        );
    }

    /// P1 finding (review round against `95907bc`, this file around line
    /// 2242): an EARLIER version of `note_liveness_evidence` wrote DIRECTLY
    /// into a lock-free side table (`Arc<scc::HashMap<SocketAddr,
    /// Instant>>`) that `release_dead_peer` merely CONSULTED via
    /// `read_sync`. Because that write came from OUTSIDE the owner's own
    /// serialization, nothing prevented it from landing between
    /// `release_dead_peer`'s read of the marker and the
    /// `addr_ownership.remove` a few lines below it -- retracting a peer
    /// that had, by then, already proven itself alive, with
    /// `retract_owner` deleting the fresh marker on the way out. A
    /// lock-free structure the owner only reads has the same check-then-act
    /// gap as every other mirror this PR found and closed --
    /// `claim_generation`, `get_required_peer_addr`, the pin token,
    /// `pinned_addr`, `pinned_addr_for`.
    ///
    /// Fixed by routing the WRITE, not just the read, through
    /// `OwnerCommand::NoteLivenessEvidence` on the owner's own serialized
    /// `handle()` stream (see that variant's doc comment, and
    /// `PeerRegistryOwner::note_liveness_evidence`). Both the marker's
    /// update and `release_dead_peer`'s check of it now run inside the
    /// SAME single-threaded task, so whichever of the two commands the
    /// owner dequeues first is fully committed before the other is even
    /// looked at -- there is no third possibility where one is "in
    /// flight" relative to the other.
    ///
    /// This asserts that guarantee directly, at the level the finding
    /// cited, rather than through the full `reap_reserved_candidates`
    /// path (see `reap_reserved_candidates_leaves_capabilities_receipts_
    /// and_ownership_untouched_when_gossip_proves_liveness` in
    /// `registry.rs` for that integration-level coverage): the marker is
    /// bumped via a fully-awaited `note_liveness_evidence` call BEFORE
    /// `release_dead_peer` is ever invoked, so FIFO order on the shared
    /// mailbox deterministically guarantees the marker is visible to the
    /// check -- no reliance on a real, unreproducible thread-level race.
    ///
    /// A genuinely adversarial reproduction of the ORIGINAL bug (the
    /// marker's write landing at the exact machine instruction between
    /// `release_dead_peer`'s read and its `addr_ownership.remove`) is not
    /// constructible here: that window existed only inside
    /// `release_dead_peer`'s own synchronous body, which contains no
    /// `.await` point on either side of the fix for a single-threaded
    /// test to hand-drive an interleaving into, and true OS-thread
    /// parallelism would make the reproduction genuinely racy -- the same
    /// "no yield point to hand-drive, real parallelism would only be
    /// racy" shape already accepted elsewhere on this PR for
    /// `connect_to_peer`'s TOCTOU and the owner's `run()` loop
    /// drain-priority bug. What IS directly provable, and is asserted
    /// here, is the fix's actual guarantee: FIFO submission order on the
    /// shared mailbox is what now determines the outcome, not wall-clock
    /// timing against an externally-mutable side table.
    #[tokio::test]
    async fn release_dead_peer_sees_liveness_evidence_committed_through_the_same_serialized_stream()
    {
        let (owner, _publisher) = owner_handle();
        let node = peer("liveness-marker-owner-serialized");
        let target = addr(30_045);
        let session = addr(30_046);

        owner
            .claim_connection_scoped(target, claim_of(node.clone(), ClaimKind::Verified), session)
            .await;

        // Exactly what a dead-peer sweep's own selection pass would have
        // fixed as the failure evidence's Instant-equivalent, BEFORE the
        // response below arrives.
        let evidence_before = std::time::Instant::now();

        // A real gap, not just program order: two `Instant::now()` calls
        // issued back-to-back with no intervening work can land on the
        // SAME clock tick on a coarse-resolution timer, which would make
        // the causal `>` comparison below spuriously false regardless of
        // the fix. See `migrate_never_ages_a_destination_with_newer_
        // direct_evidence_backwards` above for the same idiom.
        tokio::time::sleep(Duration::from_millis(20)).await;

        // THE WINDOW: ordinary liveness evidence from the SAME
        // already-claimed connection -- not a new claim, not an ownership
        // change -- exactly what `mark_response_received` records. Fully
        // awaited before `release_dead_peer` is even called, so it is
        // guaranteed to be enqueued on the owner's shared mailbox first,
        // and therefore processed first (single-threaded, FIFO).
        owner
            .note_liveness_evidence(target, std::time::Instant::now())
            .await;

        let released = owner
            .release_dead_peer(node.clone(), target, evidence_before)
            .await;

        assert_eq!(
            released,
            DeadPeerReleaseOutcome::ProvenAlive,
            "a dead-peer release must be refused once the owner's OWN serialized stream has \
             already committed direct liveness evidence causally after the failure being \
             acted on"
        );
        assert_eq!(
            owner.routes_to(&target),
            Some(node),
            "ownership must not be released for a peer now proven live through the owner's \
             own serialization -- a check-then-act gap here would have let this retract a \
             peer that had already proven itself alive"
        );
    }

    /// Address re-resolution moves ownership rather than stranding it.
    #[tokio::test]
    async fn migrate_moves_ownership_to_the_new_address() {
        let (owner, _publisher) = owner_handle();
        let node = peer("migrating");
        let from = addr(30_009);
        let to = addr(30_010);

        owner
            .claim(from, claim_of(node.clone(), ClaimKind::Verified), false)
            .await;
        owner
            .claim(from, claim_of(node.clone(), ClaimKind::Provisional), false)
            .await;
        let source = current_source(&owner, from);
        assert!(
            owner.migrate(from, to, source, false).await.moved(),
            "an owned source must move onto a free destination"
        );
        assert_eq!(owner.owner_of(&from), None);
        assert_eq!(
            owner.owner_of(&to),
            Some(Owner {
                node_id: node,
                kind: ClaimKind::Verified,
            })
        );

        // A migration onto an address a different identity owns is refused,
        // and is reported distinctly from "there was nothing to move".
        let contested = addr(30_011);
        owner
            .claim(
                contested,
                claim_of(peer("holder"), ClaimKind::Verified),
                false,
            )
            .await;
        assert_eq!(
            owner
                .migrate(to, contested, current_source(&owner, to), false)
                .await,
            MigrateOutcome::TargetOwnedByOther
        );
        // An UNOWNED source moving onto an address someone else owns is
        // blocked too: "nothing to move" would invite the caller to re-key
        // its own state onto the other identity's address.
        assert_eq!(
            owner
                .migrate(addr(30_012), contested, SourceExpectation::Unowned, false,)
                .await,
            MigrateOutcome::TargetOwnedByOther,
            "an unowned source must not be allowed onto another identity's address"
        );
        // With a free destination, an unclaimed source is still reported as
        // "nothing to move" — the non-blocking outcome.
        assert_eq!(
            owner
                .migrate(
                    addr(30_012),
                    addr(30_013),
                    SourceExpectation::Unowned,
                    false,
                )
                .await,
            MigrateOutcome::SourceUnowned,
            "an unclaimed source with a free destination has nothing to move"
        );
    }

    /// P1 regression: `migrate` moved ownership, minted a new generation at
    /// `to`, and carried freshness/pin state, but left
    /// `connection_scoped_claims` untouched -- a receipt still keyed to
    /// `from` after the move refers to an address that is no longer owned
    /// at all. Asserts the ghost-revival consequence directly (a later
    /// teardown at the migrated address CAN release it), not merely that
    /// the receipt ends up under the expected key, since an empty/absent
    /// entry is reachable for uninteresting reasons too.
    #[tokio::test]
    async fn migrate_moves_receipts_so_a_later_teardown_can_still_release() {
        let (owner, _publisher) = owner_handle();
        let node = peer("migrate-receipt-move");
        let from = addr(30_014);
        let to = addr(30_114);
        let session = addr(30_214);

        owner
            .claim_connection_scoped(from, claim_of(node.clone(), ClaimKind::Verified), session)
            .await;
        let source = current_source(&owner, from);

        assert!(
            owner.migrate(from, to, source, false).await.moved(),
            "migrate must succeed"
        );

        // The session's own, entirely legitimate teardown must find its
        // receipt has moved to `to` -- where the address it actually backs
        // now lives -- not remain stranded at the vacated `from`, and must
        // actually release `to`'s ownership itself, atomically, as part of
        // this same call (see `release_session`'s doc comment).
        let released = owner.release_session(node.clone(), session).await;
        assert_eq!(
            released
                .iter()
                .map(|(candidate_addr, _)| *candidate_addr)
                .collect::<Vec<_>>(),
            vec![to],
            "the session's receipt must have moved to `to` along with the ownership it backs, \
             and its own teardown must actually release it -- the ghost-revival consequence, \
             not merely that a key moved"
        );
        assert_eq!(
            owner.routes_to(&to),
            None,
            "the migrated address must actually be released, not merely reported as a \
             candidate"
        );
    }

    /// P1 regression: `claim` (the shared, plain-claim path used by
    /// gossip/discovery refreshes -- NOT only `claim_connection_scoped`)
    /// advances `claim_generation` for every accepted same-owner claim,
    /// including a plain refresh that never touches
    /// `claim_connection_scoped` at all. If it does not ALSO keep every
    /// still-live connection-scoped receipt for that same owner+address in
    /// sync with the new generation, an ordinary plain refresh landing
    /// between a connection's claim and its later, genuinely correct
    /// teardown leaves that session's receipt holding a stale generation:
    /// `release_session` finds and removes the receipt, hands its (now
    /// stale) generation to `release`, `release`'s CAS rejects it against
    /// the generation the plain claim advanced to, and because the receipt
    /// is already gone there is no retry -- the address's ownership is
    /// stranded permanently. Same ghost-revival shape, and same "assert the
    /// later teardown can actually release it" style, as the `migrate`
    /// regression above -- just triggered by a plain claim instead of a
    /// migration.
    #[tokio::test]
    async fn plain_claims_keep_live_receipts_in_sync_so_teardown_can_still_release() {
        let (owner, _publisher) = owner_handle();
        let node = peer("plain-claim-receipt-sync");
        let a = addr(30_050);
        let session = addr(30_051);

        owner
            .claim_connection_scoped(a, claim_of(node.clone(), ClaimKind::Verified), session)
            .await;

        // An ordinary, INDIRECT (plain, non-connection-scoped) refresh for
        // the SAME identity/address -- e.g. gossip re-announcing a peer's
        // own address -- advances the generation without ever going
        // through `claim_connection_scoped`.
        let refreshed = owner
            .claim(a, claim_of(node.clone(), ClaimKind::Verified), false)
            .await;
        assert!(
            refreshed.is_accepted(),
            "the plain refresh must be accepted"
        );

        // The connection's later, entirely legitimate teardown must still
        // find a receipt for its address, and must actually release that
        // address's ownership itself, atomically, as part of this same
        // call -- not merely report a candidate whose generation a
        // separately-ordered command could find stale by the time it runs.
        let released = owner.release_session(node.clone(), session).await;
        assert_eq!(
            released
                .iter()
                .map(|(candidate_addr, _)| *candidate_addr)
                .collect::<Vec<_>>(),
            vec![a],
            "the session's own receipt must still be found for its address, and its own \
             teardown must actually release it -- the ghost-revival consequence, not merely \
             that a receipt exists"
        );
        assert_eq!(
            owner.routes_to(&a),
            None,
            "the address must actually be released, not merely reported as a candidate"
        );
    }

    /// P1 regression: `release_session` used to return release CANDIDATES --
    /// an address plus the receipt's now-removed generation -- for the
    /// caller to pass to a SEPARATE, later `release` command. A plain
    /// same-identity claim landing between those two commands advances
    /// `claim_generation` with no receipt left to update (this session's
    /// own receipt was already removed by the first command), so the
    /// follow-up `release` rejected its now-stale generation. Because the
    /// receipt is already gone, there is no retry: an unpinned address with
    /// no receipt left that could ever release it is stranded permanently
    /// -- the exact failure this PR exists to fix, reached through the
    /// ordinary session-teardown path instead of the dead-peer sweep.
    ///
    /// `release_session` now performs the ownership retraction itself, in
    /// the SAME synchronous owner command as the receipt removal (see its
    /// doc comment), so there is no window between them for anything to
    /// land in. Proves it by racing a plain, same-identity claim for the
    /// SAME address directly against the session's teardown -- both
    /// submitted concurrently, so the owner may serialize either one
    /// first -- and asserting the address ends up correctly released
    /// regardless of which order the owner actually chose.
    #[tokio::test]
    async fn release_session_atomically_retracts_ownership_so_a_racing_plain_claim_cannot_strand_it()
     {
        let (owner, _publisher) = owner_handle();
        let node = peer("release-session-atomic-retract");
        let a = addr(30_055);
        let session = addr(30_056);

        let commit = owner
            .claim_connection_scoped(a, claim_of(node.clone(), ClaimKind::Verified), session)
            .await;
        assert!(commit.is_accepted());

        let owner_for_claim = owner.clone();
        let node_for_claim = node.clone();
        let claim_task = tokio::spawn(async move {
            owner_for_claim
                .claim(a, claim_of(node_for_claim, ClaimKind::Verified), false)
                .await
        });
        let released = owner.release_session(node.clone(), session).await;
        let claim_result = claim_task.await.expect("claim task panicked");
        assert!(
            claim_result.is_accepted(),
            "the racing plain claim must still be accepted regardless of ordering"
        );

        // Whichever order the owner actually serialized these in: if the
        // plain claim landed first, the SAME check as
        // `plain_claims_keep_live_receipts_in_sync_...` applies (the
        // receipt is kept in sync, so `release_session` still finds and
        // releases it); if `release_session` landed first, it already
        // atomically released the address before the plain claim re-claims
        // it as a fresh, later ownership epoch. Either way, THIS session's
        // own teardown must be reported as having found and released its
        // receipt -- never silently stranded by the race.
        assert_eq!(
            released.iter().map(|(addr, _)| *addr).collect::<Vec<_>>(),
            vec![a],
            "the session's teardown must have found and released its own receipt regardless \
             of ordering against the concurrent plain claim"
        );
    }

    /// P2 regression: a DNS-triggered `migrate` that carries an operator pin
    /// from `from` to `to` must publish `to` as the peer's `ConnectionPool`
    /// configured/required route in the SAME command -- not leave that
    /// publish to some later, separately-ordered step. Otherwise the owner
    /// protects `to` while `ConnectionPool::get_required_peer_addr` keeps
    /// reporting the stale `from`, reintroducing through `migrate` exactly
    /// the pin/route divergence `configure_peer` was unified to prevent.
    #[tokio::test]
    async fn migrate_moves_the_configured_route_along_with_a_carried_pin() {
        let (owner, publisher) = owner_handle();
        let node = peer("migrate-carries-pin");
        let from = addr(30_014);
        let to = addr(30_015);

        owner
            .claim(from, claim_of(node.clone(), ClaimKind::Verified), false)
            .await;
        let evicted = owner.pin(from, node.clone()).await;
        assert_eq!(
            evicted, None,
            "sanity: first pin for this peer evicts nothing"
        );

        let source = current_source(&owner, from);
        assert!(
            owner.migrate(from, to, source, false).await.moved(),
            "a pinned source must still migrate onto a free destination"
        );

        assert_eq!(
            publisher.configured_routes().last(),
            Some(&(to, node.clone(), Some(from))),
            "migrate must publish the carried pin's new address as the \
             ConnectionPool configured/required route, in the same command \
             the pin itself moves in -- and must name `from` as the evicted \
             address, so the SAME call also evicts its now-stale \
             connections_by_addr alias (see RoutingPublisher::\
             set_configured_peer_addr's own doc comment, P1 review round \
             against ba2bff2)"
        );

        // The pin itself must have moved: `to` now refuses release, `from`
        // (no longer owned at all) trivially does not need it protected.
        let token = owner.ownership_token(&to).expect("still owned at `to`");
        assert!(
            owner.release(to, node, token.generation()).await.is_none(),
            "the migrated pin must still protect `to` from release"
        );
    }

    /// P2 follow-on: `migrate` permits `to` to already be owned by the SAME
    /// identity (the merge case). If `to` already has its OWN, strictly
    /// newer direct-evidence timestamp than `from`'s, carrying `from`'s
    /// (older) timestamp over unconditionally would age `to` BACKWARDS --
    /// making a genuinely live address look reapable. That inverts the
    /// property the freshness fence exists for, the same "measuring the
    /// wrong event" shape as an indirect claim making an address look MORE
    /// fresh than it actually is. The migration must take the newer of the
    /// two timestamps instead.
    #[tokio::test]
    async fn migrate_never_ages_a_destination_with_newer_direct_evidence_backwards() {
        let (owner, _publisher) = owner_handle();
        let node = peer("migrate-preserves-newer-destination-evidence");
        let from = addr(30_016);
        let to = addr(30_017);
        let from_session = addr(31_016);
        let to_session = addr(31_017);

        // `from`'s direct (connection-scoped) claim happens first, and will
        // be comparatively stale by the time freshness is checked below.
        owner
            .claim_connection_scoped(
                from,
                claim_of(node.clone(), ClaimKind::Verified),
                from_session,
            )
            .await;

        tokio::time::sleep(Duration::from_millis(80)).await;

        // Fixed strictly AFTER `from`'s claim (now 80ms old) but strictly
        // BEFORE `to`'s claim below -- if `to`'s freshness incorrectly ended
        // up reflecting `from`'s older evidence instead of its own, this is
        // exactly the point in time that would fail to distinguish them.
        let evidence_before = std::time::Instant::now();

        // `to` already has its OWN, strictly newer direct claim -- e.g. the
        // same peer independently (re)connected there too, before this
        // migration ever ran.
        owner
            .claim_connection_scoped(to, claim_of(node.clone(), ClaimKind::Verified), to_session)
            .await;

        let source = current_source(&owner, from);
        assert!(
            owner.migrate(from, to, source, false).await.moved(),
            "a same-identity merge onto an already-owned destination must still succeed"
        );

        // `to`'s freshness must reflect ITS OWN newer evidence, not
        // `from`'s much older one: `to`'s real claim happened AFTER
        // `evidence_before`, so a causal fence checked against it must
        // still refuse to release `to`.
        assert_eq!(
            owner.release_dead_peer(node, to, evidence_before).await,
            DeadPeerReleaseOutcome::ProvenAlive,
            "migrate must not age a destination with newer direct evidence backwards to \
             the source's older timestamp -- a genuinely live address must not become \
             reapable"
        );
    }

    /// The destination is inspected before the source: a migration off an
    /// address that was never claimed must still be blocked when the
    /// destination belongs to a different identity.
    #[tokio::test]
    async fn migrate_blocks_an_unowned_source_from_a_foreign_destination() {
        let (owner, publisher) = owner_handle();
        let holder = peer("destination-holder");
        let unowned = addr(30_020);
        let held = addr(30_021);

        owner
            .claim(held, claim_of(holder.clone(), ClaimKind::Verified), false)
            .await;
        let events_before = publisher.events();

        assert_eq!(
            owner
                .migrate(unowned, held, SourceExpectation::Unowned, false)
                .await,
            MigrateOutcome::TargetOwnedByOther
        );
        assert_eq!(
            owner.routes_to(&held),
            Some(holder),
            "the destination's owner must keep its routing"
        );
        assert_eq!(
            publisher.events(),
            events_before,
            "a blocked migration must publish nothing"
        );
    }

    /// Local-address rejection lives inside the serialized owner command,
    /// not only in DNS caller prechecks, so no derived state can move first.
    #[tokio::test]
    async fn migrate_refuses_a_local_destination_without_mutation() {
        let (owner, publisher) = owner_handle();
        let node = peer("local-migration");
        let from = addr(30_039);
        let local = addr(30_040);

        owner
            .claim(from, claim_of(node.clone(), ClaimKind::Verified), false)
            .await;
        let events_before = publisher.events();

        assert_eq!(
            owner
                .migrate(from, local, current_source(&owner, from), true)
                .await,
            MigrateOutcome::TargetIsLocal
        );
        assert_eq!(owner.routes_to(&from), Some(node));
        assert_eq!(owner.routes_to(&local), None);
        assert_eq!(publisher.events(), events_before);
    }

    /// P1 follow-up regression: `migrate` mutates `addr_ownership` and
    /// `claim_committed_at` for BOTH addresses directly -- it does not go
    /// through `claim`, so it used to be the one owner command that could
    /// reach those tables without ever consulting `reap_reserved`. A
    /// `cleanup_dead_peers` sweep relies on nothing being able to move
    /// ownership onto or off of a reserved address for the duration of its
    /// non-owner destructive work; a migration was the one door left
    /// unlocked. Proves both ends: a reservation held on the SOURCE refuses
    /// the move (ownership must not be moved away out from under a sweep
    /// about to release or has already started destroying that peer's
    /// state), and a reservation held on the DESTINATION refuses it too
    /// (fresh ownership must not be installed on an address a DIFFERENT
    /// sweep is relying on staying exactly as it observed it) -- in both
    /// cases with `MigrateOutcome::ReapInProgress` specifically, not some
    /// other refusal, and with ownership at both addresses completely
    /// unchanged.
    #[tokio::test]
    async fn migrate_is_refused_while_either_end_holds_a_reap_reservation() {
        let (owner, _publisher) = owner_handle();

        // Source reserved: the sweep already committed to reaping `from`.
        {
            let node = peer("migrate-reap-source");
            let from = addr(30_205);
            let to = addr(30_206);
            owner
                .claim(from, claim_of(node.clone(), ClaimKind::Verified), false)
                .await;
            let source = current_source(&owner, from);

            let reservation = owner
                .reserve_for_reap(
                    from,
                    std::time::Instant::now(),
                    owner.ownership_token(&from),
                    None,
                    Some(node.clone()),
                )
                .await
                .expect("the exact identity just claimed above must still be reservable");

            let outcome = owner.migrate(from, to, source, false).await;
            assert_eq!(
                outcome,
                MigrateOutcome::ReapInProgress,
                "a migration whose SOURCE is reap-reserved must be refused, specifically for \
                 that reason"
            );
            assert_eq!(
                owner.routes_to(&from),
                Some(node),
                "ownership must not have moved off a reap-reserved source"
            );
            assert_eq!(owner.routes_to(&to), None);

            reservation.release().await;
        }

        // Destination reserved: a DIFFERENT sweep is relying on `to` staying
        // exactly as it observed it.
        {
            let node = peer("migrate-reap-dest");
            let from = addr(30_207);
            let to = addr(30_208);
            owner
                .claim(from, claim_of(node.clone(), ClaimKind::Verified), false)
                .await;
            let source = current_source(&owner, from);

            let reservation = owner
                .reserve_for_reap(to, std::time::Instant::now(), None, None, None)
                .await
                .expect("an unclaimed destination must be reservable");

            let outcome = owner.migrate(from, to, source, false).await;
            assert_eq!(
                outcome,
                MigrateOutcome::ReapInProgress,
                "a migration whose DESTINATION is reap-reserved must be refused, specifically \
                 for that reason"
            );
            assert_eq!(
                owner.routes_to(&from),
                Some(node),
                "the source's ownership must be untouched when the destination refuses the move"
            );
            assert_eq!(owner.routes_to(&to), None);

            reservation.release().await;
        }
    }

    /// P1 follow-up regression: `reserve_for_reap` used to return a plain
    /// `bool`. Nothing enforced that a caller holding `true` ever called the
    /// matching release, and nothing ran on its behalf if the caller's task
    /// ended before reaching it -- in particular, a hard
    /// `JoinHandle::abort()` of the task holding the reservation (NOT
    /// `select!` cancellation, which `cleanup_dead_peers` is already safe
    /// against -- see its own doc comment) used to drop the reservation's
    /// `bool` on the floor with no side effect at all, leaving
    /// `reap_reserved` holding the address forever: every future claim for
    /// it refused permanently, a worse outcome than the race the
    /// reservation exists to prevent. `GossipRegistryHandle::shutdown` and
    /// `shutdown_and_wait` both abort the exact task that runs
    /// `cleanup_dead_peers` in production, so this is not a hypothetical
    /// path.
    ///
    /// Proves the RAII guard closes it: a task is spawned holding a granted
    /// `ReapReservation`, never explicitly released, and parked so it is
    /// definitely suspended -- not merely about to complete -- when
    /// aborted. Aborting it drops the task's future, including the guard,
    /// in place. Because the guard's `Drop` impl submits the release
    /// through the SAME owner mailbox via a synchronous `try_send` rather
    /// than doing nothing, the address is claimable again immediately
    /// afterward -- proven directly, by actually submitting a claim and
    /// checking it is accepted, not merely inferred from the absence of a
    /// panic.
    #[tokio::test]
    async fn an_aborted_task_still_releases_its_reap_reservation() {
        let (owner, _publisher) = owner_handle();
        let reserved_addr = addr(30_300);
        let session_source = addr(30_301);
        let node = peer("aborted-reap-reservation");

        let owner_for_task = owner.clone();
        let task = tokio::spawn(async move {
            let _reservation = owner_for_task
                .reserve_for_reap(reserved_addr, std::time::Instant::now(), None, None, None)
                .await
                .expect("an unclaimed address must be reservable");
            // Never released -- parked here to simulate a sweep suspended
            // mid-flight (e.g. awaiting `gossip_state`'s lock for its own
            // destructive work) at the exact moment its task is hard-
            // aborted.
            std::future::pending::<()>().await;
        });

        // Give the spawned task a chance to actually reach and pass the
        // reservation's own `.await` before aborting it -- aborting before
        // it has even run would prove nothing about the guard's `Drop`
        // impl, since there would be no guard alive yet to drop.
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }

        task.abort();
        let result = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the aborted task must actually finish, not hang");
        assert!(
            result.unwrap_err().is_cancelled(),
            "the task must have been cancelled by the abort, not have panicked or completed \
             normally -- otherwise this test does not exercise the abort path at all"
        );

        // The proof: a claim for the SAME address, submitted after the
        // abort, must succeed. A leaked reservation would refuse it with
        // `ClaimRejection::ReapInProgress` forever.
        let commit = owner
            .claim_connection_scoped(
                reserved_addr,
                claim_of(node, ClaimKind::Verified),
                session_source,
            )
            .await;
        assert!(
            commit.is_accepted(),
            "a reservation left by an aborted task must not survive it -- got {commit:?}"
        );
    }

    /// P1 regression: `reserve_for_reap`'s owner-internal handler discarded
    /// `HashSet::insert`'s return value and unconditionally reported the
    /// reservation granted once the causal fence passed. Two concurrent
    /// `cleanup_dead_peers` sweeps racing to reserve the SAME address
    /// therefore both received a guard backed by ONE shared set entry:
    /// whichever released first removed the entry while the OTHER sweep's
    /// destructive actor/tombstone work was still relying on it staying
    /// held, reopening the exact race the reservation exists to prevent --
    /// reachable through the reservation mechanism itself, not around it.
    ///
    /// Proves reservations are exclusive: two reservation requests for the
    /// same address, submitted genuinely concurrently (`tokio::spawn`, not
    /// sequenced by the test -- the owner's own internal serialization is
    /// what decides which one actually lands first, and this test does not
    /// care which), must produce exactly one grant, never two.
    #[tokio::test]
    async fn concurrent_reap_reservations_for_the_same_address_are_mutually_exclusive() {
        let (owner, _publisher) = owner_handle();
        let contested_addr = addr(30_400);

        let owner_a = owner.clone();
        let owner_b = owner.clone();
        let task_a = tokio::spawn(async move {
            owner_a
                .reserve_for_reap(contested_addr, std::time::Instant::now(), None, None, None)
                .await
        });
        let task_b = tokio::spawn(async move {
            owner_b
                .reserve_for_reap(contested_addr, std::time::Instant::now(), None, None, None)
                .await
        });

        let reservation_a = task_a.await.expect("task A must not panic");
        let reservation_b = task_b.await.expect("task B must not panic");

        let granted = [reservation_a.is_some(), reservation_b.is_some()]
            .into_iter()
            .filter(|granted| *granted)
            .count();
        assert_eq!(
            granted, 1,
            "exactly one of two concurrent reservation requests for the same address must be \
             granted -- two granted means two guards share one set entry (unsafe: releasing \
             either one drops protection the other is still relying on), zero granted means a \
             legitimate reservation was wrongly refused"
        );

        // Release whichever guard actually won, so this test does not
        // itself leak a reservation.
        if let Some(reservation) = reservation_a {
            reservation.release().await;
        }
        if let Some(reservation) = reservation_b {
            reservation.release().await;
        }
    }

    /// P1 finding (review round against `c48546d`, `registry.rs:9011`):
    /// direct, whitebox proof of `try_consume`'s one-shot semantics --
    /// see `ReapReservation`'s own doc comment for the full reasoning
    /// (why a compare-and-swap closes the check-then-act gap a plain
    /// `is_still_valid()` load, repeated however many times, cannot).
    /// The FIRST call against a fresh, still-valid reservation must
    /// succeed; every call after that, against the SAME reservation, must
    /// fail -- proving this is a genuine one-shot claim, not a repeatable
    /// read that merely happens to answer `true` the first time.
    #[tokio::test]
    async fn try_consume_succeeds_exactly_once_then_fails_forever() {
        let (owner, _publisher) = owner_handle();
        let reserved_addr = addr(30_401);

        let reservation = owner
            .reserve_for_reap(reserved_addr, std::time::Instant::now(), None, None, None)
            .await
            .expect("a freshly, unconflictedly claimed address must be reservable");

        assert!(
            reservation.is_still_valid(),
            "sanity: a freshly granted reservation must read valid before anything consumes or \
             invalidates it"
        );
        assert!(
            reservation.try_consume(),
            "the first try_consume against a valid, unconsumed reservation must succeed"
        );
        assert!(
            !reservation.try_consume(),
            "a SECOND try_consume against the SAME, already-consumed reservation must fail -- \
             otherwise this is a repeatable read, not a one-shot claim, and two callers could \
             both believe they alone were authorized to proceed"
        );
        assert!(
            !reservation.is_still_valid(),
            "is_still_valid must read false after a successful consume -- the underlying flag \
             genuinely flipped, not merely reported success without recording it"
        );

        reservation.release().await;
    }

    /// Companion to the sequential proof above: under GENUINE concurrent
    /// contention (many tasks racing `try_consume` against ONE shared
    /// reservation, not sequenced by the test), still exactly one winner.
    /// This is what actually backs the claim in `ReapReservation`'s own
    /// doc comment that a `try_consume` CAS needs no owner round trip and
    /// no lock to be race-free: Rust's atomics model already guarantees a
    /// single, total modification order for every operation on one atomic
    /// object, regardless of which task or thread performs it.
    #[tokio::test]
    async fn try_consume_is_exclusive_under_genuinely_concurrent_attempts() {
        let (owner, _publisher) = owner_handle();
        let reserved_addr = addr(30_402);

        let reservation = std::sync::Arc::new(
            owner
                .reserve_for_reap(reserved_addr, std::time::Instant::now(), None, None, None)
                .await
                .expect("a freshly, unconflictedly claimed address must be reservable"),
        );

        let mut tasks = Vec::new();
        for _ in 0..16 {
            let reservation = reservation.clone();
            tasks.push(tokio::spawn(async move { reservation.try_consume() }));
        }

        let mut successes = 0usize;
        for task in tasks {
            if task.await.expect("try_consume task must not panic") {
                successes += 1;
            }
        }

        assert_eq!(
            successes, 1,
            "exactly one of many genuinely concurrent try_consume attempts against the SAME \
             reservation must succeed -- more than one means the CAS is not actually exclusive, \
             zero means try_consume can wrongly refuse a reservation nothing ever invalidated"
        );

        assert!(
            !reservation.is_still_valid(),
            "the reservation must read invalid after being consumed by whichever task won"
        );

        let reservation = std::sync::Arc::into_inner(reservation)
            .expect("no task should retain its clone past awaiting its own JoinHandle");
        reservation.release().await;
    }

    /// P1 regression: `ReapReservation::release` used to disarm
    /// (`released = true`) BEFORE awaiting its send on the bounded `tx`
    /// mailbox. A task aborted while that send was suspended waiting for
    /// mailbox capacity dropped the future with `released` already `true`
    /// -- `Drop` saw that and did nothing, and even the best-effort
    /// fallback of that era could itself fail on the very same full
    /// mailbox. The exact leak the RAII guard exists to prevent,
    /// reappearing through the guard's own failure path. The governing
    /// asymmetry: failing to TAKE a reservation is safe (the sweep just
    /// skips the candidate), but failing to RELEASE one is not (every
    /// later claim for that address is refused forever) -- so release must
    /// be reliably enqueueable even when the ordinary mailbox is not.
    ///
    /// Proves the fix holds under a genuinely, provably saturated bounded
    /// mailbox: grants a reservation, then fills `tx` to capacity via a
    /// tight, SYNCHRONOUS `try_send` loop -- no `.await` anywhere between
    /// granting the reservation and dropping its guard below, so the owner
    /// task (a separate tokio task that can only run when this one yields)
    /// gets no opportunity to drain any of the backlog first; `tx` is
    /// confirmed still full, with a direct failing `try_send`, immediately
    /// before the drop. The guard is then dropped WITHOUT calling
    /// `release()` -- exactly the code path a hard task abort's cleanup
    /// also runs (`Drop::drop`, no `.await` available either way; a plain
    /// drop and an abort's cleanup are indistinguishable from the guard's
    /// own perspective, and `an_aborted_task_still_releases_its_reap_
    /// reservation` above already covers the task-abort framing directly
    /// -- this test isolates the mailbox-saturation half of the bug on its
    /// own, deterministically, which a spawn+abort would not guarantee:
    /// the runtime could drain the backlog during the `.await` an abort
    /// join requires, before the guard's `Drop` ever ran). A later claim
    /// for the same address must succeed regardless.
    #[tokio::test(flavor = "current_thread")]
    async fn reap_reservation_release_survives_a_saturated_bounded_mailbox() {
        let (owner, _publisher) = owner_handle();
        let target_addr = addr(30_410);
        let node = peer("saturated-mailbox-reap-release");
        let session_source = addr(30_411);

        let reservation = owner
            .reserve_for_reap(target_addr, std::time::Instant::now(), None, None, None)
            .await
            .expect("an unclaimed address must be reservable");

        // Saturate the bounded mailbox, synchronously, with unrelated
        // filler commands -- no `.await` anywhere in this loop.
        loop {
            let (reply, _response) = oneshot::channel();
            let command = OwnerCommand::Claim {
                addr: addr(31_000),
                claim: claim_of(peer("mailbox-filler"), ClaimKind::Provisional),
                is_local_addr: false,
                reply,
            };
            if owner.shared.tx.try_send(command).is_err() {
                break;
            }
        }

        // Confirmed full, still with no intervening `.await` since the
        // reservation was granted above.
        let (probe_reply, _probe_response) = oneshot::channel();
        assert!(
            owner
                .shared
                .tx
                .try_send(OwnerCommand::Claim {
                    addr: addr(31_001),
                    claim: claim_of(peer("mailbox-probe"), ClaimKind::Provisional),
                    is_local_addr: false,
                    reply: probe_reply,
                })
                .is_err(),
            "the bounded mailbox must be genuinely full immediately before the guard is dropped"
        );

        // Drop without releasing -- see this test's doc comment.
        drop(reservation);

        // The mailbox backlog only starts draining once this test awaits
        // something again, below. The proof: a claim for the same address
        // must succeed regardless -- possible only if the guard's release
        // never depended on `tx`'s capacity at all.
        let commit = owner
            .claim_connection_scoped(
                target_addr,
                claim_of(node, ClaimKind::Verified),
                session_source,
            )
            .await;
        assert!(
            commit.is_accepted(),
            "a reservation dropped while the bounded mailbox is saturated must not survive -- \
             got {commit:?}"
        );
    }

    /// Shutdown: a claim submitted when the owner is gone fails closed with a
    /// rejection instead of hanging or panicking.
    #[tokio::test]
    async fn claim_fails_closed_when_the_owner_is_gone() {
        let (owner, publisher) = owner_handle();
        owner.simulate_owner_gone();

        let commit = tokio::time::timeout(
            Duration::from_secs(5),
            owner.claim(
                addr(30_012),
                claim_of(peer("orphan"), ClaimKind::Verified),
                false,
            ),
        )
        .await
        .expect("a claim against a dead owner must not hang");

        assert_eq!(
            commit,
            ClaimCommit::Rejected(ClaimRejection::OwnerUnavailable)
        );
        assert!(publisher.events().is_empty());
        assert!(owner.snapshot().is_empty());
    }

    /// A caller that resolved the moving identity separately, then had it
    /// displaced before the command was processed, moves nothing: naming the
    /// expected owner makes the move conditional on that resolution, so the
    /// displacing identity is not silently carried onto the destination.
    #[tokio::test]
    async fn migrate_is_refused_when_the_source_owner_is_not_the_expected_identity() {
        let (owner, publisher) = owner_handle();
        let expected = peer("mismatch-expected");
        let usurper = peer("mismatch-usurper");
        let from = addr(30_030);
        let to = addr(30_031);

        // The caller's resolution: `expected` owns `from` at one exact
        // generation. Model that session ending before a verified successor
        // claims the address; a stale migration receipt must not carry the
        // successor.
        owner
            .claim(from, claim_of(expected.clone(), ClaimKind::Verified), false)
            .await;
        let expected_source = current_source(&owner, from);
        owner
            .release(
                from,
                expected.clone(),
                match &expected_source {
                    SourceExpectation::Owned(token) => token.generation(),
                    SourceExpectation::Unowned => unreachable!("verified claim owns source"),
                },
            )
            .await
            .expect("expected owner releases its generation");
        // ... and is replaced before the migrate command is processed.
        assert!(
            owner
                .claim(from, claim_of(usurper.clone(), ClaimKind::Verified), false)
                .await
                .is_accepted()
        );
        let events_before = publisher.events();

        assert_eq!(
            owner
                .migrate(from, to, expected_source.clone(), false)
                .await,
            MigrateOutcome::SourceOwnerMismatch
        );
        assert!(
            owner
                .migrate(from, to, expected_source.clone(), false)
                .await
                .is_blocked(),
            "a refused move must tell the caller to perform no address-keyed mutation"
        );
        assert_eq!(
            owner.routes_to(&from),
            Some(usurper),
            "the displacing identity keeps the source address"
        );
        assert_eq!(
            owner.owner_of(&to),
            None,
            "nothing may be carried onto the destination"
        );
        assert_eq!(
            publisher.events(),
            events_before,
            "a refused migration must publish nothing"
        );

        // A source that has been released entirely is a mismatch too: the
        // identity the caller resolved is no longer there to carry across.
        let vacant = addr(30_032);
        assert_eq!(
            owner.migrate(vacant, to, expected_source, false).await,
            MigrateOutcome::SourceOwnerMismatch
        );
        assert!(owner.owner_of(&to).is_none());
    }

    /// The restore leg of a failed re-resolution is scoped the same way: if a
    /// newer claimant took the address the ownership was moved to, putting it
    /// back would drag THAT identity onto the old address. The restore is
    /// refused and the newer claimant is left undisturbed.
    #[tokio::test]
    async fn migrate_rollback_is_refused_when_a_newer_claimant_took_the_address() {
        let (owner, _publisher) = owner_handle();
        let original = peer("rollback-original");
        let newer = peer("rollback-newer");
        let old_addr = addr(30_033);
        let new_addr = addr(30_034);

        // `original`'s ownership has already been moved onto `new_addr`.
        owner
            .claim(
                old_addr,
                claim_of(original.clone(), ClaimKind::Verified),
                false,
            )
            .await;
        let source = current_source(&owner, old_addr);
        let migration = owner.migrate(old_addr, new_addr, source, false).await;
        let migrated_generation = migration
            .commit_seq()
            .expect("original ownership migrates to the new address");

        // A newer claimant takes `new_addr` before the restore runs. Verified
        // ownership cannot be displaced directly, so model the old session's
        // release followed by the new session's verified claim.
        owner
            .release(new_addr, original.clone(), migrated_generation)
            .await
            .expect("original owner releases the migrated address");
        assert!(
            owner
                .claim(
                    new_addr,
                    claim_of(newer.clone(), ClaimKind::Verified),
                    false
                )
                .await
                .is_accepted()
        );

        assert_eq!(
            owner
                .migrate(
                    new_addr,
                    old_addr,
                    SourceExpectation::Owned(OwnershipToken::new(
                        original.clone(),
                        migrated_generation,
                    )),
                    false,
                )
                .await,
            MigrateOutcome::SourceOwnerMismatch,
            "the restore must not move an identity the caller never held"
        );
        assert_eq!(
            owner.routes_to(&new_addr),
            Some(newer),
            "the newer claimant's ownership must be undisturbed"
        );
        assert_eq!(
            owner.owner_of(&old_addr),
            None,
            "and nothing may be force-restored onto the old address"
        );

        // The same restore DOES succeed while the original identity still
        // holds the address — the pin refuses only the unsafe case.
        let other_old = addr(30_035);
        let other_new = addr(30_036);
        owner
            .claim(
                other_old,
                claim_of(original.clone(), ClaimKind::Verified),
                false,
            )
            .await;
        let other_source = current_source(&owner, other_old);
        let other_generation = owner
            .migrate(other_old, other_new, other_source, false)
            .await
            .commit_seq()
            .expect("undisturbed ownership moves");
        assert!(
            owner
                .migrate(
                    other_new,
                    other_old,
                    SourceExpectation::Owned(OwnershipToken::new(
                        original.clone(),
                        other_generation,
                    )),
                    false,
                )
                .await
                .moved(),
            "an undisturbed address must still be restorable"
        );
        assert_eq!(owner.routes_to(&other_old), Some(original));
    }

    /// Merging onto an address the same identity already holds keeps the
    /// stronger kind. A destination already backed by an observed connection
    /// must not be turned back into a self-reported one by a move, which
    /// would make it displaceable again.
    #[tokio::test]
    async fn same_owner_merge_does_not_downgrade_a_verified_destination() {
        let (owner, _publisher) = owner_handle();
        let node = peer("merge-node");
        let challenger = peer("merge-challenger");
        let from = addr(30_037);
        let to = addr(30_038);

        owner
            .claim(to, claim_of(node.clone(), ClaimKind::Verified), false)
            .await;
        owner
            .claim(from, claim_of(node.clone(), ClaimKind::Verified), false)
            .await;
        owner
            .claim(from, claim_of(node.clone(), ClaimKind::Provisional), false)
            .await;

        assert!(
            owner
                .migrate(from, to, current_source(&owner, from), false)
                .await
                .moved()
        );
        assert_eq!(
            owner.owner_of(&to),
            Some(Owner {
                node_id: node.clone(),
                kind: ClaimKind::Verified,
            }),
            "a same-identity merge must keep the destination's verified kind"
        );
        assert_eq!(owner.owner_of(&from), None);

        // The consequence that matters: the destination is still able to
        // refuse a competing verified claim, which a downgrade would have
        // let through.
        assert_eq!(
            owner
                .claim(to, claim_of(challenger, ClaimKind::Verified), false)
                .await,
            ClaimCommit::Rejected(ClaimRejection::Arbitration(
                RejectReason::VerifiedOwnerPresent
            ))
        );
        assert_eq!(owner.routes_to(&to), Some(node));
    }

    /// P1 regression: the OLD `configure_peer` submitted its claim and its
    /// pin as two separately-ordered owner commands, so the eviction `pin`
    /// reports and the release of that evicted address's ownership were
    /// necessarily two separate steps too -- a window in which a concurrent
    /// `migrate` could move the still-owned evicted address elsewhere
    /// before a caller's own follow-up `release` ever ran. The atomic
    /// `configure_peer` transaction closes this by releasing the evicted
    /// address's ownership in the SAME synchronous step as the eviction
    /// itself: by the time the second `configure_peer` call returns, the
    /// first address is no longer merely unpinned but already fully
    /// unowned, with no separate caller action required or possible to race.
    #[tokio::test]
    async fn configure_peer_atomically_releases_the_evicted_pins_ownership() {
        let (owner, _publisher) = owner_handle();
        let node = peer("atomic-configure-peer-eviction");
        let addr_p = addr(30_060);
        let addr_y = addr(30_061);

        let first = owner.configure_peer(addr_p, node.clone(), None).await;
        assert!(first.claim().is_accepted());
        assert_eq!(first.evicted_pin(), None);
        assert_eq!(first.evicted_release_seq(), None);

        let second = owner.configure_peer(addr_y, node.clone(), None).await;
        assert!(second.claim().is_accepted());
        assert_eq!(second.evicted_pin(), Some(addr_p));
        assert!(
            second.evicted_release_seq().is_some(),
            "evicting a pin this peer still owns must release its ownership in the same step"
        );

        assert_eq!(
            owner.ownership_token(&addr_p),
            None,
            "the evicted pin's ownership must already be gone -- released atomically with \
             the eviction, not left dangling for a later, separately-ordered release"
        );
        assert_eq!(owner.routes_to(&addr_p), None);
        assert_eq!(owner.routes_to(&addr_y), Some(node));
    }

    /// P1 finding (review round against `ded8495`, `registry.rs:4982`):
    /// `configure_peer`'s queued retry used to validate `expected_
    /// generation` on the CALLER's own side, immediately before submitting
    /// its owner command -- never atomic with the command itself, no
    /// matter how close together the two were. "On a multi-threaded
    /// runtime, retry A can pass this check, then a later call B can bump
    /// the generation and commit its own configuration first, after which
    /// A's stale command commits last and evicts B."
    ///
    /// Proves the fix requires NO racing at all, deterministically: a
    /// stale `expected_generation` is rejected purely because
    /// `PeerRegistryOwner::configure_peer` validates it INSIDE the same
    /// atomic transaction that would install the pin, not because of any
    /// timing this test would need to construct. Submits a stale retry
    /// (presenting generation 1, the FIRST call's own value) strictly
    /// AFTER a second, genuinely newer call has already committed
    /// generation 2 and moved the pin to `addr_y` -- the exact ordering
    /// the finding describes ("commits last") -- and asserts it is
    /// rejected outright, with `addr_y`'s pin left completely untouched.
    #[tokio::test]
    async fn configure_peer_rejects_a_stale_expected_generation_even_with_no_race_at_all() {
        let (owner, _publisher) = owner_handle();
        let node = peer("configure-peer-stale-generation");
        let addr_p = addr(30_070);
        let addr_y = addr(30_071);

        let first = owner.configure_peer(addr_p, node.clone(), None).await;
        assert!(first.claim().is_accepted());
        assert_eq!(
            first.generation(),
            1,
            "sanity: a peer's first configure_peer call establishes generation 1"
        );

        let second = owner.configure_peer(addr_y, node.clone(), None).await;
        assert!(second.claim().is_accepted());
        assert_eq!(
            second.generation(),
            2,
            "sanity: a second call for the SAME peer bumps to generation 2"
        );
        assert_eq!(owner.routes_to(&addr_y), Some(node.clone()));

        // TOO LATE: a retry presenting the FIRST call's own, now-stale
        // generation (1), submitted strictly after generation 2 already
        // committed -- exactly the "commits last" ordering the finding
        // describes, reproduced here with no concurrency or timing at all.
        let stale_retry = owner.configure_peer(addr_p, node.clone(), Some(1)).await;
        assert_eq!(
            *stale_retry.claim(),
            ClaimCommit::Rejected(ClaimRejection::SupersededByNewerConfiguration),
            "a stale expected_generation must be rejected atomically at the owner, regardless \
             of when the retry's own command happens to arrive"
        );
        assert_eq!(
            stale_retry.evicted_pin(),
            None,
            "a superseded retry must touch nothing at all -- no eviction attempt"
        );
        assert_eq!(
            stale_retry.generation(),
            2,
            "the reported generation must be the CURRENT one (2), not the stale one this retry \
             presented"
        );
        assert_eq!(
            owner.routes_to(&addr_y),
            Some(node),
            "the newer call's own pin at addr_y must survive completely untouched by the \
             stale, rejected retry"
        );
        assert_eq!(
            owner.routes_to(&addr_p),
            None,
            "addr_p must remain unowned -- the stale retry must not have claimed it either"
        );
    }

    /// P1 regression: `pin` used to be processed as a command entirely
    /// separate from the claim that was supposed to justify it, so it never
    /// verified `peer_id` actually owned `addr` at the moment it ran. The
    /// atomic `configure_peer` transaction closes this by construction: the
    /// pin step only ever runs after this SAME call's own claim just
    /// committed, so a claim rejection (a different identity already
    /// verified-owns the address) must leave NEITHER a pin NOR a route
    /// behind for the rejected peer.
    #[tokio::test]
    async fn configure_peer_never_pins_when_the_claim_is_rejected() {
        let (owner, publisher) = owner_handle();
        let incumbent = peer("cfgpeer-incumbent");
        let challenger = peer("cfgpeer-challenger");
        let target = addr(30_062);

        let original = owner
            .claim(
                target,
                claim_of(incumbent.clone(), ClaimKind::Verified),
                false,
            )
            .await;
        let original_generation = original.commit_seq().expect("original claim commits");
        let events_before = publisher.events();
        let configured_routes_before = publisher.configured_routes();

        let commit = owner.configure_peer(target, challenger.clone(), None).await;

        assert!(!commit.claim().is_accepted());
        assert_eq!(commit.evicted_pin(), None);
        assert_eq!(commit.evicted_release_seq(), None);
        assert_eq!(
            owner.routes_to(&target),
            Some(incumbent.clone()),
            "a rejected claim must never install a pin for the challenger"
        );
        assert_eq!(
            publisher.events(),
            events_before,
            "a rejected claim must publish no ownership route change"
        );
        assert_eq!(
            publisher.configured_routes(),
            configured_routes_before,
            "a rejected claim must publish no configured-route write either"
        );
        // The strongest check: if the rejected claim had wrongly installed a
        // pin for `challenger` anyway, `target` would now refuse release
        // even though `incumbent` genuinely still owns it (`release`'s
        // FIRST check is `operator_pinned`). No pin must have been
        // installed, so the incumbent's own, still perfectly valid release
        // must succeed.
        assert!(
            owner
                .release(target, incumbent, original_generation)
                .await
                .is_some(),
            "a rejected challenger claim must not leave `target` pinned against its \
             genuine, still-valid incumbent owner"
        );
    }

    /// Replacing an address pin must remove the displaced peer's reverse
    /// mapping. Otherwise a later pin for the displaced peer can consult the
    /// stale reverse entry, evict the current owner's pin, and leave the
    /// address unprotected.
    #[tokio::test]
    async fn replacing_an_address_pin_clears_the_displaced_reverse_mapping() {
        let (owner, _publisher) = owner_handle();
        let first = peer("pin-reverse-first");
        let second = peer("pin-reverse-second");
        let addr_a = addr(49_000);
        let addr_b = addr(49_001);

        assert_eq!(owner.pin(addr_a, first.clone()).await, None);
        assert_eq!(owner.pinned_addr_for(&first), Some(addr_a));

        assert_eq!(owner.pin(addr_a, second.clone()).await, None);
        assert_eq!(owner.pin_owner(&addr_a), Some(second.clone()));
        assert_eq!(owner.pinned_addr_for(&first), None);
        assert_eq!(owner.pinned_addr_for(&second), Some(addr_a));

        // If the displaced reverse entry survived, this pin would treat
        // `addr_a` as `first`'s current pin and evict `second`'s live pin.
        assert_eq!(owner.pin(addr_b, first.clone()).await, None);
        assert_eq!(owner.pin_owner(&addr_a), Some(second));
        assert_eq!(owner.pin_owner(&addr_b), Some(first.clone()));
        assert_eq!(owner.pinned_addr_for(&first), Some(addr_b));
    }

    /// P2 regression: two concurrent `configure_peer` calls for the SAME
    /// peer, each targeting a different new address, each read the same
    /// "previous address" from `ConnectionPool` before either applied its
    /// own change. If `pin` trusted that caller-supplied address instead of
    /// its own reverse map, both addresses could end up pinned at once --
    /// and since a pinned address can never be reclaimed by `release`, the
    /// loser would stay reserved forever. `pin` must instead evict whatever
    /// this peer is ACTUALLY pinned at right now, so exactly one of the two
    /// concurrent calls reports an eviction and the other address is left
    /// ordinarily reclaimable.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_pins_for_the_same_peer_never_leave_two_addresses_pinned() {
        for round in 0..200u16 {
            let (owner, _publisher) = owner_handle();
            let node = peer("concurrent-pin");
            let addr_a = addr(50_000 + round);
            let addr_b = addr(51_000 + round);

            // Mirrors `configure_peer`'s own claim-before-pin sequencing.
            owner
                .claim(addr_a, claim_of(node.clone(), ClaimKind::Verified), false)
                .await;
            owner
                .claim(addr_b, claim_of(node.clone(), ClaimKind::Verified), false)
                .await;

            let barrier = Arc::new(tokio::sync::Barrier::new(2));

            let owner_a = owner.clone();
            let node_a = node.clone();
            let barrier_a = barrier.clone();
            let task_a = tokio::spawn(async move {
                barrier_a.wait().await;
                owner_a.pin(addr_a, node_a).await
            });

            let owner_b = owner.clone();
            let node_b = node.clone();
            let barrier_b = barrier.clone();
            let task_b = tokio::spawn(async move {
                barrier_b.wait().await;
                owner_b.pin(addr_b, node_b).await
            });

            let (evicted_a, evicted_b) = tokio::join!(task_a, task_b);
            let evicted_a = evicted_a.expect("task a panicked");
            let evicted_b = evicted_b.expect("task b panicked");

            // Whichever `pin` command the owner processes FIRST evicts
            // nothing (no prior pin exists yet); whichever it processes
            // SECOND evicts the first one's address. Exactly one of the two
            // must report an eviction, regardless of which order the owner
            // actually serialized them in.
            assert_ne!(
                evicted_a.is_some(),
                evicted_b.is_some(),
                "round {round}: exactly one concurrent pin command must report evicting \
                 the other's address"
            );

            // Whichever call reported an eviction ran LAST and won: its own
            // target address is the one left pinned, and the address it
            // evicted (the other call's target) is now reclaimable.
            let (still_pinned, now_reclaimable) = if let Some(evicted) = evicted_a {
                (addr_a, evicted)
            } else {
                (
                    addr_b,
                    evicted_b.expect("exactly one of the two must evict"),
                )
            };

            let token = owner
                .ownership_token(&now_reclaimable)
                .expect("still owned");
            assert!(
                owner
                    .release(now_reclaimable, node.clone(), token.generation())
                    .await
                    .is_some(),
                "round {round}: the address the losing pin evicted must be ordinarily \
                 reclaimable, not stuck forever"
            );

            let token = owner.ownership_token(&still_pinned).expect("still owned");
            assert!(
                owner
                    .release(still_pinned, node.clone(), token.generation())
                    .await
                    .is_none(),
                "round {round}: the address that won the race must still refuse release \
                 while pinned"
            );
        }
    }

    /// P2 companion: a DNS-triggered `migrate` (which carries a pin from its
    /// source to its destination) racing a concurrent `configure_peer`
    /// `pin` for the SAME peer must not leave two pins standing either --
    /// `migrate`'s pin-carry updates the same `pinned_by_peer` reverse map
    /// `pin` consults, so whichever of the two the owner serializes LAST
    /// still sees an accurate "where is this peer pinned right now".
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_migrate_and_pin_never_leave_two_pins_for_one_peer() {
        for round in 0..200u16 {
            let (owner, _publisher) = owner_handle();
            let node = peer("migrate-vs-pin");
            let addr_a = addr(52_000 + round);
            let addr_b = addr(53_000 + round);
            let addr_c = addr(54_000 + round);

            owner
                .claim(addr_a, claim_of(node.clone(), ClaimKind::Verified), false)
                .await;
            owner.pin(addr_a, node.clone()).await;
            let expected_source = current_source(&owner, addr_a);

            let barrier = Arc::new(tokio::sync::Barrier::new(2));

            let owner_m = owner.clone();
            let barrier_m = barrier.clone();
            let task_migrate = tokio::spawn(async move {
                barrier_m.wait().await;
                owner_m
                    .migrate(addr_a, addr_b, expected_source, false)
                    .await
            });

            let owner_p = owner.clone();
            let node_p = node.clone();
            let barrier_p = barrier.clone();
            let task_pin = tokio::spawn(async move {
                barrier_p.wait().await;
                // Mirrors `configure_peer`'s own claim-before-pin ordering.
                owner_p
                    .claim(addr_c, claim_of(node_p.clone(), ClaimKind::Verified), false)
                    .await;
                owner_p.pin(addr_c, node_p).await
            });

            let (migrate_result, pin_result) = tokio::join!(task_migrate, task_pin);
            let migrate_result = migrate_result.expect("migrate task panicked");
            pin_result.expect("pin task panicked");
            assert!(
                migrate_result.moved(),
                "round {round}: migrate must succeed regardless of the concurrent pin \
                 racing it -- neither touches the other's ownership decision"
            );

            // Across every address this peer still owns after the race, at
            // most one may remain pinned.
            let mut pinned_count = 0;
            for candidate in [addr_a, addr_b, addr_c] {
                if let Some(token) = owner.ownership_token(&candidate)
                    && token.owner() == &node
                    && owner
                        .release(candidate, node.clone(), token.generation())
                        .await
                        .is_none()
                {
                    pinned_count += 1;
                }
            }
            assert_eq!(
                pinned_count, 1,
                "round {round}: exactly one address may remain pinned for this peer \
                 after a migrate races a concurrent pin"
            );
        }
    }
}
