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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use arc_swap::ArcSwap;
use crossbeam_queue::ArrayQueue;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, trace, warn};

use crate::PeerId;
use crate::addr_ownership::{
    AddrClaimOutcome, Claim, ClaimKind, Decision, Owner, RejectReason, arbitrate, resolved_kind,
};

/// Mailbox depth. Claims occur on connection establishment and gossip
/// full-syncs, not on the message hot path, so a modest bound is ample; a
/// full mailbox applies backpressure to the claimant rather than dropping a
/// decision.
const OWNER_MAILBOX_CAPACITY: usize = 512;

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
    /// Operator-pin identity, published separately from ownership: a pin
    /// is a DIFFERENT fact than "who owns this address" -- decided and
    /// moved only by `configure_peer`'s atomic transaction and `migrate`'s
    /// pin carry, never by `claim`. Neither `ConnectionPool`'s derived
    /// `required_addr` (moved by every `.connect()`, configured or not)
    /// nor the ownership generation (advanced by every accepted claim,
    /// including unrelated chatter) answers "is this peer still the one I
    /// pinned here" -- only this does.
    pin_shards: [Arc<HashMap<SocketAddr, PeerId>>; ROUTING_SNAPSHOT_SHARDS],
    /// Reverse of `pin_shards`: the address, if any, `peer_id` is CURRENTLY
    /// operator-pinned to. Kept in the SAME `with_pin` step as the
    /// addr-keyed side, so the two can never disagree. Not sharded --
    /// operator pins are expected to be orders of magnitude fewer than
    /// gossiped addresses. Exists so a non-owner caller can cheaply,
    /// lock-freely check "is this peer pinned to some OTHER address" --
    /// see `Peer::connect_with_route_mode`'s use of `pinned_addr_for`.
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
    /// The authoritative "did I lose the race to a concurrent
    /// reconfiguration" check, not a "who owns this now" query: it reads
    /// the SAME pin decision `configure_peer`/`migrate` themselves just
    /// published, not a value an unrelated path (`required_addr`, the
    /// ownership generation) can move independently of the pin question.
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
/// `#[non_exhaustive]`: adding a variant to this already-public enum is an
/// unavoidable break for an exhaustive external match, but costs existing
/// consumers nothing further once marked, while preventing the same
/// silent break next time this enum grows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ClaimRejection {
    /// The arbitration truth table refused the claim.
    Arbitration(RejectReason),
    /// The owner task is not reachable. Fail closed: no address-keyed
    /// mutation may proceed on a decision that was never actually made.
    OwnerUnavailable,
    /// Another caller currently holds a reap reservation for this address.
    /// Refused unconditionally, before `arbitrate` is even consulted: the
    /// holder's destructive, non-owner work relies on nothing committing
    /// ownership out from under it while held. Worth retrying -- released
    /// promptly once the holder is done, successfully or not.
    ReapInProgress,
    /// A `configure_peer` retry presented an `expected_generation` older
    /// than the current value -- a LATER call for the SAME peer already
    /// superseded it, atomically, at the owner. Refused before touching
    /// anything; unlike `ReapInProgress`, not worth retrying, since
    /// generations only increase.
    SupersededByNewerConfiguration,
}

/// The owner's complete decision for a dead-peer release.
///
/// `ProvenAlive` is distinct from `NotApplicable`: the former means direct
/// or ordinary liveness evidence arrived after the sweep's failure boundary,
/// so callers must preserve every side table as well as ownership; the latter
/// means this identity simply has no releasable ownership (for example an
/// operator pin or an already-displaced owner), so non-owner cleanup may
/// still proceed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadPeerReleaseOutcome {
    Released(CommitSeq),
    ProvenAlive,
    NotApplicable,
}

/// Monotonic position of a committed mutation in the owner task's total
/// order. Issued by the owner task alone, so it is a true sequence number and
/// not a timestamp: `a < b` means `a` was committed strictly before `b`.
pub type CommitSeq = u64;

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
/// `#[non_exhaustive]` for the same reason as `ClaimRejection`'s own.
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
    /// the identity holding it. A caller re-keying its own identity-scoped
    /// state must resolve the identity BEFORE issuing the command; naming
    /// the expected owner makes the move conditional on that resolution
    /// still holding, so a displaced caller re-keys nothing instead of
    /// re-keying the wrong identity onto the destination.
    SourceOwnerMismatch,
    /// Another caller currently holds a reap reservation for `from`, `to`,
    /// or both. Refused before any ownership state is inspected: `migrate`
    /// mutates `addr_ownership`/`claim_committed_at` directly, without
    /// going through `claim`'s own `reap_reserved` check, so it's checked
    /// here instead -- a sweep relies on `reap_reserved` keeping both
    /// addresses fixed for its destructive work's duration. Worth
    /// retrying, like `ClaimRejection::ReapInProgress`.
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
    /// never be observed disagreeing: a caller that instead reads the pin
    /// and performs an equivalent mutation itself is never truly atomic
    /// with the owner's own commands, since `ConnectionPool`'s maps aren't
    /// protected by one lock spanning a whole owner command -- two
    /// concurrent `configure_peer` calls for the same peer could then have
    /// their pin decided in one order by the owner but this write land in
    /// the other order on `ConnectionPool`. This is the ONLY place the
    /// reindex may happen; three prior attempts fencing on other
    /// externally-observed state (ownership generation, `required_addr`,
    /// a separately-read `pinned_addr` mirror) all left the same class of
    /// gap open.
    ///
    /// `evicted_addr`, `Some` whenever this SAME command's pin decision
    /// evicted a different address from `peer_id`'s pin, matters because
    /// leaving `connections_by_addr[evicted_addr]` un-retracted is
    /// misdelivery, not just lost state: once a different identity claims
    /// `evicted_addr`, `ConnectionPool::get_connection_by_peer_id`'s
    /// address-fallback reads the stale alias, finds it `is_usable_
    /// connection` (a liveness check only, not an identity check), and
    /// publishes the OLD peer's live connection as the NEW peer's current
    /// one -- traffic addressed to the new identity delivered over the old
    /// identity's TCP stream. See `ConnectionPool::evict_pin_alias`'s own
    /// doc comment, which reintroduced exactly this for an outbound
    /// connection.
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
    /// If the eviction above also released that address's ownership, the
    /// position of that release in the owner's commit order. Released in
    /// this SAME synchronous step as the eviction, never as a separate,
    /// later command a concurrent claim or migrate could land in front of.
    evicted_release_seq: Option<CommitSeq>,
    /// This peer's CURRENT `configure_peer_generation` value as of this
    /// SAME atomic transaction -- see that field's own doc comment. A
    /// first call must capture and later present this back as
    /// `expected_generation` for a retry. Present regardless of `claim`'s
    /// outcome, including `SupersededByNewerConfiguration` itself.
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
        /// Monotonic instant when the authenticated transport supplied this
        /// evidence. Capture it before mailbox queueing so delayed owner
        /// processing cannot make old evidence appear fresh.
        evidence_at: std::time::Instant,
        reply: oneshot::Sender<ClaimCommit>,
    },
    /// Atomically take every connection-scoped receipt `peer_id` holds for
    /// `session_source`, AND release ownership of every address no OTHER
    /// live session still covers -- in this SAME command, not as
    /// candidates for a separately-ordered `Release` call (see
    /// `PeerRegistryOwner::release_session`'s doc comment for why
    /// splitting it that way strands addresses permanently).
    ReleaseSession {
        peer_id: PeerId,
        session_source: SocketAddr,
        reply: oneshot::Sender<Vec<(SocketAddr, CommitSeq)>>,
    },
    /// Release everything a peer that has been dead longer than the
    /// dead-peer timeout still holds at `addr`: every connection-scoped
    /// receipt recorded for `peer_id` at `addr` under any session, and the
    /// address ownership itself when it is not operator-pinned.
    ReleaseDeadPeer {
        peer_id: PeerId,
        addr: SocketAddr,
        evidence_before: std::time::Instant,
        reply: oneshot::Sender<DeadPeerReleaseOutcome>,
    },
    /// Read `ReleaseDeadPeer`'s causal liveness fence without performing
    /// any side effects. Cleanup uses this before destroying actors or
    /// emitting tombstones so a reconnect that raced selection protects the
    /// entire candidate, not merely its address ownership.
    HasNewerLivenessEvidence {
        addr: SocketAddr,
        evidence_before: std::time::Instant,
        reply: oneshot::Sender<bool>,
    },
    /// Best-effort read of activity since a reserved reap's baseline.
    /// Liveness evidence and operator reconfiguration are independent ways
    /// for the selection to become stale, so the caller checks both in one
    /// owner-serialized read immediately before destructive work. This is a
    /// mitigation, not an authorization: activity can still commit after
    /// this read returns and before the caller mutates shared state.
    ReapBaselineActivityDetected {
        addr: SocketAddr,
        peer_id: PeerId,
        evidence_before: std::time::Instant,
        baseline_configure_peer_generation: u64,
        reply: oneshot::Sender<bool>,
    },
    /// Read the current operator-configuration generation for `peer_id` so
    /// a reap can capture a baseline before consuming its reservation.
    ConfigurePeerGenerationOf {
        peer_id: PeerId,
        reply: oneshot::Sender<u64>,
    },
    /// Atomically checks the causal fence a dead-peer reap also checks
    /// (does `addr` have DIRECT evidence of a live owner causally NEWER
    /// than `evidence_before`?) AND revalidates the full identity selection
    /// observed for `addr` -- ownership and operator pin state -- against
    /// the owner's current state; only if every check passes is `addr`
    /// marked reserved (see `reap_reserved`'s doc comment). Returns whether
    /// the reservation was granted.
    ///
    /// A reservation, not a plain check-then-act read: a query that only
    /// answers "is it safe right now" is stale the instant a concurrent
    /// claim commits before the caller acts on it. A reservation instead
    /// gives the caller a fact the owner itself continues to enforce (via
    /// `claim`'s own refusal) for as long as it is held.
    ///
    /// Both checks are required, for different windows: the causal fence
    /// covers failure-to-selection (a claim can commit before `GossipState`
    /// reflects it). The identity-match check covers selection-to-this-
    /// command against a DIFFERENT identity claiming `addr` -- a claim
    /// that doesn't refresh `claim_committed_at` (gossip/discovery, or
    /// `configure_peer`) would slip past the causal fence alone.
    /// `expected_node_id` closes a further gap: a claim landing while
    /// selection's `GossipState` read and its separate ownership/pin reads
    /// straddled a change could otherwise pair the OLD peer's `node_id`
    /// with the NEW owner's token.
    ReserveForReap {
        addr: SocketAddr,
        /// When the `GossipState` failure evidence this candidate was
        /// selected on was recorded. Fixed at submission time, never
        /// re-derived from "now" inside the owner, so this fence cannot be
        /// satisfied merely by elapsed wall-clock delay.
        evidence_before: std::time::Instant,
        /// Ownership selection observed for `addr`. `None` means unowned
        /// at selection, and must still be unowned now.
        expected_ownership: Option<OwnershipToken>,
        /// The operator pin owner selection observed for `addr`. `None`
        /// means unpinned at selection, and must still be unpinned now.
        expected_pin: Option<PeerId>,
        /// The `PeerId` the destructive phase will act against, validated
        /// against `expected_ownership`/`expected_pin` when they name a
        /// concrete identity. Unconstrained when both are `None`:
        /// `GossipState` routinely knows a `node_id` with no owner-level
        /// claim behind it (gossip/discovery chatter, or an independently
        /// released address) -- legitimate, not evidence of a race.
        expected_node_id: Option<PeerId>,
        /// `Some(valid)` when granted -- the SAME `Arc<AtomicBool>` the
        /// owner's `reap_reserved` map stores for this address, so the
        /// caller's `ReapReservation` guard and the owner's entry share
        /// one flag. `None` when refused.
        reply: oneshot::Sender<Option<Arc<AtomicBool>>>,
    },
    /// One-shot, owner-coordinated authorization for a granted
    /// reservation's destructive work -- see `ReapReservation::
    /// try_consume`'s own doc comment for why this must be an owner round
    /// trip rather than a client-side CAS. `true` exactly once per
    /// reservation; `false` for a missing or already-consumed entry.
    ConsumeReapReservation {
        addr: SocketAddr,
        reply: oneshot::Sender<bool>,
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
    /// whatever address this SAME peer was pinned at beforehand and
    /// releasing that evicted address's ownership if `peer_id` still holds
    /// it, all in one `&mut self` step. This is the atomic transaction
    /// `GossipRegistry::configure_peer` submits in place of
    /// separately-ordered claim, pin, and release commands, closing the
    /// interleaving window a concurrent call could otherwise exploit
    /// between the claim taking effect and the pin landing -- see
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
    /// submitted as an owner command rather than writing `ConnectionPool`
    /// directly: it writes the SAME fields `install_pin`/`migrate` do via
    /// `set_configured_peer_addr`, so a caller-side "is this peer pinned
    /// elsewhere" check done outside the owner's serialization could be
    /// invalidated by a pin decision landing in the gap. `reply` carries
    /// whether the write happened -- `false` means `peer_id` is pinned to
    /// a DIFFERENT address and the write was declined; the caller MUST
    /// consult this (an earlier version discarded it and shipped a bug).
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
    /// Test-only, side-effect-free read of `claim_committed_at` for `addr` --
    /// direct evidence a connection-scoped claim ever recorded for it. No
    /// production reader exists in this crate yet; this exists so
    /// tests can verify `claim_connection_scoped`/`migrate`'s own
    /// bookkeeping of this field directly, without depending on a consumer
    /// that does not exist yet.
    #[cfg(test)]
    InspectClaimCommittedAt {
        addr: SocketAddr,
        reply: oneshot::Sender<Option<std::time::Instant>>,
    },
    /// Pure, side-effect-free read of whether `addr` currently has a live
    /// reap reservation held for it. This is test-only observability for
    /// proving that a sweep reserves one candidate at a time.
    #[cfg(test)]
    IsReapReserved {
        addr: SocketAddr,
        reply: oneshot::Sender<bool>,
    },
    /// Record direct liveness evidence for `addr`, observed at `at`, through
    /// the owner's serialized command stream. This keeps the evidence write
    /// atomic with the dead-peer release decision that consumes it.
    NoteLivenessEvidence {
        addr: SocketAddr,
        at: std::time::Instant,
    },
}

/// Shared state behind every [`RegistryOwnerHandle`] clone.
struct OwnerShared {
    tx: mpsc::Sender<OwnerCommand>,
    /// Dedicated, UNBOUNDED channel carrying `OwnerCommand::
    /// ReleaseReapReservation` exclusively -- never the bounded `tx`
    /// mailbox above. See `ReapReservation`'s doc comment for why a
    /// release must never be droppable mid-abort. Deliberately not
    /// unbounded for every command: an unbounded queue for ordinary
    /// traffic would let flooding requests grow the backlog without
    /// limit; releases are bounded instead by how many reservations are
    /// concurrently held, not by caller behavior.
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

    /// Record direct liveness evidence for an address through the same
    /// serialized owner mailbox used by dead-peer release. This is reserved
    /// for an application-level response received from the peer; indirect
    /// gossip/discovery chatter must not refresh it.
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
        self.claim_connection_scoped_at(addr, claim, session_source, std::time::Instant::now())
            .await
    }

    /// Submit a connection-scoped claim with the transport evidence instant
    /// captured before the owner mailbox can delay processing it.
    pub(crate) async fn claim_connection_scoped_at(
        &self,
        addr: SocketAddr,
        claim: Claim,
        session_source: SocketAddr,
        evidence_at: std::time::Instant,
    ) -> ClaimCommit {
        self.ensure_started();
        let (reply, response) = oneshot::channel();
        let command = OwnerCommand::ClaimConnectionScoped {
            addr,
            claim,
            session_source,
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

    /// See `OwnerCommand::ReleaseSession`'s doc comment. Returns the
    /// addresses actually released, paired with the resulting commit
    /// sequence -- callers tombstone their own `gossip_state` projection
    /// at that sequence. An unreachable owner reports nothing released.
    pub async fn release_session(
        &self,
        peer_id: PeerId,
        session_source: SocketAddr,
    ) -> Vec<(SocketAddr, CommitSeq)> {
        self.ensure_started();
        let (reply, response) = oneshot::channel();
        let command = OwnerCommand::ReleaseSession {
            peer_id,
            session_source,
            reply,
        };
        if self.shared.tx.send(command).await.is_err() {
            return Vec::new();
        }
        response.await.unwrap_or_default()
    }

    /// Release all ownership and connection-scoped receipts that remain for
    /// `peer_id` at `addr` after a dead-peer sweep. The outcome distinguishes
    /// a live peer from an address that simply has nothing applicable to
    /// release, so callers can gate unrelated side-table destruction safely.
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
            return DeadPeerReleaseOutcome::ProvenAlive;
        }
        response
            .await
            .unwrap_or(DeadPeerReleaseOutcome::ProvenAlive)
    }

    /// Read `release_dead_peer`'s causal liveness fence without applying its
    /// ownership side effects. Fail closed when the owner is unavailable:
    /// cleanup must preserve a candidate it cannot prove safe to destroy.
    pub async fn has_newer_liveness_evidence_since(
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

    /// Compatibility spelling for callers that ask the same owner-side
    /// causal fence without the historical `_since` suffix.
    pub async fn has_newer_liveness_evidence(
        &self,
        addr: SocketAddr,
        evidence_before: std::time::Instant,
    ) -> bool {
        self.has_newer_liveness_evidence_since(addr, evidence_before)
            .await
    }

    /// Read whether activity has been observed since a reserved reap's
    /// baseline. The result is true when either newer direct liveness
    /// evidence exists or the same peer was operator-reconfigured after the
    /// captured baseline. This narrows the post-consume window; it is not an
    /// atomic authorization for the caller's later mutation. An unavailable
    /// owner fails closed.
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

    /// Read the current operator-configuration generation for `peer_id`.
    /// An unavailable owner returns the fail-closed sentinel, which is not a
    /// real generation and therefore cannot authorize a later reap.
    pub async fn configure_peer_generation_of(&self, peer_id: PeerId) -> u64 {
        self.ensure_started();
        let (reply, response) = oneshot::channel();
        let command = OwnerCommand::ConfigurePeerGenerationOf { peer_id, reply };
        if self.shared.tx.send(command).await.is_err() {
            return u64::MAX;
        }
        response.await.unwrap_or(u64::MAX)
    }

    /// See `OwnerCommand::ReserveForReap`'s doc comment for the causal
    /// fence and identity checks this performs. Returns a
    /// [`ReapReservation`] guard when granted, `None` when refused
    /// (fail-closed, including when the owner is unreachable). The
    /// returned guard is what makes a granted reservation impossible to
    /// leak -- see `ReapReservation`'s own doc comment for why a bare
    /// `bool` (this method's previous return type) could not guarantee
    /// that.
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

    /// `ReapReservation::try_consume`'s owner round trip -- see that
    /// method's own doc comment. `false` on an unreachable owner, the same
    /// fail-closed default as every other "cannot obtain a decision" path.
    async fn consume_reap_reservation(&self, addr: SocketAddr) -> bool {
        self.ensure_started();
        let (reply, response) = oneshot::channel();
        if self
            .shared
            .tx
            .send(OwnerCommand::ConsumeReapReservation { addr, reply })
            .await
            .is_err()
        {
            return false;
        }
        response.await.unwrap_or(false)
    }

    /// Enqueue an `OwnerCommand::ReleaseReapReservation` on the dedicated
    /// unbounded release channel -- see `OwnerShared::release_tx`'s doc
    /// comment. Deliberately synchronous, not `async`, so it is callable
    /// from both `ReapReservation::release` and its synchronous `Drop`
    /// impl.
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
    /// atomically replacing any address this peer was previously pinned
    /// at. Returns the evicted address, if any -- the caller's cue to also
    /// release that address's ownership. Does NOT itself verify `peer_id`
    /// owns `addr`; `GossipRegistry::configure_peer` claims ownership
    /// first in the SAME atomic step rather than calling this directly.
    ///
    /// Looks up the previous pinned address from the owner's own reverse
    /// map at the moment this command runs, rather than trusting a
    /// caller-supplied value: two concurrent callers for the same peer
    /// observing the same stale "previous address" from `ConnectionPool`
    /// could otherwise each pin independently and leave both addresses
    /// pinned forever. Reading it here means whichever `pin` the owner
    /// serializes LAST always wins outright.
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
    /// `expected_generation`: `None` for a peer's first call (always
    /// applies, bumps `configure_peer_generation`); `Some(generation)` for
    /// a retry, accepted only when it is EXACTLY the stored generation --
    /// not merely not-less-than-it. A caller can only ever have legitimately
    /// learned `generation` from this SAME method's own prior response for
    /// this peer, so any other value is either stale (rejected as
    /// `SupersededByNewerConfiguration`, the same as too-small: a newer call
    /// already moved on) or not a value this fence ever produced at all --
    /// neither is worth retrying. `pub(crate)`: the retry path this
    /// parameter exists for is driven internally
    /// (`GossipRegistry::configure_peer_with_outcome_and_generation`); no
    /// caller outside this crate has a legitimate `Some(generation)` to
    /// present. See `configure_peer_generation`'s own doc comment for why
    /// this fences a stale retry from overwriting a newer request.
    pub(crate) async fn configure_peer(
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

    /// `Peer::connect`'s ordinary route update -- see
    /// `OwnerCommand::SetOrdinaryConnectRoute`'s doc comment. Returns
    /// whether `addr` actually became the effective route: `false` when
    /// declined (`peer_id` is pinned elsewhere) or the owner is
    /// unreachable.
    ///
    /// CALLERS MUST CONSULT THIS. An earlier version returned `()` and
    /// discarded the result: on a decline, it still dialed, inserted the
    /// requested address into `gossip_state`, and on success advertised a
    /// route this node never actually connected to.
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
            let owner_task = tokio::spawn(owner.run(rx, release_rx));
            tokio::spawn(Self::watch_owner_task(owner_task));
        }
    }

    /// Every `claim`/`release`/`migrate` already fails closed, independently,
    /// the moment its own send hits a closed mailbox -- but each of those
    /// sites logs a generic "unavailable" warning with no way to tell a
    /// panic apart from ordinary shutdown (`run`'s own `while let Some(..) =
    /// rx.recv()` loop exits cleanly, at `debug!`, once every handle is
    /// dropped). This watches the task itself for the one signal that
    /// distinguishes them: `JoinHandle::await` returning `Err` only on panic
    /// or external cancellation, never on a clean exit.
    async fn watch_owner_task(owner_task: tokio::task::JoinHandle<()>) {
        if let Err(join_error) = owner_task.await {
            error!(
                error = %join_error,
                "registry owner task exited unexpectedly (panic, not a clean shutdown); \
                 every subsequent claim/release/migrate will fail closed for the rest of \
                 this process's lifetime"
            );
            // Opt-in only: a dead owner silently wedges address arbitration
            // for good, which is far more likely to be mistaken for a hang
            // than diagnosed from a log line, so an operator who would
            // rather crash loudly and get restarted (under a supervisor)
            // than run on in this state can ask for that explicitly.
            // Defaulting to *off* means this can never surprise a test
            // suite or a deployment that has not opted in -- unlike
            // `panic!()`, which a detached task like this one would only
            // ever have tokio print and move past, `abort()` is the one
            // primitive that reliably takes the whole process down from
            // here, so the check is worth the environment read.
            if std::env::var_os("ICANACT_REMOTE_ABORT_ON_REGISTRY_OWNER_DEATH").is_some() {
                std::process::abort();
            }
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

    /// Observe `claim_committed_at` directly in deterministic tests -- see
    /// `OwnerCommand::InspectClaimCommittedAt`'s own doc comment.
    #[cfg(test)]
    pub(crate) async fn claim_committed_at_for_test(
        &self,
        addr: SocketAddr,
    ) -> Option<std::time::Instant> {
        self.ensure_started();
        let (reply, response) = oneshot::channel();
        self.shared
            .tx
            .send(OwnerCommand::InspectClaimCommittedAt { addr, reply })
            .await
            .ok()?;
        response.await.ok().flatten()
    }

    /// Observe whether an address currently has a live reap reservation.
    /// This read is side-effect-free and exists only for deterministic
    /// reservation-scope regression tests.
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

/// One granted reservation's owner-side bookkeeping. `consumed` is the
/// owner-authoritative record of whether `ConsumeReapReservation` has
/// already granted this entry's destructive-work authorization -- a plain
/// `bool`, not an atomic, since only the owner's own serialized command
/// processing ever reads or writes it. `valid` is a separate,
/// shared `Arc<AtomicBool>`, cloned to the matching `ReapReservation`
/// guard purely for that guard's cheap, external, non-authoritative
/// `is_still_valid()` peek.
struct ReapReservationEntry {
    valid: Arc<AtomicBool>,
    consumed: bool,
}

/// RAII guard for one reservation `RegistryOwnerHandle::reserve_for_reap`
/// granted -- the type system, not just documentation, enforces that a
/// caller either releases it or has `Drop` do so.
///
/// `release()` is the ordinary path: an explicit, awaited owner round trip
/// once the sweep's destructive work finishes. `Drop` exists only for a
/// hard task abort mid-sweep (`shutdown`/`shutdown_and_wait` both
/// `JoinHandle::abort()` the task running `cleanup_dead_peers`). Both paths
/// enqueue through the dedicated UNBOUNDED `release_tx` channel, never the
/// bounded `tx` mailbox: an unbounded send has no `.await` point an abort
/// can land inside, so by the time it returns the release is either
/// irrevocably queued or the owner task is already gone (a fresh owner
/// starts with an empty `reap_reserved`, nothing left to leak against).
/// `released` is set only strictly AFTER that enqueue succeeds -- setting
/// it earlier risks `Drop` seeing `released == true` after an aborted send
/// and leaking the reservation. Failing to TAKE a reservation is safe (the
/// sweep just skips that candidate); failing to RELEASE one is not, since
/// every later claim for that address is refused forever.
///
/// [`Self::try_consume`] is an owner round trip
/// (`OwnerCommand::ConsumeReapReservation`), not a client-side
/// compare-and-swap on `valid`: a bare CAS on a caller-held atomic races
/// an invalidating owner command (`configure_peer` evicting this address
/// from a pin) from a completely separate synchronization domain --
/// whichever runs "second" by wall-clock time still only ever sees a
/// plain store/CAS result, with no way to tell a reservation it just
/// invalidated was already consumed a moment earlier by the other side.
/// Routing consumption through the owner's own serialized command stream
/// instead means whichever the owner actually dequeues first -- the
/// consume or the invalidation -- is authoritative, and the loser can
/// observe that fact instead of silently believing it won:
/// `configure_peer`'s own handler checks `ReapReservationEntry::consumed`
/// before evicting a pin, refusing rather than invalidating out from under
/// an already-authorized reap. `valid` remains a shared `Arc<AtomicBool>`,
/// kept in step for [`Self::is_still_valid`]'s cheap, external,
/// non-authoritative peek -- but it no longer authorizes anything itself.
pub struct ReapReservation {
    owner: RegistryOwnerHandle,
    addr: SocketAddr,
    released: bool,
    valid: Arc<AtomicBool>,
}

impl ReapReservation {
    /// Cheap, synchronous, no `.await` -- advisory only. See this type's
    /// own doc comment for why this must never gate a destructive step;
    /// [`Self::try_consume`] is the actual authorization.
    pub fn is_still_valid(&self) -> bool {
        self.valid.load(Ordering::Acquire)
    }

    /// One-shot, race-free authorization for this reservation's
    /// RESERVATION-gated destructive work (not address-ownership
    /// retraction, which keeps its own separate, always-fresh fence -- see
    /// this type's own doc comment). An owner round trip, not a local CAS
    /// -- see the type's own doc comment for why. `true` means this call,
    /// and only this call, is authorized; `false` means something already
    /// invalidated the reservation and the candidate must be abandoned.
    /// Call exactly once per candidate, as the single gate for its whole
    /// sequence -- never per-step alongside a later `is_still_valid()`
    /// check treated as a second authorization.
    pub async fn try_consume(&self) -> bool {
        self.owner.consume_reap_reservation(self.addr).await
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
    /// session (refreshed only by `claim_connection_scoped`, never by
    /// plain gossip/discovery `claim`s). Independent of `GossipState`'s
    /// `failures`/`last_failure_time`, which only updates AFTER the owner
    /// has already committed the fresh claim -- so a caller checking this
    /// can never be fooled by a reconnect whose claim commit and
    /// `GossipState` update straddle its own observation, and can't be
    /// kept perpetually "fresh" by indirect chatter about a peer nothing
    /// has reconnected to.
    ///
    /// Deliberately narrow to NEW-CONNECTION/claim evidence, not ongoing
    /// liveness on an already-claimed connection -- a complementary fence
    /// for that is out of this crate's current scope.
    claim_committed_at: HashMap<SocketAddr, std::time::Instant>,
    /// Direct liveness evidence from the registry's response path. Kept
    /// outside the owner task's mailbox so an already-claimed connection can
    /// refresh this marker without fabricating a new ownership claim.
    /// Direct liveness evidence recorded only by `NoteLivenessEvidence` in
    /// this owner's serialized command stream.
    liveness_evidence_at: HashMap<SocketAddr, std::time::Instant>,
    /// Connection-scoped ownership receipts: which live authenticated
    /// sessions currently back a peer's claim on an address, and at what
    /// owner generation. Keyed by `(peer, session_source, addr)` --
    /// `session_source` is this exact connection's own discriminator, so
    /// a stale session's teardown can only ever remove its own entry.
    /// Every mutation happens from `&mut self` in the same synchronous
    /// command as the ownership commit/release it corresponds to, so a
    /// receipt can never be observed at a generation the owner doesn't
    /// simultaneously agree is current.
    connection_scoped_claims: HashMap<(PeerId, SocketAddr, SocketAddr), CommitSeq>,
    /// Addresses reserved by an explicit `GossipRegistry::configure_peer`
    /// call, independent of any connection. Invisible to
    /// `claim_connection_scoped`'s receipt bookkeeping and refused by any
    /// ordinary release path (checked directly, not inferred from the
    /// absence of a receipt): a session authenticating the same identity
    /// at a pinned address must not make the reservation releasable
    /// merely by connecting and disconnecting. Distinct from
    /// `ConnectionPool`'s `required_addr` (set by every `.connect()`,
    /// configured or not) -- conflating the two let an ordinary dial's
    /// address become permanently undisplaceable once its session ended.
    operator_pinned: HashMap<SocketAddr, PeerId>,
    /// Reverse index of `operator_pinned`: the address (if any) each peer is
    /// currently pinned at. `pin` looks a peer up here -- never trusts a
    /// caller-supplied "previous address" -- so installing a new pin always
    /// atomically replaces whatever this SAME peer was pinned at a moment
    /// ago, keeping "at most one pinned address per peer" true at every
    /// instant, not merely eventually.
    pinned_by_peer: HashMap<PeerId, SocketAddr>,
    /// Addresses a `cleanup_dead_peers` sweep has RESERVED for reaping --
    /// see `OwnerCommand::ReserveForReap`. While an address is a member
    /// here, `claim`/`claim_connection_scoped` refuse EVERY claim for it,
    /// which is what makes the sweep's later, non-owner destructive work
    /// safe to run OUTSIDE the owner's critical path without racing a
    /// concurrent reconnect. Released by `ReleaseReapReservation` once the
    /// sweep is done, successfully or not.
    ///
    /// EXCLUSIVE: granted only when THIS call actually inserts the address
    /// (`contains_key` checked immediately before inserting, same
    /// synchronous call) -- two concurrent sweeps must never share one
    /// guard, or whichever releases first would remove the entry while the
    /// other's destructive work still relied on it.
    ///
    /// The VALUE is what makes this a LIVE authority, not just a presence
    /// check. `consumed` is the owner-authoritative record of whether
    /// `ConsumeReapReservation` has already granted this entry's
    /// destructive-work authorization -- read here, from within the SAME
    /// serialized command stream, by anything that would otherwise
    /// invalidate the reservation (currently `configure_peer` evicting a
    /// reserved address from a peer's pin), so an already-consumed entry
    /// is never mistaken for one a fresh invalidation can still stop.
    /// `valid` is a shared `Arc<AtomicBool>`, cloned to the matching
    /// [`ReapReservation`] guard, for that guard's own cheap, external,
    /// non-authoritative `is_still_valid()` peek; kept in step with
    /// `consumed` but never itself the authority for either consumption
    /// or invalidation. See `ReapReservation`'s own doc comment for why
    /// consumption is an owner command rather than a client-side CAS.
    reap_reserved: HashMap<SocketAddr, ReapReservationEntry>,
    /// `GossipRegistry`'s own caller-side generation fence for
    /// `configure_peer`'s queued retry was not atomic with the pin update
    /// it guarded -- a newer call could bump the caller-side counter and
    /// commit its own pin after the check passed but before the stale
    /// retry's command reached the owner, installing the stale pin with
    /// no way for the already-passed caller-side check to catch it.
    /// Moved here, owner-side, so `configure_peer`'s atomic transaction
    /// validates a retry's generation in the SAME serialized step that
    /// installs the pin. The FIRST call for a peer bumps this
    /// monotonically and reports the new value back; every retry presents
    /// that value as `expected_generation`, accepted only on an EXACT
    /// match -- rejected outright both if a newer call has since bumped it
    /// further, and if the presented value was never actually stored here
    /// at all (see `RegistryOwnerHandle::configure_peer`'s own doc
    /// comment for why accepting the latter used to be a hole in this
    /// fence).
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
    /// bounded mailbox, and the dedicated unbounded release channel --
    /// `biased` so a ready release is always handled before a ready
    /// ordinary command, since a granted reservation should be held no
    /// longer than necessary once its release is queued. Re-checked on
    /// EVERY drained command, not just once per outer wakeup: draining
    /// `release_rx` fully then `rx` fully in two separate loops would let
    /// a synchronous burst of ordinary commands starve a release queued
    /// only after the first loop's single check, exhausting a caller's
    /// `ReapInProgress` retry budget even though the reservation it was
    /// waiting on had already been released.
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
            // Drain whatever else is queued, release commands first (same
            // priority reason as the select above), without re-suspending.
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
                evidence_at,
                reply,
            } => {
                let commit = self.claim_connection_scoped(addr, claim, session_source, evidence_at);
                let _ = reply.send(commit);
            }
            OwnerCommand::ReleaseSession {
                peer_id,
                session_source,
                reply,
            } => {
                let candidates = self.release_session(&peer_id, session_source);
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
                let has_newer_claim = self
                    .claim_committed_at
                    .get(&addr)
                    .is_some_and(|committed_at| *committed_at > evidence_before);
                let has_newer_response = self
                    .liveness_evidence_at
                    .get(&addr)
                    .copied()
                    .is_some_and(|observed_at| observed_at > evidence_before);
                let _ = reply.send(has_newer_claim || has_newer_response);
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
            OwnerCommand::ConsumeReapReservation { addr, reply } => {
                let consumed = self.consume_reap_reservation(addr);
                let _ = reply.send(consumed);
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
            OwnerCommand::InspectClaimCommittedAt { addr, reply } => {
                let _ = reply.send(self.claim_committed_at.get(&addr).copied());
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

    fn note_liveness_evidence(&mut self, addr: SocketAddr, at: std::time::Instant) {
        self.liveness_evidence_at
            .entry(addr)
            .and_modify(|existing| {
                if at > *existing {
                    *existing = at;
                }
            })
            .or_insert(at);
    }

    fn reap_baseline_activity_detected(
        &self,
        addr: SocketAddr,
        peer_id: &PeerId,
        evidence_before: std::time::Instant,
        baseline_configure_peer_generation: u64,
    ) -> bool {
        let has_newer_claim = self
            .claim_committed_at
            .get(&addr)
            .is_some_and(|committed_at| *committed_at > evidence_before);
        let has_newer_response = self
            .liveness_evidence_at
            .get(&addr)
            .copied()
            .is_some_and(|observed_at| observed_at > evidence_before);
        if has_newer_claim || has_newer_response {
            return true;
        }
        self.configure_peer_generation
            .get(peer_id)
            .copied()
            .unwrap_or(0)
            > baseline_configure_peer_generation
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
                // just advanced to, regardless of whether THIS claim is
                // itself connection-scoped: a receipt that doesn't move
                // with `claim_generation` is a stale CACHE of a token that
                // already moved, and `release`'s CAS would reject it with
                // no retry possible, stranding the address forever.
                for (key, generation) in self.connection_scoped_claims.iter_mut() {
                    if key.0 == node_id && key.2 == addr {
                        *generation = commit_seq;
                    }
                }
                // `claim_committed_at` is deliberately NOT touched here:
                // this method also serves the gossip/discovery path, whose
                // third-party claims can be repeated indefinitely regardless
                // of reachability. Only `claim_connection_scoped` (below) is
                // direct evidence of life, so only it refreshes this
                // timestamp.
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
    /// `session_source`, committed in the same synchronous step -- so two
    /// concurrent claims for the same peer+address can't finish their
    /// receipt transfer out of commit order, and a session exit racing a
    /// fresh claim can't strand a ghost receipt for the exiting session.
    /// The generation-sync transfer itself lives in `claim`, not here, and
    /// runs for every accepted same-owner claim, connection-scoped or
    /// not -- see its doc comment.
    fn claim_connection_scoped(
        &mut self,
        addr: SocketAddr,
        claim: Claim,
        session_source: SocketAddr,
        evidence_at: std::time::Instant,
    ) -> ClaimCommit {
        let peer_id = claim.node_id.clone();
        let commit = self.claim(addr, claim, /* is_local_addr */ false);
        if commit.is_accepted() {
            // Unlike the plain `claim` command, every call here is backed
            // by an actual connection -- direct evidence the peer is alive
            // right now -- so this is the one place `claim_committed_at`
            // is refreshed, regardless of whether the address ends up
            // pinned below.
            self.claim_committed_at
                .insert(addr, evidence_at);
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
                .insert((peer_id, session_source, addr), commit_seq);
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
    /// Folding both into one command closes a stranding window: a separate
    /// "find candidates, then `release` each with its generation" pair
    /// would let a plain, same-identity claim land in between and move
    /// `claim_generation`, so `release`'s CAS rejects the stale generation
    /// with no retry possible, stranding the address permanently. Folded,
    /// no CAS is needed: nothing can move `claim_generation` mid-call, so
    /// checking "is `peer_id` still `addr`'s owner right now" is enough.
    ///
    /// An address is only released when NO other live session still holds
    /// a receipt for the same peer+address, checked after this session's
    /// own entries are already removed, in the same step. Returns the
    /// addresses actually released, paired with the resulting commit
    /// sequence, for the caller to tombstone its own `gossip_state`
    /// ownership projection at.
    fn release_session(
        &mut self,
        peer_id: &PeerId,
        session_source: SocketAddr,
    ) -> Vec<(SocketAddr, CommitSeq)> {
        let mut own_entries = Vec::new();
        self.connection_scoped_claims.retain(|key, generation| {
            if &key.0 == peer_id && key.1 == session_source {
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

    /// Release everything `peer_id` still holds at `addr`: every
    /// connection-scoped receipt recorded for it there under any session
    /// (including a teardown that never ran), and the ownership record itself
    /// if this peer still owns the address and it is not operator-pinned.
    /// This is deliberately one owner-task operation so a stale peer reap
    /// cannot race a reconnect between receipt cleanup and ownership release.
    fn release_dead_peer(
        &mut self,
        peer_id: &PeerId,
        addr: SocketAddr,
        evidence_before: std::time::Instant,
    ) -> DeadPeerReleaseOutcome {
        if self
            .claim_committed_at
            .get(&addr)
            .is_some_and(|committed_at| *committed_at > evidence_before)
        {
            trace!(
                addr = %addr,
                peer = %peer_id,
                "dead-peer release refused: address has direct evidence of life after the failure this reap is acting on"
            );
            return DeadPeerReleaseOutcome::ProvenAlive;
        }
        if self
            .liveness_evidence_at
            .get(&addr)
            .copied()
            .is_some_and(|observed_at| observed_at > evidence_before)
        {
            trace!(
                addr = %addr,
                peer = %peer_id,
                "dead-peer release refused: address has response evidence of life after the failure this reap is acting on"
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

    /// Invalidate a currently-held, NOT YET CONSUMED reap reservation for
    /// `addr`. Returns `true` when it did -- the caller's cue that it
    /// genuinely prevented that reservation's destructive work from ever
    /// being authorized. Returns `false` for both a missing entry and,
    /// critically, an already-consumed one: `ConsumeReapReservation` may
    /// have granted authorization for this exact address moments earlier,
    /// from within this SAME serialized command stream, and a `store`
    /// here cannot retroactively revoke it -- the caller must not treat
    /// that as "successfully stopped," or it proceeds as if the
    /// reservation's destructive work will never run, while it may
    /// already be committed to running regardless.
    ///
    /// Any owner command that commits a fact making `addr` no longer
    /// genuinely worth reaping should call this as part of that SAME
    /// atomic commit. Currently only `configure_peer` does, the instant an
    /// operator's own reconfiguration evicts `addr` from a peer's pin --
    /// and it consults the return value precisely because of the case
    /// above.
    fn invalidate_reap_reservation(&self, addr: SocketAddr) -> bool {
        match self.reap_reserved.get(&addr) {
            Some(entry) if !entry.consumed => {
                entry.valid.store(false, Ordering::Release);
                true
            }
            _ => false,
        }
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

    /// `OwnerCommand::ReserveForReap`'s handler -- see its doc comment for
    /// why the causal fence and the ownership/pin/node_id checks are all
    /// required. `expected_node_id` is checked LAST, deliberately, against
    /// ownership/pin only after THIS call has freshly reconfirmed them
    /// current: checking it against a selection-time snapshot instead
    /// would tell us nothing about whether it still holds once this
    /// command actually runs, since all three could have gone stale
    /// together in the interim.
    fn reserve_for_reap(
        &mut self,
        addr: SocketAddr,
        evidence_before: std::time::Instant,
        expected_ownership: Option<OwnershipToken>,
        expected_pin: Option<PeerId>,
        expected_node_id: Option<PeerId>,
    ) -> Option<Arc<AtomicBool>> {
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
        // Checked only when ownership/pin (just reconfirmed current above)
        // name a concrete identity. `None`/`None` means unowned AND
        // unpinned -- a real, legitimate state (e.g. gossip/discovery
        // chatter about a peer never itself claimed), not evidence of a
        // race -- so `expected_node_id` is unconstrained there. The
        // adversarial case this closes is the opposite: a concrete,
        // just-reconfirmed identity that `expected_node_id` disagrees
        // with or is silent about -- fail-closed, like every check here.
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
        let valid = Arc::new(AtomicBool::new(true));
        self.reap_reserved.insert(
            addr,
            ReapReservationEntry {
                valid: valid.clone(),
                consumed: false,
            },
        );
        Some(valid)
    }

    /// `OwnerCommand::ConsumeReapReservation`'s handler -- the owner round
    /// trip [`ReapReservation::try_consume`] performs instead of a
    /// client-side CAS. `false` when there is no entry (already released
    /// or invalidated) or it is already consumed (a second consume attempt
    /// for the same reservation, which must never succeed twice). `true`
    /// exactly once per reservation: sets `consumed` and mirrors the flip
    /// into `valid` so the guard's own external `is_still_valid()` peek
    /// stays accurate without a second round trip.
    fn consume_reap_reservation(&mut self, addr: SocketAddr) -> bool {
        match self.reap_reserved.get_mut(&addr) {
            Some(entry) if !entry.consumed => {
                entry.consumed = true;
                entry.valid.store(false, Ordering::Release);
                true
            }
            _ => false,
        }
    }

    /// Atomically install `peer_id`'s operator pin at `addr`, evicting
    /// whatever address `pinned_by_peer` shows this SAME peer pinned at
    /// beforehand (if different), AND whatever different peer `addr`
    /// itself was previously pinned to. The first eviction is keyed off
    /// `pinned_by_peer`, not an address the caller believes was previously
    /// configured (which can be stale by the time this runs): consulting
    /// the owner's own authoritative reverse map guarantees at most one
    /// pinned address per peer at every instant. The second keeps
    /// `operator_pinned` and `pinned_by_peer` from disagreeing about who
    /// holds `addr` after a standalone conflicting pin.
    ///
    /// Returns the evicted address, if any -- the caller's cue to also
    /// release its ownership; `configure_peer` below does so in the SAME
    /// synchronous step, the standalone `pin` command does not (see its
    /// doc comment).
    ///
    /// Also publishes `addr` as `peer_id`'s route via
    /// `RoutingPublisher::set_configured_peer_addr` (see that trait
    /// method's own doc comment for why this must happen inside this same
    /// step), and the pin identity into the lock-free `RoutingSnapshot` --
    /// the answer `RegistryOwnerHandle::pin_is_current` revalidates
    /// against. Deliberately separate from `ConnectionPool`'s route and
    /// the ownership generation: neither answers the pin question.
    fn install_pin(&mut self, addr: SocketAddr, peer_id: PeerId) -> Option<SocketAddr> {
        // If a DIFFERENT peer is currently pinned at `addr` (a standalone
        // pin conflict, not this peer's own address move), its reverse
        // entry must be dropped too, mirroring what `RoutingSnapshot::
        // with_pin` already does for the published snapshot below.
        // Otherwise `operator_pinned[addr]` moves to `peer_id` here while
        // `pinned_by_peer[previous_occupant]` keeps claiming that peer is
        // still pinned at `addr` -- the two maps disagree from this point
        // on, wrongly refusing that peer's own ordinary route updates and
        // letting a later pin for it evict `addr` out from under whoever
        // holds it by then.
        let stale_occupant = self
            .operator_pinned
            .get(&addr)
            .filter(|occupant| **occupant != peer_id)
            .cloned();
        if let Some(occupant) = stale_occupant
            && self.pinned_by_peer.get(&occupant) == Some(&addr)
        {
            self.pinned_by_peer.remove(&occupant);
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
    /// `.connect()` route update performed HERE, inside the owner's own
    /// serialized command processing, instead of the caller writing
    /// `ConnectionPool` directly -- checked against `self.pinned_by_peer`
    /// (the owner's own reverse map), not a lock-free mirror a caller
    /// would otherwise have to read as a separate, non-atomic step. Runs
    /// as part of the owner's single-threaded processing, so the check
    /// and the write are one indivisible step. Declines when `peer_id` is
    /// pinned to a DIFFERENT address; reuses `set_configured_peer_addr`
    /// for the write, the same method `install_pin`/`migrate` use, adding
    /// only the conflict check in front.
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
    /// and, only if accepted, installs the operator pin in the SAME
    /// synchronous step, so by the time `install_pin` runs the claim is a
    /// fact this exact call already committed, not merely believed. If
    /// installing the pin evicts a DIFFERENT address this peer was
    /// previously pinned at, that address's ownership is released in the
    /// SAME step too, when `peer_id` still holds it.
    ///
    /// The evicted address's own `ReapReservation`, if held and NOT YET
    /// consumed, is also invalidated here (unconditional on `evicted_pin`
    /// alone, since it's the pin move, not the ownership fact, that makes
    /// a sweep's verdict stale) -- otherwise a caller already
    /// mid-destruction for the evicted address would carry on deleting a
    /// peer's actors and emitting tombstones for a peer the operator is
    /// actively reconfiguring elsewhere. Invalidating rather than refusing
    /// (as `migrate` does when either endpoint is reap-reserved) is
    /// correct here specifically because an operator reconfiguration is an
    /// explicit human action and a reap is only a heuristic sweep: this
    /// lets the operator win outright, discovered by the sweep's own
    /// `is_still_valid()` re-check before every irreversible step.
    ///
    /// If the evicted address's reservation is ALREADY consumed, though,
    /// there is no "invalidate" left to do -- the reap's destructive work
    /// is already authorized and may already be running. Checked BEFORE
    /// `claim`/`install_pin` run, so a refusal here needs no rollback: if
    /// `peer_id`'s current pin names a different, already-consumed
    /// address, the whole call is refused with the same
    /// `ClaimRejection::ReapInProgress` a direct claim against a reserved
    /// address gets, rather than proceeding to evict an address a reap is
    /// already committed to destructively acting on.
    ///
    /// `expected_generation` is validated FIRST, atomically, before the
    /// claim is even attempted -- see `configure_peer_generation`'s own
    /// doc comment for the caller-side race this closes.
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
        let generation = match expected_generation {
            // Exact match only -- not merely not-less-than. A caller can
            // only legitimately have learned a value FROM this same fence's
            // own prior response for this peer, so anything else is
            // refused the same way: a value smaller than current is a
            // stale retry a newer call already superseded; a value LARGER
            // than current was never actually stored (see this field's own
            // doc comment) and accepting it here would apply now while
            // leaving the stored generation behind, permanently valid for
            // this exact stale value to be replayed against future,
            // genuinely newer requests.
            Some(expected) if expected != current_generation => {
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
                self.configure_peer_generation.insert(peer_id.clone(), bumped);
                bumped
            }
        };
        // What `install_pin` below would evict for `peer_id`, checked
        // before any mutation: `pinned_by_peer` is untouched by anything
        // between this read and `install_pin`'s own (`claim` never writes
        // it), so this is exactly the address `install_pin`'s `evicted`
        // return will name.
        let would_evict = self
            .pinned_by_peer
            .get(&peer_id)
            .copied()
            .filter(|previous| *previous != addr);
        if let Some(previous) = would_evict
            && self
                .reap_reserved
                .get(&previous)
                .is_some_and(|entry| entry.consumed)
        {
            trace!(
                addr = %addr,
                peer_id = %peer_id,
                evicted_addr = %previous,
                "configure_peer refused: peer's previous pinned address is already committed \
                 to a reap's destructive work; evicting it now would race that work instead of \
                 stopping it"
            );
            return ConfigurePeerCommit {
                claim: ClaimCommit::Rejected(ClaimRejection::ReapInProgress),
                evicted_pin: None,
                evicted_release_seq: None,
                generation,
            };
        }
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
        if let Some(evicted_addr) = evicted_pin {
            // Cannot be the already-consumed case checked above:
            // `evicted_addr` is exactly `would_evict`, already confirmed
            // not consumed, and nothing since then (all synchronous, no
            // `.await`, within this one command) can have consumed it.
            let invalidated = self.invalidate_reap_reservation(evicted_addr);
            debug_assert!(
                invalidated || !self.reap_reserved.contains_key(&evicted_addr),
                "evicted_addr's reservation must not have become consumed between the \
                 pre-install_pin check above and this call"
            );
        }
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
    /// retract the routing publication. Callers are responsible for
    /// removing `owner` from `addr_ownership` before calling this.
    ///
    /// The receipt purge is unconditional on identity, not scoped to
    /// `owner` alone: `addr` is being fully vacated, so ANY receipt still
    /// keyed to it refers to a lifecycle generation that no longer exists.
    /// A caller that does NOT purge its own receipts first (a generic
    /// peer-table eviction going straight through `release`) would
    /// otherwise leave one behind to be silently updated to a NEW
    /// generation by the same identity's next reconnect, permanently
    /// stranding the address once that reconnect's own teardown finds an
    /// apparently still-live session that tore down long ago.
    fn retract_owner(&mut self, addr: SocketAddr, owner: Owner) -> CommitSeq {
        self.claim_generation.remove(&addr);
        self.claim_committed_at.remove(&addr);
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
        // This command mutates `addr_ownership`/`claim_committed_at` for
        // BOTH addresses directly, without going through `claim`'s own
        // `reap_reserved` check, so it's checked here instead, before
        // either address's state is read: a sweep holding a reservation
        // for either end relies on both staying fixed for its destructive
        // work's duration.
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
            // If a DIFFERENT peer was already pinned at `to` (a standalone
            // pin the migrating peer's own pin is about to displace), its
            // reverse entry must be dropped too -- the same
            // `operator_pinned`/`pinned_by_peer` desync `install_pin`
            // guards against for its own overwrite, one function over: see
            // its own doc comment for the full reasoning.
            let stale_destination_occupant = self
                .operator_pinned
                .get(&to)
                .filter(|occupant| **occupant != pinned_peer)
                .cloned();
            if let Some(occupant) = stale_destination_occupant
                && self.pinned_by_peer.get(&occupant) == Some(&to)
            {
                self.pinned_by_peer.remove(&occupant);
            }
            self.operator_pinned.insert(to, pinned_peer.clone());
            self.pinned_by_peer.insert(pinned_peer.clone(), to);
            // The pin's `ConnectionPool` route must move with it in this
            // SAME command, or the owner would protect `to` while
            // `get_required_peer_addr` kept reporting stale `from`. `from`
            // is passed as the evicted address for the same reason
            // `install_pin` does -- see `set_configured_peer_addr`'s doc
            // comment.
            if let Some(routing) = self.routing.upgrade() {
                routing.set_configured_peer_addr(to, &pinned_peer, Some(from));
            }
        }
        let commit_seq = self.advance();
        self.claim_generation.remove(&from);
        self.claim_generation.insert(to, commit_seq);
        // Connection-scoped receipts move with the ownership they back:
        // any receipt still keyed to `from` under an identity OTHER than
        // the one migrating no longer refers to a live generation and is
        // dropped (same as `retract_owner`'s purge); receipts for the
        // migrating identity are re-homed at `to` with the new generation,
        // and any receipt already at `to` for that identity is bumped to
        // match, since `to`'s own `claim_generation` just advanced
        // regardless. Left alone, either shape strands a later, correct
        // teardown that can never find a receipt at the current generation.
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
        // Carried over, never reset to "now": `migrate` is DNS-refresh
        // triggered, not direct evidence of a live connection -- resetting
        // this would let repeated DNS lookups for a peer that never
        // reconnects keep the freshness fence perpetually satisfied. `to`
        // may already have its own, strictly newer timestamp (the merge
        // case, or an independent reconnect at `to` before this ran), so
        // take the newer of the two rather than overwriting: a migration
        // must never make an address look LESS fresh than it is.
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
        if let Some(from_observed_at) = self.liveness_evidence_at.remove(&from) {
            self.liveness_evidence_at
                .entry(to)
                .and_modify(|to_observed_at| {
                    if from_observed_at > *to_observed_at {
                        *to_observed_at = from_observed_at;
                    }
                })
                .or_insert(from_observed_at);
        }
        let snapshot = self.snapshot.load_full();
        let mut snapshot = snapshot
            .with_owner(from, None)
            .with_owner(to, Some((owner.clone(), commit_seq)));
        // The pin's own publication moves with it, in this SAME snapshot
        // construction, for the same reason its route does above: a
        // caller revalidating `pin_is_current` must never observe a
        // window where the owner's pin bookkeeping already moved but the
        // published snapshot still shows `from`, or shows neither.
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

    /// A connection claim's liveness timestamp belongs to the transport
    /// event, not to the instant the owner happens to dequeue its command.
    /// Keeping the captured instant prevents mailbox delay from making old
    /// evidence look newer than a failure recorded while it waited.
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
                evidence_at,
            )
            .await;
        assert!(claim.is_accepted());

        let failure_at = std::time::Instant::now();
        assert!(matches!(
            owner.release_dead_peer(node, target, failure_at).await,
            DeadPeerReleaseOutcome::Released(_)
        ));
    }

    /// `release` -- the generic retraction path, not just
    /// `release_session`'s teardown -- used to leave any connection-scoped
    /// receipt behind, which a later reclaim by the same peer would
    /// silently carry forward as a ghost, permanently blocking the new
    /// session's own teardown. Asserts the ghost-revival consequence
    /// directly (a later teardown CAN release), not merely an empty map.
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
            owner.release(target, node.clone(), generation).await.is_some(),
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

    /// `migrate` must not leave `connection_scoped_claims` keyed to the
    /// now-unowned `from`. Asserts the ghost-revival consequence directly
    /// (a later teardown at the migrated address CAN release it).
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

    /// `claim` (the shared, plain-claim path gossip/discovery refreshes
    /// use, not only `claim_connection_scoped`) must also keep every
    /// still-live connection-scoped receipt in sync with the new
    /// generation, or a plain refresh between a connection's claim and its
    /// teardown strands that receipt permanently. Same ghost-revival shape
    /// as the `migrate` regression above, triggered by a plain claim.
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
        assert!(refreshed.is_accepted(), "the plain refresh must be accepted");

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

    /// `release_session` performs the ownership retraction itself, in the
    /// SAME synchronous owner command as the receipt removal, so there is
    /// no window for a racing plain claim to strand it (an earlier version
    /// returning candidates for a separate `release` call had exactly that
    /// window). Proves it by racing a plain, same-identity claim directly
    /// against the session's teardown, both submitted concurrently, and
    /// asserting release succeeds regardless of ordering.
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

        // Whichever order the owner serialized these in, this session's
        // own teardown must be reported as having found and released its
        // receipt -- never silently stranded by the race.
        assert_eq!(
            released.iter().map(|(addr, _)| *addr).collect::<Vec<_>>(),
            vec![a],
            "the session's teardown must have found and released its own receipt regardless \
             of ordering against the concurrent plain claim"
        );
    }

    /// A DNS-triggered `migrate` that carries an operator pin from `from`
    /// to `to` must publish `to` as the peer's `ConnectionPool` route in
    /// the SAME command, or the owner protects `to` while
    /// `get_required_peer_addr` keeps reporting stale `from`.
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
             set_configured_peer_addr's own doc comment)"
        );

        // The pin itself must have moved: `to` now refuses release, `from`
        // (no longer owned at all) trivially does not need it protected.
        let token = owner.ownership_token(&to).expect("still owned at `to`");
        assert!(
            owner.release(to, node, token.generation()).await.is_none(),
            "the migrated pin must still protect `to` from release"
        );
    }

    /// `migrate` permits `to` to already be owned by the SAME
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

        // Fixed strictly between `from`'s claim (now 80ms old) and `to`'s
        // claim below, so a mixup between the two is distinguishable.
        let evidence_before = std::time::Instant::now();

        // `to` already has its OWN, strictly newer direct claim.
        owner
            .claim_connection_scoped(to, claim_of(node.clone(), ClaimKind::Verified), to_session)
            .await;

        let source = current_source(&owner, from);
        assert!(
            owner.migrate(from, to, source, false).await.moved(),
            "a same-identity merge onto an already-owned destination must still succeed"
        );

        // `to`'s freshness must reflect ITS OWN newer evidence, not
        // `from`'s older timestamp.
        let committed_at = owner
            .claim_committed_at_for_test(to)
            .await
            .expect("`to` must have a claim_committed_at entry after the merge");
        assert!(
            committed_at > evidence_before,
            "migrate must not age a destination with newer direct evidence backwards to \
             the source's older timestamp -- a genuinely live address's claim_committed_at \
             must not regress"
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

    /// `migrate` mutates `addr_ownership`/`claim_committed_at` for BOTH
    /// addresses directly, without going through `claim`'s own
    /// `reap_reserved` check, so it must consult that table itself. Proves
    /// both ends: a reservation on the SOURCE refuses the move, and one on
    /// the DESTINATION refuses it too, both specifically with
    /// `MigrateOutcome::ReapInProgress`, ownership at both addresses
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

    /// A bare `bool` return from `reserve_for_reap` would let a hard
    /// `JoinHandle::abort()` of the holding task (not ordinary `select!`
    /// cancellation) drop the reservation with no side effect, leaking
    /// `addr` forever. Proves the RAII guard closes it: a task holding a
    /// granted `ReapReservation`, never explicitly released, is aborted
    /// mid-flight, and a claim for the same address afterward must still
    /// succeed.
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

        // Give the spawned task a chance to reach and pass the
        // reservation's own `.await` before aborting it.
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

    /// Proves reservations are exclusive: two reservation requests for the
    /// same address, submitted genuinely concurrently (the owner's own
    /// serialization decides which lands first), must produce exactly one
    /// grant, never two -- two grants would mean two guards sharing one
    /// entry, unsafe since releasing either drops protection the other
    /// still relies on.
    #[tokio::test]
    async fn concurrent_reap_reservations_for_the_same_address_are_mutually_exclusive() {
        let (owner, _publisher) = owner_handle();
        let contested_addr = addr(30_400);

        let owner_a = owner.clone();
        let owner_b = owner.clone();
        let task_a =
            tokio::spawn(async move {
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

    /// Direct, whitebox proof of `try_consume`'s one-shot semantics --
    /// see `ReapReservation`'s own doc comment for the full reasoning (why
    /// the owner's own serialized check-and-set closes the check-then-act
    /// gap a plain `is_still_valid()` load, repeated however many times,
    /// cannot). The FIRST call against a fresh, still-valid reservation must
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
            reservation.try_consume().await,
            "the first try_consume against a valid, unconsumed reservation must succeed"
        );
        assert!(
            !reservation.try_consume().await,
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
    /// reservation, each its own owner round trip), still exactly one
    /// winner -- the owner's own serialized command processing is what
    /// makes this exclusive now, not a lock-free CAS the callers share.
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
            tasks.push(tokio::spawn(async move { reservation.try_consume().await }));
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

    /// If a reservation's destructive work is already authorized
    /// (`try_consume` succeeded), a `configure_peer` call that would
    /// otherwise evict that same address from the peer's pin must not
    /// silently "invalidate" it: there is no live authorization left to
    /// revoke, so pretending to would let the reap's destructive work run
    /// concurrently with whatever `configure_peer` does next. Refused
    /// outright with the same rejection a direct claim against a reserved
    /// address gets, before any state mutates.
    #[tokio::test]
    async fn configure_peer_refuses_to_evict_an_already_consumed_reservation() {
        let (owner, _publisher) = owner_handle();
        let node = peer("configure-peer-vs-consumed-reservation");
        let addr_a = addr(30_460);
        let addr_b = addr(30_461);

        let outcome = owner.configure_peer(addr_a, node.clone(), None).await;
        assert!(outcome.claim.is_accepted(), "sanity: initial pin at A must succeed");
        assert_eq!(owner.pinned_addr_for(&node), Some(addr_a));

        // A dead-peer sweep reserves A exactly as `cleanup_dead_peers`'s
        // own selection phase would, using the identity it just observed.
        let ownership = owner.ownership_token(&addr_a);
        let pin_owner = owner.pin_owner(&addr_a);
        let reservation = owner
            .reserve_for_reap(
                addr_a,
                std::time::Instant::now(),
                ownership,
                pin_owner,
                Some(node.clone()),
            )
            .await
            .expect("a freshly, unconflictedly claimed address must be reservable");

        assert!(
            reservation.try_consume().await,
            "sanity: consumption of a fresh reservation must succeed"
        );

        // The operator tries to move the SAME peer to B, which would
        // ordinarily evict A from the pin.
        let outcome = owner.configure_peer(addr_b, node.clone(), None).await;

        assert_eq!(
            outcome.claim,
            ClaimCommit::Rejected(ClaimRejection::ReapInProgress),
            "configure_peer must refuse rather than evict an address whose reservation is \
             already consumed"
        );
        assert_eq!(
            owner.pinned_addr_for(&node),
            Some(addr_a),
            "a refused configure_peer call must leave the peer's pin completely untouched"
        );
        assert_eq!(
            owner.routes_to(&addr_b),
            None,
            "B must not have been claimed either -- the whole call was refused before any \
             mutation, not merely the pin-eviction step"
        );

        reservation.release().await;
    }

    /// Companion to the test above: when the evicted address's reservation
    /// is still live (not yet consumed), `configure_peer` must invalidate
    /// it and proceed normally -- the fix above narrows the refusal to
    /// exactly the already-consumed case, not every reservation.
    #[tokio::test]
    async fn configure_peer_invalidates_a_still_live_reservation_and_proceeds() {
        let (owner, _publisher) = owner_handle();
        let node = peer("configure-peer-vs-live-reservation");
        let addr_a = addr(30_462);
        let addr_b = addr(30_463);

        let outcome = owner.configure_peer(addr_a, node.clone(), None).await;
        assert!(outcome.claim.is_accepted(), "sanity: initial pin at A must succeed");

        let ownership = owner.ownership_token(&addr_a);
        let pin_owner = owner.pin_owner(&addr_a);
        let reservation = owner
            .reserve_for_reap(
                addr_a,
                std::time::Instant::now(),
                ownership,
                pin_owner,
                Some(node.clone()),
            )
            .await
            .expect("a freshly, unconflictedly claimed address must be reservable");

        let outcome = owner.configure_peer(addr_b, node.clone(), None).await;

        assert!(
            outcome.claim.is_accepted(),
            "configure_peer must succeed against a reservation that was never consumed"
        );
        assert_eq!(owner.pinned_addr_for(&node), Some(addr_b));
        assert!(
            !reservation.is_still_valid(),
            "the still-live reservation must have been genuinely invalidated, not left \
             dangling"
        );
    }

    /// Release is enqueued through the dedicated unbounded `release_tx`,
    /// never the bounded `tx` mailbox, precisely so it stays reliable when
    /// the ordinary mailbox is saturated (failing to RELEASE a reservation
    /// is unlike failing to TAKE one: every later claim is refused
    /// forever). Proves it under a genuinely, provably saturated bounded
    /// mailbox: fills `tx` to capacity with a synchronous `try_send` loop
    /// (no `.await`, so the owner task gets no chance to drain first),
    /// confirms it's full, then drops the guard WITHOUT calling
    /// `release()` -- the same path a hard task abort's cleanup runs. A
    /// later claim for the same address must still succeed.
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

        // The proof: a claim for the same address must succeed regardless
        // -- possible only if the guard's release never depended on
        // `tx`'s capacity at all.
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

    /// `configure_peer` releases an evicted pin's ownership in the SAME
    /// synchronous step as the eviction, not as a separate, later caller
    /// action a concurrent `migrate` could race ahead of.
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

    /// Proves `expected_generation` is validated INSIDE the atomic
    /// transaction, not on the caller's side before submitting: no racing
    /// needed, deterministically -- submits a stale retry (presenting
    /// generation 1) strictly AFTER a second, genuinely newer call has
    /// already committed generation 2 and moved the pin to `addr_y`, and
    /// asserts it's rejected outright with `addr_y` untouched.
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
        // generation (1), submitted strictly after generation 2 committed.
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

    /// `Some(expected)` used to be accepted whenever it was not LESS than
    /// the stored generation, but a value GREATER than current is never
    /// actually stored -- so an oversized retry stays "valid" forever,
    /// able to clobber every later, genuinely newer call. `Some(100)` at
    /// generation 1 must be refused outright (not silently applied while
    /// the stored generation stays at 1); a later normal call must still
    /// advance normally; and the SAME `Some(100)` retry, tried again after
    /// that, must still be refused rather than now looking "current" by
    /// coincidence.
    #[tokio::test]
    async fn configure_peer_rejects_an_expected_generation_larger_than_current() {
        let (owner, _publisher) = owner_handle();
        let node = peer("configure-peer-oversized-generation");
        let addr_a = addr(30_072);
        let addr_b = addr(30_073);

        let first = owner.configure_peer(addr_a, node.clone(), None).await;
        assert!(first.claim().is_accepted());
        assert_eq!(
            first.generation(),
            1,
            "sanity: a peer's first configure_peer call establishes generation 1"
        );

        let oversized = owner.configure_peer(addr_b, node.clone(), Some(100)).await;
        assert_eq!(
            *oversized.claim(),
            ClaimCommit::Rejected(ClaimRejection::SupersededByNewerConfiguration),
            "an expected_generation this fence never actually stored must be refused, not \
             silently applied"
        );
        assert_eq!(
            owner.routes_to(&addr_b),
            None,
            "the oversized retry must not have taken effect"
        );

        let second = owner.configure_peer(addr_a, node.clone(), None).await;
        assert!(second.claim().is_accepted());
        assert_eq!(
            second.generation(),
            2,
            "sanity: a normal call still advances to generation 2"
        );

        let repeat = owner.configure_peer(addr_b, node.clone(), Some(100)).await;
        assert_eq!(
            *repeat.claim(),
            ClaimCommit::Rejected(ClaimRejection::SupersededByNewerConfiguration),
            "the same oversized value must still be refused after a later call moved the real \
             generation on -- it was never valid to begin with, not merely stale now"
        );
        assert_eq!(
            owner.routes_to(&addr_b),
            None,
            "the oversized retry must still not have taken effect"
        );
    }

    /// `configure_peer`'s pin step only ever runs after this SAME call's
    /// own claim just committed, so a claim rejection must leave NEITHER a
    /// pin NOR a route behind for the rejected peer.
    #[tokio::test]
    async fn configure_peer_never_pins_when_the_claim_is_rejected() {
        let (owner, publisher) = owner_handle();
        let incumbent = peer("cfgpeer-incumbent");
        let challenger = peer("cfgpeer-challenger");
        let target = addr(30_062);

        let original = owner
            .claim(target, claim_of(incumbent.clone(), ClaimKind::Verified), false)
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
        // The strongest check: if the rejected claim had wrongly installed
        // a pin for `challenger`, `target` would now refuse release even
        // though `incumbent` genuinely still owns it.
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
    /// mapping. Otherwise a later pin for that peer could consult the stale
    /// reverse entry, evict the current owner's live pin, and leave the
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

        assert_eq!(owner.pin(addr_b, first.clone()).await, None);
        assert_eq!(owner.pin_owner(&addr_a), Some(second));
        assert_eq!(owner.pin_owner(&addr_b), Some(first.clone()));
        assert_eq!(owner.pinned_addr_for(&first), Some(addr_b));
    }

    /// If `pin` trusted a caller-supplied "previous address" instead of
    /// its own reverse map, two concurrent `configure_peer` calls for the
    /// SAME peer could each leave an address pinned, and since a pinned
    /// address can never be reclaimed by `release`, the loser would stay
    /// reserved forever. `pin` must evict whatever this peer is ACTUALLY
    /// pinned at, so exactly one of two concurrent calls reports an
    /// eviction and the other address stays reclaimable.
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

            // Whichever `pin` runs SECOND evicts the first one's address;
            // exactly one of the two must report an eviction.
            assert_ne!(
                evicted_a.is_some(),
                evicted_b.is_some(),
                "round {round}: exactly one concurrent pin command must report evicting \
                 the other's address"
            );

            // Whichever call reported an eviction ran LAST and won.
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

    /// A standalone `pin` for one peer at an address a DIFFERENT peer is
    /// already pinned at must not leave the owner's own reverse map
    /// (`pinned_by_peer`) disagreeing with the address-keyed map
    /// (`operator_pinned`) it just overwrote. The disagreement has two
    /// observable consequences: the displaced peer's own ordinary route
    /// updates get wrongly refused (the stale entry makes it still look
    /// pinned), and a later pin for that peer mistakes the old address for
    /// its own previous one and evicts whoever now legitimately holds it.
    #[tokio::test]
    async fn install_pin_drops_the_previous_occupants_reverse_entry_on_conflict() {
        let (owner, _publisher) = owner_handle();
        let p = peer("pin-conflict-p");
        let q = peer("pin-conflict-q");
        let a = addr(55_000);
        let elsewhere = addr(55_001);

        owner.pin(a, p.clone()).await;
        owner.pin(a, q.clone()).await;

        assert_eq!(
            owner.pinned_addr_for(&p),
            None,
            "P must no longer be reported as pinned anywhere once Q's pin displaced it at A"
        );
        assert_eq!(
            owner.pinned_addr_for(&q),
            Some(a),
            "Q must be A's current pin"
        );

        assert!(
            owner.set_ordinary_connect_route(p.clone(), elsewhere).await,
            "P's ordinary route update must succeed -- P is not actually pinned anywhere \
             after Q's conflicting pin displaced it"
        );

        let other = addr(55_002);
        owner.pin(other, p.clone()).await;
        assert_eq!(
            owner.pinned_addr_for(&q),
            Some(a),
            "Q's pin at A must survive P being pinned elsewhere -- A was never P's own \
             address to evict"
        );
    }

    /// Same `operator_pinned`/`pinned_by_peer` desync as `install_pin`'s
    /// own overwrite, one function over: `migrate` carrying a pin onto a
    /// DIFFERENT peer's already-pinned destination must drop that peer's
    /// reverse entry too, or its own ordinary route updates are wrongly
    /// refused afterward, and a later pin for it can evict the
    /// destination and clobber the migrated pin there.
    #[tokio::test]
    async fn migrate_drops_the_destinations_previous_occupants_reverse_entry_on_conflict() {
        let (owner, _publisher) = owner_handle();
        let p = peer("mig-pin-p");
        let q = peer("mig-pin-q");
        let from = addr(30_470);
        let to = addr(30_471);
        let elsewhere = addr(30_472);

        // Q is pinned at `to`, independent of ownership -- exactly what
        // the standalone `pin` API allows for an otherwise-unowned
        // address.
        owner.pin(to, q.clone()).await;

        // P owns and is pinned at `from`.
        let outcome = owner.configure_peer(from, p.clone(), None).await;
        assert!(
            outcome.claim.is_accepted(),
            "sanity: P's claim and pin at from must succeed"
        );
        let source = current_source(&owner, from);

        // `from` migrates to `to`, carrying P's pin onto Q's
        // already-pinned destination.
        let result = owner.migrate(from, to, source, false).await;
        assert!(
            result.moved(),
            "sanity: migrate must succeed onto an otherwise-unowned destination"
        );

        // The observable consequence: Q's own ordinary route update must
        // succeed -- Q is not actually pinned anywhere anymore.
        assert!(
            owner.set_ordinary_connect_route(q.clone(), elsewhere).await,
            "Q's ordinary route update must succeed -- Q is not actually pinned anywhere \
             after the migrated pin displaced it at the destination"
        );

        // And a later pin for Q must not evict the destination and
        // clobber P's migrated pin there.
        let other = addr(30_473);
        owner.pin(other, q.clone()).await;
        assert_eq!(
            owner.pinned_addr_for(&p),
            Some(to),
            "P's migrated pin must survive Q being pinned elsewhere -- the destination was \
             never Q's own address to evict"
        );
    }

    /// A DNS-triggered `migrate` (which carries a pin from its
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
