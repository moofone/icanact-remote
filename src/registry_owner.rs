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
}

const ROUTING_SNAPSHOT_SHARDS: usize = 64;

impl Default for RoutingSnapshot {
    fn default() -> Self {
        Self {
            owner_shards: std::array::from_fn(|_| Arc::new(HashMap::new())),
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
}

/// Why a claim did not take ownership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimRejection {
    /// The arbitration truth table refused the claim.
    Arbitration(RejectReason),
    /// The owner task is not reachable (shutting down, or its mailbox side
    /// was dropped). Fail closed: no address-keyed mutation may proceed on a
    /// decision that was never actually made.
    OwnerUnavailable,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
}

impl MigrateOutcome {
    /// Whether the caller is forbidden from re-keying its own address-keyed
    /// state onto the destination: either a competing identity owns the
    /// destination, or the source is no longer held by the identity the
    /// caller resolved.
    pub fn is_blocked(&self) -> bool {
        matches!(
            self,
            Self::TargetOwnedByOther | Self::TargetIsLocal | Self::SourceOwnerMismatch
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
            | Self::SourceOwnerMismatch => None,
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
    /// Record `addr` as `peer_id`'s configured/required dial target.
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
    fn set_configured_peer_addr(&self, addr: SocketAddr, peer_id: &PeerId);
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

    fn set_configured_peer_addr(&self, addr: SocketAddr, peer_id: &PeerId) {
        crate::connection_pool::ConnectionPool::set_configured_peer_addr(self, peer_id, addr);
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
}

impl ConfigurePeerCommit {
    /// The underlying claim decision.
    pub fn claim(&self) -> &ClaimCommit {
        &self.claim
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
        reply: oneshot::Sender<ClaimCommit>,
    },
    /// Atomically take every connection-scoped receipt `peer_id` holds for
    /// `session_source`, and report back only the addresses no OTHER live
    /// session still covers -- those are release candidates. Deciding
    /// "covered by another session" in this same step, against the map as it
    /// exists after this session's own entries are removed, is what makes a
    /// session exit racing a fresh claim for the same peer+address resolve
    /// consistently rather than stranding a receipt for the exiting session.
    ReleaseSession {
        peer_id: PeerId,
        session_source: SocketAddr,
        reply: oneshot::Sender<Vec<(SocketAddr, CommitSeq)>>,
    },
    /// Release everything a peer that has been dead longer than the
    /// dead-peer timeout still holds at `addr`: every connection-scoped
    /// receipt recorded for `peer_id` at `addr` under any session (a missed
    /// or still-in-flight teardown must not leave a ghost behind for a peer
    /// that is never coming back), and the address ownership itself if
    /// `peer_id` still holds it and `addr` is not operator-pinned.
    ///
    /// Refused entirely (no receipts touched, no ownership cleared) if
    /// `addr`'s ownership was committed or refreshed more recently than
    /// `dead_peer_timeout` ago -- checked against the owner's OWN
    /// `claim_committed_at` record, not any liveness snapshot the caller
    /// took. A caller's own dead-peer selection reads `gossip_state`, a
    /// separate synchronized domain that a reconnect updates only AFTER
    /// this owner already committed the fresh claim that proves the peer is
    /// alive; a snapshot taken from that domain can therefore look "dead"
    /// even after the owner itself already has fresher information. Making
    /// the owner re-derive its own answer from data it exclusively writes,
    /// rather than trust a value the caller computed earlier from a
    /// different domain, is what closes that gap regardless of which side
    /// of the caller's own snapshot the reconnect's commit landed on.
    ///
    /// Also refused, independently of the timeout, if `expected_generation`
    /// no longer matches `addr`'s current `claim_generation`. The timeout
    /// alone is a LEASE, not a fence: it only measures elapsed time since
    /// the last commit, so a stale selection that sits queued behind lock
    /// contention or earlier peers in the same sweep can still "become"
    /// valid purely by that queueing delay, even though a reconnect landed
    /// (and was itself proven live) while it waited. `expected_generation`
    /// is captured by the caller at selection time and must still hold at
    /// release time: any claim accepted for `addr` in between -- regardless
    /// of how much wall-clock time then passes before this command finally
    /// runs -- changes the generation and voids a decision made before it.
    ReleaseDeadPeer {
        peer_id: PeerId,
        addr: SocketAddr,
        dead_peer_timeout: std::time::Duration,
        /// The generation `addr` was at when the caller decided it looked
        /// dead. `None` if the caller observed no owner at all at that
        /// moment.
        expected_generation: Option<CommitSeq>,
        reply: oneshot::Sender<Option<CommitSeq>>,
    },
    /// Atomically install `peer_id`'s operator pin at `addr`, replacing
    /// whatever address (if any) the owner's own peer -> address reverse
    /// map currently shows this peer pinned at -- not merely the address a
    /// caller last observed in `ConnectionPool`, which can be stale by the
    /// time this command runs. See `PeerRegistryOwner::pin`.
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
        reply: oneshot::Sender<ConfigurePeerCommit>,
    },
    #[cfg(test)]
    InspectGeneration {
        addr: SocketAddr,
        reply: oneshot::Sender<Option<CommitSeq>>,
    },
}

/// Shared state behind every [`RegistryOwnerHandle`] clone.
struct OwnerShared {
    tx: mpsc::Sender<OwnerCommand>,
    snapshot: Arc<ArcSwap<RoutingSnapshot>>,
    /// Exactly-once start latch. The receiving half plus the publisher live
    /// here until the first command, at which point whichever caller wins the
    /// single-slot pop spawns the task. Registry construction is synchronous
    /// and may run outside a Tokio runtime, so the spawn cannot happen there.
    pending_start: ArrayQueue<StartKit>,
}

struct StartKit {
    rx: mpsc::Receiver<OwnerCommand>,
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
        let pending_start = ArrayQueue::new(1);
        // Cannot fail: the queue was just created with capacity 1.
        let _ = pending_start.push(StartKit { rx, routing });
        Self {
            shared: Arc::new(OwnerShared {
                tx,
                snapshot: Arc::new(ArcSwap::from_pointee(RoutingSnapshot::default())),
                pending_start,
            }),
        }
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
        self.ensure_started();
        let (reply, response) = oneshot::channel();
        let command = OwnerCommand::ClaimConnectionScoped {
            addr,
            claim,
            session_source,
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

    /// Atomically release every connection-scoped receipt `peer_id` holds for
    /// `session_source`, returning the addresses whose ownership no other
    /// live session still covers -- see `OwnerCommand::ReleaseSession`. An
    /// unreachable owner reports nothing to release: fail closed, the same as
    /// every other command here.
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

    /// Release everything `peer_id` still holds at `addr` -- every
    /// connection-scoped receipt recorded for it under any session, and the
    /// address ownership itself if `peer_id` still holds it and `addr` is not
    /// operator-pinned -- but ONLY if `addr`'s ownership hasn't been
    /// committed or refreshed within the last `dead_peer_timeout`, AND
    /// `expected_generation` still matches `addr`'s current generation;
    /// otherwise a no-op. See `OwnerCommand::ReleaseDeadPeer`.
    pub async fn release_dead_peer(
        &self,
        peer_id: PeerId,
        addr: SocketAddr,
        dead_peer_timeout: std::time::Duration,
        expected_generation: Option<CommitSeq>,
    ) -> Option<CommitSeq> {
        self.ensure_started();
        let (reply, response) = oneshot::channel();
        let command = OwnerCommand::ReleaseDeadPeer {
            peer_id,
            addr,
            dead_peer_timeout,
            expected_generation,
            reply,
        };
        if self.shared.tx.send(command).await.is_err() {
            return None;
        }
        response.await.unwrap_or(None)
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
    pub async fn pin(&self, addr: SocketAddr, peer_id: PeerId) -> Option<SocketAddr> {
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
    /// An owner-unavailable send failure reports a rejected claim with no
    /// eviction, the same fail-closed shape as every other command here.
    pub async fn configure_peer(&self, addr: SocketAddr, peer_id: PeerId) -> ConfigurePeerCommit {
        self.ensure_started();
        let (reply, response) = oneshot::channel();
        let command = OwnerCommand::ConfigurePeer {
            addr,
            peer_id,
            reply,
        };
        if self.shared.tx.send(command).await.is_err() {
            warn!(addr = %addr, "registry owner unavailable; failing configure_peer closed");
            return ConfigurePeerCommit {
                claim: ClaimCommit::Rejected(ClaimRejection::OwnerUnavailable),
                evicted_pin: None,
                evicted_release_seq: None,
            };
        }
        response.await.unwrap_or(ConfigurePeerCommit {
            claim: ClaimCommit::Rejected(ClaimRejection::OwnerUnavailable),
            evicted_pin: None,
            evicted_release_seq: None,
        })
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
        if let Some(StartKit { rx, routing }) = self.shared.pending_start.pop() {
            let owner = PeerRegistryOwner {
                addr_ownership: HashMap::new(),
                claim_generation: HashMap::new(),
                claim_committed_at: HashMap::new(),
                connection_scoped_claims: HashMap::new(),
                operator_pinned: HashMap::new(),
                pinned_by_peer: HashMap::new(),
                snapshot: Arc::clone(&self.shared.snapshot),
                routing,
                commit_seq: 0,
            };
            tokio::spawn(owner.run(rx));
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
    claim_committed_at: HashMap<SocketAddr, std::time::Instant>,
    /// Connection-scoped ownership receipts: which live authenticated
    /// sessions currently back a peer's claim on an address, and at what
    /// owner generation. Keyed by `(peer, session_source, addr)` --
    /// `session_source` is this exact physical connection's own
    /// discriminator (unique per connection; see `ReadContext::session_source`),
    /// so a stale session's teardown can only ever remove its own entry.
    ///
    /// Lives here, alongside `claim_generation`, rather than in a
    /// separately-synchronized map the way PR #178 first shipped it: every
    /// mutation below happens from `&mut self` in the same synchronous
    /// command as the ownership commit or release it corresponds to, so a
    /// receipt can never be observed (or left behind) at a generation the
    /// owner authority does not simultaneously agree is current.
    connection_scoped_claims: HashMap<(PeerId, SocketAddr, SocketAddr), CommitSeq>,
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
    /// Run until every sender is dropped.
    async fn run(mut self, mut rx: mpsc::Receiver<OwnerCommand>) {
        while let Some(command) = rx.recv().await {
            self.handle(command);
            // Drain whatever else is already queued without re-suspending.
            // Publication still happens per command inside `handle` rather
            // than once per batch: a reply must never be observable before
            // the snapshot that justifies it.
            while let Ok(command) = rx.try_recv() {
                self.handle(command);
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
                reply,
            } => {
                let commit = self.claim_connection_scoped(addr, claim, session_source);
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
                dead_peer_timeout,
                expected_generation,
                reply,
            } => {
                let released =
                    self.release_dead_peer(&peer_id, addr, dead_peer_timeout, expected_generation);
                let _ = reply.send(released);
            }
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
                reply,
            } => {
                let commit = self.configure_peer(addr, peer_id);
                let _ = reply.send(commit);
            }
            #[cfg(test)]
            OwnerCommand::InspectGeneration { addr, reply } => {
                let _ = reply.send(self.claim_generation.get(&addr).copied());
            }
        }
    }

    fn claim(&mut self, addr: SocketAddr, claim: Claim, is_local_addr: bool) -> ClaimCommit {
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
    /// same `&mut self` call as the commit itself, no second command can ever
    /// be handled in between, so
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
    fn claim_connection_scoped(
        &mut self,
        addr: SocketAddr,
        claim: Claim,
        session_source: SocketAddr,
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
            self.claim_committed_at
                .insert(addr, std::time::Instant::now());
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
            // A same-peer reconnect refreshes the owner generation. Transfer
            // that current generation to every still-live session receipt for
            // this address before adding the new session. If the newer
            // session closes first, the surviving older session must still
            // hold a receipt that can release the generation once it is the
            // last owner; leaving its old generation behind would leak the
            // route exactly as PR #178's unsynchronized version could.
            for (key, generation) in self.connection_scoped_claims.iter_mut() {
                if key.0 == peer_id && key.2 == addr {
                    *generation = commit_seq;
                }
            }
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

    /// Atomically release every connection-scoped receipt `peer_id` holds for
    /// `session_source`. An address is only reported back as a release
    /// candidate when NO other live session still holds a receipt for the
    /// same peer+address at this exact moment -- checked against the map
    /// after this session's own entries are already removed, in the same
    /// synchronous step, so a concurrent claim or a concurrent second
    /// session's own exit can never be interleaved into the middle of this
    /// decision.
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
        own_entries
            .into_iter()
            .filter(|(addr, _)| {
                !self
                    .connection_scoped_claims
                    .keys()
                    .any(|key| &key.0 == peer_id && key.2 == *addr)
            })
            .collect()
    }

    /// Release everything `peer_id` still holds at `addr`: every
    /// connection-scoped receipt recorded for it there under any session
    /// (ghost cleanup for a teardown that never ran), and the ownership
    /// record itself if `peer_id` is still its owner and `addr` is not
    /// operator-pinned.
    ///
    /// `dead_peer_timeout` is the stale-decision fence, checked against
    /// `claim_committed_at` -- owner-internal, exclusively owner-written
    /// state -- rather than against anything the caller observed. A
    /// dead-peer sweep's own liveness read comes from `GossipState`, which a
    /// reconnect updates only AFTER this owner already committed the fresh
    /// claim proving the peer alive; that ordering means a sweep's liveness
    /// snapshot can be stale relative to the owner's OWN state regardless of
    /// whether the reconnect's claim landed before, during, or after the
    /// sweep took its snapshot. Re-deriving the answer here, from data nothing
    /// but this task ever writes, is what closes the gap in every one of
    /// those orderings rather than just the ones a caller-supplied token
    /// happens to fence.
    ///
    /// `expected_generation` is a SECOND, independent fence, checked first:
    /// the timeout alone only asks "is there recent evidence right now", not
    /// "is the decision this command carries still the one the caller
    /// actually made". A sweep that selects this exact (peer, addr) pair can
    /// take arbitrarily long to actually reach this command -- lock
    /// contention, or simply earlier peers in the same sweep each doing
    /// their own owner round trip first -- and a reconnect's fresh claim can
    /// land at any point during that delay. Once enough time then passes
    /// AFTER that reconnect, the timeout fence alone would stop protecting
    /// it: elapsed time since the (new, genuinely fresh) commit would exceed
    /// `dead_peer_timeout` even though the address has been continuously,
    /// correctly owned the whole time. Requiring the generation to still
    /// match what the caller observed BEFORE it made this decision closes
    /// that gap: any claim accepted for `addr` since then -- regardless of
    /// how long this command then sits queued -- changes the generation and
    /// voids a decision made before it, independent of wall-clock time.
    fn release_dead_peer(
        &mut self,
        peer_id: &PeerId,
        addr: SocketAddr,
        dead_peer_timeout: std::time::Duration,
        expected_generation: Option<CommitSeq>,
    ) -> Option<CommitSeq> {
        if self.claim_generation.get(&addr).copied() != expected_generation {
            trace!(
                addr = %addr,
                peer = %peer_id,
                "dead-peer release refused: ownership generation advanced past this sweep's selection"
            );
            return None;
        }
        if self
            .claim_committed_at
            .get(&addr)
            .is_some_and(|committed_at| committed_at.elapsed() < dead_peer_timeout)
        {
            trace!(
                addr = %addr,
                peer = %peer_id,
                "dead-peer release refused: address claimed more recently than the dead-peer timeout"
            );
            return None;
        }
        self.connection_scoped_claims
            .retain(|key, _| !(&key.0 == peer_id && key.2 == addr));
        if self.operator_pinned.contains_key(&addr) {
            return None;
        }
        let still_owned = self
            .addr_ownership
            .get(&addr)
            .is_some_and(|owner| owner.node_id == *peer_id);
        if !still_owned {
            return None;
        }
        let owner = self.addr_ownership.remove(&addr)?;
        Some(self.retract_owner(addr, owner))
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
    fn install_pin(&mut self, addr: SocketAddr, peer_id: PeerId) -> Option<SocketAddr> {
        let previous = self.pinned_by_peer.insert(peer_id.clone(), addr);
        let evicted = previous.filter(|previous_addr| *previous_addr != addr);
        if let Some(evicted_addr) = evicted {
            self.operator_pinned.remove(&evicted_addr);
        }
        if let Some(routing) = self.routing.upgrade() {
            routing.set_configured_peer_addr(addr, &peer_id);
        }
        self.operator_pinned.insert(addr, peer_id);
        evicted
    }

    /// `OwnerCommand::Pin`'s handler: see `install_pin`. Kept as its own,
    /// narrower command (never called from `GossipRegistry::configure_peer`,
    /// which uses the atomic `configure_peer` below instead) for the
    /// reverse-map invariant it guarantees on its own -- see
    /// `RegistryOwnerHandle::pin`'s doc comment for why a caller needing an
    /// ownership-backed pin must use `configure_peer` instead.
    fn pin(&mut self, addr: SocketAddr, peer_id: PeerId) -> Option<SocketAddr> {
        self.install_pin(addr, peer_id)
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
    fn configure_peer(&mut self, addr: SocketAddr, peer_id: PeerId) -> ConfigurePeerCommit {
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
        }
    }

    /// Shared tail of every path that drops a recorded owner: clear its
    /// generation, advance the commit order, publish the vacancy, and
    /// retract the routing publication. Callers are responsible for removing
    /// `owner` from `addr_ownership` (and for whatever ownership-match check
    /// justified doing so) before calling this.
    fn retract_owner(&mut self, addr: SocketAddr, owner: Owner) -> CommitSeq {
        self.claim_generation.remove(&addr);
        self.claim_committed_at.remove(&addr);
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
        if let Some(pinned_peer) = self.operator_pinned.remove(&from) {
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
            if let Some(routing) = self.routing.upgrade() {
                routing.set_configured_peer_addr(to, &pinned_peer);
            }
        }
        let commit_seq = self.advance();
        self.claim_generation.remove(&from);
        self.claim_generation.insert(to, commit_seq);
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
        let snapshot = self.snapshot.load_full();
        let snapshot = snapshot
            .with_owner(from, None)
            .with_owner(to, Some((owner.clone(), commit_seq)));
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
        configured_routes: Mutex<Vec<(SocketAddr, PeerId)>>,
    }

    impl RecordingPublisher {
        fn events(&self) -> Vec<(SocketAddr, Option<PeerId>)> {
            self.events.lock().expect("publisher mutex").clone()
        }

        fn configured_routes(&self) -> Vec<(SocketAddr, PeerId)> {
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

        fn set_configured_peer_addr(&self, addr: SocketAddr, peer_id: &PeerId) {
            self.configured_routes
                .lock()
                .expect("publisher mutex")
                .push((addr, peer_id.clone()));
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
            pool.addr_to_peer_id.read_sync(&addr, |_, owner| owner.clone()),
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

    /// P1 follow-on regression: `dead_peer_timeout` alone is a LEASE, not a
    /// fence -- it only measures elapsed time since the address's last
    /// commit, so a stale dead-peer decision that sits queued long enough
    /// (lock contention, or earlier peers in the same sweep each doing
    /// their own owner round trip first) can "become" valid purely by that
    /// delay, even though a reconnect landed -- and was itself proven live
    /// -- while it waited. This reproduces exactly that: a generation is
    /// captured as a sweep's selection would, a reconnect then commits a
    /// fresh claim, and enough wall time passes that the timeout fence
    /// alone (measured from the RECONNECT's own genuinely fresh commit)
    /// would no longer protect it. The release must still be refused,
    /// because the generation captured before the reconnect no longer
    /// matches -- a decision elapsed wall time alone must never validate.
    #[tokio::test]
    async fn release_dead_peer_is_fenced_against_a_generation_captured_before_a_late_reconnect()
     {
        let (owner, _publisher) = owner_handle();
        let node = peer("late-reconnect-generation-fence");
        let target = addr(30_040);
        let old_session = addr(30_041);
        let new_session = addr(30_042);
        let dead_peer_timeout = Duration::from_millis(30);

        let original = owner
            .claim_connection_scoped(
                target,
                claim_of(node.clone(), ClaimKind::Verified),
                old_session,
            )
            .await;
        // What a dead-peer sweep would have captured as this address's
        // generation at selection time -- BEFORE the reconnect below, e.g.
        // because `old_session` had already gone quiet and `gossip_state`
        // looked dead at that exact moment.
        let observed_generation = original.commit_seq();

        // The reconnect: a fresh, genuinely live claim for the SAME
        // identity, committed strictly AFTER the sweep's selection but well
        // before its (delayed) release actually runs.
        owner
            .claim_connection_scoped(
                target,
                claim_of(node.clone(), ClaimKind::Verified),
                new_session,
            )
            .await;

        // Enough wall time now passes that the timeout fence ALONE,
        // measured from the reconnect's own commit, would no longer
        // protect it.
        tokio::time::sleep(dead_peer_timeout + Duration::from_millis(10)).await;

        let released = owner
            .release_dead_peer(node.clone(), target, dead_peer_timeout, observed_generation)
            .await;

        assert_eq!(
            released, None,
            "a dead-peer release must refuse when the generation has moved on since \
             selection, regardless of how much wall-clock time has since elapsed"
        );
        assert_eq!(
            owner.routes_to(&target),
            Some(node),
            "the reconnect's ownership must survive a stale sweep's delayed, \
             generation-mismatched release"
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
            Some(&(to, node.clone())),
            "migrate must publish the carried pin's new address as the \
             ConnectionPool configured/required route, in the same command \
             the pin itself moves in"
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
        // `from`'s much older one: a dead_peer_timeout comfortably longer
        // than `to`'s real age (just now) but shorter than `from`'s (80ms+)
        // must still refuse to release it. The generation passed here
        // matches `to`'s current one (this test is exercising the
        // freshness-timeout fence, not the separate generation fence).
        let to_generation = owner.ownership_token(&to).map(|token| token.generation());
        assert!(
            owner
                .release_dead_peer(node, to, Duration::from_millis(60), to_generation)
                .await
                .is_none(),
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

        let first = owner.configure_peer(addr_p, node.clone()).await;
        assert!(first.claim().is_accepted());
        assert_eq!(first.evicted_pin(), None);
        assert_eq!(first.evicted_release_seq(), None);

        let second = owner.configure_peer(addr_y, node.clone()).await;
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
            .claim(target, claim_of(incumbent.clone(), ClaimKind::Verified), false)
            .await;
        let original_generation = original.commit_seq().expect("original claim commits");
        let events_before = publisher.events();
        let configured_routes_before = publisher.configured_routes();

        let commit = owner.configure_peer(target, challenger.clone()).await;

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
