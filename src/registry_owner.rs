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

    fn release(
        &mut self,
        addr: SocketAddr,
        expected_owner: &PeerId,
        expected_generation: CommitSeq,
    ) -> Option<CommitSeq> {
        let matches_expectation = self
            .addr_ownership
            .get(&addr)
            .is_some_and(|owner| owner.node_id == *expected_owner)
            && self.claim_generation.get(&addr) == Some(&expected_generation);
        if !matches_expectation {
            return None;
        }
        let owner = self.addr_ownership.remove(&addr)?;
        self.claim_generation.remove(&addr);
        let commit_seq = self.advance();
        self.publish_owner_snapshot(addr, None);
        if let Some(routing) = self.routing.upgrade() {
            routing.retract_owner(addr, &owner.node_id);
        }
        Some(commit_seq)
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
        let commit_seq = self.advance();
        self.claim_generation.remove(&from);
        self.claim_generation.insert(to, commit_seq);
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
    }

    impl RecordingPublisher {
        fn events(&self) -> Vec<(SocketAddr, Option<PeerId>)> {
            self.events.lock().expect("publisher mutex").clone()
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
}
