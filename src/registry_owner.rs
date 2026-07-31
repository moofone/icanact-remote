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
//! state, so no generation tokens, CAS retry loop, or cross-lock ordering
//! rule is needed.
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

/// Immutable, lock-free-readable publication of the address ownership table.
///
/// Republished in full whenever an owner or resolved claim kind changes.
/// Unchanged same-owner refreshes still advance their projection commit fence
/// but reuse this snapshot, avoiding O(peer-count) copying on routine gossip.
/// Reads remain lock-free and never enter the owner mailbox.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RoutingSnapshot {
    owners: HashMap<SocketAddr, Owner>,
}

impl RoutingSnapshot {
    /// The recorded owner of `addr`, if any.
    pub fn owner(&self, addr: &SocketAddr) -> Option<&Owner> {
        self.owners.get(addr)
    }

    /// The identity `addr` currently routes to, if any.
    pub fn peer_id(&self, addr: &SocketAddr) -> Option<&PeerId> {
        self.owners.get(addr).map(|owner| &owner.node_id)
    }

    /// Number of addresses with a recorded owner.
    pub fn len(&self) -> usize {
        self.owners.len()
    }

    /// Whether no address has a recorded owner.
    pub fn is_empty(&self) -> bool {
        self.owners.is_empty()
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
        matches!(self, Self::TargetOwnedByOther | Self::SourceOwnerMismatch)
    }

    /// Whether ownership actually moved.
    pub fn moved(&self) -> bool {
        matches!(self, Self::Migrated { .. })
    }

    /// The move's position in the commit order, if it moved.
    pub fn commit_seq(&self) -> Option<CommitSeq> {
        match self {
            Self::Migrated { commit_seq } => Some(*commit_seq),
            Self::SourceUnowned | Self::TargetOwnedByOther | Self::SourceOwnerMismatch => None,
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
        let still_ours = self
            .addr_to_peer_id
            .read_sync(&addr, |_, current| current == peer_id)
            .unwrap_or(false);
        if still_ours {
            let _ = self.addr_to_peer_id.remove_sync(&addr);
        }
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
        /// When set, only release if this identity still owns `addr`, so a
        /// late release from a displaced owner cannot evict its successor.
        expected: Option<PeerId>,
        reply: oneshot::Sender<Option<CommitSeq>>,
    },
    Migrate {
        from: SocketAddr,
        to: SocketAddr,
        /// When set, only move if this identity still owns `from`, so a
        /// caller that resolved the moving identity before submitting the
        /// command cannot re-key a different identity that displaced it in
        /// the meantime.
        expected_source: Option<PeerId>,
        reply: oneshot::Sender<MigrateOutcome>,
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

    /// Drop the recorded ownership of `addr`, optionally only when `expected`
    /// still owns it.
    ///
    /// Returns the release's position in the commit order when an entry was
    /// actually removed, so the caller can fence its own address-keyed state
    /// at that position: a claim that committed BEFORE the release must not
    /// be able to project peer or connection state back onto an address the
    /// release has since vacated.
    pub async fn release(&self, addr: SocketAddr, expected: Option<PeerId>) -> Option<CommitSeq> {
        self.ensure_started();
        let (reply, response) = oneshot::channel();
        let command = OwnerCommand::Release {
            addr,
            expected,
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
    /// `expected_source` pins the identity the caller believes owns `from`.
    /// Any caller that resolved that identity separately — before submitting
    /// this command — must pass it, because the resolution and the move are
    /// not one atomic step and a competing claimant can displace the source's
    /// owner in between; the move is then refused rather than silently
    /// carrying a different identity onto the destination. `None` waives the
    /// check and is correct only when the caller re-keys no identity-scoped
    /// state, e.g. a seed configured by host name that has never been
    /// claimed.
    pub async fn migrate(
        &self,
        from: SocketAddr,
        to: SocketAddr,
        expected_source: Option<PeerId>,
    ) -> MigrateOutcome {
        self.ensure_started();
        let (reply, response) = oneshot::channel();
        let command = OwnerCommand::Migrate {
            from,
            to,
            expected_source,
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
}

/// The single writer. Owns `addr_ownership` outright — no `Arc`, no mutex, no
/// interior mutability — so `&mut self` alone proves exclusivity.
struct PeerRegistryOwner {
    addr_ownership: HashMap<SocketAddr, Owner>,
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
                expected,
                reply,
            } => {
                let released = self.release(addr, expected.as_ref());
                let _ = reply.send(released);
            }
            OwnerCommand::Migrate {
                from,
                to,
                expected_source,
                reply,
            } => {
                let migrated = self.migrate(from, to, expected_source.as_ref());
                let _ = reply.send(migrated);
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
                    self.addr_ownership.insert(addr, next_owner);
                }
                let commit_seq = self.advance();
                if ownership_changed {
                    self.publish();
                    if let Some(routing) = self.routing.upgrade() {
                        routing.publish_owner(addr, &node_id);
                    }
                }
                ClaimCommit::Accepted {
                    kind,
                    displaced,
                    commit_seq,
                }
            }
        }
    }

    fn release(&mut self, addr: SocketAddr, expected: Option<&PeerId>) -> Option<CommitSeq> {
        let matches_expectation = self
            .addr_ownership
            .get(&addr)
            .is_some_and(|owner| expected.is_none_or(|expected| *expected == owner.node_id));
        if !matches_expectation {
            return None;
        }
        let owner = self.addr_ownership.remove(&addr)?;
        let commit_seq = self.advance();
        self.publish();
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
    /// `expected_source`, when set, additionally pins the identity the caller
    /// resolved as the source's owner. It is checked here, inside the
    /// serialized command, so the caller's separately-resolved identity and
    /// the move are decided against the same ownership state.
    fn migrate(
        &mut self,
        from: SocketAddr,
        to: SocketAddr,
        expected_source: Option<&PeerId>,
    ) -> MigrateOutcome {
        let source = self.addr_ownership.get(&from).cloned();
        if let Some(expected) = expected_source
            && source
                .as_ref()
                .is_none_or(|owner| owner.node_id != *expected)
        {
            // Includes the source having become unowned: the caller resolved
            // an identity to carry across, and it is no longer there to carry.
            trace!(
                from = %from,
                to = %to,
                expected = %expected,
                "address migration refused: the source's owner is not the expected identity"
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
        self.publish();
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

    /// Republish the immutable snapshot readers load.
    fn publish(&self) {
        self.snapshot.store(Arc::new(RoutingSnapshot {
            owners: self.addr_ownership.clone(),
        }));
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

    /// Provisional first, then a genuinely Verified claim: the verified
    /// claimant displaces the provisional squatter and the displacement is
    /// reported so the caller can rekey identity-scoped state.
    #[tokio::test]
    async fn provisional_then_verified_displaces_and_reports_owner_change() {
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
            ClaimCommit::Accepted {
                kind: ClaimKind::Provisional,
                displaced: None,
                commit_seq: 1,
            }
        );

        let second = owner
            .claim(target, claim_of(real.clone(), ClaimKind::Verified), false)
            .await;
        assert_eq!(
            second,
            ClaimCommit::Accepted {
                kind: ClaimKind::Verified,
                displaced: Some(squatter.clone()),
                commit_seq: 2,
            }
        );
        assert!(
            second.owner_changed(),
            "owner-change rekey must be signalled"
        );
        assert_eq!(owner.routes_to(&target), Some(real.clone()));
        assert_eq!(
            publisher.events(),
            vec![(target, Some(squatter)), (target, Some(real))],
            "routing must be republished exactly once per accepted claim"
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
                commit_seq: 2,
            }
        );
        assert_eq!(
            owner.owner_of(&target).map(|owner| owner.kind),
            Some(ClaimKind::Verified)
        );
    }

    /// Routine FullSync refreshes advance the projection fence but do not
    /// rebuild the whole immutable ownership table when neither identity nor
    /// resolved claim kind changed.
    #[tokio::test]
    async fn unchanged_same_owner_refresh_reuses_snapshot_and_route_publication() {
        let (owner, publisher) = owner_handle();
        let node = peer("unchanged-refresh");
        let target = addr(30_023);

        owner
            .claim(target, claim_of(node.clone(), ClaimKind::Verified), false)
            .await;
        let snapshot = owner.snapshot();
        let published = publisher.events();

        let refresh = owner
            .claim(target, claim_of(node, ClaimKind::Provisional), false)
            .await;

        assert_eq!(
            refresh.commit_seq(),
            Some(2),
            "the projection fence advances"
        );
        assert!(
            Arc::ptr_eq(&snapshot, &owner.snapshot()),
            "an unchanged refresh must not clone and republish the full ownership map"
        );
        assert_eq!(
            publisher.events(),
            published,
            "an unchanged refresh must not republish an identical address route"
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

        owner
            .claim(target, claim_of(holder.clone(), ClaimKind::Verified), false)
            .await;
        assert!(
            owner.release(target, Some(other)).await.is_none(),
            "a non-owner must not be able to release the address"
        );
        assert_eq!(owner.routes_to(&target), Some(holder.clone()));

        assert!(owner.release(target, Some(holder)).await.is_some());
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
        assert!(
            owner.migrate(from, to, None).await.moved(),
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
            owner.migrate(to, contested, None).await,
            MigrateOutcome::TargetOwnedByOther
        );
        // An UNOWNED source moving onto an address someone else owns is
        // blocked too: "nothing to move" would invite the caller to re-key
        // its own state onto the other identity's address.
        assert_eq!(
            owner.migrate(addr(30_012), contested, None).await,
            MigrateOutcome::TargetOwnedByOther,
            "an unowned source must not be allowed onto another identity's address"
        );
        // With a free destination, an unclaimed source is still reported as
        // "nothing to move" — the non-blocking outcome.
        assert_eq!(
            owner.migrate(addr(30_012), addr(30_013), None).await,
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
            owner.migrate(unowned, held, None).await,
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

        // The caller's resolution: `expected` owns `from`.
        owner
            .claim(
                from,
                claim_of(expected.clone(), ClaimKind::Provisional),
                false,
            )
            .await;
        // ... and is displaced before the migrate command is processed.
        assert!(
            owner
                .claim(from, claim_of(usurper.clone(), ClaimKind::Verified), false)
                .await
                .is_accepted()
        );
        let events_before = publisher.events();

        assert_eq!(
            owner.migrate(from, to, Some(expected.clone())).await,
            MigrateOutcome::SourceOwnerMismatch
        );
        assert!(
            owner
                .migrate(from, to, Some(expected.clone()))
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
            owner.migrate(vacant, to, Some(expected)).await,
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
                claim_of(original.clone(), ClaimKind::Provisional),
                false,
            )
            .await;
        assert!(
            owner
                .migrate(old_addr, new_addr, Some(original.clone()))
                .await
                .moved()
        );

        // A newer claimant takes `new_addr` before the restore runs.
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
                .migrate(new_addr, old_addr, Some(original.clone()))
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
                claim_of(original.clone(), ClaimKind::Provisional),
                false,
            )
            .await;
        assert!(
            owner
                .migrate(other_old, other_new, Some(original.clone()))
                .await
                .moved()
        );
        assert!(
            owner
                .migrate(other_new, other_old, Some(original.clone()))
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
            .claim(from, claim_of(node.clone(), ClaimKind::Provisional), false)
            .await;

        assert!(owner.migrate(from, to, Some(node.clone())).await.moved());
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
