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
/// Republished in full on every committed mutation. The table is sized by the
/// peer count (tens to low thousands) and mutations happen at connection /
/// full-sync cadence, so rebuilding it is far cheaper than the per-read
/// synchronization a shared mutable map would impose on the many readers.
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
        reply: oneshot::Sender<bool>,
    },
    Migrate {
        from: SocketAddr,
        to: SocketAddr,
        reply: oneshot::Sender<bool>,
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
    /// still owns it. Returns whether an entry was actually removed.
    pub async fn release(&self, addr: SocketAddr, expected: Option<PeerId>) -> bool {
        self.ensure_started();
        let (reply, response) = oneshot::channel();
        let command = OwnerCommand::Release {
            addr,
            expected,
            reply,
        };
        if self.shared.tx.send(command).await.is_err() {
            return false;
        }
        response.await.unwrap_or(false)
    }

    /// Move ownership of `from` onto `to` (address re-resolution). No-op when
    /// `from` is unowned, or when `to` is already owned by a different
    /// identity.
    pub async fn migrate(&self, from: SocketAddr, to: SocketAddr) -> bool {
        self.ensure_started();
        let (reply, response) = oneshot::channel();
        let command = OwnerCommand::Migrate { from, to, reply };
        if self.shared.tx.send(command).await.is_err() {
            return false;
        }
        response.await.unwrap_or(false)
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
            OwnerCommand::Migrate { from, to, reply } => {
                let migrated = self.migrate(from, to);
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
                    .filter(|owner| owner.node_id != claim.node_id)
                    .map(|owner| owner.node_id);
                let node_id = claim.node_id;
                self.addr_ownership.insert(
                    addr,
                    Owner {
                        node_id: node_id.clone(),
                        kind,
                    },
                );
                self.publish();
                if let Some(routing) = self.routing.upgrade() {
                    routing.publish_owner(addr, &node_id);
                }
                ClaimCommit::Accepted { kind, displaced }
            }
        }
    }

    fn release(&mut self, addr: SocketAddr, expected: Option<&PeerId>) -> bool {
        let matches_expectation = self
            .addr_ownership
            .get(&addr)
            .is_some_and(|owner| expected.is_none_or(|expected| *expected == owner.node_id));
        if !matches_expectation {
            return false;
        }
        let Some(owner) = self.addr_ownership.remove(&addr) else {
            return false;
        };
        self.publish();
        if let Some(routing) = self.routing.upgrade() {
            routing.retract_owner(addr, &owner.node_id);
        }
        true
    }

    fn migrate(&mut self, from: SocketAddr, to: SocketAddr) -> bool {
        let Some(owner) = self.addr_ownership.get(&from).cloned() else {
            return false;
        };
        if self
            .addr_ownership
            .get(&to)
            .is_some_and(|existing| existing.node_id != owner.node_id)
        {
            return false;
        }
        self.addr_ownership.remove(&from);
        self.addr_ownership.insert(to, owner.clone());
        self.publish();
        if let Some(routing) = self.routing.upgrade() {
            routing.retract_owner(from, &owner.node_id);
            routing.publish_owner(to, &owner.node_id);
        }
        true
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
            }
        );
        assert_eq!(
            owner.owner_of(&target).map(|owner| owner.kind),
            Some(ClaimKind::Verified)
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
            !owner.release(target, Some(other)).await,
            "a non-owner must not be able to release the address"
        );
        assert_eq!(owner.routes_to(&target), Some(holder.clone()));

        assert!(owner.release(target, Some(holder)).await);
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
        assert!(owner.migrate(from, to).await);
        assert_eq!(owner.owner_of(&from), None);
        assert_eq!(
            owner.owner_of(&to),
            Some(Owner {
                node_id: node,
                kind: ClaimKind::Verified,
            })
        );

        // A migration onto an address a different identity owns is refused.
        let contested = addr(30_011);
        owner
            .claim(
                contested,
                claim_of(peer("holder"), ClaimKind::Verified),
                false,
            )
            .await;
        assert!(!owner.migrate(to, contested).await);
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
}
