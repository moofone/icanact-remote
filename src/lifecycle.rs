use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, RwLock};

use crate::PeerId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportDirection {
    Inbound,
    Outbound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionRemovalReason {
    CurrentConnectionCleared,
    DisconnectByPeerId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportLifecycleEvent {
    /// Test-only seam after an inbound candidate has sampled pre-claim
    /// ownership/peer state but before it submits its serialized claim.
    #[cfg(test)]
    InboundOwnershipSnapshotTaken {
        peer: PeerId,
        addr: SocketAddr,
    },
    /// Test-only seam after the owner actor has decided a DNS migration but
    /// before the derived gossip-state move.
    #[cfg(test)]
    DnsOwnershipMigrationDecided {
        from: SocketAddr,
        to: SocketAddr,
        moved: bool,
    },
    OutboundStart {
        peer: Option<PeerId>,
        addr: SocketAddr,
        attempt_id: u64,
    },
    OutboundSuppressedWaitInbound {
        peer: PeerId,
        addr: SocketAddr,
        attempt_id: u64,
    },
    OutboundSuppressedInboundReady {
        peer: PeerId,
        addr: SocketAddr,
        attempt_id: u64,
    },
    OutboundSuppressedInboundTimeout {
        peer: PeerId,
        addr: SocketAddr,
        attempt_id: u64,
    },
    WrongDirectionEvicted {
        peer: PeerId,
        addr: SocketAddr,
        direction: TransportDirection,
    },
    InboundReady {
        peer: PeerId,
        addr: SocketAddr,
    },
    SessionPublished {
        peer: PeerId,
        addr: SocketAddr,
        direction: TransportDirection,
    },
    /// A fully authenticated process reused an already-live peer identity.
    /// The healthy incumbent remains published; the candidate is rejected.
    DuplicateIdentityRejected {
        peer: PeerId,
        addr: SocketAddr,
        incumbent_boot_id: crate::handshake::RemoteBootId,
        rejected_boot_id: crate::handshake::RemoteBootId,
    },
    /// Fired immediately before an outbound-finalize `AcceptIncoming`
    /// decision attempts to enact its publish via compare-and-publish —
    /// unconditionally, regardless of whether that attempt goes on to
    /// succeed or lose a re-resolution against a concurrently published
    /// rival. Purely instrumentation: lets tests deterministically pin a
    /// concurrent publish into the gap between the decision's snapshot and
    /// this attempt, the same gap that produced the tie-break reconnect
    /// thrash from the outbound-finalize side.
    OutboundFinalizePublishAttempt {
        peer: PeerId,
        addr: SocketAddr,
    },
    /// Fired immediately before `publish_outbound_or_reresolve`'s bounded
    /// retry against an observed-empty peer-session slot (the "first
    /// compare-and-publish lost to a concurrent CLEAR, not a publish" case).
    /// Purely instrumentation: lets tests deterministically pin a further
    /// concurrent publish — e.g. a preferred rival — into the narrow gap
    /// between that first CAS loss and this retry, the same technique
    /// `OutboundFinalizePublishAttempt` uses for the wider gap around the
    /// whole publish attempt.
    OutboundFinalizeClearRaceRetry {
        peer: PeerId,
        addr: SocketAddr,
    },
    /// Fired immediately before `resolve_and_act_on_outbound_rival`'s
    /// `AcceptIncoming` arm retries its compare-and-publish against a rival
    /// it just re-resolved as stale/non-preferred. Purely instrumentation:
    /// lets tests deterministically pin a further concurrent publish — e.g.
    /// a fresh preferred session landing in the exact gap between that
    /// re-resolved decision and this retry — so the retry itself observably
    /// loses (`Err(Some(new_rival))` / `Err(None)`), the same technique
    /// `OutboundFinalizeClearRaceRetry` uses for the outer publish's own
    /// bounded retry.
    OutboundFinalizeAcceptIncomingRetryAttempt {
        peer: PeerId,
        addr: SocketAddr,
    },
    /// Fired immediately before `resolve_and_act_on_outbound_rival`'s
    /// `ReplaceExisting` arm retries its compare-and-publish after evicting
    /// the rival it just re-resolved as live-but-non-preferred. Purely
    /// instrumentation, the `ReplaceExisting` counterpart of
    /// `OutboundFinalizeAcceptIncomingRetryAttempt`: lets tests
    /// deterministically pin a further concurrent publish into the exact gap
    /// between that eviction and this retry, so the retry itself observably
    /// loses (`Err(Some(new_rival))` / `Err(None)`).
    OutboundFinalizeReplaceExistingRetryAttempt {
        peer: PeerId,
        addr: SocketAddr,
    },
    /// Fired immediately after `finalize_new_outbound_connection` snapshots
    /// `existing_before` (the pre-existing rival, if any, for this peer) and
    /// before the tie-break decision is computed from it a few lines below.
    /// Purely instrumentation: lets tests deterministically pin a genuine
    /// concurrent liveness change — e.g. `existing_before`'s own IO task
    /// exiting between the snapshot and the decision — into that narrow real
    /// gap, the same technique `OutboundFinalizePublishAttempt` uses for the
    /// wider publish gap.
    OutboundFinalizeExistingSnapshotTaken {
        peer: PeerId,
        addr: SocketAddr,
    },
    /// Fired immediately before `GossipRegistry::handle_peer_connection_failure`'s
    /// matched-instance branch calls `disconnect_connection_instance` to
    /// retire the failed current session by CAS'd identity. Purely
    /// instrumentation: lets tests deterministically pin a concurrent fresh
    /// publish for the same peer into the gap between the instance-id match
    /// above and this CAS attempt, so the CAS observably loses — the same
    /// technique `OutboundFinalizePublishAttempt` uses for the outbound-finalize
    /// side.
    SocketFailureMatchedInstanceTeardownAttempt {
        peer: PeerId,
        addr: SocketAddr,
    },
    SessionRemoved {
        peer: PeerId,
        addr: SocketAddr,
        direction: TransportDirection,
        reason: SessionRemovalReason,
    },
    /// Inbound-accept counterpart of `OutboundFinalizePublishAttempt`: fired
    /// immediately before `publish_inbound_or_reresolve`'s first
    /// compare-and-publish attempt for a freshly-accepted inbound
    /// connection, unconditionally regardless of whether that attempt
    /// succeeds or loses a re-resolution.
    InboundAcceptPublishAttempt {
        peer: PeerId,
        addr: SocketAddr,
    },
    /// Inbound-accept counterpart of `OutboundFinalizeClearRaceRetry`.
    InboundAcceptClearRaceRetry {
        peer: PeerId,
        addr: SocketAddr,
    },
    /// Inbound-accept counterpart of
    /// `OutboundFinalizeAcceptIncomingRetryAttempt`.
    InboundAcceptAcceptIncomingRetryAttempt {
        peer: PeerId,
        addr: SocketAddr,
    },
    /// Inbound-accept counterpart of
    /// `OutboundFinalizeReplaceExistingRetryAttempt`.
    InboundAcceptReplaceExistingRetryAttempt {
        peer: PeerId,
        addr: SocketAddr,
    },
    /// Fired immediately at the top of `finish_indexing_accepted_connection`,
    /// i.e. immediately AFTER `publish_inbound_or_reresolve`'s
    /// compare-and-publish has already won the peer-session slot for this
    /// candidate but BEFORE any of `finish_indexing_accepted_connection`'s
    /// own address-index / `connection_counter` side effects have run.
    /// Purely instrumentation: lets tests deterministically pin a concurrent
    /// evict/supersede of THIS exact just-published candidate into that
    /// narrow window, the window the reviewer finding (stale
    /// `connections_by_addr`/`addr_to_peer_id` alias plus a zombie
    /// `connection_counter` contribution for an already-evicted instance)
    /// depends on — the alias-sweep half of any such eviction runs before
    /// this candidate has any alias to sweep, so without a post-indexing
    /// revalidation it is missed entirely.
    InboundAcceptIndexAttempt {
        peer: PeerId,
        addr: SocketAddr,
    },
    /// Fired in `handle_incoming_connection_tls`, immediately before the
    /// separate ephemeral TCP-source-address (`peer_addr`) alias is written
    /// into `connections_by_addr`/`addr_to_peer_id` — a write that today
    /// happens AFTER `finish_indexing_accepted_connection` has already
    /// returned `true` (i.e. after its own `peer_state_addr` alias write
    /// and revalidation), entirely outside that guard. Purely
    /// instrumentation: lets tests deterministically pin a concurrent
    /// evict/supersede of THIS exact candidate into the window between
    /// `finish_indexing_accepted_connection` returning and this later,
    /// unguarded write — the window in which a concurrent eviction's own
    /// alias-sweep can find and remove the already-durable `peer_state_addr`
    /// alias while the not-yet-written ephemeral alias survives the sweep,
    /// only to be written moments later regardless.
    InboundAcceptEphemeralAliasAttempt {
        peer: PeerId,
        addr: SocketAddr,
    },
    /// Fired at every production `connection_counter` count-in site, exactly
    /// at the pairing point between that instance's ownership-marker
    /// (`counted_instances`) mutation and its `connection_counter`
    /// mutation — the two operations that MUST move together for exactly
    /// one `+1` to be paired with exactly one eventual `-1`. Purely
    /// instrumentation: lets tests deterministically pin a concurrent
    /// teardown (`disconnect_connection_instance`, an IO-exit release, or
    /// any other `release_counted_instance` caller) for THIS exact instance
    /// into the narrow window the review finding depends on — a teardown
    /// that races between the counter increment and the marker insert (or,
    /// symmetrically, before either has happened yet) must never leave a
    /// `connection_counter` contribution with no marker ever able to
    /// release it.
    ConnectionCountMarkerAttempt {
        instance_id: u64,
    },
    /// Fired at every production `connection_counter` count-in site
    /// (`count_in_new_instance`), immediately AFTER that instance's
    /// `counted_instances` ownership marker has been newly inserted and
    /// immediately BEFORE the paired `connection_counter` increment runs.
    /// Purely instrumentation: lets tests deterministically pin a concurrent
    /// `release_counted_instance` for THIS exact, already-marked instance
    /// into that narrow window — the window a non-`saturating` decrement
    /// depends on netting out correctly, since the release's decrement now
    /// runs strictly before this call's own increment.
    ConnectionCountIncrementAttempt {
        instance_id: u64,
    },
    /// Fired unconditionally, immediately before `get_connection_by_peer_id`
    /// attempts its internal self-heal clear of an observed-unusable current
    /// session — i.e. right after it has decided "this session is dead, I'm
    /// about to clear it" but before the clear itself runs. Purely
    /// instrumentation: lets tests deterministically pin a concurrent
    /// publish (e.g. a fresh preferred inbound landing for the same peer)
    /// into this exact gap, so the clear attempt below observably has to
    /// re-validate against reality instead of clobbering it.
    ///
    /// `get_connection_by_peer_id` is called from many places purely to
    /// decide "what does this peer currently have" — including, at one time,
    /// `finalize_new_outbound_connection`'s `existing_before` tie-break
    /// snapshot. This event is the seam that let a test prove that snapshot
    /// used to be able to erase a concurrently published fresh session as a
    /// side effect of merely being read, and that a CAS-based self-heal (or,
    /// better, a pure snapshot that never self-heals at all) closes the gap.
    GetConnectionSelfHealClearAttempt {
        peer: PeerId,
        addr: SocketAddr,
    },
    /// Fired in `merge_full_sync_from`, after the session/sequence
    /// validation critical section has released its lock and the
    /// candidate actor updates have been collected, but immediately
    /// BEFORE the second critical section re-acquires the lock to apply
    /// them (and re-check the captured session generation). Purely
    /// instrumentation: lets tests deterministically pin a concurrent
    /// newer session's arm-and-restart into this exact gap, so the
    /// generation recheck a few lines below observably has to drop this
    /// now-stale pending apply instead of overwriting the newer session's
    /// state with it.
    FullSyncApplyPendingMutation {
        peer: PeerId,
        addr: SocketAddr,
    },
    /// Fired in `apply_delta_from`, immediately before it acquires the
    /// lock for its own single critical section (which applies all
    /// `known_actors`/`removed_actors`/`peer_to_actors` mutations and
    /// re-checks any caller-supplied `session_guard` generation). Purely
    /// instrumentation: lets tests deterministically pin a concurrent
    /// newer session's arm-and-restart into the gap between a caller's
    /// own session validation (which happened, and released its lock,
    /// before calling this function) and this critical section actually
    /// running.
    DeltaApplyPendingMutation {
        peer: PeerId,
        addr: SocketAddr,
    },
    /// Fired in `GossipRegistry::handle_peer_connection_failure` /
    /// `handle_peer_connection_failure_by_peer_id`, immediately after a
    /// confirmed connection's own pool teardown work has fully completed
    /// (no `gossip_state` lock held across any of it) and immediately
    /// BEFORE the discovery-state clear and session-authentication
    /// invalidation that follow re-acquire `gossip_state`. Purely
    /// instrumentation: lets tests deterministically pin a concurrent
    /// replacement session — published and armed for the same peer — into
    /// this exact gap, so `invalidate_session_state_on_teardown`'s epoch
    /// fence a few lines below observably has to decline clearing a
    /// session it does not own instead of clobbering the replacement's.
    SocketFailurePoolTeardownComplete {
        peer: Option<PeerId>,
        addr: SocketAddr,
    },
}

pub type TransportLifecycleRecorder = Arc<dyn Fn(TransportLifecycleEvent) + Send + Sync + 'static>;

static RECORDER: OnceLock<RwLock<Option<TransportLifecycleRecorder>>> = OnceLock::new();

fn recorder_cell() -> &'static RwLock<Option<TransportLifecycleRecorder>> {
    RECORDER.get_or_init(|| RwLock::new(None))
}

pub fn set_transport_lifecycle_recorder(recorder: Option<TransportLifecycleRecorder>) {
    *recorder_cell()
        .write()
        .expect("transport lifecycle recorder lock poisoned") = recorder;
}

/// Process-wide lock serializing every installation of the global
/// [`set_transport_lifecycle_recorder`] hook. The recorder is shared, mutable,
/// global state (a single `OnceLock<RwLock<Option<...>>>` above); the default
/// parallel test harness runs many `#[test]`/`#[tokio::test]` functions
/// concurrently, so without a single shared lock, two such tests can install/
/// deregister each other's closures mid-test.
///
/// This is intentionally private: the only supported way to acquire it is
/// through [`TransportLifecycleRecorderGuard::install`], so a test cannot
/// install a recorder without holding the lock for the guard's entire
/// lifetime — correct by construction rather than by every call site
/// remembering to take a lock.
static RECORDER_INSTALL_LOCK: Mutex<()> = Mutex::new(());

/// RAII installer for [`set_transport_lifecycle_recorder`]. Acquires the
/// process-wide recorder-install lock for its entire lifetime and
/// deregisters the recorder on drop, so concurrently running tests that each
/// install a recorder are fully serialized against one another and can never
/// observe or clobber each other's hook — this is the only sanctioned way to
/// install a recorder in tests.
#[must_use = "the recorder is uninstalled when this guard is dropped"]
pub struct TransportLifecycleRecorderGuard {
    _lock: MutexGuard<'static, ()>,
}

impl TransportLifecycleRecorderGuard {
    pub fn install(recorder: TransportLifecycleRecorder) -> Self {
        let lock = RECORDER_INSTALL_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        set_transport_lifecycle_recorder(Some(recorder));
        Self { _lock: lock }
    }
}

impl Drop for TransportLifecycleRecorderGuard {
    fn drop(&mut self) {
        set_transport_lifecycle_recorder(None);
    }
}

pub(crate) fn record_transport_event(event: TransportLifecycleEvent) {
    let recorder = recorder_cell()
        .read()
        .expect("transport lifecycle recorder lock poisoned")
        .clone();
    if let Some(recorder) = recorder {
        recorder(event);
    }
}
