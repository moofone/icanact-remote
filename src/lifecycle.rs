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
    OutboundSuppressedPreferInbound {
        peer: PeerId,
        addr: SocketAddr,
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
/// process-wide [`RECORDER_INSTALL_LOCK`] for its entire lifetime and
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
