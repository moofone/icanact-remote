use std::net::SocketAddr;
use std::sync::{Arc, OnceLock, RwLock};

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

pub(crate) fn record_transport_event(event: TransportLifecycleEvent) {
    let recorder = recorder_cell()
        .read()
        .expect("transport lifecycle recorder lock poisoned")
        .clone();
    if let Some(recorder) = recorder {
        recorder(event);
    }
}
