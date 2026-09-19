use arc_swap::{ArcSwapOption, ArcSwapWeak};
use bytes::{Buf, BytesMut};
use futures::task::AtomicWaker;
use scc::HashMap as SccHashMap;
use std::cell::UnsafeCell;
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::sync::OnceLock;
use std::sync::atomic::{
    AtomicBool, AtomicIsize, AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering,
};
use std::task::{Context, Poll};
use std::{net::SocketAddr, pin::Pin, sync::Arc, time::Duration, time::Instant};
#[cfg(test)]
use tokio::io::AsyncReadExt;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio::task::{AbortHandle, JoinHandle};
#[cfg(feature = "trace-correlation")]
use tracing::trace;
use tracing::{debug, error, info, warn};

/// Atomic publication-vs-delivery linearization.
///
/// The low two bits encode the transition phase, while the remaining bits
/// identify the generation. A notifier changes `Idle -> Armed -> Entered`.
/// Publication changes `Idle/Armed -> Publishing -> Idle`; the CAS that wins
/// the Armed race is the publication-vs-entry linearization point. No mutex is
/// held across handler construction or future polling, so synchronous
/// re-entry into publication is safe.
struct DisconnectDeliveryState {
    generation: AtomicU64,
}

impl DisconnectDeliveryState {
    const fn new() -> Self {
        Self {
            generation: AtomicU64::new(0),
        }
    }

    fn try_arm(&'static self) -> Option<DisconnectDeliveryClaim> {
        loop {
            let current = self.generation.load(Ordering::Acquire);
            if current & 3 != 0 {
                return None;
            }
            let armed = current.wrapping_add(1);
            if self
                .generation
                .compare_exchange(current, armed, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Some(DisconnectDeliveryClaim {
                    state: self,
                    armed,
                    entered: false,
                });
            }
        }
    }

    fn begin(&'static self) -> DisconnectPublicationGuard {
        loop {
            let current = self.generation.load(Ordering::Acquire);
            match current & 3 {
                0 => {
                    // Idle -> Publishing. The generation changes before the
                    // pool slot is touched, so a notifier cannot arm against
                    // the old slot during this publication's commit window.
                    let publishing = current.wrapping_add(3);
                    if self
                        .generation
                        .compare_exchange(current, publishing, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        return DisconnectPublicationGuard {
                            state: self,
                            owned: true,
                        };
                    }
                }
                1 => {
                    // Armed -> Publishing supersedes the stale callback.
                    let publishing = current.wrapping_add(2);
                    if self
                        .generation
                        .compare_exchange(current, publishing, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        return DisconnectPublicationGuard {
                            state: self,
                            owned: true,
                        };
                    }
                }
                2 => {
                    // The callback already linearized entry. Publication is
                    // deliberately reentrant and needs no exclusion.
                    return DisconnectPublicationGuard {
                        state: self,
                        owned: false,
                    };
                }
                _ => std::thread::yield_now(),
            }
        }
    }

    fn finish(&self) {
        let current = self.generation.load(Ordering::Acquire);
        debug_assert_eq!(current & 3, 3);
        let _ = self.generation.compare_exchange(
            current,
            current.wrapping_add(1),
            Ordering::Release,
            Ordering::Relaxed,
        );
    }
}

static DISCONNECT_DELIVERY_STATE: DisconnectDeliveryState = DisconnectDeliveryState::new();

pub(crate) struct DisconnectDeliveryClaim {
    state: &'static DisconnectDeliveryState,
    armed: u64,
    entered: bool,
}

impl DisconnectDeliveryClaim {
    /// Enter user callback code iff publication did not supersede the armed
    /// notification. The claim is dropped immediately after this CAS, so the
    /// entered phase is only the linearization handoff, never a user-code lock.
    pub(crate) fn enter(mut self) -> bool {
        let entered = self
            .state
            .generation
            .compare_exchange(
                self.armed,
                self.armed.wrapping_add(1),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok();
        self.entered = entered;
        entered
    }
}

impl Drop for DisconnectDeliveryClaim {
    fn drop(&mut self) {
        let (expected, next) = if self.entered {
            (self.armed.wrapping_add(1), self.armed.wrapping_add(3))
        } else {
            (self.armed, self.armed.wrapping_add(3))
        };
        let _ = self.state.generation.compare_exchange(
            expected,
            next,
            Ordering::Release,
            Ordering::Acquire,
        );
    }
}

pub(crate) struct DisconnectPublicationGuard {
    state: &'static DisconnectDeliveryState,
    owned: bool,
}

impl Drop for DisconnectPublicationGuard {
    fn drop(&mut self) {
        if self.owned {
            self.state.finish();
        }
    }
}

pub(crate) fn try_arm_disconnect_delivery() -> Option<DisconnectDeliveryClaim> {
    DISCONNECT_DELIVERY_STATE.try_arm()
}

pub(crate) fn begin_disconnect_publication() -> DisconnectPublicationGuard {
    DISCONNECT_DELIVERY_STATE.begin()
}

#[cfg(any(test, feature = "test-helpers"))]
use sha2::{Digest, Sha256};

use crate::{
    GossipError, Result, current_timestamp, framing,
    registry::{GossipRegistry, RegistryMessage, resolve_peer_addr_checked},
};

include!("constants.rs");
include!("types.rs");
include!("buffers.rs");
include!("read_pipeline.rs");
include!("writer_commands.rs");
include!("stream_writer.rs");
include!("pool_index.rs");
include!("correlation.rs");
include!("handle.rs");
include!("pool_connect.rs");

pub(crate) mod reply_slots;

#[cfg(test)]
mod disconnect_delivery_tests {
    use std::{net::SocketAddr, sync::Arc, time::Duration};

    use super::{
        ConnectionDirection, ConnectionPool, DisconnectDeliveryState, LockFreeConnection,
        try_arm_disconnect_delivery,
    };

    #[test]
    fn publication_supersedes_an_armed_callback_without_waiting() {
        static STATE: DisconnectDeliveryState = DisconnectDeliveryState::new();
        let claim = STATE.try_arm().expect("test callback must arm");
        let publication = STATE.begin();
        assert!(!claim.enter(), "publication must win the armed-entry CAS");
        drop(publication);
        assert!(
            STATE.try_arm().is_some(),
            "publication must leave the state idle"
        );
    }

    #[test]
    fn address_index_publication_supersedes_an_armed_callback() {
        let pool = ConnectionPool::<()>::new(4, Duration::from_secs(1));
        let addr: SocketAddr = "127.0.0.1:41001".parse().unwrap();
        let connection = Arc::new(LockFreeConnection::new(addr, ConnectionDirection::Inbound));
        let claim = try_arm_disconnect_delivery().expect("test callback must arm");

        pool.publish_connection_by_addr(addr, connection.clone());

        assert!(
            !claim.enter(),
            "address-index publication must supersede an armed address-only callback"
        );
        assert!(
            pool.connections_by_addr
                .read_sync(&addr, |_, current| Arc::ptr_eq(current, &connection))
                .unwrap_or(false)
        );
    }
}

#[cfg(test)]
mod lease_test_support;

#[cfg(any(test, feature = "test-helpers"))]
pub(crate) mod lease_stats;

pub(crate) mod transport_stream;

#[cfg(test)]
pub(crate) mod tests;
