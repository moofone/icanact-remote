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

/// Serializes connection publication with the final peer-disconnect delivery
/// decision. Both operations are cold-path lifecycle transitions; keeping one
/// process-wide gate here avoids an await-separated check-then-enter window
/// without coupling the pool's lock-free indexes to registry state.
static DISCONNECT_DELIVERY_GATE: std::sync::Mutex<()> = std::sync::Mutex::new(());

pub(crate) fn lock_disconnect_delivery() -> std::sync::MutexGuard<'static, ()> {
    DISCONNECT_DELIVERY_GATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
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
mod lease_test_support;

#[cfg(any(test, feature = "test-helpers"))]
pub(crate) mod lease_stats;

pub(crate) mod transport_stream;

#[cfg(test)]
pub(crate) mod tests;
