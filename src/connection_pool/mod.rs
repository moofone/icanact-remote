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
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio::task::{AbortHandle, JoinHandle};
#[cfg(feature = "trace-correlation")]
use tracing::trace;
use tracing::{debug, error, info, warn};

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

pub(crate) mod transport_stream;

#[cfg(test)]
pub(crate) mod tests;
