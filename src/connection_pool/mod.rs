use arc_swap::{ArcSwapOption, ArcSwapWeak};
use bytes::{Buf, BufMut, BytesMut};
use futures::task::AtomicWaker;
use scc::HashMap as SccHashMap;
use std::cell::UnsafeCell;
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::sync::OnceLock;
use std::sync::atomic::{
    AtomicBool, AtomicU8, AtomicU16, AtomicU32, AtomicU64, AtomicUsize, Ordering,
};
use std::task::{Context, Poll};
use std::{
    collections::HashMap, net::SocketAddr, pin::Pin, sync::Arc, time::Duration, time::Instant,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::Notify;
use tokio::task::{AbortHandle, JoinHandle};
use tracing::{debug, error, info, warn};
#[cfg(feature = "trace-correlation")]
use tracing::trace;

#[cfg(any(test, feature = "test-helpers", debug_assertions))]
use sha2::{Digest, Sha256};

use crate::{
    GossipError, Result, current_timestamp, framing,
    registry::{GossipRegistry, RegistryMessage, resolve_peer_addr},
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
pub(crate) mod transport_udp;

#[cfg(test)]
mod tests;
