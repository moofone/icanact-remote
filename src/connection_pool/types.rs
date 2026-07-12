/// ACTOR_REM_2 R16f: milliseconds from a process-global monotonic start, stored
/// in `last_used` for LRU eviction. Monotonic (never steps backward), unlike the
/// wall-clock `current_timestamp()` it replaces there.
fn monotonic_now_millis() -> usize {
    use std::sync::OnceLock;
    use std::time::Instant;
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_millis() as usize
}

/// Channel IDs for stream multiplexing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ChannelId {
    TellAsk = 0x00, // Regular tell/ask channel
    Global = 0xFF,  // Global channel for all operations
}

/// Lock-free connection state representation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum ConnectionState {
    Disconnected = 0,
    Connecting = 1,
    Connected = 2,
    Failed = 3,
}

impl From<u32> for ConnectionState {
    fn from(value: u32) -> Self {
        match value {
            0 => ConnectionState::Disconnected,
            1 => ConnectionState::Connecting,
            2 => ConnectionState::Connected,
            3 => ConnectionState::Failed,
            _ => ConnectionState::Failed,
        }
    }
}

/// Direction of the TCP connection relative to this node
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionDirection {
    Inbound,
    Outbound,
}

/// Lock-free connection metadata
#[derive(Debug)]
pub struct LockFreeConnection {
    pub addr: SocketAddr,
    pub state: AtomicU32,       // ConnectionState
    pub last_used: AtomicUsize, // Timestamp
    pub bytes_written: AtomicUsize,
    pub bytes_read: AtomicUsize,
    pub failure_count: AtomicUsize,
    pub stream_handle: Option<Arc<LockFreeStreamHandle>>,
    pub(crate) correlation: Option<Arc<CorrelationTracker>>,
    pub direction: ConnectionDirection,
    /// Embedded peer_id for this connection
    /// IMPORTANT: This allows looking up peer_id for inbound connections even after
    /// addr_to_peer_id mapping has been migrated to bind address (ephemeral port removed)
    pub(crate) embedded_peer_id: Option<crate::PeerId>,
    /// Task tracker for background tasks (writer and reader)
    pub task_tracker: TaskTracker,
}

impl Clone for LockFreeConnection {
    fn clone(&self) -> Self {
        Self {
            addr: self.addr,
            state: AtomicU32::new(self.state.load(Ordering::Relaxed)),
            last_used: AtomicUsize::new(self.last_used.load(Ordering::Relaxed)),
            bytes_written: AtomicUsize::new(self.bytes_written.load(Ordering::Relaxed)),
            bytes_read: AtomicUsize::new(self.bytes_read.load(Ordering::Relaxed)),
            failure_count: AtomicUsize::new(self.failure_count.load(Ordering::Relaxed)),
            stream_handle: self.stream_handle.clone(),
            correlation: self.correlation.clone(),
            direction: self.direction,
            embedded_peer_id: self.embedded_peer_id.clone(),
            // Note: TaskTracker is not cloned - each clone gets a fresh tracker
            // This is intentional: clones are typically used for metadata snapshots,
            // not to transfer task ownership
            task_tracker: TaskTracker::new(),
        }
    }
}

impl LockFreeConnection {
    pub fn new(addr: SocketAddr, direction: ConnectionDirection) -> Self {
        Self {
            addr,
            state: AtomicU32::new(ConnectionState::Disconnected as u32),
            last_used: AtomicUsize::new(0),
            bytes_written: AtomicUsize::new(0),
            bytes_read: AtomicUsize::new(0),
            failure_count: AtomicUsize::new(0),
            stream_handle: None,
            correlation: Some(CorrelationTracker::new()),
            direction,
            embedded_peer_id: None,
            task_tracker: TaskTracker::new(),
        }
    }

    /// Abort all tracked background tasks (writer, reader).
    /// Call this when tearing down the connection to prevent resource leaks.
    pub fn abort_tasks(&self) {
        // NOTE: `JoinHandle::abort()` does not run destructors inside the task. Our IO task
        // sets `exit_flag` and cancels all pending correlation slots via an `ExitGuard` in Drop.
        // If we abort without also flipping these flags, callers can observe a "zombie handle"
        // where `ConnectionHandle::is_closed()` remains false and asks hang until timeout.
        if let Some(handle) = self.stream_handle.as_ref() {
            handle.shutdown_signal.store(true, Ordering::Release);
            handle.exit_flag.store(true, Ordering::Release);
            handle.exit_notify.notify_waiters();
        }
        if let Some(correlation) = self.correlation.as_ref() {
            correlation.cancel_all();
        }
        self.task_tracker.abort_all();
    }

    pub fn get_state(&self) -> ConnectionState {
        self.state.load(Ordering::Acquire).into()
    }

    pub fn set_state(&self, state: ConnectionState) {
        self.state.store(state as u32, Ordering::Release);
    }

    pub fn try_set_state(&self, expected: ConnectionState, new: ConnectionState) -> bool {
        self.state
            .compare_exchange(
                expected as u32,
                new as u32,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    pub fn update_last_used(&self) {
        // ACTOR_REM_2 R16f: `last_used` feeds `select_lru_eviction_victim`, which
        // only ever compares these values against each other. Use a MONOTONIC
        // clock so a backward wall-clock step (NTP correction) cannot make a
        // recently-used connection look like the least-recently-used eviction
        // victim. `last_touched` in pool_index already uses a monotonic Instant.
        self.last_used
            .store(monotonic_now_millis(), Ordering::Release);
    }

    pub fn increment_failure_count(&self) -> usize {
        self.failure_count.fetch_add(1, Ordering::AcqRel)
    }

    pub fn reset_failure_count(&self) {
        self.failure_count.store(0, Ordering::Release);
    }

    pub fn is_connected(&self) -> bool {
        self.get_state() == ConnectionState::Connected
    }

    pub(crate) fn has_live_stream(&self) -> bool {
        self.is_connected()
            && self
                .stream_handle
                .as_ref()
                .map(|handle| !handle.exit_flag.load(Ordering::Acquire))
                .unwrap_or(false)
    }

    pub fn is_failed(&self) -> bool {
        self.get_state() == ConnectionState::Failed
    }
}

/// Payloads for queued writes.
pub enum WritePayload {
    Single(bytes::Bytes),
    HeaderPayload {
        header: bytes::Bytes,
        payload: bytes::Bytes,
    },
    HeaderInline {
        header: [u8; 16],
        header_len: u8,
        payload: bytes::Bytes,
    },
    HeaderInlineAligned {
        header: [u8; 16],
        header_len: u8,
        payload: crate::AlignedBytes,
    },
    HeaderInline32 {
        header: [u8; 32],
        payload: bytes::Bytes,
    },
    HeaderPooled {
        header: bytes::Bytes,
        prefix: Option<bytes::Bytes>,
        payload: crate::typed::PooledPayload,
    },
    HeaderInlinePooled {
        header: [u8; 16],
        header_len: u8,
        prefix: Option<[u8; 16]>,
        prefix_len: u8,
        payload: crate::typed::PooledPayload,
    },
    /// DirectAsk fast path - header is [length:4][type:1][correlation_id:2][payload_len:4]
    DirectAskInline {
        header: [u8; 16], // DIRECT_ASK_FRAME_HEADER_LEN
        payload: bytes::Bytes,
    },
    Buf(Box<dyn Buf + Send>),
}

impl std::fmt::Debug for WritePayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WritePayload::Single(data) => f.debug_tuple("Single").field(&data.len()).finish(),
            WritePayload::HeaderPayload { header, payload } => f
                .debug_struct("HeaderPayload")
                .field("header_len", &header.len())
                .field("payload_len", &payload.len())
                .finish(),
            WritePayload::HeaderInline {
                header_len,
                payload,
                ..
            } => f
                .debug_struct("HeaderInline")
                .field("header_len", &header_len)
                .field("payload_len", &payload.len())
                .finish(),
            WritePayload::HeaderInlineAligned {
                header_len,
                payload,
                ..
            } => f
                .debug_struct("HeaderInlineAligned")
                .field("header_len", &header_len)
                .field("payload_len", &payload.len())
                .finish(),
            WritePayload::HeaderInline32 { payload, .. } => f
                .debug_struct("HeaderInline32")
                .field("header_len", &32)
                .field("payload_len", &payload.len())
                .finish(),
            WritePayload::HeaderPooled {
                header,
                prefix,
                payload,
            } => f
                .debug_struct("HeaderPooled")
                .field("header_len", &header.len())
                .field("prefix_len", &prefix.as_ref().map(|p| p.len()).unwrap_or(0))
                .field("payload_len", &payload.len())
                .finish(),
            WritePayload::HeaderInlinePooled {
                header_len,
                prefix,
                prefix_len,
                payload,
                ..
            } => f
                .debug_struct("HeaderInlinePooled")
                .field("header_len", &header_len)
                .field(
                    "prefix_len",
                    &if prefix.is_some() { *prefix_len } else { 0 },
                )
                .field("payload_len", &payload.len())
                .finish(),
            WritePayload::Buf(_) => f.debug_tuple("Buf").field(&"<buf>").finish(),
            WritePayload::DirectAskInline { header: _, payload } => f
                .debug_struct("DirectAskInline")
                .field("header_len", &crate::framing::DIRECT_ASK_FRAME_HEADER_LEN)
                .field("payload_len", &payload.len())
                .finish(),
        }
    }
}

// `WritePayload` is passed across tasks/threads by move.
// Do not add `Sync` here: the `Buf` variant is not required to be `Sync`.
