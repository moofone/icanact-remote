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
    /// Authenticated process incarnation from the remote Hello exchange.
    /// `None` exists only for synthetic/unit connections and pre-Hello paths.
    pub(crate) remote_boot_id: Option<crate::handshake::RemoteBootId>,
    /// Task tracker for background tasks (writer and reader)
    pub task_tracker: TaskTracker,
    /// R-11: this connection's own session discriminator, mirroring
    /// `ReadContext::session_source` and set once at construction (never
    /// mutated afterwards) -- unique per physical connection. For inbound
    /// connections this is the remote's TCP source (ephemeral port
    /// included), same as `addr`; for outbound connections it is this
    /// socket's own local ephemeral port, not the dial target (`addr`,
    /// which is the peer's fixed listening port and identical across every
    /// connection ever made to it).
    ///
    /// This is what lets `peer_info_is_from_current_session` confirm that
    /// an inbound message actually arrived on the pool's currently
    /// published connection for a peer, by comparing the message's
    /// `session_source` against this field on the `Arc<LockFreeConnection>`
    /// `peer_current_connection_snapshot` returns -- the same
    /// non-spoofable per-connection identity `Arc::ptr_eq` gives when the
    /// receiving connection's own `Arc` is directly in hand (as it is at
    /// arming time), used here as a value comparison instead because the
    /// receiving connection's `Arc` does not exist yet at the point its
    /// `ReadContext` is constructed (the stream handle -- and this struct
    /// wrapping it -- is built from that same `ReadContext`).
    pub(crate) session_source: SocketAddr,
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
            remote_boot_id: self.remote_boot_id,
            // Note: TaskTracker is not cloned - each clone gets a fresh tracker
            // This is intentional: clones are typically used for metadata snapshots,
            // not to transfer task ownership
            task_tracker: TaskTracker::new(),
            session_source: self.session_source,
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
            remote_boot_id: None,
            task_tracker: TaskTracker::new(),
            // Default matches the inbound case (session_source == addr).
            // The one outbound construction site overrides this to the
            // dialling socket's own local ephemeral port immediately after
            // construction, before the connection is published.
            session_source: addr,
        }
    }

    /// Abort all tracked background tasks (writer, reader).
    /// Call this when tearing down the connection to prevent resource leaks.
    pub fn abort_tasks(&self) {
        self.abort_tasks_inner(true);
    }

    /// Same as [`Self::abort_tasks`], but never cancels `correlation` — not
    /// the direct call here, and not indirectly through the IO task's own
    /// `ExitGuard` either.
    ///
    /// `correlation` is a SESSION-level `Arc<CorrelationTracker>`, shared by
    /// pointer across every reconnect instance for a peer (installed via
    /// `ConnectionPool::get_or_create_correlation_tracker` /
    /// `add_connection_by_peer_id`). Callers that are tearing down a
    /// DISPLACED/losing connection instance while a DIFFERENT, still-live
    /// sibling instance for the same peer keeps using that identical tracker
    /// Arc (e.g. `retire_displaced_expected` retiring `expected` after
    /// `winner` is already published, or
    /// `unpublish_rejected_outbound_candidate` discarding a candidate while
    /// restoring a still-live `existing_before`) must use this instead of
    /// `abort_tasks()`: an unconditional `cancel_all()` there would cancel
    /// the SURVIVING sibling's in-flight ask slots, not just the instance
    /// actually being torn down.
    ///
    /// Skipping the direct `cancel_all()` call below is not by itself
    /// enough: the IO task's own `ExitGuard` also cancels `correlation` on
    /// drop unless it independently infers this exact instance is
    /// superseded, and that inference reads pool state which can lag this
    /// call (the peer/addr lookup it uses may not yet reflect the surviving
    /// sibling). So before signalling exit, this also marks the stream
    /// handle's `known_superseded` flag, which the `ExitGuard` checks ahead
    /// of — and authoritatively overriding — its own inference.
    ///
    /// Callers must first confirm the tracker is actually shared with a
    /// still-live sibling (see [`shares_correlation_tracker`]) — this only
    /// skips the cancellation, it does not itself decide whether skipping is
    /// correct. A genuinely final teardown (no surviving sibling) must keep
    /// using `abort_tasks()` so its own in-flight callers still observe
    /// `ConnectionDropped` instead of hanging until timeout.
    pub(crate) fn abort_tasks_keep_correlation(&self) {
        if let Some(handle) = self.stream_handle.as_ref() {
            handle.known_superseded.store(true, Ordering::Release);
        }
        self.abort_tasks_inner(false);
    }

    fn abort_tasks_inner(&self, cancel_correlation: bool) {
        // NOTE: `JoinHandle::abort()` does not run destructors inside the task. Our IO task
        // sets `exit_flag` and cancels all pending correlation slots via an `ExitGuard` in Drop.
        // If we abort without also flipping these flags, callers can observe a "zombie handle"
        // where `ConnectionHandle::is_closed()` remains false and asks hang until timeout.
        if let Some(handle) = self.stream_handle.as_ref() {
            handle.shutdown_signal.store(true, Ordering::Release);
            handle.exit_flag.store(true, Ordering::Release);
            handle.exit_notify.notify_waiters();
        }
        if cancel_correlation && let Some(correlation) = self.correlation.as_ref() {
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

    /// True when `self` and `other` hold the exact same `Arc<CorrelationTracker>`
    /// pointer (a shared, SESSION-level tracker) rather than merely
    /// `==`-equal/independent trackers. See
    /// [`Self::abort_tasks_keep_correlation`] for why callers retiring one of
    /// two sibling instances for the same peer must check this before
    /// deciding whether `cancel_all()` is safe to run.
    pub(crate) fn shares_correlation_tracker(&self, other: &Self) -> bool {
        matches!(
            (self.correlation.as_ref(), other.correlation.as_ref()),
            (Some(a), Some(b)) if Arc::ptr_eq(a, b)
        )
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
    /// The public, generic "send these bytes" entry points
    /// (`write_bytes_control`/`write_bytes_ask`/`write_bytes_nonblocking`,
    /// `write_vectored_nonblocking`, `write_chunked_nonblocking`, and the
    /// `ConnectionHandle` methods built on them:
    /// `send_data`/`send_raw_bytes`/`send_bytes_zero_copy`/
    /// `send_binary_message`) all construct this variant.
    ///
    /// PR #183 review, round 12: this crate's wire protocol has no concept
    /// of *opaque, unframed* bytes for this variant to carry -- round 11
    /// tried an opaque/framed split on the theory that content cannot
    /// answer "is this a frame" and the caller should declare it instead,
    /// which is true about content-sniffing but false about there being a
    /// legitimate opaque case to declare. Every read path this crate has
    /// (`read_message_step_poll`/`read_message_step_nonblocking` in `read_pipeline.rs`)
    /// unconditionally decodes a control word from whatever the peer sends
    /// and fails the connection if it doesn't decode; there is no
    /// raw-passthrough mode a sender could target with genuinely unframed
    /// bytes. So every one of the methods above -- whether or not anything
    /// in this crate currently calls them -- carries complete frame(s) by
    /// the only contract this wire format supports.
    ///
    /// PR #183 review, round 13: `reject_oversize_single` (in
    /// `stream_writer.rs`) enforces that contract as a closure property --
    /// a `Single` write is accepted if and only if it consists of exactly
    /// N complete V5 frames, N >= 0, each within `max_message_size`, with
    /// nothing left over. Several valid frames concatenated in one write
    /// are judged the way separate writes would have been (not rejected
    /// for their aggregate length), but *any* leftover bytes that are not
    /// themselves a complete frame -- too short to hold a control word, a
    /// control word that doesn't decode, or a decoded frame the buffer
    /// doesn't fully supply -- are refused outright. There is no
    /// bare-length fallback for undecodable content: see that method's own
    /// doc comment for the induction argument this closure property makes
    /// sufficient on its own, independent of how a caller splits its
    /// writes.
    ///
    /// This is deliberately the only variant a caller outside this `impl`
    /// block can reach with arbitrary content -- see `TrustedFrame` for the
    /// alternative used by every internal caller that built the bytes
    /// itself and already knows they are safe. That split, not a comment on
    /// this variant, is what stops a future generic-bytes call site from
    /// silently inheriting an exemption it was never entitled to.
    Single(bytes::Bytes),
    /// A caller-opaque byte blob this crate built and validated itself --
    /// constructible only via `LockFreeStreamHandle::write_trusted_bytes_control`/
    /// `write_trusted_bytes_ask`, which are `pub(crate)`: nothing outside this
    /// crate can produce one. `reject_oversize_write_payload` exempts it
    /// unconditionally, so every call site that constructs it is exactly as
    /// trusted as that exemption -- currently: the fixed-size `RouteBind`/
    /// `StreamAbort` control frames (a handful of bytes, built from
    /// `framing`'s own header constructors, never a caller-supplied length),
    /// `ConnectionHandle::ask_batch_deferred`'s pre-concatenated batch
    /// (each request already passed `reject_oversize_inline` individually
    /// before concatenation; the aggregate is expected to exceed
    /// `max_message_size` by design and must not be gated against it), and
    /// `write_chunked_nonblocking`'s per-chunk fragments (the whole buffer
    /// is validated once, before chunking, against the same per-frame walk
    /// `Single` uses -- a fragment has no declared length of its own to
    /// check, and re-validating one as if it were a complete `Single`
    /// write could reject a fragment of already-valid content).
    TrustedFrame(bytes::Bytes),
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
    /// DirectAsk fast path - header is [length:4][type:1][correlation_id:4][payload_len:4]
    DirectAskInline {
        header: [u8; 16], // DIRECT_ASK_FRAME_HEADER_LEN
        payload: bytes::Bytes,
    },
    /// A generic `Buf` write (header chained with a caller-supplied payload,
    /// written without concatenating). `expected_len` is the exact byte
    /// count the caller declared this write would produce -- generally
    /// `header.len() + payload_len` from whatever `write_*_header` call
    /// built the frame -- captured separately from `buf` itself precisely
    /// because `buf.remaining()` is not trustworthy on its own: a caller
    /// can build a header from one length and chain a `Buf` whose actual
    /// `remaining()` disagrees with it. See `reject_oversize_write_payload`,
    /// which is the only place `expected_len` is read.
    Buf {
        buf: Box<dyn Buf + Send>,
        expected_len: usize,
    },
}

impl std::fmt::Debug for WritePayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WritePayload::Single(data) => f.debug_tuple("Single").field(&data.len()).finish(),
            WritePayload::TrustedFrame(data) => {
                f.debug_tuple("TrustedFrame").field(&data.len()).finish()
            }
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
            WritePayload::Buf { expected_len, .. } => f
                .debug_struct("Buf")
                .field("expected_len", expected_len)
                .finish(),
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
