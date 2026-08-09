/// Connection pool for maintaining persistent TCP connections to peers
/// All connections are persistent - there is no checkout/checkin
/// Lock-free connection pool using atomic operations and lock-free data structures
pub struct ConnectionPool<T = ()> {
    /// PRIMARY: Mapping Peer ID -> LockFreeConnection
    /// This is the main storage - we identify connections by peer ID, not address
    pub connections_by_peer: SccHashMap<crate::PeerId, Arc<LockFreeConnection>>,
    /// SECONDARY: Mapping SocketAddr -> Peer ID (for incoming connection identification)
    pub addr_to_peer_id: SccHashMap<SocketAddr, crate::PeerId>,
    /// Configuration: Peer ID -> Expected SocketAddr (where to connect)
    pub peer_id_to_addr: SccHashMap<crate::PeerId, SocketAddr>,
    /// Address-based connection index for fast lookup by SocketAddr
    pub connections_by_addr: SccHashMap<SocketAddr, Arc<LockFreeConnection>>,
    /// Stable per-peer session state that survives reconnects.
    peer_sessions: SccHashMap<crate::PeerId, Arc<PeerSession>>,
    /// Cold-path dial ownership gate keyed by address so concurrent callers share one outbound dial.
    outbound_dial_gates: SccHashMap<SocketAddr, Arc<OutboundDialGate>>,
    max_connections: usize,
    connection_timeout: Duration,
    /// Registry reference for handling incoming messages
    registry: ArcSwapWeak<GossipRegistry>,
    /// Shared message buffer pool for zero-allocation processing
    message_buffer_pool: Arc<MessageBufferPool>,
    /// Shared aligned bytes pool for zero-copy receive buffers
    aligned_bytes_pool: Arc<crate::AlignedBytesPool>,
    /// Shared UDP socket for datagram transport mode
    udp_socket: ArcSwapOption<UdpSocket>,
    /// Connection counter for load balancing
    connection_counter: AtomicUsize,
    routing_revision: AtomicU64,
    routing_change_notify: Arc<Notify>,
    #[cfg(test)]
    preferred_connection_checks: AtomicU64,
    _marker: PhantomData<fn() -> T>,
}

struct PeerSession {
    route_addr: std::sync::RwLock<Option<SocketAddr>>,
    required_addr: std::sync::RwLock<Option<SocketAddr>>,
    required_peer: AtomicBool,
    correlation: Arc<CorrelationTracker>,
    current_connection: ArcSwapOption<LockFreeConnection>,
    /// Consecutive consumer-classified streak-timeouts for this peer. Lives on
    /// the session (which survives reconnects) so the streak is genuinely
    /// per-peer. Reset on a successful ask or on eviction.
    consecutive_ask_timeouts: AtomicU8,
}

impl PeerSession {
    fn new() -> Self {
        Self {
            route_addr: std::sync::RwLock::new(None),
            required_addr: std::sync::RwLock::new(None),
            required_peer: AtomicBool::new(false),
            correlation: CorrelationTracker::new(),
            current_connection: ArcSwapOption::empty(),
            consecutive_ask_timeouts: AtomicU8::new(0),
        }
    }

    fn reset_ask_timeout_streak(&self) {
        self.consecutive_ask_timeouts.store(0, Ordering::Release);
    }

    /// Increment and return the new consecutive streak-timeout count.
    fn record_ask_timeout(&self) -> u8 {
        self.consecutive_ask_timeouts
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1)
    }

    fn configured_addr(&self) -> Option<SocketAddr> {
        self.route_addr()
            .or_else(|| self.required_addr())
    }

    fn set_configured_addr(&self, addr: SocketAddr) {
        *self
            .route_addr
            .write()
            .expect("peer session route_addr poisoned") = Some(addr);
    }

    fn route_addr(&self) -> Option<SocketAddr> {
        *self
            .route_addr
            .read()
            .expect("peer session route_addr poisoned")
    }

    fn required_addr(&self) -> Option<SocketAddr> {
        *self
            .required_addr
            .read()
            .expect("peer session required_addr poisoned")
    }

    fn set_required_addr(&self, addr: SocketAddr) {
        *self
            .required_addr
            .write()
            .expect("peer session required_addr poisoned") = Some(addr);
    }

    fn mark_required_peer(&self) {
        self.required_peer.store(true, Ordering::Release);
    }

    fn is_required_peer(&self) -> bool {
        self.required_peer.load(Ordering::Acquire)
    }

    fn current_connection(&self) -> Option<Arc<LockFreeConnection>> {
        self.current_connection.load_full()
    }

    fn set_current_connection(&self, connection: Option<Arc<LockFreeConnection>>) {
        self.current_connection.store(connection);
    }
}

const OUTBOUND_DIAL_PENDING: u8 = 0;
const OUTBOUND_DIAL_SUCCEEDED: u8 = 1;
const OUTBOUND_DIAL_FAILED: u8 = 2;

struct OutboundDialGate {
    state: AtomicU8,
    notify: Notify,
}

impl OutboundDialGate {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(OUTBOUND_DIAL_PENDING),
            notify: Notify::new(),
        }
    }

    fn finish(&self, succeeded: bool) {
        self.state.store(
            if succeeded {
                OUTBOUND_DIAL_SUCCEEDED
            } else {
                OUTBOUND_DIAL_FAILED
            },
            Ordering::Release,
        );
        self.notify.notify_waiters();
    }

    async fn wait(&self) {
        loop {
            let notified = self.notify.notified();
            if self.state.load(Ordering::Acquire) != OUTBOUND_DIAL_PENDING {
                return;
            }
            notified.await;
        }
    }
}

enum OutboundDialLease {
    Leader(Arc<OutboundDialGate>),
    Follower(Arc<OutboundDialGate>),
}

struct OutboundDialGateCompletion<'a, T = ()> {
    pool: &'a ConnectionPool<T>,
    addr: SocketAddr,
    gate: Arc<OutboundDialGate>,
    finished: bool,
}

impl<'a, T> OutboundDialGateCompletion<'a, T> {
    fn new(pool: &'a ConnectionPool<T>, addr: SocketAddr, gate: Arc<OutboundDialGate>) -> Self {
        Self {
            pool,
            addr,
            gate,
            finished: false,
        }
    }

    fn finish(&mut self, succeeded: bool) {
        if self.finished {
            return;
        }
        self.pool
            .finish_outbound_dial_gate(self.addr, &self.gate, succeeded);
        self.finished = true;
    }
}

impl<T> Drop for OutboundDialGateCompletion<'_, T> {
    fn drop(&mut self) {
        if !self.finished {
            self.pool
                .finish_outbound_dial_gate(self.addr, &self.gate, false);
        }
    }
}
