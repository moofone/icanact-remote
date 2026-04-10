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
    /// Shared correlation trackers by peer ID - ensures ask/response works across bidirectional connections
    correlation_trackers: SccHashMap<crate::PeerId, Arc<CorrelationTracker>>,
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
    _marker: PhantomData<fn() -> T>,
}
