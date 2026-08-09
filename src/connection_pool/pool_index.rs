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
    /// Ownership table for outstanding `connection_counter` contributions,
    /// keyed by stream-handle `instance_id`. Presence of a key IS the fact
    /// "this instance's count is still outstanding and un-released" — set by
    /// `mark_instance_counted` at the exact moment (and only at the moment)
    /// an instance's count is actually added, consumed by
    /// `release_counted_instance`'s single atomic `remove_sync`. This lets
    /// any caller who only has an `instance_id` (a socket-failure handler, an
    /// aborted IO task's exit guard) determine and consume ownership
    /// correctly without ever needing to resolve the instance through an
    /// index that may already have discarded it — and makes releases
    /// exactly-once regardless of which teardown path notices a given
    /// instance first, closing both the double-decrement (underflow) and
    /// leaked-count classes structurally rather than by convention.
    counted_instances: SccHashMap<u64, ()>,
    /// Cold-path dial ownership gate keyed by address so concurrent callers share one outbound dial.
    outbound_dial_gates: SccHashMap<SocketAddr, Arc<OutboundDialGate>>,
    max_connections: usize,
    connection_timeout: Duration,
    /// Registry reference for handling incoming messages
    registry: ArcSwapWeak<GossipRegistry>,
    /// Shared aligned bytes pool for zero-copy receive buffers
    aligned_bytes_pool: Arc<crate::AlignedBytesPool>,
    /// Connection counter for load balancing.
    ///
    /// Signed, not `AtomicUsize`: every count-in site pairs its increment
    /// with a `counted_instances` marker mutation (see
    /// [`ConnectionPool::count_in_new_instance`] /
    /// [`ConnectionPool::release_counted_instance`]), and those two sides can
    /// observably run in either order under a concurrent teardown racing a
    /// fresh count-in for the same instance. At a baseline of zero, a release
    /// that wins the race decrements before the paired increment lands,
    /// which needs to be representable as transiently negative so the
    /// following `+1` nets back to exactly zero. An unsigned counter cannot
    /// represent that and must clamp (losing the paired decrement forever,
    /// see `decrement_connection_counter`'s history) or wrap to a huge
    /// positive value (falsely tripping the `max_connections` admission
    /// gate). `isize` lets the transient dip read as a small negative number
    /// instead — safe for the `>= max_connections` admission check, which
    /// only cares about "at or over the cap", never "did we dip below zero".
    connection_counter: AtomicIsize,
    /// Test hook. Fired inside [`ConnectionPool::cleanup_stale_connections`]
    /// after it snapshots which peers look stale but before it acts on that
    /// snapshot, so a test can land a concurrent reconnect deterministically
    /// in the check-then-act gap instead of racing for it — the same
    /// technique as `OutboundDialGate`'s `race_hook`.
    #[cfg(test)]
    cleanup_stale_race_hook: std::sync::Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
    /// Test hook. Fired inside [`ConnectionPool::retire_orphaned_stale_instance`]
    /// immediately after one of its address aliases is removed from
    /// `connections_by_addr`, but before the corresponding
    /// `addr_to_peer_id`/capability cleanup for that same address — lands a
    /// concurrent, unrelated connection publishing at that exact address in
    /// the gap between the two, deterministically instead of racing for it.
    #[cfg(test)]
    retire_orphan_metadata_race_hook: std::sync::Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
    _marker: PhantomData<fn() -> T>,
}

struct PeerSession {
    /// Updated when a caller acquires this session. A disconnected,
    /// non-required session can be reclaimed only after this grace period.
    last_touched: std::sync::Mutex<Instant>,
    route_addr: std::sync::RwLock<Option<SocketAddr>>,
    required_addr: std::sync::RwLock<Option<SocketAddr>>,
    required_peer: AtomicBool,
    correlation: Arc<CorrelationTracker>,
    current_connection: ArcSwapOption<LockFreeConnection>,
    /// Consecutive consumer-classified streak-timeouts for this peer. Lives on
    /// the session (which survives reconnects) so the streak is genuinely
    /// per-peer. Reset on a successful ask or on eviction.
    consecutive_ask_timeouts: AtomicU8,
    outbound_dial_retry: OutboundDialRetry,
}

const OUTBOUND_DIAL_RETRY_FLOOR: Duration = Duration::from_secs(1);

/// Per-peer cold-path dial cadence. The state lives on `PeerSession`, so all
/// callers share it across failed connection instances without retaining an
/// address-keyed dial gate forever.
struct OutboundDialRetry {
    // LOCK-RATIONALE: failed-dial control path only; never read on a live
    // connection or message hot path. Eligibility and reservation must be one
    // atomic state transition across every address known for this peer.
    state: std::sync::Mutex<OutboundDialRetryState>,
    retry_floor: Duration,
}

struct OutboundDialRetryState {
    consecutive_failures: u8,
    retry_not_before: Option<Instant>,
    generation: u64,
}

#[derive(Clone, Copy)]
struct OutboundDialAttempt {
    generation: u64,
}

impl OutboundDialRetry {
    fn new() -> Self {
        Self::with_retry_floor(OUTBOUND_DIAL_RETRY_FLOOR)
    }

    fn with_retry_floor(retry_floor: Duration) -> Self {
        Self {
            state: std::sync::Mutex::new(OutboundDialRetryState {
                consecutive_failures: 0,
                retry_not_before: None,
                generation: 0,
            }),
            retry_floor,
        }
    }

    fn try_claim_attempt(&self) -> Option<OutboundDialAttempt> {
        let now = Instant::now();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state
            .retry_not_before
            .is_some_and(|deadline| now < deadline)
        {
            return None;
        }

        // Reserve this peer's slot while the socket attempt is in flight. If
        // the future is cancelled, the bounded reservation expires by itself.
        state.generation = state.generation.wrapping_add(1);
        state.retry_not_before = Some(now + self.retry_floor);
        Some(OutboundDialAttempt {
            generation: state.generation,
        })
    }

    fn record_failure(&self, attempt: OutboundDialAttempt) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.generation != attempt.generation {
            return;
        }
        state.consecutive_failures = state.consecutive_failures.saturating_add(1);
        let delay = if state.consecutive_failures == 1 {
            Duration::ZERO
        } else {
            self.retry_floor
        };
        state.retry_not_before = Some(Instant::now() + delay);
    }

    fn record_success(&self, attempt: OutboundDialAttempt) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.generation != attempt.generation {
            return;
        }
        state.generation = state.generation.wrapping_add(1);
        state.consecutive_failures = 0;
        state.retry_not_before = None;
    }

    fn record_neutral(&self, attempt: OutboundDialAttempt) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.generation != attempt.generation {
            return;
        }
        state.generation = state.generation.wrapping_add(1);
        state.retry_not_before = None;
    }

    fn record_published_connection(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Publication wins over every in-flight completion, including an
        // older attempt whose bounded reservation has already expired.
        state.generation = state.generation.wrapping_add(1);
        state.consecutive_failures = 0;
        state.retry_not_before = None;
    }
}

impl PeerSession {
    fn new() -> Self {
        Self {
            last_touched: std::sync::Mutex::new(Instant::now()),
            route_addr: std::sync::RwLock::new(None),
            required_addr: std::sync::RwLock::new(None),
            required_peer: AtomicBool::new(false),
            correlation: CorrelationTracker::new(),
            current_connection: ArcSwapOption::empty(),
            consecutive_ask_timeouts: AtomicU8::new(0),
            outbound_dial_retry: OutboundDialRetry::new(),
        }
    }

    fn touch(&self) {
        *self
            .last_touched
            .lock()
            .expect("peer session last_touched poisoned") = Instant::now();
    }

    fn idle_for(&self, ttl: Duration) -> bool {
        self.last_touched
            .lock()
            .expect("peer session last_touched poisoned")
            .elapsed()
            >= ttl
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
        self.route_addr().or_else(|| self.required_addr())
    }

    fn set_configured_addr(&self, addr: SocketAddr) {
        *self
            .route_addr
            .write()
            .expect("peer session route_addr poisoned") = Some(addr);
    }

    fn clear_route_addr_if(&self, addr: SocketAddr) {
        let mut route = self
            .route_addr
            .write()
            .expect("peer session route_addr poisoned");
        if *route == Some(addr) {
            *route = None;
        }
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

    /// Atomically clear the current connection iff it is still exactly
    /// `expected` (`Arc::ptr_eq`) — a single lock-free CAS on the slot
    /// itself via `ArcSwapOption::compare_and_swap`.
    ///
    /// This is deliberately NOT `read (ptr_eq check) -> store(None)` as two
    /// separate steps: that idiom has a genuine gap between the check and
    /// the store in which a concurrent `publish_current_peer_connection`
    /// (e.g. a fresh preferred inbound landing mid-teardown) can install a
    /// new `Arc`, which the second, unconditional step would then clobber.
    /// A single CAS closes that gap completely: it either finds `expected`
    /// still installed and atomically swaps it for `None`, or it finds
    /// something else and leaves the slot untouched — there is no
    /// observable state in between.
    fn compare_and_clear_current_connection(&self, expected: &Arc<LockFreeConnection>) -> bool {
        let previous = self.current_connection.compare_and_swap(expected, None);
        matches!(&*previous, Some(prev) if Arc::ptr_eq(prev, expected))
    }

    /// Like [`Self::compare_and_clear_current_connection`], but exposes what
    /// was actually found in the slot when the CAS declines, via the same
    /// single atomic CAS (no separate read).
    ///
    /// `Err(Some(other))` means a DIFFERENT connection is genuinely
    /// installed as current — a real concurrent supersession the caller must
    /// not touch. `Err(None)` means the slot was already empty: `expected`
    /// was never the peer's "current" session at all — e.g. a decision
    /// snapshot found it only via an address/alias fallback
    /// (`ConnectionPool::peer_current_connection_snapshot`) without ever
    /// promoting it there. That is NOT a concurrent supersession — nothing
    /// is being protected by declining — so a caller evicting `expected` by
    /// its own instance identity may safely proceed with that eviction
    /// either way.
    fn compare_and_take_current_connection(
        &self,
        expected: &Arc<LockFreeConnection>,
    ) -> std::result::Result<(), Option<Arc<LockFreeConnection>>> {
        let previous = self.current_connection.compare_and_swap(expected, None);
        if matches!(&*previous, Some(prev) if Arc::ptr_eq(prev, expected)) {
            Ok(())
        } else {
            Err((*previous).clone())
        }
    }

    /// Publish-side counterpart to `compare_and_clear_current_connection`:
    /// atomically install `new` as the current connection iff the slot is
    /// still exactly `expected` — `None` meaning "still empty", `Some(arc)`
    /// meaning "still holding that exact `Arc`" — via a single lock-free CAS
    /// on the underlying `ArcSwapOption`.
    ///
    /// This closes the outbound-finalize publish gap: a decision computed
    /// against a snapshot (`expected`) taken before this candidate was
    /// indexed must never be enacted by blindly overwriting whatever is
    /// installed *now*. A concurrent `publish_current_peer_connection` (a
    /// fresh preferred inbound landing in the gap between that snapshot and
    /// this call) leaves a different `Arc` in the slot; the CAS then finds a
    /// mismatch and declines, returning the connection actually installed so
    /// the caller can re-resolve the tie-break against reality instead of
    /// clobbering it. There is no observable check-then-act gap: either the
    /// slot still holds `expected` and is atomically swapped for `new`, or
    /// it holds something else and is left completely untouched.
    fn compare_and_set_current_connection(
        &self,
        expected: Option<&Arc<LockFreeConnection>>,
        new: Arc<LockFreeConnection>,
    ) -> std::result::Result<(), Option<Arc<LockFreeConnection>>> {
        let expected_owned: Option<Arc<LockFreeConnection>> = expected.cloned();
        let previous = self
            .current_connection
            .compare_and_swap(&expected_owned, Some(new));
        let matched = match (&expected_owned, &*previous) {
            (None, None) => true,
            (Some(exp), Some(prev)) => Arc::ptr_eq(exp, prev),
            _ => false,
        };
        if matched {
            Ok(())
        } else {
            Err((*previous).clone())
        }
    }
}

const OUTBOUND_DIAL_PENDING: u8 = 0;
const OUTBOUND_DIAL_SUCCEEDED: u8 = 1;
const OUTBOUND_DIAL_FAILED: u8 = 2;
struct OutboundDialGate {
    state: AtomicU8,
    notify: Notify,
    /// R-15 test hook. Fired inside [`OutboundDialGate::wait`] at the exact
    /// vulnerable point — after the state observation, before the await — so a
    /// test can drive `finish()` into the `Notified` registration gap
    /// deterministically instead of racing for it.
    #[cfg(test)]
    race_hook: std::sync::Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
}

impl OutboundDialGate {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(OUTBOUND_DIAL_PENDING),
            notify: Notify::new(),
            #[cfg(test)]
            race_hook: std::sync::Mutex::new(None),
        }
    }

    #[cfg(test)]
    fn set_race_hook(&self, hook: impl Fn() + Send + Sync + 'static) {
        *self.race_hook.lock().expect("race hook mutex poisoned") = Some(Box::new(hook));
    }

    /// Fires the R-15 hook at most once, without holding the lock across the
    /// callback (the callback re-enters `finish()` on this same gate).
    #[cfg(test)]
    fn fire_race_hook(&self) {
        let hook = self
            .race_hook
            .lock()
            .expect("race hook mutex poisoned")
            .take();
        if let Some(hook) = hook {
            hook();
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
            // R-15. The QA finding claimed a permanent lost wakeup here. That
            // specific claim was WRONG: `Notified` captures `Notify`'s
            // `notify_waiters_calls` generation counter at *construction* time
            // (tokio notify.rs `notified()`), and `finish()` broadcasts with
            // `notify_waiters()`, which bumps that counter — so constructing
            // `notified` before the state load was already sufficient to
            // observe a `finish()` landing in the load -> await window.
            //
            // But that correctness depended entirely on an undocumented
            // statement ordering: move the construction below the load and the
            // wakeup IS lost permanently, and the follower branch of
            // `get_connection*` has no timeout, so the dial hangs forever.
            //
            // `enable()` registers the waiter up front, which makes the wakeup
            // safe regardless of where the state load sits — the same
            // enable()-before-recheck discipline the write queues use in
            // `constants.rs`. Those queues genuinely need it, because they wake
            // with permit-based `notify_one()`, which has no generation
            // counter to fall back on.
            let mut notified = std::pin::pin!(self.notify.notified());
            notified.as_mut().enable();
            if self.state.load(Ordering::Acquire) != OUTBOUND_DIAL_PENDING {
                return;
            }
            #[cfg(test)]
            self.fire_race_hook();
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
