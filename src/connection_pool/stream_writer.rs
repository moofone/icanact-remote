/// Maximum streaming bytes offered before the connection owner returns to its
/// read side. Ordinary tell/ask commands never use this slice machinery.
const STREAM_WRITE_SLICE_BYTES: usize = 64 * 1024;

async fn write_vectored_all<S>(
    stream: &mut S,
    slices: &[std::io::IoSlice<'_>],
) -> std::io::Result<usize>
where
    S: AsyncWrite + Unpin,
{
    let total_len: usize = slices.iter().map(|slice| slice.len()).sum();
    if total_len == 0 {
        return Ok(0);
    }
    if !stream.is_write_vectored() {
        for slice in slices {
            stream.write_all(slice.as_ref()).await?;
        }
        return Ok(total_len);
    }

    let written = stream.write_vectored(slices).await?;
    if written == total_len {
        return Ok(written);
    }
    let mut index = 0usize;
    let mut offset = written;
    while index < slices.len() && offset >= slices[index].len() {
        offset -= slices[index].len();
        index += 1;
    }
    if index < slices.len() {
        if offset < slices[index].len() {
            stream.write_all(&slices[index].as_ref()[offset..]).await?;
            index += 1;
        }
        while index < slices.len() {
            stream.write_all(slices[index].as_ref()).await?;
            index += 1;
        }
    }
    Ok(total_len)
}

/// Perform at most one socket write for a streaming slice. A short write is
/// returned to the caller, which retains the command offset for the next turn;
/// this avoids awaiting the remainder of a large frame while the peer's read
/// side is constrained.
async fn write_vectored_once<S>(
    stream: &mut S,
    slices: &[std::io::IoSlice<'_>],
) -> std::io::Result<usize>
where
    S: AsyncWrite + Unpin,
{
    let Some(first) = slices.iter().find(|slice| !slice.is_empty()) else {
        return Ok(0);
    };
    let result = if stream.is_write_vectored() {
        stream.write_vectored(slices).await
    } else {
        std::future::poll_fn(|cx| Pin::new(&mut *stream).poll_write(cx, first.as_ref())).await
    }?;
    if result == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WriteZero,
            "streaming write made no progress",
        ));
    }
    Ok(result)
}

async fn write_streaming_command_slice<S>(
    stream: &mut S,
    pending: &mut PendingStreamingCommand,
) -> std::io::Result<(usize, bool)>
where
    S: AsyncWrite + Unpin,
{
    let offset = pending.offset;
    let (written, total_len) = match &pending.command {
        StreamingCommand::WriteBytes(data) => {
            if offset >= data.len() {
                return Ok((0, true));
            }
            let end = (offset + STREAM_WRITE_SLICE_BYTES).min(data.len());
            let written = write_vectored_once(
                stream,
                &[std::io::IoSlice::new(&data[offset..end])],
            )
            .await?;
            (written, data.len())
        }
        StreamingCommand::Flush => {
            stream.flush().await?;
            return Ok((0, true));
        }
        StreamingCommand::Abort { stream_id, reason } => {
            let header = crate::framing::write_stream_abort_header(*stream_id, *reason);
            if offset >= header.len() {
                return Ok((0, true));
            }
            let end = (offset + STREAM_WRITE_SLICE_BYTES).min(header.len());
            let written = write_vectored_once(
                stream,
                &[std::io::IoSlice::new(&header[offset..end])],
            )
            .await?;
            (written, header.len())
        }
        StreamingCommand::VectoredWrite(command) => {
            let header = command.header.as_slice();
            let total_len = header.len() + command.payload.len();
            if offset >= total_len {
                return Ok((0, true));
            }
            let budget = STREAM_WRITE_SLICE_BYTES.min(total_len - offset);
            let mut slices = [std::io::IoSlice::new(&[]), std::io::IoSlice::new(&[])];
            let slice_count;
            if offset < header.len() {
                let header_end = (offset + budget).min(header.len());
                slices[0] = std::io::IoSlice::new(&header[offset..header_end]);
                let header_bytes = header_end - offset;
                if header_bytes < budget {
                    slices[1] =
                        std::io::IoSlice::new(&command.payload[..budget - header_bytes]);
                    slice_count = 2;
                } else {
                    slice_count = 1;
                }
            } else {
                let payload_offset = offset - header.len();
                slices[0] = std::io::IoSlice::new(
                    &command.payload[payload_offset..payload_offset + budget],
                );
                slice_count = 1;
            }
            (
                write_vectored_once(stream, &slices[..slice_count]).await?,
                total_len,
            )
        }
        StreamingCommand::OwnedChunks(chunks) => {
            let total_len: usize = chunks.iter().map(|chunk| chunk.len()).sum();
            if offset >= total_len {
                return Ok((0, true));
            }
            let mut skip = offset;
            let mut remaining = STREAM_WRITE_SLICE_BYTES.min(total_len - offset);
            const MAX_IOV: usize = 64;
            let mut storage: [MaybeUninit<std::io::IoSlice<'_>>; MAX_IOV] = unsafe {
                MaybeUninit::<[MaybeUninit<std::io::IoSlice<'_>>; MAX_IOV]>::uninit()
                    .assume_init()
            };
            let mut count = 0usize;
            for chunk in chunks {
                if skip >= chunk.len() {
                    skip -= chunk.len();
                    continue;
                }
                let start = skip;
                skip = 0;
                let take = remaining.min(chunk.len() - start);
                storage[count].write(std::io::IoSlice::new(&chunk[start..start + take]));
                count += 1;
                remaining -= take;
                if remaining == 0 || count == MAX_IOV {
                    break;
                }
            }
            let slices = unsafe {
                std::slice::from_raw_parts(
                    storage.as_ptr() as *const std::io::IoSlice<'_>,
                    count,
                )
            };
            let written = write_vectored_once(stream, slices).await?;
            (written, total_len)
        }
    };
    pending.offset += written;
    Ok((written, pending.offset == total_len))
}

#[inline]
fn finish_streaming_command_slice(
    pending: PendingStreamingCommand,
    complete: bool,
    streaming_queue: &StreamingQueue,
    pending_slot: &mut Option<PendingStreamingCommand>,
) {
    if complete {
        if pending.from_shared_queue {
            streaming_queue.notify_space();
        }
    } else {
        *pending_slot = Some(pending);
    }
}

/// Truly lock-free streaming handle with dedicated background writer
#[derive(Clone)]
pub struct LockFreeStreamHandle {
    /// Unique per-handle id used to ignore disconnect callbacks from stale connections.
    instance_id: u64,
    addr: SocketAddr,
    channel_id: ChannelId,
    sequence_counter: Arc<AtomicUsize>,
    /// Direction-local stream IDs. IDs never wrap on a live connection.
    next_stream_id: Arc<AtomicU32>,
    bytes_written: Arc<AtomicUsize>, // This tracks actual TCP bytes written
    shutdown_signal: Arc<AtomicBool>,
    exit_flag: Arc<AtomicBool>,
    exit_notify: Arc<Notify>,
    /// Set by a caller that is authoritatively retiring THIS instance while
    /// its `correlation` tracker is still shared with a live sibling
    /// instance for the same peer (see
    /// `LockFreeConnection::abort_tasks_keep_correlation`). The IO task's
    /// `ExitGuard` checks this before its own peer/addr-based supersession
    /// inference, which can otherwise lag the caller's already-published
    /// decision by a check-then-act window.
    known_superseded: Arc<AtomicBool>,
    flush_pending: Arc<AtomicBool>,
    /// Atomic flag for coordinating streaming mode. Read by the IO task and
    /// `is_streaming_active` as a cheap observability signal; the actual mutual
    /// exclusion is enforced by `stream_gate`. True while a stream's frames
    /// are being queued onto the wire (gate held); an ask that has finished
    /// queueing its frames releases the gate and clears this flag *before*
    /// awaiting its response (T3, 2026-07-17 QA).
    streaming_active: Arc<AtomicBool>,
    /// ACTOR_REM_2 R16e: single-permit gate serializing `stream_large_message` /
    /// `stream_response` on this handle. Losers park on the permit instead of
    /// busy-spinning a failed CAS + `yield_now` for the winner's entire stream
    /// duration (which starved the loser and burned a scheduler slot per poll).
    stream_gate: Arc<tokio::sync::Semaphore>,
    /// Outbound route bindings for this exact transport instance.  The gate
    /// keeps a newly enqueued RouteBind immediately ahead of its first ask.
    outbound_routes: Arc<crate::route_interning::RouteTable>,
    route_bind_gate: Arc<tokio::sync::Mutex<()>>,
    /// Lock-free write queue for payload writes
    write_queue: Arc<WriteQueue>,
    /// Reserved lock-free queue for latency-critical control replies. This is
    /// intentionally independent from `write_queue`: ordinary traffic must
    /// not consume the admission capacity needed for a control-plane reply.
    immediate_write_queue: Arc<WriteQueue>,
    /// Bounded streaming command queue for background task.
    streaming_queue: Arc<StreamingQueue>,
    /// Buffer configuration that determines sizes and thresholds
    buffer_config: BufferConfig,
    /// Max allowed frame payload size (msg_len, excluding 4-byte length prefix).
    ///
    /// This comes from the registry config and is used to bound streaming chunk sizes so
    /// streaming frames themselves never exceed the reader limit.
    max_message_size: usize,
    /// Optional schema/version hash for protocol guardrails.
    schema_hash: Option<u64>,
    /// Gates `write_routed_actor_ask` (and so `RouteBind`) behind a fresh
    /// outbound connection's own identifying `FullSync` being enqueued
    /// first. `true` (not gated) for every handle by default; only
    /// `finalize_new_outbound_connection` ever arms this via
    /// `begin_identify_gate`, immediately after construction and before
    /// the handle is shared with anything that could enqueue onto it.
    identify_ready: Arc<AtomicBool>,
}

impl LockFreeStreamHandle {
    fn next_instance_id() -> u64 {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    }

    pub fn instance_id(&self) -> u64 {
        self.instance_id
    }

    /// Arm the identify gate: `write_routed_actor_ask` on this handle will
    /// park in [`Self::wait_until_identified`] until [`Self::mark_identified`]
    /// is called. Only ever called by `finalize_new_outbound_connection`,
    /// immediately after construction and before this handle is shared with
    /// anything that could enqueue onto it -- so there is no window in
    /// which a caller could observe the default (not gated) state and race
    /// ahead of the arm.
    pub(crate) fn begin_identify_gate(&self) {
        self.identify_ready.store(false, Ordering::Release);
    }

    /// Signal that this connection's identifying frame has been enqueued.
    /// Wakes any `write_routed_actor_ask` parked in
    /// [`Self::wait_until_identified`] so it can now enqueue behind it.
    pub(crate) fn mark_identified(&self) {
        self.identify_ready.store(true, Ordering::Release);
        self.exit_notify.notify_waiters();
    }

    /// Park until this handle is either identified or dead. A freshly
    /// gated outbound connection's `write_routed_actor_ask` -- and so its
    /// `RouteBind` -- must never reach the write queue ahead of the
    /// identifying frame; this is the wait side of that guarantee, keyed on
    /// the exact same `exit_notify` every teardown path (`abort_tasks`,
    /// the IO task's own `ExitGuard`) already wakes on, so a connection
    /// that fails or tears down before ever being identified wakes any
    /// waiter with an error instead of hanging it forever. Returns
    /// immediately for every handle except one gated by
    /// [`Self::begin_identify_gate`] that has not yet been identified.
    async fn wait_until_identified(&self) -> Result<()> {
        loop {
            // Subscribe before checking: a `notify_waiters()` that lands
            // between the check and a bare `.await` would otherwise be
            // missed. `Notified` created here observes any notification
            // from this point on, even one delivered before it is polled.
            let notified = self.exit_notify.notified();
            if self.identify_ready.load(Ordering::Acquire) {
                return Ok(());
            }
            if self.exit_flag.load(Ordering::Acquire) {
                return Err(GossipError::ConnectionClosed(self.addr));
            }
            notified.await;
        }
    }

    fn allocate_stream_id(&self) -> Result<u32> {
        // R-5: per-handle stream ids take the ODD partition (1, 3, 5, ...) —
        // disjoint from the process-global direct-response allocator's EVEN
        // ids — so a direct streaming response and a handle-initiated stream on
        // the same connection can never collide on `stream_id` (a collision
        // keys two live streams to one id and tears the connection down as a
        // duplicate start).
        //
        // A CAS loop (not fetch_add) so exhaustion shuts the connection down at
        // the u32::MAX sentinel WITHOUT wrapping the counter back to 1 — a wrap
        // would hand out id 1 again on a still-live connection (stream-id
        // reuse). u32::MAX is the odd sentinel (never handed out); the last id
        // returned is u32::MAX - 2. Each odd id is handed out at most once.
        loop {
            let current = self.next_stream_id.load(Ordering::Acquire);
            if current == u32::MAX {
                self.signal_shutdown();
                return Err(GossipError::Shutdown);
            }
            // current is odd and in [1, u32::MAX - 2]; +2 stays odd and <= u32::MAX.
            let next = current + 2;
            if self
                .next_stream_id
                .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(current);
            }
        }
    }

    /// Create a new lock-free streaming handle with background writer task
    ///
    /// Returns the handle, the spawned IO task, and an optional reader task.
    pub fn new<S>(
        stream: S,
        addr: SocketAddr,
        channel_id: ChannelId,
        buffer_config: BufferConfig,
        schema_hash: Option<u64>,
        read_context: Option<ReadContext>,
    ) -> (Self, JoinHandle<()>, Option<JoinHandle<()>>)
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let instance_id = Self::next_instance_id();
        let shutdown_signal = Arc::new(AtomicBool::new(false));
        let streaming_active = Arc::new(AtomicBool::new(false));
        let stream_gate = Arc::new(tokio::sync::Semaphore::new(1));
        let outbound_routes = Arc::new(crate::route_interning::RouteTable::new());
        let route_bind_gate = Arc::new(tokio::sync::Mutex::new(()));
        let flush_pending = Arc::new(AtomicBool::new(false));
        let exit_flag = Arc::new(AtomicBool::new(false));
        let exit_notify = Arc::new(Notify::new());
        let known_superseded = Arc::new(AtomicBool::new(false));

        // Create shared counter for actual TCP bytes written
        let bytes_written = Arc::new(AtomicUsize::new(0));

        // Keep immediate control replies in a separate bounded queue. Normal
        // traffic can saturate its own queue without rejecting a critical reply.
        let write_queue = WriteQueue::new(buffer_config.write_queue_capacity(), addr);
        let immediate_write_queue = WriteQueue::new(128, addr);
        let streaming_queue = StreamingQueue::new(buffer_config.write_queue_capacity(), addr);

        let max_message_size = read_context
            .as_ref()
            .map(|ctx| ctx.max_message_size)
            .unwrap_or(MASTER_BUFFER_SIZE);
        // Spawn background writer task with exclusive TCP access - NO MUTEX!
        let writer_handle = {
            let shutdown_signal = shutdown_signal.clone();
            let bytes_written_for_task = bytes_written.clone();
            let streaming_active_for_task = streaming_active.clone();
            let flush_pending_for_task = flush_pending.clone();
            let exit_flag_for_task = exit_flag.clone();
            let exit_notify_for_task = exit_notify.clone();
            let known_superseded_for_task = known_superseded.clone();
            let writer_addr = addr;
            let writer_channel_id = channel_id;
            let write_queue = write_queue.clone();
            let immediate_write_queue = immediate_write_queue.clone();
            let streaming_queue = streaming_queue.clone();

            tokio::spawn(async move {
                info!(
                    addr = %writer_addr,
                    channel_id = ?writer_channel_id,
                    "🚀 Background writer task started"
                );
                Self::io_task(
                    stream,
                    shutdown_signal,
                    bytes_written_for_task,
                    flush_pending_for_task,
                    streaming_active_for_task,
                    write_queue,
                    immediate_write_queue,
                    streaming_queue,
                    read_context,
                    instance_id,
                    exit_flag_for_task,
                    exit_notify_for_task,
                    known_superseded_for_task,
                )
                .await;
                // CRITICAL: Log when writer exits - this helps diagnose silent writer deaths
                warn!(
                    addr = %writer_addr,
                    channel_id = ?writer_channel_id,
                    "⚠️ Background writer task EXITED - no more writes possible on this connection!"
                );
            })
        };

        (
            Self {
                instance_id,
                addr,
                channel_id,
                sequence_counter: Arc::new(AtomicUsize::new(0)),
                next_stream_id: Arc::new(AtomicU32::new(1)),
                bytes_written, // This now tracks actual TCP bytes written
                shutdown_signal,
                flush_pending,
                exit_flag,
                exit_notify,
                known_superseded,
                streaming_active,
                stream_gate,
                outbound_routes,
                route_bind_gate,
                write_queue,
                immediate_write_queue,
                streaming_queue,
                buffer_config,
                max_message_size,
                schema_hash,
                identify_ready: Arc::new(AtomicBool::new(true)),
            },
            writer_handle,
            None,
        )
    }

    /// Single IO task - owns the TLS stream for both read and write.
    /// OPTIMIZED FOR MAXIMUM THROUGHPUT - NO MUTEX NEEDED!
    #[allow(clippy::too_many_arguments)]
    async fn io_task<S>(
        stream: S,
        shutdown_signal: Arc<AtomicBool>,
        bytes_written_counter: Arc<AtomicUsize>, // Track ALL bytes written to TCP
        flush_pending: Arc<AtomicBool>,
        streaming_active: Arc<AtomicBool>,
        write_queue: Arc<WriteQueue>,
        immediate_write_queue: Arc<WriteQueue>,
        streaming_queue: Arc<StreamingQueue>,
        read_context: Option<ReadContext>,
        instance_id: u64,
        exit_flag: Arc<AtomicBool>,
        exit_notify: Arc<Notify>,
        known_superseded: Arc<AtomicBool>,
    ) where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        // CRITICAL_PATH: owner-batched send queue + vectored TLS writes.
        use std::io::IoSlice;

        async fn flush_inline32_batch<S>(
            stream: &mut S,
            headers: &mut Vec<[u8; 32]>,
            payloads: &mut Vec<bytes::Bytes>,
        ) -> std::io::Result<usize>
        where
            S: AsyncWrite + Unpin,
        {
            if headers.is_empty() {
                return Ok(0);
            }

            let mut total_len = 0usize;
            const MAX_IOV: usize = OWNER_BATCH_SIZE * 2;
            let mut storage: [MaybeUninit<IoSlice<'_>>; MAX_IOV] = unsafe {
                MaybeUninit::<[MaybeUninit<IoSlice<'_>>; MAX_IOV]>::uninit().assume_init()
            };

            let count = headers.len().min(payloads.len());
            for idx in 0..count {
                storage[idx * 2].write(IoSlice::new(&headers[idx]));
                storage[idx * 2 + 1].write(IoSlice::new(&payloads[idx]));
                total_len += headers[idx].len() + payloads[idx].len();
            }

            let slices = unsafe {
                std::slice::from_raw_parts(storage.as_ptr() as *const IoSlice<'_>, count * 2)
            };
            let written = write_vectored_all(stream, slices).await?;
            headers.clear();
            payloads.clear();
            debug_assert_eq!(written, total_len);
            Ok(written)
        }

        async fn flush_direct_ask_batch<S>(
            stream: &mut S,
            headers: &mut Vec<[u8; 16]>,
            payloads: &mut Vec<bytes::Bytes>,
        ) -> std::io::Result<usize>
        where
            S: AsyncWrite + Unpin,
        {
            if headers.is_empty() {
                return Ok(0);
            }

            let mut total_len = 0usize;
            const MAX_IOV: usize = OWNER_BATCH_SIZE * 2;
            let mut storage: [MaybeUninit<IoSlice<'_>>; MAX_IOV] = unsafe {
                MaybeUninit::<[MaybeUninit<IoSlice<'_>>; MAX_IOV]>::uninit().assume_init()
            };

            let count = headers.len().min(payloads.len());
            for idx in 0..count {
                storage[idx * 2].write(IoSlice::new(&headers[idx]));
                storage[idx * 2 + 1].write(IoSlice::new(&payloads[idx]));
                total_len += headers[idx].len() + payloads[idx].len();
            }

            let slices = unsafe {
                std::slice::from_raw_parts(storage.as_ptr() as *const IoSlice<'_>, count * 2)
            };
            let written = write_vectored_all(stream, slices).await?;
            headers.clear();
            payloads.clear();
            debug_assert_eq!(written, total_len);
            Ok(written)
        }

        fn read_batch_limit_for(result: &ReadIoResult) -> usize {
            match result {
                ReadIoResult::DirectAsk { .. } => ASK_READ_BATCH_LIMIT,
                ReadIoResult::ActorAsk { .. } => ASK_READ_BATCH_LIMIT,
                ReadIoResult::Generic(crate::handle::MessageReadResult::Actor {
                    msg_type, ..
                }) if *msg_type == crate::MessageType::ActorAsk as u8 => ASK_READ_BATCH_LIMIT,
                ReadIoResult::Generic(crate::handle::MessageReadResult::DirectAsk { .. })
                | ReadIoResult::Generic(crate::handle::MessageReadResult::DirectResponse {
                    ..
                })
                | ReadIoResult::Generic(crate::handle::MessageReadResult::Response { .. }) => {
                    ASK_READ_BATCH_LIMIT
                }
                _ => READ_BATCH_LIMIT,
            }
        }

        struct ExitGuard {
            flag: Arc<AtomicBool>,
            notify: Arc<Notify>,
            // Writer-owned queues. On teardown we must wake any sender parked in
            // `push()` on a full queue, otherwise it hangs forever (the queue's
            // space notifier is only fired by `pop()`, which has stopped).
            write_queue: Arc<WriteQueue>,
            immediate_write_queue: Arc<WriteQueue>,
            streaming_queue: Arc<StreamingQueue>,
            response_correlation: Option<Arc<CorrelationTracker>>,
            registry_weak: Option<std::sync::Weak<GossipRegistry>>,
            peer_addr: Option<SocketAddr>,
            peer_id: Option<crate::PeerId>,
            instance_id: u64,
            known_superseded: Arc<AtomicBool>,
        }

        impl Drop for ExitGuard {
            fn drop(&mut self) {
                self.flag.store(true, Ordering::Release);
                self.notify.notify_waiters();
                // Wake parked `push()` callers so they observe `closed` and
                // return `ConnectionClosed` rather than hanging on a full queue.
                self.write_queue.mark_closed_and_wake();
                self.immediate_write_queue.mark_closed_and_wake();
                self.streaming_queue.mark_closed_and_wake();
                // Authoritative signal from a caller that is deliberately
                // retiring this exact instance while its correlation tracker
                // is still shared with a live sibling (e.g.
                // `retire_displaced_expected`/`unpublish_rejected_outbound_candidate`
                // via `abort_tasks_keep_correlation`). Trust it ahead of the
                // peer/addr-based inference below, which reads pool state
                // this same caller may have already mutated (or not yet
                // re-published) by the time this Drop runs.
                let mut superseded = self.known_superseded.load(Ordering::Acquire);
                let mut should_cancel_pending = !superseded;
                if let (Some(registry_weak), Some(peer_addr)) =
                    (self.registry_weak.as_ref(), self.peer_addr)
                    && let Some(registry) = registry_weak.upgrade()
                {
                    // Guard against mis-attributing disconnects from stale/duplicate
                    // connections. Tie-breaker drops and replacements are expected and
                    // must not mark the peer failed or cancel pending asks on the current link.
                    let expected_instance = self.instance_id;
                    let peer_id_hint = self.peer_id.clone();
                    let pool = &registry.connection_pool;
                    let peer_id = peer_id_hint.or_else(|| pool.get_peer_id_by_addr(&peer_addr));

                    // Skip re-deriving supersession when `known_superseded`
                    // already settled it above: this inference reads pool
                    // state the SAME caller that set that flag may have
                    // already mutated (its own address-alias sweep) or not
                    // yet re-published (a sibling's own indexing step can
                    // still be pending), so it must never downgrade an
                    // authoritative "yes" back to "no".
                    if !superseded
                        && let Some(peer_id) = peer_id.as_ref()
                        && let Some(current) = pool.get_connection_by_peer_id(peer_id)
                        && let Some(handle) = current.stream_handle.as_ref()
                        && handle.instance_id() != expected_instance
                    {
                        superseded = true;
                        debug!(
                            peer = %peer_addr,
                            peer_id = %peer_id,
                            exiting_instance = expected_instance,
                            current_instance = handle.instance_id(),
                            "IO task exited for stale connection; skipping pending cancel/failure handling"
                        );
                    } else if !superseded
                        && peer_id.is_none()
                        && let Some(current) = pool.get_lock_free_connection(peer_addr)
                        && let Some(handle) = current.stream_handle.as_ref()
                        && handle.instance_id() != expected_instance
                    {
                        superseded = true;
                        debug!(
                            peer = %peer_addr,
                            exiting_instance = expected_instance,
                            current_instance = handle.instance_id(),
                            "IO task exited for stale addr-mapped connection; skipping pending cancel/failure handling"
                        );
                    }

                    if superseded {
                        should_cancel_pending = false;
                        // The exiting IO task's OWN instance is superseded —
                        // the peer's current session (or, address-only, the
                        // addr-indexed session) is a DIFFERENT stream
                        // instance. `should_cancel_pending = false` above
                        // suppresses pending-request cancellation and
                        // peer-wide failure accounting/consensus/gossip
                        // signalling, which must never fire for a superseded
                        // exit. It must NOT, however, suppress retiring the
                        // exiting instance's own bookkeeping: this call is
                        // the sole production caller that can retire it, and
                        // skipping it here leaves a zombie
                        // `connections_by_addr` alias plus a leaked
                        // `connection_counter` contribution forever (nothing
                        // else will ever find this instance again once it is
                        // no longer indexed by address). This goes straight
                        // to the pool's instance-scoped cleanup rather than
                        // through `GossipRegistry::handle_peer_connection_failure`:
                        // that function's own superseded-instance branch is
                        // reached only when `peer_id` is resolvable; for the
                        // address-only branch above (`peer_id` unknown) it
                        // instead falls through to its unconditional
                        // peer-wide tail (marks the peer failed, may trigger
                        // consensus) — exactly the accounting this path must
                        // never fire. Calling the pool method directly is
                        // correct and synchronous for both branches, and
                        // never touches the current (winning) session: the
                        // compare-and-remove inside is keyed on
                        // `expected_instance`'s own identity.
                        let retired =
                            pool.remove_connection_instance_by_id(peer_addr, expected_instance);
                        if retired.is_none() {
                            // Not found at `peer_addr` — either a fresh
                            // reconnect already reindexed that address, or a
                            // different teardown path (e.g. a concurrent
                            // `ReplaceExisting` tie-break's
                            // `disconnect_connection_instance`) already
                            // retired this exact instance. Route through the
                            // shared ownership table rather than
                            // decrementing unconditionally: it releases the
                            // compensating count exactly once if it is still
                            // outstanding, and is a safe no-op both when
                            // another path already released it and when this
                            // instance (a rejected outbound candidate that
                            // `unpublish_rejected_outbound_candidate` aborted
                            // without ever counting it) was never counted at
                            // all — which is exactly what prevents this
                            // fallback from underflowing `connection_counter`
                            // for a candidate that never bumped it.
                            pool.release_displaced_connection_count(expected_instance);
                        }
                    }

                    if should_cancel_pending {
                        warn!(
                            peer = %peer_addr,
                            peer_id = ?peer_id,
                            stream_instance_id = expected_instance,
                            "transport_io_task_exit_current_connection"
                        );
                        if let Some(correlation) = self.response_correlation.as_ref() {
                            correlation.cancel_all();
                        }
                        tokio::spawn(async move {
                            if let Err(e) = registry
                                .handle_peer_connection_failure(peer_addr, Some(expected_instance))
                                .await
                            {
                                warn!(
                                    peer = %peer_addr,
                                    error = %e,
                                    "IO task failure handling failed"
                                );
                            }
                        });
                    }
                    return;
                }

                if superseded {
                    return;
                }
                warn!(
                    peer = ?self.peer_addr,
                    peer_id = ?self.peer_id,
                    stream_instance_id = self.instance_id,
                    "transport_io_task_exit_without_registry"
                );
                if let Some(correlation) = self.response_correlation.as_ref() {
                    correlation.cancel_all();
                }
            }
        }

        let _exit_guard = ExitGuard {
            flag: exit_flag,
            notify: exit_notify,
            write_queue: write_queue.clone(),
            immediate_write_queue: immediate_write_queue.clone(),
            streaming_queue: streaming_queue.clone(),
            response_correlation: read_context
                .as_ref()
                .and_then(|ctx| ctx.response_correlation.clone()),
            registry_weak: read_context.as_ref().map(|ctx| ctx.registry_weak.clone()),
            peer_addr: read_context.as_ref().map(|ctx| ctx.peer_addr),
            peer_id: read_context.as_ref().and_then(|ctx| ctx.peer_id.clone()),
            instance_id,
            known_superseded,
        };

        let perf = if IoPerfCounters::enabled() {
            Some(IoPerfCounters::global())
        } else {
            None
        };
        let mut perf_last = Instant::now();
        let perf_interval = perf
            .map(|_| IoPerfCounters::interval())
            .unwrap_or_else(|| Duration::from_secs(1));
        if perf.is_some() {
            if let Some(ctx) = read_context.as_ref() {
                info!(peer = %ctx.peer_addr, "IO PERF enabled");
            } else {
                info!("IO PERF enabled (no read context)");
            }
        }

        let mut stream = stream;

        // Larger batches for higher throughput (reduced syscalls)
        const OWNER_BATCH_SIZE: usize = 64;
        const READ_BATCH_LIMIT: usize = 2048;
        const ASK_READ_BATCH_LIMIT: usize = 8192;
        // Large stream slices yield to the normal queue and socket read side.
        const STREAM_ACTIVE_WRITE_BATCH: usize = 16;
        // Drain a bounded priority burst before regular traffic. Admission is
        // reserved by `immediate_write_queue`; this budget preserves regular
        // traffic progress if control replies arrive continuously.
        const IMMEDIATE_WRITE_BATCH: usize = 8;

        let mut bytes_since_flush = 0;

        // Pre-allocate reusable buffers to avoid allocations in the hot loop
        let mut write_chunks: Vec<bytes::Bytes> = Vec::with_capacity(OWNER_BATCH_SIZE * 2);
        let mut owner_batch: Vec<WriteCommand> = Vec::with_capacity(OWNER_BATCH_SIZE);
        let mut direct_ask_headers: Vec<[u8; 16]> = Vec::with_capacity(OWNER_BATCH_SIZE);
        let mut direct_ask_payloads: Vec<bytes::Bytes> = Vec::with_capacity(OWNER_BATCH_SIZE);
        let mut inline32_headers: Vec<[u8; 32]> = Vec::with_capacity(OWNER_BATCH_SIZE);
        let mut inline32_payloads: Vec<bytes::Bytes> = Vec::with_capacity(OWNER_BATCH_SIZE);
        let mut response_batch = ResponseBatch::new(READ_BATCH_LIMIT);
        let mut direct_response_batch = DirectResponseBatch::new(READ_BATCH_LIMIT);
        let mut pending_cmd: Option<WriteCommand> = None;
        let mut pending_immediate_cmd: Option<WriteCommand> = None;
        let mut pending_stream_cmd: Option<PendingStreamingCommand> = None;
        let mut local_streaming_queue = LocalStreamingQueue::new();
        let mut read_state = read_context.as_ref().map(|_| ReadState::new());
        let mut streaming_state = match read_context
            .as_ref()
            .and_then(|ctx| ctx.streaming_state_handoff.as_ref())
        {
            // R-6: inherit the accept path's first-frame StreamingState so a
            // multi-chunk StreamStart that began as the connection's first
            // frame is continued here, not rejected as "unknown stream_id"
            // against a fresh state. Race the handoff against a shutdown check
            // so a cancelled/errored accept path (or connection shutdown during
            // the wait) cannot hang the IO task forever; the accept path
            // notifies on success AND on its error path, and this loop is the
            // defensive backstop. On shutdown/no-handoff fall back to a fresh
            // state and let the main loop's shutdown check / read error exit.
            Some(handoff) => {
                loop {
                    tokio::select! {
                        _ = handoff.ready.notified() => break,
                        _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {
                            if shutdown_signal.load(Ordering::Acquire) {
                                break;
                            }
                        }
                    }
                }
                Some(
                    handoff
                        .cell
                        .lock()
                        .ok()
                        .and_then(|mut g| g.take())
                        .unwrap_or_else(crate::protocol::StreamingState::new),
                )
            }
            None => read_context
                .as_ref()
                .map(|_| crate::protocol::StreamingState::new()),
        };
        let mut last_cleanup = std::time::Instant::now();

        while !shutdown_signal.load(Ordering::Acquire) {
            let mut total_bytes_written = 0;
            let mut did_work = false;
            let mut wrote_ask_payload = false;
            let mut wrote_actor_responses = false;
            let mut wrote_fast_responses = false;
            response_batch.clear();
            direct_response_batch.clear();

            // Complete one bounded piece of the current frame before handling
            // normal writes and inbound reads. A partial frame stays ahead of
            // every other streaming command, preserving wire framing.
            if let Some(mut pending) = pending_stream_cmd
                .take()
                .or_else(|| {
                    local_streaming_queue
                        .pop_front()
                        .map(PendingStreamingCommand::local)
                })
                .or_else(|| {
                    streaming_queue
                        .pop()
                        .map(PendingStreamingCommand::shared)
                })
            {
                did_work = true;
                let command_is_flush = matches!(&pending.command, StreamingCommand::Flush);
                let (written, complete) =
                    match write_streaming_command_slice(&mut stream, &mut pending).await {
                        Ok(result) => result,
                        Err(error) => {
                            error!(%error, "streaming write error");
                            return;
                        }
                    };
                bytes_written_counter.fetch_add(written, Ordering::Relaxed);
                total_bytes_written += written;
                if command_is_flush {
                    flush_pending.store(false, Ordering::Release);
                    bytes_since_flush = 0;
                }
                finish_streaming_command_slice(
                    pending,
                    complete,
                    &streaming_queue,
                    &mut pending_stream_cmd,
                );
            }

            // A partial streaming frame owns the wire until complete. Reads may
            // still run below, but no normal/response write can interleave and
            // corrupt the frame boundary.
            if pending_stream_cmd.is_none() {
                // ACTOR_REM_2 R8: service the normal write queue when no partial
                // stream frame is outstanding. The batch remains bounded during
                // active streaming so control traffic is not starved.
                {
                let normal_batch_limit = if streaming_active.load(Ordering::Acquire) {
                    STREAM_ACTIVE_WRITE_BATCH
                } else {
                    OWNER_BATCH_SIZE
                };
                // Reuse pre-allocated buffers instead of creating new ones
                write_chunks.clear();
                owner_batch.clear();
                inline32_headers.clear();
                inline32_payloads.clear();

                // A priority command that raced the idle select still leads
                // the next batch. Drain only a bounded burst so the normal
                // queue cannot be starved by a sustained control flood.
                if let Some(cmd) = pending_immediate_cmd.take() {
                    owner_batch.push(cmd);
                }

                while owner_batch.len() < IMMEDIATE_WRITE_BATCH {
                    match immediate_write_queue.pop() {
                        Some(command) => owner_batch.push(command),
                        None => break,
                    }
                }

                if let Some(cmd) = pending_cmd.take() {
                    owner_batch.push(cmd);
                }

                let regular_batch_end = owner_batch.len() + normal_batch_limit;
                while owner_batch.len() < regular_batch_end {
                    match write_queue.pop() {
                        Some(command) => owner_batch.push(command),
                        None => break,
                    }
                }

                if !owner_batch.is_empty() {
                    did_work = true;
                    for command in owner_batch.drain(..) {
                        let is_ask_payload = matches!(&command, WriteCommand::AskPayload(_));
                        let is_immediate_payload =
                            matches!(&command, WriteCommand::ImmediatePayload(_));
                        if is_immediate_payload {
                            immediate_write_queue.notify_space();
                        } else {
                            write_queue.notify_space();
                        }
                        let payload = match command {
                            WriteCommand::Payload(payload)
                            | WriteCommand::ImmediatePayload(payload) => payload,
                            WriteCommand::AskPayload(payload) => {
                                wrote_ask_payload = true;
                                payload
                            }
                        };
                        let ask_write_start = if is_ask_payload && perf.is_some() {
                            Some(Instant::now())
                        } else {
                            None
                        };
                        if !matches!(&payload, WritePayload::DirectAskInline { .. })
                            && !direct_ask_headers.is_empty()
                        {
                            let bytes_written = match flush_direct_ask_batch(
                                &mut stream,
                                &mut direct_ask_headers,
                                &mut direct_ask_payloads,
                            )
                            .await
                            {
                                Ok(n) => n,
                                Err(_) => return,
                            };
                            bytes_written_counter.fetch_add(bytes_written, Ordering::Relaxed);
                            total_bytes_written += bytes_written;
                        }
                        match payload {
                            WritePayload::Single(data) => write_chunks.push(data),
                            WritePayload::HeaderPayload { header, payload } => {
                                if !direct_ask_headers.is_empty() {
                                    let bytes_written = match flush_direct_ask_batch(
                                        &mut stream,
                                        &mut direct_ask_headers,
                                        &mut direct_ask_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                                if !inline32_headers.is_empty() {
                                    let bytes_written = match flush_inline32_batch(
                                        &mut stream,
                                        &mut inline32_headers,
                                        &mut inline32_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                                write_chunks.push(header);
                                write_chunks.push(payload);
                            }
                            WritePayload::HeaderInline {
                                header,
                                header_len,
                                payload,
                            } => {
                                if !direct_ask_headers.is_empty() {
                                    let bytes_written = match flush_direct_ask_batch(
                                        &mut stream,
                                        &mut direct_ask_headers,
                                        &mut direct_ask_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                                if !inline32_headers.is_empty() {
                                    let bytes_written = match flush_inline32_batch(
                                        &mut stream,
                                        &mut inline32_headers,
                                        &mut inline32_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                                if !write_chunks.is_empty() {
                                    let bytes_written = match write_chunks_batched(
                                        &mut stream,
                                        &write_chunks,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                    write_chunks.clear();
                                }

                                let header_len = header_len as usize;
                                let mut header_off = 0usize;
                                let mut payload_off = 0usize;
                                let payload_len = payload.len();

                                while header_off < header_len || payload_off < payload_len {
                                    let h = &header[header_off..header_len];
                                    let p = &payload[payload_off..];
                                    let mut slices = [IoSlice::new(h), IoSlice::new(p)];
                                    let slice_count = if h.is_empty() {
                                        slices[0] = IoSlice::new(p);
                                        1
                                    } else if p.is_empty() {
                                        slices[0] = IoSlice::new(h);
                                        1
                                    } else {
                                        2
                                    };

                                    match write_vectored_all(&mut stream, &slices[..slice_count])
                                        .await
                                    {
                                        Ok(0) => break,
                                        Ok(n) => {
                                            bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                            total_bytes_written += n;
                                            if header_off < header_len {
                                                let h_rem = header_len - header_off;
                                                if n < h_rem {
                                                    header_off += n;
                                                    continue;
                                                } else {
                                                    header_off = header_len;
                                                    payload_off += n - h_rem;
                                                }
                                            } else {
                                                payload_off += n;
                                            }
                                        }
                                        Err(_) => return,
                                    }
                                }
                            }
                            WritePayload::HeaderInlineAligned {
                                header,
                                header_len,
                                payload,
                            } => {
                                if !direct_ask_headers.is_empty() {
                                    let bytes_written = match flush_direct_ask_batch(
                                        &mut stream,
                                        &mut direct_ask_headers,
                                        &mut direct_ask_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                                if !inline32_headers.is_empty() {
                                    let bytes_written = match flush_inline32_batch(
                                        &mut stream,
                                        &mut inline32_headers,
                                        &mut inline32_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                                if !write_chunks.is_empty() {
                                    let bytes_written = match write_chunks_batched(
                                        &mut stream,
                                        &write_chunks,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                    write_chunks.clear();
                                }

                                let header_len = header_len as usize;
                                let mut header_off = 0usize;
                                let mut payload_off = 0usize;
                                let payload_len = payload.len();
                                let payload_bytes = payload.as_ref();

                                while header_off < header_len || payload_off < payload_len {
                                    let h = &header[header_off..header_len];
                                    let p = &payload_bytes[payload_off..];
                                    let mut slices = [IoSlice::new(h), IoSlice::new(p)];
                                    let slice_count = if h.is_empty() {
                                        slices[0] = IoSlice::new(p);
                                        1
                                    } else if p.is_empty() {
                                        slices[0] = IoSlice::new(h);
                                        1
                                    } else {
                                        2
                                    };

                                    match write_vectored_all(&mut stream, &slices[..slice_count])
                                        .await
                                    {
                                        Ok(0) => break,
                                        Ok(n) => {
                                            bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                            total_bytes_written += n;
                                            if header_off < header_len {
                                                let h_rem = header_len - header_off;
                                                if n < h_rem {
                                                    header_off += n;
                                                    continue;
                                                } else {
                                                    header_off = header_len;
                                                    payload_off += n - h_rem;
                                                }
                                            } else {
                                                payload_off += n;
                                            }
                                        }
                                        Err(_) => return,
                                    }
                                }
                            }
                            WritePayload::HeaderInline32 { header, payload } => {
                                if !direct_ask_headers.is_empty() {
                                    let bytes_written = match flush_direct_ask_batch(
                                        &mut stream,
                                        &mut direct_ask_headers,
                                        &mut direct_ask_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                                if !write_chunks.is_empty() {
                                    let bytes_written = match write_chunks_batched(
                                        &mut stream,
                                        &write_chunks,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                    write_chunks.clear();
                                }
                                inline32_headers.push(header);
                                inline32_payloads.push(payload);
                                if inline32_headers.len() == OWNER_BATCH_SIZE {
                                    let bytes_written = match flush_inline32_batch(
                                        &mut stream,
                                        &mut inline32_headers,
                                        &mut inline32_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                            }
                            WritePayload::HeaderPooled {
                                header,
                                prefix,
                                mut payload,
                            } => {
                                if !direct_ask_headers.is_empty() {
                                    let bytes_written = match flush_direct_ask_batch(
                                        &mut stream,
                                        &mut direct_ask_headers,
                                        &mut direct_ask_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                                if !inline32_headers.is_empty() {
                                    let bytes_written = match flush_inline32_batch(
                                        &mut stream,
                                        &mut inline32_headers,
                                        &mut inline32_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                                if !write_chunks.is_empty() {
                                    const MAX_IOV: usize = 64;
                                    // Use drain to preserve buffer capacity
                                    let mut idx = 0;
                                    let mut iov: [MaybeUninit<IoSlice<'_>>; MAX_IOV] = unsafe {
                                        MaybeUninit::<[MaybeUninit<IoSlice<'_>>; MAX_IOV]>::uninit()
                                            .assume_init()
                                    };

                                    for chunk in &write_chunks {
                                        iov[idx].write(IoSlice::new(&chunk));
                                        idx += 1;
                                        if idx == MAX_IOV {
                                            let slices = unsafe {
                                                std::slice::from_raw_parts(
                                                    iov.as_ptr() as *const IoSlice<'_>,
                                                    idx,
                                                )
                                            };
                                            match write_vectored_all(&mut stream, slices).await {
                                                Ok(bytes_written) => {
                                                    bytes_written_counter.fetch_add(
                                                        bytes_written,
                                                        Ordering::Relaxed,
                                                    );
                                                    total_bytes_written += bytes_written;
                                                }
                                                Err(_) => return,
                                            }
                                            idx = 0;
                                        }
                                    }

                                    if idx > 0 {
                                        let slices = unsafe {
                                            std::slice::from_raw_parts(
                                                iov.as_ptr() as *const IoSlice<'_>,
                                                idx,
                                            )
                                        };
                                        match write_vectored_all(&mut stream, slices).await {
                                            Ok(bytes_written) => {
                                                bytes_written_counter
                                                    .fetch_add(bytes_written, Ordering::Relaxed);
                                                total_bytes_written += bytes_written;
                                            }
                                            Err(_) => return,
                                        }
                                    }
                                    write_chunks.clear();
                                }

                                if (stream.write_all(&header).await).is_err() {
                                    return;
                                }
                                bytes_written_counter.fetch_add(header.len(), Ordering::Relaxed);
                                total_bytes_written += header.len();

                                if let Some(prefix) = prefix {
                                    if (stream.write_all(&prefix).await).is_err() {
                                        return;
                                    }
                                    bytes_written_counter
                                        .fetch_add(prefix.len(), Ordering::Relaxed);
                                    total_bytes_written += prefix.len();
                                }

                                while payload.has_remaining() {
                                    match stream.write_buf(&mut payload).await {
                                        Ok(0) => return, // R-7: WriteZero mid-frame -> teardown (break would drop the remaining payload and desync the wire)
                                        Ok(n) => {
                                            bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                            total_bytes_written += n;
                                        }
                                        Err(_) => return,
                                    }
                                }
                            }
                            WritePayload::HeaderInlinePooled {
                                header,
                                header_len,
                                prefix,
                                prefix_len,
                                mut payload,
                            } => {
                                if !direct_ask_headers.is_empty() {
                                    let bytes_written = match flush_direct_ask_batch(
                                        &mut stream,
                                        &mut direct_ask_headers,
                                        &mut direct_ask_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                                if !inline32_headers.is_empty() {
                                    let bytes_written = match flush_inline32_batch(
                                        &mut stream,
                                        &mut inline32_headers,
                                        &mut inline32_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                                if !write_chunks.is_empty() {
                                    // Use drain to preserve buffer capacity
                                    let mut slices = Vec::with_capacity(write_chunks.len());
                                    for chunk in &write_chunks {
                                        slices.push(IoSlice::new(&chunk));
                                    }
                                    match write_vectored_all(&mut stream, &slices).await {
                                        Ok(bytes_written) => {
                                            bytes_written_counter
                                                .fetch_add(bytes_written, Ordering::Relaxed);
                                            total_bytes_written += bytes_written;
                                        }
                                        Err(_) => return,
                                    }
                                }

                                let header_len = header_len as usize;
                                let prefix_len = prefix_len as usize;
                                let mut header_off = 0usize;
                                let mut prefix_off = 0usize;

                                if let Some(prefix) = prefix {
                                    while header_off < header_len || prefix_off < prefix_len {
                                        let h = &header[header_off..header_len];
                                        let p = &prefix[prefix_off..prefix_len];
                                        let mut slices = [IoSlice::new(h), IoSlice::new(p)];
                                        let slice_count = if h.is_empty() {
                                            slices[0] = IoSlice::new(p);
                                            1
                                        } else if p.is_empty() {
                                            slices[0] = IoSlice::new(h);
                                            1
                                        } else {
                                            2
                                        };

                                        match write_vectored_all(
                                            &mut stream,
                                            &slices[..slice_count],
                                        )
                                        .await
                                        {
                                            Ok(0) => break,
                                            Ok(n) => {
                                                bytes_written_counter
                                                    .fetch_add(n, Ordering::Relaxed);
                                                total_bytes_written += n;
                                                if header_off < header_len {
                                                    let h_rem = header_len - header_off;
                                                    if n < h_rem {
                                                        header_off += n;
                                                        continue;
                                                    } else {
                                                        header_off = header_len;
                                                        prefix_off += n - h_rem;
                                                    }
                                                } else {
                                                    prefix_off += n;
                                                }
                                            }
                                            Err(_) => return,
                                        }
                                    }
                                } else {
                                    while header_off < header_len {
                                        let h = &header[header_off..header_len];
                                        match write_vectored_all(&mut stream, &[IoSlice::new(h)])
                                            .await
                                        {
                                            Ok(0) => break,
                                            Ok(n) => {
                                                bytes_written_counter
                                                    .fetch_add(n, Ordering::Relaxed);
                                                total_bytes_written += n;
                                                header_off += n;
                                            }
                                            Err(_) => return,
                                        }
                                    }
                                }

                                while payload.has_remaining() {
                                    match stream.write_buf(&mut payload).await {
                                        Ok(0) => return, // R-7: WriteZero mid-frame -> teardown (break would drop the remaining payload and desync the wire)
                                        Ok(n) => {
                                            bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                            total_bytes_written += n;
                                        }
                                        Err(_) => return,
                                    }
                                }
                            }
                            WritePayload::Buf(mut buf) => {
                                if !direct_ask_headers.is_empty() {
                                    let bytes_written = match flush_direct_ask_batch(
                                        &mut stream,
                                        &mut direct_ask_headers,
                                        &mut direct_ask_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                                if !inline32_headers.is_empty() {
                                    let bytes_written = match flush_inline32_batch(
                                        &mut stream,
                                        &mut inline32_headers,
                                        &mut inline32_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                                if !write_chunks.is_empty() {
                                    // Use drain to preserve buffer capacity
                                    let mut slices = Vec::with_capacity(write_chunks.len());
                                    for chunk in &write_chunks {
                                        slices.push(IoSlice::new(&chunk));
                                    }
                                    match write_vectored_all(&mut stream, &slices).await {
                                        Ok(bytes_written) => {
                                            bytes_written_counter
                                                .fetch_add(bytes_written, Ordering::Relaxed);
                                            total_bytes_written += bytes_written;
                                        }
                                        Err(_) => return,
                                    }
                                }

                                while buf.has_remaining() {
                                    match stream.write_buf(&mut buf).await {
                                        Ok(0) => return, // R-7: WriteZero mid-frame -> teardown (break would drop the remaining payload and desync the wire)
                                        Ok(n) => {
                                            bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                            total_bytes_written += n;
                                        }
                                        Err(_) => return,
                                    }
                                }
                            }
                            WritePayload::DirectAskInline { header, payload } => {
                                if !write_chunks.is_empty() {
                                    let bytes_written = match write_chunks_batched(
                                        &mut stream,
                                        &write_chunks,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                    write_chunks.clear();
                                }
                                if !inline32_headers.is_empty() {
                                    let bytes_written = match flush_inline32_batch(
                                        &mut stream,
                                        &mut inline32_headers,
                                        &mut inline32_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                                direct_ask_headers.push(header);
                                direct_ask_payloads.push(payload);
                                if direct_ask_headers.len() == OWNER_BATCH_SIZE {
                                    let bytes_written = match flush_direct_ask_batch(
                                        &mut stream,
                                        &mut direct_ask_headers,
                                        &mut direct_ask_payloads,
                                    )
                                    .await
                                    {
                                        Ok(n) => n,
                                        Err(_) => return,
                                    };
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                            }
                        }
                        if let (Some(perf), Some(start)) = (perf, ask_write_start) {
                            perf.ask_write_calls.fetch_add(1, Ordering::Relaxed);
                            perf.ask_write_ns
                                .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                        }
                        if is_immediate_payload && total_bytes_written > 0 {
                            let _ = stream.flush().await;
                            total_bytes_written = 0;
                            flush_pending.store(false, Ordering::Release);
                        }
                    }
                }

                if !inline32_headers.is_empty() {
                    let bytes_written = match flush_inline32_batch(
                        &mut stream,
                        &mut inline32_headers,
                        &mut inline32_payloads,
                    )
                    .await
                    {
                        Ok(n) => n,
                        Err(_) => return,
                    };
                    bytes_written_counter.fetch_add(bytes_written, Ordering::Relaxed);
                    total_bytes_written += bytes_written;
                }

                if !direct_ask_headers.is_empty() {
                    let bytes_written = match flush_direct_ask_batch(
                        &mut stream,
                        &mut direct_ask_headers,
                        &mut direct_ask_payloads,
                    )
                    .await
                    {
                        Ok(n) => n,
                        Err(_) => return,
                    };
                    bytes_written_counter.fetch_add(bytes_written, Ordering::Relaxed);
                    total_bytes_written += bytes_written;
                }

                if !write_chunks.is_empty() {
                    let bytes_written = match write_chunks_batched(&mut stream, &write_chunks).await
                    {
                        Ok(n) => n,
                        Err(_) => return,
                    };
                    bytes_written_counter.fetch_add(bytes_written, Ordering::Relaxed);
                    total_bytes_written += bytes_written;
                    write_chunks.clear();
                }
                }
            }

            bytes_since_flush += total_bytes_written;
            if bytes_since_flush > 0 {
                let _ = stream.flush().await;
                bytes_since_flush = 0;
                flush_pending.store(false, Ordering::Release);
            }

            if let (Some(ctx), Some(state), Some(streaming_state)) = (
                read_context.as_ref(),
                read_state.as_mut(),
                streaming_state.as_mut(),
            ) {
                if last_cleanup.elapsed() >= std::time::Duration::from_secs(30) {
                    streaming_state.cleanup_stale();
                    last_cleanup = std::time::Instant::now();
                }

                if did_work {
                    let mut reads = 0usize;
                    let mut read_batch_limit = READ_BATCH_LIMIT;
                    while reads < read_batch_limit && !local_streaming_queue.is_full() {
                        // R-I: cap per-turn byte accumulation independent of
                        // the frame-count cap above. Checked every iteration
                        // (covering both the normal and the fast-io `continue`
                        // paths below) so a peer packing its ask window with
                        // max-size response frames cannot force unbounded
                        // memory growth before `read_batch_limit` frames are
                        // seen. See `flush_response_batch_if_over_byte_cap`.
                        if let Err(e) = flush_response_batch_if_over_byte_cap(
                            &mut stream,
                            &bytes_written_counter,
                            &mut bytes_since_flush,
                            &mut response_batch,
                        )
                        .await
                        {
                            warn!(
                                peer = %ctx.peer_addr,
                                error = %e,
                                "Failed to write response batch (byte cap)"
                            );
                            return;
                        }
                        if let Err(e) = flush_direct_response_batch_if_over_byte_cap(
                            &mut stream,
                            &bytes_written_counter,
                            &mut bytes_since_flush,
                            &mut direct_response_batch,
                        )
                        .await
                        {
                            warn!(
                                peer = %ctx.peer_addr,
                                error = %e,
                                "Failed to write direct response batch (byte cap)"
                            );
                            return;
                        }

                        let read_start = perf.map(|_| Instant::now());
                        let read_result =
                            match read_message_step_nonblocking(&mut stream, state, ctx, streaming_state).await {
                                Ok(result) => result,
                                Err(e) => {
                                    warn!(
                                        peer = %ctx.peer_addr,
                                        error = %e,
                                        "IO task read error"
                                    );
                                    return;
                                }
                            };
                        if let (Some(perf), Some(start)) = (perf, read_start) {
                            if read_result.progressed || read_result.result.is_some() {
                                perf.read_calls.fetch_add(1, Ordering::Relaxed);
                                perf.read_ns.fetch_add(
                                    start.elapsed().as_nanos() as u64,
                                    Ordering::Relaxed,
                                );
                            }
                        }

                        if let Some(result) = read_result.result {
                            reads += 1;
                            read_batch_limit = read_batch_limit.max(read_batch_limit_for(&result));
                            let Some(result) = try_handle_fast_io(
                                result,
                                ctx,
                                &mut stream,
                                &bytes_written_counter,
                                &mut bytes_since_flush,
                                &mut response_batch,
                                &mut local_streaming_queue,
                                &mut direct_response_batch,
                                &mut wrote_fast_responses,
                                perf,
                            )
                            .await
                            .unwrap_or_else(|e| {
                                warn!(
                                    peer = %ctx.peer_addr,
                                    error = %e,
                                    "Failed to process fast IO message"
                                );
                                None
                            }) else {
                                continue;
                            };
                            if let Some(registry) = ctx.registry_weak.upgrade() {
                                if let Err(e) = process_read_result_io(
                                    result,
                                    streaming_state,
                                    &registry,
                                    ctx.peer_addr,
                                    ctx.session_source,
                                    ctx.peer_id.as_ref(),
                                    ctx.response_correlation.as_ref().map(|c| c.as_ref()),
                                    ctx.sync_actor_handler.as_ref().map(|v| &**v),
                                    &mut stream,
                                    &bytes_written_counter,
                                    &mut bytes_since_flush,
                                    &mut response_batch,
                                    &mut local_streaming_queue,
                                    &mut direct_response_batch,
                                    perf,
                                )
                                .await
                                {
                                    warn!(
                                        peer = %ctx.peer_addr,
                                        error = %e,
                                        "Failed to process message on IO task"
                                    );
                                }
                            } else {
                                warn!(
                                    peer = %ctx.peer_addr,
                                    "Registry dropped, stopping IO task"
                                );
                                return;
                            }
                        } else if !read_result.progressed {
                            break;
                        }
                    }
                    if !response_batch.is_empty() {
                        if let Err(e) = write_response_batch(
                            &mut stream,
                            &bytes_written_counter,
                            &mut bytes_since_flush,
                            &mut response_batch,
                        )
                        .await
                        {
                            warn!(
                                peer = %ctx.peer_addr,
                                error = %e,
                                "Failed to write response batch"
                            );
                            return;
                        }
                        wrote_actor_responses = true;
                    }
                    if !direct_response_batch.is_empty() {
                        if let Err(e) = write_direct_response_batch(
                            &mut stream,
                            &bytes_written_counter,
                            &mut bytes_since_flush,
                            &mut direct_response_batch,
                        )
                        .await
                        {
                            warn!(
                                peer = %ctx.peer_addr,
                                error = %e,
                                "Failed to write direct response batch"
                            );
                            return;
                        }
                        wrote_fast_responses = true;
                    }
                }
            }

            // Ask RTT fast path: when this loop writes ask requests and/or actor responses,
            // flush immediately to avoid waiting on generic throughput-oriented flush thresholds.
            if (wrote_ask_payload || wrote_actor_responses || wrote_fast_responses)
                && bytes_since_flush > 0
            {
                let _ = stream.flush().await;
                bytes_since_flush = 0;
                flush_pending.store(false, Ordering::Release);
            }

            if !did_work {
                if let (Some(ctx), Some(state), Some(streaming_state)) = (
                    read_context.as_ref(),
                    read_state.as_mut(),
                    streaming_state.as_mut(),
                ) {
                    // Pre-park drain: clear each queue's pending flag and
                    // re-check for a command that raced the last drain. A push
                    // landing after the clear stores a wakeup permit, so the
                    // select below cannot park forever with frames queued.
                    if let Some(cmd) = immediate_write_queue.prepare_park() {
                        pending_immediate_cmd = Some(cmd);
                        continue;
                    }
                    if let Some(cmd) = write_queue.prepare_park() {
                        pending_cmd = Some(cmd);
                        continue;
                    }
                    if let Some(cmd) = streaming_queue.prepare_park() {
                        pending_stream_cmd = Some(PendingStreamingCommand::shared(cmd));
                        continue;
                    }
                    tokio::select! {
                        // Idle path: block waiting for socket readability.
                        read_result = read_message_step_poll(&mut stream, state, ctx, streaming_state, true) => {
                            let read_start = perf.map(|_| Instant::now());
                            let read_result = match read_result {
                                Ok(result) => result,
                                Err(e) => {
                                    warn!(
                                        peer = %ctx.peer_addr,
                                        error = %e,
                                        "IO task read error"
                                    );
                                    return;
                                }
                            };

                            if let (Some(perf), Some(start)) = (perf, read_start) {
                                if read_result.progressed || read_result.result.is_some() {
                                    perf.read_calls.fetch_add(1, Ordering::Relaxed);
                                    perf.read_ns.fetch_add(
                                        start.elapsed().as_nanos() as u64,
                                        Ordering::Relaxed,
                                    );
                                }
                            }

                            if let Some(result) = read_result.result {
                                if let Some(result) = try_handle_fast_io(
                                    result,
                                    ctx,
                                    &mut stream,
                                    &bytes_written_counter,
                                    &mut bytes_since_flush,
                                    &mut response_batch,
                                    &mut local_streaming_queue,
                                    &mut direct_response_batch,
                                    &mut wrote_fast_responses,
                                    perf,
                                )
                                .await
                                .unwrap_or_else(|e| {
                                    warn!(
                                        peer = %ctx.peer_addr,
                                        error = %e,
                                        "Failed to process fast IO message"
                                    );
                                    None
                                }) {
                                    if let Some(registry) = ctx.registry_weak.upgrade() {
                                        if let Err(e) = process_read_result_io(
                                            result,
                                            streaming_state,
                                            &registry,
                                            ctx.peer_addr,
                                            ctx.session_source,
                                            ctx.peer_id.as_ref(),
                                            ctx.response_correlation.as_ref().map(|c| c.as_ref()),
                                            ctx.sync_actor_handler.as_ref().map(|v| &**v),
                                            &mut stream,
                                            &bytes_written_counter,
                                            &mut bytes_since_flush,
                                            &mut response_batch,
                                            &mut local_streaming_queue,
                                            &mut direct_response_batch,
                                            perf,
                                        )
                                        .await
                                        {
                                            warn!(
                                                peer = %ctx.peer_addr,
                                                error = %e,
                                                "Failed to process message on IO task"
                                            );
                                        }
                                    } else {
                                        warn!(
                                            peer = %ctx.peer_addr,
                                            "Registry dropped, stopping IO task"
                                        );
                                        return;
                                    }
                                }
                            }

                            // Under load the socket can become readable with many frames queued.
                            // The old "idle path" processed only a single frame per wake-up,
                            // which inflates RTT and caps ActorAsk throughput on server-heavy links.
                            //
                            // Drain additional frames non-blocking to batch handler + response writes.
                            let mut drained = 0usize;
                            let mut drain_batch_limit = READ_BATCH_LIMIT;
                            while drained < drain_batch_limit && !local_streaming_queue.is_full() {
                                // R-I: same per-turn byte cap as the primary
                                // drain loop above; see
                                // `flush_response_batch_if_over_byte_cap`.
                                if let Err(e) = flush_response_batch_if_over_byte_cap(
                                    &mut stream,
                                    &bytes_written_counter,
                                    &mut bytes_since_flush,
                                    &mut response_batch,
                                )
                                .await
                                {
                                    warn!(
                                        peer = %ctx.peer_addr,
                                        error = %e,
                                        "Failed to write response batch (byte cap)"
                                    );
                                    return;
                                }
                                if let Err(e) = flush_direct_response_batch_if_over_byte_cap(
                                    &mut stream,
                                    &bytes_written_counter,
                                    &mut bytes_since_flush,
                                    &mut direct_response_batch,
                                )
                                .await
                                {
                                    warn!(
                                        peer = %ctx.peer_addr,
                                        error = %e,
                                        "Failed to write direct response batch (byte cap)"
                                    );
                                    return;
                                }

                                let read_start = perf.map(|_| Instant::now());
                                let next = match read_message_step_nonblocking(&mut stream, state, ctx, streaming_state).await {
                                    Ok(r) => r,
                                    Err(e) => {
                                        warn!(
                                            peer = %ctx.peer_addr,
                                            error = %e,
                                            "IO task read error"
                                        );
                                        return;
                                    }
                                };
                                if let (Some(perf), Some(start)) = (perf, read_start) {
                                    if next.progressed || next.result.is_some() {
                                        perf.read_calls.fetch_add(1, Ordering::Relaxed);
                                        perf.read_ns.fetch_add(
                                            start.elapsed().as_nanos() as u64,
                                            Ordering::Relaxed,
                                        );
                                    }
                                }

                                if let Some(result) = next.result {
                                    drained += 1;
                                    drain_batch_limit =
                                        drain_batch_limit.max(read_batch_limit_for(&result));
                                    let Some(result) = try_handle_fast_io(
                                        result,
                                        ctx,
                                        &mut stream,
                                        &bytes_written_counter,
                                        &mut bytes_since_flush,
                                        &mut response_batch,
                                        &mut local_streaming_queue,
                                        &mut direct_response_batch,
                                        &mut wrote_fast_responses,
                                        perf,
                                    )
                                    .await
                                    .unwrap_or_else(|e| {
                                        warn!(
                                            peer = %ctx.peer_addr,
                                            error = %e,
                                            "Failed to process fast IO message"
                                        );
                                        None
                                    }) else {
                                        continue;
                                    };
                                    if let Some(registry) = ctx.registry_weak.upgrade() {
                                        if let Err(e) = process_read_result_io(
                                            result,
                                            streaming_state,
                                            &registry,
                                            ctx.peer_addr,
                                            ctx.session_source,
                                            ctx.peer_id.as_ref(),
                                            ctx.response_correlation.as_ref().map(|c| c.as_ref()),
                                            ctx.sync_actor_handler.as_ref().map(|v| &**v),
                                            &mut stream,
                                            &bytes_written_counter,
                                            &mut bytes_since_flush,
                                            &mut response_batch,
                                            &mut local_streaming_queue,
                                            &mut direct_response_batch,
                                            perf,
                                        )
                                        .await
                                        {
                                            warn!(
                                                peer = %ctx.peer_addr,
                                                error = %e,
                                                "Failed to process message on IO task"
                                            );
                                        }
                                    } else {
                                        warn!(
                                            peer = %ctx.peer_addr,
                                            "Registry dropped, stopping IO task"
                                        );
                                        return;
                                    }
                                } else if !next.progressed {
                                    break;
                                }
                            }

                            if !response_batch.is_empty() {
                                if let Err(e) = write_response_batch(
                                    &mut stream,
                                    &bytes_written_counter,
                                    &mut bytes_since_flush,
                                    &mut response_batch,
                                )
                                .await
                                {
                                    warn!(
                                        peer = %ctx.peer_addr,
                                        error = %e,
                                        "Failed to write response batch"
                                    );
                                    return;
                                }
                            }
                            if !direct_response_batch.is_empty() {
                                if let Err(e) = write_direct_response_batch(
                                    &mut stream,
                                    &bytes_written_counter,
                                    &mut bytes_since_flush,
                                    &mut direct_response_batch,
                                )
                                .await
                                {
                                    warn!(
                                        peer = %ctx.peer_addr,
                                        error = %e,
                                        "Failed to write direct response batch"
                                    );
                                    return;
                                }
                            }
                            // Ensure request/response traffic does not sit in TLS buffers on
                            // quiet links. The idle branch has no outer fast-flush checkpoint
                            // after this select arm, so flush direct/actor responses here.
                            if bytes_since_flush > 0 {
                                let _ = stream.flush().await;
                                bytes_since_flush = 0;
                                flush_pending.store(false, Ordering::Release);
                            }
                        }
                        // Wake on new outbound writes even if the socket is currently idle for reads.
                        // Without this, a mostly-write workload (e.g., initial gossip propagation)
                        // can stall until an unrelated read event occurs.
                        _ = immediate_write_queue.data_notify.notified() => {
                            pending_immediate_cmd = immediate_write_queue.pop();
                        }
                        _ = write_queue.data_notify.notified() => {
                            pending_cmd = write_queue.pop();
                        }
                        _ = immediate_write_queue.space_notify.notified() => {
                            // Producer wakeup only; no action needed.
                        }
                        _ = write_queue.space_notify.notified() => {
                            // Producer wakeup only; no action needed.
                        }
                        _ = streaming_queue.data_notify.notified() => {
                            // Wake on streaming commands; drained at the top of the loop.
                        }
                    }
                } else {
                    // Pre-park drain; see the read-armed variant above.
                    if let Some(cmd) = immediate_write_queue.prepare_park() {
                        pending_immediate_cmd = Some(cmd);
                        continue;
                    }
                    if let Some(cmd) = write_queue.prepare_park() {
                        pending_cmd = Some(cmd);
                        continue;
                    }
                    if let Some(cmd) = streaming_queue.prepare_park() {
                        pending_stream_cmd = Some(PendingStreamingCommand::shared(cmd));
                        continue;
                    }
                    tokio::select! {
                        _ = streaming_queue.data_notify.notified() => {
                            // Wake on streaming commands; drained at the top of the loop.
                        }
                        _ = immediate_write_queue.data_notify.notified() => {
                            pending_immediate_cmd = immediate_write_queue.pop();
                        }
                        _ = write_queue.data_notify.notified() => {
                            pending_cmd = write_queue.pop();
                        }
                        _ = immediate_write_queue.space_notify.notified() => {
                            // Producer wakeup only; no action needed.
                        }
                        _ = write_queue.space_notify.notified() => {
                            // Producer wakeup only; no action needed.
                        }
                    }
                }
            }

            if let Some(perf) = perf {
                if perf_last.elapsed() >= perf_interval {
                    let (
                        read_calls,
                        read_ns,
                        handle_calls,
                        handle_ns,
                        write_calls,
                        write_ns,
                        ask_write_calls,
                        ask_write_ns,
                    ) = perf.snapshot_and_reset();
                    let read_us = read_ns as f64 / 1000.0;
                    let handle_us = handle_ns as f64 / 1000.0;
                    let write_us = write_ns as f64 / 1000.0;
                    let ask_write_us = ask_write_ns as f64 / 1000.0;
                    info!(
                        read_calls,
                        handle_calls,
                        write_calls,
                        ask_write_calls,
                        read_us = read_us,
                        handle_us = handle_us,
                        write_us = write_us,
                        ask_write_us = ask_write_us,
                        read_avg_us = read_us / (read_calls.max(1) as f64),
                        handle_avg_us = handle_us / (handle_calls.max(1) as f64),
                        write_avg_us = write_us / (write_calls.max(1) as f64),
                        ask_write_avg_us = ask_write_us / (ask_write_calls.max(1) as f64),
                        "IO PERF"
                    );
                    perf_last = Instant::now();
                }
            }
        }
    }

    async fn enqueue_write(&self, payload: WritePayload) -> Result<()> {
        if self.exit_flag.load(Ordering::Acquire) {
            return Err(GossipError::ConnectionClosed(self.addr));
        }
        if self.shutdown_signal.load(Ordering::Acquire) {
            return Err(GossipError::Shutdown);
        }
        self.sequence_counter.fetch_add(1, Ordering::Relaxed);
        let command = WriteCommand::Payload(payload);
        match self.write_queue.try_push(command) {
            Ok(()) => {
                self.write_queue.notify_data();
                Ok(())
            }
            Err(command) => self.write_queue.push(command).await,
        }
    }

    async fn enqueue_ask_write(&self, payload: WritePayload) -> Result<()> {
        if self.exit_flag.load(Ordering::Acquire) {
            return Err(GossipError::ConnectionClosed(self.addr));
        }
        if self.shutdown_signal.load(Ordering::Acquire) {
            return Err(GossipError::Shutdown);
        }
        self.sequence_counter.fetch_add(1, Ordering::Relaxed);
        let command = WriteCommand::AskPayload(payload);
        match self.write_queue.try_push(command) {
            Ok(()) => {
                self.write_queue.notify_data();
                Ok(())
            }
            Err(command) => self.write_queue.push(command).await,
        }
    }

    fn enqueue_write_nonblocking(&self, payload: WritePayload) -> Result<()> {
        if self.exit_flag.load(Ordering::Acquire) {
            return Err(GossipError::ConnectionClosed(self.addr));
        }
        if self.shutdown_signal.load(Ordering::Acquire) {
            return Err(GossipError::Shutdown);
        }
        self.sequence_counter.fetch_add(1, Ordering::Relaxed);
        match self.write_queue.try_push(WriteCommand::Payload(payload)) {
            Ok(()) => {
                self.write_queue.notify_data();
                Ok(())
            }
            Err(_) => Err(GossipError::WriteQueueFull),
        }
    }

    fn enqueue_immediate_write_nonblocking(&self, payload: WritePayload) -> Result<()> {
        if self.exit_flag.load(Ordering::Acquire) {
            return Err(GossipError::ConnectionClosed(self.addr));
        }
        if self.shutdown_signal.load(Ordering::Acquire) {
            return Err(GossipError::Shutdown);
        }
        self.sequence_counter.fetch_add(1, Ordering::Relaxed);
        match self
            .immediate_write_queue
            .try_push(WriteCommand::ImmediatePayload(payload))
        {
            Ok(()) => {
                self.immediate_write_queue.notify_data();
                Ok(())
            }
            Err(_) => Err(GossipError::WriteQueueFull),
        }
    }

    pub async fn write_bytes_ask(&self, data: bytes::Bytes) -> Result<()> {
        self.enqueue_ask_write(WritePayload::Single(data)).await
    }

    /// Queue a compact ActorAsk, installing its connection-local route first.
    /// Both frames use the one ordered writer queue; the small gate only spans
    /// enqueueing, so no response wait or network I/O is serialized here.
    /// Falls back to the uncompact `ActorAsk` frame when the route table has
    /// no slot available for a new (actor_id, type_hash) pair.
    pub async fn write_routed_actor_ask(
        &self,
        correlation_id: u32,
        actor_id: u64,
        type_hash: u32,
        payload: bytes::Bytes,
    ) -> Result<()> {
        // A brand-new outbound connection gates this behind its own
        // identifying FullSync (see `begin_identify_gate`/`mark_identified`
        // in `finalize_new_outbound_connection`): a `RouteBind` must never
        // reach the write queue ahead of it, or the acceptor drops the
        // connection outright. No-op (returns immediately) for every other
        // handle, and for this same handle once identified.
        self.wait_until_identified().await?;

        // `route_bind_gate` serializes all routed asks, and `outbound_routes`
        // has no other mutator (the IO/parse path uses the separate inbound
        // table) — this is the serialization of bind publication with routed
        // asks. A second `write_routed_actor_ask` therefore cannot observe a
        // route until this holder drops the gate. On cancellation, locals drop
        // in reverse declaration order: the R-3 bind guard (declared below)
        // runs `remove_unbound` BEFORE `_route_guard` releases the gate, so the
        // next ask sees the route gone and re-binds rather than reusing a
        // stale `needs_bind == false` slot.
        let _route_guard = self.route_bind_gate.lock().await;
        let route = crate::route_interning::RouteKey { actor_id, type_hash };
        let Some((route_slot, needs_bind)) = self.outbound_routes.slot_for(route) else {
            // The connection-local slot space is exhausted (either the
            // MAX_ROUTES_PER_CONNECTION cap, or, vanishingly rarely, the
            // slot-id counter itself). Slots are never unilaterally recycled:
            // the peer only learns a slot is free via an explicit unbind it
            // acknowledges, and no such handshake exists today, so evicting
            // one here could hand a still-referenced id to a new
            // (actor_id, type_hash) and let the peer misroute a compact
            // frame to the wrong actor. Falling back to the uncompact
            // `ActorAsk` frame is always safe instead: it carries actor_id
            // and type_hash directly and needs no connection-local slot at
            // all, so this ask (and every later one on this connection) still
            // succeeds rather than the connection being torn down.
            let header =
                crate::framing::write_actor_ask_header(correlation_id, actor_id, type_hash, payload.len());
            return self
                .write_header_and_payload_ask_inline32(header, payload)
                .await;
        };
        if needs_bind {
            let bind = crate::framing::write_route_bind_header(route_slot, actor_id, type_hash);
            // R-3: arm an RAII rollback for the fresh allocation. If this future
            // is dropped while the bind enqueue below is parked on a full write
            // queue (routine under load — every ask is timeout-wrapped), the
            // guard's Drop removes the route so the next ask re-emits RouteBind
            // instead of sending RoutedActorAsk for a slot the peer never
            // learned ("unknown route slot" -> connection teardown). Disarmed
            // only after the enqueue returns Ok — that is the commit point; the
            // writer task owns retry/teardown beyond it.
            let mut bind_guard = crate::route_interning::UnboundRouteGuard::new(
                self.outbound_routes.as_ref(),
                route_slot,
                route,
            );
            if let Err(error) = self.write_bytes_control(bytes::Bytes::copy_from_slice(&bind)).await {
                // guard drops armed -> remove_unbound(route_slot, route)
                return Err(error);
            }
            bind_guard.disarm();
        }
        let header = crate::framing::write_routed_actor_ask_header(
            correlation_id,
            route_slot,
            payload.len(),
        );
        self.write_header_and_payload_ask_inline(header, 16, payload)
            .await
    }

    pub async fn write_bytes_control(&self, data: bytes::Bytes) -> Result<()> {
        self.enqueue_write(WritePayload::Single(data)).await
    }

    pub async fn write_header_and_payload_control(
        &self,
        header: bytes::Bytes,
        payload: bytes::Bytes,
    ) -> Result<()> {
        self.enqueue_write(WritePayload::HeaderPayload { header, payload })
            .await
    }

    /// Write header + payload inline without permit allocation for tell messages.
    /// This is a fast path for high-throughput tell operations.
    pub async fn write_header_and_payload_control_inline(
        &self,
        header: [u8; 16],
        header_len: u8,
        payload: bytes::Bytes,
    ) -> Result<()> {
        self.enqueue_write(WritePayload::HeaderInline {
            header,
            header_len,
            payload,
        })
        .await
    }

    pub async fn write_header_and_payload_control_inline_aligned(
        &self,
        header: [u8; 16],
        header_len: u8,
        payload: crate::AlignedBytes,
    ) -> Result<()> {
        self.enqueue_write(WritePayload::HeaderInlineAligned {
            header,
            header_len,
            payload,
        })
        .await
    }

    pub async fn write_header_and_payload_control_inline32(
        &self,
        header: [u8; 32],
        payload: bytes::Bytes,
    ) -> Result<()> {
        self.enqueue_write(WritePayload::HeaderInline32 { header, payload })
            .await
    }

    /// Non-blocking variant of `write_header_and_payload_control_inline32`.
    ///
    /// This avoids awaiting on the write queue and is useful for building sync `tell` APIs
    /// on top of the background writer task.
    pub fn write_header_and_payload_control_inline32_nonblocking(
        &self,
        header: [u8; 32],
        payload: bytes::Bytes,
    ) -> Result<()> {
        self.enqueue_write_nonblocking(WritePayload::HeaderInline32 { header, payload })
    }

    /// Non-blocking variant of `write_header_and_payload_control_inline`.
    pub fn write_header_and_payload_control_inline_nonblocking(
        &self,
        header: [u8; 16],
        header_len: u8,
        payload: bytes::Bytes,
    ) -> Result<()> {
        self.enqueue_write_nonblocking(WritePayload::HeaderInline {
            header,
            header_len,
            payload,
        })
    }

    pub fn write_header_and_payload_control_inline_immediate_nonblocking(
        &self,
        header: [u8; 16],
        header_len: u8,
        payload: bytes::Bytes,
    ) -> Result<()> {
        self.enqueue_immediate_write_nonblocking(WritePayload::HeaderInline {
            header,
            header_len,
            payload,
        })
    }

    pub async fn write_header_and_payload_ask(
        &self,
        header: bytes::Bytes,
        payload: bytes::Bytes,
    ) -> Result<()> {
        self.enqueue_ask_write(WritePayload::HeaderPayload { header, payload })
            .await
    }

    /// Write header + payload inline for ask messages.
    pub async fn write_header_and_payload_ask_inline(
        &self,
        header: [u8; 16],
        header_len: u8,
        payload: bytes::Bytes,
    ) -> Result<()> {
        self.enqueue_ask_write(WritePayload::HeaderInline {
            header,
            header_len,
            payload,
        })
        .await
    }

    pub async fn write_header_and_payload_ask_inline32(
        &self,
        header: [u8; 32],
        payload: bytes::Bytes,
    ) -> Result<()> {
        self.enqueue_ask_write(WritePayload::HeaderInline32 { header, payload })
            .await
    }

    /// Write DirectAsk header + payload inline (fast path for direct ask)
    /// Wire format: [length:4][type:1][correlation_id:4][payload_len:4][payload:N]
    pub async fn write_direct_ask_inline(
        &self,
        header: [u8; 16], // DIRECT_ASK_FRAME_HEADER_LEN
        payload: bytes::Bytes,
    ) -> Result<()> {
        self.enqueue_ask_write(WritePayload::DirectAskInline { header, payload })
            .await
    }

    /// Write DirectResponse inline (same format as DirectAsk)
    /// Wire format: [length:4][type:1][correlation_id:4][payload_len:4][payload:N]
    pub async fn write_direct_response_inline(
        &self,
        header: [u8; 16], // DIRECT_RESPONSE_FRAME_HEADER_LEN
        payload: bytes::Bytes,
    ) -> Result<()> {
        // DirectResponse has same wire format as DirectAsk, so reuse the implementation
        self.write_direct_ask_inline(header, payload).await
    }

    pub async fn write_pooled_control(
        &self,
        header: bytes::Bytes,
        prefix: Option<bytes::Bytes>,
        payload: crate::typed::PooledPayload,
    ) -> Result<()> {
        self.enqueue_write(WritePayload::HeaderPooled {
            header,
            prefix,
            payload,
        })
        .await
    }

    pub async fn write_pooled_control_inline(
        &self,
        header: [u8; 16],
        header_len: u8,
        prefix: Option<[u8; 16]>,
        prefix_len: u8,
        payload: crate::typed::PooledPayload,
    ) -> Result<()> {
        self.enqueue_write(WritePayload::HeaderInlinePooled {
            header,
            header_len,
            prefix,
            prefix_len,
            payload,
        })
        .await
    }

    pub fn write_pooled_control_inline_nonblocking(
        &self,
        header: [u8; 16],
        header_len: u8,
        prefix: Option<[u8; 16]>,
        prefix_len: u8,
        payload: crate::typed::PooledPayload,
    ) -> Result<()> {
        self.enqueue_write_nonblocking(WritePayload::HeaderInlinePooled {
            header,
            header_len,
            prefix,
            prefix_len,
            payload,
        })
    }

    pub fn write_pooled_control_inline_immediate_nonblocking(
        &self,
        header: [u8; 16],
        header_len: u8,
        prefix: Option<[u8; 16]>,
        prefix_len: u8,
        payload: crate::typed::PooledPayload,
    ) -> Result<()> {
        self.enqueue_immediate_write_nonblocking(WritePayload::HeaderInlinePooled {
            header,
            header_len,
            prefix,
            prefix_len,
            payload,
        })
    }

    pub async fn write_pooled_ask(
        &self,
        header: bytes::Bytes,
        prefix: Option<bytes::Bytes>,
        payload: crate::typed::PooledPayload,
    ) -> Result<()> {
        self.enqueue_ask_write(WritePayload::HeaderPooled {
            header,
            prefix,
            payload,
        })
        .await
    }

    pub async fn write_pooled_ask_inline(
        &self,
        header: [u8; 16],
        header_len: u8,
        prefix: Option<[u8; 16]>,
        prefix_len: u8,
        payload: crate::typed::PooledPayload,
    ) -> Result<()> {
        self.enqueue_ask_write(WritePayload::HeaderInlinePooled {
            header,
            header_len,
            prefix,
            prefix_len,
            payload,
        })
        .await
    }

    pub async fn write_buf_control<B>(&self, buf: B) -> Result<()>
    where
        B: Buf + Send + 'static,
    {
        self.enqueue_write(WritePayload::Buf(Box::new(buf))).await
    }

    pub async fn write_buf_ask<B>(&self, buf: B) -> Result<()>
    where
        B: Buf + Send + 'static,
    {
        self.enqueue_ask_write(WritePayload::Buf(Box::new(buf)))
            .await
    }

    /// Enqueue bytes for the background writer (non-blocking).
    pub fn write_bytes_nonblocking(&self, data: bytes::Bytes) -> Result<()> {
        self.enqueue_write_nonblocking(WritePayload::Single(data))
    }

    /// Enqueue header + payload without concatenating.
    pub fn write_header_and_payload_nonblocking(
        &self,
        header: bytes::Bytes,
        payload: bytes::Bytes,
    ) -> Result<()> {
        self.enqueue_write_nonblocking(WritePayload::HeaderPayload { header, payload })
    }

    /// Enqueue header + payload (legacy name kept for compatibility).
    pub fn write_header_and_payload_nonblocking_checked(
        &self,
        header: bytes::Bytes,
        payload: bytes::Bytes,
    ) -> Result<()> {
        self.enqueue_write_nonblocking(WritePayload::HeaderPayload { header, payload })
    }

    /// Flush the writer immediately - used for low-latency ask operations
    pub fn flush_immediately(&self) -> Result<()> {
        // Coalesce flush requests to avoid flooding the writer task.
        if self
            .flush_pending
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            match self.streaming_queue.try_push(StreamingCommand::Flush) {
                Ok(()) => Ok(()),
                Err(_) => {
                    // Queue is full (backpressure). Drop the explicit flush request and allow
                    // the writer's periodic/threshold flush to handle it.
                    self.flush_pending.store(false, Ordering::Release);
                    Ok(())
                }
            }
        } else {
            Ok(())
        }
    }

    /// Write data with vectored batching - still no blocking
    pub fn write_vectored_nonblocking(&self, data_chunks: &[&[u8]]) -> Result<()> {
        if data_chunks.is_empty() {
            return Ok(());
        }

        // Use BytesMut for efficient concatenation
        let total_len: usize = data_chunks.iter().map(|chunk| chunk.len()).sum();
        let mut combined_buffer = bytes::BytesMut::with_capacity(total_len);

        for chunk in data_chunks {
            combined_buffer.extend_from_slice(chunk); // ALLOW_COPY
        }

        self.write_bytes_nonblocking(combined_buffer.freeze())
    }

    /// Write large data in chunks to avoid blocking
    pub fn write_chunked_nonblocking(&self, data: &[u8], chunk_size: usize) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        for chunk in data.chunks(chunk_size) {
            let _ = self.write_bytes_nonblocking(
                bytes::Bytes::copy_from_slice(chunk), /* ALLOW_COPY */
            );
        }

        Ok(())
    }

    /// Get channel ID
    pub fn channel_id(&self) -> ChannelId {
        self.channel_id
    }

    /// Get total bytes written
    pub fn bytes_written(&self) -> usize {
        self.bytes_written.load(Ordering::Relaxed)
    }

    /// True when a streaming send/response is in progress and the IO task will avoid
    /// draining the normal payload queue (streaming takes exclusive priority).
    pub fn is_streaming_active(&self) -> bool {
        self.streaming_active.load(Ordering::Acquire)
    }

    /// Get sequence counter
    pub fn sequence_number(&self) -> usize {
        self.sequence_counter.load(Ordering::Relaxed)
    }

    /// Get socket address
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Mark this connection shut down and wake any writer parked in the idle
    /// select so it observes the flag at the next loop iteration. Without the
    /// wake, an idle connection's writer never re-checks `shutdown_signal` and
    /// `wait_for_exit` / `task.await` hangs forever (R-2). The queue data
    /// notifiers are consumer-only, so these permits are never stolen by a
    /// producer (producers wait on `space_notify`).
    fn signal_shutdown(&self) {
        self.shutdown_signal.store(true, Ordering::Release);
        self.write_queue.wake_consumer();
        self.streaming_queue.wake_consumer();
    }

    /// Shutdown the background writer task
    pub fn shutdown(&self) {
        self.signal_shutdown();
    }

    /// Wait until the IO task exits.
    pub async fn wait_for_exit(&self) {
        while !self.exit_flag.load(Ordering::Acquire) {
            self.exit_notify.notified().await;
        }
    }

    /// Get the streaming threshold for this connection
    ///
    /// Returns the threshold above which messages should be streamed rather than
    /// queued through the background writer. This is always derived from the buffer size.
    pub fn streaming_threshold(&self) -> usize {
        self.buffer_config.streaming_threshold()
    }

    pub fn schema_hash(&self) -> Option<u64> {
        self.schema_hash
    }

    fn max_stream_chunk_size(&self) -> Result<usize> {
        // The first request frame has the largest V5 stream metadata header;
        // every chunk must fit even when it is carried by StreamStartData.
        const STREAM_FRAME_OVERHEAD: usize = crate::framing::STREAM_REQUEST_START_HEADER_LEN;

        let max_chunk = self.max_message_size.saturating_sub(STREAM_FRAME_OVERHEAD);
        if max_chunk == 0 {
            return Err(GossipError::InvalidConfig(format!(
                "max_message_size={} too small for streaming (overhead={})",
                self.max_message_size, STREAM_FRAME_OVERHEAD
            )));
        }
        Ok(std::cmp::min(STREAM_CHUNK_SIZE, max_chunk))
    }

    /// ACTOR_REM_2 R16e: acquire exclusive streaming mode on this handle without
    /// busy-spinning. A concurrent streamer parks on the single-permit gate (FIFO,
    /// no CPU burn) instead of looping `compare_exchange` + `yield_now` for the
    /// holder's entire stream. The returned guard releases the permit and clears
    /// the `streaming_active` observability flag on drop.
    async fn acquire_streaming_mode(&self) -> Result<StreamingGuard> {
        let permit = self
            .stream_gate
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| GossipError::Shutdown)?;
        self.streaming_active.store(true, Ordering::Release);
        Ok(StreamingGuard {
            flag: self.streaming_active.clone(),
            _permit: permit,
        })
    }

    /// Canonical owned-Bytes request/tell stream path. Payload bytes are only
    /// sliced for vectored TLS writes; no application payload copy is made.
    pub async fn stream_large_message_bytes(
        &self,
        payload: bytes::Bytes,
        type_hash: u32,
        actor_id: u64,
    ) -> Result<()> {
        if payload.is_empty() {
            return Ok(());
        }
        let chunk_size = self.max_stream_chunk_size()?;
        let _guard = self.acquire_streaming_mode().await?;
        let stream_id = self.allocate_stream_id()?;
        // R-9: reject locally at MAX_STREAM_SIZE — receivers hard-reject a larger
        // stream as a FATAL error, so sending it would tear the connection down.
        if payload.len() > crate::MAX_STREAM_SIZE {
            return Err(GossipError::MessageTooLarge {
                size: payload.len(),
                max: crate::MAX_STREAM_SIZE,
            });
        }
        let total_size = u32::try_from(payload.len()).map_err(|_| GossipError::MessageTooLarge {
            size: payload.len(),
            max: crate::MAX_STREAM_SIZE,
        })?;
        let first_len = payload.len().min(chunk_size);
        self.streaming_queue.push(StreamingCommand::VectoredWrite(VectoredSendItem {
            header: InlineFrameHeader::from_array(crate::framing::write_stream_request_start_header(
                stream_id,
                0,
                total_size,
                actor_id,
                type_hash,
                first_len,
            )),
            payload: payload.slice(..first_len),
        })).await?;
        let mut abort_guard = StreamAbortGuard::new(self, stream_id);
        let mut offset = first_len;
        let mut index = 1u32;
        while offset < payload.len() {
            let end = (offset + chunk_size).min(payload.len());
            self.streaming_queue.push(StreamingCommand::VectoredWrite(VectoredSendItem {
                header: InlineFrameHeader::from_array(crate::framing::write_stream_data_header(
                    false,
                    stream_id,
                    index,
                    end - offset,
                )),
                payload: payload.slice(offset..end),
            })).await?;
            offset = end;
            index = index.checked_add(1).ok_or_else(|| GossipError::Network(
                std::io::Error::new(std::io::ErrorKind::InvalidData, "stream chunk index exhausted"),
            ))?;
        }
        self.streaming_queue.push(StreamingCommand::Flush).await?;
        abort_guard.disarm();
        Ok(())
    }

    /// Copying convenience wrapper for callers that only own a slice.
    pub async fn stream_large_message(
        &self,
        msg: &[u8],
        type_hash: u32,
        actor_id: u64,
    ) -> Result<()> {
        self.stream_large_message_bytes(bytes::Bytes::copy_from_slice(msg), type_hash, actor_id)
            .await
    }

    /// Stream a response back to the caller, using streaming protocol for large payloads.
    /// This is used when a response exceeds the streaming threshold.
    ///
    /// NOTE: This method copies the payload. For zero-copy streaming responses,
    /// use `stream_response_bytes` with pre-owned `Bytes` instead.
    ///
    /// # Arguments
    /// * `payload` - The response payload bytes
    /// * `correlation_id` - The correlation ID from the original request (for response matching)
    pub async fn stream_response(&self, payload: &[u8], correlation_id: u32) -> Result<()> {
        // Convert to Bytes and use zero-copy implementation
        self.stream_response_bytes(bytes::Bytes::copy_from_slice(payload), correlation_id) // ALLOW_COPY
            .await
    }

    /// Stream an owned response using V5 StartData plus zero-copy data chunks.
    pub async fn stream_response_bytes(
        &self,
        payload: bytes::Bytes,
        correlation_id: u32,
    ) -> Result<()> {
        if payload.is_empty() {
            return self.write_response_inline(payload, correlation_id).await;
        }
        let chunk_size = self.max_stream_chunk_size()?;
        let _guard = self.acquire_streaming_mode().await?;
        let stream_id = self.allocate_stream_id()?;
        // R-9: reject locally at MAX_STREAM_SIZE — receivers hard-reject a larger
        // stream as a FATAL error, so sending it would tear the connection down.
        if payload.len() > crate::MAX_STREAM_SIZE {
            return Err(GossipError::MessageTooLarge {
                size: payload.len(),
                max: crate::MAX_STREAM_SIZE,
            });
        }
        let total_size = u32::try_from(payload.len()).map_err(|_| {
            GossipError::MessageTooLarge {
                size: payload.len(),
                max: crate::MAX_STREAM_SIZE,
            }
        })?;
        let first_len = payload.len().min(chunk_size);
        let first_header = crate::framing::write_stream_response_start_header(
            stream_id,
            correlation_id,
            total_size,
            first_len,
        );
        self.streaming_queue
            .push(StreamingCommand::VectoredWrite(VectoredSendItem {
                header: InlineFrameHeader::from_array(first_header),
                payload: payload.slice(..first_len),
            }))
            .await?;
        let mut abort_guard = StreamAbortGuard::new(self, stream_id);
        let mut offset = first_len;
        let mut index = 1u32;
        while offset < payload.len() {
            let end = (offset + chunk_size).min(payload.len());
            let header = crate::framing::write_stream_data_header(
                true,
                stream_id,
                index,
                end - offset,
            );
            self.streaming_queue
            .push(StreamingCommand::VectoredWrite(VectoredSendItem {
                    header: InlineFrameHeader::from_array(header),
                    payload: payload.slice(offset..end),
                }))
                .await?;
            offset = end;
            index = index.checked_add(1).ok_or_else(|| {
                GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "stream chunk index exhausted",
                ))
            })?;
        }
        self.streaming_queue.push(StreamingCommand::Flush).await?;
        abort_guard.disarm();
        Ok(())
    }

    /// Send a response, streaming it when it exceeds the streaming threshold
    /// (R-9) and otherwise using the inline write queue.
    /// Write a response as a single inline Response frame (the <= streaming
    /// threshold path). Shared by `send_response_auto(_bytes)` and the empty
    /// branch of `stream_response_bytes` so the auto-stream decision does not
    /// recurse between them (R-9).
    async fn write_response_inline(
        &self,
        payload: bytes::Bytes,
        correlation_id: u32,
    ) -> Result<()> {
        let header = framing::write_ask_response_header(
            crate::MessageType::Response,
            correlation_id,
            payload.len(),
        );
        self.write_header_and_payload_control_inline(header, 16, payload)
            .await
    }

    pub async fn send_response_auto(
        &self,
        payload: bytes::Bytes,
        correlation_id: u32,
    ) -> Result<()> {
        // R-9: route large deferred replies through streaming, mirroring the
        // immediate-response path. A single inline Response frame above the
        // peer's max_message_size is rejected as MessageTooLarge (teardown),
        // and at >= 2^27 bytes the frame header length check would panic the
        // responding task.
        if payload.len() > self.streaming_threshold() {
            return self.stream_response_bytes(payload, correlation_id).await;
        }
        self.write_response_inline(payload, correlation_id).await
    }

    /// Send a response with owned Bytes, streaming it when it exceeds the
    /// streaming threshold (R-9) and otherwise using the inline write queue.
    ///
    /// # Arguments
    /// * `correlation_id` - The correlation ID from the original request
    /// * `payload` - The response payload as owned Bytes
    ///
    /// # Returns
    /// Ok(()) on success, or an error if sending failed
    pub async fn send_response_auto_bytes(
        &self,
        correlation_id: u32,
        payload: bytes::Bytes,
    ) -> Result<()> {
        // R-9: route large deferred replies through streaming, mirroring the
        // immediate-response path (see `send_response_auto`).
        if payload.len() > self.streaming_threshold() {
            return self.stream_response_bytes(payload, correlation_id).await;
        }
        self.write_response_inline(payload, correlation_id).await
    }

    /// Cold-path cancellation for a partially queued V5 stream.
    pub async fn abort_stream(&self, stream_id: u32, reason: u32) -> Result<()> {
        self.write_bytes_control(bytes::Bytes::copy_from_slice(
            &crate::framing::write_stream_abort_header(stream_id, reason),
        ))
        .await
    }

    /// Drop-safe cancellation for a partially queued V5 stream.
    ///
    /// The abort uses the same FIFO as stream chunks, so it is ordered after
    /// every chunk accepted before cancellation. If that FIFO cannot accept an
    /// abort, shut the transport down: a closed socket releases the peer's
    /// reassembly immediately, whereas silently losing the abort leaks it for
    /// the idle timeout.
    fn try_abort_stream(&self, stream_id: u32, reason: u32) {
        if self
            .streaming_queue
            .try_push(StreamingCommand::Abort { stream_id, reason })
            .is_err()
        {
            self.signal_shutdown();
        }
    }

    /// Zero-copy vectored write for header + payload in single operation
    /// This eliminates copying payload data into frame buffer - optimal for streaming
    pub async fn write_bytes_vectored<const N: usize>(
        &self,
        header: [u8; N],
        payload: bytes::Bytes,
    ) -> Result<()> {
        // Create vectored command that preserves both header and payload as separate Bytes
        let command = VectoredSendItem {
            header: InlineFrameHeader::from_array(header),
            payload,
        };

        // A stream's StartData and Data frames must share one FIFO queue.
        // Waiting for capacity preserves frame order under backpressure.
        self.streaming_queue
            .push(StreamingCommand::VectoredWrite(command))
            .await
    }

    /// Send owned chunks without copying - optimal for streaming large messages
    pub fn write_owned_chunks(&self, chunks: Vec<bytes::Bytes>) -> Result<()> {
        if chunks.is_empty() {
            return Ok(());
        }

        // Send chunks as a batch via the bounded streaming queue for optimal vectored I/O.
        let command = StreamingCommand::OwnedChunks(chunks);
        self.streaming_queue
            .try_push(command)
            .map_err(|_| GossipError::WriteQueueFull)?;

        Ok(())
    }
}

/// Arms after `StreamStart` has entered the streaming FIFO. Dropping an async
/// sender while it is backpressured must release the peer's pre-allocation,
/// but `Drop` may not await. The guard therefore publishes an ordered abort on
/// the same queue and fails closed if the queue is no longer usable.
struct StreamAbortGuard<'a> {
    handle: &'a LockFreeStreamHandle,
    stream_id: u32,
    armed: bool,
}

impl<'a> StreamAbortGuard<'a> {
    fn new(handle: &'a LockFreeStreamHandle, stream_id: u32) -> Self {
        Self { handle, stream_id, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for StreamAbortGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.handle.try_abort_stream(self.stream_id, 1);
        }
    }
}

/// Guard to ensure streaming_active is released on drop. Holds the streaming
/// gate permit (R16e) so the next waiter is admitted, and clears the
/// observability flag, when the stream finishes or is dropped/cancelled.
struct StreamingGuard {
    flag: Arc<AtomicBool>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl Drop for StreamingGuard {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
    }
}

impl Debug for LockFreeStreamHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LockFreeStreamHandle")
            .field("addr", &self.addr)
            .field("channel_id", &self.channel_id)
            .field("bytes_written", &self.bytes_written.load(Ordering::Relaxed))
            .field("sequence", &self.sequence_counter.load(Ordering::Relaxed))
            .finish()
    }
}

#[derive(Debug, PartialEq, Eq)]
#[cfg(any(test, feature = "test-helpers", debug_assertions))]
#[allow(dead_code)]
enum DirectPayloadError {
    HeaderTooShort,
    PayloadTruncated { expected: usize, available: usize },
}

#[cfg(any(test, feature = "test-helpers", debug_assertions))]
#[allow(dead_code)]
fn parse_direct_message_payload<'a>(
    msg_data: &'a [u8],
) -> std::result::Result<&'a [u8], DirectPayloadError> {
    if msg_data.len() < crate::framing::DIRECT_ASK_HEADER_LEN {
        return Err(DirectPayloadError::HeaderTooShort);
    }

    let payload_len =
        u32::from_be_bytes([msg_data[3], msg_data[4], msg_data[5], msg_data[6]]) as usize;
    let payload_start = crate::framing::DIRECT_ASK_HEADER_LEN;
    let payload_end = payload_start + payload_len;

    if msg_data.len() < payload_end {
        return Err(DirectPayloadError::PayloadTruncated {
            expected: payload_len,
            available: msg_data.len().saturating_sub(payload_start),
        });
    }

    Ok(&msg_data[payload_start..payload_end])
}

#[cfg(test)]
mod route_interning_tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    async fn run_routed_ask_route_reuse_body() {
        let (client, mut peer) = tokio::io::duplex(1024);
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9901".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );

        writer
            .write_routed_actor_ask(17, 7, 9, bytes::Bytes::from_static(b"one"))
            .await
            .unwrap();
        let mut bind = [0u8; crate::framing::ROUTE_BIND_FRAME_HEADER_LEN];
        peer.read_exact(&mut bind).await.unwrap();
        assert_eq!(
            crate::framing::decode_control(bind[..4].try_into().unwrap()).unwrap().kind,
            crate::framing::WireKind::RouteBind
        );

        let mut first = [0u8; crate::framing::ROUTED_ACTOR_ASK_FRAME_HEADER_LEN + 3];
        peer.read_exact(&mut first).await.unwrap();
        assert_eq!(
            crate::framing::decode_control(first[..4].try_into().unwrap()).unwrap().kind,
            crate::framing::WireKind::RoutedActorAsk
        );
        assert_eq!(&first[crate::framing::ROUTED_ACTOR_ASK_FRAME_HEADER_LEN..], b"one");

        writer
            .write_routed_actor_ask(18, 7, 9, bytes::Bytes::from_static(b"two"))
            .await
            .unwrap();
        let mut second = [0u8; crate::framing::ROUTED_ACTOR_ASK_FRAME_HEADER_LEN + 3];
        peer.read_exact(&mut second).await.unwrap();
        assert_eq!(
            crate::framing::decode_control(second[..4].try_into().unwrap()).unwrap().kind,
            crate::framing::WireKind::RoutedActorAsk
        );
        assert_eq!(&second[crate::framing::ROUTED_ACTOR_ASK_FRAME_HEADER_LEN..], b"two");

        writer.shutdown();
        task.await.unwrap();
    }

    // R-2 (#144 regression): this test hung forever on remote main — every
    // routed ask and its bind drained fine, but `writer.shutdown()` only stored
    // the flag without waking the writer parked in the idle select, so
    // `task.await` never returned (plain `cargo test` never terminated). The
    // timeout converts any future regression from a suite-wide hang into a
    // clean test failure and STAYS after the fix.
    #[tokio::test]
    async fn first_routed_ask_binds_before_compact_frame_and_reuses_the_route() {
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            run_routed_ask_route_reuse_body(),
        )
        .await;
        assert!(
            outcome.is_ok(),
            "routed-ask route-reuse flow must not hang (R-2): shutdown must wake the parked writer"
        );
    }

    /// R-2: same route-reuse invariant on the multi-thread flavor, so the
    /// contract is pinned independently of the runtime flavor.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn qa_r2_route_reuse_multithread() {
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            run_routed_ask_route_reuse_body(),
        )
        .await;
        assert!(
            outcome.is_ok(),
            "routed-ask route-reuse flow must not hang on the multi-thread runtime (R-2)"
        );
    }

    /// R-2: the exact lost-wakeup point — `shutdown()` must wake a writer parked
    /// in the idle select. Before the fix it only stored the flag, so a parked
    /// writer never re-checked it and `task` (the writer's JoinHandle) hung
    /// forever. Two `yield_now`s let the current-thread runtime run the spawned
    /// writer to its idle park point (no frames queued, no read context) before
    /// shutdown is called, making the regression deterministic rather than
    /// timing-dependent.
    #[tokio::test]
    async fn qa_r2_shutdown_wakes_parked_writer() {
        let (client, _peer) = tokio::io::duplex(128);
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9903".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        writer.shutdown();
        let exited = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
        assert!(
            exited.is_ok(),
            "shutdown() must wake a writer parked in the idle select (R-2)"
        );
    }

    /// R-8: cancellation must be ordered after the accepted StreamStart. The
    /// abort used to go through the normal queue, which is serviced between
    /// streaming turns and could overtake queued stream data.
    #[tokio::test]
    async fn qa_r8_cancelled_stream_abort_follows_stream_start() {
        let (client, mut peer) = tokio::io::duplex(1024);
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9905".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let stream_id = 7;
        let start = crate::framing::write_stream_request_start_header(
            stream_id, 1, 1, 2, 3, 1,
        );
        writer
            .streaming_queue
            .push(StreamingCommand::VectoredWrite(VectoredSendItem {
                header: InlineFrameHeader::from_array(start),
                payload: bytes::Bytes::from_static(b"x"),
            }))
            .await
            .unwrap();
        drop(StreamAbortGuard::new(&writer, stream_id));

        let mut received_start = vec![0u8; start.len() + 1];
        peer.read_exact(&mut received_start).await.unwrap();
        assert_eq!(&received_start[..start.len()], &start);
        assert_eq!(&received_start[start.len()..], b"x");

        let expected_abort = crate::framing::write_stream_abort_header(stream_id, 1);
        let mut abort = [0u8; crate::framing::STREAM_DATA_HEADER_LEN + crate::framing::LENGTH_PREFIX_LEN];
        peer.read_exact(&mut abort).await.unwrap();
        assert_eq!(abort, expected_abort);

        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task)
            .await
            .expect("writer exits after R-8 cancellation test");
    }

    #[tokio::test]
    async fn failed_first_bind_does_not_publish_a_route_for_retry() {
        let (client, _peer) = tokio::io::duplex(128);
        let (writer, _task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9902".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        writer.shutdown();
        assert!(writer
            .write_routed_actor_ask(17, 7, 9, bytes::Bytes::from_static(b"one"))
            .await
            .is_err());
        let (_, retry_is_fresh) = writer
            .outbound_routes
            .slot_for(crate::route_interning::RouteKey { actor_id: 7, type_hash: 9 })
            .unwrap();
        assert!(retry_is_fresh, "failed bind must not publish its route");
    }

    /// P1: a full route table must fall back to the uncompact `ActorAsk`
    /// frame (which carries `actor_id`/`type_hash` directly and needs no
    /// connection-local slot) instead of permanently failing every new ask
    /// on the connection with a misleading `GossipError::Shutdown`.
    #[tokio::test]
    async fn route_table_full_falls_back_to_uncompact_actor_ask_instead_of_failing() {
        let (client, mut peer) = tokio::io::duplex(1024);
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9910".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );

        for i in 0..crate::route_interning::MAX_ROUTES_PER_CONNECTION as u64 {
            writer
                .outbound_routes
                .slot_for(crate::route_interning::RouteKey {
                    actor_id: i,
                    type_hash: 1,
                })
                .expect("table has room until MAX_ROUTES_PER_CONNECTION");
        }

        let result = writer
            .write_routed_actor_ask(1, 999_999, 42, bytes::Bytes::from_static(b"payload"))
            .await;
        assert!(
            result.is_ok(),
            "a full route table must fall back to an uncompact ask, not fail the send: {result:?}"
        );

        let mut header = [0u8; crate::framing::ACTOR_ASK_FRAME_HEADER_LEN];
        peer.read_exact(&mut header).await.unwrap();
        let control =
            crate::framing::decode_control(header[..4].try_into().unwrap()).unwrap();
        assert_eq!(
            control.kind,
            crate::framing::WireKind::ActorAsk,
            "fallback must use the uncompact ActorAsk frame, which needs no connection-local slot"
        );
        let correlation_id = u32::from_be_bytes(header[4..8].try_into().unwrap());
        let actor_id = u64::from_be_bytes(header[8..16].try_into().unwrap());
        let type_hash = u32::from_be_bytes(header[16..20].try_into().unwrap());
        assert_eq!(correlation_id, 1);
        assert_eq!(actor_id, 999_999);
        assert_eq!(type_hash, 42);

        let mut payload = [0u8; 7];
        peer.read_exact(&mut payload).await.unwrap();
        assert_eq!(&payload, b"payload");

        writer.shutdown();
        task.await.unwrap();
    }

    /// P1: the fallback must be sustained, not a one-shot escape hatch — every
    /// subsequent new route on a full table must also succeed via fallback,
    /// existing (already-interned) routes must keep using the compact frame,
    /// and the connection must stay open throughout (no teardown).
    #[tokio::test]
    async fn route_table_full_keeps_connection_open_for_known_and_new_routes() {
        let (client, mut peer) = tokio::io::duplex(4096);
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9911".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );

        for i in 0..crate::route_interning::MAX_ROUTES_PER_CONNECTION as u64 {
            writer
                .outbound_routes
                .slot_for(crate::route_interning::RouteKey {
                    actor_id: i,
                    type_hash: 1,
                })
                .expect("table has room until MAX_ROUTES_PER_CONNECTION");
        }

        // A known (already-interned) route still uses the compact frame.
        writer
            .write_routed_actor_ask(10, 0, 1, bytes::Bytes::from_static(b"known"))
            .await
            .expect("an already-interned route must not be affected by a full table");
        let mut known_header = [0u8; crate::framing::ROUTED_ACTOR_ASK_FRAME_HEADER_LEN];
        peer.read_exact(&mut known_header).await.unwrap();
        assert_eq!(
            crate::framing::decode_control(known_header[..4].try_into().unwrap())
                .unwrap()
                .kind,
            crate::framing::WireKind::RoutedActorAsk,
            "an already-known route is unaffected by the table being full"
        );
        let mut known_payload = [0u8; 5];
        peer.read_exact(&mut known_payload).await.unwrap();
        assert_eq!(&known_payload, b"known");

        // Two distinct brand-new routes both fall back cleanly; the
        // connection must not be torn down after the first one.
        for (correlation_id, actor_id) in [(20u32, 1_000_000u64), (21u32, 1_000_001u64)] {
            let result = writer
                .write_routed_actor_ask(
                    correlation_id,
                    actor_id,
                    7,
                    bytes::Bytes::from_static(b"new"),
                )
                .await;
            assert!(
                result.is_ok(),
                "sustained fallback must keep succeeding for new routes: {result:?}"
            );
            let mut header = [0u8; crate::framing::ACTOR_ASK_FRAME_HEADER_LEN];
            peer.read_exact(&mut header).await.unwrap();
            assert_eq!(
                crate::framing::decode_control(header[..4].try_into().unwrap())
                    .unwrap()
                    .kind,
                crate::framing::WireKind::ActorAsk
            );
            let mut payload = [0u8; 3];
            peer.read_exact(&mut payload).await.unwrap();
            assert_eq!(&payload, b"new");
        }

        assert!(
            !writer.shutdown_signal.load(std::sync::atomic::Ordering::Relaxed),
            "a full route table must never trigger connection shutdown"
        );

        writer.shutdown();
        task.await.unwrap();
    }

    /// R-3: cancelling `write_routed_actor_ask` while its RouteBind enqueue is
    /// parked on a full write queue must roll back the freshly-allocated route.
    /// Without the RAII guard the route stays marked bound, so the next ask
    /// sends a RoutedActorAsk for a slot the peer never learned -> the peer
    /// returns "unknown route slot" and tears the whole connection down.
    ///
    /// Current-thread flavor is deliberate: the spawned writer must NOT drain
    /// the queue during the test (otherwise it keeps a frame in-flight and
    /// leaves queue space, so the bind's `try_push` would succeed). None of the
    /// awaits below yield until the final `task.await`, so the writer never runs
    /// until cleanup and the queue stays genuinely full.
    #[tokio::test]
    async fn qa_r3_cancelled_bind_enqueue_unbinds_route() {
        let (client, peer) = tokio::io::duplex(8);
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9904".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default().with_write_queue_capacity(128),
            None,
            None,
        );

        // Saturate the write queue. Each push completes without yielding while
        // there is space; a zero-duration timeout turns the parked (129th) push
        // into a clean "queue is full" signal — no sleep-based synchronization.
        let pad = bytes::Bytes::from_static(b"padpadpadpad");
        while let Ok(Ok(())) = tokio::time::timeout(
            std::time::Duration::from_millis(0),
            writer.write_bytes_control(pad.clone()),
        )
        .await
        {}

        // A routed ask for a fresh route: slot_for marks it bound, then the
        // RouteBind enqueue parks on the full queue and is cancelled at once.
        let route = crate::route_interning::RouteKey { actor_id: 42, type_hash: 7 };
        let cancelled = tokio::time::timeout(
            std::time::Duration::from_millis(0),
            writer.write_routed_actor_ask(1, 42, 7, bytes::Bytes::from_static(b"ask")),
        )
        .await;
        assert!(
            cancelled.is_err(),
            "the RouteBind enqueue should park on the full write queue"
        );

        // R-3: the cancelled fresh allocation must roll back, so the next
        // slot_for still needs a bind. Before the fix the route stayed bound
        // (needs_bind == false) and the next ask sent RoutedActorAsk for a slot
        // the peer never learned.
        let (_, needs_bind) = writer.outbound_routes.slot_for(route).unwrap();
        assert!(
            needs_bind,
            "cancelled bind enqueue must roll back the route (R-3)"
        );

        // Cleanup: drop the unread peer half so the writer's blocked write
        // errors and the task exits; the current-thread runtime drains it at
        // task.await.
        drop(peer);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    /// R-5: per-handle stream ids occupy the odd partition (disjoint from the
    /// direct-response allocator's even ids), so they can never collide on
    /// `stream_id` with a direct streaming response on the same connection.
    #[tokio::test]
    async fn qa_r5_handle_stream_ids_are_odd() {
        let (client, _peer) = tokio::io::duplex(64);
        let (writer, _task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9905".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let mut ids = std::collections::HashSet::new();
        for _ in 0..10_000 {
            let id = writer.allocate_stream_id().unwrap();
            assert!(
                id != 0 && id % 2 == 1,
                "handle stream id must be odd and nonzero, got {id}"
            );
            assert!(ids.insert(id), "handle stream id reused: {id}");
        }
    }

    /// R-5 (review follow-up): the per-handle allocator must exhaust at the
    /// u32::MAX sentinel WITHOUT wrapping back to 1 (which would reuse id 1 on
    /// a still-live connection). u32::MAX - 2 is the last id handed out;
    /// u32::MAX is the sentinel that shuts the connection down.
    #[tokio::test]
    async fn qa_r5_handle_allocator_exhausts_without_wrapping_to_one() {
        let (client, _peer) = tokio::io::duplex(64);
        let (writer, _task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9906".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        // Park the allocator one id before the sentinel.
        writer
            .next_stream_id
            .store(u32::MAX - 2, Ordering::Release);
        assert_eq!(writer.allocate_stream_id().unwrap(), u32::MAX - 2);
        // u32::MAX is the sentinel: exhaustion -> Shutdown, no wrap to 1.
        assert!(writer.allocate_stream_id().is_err());
        // Subsequent allocations stay shut down (never hand out id 1 again).
        assert!(writer.allocate_stream_id().is_err());
        assert_eq!(
            writer.next_stream_id.load(Ordering::Acquire),
            u32::MAX,
            "counter must rest on the sentinel, not wrap to 1"
        );
    }

    /// R-9: a streaming payload larger than MAX_STREAM_SIZE must fail locally
    /// (MessageTooLarge) before any frame is emitted -- every receiver hard-
    /// rejects such a stream as a FATAL error, so sending it would tear the
    /// connection down with collateral loss.
    #[tokio::test]
    async fn qa_r9_oversized_stream_fails_locally() {
        let (client, _peer) = tokio::io::duplex(64);
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9907".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let oversized = bytes::BytesMut::zeroed(crate::MAX_STREAM_SIZE + 1).freeze();
        let err = writer
            .stream_large_message_bytes(oversized, 1, 7)
            .await
            .unwrap_err();
        assert!(
            matches!(err, crate::GossipError::MessageTooLarge { .. }),
            "expected MessageTooLarge, got {err:?}"
        );
        writer.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }

    /// R-9: a deferred reply above the streaming threshold is streamed (a
    /// StreamResponseStart frame), not written as one inline Response frame
    /// (which the peer would reject as MessageTooLarge, and which would panic
    /// the frame header length check at >= 2^27 bytes).
    #[tokio::test]
    async fn qa_r9_large_deferred_reply_streams_not_inlines() {
        let (client, peer) = tokio::io::duplex(8 * 1024);
        let (writer, task, _) = LockFreeStreamHandle::new(
            client,
            "127.0.0.1:9908".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        // Above the default ~1MB streaming threshold.
        let payload = bytes::BytesMut::zeroed(2 * 1024 * 1024).freeze();
        writer.send_response_auto_bytes(42, payload).await.unwrap();
        // The first frame on the wire must be a StreamResponseStart, not an
        // inline Response.
        let mut ctrl = [0u8; crate::framing::LENGTH_PREFIX_LEN];
        let mut peer = peer;
        peer.read_exact(&mut ctrl).await.unwrap();
        let kind = crate::framing::decode_control(ctrl).unwrap().kind;
        assert_eq!(
            kind,
            crate::framing::WireKind::StreamResponseStart,
            "large deferred reply must stream (R-9)"
        );
        writer.shutdown();
        drop(peer);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), task).await;
    }
}
