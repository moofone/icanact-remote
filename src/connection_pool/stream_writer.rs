/// Truly lock-free streaming handle with dedicated background writer
#[derive(Clone)]
pub struct LockFreeStreamHandle {
    /// Unique per-handle id used to ignore disconnect callbacks from stale connections.
    instance_id: u64,
    addr: SocketAddr,
    channel_id: ChannelId,
    sequence_counter: Arc<AtomicUsize>,
    frame_sequence: Arc<AtomicUsize>,
    bytes_written: Arc<AtomicUsize>, // This tracks actual TCP bytes written
    shutdown_signal: Arc<AtomicBool>,
    exit_flag: Arc<AtomicBool>,
    exit_notify: Arc<Notify>,
    flush_pending: Arc<AtomicBool>,
    /// Atomic flag for coordinating streaming mode
    streaming_active: Arc<AtomicBool>,
    /// Lock-free write queue for payload writes
    write_queue: Arc<WriteQueue>,
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
}

impl LockFreeStreamHandle {
    fn next_instance_id() -> u64 {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    }

    pub fn instance_id(&self) -> u64 {
        self.instance_id
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
        let flush_pending = Arc::new(AtomicBool::new(false));
        let exit_flag = Arc::new(AtomicBool::new(false));
        let exit_notify = Arc::new(Notify::new());

        // Create shared counter for actual TCP bytes written
        let bytes_written = Arc::new(AtomicUsize::new(0));

        // Create lock-free queue for payload writes and channel for streaming commands
        let write_queue = WriteQueue::new(buffer_config.write_queue_capacity());
        let streaming_queue = StreamingQueue::new(buffer_config.write_queue_capacity());

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
            let writer_addr = addr;
            let writer_channel_id = channel_id;
            let write_queue = write_queue.clone();
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
                    streaming_queue,
                    read_context,
                    instance_id,
                    exit_flag_for_task,
                    exit_notify_for_task,
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
                frame_sequence: Arc::new(AtomicUsize::new(0)),
                bytes_written, // This now tracks actual TCP bytes written
                shutdown_signal,
                flush_pending,
                exit_flag,
                exit_notify,
                streaming_active,
                write_queue,
                streaming_queue,
                buffer_config,
                max_message_size,
                schema_hash,
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
        streaming_queue: Arc<StreamingQueue>,
        read_context: Option<ReadContext>,
        instance_id: u64,
        exit_flag: Arc<AtomicBool>,
        exit_notify: Arc<Notify>,
    ) where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        // CRITICAL_PATH: owner-batched send queue + vectored TLS writes.
        use std::io::IoSlice;

        async fn write_vectored_all<S>(
            stream: &mut S,
            slices: &[IoSlice<'_>],
        ) -> std::io::Result<usize>
        where
            S: AsyncWrite + Unpin,
        {
            let total_len: usize = slices.iter().map(|s| s.len()).sum();
            if total_len == 0 {
                return Ok(0);
            }

            // If the underlying stream doesn't support vectored writes, preserve ordering by
            // writing each slice sequentially.
            if !stream.is_write_vectored() {
                for s in slices {
                    stream.write_all(s.as_ref()).await?;
                }
                return Ok(total_len);
            }

            let n = stream.write_vectored(slices).await?;
            if n == total_len {
                return Ok(n);
            }

            // Short write: complete the remainder sequentially without allocating a combined buffer.
            let mut idx = 0usize;
            let mut off = n;
            while idx < slices.len() && off >= slices[idx].len() {
                off -= slices[idx].len();
                idx += 1;
            }
            if idx < slices.len() {
                if off < slices[idx].len() {
                    let b = &slices[idx].as_ref()[off..];
                    stream.write_all(b).await?;
                    idx += 1;
                }
                while idx < slices.len() {
                    stream.write_all(slices[idx].as_ref()).await?;
                    idx += 1;
                }
            }
            Ok(total_len)
        }

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
                ReadIoResult::Generic(crate::handle::MessageReadResult::Actor { msg_type, .. })
                    if *msg_type == crate::MessageType::ActorAsk as u8 =>
                {
                    ASK_READ_BATCH_LIMIT
                }
                ReadIoResult::Generic(crate::handle::MessageReadResult::DirectAsk { .. })
                | ReadIoResult::Generic(crate::handle::MessageReadResult::DirectResponse { .. })
                | ReadIoResult::Generic(crate::handle::MessageReadResult::Response { .. }) => {
                    ASK_READ_BATCH_LIMIT
                }
                _ => READ_BATCH_LIMIT,
            }
        }

        struct ExitGuard {
            flag: Arc<AtomicBool>,
            notify: Arc<Notify>,
            response_correlation: Option<Arc<CorrelationTracker>>,
            registry_weak: Option<std::sync::Weak<GossipRegistry>>,
            peer_addr: Option<SocketAddr>,
            peer_id: Option<crate::PeerId>,
            instance_id: u64,
        }

        impl Drop for ExitGuard {
            fn drop(&mut self) {
                self.flag.store(true, Ordering::Release);
                self.notify.notify_waiters();
                let mut should_cancel_pending = true;
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

                    if let Some(peer_id) = peer_id.as_ref()
                        && let Some(current) = pool.get_connection_by_peer_id(peer_id)
                        && let Some(handle) = current.stream_handle.as_ref()
                        && handle.instance_id() != expected_instance
                    {
                        should_cancel_pending = false;
                        debug!(
                            peer = %peer_addr,
                            peer_id = %peer_id,
                            exiting_instance = expected_instance,
                            current_instance = handle.instance_id(),
                            "IO task exited for stale connection; skipping pending cancel/failure handling"
                        );
                    } else if peer_id.is_none()
                        && let Some(current) = pool.get_lock_free_connection(peer_addr)
                        && let Some(handle) = current.stream_handle.as_ref()
                        && handle.instance_id() != expected_instance
                    {
                        should_cancel_pending = false;
                        debug!(
                            peer = %peer_addr,
                            exiting_instance = expected_instance,
                            current_instance = handle.instance_id(),
                            "IO task exited for stale addr-mapped connection; skipping pending cancel/failure handling"
                        );
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
                            if let Err(e) = registry.handle_peer_connection_failure(peer_addr).await
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
            response_correlation: read_context
                .as_ref()
                .and_then(|ctx| ctx.response_correlation.clone()),
            registry_weak: read_context.as_ref().map(|ctx| ctx.registry_weak.clone()),
            peer_addr: read_context.as_ref().map(|ctx| ctx.peer_addr),
            peer_id: read_context.as_ref().and_then(|ctx| ctx.peer_id.clone()),
            instance_id,
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
        const FLUSH_THRESHOLD: usize = 64 * 1024; // Favor batching on tell; ask has its own fast flush path

        let mut bytes_since_flush = 0;
        let mut last_flush = std::time::Instant::now();

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
        let mut read_state = read_context.as_ref().map(|_| ReadState::new());
        let mut streaming_state = read_context
            .as_ref()
            .map(|_| crate::protocol::StreamingState::new());
        let mut last_cleanup = std::time::Instant::now();

        while !shutdown_signal.load(Ordering::Relaxed) {
            let mut total_bytes_written = 0;
            let mut did_work = false;
            let mut wrote_ask_payload = false;
            let mut wrote_actor_responses = false;
            let mut wrote_fast_responses = false;
            response_batch.clear();
            direct_response_batch.clear();

            while let Some(cmd) = streaming_queue.pop() {
                did_work = true;
                match cmd {
                    StreamingCommand::WriteBytes(data) => match stream.write_all(&data).await {
                        Ok(_) => {
                            bytes_written_counter.fetch_add(data.len(), Ordering::Relaxed);
                            total_bytes_written += data.len();
                        }
                        Err(e) => {
                            error!("Streaming write error: {}", e);
                            return;
                        }
                    },
                    StreamingCommand::Flush => {
                        let _ = stream.flush().await;
                        flush_pending.store(false, Ordering::Release);
                        last_flush = std::time::Instant::now();
                        bytes_since_flush = 0;
                    }
                    StreamingCommand::VectoredWrite(cmd) => {
                        // Handle short writes by falling back to sequential write_all
                        // TCP can return partial writes under backpressure
                        let total_len = cmd.header.len() + cmd.payload.len();
                        let header_slice = std::io::IoSlice::new(&cmd.header);
                        let payload_slice = std::io::IoSlice::new(&cmd.payload);
                        let bufs = &[header_slice, payload_slice];

                        match write_vectored_all(&mut stream, bufs).await {
                            Ok(n) if n == total_len => {
                                bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                total_bytes_written += n;
                            }
                            Ok(n) => {
                                // Short write - write remaining bytes sequentially using stack buffer
                                bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                total_bytes_written += n;
                                let _remaining = total_len - n;
                                let mut offset = n;
                                // Write header portion if needed
                                if offset < cmd.header.len() {
                                    let h_rem = cmd.header.len() - offset;
                                    if let Err(_) = stream.write_all(&cmd.header[offset..]).await {
                                        return;
                                    }
                                    bytes_written_counter.fetch_add(h_rem, Ordering::Relaxed);
                                    total_bytes_written += h_rem;
                                    offset = 0;
                                } else {
                                    offset -= cmd.header.len();
                                }
                                // Write payload portion
                                if let Err(_) = stream.write_all(&cmd.payload[offset..]).await {
                                    return;
                                }
                                bytes_written_counter
                                    .fetch_add(cmd.payload.len() - offset, Ordering::Relaxed);
                                total_bytes_written += cmd.payload.len() - offset;
                            }
                            Err(e) => {
                                error!("Vectored write error: {}", e);
                                return;
                            }
                        }
                    }
                    StreamingCommand::OwnedChunks(chunks) => {
                        // Handle short writes for owned chunks
                        let total_len: usize = chunks.iter().map(|c| c.len()).sum();
                        const MAX_IOV: usize = 64;
                        let mut slice_storage: [MaybeUninit<std::io::IoSlice<'_>>; MAX_IOV] = unsafe {
                            MaybeUninit::<[MaybeUninit<std::io::IoSlice<'_>>; MAX_IOV]>::uninit()
                                .assume_init()
                        };
                        let chunk_count = chunks.len().min(MAX_IOV);
                        for (idx, chunk) in chunks.iter().take(MAX_IOV).enumerate() {
                            slice_storage[idx].write(std::io::IoSlice::new(&chunk));
                        }
                        let slices = unsafe {
                            std::slice::from_raw_parts(
                                slice_storage.as_ptr() as *const std::io::IoSlice<'_>,
                                chunk_count,
                            )
                        };

                        match write_vectored_all(&mut stream, slices).await {
                            Ok(n) if n == total_len => {
                                bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                total_bytes_written += n;
                            }
                            Ok(n) => {
                                // Short write - write remaining bytes sequentially
                                bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                total_bytes_written += n;
                                let mut remaining = total_len - n;
                                let mut chunk_idx = 0;
                                let mut offset_in_chunk = n;
                                // Find which chunk we left off in
                                while chunk_idx < chunks.len()
                                    && offset_in_chunk >= chunks[chunk_idx].len()
                                {
                                    offset_in_chunk -= chunks[chunk_idx].len();
                                    chunk_idx += 1;
                                }
                                // Continue writing from current position
                                while chunk_idx < chunks.len() && remaining > 0 {
                                    match stream.write(&chunks[chunk_idx][offset_in_chunk..]).await
                                    {
                                        Ok(written) => {
                                            bytes_written_counter
                                                .fetch_add(written, Ordering::Relaxed);
                                            total_bytes_written += written;
                                            remaining -= written;
                                            offset_in_chunk += written;
                                            if offset_in_chunk >= chunks[chunk_idx].len() {
                                                chunk_idx += 1;
                                                offset_in_chunk = 0;
                                            }
                                        }
                                        Err(e) => {
                                            error!("Chunk batch write completion error: {}", e);
                                            return;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Chunk batch write error: {}", e);
                                return;
                            }
                        }
                    }
                }
                streaming_queue.notify_space();
            }

            if !streaming_active.load(Ordering::Acquire) {
                // Reuse pre-allocated buffers instead of creating new ones
                write_chunks.clear();
                owner_batch.clear();
                inline32_headers.clear();
                inline32_payloads.clear();

                if let Some(cmd) = pending_cmd.take() {
                    owner_batch.push(cmd);
                }

                while owner_batch.len() < OWNER_BATCH_SIZE {
                    match write_queue.pop() {
                        Some(command) => owner_batch.push(command),
                        None => break,
                    }
                }

                if !owner_batch.is_empty() {
                    did_work = true;
                    for command in owner_batch.drain(..) {
                        write_queue.notify_space();
                        let is_ask_payload = matches!(&command, WriteCommand::AskPayload(_));
                        let payload = match command {
                            WriteCommand::Payload(payload) => payload,
                            WriteCommand::AskPayload(payload) => {
                                wrote_ask_payload = true;
                                payload
                            }
                        };
                        let ask_write_start =
                            if is_ask_payload && perf.is_some() {
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
                                        Ok(0) => break,
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
                                        Ok(0) => break,
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
                                        Ok(0) => break,
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
                                    let bytes_written =
                                        match write_chunks_batched(&mut stream, &write_chunks).await
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

            bytes_since_flush += total_bytes_written;
            let elapsed = last_flush.elapsed();

            if should_flush(
                bytes_since_flush,
                elapsed,
                FLUSH_THRESHOLD,
                WRITER_MAX_LATENCY,
            ) {
                let _ = stream.flush().await;
                bytes_since_flush = 0;
                last_flush = std::time::Instant::now();
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
                    while reads < read_batch_limit {
                        let read_start = perf.map(|_| Instant::now());
                        let read_result =
                            match read_message_step_nonblocking(&mut stream, state, ctx).await {
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
                            read_batch_limit =
                                read_batch_limit.max(read_batch_limit_for(&result));
                            let Some(result) = try_handle_fast_io(
                                result,
                                ctx,
                                &mut stream,
                                &bytes_written_counter,
                                &mut bytes_since_flush,
                                &mut response_batch,
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
                                    ctx.response_correlation.as_ref().map(|c| c.as_ref()),
                                    ctx.sync_actor_handler.as_ref().map(|v| &**v),
                                    &mut stream,
                                    &bytes_written_counter,
                                    &mut bytes_since_flush,
                                    &mut response_batch,
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
                last_flush = std::time::Instant::now();
                flush_pending.store(false, Ordering::Release);
            }

            if !did_work {
                if let (Some(ctx), Some(state), Some(streaming_state)) = (
                    read_context.as_ref(),
                    read_state.as_mut(),
                    streaming_state.as_mut(),
                ) {
                    tokio::select! {
                        // Idle path: block waiting for socket readability.
                        read_result = read_message_step_poll(&mut stream, state, ctx, true) => {
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
                                let Some(result) = try_handle_fast_io(
                                    result,
                                    ctx,
                                    &mut stream,
                                    &bytes_written_counter,
                                    &mut bytes_since_flush,
                                    &mut response_batch,
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
                                        ctx.response_correlation.as_ref().map(|c| c.as_ref()),
                                        ctx.sync_actor_handler.as_ref().map(|v| &**v),
                                        &mut stream,
                                        &bytes_written_counter,
                                        &mut bytes_since_flush,
                                        &mut response_batch,
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

                            // Under load the socket can become readable with many frames queued.
                            // The old "idle path" processed only a single frame per wake-up,
                            // which inflates RTT and caps ActorAsk throughput on server-heavy links.
                            //
                            // Drain additional frames non-blocking to batch handler + response writes.
                            let mut drained = 0usize;
                            let mut drain_batch_limit = READ_BATCH_LIMIT;
                            while drained < drain_batch_limit {
                                let read_start = perf.map(|_| Instant::now());
                                let next = match read_message_step_nonblocking(&mut stream, state, ctx).await {
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
                                            ctx.response_correlation.as_ref().map(|c| c.as_ref()),
                                            ctx.sync_actor_handler.as_ref().map(|v| &**v),
                                            &mut stream,
                                            &bytes_written_counter,
                                            &mut bytes_since_flush,
                                            &mut response_batch,
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
                            // Ensure actor responses don't sit in the kernel/TLS buffers indefinitely
                            // on links that are primarily request->response (server-side).
                            if should_flush(
                                bytes_since_flush,
                                last_flush.elapsed(),
                                FLUSH_THRESHOLD,
                                WRITER_MAX_LATENCY,
                            ) {
                                let _ = stream.flush().await;
                                bytes_since_flush = 0;
                                last_flush = std::time::Instant::now();
                                flush_pending.store(false, Ordering::Release);
                            }
                        }
                        // Wake on new outbound writes even if the socket is currently idle for reads.
                        // Without this, a mostly-write workload (e.g., initial gossip propagation)
                        // can stall until an unrelated read event occurs.
                        _ = write_queue.data_notify.notified() => {
                            pending_cmd = write_queue.pop();
                        }
                        _ = write_queue.space_notify.notified() => {
                            // Producer wakeup only; no action needed.
                        }
                        _ = streaming_queue.data_notify.notified() => {
                            // Wake on streaming commands; drained at the top of the loop.
                        }
                        _ = tokio::time::sleep(WRITER_MAX_LATENCY) => {}
                    }
                } else {
                    tokio::select! {
                        _ = streaming_queue.data_notify.notified() => {
                            // Wake on streaming commands; drained at the top of the loop.
                        }
                        _ = write_queue.data_notify.notified() => {
                            pending_cmd = write_queue.pop();
                        }
                        _ = write_queue.space_notify.notified() => {
                            // Producer wakeup only; no action needed.
                        }
                        _ = tokio::time::sleep(WRITER_MAX_LATENCY) => {}
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
                    ) =
                        perf.snapshot_and_reset();
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
                self.write_queue.notify_data_if_empty_to_non_empty();
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
                self.write_queue.notify_data_if_empty_to_non_empty();
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
                self.write_queue.notify_data_if_empty_to_non_empty();
                Ok(())
            }
            Err(_) => Err(GossipError::WriteQueueFull),
        }
    }

    pub async fn write_bytes_ask(&self, data: bytes::Bytes) -> Result<()> {
        self.enqueue_ask_write(WritePayload::Single(data)).await
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
    /// Wire format: [length:4][type:1][correlation_id:2][payload_len:4][payload:N]
    pub async fn write_direct_ask_inline(
        &self,
        header: [u8; 16], // DIRECT_ASK_FRAME_HEADER_LEN
        payload: bytes::Bytes,
    ) -> Result<()> {
        self.enqueue_ask_write(WritePayload::DirectAskInline { header, payload })
            .await
    }

    /// Write DirectResponse inline (same format as DirectAsk)
    /// Wire format: [length:4][type:1][correlation_id:2][payload_len:4][payload:N]
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

    /// Enqueue bytes (legacy name kept for compatibility).
    pub fn write_bytes_nonblocking_checked(&self, data: bytes::Bytes) -> Result<()> {
        self.enqueue_write_nonblocking(WritePayload::Single(data))
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

    /// Get next stream frame sequence ID (wraps at u16::MAX)
    fn next_frame_sequence_id(&self) -> u16 {
        (self.frame_sequence.fetch_add(1, Ordering::Relaxed) & 0xFFFF) as u16
    }

    /// Get socket address
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Shutdown the background writer task
    pub fn shutdown(&self) {
        self.shutdown_signal.store(true, Ordering::Relaxed);
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
        // Streaming frame wire format excludes the 4-byte length prefix from `msg_len`.
        // `msg_len` for stream data frames is: type(1) + corr(2) + reserved(9) + header(36) + chunk(N).
        const STREAM_FRAME_OVERHEAD: usize = 12 + crate::StreamHeader::SERIALIZED_SIZE;

        let max_chunk = self.max_message_size.saturating_sub(STREAM_FRAME_OVERHEAD);
        if max_chunk == 0 {
            return Err(GossipError::InvalidConfig(format!(
                "max_message_size={} too small for streaming (overhead={})",
                self.max_message_size, STREAM_FRAME_OVERHEAD
            )));
        }
        Ok(std::cmp::min(STREAM_CHUNK_SIZE, max_chunk))
    }

    /// Stream a large message directly to the socket, bypassing the write queue
    /// This provides maximum performance for large messages like PreBacktest
    pub async fn stream_large_message(
        &self,
        msg: &[u8],
        type_hash: u32,
        actor_id: u64,
    ) -> Result<()> {
        use crate::{MessageType, StreamHeader, current_timestamp_nanos};

        let chunk_size = self.max_stream_chunk_size()?;

        // Acquire streaming mode atomically
        while self
            .streaming_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            tokio::task::yield_now().await;
        }

        // Ensure we release streaming mode on exit
        let _guard = StreamingGuard {
            flag: self.streaming_active.clone(),
        };

        // Generate unique stream ID using nanoseconds to avoid collisions
        let stream_id = current_timestamp_nanos();

        let schema_hash = self.schema_hash();

        // Helper to serialize message with type and header
        fn serialize_stream_message(
            msg_type: MessageType,
            header: &StreamHeader,
            schema_hash: Option<u64>,
        ) -> Vec<u8> {
            // Message format: [length:4][type:1][correlation_id:2][reserved:9][header:36]
            let inner_size = 12 + StreamHeader::SERIALIZED_SIZE; // type(1) + corr(2) + reserved(9) + header
            let mut message = Vec::with_capacity(4 + inner_size);

            // Length prefix (required by protocol)
            message.extend_from_slice(&(inner_size as u32).to_be_bytes()); // ALLOW_COPY

            // Header
            message.push(msg_type as u8);
            message.extend_from_slice(&[0, 0]); // ALLOW_COPY correlation_id (not used for streaming)
            let mut reserved = [0u8; 9];
            crate::framing::write_schema_hash(&mut reserved, schema_hash);
            message.extend_from_slice(&reserved); // ALLOW_COPY 9 reserved bytes for 32-byte alignment
            message.extend_from_slice(&header.to_bytes()); // ALLOW_COPY
            message
        }

        // Send StreamStart header
        let start_header = StreamHeader {
            stream_id,
            total_size: msg.len() as u64,
            chunk_size: 0,
            chunk_index: 0,
            type_hash,
            actor_id,
        };

        let start_msg =
            serialize_stream_message(MessageType::StreamStart, &start_header, schema_hash);
        self.streaming_queue
            .push(StreamingCommand::WriteBytes(start_msg.into()))
            .await?;

        // Stream chunks directly
        for (idx, chunk) in msg.chunks(chunk_size).enumerate() {
            let data_header = StreamHeader {
                stream_id,
                total_size: msg.len() as u64,
                chunk_size: chunk.len() as u32,
                chunk_index: idx as u32,
                type_hash,
                actor_id,
            };

            // Create combined message with proper length prefix
            // Message format: [length:4][type:1][correlation_id:2][reserved:9][header:36][chunk_data:N]
            let inner_size = 12 + StreamHeader::SERIALIZED_SIZE + chunk.len(); // type(1) + corr(2) + reserved(9) + header + data
            let mut chunk_msg = Vec::with_capacity(4 + inner_size);

            // Length prefix (includes header + chunk data)
            chunk_msg.extend_from_slice(&(inner_size as u32).to_be_bytes()); // ALLOW_COPY

            // Header
            chunk_msg.push(MessageType::StreamData as u8);
            chunk_msg.extend_from_slice(&[0, 0]); // ALLOW_COPY correlation_id
            let mut reserved = [0u8; 9];
            crate::framing::write_schema_hash(&mut reserved, schema_hash);
            chunk_msg.extend_from_slice(&reserved); // ALLOW_COPY 9 reserved bytes for 32-byte alignment
            chunk_msg.extend_from_slice(&data_header.to_bytes()); // ALLOW_COPY

            // Chunk data
            chunk_msg.extend_from_slice(chunk); // ALLOW_COPY

            self.streaming_queue
                .push(StreamingCommand::WriteBytes(chunk_msg.into()))
                .await?;

            // Yield periodically to prevent blocking
            if idx % 10 == 0 {
                self.streaming_queue.push(StreamingCommand::Flush).await?;
                tokio::task::yield_now().await;
            }
        }

        // Send StreamEnd
        let end_msg = serialize_stream_message(MessageType::StreamEnd, &start_header, schema_hash);
        self.streaming_queue
            .push(StreamingCommand::WriteBytes(end_msg.into()))
            .await?;
        self.streaming_queue.push(StreamingCommand::Flush).await?;

        debug!(
            "✅ STREAMING: Successfully streamed {} MB in {} chunks",
            msg.len() as f64 / 1_048_576.0,
            msg.len().div_ceil(chunk_size)
        );

        Ok(())
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
    pub async fn stream_response(&self, payload: &[u8], correlation_id: u16) -> Result<()> {
        // Convert to Bytes and use zero-copy implementation
        self.stream_response_bytes(bytes::Bytes::copy_from_slice(payload), correlation_id) // ALLOW_COPY
            .await
    }

    /// Zero-copy stream a response back to the caller.
    /// Uses vectored writes to avoid copying the payload data.
    ///
    /// # Arguments
    /// * `payload` - The response payload as owned Bytes
    /// * `correlation_id` - The correlation ID from the original request
    pub async fn stream_response_bytes(
        &self,
        payload: bytes::Bytes,
        correlation_id: u16,
    ) -> Result<()> {
        use crate::{MessageType, StreamHeader, current_timestamp_nanos};
        use bytes::BufMut;

        let chunk_size = self.max_stream_chunk_size()?;

        // Acquire streaming mode atomically
        while self
            .streaming_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            tokio::task::yield_now().await;
        }

        // Ensure we release streaming mode on exit
        let _guard = StreamingGuard {
            flag: self.streaming_active.clone(),
        };

        // Generate unique stream ID for this response stream
        let stream_id = current_timestamp_nanos();

        let schema_hash = self.schema_hash();

        // Helper to build stream response header bytes (zero-copy friendly)
        fn build_stream_response_header(
            msg_type: MessageType,
            header: &StreamHeader,
            correlation_id: u16,
            chunk_len: usize,
            schema_hash: Option<u64>,
        ) -> bytes::Bytes {
            // Message format: [length:4][type:1][correlation_id:2][reserved:9][header:36]
            let inner_size = 12 + StreamHeader::SERIALIZED_SIZE + chunk_len;
            let mut message =
                bytes::BytesMut::with_capacity(4 + 12 + StreamHeader::SERIALIZED_SIZE);

            // Length prefix
            message.put_u32(inner_size as u32);

            // Header with correlation ID for response matching
            message.put_u8(msg_type as u8);
            message.put_u16(correlation_id);
            let mut reserved = [0u8; 9];
            crate::framing::write_schema_hash(&mut reserved, schema_hash);
            message.put_slice(&reserved); // 9 reserved bytes
            message.put_slice(&header.to_bytes());
            message.freeze()
        }

        // Use StreamResponseStart to indicate this is a streaming response
        let start_header = StreamHeader {
            stream_id,
            total_size: payload.len() as u64,
            chunk_size: 0,
            chunk_index: 0,
            type_hash: 0, // Not needed for responses
            actor_id: 0,  // Actor ID doesn't matter - message type distinguishes responses
        };

        // Send StreamResponseStart
        let start_msg = build_stream_response_header(
            MessageType::StreamResponseStart,
            &start_header,
            correlation_id,
            0,
            schema_hash,
        );
        self.streaming_queue
            .push(StreamingCommand::WriteBytes(start_msg))
            .await?;

        // Stream response chunks using zero-copy slices
        let total_len = payload.len();
        let num_chunks = total_len.div_ceil(chunk_size);

        for idx in 0..num_chunks {
            let start = idx * chunk_size;
            let end = std::cmp::min(start + chunk_size, total_len);
            let chunk_len = end - start;

            // Zero-copy slice of the payload
            let chunk_data = payload.slice(start..end);

            let data_header = StreamHeader {
                stream_id,
                total_size: total_len as u64,
                chunk_size: chunk_len as u32,
                chunk_index: idx as u32,
                type_hash: 0,
                actor_id: 0,
            };

            // Build header bytes (small, okay to allocate)
            let header_bytes = build_stream_response_header(
                MessageType::StreamResponseData,
                &data_header,
                correlation_id,
                chunk_len,
                schema_hash,
            );

            // Use vectored write: header + chunk_data (zero-copy)
            self.streaming_queue
                .push(StreamingCommand::VectoredWrite(VectoredSendItem {
                    header: header_bytes,
                    payload: chunk_data,
                }))
                .await?;

            // Yield periodically to prevent blocking
            if idx % 10 == 0 {
                self.streaming_queue.push(StreamingCommand::Flush).await?;
                tokio::task::yield_now().await;
            }
        }

        // Send StreamResponseEnd
        let end_msg = build_stream_response_header(
            MessageType::StreamResponseEnd,
            &start_header,
            correlation_id,
            0,
            schema_hash,
        );
        self.streaming_queue
            .push(StreamingCommand::WriteBytes(end_msg))
            .await?;
        self.streaming_queue.push(StreamingCommand::Flush).await?;

        debug!(
            "✅ STREAMING RESPONSE: Successfully streamed {} bytes in {} chunks (correlation_id: {})",
            total_len, num_chunks, correlation_id
        );

        Ok(())
    }

    /// Send a response using the inline write queue (never streaming).
    pub async fn send_response_auto(
        &self,
        payload: bytes::Bytes,
        correlation_id: u16,
    ) -> Result<()> {
        let header = framing::write_ask_response_header(
            crate::MessageType::Response,
            correlation_id,
            payload.len(),
        );
        self.write_header_and_payload_control_inline(header, 16, payload)
            .await
    }

    /// Send a response with owned Bytes using the inline write queue (never streaming).
    ///
    /// # Arguments
    /// * `correlation_id` - The correlation ID from the original request
    /// * `payload` - The response payload as owned Bytes
    ///
    /// # Returns
    /// Ok(()) on success, or an error if sending failed
    pub async fn send_response_auto_bytes(
        &self,
        correlation_id: u16,
        payload: bytes::Bytes,
    ) -> Result<()> {
        let header = framing::write_ask_response_header(
            crate::MessageType::Response,
            correlation_id,
            payload.len(),
        );
        self.write_header_and_payload_control_inline(header, 16, payload)
            .await
    }

    /// Zero-copy vectored write for header + payload in single operation
    /// This eliminates copying payload data into frame buffer - optimal for streaming
    pub async fn write_bytes_vectored(
        &self,
        header: bytes::Bytes,
        payload: bytes::Bytes,
    ) -> Result<()> {
        // Create vectored command that preserves both header and payload as separate Bytes
        let command = VectoredSendItem { header, payload };

        // Prefer the streaming queue for vectored operations; if it's full, fall back
        // to the normal payload queue (still zero-copy: header + payload remain `Bytes`).
        match self
            .streaming_queue
            .try_push(StreamingCommand::VectoredWrite(command))
        {
            Ok(()) => Ok(()),
            Err(StreamingCommand::VectoredWrite(vectored_cmd)) => {
                self.enqueue_write(WritePayload::HeaderPayload {
                    header: vectored_cmd.header,
                    payload: vectored_cmd.payload,
                })
                .await
            }
            Err(_) => Err(GossipError::Shutdown),
        }
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

/// Guard to ensure streaming_active is released on drop
struct StreamingGuard {
    flag: Arc<AtomicBool>,
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
